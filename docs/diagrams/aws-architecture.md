# VisionForge on AWS: deployment architecture

Diagram: [aws-architecture.drawio](aws-architecture.drawio) (open in draw.io or diagrams.net).

## Request flow

1. **Client → AWS WAF → ALB.** WAF applies rate-based rules per IP and per `X-Client-ID`. These replace nginx's limits, but WAF counts over windows of a minute or more and has no concurrent-connection limit.
2. **ALB → Ingest API** (`POST /jobs`, `GET /jobs/{id}`). The API streams the upload to S3, counts the pages, and in one transaction inserts the job and a `split` outbox row into RDS.
3. **RDS → Outbox Publisher.** A `NOTIFY` wakes the publisher, which claims unsent rows (`FOR UPDATE SKIP LOCKED`), sends them to SQS and marks them sent.
4. **SQS `vf-split` → Splitter (Lambda).** The Lambda writes one single-page PDF per page to S3, then the pages and their `fast` outbox rows to RDS.
5. **SQS `vf-fast` → Fast worker → layout model** (via the NAT gateway). The result goes to S3; the worker commits the page state and a `slow` outbox row.
6. **SQS `vf-slow` → Slow worker → VLM** (via the NAT gateway). The worker writes the final result to S3 and commits `COMPLETED` plus a page event.
7. **RDS → Stream Gateway → Client.** Each page event `NOTIFY`s the gateway, which pushes it to the client over SSE or WebSocket through the ALB.
8. **ALB → Eval API** (`POST /evaluate`). This path is stateless: it scores a predicted tree against a ground-truth tree (CER, WER, box IoU and tree edit distance) and returns the result. It touches no database, queue or bucket.

Retries go back through steps 3 and 5/6 as delayed outbox messages. Given-up pages are copied to each queue's DLQ.

## Services

| Component | Runs on | Why |
|---|---|---|
| Ingest API | EC2 Auto Scaling group behind the ALB | Stateless HTTP; scales with upload traffic |
| Stream Gateway | Same ASG nodes, behind the same ALB | Long-lived SSE/WebSocket connections. Its 15 s heartbeat stays under the ALB's 60 s idle timeout |
| Eval API | Same ASG nodes, as its own container | Stateless and CPU-bound. A 50-node tree diff takes under 10 ms; requests over 1,000 nodes or the work budget get a 422 |
| Splitter | Lambda with an SQS trigger | Bursty, short and stateless. Container image (pikepdf), 1–2 GB ephemeral storage, capped concurrency |
| Outbox Publisher | ECS Fargate, 2 tasks | On the critical path; `SKIP LOCKED` makes two copies safe. Needs a persistent connection for `LISTEN` |
| Fast worker | ECS Fargate, exactly 1 task | Long-running control loop, Redis slots, token bucket; designed as one worker per stage |
| Slow worker | ECS Fargate, exactly 1 task | Same as the fast worker |
| RDS PostgreSQL | Managed | Source of truth: jobs, pages, outbox, events |
| ElastiCache Redis | Managed, cluster mode disabled | Rate limits, concurrency slots, 10 s windows, circuit breaker. The Lua scripts touch two keys at once |
| SQS | Managed | `vf-split`, `vf-fast`, `vf-slow`, each with a DLQ |
| S3 | Managed, reached through a gateway endpoint | Uploads, page PDFs, results |
| NAT gateway | Public subnet | Outbound calls to the external model APIs |
| Jenkins | Its own EC2 instance | Kept off the API nodes: it holds credentials and runs builds. It pushes images to ECR |
| Secrets Manager, ECR, CloudWatch Logs, Managed Prometheus | Managed | Connection strings, images, JSON logs and alarms, and metrics (an ADOT sidecar scrapes `:9100`) |

## Key design decisions

- **Three containers on each ASG node:** ingest, stream gateway and eval API, each with its own ALB target group on a different port. The ALB routes by path: `/jobs/*/stream` and `/ws` go to the stream gateway, `/evaluate` to the eval API, and everything else under `/jobs` to ingest.
- **Cap the eval container's CPU** (for example at half the node's vCPUs). Evaluation is pure CPU work, so without a cap a burst of large trees could slow uploads and streams on the same node. Because the ASG scales on the node's total load, heavy evaluation traffic also adds ingest capacity. If evaluation ever becomes a large share of the load, give it its own target and scaling group.
- **One task per worker, stop-before-start deploys.** Set `minimumHealthyPercent: 0` and `maximumPercent: 100`, and `stopTimeout` of about 60 s (workers drain for up to 30 s). Two overlapping workers of one stage would clear each other's Redis slots and could judge the same window twice.
- **Publisher and stream gateway connect to RDS directly**, not through a pooling proxy, because `LISTEN` needs its own dedicated session.
- **Queues are created by infrastructure code, and services look them up with `GetQueueUrl`.** `CreateQueue` fails on real SQS when the queue already exists with different settings.
- **Private subnets for all compute.** S3 is reached through a gateway endpoint; SQS, ECR, Logs and Secrets Manager through interface endpoints or the NAT.
- Every subnet exists in both AZs; the diagram draws each once.
- **Alarms:** `vf_outbox_oldest_seconds`, `vf_queue_oldest_seconds`, DLQ depth, and breaker open.
