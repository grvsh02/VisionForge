# VisionForge

A high-throughput OCR pipeline orchestrator. It ingests multi-page documents (up to 100 pages / 100 MB) from
many customers and runs each page through two model endpoints with very different capacities:

- a layout model: 50 ms, 100 RPS
- a VLM: 1.5–3 s, 10 RPS

How it works, in short:
- **Postgres** is the source of truth.
- A **transactional outbox** feeds **SQS** queues. **Workers** adapt each model's rate and concurrency to 429s, latency and errors.
- The depth of the VLM queue throttles the layout stage.
- Page results are streamed out of order with sequencing headers, so clients can reassemble documents deterministically.
- Memory stays bounded, and the pipeline survives SIGKILL without re-running finished pages or duplicating model calls.

## Architecture

**Request flow** ([interactive diagram](docs/diagrams/request-flow.html), with a guided view per route: upload, page processing, retries and failures, streaming, reads and evaluation):

![VisionForge request flow](docs/diagrams/request-flow.visual-check.1440x900.light.png)

**AWS deployment** ([draw.io source](docs/diagrams/aws-architecture.drawio), [guide](docs/diagrams/aws-architecture.md)):

![VisionForge on AWS](docs/diagrams/aws-architecture.drawio.png)

The design in depth is in [architecture.md](architecture.md): queue and state machine design, backpressure strategy and memory bounds, tree edit distance complexity, and known trade-offs with the scale-out path.

## Quick start

Requires Docker (compose v2) and [uv](https://docs.astral.sh/uv/).

```bash
make fast-up          # build the image, start the stack; mock models run 10x faster (same rate limits)
```

```bash
uv run python scripts/gen_fixtures.py
```

```bash
uv run python client/cli.py --client-id acme submit fixtures/doc20.pdf --stream --out out.json
```

To upload a document and log each result as it arrives over the WebSocket (the script reconnects and resumes on drops):

```bash
uv run python scripts/ws_upload.py fixtures/doc20.pdf --client-id acme
```

Everything received is saved under `results/<job_id>/` (change it with `--out-dir`):
- `frames.jsonl`: every frame exactly as received, with an arrival timestamp
- `pages/page_NNNN.json`: one file per page result
- `document.json`: all pages in document order
- `summary.json`: status, arrival order, missing pages, reconnects

Interrupted runs (Ctrl-C or `kill`) still leave consistent files, with `status: "interrupted"`.

- Edge: `http://localhost:8080`
- Grafana: `http://localhost:3000` (the "VisionForge pipeline" dashboard)
- Prometheus: `:9090`
- S3 API (SeaweedFS): `:8333`
- SQS API (ElasticMQ): `:9324`, with a queue UI on `:9325`

Use `make up` for real mock latencies and `make down` to stop the stack and delete its volumes.

## Services

| Service | Role |
|---|---|
| `edge` | nginx. Rejects requests with no `X-Client-ID`, rate-limits uploads and caps connections per IP and per client, streams uploads through without buffering, and never buffers SSE. |
| `ingest-api` | `POST /jobs` streams the body to S3 (5 MB multipart parts) and to a temp file, then counts the pages. Unreadable documents and documents over 100 pages are rejected. In one transaction it inserts the job and an outbox row for the `split` queue. Also serves `GET /jobs/{id}` and `GET /jobs/{id}/result`. |
| `outbox-publisher` | Claims unsent outbox rows (`FOR UPDATE SKIP LOCKED`, so several can run), sends them to SQS and marks them sent. It is woken by `NOTIFY outbox` and also polls every 10 s. |
| `splitter` | Consumes `split`. Spools the upload to disk, writes one single-page PDF per page to S3 (multi-page TIFFs frame by frame), then in one transaction inserts the pages and their outbox rows for the layout queue. |
| `fast-worker` (layout), `slow-worker` (VLM) | The layout worker and the VLM worker: one image, with `VF_STAGE=fast` or `slow` picking the stage, and one instance per stage. Each consumes its queue, calls its model under the adaptive limits, writes results to S3 and commits the next state (see below). |
| `stream-gateway` | SSE and WebSocket. Each subscriber pulls from the durable event log at its own cursor and pace. Stateless. |
| `eval-api` | `POST /evaluate`: scores an extracted tree against a ground-truth tree (CER/WER, box IoU, tree edit distance). Stateless and CPU-bound, so it runs in its own container (on AWS it shares the ingest nodes with a CPU cap). |
| `mock-layout`, `mock-vlm` | Mock models with the required latency, rate limit (429 + `Retry-After`) and failure rate, plus idempotency. Latency rises when they're overloaded. `/admin/chaos` changes the rate limit, capacity, latency, failure rate and error status at runtime. |
| `postgres`, `redis`, `s3`, `sqs`, `prometheus`, `grafana` | Infrastructure. |

## API

```
POST /jobs                  X-Client-ID, Content-Type: application/pdf|image/tiff|image/png|image/jpeg
                            → 202 {job_id, total_pages, stream_url}
                            → 422 unreadable or > 100 pages | 413 > 100 MB | 429 edge rate limit
GET  /jobs/{id}             status + page-status counts (only the owning client)
GET  /jobs/{id}/result      pages in document order
GET  /jobs/{id}/stream      SSE; resume with Last-Event-ID
GET  /jobs/{id}/ws          WebSocket (?client_id=&last_event_id=)
POST /evaluate              {predicted: DocNode, ground_truth: DocNode, iou_threshold?: 0.5}
                            → 200 {text, bbox, tree, timings_ms} | 422 invalid, > 1000 nodes or too complex
```

Stream frames:

```
id: 17
event: page_result          # job_split | page_result | page_fallback | page_failed | job_complete | job_failed
data: {"header": {"job_id", "client_id", "seq": 17, "page_index": 10, "total_pages": 100,
                  "watermark": 2,                     # every page < 2 has been emitted
                  "window": {"base": 2, "size": 64, "bitmap": "<base64>"},  # emitted pages in [base, base+size)
                  "emitted_at"},
       "payload": {page_index, source, confidence, low_confidence, tree: DocNode, text, tables_markdown, key_values}}
```

`seq` is per job and gap-free. It is allocated in the same transaction that completes the page, under a lock on
the job row. The reference client (`client/cli.py`) keeps a reorder buffer, flushes the contiguous prefix,
reconnects with `Last-Event-ID` when it sees a gap in `seq`, and ignores duplicates.

## How it works

### Source of truth and the outbox

Every state change is one Postgres transaction. If that change creates work, the same transaction also inserts an
`outbox_events` row, so there is never a committed state with a lost message, or a message for a state that
was rolled back. The publisher sends outbox rows to SQS (`DelaySeconds` = the row's `delay_s`) and marks them sent.
If it crashes between sending and marking, the rows are sent again. Delivery is at least once, and consumers make
duplicates harmless.

Page lifecycle (`pages.status`, `pages.stage`; `FAST_*` is the layout stage and `SLOW_*` the VLM stage):

```
PENDING ─► FAST_QUEUED ─► FAST_PROCESSING ─► FAST_COMPLETED ─► SLOW_QUEUED ─► SLOW_PROCESSING ─► COMPLETED
                ▲               │                                   ▲               │
                └─ RETRY_WAIT ◄─┘  (layout stage)                   └─ RETRY_WAIT ◄─┘  (VLM stage)
                                                                    any stage ─► FAILED
```

Every transition is conditional on the expected `stage` and `status`. A duplicate or stale message therefore
changes zero rows and is dropped. Only one of two workers holding the same message can commit.

### Queues

There are three SQS queues, `vf-split`, `vf-fast` (layout) and `vf-slow` (VLM), and each has a DLQ.
- **Message lifetime:** a message is deleted only after the state it produced is committed.
- **Visibility:** received messages stay invisible for 60 s, and a heartbeat extends that every 20 s while the work continues. If a worker is SIGKILLed, its messages reappear within 60 s and another worker takes them.
- **Poison messages:** a message delivered more than 3 times (it keeps crashing its consumers) is given up by the worker. As a backstop, SQS moves anything received 5 times to the DLQ.
- **Given-up pages:** workers also copy every page they give up on to the stage's DLQ for inspection.

A worker only receives a message once it holds a concurrency slot (and, for the layout worker, while admission is not
paused), so waiting work stays in SQS, not in worker memory.

### Calling a model

The worker classifies each call by its result ([errors.py](libs/vf_common/errors.py)):

| Result | Class | Action |
|---|---|---|
| 2xx | ok | write `results/{job}/pages/{idx:04d}/{fast,slow,final}.json` (layout, VLM and final result) to S3, commit the next state |
| 429 | throttled | freeze the shared token bucket until `Retry-After`; retry after it **without using an attempt** |
| timeout, 408, 5xx, connection error | retry | retry with full-jitter backoff (`uniform(0, min(60, 1·2^attempt))` s), 3 attempts |
| 401, 403 | auth | trip the circuit breaker; retry after it cools down, without using an attempt |
| 400, 404, other 4xx | permanent | give up |

- **Retries:** a retry is a `RETRY_WAIT` state plus a delayed outbox message, so it survives crashes and holds no slot while it waits.
- **Giving up:**
  - The layout worker sends the page on to the VLM without layout hints. A permanent error from the layout model (bad input) fails the page.
  - The VLM worker falls back to the layout result (`COMPLETED`, `source: layout_fallback`, `low_confidence: true`, confidence < 0.5), or fails the page if there is no layout result.
- **Idempotency:** each call carries `Idempotency-Key = sha256(job:page:model:version)`. The mock caches 2xx responses per key and joins concurrent duplicates, so a call that was in flight when a worker was SIGKILLed replays instead of running inference again. Results go to deterministic S3 paths, so rewriting them is harmless.

### Adaptive limits

Each model has two limits: a **rate** (a token bucket that spaces calls 1/rate apart, with no bursts) and a **concurrency** (slots; a restarted worker
clears the ones its dead predecessor left). Each stage runs a single worker, so nothing needs a lock. The state is kept in Redis only so
that it survives a worker restart: a crash during an overload doesn't reset the limits. [controller.py](libs/vf_common/ratelimit/controller.py) holds the rules as pure functions.

| Model | Start | Maximum (hard limit) |
|---|---|---|
| Layout model | 80 RPS, 5 concurrent | 100 RPS, 10 concurrent |
| VLM | 8 RPS, 20 concurrent | 10 RPS, 30 concurrent |

The worker records every call's outcome in 10 s windows. Its control loop runs every second, and the first run after a window ends judges it. `ctl:<stage>` stores the id of the last judged window, so a window is never judged twice, even across a restart:

- **Overload** is any of:
  - p95 latency > 2× nominal (layout 50 ms, VLM 3 s)
  - ≥ 1% 429s
  - ≥ 5% timeouts
  - 5xx rate > 2× the model's normal error rate

  Windows with fewer than 10 calls count as overloaded on any 429.
- **Hysteresis:**
  - Two overloaded windows in a row halve both limits, and each further overloaded window halves them again.
  - Three healthy windows in a row start raising them by 10% of the hard limit per window.
  - Limits are clamped between 1 RPS / 1 concurrent and the hard limit.

  In practice a storm is answered within about 20 s, and a VLM at the 1 RPS floor is back at 10 RPS about 2 minutes after it recovers (30 s of healthy windows, then +1 RPS per 10 s).

**Retry budget.** Retries may be at most 10% of first attempts over the last two windows, with a minimum of 5. The
budget is checked atomically when a retry is about to call the model. If it is spent, the retry is put back in the
queue at 30–60 s (jittered, so deferred retries do not return together) without calling the model or using an attempt. Pages are deferred, never dropped,
and retries can't pile onto a struggling model. When there is little traffic, such as the tail of a batch, only the
minimum applies: failed pages then finish a few per 20 s, and the rest wait a minute or two.

**Circuit breaker.** One breaker per model. It opens at a 50% failure rate over the last
20 calls (or immediately on 401/403), stays open 10 s, then allows 3 half-open probes. While it is open, the worker
stops taking messages, so pages wait in SQS.

**Queue-level backpressure.** The layout worker's rate is also capped by the VLM queue's depth:

| VLM queue depth | < 100 | 100–499 | 500–999 | ≥ 1000 |
|---|---|---|---|---|
| Layout rate cap | 100 RPS | 50 RPS | 20 RPS | paused |

Pressure on the VLM then travels backwards to the layout stage instead of piling up between the two stages.

**DoS protection at the edge.** nginx limits uploads (`POST /jobs`) to 10/s with bursts of 20 and 10 concurrent, and
result streams (SSE/WebSocket) to 50 concurrent. Each limit applies per client IP and per `X-Client-ID`. Excess
uploads get `429` with `Retry-After`; excess connections get `503`.

### Memory bounds

- **Uploads:** streamed to S3 object storage in 5 MB parts. Up to 3 parts per upload go to S3 while the next is received, with at most 4 in flight across the process. That brings a 100 MB upload from about 2.3 s to 0.7 s. Eight simultaneous 100 MB uploads peak at 195 MiB of the ingest container's 256 MiB.
- **Splitter:** qpdf caches every stream it reads for the lifetime of an open document; measured, one open grew RSS by 103 MiB on a 98 MiB PDF. The splitter reopens the source every 8 pages, which kept peak RSS at 12 MiB, and a unit test guards this.
- **Workers:** hold at most their concurrency limit of pages; the backlog stays in SQS.
- **Stream subscribers:** they hold no buffers. A NOTIFY only wakes them, and they read the log at their own cursor.
- **Container limits:** every container has a `mem_limit`. Two limits needed care in practice:
  - **TCP socket buffers.** These are charged to a container's memory cgroup. On a fast link, 4 concurrent 100 MB uploads autotuned nginx's buffers to 117 MiB and OOM-killed the edge. The edge and ingest containers cap `tcp_rmem`/`tcp_wmem` at 1 MiB, which measured 22 MiB peak with unchanged upload speed.
  - **SeaweedFS.** It is a Go program and ignores the cgroup limit until it is OOM-killed. `GOMEMLIMIT=700MiB` keeps its heap at about 560 MiB under the same load.

### Observability

Every service exposes Prometheus metrics. The main ones:
- **Model calls and limits:**
  - `vf_model_calls_total{model,outcome}` and `vf_model_latency_seconds`
  - `vf_adaptive_rps`, `vf_adaptive_concurrency`, `vf_concurrency_in_use`
  - `vf_controller_verdicts_total{verdict}`, `vf_breaker_open`
- **Queues and backpressure:**
  - `vf_queue_depth{queue}` (DLQs included), `vf_queue_oldest_seconds{stage}`, `vf_queue_wait_seconds{stage}`
  - `vf_slow_drain_time_seconds`, `vf_fast_admission_rps`
- **Outbox:** `vf_outbox_lag_seconds`, `vf_outbox_backlog`, `vf_outbox_oldest_seconds`, `vf_outbox_published_total`
- **Outcomes:**
  - `vf_pages_total{stage,outcome}`, `vf_retries_total{stage,reason}`, `vf_dlq_sent_total`
  - `vf_page_end_to_end_seconds{source}`
- **Memory:** `vf_process_rss_bytes`

### Evaluation (`POST /evaluate`)

Both trees use the pipeline's output schema (`DocNode`): pass a page result's `tree`, or a `document` root whose children are `page` nodes. Children are read in `order`. The code is in [evaluation.py](libs/vf_common/evaluation.py).

- **CER / WER:** Levenshtein distance between the reading-order text streams (whitespace collapsed, case kept), divided by the ground truth's characters or words. The implementation is bit-parallel (Myers / Hyyrö), so a 6,000-character page takes about 18 ms.
- **IoU:** each predicted box is matched to at most one ground-truth box on the same page, greedily by highest IoU. The response has `mean_iou` (missed ground-truth boxes count as 0), `matched_mean_iou`, precision / recall / F1 at `iou_threshold`, and `type_accuracy` of matched boxes.
- **Tree edit distance:** Zhang–Shasha on node types, where insert, delete and relabel each cost 1. It also reports `ted_normalized`, which is TED divided by the larger tree's node count.
  - **Memory:** two n₁×n₂ tables, allocated once, with trees capped at 1,000 nodes.
  - **Time:** the algorithm's work is known before it runs. Both trees are mirrored when that's cheaper (a simplified APTED path choice), and anything over the work budget gets a 422 instead of hanging.
  - **Measured:** a 50-node tree takes 0.4–9.5 ms depending on shape, against the 100 ms target. Wide 1,000-node trees take up to about 1.6 s. Deep zigzag trees, the algorithm's worst case, are refused in about 2 ms.

## Testing

```bash
make test          # unit: controller rules, error table, Redis limits/budget/breaker (fakeredis), splitter, mocks
```

```bash
make integration   # compose postgres + ElasticMQ: repo/outbox SQL, the real publisher, workers and splitter end to end
```

```bash
make e2e           # against the running stack: many clients, fallback, backpressure, resume, auth
```

```bash
make chaos         # SIGKILL workers/splitter/publisher during 5 x 100-page jobs; verifies exactly-once
```

```bash
make backpressure  # 429-storm and overload the mock VLM; watch limits halve and recover, and layout admission drop
```

```bash
make load          # sustained load with docker-stats memory sampling
```

```bash
make k6-ingest     # k6: 50 concurrent jobs x 20 pages through the edge; throughput + latency percentiles
```

`make integration` works with any Postgres where the user can create databases and any SQS endpoint:
`VF_TEST_DATABASE_URL=... VF_TEST_SQS_URL=... uv run pytest tests/integration -m "not e2e"`. For the full memory run
from the plan, use `uv run python scripts/loadtest.py --jobs 200 --pages 100 --page-kb 1000`. It takes about
35 minutes at roughly 10 pages/s, because the VLM is the bottleneck.

## Deliberately out of scope for v1

- Priority queues and per-customer fairness. All pages share one queue per stage, first in, first out. The planned next step, a 70/30 split favouring small documents, is in [architecture.md](architecture.md) §11.
- Authentication. `X-Client-ID` is trusted as sent; in production the edge would derive it from an API key.
