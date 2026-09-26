"""Stage worker (``VF_STAGE=fast|slow``): consumes one SQS queue and calls one model.

For each page:
  1. Admission: the model's circuit breaker allows calls, a concurrency slot is free (the
     limit adapts and is shared through Redis) and, for the fast stage, the slow queue is
     not too deep. Only then is a message taken from SQS, so waiting work stays in SQS.
  2. The page is marked <STAGE>_PROCESSING. A stale duplicate message is simply dropped.
  3. Wait for a token from the model's shared token bucket (the rate adapts).
  4. Read the page (and for the slow stage, the fast result) from S3 and call the model
     with an idempotency key, then act on the outcome class (vf_common/errors.py):
       ok         result to S3; hand the page to the next stage / complete it
       throttled  (429) freeze the bucket until Retry-After; retry later, no attempt used
       retry      (timeout/5xx) retry with backoff + jitter (bounded attempts); a retry that
                  comes back while the retry budget is spent is re-queued without a call
       auth       (401/403) open the circuit; retry after it cools down, no attempt used
       permanent  (400/404) give up
  5. Delete the message only after the new state is committed.

Giving up: the fast stage continues to the slow model without a fast result; the slow stage
falls back to the fast result (low_confidence) or, without one, fails the page. Given-up
messages are also copied to the stage's DLQ for inspection.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
import uuid
from datetime import UTC, datetime

import asyncpg
from redis.asyncio import Redis

from services.worker import stages
from services.worker.clients import CallResult, ModelClient
from vf_common import metrics as m
from vf_common import repo
from vf_common import tracing
from vf_common.config import MODELS, Settings, get_settings
from vf_common.db import create_pool
from vf_common.errors import MAX_ATTEMPTS, Outcome, backoff_s, deferral_s
from vf_common.queues import MAX_DELIVERIES, Message, Queues
from vf_common.ratelimit import breaker as brk
from vf_common.ratelimit.breaker import CircuitBreaker
from vf_common.ratelimit.controller import Limits, fast_admission_rps
from vf_common.ratelimit.limits import ModelLimits
from vf_common.storage import Storage, result_key

log = logging.getLogger("worker")


class Worker:
    def __init__(self, settings: Settings, pool: asyncpg.Pool, redis: Redis, storage: Storage,
                 queues: Queues) -> None:
        self.model = MODELS[settings.stage]
        self.stage = self.model.stage
        self.pool, self.storage, self.queues = pool, storage, queues
        self.limits = ModelLimits(redis, self.model)
        self.breaker = CircuitBreaker(redis)
        self.client = ModelClient(settings, self.model)
        self.current = Limits.initial(self.model)  # refreshed from Redis every second
        self.admission_rps = self.model.hard_rps  # fast stage only: cap from the slow queue depth
        self.inflight: dict[str, Message] = {}  # concurrency-slot holder -> message
        self.tasks: set[asyncio.Task] = set()
        self.stopping = asyncio.Event()

    # --- loops ---------------------------------------------------------------------------
    async def run(self) -> None:
        log.info("worker starting", extra={"fields": {"stage": self.stage, "model": self.model.name}})
        await self.limits.clear_slots()  # left by a crashed predecessor, if any
        await self._control()  # load the current limits before taking work
        consume = asyncio.create_task(self._consume())
        others = [asyncio.create_task(self._every(1, self._control)),
                  asyncio.create_task(self._every(20, self._heartbeat)),
                  asyncio.create_task(self._every(5, self._gauges))]
        await self.stopping.wait()
        consume.cancel()
        if self.tasks:
            await asyncio.wait(self.tasks, timeout=30)
        for task in others:
            task.cancel()

    async def _every(self, seconds: float, fn) -> None:
        while True:
            await asyncio.sleep(seconds)
            try:
                await fn()
            except Exception:
                log.exception("%s failed", fn.__name__)

    async def _consume(self) -> None:
        while not self.stopping.is_set():
            if self.admission_rps <= 0:  # fast stage paused: the slow stage must catch up first
                await asyncio.sleep(1)
                continue
            decision = await self.breaker.allow(self.model.name)
            if not decision.allowed:
                await asyncio.sleep(min(max(decision.wait_s, 0.1), 1.0))
                continue
            probe = decision.decision == "probe"
            holder = uuid.uuid4().hex
            if not await self.limits.acquire_slot(holder, self.current.concurrency):
                if probe:
                    await self.breaker.release(self.model.name)
                await asyncio.sleep(0.05)
                continue
            try:
                msg = await self.queues.receive(self.stage, wait_s=2)
            except Exception:
                log.exception("receive failed")
                msg = None
                await asyncio.sleep(1)
            if msg is None:
                await self.limits.release_slot(holder)
                if probe:
                    await self.breaker.release(self.model.name)
                continue
            task = asyncio.create_task(self._handle(msg, holder, probe))
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)

    async def _control(self) -> None:
        """Judge the window that just ended (once per window) and pick up the limits."""
        judged = await self.limits.evaluate()
        if judged:
            verdict, limits = judged
            m.CONTROLLER_VERDICTS.labels(self.model.name, verdict).inc()
            if (limits.rps, limits.concurrency) != (self.current.rps, self.current.concurrency):
                log.info("limits changed", extra={"fields": {
                    "model": self.model.name, "verdict": verdict, "rps": limits.rps, "concurrency": limits.concurrency}})
        self.current = await self.limits.current()
        if self.stage == "fast":
            self.admission_rps = fast_admission_rps(await self.queues.depth("slow"))
            m.FAST_ADMISSION_RPS.set(self.admission_rps)
        m.ADAPTIVE_RPS.labels(self.model.name).set(self.current.rps)
        m.ADAPTIVE_CONCURRENCY.labels(self.model.name).set(self.current.concurrency)

    async def _heartbeat(self) -> None:
        """Keep in-flight messages invisible and their concurrency slots held."""
        for holder, msg in list(self.inflight.items()):
            await self.queues.extend(self.stage, msg)
            await self.limits.refresh_slot(holder)

    async def _gauges(self) -> None:
        m.sample_rss()
        depth = await self.queues.depth(self.stage)
        m.QUEUE_DEPTH.labels(self.stage).set(depth)
        m.QUEUE_DEPTH.labels(f"{self.stage}-dlq").set(await self.queues.depth(f"{self.stage}-dlq"))
        m.CONCURRENCY_IN_USE.labels(self.model.name).set(await self.limits.slots_in_use())
        m.BREAKER_OPEN.labels(self.model.name).set(await self.breaker.state(self.model.name) != "closed")
        async with self.pool.acquire() as conn:
            counts = await repo.status_counts(conn)
        m.QUEUE_OLDEST.labels(self.stage).set(counts.get(repo.QUEUED[self.stage], (0, 0.0))[1])
        if self.stage == "slow":
            m.DRAIN_TIME.set(depth / max(await self.limits.recent_success_rps(), 0.1))

    # --- one page ------------------------------------------------------------------------
    async def _handle(self, msg: Message, holder: str, probe: bool) -> None:
        # Each message runs in its own task, so these log fields stay with this page only.
        with tracing.bound(request_id=msg.body.get("request_id"), job_id=msg.body.get("job_id"),
                           page=msg.body.get("idx"), stage=self.stage):
            await self._process(msg, holder, probe)

    async def _process(self, msg: Message, holder: str, probe: bool) -> None:
        self.inflight[holder] = msg
        verdict_recorded = False
        try:
            async with self.pool.acquire() as conn:
                page = await repo.start_stage(conn, self.stage, uuid.UUID(msg.body["job_id"]), msg.body["idx"])
            if page is None:
                m.PAGES.labels(self.stage, "duplicate").inc()
            elif msg.receive_count > MAX_DELIVERIES:
                # Earlier deliveries never finished (e.g. the page keeps crashing workers).
                await self._give_up(page, f"delivered {msg.receive_count} times without finishing")
            else:
                m.QUEUE_WAIT.labels(self.stage).observe(page["waited_s"])
                verdict_recorded = await self._call_model(page)
            await self.queues.delete(self.stage, msg)
        except Exception:
            log.exception("unexpected error; the page will be retried",
                          extra={"fields": {"message": msg.body}})
            try:
                await self.queues.extend(self.stage, msg, 10)  # retry soon, not after the full timeout
            except Exception:  # noqa: BLE001 - it will reappear after the visibility timeout anyway
                pass
        finally:
            self.inflight.pop(holder, None)
            await self.limits.release_slot(holder)
            if probe and not verdict_recorded:
                await self.breaker.release(self.model.name)  # hand back the unused probe

    async def _call_model(self, page: asyncpg.Record) -> bool:
        """Call the model once and act on the outcome. Returns True if the circuit breaker
        got a verdict from this call (so a half-open probe was used)."""
        attempts = page[f"{self.stage}_attempts"]
        if attempts > 0 and not await self.limits.try_spend_retry():
            # Retry budget spent: don't send this retry now. Re-queue it near the maximum delay
            # without calling the model or using an attempt, so no page is dropped and retry
            # traffic stays capped (the budget refills as calls stop counting against it).
            await self._retry(page, deferral_s(), False, "budget_deferred", page["last_error"])
            return False
        await self._wait_for_token()
        data = await self.storage.get_bytes(page["object_key"])
        hints = await self._fast_result(page) if self.stage == "slow" else None
        idem = stages.idempotency_key(str(page["job_id"]), page["idx"], self.model.name)
        res = await self.client.predict(data, idem, hints)
        outcome = res.outcome
        verdict = await self._observe(res, is_retry=attempts > 0)
        async with self.pool.acquire() as conn:
            await repo.record_attempt(conn, page=page, stage=self.stage, idem_key=idem, outcome=outcome,
                                      http_status=res.status, latency_ms=int(res.latency_s * 1000))
        log.info("model call", extra={"fields": {"model": self.model.name, "outcome": outcome, "status": res.status,
                                                 "latency_ms": int(res.latency_s * 1000), "attempt": attempts + 1,
                                                 "replay": res.replay}})
        if outcome == Outcome.OK:
            await self._succeed(page, res.body)
        elif outcome == Outcome.THROTTLED:
            await self.limits.freeze(res.retry_after_s)  # every call waits for Retry-After
            await self._retry(page, res.retry_after_s, False, "throttled", res.error)
        elif outcome == Outcome.AUTH:
            await self.breaker.trip(self.model.name)
            await self._retry(page, brk.OPEN_S, False, "auth", res.error)
            verdict = True
        elif outcome == Outcome.RETRY:
            if attempts + 1 >= MAX_ATTEMPTS:
                await self._give_up(page, f"{MAX_ATTEMPTS} attempts failed: {res.error}")
            else:
                await self._retry(page, backoff_s(attempts + 1), True, "failure", res.error)
        elif self.stage == "fast":  # permanent: the page itself is bad
            await self._fail(page, f"bad input: {res.error}")
        else:
            await self._give_up(page, f"permanent error: {res.error}")
        return verdict

    async def _wait_for_token(self) -> None:
        while True:
            rate = min(self.current.rps, self.admission_rps)
            granted, wait = await self.limits.take_token(rate)
            if granted:
                if wait > 0:
                    await asyncio.sleep(wait)
                return
            await asyncio.sleep(min(wait, 1.0))

    async def _observe(self, res: CallResult, is_retry: bool) -> bool:
        m.MODEL_CALLS.labels(self.model.name, "replay" if res.replay else res.outcome).inc()
        if res.replay:
            return False  # served from the idempotency cache: says nothing about the model
        m.MODEL_LATENCY.labels(self.model.name).observe(res.latency_s)
        await self.limits.record(res.outcome, res.status, res.timed_out, res.latency_s, is_retry)
        if res.outcome in (Outcome.OK, Outcome.RETRY):
            await self.breaker.record(self.model.name, res.outcome == Outcome.OK)
            return True
        return False

    async def _fast_result(self, page: asyncpg.Record) -> dict | None:
        if not page["has_fast_result"]:
            return None
        return await self.storage.get_json(result_key(page["job_id"], page["idx"], "fast"))

    async def _put(self, page: asyncpg.Record, name: str, data: dict) -> None:
        start = time.perf_counter()
        await self.storage.put_json(result_key(page["job_id"], page["idx"], name), data)
        m.S3_WRITE.observe(time.perf_counter() - start)

    async def _succeed(self, page: asyncpg.Record, body: dict) -> None:
        if self.stage == "fast":
            await self._put(page, "fast", body)
            async with self.pool.acquire() as conn:
                ok = await repo.fast_done(conn, page, has_result=True)
        else:
            result = stages.vlm_result(page["idx"], body)
            await self._put(page, "slow", body)
            await self._put(page, "final", result)
            async with self.pool.acquire() as conn:
                ok = await repo.complete_page(conn, page, result=result, source="vlm", low_confidence=False)
            if ok:
                m.PAGE_LATENCY.labels("vlm").observe((datetime.now(UTC) - page["created_at"]).total_seconds())
        m.PAGES.labels(self.stage, "ok" if ok else "duplicate").inc()

    async def _retry(self, page: asyncpg.Record, delay_s: float, count_attempt: bool, reason: str,
                     error: str | None) -> None:
        async with self.pool.acquire() as conn:
            await repo.schedule_retry(conn, page, stage=self.stage, delay_s=delay_s,
                                      count_attempt=count_attempt, error=error)
        m.RETRIES.labels(self.stage, reason).inc()

    async def _give_up(self, page: asyncpg.Record, reason: str) -> None:
        if self.stage == "fast":  # continue to the slow model without a fast result
            async with self.pool.acquire() as conn:
                await repo.fast_done(conn, page, has_result=False, error=reason)
            m.PAGES.labels(self.stage, "gave_up").inc()
            return
        fast = await self._fast_result(page)
        async with self.pool.acquire() as conn:
            if fast is not None:
                result = stages.fallback_result(page["idx"], fast, reason)
                await self._put(page, "final", result)
                await repo.complete_page(conn, page, result=result, source="layout_fallback",
                                         low_confidence=True, error=reason)
                m.PAGE_LATENCY.labels("layout_fallback").observe(
                    (datetime.now(UTC) - page["created_at"]).total_seconds())
            else:
                await repo.fail_page(conn, page, stage="slow", result=stages.failed_result(page["idx"], reason),
                                     error=reason)
        m.PAGES.labels(self.stage, "fallback" if fast is not None else "failed").inc()
        await self._to_dlq(page, reason)

    async def _fail(self, page: asyncpg.Record, reason: str) -> None:
        async with self.pool.acquire() as conn:
            await repo.fail_page(conn, page, stage=self.stage, result=stages.failed_result(page["idx"], reason),
                                 error=reason)
        m.PAGES.labels(self.stage, "failed").inc()
        await self._to_dlq(page, reason)

    async def _to_dlq(self, page: asyncpg.Record, reason: str) -> None:
        try:
            await self.queues.send_batch(f"{self.stage}-dlq", [(
                {"job_id": str(page["job_id"]), "idx": page["idx"], "stage": self.stage, "reason": reason,
                 "request_id": page.get("request_id")}, 0)])
            m.DLQ_SENT.labels(self.stage).inc()
        except Exception:  # noqa: BLE001 - the page's state is already committed; the copy is for inspection
            log.exception("could not copy to DLQ")


async def main() -> None:
    settings = get_settings()
    m.setup_logging(settings.log_level)
    m.serve_metrics(settings.metrics_port)
    pool = await create_pool(max_size=20)
    redis = Redis.from_url(settings.redis_url)
    storage = await Storage(settings).start()
    queues = await Queues(settings).start()
    worker = Worker(settings, pool, redis, storage, queues)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.stopping.set)
    try:
        await worker.run()
    finally:
        await worker.client.close()
        await queues.close()
        await storage.close()
        await redis.aclose()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
