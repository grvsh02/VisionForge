"""Repository/SQL tests against a real Postgres (set VF_TEST_DATABASE_URL, e.g. the compose
postgres at postgresql://vf:vf@localhost:5432/vf). A throwaway database is created per test."""

from __future__ import annotations

import asyncio
import uuid

import asyncpg
import pytest

from tests.integration.conftest import PG_DSN, create_split_job
from vf_common import repo

pytestmark = pytest.mark.skipif(not PG_DSN, reason="set VF_TEST_DATABASE_URL to run Postgres tests")


async def outbox(conn, queue=None):
    rows = await conn.fetch("SELECT queue, payload, delay_s, sent_at FROM outbox_events ORDER BY id")
    return [r for r in rows if queue is None or r["queue"] == queue]


async def page(conn, job_id, idx=0):
    return await conn.fetchrow("SELECT * FROM pages WHERE job_id = $1 AND idx = $2", job_id, idx)


async def publish_all(conn):
    async with conn.transaction():
        rows = await repo.claim_outbox(conn)
        await repo.mark_outbox_sent(conn, rows)
    return rows


async def test_job_and_its_split_message_commit_together(pool):
    async with pool.acquire() as conn:
        await repo.ensure_client(conn, "acme")
        job_id = uuid.uuid4()
        await repo.create_job(conn, job_id=job_id, client_id="acme", content_type="application/pdf",
                              object_key="k", size_bytes=1, total_pages=3)
        assert [(r["queue"], r["payload"]) for r in await outbox(conn)] == [("split", {"job_id": str(job_id)})]
        with pytest.raises(asyncpg.UniqueViolationError):  # the job insert fails ...
            await repo.create_job(conn, job_id=job_id, client_id="acme", content_type="application/pdf",
                                  object_key="k", size_bytes=1, total_pages=3)
        assert len(await outbox(conn)) == 1  # ... so no message is published for it


async def test_split_creates_pages_and_fast_messages_once(pool):
    async with pool.acquire() as conn:
        job_id = await create_split_job(conn, "acme", 3)
        assert not await repo.split_done(conn, job_id, "acme", ["x"])  # duplicate split message
        fast = await outbox(conn, "fast")
        assert sorted(r["payload"]["idx"] for r in fast) == [0, 1, 2]
        assert {r["status"] for r in await conn.fetch("SELECT status FROM pages")} == {"PENDING"}
        assert [e["kind"] for e in await repo.fetch_events(conn, job_id, 0, 10)] == ["job_split"]


async def test_publishers_never_claim_the_same_rows(pool):
    async with pool.acquire() as conn:
        await create_split_job(conn, "acme", 10)
    a, b = await pool.acquire(), await pool.acquire()
    try:
        async with a.transaction(), b.transaction():
            rows_a = await repo.claim_outbox(a, 6)
            rows_b = await repo.claim_outbox(b, 6)  # SKIP LOCKED: gets the rest
        assert len(rows_a) == 6 and len(rows_b) == 4
        assert not {r["id"] for r in rows_a} & {r["id"] for r in rows_b}
    finally:
        await pool.release(a)
        await pool.release(b)


async def test_publishing_marks_pages_queued_unless_a_worker_already_started(pool):
    async with pool.acquire() as conn:
        job_id = await create_split_job(conn, "acme", 2)
        async with conn.transaction():
            rows = await repo.claim_outbox(conn)
            await repo.start_stage(conn, "fast", job_id, 1)  # consumed before the publisher committed
            await repo.mark_outbox_sent(conn, rows)
        assert (await page(conn, job_id, 0))["status"] == "FAST_QUEUED"
        assert (await page(conn, job_id, 1))["status"] == "FAST_PROCESSING"  # not overwritten
        assert all(r["sent_at"] is not None for r in await outbox(conn))


async def test_page_lifecycle_fast_to_slow_to_completed(pool):
    async with pool.acquire() as conn:
        job_id = await create_split_job(conn, "acme", 1)
        await publish_all(conn)
        p = await repo.start_stage(conn, "fast", job_id, 0)
        assert p["status"] == "FAST_PROCESSING" and p["waited_s"] >= 0
        assert await repo.fast_done(conn, p, has_result=True)
        assert [r["payload"] for r in await outbox(conn, "slow")] == [{"job_id": str(job_id), "idx": 0}]
        await publish_all(conn)
        assert (await page(conn, job_id))["status"] == "SLOW_QUEUED"
        p = await repo.start_stage(conn, "slow", job_id, 0)
        assert await repo.complete_page(conn, p, result={"page_index": 0}, source="vlm", low_confidence=False)
        done = await page(conn, job_id)
        assert (done["status"], done["source"], done["has_fast_result"]) == ("COMPLETED", "vlm", True)
        events = await repo.fetch_events(conn, job_id, 0, 10)
        assert [(e["kind"], e["envelope"]["header"]["watermark"]) for e in events] == [
            ("job_split", 0), ("page_result", 1), ("job_complete", 1)]
        assert (await repo.get_job(conn, job_id))["status"] == "COMPLETE"


async def test_duplicate_and_stale_messages_are_harmless(pool):
    async with pool.acquire() as conn:
        job_id = await create_split_job(conn, "acme", 1)
        first = await repo.start_stage(conn, "fast", job_id, 0)
        second = await repo.start_stage(conn, "fast", job_id, 0)  # the same message delivered twice
        assert first and second
        assert await repo.fast_done(conn, first, has_result=True)
        assert not await repo.fast_done(conn, second, has_result=True)  # second commit matches nothing
        assert len(await outbox(conn, "slow")) == 1  # the slow stage is queued once
        assert await repo.start_stage(conn, "fast", job_id, 0) is None  # stale fast message: dropped


async def test_retry_waits_in_retry_wait_with_a_delayed_message(pool):
    async with pool.acquire() as conn:
        job_id = await create_split_job(conn, "acme", 1)
        await publish_all(conn)
        p = await repo.start_stage(conn, "fast", job_id, 0)
        assert await repo.schedule_retry(conn, p, stage="fast", delay_s=4.7, count_attempt=True, error="503")
        row = await page(conn, job_id)
        assert (row["status"], row["fast_attempts"], row["last_error"]) == ("RETRY_WAIT", 1, "503")
        assert (await outbox(conn, "fast"))[-1]["delay_s"] == 4
        await publish_all(conn)
        assert (await page(conn, job_id))["status"] == "FAST_QUEUED"
        p = await repo.start_stage(conn, "fast", job_id, 0)
        assert await repo.schedule_retry(conn, p, stage="fast", delay_s=1, count_attempt=False, error="429")
        assert (await page(conn, job_id))["fast_attempts"] == 1  # a 429 doesn't use an attempt


async def test_concurrent_completions_emit_a_single_job_complete(pool):
    async with pool.acquire() as conn:
        job_id = await create_split_job(conn, "acme", 20)
        for i in range(20):
            await repo.fast_done(conn, await repo.start_stage(conn, "fast", job_id, i), has_result=False)
        pages = [await repo.start_stage(conn, "slow", job_id, i) for i in range(20)]

    async def finish(p):
        async with pool.acquire() as c:
            if p["idx"] % 2:
                return await repo.fail_page(c, p, stage="slow", result={}, error="x")
            return await repo.complete_page(c, p, result={}, source="layout_fallback", low_confidence=True)

    assert all(await asyncio.gather(*(finish(p) for p in pages)))
    async with pool.acquire() as conn:
        events = await repo.fetch_events(conn, job_id, 0, 100)
    assert [e["seq"] for e in events] == list(range(1, 23))  # split + 20 pages + complete, gap-free
    assert [e["kind"] for e in events].count("job_complete") == 1
    assert events[-1]["envelope"]["payload"]["partial"] is True


async def test_status_counts_and_outbox_backlog(pool):
    async with pool.acquire() as conn:
        await create_split_job(conn, "acme", 3)
        assert (await repo.status_counts(conn))["PENDING"][0] == 3
        assert (await repo.outbox_backlog(conn))[0] == 3
        await publish_all(conn)
        assert (await repo.status_counts(conn))["FAST_QUEUED"][0] == 3
        assert (await repo.outbox_backlog(conn))[0] == 0
