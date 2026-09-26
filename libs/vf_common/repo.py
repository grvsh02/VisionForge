"""All SQL used by the services.

Every state change that must be followed by work in another stage inserts an outbox row
in the *same transaction* (architecture doc, section 5), so a queue message can never be
lost or sent for a change that rolled back. Every stage transition is conditional on the
page still being in the expected stage and status, which makes duplicate SQS deliveries
harmless: the second worker's update matches no row and it just drops the message.
"""

from __future__ import annotations

import uuid
from typing import Any

import asyncpg

from vf_common.envelope import build_header
from vf_common.models import TERMINAL_STATUSES, EventKind, JobStatus, PageStatus

_TERMINAL = [s.value for s in TERMINAL_STATUSES]
PROCESSING = {"fast": PageStatus.FAST_PROCESSING.value, "slow": PageStatus.SLOW_PROCESSING.value}
QUEUED = {"fast": PageStatus.FAST_QUEUED.value, "slow": PageStatus.SLOW_QUEUED.value}


# --- clients and jobs ----------------------------------------------------------------

async def ensure_client(conn: asyncpg.Connection, client_id: str) -> None:
    await conn.execute("INSERT INTO clients (client_id) VALUES ($1) ON CONFLICT DO NOTHING", client_id)


async def create_job(conn: asyncpg.Connection, *, job_id: uuid.UUID, client_id: str, content_type: str,
                     object_key: str, size_bytes: int, total_pages: int) -> None:
    """Register an uploaded document and queue it for splitting (one transaction)."""
    async with conn.transaction():
        await conn.execute(
            "INSERT INTO jobs (id, client_id, status, content_type, object_key, size_bytes, total_pages) "
            "VALUES ($1, $2, 'UPLOADED', $3, $4, $5, $6)",
            job_id, client_id, content_type, object_key, size_bytes, total_pages)
        await _outbox(conn, "split", {"job_id": str(job_id)})


async def get_job(conn: asyncpg.Connection, job_id: uuid.UUID) -> asyncpg.Record | None:
    return await conn.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)


async def job_page_counts(conn: asyncpg.Connection, job_id: uuid.UUID) -> dict[str, int]:
    rows = await conn.fetch("SELECT status, count(*) AS n FROM pages WHERE job_id = $1 GROUP BY status", job_id)
    return {r["status"]: r["n"] for r in rows}


async def job_results(conn: asyncpg.Connection, job_id: uuid.UUID) -> list[asyncpg.Record]:
    """Final page results in document order (the same payloads that were streamed)."""
    return await conn.fetch(
        "SELECT page_idx, kind, envelope->'payload' AS result FROM events "
        "WHERE job_id = $1 AND page_idx IS NOT NULL ORDER BY page_idx", job_id)


async def fetch_events(conn: asyncpg.Connection, job_id: uuid.UUID, after_seq: int,
                       limit: int) -> list[asyncpg.Record]:
    return await conn.fetch(
        "SELECT seq, kind, envelope FROM events WHERE job_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
        job_id, after_seq, limit)


async def _append_event(conn: asyncpg.Connection, job_id: uuid.UUID, kind: EventKind,
                        page_idx: int | None, payload: dict[str, Any], *, mark: str | None = None) -> int:
    """Allocate the next per-job seq (locking the job row) and append a stream event.
    Returns how many of the job's pages are finished."""
    set_status = ", status = $2, completed_at = now()" if mark else ""
    job = await conn.fetchrow(
        f"UPDATE jobs SET next_seq = next_seq + 1{set_status} WHERE id = $1 "
        "RETURNING next_seq, total_pages, client_id", *([job_id, mark] if mark else [job_id]))
    done = [r["idx"] for r in await conn.fetch(
        "SELECT idx FROM pages WHERE job_id = $1 AND status = ANY($2::text[])", job_id, _TERMINAL)]
    header = build_header(job_id=str(job_id), client_id=job["client_id"], seq=job["next_seq"],
                          page_index=page_idx, total_pages=job["total_pages"], done_pages=done)
    await conn.execute(
        "INSERT INTO events (job_id, seq, kind, page_idx, envelope) VALUES ($1, $2, $3, $4, $5)",
        job_id, job["next_seq"], kind.value, page_idx, {"header": header, "payload": payload})
    return len(done)


# --- outbox --------------------------------------------------------------------------

async def _outbox(conn: asyncpg.Connection, queue: str, payload: dict, delay_s: float = 0) -> None:
    await conn.execute("INSERT INTO outbox_events (queue, payload, delay_s) VALUES ($1, $2, $3)",
                       queue, payload, int(delay_s))


async def claim_outbox(conn: asyncpg.Connection, limit: int = 100) -> list[asyncpg.Record]:
    """Unsent outbox rows, oldest first, locked so parallel publishers skip them.
    Must run inside the transaction that later calls ``mark_outbox_sent``."""
    return await conn.fetch(
        "SELECT id, queue, payload, delay_s, created_at FROM outbox_events WHERE sent_at IS NULL "
        "ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED", limit)


async def mark_outbox_sent(conn: asyncpg.Connection, rows: list[asyncpg.Record]) -> None:
    """Mark rows sent and move their pages to <STAGE>_QUEUED (unless a worker already
    picked the message up, which can happen before this commits)."""
    await conn.execute("UPDATE outbox_events SET sent_at = now() WHERE id = ANY($1::bigint[])",
                       [r["id"] for r in rows])
    pages = [r for r in rows if r["queue"] in QUEUED]
    if pages:
        await conn.execute(
            "UPDATE pages p SET status = CASE t.queue WHEN 'fast' THEN 'FAST_QUEUED' ELSE 'SLOW_QUEUED' END, "
            "status_at = now() "
            "FROM unnest($1::uuid[], $2::int[], $3::text[]) AS t(job_id, idx, queue) "
            "WHERE p.job_id = t.job_id AND p.idx = t.idx AND p.stage = t.queue "
            "AND p.status IN ('PENDING', 'FAST_COMPLETED', 'RETRY_WAIT')",
            [uuid.UUID(r["payload"]["job_id"]) for r in pages], [r["payload"]["idx"] for r in pages],
            [r["queue"] for r in pages])


async def outbox_backlog(conn: asyncpg.Connection) -> tuple[int, float]:
    """(unsent rows, age of the oldest in seconds)."""
    row = await conn.fetchrow(
        "SELECT count(*) AS n, coalesce(extract(epoch FROM now() - min(created_at)), 0) AS age "
        "FROM outbox_events WHERE sent_at IS NULL")
    return row["n"], float(row["age"])


# --- splitting -----------------------------------------------------------------------

async def split_done(conn: asyncpg.Connection, job_id: uuid.UUID, client_id: str, page_keys: list[str]) -> bool:
    """Create the job's pages and queue each one for the fast stage (one transaction).
    Returns False if the job was already split (a duplicate split message)."""
    async with conn.transaction():
        status = await conn.fetchval("SELECT status FROM jobs WHERE id = $1 FOR UPDATE", job_id)
        if status != JobStatus.UPLOADED:
            return False
        idxs = list(range(len(page_keys)))
        await conn.execute(
            "INSERT INTO pages (job_id, idx, client_id, status, object_key) "
            "SELECT $1, t.i, $2, 'PENDING', t.k FROM unnest($3::int[], $4::text[]) AS t(i, k)",
            job_id, client_id, idxs, page_keys)
        await conn.execute(
            "INSERT INTO outbox_events (queue, payload) "
            "SELECT 'fast', jsonb_build_object('job_id', $1::text, 'idx', i) FROM unnest($2::int[]) AS i",
            str(job_id), idxs)
        await conn.execute("UPDATE jobs SET status = 'SPLIT', total_pages = $2 WHERE id = $1", job_id, len(page_keys))
        await _append_event(conn, job_id, EventKind.JOB_SPLIT, None, {"total_pages": len(page_keys)})
        return True


async def fail_job(conn: asyncpg.Connection, job_id: uuid.UUID, error: str) -> None:
    async with conn.transaction():
        await conn.execute("UPDATE jobs SET error = $2 WHERE id = $1", job_id, error)
        await _append_event(conn, job_id, EventKind.JOB_FAILED, None, {"error": error},
                            mark=JobStatus.FAILED.value)


# --- page stages ---------------------------------------------------------------------

async def start_stage(conn: asyncpg.Connection, stage: str, job_id: uuid.UUID, idx: int) -> asyncpg.Record | None:
    """Mark a page as being processed by ``stage``. None if the page is no longer in that
    stage (the message is a stale duplicate), so the caller just deletes the message."""
    return await conn.fetchrow(
        "WITH before AS (SELECT status_at FROM pages WHERE job_id = $1 AND idx = $2 FOR UPDATE) "
        "UPDATE pages SET status = $4, status_at = now() FROM before "
        "WHERE job_id = $1 AND idx = $2 AND stage = $3 AND status <> ALL($5::text[]) "
        "RETURNING pages.*, extract(epoch FROM now() - before.status_at)::float8 AS waited_s",
        job_id, idx, stage, PROCESSING[stage], _TERMINAL)


_IN_STAGE = "job_id = $1 AND idx = $2 AND stage = $3 AND status = $4"


async def fast_done(conn: asyncpg.Connection, page: asyncpg.Record, *, has_result: bool,
                    error: str | None = None) -> bool:
    """Fast stage finished (with or without a result): hand the page to the slow stage."""
    async with conn.transaction():
        res = await conn.execute(
            "UPDATE pages SET status = 'FAST_COMPLETED', stage = 'slow', has_fast_result = $5, "
            f"last_error = coalesce($6, last_error), status_at = now() WHERE {_IN_STAGE}",
            page["job_id"], page["idx"], "fast", PROCESSING["fast"], has_result, error)
        if not res.endswith(" 1"):
            return False
        await _outbox(conn, "slow", {"job_id": str(page["job_id"]), "idx": page["idx"]})
        return True


async def schedule_retry(conn: asyncpg.Connection, page: asyncpg.Record, *, stage: str, delay_s: float,
                         count_attempt: bool, error: str | None) -> bool:
    """RETRY_WAIT now, and a delayed message for the same stage (doc: RETRY_WAIT -> *_QUEUED)."""
    column = f"{stage}_attempts"
    async with conn.transaction():
        res = await conn.execute(
            f"UPDATE pages SET status = 'RETRY_WAIT', {column} = {column} + $5, last_error = $6, "
            f"status_at = now() WHERE {_IN_STAGE}",
            page["job_id"], page["idx"], stage, PROCESSING[stage], 1 if count_attempt else 0, error)
        if not res.endswith(" 1"):
            return False
        await _outbox(conn, stage, {"job_id": str(page["job_id"]), "idx": page["idx"]}, delay_s)
        return True


async def complete_page(conn: asyncpg.Connection, page: asyncpg.Record, *, result: dict, source: str,
                        low_confidence: bool, error: str | None = None) -> bool:
    """Slow stage finished: the page is COMPLETED and its result is streamed to the client."""
    kind = EventKind.PAGE_RESULT if source == "vlm" else EventKind.PAGE_FALLBACK
    return await _finish(conn, page, "slow", PageStatus.COMPLETED, kind, result, source, low_confidence, error)


async def fail_page(conn: asyncpg.Connection, page: asyncpg.Record, *, stage: str, result: dict,
                    error: str) -> bool:
    return await _finish(conn, page, stage, PageStatus.FAILED, EventKind.PAGE_FAILED, result, "none", True, error)


async def _finish(conn: asyncpg.Connection, page: asyncpg.Record, stage: str, status: PageStatus,
                  kind: EventKind, result: dict, source: str, low_confidence: bool, error: str | None) -> bool:
    async with conn.transaction():
        res = await conn.execute(
            "UPDATE pages SET status = $5, source = $6, low_confidence = $7, last_error = coalesce($8, last_error), "
            f"status_at = now(), completed_at = now() WHERE {_IN_STAGE}",
            page["job_id"], page["idx"], stage, PROCESSING[stage], status.value, source, low_confidence, error)
        if not res.endswith(" 1"):
            return False
        job_id = page["job_id"]
        done = await _append_event(conn, job_id, kind, page["idx"], result)
        total = await conn.fetchval("SELECT total_pages FROM jobs WHERE id = $1", job_id)
        if done == total:
            summary = await job_page_counts(conn, job_id)
            await _append_event(conn, job_id, EventKind.JOB_COMPLETE, None,
                                {"total_pages": total, "states": summary,
                                 "partial": summary.get(PageStatus.FAILED.value, 0) > 0},
                                mark=JobStatus.COMPLETE.value)
        return True


async def record_attempt(conn: asyncpg.Connection, *, page: asyncpg.Record, stage: str, idem_key: str,
                         outcome: str, http_status: int | None, latency_ms: int) -> None:
    await conn.execute(
        "INSERT INTO model_attempts (job_id, page_idx, stage, idem_key, outcome, http_status, latency_ms) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7)",
        page["job_id"], page["idx"], stage, idem_key, outcome, http_status, latency_ms)


# --- gauges --------------------------------------------------------------------------

async def status_counts(conn: asyncpg.Connection) -> dict[str, tuple[int, float]]:
    """Unfinished pages per status: (count, seconds the oldest has been in that status)."""
    rows = await conn.fetch(
        "SELECT status, count(*) AS n, extract(epoch FROM now() - min(status_at)) AS oldest "
        "FROM pages WHERE status <> ALL($1::text[]) GROUP BY status", _TERMINAL)
    return {r["status"]: (r["n"], float(r["oldest"])) for r in rows}
