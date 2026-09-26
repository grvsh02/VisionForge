"""Outbox publisher: moves committed outbox rows to their SQS queues (architecture doc, section 5).

Loop: in one transaction, lock up to 100 unsent rows (SKIP LOCKED, so several publishers
can run), send them to SQS, then mark them sent and move their pages to <STAGE>_QUEUED.
If the process dies after sending but before committing, the rows are sent again later:
delivery is at-least-once and every consumer is idempotent. A NOTIFY from the outbox
table wakes the loop as soon as new rows commit; otherwise it checks once a second.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections import defaultdict
from datetime import UTC, datetime

import asyncpg

from vf_common import metrics as m
from vf_common import repo
from vf_common.config import get_settings
from vf_common.db import create_pool
from vf_common.queues import Queues

log = logging.getLogger("outbox")
BATCH = 100


async def publish_once(pool: asyncpg.Pool, queues: Queues) -> int:
    """Publish one batch; returns how many rows were sent."""
    async with pool.acquire() as conn, conn.transaction():
        rows = await repo.claim_outbox(conn, BATCH)
        if not rows:
            return 0
        by_queue: dict[str, list] = defaultdict(list)
        for row in rows:
            by_queue[row["queue"]].append((row["payload"], row["delay_s"]))
        for queue, messages in by_queue.items():
            await queues.send_batch(queue, messages)
            m.OUTBOX_PUBLISHED.labels(queue).inc(len(messages))
        await repo.mark_outbox_sent(conn, rows)
    now = datetime.now(UTC)
    for row in rows:
        m.OUTBOX_LAG.observe((now - row["created_at"]).total_seconds())
    return len(rows)


async def run(pool: asyncpg.Pool, queues: Queues, dsn: str, stopping: asyncio.Event) -> None:
    wake = asyncio.Event()
    listener = await asyncpg.connect(dsn)
    await listener.add_listener("outbox", lambda *_: wake.set())
    try:
        while not stopping.is_set():
            wake.clear()
            try:
                if await publish_once(pool, queues) == BATCH:
                    continue  # more waiting: go again straight away
            except Exception:
                log.exception("publish failed; retrying")
                await asyncio.sleep(1)
                continue
            async with pool.acquire() as conn:
                backlog, oldest = await repo.outbox_backlog(conn)
            m.OUTBOX_BACKLOG.set(backlog)
            m.OUTBOX_OLDEST.set(oldest)
            try:
                await asyncio.wait_for(wake.wait(), 10.0)
            except TimeoutError:
                pass
    finally:
        await listener.close()


async def main() -> None:
    settings = get_settings()
    m.setup_logging(settings.log_level)
    m.serve_metrics(settings.metrics_port)
    pool = await create_pool()
    queues = await Queues(settings).start()
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stopping.set)
    log.info("outbox publisher started")
    try:
        await run(pool, queues, settings.database_url, stopping)
    finally:
        await queues.close()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
