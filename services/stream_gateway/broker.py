"""Wake-up fan-out for stream subscribers.

One dedicated connection per gateway process LISTENs on ``job_events``. A notification
only *wakes* the subscribers of that job; each subscriber then pulls events from the
durable log at its own cursor and pace. Nothing is buffered per subscriber, so a slow
client costs no memory, and a missed notification (e.g. during a LISTEN reconnect) only
delays delivery until the subscriber's periodic poll.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict

import asyncpg

log = logging.getLogger("stream.broker")


class JobSignals:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._subs: dict[str, set[asyncio.Event]] = defaultdict(set)
        self._conn: asyncpg.Connection | None = None
        self._task: asyncio.Task | None = None

    def subscribe(self, job_id: str) -> asyncio.Event:
        ev = asyncio.Event()
        self._subs[job_id].add(ev)
        return ev

    def unsubscribe(self, job_id: str, ev: asyncio.Event) -> None:
        subs = self._subs.get(job_id)
        if subs is not None:
            subs.discard(ev)
            if not subs:
                del self._subs[job_id]

    @property
    def subscriber_count(self) -> int:
        return sum(len(s) for s in self._subs.values())

    def _on_notify(self, _conn, _pid, _channel, payload: str) -> None:
        job_id = payload.split(":", 1)[0]
        for ev in self._subs.get(job_id, ()):
            ev.set()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._supervise())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        if self._conn and not self._conn.is_closed():
            await self._conn.close()

    async def _supervise(self) -> None:
        while True:
            try:
                if self._conn is None or self._conn.is_closed():
                    self._conn = await asyncpg.connect(self.dsn)
                    await self._conn.add_listener("job_events", self._on_notify)
                    log.info("listening on job_events")
                    for subs in self._subs.values():  # catch up anything missed while down
                        for ev in subs:
                            ev.set()
            except Exception:
                log.exception("LISTEN connection failed; retrying")
                self._conn = None
            await asyncio.sleep(2)
