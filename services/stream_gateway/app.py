"""Stream gateway: replayable SSE and WebSocket result streams.

GET /jobs/{id}/stream   SSE; resume with the Last-Event-ID header (or ?last_event_id=)
GET /jobs/{id}/ws       WebSocket; client id via X-Client-ID header or ?client_id=

Each subscriber pulls from the durable ``events`` log at its own cursor, so any gateway
replica can serve any client and a reconnect resumes exactly after the last seq seen.
Every committed event is sent as soon as it is read; prioritisation between clients
happens earlier, at dispatch.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from prometheus_client import make_asgi_app

from services.stream_gateway.broker import JobSignals
from vf_common import metrics as m
from vf_common import repo
from vf_common import tracing
from vf_common.config import get_settings
from vf_common.db import create_pool
from vf_common.models import CLIENT_ID_RE, FINAL_EVENT_KINDS, JobStatus

FINAL_KINDS = {k.value for k in FINAL_EVENT_KINDS}
FETCH_LIMIT = 64
POLL_S = 2.0
HEARTBEAT_S = 15.0
settings = get_settings()


@dataclass
class Frame:
    seq: int
    kind: str
    data: dict[str, Any]

    @property
    def final(self) -> bool:
        return self.kind in FINAL_KINDS

    def sse(self) -> str:
        return f"id: {self.seq}\nevent: {self.kind}\ndata: {json.dumps(self.data, separators=(',', ':'))}\n\n"


def to_frame(row: dict[str, Any]) -> Frame:
    return Frame(row["seq"], row["kind"], row["envelope"])


@asynccontextmanager
async def lifespan(app: FastAPI):
    m.setup_logging(settings.log_level)
    app.state.pool = await create_pool(max_size=20)
    app.state.signals = JobSignals(settings.database_url)
    await app.state.signals.start()
    yield
    await app.state.signals.stop()
    await app.state.pool.close()


app = FastAPI(title="visionforge-stream", lifespan=lifespan)
app.add_middleware(tracing.RequestIdMiddleware)
app.mount("/metrics", make_asgi_app())


async def _authorize(app_: FastAPI, job_id: uuid.UUID, client_id: str | None) -> None:
    if not client_id or not CLIENT_ID_RE.match(client_id):
        raise HTTPException(400, "X-Client-ID is required")
    async with app_.state.pool.acquire() as conn:
        job = await repo.get_job(conn, job_id)
    if job is None or job["client_id"] != client_id:
        raise HTTPException(404, "job not found")


async def frames_for(app_: FastAPI, job_id: uuid.UUID, after_seq: int) -> AsyncIterator[Frame | None]:
    """Yield frames from ``after_seq`` onwards; ``None`` means "send a heartbeat"."""
    signals: JobSignals = app_.state.signals
    wake = signals.subscribe(str(job_id))
    cursor = after_seq
    idle = 0.0
    m.STREAM_SUBSCRIBERS.inc()
    try:
        while True:
            wake.clear()  # clear before reading so a concurrent notify is never lost
            async with app_.state.pool.acquire() as conn:
                rows = [dict(r) for r in await repo.fetch_events(conn, job_id, cursor, FETCH_LIMIT)]
                if not rows:
                    status = await conn.fetchval("SELECT status FROM jobs WHERE id = $1", job_id)
            if rows:
                idle = 0.0
                for frame in map(to_frame, rows):
                    yield frame
                    m.STREAM_FRAMES.labels(frame.kind).inc()
                    cursor = frame.seq
                    if frame.final:
                        return
                continue
            if status in (JobStatus.COMPLETE, JobStatus.FAILED):
                return  # resumed after the final event: nothing left to send
            try:
                await asyncio.wait_for(wake.wait(), POLL_S)
            except TimeoutError:
                idle += POLL_S
                if idle >= HEARTBEAT_S:
                    idle = 0.0
                    yield None
    finally:
        signals.unsubscribe(str(job_id), wake)
        m.STREAM_SUBSCRIBERS.dec()


def _resume_seq(header_value: str | None, query_value: int | None) -> int:
    if header_value and header_value.strip().isdigit():
        return int(header_value.strip())
    return query_value or 0


@app.get("/jobs/{job_id}/stream")
async def stream(job_id: uuid.UUID, request: Request, x_client_id: str | None = Header(None),
                 last_event_id: str | None = Header(None), last_event_id_q: int | None = Query(None, alias="last_event_id")):
    await _authorize(request.app, job_id, x_client_id)
    after = _resume_seq(last_event_id, last_event_id_q)

    async def body() -> AsyncIterator[str]:
        yield "retry: 1000\n\n"
        async for frame in frames_for(request.app, job_id, after):
            if await request.is_disconnected():
                return
            yield ": ping\n\n" if frame is None else frame.sse()

    return StreamingResponse(body(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.websocket("/jobs/{job_id}/ws")
async def ws_stream(websocket: WebSocket, job_id: uuid.UUID, client_id: str | None = Query(None),
                    last_event_id: int | None = Query(None)):
    cid = websocket.headers.get("x-client-id") or client_id
    try:
        await _authorize(websocket.app, job_id, cid)
    except HTTPException as exc:
        await websocket.close(code=4400 if exc.status_code == 400 else 4404, reason=exc.detail)
        return
    await websocket.accept()
    try:
        async for frame in frames_for(websocket.app, job_id, last_event_id or 0):
            if frame is None:
                await websocket.send_json({"event": "ping"})
            else:
                await websocket.send_json({"id": frame.seq, "event": frame.kind, "data": frame.data})
        await websocket.close()
    except WebSocketDisconnect:
        pass


@app.get("/healthz")
async def healthz(request: Request):
    return {"ok": True, "subscribers": request.app.state.signals.subscriber_count}
