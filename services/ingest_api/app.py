"""Ingest API: accepts documents per client and streams them into object storage.

POST /jobs              body = raw PDF/TIFF/PNG/JPEG bytes, header X-Client-ID (required)
GET  /jobs/{id}         status and page-state counts (scoped to the caller)
GET  /jobs/{id}/result  pages in document order (scoped to the caller)

The body is never buffered in memory: it is streamed to S3 in 5MB multipart parts and, at
the same time, to a temp file on disk. Once the upload finishes, its pages are counted from
that file (PDFs keep their page index at the end, so the whole file is needed) and stored
on the job. Unreadable documents and documents over ``max_pages`` are rejected with 422.

Every valid upload is accepted: the job row and its "split this" outbox event commit in one
transaction, and the outbox publisher puts it on the split queue. Abuse protection (request
rate and concurrent connections per IP and per X-Client-ID) lives in the nginx edge.
"""

from __future__ import annotations

import asyncio
import tempfile
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Request
from prometheus_client import make_asgi_app

from vf_common import metrics as m
from vf_common import repo
from vf_common.config import get_settings
from vf_common.db import create_pool
from vf_common.documents import InvalidDocument, count_pages
from vf_common.models import CLIENT_ID_RE
from vf_common.storage import Storage, UploadTooLarge

ACCEPTED_TYPES = {"application/pdf", "image/tiff", "image/png", "image/jpeg"}
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    m.setup_logging(settings.log_level)
    app.state.pool = await create_pool(max_size=20)
    app.state.storage = await Storage(settings).start()
    await app.state.storage.ensure_bucket()
    yield
    await app.state.storage.close()
    await app.state.pool.close()


app = FastAPI(title="visionforge-ingest", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


def require_client(client_id: str | None) -> str:
    if not client_id or not CLIENT_ID_RE.match(client_id):
        raise HTTPException(400, "X-Client-ID header is required ([A-Za-z0-9._-]{1,64})")
    return client_id


@app.post("/jobs", status_code=202)
async def submit_job(request: Request, x_client_id: str | None = Header(None),
                     content_type: str | None = Header(None),
                     content_length: int | None = Header(None)):
    client_id = require_client(x_client_id)
    ctype = (content_type or "").split(";")[0].strip().lower()
    if ctype not in ACCEPTED_TYPES:
        m.UPLOADS.labels("bad_type").inc()
        raise HTTPException(415, f"Content-Type must be one of {sorted(ACCEPTED_TYPES)}")
    if content_length is not None and content_length > settings.max_upload_bytes:
        m.UPLOADS.labels("too_large").inc()
        raise HTTPException(413, f"upload exceeds {settings.max_upload_bytes} bytes")

    pool, storage = request.app.state.pool, request.app.state.storage
    async with pool.acquire() as conn:
        await repo.ensure_client(conn, client_id)

    job_id = uuid.uuid4()
    object_key = f"uploads/{client_id}/{job_id}"
    with tempfile.TemporaryDirectory(prefix="vf-ingest-") as tmp:
        local = Path(tmp) / "upload"
        try:
            with local.open("wb") as fh:
                size = await storage.upload_stream(object_key, request.stream(), content_type=ctype,
                                                   max_bytes=settings.max_upload_bytes, tee=fh)
        except UploadTooLarge as exc:
            m.UPLOADS.labels("too_large").inc()
            raise HTTPException(413, str(exc)) from exc
        if size == 0:
            await storage.delete(object_key)
            raise HTTPException(400, "empty body")
        try:
            # Reads only the PDF's page index (or TIFF frame directory); off the event loop.
            pages = await asyncio.to_thread(count_pages, local, ctype, settings.max_pages)
        except InvalidDocument as exc:
            await storage.delete(object_key)
            m.UPLOADS.labels("invalid_document").inc()
            raise HTTPException(422, str(exc)) from exc

    async with pool.acquire() as conn:
        await repo.create_job(conn, job_id=job_id, client_id=client_id, content_type=ctype,
                              object_key=object_key, size_bytes=size, total_pages=pages)
    m.UPLOADS.labels("accepted").inc()
    return {"job_id": str(job_id), "status": "UPLOADED", "size_bytes": size, "total_pages": pages,
            "stream_url": f"/jobs/{job_id}/stream"}


async def _owned_job(request: Request, job_id: uuid.UUID, client_id: str):
    async with request.app.state.pool.acquire() as conn:
        job = await repo.get_job(conn, job_id)
    if job is None or job["client_id"] != client_id:
        raise HTTPException(404, "job not found")
    return job


@app.get("/jobs/{job_id}")
async def job_status(job_id: uuid.UUID, request: Request, x_client_id: str | None = Header(None)):
    client_id = require_client(x_client_id)
    job = await _owned_job(request, job_id, client_id)
    async with request.app.state.pool.acquire() as conn:
        counts = await repo.job_page_counts(conn, job_id)
    return {"job_id": str(job_id), "status": job["status"], "total_pages": job["total_pages"],
            "pages": counts, "error": job["error"], "created_at": job["created_at"].isoformat(),
            "completed_at": job["completed_at"].isoformat() if job["completed_at"] else None}


@app.get("/jobs/{job_id}/result")
async def job_result(job_id: uuid.UUID, request: Request, x_client_id: str | None = Header(None)):
    client_id = require_client(x_client_id)
    job = await _owned_job(request, job_id, client_id)
    async with request.app.state.pool.acquire() as conn:
        rows = await repo.job_results(conn, job_id)
    return {"job_id": str(job_id), "status": job["status"], "total_pages": job["total_pages"],
            "pages": [{"page_index": r["idx"], "state": r["state"], "source": r["source"],
                       "low_confidence": r["low_confidence"], "result": r["final_result"]} for r in rows]}


@app.get("/healthz")
async def healthz():
    return {"ok": True}
