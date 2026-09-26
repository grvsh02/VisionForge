"""Splitter: turns an uploaded document into per-page objects and page rows.

Memory stays bounded: the source object is streamed to ephemeral disk (PDFs keep
their xref at the end, so random access is required), then pikepdf -- which loads
objects lazily from the file -- copies one page at a time into its own single-page
PDF. Multi-page TIFFs are walked frame by frame with PIL ``seek``.

Work arrives on the split queue. The pages, their fast-queue outbox rows and the job's
SPLIT status commit in one transaction, and the message is deleted only afterwards: if the
splitter dies mid-split, the message reappears and the split is simply redone.
"""

from __future__ import annotations

import asyncio
import io
import logging
import signal
import tempfile
import uuid
from collections.abc import Iterator
from pathlib import Path

import pikepdf
from PIL import Image, ImageSequence, UnidentifiedImageError

from vf_common import metrics as m
from vf_common import repo
from vf_common.config import get_settings
from vf_common.documents import InvalidDocument, count_pages
from vf_common.db import create_pool
from vf_common.models import JobStatus
from vf_common.queues import MAX_DELIVERIES, Message, Queues
from vf_common.storage import Storage

log = logging.getLogger("splitter")

DOCUMENT_ERRORS = (InvalidDocument, pikepdf.PdfError, UnidentifiedImageError)

PAGES_PER_OPEN = 8


def iter_pdf_pages(path: Path, max_pages: int) -> Iterator[bytes]:
    count = count_pages(path, "application/pdf", max_pages)
    for start in range(0, count, PAGES_PER_OPEN):
        with pikepdf.open(path) as src:
            for i in range(start, min(start + PAGES_PER_OPEN, count)):
                with pikepdf.new() as dst:
                    dst.pages.append(src.pages[i])
                    buf = io.BytesIO()
                    dst.save(buf)
                yield buf.getvalue()


def iter_image_pages(path: Path, max_pages: int) -> Iterator[bytes]:
    count_pages(path, "image/tiff", max_pages)
    with Image.open(path) as img:
        for frame in ImageSequence.Iterator(img):
            buf = io.BytesIO()
            frame.convert("RGB").save(buf, format="PDF")
            yield buf.getvalue()


def page_iterator(path: Path, content_type: str, max_pages: int) -> Iterator[bytes]:
    if content_type == "application/pdf":
        return iter_pdf_pages(path, max_pages)
    return iter_image_pages(path, max_pages)


class Splitter:
    """Consumes the split queue: one uploaded document per message."""

    def __init__(self, settings, pool, storage: Storage, queues: Queues) -> None:
        self.s = settings
        self.pool = pool
        self.storage = storage
        self.queues = queues
        self.stopping = asyncio.Event()

    async def run(self) -> None:
        while not self.stopping.is_set():
            try:
                msg = await self.queues.receive("split", wait_s=5)
            except Exception:
                log.exception("receive failed")
                await asyncio.sleep(1)
                continue
            if msg is not None:
                await self._handle(msg)
            m.sample_rss()

    async def _handle(self, msg: Message) -> None:
        job_id = uuid.UUID(msg.body["job_id"])
        async with self.pool.acquire() as conn:
            job = await repo.get_job(conn, job_id)
        if job is None or job["status"] != JobStatus.UPLOADED:  # already split: a duplicate
            await self.queues.delete("split", msg)
            return
        if msg.receive_count > MAX_DELIVERIES:
            # Earlier deliveries never finished (e.g. this document keeps crashing splitters).
            async with self.pool.acquire() as conn:
                await repo.fail_job(conn, job_id, f"split abandoned after {msg.receive_count - 1} attempts")
            await self.queues.delete("split", msg)
            return
        keep_invisible = asyncio.create_task(self._extend_while_working(msg))
        try:
            keys = await self._split(job)
            async with self.pool.acquire() as conn:
                if await repo.split_done(conn, job_id, job["client_id"], keys):
                    log.info("job split", extra={"fields": {"job_id": str(job_id), "pages": len(keys)}})
            await self.queues.delete("split", msg)
        except DOCUMENT_ERRORS as exc:
            log.warning("document rejected", extra={"fields": {"job_id": str(job_id), "error": str(exc)}})
            async with self.pool.acquire() as conn:
                await repo.fail_job(conn, job_id, f"split failed: {exc}")
            await self.queues.delete("split", msg)
        except Exception:  # infrastructure (object store, database): retry with backoff
            log.exception("split attempt failed", extra={"fields": {"job_id": str(job_id),
                                                                    "attempt": msg.receive_count}})
            await self.queues.extend("split", msg, 5 * 2 ** msg.receive_count)
        finally:
            keep_invisible.cancel()

    async def _extend_while_working(self, msg: Message) -> None:
        while True:
            await asyncio.sleep(20)
            await self.queues.extend("split", msg)

    async def _split(self, job) -> list[str]:
        """Write one single-page PDF per page to S3; returns their keys. Rerunning it
        overwrites the same keys, so a split interrupted by a crash is simply redone."""
        keys: list[str] = []
        with tempfile.TemporaryDirectory(prefix="vf-split-") as tmp:
            path = Path(tmp) / "source"
            await self.storage.download_to_file(job["object_key"], path)
            pages = page_iterator(path, job["content_type"], self.s.max_pages)
            try:
                while (data := await asyncio.to_thread(next, pages, None)) is not None:
                    key = f"pages/{job['id']}/{len(keys):04d}.pdf"
                    await self.storage.put_bytes(key, data, "application/pdf")
                    keys.append(key)
            finally:
                pages.close()
        return keys


async def main() -> None:
    settings = get_settings()
    m.setup_logging(settings.log_level)
    m.serve_metrics(settings.metrics_port)
    pool = await create_pool()
    storage = await Storage(settings).start()
    await storage.ensure_bucket()
    queues = await Queues(settings).start()
    splitter = Splitter(settings, pool, storage, queues)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, splitter.stopping.set)
    try:
        await splitter.run()
    finally:
        await queues.close()
        await storage.close()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
