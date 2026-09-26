"""Upload a document, log every result that arrives over the job's WebSocket, and save
everything received to a results directory.

  uv run python scripts/ws_upload.py fixtures/doc20.pdf --client-id acme
  uv run python scripts/ws_upload.py scan.tiff --client-id acme --out-dir /tmp/runs --raw

Results go to ``<out-dir>/<job_id>/`` (default ``results/<job_id>/``):

  frames.jsonl          every frame exactly as received over the WebSocket, in arrival order,
                        each stamped with ``received_at`` (keep-alive pings are not stored)
  pages/page_NNNN.json  one file per page result: the result plus its seq, header and arrival time
  document.json         the page results in document order (written when the run ends)
  summary.json          job id, source file, status, page counts, arrival order, reconnects, timings

Frames are logged as they arrive (pages finish out of order). If the socket drops, the
script reconnects with ``last_event_id`` so no result is lost or repeated. It exits when
the job completes or fails.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import mimetypes
import os
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

log = logging.getLogger("ws_upload")

PAGE_EVENTS = {"page_result", "page_fallback", "page_failed"}
FINAL_EVENTS = {"job_complete", "job_failed"}


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def _write_json(path: Path, data: Any) -> None:
    """Write atomically, so an interrupted run never leaves a half-written file."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


class ResultStore:
    """Persists everything drawn from the WebSocket for one job (layout in the module docstring)."""

    def __init__(self, root: Path, *, job_id: str, client_id: str, source: Path, base_url: str) -> None:
        self.dir = root / job_id
        self.pages_dir = self.dir / "pages"
        self.pages_dir.mkdir(parents=True, exist_ok=True)
        self._frames = (self.dir / "frames.jsonl").open("a", encoding="utf-8")
        self.meta = {"job_id": job_id, "client_id": client_id, "source_file": str(source.resolve()),
                     "source_bytes": source.stat().st_size, "base_url": base_url, "started_at": _now()}
        self.total_pages: int | None = None
        self.pages: dict[int, dict[str, Any]] = {}
        self.arrival_order: list[int] = []
        self.frames = 0
        self.connections = 0
        self.final_payload: dict[str, Any] | None = None
        self._started = time.monotonic()
        self._write_summary("streaming")  # something useful exists even if the run is killed

    def record_frame(self, frame: dict[str, Any]) -> None:
        self._frames.write(json.dumps({"received_at": _now(), **frame}) + "\n")
        self._frames.flush()
        self.frames += 1

    def save_page(self, event: str, envelope: dict[str, Any]) -> Path:
        header = envelope["header"]
        idx = header["page_index"]
        record = {"page_index": idx, "event": event, "seq": header["seq"], "received_at": _now(),
                  "header": header, "result": envelope["payload"]}
        path = self.pages_dir / f"page_{idx:04d}.json"
        _write_json(path, record)
        self.pages[idx] = record
        self.arrival_order.append(idx)
        return path

    def finish(self, status: str) -> None:
        self._frames.close()
        indexes = range(self.total_pages) if self.total_pages is not None else sorted(self.pages)
        missing = [i for i in indexes if i not in self.pages]
        _write_json(self.dir / "document.json", {
            "job_id": self.meta["job_id"], "total_pages": self.total_pages,
            "complete": status == "complete" and not missing, "missing_pages": missing,
            "pages": [self.pages[i]["result"] for i in sorted(self.pages)]})
        self._write_summary(status, missing=missing)

    def _write_summary(self, status: str, missing: list[int] | None = None) -> None:
        sources: dict[str, int] = {}
        for record in self.pages.values():
            source = record["result"].get("source", "unknown")
            sources[source] = sources.get(source, 0) + 1
        _write_json(self.dir / "summary.json", {
            **self.meta, "status": status, "updated_at": _now(),
            "elapsed_s": round(time.monotonic() - self._started, 2),
            "total_pages": self.total_pages, "pages_received": len(self.pages),
            "missing_pages": missing or [], "pages_by_source": sources,
            "low_confidence_pages": sorted(i for i, r in self.pages.items() if r["result"].get("low_confidence")),
            "arrival_order": self.arrival_order, "frames_received": self.frames,
            "websocket_connections": self.connections, "final_event_payload": self.final_payload})


async def upload(base_url: str, client_id: str, path: Path) -> dict[str, Any]:
    """Stream the file to POST /jobs and return the accepted job."""
    ctype = mimetypes.guess_type(path.name)[0] or "application/pdf"

    async def chunks():
        with path.open("rb") as fh:
            while chunk := fh.read(1024 * 1024):
                yield chunk

    async with httpx.AsyncClient(timeout=300) as http:
        resp = await http.post(f"{base_url}/jobs", content=chunks(), headers={
            "X-Client-ID": client_id, "Content-Type": ctype, "Content-Length": str(path.stat().st_size)})
    if resp.status_code == 429:
        raise SystemExit(f"rate limited by the edge (too many requests): {resp.text}")
    if resp.status_code != 202:
        raise SystemExit(f"upload failed: HTTP {resp.status_code}: {resp.text}")
    job = resp.json()
    log.info("uploaded %s (%.1f MiB, %d pages) as job %s", path.name, job["size_bytes"] / 2**20,
             job["total_pages"], job["job_id"])
    return job


def log_page(envelope: dict) -> None:
    header, page = envelope["header"], envelope["payload"]
    log.info("page %3s  source=%-15s confidence=%.2f%s  watermark=%s/%s",
             header["page_index"], page.get("source"), page.get("confidence", 0),
             "  LOW-CONFIDENCE" if page.get("low_confidence") else "",
             header["watermark"], header["total_pages"])


def handle(frame: dict, store: ResultStore, raw: bool) -> str | None:
    """Log and store one WebSocket message; return the final event name once the job ends."""
    event = frame.get("event")
    if event == "ping":
        log.debug("ping")
        return None
    store.record_frame(frame)
    if raw:
        log.info("%s", json.dumps(frame))
    data = frame["data"]
    if event in PAGE_EVENTS:
        store.save_page(event, data)
        if not raw:
            log_page(data)
    elif event == "job_split":
        store.total_pages = data["payload"]["total_pages"]
        log.info("job split into %d pages", store.total_pages)
    elif event == "job_complete":
        store.final_payload = data["payload"]
        log.info("job complete: %s", data["payload"]["states"])
    elif event == "job_failed":
        store.final_payload = data["payload"]
        log.error("job failed: %s", data["payload"].get("error"))
    return event if event in FINAL_EVENTS else None


def _describe(exc: Exception) -> str:
    if isinstance(exc, InvalidStatus):
        return f"HTTP {exc.response.status_code}"
    if isinstance(exc, ConnectionClosed):
        return f"closed with code {exc.rcvd.code if exc.rcvd else 'none'}"
    return repr(exc)


async def listen(base_url: str, client_id: str, job_id: str, store: ResultStore, raw: bool) -> str:
    """Consume the job's WebSocket until it ends; returns ``job_complete`` or ``job_failed``."""
    ws_base = base_url.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
    last_seq = 0
    attempt = 0
    while True:
        query = urlencode({"client_id": client_id, "last_event_id": last_seq})
        url = f"{ws_base}/jobs/{job_id}/ws?{query}"
        try:
            async with connect(url, additional_headers={"X-Client-ID": client_id}) as ws:
                store.connections += 1
                log.info("websocket connected%s", f" (resuming after seq {last_seq})" if last_seq else "")
                attempt = 0
                async for message in ws:
                    frame = json.loads(message)
                    if "id" in frame:
                        if frame["id"] <= last_seq:
                            continue  # already seen before a reconnect
                        last_seq = frame["id"]
                    if final := handle(frame, store, raw):
                        return final
                log.warning("server closed the websocket before the job finished; reconnecting")
        except (InvalidStatus, ConnectionClosed, OSError) as exc:
            if isinstance(exc, InvalidStatus) and exc.response.status_code < 500:
                # 4xx (bad client id, job not found) will not fix itself; 5xx is a gateway restart.
                raise SystemExit(f"websocket rejected: HTTP {exc.response.status_code}") from exc
            attempt += 1
            if attempt > 10:
                raise SystemExit(f"giving up after {attempt} reconnect attempts: {exc!r}") from exc
            log.warning("websocket unavailable (%s); reconnecting from seq %d", _describe(exc), last_seq)
        await asyncio.sleep(min(5.0, 0.5 * 2 ** attempt))


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("file", type=Path, help="PDF, TIFF, PNG or JPEG to upload")
    ap.add_argument("--client-id", required=True, help="sent as X-Client-ID")
    ap.add_argument("--base-url", default="http://localhost:8080", help="edge gateway URL")
    ap.add_argument("--out-dir", type=Path, default=Path("results"),
                    help="where to save results; each job gets its own <job_id> subdirectory (default: results)")
    ap.add_argument("--raw", action="store_true", help="log each raw JSON frame")
    ap.add_argument("-v", "--verbose", action="store_true", help="also log keep-alive pings")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s.%(msecs)03d %(levelname)-5s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if not args.file.is_file():
        raise SystemExit(f"no such file: {args.file}")
    # Ctrl-C or `kill` cancels the run, so the finally block below still records what arrived.
    main_task = asyncio.current_task()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, main_task.cancel)

    job = await upload(args.base_url, args.client_id, args.file)
    store = ResultStore(args.out_dir, job_id=job["job_id"], client_id=args.client_id, source=args.file,
                        base_url=args.base_url)
    log.info("saving results to %s", store.dir)
    status = "interrupted"
    try:
        final = await listen(args.base_url, args.client_id, job["job_id"], store, args.raw)
        status = "complete" if final == "job_complete" else "failed"
    finally:
        store.finish(status)
        log.info("%s: %d/%s pages saved to %s", status, len(store.pages), store.total_pages, store.dir)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        sys.exit(130)
