"""VisionForge reference client: submit documents and reassemble streamed results in order.

  python client/cli.py --client-id acme submit doc.pdf --stream --out out.json
  python client/cli.py --client-id acme stream <job_id> --out out.json
  python client/cli.py --client-id acme status <job_id>

Stream assembly: pages arrive out of order; the client keeps a reorder buffer keyed by
page index and flushes the contiguous prefix. Each frame's ``seq`` is monotonic, so a
gap triggers a reconnect with ``Last-Event-ID`` and duplicates are ignored, which
makes reconnects (gateway restarts, dropped connections) lossless and idempotent.
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

PAGE_KINDS = {"page_result", "page_fallback", "page_failed"}


@dataclass
class Assembler:
    total_pages: int | None = None
    last_seq: int = 0
    next_index: int = 0
    buffer: dict[int, dict[str, Any]] = field(default_factory=dict)
    ordered: list[dict[str, Any]] = field(default_factory=list)
    arrival_order: list[int] = field(default_factory=list)
    out_of_order: int = 0
    max_buffered: int = 0
    duplicates: int = 0
    complete: bool = False
    failed: str | None = None

    def accept(self, kind: str, data: dict[str, Any]) -> list[dict[str, Any]]:
        """Apply one frame; returns the pages newly flushed in document order."""
        header = data["header"]
        seq = header["seq"]
        if seq <= self.last_seq:
            self.duplicates += 1
            return []
        self.last_seq = seq
        if self.total_pages is None and header.get("total_pages"):
            self.total_pages = header["total_pages"]
        return self._apply(data, header, kind)

    def _apply(self, env: dict[str, Any], h: dict[str, Any], kind: str) -> list[dict[str, Any]]:
        if kind == "job_split":
            self.total_pages = env["payload"]["total_pages"]
            return []
        if kind == "job_complete":
            self.complete = True
            return []
        if kind == "job_failed":
            self.failed = env["payload"].get("error", "job failed")
            return []
        idx = h["page_index"]
        self.arrival_order.append(idx)
        if idx != self.next_index:
            self.out_of_order += 1
        self.buffer[idx] = env["payload"]
        self.max_buffered = max(self.max_buffered, len(self.buffer))
        flushed = []
        while self.next_index in self.buffer:
            flushed.append(self.buffer.pop(self.next_index))
            self.next_index += 1
        self.ordered += flushed
        return flushed


def iter_sse(resp: httpx.Response) -> Iterator[tuple[str, str, str]]:
    event, data, eid = "message", [], ""
    for line in resp.iter_lines():
        if line == "":
            if data:
                yield eid, event, "\n".join(data)
            event, data, eid = "message", [], ""
        elif line.startswith(":"):
            continue
        else:
            name, _, value = line.partition(":")
            value = value[1:] if value.startswith(" ") else value
            if name == "data":
                data.append(value)
            elif name == "event":
                event = value
            elif name == "id":
                eid = value


def stream_job(base: str, client_id: str, job_id: str, *, quiet: bool = False,
               max_reconnects: int = 50) -> Assembler:
    asm = Assembler()
    reconnects = 0
    with httpx.Client(timeout=httpx.Timeout(10.0, read=60.0)) as http:
        while not (asm.complete or asm.failed):
            headers = {"X-Client-ID": client_id, "Last-Event-ID": str(asm.last_seq)}
            try:
                with http.stream("GET", f"{base}/jobs/{job_id}/stream", headers=headers) as resp:
                    resp.raise_for_status()
                    for _eid, kind, raw in iter_sse(resp):
                        data = json.loads(raw)
                        header = data["header"]
                        if header["seq"] > asm.last_seq + 1:
                            raise ConnectionError(f"seq gap: expected {asm.last_seq + 1}, got {header['seq']}")
                        for page in asm.accept(kind, data):
                            if not quiet:
                                print(f"  page {page['page_index']:>3} in order "
                                      f"(source={page['source']}, low_confidence={page['low_confidence']})")
                        if not quiet and kind in PAGE_KINDS:
                            print(f"seq={header['seq']:<4} {kind:<13} page={header.get('page_index')!s:<4} "
                                  f"watermark={header['watermark']:<4} buffered={len(asm.buffer):<3}")
                        if asm.complete or asm.failed:
                            break
            except (httpx.HTTPError, ConnectionError) as exc:
                reconnects += 1
                if reconnects > max_reconnects:
                    raise
                if not quiet:
                    print(f"reconnecting after {exc!r} from seq {asm.last_seq}", file=sys.stderr)
                time.sleep(min(5, 0.5 * reconnects))
    return asm


def submit(base: str, client_id: str, path: Path) -> dict[str, Any]:
    ctype = "application/pdf" if path.suffix.lower() == ".pdf" else mimetypes.guess_type(path.name)[0]

    def chunks() -> Iterator[bytes]:
        with path.open("rb") as fh:
            while chunk := fh.read(1024 * 1024):
                yield chunk

    with httpx.Client(timeout=120) as http:
        resp = http.post(f"{base}/jobs", content=chunks(),
                         headers={"X-Client-ID": client_id, "Content-Type": ctype or "application/octet-stream",
                                  "Content-Length": str(path.stat().st_size)})
    if resp.status_code == 429:
        raise SystemExit(f"rate limited by the edge: {resp.text}")
    resp.raise_for_status()
    return resp.json()


def write_output(asm: Assembler, job_id: str, out: Path) -> None:
    out.write_text(json.dumps({"job_id": job_id, "total_pages": asm.total_pages, "complete": asm.complete,
                               "error": asm.failed, "pages": asm.ordered}, indent=2))


def summary(asm: Assembler, job_id: str, started: float) -> str:
    return (f"job {job_id}: {len(asm.ordered)}/{asm.total_pages} pages in order, "
            f"{asm.out_of_order} arrived out of order, max buffered {asm.max_buffered}, "
            f"{asm.duplicates} duplicates ignored, "
            f"{time.time() - started:.1f}s")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base-url", default="http://localhost:8080")
    ap.add_argument("--client-id", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("submit")
    s.add_argument("file", type=Path)
    s.add_argument("--stream", action="store_true")
    s.add_argument("--out", type=Path)
    st = sub.add_parser("stream")
    st.add_argument("job_id")
    st.add_argument("--out", type=Path)
    sub.add_parser("status").add_argument("job_id")
    args = ap.parse_args()

    started = time.time()
    if args.cmd == "status":
        r = httpx.get(f"{args.base_url}/jobs/{args.job_id}", headers={"X-Client-ID": args.client_id})
        print(json.dumps(r.json(), indent=2))
        return
    if args.cmd == "submit":
        job = submit(args.base_url, args.client_id, args.file)
        print(f"submitted job {job['job_id']} ({job['total_pages']} pages, {job['size_bytes']} bytes)")
        if not args.stream:
            return
        job_id = job["job_id"]
    else:
        job_id = args.job_id
    asm = stream_job(args.base_url, args.client_id, job_id)
    print(summary(asm, job_id, started))
    if args.out:
        write_output(asm, job_id, args.out)
        print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
