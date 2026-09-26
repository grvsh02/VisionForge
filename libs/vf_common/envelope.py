"""Stream envelope header math: monotonic seq, contiguous watermark and window bitmap.

Page indexes are 0-based. ``watermark`` is the lowest page index that has not been
emitted yet, so every page ``< watermark`` has been emitted (``watermark == total``
means the document is complete). The window describes which pages in
``[base, base + size)`` are already emitted, where ``base == watermark``; bit ``i``
of the bitmap is ``byte[i // 8] >> (i % 8) & 1``.
"""

from __future__ import annotations

import base64
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

WINDOW_SIZE = 64


def watermark(done: set[int], total: int) -> int:
    w = 0
    while w < total and w in done:
        w += 1
    return w


def window_bitmap(done: set[int], base: int, size: int = WINDOW_SIZE) -> str:
    buf = bytearray((size + 7) // 8)
    for i in range(size):
        if base + i in done:
            buf[i // 8] |= 1 << (i % 8)
    return base64.b64encode(bytes(buf)).decode("ascii")


def decode_bitmap(bitmap: str, base: int, size: int = WINDOW_SIZE) -> set[int]:
    raw = base64.b64decode(bitmap)
    return {base + i for i in range(size) if raw[i // 8] >> (i % 8) & 1}


def build_header(
    *,
    job_id: str,
    client_id: str,
    seq: int,
    page_index: int | None,
    total_pages: int,
    done_pages: Iterable[int],
    window_size: int = WINDOW_SIZE,
    request_id: str | None = None,
) -> dict[str, Any]:
    done = set(done_pages)
    wm = watermark(done, total_pages)
    return {
        "job_id": job_id,
        "client_id": client_id,
        "request_id": request_id,
        "seq": seq,
        "page_index": page_index,
        "total_pages": total_pages,
        "watermark": wm,
        "window": {"base": wm, "size": window_size, "bitmap": window_bitmap(done, wm, window_size)},
        "emitted_at": datetime.now(UTC).isoformat(),
    }
