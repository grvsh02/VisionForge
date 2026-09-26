"""Request correlation: one X-Request-ID per ingestion, carried through the whole pipeline.

The ingest API takes a well-formed ``X-Request-ID`` from the client, or generates one, returns
it on the response, and stores it on the job. From there it travels:

  - in every outbox / SQS message (split, layout, VLM, retries, DLQ copies),
  - to the model APIs as an ``X-Request-ID`` header,
  - in every stream event header (``header.request_id``),
  - into every log line written while handling that job's work.

Log lines pick it up from a context variable (together with ``job_id`` and ``page`` when
set), so functions don't pass it around. asyncio copies the context into each task, so a
value bound inside one page's task never leaks into another's.
"""

from __future__ import annotations

import contextvars
import re
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

HEADER = "X-Request-ID"
_VALID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")  # safe to log and to echo back in a header

_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar("vf_log_context", default={})


def new_request_id(incoming: str | None = None) -> str:
    """The client's ID if it is well-formed, otherwise a fresh one."""
    if incoming and _VALID.match(incoming):
        return incoming
    return uuid.uuid4().hex


def current() -> dict[str, Any]:
    """Fields to add to every log line (request_id, job_id, page)."""
    return _context.get()


def request_id() -> str | None:
    return _context.get().get("request_id")


def headers() -> dict[str, str]:
    """Headers that forward the current request ID to another service."""
    rid = request_id()
    return {HEADER: rid} if rid else {}


@contextmanager
def bound(**fields: Any) -> Iterator[None]:
    """Add fields to the log context for the duration of a block (None values are skipped)."""
    token = _context.set({**_context.get(), **{k: v for k, v in fields.items() if v is not None}})
    try:
        yield
    finally:
        _context.reset(token)


class RequestIdMiddleware:
    """ASGI middleware: pick the request's ID, bind it for logging, echo it on the response.

    Plain ASGI rather than Starlette's BaseHTTPMiddleware, so streaming responses (SSE) are
    passed through untouched.
    """

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        incoming = dict(scope.get("headers") or []).get(HEADER.lower().encode())
        rid = new_request_id(incoming.decode("latin-1") if incoming else None)

        async def send_with_id(message) -> None:
            if message["type"] == "http.response.start":
                message = {**message, "headers": [*message.get("headers", []), (HEADER.lower().encode(), rid.encode())]}
            await send(message)

        with bound(request_id=rid):
            await self.app(scope, receive, send_with_id)
