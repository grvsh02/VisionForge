"""Domain types shared across services."""

from __future__ import annotations

import re
from enum import StrEnum

from pydantic import BaseModel, Field

CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


class JobStatus(StrEnum):
    UPLOADED = "UPLOADED"
    SPLIT = "SPLIT"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class PageStatus(StrEnum):
    """Page lifecycle from the architecture doc (section 4)."""

    PENDING = "PENDING"
    FAST_QUEUED = "FAST_QUEUED"
    FAST_PROCESSING = "FAST_PROCESSING"
    FAST_COMPLETED = "FAST_COMPLETED"
    SLOW_QUEUED = "SLOW_QUEUED"
    SLOW_PROCESSING = "SLOW_PROCESSING"
    RETRY_WAIT = "RETRY_WAIT"  # `pages.stage` says which stage it will be retried in
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


TERMINAL_STATUSES = (PageStatus.COMPLETED, PageStatus.FAILED)


class EventKind(StrEnum):
    JOB_SPLIT = "job_split"
    PAGE_RESULT = "page_result"
    PAGE_FALLBACK = "page_fallback"
    PAGE_FAILED = "page_failed"
    JOB_COMPLETE = "job_complete"
    JOB_FAILED = "job_failed"


FINAL_EVENT_KINDS = (EventKind.JOB_COMPLETE, EventKind.JOB_FAILED)


class BBox(BaseModel):
    x: float
    y: float
    w: float
    h: float


class DocNode(BaseModel):
    """Output tree node; carries everything a future evaluation service needs."""

    type: str
    text: str | None = None
    bbox: BBox | None = None
    order: int | None = None
    confidence: float | None = None
    children: list["DocNode"] = Field(default_factory=list)


class PageResult(BaseModel):
    page_index: int
    source: str  # "vlm" | "layout_fallback" | "none"
    confidence: float
    low_confidence: bool
    tree: DocNode
    text: str = ""
    tables_markdown: list[str] = Field(default_factory=list)
    key_values: dict[str, str] = Field(default_factory=dict)
    error: str | None = None
