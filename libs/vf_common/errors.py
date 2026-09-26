"""Error classification and retry timing (architecture doc, section 7)."""

from __future__ import annotations

import random
from enum import StrEnum

MAX_ATTEMPTS = 3  # model calls per page per stage before giving up
BASE_DELAY_S = 1.0
MAX_DELAY_S = 60.0


class Outcome(StrEnum):
    OK = "ok"
    THROTTLED = "throttled"  # 429: slow down, retry later, doesn't use an attempt
    RETRY = "retry"  # 408 / timeout / 5xx / connection error: bounded retry with backoff
    AUTH = "auth"  # 401 / 403: configuration problem, open the circuit
    PERMANENT = "permanent"  # 400 / 404 / other 4xx: retrying cannot help


def classify(status: int | None, timed_out: bool = False) -> Outcome:
    if timed_out or status is None or status == 408 or status >= 500:
        return Outcome.RETRY
    if status < 300:
        return Outcome.OK
    if status == 429:
        return Outcome.THROTTLED
    if status in (401, 403):
        return Outcome.AUTH
    return Outcome.PERMANENT


def backoff_s(attempt: int) -> float:
    """Exponential backoff with full jitter: uniform(0, min(MAX, BASE * 2^attempt))."""
    return random.uniform(0, min(MAX_DELAY_S, BASE_DELAY_S * 2 ** attempt))


def deferral_s() -> float:
    """Delay for a retry deferred by the retry budget: near the maximum, but jittered so
    deferred retries don't all come back in the same window and defer each other again."""
    return random.uniform(MAX_DELAY_S / 2, MAX_DELAY_S)
