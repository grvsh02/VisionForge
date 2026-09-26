"""Adaptive backpressure decisions (architecture doc, sections 6 and 9). Pure functions only.

Every model has two limits: a requests-per-second rate and a maximum concurrency. Each
10-second window of call outcomes is judged OVERLOAD, HEALTHY or UNKNOWN:

  OVERLOAD  p95 latency > 2x nominal, or >= 1% 429s, or >= 5% timeouts,
            or 5xx rate > 2x the model's normal error rate
  HEALTHY   none of the above (with enough samples, or an idle/clean small window)

Hysteresis: 2 overloaded windows in a row halve both limits (and every further overloaded
window halves again); 3 healthy windows in a row start raising them by 10% of the maximum
per window. Limits are always clamped to [minimum, hard provider limit].

Fast-stage admission (section 9) is capped by how many pages wait for the slow stage, so
pressure on the slow model travels backwards instead of piling up in the slow queue.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from vf_common.config import Model

WINDOW_S = 10
MIN_SAMPLES = 10
OVERLOAD_WINDOWS = 2
HEALTHY_WINDOWS = 3
MIN_RPS = 1.0
MIN_CONCURRENCY = 1
RETRY_BUDGET_RATIO = 0.10  # retries may be at most 10% of first attempts (rolling 2 windows)...
RETRY_BUDGET_MIN = 5  # ...but a few are always allowed, so low traffic can still retry

OVERLOAD, HEALTHY, UNKNOWN = "overload", "healthy", "unknown"


@dataclass
class Window:
    """Outcomes of one model's calls during one window."""

    calls: int = 0
    throttled: int = 0  # 429
    timeouts: int = 0
    server_errors: int = 0  # 5xx
    latencies: list[float] = field(default_factory=list)


@dataclass
class Limits:
    rps: float
    concurrency: int
    overload_streak: int = 0
    healthy_streak: int = 0

    @classmethod
    def initial(cls, model: Model) -> "Limits":
        return cls(model.start_rps, model.start_concurrency)


def p95(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(0.95 * len(ordered)) - 1)] if ordered else 0.0


def judge(w: Window, model: Model) -> str:
    """OVERLOAD, HEALTHY or UNKNOWN for one window."""
    slow = w.latencies and p95(w.latencies) > 2 * model.nominal_latency_s
    if w.calls < MIN_SAMPLES:
        # Too few calls to compute rates, but any 429 is a strong signal on its own, and an
        # idle or spotless small window lets limits recover even at very low traffic.
        if w.throttled:
            return OVERLOAD
        return HEALTHY if not (w.timeouts or w.server_errors or slow) else UNKNOWN
    if (slow or w.throttled / w.calls >= 0.01 or w.timeouts / w.calls >= 0.05
            or w.server_errors / w.calls > 2 * model.normal_error_rate):
        return OVERLOAD
    return HEALTHY


def next_limits(limits: Limits, verdict: str, model: Model) -> Limits:
    """Apply one window's verdict with hysteresis; always clamp to the model's bounds."""
    rps, conc = limits.rps, limits.concurrency
    over, healthy = limits.overload_streak, limits.healthy_streak
    if verdict == OVERLOAD:
        over, healthy = over + 1, 0
        if over >= OVERLOAD_WINDOWS:
            rps, conc = rps * 0.5, math.floor(conc * 0.5)
    elif verdict == HEALTHY:
        over, healthy = 0, healthy + 1
        if healthy >= HEALTHY_WINDOWS:
            rps += 0.1 * model.hard_rps
            conc += max(1, round(0.1 * model.max_concurrency))
    rps = min(model.hard_rps, max(MIN_RPS, rps))
    conc = min(model.max_concurrency, max(MIN_CONCURRENCY, conc))
    return Limits(round(rps, 3), conc, over, healthy)


def fast_admission_rps(slow_queue_depth: int) -> float:
    """Cap on the fast stage's rate from the slow queue's depth (doc's example policy)."""
    if slow_queue_depth < 100:
        return 100.0
    if slow_queue_depth < 500:
        return 50.0
    if slow_queue_depth < 1000:
        return 20.0
    return 0.0  # paused until the slow stage catches up
