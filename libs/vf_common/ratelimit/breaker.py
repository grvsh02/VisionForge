"""Circuit breaker per model, kept in Redis so it survives a worker restart (architecture doc, section 10).

CLOSED passes calls. It opens when at least half of the last 20 calls failed (5xx/timeouts),
or immediately on 401/403 (``trip``). OPEN stops calls for ``open_s``; then HALF_OPEN lets 3
probe calls through, and 3 successes close it again. A probe that ends without a verdict
(a 429, a cache replay, a crash before the call) is handed back with ``release``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from importlib import resources

from redis.asyncio import Redis

OPEN_S = 10.0


@dataclass
class BreakerDecision:
    decision: str  # allow | probe | deny
    wait_s: float
    state: str  # closed | open | half_open

    @property
    def allowed(self) -> bool:
        return self.decision != "deny"


def _s(v) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)


class CircuitBreaker:
    def __init__(self, redis: Redis, *, window: int = 20, threshold: float = 0.5,
                 open_s: float = OPEN_S, max_probes: int = 3, clock=time.time) -> None:
        self.redis = redis
        self.params = [window, threshold, open_s, max_probes]
        self.clock = clock
        self._script = redis.register_script(
            resources.files("vf_common.ratelimit").joinpath("breaker.lua").read_text())

    def _keys(self, model: str) -> list[str]:
        return [f"brk:{model}", f"brk:{model}:win"]

    async def _call(self, op: str, model: str, success: bool = False) -> list:
        return await self._script(keys=self._keys(model),
                                  args=[op, self.clock(), "1" if success else "0", *self.params])

    async def allow(self, model: str) -> BreakerDecision:
        decision, wait, _degraded, state = await self._call("allow", model)
        return BreakerDecision(_s(decision), float(_s(wait)), _s(state))

    async def record(self, model: str, success: bool) -> str:
        return _s((await self._call("record", model, success))[0])

    async def trip(self, model: str) -> None:
        await self._call("trip", model)

    async def release(self, model: str) -> None:
        await self._call("release", model)

    async def state(self, model: str) -> str:
        return _s(await self.redis.hget(f"brk:{model}", "state") or b"closed")
