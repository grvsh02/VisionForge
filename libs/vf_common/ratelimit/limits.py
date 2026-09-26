"""Redis-backed limits for one model. Each stage runs a single worker; the state lives in
Redis so it survives a worker restart (a crash during an overload doesn't reset the limits).

Keys (``<stage>`` is fast or slow):
  ctl:<stage>             current Limits (rps, concurrency, streaks) + last judged window
  win:<stage>:<window>    outcome counters for one 10 s window, lat:<stage>:<window> latencies
  bucket:<stage>          token bucket at the current rps (frozen after a 429)
  slots:<stage>           concurrency slots held by in-flight calls (expire if a worker dies)
"""

from __future__ import annotations

import time
from importlib import resources

from redis.asyncio import Redis

from vf_common.config import Model
from vf_common.errors import Outcome
from vf_common.ratelimit import controller as ctl
from vf_common.ratelimit.controller import Limits, Window

SLOT_TTL_S = 60  # a slot outlives its holder by at most this long if the worker crashes


def _s(v) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)


class ModelLimits:
    def __init__(self, redis: Redis, model: Model, clock=time.time) -> None:
        self.redis, self.model, self.clock = redis, model, clock
        self.stage = model.stage
        self._lua = redis.register_script(resources.files("vf_common.ratelimit").joinpath("limits.lua").read_text())

    # --- current limits --------------------------------------------------------------
    async def _state(self) -> tuple[Limits, int]:
        """(current limits, id of the last window judged; -1 if none yet)."""
        raw = {_s(k): _s(v) for k, v in (await self.redis.hgetall(f"ctl:{self.stage}")).items()}
        if "rps" not in raw:
            return Limits.initial(self.model), -1
        return (Limits(float(raw["rps"]), int(raw["concurrency"]), int(raw["overload_streak"]),
                       int(raw["healthy_streak"])), int(raw.get("window", -1)))

    async def current(self) -> Limits:
        return (await self._state())[0]

    # --- rolling window --------------------------------------------------------------
    def _window_id(self, offset: int = 0) -> int:
        return int(self.clock() // ctl.WINDOW_S) + offset

    async def record(self, outcome: Outcome, status: int | None, timed_out: bool, latency_s: float,
                     is_retry: bool) -> None:
        wid = self._window_id()
        key = f"win:{self.stage}:{wid}"
        pipe = self.redis.pipeline(transaction=False)
        pipe.hincrby(key, "calls", 1)
        if not is_retry:  # retries are counted when reserved (try_spend_retry)
            pipe.hincrby(key, "first", 1)
        if outcome == Outcome.THROTTLED:
            pipe.hincrby(key, "throttled", 1)
        if timed_out:
            pipe.hincrby(key, "timeouts", 1)
        elif status is not None and status >= 500:
            pipe.hincrby(key, "server_errors", 1)
        if not timed_out:
            pipe.rpush(f"lat:{self.stage}:{wid}", latency_s)
        pipe.expire(key, 6 * ctl.WINDOW_S)
        pipe.expire(f"lat:{self.stage}:{wid}", 6 * ctl.WINDOW_S)
        await pipe.execute()

    async def _window(self, wid: int) -> tuple[Window, dict[str, int]]:
        raw = {_s(k): int(v) for k, v in (await self.redis.hgetall(f"win:{self.stage}:{wid}")).items()}
        latencies = [float(_s(x)) for x in await self.redis.lrange(f"lat:{self.stage}:{wid}", 0, -1)]
        window = Window(raw.get("calls", 0), raw.get("throttled", 0), raw.get("timeouts", 0),
                        raw.get("server_errors", 0), latencies)
        return window, raw

    async def evaluate(self) -> tuple[str, Limits] | None:
        """Judge the window that just ended and update the limits. The worker calls this every
        second; only the first call after a window ends does anything."""
        wid = self._window_id(-1)
        limits, last_judged = await self._state()
        if last_judged >= wid:
            return None  # already judged (also after a restart)
        window, _ = await self._window(wid)
        verdict = ctl.judge(window, self.model)
        new = ctl.next_limits(limits, verdict, self.model)
        await self.redis.hset(f"ctl:{self.stage}", mapping={
            "rps": new.rps, "concurrency": new.concurrency, "overload_streak": new.overload_streak,
            "healthy_streak": new.healthy_streak, "window": wid})
        return verdict, new

    async def recent_success_rps(self) -> float:
        """Successful calls per second in the last complete window (drain-time estimate)."""
        _, raw = await self._window(self._window_id(-1))
        bad = raw.get("throttled", 0) + raw.get("timeouts", 0) + raw.get("server_errors", 0)
        return max(0, raw.get("calls", 0) - bad) / ctl.WINDOW_S

    async def try_spend_retry(self) -> bool:
        """Reserve one retry within the budget (current + previous window), atomically."""
        wid = self._window_id()
        return bool(await self._lua(
            keys=[f"win:{self.stage}:{wid}", f"win:{self.stage}:{wid - 1}"],
            args=["retry", ctl.RETRY_BUDGET_RATIO, ctl.RETRY_BUDGET_MIN, 6 * ctl.WINDOW_S]))

    # --- token bucket ----------------------------------------------------------------
    async def take_token(self, rate: float, max_wait_s: float = 1.0) -> tuple[bool, float]:
        granted, wait = await self._lua(keys=[f"bucket:{self.stage}"],
                                        args=["take", self.clock(), max(rate, 0.01), max_wait_s])
        return bool(int(granted)), float(_s(wait))

    async def freeze(self, seconds: float) -> None:
        await self._lua(keys=[f"bucket:{self.stage}"], args=["freeze", self.clock() + seconds])

    # --- concurrency slots -----------------------------------------------------------
    async def acquire_slot(self, holder: str, limit: int) -> bool:
        return bool(await self._lua(keys=[f"slots:{self.stage}"],
                                    args=["acquire", self.clock(), holder, limit, SLOT_TTL_S]))

    async def refresh_slot(self, holder: str) -> None:
        await self._lua(keys=[f"slots:{self.stage}"], args=["refresh", self.clock(), holder, SLOT_TTL_S])

    async def release_slot(self, holder: str) -> None:
        await self._lua(keys=[f"slots:{self.stage}"], args=["release", holder])

    async def clear_slots(self) -> None:
        """Called when the worker starts. There is one worker per stage, so any slot still in
        Redis belongs to a worker that died; without this, those slots would block every
        new call until they expire (up to SLOT_TTL_S)."""
        await self.redis.delete(f"slots:{self.stage}")

    async def slots_in_use(self) -> int:
        await self.redis.zremrangebyscore(f"slots:{self.stage}", "-inf", self.clock())
        return await self.redis.zcard(f"slots:{self.stage}")
