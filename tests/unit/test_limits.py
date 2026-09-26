import asyncio

import fakeredis
import pytest

from vf_common.config import MODELS
from vf_common.errors import Outcome
from vf_common.ratelimit import controller as ctl
from vf_common.ratelimit.limits import SLOT_TTL_S, ModelLimits


class Clock:
    t = 10_000.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def redis():
    return fakeredis.FakeAsyncRedis()


@pytest.fixture
def limits(redis, clock):
    return ModelLimits(redis, MODELS["slow"], clock=clock)


async def test_starts_at_the_initial_limits(limits):
    current = await limits.current()
    assert (current.rps, current.concurrency) == (8, 20)


async def test_token_bucket_spaces_callers_at_the_rate(limits, clock):
    clock.t += 60  # idle for a long time: still no burst
    grants = [await limits.take_token(rate=4) for _ in range(8)]
    assert [wait for ok, wait in grants if ok] == [0, 0.25, 0.5, 0.75, 1.0]  # 1/rate apart, max 1 s ahead
    assert [ok for ok, _ in grants[5:]] == [False] * 3


async def test_freeze_blocks_every_caller_until_retry_after(limits, clock):
    await limits.freeze(2.0)
    ok, wait = await limits.take_token(rate=8)
    assert not ok and wait == pytest.approx(2.0)
    clock.t += 2.5
    assert (await limits.take_token(rate=8))[0]


async def test_concurrency_slots_are_shared_and_expire(limits, clock):
    assert await limits.acquire_slot("a", limit=2)
    assert await limits.acquire_slot("b", limit=2)
    assert not await limits.acquire_slot("c", limit=2)
    await limits.release_slot("a")
    assert await limits.acquire_slot("c", limit=2)
    clock.t += SLOT_TTL_S - 1
    await limits.refresh_slot("c")  # c's worker is alive; b's crashed
    clock.t += 2
    assert await limits.slots_in_use() == 1
    assert await limits.acquire_slot("d", limit=2)


async def test_a_restarted_worker_frees_the_slots_of_the_one_that_died(redis, limits, clock):
    for i in range(20):
        assert await limits.acquire_slot(f"dead-{i}", limit=20)  # then the worker was SIGKILLed
    restarted = ModelLimits(redis, MODELS["slow"], clock=clock)
    assert not await restarted.acquire_slot("new", limit=20)  # blocked until the old slots expire...
    await restarted.clear_slots()  # ...unless cleared on start
    assert await restarted.acquire_slot("new", limit=20)


async def record(limits, n, outcome=Outcome.OK, status=200, latency=2.0):
    for _ in range(n):
        await limits.record(outcome, status, False, latency, is_retry=False)


async def test_each_window_is_judged_once(limits, clock):
    for window in range(2):  # two overloaded windows
        await record(limits, 20, Outcome.THROTTLED, 429)
        clock.t += ctl.WINDOW_S
        results = [await limits.evaluate() for _ in range(5)]  # _control runs every second
        assert results[0] is not None and results[1:] == [None] * 4
    current = await limits.current()
    assert (current.rps, current.concurrency) == (4, 10)


async def test_a_restarted_worker_keeps_the_limits_and_does_not_rejudge(redis, limits, clock):
    for _ in range(2):
        await record(limits, 20, Outcome.THROTTLED, 429)
        clock.t += ctl.WINDOW_S
        await limits.evaluate()
    restarted = ModelLimits(redis, MODELS["slow"], clock=clock)
    assert await restarted.evaluate() is None  # the window before the restart was already judged
    current = await restarted.current()
    assert (current.rps, current.concurrency, current.overload_streak) == (4, 10, 2)


async def test_retry_budget_is_reserved_atomically(limits):
    await record(limits, 100)  # 100 first attempts -> budget of 10 retries
    grants = await asyncio.gather(*(limits.try_spend_retry() for _ in range(25)))
    assert sum(grants) == 10  # concurrent retries can't all slip past the budget


async def test_retry_budget_always_allows_a_few_and_refills_as_windows_pass(limits, clock):
    assert [await limits.try_spend_retry() for _ in range(6)] == [True] * 5 + [False]
    clock.t += 2 * ctl.WINDOW_S  # the retries age out of the two-window budget
    assert await limits.try_spend_retry()
