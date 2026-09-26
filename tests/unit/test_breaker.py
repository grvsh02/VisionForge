import fakeredis
import pytest

from vf_common.ratelimit.breaker import CircuitBreaker


class Clock:
    t = 1_000.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def brk(clock):
    return CircuitBreaker(fakeredis.FakeAsyncRedis(), window=4, threshold=0.5, open_s=10, max_probes=3, clock=clock)


async def test_opens_on_failure_rate_then_probes_then_closes(brk, clock):
    for ok in (True, False, True, False):  # 50% failures over the window
        await brk.record("vlm", success=ok)
    assert (await brk.allow("vlm")).decision == "deny"
    clock.t += 10
    assert [(await brk.allow("vlm")).decision for _ in range(4)] == ["probe", "probe", "probe", "deny"]
    for _ in range(3):
        await brk.record("vlm", success=True)
    assert (await brk.allow("vlm")).decision == "allow"


async def test_failed_probe_reopens(brk, clock):
    for _ in range(4):
        await brk.record("vlm", success=False)
    clock.t += 10
    assert (await brk.allow("vlm")).decision == "probe"
    assert await brk.record("vlm", success=False) == "open"
    assert (await brk.allow("vlm")).decision == "deny"


async def test_trip_opens_immediately(brk):
    """401/403: a configuration problem, no need to wait for a failure rate."""
    assert (await brk.allow("vlm")).decision == "allow"
    await brk.trip("vlm")
    assert (await brk.allow("vlm")).state == "open"


async def test_released_probes_let_the_breaker_close_under_light_traffic(brk, clock):
    await brk.trip("vlm")
    clock.t += 10
    for _ in range(10):  # idle polls: take a probe, find no message, hand it back
        assert (await brk.allow("vlm")).decision == "probe"
        await brk.release("vlm")
    for _ in range(3):
        assert (await brk.allow("vlm")).decision == "probe"
        await brk.record("vlm", success=True)
    assert (await brk.allow("vlm")).state == "closed"
