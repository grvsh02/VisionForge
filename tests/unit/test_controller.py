from vf_common.config import MODELS
from vf_common.ratelimit import controller as ctl
from vf_common.ratelimit.controller import HEALTHY, OVERLOAD, UNKNOWN, Limits, Window

SLOW, FAST = MODELS["slow"], MODELS["fast"]


def window(calls=100, throttled=0, timeouts=0, server_errors=0, latency=2.0):
    return Window(calls, throttled, timeouts, server_errors, [latency] * (calls - timeouts))


def test_judge_overload_signals():
    assert ctl.judge(window(), SLOW) == HEALTHY
    assert ctl.judge(window(latency=6.5), SLOW) == OVERLOAD  # p95 > 2 x nominal (3 s)
    assert ctl.judge(window(throttled=1), SLOW) == OVERLOAD  # >= 1% 429s
    assert ctl.judge(window(timeouts=5), SLOW) == OVERLOAD  # >= 5% timeouts
    assert ctl.judge(window(server_errors=11), SLOW) == OVERLOAD  # > 2 x the normal 5%
    assert ctl.judge(window(server_errors=9), SLOW) == HEALTHY  # normal failure noise


def test_judge_small_windows():
    assert ctl.judge(Window(), SLOW) == HEALTHY  # idle: lets limits recover at no traffic
    assert ctl.judge(window(calls=3), SLOW) == HEALTHY
    assert ctl.judge(window(calls=3, throttled=1), SLOW) == OVERLOAD  # a 429 counts on its own
    assert ctl.judge(window(calls=3, server_errors=1), SLOW) == UNKNOWN  # too few to judge a rate


def test_two_overloaded_windows_halve_both_limits():
    limits = Limits(8, 20)
    limits = ctl.next_limits(limits, OVERLOAD, SLOW)
    assert (limits.rps, limits.concurrency) == (8, 20)  # hysteresis: one window is not enough
    limits = ctl.next_limits(limits, OVERLOAD, SLOW)
    assert (limits.rps, limits.concurrency) == (4, 10)
    limits = ctl.next_limits(limits, OVERLOAD, SLOW)
    assert (limits.rps, limits.concurrency) == (2, 5)  # keeps halving while overloaded


def test_three_healthy_windows_then_plus_ten_percent_per_window():
    limits = Limits(4, 10)
    for _ in range(2):
        limits = ctl.next_limits(limits, HEALTHY, SLOW)
    assert (limits.rps, limits.concurrency) == (4, 10)
    limits = ctl.next_limits(limits, HEALTHY, SLOW)
    assert (limits.rps, limits.concurrency) == (5, 13)  # +1 RPS (10% of 10), +3 slots (10% of 30)
    limits = ctl.next_limits(limits, HEALTHY, SLOW)
    assert (limits.rps, limits.concurrency) == (6, 16)


def test_limits_are_clamped_and_unknown_changes_nothing():
    top = Limits(10, 30, healthy_streak=5)
    assert (ctl.next_limits(top, HEALTHY, SLOW).rps, ctl.next_limits(top, HEALTHY, SLOW).concurrency) == (10, 30)
    bottom = Limits(1, 1, overload_streak=5)
    low = ctl.next_limits(bottom, OVERLOAD, SLOW)
    assert (low.rps, low.concurrency) == (ctl.MIN_RPS, ctl.MIN_CONCURRENCY)
    mid = Limits(5, 12, overload_streak=1, healthy_streak=0)
    assert ctl.next_limits(mid, UNKNOWN, SLOW) == mid
    assert ctl.next_limits(Limits(100, 10, healthy_streak=3), HEALTHY, FAST).rps == 100  # fast hard limit


def test_recovery_from_half_to_full_takes_about_a_minute():
    limits, windows = Limits(5, 15), 0
    while limits.rps < SLOW.hard_rps:
        limits = ctl.next_limits(limits, HEALTHY, SLOW)
        windows += 1
    assert windows * ctl.WINDOW_S <= 80


def test_fast_admission_follows_slow_queue_depth():
    assert [ctl.fast_admission_rps(d) for d in (0, 99, 100, 499, 500, 999, 1000, 5000)] == \
        [100, 100, 50, 50, 20, 20, 0, 0]
