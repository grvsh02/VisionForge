import pytest

from vf_common.errors import BASE_DELAY_S, MAX_DELAY_S, Outcome, backoff_s, classify, deferral_s


@pytest.mark.parametrize("status,timed_out,outcome", [
    (200, False, Outcome.OK),
    (None, True, Outcome.RETRY),   # timeout
    (None, False, Outcome.RETRY),  # connection error
    (408, False, Outcome.RETRY),
    (500, False, Outcome.RETRY), (502, False, Outcome.RETRY), (503, False, Outcome.RETRY),
    (504, False, Outcome.RETRY),
    (429, False, Outcome.THROTTLED),
    (401, False, Outcome.AUTH), (403, False, Outcome.AUTH),
    (400, False, Outcome.PERMANENT), (404, False, Outcome.PERMANENT), (422, False, Outcome.PERMANENT),
])
def test_classification_follows_the_error_table(status, timed_out, outcome):
    assert classify(status, timed_out) == outcome


def test_backoff_is_full_jitter_and_capped():
    for attempt in range(1, 12):
        samples = [backoff_s(attempt) for _ in range(200)]
        cap = min(MAX_DELAY_S, BASE_DELAY_S * 2 ** attempt)
        assert all(0 <= s <= cap for s in samples)
        assert max(samples) > 0.5 * cap  # spread over the whole range, not clustered


def test_budget_deferrals_are_spread_over_the_second_half_of_the_max_delay():
    samples = [deferral_s() for _ in range(200)]
    assert all(MAX_DELAY_S / 2 <= s <= MAX_DELAY_S for s in samples)
    assert max(samples) - min(samples) > MAX_DELAY_S / 4  # not all in the same window
