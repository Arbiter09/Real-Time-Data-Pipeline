"""Unit tests for the retry/backoff layer.

These matter more than usual because the numbers this module produces
("average retries to success", "backoff 1s -> 2s -> 4s") go straight onto a
resume. If the accounting is wrong, the claim is wrong, and no amount of
careful benchmarking downstream fixes it.

    docker exec rtdp-producer python -m pytest tests/test_retry.py -v
"""
from __future__ import annotations

import sys

import pytest

sys.path.insert(0, "/opt/app")

from common.config import RetryConfig
from common.retry import (BackoffPolicy, RetriesExhausted, RetryStats,
                          batched, call_with_retry)


def fixed_policy(schedule="1000,2000,4000", attempts=4, jitter=0.0):
    return BackoffPolicy(RetryConfig(
        backoff_schedule_ms=[int(x) for x in schedule.split(",")] if schedule else [],
        max_attempts=attempts, jitter_ratio=jitter))


class Recorder:
    """Captures sleeps instead of performing them, so tests stay fast."""

    def __init__(self):
        self.sleeps = []

    def __call__(self, seconds):
        self.sleeps.append(seconds)


# ---------------------------------------------------------------------------
# schedule
# ---------------------------------------------------------------------------
def test_backoff_schedule_is_exactly_1s_2s_4s():
    p = fixed_policy()
    assert [p.delay_ms(i) for i in range(3)] == [1000.0, 2000.0, 4000.0]


def test_schedule_clamps_past_its_end():
    """max_attempts and schedule length are tuned independently, so running off
    the end of the schedule must reuse the last delay, not crash."""
    p = fixed_policy(attempts=8)
    assert p.delay_ms(7) == 4000.0


def test_jitter_stays_within_the_stated_band():
    p = fixed_policy(jitter=0.1)
    for _ in range(200):
        d = p.delay_ms(0)
        assert 900.0 <= d <= 1100.0, d


def test_disabled_backoff_never_sleeps():
    p = fixed_policy(schedule="")
    assert p.cfg.enabled is False
    assert p.delay_ms(0) == 0.0
    assert "DISABLED" in p.describe()


# ---------------------------------------------------------------------------
# retry behaviour
# ---------------------------------------------------------------------------
def test_success_first_try_records_zero_retries():
    stats = RetryStats()
    sleeper = Recorder()
    assert call_with_retry(lambda: "ok", policy=fixed_policy(), stats=stats,
                           sleep=sleeper) == "ok"
    assert stats.successes == 1
    assert stats.attempts == 1
    assert stats.retries_before_success == 0
    assert stats.avg_retries_to_success == 0.0
    assert sleeper.sleeps == []


def test_two_failures_then_success_sleeps_1s_then_2s():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise TimeoutError("transient")
        return "ok"

    stats = RetryStats()
    sleeper = Recorder()
    assert call_with_retry(flaky, policy=fixed_policy(), stats=stats,
                           sleep=sleeper) == "ok"
    assert calls["n"] == 3
    assert stats.attempts == 3
    assert stats.retries_before_success == 2
    assert sleeper.sleeps == [1.0, 2.0]
    assert stats.failure_reasons == {"TimeoutError": 2}


def test_exhaustion_raises_and_is_counted():
    def always_fails():
        raise ConnectionError("down")

    stats = RetryStats()
    sleeper = Recorder()
    with pytest.raises(RetriesExhausted) as exc:
        call_with_retry(always_fails, policy=fixed_policy(), stats=stats,
                        sleep=sleeper)
    assert exc.value.attempts == 4
    assert stats.attempts == 4
    assert stats.successes == 0
    assert stats.failures_exhausted == 1
    # Three sleeps for four attempts: no backoff after the final failure.
    assert sleeper.sleeps == [1.0, 2.0, 4.0]


def test_permanent_errors_skip_the_retry_budget_entirely():
    """Retrying a deterministic failure is a slower way to fail.

    A malformed record must reach the DLQ on attempt 1, not after burning
    seven seconds of backoff.
    """
    def bad_data():
        raise ValueError("fare_amount is null")

    stats = RetryStats()
    sleeper = Recorder()
    with pytest.raises(RetriesExhausted) as exc:
        call_with_retry(bad_data, policy=fixed_policy(), stats=stats,
                        retry_on=(ConnectionError,), give_up_on=(ValueError,),
                        sleep=sleeper)
    assert exc.value.attempts == 1
    assert sleeper.sleeps == []
    assert stats.attempts == 1


def test_disabled_backoff_still_retries_but_without_waiting():
    """The A/B control arm: same attempts, no spacing between them."""
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 4:
            raise TimeoutError("transient")
        return "ok"

    stats = RetryStats()
    sleeper = Recorder()
    call_with_retry(flaky, policy=fixed_policy(schedule=""), stats=stats,
                    sleep=sleeper)
    assert calls["n"] == 4
    assert stats.retries_before_success == 3
    assert sleeper.sleeps == []          # retried hard, waited never


# ---------------------------------------------------------------------------
# accounting
# ---------------------------------------------------------------------------
def test_avg_retries_counts_retries_not_attempts():
    """The distinction that decides whether the reported figure is inflated by
    exactly 1.0. A first-try success contributes 0, not 1."""
    stats = RetryStats(successes=4, retries_before_success=2)
    assert stats.avg_retries_to_success == 0.5


def test_merge_combines_two_partitions_worth_of_stats():
    a = RetryStats(attempts=5, successes=4, retries_before_success=1,
                   failure_reasons={"WriteTimeout": 1})
    b = RetryStats(attempts=3, successes=3, retries_before_success=2,
                   failure_reasons={"WriteTimeout": 2, "Unavailable": 1})
    a.merge(b)
    assert a.attempts == 8
    assert a.successes == 7
    assert a.retries_before_success == 3
    assert a.failure_reasons == {"WriteTimeout": 3, "Unavailable": 1}


def test_batched_chunks_and_keeps_the_remainder():
    assert [len(c) for c in batched(range(10), 4)] == [4, 4, 2]
    assert list(batched([], 4)) == []
