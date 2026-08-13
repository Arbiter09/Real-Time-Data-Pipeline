"""Bounded retry with an explicit, configurable backoff schedule.

The whole point of this module is that the SAME code path serves both arms of
the Section 7.3 A/B test. Turning backoff off is a config change
(RETRY_BACKOFF_SCHEDULE_MS="") and nothing else, so the two arms differ in one
variable rather than in two code paths that drifted apart.

It also counts attempts, because "average retries to success" is a number the
resume claims and therefore a number this code has to actually produce.
"""
from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional, Tuple, Type, TypeVar

from common.config import RetryConfig

log = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class RetryStats:
    """Accumulates across many calls; drained by the metrics exporter."""
    attempts: int = 0
    successes: int = 0
    failures_exhausted: int = 0
    retries_before_success: int = 0
    total_backoff_ms: float = 0.0
    # reason -> count, so the README can say what *kind* of failure was retried
    # rather than hand-waving about "broker and write-timeout errors".
    failure_reasons: dict = field(default_factory=dict)

    def record_reason(self, exc: BaseException) -> None:
        key = type(exc).__name__
        self.failure_reasons[key] = self.failure_reasons.get(key, 0) + 1

    @property
    def avg_retries_to_success(self) -> float:
        """Mean number of RETRIES (not attempts) taken by calls that succeeded.

        A call that succeeds first try contributes 0. This is the honest
        reading of "average retries to recovery"; reporting attempts instead
        would inflate it by exactly 1.0.
        """
        if self.successes == 0:
            return 0.0
        return self.retries_before_success / self.successes

    def as_dict(self) -> dict:
        return {
            "attempts": self.attempts,
            "successes": self.successes,
            "failures_exhausted": self.failures_exhausted,
            "retries_before_success": self.retries_before_success,
            "avg_retries_to_success": round(self.avg_retries_to_success, 4),
            "total_backoff_ms": round(self.total_backoff_ms, 1),
            "failure_reasons": dict(sorted(self.failure_reasons.items())),
        }

    def merge(self, other: "RetryStats") -> None:
        self.attempts += other.attempts
        self.successes += other.successes
        self.failures_exhausted += other.failures_exhausted
        self.retries_before_success += other.retries_before_success
        self.total_backoff_ms += other.total_backoff_ms
        for k, v in other.failure_reasons.items():
            self.failure_reasons[k] = self.failure_reasons.get(k, 0) + v


class RetriesExhausted(Exception):
    """Raised when every attempt failed. Carries what the DLQ record needs."""

    def __init__(self, attempts: int, last_error: BaseException):
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(f"exhausted after {attempts} attempts: "
                         f"{type(last_error).__name__}: {last_error}")


class BackoffPolicy:
    """Maps retry index -> sleep duration.

    Schedule [1000, 2000, 4000] with max_attempts=4 means:
        attempt 1 fails -> sleep 1s
        attempt 2 fails -> sleep 2s
        attempt 3 fails -> sleep 4s
        attempt 4 fails -> give up, DLQ
    An empty schedule means no sleeping at all (control arm).
    """

    def __init__(self, cfg: Optional[RetryConfig] = None):
        self.cfg = cfg or RetryConfig()

    def delay_ms(self, retry_index: int) -> float:
        sched = self.cfg.backoff_schedule_ms
        if not sched:
            return 0.0
        # Clamp past the end of the schedule rather than crashing, so
        # max_attempts and the schedule length can be tuned independently.
        base = sched[min(retry_index, len(sched) - 1)]
        if self.cfg.jitter_ratio <= 0:
            return float(base)
        # Full-spectrum jitter around the base. Without jitter, every client
        # that failed on the same broker outage retries on the same tick and
        # rebuilds the thundering herd the backoff was meant to break up.
        spread = base * self.cfg.jitter_ratio
        return max(0.0, random.uniform(base - spread, base + spread))

    def describe(self) -> str:
        return self.cfg.describe()


def call_with_retry(
    fn: Callable[[], T],
    *,
    policy: BackoffPolicy,
    stats: RetryStats,
    retry_on: Tuple[Type[BaseException], ...] = (Exception,),
    give_up_on: Tuple[Type[BaseException], ...] = (),
    sleep: Callable[[float], None] = time.sleep,
    on_retry: Optional[Callable[[int, BaseException], None]] = None,
) -> T:
    """Run `fn`, retrying per `policy`. Raises RetriesExhausted on give-up.

    `give_up_on` exists so that permanent errors (a malformed row, a schema
    violation) go straight to the DLQ instead of burning 7 seconds of backoff
    on something that can never succeed. Retrying a deterministic failure is
    just a slower way to fail.
    """
    max_attempts = max(1, policy.cfg.max_attempts)
    last_exc: BaseException | None = None

    for attempt in range(max_attempts):
        stats.attempts += 1
        try:
            result = fn()
        except give_up_on as exc:            # non-retryable: fail immediately
            stats.record_reason(exc)
            stats.failures_exhausted += 1
            raise RetriesExhausted(attempt + 1, exc) from exc
        except retry_on as exc:
            last_exc = exc
            stats.record_reason(exc)
            if attempt == max_attempts - 1:
                break
            wait_ms = policy.delay_ms(attempt)
            stats.total_backoff_ms += wait_ms
            if on_retry is not None:
                on_retry(attempt + 1, exc)
            log.debug("attempt %d/%d failed (%s), backing off %.0fms",
                      attempt + 1, max_attempts, type(exc).__name__, wait_ms)
            if wait_ms > 0:
                sleep(wait_ms / 1000.0)
        else:
            stats.successes += 1
            stats.retries_before_success += attempt
            return result

    stats.failures_exhausted += 1
    assert last_exc is not None
    raise RetriesExhausted(max_attempts, last_exc)


def batched(items: Iterable, size: int):
    """Chunk an iterable. Used to bound Cassandra batch sizes."""
    batch = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
