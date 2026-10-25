"""Retry policy: backoff math and what to do with a failed attempt."""

import random
from collections.abc import Callable
from dataclasses import dataclass

from .errors import is_retryable

RETRY = "retry"
DEAD = "dead"

# 2**32 seconds is far past any sane cap; clamping avoids float overflow.
_MAX_EXPONENT = 32


def backoff_delay(
    attempt: int,
    base: float = 2.0,
    cap: float = 300.0,
    jitter: float = 0.2,
    rand: Callable[[], float] = random.random,
) -> float:
    """Delay before retry number `attempt` (1-based): base * 2**(attempt-1),
    capped, plus up to `jitter` fraction of random extra so retries spread out."""
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    delay = min(cap, base * 2 ** min(attempt - 1, _MAX_EXPONENT))
    return delay + delay * jitter * rand()


@dataclass(frozen=True)
class Outcome:
    action: str
    delay: float = 0.0
    reason: str = ""


def decide_failure(
    exc: BaseException,
    attempts: int,
    max_attempts: int,
    backoff: Callable[[int], float] = backoff_delay,
) -> Outcome:
    if not is_retryable(exc):
        return Outcome(DEAD, reason="permanent error")
    if attempts >= max_attempts:
        return Outcome(DEAD, reason=f"exhausted {max_attempts} attempts")
    return Outcome(RETRY, delay=backoff(attempts))
