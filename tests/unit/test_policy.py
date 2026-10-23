import pytest

from taskqueue import policy
from taskqueue.errors import PermanentError, RetryableError, is_retryable


def test_backoff_doubles_without_jitter():
    delays = [policy.backoff_delay(n, base=2, cap=300, rand=lambda: 0) for n in range(1, 5)]
    assert delays == [2, 4, 8, 16]


def test_backoff_is_capped():
    assert policy.backoff_delay(20, base=2, cap=60, rand=lambda: 0) == 60


def test_backoff_jitter_stays_within_bounds():
    assert policy.backoff_delay(3, base=2, jitter=0.2, rand=lambda: 1.0) == pytest.approx(8 * 1.2)
    for _ in range(200):
        assert 8 <= policy.backoff_delay(3, base=2, jitter=0.2) <= 9.6


def test_backoff_handles_huge_attempt_numbers():
    assert policy.backoff_delay(10_000, base=2, cap=300, rand=lambda: 0) == 300


def test_backoff_rejects_attempt_zero():
    with pytest.raises(ValueError):
        policy.backoff_delay(0)


@pytest.mark.parametrize("exc, retryable", [
    (RetryableError("timeout"), True),
    (TimeoutError(), True),
    (ConnectionError(), True),
    (RuntimeError("boom"), True),
    (PermanentError("bad"), False),
    (ValueError("not a number"), False),
    (KeyError("to"), False),
    (TypeError(), False),
])
def test_error_classification(exc, retryable):
    assert is_retryable(exc) is retryable


def test_retryable_failure_schedules_retry_with_backoff():
    outcome = policy.decide_failure(RetryableError(), attempts=2, max_attempts=5, backoff=lambda n: n * 10)
    assert outcome == policy.Outcome(policy.RETRY, delay=20)


def test_last_attempt_goes_dead():
    outcome = policy.decide_failure(RetryableError(), attempts=5, max_attempts=5)
    assert outcome.action == policy.DEAD
    assert "exhausted" in outcome.reason


def test_permanent_error_skips_retries():
    outcome = policy.decide_failure(ValueError(), attempts=1, max_attempts=5)
    assert outcome.action == policy.DEAD
    assert outcome.reason == "permanent error"
