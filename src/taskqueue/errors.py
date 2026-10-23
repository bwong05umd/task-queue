class JobError(Exception):
    """Base class for errors raised by job handlers."""


class RetryableError(JobError):
    """A transient failure (timeout, 5xx, lock contention). The job is retried."""


class PermanentError(JobError):
    """A failure that retrying cannot fix (bad payload, unknown type)."""


# Built-in exceptions that almost always mean the payload is wrong.
_PERMANENT_BUILTINS = (ValueError, TypeError, KeyError)


def is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, RetryableError):
        return True
    if isinstance(exc, PermanentError):
        return False
    if isinstance(exc, _PERMANENT_BUILTINS):
        return False
    return True
