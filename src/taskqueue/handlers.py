"""Sample job handlers. A handler takes (payload, ctx) and returns a JSON-able
result, or raises. Delivery is at-least-once, so handlers with side effects
dedupe on ctx.job_id."""

import hashlib
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Callable

import redis

from .errors import PermanentError, RetryableError

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobContext:
    job_id: str
    attempt: int
    redis: redis.Redis
    key_prefix: str


Handler = Callable[[dict, JobContext], object]
HANDLERS: dict[str, Handler] = {}


def handler(name: str):
    def register(fn: Handler) -> Handler:
        HANDLERS[name] = fn
        return fn
    return register


@handler("email.send")
def send_email(payload: dict, ctx: JobContext):
    to = payload["to"]
    if "@" not in to:
        raise PermanentError(f"invalid address: {to!r}")
    # Idempotency: claim the send for this job id first, so a redelivered job
    # does not email the same person twice.
    if not ctx.redis.set(f"{ctx.key_prefix}idem:email:{ctx.job_id}", "1", nx=True, ex=7 * 86400):
        return {"skipped": "already sent for this job"}
    time.sleep(0.05)  # pretend to call an email provider
    log.info("email sent (stub)")
    return {"message_id": uuid.uuid4().hex, "to": to}


@handler("image.resize")
def resize_image(payload: dict, ctx: JobContext):
    width, height = int(payload["width"]), int(payload["height"])
    target = int(payload["target_width"])
    if min(width, height, target) <= 0:
        raise PermanentError("dimensions must be positive")
    # Burn a little CPU so this behaves like real work under load.
    digest = b""
    for _ in range(2000):
        digest = hashlib.sha256(digest + str(width * height).encode()).digest()
    return {"width": target, "height": round(height * target / width)}


@handler("sleep")
def sleep(payload: dict, ctx: JobContext):
    seconds = float(payload.get("seconds", 1))
    time.sleep(seconds)
    return {"slept": seconds}


@handler("flaky")
def flaky(payload: dict, ctx: JobContext):
    """Fails the first `fail_times` attempts, then succeeds."""
    fail_times = int(payload.get("fail_times", 2))
    if ctx.attempt <= fail_times:
        raise RetryableError(f"simulated transient failure {ctx.attempt}/{fail_times}")
    return {"succeeded_on_attempt": ctx.attempt}


@handler("poison")
def poison(payload: dict, ctx: JobContext):
    """Always fails with a retryable error, so it ends in the dead-letter queue."""
    raise RetryableError("poison job always fails")
