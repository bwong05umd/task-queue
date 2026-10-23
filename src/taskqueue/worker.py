"""Worker: claim a job with a lease, run its handler, record the outcome.

Lifecycle of one attempt:
  1. Redis: atomically pop the id and lease it in `processing`.
  2. MySQL: mark running under a fresh run token (fencing), open an attempt row.
  3. Run the handler while a heartbeat thread keeps extending the lease.
  4. MySQL first (fenced on the token), then Redis: ack, schedule a retry, or bury.

If the worker dies anywhere in between, the lease expires and the scheduler's
reaper puts the job back on the queue. Delivery is at-least-once."""

import functools
import logging
import signal
import threading
import time
import traceback
import uuid
from typing import Callable

from redis.exceptions import RedisError
from sqlalchemy.exc import OperationalError

from . import config, logs, metrics, policy, states
from .broker import Broker
from .db import JobStore
from .errors import PermanentError
from .handlers import HANDLERS, JobContext

log = logging.getLogger("taskqueue.worker")


class Worker:
    def __init__(self, settings: config.Settings, store: JobStore, broker: Broker,
                 handlers: dict | None = None, db_retry_seconds: float = 30.0):
        self.s = settings
        self.store = store
        self.broker = broker
        self.handlers = HANDLERS if handlers is None else handlers
        self.lease_ms = int(settings.lease_seconds * 1000)
        self.backoff: Callable[[int], float] = functools.partial(
            policy.backoff_delay, base=settings.backoff_base, cap=settings.backoff_cap)
        self.db_retry_seconds = db_retry_seconds
        self.stopping = threading.Event()
        self.current_job: str | None = None
        self.last_heartbeat = 0.0

    # --- main loop ----------------------------------------------------------

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._stop)
        signal.signal(signal.SIGINT, self._stop)
        threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True).start()
        log.info("worker started")
        while not self.stopping.is_set():
            try:
                if not self.process_one():
                    self.stopping.wait(self.s.poll_interval)
            except (RedisError, OperationalError):
                log.exception("backend unavailable; retrying shortly")
                self.stopping.wait(1.0)
            except Exception:
                log.exception("unexpected error in worker loop")
                self.stopping.wait(1.0)
        self.broker.worker_gone(self.s.worker_id)
        log.info("worker stopped")

    def _stop(self, signum, frame) -> None:
        log.info("shutdown requested; finishing the current job first")
        self.stopping.set()

    def process_one(self) -> bool:
        """Claim and run one job. Returns False if the queue was empty."""
        job_id = self.broker.claim(self.lease_ms)
        if job_id is None:
            return False
        # From here the job is leased: heartbeats extend it while we work.
        self.current_job = job_id
        try:
            with logs.log_context(job_id=job_id):
                self._process(job_id)
        finally:
            self.current_job = None
        return True

    def _process(self, job_id: str) -> None:
        token = uuid.uuid4().hex
        job = self._db(self.store.claim, job_id, token, self.s.worker_id)
        if job is None:
            # Already succeeded or dead: a duplicate delivery. Drop it.
            status = self.store.status_of(job_id)
            log.info("skipping duplicate delivery", extra=logs.fields(status=status))
            if status == states.DEAD:
                self.broker.bury(job_id)
            else:
                self.broker.ack(job_id)
            return

        with logs.log_context(attempt=job["attempts"], job_type=job["type"]):
            if job["attempts"] > job["max_attempts"]:
                # Every attempt so far lost its lease (e.g. it crashes the worker).
                self._bury(job, token, f"lease lost on all {job['max_attempts']} attempts", None)
                return
            self._execute(job, token)

    def _execute(self, job: dict, token: str) -> None:
        job_id, job_type, attempt = job["id"], job["type"], job["attempts"]
        metrics.JOBS_STARTED.labels(job_type).inc()
        log.info("job started")
        ctx = JobContext(job_id=job_id, attempt=attempt, redis=self.broker.r, key_prefix=self.s.key_prefix)
        started = time.monotonic()
        try:
            handler = self.handlers.get(job_type)
            if handler is None:
                raise PermanentError(f"unknown job type: {job_type}")
            result = handler(job["payload"], ctx)
        except Exception as exc:
            elapsed = time.monotonic() - started
            metrics.JOBS_FAILED.labels(job_type).inc()
            metrics.JOB_DURATION.labels(job_type, "failed").observe(elapsed)
            error = f"{type(exc).__name__}: {exc}"
            tb = traceback.format_exc()
            outcome = policy.decide_failure(exc, attempt, job["max_attempts"], self.backoff)
            if outcome.action == policy.RETRY:
                if self._db(self.store.schedule_retry, job_id, token, outcome.delay, error, tb):
                    self.broker.retry(job_id, int(outcome.delay * 1000))
                    metrics.JOBS_RETRIED.labels(job_type).inc()
                    log.warning("job failed; retry scheduled",
                                extra=logs.fields(error=error, delay_seconds=round(outcome.delay, 3)))
                else:
                    self._stale(job_type)
            else:
                self._bury(job, token, f"{outcome.reason}: {error}", tb)
            return

        elapsed = time.monotonic() - started
        metrics.JOB_DURATION.labels(job_type, "succeeded").observe(elapsed)
        if self._db(self.store.complete, job_id, token, result):
            self.broker.ack(job_id)
            metrics.JOBS_SUCCEEDED.labels(job_type).inc()
            log.info("job succeeded", extra=logs.fields(duration_seconds=round(elapsed, 4)))
        else:
            self._stale(job_type)

    def _bury(self, job: dict, token: str, error: str, tb: str | None) -> None:
        if self._db(self.store.mark_dead, job["id"], token, error, tb):
            self.broker.bury(job["id"])
            metrics.JOBS_DEAD.labels(job["type"]).inc()
            log.error("job moved to dead-letter queue", extra=logs.fields(error=error))
        else:
            self._stale(job["type"])

    def _stale(self, job_type: str) -> None:
        # Our lease expired and another worker re-claimed the job, so our token
        # no longer matches. Leave Redis alone: the lease now belongs to them.
        metrics.JOBS_STALE.labels(job_type).inc()
        log.warning("lease was lost; discarding this attempt's result")

    def _db(self, fn, *args):
        """Run a MySQL call, retrying through short outages (e.g. a restart).
        The heartbeat keeps the lease alive meanwhile; if MySQL stays down past
        the deadline we give up and let the lease expire."""
        deadline = time.monotonic() + self.db_retry_seconds
        delay = 0.25
        while True:
            try:
                return fn(*args)
            except OperationalError:
                if time.monotonic() > deadline:
                    raise
                log.warning("mysql unavailable; retrying", extra=logs.fields(in_seconds=delay))
                time.sleep(delay)
                delay = min(delay * 2, 5.0)

    # --- heartbeat ----------------------------------------------------------

    def _heartbeat_loop(self) -> None:
        while not self.stopping.wait(self.s.heartbeat_seconds):
            self.heartbeat()

    def heartbeat(self) -> None:
        try:
            self.broker.worker_heartbeat(self.s.worker_id)
            job_id = self.current_job
            if job_id and not self.broker.extend_lease(job_id, self.lease_ms):
                log.warning("lease already expired; job may run twice", extra=logs.fields(job_id=job_id))
            self.last_heartbeat = time.monotonic()
        except RedisError:
            log.exception("heartbeat failed")

    def health(self) -> tuple[bool, dict]:
        age = time.monotonic() - self.last_heartbeat
        ok = age < self.s.heartbeat_seconds * 3
        return ok, {"status": "ok" if ok else "unhealthy", "worker_id": self.s.worker_id,
                    "heartbeat_age_seconds": round(age, 2), "current_job": self.current_job}


def main() -> None:
    settings = config.load()
    logs.setup("worker", worker_id=settings.worker_id)
    worker = Worker(settings, JobStore(settings.mysql_url), Broker(settings.redis_url, settings.key_prefix))
    worker.heartbeat()
    metrics.serve_side_port(settings.metrics_port, worker.health)
    worker.run()


if __name__ == "__main__":
    main()
