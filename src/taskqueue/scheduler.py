"""Scheduler: the housekeeping loop. Safe to run more than one copy, since
every Redis move it makes is an atomic script.

Each tick it
  - promotes retries whose backoff has elapsed (delayed -> queue),
  - reaps expired leases from dead or stalled workers (processing -> queue),
and every so often it reconciles MySQL against Redis, re-pushing unfinished
jobs that Redis lost track of (API crashed mid-enqueue, Redis lost data)."""

import logging
import signal
import threading
import time

from redis.exceptions import RedisError
from sqlalchemy.exc import OperationalError

from . import config, logs, metrics
from .broker import Broker
from .db import JobStore

log = logging.getLogger("taskqueue.scheduler")


class Scheduler:
    def __init__(self, settings: config.Settings, store: JobStore, broker: Broker):
        self.s = settings
        self.store = store
        self.broker = broker
        self.stopping = threading.Event()
        self.last_tick = 0.0
        self.last_reconcile = 0.0

    def tick(self) -> dict:
        promoted = self.broker.promote_due()
        reaped = self.broker.reap_expired()
        if promoted:
            metrics.RETRIES_PROMOTED.inc(len(promoted))
            log.info("promoted due retries", extra=logs.fields(count=len(promoted)))
        if reaped:
            metrics.LEASES_REAPED.inc(len(reaped))
            log.warning("reaped expired leases", extra=logs.fields(job_ids=reaped))
        self.last_tick = time.monotonic()
        return {"promoted": promoted, "reaped": reaped}

    def reconcile(self) -> list[str]:
        requeued = []
        for row in self.store.stale_unfinished(self.s.reconcile_after_seconds):
            if self.broker.requeue_if_unknown(row["id"], int(row["delay_us"]) // 1000):
                requeued.append(row["id"])
        if requeued:
            metrics.JOBS_RECONCILED.inc(len(requeued))
            log.warning("re-pushed jobs missing from redis", extra=logs.fields(job_ids=requeued))
        self.last_reconcile = time.monotonic()
        return requeued

    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.stopping.set())
        signal.signal(signal.SIGINT, lambda *_: self.stopping.set())
        log.info("scheduler started")
        while not self.stopping.is_set():
            try:
                self.tick()
                if time.monotonic() - self.last_reconcile >= self.s.reconcile_every_seconds:
                    self.reconcile()
            except (RedisError, OperationalError):
                log.exception("backend unavailable; retrying shortly")
            self.stopping.wait(self.s.scheduler_tick_seconds)
        log.info("scheduler stopped")

    def health(self) -> tuple[bool, dict]:
        age = time.monotonic() - self.last_tick
        ok = age < max(5.0, self.s.scheduler_tick_seconds * 10)
        return ok, {"status": "ok" if ok else "unhealthy", "last_tick_age_seconds": round(age, 2)}


def main() -> None:
    settings = config.load()
    logs.setup("scheduler")
    scheduler = Scheduler(settings, JobStore(settings.mysql_url), Broker(settings.redis_url, settings.key_prefix))
    scheduler.last_tick = time.monotonic()
    metrics.serve_side_port(settings.metrics_port, scheduler.health)
    scheduler.run()


if __name__ == "__main__":
    main()
