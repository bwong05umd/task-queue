"""Integration tests run against real Redis and MySQL. In Compose:

    docker compose --profile test run --rm tests

They skip themselves when the backends are not reachable."""

import dataclasses
import time

import pytest

pytest.importorskip("redis")
pytest.importorskip("sqlalchemy")

from sqlalchemy import text  # noqa: E402

from taskqueue import config  # noqa: E402
from taskqueue.broker import Broker  # noqa: E402
from taskqueue.db import JobStore  # noqa: E402
from taskqueue.scheduler import Scheduler  # noqa: E402
from taskqueue.worker import Worker  # noqa: E402

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def settings():
    s = config.load()
    return dataclasses.replace(
        s,
        lease_seconds=0.5,
        heartbeat_seconds=0.1,
        backoff_base=0.05,
        backoff_cap=1.0,
        reconcile_after_seconds=0,
        worker_id="test-worker",
    )


@pytest.fixture(scope="session")
def _backends(settings):
    store, broker = JobStore(settings.mysql_url), Broker(settings.redis_url, settings.key_prefix)
    try:
        broker.ping()
        store.init_schema()
    except Exception as exc:
        pytest.skip(f"redis/mysql not reachable: {exc}")
    return store, broker


@pytest.fixture
def store(_backends):
    store = _backends[0]
    with store.engine.begin() as c:
        c.execute(text("DELETE FROM job_attempts"))
        c.execute(text("DELETE FROM jobs"))
    return store


@pytest.fixture
def broker(_backends):
    broker = _backends[1]
    keys = list(broker.r.scan_iter(f"{broker.prefix}*"))
    if keys:
        broker.r.delete(*keys)
    return broker


@pytest.fixture
def worker(settings, store, broker):
    return Worker(settings, store, broker, db_retry_seconds=2)


@pytest.fixture
def scheduler(settings, store, broker):
    return Scheduler(settings, store, broker)


@pytest.fixture
def submit(store, broker, settings):
    counter = iter(range(1_000_000))

    def _submit(job_type, payload=None, max_attempts=None, push=True):
        job_id = f"00000000-0000-0000-0000-{next(counter):012d}"
        store.create(job_id, job_type, payload or {}, max_attempts or settings.default_max_attempts)
        if push:
            broker.enqueue(job_id)
        return job_id

    return _submit


def drain(worker, scheduler, until, timeout=10.0):
    """Run worker + scheduler steps until `until()` is true."""
    deadline = time.monotonic() + timeout
    while not until():
        if time.monotonic() > deadline:
            raise AssertionError("condition not reached before timeout")
        scheduler.tick()
        if not worker.process_one():
            time.sleep(0.02)
