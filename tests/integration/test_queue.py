import time
import uuid

from taskqueue import states

from .conftest import drain


def status(store, job_id):
    return store.status_of(job_id)


# --- Milestone 1: basic queue ---------------------------------------------


def test_job_runs_and_succeeds(worker, store, broker, submit):
    job_id = submit("image.resize", {"width": 800, "height": 600, "target_width": 400})
    assert worker.process_one()
    job = store.get(job_id)
    assert job["status"] == states.SUCCEEDED
    assert job["result"] == {"width": 400, "height": 300}
    assert job["attempts"] == 1
    assert broker.depths() == {"queue": 0, "processing": 0, "delayed": 0, "dead": 0}


def test_empty_queue_returns_false(worker):
    assert worker.process_one() is False


def test_each_job_is_claimed_once(broker, submit):
    ids = {submit("sleep", {"seconds": 0}) for _ in range(50)}
    claimed = [broker.claim(10_000) for _ in range(60)]
    claimed = [c for c in claimed if c]
    assert sorted(claimed) == sorted(ids)


# --- Milestone 2: reliable delivery ---------------------------------------


def test_expired_lease_is_reaped_back_onto_queue(broker, scheduler, submit):
    job_id = submit("sleep")
    assert broker.claim(lease_ms=100) == job_id
    assert scheduler.tick()["reaped"] == []
    time.sleep(0.15)
    assert scheduler.tick()["reaped"] == [job_id]
    assert broker.r.lrange(broker.queue, 0, -1) == [job_id]


def test_heartbeat_keeps_lease_alive(broker, scheduler, submit):
    job_id = submit("sleep")
    broker.claim(lease_ms=150)
    for _ in range(4):
        time.sleep(0.1)
        assert broker.extend_lease(job_id, 150)
    assert scheduler.tick()["reaped"] == []


def test_crashed_worker_job_finishes_elsewhere(worker, scheduler, store, broker, submit, settings):
    job_id = submit("email.send", {"to": "a@example.com"})
    # Worker A claims the job, then "crashes": no heartbeat, no result.
    assert broker.claim(int(settings.lease_seconds * 1000)) == job_id
    stale_token = uuid.uuid4().hex
    assert store.claim(job_id, stale_token, "worker-a") is not None

    time.sleep(settings.lease_seconds + 0.1)
    drain(worker, scheduler, lambda: status(store, job_id) == states.SUCCEEDED)

    history = store.attempts(job_id)
    assert [a["outcome"] for a in history] == ["lost", "succeeded"]
    assert [a["worker_id"] for a in history] == ["worker-a", "test-worker"]
    # Worker A wakes up late: its write is fenced out.
    assert store.complete(job_id, stale_token, {"late": True}) is False
    assert store.get(job_id)["result"]["to"] == "a@example.com"


def test_duplicate_delivery_is_skipped(worker, store, broker, submit):
    job_id = submit("email.send", {"to": "a@example.com"})
    worker.process_one()
    first = store.get(job_id)
    broker.enqueue(job_id)  # e.g. redelivered after the result was saved
    assert worker.process_one()
    assert store.get(job_id) == first
    assert len(store.attempts(job_id)) == 1
    assert broker.depths()["processing"] == 0


def test_email_handler_is_idempotent(worker, store, broker, submit):
    job_id = submit("email.send", {"to": "a@example.com"})
    broker.r.set(f"{broker.prefix}idem:email:{job_id}", "1")  # sent by a previous delivery
    worker.process_one()
    assert store.get(job_id)["result"] == {"skipped": "already sent for this job"}


# --- Milestone 3: retries with backoff ------------------------------------


def test_flaky_job_retries_then_succeeds(worker, scheduler, store, broker, submit):
    job_id = submit("flaky", {"fail_times": 2}, max_attempts=5)
    worker.process_one()
    job = store.get(job_id)
    assert job["status"] == states.QUEUED
    assert job["next_run_at"] is not None
    assert "simulated transient failure 1/2" in job["last_error"]
    assert broker.depths()["delayed"] == 1

    drain(worker, scheduler, lambda: status(store, job_id) == states.SUCCEEDED)
    job = store.get(job_id)
    assert job["attempts"] == 3
    assert job["result"] == {"succeeded_on_attempt": 3}
    assert [a["outcome"] for a in store.attempts(job_id)] == ["retry", "retry", "succeeded"]


def test_retry_waits_for_backoff(worker, scheduler, broker, submit):
    submit("flaky", {"fail_times": 1})
    worker.process_one()
    assert scheduler.tick()["promoted"] == []  # backoff not elapsed yet
    time.sleep(0.2)
    assert len(scheduler.tick()["promoted"]) == 1


def test_permanent_error_is_not_retried(worker, store, submit):
    job_id = submit("sleep", {"seconds": "not-a-number"})
    worker.process_one()
    job = store.get(job_id)
    assert job["status"] == states.DEAD
    assert job["attempts"] == 1
    assert job["last_error"].startswith("permanent error: ValueError")
    assert "Traceback" in store.attempts(job_id)[0]["traceback"]


# --- Milestone 4: dead-letter queue ---------------------------------------


def test_poison_job_lands_in_dlq_exactly_once(worker, scheduler, store, broker, submit):
    job_id = submit("poison", max_attempts=3)
    drain(worker, scheduler, lambda: status(store, job_id) == states.DEAD)
    # Nothing more happens however long we keep running.
    for _ in range(20):
        scheduler.tick()
        worker.process_one()
    assert broker.r.zrange(broker.dead, 0, -1) == [job_id]
    history = store.attempts(job_id)
    assert [a["outcome"] for a in history] == ["retry", "retry", "dead"]
    assert all(a["worker_id"] and a["started_at"] and a["error"] for a in history)


def test_replay_requeues_with_attempts_reset(worker, scheduler, store, broker, submit):
    job_id = submit("poison", max_attempts=1)
    worker.process_one()
    assert store.replay(job_id)
    broker.replay(job_id)
    job = store.get(job_id)
    assert (job["status"], job["attempts"]) == (states.QUEUED, 0)
    assert broker.depths()["dead"] == 0
    assert broker.r.lrange(broker.queue, 0, -1) == [job_id]
    assert store.replay(job_id) is False  # only dead jobs can be replayed


def test_job_that_keeps_losing_its_lease_goes_dead(worker, scheduler, store, broker, submit, settings):
    job_id = submit("sleep", max_attempts=2)
    for n in range(2):
        broker.claim(50)
        store.claim(job_id, uuid.uuid4().hex, f"crashing-{n}")
        time.sleep(0.06)
        scheduler.tick()
    worker.process_one()
    job = store.get(job_id)
    assert job["status"] == states.DEAD
    assert "lease lost" in job["last_error"]


# --- Reconciliation -------------------------------------------------------


def test_reconcile_repushes_jobs_redis_lost(scheduler, store, broker, submit):
    lost = submit("sleep", push=False)  # API died before the Redis push
    tracked = submit("sleep")  # still in the queue
    time.sleep(1.1)
    assert scheduler.reconcile() == [lost]
    assert sorted(broker.r.lrange(broker.queue, 0, -1)) == sorted([lost, tracked])
    assert scheduler.reconcile() == []


def test_dead_job_succeeds_when_replayed_after_outage(worker, scheduler, store, broker, submit):
    broker.r.set(f"{broker.prefix}outage", "1")
    job_id = submit("external.call", max_attempts=2)
    drain(worker, scheduler, lambda: status(store, job_id) == states.DEAD)
    broker.r.delete(f"{broker.prefix}outage")
    assert store.replay(job_id)
    broker.replay(job_id)
    worker.process_one()
    job = store.get(job_id)
    assert job["status"] == states.SUCCEEDED
    assert [a["outcome"] for a in store.attempts(job_id)] == ["retry", "dead", "succeeded"]
