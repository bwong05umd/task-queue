import pytest
from fastapi.testclient import TestClient

from taskqueue.api import create_app


@pytest.fixture
def client(settings, store, broker):
    with TestClient(create_app(settings, store, broker, register_metrics=False)) as c:
        yield c


def test_submit_and_get(client, worker):
    r = client.post("/jobs", json={"type": "sleep", "payload": {"seconds": 0}})
    assert r.status_code == 201
    job_id = r.json()["id"]
    assert r.json()["status"] == "queued"
    worker.process_one()
    body = client.get(f"/jobs/{job_id}").json()
    assert body["status"] == "succeeded"
    assert len(body["attempt_history"]) == 1


def test_unknown_type_rejected(client):
    r = client.post("/jobs", json={"type": "nope"})
    assert r.status_code == 400


def test_missing_job_is_404(client):
    assert client.get("/jobs/does-not-exist").status_code == 404


def test_idempotency_key_returns_existing_job(client, broker):
    body = {"type": "sleep", "idempotency_key": "order-42"}
    first = client.post("/jobs", json=body)
    second = client.post("/jobs", json=body)
    assert (first.status_code, second.status_code) == (201, 200)
    assert first.json()["id"] == second.json()["id"]
    assert broker.depths()["queue"] == 1


def test_dead_job_endpoints(client, worker):
    job_id = client.post("/jobs", json={"type": "poison", "max_attempts": 1}).json()["id"]
    worker.process_one()
    dead = client.get("/dead-jobs").json()
    assert [j["id"] for j in dead] == [job_id]
    assert client.get(f"/dead-jobs/{job_id}").json()["attempt_history"][0]["outcome"] == "dead"

    r = client.post(f"/dead-jobs/{job_id}/replay")
    assert r.status_code == 200 and r.json()["status"] == "queued"
    assert client.post(f"/dead-jobs/{job_id}/replay").status_code == 409

    worker.process_one()
    assert client.post("/dead-jobs/replay", params={"type": "poison"}).json() == {"replayed": [job_id]}
    worker.process_one()
    assert client.delete("/dead-jobs").json() == {"purged": 1}
    assert client.get(f"/jobs/{job_id}").status_code == 404


def test_health(client):
    assert client.get("/health").json() == {"status": "ok", "mysql": "ok", "redis": "ok"}
