#!/usr/bin/env python3
"""Failure drills against the running Compose stack. Asserts no job is lost.

    docker compose up -d
    python3 scripts/chaos_test.py            # all drills
    python3 scripts/chaos_test.py kill-worker

Uses only the standard library, so it runs with any python3."""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

API = os.environ.get("API_URL", "http://localhost:8000")
HEADERS = {"Content-Type": "application/json", **({"X-API-Key": k} if (k := os.environ.get("API_KEY")) else {})}


def call(method, path, body=None):
    req = urllib.request.Request(
        API + path,
        method=method,
        data=json.dumps(body).encode() if body else None,
        headers=HEADERS,
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def submit(job_type, payload=None, **kw):
    return call("POST", "/jobs", {"type": job_type, "payload": payload or {}, **kw})["id"]


def get(job_id):
    return call("GET", f"/jobs/{job_id}")


def wait_for(cond, timeout, what):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if cond():
                return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass  # API briefly unavailable during a restart
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def all_succeeded(ids):
    return all(get(i)["status"] == "succeeded" for i in ids)


def drill_kill_worker():
    job_id = submit("sleep", {"seconds": 6})
    wait_for(lambda: get(job_id)["status"] == "running", 15, "job to start")
    victim = get(job_id)["attempt_history"][-1]["worker_id"].rsplit("-", 1)[0]  # container hostname
    print(f"  job {job_id[:8]} running on {victim}; docker kill")
    subprocess.run(["docker", "kill", victim], check=True, capture_output=True)
    wait_for(lambda: get(job_id)["status"] == "succeeded", 60, "job to finish on another worker")
    history = get(job_id)["attempt_history"]
    assert [a["outcome"] for a in history] == ["lost", "succeeded"], history
    assert history[0]["worker_id"] != history[1]["worker_id"]
    print(f"  recovered: finished on {history[1]['worker_id']} after the lease expired")
    compose("up", "-d", "--scale", "worker=3", "worker")  # replace the killed worker


def drill_restart(service):
    ids = [submit("sleep", {"seconds": 0.2}) for _ in range(60)]
    print(f"  submitted {len(ids)} jobs; restarting {service}")
    compose("restart", service)
    wait_for(lambda: all_succeeded(ids), 120, f"all jobs to succeed after {service} restart")
    print(f"  all {len(ids)} jobs succeeded")


def drill_poison_and_replay():
    job_id = submit("poison", max_attempts=3)
    wait_for(lambda: get(job_id)["status"] == "dead", 60, "poison job to reach the DLQ")
    dead = [j["id"] for j in call("GET", "/dead-jobs?type=poison&limit=500")]
    assert dead.count(job_id) == 1
    print(f"  poison job dead after {get(job_id)['attempts']} attempts; replaying")
    assert call("POST", f"/dead-jobs/{job_id}/replay")["status"] == "queued"
    wait_for(lambda: get(job_id)["status"] == "dead", 60, "replayed poison job to die again")
    print("  replayed job ran again and returned to the DLQ")


DRILLS = {
    "kill-worker": drill_kill_worker,
    "restart-redis": lambda: drill_restart("redis"),
    "restart-mysql": lambda: drill_restart("mysql"),
    "poison": drill_poison_and_replay,
}

if __name__ == "__main__":
    names = sys.argv[1:] or list(DRILLS)
    for name in names:
        print(f"[{name}]")
        DRILLS[name]()
    print("all drills passed")
