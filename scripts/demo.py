#!/usr/bin/env python3
"""Narrated demo against the local Compose stack (good for a screen recording):

  1. submit a batch of jobs and watch them finish
  2. start a long job, docker kill its worker, watch another worker finish it
  3. a job hits a downstream outage, retries with backoff and lands in the
     dead-letter queue; inspect it, end the outage, replay it, watch it succeed

    docker compose up -d
    python3 scripts/demo.py            # pauses between steps
    python3 scripts/demo.py --no-pause

Uses only the standard library."""

import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from chaos_test import call, get, submit  # noqa: E402

PAUSE = "--no-pause" not in sys.argv
BOLD, DIM, GREEN, RED, RESET = "\033[1m", "\033[2m", "\033[32m", "\033[31m", "\033[0m"


def step(title: str) -> None:
    print(f"\n{BOLD}== {title}{RESET}")
    if PAUSE:
        input(f"{DIM}(press Enter){RESET}")


def say(msg: str) -> None:
    print(f"   {msg}", flush=True)


def redis(*args: str) -> None:
    subprocess.run(["docker", "compose", "exec", "-T", "redis", "redis-cli", *args], check=True, capture_output=True)


def watch(job_id: str, until: set[str], timeout: float = 90) -> dict:
    last = None
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = get(job_id)
        state = (job["status"], job["attempts"])
        if state != last:
            colour = GREEN if job["status"] == "succeeded" else RED if job["status"] == "dead" else ""
            err = f"  {DIM}{job['last_error']}{RESET}" if job["last_error"] and job["status"] != "succeeded" else ""
            say(f"{colour}{job['status']:<9}{RESET} attempt {job['attempts']}{err}")
            last = state
        if job["status"] in until:
            return job
        time.sleep(0.3)
    raise SystemExit(f"timed out waiting for {job_id}")


def main() -> None:
    step("1. Submit 30 jobs (emails, image resizes, sleeps, flaky jobs that retry)")
    kinds = [
        ("email.send", {"to": "ada@example.com"}),
        ("image.resize", {"width": 1920, "height": 1080, "target_width": 640}),
        ("sleep", {"seconds": 0.5}),
        ("flaky", {"fail_times": 1}),
    ]
    ids = [submit(*kinds[i % len(kinds)]) for i in range(30)]
    say(f"submitted {len(ids)} jobs")
    while True:
        statuses = [get(i)["status"] for i in ids]
        done = statuses.count("succeeded")
        say(f"{done}/{len(ids)} succeeded, {statuses.count('queued')} queued, {statuses.count('running')} running")
        if done == len(ids):
            break
        time.sleep(1)
    retried = sum(1 for i in ids if get(i)["attempts"] > 1)
    say(f"{GREEN}all done{RESET}; {retried} flaky jobs needed a retry")

    step("2. Kill a worker in the middle of a job")
    job_id = submit("sleep", {"seconds": 8})
    while get(job_id)["status"] != "running":
        time.sleep(0.2)
    worker = get(job_id)["attempt_history"][-1]["worker_id"]
    container = worker.rsplit("-", 1)[0]
    say(f"job {job_id[:8]} is running on worker {worker}")
    say(f"{RED}docker kill {container}{RESET}")
    subprocess.run(["docker", "kill", container], check=True, capture_output=True)
    say("the lease stops being renewed; the reaper requeues the job once it expires (~15s)")
    job = watch(job_id, {"succeeded"})
    for a in job["attempt_history"]:
        say(f"attempt {a['attempt']}: {a['outcome']:<9} on {a['worker_id']}")
    subprocess.run(["docker", "compose", "up", "-d", "--scale", "worker=3", "worker"], check=True, capture_output=True)
    say("replacement worker started")

    step("3. Downstream outage: retries with backoff, then the dead-letter queue")
    redis("SET", "tq:outage", "1")
    say("downstream service is now DOWN")
    job_id = submit("external.call", {"endpoint": "https://example.com/hook"}, max_attempts=3)
    watch(job_id, {"dead"})
    dead = call("GET", f"/dead-jobs/{job_id}")
    say(f"in the DLQ after {dead['attempts']} attempts; history:")
    for a in dead["attempt_history"]:
        say(f"  attempt {a['attempt']}: {a['outcome']:<6} {a['error']}")

    step("4. Fix the outage and replay the dead job")
    redis("DEL", "tq:outage")
    say("downstream service is back UP")
    say(f"POST /dead-jobs/{job_id[:8]}.../replay")
    call("POST", f"/dead-jobs/{job_id}/replay")
    job = watch(job_id, {"succeeded", "dead"})
    say(f"{GREEN}result: {job['result']}{RESET}")
    print(f"\n{BOLD}Demo complete.{RESET} Grafana: http://localhost:3000\n")


if __name__ == "__main__":
    main()
