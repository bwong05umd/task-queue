#!/usr/bin/env python3
"""Load-test runner: resets the stack, scales workers, runs a k6 scenario
against the API, waits for the queue to drain, then measures the worker side
from MySQL. Each run appends one JSON line to loadtest/results/results.jsonl.

    docker compose up -d
    python3 scripts/loadtest.py burst  --jobs 3000 --workers 1 5 10
    python3 scripts/loadtest.py steady --rate 150 --duration 3m --workers 5
    python3 scripts/loadtest.py mixed  --rate 100 --duration 2m --fail-pct 10 --workers 5
    python3 scripts/loadtest.py report           # markdown tables from results.jsonl

WARNING: every run deletes all jobs from the dev database and Redis first, so
measurements start from an empty queue. Do not point it at data you need.
Uses only the standard library."""

import argparse
import json
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "loadtest" / "results" / "results.jsonl"
METRICS_URL = "http://localhost:8000/metrics"


def sh(*args: str, check: bool = True) -> str:
    out = subprocess.run(args, cwd=ROOT, capture_output=True, text=True)
    if check and out.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed:\n{out.stderr[-2000:]}")
    return out.stdout


def mysql(sql: str) -> list[list[str]]:
    # The SQL travels as $0 so the shell never re-parses it.
    out = sh(
        "docker",
        "compose",
        "exec",
        "-T",
        "mysql",
        "sh",
        "-c",
        'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -B -N -e "$0" 2>/dev/null',
        sql,
    )
    return [line.split("\t") for line in out.splitlines() if line]


def redis(*args: str) -> str:
    return sh("docker", "compose", "exec", "-T", "redis", "redis-cli", *args).strip()


def metric(name: str) -> float | None:
    with urllib.request.urlopen(METRICS_URL, timeout=10) as r:
        for line in r.read().decode().splitlines():
            if line.startswith(name + " ") or line.startswith(name + "{"):
                return float(line.rsplit(" ", 1)[1])
    return None


def reset() -> None:
    mysql("DELETE FROM job_attempts; DELETE FROM jobs;")
    redis("DEL", "tq:queue", "tq:processing", "tq:delayed", "tq:dead")


def scale_workers(n: int) -> None:
    sh("docker", "compose", "up", "-d", "--scale", f"worker={n}", "--no-recreate", "worker")
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if metric("tq_active_workers") == n:
            return
        time.sleep(1)
    raise RuntimeError(f"expected {n} active workers, saw {metric('tq_active_workers')}")


class DepthSampler(threading.Thread):
    """Samples the ready-queue length twice a second while a run is going."""

    def __init__(self):
        super().__init__(daemon=True)
        self.samples: list[int] = []
        self.stop = threading.Event()

    def run(self):
        while not self.stop.wait(0.5):
            try:
                self.samples.append(int(redis("LLEN", "tq:queue")))
            except (RuntimeError, ValueError):
                pass


def run_k6(env: dict) -> dict:
    args = ["docker", "compose", "--profile", "load", "run", "--rm", "-T", "k6", "run", "-q"]
    for k, v in env.items():
        args += ["-e", f"{k}={v}"]
    out = subprocess.run([*args, "/scripts/enqueue.js"], cwd=ROOT, capture_output=True, text=True)
    summary = next((line for line in reversed(out.stdout.splitlines()) if line.startswith("{")), None)
    if summary is None:
        raise RuntimeError(f"k6 produced no summary:\n{out.stderr[-2000:]}")
    return json.loads(summary)


def wait_for_drain(timeout: float) -> float:
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        pending = int(mysql("SELECT COUNT(*) FROM jobs WHERE status IN ('queued', 'running')")[0][0])
        if pending == 0:
            return time.monotonic() - started
        time.sleep(1)
    raise RuntimeError("queue did not drain in time")


def pct(values: list[float], p: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p / 100 * (len(values) - 1))))]


def measure_workers() -> dict:
    counts = {s: int(n) for s, n in mysql("SELECT status, COUNT(*) FROM jobs GROUP BY status")}
    e2e = [
        float(r[0])
        for r in mysql(
            "SELECT TIMESTAMPDIFF(MICROSECOND, created_at, finished_at) / 1e6 FROM jobs "
            "WHERE status = 'succeeded' ORDER BY created_at"
        )
    ]
    window = mysql(
        "SELECT TIMESTAMPDIFF(MICROSECOND, MIN(started_at), MAX(finished_at)) / 1e6, COUNT(*) " "FROM job_attempts"
    )[0]
    processing_seconds, attempts = float(window[0] or 0), int(window[1])
    # e2e by submission order: later jobs sat behind a deeper queue.
    fifths = [e2e[i * len(e2e) // 5 : (i + 1) * len(e2e) // 5] for i in range(5)] if len(e2e) >= 5 else []
    return {
        "status_counts": counts,
        "attempts": attempts,
        "processing_seconds": round(processing_seconds, 2),
        "worker_jobs_per_sec": round(counts.get("succeeded", 0) / processing_seconds, 1)
        if processing_seconds
        else None,
        "e2e_p50": pct(e2e, 50),
        "e2e_p95": pct(e2e, 95),
        "e2e_p99": pct(e2e, 99),
        "e2e_p50_by_fifth": [round(statistics.median(f), 3) for f in fifths if f],
    }


def k6_numbers(summary: dict) -> dict:
    m = summary["metrics"]
    d = m["http_req_duration"]["values"]
    return {
        "requests": int(m["http_reqs"]["values"]["count"]),
        "enqueue_per_sec": round(m["http_reqs"]["values"]["rate"], 1),
        "failed_rate": m["http_req_failed"]["values"]["rate"],
        "api_p50_ms": round(d["med"], 2),
        "api_p95_ms": round(d["p(95)"], 2),
        "api_p99_ms": round(d["p(99)"], 2),
    }


def run_once(kind: str, workers: int, env: dict, label: str, drain_timeout: float) -> dict:
    print(f"\n== {kind} | workers={workers} | {env} | {label}", flush=True)
    reset()
    scale_workers(workers)
    sampler = DepthSampler()
    sampler.start()
    started = time.monotonic()
    summary = run_k6(env)
    k6_seconds = time.monotonic() - started
    drain = wait_for_drain(drain_timeout)
    sampler.stop.set()
    result = {
        "at": datetime.now(UTC).isoformat(timespec="seconds"),
        "label": label,
        "kind": kind,
        "workers": workers,
        "env": env,
        "k6_seconds": round(k6_seconds, 1),
        "drain_after_k6_seconds": round(drain, 1),
        "max_queue_depth": max(sampler.samples, default=0),
        **k6_numbers(summary),
        **measure_workers(),
    }
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS.open("a") as f:
        f.write(json.dumps(result) + "\n")
    print(json.dumps(result, indent=2), flush=True)
    return result


def report() -> None:
    rows = [json.loads(line) for line in RESULTS.read_text().splitlines() if line.strip()]
    fmt = lambda v, nd=2: "–" if v is None else f"{v:.{nd}f}" if isinstance(v, float) else str(v)  # noqa: E731
    print(
        "| label | scenario | workers | jobs | enqueue/s | API p50 / p95 / p99 ms | worker jobs/s "
        "| e2e p50 / p95 / p99 s | max depth | outcome |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        scen = r["kind"] + ("" if r["kind"] == "burst" else f" {r['env'].get('RATE')}/s {r['env'].get('DURATION')}")
        outcome = ", ".join(f"{k} {v}" for k, v in sorted(r["status_counts"].items()))
        print(
            f"| {r['label']} | {scen} | {r['workers']} | {r['requests']} | {fmt(r['enqueue_per_sec'], 1)} "
            f"| {fmt(r['api_p50_ms'], 1)} / {fmt(r['api_p95_ms'], 1)} / {fmt(r['api_p99_ms'], 1)} "
            f"| {fmt(r['worker_jobs_per_sec'], 1)} "
            f"| {fmt(r['e2e_p50'])} / {fmt(r['e2e_p95'])} / {fmt(r['e2e_p99'])} "
            f"| {r['max_queue_depth']} | {outcome} |"
        )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("kind", choices=["burst", "steady", "mixed", "report"])
    ap.add_argument("--workers", type=int, nargs="+", default=[3])
    ap.add_argument("--jobs", type=int, default=3000)
    ap.add_argument("--vus", type=int, default=50)
    ap.add_argument("--rate", type=int, default=100)
    ap.add_argument("--duration", default="3m")
    ap.add_argument("--fail-pct", type=float, default=10)
    ap.add_argument("--job-type", default="image.resize")
    ap.add_argument("--label", default="baseline")
    ap.add_argument("--drain-timeout", type=float, default=900)
    a = ap.parse_args()

    if a.kind == "report":
        report()
        return
    env = {"JOB_TYPE": a.job_type}
    if a.kind == "burst":
        env |= {"SCENARIO": "burst", "JOBS": a.jobs, "VUS": a.vus}
    else:
        env |= {"SCENARIO": "steady", "RATE": a.rate, "DURATION": a.duration}
        if a.kind == "mixed":
            env["FAIL_PCT"] = a.fail_pct
    for n in a.workers:
        run_once(a.kind, n, env, a.label, a.drain_timeout)


if __name__ == "__main__":
    sys.exit(main())
