# Task Queue

A small Celery/Sidekiq-style job queue: a FastAPI service accepts jobs, a pool of
workers runs them in parallel, and the system survives worker crashes and Redis
or MySQL restarts without losing work.

**Stack:** Python 3.12 · FastAPI · Redis (broker) · MySQL (source of truth) ·
Prometheus + Grafana · Docker Compose.

```
            POST /jobs                         ┌──────────────── scheduler ────────────────┐
 client ───────────────► api ──INSERT──► MySQL │ promote due retries  delayed ─► queue      │
                          │                ▲   │ reap expired leases  processing ─► queue   │
                          └─RPUSH─► Redis  │   │ reconcile MySQL vs Redis (re-push lost)    │
                                    queue  │   └────────────────────────────────────────────┘
                                      │    │
              claim (Lua: LPOP+ZADD)  ▼    │ fenced status writes (run_token)
                               worker ×N ──┘ heartbeat extends lease in `processing`
                                      │
                     ack / retry ─► delayed / bury ─► dead
```

## Quickstart

```bash
cp .env.example .env        # then set real passwords
docker compose up -d        # api, 3 workers, scheduler, redis, mysql, prometheus, grafana
python3 scripts/submit.py 50
curl localhost:8000/jobs?limit=5
```

- API docs: http://localhost:8000/docs
- Grafana: http://localhost:3000 (dashboard "Task Queue"; anonymous view, admin password in `.env`)
- Prometheus: http://localhost:9090

Scale workers with `docker compose up -d --scale worker=5`.

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | Submit `{type, payload, max_attempts?, idempotency_key?}`. 201 new, 200 if the idempotency key was already used |
| `GET` | `/jobs/{id}` | Status, result, last error, and full `attempt_history` |
| `GET` | `/jobs?status=&type=` | List jobs |
| `GET` | `/dead-jobs?type=` | List the dead-letter queue |
| `GET` | `/dead-jobs/{id}` | Inspect one dead job with every attempt's error, traceback, worker and time |
| `POST` | `/dead-jobs/{id}/replay` | Requeue with attempts reset |
| `POST` | `/dead-jobs/replay?type=` | Bulk replay by type |
| `DELETE` | `/dead-jobs?type=` | Purge dead jobs |
| `GET` | `/health`, `/metrics` | Health (checks MySQL + Redis) and Prometheus metrics |

Sample job types: `email.send`, `image.resize`, `sleep`, `flaky` (fails the first
`fail_times` attempts), `poison` (always fails). Add your own in
`src/taskqueue/handlers.py` with the `@handler("name")` decorator; raise
`RetryableError` or `PermanentError` to steer retries.

## Job lifecycle

`queued → running → succeeded`, or on failure `running → queued` (retry after
backoff) until `max_attempts`, then `dead`. Permanent errors (bad payload,
unknown type, `ValueError`/`KeyError`/`TypeError`) skip retries and go straight to
`dead`. Replay moves `dead → queued`.

## Tests

```bash
docker compose --profile test run --rm tests      # unit + integration (real Redis/MySQL)
python3 scripts/chaos_test.py                     # failure drills against the running stack
```

The drills kill a worker mid-job, restart Redis, restart MySQL, and push a poison
job through the DLQ and back, and fail if any job is lost.

## Load testing (Milestone 6)

k6 drives `POST /jobs`; `scripts/loadtest.py` resets the stack, scales the
workers, runs a scenario, waits for the queue to drain and measures the worker
side from MySQL. Each run appends a line to `loadtest/results/results.jsonl`.

```bash
python3 scripts/loadtest.py burst  --jobs 5000 --workers 1 5 10
python3 scripts/loadtest.py steady --rate 150 --duration 3m --workers 5
python3 scripts/loadtest.py mixed  --rate 100 --duration 2m --fail-pct 10 --workers 5
python3 scripts/loadtest.py report      # every run as a markdown table
```

The runner **deletes all jobs** in the dev database before each run.

Setup: MacBook Air (Apple silicon), Colima VM with 4 CPUs and 6 GB of RAM, so
the API, workers, MySQL, Redis and k6 all share those 4 cores. Jobs are
`image.resize` (about 1 ms of CPU each). Burst runs use 50 concurrent clients.
All 107,999 jobs across these runs finished; none were lost.

| Scenario | Workers | Enqueue/s before → after | API p50 / p99 ms before → after | Worker jobs/s before → after | e2e p50 / p99 s before → after |
|---|---|---|---|---|---|
| Burst 5,000 | 1 | 632 → **1009** | 72 / 171 → 37 / 191 | 228 → 222 | 7.94 / 13.95 → 10.57 / 17.63 |
| Burst 5,000 | 5 | 514 → **853** | 89 / 189 → 49 / 156 | 496 → 361 | 0.43 / 0.81 → 4.05 / 7.99 |
| Burst 5,000 | 10 | 404 → **625** | 107 / 437 → 70 / 214 | 404 → 455 | 0.10 / 0.23 → 2.64 / 3.93 |
| Steady 150/s, 3 min | 5 | 150 → **150** | 2 / 24 → 3 / 28 | 150 → 150 | 0.04 / 0.10 → 0.04 / 0.10 |
| Mixed 100/s, 2 min, 10% failing | 5 | 100 → **100** | 2 / 15 → 3 / 43 | 90 → 90 | 0.05 / 2.71 → 0.05 / 2.73 |

**Finding the bottleneck.** In the baseline, adding workers made enqueue
throughput *fall* (632 → 404/s) while workers stayed idle and only kept pace
with the enqueue rate. Container CPU during a 10-worker burst showed the API
pinned at about one core (one Python process), with MySQL around 60% and Redis
under 10%.

**The fix, and a trap found along the way.** Running the API as 4 uvicorn
processes (`API_WORKERS`) first made things *worse*: every keep-alive request
took about 46 ms, even a 400 error that touches no database. Bisecting showed
that one process answered in 0.5 ms and two processes in 46 ms. uvicorn's
multi-process mode creates the shared listening socket with protocol 0, and
asyncio only turns on `TCP_NODELAY` for sockets that report `IPPROTO_TCP`. So
every connection kept Nagle's algorithm on, and each response waited for a
delayed ACK. The API now binds that socket itself with an explicit
`IPPROTO_TCP` (`listen_socket()` in `api.py`, with a regression test), and
keep-alive latency dropped from 46 ms to 0.5 ms.

**Result.** Burst enqueue throughput went up 55–66% (to 1,009 / 853 / 625 jobs/s
for 1 / 5 / 10 workers), and median API latency fell by about a third to a
half. Steady and mixed runs were never API-bound and are unchanged.

**What limits it now.** The workers. One worker handles about 220 jobs/s (about
4.5 ms per job: a claim script, then a 4-statement claim transaction and a
2-statement finish transaction in MySQL). With the API taking more CPU, end-to-end
latency in bursts got *longer*, because jobs now arrive faster than workers can
drain them. Median end-to-end time grows with queue position: in the baseline
1-worker burst it climbed from 1.9s for the first fifth of jobs to 13.0s
for the last fifth. On 4 shared cores, 10 workers top out around 455 jobs/s. The
next steps would be fewer MySQL round trips per job (fold the attempt-row
writes into the job update, claim in batches) and more cores.

**Retries and the DLQ under load.** In the mixed run (5% flaky, 5% poison), every
flaky job succeeded on its retry and every poison job reached the dead-letter
queue after 3 attempts (592 dead, 11,406 succeeded).
p95 end-to-end latency rose to about 2.2s because of the retry backoff, while
p50 stayed at 0.05s.

## Layout

```
src/taskqueue/   api.py worker.py scheduler.py   # the three processes
                 broker.py (Redis + Lua) db.py (MySQL) policy.py (backoff, retry decisions)
                 handlers.py states.py errors.py logs.py metrics.py config.py
tests/unit/      pure logic: backoff, error classification, state transitions, log format
tests/integration/  queue, leases, fencing, retries, DLQ, reconcile, API
deploy/          mysql init, prometheus, grafana provisioning + dashboard
scripts/         submit.py, chaos_test.py, loadtest.py
loadtest/        k6 scenarios (enqueue.js) and results/results.jsonl
docs/design.md   design decisions and tradeoffs
```

Configuration is environment variables; see `src/taskqueue/config.py`
(`LEASE_SECONDS`, `HEARTBEAT_SECONDS`, `DEFAULT_MAX_ATTEMPTS`, `BACKOFF_BASE`, ...).
