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

## Layout

```
src/taskqueue/   api.py worker.py scheduler.py   # the three processes
                 broker.py (Redis + Lua) db.py (MySQL) policy.py (backoff, retry decisions)
                 handlers.py states.py errors.py logs.py metrics.py config.py
tests/unit/      pure logic: backoff, error classification, state transitions, log format
tests/integration/  queue, leases, fencing, retries, DLQ, reconcile, API
deploy/          mysql init, prometheus, grafana provisioning + dashboard
scripts/         submit.py, chaos_test.py
docs/design.md   design decisions and tradeoffs
```

Configuration is environment variables; see `src/taskqueue/config.py`
(`LEASE_SECONDS`, `HEARTBEAT_SECONDS`, `DEFAULT_MAX_ATTEMPTS`, `BACKOFF_BASE`, ...).
