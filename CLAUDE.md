# Task Queue

Mini Celery/Sidekiq in Python: FastAPI API, workers, scheduler (reaper, retry
promoter, reconciler), Redis broker, MySQL source of truth. Build plan lives in
the "Task Queue Build Plan" Claude Doc; Milestones 1-6 are done, 7 (CI,
deploy, README polish) is next.

- Run: `docker compose up -d` (needs `.env`, see `.env.example`)
- Tests: `docker compose --profile test run --rm tests`; drills: `python3 scripts/chaos_test.py`
- Load tests: `python3 scripts/loadtest.py ...` (wipes dev jobs first); results in README
- Code: `src/taskqueue/`; Lua scripts in `broker.py`; all SQL in `db.py`
- Invariants: MySQL wins over Redis; every post-claim write is fenced on
  `run_token`; workers write MySQL before Redis. See `docs/design.md`.
- Never commit `.env`.
