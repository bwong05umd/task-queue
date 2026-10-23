# Task Queue

Mini Celery/Sidekiq in Python: FastAPI API, workers, scheduler (reaper, retry
promoter, reconciler), Redis broker, MySQL source of truth. Build plan lives in
the "Task Queue Build Plan" Claude Doc; Milestones 1-5 are done, 6 (load
testing) and 7 (CI, deploy, README polish) are next.

- Run: `docker compose up -d` (needs `.env`, see `.env.example`)
- Tests: `docker compose --profile test run --rm tests`; drills: `python3 scripts/chaos_test.py`
- Code: `src/taskqueue/`; Lua scripts in `broker.py`; all SQL in `db.py`
- Invariants: MySQL wins over Redis; every post-claim write is fenced on
  `run_token`; workers write MySQL before Redis. See `docs/design.md`.
- Never commit `.env`.
