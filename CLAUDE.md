# Task Queue

Mini Celery/Sidekiq in Python: FastAPI API, workers, scheduler (reaper, retry
promoter, reconciler), Redis broker, MySQL source of truth. Milestones 1-7
of the build plan are done; stretch goals (priorities, recurring jobs, rate
limits, admin dashboard) are next.

- Run: `docker compose up -d` (needs `.env`, see `.env.example`)
- Tests: `docker compose --profile test run --rm tests`; drills: `python3 scripts/chaos_test.py`
- Lint: ruff (`ruff check . && ruff format --check .`); CI in `.github/workflows/ci.yml`
- Demo: `python3 scripts/demo.py`; deploy: `DEPLOY_HOST=user@vm scripts/deploy.sh` (see README)
- Load tests: `python3 scripts/loadtest.py ...` (wipes dev jobs first); results in README
- Code: `src/taskqueue/`; Lua scripts in `broker.py`; all SQL in `db.py`
- Invariants: MySQL wins over Redis; every post-claim write is fenced on
  `run_token`; workers write MySQL before Redis. See `docs/design.md`.
- Never commit `.env`.
