"""MySQL access. MySQL is the source of truth for job state; Redis only holds
what is waiting to run.

Every write a worker makes after claiming a job is fenced on `run_token`: a
claim issues a new token, so a stale worker whose lease expired (and whose job
was picked up by someone else) can no longer overwrite the newer result."""

import json

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.exc import IntegrityError

from . import states

SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS jobs (
      id CHAR(36) NOT NULL PRIMARY KEY,
      type VARCHAR(64) NOT NULL,
      payload JSON NOT NULL,
      status VARCHAR(16) NOT NULL,
      attempts INT NOT NULL DEFAULT 0,
      max_attempts INT NOT NULL,
      next_run_at DATETIME(3) NULL,
      run_token CHAR(32) NULL,
      idempotency_key VARCHAR(128) NULL,
      result JSON NULL,
      last_error TEXT NULL,
      created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
      updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
      started_at DATETIME(3) NULL,
      finished_at DATETIME(3) NULL,
      UNIQUE KEY uq_jobs_idempotency_key (idempotency_key),
      KEY ix_jobs_status_updated (status, updated_at),
      KEY ix_jobs_type_status (type, status)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS job_attempts (
      id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
      job_id CHAR(36) NOT NULL,
      attempt INT NOT NULL,
      worker_id VARCHAR(128) NOT NULL,
      run_token CHAR(32) NOT NULL,
      started_at DATETIME(3) NOT NULL,
      finished_at DATETIME(3) NULL,
      outcome VARCHAR(16) NULL,
      error TEXT NULL,
      traceback TEXT NULL,
      KEY ix_job_attempts_job (job_id, attempt)
    )
    """,
)

_CLAIMABLE = ", ".join(f"'{s}'" for s in states.CLAIMABLE)
_JOB_COLUMNS = (
    "id, type, payload, status, attempts, max_attempts, next_run_at, idempotency_key, "
    "result, last_error, created_at, updated_at, started_at, finished_at"
)


def _job(row) -> dict:
    job = dict(row._mapping)
    for key in ("payload", "result"):
        if isinstance(job.get(key), (str, bytes)):
            job[key] = json.loads(job[key])
    return job


class JobStore:
    def __init__(self, url: str):
        self.engine = create_engine(url, pool_pre_ping=True, pool_recycle=1800, pool_size=10, max_overflow=10)

    def init_schema(self) -> None:
        with self.engine.begin() as c:
            for ddl in SCHEMA:
                c.execute(text(ddl))

    def ping(self) -> None:
        with self.engine.connect() as c:
            c.execute(text("SELECT 1"))

    # --- API side -----------------------------------------------------------

    def create(
        self, job_id: str, job_type: str, payload: dict, max_attempts: int, idempotency_key: str | None = None
    ) -> tuple[dict, bool]:
        """Insert a queued job. Returns (job, created); with an idempotency key
        that was already used, returns the existing job and created=False."""
        try:
            with self.engine.begin() as c:
                c.execute(
                    text(
                        "INSERT INTO jobs (id, type, payload, status, max_attempts, idempotency_key) "
                        "VALUES (:id, :type, :payload, :status, :max_attempts, :idem)"
                    ),
                    {
                        "id": job_id,
                        "type": job_type,
                        "payload": json.dumps(payload),
                        "status": states.QUEUED,
                        "max_attempts": max_attempts,
                        "idem": idempotency_key,
                    },
                )
        except IntegrityError:
            existing = idempotency_key and self.get_by_idempotency_key(idempotency_key)
            if not existing:
                raise
            return existing, False
        return self.get(job_id), True

    def get(self, job_id: str) -> dict | None:
        with self.engine.connect() as c:
            row = c.execute(text(f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = :id"), {"id": job_id}).first()
        return _job(row) if row else None

    def get_by_idempotency_key(self, key: str) -> dict | None:
        with self.engine.connect() as c:
            row = c.execute(text(f"SELECT {_JOB_COLUMNS} FROM jobs WHERE idempotency_key = :k"), {"k": key}).first()
        return _job(row) if row else None

    def attempts(self, job_id: str) -> list[dict]:
        with self.engine.connect() as c:
            rows = c.execute(
                text(
                    "SELECT attempt, worker_id, started_at, finished_at, outcome, error, traceback "
                    "FROM job_attempts WHERE job_id = :id ORDER BY id"
                ),
                {"id": job_id},
            )
            return [dict(r._mapping) for r in rows]

    def list_jobs(
        self, status: str | None = None, job_type: str | None = None, limit: int = 50, offset: int = 0
    ) -> list[dict]:
        where, params = ["1 = 1"], {"limit": limit, "offset": offset}
        if status:
            where.append("status = :status")
            params["status"] = status
        if job_type:
            where.append("type = :type")
            params["type"] = job_type
        sql = (
            f"SELECT {_JOB_COLUMNS} FROM jobs WHERE {' AND '.join(where)} "
            "ORDER BY updated_at DESC LIMIT :limit OFFSET :offset"
        )
        with self.engine.connect() as c:
            return [_job(r) for r in c.execute(text(sql), params)]

    def counts_by_status(self) -> dict[str, int]:
        with self.engine.connect() as c:
            rows = c.execute(text("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status"))
            return {r.status: r.n for r in rows}

    # --- worker side --------------------------------------------------------

    def claim(self, job_id: str, token: str, worker_id: str) -> dict | None:
        """Mark the job running under a new run token and open an attempt row.
        Returns None if the job is already finished (a duplicate delivery)."""
        with self.engine.begin() as c:
            res = c.execute(
                text(
                    f"UPDATE jobs SET status = '{states.RUNNING}', attempts = attempts + 1, run_token = :token, "
                    f"next_run_at = NULL, started_at = NOW(3) WHERE id = :id AND status IN ({_CLAIMABLE})"
                ),
                {"id": job_id, "token": token},
            )
            if res.rowcount == 0:
                return None
            # An attempt still open here belonged to a worker that lost its lease.
            c.execute(
                text(
                    "UPDATE job_attempts SET outcome = 'lost', finished_at = NOW(3), "
                    "error = 'lease expired before the attempt finished' "
                    "WHERE job_id = :id AND finished_at IS NULL"
                ),
                {"id": job_id},
            )
            job = _job(
                c.execute(
                    text(
                        f"SELECT {_JOB_COLUMNS}, TIMESTAMPDIFF(MICROSECOND, created_at, NOW(3)) AS age_us "
                        "FROM jobs WHERE id = :id"
                    ),
                    {"id": job_id},
                ).one()
            )
            c.execute(
                text(
                    "INSERT INTO job_attempts (job_id, attempt, worker_id, run_token, started_at) "
                    "VALUES (:id, :attempt, :worker, :token, NOW(3))"
                ),
                {"id": job_id, "attempt": job["attempts"], "worker": worker_id, "token": token},
            )
            return job

    def status_of(self, job_id: str) -> str | None:
        with self.engine.connect() as c:
            return c.execute(text("SELECT status FROM jobs WHERE id = :id"), {"id": job_id}).scalar()

    def _finish(
        self,
        job_id: str,
        token: str,
        assignments: str,
        params: dict,
        outcome: str,
        error: str | None = None,
        tb: str | None = None,
    ) -> bool:
        with self.engine.begin() as c:
            res = c.execute(
                text(
                    f"UPDATE jobs SET {assignments}, run_token = NULL "
                    f"WHERE id = :id AND run_token = :token AND status = '{states.RUNNING}'"
                ),
                {"id": job_id, "token": token, **params},
            )
            if res.rowcount == 0:
                return False
            c.execute(
                text(
                    "UPDATE job_attempts SET outcome = :outcome, error = :error, traceback = :tb, "
                    "finished_at = NOW(3) WHERE job_id = :id AND run_token = :token"
                ),
                {"id": job_id, "token": token, "outcome": outcome, "error": error, "tb": tb},
            )
            return True

    def complete(self, job_id: str, token: str, result) -> bool:
        return self._finish(
            job_id,
            token,
            f"status = '{states.SUCCEEDED}', result = :result, finished_at = NOW(3)",
            {"result": json.dumps(result)},
            "succeeded",
        )

    def schedule_retry(self, job_id: str, token: str, delay_seconds: float, error: str, tb: str) -> bool:
        return self._finish(
            job_id,
            token,
            f"status = '{states.QUEUED}', next_run_at = NOW(3) + INTERVAL :delay_us MICROSECOND, last_error = :error",
            {"delay_us": int(delay_seconds * 1_000_000), "error": error},
            "retry",
            error,
            tb,
        )

    def mark_dead(self, job_id: str, token: str, error: str, tb: str | None) -> bool:
        return self._finish(
            job_id,
            token,
            f"status = '{states.DEAD}', last_error = :error, finished_at = NOW(3)",
            {"error": error},
            "dead",
            error,
            tb,
        )

    # --- dead-letter queue --------------------------------------------------

    def dead_ids(self, job_type: str | None = None, limit: int = 1000) -> list[str]:
        sql = f"SELECT id FROM jobs WHERE status = '{states.DEAD}'"
        params: dict = {"limit": limit}
        if job_type:
            sql += " AND type = :type"
            params["type"] = job_type
        with self.engine.connect() as c:
            return [r.id for r in c.execute(text(sql + " ORDER BY finished_at LIMIT :limit"), params)]

    def replay(self, job_id: str) -> bool:
        """Move a dead job back to queued with its attempts reset. History is kept."""
        with self.engine.begin() as c:
            res = c.execute(
                text(
                    f"UPDATE jobs SET status = '{states.QUEUED}', attempts = 0, next_run_at = NULL, "
                    f"finished_at = NULL, run_token = NULL WHERE id = :id AND status = '{states.DEAD}'"
                ),
                {"id": job_id},
            )
            return res.rowcount == 1

    def purge_dead(self, ids: list[str]) -> int:
        if not ids:
            return 0
        with self.engine.begin() as c:
            res = c.execute(
                text(f"DELETE FROM jobs WHERE status = '{states.DEAD}' AND id IN :ids").bindparams(
                    bindparam("ids", expanding=True)
                ),
                {"ids": ids},
            )
            c.execute(
                text("DELETE FROM job_attempts WHERE job_id IN :ids").bindparams(bindparam("ids", expanding=True)),
                {"ids": ids},
            )
            return res.rowcount

    # --- reconciliation -----------------------------------------------------

    def stale_unfinished(self, older_than_seconds: float, limit: int = 500) -> list[dict]:
        """Unfinished jobs untouched for a while. The scheduler checks each one is
        still somewhere in Redis and re-pushes any that were lost (e.g. the API
        died between the MySQL insert and the Redis push)."""
        with self.engine.connect() as c:
            rows = c.execute(
                text(
                    "SELECT id, status, GREATEST(0, COALESCE(TIMESTAMPDIFF(MICROSECOND, NOW(3), next_run_at), 0)) "
                    f"AS delay_us FROM jobs WHERE status IN ({_CLAIMABLE}) "
                    "AND updated_at < NOW(3) - INTERVAL :s SECOND ORDER BY updated_at LIMIT :limit"
                ),
                {"s": int(older_than_seconds), "limit": limit},
            )
            return [dict(r._mapping) for r in rows]
