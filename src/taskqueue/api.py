"""HTTP API: submit jobs, read their status, and manage the dead-letter queue."""

import hmac
import logging
import socket
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Literal

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request, Response, Security
from fastapi.security import APIKeyHeader
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest
from pydantic import BaseModel, Field
from redis.exceptions import RedisError

from . import config, logs, states
from .broker import Broker
from .db import JobStore
from .handlers import HANDLERS
from .metrics import QueueCollector

log = logging.getLogger("taskqueue.api")


class JobIn(BaseModel):
    type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int | None = Field(default=None, ge=1, le=25)
    idempotency_key: str | None = Field(default=None, max_length=128)


def _wait_for_mysql(store: JobStore, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            store.init_schema()
            return
        except Exception:
            if time.monotonic() > deadline:
                raise
            log.warning("mysql not ready; retrying")
            time.sleep(2)


def create_app(
    settings: config.Settings | None = None,
    store: JobStore | None = None,
    broker: Broker | None = None,
    register_metrics: bool = True,
) -> FastAPI:
    settings = settings or config.load()
    store = store or JobStore(settings.mysql_url)
    broker = broker or Broker(settings.redis_url, settings.key_prefix)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        _wait_for_mysql(store)
        yield

    app = FastAPI(title="Task Queue", version="0.1.0", lifespan=lifespan)
    if register_metrics:
        REGISTRY.register(QueueCollector(broker, store, active_window_seconds=settings.heartbeat_seconds * 3))

    @app.middleware("http")
    async def access_log(request: Request, call_next):
        started = time.monotonic()
        response = await call_next(request)
        if request.url.path not in ("/metrics", "/health"):
            log.info(
                "request",
                extra=logs.fields(
                    method=request.method,
                    path=request.url.path,
                    status=response.status_code,
                    duration_ms=round((time.monotonic() - started) * 1000, 2),
                ),
            )
        return response

    def _job_or_404(job_id: str) -> dict:
        job = store.get(job_id)
        if job is None:
            raise HTTPException(404, "job not found")
        return job

    api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

    def require_api_key(key: str | None = Security(api_key_header)) -> None:
        # Auth is off when API_KEY is unset (local development).
        if settings.api_key and not (key and hmac.compare_digest(key, settings.api_key)):
            raise HTTPException(401, "missing or invalid X-API-Key header")

    # Everything that reads or changes jobs needs the key; /health and /metrics
    # stay open for load balancers and Prometheus.
    jobs = APIRouter(dependencies=[Security(require_api_key)])

    @jobs.post("/jobs", status_code=201)
    def submit(body: JobIn, response: Response):
        if body.type not in HANDLERS:
            raise HTTPException(400, f"unknown job type {body.type!r}; known: {sorted(HANDLERS)}")
        job_id = str(uuid.uuid4())
        job, created = store.create(
            job_id, body.type, body.payload, body.max_attempts or settings.default_max_attempts, body.idempotency_key
        )
        if not created:
            response.status_code = 200
            return job
        try:
            broker.enqueue(job_id)
        except RedisError:
            # The job is safely recorded in MySQL; the scheduler's reconcile
            # pass will push it once Redis is reachable again.
            log.exception("enqueue failed; job will be picked up by reconcile", extra=logs.fields(job_id=job_id))
        log.info("job submitted", extra=logs.fields(job_id=job_id, job_type=body.type))
        return job

    @jobs.get("/jobs")
    def list_jobs(
        status: Literal["queued", "running", "succeeded", "dead"] | None = None,
        type: str | None = None,
        limit: int = Query(50, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ):
        return store.list_jobs(status, type, limit, offset)

    @jobs.get("/jobs/{job_id}")
    def get_job(job_id: str):
        job = _job_or_404(job_id)
        job["attempt_history"] = store.attempts(job_id)
        return job

    @jobs.get("/dead-jobs")
    def list_dead(type: str | None = None, limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0)):
        return store.list_jobs(states.DEAD, type, limit, offset)

    @jobs.post("/dead-jobs/replay")
    def replay_by_type(type: str = Query(..., description="Replay every dead job of this type")):
        replayed = []
        for job_id in store.dead_ids(type):
            if store.replay(job_id):
                broker.replay(job_id)
                replayed.append(job_id)
        log.info("bulk replay", extra=logs.fields(job_type=type, count=len(replayed)))
        return {"replayed": replayed}

    @jobs.get("/dead-jobs/{job_id}")
    def get_dead(job_id: str):
        job = _job_or_404(job_id)
        if job["status"] != states.DEAD:
            raise HTTPException(404, "job is not in the dead-letter queue")
        job["attempt_history"] = store.attempts(job_id)
        return job

    @jobs.post("/dead-jobs/{job_id}/replay")
    def replay(job_id: str):
        _job_or_404(job_id)
        if not store.replay(job_id):
            raise HTTPException(409, "job is not in the dead-letter queue")
        broker.replay(job_id)
        log.info("job replayed", extra=logs.fields(job_id=job_id))
        return store.get(job_id)

    @jobs.delete("/dead-jobs")
    def purge(type: str | None = None):
        ids = store.dead_ids(type, limit=10_000)
        purged = store.purge_dead(ids)
        broker.forget_dead(ids)
        log.warning("purged dead jobs", extra=logs.fields(job_type=type, count=purged))
        return {"purged": purged}

    app.include_router(jobs)

    @app.get("/health")
    def health(response: Response):
        checks = {}
        for name, ping in (("mysql", store.ping), ("redis", broker.ping)):
            try:
                ping()
                checks[name] = "ok"
            except Exception as exc:
                checks[name] = f"error: {type(exc).__name__}"
        ok = all(v == "ok" for v in checks.values())
        response.status_code = 200 if ok else 503
        return {"status": "ok" if ok else "unhealthy", **checks}

    @app.get("/metrics")
    def prometheus_metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


def app_factory() -> FastAPI:
    """Entry point for each uvicorn worker process, which needs its own logging setup."""
    logs.setup("api")
    return create_app()


def listen_socket(host: str, port: int) -> socket.socket:
    """A listening socket shared by the uvicorn worker processes.

    The protocol must be IPPROTO_TCP explicitly: asyncio only sets TCP_NODELAY
    on accepted connections whose socket reports proto == IPPROTO_TCP, and
    uvicorn's own bind_socket() leaves it 0. Without TCP_NODELAY, Nagle's
    algorithm plus delayed ACKs stall every keep-alive response by ~40 ms
    (found in the Milestone 6 load test; see the README)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.set_inheritable(True)
    return sock


def main() -> None:
    import os

    import uvicorn
    from uvicorn.supervisors import Multiprocess

    workers = int(os.environ.get("API_WORKERS", "4"))
    config = uvicorn.Config(
        "taskqueue.api:app_factory",
        factory=True,
        host="0.0.0.0",
        port=8000,
        workers=workers,
        log_config=None,
        access_log=False,
    )
    server = uvicorn.Server(config)
    if workers > 1:
        Multiprocess(config, target=server.run, sockets=[listen_socket(config.host, config.port)]).run()
    else:
        server.run()


if __name__ == "__main__":
    main()
