import os
import socket
from dataclasses import dataclass
from urllib.parse import quote_plus


@dataclass(frozen=True)
class Settings:
    redis_url: str
    mysql_url: str
    key_prefix: str
    lease_seconds: float
    heartbeat_seconds: float
    poll_interval: float
    default_max_attempts: int
    backoff_base: float
    backoff_cap: float
    reconcile_after_seconds: float
    reconcile_every_seconds: float
    scheduler_tick_seconds: float
    metrics_port: int
    worker_id: str


def _mysql_url() -> str:
    if url := os.environ.get("MYSQL_URL"):
        return url
    user = quote_plus(os.environ.get("MYSQL_USER", "taskqueue"))
    password = quote_plus(os.environ.get("MYSQL_PASSWORD", ""))
    host = os.environ.get("MYSQL_HOST", "localhost")
    port = os.environ.get("MYSQL_PORT", "3306")
    db = os.environ.get("MYSQL_DATABASE", "taskqueue")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"


def load() -> Settings:
    env = os.environ.get
    return Settings(
        redis_url=env("REDIS_URL", "redis://localhost:6379/0"),
        mysql_url=_mysql_url(),
        key_prefix=env("KEY_PREFIX", "tq:"),
        lease_seconds=float(env("LEASE_SECONDS", "15")),
        heartbeat_seconds=float(env("HEARTBEAT_SECONDS", "5")),
        poll_interval=float(env("POLL_INTERVAL", "0.1")),
        default_max_attempts=int(env("DEFAULT_MAX_ATTEMPTS", "5")),
        backoff_base=float(env("BACKOFF_BASE", "2")),
        backoff_cap=float(env("BACKOFF_CAP", "300")),
        reconcile_after_seconds=float(env("RECONCILE_AFTER_SECONDS", "60")),
        reconcile_every_seconds=float(env("RECONCILE_EVERY_SECONDS", "30")),
        scheduler_tick_seconds=float(env("SCHEDULER_TICK_SECONDS", "0.5")),
        metrics_port=int(env("METRICS_PORT", "9100")),
        worker_id=env("WORKER_ID", f"{socket.gethostname()}-{os.getpid()}"),
    )
