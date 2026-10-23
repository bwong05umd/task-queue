"""Prometheus metrics. Workers and the scheduler count what they do and serve
/metrics and /health on a side port; the API exposes queue-wide gauges read
from Redis and MySQL at scrape time."""

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from prometheus_client.core import GaugeMetricFamily

log = logging.getLogger(__name__)

JOBS_STARTED = Counter("tq_jobs_started_total", "Job attempts started", ["type"])
JOBS_SUCCEEDED = Counter("tq_jobs_succeeded_total", "Jobs that succeeded", ["type"])
JOBS_FAILED = Counter("tq_jobs_failed_total", "Job attempts that raised", ["type"])
JOBS_RETRIED = Counter("tq_jobs_retried_total", "Failed attempts scheduled for retry", ["type"])
JOBS_DEAD = Counter("tq_jobs_dead_total", "Jobs moved to the dead-letter queue", ["type"])
JOBS_STALE = Counter("tq_jobs_stale_total", "Results discarded because the lease was lost", ["type"])
JOB_DURATION = Histogram(
    "tq_job_duration_seconds", "Handler run time", ["type", "outcome"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120),
)
LEASES_REAPED = Counter("tq_leases_reaped_total", "Expired leases put back on the queue")
RETRIES_PROMOTED = Counter("tq_retries_promoted_total", "Delayed retries moved onto the queue")
JOBS_RECONCILED = Counter("tq_jobs_reconciled_total", "Jobs re-pushed after going missing from Redis")


class QueueCollector:
    """Gauges computed on scrape: queue depths, active workers, jobs by status."""

    def __init__(self, broker, store, active_window_seconds: float):
        self.broker = broker
        self.store = store
        self.window = active_window_seconds

    def collect(self):
        try:
            depth = GaugeMetricFamily("tq_queue_depth", "Jobs in each Redis structure", labels=["queue"])
            for name, n in self.broker.depths().items():
                depth.add_metric([name], n)
            yield depth
            yield GaugeMetricFamily("tq_active_workers", "Workers with a recent heartbeat",
                                    value=self.broker.active_workers(self.window))
        except Exception:
            log.exception("could not read queue gauges from redis")
        try:
            by_status = GaugeMetricFamily("tq_jobs_by_status", "Jobs in MySQL by status", labels=["status"])
            for status, n in self.store.counts_by_status().items():
                by_status.add_metric([status], n)
            yield by_status
        except Exception:
            log.exception("could not read job counts from mysql")


def serve_side_port(port: int, health: Callable[[], tuple[bool, dict]]) -> ThreadingHTTPServer:
    """Serve /metrics and /health from a background thread."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/metrics":
                body, status, ctype = generate_latest(), 200, CONTENT_TYPE_LATEST
            elif self.path == "/health":
                ok, detail = health()
                body, status, ctype = json.dumps(detail).encode(), 200 if ok else 503, "application/json"
            else:
                body, status, ctype = b"not found", 404, "text/plain"
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, name="side-port", daemon=True).start()
    return server
