"""Structured JSON logging. Fields set with log_context() go on every line."""

import contextvars
import json
import logging
import os
import sys
from contextlib import contextmanager
from datetime import UTC, datetime

_context: contextvars.ContextVar[dict | None] = contextvars.ContextVar("log_context", default=None)
_static: dict = {}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
            **_static,
            **(_context.get() or {}),
            **getattr(record, "fields", {}),
        }
        if record.exc_info:
            data["exc"] = self.formatException(record.exc_info)
        return json.dumps(data, default=str)


@contextmanager
def log_context(**fields):
    token = _context.set({**(_context.get() or {}), **fields})
    try:
        yield
    finally:
        _context.reset(token)


def setup(service: str, **static_fields) -> None:
    _static.update(service=service, **static_fields)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())


def fields(**kw) -> dict:
    """Usage: log.info("msg", extra=fields(delay=2.5))"""
    return {"fields": kw}
