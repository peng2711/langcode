"""Minimal structured logging with stable execution correlation fields."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any


_CONTEXT_FIELDS = (
    "service",
    "runtime_id",
    "agent_card_id",
    "agent_card_version",
    "thread_id",
    "task_id",
    "execution_id",
    "attempt",
    "event_id",
    "correlation_id",
)
_SENSITIVE_KEYS = {"api_key", "authorization", "command", "content", "prompt"}


class JsonFormatter(logging.Formatter):
    """Emit one JSON object per log record and redact sensitive extra fields."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in _CONTEXT_FIELDS:
            payload[field] = getattr(record, field, None)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key.startswith("_") or key in payload or key in {
                "args",
                "created",
                "exc_info",
                "exc_text",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "message",
                "module",
                "msecs",
                "msg",
                "name",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "thread",
                "threadName",
            }:
                continue
            payload[key] = "***redacted***" if key.lower() in _SENSITIVE_KEYS else value
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_structured_logging(service: str) -> None:
    """Configure root logging once for CLI and Runtime processes."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    logging.getLogger(__name__).debug(
        "Structured logging configured",
        extra={"service": service},
    )
