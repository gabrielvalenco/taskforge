"""Logging estruturado com contexto por job (contextvars)."""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime

_current_job: ContextVar[str | None] = ContextVar("taskforge_current_job", default=None)


@contextmanager
def bind_job(job_id: str) -> Iterator[None]:
    """Associa ``job_id`` a todos os logs emitidos dentro do bloco (por thread/contexto)."""
    token = _current_job.set(job_id)
    try:
        yield
    finally:
        _current_job.reset(token)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        job_id = _current_job.get()
        if job_id:
            payload["job_id"] = job_id
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


class _TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        job_id = _current_job.get()
        return f"{base} [job={job_id}]" if job_id else base


def configure_logging(level: str = "INFO", *, json_logs: bool = True) -> None:
    """Logs vão para stderr; stdout fica livre para a saída útil da CLI."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        JsonFormatter()
        if json_logs
        else _TextFormatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
