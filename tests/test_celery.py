"""Testes do backend Celery em modo *eager* (a task roda no próprio processo, sem broker)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from taskforge.bootstrap import Application
from taskforge.celery_app.app import CPU_QUEUE, IO_QUEUE, RUN_HANDLER_TASK, create_celery_app
from taskforge.celery_app.backend import CeleryExecutionBackend
from taskforge.config import Settings
from taskforge.domain.models import ExecutionMode, JobStatus
from taskforge.exceptions import PermanentError, TransientError


def eager_settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "celery_eager": True,
        "celery_broker_url": "memory://",
        "celery_result_backend": "cache+memory://",
        "celery_task_retries": 0,
        "job_timeout_seconds": 30,
    }
    return Settings(**{**base, **overrides})  # type: ignore[arg-type]


@pytest.fixture
def backend() -> Iterator[CeleryExecutionBackend]:
    settings = eager_settings()
    with CeleryExecutionBackend(create_celery_app(settings), result_timeout=10) as b:
        yield b


def test_app_configuration() -> None:
    app = create_celery_app(eager_settings())
    assert app.conf.task_serializer == "json" and app.conf.accept_content == ["json"]
    assert app.conf.task_acks_late is True
    assert app.conf.worker_prefetch_multiplier == 1
    assert app.conf.task_time_limit > app.conf.task_soft_time_limit
    assert RUN_HANDLER_TASK in app.tasks
    assert IO_QUEUE != CPU_QUEUE


def test_runs_handler_and_returns_result(backend: CeleryExecutionBackend) -> None:
    future = backend.submit("echo", {"a": 1}, ExecutionMode.INLINE)
    assert future.result(timeout=10) == {"echo": {"a": 1}}


def test_cpu_handler_through_celery(backend: CeleryExecutionBackend) -> None:
    future = backend.submit("primes.count", {"limit": 100}, ExecutionMode.PROCESS)
    assert future.result(timeout=10) == {"limit": 100, "count": 25}


def test_permanent_error_propagates_with_type(backend: CeleryExecutionBackend) -> None:
    future = backend.submit("util.fail", {"kind": "permanent"}, ExecutionMode.INLINE)
    with pytest.raises(PermanentError):
        future.result(timeout=10)


def test_transient_error_propagates_with_type(backend: CeleryExecutionBackend) -> None:
    future = backend.submit("util.fail", {"kind": "transient"}, ExecutionMode.INLINE)
    with pytest.raises(TransientError):
        future.result(timeout=10)


def test_full_application_with_celery_backend(tmp_path: Path) -> None:
    settings = eager_settings(
        execution_backend="celery",
        database_path=":memory:",
        poll_wait_seconds=0.01,
        heartbeat_file=str(tmp_path / "hb"),
    )
    with Application.build(settings) as app:
        ok = app.dispatcher.submit("echo", {"via": "celery"})
        primes = app.dispatcher.submit("primes.count", {"limit": 1000})
        bad = app.dispatcher.submit("util.fail", {"kind": "permanent"})
        app.worker.run_once()

        assert app.repository.get(ok.id).status is JobStatus.SUCCEEDED
        assert app.repository.get(primes.id).result == {"limit": 1000, "count": 168}
        assert app.repository.get(bad.id).status is JobStatus.FAILED
