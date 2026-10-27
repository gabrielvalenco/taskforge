from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from helpers import (
    BarrierHandler,
    BoomHandler,
    ConnHandler,
    FlakyHandler,
    SlowHandler,
)
from taskforge.adapters.queues.memory import InMemoryQueue
from taskforge.bootstrap import Application
from taskforge.config import Settings
from taskforge.handlers.builtin import EchoHandler, FailHandler, PrimeCountHandler
from taskforge.handlers.registry import HandlerRegistry
from taskforge.patterns.retry import RetryPolicy


@pytest.fixture(autouse=True)
def _reset_handler_state() -> None:
    FlakyHandler.calls.clear()
    BarrierHandler.barrier = None


@pytest.fixture
def registry() -> HandlerRegistry:
    reg = HandlerRegistry()
    for cls in (
        EchoHandler,
        FailHandler,
        PrimeCountHandler,
        FlakyHandler,
        BarrierHandler,
        SlowHandler,
        BoomHandler,
        ConnHandler,
    ):
        reg.register(cls)
    return reg


@pytest.fixture
def make_app(registry: HandlerRegistry, tmp_path: Path) -> Iterator[Callable[..., Application]]:
    """Monta uma Application completa (fila em memória + SQLite em memória + pools locais)."""
    apps: list[Application] = []

    def _make(
        *,
        queue: InMemoryQueue | None = None,
        job_timeout: float = 5.0,
        batch_size: int = 8,
        max_deliveries: int = 5,
        visibility: float = 30.0,
    ) -> Application:
        settings = Settings(
            queue_backend="memory",
            database_path=":memory:",
            io_threads=8,
            cpu_processes=1,
            batch_size=batch_size,
            poll_wait_seconds=0.01,
            job_timeout_seconds=job_timeout,
            heartbeat_file=str(tmp_path / "heartbeat"),
            log_json=False,
        )
        app = Application.build(
            settings,
            registry=registry,
            queue=queue
            or InMemoryQueue("test", max_deliveries=max_deliveries, visibility_timeout=visibility),
            retry_policy=RetryPolicy(base_delay=0.0, max_delay=0.0, jitter=0.0),
        )
        apps.append(app)
        return app

    yield _make
    for app in apps:
        app.close()
