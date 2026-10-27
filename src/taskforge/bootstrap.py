"""Composition root: o único lugar que conhece as implementações concretas.

O resto do código recebe suas dependências por construtor (Dependency Injection manual).
``ExitStack`` garante que, se a montagem falhar no meio, o que já foi aberto é fechado; e
que ``close`` encerra tudo na ordem inversa da criação (backend, repositório, fila).
"""

from __future__ import annotations

import logging
from contextlib import ExitStack
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Self

from taskforge.adapters.queues.factory import QueueFactory, default_queue_factory
from taskforge.adapters.repositories.sqlite import SQLiteJobRepository
from taskforge.concurrency.backends import LocalExecutionBackend
from taskforge.concurrency.pools import PoolManager
from taskforge.config import Settings
from taskforge.exceptions import ConfigurationError
from taskforge.handlers import builtin as _builtin  # noqa: F401  (registra os handlers padrão)
from taskforge.handlers.registry import HandlerRegistry, default_registry
from taskforge.handlers.runner import PLUGINS_ENV, import_plugins
from taskforge.patterns.circuit_breaker import CircuitBreaker
from taskforge.patterns.event_bus import EventBus, MetricsCollector
from taskforge.patterns.retry import RetryPolicy
from taskforge.ports.execution import ExecutionBackend
from taskforge.ports.queue import MessageQueue
from taskforge.ports.repository import JobRepository
from taskforge.services.dispatcher import JobDispatcher
from taskforge.services.intake import build_intake_chain
from taskforge.services.worker import Worker

logger = logging.getLogger(__name__)


def create_execution_backend(settings: Settings, registry: HandlerRegistry) -> ExecutionBackend:
    if settings.execution_backend == "celery":
        try:
            from taskforge.celery_app.app import create_celery_app
            from taskforge.celery_app.backend import CeleryExecutionBackend
        except ImportError as exc:
            raise ConfigurationError(
                "backend celery requer: pip install 'taskforge[celery]'"
            ) from exc
        return CeleryExecutionBackend(
            create_celery_app(settings),
            result_timeout=settings.job_timeout_seconds,
            max_waiters=max(settings.io_threads, settings.batch_size),
        )
    pools = PoolManager(io_threads=settings.io_threads, cpu_processes=settings.cpu_processes)
    return LocalExecutionBackend(pools, registry)


def _heartbeat_writer(path: str) -> object:
    target = Path(path)

    def beat() -> None:
        target.touch()

    return beat


@dataclass
class Application:
    settings: Settings
    queue: MessageQueue
    repository: JobRepository
    registry: HandlerRegistry
    bus: EventBus
    metrics: MetricsCollector
    backend: ExecutionBackend
    dispatcher: JobDispatcher
    worker: Worker
    _stack: ExitStack = field(repr=False)

    @classmethod
    def build(
        cls,
        settings: Settings,
        *,
        queue_factory: QueueFactory | None = None,
        registry: HandlerRegistry | None = None,
        queue: MessageQueue | None = None,
        repository: JobRepository | None = None,
        backend: ExecutionBackend | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Self:
        registry = registry or default_registry
        import_plugins(_plugins_spec())
        with ExitStack() as stack:
            queue = queue or (queue_factory or default_queue_factory()).create(settings)
            stack.enter_context(queue)
            repository = repository or SQLiteJobRepository(settings.database_path)
            stack.enter_context(repository)
            backend = backend or create_execution_backend(settings, registry)
            stack.enter_context(backend)

            bus = EventBus()
            metrics = MetricsCollector()
            metrics.attach(bus)

            dispatcher = JobDispatcher(
                publisher=queue,
                repository=repository,
                registry=registry,
                bus=bus,
                retry_policy=RetryPolicy(max_attempts=4, base_delay=0.2, max_delay=5.0),
                breaker=CircuitBreaker("queue-publish", failure_threshold=5, recovery_timeout=30.0),
                max_message_bytes=settings.max_message_bytes,
            )
            worker = Worker(
                consumer=queue,
                repository=repository,
                backend=backend,
                registry=registry,
                bus=bus,
                intake=build_intake_chain(registry, max_bytes=settings.max_message_bytes),
                retry_policy=retry_policy
                or RetryPolicy(
                    base_delay=settings.retry_base_delay_seconds,
                    max_delay=settings.retry_max_delay_seconds,
                ),
                job_timeout=settings.job_timeout_seconds,
                batch_size=settings.batch_size,
                poll_wait=settings.poll_wait_seconds,
                heartbeat=_heartbeat_writer(settings.heartbeat_file),  # type: ignore[arg-type]
            )
            # Tudo montado: transfere a posse dos recursos para a Application.
            return cls(
                settings=settings,
                queue=queue,
                repository=repository,
                registry=registry,
                bus=bus,
                metrics=metrics,
                backend=backend,
                dispatcher=dispatcher,
                worker=worker,
                _stack=stack.pop_all(),
            )

    def close(self) -> None:
        self._stack.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


def _plugins_spec() -> str:
    import os

    return os.environ.get(PLUGINS_ENV, "")
