"""Lado produtor: valida, persiste e publica um job.

Depende só de abstrações (``Publisher``, ``JobRepository``, ``HandlerRegistry``,
``EventBus``): Dependency Inversion. Resiliência em camadas: retry com backoff por fora,
circuit breaker por dentro (o retry desiste de imediato quando o circuito abre).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from contextlib import nullcontext
from dataclasses import replace
from typing import Any

import pydantic

from taskforge.domain.events import JobFailed, JobSubmitted
from taskforge.domain.models import Job, JobMessage, summarize_validation_error
from taskforge.exceptions import (
    CircuitOpenError,
    HandlerNotFoundError,
    JobValidationError,
    QueueError,
    RetryExhaustedError,
)
from taskforge.handlers.registry import HandlerRegistry
from taskforge.patterns.circuit_breaker import CircuitBreaker
from taskforge.patterns.event_bus import EventBus
from taskforge.patterns.retry import RetryPolicy, call_with_retry
from taskforge.ports.queue import Publisher
from taskforge.ports.repository import JobRepository

logger = logging.getLogger(__name__)


class JobDispatcher:
    def __init__(
        self,
        *,
        publisher: Publisher,
        repository: JobRepository,
        registry: HandlerRegistry,
        bus: EventBus,
        retry_policy: RetryPolicy | None = None,
        breaker: CircuitBreaker | None = None,
        max_message_bytes: int | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        base = retry_policy or RetryPolicy()
        # Com o circuito aberto, repetir só atrasa o erro: não faz retry nesse caso.
        self._retry = replace(
            base,
            retry_if=lambda exc: base.retry_if(exc) and not isinstance(exc, CircuitOpenError),
        )
        self._publisher = publisher
        self._repository = repository
        self._registry = registry
        self._bus = bus
        self._breaker = breaker
        self._max_bytes = max_message_bytes
        self._sleep = sleep

    def submit(
        self,
        handler: str,
        payload: Mapping[str, Any] | None = None,
        *,
        max_attempts: int = 3,
    ) -> Job:
        if handler not in self._registry:
            raise HandlerNotFoundError(
                f"handler desconhecido: {handler!r}",
                context={"known": self._registry.names()},
            )
        try:
            job = Job(handler=handler, payload=dict(payload or {}), max_attempts=max_attempts)
            body = JobMessage.from_job(job).to_body()
        except pydantic.ValidationError as exc:
            raise JobValidationError(summarize_validation_error(exc)) from exc
        except (TypeError, ValueError) as exc:  # payload não serializável em JSON
            raise JobValidationError(f"payload inválido: {exc}") from exc
        if self._max_bytes is not None and len(body.encode("utf-8")) > self._max_bytes:
            raise JobValidationError(f"payload excede o limite de {self._max_bytes} bytes")

        self._repository.add(job)
        try:
            call_with_retry(lambda: self._publish_once(job, body), self._retry, sleep=self._sleep)
        except (RetryExhaustedError, CircuitOpenError, QueueError) as exc:
            self._repository.save(job.fail(f"publicação falhou: {exc}"))
            self._bus.publish(JobFailed(job.id, str(exc)))
            raise
        self._bus.publish(JobSubmitted(job.id, handler))
        logger.info("job %s (%s) publicado", job.id, handler)
        return job

    def _publish_once(self, job: Job, body: str) -> str:
        guard = self._breaker.guard() if self._breaker is not None else nullcontext()
        with guard:
            return self._publisher.publish(
                body,
                attributes={"handler": job.handler, "job_id": job.id},
                dedup_key=job.id,
            )
