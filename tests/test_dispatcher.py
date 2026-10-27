from __future__ import annotations

from collections.abc import Mapping

import pytest

from taskforge.adapters.repositories.memory import InMemoryJobRepository
from taskforge.domain.events import JobFailed, JobSubmitted
from taskforge.domain.models import JobMessage, JobStatus
from taskforge.exceptions import (
    CircuitOpenError,
    HandlerNotFoundError,
    JobValidationError,
    QueueUnavailableError,
    RetryExhaustedError,
)
from taskforge.handlers.builtin import EchoHandler
from taskforge.handlers.registry import HandlerRegistry
from taskforge.patterns.circuit_breaker import CircuitBreaker
from taskforge.patterns.event_bus import EventBus
from taskforge.patterns.retry import RetryPolicy
from taskforge.ports.queue import Publisher
from taskforge.services.dispatcher import JobDispatcher


class ScriptedPublisher(Publisher):
    """Falha as primeiras ``failures`` chamadas, depois funciona."""

    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.calls = 0
        self.published: list[tuple[str, Mapping[str, str] | None]] = []
        self.dedup_keys: list[str | None] = []

    def publish(
        self,
        body: str,
        *,
        attributes: Mapping[str, str] | None = None,
        dedup_key: str | None = None,
    ) -> str:
        self.calls += 1
        self.dedup_keys.append(dedup_key)
        if self.calls <= self.failures:
            raise QueueUnavailableError("broker fora")
        self.published.append((body, attributes))
        return f"id-{self.calls}"


def build(
    publisher: Publisher,
    *,
    policy: RetryPolicy | None = None,
    breaker: CircuitBreaker | None = None,
    max_bytes: int | None = None,
) -> tuple[JobDispatcher, InMemoryJobRepository, EventBus, list[float]]:
    registry = HandlerRegistry()
    registry.register(EchoHandler)
    repository = InMemoryJobRepository()
    bus = EventBus()
    sleeps: list[float] = []
    dispatcher = JobDispatcher(
        publisher=publisher,
        repository=repository,
        registry=registry,
        bus=bus,
        retry_policy=policy or RetryPolicy(max_attempts=3, base_delay=1, jitter=0),
        breaker=breaker,
        max_message_bytes=max_bytes,
        sleep=sleeps.append,
    )
    return dispatcher, repository, bus, sleeps


def test_submit_persists_publishes_and_emits_event() -> None:
    publisher = ScriptedPublisher()
    dispatcher, repository, bus, _ = build(publisher)
    events: list[JobSubmitted] = []
    bus.subscribe(JobSubmitted, events.append)

    job = dispatcher.submit("echo", {"a": 1}, max_attempts=4)

    assert repository.get(job.id).status is JobStatus.PENDING
    [(body, attributes)] = publisher.published
    message = JobMessage.from_body(body)
    assert (message.job_id, message.handler, message.payload, message.max_attempts) == (
        job.id,
        "echo",
        {"a": 1},
        4,
    )
    assert attributes == {"handler": "echo", "job_id": job.id}
    assert [e.job_id for e in events] == [job.id]


def test_unknown_handler_fails_fast_and_persists_nothing() -> None:
    publisher = ScriptedPublisher()
    dispatcher, repository, _, _ = build(publisher)
    with pytest.raises(HandlerNotFoundError):
        dispatcher.submit("nao.existe")
    assert publisher.calls == 0 and repository.list_jobs() == []


@pytest.mark.parametrize("kwargs", [{"max_attempts": 0}, {"max_attempts": 99}])
def test_invalid_job_parameters(kwargs: dict[str, int]) -> None:
    dispatcher, repository, _, _ = build(ScriptedPublisher())
    with pytest.raises(JobValidationError):
        dispatcher.submit("echo", {}, **kwargs)
    assert repository.list_jobs() == []


def test_non_serializable_payload_is_a_validation_error() -> None:
    dispatcher, repository, _, _ = build(ScriptedPublisher())
    with pytest.raises(JobValidationError):
        dispatcher.submit("echo", {"x": object()})
    assert repository.list_jobs() == []


def test_oversized_message_is_rejected_before_publishing() -> None:
    publisher = ScriptedPublisher()
    dispatcher, repository, _, _ = build(publisher, max_bytes=100)
    with pytest.raises(JobValidationError, match="limite"):
        dispatcher.submit("echo", {"blob": "x" * 500})
    assert publisher.calls == 0 and repository.list_jobs() == []


def test_transient_publish_failures_are_retried_with_backoff() -> None:
    publisher = ScriptedPublisher(failures=2)
    dispatcher, repository, _, sleeps = build(publisher)
    job = dispatcher.submit("echo")
    assert publisher.calls == 3
    assert sleeps == [1, 2]
    assert repository.get(job.id).status is JobStatus.PENDING
    # Todas as tentativas usam a mesma chave: o broker pode descartar a duplicata.
    assert publisher.dedup_keys == [job.id] * 3


def test_publish_failure_marks_job_failed_and_raises() -> None:
    publisher = ScriptedPublisher(failures=99)
    dispatcher, repository, bus, _ = build(publisher)
    failures: list[JobFailed] = []
    bus.subscribe(JobFailed, failures.append)

    with pytest.raises(RetryExhaustedError):
        dispatcher.submit("echo")

    [job] = repository.list_jobs()
    assert job.status is JobStatus.FAILED and "publicação falhou" in (job.error or "")
    assert len(failures) == 1


def test_open_circuit_stops_calling_the_broker_and_skips_retries() -> None:
    publisher = ScriptedPublisher(failures=99)
    breaker = CircuitBreaker("q", failure_threshold=2, recovery_timeout=60)
    dispatcher, repository, _, sleeps = build(
        publisher, policy=RetryPolicy(max_attempts=1, base_delay=1), breaker=breaker
    )
    for _ in range(2):
        with pytest.raises(RetryExhaustedError):
            dispatcher.submit("echo")
    assert publisher.calls == 2

    with pytest.raises(CircuitOpenError):
        dispatcher.submit("echo")
    assert publisher.calls == 2, "com o circuito aberto o broker nem é chamado"
    assert sleeps == [], "e não há espera de retry"
    assert all(j.status is JobStatus.FAILED for j in repository.list_jobs())
