from __future__ import annotations

import pytest

from taskforge.domain.models import Job, JobMessage, JobStatus
from taskforge.exceptions import InvalidTransitionError, MessageDecodeError


def test_happy_path_transitions() -> None:
    job = Job(handler="echo")
    assert job.status is JobStatus.PENDING and job.attempts == 0

    running = job.start()
    assert running.status is JobStatus.RUNNING and running.attempts == 1
    assert job.status is JobStatus.PENDING, "Job é imutável: a instância original não muda"

    done = running.succeed({"ok": True})
    assert done.status is JobStatus.SUCCEEDED
    assert done.result == {"ok": True} and done.is_terminal


def test_retry_cycle_counts_attempts() -> None:
    job = Job(handler="echo", max_attempts=2).start()
    assert job.can_retry
    job = job.retry("boom").start()
    assert job.attempts == 2 and not job.can_retry
    assert job.dead_letter("esgotou").status is JobStatus.DEAD_LETTERED


def test_redelivery_while_running_is_allowed() -> None:
    job = Job(handler="echo").start().start()
    assert job.status is JobStatus.RUNNING and job.attempts == 2


@pytest.mark.parametrize("terminal", ["succeed", "fail", "dead_letter"])
def test_terminal_states_are_final(terminal: str) -> None:
    job = Job(handler="echo").start()
    job = job.succeed({}) if terminal == "succeed" else getattr(job, terminal)("x")
    with pytest.raises(InvalidTransitionError):
        job.start()


def test_cannot_succeed_a_pending_job() -> None:
    with pytest.raises(InvalidTransitionError):
        Job(handler="echo").succeed({})


def test_error_is_truncated() -> None:
    job = Job(handler="echo").start().fail("x" * 10_000)
    assert job.error is not None and len(job.error) == 2000


@pytest.mark.parametrize("bad", ["", "Echo", "1abc", "has space", "a" * 101])
def test_handler_name_is_validated(bad: str) -> None:
    with pytest.raises(ValueError):
        Job(handler=bad)


def test_message_roundtrip() -> None:
    job = Job(handler="primes.count", payload={"limit": 10}, max_attempts=4)
    message = JobMessage.from_job(job)
    decoded = JobMessage.from_body(message.to_body())
    assert decoded == message
    rebuilt = decoded.to_job()
    assert (rebuilt.id, rebuilt.handler, rebuilt.payload, rebuilt.max_attempts) == (
        job.id,
        "primes.count",
        {"limit": 10},
        4,
    )


@pytest.mark.parametrize(
    "body",
    ["not json", "{}", '{"job_id": "1"}', '{"job_id": "1", "handler": "BAD NAME"}', "[]", ""],
)
def test_message_decode_errors(body: str) -> None:
    with pytest.raises(MessageDecodeError):
        JobMessage.from_body(body)


def test_decode_error_does_not_leak_input() -> None:
    secret = "senha-super-secreta"
    body = f'{{"job_id": "1", "handler": "ok", "payload": "{secret}"}}'
    with pytest.raises(MessageDecodeError) as info:
        JobMessage.from_body(body)
    assert secret not in str(info.value)


def test_unknown_fields_are_ignored_for_forward_compat() -> None:
    body = '{"job_id": "1", "handler": "echo", "novo_campo": 1}'
    assert JobMessage.from_body(body).job_id == "1"
