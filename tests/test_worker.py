from __future__ import annotations

import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from helpers import BarrierHandler
from taskforge.adapters.queues.memory import InMemoryQueue
from taskforge.bootstrap import Application
from taskforge.domain.models import Job, JobMessage, JobStatus
from taskforge.exceptions import QueueUnavailableError

MakeApp = Callable[..., Application]


def queue_of(app: Application) -> InMemoryQueue:
    assert isinstance(app.queue, InMemoryQueue)
    return app.queue


def test_success_flow(make_app: MakeApp) -> None:
    app = make_app()
    job = app.dispatcher.submit("echo", {"a": 1})
    assert app.worker.run_once() == 1

    saved = app.repository.get(job.id)
    assert saved.status is JobStatus.SUCCEEDED
    assert saved.result == {"echo": {"a": 1}} and saved.attempts == 1
    assert queue_of(app).depth == 0 and queue_of(app).in_flight == 0
    metrics = app.metrics.snapshot()
    assert (metrics["JobSubmitted"], metrics["JobStarted"], metrics["JobSucceeded"]) == (1, 1, 1)


def test_empty_queue_is_a_noop(make_app: MakeApp) -> None:
    assert make_app().worker.run_once() == 0


def test_transient_failure_is_retried_then_succeeds(make_app: MakeApp) -> None:
    app = make_app()
    job = app.dispatcher.submit("test.flaky", {"key": "a", "fail_times": 1}, max_attempts=3)

    app.worker.run_once()
    assert app.repository.get(job.id).status is JobStatus.RETRYING
    assert queue_of(app).depth == 1, "a mensagem voltou para a fila"

    app.worker.run_once()
    final = app.repository.get(job.id)
    assert final.status is JobStatus.SUCCEEDED and final.attempts == 2
    assert app.metrics.snapshot()["JobRetrying"] == 1


def test_exhausted_retries_go_to_dead_letter(make_app: MakeApp) -> None:
    app = make_app()
    job = app.dispatcher.submit("test.flaky", {"key": "b", "fail_times": 99}, max_attempts=2)

    app.worker.run_once()
    app.worker.run_once()

    final = app.repository.get(job.id)
    assert final.status is JobStatus.DEAD_LETTERED and final.attempts == 2
    assert "tentativas esgotadas" in (final.error or "")
    assert len(queue_of(app).dead_letters) == 1 and queue_of(app).depth == 0
    assert app.worker.run_once() == 0


def test_permanent_failure_is_recorded_and_message_is_acked(make_app: MakeApp) -> None:
    app = make_app()
    job = app.dispatcher.submit("util.fail", {"kind": "permanent"})
    app.worker.run_once()

    final = app.repository.get(job.id)
    assert final.status is JobStatus.FAILED and final.attempts == 1
    assert queue_of(app).depth == 0 and queue_of(app).dead_letters == []


def test_unclassified_handler_exception_is_permanent(make_app: MakeApp) -> None:
    app = make_app()
    job = app.dispatcher.submit("test.boom")
    app.worker.run_once()
    final = app.repository.get(job.id)
    assert final.status is JobStatus.FAILED and "ValueError" in (final.error or "")


def test_raw_network_error_is_treated_as_transient(make_app: MakeApp) -> None:
    app = make_app()
    job = app.dispatcher.submit("test.conn", max_attempts=2)
    app.worker.run_once()
    assert app.repository.get(job.id).status is JobStatus.RETRYING


def test_garbage_message_goes_to_dead_letter(make_app: MakeApp) -> None:
    app = make_app()
    queue_of(app).publish("isto não é json")
    app.worker.run_once()
    assert len(queue_of(app).dead_letters) == 1 and queue_of(app).depth == 0


def test_unknown_handler_message_is_failed_and_dead_lettered(make_app: MakeApp) -> None:
    app = make_app()
    queue_of(app).publish(JobMessage(job_id="x1", handler="nao.existe").to_body())
    app.worker.run_once()
    assert app.repository.get("x1").status is JobStatus.FAILED
    assert len(queue_of(app).dead_letters) == 1


def test_oversized_message_is_rejected(make_app: MakeApp) -> None:
    app = make_app()
    queue_of(app).publish("x" * 300_000)
    app.worker.run_once()
    assert len(queue_of(app).dead_letters) == 1


def test_redelivery_of_finished_job_is_idempotent(make_app: MakeApp) -> None:
    app = make_app()
    job = app.dispatcher.submit("test.flaky", {"key": "idem"})
    app.worker.run_once()
    assert app.metrics.snapshot()["JobStarted"] == 1

    queue_of(app).publish(JobMessage.from_job(job).to_body())  # duplicata na fila
    assert app.worker.run_once() == 1

    assert app.metrics.snapshot()["JobStarted"] == 1, "o handler não pode rodar de novo"
    assert queue_of(app).depth == 0 and queue_of(app).in_flight == 0


def test_poison_pill_after_repeated_worker_crashes(make_app: MakeApp) -> None:
    app = make_app()
    crashed = Job(handler="echo", max_attempts=2).start().start()  # 2 entregas sem concluir
    app.repository.add(crashed)
    queue_of(app).publish(JobMessage.from_job(crashed).to_body())

    app.worker.run_once()

    final = app.repository.get(crashed.id)
    assert final.status is JobStatus.DEAD_LETTERED
    assert len(queue_of(app).dead_letters) == 1


def test_timeout_is_transient_and_dead_letters_when_exhausted(make_app: MakeApp) -> None:
    app = make_app(job_timeout=0.05)
    job = app.dispatcher.submit("test.slow", {"seconds": 0.4}, max_attempts=1)
    app.worker.run_once()
    final = app.repository.get(job.id)
    assert final.status is JobStatus.DEAD_LETTERED
    assert "JobTimeoutError" in (final.error or "")


def test_batch_runs_jobs_concurrently(make_app: MakeApp) -> None:
    """Se os 3 jobs não rodassem ao mesmo tempo, a barreira (3 partes) estouraria o timeout."""
    BarrierHandler.barrier = threading.Barrier(3, timeout=5)
    app = make_app(batch_size=8)
    jobs = [app.dispatcher.submit("test.barrier") for _ in range(3)]
    assert app.worker.run_once() == 3
    assert all(app.repository.get(j.id).status is JobStatus.SUCCEEDED for j in jobs)


def test_one_bad_job_does_not_poison_the_batch(make_app: MakeApp) -> None:
    app = make_app()
    good = app.dispatcher.submit("echo", {"n": 1})
    bad = app.dispatcher.submit("test.boom")
    queue_of(app).publish("lixo")
    good2 = app.dispatcher.submit("echo", {"n": 2})
    assert app.worker.run_once() == 4
    assert app.repository.get(good.id).status is JobStatus.SUCCEEDED
    assert app.repository.get(good2.id).status is JobStatus.SUCCEEDED
    assert app.repository.get(bad.id).status is JobStatus.FAILED


@pytest.mark.slow
def test_cpu_bound_job_runs_in_a_separate_process(make_app: MakeApp) -> None:
    app = make_app()
    ok = app.dispatcher.submit("primes.count", {"limit": 1000})
    invalid = app.dispatcher.submit("primes.count", {"limit": "muitos"})
    app.worker.run_once()

    assert app.repository.get(ok.id).result == {"limit": 1000, "count": 168}
    failed = app.repository.get(invalid.id)
    # O JobValidationError nasceu no processo filho e voltou via pickle com o tipo preservado.
    assert failed.status is JobStatus.FAILED and "JobValidationError" in (failed.error or "")


def test_run_forever_processes_then_stops_gracefully(make_app: MakeApp, tmp_path: Path) -> None:
    app = make_app()
    job = app.dispatcher.submit("echo", {"x": 1})
    stop = threading.Event()
    thread = threading.Thread(target=app.worker.run_forever, args=(stop,))
    thread.start()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not app.repository.get(job.id).is_terminal:
        time.sleep(0.01)
    stop.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert app.repository.get(job.id).status is JobStatus.SUCCEEDED
    assert (tmp_path / "heartbeat").exists(), "o worker precisa emitir heartbeat"


def test_queue_outage_propagates_from_run_once(make_app: MakeApp) -> None:
    class DownQueue(InMemoryQueue):
        def receive(self, max_messages: int = 1, wait_seconds: float = 1.0) -> list:  # type: ignore[type-arg]
            raise QueueUnavailableError("broker fora")

    app = make_app(queue=DownQueue("down"))
    with pytest.raises(QueueUnavailableError):
        app.worker.run_once()


def test_run_forever_survives_a_queue_outage(make_app: MakeApp) -> None:
    class FlappingQueue(InMemoryQueue):
        outages = 1

        def receive(self, max_messages: int = 1, wait_seconds: float = 1.0) -> list:  # type: ignore[type-arg]
            if self.outages > 0:
                self.outages -= 1
                raise QueueUnavailableError("broker fora")
            return super().receive(max_messages, wait_seconds)

    app = make_app(queue=FlappingQueue("flap"))
    job = app.dispatcher.submit("echo")
    stop = threading.Event()
    thread = threading.Thread(target=app.worker.run_forever, args=(stop,))
    thread.start()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and not app.repository.get(job.id).is_terminal:
        time.sleep(0.02)
    stop.set()
    thread.join(timeout=5)
    assert app.repository.get(job.id).status is JobStatus.SUCCEEDED
