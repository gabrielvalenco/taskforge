from __future__ import annotations

import json
import logging
import os
import signal
import threading

import pytest

from helpers import FakeClock
from taskforge.domain.models import JobMessage
from taskforge.exceptions import HandlerNotFoundError, JobValidationError, MessageDecodeError
from taskforge.handlers.builtin import EchoHandler
from taskforge.handlers.registry import HandlerRegistry
from taskforge.logging_setup import JsonFormatter, bind_job
from taskforge.ports.queue import QueueMessage
from taskforge.resources import ShutdownSignal, Stopwatch, suppress_logged, timed
from taskforge.services.intake import IntakeContext, IntakeValidator, build_intake_chain


def message(body: str) -> QueueMessage:
    return QueueMessage(id="m", body=body, receipt="r")


@pytest.fixture
def chain() -> IntakeValidator:
    registry = HandlerRegistry()
    registry.register(EchoHandler)
    return build_intake_chain(registry, max_bytes=500)


# --- Chain of Responsibility ---------------------------------------------------------------


def test_valid_message_passes_and_is_decoded(chain: IntakeValidator) -> None:
    body = JobMessage(job_id="j", handler="echo").to_body()
    ctx = chain.validate(IntakeContext(message(body)))
    assert ctx.job_message is not None and ctx.job_message.job_id == "j"


def test_each_link_rejects_its_own_problem(chain: IntakeValidator) -> None:
    with pytest.raises(JobValidationError, match="excede"):
        chain.validate(IntakeContext(message("x" * 501)))
    with pytest.raises(MessageDecodeError):
        chain.validate(IntakeContext(message("{")))
    with pytest.raises(HandlerNotFoundError):
        chain.validate(IntakeContext(message(JobMessage(job_id="j", handler="nada").to_body())))


def test_size_is_checked_before_decoding(chain: IntakeValidator) -> None:
    """Corpo gigante e inválido deve falhar por tamanho, sem gastar CPU parseando."""
    with pytest.raises(JobValidationError, match="excede"):
        chain.validate(IntakeContext(message("{" * 10_000)))


def test_chain_is_extensible() -> None:
    class RequireTenant(IntakeValidator):
        def _check(self, ctx: IntakeContext) -> None:
            if "tenant" not in ctx.message.attributes:
                raise JobValidationError("sem tenant")

    registry = HandlerRegistry()
    registry.register(EchoHandler)
    head = build_intake_chain(registry, max_bytes=500)
    head.set_next(RequireTenant())  # substitui o resto da cadeia; ilustra o ponto de extensão
    with pytest.raises(JobValidationError, match="tenant"):
        head.validate(IntakeContext(message("x")))


# --- Context managers ------------------------------------------------------------------------


def test_timed_measures_even_when_the_block_raises(caplog: pytest.LogCaptureFixture) -> None:
    clock = FakeClock(0.0)
    log = logging.getLogger("t")
    with caplog.at_level(logging.INFO, logger="t"), pytest.raises(RuntimeError):
        with timed("operação", log=log, level=logging.INFO, clock=clock) as watch:
            clock.advance(2.5)
            raise RuntimeError("falhou")
    assert watch.elapsed == 2.5
    assert "operação concluído em 2.500s" in caplog.text


def test_stopwatch_is_live_until_stopped() -> None:
    clock = FakeClock(0.0)
    watch = Stopwatch(clock)
    clock.advance(1)
    assert watch.elapsed == 1
    watch.stop()
    clock.advance(5)
    assert watch.elapsed == 1


def test_suppress_logged_swallows_and_logs(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.ERROR):
        with suppress_logged(ValueError, message="limpeza falhou"):
            raise ValueError("detalhe")
    assert "limpeza falhou" in caplog.text and "detalhe" in caplog.text
    with pytest.raises(KeyError), suppress_logged(ValueError):
        raise KeyError("outro tipo não é engolido")


def test_shutdown_signal_sets_event_and_restores_handlers() -> None:
    before = signal.getsignal(signal.SIGTERM)
    with ShutdownSignal() as shutdown:
        assert not shutdown.event.is_set()
        os.kill(os.getpid(), signal.SIGTERM)
        assert shutdown.event.wait(timeout=2)
    assert signal.getsignal(signal.SIGTERM) is before


def test_second_signal_forces_exit() -> None:
    with ShutdownSignal() as shutdown:
        os.kill(os.getpid(), signal.SIGTERM)
        assert shutdown.event.wait(timeout=2)
        with pytest.raises(KeyboardInterrupt):
            os.kill(os.getpid(), signal.SIGTERM)
            for _ in range(1000):  # dá tempo ao interpretador de entregar o sinal
                pass


def test_shutdown_signal_in_non_main_thread_does_not_install_handlers() -> None:
    errors: list[BaseException] = []

    def run() -> None:
        try:
            with ShutdownSignal() as shutdown:
                shutdown.trigger()
                assert shutdown.event.is_set()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    thread.join()
    assert errors == []


# --- Logging ---------------------------------------------------------------------------------


def record(msg: str = "olá", exc: bool = False) -> logging.LogRecord:
    info = None
    if exc:
        try:
            raise ValueError("x")
        except ValueError:
            import sys

            info = sys.exc_info()
    return logging.LogRecord("n", logging.WARNING, __file__, 1, msg, None, info)


def test_json_formatter_includes_bound_job_id() -> None:
    formatter = JsonFormatter()
    assert "job_id" not in json.loads(formatter.format(record()))
    with bind_job("abc"):
        payload = json.loads(formatter.format(record("mensagem ç")))
    assert payload["job_id"] == "abc" and payload["message"] == "mensagem ç"
    assert payload["level"] == "WARNING"
    assert "job_id" not in json.loads(formatter.format(record())), "contexto é restaurado"


def test_json_formatter_includes_exception() -> None:
    assert "ValueError" in json.loads(JsonFormatter().format(record(exc=True)))["exc"]


def test_bind_job_is_isolated_per_thread() -> None:
    seen: dict[str, str | None] = {}

    def work() -> None:
        payload = json.loads(JsonFormatter().format(record()))
        seen["thread"] = payload.get("job_id")

    with bind_job("principal"):
        thread = threading.Thread(target=work)
        thread.start()
        thread.join()
    assert seen["thread"] is None
