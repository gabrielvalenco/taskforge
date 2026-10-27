"""Lado consumidor: recebe lotes, executa em paralelo e confirma na fila.

Fluxo de ``run_once`` (um lote):

1. recebe até ``batch_size`` mensagens;
2. para cada uma: valida (chain), carrega/cria o Job, marca RUNNING e **submete** ao backend
   (threads/processos/Celery) sem esperar;
3. espera cada Future com o prazo restante e decide: ack, nack com backoff ou dead-letter.

Todo acesso à fila e ao repositório acontece na thread do worker (clientes de fila nem
sempre são thread-safe); só o código dos handlers roda em paralelo.

Semântica: at-least-once. Se o worker cair antes do ack, a mensagem volta. Por isso, um Job
já finalizado que reaparece é apenas confirmado (idempotência), e um Job que estourou
``max_attempts`` por quedas repetidas vai para a dead-letter (poison pill).

Limitação conhecida: não dá para matar uma *thread* em execução. Num timeout o Future é
abandonado e a mensagem é reentregue, então handlers precisam ser idempotentes.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import CancelledError, Future
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from typing import Any

from taskforge.domain.events import (
    JobDeadLettered,
    JobFailed,
    JobRetrying,
    JobStarted,
    JobSucceeded,
)
from taskforge.domain.models import Job, JobMessage
from taskforge.exceptions import (
    InvalidTransitionError,
    JobTimeoutError,
    MessageDecodeError,
    PermanentError,
    QueueError,
    TransientError,
    WorkerCrashedError,
    is_retryable,
)
from taskforge.handlers.registry import HandlerRegistry
from taskforge.logging_setup import bind_job
from taskforge.patterns.event_bus import EventBus
from taskforge.patterns.retry import RetryPolicy
from taskforge.ports.execution import ExecutionBackend
from taskforge.ports.queue import Consumer, QueueMessage
from taskforge.ports.repository import JobRepository
from taskforge.resources import suppress_logged
from taskforge.services.intake import IntakeContext, IntakeValidator

logger = logging.getLogger(__name__)

_MIN_BACKOFF = 0.5
_MAX_BACKOFF = 30.0


@dataclass(slots=True)
class _Inflight:
    message: QueueMessage
    job: Job
    future: Future[dict[str, Any]]
    started: float
    deadline: float


class Worker:
    def __init__(
        self,
        *,
        consumer: Consumer,
        repository: JobRepository,
        backend: ExecutionBackend,
        registry: HandlerRegistry,
        bus: EventBus,
        intake: IntakeValidator,
        retry_policy: RetryPolicy | None = None,
        job_timeout: float = 300.0,
        batch_size: int = 8,
        poll_wait: float = 1.0,
        heartbeat: Callable[[], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._consumer = consumer
        self._repository = repository
        self._backend = backend
        self._registry = registry
        self._bus = bus
        self._intake = intake
        self._retry_policy = retry_policy or RetryPolicy(base_delay=1.0, max_delay=60.0)
        self._job_timeout = job_timeout
        self._batch_size = batch_size
        self._poll_wait = poll_wait
        self._heartbeat = heartbeat
        self._clock = clock

    # --- Loop ------------------------------------------------------------------------

    def run_forever(self, stop: threading.Event) -> None:
        backoff = _MIN_BACKOFF
        logger.info("worker iniciado")
        while not stop.is_set():
            if self._heartbeat is not None:
                with suppress_logged(Exception, log=logger, message="falha no heartbeat"):
                    self._heartbeat()
            try:
                self.run_once()
                backoff = _MIN_BACKOFF
            except QueueError as exc:
                logger.error("fila indisponível (%s); nova tentativa em %.1fs", exc, backoff)
                stop.wait(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF)
            except Exception:
                logger.exception("erro inesperado no loop do worker")
                stop.wait(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF)
        logger.info("worker encerrado")

    def run_once(self) -> int:
        """Processa um lote. Devolve quantas mensagens foram recebidas."""
        messages = self._consumer.receive(
            max_messages=self._batch_size, wait_seconds=self._poll_wait
        )
        inflight: list[_Inflight] = []
        for message in messages:
            try:
                item = self._start(message)
            except Exception:
                logger.exception("falha ao iniciar mensagem %s (será reentregue)", message.id)
                continue
            if item is not None:
                inflight.append(item)
        for item in inflight:
            with bind_job(item.job.id):
                try:
                    self._finish(item)
                except Exception:
                    # Sem ack: a fila reentrega (at-least-once). Nunca derruba o lote.
                    logger.exception("falha ao finalizar job (será reentregue)")
        return len(messages)

    # --- Início ----------------------------------------------------------------------

    def _start(self, message: QueueMessage) -> _Inflight | None:
        try:
            ctx = self._intake.validate(IntakeContext(message=message))
        except PermanentError as exc:
            self._reject(message, exc)
            return None
        assert ctx.job_message is not None
        job = self._load_or_create(ctx.job_message)

        with bind_job(job.id):
            if job.is_terminal:
                logger.info("job já finalizado (%s); confirmando redelivery", job.status.value)
                self._settle(self._consumer.ack, message)
                return None
            if job.attempts >= job.max_attempts:
                text = f"excedeu {job.max_attempts} tentativa(s) sem concluir (worker caiu?)"
                self._dead_letter(message, job, text)
                return None

            try:
                job = job.start()
            except InvalidTransitionError:
                logger.exception("transição inválida; descartando mensagem")
                self._settle(self._consumer.nack, message, requeue=False)
                return None
            self._repository.save(job)
            self._bus.publish(JobStarted(job.id, job.attempts))

            try:
                future = self._backend.submit(
                    job.handler, job.payload, self._registry.mode_of(job.handler)
                )
            except Exception as exc:  # noqa: BLE001  (backend arbitrário: tratado como transitório)
                self._on_failure(
                    message, job, TransientError(f"falha ao submeter: {type(exc).__name__}: {exc}")
                )
                return None
            now = self._clock()
            return _Inflight(message, job, future, started=now, deadline=now + self._job_timeout)

    def _load_or_create(self, job_message: JobMessage) -> Job:
        job = self._repository.find(job_message.job_id)
        if job is None:
            job = job_message.to_job()
            self._repository.add(job)
        return job

    # --- Término ---------------------------------------------------------------------

    def _finish(self, item: _Inflight) -> None:
        result, error = self._collect(item)
        duration = self._clock() - item.started
        if error is None:
            assert result is not None
            job = item.job.succeed(result)
            self._repository.save(job)
            self._bus.publish(JobSucceeded(job.id, duration))
            self._settle(self._consumer.ack, item.message)
        else:
            self._on_failure(item.message, item.job, error)

    def _collect(self, item: _Inflight) -> tuple[dict[str, Any] | None, BaseException | None]:
        remaining = max(0.0, item.deadline - self._clock())
        try:
            return item.future.result(timeout=remaining), None
        except TimeoutError as exc:
            if item.future.done():  # o handler é que levantou TimeoutError
                return None, exc
            item.future.cancel()
            return None, JobTimeoutError(f"excedeu o limite de {self._job_timeout:g}s")
        except BrokenProcessPool:
            return None, WorkerCrashedError("o processo que executava o job morreu")
        except CancelledError:
            return None, TransientError("execução cancelada")
        except Exception as exc:  # noqa: BLE001  (o erro do handler é classificado adiante)
            return None, exc

    def _on_failure(self, message: QueueMessage, job: Job, error: BaseException) -> None:
        text = f"{type(error).__name__}: {error}"
        if not is_retryable(error):
            self._repository.save(job.fail(text))
            self._bus.publish(JobFailed(job.id, text))
            logger.warning("job falhou de forma permanente: %s", text)
            # O job está resolvido (como FAILED); a mensagem cumpriu seu papel.
            self._settle(self._consumer.ack, message)
        elif job.can_retry:
            delay = self._retry_policy.delay_for(job.attempts)
            self._repository.save(job.retry(text))
            self._bus.publish(JobRetrying(job.id, text, job.attempts, delay))
            logger.warning("falha transitória (%s); retry em %.1fs", text, delay)
            self._settle(self._consumer.nack, message, requeue=True, delay=delay)
        else:
            self._dead_letter(message, job, f"tentativas esgotadas: {text}")

    def _dead_letter(self, message: QueueMessage, job: Job, text: str) -> None:
        self._repository.save(job.dead_letter(text))
        self._bus.publish(JobDeadLettered(job.id, text))
        logger.error("job enviado para a dead-letter: %s", text)
        self._settle(self._consumer.nack, message, requeue=False)

    def _reject(self, message: QueueMessage, exc: PermanentError) -> None:
        """Mensagem que não dá para processar: vai para a DLQ para inspeção humana."""
        logger.warning("mensagem %s rejeitada: %s", message.id, exc)
        with suppress_logged(Exception, log=logger, message="falha ao registrar rejeição"):
            try:
                job_message = JobMessage.from_body(message.body)
            except MessageDecodeError:
                job_message = None
            if job_message is not None:
                job = self._load_or_create(job_message)
                if not job.is_terminal:
                    text = f"mensagem rejeitada: {exc}"
                    self._repository.save(job.fail(text))
                    self._bus.publish(JobFailed(job.id, text))
        self._settle(self._consumer.nack, message, requeue=False)

    def _settle(self, action: Callable[..., None], message: QueueMessage, **kwargs: Any) -> None:
        """Ack/nack nunca devem derrubar o worker: sem confirmação, a fila reentrega."""
        try:
            action(message, **kwargs)
        except QueueError as exc:
            logger.warning("não foi possível confirmar %s: %s", message.id, exc)
