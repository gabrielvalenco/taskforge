"""Modelos de domínio: Job (estado) e JobMessage (contrato na fila).

``Job`` é imutável. Cada transição devolve uma *nova* instância, o que elimina uma classe
inteira de bugs de concorrência (ninguém muta um objeto compartilhado entre threads) e
torna as transições fáceis de testar.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self
from uuid import uuid4

import pydantic
from pydantic import BaseModel, ConfigDict, Field

from taskforge.exceptions import InvalidTransitionError, MessageDecodeError

HANDLER_NAME_PATTERN = r"^[a-z][a-z0-9_.-]{0,99}$"
_MAX_ERROR_CHARS = 2000


def utcnow() -> datetime:
    return datetime.now(UTC)


def summarize_validation_error(exc: pydantic.ValidationError) -> str:
    """Resume erros do pydantic sem vazar o valor de entrada (pode ter dados sensíveis)."""
    parts = [
        f"{'.'.join(str(p) for p in err['loc']) or '<raiz>'}: {err['msg']}"
        for err in exc.errors(include_input=False, include_url=False)
    ]
    return "; ".join(parts)


class ExecutionMode(StrEnum):
    """Onde um handler deve rodar. Depende da natureza da carga, não do handler em si."""

    INLINE = "inline"  # trivial, roda na thread do worker
    THREAD = "thread"  # I/O bound: rede, disco, bancos
    PROCESS = "process"  # CPU bound: contorna o GIL


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"


_ALLOWED: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.PENDING: frozenset({JobStatus.RUNNING, JobStatus.FAILED}),
    # RUNNING -> RUNNING cobre a redelivery após queda do worker (mensagem volta à fila).
    JobStatus.RUNNING: frozenset(
        {
            JobStatus.RUNNING,
            JobStatus.SUCCEEDED,
            JobStatus.RETRYING,
            JobStatus.FAILED,
            JobStatus.DEAD_LETTERED,
        }
    ),
    JobStatus.RETRYING: frozenset({JobStatus.RUNNING, JobStatus.FAILED, JobStatus.DEAD_LETTERED}),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.DEAD_LETTERED: frozenset(),
}

_TERMINAL = frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD_LETTERED})


class Job(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: uuid4().hex)
    handler: str = Field(pattern=HANDLER_NAME_PATTERN)
    payload: dict[str, Any] = Field(default_factory=dict)
    status: JobStatus = JobStatus.PENDING
    attempts: int = Field(default=0, ge=0)
    max_attempts: int = Field(default=3, ge=1, le=20)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    result: dict[str, Any] | None = None
    error: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status in _TERMINAL

    @property
    def can_retry(self) -> bool:
        return self.attempts < self.max_attempts

    def _transition(self, target: JobStatus, **changes: Any) -> Self:
        if target not in _ALLOWED[self.status]:
            raise InvalidTransitionError(
                f"transição inválida {self.status.value} -> {target.value}",
                context={"job_id": self.id},
            )
        return self.model_copy(update={"status": target, "updated_at": utcnow(), **changes})

    def start(self) -> Self:
        return self._transition(JobStatus.RUNNING, attempts=self.attempts + 1)

    def succeed(self, result: dict[str, Any]) -> Self:
        return self._transition(JobStatus.SUCCEEDED, result=result, error=None)

    def retry(self, error: str) -> Self:
        return self._transition(JobStatus.RETRYING, error=error[:_MAX_ERROR_CHARS])

    def fail(self, error: str) -> Self:
        return self._transition(JobStatus.FAILED, error=error[:_MAX_ERROR_CHARS])

    def dead_letter(self, error: str) -> Self:
        return self._transition(JobStatus.DEAD_LETTERED, error=error[:_MAX_ERROR_CHARS])


class JobMessage(BaseModel):
    """Contrato publicado na fila. Só carrega o necessário para executar.

    O estado do job (tentativas, resultado) vive no repositório, que é a fonte da verdade.
    Mensagens são "ordens de trabalho", não registros de estado.
    """

    model_config = ConfigDict(frozen=True, extra="ignore")

    schema_version: int = 1
    job_id: str = Field(min_length=1, max_length=64)
    handler: str = Field(pattern=HANDLER_NAME_PATTERN)
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=3, ge=1, le=20)

    @classmethod
    def from_job(cls, job: Job) -> JobMessage:
        return cls(
            job_id=job.id,
            handler=job.handler,
            payload=job.payload,
            max_attempts=job.max_attempts,
        )

    def to_job(self) -> Job:
        return Job(
            id=self.job_id,
            handler=self.handler,
            payload=self.payload,
            max_attempts=self.max_attempts,
        )

    def to_body(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_body(cls, body: str) -> JobMessage:
        try:
            return cls.model_validate_json(body)
        except pydantic.ValidationError as exc:
            raise MessageDecodeError(
                f"mensagem inválida: {summarize_validation_error(exc)}"
            ) from exc
