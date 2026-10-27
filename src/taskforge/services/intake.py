"""Validação de mensagens recebidas (Chain of Responsibility).

Cada elo faz uma checagem e passa adiante. Para acrescentar uma regra (assinatura,
allowlist de tenant, schema version) basta criar outro elo, sem tocar nos existentes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from taskforge.domain.models import JobMessage
from taskforge.exceptions import HandlerNotFoundError, JobValidationError
from taskforge.handlers.registry import HandlerRegistry
from taskforge.ports.queue import QueueMessage


@dataclass(slots=True)
class IntakeContext:
    message: QueueMessage
    job_message: JobMessage | None = None


class IntakeValidator(ABC):
    def __init__(self) -> None:
        self._next: IntakeValidator | None = None

    def set_next(self, nxt: IntakeValidator) -> IntakeValidator:
        self._next = nxt
        return nxt

    def validate(self, ctx: IntakeContext) -> IntakeContext:
        self._check(ctx)
        return self._next.validate(ctx) if self._next is not None else ctx

    @abstractmethod
    def _check(self, ctx: IntakeContext) -> None: ...


class SizeValidator(IntakeValidator):
    def __init__(self, max_bytes: int) -> None:
        super().__init__()
        self._max_bytes = max_bytes

    def _check(self, ctx: IntakeContext) -> None:
        size = len(ctx.message.body.encode("utf-8"))
        if size > self._max_bytes:
            raise JobValidationError(
                f"mensagem de {size} bytes excede o limite de {self._max_bytes}"
            )


class DecodeValidator(IntakeValidator):
    def _check(self, ctx: IntakeContext) -> None:
        ctx.job_message = JobMessage.from_body(ctx.message.body)


class KnownHandlerValidator(IntakeValidator):
    def __init__(self, registry: HandlerRegistry) -> None:
        super().__init__()
        self._registry = registry

    def _check(self, ctx: IntakeContext) -> None:
        assert ctx.job_message is not None, "DecodeValidator deve vir antes"
        if ctx.job_message.handler not in self._registry:
            raise HandlerNotFoundError(f"handler desconhecido: {ctx.job_message.handler!r}")


def build_intake_chain(registry: HandlerRegistry, *, max_bytes: int) -> IntakeValidator:
    head = SizeValidator(max_bytes)
    head.set_next(DecodeValidator()).set_next(KnownHandlerValidator(registry))
    return head
