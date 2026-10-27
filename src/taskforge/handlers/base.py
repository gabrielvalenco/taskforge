"""Contrato dos handlers (Strategy + Template Method)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any, ClassVar

from taskforge.domain.models import ExecutionMode


class JobHandler(ABC):
    """Cada handler é uma *estratégia* intercambiável, escolhida pelo nome do job.

    ``run`` é o template method: valida, executa e finaliza. Subclasses implementam
    apenas ``execute`` e, se quiserem, ``validate``/``finalize``. Handlers devem ser
    **stateless** e **idempotentes**: a entrega da fila é at-least-once.
    """

    name: ClassVar[str]
    mode: ClassVar[ExecutionMode] = ExecutionMode.THREAD

    def run(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        validated = self.validate(payload)
        return self.finalize(self.execute(validated))

    def validate(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        """Gancho: levante ``JobValidationError`` para payload inválido."""
        return payload

    @abstractmethod
    def execute(self, payload: Mapping[str, Any]) -> dict[str, Any]: ...

    def finalize(self, result: dict[str, Any]) -> dict[str, Any]:
        """Gancho: pós-processamento do resultado."""
        return result
