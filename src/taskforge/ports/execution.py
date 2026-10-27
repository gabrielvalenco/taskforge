"""Porta de execução: *onde* o código de um handler roda (pools locais ou Celery)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from concurrent.futures import Future
from types import TracebackType
from typing import Any, Self

from taskforge.domain.models import ExecutionMode


class ExecutionBackend(ABC):
    @abstractmethod
    def submit(
        self, handler: str, payload: Mapping[str, Any], mode: ExecutionMode
    ) -> Future[dict[str, Any]]:
        """Agenda a execução e devolve um Future com o resultado do handler."""

    def close(self) -> None:  # noqa: B027
        """Encerra pools/conexões."""

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
