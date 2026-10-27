"""Tradução de erros de SDKs de terceiros para a hierarquia do projeto."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from taskforge.exceptions import QueueUnavailableError, TaskForgeError


@contextmanager
def translate_errors(
    action: str, target: type[TaskForgeError] = QueueUnavailableError
) -> Iterator[None]:
    """Converte qualquer exceção do SDK em ``target``, preservando a causa original.

    Usar apenas ao redor das chamadas ao cliente remoto (nunca ao redor de lógica
    própria), senão bugs locais virariam "fila indisponível".
    """
    try:
        yield
    except TaskForgeError:
        raise
    except Exception as exc:
        raise target(f"{action} falhou: {type(exc).__name__}: {exc}") from exc
