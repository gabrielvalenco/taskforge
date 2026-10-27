"""Hierarquia de exceções do TaskForge.

Decisões de design:

* Toda exceção própria herda de ``TaskForgeError``: a borda da aplicação (CLI, worker)
  precisa de um único ``except`` para tudo que é "esperado".
* ``retryable`` é atributo de *classe*. Quem decide se vale tentar de novo é o tipo do
  erro, e não uma cadeia de ``isinstance`` espalhada pelo código (Open/Closed: um novo
  erro transitório só precisa declarar ``retryable = True``).
* Os construtores aceitam apenas ``message`` como argumento posicional obrigatório. Isso
  mantém as exceções serializáveis com ``pickle``, requisito para atravessar
  ``ProcessPoolExecutor`` e o backend de resultados do Celery.
"""

from __future__ import annotations

from typing import Any


class TaskForgeError(Exception):
    """Base de todas as exceções do projeto."""

    retryable: bool = False

    def __init__(self, message: str = "", *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.context: dict[str, Any] = dict(context or {})

    def __str__(self) -> str:
        return self.message


# --- Classificação por comportamento de retry -----------------------------------------


class TransientError(TaskForgeError):
    """Falha provavelmente temporária: vale tentar de novo."""

    retryable = True


class PermanentError(TaskForgeError):
    """Falha determinística: repetir não resolve."""

    retryable = False


# --- Configuração ----------------------------------------------------------------------


class ConfigurationError(TaskForgeError):
    """Configuração ausente ou inválida (erro do operador, não de runtime)."""


# --- Domínio ---------------------------------------------------------------------------


class InvalidTransitionError(PermanentError):
    """Transição de estado inválida para um Job."""


class JobValidationError(PermanentError):
    """O job (ou seu payload) não passou na validação."""


class MessageDecodeError(JobValidationError):
    """O corpo da mensagem não é um JobMessage válido."""


class HandlerNotFoundError(JobValidationError):
    """Nenhum handler registrado com o nome pedido."""


class HandlerError(PermanentError):
    """O handler falhou com uma exceção não classificada."""


# --- Persistência ----------------------------------------------------------------------


class RepositoryError(TaskForgeError):
    """Falha ao acessar o repositório de jobs."""


class JobNotFoundError(RepositoryError):
    """Job inexistente."""


class RepositoryUnavailableError(RepositoryError, TransientError):
    """Repositório temporariamente indisponível (lock, rede)."""

    retryable = True


# --- Filas -----------------------------------------------------------------------------


class QueueError(TaskForgeError):
    """Falha genérica de fila."""


class QueueUnavailableError(QueueError, TransientError):
    """Broker inacessível ou respondeu com erro temporário."""

    retryable = True


class MessageRejectedError(QueueError):
    """Recibo expirado/desconhecido ao confirmar ou devolver uma mensagem."""


# --- Resiliência e execução ------------------------------------------------------------


class CircuitOpenError(TransientError):
    """O circuit breaker está aberto: a chamada nem foi tentada."""


class RetryExhaustedError(TaskForgeError):
    """Todas as tentativas foram consumidas. A causa original fica em ``__cause__``."""

    def __init__(
        self,
        message: str = "",
        *,
        attempts: int = 0,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, context=context)
        self.attempts = attempts


class JobTimeoutError(TransientError):
    """O job excedeu o tempo máximo de execução."""


class WorkerCrashedError(TransientError):
    """O processo que executava o job morreu (OOM, segfault, kill)."""


_NETWORK_ERRORS: tuple[type[BaseException], ...] = (ConnectionError, TimeoutError)


def is_retryable(exc: BaseException) -> bool:
    """Decide se um erro é transitório.

    Erros do projeto declaram isso na própria classe. Para exceções de bibliotecas,
    só erros de rede óbvios são considerados transitórios; o resto é permanente
    (falhar rápido é mais seguro do que repetir um bug em loop).
    """
    if isinstance(exc, TaskForgeError):
        return exc.retryable
    return isinstance(exc, _NETWORK_ERRORS)
