"""Porta de fila.

Interface Segregation: quem só publica (``JobDispatcher``) depende de ``Publisher``; quem
só consome (``Worker``) depende de ``Consumer``. Os adapters (memória, Redis, SQS,
Service Bus) implementam ``MessageQueue``, que une as duas.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import TracebackType
from typing import Self


@dataclass(frozen=True, slots=True)
class QueueMessage:
    id: str
    body: str
    receipt: str  # token opaco usado em ack/nack (ReceiptHandle, lock token, envelope...)
    delivery_count: int = 1
    attributes: Mapping[str, str] = field(default_factory=dict)


class Publisher(ABC):
    @abstractmethod
    def publish(
        self,
        body: str,
        *,
        attributes: Mapping[str, str] | None = None,
        dedup_key: str | None = None,
    ) -> str:
        """Publica ``body`` e devolve o id da mensagem.

        ``dedup_key`` é uma chave de idempotência da *publicação*: quando o broker suporta
        (Service Bus: MessageId + duplicate detection), reenvios com a mesma chave não
        geram mensagens duplicadas. Brokers sem suporte a ignoram.
        """


class Consumer(ABC):
    @abstractmethod
    def receive(self, max_messages: int = 1, wait_seconds: float = 1.0) -> list[QueueMessage]:
        """Recebe até ``max_messages`` (long polling por até ``wait_seconds``).

        Semântica at-least-once: a mensagem fica invisível até ``ack``/``nack`` ou até
        o timeout de visibilidade expirar.
        """

    @abstractmethod
    def ack(self, message: QueueMessage) -> None:
        """Confirma o processamento; a mensagem sai definitivamente da fila."""

    @abstractmethod
    def nack(self, message: QueueMessage, *, requeue: bool = True, delay: float = 0.0) -> None:
        """Devolve a mensagem.

        ``requeue=True`` reentrega após ``delay`` segundos (backoff). ``requeue=False``
        manda para a dead-letter queue.
        """


class MessageQueue(Publisher, Consumer, ABC):
    name: str = "queue"

    def close(self) -> None:
        """Libera conexões. Chamado automaticamente ao sair do ``with``."""

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
