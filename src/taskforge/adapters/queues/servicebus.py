"""Adapter Azure Service Bus (modo peek-lock).

Detalhes que diferem do SQS:

* a confirmação precisa ser feita pelo *mesmo receiver* que recebeu a mensagem, então o
  adapter guarda a mensagem nativa indexada pelo lock token;
* ``receiver``/``sender`` não são thread-safe: todas as operações passam por um lock;
* o Service Bus move para a DLQ sozinho ao estourar ``MaxDeliveryCount`` (configurado no
  Terraform); ``nack(requeue=False)`` faz o dead-letter explícito;
* abandonar uma mensagem não permite atraso, então com ``delay > 0`` agendamos uma cópia
  futura e completamos a original.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from taskforge.adapters.queues._errors import translate_errors
from taskforge.exceptions import MessageRejectedError
from taskforge.ports.queue import MessageQueue, QueueMessage

logger = logging.getLogger(__name__)


class ServiceBusQueue(MessageQueue):
    def __init__(
        self,
        queue_name: str,
        *,
        client: Any,
        message_factory: Callable[..., Any] | None = None,
        close_client: bool = False,
    ) -> None:
        self.name = queue_name
        self._client = client
        self._factory = message_factory or _default_message_factory
        self._close_client = close_client
        self._lock = threading.Lock()
        self._inflight: dict[str, Any] = {}
        with translate_errors("service bus connect"):
            self._sender = client.get_queue_sender(queue_name=queue_name)
            self._receiver = client.get_queue_receiver(queue_name=queue_name)

    def publish(
        self,
        body: str,
        *,
        attributes: Mapping[str, str] | None = None,
        dedup_key: str | None = None,
    ) -> str:
        kwargs: dict[str, Any] = {"application_properties": dict(attributes or {})}
        if dedup_key is not None:
            # Com duplicate detection habilitado na fila (ver Terraform), o broker descarta
            # reenvios com o mesmo MessageId dentro da janela configurada.
            kwargs["message_id"] = dedup_key
        message = self._factory(body, **kwargs)
        with self._lock, translate_errors("service bus send"):
            self._sender.send_messages(message)
        return str(getattr(message, "message_id", "") or "")

    def receive(self, max_messages: int = 1, wait_seconds: float = 1.0) -> list[QueueMessage]:
        with self._lock:
            with translate_errors("service bus receive"):
                raw_messages = self._receiver.receive_messages(
                    max_message_count=max_messages, max_wait_time=max(wait_seconds, 0.1)
                )
            result: list[QueueMessage] = []
            for raw in raw_messages:
                receipt = str(raw.lock_token)
                self._inflight[receipt] = raw
                result.append(
                    QueueMessage(
                        id=str(raw.message_id),
                        body=str(raw),
                        receipt=receipt,
                        # O SDK pode contar a partir de 0 ou 1; o Service Bus aplica
                        # MaxDeliveryCount por conta própria, então isto é só informativo.
                        delivery_count=max(int(raw.delivery_count or 0), 1),
                        attributes=_decode_properties(raw.application_properties),
                    )
                )
            return result

    def ack(self, message: QueueMessage) -> None:
        with self._lock:
            raw = self._pop(message)
            with translate_errors("service bus complete"):
                self._receiver.complete_message(raw)

    def nack(self, message: QueueMessage, *, requeue: bool = True, delay: float = 0.0) -> None:
        with self._lock:
            raw = self._pop(message)
            with translate_errors("service bus nack"):
                if not requeue:
                    self._receiver.dead_letter_message(
                        raw, reason="rejected", error_description="rejeitada pelo worker"
                    )
                elif delay > 0:
                    clone = self._factory(
                        message.body, application_properties=dict(message.attributes)
                    )
                    when = datetime.now(UTC) + timedelta(seconds=delay)
                    self._sender.schedule_messages(clone, when)
                    self._receiver.complete_message(raw)
                else:
                    self._receiver.abandon_message(raw)

    def close(self) -> None:
        with self._lock:
            for resource in (self._receiver, self._sender):
                try:
                    resource.close()
                except Exception:
                    logger.exception("falha ao fechar recurso do service bus")
            if self._close_client:
                self._client.close()

    def _pop(self, message: QueueMessage) -> Any:
        raw = self._inflight.pop(message.receipt, None)
        if raw is None:
            raise MessageRejectedError("lock token desconhecido ou já finalizado")
        return raw


def _default_message_factory(body: str, **kwargs: Any) -> Any:
    from azure.servicebus import ServiceBusMessage

    return ServiceBusMessage(body, **kwargs)


def _decode_properties(properties: Mapping[Any, Any] | None) -> dict[str, str]:
    """O SDK devolve chaves/valores como bytes dependendo da versão do protocolo AMQP."""

    def text(value: Any) -> str:
        return value.decode() if isinstance(value, bytes) else str(value)

    return {text(k): text(v) for k, v in (properties or {}).items()}
