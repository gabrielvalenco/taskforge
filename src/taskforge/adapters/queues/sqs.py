"""Adapter AWS SQS (padrão Adapter sobre o cliente boto3).

O cliente é injetado: em produção vem de ``boto3.client("sqs")``, nos testes é um fake.
A dead-letter queue "de verdade" é configurada no Terraform (redrive policy com
``maxReceiveCount``); aqui ``dead_letter_url`` só serve para o caso de rejeição explícita.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from taskforge.adapters.queues._errors import translate_errors
from taskforge.ports.queue import MessageQueue, QueueMessage

logger = logging.getLogger(__name__)

_SQS_MAX_BATCH = 10
_SQS_MAX_LONG_POLL = 20
_SQS_MAX_VISIBILITY = 43_200  # 12 h


class SQSQueue(MessageQueue):
    def __init__(
        self,
        queue_url: str,
        *,
        client: Any,
        dead_letter_url: str | None = None,
        visibility_timeout: int = 60,
        name: str | None = None,
    ) -> None:
        self.name = name or queue_url.rsplit("/", 1)[-1]
        self._url = queue_url
        self._client = client
        self._dlq_url = dead_letter_url
        self._visibility_timeout = visibility_timeout

    def publish(
        self,
        body: str,
        *,
        attributes: Mapping[str, str] | None = None,
        dedup_key: str | None = None,
    ) -> str:
        # Filas standard não deduplicam (só FIFO, via MessageDeduplicationId). O Worker é
        # idempotente, então uma duplicata de publicação é inofensiva.
        with translate_errors("sqs send_message"):
            response = self._client.send_message(
                QueueUrl=self._url,
                MessageBody=body,
                MessageAttributes=_encode_attributes(attributes),
            )
        return str(response["MessageId"])

    def receive(self, max_messages: int = 1, wait_seconds: float = 1.0) -> list[QueueMessage]:
        with translate_errors("sqs receive_message"):
            response = self._client.receive_message(
                QueueUrl=self._url,
                MaxNumberOfMessages=max(1, min(max_messages, _SQS_MAX_BATCH)),
                WaitTimeSeconds=max(0, min(int(wait_seconds), _SQS_MAX_LONG_POLL)),
                VisibilityTimeout=self._visibility_timeout,
                AttributeNames=["ApproximateReceiveCount"],
                MessageAttributeNames=["All"],
            )
        return [
            QueueMessage(
                id=raw["MessageId"],
                body=raw["Body"],
                receipt=raw["ReceiptHandle"],
                delivery_count=int(raw.get("Attributes", {}).get("ApproximateReceiveCount", 1)),
                attributes={
                    key: value.get("StringValue", "")
                    for key, value in raw.get("MessageAttributes", {}).items()
                },
            )
            for raw in response.get("Messages", [])
        ]

    def ack(self, message: QueueMessage) -> None:
        with translate_errors("sqs delete_message"):
            self._client.delete_message(QueueUrl=self._url, ReceiptHandle=message.receipt)

    def nack(self, message: QueueMessage, *, requeue: bool = True, delay: float = 0.0) -> None:
        if requeue:
            # Visibilidade = backoff: a mensagem reaparece depois de ``delay`` segundos.
            self._change_visibility(message, int(max(0.0, delay)))
        elif self._dlq_url:
            with translate_errors("sqs dead-letter"):
                self._client.send_message(
                    QueueUrl=self._dlq_url,
                    MessageBody=message.body,
                    MessageAttributes=_encode_attributes(message.attributes),
                )
                self._client.delete_message(QueueUrl=self._url, ReceiptHandle=message.receipt)
        else:
            logger.warning(
                "sem dead_letter_url: devolvendo %s com visibilidade 0; a redrive policy da "
                "fila a moverá para a DLQ ao atingir maxReceiveCount",
                message.id,
            )
            self._change_visibility(message, 0)

    def _change_visibility(self, message: QueueMessage, seconds: int) -> None:
        with translate_errors("sqs change_message_visibility"):
            self._client.change_message_visibility(
                QueueUrl=self._url,
                ReceiptHandle=message.receipt,
                VisibilityTimeout=min(seconds, _SQS_MAX_VISIBILITY),
            )


def _encode_attributes(attributes: Mapping[str, str] | None) -> dict[str, dict[str, str]]:
    return {
        key: {"DataType": "String", "StringValue": value}
        for key, value in (attributes or {}).items()
    }
