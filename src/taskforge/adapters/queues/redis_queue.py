"""Fila sobre Redis (listas) com o padrão "reliable queue".

Estruturas (prefixo ``taskforge:<fila>``):

* ``ready``      lista; produtores fazem LPUSH, consumidores fazem BLMOVE pela direita (FIFO)
* ``processing`` lista; mensagens entregues e ainda não confirmadas
* ``leases``     zset id -> instante de expiração do lease (detecta consumidor morto)
* ``delayed``    zset envelope -> instante em que volta para ``ready`` (backoff)
* ``deliveries`` hash id -> número de entregas
* ``dead``       lista de mensagens mortas (dead-letter)

Garantia: at-least-once. Em corridas raras entre o "reaper" e um ack tardio, uma mensagem
pode ser entregue duas vezes (preferimos duplicar a perder). O Worker é idempotente: um
job já finalizado só é confirmado de novo, nunca reexecutado.

Para volume alto, considere Redis Streams com consumer groups (XREADGROUP/XAUTOCLAIM),
que entregam contagem de entregas e reivindicação de pendentes de forma nativa.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Mapping
from typing import Any
from uuid import uuid4

from taskforge.adapters.queues._errors import translate_errors
from taskforge.exceptions import MessageRejectedError
from taskforge.ports.queue import MessageQueue, QueueMessage

logger = logging.getLogger(__name__)

_MAX_PROMOTE = 100


class RedisQueue(MessageQueue):
    def __init__(
        self,
        name: str = "taskforge-jobs",
        *,
        client: Any,
        max_deliveries: int = 5,
        visibility_timeout: float = 60.0,
        clock: Callable[[], float] = time.time,
        close_client: bool = False,
    ) -> None:
        self.name = name
        self._client = client
        self._max_deliveries = max_deliveries
        self._visibility_timeout = visibility_timeout
        self._clock = clock
        self._close_client = close_client
        prefix = f"taskforge:{name}"
        self._ready = f"{prefix}:ready"
        self._processing = f"{prefix}:processing"
        self._leases = f"{prefix}:leases"
        self._delayed = f"{prefix}:delayed"
        self._deliveries = f"{prefix}:deliveries"
        self._dead = f"{prefix}:dead"

    # --- Publisher -------------------------------------------------------------------

    def publish(
        self,
        body: str,
        *,
        attributes: Mapping[str, str] | None = None,
        dedup_key: str | None = None,
    ) -> str:
        message_id = uuid4().hex
        envelope = json.dumps(
            {"id": message_id, "body": body, "attributes": dict(attributes or {})}
        )
        with translate_errors("redis publish"):
            self._client.lpush(self._ready, envelope)
        return message_id

    # --- Consumer --------------------------------------------------------------------

    def receive(self, max_messages: int = 1, wait_seconds: float = 1.0) -> list[QueueMessage]:
        raws: list[str] = []
        with translate_errors("redis receive"):
            self._promote_delayed()
            self.reclaim_expired()
            first = self._move_to_processing()
            if first is None and wait_seconds > 0:
                first = self._client.blmove(
                    self._ready, self._processing, wait_seconds, src="RIGHT", dest="LEFT"
                )
            if first is not None:
                raws.append(first)
                while len(raws) < max_messages:
                    nxt = self._move_to_processing()
                    if nxt is None:
                        break
                    raws.append(nxt)
            return self._lease_all(raws)

    def ack(self, message: QueueMessage) -> None:
        with translate_errors("redis ack"):
            pipe = self._client.pipeline(transaction=True)
            pipe.lrem(self._processing, 1, message.receipt)
            pipe.zrem(self._leases, message.id)
            pipe.hdel(self._deliveries, message.id)
            removed = pipe.execute()[0]
        if not removed:
            raise MessageRejectedError("recibo desconhecido ou expirado")

    def nack(self, message: QueueMessage, *, requeue: bool = True, delay: float = 0.0) -> None:
        with translate_errors("redis nack"):
            self._release(
                message.receipt,
                message.id,
                message.delivery_count,
                requeue=requeue,
                delay=delay,
            )

    def close(self) -> None:
        if self._close_client:
            self._client.close()

    # --- Manutenção ------------------------------------------------------------------

    def reclaim_expired(self) -> int:
        """Devolve à fila mensagens cujo consumidor sumiu (lease expirado)."""
        expired = {
            _as_str(member)
            for member in self._client.zrangebyscore(self._leases, "-inf", self._clock())
        }
        if not expired:
            return 0
        reclaimed = 0
        for raw in self._client.lrange(self._processing, 0, -1):
            envelope = _parse(raw)
            if envelope is None or envelope["id"] not in expired:
                continue
            deliveries = int(self._client.hget(self._deliveries, envelope["id"]) or 1)
            self._release(_as_str(raw), envelope["id"], deliveries, requeue=True, delay=0.0)
            reclaimed += 1
            logger.warning("mensagem %s reivindicada (lease expirado)", envelope["id"])
        return reclaimed

    @property
    def depth(self) -> int:
        return int(self._client.llen(self._ready))

    @property
    def dead_letters(self) -> list[str]:
        return [_as_str(r) for r in self._client.lrange(self._dead, 0, -1)]

    # --- Internos --------------------------------------------------------------------

    def _move_to_processing(self) -> str | None:
        raw = self._client.lmove(self._ready, self._processing, src="RIGHT", dest="LEFT")
        return None if raw is None else _as_str(raw)

    def _lease_all(self, raws: list[str]) -> list[QueueMessage]:
        messages: list[QueueMessage] = []
        expires_at = self._clock() + self._visibility_timeout
        for raw in raws:
            envelope = _parse(raw)
            if envelope is None:  # lixo na fila: manda para a DLQ em vez de travar o consumo
                logger.error("envelope corrompido movido para a DLQ")
                pipe = self._client.pipeline(transaction=True)
                pipe.lpush(self._dead, raw)
                pipe.lrem(self._processing, 1, raw)
                pipe.execute()
                continue
            deliveries = int(self._client.hincrby(self._deliveries, envelope["id"], 1))
            self._client.zadd(self._leases, {envelope["id"]: expires_at})
            messages.append(
                QueueMessage(
                    id=envelope["id"],
                    body=envelope["body"],
                    receipt=raw,
                    delivery_count=deliveries,
                    attributes=envelope.get("attributes", {}),
                )
            )
        return messages

    def _release(
        self, raw: str, message_id: str, deliveries: int, *, requeue: bool, delay: float
    ) -> None:
        """Destino primeiro, remoção depois (MULTI/EXEC): duplicar é aceitável, perder não."""
        pipe = self._client.pipeline(transaction=True)
        if not requeue or deliveries >= self._max_deliveries:
            pipe.lpush(self._dead, raw)
            pipe.hdel(self._deliveries, message_id)
        elif delay > 0:
            pipe.zadd(self._delayed, {raw: self._clock() + delay})
        else:
            pipe.lpush(self._ready, raw)
        pipe.lrem(self._processing, 1, raw)
        pipe.zrem(self._leases, message_id)
        pipe.execute()

    def _promote_delayed(self) -> None:
        due = self._client.zrangebyscore(
            self._delayed, "-inf", self._clock(), start=0, num=_MAX_PROMOTE
        )
        for raw in due:
            pipe = self._client.pipeline(transaction=True)
            pipe.zrem(self._delayed, raw)
            pipe.lpush(self._ready, raw)
            pipe.execute()


def _as_str(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _parse(raw: Any) -> dict[str, Any] | None:
    try:
        data = json.loads(_as_str(raw))
    except ValueError:
        return None
    if isinstance(data, dict) and "id" in data and "body" in data:
        return data
    return None
