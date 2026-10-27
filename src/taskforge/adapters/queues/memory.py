"""Fila em memória: desenvolvimento local e testes.

Implementa a mesma semântica das filas reais: visibilidade com timeout, redelivery,
contador de entregas, atraso no requeue e dead-letter após ``max_deliveries``.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from uuid import uuid4

from taskforge.exceptions import MessageRejectedError, QueueUnavailableError
from taskforge.ports.queue import MessageQueue, QueueMessage


@dataclass(slots=True)
class _Entry:
    id: str
    body: str
    attributes: dict[str, str]
    deliveries: int = 0
    available_at: float = 0.0


@dataclass(slots=True)
class _Lease:
    entry: _Entry
    expires_at: float


class InMemoryQueue(MessageQueue):
    def __init__(
        self,
        name: str = "memory",
        *,
        max_deliveries: int = 5,
        visibility_timeout: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.name = name
        self._max_deliveries = max_deliveries
        self._visibility_timeout = visibility_timeout
        self._clock = clock
        self._ready: list[_Entry] = []
        self._leases: dict[str, _Lease] = {}
        self._dead: list[QueueMessage] = []
        self._cond = threading.Condition()
        self._closed = False

    # --- Publisher -------------------------------------------------------------------

    def publish(
        self,
        body: str,
        *,
        attributes: Mapping[str, str] | None = None,
        dedup_key: str | None = None,
    ) -> str:
        with self._cond:
            if self._closed:
                raise QueueUnavailableError(f"fila {self.name!r} fechada")
            entry = _Entry(id=uuid4().hex, body=body, attributes=dict(attributes or {}))
            self._ready.append(entry)
            self._cond.notify()
            return entry.id

    # --- Consumer --------------------------------------------------------------------

    def receive(self, max_messages: int = 1, wait_seconds: float = 1.0) -> list[QueueMessage]:
        deadline = self._clock() + max(0.0, wait_seconds)
        with self._cond:
            while True:
                if self._closed:
                    return []
                self._reclaim_expired()
                batch = self._take(max_messages)
                if batch:
                    return batch
                remaining = deadline - self._clock()
                if remaining <= 0:
                    return []
                self._cond.wait(timeout=min(remaining, 0.05))

    def ack(self, message: QueueMessage) -> None:
        with self._cond:
            if self._leases.pop(message.receipt, None) is None:
                raise MessageRejectedError("recibo desconhecido ou expirado")

    def nack(self, message: QueueMessage, *, requeue: bool = True, delay: float = 0.0) -> None:
        with self._cond:
            lease = self._leases.pop(message.receipt, None)
            if lease is None:
                raise MessageRejectedError("recibo desconhecido ou expirado")
            self._release(lease.entry, requeue=requeue, delay=delay)

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    # --- Introspecção (útil em testes e na CLI) --------------------------------------

    @property
    def depth(self) -> int:
        with self._cond:
            return len(self._ready)

    @property
    def in_flight(self) -> int:
        with self._cond:
            return len(self._leases)

    @property
    def dead_letters(self) -> list[QueueMessage]:
        with self._cond:
            return list(self._dead)

    # --- Internos (sempre chamados com o lock) ---------------------------------------

    def _take(self, limit: int) -> list[QueueMessage]:
        now = self._clock()
        taken: list[QueueMessage] = []
        remaining: list[_Entry] = []
        for entry in self._ready:
            if len(taken) < limit and entry.available_at <= now:
                entry.deliveries += 1
                receipt = uuid4().hex
                self._leases[receipt] = _Lease(entry, now + self._visibility_timeout)
                taken.append(
                    QueueMessage(
                        id=entry.id,
                        body=entry.body,
                        receipt=receipt,
                        delivery_count=entry.deliveries,
                        attributes=dict(entry.attributes),
                    )
                )
            else:
                remaining.append(entry)
        self._ready = remaining
        return taken

    def _reclaim_expired(self) -> None:
        now = self._clock()
        expired = [r for r, lease in self._leases.items() if lease.expires_at <= now]
        for receipt in expired:
            self._release(self._leases.pop(receipt).entry, requeue=True, delay=0.0)

    def _release(self, entry: _Entry, *, requeue: bool, delay: float) -> None:
        if not requeue or entry.deliveries >= self._max_deliveries:
            self._dead.append(
                QueueMessage(
                    id=entry.id,
                    body=entry.body,
                    receipt="",
                    delivery_count=entry.deliveries,
                    attributes=dict(entry.attributes),
                )
            )
            return
        entry.available_at = self._clock() + max(0.0, delay)
        self._ready.append(entry)
        self._cond.notify()
