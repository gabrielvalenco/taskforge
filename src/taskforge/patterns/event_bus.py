"""Barramento de eventos em memória (padrão Observer) + coletor de métricas.

Isolamento de falhas: um subscriber com bug nunca derruba quem publica nem os demais
subscribers.
"""

from __future__ import annotations

import contextlib
import logging
import threading
from collections import Counter, defaultdict
from collections.abc import Callable
from typing import Any, TypeVar

from taskforge.domain.events import Event, JobSucceeded

E = TypeVar("E", bound=Event)

logger = logging.getLogger(__name__)


class EventBus:
    def __init__(self) -> None:
        self._handlers: dict[type[Event], list[Callable[[Any], None]]] = defaultdict(list)
        self._lock = threading.Lock()

    def subscribe(self, event_type: type[E], handler: Callable[[E], None]) -> Callable[[], None]:
        """Registra ``handler`` para ``event_type`` e suas subclasses.

        Devolve uma função que cancela a inscrição.
        """
        with self._lock:
            self._handlers[event_type].append(handler)

        def unsubscribe() -> None:
            with self._lock, contextlib.suppress(ValueError):
                self._handlers[event_type].remove(handler)

        return unsubscribe

    def publish(self, event: Event) -> None:
        with self._lock:
            targets = [h for cls in type(event).__mro__ for h in self._handlers.get(cls, ())]
        for handler in targets:
            try:
                handler(event)
            except Exception:
                logger.exception("subscriber %r falhou ao tratar %s", handler, type(event).__name__)


class MetricsCollector:
    """Subscriber que conta eventos por tipo. Troque por Prometheus/StatsD na borda."""

    def __init__(self) -> None:
        self._counts: Counter[str] = Counter()
        self._total_duration = 0.0
        self._lock = threading.Lock()

    def attach(self, bus: EventBus) -> None:
        bus.subscribe(Event, self.on_event)

    def on_event(self, event: Event) -> None:
        with self._lock:
            self._counts[type(event).__name__] += 1
            if isinstance(event, JobSucceeded):
                self._total_duration += event.duration_s

    def snapshot(self) -> dict[str, float]:
        with self._lock:
            data: dict[str, float] = dict(self._counts)
            data["total_success_duration_s"] = round(self._total_duration, 6)
            return data
