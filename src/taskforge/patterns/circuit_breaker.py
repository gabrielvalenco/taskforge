"""Circuit breaker thread-safe (CLOSED -> OPEN -> HALF_OPEN -> CLOSED).

Evita martelar um broker que já está fora do ar e dá tempo para ele se recuperar.
Uso::

    breaker = CircuitBreaker("sqs", failure_threshold=5, recovery_timeout=30)
    with breaker.guard():
        queue.publish(...)
"""

from __future__ import annotations

import functools
import logging
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from enum import StrEnum
from typing import ParamSpec, TypeVar

from taskforge.exceptions import CircuitOpenError, is_retryable

P = ParamSpec("P")
T = TypeVar("T")

logger = logging.getLogger(__name__)


class BreakerState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 1,
        trip_on: Callable[[BaseException], bool] = is_retryable,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1 or half_open_max_calls < 1:
            raise ValueError("failure_threshold e half_open_max_calls devem ser >= 1")
        self.name = name
        self._threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_max_calls = half_open_max_calls
        self._trip_on = trip_on
        self._clock = clock
        self._lock = threading.Lock()
        self._state = BreakerState.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._probes = 0

    @property
    def state(self) -> BreakerState:
        with self._lock:
            self._maybe_half_open()
            return self._state

    def _maybe_half_open(self) -> None:
        if (
            self._state is BreakerState.OPEN
            and self._clock() - self._opened_at >= self._recovery_timeout
        ):
            self._set_state(BreakerState.HALF_OPEN)
            self._probes = 0

    def _set_state(self, new: BreakerState) -> None:
        if new is not self._state:
            logger.warning("circuit breaker %r: %s -> %s", self.name, self._state, new)
            self._state = new

    def _open(self) -> None:
        self._opened_at = self._clock()
        self._failures = 0
        self._set_state(BreakerState.OPEN)

    def _acquire(self) -> bool:
        """Devolve True se esta chamada é uma sonda (probe) em HALF_OPEN."""
        with self._lock:
            self._maybe_half_open()
            if self._state is BreakerState.OPEN:
                raise CircuitOpenError(f"circuito {self.name!r} aberto")
            if self._state is BreakerState.HALF_OPEN:
                if self._probes >= self._half_open_max_calls:
                    raise CircuitOpenError(f"circuito {self.name!r} em teste (half-open)")
                self._probes += 1
                return True
            return False

    def _on_success(self, is_probe: bool) -> None:
        with self._lock:
            if is_probe:
                self._probes = max(0, self._probes - 1)
                if self._state is BreakerState.HALF_OPEN:
                    self._failures = 0
                    self._set_state(BreakerState.CLOSED)
            elif self._state is BreakerState.CLOSED:
                self._failures = 0

    def _on_failure(self, is_probe: bool) -> None:
        with self._lock:
            if is_probe:
                self._probes = max(0, self._probes - 1)
                if self._state is BreakerState.HALF_OPEN:
                    self._open()
            elif self._state is BreakerState.CLOSED:
                self._failures += 1
                if self._failures >= self._threshold:
                    self._open()

    def _on_neutral(self, is_probe: bool) -> None:
        with self._lock:
            if is_probe:
                self._probes = max(0, self._probes - 1)

    @contextmanager
    def guard(self) -> Iterator[None]:
        """Protege um bloco. Erros que não "tripam" (ex.: validação) contam como sucesso."""
        is_probe = self._acquire()
        try:
            yield
        except BaseException as exc:
            if not isinstance(exc, Exception):
                self._on_neutral(is_probe)  # KeyboardInterrupt etc.: sem veredito
            elif self._trip_on(exc):
                self._on_failure(is_probe)
            else:
                self._on_success(is_probe)
            raise
        else:
            self._on_success(is_probe)

    def __call__(self, fn: Callable[P, T]) -> Callable[P, T]:
        """Uso como decorator: ``@breaker``."""

        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            with self.guard():
                return fn(*args, **kwargs)

        return wrapper
