"""Context managers utilitários.

Exemplos das duas formas de escrever um context manager em Python:

* ``@contextmanager`` para fluxos simples (``timed``, ``suppress_logged``);
* classe com ``__enter__``/``__exit__`` quando há estado a restaurar (``ShutdownSignal``).
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from types import FrameType, TracebackType
from typing import Self

logger = logging.getLogger(__name__)


class Stopwatch:
    def __init__(self, clock: Callable[[], float] = time.perf_counter) -> None:
        self._clock = clock
        self._start = clock()
        self._end: float | None = None

    def stop(self) -> None:
        if self._end is None:
            self._end = self._clock()

    @property
    def elapsed(self) -> float:
        end = self._clock() if self._end is None else self._end
        return end - self._start


@contextmanager
def timed(
    label: str,
    *,
    log: logging.Logger | None = None,
    level: int = logging.DEBUG,
    clock: Callable[[], float] = time.perf_counter,
) -> Iterator[Stopwatch]:
    """Mede o tempo do bloco, inclusive quando ele levanta exceção."""
    watch = Stopwatch(clock)
    try:
        yield watch
    finally:
        watch.stop()
        if log is not None:
            log.log(level, "%s concluído em %.3fs", label, watch.elapsed)


@contextmanager
def suppress_logged(
    *exceptions: type[BaseException],
    log: logging.Logger = logger,
    message: str = "erro ignorado",
) -> Iterator[None]:
    """Como ``contextlib.suppress``, mas deixa rastro no log. Útil em limpeza (close)."""
    caught: tuple[type[BaseException], ...] = exceptions or (Exception,)
    try:
        yield
    except caught:
        log.exception(message)


class ShutdownSignal:
    """Converte SIGINT/SIGTERM em um ``threading.Event`` e restaura os handlers ao sair.

    O primeiro sinal pede parada graciosa (termina o lote atual). O segundo, enquanto o
    shutdown ainda está em andamento, levanta ``KeyboardInterrupt`` para forçar a saída.
    Fora da thread principal o Python não permite instalar handlers; nesse caso o
    objeto funciona como um Event comum (útil em testes).
    """

    def __init__(
        self, signals: tuple[signal.Signals, ...] = (signal.SIGINT, signal.SIGTERM)
    ) -> None:
        self.event = threading.Event()
        self._signals = signals
        self._previous: dict[signal.Signals, object] = {}

    def __enter__(self) -> Self:
        if threading.current_thread() is threading.main_thread():
            for sig in self._signals:
                self._previous[sig] = signal.signal(sig, self._handle)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        for sig, previous in self._previous.items():
            signal.signal(sig, previous)  # type: ignore[arg-type]
        self._previous.clear()

    def _handle(self, signum: int, frame: FrameType | None) -> None:
        if self.event.is_set():
            raise KeyboardInterrupt
        logger.info("sinal %s recebido: encerrando após o lote atual", signum)
        self.event.set()

    def trigger(self) -> None:
        self.event.set()
