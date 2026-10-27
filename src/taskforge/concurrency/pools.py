"""Gerenciador de pools de threads e processos.

Escolha do modo (regra de bolso):

* I/O-bound (rede, disco, banco): threads. O GIL é liberado durante a espera.
* CPU-bound (hash, parsing pesado, numérico puro): processos. Cada processo tem seu GIL.

Pontos de atenção implementados aqui:

* processos usam ``spawn`` (seguro com threads; ``fork`` em processo multithread pode
  travar) e, opcionalmente, ``max_tasks_per_child`` para conter vazamento de memória;
* se um processo filho morre (OOM, segfault), o ``ProcessPoolExecutor`` inteiro fica
  "quebrado". O manager descarta o pool quebrado e cria outro na próxima submissão;
* os pools são criados sob demanda e encerrados pelo context manager.
"""

from __future__ import annotations

import functools
import logging
import multiprocessing
import threading
from collections.abc import Callable
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from types import TracebackType
from typing import Any, Self

from taskforge.domain.models import ExecutionMode

logger = logging.getLogger(__name__)


def _run_inline(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Future[Any]:
    future: Future[Any] = Future()
    try:
        future.set_result(fn(*args, **kwargs))
    except Exception as exc:  # noqa: BLE001  (o erro é entregue ao chamador via Future)
        future.set_exception(exc)
    return future


class PoolManager:
    def __init__(
        self,
        *,
        io_threads: int = 16,
        cpu_processes: int = 2,
        start_method: str = "spawn",
        max_tasks_per_child: int | None = None,
    ) -> None:
        self._io_threads = io_threads
        self._cpu_processes = cpu_processes
        self._mp_context = multiprocessing.get_context(start_method)
        self._max_tasks_per_child = max_tasks_per_child
        self._lock = threading.Lock()
        self._threads: ThreadPoolExecutor | None = None
        self._processes: ProcessPoolExecutor | None = None
        self._closed = False

    def submit(
        self, mode: ExecutionMode, fn: Callable[..., Any], /, *args: Any, **kwargs: Any
    ) -> Future[Any]:
        if mode is ExecutionMode.INLINE:
            return _run_inline(fn, *args, **kwargs)
        if mode is ExecutionMode.THREAD:
            return self._thread_pool().submit(fn, *args, **kwargs)
        return self._submit_process(fn, *args, **kwargs)

    # --- Threads ---------------------------------------------------------------------

    def _thread_pool(self) -> ThreadPoolExecutor:
        with self._lock:
            self._ensure_open()
            if self._threads is None:
                self._threads = ThreadPoolExecutor(
                    max_workers=self._io_threads, thread_name_prefix="taskforge-io"
                )
            return self._threads

    # --- Processos -------------------------------------------------------------------

    def _process_pool(self) -> ProcessPoolExecutor:
        with self._lock:
            self._ensure_open()
            if self._processes is None:
                self._processes = ProcessPoolExecutor(
                    max_workers=self._cpu_processes,
                    mp_context=self._mp_context,
                    max_tasks_per_child=self._max_tasks_per_child,
                )
            return self._processes

    def _submit_process(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Future[Any]:
        for attempt in (1, 2):
            pool = self._process_pool()
            try:
                future = pool.submit(fn, *args, **kwargs)
            except BrokenProcessPool:
                self._discard_process_pool(pool)
                if attempt == 2:
                    raise
                continue
            future.add_done_callback(functools.partial(self._on_process_done, pool=pool))
            return future
        raise AssertionError("inalcançável")  # pragma: no cover

    def _on_process_done(self, future: Future[Any], pool: ProcessPoolExecutor) -> None:
        if not future.cancelled() and isinstance(future.exception(), BrokenProcessPool):
            self._discard_process_pool(pool)

    def _discard_process_pool(self, pool: ProcessPoolExecutor) -> None:
        # Um pool quebrado já foi encerrado pelo próprio executor; basta esquecê-lo.
        with self._lock:
            if self._processes is pool:
                logger.error("pool de processos quebrado; será recriado na próxima submissão")
                self._processes = None

    # --- Ciclo de vida ---------------------------------------------------------------

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("PoolManager já foi encerrado")

    def close(self, *, wait: bool = True) -> None:
        with self._lock:
            self._closed = True
            pools = [p for p in (self._threads, self._processes) if p is not None]
            self._threads = self._processes = None
        for pool in pools:
            pool.shutdown(wait=wait, cancel_futures=not wait)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close(wait=True)
