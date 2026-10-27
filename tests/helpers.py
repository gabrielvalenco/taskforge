"""Utilitários de teste. Funções de módulo aqui precisam ser importáveis por processos filhos."""

from __future__ import annotations

import os
import threading
import time
from collections import defaultdict
from collections.abc import Mapping
from typing import Any, ClassVar

from taskforge.domain.models import ExecutionMode
from taskforge.exceptions import TransientError
from taskforge.handlers.base import JobHandler


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# --- Funções para o ProcessPoolExecutor (precisam ser top-level) -------------------------


def add(a: int, b: int) -> int:
    return a + b


def get_pid() -> int:
    return os.getpid()


def crash_process() -> None:
    os._exit(1)


# --- Handlers de teste (rodam em THREAD/INLINE dentro do processo de teste) --------------


class FlakyHandler(JobHandler):
    name = "test.flaky"
    mode = ExecutionMode.THREAD
    calls: ClassVar[dict[str, int]] = defaultdict(int)
    _lock = threading.Lock()

    def execute(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            self.calls[payload["key"]] += 1
            n = self.calls[payload["key"]]
        if n <= payload.get("fail_times", 0):
            raise TransientError(f"falha {n}")
        return {"calls": n}


class BarrierHandler(JobHandler):
    """Só termina se N execuções estiverem rodando ao mesmo tempo."""

    name = "test.barrier"
    mode = ExecutionMode.THREAD
    barrier: ClassVar[threading.Barrier | None] = None

    def execute(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        assert self.barrier is not None
        self.barrier.wait(timeout=5)
        return {"ok": True}


class SlowHandler(JobHandler):
    name = "test.slow"
    mode = ExecutionMode.THREAD

    def execute(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        time.sleep(float(payload["seconds"]))
        return {"slept": payload["seconds"]}


class BoomHandler(JobHandler):
    """Levanta uma exceção "crua" (não classificada)."""

    name = "test.boom"
    mode = ExecutionMode.INLINE

    def execute(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        raise ValueError("bug no handler")


class ConnHandler(JobHandler):
    """Levanta um erro de rede "cru": deve ser tratado como transitório."""

    name = "test.conn"
    mode = ExecutionMode.INLINE

    def execute(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        raise ConnectionError("conexão recusada")
