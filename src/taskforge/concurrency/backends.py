"""Backend de execução local: threads e processos do próprio worker."""

from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import Future
from typing import Any

from taskforge.concurrency.pools import PoolManager
from taskforge.domain.models import ExecutionMode
from taskforge.handlers.registry import HandlerRegistry
from taskforge.handlers.runner import execute_handler, run_handler
from taskforge.ports.execution import ExecutionBackend


class LocalExecutionBackend(ExecutionBackend):
    """INLINE/THREAD usam o ``registry`` injetado (aceita handlers de teste).

    PROCESS usa o registry global do processo filho (via ``execute_handler``), porque
    registries não atravessam a fronteira de processo.
    """

    def __init__(self, pools: PoolManager, registry: HandlerRegistry) -> None:
        self._pools = pools
        self._registry = registry

    def submit(
        self, handler: str, payload: Mapping[str, Any], mode: ExecutionMode
    ) -> Future[dict[str, Any]]:
        data = dict(payload)
        if mode is ExecutionMode.PROCESS:
            return self._pools.submit(mode, execute_handler, handler, data)
        return self._pools.submit(mode, run_handler, self._registry, handler, data)

    def close(self) -> None:
        self._pools.close(wait=True)
