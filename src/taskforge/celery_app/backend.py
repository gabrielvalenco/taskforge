"""ExecutionBackend que delega a execução a workers Celery.

O Worker do TaskForge continua dono do ciclo de vida da mensagem (ack/nack/DLQ); o Celery
só executa. Cada ``submit`` publica a task e devolve um Future que é resolvido por uma
thread aguardando o resultado, assim o lote inteiro roda em paralelo no cluster.
"""

from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from celery import Celery
from celery.exceptions import TimeoutError as CeleryTimeoutError
from celery.exceptions import WorkerLostError

from taskforge.celery_app.tasks import CPU_QUEUE, IO_QUEUE, RUN_HANDLER_TASK
from taskforge.domain.models import ExecutionMode
from taskforge.exceptions import JobTimeoutError, WorkerCrashedError
from taskforge.ports.execution import ExecutionBackend


class CeleryExecutionBackend(ExecutionBackend):
    def __init__(self, app: Celery, *, result_timeout: float, max_waiters: int = 32) -> None:
        self._app = app
        self._timeout = result_timeout
        self._waiters = ThreadPoolExecutor(
            max_workers=max_waiters, thread_name_prefix="celery-wait"
        )

    def submit(
        self, handler: str, payload: Mapping[str, Any], mode: ExecutionMode
    ) -> Future[dict[str, Any]]:
        queue = CPU_QUEUE if mode is ExecutionMode.PROCESS else IO_QUEUE
        # apply_async respeita task_always_eager (usado nos testes); send_task não.
        async_result = self._app.tasks[RUN_HANDLER_TASK].apply_async(
            args=[handler, dict(payload)], queue=queue
        )
        return self._waiters.submit(self._wait, async_result)

    def _wait(self, async_result: Any) -> dict[str, Any]:
        try:
            # Esta thread nunca roda dentro de uma task Celery. O guarda "Never call result.get()
            # within a task" usa um flag global ao processo, que no modo eager é ligado por outra
            # thread executando uma task inline, gerando um falso positivo intermitente.
            result: dict[str, Any] = async_result.get(
                timeout=self._timeout, propagate=True, disable_sync_subtasks=False
            )
        except CeleryTimeoutError as exc:
            raise JobTimeoutError(f"sem resultado do Celery em {self._timeout:g}s") from exc
        except WorkerLostError as exc:
            raise WorkerCrashedError("worker Celery morreu durante a task") from exc
        return result

    def close(self) -> None:
        self._waiters.shutdown(wait=True, cancel_futures=True)
        self._app.close()
