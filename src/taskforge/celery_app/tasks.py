"""Tasks Celery. A lógica de negócio continua em ``execute_handler``: a task é só cola.

Camadas de retry (propositalmente separadas):

* Celery (aqui): retries rápidos *no mesmo worker* para falhas transitórias, com backoff
  exponencial e jitter (``autoretry_for=(TransientError,)``);
* Worker/fila (``services.worker``): se ainda falhar, o job volta para a fila com atraso e
  conta como uma tentativa do Job, até ``max_attempts`` e então dead-letter.
"""

from __future__ import annotations

from typing import Any

from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded

from taskforge.exceptions import HandlerError, JobTimeoutError, TransientError
from taskforge.handlers.runner import execute_handler

RUN_HANDLER_TASK = "taskforge.run_handler"
IO_QUEUE = "taskforge.io"
CPU_QUEUE = "taskforge.cpu"


def register_tasks(app: Celery, *, max_retries: int = 2) -> Any:
    @app.task(  # type: ignore[untyped-decorator]
        name=RUN_HANDLER_TASK,
        bind=True,
        acks_late=True,
        autoretry_for=(TransientError,),
        retry_backoff=2,
        retry_backoff_max=60,
        retry_jitter=True,
        max_retries=max_retries,
    )
    def run_handler(self: Any, handler: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            return execute_handler(handler, payload)
        except HandlerError as exc:
            # O wrapper do runner transforma qualquer Exception em HandlerError; o soft
            # time limit do Celery chega como exceção e precisa virar timeout (transitório).
            if isinstance(exc.__cause__, SoftTimeLimitExceeded):
                raise JobTimeoutError(f"soft time limit em {handler!r}") from exc.__cause__
            raise

    return run_handler
