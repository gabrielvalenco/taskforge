"""Fábrica do app Celery (broker e result backend em Redis).

Duas filas separadas por natureza da carga, cada uma com seu tipo de worker:

* ``taskforge.io``  workers com pool de threads/gevent  (I/O-bound, alta concorrência)
* ``taskforge.cpu`` workers com pool prefork            (CPU-bound, um processo por core)

Configurações de confiabilidade: ``acks_late`` + ``reject_on_worker_lost`` (a task volta
para a fila se o processo morrer), ``prefetch_multiplier=1`` (distribuição justa de tasks
longas) e serialização JSON apenas (nunca pickle vindo da rede).
"""

from __future__ import annotations

from typing import Any

from celery import Celery

from taskforge.celery_app.tasks import CPU_QUEUE, IO_QUEUE, RUN_HANDLER_TASK, register_tasks
from taskforge.config import Settings

__all__ = ["CPU_QUEUE", "IO_QUEUE", "RUN_HANDLER_TASK", "create_celery_app"]


def create_celery_app(settings: Settings) -> Celery:
    app = Celery("taskforge", broker=settings.broker_url, backend=settings.result_backend_url)
    conf: dict[str, Any] = {
        "task_serializer": "json",
        "result_serializer": "json",
        "accept_content": ["json"],
        "timezone": "UTC",
        "enable_utc": True,
        "task_acks_late": True,
        "task_reject_on_worker_lost": True,
        "worker_prefetch_multiplier": 1,
        "task_track_started": True,
        "result_expires": 3600,
        "broker_connection_retry_on_startup": True,
        "task_default_queue": IO_QUEUE,
        "task_routes": {RUN_HANDLER_TASK: {"queue": IO_QUEUE}},
        "task_soft_time_limit": settings.job_timeout_seconds,
        "task_time_limit": settings.job_timeout_seconds + 30,
        "task_always_eager": settings.celery_eager,
        "task_store_eager_result": settings.celery_eager,
    }
    app.conf.update(conf)
    register_tasks(app, max_retries=settings.celery_task_retries)
    return app
