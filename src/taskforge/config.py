"""Configuração via variáveis de ambiente (12-factor), validada com pydantic."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Literal, Self

import pydantic
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

from taskforge.exceptions import ConfigurationError

ENV_PREFIX = "TASKFORGE_"

QueueBackend = Literal["memory", "redis", "sqs", "servicebus"]
ExecutionBackendName = Literal["local", "celery"]


def _default_processes() -> int:
    return max(1, (os.cpu_count() or 2) - 1)


class Settings(BaseModel):
    """Todas as chaves viram ``TASKFORGE_<NOME_EM_MAIUSCULAS>``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # Fila
    queue_backend: QueueBackend = "memory"
    queue_name: str = Field(default="taskforge-jobs", min_length=1)
    max_deliveries: int = Field(default=5, ge=1, le=100)
    visibility_timeout_seconds: float = Field(default=60.0, gt=0)

    redis_url: str = "redis://localhost:6379/0"

    sqs_queue_url: str | None = None
    sqs_dlq_url: str | None = None
    aws_region: str | None = None
    aws_endpoint_url: str | None = None  # LocalStack em desenvolvimento

    servicebus_connection_string: SecretStr | None = None
    servicebus_queue_name: str | None = None

    # Persistência
    database_path: str = "taskforge.db"

    # Execução
    execution_backend: ExecutionBackendName = "local"
    io_threads: int = Field(default=16, ge=1, le=512)
    cpu_processes: int = Field(default_factory=_default_processes, ge=1, le=128)
    batch_size: int = Field(default=8, ge=1, le=10)
    poll_wait_seconds: float = Field(default=5.0, ge=0, le=20)
    job_timeout_seconds: float = Field(default=300.0, gt=0)
    max_message_bytes: int = Field(default=256_000, gt=0)
    retry_base_delay_seconds: float = Field(default=1.0, ge=0)
    retry_max_delay_seconds: float = Field(default=60.0, ge=0)

    # Celery
    celery_broker_url: str | None = None
    celery_result_backend: str | None = None
    celery_task_retries: int = Field(default=2, ge=0, le=10)
    celery_eager: bool = False

    # Operação
    heartbeat_file: str = "/tmp/taskforge.heartbeat"  # noqa: S108
    log_level: str = "INFO"
    log_json: bool = True

    @property
    def broker_url(self) -> str:
        return self.celery_broker_url or self.redis_url

    @property
    def result_backend_url(self) -> str:
        return self.celery_result_backend or self.redis_url

    @model_validator(mode="after")
    def _check_backend_requirements(self) -> Self:
        missing: list[str] = []
        if self.queue_backend == "sqs" and not self.sqs_queue_url:
            missing.append(f"{ENV_PREFIX}SQS_QUEUE_URL")
        if self.queue_backend == "servicebus":
            if self.servicebus_connection_string is None:
                missing.append(f"{ENV_PREFIX}SERVICEBUS_CONNECTION_STRING")
            if not self.servicebus_queue_name:
                missing.append(f"{ENV_PREFIX}SERVICEBUS_QUEUE_NAME")
        if missing:
            raise ValueError("variáveis obrigatórias ausentes: " + ", ".join(missing))
        return self

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        source = os.environ if env is None else env
        values: dict[str, str] = {}
        for name in cls.model_fields:
            raw = source.get(f"{ENV_PREFIX}{name.upper()}", "")
            if raw != "":
                values[name] = raw
        try:
            return cls.model_validate(values)
        except pydantic.ValidationError as exc:
            problems = "; ".join(
                f"{ENV_PREFIX}{'.'.join(str(p) for p in err['loc']).upper() or '?'}: {err['msg']}"
                for err in exc.errors(include_input=False, include_url=False)
            )
            raise ConfigurationError(f"configuração inválida: {problems}") from exc
