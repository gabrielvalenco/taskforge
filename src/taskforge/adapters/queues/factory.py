"""Factory de filas com registro dinâmico (Factory Method + Registry).

Open/Closed: para adicionar um novo broker (Pub/Sub, Kafka...) basta chamar
``factory.register("nome", builder)``; nenhum código existente muda. Os imports de SDKs
pesados acontecem dentro dos builders, então quem usa só Redis não precisa ter boto3.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any

from taskforge.adapters.queues.memory import InMemoryQueue
from taskforge.config import Settings
from taskforge.exceptions import ConfigurationError
from taskforge.ports.queue import MessageQueue

QueueBuilder = Callable[[Settings], MessageQueue]


class QueueFactory:
    def __init__(self) -> None:
        self._builders: dict[str, QueueBuilder] = {}

    def register(self, backend: str, builder: QueueBuilder, *, replace: bool = False) -> None:
        if backend in self._builders and not replace:
            raise ConfigurationError(f"backend de fila já registrado: {backend!r}")
        self._builders[backend] = builder

    @property
    def backends(self) -> list[str]:
        return sorted(self._builders)

    def create(self, settings: Settings) -> MessageQueue:
        try:
            builder = self._builders[settings.queue_backend]
        except KeyError:
            raise ConfigurationError(
                f"backend de fila desconhecido: {settings.queue_backend!r} "
                f"(disponíveis: {', '.join(self.backends)})"
            ) from None
        return builder(settings)


def _require(module: str, extra: str) -> Any:
    try:
        return importlib.import_module(module)
    except ImportError as exc:
        raise ConfigurationError(
            f"o módulo {module!r} é necessário para este backend: pip install 'taskforge[{extra}]'"
        ) from exc


def _build_memory(settings: Settings) -> MessageQueue:
    return InMemoryQueue(
        settings.queue_name,
        max_deliveries=settings.max_deliveries,
        visibility_timeout=settings.visibility_timeout_seconds,
    )


def _build_redis(settings: Settings) -> MessageQueue:
    from taskforge.adapters.queues.redis_queue import RedisQueue

    redis = _require("redis", "redis")
    client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    return RedisQueue(
        settings.queue_name,
        client=client,
        max_deliveries=settings.max_deliveries,
        visibility_timeout=settings.visibility_timeout_seconds,
        close_client=True,
    )


def _build_sqs(settings: Settings) -> MessageQueue:
    from taskforge.adapters.queues.sqs import SQSQueue

    boto3 = _require("boto3", "aws")
    client = boto3.client(
        "sqs", region_name=settings.aws_region, endpoint_url=settings.aws_endpoint_url
    )
    assert settings.sqs_queue_url is not None  # garantido pelo validador de Settings
    return SQSQueue(
        settings.sqs_queue_url,
        client=client,
        dead_letter_url=settings.sqs_dlq_url,
        visibility_timeout=int(settings.visibility_timeout_seconds),
    )


def _build_servicebus(settings: Settings) -> MessageQueue:
    from taskforge.adapters.queues.servicebus import ServiceBusQueue

    servicebus = _require("azure.servicebus", "azure")
    assert settings.servicebus_connection_string is not None
    assert settings.servicebus_queue_name is not None
    client = servicebus.ServiceBusClient.from_connection_string(
        settings.servicebus_connection_string.get_secret_value()
    )
    return ServiceBusQueue(settings.servicebus_queue_name, client=client, close_client=True)


def default_queue_factory() -> QueueFactory:
    factory = QueueFactory()
    factory.register("memory", _build_memory)
    factory.register("redis", _build_redis)
    factory.register("sqs", _build_sqs)
    factory.register("servicebus", _build_servicebus)
    return factory
