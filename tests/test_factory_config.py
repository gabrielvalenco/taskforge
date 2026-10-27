from __future__ import annotations

import pytest

from taskforge.adapters.queues.factory import QueueFactory, default_queue_factory
from taskforge.adapters.queues.memory import InMemoryQueue
from taskforge.adapters.queues.redis_queue import RedisQueue
from taskforge.adapters.queues.sqs import SQSQueue
from taskforge.config import Settings
from taskforge.exceptions import ConfigurationError


def test_defaults() -> None:
    settings = Settings.from_env({})
    assert settings.queue_backend == "memory"
    assert settings.broker_url == settings.redis_url
    assert settings.cpu_processes >= 1


def test_from_env_parses_types_and_ignores_empty_values() -> None:
    settings = Settings.from_env(
        {
            "TASKFORGE_QUEUE_BACKEND": "redis",
            "TASKFORGE_IO_THREADS": "32",
            "TASKFORGE_JOB_TIMEOUT_SECONDS": "12.5",
            "TASKFORGE_LOG_JSON": "false",
            "TASKFORGE_REDIS_URL": "",  # vazio = usar o padrão
            "OUTRA_VARIAVEL": "ignorada",
        }
    )
    assert settings.queue_backend == "redis"
    assert settings.io_threads == 32
    assert settings.job_timeout_seconds == 12.5
    assert settings.log_json is False
    assert settings.redis_url.startswith("redis://")


def test_invalid_value_names_the_variable() -> None:
    with pytest.raises(ConfigurationError, match="TASKFORGE_IO_THREADS"):
        Settings.from_env({"TASKFORGE_IO_THREADS": "0"})
    with pytest.raises(ConfigurationError, match="TASKFORGE_QUEUE_BACKEND"):
        Settings.from_env({"TASKFORGE_QUEUE_BACKEND": "kafka"})


def test_sqs_requires_queue_url() -> None:
    with pytest.raises(ConfigurationError, match="TASKFORGE_SQS_QUEUE_URL"):
        Settings.from_env({"TASKFORGE_QUEUE_BACKEND": "sqs"})


def test_servicebus_requires_connection_string_and_queue() -> None:
    with pytest.raises(ConfigurationError) as info:
        Settings.from_env({"TASKFORGE_QUEUE_BACKEND": "servicebus"})
    assert "SERVICEBUS_CONNECTION_STRING" in str(info.value)
    assert "SERVICEBUS_QUEUE_NAME" in str(info.value)


def test_secret_is_not_leaked_in_repr() -> None:
    settings = Settings.from_env(
        {
            "TASKFORGE_QUEUE_BACKEND": "servicebus",
            "TASKFORGE_SERVICEBUS_CONNECTION_STRING": "Endpoint=sb://x/;SharedAccessKey=SEGREDO",
            "TASKFORGE_SERVICEBUS_QUEUE_NAME": "jobs",
        }
    )
    assert "SEGREDO" not in repr(settings)
    assert settings.servicebus_connection_string is not None
    assert "SEGREDO" in settings.servicebus_connection_string.get_secret_value()


def test_factory_builds_memory_queue() -> None:
    queue = default_queue_factory().create(Settings(queue_name="abc", max_deliveries=3))
    assert isinstance(queue, InMemoryQueue) and queue.name == "abc"


def test_factory_builds_redis_queue_lazily() -> None:
    queue = default_queue_factory().create(Settings(queue_backend="redis"))  # não conecta ainda
    assert isinstance(queue, RedisQueue)
    queue.close()


def test_factory_builds_sqs_queue() -> None:
    settings = Settings(
        queue_backend="sqs",
        sqs_queue_url="https://sqs.us-east-1.amazonaws.com/1/jobs",
        aws_region="us-east-1",
        aws_endpoint_url="http://localhost:4566",
    )
    assert isinstance(default_queue_factory().create(settings), SQSQueue)


def test_factory_is_open_for_extension() -> None:
    factory = QueueFactory()
    factory.register("custom", lambda s: InMemoryQueue("custom"))
    assert factory.backends == ["custom"]
    with pytest.raises(ConfigurationError, match="já registrado"):
        factory.register("custom", lambda s: InMemoryQueue())
    factory.register("custom", lambda s: InMemoryQueue("novo"), replace=True)


def test_factory_unknown_backend_lists_available() -> None:
    with pytest.raises(ConfigurationError, match="disponíveis"):
        QueueFactory().create(Settings())


def test_missing_sdk_gives_actionable_message(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    real_import_module = importlib.import_module

    def fake_import_module(name: str, *args: object, **kwargs: object) -> object:
        if name == "boto3":
            raise ImportError("no boto3")
        return real_import_module(name, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(importlib, "import_module", fake_import_module)
    settings = Settings(queue_backend="sqs", sqs_queue_url="https://x/1/q", aws_region="us-east-1")
    with pytest.raises(ConfigurationError, match=r"taskforge\[aws\]"):
        default_queue_factory().create(settings)
