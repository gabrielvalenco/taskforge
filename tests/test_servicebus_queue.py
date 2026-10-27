from __future__ import annotations

from typing import Any

import pytest

from taskforge.adapters.queues.servicebus import ServiceBusQueue
from taskforge.exceptions import MessageRejectedError, QueueUnavailableError


class FakeMessage:
    def __init__(
        self,
        body: str,
        message_id: str = "mid",
        lock_token: str = "lock-1",
        delivery_count: int = 0,
        application_properties: dict[Any, Any] | None = None,
    ) -> None:
        self.body = body
        self.message_id = message_id
        self.lock_token = lock_token
        self.delivery_count = delivery_count
        self.application_properties = application_properties or {}

    def __str__(self) -> str:
        return self.body


class FakeSender:
    def __init__(self) -> None:
        self.sent: list[FakeMessage] = []
        self.scheduled: list[tuple[FakeMessage, Any]] = []
        self.closed = False
        self.error: Exception | None = None

    def send_messages(self, message: FakeMessage) -> None:
        if self.error:
            raise self.error
        self.sent.append(message)

    def schedule_messages(self, message: FakeMessage, when: Any) -> None:
        self.scheduled.append((message, when))

    def close(self) -> None:
        self.closed = True


class FakeReceiver:
    def __init__(self) -> None:
        self.inbox: list[FakeMessage] = []
        self.completed: list[FakeMessage] = []
        self.abandoned: list[FakeMessage] = []
        self.dead: list[tuple[FakeMessage, str]] = []
        self.closed = False
        self.receive_kwargs: dict[str, Any] = {}

    def receive_messages(self, max_message_count: int, max_wait_time: float) -> list[FakeMessage]:
        self.receive_kwargs = {"max": max_message_count, "wait": max_wait_time}
        batch, self.inbox = self.inbox[:max_message_count], self.inbox[max_message_count:]
        return batch

    def complete_message(self, message: FakeMessage) -> None:
        self.completed.append(message)

    def abandon_message(self, message: FakeMessage) -> None:
        self.abandoned.append(message)

    def dead_letter_message(
        self, message: FakeMessage, reason: str, error_description: str
    ) -> None:
        self.dead.append((message, reason))

    def close(self) -> None:
        self.closed = True


class FakeClient:
    def __init__(self) -> None:
        self.sender = FakeSender()
        self.receiver = FakeReceiver()
        self.closed = False

    def get_queue_sender(self, queue_name: str) -> FakeSender:
        return self.sender

    def get_queue_receiver(self, queue_name: str) -> FakeReceiver:
        return self.receiver

    def close(self) -> None:
        self.closed = True


def factory(body: str, **kwargs: Any) -> FakeMessage:
    return FakeMessage(
        body,
        message_id=kwargs.get("message_id", "out"),
        application_properties=kwargs.get("application_properties"),
    )


@pytest.fixture
def client() -> FakeClient:
    return FakeClient()


@pytest.fixture
def queue(client: FakeClient) -> ServiceBusQueue:
    return ServiceBusQueue("jobs", client=client, message_factory=factory)


def test_publish(queue: ServiceBusQueue, client: FakeClient) -> None:
    assert queue.publish("corpo", attributes={"k": "v"}) == "out"
    [sent] = client.sender.sent
    assert sent.body == "corpo" and sent.application_properties == {"k": "v"}


def test_publish_dedup_key_becomes_message_id(queue: ServiceBusQueue, client: FakeClient) -> None:
    queue.publish("corpo", dedup_key="job-123")
    assert client.sender.sent[0].message_id == "job-123"


def test_receive_maps_message_and_decodes_bytes_properties(
    queue: ServiceBusQueue, client: FakeClient
) -> None:
    client.receiver.inbox = [
        FakeMessage("b", delivery_count=2, application_properties={b"handler": b"echo"})
    ]
    [msg] = queue.receive(max_messages=5, wait_seconds=0)
    assert (msg.id, msg.body, msg.receipt, msg.delivery_count) == ("mid", "b", "lock-1", 2)
    assert dict(msg.attributes) == {"handler": "echo"}
    assert client.receiver.receive_kwargs["wait"] >= 0.1, "espera mínima evita busy loop"


def test_first_delivery_count_is_at_least_one(queue: ServiceBusQueue, client: FakeClient) -> None:
    client.receiver.inbox = [FakeMessage("b", delivery_count=0)]
    assert queue.receive()[0].delivery_count == 1


def test_ack_completes_native_message(queue: ServiceBusQueue, client: FakeClient) -> None:
    client.receiver.inbox = [FakeMessage("b")]
    msg = queue.receive()[0]
    queue.ack(msg)
    assert [m.lock_token for m in client.receiver.completed] == ["lock-1"]
    with pytest.raises(MessageRejectedError):
        queue.ack(msg)  # já finalizada


def test_nack_requeue_abandons(queue: ServiceBusQueue, client: FakeClient) -> None:
    client.receiver.inbox = [FakeMessage("b")]
    queue.nack(queue.receive()[0])
    assert len(client.receiver.abandoned) == 1 and not client.sender.scheduled


def test_nack_with_delay_schedules_copy_and_completes_original(
    queue: ServiceBusQueue, client: FakeClient
) -> None:
    client.receiver.inbox = [FakeMessage("b", application_properties={"k": "v"})]
    queue.nack(queue.receive()[0], delay=30)
    [(clone, when)] = client.sender.scheduled
    assert clone.body == "b" and clone.application_properties == {"k": "v"}
    assert when is not None
    assert len(client.receiver.completed) == 1 and not client.receiver.abandoned


def test_nack_dead_letter(queue: ServiceBusQueue, client: FakeClient) -> None:
    client.receiver.inbox = [FakeMessage("b")]
    queue.nack(queue.receive()[0], requeue=False)
    assert [reason for _, reason in client.receiver.dead] == ["rejected"]


def test_sdk_errors_are_translated(queue: ServiceBusQueue, client: FakeClient) -> None:
    client.sender.error = RuntimeError("ServiceBusError")
    with pytest.raises(QueueUnavailableError) as info:
        queue.publish("x")
    assert isinstance(info.value.__cause__, RuntimeError)


def test_close_releases_resources(queue: ServiceBusQueue, client: FakeClient) -> None:
    with queue:
        pass
    assert client.sender.closed and client.receiver.closed
    assert not client.closed, "o cliente só é fechado se o adapter for dono dele"


def test_close_client_when_owned(client: FakeClient) -> None:
    ServiceBusQueue("jobs", client=client, message_factory=factory, close_client=True).close()
    assert client.closed
