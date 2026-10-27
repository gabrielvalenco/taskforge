from __future__ import annotations

from typing import Any

import pytest

from taskforge.adapters.queues.sqs import SQSQueue
from taskforge.exceptions import QueueUnavailableError

URL = "https://sqs.us-east-1.amazonaws.com/123456789012/jobs"
DLQ = "https://sqs.us-east-1.amazonaws.com/123456789012/jobs-dlq"


class FakeSQS:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.deleted: list[dict[str, Any]] = []
        self.visibility: list[dict[str, Any]] = []
        self.inbox: list[dict[str, Any]] = []
        self.receive_kwargs: dict[str, Any] = {}
        self.error: Exception | None = None

    def _maybe_fail(self) -> None:
        if self.error:
            raise self.error

    def send_message(self, **kw: Any) -> dict[str, str]:
        self._maybe_fail()
        self.sent.append(kw)
        return {"MessageId": f"m{len(self.sent)}"}

    def receive_message(self, **kw: Any) -> dict[str, Any]:
        self._maybe_fail()
        self.receive_kwargs = kw
        messages, self.inbox = self.inbox, []
        return {"Messages": messages} if messages else {}

    def delete_message(self, **kw: Any) -> None:
        self._maybe_fail()
        self.deleted.append(kw)

    def change_message_visibility(self, **kw: Any) -> None:
        self._maybe_fail()
        self.visibility.append(kw)


@pytest.fixture
def sqs() -> FakeSQS:
    return FakeSQS()


def raw(body: str = "b", count: str = "1") -> dict[str, Any]:
    return {
        "MessageId": "id-1",
        "ReceiptHandle": "rh-1",
        "Body": body,
        "Attributes": {"ApproximateReceiveCount": count},
        "MessageAttributes": {"handler": {"DataType": "String", "StringValue": "echo"}},
    }


def test_publish_encodes_attributes(sqs: FakeSQS) -> None:
    queue = SQSQueue(URL, client=sqs)
    assert queue.name == "jobs"
    assert queue.publish("corpo", attributes={"handler": "echo"}) == "m1"
    assert sqs.sent == [
        {
            "QueueUrl": URL,
            "MessageBody": "corpo",
            "MessageAttributes": {"handler": {"DataType": "String", "StringValue": "echo"}},
        }
    ]


def test_receive_maps_fields_and_clamps_limits(sqs: FakeSQS) -> None:
    sqs.inbox = [raw("payload", count="3")]
    queue = SQSQueue(URL, client=sqs, visibility_timeout=90)
    [msg] = queue.receive(max_messages=50, wait_seconds=99)
    assert (msg.id, msg.body, msg.receipt, msg.delivery_count) == ("id-1", "payload", "rh-1", 3)
    assert dict(msg.attributes) == {"handler": "echo"}
    assert sqs.receive_kwargs["MaxNumberOfMessages"] == 10
    assert sqs.receive_kwargs["WaitTimeSeconds"] == 20
    assert sqs.receive_kwargs["VisibilityTimeout"] == 90


def test_receive_empty(sqs: FakeSQS) -> None:
    assert SQSQueue(URL, client=sqs).receive() == []


def test_ack_deletes(sqs: FakeSQS) -> None:
    sqs.inbox = [raw()]
    queue = SQSQueue(URL, client=sqs)
    queue.ack(queue.receive()[0])
    assert sqs.deleted == [{"QueueUrl": URL, "ReceiptHandle": "rh-1"}]


def test_nack_requeue_uses_delay_as_visibility(sqs: FakeSQS) -> None:
    sqs.inbox = [raw()]
    queue = SQSQueue(URL, client=sqs)
    queue.nack(queue.receive()[0], delay=12.7)
    assert sqs.visibility == [{"QueueUrl": URL, "ReceiptHandle": "rh-1", "VisibilityTimeout": 12}]


def test_nack_dead_letter_with_dlq_url(sqs: FakeSQS) -> None:
    sqs.inbox = [raw("morta")]
    queue = SQSQueue(URL, client=sqs, dead_letter_url=DLQ)
    queue.nack(queue.receive()[0], requeue=False)
    assert sqs.sent[0]["QueueUrl"] == DLQ and sqs.sent[0]["MessageBody"] == "morta"
    assert sqs.deleted == [{"QueueUrl": URL, "ReceiptHandle": "rh-1"}]


def test_nack_dead_letter_without_dlq_url_relies_on_redrive(sqs: FakeSQS) -> None:
    sqs.inbox = [raw()]
    queue = SQSQueue(URL, client=sqs)
    queue.nack(queue.receive()[0], requeue=False)
    assert sqs.deleted == []
    assert sqs.visibility[0]["VisibilityTimeout"] == 0


@pytest.mark.parametrize("operation", ["publish", "receive", "ack", "nack"])
def test_sdk_errors_are_translated(sqs: FakeSQS, operation: str) -> None:
    sqs.inbox = [raw()]
    queue = SQSQueue(URL, client=sqs)
    message = queue.receive()[0]
    sqs.error = RuntimeError("AccessDenied")
    actions = {
        "publish": lambda: queue.publish("x"),
        "receive": lambda: queue.receive(),
        "ack": lambda: queue.ack(message),
        "nack": lambda: queue.nack(message),
    }
    with pytest.raises(QueueUnavailableError) as info:
        actions[operation]()
    assert isinstance(info.value.__cause__, RuntimeError)
    assert "AccessDenied" in str(info.value)
