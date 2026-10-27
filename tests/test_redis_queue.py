from __future__ import annotations

import fakeredis
import pytest

from helpers import FakeClock
from taskforge.adapters.queues.redis_queue import RedisQueue
from taskforge.exceptions import MessageRejectedError, QueueUnavailableError


@pytest.fixture
def client() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=True)


def make(client: fakeredis.FakeRedis, clock: FakeClock | None = None, **kw: object) -> RedisQueue:
    return RedisQueue("t", client=client, clock=clock or FakeClock(), **kw)  # type: ignore[arg-type]


def test_roundtrip_and_ack(client: fakeredis.FakeRedis) -> None:
    queue = make(client)
    message_id = queue.publish("corpo", attributes={"k": "v"})
    [msg] = queue.receive(wait_seconds=0)
    assert (msg.id, msg.body, msg.delivery_count, dict(msg.attributes)) == (
        message_id,
        "corpo",
        1,
        {"k": "v"},
    )
    assert client.llen("taskforge:t:processing") == 1
    queue.ack(msg)
    assert client.llen("taskforge:t:processing") == 0
    assert client.hlen("taskforge:t:deliveries") == 0


def test_fifo_order(client: fakeredis.FakeRedis) -> None:
    queue = make(client)
    for i in range(3):
        queue.publish(str(i))
    assert [m.body for m in queue.receive(max_messages=3, wait_seconds=0)] == ["0", "1", "2"]


def test_nack_requeues_with_delivery_count(client: fakeredis.FakeRedis) -> None:
    queue = make(client)
    queue.publish("a")
    [first] = queue.receive(wait_seconds=0)
    queue.nack(first)
    [second] = queue.receive(wait_seconds=0)
    assert second.id == first.id and second.delivery_count == 2


def test_dead_letter_after_max_deliveries(client: fakeredis.FakeRedis) -> None:
    queue = make(client, max_deliveries=2)
    queue.publish("a")
    for _ in range(2):
        [msg] = queue.receive(wait_seconds=0)
        queue.nack(msg)
    assert queue.depth == 0 and len(queue.dead_letters) == 1


def test_explicit_dead_letter(client: fakeredis.FakeRedis) -> None:
    queue = make(client)
    queue.publish("a")
    [msg] = queue.receive(wait_seconds=0)
    queue.nack(msg, requeue=False)
    assert len(queue.dead_letters) == 1 and queue.depth == 0


def test_delayed_requeue(client: fakeredis.FakeRedis) -> None:
    clock = FakeClock()
    queue = make(client, clock)
    queue.publish("a")
    [msg] = queue.receive(wait_seconds=0)
    queue.nack(msg, delay=10)
    assert queue.receive(wait_seconds=0) == []
    clock.advance(10)
    assert len(queue.receive(wait_seconds=0)) == 1


def test_expired_lease_is_reclaimed(client: fakeredis.FakeRedis) -> None:
    clock = FakeClock()
    queue = make(client, clock, visibility_timeout=30)
    queue.publish("a")
    [first] = queue.receive(wait_seconds=0)
    clock.advance(31)  # consumidor morreu
    [again] = queue.receive(wait_seconds=0)
    assert again.id == first.id and again.delivery_count == 2


def test_ack_with_unknown_receipt(client: fakeredis.FakeRedis) -> None:
    queue = make(client)
    queue.publish("a")
    [msg] = queue.receive(wait_seconds=0)
    queue.ack(msg)
    with pytest.raises(MessageRejectedError):
        queue.ack(msg)


def test_corrupt_envelope_goes_to_dead_letter(client: fakeredis.FakeRedis) -> None:
    queue = make(client)
    client.lpush("taskforge:t:ready", "isto não é json")
    queue.publish("valida")
    messages = queue.receive(max_messages=5, wait_seconds=0)
    assert [m.body for m in messages] == ["valida"]
    assert queue.dead_letters == ["isto não é json"]


def test_blocking_receive_returns_empty_on_timeout(client: fakeredis.FakeRedis) -> None:
    assert make(client).receive(wait_seconds=0.05) == []


def test_client_errors_become_queue_unavailable() -> None:
    class Broken:
        def lpush(self, *_: object) -> None:
            raise ConnectionError("redis fora")

    with pytest.raises(QueueUnavailableError) as info:
        RedisQueue("t", client=Broken()).publish("x")
    assert isinstance(info.value.__cause__, ConnectionError)
