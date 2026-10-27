from __future__ import annotations

import threading
import time

import pytest

from helpers import FakeClock
from taskforge.adapters.queues.memory import InMemoryQueue
from taskforge.exceptions import MessageRejectedError, QueueUnavailableError


def make(clock: FakeClock | None = None, **kw: object) -> InMemoryQueue:
    return InMemoryQueue("t", clock=clock or time.monotonic, **kw)  # type: ignore[arg-type]


def test_publish_receive_ack() -> None:
    queue = make()
    queue.publish("a", attributes={"k": "v"})
    [msg] = queue.receive(wait_seconds=0)
    assert (msg.body, msg.delivery_count, dict(msg.attributes)) == ("a", 1, {"k": "v"})
    assert queue.in_flight == 1
    queue.ack(msg)
    assert queue.in_flight == 0 and queue.depth == 0


def test_fifo_and_batch_limit() -> None:
    queue = make()
    for i in range(5):
        queue.publish(str(i))
    batch = queue.receive(max_messages=3, wait_seconds=0)
    assert [m.body for m in batch] == ["0", "1", "2"]
    assert queue.depth == 2


def test_nack_requeue_increments_delivery_count() -> None:
    queue = make()
    queue.publish("a")
    [first] = queue.receive(wait_seconds=0)
    queue.nack(first)
    [second] = queue.receive(wait_seconds=0)
    assert second.delivery_count == 2 and second.id == first.id


def test_dead_letter_after_max_deliveries() -> None:
    queue = make(max_deliveries=2)
    queue.publish("a")
    for _ in range(2):
        [msg] = queue.receive(wait_seconds=0)
        queue.nack(msg)
    assert queue.depth == 0
    assert [m.body for m in queue.dead_letters] == ["a"]


def test_explicit_dead_letter() -> None:
    queue = make()
    queue.publish("a")
    [msg] = queue.receive(wait_seconds=0)
    queue.nack(msg, requeue=False)
    assert len(queue.dead_letters) == 1 and queue.depth == 0


def test_visibility_timeout_redelivers() -> None:
    clock = FakeClock()
    queue = make(clock, visibility_timeout=30)
    queue.publish("a")
    [first] = queue.receive(wait_seconds=0)
    clock.advance(31)  # consumidor sumiu sem ack
    [again] = queue.receive(wait_seconds=0)
    assert again.id == first.id and again.delivery_count == 2
    with pytest.raises(MessageRejectedError):
        queue.ack(first)  # recibo antigo não vale mais


def test_nack_delay_hides_message_until_due() -> None:
    clock = FakeClock()
    queue = make(clock)
    queue.publish("a")
    [msg] = queue.receive(wait_seconds=0)
    queue.nack(msg, delay=5)
    assert queue.receive(wait_seconds=0) == []
    clock.advance(5)
    assert len(queue.receive(wait_seconds=0)) == 1


def test_unknown_receipt() -> None:
    queue = make()
    queue.publish("a")
    [msg] = queue.receive(wait_seconds=0)
    queue.ack(msg)
    with pytest.raises(MessageRejectedError):
        queue.ack(msg)
    with pytest.raises(MessageRejectedError):
        queue.nack(msg)


def test_receive_blocks_until_publish() -> None:
    queue = make()
    threading.Timer(0.1, lambda: queue.publish("tarde")).start()
    started = time.monotonic()
    [msg] = queue.receive(wait_seconds=2)
    assert msg.body == "tarde"
    assert time.monotonic() - started < 1.5


def test_receive_times_out_empty() -> None:
    started = time.monotonic()
    assert make().receive(wait_seconds=0.1) == []
    assert time.monotonic() - started >= 0.09


def test_close() -> None:
    queue = make()
    with queue:
        queue.publish("a")
    assert queue.receive(wait_seconds=0) == []
    with pytest.raises(QueueUnavailableError):
        queue.publish("b")


def test_concurrent_consumers_never_share_a_message() -> None:
    queue = make()
    total = 200
    for i in range(total):
        queue.publish(str(i))
    seen: list[str] = []
    lock = threading.Lock()

    def consume() -> None:
        while True:
            batch = queue.receive(max_messages=5, wait_seconds=0)
            if not batch:
                return
            with lock:
                seen.extend(m.body for m in batch)
            for m in batch:
                queue.ack(m)

    threads = [threading.Thread(target=consume) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(seen, key=int) == [str(i) for i in range(total)]
