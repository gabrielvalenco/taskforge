from __future__ import annotations

import logging

import pytest

from taskforge.domain.events import (
    Event,
    JobEvent,
    JobFailed,
    JobStarted,
    JobSubmitted,
    JobSucceeded,
)
from taskforge.patterns.event_bus import EventBus, MetricsCollector


def test_subscribers_receive_matching_events_only() -> None:
    bus = EventBus()
    started: list[JobStarted] = []
    bus.subscribe(JobStarted, started.append)
    bus.publish(JobStarted("j1", 1))
    bus.publish(JobFailed("j1", "x"))
    assert [e.job_id for e in started] == ["j1"]


def test_subscribing_to_base_class_receives_subclasses() -> None:
    bus = EventBus()
    everything: list[Event] = []
    jobs: list[Event] = []
    bus.subscribe(Event, everything.append)
    bus.subscribe(JobEvent, jobs.append)
    bus.publish(JobSubmitted("a", "echo"))
    bus.publish(JobFailed("b", "x"))
    assert len(everything) == len(jobs) == 2


def test_unsubscribe() -> None:
    bus = EventBus()
    seen: list[Event] = []
    unsubscribe = bus.subscribe(Event, seen.append)
    bus.publish(JobStarted("a", 1))
    unsubscribe()
    unsubscribe()  # idempotente
    bus.publish(JobStarted("b", 1))
    assert len(seen) == 1


def test_failing_subscriber_is_isolated(caplog: pytest.LogCaptureFixture) -> None:
    bus = EventBus()
    seen: list[Event] = []

    def broken(_: Event) -> None:
        raise RuntimeError("bug no subscriber")

    bus.subscribe(Event, broken)
    bus.subscribe(Event, seen.append)
    with caplog.at_level(logging.ERROR):
        bus.publish(JobStarted("a", 1))
    assert len(seen) == 1, "o segundo subscriber precisa receber mesmo com o primeiro quebrando"
    assert "bug no subscriber" in caplog.text


def test_metrics_collector() -> None:
    bus = EventBus()
    metrics = MetricsCollector()
    metrics.attach(bus)
    bus.publish(JobSubmitted("a", "echo"))
    bus.publish(JobSucceeded("a", 1.5))
    bus.publish(JobSucceeded("b", 0.5))
    snapshot = metrics.snapshot()
    assert snapshot["JobSubmitted"] == 1
    assert snapshot["JobSucceeded"] == 2
    assert snapshot["total_success_duration_s"] == 2.0
