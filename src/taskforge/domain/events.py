"""Eventos de domínio publicados no EventBus (padrão Observer)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from taskforge.domain.models import utcnow


@dataclass(frozen=True, slots=True, kw_only=True)
class Event:
    occurred_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True, slots=True)
class JobEvent(Event):
    job_id: str


@dataclass(frozen=True, slots=True)
class JobSubmitted(JobEvent):
    handler: str


@dataclass(frozen=True, slots=True)
class JobStarted(JobEvent):
    attempt: int


@dataclass(frozen=True, slots=True)
class JobSucceeded(JobEvent):
    duration_s: float


@dataclass(frozen=True, slots=True)
class JobRetrying(JobEvent):
    error: str
    attempt: int
    delay_s: float


@dataclass(frozen=True, slots=True)
class JobFailed(JobEvent):
    error: str


@dataclass(frozen=True, slots=True)
class JobDeadLettered(JobEvent):
    error: str
