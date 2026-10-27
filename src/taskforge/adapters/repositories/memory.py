"""Repositório em memória (testes e demo)."""

from __future__ import annotations

import threading

from taskforge.domain.models import Job, JobStatus
from taskforge.exceptions import JobNotFoundError, RepositoryError
from taskforge.ports.repository import JobRepository


class InMemoryJobRepository(JobRepository):
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()

    def add(self, job: Job) -> None:
        with self._lock:
            if job.id in self._jobs:
                raise RepositoryError(f"job já existe: {job.id}")
            self._jobs[job.id] = job

    def find(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def save(self, job: Job) -> None:
        with self._lock:
            if job.id not in self._jobs:
                raise JobNotFoundError(f"job não encontrado: {job.id}")
            self._jobs[job.id] = job

    def list_jobs(self, status: JobStatus | None = None, limit: int = 100) -> list[Job]:
        with self._lock:
            jobs = [j for j in self._jobs.values() if status is None or j.status is status]
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return jobs[:limit]
