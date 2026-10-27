"""Porta de persistência (padrão Repository)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from types import TracebackType
from typing import Self

from taskforge.domain.models import Job, JobStatus
from taskforge.exceptions import JobNotFoundError


class JobRepository(ABC):
    @abstractmethod
    def add(self, job: Job) -> None:
        """Insere um job novo. Levanta ``RepositoryError`` se o id já existir."""

    @abstractmethod
    def find(self, job_id: str) -> Job | None: ...

    @abstractmethod
    def save(self, job: Job) -> None:
        """Atualiza um job existente. Levanta ``JobNotFoundError`` se não existir."""

    @abstractmethod
    def list_jobs(self, status: JobStatus | None = None, limit: int = 100) -> list[Job]: ...

    def get(self, job_id: str) -> Job:
        """Template method: ``find`` + erro de domínio em vez de ``None``."""
        job = self.find(job_id)
        if job is None:
            raise JobNotFoundError(f"job não encontrado: {job_id}")
        return job

    def close(self) -> None:  # noqa: B027
        """Libera recursos (conexões)."""

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
