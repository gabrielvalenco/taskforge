"""Repositório SQLite.

Uma conexão compartilhada protegida por lock, em modo WAL (vários processos podem ler
enquanto um escreve). Serve para um nó ou para o desenvolvimento; em produção multi-nó
troque por Postgres implementando a mesma porta ``JobRepository``.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from taskforge.domain.models import Job, JobStatus
from taskforge.exceptions import (
    JobNotFoundError,
    RepositoryError,
    RepositoryUnavailableError,
)
from taskforge.ports.repository import JobRepository

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id         TEXT PRIMARY KEY,
    status     TEXT NOT NULL,
    handler    TEXT NOT NULL,
    created_at TEXT NOT NULL,
    body       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs (status, created_at);
"""


class SQLiteJobRepository(JobRepository):
    def __init__(self, path: str | Path = ":memory:", *, timeout: float = 30.0) -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(path), timeout=timeout, check_same_thread=False)
        with self._session() as conn:
            if str(path) != ":memory:":
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
            conn.executescript(_SCHEMA)

    @contextmanager
    def _session(self) -> Iterator[sqlite3.Connection]:
        """Lock + transação (commit/rollback automático) + tradução de erros."""
        with self._lock:
            try:
                with self._conn:
                    yield self._conn
            except sqlite3.OperationalError as exc:  # database is locked, disk I/O...
                raise RepositoryUnavailableError(f"sqlite indisponível: {exc}") from exc
            except sqlite3.Error as exc:
                raise RepositoryError(f"erro de sqlite: {exc}") from exc

    def add(self, job: Job) -> None:
        try:
            with self._session() as conn:
                conn.execute(
                    "INSERT INTO jobs (id, status, handler, created_at, body) VALUES (?,?,?,?,?)",
                    (
                        job.id,
                        job.status.value,
                        job.handler,
                        job.created_at.isoformat(),
                        job.model_dump_json(),
                    ),
                )
        except RepositoryError as exc:
            if isinstance(exc.__cause__, sqlite3.IntegrityError):
                raise RepositoryError(f"job já existe: {job.id}") from exc.__cause__
            raise

    def find(self, job_id: str) -> Job | None:
        with self._session() as conn:
            row = conn.execute("SELECT body FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return None if row is None else Job.model_validate_json(row[0])

    def save(self, job: Job) -> None:
        with self._session() as conn:
            cursor = conn.execute(
                "UPDATE jobs SET status = ?, body = ? WHERE id = ?",
                (job.status.value, job.model_dump_json(), job.id),
            )
            if cursor.rowcount == 0:
                raise JobNotFoundError(f"job não encontrado: {job.id}")

    def list_jobs(self, status: JobStatus | None = None, limit: int = 100) -> list[Job]:
        query = "SELECT body FROM jobs"
        params: tuple[object, ...] = ()
        if status is not None:
            query += " WHERE status = ?"
            params = (status.value,)
        query += " ORDER BY created_at DESC LIMIT ?"
        with self._session() as conn:
            rows = conn.execute(query, (*params, limit)).fetchall()
        return [Job.model_validate_json(r[0]) for r in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
