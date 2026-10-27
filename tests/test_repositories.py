from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from taskforge.adapters.repositories.memory import InMemoryJobRepository
from taskforge.adapters.repositories.sqlite import SQLiteJobRepository
from taskforge.domain.models import Job, JobStatus
from taskforge.exceptions import JobNotFoundError, RepositoryError
from taskforge.ports.repository import JobRepository

Factory = Callable[[Path], JobRepository]

FACTORIES: dict[str, Factory] = {
    "memory": lambda tmp: InMemoryJobRepository(),
    "sqlite-memory": lambda tmp: SQLiteJobRepository(":memory:"),
    "sqlite-file": lambda tmp: SQLiteJobRepository(tmp / "jobs.db"),
}


@pytest.fixture(params=list(FACTORIES))
def repo(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[JobRepository]:
    repository = FACTORIES[request.param](tmp_path)
    yield repository
    repository.close()


def test_add_get_roundtrip(repo: JobRepository) -> None:
    job = Job(handler="echo", payload={"a": [1, 2]})
    repo.add(job)
    assert repo.get(job.id) == job
    assert repo.find("inexistente") is None


def test_duplicate_add_is_rejected(repo: JobRepository) -> None:
    job = Job(handler="echo")
    repo.add(job)
    with pytest.raises(RepositoryError, match="já existe"):
        repo.add(job)


def test_get_missing_raises_domain_error(repo: JobRepository) -> None:
    with pytest.raises(JobNotFoundError):
        repo.get("nada")


def test_save_updates_and_requires_existing(repo: JobRepository) -> None:
    job = Job(handler="echo")
    repo.add(job)
    done = job.start().succeed({"ok": 1})
    repo.save(done)
    assert repo.get(job.id).status is JobStatus.SUCCEEDED
    with pytest.raises(JobNotFoundError):
        repo.save(Job(handler="echo"))


def test_list_filters_orders_and_limits(repo: JobRepository) -> None:
    jobs = [Job(handler="echo") for _ in range(5)]
    for job in jobs:
        repo.add(job)
    repo.save(jobs[0].start().succeed({}))
    assert len(repo.list_jobs()) == 5
    assert [j.id for j in repo.list_jobs(JobStatus.SUCCEEDED)] == [jobs[0].id]
    assert len(repo.list_jobs(JobStatus.PENDING)) == 4
    assert len(repo.list_jobs(limit=2)) == 2
    newest_first = [j.created_at for j in repo.list_jobs()]
    assert newest_first == sorted(newest_first, reverse=True)


def test_concurrent_writers(repo: JobRepository) -> None:
    errors: list[BaseException] = []

    def work() -> None:
        try:
            for _ in range(20):
                job = Job(handler="echo")
                repo.add(job)
                repo.save(job.start())
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=work) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert len(repo.list_jobs(limit=1000)) == 160


def test_sqlite_persists_across_connections(tmp_path: Path) -> None:
    path = tmp_path / "persist.db"
    job = Job(handler="echo")
    with SQLiteJobRepository(path) as first:
        first.add(job)
    with SQLiteJobRepository(path) as second:
        assert second.get(job.id) == job
