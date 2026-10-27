from __future__ import annotations

import os
import threading
import time
from concurrent.futures.process import BrokenProcessPool

import pytest

import helpers
from taskforge.concurrency.pools import PoolManager
from taskforge.domain.models import ExecutionMode


def test_inline_runs_synchronously_and_captures_errors() -> None:
    with PoolManager() as pools:
        assert pools.submit(ExecutionMode.INLINE, helpers.add, 1, 2).result() == 3
        future = pools.submit(ExecutionMode.INLINE, helpers.add, 1, "x")
        assert future.done()
        with pytest.raises(TypeError):
            future.result()


def test_threads_run_off_the_calling_thread() -> None:
    with PoolManager(io_threads=2) as pools:
        name = pools.submit(ExecutionMode.THREAD, lambda: threading.current_thread().name).result()
    assert name.startswith("taskforge-io")


def test_threads_run_concurrently() -> None:
    barrier = threading.Barrier(4, timeout=5)
    with PoolManager(io_threads=4) as pools:
        futures = [pools.submit(ExecutionMode.THREAD, barrier.wait) for _ in range(4)]
        assert sorted(f.result() for f in futures) == [0, 1, 2, 3]


def test_closed_manager_rejects_work() -> None:
    pools = PoolManager()
    pools.close()
    with pytest.raises(RuntimeError, match="encerrado"):
        pools.submit(ExecutionMode.THREAD, helpers.add, 1, 2)


@pytest.mark.slow
def test_processes_run_in_a_different_process() -> None:
    with PoolManager(cpu_processes=1) as pools:
        pid = pools.submit(ExecutionMode.PROCESS, helpers.get_pid).result(timeout=30)
    assert pid != os.getpid()


@pytest.mark.slow
def test_process_pool_recovers_after_a_crash() -> None:
    with PoolManager(cpu_processes=1) as pools:
        crashing = pools.submit(ExecutionMode.PROCESS, helpers.crash_process)
        with pytest.raises(BrokenProcessPool):
            crashing.result(timeout=30)
        time.sleep(0.2)  # o callback que descarta o pool quebrado roda em outra thread
        assert pools.submit(ExecutionMode.PROCESS, helpers.add, 20, 22).result(timeout=30) == 42
