from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from taskforge.cli import main


@pytest.fixture(autouse=True)
def cli_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("TASKFORGE_DATABASE_PATH", str(tmp_path / "cli.db"))
    monkeypatch.setenv("TASKFORGE_HEARTBEAT_FILE", str(tmp_path / "hb"))
    monkeypatch.setenv("TASKFORGE_POLL_WAIT_SECONDS", "0")
    monkeypatch.setenv("TASKFORGE_LOG_JSON", "false")
    monkeypatch.setenv("TASKFORGE_QUEUE_BACKEND", "memory")


def test_handlers_lists_builtin(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["handlers"]) == 0
    out = capsys.readouterr().out
    assert "echo" in out and "primes.count" in out and "process" in out


def test_submit_then_status_and_jobs(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["submit", "echo", "--payload", '{"a": 1}']) == 0
    captured = capsys.readouterr()
    assert "memory" in captured.err, "deve avisar que a fila em memória não é compartilhada"
    job_id = json.loads(captured.out)["id"]

    assert main(["status", job_id]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "pending"

    assert main(["jobs", "--status", "pending"]) == 0
    assert job_id in capsys.readouterr().out


def test_status_of_unknown_job_is_a_clean_error(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["status", "inexistente"]) == 2
    assert "não encontrado" in capsys.readouterr().err


@pytest.mark.parametrize("payload", ["não é json", "[1, 2]"])
def test_submit_rejects_bad_payload(payload: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["submit", "echo", "--payload", payload]) == 2
    assert "payload" in capsys.readouterr().err


def test_submit_unknown_handler(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["submit", "nao.existe"]) == 2
    assert "desconhecido" in capsys.readouterr().err


def test_invalid_configuration_is_reported(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("TASKFORGE_QUEUE_BACKEND", "sqs")
    assert main(["handlers"]) == 2
    assert "TASKFORGE_SQS_QUEUE_URL" in capsys.readouterr().err


def test_worker_once_exits_cleanly() -> None:
    assert main(["worker", "--once"]) == 0


def test_health(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    heartbeat = tmp_path / "hb"
    assert main(["health"]) == 1, "sem heartbeat = não saudável"
    heartbeat.touch()
    assert main(["health"]) == 0
    old = time.time() - 3600
    os.utime(heartbeat, (old, old))
    assert main(["health", "--max-age", "60"]) == 1
    assert "velho" in capsys.readouterr().err


@pytest.mark.slow
def test_demo_end_to_end(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["demo"]) == 0
    out = capsys.readouterr().out
    assert "148933" in out, "contagem de primos até 2.000.000"
    assert "dead_lettered" in out and "failed" in out and "succeeded" in out
