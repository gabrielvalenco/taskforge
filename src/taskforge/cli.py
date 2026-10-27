"""CLI: ``taskforge submit | worker | status | jobs | handlers | demo | health``."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path

from taskforge import __version__
from taskforge.bootstrap import Application
from taskforge.config import Settings
from taskforge.domain.models import Job, JobStatus
from taskforge.exceptions import ConfigurationError, TaskForgeError
from taskforge.logging_setup import configure_logging
from taskforge.resources import ShutdownSignal


def _settings() -> Settings:
    settings = Settings.from_env()
    configure_logging(settings.log_level, json_logs=settings.log_json)
    return settings


def _dump(job: Job) -> str:
    return job.model_dump_json()


def _cmd_submit(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.payload)
    except json.JSONDecodeError as exc:
        raise ConfigurationError(f"--payload não é JSON válido: {exc}") from exc
    if not isinstance(payload, dict):
        raise ConfigurationError("--payload deve ser um objeto JSON")
    settings = _settings()
    if settings.queue_backend == "memory":
        print(
            "aviso: o backend 'memory' vive apenas neste processo; use redis/sqs/servicebus "
            "para que um worker separado receba o job",
            file=sys.stderr,
        )
    with Application.build(settings) as app:
        job = app.dispatcher.submit(args.handler, payload, max_attempts=args.max_attempts)
    print(_dump(job))
    return 0


def _cmd_worker(args: argparse.Namespace) -> int:
    settings = _settings()
    with Application.build(settings) as app, ShutdownSignal() as shutdown:
        if args.once:
            app.worker.run_once()
        else:
            app.worker.run_forever(shutdown.event)
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    with Application.build(_settings()) as app:
        print(_dump(app.repository.get(args.job_id)))
    return 0


def _cmd_jobs(args: argparse.Namespace) -> int:
    status = JobStatus(args.status) if args.status else None
    with Application.build(_settings()) as app:
        for job in app.repository.list_jobs(status=status, limit=args.limit):
            print(_dump(job))
    return 0


def _cmd_handlers(_: argparse.Namespace) -> int:
    with Application.build(_settings()) as app:
        for name in app.registry.names():
            print(f"{name:<16} {app.registry.mode_of(name).value}")
    return 0


def _cmd_health(args: argparse.Namespace) -> int:
    path = Path(Settings.from_env().heartbeat_file)
    try:
        age = time.time() - path.stat().st_mtime
    except OSError:
        print("sem heartbeat", file=sys.stderr)
        return 1
    if age > args.max_age:
        print(f"heartbeat velho: {age:.0f}s", file=sys.stderr)
        return 1
    return 0


def _cmd_demo(_: argparse.Namespace) -> int:
    """Roda produtor e worker no mesmo processo, com fila em memória e SQLite em memória."""
    settings = Settings(
        queue_backend="memory",
        database_path=":memory:",
        execution_backend="local",
        io_threads=4,
        cpu_processes=2,
        poll_wait_seconds=0.2,
        job_timeout_seconds=60,
        retry_base_delay_seconds=0.3,
        retry_max_delay_seconds=1.0,
        log_json=False,
        log_level="WARNING",
        heartbeat_file="/tmp/taskforge-demo.heartbeat",  # noqa: S108
    )
    configure_logging(settings.log_level, json_logs=False)
    with Application.build(settings) as app:
        jobs = [
            app.dispatcher.submit("echo", {"msg": "olá"}),
            app.dispatcher.submit("primes.count", {"limit": 2_000_000}),
            app.dispatcher.submit("primes.count", {"limit": 3_000_000}),
            app.dispatcher.submit("util.sleep", {"seconds": 0.3}),
            app.dispatcher.submit("util.fail", {"kind": "permanent"}),
            app.dispatcher.submit("util.fail", {"kind": "transient"}, max_attempts=2),
        ]
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if all(app.repository.get(j.id).is_terminal for j in jobs):
                break
            app.worker.run_once()
        print(f"{'handler':<14} {'status':<14} {'tentativas':<11} resultado/erro")
        for submitted in jobs:
            job = app.repository.get(submitted.id)
            detail = job.result if job.result is not None else job.error
            print(f"{job.handler:<14} {job.status.value:<14} {job.attempts:<11} {detail}")
        print("métricas:", json.dumps(app.metrics.snapshot(), ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="taskforge", description=__doc__)
    parser.add_argument("--version", action="version", version=f"taskforge {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    submit = sub.add_parser("submit", help="publica um job")
    submit.add_argument("handler")
    submit.add_argument("--payload", default="{}", help="objeto JSON")
    submit.add_argument("--max-attempts", type=int, default=3)
    submit.set_defaults(func=_cmd_submit)

    worker = sub.add_parser("worker", help="consome a fila")
    worker.add_argument("--once", action="store_true", help="processa um lote e sai")
    worker.set_defaults(func=_cmd_worker)

    status = sub.add_parser("status", help="mostra um job")
    status.add_argument("job_id")
    status.set_defaults(func=_cmd_status)

    jobs = sub.add_parser("jobs", help="lista jobs")
    jobs.add_argument("--status", choices=[s.value for s in JobStatus])
    jobs.add_argument("--limit", type=int, default=20)
    jobs.set_defaults(func=_cmd_jobs)

    sub.add_parser("handlers", help="lista handlers").set_defaults(func=_cmd_handlers)

    health = sub.add_parser("health", help="healthcheck do worker (lê o heartbeat)")
    health.add_argument("--max-age", type=float, default=60.0)
    health.set_defaults(func=_cmd_health)

    sub.add_parser("demo", help="demonstração ponta a ponta em um processo").set_defaults(
        func=_cmd_demo
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    func: Callable[[argparse.Namespace], int] = args.func
    try:
        return func(args)
    except TaskForgeError as exc:
        print(f"erro: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
