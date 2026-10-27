from __future__ import annotations

import hashlib
import urllib.error
from typing import Any

import pytest

from taskforge.domain.models import ExecutionMode
from taskforge.exceptions import (
    ConfigurationError,
    HandlerError,
    HandlerNotFoundError,
    JobValidationError,
    PermanentError,
    TaskForgeError,
    TransientError,
)
from taskforge.handlers.base import JobHandler
from taskforge.handlers.builtin import (
    EchoHandler,
    FailHandler,
    HttpFetchHandler,
    PrimeCountHandler,
    SleepHandler,
)
from taskforge.handlers.registry import HandlerRegistry
from taskforge.handlers.runner import import_plugins, run_handler

PUBLIC_IP = "93.184.216.34"


# --- primes.count --------------------------------------------------------------------


@pytest.mark.parametrize(("limit", "count"), [(2, 1), (10, 4), (100, 25), (10_000, 1229)])
def test_prime_count(limit: int, count: int) -> None:
    assert PrimeCountHandler().run({"limit": limit}) == {"limit": limit, "count": count}


@pytest.mark.parametrize("bad", [None, "10", 1, 2.5, True, 50_000_000, -3])
def test_prime_count_validation(bad: Any) -> None:
    with pytest.raises(JobValidationError):
        PrimeCountHandler().run({"limit": bad})


# --- http.fetch ----------------------------------------------------------------------


def http(
    status: int = 200,
    body: bytes = b"ola",
    ips: tuple[str, ...] = (PUBLIC_IP,),
    error: Exception | None = None,
) -> HttpFetchHandler:
    def fetcher(url: str, timeout: float, max_bytes: int) -> tuple[int, bytes]:
        if error:
            raise error
        return status, body

    return HttpFetchHandler(fetcher=fetcher, resolver=lambda host: ips)


def test_http_fetch_ok() -> None:
    result = http(body=b"conteudo").run({"url": "https://exemplo.com/x"})
    assert result == {
        "url": "https://exemplo.com/x",
        "status": 200,
        "bytes": 8,
        "truncated": False,
        "sha256": hashlib.sha256(b"conteudo").hexdigest(),
    }


def test_http_fetch_truncates_large_bodies() -> None:
    big = b"x" * (HttpFetchHandler.MAX_BYTES + 10)
    result = http(body=big).run({"url": "https://exemplo.com"})
    assert result["truncated"] is True and result["bytes"] == HttpFetchHandler.MAX_BYTES


@pytest.mark.parametrize(
    "ip", ["127.0.0.1", "10.0.0.5", "192.168.1.1", "169.254.169.254", "::1", "172.16.0.9"]
)
def test_http_fetch_blocks_ssrf_targets(ip: str) -> None:
    with pytest.raises(JobValidationError, match="não público"):
        http(ips=(ip,)).run({"url": "http://qualquer.com"})


def test_http_fetch_blocks_when_any_resolved_ip_is_private() -> None:
    with pytest.raises(JobValidationError):
        http(ips=(PUBLIC_IP, "10.0.0.1")).run({"url": "http://rebind.com"})


@pytest.mark.parametrize(
    "url", ["file:///etc/passwd", "ftp://x.com/a", "javascript:alert(1)", "http://"]
)
def test_http_fetch_rejects_bad_urls(url: str) -> None:
    with pytest.raises(JobValidationError):
        http().run({"url": url})


@pytest.mark.parametrize(
    "payload",
    [{}, {"url": 3}, {"url": "http://a.com", "timeout": 0}, {"url": "http://a.com", "timeout": 99}],
)
def test_http_fetch_rejects_bad_payload(payload: dict[str, Any]) -> None:
    with pytest.raises(JobValidationError):
        http().run(payload)


def test_http_fetch_5xx_and_429_are_transient() -> None:
    for status in (429, 500, 503):
        with pytest.raises(TransientError):
            http(status=status).run({"url": "http://a.com"})


def test_http_fetch_4xx_is_a_result_not_an_error() -> None:
    assert http(status=404).run({"url": "http://a.com"})["status"] == 404


def test_http_fetch_network_errors_are_transient() -> None:
    with pytest.raises(TransientError):
        http(error=urllib.error.URLError("dns")).run({"url": "http://a.com"})
    with pytest.raises(TransientError):
        http(error=TimeoutError()).run({"url": "http://a.com"})


def test_http_fetch_resolver_failure_is_transient() -> None:
    def resolver(host: str) -> list[str]:
        raise OSError("dns fora")

    with pytest.raises(TransientError):
        HttpFetchHandler(resolver=resolver).run({"url": "http://a.com"})


# --- outros builtins -------------------------------------------------------------------


def test_echo_and_sleep_and_fail() -> None:
    assert EchoHandler().run({"a": 1}) == {"echo": {"a": 1}}
    assert SleepHandler().run({"seconds": 0}) == {"slept": 0.0}
    with pytest.raises(JobValidationError):
        SleepHandler().run({"seconds": 999})
    with pytest.raises(TransientError):
        FailHandler().run({"kind": "transient"})
    with pytest.raises(PermanentError):
        FailHandler().run({"kind": "permanent"})
    with pytest.raises(JobValidationError):
        FailHandler().run({"kind": "outro"})


# --- registry ------------------------------------------------------------------------


def test_registry_basics() -> None:
    registry = HandlerRegistry()
    registry.register(EchoHandler)
    registry.register(EchoHandler)  # idempotente para a mesma classe
    assert "echo" in registry and len(registry) == 1
    assert registry.mode_of("echo") is ExecutionMode.INLINE
    assert registry.get("echo") is registry.get("echo"), "instância é reutilizada"


def test_registry_rejects_conflicts_and_bad_names() -> None:
    registry = HandlerRegistry()
    registry.register(EchoHandler)

    class Impostor(JobHandler):
        name = "echo"

        def execute(self, payload: Any) -> dict[str, Any]:
            return {}

    class BadName(JobHandler):
        name = "Nome Ruim"

        def execute(self, payload: Any) -> dict[str, Any]:
            return {}

    with pytest.raises(ConfigurationError, match="já registrado"):
        registry.register(Impostor)
    with pytest.raises(ConfigurationError, match="inválido"):
        registry.register(BadName)


def test_registry_unknown_handler_lists_known() -> None:
    registry = HandlerRegistry()
    registry.register(EchoHandler)
    with pytest.raises(HandlerNotFoundError) as info:
        registry.get("nada")
    assert info.value.context["known"] == ["echo"]
    with pytest.raises(HandlerNotFoundError):
        registry.mode_of("nada")


# --- runner --------------------------------------------------------------------------


def make_registry(execute: Any) -> HandlerRegistry:
    class Custom(JobHandler):
        name = "custom"

        def execute(self, payload: Any) -> dict[str, Any]:
            return execute(payload)  # type: ignore[no-any-return]

    registry = HandlerRegistry()
    registry.register(Custom)
    return registry


def test_runner_wraps_unknown_errors_as_permanent_with_cause() -> None:
    def boom(_: Any) -> dict[str, Any]:
        raise ValueError("bug")

    with pytest.raises(HandlerError, match="ValueError: bug") as info:
        run_handler(make_registry(boom), "custom", {})
    assert isinstance(info.value.__cause__, ValueError)


def test_runner_classifies_network_errors_as_transient() -> None:
    def down(_: Any) -> dict[str, Any]:
        raise ConnectionError("recusada")

    with pytest.raises(TransientError):
        run_handler(make_registry(down), "custom", {})


def test_runner_lets_domain_errors_through_untouched() -> None:
    def invalid(_: Any) -> dict[str, Any]:
        raise JobValidationError("payload ruim")

    with pytest.raises(JobValidationError):
        run_handler(make_registry(invalid), "custom", {})


@pytest.mark.parametrize("bad_result", ["texto", [1, 2], {"obj": object()}, {"s": {1, 2}}])
def test_runner_rejects_bad_results(bad_result: Any) -> None:
    with pytest.raises(HandlerError):
        run_handler(make_registry(lambda _: bad_result), "custom", {})


def test_plugins_import_errors_are_actionable() -> None:
    with pytest.raises(ConfigurationError, match="plugin"):
        import_plugins("modulo_que_nao_existe_xyz")
    import_plugins("")  # vazio é no-op
    import_plugins("json")  # módulo existente


def test_all_runner_errors_are_taskforge_errors() -> None:
    def boom(_: Any) -> dict[str, Any]:
        raise KeyError("k")

    with pytest.raises(TaskForgeError):
        run_handler(make_registry(boom), "custom", {})
