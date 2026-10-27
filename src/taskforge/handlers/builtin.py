"""Handlers que acompanham o projeto.

Cobrem os três modos de execução:

* ``echo``          INLINE   trivial
* ``primes.count``  PROCESS  CPU-bound (crivo de Eratóstenes), contorna o GIL
* ``http.fetch``    THREAD   I/O-bound, com proteção contra SSRF
* ``util.sleep``    THREAD   espera (demonstra timeout)
* ``util.fail``     INLINE   falha controlada (demonstra retry e dead-letter)
"""

from __future__ import annotations

import hashlib
import ipaddress
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from taskforge.domain.models import ExecutionMode
from taskforge.exceptions import JobValidationError, PermanentError, TransientError
from taskforge.handlers.base import JobHandler
from taskforge.handlers.registry import register_handler


def _is_plain_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


@register_handler
class EchoHandler(JobHandler):
    name = "echo"
    mode = ExecutionMode.INLINE

    def execute(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return {"echo": dict(payload)}


@register_handler
class PrimeCountHandler(JobHandler):
    name = "primes.count"
    mode = ExecutionMode.PROCESS
    MAX_LIMIT = 20_000_000

    def validate(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        limit = payload.get("limit")
        if not _is_plain_int(limit) or not 2 <= limit <= self.MAX_LIMIT:  # type: ignore[operator]
            raise JobValidationError(f"'limit' deve ser inteiro entre 2 e {self.MAX_LIMIT}")
        return payload

    def execute(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        limit = int(payload["limit"])
        sieve = bytearray([1]) * (limit + 1)
        sieve[0] = sieve[1] = 0
        for i in range(2, int(limit**0.5) + 1):
            if sieve[i]:
                sieve[i * i :: i] = bytes(len(range(i * i, limit + 1, i)))
        return {"limit": limit, "count": sieve.count(1)}


Fetcher = Callable[[str, float, int], tuple[int, bytes]]
Resolver = Callable[[str], Iterable[str]]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Redirects poderiam levar a um host interno depois da checagem de SSRF."""

    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


def _default_fetcher(url: str, timeout: float, max_bytes: int) -> tuple[int, bytes]:
    request = urllib.request.Request(url, headers={"User-Agent": "taskforge/0.1"})
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(request, timeout=timeout) as response:
            return response.status, response.read(max_bytes + 1)
    except urllib.error.HTTPError as exc:  # 3xx/4xx/5xx: o status é o resultado
        return exc.code, b""


def _default_resolver(host: str) -> set[str]:
    return {str(info[4][0]) for info in socket.getaddrinfo(host, None)}


@register_handler
class HttpFetchHandler(JobHandler):
    """Baixa uma URL pública e devolve status, tamanho e SHA-256.

    Proteção contra SSRF: só http/https, sem redirects, e o host precisa resolver apenas
    para IPs globais (bloqueia loopback, redes privadas e o endpoint de metadados da
    nuvem 169.254.169.254). Limitação conhecida: a resolução DNS é refeita pelo urllib,
    então ainda há janela para DNS rebinding. Em produção, complemente com egress firewall.
    """

    name = "http.fetch"
    mode = ExecutionMode.THREAD
    MAX_BYTES = 5_000_000

    def __init__(self, fetcher: Fetcher = _default_fetcher, resolver: Resolver = _default_resolver):
        self._fetcher = fetcher
        self._resolver = resolver

    def validate(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        url = payload.get("url")
        if not isinstance(url, str):
            raise JobValidationError("'url' é obrigatória")
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise JobValidationError("apenas URLs http(s) com host são aceitas")
        timeout = payload.get("timeout", 10.0)
        if (
            not isinstance(timeout, int | float)
            or isinstance(timeout, bool)
            or not 0 < timeout <= 30
        ):
            raise JobValidationError("'timeout' deve estar entre 0 e 30 segundos")
        self._assert_public_host(parsed.hostname)
        return {"url": url, "timeout": float(timeout)}

    def _assert_public_host(self, host: str) -> None:
        try:
            addresses = list(self._resolver(host))
        except OSError as exc:
            raise TransientError(f"falha ao resolver {host!r}: {exc}") from exc
        if not addresses:
            raise JobValidationError(f"host sem endereço: {host!r}")
        for raw in addresses:
            if not ipaddress.ip_address(raw).is_global:
                raise JobValidationError(f"host {host!r} resolve para endereço não público")

    def execute(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        try:
            status, body = self._fetcher(payload["url"], payload["timeout"], self.MAX_BYTES)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            raise TransientError(f"falha de rede: {exc}") from exc
        if status == 429 or status >= 500:
            raise TransientError(f"servidor respondeu {status}")
        return {
            "url": payload["url"],
            "status": status,
            "bytes": min(len(body), self.MAX_BYTES),
            "truncated": len(body) > self.MAX_BYTES,
            "sha256": hashlib.sha256(body[: self.MAX_BYTES]).hexdigest(),
        }


@register_handler
class SleepHandler(JobHandler):
    name = "util.sleep"
    mode = ExecutionMode.THREAD

    def validate(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        seconds = payload.get("seconds", 0)
        if (
            not isinstance(seconds, int | float)
            or isinstance(seconds, bool)
            or not 0 <= seconds <= 60
        ):
            raise JobValidationError("'seconds' deve estar entre 0 e 60")
        return payload

    def execute(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        seconds = float(payload.get("seconds", 0))
        time.sleep(seconds)
        return {"slept": seconds}


@register_handler
class FailHandler(JobHandler):
    name = "util.fail"
    mode = ExecutionMode.INLINE

    def validate(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        if payload.get("kind") not in {"transient", "permanent"}:
            raise JobValidationError("'kind' deve ser 'transient' ou 'permanent'")
        return payload

    def execute(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        if payload["kind"] == "transient":
            raise TransientError("falha transitória simulada")
        raise PermanentError("falha permanente simulada")
