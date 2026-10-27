"""Ponto de entrada único para executar um handler, em qualquer backend.

``execute_handler`` é uma função de módulo (picklável): é ela que atravessa a fronteira
de processo no ``ProcessPoolExecutor`` e é chamada pelas tasks do Celery. Ao ser importado
no processo filho, este módulo também importa ``builtin``, o que registra os handlers.
Handlers próprios para modo PROCESS devem ser carregados via ``TASKFORGE_PLUGINS``
(lista de módulos separados por vírgula), que cada processo importa antes de executar.
"""

from __future__ import annotations

import importlib
import json
import os
from collections.abc import Mapping
from functools import lru_cache
from typing import Any

from taskforge.exceptions import (
    ConfigurationError,
    HandlerError,
    TaskForgeError,
    TransientError,
    is_retryable,
)
from taskforge.handlers import builtin as _builtin  # noqa: F401  (efeito colateral: registro)
from taskforge.handlers.registry import HandlerRegistry, default_registry

PLUGINS_ENV = "TASKFORGE_PLUGINS"


@lru_cache(maxsize=8)
def import_plugins(spec: str) -> None:
    for module in (m.strip() for m in spec.split(",")):
        if not module:
            continue
        try:
            importlib.import_module(module)
        except ImportError as exc:
            raise ConfigurationError(f"plugin {module!r} não pôde ser importado: {exc}") from exc


def run_handler(registry: HandlerRegistry, name: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Executa e normaliza erros: tudo que sai daqui é ``TaskForgeError`` e é picklável."""
    handler = registry.get(name)
    try:
        result = handler.run(payload)
    except TaskForgeError:
        raise
    except Exception as exc:
        text = f"{type(exc).__name__}: {exc}"
        if is_retryable(exc):
            raise TransientError(text) from exc
        raise HandlerError(text) from exc
    if not isinstance(result, dict):
        raise HandlerError(f"handler {name!r} deve devolver dict, devolveu {type(result).__name__}")
    try:
        json.dumps(result)
    except (TypeError, ValueError) as exc:
        raise HandlerError(f"resultado de {name!r} não é serializável em JSON: {exc}") from exc
    return result


def execute_handler(name: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    import_plugins(os.environ.get(PLUGINS_ENV, ""))
    return run_handler(default_registry, name, dict(payload))
