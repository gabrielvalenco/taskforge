"""Registro de handlers (Registry). ``register_handler`` serve como decorator de classe."""

from __future__ import annotations

import re
import threading

from taskforge.domain.models import HANDLER_NAME_PATTERN, ExecutionMode
from taskforge.exceptions import ConfigurationError, HandlerNotFoundError
from taskforge.handlers.base import JobHandler

_NAME_RE = re.compile(HANDLER_NAME_PATTERN)


class HandlerRegistry:
    def __init__(self) -> None:
        self._classes: dict[str, type[JobHandler]] = {}
        self._instances: dict[str, JobHandler] = {}
        self._lock = threading.Lock()

    def register(self, handler_cls: type[JobHandler]) -> type[JobHandler]:
        name = getattr(handler_cls, "name", None)
        if not isinstance(name, str) or not _NAME_RE.match(name):
            raise ConfigurationError(
                f"{handler_cls.__name__}.name inválido: {name!r} (use {HANDLER_NAME_PATTERN})"
            )
        with self._lock:
            existing = self._classes.get(name)
            if existing is not None and existing is not handler_cls:
                raise ConfigurationError(f"handler já registrado: {name!r}")
            self._classes[name] = handler_cls
        return handler_cls

    def get(self, name: str) -> JobHandler:
        with self._lock:
            instance = self._instances.get(name)
            if instance is not None:
                return instance
            handler_cls = self._classes.get(name)
            if handler_cls is None:
                raise HandlerNotFoundError(
                    f"handler desconhecido: {name!r}", context={"known": sorted(self._classes)}
                )
            instance = self._instances[name] = handler_cls()
            return instance

    def mode_of(self, name: str) -> ExecutionMode:
        with self._lock:
            handler_cls = self._classes.get(name)
        if handler_cls is None:
            raise HandlerNotFoundError(f"handler desconhecido: {name!r}")
        return handler_cls.mode

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._classes)

    def __contains__(self, name: object) -> bool:
        with self._lock:
            return name in self._classes

    def __len__(self) -> int:
        with self._lock:
            return len(self._classes)


default_registry = HandlerRegistry()


def register_handler(handler_cls: type[JobHandler]) -> type[JobHandler]:
    """Decorator: registra a classe no registry global."""
    return default_registry.register(handler_cls)
