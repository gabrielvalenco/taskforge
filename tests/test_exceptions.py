from __future__ import annotations

import pickle

import pytest

from taskforge import exceptions as exc_module
from taskforge.exceptions import (
    CircuitOpenError,
    PermanentError,
    QueueError,
    QueueUnavailableError,
    RetryExhaustedError,
    TaskForgeError,
    TransientError,
    is_retryable,
)


def _all_subclasses(cls: type[BaseException]) -> set[type[BaseException]]:
    found: set[type[BaseException]] = set()
    for sub in cls.__subclasses__():
        found.add(sub)
        found |= _all_subclasses(sub)
    return found


@pytest.mark.parametrize("cls", sorted(_all_subclasses(TaskForgeError), key=lambda c: c.__name__))
def test_every_exception_survives_pickle(cls: type[TaskForgeError]) -> None:
    """Requisito para atravessar ProcessPoolExecutor e o backend de resultados do Celery."""
    original = cls("mensagem", context={"k": 1})
    clone = pickle.loads(pickle.dumps(original))
    assert type(clone) is cls
    assert str(clone) == "mensagem"
    assert clone.retryable == original.retryable
    assert clone.context == {"k": 1}


def test_retry_exhausted_keeps_attempts_after_pickle() -> None:
    clone = pickle.loads(pickle.dumps(RetryExhaustedError("x", attempts=4)))
    assert clone.attempts == 4


def test_module_exports_cover_all_classes() -> None:
    declared = {
        n
        for n, v in vars(exc_module).items()
        if isinstance(v, type) and issubclass(v, TaskForgeError)
    }
    assert declared == {c.__name__ for c in _all_subclasses(TaskForgeError)} | {"TaskForgeError"}


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (TransientError("x"), True),
        (PermanentError("x"), False),
        (QueueUnavailableError("x"), True),
        (QueueError("x"), False),
        (CircuitOpenError("x"), True),
        (ConnectionResetError(), True),
        (TimeoutError(), True),
        (ValueError("bug"), False),
        (KeyError("k"), False),
    ],
)
def test_is_retryable(exc: BaseException, expected: bool) -> None:
    assert is_retryable(exc) is expected
