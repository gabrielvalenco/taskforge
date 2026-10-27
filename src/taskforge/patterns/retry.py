"""Retry com backoff exponencial e jitter.

Funciona como função (``call_with_retry``) e como decorator (``retry``). ``sleep`` e ``rng``
são injetáveis para que os testes rodem instantaneamente e de forma determinística.
"""

from __future__ import annotations

import functools
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass

from taskforge.exceptions import RetryExhaustedError, is_retryable

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay: float = 0.2
    max_delay: float = 10.0
    multiplier: float = 2.0
    jitter: float = 0.2  # fração do atraso: 0.2 = +-20%
    retry_if: Callable[[BaseException], bool] = is_retryable

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts deve ser >= 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("atrasos não podem ser negativos")
        if self.multiplier < 1:
            raise ValueError("multiplier deve ser >= 1")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter deve estar entre 0 e 1")

    def delay_for(self, attempt: int, rng: Callable[[], float] = random.random) -> float:
        """Atraso antes da tentativa seguinte, dado que ``attempt`` (1-based) falhou."""
        raw = min(self.max_delay, self.base_delay * self.multiplier ** (attempt - 1))
        factor = 1.0 + self.jitter * (2.0 * rng() - 1.0)
        return min(self.max_delay, max(0.0, raw * factor))


def call_with_retry[T](
    fn: Callable[[], T],
    policy: RetryPolicy,
    *,
    sleep: Callable[[float], None] = time.sleep,
    rng: Callable[[], float] = random.random,
    on_retry: Callable[[int, BaseException, float], None] | None = None,
) -> T:
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn()
        except Exception as exc:
            if not policy.retry_if(exc):
                raise
            if attempt >= policy.max_attempts:
                raise RetryExhaustedError(
                    f"falhou após {attempt} tentativa(s): {exc}", attempts=attempt
                ) from exc
            delay = policy.delay_for(attempt, rng)
            logger.warning(
                "tentativa %d/%d falhou (%s); nova tentativa em %.2fs",
                attempt,
                policy.max_attempts,
                exc,
                delay,
            )
            if on_retry is not None:
                on_retry(attempt, exc, delay)
            sleep(delay)


def retry[**P, T](
    policy: RetryPolicy | None = None,
    *,
    sleep: Callable[[float], None] = time.sleep,
    rng: Callable[[], float] = random.random,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator: ``@retry(RetryPolicy(max_attempts=5))``."""
    effective = policy or RetryPolicy()

    def decorator(fn: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            return call_with_retry(lambda: fn(*args, **kwargs), effective, sleep=sleep, rng=rng)

        return wrapper

    return decorator
