from __future__ import annotations

import pytest

from taskforge.exceptions import PermanentError, RetryExhaustedError, TransientError
from taskforge.patterns.retry import RetryPolicy, call_with_retry, retry


def test_exponential_delays_are_capped() -> None:
    policy = RetryPolicy(base_delay=1, multiplier=2, max_delay=5, jitter=0)
    assert [policy.delay_for(n) for n in (1, 2, 3, 4, 5)] == [1, 2, 4, 5, 5]


def test_jitter_range() -> None:
    policy = RetryPolicy(base_delay=10, max_delay=100, jitter=0.2)
    assert policy.delay_for(1, rng=lambda: 0.0) == pytest.approx(8.0)
    assert policy.delay_for(1, rng=lambda: 0.5) == pytest.approx(10.0)
    assert policy.delay_for(1, rng=lambda: 1.0) == pytest.approx(12.0)


@pytest.mark.parametrize(
    "kwargs",
    [{"max_attempts": 0}, {"base_delay": -1}, {"multiplier": 0.5}, {"jitter": 1.5}],
)
def test_invalid_policy(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]


def test_succeeds_after_transient_failures() -> None:
    calls: list[int] = []
    sleeps: list[float] = []

    def flaky() -> str:
        calls.append(1)
        if len(calls) < 3:
            raise TransientError("ainda não")
        return "ok"

    policy = RetryPolicy(max_attempts=5, base_delay=1, jitter=0)
    assert call_with_retry(flaky, policy, sleep=sleeps.append) == "ok"
    assert len(calls) == 3
    assert sleeps == [1, 2]


def test_permanent_error_is_not_retried() -> None:
    sleeps: list[float] = []

    def broken() -> None:
        raise PermanentError("fatal")

    with pytest.raises(PermanentError):
        call_with_retry(broken, RetryPolicy(), sleep=sleeps.append)
    assert sleeps == []


def test_exhaustion_keeps_cause_and_attempts() -> None:
    def always() -> None:
        raise TransientError("sempre")

    with pytest.raises(RetryExhaustedError) as info:
        call_with_retry(always, RetryPolicy(max_attempts=3, base_delay=0), sleep=lambda _: None)
    assert info.value.attempts == 3
    assert isinstance(info.value.__cause__, TransientError)


def test_on_retry_callback() -> None:
    seen: list[tuple[int, str, float]] = []

    def always() -> None:
        raise TransientError("x")

    with pytest.raises(RetryExhaustedError):
        call_with_retry(
            always,
            RetryPolicy(max_attempts=3, base_delay=1, jitter=0),
            sleep=lambda _: None,
            on_retry=lambda n, exc, delay: seen.append((n, str(exc), delay)),
        )
    assert seen == [(1, "x", 1.0), (2, "x", 2.0)]


def test_custom_retry_predicate() -> None:
    calls = 0

    def fn() -> None:
        nonlocal calls
        calls += 1
        raise KeyError("k")

    policy = RetryPolicy(max_attempts=2, base_delay=0, retry_if=lambda e: isinstance(e, KeyError))
    with pytest.raises(RetryExhaustedError):
        call_with_retry(fn, policy, sleep=lambda _: None)
    assert calls == 2


def test_decorator_preserves_metadata_and_retries() -> None:
    calls = 0

    @retry(RetryPolicy(max_attempts=3, base_delay=0), sleep=lambda _: None)
    def fetch(x: int) -> int:
        """doc"""
        nonlocal calls
        calls += 1
        if calls < 2:
            raise TransientError("x")
        return x * 2

    assert fetch(21) == 42
    assert fetch.__name__ == "fetch" and fetch.__doc__ == "doc"
