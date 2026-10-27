from __future__ import annotations

import pytest

from helpers import FakeClock
from taskforge.exceptions import CircuitOpenError, JobValidationError, TransientError
from taskforge.patterns.circuit_breaker import BreakerState, CircuitBreaker


def fail() -> None:
    raise TransientError("fora do ar")


def make(clock: FakeClock, **kw: object) -> CircuitBreaker:
    return CircuitBreaker("t", failure_threshold=3, recovery_timeout=10, clock=clock, **kw)  # type: ignore[arg-type]


def trip(breaker: CircuitBreaker, times: int = 3) -> None:
    for _ in range(times):
        with pytest.raises(TransientError), breaker.guard():
            fail()


def test_opens_after_threshold_and_fails_fast() -> None:
    breaker = make(FakeClock())
    trip(breaker)
    assert breaker.state is BreakerState.OPEN
    called = False
    with pytest.raises(CircuitOpenError):
        with breaker.guard():
            called = True
    assert not called, "com o circuito aberto o corpo nem deve executar"


def test_success_resets_failure_count() -> None:
    breaker = make(FakeClock())
    trip(breaker, 2)
    with breaker.guard():
        pass
    trip(breaker, 2)  # contagem recomeçou: 2 < 3
    assert breaker.state is BreakerState.CLOSED


def test_half_open_success_closes() -> None:
    clock = FakeClock()
    breaker = make(clock)
    trip(breaker)
    clock.advance(10)
    assert breaker.state is BreakerState.HALF_OPEN
    with breaker.guard():
        pass
    assert breaker.state is BreakerState.CLOSED


def test_half_open_failure_reopens() -> None:
    clock = FakeClock()
    breaker = make(clock)
    trip(breaker)
    clock.advance(10)
    trip(breaker, 1)
    assert breaker.state is BreakerState.OPEN
    clock.advance(9)
    assert breaker.state is BreakerState.OPEN, "o relógio de recuperação reinicia"


def test_half_open_limits_concurrent_probes() -> None:
    clock = FakeClock()
    breaker = make(clock)
    trip(breaker)
    clock.advance(10)
    with breaker.guard():  # sonda em andamento
        with pytest.raises(CircuitOpenError):
            with breaker.guard():
                pass
    assert breaker.state is BreakerState.CLOSED


def test_non_tripping_errors_do_not_count() -> None:
    breaker = make(FakeClock())
    for _ in range(10):
        with pytest.raises(JobValidationError), breaker.guard():
            raise JobValidationError("payload ruim")
    assert breaker.state is BreakerState.CLOSED


def test_decorator_usage() -> None:
    breaker = make(FakeClock())

    @breaker
    def call() -> None:
        fail()

    for _ in range(3):
        with pytest.raises(TransientError):
            call()
    with pytest.raises(CircuitOpenError):
        call()


def test_keyboard_interrupt_is_neutral_for_probes() -> None:
    clock = FakeClock()
    breaker = make(clock)
    trip(breaker)
    clock.advance(10)
    with pytest.raises(KeyboardInterrupt), breaker.guard():
        raise KeyboardInterrupt
    # a sonda foi liberada sem veredito: ainda dá para testar de novo
    with breaker.guard():
        pass
    assert breaker.state is BreakerState.CLOSED


def test_invalid_arguments() -> None:
    with pytest.raises(ValueError):
        CircuitBreaker("x", failure_threshold=0)
