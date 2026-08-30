from __future__ import annotations

import pytest

from ismet.errors import CircuitOpen
from ismet.transport.circuit import CircuitBreaker, CircuitState
from ismet.transport.clock import ManualClock


def test_opens_after_threshold_and_recovers() -> None:
    clock = ManualClock()
    cb = CircuitBreaker("api", failure_threshold=2, recovery_timeout=5, clock=clock)
    cb.check()
    cb.record_failure()
    assert cb.state is CircuitState.CLOSED
    cb.record_failure()
    assert cb.state is CircuitState.OPEN
    with pytest.raises(CircuitOpen) as info:
        cb.check()
    assert info.value.retry_after == pytest.approx(5)
    clock.advance(5)
    assert cb.state is CircuitState.HALF_OPEN
    cb.check()  # one probe allowed
    with pytest.raises(CircuitOpen):
        cb.check()  # second probe refused
    cb.record_success()
    assert cb.state is CircuitState.CLOSED
    assert cb.failures == 0


def test_half_open_failure_reopens() -> None:
    clock = ManualClock()
    cb = CircuitBreaker("api", failure_threshold=1, recovery_timeout=1, clock=clock)
    cb.record_failure()
    clock.advance(1)
    cb.check()
    cb.record_failure()
    assert cb.state is CircuitState.OPEN
    cb.reset()
    assert cb.state is CircuitState.CLOSED


def test_invalid_parameters() -> None:
    with pytest.raises(ValueError):
        CircuitBreaker("x", failure_threshold=0)
    with pytest.raises(ValueError):
        CircuitBreaker("x", recovery_timeout=-1)
