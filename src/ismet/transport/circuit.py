"""Circuit breaker: stop hammering a failing endpoint, probe for recovery."""

from __future__ import annotations

from enum import Enum

from ismet.errors import CircuitOpen
from ismet.transport.clock import SYSTEM_CLOCK, Clock


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Opens after ``failure_threshold`` consecutive failures.

    While open, :meth:`check` raises :class:`CircuitOpen` without calling out.
    After ``recovery_timeout`` seconds one probe call is allowed (half-open);
    success closes the circuit, failure re-opens it. A probe that ends without
    a recorded outcome (cancelled, or failed in a way the caller does not
    count) must call :meth:`release_probe` so the next call may probe again.
    """

    def __init__(
        self,
        name: str,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        if failure_threshold < 1 or recovery_timeout < 0:
            raise ValueError("failure_threshold >= 1 and recovery_timeout >= 0")
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._clock = clock
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._probing = False

    @property
    def state(self) -> CircuitState:
        if (
            self._state is CircuitState.OPEN
            and self._elapsed() >= self.recovery_timeout
        ):
            self._state = CircuitState.HALF_OPEN
            self._probing = False
        return self._state

    @property
    def failures(self) -> int:
        return self._failures

    def _elapsed(self) -> float:
        return self._clock.monotonic() - self._opened_at

    def check(self) -> None:
        """Raise :class:`CircuitOpen` unless a call may proceed."""
        state = self.state
        if state is CircuitState.CLOSED:
            return
        if state is CircuitState.HALF_OPEN and not self._probing:
            self._probing = True
            return
        remaining = max(0.0, self.recovery_timeout - self._elapsed())
        raise CircuitOpen(self.name, remaining)

    def record_success(self) -> None:
        self._failures = 0
        self._state = CircuitState.CLOSED
        self._probing = False

    def record_failure(self) -> None:
        self._failures += 1
        if (
            self._state is CircuitState.HALF_OPEN
            or self._failures >= self.failure_threshold
        ):
            self._state = CircuitState.OPEN
            self._opened_at = self._clock.monotonic()
            self._probing = False

    def release_probe(self) -> None:
        """Give up the half-open probe slot without recording an outcome."""
        if self._state is CircuitState.HALF_OPEN:
            self._probing = False

    def reset(self) -> None:
        self.record_success()


__all__ = ["CircuitBreaker", "CircuitState"]
