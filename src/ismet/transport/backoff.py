"""Exponential backoff and a generic async retry helper."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from typing import TypeVar

from ismet.errors import IsmetError, RateLimited, TransportError

T = TypeVar("T")


@dataclass(frozen=True)
class ExponentialBackoff:
    """Delays ``base * factor**n`` capped at ``maximum`` with full jitter.

    With ``jitter`` set, each delay is drawn uniformly from ``[0, d]`` where
    ``d`` is the un-jittered delay, which spreads retries from many clients.
    """

    base: float = 0.2
    factor: float = 2.0
    maximum: float = 30.0
    jitter: bool = True

    def __post_init__(self) -> None:
        if self.base < 0 or self.factor < 1 or self.maximum < 0:
            raise ValueError("base >= 0, factor >= 1, maximum >= 0 required")

    def delay(self, attempt: int, rng: random.Random | None = None) -> float:
        """Delay before retry number ``attempt`` (0-based)."""
        raw = min(self.maximum, self.base * (self.factor**attempt))
        if not self.jitter:
            return raw
        return (rng or random).uniform(0.0, raw)

    def delays(self, rng: random.Random | None = None) -> Iterator[float]:
        attempt = 0
        while True:
            yield self.delay(attempt, rng)
            attempt += 1


def default_should_retry(exc: BaseException) -> bool:
    """Retry on retryable transport errors and rate limits; nothing else."""
    if isinstance(exc, TransportError):
        return exc.retryable
    return isinstance(exc, RateLimited)


@dataclass(frozen=True)
class RetryPolicy:
    """How many times to retry, with what backoff, on which exceptions."""

    max_attempts: int = 3
    backoff: ExponentialBackoff = ExponentialBackoff()
    should_retry: Callable[[BaseException], bool] = default_should_retry
    max_retry_after: float = 60.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

    def delay_for(self, attempt: int, exc: BaseException) -> float:
        """Delay before the next attempt, honouring ``RateLimited.retry_after``."""
        if isinstance(exc, RateLimited) and exc.retry_after is not None:
            return min(max(exc.retry_after, 0.0), self.max_retry_after)
        return self.backoff.delay(attempt)


NO_RETRY = RetryPolicy(max_attempts=1)
DEFAULT_RETRY = RetryPolicy()


async def retry(
    fn: Callable[[], Awaitable[T]],
    policy: RetryPolicy = DEFAULT_RETRY,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    on_retry: Callable[[int, BaseException, float], None] | None = None,
) -> T:
    """Call ``fn`` until it succeeds or ``policy.max_attempts`` is exhausted.

    The last exception is re-raised unchanged. ``on_retry(attempt, exc, delay)``
    is invoked before each sleep for logging or metrics.
    """
    attempt = 0
    while True:
        try:
            return await fn()
        except IsmetError as exc:
            attempt += 1
            if attempt >= policy.max_attempts or not policy.should_retry(exc):
                raise
            delay = policy.delay_for(attempt - 1, exc)
            if on_retry is not None:
                on_retry(attempt, exc, delay)
            await sleep(delay)


__all__ = [
    "DEFAULT_RETRY",
    "NO_RETRY",
    "ExponentialBackoff",
    "RetryPolicy",
    "default_should_retry",
    "retry",
]
