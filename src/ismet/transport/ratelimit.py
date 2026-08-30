"""Token-bucket rate limiting keyed by endpoint group."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from ismet.transport.clock import SYSTEM_CLOCK, Clock


@dataclass(frozen=True)
class RateLimitSpec:
    """``rate`` tokens per second, bursting up to ``capacity``."""

    rate: float
    capacity: int

    def __post_init__(self) -> None:
        if self.rate <= 0 or self.capacity < 1:
            raise ValueError("rate > 0 and capacity >= 1 required")

    @classmethod
    def per_minute(cls, count: int, burst: int | None = None) -> RateLimitSpec:
        return cls(rate=count / 60.0, capacity=burst or count)

    @classmethod
    def per_second(cls, count: int, burst: int | None = None) -> RateLimitSpec:
        return cls(rate=float(count), capacity=burst or count)


class TokenBucket:
    """Async token bucket. ``acquire`` waits until a token is available."""

    def __init__(
        self,
        spec: RateLimitSpec,
        *,
        clock: Clock = SYSTEM_CLOCK,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.spec = spec
        self._clock = clock
        self._sleep = sleep
        self._tokens = float(spec.capacity)
        self._last = clock.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock.monotonic()
        elapsed = max(0.0, now - self._last)
        self._last = now
        self._tokens = min(
            float(self.spec.capacity), self._tokens + elapsed * self.spec.rate
        )

    @property
    def available(self) -> float:
        self._refill()
        return self._tokens

    async def acquire(self, tokens: int = 1) -> float:
        """Take ``tokens``; returns seconds waited."""
        if tokens > self.spec.capacity:
            raise ValueError(f"cannot acquire {tokens} > capacity {self.spec.capacity}")
        waited = 0.0
        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return waited
                deficit = tokens - self._tokens
                delay = deficit / self.spec.rate
                waited += delay
                await self._sleep(delay)


class RateLimiter:
    """A set of token buckets keyed by name, with a default bucket."""

    def __init__(
        self,
        specs: dict[str, RateLimitSpec] | None = None,
        *,
        default: RateLimitSpec | None = None,
        clock: Clock = SYSTEM_CLOCK,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._clock = clock
        self._sleep = sleep
        self._buckets: dict[str, TokenBucket] = {
            k: TokenBucket(v, clock=clock, sleep=sleep)
            for k, v in (specs or {}).items()
        }
        self._default = (
            TokenBucket(default, clock=clock, sleep=sleep) if default else None
        )

    def bucket(self, key: str | None) -> TokenBucket | None:
        if key is not None and key in self._buckets:
            return self._buckets[key]
        return self._default

    async def acquire(self, key: str | None = None, tokens: int = 1) -> float:
        """Wait for the bucket for ``key`` (or the default). No bucket: no wait."""
        bucket = self.bucket(key)
        if bucket is None:
            return 0.0
        return await bucket.acquire(tokens)


__all__ = ["RateLimitSpec", "RateLimiter", "TokenBucket"]
