from __future__ import annotations

import pytest

from ismet.transport.clock import ManualClock
from ismet.transport.ratelimit import RateLimiter, RateLimitSpec, TokenBucket


def _sleeper(clock: ManualClock, log: list[float]):  # type: ignore[no-untyped-def]
    async def sleep(d: float) -> None:
        log.append(d)
        clock.advance(d)

    return sleep


def test_spec_validation_and_constructors() -> None:
    with pytest.raises(ValueError):
        RateLimitSpec(rate=0, capacity=1)
    with pytest.raises(ValueError):
        RateLimitSpec(rate=1, capacity=0)
    assert RateLimitSpec.per_minute(60).rate == 1.0
    assert RateLimitSpec.per_minute(120, burst=5).capacity == 5
    assert RateLimitSpec.per_second(10).capacity == 10


async def test_token_bucket_waits_when_empty() -> None:
    clock = ManualClock()
    log: list[float] = []
    bucket = TokenBucket(
        RateLimitSpec(rate=2, capacity=2), clock=clock, sleep=_sleeper(clock, log)
    )
    assert await bucket.acquire() == 0
    assert await bucket.acquire() == 0
    waited = await bucket.acquire()
    assert waited == pytest.approx(0.5)
    assert log == [pytest.approx(0.5)]
    clock.advance(10)
    assert bucket.available == 2
    with pytest.raises(ValueError):
        await bucket.acquire(3)


async def test_rate_limiter_routes_by_key() -> None:
    clock = ManualClock()
    log: list[float] = []
    limiter = RateLimiter(
        {"orders": RateLimitSpec(rate=1, capacity=1)},
        default=RateLimitSpec(rate=100, capacity=100),
        clock=clock,
        sleep=_sleeper(clock, log),
    )
    assert await limiter.acquire("orders") == 0
    assert await limiter.acquire("orders") == pytest.approx(1.0)
    assert await limiter.acquire("quotes") == 0
    assert await limiter.acquire() == 0
    assert limiter.bucket("orders") is not limiter.bucket("quotes")


async def test_rate_limiter_without_buckets_never_waits() -> None:
    assert await RateLimiter().acquire("anything") == 0
    assert RateLimiter().bucket("x") is None
