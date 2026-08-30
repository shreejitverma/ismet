from __future__ import annotations

import random

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ismet.errors import AuthError, RateLimited, TransportError
from ismet.transport.backoff import (
    ExponentialBackoff,
    RetryPolicy,
    default_should_retry,
    retry,
)


def test_backoff_without_jitter_is_exponential_and_capped() -> None:
    b = ExponentialBackoff(base=1, factor=2, maximum=5, jitter=False)
    assert [b.delay(i) for i in range(4)] == [1, 2, 4, 5]
    it = b.delays()
    assert [next(it) for _ in range(3)] == [1, 2, 4]


@given(st.integers(min_value=0, max_value=50), st.integers())
def test_jittered_delay_within_bounds(attempt: int, seed: int) -> None:
    b = ExponentialBackoff(base=0.5, factor=2, maximum=10)
    d = b.delay(attempt, random.Random(seed))
    assert 0 <= d <= min(10, 0.5 * 2**attempt)


@given(st.integers(min_value=0, max_value=10**6))
def test_delay_never_overflows_for_large_attempts(attempt: int) -> None:
    b = ExponentialBackoff(base=0.5, factor=2.0, maximum=30.0, jitter=False)
    assert 0.0 <= b.delay(attempt) <= 30.0
    assert 0.0 <= b.delay(attempt, random.Random(attempt)) <= 30.0


def test_huge_attempt_is_capped_at_maximum() -> None:
    b = ExponentialBackoff(base=0.5, factor=2.0, maximum=30.0, jitter=False)
    assert b.delay(10_000) == 30.0
    it = ExponentialBackoff(base=0.5, factor=2.0, maximum=30.0).delays()
    assert all(0.0 <= next(it) <= 30.0 for _ in range(2_000))
    zero = ExponentialBackoff(base=0, factor=2.0, maximum=30.0, jitter=False)
    assert zero.delay(10_000) == 0.0


def test_invalid_parameters() -> None:
    with pytest.raises(ValueError):
        ExponentialBackoff(base=-1)
    with pytest.raises(ValueError):
        ExponentialBackoff(factor=0.5)
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)


def test_default_should_retry() -> None:
    assert default_should_retry(TransportError("x"))
    assert not default_should_retry(TransportError("x", retryable=False))
    assert default_should_retry(RateLimited("x"))
    assert not default_should_retry(AuthError("x"))
    assert not default_should_retry(ValueError("x"))


async def test_retry_succeeds_after_retryable_failures() -> None:
    calls = 0
    slept: list[float] = []
    seen: list[tuple[int, float]] = []

    async def fn() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise TransportError("boom")
        return "ok"

    async def sleep(d: float) -> None:
        slept.append(d)

    policy = RetryPolicy(
        max_attempts=3, backoff=ExponentialBackoff(base=1, jitter=False)
    )
    result = await retry(
        fn, policy, sleep=sleep, on_retry=lambda a, _, d: seen.append((a, d))
    )
    assert result == "ok"
    assert calls == 3
    assert slept == [1, 2]
    assert seen == [(1, 1), (2, 2)]


async def test_retry_gives_up_after_max_attempts() -> None:
    calls = 0

    async def fn() -> None:
        nonlocal calls
        calls += 1
        raise TransportError("boom")

    async def sleep(d: float) -> None:
        pass

    with pytest.raises(TransportError):
        await retry(fn, RetryPolicy(max_attempts=2), sleep=sleep)
    assert calls == 2


async def test_retry_does_not_retry_non_retryable() -> None:
    calls = 0

    async def fn() -> None:
        nonlocal calls
        calls += 1
        raise AuthError("nope")

    with pytest.raises(AuthError):
        await retry(fn, RetryPolicy(max_attempts=5))
    assert calls == 1


async def test_retry_honours_retry_after_capped() -> None:
    slept: list[float] = []
    calls = 0

    async def fn() -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RateLimited("slow", retry_after=500)
        if calls == 2:
            raise RateLimited("slow", retry_after=0.25)
        return calls

    async def sleep(d: float) -> None:
        slept.append(d)

    policy = RetryPolicy(max_attempts=3, max_retry_after=10)
    assert await retry(fn, policy, sleep=sleep) == 3
    assert slept == [10, 0.25]
