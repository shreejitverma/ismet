from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import httpx
import pytest
from pydantic import SecretStr

from ismet.errors import (
    AmbiguousOutcome,
    AuthError,
    CircuitOpen,
    RateLimited,
    TransportError,
    VenueError,
)
from ismet.transport.backoff import NO_RETRY, ExponentialBackoff, RetryPolicy
from ismet.transport.circuit import CircuitBreaker, CircuitState
from ismet.transport.clock import ManualClock
from ismet.transport.http import (
    BearerAuth,
    HeaderAuth,
    HttpTransport,
    QueryAuth,
    RequestInfo,
    map_response_error,
    retry_after_seconds,
)
from ismet.transport.ratelimit import RateLimiter, RateLimitSpec

FAST = RetryPolicy(max_attempts=3, backoff=ExponentialBackoff(base=0, jitter=False))


def make(handler, **kw):  # type: ignore[no-untyped-def]
    return HttpTransport(
        "https://api.example.test/",
        transport=httpx.MockTransport(handler),
        retry_policy=kw.pop("retry_policy", FAST),
        **kw,
    )


async def test_json_floats_become_decimal() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text='{"price": 101.10, "n": 3}')

    async with make(handler) as t:
        data = await t.get_json("/quote")
    assert data == {"price": Decimal("101.10"), "n": 3}
    assert isinstance(data["price"], Decimal)
    assert not t.is_open


async def test_post_json_and_response_text() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.read() == b'{"a":1}'
        return httpx.Response(201, text='{"ok": true}')

    t = make(handler)
    assert await t.post_json("/orders", {"a": 1}) == {"ok": True}
    r = await t.request("POST", "/orders", json_body={"a": 1})
    assert r.text() == '{"ok": true}'
    await t.close()


@pytest.mark.parametrize(
    ("status", "exc"),
    [(401, AuthError), (403, AuthError), (429, RateLimited), (404, VenueError)],
)
async def test_non_retryable_statuses_map_and_do_not_retry(
    status: int, exc: type[Exception]
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            status, json={"code": "E42"}, headers={"Retry-After": "3"}
        )

    t = make(
        handler, retry_policy=RetryPolicy(max_attempts=3, should_retry=lambda _: False)
    )
    with pytest.raises(exc) as info:
        await t.request("GET", "/x")
    assert calls == 1
    if status == 404:
        assert isinstance(info.value, VenueError)
        assert info.value.code == "E42"
        assert info.value.status == 404
    if status == 429:
        assert isinstance(info.value, RateLimited)
        assert info.value.retry_after == 3


async def test_server_errors_retry_then_succeed() -> None:
    calls = 0
    seen: list[RequestInfo] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503 if calls < 3 else 200, json={"ok": 1})

    t = make(handler, hooks=[seen.append])
    assert await t.get_json("/x") == {"ok": 1}
    assert calls == 3
    assert [i.status for i in seen] == [503, 503, 200]
    assert [i.attempt for i in seen] == [1, 2, 3]
    assert isinstance(seen[0].error, TransportError)


async def test_timeout_and_transport_failures_are_retryable() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ReadTimeout("slow")
        if calls == 2:
            raise httpx.ConnectError("down")
        return httpx.Response(200, json=[])

    assert await make(handler).get_json("/x") == []
    assert calls == 3


async def test_rate_limited_retry_after_is_honoured() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"retry-after": "0"})
        return httpx.Response(200, json=1)

    assert await make(handler).get_json("/x") == 1


async def test_circuit_breaker_opens_and_blocks() -> None:
    clock = ManualClock()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    breaker = CircuitBreaker(
        "api", failure_threshold=2, recovery_timeout=60, clock=clock
    )
    t = make(handler, breaker=breaker, clock=clock, retry_policy=NO_RETRY)
    for _ in range(2):
        with pytest.raises(TransportError):
            await t.request("GET", "/x")
    with pytest.raises(CircuitOpen):
        await t.request("GET", "/x")


async def test_open_circuit_is_checked_before_rate_limit_tokens() -> None:
    clock = ManualClock()
    slept: list[float] = []

    async def sleep(d: float) -> None:
        slept.append(d)
        clock.advance(d)

    limiter = RateLimiter(
        default=RateLimitSpec(rate=1, capacity=1), clock=clock, sleep=sleep
    )
    breaker = CircuitBreaker(
        "api", failure_threshold=1, recovery_timeout=60, clock=clock
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    t = make(
        handler,
        breaker=breaker,
        rate_limiter=limiter,
        clock=clock,
        retry_policy=NO_RETRY,
    )
    with pytest.raises(TransportError):
        await t.request("GET", "/x")
    assert slept == []
    for _ in range(2):
        with pytest.raises(CircuitOpen):
            await t.request("GET", "/x")
    assert slept == []


async def test_cancelled_half_open_probe_releases_breaker() -> None:
    clock = ManualClock()
    blocked = asyncio.Event()
    release = asyncio.Event()

    async def sleep(d: float) -> None:
        blocked.set()
        await release.wait()
        clock.advance(d)

    limiter = RateLimiter(
        default=RateLimitSpec(rate=1, capacity=1), clock=clock, sleep=sleep
    )
    breaker = CircuitBreaker(
        "api", failure_threshold=1, recovery_timeout=0, clock=clock
    )
    statuses = [500, 200]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(statuses.pop(0), json={})

    t = make(
        handler,
        breaker=breaker,
        rate_limiter=limiter,
        clock=clock,
        retry_policy=NO_RETRY,
    )
    with pytest.raises(TransportError):
        await t.request("GET", "/x")
    assert breaker.state is CircuitState.HALF_OPEN

    probe = asyncio.create_task(t.request("GET", "/x"))
    await asyncio.wait_for(blocked.wait(), 5)
    probe.cancel()
    with pytest.raises(asyncio.CancelledError):
        await probe

    assert breaker.state is CircuitState.HALF_OPEN
    breaker.check()
    breaker.release_probe()
    clock.advance(1)
    assert (await t.request("GET", "/x")).status == 200
    assert breaker.state is CircuitState.CLOSED


async def test_uncounted_half_open_failure_releases_breaker() -> None:
    clock = ManualClock()
    statuses = [500, 401, 200]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(statuses.pop(0), json={})

    breaker = CircuitBreaker(
        "api", failure_threshold=1, recovery_timeout=1, clock=clock
    )
    t = make(handler, breaker=breaker, clock=clock, retry_policy=NO_RETRY)
    with pytest.raises(TransportError):
        await t.request("GET", "/x")
    clock.advance(1)
    with pytest.raises(AuthError):
        await t.request("GET", "/x")
    assert breaker.state is CircuitState.HALF_OPEN
    assert (await t.request("GET", "/x")).status == 200
    assert breaker.state is CircuitState.CLOSED


async def test_auth_and_venue_errors_do_not_trip_breaker() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401)

    breaker = CircuitBreaker("api", failure_threshold=1)
    t = make(handler, breaker=breaker, retry_policy=NO_RETRY)
    with pytest.raises(AuthError):
        await t.request("GET", "/x")
    assert breaker.failures == 0


async def test_rate_limiter_is_consulted() -> None:
    clock = ManualClock()
    slept: list[float] = []

    async def sleep(d: float) -> None:
        slept.append(d)
        clock.advance(d)

    limiter = RateLimiter(
        default=RateLimitSpec(rate=1, capacity=1), clock=clock, sleep=sleep
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

    t = make(handler, rate_limiter=limiter, clock=clock)
    await t.get_json("/a")
    await t.get_json("/b")
    assert slept == [pytest.approx(1.0)]


async def test_auth_helpers_inject_secrets() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={})

    await make(handler, auth=BearerAuth(SecretStr("tok"))).get_json("/1")
    await make(handler, auth=HeaderAuth("X-API-Key", "k1")).get_json("/2")
    await make(handler, auth=QueryAuth("token", "q1")).get_json("/3", params={"a": "b"})
    assert captured[0].headers["Authorization"] == "Bearer tok"
    assert captured[1].headers["X-API-Key"] == "k1"
    assert captured[2].url.params["token"] == "q1"
    assert captured[2].url.params["a"] == "b"


async def test_hooks_never_receive_query_auth_secret() -> None:
    seen: list[RequestInfo] = []
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.url.params["token"] == "q1"
        return httpx.Response(500 if calls == 1 else 200, json={})

    t = make(handler, auth=QueryAuth("token", "q1"), hooks=[seen.append])
    await t.get_json("/quote", params={"a": "b"})
    assert [i.status for i in seen] == [500, 200]
    assert all(i.url == "/quote" for i in seen)
    assert all("q1" not in repr(i) for i in seen)


@pytest.mark.parametrize("status", [301, 302, 304])
async def test_redirects_are_not_followed_and_map_to_ismet_error(
    status: int,
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(status, headers={"Location": "https://login.test/"})

    t = make(handler)
    with pytest.raises(TransportError, match=f"HTTP {status}") as info:
        await t.get_json("/x")
    assert calls == 1
    assert info.value.status == status and info.value.retryable is False
    if status != 304:
        assert "https://login.test/" in str(info.value)


async def test_non_json_success_body_raises_transport_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>maintenance</html>")

    t = make(handler)
    with pytest.raises(TransportError, match="not JSON") as info:
        await t.get_json("/x")
    assert info.value.status == 200 and info.value.retryable is False
    assert "maintenance" in str(info.value)
    with pytest.raises(TransportError, match="not JSON"):
        await t.post_json("/x", {})
    assert (await t.request("GET", "/x")).text() == "<html>maintenance</html>"


def test_map_response_error_variants() -> None:
    e = map_response_error(429, b"", {"retry-after": "bogus"})
    assert isinstance(e, RateLimited) and e.retry_after is None
    for not_a_wait in ("inf", "nan"):
        limited = map_response_error(429, b"", {"retry-after": not_a_wait})
        assert isinstance(limited, RateLimited) and limited.retry_after is None
    v = map_response_error(400, b"not json", {})
    assert isinstance(v, VenueError) and v.payload is None and v.code is None
    v2 = map_response_error(422, b'{"error_code": 7}', {})
    assert isinstance(v2, VenueError) and v2.code == "7"
    s = map_response_error(502, b"bad gateway", {})
    assert isinstance(s, TransportError) and s.retryable and s.status == 502
    r = map_response_error(302, b"", {"location": "/login"})
    assert isinstance(r, TransportError) and not r.retryable and r.status == 302
    assert "/login" in str(r)


async def test_a_failing_hook_never_replaces_the_result_of_a_request() -> None:
    def broken(info: object) -> None:
        raise OSError("metrics backend is down")

    seen: list[object] = []

    def ok(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": 1})

    t = make(ok, hooks=[broken, seen.append])
    assert await t.get_json("/x") == {"ok": 1}
    assert len(seen) == 1


async def test_a_failing_hook_keeps_the_ambiguous_outcome_of_an_order() -> None:
    def broken(info: object) -> None:
        raise OSError("metrics backend is down")

    def lost(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("no answer", request=request)

    t = make(lost, hooks=[broken])
    with pytest.raises(AmbiguousOutcome):
        await t.request("POST", "/order", json_body={"q": 1}, idempotent=False)


IN_FIVE_MINUTES = "Sun, 01 Nov 2026 23:00:00 GMT"
FROZEN = datetime(2026, 11, 1, 22, 0, tzinfo=timezone.utc)


def test_a_retry_after_date_is_read_against_the_response_date() -> None:
    headers = {
        "retry-after": IN_FIVE_MINUTES,
        "date": "Sun, 01 Nov 2026 22:55:00 GMT",
    }
    assert retry_after_seconds(headers, clock=ManualClock(FROZEN)) == 300.0


@pytest.mark.parametrize(
    "headers",
    [
        {"retry-after": IN_FIVE_MINUTES},
        {"retry-after": IN_FIVE_MINUTES, "date": "not a date"},
    ],
    ids=["no-date-header", "unreadable-date-header"],
)
def test_a_retry_after_date_without_a_usable_date_uses_the_clock(
    headers: dict[str, str],
) -> None:
    assert retry_after_seconds(headers, clock=ManualClock(FROZEN)) == 3600.0


def test_a_retry_after_date_in_the_past_means_no_wait_not_a_negative_one() -> None:
    late = ManualClock(datetime(2026, 11, 2, tzinfo=timezone.utc))
    assert retry_after_seconds({"retry-after": IN_FIVE_MINUTES}, clock=late) == 0.0


@pytest.mark.parametrize("value", ["soon", "", "5 seconds", "12:00", "nan", "inf"])
def test_an_unreadable_retry_after_is_still_none(value: str) -> None:
    assert retry_after_seconds({"retry-after": value}) is None


def test_seconds_are_still_seconds() -> None:
    assert retry_after_seconds({"retry-after": "120"}) == 120.0
    assert retry_after_seconds({"retry-after": "-5"}) == 0.0
    assert retry_after_seconds({}) is None


async def test_the_transport_reads_a_date_form_retry_after_with_its_own_clock() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": IN_FIVE_MINUTES})

    async with make(
        handler, retry_policy=NO_RETRY, clock=ManualClock(FROZEN)
    ) as transport:
        with pytest.raises(RateLimited) as caught:
            await transport.request("GET", "/x")
    assert caught.value.retry_after == 3600.0
