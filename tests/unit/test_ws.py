from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import InvalidStatus, InvalidURI
from websockets.http11 import Request, Response

from ismet.errors import AuthError, RateLimited, TransportError
from ismet.transport.backoff import ExponentialBackoff
from ismet.transport.clock import ManualClock
from ismet.transport.ws import (
    BackpressurePolicy,
    ConnectionState,
    SequenceTracker,
    WebSocketTransport,
)

Handler = Callable[[ServerConnection], Awaitable[None]]
ProcessRequest = Callable[[ServerConnection, Request], Response | None]
FAST = ExponentialBackoff(base=0.01, maximum=0.02, jitter=False)


@pytest.fixture
async def server_factory() -> AsyncIterator[Callable[..., Awaitable[str]]]:
    servers: list[Server] = []

    async def start(handler: Handler, **serve_kwargs: object) -> str:
        srv = await serve(handler, "127.0.0.1", 0, **serve_kwargs)  # type: ignore[arg-type]
        servers.append(srv)
        port = next(iter(srv.sockets)).getsockname()[1]
        return f"ws://127.0.0.1:{port}"

    yield start
    for srv in servers:
        srv.close()
        await srv.wait_closed()


async def idle(conn: ServerConnection) -> None:
    await conn.wait_closed()


def reject_with(
    status: int, times: int | None = None, seen: list[int] | None = None
) -> ProcessRequest:
    def process_request(conn: ServerConnection, request: Request) -> Response | None:
        if times is not None and seen is not None and len(seen) >= times:
            return None
        if seen is not None:
            seen.append(status)
        return conn.respond(status, "rejected")

    return process_request


def make(url: str, **kw: object) -> WebSocketTransport:
    kw.setdefault("backoff", FAST)
    kw.setdefault("heartbeat_interval", None)
    kw.setdefault("heartbeat_timeout", None)
    return WebSocketTransport(url, **kw)  # type: ignore[arg-type]


async def test_receives_decoded_messages_and_sends(server_factory) -> None:  # type: ignore[no-untyped-def]
    got: list[str] = []

    async def handler(conn: ServerConnection) -> None:
        async for raw in conn:
            got.append(str(raw))
            await conn.send(json.dumps({"echo": json.loads(raw), "px": 1.5}))

    url = await server_factory(handler)
    states: list[ConnectionState] = []
    async with make(url, on_state=states.append) as ws:
        await ws.send_json({"sub": "ACME"})
        msg = await asyncio.wait_for(ws.messages().__anext__(), 5)
    assert msg == {"echo": {"sub": "ACME"}, "px": Decimal("1.5")}
    assert got == ['{"sub": "ACME"}']
    assert ws.stats.sent == 1 and ws.stats.received == 1
    assert states[0] is ConnectionState.CONNECTING
    assert ConnectionState.CONNECTED in states
    assert states[-1] is ConnectionState.CLOSED


async def test_reconnects_and_calls_on_connect_again(server_factory) -> None:  # type: ignore[no-untyped-def]
    connections = 0

    async def handler(conn: ServerConnection) -> None:
        nonlocal connections
        connections += 1
        await conn.send(json.dumps({"n": connections}))
        if connections == 1:
            await conn.close()
        else:
            await asyncio.sleep(5)

    url = await server_factory(handler)
    resubscribed = 0

    async def on_connect(t: WebSocketTransport) -> None:
        nonlocal resubscribed
        resubscribed += 1

    ws = make(url, on_connect=on_connect)
    await ws.start()
    it = ws.messages()
    first = await asyncio.wait_for(it.__anext__(), 5)
    second = await asyncio.wait_for(it.__anext__(), 5)
    await ws.close()
    assert (first, second) == ({"n": 1}, {"n": 2})
    assert resubscribed == 2
    assert ws.stats.reconnects == 1


async def test_gives_up_after_max_reconnects() -> None:
    ws = make("ws://127.0.0.1:1", max_reconnects=1)
    await ws.start(wait_connected=False)
    with pytest.raises(TransportError, match="failed after 1 reconnects"):
        async for _ in ws.messages():
            pass
    assert ws.state is ConnectionState.FAILED
    with pytest.raises(TransportError):
        await ws.wait_connected(1)
    await ws.close()


async def test_max_reconnects_exhaustion_fails_start_promptly() -> None:
    # Promptly means before the start timeout, not after it. Windows answers a
    # closed port only after about 2s of SYN retries per attempt, so a fixed
    # wall-clock bound below that would measure the OS, not the transport.
    timeout = 20.0
    ws = make("ws://127.0.0.1:1", max_reconnects=1)
    started = time.monotonic()
    with pytest.raises(TransportError, match="failed after 1 reconnects") as info:
        await ws.start(timeout=timeout)
    assert time.monotonic() - started < timeout / 2
    assert ws.state is ConnectionState.FAILED
    assert isinstance(info.value.__cause__, OSError)
    await ws.close()


async def test_clean_server_close_respects_max_reconnects(server_factory) -> None:  # type: ignore[no-untyped-def]
    async def handler(conn: ServerConnection) -> None:
        await conn.close()

    url = await server_factory(handler)
    ws = make(url, max_reconnects=1)
    await ws.start(wait_connected=False)
    with pytest.raises(TransportError, match="failed after 1 reconnects") as info:
        async for _ in ws.messages():
            pass
    assert "server closed the connection" in str(info.value)
    assert ws.state is ConnectionState.FAILED
    assert ws.stats.connects == 2 and ws.stats.reconnects == 1
    await ws.close()


async def test_undecodable_frame_fails_instead_of_hanging(server_factory) -> None:  # type: ignore[no-untyped-def]
    async def handler(conn: ServerConnection) -> None:
        await conn.send("ping")
        await conn.wait_closed()

    url = await server_factory(handler)
    ws = make(url)
    await ws.start()
    with pytest.raises(TransportError, match="JSONDecodeError") as info:
        async for _ in ws.messages():
            pass
    assert info.value.retryable is False
    assert isinstance(info.value.__cause__, json.JSONDecodeError)
    assert ws.state is ConnectionState.FAILED
    with pytest.raises(TransportError, match="JSONDecodeError"):
        await ws.wait_connected(1)
    await ws.close()


async def test_on_connect_exception_fails_start_with_cause(server_factory) -> None:  # type: ignore[no-untyped-def]
    async def handler(conn: ServerConnection) -> None:
        await conn.wait_closed()

    async def on_connect(t: WebSocketTransport) -> None:
        raise ValueError("bad handshake reply")

    url = await server_factory(handler)
    ws = make(url, on_connect=on_connect)
    started = time.monotonic()
    with pytest.raises(TransportError, match="bad handshake reply") as info:
        await ws.start(timeout=10)
    assert time.monotonic() - started < 2.0
    assert isinstance(info.value.__cause__, ValueError)
    assert ws.state is ConnectionState.FAILED
    await ws.close()


async def test_send_on_dropped_socket_reconnects(server_factory) -> None:  # type: ignore[no-untyped-def]
    connections = 0

    async def handler(conn: ServerConnection) -> None:
        nonlocal connections
        connections += 1
        if connections == 1:
            await conn.close()
            return
        async for raw in conn:
            await conn.send(json.dumps({"ack": json.loads(raw)}))

    url = await server_factory(handler)
    errors: list[TransportError] = []

    async def on_connect(t: WebSocketTransport) -> None:
        if connections == 1:
            assert t._conn is not None
            await t._conn.wait_closed()
        try:
            await t.send_json({"sub": "ACME"})
        except TransportError as exc:
            errors.append(exc)
            raise

    ws = make(url, on_connect=on_connect)
    await ws.start(wait_connected=False)
    msg = await asyncio.wait_for(ws.messages().__anext__(), 5)
    assert msg == {"ack": {"sub": "ACME"}}
    assert ws.state is ConnectionState.CONNECTED
    assert ws.stats.connects == 2 and ws.stats.reconnects == 1
    assert ws.stats.sent == 1
    assert len(errors) == 1 and "closed while sending" in str(errors[0])
    assert errors[0].retryable is True
    await ws.close()


async def test_retryable_on_connect_error_counts_attempts(server_factory) -> None:  # type: ignore[no-untyped-def]
    async def handler(conn: ServerConnection) -> None:
        await conn.wait_closed()

    async def on_connect(t: WebSocketTransport) -> None:
        raise TransportError("token fetch timed out", retryable=True)

    url = await server_factory(handler)
    ws = make(url, on_connect=on_connect, max_reconnects=1)
    with pytest.raises(TransportError, match="failed after 1 reconnects") as info:
        await ws.start(timeout=10)
    assert "token fetch timed out" in str(info.value)
    assert isinstance(info.value.__cause__, TransportError)
    assert ws.stats.connects == 2 and ws.state is ConnectionState.FAILED
    await ws.close()


async def test_non_retryable_error_from_on_connect_is_terminal(server_factory) -> None:  # type: ignore[no-untyped-def]
    async def handler(conn: ServerConnection) -> None:
        await conn.wait_closed()

    err = TransportError("credentials rejected", retryable=False)

    async def on_connect(t: WebSocketTransport) -> None:
        raise err

    url = await server_factory(handler)
    ws = make(url, on_connect=on_connect)
    with pytest.raises(TransportError) as info:
        await ws.start(timeout=10)
    assert info.value is err
    assert ws.stats.connects == 1 and ws.state is ConnectionState.FAILED
    await ws.close()


async def test_wait_connected_times_out() -> None:
    ws = make("ws://127.0.0.1:1")
    with pytest.raises(TransportError, match="not connected within"):
        await ws.start(timeout=0.05)
    assert ws._task is None
    assert ws.state is ConnectionState.CLOSED
    assert not any(t.get_name().startswith("ismet-ws:") for t in asyncio.all_tasks())
    assert [m async for m in ws.messages()] == []
    await ws.close()


async def test_timed_out_start_carries_last_connect_error(server_factory) -> None:  # type: ignore[no-untyped-def]
    url = await server_factory(idle, process_request=reject_with(503))
    ws = make(url)
    with pytest.raises(TransportError, match="not connected within") as info:
        await ws.start(timeout=0.5)
    assert isinstance(info.value.__cause__, InvalidStatus)
    assert info.value.__cause__.response.status_code == 503
    assert ws._task is None and ws.state is ConnectionState.CLOSED


@pytest.mark.parametrize(
    ("status", "exc_type"),
    [(401, AuthError), (403, AuthError), (404, TransportError)],
)
async def test_terminal_handshake_rejection_fails_fast(  # type: ignore[no-untyped-def]
    server_factory, status: int, exc_type: type[Exception]
) -> None:
    url = await server_factory(idle, process_request=reject_with(status))
    ws = make(url)
    started = time.monotonic()
    with pytest.raises(exc_type, match=f"HTTP {status}") as info:
        await ws.start(timeout=10)
    assert time.monotonic() - started < 2.0
    assert url in str(info.value)
    assert isinstance(info.value.__cause__, InvalidStatus)
    assert isinstance(info.value, AuthError | TransportError)
    assert info.value.status == status
    if isinstance(info.value, TransportError):
        assert info.value.retryable is False
    assert ws.state is ConnectionState.FAILED
    assert ws.stats.connects == 0 and ws._task is None
    with pytest.raises(exc_type):
        async for _ in ws.messages():
            pass
    await ws.close()


@pytest.mark.parametrize("status", [429, 503])
async def test_retryable_handshake_rejection_reconnects(  # type: ignore[no-untyped-def]
    server_factory, status: int
) -> None:
    seen: list[int] = []
    url = await server_factory(
        idle, process_request=reject_with(status, times=2, seen=seen)
    )
    ws = make(url)
    await ws.start(timeout=10)
    assert ws.state is ConnectionState.CONNECTED
    assert seen == [status, status]
    assert ws.stats.connects == 1 and ws.stats.reconnects == 1
    await ws.close()


async def test_invalid_uri_fails_fast() -> None:
    ws = make("http://127.0.0.1:1")
    started = time.monotonic()
    with pytest.raises(TransportError, match="invalid URI") as info:
        await ws.start(timeout=10)
    assert time.monotonic() - started < 2.0
    assert isinstance(info.value.__cause__, InvalidURI)
    assert info.value.retryable is False
    assert ws.state is ConnectionState.FAILED and ws._task is None
    await ws.close()


async def test_consumer_attached_before_start_keeps_receiving(server_factory) -> None:  # type: ignore[no-untyped-def]
    async def handler(conn: ServerConnection) -> None:
        await conn.send(json.dumps({"n": 1}))
        await conn.wait_closed()

    url = await server_factory(handler)
    ws = make(url)
    received: list[object] = []

    async def collect() -> None:
        async for msg in ws.messages():
            received.append(msg)

    consumer = asyncio.create_task(collect())
    await asyncio.sleep(0)
    await ws.start(timeout=10)
    for _ in range(50):
        if received:
            break
        await asyncio.sleep(0.02)
    assert received == [{"n": 1}]
    await ws.close()
    await asyncio.wait_for(consumer, 5)
    assert consumer.done() and consumer.exception() is None

    assert [m async for m in ws.messages()] == []
    restarted = asyncio.create_task(collect())
    await asyncio.sleep(0)
    await ws.start(timeout=10)
    for _ in range(50):
        if len(received) == 2:
            break
        await asyncio.sleep(0.02)
    assert received == [{"n": 1}, {"n": 1}]
    await ws.close()
    await asyncio.wait_for(restarted, 5)
    assert restarted.exception() is None


async def test_restart_after_failure_starts_clean(server_factory) -> None:  # type: ignore[no-untyped-def]
    async def handler(conn: ServerConnection) -> None:
        await conn.send(json.dumps({"ok": 1}))
        await conn.wait_closed()

    url = await server_factory(
        handler, process_request=reject_with(401, times=1, seen=[])
    )
    ws = make(url)
    stats = ws.stats
    with pytest.raises(AuthError):
        await ws.start(timeout=10)
    await ws.close()

    await ws.start(timeout=10)
    assert ws.state is ConnectionState.CONNECTED
    assert await asyncio.wait_for(ws.messages().__anext__(), 5) == {"ok": 1}
    assert ws.stats is stats
    assert stats.connects == 1 and stats.received == 1
    await ws.close()

    await ws.start(timeout=10)
    assert await asyncio.wait_for(ws.messages().__anext__(), 5) == {"ok": 1}
    assert ws.stats is stats
    assert stats.connects == 1 and stats.received == 1
    await ws.close()
    assert [m async for m in ws.messages()] == []


async def test_wait_connected_timeout_keeps_stored_failure_cause() -> None:
    ws = make("ws://127.0.0.1:1")
    cause = InvalidURI("ws://127.0.0.1:1", "bad")
    failure = TransportError("terminal", retryable=False)
    failure.__cause__ = cause
    ws._failure = failure
    with pytest.raises(TransportError) as info:
        await ws.wait_connected(0)
    assert info.value is failure
    assert info.value.__cause__ is cause


async def test_send_before_connect_raises() -> None:
    ws = make("ws://127.0.0.1:1")
    with pytest.raises(TransportError, match="not connected"):
        await ws.send_json({})


async def test_backpressure_policies(server_factory) -> None:  # type: ignore[no-untyped-def]
    async def handler(conn: ServerConnection) -> None:
        for i in range(5):
            await conn.send(json.dumps(i))
        await asyncio.sleep(5)

    url = await server_factory(handler)
    for policy, expected in (
        (BackpressurePolicy.DROP_OLDEST, [3, 4]),
        (BackpressurePolicy.DROP_NEWEST, [0, 1]),
    ):
        ws = make(url, queue_size=2, backpressure=policy)
        await ws.start()
        await asyncio.sleep(0.2)
        it = ws.messages()
        got = [await it.__anext__(), await it.__anext__()]
        await ws.close()
        assert got == expected, policy
        assert ws.stats.dropped == 3


async def test_close_is_idempotent_and_ends_messages(server_factory) -> None:  # type: ignore[no-untyped-def]
    async def handler(conn: ServerConnection) -> None:
        await asyncio.sleep(5)

    url = await server_factory(handler)
    ws = make(url)
    await ws.start()
    await ws.close()
    await ws.close()
    assert [m async for m in ws.messages()] == []


def test_sequence_tracker() -> None:
    t = SequenceTracker()
    assert t.classify(10).missing == 0
    assert t.classify(11).missing == 0
    assert t.classify(14).missing == 2
    assert t.classify(13).missing == 0
    assert t.last == 14 and t.gaps == 1


async def test_on_rejected_sees_the_handshake_and_can_end_the_stream() -> None:
    seen: list[tuple[int, bytes, str | None]] = []
    verdict = RateLimited("too much", retry_after=9.0)

    def process_request(conn: ServerConnection, request: Request) -> Response:
        response = conn.respond(429, "slow down")
        response.headers["Retry-After"] = "9"
        return response

    def on_rejected(status, body, headers):  # type: ignore[no-untyped-def]
        seen.append((status, body, headers.get("retry-after")))
        return verdict

    async def handler(conn: ServerConnection) -> None:
        await conn.wait_closed()

    async with serve(
        handler, "127.0.0.1", 0, process_request=process_request
    ) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        ws = WebSocketTransport(f"ws://127.0.0.1:{port}", on_rejected=on_rejected)
        with pytest.raises(RateLimited) as caught:
            await ws.start()
    assert caught.value is verdict
    assert len(seen) == 1 and seen[0][0] == 429 and seen[0][2] == "9"
    assert b"slow down" in seen[0][1]


async def test_a_failing_on_rejected_leaves_the_normal_handling_in_place(
    caplog: pytest.LogCaptureFixture,
) -> None:
    attempts = 0

    def process_request(conn: ServerConnection, request: Request) -> Response | None:
        nonlocal attempts
        attempts += 1
        return conn.respond(404, "gone")

    def on_rejected(status, body, headers):  # type: ignore[no-untyped-def]
        raise RuntimeError("bug in the hook")

    async def handler(conn: ServerConnection) -> None:
        await conn.wait_closed()

    async with serve(
        handler, "127.0.0.1", 0, process_request=process_request
    ) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        ws = WebSocketTransport(f"ws://127.0.0.1:{port}", on_rejected=on_rejected)
        with pytest.raises(TransportError, match="404"):
            await ws.start()
    assert attempts == 1 and "on_rejected" in caplog.text


async def _wait_for(condition) -> None:  # type: ignore[no-untyped-def]
    """Wait for a condition another task will make true; the bound only fails a bug."""
    deadline = time.monotonic() + 10
    while not condition():
        assert time.monotonic() < deadline, "the condition never became true"
        await asyncio.sleep(0.005)


async def test_the_reconnect_log_states_the_wait_a_handshake_was_told(
    server_factory, caplog: pytest.LogCaptureFixture
) -> None:  # type: ignore[no-untyped-def]
    def process_request(conn: ServerConnection, request: Request) -> Response:
        response = conn.respond(429, "slow down")
        response.headers["Retry-After"] = "300"
        return response

    url = await server_factory(idle, process_request=process_request)
    ws = make(url)
    with caplog.at_level(logging.WARNING, logger="ismet.transport.ws"):
        await ws.start(wait_connected=False)
        await _wait_for(lambda: "in 300.0s" in caplog.text)
    await ws.close()


async def test_a_handshake_date_form_retry_after_uses_the_injected_clock(
    server_factory, caplog: pytest.LogCaptureFixture
) -> None:  # type: ignore[no-untyped-def]
    def process_request(conn: ServerConnection, request: Request) -> Response:
        response = conn.respond(429, "slow down")
        del response.headers["Date"]
        response.headers["Retry-After"] = "Sun, 01 Nov 2026 23:00:00 GMT"
        return response

    url = await server_factory(idle, process_request=process_request)
    clock = ManualClock(datetime(2026, 11, 1, 22, 55, tzinfo=timezone.utc))
    ws = make(url, clock=clock)
    with caplog.at_level(logging.WARNING, logger="ismet.transport.ws"):
        await ws.start(wait_connected=False)
        await _wait_for(lambda: "in 300.0s" in caplog.text)
    await ws.close()


async def test_a_control_frame_eviction_does_not_swallow_a_newest_drop() -> None:
    ws = make(
        "ws://127.0.0.1:1", queue_size=3, backpressure=BackpressurePolicy.DROP_NEWEST
    )

    async def arrive(name: str, *, is_data: bool = True) -> None:
        await ws._put((name, ws.stats.dropped), is_data=is_data)

    for name in "ABCD":  # D finds the queue full and is dropped.
        await arrive(name)
    await arrive("control", is_data=False)  # Evicts A.
    assert ws.stats.dropped == 2 and ws.stats.evicted == 1

    told: dict[str, bool] = {}
    for _ in range(3):
        name, stamped = ws._queue.get_nowait()
        told[name] = ws.lost_before(stamped)
    await arrive("E")
    name, stamped = ws._queue.get_nowait()
    told[name] = ws.lost_before(stamped)

    assert told["B"] is True  # A was evicted ahead of it.
    assert told["C"] is False  # Nothing was lost between B and C.
    assert told["control"] or told["E"]  # D was lost, and something must say so.
