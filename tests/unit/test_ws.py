from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from decimal import Decimal

import pytest
from websockets.asyncio.server import Server, ServerConnection, serve

from ismet.errors import TransportError
from ismet.transport.backoff import ExponentialBackoff
from ismet.transport.ws import (
    BackpressurePolicy,
    ConnectionState,
    SequenceTracker,
    WebSocketTransport,
)

Handler = Callable[[ServerConnection], Awaitable[None]]
FAST = ExponentialBackoff(base=0.01, maximum=0.02, jitter=False)


@pytest.fixture
async def server_factory() -> AsyncIterator[Callable[[Handler], Awaitable[str]]]:
    servers: list[Server] = []

    async def start(handler: Handler) -> str:
        srv = await serve(handler, "127.0.0.1", 0)
        servers.append(srv)
        port = next(iter(srv.sockets)).getsockname()[1]
        return f"ws://127.0.0.1:{port}"

    yield start
    for srv in servers:
        srv.close()
        await srv.wait_closed()


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
    ws = make("ws://127.0.0.1:1", max_reconnects=1)
    started = time.monotonic()
    with pytest.raises(TransportError, match="failed after 1 reconnects") as info:
        await ws.start(timeout=10)
    assert time.monotonic() - started < 2.0
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


async def test_send_on_closed_socket_raises_transport_error(server_factory) -> None:  # type: ignore[no-untyped-def]
    async def handler(conn: ServerConnection) -> None:
        await conn.close()

    url = await server_factory(handler)
    errors: list[TransportError] = []

    async def on_connect(t: WebSocketTransport) -> None:
        assert t._conn is not None
        await t._conn.wait_closed()
        try:
            await t.send_json({"sub": "ACME"})
        except TransportError as exc:
            errors.append(exc)

    ws = make(url, on_connect=on_connect, max_reconnects=0)
    await ws.start(wait_connected=False)
    with pytest.raises(TransportError, match="failed after 0 reconnects"):
        async for _ in ws.messages():
            pass
    assert len(errors) == 1 and "closed while sending" in str(errors[0])
    assert ws.stats.sent == 0
    await ws.close()


async def test_wait_connected_times_out() -> None:
    ws = make("ws://127.0.0.1:1")
    with pytest.raises(TransportError, match="not connected within"):
        await ws.start(timeout=0.05)
    await ws.close()


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
    assert t.observe(10) == 0
    assert t.observe(11) == 0
    assert t.observe(14) == 2
    assert t.observe(13) == 0
    assert t.last == 14 and t.gaps == 1
