"""The client's gap policy over real recorded feeds.

The oracle is the recordings: these are uninterrupted live captures, so nothing was
lost, although their sequence numbers jump.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from itertools import pairwise
from typing import Any

import pytest
from websockets.asyncio.server import ServerConnection, serve
from websockets.http11 import Request
from websockets.http11 import Response as WsResponse

from ismet.client import IsmetClient
from ismet.errors import SequenceGap, TransportError, VenueError
from ismet.marketdata import GapPolicy
from ismet.models import QualityFlag, Quote, Symbol
from ismet.providers.binance import BinanceProvider
from ismet.providers.coinbase import CoinbaseProvider
from ismet.testing.replay import WsReplayServer
from ismet.transport.ws import BackpressurePolicy, WebSocketTransport

from ._support import FAST_BACKOFF, frames, replay, session, take


def _binance(server: WsReplayServer) -> BinanceProvider:
    return BinanceProvider(
        http_transport=replay("binance", "conformance"),
        ws_url=server.url,
        ws_backoff=FAST_BACKOFF,
    )


def _coinbase(server: WsReplayServer) -> CoinbaseProvider:
    return CoinbaseProvider(ws_url=server.url, ws_backoff=FAST_BACKOFF)


FEEDS = [
    pytest.param(_binance, "binance", "book_ticker", "u", "BTCUSDT@BNCX", id="binance"),
    pytest.param(_coinbase, "coinbase", "ticker", "sequence", "BTC-USD@ZCBS", id="cb"),
]


@pytest.mark.parametrize(("build", "vendor", "name", "field", "symbol"), FEEDS)
async def test_a_sparse_quote_feed_has_no_gaps_and_does_not_die_on_raise(
    build: Any, vendor: str, name: str, field: str, symbol: str
) -> None:
    recorded = frames(vendor, name)
    numbers = [f[field] for f in recorded]
    assert any(b - a != 1 for a, b in pairwise(numbers))
    gaps: list[int] = []
    async with (
        WsReplayServer([session(vendor, name)]) as server,
        IsmetClient([build(server)]) as client,
    ):
        quotes = await take(
            client.stream_quotes(
                [symbol],
                gap_policy=GapPolicy.RAISE,
                on_gap=lambda _, o: gaps.append(o.sequence),
            ),
            len(recorded),
        )
    assert [q.sequence for q in quotes] == numbers
    assert all(q.quality == frozenset() for q in quotes)
    assert gaps == []


async def test_coinbase_matches_with_a_sparse_sequence_have_no_gaps() -> None:
    recorded = frames("coinbase", "matches")
    numbers = [f["sequence"] for f in recorded[1:]]
    assert any(b - a != 1 for a, b in pairwise(numbers))
    async with (
        WsReplayServer([session("coinbase", "matches")]) as server,
        IsmetClient([_coinbase(server)]) as client,
    ):
        trades = await take(
            client.stream_trades(["BTC-USD@ZCBS"], gap_policy=GapPolicy.RAISE),
            len(numbers),
        )
    assert [t.sequence for t in trades] == numbers


TRADE_FEEDS = [
    pytest.param(_binance, "binance", "trade", "t", "BTCUSDT@BNCX", id="binance"),
    pytest.param(
        _coinbase, "coinbase", "matches", "sequence", "BTC-USD@ZCBS", id="coinbase"
    ),
]


@pytest.mark.parametrize(("build", "vendor", "name", "field", "symbol"), TRADE_FEEDS)
async def test_trades_really_lost_across_a_reconnect_follow_the_policy(
    build: Any, vendor: str, name: str, field: str, symbol: str
) -> None:
    plan = [slice(0, 5), slice(8, 12)]
    first_after_the_hole = frames(vendor, name)[8][field]
    async with (
        WsReplayServer([session(vendor, name)], plan=plan) as server,
        IsmetClient([build(server)]) as client,
    ):
        with pytest.raises(SequenceGap):
            await take(client.stream_trades([symbol], gap_policy=GapPolicy.RAISE), 8)
    gaps: list[int | None] = []
    async with (
        WsReplayServer([session(vendor, name)], plan=plan) as server,
        IsmetClient([build(server)]) as client,
    ):
        trades = await take(
            client.stream_trades([symbol], on_gap=lambda _, o: gaps.append(o.sequence)),
            7,
        )
    flagged = [t for t in trades if QualityFlag.AFTER_GAP in t.quality]
    assert [t.sequence for t in flagged] == [first_after_the_hole]
    assert gaps == [first_after_the_hole]


async def test_the_first_quote_after_a_reconnect_is_the_gap_on_a_sparse_feed() -> None:
    plan = [slice(0, 4), slice(4, 8)]
    gaps: list[int | None] = []
    wanted = frames("coinbase", "ticker")
    async with (
        WsReplayServer([session("coinbase", "ticker")], plan=plan) as server,
        IsmetClient([_coinbase(server)]) as client,
    ):
        await take(
            client.stream_quotes(
                ["BTC-USD@ZCBS"], on_gap=lambda _, o: gaps.append(o.sequence)
            ),
            8,
        )
    assert gaps == [wanted[4]["sequence"]]


DROP_FEEDS = [
    pytest.param(
        lambda url, policy: BinanceProvider(
            http_transport=replay("binance", "conformance"),
            ws_url=url,
            ws_queue_size=2,
            ws_backpressure=policy,
        ),
        "binance",
        "book_ticker",
        "u",
        Symbol(venue="BNCX", ticker="BTCUSDT"),
        id="binance",
    ),
    pytest.param(
        lambda url, policy: CoinbaseProvider(
            ws_url=url, ws_queue_size=2, ws_backpressure=policy
        ),
        "coinbase",
        "ticker",
        "sequence",
        Symbol(venue="ZCBS", ticker="BTC-USD"),
        id="coinbase",
    ),
]


async def _slow_start(provider: Any, symbol: Symbol, quotes: list[Quote]) -> None:
    async for quote in provider.stream_quotes([symbol]):
        quotes.append(quote)
        if len(quotes) == 1:
            await asyncio.sleep(0.2)


@pytest.mark.parametrize(
    "policy", [BackpressurePolicy.DROP_NEWEST, BackpressurePolicy.DROP_OLDEST]
)
@pytest.mark.parametrize(("build", "vendor", "name", "field", "symbol"), DROP_FEEDS)
async def test_the_first_quote_after_dropped_frames_carries_the_gap_flag(
    build: Any,
    vendor: str,
    name: str,
    field: str,
    symbol: Symbol,
    policy: BackpressurePolicy,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The oracle is the recording: any frame it holds and the stream lost is a hole."""
    recorded = session(vendor, name).frames
    numbers = [f[field] for f in frames(vendor, name)]
    burst = len(recorded) // 2
    second_burst = asyncio.Event()

    async def handler(conn: ServerConnection) -> None:
        await conn.recv()
        for raw in recorded[:burst]:
            await conn.send(raw)
        await second_burst.wait()
        for raw in recorded[burst:]:
            await conn.send(raw)
        await conn.wait_closed()

    quotes: list[Quote] = []
    async with serve(handler, "127.0.0.1", 0) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        async with build(f"ws://127.0.0.1:{port}", policy) as provider:
            consumer = asyncio.create_task(_slow_start(provider, symbol, quotes))
            await asyncio.sleep(0.5)
            second_burst.set()
            await asyncio.sleep(0.3)
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
    holes = 0
    for before, after in pairwise(quotes):
        if numbers.index(after.sequence) > numbers.index(before.sequence) + 1:
            holes += 1
            assert QualityFlag.AFTER_GAP in after.quality, after.sequence
    assert holes >= 1
    assert "dropped a message" in caplog.text


async def test_a_venue_refusal_survives_a_full_dropping_queue() -> None:
    data = session("binance", "book_ticker").frames
    refusal = json.dumps({"code": 2, "msg": "refused"})

    async def handler(conn: ServerConnection) -> None:
        await conn.recv()
        for raw in data[:10]:
            await conn.send(raw)
        await conn.send(refusal)
        await conn.wait_closed()

    async with serve(handler, "127.0.0.1", 0) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        provider = BinanceProvider(
            http_transport=replay("binance", "conformance"),
            ws_url=f"ws://127.0.0.1:{port}",
            ws_queue_size=2,
            ws_backpressure=BackpressurePolicy.DROP_NEWEST,
        )
        async with provider:
            stream = provider.stream_quotes([Symbol(venue="BNCX", ticker="BTCUSDT")])
            await asyncio.wait_for(anext(stream), 10)
            await asyncio.sleep(0.2)
            with pytest.raises(VenueError, match="refused"):
                while True:
                    await asyncio.wait_for(anext(stream), 2)


async def test_a_handshake_429_waits_out_its_retry_after() -> None:
    attempts: list[float] = []
    # Only this test's client asks for this path: a client left reconnecting by
    # another test can land on the reused port, and must not count here.
    path = f"/t-{uuid.uuid4().hex}"

    def process_request(conn: ServerConnection, request: Request) -> WsResponse | None:
        if request.path != path:
            return conn.respond(404, "not this test")
        attempts.append(time.monotonic())
        if len(attempts) > 1:
            return None
        response = conn.respond(429, "slow down")
        response.headers["Retry-After"] = "0.4"
        return response

    async def handler(conn: ServerConnection) -> None:
        await conn.wait_closed()

    async with serve(
        handler, "127.0.0.1", 0, process_request=process_request
    ) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        ws = WebSocketTransport(f"ws://127.0.0.1:{port}{path}", backoff=FAST_BACKOFF)
        await ws.start()
        await ws.close()
    assert len(attempts) == 2
    assert attempts[1] - attempts[0] >= 0.4


async def test_before_connect_runs_before_every_handshake_even_a_refused_one() -> None:
    handshakes = 0
    charged: list[int] = []

    def process_request(conn: ServerConnection, request: Request) -> WsResponse:
        nonlocal handshakes
        handshakes += 1
        return conn.respond(503, "down")

    async def before_connect() -> None:
        charged.append(handshakes)

    async def handler(conn: ServerConnection) -> None:
        await conn.wait_closed()

    async with serve(
        handler, "127.0.0.1", 0, process_request=process_request
    ) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        ws = WebSocketTransport(
            f"ws://127.0.0.1:{port}",
            backoff=FAST_BACKOFF,
            before_connect=before_connect,
            max_reconnects=2,
        )
        with pytest.raises(TransportError):
            await ws.start()
    assert charged == [0, 1, 2]
