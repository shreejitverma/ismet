"""The client's gap policy over real recorded feeds.

The oracle is the recordings: these are uninterrupted live captures, so nothing was
lost, although their sequence numbers jump.
"""

from __future__ import annotations

from itertools import pairwise
from typing import Any

import pytest

from ismet.client import IsmetClient
from ismet.errors import SequenceGap
from ismet.marketdata import GapPolicy
from ismet.models import QualityFlag
from ismet.providers.binance import BinanceProvider
from ismet.providers.coinbase import CoinbaseProvider
from ismet.testing.replay import WsReplayServer

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
    pytest.param(_binance, "binance", "trade", "BTCUSDT@BNCX", id="binance"),
    pytest.param(_coinbase, "coinbase", "matches", "BTC-USD@ZCBS", id="coinbase"),
]


@pytest.mark.parametrize(("build", "vendor", "name", "symbol"), TRADE_FEEDS)
async def test_trades_really_lost_across_a_reconnect_follow_the_policy(
    build: Any, vendor: str, name: str, symbol: str
) -> None:
    plan = [slice(0, 5), slice(8, 12)]
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
    assert [t.sequence for t in flagged] == gaps
    assert len(gaps) == 1


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
