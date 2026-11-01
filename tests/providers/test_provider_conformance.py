"""``run_conformance`` against replayed real traffic for both providers."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ismet.capabilities import Capability
from ismet.models import Symbol
from ismet.providers.binance import BinanceProvider
from ismet.providers.coinbase import CoinbaseProvider
from ismet.testing import assert_conformant, run_conformance
from ismet.testing.replay import UnrecordedRequestError, WsReplayServer

from ._support import manifest, replay, session


def _freeze_conformance_clock(monkeypatch: pytest.MonkeyPatch, provider: str) -> None:
    """Conformance asks for bars up to now, but the replay only knows record time."""
    end = manifest(provider, "conformance_end")

    class Frozen(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:  # type: ignore[override]  # noqa: ARG003
            return end

    monkeypatch.setattr("ismet.testing.conformance.datetime", Frozen)


async def test_binance_is_conformant(monkeypatch: pytest.MonkeyPatch) -> None:
    _freeze_conformance_clock(monkeypatch, "binance")
    sessions = [session("binance", "book_ticker"), session("binance", "trade")]
    async with WsReplayServer(sessions) as server:
        provider = BinanceProvider(
            http_transport=replay("binance", "conformance"), ws_url=server.url
        )
        async with provider:
            report = await run_conformance(
                provider, symbol=Symbol(venue="BNCX", ticker="BTCUSDT")
            )
    assert_conformant(report)
    assert server.unexpected == []
    _assert_all_capabilities_ran(report)


async def test_coinbase_is_conformant(monkeypatch: pytest.MonkeyPatch) -> None:
    _freeze_conformance_clock(monkeypatch, "coinbase")
    sessions = [session("coinbase", "ticker"), session("coinbase", "matches")]
    async with WsReplayServer(sessions) as server:
        provider = CoinbaseProvider(
            http_transport=replay("coinbase", "conformance"), ws_url=server.url
        )
        async with provider:
            report = await run_conformance(
                provider, symbol=Symbol(venue="ZCBS", ticker="BTC-USD")
            )
    assert_conformant(report)
    assert server.unexpected == []
    _assert_all_capabilities_ran(report)


def _assert_all_capabilities_ran(report) -> None:  # type: ignore[no-untyped-def]
    ran = {(c.capability, c.name) for c in report.checks}
    assert {
        (Capability.REFERENCE_DATA, "instrument_fields"),
        (Capability.REFERENCE_DATA, "search_returns_symbol"),
        (Capability.MARKET_DATA, "quote_invariants"),
        (Capability.MARKET_DATA, "order_book_invariants"),
        (Capability.HISTORICAL, "bars_invariants"),
        (Capability.STREAMING, "stream_quotes"),
        (Capability.STREAMING, "stream_trades"),
    } <= ran
    assert (Capability.TRADING, "not_supported_raises") in ran
    assert (Capability.ACCOUNT, "not_supported_raises") in ran


async def test_conformance_discovers_the_symbol_through_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _freeze_conformance_clock(monkeypatch, "binance")
    sessions = [session("binance", "book_ticker"), session("binance", "trade")]
    async with WsReplayServer(sessions) as server:
        provider = BinanceProvider(
            http_transport=replay("binance", "conformance"), ws_url=server.url
        )
        async with provider:
            report = await run_conformance(provider, search_query="BTCUSDT")
    assert_conformant(report)


async def test_replay_really_guards_the_network() -> None:
    provider = BinanceProvider(http_transport=replay("binance", "conformance"))
    async with provider:
        with pytest.raises(UnrecordedRequestError):
            await provider.quote(Symbol(venue="BNCX", ticker="ETHBTC"))
