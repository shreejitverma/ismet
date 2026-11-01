"""Adapter behaviour through replay. No test here reaches the network."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from ismet.capabilities import Capability
from ismet.config import ProviderSettings
from ismet.errors import RateLimited, TransportError, ValidationError, VenueError
from ismet.models import Interval, QualityFlag, Symbol
from ismet.providers.coinbase import (
    RATE_LIMITS,
    REST_URL,
    WS_URL,
    CoinbaseProvider,
)
from ismet.testing.replay import WsReplayServer
from ismet.transport.backoff import NO_RETRY, ExponentialBackoff, RetryPolicy
from ismet.transport.clock import ManualClock

from ._support import FAST_BACKOFF, frames, manifest, replay, session, take

BTC = Symbol(venue="ZCBS", ticker="BTC-USD")
UTC = timezone.utc
ISO = "%Y-%m-%dT%H:%M:%SZ"


def _provider(cassette: str = "conformance", **options) -> CoinbaseProvider:  # type: ignore[no-untyped-def]
    return CoinbaseProvider(http_transport=replay("coinbase", cassette), **options)


def _requests(provider: CoinbaseProvider) -> list[str]:
    return [url for _, url in provider._http._httpx_transport.requests]  # type: ignore[union-attr]


def _query(url: str) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}


def test_declares_exactly_the_market_data_capabilities() -> None:
    provider = CoinbaseProvider()
    assert provider.capabilities() == {
        Capability.MARKET_DATA,
        Capability.HISTORICAL,
        Capability.STREAMING,
        Capability.REFERENCE_DATA,
    }
    assert provider.venues == {"ZCBS"}


def test_default_hosts_and_settings() -> None:
    assert REST_URL == "https://api.exchange.coinbase.com"
    assert WS_URL == "wss://ws-feed.exchange.coinbase.com"
    settings = ProviderSettings(
        name="coinbase", options={"rest_url": "https://a.example", "ws_url": "wss://b"}
    )
    provider = CoinbaseProvider.from_settings(settings)
    assert provider._http.base_url == "https://a.example"
    assert provider._ws_url == "wss://b"


def test_rate_limits_match_the_documentation() -> None:
    assert RATE_LIMITS["public"].rate == 10
    assert RATE_LIMITS["public"].capacity == 15
    assert RATE_LIMITS["ws_subscribe"].rate == 8
    assert RATE_LIMITS["ws_subscribe"].capacity == 20


async def test_requests_are_charged_to_the_public_bucket() -> None:
    clock = ManualClock()
    async with _provider(clock=clock) as provider:
        bucket = provider._limiter.bucket("public")
        assert bucket is not None
        await provider.quote(BTC)
        assert bucket.available == 14


async def test_quote_uses_level_one_and_spends_no_reference_request() -> None:
    async with _provider() as provider:
        quote = await provider.quote(BTC)
    assert quote.currency == "USD"
    assert quote.bid is not None
    assert quote.exchange_ts_ns is not None
    assert [urlsplit(u).path for u in _requests(provider)] == ["/products/BTC-USD/book"]
    assert _query(_requests(provider)[0]) == {"level": "1"}


async def test_order_book_uses_level_two_and_slices() -> None:
    async with _provider() as provider:
        book = await provider.order_book(BTC, 5)
    assert len(book.bids) == len(book.asks) == 5
    assert _query(_requests(provider)[0]) == {"level": "2"}


async def test_depth_one_avoids_the_whole_book() -> None:
    async with _provider() as provider:
        book = await provider.order_book(BTC, 1)
    assert len(book.bids) == 1
    assert _query(_requests(provider)[0]) == {"level": "1"}


async def test_refusals_happen_before_any_request() -> None:
    async with _provider() as provider:
        with pytest.raises(ValidationError, match="depth"):
            await provider.order_book(BTC, 0)
        with pytest.raises(ValidationError, match="ZCBS"):
            await provider.quote(Symbol(venue="BNCX", ticker="BTC-USD"))
        with pytest.raises(ValidationError, match="upper case"):
            await provider.quote(Symbol(venue="ZCBS", ticker="btc-usd"))
    assert _requests(provider) == []


async def test_ticker_cannot_escape_the_url_path() -> None:
    evil = Symbol(venue="ZCBS", ticker="A/../../X")
    async with _provider(retry_policy=NO_RETRY) as provider:
        with pytest.raises(AssertionError, match="no recording"):
            await provider.quote(evil)
    assert "A%2F..%2F..%2FX" in _requests(provider)[0]


async def test_bars_paginate_in_aligned_windows_ascending() -> None:
    start = manifest("coinbase", "bars_paged_start")
    end = manifest("coinbase", "bars_paged_end")
    async with _provider("bars_paged", candles_per_request=3) as provider:
        bars = await provider.bars(BTC, Interval.M1, start, end)
    stamps = [b.exchange_ts for b in bars]
    assert stamps == sorted(set(stamps))
    assert all(start <= ts < end for ts in stamps)
    pages = [_query(u) for u in _requests(provider)]
    minute = timedelta(minutes=1)
    assert [(p["start"], p["end"]) for p in pages] == [
        ((start + i * minute).strftime(ISO), (start + j * minute).strftime(ISO))
        for i, j in ((0, 2), (3, 5), (6, 6))
    ]
    assert all(p["granularity"] == "60" for p in pages)


async def test_bars_cross_a_real_300_candle_page_boundary() -> None:
    start = manifest("coinbase", "bars_full_pages_start")
    end = manifest("coinbase", "conformance_end")
    async with _provider("bars_full_pages") as provider:
        bars = await provider.bars(BTC, Interval.M1, start, end)
    pages = [_query(u) for u in _requests(provider)]
    assert len(pages) == 2
    first = datetime.strptime(pages[0]["start"], ISO).replace(tzinfo=UTC)
    last = datetime.strptime(pages[0]["end"], ISO).replace(tzinfo=UTC)
    assert last - first == timedelta(minutes=299)
    second = datetime.strptime(pages[1]["start"], ISO).replace(tzinfo=UTC)
    assert second - last == timedelta(minutes=1)
    stamps = [b.exchange_ts for b in bars]
    assert stamps == sorted(set(stamps))
    assert stamps[0] >= start
    assert stamps[-1] < end
    assert any(ts > last for ts in stamps), "the second page must contribute bars"
    assert any(ts <= last for ts in stamps), "the first page must contribute bars"


async def test_an_unaligned_range_asks_for_aligned_buckets_and_excludes_end() -> None:
    start = manifest("coinbase", "bars_paged_start")
    end = manifest("coinbase", "bars_paged_end")
    almost_a_minute = timedelta(seconds=59, microseconds=999_000)
    async with _provider("bars_paged", candles_per_request=3) as provider:
        bars = await provider.bars(BTC, Interval.M1, start - almost_a_minute, end)
    assert all(b.exchange_ts >= start for b in bars)
    assert all(b.exchange_ts < end for b in bars)
    assert _query(_requests(provider)[0])["start"] == start.strftime(ISO)


async def test_daily_bars_and_open_bucket() -> None:
    async with _provider("bars_daily") as provider:
        daily = await provider.bars(
            BTC,
            Interval.D1,
            manifest("coinbase", "bars_daily_start"),
            manifest("coinbase", "bars_daily_end"),
        )
    assert all(b.session_date == b.exchange_ts.date() for b in daily)
    clock = ManualClock(manifest("coinbase", "recorded_at"))
    async with _provider("bars_open", clock=clock) as provider:
        recent = await provider.bars(
            BTC,
            Interval.M1,
            manifest("coinbase", "bars_open_start"),
            manifest("coinbase", "bars_open_end"),
        )
    assert recent
    assert all(b.exchange_ts + timedelta(minutes=1) <= clock.now() for b in recent)


@pytest.mark.parametrize(
    "interval",
    [Interval.S1, Interval.M30, Interval.H4, Interval.W1, Interval.MO1],
)
async def test_unsupported_intervals_raise_before_any_request(
    interval: Interval,
) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    async with _provider() as provider:
        with pytest.raises(ValidationError, match=r"no .* candles"):
            await provider.bars(BTC, interval, now, now + timedelta(days=1))
    assert _requests(provider) == []


async def test_range_validation() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    async with _provider() as provider:
        with pytest.raises(ValidationError, match="timezone-aware"):
            await provider.bars(BTC, Interval.M1, datetime(2026, 1, 1), now)
        with pytest.raises(ValidationError, match="before end"):
            await provider.bars(BTC, Interval.M1, now, now)
        with pytest.raises(ValidationError, match="max 100000"):
            await provider.bars(BTC, Interval.M1, now, now + timedelta(days=100))
    assert _requests(provider) == []
    for bad in (0, 301):
        with pytest.raises(ValidationError, match="candles_per_request"):
            CoinbaseProvider(candles_per_request=bad)


async def test_search_ranks_and_caches() -> None:
    clock = ManualClock()
    async with _provider(clock=clock, search_cache_seconds=60) as provider:
        exact = await provider.search("btc-usd", venue="ZCBS", limit=5)
        assert exact[0].symbol == BTC
        assert await provider.search("BTC", venue="XNAS") == []
        assert await provider.search("BTC", limit=0) == []
        by_asset = await provider.search("BTC")
        assert by_asset[-1].symbol.ticker == "ZRX-BTC"
        assert by_asset[-1].active is False
        assert sum(urlsplit(u).path == "/products" for u in _requests(provider)) == 1
        clock.advance(61)
        await provider.search("BTC")
        assert sum(urlsplit(u).path == "/products" for u in _requests(provider)) == 2


async def test_search_keeps_a_single_letter_base() -> None:
    async with _provider() as provider:
        hits = await provider.search("T-USD")
    assert hits[0].symbol.ticker == "T-USD"
    assert hits[0].base_asset == "T"


async def test_recorded_not_found_is_a_venue_error_without_a_code() -> None:
    bad = Symbol(venue="ZCBS", ticker="NOPE-USD")
    async with _provider("errors", retry_policy=NO_RETRY) as provider:
        with pytest.raises(VenueError) as caught:
            await provider.quote(bad)
    assert caught.value.status == 404
    assert caught.value.code is None
    assert caught.value.payload == {"message": "NotFound"}


async def test_recorded_candle_rejections_are_venue_errors() -> None:
    async with _provider("errors", retry_policy=NO_RETRY) as provider:
        with pytest.raises(VenueError, match="Unsupported granularity"):
            await provider._get("/products/BTC-USD/candles", {"granularity": 7})
        with pytest.raises(VenueError, match="exceeds 300"):
            await provider._get(
                "/products/BTC-USD/candles",
                {
                    "granularity": 60,
                    "start": "2026-10-31T00:00:00Z",
                    "end": "2026-10-31T06:00:00Z",
                },
            )


# The rate limit page documents the status only, so these bodies are empty.
async def test_429_is_rate_limited_after_policy_retries() -> None:
    seen: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(429)

    policy = RetryPolicy(
        max_attempts=2, backoff=ExponentialBackoff(base=0, jitter=False)
    )
    provider = CoinbaseProvider(
        http_transport=httpx.MockTransport(respond), retry_policy=policy
    )
    async with provider:
        with pytest.raises(RateLimited):
            await provider.quote(BTC)
    assert len(seen) == 2


async def test_5xx_is_a_transport_error() -> None:
    provider = CoinbaseProvider(
        http_transport=httpx.MockTransport(lambda _: httpx.Response(502)),
        retry_policy=NO_RETRY,
    )
    async with provider:
        with pytest.raises(TransportError) as caught:
            await provider.quote(BTC)
    assert caught.value.status == 502


def _stream_provider(server: WsReplayServer, **options) -> CoinbaseProvider:  # type: ignore[no-untyped-def]
    return CoinbaseProvider(ws_url=server.url, ws_backoff=FAST_BACKOFF, **options)


async def test_stream_quotes_subscribes_and_normalizes() -> None:
    async with WsReplayServer([session("coinbase", "ticker")]) as server:
        provider = _stream_provider(server)
        async with provider:
            quotes = await take(provider.stream_quotes([BTC]), 10)
    assert server.connections[0].received[0] == {
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["ticker"],
    }
    wanted = frames("coinbase", "ticker")[:10]
    assert [q.sequence for q in quotes] == [f["sequence"] for f in wanted]
    assert all(q.currency == "USD" for q in quotes)
    assert all(q.quality == frozenset() for q in quotes), "sparse sequence is normal"


async def test_last_match_on_first_connect_sets_position_without_an_event() -> None:
    async with WsReplayServer([session("coinbase", "matches")]) as server:
        provider = _stream_provider(server)
        async with provider:
            trades = await take(provider.stream_trades([BTC]), 5)
    received = frames("coinbase", "matches")
    assert received[0]["type"] == "last_match"
    assert [t.trade_id for t in trades] == [str(f["trade_id"]) for f in received[1:6]]
    assert all(t.quality == frozenset() for t in trades)


async def test_stream_trades_routes_each_frame_to_its_symbol() -> None:
    recorded = session("coinbase", "matches_multi")
    eth = Symbol(venue="ZCBS", ticker="ETH-USD")
    data = [f for f in frames("coinbase", "matches_multi") if f["type"] == "match"]
    async with WsReplayServer([recorded]) as server:
        provider = _stream_provider(server)
        async with provider:
            trades = await take(provider.stream_trades([BTC, eth]), len(data))
    assert server.connections[0].received[0]["product_ids"] == ["BTC-USD", "ETH-USD"]
    assert [(t.symbol.ticker, t.trade_id) for t in trades] == [
        (f["product_id"], str(f["trade_id"])) for f in data
    ]


async def test_reconnect_resubscribes_and_loses_nothing_when_nothing_was_missed() -> (
    None
):
    plan = [slice(0, 5), slice(5, 12)]
    async with WsReplayServer([session("coinbase", "matches")], plan=plan) as server:
        provider = _stream_provider(server)
        async with provider:
            trades = await take(provider.stream_trades([BTC]), 11)
    subscribe = {
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["matches"],
    }
    assert [c.received for c in server.connections] == [[subscribe], [subscribe]]
    wanted = frames("coinbase", "matches")[1:12]
    assert [t.trade_id for t in trades] == [str(f["trade_id"]) for f in wanted]
    assert all(t.quality == frozenset() for t in trades)


async def test_a_trade_repeated_after_reconnect_is_delivered_once() -> None:
    plan = [slice(0, 5), slice(4, 9)]
    async with WsReplayServer([session("coinbase", "matches")], plan=plan) as server:
        provider = _stream_provider(server)
        async with provider:
            trades = await take(provider.stream_trades([BTC]), 8)
    wanted = frames("coinbase", "matches")[1:9]
    assert [t.trade_id for t in trades] == [str(f["trade_id"]) for f in wanted]


async def test_trades_missed_across_a_reconnect_flag_the_next_trade() -> None:
    plan = [slice(0, 5), slice(8, 12)]
    async with WsReplayServer([session("coinbase", "matches")], plan=plan) as server:
        provider = _stream_provider(server)
        async with provider:
            trades = await take(provider.stream_trades([BTC]), 8)
    assert trades[4].quality == {QualityFlag.AFTER_GAP}
    assert all(t.quality == frozenset() for t in trades[:4] + trades[5:])


async def test_first_quote_after_reconnect_is_flagged() -> None:
    plan = [slice(0, 4), slice(4, 8)]
    async with WsReplayServer([session("coinbase", "ticker")], plan=plan) as server:
        provider = _stream_provider(server)
        async with provider:
            quotes = await take(provider.stream_quotes([BTC]), 8)
    assert [q.quality for q in quotes] == [
        *[frozenset()] * 4,
        frozenset({QualityFlag.AFTER_GAP}),
        *[frozenset()] * 3,
    ]


async def test_recorded_subscription_rejection_ends_the_stream() -> None:
    bad = Symbol(venue="ZCBS", ticker="NOPE-USD")
    async with WsReplayServer([session("coinbase", "rejected")]) as server:
        provider = _stream_provider(server)
        async with provider:
            with pytest.raises(VenueError, match="not a valid product"):
                await take(provider.stream_trades([bad]), 1)


async def test_stream_argument_validation() -> None:
    provider = CoinbaseProvider()
    async with provider:
        with pytest.raises(ValidationError, match="at least one"):
            await take(provider.stream_quotes([]), 1)


async def test_closing_the_provider_ends_a_running_stream() -> None:
    async with WsReplayServer([session("coinbase", "ticker")]) as server:
        provider = _stream_provider(server)
        await provider.open()
        stream = provider.stream_quotes([BTC])
        await stream.__anext__()
        await provider.close()
        _ = [q async for q in stream]
    assert provider._streams == set()
