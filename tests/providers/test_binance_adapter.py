"""Adapter behaviour through replay. No test here reaches the network."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from ismet.capabilities import Capability
from ismet.config import ProviderSettings, Settings
from ismet.errors import (
    AuthError,
    ConfigError,
    RateLimited,
    TransportError,
    ValidationError,
    VenueError,
)
from ismet.models import Interval, QualityFlag, Symbol
from ismet.providers.binance import (
    BINANCE_RETRY,
    RATE_LIMITS,
    REQUEST_WEIGHT_PER_MINUTE,
    REST_URL,
    WS_CONNECTIONS_PER_5_MINUTES,
    WS_URL,
    BinanceProvider,
)
from ismet.testing.replay import ReplayTransport, WsReplayServer, load_exchanges
from ismet.transport.backoff import NO_RETRY, ExponentialBackoff, RetryPolicy
from ismet.transport.clock import ManualClock

from ._support import (
    FAST_BACKOFF,
    FIXTURES,
    body,
    frames,
    manifest,
    ms,
    replay,
    session,
    take,
)

BTC = Symbol(venue="BNCX", ticker="BTCUSDT")
ETH = Symbol(venue="BNCX", ticker="ETHUSDT")
UTC = timezone.utc


def _provider(cassette: str = "conformance", **options) -> BinanceProvider:  # type: ignore[no-untyped-def]
    return BinanceProvider(http_transport=replay("binance", cassette), **options)


def _requests(provider: BinanceProvider) -> list[str]:
    return [url for _, url in provider._http._httpx_transport.requests]  # type: ignore[union-attr]


def _query(url: str) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}


def test_declares_exactly_the_market_data_capabilities() -> None:
    provider = BinanceProvider()
    assert provider.capabilities() == {
        Capability.MARKET_DATA,
        Capability.HISTORICAL,
        Capability.STREAMING,
        Capability.REFERENCE_DATA,
    }
    assert provider.venues == {"BNCX"}
    assert provider.serves("bncx")


def test_defaults_are_the_market_data_only_hosts() -> None:
    assert REST_URL == "https://data-api.binance.vision"
    assert WS_URL == "wss://data-stream.binance.vision/ws"


def test_from_settings_reads_urls() -> None:
    settings = ProviderSettings(
        name="binance", options={"rest_url": "https://a.example", "ws_url": "wss://b"}
    )
    provider = BinanceProvider.from_settings(settings)
    assert provider._http.base_url == "https://a.example"
    assert provider._ws_url == "wss://b"


def test_rate_limit_declarations_match_the_venue() -> None:
    limits = body("binance", "conformance", "exchangeInfo?symbol=BTCUSDT")["rateLimits"]
    weight = next(x for x in limits if x["rateLimitType"] == "REQUEST_WEIGHT")
    assert (weight["interval"], weight["intervalNum"]) == ("MINUTE", 1)
    assert weight["limit"] == REQUEST_WEIGHT_PER_MINUTE


@pytest.mark.parametrize(
    ("endpoint", "documented_weight"),
    [
        ("exchange_info", 20),
        ("klines", 2),
        ("depth_100", 5),
        ("depth_500", 25),
        ("depth_1000", 50),
        ("depth_5000", 250),
    ],
)
async def test_each_endpoint_is_charged_its_documented_weight(
    endpoint: str, documented_weight: int
) -> None:
    provider = BinanceProvider(clock=ManualClock())
    bucket = provider._limiter.bucket("weight")
    assert bucket is not None
    before = bucket.available
    await provider._limiter.acquire(endpoint)
    assert before - bucket.available == documented_weight


def test_buckets_cannot_exceed_the_fixed_window_limits() -> None:
    weight = RATE_LIMITS["weight"]
    assert weight.capacity + weight.rate * 60 <= REQUEST_WEIGHT_PER_MINUTE
    connect = RATE_LIMITS["ws_connect"]
    assert connect.capacity + connect.rate * 300 <= WS_CONNECTIONS_PER_5_MINUTES


async def test_requests_are_charged_their_documented_weight() -> None:
    clock = ManualClock()
    async with _provider(clock=clock) as provider:
        bucket = provider._limiter.bucket("weight")
        assert bucket is not None
        full = bucket.available
        await provider.instrument(BTC)
        assert bucket.available == full - 20
        await provider.quote(BTC)
        assert bucket.available == full - 25
        await provider.order_book(BTC, 5)
        assert bucket.available == full - 30


@pytest.mark.parametrize(
    ("depth", "key"),
    [
        (1, "depth_100"),
        (100, "depth_100"),
        (101, "depth_500"),
        (500, "depth_500"),
        (1000, "depth_1000"),
        (5000, "depth_5000"),
    ],
)
def test_depth_weight_tiers(depth: int, key: str) -> None:
    from ismet.providers.binance import _depth_rate_key

    assert _depth_rate_key(depth) == key


async def test_quote_reads_currency_once_then_only_the_book() -> None:
    async with _provider() as provider:
        first = await provider.quote(BTC)
        await provider.quote(BTC)
    urls = _requests(provider)
    assert sum("exchangeInfo" in u for u in urls) == 1
    assert sum("depth" in u for u in urls) == 2
    assert first.currency == "USDT"
    assert first.bid is not None
    assert first.ask is not None


async def test_quote_stamps_received_time_from_the_provider_clock() -> None:
    clock = ManualClock(datetime(2030, 1, 1, tzinfo=UTC))
    async with _provider(clock=clock) as provider:
        quote = await provider.quote(BTC)
        book = await provider.order_book(BTC, 5)
    assert quote.received_ts == clock.now()
    assert quote.exchange_ts == clock.now()
    assert book.exchange_ts == clock.now()


async def test_order_book_sends_the_requested_depth() -> None:
    async with _provider() as provider:
        book = await provider.order_book(BTC, 5)
    assert len(book.bids) == len(book.asks) == 5
    assert any("limit=5" in u for u in _requests(provider))


@pytest.mark.parametrize("depth", [0, -1, 5001])
async def test_depth_out_of_range_raises_before_any_request(depth: int) -> None:
    async with _provider() as provider:
        with pytest.raises(ValidationError, match="depth"):
            await provider.order_book(BTC, depth)
    assert _requests(provider) == []


async def test_wrong_venue_and_non_native_ticker_are_refused() -> None:
    async with _provider() as provider:
        with pytest.raises(ValidationError, match="BNCX"):
            await provider.quote(Symbol(venue="ZCBS", ticker="BTCUSDT"))
        with pytest.raises(ValidationError, match="native"):
            await provider.quote(Symbol(venue="BNCX", ticker="btcusdt"))
    assert _requests(provider) == []


async def test_bars_cover_the_conformance_window() -> None:
    end = manifest("binance", "conformance_end")
    start = end - timedelta(minutes=30)
    async with _provider() as provider:
        bars = await provider.bars(BTC, Interval.M1, start, end)
    assert len(bars) == 30
    assert bars[0].exchange_ts == start
    assert bars[-1].exchange_ts == end - timedelta(minutes=1)


async def test_bars_paginate_ascending_without_duplicates() -> None:
    start = manifest("binance", "bars_paged_start")
    end = manifest("binance", "bars_paged_end")
    async with _provider("bars_paged", klines_per_request=3) as provider:
        bars = await provider.bars(BTC, Interval.M1, start, end)
    stamps = [b.exchange_ts for b in bars]
    assert stamps == [start + timedelta(minutes=i) for i in range(7)]
    pages = [_query(u) for u in _requests(provider) if "klines" in u]
    assert len(pages) == 3
    assert all(p["limit"] == "3" for p in pages)
    assert all(p["endTime"] == str(ms(end) - 1) for p in pages)
    assert int(pages[0]["startTime"]) == ms(start)
    assert int(pages[1]["startTime"]) == ms(start + timedelta(minutes=2)) + 1
    assert int(pages[2]["startTime"]) == ms(start + timedelta(minutes=5)) + 1


async def test_bars_exclude_end_and_round_a_sub_millisecond_start_up() -> None:
    start = manifest("binance", "bars_paged_start")
    end = manifest("binance", "bars_paged_end")
    tiny = timedelta(microseconds=1)
    async with _provider("bars_paged", klines_per_request=3) as provider:
        bars = await provider.bars(BTC, Interval.M1, start - tiny, end)
    assert bars[0].exchange_ts == start
    assert bars[-1].exchange_ts < end


async def test_bars_stop_on_a_short_page_with_one_request() -> None:
    end = manifest("binance", "conformance_end")
    async with _provider() as provider:
        await provider.bars(BTC, Interval.M1, end - timedelta(minutes=30), end)
    assert sum("klines" in u for u in _requests(provider)) == 1


async def test_daily_bars_have_session_dates() -> None:
    start = manifest("binance", "bars_daily_start")
    end = manifest("binance", "bars_daily_end")
    async with _provider("bars_daily") as provider:
        bars = await provider.bars(BTC, Interval.D1, start, end)
    assert [b.session_date for b in bars] == [
        (start + timedelta(days=i)).date() for i in range(3)
    ]


async def test_bars_drop_the_open_kline_via_the_provider_clock() -> None:
    start = manifest("binance", "bars_open_start")
    end = manifest("binance", "bars_open_end")
    clock = ManualClock(manifest("binance", "recorded_at"))
    async with _provider("bars_open", clock=clock) as provider:
        bars = await provider.bars(BTC, Interval.M1, start, end)
    assert bars
    assert all(b.exchange_ts + timedelta(minutes=1) <= clock.now() for b in bars)


async def test_bars_refuse_a_vendor_that_does_not_advance() -> None:
    rows = body("binance", "bars_paged", "klines")
    start = datetime.fromtimestamp((rows[-1][0] + 1) / 1000, UTC)

    def respond(request: httpx.Request) -> httpx.Response:
        if "exchangeInfo" in str(request.url):
            info = body("binance", "conformance", "exchangeInfo?symbol=BTCUSDT")
            return httpx.Response(200, json=info)
        return httpx.Response(200, json=rows)

    provider = BinanceProvider(
        http_transport=httpx.MockTransport(respond), klines_per_request=3
    )
    async with provider:
        with pytest.raises(TransportError, match="did not advance"):
            await provider.bars(BTC, Interval.M1, start, start + timedelta(hours=1))


async def test_bars_validate_before_any_request() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    async with _provider() as provider:
        with pytest.raises(ValidationError, match="timezone-aware"):
            await provider.bars(BTC, Interval.M1, datetime(2026, 1, 1), now)
        with pytest.raises(ValidationError, match="before end"):
            await provider.bars(BTC, Interval.M1, now, now)
        with pytest.raises(ValidationError, match="max 100000"):
            await provider.bars(BTC, Interval.S1, now, now + timedelta(days=30))
    assert _requests(provider) == []


def test_klines_per_request_is_bounded_by_the_vendor_maximum() -> None:
    for bad in (0, 1001):
        with pytest.raises(ValidationError, match="klines_per_request"):
            BinanceProvider(klines_per_request=bad)


async def test_instrument_reads_fresh_each_time() -> None:
    async with _provider() as provider:
        instrument = await provider.instrument(BTC)
        await provider.instrument(BTC)
    assert instrument.tick_size > 0
    assert sum("exchangeInfo" in u for u in _requests(provider)) == 2


async def test_single_letter_quote_asset_is_a_real_currency() -> None:
    btcu = Symbol(venue="BNCX", ticker="BTCU")
    exchanges = load_exchanges(FIXTURES / "binance" / "rest_instruments.json")
    provider = BinanceProvider(http_transport=ReplayTransport(exchanges))
    async with provider:
        assert await provider._currency("BTCU") == "U"
        assert (await provider.instrument(btcu)).currency == "U"


async def test_search_ranks_exact_first_and_active_before_suspended() -> None:
    async with _provider() as provider:
        hits = await provider.search("btcusdt", venue="BNCX", limit=5)
        assert [i.symbol.ticker for i in hits] == ["BTCUSDT"]
        by_asset = await provider.search("BTC", limit=20)
    tickers = [i.symbol.ticker for i in by_asset]
    assert tickers[-1] == "NEOBTC"
    assert by_asset[-1].active is False
    assert all(i.active for i in by_asset[:-1])


async def test_search_includes_listings_with_single_letter_assets() -> None:
    async with _provider() as provider:
        hits = await provider.search("BTCU")
    assert "BTCU" in [i.symbol.ticker for i in hits]


async def test_search_keeps_a_non_ascii_listing() -> None:
    async with _provider() as provider:
        hits = await provider.search("币安")
    assert [i.symbol.ticker for i in hits] == ["币安人生USDT"]
    assert hits[0].base_asset == "币安人生"


async def test_search_uses_one_cached_exchange_info_until_it_expires() -> None:
    clock = ManualClock()
    async with _provider(clock=clock, search_cache_seconds=60) as provider:
        await provider.search("BTC")
        await provider.search("ETH")
        full = [u for u in _requests(provider) if "symbol=" not in u]
        assert len(full) == 1
        clock.advance(61)
        await provider.search("BTC")
        full = [u for u in _requests(provider) if "symbol=" not in u]
        assert len(full) == 2


async def test_search_honours_venue_and_limit() -> None:
    async with _provider() as provider:
        assert await provider.search("BTC", venue="XNAS") == []
        assert await provider.search("BTC", limit=0) == []
        assert len(await provider.search("BTC", limit=2)) == 2
    assert sum("symbol=" not in u for u in _requests(provider)) == 1


async def test_recorded_invalid_symbol_preserves_the_raw_code() -> None:
    bad = Symbol(venue="BNCX", ticker="NOSUCHSYMBOL")
    async with _provider("errors", retry_policy=NO_RETRY) as provider:
        with pytest.raises(VenueError) as caught:
            await provider.instrument(bad)
    assert caught.value.code == "-1121"
    assert caught.value.status == 400
    assert caught.value.payload["msg"] == "Invalid symbol."


async def test_recorded_geo_block_is_a_venue_error_with_status_451() -> None:
    provider = BinanceProvider(
        rest_url="https://api.binance.com",
        http_transport=replay("binance", "geo_blocked"),
        retry_policy=NO_RETRY,
    )
    async with provider:
        with pytest.raises(VenueError) as caught:
            await provider._get("/api/v3/time", {}, "klines")
    assert caught.value.status == 451
    assert caught.value.code == "0"
    assert "restricted location" in str(caught.value)


# Bodies below are from errors.md, or empty where rest-api.md documents only the code.
TOO_MUCH_WEIGHT = {
    "code": -1003,
    "msg": "Too much request weight used; current limit is 6000 request weight per "
    "1 MINUTE. Please use WebSocket Streams for live updates to avoid polling the API.",
}


def _serving(*responses: httpx.Response) -> tuple[httpx.MockTransport, list[str]]:
    seen: list[str] = []
    queue = list(responses)

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return queue.pop(0) if len(queue) > 1 else queue[0]

    return httpx.MockTransport(respond), seen


async def test_429_with_a_short_retry_after_is_retried_after_it() -> None:
    ok = httpx.Response(200, json={"symbols": []})
    limited = httpx.Response(429, headers={"Retry-After": "0"}, json=TOO_MUCH_WEIGHT)
    transport, seen = _serving(limited, ok)
    provider = BinanceProvider(http_transport=transport)
    async with provider:
        assert await provider._get("/api/v3/exchangeInfo", {}, "exchange_info")
    assert len(seen) == 2


async def test_429_with_a_retry_after_beyond_the_cap_is_not_retried() -> None:
    limited = httpx.Response(429, headers={"Retry-After": "120"}, json=TOO_MUCH_WEIGHT)
    transport, seen = _serving(limited)
    provider = BinanceProvider(http_transport=transport)
    async with provider:
        with pytest.raises(RateLimited) as caught:
            await provider._get("/api/v3/depth", {}, "klines")
    assert caught.value.retry_after == 120
    assert len(seen) == 1


async def test_429_without_retry_after_is_not_retried() -> None:
    transport, seen = _serving(httpx.Response(429, json=TOO_MUCH_WEIGHT))
    provider = BinanceProvider(http_transport=transport)
    async with provider:
        with pytest.raises(RateLimited):
            await provider._get("/api/v3/depth", {}, "klines")
    assert len(seen) == 1


async def test_418_ban_is_rate_limited_never_retried_and_says_until_when() -> None:
    clock = ManualClock(datetime(2030, 1, 1, tzinfo=UTC))
    banned_until = int(clock.now().timestamp() * 1000) + 90_000
    ban = httpx.Response(
        418,
        json={
            "code": -1003,
            "msg": f"Way too much request weight used; IP banned until {banned_until}. "
            "Please use WebSocket Streams for live updates to avoid bans.",
        },
    )
    transport, seen = _serving(ban)
    provider = BinanceProvider(http_transport=transport, clock=clock)
    async with provider:
        with pytest.raises(RateLimited, match="banned") as caught:
            await provider._get("/api/v3/depth", {}, "klines")
    assert caught.value.retry_after == pytest.approx(90)
    assert len(seen) == 1


async def test_a_ban_is_honoured_locally_until_it_ends() -> None:
    clock = ManualClock(datetime(2030, 1, 1, tzinfo=UTC))
    banned_until = int(clock.now().timestamp() * 1000) + 90_000
    ban = httpx.Response(
        418, json={"code": -1003, "msg": f"IP banned until {banned_until}."}
    )
    transport, seen = _serving(ban)
    provider = BinanceProvider(http_transport=transport, clock=clock)
    async with provider:
        with pytest.raises(RateLimited):
            await provider._get("/api/v3/depth", {}, "klines")
        clock.advance(60)
        with pytest.raises(RateLimited, match="not sending") as caught:
            await provider._get("/api/v3/depth", {}, "klines")
        assert caught.value.retry_after == pytest.approx(30)
        assert len(seen) == 1
        clock.advance(31)
        with pytest.raises(RateLimited):
            await provider._get("/api/v3/depth", {}, "klines")
        assert len(seen) == 2


async def test_403_waf_block_is_auth_error_and_not_retried() -> None:
    transport, seen = _serving(httpx.Response(403))
    provider = BinanceProvider(http_transport=transport)
    async with provider:
        with pytest.raises(AuthError) as caught:
            await provider._get("/api/v3/depth", {}, "klines")
    assert caught.value.status == 403
    assert len(seen) == 1


async def test_5xx_is_transport_error_after_policy_retries() -> None:
    transport, seen = _serving(httpx.Response(503))
    policy = RetryPolicy(
        max_attempts=3,
        backoff=ExponentialBackoff(base=0, jitter=False),
        should_retry=BINANCE_RETRY.should_retry,
    )
    provider = BinanceProvider(http_transport=transport, retry_policy=policy)
    async with provider:
        with pytest.raises(TransportError) as caught:
            await provider._get("/api/v3/depth", {}, "klines")
    assert caught.value.status == 503
    assert caught.value.retryable is True
    assert len(seen) == 3


def _stream_provider(server: WsReplayServer, **options) -> BinanceProvider:  # type: ignore[no-untyped-def]
    return BinanceProvider(
        http_transport=replay("binance", "conformance"),
        ws_url=server.url,
        ws_backoff=FAST_BACKOFF,
        **options,
    )


async def test_stream_quotes_subscribes_and_normalizes() -> None:
    recorded = session("binance", "book_ticker")
    async with WsReplayServer([recorded]) as server:
        provider = _stream_provider(server)
        async with provider:
            quotes = await take(provider.stream_quotes([BTC]), 10)
    assert server.connections[0].received[0] == {
        "method": "SUBSCRIBE",
        "params": ["btcusdt@bookTicker"],
        "id": 1,
    }
    assert server.unexpected == []
    wanted = frames("binance", "book_ticker")[:10]
    assert [q.sequence for q in quotes] == [f["u"] for f in wanted]
    assert all(q.currency == "USDT" for q in quotes)
    assert all(q.symbol == BTC for q in quotes)
    assert all(q.quality == frozenset() for q in quotes)


async def test_stream_received_time_is_stamped_on_arrival() -> None:
    clock = ManualClock(datetime(2030, 1, 1, tzinfo=UTC))
    async with WsReplayServer([session("binance", "book_ticker")]) as server:
        provider = _stream_provider(server, clock=clock)
        async with provider:
            quotes = await take(provider.stream_quotes([BTC]), 3)
    assert all(q.received_ts == clock.now() for q in quotes)


async def test_stream_trades_routes_each_frame_to_its_symbol() -> None:
    recorded = session("binance", "trade_multi")
    exchanges = [
        *load_exchanges(FIXTURES / "binance" / "rest_conformance.json"),
    ]
    btc_info = next(e for e in exchanges if "symbol=BTCUSDT" in e.url)
    eth_info = replace(
        btc_info,
        url=btc_info.url.replace("BTCUSDT", "ETHUSDT"),
        note="BTCUSDT reference data reused: only the quote asset is read",
    )
    async with WsReplayServer([recorded]) as server:
        provider = BinanceProvider(
            http_transport=ReplayTransport([*exchanges, eth_info]),
            ws_url=server.url,
            ws_backoff=FAST_BACKOFF,
        )
        async with provider:
            trades = await take(
                provider.stream_trades([BTC, ETH]), len(recorded.frames)
            )
    assert server.connections[0].received[0]["params"] == [
        "btcusdt@trade",
        "ethusdt@trade",
    ]
    wanted = frames("binance", "trade_multi")
    assert [(t.symbol.ticker, t.sequence) for t in trades] == [
        (f["s"], f["t"]) for f in wanted
    ]
    assert all(t.quality == frozenset() for t in trades)


async def test_reconnect_resubscribes_and_loses_nothing_when_nothing_was_missed() -> (
    None
):
    recorded = session("binance", "trade")
    plan = [slice(0, 5), slice(5, 12)]
    async with WsReplayServer([recorded], plan=plan) as server:
        provider = _stream_provider(server)
        async with provider:
            trades = await take(provider.stream_trades([BTC]), 12)
    subscribe = {"method": "SUBSCRIBE", "params": ["btcusdt@trade"], "id": 1}
    assert len(server.connections) == 2
    assert all(c.received == [subscribe] for c in server.connections)
    wanted = frames("binance", "trade")[:12]
    assert [t.sequence for t in trades] == [f["t"] for f in wanted]
    assert all(t.quality == frozenset() for t in trades)


async def test_a_trade_repeated_after_reconnect_is_delivered_once() -> None:
    recorded = session("binance", "trade")
    plan = [slice(0, 5), slice(4, 8)]
    async with WsReplayServer([recorded], plan=plan) as server:
        provider = _stream_provider(server)
        async with provider:
            trades = await take(provider.stream_trades([BTC]), 8)
    wanted = frames("binance", "trade")[:8]
    assert [t.sequence for t in trades] == [f["t"] for f in wanted]


async def test_trades_missed_across_a_reconnect_flag_the_next_trade(
    caplog: pytest.LogCaptureFixture,
) -> None:
    recorded = session("binance", "trade")
    plan = [slice(0, 5), slice(8, 12)]
    async with WsReplayServer([recorded], plan=plan) as server:
        provider = _stream_provider(server)
        async with provider:
            trades = await take(provider.stream_trades([BTC]), 9)
    wanted = frames("binance", "trade")
    assert [t.sequence for t in trades] == [f["t"] for f in wanted[:5] + wanted[8:12]]
    assert trades[5].quality == {QualityFlag.AFTER_GAP}
    assert all(t.quality == frozenset() for t in trades[:5] + trades[6:])
    assert "3 events missing" in caplog.text


async def test_first_quote_after_reconnect_is_flagged_because_u_cannot_show_loss() -> (
    None
):
    recorded = session("binance", "book_ticker")
    plan = [slice(0, 4), slice(4, 8)]
    async with WsReplayServer([recorded], plan=plan) as server:
        provider = _stream_provider(server)
        async with provider:
            quotes = await take(provider.stream_quotes([BTC]), 8)
    assert [q.quality for q in quotes] == [
        *[frozenset()] * 4,
        frozenset({QualityFlag.AFTER_GAP}),
        *[frozenset()] * 3,
    ]


async def test_recorded_venue_rejection_ends_the_stream_with_the_raw_code() -> None:
    rejected = session("binance", "rejected")
    answering_subscribe = replace(
        rejected,
        sent=({"method": "SUBSCRIBE", "params": ["btcusdt@trade"], "id": 1},),
    )
    async with WsReplayServer([answering_subscribe]) as server:
        provider = _stream_provider(server)
        async with provider:
            with pytest.raises(VenueError) as caught:
                await take(provider.stream_trades([BTC]), 1)
    assert caught.value.code == "2"


async def test_unknown_symbol_fails_before_connecting() -> None:
    bad = Symbol(venue="BNCX", ticker="NOSUCHSYMBOL")
    async with WsReplayServer([]) as server:
        provider = BinanceProvider(
            http_transport=replay("binance", "errors"),
            retry_policy=NO_RETRY,
            ws_url=server.url,
        )
        async with provider:
            with pytest.raises(VenueError) as caught:
                await take(provider.stream_trades([bad]), 1)
    assert caught.value.code == "-1121"
    assert server.connections == []


async def test_stream_argument_validation() -> None:
    provider = BinanceProvider(http_transport=replay("binance", "conformance"))
    async with provider:
        with pytest.raises(ValidationError, match="at least one"):
            await take(provider.stream_quotes([]), 1)
        too_many = [Symbol(venue="BNCX", ticker=f"A{i}USDT") for i in range(1025)]
        with pytest.raises(ValidationError, match="1024"):
            await take(provider.stream_quotes(too_many), 1)


async def test_control_frames_are_not_events(caplog: pytest.LogCaptureFixture) -> None:
    recorded = session("binance", "book_ticker")
    shutdown = json.dumps({"e": "serverShutdown", "E": 1770123456789})
    documented = replace(recorded, control=(*recorded.control, shutdown))
    async with WsReplayServer([documented]) as server:
        provider = _stream_provider(server)
        async with provider:
            quotes = await take(provider.stream_quotes([BTC]), 2)
    assert len(quotes) == 2
    assert "server shutdown" in caplog.text


async def test_closing_the_provider_ends_a_running_stream() -> None:
    recorded = session("binance", "book_ticker")
    async with WsReplayServer([recorded]) as server:
        provider = _stream_provider(server)
        await provider.open()
        stream = provider.stream_quotes([BTC])
        first = await stream.__anext__()
        assert isinstance(first.bid, Decimal)
        await provider.close()
        remaining = [q async for q in stream]
    wanted = frames("binance", "book_ticker")[1 : 1 + len(remaining)]
    assert [q.sequence for q in remaining] == [f["u"] for f in wanted]
    assert provider._streams == set()


async def test_a_server_that_only_acknowledges_exhausts_the_reconnect_bound() -> None:
    recorded = session("binance", "book_ticker")
    async with (
        WsReplayServer([recorded], plan=[slice(0, 0)] * 6) as server,
        _stream_provider(server, ws_max_reconnects=2) as provider,
    ):
        with pytest.raises(TransportError, match="after 2 reconnects"):
            await take(provider.stream_quotes([BTC]), 1)
    assert len(server.connections) == 3


def _kline(opened: datetime) -> list[object]:
    close_time = opened + timedelta(seconds=59, milliseconds=999)
    return [
        ms(opened),
        "1",
        "2",
        "1",
        "1.5",
        "3",
        ms(close_time),
        "4",
        5,
        "1",
        "1",
        "0",
    ]


async def test_a_fast_host_clock_cannot_pass_a_partial_kline_off_as_closed() -> None:
    """The host clock runs 3s fast, so its 02:09:01 is really 02:08:58.

    By the host clock the 02:08 kline closed at 02:08:59.999, but Binance's own ``Date``
    says the response was made a second and more before that close.
    """
    opened = datetime(2026, 11, 1, 2, 8, tzinfo=timezone.utc)
    prior = opened - timedelta(minutes=1)
    info = body("binance", "conformance", "exchangeInfo?symbol=BTCUSDT")

    def respond(request: httpx.Request) -> httpx.Response:
        if "exchangeInfo" in str(request.url):
            return httpx.Response(200, json=info)
        return httpx.Response(
            200,
            json=[_kline(prior), _kline(opened)],
            headers={"date": "Sun, 01 Nov 2026 02:08:58 GMT"},
        )

    provider = BinanceProvider(
        http_transport=httpx.MockTransport(respond),
        clock=ManualClock(datetime(2026, 11, 1, 2, 9, 1, tzinfo=timezone.utc)),
    )
    async with provider:
        bars = await provider.bars(
            BTC, Interval.M1, prior, opened + timedelta(minutes=1)
        )
    assert [b.exchange_ts for b in bars] == [prior]


async def test_a_ban_on_the_klines_path_is_still_remembered() -> None:
    calls = 0
    info = body("binance", "conformance", "exchangeInfo?symbol=BTCUSDT")

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        if "exchangeInfo" in str(request.url):
            return httpx.Response(200, json=info)
        calls += 1
        return httpx.Response(418, json={"code": -1003, "msg": "IP banned"})

    start = datetime(2026, 11, 1, 2, 8, tzinfo=timezone.utc)
    provider = BinanceProvider(
        http_transport=httpx.MockTransport(respond),
        retry_policy=NO_RETRY,
        clock=ManualClock(start + timedelta(hours=1)),
    )
    async with provider:
        for _ in range(2):
            with pytest.raises(RateLimited):
                await provider.bars(BTC, Interval.M1, start, start + timedelta(hours=1))
    assert calls == 1


def _unreachable(**options: object) -> ProviderSettings:
    return ProviderSettings(
        name="binance", options={"ws_url": "ws://127.0.0.1:1", **options}
    )


async def test_the_reconnect_bound_is_reachable_from_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ISMET_BINANCE_WS_MAX_RECONNECTS", "0")
    monkeypatch.setenv("ISMET_BINANCE_WS_URL", "ws://127.0.0.1:1")
    monkeypatch.setenv("ISMET_PROVIDERS", "binance")
    settings = Settings.load().for_provider("binance")
    provider = BinanceProvider.from_settings(settings)
    provider._currencies["BTCUSDT"] = "USDT"
    async with provider:
        with pytest.raises(TransportError, match="after 0 reconnects"):
            await take(provider.stream_trades([BTC]), 1)


@pytest.mark.parametrize("value", [-1, "many", True, 1.5])
def test_a_reconnect_bound_that_is_not_a_count_is_refused(value: object) -> None:
    with pytest.raises(ConfigError, match="ws_max_reconnects"):
        BinanceProvider.from_settings(_unreachable(ws_max_reconnects=value))
