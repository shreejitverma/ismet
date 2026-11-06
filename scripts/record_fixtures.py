"""Regenerate tests/fixtures/<provider>/ from the real network.

    uv run --extra dev python scripts/record_fixtures.py [binance] [coinbase]

Public market data only. Every fixture is a real response. Three responses are too
big to commit whole, so the recorder keeps a real subset and says so in the
exchange's ``note``: the full symbol or product lists, and the Coinbase level 2 book.
The script stops if a file comes out above MAX_FILE_BYTES or a secret pattern matches.

``manifest.json`` holds the instants the tests must replay at. Requests carry time
ranges, so the replay tests freeze the clock to these values.
"""

from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ismet.models import Interval, Symbol
from ismet.providers.binance import WS_URL as BINANCE_WS_URL
from ismet.providers.binance import BinanceProvider
from ismet.providers.coinbase import WS_URL as COINBASE_WS_URL
from ismet.providers.coinbase import CoinbaseProvider
from ismet.testing.replay import (
    RecordedExchange,
    RecordingTransport,
    find_secrets,
    record_session,
    save_exchanges,
    save_session,
)

FIXTURES = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
MAX_FILE_BYTES = 60_000
WS_FRAMES = 25
WS_TIMEOUT = 90.0
BINANCE_KEPT_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "ETHBTC",
    "NEOBTC",
    "TUSDT",
    "BTCU",
    "币安人生USDT",
)
COINBASE_KEPT_PRODUCTS = (
    "BTC-USD",
    "ETH-USD",
    "ETH-BTC",
    "T-USD",
    "ZRX-BTC",
    "BTC-INR",
)
COINBASE_BOOK_LEVELS = 10
BINANCE_INSTRUMENTS = ("NEOBTC", "TUSDT", "BTCU", "币安人生USDT")
COINBASE_INSTRUMENTS = ("T-USD", "ZRX-BTC", "BTC-INR")


def _minute(moment: datetime) -> datetime:
    return moment.replace(second=0, microsecond=0)


def _iso(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def _keep_symbols(exchange: RecordedExchange) -> RecordedExchange:
    if "exchangeInfo" not in exchange.url or "symbol=" in exchange.url:
        return exchange
    document = json.loads(exchange.body)
    total = len(document["symbols"])
    document["symbols"] = [
        s for s in document["symbols"] if s["symbol"] in BINANCE_KEPT_SYMBOLS
    ]
    return replace(
        exchange,
        body=json.dumps(document, ensure_ascii=False, separators=(",", ":")),
        note=f"symbols filtered to {len(document['symbols'])} of {total} by recorder",
    )


def _keep_products(exchange: RecordedExchange) -> RecordedExchange:
    if not exchange.url.endswith("/products"):
        return exchange
    document = json.loads(exchange.body)
    kept = [p for p in document if p["id"] in COINBASE_KEPT_PRODUCTS]
    return replace(
        exchange,
        body=json.dumps(kept, separators=(",", ":")),
        note=f"products filtered to {len(kept)} of {len(document)} by recorder",
    )


def _keep_book_levels(exchange: RecordedExchange) -> RecordedExchange:
    if "level=2" not in exchange.url:
        return exchange
    document = json.loads(exchange.body)
    totals = len(document["bids"]), len(document["asks"])
    document["bids"] = document["bids"][:COINBASE_BOOK_LEVELS]
    document["asks"] = document["asks"][:COINBASE_BOOK_LEVELS]
    return replace(
        exchange,
        body=json.dumps(document, separators=(",", ":")),
        note=f"cut to the best {COINBASE_BOOK_LEVELS} of {totals} levels by recorder",
    )


def _check_outputs(directory: Path) -> None:
    for path in sorted(directory.glob("*.json")):
        size = path.stat().st_size
        print(f"  {path.name}: {size} bytes")
        if size > MAX_FILE_BYTES:
            raise SystemExit(f"{path} is {size} bytes; record less")
        found = find_secrets(path.read_text(encoding="utf-8"))
        if found:
            raise SystemExit(f"{path} matches secret patterns: {found}")


async def _binance_rest(
    directory: Path, recorded_at: str, end: datetime
) -> dict[str, str]:
    btc = Symbol(venue="BNCX", ticker="BTCUSDT")
    start = end - timedelta(minutes=30)

    async def session(
        name: str,
        run: Callable[[BinanceProvider], Awaitable[Any]],
        rest_url: str = "https://data-api.binance.vision",
        **options: Any,
    ) -> None:
        recorder = RecordingTransport()
        provider = BinanceProvider(
            rest_url=rest_url, http_transport=recorder, **options
        )
        async with provider:
            await run(provider)
        await recorder.aclose()
        save_exchanges(
            directory / f"rest_{name}.json",
            [_keep_symbols(e) for e in recorder.exchanges],
            recorded_at,
        )

    async def conformance(provider: BinanceProvider) -> None:
        await provider.instrument(btc)
        await provider.search("BTCUSDT", venue="BNCX", limit=5)
        await provider.quote(btc)
        await provider.order_book(btc, 5)
        await provider.bars(btc, Interval.M1, start, end)

    async def instruments(provider: BinanceProvider) -> None:
        for ticker in BINANCE_INSTRUMENTS:
            await provider._symbol_info(ticker)

    async def empty_book(provider: BinanceProvider) -> None:
        await provider.quote(Symbol(venue="BNCX", ticker="NEOBTC"))

    async def paged(provider: BinanceProvider) -> None:
        await provider.bars(btc, Interval.M1, start, start + timedelta(minutes=7))

    async def daily(provider: BinanceProvider) -> None:
        day = end.replace(hour=0, minute=0)
        await provider.bars(btc, Interval.D1, day - timedelta(days=3), day)

    async def open_bar(provider: BinanceProvider) -> None:
        await provider.bars(
            btc, Interval.M1, end - timedelta(minutes=3), end + timedelta(minutes=2)
        )

    async def errors(provider: BinanceProvider) -> None:
        for path, params in (
            ("/api/v3/depth", {"symbol": "NOSUCHSYMBOL", "limit": 1}),
            (
                "/api/v3/exchangeInfo",
                {"symbol": "NOSUCHSYMBOL", "showPermissionSets": "false"},
            ),
            ("/api/v3/klines", {"symbol": "BTCUSDT", "interval": "7m"}),
        ):
            try:
                await provider._get(path, params, "klines")
            except Exception as exc:
                print("   expected:", type(exc).__name__, exc)

    async def geo_blocked(provider: BinanceProvider) -> None:
        try:
            await provider._get("/api/v3/time", {}, "klines")
        except Exception as exc:
            print("   expected:", type(exc).__name__, exc)

    await session("conformance", conformance)
    await session("instruments", instruments)
    await session("empty_book", empty_book)
    await session("bars_paged", paged, klines_per_request=3)
    await session("bars_daily", daily)
    await session("bars_open", open_bar)
    await session("errors", errors)
    await session("geo_blocked", geo_blocked, rest_url="https://api.binance.com")
    return {
        "bars_paged_start": _iso(start),
        "bars_paged_end": _iso(start + timedelta(minutes=7)),
        "bars_daily_start": _iso(end.replace(hour=0, minute=0) - timedelta(days=3)),
        "bars_daily_end": _iso(end.replace(hour=0, minute=0)),
        "bars_open_start": _iso(end - timedelta(minutes=3)),
        "bars_open_end": _iso(end + timedelta(minutes=2)),
    }


async def _coinbase_rest(
    directory: Path, recorded_at: str, end: datetime
) -> dict[str, str]:
    btc = Symbol(venue="ZCBS", ticker="BTC-USD")
    start = end - timedelta(minutes=30)
    long_start = end - timedelta(minutes=310)

    async def session(
        name: str, run: Callable[[CoinbaseProvider], Awaitable[Any]], **options: Any
    ) -> None:
        recorder = RecordingTransport()
        provider = CoinbaseProvider(http_transport=recorder, **options)
        async with provider:
            await run(provider)
        await recorder.aclose()
        save_exchanges(
            directory / f"rest_{name}.json",
            [_keep_book_levels(_keep_products(e)) for e in recorder.exchanges],
            recorded_at,
        )

    async def conformance(provider: CoinbaseProvider) -> None:
        await provider.instrument(btc)
        await provider.search("BTC-USD", venue="ZCBS", limit=5)
        await provider.quote(btc)
        await provider.order_book(btc, 5)
        await provider.bars(btc, Interval.M1, start, end)

    async def instruments(provider: CoinbaseProvider) -> None:
        for ticker in COINBASE_INSTRUMENTS:
            await provider.instrument(Symbol(venue="ZCBS", ticker=ticker))

    async def paged(provider: CoinbaseProvider) -> None:
        await provider.bars(btc, Interval.M1, start, start + timedelta(minutes=7))

    async def full_pages(provider: CoinbaseProvider) -> None:
        await provider.bars(btc, Interval.M1, long_start, end)

    async def daily(provider: CoinbaseProvider) -> None:
        day = end.replace(hour=0, minute=0)
        await provider.bars(btc, Interval.D1, day - timedelta(days=3), day)

    async def open_bar(provider: CoinbaseProvider) -> None:
        await provider.bars(
            btc, Interval.M1, end - timedelta(minutes=3), end + timedelta(minutes=2)
        )

    async def errors(provider: CoinbaseProvider) -> None:
        range_start = "2026-10-31T00:00:00Z"
        for path, params in (
            ("/products/NOPE-USD/book", {"level": 1}),
            ("/products/BTC-USD/candles", {"granularity": 7}),
            (
                "/products/BTC-USD/candles",
                {
                    "granularity": 60,
                    "start": range_start,
                    "end": "2026-10-31T06:00:00Z",
                },
            ),
        ):
            try:
                await provider._get(path, params)
            except Exception as exc:
                print("   expected:", type(exc).__name__, exc)

    await session("conformance", conformance)
    await session("instruments", instruments)
    await session("bars_paged", paged, candles_per_request=3)
    await session("bars_full_pages", full_pages)
    await session("bars_daily", daily)
    await session("bars_open", open_bar)
    await session("errors", errors)
    return {
        "bars_paged_start": _iso(start),
        "bars_paged_end": _iso(start + timedelta(minutes=7)),
        "bars_full_pages_start": _iso(long_start),
        "bars_daily_start": _iso(end.replace(hour=0, minute=0) - timedelta(days=3)),
        "bars_daily_end": _iso(end.replace(hour=0, minute=0)),
        "bars_open_start": _iso(end - timedelta(minutes=3)),
        "bars_open_end": _iso(end + timedelta(minutes=2)),
    }


def _binance_data(frame: Mapping[str, Any]) -> bool:
    return "s" in frame


def _coinbase_data(frame: Mapping[str, Any]) -> bool:
    return frame.get("type") in ("ticker", "match", "last_match")


async def _binance_ws(directory: Path, recorded_at: str) -> None:
    def subscribe(*params: str) -> dict[str, Any]:
        return {"method": "SUBSCRIBE", "params": list(params), "id": 1}

    for name, sent in (
        ("book_ticker", subscribe("btcusdt@bookTicker")),
        ("trade", subscribe("btcusdt@trade")),
        ("trade_multi", subscribe("btcusdt@trade", "ethusdt@trade")),
    ):
        session = await record_session(
            BINANCE_WS_URL,
            [sent],
            _binance_data,
            frames=WS_FRAMES,
            control=1,
            timeout=WS_TIMEOUT,
        )
        if len(session.frames) < WS_FRAMES:
            raise SystemExit(f"binance {name}: only {len(session.frames)} frames")
        save_session(directory / f"ws_{name}.json", session, recorded_at)
    rejected = await record_session(
        BINANCE_WS_URL,
        [{"method": "NOPE", "params": [], "id": 1}],
        _binance_data,
        frames=1,
        control=1,
        timeout=3.0,
    )
    save_session(directory / "ws_rejected.json", rejected, recorded_at)


async def _coinbase_ws(directory: Path, recorded_at: str) -> None:
    def subscribe(channel: str, *products: str) -> dict[str, Any]:
        return {
            "type": "subscribe",
            "product_ids": list(products),
            "channels": [channel],
        }

    for name, sent in (
        ("ticker", subscribe("ticker", "BTC-USD")),
        ("matches", subscribe("matches", "BTC-USD")),
        ("matches_multi", subscribe("matches", "BTC-USD", "ETH-USD")),
    ):
        session = await record_session(
            COINBASE_WS_URL,
            [sent],
            _coinbase_data,
            frames=WS_FRAMES,
            control=1,
            timeout=WS_TIMEOUT,
        )
        if len(session.frames) < WS_FRAMES:
            raise SystemExit(f"coinbase {name}: only {len(session.frames)} frames")
        save_session(directory / f"ws_{name}.json", session, recorded_at)
    rejected = await record_session(
        COINBASE_WS_URL,
        [subscribe("matches", "NOPE-USD")],
        _coinbase_data,
        frames=1,
        control=2,
        timeout=3.0,
    )
    save_session(directory / "ws_rejected.json", rejected, recorded_at)


async def record(provider: str) -> None:
    directory = FIXTURES / provider
    directory.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    recorded_at = _iso(now)
    end = _minute(now)
    if provider == "binance":
        times = await _binance_rest(directory, recorded_at, end)
        await _binance_ws(directory, recorded_at)
    else:
        times = await _coinbase_rest(directory, recorded_at, end)
        await _coinbase_ws(directory, recorded_at)
    manifest = {"recorded_at": recorded_at, "conformance_end": _iso(end), **times}
    (directory / "manifest.json").write_text(
        json.dumps(manifest, indent=1) + "\n", encoding="utf-8"
    )
    print(provider)
    _check_outputs(directory)


def main() -> None:
    wanted = sys.argv[1:] or ["binance", "coinbase"]
    unknown = set(wanted) - {"binance", "coinbase"}
    if unknown:
        raise SystemExit(f"unknown providers: {sorted(unknown)}")
    for provider in wanted:
        asyncio.run(record(provider))


if __name__ == "__main__":
    main()
