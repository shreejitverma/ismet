from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import ClassVar

import pytest

from ismet.capabilities import Capability
from ismet.models import Interval, Quote, Symbol
from ismet.providers import Provider
from ismet.providers.mock import MockProvider
from ismet.testing import assert_conformant, run_conformance

UTC = timezone.utc


async def test_mock_provider_is_conformant(sym: Symbol) -> None:
    report = await run_conformance(MockProvider(tick_interval=0), symbol=sym)
    assert_conformant(report)
    names = {(c.capability, c.name) for c in report.checks}
    assert (Capability.TRADING, "not_supported_raises") in names
    assert (Capability.TRADING, "place_order") not in names
    assert (Capability.STREAMING, "stream_quotes") in names
    assert "PASS" in report.summary()


async def test_symbol_discovered_via_search_when_omitted() -> None:
    report = await run_conformance(MockProvider(tick_interval=0), search_query="wayne")
    assert report.passed, report.summary()
    assert any(c.detail == "WAYNE@XMOK" for c in report.checks)


async def test_report_fails_without_symbol_or_venues() -> None:
    class NoVenues(Provider):
        name = "novenues"

    r = await run_conformance(NoVenues())
    assert not r.passed and r.checks[0].name == "venues"

    r2 = await run_conformance(MockProvider(), search_query="zzz")
    assert not r2.passed and r2.checks[0].name == "symbol"
    with pytest.raises(AssertionError, match="FAIL"):
        assert_conformant(r2)


async def test_invariant_violations_are_reported(sym: Symbol) -> None:
    class Sloppy(Provider):
        name = "sloppy"
        venues = frozenset({"XMOK"})

        async def quote(self, symbol: Symbol) -> Quote:
            now = datetime.now(UTC)
            return Quote(
                symbol=symbol, exchange_ts=now, received_ts=now, last=Decimal("1.005")
            )

        async def order_book(self, symbol: Symbol, depth: int = 10):  # type: ignore[no-untyped-def]
            return await MockProvider().order_book(symbol, depth)

        async def instrument(self, symbol: Symbol):  # type: ignore[no-untyped-def]
            return await MockProvider().instrument(symbol)

        async def search(
            self, query: str, *, venue: str | None = None, limit: int = 20
        ):  # type: ignore[no-untyped-def]
            return await MockProvider().search(query, venue=venue, limit=limit)

        async def stream_quotes(self, symbols):  # type: ignore[no-untyped-def]
            for s in symbols:
                yield await self.quote(s)

        async def stream_trades(self, symbols):  # type: ignore[no-untyped-def]
            raise RuntimeError("stream broke")
            yield

        def require(self, capability: Capability) -> None:  # never raises
            return None

    report = await run_conformance(Sloppy(), symbol=sym, stream_messages=1)
    assert Capability.HISTORICAL not in report.capabilities
    failed = {c.name: c.detail for c in report.failures}
    assert "not aligned to tick" in failed["quote_invariants"]
    assert failed["not_supported_raises"] == "require() did not raise"
    assert "RuntimeError" in failed["stream_trades"]
    assert "not aligned" in failed["stream_quotes"]
    assert "FAIL" in report.summary()


async def test_a_stream_that_ends_before_delivering_fails(sym: Symbol) -> None:
    """A declared stream must deliver: ending at once proves nothing works."""

    class EndsAtOnce(MockProvider):
        name = "endsatonce"

        async def stream_quotes(self, symbols):  # type: ignore[no-untyped-def, override]
            return
            yield

        async def stream_trades(self, symbols):  # type: ignore[no-untyped-def, override]
            quote = await self.quote(symbols[0])
            yield quote
            return

    report = await run_conformance(
        EndsAtOnce(tick_interval=0), symbol=sym, stream_messages=3
    )
    failed = {c.name: c.detail for c in report.failures}
    assert "stream_quotes" in failed and "0" in failed["stream_quotes"]
    assert "stream_trades" in failed and "1" in failed["stream_trades"]
    with pytest.raises(AssertionError, match="stream_quotes"):
        assert_conformant(report)


async def test_timeouts_are_reported(sym: Symbol) -> None:
    class Slow(MockProvider):
        name = "slow"

        async def instrument(self, symbol: Symbol):  # type: ignore[no-untyped-def, override]
            import asyncio

            await asyncio.sleep(10)

    report = await run_conformance(
        Slow(tick_interval=1.0), symbol=sym, timeout=0.05, stream_messages=5
    )
    failed = {c.name: c.detail for c in report.failures}
    assert "timed out" in failed["instrument"]
    assert "within 0.05s" in failed["stream_quotes"]


OTHER = Symbol(venue="XMOK", ticker="GLOBEX")
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
STREAMS = {"stream_quotes", "stream_trades"}


class AnswersForAnotherSymbol(MockProvider):
    name = "wrong-symbol"

    async def quote(self, symbol: Symbol) -> Quote:
        return await super().quote(OTHER)

    async def order_book(self, symbol: Symbol, depth: int = 10):  # type: ignore[no-untyped-def]
        return await super().order_book(OTHER, depth)

    async def bars(self, symbol, interval, start, end):  # type: ignore[no-untyped-def]
        return await super().bars(OTHER, interval, start, end)

    def stream_quotes(self, symbols):  # type: ignore[no-untyped-def]
        return super().stream_quotes([OTHER])

    def stream_trades(self, symbols):  # type: ignore[no-untyped-def]
        return super().stream_trades([OTHER])


class LabelsBarsDaily(MockProvider):
    name = "wrong-interval"

    async def bars(self, symbol, interval, start, end):  # type: ignore[no-untyped-def]
        return [
            bar.model_copy(update={"interval": Interval.D1})
            for bar in await super().bars(symbol, interval, start, end)
        ]


class BarsOffTheInterval(MockProvider):
    name = "off-interval"

    async def bars(self, symbol, interval, start, end):  # type: ignore[no-untyped-def]
        bars = await super().bars(symbol, interval, start, end)
        return [
            bar.model_copy(
                update={"exchange_ts": bar.exchange_ts + timedelta(seconds=30)}
            )
            if i % 2
            else bar
            for i, bar in enumerate(bars)
        ]


# 09:30 and 13:30 New York on the Friday and Monday around the US change to daylight
# time on 2026-03-08: EST is UTC-5 and EDT is UTC-4.
DST_GRID = [
    datetime(2026, 3, 6, 14, 30, tzinfo=UTC),
    datetime(2026, 3, 6, 18, 30, tzinfo=UTC),
    datetime(2026, 3, 9, 13, 30, tzinfo=UTC),
    datetime(2026, 3, 9, 17, 30, tzinfo=UTC),
]


class LocalGridAcrossDst(MockProvider):
    name = "local-grid"
    stamps: ClassVar[list[datetime]] = DST_GRID

    async def bars(self, symbol, interval, start, end):  # type: ignore[no-untyped-def]
        hour = await super().bars(symbol, Interval.M1, end - timedelta(hours=1), end)
        template = hour[0]
        return [
            template.model_copy(update={"exchange_ts": ts, "interval": interval})
            for ts in self.stamps
        ]


class OffGridByHalfAnHour(LocalGridAcrossDst):
    name = "half-hour-off"
    stamps: ClassVar[list[datetime]] = [
        *DST_GRID[:3],
        DST_GRID[3] + timedelta(minutes=30),
    ]


class InstrumentForAnotherSymbol(MockProvider):
    name = "wrong-instrument"

    async def instrument(self, symbol: Symbol):  # type: ignore[no-untyped-def]
        return await super().instrument(OTHER)

    async def search(self, query: str, *, venue: str | None = None, limit: int = 20):  # type: ignore[no-untyped-def]
        # Mock search builds results through instrument(); keep it honest.
        return await MockProvider().search(query, venue=venue, limit=limit)


class BookTooDeep(MockProvider):
    name = "too-deep"

    async def order_book(self, symbol: Symbol, depth: int = 10):  # type: ignore[no-untyped-def]
        return await super().order_book(symbol, depth * 5)


class OneSideTooDeep(MockProvider):
    name = "one-side-too-deep"

    async def order_book(self, symbol: Symbol, depth: int = 10):  # type: ignore[no-untyped-def]
        book = await super().order_book(symbol, depth + 1)
        return book.model_copy(update={"bids": book.bids[:depth]})


class QuotesFromTheEpoch(MockProvider):
    name = "epoch-quotes"

    async def quote(self, symbol: Symbol) -> Quote:
        quote = await super().quote(symbol)
        return quote.model_copy(update={"exchange_ts": EPOCH})


class BookFromTheEpoch(MockProvider):
    name = "epoch-book"

    async def order_book(self, symbol: Symbol, depth: int = 10):  # type: ignore[no-untyped-def]
        book = await super().order_book(symbol, depth)
        return book.model_copy(update={"exchange_ts": EPOCH})


class TradesFromTheEpoch(MockProvider):
    name = "epoch-trades"

    async def stream_trades(self, symbols):  # type: ignore[no-untyped-def, override]
        async for trade in super().stream_trades(symbols):
            yield trade.model_copy(update={"exchange_ts": EPOCH})


@pytest.mark.parametrize(
    ("wrong", "failing", "named"),
    [
        (
            AnswersForAnotherSymbol,
            {"quote_invariants", "order_book_invariants", "bars_invariants"} | STREAMS,
            "GLOBEX@XMOK",
        ),
        (LabelsBarsDaily, {"bars_invariants"}, "1d"),
        (BarsOffTheInterval, {"bars_invariants"}, "not a whole number of 1m"),
        (InstrumentForAnotherSymbol, {"instrument_symbol"}, "GLOBEX@XMOK"),
        (BookTooDeep, {"order_book_invariants"}, "25 bid levels"),
        (OneSideTooDeep, {"order_book_invariants"}, "6 ask levels"),
        (QuotesFromTheEpoch, {"quote_invariants"}, "1970"),
        (BookFromTheEpoch, {"order_book_invariants"}, "1970"),
        (TradesFromTheEpoch, {"stream_trades"}, "1970"),
    ],
    ids=lambda value: value.name if isinstance(value, type) else None,
)
async def test_a_wrong_provider_fails_exactly_the_check_that_covers_it(
    sym: Symbol, wrong: type[MockProvider], failing: set[str], named: str
) -> None:
    report = await run_conformance(wrong(tick_interval=0), symbol=sym)
    assert {check.name for check in report.failures} == failing, report.summary()
    assert all(named in check.detail for check in report.failures), report.summary()


async def test_four_hour_bars_on_a_local_grid_pass_across_a_dst_change(
    sym: Symbol,
) -> None:
    lookback = datetime.now(UTC) - datetime(2026, 3, 1, tzinfo=UTC)
    options = {"symbol": sym, "interval": Interval.H4, "lookback": lookback}
    report = await run_conformance(LocalGridAcrossDst(tick_interval=0), **options)
    bars = next(c for c in report.checks if c.name == "bars_invariants")
    assert bars.passed, bars.detail
    report = await run_conformance(OffGridByHalfAnHour(tick_interval=0), **options)
    bars = next(c for c in report.checks if c.name == "bars_invariants")
    assert not bars.passed and "not a whole number of 4h" in bars.detail


async def test_a_book_of_exactly_the_asked_depth_passes(sym: Symbol) -> None:
    report = await run_conformance(MockProvider(tick_interval=0), symbol=sym)
    book = next(c for c in report.checks if c.name == "order_book_invariants")
    assert book.passed and not book.detail
