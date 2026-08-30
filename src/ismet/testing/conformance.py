"""Provider conformance suite.

Runs every capability a provider declares against a symbol it serves and
checks the invariants the rest of ismet relies on. The report is plain data
so it can be rendered into the adapter matrix.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TypeVar, cast

from ismet.capabilities import (
    PROTOCOLS,
    Capability,
    HistoricalCapability,
    MarketDataCapability,
    ReferenceDataCapability,
    StreamingCapability,
)
from ismet.errors import NotSupported
from ismet.models import Bar, Instrument, Interval, MarketEvent, Symbol
from ismet.providers import Provider

T = TypeVar("T")


@dataclass(frozen=True)
class Check:
    capability: Capability | None
    name: str
    passed: bool
    detail: str = ""


@dataclass
class ConformanceReport:
    provider: str
    capabilities: frozenset[Capability]
    checks: list[Check] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def failures(self) -> list[Check]:
        return [c for c in self.checks if not c.passed]

    def summary(self) -> str:
        lines = [f"{self.provider}: {'PASS' if self.passed else 'FAIL'}"]
        for c in self.checks:
            scope = c.capability.value if c.capability else "core"
            mark = "ok " if c.passed else "FAIL"
            lines.append(
                f"  [{mark}] {scope}.{c.name}" + (f": {c.detail}" if c.detail else "")
            )
        return "\n".join(lines)


def _event_invariants(event: MarketEvent, instrument: Instrument | None) -> list[str]:
    problems: list[str] = []
    if event.exchange_ts.tzinfo is None or event.received_ts.tzinfo is None:
        problems.append("naive timestamp")
    if event.exchange_ts > event.received_ts:
        problems.append("exchange_ts after received_ts")
    if instrument is not None:
        for name in ("bid", "ask", "last", "price", "open", "high", "low", "close"):
            value = getattr(event, name, None)
            if isinstance(value, Decimal) and not instrument.is_tick_aligned(value):
                problems.append(
                    f"{name}={value} not aligned to tick {instrument.tick_size}"
                )
    return problems


async def run_conformance(
    provider: Provider,
    *,
    symbol: Symbol | None = None,
    interval: Interval = Interval.M1,
    lookback: timedelta = timedelta(minutes=30),
    stream_messages: int = 3,
    timeout: float = 10.0,
    search_query: str | None = None,
) -> ConformanceReport:
    """Exercise every declared capability and return a report."""
    caps = provider.capabilities()
    report = ConformanceReport(provider.name, caps)
    add = report.checks.append

    if symbol is None:
        if not provider.venues:
            add(Check(None, "venues", False, "provider declares no venues"))
            return report
        instrument_for_symbol = None
        if Capability.REFERENCE_DATA in caps and search_query is not None:
            found = await cast(ReferenceDataCapability, provider).search(
                search_query, limit=1
            )
            instrument_for_symbol = found[0] if found else None
        if instrument_for_symbol is None:
            add(Check(None, "symbol", False, "no symbol given and search found none"))
            return report
        symbol = instrument_for_symbol.symbol

    add(
        Check(None, "venues", bool(provider.venues), ", ".join(sorted(provider.venues)))
    )
    add(Check(None, "serves_symbol", provider.serves(symbol.venue), str(symbol)))

    for cap in PROTOCOLS:
        if cap in caps:
            continue
        try:
            provider.require(cap)
        except NotSupported as exc:
            add(Check(cap, "not_supported_raises", True, str(exc)))
        else:
            add(Check(cap, "not_supported_raises", False, "require() did not raise"))

    instrument: Instrument | None = None
    if Capability.REFERENCE_DATA in caps:
        ref = cast(ReferenceDataCapability, provider)
        instrument = await _timed(
            report,
            cap_=Capability.REFERENCE_DATA,
            name="instrument",
            fn=lambda: ref.instrument(symbol),
            timeout=timeout,
        )
        if instrument is not None:
            add(
                Check(
                    Capability.REFERENCE_DATA,
                    "instrument_fields",
                    instrument.tick_size > 0
                    and instrument.lot_size > 0
                    and bool(instrument.currency),
                    f"tick={instrument.tick_size} lot={instrument.lot_size} "
                    f"ccy={instrument.currency}",
                )
            )
        query = search_query or symbol.ticker
        results = await _timed(
            report,
            cap_=Capability.REFERENCE_DATA,
            name="search",
            fn=lambda: ref.search(query, venue=symbol.venue, limit=5),
            timeout=timeout,
        )
        if results is not None:
            add(
                Check(
                    Capability.REFERENCE_DATA,
                    "search_returns_symbol",
                    any(i.symbol == symbol for i in results),
                    f"{len(results)} results",
                )
            )

    if Capability.MARKET_DATA in caps:
        md = cast(MarketDataCapability, provider)
        quote = await _timed(
            report,
            cap_=Capability.MARKET_DATA,
            name="quote",
            fn=lambda: md.quote(symbol),
            timeout=timeout,
        )
        if quote is not None:
            problems = _event_invariants(quote, instrument)
            if quote.bid is None and quote.ask is None and quote.last is None:
                problems.append("quote has no bid, ask, or last")
            add(
                Check(
                    Capability.MARKET_DATA,
                    "quote_invariants",
                    not problems,
                    "; ".join(problems),
                )
            )
        book = await _timed(
            report,
            cap_=Capability.MARKET_DATA,
            name="order_book",
            fn=lambda: md.order_book(symbol, 5),
            timeout=timeout,
        )
        if book is not None:
            problems = _event_invariants(book, instrument)
            if instrument is not None:
                for lvl in (*book.bids, *book.asks):
                    if not instrument.is_tick_aligned(lvl.price):
                        problems.append(f"level {lvl.price} not tick aligned")
                        break
            add(
                Check(
                    Capability.MARKET_DATA,
                    "order_book_invariants",
                    not problems,
                    "; ".join(problems),
                )
            )

    if Capability.HISTORICAL in caps:
        hist = cast(HistoricalCapability, provider)
        end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        start = end - lookback
        bars = await _timed(
            report,
            cap_=Capability.HISTORICAL,
            name="bars",
            fn=lambda: hist.bars(symbol, interval, start, end),
            timeout=timeout,
        )
        if bars is not None:
            problems = _bars_invariants(bars, instrument, start, end)
            add(
                Check(
                    Capability.HISTORICAL,
                    "bars_invariants",
                    not problems,
                    f"{len(bars)} bars"
                    + ("; " + "; ".join(problems) if problems else ""),
                )
            )

    if Capability.STREAMING in caps:
        st = cast(StreamingCapability, provider)
        report.checks.append(
            await _check_stream(
                "stream_quotes",
                st.stream_quotes([symbol]),
                instrument,
                stream_messages,
                timeout,
            )
        )
        report.checks.append(
            await _check_stream(
                "stream_trades",
                st.stream_trades([symbol]),
                instrument,
                stream_messages,
                timeout,
            )
        )

    return report


async def _check_stream(
    name: str,
    stream: AsyncIterator[MarketEvent],
    instrument: Instrument | None,
    wanted: int,
    timeout: float,
) -> Check:
    received: list[MarketEvent] = []

    async def drain() -> None:
        try:
            async for event in stream:
                received.append(event)
                if len(received) >= wanted:
                    break
        finally:
            aclose = getattr(stream, "aclose", None)
            if aclose is not None:
                with contextlib.suppress(Exception):
                    await aclose()

    try:
        await asyncio.wait_for(drain(), timeout)
    except asyncio.TimeoutError:
        return Check(
            Capability.STREAMING,
            name,
            False,
            f"got {len(received)}/{wanted} within {timeout}s",
        )
    except Exception as exc:
        return Check(Capability.STREAMING, name, False, f"{type(exc).__name__}: {exc}")
    problems = [p for e in received for p in _event_invariants(e, instrument)]
    seqs = [e.sequence for e in received if e.sequence is not None]
    if seqs and seqs != sorted(seqs):
        problems.append("sequence numbers not monotonic")
    detail = f"{len(received)} messages"
    if problems:
        detail += "; " + "; ".join(problems)
    return Check(Capability.STREAMING, name, not problems, detail)


def _bars_invariants(
    bars: list[Bar], instrument: Instrument | None, start: datetime, end: datetime
) -> list[str]:
    problems: list[str] = []
    if not bars:
        return ["no bars returned"]
    stamps = [b.exchange_ts for b in bars]
    if stamps != sorted(stamps):
        problems.append("bars not in ascending time order")
    if len(set(stamps)) != len(stamps):
        problems.append("duplicate bar timestamps")
    if stamps[0] < start or stamps[-1] >= end:
        problems.append("bars outside [start, end)")
    for b in bars:
        problems.extend(_event_invariants(b, instrument))
        if problems:
            break
    return problems


async def _timed(
    report: ConformanceReport,
    *,
    cap_: Capability,
    name: str,
    fn: Callable[[], Awaitable[T]],
    timeout: float,
) -> T | None:
    try:
        result = await asyncio.wait_for(fn(), timeout)
    except asyncio.TimeoutError:
        report.checks.append(Check(cap_, name, False, f"timed out after {timeout}s"))
        return None
    except Exception as exc:
        report.checks.append(Check(cap_, name, False, f"{type(exc).__name__}: {exc}"))
        return None
    report.checks.append(Check(cap_, name, True))
    return result


def assert_conformant(report: ConformanceReport) -> None:
    """Raise ``AssertionError`` with the full summary if any check failed."""
    if not report.passed:
        raise AssertionError(report.summary())


__all__ = [
    "Check",
    "ConformanceReport",
    "assert_conformant",
    "run_conformance",
]
