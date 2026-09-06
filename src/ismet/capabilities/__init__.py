"""Capability protocols a provider may implement.

A provider declares what it can do by implementing these protocols. The client
discovers them with :func:`capabilities_of` and raises
:class:`ismet.errors.NotSupported` at call time for anything missing, so a
caller never gets a ``NotImplementedError`` from deep inside an adapter.

Trading and account protocols arrive with milestone M2; their enum members
exist now so discovery output is forward compatible.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from ismet.models import Bar, Instrument, Interval, OrderBook, Quote, Symbol, Trade


class Capability(str, Enum):
    MARKET_DATA = "market_data"
    HISTORICAL = "historical"
    STREAMING = "streaming"
    REFERENCE_DATA = "reference_data"
    TRADING = "trading"
    ACCOUNT = "account"


@runtime_checkable
class MarketDataCapability(Protocol):
    """Snapshot market data."""

    async def quote(self, symbol: Symbol) -> Quote: ...

    async def order_book(self, symbol: Symbol, depth: int = 10) -> OrderBook: ...


@runtime_checkable
class HistoricalCapability(Protocol):
    """Historical bars. ``start`` inclusive, ``end`` exclusive, both aware."""

    async def bars(
        self, symbol: Symbol, interval: Interval, start: datetime, end: datetime
    ) -> list[Bar]: ...


@runtime_checkable
class StreamingCapability(Protocol):
    """Real-time streams. Iterators end only when the caller stops."""

    def stream_quotes(self, symbols: Sequence[Symbol]) -> AsyncIterator[Quote]: ...

    def stream_trades(self, symbols: Sequence[Symbol]) -> AsyncIterator[Trade]: ...


@runtime_checkable
class ReferenceDataCapability(Protocol):
    """Instrument lookup and search."""

    async def instrument(self, symbol: Symbol) -> Instrument: ...

    async def search(
        self, query: str, *, venue: str | None = None, limit: int = 20
    ) -> list[Instrument]: ...


PROTOCOLS: dict[Capability, type[Any]] = {
    Capability.MARKET_DATA: MarketDataCapability,
    Capability.HISTORICAL: HistoricalCapability,
    Capability.STREAMING: StreamingCapability,
    Capability.REFERENCE_DATA: ReferenceDataCapability,
}


def capabilities_of(obj: object) -> frozenset[Capability]:
    """The capabilities ``obj`` structurally implements."""
    return frozenset(cap for cap, proto in PROTOCOLS.items() if isinstance(obj, proto))


__all__ = [
    "PROTOCOLS",
    "Capability",
    "HistoricalCapability",
    "MarketDataCapability",
    "ReferenceDataCapability",
    "StreamingCapability",
    "capabilities_of",
]
