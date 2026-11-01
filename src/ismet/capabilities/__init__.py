"""Capability protocols a provider may implement.

A provider declares what it can do by implementing these protocols. The client
discovers them with :func:`capabilities_of` and raises
:class:`ismet.errors.NotSupported` at call time for anything missing, so a
caller never gets a ``NotImplementedError`` from deep inside an adapter.

Trading adapters are held to a stricter contract than market data, because the
order manager's safety rests on it: see :class:`TradingCapability`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from ismet.models import Bar, Instrument, Interval, OrderBook, Quote, Symbol, Trade
from ismet.models.orders import (
    Balance,
    Fill,
    OrderAck,
    OrderEvent,
    OrderRequest,
    Position,
    TradingRules,
    VenueOrderSnapshot,
)


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


@runtime_checkable
class TradingCapability(Protocol):
    """Order entry.

    The contract the order manager depends on:

    - ``place_order`` sends the request at most once and uses
      ``request.client_order_id`` as the venue's client order id. A venue-level
      refusal raises :class:`~ismet.errors.VenueError`, or its subclass
      :class:`~ismet.errors.DuplicateOrderId` when the venue already holds that client
      order id. A failure where the order
      may have arrived raises :class:`~ismet.errors.AmbiguousOutcome`. A failure
      that certainly did not deliver raises another
      :class:`~ismet.errors.TransportError`.
    - ``order_status`` finds an order by client order id and returns ``None`` only
      when the venue says it does not exist. A failed query raises.
    - ``cancel_order`` accepts a client order id whether or not an ack was seen.
    - ``stream_executions`` yields venue events until cancelled, and raises if the
      connection is lost and cannot be restored. Reports may be duplicated or
      reordered; fills must carry a stable ``fill_id``. Subscribe as early as possible;
      the order manager reconciles again shortly after connecting, to cover the time a
      subscription takes to go live.
    """

    def trading_rules(self) -> TradingRules: ...

    async def place_order(self, request: OrderRequest) -> OrderAck: ...

    async def cancel_order(self, client_order_id: str, symbol: Symbol) -> None: ...

    async def amend_order(
        self,
        client_order_id: str,
        symbol: Symbol,
        *,
        quantity: Decimal | None = None,
        limit_price: Decimal | None = None,
    ) -> None: ...

    async def order_status(
        self, client_order_id: str, symbol: Symbol
    ) -> VenueOrderSnapshot | None: ...

    async def open_orders(
        self, symbol: Symbol | None = None
    ) -> list[VenueOrderSnapshot]: ...

    async def fills_since(
        self, since: datetime, symbol: Symbol | None = None
    ) -> list[Fill]: ...

    def stream_executions(self) -> AsyncIterator[OrderEvent]: ...


@runtime_checkable
class AccountCapability(Protocol):
    """Positions and balances as the venue reports them."""

    async def positions(self) -> list[Position]: ...

    async def balances(self) -> list[Balance]: ...


PROTOCOLS: dict[Capability, type[Any]] = {
    Capability.MARKET_DATA: MarketDataCapability,
    Capability.HISTORICAL: HistoricalCapability,
    Capability.STREAMING: StreamingCapability,
    Capability.REFERENCE_DATA: ReferenceDataCapability,
    Capability.TRADING: TradingCapability,
    Capability.ACCOUNT: AccountCapability,
}


def capabilities_of(obj: object) -> frozenset[Capability]:
    """The capabilities ``obj`` structurally implements."""
    return frozenset(cap for cap, proto in PROTOCOLS.items() if isinstance(obj, proto))


__all__ = [
    "PROTOCOLS",
    "AccountCapability",
    "Capability",
    "HistoricalCapability",
    "MarketDataCapability",
    "ReferenceDataCapability",
    "StreamingCapability",
    "TradingCapability",
    "capabilities_of",
]
