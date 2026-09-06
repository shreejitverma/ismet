"""Market data domain models: quotes, trades, bars, order books, instruments."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ismet.models.symbol import Symbol
from ismet.models.types import (
    AssetClass,
    Currency,
    DecimalStrict,
    Interval,
    Price,
    Quantity,
    Ratio,
    Side,
    Timestamp,
)


class MarketEvent(BaseModel):
    """Base for every timestamped event.

    ``exchange_ts`` is the venue's own timestamp for the event; ``received_ts``
    is when ismet received it. Both are timezone-aware and normalised to UTC.
    ``sequence`` is the venue or provider sequence number when one exists, used
    for gap detection on streams.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    symbol: Symbol
    exchange_ts: Timestamp
    received_ts: Timestamp
    sequence: int | None = None

    @model_validator(mode="after")
    def _ordering(self) -> MarketEvent:
        if self.exchange_ts > self.received_ts:
            raise ValueError(
                f"exchange_ts {self.exchange_ts.isoformat()} is after received_ts "
                f"{self.received_ts.isoformat()}"
            )
        return self


class Quote(MarketEvent):
    """Top-of-book snapshot. Any side may be absent when the book is empty."""

    bid: Price | None = None
    ask: Price | None = None
    bid_size: Quantity | None = None
    ask_size: Quantity | None = None
    last: Price | None = None
    last_size: Quantity | None = None
    currency: Currency | None = None

    @model_validator(mode="after")
    def _crossed(self) -> Quote:
        if self.bid is not None and self.ask is not None and self.bid > self.ask:
            raise ValueError(f"crossed quote: bid {self.bid} > ask {self.ask}")
        return self

    @property
    def mid(self) -> Decimal | None:
        """Midpoint of bid and ask, or ``None`` if either side is missing."""
        if self.bid is None or self.ask is None:
            return None
        return (self.bid + self.ask) / 2

    @property
    def spread(self) -> Decimal | None:
        """``ask - bid``, or ``None`` if either side is missing."""
        if self.bid is None or self.ask is None:
            return None
        return self.ask - self.bid


class Trade(MarketEvent):
    """A single execution."""

    price: Price
    size: Quantity
    side: Side = Side.UNKNOWN
    trade_id: str | None = None
    conditions: tuple[str, ...] = ()
    currency: Currency | None = None


class Bar(MarketEvent):
    """OHLCV bar. ``exchange_ts`` is the bar's open time."""

    interval: Interval
    open: Price
    high: Price
    low: Price
    close: Price
    volume: Quantity
    vwap: Price | None = None
    trade_count: int | None = Field(default=None, ge=0)
    currency: Currency | None = None

    @model_validator(mode="after")
    def _ohlc(self) -> Bar:
        if not (self.low <= self.open <= self.high):
            raise ValueError(f"open {self.open} outside [{self.low}, {self.high}]")
        if not (self.low <= self.close <= self.high):
            raise ValueError(f"close {self.close} outside [{self.low}, {self.high}]")
        return self


class Level(BaseModel):
    """One price level of an order book."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    price: Price
    size: Quantity
    count: int | None = Field(default=None, ge=0)


class OrderBook(MarketEvent):
    """Depth snapshot. Bids descend by price, asks ascend."""

    bids: tuple[Level, ...] = ()
    asks: tuple[Level, ...] = ()
    currency: Currency | None = None

    @model_validator(mode="after")
    def _sorted(self) -> OrderBook:
        bid_prices = [lvl.price for lvl in self.bids]
        ask_prices = [lvl.price for lvl in self.asks]
        if bid_prices != sorted(bid_prices, reverse=True):
            raise ValueError("bids must be sorted by descending price")
        if ask_prices != sorted(ask_prices):
            raise ValueError("asks must be sorted by ascending price")
        if bid_prices and ask_prices and bid_prices[0] > ask_prices[0]:
            raise ValueError(
                f"crossed book: best bid {bid_prices[0]} > best ask {ask_prices[0]}"
            )
        return self

    @property
    def best_bid(self) -> Level | None:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Level | None:
        return self.asks[0] if self.asks else None


class Instrument(BaseModel):
    """Static reference data for a tradable instrument."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    symbol: Symbol
    name: str
    asset_class: AssetClass = AssetClass.UNKNOWN
    currency: Currency
    tick_size: Price
    lot_size: Quantity = Decimal(1)
    multiplier: Ratio = Decimal(1)
    min_quantity: Quantity | None = None
    expiry: Timestamp | None = None
    underlying: Symbol | None = None
    active: bool = True

    def is_tick_aligned(self, price: Decimal) -> bool:
        """Whether ``price`` is an exact multiple of ``tick_size``."""
        return price % self.tick_size == 0

    def is_lot_aligned(self, quantity: Decimal) -> bool:
        """Whether ``quantity`` is an exact multiple of ``lot_size``."""
        return quantity % self.lot_size == 0

    def round_price(self, price: Decimal) -> Decimal:
        """Round ``price`` down to the nearest tick."""
        return (price // self.tick_size) * self.tick_size


__all__ = [
    "Bar",
    "DecimalStrict",
    "Instrument",
    "Level",
    "MarketEvent",
    "OrderBook",
    "Quote",
    "Trade",
]
