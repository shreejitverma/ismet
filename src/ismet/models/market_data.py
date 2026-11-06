"""Market data domain models: quotes, trades, bars, and order books.

Events describe what the market said, including states a naive model would
reject: a locked or crossed market, a venue clock ahead of the local clock. Those
are real conditions a consumer must see and decide on, so they are exposed as
:class:`~ismet.models.types.BookState` and
:class:`~ismet.models.types.QualityFlag`, not raised as errors. Only structurally
impossible data (an unsorted book, a close outside the bar's range) is rejected.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ismet.models.decimals import midpoint, sub
from ismet.models.symbol import Symbol
from ismet.models.types import (
    Adjustment,
    BookState,
    Currency,
    FeedKind,
    Interval,
    Price,
    QualityFlag,
    Quantity,
    Side,
    Timeliness,
    Timestamp,
    TradeAction,
)

NANOS_PER_MICRO = 1_000
NANOS_PER_SECOND = 1_000_000_000
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def datetime_from_ns(ns: int) -> datetime:
    """UTC datetime for integer epoch nanoseconds, floored to microseconds."""
    return _EPOCH + timedelta(microseconds=ns // NANOS_PER_MICRO)


class DataSource(BaseModel):
    """Where an event came from and how current it is by contract."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: str
    feed: str | None = None
    kind: FeedKind = FeedKind.UNKNOWN
    timeliness: Timeliness = Timeliness.UNKNOWN
    delay_seconds: int | None = Field(default=None, ge=0)


class MarketEvent(BaseModel):
    """Base for every timestamped event.

    ``exchange_ts`` is the venue's own timestamp for the event; ``received_ts``
    is when ismet received it. Both are timezone-aware and normalised to UTC.
    A venue clock ahead of the local clock is a measurement, not invalid data:
    see :attr:`clock_skew`.

    ``exchange_ts_ns`` carries integer epoch nanoseconds when a feed publishes
    them, because ``datetime`` stops at microseconds. ``sequence`` is the venue
    or provider sequence number when one exists, used for gap detection.
    ``source`` and ``quality`` declare provenance and known data conditions.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    symbol: Symbol
    exchange_ts: Timestamp
    received_ts: Timestamp
    exchange_ts_ns: int | None = None
    sequence: int | None = None
    source: DataSource | None = None
    quality: frozenset[QualityFlag] = frozenset()

    @model_validator(mode="after")
    def _nanos_consistent(self) -> MarketEvent:
        if (
            self.exchange_ts_ns is not None
            and datetime_from_ns(self.exchange_ts_ns) != self.exchange_ts
        ):
            raise ValueError(
                f"exchange_ts_ns {self.exchange_ts_ns} does not match exchange_ts "
                f"{self.exchange_ts.isoformat()}"
            )
        return self

    @property
    def clock_skew(self) -> timedelta:
        """How far ``exchange_ts`` is ahead of ``received_ts``; zero if behind."""
        return max(self.exchange_ts - self.received_ts, timedelta(0))

    @property
    def latency(self) -> timedelta:
        """``received_ts - exchange_ts``. Negative means clocks disagree."""
        return self.received_ts - self.exchange_ts

    @property
    def flags(self) -> frozenset[QualityFlag]:
        """Declared ``quality`` plus conditions derivable from this event alone."""
        derived = set(self.quality)
        if self.exchange_ts > self.received_ts:
            derived.add(QualityFlag.CLOCK_SKEW)
        return frozenset(derived)


def _book_state(best_bid: Decimal | None, best_ask: Decimal | None) -> BookState:
    if best_bid is None and best_ask is None:
        return BookState.EMPTY
    if best_bid is None or best_ask is None:
        return BookState.ONE_SIDED
    if best_bid < best_ask:
        return BookState.NORMAL
    return BookState.LOCKED if best_bid == best_ask else BookState.CROSSED


def _with_state(
    flags: frozenset[QualityFlag], state: BookState
) -> frozenset[QualityFlag]:
    if state is BookState.CROSSED:
        return flags | {QualityFlag.CROSSED}
    if state is BookState.LOCKED:
        return flags | {QualityFlag.LOCKED}
    return flags


class Quote(MarketEvent):
    """Top-of-book snapshot. Any side may be absent when the book is empty.

    Locked and crossed quotes are valid; check :attr:`book_state` before treating
    the market as tradable at the touch.
    """

    bid: Price | None = None
    ask: Price | None = None
    bid_size: Quantity | None = None
    ask_size: Quantity | None = None
    last: Price | None = None
    last_size: Quantity | None = None
    currency: Currency | None = None

    @property
    def book_state(self) -> BookState:
        return _book_state(self.bid, self.ask)

    @property
    def flags(self) -> frozenset[QualityFlag]:
        return _with_state(super().flags, self.book_state)

    @property
    def mid(self) -> Decimal | None:
        """Exact midpoint of bid and ask, or ``None`` if either side is missing."""
        if self.bid is None or self.ask is None:
            return None
        return midpoint(self.bid, self.ask)

    @property
    def spread(self) -> Decimal | None:
        """Exact ``ask - bid``, or ``None`` if a side is missing. May be negative."""
        if self.bid is None or self.ask is None:
            return None
        return sub(self.ask, self.bid)


class Trade(MarketEvent):
    """A single execution, or a venue correction or cancel of an earlier one."""

    price: Price
    size: Quantity
    side: Side = Side.UNKNOWN
    trade_id: str | None = None
    conditions: tuple[str, ...] = ()
    currency: Currency | None = None
    action: TradeAction = TradeAction.NEW
    corrects_trade_id: str | None = None
    eligible_for_bars: bool | None = None


class Bar(MarketEvent):
    """OHLCV bar. ``exchange_ts`` is the bar's open time.

    ``session_date`` is the venue trading date for session-based intervals.
    ``adjustment`` states what corporate actions the prices reflect, and
    ``revision`` increases when a vendor restates a bar.
    """

    interval: Interval
    open: Price
    high: Price
    low: Price
    close: Price
    volume: Quantity
    vwap: Price | None = None
    trade_count: int | None = Field(default=None, ge=0)
    currency: Currency | None = None
    adjustment: Adjustment = Adjustment.UNKNOWN
    session_date: date | None = None
    revision: int = Field(default=0, ge=0)

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
    """Depth snapshot. Bids descend by price, asks ascend, without duplicates.

    Sort order is structural and enforced. A crossed or locked book is a market
    condition, reported through :attr:`book_state`.
    """

    bids: tuple[Level, ...] = ()
    asks: tuple[Level, ...] = ()
    currency: Currency | None = None

    @model_validator(mode="after")
    def _sorted(self) -> OrderBook:
        bid_prices = [lvl.price for lvl in self.bids]
        ask_prices = [lvl.price for lvl in self.asks]
        if bid_prices != sorted(set(bid_prices), reverse=True):
            raise ValueError("bids must be sorted by descending price")
        if ask_prices != sorted(set(ask_prices)):
            raise ValueError("asks must be sorted by ascending price")
        return self

    @property
    def best_bid(self) -> Level | None:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Level | None:
        return self.asks[0] if self.asks else None

    @property
    def book_state(self) -> BookState:
        return _book_state(
            self.bids[0].price if self.bids else None,
            self.asks[0].price if self.asks else None,
        )

    @property
    def flags(self) -> frozenset[QualityFlag]:
        return _with_state(super().flags, self.book_state)


__all__ = [
    "Bar",
    "DataSource",
    "Level",
    "MarketEvent",
    "OrderBook",
    "Quote",
    "Trade",
    "datetime_from_ns",
]
