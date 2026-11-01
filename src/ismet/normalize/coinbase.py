"""Coinbase Exchange payloads to models.

Payload shapes are from https://docs.cdp.coinbase.com/exchange/ (REST reference
for products, and the WebSocket feed channels page), retrieved 2026-11-01, and
checked against recorded responses.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any

from ismet.errors import ValidationError, VenueError
from ismet.models import (
    Adjustment,
    AssetClass,
    Bar,
    Instrument,
    Interval,
    Level,
    ListingStatus,
    OrderBook,
    Quote,
    Side,
    Symbol,
    Trade,
    datetime_from_ns,
)
from ismet.normalize._common import (
    normalizer,
    parse_iso8601_utc,
    register_asset,
    venue_source,
)

PROVIDER = "coinbase"
FEED_BOOK = "rest:book"
FEED_CANDLES = "rest:candles"
FEED_TICKER = "ws:ticker"
FEED_MATCHES = "ws:matches"

CANDLE_GRANULARITY: Mapping[Interval, int] = {
    Interval.M1: 60,
    Interval.M5: 300,
    Interval.M15: 900,
    Interval.H1: 3600,
    Interval.D1: 86400,
}
"""Documented granularities that ismet has an interval for. Coinbase also has 6h,
which :class:`Interval` lacks, and has no 1s, 30m, 4h, 1w or 1M."""

_NANOS_PER_SECOND = 1_000_000_000

_normalizes = normalizer(PROVIDER)


def _exchange_time(
    payload: Mapping[str, Any], received: datetime
) -> tuple[datetime, int | None]:
    text = payload.get("time")
    return (received, None) if text is None else parse_iso8601_utc(text)


def _level(row: Sequence[Any]) -> Level:
    return Level(price=row[0], size=row[1], count=row[2])


@_normalizes
def quote_from_book(
    payload: Mapping[str, Any],
    symbol: Symbol,
    received: datetime,
    currency: str | None,
) -> Quote:
    """Top of book from ``GET /products/{id}/book?level=1``.

    ``time`` and ``sequence`` describe the snapshot, unlike ``/ticker``, whose
    ``time`` is the last trade's and whose response has no sequence. ``last`` is
    ``None`` because this endpoint does not carry it. If ``time`` is ever absent,
    ``exchange_ts`` falls back to ``received``.
    """
    exchange_ts, exchange_ns = _exchange_time(payload, received)
    bids, asks = payload["bids"], payload["asks"]
    best_bid = _level(bids[0]) if bids else None
    best_ask = _level(asks[0]) if asks else None
    return Quote(
        symbol=symbol,
        exchange_ts=exchange_ts,
        exchange_ts_ns=exchange_ns,
        received_ts=received,
        sequence=payload["sequence"],
        source=venue_source(PROVIDER, FEED_BOOK),
        bid=best_bid.price if best_bid else None,
        bid_size=best_bid.size if best_bid else None,
        ask=best_ask.price if best_ask else None,
        ask_size=best_ask.size if best_ask else None,
        currency=currency,
    )


@_normalizes
def order_book_from_book(
    payload: Mapping[str, Any],
    symbol: Symbol,
    received: datetime,
    depth: int,
    currency: str | None,
) -> OrderBook:
    """The best ``depth`` levels per side from ``GET /products/{id}/book``.

    Levels carry the venue's order count. Coinbase documents that size is the
    level's combined size and must not be multiplied by the count.
    """
    exchange_ts, exchange_ns = _exchange_time(payload, received)
    return OrderBook(
        symbol=symbol,
        exchange_ts=exchange_ts,
        exchange_ts_ns=exchange_ns,
        received_ts=received,
        sequence=payload["sequence"],
        source=venue_source(PROVIDER, FEED_BOOK),
        bids=tuple(_level(row) for row in payload["bids"][:depth]),
        asks=tuple(_level(row) for row in payload["asks"][:depth]),
        currency=currency,
    )


@_normalizes
def bars_from_candles(
    rows: Sequence[Sequence[Any]],
    symbol: Symbol,
    interval: Interval,
    received: datetime,
    currency: str | None,
) -> list[Bar]:
    """Closed bars from ``GET /products/{id}/candles``, in the order received.

    Each row is ``[time, low, high, open, close, volume]``: low comes before high
    and open comes third, and rows arrive newest first. A bucket still open at
    ``received`` is dropped because its values are not final. Coinbase publishes
    nothing for a bucket with no trades, so a gap in the result is an interval with
    no ticks, and ``trade_count`` is not available.
    """
    granularity = timedelta(seconds=CANDLE_GRANULARITY[interval])
    source = venue_source(PROVIDER, FEED_CANDLES)
    bars: list[Bar] = []
    for row in rows:
        opened = datetime_from_ns(int(row[0]) * _NANOS_PER_SECOND)
        if opened + granularity > received:
            continue
        bars.append(
            Bar(
                symbol=symbol,
                exchange_ts=opened,
                received_ts=received,
                source=source,
                interval=interval,
                low=row[1],
                high=row[2],
                open=row[3],
                close=row[4],
                volume=row[5],
                currency=currency,
                adjustment=Adjustment.RAW,
                session_date=opened.date() if interval.is_session_based else None,
            )
        )
    return bars


def _status(payload: Mapping[str, Any]) -> ListingStatus:
    if payload["status"] == "delisted":
        return ListingStatus.DELISTED
    if payload["status"] == "offline":
        return ListingStatus.SUSPENDED
    if payload["status"] != "online":
        return ListingStatus.UNKNOWN
    if payload["trading_disabled"]:
        return ListingStatus.HALTED
    return ListingStatus.SUSPENDED if payload["cancel_only"] else ListingStatus.ACTIVE


@_normalizes
def instrument_from_product(payload: Mapping[str, Any], symbol: Symbol) -> Instrument:
    """Instrument from ``GET /products/{id}`` or one entry of ``GET /products``.

    Registers both assets. Tick is ``quote_increment``, lot is ``base_increment``,
    minimum notional is ``min_market_funds``. The response has no quantity limits
    (no ``base_min_size``, as of 2026-11-01), so those are ``None``. A base
    asset ismet cannot represent (``T``) leaves ``base_asset`` ``None``; a quote
    asset it cannot represent raises :class:`~ismet.errors.ValidationError`.
    """
    quote_asset = payload["quote_currency"]
    currency = register_asset(quote_asset)
    if currency is None:
        raise ValidationError(
            f"{symbol}: quote asset {quote_asset!r} is not representable as an "
            "ismet currency code"
        )
    status = _status(payload)
    return Instrument(
        symbol=symbol,
        name=payload["display_name"],
        asset_class=AssetClass.CRYPTO,
        currency=currency,
        tick_size=payload["quote_increment"],
        lot_size=payload["base_increment"],
        min_notional=payload.get("min_market_funds") or None,
        base_asset=register_asset(payload["base_currency"]),
        status=status,
        active=status is ListingStatus.ACTIVE,
    )


def quote_currency_of(product_id: str) -> str | None:
    """Quote asset from a native ``BASE-QUOTE`` id, ``None`` if not representable.

    The id is the venue's own format, so no request is spent on it.
    """
    _, separator, quote_asset = product_id.rpartition("-")
    return register_asset(quote_asset) if separator else None


@_normalizes
def quote_from_ticker(
    frame: Mapping[str, Any],
    symbol: Symbol,
    received: datetime,
    currency: str | None,
) -> Quote:
    """Quote from a ``ticker`` channel frame.

    The channel fires on matches, batching cascades, so the best bid and ask are
    as of the latest match and do not update between trades. The frame's ``side``
    is unused; it is the taker's, the opposite convention to ``matches``.
    """
    exchange_ts, exchange_ns = parse_iso8601_utc(frame["time"])
    return Quote(
        symbol=symbol,
        exchange_ts=exchange_ts,
        exchange_ts_ns=exchange_ns,
        received_ts=received,
        sequence=frame["sequence"],
        source=venue_source(PROVIDER, FEED_TICKER),
        bid=frame["best_bid"],
        bid_size=frame["best_bid_size"],
        ask=frame["best_ask"],
        ask_size=frame["best_ask_size"],
        last=frame["price"],
        last_size=frame["last_size"],
        currency=currency,
    )


_AGGRESSOR_FROM_MAKER = {"buy": Side.SELL, "sell": Side.BUY}


@_normalizes
def trade_from_match(
    frame: Mapping[str, Any],
    symbol: Symbol,
    received: datetime,
    currency: str | None,
) -> Trade:
    """Trade from a ``match`` or ``last_match`` frame of the ``matches`` channel.

    The frame's ``side`` is the maker's, so :attr:`Trade.side`, the aggressor, is
    its opposite.
    """
    exchange_ts, exchange_ns = parse_iso8601_utc(frame["time"])
    return Trade(
        symbol=symbol,
        exchange_ts=exchange_ts,
        exchange_ts_ns=exchange_ns,
        received_ts=received,
        sequence=frame["sequence"],
        source=venue_source(PROVIDER, FEED_MATCHES),
        price=frame["price"],
        size=frame["size"],
        side=_AGGRESSOR_FROM_MAKER[frame["side"]],
        trade_id=str(frame["trade_id"]),
        currency=currency,
    )


def control_error(frame: Mapping[str, Any]) -> VenueError | None:
    """The error in a control frame, if it is one. Coinbase sends no error code."""
    if frame.get("type") != "error":
        return None
    return VenueError(
        f"coinbase feed rejected a request: {frame.get('message')}: "
        f"{frame.get('reason')}",
        payload=frame,
    )


__all__ = [
    "CANDLE_GRANULARITY",
    "FEED_BOOK",
    "FEED_CANDLES",
    "FEED_MATCHES",
    "FEED_TICKER",
    "PROVIDER",
    "bars_from_candles",
    "control_error",
    "instrument_from_product",
    "order_book_from_book",
    "quote_currency_of",
    "quote_from_book",
    "quote_from_ticker",
    "trade_from_match",
]
