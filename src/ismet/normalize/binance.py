"""Binance Spot payloads to models.

Payload shapes are from https://github.com/binance/binance-spot-api-docs
(``rest-api.md``, ``web-socket-streams.md``, ``filters.md``, ``enums.md``,
``errors.md``), retrieved 2026-11-01, and checked against recorded responses.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
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
)
from ismet.models.types import to_decimal
from ismet.normalize._common import (
    datetime_from_ms,
    normalizer,
    register_asset,
    strict_bool,
    strict_int,
    venue_source,
)

PROVIDER = "binance"
FEED_DEPTH = "rest:depth"
FEED_KLINES = "rest:klines"
FEED_BOOK_TICKER = "ws:bookTicker"
FEED_TRADE = "ws:trade"

KLINE_INTERVALS: Mapping[Interval, str] = {
    Interval.S1: "1s",
    Interval.M1: "1m",
    Interval.M5: "5m",
    Interval.M15: "15m",
    Interval.M30: "30m",
    Interval.H1: "1h",
    Interval.H4: "4h",
    Interval.D1: "1d",
    Interval.W1: "1w",
    Interval.MO1: "1M",
}
"""Every ismet interval is a documented Binance kline interval."""

_STATUS: Mapping[str, ListingStatus] = {
    "TRADING": ListingStatus.ACTIVE,
    "HALT": ListingStatus.HALTED,
    "BREAK": ListingStatus.SUSPENDED,
    "END_OF_DAY": ListingStatus.SUSPENDED,
    "CANCEL_ONLY": ListingStatus.SUSPENDED,
}
"""``enums.md`` symbol statuses. No status means delisted; a delisted pair stays
``BREAK`` indefinitely, so ``SUSPENDED`` is the most this field can claim."""

_MS_PER_SECOND = 1000

_normalizes = normalizer(PROVIDER)


def _level(row: Sequence[Any]) -> Level:
    return Level(price=row[0], size=row[1])


@_normalizes
def quote_from_depth(
    payload: Mapping[str, Any],
    symbol: Symbol,
    received: datetime,
    currency: str | None,
) -> Quote:
    """Top of book from ``GET /api/v3/depth?limit=1``.

    The endpoint has no timestamp, so ``exchange_ts`` is ``received``: it is an
    upper bound on the snapshot's time, not a venue measurement, and ``latency`` is
    zero by construction. See the adapter docstring for why no timestamped endpoint
    is used instead. An empty side is ``None``.
    """
    bids, asks = payload["bids"], payload["asks"]
    best_bid = _level(bids[0]) if bids else None
    best_ask = _level(asks[0]) if asks else None
    return Quote(
        symbol=symbol,
        exchange_ts=received,
        received_ts=received,
        sequence=strict_int(payload["lastUpdateId"]),
        source=venue_source(PROVIDER, FEED_DEPTH),
        bid=best_bid.price if best_bid else None,
        bid_size=best_bid.size if best_bid else None,
        ask=best_ask.price if best_ask else None,
        ask_size=best_ask.size if best_ask else None,
        currency=currency,
    )


@_normalizes
def order_book_from_depth(
    payload: Mapping[str, Any],
    symbol: Symbol,
    received: datetime,
    currency: str | None,
) -> OrderBook:
    """Depth snapshot from ``GET /api/v3/depth``. ``exchange_ts`` is ``received``.

    Same absence of a venue timestamp as :func:`quote_from_depth`.
    ``lastUpdateId`` is the sequence.
    """
    return OrderBook(
        symbol=symbol,
        exchange_ts=received,
        received_ts=received,
        sequence=strict_int(payload["lastUpdateId"]),
        source=venue_source(PROVIDER, FEED_DEPTH),
        bids=tuple(_level(row) for row in payload["bids"]),
        asks=tuple(_level(row) for row in payload["asks"]),
        currency=currency,
    )


@_normalizes
def bars_from_klines(
    rows: Sequence[Sequence[Any]],
    symbol: Symbol,
    interval: Interval,
    received: datetime,
    currency: str | None,
    *,
    as_of: datetime,
) -> list[Bar]:
    """Closed bars from ``GET /api/v3/klines``, ``exchange_ts`` being the open time.

    A kline still open at ``as_of``, when the response was made, is dropped: Binance
    returns it, but its OHLCV is not final and a caller keying on open time would never
    see it change.
    Binance publishes zero-volume klines (``trade_count`` 0) and they are kept.
    Volume is in the base asset. ``vwap`` stays ``None`` rather than being derived
    from quote volume, which would need rounding. A close time that does not end the
    interval means the row is not what it claims, so the payload is refused.
    """
    source = venue_source(PROVIDER, FEED_KLINES)
    bars: list[Bar] = []
    for row in rows:
        open_ms, close_ms = strict_int(row[0]), strict_int(row[6])
        _check_kline_span(open_ms, close_ms, interval)
        if datetime_from_ms(close_ms) >= as_of:
            continue
        opened = datetime_from_ms(open_ms)
        bars.append(
            Bar(
                symbol=symbol,
                exchange_ts=opened,
                received_ts=received,
                source=source,
                interval=interval,
                open=row[1],
                high=row[2],
                low=row[3],
                close=row[4],
                volume=row[5],
                trade_count=row[8],
                currency=currency,
                adjustment=Adjustment.RAW,
                session_date=opened.date() if interval.is_session_based else None,
            )
        )
    return bars


def _check_kline_span(open_ms: int, close_ms: int, interval: Interval) -> None:
    seconds = interval.nominal_seconds
    if close_ms <= open_ms or (
        seconds is not None and close_ms != open_ms + seconds * _MS_PER_SECOND - 1
    ):
        raise ValueError(
            f"{interval.value} kline opening at {open_ms} closes at {close_ms}"
        )


def _filter(payload: Mapping[str, Any], kind: str) -> Mapping[str, Any] | None:
    return next((f for f in payload["filters"] if f["filterType"] == kind), None)


@_normalizes
def instrument_from_symbol_info(
    payload: Mapping[str, Any], symbol: Symbol
) -> Instrument:
    """Instrument from one ``symbols`` entry of ``GET /api/v3/exchangeInfo``.

    Registers both assets. Tick is ``PRICE_FILTER.tickSize``, lot is
    ``LOT_SIZE.stepSize`` (``MARKET_LOT_SIZE`` often reports a zero step and
    describes market orders only), minimum notional is ``NOTIONAL`` or the older
    ``MIN_NOTIONAL``.

    A base asset ismet cannot represent (``T``, ``币安人生``) leaves ``base_asset``
    ``None``. A quote asset it cannot represent (``U``, on 49 pairs when recorded)
    or a tick of zero, which ``filters.md`` allows and means "rule disabled", leaves
    no valid :class:`Instrument`, so :class:`~ismet.errors.ValidationError` is
    raised. Market data for such a symbol still works, with ``currency`` ``None``.
    """
    quote_asset = payload["quoteAsset"]
    currency = register_asset(quote_asset)
    if currency is None:
        raise ValidationError(
            f"{symbol}: quote asset {quote_asset!r} is not representable as an "
            "ismet currency code"
        )
    price_filter = _filter(payload, "PRICE_FILTER")
    lot_filter = _filter(payload, "LOT_SIZE")
    if price_filter is None or lot_filter is None:
        raise ValidationError(f"{symbol}: PRICE_FILTER or LOT_SIZE is absent")
    tick = price_filter["tickSize"]
    if to_decimal(tick) == 0:
        raise ValidationError(f"{symbol}: Binance has disabled the tick size rule")
    notional = _filter(payload, "NOTIONAL") or _filter(payload, "MIN_NOTIONAL")
    status = _STATUS.get(payload["status"], ListingStatus.UNKNOWN)
    return Instrument(
        symbol=symbol,
        name=f"{payload['baseAsset']}/{quote_asset}",
        asset_class=AssetClass.CRYPTO,
        currency=currency,
        tick_size=tick,
        lot_size=lot_filter["stepSize"],
        min_quantity=lot_filter["minQty"],
        max_quantity=lot_filter["maxQty"],
        min_notional=notional["minNotional"] if notional else None,
        base_asset=register_asset(payload["baseAsset"]),
        status=status,
        active=status is ListingStatus.ACTIVE,
    )


def quote_asset_of(payload: Mapping[str, Any]) -> str | None:
    """The market data currency for a symbol, ``None`` if not representable."""
    return register_asset(payload["quoteAsset"])


@_normalizes
def next_kline_start(rows: Sequence[Sequence[Any]]) -> int:
    """``startTime`` for the page after ``rows``: klines are keyed by open time."""
    return strict_int(rows[-1][0]) + 1


@_normalizes
def quote_from_book_ticker(
    frame: Mapping[str, Any],
    symbol: Symbol,
    received: datetime,
    currency: str | None,
) -> Quote:
    """Quote from a ``<symbol>@bookTicker`` frame.

    Binance Spot sends no time in this frame, so ``exchange_ts`` is ``received``,
    and ``u`` is the order book update id. Successive ``u`` values jump by more than
    one in normal operation, because the stream fires only on changes to the top of
    the book, so ``u`` shows order but not loss.
    """
    return Quote(
        symbol=symbol,
        exchange_ts=received,
        received_ts=received,
        sequence=strict_int(frame["u"]),
        source=venue_source(PROVIDER, FEED_BOOK_TICKER),
        bid=frame["b"],
        bid_size=frame["B"],
        ask=frame["a"],
        ask_size=frame["A"],
        currency=currency,
    )


@_normalizes
def trade_from_frame(
    frame: Mapping[str, Any],
    symbol: Symbol,
    received: datetime,
    currency: str | None,
) -> Trade:
    """Trade from a ``<symbol>@trade`` frame.

    ``exchange_ts`` is the trade time ``T``, not the event time ``E``. ``m`` says
    the buyer was the maker, so ``m`` true means the aggressor sold.
    """
    return Trade(
        symbol=symbol,
        exchange_ts=datetime_from_ms(frame["T"]),
        received_ts=received,
        sequence=strict_int(frame["t"]),
        source=venue_source(PROVIDER, FEED_TRADE),
        price=frame["p"],
        size=frame["q"],
        side=Side.SELL if strict_bool(frame["m"]) else Side.BUY,
        trade_id=str(frame["t"]),
        currency=currency,
    )


def control_error(frame: Mapping[str, Any]) -> VenueError | None:
    """The error in a control frame, if it is one.

    Both shapes occur: ``{"error": {"code", "msg"}, "id"}`` was recorded for a
    rejected method, and ``{"code", "msg"}`` is what ``web-socket-streams.md``
    documents. Their absence proves little: subscribing to a stream that does not
    exist is acknowledged with ``{"result": null}``.
    """
    body = frame.get("error", frame)
    if "code" not in body or "msg" not in body:
        return None
    return VenueError(
        f"binance stream rejected a request: {body['msg']}",
        code=str(body["code"]),
        payload=frame,
    )


__all__ = [
    "FEED_BOOK_TICKER",
    "FEED_DEPTH",
    "FEED_KLINES",
    "FEED_TRADE",
    "KLINE_INTERVALS",
    "PROVIDER",
    "bars_from_klines",
    "control_error",
    "instrument_from_symbol_info",
    "next_kline_start",
    "order_book_from_depth",
    "quote_asset_of",
    "quote_from_book_ticker",
    "quote_from_depth",
    "trade_from_frame",
]
