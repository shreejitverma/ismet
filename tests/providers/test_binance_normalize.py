"""Field-by-field checks of the Binance mapping against recorded payloads."""

from __future__ import annotations

import json
from datetime import date, timedelta
from decimal import Decimal
from itertools import pairwise

import pytest

from ismet.errors import TransportError, ValidationError
from ismet.models import (
    Adjustment,
    AssetClass,
    FeedKind,
    Interval,
    Level,
    ListingStatus,
    Side,
    Symbol,
    Timeliness,
)
from ismet.normalize import binance as nb
from ismet.normalize._common import datetime_from_ms

from ._support import RECEIVED, body, frames, manifest, session

D = Decimal
BTC = Symbol(venue="BNCX", ticker="BTCUSDT")
ETH = Symbol(venue="BNCX", ticker="ETHUSDT")


def _symbol_info(ticker: str):  # type: ignore[no-untyped-def]
    cassette = "conformance" if ticker == "BTCUSDT" else "instruments"
    payload = body("binance", cassette, "exchangeInfo?symbol=" + _encoded(ticker))
    return payload["symbols"][0]


def _encoded(ticker: str) -> str:
    return ticker if ticker.isascii() else "%E5%B8%81%E5%AE%89"


def _filters(info, kind: str):  # type: ignore[no-untyped-def]
    return next(f for f in info["filters"] if f["filterType"] == kind)


def _assert_source(event, feed: str) -> None:  # type: ignore[no-untyped-def]
    assert event.source.provider == "binance"
    assert event.source.feed == feed
    assert event.source.kind is FeedKind.VENUE_API
    assert event.source.timeliness is Timeliness.REALTIME


def test_quote_from_depth_maps_every_field() -> None:
    payload = body("binance", "conformance", "depth?symbol=BTCUSDT&limit=1")
    quote = nb.quote_from_depth(payload, BTC, RECEIVED, "USDT")
    assert quote.symbol == BTC
    assert quote.bid == D(payload["bids"][0][0])
    assert quote.bid_size == D(payload["bids"][0][1])
    assert quote.ask == D(payload["asks"][0][0])
    assert quote.ask_size == D(payload["asks"][0][1])
    assert quote.sequence == payload["lastUpdateId"]
    assert quote.exchange_ts == RECEIVED
    assert quote.received_ts == RECEIVED
    assert quote.exchange_ts_ns is None
    assert quote.last is None
    assert quote.last_size is None
    assert quote.currency == "USDT"
    _assert_source(quote, "rest:depth")


def test_empty_book_on_a_suspended_symbol_is_none_not_zero() -> None:
    payload = body("binance", "empty_book", "depth?symbol=NEOBTC")
    assert payload["bids"] == []
    assert payload["asks"] == []
    quote = nb.quote_from_depth(
        payload, Symbol(venue="BNCX", ticker="NEOBTC"), RECEIVED, "BTC"
    )
    assert quote.bid is None
    assert quote.ask is None
    assert quote.bid_size is None
    assert quote.ask_size is None
    assert quote.sequence == payload["lastUpdateId"]


def test_order_book_maps_every_level() -> None:
    payload = body("binance", "conformance", "depth?symbol=BTCUSDT&limit=5")
    book = nb.order_book_from_depth(payload, BTC, RECEIVED, "USDT")
    assert book.bids == tuple(Level(price=D(p), size=D(s)) for p, s in payload["bids"])
    assert book.asks == tuple(Level(price=D(p), size=D(s)) for p, s in payload["asks"])
    assert len(book.bids) == 5
    assert len(book.asks) == 5
    assert book.sequence == payload["lastUpdateId"]
    assert book.exchange_ts == RECEIVED
    assert book.received_ts == RECEIVED
    assert book.currency == "USDT"
    _assert_source(book, "rest:depth")


def test_klines_map_every_field() -> None:
    rows = body("binance", "conformance", "klines")
    bars = nb.bars_from_klines(rows, BTC, Interval.M1, RECEIVED, "USDT", as_of=RECEIVED)
    assert len(bars) == len(rows) == 30
    for row, bar in zip(rows, bars, strict=True):
        assert bar.exchange_ts == datetime_from_ms(row[0])
        assert bar.open == D(row[1])
        assert bar.high == D(row[2])
        assert bar.low == D(row[3])
        assert bar.close == D(row[4])
        assert bar.volume == D(row[5])
        assert bar.trade_count == row[8]
        assert bar.interval is Interval.M1
        assert bar.adjustment is Adjustment.RAW
        assert bar.currency == "USDT"
        assert bar.received_ts == RECEIVED
        assert bar.sequence is None
        assert bar.vwap is None
        assert bar.session_date is None
        _assert_source(bar, "rest:klines")


def test_daily_klines_carry_a_session_date() -> None:
    rows = body("binance", "bars_daily", "klines")
    bars = nb.bars_from_klines(rows, BTC, Interval.D1, RECEIVED, "USDT", as_of=RECEIVED)
    assert len(bars) == 3
    start = manifest("binance", "bars_daily_start")
    assert [b.session_date for b in bars] == [
        (start + timedelta(days=i)).date() for i in range(3)
    ]
    assert all(isinstance(b.session_date, date) for b in bars)


def test_kline_still_open_at_receipt_is_dropped() -> None:
    rows = body("binance", "bars_open", "klines")
    recorded_at = manifest("binance", "recorded_at")
    closes = [datetime_from_ms(r[6]) for r in rows]
    assert closes[-1] >= recorded_at, "fixture must contain an open kline"
    bars = nb.bars_from_klines(
        rows, BTC, Interval.M1, RECEIVED, "USDT", as_of=recorded_at
    )
    assert len(bars) == sum(c < recorded_at for c in closes)
    assert len(bars) < len(rows)
    later = closes[-1] + timedelta(milliseconds=1)
    closed = nb.bars_from_klines(rows, BTC, Interval.M1, RECEIVED, "USDT", as_of=later)
    assert len(closed) == len(rows)


def test_instrument_maps_every_filter() -> None:
    info = _symbol_info("BTCUSDT")
    instrument = nb.instrument_from_symbol_info(info, BTC)
    lot = _filters(info, "LOT_SIZE")
    assert instrument.symbol == BTC
    assert instrument.name == "BTC/USDT"
    assert instrument.asset_class is AssetClass.CRYPTO
    assert instrument.currency == "USDT"
    assert instrument.base_asset == "BTC"
    assert instrument.tick_size == D(_filters(info, "PRICE_FILTER")["tickSize"])
    assert instrument.lot_size == D(lot["stepSize"])
    assert instrument.min_quantity == D(lot["minQty"])
    assert instrument.max_quantity == D(lot["maxQty"])
    assert instrument.min_notional == D(_filters(info, "NOTIONAL")["minNotional"])
    assert instrument.status is ListingStatus.ACTIVE
    assert instrument.active is True
    assert instrument.tick_ladder is None


def test_lot_comes_from_lot_size_not_market_lot_size() -> None:
    info = _symbol_info("BTCUSDT")
    assert D(_filters(info, "MARKET_LOT_SIZE")["stepSize"]) == 0
    assert nb.instrument_from_symbol_info(info, BTC).lot_size > 0


def test_suspended_listing_is_not_active() -> None:
    info = _symbol_info("NEOBTC")
    assert info["status"] == "BREAK"
    instrument = nb.instrument_from_symbol_info(
        info, Symbol(venue="BNCX", ticker="NEOBTC")
    )
    assert instrument.status is ListingStatus.SUSPENDED
    assert instrument.active is False


@pytest.mark.parametrize("ticker", ["TUSDT", "币安人生USDT"])
def test_unusual_real_base_assets_are_kept(ticker: str) -> None:
    info = _symbol_info(ticker)
    instrument = nb.instrument_from_symbol_info(
        info, Symbol(venue="BNCX", ticker=ticker)
    )
    assert instrument.base_asset == info["baseAsset"]
    assert instrument.currency == "USDT"
    assert info["baseAsset"] in instrument.name


def test_single_letter_quote_asset_is_representable() -> None:
    info = _symbol_info("BTCU")
    assert nb.quote_asset_of(info) == "U"
    instrument = nb.instrument_from_symbol_info(
        info, Symbol(venue="BNCX", ticker="BTCU")
    )
    assert instrument.currency == "U"


def test_unrepresentable_quote_asset_is_an_explicit_refusal() -> None:
    info = {**_symbol_info("BTCU"), "quoteAsset": "XTS"}
    assert nb.quote_asset_of(info) is None
    with pytest.raises(ValidationError, match="not representable"):
        nb.instrument_from_symbol_info(info, Symbol(venue="BNCX", ticker="BTCU"))


def test_a_disabled_tick_rule_refuses_rather_than_inventing_a_tick() -> None:
    info = dict(_symbol_info("BTCUSDT"))
    filters = [dict(f) for f in info["filters"]]
    next(f for f in filters if f["filterType"] == "PRICE_FILTER")["tickSize"] = (
        "0.00000000"
    )
    info["filters"] = filters
    with pytest.raises(ValidationError, match="disabled the tick"):
        nb.instrument_from_symbol_info(info, BTC)


def test_book_ticker_frame_maps_every_field() -> None:
    for frame in frames("binance", "book_ticker"):
        quote = nb.quote_from_book_ticker(frame, BTC, RECEIVED, "USDT")
        assert quote.bid == D(frame["b"])
        assert quote.bid_size == D(frame["B"])
        assert quote.ask == D(frame["a"])
        assert quote.ask_size == D(frame["A"])
        assert quote.sequence == frame["u"]
        assert quote.exchange_ts == RECEIVED
        assert quote.received_ts == RECEIVED
        assert quote.last is None
        assert quote.currency == "USDT"
        _assert_source(quote, "ws:bookTicker")


def test_book_ticker_update_ids_are_sparse_so_jumps_prove_nothing() -> None:
    numbers = [f["u"] for f in frames("binance", "book_ticker")]
    assert numbers == sorted(numbers)
    assert any(b - a > 1 for a, b in pairwise(numbers))


def test_trade_frame_maps_every_field() -> None:
    for frame in frames("binance", "trade"):
        trade = nb.trade_from_frame(frame, BTC, RECEIVED, "USDT")
        assert trade.exchange_ts == datetime_from_ms(frame["T"])
        assert trade.received_ts == RECEIVED
        assert trade.sequence == frame["t"]
        assert trade.trade_id == str(frame["t"])
        assert trade.price == D(frame["p"])
        assert trade.size == D(frame["q"])
        assert trade.side is (Side.SELL if frame["m"] else Side.BUY)
        assert trade.currency == "USDT"
        _assert_source(trade, "ws:trade")


def test_trade_side_is_the_aggressor_on_both_values_of_m() -> None:
    frame = frames("binance", "trade")[0]
    buyer_maker = nb.trade_from_frame({**frame, "m": True}, BTC, RECEIVED, "USDT")
    seller_maker = nb.trade_from_frame({**frame, "m": False}, BTC, RECEIVED, "USDT")
    assert buyer_maker.side is Side.SELL
    assert seller_maker.side is Side.BUY


def test_trade_ids_are_consecutive_per_symbol() -> None:
    received = frames("binance", "trade_multi")
    tickers = {f["s"] for f in received}
    assert tickers == {"BTCUSDT", "ETHUSDT"}
    for ticker in tickers:
        ids = [f["t"] for f in received if f["s"] == ticker]
        assert ids == list(range(ids[0], ids[0] + len(ids)))


def test_recorded_rejection_is_a_venue_error_with_the_raw_code() -> None:
    frame = json.loads(session("binance", "rejected").control[0])
    error = nb.control_error(frame)
    assert error is not None
    assert error.code == "2"
    assert "unknown variant" in str(error)
    assert error.payload == frame


def test_documented_flat_error_shape_is_recognised() -> None:
    frame = {"code": 3, "msg": "Invalid JSON: expected value at line 1 column 1"}
    error = nb.control_error(frame)
    assert error is not None
    assert error.code == "3"


def test_acknowledgement_is_not_an_error() -> None:
    assert nb.control_error({"result": None, "id": 1}) is None
    assert nb.control_error({"e": "serverShutdown", "E": 1770123456789}) is None


@pytest.mark.parametrize(
    ("call", "payload"),
    [
        (nb.quote_from_depth, {"bids": []}),
        (
            nb.order_book_from_depth,
            {"lastUpdateId": 1, "bids": [["x", "1"]], "asks": []},
        ),
        (nb.quote_from_book_ticker, {"u": 1}),
        (nb.trade_from_frame, {"t": 1}),
    ],
)
def test_malformed_payloads_become_non_retryable_transport_errors(
    call, payload
) -> None:
    with pytest.raises(TransportError) as caught:
        call(payload, BTC, RECEIVED, "USDT")
    assert caught.value.retryable is False


def test_float_is_impossible_because_prices_keep_their_text() -> None:
    frame = frames("binance", "trade")[0]
    price = nb.trade_from_frame(frame, BTC, RECEIVED, "USDT").price
    assert str(price) == frame["p"]


def test_interval_map_covers_only_documented_values() -> None:
    documented = {"1s", "1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"}
    assert set(nb.KLINE_INTERVALS.values()) == documented
    assert set(nb.KLINE_INTERVALS) == set(Interval)
