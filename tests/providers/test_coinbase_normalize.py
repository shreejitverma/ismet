"""Field-by-field checks of the Coinbase mapping against recorded payloads."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from itertools import pairwise

import pytest

from ismet.errors import TransportError
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
    datetime_from_ns,
)
from ismet.normalize import coinbase as nc
from ismet.normalize._common import parse_iso8601_utc

from ._support import RECEIVED, body, frames, manifest, session

D = Decimal
BTC = Symbol(venue="ZCBS", ticker="BTC-USD")


def _assert_source(event, feed: str) -> None:  # type: ignore[no-untyped-def]
    assert event.source.provider == "coinbase"
    assert event.source.feed == feed
    assert event.source.kind is FeedKind.VENUE_API
    assert event.source.timeliness is Timeliness.REALTIME


def test_quote_from_book_maps_every_field() -> None:
    payload = body("coinbase", "conformance", "level=1")
    quote = nc.quote_from_book(payload, BTC, RECEIVED, "USD")
    expected_ts, expected_ns = parse_iso8601_utc(payload["time"])
    assert quote.symbol == BTC
    assert quote.bid == D(payload["bids"][0][0])
    assert quote.bid_size == D(payload["bids"][0][1])
    assert quote.ask == D(payload["asks"][0][0])
    assert quote.ask_size == D(payload["asks"][0][1])
    assert quote.sequence == payload["sequence"]
    assert quote.exchange_ts == expected_ts
    assert quote.exchange_ts_ns == expected_ns
    assert quote.received_ts == RECEIVED
    assert quote.last is None
    assert quote.last_size is None
    assert quote.currency == "USD"
    _assert_source(quote, "rest:book")


@pytest.mark.parametrize("level", ["level=1", "level=2"])
def test_a_book_without_a_time_is_refused_rather_than_presented_as_fresh(
    level: str,
) -> None:
    payload = dict(body("coinbase", "conformance", level))
    del payload["time"]
    with pytest.raises(TransportError) as caught:
        if level == "level=1":
            nc.quote_from_book(payload, BTC, RECEIVED, "USD")
        else:
            nc.order_book_from_book(payload, BTC, RECEIVED, 1, "USD")
    assert caught.value.retryable is False


def test_copy_made_at_is_date_minus_age_and_never_after_receipt() -> None:
    received = datetime(2026, 11, 1, 2, 10, 1, tzinfo=timezone.utc)
    headers = {"date": "Sun, 01 Nov 2026 02:10:01 GMT", "age": "82"}
    assert nc.copy_made_at(headers, received) == datetime(
        2026, 11, 1, 2, 8, 39, tzinfo=timezone.utc
    )
    ahead = {"date": "Sun, 01 Nov 2026 02:10:05 GMT"}
    assert nc.copy_made_at(ahead, received) == received
    assert nc.copy_made_at({}, received) == received
    assert nc.copy_made_at({"date": "not a date"}, received) == received
    assert nc.copy_made_at({"date": headers["date"], "age": "x"}, received) == received


def test_order_book_maps_levels_with_counts_and_slices_to_depth() -> None:
    payload = body("coinbase", "conformance", "level=2")
    book = nc.order_book_from_book(payload, BTC, RECEIVED, 5, "USD")
    assert book.bids == tuple(
        Level(price=D(p), size=D(s), count=n) for p, s, n in payload["bids"][:5]
    )
    assert book.asks == tuple(
        Level(price=D(p), size=D(s), count=n) for p, s, n in payload["asks"][:5]
    )
    assert book.sequence == payload["sequence"]
    assert book.exchange_ts == parse_iso8601_utc(payload["time"])[0]
    assert book.exchange_ts_ns == parse_iso8601_utc(payload["time"])[1]
    assert book.received_ts == RECEIVED
    assert book.currency == "USD"
    _assert_source(book, "rest:book")
    deeper = nc.order_book_from_book(payload, BTC, RECEIVED, 500, "USD")
    assert len(deeper.bids) == len(payload["bids"])


def test_candles_map_low_high_open_close_in_vendor_order() -> None:
    rows = body("coinbase", "conformance", "candles")
    bars = nc.bars_from_candles(rows, BTC, Interval.M1, RECEIVED, "USD")
    assert len(bars) == len(rows) > 0
    for row, bar in zip(rows, bars, strict=True):
        assert bar.exchange_ts == datetime_from_ns(row[0] * 1_000_000_000)
        assert bar.low == D(str(row[1]))
        assert bar.high == D(str(row[2]))
        assert bar.open == D(str(row[3]))
        assert bar.close == D(str(row[4]))
        assert bar.volume == D(str(row[5]))
        assert bar.low <= bar.high
        assert bar.interval is Interval.M1
        assert bar.adjustment is Adjustment.RAW
        assert bar.currency == "USD"
        assert bar.received_ts == RECEIVED
        assert bar.trade_count is None
        assert bar.sequence is None
        assert bar.session_date is None
        _assert_source(bar, "rest:candles")


def test_candles_arrive_newest_first() -> None:
    rows = body("coinbase", "conformance", "candles")
    times = [r[0] for r in rows]
    assert times == sorted(times, reverse=True)


def test_daily_candles_carry_a_session_date() -> None:
    rows = body("coinbase", "bars_daily", "candles")
    bars = nc.bars_from_candles(rows, BTC, Interval.D1, RECEIVED, "USD")
    assert bars
    assert all(b.session_date == b.exchange_ts.date() for b in bars)


def test_candle_still_open_at_receipt_is_dropped() -> None:
    rows = body("coinbase", "bars_open", "candles")
    recorded_at = manifest("coinbase", "recorded_at")
    assert max(r[0] for r in rows) + 60 > recorded_at.timestamp()
    bars = nc.bars_from_candles(rows, BTC, Interval.M1, recorded_at, "USD")
    assert len(bars) == sum(r[0] + 60 <= recorded_at.timestamp() for r in rows)
    assert len(bars) < len(rows)


def test_instrument_maps_every_field() -> None:
    product = body("coinbase", "conformance", "products/BTC-USD")
    instrument = nc.instrument_from_product(product, BTC)
    assert instrument.symbol == BTC
    assert instrument.name == product["display_name"]
    assert instrument.asset_class is AssetClass.CRYPTO
    assert instrument.currency == "USD"
    assert instrument.base_asset == "BTC"
    assert instrument.tick_size == D(product["quote_increment"])
    assert instrument.lot_size == D(product["base_increment"])
    assert instrument.min_notional == D(product["min_market_funds"])
    assert instrument.min_quantity is None
    assert instrument.max_quantity is None
    assert instrument.status is ListingStatus.ACTIVE
    assert instrument.active is True


def test_delisted_product_is_delisted() -> None:
    product = body("coinbase", "instruments", "products/ZRX-BTC")
    assert product["status"] == "delisted"
    instrument = nc.instrument_from_product(
        product, Symbol(venue="ZCBS", ticker="ZRX-BTC")
    )
    assert instrument.status is ListingStatus.DELISTED
    assert instrument.active is False


def test_limit_only_product_stays_active() -> None:
    product = body("coinbase", "instruments", "products/BTC-INR")
    assert product["limit_only"] is True
    instrument = nc.instrument_from_product(
        product, Symbol(venue="ZCBS", ticker="BTC-INR")
    )
    assert instrument.status is ListingStatus.ACTIVE
    assert instrument.currency == "INR"


def test_single_letter_base_asset_is_kept() -> None:
    product = body("coinbase", "instruments", "products/T-USD")
    instrument = nc.instrument_from_product(
        product, Symbol(venue="ZCBS", ticker="T-USD")
    )
    assert instrument.base_asset == "T"
    assert instrument.currency == "USD"


@pytest.mark.parametrize(
    ("status", "disabled", "cancel_only", "expected"),
    [
        ("online", True, False, ListingStatus.HALTED),
        ("online", False, True, ListingStatus.SUSPENDED),
        ("offline", False, False, ListingStatus.SUSPENDED),
        ("internal", False, False, ListingStatus.UNKNOWN),
        ("something-new", False, False, ListingStatus.UNKNOWN),
    ],
)
def test_status_mapping_for_states_not_in_the_recording(
    status: str, disabled: bool, cancel_only: bool, expected: ListingStatus
) -> None:
    product = {
        **body("coinbase", "conformance", "products/BTC-USD"),
        "status": status,
        "trading_disabled": disabled,
        "cancel_only": cancel_only,
    }
    instrument = nc.instrument_from_product(product, BTC)
    assert instrument.status is expected
    assert instrument.active is False


def test_ticker_frame_maps_every_field() -> None:
    for frame in frames("coinbase", "ticker"):
        quote = nc.quote_from_ticker(frame, BTC, RECEIVED, "USD")
        expected_ts, expected_ns = parse_iso8601_utc(frame["time"])
        assert quote.bid == D(frame["best_bid"])
        assert quote.bid_size == D(frame["best_bid_size"])
        assert quote.ask == D(frame["best_ask"])
        assert quote.ask_size == D(frame["best_ask_size"])
        assert quote.last == D(frame["price"])
        assert quote.last_size == D(frame["last_size"])
        assert quote.sequence == frame["sequence"]
        assert quote.exchange_ts == expected_ts
        assert quote.exchange_ts_ns == expected_ns
        assert quote.received_ts == RECEIVED
        assert quote.currency == "USD"
        _assert_source(quote, "ws:ticker")


def test_match_frame_maps_every_field_and_inverts_side() -> None:
    received = frames("coinbase", "matches")
    assert {f["side"] for f in received} == {"buy", "sell"}
    for frame in received:
        trade = nc.trade_from_match(frame, BTC, RECEIVED, "USD")
        expected_ts, expected_ns = parse_iso8601_utc(frame["time"])
        assert trade.exchange_ts == expected_ts
        assert trade.exchange_ts_ns == expected_ns
        assert trade.received_ts == RECEIVED
        assert trade.sequence == frame["sequence"]
        assert trade.trade_id == str(frame["trade_id"])
        assert trade.price == D(frame["price"])
        assert trade.size == D(frame["size"])
        assert trade.side is (Side.BUY if frame["side"] == "sell" else Side.SELL)
        assert trade.currency == "USD"
        _assert_source(trade, "ws:matches")


def test_channel_sequence_is_sparse_but_trade_ids_are_consecutive() -> None:
    received = frames("coinbase", "matches")
    numbers = [f["sequence"] for f in received]
    trade_ids = [f["trade_id"] for f in received]
    assert numbers == sorted(numbers)
    assert any(b - a > 1 for a, b in pairwise(numbers))
    assert trade_ids == list(range(trade_ids[0], trade_ids[0] + len(trade_ids)))


def test_matches_channel_opens_with_a_single_last_match() -> None:
    kinds = [f["type"] for f in frames("coinbase", "matches")]
    assert kinds[0] == "last_match"
    assert "last_match" not in kinds[1:]


def test_recorded_feed_rejection_is_a_venue_error() -> None:
    frame = json.loads(session("coinbase", "rejected").control[0])
    error = nc.control_error(frame)
    assert error is not None
    assert error.code is None
    assert "NOPE-USD is not a valid product" in str(error)
    assert error.payload == frame
    empty = json.loads(session("coinbase", "rejected").control[1])
    assert nc.control_error(empty) is None


@pytest.mark.parametrize(
    ("product_id", "expected"),
    [("BTC-USD", "USD"), ("ETH-BTC", "BTC"), ("BTCUSD", None), ("X-XTS", None)],
)
def test_quote_currency_of(product_id: str, expected: str | None) -> None:
    assert nc.quote_currency_of(product_id) == expected


@pytest.mark.parametrize(
    ("call", "payload"),
    [
        (nc.quote_from_book, {"bids": []}),
        (nc.quote_from_ticker, {"time": "not a time"}),
        (nc.trade_from_match, {"side": "sideways"}),
    ],
)
def test_malformed_payloads_become_non_retryable_transport_errors(
    call, payload
) -> None:
    with pytest.raises(TransportError) as caught:
        call(payload, BTC, RECEIVED, "USD")
    assert caught.value.retryable is False


def test_granularity_map_is_documented_values_only() -> None:
    assert set(nc.CANDLE_GRANULARITY.values()) <= {60, 300, 900, 3600, 21600, 86400}
