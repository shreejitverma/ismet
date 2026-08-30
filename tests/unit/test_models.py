from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from ismet.models import (
    AssetClass,
    Bar,
    Instrument,
    Interval,
    Level,
    OrderBook,
    Quote,
    Side,
    Symbol,
    Trade,
)

D = Decimal


def test_symbol_parse_and_key() -> None:
    s = Symbol.parse("acme@xmok")
    assert s.key == ("XMOK", "acme")
    assert str(s) == "acme@XMOK"
    assert Symbol.parse("ACME", venue="xmok").venue == "XMOK"
    assert Symbol.parse("ACME@XMOK", venue="XMOK").ticker == "ACME"
    with pytest.raises(ValueError, match="venue mismatch"):
        Symbol.parse("ACME@XNAS", venue="XMOK")
    with pytest.raises(ValueError, match="has no venue"):
        Symbol.parse("ACME")
    with pytest.raises(ValidationError):
        Symbol(venue="XMOK", ticker="")


def test_symbol_is_hashable_and_frozen(sym: Symbol) -> None:
    assert {sym: 1}[Symbol(venue="XMOK", ticker="ACME")] == 1
    with pytest.raises(ValidationError):
        sym.ticker = "X"  # type: ignore[misc]


def test_quote_mid_spread_and_crossed(sym: Symbol, now: datetime) -> None:
    q = Quote(symbol=sym, exchange_ts=now, received_ts=now, bid="10", ask="10.02")
    assert q.mid == D("10.01")
    assert q.spread == D("0.02")
    assert Quote(symbol=sym, exchange_ts=now, received_ts=now).mid is None
    with pytest.raises(ValidationError, match="crossed"):
        Quote(symbol=sym, exchange_ts=now, received_ts=now, bid="11", ask="10")


def test_exchange_ts_after_received_ts_rejected(sym: Symbol, now: datetime) -> None:
    with pytest.raises(ValidationError, match="after received_ts"):
        Trade(
            symbol=sym,
            exchange_ts=now + timedelta(seconds=1),
            received_ts=now,
            price="1",
            size="1",
        )


def test_trade_defaults(sym: Symbol, now: datetime) -> None:
    t = Trade(symbol=sym, exchange_ts=now, received_ts=now, price="5", size="0")
    assert t.side is Side.UNKNOWN
    assert t.conditions == ()
    with pytest.raises(ValidationError):
        Trade(
            symbol=sym, exchange_ts=now, received_ts=now, price="5", size="1", extra=1
        )  # type: ignore[call-arg]


def test_bar_ohlc_consistency(sym: Symbol, now: datetime) -> None:
    base = {
        "symbol": sym,
        "exchange_ts": now,
        "received_ts": now,
        "interval": Interval.M1,
    }
    Bar(**base, open="10", high="11", low="9", close="10.5", volume="100")
    with pytest.raises(ValidationError, match="open"):
        Bar(**base, open="12", high="11", low="9", close="10", volume="1")
    with pytest.raises(ValidationError, match="close"):
        Bar(**base, open="10", high="11", low="9", close="8", volume="1")


def test_order_book_ordering_and_best_levels(sym: Symbol, now: datetime) -> None:
    base = {"symbol": sym, "exchange_ts": now, "received_ts": now}
    book = OrderBook(
        **base,
        bids=(Level(price="10", size="1"), Level(price="9", size="2")),
        asks=(Level(price="11", size="1"), Level(price="12", size="2")),
    )
    assert book.best_bid is not None and book.best_bid.price == D(10)
    assert book.best_ask is not None and book.best_ask.price == D(11)
    assert OrderBook(**base).best_bid is None
    with pytest.raises(ValidationError, match="descending"):
        OrderBook(
            **base, bids=(Level(price="9", size="1"), Level(price="10", size="1"))
        )
    with pytest.raises(ValidationError, match="ascending"):
        OrderBook(
            **base, asks=(Level(price="12", size="1"), Level(price="11", size="1"))
        )
    with pytest.raises(ValidationError, match="crossed book"):
        OrderBook(
            **base,
            bids=(Level(price="12", size="1"),),
            asks=(Level(price="11", size="1"),),
        )


def test_instrument_alignment_helpers(sym: Symbol) -> None:
    inst = Instrument(
        symbol=sym,
        name="Acme",
        asset_class=AssetClass.EQUITY,
        currency="usd",
        tick_size="0.05",
        lot_size="100",
    )
    assert inst.currency == "USD"
    assert inst.is_tick_aligned(D("10.10"))
    assert not inst.is_tick_aligned(D("10.11"))
    assert inst.is_lot_aligned(D(300))
    assert not inst.is_lot_aligned(D(150))
    assert inst.round_price(D("10.13")) == D("10.10")
    assert inst.multiplier == D(1)
