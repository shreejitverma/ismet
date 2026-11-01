from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ismet.accounting import (
    LotMethod,
    PositionLedger,
    build_ledger,
    convert,
    slippage_bps,
    value_books,
)
from ismet.errors import ValidationError
from ismet.models import Symbol
from ismet.models.orders import Fill, OrderSide

D = Decimal
TS = datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)
SYM = Symbol(venue="XSIM", ticker="ACME")
_n = {"i": 0}


def fill(
    side: OrderSide,
    quantity: str,
    price: str,
    fee: str | None = None,
    fee_currency: str = "USD",
) -> Fill:
    _n["i"] += 1
    return Fill(
        fill_id=f"F{_n['i']:06d}",
        client_order_id="C1",
        symbol=SYM,
        side=side,
        price=price,
        quantity=quantity,
        ts=TS + timedelta(seconds=_n["i"]),
        fee=fee,
        fee_currency=None if fee is None else fee_currency,
    )


BUY, SELL = OrderSide.BUY, OrderSide.SELL


def test_fifo_realizes_against_the_oldest_lot() -> None:
    ledger = build_ledger(
        [fill(BUY, "100", "10"), fill(BUY, "100", "20"), fill(SELL, "150", "30")]
    )
    assert ledger.realized_pnl == D(100 * 20 + 50 * 10)
    assert ledger.quantity == 50
    assert ledger.average_open_price == 20
    assert ledger.unrealized_pnl(D(25)) == D(250)


def test_lifo_realizes_against_the_newest_lot() -> None:
    ledger = build_ledger(
        [fill(BUY, "100", "10"), fill(BUY, "100", "20"), fill(SELL, "150", "30")],
        method=LotMethod.LIFO,
    )
    assert ledger.realized_pnl == D(100 * 10 + 50 * 20)
    assert ledger.average_open_price == 10


def test_average_cost_blends_lots() -> None:
    ledger = build_ledger(
        [fill(BUY, "100", "10"), fill(BUY, "100", "20"), fill(SELL, "100", "30")],
        method=LotMethod.AVERAGE,
    )
    assert ledger.realized_pnl == D(1500)
    assert ledger.average_open_price == 15


def test_short_position_profits_when_price_falls() -> None:
    ledger = build_ledger([fill(SELL, "10", "100"), fill(BUY, "4", "90")])
    assert ledger.realized_pnl == D(40)
    assert ledger.quantity == -6
    assert ledger.unrealized_pnl(D(80)) == D(120)


def test_a_fill_through_flat_opens_the_other_side() -> None:
    ledger = build_ledger([fill(BUY, "10", "100"), fill(SELL, "25", "110")])
    assert ledger.realized_pnl == D(100)
    assert ledger.quantity == -15
    assert ledger.lots[0].price == 110


def test_multiplier_scales_money_and_fees_are_tracked_separately() -> None:
    ledger = build_ledger(
        [fill(BUY, "2", "5.00", "1.50"), fill(SELL, "2", "6.00", "1.50")],
        multiplier=D(100),
    )
    assert ledger.realized_pnl == D(200)
    assert ledger.fees == {"USD": D("3.00")}


def test_negative_prices_book_correctly() -> None:
    ledger = build_ledger([fill(BUY, "1", "-37.63"), fill(SELL, "1", "10")])
    assert ledger.realized_pnl == D("47.63")


def test_replayed_fill_is_counted_once() -> None:
    ledger = PositionLedger()
    trade = fill(BUY, "10", "100")
    ledger.apply(trade)
    assert ledger.apply(trade) == []
    assert ledger.quantity == 10


def test_flat_ledger_has_no_average_price() -> None:
    assert PositionLedger().average_open_price is None


quantities = st.integers(min_value=1, max_value=500)
prices = st.decimals(min_value=-100, max_value=500, places=2)
trades = st.lists(
    st.tuples(st.sampled_from([BUY, SELL]), quantities, prices), min_size=1, max_size=30
)


@pytest.mark.parametrize("method", [LotMethod.FIFO, LotMethod.LIFO])
@given(tape=trades, mark=prices)
def test_total_pnl_equals_cash_flow_plus_mark_value(
    method: LotMethod, tape: list[tuple[OrderSide, int, Decimal]], mark: Decimal
) -> None:
    """Whatever the lot method, total P&L is an accounting identity."""
    fills = [fill(side, str(q), str(p)) for side, q, p in tape]
    ledger = build_ledger(fills, method=method)
    cash = sum((-D(f.side.sign) * f.quantity * f.price for f in fills), D(0))
    net = sum((D(f.side.sign) * f.quantity for f in fills), D(0))
    assert ledger.quantity == net
    assert ledger.realized_pnl + ledger.unrealized_pnl(mark) == cash + net * mark
    signs = {lot.quantity > 0 for lot in ledger.lots}
    assert len(signs) <= 1


def test_convert_uses_declared_rates_and_refuses_to_assume_parity() -> None:
    rates = {"EURUSD": D("1.25")}
    assert convert(D(100), "EUR", "USD", rates) == D(125)
    assert convert(D(125), "USD", "EUR", rates) == D(100)
    assert convert(D(5), "USD", "USD", {}) == D(5)
    with pytest.raises(ValidationError, match="no rate"):
        convert(D(1), "JPY", "USD", rates)


def test_value_books_across_currencies_and_requires_a_mark() -> None:
    usd = build_ledger([fill(BUY, "10", "100", "1"), fill(SELL, "5", "110", "1")])
    eur = build_ledger([fill(BUY, "10", "50")])
    result = value_books(
        {"a": (usd, "USD", D(120)), "b": (eur, "EUR", D(60))},
        currency="USD",
        rates={"EURUSD": D(2)},
    )
    assert result.realized == D(50)
    assert result.unrealized == D(100 + 200)
    assert result.fees == D(2)
    assert result.net == D(348)
    assert result.rates_used == {"EURUSD": D(2)}
    with pytest.raises(ValidationError, match="no mark"):
        value_books({"a": (usd, "USD", None)}, currency="USD", rates={})
    closed = build_ledger([fill(BUY, "1", "1"), fill(SELL, "1", "2")])
    assert value_books({"c": (closed, "USD", None)}, currency="USD", rates={}).net == 1


def test_slippage_is_a_cost_on_both_sides() -> None:
    assert slippage_bps(BUY, D(100), D("100.10")) == 10
    assert slippage_bps(SELL, D(100), D("99.90")) == 10
    assert slippage_bps(SELL, D(100), D("100.10")) == -10
    with pytest.raises(ValidationError):
        slippage_bps(BUY, D(0), D(1))
