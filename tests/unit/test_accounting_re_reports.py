"""What the books do when a venue reports a known fill again.

The oracle is ``ismet.execution.state``'s rule for the same reports, and the sums worked
by hand below: the books and the order state must not disagree about one execution.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from ismet.accounting import PositionLedger
from ismet.errors import ValidationError
from ismet.models import Symbol
from ismet.models.orders import Fill, OrderSide

D = Decimal
TS = datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)
SYM = Symbol(venue="ZSIM", ticker="ACME")


def fill(**changes: object) -> Fill:
    fields: dict[str, object] = {
        "fill_id": "F1",
        "client_order_id": "C1",
        "symbol": SYM,
        "side": OrderSide.BUY,
        "price": "100",
        "quantity": "10",
        "ts": TS,
    }
    fields.update(changes)
    return Fill.model_validate(fields)


def test_a_fee_that_arrives_later_is_booked_and_no_longer_unknown() -> None:
    ledger = PositionLedger()
    ledger.apply(fill())
    assert ledger.fills_with_unknown_fee == 1 and ledger.fees == {}
    assert ledger.apply(fill(fee="2", fee_currency="USD")) == []
    assert ledger.fills_with_unknown_fee == 0
    assert ledger.fees == {"USD": D("2")}
    assert ledger.quantity == D(10)


def test_a_fee_reported_again_is_not_booked_again() -> None:
    ledger = PositionLedger()
    ledger.apply(fill())
    ledger.apply(fill(fee="2", fee_currency="USD"))
    ledger.apply(fill(fee="2", fee_currency="USD"))
    ledger.apply(fill())
    assert ledger.fees == {"USD": D("2")} and ledger.fills_with_unknown_fee == 0


def test_a_late_fee_with_no_currency_is_refused_and_changes_nothing() -> None:
    ledger = PositionLedger()
    ledger.apply(fill())
    with pytest.raises(ValidationError, match="no currency"):
        ledger.apply(fill(fee="2"))
    assert ledger.fills_with_unknown_fee == 1 and ledger.fees == {}


@pytest.mark.parametrize(
    "second",
    [
        {"fee": "2", "fee_currency": "USD"},
        {"fee": "1", "fee_currency": "EUR"},
    ],
)
def test_two_different_reported_fees_cannot_both_be_true(
    second: dict[str, str],
) -> None:
    ledger = PositionLedger()
    ledger.apply(fill(fee="1", fee_currency="USD"))
    with pytest.raises(ValidationError, match="different"):
        ledger.apply(fill(**second))
    assert ledger.fees == {"USD": D("1")}


@pytest.mark.parametrize(
    "changed",
    [
        {"price": "101"},
        {"quantity": "12"},
        {"side": OrderSide.SELL},
        {"symbol": Symbol(venue="ZSIM", ticker="OTHER")},
    ],
)
def test_a_report_of_a_different_execution_raises(changed: dict[str, object]) -> None:
    ledger = PositionLedger()
    ledger.apply(fill())
    with pytest.raises(ValidationError, match="different terms"):
        ledger.apply(fill(**changed))
    assert ledger.quantity == D(10)


def test_a_report_with_less_detail_changes_nothing() -> None:
    ledger = PositionLedger()
    ledger.apply(fill(fee="1", fee_currency="USD"))
    assert ledger.apply(fill()) == []
    assert ledger.fees == {"USD": D("1")} and ledger.fills_with_unknown_fee == 0
