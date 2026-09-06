from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import BaseModel, ValidationError

from ismet.models.types import (
    Currency,
    Interval,
    Price,
    Quantity,
    Timestamp,
    VenueCode,
    to_decimal,
)


class M(BaseModel):
    price: Price | None = None
    qty: Quantity | None = None
    ts: Timestamp | None = None
    ccy: Currency | None = None
    mic: VenueCode | None = None


def test_to_decimal_accepts_str_int_decimal() -> None:
    assert to_decimal("1.50") == Decimal("1.50")
    assert to_decimal(3) == Decimal(3)
    assert to_decimal(Decimal("2.25")) == Decimal("2.25")
    assert to_decimal(" 7 ") == Decimal(7)


@pytest.mark.parametrize("bad", [1.5, True, False, object(), None])
def test_to_decimal_rejects_non_decimal_types(bad: object) -> None:
    with pytest.raises(TypeError):
        to_decimal(bad)


@pytest.mark.parametrize("bad", ["abc", "NaN", "Infinity", "-inf", ""])
def test_to_decimal_rejects_non_finite_or_garbage(bad: str) -> None:
    with pytest.raises(ValueError):
        to_decimal(bad)


@given(st.decimals(allow_nan=False, allow_infinity=False, places=8))
def test_to_decimal_roundtrips_string(value: Decimal) -> None:
    assert to_decimal(str(value)) == value


def test_float_rejected_at_model_boundary() -> None:
    with pytest.raises(ValidationError, match="float is not accepted"):
        M(price=1.5)  # type: ignore[arg-type]


def test_price_must_be_positive_and_quantity_non_negative() -> None:
    with pytest.raises(ValidationError, match="> 0"):
        M(price="0")
    assert M(qty="0").qty == Decimal(0)
    with pytest.raises(ValidationError, match=">= 0"):
        M(qty="-1")


def test_decimal_serialises_as_string_in_json() -> None:
    assert M(price="10.10").model_dump_json() == (
        '{"price":"10.10","qty":null,"ts":null,"ccy":null,"mic":null}'
    )


def test_naive_datetime_rejected_and_aware_normalised_to_utc() -> None:
    with pytest.raises(ValidationError):
        M(ts=datetime(2026, 1, 1))
    ist = timezone(timedelta(hours=5, minutes=30))
    ts = M(ts=datetime(2026, 1, 1, 9, 30, tzinfo=ist)).ts
    assert ts == datetime(2026, 1, 1, 4, 0, tzinfo=timezone.utc)
    assert ts.tzinfo == timezone.utc


def test_currency_and_mic_validation() -> None:
    assert M(ccy=" usd ").ccy == "USD"
    assert M(ccy="usdt").ccy == "USDT"
    with pytest.raises(ValidationError):
        M(ccy="US")
    assert M(mic="xnas").mic == "XNAS"
    with pytest.raises(ValidationError):
        M(mic="NASDAQ")


def test_interval_seconds() -> None:
    assert Interval.M1.seconds == 60
    assert Interval.D1.seconds == 86400
    assert Interval.MO1.seconds is None
    assert Interval("1h") is Interval.H1
