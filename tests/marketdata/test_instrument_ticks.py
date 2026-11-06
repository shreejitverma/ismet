"""Tick lookup near a band boundary, where rounding would pick the wrong band."""

from __future__ import annotations

from decimal import Decimal

import pydantic
import pytest

from ismet.models.instrument import TickBand, TickLadder


def test_a_price_just_below_a_boundary_keeps_the_lower_band_at_any_precision() -> None:
    ladder = TickLadder(
        bands=(
            TickBand(from_price=Decimal(0), tick=Decimal("0.01")),
            TickBand(from_price=Decimal(1000), tick=Decimal(1)),
        )
    )
    # 32 significant digits: the ambient context of 28 would round this up to 1000.
    below = Decimal("999.99999999999999999999999999999")
    assert ladder.tick_at(below) == Decimal("0.01")
    assert ladder.tick_at(Decimal(1000)) == Decimal(1)


@pytest.mark.parametrize("value", [0.1, 0.0], ids=["fraction", "zero"])
def test_a_band_refuses_a_float_boundary(value: float) -> None:
    with pytest.raises(pydantic.ValidationError, match="float"):
        TickBand(from_price=value, tick=Decimal(1))  # type: ignore[arg-type]


def test_a_band_takes_text_ints_and_decimals_exactly() -> None:
    for value in ("0.1", Decimal("0.1")):
        assert TickBand(from_price=value, tick="0.1").from_price == Decimal("0.1")  # type: ignore[arg-type]
    assert TickBand(from_price=0, tick="1").from_price == 0  # type: ignore[arg-type]
