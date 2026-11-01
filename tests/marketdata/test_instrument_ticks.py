"""Tick lookup near a band boundary, where rounding would pick the wrong band."""

from __future__ import annotations

from decimal import Decimal

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
