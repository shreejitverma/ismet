"""Instrument reference data: tick ladders, lots, contract terms, lifecycle."""

from __future__ import annotations

from bisect import bisect_right
from datetime import date
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, ConfigDict, model_validator

from ismet.models.decimals import Rounding, is_multiple, round_to_step
from ismet.models.symbol import Symbol
from ismet.models.types import (
    AssetClass,
    Currency,
    Lei,
    PositiveDecimal,
    Price,
    Quantity,
    Timestamp,
)


class ListingStatus(str, Enum):
    ACTIVE = "active"
    HALTED = "halted"
    SUSPENDED = "suspended"
    DELISTED = "delisted"
    PENDING = "pending"
    UNKNOWN = "unknown"


class OptionRight(str, Enum):
    CALL = "call"
    PUT = "put"


class ExerciseStyle(str, Enum):
    AMERICAN = "american"
    EUROPEAN = "european"
    BERMUDAN = "bermudan"


class SettlementMethod(str, Enum):
    PHYSICAL = "physical"
    CASH = "cash"


class QuoteConvention(str, Enum):
    """How the numbers in a price field relate to money."""

    CURRENCY_UNITS = "currency_units"
    MINOR_UNITS = "minor_units"
    PERCENT_OF_PAR = "percent_of_par"
    THIRTY_SECONDS = "thirty_seconds"
    YIELD = "yield"
    POINTS = "points"


class TickBand(BaseModel):
    """Tick size for prices at or above ``from_price``, until the next band."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    from_price: Decimal
    tick: PositiveDecimal


class TickLadder(BaseModel):
    """Price-banded tick sizes, such as Reg NMS sub-dollar or TSE tables.

    Bands apply to the absolute price. Each boundary must be a multiple of the
    ticks on both sides, so that rounding up to a boundary is itself aligned.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    bands: tuple[TickBand, ...]

    @model_validator(mode="after")
    def _valid(self) -> TickLadder:
        if not self.bands or self.bands[0].from_price != 0:
            raise ValueError("a tick ladder must start with a band at price 0")
        starts = [band.from_price for band in self.bands]
        if starts != sorted(set(starts)):
            raise ValueError("tick bands must ascend by from_price without duplicates")
        for below, above in zip(self.bands, self.bands[1:], strict=False):
            for tick in (below.tick, above.tick):
                if not is_multiple(above.from_price, tick):
                    raise ValueError(
                        f"band boundary {above.from_price} is not a multiple of "
                        f"tick {tick}"
                    )
        return self

    def tick_at(self, price: Decimal) -> Decimal:
        """The tick applying at ``price``."""
        starts = [band.from_price for band in self.bands]
        return self.bands[bisect_right(starts, abs(price)) - 1].tick


class Instrument(BaseModel):
    """Static reference data for a tradable instrument.

    ``tick_size`` is the tick for a flat tick regime and the finest applicable
    tick when ``tick_ladder`` is set; the ladder wins wherever it exists. Option,
    future, and perpetual terms are required together by asset class.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    symbol: Symbol
    name: str
    asset_class: AssetClass = AssetClass.UNKNOWN
    currency: Currency
    tick_size: PositiveDecimal
    tick_ladder: TickLadder | None = None
    lot_size: PositiveDecimal = Decimal(1)
    multiplier: PositiveDecimal = Decimal(1)
    min_quantity: Quantity | None = None
    max_quantity: Quantity | None = None
    min_notional: Quantity | None = None
    quote_convention: QuoteConvention = QuoteConvention.CURRENCY_UNITS
    base_asset: Currency | None = None
    expiry: Timestamp | None = None
    underlying: Symbol | None = None
    option_right: OptionRight | None = None
    strike: Price | None = None
    exercise_style: ExerciseStyle | None = None
    settlement_method: SettlementMethod | None = None
    settlement_cycle_days: int | None = None
    funding_interval_hours: int | None = None
    issuer_lei: Lei | None = None
    listing_date: date | None = None
    delisting_date: date | None = None
    status: ListingStatus = ListingStatus.UNKNOWN
    active: bool = True

    @model_validator(mode="after")
    def _terms(self) -> Instrument:
        if self.asset_class is AssetClass.OPTION:
            missing = [
                name
                for name in ("option_right", "strike", "expiry", "underlying")
                if getattr(self, name) is None
            ]
            if missing:
                raise ValueError(f"option is missing {', '.join(missing)}")
        elif self.option_right is not None or self.strike is not None:
            raise ValueError("option_right and strike apply only to options")
        if self.asset_class is AssetClass.FUTURE and self.expiry is None:
            raise ValueError("future is missing expiry")
        if (
            self.listing_date is not None
            and self.delisting_date is not None
            and self.delisting_date < self.listing_date
        ):
            raise ValueError("delisting_date is before listing_date")
        return self

    def tick_at(self, price: Decimal) -> Decimal:
        """The tick that applies at ``price``."""
        if self.tick_ladder is not None:
            return self.tick_ladder.tick_at(price)
        return self.tick_size

    def is_tick_aligned(self, price: Decimal) -> bool:
        """Whether ``price`` is an exact multiple of the tick at ``price``."""
        return is_multiple(price, self.tick_at(price))

    def is_lot_aligned(self, quantity: Decimal) -> bool:
        """Whether ``quantity`` is an exact multiple of ``lot_size``."""
        return is_multiple(quantity, self.lot_size)

    def round_price(self, price: Decimal, rounding: Rounding) -> Decimal:
        """Round ``price`` to a valid tick in the stated direction.

        There is no default direction because the right one depends on intent:
        a passive buy rounds with ``FLOOR``, a passive sell with ``CEILING``.
        """
        return round_to_step(price, self.tick_at(price), rounding)

    def round_quantity(self, quantity: Decimal) -> Decimal:
        """Round ``quantity`` down to a whole lot. Never rounds a size up."""
        return round_to_step(quantity, self.lot_size, Rounding.TOWARD_ZERO)


__all__ = [
    "ExerciseStyle",
    "Instrument",
    "ListingStatus",
    "OptionRight",
    "QuoteConvention",
    "SettlementMethod",
    "TickBand",
    "TickLadder",
]
