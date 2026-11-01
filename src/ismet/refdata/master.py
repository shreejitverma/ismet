"""A point-in-time security master, and corporate action adjustment.

Questions in finance are time-stamped twice. *What was true on that date*, and *what
did we know on that date*. A backtest that uses today's knowledge of a past ticker
change has look-ahead bias, so every record here has a validity interval and a
``known_from`` time, and :meth:`SecurityMaster.resolve` answers as of both.

Identity follows the security, not the ticker: FB and META are one ``security_id``,
and a recycled ticker maps to different securities at different dates.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum

from ismet.errors import ValidationError
from ismet.models import Adjustment, Bar, Symbol
from ismet.models.decimals import mul, ratio, sub

_FOREVER = date.max
_BEGINNING = datetime(1900, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class Listing:
    """``symbol`` meant ``security_id`` for dates in ``[valid_from, valid_to)``.

    ``known_from`` is when this fact entered the records. A correction is a new
    record with a later ``known_from``, leaving the old one in place, so a past query
    still returns what was believed then.
    """

    security_id: str
    symbol: Symbol
    valid_from: date
    valid_to: date = _FOREVER
    known_from: datetime = _BEGINNING
    isin: str | None = None
    figi: str | None = None

    def __post_init__(self) -> None:
        if self.valid_to <= self.valid_from:
            raise ValidationError(f"listing of {self.symbol} has an empty date range")

    def valid_on(self, day: date) -> bool:
        return self.valid_from <= day < self.valid_to


@dataclass
class SecurityMaster:
    """Listing history, queried as of a date and a knowledge time."""

    listings: list[Listing] = field(default_factory=list)

    def add(self, listing: Listing) -> None:
        self.listings.append(listing)

    def resolve(
        self, symbol: Symbol, on: date, *, known_at: datetime | None = None
    ) -> str | None:
        """The security ``symbol`` meant on ``on``, as known at ``known_at``.

        Where records conflict, the latest known wins. Returns ``None`` if there was
        no such listing on that date.
        """
        candidates = [
            listing
            for listing in self.listings
            if listing.symbol == symbol
            and listing.valid_on(on)
            and (known_at is None or listing.known_from <= known_at)
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda listing: listing.known_from).security_id

    def symbol_on(
        self, security_id: str, on: date, *, known_at: datetime | None = None
    ) -> Symbol | None:
        """The symbol a security traded under on ``on``, or ``None``."""
        candidates = [
            listing
            for listing in self.listings
            if listing.security_id == security_id
            and listing.valid_on(on)
            and (known_at is None or listing.known_from <= known_at)
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda listing: listing.known_from).symbol

    def history(self, security_id: str) -> list[Listing]:
        """Every listing of a security, in date order."""
        return sorted(
            (lst for lst in self.listings if lst.security_id == security_id),
            key=lambda lst: (lst.valid_from, lst.known_from),
        )

    def universe(self, venue: str, on: date) -> list[str]:
        """Securities listed on ``venue`` on ``on``, including since-delisted ones."""
        return sorted(
            {
                listing.security_id
                for listing in self.listings
                if listing.symbol.venue == venue and listing.valid_on(on)
            }
        )


class ActionKind(str, Enum):
    SPLIT = "split"
    CASH_DIVIDEND = "cash_dividend"
    STOCK_DIVIDEND = "stock_dividend"
    RIGHTS = "rights"
    SPIN_OFF = "spin_off"
    MERGER = "merger"
    SYMBOL_CHANGE = "symbol_change"


@dataclass(frozen=True)
class CorporateAction:
    """An action taking effect at the open of ``ex_date``.

    For a split or stock dividend, ``ratio`` is new shares per old share (a 4-for-1
    split is 4). For a cash dividend, ``amount`` is per share. Rights, spin-offs, and
    mergers are recorded for lifecycle purposes, and they are not auto-adjusted here:
    their factors depend on values this module does not have. Supply them to
    :func:`adjust_bars` as an explicit ``price_factor``.
    """

    kind: ActionKind
    ex_date: date
    ratio: Decimal | None = None
    amount: Decimal | None = None
    price_factor: Decimal | None = None
    known_from: datetime = _BEGINNING

    def __post_init__(self) -> None:
        if self.kind in (ActionKind.SPLIT, ActionKind.STOCK_DIVIDEND) and (
            self.ratio is None or self.ratio <= 0
        ):
            raise ValidationError(f"{self.kind.value} requires a positive ratio")
        if self.kind is ActionKind.CASH_DIVIDEND and self.amount is None:
            raise ValidationError("cash dividend requires an amount")


@dataclass(frozen=True)
class _Factors:
    price: Decimal
    volume: Decimal


def _factors_for(
    bar: Bar,
    actions: Sequence[CorporateAction],
    closes_before: dict[date, Decimal],
    *,
    include_dividends: bool,
) -> _Factors:
    price = Decimal(1)
    volume = Decimal(1)
    bar_date = bar.session_date or bar.exchange_ts.date()
    for action in actions:
        if action.ex_date <= bar_date:
            continue
        if action.price_factor is not None:
            price = mul(price, action.price_factor)
        elif action.kind in (ActionKind.SPLIT, ActionKind.STOCK_DIVIDEND):
            assert action.ratio is not None
            price = ratio(price, action.ratio)
            volume = mul(volume, action.ratio)
        elif action.kind is ActionKind.CASH_DIVIDEND and include_dividends:
            assert action.amount is not None
            prior = closes_before.get(action.ex_date)
            if prior is None:
                raise ValidationError(
                    f"no close before ex-date {action.ex_date} to scale the dividend"
                )
            price = mul(price, ratio(sub(prior, action.amount), prior))
    return _Factors(price, volume)


def adjust_bars(
    bars: Iterable[Bar],
    actions: Iterable[CorporateAction],
    *,
    include_dividends: bool = False,
    known_at: datetime | None = None,
) -> list[Bar]:
    """Back-adjust raw bars for actions, so a series is continuous at today's scale.

    Only raw bars are accepted: adjusting data already adjusted is the classic way to
    apply a split twice. ``known_at`` limits the actions to those known at that time,
    which gives the series a backtest would have seen on that date. Dividends use the
    standard ratio method, ``(close_before - dividend) / close_before``, with the raw
    close of the session before the ex-date. Results round to 50 significant digits;
    the raw series remains the source of truth.
    """
    ordered = sorted(bars, key=lambda bar: bar.exchange_ts)
    usable = [a for a in actions if known_at is None or a.known_from <= known_at]
    closes_before: dict[date, Decimal] = {}
    for action in usable:
        before = [
            bar
            for bar in ordered
            if (bar.session_date or bar.exchange_ts.date()) < action.ex_date
        ]
        if before:
            closes_before[action.ex_date] = before[-1].close
    result: list[Bar] = []
    target = Adjustment.SPLIT_DIVIDEND if include_dividends else Adjustment.SPLIT
    for bar in ordered:
        if bar.adjustment is not Adjustment.RAW:
            raise ValidationError(
                f"adjust_bars needs raw bars; got {bar.adjustment.value} at "
                f"{bar.exchange_ts.isoformat()}"
            )
        factors = _factors_for(
            bar, usable, closes_before, include_dividends=include_dividends
        )
        result.append(
            bar.model_copy(
                update={
                    "open": mul(bar.open, factors.price),
                    "high": mul(bar.high, factors.price),
                    "low": mul(bar.low, factors.price),
                    "close": mul(bar.close, factors.price),
                    "volume": mul(bar.volume, factors.volume),
                    "vwap": None if bar.vwap is None else mul(bar.vwap, factors.price),
                    "adjustment": target,
                }
            )
        )
    return result


__all__ = [
    "ActionKind",
    "CorporateAction",
    "Listing",
    "SecurityMaster",
    "adjust_bars",
]
