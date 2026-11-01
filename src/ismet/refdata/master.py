"""A point-in-time security master, and corporate action adjustment.

Questions in finance are time-stamped twice. *What was true on that date*, and *what
did we know on that date*. A backtest that uses today's knowledge of a past ticker
change has look-ahead bias, so every record here has a validity interval and a
``known_from`` time, and :meth:`SecurityMaster.resolve` answers as of both.

Identity follows the security, not the ticker: FB and META are one ``security_id``,
and a recycled ticker maps to different securities at different dates.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Hashable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import TypeVar

from ismet.calendars import TradingCalendar, get_calendar
from ismet.errors import ValidationError
from ismet.models import Adjustment, Bar, Symbol
from ismet.models.decimals import add, mul, ratio, sub

_FOREVER = date.max
_BEGINNING = datetime(1900, 1, 1, tzinfo=timezone.utc)
_Answer = TypeVar("_Answer", bound=Hashable)


@dataclass(frozen=True)
class Listing:
    """``symbol`` meant ``security_id`` for dates in ``[valid_from, valid_to)``.

    ``known_from`` is when this fact entered the records. A correction is a new
    record for the same ``symbol`` and ``valid_from`` with a later ``known_from``. It
    replaces the old record from that time on, so it can change the security or end the
    range, and the old one stays in place, so a past query still returns what was
    believed then. A correction cannot change ``valid_from``, which is part of what it
    replaces: record the listing with the right start, and take the wrong one back with
    :meth:`SecurityMaster.retract`, which sets ``retracted``.
    """

    security_id: str
    symbol: Symbol
    valid_from: date
    valid_to: date = _FOREVER
    known_from: datetime = _BEGINNING
    isin: str | None = None
    figi: str | None = None
    retracted: bool = False

    def __post_init__(self) -> None:
        if self.valid_to <= self.valid_from:
            raise ValidationError(f"listing of {self.symbol} has an empty date range")

    def valid_on(self, day: date) -> bool:
        return self.valid_from <= day < self.valid_to


def _latest_known(
    candidates: Iterable[Listing], answer: Callable[[Listing], _Answer], subject: str
) -> _Answer | None:
    """The answer from the latest known records. Records that tie must agree."""
    listings = list(candidates)
    if not listings:
        return None
    newest = max(listing.known_from for listing in listings)
    answers = {answer(lst) for lst in listings if lst.known_from == newest}
    if len(answers) > 1:
        raise ValidationError(
            f"records for {subject} known from {newest.isoformat()} conflict: "
            f"{sorted(str(a) for a in answers)}; close one or correct it"
        )
    return answers.pop()


@dataclass
class SecurityMaster:
    """Listing history, queried as of a date and a knowledge time."""

    listings: list[Listing] = field(default_factory=list)

    def add(self, listing: Listing) -> None:
        self.listings.append(listing)

    def retract(
        self, symbol: Symbol, valid_from: date, *, known_from: datetime
    ) -> None:
        """Record that the listing of ``symbol`` from ``valid_from`` was never true.

        Queries stop returning it from ``known_from`` on, and earlier ones still see
        what was believed then. A later record for the same symbol and date reinstates
        it.
        """
        self.add(Listing("", symbol, valid_from, known_from=known_from, retracted=True))

    def _in_force(self, known_at: datetime | None) -> list[Listing]:
        """Records known at ``known_at`` that no later record replaced or retracted."""
        known = [
            listing
            for listing in self.listings
            if known_at is None or listing.known_from <= known_at
        ]
        newest: dict[tuple[Symbol, date], datetime] = {}
        for listing in known:
            key = (listing.symbol, listing.valid_from)
            newest[key] = max(newest.get(key, listing.known_from), listing.known_from)
        current = [
            listing
            for listing in known
            if listing.known_from == newest[(listing.symbol, listing.valid_from)]
        ]
        taken_back = {
            (listing.symbol, listing.valid_from)
            for listing in current
            if listing.retracted
        }
        return [
            listing
            for listing in current
            if (listing.symbol, listing.valid_from) not in taken_back
        ]

    def resolve(
        self, symbol: Symbol, on: date, *, known_at: datetime | None = None
    ) -> str | None:
        """The security ``symbol`` meant on ``on``, as known at ``known_at``.

        Where records conflict, the latest known wins, and a tie between different
        securities raises :class:`~ismet.errors.ValidationError`. Returns ``None`` if
        there was no such listing on that date.
        """
        return _latest_known(
            (
                listing
                for listing in self._in_force(known_at)
                if listing.symbol == symbol and listing.valid_on(on)
            ),
            lambda listing: listing.security_id,
            f"{symbol} on {on}",
        )

    def symbol_on(
        self, security_id: str, on: date, *, known_at: datetime | None = None
    ) -> Symbol | None:
        """The symbol a security traded under on ``on``, or ``None``.

        A tie between different symbols raises, as :meth:`resolve` does.
        """
        return _latest_known(
            (
                listing
                for listing in self._in_force(known_at)
                if listing.security_id == security_id and listing.valid_on(on)
            ),
            lambda listing: listing.symbol,
            f"{security_id} on {on}",
        )

    def history(
        self, security_id: str, *, known_at: datetime | None = None
    ) -> list[Listing]:
        """Every listing of a security in date order, as known at ``known_at``."""
        return sorted(
            (
                listing
                for listing in self._in_force(known_at)
                if listing.security_id == security_id
            ),
            key=lambda listing: (listing.valid_from, listing.known_from),
        )

    def universe(
        self, venue: str, on: date, *, known_at: datetime | None = None
    ) -> list[str]:
        """Securities listed on ``venue`` on ``on``, including since-delisted ones.

        Each symbol resolves by the rule :meth:`resolve` uses, as known at ``known_at``.
        """
        code = venue.strip().upper()
        by_symbol: dict[Symbol, list[Listing]] = {}
        for listing in self._in_force(known_at):
            if listing.symbol.venue == code and listing.valid_on(on):
                by_symbol.setdefault(listing.symbol, []).append(listing)
        found: set[str] = set()
        for symbol, listings in by_symbol.items():
            security = _latest_known(
                listings, lambda lst: lst.security_id, f"{symbol} on {on}"
            )
            assert security is not None
            found.add(security)
        return sorted(found)


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

    A correction is a new record of the same ``kind`` and ``ex_date`` with a later
    ``known_from``. Two cash dividends on one ex-date are both paid only if they share a
    ``known_from``, so a special dividend announced later must be recorded with the
    regular one, or combined with it. Dividends on one ex-date scale by their total.

    A correction replaces only the record with the same ``kind`` and ``ex_date``, so a
    restated ex-date leaves the old record in force. Retract it with a neutral record
    on the old ex-date, also with a later ``known_from``: ``ratio=1`` for a split or
    stock dividend, and ``price_factor=1`` for anything else.
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
    """Held as a fraction, so a 3-for-1 split of 300 is exactly 100."""

    price_numerator: Decimal
    price_denominator: Decimal
    volume: Decimal

    def price(self, value: Decimal) -> Decimal:
        return ratio(mul(value, self.price_numerator), self.price_denominator)


def _rounded(value: Decimal) -> Decimal:
    """Keep adjusted values within what the models accept. The raw bars stay exact."""
    return ratio(value, Decimal(1))


def _dividend_factor(ex_date: date, total: Decimal, prior: Decimal) -> Decimal:
    if prior <= 0:
        raise ValidationError(
            f"the ratio method needs a positive close before ex-date {ex_date}"
        )
    factor = ratio(sub(prior, total), prior)
    if factor <= 0:
        raise ValidationError(
            f"dividends of {total} on {ex_date} are not below the prior close {prior}; "
            "supply an explicit price_factor"
        )
    return factor


def _factors_for(
    bar_date: date,
    actions: Sequence[CorporateAction],
    closes_before: dict[date, Decimal],
    *,
    include_dividends: bool,
) -> _Factors:
    numerator = Decimal(1)
    denominator = Decimal(1)
    volume = Decimal(1)
    dividends: dict[date, Decimal] = {}
    for action in actions:
        if action.ex_date <= bar_date:
            continue
        is_dividend = action.kind is ActionKind.CASH_DIVIDEND
        if is_dividend and not include_dividends:
            continue
        if action.price_factor is not None:
            numerator = _rounded(mul(numerator, action.price_factor))
        elif action.kind in (ActionKind.SPLIT, ActionKind.STOCK_DIVIDEND):
            assert action.ratio is not None
            denominator = _rounded(mul(denominator, action.ratio))
            volume = _rounded(mul(volume, action.ratio))
        elif is_dividend:
            assert action.amount is not None
            total = dividends.get(action.ex_date, Decimal(0))
            dividends[action.ex_date] = add(total, action.amount)
    for ex_date, total in dividends.items():
        factor = _dividend_factor(ex_date, total, closes_before[ex_date])
        numerator = _rounded(mul(numerator, factor))
    return _Factors(numerator, denominator, volume)


def _actions_in_force(actions: Iterable[CorporateAction]) -> list[CorporateAction]:
    """The latest known record of each kind on each ex-date. Duplicates raise."""
    usable = list(actions)
    newest: dict[tuple[ActionKind, date], datetime] = {}
    for action in usable:
        key = (action.kind, action.ex_date)
        newest[key] = max(newest.get(key, action.known_from), action.known_from)
    current = [a for a in usable if a.known_from == newest[(a.kind, a.ex_date)]]
    per_day = Counter(
        (a.kind, a.ex_date) for a in current if a.kind is not ActionKind.CASH_DIVIDEND
    )
    repeats: list[tuple[ActionKind, date]] = [
        key for key, count in per_day.items() if count > 1
    ]
    repeats += [
        (a.kind, a.ex_date) for a, count in Counter(current).items() if count > 1
    ]
    if repeats:
        kind, ex_date = repeats[0]
        raise ValidationError(
            f"more than one {kind.value} on {ex_date} with the same known_from; "
            "correct one with a later known_from, or remove the duplicate"
        )
    return current


def _trading_date(bar: Bar, calendar: TradingCalendar | None) -> date:
    """A bar's venue trading date, which can differ from its UTC date."""
    if bar.session_date is not None:
        return bar.session_date
    resolved = calendar if calendar is not None else get_calendar(bar.symbol.venue)
    return resolved.local_date(bar.exchange_ts)


def _closes_before_dividends(
    ordered: Sequence[Bar],
    dates: Sequence[date],
    actions: Sequence[CorporateAction],
    calendar: TradingCalendar,
) -> dict[date, Decimal]:
    closes: dict[date, Decimal] = {}
    for action in actions:
        needs_close = (
            action.kind is ActionKind.CASH_DIVIDEND
            and action.price_factor is None
            and any(day < action.ex_date for day in dates)
        )
        if not needs_close:
            continue
        prior = calendar.previous_session_date(action.ex_date)
        found = [b for b, day in zip(ordered, dates, strict=True) if day == prior]
        if not found:
            raise ValidationError(
                f"no bar on {prior}, the session before ex-date {action.ex_date}, to "
                "scale the dividend; supply an explicit price_factor"
            )
        closes[action.ex_date] = found[-1].close
    return closes


def adjust_bars(
    bars: Iterable[Bar],
    actions: Iterable[CorporateAction],
    *,
    include_dividends: bool = False,
    known_at: datetime | None = None,
    calendar: TradingCalendar | None = None,
) -> list[Bar]:
    """Back-adjust raw bars for actions, so a series is continuous at today's scale.

    Only raw bars are accepted: adjusting data already adjusted is the classic way to
    apply a split twice. ``known_at`` limits the actions to those known at that time and
    with an ex-date on or before its venue-local date, which gives the series a backtest
    would have seen then. Dividends use the standard ratio method, ``(close_before -
    dividend) / close_before``, with the raw close of the session before the ex-date; a
    series without that bar raises. Results round to 50 significant digits; the raw
    series remains the source of truth. Dates come from ``calendar``, or the bundled
    calendar for the bars' venue.
    """
    ordered = sorted(bars, key=lambda bar: bar.exchange_ts)
    for bar in ordered:
        if bar.adjustment is not Adjustment.RAW:
            raise ValidationError(
                f"adjust_bars needs raw bars; got {bar.adjustment.value} at "
                f"{bar.exchange_ts.isoformat()}"
            )
    if not ordered:
        return []
    venue_calendar = (
        calendar if calendar is not None else get_calendar(ordered[0].symbol.venue)
    )
    known = list(actions)
    if known_at is not None:
        today = venue_calendar.local_date(known_at)
        known = [a for a in known if a.known_from <= known_at and a.ex_date <= today]
    usable = _actions_in_force(known)
    dates = [_trading_date(bar, venue_calendar) for bar in ordered]
    closes_before = (
        _closes_before_dividends(ordered, dates, usable, venue_calendar)
        if include_dividends
        else {}
    )
    result: list[Bar] = []
    target = Adjustment.SPLIT_DIVIDEND if include_dividends else Adjustment.SPLIT
    for bar, day in zip(ordered, dates, strict=True):
        factors = _factors_for(
            day, usable, closes_before, include_dividends=include_dividends
        )
        fields = bar.model_dump()
        for name in ("open", "high", "low", "close", "vwap"):
            if fields[name] is not None:
                fields[name] = factors.price(fields[name])
        fields["volume"] = _rounded(mul(bar.volume, factors.volume))
        fields["adjustment"] = target
        result.append(Bar.model_validate(fields))
    return result


__all__ = [
    "ActionKind",
    "CorporateAction",
    "Listing",
    "SecurityMaster",
    "adjust_bars",
]
