"""Trading calendars: sessions, holidays, half-days, breaks, and settlement dates.

Bundled venue data is a snapshot derived from ``exchange_calendars`` by
``scripts/update_calendars.py``. Each calendar knows the dates its data covers, and
a query outside them raises :class:`~ismet.errors.CalendarOutOfRange`. Treating an
unknown future date as an ordinary weekday is how a system sends orders into a
holiday, so it is not done.

Holidays can change after a release. :meth:`TradingCalendar.with_closure` and
:meth:`TradingCalendar.with_hours` apply an announced change immediately, and
:func:`load_calendar_file` replaces a venue's data without a package upgrade.

Sessions cover regular hours and intraday breaks. Pre-open, auction, and extended
sessions are venue-specific and are not modelled yet.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from functools import lru_cache
from importlib import resources
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ismet.errors import CalendarOutOfRange, ValidationError

_ONE_DAY = timedelta(days=1)


class MarketPhase(str, Enum):
    """What a venue is doing at an instant."""

    CLOSED = "closed"
    OPEN = "open"
    BREAK = "break"


@dataclass(frozen=True)
class _Hours:
    open: time
    close: time
    break_start: time | None = None
    break_end: time | None = None


@dataclass(frozen=True)
class Session:
    """One trading session. Instants are UTC; ``date`` is the venue trading date."""

    venue: str
    date: date
    open: datetime
    close: datetime
    break_start: datetime | None = None
    break_end: datetime | None = None
    early_close: bool = False
    late_open: bool = False

    def phase_at(self, instant: datetime) -> MarketPhase:
        if not self.open <= instant < self.close:
            return MarketPhase.CLOSED
        if (
            self.break_start is not None
            and self.break_end is not None
            and self.break_start <= instant < self.break_end
        ):
            return MarketPhase.BREAK
        return MarketPhase.OPEN


def _aware(instant: datetime) -> datetime:
    if instant.tzinfo is None:
        raise ValidationError("calendar queries require a timezone-aware datetime")
    return instant.astimezone(timezone.utc)


def _parse_time(text: str | None) -> time | None:
    return None if text is None else time.fromisoformat(text)


def _hours_from(values: list[str | None]) -> _Hours:
    open_text, close_text, break_start, break_end = values
    if open_text is None or close_text is None:
        raise ValueError("calendar data has a session without open or close")
    return _Hours(
        time.fromisoformat(open_text),
        time.fromisoformat(close_text),
        _parse_time(break_start),
        _parse_time(break_end),
    )


class TradingCalendar:
    """Sessions for one venue over a known range of dates. Immutable."""

    def __init__(
        self,
        venue: str,
        tz: str,
        first: date,
        last: date,
        eras: tuple[tuple[date, _Hours], ...],
        closed: frozenset[date],
        extra: frozenset[date],
        special: dict[date, _Hours],
        *,
        source: str = "",
    ) -> None:
        if not eras:
            raise ValueError(f"calendar {venue} has no regular hours")
        self.venue = venue
        self.tz = ZoneInfo(tz)
        self.first_date = first
        self.last_date = last
        self.source = source
        self._eras = eras
        self._era_starts = [start for start, _ in eras]
        self._closed = closed
        self._extra = extra
        self._special = dict(special)

    def __repr__(self) -> str:
        return (
            f"TradingCalendar({self.venue}, {self.tz.key}, "
            f"{self.first_date}..{self.last_date})"
        )

    # Editing returns a new calendar, so a shared instance can never change.

    def _replace(
        self,
        *,
        closed: frozenset[date] | None = None,
        extra: frozenset[date] | None = None,
        special: dict[date, _Hours] | None = None,
    ) -> TradingCalendar:
        return TradingCalendar(
            self.venue,
            self.tz.key,
            self.first_date,
            self.last_date,
            self._eras,
            self._closed if closed is None else closed,
            self._extra if extra is None else extra,
            self._special if special is None else special,
            source=self.source,
        )

    def with_closure(self, day: date) -> TradingCalendar:
        """A copy with ``day`` closed, for an announced unscheduled closure."""
        self._check_range(day)
        special = {d: h for d, h in self._special.items() if d != day}
        return self._replace(
            closed=self._closed | {day}, extra=self._extra - {day}, special=special
        )

    def with_hours(
        self,
        day: date,
        open_time: time,
        close_time: time,
        *,
        break_start: time | None = None,
        break_end: time | None = None,
    ) -> TradingCalendar:
        """A copy with special venue-local hours on ``day``, opening it if closed."""
        self._check_range(day)
        if not open_time < close_time:
            raise ValidationError("open must be before close")
        hours = _Hours(open_time, close_time, break_start, break_end)
        extra = self._extra | {day} if day.weekday() >= 5 else self._extra
        return self._replace(
            closed=self._closed - {day},
            extra=extra,
            special={**self._special, day: hours},
        )

    # Dates

    def _check_range(self, day: date) -> None:
        if not self.first_date <= day <= self.last_date:
            raise CalendarOutOfRange(
                f"{self.venue} calendar data covers {self.first_date} to "
                f"{self.last_date}; {day} is outside it. Update ismet, or supply "
                "newer data with load_calendar_file()"
            )

    def _is_session_unchecked(self, day: date) -> bool:
        if day.weekday() >= 5:
            return day in self._extra
        return day not in self._closed

    def is_session(self, day: date) -> bool:
        """Whether ``day`` is a trading date."""
        self._check_range(day)
        return self._is_session_unchecked(day)

    def _era_hours(self, day: date) -> _Hours:
        best = self._eras[0][1]
        for start, hours in self._eras:
            if start > day:
                break
            best = hours
        return best

    def _at(self, day: date, moment: time) -> datetime:
        return datetime.combine(day, moment, tzinfo=self.tz).astimezone(timezone.utc)

    def session(self, day: date) -> Session | None:
        """The session on ``day``, or ``None`` if the venue does not trade."""
        if not self.is_session(day):
            return None
        regular = self._era_hours(day)
        hours = self._special.get(day, regular)
        return Session(
            self.venue,
            day,
            self._at(day, hours.open),
            self._at(day, hours.close),
            None if hours.break_start is None else self._at(day, hours.break_start),
            None if hours.break_end is None else self._at(day, hours.break_end),
            early_close=hours.close < regular.close,
            late_open=hours.open > regular.open,
        )

    def sessions(self, start: date, end: date) -> list[Session]:
        """Sessions with ``start <= date <= end``."""
        self._check_range(start)
        self._check_range(end)
        out: list[Session] = []
        day = start
        while day <= end:
            found = self.session(day)
            if found is not None:
                out.append(found)
            day += _ONE_DAY
        return out

    def closed_weekdays(self, start: date, end: date) -> list[date]:
        """Weekdays in the range with no session: holidays and closures."""
        self._check_range(start)
        self._check_range(end)
        count = (end - start).days + 1
        days = (start + timedelta(days=i) for i in range(count))
        return [d for d in days if d.weekday() < 5 and d in self._closed]

    def next_session_date(self, day: date) -> date:
        """The first trading date strictly after ``day``."""
        candidate = day + _ONE_DAY
        while not self.is_session(candidate):
            candidate += _ONE_DAY
        return candidate

    def previous_session_date(self, day: date) -> date:
        """The last trading date strictly before ``day``."""
        candidate = day - _ONE_DAY
        while not self.is_session(candidate):
            candidate -= _ONE_DAY
        return candidate

    def add_sessions(self, day: date, count: int) -> date:
        """Move ``count`` trading dates from ``day``; negative moves backward.

        ``day`` itself need not be a trading date. With ``count == 0`` it must be.
        """
        if count == 0:
            if not self.is_session(day):
                raise ValidationError(f"{day} is not a {self.venue} session")
            return day
        current = day
        step = self.next_session_date if count > 0 else self.previous_session_date
        for _ in range(abs(count)):
            current = step(current)
        return current

    def settlement_date(self, trade_date: date, cycle_days: int) -> date:
        """Trade date plus ``cycle_days`` venue business days (T+n).

        Securities settlement can also depend on depository and currency holidays,
        which this calendar does not model; confirm against the clearing house for
        anything that moves cash.
        """
        if cycle_days < 0:
            raise ValidationError("cycle_days must be >= 0")
        return self.add_sessions(trade_date, cycle_days)

    # Instants

    def _local_date(self, instant: datetime) -> date:
        return instant.astimezone(self.tz).date()

    def session_of(self, instant: datetime) -> Session | None:
        """The session containing ``instant``, including its break, or ``None``."""
        utc = _aware(instant)
        session = self.session(self._local_date(utc))
        if session is not None and session.open <= utc < session.close:
            return session
        return None

    def phase_at(self, instant: datetime) -> MarketPhase:
        utc = _aware(instant)
        session = self.session(self._local_date(utc))
        return MarketPhase.CLOSED if session is None else session.phase_at(utc)

    def is_open(self, instant: datetime) -> bool:
        """Whether the venue is in continuous trading: open and not on a break."""
        return self.phase_at(instant) is MarketPhase.OPEN

    def next_open(self, instant: datetime) -> datetime:
        """The first time strictly after ``instant`` the venue starts trading.

        A return from an intraday break counts as an open.
        """
        utc = _aware(instant)
        day = self._local_date(utc)
        while True:
            session = self.session(day)
            if session is not None:
                if session.open > utc:
                    return session.open
                if session.break_end is not None and session.break_end > utc:
                    return session.break_end
            day += _ONE_DAY

    def next_close(self, instant: datetime) -> datetime:
        """The first time strictly after ``instant`` the venue stops trading.

        The start of an intraday break counts as a close.
        """
        utc = _aware(instant)
        day = self._local_date(utc)
        while True:
            session = self.session(day)
            if session is not None:
                if session.break_start is not None and session.break_start > utc:
                    return session.break_start
                if session.close > utc:
                    return session.close
            day += _ONE_DAY


class AlwaysOpenCalendar(TradingCalendar):
    """A 24x7 market: every UTC date is one session, for crypto spot venues."""

    def __init__(self, venue: str, *, source: str = "continuous market") -> None:
        hours = _Hours(time(0, 0), time(23, 59, 59, 999999))
        super().__init__(
            venue,
            "UTC",
            date(2009, 1, 3),
            date(2199, 12, 31),
            ((date(2009, 1, 3), hours),),
            frozenset(),
            frozenset(),
            {},
            source=source,
        )

    def _is_session_unchecked(self, day: date) -> bool:  # noqa: ARG002
        return True


def calendar_from_dict(
    venue: str, data: dict[str, Any], *, source: str = ""
) -> TradingCalendar:
    """Build a calendar from the snapshot encoding used by the bundled data."""
    return TradingCalendar(
        venue,
        data["tz"],
        date.fromisoformat(data["first"]),
        date.fromisoformat(data["last"]),
        tuple(
            (date.fromisoformat(era[0]), _hours_from(list(era[1:])))
            for era in data["eras"]
        ),
        frozenset(date.fromisoformat(d) for d in data["closed"]),
        frozenset(date.fromisoformat(d) for d in data["extra"]),
        {
            date.fromisoformat(d): _hours_from(list(hours))
            for d, hours in data["special"].items()
        },
        source=source,
    )


@lru_cache(maxsize=1)
def _snapshot() -> dict[str, Any]:
    text = (
        resources.files("ismet.calendars")
        .joinpath("data")
        .joinpath("calendars.json")
        .read_text(encoding="utf-8")
    )
    loaded: dict[str, Any] = json.loads(text)
    return loaded


_overrides: dict[str, TradingCalendar] = {}


@lru_cache(maxsize=64)
def _bundled(code: str) -> TradingCalendar:
    snapshot = _snapshot()
    target = snapshot["aliases"].get(code, code)
    return calendar_from_dict(
        code, snapshot["venues"][target], source=snapshot["source"]
    )


def available_calendars() -> list[str]:
    """Venue codes with bundled or registered calendars."""
    snapshot = _snapshot()
    return sorted({*snapshot["venues"], *snapshot["aliases"], *_overrides})


def get_calendar(venue: str) -> TradingCalendar:
    """The calendar for ``venue``. Registered calendars win over bundled data."""
    code = venue.strip().upper()
    if code in _overrides:
        return _overrides[code]
    snapshot = _snapshot()
    if code in snapshot["venues"] or code in snapshot["aliases"]:
        return _bundled(code)
    raise ValidationError(
        f"no trading calendar for {code}; have {available_calendars()}. "
        "Use register_calendar() to supply one, or AlwaysOpenCalendar for a 24x7 venue"
    )


def register_calendar(calendar: TradingCalendar) -> None:
    """Make ``calendar`` the one :func:`get_calendar` returns for its venue."""
    _overrides[calendar.venue] = calendar


def load_calendar_file(path: str | Path, venue: str) -> TradingCalendar:
    """Read one venue's calendar from a JSON file and register it.

    The file uses the bundled encoding, documented in ``scripts/update_calendars.py``.
    """
    file_path = Path(path)
    data = json.loads(file_path.read_text(encoding="utf-8"))
    calendar = calendar_from_dict(venue.upper(), data, source=str(file_path))
    register_calendar(calendar)
    return calendar


for _code, _name in (
    ("XMOK", "mock venue"),
    ("XSIM", "simulated venue"),
    ("BNCX", "Binance spot trades continuously"),
    ("ZCBS", "Coinbase Exchange spot trades continuously"),
):
    _overrides[_code] = AlwaysOpenCalendar(_code, source=_name)


__all__ = [
    "AlwaysOpenCalendar",
    "MarketPhase",
    "Session",
    "TradingCalendar",
    "available_calendars",
    "calendar_from_dict",
    "get_calendar",
    "load_calendar_file",
    "register_calendar",
]
