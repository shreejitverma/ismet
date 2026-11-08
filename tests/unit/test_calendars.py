"""Known answers checked against exchange notices, independent of the snapshot."""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ismet.calendars import (
    AlwaysOpenCalendar,
    MarketPhase,
    available_calendars,
    get_calendar,
    load_calendar_file,
    register_calendar,
)
from ismet.errors import CalendarOutOfRange, ValidationError

NY = ZoneInfo("America/New_York")
UTC = timezone.utc
XNYS = get_calendar("XNYS")


@pytest.mark.parametrize(
    "day",
    [
        date(2025, 1, 1),  # New Year
        date(2025, 1, 9),  # National day of mourning, President Carter
        date(2022, 6, 20),  # Juneteenth, first observed
        date(2024, 3, 29),  # Good Friday
        date(2021, 12, 24),  # Christmas observed
        date(2012, 10, 29),  # Hurricane Sandy
        date(2026, 11, 26),  # Thanksgiving
    ],
)
def test_nyse_closures(day: date) -> None:
    assert XNYS.session(day) is None
    assert day in XNYS.closed_weekdays(day, day)


@pytest.mark.parametrize(
    "day",
    [date(2024, 7, 3), date(2026, 11, 27), date(2023, 11, 24), date(2024, 12, 24)],
)
def test_nyse_half_days_close_at_1pm_eastern(day: date) -> None:
    session = XNYS.session(day)
    assert session is not None and session.early_close
    assert session.close == datetime.combine(day, time(13, 0), tzinfo=NY)
    assert session.open == datetime.combine(day, time(9, 30), tzinfo=NY)


def test_open_tracks_daylight_saving_in_utc() -> None:
    winter = XNYS.session(date(2026, 3, 6))
    summer = XNYS.session(date(2026, 3, 9))
    assert winter is not None and summer is not None
    assert winter.open.hour == 14 and summer.open.hour == 13
    assert not summer.early_close


def test_is_open_next_open_next_close() -> None:
    friday_close = datetime(2026, 3, 6, 21, 0, tzinfo=UTC)
    assert not XNYS.is_open(friday_close)
    assert XNYS.next_open(friday_close) == datetime(2026, 3, 9, 13, 30, tzinfo=UTC)
    during = datetime(2026, 3, 9, 15, 0, tzinfo=UTC)
    assert XNYS.is_open(during)
    assert XNYS.phase_at(during) is MarketPhase.OPEN
    assert XNYS.next_close(during) == datetime(2026, 3, 9, 20, 0, tzinfo=UTC)
    assert XNYS.session_of(during) is not None
    assert XNYS.session_of(friday_close) is None
    at_open = datetime(2026, 3, 9, 13, 30, tzinfo=UTC)
    assert XNYS.is_open(at_open)
    assert XNYS.next_open(at_open) > at_open
    at_close = datetime(2026, 3, 9, 20, 0, tzinfo=UTC)
    assert not XNYS.is_open(at_close)


def test_lunch_breaks_on_asian_venues() -> None:
    hkex = get_calendar("XHKG")
    tokyo = get_calendar("XTKS")
    lunch = datetime(2026, 3, 9, 4, 30, tzinfo=UTC)  # 12:30 Hong Kong
    assert hkex.phase_at(lunch) is MarketPhase.BREAK
    assert not hkex.is_open(lunch)
    assert hkex.session_of(lunch) is not None
    assert hkex.next_open(lunch) == datetime(2026, 3, 9, 5, 0, tzinfo=UTC)
    morning = datetime(2026, 3, 9, 2, 0, tzinfo=UTC)
    assert hkex.next_close(morning) == datetime(2026, 3, 9, 4, 0, tzinfo=UTC)
    session = tokyo.session(date(2026, 3, 9))
    assert session is not None and session.break_start is not None


def test_india_calendars_include_republic_day() -> None:
    for code in ("XBOM", "XNSE"):
        calendar = get_calendar(code)
        assert not calendar.is_session(date(2026, 1, 26))
        session = calendar.session(date(2026, 3, 9))
        assert session is not None
        assert session.open == datetime(2026, 3, 9, 3, 45, tzinfo=UTC)
        assert session.close == datetime(2026, 3, 9, 10, 0, tzinfo=UTC)


def test_out_of_range_query_raises_rather_than_guessing() -> None:
    assert XNYS.first_date == date(2007, 1, 3)
    for day in (XNYS.last_date + timedelta(days=1), date(1999, 1, 4)):
        with pytest.raises(CalendarOutOfRange, match="covers"):
            XNYS.is_session(day)
    with pytest.raises(CalendarOutOfRange):
        XNYS.next_open(datetime.combine(XNYS.last_date, time(23), tzinfo=NY))


def test_naive_datetime_is_rejected() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        XNYS.is_open(datetime(2026, 3, 9, 15, 0))


def test_session_arithmetic_and_settlement() -> None:
    friday = date(2026, 3, 6)
    assert XNYS.add_sessions(friday, 1) == date(2026, 3, 9)
    assert XNYS.add_sessions(date(2026, 3, 9), -1) == friday
    assert XNYS.add_sessions(friday, 0) == friday
    with pytest.raises(ValidationError):
        XNYS.add_sessions(date(2026, 3, 7), 0)
    # T+1 across Thanksgiving: Wednesday trade settles on Friday.
    assert XNYS.settlement_date(date(2026, 11, 25), 1) == date(2026, 11, 27)
    with pytest.raises(ValidationError):
        XNYS.settlement_date(friday, -1)
    sessions = XNYS.sessions(date(2026, 11, 23), date(2026, 11, 27))
    assert [s.date.day for s in sessions] == [23, 24, 25, 27]


def test_with_closure_and_hours_copy_rather_than_mutate() -> None:
    day = date(2026, 3, 9)
    closed = XNYS.with_closure(day)
    assert closed.session(day) is None
    assert XNYS.session(day) is not None
    shortened = XNYS.with_hours(day, time(9, 30), time(12, 0))
    session = shortened.session(day)
    assert session is not None and session.early_close
    assert closed.with_hours(day, time(10, 0), time(12, 0)).is_session(day)
    saturday = date(2026, 3, 7)
    weekend = XNYS.with_hours(saturday, time(10, 0), time(12, 0))
    assert weekend.is_session(saturday)
    assert (
        XNYS.with_hours(saturday, time(10, 0), time(12, 0))
        .with_closure(saturday)
        .session(saturday)
        is None
    )
    with pytest.raises(ValidationError):
        XNYS.with_hours(day, time(12, 0), time(9, 0))
    with pytest.raises(CalendarOutOfRange):
        XNYS.with_closure(date(1990, 1, 2))


def test_always_open_calendar() -> None:
    crypto = get_calendar("BNCX")
    assert isinstance(crypto, AlwaysOpenCalendar)
    sunday = datetime(2026, 3, 8, 3, 0, tzinfo=UTC)
    assert crypto.is_open(sunday)
    assert crypto.is_session(date(2026, 3, 8))
    assert crypto.closed_weekdays(date(2026, 3, 2), date(2026, 3, 8)) == []


def test_registry_and_file_override(tmp_path: Path) -> None:
    assert {"XNYS", "XNAS", "XNSE", "XBOM", "XLON", "BNCX"} <= set(
        available_calendars()
    )
    assert get_calendar("xnas").session(date(2026, 3, 9)) is not None
    with pytest.raises(ValidationError, match="no trading calendar"):
        get_calendar("XOFF")
    path = tmp_path / "custom.json"
    path.write_text(
        json.dumps(
            {
                "tz": "UTC",
                "first": "2030-01-01",
                "last": "2030-12-31",
                "eras": [["2030-01-01", "09:00", "17:00", None, None]],
                "closed": ["2030-01-01"],
                "extra": [],
                "special": {"2030-01-02": ["10:00", "12:00", None, None]},
            }
        ),
        encoding="utf-8",
    )
    calendar = load_calendar_file(path, "ZOVR")
    assert get_calendar("ZOVR") is calendar
    assert not calendar.is_session(date(2030, 1, 1))
    session = calendar.session(date(2030, 1, 2))
    assert session is not None and session.late_open and session.early_close
    register_calendar(AlwaysOpenCalendar("ZOVR"))
    assert isinstance(get_calendar("ZOVR"), AlwaysOpenCalendar)


@given(st.datetimes(min_value=datetime(2010, 1, 1), max_value=datetime(2026, 12, 1)))
def test_next_open_and_close_are_consistent(naive: datetime) -> None:
    instant = naive.replace(tzinfo=UTC)
    opened = XNYS.next_open(instant)
    closed = XNYS.next_close(instant)
    assert opened > instant and closed > instant
    assert XNYS.is_open(opened)
    assert not XNYS.is_open(closed)
    if XNYS.is_open(instant):
        assert closed < opened
    else:
        assert opened < closed


def _past_end() -> date:
    return XNYS.last_date + timedelta(days=1)


def _before_start() -> date:
    return XNYS.first_date - timedelta(days=1)


@pytest.mark.parametrize(
    "call",
    [
        lambda: XNYS.is_session(_past_end()),
        lambda: XNYS.session(_past_end()),
        lambda: XNYS.is_session(_before_start()),
        lambda: XNYS.previous_session_date(XNYS.first_date),
        lambda: XNYS.next_session_date(XNYS.last_date),
        lambda: XNYS.sessions(XNYS.first_date, _past_end()),
        lambda: XNYS.sessions(_before_start(), XNYS.first_date),
        lambda: XNYS.closed_weekdays(XNYS.first_date, _past_end()),
        lambda: XNYS.closed_weekdays(_before_start(), XNYS.first_date),
        lambda: XNYS.settlement_date(XNYS.last_date, 1),
        lambda: XNYS.add_sessions(XNYS.first_date, -1),
        lambda: XNYS.session_of(datetime.combine(_past_end(), time(15), tzinfo=NY)),
        lambda: XNYS.phase_at(datetime.combine(_past_end(), time(15), tzinfo=NY)),
        lambda: XNYS.is_open(datetime.combine(_past_end(), time(15), tzinfo=NY)),
        lambda: XNYS.next_open(datetime.combine(XNYS.last_date, time(23), tzinfo=NY)),
        lambda: XNYS.next_close(datetime.combine(XNYS.last_date, time(23), tzinfo=NY)),
        lambda: XNYS.with_hours(_past_end(), time(9), time(16)),
        lambda: XNYS.with_closure(_past_end()),
    ],
)
def test_every_entry_point_refuses_dates_outside_the_data(call) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(CalendarOutOfRange):
        call()


def test_the_first_and_last_covered_dates_are_answered() -> None:
    """The snapshot runs from NYSE's session of 2007-01-03 to that of 2027-10-29."""
    assert (XNYS.first_date, XNYS.last_date) == (date(2007, 1, 3), date(2027, 10, 29))
    assert XNYS.is_session(XNYS.first_date) and XNYS.is_session(XNYS.last_date)
    assert [s.date for s in XNYS.sessions(XNYS.last_date, XNYS.last_date)] == [
        date(2027, 10, 29)
    ]


def test_a_24x7_calendar_has_no_closed_moment_between_days() -> None:
    crypto = get_calendar("BNCX")
    last_instant = datetime(2026, 3, 9, 23, 59, 59, 999999, tzinfo=timezone.utc)
    assert crypto.is_open(last_instant)
    today = crypto.session(date(2026, 3, 9))
    tomorrow = crypto.session(date(2026, 3, 10))
    assert today is not None and tomorrow is not None
    assert today.close == tomorrow.open == datetime(2026, 3, 10, tzinfo=timezone.utc)
    assert not today.early_close


def test_session_lookup_uses_the_venue_date_not_the_utc_date() -> None:
    sydney = get_calendar("XASX")
    session = sydney.session(date(2025, 1, 6))
    assert session is not None and session.open.date() == date(2025, 1, 5)
    during = session.open + timedelta(minutes=1)
    assert sydney.is_open(during)
    assert sydney.local_date(during) == date(2025, 1, 6)


def test_no_bundled_venue_has_a_weekend_session_and_the_gap_matrix_agrees() -> None:
    root = Path(__file__).parents[2]
    data = json.loads(
        (root / "src/ismet/calendars/data/calendars.json").read_text(encoding="utf-8")
    )
    for venue, body in data["venues"].items():
        assert body["extra"] == [], venue
        weekend = [
            day for day in body["special"] if date.fromisoformat(day).weekday() >= 5
        ]
        assert weekend == [], venue
    matrix = (root / "docs/GAP_MATRIX.md").read_text(encoding="utf-8")
    assert "special weekend sessions are unconfirmed" not in matrix
    assert "no weekend session" in matrix


# Extending a calendar past its snapshot from the venue's own holiday list. The
# dates below are test inputs, not claims about any venue's real 2027 holidays.

XBOM = get_calendar("XBOM")
EXTENSION_SOURCE = "test circular 2027/1"


def test_an_extension_continues_the_regular_hours_with_the_listed_closures() -> None:
    last = XBOM.session(XBOM.last_date)
    assert last is not None
    extended = XBOM.extended_through(
        date(2027, 1, 29), closed=[date(2027, 1, 26)], source=EXTENSION_SOURCE
    )
    session = extended.session(date(2027, 1, 4))
    assert session is not None
    assert session.open.timetz() == last.open.timetz()
    assert session.close.timetz() == last.close.timetz()
    assert not extended.is_session(date(2027, 1, 26))
    assert not extended.is_session(date(2027, 1, 2))  # a Saturday
    assert extended.next_session_date(XBOM.last_date) == date(2027, 1, 1)
    assert extended.last_date == date(2027, 1, 29)
    assert extended.source.endswith(f"extended to 2027-01-29 from {EXTENSION_SOURCE}")
    with pytest.raises(CalendarOutOfRange):
        extended.is_session(date(2027, 2, 1))


def test_extending_leaves_the_bundled_calendar_unchanged() -> None:
    extended = XBOM.extended_through(
        date(2027, 1, 29), closed=[], source=EXTENSION_SOURCE
    )
    assert extended.is_session(date(2027, 1, 26))
    with pytest.raises(CalendarOutOfRange):
        get_calendar("XBOM").is_session(date(2027, 1, 4))


def test_an_extended_day_can_take_special_hours() -> None:
    extended = XBOM.extended_through(
        date(2027, 1, 29), closed=[], source=EXTENSION_SOURCE
    ).with_hours(date(2027, 1, 27), time(9, 15), time(12, 0))
    session = extended.session(date(2027, 1, 27))
    assert session is not None
    assert session.close.astimezone(extended.tz).time() == time(12, 0)
    assert session.early_close


@pytest.mark.parametrize(
    ("last", "closed", "source", "named"),
    [
        (date(2027, 1, 29), [], " ", "source"),
        (date(2026, 12, 31), [], EXTENSION_SOURCE, "already covers"),
        (date(2027, 1, 29), [date(2026, 12, 25)], EXTENSION_SOURCE, "outside"),
        (date(2027, 1, 29), [date(2027, 2, 1)], EXTENSION_SOURCE, "outside"),
        (date(2027, 1, 29), [date(2027, 1, 2)], EXTENSION_SOURCE, "weekend"),
    ],
    ids=["no-source", "not-later", "before-gap", "after-end", "weekend"],
)
def test_an_extension_refuses_what_it_cannot_honour(
    last: date, closed: list[date], source: str, named: str
) -> None:
    with pytest.raises(ValidationError, match=named):
        XBOM.extended_through(last, closed=closed, source=source)
