"""Conformance fails a provider whose bars are still open or have holes.

The clock is frozen for both the suite and the mock venue, so which bar is still
open is fixed rather than depending on where in the minute the test runs.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import ClassVar

import pytest

from ismet.models import Bar, Interval, Symbol
from ismet.providers.mock import MockProvider
from ismet.testing import run_conformance
from ismet.testing.conformance import (
    MAX_PLAUSIBLE_CLOCK_SKEW,
    _bars_closed,
    _bars_complete,
    _bars_range,
)
from ismet.transport.clock import ManualClock

UTC = timezone.utc
NOW = datetime(2026, 11, 10, 14, 37, 30, tzinfo=UTC)
MINUTE = timedelta(minutes=1)
BAR_CHECKS = ("bars", "bars_invariants", "bars_closed", "bars_complete")


def freeze(monkeypatch: pytest.MonkeyPatch, now: datetime) -> ManualClock:
    class Frozen(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:  # type: ignore[override]  # noqa: ARG003
            return now

    monkeypatch.setattr("ismet.testing.conformance.datetime", Frozen)
    return ManualClock(now)


async def bar_checks(
    provider: MockProvider, sym: Symbol, **kwargs: object
) -> dict[str, tuple[bool, bool, str]]:
    report = await run_conformance(provider, symbol=sym, **kwargs)  # type: ignore[arg-type]
    return {
        c.name: (c.passed, c.skipped, c.detail)
        for c in report.checks
        if c.name in BAR_CHECKS
    }


class ReturnsTheOpenBar(MockProvider):
    name = "open-bar"

    async def bars(self, symbol, interval, start, end):  # type: ignore[no-untyped-def]
        bars = await super().bars(symbol, interval, start, end)
        last = bars[-1]
        if interval is Interval.D1:
            forming = last.exchange_ts + timedelta(days=1)
            update = {"exchange_ts": forming, "session_date": forming.date()}
        else:
            step = timedelta(seconds=interval.seconds)
            update = {"exchange_ts": last.exchange_ts + step}
        return [*bars, last.model_copy(update=update)]


class DropsABar(MockProvider):
    name = "drops-a-bar"

    async def bars(self, symbol, interval, start, end):  # type: ignore[no-untyped-def]
        bars = await super().bars(symbol, interval, start, end)
        return bars[:10] + bars[11:]


class DropsABarUndeclared(DropsABar):
    name = "drops-a-bar-undeclared"
    bars_every_interval: ClassVar[bool] = False


class StopsEarly(MockProvider):
    name = "stops-early"

    async def bars(self, symbol, interval, start, end):  # type: ignore[no-untyped-def]
        return (await super().bars(symbol, interval, start, end))[:-10]


class DropsTheNewestBar(MockProvider):
    name = "drops-newest"

    async def bars(self, symbol, interval, start, end):  # type: ignore[no-untyped-def]
        return (await super().bars(symbol, interval, start, end))[:-1]


class HalfHourBarsLabelledOneMinute(MockProvider):
    name = "30m-as-1m"

    async def bars(self, symbol, interval, start, end):  # type: ignore[no-untyped-def]
        bars = await super().bars(symbol, interval, start, end)
        return [b for b in bars if b.exchange_ts.minute % 30 == 0]


async def test_the_mock_serves_every_closed_bar_and_not_the_open_one(
    monkeypatch: pytest.MonkeyPatch, sym: Symbol
) -> None:
    clock = freeze(monkeypatch, NOW)
    mock = MockProvider(tick_interval=0, clock=clock)
    checks = await bar_checks(mock, sym)
    assert checks["bars_invariants"] == (True, False, "29 bars")
    assert checks["bars_closed"] == (True, False, "")
    assert checks["bars_complete"] == (True, False, "")
    # Asked for the forming minute alone, the venue has nothing closed to serve.
    forming = _bars_range(NOW, Interval.M1, MINUTE)
    assert await mock.bars(sym, Interval.M1, *forming) == []
    start, end = _bars_range(NOW, Interval.M1, 30 * MINUTE)
    stamps = [b.exchange_ts for b in await mock.bars(sym, Interval.M1, start, end)]
    assert stamps[-1] == datetime(2026, 11, 10, 14, 36, tzinfo=UTC)


@pytest.mark.parametrize(
    ("wrong", "failing", "named", "kwargs"),
    [
        (ReturnsTheOpenBar, "bars_closed", "bar 2026-11-10T14:37:00", {}),
        (
            ReturnsTheOpenBar,
            "bars_closed",
            "bar 2026-11-10T14:00:00",
            {"interval": Interval.H1, "lookback": timedelta(hours=6)},
        ),
        (
            ReturnsTheOpenBar,
            "bars_closed",
            "bar 2026-11-10T00:00:00",
            {"interval": Interval.D1, "lookback": timedelta(days=5)},
        ),
        (DropsABar, "bars_complete", "1 1m bars missing", {}),
        (StopsEarly, "bars_complete", "10 1m bars missing", {}),
        (
            HalfHourBarsLabelledOneMinute,
            "bars_complete",
            "1m bars missing",
            {"lookback": timedelta(hours=3)},
        ),
    ],
    ids=["open-1m", "open-1h", "open-1d", "hole", "stops-early", "30m-as-1m"],
)
async def test_a_provider_with_open_or_missing_bars_fails_that_check(
    monkeypatch: pytest.MonkeyPatch,
    sym: Symbol,
    wrong: type[MockProvider],
    failing: str,
    named: str,
    kwargs: dict[str, object],
) -> None:
    clock = freeze(monkeypatch, NOW)
    checks = await bar_checks(wrong(tick_interval=0, clock=clock), sym, **kwargs)
    failed = {name: detail for name, (ok, _, detail) in checks.items() if not ok}
    assert set(failed) == {failing}, checks
    assert named in failed[failing], failed


async def test_hourly_and_daily_bars_of_the_mock_pass(
    monkeypatch: pytest.MonkeyPatch, sym: Symbol
) -> None:
    clock = freeze(monkeypatch, NOW)
    cases = ((Interval.H1, timedelta(hours=6)), (Interval.D1, timedelta(days=5)))
    for interval, lookback in cases:
        checks = await bar_checks(
            MockProvider(tick_interval=0, clock=clock),
            sym,
            interval=interval,
            lookback=lookback,
        )
        assert all(ok for ok, _, _ in checks.values()), checks
        assert checks["bars_closed"][1] is False


async def test_a_missing_bar_is_not_checked_unless_the_provider_declares_it(
    monkeypatch: pytest.MonkeyPatch, sym: Symbol
) -> None:
    clock = freeze(monkeypatch, NOW)
    checks = await bar_checks(DropsABarUndeclared(tick_interval=0, clock=clock), sym)
    passed, skipped, detail = checks["bars_complete"]
    assert passed and skipped
    assert "does not declare bars_every_interval" in detail


async def test_a_bar_open_by_less_than_the_skew_allowance_passes(
    monkeypatch: pytest.MonkeyPatch, sym: Symbol
) -> None:
    # The forming 14:37 bar closes 3s after this answer: a venue clock that far
    # ahead of ours would already call it closed.
    now = datetime(2026, 11, 10, 14, 37, 57, tzinfo=UTC)
    clock = freeze(monkeypatch, now)
    checks = await bar_checks(ReturnsTheOpenBar(tick_interval=0, clock=clock), sym)
    assert checks["bars_closed"] == (True, False, "")


async def test_a_bar_closed_within_the_skew_allowance_may_be_absent(
    monkeypatch: pytest.MonkeyPatch, sym: Symbol
) -> None:
    # The 14:37 bar closed 3s before the request; a venue clock behind ours may not
    # have closed it yet, so leaving it out is not a missing bar.
    now = datetime(2026, 11, 10, 14, 38, 3, tzinfo=UTC)
    assert timedelta(seconds=3) < MAX_PLAUSIBLE_CLOCK_SKEW
    clock = freeze(monkeypatch, now)
    checks = await bar_checks(DropsTheNewestBar(tick_interval=0, clock=clock), sym)
    assert checks["bars_complete"] == (True, False, "")


@pytest.mark.parametrize(
    ("interval", "start", "end"),
    [
        (Interval.M1, "14:08", "14:38"),
        (Interval.H1, "14:08", "15:37"),
        (Interval.D1, "14:08", "14:38"),
    ],
)
def test_the_range_asked_covers_the_bar_still_forming(
    interval: Interval, start: str, end: str
) -> None:
    def at(hhmm: str) -> datetime:
        hour, minute = map(int, hhmm.split(":"))
        return NOW.replace(hour=hour, minute=minute, second=0)

    assert _bars_range(NOW, interval, 30 * MINUTE) == (at(start), at(end))


XNYS = Symbol(venue="XNYS", ticker="IBM")


def bars_at(symbol: Symbol, *stamps: datetime) -> list[Bar]:
    price = Decimal("100")
    return [
        Bar(
            symbol=symbol,
            exchange_ts=ts,
            received_ts=ts + MINUTE,
            interval=Interval.M1,
            open=price,
            high=price,
            low=price,
            close=price,
            volume=Decimal("1"),
        )
        for ts in stamps
    ]


# The NYSE regular session is 14:30 to 21:00 UTC in November, after the change
# back to standard time on 2026-11-01. 2026-11-06 is a Friday.
FRIDAY_CLOSE = datetime(2026, 11, 6, 21, 0, tzinfo=UTC)
MONDAY_OPEN = datetime(2026, 11, 9, 14, 30, tzinfo=UTC)


def test_bars_missing_while_the_venue_is_closed_are_not_missing() -> None:
    bars = bars_at(
        XNYS,
        FRIDAY_CLOSE - 2 * MINUTE,
        FRIDAY_CLOSE - MINUTE,
        MONDAY_OPEN,
        MONDAY_OPEN + MINUTE,
    )
    check = _bars_complete(
        bars,
        XNYS,
        Interval.M1,
        FRIDAY_CLOSE - 2 * MINUTE,
        MONDAY_OPEN + 2 * MINUTE,
        MONDAY_OPEN + 2 * MINUTE,
        declared=True,
    )
    assert (check.passed, check.skipped) == (True, False), check.detail


def test_a_bar_missing_in_the_session_is_missing() -> None:
    bars = bars_at(XNYS, FRIDAY_CLOSE - 3 * MINUTE, FRIDAY_CLOSE - MINUTE, MONDAY_OPEN)
    check = _bars_complete(
        bars,
        XNYS,
        Interval.M1,
        FRIDAY_CLOSE - 3 * MINUTE,
        MONDAY_OPEN + MINUTE,
        MONDAY_OPEN + MINUTE,
        declared=True,
    )
    assert not check.passed
    assert check.detail == (
        "1 1m bars missing while XNYS is open, the first at 2026-11-06T20:58:00+00:00"
    )


@pytest.mark.parametrize(
    ("symbol", "day", "named"),
    [
        (
            Symbol(venue="XBOM", ticker="INFY"),
            datetime(2027, 3, 1, 4, 0, tzinfo=UTC),
            "2027",
        ),
        (
            Symbol(venue="XOSL", ticker="EQNR"),
            datetime(2026, 11, 9, 9, 0, tzinfo=UTC),
            "XOSL",
        ),
    ],
    ids=["beyond-the-calendar-data", "venue-without-a-calendar"],
)
def test_without_a_calendar_for_the_range_completeness_is_not_judged(
    symbol: Symbol, day: datetime, named: str
) -> None:
    bars = bars_at(symbol, day, day + 2 * MINUTE)
    check = _bars_complete(
        bars,
        symbol,
        Interval.M1,
        day,
        day + 3 * MINUTE,
        day + 4 * MINUTE,
        declared=True,
    )
    assert check.passed and check.skipped
    assert check.detail.startswith("not checked: ")
    assert named in check.detail


def daily(symbol: Symbol, day: datetime) -> list[Bar]:
    return [bars_at(symbol, day)[0].model_copy(update={"interval": Interval.D1})]


def test_a_daily_bar_beyond_the_calendar_data_is_not_judged() -> None:
    xbom = Symbol(venue="XBOM", ticker="INFY")
    day = datetime(2027, 3, 1, tzinfo=UTC)
    check = _bars_closed(daily(xbom, day), xbom, Interval.D1, day)
    assert check.passed and check.skipped
    assert check.detail.startswith("not checked: ")


def test_a_daily_bar_dated_to_a_day_without_a_session_is_not_judged() -> None:
    # Saturday 2026-11-07: no NYSE session, so nothing says when the bar closes.
    saturday = datetime(2026, 11, 7, 5, 0, tzinfo=UTC)
    check = _bars_closed(daily(XNYS, saturday), XNYS, Interval.D1, saturday)
    assert (check.passed, check.skipped) == (True, False)


def test_a_daily_bar_dated_by_session_date_closes_with_that_session() -> None:
    # Stamped at local midnight, outside the session, but dated to Friday's.
    midnight = datetime(2026, 11, 6, 5, 0, tzinfo=UTC)
    bars = [
        daily(XNYS, midnight)[0].model_copy(update={"session_date": date(2026, 11, 6)})
    ]
    before_close = FRIDAY_CLOSE - MINUTE
    open_check = _bars_closed(bars, XNYS, Interval.D1, before_close)
    assert not open_check.passed
    assert "closes 2026-11-06T21:00:00+00:00" in open_check.detail
    assert _bars_closed(bars, XNYS, Interval.D1, FRIDAY_CLOSE).passed


@pytest.mark.parametrize("interval", [Interval.W1, Interval.MO1])
def test_weekly_and_monthly_bars_are_not_judged_open(interval: Interval) -> None:
    bars = [daily(XNYS, NOW)[0].model_copy(update={"interval": interval})]
    check = _bars_closed(bars, XNYS, interval, NOW - timedelta(days=40))
    assert check.passed and check.skipped
    assert check.detail == f"not checked: {interval.value} bars"
