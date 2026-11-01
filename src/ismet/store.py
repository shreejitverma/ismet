"""A bitemporal bar store and a resumable backfill.

Vendors restate history. A research result is reproducible only if it can read the
data as it stood when the result was made, so bars are never overwritten: a changed
bar is a new row with a later ``known_at``, and :meth:`BarStore.read` takes an
``as_of``. Storage is SQLite from the standard library, with decimals held as text so
nothing passes through a float.

For large research datasets, export from here to Parquet with the dataframe helpers.
This is the system of record rather than a columnar analytics engine.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, localcontext
from pathlib import Path

from ismet.calendars import Session, TradingCalendar
from ismet.errors import CalendarOutOfRange, IsmetError, ValidationError
from ismet.export import BarSeries
from ismet.models import Bar, Interval, Symbol
from ismet.models.decimals import _EXACT
from ismet.refdata.codes import accepting_stored_codes
from ismet.refdata.master import trading_date
from ismet.transport.clock import SYSTEM_CLOCK, Clock

logger = logging.getLogger("ismet.store")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS bars (
    venue TEXT NOT NULL,
    ticker TEXT NOT NULL,
    interval TEXT NOT NULL,
    bar_ts TEXT NOT NULL,
    known_at TEXT NOT NULL,
    digest TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (venue, ticker, interval, bar_ts, known_at)
);
CREATE TABLE IF NOT EXISTS backfill_progress (
    venue TEXT NOT NULL,
    ticker TEXT NOT NULL,
    interval TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    bars INTEGER NOT NULL,
    PRIMARY KEY (venue, ticker, interval, window_start, window_end)
);
"""


def _content_digest(bar: Bar) -> str:
    # received_ts and source describe the delivery, and symbol's other fields describe
    # the listing, whose identity is the row's key. None of them is the data.
    fields = bar.model_dump(mode="json", exclude={"received_ts", "source", "symbol"})
    fields["quality"] = sorted(fields["quality"])  # a set's order varies by process
    with localcontext(_EXACT):
        for name, value in bar:
            if isinstance(value, Decimal):
                fields[name] = format(value.normalize(), "f")  # 10.10 equals 10.1
    payload = json.dumps(fields, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _stored_bar(payload: str) -> Bar:
    with accepting_stored_codes():
        return Bar.model_validate_json(payload)


def _utc_text(moment: datetime, name: str) -> str:
    if moment.tzinfo is None:
        raise ValidationError(f"{name} must be timezone-aware")
    return moment.astimezone(timezone.utc).isoformat()


@dataclass(frozen=True)
class WriteResult:
    inserted: int
    unchanged: int
    revised: int


class BarStore:
    """Bars keyed by symbol, interval, bar time, and the time they became known."""

    def __init__(self, path: str | Path, *, clock: Clock = SYSTEM_CLOCK) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock
        self._conn = sqlite3.connect(self.path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> BarStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def write(self, bars: Iterable[Bar]) -> WriteResult:
        """Store bars. Idempotent: rewriting identical data adds nothing.

        Replaying a lower ``revision`` than the latest stored adds nothing either, so it
        cannot roll a restatement back. Equal revisions with different data are a new
        version, for vendors that do not number them. A bar whose ``adjustment`` differs
        from the latest stored version raises :class:`~ismet.errors.ValidationError`
        and nothing in the call is written, because a read would mix price scales.
        """
        inserted = unchanged = revised = 0
        with self._conn:
            # Hold the write lock from the first read, or another writer's revision
            # could land between that read and the insert, and this one would bury it.
            self._conn.execute("BEGIN IMMEDIATE")
            now = self._clock.now().astimezone(timezone.utc)
            for bar in bars:
                key = (
                    bar.symbol.venue,
                    bar.symbol.ticker,
                    bar.interval.value,
                    bar.exchange_ts.isoformat(),
                )
                latest = self._conn.execute(
                    "SELECT digest, known_at, payload FROM bars WHERE venue=? "
                    "AND ticker=? "
                    "AND interval=? "
                    "AND bar_ts=? ORDER BY known_at DESC LIMIT 1",
                    key,
                ).fetchone()
                digest = _content_digest(bar)
                known_at = now
                if latest is None:
                    inserted += 1
                else:
                    stored = _stored_bar(latest[2])
                    if stored.adjustment is not bar.adjustment:
                        raise ValidationError(
                            f"{bar.symbol} {bar.interval.value} at "
                            f"{bar.exchange_ts.isoformat()} is stored as "
                            f"{stored.adjustment.value}, and this bar is "
                            f"{bar.adjustment.value}; keep raw bars in the store and "
                            "adjust on read"
                        )
                    if latest[0] == digest or bar.revision < stored.revision:
                        unchanged += 1
                        continue
                    revised += 1
                    # Each version needs its own, strictly later, knowledge time.
                    prior = datetime.fromisoformat(latest[1])
                    known_at = max(now, prior + timedelta(microseconds=1))
                self._conn.execute(
                    "INSERT INTO bars VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (*key, known_at.isoformat(), digest, bar.model_dump_json()),
                )
        return WriteResult(inserted, unchanged, revised)

    def read(
        self,
        symbol: Symbol,
        interval: Interval,
        start: datetime,
        end: datetime,
        *,
        as_of: datetime | None = None,
    ) -> BarSeries:
        """Bars with ``start <= time < end``, as they were known at ``as_of``.

        With no ``as_of``, the latest version of each bar. A range holding more than
        one ``adjustment`` raises :class:`~ismet.errors.ValidationError`, because its
        prices are on different scales. :meth:`checksum` reads the same way.
        """
        begin = _utc_text(start, "start")
        finish = _utc_text(end, "end")
        cutoff = None if as_of is None else _utc_text(as_of, "as_of")
        rows = self._conn.execute(
            "SELECT payload FROM bars AS b WHERE venue=? AND ticker=? AND interval=? "
            "AND bar_ts>=? AND bar_ts<? AND known_at=("
            "  SELECT MAX(known_at) FROM bars WHERE venue=b.venue AND ticker=b.ticker "
            "  AND interval=b.interval AND bar_ts=b.bar_ts "
            "  AND (? IS NULL OR known_at<=?)"
            ") ORDER BY bar_ts",
            (
                symbol.venue,
                symbol.ticker,
                interval.value,
                begin,
                finish,
                cutoff,
                cutoff,
            ),
        ).fetchall()
        bars = BarSeries(_stored_bar(row[0]) for row in rows)
        scales = sorted({bar.adjustment.value for bar in bars})
        if len(scales) > 1:
            raise ValidationError(
                f"{symbol} {interval.value} from {begin} to {finish} mixes price "
                f"scales {scales}; keep raw bars in the store and adjust on read"
            )
        return bars

    def revisions(self, symbol: Symbol, interval: Interval, bar_ts: datetime) -> int:
        """How many versions of one bar are stored."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM bars WHERE venue=? AND ticker=? AND interval=? "
            "AND bar_ts=?",
            (
                symbol.venue,
                symbol.ticker,
                interval.value,
                _utc_text(bar_ts, "bar_ts"),
            ),
        ).fetchone()
        return int(row[0])

    def checksum(
        self,
        symbol: Symbol,
        interval: Interval,
        start: datetime,
        end: datetime,
        *,
        as_of: datetime | None = None,
    ) -> str:
        """A digest of a dataset as of a time, for a research manifest."""
        digest = hashlib.sha256()
        bars = self.read(symbol, interval, start, end, as_of=as_of)
        digest.update(f"{len(bars)} bars\n".encode("ascii"))
        for bar in bars:
            digest.update(_content_digest(bar).encode("ascii"))
        return digest.hexdigest()

    def missing_sessions(
        self, symbol: Symbol, calendar: TradingCalendar, start: date, end: date
    ) -> list[date]:
        """Trading dates with no daily bar stored, per the venue's calendar."""
        # A venue's trading date can differ from the UTC date, so read a day wider.
        begin = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
        finish = datetime.combine(end, datetime.min.time(), tzinfo=timezone.utc)
        bars = self.read(
            symbol, Interval.D1, begin - timedelta(days=1), finish + timedelta(days=2)
        )
        present = {trading_date(bar, calendar) for bar in bars}
        return [
            session.date
            for session in calendar.sessions(start, end)
            if session.date not in present
        ]

    # Backfill bookkeeping

    def window_done(
        self, symbol: Symbol, interval: Interval, start: datetime, end: datetime
    ) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM backfill_progress WHERE venue=? AND ticker=? AND interval=? "
            "AND window_start=? AND window_end=?",
            (
                symbol.venue,
                symbol.ticker,
                interval.value,
                start.isoformat(),
                end.isoformat(),
            ),
        ).fetchone()
        return row is not None

    def mark_window_done(
        self,
        symbol: Symbol,
        interval: Interval,
        start: datetime,
        end: datetime,
        bars: int,
    ) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO backfill_progress VALUES (?, ?, ?, ?, ?, ?)",
                (
                    symbol.venue,
                    symbol.ticker,
                    interval.value,
                    start.isoformat(),
                    end.isoformat(),
                    bars,
                ),
            )


_DEFAULT_WINDOW = timedelta(days=30)
FetchBars = Callable[[Symbol, Interval, datetime, datetime], Awaitable[list[Bar]]]


@dataclass
class BackfillReport:
    """Hard accounting for a backfill.

    Complete only if every window was fetched and none came back empty without proof
    that empty was right, or short of what the calendar expects. A vendor outage can
    look exactly like an empty window, and a partial answer like a full one, so such a
    window stays open and is fetched again next time. A session with truly no trades
    keeps its window open too: the report stays honest about what it cannot prove.
    """

    windows_total: int = 0
    windows_skipped: int = 0
    windows_fetched: int = 0
    windows_empty_unproven: int = 0
    windows_incomplete: int = 0
    windows_failed: int = 0
    bars_fetched: int = 0
    bars_inserted: int = 0
    bars_unchanged: int = 0
    bars_revised: int = 0
    failures: list[tuple[datetime, str]] = field(default_factory=list)
    sessions_missing: list[date] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return (
            self.windows_failed == 0
            and self.windows_empty_unproven == 0
            and self.windows_incomplete == 0
            and self.windows_skipped + self.windows_fetched == self.windows_total
        )


def _check_window(
    bars: list[Bar],
    symbol: Symbol,
    interval: Interval,
    start: datetime,
    end: datetime,
) -> None:
    for bar in bars:
        if bar.symbol != symbol or bar.interval is not interval:
            raise ValidationError(
                f"fetch for {symbol} {interval.value} returned {bar.symbol} "
                f"{bar.interval.value}"
            )
        if not start <= bar.exchange_ts < end:
            raise ValidationError(
                f"bar at {bar.exchange_ts} is outside the window {start} to {end}"
            )


def _longest_period(interval: Interval) -> timedelta:
    seconds = interval.nominal_seconds
    return timedelta(days=31) if seconds is None else timedelta(seconds=seconds)


async def backfill(
    fetch: FetchBars,
    store: BarStore,
    symbol: Symbol,
    interval: Interval,
    start: datetime,
    end: datetime,
    *,
    window: timedelta | None = None,
    calendar: TradingCalendar | None = None,
) -> BackfillReport:
    """Fetch ``[start, end)`` in windows into ``store``. Safe to rerun after a failure.

    Finished windows are recorded and skipped next time, unless ``calendar`` shows a
    session in one with no bar, as a window finished without a calendar or from a later
    start can: that window is fetched again and judged as new. A window's failure is
    logged and counted, and the rest continue. An empty window counts as finished only
    if ``calendar`` shows no session touching it. With ``calendar``, every session must
    have at least one bar, at daily or shorter intervals, for the window holding its
    close to finish; a longer interval with ``calendar`` raises, since one bar spans
    many sessions.
    The bars can be in an earlier window, since a session can straddle several, and the
    store is what is asked. Sessions that began before ``start`` or close after ``end``
    were only partly requested, so they are not required. The bars received are kept
    either way. Without a calendar, a non-empty answer is taken as final, since nothing
    can contradict it. Check :attr:`BackfillReport.complete`.

    ``window`` defaults to 30 days, or 31 for monthly bars. One shorter than the
    interval's longest period raises, because it could hold no bar and so never finish.
    For the same reason a tail shorter than that period joins the window before it. A
    whole range that short keeps its window open if it comes back empty.
    """
    period = _longest_period(interval)
    if window is None:
        window = max(_DEFAULT_WINDOW, period)
    if window <= timedelta(0):
        raise ValidationError("window must be positive")
    if window < period:
        raise ValidationError(
            f"window {window} can hold no {interval.value} bar, whose period can be "
            f"{period}; use a window of at least that"
        )
    if start >= end:
        raise ValidationError(f"backfill start {start} is not before end {end}")
    if calendar is not None and interval in (Interval.W1, Interval.MO1):
        raise ValidationError(
            f"a calendar checks that each session has a bar, which {interval.value} "
            "bars cannot show; backfill them without one"
        )
    report = BackfillReport()
    cursor = start
    while cursor < end:
        upper = min(cursor + window, end)
        if end - upper < period:
            upper = end
        report.windows_total += 1
        await _backfill_window(
            fetch, store, symbol, interval, cursor, upper, calendar, start, report
        )
        cursor = upper
    return report


async def _backfill_window(
    fetch: FetchBars,
    store: BarStore,
    symbol: Symbol,
    interval: Interval,
    start: datetime,
    end: datetime,
    calendar: TradingCalendar | None,
    requested_from: datetime,
    report: BackfillReport,
) -> None:
    try:
        if store.window_done(symbol, interval, start, end) and (
            calendar is None
            or not _sessions_without_bars(
                store, symbol, interval, calendar, start, end, requested_from
            )
        ):
            report.windows_skipped += 1
            return
        bars = await fetch(symbol, interval, start, end)
        _check_window(bars, symbol, interval, start, end)
        report.bars_fetched += len(bars)
        result = store.write(bars)
        report.bars_inserted += result.inserted
        report.bars_unchanged += result.unchanged
        report.bars_revised += result.revised
        if not bars and not _proven_empty(calendar, start, end):
            report.windows_empty_unproven += 1
            return
        missing = (
            _sessions_without_bars(
                store, symbol, interval, calendar, start, end, requested_from
            )
            if calendar is not None and bars
            else []
        )
        if missing:
            report.windows_incomplete += 1
            report.sessions_missing.extend(missing)
            return
        store.mark_window_done(symbol, interval, start, end, len(bars))
    # ValueError: a stored row that no longer parses is this window's failure.
    except (IsmetError, sqlite3.Error, ValueError) as exc:
        logger.warning("backfill window %s failed: %s", start, exc)
        report.windows_failed += 1
        report.failures.append((start, repr(exc)))
        return
    report.windows_fetched += 1


def _sessions_touching(
    calendar: TradingCalendar, start: datetime, end: datetime
) -> list[Session]:
    """Sessions with any moment in ``[start, end)``. Raises past the calendar's data."""
    first = calendar.local_date(start)
    last = calendar.local_date(end - timedelta(microseconds=1))
    return [
        session
        for session in calendar.sessions(first, last)
        if session.open < end and start < session.close
    ]


def _sessions_without_bars(
    store: BarStore,
    symbol: Symbol,
    interval: Interval,
    calendar: TradingCalendar,
    start: datetime,
    end: datetime,
    requested_from: datetime,
) -> list[date]:
    """Sessions closing in this window with no bar stored. Unprovable ones count too."""
    try:
        sessions = _sessions_touching(calendar, start, end)
    except CalendarOutOfRange:
        return [calendar.local_date(start)]
    due = [
        session
        for session in sessions
        if requested_from <= session.open and session.close <= end
    ]
    if not due:
        return []
    # A daily bar can be stamped before its session opens, so read a day wider.
    earliest = min(start, due[0].open) - timedelta(days=1)
    stored = store.read(symbol, interval, earliest, end)
    present = {trading_date(bar, calendar) for bar in stored}
    return [session.date for session in due if session.date not in present]


def _proven_empty(
    calendar: TradingCalendar | None, start: datetime, end: datetime
) -> bool:
    if calendar is None:
        return False
    try:
        return not _sessions_touching(calendar, start, end)
    except CalendarOutOfRange:
        return False


__all__ = ["BackfillReport", "BarStore", "WriteResult", "backfill"]
