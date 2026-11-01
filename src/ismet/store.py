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
import sqlite3
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from ismet.calendars import TradingCalendar
from ismet.errors import IsmetError, ValidationError
from ismet.export import BarSeries
from ismet.models import Bar, Interval, Symbol
from ismet.transport.clock import SYSTEM_CLOCK, Clock

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
    # received_ts and source describe the delivery, not the data.
    payload = bar.model_dump_json(exclude={"received_ts", "source"})
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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
        """Store bars. Idempotent: rewriting identical data adds nothing."""
        known_at = self._clock.now().astimezone(timezone.utc).isoformat()
        inserted = unchanged = revised = 0
        with self._conn:
            for bar in bars:
                key = (
                    bar.symbol.venue,
                    bar.symbol.ticker,
                    bar.interval.value,
                    bar.exchange_ts.isoformat(),
                )
                latest = self._conn.execute(
                    "SELECT digest FROM bars WHERE venue=? AND ticker=? AND interval=? "
                    "AND bar_ts=? ORDER BY known_at DESC LIMIT 1",
                    key,
                ).fetchone()
                digest = _content_digest(bar)
                if latest is not None and latest[0] == digest:
                    unchanged += 1
                    continue
                if latest is None:
                    inserted += 1
                else:
                    revised += 1
                self._conn.execute(
                    "INSERT INTO bars VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (*key, known_at, digest, bar.model_dump_json()),
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
        """Bars with ``start <= time < end``, as they were known at ``as_of``."""
        if start.tzinfo is None or end.tzinfo is None:
            raise ValidationError("start and end must be timezone-aware")
        cutoff = (as_of or self._clock.now()).astimezone(timezone.utc).isoformat()
        rows = self._conn.execute(
            "SELECT payload FROM bars AS b WHERE venue=? AND ticker=? AND interval=? "
            "AND bar_ts>=? AND bar_ts<? AND known_at=("
            "  SELECT MAX(known_at) FROM bars WHERE venue=b.venue AND ticker=b.ticker "
            "  AND interval=b.interval AND bar_ts=b.bar_ts AND known_at<=?"
            ") ORDER BY bar_ts",
            (
                symbol.venue,
                symbol.ticker,
                interval.value,
                start.astimezone(timezone.utc).isoformat(),
                end.astimezone(timezone.utc).isoformat(),
                cutoff,
            ),
        ).fetchall()
        return BarSeries(Bar.model_validate_json(row[0]) for row in rows)

    def revisions(self, symbol: Symbol, interval: Interval, bar_ts: datetime) -> int:
        """How many versions of one bar are stored."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM bars WHERE venue=? AND ticker=? AND interval=? "
            "AND bar_ts=?",
            (
                symbol.venue,
                symbol.ticker,
                interval.value,
                bar_ts.astimezone(timezone.utc).isoformat(),
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
        for bar in self.read(symbol, interval, start, end, as_of=as_of):
            digest.update(_content_digest(bar).encode("ascii"))
        return digest.hexdigest()

    def missing_sessions(
        self, symbol: Symbol, calendar: TradingCalendar, start: date, end: date
    ) -> list[date]:
        """Trading dates with no daily bar stored, per the venue's calendar."""
        begin = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
        finish = datetime.combine(end, datetime.min.time(), tzinfo=timezone.utc)
        bars = self.read(symbol, Interval.D1, begin, finish + timedelta(days=2))
        present = {bar.session_date or bar.exchange_ts.date() for bar in bars}
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


FetchBars = Callable[[Symbol, Interval, datetime, datetime], Awaitable[list[Bar]]]


@dataclass
class BackfillReport:
    """Hard accounting for a backfill. It is complete only if no window failed."""

    windows_total: int = 0
    windows_skipped: int = 0
    windows_fetched: int = 0
    windows_empty: int = 0
    windows_failed: int = 0
    bars_fetched: int = 0
    bars_inserted: int = 0
    bars_unchanged: int = 0
    bars_revised: int = 0

    @property
    def complete(self) -> bool:
        accounted = self.windows_skipped + self.windows_fetched + self.windows_failed
        return self.windows_failed == 0 and accounted == self.windows_total


async def backfill(
    fetch: FetchBars,
    store: BarStore,
    symbol: Symbol,
    interval: Interval,
    start: datetime,
    end: datetime,
    *,
    window: timedelta = timedelta(days=30),
) -> BackfillReport:
    """Fetch ``[start, end)`` in windows into ``store``. Safe to rerun after a failure.

    Finished windows are recorded and skipped next time. A window's failure is
    counted and the rest continue, so one bad window costs one retry. A finished
    window that returned no bars is counted separately, because "empty" and "fine" are
    different answers. Check :attr:`BackfillReport.complete`.
    """
    if window <= timedelta(0):
        raise ValidationError("window must be positive")
    report = BackfillReport()
    cursor = start
    while cursor < end:
        upper = min(cursor + window, end)
        report.windows_total += 1
        if store.window_done(symbol, interval, cursor, upper):
            report.windows_skipped += 1
        else:
            try:
                bars = await fetch(symbol, interval, cursor, upper)
            except IsmetError:
                report.windows_failed += 1
            else:
                result = store.write(bars)
                store.mark_window_done(symbol, interval, cursor, upper, len(bars))
                report.windows_fetched += 1
                report.windows_empty += 0 if bars else 1
                report.bars_fetched += len(bars)
                report.bars_inserted += result.inserted
                report.bars_unchanged += result.unchanged
                report.bars_revised += result.revised
        cursor = upper
    return report


__all__ = ["BackfillReport", "BarStore", "WriteResult", "backfill"]
