from __future__ import annotations

import importlib
import os
import sqlite3
import subprocess
import sys
import textwrap
import threading
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, localcontext
from pathlib import Path

import pytest

import ismet.store as store_module
from ismet import IsmetClient
from ismet.calendars import get_calendar
from ismet.errors import ConfigError, SequenceGap, TransportError, ValidationError
from ismet.execution import BreakKind, ReconBreak, apply_event, new_order
from ismet.export import BarSeries, to_pandas, to_polars, to_records
from ismet.marketdata import GapPolicy
from ismet.models import Adjustment, Bar, Interval, QualityFlag, Quote, Symbol
from ismet.models.market_data import DataSource, Level, OrderBook, Timeliness, Trade
from ismet.models.orders import (
    Canceled,
    Fill,
    Filled,
    OrderRequest,
    OrderSide,
    OrderStatus,
    Restated,
    SubmitIntent,
)
from ismet.observability import (
    InMemoryMetrics,
    Metrics,
    break_hook,
    http_hook,
    order_event_hook,
)
from ismet.providers.mock import MockProvider
from ismet.refdata.master import ActionKind, CorporateAction, adjust_bars
from ismet.store import BarStore, backfill
from ismet.transport.clock import ManualClock
from ismet.transport.http import RequestInfo

UTC = timezone.utc
D = Decimal
SYM = Symbol(venue="XNYS", ticker="ACME")
DAY = datetime(2026, 3, 9, 14, 30, tzinfo=UTC)
XNYS = get_calendar("XNYS")


def bar(ts: datetime = DAY, close: str = "10.10", **kw: object) -> Bar:
    fields: dict[str, object] = {
        "symbol": SYM,
        "exchange_ts": ts,
        "received_ts": ts,
        "interval": Interval.D1,
        "open": "10",
        "high": "11",
        "low": "9",
        "close": close,
        "volume": "100",
        "adjustment": Adjustment.RAW,
        "session_date": ts.date(),
    }
    fields.update(kw)
    return Bar.model_validate(fields)


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(datetime(2026, 3, 10, tzinfo=UTC))


@pytest.fixture
def store(tmp_path: Path, clock: ManualClock):  # type: ignore[no-untyped-def]
    with BarStore(tmp_path / "bars.db", clock=clock) as instance:
        yield instance


def span() -> tuple[datetime, datetime]:
    return DAY - timedelta(days=1), DAY + timedelta(days=1)


def test_write_is_idempotent_and_preserves_decimals(store: BarStore) -> None:
    assert store.write([bar()]).inserted == 1
    # A different delivery of identical data is not a revision.
    later = bar(received_ts=DAY + timedelta(hours=1))
    result = store.write([later])
    assert (result.inserted, result.unchanged, result.revised) == (0, 1, 0)
    stored = store.read(SYM, Interval.D1, *span())
    assert len(stored) == 1 and stored[0].close == D("10.10")
    assert str(stored[0].close) == "10.10"


def test_restatement_keeps_history_readable_as_of_any_time(
    store: BarStore, clock: ManualClock
) -> None:
    store.write([bar(close="10.10")])
    before_restatement = clock.now()
    clock.advance(86400)
    assert store.write([bar(close="10.20", revision=1)]).revised == 1
    start, end = span()
    assert store.read(SYM, Interval.D1, start, end)[0].close == D("10.20")
    old = store.read(SYM, Interval.D1, start, end, as_of=before_restatement)
    assert old[0].close == D("10.10")
    before_any = before_restatement - timedelta(days=1)
    assert store.read(SYM, Interval.D1, start, end, as_of=before_any) == []
    assert store.revisions(SYM, Interval.D1, DAY) == 2
    assert store.checksum(SYM, Interval.D1, start, end) != store.checksum(
        SYM, Interval.D1, start, end, as_of=before_restatement
    )


def test_read_range_is_half_open_and_requires_aware_times(store: BarStore) -> None:
    store.write([bar()])
    assert store.read(SYM, Interval.D1, DAY, DAY + timedelta(days=1))
    assert not store.read(SYM, Interval.D1, DAY - timedelta(days=1), DAY)
    with pytest.raises(ValidationError):
        store.read(SYM, Interval.D1, datetime(2026, 1, 1), DAY)


def test_missing_sessions_use_the_calendar(store: BarStore) -> None:
    store.write([bar()])
    missing = store.missing_sessions(
        SYM, get_calendar("XNYS"), date(2026, 3, 6), date(2026, 3, 10)
    )
    assert missing == [date(2026, 3, 6), date(2026, 3, 10)]


async def test_backfill_accounts_for_every_window_and_resumes(store: BarStore) -> None:
    calls: list[datetime] = []
    fail_once = {"armed": True}

    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        calls.append(start)
        if start == DAY + timedelta(days=1) and fail_once["armed"]:
            fail_once["armed"] = False
            raise TransportError("flaky")
        return [bar(start)]

    args = (fetch, store, SYM, Interval.D1, DAY, DAY + timedelta(days=3))
    first = await backfill(*args, window=timedelta(days=1))
    assert first.windows_total == 3 and first.windows_failed == 1
    assert first.bars_inserted == 2 and not first.complete
    assert first.failures[0][0] == DAY + timedelta(days=1)
    assert "flaky" in first.failures[0][1]
    calls.clear()
    second = await backfill(*args, window=timedelta(days=1))
    assert second.complete
    assert second.windows_skipped == 2 and second.windows_fetched == 1
    assert calls == [DAY + timedelta(days=1)]
    with pytest.raises(ValidationError):
        await backfill(*args, window=timedelta(0))


async def test_backfill_never_treats_an_empty_answer_as_final_without_proof(
    store: BarStore,
) -> None:
    answers: list[list[Bar]] = [[], [bar()]]

    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        return answers.pop(0)

    span_args = (fetch, store, SYM, Interval.D1, DAY, DAY + timedelta(days=1))
    outage = await backfill(*span_args, window=timedelta(days=1))
    assert outage.windows_empty_unproven == 1 and not outage.complete
    recovered = await backfill(*span_args, window=timedelta(days=1))
    assert recovered.complete and recovered.bars_inserted == 1


async def test_backfill_accepts_an_empty_window_the_calendar_says_has_no_session(
    store: BarStore,
) -> None:
    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        return []

    saturday = datetime(2026, 3, 7, 12, tzinfo=UTC)  # Saturday in New York too
    report = await backfill(
        fetch,
        store,
        SYM,
        Interval.D1,
        saturday,
        saturday + timedelta(days=1),
        window=timedelta(days=1),
        calendar=get_calendar("XNYS"),
    )
    assert report.complete and report.windows_fetched == 1


@pytest.mark.parametrize(
    "wrong",
    [
        {"symbol": Symbol(venue="XNYS", ticker="OTHER")},
        {"interval": Interval.H1},
        {
            "exchange_ts": DAY + timedelta(days=9),
            "received_ts": DAY + timedelta(days=9),
        },
    ],
)
async def test_backfill_rejects_data_that_is_not_what_was_asked_for(
    store: BarStore, wrong: dict[str, object]
) -> None:
    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        return [bar(**wrong)]

    report = await backfill(
        fetch,
        store,
        SYM,
        Interval.D1,
        DAY,
        DAY + timedelta(days=1),
        window=timedelta(days=1),
    )
    assert report.windows_failed == 1 and not report.complete
    assert store.read(SYM, Interval.D1, *span()) == []


def test_same_clock_tick_and_same_batch_restatements_are_stored_in_order(
    store: BarStore,
) -> None:
    result = store.write([bar(close="10.10"), bar(close="10.20")])
    assert (result.inserted, result.revised) == (1, 1)
    store.write([bar(close="10.30")])
    assert store.revisions(SYM, Interval.D1, DAY) == 3
    assert store.read(SYM, Interval.D1, *span())[0].close == D("10.30")


def test_reformatting_a_price_is_not_a_revision(store: BarStore) -> None:
    store.write([bar(close="10.10")])
    result = store.write([bar(close="10.1")])
    assert (result.inserted, result.unchanged, result.revised) == (0, 1, 0)
    assert store.revisions(SYM, Interval.D1, DAY) == 1


async def test_backfill_refuses_a_range_that_is_not_a_range(store: BarStore) -> None:
    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        raise AssertionError("nothing should be fetched")

    for start, end in (
        (DAY + timedelta(days=5), DAY),
        (DAY, DAY),
    ):
        with pytest.raises(ValidationError, match="not before"):
            await backfill(fetch, store, SYM, Interval.D1, start, end)


async def test_backfill_leaves_a_window_open_when_sessions_are_missing(
    store: BarStore,
) -> None:
    """XNYS traded every weekday from 2026-03-02 to 03-13; one bar is not enough."""
    calendar = get_calendar("XNYS")
    start = datetime(2026, 3, 2, tzinfo=UTC)
    end = datetime(2026, 3, 14, tzinfo=UTC)
    weekdays = [date(2026, 3, d) for d in (2, 3, 4, 5, 6, 9, 10, 11, 12, 13)]
    answers = [
        [bar(DAY)],
        [
            bar(datetime(d.year, d.month, d.day, 14, 30, tzinfo=UTC), session_date=d)
            for d in weekdays
        ],
    ]
    calls = 0

    async def fetch(symbol, interval, lower, upper):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        return answers.pop(0)

    args = (fetch, store, SYM, Interval.D1, start, end)
    partial = await backfill(*args, window=timedelta(days=30), calendar=calendar)
    assert not partial.complete and partial.windows_incomplete == 1
    assert partial.windows_fetched == 0 and partial.bars_inserted == 1
    assert partial.sessions_missing == [d for d in weekdays if d != date(2026, 3, 9)]
    full = await backfill(*args, window=timedelta(days=30), calendar=calendar)
    assert full.complete and full.windows_fetched == 1 and calls == 2
    assert store.missing_sessions(SYM, calendar, weekdays[0], weekdays[-1]) == []
    rerun = await backfill(*args, window=timedelta(days=30), calendar=calendar)
    assert rerun.complete and rerun.windows_skipped == 1 and calls == 2


async def test_backfill_judges_intraday_windows_by_session_presence(
    store: BarStore,
) -> None:
    start = datetime(2026, 3, 9, tzinfo=UTC)
    end = datetime(2026, 3, 11, tzinfo=UTC)
    first_session_only = bar(DAY, interval=Interval.H1, session_date=date(2026, 3, 9))

    async def fetch(symbol, interval, lower, upper):  # type: ignore[no-untyped-def]
        return [first_session_only]

    report = await backfill(
        fetch,
        store,
        SYM,
        Interval.H1,
        start,
        end,
        window=timedelta(days=2),
        calendar=get_calendar("XNYS"),
    )
    assert report.windows_incomplete == 1
    assert report.sessions_missing == [date(2026, 3, 10)]


def test_records_keep_quality_flags_and_provenance() -> None:
    flagged = bar(
        quality=frozenset({QualityFlag.OUTLIER, QualityFlag.AFTER_GAP}),
        source=DataSource(
            provider="vendor", timeliness=Timeliness.DELAYED, delay_seconds=900
        ),
    )
    record = to_records([flagged])[0]
    assert record["quality"] == ",".join(
        sorted((QualityFlag.OUTLIER.value, QualityFlag.AFTER_GAP.value))
    )
    assert record["source"] == "vendor"
    assert record["timeliness"] == Timeliness.DELAYED.value
    clean = to_records([bar()])[0]
    assert clean["quality"] == "" and clean["source"] is None


def test_terminal_counter_counts_an_order_once_when_it_closes() -> None:
    metrics = InMemoryMetrics()
    hook = order_event_hook(metrics)
    request = OrderRequest(
        client_order_id="C1",
        symbol=SYM,
        side=OrderSide.BUY,
        quantity="1",
        limit_price="1",
    )
    state = _submitting(
        new_order(SubmitIntent(client_order_id="C1", ts=DAY, request=request))
    )
    canceled = Canceled(client_order_id="C1", ts=DAY)
    closed = apply_event(state, canceled)
    hook(canceled, closed)
    fill = Filled(
        client_order_id="C1",
        ts=DAY,
        fill=Fill(
            client_order_id="C1",
            fill_id="F1",
            symbol=SYM,
            side=OrderSide.BUY,
            price="1",
            quantity="1",
            ts=DAY,
        ),
    )
    late = apply_event(closed, fill)
    hook(fill, late)
    assert metrics.count("ismet_orders_terminal_total", status="canceled") == 1
    assert metrics.count("ismet_orders_terminal_total", status="filled") == 0
    reopened = Restated(
        client_order_id="C1", ts=DAY, status=OrderStatus.ACCEPTED, reason="venue"
    )
    hook(reopened, apply_event(closed, reopened))
    hook(canceled, closed)
    assert metrics.count("ismet_orders_terminal_total", status="canceled") == 2


def test_digest_does_not_depend_on_set_ordering(store: BarStore) -> None:
    flags = {
        QualityFlag.STALE,
        QualityFlag.OUTLIER,
        QualityFlag.RESTATED,
        QualityFlag.LOCKED,
    }
    store.write([bar(quality=frozenset(flags))])
    rewrite = store.write(
        [bar(quality=frozenset(sorted(flags, key=str, reverse=True)))]
    )
    assert rewrite.unchanged == 1


def test_digest_is_the_same_in_every_process() -> None:
    """A set's order varies with the hash seed, so it must not reach the digest."""
    script = textwrap.dedent(
        """
        from datetime import datetime, timezone
        from ismet.models import Adjustment, Bar, Interval, QualityFlag, Symbol
        from ismet.store import _content_digest

        ts = datetime(2026, 3, 9, 14, 30, tzinfo=timezone.utc)
        print(_content_digest(Bar(
            symbol=Symbol(venue="XNYS", ticker="A"), exchange_ts=ts, received_ts=ts,
            interval=Interval.D1, open="1", high="1", low="1", close="1", volume="1",
            adjustment=Adjustment.RAW, quality=frozenset(QualityFlag),
        )))
        """
    )
    digests = {
        subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            check=True,
            env={**os.environ, "PYTHONHASHSEED": str(seed)},
        ).stdout
        for seed in (1, 2, 3, 4, 5, 6)
    }
    assert len(digests) == 1


def test_naive_times_are_refused_and_empty_checksums_differ(store: BarStore) -> None:
    start, end = span()
    with pytest.raises(ValidationError, match="as_of"):
        store.read(SYM, Interval.D1, start, end, as_of=datetime(2026, 3, 10))
    with pytest.raises(ValidationError, match="bar_ts"):
        store.revisions(SYM, Interval.D1, datetime(2026, 3, 9))
    empty = store.checksum(SYM, Interval.D1, start, end)
    store.write([bar()])
    assert store.checksum(SYM, Interval.D1, start, end) != empty


def test_missing_sessions_handle_venues_whose_date_differs_from_utc(
    store: BarStore,
) -> None:
    sydney = Symbol(venue="XASX", ticker="BHP")
    calendar = get_calendar("XASX")
    session = calendar.session(date(2025, 1, 6))
    assert session is not None and session.open.date() == date(2025, 1, 5)
    store.write([bar(session.open, symbol=sydney, session_date=None)])
    missing = store.missing_sessions(
        sydney, calendar, date(2025, 1, 6), date(2025, 1, 6)
    )
    assert missing == []


def test_dataframes_keep_decimals_exact_by_default() -> None:
    series = BarSeries([bar(close="10.123456789012345678")])
    assert series.to_records()[0]["close"] == D("10.123456789012345678")
    assert series.to_pandas()["close"].iloc[0] == D("10.123456789012345678")
    assert isinstance(series.to_pandas(lossy_float=True)["close"].iloc[0].item(), float)
    frame = series.to_polars()
    assert str(frame["close"].dtype).startswith("Decimal")
    assert frame["close"][0] == D("10.123456789012345678")
    assert series.to_polars(lossy_float=True)["close"].dtype.is_float()
    assert to_records(series)[0]["venue"] == "XNYS"
    assert len(to_pandas(series)) == len(to_polars(series)) == 1


def test_missing_optional_dependency_says_how_to_install(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_import = importlib.import_module

    def refuse(name: str, *args: object) -> object:
        if name in ("pandas", "polars"):
            raise ImportError(name)
        return real_import(name, *args)

    monkeypatch.setattr("importlib.import_module", refuse)
    with pytest.raises(ConfigError, match=r"ismet\[pandas\]"):
        to_pandas([])
    with pytest.raises(ConfigError, match=r"ismet\[polars\]"):
        to_polars([])


def test_metrics_hooks() -> None:
    metrics = InMemoryMetrics()
    assert isinstance(metrics, Metrics)
    hook = http_hook(metrics, provider="p")
    hook(RequestInfo("GET", "/x", 1, 0.25, 200))
    hook(RequestInfo("GET", "/x", 2, 0.5, None, TransportError("x")))
    labels = {"provider": "p", "method": "GET"}
    assert metrics.count("ismet_http_requests_total", outcome="ok", **labels) == 1
    assert metrics.count("ismet_http_requests_total", outcome="error", **labels) == 1
    assert metrics.count("nothing") == 0
    metrics.gauge("g", 2.0)
    assert list(metrics.gauges.values()) == [2.0]

    request = OrderRequest(
        client_order_id="C1",
        symbol=SYM,
        side=OrderSide.BUY,
        quantity="1",
        limit_price="1",
    )
    state = new_order(SubmitIntent(client_order_id="C1", ts=DAY, request=request))
    canceled = Canceled(client_order_id="C1", ts=DAY)
    order_hook = order_event_hook(metrics)
    order_hook(canceled, state)
    assert metrics.count("ismet_order_events_total", kind="canceled") == 1
    assert metrics.count("ismet_orders_terminal_total", status="canceled") == 0
    order_hook(canceled, apply_event(_submitting(state), canceled))
    assert metrics.count("ismet_orders_terminal_total", status="canceled") == 1
    break_hook(metrics)(ReconBreak(BreakKind.ORPHAN_ORDER, "x", "y"))
    assert metrics.count("ismet_recon_breaks_total", kind="orphan_order") == 1


def _submitting(state):  # type: ignore[no-untyped-def]
    from ismet.models.orders import SendStarted

    return apply_event(state, SendStarted(client_order_id="C1", ts=DAY))


class Gappy(MockProvider):
    name = "gappy"

    async def stream_quotes(self, symbols):  # type: ignore[no-untyped-def,override]
        for sequence in (1, 2, 2, 5):
            yield Quote(
                symbol=symbols[0],
                exchange_ts=DAY,
                received_ts=DAY,
                sequence=sequence,
                bid="1",
                ask="2",
            )

    async def stream_trades(self, symbols):  # type: ignore[no-untyped-def,override]
        async for quote in self.stream_quotes(symbols):
            yield await self._as_trade(quote)

    async def _as_trade(self, quote: Quote):  # type: ignore[no-untyped-def]
        from ismet.models import Trade

        return Trade(
            symbol=quote.symbol,
            exchange_ts=DAY,
            received_ts=DAY,
            sequence=quote.sequence,
            price="1",
            size="1",
        )


async def test_client_streams_drop_duplicates_and_flag_gaps() -> None:
    seen: list[int] = []
    async with IsmetClient([Gappy()]) as client:
        quotes = [
            q
            async for q in client.stream_quotes(
                ["ACME"], venue="XMOK", on_gap=lambda _, o: seen.append(o.missing)
            )
        ]
        trades = [t async for t in client.stream_trades(["ACME"], venue="XMOK")]
    assert [q.sequence for q in quotes] == [1, 2, 5]
    assert QualityFlag.AFTER_GAP in quotes[-1].quality
    assert seen == [2]
    assert [t.sequence for t in trades] == [1, 2, 5]
    assert client.calendar("XNYS").venue == "XNYS"


async def test_client_stream_can_treat_gaps_as_fatal() -> None:
    async with IsmetClient([Gappy()]) as client:
        with pytest.raises(SequenceGap):
            async for _ in client.stream_quotes(
                ["ACME"], venue="XMOK", gap_policy=GapPolicy.RAISE
            ):
                pass


def test_a_stale_replay_cannot_overwrite_a_newer_restatement(store: BarStore) -> None:
    store.write([bar(close="10", revision=0)])
    store.write([bar(close="11", revision=1)])
    result = store.write([bar(close="10", revision=0)])
    assert (result.inserted, result.unchanged, result.revised) == (0, 1, 0)
    span = (SYM, Interval.D1, DAY, DAY + timedelta(minutes=1))
    assert [b.close for b in store.read(*span)] == [D("11")]
    assert store.revisions(SYM, Interval.D1, DAY) == 2


def test_a_correction_without_a_revision_number_is_still_a_revision(
    store: BarStore,
) -> None:
    store.write([bar(close="10")])
    assert store.write([bar(close="11")]).revised == 1


def test_a_series_cannot_mix_adjustment_scales(store: BarStore) -> None:
    store.write([bar(close="10")])
    adjusted = bar(close="10.5", adjustment=Adjustment.SPLIT)
    with pytest.raises(ValidationError, match="split"):
        store.write([adjusted])
    span = (SYM, Interval.D1, DAY, DAY + timedelta(minutes=1))
    assert [(b.close, b.adjustment) for b in store.read(*span)] == [
        (D("10"), Adjustment.RAW)
    ]
    assert store.revisions(SYM, Interval.D1, DAY) == 1


def test_descriptive_symbol_fields_are_not_a_revision(store: BarStore) -> None:
    store.write([bar()])
    described = Symbol(venue="XNYS", ticker="ACME", vendor_codes={"polygon": "ACME"})
    span = (SYM, Interval.D1, DAY, DAY + timedelta(minutes=1))
    before = store.checksum(*span)
    result = store.write([bar(symbol=described)])
    assert (result.inserted, result.unchanged, result.revised) == (0, 1, 0)
    assert store.checksum(*span) == before


def _at(ts: datetime, symbol: Symbol) -> Bar:
    return bar(ts=ts, symbol=symbol, interval=Interval.H1, session_date=None)


ASX = Symbol(venue="XASX", ticker="BHP")


async def test_a_session_straddling_windows_with_no_bar_keeps_the_report_open(
    store: BarStore,
) -> None:
    """Sydney's Monday 2026-03-09 runs 23:00 Sunday to 05:00 Monday UTC.

    Each one-day UTC window holds a part of every session and none wholly, and the
    Tuesday session here got no bar on either side of midnight.
    """
    sunday = datetime(2026, 3, 8, tzinfo=UTC)
    answers = {
        sunday: [_at(sunday + timedelta(hours=23), ASX)],
        sunday + timedelta(days=1): [_at(sunday + timedelta(days=1, hours=1), ASX)],
        sunday + timedelta(days=2): [_at(sunday + timedelta(days=2, hours=23), ASX)],
    }

    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        return answers[start]

    report = await backfill(
        fetch,
        store,
        ASX,
        Interval.H1,
        sunday,
        sunday + timedelta(days=3),
        window=timedelta(days=1),
        calendar=get_calendar("XASX"),
    )
    assert report.sessions_missing == [date(2026, 3, 10)]
    assert not report.complete
    assert report.windows_incomplete == 1


async def test_a_straddling_session_is_found_by_a_bar_in_the_window_before(
    store: BarStore,
) -> None:
    sunday = datetime(2026, 3, 8, tzinfo=UTC)
    answers = {
        # Monday's session has its only bar before midnight, Tuesday's after.
        sunday: [_at(sunday.replace(hour=23), ASX)],
        sunday + timedelta(days=1): [_at(sunday + timedelta(days=1, hours=23), ASX)],
    }

    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        return answers[start]

    report = await backfill(
        fetch,
        store,
        ASX,
        Interval.H1,
        sunday,
        sunday + timedelta(days=2),
        window=timedelta(days=1),
        calendar=get_calendar("XASX"),
    )
    assert report.complete and report.sessions_missing == []


async def test_a_session_begun_before_the_requested_range_is_not_required(
    store: BarStore,
) -> None:
    """Only the last hour of that session was asked for, so it proves nothing."""
    start = datetime(2026, 3, 9, 4, tzinfo=UTC)  # inside Sydney's Monday session

    async def fetch(symbol, interval, begin, end):  # type: ignore[no-untyped-def]
        return [_at(datetime(2026, 3, 9, 23, tzinfo=UTC), ASX)]

    report = await backfill(
        fetch,
        store,
        ASX,
        Interval.H1,
        start,
        datetime(2026, 3, 10, tzinfo=UTC),
        window=timedelta(days=1),
        calendar=get_calendar("XASX"),
    )
    assert report.complete


async def test_a_weekend_window_from_utc_midnight_finishes_and_is_not_refetched(
    store: BarStore,
) -> None:
    """Saturday 00:00 UTC is still Friday evening in New York, after the close."""
    saturday = datetime(2026, 3, 7, tzinfo=UTC)
    calls: list[datetime] = []

    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        calls.append(start)
        return []

    span = (fetch, store, SYM, Interval.D1, saturday, saturday + timedelta(days=1))
    calendar = get_calendar("XNYS")
    first = await backfill(*span, window=timedelta(days=1), calendar=calendar)
    assert first.complete and first.windows_fetched == 1
    second = await backfill(*span, window=timedelta(days=1), calendar=calendar)
    assert second.complete and second.windows_skipped == 1
    assert calls == [saturday]


async def test_an_empty_window_cutting_through_a_session_is_not_proven_empty(
    store: BarStore,
) -> None:
    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        return []

    monday = datetime(2026, 3, 9, 15, tzinfo=UTC)  # the session is open
    report = await backfill(
        fetch,
        store,
        SYM,
        Interval.H1,
        monday,
        monday + timedelta(hours=1),
        window=timedelta(hours=1),
        calendar=get_calendar("XNYS"),
    )
    assert report.windows_empty_unproven == 1 and not report.complete


def flat_bar(ts: datetime, price: str, **kw: object) -> Bar:
    return bar(ts, close=price, open=price, high=price, low=price, **kw)


def test_a_read_refuses_a_range_on_two_price_scales(store: BarStore) -> None:
    next_day = DAY + timedelta(days=1)
    store.write([flat_bar(DAY, "100")])
    store.write([flat_bar(next_day, "50", adjustment=Adjustment.SPLIT)])
    whole = (SYM, Interval.D1, DAY, next_day + timedelta(minutes=1))
    with pytest.raises(ValidationError, match="split"):
        store.read(*whole)
    with pytest.raises(ValidationError, match="split"):
        store.checksum(*whole)
    assert [b.close for b in store.read(SYM, Interval.D1, DAY, next_day)] == [D("100")]


def test_the_digest_ignores_a_low_ambient_precision(store: BarStore) -> None:
    span_of_day = (SYM, Interval.D1, DAY, DAY + timedelta(minutes=1))
    with localcontext() as ctx:
        ctx.prec = 3
        store.write([bar(close="10.01")])
        assert store.write([bar(close="10.01")]).unchanged == 1
        assert store.write([bar(close="10.02")]).revised == 1
    assert store.read(*span_of_day)[0].close == D("10.02")


def test_a_restatement_past_the_default_precision_is_kept(store: BarStore) -> None:
    span_of_day = (SYM, Interval.D1, DAY, DAY + timedelta(minutes=1))
    store.write([bar(volume="1234567890123.123456789012345678")])
    restated = "1234567890123.123456789012345999"
    assert store.write([bar(volume=restated)]).revised == 1
    assert store.read(*span_of_day)[0].volume == D(restated)


async def test_a_skipped_window_cannot_hide_a_missing_session(store: BarStore) -> None:
    monday = datetime(2026, 3, 9, tzinfo=UTC)
    saturday = monday + timedelta(days=5)
    only_monday = bar(DAY, session_date=date(2026, 3, 9))

    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        return [only_monday]

    args = (fetch, store, SYM, Interval.D1, monday, saturday)
    first = await backfill(*args, window=timedelta(days=5))
    assert first.complete and first.windows_fetched == 1
    report = await backfill(*args, window=timedelta(days=5), calendar=XNYS)
    assert not report.complete
    assert report.windows_skipped == 0 and report.windows_incomplete == 1
    assert report.sessions_missing == [
        date(2026, 3, 10),
        date(2026, 3, 11),
        date(2026, 3, 12),
        date(2026, 3, 13),
    ]


async def test_a_window_finished_from_a_later_start_reopens_for_an_earlier_one(
    store: BarStore,
) -> None:
    # NYSE opens at 13:30 UTC on 2026-03-09, so a run from 14:00 does not require it.
    late = datetime(2026, 3, 9, 14, tzinfo=UTC)
    day = timedelta(days=1)
    tuesday_bar = bar(late + timedelta(hours=23), session_date=date(2026, 3, 10))

    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        return [b for b in [tuesday_bar] if start <= b.exchange_ts < end]

    end = late + day
    first = await backfill(
        fetch, store, SYM, Interval.D1, late, end, window=day, calendar=XNYS
    )
    assert first.complete
    again = await backfill(
        fetch, store, SYM, Interval.D1, late - day, end, window=day, calendar=XNYS
    )
    assert again.windows_skipped == 0
    assert again.sessions_missing == [date(2026, 3, 9)]
    assert not again.complete


async def test_a_clean_rerun_with_a_calendar_still_skips_without_fetching(
    store: BarStore,
) -> None:
    monday = datetime(2026, 3, 9, tzinfo=UTC)
    calls = 0

    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        return [bar(DAY, session_date=date(2026, 3, 9))]

    args = (fetch, store, SYM, Interval.D1, monday, monday + timedelta(days=1))
    assert (await backfill(*args, calendar=XNYS)).complete
    rerun = await backfill(*args, calendar=XNYS)
    assert rerun.complete and rerun.windows_skipped == 1
    assert calls == 1


def test_an_undated_daily_bar_outside_a_session_cannot_count_as_present(
    store: BarStore,
) -> None:
    midnight = datetime(2026, 3, 10, tzinfo=UTC)
    store.write([bar(midnight, session_date=None)])
    with pytest.raises(ValidationError, match="session_date"):
        store.missing_sessions(SYM, XNYS, date(2026, 3, 9), date(2026, 3, 10))


def test_an_undated_daily_bar_inside_a_session_takes_that_session(
    store: BarStore,
) -> None:
    store.write([bar(DAY, session_date=None)])
    assert store.missing_sessions(SYM, XNYS, date(2026, 3, 9), date(2026, 3, 10)) == [
        date(2026, 3, 10)
    ]


def test_records_carry_flags_derived_from_the_data() -> None:
    ts = DAY
    # A bid above the ask is crossed by definition.
    crossed = Quote(symbol=SYM, exchange_ts=ts, received_ts=ts, bid="11", ask="10")
    assert to_records([crossed])[0]["quality"] == "crossed"
    skewed = bar(ts, received_ts=ts - timedelta(seconds=5))
    assert to_records([skewed])[0]["quality"] == "clock_skew"
    assert to_records([bar(ts)])[0]["quality"] == ""


def test_polars_refuses_a_column_whose_shared_scale_overflows() -> None:
    pytest.importorskip("polars")
    # Each close fits alone. Together: 23 integer digits and 19 fractional digits.
    wide = flat_bar(DAY, "12345678901234567890123.5")
    fine = flat_bar(DAY + timedelta(days=1), "0.1234567890123456789")
    assert to_polars([wide])["close"].to_list() == [wide.close]
    assert to_polars([fine])["close"].to_list() == [fine.close]
    series = BarSeries([wide, fine])
    with pytest.raises(ValidationError, match="lossy_float"):
        series.to_polars()
    assert series.to_polars(lossy_float=True)["close"].null_count() == 0


def test_polars_keeps_exactly_38_digits_without_loss() -> None:
    pytest.importorskip("polars")
    # The polars Decimal type holds a precision of 38 at most.
    top = "1" * 19 + "." + "2" * 19
    frame = to_polars([flat_bar(DAY, top)])
    assert frame["close"].dtype.is_decimal()
    assert frame["close"].to_list() == [D(top)]
    with pytest.raises(ValidationError, match="lossy_float"):
        to_polars([flat_bar(DAY, "1" + top)])


def test_polars_names_the_way_out_when_decimals_are_too_wide() -> None:
    pytest.importorskip("polars")
    split = CorporateAction(ActionKind.SPLIT, date(2026, 3, 10), ratio=D(3))
    raw = bar(close="100", open="100", high="100", low="100", session_date=DAY.date())
    series = BarSeries(adjust_bars([raw], [split]))
    assert series[0].close != D(100)
    with pytest.raises(ValidationError, match="lossy_float"):
        series.to_polars()
    with pytest.raises(ValidationError, match="lossy_float"):
        to_polars(series)
    assert series.to_polars(lossy_float=True)["close"].dtype.is_float()


# Nested levels, integer gaps, longer intervals, and concurrent writers


def book_of(price: str) -> OrderBook:
    return OrderBook(
        symbol=SYM,
        exchange_ts=DAY,
        received_ts=DAY,
        bids=[Level(price=price, size="1")],
        asks=[],
    )


def test_polars_refuses_book_levels_that_share_too_wide_a_scale() -> None:
    pytest.importorskip("polars")
    books = [book_of("1" + "0" * 29), book_of("0." + "1" * 30)]
    with pytest.raises(ValidationError, match=r"bids\.price"):
        to_polars(books)


def test_polars_refuses_one_book_level_over_its_digit_limit() -> None:
    pytest.importorskip("polars")
    with pytest.raises(ValidationError, match=r"bids\.price"):
        to_polars([book_of("1." + "1" * 40)])


def test_book_levels_become_floats_only_when_asked() -> None:
    polars = pytest.importorskip("polars")
    frame = to_polars([book_of("1." + "1" * 40)], lossy_float=True)
    assert isinstance(frame["bids"][0][0]["price"], float)
    assert frame.schema["bids"] != polars.Null


def test_polars_keeps_book_levels_exact_when_they_fit() -> None:
    pytest.importorskip("polars")
    frame = to_polars([book_of("100.25"), book_of("0.5")])
    assert frame["bids"][0][0]["price"] == D("100.25")
    assert frame["bids"][1][0]["price"] == D("0.5")


def test_pandas_keeps_large_integers_exact_beside_a_missing_value() -> None:
    pytest.importorskip("pandas")
    sequence = 2**60 + 1
    stamp = datetime(2026, 3, 9, 15, 0, tzinfo=UTC) + timedelta(microseconds=123456)
    nanos = (int(stamp.timestamp()) * 10**9) + 123456789
    exact = Trade(
        symbol=SYM,
        exchange_ts=stamp,
        received_ts=stamp,
        price="1",
        size="1",
        exchange_ts_ns=nanos,
        sequence=sequence,
    )
    bare = Trade(symbol=SYM, exchange_ts=DAY, received_ts=DAY, price="1", size="1")
    frame = to_pandas([exact, bare])
    assert frame["exchange_ts_ns"][0] == nanos
    assert frame["sequence"][0] == sequence
    assert frame["sequence"][1] is None


async def test_a_calendar_is_refused_for_bars_longer_than_a_session(
    tmp_path: Path,
) -> None:
    async def fetch(*_: object) -> list[Bar]:
        raise AssertionError("nothing should be fetched")

    for interval in (Interval.W1, Interval.MO1):
        with (
            BarStore(tmp_path / "bars.db") as store,
            pytest.raises(ValidationError, match=interval.value),
        ):
            await backfill(
                fetch,
                store,
                SYM,
                interval,
                DAY,
                DAY + timedelta(days=30),
                calendar=XNYS,
            )


def test_a_concurrent_restatement_is_not_rolled_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "bars.db"
    with BarStore(path) as seed:
        seed.write([bar(close="10", revision=0)])
    errors: list[BaseException] = []

    def other_writer() -> None:
        try:
            with BarStore(path, clock=ManualClock(DAY)) as other:
                other.write([bar(close="10.8", revision=2)])
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=other_writer)
    digest = store_module._content_digest
    started: list[bool] = []

    def interleaved(item: Bar) -> str:
        if not started:
            started.append(True)
            thread.start()
            thread.join(0.3)
        return digest(item)

    monkeypatch.setattr(store_module, "_content_digest", interleaved)
    with BarStore(path, clock=ManualClock(DAY + timedelta(days=1))) as store:
        store.write([bar(close="10.5", revision=1)])
        thread.join(10)
        assert not errors
        got = store.read(SYM, Interval.D1, DAY, DAY + timedelta(days=1))
    assert [b.close for b in got] == [D("10.8")]
    assert got[0].revision == 2


def _month_starts(start: datetime, end: datetime) -> list[datetime]:
    moment = datetime(start.year, start.month, 1, tzinfo=UTC)
    found = []
    while moment < end:
        if moment >= start:
            found.append(moment)
        moment = datetime(
            moment.year + moment.month // 12, moment.month % 12 + 1, 1, tzinfo=UTC
        )
    return found


def _monthly_vendor(calls: list[datetime]):  # type: ignore[no-untyped-def]
    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        calls.append(start)
        return [bar(ts, interval=Interval.MO1) for ts in _month_starts(start, end)]

    return fetch


async def test_a_monthly_backfill_with_the_default_window_completes(
    store: BarStore,
) -> None:
    calls: list[datetime] = []
    span = (
        _monthly_vendor(calls),
        store,
        SYM,
        Interval.MO1,
        datetime(2025, 11, 2, tzinfo=UTC),
        datetime(2026, 3, 1, tzinfo=UTC),
    )
    first = await backfill(*span)
    assert first.complete and first.windows_empty_unproven == 0
    # December, January and February: March 1 is outside the half-open range.
    assert first.bars_inserted == 3
    calls.clear()
    second = await backfill(*span)
    assert second.complete and calls == []


async def test_a_backfill_ending_mid_period_does_not_stay_open_forever(
    store: BarStore,
) -> None:
    calls: list[datetime] = []

    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        calls.append(start)
        # Hourly bars on the hour.
        first = start.replace(minute=0, second=0, microsecond=0)
        if first < start:
            first += timedelta(hours=1)
        hours = []
        while first < end:
            hours.append(bar(first, interval=Interval.H1))
            first += timedelta(hours=1)
        return hours

    # Three one-hour windows would leave 02:10 to 02:15, which holds no hourly bar.
    begin = datetime(2026, 3, 9, 0, 10, tzinfo=UTC)
    span = (fetch, store, SYM, Interval.H1, begin, begin + timedelta(minutes=125))
    first = await backfill(*span, window=timedelta(hours=1))
    assert first.complete and first.bars_inserted == 2
    calls.clear()
    assert (await backfill(*span, window=timedelta(hours=1))).complete and calls == []


@pytest.mark.parametrize(
    ("interval", "window"),
    [
        (Interval.MO1, timedelta(days=30)),
        (Interval.W1, timedelta(days=6)),
        (Interval.H1, timedelta(minutes=59)),
    ],
)
async def test_a_window_that_can_miss_every_bar_is_refused(
    store: BarStore, interval: Interval, window: timedelta
) -> None:
    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        raise AssertionError("nothing should be fetched")

    begin = datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(ValidationError, match="window"):
        await backfill(
            fetch,
            store,
            SYM,
            interval,
            begin,
            begin + timedelta(days=400),
            window=window,
        )


async def test_a_refused_progress_write_counts_as_that_windows_failure(
    store: BarStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[datetime] = []

    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        calls.append(start)
        return [bar(start)]

    real = store.mark_window_done
    armed = {"locked": True}

    def locked_once(*args):  # type: ignore[no-untyped-def]
        if armed["locked"]:
            armed["locked"] = False
            raise sqlite3.OperationalError("database is locked")
        real(*args)

    monkeypatch.setattr(store, "mark_window_done", locked_once)
    span = (fetch, store, SYM, Interval.D1, DAY, DAY + timedelta(days=2))
    first = await backfill(*span, window=timedelta(days=1))
    assert calls == [DAY, DAY + timedelta(days=1)]  # the second window still ran
    assert first.windows_failed == 1 and first.windows_fetched == 1
    assert first.bars_inserted == 2 and not first.complete
    assert "locked" in first.failures[0][1]
    second = await backfill(*span, window=timedelta(days=1))
    assert second.complete and second.windows_skipped == 1


async def test_bars_written_before_a_later_failure_are_still_accounted_for(
    store: BarStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        return [bar(start)]

    def locked(*args):  # type: ignore[no-untyped-def]
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(store, "mark_window_done", locked)
    report = await backfill(
        fetch, store, SYM, Interval.D1, DAY, DAY + timedelta(days=1)
    )
    assert report.bars_fetched == 1 and report.bars_inserted == 1
    assert len(store.read(SYM, Interval.D1, DAY, DAY + timedelta(days=1))) == 1


async def test_a_corrupt_stored_row_fails_its_window_and_the_rest_continue(
    store: BarStore,
) -> None:
    store.write([bar(DAY)])
    connection = sqlite3.connect(store.path)
    connection.execute("UPDATE bars SET payload = '{bad'")
    connection.commit()
    connection.close()
    calls: list[datetime] = []

    async def fetch(symbol, interval, start, end):  # type: ignore[no-untyped-def]
        calls.append(start)
        return [bar(start)]

    report = await backfill(
        fetch,
        store,
        SYM,
        Interval.D1,
        DAY,
        DAY + timedelta(days=2),
        window=timedelta(days=1),
    )
    assert calls == [DAY, DAY + timedelta(days=1)]
    assert report.windows_failed == 1 and report.windows_fetched == 1


def _in_fresh_process(script: str, *args: str) -> str:
    """Run in a new interpreter, whose process-wide code registries start empty."""
    done = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    return done.stdout


WRITE_PRIVATE_CODES = """
    import sys
    from datetime import datetime, timezone
    from decimal import Decimal
    from ismet.models import Adjustment, Bar, Interval, Symbol
    from ismet.refdata import register_crypto_asset
    from ismet.refdata.codes import register_private_venue
    from ismet.store import BarStore

    register_crypto_asset("WIF")
    register_private_venue("ZABC", "A private venue")
    ts = datetime(2026, 3, 9, 14, 30, tzinfo=timezone.utc)
    bars = [
        Bar(
            symbol=Symbol(venue=venue, ticker="ACME"), exchange_ts=ts, received_ts=ts,
            interval=Interval.D1, open=Decimal("1.5"), high=Decimal("1.5"),
            low=Decimal("1.5"), close=Decimal("1.5"), volume=Decimal(1), currency="WIF",
            adjustment=Adjustment.RAW,
        )
        for venue in ("XNYS", "ZABC")
    ]
    with BarStore(sys.argv[1]) as store:
        store.write(bars)
"""


def test_a_bar_with_a_registered_currency_reads_in_a_new_process(
    tmp_path: Path,
) -> None:
    path = str(tmp_path / "bars.db")
    _in_fresh_process(WRITE_PRIVATE_CODES, path)
    out = _in_fresh_process(
        """
        import sys
        from datetime import datetime, timezone
        from ismet.models import Interval, Symbol
        from ismet.store import BarStore

        begin = datetime(2026, 3, 9, tzinfo=timezone.utc)
        end = datetime(2026, 3, 10, tzinfo=timezone.utc)
        with BarStore(sys.argv[1]) as store:
            symbol = Symbol(venue="XNYS", ticker="ACME")
            bars = store.read(symbol, Interval.D1, begin, end)
        print(bars[0].currency, bars[0].close)
        """,
        path,
    )
    assert out.split() == ["WIF", "1.5"]


def test_a_new_process_can_write_over_a_bar_with_a_registered_currency(
    tmp_path: Path,
) -> None:
    path = str(tmp_path / "bars.db")
    _in_fresh_process(WRITE_PRIVATE_CODES, path)
    out = _in_fresh_process(
        """
        import sys
        from datetime import datetime, timezone
        from decimal import Decimal
        from ismet.models import Adjustment, Bar, Interval, Symbol
        from ismet.refdata import register_crypto_asset
        from ismet.store import BarStore

        ts = datetime(2026, 3, 9, 14, 30, tzinfo=timezone.utc)
        register_crypto_asset("OTHER")
        same_slot = Bar(
            symbol=Symbol(venue="XNYS", ticker="ACME"), exchange_ts=ts, received_ts=ts,
            interval=Interval.D1, open=Decimal(2), high=Decimal(2), low=Decimal(2),
            close=Decimal(2), volume=Decimal(1), currency="OTHER",
            adjustment=Adjustment.RAW,
        )
        with BarStore(sys.argv[1]) as store:
            print(store.write([same_slot]).revised)
        """,
        path,
    )
    assert out.strip() == "1"


def test_adjusting_stored_bars_works_in_a_new_process(tmp_path: Path) -> None:
    path = str(tmp_path / "bars.db")
    _in_fresh_process(WRITE_PRIVATE_CODES, path)
    out = _in_fresh_process(
        """
        import sys
        from datetime import date, datetime, timezone
        from decimal import Decimal
        from ismet.calendars import get_calendar
        from ismet.models import Interval, Symbol
        from ismet.refdata.master import ActionKind, CorporateAction, adjust_bars
        from ismet.store import BarStore

        symbol = Symbol(venue="XNYS", ticker="ACME")
        begin = datetime(2026, 3, 9, tzinfo=timezone.utc)
        end = datetime(2026, 3, 10, tzinfo=timezone.utc)
        with BarStore(sys.argv[1]) as store:
            bars = store.read(symbol, Interval.D1, begin, end)
        split = CorporateAction(
            kind=ActionKind.SPLIT, ex_date=date(2026, 3, 11), ratio=Decimal(3)
        )
        adjusted = adjust_bars(bars, [split], calendar=get_calendar("XNYS"))
        print(adjusted[0].close, adjusted[0].currency)
        """,
        path,
    )
    assert out.split() == ["0.5", "WIF"]


def test_a_stored_code_of_the_wrong_shape_is_still_refused(store: BarStore) -> None:
    store.write([bar(DAY, currency="USD")])
    connection = sqlite3.connect(store.path)
    rows = connection.execute("SELECT payload FROM bars").fetchall()
    tampered = rows[0][0].replace('"currency":"USD"', '"currency":"NOT A CODE"')
    assert tampered != rows[0][0]
    connection.execute("UPDATE bars SET payload = ?", (tampered,))
    connection.commit()
    connection.close()
    with pytest.raises(ValueError, match="currency"):
        store.read(SYM, Interval.D1, DAY, DAY + timedelta(days=1))


def test_code_checks_are_back_on_after_reading_stored_rows(store: BarStore) -> None:
    store.write([bar(DAY)])
    store.read(SYM, Interval.D1, DAY, DAY + timedelta(days=1))
    with pytest.raises(ValueError, match="unknown venue"):
        Symbol(venue="ZZZY", ticker="ACME")
    with pytest.raises(ValueError, match="unknown currency"):
        bar(DAY, currency="NOTREGISTERED")
