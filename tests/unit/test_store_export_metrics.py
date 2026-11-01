from __future__ import annotations

import importlib
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from ismet import IsmetClient
from ismet.calendars import get_calendar
from ismet.errors import ConfigError, SequenceGap, TransportError, ValidationError
from ismet.execution import BreakKind, ReconBreak, apply_event, new_order
from ismet.export import BarSeries, to_pandas, to_polars, to_records
from ismet.marketdata import GapPolicy
from ismet.models import Adjustment, Bar, Interval, QualityFlag, Quote, Symbol
from ismet.models.orders import (
    Canceled,
    OrderRequest,
    OrderSide,
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
from ismet.store import BarStore, backfill
from ismet.transport.clock import ManualClock
from ismet.transport.http import RequestInfo

UTC = timezone.utc
D = Decimal
SYM = Symbol(venue="XNYS", ticker="ACME")
DAY = datetime(2026, 3, 9, 14, 30, tzinfo=UTC)


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
    assert store.read(
        SYM, Interval.D1, *span(), as_of=datetime(2030, 1, 1, tzinfo=UTC)
    )[0].close == D("10.30")


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
