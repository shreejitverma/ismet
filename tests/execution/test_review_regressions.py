"""One test per defect found by independent review, each reproducing its scenario."""

from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from ismet.accounting import PositionLedger, build_ledger, value_books
from ismet.calendars import AlwaysOpenCalendar, get_calendar
from ismet.errors import (
    AmbiguousOutcome,
    ConfigError,
    JournalError,
    KillSwitchEngaged,
    RiskRejected,
    TransportError,
    ValidationError,
)
from ismet.execution import (
    BreakKind,
    Journal,
    MemoryJournal,
    OrderManager,
    RecordKind,
    RiskConfig,
    RiskContext,
    RiskEngine,
    RiskLimits,
    SqliteJournal,
    events_of,
    new_order,
)
from ismet.marketdata import BookBuilder, BookDelta, Issue, QualityMonitor
from ismet.models import (
    AssetClass,
    Bar,
    Interval,
    Level,
    ListingStatus,
    Quote,
    Rounding,
    Symbol,
)
from ismet.models.decimals import round_to_step
from ismet.models.orders import (
    Fill,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    ReplaceRequested,
    Restated,
    ShortSaleMark,
    SubmitIntent,
    TimeInForce,
    TradingRules,
)
from ismet.refdata.master import ActionKind, CorporateAction, adjust_bars
from ismet.testing.simvenue import SIM_VENUE, Fault, SimVenue, sim_instrument
from ismet.transport.backoff import ExponentialBackoff
from ismet.transport.clock import ManualClock
from ismet.transport.http import HttpTransport
from ismet.transport.ws import SequenceOutcome, SequenceTracker

from .conftest import (
    ACME,
    Factory,
    count_kill_polls,
    order,
    permissive_config,
    until,
)

D = Decimal
S = OrderStatus
UTC = timezone.utc
NOW = datetime(2026, 3, 9, 15, 0, tzinfo=UTC)
BUY, SELL = OrderSide.BUY, OrderSide.SELL
_NO_DELAY = ExponentialBackoff(base=0, jitter=False)


# Delivery ambiguity in the transport


def accepted_handler(response: httpx.Response):  # type: ignore[no-untyped-def]
    return httpx.MockTransport(lambda _: response)


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, text="not json"),
        httpx.Response(200, text=""),
        httpx.Response(302, headers={"location": "/elsewhere"}),
    ],
)
async def test_an_unreadable_answer_to_an_order_post_is_ambiguous(
    response: httpx.Response,
) -> None:
    async with HttpTransport("https://v", transport=accepted_handler(response)) as t:
        with pytest.raises(AmbiguousOutcome):
            await t.post_json("/orders", {})


@pytest.mark.parametrize("exc", [httpx.ReadTimeout, httpx.ReadError])
async def test_declared_idempotent_post_is_still_ambiguous_after_retries(
    exc: type[httpx.HTTPError],
) -> None:
    calls: list[int] = []

    def boom(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        raise exc("lost", request=request)

    async with HttpTransport("https://v", transport=httpx.MockTransport(boom)) as t:
        t.retry_policy = replace(t.retry_policy, backoff=_NO_DELAY)
        with pytest.raises(AmbiguousOutcome):
            await t.post_json("/orders", {}, idempotent=True)
    assert len(calls) == t.retry_policy.max_attempts


# The send path


async def test_cancel_during_the_send_started_write_stops_the_send(
    make_manager: Factory, venue: SimVenue
) -> None:
    holder: list[OrderManager] = []

    def before_send(step: str, cid: str) -> None:
        if step == "after_intent":
            holder.append(asyncio.create_task(holder[0].cancel(cid)))  # type: ignore[arg-type]

    oms = make_manager(checkpoint=before_send)
    holder.append(oms)
    await oms.start()
    try:
        await venue.drain()
        state = await oms.submit(order("RS1", price="9.50"))
        await holder[1]  # type: ignore[misc]
        assert oms.order("RS1").status is S.REJECTED, state
        assert venue.attempts["RS1"] == 0
    finally:
        await oms.abort()


async def test_kill_during_the_send_started_write_stops_the_send(
    make_manager: Factory, venue: SimVenue
) -> None:
    holder: list[OrderManager] = []

    def after_started(step: str, _: str) -> None:
        if step == "after_send_started":
            holder[0]._kill = holder[0]._kill.__class__(True, "halt")

    oms = make_manager(checkpoint=after_started)
    holder.append(oms)
    await oms.start()
    try:
        await venue.drain()
        state = await oms.submit(order("RS2", price="9.50"))
        assert state.status is S.REJECTED and state.reject_code == "KILL_SWITCH"
        assert venue.attempts["RS2"] == 0
    finally:
        await oms.abort()


async def test_concurrent_conflicting_submits_cannot_share_one_order(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    results = await asyncio.gather(
        oms.submit(order("RS3", price="9.50")),
        oms.submit(order("RS3", price="9.40", quantity="999")),
        return_exceptions=True,
    )
    assert sum(isinstance(r, ValidationError) for r in results) == 1
    assert venue.attempts["RS3"] == 1


async def test_a_venue_duplicate_id_is_resolved_by_query_not_treated_as_a_rejection(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.add_external_order(order("RS4", price="9.50"))
    state = await oms.submit(order("RS4", price="9.50"))
    assert state.status is S.UNKNOWN
    await oms.reconcile()
    assert oms.order("RS4").status is S.ACCEPTED


class IntentGate(MemoryJournal):
    """Holds the write of an order's intent open, so a test can cancel during it."""

    def __init__(self) -> None:
        super().__init__()
        self.writing = threading.Event()
        self.release = threading.Event()

    def append(self, *args: object, **kw: object):  # type: ignore[no-untyped-def]
        if kw.get("unique_intent"):
            self.writing.set()
            assert self.release.wait(5)
        return super().append(*args, **kw)  # type: ignore[arg-type]


async def test_cancelled_caller_cannot_split_the_journal_from_memory(
    venue: SimVenue, clock: ManualClock
) -> None:
    journal = IntentGate()
    oms = OrderManager(
        venue, journal, RiskEngine(permissive_config()), clock=clock, stream_settle=None
    )
    await oms.start()
    try:
        await venue.drain()
        task = asyncio.create_task(oms.submit(order("RS5", price="9.50")))
        assert await asyncio.to_thread(journal.writing.wait, 5)
        task.cancel()
        await asyncio.sleep(0.01)
        journal.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        journalled = [r.client_order_id for r in journal.records()]
        assert "RS5" in journalled
        state = oms.order("RS5")
        assert state.status is S.REJECTED
        assert state.reject_code == "CANCELED_BEFORE_SEND"
        assert (await oms.submit(order("RS5", price="9.50"))).status is S.REJECTED
        assert venue.attempts["RS5"] == 0
    finally:
        await oms.abort()


async def test_failed_start_leaves_no_thread_or_started_state(
    venue: SimVenue, clock: ManualClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    import threading

    async def boom() -> list[object]:
        raise ValidationError("account service down")

    monkeypatch.setattr(venue, "positions", boom)
    oms = OrderManager(
        venue, MemoryJournal(), RiskEngine(permissive_config()), clock=clock
    )
    with pytest.raises(ValidationError):
        await oms.start()
    assert not [t for t in threading.enumerate() if t.name.startswith("ismet-journal")]
    with pytest.raises(ConfigError):
        await oms.submit(order("RS6", price="9.50"))


async def test_restarting_one_instance_rebuilds_rather_than_replays_on_top(
    make_manager: Factory, venue: SimVenue
) -> None:
    oms = make_manager()
    await oms.start()
    await venue.drain()
    await oms.submit(order("RS7", price="9.50"))
    await venue.drain()
    await oms.stop()
    await oms.start()
    try:
        assert not oms.order("RS7").anomalies
        assert oms.order("RS7").status is S.ACCEPTED
    finally:
        await oms.abort()


async def test_truncated_journal_is_refused_given_a_saved_head(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    journal = SqliteJournal(journal_path)
    journal.append(RecordKind.ANOMALY, "{}")
    oms = OrderManager(venue, journal, RiskEngine(permissive_config()), clock=clock)
    with pytest.raises(JournalError, match="truncated or replaced"):
        await oms.start(expected_head_hash="f" * 64)
    good = OrderManager(venue, journal, RiskEngine(permissive_config()), clock=clock)
    await good.start(expected_head_hash=journal.head_hash())
    await good.abort()
    journal.close()


async def test_recovered_submitting_order_blocks_new_risk_while_unresolved(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    class Crash(BaseException):
        pass

    def crash(step: str, _: str) -> None:
        if step == "after_send_started":
            raise Crash

    journal = SqliteJournal(journal_path)
    first = OrderManager(
        venue, journal, RiskEngine(permissive_config()), clock=clock, checkpoint=crash
    )
    await first.start()
    with pytest.raises(Crash):
        await first.submit(order("RS8", price="9.50"))
    await first.abort()
    journal.close()

    journal = SqliteJournal(journal_path)
    second = OrderManager(
        venue,
        journal,
        RiskEngine(permissive_config()),
        clock=clock,
        reconcile_interval=3600,
        unknown_grace=3600,
    )
    await second.start()
    try:
        assert second.order("RS8").status is S.UNKNOWN
        with pytest.raises(RiskRejected) as caught:
            await second.submit(order("RS9", price="9.40"))
        assert caught.value.check == "unknown_order"
    finally:
        await second.abort()
        journal.close()


# Kill switch


async def test_pending_cancel_is_sent_again_by_the_sweep(
    make_manager: Factory,
    venue: SimVenue,
    clock: ManualClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oms = make_manager()
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("KS1", price="9.50"))
        await venue.drain()
        real_cancel = venue.cancel_order
        blackhole = {"on": True}

        async def lost_cancel(cid: str, symbol: Symbol) -> None:
            if not blackhole["on"]:
                await real_cancel(cid, symbol)

        monkeypatch.setattr(venue, "cancel_order", lost_cancel)
        await oms.engage_kill("halt")
        assert oms.order("KS1").status is S.PENDING_CANCEL
        blackhole["on"] = False
        clock.advance(5)
        state = await oms.wait_for("KS1", lambda o: o.status is S.CANCELED, 3)
        assert state.is_terminal
    finally:
        await oms.abort()


async def test_a_cancel_that_raises_an_adapter_bug_is_retried_not_stuck(
    make_manager: Factory, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    oms = make_manager()
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("KS2", price="9.50"))
        await venue.drain()
        real_cancel = venue.cancel_order
        failures = {"left": 2}

        async def flaky(cid: str, symbol: Symbol) -> None:
            if failures["left"]:
                failures["left"] -= 1
                raise RuntimeError("adapter bug")
            await real_cancel(cid, symbol)

        monkeypatch.setattr(venue, "cancel_order", flaky)
        await oms.engage_kill("halt")
        state = await oms.wait_for("KS2", lambda o: o.status is S.CANCELED, 5)
        assert state.is_terminal
    finally:
        await oms.abort()


async def test_an_automatic_halt_cannot_stop_an_operators_cancel_sweep(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.engage_kill("operator halt", operator="ops", cancel_open=True)
    await oms._halt("overfill")
    await oms.engage_kill("again", operator="x", cancel_open=False)
    assert oms.kill_state.engaged and oms.kill_state.cancel_open
    assert oms.kill_state.reason == "operator halt"


async def test_keep_orders_kill_can_be_widened_to_cancel(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("KS3", price="9.50"))
    await venue.drain()
    await oms.engage_kill("first", cancel_open=False)
    assert oms.order("KS3").status is S.ACCEPTED
    await oms.engage_kill("second", cancel_open=True)
    assert oms.kill_state.cancel_open and oms.kill_state.reason == "first"
    await oms.wait_for("KS3", lambda o: o.is_terminal, 3)


class FlakyJournal(MemoryJournal):
    failing = False

    def append(self, *args: object, **kw: object):  # type: ignore[no-untyped-def]
        if self.failing:
            raise JournalError("database is locked")
        return super().append(*args, **kw)  # type: ignore[arg-type]


async def test_a_kill_that_missed_the_journal_is_written_when_it_recovers(
    venue: SimVenue, clock: ManualClock
) -> None:
    journal = FlakyJournal()
    oms = OrderManager(
        venue,
        journal,
        RiskEngine(permissive_config()),
        clock=clock,
        kill_poll_interval=0.001,
    )
    await oms.start()
    try:
        journal.failing = True
        await oms.engage_kill("disk trouble", cancel_open=False)
        assert not journal.kill_state().engaged
        journal.failing = False
        for _ in range(500):
            if journal.kill_state().engaged:
                break
            await asyncio.sleep(0.001)
        assert journal.kill_state().engaged
        assert journal.kill_state().reason == "disk trouble"
    finally:
        await oms.abort()


# Fills are never lost


class NoAccount(SimVenue):
    positions = None  # type: ignore[assignment]
    balances = None  # type: ignore[assignment]


def account_free(clock: ManualClock) -> SimVenue:
    venue = NoAccount(clock=clock, instruments=[sim_instrument("ACME")])
    venue.set_quote(ACME, "9.99", "10.01")
    return venue


def bare_manager(venue: SimVenue, clock: ManualClock) -> OrderManager:
    return OrderManager(
        venue,
        MemoryJournal(),
        RiskEngine(permissive_config()),
        clock=clock,
        assume_flat=True,
        unknown_grace=0.0,
        reconcile_interval=3600,
        stream_settle=None,
        kill_poll_interval=0.01,
    )


async def test_order_is_not_closed_while_its_fills_cannot_be_recovered(
    clock: ManualClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    venue = account_free(clock)
    oms = bare_manager(venue, clock)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("FL1", price="9.50"))
        await venue.drain()
        venue.report_faults.drop_next = 1
        venue.trade_through(ACME, "9.50", "100")
        real = venue.fills_since

        async def down(*_: object) -> list[Fill]:
            raise ValidationError("fills service down")

        monkeypatch.setattr(venue, "fills_since", down)
        report = await oms.reconcile()
        assert not report.complete
        assert oms.order("FL1").is_open and oms.order("FL1").filled_quantity == 0
        monkeypatch.setattr(venue, "fills_since", real)
        await oms.reconcile()
        state = oms.order("FL1")
        assert state.status is S.FILLED and state.filled_quantity == 100
        assert oms.net_position(ACME) == 100
    finally:
        await oms.abort()


async def test_venue_purging_a_filled_order_does_not_lose_the_fill(
    clock: ManualClock,
) -> None:
    venue = account_free(clock)
    oms = bare_manager(venue, clock)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("FL2", price="9.50"))
        await venue.drain()
        venue.report_faults.drop_next = 1
        venue.trade_through(ACME, "9.50", "100")
        venue.forget_order("FL2")
        report = await oms.reconcile()
        assert oms.order("FL2").status is S.FILLED
        assert oms.net_position(ACME) == 100
        # The fill explains the disappearance, so this is not a break and must not halt.
        assert not report.breaks and not oms.kill_state.engaged
    finally:
        await oms.abort()


async def test_order_that_arrived_and_filled_while_unknown_is_not_declared_rejected(
    clock: ManualClock,
) -> None:
    venue = account_free(clock)
    oms = bare_manager(venue, clock)
    await oms.start()
    try:
        await venue.drain()
        venue.faults.append(Fault.LOSE_RESPONSE)
        venue.report_faults.drop_next = 2
        await oms.submit(order("FL3", price="10.01"))  # fills immediately at the venue
        venue.forget_order("FL3")
        await oms.reconcile()
        state = oms.order("FL3")
        assert state.status is S.FILLED and state.filled_quantity == 100
    finally:
        await oms.abort()


async def test_venue_purge_with_no_fills_halts_new_orders(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("FL4", price="9.50"))
    await venue.drain()
    venue.forget_order("FL4")
    report = await oms.reconcile()
    assert [b.kind for b in report.breaks] == [BreakKind.MISSING_AT_VENUE]
    assert oms.kill_state.engaged


async def test_lost_replace_report_is_adopted_from_the_venue(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("FL5", price="9.50"))
    await venue.drain()
    venue.report_faults.drop_next = 1
    await oms.amend("FL5", quantity=D(1000))
    await venue.drain()
    await oms.reconcile()
    state = oms.order("FL5")
    assert state.quantity == 1000 and state.leaves_quantity == 1000
    assert state.status is S.ACCEPTED


async def test_replace_the_venue_never_applied_returns_to_old_terms(
    oms: OrderManager, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    await venue.drain()
    await oms.submit(order("FL6", price="9.50"))
    await venue.drain()

    async def ignored(*_: object, **__: object) -> None:
        return None

    monkeypatch.setattr(venue, "amend_order", ignored)
    state = await oms.amend("FL6", quantity=D(500))
    assert state.status is S.PENDING_REPLACE
    assert state.leaves_quantity == 500  # worst case while it is undecided
    await oms.reconcile()
    state = oms.order("FL6")
    assert state.status is S.ACCEPTED and state.quantity == 100
    assert state.leaves_quantity == 100


# Reconciliation that cannot see stops trading


async def test_failed_venue_queries_stop_new_orders_until_a_pass_succeeds(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.fail_queries = 100_000  # background passes keep retrying, and keep failing
    report = await oms.reconcile()
    assert not report.complete
    with pytest.raises(RiskRejected) as caught:
        await oms.submit(order("BL1", price="9.50"))
    assert caught.value.check == "reconciliation"
    assert venue.attempts["BL1"] == 0
    venue.fail_queries = 0
    assert (await oms.reconcile()).clean
    assert (await oms.submit(order("BL2", price="9.50"))).status is S.ACCEPTED


async def test_an_adapter_bug_in_one_status_query_does_not_disable_reconciliation(
    oms: OrderManager, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    await venue.drain()
    await oms.submit(order("BL3", price="9.50"))
    await venue.drain()

    async def bug(*_: object) -> None:
        raise KeyError("adapter bug")

    monkeypatch.setattr(venue, "order_status", bug)
    report = await oms.reconcile()
    assert not report.complete
    assert any("KeyError" in b.detail for b in report.breaks)
    with pytest.raises(RiskRejected):
        await oms.submit(order("BL4", price="9.50"))


async def test_a_dead_execution_stream_stops_new_orders(
    oms: OrderManager, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    await venue.drain()

    def refused():  # type: ignore[no-untyped-def]
        raise TransportError("simulated: stream endpoint down")

    # Reconnects are jittered from zero, so the stream must stay down to be observed.
    monkeypatch.setattr(venue, "stream_executions", refused)
    venue.disconnect_stream()
    await until(lambda: not oms.stream_healthy)
    with pytest.raises(RiskRejected) as caught:
        await oms.submit(order("BL5", price="9.50"))
    assert caught.value.check == "execution_stream"


async def test_stream_reconnects_even_if_its_reconcile_keeps_failing(
    oms: OrderManager, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    await venue.drain()

    async def bug(*_: object) -> list[object]:
        raise KeyError("adapter bug")

    monkeypatch.setattr(venue, "open_orders", bug)
    venue.disconnect_stream()
    await until(lambda: not oms.stream_healthy)
    await until(lambda: bool(venue._subscribers))


async def test_failed_protective_cancel_keeps_the_order_unknown(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.faults.append(Fault.LOSE_REQUEST)
    await oms.submit(order("BL6", price="9.50"))
    venue.fail_cancels = 1
    report = await oms.reconcile()
    assert not report.complete
    assert oms.order("BL6").status is S.UNKNOWN


async def test_a_found_order_restarts_the_grace_period(
    make_manager: Factory,
    venue: SimVenue,
    clock: ManualClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oms = make_manager(unknown_grace=5.0)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("BL7", price="9.50"))
        await venue.drain()
        real = venue.order_status

        async def transient(*_: object) -> None:
            return None

        monkeypatch.setattr(venue, "order_status", transient)
        await oms.reconcile()  # starts the timer
        monkeypatch.setattr(venue, "order_status", real)
        await oms.reconcile()  # found: the timer must reset
        clock.advance(3600)
        monkeypatch.setattr(venue, "order_status", transient)
        await oms.reconcile()  # a single miss an hour later
        monkeypatch.setattr(venue, "order_status", real)
        snapshot = await venue.order_status("BL7", ACME)
        assert snapshot is not None and snapshot.status is S.ACCEPTED
    finally:
        await oms.abort()


async def test_a_fill_for_an_unknown_order_keeps_its_details(
    oms: OrderManager, venue: SimVenue, journal: SqliteJournal
) -> None:
    await venue.drain()
    venue.add_external_order(order("BL8", price="9.50"))
    venue.trade_through(ACME, "9.50", "7")
    await venue.drain()
    records = [r for r in journal.records() if r.client_order_id == "BL8"]
    assert any(
        r.kind is RecordKind.ANOMALY and '\\"quantity\\":\\"7\\"' in r.payload
        for r in records
    )


async def test_order_filling_during_the_open_orders_call_is_not_an_orphan(
    oms: OrderManager, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    await venue.drain()
    await oms.submit(order("BL9", price="9.50"))
    await venue.drain()
    real = venue.open_orders

    async def stale_list(symbol: Symbol | None = None) -> list[object]:
        listing = await real(symbol)
        venue.trade_through(ACME, "9.50", "100")
        await venue.drain()
        return listing  # type: ignore[return-value]

    monkeypatch.setattr(venue, "open_orders", stale_list)
    report = await oms.reconcile()
    assert BreakKind.ORPHAN_ORDER not in {b.kind for b in report.breaks}
    assert not oms.kill_state.engaged


async def test_fill_during_the_positions_call_is_not_a_break(
    oms: OrderManager, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    await venue.drain()
    await oms.submit(order("BL10", price="9.50"))
    await venue.drain()
    real = venue.positions
    calls = {"n": 0}

    async def stale_positions() -> list[object]:
        calls["n"] += 1
        before = await real()
        if calls["n"] == 1:
            venue.trade_through(ACME, "9.50", "100")
            await venue.drain()
        return before  # type: ignore[return-value]

    monkeypatch.setattr(venue, "positions", stale_positions)
    report = await oms.reconcile()
    assert BreakKind.POSITION_MISMATCH not in {b.kind for b in report.breaks}
    assert not oms.kill_state.engaged


async def test_risk_changes_are_journalled(
    oms: OrderManager, journal: SqliteJournal
) -> None:
    config = permissive_config(default=RiskLimits(max_order_quantity=D(1)))
    await oms.update_risk_config(config, operator="ops")
    assert [r for r in journal.records() if r.kind is RecordKind.CONFIG_CHANGE]
    with pytest.raises(RiskRejected):
        await oms.submit(order("CF1", quantity="5"))
    with pytest.raises(ValidationError):
        await oms.update_risk_config(config, operator="")


# Instrument and exposure


async def test_reference_data_expires_so_a_halt_is_seen(
    make_manager: Factory, venue: SimVenue, clock: ManualClock
) -> None:
    oms = make_manager(instrument_ttl=60.0)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("IN1", price="9.50"))
        halted = sim_instrument("ACME", status=ListingStatus.HALTED)
        venue.add_instrument(halted)
        clock.advance(61)
        with pytest.raises(RiskRejected) as caught:
            await oms.submit(order("IN2", price="9.40"))
        assert caught.value.check == "instrument"
    finally:
        await oms.abort()


async def test_failed_reference_refresh_refuses_the_order(
    make_manager: Factory,
    venue: SimVenue,
    clock: ManualClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oms = make_manager(instrument_ttl=60.0)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("IN3", price="9.50"))

        async def down(_: Symbol) -> None:
            raise ValidationError("down")

        monkeypatch.setattr(venue, "instrument", down)
        clock.advance(61)
        with pytest.raises(RiskRejected):
            await oms.submit(order("IN4", price="9.40"))
    finally:
        await oms.abort()


async def test_gross_exposure_keeps_the_multiplier_after_a_restart(
    journal_path: Path, clock: ManualClock
) -> None:
    venue = SimVenue(clock=clock, instruments=[sim_instrument("ACME", multiplier="50")])
    venue.set_quote(ACME, "9.99", "10.01")
    tight = permissive_config(default=RiskLimits(max_gross_notional=D(100_000)))

    def build() -> tuple[OrderManager, SqliteJournal]:
        journal = SqliteJournal(journal_path)
        return (
            OrderManager(
                venue, journal, RiskEngine(tight), clock=clock, reconcile_interval=3600
            ),
            journal,
        )

    first, journal = build()
    await first.start()
    await venue.drain()
    await first.submit(order("GR1", price="10.01", quantity="30"))
    await first.wait_for("GR1", lambda o: o.is_terminal, 2)
    assert first._gross_notional() == D("15015.00")
    await first.abort()
    journal.close()

    second, journal = build()
    await second.start()
    try:
        # Marks are not trusted across a restart, so gross is unknown until a quote.
        assert second._gross_notional() is None
        # With the multiplier of 50, the position and these 200 lots come to about
        # 115000, over the limit. Without it they come to about 2300 and would pass.
        with pytest.raises(RiskRejected):
            await second.submit(order("GR2", price="9.50", quantity="200"))
    finally:
        await second.abort()
        journal.close()


async def test_a_zero_limit_price_is_a_price_not_a_missing_value(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("GR3", price="0.00"))
    await venue.drain()
    assert oms._gross_notional() == 0


async def test_cancelling_an_old_order_does_not_rewind_the_last_price(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("GR4", price="9.50"))
    await venue.drain()
    venue.trade_through(ACME, "9.50", "10")
    await oms.wait_for("GR4", lambda o: o.filled_quantity == 10, 2)
    await oms.submit(order("GR5", price="10.01", quantity="1"))
    await oms.wait_for("GR5", lambda o: o.is_terminal, 2)
    assert oms._mark(ACME) == D("10.01")
    await oms.cancel("GR4")
    await oms.wait_for("GR4", lambda o: o.is_terminal, 2)
    assert oms._mark(ACME) == D("10.01")


async def test_price_only_amend_is_judged_on_leaves_not_total(
    make_manager: Factory, clock: ManualClock
) -> None:
    venue = SimVenue(
        clock=clock,
        instruments=[sim_instrument("ACME")],
        initial_positions={ACME: D(100)},
    )
    venue.set_quote(ACME, "9.99", "10.01")
    journal = MemoryJournal()
    oms = OrderManager(
        venue,
        journal,
        RiskEngine(
            permissive_config(allow_short=False),
        ),
        clock=clock,
        reconcile_interval=3600,
    )
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("AM1", side=SELL, price="10.50", quantity="100"))
        await venue.drain()
        venue.trade_through(ACME, "10.50", "90")
        await oms.wait_for("AM1", lambda o: o.filled_quantity == 90, 2)
        assert oms.net_position(ACME) == 10
        state = await oms.amend("AM1", limit_price=D("10.60"))
        await oms.wait_for("AM1", lambda o: o.limit_price == D("10.60"), 2)
        assert state.status is not S.REJECTED
    finally:
        await oms.abort()


async def test_amend_cannot_put_a_limit_price_on_a_market_order(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(
        order("AM2", price=None, order_type=OrderType.STOP, stop_price="10.50")
    )
    await venue.drain()
    with pytest.raises(ValidationError, match="invalid amend"):
        await oms.amend("AM2", limit_price=D("9.00"))


# The risk engine


SYM = Symbol(venue="XNYS", ticker="ACME")
RULES = TradingRules(
    order_types=frozenset(OrderType),
    time_in_force=frozenset(TimeInForce),
    supports_short=True,
)
QUOTE = Quote(symbol=SYM, exchange_ts=NOW, received_ts=NOW, bid="100", ask="101")
_seq = {"n": 0}


def instrument(**kw: object) -> object:
    from ismet.models import Instrument

    return Instrument.model_validate(
        {
            "symbol": SYM,
            "name": "A",
            "currency": "USD",
            "tick_size": "0.01",
            "asset_class": AssetClass.EQUITY,
            "status": ListingStatus.ACTIVE,
            **kw,
        }
    )


def req(**kw: object) -> OrderRequest:
    _seq["n"] += 1
    fields: dict[str, object] = {
        "client_order_id": f"Q{_seq['n']}",
        "symbol": SYM,
        "side": BUY,
        "quantity": "10",
        "limit_price": "100.00",
    }
    fields.update(kw)
    return OrderRequest.model_validate(fields)


def decide(request: OrderRequest, limits: RiskLimits | None = None, **kw: object):  # type: ignore[no-untyped-def]
    config = RiskConfig(
        default=limits or RiskLimits(),
        duplicate_window_seconds=D(0),
        allow_short=True,
    )
    fields: dict[str, object] = {
        "request": request,
        "instrument": instrument(),
        "reference": QUOTE,
        "now": NOW,
        "position": D(0),
        "calendar": get_calendar("XNYS"),
        "rules": RULES,
        "gross_notional": D(0),
    }
    fields.update(kw)
    return RiskEngine(config).check(RiskContext(**fields))  # type: ignore[arg-type]


def test_flipping_through_zero_cannot_bypass_the_position_limit() -> None:
    limits = RiskLimits(max_position=D(50))
    assert not decide(req(quantity="200"), limits, position=D(-100)).approved
    assert not decide(
        req(
            side=SELL,
            quantity="200",
            limit_price="100.00",
            short_sale=ShortSaleMark.SHORT_EXEMPT,
        ),
        limits,
        position=D(100),
    ).approved
    assert decide(req(quantity="100"), limits, position=D(-100)).approved


def test_notional_is_judged_at_the_market_when_the_order_price_is_far_away() -> None:
    limits = RiskLimits(max_order_notional=D(1000))
    low_sell = req(
        side=SELL,
        quantity="1000",
        limit_price="0.01",
        short_sale=ShortSaleMark.SHORT_EXEMPT,
    )
    assert decide(low_sell, limits).check == "max_order_notional"
    assert (
        decide(
            req(quantity="10", limit_price="50.00"),
            RiskLimits(max_order_notional=D(1000)),
        ).approved
        is False
    )
    assert decide(req(quantity="5", limit_price="50.00"), limits).approved


def test_stop_already_through_the_market_is_refused() -> None:
    stop = req(
        order_type=OrderType.STOP, limit_price=None, stop_price="0.01", quantity="1"
    )
    assert decide(stop).check == "stop_price"
    sell_stop = req(
        side=SELL,
        order_type=OrderType.STOP,
        limit_price=None,
        stop_price="150.00",
        quantity="1",
        short_sale=ShortSaleMark.SHORT_EXEMPT,
    )
    assert decide(sell_stop).check == "stop_price"
    fine = req(
        order_type=OrderType.STOP, limit_price=None, stop_price="105.00", quantity="1"
    )
    assert decide(fine).approved


@pytest.mark.parametrize("position", ["0", "50", "5"])
def test_reduce_only_buy_cannot_add_or_flip(position: str) -> None:
    signed = D(position) if position != "5" else D(-5)
    assert decide(req(reduce_only=True), position=signed).check == "reduce_only"


def test_reduce_only_passes_when_it_really_reduces() -> None:
    assert decide(req(reduce_only=True, quantity="10"), position=D(-10)).approved
    assert decide(
        req(side=SELL, reduce_only=True, quantity="10"), position=D(10)
    ).approved
    assert decide(req(reduce_only=True), position=None).check == "reduce_only"


def test_zero_multiplier_is_rejected() -> None:
    with pytest.raises(ValueError):
        instrument(multiplier="0")
    with pytest.raises(ValueError):
        instrument(multiplier="-1")


def test_gross_limit_never_blocks_an_order_that_only_reduces() -> None:
    limits = RiskLimits(max_gross_notional=D(1000))
    closing = req(side=SELL, quantity="50", limit_price="100.00")
    assert decide(closing, limits, position=D(100), gross_notional=D(9999)).approved
    assert decide(closing, limits, position=D(100), gross_notional=None).approved
    assert not decide(req(), limits, position=D(100), gross_notional=None).approved


def test_expired_gtd_and_off_hours_marketable_orders_are_refused() -> None:
    expired = req(time_in_force=TimeInForce.GTD, expire_at=NOW - timedelta(days=1))
    assert decide(expired).check == "time_in_force"
    saturday = datetime(2026, 3, 7, 15, 0, tzinfo=UTC)
    fresh = QUOTE.model_copy(update={"exchange_ts": saturday, "received_ts": saturday})
    market = req(
        order_type=OrderType.MARKET, limit_price=None, time_in_force=TimeInForce.GTC
    )
    assert decide(market, now=saturday, reference=fresh).check == "session"
    resting = req(time_in_force=TimeInForce.GTC)
    assert decide(resting, now=saturday, reference=fresh).approved


def test_future_dated_quote_is_not_fresh() -> None:
    future = QUOTE.model_copy(update={"exchange_ts": NOW + timedelta(days=30)})
    assert decide(req(), reference=future).check == "stale_data"
    slight = QUOTE.model_copy(update={"exchange_ts": NOW + timedelta(seconds=1)})
    assert decide(req(), reference=slight).approved


@pytest.mark.parametrize(
    "terms",
    [
        {"status": ListingStatus.PENDING},
        {"delisting_date": date(2020, 1, 2)},
        {"listing_date": date(2030, 1, 2)},
    ],
)
def test_not_yet_listed_or_delisted_instruments_are_refused(
    terms: dict[str, object],
) -> None:
    assert decide(req(), instrument=instrument(**terms)).check == "instrument"


def test_scoped_open_order_limits_count_only_their_scope() -> None:
    other = Symbol(venue="XNYS", ticker="OTHER")
    others = [
        new_order(
            SubmitIntent(
                client_order_id=f"O{i}",
                ts=NOW,
                request=req(symbol=other, client_order_id=f"O{i}", limit_price="90.00"),
            )
        )
        for i in range(3)
    ]
    config = RiskConfig(
        default=RiskLimits(),
        per_symbol={"ACME@XNYS": RiskLimits(max_open_orders=2)},
        duplicate_window_seconds=D(0),
    )
    context = RiskContext(
        request=req(),
        instrument=instrument(),
        reference=QUOTE,
        now=NOW,
        open_orders=others,
        position=D(0),
        calendar=get_calendar("XNYS"),
        rules=RULES,
        gross_notional=D(0),
    )
    assert RiskEngine(config).check(context).approved


def test_a_per_symbol_key_that_could_match_nothing_is_refused() -> None:
    with pytest.raises(ValueError, match="TICKER@MIC"):
        RiskConfig(default=RiskLimits(), per_symbol={"ACME": RiskLimits()})
    config = RiskConfig(default=RiskLimits(), per_symbol={"ACME@xnys": RiskLimits()})
    assert "ACME@XNYS" in config.per_symbol


def test_client_order_id_cannot_end_in_a_newline() -> None:
    with pytest.raises(ValueError):
        req(client_order_id="abc\n")


def test_ticker_containing_at_sign_round_trips() -> None:
    symbol = Symbol(venue="XCME", ticker="@ES")
    assert Symbol.parse(str(symbol)) == symbol
    assert Symbol.parse("@ES", venue="XCME") == symbol


def test_pending_upsize_counts_in_worst_case_exposure() -> None:
    state = new_order(
        SubmitIntent(client_order_id="P1", ts=NOW, request=req(client_order_id="P1"))
    )
    from ismet.execution import apply_event

    state = replace(state, status=S.ACCEPTED)
    pending = apply_event(
        state, ReplaceRequested(client_order_id="P1", ts=NOW, quantity="150")
    )
    assert pending.leaves_quantity == 150
    downsize = apply_event(
        state, ReplaceRequested(client_order_id="P1", ts=NOW, quantity="5")
    )
    assert downsize.leaves_quantity == 10


# Money


def fill(
    fill_id: str,
    side: OrderSide,
    quantity: str,
    price: str,
    fee: str | None = None,
    ccy: str | None = None,
    symbol: Symbol = ACME,
) -> Fill:
    return Fill(
        fill_id=fill_id,
        client_order_id="C",
        symbol=symbol,
        side=side,
        price=price,
        quantity=quantity,
        ts=NOW,
        fee=fee,
        fee_currency=ccy,
    )


def test_fees_in_different_currencies_are_not_added_together() -> None:
    ledger = build_ledger(
        [
            fill("A", BUY, "1", "10", "0.001", "BNB"),
            fill("B", SELL, "1", "11", "5", "USD"),
        ]
    )
    assert ledger.fees == {"BNB": D("0.001"), "USD": D(5)}
    valued = value_books(
        {"x": (ledger, "USD", None)},
        currency="USD",
        rates={"BNBUSD": D(600)},
    )
    assert valued.fees == D("5.600")
    with pytest.raises(ValidationError, match="no rate"):
        value_books({"x": (ledger, "USD", None)}, currency="USD", rates={})


def test_unknown_fee_is_counted_not_treated_as_zero() -> None:
    ledger = build_ledger([fill("A", BUY, "1", "10")])
    assert ledger.fills_with_unknown_fee == 1 and ledger.fees == {}
    with pytest.raises(ValidationError, match="no currency"):
        build_ledger([fill("B", BUY, "1", "10", "1")])


def test_ledger_refuses_fills_from_another_symbol() -> None:
    other = Symbol(venue="ZSIM", ticker="OTHER")
    ledger = PositionLedger()
    ledger.apply(fill("A", BUY, "1", "10"))
    with pytest.raises(ValidationError, match="OTHER"):
        ledger.apply(fill("B", SELL, "1", "10", symbol=other))


def test_full_close_leaves_exactly_zero_beyond_the_ambient_context() -> None:
    size = "1234567890123.123456789012345678"
    ledger = build_ledger([fill("A", BUY, size, "2"), fill("B", SELL, size, "3")])
    assert ledger.quantity == 0 and not ledger.lots
    assert ledger.realized_pnl == D(size)
    assert (
        len(ledger.realized_trades) == 1
    )  # one exact match, not a rounded one and a rest
    assert ledger.realized_trades[0].quantity == D(size)


def test_rounding_never_produces_negative_zero() -> None:
    for rounding in (Rounding.CEILING, Rounding.TOWARD_ZERO, Rounding.HALF_EVEN):
        result = round_to_step(D("-0.003"), D("0.01"), rounding)
        assert result == 0 and str(result) == "0.00"


# Books


def test_every_level_of_a_batched_update_is_applied() -> None:
    book = BookBuilder(ACME)
    book.apply_snapshot(100, [(D(10), D(1))], [(D(11), D(1))])
    results = [
        book.apply_delta(BookDelta(BUY, D("10.5"), D(2), 103, first_sequence=101)),
        book.apply_delta(BookDelta(SELL, D(11), D(0), 103, first_sequence=101)),
        book.apply_delta(BookDelta(SELL, D(12), D(3), 103, first_sequence=101)),
    ]
    assert results == [True, True, True]
    snapshot = book.snapshot(exchange_ts=NOW, received_ts=NOW)
    assert snapshot.bids[0] == Level(price=D("10.5"), size=D(2))
    assert [x.price for x in snapshot.asks] == [D(12)]


def test_stale_snapshot_cannot_roll_back_a_synced_book() -> None:
    book = BookBuilder(ACME)
    book.apply_snapshot(100, [(D(10), D(1))], [])
    book.apply_delta(BookDelta(BUY, D(10), D(9), 101))
    assert not book.apply_snapshot(50, [(D(9), D(1))], [])
    assert book.stale_snapshots == 1 and book.last_sequence == 101
    assert book.snapshot(exchange_ts=NOW, received_ts=NOW).bids[0].size == 9


def test_book_is_independent_of_the_ambient_decimal_context() -> None:
    import decimal

    book = BookBuilder(ACME)
    price = D("12345678901234567890123456789.5")
    with decimal.localcontext() as ctx:
        ctx.prec = 4
        book.apply_snapshot(1, [(price, D(1)), (price + 1, D(1))], [])
        snapshot = book.snapshot(exchange_ts=NOW, received_ts=NOW)
    assert snapshot.bids[1].price == price


# Quality and sequences


def test_non_adjacent_duplicates_are_duplicates() -> None:
    tracker = SequenceTracker()
    outcomes = [tracker.classify(n).outcome for n in (5, 6, 7, 5)]
    assert outcomes[-1] is SequenceOutcome.DUPLICATE
    tracker = SequenceTracker()
    for n in (1, 5):
        tracker.classify(n)
    assert tracker.classify(3).outcome is SequenceOutcome.OUT_OF_ORDER
    assert tracker.classify(3).outcome is SequenceOutcome.DUPLICATE
    assert tracker.classify(2).outcome is SequenceOutcome.OUT_OF_ORDER


def test_old_data_arriving_now_is_stale_and_absent_symbols_are_reported() -> None:
    from ismet.models import Trade

    monitor = QualityMonitor()
    old = Trade(
        symbol=ACME,
        exchange_ts=NOW - timedelta(hours=6),
        received_ts=NOW,
        price="1",
        size="1",
    )
    assert Issue.STALE in {i.issue for i in monitor.observe(old)}
    other = Symbol(venue="ZSIM", ticker="QUIET")
    stale = monitor.stale_symbols(NOW, expected=[ACME, other])
    assert [i.symbol for i in stale] == [other]


def test_an_outlier_does_not_become_the_baseline_until_confirmed() -> None:
    from ismet.models import Trade

    monitor = QualityMonitor()

    def seen(price: str) -> list[Issue]:
        trade = Trade(
            symbol=ACME, exchange_ts=NOW, received_ts=NOW, price=price, size="1"
        )
        return [i.issue for i in monitor.observe(trade)]

    assert seen("100") == []
    assert seen("1000") == [Issue.OUTLIER]
    assert seen("101") == []  # the good price is not flagged against the bad one
    assert seen("1000") == [Issue.OUTLIER]
    assert seen("1000") == [Issue.OUTLIER]
    assert seen("1000") == [Issue.OUTLIER]  # third in a row: now the level
    assert seen("1001") == []
    assert monitor.compare_sources(ACME, D(0), D(0)) is None


# Calendars


def test_editing_an_always_open_calendar_changes_only_that_day() -> None:
    calendar = AlwaysOpenCalendar("ZCBS").with_closure(date(2026, 3, 9))
    assert not calendar.is_session(date(2026, 3, 9))
    assert calendar.is_session(date(2026, 3, 7))  # still trades on Saturday
    assert isinstance(calendar, AlwaysOpenCalendar)


def test_breaks_must_be_complete_and_inside_the_session() -> None:
    from datetime import time

    xnys = get_calendar("XNYS")
    day = date(2026, 3, 9)
    with pytest.raises(ValidationError, match="both"):
        xnys.with_hours(day, time(9), time(16), break_start=time(12))
    with pytest.raises(ValidationError, match="within"):
        xnys.with_hours(
            day, time(9), time(16), break_start=time(17), break_end=time(18)
        )


# Corporate actions


def bar(day: date, close: str, venue: str = "XNAS", **kw: object) -> Bar:
    ts = datetime(day.year, day.month, day.day, 14, 30, tzinfo=UTC)
    fields: dict[str, object] = {
        "symbol": Symbol(venue=venue, ticker="X"),
        "exchange_ts": ts,
        "received_ts": ts,
        "interval": Interval.D1,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": "100",
        "adjustment": "raw",
        "session_date": day,
    }
    fields.update(kw)
    return Bar.model_validate(fields)


def test_many_dividends_stay_exact_enough_and_reload() -> None:
    calendar = get_calendar("XNAS")
    ex_dates = [date(2020 + i // 4, 2 + (i % 4) * 3, 3) for i in range(12)]
    ex_dates = [calendar.next_session_date(day) for day in ex_dates]
    bars = [bar(calendar.previous_session_date(day), "97.13") for day in ex_dates]
    bars.append(bar(date(2023, 3, 1), "97.13"))
    actions = [
        CorporateAction(ActionKind.CASH_DIVIDEND, day, amount=D("0.37"))
        for day in ex_dates
    ]
    adjusted = adjust_bars(bars, actions, include_dividends=True)
    assert adjusted[0].close < adjusted[-1].close
    assert Bar.model_validate_json(adjusted[0].model_dump_json()) == adjusted[0]


def test_dividend_above_the_close_or_on_a_zero_close_is_refused() -> None:
    # 2026-01-30 is the session before Monday 2026-02-02.
    bars = [bar(date(2026, 1, 30), "1"), bar(date(2026, 3, 2), "1")]
    big = CorporateAction(ActionKind.CASH_DIVIDEND, date(2026, 2, 2), amount=D(5))
    with pytest.raises(ValidationError, match="not below"):
        adjust_bars(bars, [big], include_dividends=True)
    zero = [bar(date(2026, 1, 30), "0"), bar(date(2026, 3, 2), "1")]
    with pytest.raises(ValidationError, match="positive close"):
        adjust_bars(zero, [big], include_dividends=True)


def test_price_factor_on_a_dividend_respects_include_dividends() -> None:
    bars = [bar(date(2026, 1, 2), "100")]
    action = CorporateAction(
        ActionKind.CASH_DIVIDEND, date(2026, 2, 2), amount=D(1), price_factor=D("0.9")
    )
    assert adjust_bars(bars, [action])[0].close == 100
    assert adjust_bars(bars, [action], include_dividends=True)[0].close == 90


def test_adjustment_uses_the_venue_date_not_the_utc_date() -> None:
    # 20:30 New York on the day before the ex-date is already the next day in UTC.
    after_hours = bar(
        date(2026, 3, 2),
        "100",
        exchange_ts=datetime(2026, 3, 3, 1, 30, tzinfo=UTC),
        received_ts=datetime(2026, 3, 3, 1, 30, tzinfo=UTC),
        session_date=None,
    )
    split = CorporateAction(ActionKind.SPLIT, date(2026, 3, 3), ratio=D(2))
    assert adjust_bars([after_hours], [split])[0].close == 50


# Third review: fills on closed orders, adopted kill states, venue realism


async def test_offsetting_dropped_fills_on_closed_orders_are_booked(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("TR1", price="9.50"))
    await oms.submit(
        order(
            "TR2",
            side=SELL,
            price="10.50",
            short_sale=ShortSaleMark.SHORT_EXEMPT,
        )
    )
    await venue.drain()
    venue.report_faults.drop_next = 2
    venue.trade_through(ACME, "9.50", "40")
    venue.trade_through(ACME, "10.50", "40")
    await venue.cancel_order("TR1", ACME)
    await venue.cancel_order("TR2", ACME)
    await venue.drain()
    assert oms.order("TR1").is_terminal and oms.order("TR2").is_terminal
    report = await oms.reconcile()
    assert oms.order("TR1").filled_quantity == 40
    assert oms.order("TR2").filled_quantity == 40
    assert {"TR1", "TR2"} <= set(report.resolved)


async def test_venue_with_no_account_api_books_a_fill_on_a_closed_order(
    clock: ManualClock,
) -> None:
    venue = account_free(clock)
    oms = bare_manager(venue, clock)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("TR3", price="9.50"))
        await venue.drain()
        venue.report_faults.drop_next = 1
        venue.trade_through(ACME, "9.50", "40")
        await venue.cancel_order("TR3", ACME)
        await venue.drain()
        report = await oms.reconcile()
        assert oms.net_position(ACME) == 40
        assert not report.positions_verified
    finally:
        await oms.abort()


async def test_fill_recovery_survives_a_venue_clock_behind_ours(
    journal_path: Path, clock: ManualClock
) -> None:
    behind = ManualClock(clock.now() - timedelta(milliseconds=500))
    venue = SimVenue(clock=behind, instruments=[sim_instrument("ACME")])
    venue.set_quote(ACME, "9.99", "10.01")
    journal = SqliteJournal(journal_path)
    oms = OrderManager(
        venue,
        journal,
        RiskEngine(permissive_config()),
        clock=clock,
        unknown_grace=0.0,
        reconcile_interval=3600,
        stream_settle=None,
    )
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("TR4", price="9.50"))
        await venue.drain()
        venue.report_faults.drop_next = 1
        venue.trade_through(ACME, "9.50", "40")
        await oms.reconcile()
        assert oms.order("TR4").filled_quantity == 40
        assert not oms.kill_state.engaged
    finally:
        await oms.abort()
        journal.close()


async def test_a_failed_sweep_leaves_the_fill_for_the_next_pass(
    clock: ManualClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No account API, so the sweep is the only thing that can find this fill.
    venue = account_free(clock)
    oms = bare_manager(venue, clock)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("TR5", price="9.50"))
        await venue.drain()
        venue.report_faults.drop_next = 1
        venue.trade_through(ACME, "9.50", "40")
        await venue.cancel_order("TR5", ACME)
        await venue.drain()
        real = venue.fills_since

        async def down(*_: object) -> list[Fill]:
            raise ValidationError("down")

        monkeypatch.setattr(venue, "fills_since", down)
        clock.advance(7200)  # longer than the lookback, so only a held window finds it
        assert not (await oms.reconcile()).complete
        monkeypatch.setattr(venue, "fills_since", real)
        clock.advance(7200)
        await oms.reconcile()
        assert oms.order("TR5").filled_quantity == 40
    finally:
        await oms.abort()


async def test_sweep_looks_back_past_the_current_moment(clock: ManualClock) -> None:
    venue = account_free(clock)
    oms = bare_manager(venue, clock)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("TR5B", price="9.50"))
        await venue.drain()
        venue.report_faults.drop_next = 1
        venue.trade_through(ACME, "9.50", "40")
        await venue.cancel_order("TR5B", ACME)
        await venue.drain()
        clock.advance(10)
        await oms.reconcile()
        assert oms.order("TR5B").filled_quantity == 40
    finally:
        await oms.abort()


async def test_unknown_multiplier_fails_gross_closed_rather_than_assuming_one(
    journal_path: Path, clock: ManualClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    venue = SimVenue(
        clock=clock,
        instruments=[sim_instrument("ACME", multiplier="50")],
        initial_positions={ACME: D(30)},
    )
    venue.set_quote(ACME, "9.99", "10.01")

    async def down(_: Symbol) -> None:
        raise ValidationError("reference data down")

    monkeypatch.setattr(venue, "instrument", down)
    journal = SqliteJournal(journal_path)
    oms = OrderManager(
        venue,
        journal,
        RiskEngine(permissive_config()),
        clock=clock,
        reconcile_interval=3600,
        stream_settle=None,
    )
    await oms.start()
    try:
        oms._set_mark(ACME, D(10))
        assert oms._gross_notional() is None
    finally:
        await oms.abort()
        journal.close()


async def test_fill_after_a_cancel_is_recorded_without_halting(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("TR6", price="9.50"))
    await venue.drain()
    venue.report_faults.hold_next = 1
    venue.trade_through(ACME, "9.50", "40")
    await venue.cancel_order("TR6", ACME)
    await oms.wait_for("TR6", lambda o: o.is_terminal, 2)
    venue.release_held_reports()
    state = await oms.wait_for("TR6", lambda o: o.filled_quantity == 40, 2)
    assert any("after terminal" in a for a in state.anomalies)
    assert not oms.kill_state.engaged


async def test_fill_on_an_order_believed_rejected_halts(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.faults.append(Fault.LOSE_REQUEST)
    await oms.submit(order("TR7", price="9.50"))
    await oms.reconcile()
    assert oms.order("TR7").status is S.REJECTED
    venue.add_external_order(order("TR7", price="9.50"))
    venue.trade_through(ACME, "9.50", "1")
    await oms.wait_for("TR7", lambda o: o.filled_quantity == 1, 2)
    await venue.drain()
    assert oms.kill_state.engaged


async def test_rejection_after_acceptance_closes_the_order(
    oms: OrderManager, venue: SimVenue
) -> None:
    from ismet.models.orders import EventOrigin, Rejected

    await venue.drain()
    await oms.submit(order("TR8", price="9.50"))
    await venue.drain()
    venue._publish(
        Rejected(
            client_order_id="TR8",
            ts=venue.clock.now(),
            origin=EventOrigin.VENUE,
            reason="margin check failed",
        )
    )
    state = await oms.wait_for("TR8", lambda o: o.is_terminal, 2)
    assert state.status is S.REJECTED and not state.anomalies


async def test_forged_release_is_not_adopted_by_a_running_manager(
    oms: OrderManager, venue: SimVenue, journal_path: Path
) -> None:
    import sqlite3

    await venue.drain()
    await oms.engage_kill("halt", operator="ops", cancel_open=False)
    # Hold the write lock, since the running manager can append at any moment.
    conn = sqlite3.connect(journal_path, isolation_level=None, timeout=30)
    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute("SELECT MAX(seq) FROM journal").fetchone()
    conn.execute(
        "INSERT INTO journal (seq, ts, kind, client_order_id, payload, unique_intent, "
        "prev_hash, hash) "
        "VALUES (?, ?, 'kill_released', NULL, ?, 0, 'forged', 'forged')",
        (row[0] + 1, NOW.isoformat(), '{"reason": "x", "operator": "mallory"}'),
    )
    conn.execute("COMMIT")
    conn.close()
    with pytest.raises(KillSwitchEngaged):
        await oms.submit(order("TR9", price="9.50"))
    assert oms.kill_state.engaged


async def test_release_naming_no_operator_is_not_adopted(
    oms: OrderManager, venue: SimVenue, journal_path: Path
) -> None:
    await venue.drain()
    await oms.engage_kill("halt", operator="ops", cancel_open=False)
    other = SqliteJournal(journal_path)
    other.append(RecordKind.KILL_RELEASED, '{"reason": "x"}')
    other.close()
    with pytest.raises(KillSwitchEngaged):
        await oms.submit(order("TR10", price="9.50"))


@pytest.mark.parametrize(
    "payload",
    [
        '{"reason": "x"}',
        '{"reason": "x", "operator": ""}',
        '{"reason": "x", "operator": "   "}',
        '{"reason": "x", "operator": null}',
    ],
)
async def test_release_naming_no_operator_does_not_undo_the_kill_on_restart(
    make_manager: Factory, venue: SimVenue, journal: Journal, payload: str
) -> None:
    journal.append(RecordKind.KILL_ENGAGED, '{"reason": "halt", "operator": "ops"}')
    journal.append(RecordKind.KILL_RELEASED, payload)
    assert journal.kill_state().engaged and journal.kill_state().reason == "halt"
    oms = make_manager()
    await oms.start()
    try:
        await venue.drain()
        with pytest.raises(KillSwitchEngaged):
            await oms.submit(order("TR10B", price="9.50"))
        assert venue.attempts["TR10B"] == 0
    finally:
        await oms.abort()


async def test_a_named_release_after_a_nameless_one_still_clears_the_kill(
    make_manager: Factory, venue: SimVenue, journal: Journal
) -> None:
    journal.append(RecordKind.KILL_ENGAGED, '{"reason": "halt", "operator": "ops"}')
    journal.append(RecordKind.KILL_RELEASED, '{"reason": "x", "operator": " "}')
    journal.append(RecordKind.KILL_RELEASED, '{"reason": "ok", "operator": "pat"}')
    oms = make_manager()
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("TR10C", price="9.50"))
        assert venue.attempts["TR10C"] == 1
    finally:
        await oms.abort()


async def test_a_blank_operator_cannot_release_the_kill_through_the_api(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.engage_kill("halt", operator="ops", cancel_open=False)
    with pytest.raises(ValidationError):
        await oms.release_kill("y", operator="   ")
    assert oms.kill_state.engaged


async def test_keep_orders_kill_leaves_open_orders_working_across_polls(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("TR11", price="9.50"))
    await venue.drain()
    await oms.engage_kill("halt", cancel_open=False)
    polls = count_kill_polls(oms._journal)
    await until(lambda: polls["reads"] >= 5)
    assert oms.order("TR11").status is S.ACCEPTED
    assert venue.attempts["TR11"] == 1


async def test_short_sale_through_the_manager_needs_a_configured_locate(
    make_manager: Factory, venue: SimVenue
) -> None:
    oms = make_manager()
    await oms.start()
    try:
        await venue.drain()
        with pytest.raises(RiskRejected) as caught:
            await oms.submit(
                order(
                    "TR12",
                    side=SELL,
                    price="10.50",
                    short_sale=ShortSaleMark.SHORT,
                )
            )
        assert "locate" in caught.value.reason
    finally:
        await oms.abort()
    allowed = make_manager(locate_check=lambda _: True)
    await allowed.start()
    try:
        state = await allowed.submit(
            order("TR13", side=SELL, price="10.50", short_sale=ShortSaleMark.SHORT)
        )
        assert state.status is S.ACCEPTED
    finally:
        await allowed.abort()


@pytest.mark.parametrize(
    ("fault", "code"),
    [(Fault.RATE_LIMITED, "RateLimited"), (Fault.AUTH_FAILED, "AuthError")],
)
async def test_refusals_that_certainly_did_not_execute_close_the_order(
    oms: OrderManager, venue: SimVenue, fault: Fault, code: str
) -> None:
    await venue.drain()
    venue.faults.append(fault)
    state = await oms.submit(order(f"TR-{code}", price="9.50"))
    assert state.status is S.REJECTED and state.reject_code == code


async def test_order_slow_to_appear_at_the_venue_is_not_declared_lost(
    make_manager: Factory, venue: SimVenue, clock: ManualClock
) -> None:
    oms = make_manager(unknown_grace=5.0)
    await oms.start()
    try:
        await venue.drain()
        venue.status_lag_queries = 2
        await oms.submit(order("TR14", price="9.50"))
        await venue.drain()
        await oms.reconcile()
        assert oms.order("TR14").status is S.ACCEPTED
        await oms.reconcile()
        assert oms.order("TR14").status is S.ACCEPTED
        clock.advance(1)
        await oms.reconcile()
        assert oms.order("TR14").status is S.ACCEPTED
        assert not oms.kill_state.engaged
    finally:
        await oms.abort()


async def test_a_fee_arriving_later_enriches_the_fill_rather_than_halting(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.fee_only_on_query = True
    await oms.submit(order("TR15", price="9.50"))
    await venue.drain()
    venue.trade_through(ACME, "9.50", "40")
    state = await oms.wait_for("TR15", lambda o: o.filled_quantity == 40, 2)
    assert state.fills[0].fee is None
    await oms.reconcile()
    state = oms.order("TR15")
    assert state.fills[0].fee == D(0)
    assert state.filled_quantity == 40 and not state.anomalies
    assert not oms.kill_state.engaged


async def test_position_view_lagging_fills_does_not_halt(
    make_manager: Factory, venue: SimVenue, clock: ManualClock
) -> None:
    oms = make_manager(unknown_grace=5.0)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("TR16", price="9.50"))
        await venue.drain()
        venue.positions_lag_queries = 3
        venue.trade_through(ACME, "9.50", "40")
        await oms.wait_for("TR16", lambda o: o.filled_quantity == 40, 2)
        report = await oms.reconcile()
        assert not report.complete and not oms.kill_state.engaged
        await oms.reconcile()
        assert not oms.kill_state.engaged
    finally:
        await oms.abort()


async def test_persistent_position_difference_halts_after_the_grace_period(
    make_manager: Factory, venue: SimVenue, clock: ManualClock
) -> None:
    oms = make_manager(unknown_grace=5.0)
    await oms.start()
    try:
        await venue.drain()
        venue.add_external_fill(ACME, BUY, D(25))
        await oms.reconcile()
        assert not oms.kill_state.engaged
        clock.advance(6)
        report = await oms.reconcile()
        assert [b.kind for b in report.breaks] == [BreakKind.POSITION_MISMATCH]
        assert oms.kill_state.engaged
    finally:
        await oms.abort()


async def test_a_bust_while_down_halts_rather_than_guessing(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    journal = SqliteJournal(journal_path)
    oms = OrderManager(
        venue,
        journal,
        RiskEngine(permissive_config()),
        clock=clock,
        unknown_grace=0.0,
        reconcile_interval=3600,
        stream_settle=None,
    )
    await oms.start()
    await venue.drain()
    await oms.submit(order("TR17", price="9.50"))
    await venue.drain()
    venue.trade_through(ACME, "9.50", "40")
    state = await oms.wait_for("TR17", lambda o: o.filled_quantity == 40, 2)
    await oms.abort()
    venue.bust(state.fills[0].fill_id)
    second = OrderManager(
        venue,
        journal,
        RiskEngine(permissive_config()),
        clock=clock,
        unknown_grace=0.0,
        reconcile_interval=3600,
        stream_settle=None,
    )
    report = await second.start()
    try:
        assert second.kill_state.engaged
        kinds = {b.kind for b in report.breaks}
        assert kinds & {BreakKind.FILL_MISMATCH, BreakKind.POSITION_MISMATCH}
    finally:
        await second.abort()
        journal.close()


async def test_concurrent_submits_cannot_jointly_exceed_a_limit(
    make_manager: Factory, venue: SimVenue
) -> None:
    oms = make_manager(
        risk=RiskEngine(permissive_config(default=RiskLimits(max_position=D(150))))
    )
    await oms.start()
    try:
        await venue.drain()
        results = await asyncio.gather(
            *(oms.submit(order(f"TR-C{i}", price="9.50")) for i in range(3)),
            return_exceptions=True,
        )
        accepted = [r for r in results if not isinstance(r, Exception)]
        assert len(accepted) == 1
    finally:
        await oms.abort()


# Round two: corrected fills, the first start, and guards no test used to exercise


async def test_a_query_reporting_a_known_fill_with_other_terms_halts_once(
    make_manager: Factory, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    anomalies: list[str] = []
    oms = make_manager(on_anomaly=lambda _, message: anomalies.append(message))
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("TR18", quantity="10", price="9.50"))
        await venue.drain()
        venue.trade_through(ACME, "9.50", "10")
        first = await oms.wait_for("TR18", lambda o: o.is_terminal, 2)
        assert first.status is S.FILLED and first.average_price == D("9.50")
        real = venue.fills_since

        async def corrected(
            since: datetime, symbol: Symbol | None = None
        ) -> list[Fill]:
            fills = await real(since, symbol)
            return [f.model_copy(update={"price": D("9.51")}) for f in fills]

        monkeypatch.setattr(venue, "fills_since", corrected)
        await oms.reconcile()
        assert oms.kill_state.engaged
        assert "different terms" in oms.kill_state.reason
        assert any("different terms" in a for a in anomalies)
        state = oms.order("TR18")
        assert state.filled_quantity == 10 and state.average_price == D("9.50")
        await oms.release_kill("venue confirmed the correction", operator="pat")
        await oms.reconcile()
        assert not oms.kill_state.engaged
    finally:
        await oms.abort()
    restarted = make_manager()
    await restarted.start()
    try:
        assert not restarted.kill_state.engaged
        await restarted.reconcile()
        assert not restarted.kill_state.engaged
    finally:
        await restarted.abort()


async def test_a_first_start_does_not_treat_fills_already_in_the_baseline_as_orphans(
    make_manager: Factory, venue: SimVenue, clock: ManualClock
) -> None:
    # Before ismet ever ran on this account, another terminal traded in it.
    venue.add_external_order(order("TR19-OLD", quantity="5", price="9.50"))
    venue.trade_through(ACME, "9.50", "5")
    clock.advance(60)
    oms = make_manager()
    report = await oms.start()
    try:
        assert not report.breaks and not oms.kill_state.engaged
        assert oms.net_position(ACME) == 5
    finally:
        await oms.abort()
    restarted = make_manager()
    report = await restarted.start()
    try:
        assert not report.breaks and not restarted.kill_state.engaged
        # A fill the venue dates after the baseline is still an orphan.
        clock.advance(60)
        venue.add_external_order(order("TR19-NEW", quantity="1", price="9.50"))
        venue.report_faults.drop_next = 1
        venue.trade_through(ACME, "9.50", "1")
        report = await restarted.reconcile()
        assert BreakKind.ORPHAN_FILL in [b.kind for b in report.breaks]
        assert restarted.kill_state.engaged
    finally:
        await restarted.abort()


async def test_an_old_quote_for_a_held_symbol_leaves_gross_exposure_unknown(
    clock: ManualClock,
) -> None:
    other = Symbol(venue=SIM_VENUE, ticker="OTHR")
    venue = SimVenue(
        clock=clock,
        instruments=[sim_instrument("ACME"), sim_instrument("OTHR")],
        initial_positions={other: D(100)},
    )
    venue.set_quote(ACME, "9.99", "10.01")
    venue.set_quote(other, "4.99", "5.01")

    async def reference(symbol: Symbol) -> Quote:
        quote = await venue.quote(symbol)
        if symbol == other:
            stamp = clock.now() - timedelta(hours=1)
            return quote.model_copy(update={"exchange_ts": stamp, "received_ts": stamp})
        return quote

    oms = OrderManager(
        venue,
        MemoryJournal(),
        RiskEngine(
            permissive_config(default=RiskLimits(max_gross_notional=D(100_000)))
        ),
        clock=clock,
        reference_source=reference,
        unknown_grace=0.0,
        reconcile_interval=3600.0,
        kill_poll_interval=0.01,
        stream_settle=None,
    )
    await oms.start()
    try:
        await venue.drain()
        # 100 OTHR at 5.00 would be 500, far inside the limit, if the quote counted.
        with pytest.raises(RiskRejected, match="gross exposure unknown"):
            await oms.submit(order("TR20", quantity="1", price="9.50"))
        assert venue.attempts["TR20"] == 0
    finally:
        await oms.abort()


async def test_an_unrecorded_kill_is_not_undone_by_a_newer_release_in_the_journal(
    venue: SimVenue, clock: ManualClock
) -> None:
    class KillWritesFail(MemoryJournal):
        def append(self, *args: object, **kw: object):  # type: ignore[no-untyped-def]
            if args[0] is RecordKind.KILL_ENGAGED:
                raise JournalError("disk full")
            return super().append(*args, **kw)  # type: ignore[arg-type]

    journal = KillWritesFail()
    oms = OrderManager(
        venue,
        journal,
        RiskEngine(permissive_config()),
        clock=clock,
        kill_poll_interval=0.001,
        stream_settle=None,
    )
    await oms.start()
    try:
        await venue.drain()
        await oms.engage_kill("disk is failing", operator="ops", cancel_open=False)
        assert oms.kill_state.engaged
        # Another process releases what it can see, which is no kill at all.
        journal.append(RecordKind.KILL_RELEASED, '{"reason": "ok", "operator": "pat"}')
        polls = count_kill_polls(journal)
        await until(lambda: polls["reads"] >= 5)
        assert oms.kill_state.engaged
        with pytest.raises(KillSwitchEngaged) as caught:
            await oms.submit(order("TR21", price="9.50"))
        assert caught.value.reason == "disk is failing"
        assert venue.attempts["TR21"] == 0
    finally:
        await oms.abort()


async def test_a_status_answer_older_than_a_partial_fill_is_asked_again(
    oms: OrderManager, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    await venue.drain()
    await oms.submit(order("TR22", price="9.50"))
    await venue.drain()
    real = venue.order_status
    calls = {"n": 0}

    async def fills_after_answering(cid: str, symbol: Symbol):  # type: ignore[no-untyped-def]
        answer = await real(cid, symbol)
        calls["n"] += 1
        if calls["n"] == 1:
            venue.trade_through(ACME, "9.50", "40")
            await oms.wait_for("TR22", lambda o: o.filled_quantity == 40, 2)
        return answer

    monkeypatch.setattr(venue, "order_status", fills_after_answering)
    await oms.reconcile()
    assert calls["n"] == 2
    state = oms.order("TR22")
    assert state.status is S.PARTIALLY_FILLED and state.filled_quantity == 40
    assert not state.anomalies
    assert not [e for e in events_of(oms._journal) if isinstance(e, Restated)]


async def test_a_cancel_during_the_intent_write_stops_the_send(
    make_manager: Factory, venue: SimVenue
) -> None:
    cancels: list[asyncio.Task[object]] = []

    def cancel_at_checkpoint(name: str, cid: str) -> None:
        if name == "after_intent":
            cancels.append(asyncio.create_task(oms.cancel(cid)))

    oms = make_manager(checkpoint=cancel_at_checkpoint)
    await oms.start()
    try:
        await venue.drain()
        state = await oms.submit(order("TR23", price="9.50"))
        await asyncio.gather(*cancels)
        assert state.status is S.REJECTED
        assert state.reject_code == "CANCELED_BEFORE_SEND"
        assert venue.attempts["TR23"] == 0
        assert not oms.order("TR23").anomalies
    finally:
        await oms.abort()


async def test_an_event_the_journal_missed_is_written_by_the_background_retry(
    venue: SimVenue, clock: ManualClock
) -> None:
    journal = FlakyJournal()
    oms = OrderManager(
        venue,
        journal,
        RiskEngine(permissive_config()),
        clock=clock,
        kill_poll_interval=0.001,
        reconcile_interval=3600.0,
        unknown_grace=0.0,
        stream_settle=None,
    )
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("TR24", price="9.50"))
        await venue.drain()
        journal.failing = True
        venue.trade_through(ACME, "9.50", "40")
        await oms.wait_for("TR24", lambda o: o.filled_quantity == 40, 2)
        assert "fill" not in [e.kind for e in events_of(journal)]
        journal.failing = False
        await until(lambda: "fill" in [e.kind for e in events_of(journal)])
    finally:
        await oms.abort()


async def test_a_sweep_while_events_await_the_journal_does_not_move_the_window(
    clock: ManualClock,
) -> None:
    class NoAccount(SimVenue):
        positions = None  # type: ignore[assignment]
        balances = None  # type: ignore[assignment]

    venue = NoAccount(clock=clock, instruments=[sim_instrument("ACME")])
    venue.set_quote(ACME, "9.99", "10.01")
    journal = FlakyJournal()

    def build() -> OrderManager:
        return OrderManager(
            venue,
            journal,
            RiskEngine(permissive_config()),
            clock=clock,
            assume_flat=True,
            kill_poll_interval=3600.0,
            reconcile_interval=3600.0,
            unknown_grace=0.0,
            stream_settle=None,
            fill_lookback=3600.0,
        )

    oms = build()
    await oms.start()
    await venue.drain()
    await oms.submit(order("TR25", price="9.50"))
    await venue.drain()
    await oms.cancel("TR25")
    await oms.wait_for("TR25", lambda o: o.is_terminal, 2)
    # The venue executes it after all, and the journal is down as the fill arrives.
    venue._orders["TR25"].status = S.ACCEPTED
    clock.advance(1)
    journal.failing = True
    venue.trade_through(ACME, "9.50", "40")
    await oms.wait_for("TR25", lambda o: o.filled_quantity == 40, 2)
    journal.failing = False
    clock.advance(7200)
    await oms.reconcile()  # the disk is back, but the fill is not on it yet
    await oms.abort()  # and the process dies before the retry writes it
    restarted = build()
    await restarted.start()
    try:
        assert restarted.order("TR25").filled_quantity == 40
        assert restarted.net_position(ACME) == 40
    finally:
        await restarted.abort()


async def test_a_reconcile_that_raises_refuses_orders_and_keeps_reconciling(
    make_manager: Factory,
    venue: SimVenue,
    journal: Journal,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oms = make_manager(reconcile_interval=0.01)
    await oms.start()
    try:
        await venue.drain()
        real_records = journal.records
        broken = {"on": True}

        def records(*, after_seq: int = 0):  # type: ignore[no-untyped-def]
            if broken["on"]:
                raise JournalError("read failed")
            return real_records(after_seq=after_seq)

        monkeypatch.setattr(journal, "records", records)
        refusals: list[str] = []

        async def refused_for_reconciliation() -> bool:
            try:
                await oms.submit(order(quantity="1", price="9.50"))
            except RiskRejected as exc:
                refusals.append(exc.check)
                return exc.check == "reconciliation"
            return False

        for _ in range(500):
            if await refused_for_reconciliation():
                break
            await asyncio.sleep(0.001)
        else:
            pytest.fail(f"never refused for reconciliation: {refusals[-3:]}")
        broken["on"] = False
        for _ in range(500):
            try:
                accepted = await oms.submit(order(quantity="1", price="9.50"))
            except RiskRejected:
                await asyncio.sleep(0.001)
                continue
            assert accepted.status is S.ACCEPTED
            break
        else:
            pytest.fail("reconciliation never recovered")
    finally:
        await oms.abort()
