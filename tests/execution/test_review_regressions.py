"""One test per defect found by independent review, each reproducing its scenario."""

from __future__ import annotations

import asyncio
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
    RiskRejected,
    ValidationError,
)
from ismet.execution import (
    BreakKind,
    MemoryJournal,
    OrderManager,
    RecordKind,
    RiskConfig,
    RiskContext,
    RiskEngine,
    RiskLimits,
    SqliteJournal,
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
    ShortSaleMark,
    SubmitIntent,
    TimeInForce,
    TradingRules,
)
from ismet.refdata.master import ActionKind, CorporateAction, adjust_bars
from ismet.testing.simvenue import Fault, SimVenue, sim_instrument
from ismet.transport.backoff import ExponentialBackoff
from ismet.transport.clock import ManualClock
from ismet.transport.http import HttpTransport
from ismet.transport.ws import SequenceOutcome, SequenceTracker

from .conftest import ACME, Factory, order, permissive_config

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


async def test_cancelled_caller_cannot_split_the_journal_from_memory(
    make_manager: Factory, venue: SimVenue, journal: SqliteJournal
) -> None:
    oms = make_manager()
    await oms.start()
    try:
        await venue.drain()
        task = asyncio.create_task(oms.submit(order("RS5", price="9.50")))
        for _ in range(3):
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        journalled = {r.client_order_id for r in journal.records()} - {None}
        assert ("RS5" in journalled) == ("RS5" in oms.orders)
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
        await oms.reconcile()
        assert oms.order("FL2").status is S.FILLED
        assert oms.net_position(ACME) == 100
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
    venue.fail_queries = 3
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
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.disconnect_stream()
    while oms.stream_healthy:
        await asyncio.sleep(0)
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
    while oms.stream_healthy:
        await asyncio.sleep(0)
    for _ in range(2000):
        if venue._subscribers:
            break
        await asyncio.sleep(0.001)
    assert venue._subscribers


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
    before = first._gross_notional()
    await first.abort()
    journal.close()

    second, journal = build()
    await second.start()
    try:
        assert second._gross_notional() == before == D("15015.00")
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
    assert oms._last_fill_price[ACME] == D("10.01")
    await oms.cancel("GR4")
    await oms.wait_for("GR4", lambda o: o.is_terminal, 2)
    assert oms._last_fill_price[ACME] == D("10.01")


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
    other = Symbol(venue="XSIM", ticker="OTHER")
    ledger = PositionLedger()
    ledger.apply(fill("A", BUY, "1", "10"))
    with pytest.raises(ValidationError, match="OTHER"):
        ledger.apply(fill("B", SELL, "1", "10", symbol=other))


def test_full_close_leaves_exactly_zero_beyond_the_ambient_context() -> None:
    size = "1234567890123.123456789012345678"
    ledger = build_ledger([fill("A", BUY, size, "2"), fill("B", SELL, size, "3")])
    assert ledger.quantity == 0 and not ledger.lots
    assert ledger.realized_pnl == D(size)


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
    other = Symbol(venue="XSIM", ticker="QUIET")
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
    bars = [bar(date(2020 + i // 4, 1 + (i % 4) * 3, 2), "97.13") for i in range(14)]
    actions = [
        CorporateAction(
            ActionKind.CASH_DIVIDEND,
            date(2020 + i // 4, 2 + (i % 4) * 3, 2),
            amount=D("0.37"),
        )
        for i in range(12)
    ]
    adjusted = adjust_bars(bars, actions, include_dividends=True)
    assert adjusted[0].close < adjusted[-1].close
    assert Bar.model_validate_json(adjusted[0].model_dump_json()) == adjusted[0]


def test_dividend_above_the_close_or_on_a_zero_close_is_refused() -> None:
    bars = [bar(date(2026, 1, 2), "1"), bar(date(2026, 3, 2), "1")]
    big = CorporateAction(ActionKind.CASH_DIVIDEND, date(2026, 2, 2), amount=D(5))
    with pytest.raises(ValidationError, match="not below"):
        adjust_bars(bars, [big], include_dividends=True)
    zero = [bar(date(2026, 1, 2), "0"), bar(date(2026, 3, 2), "1")]
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
