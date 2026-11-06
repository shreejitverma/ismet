"""Kill switch, orphan fill, overfill and stream health behaviour from worked scenarios.

The rule each test defends is stated by the module docstring of the order manager or
by the behaviour its name describes, and each expected value is written out by hand.
"""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pydantic
import pytest

from ismet.errors import (
    JournalError,
    KillSwitchEngaged,
    NoSendRecord,
    OrderStateError,
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
    new_order,
)
from ismet.execution.journal import VerifyResult
from ismet.execution.reconcile import ReconBreak
from ismet.execution.risk import check_open_orders
from ismet.execution.state import apply_event
from ismet.models import Symbol
from ismet.models.orders import (
    Acknowledged,
    EventOrigin,
    Fill,
    Filled,
    OrderEvent,
    OrderRequest,
    OrderSide,
    OrderType,
    Replaced,
    ReplaceRequested,
    SendStarted,
    SubmitIntent,
    TimeInForce,
    dump_order_event,
)
from ismet.models.orders import (
    OrderStatus as S,
)
from ismet.testing.simvenue import SIM_VENUE, Fault, SimVenue, sim_instrument
from ismet.transport.clock import ManualClock

from .conftest import (
    ACME,
    START,
    Factory,
    lose_stream,
    order,
    permissive_config,
    until,
)

D = Decimal
OTHR = Symbol(venue=SIM_VENUE, ticker="OTHR")


def fill(
    cid: str, fill_id: str = "1001", quantity: str = "500", price: str = "9.50"
) -> Fill:
    return Fill(
        fill_id=fill_id,
        client_order_id=cid,
        symbol=ACME,
        side=OrderSide.BUY,
        price=price,
        quantity=quantity,
        ts=START,
    )


def filled(cid: str, **kw: str) -> Filled:
    return Filled(
        client_order_id=cid, ts=START, origin=EventOrigin.VENUE, fill=fill(cid, **kw)
    )


def kinds_of(journal: Journal) -> list[RecordKind]:
    return [r.kind for r in journal.records()]


# Kill switch decisions come from the journal


async def test_a_new_halt_after_a_release_elsewhere_is_recorded(
    make_manager: Factory, venue: SimVenue, journal: Journal
) -> None:
    oms = make_manager(kill_poll_interval=3600)
    await oms.start()
    try:
        await venue.drain()
        await oms.engage_kill("first", operator="ops", cancel_open=False)
        journal.append(
            RecordKind.KILL_RELEASED,
            json.dumps({"reason": "clear", "operator": "ops"}),
        )
        await oms.engage_kill("second", operator="ops", cancel_open=False)
        assert journal.kill_state().engaged
        assert journal.kill_state().reason == "second"
        with pytest.raises(KillSwitchEngaged):
            await oms.submit(order("KD1", price="9.50"))
    finally:
        await oms.abort()


async def test_a_break_halt_keeps_a_sweep_an_operator_asked_for_elsewhere(
    make_manager: Factory, venue: SimVenue, journal: Journal
) -> None:
    oms = make_manager(kill_poll_interval=3600)
    await oms.start()
    try:
        await venue.drain()
        journal.append(
            RecordKind.KILL_ENGAGED,
            json.dumps({"reason": "desk", "operator": "ops", "cancel_open": True}),
        )
        venue.add_external_fill(ACME, OrderSide.BUY, D(1))
        await oms.reconcile()
        state = journal.kill_state()
        assert state.engaged and state.cancel_open and state.reason == "desk"
        assert oms.kill_state.cancel_open
    finally:
        await oms.abort()


class DownJournal(MemoryJournal):
    down = False

    def append(self, *args: object, **kw: object):  # type: ignore[no-untyped-def]
        if self.down:
            raise JournalError("disk gone")
        return super().append(*args, **kw)  # type: ignore[arg-type]

    def kill_state(self):  # type: ignore[no-untyped-def]
        if self.down:
            raise JournalError("disk gone")
        return super().kill_state()


async def test_the_kill_switch_still_engages_with_the_journal_down(
    venue: SimVenue, clock: ManualClock
) -> None:
    journal = DownJournal()
    oms = OrderManager(
        venue,
        journal,
        RiskEngine(permissive_config()),
        clock=clock,
        stream_settle=None,
    )
    await oms.start()
    try:
        await venue.drain()
        journal.down = True
        state = await oms.engage_kill("", operator="", cancel_open=False)
        assert state.engaged and oms.kill_state.engaged
    finally:
        await oms.abort()


async def test_release_with_the_journal_down_changes_nothing(
    venue: SimVenue, clock: ManualClock
) -> None:
    journal = DownJournal()
    oms = OrderManager(
        venue,
        journal,
        RiskEngine(permissive_config()),
        clock=clock,
        stream_settle=None,
    )
    await oms.start()
    try:
        await venue.drain()
        await oms.engage_kill("halt", operator="ops", cancel_open=False)
        journal.down = True
        with pytest.raises(JournalError):
            await oms.release_kill("clear", operator="ops")
        assert oms.kill_state.engaged
        journal.down = False
        with pytest.raises(KillSwitchEngaged):
            await oms.submit(order("KD2", price="9.50"))
    finally:
        await oms.abort()


@pytest.mark.parametrize(("reason", "operator"), [(" ", "ops"), ("clear", " ")])
async def test_a_release_needs_both_a_name_and_a_reason(
    oms: OrderManager, venue: SimVenue, reason: str, operator: str
) -> None:
    await venue.drain()
    await oms.engage_kill("halt", operator="ops", cancel_open=False)
    with pytest.raises(ValidationError):
        await oms.release_kill(reason, operator=operator)
    assert oms.kill_state.engaged


async def test_an_amend_during_a_kill_never_reaches_the_venue(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("KD3", price="9.50"))
    await venue.drain()
    await oms.engage_kill("halt", cancel_open=False)
    with pytest.raises(KillSwitchEngaged):
        await oms.amend("KD3", quantity=D(500))
    snapshot = await venue.order_status("KD3", ACME)
    assert snapshot is not None and snapshot.quantity == 100


# Orphan fills


async def test_the_same_fill_id_on_two_unknown_orders_is_two_breaks(
    oms: OrderManager, venue: SimVenue, clock: ManualClock
) -> None:
    await venue.drain()
    clock.advance(1)
    venue._fills += [
        fill("OUT-A").model_copy(update={"ts": clock.now()}),
        fill("OUT-B").model_copy(update={"ts": clock.now()}),
    ]
    report = await oms.reconcile()
    found = [b.subject for b in report.breaks if b.kind is BreakKind.ORPHAN_FILL]
    assert sorted(found) == ["OUT-A:1001", "OUT-B:1001"]
    again = await oms.reconcile()
    assert BreakKind.ORPHAN_FILL not in [b.kind for b in again.breaks]


async def test_the_stream_reports_one_fill_id_on_two_unknown_orders_twice(
    make_manager: Factory, venue: SimVenue
) -> None:
    seen: list[str] = []
    oms = make_manager(on_break=lambda item: seen.append(item.subject))
    await oms.start()
    try:
        await venue.drain()
        for cid in ("OUT-A", "OUT-A", "OUT-B"):
            venue._deliver(filled(cid))
        await until(lambda: len(seen) >= 2)
        await venue.drain()
        assert seen == ["OUT-A:1001", "OUT-B:1001"]
    finally:
        await oms.abort()


async def test_a_released_orphan_fill_stays_reported_across_a_restart(
    make_manager: Factory, venue: SimVenue, clock: ManualClock, journal: Journal
) -> None:
    oms = make_manager()
    await oms.start()
    await venue.drain()
    clock.advance(1)
    venue._fills.append(fill("OUT-A").model_copy(update={"ts": clock.now()}))
    await oms.reconcile()
    assert oms.kill_state.engaged
    await oms.release_kill("checked", operator="ops")
    await oms.abort()
    restarted = make_manager()
    report = await restarted.start()
    try:
        assert BreakKind.ORPHAN_FILL not in [b.kind for b in report.breaks]
        assert not restarted.kill_state.engaged
    finally:
        await restarted.abort()


async def test_a_cancelled_pass_does_not_lose_an_orphan_fill(
    oms: OrderManager,
    venue: SimVenue,
    clock: ManualClock,
    journal: Journal,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await venue.drain()
    clock.advance(1)
    venue._fills.append(fill("OUT-A").model_copy(update={"ts": clock.now()}))
    asked = asyncio.Event()

    async def hang() -> list[object]:
        asked.set()
        await asyncio.Event().wait()
        return []

    with monkeypatch.context() as patch:
        patch.setattr(venue, "positions", hang)
        task = asyncio.create_task(oms.reconcile())
        await asyncio.wait_for(asked.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    report = await oms.reconcile()
    assert BreakKind.ORPHAN_FILL in [b.kind for b in report.breaks]
    assert oms.kill_state.engaged
    assert kinds_of(journal).count(RecordKind.RECON_BREAK) == 1


async def test_the_halt_is_durable_before_the_break_that_caused_it(
    oms: OrderManager, venue: SimVenue, clock: ManualClock, journal: Journal
) -> None:
    await venue.drain()
    clock.advance(1)
    venue._fills.append(fill("OUT-A").model_copy(update={"ts": clock.now()}))
    await oms.reconcile()
    kinds = kinds_of(journal)
    assert kinds.index(RecordKind.KILL_ENGAGED) < kinds.index(RecordKind.RECON_BREAK)


async def test_one_fill_id_corrected_on_two_orders_is_raised_for_each(
    oms: OrderManager, venue: SimVenue, clock: ManualClock
) -> None:
    await venue.drain()
    await oms.submit(order("CT1", price="9.50"))
    await oms.submit(order("CT2", price="9.50"))
    await venue.drain()
    venue._deliver(filled("CT1", quantity="10"))
    venue._deliver(filled("CT2", quantity="10"))
    await oms.wait_for("CT1", lambda o: o.filled_quantity == 10, 2)
    await oms.wait_for("CT2", lambda o: o.filled_quantity == 10, 2)
    clock.advance(1)
    venue._fills += [
        fill("CT1", quantity="20").model_copy(update={"ts": clock.now()}),
        fill("CT2", quantity="20").model_copy(update={"ts": clock.now()}),
    ]
    await oms.reconcile()
    for cid in ("CT1", "CT2"):
        assert [a for a in oms.order(cid).anomalies if "different terms" in a]


# Reconciliation never closes an order over a difference


async def test_a_terminal_venue_status_does_not_close_an_order_with_missing_fills(
    oms: OrderManager, venue: SimVenue, clock: ManualClock
) -> None:
    await venue.drain()
    await oms.submit(order("TC1", price="9.50"))
    await venue.drain()
    held = venue._orders["TC1"]
    held.status = S.FILLED
    held.filled = D(100)
    report = await oms.reconcile()
    assert BreakKind.FILL_MISMATCH in [b.kind for b in report.breaks]
    state = oms.order("TC1")
    assert state.is_open and state.filled_quantity == 0
    clock.advance(1)
    venue._fills.append(
        fill("TC1", quantity="100").model_copy(update={"ts": clock.now()})
    )
    await oms.reconcile()
    state = oms.order("TC1")
    assert state.status is S.FILLED and state.filled_quantity == 100


async def test_a_fill_landing_during_the_pass_is_found_by_the_position_check(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("PC1", price="9.50"))
    await venue.drain()
    venue.report_faults.drop_next = 1
    armed = [True]

    def trade(method: str) -> None:
        if method == "fills_since" and armed:
            armed.clear()
            venue.trade_through(ACME, "9.50", "40")

    venue.after_answer = trade
    report = await oms.reconcile()
    venue.after_answer = None
    assert oms.order("PC1").filled_quantity == 40
    assert not [b for b in report.breaks if b.is_critical]
    assert not oms.kill_state.engaged


# Halts that a crash could have lost


async def test_an_overfill_recorded_before_a_crash_halts_on_restart(
    make_manager: Factory, venue: SimVenue, journal: Journal
) -> None:
    oms = make_manager()
    await oms.start()
    await venue.drain()
    await oms.submit(order("CR1", price="9.50"))
    await venue.drain()
    await oms.abort()
    journal.append(
        RecordKind.ORDER_EVENT,
        dump_order_event(filled("CR1", quantity="150")),
        client_order_id="CR1",
    )
    restarted = make_manager()
    await restarted.start()
    try:
        assert restarted.kill_state.engaged
        assert journal.kill_state().engaged
        assert journal.kill_state().operator == "oms"
    finally:
        await restarted.abort()


async def test_a_released_overfill_does_not_halt_again_on_restart(
    make_manager: Factory, venue: SimVenue, journal: Journal
) -> None:
    oms = make_manager()
    await oms.start()
    await venue.drain()
    await oms.submit(order("CR2", price="9.50"))
    await venue.drain()
    venue._deliver(filled("CR2", quantity="150"))
    await until(lambda: journal.kill_state().engaged)
    await oms.release_kill("checked", operator="ops")
    await oms.abort()
    # The simulated venue never heard of that fill, so the breaks must not halt either.
    restarted = make_manager(halt_on_break=False)
    await restarted.start()
    try:
        assert not restarted.kill_state.engaged
    finally:
        await restarted.abort()


async def test_an_anomaly_hook_that_raises_does_not_stop_the_halt(
    make_manager: Factory, venue: SimVenue, journal: Journal
) -> None:
    """The halt lands, the record lands, and the stream that carried it stays up."""

    def explode(*_: object) -> None:
        raise RuntimeError("hook bug")

    oms = make_manager(on_anomaly=explode)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("CR3", price="9.50"))
        await venue.drain()
        venue._deliver(filled("CR3", quantity="150"))
        await until(lambda: oms.kill_state.engaged)
        # A later report on the same stream is still consumed, so the hook's exception
        # ended neither the consumer nor the record of the anomaly it was told about.
        venue._deliver(filled("CR3", fill_id="1002", quantity="1"))
        await oms.wait_for("CR3", lambda o: len(o.fills) == 2, 2)
        assert oms.stream_healthy
        anomalies = [r for r in journal.records() if r.kind is RecordKind.ANOMALY]
        assert anomalies and "overfill" in anomalies[0].payload
    finally:
        await oms.abort()


def replaced_order(fill_quantity: str, new_quantity: int) -> object:
    state = new_order(
        SubmitIntent(
            client_order_id="RP1", ts=START, request=order("RP1", quantity="10")
        )
    )
    events: list[OrderEvent] = [
        SendStarted(client_order_id="RP1", ts=START),
        Acknowledged(client_order_id="RP1", ts=START, venue_order_id="V1"),
        ReplaceRequested(client_order_id="RP1", ts=START, quantity=D(new_quantity)),
        filled("RP1", quantity=fill_quantity),
        Replaced(client_order_id="RP1", ts=START, quantity=D(new_quantity)),
    ]
    for event in events:
        state = apply_event(state, event)
    return state


def test_a_replace_settling_below_what_filled_is_an_overfill() -> None:
    state = replaced_order("6", 5)
    assert state.status is S.FILLED
    assert (state.quantity, state.filled_quantity) == (5, 6)
    assert state.anomalies == ("overfill: 6 filled against 5",)


def test_a_replace_settling_above_what_filled_is_not() -> None:
    state = replaced_order("4", 5)
    assert state.status is S.PARTIALLY_FILLED
    assert state.anomalies == ()


# Risk


def test_a_per_symbol_limit_applies_whatever_the_case_of_its_key() -> None:
    config = permissive_config(
        per_symbol={"acme@ZSIM": RiskLimits(max_order_quantity=D(1))}
    )
    labels = [label for label, _ in config.limits_for(order("RK1", quantity="500"))]
    assert labels == ["default", "symbol:ACME@ZSIM"]


def test_per_symbol_keys_that_collide_are_refused() -> None:
    with pytest.raises(pydantic.ValidationError, match="two keys"):
        RiskConfig(
            default=RiskLimits(),
            per_symbol={
                "acme@ZSIM": RiskLimits(max_order_quantity=D(1)),
                "ACME@ZSIM": RiskLimits(max_order_quantity=D(1000)),
            },
        )


@pytest.mark.parametrize(
    ("scope", "terms"),
    [
        ("symbol:ACME@ZSIM", {}),
        ("account:A1", {"account": "A1"}),
        ("strategy:S1", {"strategy": "S1"}),
    ],
)
def test_a_scoped_open_order_limit_counts_the_orders_in_its_scope(
    scope: str, terms: dict[str, str]
) -> None:
    open_orders = [
        new_order(
            SubmitIntent(
                client_order_id=f"SC{i}",
                ts=START,
                request=order(f"SC{i}", **terms),
            )
        )
        for i in range(2)
    ]
    context = RiskContext(
        request=order("SC9", **terms),
        instrument=None,
        reference=None,
        now=START,
        open_orders=open_orders,
    )
    decision = check_open_orders(context, RiskLimits(max_open_orders=2), scope)
    assert decision.check == "max_open_orders"
    assert decision.reason.startswith("2 open orders")


def gross_config() -> RiskConfig:
    return permissive_config(default=RiskLimits(max_gross_notional=D(100_000_000)))


@pytest.fixture
def two_venue(clock: ManualClock) -> SimVenue:
    sim = SimVenue(
        clock=clock, instruments=[sim_instrument("ACME"), sim_instrument("OTHR")]
    )
    sim.set_quote(ACME, "9.99", "10.01")
    sim.set_quote(OTHR, "9.99", "10.01")
    return sim


async def test_an_open_order_with_no_instrument_makes_gross_exposure_unknown(
    two_venue: SimVenue, journal: Journal, clock: ManualClock
) -> None:
    options = {"clock": clock, "stream_settle": None, "reconcile_interval": 3600.0}
    first = OrderManager(two_venue, journal, RiskEngine(gross_config()), **options)
    await first.start()
    await two_venue.drain()
    await first.submit(order("GE1", price="9.50", symbol=OTHR))
    await two_venue.drain()
    await first.abort()

    async def lookup(symbol: Symbol):  # type: ignore[no-untyped-def]
        if symbol == OTHR:
            raise TransportError("reference data down")
        return sim_instrument("ACME")

    second = OrderManager(
        two_venue,
        journal,
        RiskEngine(gross_config()),
        instrument_source=lookup,
        **options,
    )
    await second.start()
    try:
        with pytest.raises(RiskRejected) as caught:
            await second.submit(order("GE2", price="9.50"))
        assert caught.value.reason == "gross exposure unknown"
    finally:
        await second.abort()


async def test_a_position_marked_at_zero_makes_gross_exposure_unknown(
    two_venue: SimVenue, journal: Journal, clock: ManualClock
) -> None:
    oms = OrderManager(
        two_venue,
        journal,
        RiskEngine(gross_config()),
        clock=clock,
        stream_settle=None,
        reconcile_interval=3600.0,
    )
    await oms.start()
    try:
        await two_venue.drain()
        await oms.adjust_position(OTHR, D(1000), operator="ops", reason="transfer")
        two_venue.set_quote(OTHR, "0", "0")
        with pytest.raises(RiskRejected) as caught:
            await oms.submit(order("GE3", price="9.50"))
        assert caught.value.reason == "gross exposure unknown"
    finally:
        await oms.abort()


# Position adjustments


@pytest.mark.parametrize("delta", ["NaN", "sNaN", "Infinity", "1E+1000"])
async def test_an_adjustment_out_of_range_is_refused_before_it_is_journalled(
    oms: OrderManager, journal: Journal, delta: str
) -> None:
    before = kinds_of(journal)
    with pytest.raises(ValidationError):
        await oms.adjust_position(ACME, D(delta), operator="ops", reason="typo")
    assert kinds_of(journal) == before
    assert oms.net_position(ACME) == 0


# Stream health


async def test_a_stream_that_never_holds_is_not_healthy_and_backs_off(
    make_manager: Factory, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fails_late():  # type: ignore[no-untyped-def]
        await asyncio.sleep(0.01)
        raise TransportError("connect failed")
        yield

    attempts: list[int] = []

    class Recording:
        def __init__(self, **_: object) -> None:
            pass

        def delay(self, attempt: int) -> float:
            attempts.append(attempt)
            return 0.0

    monkeypatch.setattr("ismet.execution.manager.ExponentialBackoff", Recording)
    monkeypatch.setattr(venue, "stream_executions", fails_late)
    oms = make_manager(stream_settle=0.05)
    await oms.start()
    try:
        assert not oms.stream_healthy
        await until(lambda: len(attempts) >= 3)
        assert attempts[:3] == [0, 1, 2]
        with pytest.raises(RiskRejected) as caught:
            await oms.submit(order("SH1", price="9.50"))
        assert caught.value.check == "execution_stream"
    finally:
        await oms.abort()


async def test_a_stream_that_holds_is_healthy_when_start_returns(
    make_manager: Factory, venue: SimVenue
) -> None:
    oms = make_manager(stream_settle=0.02)
    await oms.start()
    try:
        assert oms.stream_healthy
        assert (await oms.submit(order("SH2", price="9.50"))).is_open
    finally:
        await oms.abort()


async def test_a_stream_that_delivers_and_dies_never_counts_as_up(
    make_manager: Factory, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With ``stream_settle`` an hour, a stream living one event never held up."""
    attempts: list[int] = []
    healthy: list[bool] = []
    oms = make_manager(stream_settle=3600.0, halt_on_break=False)

    async def delivers_once():  # type: ignore[no-untyped-def]
        yield Acknowledged(client_order_id="OUT-Z", ts=START, venue_order_id="V")
        # The consumer has taken the event: this is the moment it could wrongly
        # count the stream as up.
        healthy.append(oms.stream_healthy)
        raise TransportError("dropped")

    class Recording:
        def __init__(self, **_: object) -> None:
            pass

        def delay(self, attempt: int) -> float:
            attempts.append(attempt)
            healthy.append(oms.stream_healthy)
            return 0.0

    monkeypatch.setattr("ismet.execution.manager.ExponentialBackoff", Recording)
    monkeypatch.setattr(venue, "stream_executions", delivers_once)
    await asyncio.wait_for(oms.start(), 5)
    try:
        await until(lambda: len(attempts) >= 4)
        assert attempts[:4] == [0, 1, 2, 3]
        assert not any(healthy)
    finally:
        await oms.abort()


def gated_open_orders(
    venue: SimVenue, monkeypatch: pytest.MonkeyPatch, *, open_after: int = 0
) -> tuple[asyncio.Event, asyncio.Event]:
    """Hold the venue's open order query once ``open_after`` answers have gone out."""
    reached, release = asyncio.Event(), asyncio.Event()
    real = venue.open_orders
    answered = [0]

    async def held():  # type: ignore[no-untyped-def]
        if answered[0] >= open_after:
            reached.set()
            await release.wait()
        answered[0] += 1
        return await real()

    monkeypatch.setattr(venue, "open_orders", held)
    return reached, release


async def test_no_order_is_admitted_while_the_catch_up_is_running(
    oms: OrderManager, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    await venue.drain()
    await oms.submit(order("CU1", price="9.50"))
    await venue.drain()
    reached, release = gated_open_orders(venue, monkeypatch)
    await lose_stream(oms, venue)
    await asyncio.wait_for(reached.wait(), 5)
    await until(lambda: bool(venue._subscribers))
    venue.trade_through(ACME, "9.50", "10")
    await oms.wait_for("CU1", lambda o: o.filled_quantity == 10, 5)
    assert not oms.stream_healthy
    with pytest.raises(RiskRejected) as caught:
        await oms.submit(order("CU2", price="9.50"))
    assert caught.value.check == "execution_stream"
    release.set()
    await until(lambda: oms.stream_healthy)


async def test_no_order_is_admitted_before_the_settled_follow_up_finishes(
    make_manager: Factory, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    oms = make_manager(stream_settle=0.01)
    await oms.start()
    try:
        await venue.drain()
        reached, release = gated_open_orders(venue, monkeypatch, open_after=1)
        await lose_stream(oms, venue)
        await asyncio.wait_for(reached.wait(), 5)
        assert not oms.stream_healthy
        release.set()
        await until(lambda: oms.stream_healthy)
    finally:
        await oms.abort()


class BreakJournalDown(MemoryJournal):
    failing = False

    def append(self, *args: object, **kw: object):  # type: ignore[no-untyped-def]
        if self.failing and args[0] is RecordKind.RECON_BREAK:
            raise JournalError("disk gone")
        return super().append(*args, **kw)  # type: ignore[arg-type]


async def test_an_old_orphan_fill_survives_a_break_that_never_reached_the_journal(
    venue: SimVenue, clock: ManualClock
) -> None:
    journal = BreakJournalDown()

    def build() -> OrderManager:
        return OrderManager(
            venue,
            journal,
            RiskEngine(permissive_config()),
            clock=clock,
            stream_settle=None,
            reconcile_interval=3600.0,
            fill_lookback=3600.0,
            unknown_grace=0.0,
        )

    oms = build()
    await oms.start()
    await venue.drain()
    clock.advance(1)
    venue._fills.append(fill("OUT-A").model_copy(update={"ts": clock.now()}))
    clock.advance(7200)
    journal.failing = True
    await oms.reconcile()
    journal.failing = False
    await oms.abort()
    restarted = build()
    report = await restarted.start()
    try:
        assert [
            b.subject for b in report.breaks if b.kind is BreakKind.ORPHAN_FILL
        ] == ["OUT-A:1001"]
        assert kinds_of(journal).count(RecordKind.RECON_BREAK) == 1
    finally:
        await restarted.abort()


def orphan_manager(
    venue: SimVenue, journal: Journal, clock: ManualClock, **options: object
) -> OrderManager:
    settings: dict[str, object] = {
        "clock": clock,
        "stream_settle": None,
        "reconcile_interval": 3600.0,
        "fill_lookback": 3600.0,
        "unknown_grace": 0.0,
    }
    settings.update(options)
    return OrderManager(venue, journal, RiskEngine(permissive_config()), **settings)  # type: ignore[arg-type]


def orphan_subjects(breaks: list[ReconBreak]) -> list[str]:
    return [b.subject for b in breaks if b.kind is BreakKind.ORPHAN_FILL]


async def test_an_orphan_fill_whose_break_never_landed_is_recorded_by_the_next_pass(
    venue: SimVenue, clock: ManualClock
) -> None:
    """A fill is on record once its break reaches the journal, not once it is seen."""
    journal = BreakJournalDown()
    oms = orphan_manager(venue, journal, clock)
    await oms.start()
    await venue.drain()
    clock.advance(1)
    venue._fills.append(fill("OUT-B").model_copy(update={"ts": clock.now()}))
    clock.advance(7200)
    journal.failing = True
    first = await oms.reconcile()
    assert orphan_subjects(first.breaks) == ["OUT-B:1001"]
    assert kinds_of(journal).count(RecordKind.RECON_BREAK) == 0

    journal.failing = False
    clock.advance(1)
    second = await oms.reconcile()
    try:
        assert orphan_subjects(second.breaks) == ["OUT-B:1001"]
        assert kinds_of(journal).count(RecordKind.RECON_BREAK) == 1
        third = await oms.reconcile()
        assert orphan_subjects(third.breaks) == []
    finally:
        await oms.abort()
    restarted = orphan_manager(venue, journal, clock)
    report = await restarted.start()
    try:
        assert orphan_subjects(report.breaks) == []
        assert kinds_of(journal).count(RecordKind.RECON_BREAK) == 1
    finally:
        await restarted.abort()


async def test_a_streamed_orphan_fill_whose_record_fails_stops_new_orders(
    venue: SimVenue, clock: ManualClock
) -> None:
    """A venue report that could not be journalled stops new orders, like any other,
    and the next sweep after the journal recovers puts it on record."""
    journal = BreakJournalDown()
    oms = orphan_manager(venue, journal, clock, halt_on_break=False)
    await oms.start()
    await venue.drain()
    journal.failing = True
    venue._deliver(filled("OUT-C"))
    await venue.drain()
    try:
        assert kinds_of(journal).count(RecordKind.RECON_BREAK) == 0
        with pytest.raises(KillSwitchEngaged, match="journal failure"):
            await oms.submit(order(price="9.50"))
        journal.failing = False
        venue._fills.append(fill("OUT-C"))
        clock.advance(1)
        report = await oms.reconcile()
        assert orphan_subjects(report.breaks) == ["OUT-C:1001"]
        assert kinds_of(journal).count(RecordKind.RECON_BREAK) == 1
    finally:
        await oms.abort()


async def test_a_clean_pass_still_moves_the_sweep_window(
    oms: OrderManager, venue: SimVenue, clock: ManualClock, journal: Journal
) -> None:
    await venue.drain()
    clock.advance(7200)
    moved_to = clock.now()
    await oms.reconcile()
    marks = [
        json.loads(r.payload)["at"]
        for r in journal.records()
        if r.kind is RecordKind.SWEEP_MARK
    ]
    assert marks[-1] == moved_to.isoformat()


# Not found


async def test_an_order_arriving_while_its_fills_are_fetched_is_not_closed(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.faults.append(Fault.LOSE_REQUEST)
    late = order("NF1", price="9.50")
    await oms.submit(late)
    assert oms.order("NF1").status is S.UNKNOWN
    armed = [True]

    def arrives(method: str) -> None:
        if method == "fills_since" and armed:
            armed.clear()
            venue.add_external_order(late)
            venue._deliver(
                Acknowledged(client_order_id="NF1", ts=START, venue_order_id="V9")
            )

    venue.after_answer = arrives
    await oms.reconcile()
    venue.after_answer = None
    assert oms.order("NF1").is_open
    assert oms.order("NF1").reject_code is None
    await oms.reconcile()
    assert oms.order("NF1").status is S.ACCEPTED


async def test_an_order_the_venue_never_got_still_closes_as_not_found(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.faults.append(Fault.LOSE_REQUEST)
    await oms.submit(order("NF2", price="9.50"))
    await oms.reconcile()
    state = oms.order("NF2")
    assert state.status is S.REJECTED and state.reject_code == "NOT_FOUND_AT_VENUE"


async def test_a_purged_order_with_fills_closes_as_canceled_with_a_break(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("NF3", price="9.50"))
    await venue.drain()
    venue.report_faults.drop_next = 1
    venue.trade_through(ACME, "9.50", "40")
    venue.forget_order("NF3")
    report = await oms.reconcile()
    state = oms.order("NF3")
    assert state.status is S.CANCELED and state.filled_quantity == 40
    assert BreakKind.MISSING_AT_VENUE in [b.kind for b in report.breaks]


# Fills that belong to something else


@pytest.mark.parametrize(
    "terms", [{"side": OrderSide.SELL}, {"symbol": OTHR}], ids=["side", "symbol"]
)
async def test_a_fill_for_another_side_or_symbol_halts(
    oms: OrderManager, venue: SimVenue, terms: dict[str, object]
) -> None:
    await venue.drain()
    await oms.submit(order("FM1", price="9.50"))
    await venue.drain()
    foreign = fill("FM1", quantity="40").model_copy(update=terms)
    venue._deliver(
        Filled(client_order_id="FM1", ts=START, origin=EventOrigin.VENUE, fill=foreign)
    )
    await until(lambda: oms.kill_state.engaged)
    assert [a for a in oms.order("FM1").anomalies if "does not match" in a]


def test_a_fill_on_the_orders_own_terms_is_not_an_anomaly() -> None:
    state = replaced_order("4", 5)
    assert not state.anomalies


async def test_a_foreign_fill_recorded_before_a_crash_halts_on_restart(
    make_manager: Factory, venue: SimVenue, journal: Journal
) -> None:
    oms = make_manager()
    await oms.start()
    await venue.drain()
    await oms.submit(order("FM2", price="9.50"))
    await venue.drain()
    await oms.abort()
    foreign = fill("FM2", quantity="40").model_copy(update={"side": OrderSide.SELL})
    journal.append(
        RecordKind.ORDER_EVENT,
        dump_order_event(
            Filled(
                client_order_id="FM2", ts=START, origin=EventOrigin.VENUE, fill=foreign
            )
        ),
        client_order_id="FM2",
    )
    restarted = make_manager(halt_on_break=False)
    await restarted.start()
    try:
        assert restarted.kill_state.engaged
    finally:
        await restarted.abort()


# Scoped limits


@pytest.mark.parametrize("scope", ["per_account", "per_strategy"])
@pytest.mark.parametrize(
    "limits",
    [{"max_position": D(100)}, {"max_gross_notional": D(100)}],
    ids=["position", "gross"],
)
def test_position_limits_per_account_or_strategy_are_refused(
    scope: str, limits: dict[str, Decimal]
) -> None:
    with pytest.raises(pydantic.ValidationError, match="net position per symbol"):
        RiskConfig(default=RiskLimits(), **{scope: {"A": RiskLimits(**limits)}})


def test_order_level_limits_per_account_and_position_limits_per_symbol_remain() -> None:
    config = RiskConfig(
        default=RiskLimits(),
        per_account={"A": RiskLimits(max_order_quantity=D(1), max_open_orders=1)},
        per_strategy={"S": RiskLimits(max_order_notional=D(1))},
        per_symbol={"ACME@ZSIM": RiskLimits(max_position=D(1))},
    )
    labels = [
        label
        for label, _ in config.limits_for(
            order("SL1", account="A", strategy="S", quantity="1")
        )
    ]
    assert labels == ["default", "account:A", "strategy:S", "symbol:ACME@ZSIM"]


# Anomalies with the journal failing


class AnomalyOsError(MemoryJournal):
    def append(self, *args: object, **kw: object):  # type: ignore[no-untyped-def]
        if args[0] is RecordKind.ANOMALY:
            raise OSError("disk full")
        return super().append(*args, **kw)  # type: ignore[arg-type]


async def test_an_oserror_writing_an_anomaly_does_not_skip_the_halt(
    venue: SimVenue, clock: ManualClock
) -> None:
    oms = OrderManager(
        venue,
        AnomalyOsError(),
        RiskEngine(permissive_config()),
        clock=clock,
        stream_settle=None,
        reconcile_interval=3600.0,
        halt_on_break=False,
    )
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("AO1", price="9.50"))
        await venue.drain()
        venue._deliver(filled("AO1", quantity="150"))
        await until(lambda: oms.kill_state.engaged)
        assert oms.stream_healthy
    finally:
        await oms.abort()


async def test_the_halt_is_durable_before_the_anomaly_that_caused_it(
    oms: OrderManager, venue: SimVenue, journal: Journal
) -> None:
    await venue.drain()
    await oms.submit(order("AO3", price="9.50"))
    await venue.drain()
    venue._deliver(filled("AO3", quantity="150"))
    await until(lambda: RecordKind.ANOMALY in kinds_of(journal))
    kinds = kinds_of(journal)
    assert kinds.index(RecordKind.KILL_ENGAGED) < kinds.index(RecordKind.ANOMALY)


# Marks


async def test_a_late_fill_at_an_old_price_is_not_a_current_mark(
    oms: OrderManager, venue: SimVenue, clock: ManualClock
) -> None:
    await venue.drain()
    await oms.submit(order("MK1", price="9.50"))
    await venue.drain()
    old = clock.now()
    clock.advance(600)
    venue._deliver(
        Filled(
            client_order_id="MK1",
            ts=clock.now(),
            origin=EventOrigin.VENUE,
            fill=fill("MK1", quantity="10").model_copy(update={"ts": old}),
        )
    )
    await oms.wait_for("MK1", lambda o: o.filled_quantity == 10, 2)
    assert oms._mark(ACME) == D("10.00")


async def test_a_current_fill_is_a_mark(
    oms: OrderManager, venue: SimVenue, clock: ManualClock
) -> None:
    await venue.drain()
    await oms.submit(order("MK2", price="9.50"))
    await venue.drain()
    venue.trade_through(ACME, "9.50", "10")
    await oms.wait_for("MK2", lambda o: o.filled_quantity == 10, 2)
    assert oms._mark(ACME) == D("9.50")


# The simulator


async def test_an_unelected_stop_limit_does_not_fill_on_a_print_at_its_limit(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(
        order(
            "SS1",
            price="10.60",
            order_type=OrderType.STOP_LIMIT,
            stop_price="10.50",
            time_in_force=TimeInForce.GTC,
        )
    )
    await venue.drain()
    venue.trade_through(ACME, "10.00", "100")
    snapshot = await venue.order_status("SS1", ACME)
    assert snapshot is not None and snapshot.filled_quantity == 0


async def test_an_elected_stop_limit_fills_on_a_print_at_its_limit(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(
        order(
            "SS2",
            price="10.60",
            order_type=OrderType.STOP_LIMIT,
            stop_price="10.50",
            time_in_force=TimeInForce.GTC,
        )
    )
    await venue.drain()
    venue.trade_through(ACME, "10.55", "100")
    snapshot = await venue.order_status("SS2", ACME)
    assert snapshot is not None and snapshot.filled_quantity == 100


def sell_stop_limit(cid: str) -> OrderRequest:
    """A sell stop at 9.50 with its limit at 9.40, both below the 9.99 bid."""
    return order(
        cid,
        side=OrderSide.SELL,
        price="9.40",
        order_type=OrderType.STOP_LIMIT,
        stop_price="9.50",
        time_in_force=TimeInForce.GTC,
    )


def buy_stop_limit(cid: str) -> OrderRequest:
    """A buy stop at 10.50 with its limit at 10.60, both above the 10.01 ask."""
    return order(
        cid,
        price="10.60",
        order_type=OrderType.STOP_LIMIT,
        stop_price="10.50",
        time_in_force=TimeInForce.GTC,
    )


async def filled_at_venue(venue: SimVenue, cid: str) -> Decimal:
    snapshot = await venue.order_status(cid, ACME)
    assert snapshot is not None
    return snapshot.filled_quantity


async def test_an_unelected_sell_stop_limit_does_not_fill_on_a_print_at_its_limit(
    venue: SimVenue,
) -> None:
    await venue.place_order(sell_stop_limit("SS3"))
    venue.trade_through(ACME, "10.00", "100")  # above the stop, so not elected
    assert await filled_at_venue(venue, "SS3") == 0


async def test_an_elected_sell_stop_limit_fills_on_a_print_through_its_stop(
    venue: SimVenue,
) -> None:
    await venue.place_order(sell_stop_limit("SS4"))
    venue.trade_through(ACME, "9.45", "100")  # at or below the stop, above the limit
    assert await filled_at_venue(venue, "SS4") == 100


async def test_a_stop_limit_elected_by_a_quote_rests_at_its_limit(
    venue: SimVenue,
) -> None:
    """The ask reaches the stop but not the limit, so the order rests, elected."""
    await venue.place_order(buy_stop_limit("SS5"))
    venue.set_quote(ACME, "10.52", "10.70")
    assert await filled_at_venue(venue, "SS5") == 0
    venue.trade_through(ACME, "10.30", "100")  # below the stop, within the limit
    assert await filled_at_venue(venue, "SS5") == 100


async def test_a_stop_limit_elected_by_a_quote_takes_the_ask_within_its_limit(
    venue: SimVenue,
) -> None:
    await venue.place_order(buy_stop_limit("SS6"))
    venue.set_quote(ACME, "10.52", "10.55")
    snapshot = await venue.order_status("SS6", ACME)
    assert snapshot is not None and snapshot.filled_quantity == 100
    assert [f.price for f in await venue.fills_since(START)] == [D("10.55")]


async def test_an_unelected_stop_limit_does_not_fill_on_an_amend(
    venue: SimVenue,
) -> None:
    await venue.place_order(buy_stop_limit("SS7"))
    await venue.amend_order("SS7", ACME, quantity=D(200))
    assert await filled_at_venue(venue, "SS7") == 0


async def test_an_unelected_stop_does_not_fill_on_an_amend(venue: SimVenue) -> None:
    await venue.place_order(
        order("SS8", price=None, order_type=OrderType.STOP, stop_price="10.50")
    )
    await venue.amend_order("SS8", ACME, quantity=D(200))
    assert await filled_at_venue(venue, "SS8") == 0


async def test_an_elected_stop_fills_when_liquidity_appears_below_the_stop(
    venue: SimVenue,
) -> None:
    """Elected with no ask quoted, then an ask appears back through the stop."""
    venue.set_quote(ACME, "10.00", None)
    await venue.place_order(
        order("SS11", price=None, order_type=OrderType.STOP, stop_price="10.50")
    )
    venue.trade_through(ACME, "10.55", "100")
    assert await filled_at_venue(venue, "SS11") == 0
    venue.set_quote(ACME, "10.00", "10.40")
    assert await filled_at_venue(venue, "SS11") == 100


async def test_an_elected_stop_that_partially_filled_takes_later_liquidity(
    venue: SimVenue,
) -> None:
    venue.set_quote(ACME, "10.00", "10.55", ask_size="40")
    await venue.place_order(
        order("SS12", price=None, order_type=OrderType.STOP, stop_price="10.50")
    )
    venue.trade_through(ACME, "10.55", "100")
    assert await filled_at_venue(venue, "SS12") == 40
    venue.set_quote(ACME, "10.00", "10.40", ask_size="100")
    assert await filled_at_venue(venue, "SS12") == 100


async def test_a_stop_elected_by_a_print_takes_the_touch(venue: SimVenue) -> None:
    """A stop with no limit becomes a market order once elected."""
    await venue.place_order(
        order("SS9", price=None, order_type=OrderType.STOP, stop_price="10.50")
    )
    venue.trade_through(ACME, "10.55", "100")
    assert await filled_at_venue(venue, "SS9") == 100
    assert [f.price for f in await venue.fills_since(START)] == [D("10.01")]


async def test_a_sell_stop_elected_by_a_print_takes_the_bid(venue: SimVenue) -> None:
    await venue.place_order(
        order(
            "SS10",
            side=OrderSide.SELL,
            price=None,
            order_type=OrderType.STOP,
            stop_price="9.50",
        )
    )
    venue.trade_through(ACME, "9.60", "100")  # above the stop, so not elected
    assert await filled_at_venue(venue, "SS10") == 0
    venue.trade_through(ACME, "9.50", "100")
    assert await filled_at_venue(venue, "SS10") == 100
    assert [f.price for f in await venue.fills_since(START)] == [D("9.99")]


# Send records


class RecordKeepingVenue(SimVenue):
    """A venue whose adapter keeps its own send records, as the FIX provider does.

    With ``lost_rows`` every status query raises, as the FIX provider's does once its
    store is gone, whether or not the venue still holds the order.
    """

    record_id: str | None = "store-1"
    lost_rows: bool = False

    def send_record_id(self) -> str | None:
        return self.record_id

    async def order_status(self, client_order_id, symbol):  # type: ignore[no-untyped-def]
        if self.lost_rows:
            raise NoSendRecord(f"no send record for {client_order_id}")
        return await super().order_status(client_order_id, symbol)


def record_venue(clock: ManualClock) -> RecordKeepingVenue:
    sim = RecordKeepingVenue(clock=clock, instruments=[sim_instrument("ACME")])
    sim.set_quote(ACME, "9.99", "10.01")
    return sim


def record_manager(
    venue: SimVenue, journal: Journal, clock: ManualClock
) -> OrderManager:
    return OrderManager(
        venue,
        journal,
        RiskEngine(permissive_config()),
        clock=clock,
        stream_settle=None,
        reconcile_interval=3600.0,
        unknown_grace=0.0,
    )


async def unknown_order(
    venue: RecordKeepingVenue, journal: Journal, clock: ManualClock
) -> None:
    oms = record_manager(venue, journal, clock)
    await oms.start()
    await venue.drain()
    venue.faults.append(Fault.LOSE_REQUEST)
    await oms.submit(order("SR1", price="9.50"))
    assert oms.order("SR1").status is S.UNKNOWN
    await oms.abort()
    venue.lost_rows = True


async def test_no_record_under_the_journalled_id_closes_as_never_sent(
    journal: Journal, clock: ManualClock
) -> None:
    venue = record_venue(clock)
    await unknown_order(venue, journal, clock)
    oms = record_manager(venue, journal, clock)
    await oms.start()
    try:
        state = oms.order("SR1")
        assert state.status is S.REJECTED and state.reject_code == "NOT_FOUND_AT_VENUE"
        assert state.send_record_id == "store-1"
    finally:
        await oms.abort()


async def test_no_record_under_a_different_id_stays_unknown_on_every_restart(
    journal: Journal, clock: ManualClock
) -> None:
    venue = record_venue(clock)
    await unknown_order(venue, journal, clock)
    venue.record_id = "store-2"
    for _ in range(3):
        oms = record_manager(venue, journal, clock)
        report = await oms.start()
        try:
            assert oms.order("SR1").status is S.UNKNOWN
            assert BreakKind.QUERY_FAILED in [b.kind for b in report.breaks]
            with pytest.raises(RiskRejected):
                await oms.submit(order(price="9.50"))
        finally:
            await oms.abort()


async def test_no_record_with_no_journalled_id_stays_unknown(
    journal: Journal, clock: ManualClock
) -> None:
    venue = record_venue(clock)
    venue.record_id = None
    await unknown_order(venue, journal, clock)
    venue.record_id = "store-1"
    oms = record_manager(venue, journal, clock)
    await oms.start()
    try:
        assert oms.order("SR1").status is S.UNKNOWN
    finally:
        await oms.abort()


async def lost_store_after(
    venue: RecordKeepingVenue, journal: Journal, clock: ManualClock, cid: str
) -> OrderManager:
    """Restart on a new, empty store with ``cid`` journalled under the old one."""
    venue.lost_rows = True
    venue.record_id = "store-2"
    restarted = record_manager(venue, journal, clock)
    report = await restarted.start()
    assert BreakKind.QUERY_FAILED in [b.kind for b in report.breaks]
    assert restarted.order(cid).status is S.UNKNOWN
    return restarted


async def test_a_working_order_on_a_lost_store_waits_for_the_operator(
    journal: Journal, clock: ManualClock
) -> None:
    """Nothing closes it by itself, and ``resolve_unknown`` is its route out."""
    venue = record_venue(clock)
    oms = record_manager(venue, journal, clock)
    await oms.start()
    await venue.drain()
    assert (await oms.submit(order("SR4", price="9.50"))).status is S.ACCEPTED
    await oms.abort()

    restarted = await lost_store_after(venue, journal, clock, "SR4")
    try:
        with pytest.raises(RiskRejected):
            await restarted.submit(order(price="9.50", symbol=OTHR))
        assert [o.client_order_id for o in await venue.open_orders()] == ["SR4"]
        for _ in range(2):
            await restarted.reconcile()
            assert restarted.order("SR4").status is S.UNKNOWN
        assert [o.client_order_id for o in await venue.open_orders()] == ["SR4"]

        venue.forget_order("SR4")  # the operator cancelled at the counterparty
        state = await restarted.resolve_unknown(
            "SR4", operator="ops", reason="cancelled and confirmed at the venue"
        )
        assert state.status is S.REJECTED
        assert state.reject_code == "RESOLVED_BY_OPERATOR"
        assert (await restarted.reconcile()).clean
        assert (await restarted.submit(order("SR5", price="9.50"))).status is S.ACCEPTED
    finally:
        await restarted.abort()


async def test_a_partly_filled_order_on_a_lost_store_closes_canceled_with_its_fills(
    journal: Journal, clock: ManualClock
) -> None:
    venue = record_venue(clock)
    oms = record_manager(venue, journal, clock)
    await oms.start()
    await venue.drain()
    await oms.submit(order("SR6", price="9.50"))
    await venue.drain()
    venue.trade_through(ACME, "9.50", "40")
    await oms.wait_for("SR6", lambda o: o.filled_quantity == 40, 5)
    await oms.abort()

    restarted = await lost_store_after(venue, journal, clock, "SR6")
    try:
        assert restarted.order("SR6").filled_quantity == 40
        venue.forget_order("SR6")
        state = await restarted.resolve_unknown(
            "SR6", operator="ops", reason="cancelled and confirmed at the venue"
        )
        assert state.status is S.CANCELED
        assert state.filled_quantity == 40
        assert [f.quantity for f in state.fills] == [D(40)]
        assert restarted.net_position(ACME) == 40
        assert (await restarted.reconcile()).clean
    finally:
        await restarted.abort()
    again = record_manager(venue, journal, clock)
    await again.start()
    try:
        assert again.order("SR6").status is S.CANCELED
        assert again.net_position(ACME) == 40
    finally:
        await again.abort()


async def test_a_venue_with_no_send_records_journals_no_id(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    state = await oms.submit(order("SR2", price="9.50"))
    assert state.send_record_id is None


# Operator resolution


async def stuck_order(
    journal: Journal, clock: ManualClock
) -> tuple[RecordKeepingVenue, OrderManager]:
    venue = record_venue(clock)
    await unknown_order(venue, journal, clock)
    venue.record_id = "store-2"
    oms = record_manager(venue, journal, clock)
    await oms.start()
    return venue, oms


async def test_an_operator_can_close_an_unknown_order_and_it_survives_restart(
    journal: Journal, clock: ManualClock
) -> None:
    venue, oms = await stuck_order(journal, clock)
    state = await oms.resolve_unknown("SR1", operator="ops", reason="broker confirmed")
    assert state.status is S.REJECTED
    assert state.reject_code == "RESOLVED_BY_OPERATOR"
    assert "ops" in str(state.reject_reason)
    assert "broker confirmed" in str(state.reject_reason)
    await oms.abort()
    restarted = record_manager(venue, journal, clock)
    await restarted.start()
    try:
        assert restarted.order("SR1").reject_code == "RESOLVED_BY_OPERATOR"
    finally:
        await restarted.abort()


@pytest.mark.parametrize(("reason", "operator"), [(" ", "ops"), ("why", " ")])
async def test_resolving_needs_a_name_and_a_reason(
    journal: Journal, clock: ManualClock, reason: str, operator: str
) -> None:
    _, oms = await stuck_order(journal, clock)
    try:
        with pytest.raises(ValidationError):
            await oms.resolve_unknown("SR1", operator=operator, reason=reason)
        assert oms.order("SR1").status is S.UNKNOWN
    finally:
        await oms.abort()


async def test_only_an_unknown_order_can_be_resolved(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("SR3", price="9.50"))
    await venue.drain()
    with pytest.raises(OrderStateError):
        await oms.resolve_unknown("SR3", operator="ops", reason="why")
    assert oms.order("SR3").status is S.ACCEPTED


async def test_resolving_refuses_a_broken_chain(
    journal: Journal, clock: ManualClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, oms = await stuck_order(journal, clock)
    try:
        monkeypatch.setattr(
            journal,
            "verify",
            lambda: VerifyResult(False, 1, "", 1, "altered"),
        )
        with pytest.raises(JournalError, match="failed verification"):
            await oms.resolve_unknown("SR1", operator="ops", reason="why")
        assert oms.order("SR1").status is S.UNKNOWN
    finally:
        await oms.abort()
