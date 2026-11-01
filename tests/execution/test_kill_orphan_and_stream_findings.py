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
from ismet.execution.risk import check_open_orders
from ismet.execution.state import apply_event
from ismet.models import Symbol
from ismet.models.orders import (
    Acknowledged,
    EventOrigin,
    Fill,
    Filled,
    OrderEvent,
    OrderSide,
    Replaced,
    ReplaceRequested,
    SendStarted,
    SubmitIntent,
    dump_order_event,
)
from ismet.models.orders import (
    OrderStatus as S,
)
from ismet.testing.simvenue import SIM_VENUE, SimVenue, sim_instrument
from ismet.transport.clock import ManualClock

from .conftest import ACME, START, Factory, order, permissive_config, until

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
    make_manager: Factory, venue: SimVenue
) -> None:
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


async def test_a_stream_that_delivers_is_healthy_without_waiting_out_the_settle(
    make_manager: Factory, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def delivers():  # type: ignore[no-untyped-def]
        yield filled("OUT-Z")
        await asyncio.Event().wait()

    monkeypatch.setattr(venue, "stream_executions", delivers)
    oms = make_manager(stream_settle=3600.0)
    await asyncio.wait_for(oms.start(), 5)
    try:
        assert oms.stream_healthy
    finally:
        await oms.abort()
