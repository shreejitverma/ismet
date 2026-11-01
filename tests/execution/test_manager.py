"""The order manager's behaviour against the simulated venue."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path

import pytest

from ismet.errors import (
    ConfigError,
    JournalError,
    KillSwitchEngaged,
    NotSupported,
    OrderStateError,
    RiskRejected,
    ValidationError,
)
from ismet.execution import (
    BreakKind,
    MemoryJournal,
    OrderManager,
    RecordKind,
    RiskEngine,
    RiskLimits,
    SqliteJournal,
    events_of,
    replay,
)
from ismet.models.orders import (
    Environment,
    OrderSide,
    OrderStatus,
    OrderType,
    ShortSaleMark,
    TimeInForce,
)
from ismet.testing.simvenue import Fault, SimVenue
from ismet.transport.clock import ManualClock

from .conftest import ACME, Factory, order, permissive_config

D = Decimal
S = OrderStatus


async def test_passive_order_is_acknowledged_then_filled(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    state = await oms.submit(order("A1", price="9.50"))
    assert state.status is S.ACCEPTED and state.venue_order_id
    venue.trade_through(ACME, "9.50", "40")
    state = await oms.wait_for("A1", lambda o: o.filled_quantity == 40, 2)
    assert state.status is S.PARTIALLY_FILLED and state.leaves_quantity == 60
    venue.trade_through(ACME, "9.50", "60")
    state = await oms.wait_for("A1", lambda o: o.is_terminal, 2)
    assert state.status is S.FILLED and state.average_price == D("9.50")
    assert oms.net_position(ACME) == 100
    assert venue.accepted["A1"] == 1


async def test_marketable_order_fills_at_the_touch(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("M1", price=None))
    state = await oms.wait_for("M1", lambda o: o.is_terminal, 2)
    assert state.status is S.FILLED and state.average_price == D("10.01")


async def test_journal_replay_reproduces_live_state_exactly(
    oms: OrderManager, venue: SimVenue, journal: SqliteJournal
) -> None:
    await venue.drain()
    await oms.submit(order("J1", price="9.50"))
    venue.trade_through(ACME, "9.50", "40")
    await oms.wait_for("J1", lambda o: o.filled_quantity == 40, 2)
    await oms.cancel("J1")
    live = await oms.wait_for("J1", lambda o: o.is_terminal, 2)
    assert replay(events_of(journal))["J1"] == live
    assert journal.verify().ok


async def test_submit_is_idempotent_for_the_same_request(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    request = order("I1", price="9.50")
    first = await oms.submit(request)
    again = await oms.submit(request)
    assert again.client_order_id == first.client_order_id
    assert venue.attempts["I1"] == 1
    with pytest.raises(ValidationError, match="different order"):
        await oms.submit(order("I1", price="9.51"))


@pytest.mark.parametrize(
    "fault", [Fault.LOSE_RESPONSE, Fault.SERVER_ERROR_ACCEPTED, Fault.ADAPTER_BUG]
)
async def test_lost_response_is_unknown_never_resent(
    oms: OrderManager, venue: SimVenue, fault: Fault
) -> None:
    await venue.drain()
    venue.faults.append(fault)
    state = await oms.submit(order("U1", price="9.50"))
    assert state.status is S.UNKNOWN
    assert venue.attempts["U1"] == 1


async def test_accepted_but_unacknowledged_order_is_found_by_reconcile(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.faults.append(Fault.LOSE_RESPONSE)
    venue.report_faults.drop_next = 1  # the stream's ack is lost too
    state = await oms.submit(order("U2", price="9.50"))
    assert state.status is S.UNKNOWN
    report = await oms.reconcile()
    assert "U2" in report.resolved
    assert oms.order("U2").status is S.ACCEPTED
    assert oms.order("U2").venue_order_id
    assert venue.accepted["U2"] == 1


async def test_request_that_never_arrived_is_closed_after_a_cancel_attempt(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.faults.append(Fault.LOSE_REQUEST)
    state = await oms.submit(order("U3", price="9.50"))
    assert state.status is S.UNKNOWN
    report = await oms.reconcile()
    closed = oms.order("U3")
    assert closed.status is S.REJECTED and closed.reject_code == "NOT_FOUND_AT_VENUE"
    assert "U3" in report.resolved and not report.breaks
    assert venue.accepted["U3"] == 0


async def test_not_found_waits_out_the_grace_period(
    make_manager: Factory, venue: SimVenue, clock: ManualClock
) -> None:
    oms = make_manager(unknown_grace=5.0)
    await oms.start()
    try:
        await venue.drain()
        venue.faults.append(Fault.LOSE_REQUEST)
        await oms.submit(order("U4", price="9.50"))
        report = await oms.reconcile()
        assert oms.order("U4").status is S.UNKNOWN and not report.complete
        clock.advance(6)
        await oms.reconcile()
        assert oms.order("U4").status is S.REJECTED
    finally:
        await oms.abort()


async def test_unknown_order_blocks_new_orders_in_that_symbol(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.faults.append(Fault.LOSE_REQUEST)
    await oms.submit(order("U5", price="9.50"))
    with pytest.raises(RiskRejected) as caught:
        await oms.submit(order("U6", price="9.40"))
    assert caught.value.check == "unknown_order"
    assert venue.attempts["U6"] == 0


async def test_query_failure_keeps_the_order_unknown(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.faults.append(Fault.LOSE_RESPONSE)
    venue.report_faults.drop_next = 1
    await oms.submit(order("U7", price="9.50"))
    venue.fail_queries = 10
    report = await oms.reconcile()
    assert oms.order("U7").status is S.UNKNOWN
    assert not report.complete and not report.clean
    assert {b.kind for b in report.breaks} == {BreakKind.QUERY_FAILED}
    assert not oms.kill_state.engaged  # a failed query is not itself a position break
    venue.fail_queries = 0
    await oms.reconcile()
    assert oms.order("U7").status is S.ACCEPTED


async def test_refused_connection_and_venue_reject_close_the_order(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.faults.extend([Fault.REFUSE_CONNECTION, Fault.REJECT])
    refused = await oms.submit(order("X1", price="9.50"))
    assert refused.status is S.REJECTED and refused.reject_code == "NOT_DELIVERED"
    rejected = await oms.submit(order("X2", price="9.50"))
    assert rejected.status is S.REJECTED
    assert rejected.reject_code == "INSUFFICIENT_FUNDS"


async def test_risk_rejection_is_journalled_and_nothing_is_sent(
    make_manager: Factory, venue: SimVenue, journal: SqliteJournal
) -> None:
    oms = make_manager(
        risk=RiskEngine(permissive_config(default=RiskLimits(max_order_quantity=D(10))))
    )
    await oms.start()
    try:
        with pytest.raises(RiskRejected):
            await oms.submit(order("R1", quantity="11"))
        assert venue.attempts["R1"] == 0
        assert "R1" not in oms.orders
        rejections = [r for r in journal.records() if r.kind is RecordKind.RISK_REJECT]
        assert [r.client_order_id for r in rejections] == ["R1"]
    finally:
        await oms.abort()


async def test_duplicate_and_out_of_order_reports_are_harmless(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("D1", price="9.50"))
    venue.report_faults.duplicate_next = 1
    venue.trade_through(ACME, "9.50", "40")
    await oms.wait_for("D1", lambda o: o.filled_quantity == 40, 2)
    venue.report_faults.hold_next = 2
    venue.trade_through(ACME, "9.50", "10")
    venue.trade_through(ACME, "9.50", "50")
    venue.release_held_reports()
    state = await oms.wait_for("D1", lambda o: o.filled_quantity == 100, 2)
    assert state.status is S.FILLED
    assert oms.net_position(ACME) == 100
    assert not state.anomalies


async def test_dropped_fill_report_is_recovered_by_reconcile(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("F1", price="9.50"))
    await venue.drain()
    venue.report_faults.drop_next = 1
    venue.trade_through(ACME, "9.50", "40")
    await venue.drain()
    assert oms.order("F1").filled_quantity == 0
    report = await oms.reconcile()
    assert oms.order("F1").filled_quantity == 40
    assert oms.order("F1").status is S.PARTIALLY_FILLED
    assert oms.net_position(ACME) == 40
    assert "F1" in report.resolved and report.clean


async def test_stream_disconnect_reconnects_and_recovers_missed_fills(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("S1", price="9.50"))
    await venue.drain()
    venue.disconnect_stream()
    while oms.stream_healthy:
        await asyncio.sleep(0)
    venue.trade_through(ACME, "9.50", "100")
    state = await oms.wait_for("S1", lambda o: o.is_terminal, 5)
    assert state.status is S.FILLED
    while not oms.stream_healthy:
        await asyncio.sleep(0)


async def test_cancel_is_idempotent_and_too_late_is_harmless(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("C1", price="9.50"))
    state = await oms.cancel("C1")
    assert state.status in (S.PENDING_CANCEL, S.CANCELED)
    state = await oms.wait_for("C1", lambda o: o.is_terminal, 2)
    assert state.status is S.CANCELED
    assert (await oms.cancel("C1")).status is S.CANCELED
    await oms.submit(order("C2", price="9.50"))
    await venue.drain()
    venue.report_faults.drop_next = 1  # the venue fills, and we have not heard
    venue.trade_through(ACME, "9.50", "100")
    await oms.cancel("C2")  # refused as too late; no error reaches the caller
    assert oms.order("C2").status is not S.PENDING_CANCEL
    await oms.reconcile()
    assert oms.order("C2").status is S.FILLED


async def test_lost_cancel_response_resolves_through_reconcile(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("C3", price="9.50"))
    await venue.drain()
    venue.fail_cancels = 1
    assert (await oms.cancel("C3")).status is S.UNKNOWN
    await oms.reconcile()
    assert oms.order("C3").status is S.ACCEPTED
    venue.lose_cancel_response = 1
    venue.report_faults.drop_next = 1
    assert (await oms.cancel("C3")).status is S.UNKNOWN
    await oms.reconcile()
    assert oms.order("C3").status is S.CANCELED


async def test_cancel_all_and_stop(oms: OrderManager, venue: SimVenue) -> None:
    await venue.drain()
    await oms.submit(order("K1", price="9.50"))
    await oms.submit(order("K2", price="9.40"))
    await oms.cancel_all(ACME)
    await oms.wait_for("K1", lambda o: o.is_terminal, 2)
    await oms.wait_for("K2", lambda o: o.is_terminal, 2)
    assert not oms.open_orders()
    await oms.submit(order("K3", price="9.30"))
    remaining = await oms.stop()
    assert [o.client_order_id for o in remaining] == ["K3"]
    assert venue.attempts["K3"] == 1


async def test_stop_can_cancel_open_orders(
    make_manager: Factory, venue: SimVenue
) -> None:
    oms = make_manager()
    await oms.start()
    await venue.drain()
    await oms.submit(order("K4", price="9.50"))
    await oms.stop(cancel_open=True)
    snapshot = await venue.order_status("K4", ACME)
    assert snapshot is not None and snapshot.status is S.CANCELED


async def test_amend_runs_risk_on_the_resulting_terms(
    make_manager: Factory, venue: SimVenue
) -> None:
    oms = make_manager(
        risk=RiskEngine(
            permissive_config(default=RiskLimits(max_order_quantity=D(200)))
        )
    )
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("A1", price="9.50"))
        await venue.drain()
        with pytest.raises(RiskRejected):
            await oms.amend("A1", quantity=D(500))
        assert oms.order("A1").quantity == 100
        await oms.amend("A1", quantity=D(150), limit_price=D("9.60"))
        state = await oms.wait_for("A1", lambda o: o.quantity == 150, 2)
        assert state.limit_price == D("9.60")
        assert state.status is S.ACCEPTED
        with pytest.raises(ValidationError, match="not above filled"):
            venue.trade_through(ACME, "9.60", "20")
            await oms.wait_for("A1", lambda o: o.filled_quantity == 20, 2)
            await oms.amend("A1", quantity=D(10))
        await oms.cancel("A1")
        await oms.wait_for("A1", lambda o: o.is_terminal, 2)
        with pytest.raises(OrderStateError, match="cannot amend"):
            await oms.amend("A1", quantity=D(100))
    finally:
        await oms.abort()


async def test_amend_refused_by_the_venue_keeps_old_terms(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("A2", price="9.50"))
    await venue.drain()
    venue.forget_order("A2")
    state = await oms.amend("A2", quantity=D(150))
    assert state.status is S.ACCEPTED and state.quantity == 100


async def test_amend_requires_venue_support(
    oms: OrderManager, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    await venue.drain()
    await oms.submit(order("A3", price="9.50"))
    rules = venue.trading_rules().model_copy(update={"supports_amend": False})
    monkeypatch.setattr(venue, "trading_rules", lambda: rules)
    with pytest.raises(NotSupported):
        await oms.amend("A3", quantity=D(150))


# Kill switch


async def test_kill_switch_blocks_new_orders_and_cancels_open_ones(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("KS1", price="9.50"))
    await venue.drain()
    await oms.engage_kill("test halt", operator="ops")
    with pytest.raises(KillSwitchEngaged, match="test halt"):
        await oms.submit(order("KS2", price="9.50"))
    assert venue.attempts["KS2"] == 0
    state = await oms.wait_for("KS1", lambda o: o.is_terminal, 2)
    assert state.status is S.CANCELED
    await oms.release_kill("all clear", operator="ops")
    assert (await oms.submit(order("KS3", price="9.50"))).status is S.ACCEPTED


async def test_kill_switch_engaged_by_another_process_stops_this_one(
    oms: OrderManager, venue: SimVenue, journal_path: Path
) -> None:
    await venue.drain()
    await oms.submit(order("KP1", price="9.50"))
    await venue.drain()
    other_process = SqliteJournal(journal_path)
    other_process.append(
        RecordKind.KILL_ENGAGED,
        '{"reason": "from the CLI", "operator": "ops", "cancel_open": true}',
    )
    other_process.close()
    with pytest.raises(KillSwitchEngaged, match="from the CLI"):
        await oms.submit(order("KP2", price="9.50"))
    state = await oms.wait_for("KP1", lambda o: o.is_terminal, 2)
    assert state.status is S.CANCELED


async def test_kill_switch_keeps_retrying_failed_cancels(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("KR1", price="9.50"))
    await venue.drain()
    venue.fail_cancels = 3
    await oms.engage_kill("halt")
    state = await oms.wait_for("KR1", lambda o: o.status is S.CANCELED, 5)
    assert state.is_terminal


async def test_kill_state_survives_restart(
    make_manager: Factory, venue: SimVenue
) -> None:
    first = make_manager()
    await first.start()
    await first.engage_kill("halted before restart", cancel_open=False)
    await first.abort()
    second = make_manager()
    await second.start()
    try:
        assert second.kill_state.engaged
        with pytest.raises(KillSwitchEngaged):
            await second.submit(order("KT1", price="9.50"))
    finally:
        await second.abort()


async def test_release_requires_an_operator(oms: OrderManager) -> None:
    await oms.engage_kill("x", cancel_open=False)
    with pytest.raises(ValidationError):
        await oms.release_kill("y", operator="")


# Reconciliation breaks


async def test_orphan_order_at_the_venue_halts_new_orders(
    make_manager: Factory, venue: SimVenue
) -> None:
    seen: list[BreakKind] = []
    oms = make_manager(on_break=lambda b: seen.append(b.kind))
    await oms.start()
    try:
        venue.add_external_order(order("EXT1", price="9.50"))
        report = await oms.reconcile()
        assert [b.kind for b in report.breaks] == [BreakKind.ORPHAN_ORDER]
        assert BreakKind.ORPHAN_ORDER in seen
        assert oms.kill_state.engaged and not oms.kill_state.cancel_open
        with pytest.raises(KillSwitchEngaged):
            await oms.submit(order("B1", price="9.50"))
    finally:
        await oms.abort()


async def test_order_arriving_after_it_was_declared_lost_is_an_orphan_break(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.faults.append(Fault.LOSE_REQUEST)
    await oms.submit(order("LATE1", price="9.50"))
    await oms.reconcile()
    assert oms.order("LATE1").status is S.REJECTED
    # The delayed request finally lands at the venue.
    venue.add_external_order(order("LATE1", price="9.50"))
    report = await oms.reconcile()
    assert [b.kind for b in report.breaks] == [BreakKind.ORPHAN_ORDER]
    assert "rejected" in report.breaks[0].detail
    assert oms.kill_state.engaged


async def test_position_break_is_reported_and_halts(
    oms: OrderManager, venue: SimVenue
) -> None:
    venue.add_external_fill(ACME, OrderSide.BUY, D(25))
    report = await oms.reconcile()
    assert [b.kind for b in report.breaks] == [BreakKind.POSITION_MISMATCH]
    assert "difference -25" in report.breaks[0].detail
    assert oms.kill_state.engaged


async def test_order_lost_by_the_venue_is_a_recorded_break(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("L1", price="9.50"))
    await venue.drain()
    venue.forget_order("L1")
    report = await oms.reconcile()
    assert oms.order("L1").status is S.CANCELED
    assert [b.kind for b in report.breaks] == [BreakKind.MISSING_AT_VENUE]
    assert not report.breaks[0].is_critical


async def test_halt_on_break_can_be_disabled(
    make_manager: Factory, venue: SimVenue
) -> None:
    oms = make_manager(halt_on_break=False)
    await oms.start()
    try:
        venue.add_external_fill(ACME, OrderSide.BUY, D(25))
        await oms.reconcile()
        assert not oms.kill_state.engaged
    finally:
        await oms.abort()


async def test_venue_report_for_an_unknown_order_is_a_break(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.add_external_order(order("EXT2", price="9.50"))
    venue.trade_through(ACME, "9.50", "1")
    await venue.drain()
    assert oms.kill_state.engaged
    assert "EXT2" not in oms.orders


async def test_overfill_from_the_venue_halts_trading(
    make_manager: Factory, venue: SimVenue, journal: SqliteJournal
) -> None:
    anomalies: list[str] = []
    oms = make_manager(on_anomaly=lambda _, message: anomalies.append(message))
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("O1", price="9.50"))
        await venue.drain()
        venue._orders["O1"].quantity = D(1000)  # the venue's books disagree with ours
        venue.trade_through(ACME, "9.50", "150")
        await oms.wait_for("O1", lambda o: o.filled_quantity == 150, 2)
        await venue.drain()
        assert any("overfill" in a for a in anomalies)
        assert oms.kill_state.engaged
        assert oms.net_position(ACME) == 150
        kinds = [r.kind for r in journal.records()]
        assert RecordKind.ANOMALY in kinds
    finally:
        await oms.abort()


async def test_bust_reverses_the_position(oms: OrderManager, venue: SimVenue) -> None:
    await venue.drain()
    await oms.submit(order("BU1", price="9.50"))
    await venue.drain()
    venue.trade_through(ACME, "9.50", "40")
    state = await oms.wait_for("BU1", lambda o: o.filled_quantity == 40, 2)
    venue.bust(state.fills[0].fill_id)
    await oms.wait_for("BU1", lambda o: o.filled_quantity == 0, 2)
    assert oms.net_position(ACME) == 0


# Failure of ismet's own dependencies


class FailingJournal(MemoryJournal):
    def __init__(self) -> None:
        super().__init__()
        self.fail_after: int | None = None

    def append(self, *args: object, **kw: object):  # type: ignore[no-untyped-def]
        if self.fail_after is not None and len(self._records) >= self.fail_after:
            raise JournalError("disk full")
        return super().append(*args, **kw)  # type: ignore[arg-type]


async def test_nothing_is_sent_if_the_intent_cannot_be_journalled(
    venue: SimVenue, clock: ManualClock
) -> None:
    journal = FailingJournal()
    oms = OrderManager(venue, journal, RiskEngine(permissive_config()), clock=clock)
    await oms.start()
    try:
        journal.fail_after = 0
        with pytest.raises(JournalError):
            await oms.submit(order("JF1", price="9.50"))
        assert venue.attempts["JF1"] == 0
    finally:
        await oms.abort()


async def test_nothing_is_sent_if_send_started_cannot_be_journalled(
    venue: SimVenue, clock: ManualClock
) -> None:
    journal = FailingJournal()
    oms = OrderManager(venue, journal, RiskEngine(permissive_config()), clock=clock)
    await oms.start()
    try:
        journal.fail_after = len(journal._records) + 1
        with pytest.raises(JournalError):
            await oms.submit(order("JF2", price="9.50"))
        assert venue.attempts["JF2"] == 0
        assert oms.order("JF2").status is S.PENDING_SUBMIT
    finally:
        await oms.abort()


async def test_journal_failure_on_a_venue_fill_keeps_the_fill_and_stops_trading(
    venue: SimVenue, clock: ManualClock
) -> None:
    journal = FailingJournal()
    oms = OrderManager(venue, journal, RiskEngine(permissive_config()), clock=clock)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("JF3", price="9.50"))
        await venue.drain()
        journal.fail_after = 0
        venue.trade_through(ACME, "9.50", "40")
        await oms.wait_for("JF3", lambda o: o.filled_quantity == 40, 2)
        assert oms.net_position(ACME) == 40
        with pytest.raises(KillSwitchEngaged, match="journal failure"):
            await oms.submit(order("JF4", price="9.40"))
        assert venue.attempts["JF4"] == 0
    finally:
        await oms.abort()


async def test_tampered_journal_prevents_startup(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    import sqlite3

    journal = SqliteJournal(journal_path)
    journal.append(RecordKind.ANOMALY, "{}")
    journal.close()
    conn = sqlite3.connect(journal_path)
    conn.execute("DROP TRIGGER journal_no_update")
    conn.execute("UPDATE journal SET payload = '{\"x\":1}'")
    conn.commit()
    conn.close()
    reopened = SqliteJournal(journal_path)
    oms = OrderManager(venue, reopened, RiskEngine(permissive_config()), clock=clock)
    with pytest.raises(JournalError, match="failed verification"):
        await oms.start()
    assert await oms.stop() == []
    reopened.close()


async def test_hook_failures_do_not_affect_order_handling(
    make_manager: Factory, venue: SimVenue
) -> None:
    def explode(*_: object) -> None:
        raise RuntimeError("hook bug")

    oms = make_manager(on_event=explode, on_break=explode, on_anomaly=explode)
    await oms.start()
    try:
        await venue.drain()
        state = await oms.submit(order("H1", price="9.50"))
        assert state.status is S.ACCEPTED
        venue.add_external_fill(ACME, OrderSide.BUY, D(1))
        report = await oms.reconcile()
        assert report.breaks
    finally:
        await oms.abort()


# Configuration


class LiveSim(SimVenue):
    environment = Environment.LIVE


class NoDeclaration(SimVenue):
    environment = Environment.LIVE
    name = "undeclared"


async def test_live_provider_requires_explicit_acknowledgement() -> None:
    journal = MemoryJournal()
    risk = RiskEngine(permissive_config())
    with pytest.raises(ConfigError, match="allow_live"):
        OrderManager(LiveSim(), journal, risk)
    OrderManager(LiveSim(), journal, risk, allow_live=True)


async def test_provider_without_trading_cannot_be_used() -> None:
    from ismet.providers.mock import MockProvider

    with pytest.raises(NotSupported):
        OrderManager(MockProvider(), MemoryJournal(), RiskEngine(permissive_config()))


async def test_calls_before_start_and_double_start_are_refused(
    make_manager: Factory,
) -> None:
    oms = make_manager()
    with pytest.raises(ConfigError, match="not started"):
        await oms.submit(order("Z1", price="9.50"))
    await oms.start()
    try:
        with pytest.raises(ConfigError, match="already started"):
            await oms.start()
        with pytest.raises(ValidationError, match="unknown order"):
            oms.order("nope")
    finally:
        await oms.abort()


async def test_short_sale_rejected_without_known_position(
    venue: SimVenue, clock: ManualClock
) -> None:
    class NoAccount(SimVenue):
        positions = None  # type: ignore[assignment]

    bare = NoAccount(clock=clock, instruments=[await venue.instrument(ACME)])
    bare.set_quote(ACME, "9.99", "10.01")
    oms = OrderManager(
        bare, MemoryJournal(), RiskEngine(permissive_config()), clock=clock
    )
    assert oms.net_position(ACME) is None


async def test_time_in_force_behaviour_through_the_oms(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    ioc = await oms.submit(order("T1", price="9.50", time_in_force=TimeInForce.IOC))
    assert ioc.client_order_id == "T1"
    state = await oms.wait_for("T1", lambda o: o.is_terminal, 2)
    assert state.status is S.CANCELED
    venue.set_quote(ACME, "9.99", "10.01", ask_size="50")
    await oms.submit(order("T2", price="10.01", time_in_force=TimeInForce.FOK))
    state = await oms.wait_for("T2", lambda o: o.is_terminal, 2)
    assert state.status is S.CANCELED and state.filled_quantity == 0
    await oms.submit(order("T3", price="9.50"))
    await venue.drain()
    venue.expire_day_orders()
    state = await oms.wait_for("T3", lambda o: o.is_terminal, 2)
    assert state.status is S.EXPIRED


async def test_stop_order_elects_and_fills(oms: OrderManager, venue: SimVenue) -> None:
    await venue.drain()
    await oms.submit(
        order("ST1", price=None, order_type=OrderType.STOP, stop_price="10.50")
    )
    await venue.drain()
    assert oms.order("ST1").status is S.ACCEPTED
    venue.set_quote(ACME, "10.49", "10.51")
    state = await oms.wait_for("ST1", lambda o: o.filled_quantity > 0, 2)
    assert state.average_price == D("10.51")


async def test_wait_for_times_out(oms: OrderManager, venue: SimVenue) -> None:
    await venue.drain()
    await oms.submit(order("W1", price="9.50"))
    with pytest.raises(asyncio.TimeoutError):
        await oms.wait_for("W1", lambda o: o.is_terminal, 0.01)


async def test_cancelled_caller_leaves_the_order_unknown_not_lost(
    oms: OrderManager, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    await venue.drain()
    started = asyncio.Event()

    async def slow_place(request):  # type: ignore[no-untyped-def]
        started.set()
        await asyncio.sleep(60)

    monkeypatch.setattr(venue, "place_order", slow_place)
    task = asyncio.create_task(oms.submit(order("CC1", price="9.50")))
    await started.wait()
    report = await oms.reconcile()
    assert "CC1" not in report.resolved  # in flight, so reconcile must leave it alone
    assert oms.order("CC1").status is S.SUBMITTING
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert oms.order("CC1").status is S.UNKNOWN


async def test_position_index_agrees_with_a_full_recount_after_any_history(
    oms: OrderManager, venue: SimVenue, journal: SqliteJournal
) -> None:
    await venue.drain()
    await oms.submit(order("P1", price="9.50"))
    await oms.submit(
        order(
            "P2",
            side=OrderSide.SELL,
            price="10.50",
            quantity="30",
            short_sale=ShortSaleMark.SHORT_EXEMPT,
        )
    )
    await venue.drain()
    venue.trade_through(ACME, "9.50", "60")
    venue.trade_through(ACME, "10.50", "30")
    filled = await oms.wait_for("P1", lambda o: o.filled_quantity == 60, 2)
    await oms.wait_for("P2", lambda o: o.is_terminal, 2)
    venue.bust(filled.fills[0].fill_id)
    await oms.wait_for("P1", lambda o: o.filled_quantity == 0, 2)
    await venue.drain()
    recount = sum(
        (D(o.request.side.sign) * o.filled_quantity for o in oms.orders.values()),
        D(0),
    )
    assert oms.net_position(ACME) == recount == D(-30)
    assert {o.client_order_id for o in oms.open_orders()} == {
        o.client_order_id for o in oms.orders.values() if o.is_open
    }
    assert oms.open_orders(ACME) == oms.open_orders()


async def test_kill_switch_stays_engaged_when_the_journal_cannot_record_it(
    venue: SimVenue, clock: ManualClock
) -> None:
    journal = FailingJournal()
    oms = OrderManager(
        venue,
        journal,
        RiskEngine(permissive_config()),
        clock=clock,
        kill_poll_interval=0.001,
    )
    await oms.start()
    try:
        journal.fail_after = 0  # reads still work and report "not engaged"
        await oms.engage_kill("disk is failing", cancel_open=False)
        await asyncio.sleep(0.02)  # let the poll read the journal many times
        assert oms.kill_state.engaged
        with pytest.raises(KillSwitchEngaged):
            await oms.submit(order("KJ1", price="9.50"))
        assert venue.attempts["KJ1"] == 0
    finally:
        await oms.abort()


async def test_a_stale_journal_read_cannot_undo_a_local_engage(
    oms: OrderManager, journal: SqliteJournal
) -> None:
    before = journal.kill_state()
    await oms.engage_kill("halt", cancel_open=False)
    oms._merge_kill(before)
    assert oms.kill_state.engaged


async def test_a_newer_release_from_another_process_is_adopted(
    oms: OrderManager, journal_path: Path
) -> None:
    await oms.engage_kill("halt", cancel_open=False)
    other = SqliteJournal(journal_path)
    other.append(RecordKind.KILL_RELEASED, '{"reason": "ok", "operator": "ops"}')
    other.close()
    await oms.submit(order("KR2", price="9.50"))
    assert not oms.kill_state.engaged
