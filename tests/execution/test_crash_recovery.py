"""Kill the process at every step of the order path, restart, and check the books.

Each scenario runs a real SQLite journal on disk. ``SimulatedCrash`` derives from
``BaseException``, so nothing in the order manager can catch it and write a tidy
final record, which is what a power cut does. The manager is then discarded and a new
one built on the same file, as a restarted process would be.

The properties under test are the product's core safety claims:

- an order is never sent twice, however the process died;
- every fill the venue made ends up in the books;
- once recovery finishes, the books match the venue.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path

import pytest

from ismet.execution import (
    OrderManager,
    RecordKind,
    RiskEngine,
    SqliteJournal,
)
from ismet.models.orders import OrderSide, OrderStatus
from ismet.testing.simvenue import Fault, SimVenue
from ismet.transport.clock import ManualClock

from .conftest import ACME, order, permissive_config

S = OrderStatus
D = Decimal
CHECKPOINTS = ("after_intent", "after_send_started", "after_venue_accept")


class SimulatedCrash(BaseException):
    """The process died here."""


def crash_at(name: str) -> Callable[[str, str], None]:
    def checkpoint(step: str, _: str) -> None:
        if step == name:
            raise SimulatedCrash(step)

    return checkpoint


def build(
    path: Path,
    venue: SimVenue,
    clock: ManualClock,
    **kw: object,
) -> tuple[OrderManager, SqliteJournal]:
    journal = SqliteJournal(path)
    manager = OrderManager(
        venue,
        journal,
        RiskEngine(permissive_config()),
        clock=clock,
        unknown_grace=0.0,
        reconcile_interval=3600.0,
        kill_poll_interval=0.01,
        stream_settle=None,
        **kw,  # type: ignore[arg-type]
    )
    return manager, journal


async def crash_during_submit(
    path: Path, venue: SimVenue, clock: ManualClock, step: str, price: str = "9.50"
) -> None:
    manager, journal = build(path, venue, clock, checkpoint=crash_at(step))
    await manager.start()
    await venue.drain()
    try:
        with pytest.raises(SimulatedCrash):
            await manager.submit(order("CR1", price=price))
    finally:
        await manager.abort()
        journal.close()


async def recover(
    path: Path, venue: SimVenue, clock: ManualClock
) -> tuple[OrderManager, SqliteJournal]:
    manager, journal = build(path, venue, clock)
    await manager.start()
    await venue.drain()
    await manager.reconcile()
    return manager, journal


async def test_crash_before_the_send_started_never_reaches_the_venue(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    await crash_during_submit(journal_path, venue, clock, "after_intent")
    assert venue.attempts["CR1"] == 0
    manager, journal = await recover(journal_path, venue, clock)
    try:
        state = manager.order("CR1")
        assert state.status is S.REJECTED and state.reject_code == "NEVER_SENT"
        assert venue.attempts["CR1"] == 0
    finally:
        await manager.abort()
        journal.close()


async def test_crash_after_send_started_but_before_the_call_is_resolved_by_query(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    await crash_during_submit(journal_path, venue, clock, "after_send_started")
    assert venue.attempts["CR1"] == 0
    manager, journal = await recover(journal_path, venue, clock)
    try:
        state = manager.order("CR1")
        assert state.status is S.REJECTED
        assert state.reject_code == "NOT_FOUND_AT_VENUE"
        assert venue.attempts["CR1"] == 0
    finally:
        await manager.abort()
        journal.close()


async def test_crash_after_the_venue_accepted_adopts_the_live_order(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    await crash_during_submit(journal_path, venue, clock, "after_venue_accept")
    assert venue.accepted["CR1"] == 1
    manager, journal = await recover(journal_path, venue, clock)
    try:
        state = manager.order("CR1")
        assert state.status is S.ACCEPTED and state.venue_order_id
        assert venue.attempts["CR1"] == 1
        venue.trade_through(ACME, "9.50", "100")
        done = await manager.wait_for("CR1", lambda o: o.is_terminal, 2)
        assert done.status is S.FILLED
        assert manager.net_position(ACME) == 100
    finally:
        await manager.abort()
        journal.close()


async def test_fills_made_while_the_process_was_down_are_recovered(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    await crash_during_submit(journal_path, venue, clock, "after_venue_accept")
    venue.trade_through(ACME, "9.50", "30")
    venue.trade_through(ACME, "9.50", "20")
    manager, journal = await recover(journal_path, venue, clock)
    try:
        state = manager.order("CR1")
        assert state.filled_quantity == 50
        assert state.status is S.PARTIALLY_FILLED
        assert len(state.fills) == 2
        assert manager.net_position(ACME) == 50
        assert not manager.kill_state.engaged
    finally:
        await manager.abort()
        journal.close()


async def test_order_filled_completely_while_down_ends_filled(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    await crash_during_submit(journal_path, venue, clock, "after_venue_accept")
    venue.trade_through(ACME, "9.50", "100")
    manager, journal = await recover(journal_path, venue, clock)
    try:
        assert manager.order("CR1").status is S.FILLED
        assert manager.net_position(ACME) == 100
    finally:
        await manager.abort()
        journal.close()


async def test_order_cancelled_while_down_ends_cancelled(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    await crash_during_submit(journal_path, venue, clock, "after_venue_accept")
    await venue.cancel_order("CR1", ACME)
    manager, journal = await recover(journal_path, venue, clock)
    try:
        assert manager.order("CR1").status is S.CANCELED
    finally:
        await manager.abort()
        journal.close()


@pytest.mark.parametrize("step", CHECKPOINTS)
@pytest.mark.parametrize(
    "fault",
    [None, Fault.LOSE_RESPONSE, Fault.LOSE_REQUEST, Fault.SERVER_ERROR_ACCEPTED],
)
@pytest.mark.parametrize("fills_while_down", ["0", "40", "100"])
async def test_books_match_the_venue_after_any_crash(
    journal_path: Path,
    venue: SimVenue,
    clock: ManualClock,
    step: str,
    fault: Fault | None,
    fills_while_down: str,
) -> None:
    """The full matrix: where it died, what the network did, what the market did."""
    if fault is not None:
        venue.faults.append(fault)
    manager, journal = build(journal_path, venue, clock, checkpoint=crash_at(step))
    await manager.start()
    await venue.drain()
    try:
        # A fault can end the send before the checkpoint is reached.
        with contextlib.suppress(SimulatedCrash):
            await manager.submit(order("CR1", price="9.50"))
    finally:
        await manager.abort()
        journal.close()
    venue.trade_through(ACME, "9.50", fills_while_down)

    manager, journal = await recover(journal_path, venue, clock)
    try:
        state = manager.order("CR1")
        snapshot = await venue.order_status("CR1", ACME)
        # A fault is only consumed by the call to the venue, which happens only after
        # the send-started record.
        sent = 1 if step == "after_venue_accept" else 0
        assert venue.attempts["CR1"] == sent, "wrong number of sends"
        assert venue.accepted["CR1"] <= 1
        assert state.status not in (S.UNKNOWN, S.SUBMITTING)
        if snapshot is None:
            assert state.status is S.REJECTED
            assert state.filled_quantity == 0
        else:
            assert state.status is snapshot.status
            assert state.filled_quantity == snapshot.filled_quantity
            assert state.filled_quantity == D(fills_while_down)
        positions = {p.symbol: p.quantity for p in await venue.positions()}
        assert manager.net_position(ACME) == positions.get(ACME, D(0))
        report = await manager.reconcile()
        assert report.clean
        assert not manager.kill_state.engaged
        assert journal.verify().ok
    finally:
        await manager.abort()
        journal.close()


async def test_repeated_crashes_during_recovery_converge(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    await crash_during_submit(journal_path, venue, clock, "after_venue_accept")
    venue.trade_through(ACME, "9.50", "25")
    for _ in range(3):
        manager, journal = build(journal_path, venue, clock)
        await manager.start()
        await manager.abort()
        journal.close()
    manager, journal = await recover(journal_path, venue, clock)
    try:
        assert manager.order("CR1").filled_quantity == 25
        assert venue.attempts["CR1"] == 1
        assert journal.verify().ok
    finally:
        await manager.abort()
        journal.close()


async def test_a_retried_submit_after_restart_does_not_resend(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    """A strategy restarted after a crash resubmits the same deterministic id."""
    await crash_during_submit(journal_path, venue, clock, "after_venue_accept")
    manager, journal = await recover(journal_path, venue, clock)
    try:
        again = await manager.submit(order("CR1", price="9.50"))
        assert again.client_order_id == "CR1"
        assert venue.attempts["CR1"] == 1
    finally:
        await manager.abort()
        journal.close()


async def test_fills_while_down_recovered_when_time_has_passed(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    await crash_during_submit(journal_path, venue, clock, "after_venue_accept")
    clock.advance(3600)
    venue.trade_through(ACME, "9.50", "40")
    clock.advance(3600)
    manager, journal = await recover(journal_path, venue, clock)
    try:
        assert manager.order("CR1").filled_quantity == 40
        assert not manager.kill_state.engaged
    finally:
        await manager.abort()
        journal.close()


async def test_truncating_the_kill_record_is_refused_on_restart(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    import sqlite3

    from ismet.errors import JournalError

    manager, journal = build(journal_path, venue, clock)
    await manager.start()
    await manager.engage_kill("halt", operator="ops", cancel_open=False)
    await manager.abort()
    seq = journal.kill_state().seq
    journal.close()
    conn = sqlite3.connect(journal_path)
    conn.execute("DROP TRIGGER journal_no_delete")
    conn.execute("DELETE FROM journal WHERE seq >= ?", (seq,))
    conn.commit()
    conn.close()
    manager, journal = build(journal_path, venue, clock)
    with pytest.raises(JournalError, match="truncated"):
        await manager.start()
    journal.close()


async def test_position_baseline_survives_restart_and_is_not_retaken(
    journal_path: Path, clock: ManualClock
) -> None:
    from .conftest import sim_instrument

    venue = SimVenue(
        clock=clock,
        instruments=[sim_instrument("ACME")],
        initial_positions={ACME: D(500)},
    )
    venue.set_quote(ACME, "9.99", "10.01")
    manager, journal = await recover(journal_path, venue, clock)
    assert manager.net_position(ACME) == 500
    await manager.submit(order("B1", side=OrderSide.SELL, price="9.99", quantity="100"))
    await manager.wait_for("B1", lambda o: o.is_terminal, 2)
    assert manager.net_position(ACME) == 400
    await manager.abort()
    journal.close()

    manager, journal = await recover(journal_path, venue, clock)
    try:
        assert manager.net_position(ACME) == 400
        baselines = [
            r for r in journal.records() if r.kind is RecordKind.POSITION_BASELINE
        ]
        assert len(baselines) == 1
    finally:
        await manager.abort()
        journal.close()


async def test_kill_engaged_between_intent_and_send_stops_the_send(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    holder: list[OrderManager] = []

    def engage(step: str, _: str) -> None:
        if step == "after_intent":
            holder[0]._kill = holder[0]._kill.__class__(True, "halted mid-flight")

    manager, journal = build(journal_path, venue, clock, checkpoint=engage)
    holder.append(manager)
    await manager.start()
    try:
        state = await manager.submit(order("KM1", price="9.50"))
        assert state.status is S.REJECTED and state.reject_code == "KILL_SWITCH"
        assert venue.attempts["KM1"] == 0
    finally:
        await manager.abort()
        journal.close()
