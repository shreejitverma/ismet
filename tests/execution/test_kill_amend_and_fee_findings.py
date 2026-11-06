"""Kill switch timing, amend after a kill, fee corrections and replay findings.

Each test reproduces the finding's sequence against a real SQLite journal where the
finding depends on one. A ``HoldingJournal`` lets a test pause one write or read on the
journal thread, so the order of events is fixed by gates rather than by timing.
"""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Callable, Coroutine
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from ismet.errors import JournalError, KillSwitchEngaged
from ismet.execution import (
    MemoryJournal,
    OrderManager,
    RecordKind,
    RiskEngine,
    SqliteJournal,
    events_of,
    new_order,
)
from ismet.execution.journal import JournalRecord, KillState
from ismet.execution.state import apply_event
from ismet.models.decimals import add
from ismet.models.orders import (
    Acknowledged,
    EventOrigin,
    Fill,
    Filled,
    OrderSide,
    OrderStatus,
    OutcomeUnknown,
    ReplaceRejected,
    SendStarted,
    SubmitIntent,
)
from ismet.refdata.codes import accepting_stored_codes
from ismet.testing.simvenue import SimVenue
from ismet.transport.clock import ManualClock

from .conftest import ACME, START, Factory, order, permissive_config, until

D = Decimal
S = OrderStatus
DESK_KILL = {"reason": "desk", "operator": "ops", "cancel_open": True}


class HoldingJournal(SqliteJournal):
    """A real journal whose next write of one kind, or whose reads, can be held back.

    ``hold_payload`` names a record to hold by a substring of its payload; ``holding``
    is set once that write has begun and is blocked, and ``release`` lets it through.
    ``read_gate``, once set, holds every ``kill_state`` read until it is set, and
    ``reads_holding`` says one is waiting. All are threading events because the
    journal runs off the event loop. Waits are bounded so a mistake fails, not hangs.
    """

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.hold_payload: str | None = None
        self.holding = threading.Event()
        self.release = threading.Event()
        self.read_gate: threading.Event | None = None
        self.reads_holding = threading.Event()

    def append(
        self,
        kind: RecordKind,
        payload: str,
        *,
        client_order_id: str | None = None,
        ts: datetime | None = None,
        unique_intent: bool = False,
    ) -> JournalRecord:
        if self.hold_payload is not None and self.hold_payload in payload:
            self.hold_payload = None
            self.holding.set()
            self.release.wait(5)
        return super().append(
            kind,
            payload,
            client_order_id=client_order_id,
            ts=ts,
            unique_intent=unique_intent,
        )

    def kill_state(self) -> KillState:
        gate = self.read_gate
        if gate is not None:
            self.reads_holding.set()
            gate.wait(5)
        return super().kill_state()


def manager_over(
    venue: SimVenue,
    journal: MemoryJournal | SqliteJournal,
    clock: ManualClock,
    **kw: object,
) -> OrderManager:
    options: dict[str, object] = {
        "clock": clock,
        "unknown_grace": 0.0,
        "reconcile_interval": 3600.0,
        "kill_poll_interval": 3600.0,  # no poll may rescue the sequence under test
        "stream_settle": None,
    }
    options.update(kw)
    return OrderManager(
        venue,
        journal,
        RiskEngine(permissive_config()),
        **options,  # type: ignore[arg-type]
    )


# kill-engage-reads-journal-first


async def test_engage_kill_stops_a_send_whose_record_is_being_written(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    """The kill arrives while SendStarted is on disk's time; the send must not go out.

    The journal read the kill does queues behind that write, so memory must be engaged
    before the read, or the second refusal check still sees a clear switch.
    """
    journal = HoldingJournal(journal_path)
    reads_may_finish = threading.Event()

    def at(step: str, _: str) -> None:
        if step == "after_send_started":
            reads_may_finish.set()

    oms = manager_over(venue, journal, clock, checkpoint=at)
    await oms.start()
    try:
        await venue.drain()
        journal.hold_payload = "send_started"
        submit = asyncio.create_task(oms.submit(order("KH1", price="9.50")))
        await until(journal.holding.is_set)
        journal.read_gate = reads_may_finish
        engage = asyncio.create_task(
            oms.engage_kill("halt", operator="ops", cancel_open=False)
        )
        await asyncio.sleep(0)  # engage_kill runs up to its first await
        assert oms.kill_state.engaged, "memory must be engaged before any await"
        journal.release.set()
        state = await submit
        await engage
        assert state.status is S.REJECTED and state.reject_code == "KILL_SWITCH"
        assert venue.attempts["KH1"] == 0
        assert journal.kill_state().engaged
    finally:
        await oms.abort()
        journal.close()


async def engage(oms: OrderManager) -> None:
    await oms.engage_kill("halt", operator="ops", cancel_open=False)


async def halt(oms: OrderManager) -> None:
    await oms._halt("halt")


@pytest.mark.parametrize("stop", [engage, halt])
async def test_a_kill_cancelled_during_its_journal_read_still_halts_and_records(
    journal_path: Path,
    venue: SimVenue,
    clock: ManualClock,
    stop: Callable[[OrderManager], Coroutine[Any, Any, None]],
) -> None:
    journal = HoldingJournal(journal_path)
    oms = manager_over(venue, journal, clock)
    await oms.start()
    try:
        await venue.drain()
        gate = threading.Event()
        journal.read_gate = gate
        task = asyncio.create_task(stop(oms))
        await until(journal.reads_holding.is_set)
        assert oms.kill_state.engaged, "memory must be engaged before the read"
        task.cancel()
        gate.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert oms.kill_state.engaged
        durable = journal.kill_state()
        assert durable.engaged and durable.reason == "halt"
        with pytest.raises(KillSwitchEngaged):
            await oms.submit(order("KH2", price="9.50"))
    finally:
        await oms.abort()
        journal.close()


async def test_a_second_engage_after_a_cancelled_one_is_still_recorded(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    """A flip memory made before its read is not the same as a recorded kill."""
    journal = HoldingJournal(journal_path)
    oms = manager_over(venue, journal, clock)
    await oms.start()
    try:
        await venue.drain()
        gate = threading.Event()
        journal.read_gate = gate
        task = asyncio.create_task(
            oms.engage_kill("first", operator="ops", cancel_open=False)
        )
        await until(journal.reads_holding.is_set)
        task.cancel()
        gate.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await oms.engage_kill("second", operator="ops", cancel_open=False)
        kills = [r for r in journal.records() if r.kind is RecordKind.KILL_ENGAGED]
        assert [json.loads(r.payload)["reason"] for r in kills] == ["first"]
        assert oms.kill_state.engaged and oms.kill_state.reason == "first"
    finally:
        await oms.abort()
        journal.close()


async def test_a_halt_with_stale_memory_adopts_a_wider_kill_from_elsewhere(
    make_manager: Factory, venue: SimVenue, journal: SqliteJournal
) -> None:
    """Memory says engaged without cancels; another process released and re-engaged
    with cancels since. The halt must adopt that and must not record anything narrower.
    """
    oms = make_manager(kill_poll_interval=3600)
    await oms.start()
    try:
        await venue.drain()
        await oms.engage_kill("first", operator="ops", cancel_open=False)
        journal.append(
            RecordKind.KILL_RELEASED, json.dumps({"reason": "clear", "operator": "ops"})
        )
        journal.append(RecordKind.KILL_ENGAGED, json.dumps(DESK_KILL))
        before = len(list(journal.records()))
        await oms._halt("overfill")
        assert oms.kill_state.engaged and oms.kill_state.cancel_open
        assert oms.kill_state.reason == "desk"
        assert len(list(journal.records())) == before
        durable = journal.kill_state()
        assert durable.engaged and durable.cancel_open and durable.reason == "desk"
    finally:
        await oms.abort()


async def test_a_halt_racing_a_wider_engage_ends_up_cancelling(
    make_manager: Factory, venue: SimVenue, journal: SqliteJournal
) -> None:
    """Another process engages with cancels after the halt read the journal and before
    it recorded, so the halt's narrower record lands last and memory holds it.
    """
    oms = make_manager(kill_poll_interval=3600)
    await oms.start()
    try:
        await venue.drain()
        real_read = journal.kill_state
        raced = False

        def read_then_lose_the_race() -> KillState:
            nonlocal raced
            state = real_read()
            if not raced:
                raced = True
                journal.append(RecordKind.KILL_ENGAGED, json.dumps(DESK_KILL))
            return state

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(journal, "kill_state", read_then_lose_the_race)
            await oms._halt("overfill")
        assert raced
        assert journal.kill_state().cancel_open
        await oms._merge_kill(journal.kill_state())
        assert oms.kill_state.engaged and oms.kill_state.cancel_open
    finally:
        await oms.abort()


@pytest.mark.parametrize(
    "journal_up", [True, False], ids=["journal-up", "journal-down"]
)
async def test_a_halt_cannot_narrow_a_wider_engage_decided_while_it_read(
    make_manager: Factory, venue: SimVenue, journal: SqliteJournal, journal_up: bool
) -> None:
    """In one process: a halt reads the journal, an operator's wider engage decides
    before the halt resumes, and the halt's stale read must not narrow memory or
    replace the operator's record. With the journal down, the operator's record is the
    one left to retry, so the cancel request survives the outage.
    """
    oms = make_manager(kill_poll_interval=3600)
    await oms.start()
    try:
        await venue.drain()
        real_read = oms._durable_kill
        halt_has_read, halt_may_go = asyncio.Event(), asyncio.Event()
        reads = 0

        async def read_then_stall_once(memory: KillState) -> KillState:
            nonlocal reads
            reads += 1
            found = await real_read(memory)
            if reads == 1:
                halt_has_read.set()
                await halt_may_go.wait()
            return found

        def journal_down(*_: object, **__: object) -> JournalRecord:
            raise OSError("disk gone")

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(oms, "_durable_kill", read_then_stall_once)
            if not journal_up:
                patch.setattr(journal, "append", journal_down)
            halt = asyncio.ensure_future(oms._halt("overfill"))
            await halt_has_read.wait()
            await oms.engage_kill("desk pull", operator="desk", cancel_open=True)
            halt_may_go.set()
            await halt
            assert oms.kill_state.engaged and oms.kill_state.cancel_open
            if not journal_up:
                pending = oms._kill_unrecorded
                assert pending is not None and pending["cancel_open"] is True
        if journal_up:
            assert journal.kill_state().cancel_open
    finally:
        await oms.abort()


async def test_an_engage_in_flight_is_written_once_and_cannot_undo_a_release(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    """The kill loop retries an unrecorded engage. While the engage's own write is in
    flight it must not write the engage again, or that copy can land after an
    operator's release and halt the next start."""
    journal = HoldingJournal(journal_path)
    oms = manager_over(venue, journal, clock)
    await oms.start()
    try:
        await venue.drain()
        journal.hold_payload = '"desk"'
        engage = asyncio.create_task(
            oms.engage_kill("desk", operator="ops", cancel_open=False)
        )
        await until(journal.holding.is_set)
        retry = asyncio.create_task(oms._record_kill())
        await asyncio.sleep(0.05)
        journal.release.set()
        await engage
        await retry
        await oms.release_kill("checked", operator="ops")
        kinds = [r.kind for r in journal.records()]
        assert kinds.count(RecordKind.KILL_ENGAGED) == 1
        assert not journal.kill_state().engaged
    finally:
        journal.release.set()
        await oms.abort()
        journal.close()


# amend-sent-after-kill


async def test_an_amend_overtaken_by_a_kill_while_its_record_is_written_is_refused(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    journal = HoldingJournal(journal_path)
    oms = manager_over(venue, journal, clock)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("KA1", price="9.50"))
        await venue.drain()
        journal.hold_payload = "replace_requested"
        amend = asyncio.create_task(oms.amend("KA1", quantity=D(5000)))
        await until(journal.holding.is_set)
        engage = asyncio.create_task(
            oms.engage_kill("halt", operator="ops", cancel_open=False)
        )
        await asyncio.sleep(0)
        journal.release.set()
        state = await amend
        await engage
        snapshot = await venue.order_status("KA1", ACME)
        assert snapshot is not None and snapshot.quantity == 100
        assert state.status is S.ACCEPTED and state.quantity == 100
        events = [e for e in events_of(journal) if e.client_order_id == "KA1"]
        assert isinstance(events[-1], ReplaceRejected)
        assert events[-1].code == "KILL_SWITCH"
    finally:
        await oms.abort()
        journal.close()


class EngagedElsewhereDuringAppend(SqliteJournal):
    """A real journal on which another process engages as one record is written."""

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.other = SqliteJournal(path)
        self.engage_after: str | None = None

    def append(
        self,
        kind: RecordKind,
        payload: str,
        *,
        client_order_id: str | None = None,
        ts: datetime | None = None,
        unique_intent: bool = False,
    ) -> JournalRecord:
        record = super().append(
            kind,
            payload,
            client_order_id=client_order_id,
            ts=ts,
            unique_intent=unique_intent,
        )
        if self.engage_after is not None and self.engage_after in payload:
            self.engage_after = None
            self.other.append(RecordKind.KILL_ENGAGED, json.dumps(DESK_KILL))
        return record

    def close(self) -> None:
        self.other.close()
        super().close()


async def test_an_amend_overtaken_by_a_kill_from_another_process_is_refused(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    """``ismet kill engage`` commits while ReplaceRequested is being written."""
    journal = EngagedElsewhereDuringAppend(journal_path)
    oms = manager_over(venue, journal, clock)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("KA2", price="9.50"))
        await venue.drain()
        journal.engage_after = "replace_requested"
        state = await oms.amend("KA2", quantity=D(5000))
        assert journal.engage_after is None
        snapshot = await venue.order_status("KA2", ACME)
        assert snapshot is not None and snapshot.quantity == 100
        assert state.status is S.ACCEPTED and state.quantity == 100
        events = [e for e in events_of(journal) if e.client_order_id == "KA2"]
        assert isinstance(events[-1], ReplaceRejected)
        assert events[-1].code == "KILL_SWITCH"
        assert oms.kill_state.engaged and oms.kill_state.reason == "desk"
    finally:
        await oms.abort()
        journal.close()


class ReadFailingAfterAppend(MemoryJournal):
    fail_reads_after: str | None = None
    fail_reads = False

    def append(self, kind: RecordKind, payload: str, **kw: Any) -> JournalRecord:
        record = super().append(kind, payload, **kw)
        if self.fail_reads_after is not None and self.fail_reads_after in payload:
            self.fail_reads = True
        return record

    def kill_state(self) -> KillState:
        if self.fail_reads:
            raise JournalError("disk gone")
        return super().kill_state()


async def test_an_amend_whose_kill_switch_cannot_be_read_is_refused(
    venue: SimVenue, clock: ManualClock
) -> None:
    journal = ReadFailingAfterAppend()
    oms = manager_over(venue, journal, clock)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("KA3", price="9.50"))
        await venue.drain()
        journal.fail_reads_after = "replace_requested"
        state = await oms.amend("KA3", quantity=D(5000))
        snapshot = await venue.order_status("KA3", ACME)
        assert snapshot is not None and snapshot.quantity == 100
        assert state.status is S.ACCEPTED and state.quantity == 100
        events = [e for e in events_of(journal) if e.client_order_id == "KA3"]
        assert isinstance(events[-1], ReplaceRejected)
        assert events[-1].code == "JOURNAL_UNREADABLE"
    finally:
        journal.fail_reads = False
        await oms.abort()


async def cancel_it(oms: OrderManager, venue: SimVenue, cid: str) -> None:
    await oms.cancel(cid)


async def fill_it(oms: OrderManager, venue: SimVenue, cid: str) -> None:
    last = Fill(
        fill_id="LAST",
        client_order_id=cid,
        symbol=ACME,
        side=OrderSide.BUY,
        price="9.50",
        quantity="100",
        ts=START,
    )
    venue._deliver(
        Filled(client_order_id=cid, ts=START, origin=EventOrigin.VENUE, fill=last)
    )
    await venue.drain()
    assert oms.order(cid).status is S.FILLED


@pytest.mark.parametrize("overtake", [cancel_it, fill_it], ids=["cancel", "last-fill"])
async def test_an_amend_overtaken_while_it_reads_the_kill_switch_is_not_sent(
    venue: SimVenue,
    clock: ManualClock,
    overtake: Callable[[OrderManager, SimVenue, str], Coroutine[Any, Any, None]],
) -> None:
    """The read after ReplaceRequested is written, the second of the amend, is where
    the cancel or the last fill lands."""
    journal = MemoryJournal()
    oms = manager_over(venue, journal, clock)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("KA4", price="9.50"))
        await venue.drain()
        amends: list[str] = []

        async def amend_order(cid: str, *_: object, **__: object) -> None:
            amends.append(cid)
            raise RuntimeError("venue connection reset")

        real_merge = oms._merge_kill
        reads = 0

        async def overtaken_during_read(durable: KillState) -> None:
            nonlocal reads
            reads += 1
            if reads == 2:
                await overtake(oms, venue, "KA4")
            await real_merge(durable)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(venue, "amend_order", amend_order)
            patch.setattr(oms, "_merge_kill", overtaken_during_read)
            state = await oms.amend("KA4", quantity=D(50))
        assert reads >= 2
        assert amends == []
        assert state.status is not S.UNKNOWN
        assert not [
            e
            for e in events_of(journal)
            if e.client_order_id == "KA4" and isinstance(e, OutcomeUnknown)
        ]
    finally:
        await oms.abort()


async def test_a_release_read_during_a_local_engage_cannot_clear_it(
    make_manager: Factory, venue: SimVenue, journal: SqliteJournal
) -> None:
    """Memory is engaged at a low record and another process released since. The kill
    loop's merge is verifying that release when an operator here engages; the merge
    must not clear memory under the engage, and no order may go out.
    """
    oms = make_manager(kill_poll_interval=3600)
    await oms.start()
    try:
        await venue.drain()
        await oms.engage_kill("first", operator="ops", cancel_open=False)
        journal.append(
            RecordKind.KILL_RELEASED, json.dumps({"reason": "clear", "operator": "ops"})
        )
        real_verify = oms._newer_kill
        verifying, verify_may_finish = asyncio.Event(), asyncio.Event()
        engage_read, engage_may_finish = asyncio.Event(), asyncio.Event()
        real_durable = oms._durable_kill

        async def stalled_verify(memory: KillState, durable: KillState) -> KillState:
            found = await real_verify(memory, durable)
            verifying.set()
            await verify_may_finish.wait()
            return found

        async def stalled_read(memory: KillState) -> KillState:
            found = await real_durable(memory)
            engage_read.set()
            await engage_may_finish.wait()
            return found

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(oms, "_newer_kill", stalled_verify)
            merge = asyncio.ensure_future(oms._merge_kill(journal.kill_state()))
            await verifying.wait()
            patch.setattr(oms, "_newer_kill", real_verify)
            patch.setattr(oms, "_durable_kill", stalled_read)
            engage = asyncio.ensure_future(
                oms.engage_kill("desk pull", operator="desk", cancel_open=False)
            )
            await asyncio.sleep(0)
            verify_may_finish.set()
            await merge
            assert oms.kill_state.engaged, "the merge cleared an engage in flight"
            with pytest.raises(KillSwitchEngaged):
                await oms.submit(order("KM1", price="9.50"))
            assert venue.attempts["KM1"] == 0
            await engage_read.wait()
            engage_may_finish.set()
            await engage
        assert oms.kill_state.engaged and oms.kill_state.reason == "desk pull"
        assert journal.kill_state().engaged
    finally:
        await oms.abort()


# other-process-kill-not-reread-before-send


async def test_a_kill_from_another_process_during_the_send_stops_the_send(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    """``ismet kill engage`` commits between the first check and SendStarted."""
    other = SqliteJournal(journal_path)

    def at(step: str, _: str) -> None:
        if step == "after_intent":
            other.append(RecordKind.KILL_ENGAGED, json.dumps(DESK_KILL))

    journal = SqliteJournal(journal_path)
    oms = manager_over(venue, journal, clock, checkpoint=at)
    await oms.start()
    try:
        await venue.drain()
        state = await oms.submit(order("KP1", price="9.50"))
        assert state.status is S.REJECTED and state.reject_code == "KILL_SWITCH"
        assert venue.attempts["KP1"] == 0
        assert oms.kill_state.engaged and oms.kill_state.reason == "desk"
        kinds = [r.kind for r in journal.records()]
        assert kinds.index(RecordKind.KILL_ENGAGED) < len(kinds) - 1
    finally:
        await oms.abort()
        journal.close()
        other.close()


class ReadFailingJournal(MemoryJournal):
    fail_reads = False

    def kill_state(self) -> KillState:
        if self.fail_reads:
            raise JournalError("disk gone")
        return super().kill_state()


async def test_a_kill_switch_that_cannot_be_read_before_the_venue_call_refuses(
    venue: SimVenue, clock: ManualClock
) -> None:
    journal = ReadFailingJournal()

    def at(step: str, _: str) -> None:
        if step == "after_send_started":
            journal.fail_reads = True

    oms = manager_over(venue, journal, clock, checkpoint=at)
    await oms.start()
    try:
        await venue.drain()
        state = await oms.submit(order("KP2", price="9.50"))
        assert venue.attempts["KP2"] == 0
        assert state.status is S.REJECTED
        assert state.reject_code == "JOURNAL_UNREADABLE"
        assert not oms.open_orders()
    finally:
        await oms.abort()


# fee-correction-silently-dropped


def fill_with_fee(fee: str) -> Fill:
    return Fill(
        fill_id="F1",
        client_order_id="FE0",
        symbol=ACME,
        side=OrderSide.BUY,
        price="10",
        quantity="100",
        ts=START,
        fee=fee,
        fee_currency="USD",
    )


def test_a_known_fill_with_another_fee_keeps_its_fee_and_notes_the_dispute() -> None:
    state = new_order(
        SubmitIntent(client_order_id="FE0", ts=START, request=order("FE0"))
    )
    for event in (
        SendStarted(client_order_id="FE0", ts=START),
        Acknowledged(client_order_id="FE0", ts=START, venue_order_id="V1"),
        Filled(client_order_id="FE0", ts=START, fill=fill_with_fee("1")),
    ):
        state = apply_event(state, event)
    assert state.anomalies == ()
    disputed = apply_event(
        state, Filled(client_order_id="FE0", ts=START, fill=fill_with_fee("9"))
    )
    assert disputed.fills[0].fee == 1
    assert len(disputed.anomalies) == 1
    assert "different fee" in disputed.anomalies[0]
    assert "1 USD" in disputed.anomalies[0] and "9 USD" in disputed.anomalies[0]
    assert disputed.status is S.FILLED


def fills_recorded(journal: SqliteJournal, cid: str) -> list[Filled]:
    return [
        e
        for e in events_of(journal)
        if isinstance(e, Filled) and e.client_order_id == cid
    ]


def test_a_disputed_fee_never_books_an_amount_in_another_reports_currency() -> None:
    known = fill_with_fee("1").model_copy(update={"fee": None})
    report = fill_with_fee("1").model_copy(
        update={"fee_currency": "EUR", "venue_order_id": "V9"}
    )
    state = new_order(
        SubmitIntent(client_order_id="FE0", ts=START, request=order("FE0"))
    )
    for event in (
        SendStarted(client_order_id="FE0", ts=START),
        Acknowledged(client_order_id="FE0", ts=START, venue_order_id="V1"),
        Filled(client_order_id="FE0", ts=START, fill=known),
        Filled(client_order_id="FE0", ts=START, fill=report),
    ):
        state = apply_event(state, event)
    booked = state.fills[0]
    assert (booked.fee, booked.fee_currency) == (None, "USD")
    assert booked.venue_order_id == "V9"
    assert any("different fee" in a for a in state.anomalies)


async def test_a_fill_re_reported_with_another_fee_is_recorded_once_and_flagged(
    make_manager: Factory, venue: SimVenue, journal: SqliteJournal, clock: ManualClock
) -> None:
    anomalies: list[tuple[str, str]] = []
    oms = make_manager(on_anomaly=lambda cid, message: anomalies.append((cid, message)))
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("FE1", price="10.01"))
        booked = (await oms.wait_for("FE1", lambda o: o.status is S.FILLED, 2)).fills[0]
        assert booked.fee is not None
        disputed = booked.model_copy(update={"fee": add(booked.fee, D(8))})
        venue._fills[:] = [
            disputed if f.fill_id == booked.fill_id else f for f in venue._fills
        ]
        report = await oms.reconcile()
        assert report.breaks == []
        state = oms.order("FE1")
        assert state.fills[0].fee == booked.fee
        assert [a for a in state.anomalies if "different fee" in a]
        assert not oms.kill_state.engaged
        assert anomalies and "different fee" in anomalies[-1][1]
        recorded = fills_recorded(journal, "FE1")
        assert len(recorded) == 2
        assert recorded[-1].fill.fee == disputed.fee
        assert recorded[-1].origin is EventOrigin.RECONCILE
        await oms.reconcile()
        assert len(fills_recorded(journal, "FE1")) == 2
    finally:
        await oms.abort()
    restarted = make_manager()
    await restarted.start()
    try:
        await restarted.reconcile()
        assert len(fills_recorded(journal, "FE1")) == 2
        assert restarted.order("FE1").fills[0].fee == booked.fee
        assert not restarted.kill_state.engaged
    finally:
        await restarted.abort()


async def test_a_fee_dispute_that_also_names_the_currency_is_raised_once(
    make_manager: Factory, venue: SimVenue, journal: SqliteJournal
) -> None:
    """The sweep's report names a fee currency the record lacked and disputes the
    fee. The booked fee keeps its own currency, and a later sweep must not raise the
    dispute again."""
    anomalies: list[str] = []
    oms = make_manager(on_anomaly=lambda _cid, message: anomalies.append(message))
    await oms.start()
    try:
        await venue.drain()
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(venue, "_currency", None)
            await oms.submit(order("FE2", price="10.01"))
            booked = (
                await oms.wait_for("FE2", lambda o: o.status is S.FILLED, 2)
            ).fills[0]
        assert booked.fee is not None and booked.fee_currency is None
        disputed = booked.model_copy(
            update={"fee": add(booked.fee, D(8)), "fee_currency": "USD"}
        )
        venue._fills[:] = [
            disputed if f.fill_id == booked.fill_id else f for f in venue._fills
        ]
        await oms.reconcile()
        state = oms.order("FE2")
        assert state.fills[0].fee == booked.fee
        assert state.fills[0].fee_currency is None
        assert len([a for a in anomalies if "different fee" in a]) == 1
        assert len(fills_recorded(journal, "FE2")) == 2
        await oms.reconcile()
        assert len([a for a in anomalies if "different fee" in a]) == 1
        assert len(fills_recorded(journal, "FE2")) == 2
        assert not oms.kill_state.engaged
    finally:
        await oms.abort()


# replay-rejects-unregistered-codes


async def test_replay_accepts_a_fee_currency_only_the_old_process_had_registered(
    journal_path: Path, venue: SimVenue, clock: ManualClock
) -> None:
    journal = SqliteJournal(journal_path)
    oms = manager_over(venue, journal, clock)
    await oms.start()
    await venue.drain()
    await oms.submit(order("FX1", price="9.50"))
    await venue.drain()
    with accepting_stored_codes():  # as the process that registered the asset saw it
        foreign = Fill(
            fill_id="F9",
            client_order_id="FX1",
            symbol=ACME,
            side=OrderSide.BUY,
            price="9.50",
            quantity="100",
            ts=clock.now(),
            fee="0.1",
            fee_currency="ZZTOK",
        )
    venue._deliver(
        Filled(
            client_order_id="FX1",
            ts=clock.now(),
            origin=EventOrigin.VENUE,
            fill=foreign,
        )
    )
    await oms.wait_for("FX1", lambda o: o.status is S.FILLED, 2)
    await oms.abort()
    journal.close()
    venue.forget_order("FX1")
    venue.add_external_fill(ACME, OrderSide.BUY, D(100))

    journal = SqliteJournal(journal_path)
    restarted = manager_over(venue, journal, clock)
    try:
        await restarted.start()
        state = restarted.order("FX1")
        assert state.status is S.FILLED
        assert state.fills[0].fee_currency == "ZZTOK"
        assert [e.fill.fee_currency for e in fills_recorded(journal, "FX1")] == [
            "ZZTOK"
        ]
    finally:
        await restarted.abort()
        journal.close()
