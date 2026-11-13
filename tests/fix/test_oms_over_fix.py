"""The order manager, through FixProvider, over real sockets, against the acceptor.

``acceptor.orders`` and ``acceptor.received`` are what reached the venue, so they are
the evidence for the claim that matters: one intent is at most one order.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from ismet.errors import ConfigError, RiskRejected
from ismet.execution import (
    Journal,
    OrderManager,
    RiskConfig,
    RiskEngine,
    RiskLimits,
    SqliteJournal,
)
from ismet.execution.manager import CheckpointHook, EventHook
from ismet.execution.reconcile import BreakKind, ReconBreak
from ismet.fix.provider import FixProvider
from ismet.fix.store import FixStore, MemoryFixStore, SqliteFixStore
from ismet.models import (
    AssetClass,
    DataSource,
    FeedKind,
    Instrument,
    Quote,
    Symbol,
    Timeliness,
)
from ismet.models.orders import (
    CancelRequested,
    OrderEvent,
    OrderRequest,
    OrderSide,
    OrderStatus,
)

from .acceptor import FixAcceptor, VenueOrder
from .conftest import eventually, session_config

ACME = Symbol(venue="XNAS", ticker="ACME")


def instrument() -> Instrument:
    return Instrument.model_validate(
        {
            "symbol": ACME,
            "name": "ACME",
            "asset_class": AssetClass.EQUITY,
            "currency": "USD",
            "tick_size": Decimal("0.01"),
        }
    )


async def instrument_source(symbol: Symbol) -> Instrument:
    return instrument()


async def quote_source(symbol: Symbol) -> Quote:
    now = datetime.now(timezone.utc)
    return Quote(
        symbol=symbol,
        exchange_ts=now,
        received_ts=now,
        bid=Decimal("9.99"),
        ask=Decimal("10.01"),
        bid_size=Decimal(1000),
        ask_size=Decimal(1000),
        currency="USD",
        source=DataSource(
            provider="test", kind=FeedKind.SIMULATED, timeliness=Timeliness.REALTIME
        ),
    )


def risk(*, block_while_unknown: bool = False) -> RiskEngine:
    return RiskEngine(
        RiskConfig(
            default=RiskLimits(
                max_order_quantity=Decimal(1_000_000),
                max_order_notional=Decimal(100_000_000),
                max_position=Decimal(10_000_000),
                max_open_orders=10_000,
            ),
            duplicate_window_seconds=Decimal(0),
            allow_short=True,
            prevent_self_match=False,
            enforce_session=False,
            block_while_unknown=block_while_unknown,
        )
    )


# The one ack timeout also bounds the answers a lost-ack test expects, such as the
# protective cancel reconcile sends, so it must outlast a slow runner's reply.
LOST_ACK_TIMEOUT = 1.0


class SimulatedCrash(BaseException):
    """The process died here."""


def crash_at(name: str) -> CheckpointHook:
    def checkpoint(step: str, _: str) -> None:
        if step == name:
            raise SimulatedCrash(step)

    return checkpoint


def request(cid: str = "C1", **kw: Any) -> OrderRequest:
    fields: dict[str, Any] = {
        "client_order_id": cid,
        "symbol": ACME,
        "side": OrderSide.BUY,
        "quantity": "100",
        "limit_price": "10.00",
    }
    fields.update(kw)
    return OrderRequest.model_validate(fields)


class Rig:
    """A provider and an order manager that can be built, killed, and rebuilt."""

    def __init__(self, acceptor: FixAcceptor, path: Path) -> None:
        self.acceptor = acceptor
        self.fix_path = path / "fix.db"
        self.journal_path = path / "journal.db"
        self.store: FixStore = MemoryFixStore()
        self.journal: Journal | None = None
        self.provider: FixProvider | None = None
        self.oms: OrderManager | None = None
        self.breaks: list[ReconBreak] = []

    async def build(
        self,
        *,
        # Generous, so a slow runner is not a lost ack. Tests of a lost ack pass
        # LOST_ACK_TIMEOUT so the timeout they exercise stays short.
        ack_timeout: float = 5.0,
        unknown_grace: float = 0.0,
        checkpoint: CheckpointHook | None = None,
        on_event: EventHook | None = None,
        block_while_unknown: bool = False,
        **session: Any,
    ) -> OrderManager:
        self.store = SqliteFixStore(self.fix_path, "s")
        self.journal = SqliteJournal(self.journal_path)
        self.provider = FixProvider(
            session_config(self.acceptor, **session),
            self.store,
            ack_timeout=ack_timeout,
            query_timeout=1.0,
            logon_timeout=15.0,
            venues=(ACME.venue,),
        )
        await self.provider.open()
        self.oms = OrderManager(
            self.provider,
            self.journal,
            risk(block_while_unknown=block_while_unknown),
            instrument_source=instrument_source,
            reference_source=quote_source,
            allow_live=True,
            assume_flat=True,
            halt_on_break=False,
            unknown_grace=unknown_grace,
            reconcile_interval=3600.0,
            kill_poll_interval=0.01,
            on_break=self.breaks.append,
            on_event=on_event,
            checkpoint=checkpoint,
        )
        await self.oms.start()
        return self.oms

    async def teardown(self) -> None:
        if self.oms is not None:
            await self.oms.abort()
            self.oms = None
        if self.provider is not None:
            await self.provider.close()
            self.provider = None
        if self.journal is not None:
            self.journal.close()
            self.journal = None
        self.store.close()


RigFactory = Callable[[], Awaitable[Rig]]


@pytest.fixture
async def rig(acceptor: FixAcceptor, tmp_path: Path) -> AsyncIterator[Rig]:
    instance = Rig(acceptor, tmp_path)
    yield instance
    await instance.teardown()


class TestLostAck:
    async def test_lost_ack_then_reconcile_reaches_the_venue_once(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        """The scenario this adapter exists for."""
        oms = await rig.build(ack_timeout=LOST_ACK_TIMEOUT)
        acceptor.faults.lose_ack = 1

        state = await oms.submit(request())

        assert state.status is OrderStatus.UNKNOWN
        assert "no acknowledgement" in _unknown_reason(rig)
        assert list(acceptor.orders) == ["C1"]

        report = await oms.reconcile()

        assert oms.order("C1").status is OrderStatus.ACCEPTED
        assert oms.order("C1").venue_order_id == "O1"
        assert "C1" in report.resolved
        assert report.clean, report.breaks

        wires = acceptor.of_type("D")
        assert len(wires) == 1
        assert wires[0].get(11) == "C1"
        assert wires[0].get(43) is None
        assert wires[0].get(97) is None
        assert acceptor.orders["C1"].arrivals == 1
        assert len(acceptor.of_type("H")) == 1

        # Repeating the call changes nothing, and neither do further passes.
        assert (await oms.submit(request())).status is OrderStatus.ACCEPTED
        await oms.reconcile()
        assert len(acceptor.of_type("D")) == 1

        # The order is real and works normally from here on.
        exec_id = await acceptor.fill("C1", "100", "10.00")
        filled = await oms.wait_for("C1", lambda s: s.status is OrderStatus.FILLED, 2.0)
        assert [f.fill_id for f in filled.fills] == [exec_id]
        assert oms.net_position(ACME) == Decimal(100)
        assert len(acceptor.of_type("D")) == 1

    async def test_a_late_ack_resolves_the_order_without_a_query(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build(ack_timeout=LOST_ACK_TIMEOUT)
        acceptor.faults.lose_ack = 1
        assert (await oms.submit(request())).status is OrderStatus.UNKNOWN
        await acceptor.exec_report("C1", "0", "0")
        state = await oms.wait_for(
            "C1", lambda s: s.status is OrderStatus.ACCEPTED, 2.0
        )
        assert state.venue_order_id == "O1"
        assert len(acceptor.of_type("D")) == 1

    async def test_order_that_filled_while_the_ack_was_lost(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build(ack_timeout=LOST_ACK_TIMEOUT)
        acceptor.faults.lose_ack = 1
        assert (await oms.submit(request())).status is OrderStatus.UNKNOWN
        await acceptor.fill("C1", "100", "10.00")
        await oms.wait_for("C1", lambda s: s.filled_quantity == Decimal(100), 2.0)
        await oms.reconcile()
        assert oms.order("C1").status is OrderStatus.FILLED
        assert len(acceptor.of_type("D")) == 1

    async def test_order_the_venue_never_kept_is_rejected_after_the_grace_period(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build(ack_timeout=LOST_ACK_TIMEOUT)
        acceptor.faults.lose_ack = 1
        state = await oms.submit(request())
        assert state.status is OrderStatus.UNKNOWN
        # A venue failover: it saw the order and then lost it.
        acceptor.orders.clear()
        acceptor.wire.clear()
        await oms.reconcile()
        final = oms.order("C1")
        assert final.status is OrderStatus.REJECTED
        assert final.reject_code == "NOT_FOUND_AT_VENUE"
        assert len(acceptor.of_type("D")) == 1
        assert len(acceptor.of_type("F")) == 1, "a cancel kills anything arriving late"
        assert len(acceptor.of_type("H")) == 2

    async def test_a_failed_query_leaves_the_order_unknown(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build(ack_timeout=LOST_ACK_TIMEOUT)
        acceptor.faults.lose_ack = 1
        await oms.submit(request())
        acceptor.faults.ignore_status_requests = True
        report = await oms.reconcile()
        assert oms.order("C1").status is OrderStatus.UNKNOWN
        assert not report.complete
        assert report.breaks[0].kind is BreakKind.QUERY_FAILED
        assert len(acceptor.of_type("D")) == 1


class TestConnectionLoss:
    async def test_session_down_before_writing_is_not_delivered(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build()
        await acceptor.stop()
        assert rig.provider is not None
        await eventually(lambda: not rig.provider.session.is_ready)  # type: ignore[union-attr]
        state = await oms.submit(request())
        assert state.status is OrderStatus.REJECTED
        assert state.reject_code == "NOT_DELIVERED"
        assert acceptor.of_type("D") == []

    async def test_disconnect_after_sending_is_unknown_then_resolved(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build(ack_timeout=5.0)
        acceptor.faults.lose_ack = 1
        acceptor.faults.drop_after_type["D"] = 1
        state = await oms.submit(request())
        assert state.status is OrderStatus.UNKNOWN
        assert rig.provider is not None
        await rig.provider.session.wait_ready(5.0)
        await oms.reconcile()
        assert oms.order("C1").status is OrderStatus.ACCEPTED
        assert len(acceptor.of_type("D")) == 1
        assert acceptor.orders["C1"].arrivals == 1

    async def test_order_lost_on_the_wire_reaches_the_venue_once_by_session_resend(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build(ack_timeout=0.05, logon_timeout=0.1)
        assert rig.provider is not None
        session = rig.provider.session
        assert session._writer is not None
        writer = session._writer
        real = writer.write

        def half_then_cut(data: bytes) -> None:
            real(data[: len(data) // 2])
            writer.transport.abort()

        writer.write = half_then_cut  # type: ignore[method-assign]
        # Hold the session out until the ack timeout has fired: a reconnect inside
        # it resends the order first, and the test then races its own recovery.
        acceptor.faults.ignore_logon = True
        state = await oms.submit(request())
        assert state.status is OrderStatus.UNKNOWN
        assert acceptor.of_type("D") == []
        acceptor.faults.ignore_logon = False
        await session.wait_ready(5.0)
        await eventually(lambda: "C1" in acceptor.orders)
        await oms.wait_for("C1", lambda s: s.status is OrderStatus.ACCEPTED, 2.0)
        assert acceptor.orders["C1"].arrivals == 1
        assert list(acceptor.orders) == ["C1"]
        assert all(m.get(43) == "Y" for m in acceptor.of_type("D"))

    async def test_duplicate_exec_reports_count_once(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build()
        await oms.submit(request())
        await acceptor.fill("C1", "40", "10.00")
        await oms.wait_for("C1", lambda s: s.filled_quantity == Decimal(40), 2.0)
        fields = acceptor.sent[max(acceptor.sent)][1]
        await acceptor.send("8", list(fields))
        await acceptor.send("8", [*fields, (97, "Y")])
        assert rig.provider is not None
        await eventually(
            lambda: rig.provider.session.next_inbound == acceptor.next_out  # type: ignore[union-attr]
        )
        await oms.reconcile()
        assert oms.order("C1").filled_quantity == Decimal(40)
        assert oms.net_position(ACME) == Decimal(40)

    async def test_a_gap_in_exec_reports_is_filled_in_order(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build()
        await oms.submit(request())
        acceptor.faults.garble_next = 1
        await acceptor.fill("C1", "40", "10.00")
        await acceptor.fill("C1", "60", "10.00")
        state = await oms.wait_for("C1", lambda s: s.status is OrderStatus.FILLED, 3.0)
        assert [f.quantity for f in state.fills] == [Decimal(40), Decimal(60)]
        assert state.anomalies == ()


class TestUnreadableReports:
    async def test_an_unreadable_fill_on_a_closing_order_cannot_pass_unnoticed(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        """The fill is unreadable and the order then closes: exposure is wrong."""
        oms = await rig.build(unknown_grace=0.0)
        await oms.submit(request())
        venue_order = acceptor.orders["C1"]
        venue_order.filled = Decimal(40)
        venue_order.status = "1"
        await acceptor.exec_report(
            "C1",
            "F",
            "1",
            last_qty=Decimal(40),
            last_px=Decimal(10),
            extra=[(13, "3"), (12, "bad")],
        )
        venue_order.status = "4"
        await acceptor.exec_report("C1", "4", "4")
        await eventually(
            lambda: any(b.kind is BreakKind.FILL_MISMATCH for b in rig.breaks),
            timeout=5.0,
        )
        assert oms.stream_healthy is False
        with contextlib.suppress(RiskRejected):
            await oms.submit(request("C2"))
        assert "C2" not in acceptor.orders
        assert rig.provider is not None
        rows = await rig.provider.quarantined()
        assert [row.halted for row in rows] == [True, True]
        assert "not a decimal" in rows[0].reason
        assert "CumQty" in rows[1].reason

    async def test_a_fill_the_venue_gap_filled_cannot_close_the_order_at_zero(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        """The venue traded 40, lost the report, and closed the order with CumQty 40."""
        oms = await rig.build(unknown_grace=0.0)
        await oms.submit(request())
        venue_order = acceptor.orders["C1"]
        venue_order.filled = Decimal(40)
        acceptor.faults.skip_seq_next = 1
        venue_order.status = "4"
        await acceptor.exec_report("C1", "4", "4")
        await eventually(
            lambda: any(b.kind is BreakKind.FILL_MISMATCH for b in rig.breaks),
            timeout=5.0,
        )
        assert acceptor.resend_requests_answered >= 1
        assert oms.stream_healthy is False
        with contextlib.suppress(RiskRejected):
            await oms.submit(request("C2"))
        assert "C2" not in acceptor.orders
        assert rig.provider is not None
        assert [q.halted for q in await rig.provider.quarantined()] == [True]


class TestBusts:
    async def test_a_bust_reduces_the_position(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build()
        await oms.submit(request())
        exec_id = await acceptor.fill("C1", "40", "10.00")
        await oms.wait_for("C1", lambda s: s.filled_quantity == Decimal(40), 2.0)
        await acceptor.bust("C1", exec_id, "40")
        state = await oms.wait_for("C1", lambda s: s.filled_quantity == Decimal(0), 2.0)
        assert state.status is OrderStatus.ACCEPTED
        assert oms.net_position(ACME) == Decimal(0)


class TestCancelAndAmend:
    async def test_cancel_through_the_order_manager(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build()
        await oms.submit(request())
        await oms.cancel("C1")
        state = await oms.wait_for(
            "C1", lambda s: s.status is OrderStatus.CANCELED, 2.0
        )
        assert state.is_terminal
        assert acceptor.of_type("F")[0].get(11) == "C1~1"

    async def test_cancel_rejected_returns_to_working(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build()
        await oms.submit(request())
        acceptor.faults.cancel_reject = "0"
        state = await oms.cancel("C1")
        assert state.status is OrderStatus.ACCEPTED
        assert state.reject_code == "0"

    async def test_amend_through_the_order_manager(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build()
        await oms.submit(request())
        await oms.amend("C1", limit_price=Decimal("10.50"))
        state = await oms.wait_for(
            "C1", lambda s: s.limit_price == Decimal("10.50"), 2.0
        )
        assert state.status is OrderStatus.ACCEPTED
        assert acceptor.of_type("G")[0].get(44) == "10.50"

    async def test_amend_rejected_returns_to_working(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build()
        await oms.submit(request())
        acceptor.faults.replace_reject = "0"
        state = await oms.amend("C1", limit_price=Decimal("10.50"))
        assert state.status is OrderStatus.ACCEPTED
        assert state.limit_price == Decimal("10.00")


class TestDuplicateOrderId:
    async def test_venue_duplicate_goes_to_reconciliation_not_to_rejected(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        """The venue already has the order, so calling it rejected would hide it."""
        oms = await rig.build(ack_timeout=LOST_ACK_TIMEOUT)
        acceptor.orders["C1"] = VenueOrder(
            "C1", "C1", "ACME", "1", Decimal(100), Decimal(10), "O7"
        )
        acceptor.wire["C1"] = "C1"
        acceptor.faults.order_reject = "6"
        acceptor.faults.reject_even_if_known = True
        state = await oms.submit(request())
        assert state.status is OrderStatus.UNKNOWN
        acceptor.faults.order_reject = None
        await oms.reconcile()
        assert oms.order("C1").status is OrderStatus.ACCEPTED
        assert oms.order("C1").venue_order_id == "O7"


class TestRejection:
    async def test_a_venue_reject_is_a_rejected_order(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build()
        acceptor.faults.order_reject = "3"
        state = await oms.submit(request())
        assert state.status is OrderStatus.REJECTED
        assert state.reject_code == "3"
        assert state.anomalies == ()

    async def test_a_report_for_an_order_ismet_never_sent_is_a_break(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        await rig.build()
        acceptor.orders["ALIEN"] = VenueOrder(
            "ALIEN", "ALIEN", "ACME", "1", Decimal(1), None, "O99"
        )
        await acceptor.exec_report("ALIEN", "0", "0")
        await eventually(lambda: bool(rig.breaks))
        assert rig.breaks[0].kind is BreakKind.ORPHAN_ORDER


class TestCrashRecovery:
    async def test_process_death_after_the_send_resolves_on_restart(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build(ack_timeout=LOST_ACK_TIMEOUT)
        acceptor.faults.lose_ack = 1
        await oms.submit(request())
        await rig.teardown()

        restarted = await rig.build(ack_timeout=LOST_ACK_TIMEOUT)

        assert restarted.order("C1").status is OrderStatus.ACCEPTED
        assert len(acceptor.of_type("D")) == 1
        assert acceptor.orders["C1"].arrivals == 1

    async def test_fill_stored_by_the_provider_but_not_journalled_is_recovered(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        """The process dies after the provider stored a fill, before the OMS saw it."""
        oms = await rig.build()
        await oms.submit(request())
        await oms.abort()
        rig.oms = None
        assert rig.provider is not None
        session = rig.provider.session
        exec_id = await acceptor.fill("C1", "40", "10.00")
        await eventually(lambda: session.next_inbound == acceptor.next_out)
        rig.provider._backlog.clear()
        await rig.teardown()

        restarted = await rig.build()

        assert restarted.order("C1").filled_quantity == Decimal(40)
        assert [f.fill_id for f in restarted.order("C1").fills] == [exec_id]
        assert restarted.net_position(ACME) == Decimal(40)
        assert not [b for b in rig.breaks if b.is_critical]

    async def test_fills_during_downtime_replay_through_the_session(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build()
        await oms.submit(request())
        await rig.teardown()
        # The venue kept working while we were down, and has nowhere to send it yet.
        order = acceptor.orders["C1"]
        order.filled = Decimal(40)
        order.status = "1"
        acceptor.next_out += 1
        seq = acceptor.next_out - 1
        acceptor.sent[seq] = (
            "8",
            [
                (37, "O1"),
                (11, "C1"),
                (17, "DOWNTIME"),
                (150, "F"),
                (39, "1"),
                (54, "1"),
                (55, "ACME"),
                (38, "100"),
                (14, "40"),
                (151, "60"),
                (6, "10"),
                (32, "40"),
                (31, "10"),
            ],
            "20260309-14:30:15.000",
        )

        restarted = await rig.build()

        state = await restarted.wait_for(
            "C1", lambda s: s.filled_quantity == Decimal(40), 3.0
        )
        assert [f.fill_id for f in state.fills] == ["DOWNTIME"]
        assert state.anomalies == ()

    async def test_sequence_numbers_are_preserved_across_a_restart(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build()
        await oms.submit(request())
        # Read the ack before stopping: one still unread is a gap the restart rightly
        # asks to resend, which a loaded machine can leave behind a short ack timeout.
        await oms.wait_for("C1", lambda s: s.status is OrderStatus.ACCEPTED, 5.0)
        await rig.teardown()
        await rig.build()
        assert acceptor.resend_requests_answered == 0
        assert len(acceptor.logons) == 2
        assert int(acceptor.logons[1].require(34)) > 1

    async def test_death_between_send_started_and_the_store_row_closes_as_never_sent(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        """The provider writes its row before the socket, so an intact store with no
        row proves nothing left: the order closes and trading resumes."""
        oms = await rig.build(checkpoint=crash_at("after_send_started"))
        with pytest.raises(SimulatedCrash):
            await oms.submit(request())
        await rig.teardown()
        assert acceptor.of_type("D") == []

        # The journal replays C1 as unknown, and the startup reconciliation closes it.
        restarted = await rig.build(block_while_unknown=True)
        report = await restarted.reconcile()
        assert not [b for b in report.breaks if b.kind is BreakKind.QUERY_FAILED]
        closed = restarted.order("C1")
        assert closed.status is OrderStatus.REJECTED
        assert closed.reject_code == "NOT_FOUND_AT_VENUE"
        assert closed.filled_quantity == Decimal(0)
        assert acceptor.of_type("D") == []
        assert acceptor.of_type("H") == [], "the store answered, not the venue"

        await restarted.submit(request("C2"))
        # A late ack under load leaves C2 unknown for a moment; it must still land.
        await restarted.wait_for("C2", lambda s: s.status is OrderStatus.ACCEPTED, 5.0)
        assert [m.get(11) for m in acceptor.of_type("D")] == ["C2"]
        assert list(acceptor.orders) == ["C2"]

    async def test_death_before_the_store_row_with_a_lost_store_stays_refused(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        """A new empty store proves nothing, so the order stays unknown and new orders
        stay refused until an operator resolves it."""
        oms = await rig.build(checkpoint=crash_at("after_send_started"))
        with pytest.raises(SimulatedCrash):
            await oms.submit(request())
        await rig.teardown()
        for lost in rig.fix_path.parent.glob(f"{rig.fix_path.name}*"):
            lost.unlink()
        # The venue began a new session too, so a fresh store can log on at all.
        acceptor.next_in = 1
        acceptor.next_out = 1
        acceptor.sent.clear()

        restarted = await rig.build(block_while_unknown=True)
        report = await restarted.reconcile()
        assert restarted.order("C1").status is OrderStatus.UNKNOWN
        failed = [b for b in report.breaks if b.kind is BreakKind.QUERY_FAILED]
        assert failed and "no local record" in failed[0].detail
        with pytest.raises(RiskRejected):
            await restarted.submit(request("C2"))
        assert acceptor.of_type("D") == []

    @staticmethod
    def _lose_the_store(acceptor: FixAcceptor, rig: Rig) -> None:
        for lost in rig.fix_path.parent.glob(f"{rig.fix_path.name}*"):
            lost.unlink()
        acceptor.next_in = 1
        acceptor.next_out = 1
        acceptor.sent.clear()

    async def test_a_lost_store_stays_refused_on_every_later_restart(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        """Each logon consumes a number, so a lost store soon looks used. Its id does
        not change into the one the order was sent under."""
        oms = await rig.build(checkpoint=crash_at("after_send_started"))
        with pytest.raises(SimulatedCrash):
            await oms.submit(request())
        await rig.teardown()
        self._lose_the_store(acceptor, rig)
        for _ in range(3):
            restarted = await rig.build(block_while_unknown=True)
            report = await restarted.reconcile()
            assert restarted.order("C1").status is OrderStatus.UNKNOWN
            assert [b for b in report.breaks if b.kind is BreakKind.QUERY_FAILED]
            await rig.teardown()
        assert acceptor.of_type("F") == []

    async def test_a_working_order_is_not_closed_on_a_lost_store(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        """The venue still holds it, so no restart may report it gone."""
        oms = await rig.build()
        await oms.submit(request())
        await rig.teardown()
        self._lose_the_store(acceptor, rig)
        for _ in range(3):
            restarted = await rig.build()
            await restarted.reconcile()
            state = restarted.order("C1")
            assert state.is_open
            assert state.reject_code is None
            await rig.teardown()
        assert list(acceptor.orders) == ["C1"]
        assert acceptor.of_type("F") == []

    async def test_a_working_order_on_a_lost_store_is_resolved_by_an_operator(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        """It becomes unknown, which is where ``resolve_unknown`` applies. Nothing
        cancels or closes it by itself, and every new order waits meanwhile."""
        oms = await rig.build()
        await oms.submit(request())
        await rig.teardown()
        self._lose_the_store(acceptor, rig)

        restarted = await rig.build()
        report = await restarted.reconcile()
        assert [b.kind for b in report.breaks] == [BreakKind.QUERY_FAILED]
        assert restarted.order("C1").status is OrderStatus.UNKNOWN
        with pytest.raises(RiskRejected):
            await restarted.submit(request("C2"))
        assert acceptor.of_type("F") == []
        assert acceptor.orders["C1"].status == "0"

        # The operator cancels at the counterparty and confirms, then resolves.
        acceptor.orders["C1"].status = "4"
        state = await restarted.resolve_unknown(
            "C1", operator="ops", reason="cancelled and confirmed at the venue"
        )
        assert state.status is OrderStatus.REJECTED
        assert state.reject_code == "RESOLVED_BY_OPERATOR"
        assert (await restarted.reconcile()).clean
        assert (await restarted.submit(request("C2"))).status is OrderStatus.ACCEPTED
        assert [m.get(11) for m in acceptor.of_type("D")] == ["C1", "C2"]
        await rig.teardown()

        again = await rig.build()
        assert (await again.reconcile()).clean
        assert again.order("C1").reject_code == "RESOLVED_BY_OPERATOR"
        assert again.order("C2").status is OrderStatus.ACCEPTED

    async def test_a_wiped_store_that_carried_a_new_order_does_not_clear_the_old_one(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build(checkpoint=crash_at("after_send_started"))
        with pytest.raises(SimulatedCrash):
            await oms.submit(request())
        await rig.teardown()
        self._lose_the_store(acceptor, rig)

        await rig.build()
        assert rig.provider is not None
        await rig.provider.place_order(request("C2"))
        assert [m.get(11) for m in acceptor.of_type("D")] == ["C2"]
        await rig.teardown()

        again = await rig.build()
        await again.reconcile()
        assert again.order("C1").status is OrderStatus.UNKNOWN


class TestCancelDuringPlacement:
    async def test_a_cancel_during_the_halt_check_waits_for_the_row_and_is_sent(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        """The send checks the quarantine before it writes its row. A cancel that
        looks for the row meanwhile must wait, not be refused while the order goes
        out anyway."""
        cancel_recorded = asyncio.Event()

        def on_event(event: OrderEvent, _: Any) -> None:
            if isinstance(event, CancelRequested):
                cancel_recorded.set()

        oms = await rig.build(on_event=on_event)
        # Hold the send inside its quarantine check, on the store's own thread.
        reached, release = threading.Event(), threading.Event()
        real_quarantined = rig.store.quarantined

        def gated_quarantined():  # type: ignore[no-untyped-def]
            reached.set()
            release.wait(5.0)
            return real_quarantined()

        rig.store.quarantined = gated_quarantined  # type: ignore[method-assign]
        placing = asyncio.create_task(oms.submit(request()))
        await eventually(reached.is_set)
        cancelling = asyncio.create_task(oms.cancel("C1"))
        # The cancel is journalled, then goes to the provider; give it the turns it
        # takes to get there, so it is in front of the row's write either way.
        await asyncio.wait_for(cancel_recorded.wait(), 5.0)
        for _ in range(20):
            await asyncio.sleep(0)
        release.set()

        await placing
        await cancelling
        state = await oms.wait_for("C1", lambda s: s.is_terminal, 3.0)
        assert state.status is OrderStatus.CANCELED
        assert acceptor.orders["C1"].status == "4"
        assert [m.msg_type for m in acceptor.received if m.msg_type in "DF"] == [
            "D",
            "F",
        ]


class TestHaltAcrossRestart:
    async def test_the_first_order_after_a_restart_is_refused_while_halted(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        """``submit`` right after ``start`` can run before the stream loop has read
        the halt, so the provider itself must refuse."""
        oms = await rig.build()
        await oms.submit(request())
        await acceptor.send("8", [(150, "F"), (17, "NOID"), (32, "10"), (31, "5")])
        await eventually(lambda: oms.stream_healthy is False)
        await rig.teardown()

        restarted = await rig.build()
        with contextlib.suppress(RiskRejected):
            state = await restarted.submit(request("C2"))
            assert state.status is OrderStatus.REJECTED
        assert [m.get(11) for m in acceptor.of_type("D")] == ["C1"]
        assert list(acceptor.orders) == ["C1"]


class TestSafetyInterlocks:
    async def test_a_live_provider_needs_an_explicit_opt_in(
        self, acceptor: FixAcceptor, tmp_path: Path
    ) -> None:
        provider = FixProvider(session_config(acceptor), MemoryFixStore())
        journal = SqliteJournal(tmp_path / "j.db")
        with pytest.raises(ConfigError, match="allow_live"):
            OrderManager(provider, journal, risk())
        journal.close()

    async def test_too_low_sequence_stops_the_session_and_new_orders_fail_safely(
        self, acceptor: FixAcceptor, rig: Rig
    ) -> None:
        oms = await rig.build()
        await oms.submit(request())
        assert rig.provider is not None
        session = rig.provider.session
        await acceptor.send_raw_seq(1, "0", [])
        await eventually(lambda: session.failure is not None)
        await eventually(lambda: oms.stream_healthy is False)
        # Refused by risk now that the stream is down, or by the provider if it is not.
        with contextlib.suppress(RiskRejected):
            state = await oms.submit(request("C2"))
            assert state.status is OrderStatus.REJECTED
        assert "C2" not in acceptor.orders
        assert len(acceptor.orders) == 1


def _unknown_reason(rig: Rig) -> str:
    assert rig.journal is not None
    return " ".join(
        record.payload
        for record in rig.journal.records()
        if "outcome_unknown" in record.payload
    )
