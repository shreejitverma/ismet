"""Exposure, recovery and race behaviour, from worked examples.

Each expected value is computed by hand in the test or follows from the venue's own
script, never from the code under test.
"""

from __future__ import annotations

import asyncio
import io
import json
import subprocess
import sys
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from ismet.accounting import PositionLedger, build_ledger, value_books
from ismet.calendars import get_calendar
from ismet.errors import ConfigError, RiskRejected, ValidationError
from ismet.execution import (
    Journal,
    MemoryJournal,
    OrderManager,
    RiskContext,
    RiskEngine,
    RiskLimits,
    SqliteJournal,
)
from ismet.execution.journal import RecordKind, export_jsonl, verify_jsonl
from ismet.execution.manager import events_of
from ismet.execution.reconcile import BreakKind
from ismet.execution.risk import check_instrument, check_self_match
from ismet.execution.state import apply_event, new_order
from ismet.models import Symbol
from ismet.models.orders import (
    Acknowledged,
    CancelRequested,
    Fill,
    FillBusted,
    Filled,
    OrderSide,
    OrderStatus,
    OutcomeUnknown,
    Position,
    Replaced,
    ReplaceRejected,
    ReplaceRequested,
    Restated,
    SendStarted,
    SubmitIntent,
)
from ismet.testing.simvenue import SIM_VENUE, Fault, SimVenue, sim_instrument
from ismet.testing.trading_conformance import run_trading_conformance
from ismet.transport.clock import ManualClock

from .conftest import ACME, START, order, permissive_config, until

D = Decimal
S = OrderStatus
SELL = OrderSide.SELL
OTHER = Symbol(venue=SIM_VENUE, ticker="OTHR")
TOYO = Symbol(venue=SIM_VENUE, ticker="TOYO")
TS = START
BASE = {"client_order_id": "C1", "ts": TS}


def state_after(*events: object):  # type: ignore[no-untyped-def]
    request = order("C1", quantity="100", price="10")
    state = new_order(SubmitIntent(**BASE, request=request))
    for event in events:
        state = apply_event(state, event)  # type: ignore[arg-type]
    return state


def filled(fill_id: str, quantity: str, price: str = "10") -> Filled:
    return Filled(
        **BASE,
        fill=Fill(
            fill_id=fill_id,
            client_order_id="C1",
            symbol=ACME,
            side=OrderSide.BUY,
            price=price,
            quantity=quantity,
            ts=TS,
        ),
    )


WORKING = (SendStarted(**BASE), Acknowledged(**BASE, venue_order_id="V1"))


def gross_config(limit: str, **overrides: object):  # type: ignore[no-untyped-def]
    return permissive_config(
        default=RiskLimits(max_gross_notional=D(limit)), **overrides
    )


def gross_venue(clock: ManualClock, **positions: object) -> SimVenue:
    held = {Symbol(venue=SIM_VENUE, ticker=k): D(str(v)) for k, v in positions.items()}
    venue = SimVenue(
        clock=clock,
        instruments=[sim_instrument("ACME"), sim_instrument("OTHR")],
        initial_positions=held,
    )
    venue.set_quote(ACME, "9.99", "10.01")
    return venue


def manager(venue: SimVenue, journal: Journal, clock: ManualClock, config=None, **kw):  # type: ignore[no-untyped-def]
    options = {
        "clock": clock,
        "unknown_grace": 0.0,
        "reconcile_interval": 3600.0,
        "kill_poll_interval": 0.01,
        "stream_settle": None,
    }
    options.update(kw)
    return OrderManager(
        venue,
        journal,
        RiskEngine(config if config is not None else permissive_config()),
        **options,
    )


# The reducer


def test_a_busted_fill_cannot_be_booked_again_and_a_repeated_bust_changes_nothing() -> (
    None
):
    bust = FillBusted(**BASE, fill_id="F1", reason="busted")
    state = state_after(*WORKING, filled("F1", "40"), bust)
    assert state.filled_quantity == 0
    assert apply_event(state, bust) == state
    again = apply_event(state, filled("F1", "40"))
    assert again.filled_quantity == 0
    assert "reported again after its bust" in again.anomalies[-1]


def test_a_bust_that_arrives_before_its_fill_still_stops_the_fill() -> None:
    state = state_after(*WORKING, FillBusted(**BASE, fill_id="F1", reason="busted"))
    assert state.filled_quantity == 0
    assert apply_event(state, filled("F1", "40")).filled_quantity == 0


def test_a_restatement_ends_a_requested_change() -> None:
    state = state_after(
        *WORKING,
        ReplaceRequested(**BASE, quantity="150"),
        OutcomeUnknown(**BASE, reason="lost"),
    )
    assert state.leaves_quantity == 150
    state = apply_event(state, Restated(**BASE, status=S.ACCEPTED, reason="venue"))
    assert state.leaves_quantity == 100
    state = apply_event(state, Replaced(**BASE, limit_price="11"))
    assert state.quantity == 100 and state.limit_price == 11


def test_a_fill_within_the_requested_size_is_not_an_overfill() -> None:
    state = state_after(
        *WORKING, ReplaceRequested(**BASE, quantity="150"), filled("F1", "120")
    )
    assert state.status is S.PENDING_REPLACE and not state.anomalies
    assert apply_event(state, Replaced(**BASE)).status is S.PARTIALLY_FILLED


def test_a_refused_change_makes_that_fill_an_overfill() -> None:
    state = state_after(
        *WORKING, ReplaceRequested(**BASE, quantity="150"), filled("F1", "120")
    )
    state = apply_event(state, ReplaceRejected(**BASE, reason="no"))
    assert state.status is S.FILLED
    assert "overfill: 120 filled against 100" in state.anomalies[-1]


def test_a_change_requested_too_late_sets_no_terms() -> None:
    state = state_after(
        *WORKING, CancelRequested(**BASE), ReplaceRequested(**BASE, quantity="150")
    )
    assert state.status is S.PENDING_CANCEL
    assert state.pending_quantity is None


def test_self_match_also_judges_the_price_a_resting_order_is_moving_to() -> None:
    resting = state_after(
        *WORKING, ReplaceRequested(**BASE, limit_price="10.50")
    )  # a buy moving from 10 up to 10.50
    sell = order("C2", side=SELL, price="10.25")
    decision = check_self_match(
        RiskContext(
            request=sell,
            instrument=None,
            reference=None,
            now=TS,
            open_orders=[resting],
        ),
        permissive_config(prevent_self_match=True),
    )
    assert decision.check == "self_match"


# The send path


async def test_an_order_stopped_short_is_closed_without_querying_the_venue(
    venue: SimVenue, clock: ManualClock
) -> None:
    class Crash(BaseException):
        pass

    armed = {"on": True}

    def stop(step: str, _: str) -> None:
        if step == "after_intent" and armed["on"]:
            armed["on"] = False
            raise Crash

    oms = manager(venue, MemoryJournal(), clock, checkpoint=stop)
    await oms.start()
    try:
        await venue.drain()
        with pytest.raises(Crash):
            await oms.submit(order("PS1", price="9.50"))
        assert oms.order("PS1").status is S.PENDING_SUBMIT
        await oms.reconcile()
        state = oms.order("PS1")
        assert (state.status, state.reject_code) == (S.REJECTED, "NEVER_SENT")
        assert not oms.kill_state.engaged and venue.attempts["PS1"] == 0
    finally:
        await oms.abort()


async def test_a_fill_on_an_order_never_sent_halts(
    venue: SimVenue, clock: ManualClock
) -> None:
    class Crash(BaseException):
        pass

    def stop(step: str, _: str) -> None:
        if step == "after_intent":
            raise Crash

    oms = manager(venue, MemoryJournal(), clock, checkpoint=stop)
    await oms.start()
    try:
        await venue.drain()
        request = order("PS2", price="9.50")
        with pytest.raises(Crash):
            await oms.submit(request)
        venue._deliver(
            Filled(
                client_order_id="PS2",
                ts=TS,
                fill=Fill(
                    fill_id="F1",
                    client_order_id="PS2",
                    symbol=ACME,
                    side=OrderSide.BUY,
                    price="9.50",
                    quantity="1",
                    ts=TS,
                ),
            )
        )
        await venue.drain()
        await until(lambda: oms.kill_state.engaged)
        assert "before the send started" in oms.kill_state.reason
    finally:
        await oms.abort()


async def test_amend_is_judged_on_fills_that_land_while_it_waits(
    oms: OrderManager, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    await venue.drain()
    await oms.submit(order("AM1", quantity="10", price="9.50"))
    venue.trade_through(ACME, "9.50", "5")
    await oms.wait_for("AM1", lambda o: o.filled_quantity == 5, 2)
    quote = venue.quote

    async def trades_meanwhile(symbol: Symbol):  # type: ignore[no-untyped-def]
        venue.trade_through(ACME, "9.50", "2")
        await oms.wait_for("AM1", lambda o: o.filled_quantity == 7, 2)
        return await quote(symbol)

    monkeypatch.setattr(venue, "quote", trades_meanwhile)
    with pytest.raises(ValidationError, match="not above filled 7"):
        await oms.amend("AM1", quantity=D(6))
    assert oms.order("AM1").pending_quantity is None
    assert venue._orders["AM1"].quantity == 10


async def test_cancels_do_not_wait_for_each_others_answers(
    oms: OrderManager, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    await venue.drain()
    for index in range(5):
        await oms.submit(order(f"KS{index}", price="9.50", quantity="1"))
    started: list[str] = []
    answer = asyncio.Event()

    async def stalled(client_order_id: str, symbol: Symbol) -> None:
        started.append(client_order_id)
        await answer.wait()

    monkeypatch.setattr(venue, "cancel_order", stalled)
    sweep = asyncio.create_task(oms.cancel_all())
    await until(lambda: len(started) == 5)
    answer.set()
    assert len(await sweep) == 5


# Reconciliation races


async def test_a_status_answer_older_than_a_stream_fill_does_not_reopen_the_order(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("RC1", price="9.50"))
    await venue.drain()
    calls = {"n": 0}

    def trade_after_answering(method: str) -> None:
        if method == "order_status":
            calls["n"] += 1
            if calls["n"] == 1:
                venue.trade_through(ACME, "9.50", "100")

    venue.after_answer = trade_after_answering
    venue.answer_ticks = 50
    await oms.reconcile()
    state = await oms.wait_for("RC1", lambda o: o.is_terminal, 2)
    assert state.status is S.FILLED and not state.anomalies
    assert not oms.open_orders()
    venue.after_answer = None
    assert not [e for e in events_of(oms._journal) if isinstance(e, Restated)]


async def test_a_conclusion_is_dropped_when_a_fill_lands_while_it_is_written(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.faults.append(Fault.LOSE_RESPONSE)
    venue.report_faults.drop_next = 1
    state = await oms.submit(order("RC7", price="9.50"))
    assert state.status is S.UNKNOWN
    await venue.drain()
    calls = {"n": 0}

    def trade_after_answering(method: str) -> None:
        if method == "order_status":
            calls["n"] += 1
            if calls["n"] == 1:
                venue.trade_through(ACME, "9.50", "100")

    venue.after_answer = trade_after_answering
    venue.answer_ticks = 2
    await oms.reconcile()
    venue.after_answer = None
    state = await oms.wait_for("RC7", lambda o: o.is_terminal, 2)
    assert state.status is S.FILLED and not state.anomalies
    assert not [e for e in events_of(oms._journal) if isinstance(e, Restated)]


async def test_a_fill_landing_during_the_fill_query_is_not_a_mismatch(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("RC2", price="9.50"))
    await venue.drain()
    venue.report_faults.drop_next = 1
    venue.trade_through(ACME, "9.50", "50")
    calls = {"n": 0}

    def trade_once(method: str) -> None:
        if method == "fills_since":
            calls["n"] += 1
            if calls["n"] == 1:
                venue.trade_through(ACME, "9.50", "10")

    venue.after_answer = trade_once
    venue.answer_ticks = 50
    report = await oms.reconcile()
    venue.after_answer = None
    assert oms.order("RC2").filled_quantity == 60
    assert not [b for b in report.breaks if b.kind is BreakKind.FILL_MISMATCH]
    assert not oms.kill_state.engaged


async def test_a_real_fill_difference_still_halts(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("RC8", price="9.50"))
    await venue.drain()
    venue._orders["RC8"].filled = D(30)
    report = await oms.reconcile()
    assert [b.kind for b in report.breaks if b.subject == "RC8"] == [
        BreakKind.FILL_MISMATCH
    ]
    assert oms.kill_state.engaged


async def test_a_lost_replace_and_its_fills_are_judged_on_the_new_size(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("RC3", price="9.50"))
    await venue.drain()
    venue.report_faults.drop_next = 2
    await oms.amend("RC3", quantity=D(150))
    venue.trade_through(ACME, "9.50", "120")
    await oms.reconcile()
    state = oms.order("RC3")
    assert (state.quantity, state.filled_quantity) == (150, 120)
    assert state.status is S.PARTIALLY_FILLED and not state.anomalies
    assert not oms.kill_state.engaged


async def test_a_size_change_made_at_the_venue_is_adopted_before_its_fills(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("RC9", price="9.50"))
    await venue.drain()
    venue._orders["RC9"].quantity = D(150)
    venue.report_faults.drop_next = 1
    venue.trade_through(ACME, "9.50", "120")
    await oms.reconcile()
    state = oms.order("RC9")
    assert (state.quantity, state.filled_quantity) == (150, 120)
    assert not state.anomalies and not oms.kill_state.engaged


async def test_a_venue_order_with_another_side_under_our_id_is_not_adopted(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.add_external_order(order("RC4", side=SELL, price="11"))
    state = await oms.submit(order("RC4", price="9.50"))
    assert state.status is S.UNKNOWN
    report = await oms.reconcile()
    assert [b.kind for b in report.breaks if b.subject == "RC4"] == [
        BreakKind.ORPHAN_ORDER
    ]
    assert oms.order("RC4").status is S.UNKNOWN and oms.kill_state.engaged


async def test_a_venue_that_keeps_busted_fills_in_its_history_cannot_rebook_them(
    oms: OrderManager, venue: SimVenue
) -> None:
    venue.retain_busted_fills = True
    await venue.drain()
    await oms.submit(order("RC5", price="9.50"))
    await venue.drain()
    venue.trade_through(ACME, "9.50", "40")
    state = await oms.wait_for("RC5", lambda o: o.filled_quantity == 40, 2)
    venue.bust(state.fills[0].fill_id)
    await oms.wait_for("RC5", lambda o: o.filled_quantity == 0, 2)
    records = len(list(oms._journal.records()))
    await oms.reconcile()
    await oms.reconcile()
    assert oms.order("RC5").filled_quantity == 0
    assert oms.net_position(ACME) == 0
    assert len(list(oms._journal.records())) == records


async def test_a_repeated_bust_reaches_the_journal_once(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    await oms.submit(order("RC6", price="9.50"))
    await venue.drain()
    venue.trade_through(ACME, "9.50", "40")
    state = await oms.wait_for("RC6", lambda o: o.filled_quantity == 40, 2)
    fill_id = state.fills[0].fill_id
    venue.report_faults.duplicate_next = 1
    venue.bust(fill_id)
    await venue.drain()
    busts = [e for e in events_of(oms._journal) if isinstance(e, FillBusted)]
    assert len(busts) == 1


async def test_a_fee_known_only_to_the_query_is_learned_once(
    clock: ManualClock,
) -> None:
    venue = SimVenue(
        clock=clock, instruments=[sim_instrument("ACME")], taker_fee_bps=D(10)
    )
    venue.set_quote(ACME, "9.99", "10.01")
    venue.fee_only_on_query = True
    oms = manager(venue, MemoryJournal(), clock)
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("FE1", price="9.50"))
        await venue.drain()
        venue.trade_through(ACME, "9.50", "40")
        state = await oms.wait_for("FE1", lambda o: o.filled_quantity == 40, 2)
        assert state.fills[0].fee is None
        await oms.reconcile()
        state = oms.order("FE1")
        # 40 at 9.50 is 380, and 10 bps of 380 is 0.38.
        assert state.fills[0].fee == D("0.38")
        assert state.filled_quantity == 40 and not state.anomalies
        records = len(list(oms._journal.records()))
        await oms.reconcile()
        assert len(list(oms._journal.records())) == records
    finally:
        await oms.abort()


# Gross exposure


async def test_gross_values_a_position_at_the_market_not_its_cost(
    clock: ManualClock,
) -> None:
    class CostBasisBroker(SimVenue):
        async def positions(self) -> list[Position]:
            return [
                Position(symbol=OTHER, quantity=D(1000), average_price=D(1)),
            ]

    venue = CostBasisBroker(
        clock=clock, instruments=[sim_instrument("ACME"), sim_instrument("OTHR")]
    )
    venue.set_quote(ACME, "9.99", "10.01")
    venue.set_quote(OTHER, "499", "501")
    oms = manager(venue, MemoryJournal(), clock, gross_config("100000"))
    await oms.start()
    try:
        await venue.drain()
        with pytest.raises(RiskRejected) as caught:
            await oms.submit(order("GR1", quantity="1", price="9.50"))
        # 1000 at a mid of 500 is 500000, whatever the broker paid for them.
        assert caught.value.check == "max_gross_notional"
        assert "gross notional 500010.01" in caught.value.reason
    finally:
        await oms.abort()


async def test_a_held_symbol_that_moves_is_valued_at_its_new_price(
    clock: ManualClock,
) -> None:
    venue = gross_venue(clock, OTHR=1000)
    venue.set_quote(OTHER, "9", "11")
    oms = manager(venue, MemoryJournal(), clock, gross_config("100000"))
    await oms.start()
    try:
        await venue.drain()
        await oms.submit(order("GR2", quantity="1", price="9.50"))
        venue.set_quote(OTHER, "499", "501")
        clock.advance(6)
        with pytest.raises(RiskRejected) as caught:
            await oms.submit(order("GR3", quantity="1", price="9.50"))
        assert caught.value.check == "max_gross_notional"
    finally:
        await oms.abort()


async def test_a_restart_does_not_value_positions_at_old_fill_prices(
    clock: ManualClock, journal_path: Path
) -> None:
    venue = gross_venue(clock)
    journal = SqliteJournal(journal_path)
    config = gross_config("100000")
    oms = manager(venue, journal, clock, config)
    await oms.start()
    await venue.drain()
    await oms.submit(order("GR12", quantity="1000", price="10.01"))
    await venue.drain()
    assert oms.net_position(ACME) == 1000
    await oms.abort()
    venue.set_quote(ACME, "499", "501")
    restarted = manager(venue, journal, clock, config)
    await restarted.start()
    assert restarted._gross_notional() is None
    await restarted.abort()
    journal.close()


async def test_a_held_symbol_with_no_quote_makes_gross_unknown(
    clock: ManualClock,
) -> None:
    venue = gross_venue(clock, OTHR=1000)
    oms = manager(venue, MemoryJournal(), clock, gross_config("100000"))
    await oms.start()
    try:
        await venue.drain()
        with pytest.raises(RiskRejected, match="gross exposure unknown"):
            await oms.submit(order("GR4", quantity="1", price="9.50"))
    finally:
        await oms.abort()


async def test_gross_is_unknown_while_holdings_are_unknown(
    clock: ManualClock,
) -> None:
    class NoAccount(SimVenue):
        """None hides a method from the capability check."""

        positions = None  # type: ignore[assignment]
        balances = None  # type: ignore[assignment]

    venue = NoAccount(clock=clock, instruments=[sim_instrument("ACME")])
    venue.set_quote(ACME, "9.99", "10.01")
    oms = manager(venue, MemoryJournal(), clock, gross_config("1000000"))
    await oms.start()
    try:
        await venue.drain()
        with pytest.raises(RiskRejected, match="gross exposure unknown"):
            await oms.submit(order("GR11", quantity="1", price="9.50"))
    finally:
        await oms.abort()


async def test_many_small_reducers_cannot_walk_past_the_gross_limit(
    clock: ManualClock,
) -> None:
    venue = gross_venue(clock, ACME=-100)
    oms = manager(venue, MemoryJournal(), clock, gross_config("1500"))
    await oms.start()
    try:
        await venue.drain()
        # Reducing 100 short to 1 short needs no room under the limit.
        await oms.submit(order("GR5", quantity="99", price="9.50"))
        # With that working, a second 99 would end long: 1000 + 940.5 + 940.5.
        with pytest.raises(RiskRejected) as caught:
            await oms.submit(order("GR6", quantity="99", price="9.50"))
        assert caught.value.check == "max_gross_notional"
    finally:
        await oms.abort()


async def test_a_pending_amend_counts_at_the_worse_price(
    oms: OrderManager, venue: SimVenue, monkeypatch: pytest.MonkeyPatch
) -> None:
    await venue.drain()
    await oms.submit(order("GR7", quantity="100", price="9.50"))
    await venue.drain()

    async def lost(*_: object, **__: object) -> None:
        raise OSError("response lost")

    monkeypatch.setattr(venue, "amend_order", lost)
    state = await oms.amend("GR7", limit_price=D("9.80"))
    assert state.status is S.UNKNOWN
    assert oms._gross_notional() == D(980)


async def test_gross_refuses_to_add_currencies(clock: ManualClock) -> None:
    venue = gross_venue(clock, TOYO=1000)
    venue.add_instrument(sim_instrument("TOYO", currency="JPY"))
    venue.set_quote(TOYO, "2999", "3001")
    oms = manager(venue, MemoryJournal(), clock, gross_config("100000000"))
    await oms.start()
    try:
        await venue.drain()
        with pytest.raises(RiskRejected, match="gross exposure unknown"):
            await oms.submit(order("GR8", quantity="1", price="9.50"))
    finally:
        await oms.abort()


async def test_an_order_outside_the_declared_limit_currency_is_refused(
    clock: ManualClock,
) -> None:
    venue = gross_venue(clock)
    venue.add_instrument(sim_instrument("TOYO", currency="JPY"))
    venue.set_quote(TOYO, "2999", "3001")
    config = permissive_config(limit_currency="USD")
    oms = manager(venue, MemoryJournal(), clock, config)
    await oms.start()
    try:
        await venue.drain()
        with pytest.raises(RiskRejected) as caught:
            await oms.submit(order("GR9", symbol=TOYO, quantity="1", price="2990"))
        assert caught.value.check == "limit_currency"
        await oms.submit(order("GR10", quantity="1", price="9.50"))
    finally:
        await oms.abort()


async def test_a_zero_quote_is_no_price_for_a_market_order(
    oms: OrderManager, venue: SimVenue
) -> None:
    await venue.drain()
    venue.set_quote(ACME, "0", "0")
    with pytest.raises(RiskRejected) as caught:
        await oms.submit(order("ZQ1", quantity="1000000", price=None))
    assert caught.value.check == "max_order_notional"
    assert venue.attempts["ZQ1"] == 0


@pytest.mark.parametrize("entry", ["ACME@zsim", "acme", " ACME "])
async def test_a_restricted_symbol_matches_however_it_was_spelled(
    venue: SimVenue, clock: ManualClock, entry: str
) -> None:
    config = permissive_config(restricted_symbols=frozenset({entry}))
    oms = manager(venue, MemoryJournal(), clock, config)
    await oms.start()
    try:
        await venue.drain()
        with pytest.raises(RiskRejected) as caught:
            await oms.submit(order("RS1", price="9.50"))
        assert caught.value.check == "restricted"
    finally:
        await oms.abort()


def test_a_delisting_is_judged_on_the_date_at_the_venue() -> None:
    # 23:30 UTC on March 9 is 10:30 on March 10 in Sydney.
    now = datetime(2026, 3, 9, 23, 30, tzinfo=timezone.utc)
    symbol = Symbol(venue="XASX", ticker="BHP")
    instrument = sim_instrument("BHP", delisting_date=date(2026, 3, 10)).model_copy(
        update={"symbol": symbol}
    )
    request = order("DL1", symbol=symbol)
    sydney = RiskContext(request, instrument, None, now, calendar=get_calendar("XASX"))
    assert check_instrument(sydney).reason == "BHP@XASX was delisted"
    unknown = replace(sydney, calendar=None)
    assert "no trading calendar" in check_instrument(unknown).reason


# Durable operator state and recovery


async def test_tightened_limits_cannot_silently_revert_on_restart(
    venue: SimVenue, clock: ManualClock, journal_path: Path
) -> None:
    journal = SqliteJournal(journal_path)
    tight = permissive_config(allow_short=False)
    oms = manager(venue, journal, clock)
    await oms.start()
    await oms.update_risk_config(tight, operator="pat")
    await oms.abort()
    with pytest.raises(ConfigError, match="latest journalled"):
        await manager(venue, journal, clock).start()
    restarted = manager(venue, journal, clock, tight)
    await restarted.start()
    await restarted.abort()
    journal.close()


async def test_an_outside_position_change_can_be_cleared_and_stays_cleared(
    venue: SimVenue, clock: ManualClock, journal_path: Path
) -> None:
    journal = SqliteJournal(journal_path)
    oms = manager(venue, journal, clock)
    await oms.start()
    await venue.drain()
    venue.add_external_fill(ACME, OrderSide.BUY, D(50))
    report = await oms.reconcile()
    assert [b.kind for b in report.breaks] == [BreakKind.POSITION_MISMATCH]
    assert oms.kill_state.engaged
    await oms.adjust_position(ACME, D(50), operator="pat", reason="2:1 split")
    await oms.release_kill("explained", operator="pat")
    report = await oms.reconcile()
    assert not report.breaks and not oms.kill_state.engaged
    assert oms.net_position(ACME) == 50
    await oms.abort()
    restarted = manager(venue, journal, clock)
    report = await restarted.start()
    assert not report.breaks and restarted.net_position(ACME) == 50
    await restarted.abort()
    journal.close()


async def test_the_cli_clears_a_position_break_in_a_running_process(
    venue: SimVenue, clock: ManualClock, journal_path: Path
) -> None:
    journal = SqliteJournal(journal_path)
    oms = manager(venue, journal, clock)
    await oms.start()
    await venue.drain()
    venue.add_external_fill(ACME, OrderSide.SELL, D(7))
    result = await asyncio.to_thread(
        subprocess.run,
        [
            sys.executable,
            "-m",
            "ismet",
            "position",
            "adjust",
            str(ACME),
            "--journal",
            str(journal_path),
            "--delta",
            "-7",
            "--reason",
            "transfer out",
            "--operator",
            "pat",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    report = await oms.reconcile()
    assert not report.breaks and oms.net_position(ACME) == -7
    await oms.abort()
    journal.close()


@pytest.mark.parametrize("named", [{}, {"operator": ""}, {"operator": "   "}])
async def test_a_forged_position_adjustment_with_no_operator_is_ignored(
    venue: SimVenue, clock: ManualClock, named: dict[str, str]
) -> None:
    journal = MemoryJournal()
    oms = manager(venue, journal, clock)
    await oms.start()
    journal.append(
        RecordKind.POSITION_ADJUSTED,
        json.dumps({"symbol": str(ACME), "delta": "5", **named}),
    )
    await oms.reconcile()
    assert oms.net_position(ACME) == 0
    await oms.abort()


async def test_an_orphan_fill_halts_once_and_a_release_is_final(
    venue: SimVenue, clock: ManualClock, journal_path: Path
) -> None:
    journal = SqliteJournal(journal_path)
    oms = manager(venue, journal, clock)
    await oms.start()
    await venue.drain()
    venue.add_external_order(order("EXT1", quantity="1", price="9.50"))
    venue.report_faults.drop_next = 1
    venue.trade_through(ACME, "9.50", "1")
    report = await oms.reconcile()
    assert BreakKind.ORPHAN_FILL in [b.kind for b in report.breaks]
    assert oms.kill_state.engaged
    await oms.adjust_position(ACME, D(1), operator="pat", reason="other terminal")
    await oms.release_kill("explained", operator="pat")
    report = await oms.reconcile()
    assert BreakKind.ORPHAN_FILL not in [b.kind for b in report.breaks]
    assert not oms.kill_state.engaged
    await oms.abort()
    restarted = manager(venue, journal, clock)
    report = await restarted.start()
    assert BreakKind.ORPHAN_FILL not in [b.kind for b in report.breaks]
    assert not restarted.kill_state.engaged
    await restarted.abort()
    journal.close()


async def test_a_restart_sweeps_from_the_last_clean_sweep_not_the_last_record(
    clock: ManualClock, journal_path: Path
) -> None:
    class NoAccount(SimVenue):
        """No position check can rescue the fill, as on a FIX venue."""

        positions = None  # type: ignore[assignment]
        balances = None  # type: ignore[assignment]

    venue = NoAccount(clock=clock, instruments=[sim_instrument("ACME")])
    venue.set_quote(ACME, "9.99", "10.01")
    journal = SqliteJournal(journal_path)
    oms = manager(venue, journal, clock, fill_lookback=3600.0, assume_flat=True)
    await oms.start()
    await venue.drain()
    await oms.submit(order("SW1", price="9.50"))
    await venue.drain()
    venue.report_faults.drop_next = 1
    venue.trade_through(ACME, "9.50", "40")
    await oms.cancel("SW1")
    await oms.wait_for("SW1", lambda o: o.is_terminal, 2)
    assert oms.order("SW1").filled_quantity == 0
    clock.advance(3 * 3600)
    venue.fail_queries = 2
    await oms.reconcile()
    assert oms.order("SW1").filled_quantity == 0
    await oms.abort()
    venue.fail_queries = 0
    restarted = manager(venue, journal, clock, fill_lookback=3600.0, assume_flat=True)
    await restarted.start()
    assert restarted.order("SW1").filled_quantity == 40
    assert restarted.net_position(ACME) == 40
    await restarted.abort()
    journal.close()


async def test_a_restart_does_not_search_older_than_the_durable_mark_needs(
    venue: SimVenue, clock: ManualClock, journal_path: Path
) -> None:
    journal = SqliteJournal(journal_path)
    oms = manager(venue, journal, clock, fill_lookback=3600.0)
    await oms.start()
    await venue.drain()
    clock.advance(3600)
    marked_at = clock.now()
    await oms.reconcile()
    await oms.abort()
    clock.advance(7200)
    asked: list[datetime] = []
    fills_since = venue.fills_since

    async def spy(since: datetime, symbol: Symbol | None = None) -> list[Fill]:
        if symbol is None:
            asked.append(since)
        return await fills_since(since, symbol)

    venue.fills_since = spy  # type: ignore[method-assign]
    restarted = manager(venue, journal, clock, fill_lookback=3600.0)
    await restarted.start()
    assert asked[0] == marked_at - timedelta(hours=1)
    await restarted.abort()
    journal.close()


async def test_a_fill_the_journal_missed_is_written_before_what_followed_it(
    venue: SimVenue, clock: ManualClock
) -> None:
    class Flaky(MemoryJournal):
        failing = False

        def append(self, *args: object, **kw: object):  # type: ignore[no-untyped-def]
            if self.failing and args[0] is RecordKind.ORDER_EVENT:
                from ismet.errors import JournalError

                raise JournalError("locked")
            return super().append(*args, **kw)  # type: ignore[arg-type]

    journal = Flaky()
    oms = manager(venue, journal, clock)
    await oms.start()
    await venue.drain()
    await oms.submit(order("UJ1", price="9.50"))
    await venue.drain()
    journal.failing = True
    venue.trade_through(ACME, "9.50", "40")
    await oms.wait_for("UJ1", lambda o: o.filled_quantity == 40, 2)
    journal.failing = False
    await oms.cancel("UJ1")
    await oms.wait_for("UJ1", lambda o: o.is_terminal, 2)
    kinds = [e.kind for e in events_of(journal)]
    assert kinds.index("fill") < kinds.index("canceled")
    await oms.abort()
    restarted = manager(venue, journal, clock)
    await restarted.start()
    assert restarted.order("UJ1").filled_quantity == 40
    await restarted.abort()


async def test_positions_split_across_rows_are_one_position(
    clock: ManualClock,
) -> None:
    class TwoLots(SimVenue):
        async def positions(self) -> list[Position]:
            return [
                Position(symbol=ACME, quantity=D(30)),
                Position(symbol=ACME, quantity=D(70)),
            ]

    venue = TwoLots(clock=clock, instruments=[sim_instrument("ACME")])
    venue.set_quote(ACME, "9.99", "10.01")
    oms = manager(venue, MemoryJournal(), clock)
    report = await oms.start()
    assert oms.net_position(ACME) == 100 and not report.breaks
    await oms.abort()


# The journal


def test_verify_tolerates_a_commit_that_lands_while_it_reads(
    journal_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = SqliteJournal(journal_path)
    other = SqliteJournal(journal_path)
    journal.append(RecordKind.ANOMALY, "{}")
    read_anchor = journal._read_anchor

    def commit_then_read() -> tuple[int, str] | None:
        anchor = read_anchor()
        other.append(RecordKind.ANOMALY, "{}")
        return anchor

    monkeypatch.setattr(journal, "_read_anchor", commit_then_read)
    assert journal.verify().ok
    monkeypatch.setattr(journal, "_read_anchor", read_anchor)
    other.append(RecordKind.ANOMALY, "{}")
    assert journal.verify().ok
    journal.close()
    other.close()


def test_an_export_can_be_verified_on_its_own(journal_path: Path) -> None:
    journal = SqliteJournal(journal_path)
    journal.append(
        RecordKind.ORDER_EVENT, '{"a": 1}', client_order_id="C", unique_intent=True
    )
    journal.append(RecordKind.ANOMALY, '{"b":   2}')
    stream = io.StringIO()
    assert export_jsonl(journal, stream) == 2
    lines = stream.getvalue().splitlines()
    result = verify_jsonl(lines)
    assert result.ok and result.head_hash == journal.head_hash()
    tampered = lines[0].replace('"a\\": 1', '"a\\": 2')
    assert tampered != lines[0]
    assert not verify_jsonl([tampered, lines[1]]).ok
    journal.close()


# The books


def fill_of(cid: str, fill_id: str, side: OrderSide, price: str, quantity: str = "1"):  # type: ignore[no-untyped-def]
    return Fill(
        fill_id=fill_id,
        client_order_id=cid,
        symbol=ACME,
        side=side,
        price=price,
        quantity=quantity,
        ts=TS,
    )


def test_the_same_execution_id_on_two_orders_is_two_fills() -> None:
    ledger = PositionLedger()
    ledger.apply(fill_of("A", "1001", OrderSide.BUY, "10"))
    ledger.apply(fill_of("B", "1001", OrderSide.BUY, "11"))
    assert ledger.quantity == 2


def test_a_fill_re_reported_with_other_terms_is_refused() -> None:
    ledger = PositionLedger()
    ledger.apply(fill_of("A", "1", OrderSide.BUY, "10"))
    ledger.apply(fill_of("A", "1", OrderSide.BUY, "10"))
    assert ledger.quantity == 1
    with pytest.raises(ValidationError, match="different terms"):
        ledger.apply(fill_of("A", "1", OrderSide.BUY, "11"))


def test_same_time_fills_keep_their_order_rather_than_sorting_by_id() -> None:
    ledger = build_ledger(
        [
            fill_of("A", "9", OrderSide.BUY, "10"),
            fill_of("A", "10", OrderSide.BUY, "20"),
            fill_of("A", "11", SELL, "30"),
        ]
    )
    # FIFO sells the lot bought at 10 for 30, a gain of 20. The lot at 20 stays open.
    assert ledger.realized_pnl == 20
    assert [lot.price for lot in ledger.lots] == [20]


def test_a_valuation_says_when_fees_are_unknown() -> None:
    ledger = PositionLedger()
    ledger.apply(fill_of("A", "1", OrderSide.BUY, "10"))
    valuation = value_books({"acme": (ledger, "USD", D(11))}, currency="USD", rates={})
    assert valuation.fills_with_unknown_fee == 1


# The conformance suite


def conformance_order(cid: str):  # type: ignore[no-untyped-def]
    return order(cid, price="9.00")


async def test_a_dead_execution_stream_fails_conformance() -> None:
    class DeadStream(SimVenue):
        def stream_executions(self):  # type: ignore[no-untyped-def]
            async def empty():  # type: ignore[no-untyped-def]
                return
                yield

            return empty()

    sim = DeadStream(instruments=[sim_instrument("ACME")])
    sim.set_quote(ACME, "9.99", "10.01")
    report = await run_trading_conformance(sim, conformance_order)
    assert "stream_reports_new_order" in {c.name for c in report.failures}


async def test_a_plain_refusal_of_a_duplicate_id_fails_conformance() -> None:
    from ismet.errors import DuplicateOrderId, VenueError

    class PlainRefusal(SimVenue):
        async def place_order(self, request):  # type: ignore[no-untyped-def]
            try:
                return await super().place_order(request)
            except DuplicateOrderId as exc:
                raise VenueError(str(exc), code="REJECTED") from None

    sim = PlainRefusal(instruments=[sim_instrument("ACME")])
    sim.set_quote(ACME, "9.99", "10.01")
    report = await run_trading_conformance(sim, conformance_order)
    assert "duplicate_client_id_refused" in {c.name for c in report.failures}
