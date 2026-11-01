"""The order state machine, exhaustively and as a stateful property."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from ismet.errors import OrderStateError
from ismet.execution.state import (
    TRANSITIONS,
    OrderState,
    apply_event,
    new_order,
    replay,
)
from ismet.models import Symbol
from ismet.models.orders import (
    Acknowledged,
    Canceled,
    CancelRejected,
    CancelRequested,
    Expired,
    Fill,
    FillBusted,
    Filled,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OutcomeUnknown,
    Rejected,
    Replaced,
    ReplaceRejected,
    ReplaceRequested,
    Restated,
    SendStarted,
    SubmitIntent,
    dump_order_event,
    event_kinds,
    parse_order_event,
)

S = OrderStatus
D = Decimal
TS = datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)
SYM = Symbol(venue="XSIM", ticker="ACME")
CID = "C1"
REQ = OrderRequest(
    client_order_id=CID,
    symbol=SYM,
    side=OrderSide.BUY,
    quantity="100",
    limit_price="10",
)
BASE = {"client_order_id": CID, "ts": TS}
INTENT = SubmitIntent(**BASE, request=REQ)


def fill(fill_id: str = "F1", quantity: str = "40", price: str = "10") -> Filled:
    return Filled(
        **BASE,
        fill=Fill(
            fill_id=fill_id,
            client_order_id=CID,
            symbol=SYM,
            side=OrderSide.BUY,
            price=price,
            quantity=quantity,
            ts=TS,
        ),
    )


LIFECYCLE = {
    "send_started": SendStarted(**BASE),
    "ack": Acknowledged(**BASE, venue_order_id="V1"),
    "reject": Rejected(**BASE, reason="no"),
    "outcome_unknown": OutcomeUnknown(**BASE, reason="lost"),
    "cancel_requested": CancelRequested(**BASE),
    "canceled": Canceled(**BASE),
    "cancel_rejected": CancelRejected(**BASE, reason="late"),
    "replace_requested": ReplaceRequested(**BASE, quantity="50"),
    "replaced": Replaced(**BASE, quantity="50"),
    "replace_rejected": ReplaceRejected(**BASE, reason="no"),
    "expired": Expired(**BASE),
}


def in_status(status: OrderStatus) -> OrderState:
    from dataclasses import replace

    return replace(new_order(INTENT), status=status)


def test_every_lifecycle_kind_is_in_the_table_and_every_kind_is_handled() -> None:
    assert set(LIFECYCLE) == set(TRANSITIONS)
    assert set(event_kinds()) == set(TRANSITIONS) | {
        "intent",
        "fill",
        "bust",
        "restated",
    }


@pytest.mark.parametrize("kind", sorted(LIFECYCLE))
@pytest.mark.parametrize("status", list(OrderStatus))
def test_every_status_and_event_pair_follows_the_table(
    kind: str, status: OrderStatus
) -> None:
    before = in_status(status)
    after = apply_event(before, LIFECYCLE[kind])
    target = TRANSITIONS[kind].get(status)
    if target is None:
        assert after.status is status
        assert any("illegal transition" in a for a in after.anomalies)
    else:
        assert not after.anomalies
        if isinstance(target, OrderStatus):
            assert after.status is target


@pytest.mark.parametrize("terminal", [S.FILLED, S.CANCELED, S.REJECTED, S.EXPIRED])
@pytest.mark.parametrize("kind", sorted(LIFECYCLE))
def test_lifecycle_events_never_reopen_a_terminal_order(
    terminal: OrderStatus, kind: str
) -> None:
    assert apply_event(in_status(terminal), LIFECYCLE[kind]).status.is_terminal


def test_full_lifecycle_and_exact_average_price() -> None:
    state = replay(
        [
            INTENT,
            SendStarted(**BASE),
            Acknowledged(**BASE, venue_order_id="V1"),
            fill("F1", "30", "10.00"),
            fill("F2", "70", "10.10"),
        ]
    )[CID]
    assert state.status is S.FILLED and state.is_terminal
    assert state.filled_quantity == 100
    assert state.filled_notional == D("1007.00")
    assert state.average_price == D("10.07")
    assert state.leaves_quantity == 0
    assert state.venue_order_id == "V1"


def test_replaying_a_duplicate_fill_counts_it_once() -> None:
    state = apply_event(apply_event(new_order(INTENT), SendStarted(**BASE)), fill())
    assert apply_event(state, fill()) == state


def test_conflicting_refill_is_not_counted_and_is_surfaced() -> None:
    state = apply_event(apply_event(new_order(INTENT), SendStarted(**BASE)), fill())
    after = apply_event(state, fill("F1", "41"))
    assert after.filled_quantity == 40
    assert "different terms" in after.anomalies[-1]


def test_fill_after_cancel_is_counted_and_surfaced() -> None:
    state = in_status(S.CANCELED)
    after = apply_event(state, fill())
    assert after.filled_quantity == 40
    assert after.status is S.CANCELED
    assert "after terminal" in after.anomalies[-1]
    assert after.leaves_quantity == 0


def test_fill_before_send_is_counted_and_surfaced() -> None:
    after = apply_event(new_order(INTENT), fill())
    assert after.filled_quantity == 40
    assert "before the send" in after.anomalies[-1]


def test_overfill_is_counted_and_surfaced() -> None:
    state = apply_event(new_order(INTENT), SendStarted(**BASE))
    after = apply_event(state, fill("F1", "150"))
    assert after.status is S.FILLED
    assert after.filled_quantity == 150
    assert any("overfill" in a for a in after.anomalies)


def test_fill_while_status_unknown_keeps_it_unknown_until_full() -> None:
    state = in_status(S.UNKNOWN)
    partly = apply_event(state, fill("F1", "40"))
    assert partly.status is S.UNKNOWN
    assert apply_event(partly, fill("F2", "60")).status is S.FILLED


def test_pending_cancel_survives_a_partial_fill() -> None:
    state = in_status(S.PENDING_CANCEL)
    assert apply_event(state, fill()).status is S.PENDING_CANCEL


def test_cancel_rejected_returns_to_the_status_the_fills_imply() -> None:
    state = apply_event(in_status(S.PENDING_CANCEL), fill())
    assert apply_event(state, CancelRejected(**BASE, reason="x")).status is (
        S.PARTIALLY_FILLED
    )
    assert (
        apply_event(
            in_status(S.PENDING_CANCEL), CancelRejected(**BASE, reason="x")
        ).status
        is S.ACCEPTED
    )


def test_bust_reverses_a_fill_and_asks_the_venue_after_a_full_fill() -> None:
    state = apply_event(in_status(S.ACCEPTED), fill("F1", "100"))
    assert state.status is S.FILLED
    after = apply_event(state, FillBusted(**BASE, fill_id="F1"))
    assert after.filled_quantity == 0 and after.filled_notional == 0
    assert after.status is S.UNKNOWN
    partial = apply_event(in_status(S.ACCEPTED), fill("F2"))
    assert partial.status is S.PARTIALLY_FILLED
    assert apply_event(partial, FillBusted(**BASE, fill_id="F2")).status is S.ACCEPTED
    missing = apply_event(partial, FillBusted(**BASE, fill_id="nope"))
    assert "unknown fill" in missing.anomalies[-1]


def test_replace_changes_terms_only_when_the_venue_confirms() -> None:
    state = in_status(S.ACCEPTED)
    pending = apply_event(
        state, ReplaceRequested(**BASE, quantity="50", limit_price="9.5")
    )
    assert pending.status is S.PENDING_REPLACE and pending.quantity == 100
    done = apply_event(pending, Replaced(**BASE))
    assert done.status is S.ACCEPTED
    assert done.quantity == 50 and done.limit_price == D("9.5")
    refused = apply_event(pending, ReplaceRejected(**BASE, reason="no"))
    assert refused.status is S.ACCEPTED and refused.quantity == 100
    assert refused.pending_quantity is None


def test_replace_down_to_filled_quantity_completes_the_order() -> None:
    state = apply_event(in_status(S.ACCEPTED), fill("F1", "50"))
    pending = apply_event(state, ReplaceRequested(**BASE, quantity="50"))
    assert apply_event(pending, Replaced(**BASE)).status is S.FILLED


def test_restated_is_authoritative_and_flags_contradiction() -> None:
    after = apply_event(
        in_status(S.UNKNOWN),
        Restated(**BASE, status=S.ACCEPTED, venue_order_id="V9"),
    )
    assert after.status is S.ACCEPTED and after.venue_order_id == "V9"
    reopened = apply_event(in_status(S.CANCELED), Restated(**BASE, status=S.ACCEPTED))
    assert reopened.status is S.ACCEPTED
    assert "restated terminal" in reopened.anomalies[-1]


def test_duplicate_intent_and_foreign_event_are_rejected() -> None:
    assert "duplicate intent" in apply_event(new_order(INTENT), INTENT).anomalies[-1]
    other = {"client_order_id": "C2", "ts": TS}
    with pytest.raises(OrderStateError):
        apply_event(new_order(INTENT), SendStarted(**other))
    with pytest.raises(OrderStateError, match="no intent"):
        replay([SendStarted(**BASE)])


@pytest.mark.parametrize(
    "event",
    [
        INTENT,
        fill(),
        *LIFECYCLE.values(),
        FillBusted(**BASE, fill_id="F1"),
        Restated(**BASE, status=S.FILLED),
    ],
)
def test_every_event_round_trips_through_journal_json(event: object) -> None:
    text = dump_order_event(event)  # type: ignore[arg-type]
    assert parse_order_event(text) == event


def test_replace_requested_must_change_something() -> None:
    with pytest.raises(ValueError):
        ReplaceRequested(**BASE)


class OrderLifecycle(RuleBasedStateMachine):
    """Any sequence of events keeps the money consistent with the fills."""

    def __init__(self) -> None:
        super().__init__()
        self.state = apply_event(new_order(INTENT), SendStarted(**BASE))
        self.next_fill = 0
        self.expected_live: dict[str, tuple[Decimal, Decimal]] = {}

    @rule(kind=st.sampled_from(sorted(LIFECYCLE)))
    def lifecycle(self, kind: str) -> None:
        self.state = apply_event(self.state, LIFECYCLE[kind])

    @rule(
        quantity=st.integers(min_value=1, max_value=60),
        price=st.decimals(min_value=-50, max_value=200, places=2),
    )
    def new_fill(self, quantity: int, price: Decimal) -> None:
        self.next_fill += 1
        fill_id = f"F{self.next_fill}"
        self.state = apply_event(self.state, fill(fill_id, str(quantity), str(price)))
        self.expected_live[fill_id] = (D(quantity), price)

    @rule()
    def duplicate_fill(self) -> None:
        if self.state.fills:
            event = Filled(**BASE, fill=self.state.fills[0])
            self.state = apply_event(self.state, event)

    @rule()
    def bust_first(self) -> None:
        if self.state.fills:
            fill_id = self.state.fills[0].fill_id
            self.state = apply_event(self.state, FillBusted(**BASE, fill_id=fill_id))
            del self.expected_live[fill_id]

    @rule(status=st.sampled_from(list(OrderStatus)))
    def restate(self, status: OrderStatus) -> None:
        self.state = apply_event(self.state, Restated(**BASE, status=status))

    @invariant()
    def money_matches_the_surviving_fills(self) -> None:
        quantities = [q for q, _ in self.expected_live.values()]
        notional = [q * p for q, p in self.expected_live.values()]
        assert self.state.filled_quantity == sum(quantities, D(0))
        assert self.state.filled_notional == sum(notional, D(0))
        assert {f.fill_id for f in self.state.fills} == set(self.expected_live)

    @invariant()
    def leaves_is_never_negative_and_zero_when_terminal(self) -> None:
        assert self.state.leaves_quantity >= 0
        if self.state.is_terminal:
            assert self.state.leaves_quantity == 0


TestOrderLifecycle = OrderLifecycle.TestCase
TestOrderLifecycle.settings = settings(max_examples=150, stateful_step_count=40)
