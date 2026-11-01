"""The order state machine: a pure reducer from events to state.

:data:`TRANSITIONS` states, for every status and every lifecycle event, the status
that results. A pair missing from the table is an illegal transition. The reducer
never raises on one and never drops it: venue reports are facts that must not be
lost, so the state keeps its status, records an anomaly, and the order manager
surfaces it. Money is handled separately from status, so a fill is always counted
even when its arrival contradicts the status (a fill after a cancel is real).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from decimal import Decimal
from enum import Enum

from ismet.errors import OrderStateError
from ismet.models.decimals import add, mul, ratio, sub
from ismet.models.orders import (
    Acknowledged,
    Canceled,
    CancelRejected,
    CancelRequested,
    Expired,
    Fill,
    FillBusted,
    Filled,
    OrderEvent,
    OrderRequest,
    OrderStatus,
    OutcomeUnknown,
    Rejected,
    Replaced,
    ReplaceRejected,
    ReplaceRequested,
    Restated,
    SendStarted,
    SubmitIntent,
)

S = OrderStatus


class _Next(Enum):
    """A target decided by the fill count rather than fixed by the table."""

    WORKING = "working"


_LIVE_WORKING = (S.ACCEPTED, S.PARTIALLY_FILLED)
_PENDING = (S.PENDING_CANCEL, S.PENDING_REPLACE)
_ALL_LIVE = (S.SUBMITTING, S.UNKNOWN, *_LIVE_WORKING, *_PENDING)

Target = OrderStatus | _Next

TRANSITIONS: dict[str, dict[OrderStatus, Target]] = {
    "send_started": {S.PENDING_SUBMIT: S.SUBMITTING},
    "ack": {
        S.SUBMITTING: _Next.WORKING,
        S.UNKNOWN: _Next.WORKING,
        S.ACCEPTED: S.ACCEPTED,
        S.PARTIALLY_FILLED: S.PARTIALLY_FILLED,
        S.PENDING_CANCEL: S.PENDING_CANCEL,
        S.PENDING_REPLACE: S.PENDING_REPLACE,
        S.FILLED: S.FILLED,
        S.CANCELED: S.CANCELED,
        S.EXPIRED: S.EXPIRED,
    },
    "reject": {
        S.PENDING_SUBMIT: S.REJECTED,
        S.SUBMITTING: S.REJECTED,
        S.UNKNOWN: S.REJECTED,
        S.REJECTED: S.REJECTED,
    },
    "outcome_unknown": {
        S.SUBMITTING: S.UNKNOWN,
        S.UNKNOWN: S.UNKNOWN,
        S.ACCEPTED: S.UNKNOWN,
        S.PARTIALLY_FILLED: S.UNKNOWN,
        S.PENDING_CANCEL: S.UNKNOWN,
        S.PENDING_REPLACE: S.UNKNOWN,
    },
    "cancel_requested": {
        S.ACCEPTED: S.PENDING_CANCEL,
        S.PARTIALLY_FILLED: S.PENDING_CANCEL,
        S.PENDING_CANCEL: S.PENDING_CANCEL,
        S.PENDING_REPLACE: S.PENDING_CANCEL,
        S.SUBMITTING: S.SUBMITTING,
        S.UNKNOWN: S.UNKNOWN,
    },
    "canceled": {
        **dict.fromkeys(_ALL_LIVE, S.CANCELED),
        S.CANCELED: S.CANCELED,
    },
    "cancel_rejected": {
        S.PENDING_CANCEL: _Next.WORKING,
        S.ACCEPTED: S.ACCEPTED,
        S.PARTIALLY_FILLED: S.PARTIALLY_FILLED,
        S.PENDING_REPLACE: S.PENDING_REPLACE,
        S.UNKNOWN: S.UNKNOWN,
        S.SUBMITTING: S.SUBMITTING,
        S.FILLED: S.FILLED,
        S.CANCELED: S.CANCELED,
        S.REJECTED: S.REJECTED,
        S.EXPIRED: S.EXPIRED,
    },
    "replace_requested": {
        S.ACCEPTED: S.PENDING_REPLACE,
        S.PARTIALLY_FILLED: S.PENDING_REPLACE,
    },
    "replaced": {
        S.PENDING_REPLACE: _Next.WORKING,
        S.PENDING_CANCEL: S.PENDING_CANCEL,
        S.UNKNOWN: _Next.WORKING,
        # A repeated report, or a change made at the venue: the new terms are a fact.
        S.ACCEPTED: _Next.WORKING,
        S.PARTIALLY_FILLED: _Next.WORKING,
    },
    "replace_rejected": {
        S.PENDING_REPLACE: _Next.WORKING,
        S.PENDING_CANCEL: S.PENDING_CANCEL,
        S.UNKNOWN: S.UNKNOWN,
        S.FILLED: S.FILLED,
        S.CANCELED: S.CANCELED,
        S.EXPIRED: S.EXPIRED,
    },
    "expired": {
        **dict.fromkeys(_ALL_LIVE, S.EXPIRED),
        S.EXPIRED: S.EXPIRED,
    },
}
"""Lifecycle transitions. ``intent``, ``fill``, ``bust``, and ``restated`` are not
listed because they do not depend on this table: an intent creates the order, fills
and busts are counted from quantities, and a restatement is the venue's own word.
"""


@dataclass(frozen=True)
class OrderState:
    """Everything known about one order, derived only from its events.

    ``quantity`` and ``limit_price`` are the current terms, which a replace can
    change. ``filled_notional`` is the exact sum of price times quantity, and it is
    the source of truth behind :attr:`average_price`.
    """

    request: OrderRequest
    status: OrderStatus = S.PENDING_SUBMIT
    quantity: Decimal = Decimal(0)
    limit_price: Decimal | None = None
    venue_order_id: str | None = None
    filled_quantity: Decimal = Decimal(0)
    filled_notional: Decimal = Decimal(0)
    fills: tuple[Fill, ...] = ()
    pending_quantity: Decimal | None = None
    pending_limit_price: Decimal | None = None
    reject_reason: str | None = None
    reject_code: str | None = None
    anomalies: tuple[str, ...] = ()

    @property
    def client_order_id(self) -> str:
        return self.request.client_order_id

    @property
    def is_terminal(self) -> bool:
        return self.status.is_terminal

    @property
    def is_open(self) -> bool:
        """Whether this order may still execute, including while status is unknown."""
        return not self.is_terminal

    @property
    def leaves_quantity(self) -> Decimal:
        """Quantity that may still execute. Zero once terminal or overfilled."""
        if self.is_terminal:
            return Decimal(0)
        return max(sub(self.quantity, self.filled_quantity), Decimal(0))

    @property
    def average_price(self) -> Decimal | None:
        if self.filled_quantity == 0:
            return None
        return ratio(self.filled_notional, self.filled_quantity)


def new_order(intent: SubmitIntent) -> OrderState:
    """The state an order starts in."""
    request = intent.request
    return OrderState(
        request=request,
        quantity=request.quantity,
        limit_price=request.limit_price,
    )


def _working_status(state: OrderState) -> OrderStatus:
    if state.filled_quantity >= state.quantity:
        return S.FILLED
    return S.PARTIALLY_FILLED if state.filled_quantity > 0 else S.ACCEPTED


def _note(state: OrderState, message: str) -> OrderState:
    return replace(state, anomalies=(*state.anomalies, message))


def _apply_fill(state: OrderState, fill: Fill) -> OrderState:
    for known in state.fills:
        if known.fill_id == fill.fill_id:
            if known == fill:
                return state  # the same report delivered twice
            return _note(state, f"fill {fill.fill_id} re-reported with different terms")
    updated = replace(
        state,
        fills=(*state.fills, fill),
        filled_quantity=add(state.filled_quantity, fill.quantity),
        filled_notional=add(state.filled_notional, mul(fill.price, fill.quantity)),
    )
    if updated.filled_quantity > updated.quantity:
        updated = _note(
            updated,
            f"overfill: {updated.filled_quantity} filled against {updated.quantity}",
        )
    if state.status is S.PENDING_SUBMIT:
        updated = _note(updated, f"fill {fill.fill_id} before the send started")
    elif state.status in (S.CANCELED, S.EXPIRED, S.REJECTED):
        updated = _note(
            updated, f"fill {fill.fill_id} after terminal status {state.status.value}"
        )
    if updated.filled_quantity >= updated.quantity:
        return replace(updated, status=S.FILLED)
    if state.status in (S.ACCEPTED, S.SUBMITTING):
        return replace(updated, status=S.PARTIALLY_FILLED)
    return updated


def _apply_bust(state: OrderState, event: FillBusted) -> OrderState:
    target = next((f for f in state.fills if f.fill_id == event.fill_id), None)
    if target is None:
        return _note(state, f"bust of unknown fill {event.fill_id}")
    updated = replace(
        state,
        fills=tuple(f for f in state.fills if f.fill_id != event.fill_id),
        filled_quantity=sub(state.filled_quantity, target.quantity),
        filled_notional=sub(state.filled_notional, mul(target.price, target.quantity)),
    )
    if state.status is S.FILLED:
        # Whether the remainder is working again is the venue's call, so ask it.
        return replace(updated, status=S.UNKNOWN)
    if state.status is S.PARTIALLY_FILLED and updated.filled_quantity == 0:
        return replace(updated, status=S.ACCEPTED)
    return updated


def _apply_restated(state: OrderState, event: Restated) -> OrderState:
    updated = state
    if state.is_terminal and state.status is not event.status:
        updated = _note(
            updated,
            f"venue restated terminal {state.status.value} as {event.status.value}",
        )
    return replace(
        updated,
        status=event.status,
        venue_order_id=event.venue_order_id or state.venue_order_id,
    )


def _transition(
    state: OrderState, kind: str, table: dict[OrderStatus, Target]
) -> OrderState:
    target = table.get(state.status)
    if target is None:
        return _note(state, f"illegal transition: {kind} in {state.status.value}")
    status = _working_status(state) if isinstance(target, _Next) else target
    return replace(state, status=status)


def apply_event(state: OrderState, event: OrderEvent) -> OrderState:
    """Return the state after ``event``. Pure; ``state`` is not changed."""
    if event.client_order_id != state.client_order_id:
        raise OrderStateError(
            f"event for {event.client_order_id} applied to {state.client_order_id}"
        )
    if isinstance(event, SubmitIntent):
        return _note(state, "duplicate intent for an existing order")
    if isinstance(event, Filled):
        return _apply_fill(state, event.fill)
    if isinstance(event, FillBusted):
        return _apply_bust(state, event)
    if isinstance(event, Restated):
        return _apply_restated(state, event)

    next_state = _transition(state, event.kind, TRANSITIONS[event.kind])
    if isinstance(event, Acknowledged):
        return replace(next_state, venue_order_id=event.venue_order_id)
    if isinstance(event, Rejected):
        return replace(next_state, reject_reason=event.reason, reject_code=event.code)
    if isinstance(event, ReplaceRequested):
        return replace(
            next_state,
            pending_quantity=event.quantity,
            pending_limit_price=event.limit_price,
        )
    if isinstance(event, Replaced):
        return _apply_replaced(state, next_state, event)
    if isinstance(event, ReplaceRejected):
        return replace(next_state, pending_quantity=None, pending_limit_price=None)
    if isinstance(event, CancelRejected):
        return replace(next_state, reject_reason=event.reason, reject_code=event.code)
    if isinstance(
        event, SendStarted | OutcomeUnknown | CancelRequested | Canceled | Expired
    ):
        return next_state
    raise OrderStateError(f"unhandled event kind {event.kind}")  # pragma: no cover


def _apply_replaced(
    state: OrderState, next_state: OrderState, event: Replaced
) -> OrderState:
    if state.status not in TRANSITIONS["replaced"]:
        return next_state  # an order that has ended keeps the terms it ended with
    quantity = event.quantity or state.pending_quantity or state.quantity
    limit_price = (
        event.limit_price
        if event.limit_price is not None
        else (
            state.pending_limit_price
            if state.pending_limit_price is not None
            else state.limit_price
        )
    )
    updated = replace(
        next_state,
        quantity=quantity,
        limit_price=limit_price,
        pending_quantity=None,
        pending_limit_price=None,
        venue_order_id=event.venue_order_id or state.venue_order_id,
    )
    if next_state.status in _LIVE_WORKING or next_state.status is S.FILLED:
        return replace(updated, status=_working_status(updated))
    return updated


def replay(events: Iterable[OrderEvent]) -> dict[str, OrderState]:
    """Rebuild every order from an event history, in journal order."""
    orders: dict[str, OrderState] = {}
    for event in events:
        cid = event.client_order_id
        if isinstance(event, SubmitIntent) and cid not in orders:
            orders[cid] = new_order(event)
        elif cid in orders:
            orders[cid] = apply_event(orders[cid], event)
        else:
            raise OrderStateError(f"event {event.kind} for {cid} has no intent")
    return orders


__all__ = ["TRANSITIONS", "OrderState", "apply_event", "new_order", "replay"]
