"""Order, fill, and account models, and the events that drive an order's life.

An order is what the journal says happened to it, so each change is a typed,
immutable event. Each event type carries exactly the fields it needs, which makes
"a fill without a price" or "a limit order without a limit" unrepresentable.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    model_validator,
)

from ismet.models.symbol import Symbol
from ismet.models.types import (
    Currency,
    DecimalStrict,
    PositiveDecimal,
    Price,
    Quantity,
    Timestamp,
)

_CLIENT_ORDER_ID = re.compile(r"[A-Za-z0-9_.:-]{1,64}")


def _client_order_id(value: str) -> str:
    if not _CLIENT_ORDER_ID.fullmatch(value):
        raise ValueError(
            f"invalid client order id {value!r}: 1-64 characters of A-Z a-z 0-9 _ . : -"
        )
    return value


ClientOrderId = Annotated[str, AfterValidator(_client_order_id)]


class Environment(str, Enum):
    """Where orders go. Anything undeclared is treated as live."""

    SIMULATED = "simulated"
    PAPER = "paper"
    LIVE = "live"


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"

    @property
    def sign(self) -> int:
        return 1 if self is OrderSide.BUY else -1


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class TimeInForce(str, Enum):
    DAY = "day"
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"
    GTD = "gtd"
    OPG = "opg"
    CLS = "cls"


class ShortSaleMark(str, Enum):
    """Sale marking in the sense of Reg SHO Rule 200(g). Buys are ``NOT_SHORT``."""

    NOT_SHORT = "not_short"
    SHORT = "short"
    SHORT_EXEMPT = "short_exempt"


class Liquidity(str, Enum):
    MAKER = "maker"
    TAKER = "taker"
    UNKNOWN = "unknown"


class OrderStatus(str, Enum):
    """Where an order is in its life, as far as ismet can prove.

    ``PENDING_SUBMIT``: journalled, and the send has not started, so it is certain
    the venue never saw it. ``SUBMITTING``: the send started. ``UNKNOWN``: the venue
    may or may not have it, and only a query can tell.
    """

    PENDING_SUBMIT = "pending_submit"
    SUBMITTING = "submitting"
    UNKNOWN = "unknown"
    ACCEPTED = "accepted"
    PARTIALLY_FILLED = "partially_filled"
    PENDING_CANCEL = "pending_cancel"
    PENDING_REPLACE = "pending_replace"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL


_TERMINAL = frozenset(
    {
        OrderStatus.FILLED,
        OrderStatus.CANCELED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    }
)


class OrderRequest(BaseModel):
    """What the caller wants done. Immutable and validated before any risk check.

    Required prices follow the order type. A stop order has no limit price and a
    market order has neither. ``GTD`` requires ``expire_at``.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    client_order_id: ClientOrderId
    symbol: Symbol
    side: OrderSide
    quantity: PositiveDecimal
    order_type: OrderType = OrderType.LIMIT
    time_in_force: TimeInForce = TimeInForce.DAY
    limit_price: Price | None = None
    stop_price: Price | None = None
    expire_at: Timestamp | None = None
    account: str | None = None
    strategy: str | None = None
    short_sale: ShortSaleMark = ShortSaleMark.NOT_SHORT
    post_only: bool = False
    reduce_only: bool = False

    @model_validator(mode="after")
    def _shape(self) -> OrderRequest:
        needs_limit = self.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT)
        needs_stop = self.order_type in (OrderType.STOP, OrderType.STOP_LIMIT)
        if needs_limit and self.limit_price is None:
            raise ValueError(f"{self.order_type.value} order requires limit_price")
        if not needs_limit and self.limit_price is not None:
            raise ValueError(f"{self.order_type.value} order cannot have limit_price")
        if needs_stop and self.stop_price is None:
            raise ValueError(f"{self.order_type.value} order requires stop_price")
        if not needs_stop and self.stop_price is not None:
            raise ValueError(f"{self.order_type.value} order cannot have stop_price")
        if (self.time_in_force is TimeInForce.GTD) != (self.expire_at is not None):
            raise ValueError("expire_at is required with, and only with, GTD")
        if (
            self.side is OrderSide.BUY
            and self.short_sale is not ShortSaleMark.NOT_SHORT
        ):
            raise ValueError("a buy order cannot be marked short")
        if self.post_only and self.order_type is not OrderType.LIMIT:
            raise ValueError("post_only applies only to limit orders")
        if self.post_only and self.time_in_force in (TimeInForce.IOC, TimeInForce.FOK):
            raise ValueError("post_only conflicts with immediate time in force")
        return self


class Fill(BaseModel):
    """One execution against an order. ``fee`` is negative for a rebate."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    fill_id: str = Field(min_length=1)
    client_order_id: ClientOrderId
    symbol: Symbol
    side: OrderSide
    price: Price
    quantity: PositiveDecimal
    ts: Timestamp
    venue_order_id: str | None = None
    fee: DecimalStrict | None = None
    fee_currency: Currency | None = None
    liquidity: Liquidity = Liquidity.UNKNOWN


class OrderAck(BaseModel):
    """A venue's acceptance of a new order."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    client_order_id: ClientOrderId
    venue_order_id: str
    ts: Timestamp


class VenueOrderSnapshot(BaseModel):
    """The venue's own view of an order, used as truth by reconciliation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    client_order_id: ClientOrderId
    venue_order_id: str | None
    symbol: Symbol
    side: OrderSide
    quantity: PositiveDecimal
    filled_quantity: Quantity
    status: OrderStatus
    limit_price: Price | None = None
    ts: Timestamp


class Position(BaseModel):
    """A net position as reported by a venue. Short is negative."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    symbol: Symbol
    quantity: DecimalStrict
    average_price: Price | None = None
    account: str | None = None


class Balance(BaseModel):
    """A cash or asset balance. Missing detail is ``None``, never zero."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    currency: Currency
    total: DecimalStrict
    available: DecimalStrict | None = None
    settled: DecimalStrict | None = None
    account: str | None = None


class TradingRules(BaseModel):
    """What a provider's order entry supports, declared and validated before send."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    order_types: frozenset[OrderType]
    time_in_force: frozenset[TimeInForce]
    supports_amend: bool = False
    supports_short: bool = False
    supports_post_only: bool = False
    supports_cancel_on_disconnect: bool = False
    client_order_id_max_length: int = Field(default=64, ge=1)


class EventOrigin(str, Enum):
    """Who produced an event, for the audit trail."""

    OMS = "oms"
    VENUE = "venue"
    RECONCILE = "reconcile"


class _Event(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    client_order_id: ClientOrderId
    ts: Timestamp
    origin: EventOrigin = EventOrigin.OMS


class SubmitIntent(_Event):
    kind: Literal["intent"] = "intent"
    request: OrderRequest


class SendStarted(_Event):
    kind: Literal["send_started"] = "send_started"


class Acknowledged(_Event):
    kind: Literal["ack"] = "ack"
    venue_order_id: str


class Rejected(_Event):
    kind: Literal["reject"] = "reject"
    reason: str
    code: str | None = None


class OutcomeUnknown(_Event):
    kind: Literal["outcome_unknown"] = "outcome_unknown"
    reason: str


class Filled(_Event):
    kind: Literal["fill"] = "fill"
    fill: Fill


class FillBusted(_Event):
    """The venue cancelled an earlier execution."""

    kind: Literal["bust"] = "bust"
    fill_id: str
    reason: str = ""


class CancelRequested(_Event):
    kind: Literal["cancel_requested"] = "cancel_requested"


class Canceled(_Event):
    kind: Literal["canceled"] = "canceled"
    reason: str = ""


class CancelRejected(_Event):
    kind: Literal["cancel_rejected"] = "cancel_rejected"
    reason: str
    code: str | None = None


class ReplaceRequested(_Event):
    kind: Literal["replace_requested"] = "replace_requested"
    quantity: PositiveDecimal | None = None
    limit_price: Price | None = None

    @model_validator(mode="after")
    def _something(self) -> ReplaceRequested:
        if self.quantity is None and self.limit_price is None:
            raise ValueError("a replace must change quantity or limit_price")
        return self


class Replaced(_Event):
    kind: Literal["replaced"] = "replaced"
    quantity: PositiveDecimal | None = None
    limit_price: Price | None = None
    venue_order_id: str | None = None


class ReplaceRejected(_Event):
    kind: Literal["replace_rejected"] = "replace_rejected"
    reason: str
    code: str | None = None


class Expired(_Event):
    kind: Literal["expired"] = "expired"


class Restated(_Event):
    """The venue's authoritative status, learned by query rather than by report."""

    kind: Literal["restated"] = "restated"
    status: OrderStatus
    venue_order_id: str | None = None
    venue_filled_quantity: Quantity | None = None
    reason: str = ""


OrderEvent = Annotated[
    SubmitIntent
    | SendStarted
    | Acknowledged
    | Rejected
    | OutcomeUnknown
    | Filled
    | FillBusted
    | CancelRequested
    | Canceled
    | CancelRejected
    | ReplaceRequested
    | Replaced
    | ReplaceRejected
    | Expired
    | Restated,
    Field(discriminator="kind"),
]

ORDER_EVENT_ADAPTER: TypeAdapter[OrderEvent] = TypeAdapter(OrderEvent)


def parse_order_event(payload: str | bytes) -> OrderEvent:
    """Decode an event from its journal JSON."""
    return ORDER_EVENT_ADAPTER.validate_json(payload)


def dump_order_event(event: OrderEvent) -> str:
    """Encode an event as canonical JSON; decimals are strings."""
    return ORDER_EVENT_ADAPTER.dump_json(event).decode("utf-8")


def event_kinds() -> tuple[str, ...]:
    """Every event ``kind``, for exhaustive transition tests."""
    return (
        "intent",
        "send_started",
        "ack",
        "reject",
        "outcome_unknown",
        "fill",
        "bust",
        "cancel_requested",
        "canceled",
        "cancel_rejected",
        "replace_requested",
        "replaced",
        "replace_rejected",
        "expired",
        "restated",
    )


__all__ = [
    "ORDER_EVENT_ADAPTER",
    "Acknowledged",
    "Balance",
    "CancelRejected",
    "CancelRequested",
    "Canceled",
    "ClientOrderId",
    "Environment",
    "EventOrigin",
    "Expired",
    "Fill",
    "FillBusted",
    "Filled",
    "Liquidity",
    "OrderAck",
    "OrderEvent",
    "OrderRequest",
    "OrderSide",
    "OrderStatus",
    "OrderType",
    "OutcomeUnknown",
    "Position",
    "Rejected",
    "ReplaceRejected",
    "ReplaceRequested",
    "Replaced",
    "Restated",
    "SendStarted",
    "ShortSaleMark",
    "SubmitIntent",
    "TimeInForce",
    "TradingRules",
    "VenueOrderSnapshot",
    "dump_order_event",
    "event_kinds",
    "parse_order_event",
]
