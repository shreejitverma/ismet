"""Pre-trade risk checks, which fail closed.

Every check answers "is this order provably acceptable". Missing information is a
rejection: no instrument, no reference price, no calendar answer, or an exception
in a check all refuse the order. Limits are layered by scope (global, account,
strategy, symbol), and every applicable scope must pass.

Worst-case exposure counts everything that could still execute, including orders
whose status is unknown. A venue-side crash or lost response therefore tightens
what the engine will allow rather than loosening it.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from ismet.calendars import TradingCalendar
from ismet.errors import CalendarOutOfRange
from ismet.execution.state import OrderState
from ismet.models import AssetClass, BookState, Instrument, ListingStatus, Quote, Symbol
from ismet.models.decimals import add, mul, ratio, sub
from ismet.models.orders import (
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    ShortSaleMark,
    TimeInForce,
    TradingRules,
)
from ismet.models.types import PositiveDecimal, Quantity

_SESSION_ORDER_TIF = (
    TimeInForce.OPG,
    TimeInForce.CLS,
    TimeInForce.GTC,
    TimeInForce.GTD,
)


class RiskLimits(BaseModel):
    """Limits for one scope. ``None`` means this scope does not constrain that value."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    max_order_quantity: PositiveDecimal | None = None
    max_order_notional: PositiveDecimal | None = None
    max_position: Quantity | None = None
    max_gross_notional: PositiveDecimal | None = None
    max_open_orders: int | None = Field(default=None, ge=0)
    max_orders_per_second: int | None = Field(default=None, ge=1)
    price_collar_bps: PositiveDecimal | None = None


class RiskConfig(BaseModel):
    """The whole risk configuration. Replaced atomically and audited on change.

    ``default`` limits are required so a forgotten limit cannot mean "unlimited":
    to run without a limit, a value must be set explicitly very high.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    default: RiskLimits
    per_account: dict[str, RiskLimits] = Field(default_factory=dict)
    per_strategy: dict[str, RiskLimits] = Field(default_factory=dict)
    per_symbol: dict[str, RiskLimits] = Field(default_factory=dict)
    restricted_symbols: frozenset[str] = frozenset()
    max_reference_age_seconds: PositiveDecimal = Decimal(5)
    duplicate_window_seconds: Decimal = Decimal(1)
    allow_short: bool = False
    enforce_session: bool = True
    prevent_self_match: bool = True
    block_while_unknown: bool = True

    def limits_for(self, request: OrderRequest) -> list[tuple[str, RiskLimits]]:
        """Every scope that applies to ``request``, labelled for messages."""
        scopes = [("default", self.default)]
        if request.account is not None and request.account in self.per_account:
            scopes.append(
                (f"account:{request.account}", self.per_account[request.account])
            )
        if request.strategy is not None and request.strategy in self.per_strategy:
            scopes.append(
                (f"strategy:{request.strategy}", self.per_strategy[request.strategy])
            )
        key = str(request.symbol)
        if key in self.per_symbol:
            scopes.append((f"symbol:{key}", self.per_symbol[key]))
        return scopes


@dataclass(frozen=True)
class RiskContext:
    """Everything a decision may depend on, gathered once so the decision is pure."""

    request: OrderRequest
    instrument: Instrument | None
    reference: Quote | None
    now: datetime
    open_orders: Sequence[OrderState] = ()
    position: Decimal | None = None
    calendar: TradingCalendar | None = None
    rules: TradingRules | None = None
    locate_available: bool = False
    gross_notional: Decimal | None = None


@dataclass(frozen=True)
class RiskDecision:
    approved: bool
    check: str = ""
    reason: str = ""


APPROVED = RiskDecision(True)


def _reject(check: str, reason: str) -> RiskDecision:
    return RiskDecision(False, check, reason)


def _reference_price(ctx: RiskContext) -> Decimal | None:
    """The price an order would be expected to trade near, on its own side."""
    quote = ctx.reference
    if quote is None:
        return None
    touch = quote.ask if ctx.request.side is OrderSide.BUY else quote.bid
    if touch is not None:
        return touch
    return quote.mid if quote.mid is not None else quote.last


def _pricing_for_notional(ctx: RiskContext) -> Decimal | None:
    if ctx.request.limit_price is not None:
        return ctx.request.limit_price
    if ctx.request.stop_price is not None:
        return ctx.request.stop_price
    return _reference_price(ctx)


def check_supported(ctx: RiskContext) -> RiskDecision:
    rules, request = ctx.rules, ctx.request
    if rules is None:
        return _reject("order_support", "provider declared no trading rules")
    if request.order_type not in rules.order_types:
        return _reject(
            "order_support", f"order type {request.order_type.value} unsupported"
        )
    if request.time_in_force not in rules.time_in_force:
        return _reject(
            "order_support", f"time in force {request.time_in_force.value} unsupported"
        )
    if request.post_only and not rules.supports_post_only:
        return _reject("order_support", "post_only unsupported")
    if len(request.client_order_id) > rules.client_order_id_max_length:
        return _reject(
            "order_support",
            f"client order id longer than {rules.client_order_id_max_length}",
        )
    return APPROVED


def check_instrument(ctx: RiskContext) -> RiskDecision:
    instrument, request = ctx.instrument, ctx.request
    if instrument is None:
        return _reject("instrument", f"no reference data for {request.symbol}")
    if not instrument.active or instrument.status in (
        ListingStatus.HALTED,
        ListingStatus.SUSPENDED,
        ListingStatus.DELISTED,
    ):
        return _reject(
            "instrument", f"{request.symbol} is not tradable: {instrument.status.value}"
        )
    if instrument.expiry is not None and instrument.expiry <= ctx.now:
        return _reject("instrument", f"{request.symbol} expired {instrument.expiry}")
    for label, price in (("limit", request.limit_price), ("stop", request.stop_price)):
        if price is not None and not instrument.is_tick_aligned(price):
            return _reject(
                "tick_size",
                f"{label} price {price} is not on tick {instrument.tick_at(price)}",
            )
    if not instrument.is_lot_aligned(request.quantity):
        return _reject(
            "lot_size",
            f"quantity {request.quantity} is not a multiple of {instrument.lot_size}",
        )
    if (
        instrument.min_quantity is not None
        and request.quantity < instrument.min_quantity
    ):
        return _reject("min_quantity", f"below minimum {instrument.min_quantity}")
    if (
        instrument.max_quantity is not None
        and request.quantity > instrument.max_quantity
    ):
        return _reject("max_quantity", f"above venue maximum {instrument.max_quantity}")
    return APPROVED


def check_session(ctx: RiskContext, config: RiskConfig) -> RiskDecision:
    if not config.enforce_session or ctx.request.time_in_force in _SESSION_ORDER_TIF:
        return APPROVED
    if ctx.calendar is None:
        return _reject("session", f"no trading calendar for {ctx.request.symbol.venue}")
    try:
        is_open = ctx.calendar.is_open(ctx.now)
    except CalendarOutOfRange as exc:
        return _reject("session", str(exc))
    if not is_open:
        return _reject(
            "session",
            f"{ctx.request.symbol.venue} is not in continuous trading at "
            f"{ctx.now.isoformat()}",
        )
    return APPROVED


def check_restricted(ctx: RiskContext, config: RiskConfig) -> RiskDecision:
    symbol = ctx.request.symbol
    if (
        str(symbol) in config.restricted_symbols
        or symbol.ticker in config.restricted_symbols
    ):
        return _reject("restricted", f"{symbol} is on the restricted list")
    return APPROVED


def check_short_sale(ctx: RiskContext, config: RiskConfig) -> RiskDecision:
    request = ctx.request
    if request.side is not OrderSide.SELL:
        return APPROVED
    if ctx.position is None:
        return _reject("short_sale", "position unknown; cannot classify the sale")
    pending_sells = _sum_leaves(ctx.open_orders, request.symbol, OrderSide.SELL)
    resulting = sub(sub(ctx.position, pending_sells), request.quantity)
    marked_short = request.short_sale is not ShortSaleMark.NOT_SHORT
    if resulting >= 0:
        return APPROVED
    if request.reduce_only:
        return _reject("short_sale", "reduce-only sale would open a short position")
    if not marked_short:
        return _reject(
            "short_sale",
            f"sale would leave position {resulting} but is not marked short",
        )
    if not config.allow_short:
        return _reject("short_sale", "short selling is disabled by configuration")
    if ctx.rules is not None and not ctx.rules.supports_short:
        return _reject("short_sale", "provider does not support short selling")
    instrument = ctx.instrument
    needs_locate = (
        instrument is not None
        and instrument.asset_class in (AssetClass.EQUITY, AssetClass.ETF)
        and request.short_sale is ShortSaleMark.SHORT
    )
    if needs_locate and not ctx.locate_available:
        return _reject("short_sale", "no locate is recorded for this short sale")
    return APPROVED


def check_reference(ctx: RiskContext, config: RiskConfig) -> RiskDecision:
    """A marketable order needs a fresh, sane market to be judged against.

    Without a quote a limit order cannot be shown to be passive, so it counts as
    marketable and is refused.
    """
    quote = ctx.reference
    if quote is None:
        return _reject("reference_price", "no reference quote")
    age = ctx.now - quote.exchange_ts
    limit = timedelta(seconds=float(config.max_reference_age_seconds))
    if age > limit:
        return _reject(
            "stale_data",
            f"reference quote is {age.total_seconds():.3f}s old, limit "
            f"{config.max_reference_age_seconds}s",
        )
    if ctx.request.order_type in (
        OrderType.MARKET,
        OrderType.STOP,
    ) and quote.book_state in (
        BookState.CROSSED,
        BookState.EMPTY,
        BookState.ONE_SIDED,
    ):
        return _reject(
            "reference_price",
            f"market order with a {quote.book_state.value} book",
        )
    return APPROVED


def _price_diff_bps(price: Decimal, reference: Decimal) -> Decimal | None:
    if reference == 0:
        return None
    return ratio(mul(abs(sub(price, reference)), Decimal(10000)), abs(reference))


def check_collar(ctx: RiskContext, limits: RiskLimits, scope: str) -> RiskDecision:
    """A fat-finger check: refuse prices far on the aggressive side of the market."""
    if limits.price_collar_bps is None or ctx.request.limit_price is None:
        return APPROVED
    reference = _reference_price(ctx)
    if reference is None:
        return _reject("price_collar", "no reference price to collar against")
    price = ctx.request.limit_price
    aggressive = (
        price > reference if ctx.request.side is OrderSide.BUY else price < reference
    )
    if not aggressive:
        return APPROVED
    diff = _price_diff_bps(price, reference)
    if diff is None:
        return _reject("price_collar", "reference price is zero")
    if diff > limits.price_collar_bps:
        return _reject(
            "price_collar",
            f"{price} is {diff:.2f} bps through {reference}; {scope} limit "
            f"{limits.price_collar_bps} bps",
        )
    return APPROVED


def _notional(ctx: RiskContext, price: Decimal) -> Decimal:
    multiplier = ctx.instrument.multiplier if ctx.instrument is not None else Decimal(1)
    return abs(mul(mul(price, ctx.request.quantity), multiplier))


def check_size(ctx: RiskContext, limits: RiskLimits, scope: str) -> RiskDecision:
    quantity = ctx.request.quantity
    if limits.max_order_quantity is not None and quantity > limits.max_order_quantity:
        return _reject(
            "max_order_quantity",
            f"quantity {quantity} exceeds {scope} limit {limits.max_order_quantity}",
        )
    needs_price = limits.max_order_notional is not None or (
        ctx.instrument is not None and ctx.instrument.min_notional is not None
    )
    if not needs_price:
        return APPROVED
    price = _pricing_for_notional(ctx)
    if price is None:
        return _reject("max_order_notional", "no price available to compute notional")
    notional = _notional(ctx, price)
    if limits.max_order_notional is not None and notional > limits.max_order_notional:
        return _reject(
            "max_order_notional",
            f"notional {notional} exceeds {scope} limit {limits.max_order_notional}",
        )
    if (
        ctx.instrument is not None
        and ctx.instrument.min_notional is not None
        and notional < ctx.instrument.min_notional
    ):
        return _reject("min_notional", f"notional {notional} below venue minimum")
    return APPROVED


def _sum_leaves(
    orders: Iterable[OrderState], symbol: Symbol, side: OrderSide
) -> Decimal:
    result = Decimal(0)
    for order in orders:
        if order.request.symbol == symbol and order.request.side is side:
            result = add(result, order.leaves_quantity)
    return result


def check_position(ctx: RiskContext, limits: RiskLimits, scope: str) -> RiskDecision:
    if limits.max_position is None:
        return APPROVED
    if ctx.position is None:
        return _reject("max_position", "position unknown")
    request = ctx.request
    same_side = _sum_leaves(ctx.open_orders, request.symbol, request.side)
    worst = add(
        ctx.position,
        mul(Decimal(request.side.sign), add(same_side, request.quantity)),
    )
    if abs(worst) > limits.max_position and abs(worst) > abs(ctx.position):
        return _reject(
            "max_position",
            f"worst-case position {worst} exceeds {scope} limit {limits.max_position}",
        )
    return APPROVED


def check_gross(ctx: RiskContext, limits: RiskLimits, scope: str) -> RiskDecision:
    if limits.max_gross_notional is None:
        return APPROVED
    if ctx.gross_notional is None:
        return _reject("max_gross_notional", "gross exposure unknown")
    price = _pricing_for_notional(ctx)
    if price is None:
        return _reject("max_gross_notional", "no price available")
    projected = add(ctx.gross_notional, _notional(ctx, price))
    if projected > limits.max_gross_notional:
        return _reject(
            "max_gross_notional",
            f"gross notional {projected} exceeds {scope} limit "
            f"{limits.max_gross_notional}",
        )
    return APPROVED


def check_open_orders(ctx: RiskContext, limits: RiskLimits, scope: str) -> RiskDecision:
    if limits.max_open_orders is None:
        return APPROVED
    if len(ctx.open_orders) >= limits.max_open_orders:
        return _reject(
            "max_open_orders",
            f"{len(ctx.open_orders)} open orders; {scope} limit "
            f"{limits.max_open_orders}",
        )
    return APPROVED


def check_unknown(ctx: RiskContext, config: RiskConfig) -> RiskDecision:
    """While an order's fate is unknown, do not add risk in the same instrument."""
    if not config.block_while_unknown:
        return APPROVED
    for order in ctx.open_orders:
        if (
            order.request.symbol == ctx.request.symbol
            and order.status is OrderStatus.UNKNOWN
        ):
            return _reject(
                "unknown_order",
                f"{order.client_order_id} in {ctx.request.symbol} has unknown "
                "status and is being reconciled",
            )
    return APPROVED


def check_self_match(ctx: RiskContext, config: RiskConfig) -> RiskDecision:
    if not config.prevent_self_match:
        return APPROVED
    request = ctx.request
    for order in ctx.open_orders:
        if order.request.symbol != request.symbol or order.request.side is request.side:
            continue
        resting = order.limit_price
        price = request.limit_price
        crosses = (
            resting is None
            or price is None
            or (price >= resting if request.side is OrderSide.BUY else price <= resting)
        )
        if crosses:
            return _reject(
                "self_match",
                f"would trade against own order {order.client_order_id}",
            )
    return APPROVED


@dataclass
class _Throttle:
    """Sliding one-second window of accepted orders."""

    sent: deque[datetime] = field(default_factory=deque)

    def count(self, now: datetime) -> int:
        cutoff = now - timedelta(seconds=1)
        while self.sent and self.sent[0] <= cutoff:
            self.sent.popleft()
        return len(self.sent)


class RiskEngine:
    """Runs every check in order and fails closed. Stateful only for time windows."""

    def __init__(self, config: RiskConfig) -> None:
        self._config = config
        self._throttle = _Throttle()
        self._recent: deque[tuple[datetime, tuple[object, ...]]] = deque()

    @property
    def config(self) -> RiskConfig:
        return self._config

    def update_config(self, config: RiskConfig) -> None:
        """Replace limits atomically. The order manager journals the change."""
        self._config = config

    def check(self, ctx: RiskContext, *, is_amend: bool = False) -> RiskDecision:
        """Decide on ``ctx``. Any internal error rejects the order.

        An amend is checked on its resulting terms, without the duplicate check,
        since it does not create a new order.
        """
        try:
            decision = self._evaluate(ctx, is_amend=is_amend)
        except Exception as exc:
            return _reject("risk_engine_error", f"{type(exc).__name__}: {exc}")
        if decision.approved:
            self._record(ctx)
        return decision

    def _evaluate(self, ctx: RiskContext, *, is_amend: bool) -> RiskDecision:
        config = self._config
        single: list[Callable[[], RiskDecision]] = [
            lambda: check_supported(ctx),
            lambda: check_instrument(ctx),
            lambda: check_restricted(ctx, config),
            lambda: check_session(ctx, config),
            lambda: check_unknown(ctx, config),
            lambda: check_reference(ctx, config),
            lambda: check_short_sale(ctx, config),
            lambda: check_self_match(ctx, config),
        ]
        if not is_amend:
            single.append(lambda: self._check_duplicate(ctx, config))
        for run in single:
            decision = run()
            if not decision.approved:
                return decision
        for scope, limits in config.limits_for(ctx.request):
            for scoped in (
                check_size,
                check_collar,
                check_position,
                check_gross,
                check_open_orders,
            ):
                decision = scoped(ctx, limits, scope)
                if not decision.approved:
                    return decision
            if limits.max_orders_per_second is not None:
                sent = self._throttle.count(ctx.now)
                if sent >= limits.max_orders_per_second:
                    return _reject(
                        "order_rate",
                        f"{sent} orders in the last second; {scope} limit "
                        f"{limits.max_orders_per_second}",
                    )
        return APPROVED

    @staticmethod
    def _fingerprint(request: OrderRequest) -> tuple[object, ...]:
        return (
            request.symbol.key,
            request.side,
            request.quantity,
            request.order_type,
            request.limit_price,
            request.stop_price,
            request.account,
            request.strategy,
        )

    def _check_duplicate(self, ctx: RiskContext, config: RiskConfig) -> RiskDecision:
        window = timedelta(seconds=float(config.duplicate_window_seconds))
        cutoff = ctx.now - window
        while self._recent and self._recent[0][0] <= cutoff:
            self._recent.popleft()
        fingerprint = self._fingerprint(ctx.request)
        for _, seen in self._recent:
            if seen == fingerprint:
                return _reject(
                    "duplicate_order",
                    f"identical order within {config.duplicate_window_seconds}s",
                )
        return APPROVED

    def _record(self, ctx: RiskContext) -> None:
        self._throttle.sent.append(ctx.now)
        if self._config.duplicate_window_seconds > 0:
            self._recent.append((ctx.now, self._fingerprint(ctx.request)))


__all__ = [
    "APPROVED",
    "RiskConfig",
    "RiskContext",
    "RiskDecision",
    "RiskEngine",
    "RiskLimits",
]
