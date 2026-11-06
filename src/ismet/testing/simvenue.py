"""A deterministic simulated venue with scriptable faults.

Implements every capability, so the order manager and user strategies run against
the same API as a real broker. Market liquidity comes from :meth:`SimVenue.set_quote`,
and passive orders execute through :meth:`SimVenue.trade_through`. Nothing is random.

The fault script exists to prove the order path's safety claims. ``attempts`` and
``accepted`` count what actually reached the venue, independent of what the order
manager believes.
"""

from __future__ import annotations

import asyncio
from collections import Counter, deque
from collections.abc import AsyncIterator, Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import ClassVar

from ismet import _aio
from ismet.errors import (
    AmbiguousOutcome,
    AuthError,
    DuplicateOrderId,
    RateLimited,
    TransportError,
    VenueError,
)
from ismet.models import (
    AssetClass,
    DataSource,
    FeedKind,
    Instrument,
    Level,
    OrderBook,
    Quote,
    Symbol,
    Timeliness,
)
from ismet.models.decimals import add, mul, sub
from ismet.models.orders import (
    Acknowledged,
    Balance,
    Canceled,
    Environment,
    EventOrigin,
    Expired,
    Fill,
    FillBusted,
    Filled,
    Liquidity,
    OrderAck,
    OrderEvent,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    Replaced,
    TimeInForce,
    TradingRules,
    VenueOrderSnapshot,
)
from ismet.providers.base import Provider
from ismet.transport.clock import SYSTEM_CLOCK, Clock

SIM_VENUE = "ZSIM"
_WORKING = (OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED)


class Fault(str, Enum):
    """One scripted failure for the next ``place_order`` call."""

    LOSE_RESPONSE = "lose_response"
    """The venue accepts the order, and the response never arrives."""
    LOSE_REQUEST = "lose_request"
    """The request never arrives, and the caller cannot tell."""
    REFUSE_CONNECTION = "refuse_connection"
    """Connecting fails, so the order certainly was not delivered."""
    REJECT = "reject"
    """The venue refuses the order for a business reason."""
    SERVER_ERROR_ACCEPTED = "server_error_accepted"
    """The venue accepts the order, then answers with a 5xx."""
    ADAPTER_BUG = "adapter_bug"
    """An unexpected exception, which says nothing about delivery."""
    RATE_LIMITED = "rate_limited"
    """The venue throttled the request and did not process it."""
    AUTH_FAILED = "auth_failed"
    """The venue refused the credentials and did not process the request."""


@dataclass
class _Order:
    request: OrderRequest
    venue_order_id: str
    quantity: Decimal
    limit_price: Decimal | None
    status: OrderStatus = OrderStatus.ACCEPTED
    filled: Decimal = Decimal(0)
    arrived: int = 0
    ts: datetime | None = None
    elected: bool = False

    @property
    def leaves(self) -> Decimal:
        return sub(self.quantity, self.filled)


@dataclass
class _Market:
    bid: Decimal | None = None
    ask: Decimal | None = None
    bid_size: Decimal = Decimal(0)
    ask_size: Decimal = Decimal(0)
    last: Decimal | None = None


@dataclass
class ReportFaults:
    """Faults in the execution report stream, applied to subsequent reports."""

    drop_next: int = 0
    duplicate_next: int = 0
    hold_next: int = 0
    held: list[OrderEvent] = field(default_factory=list)


class SimVenue(Provider):
    """A simulated broker and exchange. See the module docstring."""

    name: ClassVar[str] = "sim"
    venues: ClassVar[frozenset[str]] = frozenset({SIM_VENUE})
    environment = Environment.SIMULATED

    def __init__(
        self,
        *,
        clock: Clock = SYSTEM_CLOCK,
        instruments: Iterable[Instrument] = (),
        initial_positions: dict[Symbol, Decimal] | None = None,
        cash: Decimal = Decimal(1_000_000),
        currency: str = "USD",
        taker_fee_bps: Decimal = Decimal(0),
        supports_short: bool = True,
    ) -> None:
        super().__init__(clock=clock)
        self._instruments = {i.symbol: i for i in instruments}
        self._markets: dict[Symbol, _Market] = {}
        self._orders: dict[str, _Order] = {}
        self._positions: dict[Symbol, Decimal] = dict(initial_positions or {})
        self._cash = cash
        self._currency = currency
        self._fee_bps = taker_fee_bps
        self._supports_short = supports_short
        self._fills: list[Fill] = []
        self._subscribers: list[asyncio.Queue[OrderEvent | Exception]] = []
        self._sequence = 0
        self.faults: deque[Fault] = deque()
        self.report_faults = ReportFaults()
        self.attempts: Counter[str] = Counter()
        self.accepted: Counter[str] = Counter()
        self.fail_queries = 0
        self.fail_cancels = 0
        self.lose_cancel_response = 0
        self.status_lag_queries = 0
        """A new order stays invisible to this many status queries."""
        self.positions_lag_queries = 0
        """Positions stay frozen for this many queries after each fill."""
        self.fee_only_on_query = False
        """Stream fills carry no fee; the fee appears only in ``fills_since``."""
        self.retain_busted_fills = False
        """``fills_since`` keeps reporting a busted fill, as some venues do."""
        self.answer_ticks = 1
        """Event loop turns a venue call takes after fixing its answer, as a network
        call would, so stream events can overtake any answer. Zero never suspends."""
        self.after_answer: Callable[[str], None] | None = None
        """Called with the method name once its answer is fixed, before it returns:
        what this does is what the caller's answer does not yet know."""
        self._status_lag: Counter[str] = Counter()
        self._positions_view: dict[Symbol, Decimal] | None = None
        self._positions_view_left = 0

    # Scenario control

    def add_instrument(self, instrument: Instrument) -> None:
        self._instruments[instrument.symbol] = instrument

    def set_quote(
        self,
        symbol: Symbol,
        bid: Decimal | str | None,
        ask: Decimal | str | None,
        *,
        bid_size: Decimal | str = "1000",
        ask_size: Decimal | str = "1000",
    ) -> None:
        """Set external liquidity at the touch, then trigger any elected stops."""
        market = self._markets.setdefault(symbol, _Market())
        market.bid = None if bid is None else Decimal(bid)
        market.ask = None if ask is None else Decimal(ask)
        market.bid_size = Decimal(bid_size)
        market.ask_size = Decimal(ask_size)
        self._elect_stops(symbol)

    def trade_through(
        self, symbol: Symbol, price: Decimal | str, quantity: Decimal | str
    ) -> None:
        """An external print: fills resting orders at or better, in price-time order."""
        print_price, remaining = Decimal(price), Decimal(quantity)
        self._markets.setdefault(symbol, _Market()).last = print_price
        for order in list(self._orders.values()):
            if (
                order.request.symbol != symbol
                or order.elected
                or order.status not in _WORKING
                or not self._stop_reached(order, print_price)
            ):
                continue
            order.elected = True
            if order.limit_price is None:
                self._take_liquidity(order)  # a stop is a market order once elected
        for order in self._resting_in_priority(symbol):
            if remaining <= 0:
                break
            limit = order.limit_price
            if limit is None:
                continue
            crossed = (
                print_price <= limit
                if order.request.side is OrderSide.BUY
                else print_price >= limit
            )
            if not crossed:
                continue
            quantity_now = min(order.leaves, remaining)
            self._execute(order, limit, quantity_now, Liquidity.MAKER)
            remaining = sub(remaining, quantity_now)

    def forget_order(self, client_order_id: str) -> None:
        """Lose an order, as a venue failover might."""
        del self._orders[client_order_id]

    def add_external_order(self, request: OrderRequest) -> None:
        """An order placed outside ismet, for example from another terminal."""
        self._orders[request.client_order_id] = _Order(
            request, self._next_id("EXT"), request.quantity, request.limit_price
        )

    def add_external_fill(
        self, symbol: Symbol, side: OrderSide, quantity: Decimal
    ) -> None:
        """A position change that no order explains."""
        self._adjust_position(symbol, mul(Decimal(side.sign), quantity))

    def bust(self, fill_id: str) -> None:
        """Cancel an execution and publish the bust."""
        fill = next(f for f in self._fills if f.fill_id == fill_id)
        if not self.retain_busted_fills:
            self._fills.remove(fill)
        self._adjust_position(fill.symbol, mul(Decimal(-fill.side.sign), fill.quantity))
        order = self._orders[fill.client_order_id]
        order.filled = sub(order.filled, fill.quantity)
        if order.status is OrderStatus.FILLED:
            order.status = (
                OrderStatus.PARTIALLY_FILLED
                if order.filled > 0
                else OrderStatus.CANCELED
            )
        elif order.status is OrderStatus.PARTIALLY_FILLED and order.filled == 0:
            order.status = OrderStatus.ACCEPTED
        self._publish(
            FillBusted(
                client_order_id=fill.client_order_id,
                ts=self.clock.now(),
                origin=EventOrigin.VENUE,
                fill_id=fill_id,
                reason="busted by the venue",
            )
        )

    def expire_day_orders(self) -> None:
        """End the trading day: every working DAY order expires."""
        for order in self._orders.values():
            if (
                order.status in _WORKING
                and order.request.time_in_force is TimeInForce.DAY
            ):
                order.status = OrderStatus.EXPIRED
                self._publish(
                    Expired(
                        client_order_id=order.request.client_order_id,
                        ts=self.clock.now(),
                        origin=EventOrigin.VENUE,
                    )
                )

    async def drain(self, *, timeout: float = 5.0) -> None:
        """Wait until a stream consumer exists and has processed every report."""

        async def wait() -> None:
            while not self._subscribers:
                await asyncio.sleep(0)
            for queue in list(self._subscribers):
                await queue.join()

        await _aio.wait_for(wait(), timeout)

    def disconnect_stream(self) -> None:
        """Break every execution stream."""
        for queue in self._subscribers:
            queue.put_nowait(TransportError("simulated stream disconnect"))

    def release_held_reports(self) -> None:
        """Deliver reports held back by ``report_faults``, newest first."""
        held, self.report_faults.held = self.report_faults.held, []
        for event in reversed(held):
            self._deliver(event)

    # Internals

    async def _answer(self, method: str) -> None:
        if self.after_answer is not None:
            self.after_answer(method)
        for _ in range(self.answer_ticks):
            await asyncio.sleep(0)

    def _next_id(self, prefix: str) -> str:
        self._sequence += 1
        return f"{prefix}{self._sequence:08d}"

    def _publish(self, event: OrderEvent) -> None:
        faults = self.report_faults
        if faults.drop_next > 0:
            faults.drop_next -= 1
            return
        if faults.hold_next > 0:
            faults.hold_next -= 1
            faults.held.append(event)
            return
        self._deliver(event)
        if faults.duplicate_next > 0:
            faults.duplicate_next -= 1
            self._deliver(event)

    def _deliver(self, event: OrderEvent) -> None:
        for queue in self._subscribers:
            queue.put_nowait(event)

    def _adjust_position(self, symbol: Symbol, delta: Decimal) -> None:
        if self.positions_lag_queries:
            self._positions_view = dict(self._positions)
            self._positions_view_left = self.positions_lag_queries
        self._positions[symbol] = add(self._positions.get(symbol, Decimal(0)), delta)

    def _execute(
        self, order: _Order, price: Decimal, quantity: Decimal, liquidity: Liquidity
    ) -> None:
        request = order.request
        order.filled = add(order.filled, quantity)
        order.status = (
            OrderStatus.FILLED if order.leaves == 0 else OrderStatus.PARTIALLY_FILLED
        )
        self._adjust_position(request.symbol, mul(Decimal(request.side.sign), quantity))
        notional = mul(price, quantity)
        fee = mul(notional, self._fee_bps) / Decimal(10000)
        self._cash = sub(self._cash, mul(Decimal(request.side.sign), notional))
        self._cash = sub(self._cash, fee)
        fill = Fill(
            fill_id=self._next_id("F"),
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            price=price,
            quantity=quantity,
            ts=self.clock.now(),
            venue_order_id=order.venue_order_id,
            fee=fee,
            fee_currency=self._currency,
            liquidity=liquidity,
        )
        self._fills.append(fill)
        reported = (
            fill.model_copy(update={"fee": None, "fee_currency": None})
            if self.fee_only_on_query
            else fill
        )
        self._publish(
            Filled(
                client_order_id=request.client_order_id,
                ts=self.clock.now(),
                origin=EventOrigin.VENUE,
                fill=reported,
            )
        )

    def _resting_in_priority(self, symbol: Symbol) -> list[_Order]:
        orders = [
            o
            for o in self._orders.values()
            if o.request.symbol == symbol
            and o.status in _WORKING
            and (
                o.request.order_type is OrderType.LIMIT
                or (o.request.order_type is OrderType.STOP_LIMIT and o.elected)
            )
            and o.limit_price is not None
        ]

        def priority(order: _Order) -> tuple[Decimal, int]:
            assert order.limit_price is not None
            sign = -1 if order.request.side is OrderSide.BUY else 1
            return (mul(Decimal(sign), order.limit_price), order.arrived)

        return sorted(orders, key=priority)

    def _take_liquidity(self, order: _Order) -> None:
        market = self._markets.setdefault(order.request.symbol, _Market())
        buying = order.request.side is OrderSide.BUY
        touch = market.ask if buying else market.bid
        size = market.ask_size if buying else market.bid_size
        if touch is None or size <= 0:
            return
        limit = order.limit_price
        if limit is not None and (limit < touch if buying else limit > touch):
            return
        quantity = min(order.leaves, size)
        self._execute(order, touch, quantity, Liquidity.TAKER)
        if buying:
            market.ask_size = sub(market.ask_size, quantity)
        else:
            market.bid_size = sub(market.bid_size, quantity)

    def _available_at(self, request: OrderRequest) -> Decimal:
        market = self._markets.setdefault(request.symbol, _Market())
        buying = request.side is OrderSide.BUY
        touch = market.ask if buying else market.bid
        size = market.ask_size if buying else market.bid_size
        if touch is None:
            return Decimal(0)
        limit = request.limit_price
        if limit is not None and (limit < touch if buying else limit > touch):
            return Decimal(0)
        return size

    def _would_cross(self, request: OrderRequest) -> bool:
        return self._available_at(request) > 0

    @staticmethod
    def _stop_reached(order: _Order, price: Decimal) -> bool:
        stop = order.request.stop_price
        if stop is None:
            return False
        return price >= stop if order.request.side is OrderSide.BUY else price <= stop

    def _elect_stops(self, symbol: Symbol) -> None:
        market = self._markets[symbol]
        for order in list(self._orders.values()):
            request = order.request
            if (
                request.symbol != symbol
                or request.stop_price is None
                or order.status is not OrderStatus.ACCEPTED
                or order.filled > 0
            ):
                continue
            buying = request.side is OrderSide.BUY
            touch = market.ask if buying else market.bid
            if touch is None:
                continue
            if self._stop_reached(order, touch):
                order.elected = True
                self._take_liquidity(order)

    # Provider lifecycle

    async def open(self) -> None:
        """Nothing to connect."""

    async def close(self) -> None:
        for queue in self._subscribers:
            queue.put_nowait(TransportError("simulated venue closed"))
        self._subscribers = []

    # TradingCapability

    def trading_rules(self) -> TradingRules:
        return TradingRules(
            order_types=frozenset(OrderType),
            time_in_force=frozenset(TimeInForce),
            supports_amend=True,
            supports_short=self._supports_short,
            supports_post_only=True,
            supports_cancel_on_disconnect=False,
        )

    async def place_order(self, request: OrderRequest) -> OrderAck:
        cid = request.client_order_id
        self.attempts[cid] += 1
        fault = self.faults.popleft() if self.faults else None
        if fault is Fault.LOSE_REQUEST:
            raise AmbiguousOutcome("simulated: request lost in transit")
        if fault is Fault.REFUSE_CONNECTION:
            raise TransportError("simulated: connection refused")
        if fault is Fault.ADAPTER_BUG:
            raise RuntimeError("simulated: adapter bug after send")
        if fault is Fault.RATE_LIMITED:
            raise RateLimited("simulated: too many requests", retry_after=1.0)
        if fault is Fault.AUTH_FAILED:
            raise AuthError("simulated: token expired", status=401)
        if fault is Fault.REJECT:
            raise VenueError("simulated: insufficient funds", code="INSUFFICIENT_FUNDS")
        if cid in self._orders:
            raise DuplicateOrderId(
                "duplicate client order id", code="DUPLICATE_CLIENT_ORDER_ID"
            )
        if request.post_only and self._would_cross(request):
            raise VenueError("post-only order would cross", code="WOULD_CROSS")
        self.accepted[cid] += 1
        self._status_lag[cid] = self.status_lag_queries
        order = _Order(
            request,
            self._next_id("V"),
            request.quantity,
            request.limit_price,
            arrived=self._sequence,
            ts=self.clock.now(),
        )
        self._orders[cid] = order
        self._publish(
            Acknowledged(
                client_order_id=cid,
                ts=self.clock.now(),
                origin=EventOrigin.VENUE,
                venue_order_id=order.venue_order_id,
            )
        )
        self._match_new(order)
        if fault is Fault.LOSE_RESPONSE:
            raise AmbiguousOutcome("simulated: response lost after acceptance")
        if fault is Fault.SERVER_ERROR_ACCEPTED:
            raise AmbiguousOutcome("simulated: 502 after acceptance", status=502)
        await self._answer("place_order")
        return OrderAck(
            client_order_id=cid,
            venue_order_id=order.venue_order_id,
            ts=self.clock.now(),
        )

    def _match_new(self, order: _Order) -> None:
        request = order.request
        if request.order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
            return
        tif = request.time_in_force
        if tif is TimeInForce.FOK and self._available_at(request) < order.quantity:
            self._cancel_internal(order, "fill or kill could not fill completely")
            return
        self._take_liquidity(order)
        if order.leaves > 0 and (
            tif is TimeInForce.IOC or request.order_type is OrderType.MARKET
        ):
            self._cancel_internal(order, "unfilled remainder of an immediate order")

    def _cancel_internal(self, order: _Order, reason: str) -> None:
        order.status = OrderStatus.CANCELED
        self._publish(
            Canceled(
                client_order_id=order.request.client_order_id,
                ts=self.clock.now(),
                origin=EventOrigin.VENUE,
                reason=reason,
            )
        )

    async def cancel_order(self, client_order_id: str, symbol: Symbol) -> None:  # noqa: ARG002
        if self.fail_cancels > 0:
            self.fail_cancels -= 1
            raise AmbiguousOutcome("simulated: cancel response lost")
        order = self._orders.get(client_order_id)
        if order is None:
            raise VenueError("unknown order", code="UNKNOWN_ORDER")
        if order.status.is_terminal:
            raise VenueError("too late to cancel", code="ORDER_ALREADY_CLOSED")
        self._cancel_internal(order, "canceled by client")
        if self.lose_cancel_response > 0:
            self.lose_cancel_response -= 1
            raise AmbiguousOutcome("simulated: cancel applied, response lost")
        await self._answer("cancel_order")

    async def amend_order(
        self,
        client_order_id: str,
        symbol: Symbol,  # noqa: ARG002
        *,
        quantity: Decimal | None = None,
        limit_price: Decimal | None = None,
    ) -> None:
        order = self._orders.get(client_order_id)
        if order is None:
            raise VenueError("unknown order", code="UNKNOWN_ORDER")
        if order.status.is_terminal:
            raise VenueError("too late to amend", code="ORDER_ALREADY_CLOSED")
        if quantity is not None:
            order.quantity = quantity
        if limit_price is not None:
            order.limit_price = limit_price
        self._publish(
            Replaced(
                client_order_id=client_order_id,
                ts=self.clock.now(),
                origin=EventOrigin.VENUE,
                quantity=quantity,
                limit_price=limit_price,
                venue_order_id=order.venue_order_id,
            )
        )
        if order.request.stop_price is None or order.elected:
            self._take_liquidity(order)
        await self._answer("amend_order")

    def _snapshot(self, order: _Order) -> VenueOrderSnapshot:
        return VenueOrderSnapshot(
            client_order_id=order.request.client_order_id,
            venue_order_id=order.venue_order_id,
            symbol=order.request.symbol,
            side=order.request.side,
            quantity=order.quantity,
            filled_quantity=order.filled,
            status=order.status,
            limit_price=order.limit_price,
            ts=self.clock.now(),
        )

    def _maybe_fail_query(self) -> None:
        if self.fail_queries > 0:
            self.fail_queries -= 1
            raise TransportError("simulated: query failed")

    async def order_status(
        self,
        client_order_id: str,
        symbol: Symbol,  # noqa: ARG002
    ) -> VenueOrderSnapshot | None:
        self._maybe_fail_query()
        order = self._orders.get(client_order_id)
        answer = None if order is None else self._snapshot(order)
        if self._status_lag[client_order_id] > 0:
            self._status_lag[client_order_id] -= 1
            answer = None
        await self._answer("order_status")
        return answer

    async def open_orders(
        self, symbol: Symbol | None = None
    ) -> list[VenueOrderSnapshot]:
        self._maybe_fail_query()
        answer = [
            self._snapshot(o)
            for o in self._orders.values()
            if not o.status.is_terminal
            and (symbol is None or o.request.symbol == symbol)
        ]
        await self._answer("open_orders")
        return answer

    async def fills_since(
        self, since: datetime, symbol: Symbol | None = None
    ) -> list[Fill]:
        self._maybe_fail_query()
        answer = [
            f
            for f in self._fills
            if f.ts >= since and (symbol is None or f.symbol == symbol)
        ]
        await self._answer("fills_since")
        return answer

    def stream_executions(self) -> AsyncIterator[OrderEvent]:
        """Subscribe now, so nothing published after this call is missed."""
        queue: asyncio.Queue[OrderEvent | Exception] = asyncio.Queue()
        self._subscribers.append(queue)
        return self._drain_subscription(queue)

    async def _drain_subscription(
        self, queue: asyncio.Queue[OrderEvent | Exception]
    ) -> AsyncIterator[OrderEvent]:
        try:
            while True:
                item = await queue.get()
                if isinstance(item, Exception):
                    queue.task_done()
                    raise item
                yield item
                # The consumer asking for the next event means it finished this one.
                queue.task_done()
        finally:
            if queue in self._subscribers:
                self._subscribers.remove(queue)

    # AccountCapability

    async def positions(self) -> list[Position]:
        self._maybe_fail_query()
        view = self._positions
        if self._positions_view is not None:
            view = self._positions_view
            self._positions_view_left -= 1
            if self._positions_view_left <= 0:
                self._positions_view = None
        answer = [
            Position(symbol=symbol, quantity=quantity)
            for symbol, quantity in view.items()
            if quantity != 0
        ]
        await self._answer("positions")
        return answer

    async def balances(self) -> list[Balance]:
        return [
            Balance(currency=self._currency, total=self._cash, available=self._cash)
        ]

    # MarketDataCapability

    async def quote(self, symbol: Symbol) -> Quote:
        market = self._markets.setdefault(symbol, _Market())
        now = self.clock.now()
        return Quote(
            symbol=symbol,
            exchange_ts=now,
            received_ts=now,
            bid=market.bid,
            ask=market.ask,
            bid_size=market.bid_size if market.bid is not None else None,
            ask_size=market.ask_size if market.ask is not None else None,
            last=market.last,
            currency=self._currency,
            source=DataSource(
                provider=self.name,
                kind=FeedKind.SIMULATED,
                timeliness=Timeliness.REALTIME,
            ),
        )

    async def order_book(self, symbol: Symbol, depth: int = 10) -> OrderBook:  # noqa: ARG002
        market = self._markets.setdefault(symbol, _Market())
        now = self.clock.now()
        return OrderBook(
            symbol=symbol,
            exchange_ts=now,
            received_ts=now,
            bids=(
                (Level(price=market.bid, size=market.bid_size),)
                if market.bid is not None
                else ()
            ),
            asks=(
                (Level(price=market.ask, size=market.ask_size),)
                if market.ask is not None
                else ()
            ),
        )

    # ReferenceDataCapability

    async def instrument(self, symbol: Symbol) -> Instrument:
        found = self._instruments.get(symbol)
        if found is None:
            raise VenueError(f"unknown symbol {symbol}", code="UNKNOWN_SYMBOL")
        return found

    async def search(
        self, query: str, *, venue: str | None = None, limit: int = 20
    ) -> list[Instrument]:
        if venue is not None and venue.upper() != SIM_VENUE:
            return []
        needle = query.strip().upper()
        hits = [
            i
            for i in self._instruments.values()
            if needle in i.symbol.ticker.upper() or needle in i.name.upper()
        ]
        return hits[: max(0, limit)]


def sim_instrument(ticker: str, **terms: object) -> Instrument:
    """A plain equity on the simulated venue, for tests and examples."""
    fields: dict[str, object] = {
        "symbol": Symbol(venue=SIM_VENUE, ticker=ticker),
        "name": ticker,
        "asset_class": AssetClass.EQUITY,
        "currency": "USD",
        "tick_size": Decimal("0.01"),
    }
    fields.update(terms)
    return Instrument.model_validate(fields)


__all__ = [
    "SIM_VENUE",
    "Fault",
    "ReportFaults",
    "SimVenue",
    "sim_instrument",
]
