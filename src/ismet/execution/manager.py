"""The order manager: the only path from intent to venue.

The send path, in order:

1. Refuse if the kill switch is engaged (checked in the durable journal, so another
   process can stop this one).
2. Run pre-trade risk. Rejections are journalled and raised.
3. Journal the intent. If that fails, nothing is sent.
4. Journal that the send started.
5. Call the venue exactly once.
6. Journal the outcome. A failure after a possible delivery becomes ``UNKNOWN``,
   which only a query to the venue can resolve. The order is never resent.

Those durable steps let recovery after a crash answer the question that matters.
An order with no ``send_started`` record was certainly never sent. An order with one
may have been, so the venue is asked, by client order id.

Venue reports are facts. They are recorded even if they contradict the order's
state, and any contradiction becomes an anomaly that a person is told about. If the
journal itself fails while a venue event is being recorded, the event is still
applied in memory and new orders stop, because losing a fill is worse than an
incomplete audit record.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal
from typing import Any, TypeVar, cast

from ismet.calendars import TradingCalendar, get_calendar
from ismet.capabilities import (
    AccountCapability,
    Capability,
    MarketDataCapability,
    ReferenceDataCapability,
    TradingCapability,
)
from ismet.errors import (
    AmbiguousOutcome,
    AuthError,
    ConfigError,
    IsmetError,
    JournalError,
    KillSwitchEngaged,
    NotSupported,
    OrderStateError,
    RateLimited,
    RiskRejected,
    TransportError,
    ValidationError,
    VenueError,
)
from ismet.execution.journal import Journal, KillState, RecordKind
from ismet.execution.reconcile import BreakKind, ReconBreak, ReconReport
from ismet.execution.risk import RiskContext, RiskEngine
from ismet.execution.state import OrderState, apply_event, new_order
from ismet.models import Instrument, Quote, Symbol
from ismet.models.decimals import add, mul, sub
from ismet.models.orders import (
    Acknowledged,
    CancelRejected,
    CancelRequested,
    Environment,
    EventOrigin,
    Fill,
    Filled,
    OrderEvent,
    OrderRequest,
    OrderStatus,
    OutcomeUnknown,
    Rejected,
    ReplaceRejected,
    ReplaceRequested,
    Restated,
    SendStarted,
    SubmitIntent,
    TradingRules,
    VenueOrderSnapshot,
    dump_order_event,
    parse_order_event,
)
from ismet.providers.base import Provider
from ismet.transport.backoff import ExponentialBackoff
from ismet.transport.clock import SYSTEM_CLOCK, Clock

logger = logging.getLogger("ismet.execution")

T = TypeVar("T")
EventHook = Callable[[OrderEvent, OrderState], None]
BreakHook = Callable[[ReconBreak], None]
AnomalyHook = Callable[[str, str], None]
CheckpointHook = Callable[[str, str], None]
_WORKING = (OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED)
_PENDING = (OrderStatus.PENDING_CANCEL, OrderStatus.PENDING_REPLACE)
_RESOLVE = (OrderStatus.SUBMITTING, OrderStatus.UNKNOWN)
_HALTING_ANOMALIES = ("overfill", "re-reported with different terms", "after terminal")


def _bundled_calendar(venue: str) -> TradingCalendar | None:
    try:
        return get_calendar(venue)
    except ValidationError:
        return None


class OrderManager:
    """Owns order state for one provider, and all access to its order entry.

    Use as an async context manager, or call :meth:`start` and :meth:`stop`.
    ``allow_live`` must be true to trade through a provider whose ``environment``
    is ``LIVE``, which is also the default for providers that do not declare one.

    ``assume_flat`` is for venues with no account API: positions start at zero and
    are tracked from fills. Without it such a venue's sales are rejected, because
    its position is unknown.

    ``checkpoint(name, client_order_id)`` is called at each step of the send path.
    It exists so crash tests can stop the process at any step.
    """

    def __init__(
        self,
        provider: Provider,
        journal: Journal,
        risk: RiskEngine,
        *,
        instrument_source: Callable[[Symbol], Awaitable[Instrument]] | None = None,
        reference_source: Callable[[Symbol], Awaitable[Quote]] | None = None,
        calendar_source: Callable[[str], TradingCalendar | None] = _bundled_calendar,
        clock: Clock = SYSTEM_CLOCK,
        allow_live: bool = False,
        assume_flat: bool = False,
        halt_on_break: bool = True,
        unknown_grace: float = 5.0,
        reconcile_interval: float = 30.0,
        kill_poll_interval: float = 0.5,
        locate_check: Callable[[OrderRequest], bool] | None = None,
        on_event: EventHook | None = None,
        on_break: BreakHook | None = None,
        on_anomaly: AnomalyHook | None = None,
        checkpoint: CheckpointHook | None = None,
    ) -> None:
        provider.require(Capability.TRADING)
        if provider.environment is Environment.LIVE and not allow_live:
            raise ConfigError(
                f"provider {provider.name!r} is LIVE; pass allow_live=True to send "
                "real orders"
            )
        self._provider = provider
        self._venue = cast(TradingCapability, provider)
        self._journal = journal
        self._risk = risk
        self._clock = clock
        self._instrument_source = instrument_source
        self._reference_source = reference_source
        self._calendar_source = calendar_source
        self._assume_flat = assume_flat
        self._halt_on_break = halt_on_break
        self._unknown_grace = unknown_grace
        self._reconcile_interval = reconcile_interval
        self._kill_poll_interval = kill_poll_interval
        self._locate_check = locate_check
        self._on_event = on_event
        self._on_break = on_break
        self._on_anomaly = on_anomaly
        self._checkpoint = checkpoint

        self._orders: dict[str, OrderState] = {}
        # Derived indexes, kept so submit does not slow as finished orders accumulate.
        self._open: dict[str, OrderState] = {}
        self._filled_net: dict[Symbol, Decimal] = {}
        self._last_fill_price: dict[Symbol, Decimal] = {}
        self._symbols_seen: set[Symbol] = set()
        self._created_at: dict[str, datetime] = {}
        self._baseline: dict[Symbol, Decimal] | None = None
        self._unknown_since: dict[str, float] = {}
        self._in_flight: set[str] = set()
        self._instruments: dict[Symbol, Instrument] = {}
        self._kill = KillState(False)
        self._journal_failed: str | None = None
        self.stream_healthy = False

        self._executor: ThreadPoolExecutor | None = None
        self._state_lock = asyncio.Lock()
        self._submit_lock = asyncio.Lock()
        self._recon_lock = asyncio.Lock()
        self._changed = asyncio.Condition()
        self._tasks: list[asyncio.Task[None]] = []
        self._started = False

    # Lifecycle

    async def __aenter__(self) -> OrderManager:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def start(self, *, verify_journal: bool = True) -> ReconReport:
        """Verify and replay the journal, reconcile, and start background work.

        Returns the first reconciliation. A journal that fails verification raises
        :class:`~ismet.errors.JournalError`, and nothing starts.
        """
        if self._started:
            raise ConfigError("order manager already started")
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="ismet-journal"
        )
        if verify_journal:
            result = await self._call_journal(self._journal.verify)
            if not result.ok:
                self._shutdown_executor()
                raise JournalError(
                    f"journal failed verification at record {result.first_bad_seq}: "
                    f"{result.detail}"
                )
        await self._recover()
        await self._establish_baseline()
        self._started = True
        report = await self.reconcile()
        self._tasks = [
            asyncio.create_task(self._stream_loop(), name="ismet-oms-stream"),
            asyncio.create_task(self._reconcile_loop(), name="ismet-oms-reconcile"),
            asyncio.create_task(self._kill_loop(), name="ismet-oms-kill"),
        ]
        return report

    async def stop(self, *, cancel_open: bool = False) -> list[OrderState]:
        """Stop background work and return the orders still open at the venue.

        Open orders are left working unless ``cancel_open`` is set. A later
        :meth:`start` on the same journal picks them up again.
        """
        if not self._started:
            self._shutdown_executor()
            return []
        if cancel_open:
            await self.cancel_all()
        await self.abort()
        return [o for o in self._orders.values() if o.is_open]

    async def abort(self) -> None:
        """Stop background work without touching any order or writing any record."""
        self._started = False
        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._shutdown_executor()

    def _shutdown_executor(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None

    def _require_started(self) -> None:
        if not self._started:
            raise ConfigError("order manager is not started")

    # Journal access: durable, ordered, and off the event loop

    async def _call_journal(self, fn: Callable[[], T]) -> T:
        if self._executor is None:
            raise ConfigError("order manager is not started")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, fn)

    async def _append(
        self,
        kind: RecordKind,
        payload: Mapping[str, Any] | str,
        *,
        client_order_id: str | None = None,
        unique_intent: bool = False,
    ) -> None:
        text = payload if isinstance(payload, str) else json.dumps(payload, default=str)
        ts = self._clock.now()
        await self._call_journal(
            lambda: self._journal.append(
                kind,
                text,
                client_order_id=client_order_id,
                ts=ts,
                unique_intent=unique_intent,
            )
        )

    def _pass(self, name: str, client_order_id: str) -> None:
        if self._checkpoint is not None:
            self._checkpoint(name, client_order_id)

    # Recovery

    async def _recover(self) -> None:
        records = await self._call_journal(lambda: list(self._journal.records()))
        for record in records:
            if record.kind is RecordKind.ORDER_EVENT:
                event = parse_order_event(record.payload)
                cid = event.client_order_id
                if isinstance(event, SubmitIntent) and cid not in self._orders:
                    self._store(new_order(event))
                    self._created_at[cid] = event.ts
                elif cid in self._orders:
                    self._store(apply_event(self._orders[cid], event))
                else:
                    raise JournalError(f"event {event.kind} for {cid} has no intent")
            elif record.kind is RecordKind.POSITION_BASELINE:
                self._baseline = {
                    Symbol.parse(key): Decimal(value)
                    for key, value in json.loads(record.payload).items()
                }
        self._kill = await self._call_journal(self._journal.kill_state)
        for state in list(self._orders.values()):
            if state.status is OrderStatus.PENDING_SUBMIT:
                await self._record(
                    Rejected(
                        client_order_id=state.client_order_id,
                        ts=self._clock.now(),
                        origin=EventOrigin.RECONCILE,
                        reason="process stopped before the send started; never sent",
                        code="NEVER_SENT",
                    )
                )

    async def _establish_baseline(self) -> None:
        if self._baseline is not None:
            return
        if isinstance(self._provider, AccountCapability):
            positions = await self._provider.positions()
            baseline = {p.symbol: p.quantity for p in positions}
        elif self._assume_flat:
            baseline = {}
        else:
            return
        await self._append(
            RecordKind.POSITION_BASELINE,
            {str(symbol): str(quantity) for symbol, quantity in baseline.items()},
        )
        self._baseline = baseline

    # State

    @property
    def orders(self) -> Mapping[str, OrderState]:
        return dict(self._orders)

    def order(self, client_order_id: str) -> OrderState:
        try:
            return self._orders[client_order_id]
        except KeyError:
            raise ValidationError(f"unknown order {client_order_id}") from None

    def open_orders(self, symbol: Symbol | None = None) -> list[OrderState]:
        return [
            o
            for o in self._open.values()
            if symbol is None or o.request.symbol == symbol
        ]

    def _store(self, state: OrderState) -> None:
        """The one place order state changes, so the indexes cannot drift from it."""
        cid = state.client_order_id
        symbol = state.request.symbol
        prior = self._orders.get(cid)
        filled_before = prior.filled_quantity if prior is not None else Decimal(0)
        self._orders[cid] = state
        self._symbols_seen.add(symbol)
        if state.is_open:
            self._open[cid] = state
        else:
            self._open.pop(cid, None)
        change = sub(state.filled_quantity, filled_before)
        if change != 0:
            signed = mul(Decimal(state.request.side.sign), change)
            self._filled_net[symbol] = add(
                self._filled_net.get(symbol, Decimal(0)), signed
            )
        if state.fills:
            self._last_fill_price[symbol] = state.fills[-1].price

    def net_position(self, symbol: Symbol) -> Decimal | None:
        """Baseline plus every recorded fill, or ``None`` if the baseline is unknown."""
        if self._baseline is None:
            return None
        return add(
            self._baseline.get(symbol, Decimal(0)),
            self._filled_net.get(symbol, Decimal(0)),
        )

    def _tracked_symbols(self) -> set[Symbol]:
        symbols = set(self._symbols_seen)
        if self._baseline:
            symbols |= set(self._baseline)
        return symbols

    async def _record(self, event: OrderEvent, *, durable: bool = False) -> OrderState:
        """Journal and apply one event, atomically with respect to other events.

        ``durable=True`` is for events that must be on disk before the next step,
        and raises if the journal fails. Otherwise a journal failure stops new
        orders, and the event still reaches memory.
        """
        anomalies: tuple[str, ...]
        async with self._state_lock:
            try:
                await self._append(
                    RecordKind.ORDER_EVENT,
                    dump_order_event(event),
                    client_order_id=event.client_order_id,
                    unique_intent=isinstance(event, SubmitIntent),
                )
            except (JournalError, ConfigError, OSError) as exc:
                if durable:
                    raise
                self._journal_failed = str(exc)
                logger.critical("journal write failed: %s", exc)
            cid = event.client_order_id
            if isinstance(event, SubmitIntent):
                state = new_order(event)
                self._created_at[cid] = event.ts
                before: tuple[str, ...] = ()
            else:
                prior = self._orders[cid]
                before = prior.anomalies
                state = apply_event(prior, event)
            self._store(state)
            anomalies = state.anomalies[len(before) :]
            if state.status is OrderStatus.UNKNOWN:
                self._unknown_since.setdefault(cid, self._clock.monotonic())
            elif state.status is not OrderStatus.SUBMITTING:
                self._unknown_since.pop(cid, None)
        for message in anomalies:
            await self._raise_anomaly(cid, message)
        self._notify_event(event, state)
        async with self._changed:
            self._changed.notify_all()
        return state

    def _notify_event(self, event: OrderEvent, state: OrderState) -> None:
        if self._on_event is None:
            return
        try:
            self._on_event(event, state)
        except Exception:
            logger.exception("on_event hook failed")

    async def _raise_anomaly(self, cid: str, message: str) -> None:
        logger.error("order %s anomaly: %s", cid, message)
        with contextlib.suppress(IsmetError):
            await self._append(
                RecordKind.ANOMALY, {"message": message}, client_order_id=cid
            )
        if self._on_anomaly is not None:
            try:
                self._on_anomaly(cid, message)
            except Exception:
                logger.exception("on_anomaly hook failed")
        if self._started and any(tag in message for tag in _HALTING_ANOMALIES):
            await self.engage_kill(
                f"order {cid}: {message}", operator="oms", cancel_open=False
            )

    async def wait_for(
        self,
        client_order_id: str,
        predicate: Callable[[OrderState], bool],
        timeout: float,
    ) -> OrderState:
        """Wait until the order satisfies ``predicate``; raises ``TimeoutError``."""

        async def waiter() -> OrderState:
            async with self._changed:
                while not predicate(self.order(client_order_id)):
                    await self._changed.wait()
                return self.order(client_order_id)

        return await asyncio.wait_for(waiter(), timeout)

    # Kill switch

    @property
    def kill_state(self) -> KillState:
        return self._kill

    async def engage_kill(
        self, reason: str, *, operator: str = "api", cancel_open: bool = True
    ) -> KillState:
        """Stop new orders, durably. With ``cancel_open``, also cancel what is open.

        In-memory state changes first, so the switch works even if the journal is
        down. Cancels are retried by a background sweep until nothing is open.
        """
        self._kill = KillState(True, reason, operator, self._kill.seq, cancel_open)
        logger.critical("kill switch engaged by %s: %s", operator, reason)
        try:
            await self._append(
                RecordKind.KILL_ENGAGED,
                {"reason": reason, "operator": operator, "cancel_open": cancel_open},
            )
        except IsmetError as exc:
            self._journal_failed = str(exc)
            logger.critical("kill switch could not be journalled: %s", exc)
        if cancel_open and self._started:
            await self.cancel_all()
        return self._kill

    async def release_kill(self, reason: str, *, operator: str) -> KillState:
        """Allow new orders again. Requires a named operator; journalled."""
        if not operator:
            raise ValidationError("releasing the kill switch requires an operator")
        await self._append(
            RecordKind.KILL_RELEASED, {"reason": reason, "operator": operator}
        )
        self._kill = KillState(False, reason, operator, self._kill.seq)
        logger.warning("kill switch released by %s: %s", operator, reason)
        return self._kill

    async def _check_kill(self) -> None:
        durable = await self._call_journal(self._journal.kill_state)
        if durable.seq != self._kill.seq or durable.engaged:
            self._kill = durable
        if self._kill.engaged:
            raise KillSwitchEngaged(self._kill.reason)
        if self._journal_failed is not None:
            raise KillSwitchEngaged(f"journal failure: {self._journal_failed}")

    async def _kill_loop(self) -> None:
        while True:
            await asyncio.sleep(self._kill_poll_interval)
            try:
                self._kill = await self._call_journal(self._journal.kill_state)
                if self._kill.engaged and self._kill.cancel_open and self.open_orders():
                    await self.cancel_all()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("kill switch sweep failed")

    # Submit

    async def _instrument(self, symbol: Symbol) -> Instrument | None:
        cached = self._instruments.get(symbol)
        if cached is not None:
            return cached
        try:
            if self._instrument_source is not None:
                found = await self._instrument_source(symbol)
            elif isinstance(self._provider, ReferenceDataCapability):
                found = await self._provider.instrument(symbol)
            else:
                return None
        except IsmetError:
            logger.exception("instrument lookup failed for %s", symbol)
            return None
        self._instruments[symbol] = found
        return found

    async def _reference(self, symbol: Symbol) -> Quote | None:
        try:
            if self._reference_source is not None:
                return await self._reference_source(symbol)
            if isinstance(self._provider, MarketDataCapability):
                return await self._provider.quote(symbol)
        except IsmetError:
            logger.exception("reference quote failed for %s", symbol)
        return None

    def _gross_notional(self) -> Decimal | None:
        """Absolute value of positions and open orders at their last known prices.

        ``None`` if any position has no known price, which fails a gross limit closed.
        """
        last_price = self._last_fill_price
        gross = Decimal(0)
        for symbol in self._tracked_symbols():
            position = self.net_position(symbol)
            if position is None:
                return None
            if position == 0:
                continue
            if symbol not in last_price:
                return None
            instrument = self._instruments.get(symbol)
            multiplier = instrument.multiplier if instrument else Decimal(1)
            gross = add(gross, abs(mul(mul(position, last_price[symbol]), multiplier)))
        for order in self.open_orders():
            price = order.limit_price or order.request.stop_price
            if price is None:
                price = last_price.get(order.request.symbol)
            if price is None:
                return None
            instrument = self._instruments.get(order.request.symbol)
            multiplier = instrument.multiplier if instrument else Decimal(1)
            gross = add(gross, abs(mul(mul(order.leaves_quantity, price), multiplier)))
        return gross

    def _risk_context(
        self,
        request: OrderRequest,
        instrument: Instrument | None,
        reference: Quote | None,
        *,
        exclude: str | None = None,
    ) -> RiskContext:
        rules: TradingRules | None
        try:
            rules = self._venue.trading_rules()
        except IsmetError:
            rules = None
        locate = (
            self._locate_check(request) if self._locate_check is not None else False
        )
        return RiskContext(
            request=request,
            instrument=instrument,
            reference=reference,
            now=self._clock.now(),
            open_orders=[o for o in self.open_orders() if o.client_order_id != exclude],
            position=self.net_position(request.symbol),
            calendar=self._calendar_source(request.symbol.venue),
            rules=rules,
            locate_available=locate,
            gross_notional=self._gross_notional(),
        )

    async def submit(self, request: OrderRequest) -> OrderState:
        """Send ``request`` once, after risk checks. Returns the resulting state.

        Repeating a call with the same request and client order id returns the
        existing order without sending again, so a caller may retry safely. Raises
        :class:`~ismet.errors.RiskRejected` or
        :class:`~ismet.errors.KillSwitchEngaged` if it was refused before sending.
        A venue rejection is a returned state, not an exception, because the order
        existed. Check ``state.status``.
        """
        self._require_started()
        cid = request.client_order_id
        existing = self._orders.get(cid)
        if existing is not None:
            if existing.request != request:
                raise ValidationError(
                    f"client order id {cid} already used for a different order"
                )
            return existing
        instrument = await self._instrument(request.symbol)
        reference = await self._reference(request.symbol)
        async with self._submit_lock:
            existing = self._orders.get(cid)
            if existing is not None:
                return existing
            await self._check_kill()
            decision = self._risk.check(
                self._risk_context(request, instrument, reference)
            )
            if not decision.approved:
                await self._append(
                    RecordKind.RISK_REJECT,
                    {
                        "check": decision.check,
                        "reason": decision.reason,
                        "request": request.model_dump(mode="json"),
                    },
                    client_order_id=cid,
                )
                raise RiskRejected(decision.check, decision.reason)
            await self._record(
                SubmitIntent(
                    client_order_id=cid, ts=self._clock.now(), request=request
                ),
                durable=True,
            )
        self._pass("after_intent", cid)
        return await self._send(request)

    async def _send(self, request: OrderRequest) -> OrderState:
        cid = request.client_order_id
        if (
            self._kill.engaged
            or self._orders[cid].status is not OrderStatus.PENDING_SUBMIT
        ):
            if self._orders[cid].status is OrderStatus.PENDING_SUBMIT:
                return await self._record(
                    Rejected(
                        client_order_id=cid,
                        ts=self._clock.now(),
                        reason="kill switch engaged before the send started",
                        code="KILL_SWITCH",
                    )
                )
            return self._orders[cid]
        await self._record(
            SendStarted(client_order_id=cid, ts=self._clock.now()), durable=True
        )
        self._pass("after_send_started", cid)
        self._in_flight.add(cid)
        try:
            ack = await self._venue.place_order(request)
        except AmbiguousOutcome as exc:
            return await self._mark_unknown(cid, str(exc))
        except VenueError as exc:
            return await self._rejected(cid, str(exc), exc.code)
        except (AuthError, RateLimited) as exc:
            return await self._rejected(cid, str(exc), type(exc).__name__)
        except TransportError as exc:
            # Certainly not delivered, per the TradingCapability contract.
            return await self._rejected(cid, str(exc), "NOT_DELIVERED")
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await self._mark_unknown(cid, "caller cancelled during the send")
            raise
        except Exception as exc:
            # An adapter bug says nothing about delivery, so assume it may have sent.
            logger.exception("place_order failed unexpectedly for %s", cid)
            return await self._mark_unknown(cid, f"{type(exc).__name__}: {exc}")
        finally:
            self._in_flight.discard(cid)
        self._pass("after_venue_accept", cid)
        return await self._record(
            Acknowledged(
                client_order_id=cid,
                ts=self._clock.now(),
                origin=EventOrigin.VENUE,
                venue_order_id=ack.venue_order_id,
            )
        )

    async def _rejected(self, cid: str, reason: str, code: str | None) -> OrderState:
        return await self._record(
            Rejected(
                client_order_id=cid,
                ts=self._clock.now(),
                origin=EventOrigin.VENUE,
                reason=reason,
                code=code,
            )
        )

    async def _mark_unknown(self, cid: str, reason: str) -> OrderState:
        return await self._record(
            OutcomeUnknown(client_order_id=cid, ts=self._clock.now(), reason=reason)
        )

    # Cancel and amend

    async def cancel(self, client_order_id: str) -> OrderState:
        """Ask the venue to cancel. Idempotent; a terminal order is returned as is."""
        self._require_started()
        state = self.order(client_order_id)
        if state.is_terminal:
            return state
        if state.status is OrderStatus.PENDING_SUBMIT:
            return await self._record(
                Rejected(
                    client_order_id=client_order_id,
                    ts=self._clock.now(),
                    reason="cancelled before the send started",
                    code="CANCELED_BEFORE_SEND",
                )
            )
        await self._record(
            CancelRequested(client_order_id=client_order_id, ts=self._clock.now())
        )
        try:
            await self._venue.cancel_order(client_order_id, state.request.symbol)
        except VenueError as exc:
            return await self._record(
                CancelRejected(
                    client_order_id=client_order_id,
                    ts=self._clock.now(),
                    origin=EventOrigin.VENUE,
                    reason=str(exc),
                    code=exc.code,
                )
            )
        except (TransportError, AuthError, RateLimited) as exc:
            return await self._mark_unknown(client_order_id, f"cancel: {exc}")
        return self.order(client_order_id)

    async def cancel_all(self, symbol: Symbol | None = None) -> list[OrderState]:
        """Cancel every open order, optionally in one symbol. Never stops early."""
        results: list[OrderState] = []
        for order in self.open_orders(symbol):
            if order.status is OrderStatus.PENDING_CANCEL:
                results.append(order)
                continue
            try:
                results.append(await self.cancel(order.client_order_id))
            except IsmetError:
                logger.exception("cancel failed for %s", order.client_order_id)
                results.append(order)
        return results

    async def amend(
        self,
        client_order_id: str,
        *,
        quantity: Decimal | None = None,
        limit_price: Decimal | None = None,
    ) -> OrderState:
        """Change a working order's terms, after risk checks on the result."""
        self._require_started()
        state = self.order(client_order_id)
        if not self._venue.trading_rules().supports_amend:
            raise NotSupported(self._provider.name, "amend_order")
        if state.status not in _WORKING:
            raise OrderStateError(
                f"cannot amend {client_order_id} in {state.status.value}"
            )
        if quantity is not None and quantity <= state.filled_quantity:
            raise ValidationError(
                f"new quantity {quantity} is not above filled {state.filled_quantity}"
            )
        symbol = state.request.symbol
        amended = state.request.model_copy(
            update={
                "quantity": quantity if quantity is not None else state.quantity,
                "limit_price": (
                    limit_price if limit_price is not None else state.limit_price
                ),
            }
        )
        instrument = await self._instrument(symbol)
        reference = await self._reference(symbol)
        async with self._submit_lock:
            await self._check_kill()
            decision = self._risk.check(
                self._risk_context(
                    amended, instrument, reference, exclude=client_order_id
                ),
                is_amend=True,
            )
            if not decision.approved:
                await self._append(
                    RecordKind.RISK_REJECT,
                    {
                        "check": decision.check,
                        "reason": decision.reason,
                        "request": amended.model_dump(mode="json"),
                    },
                    client_order_id=client_order_id,
                )
                raise RiskRejected(decision.check, decision.reason)
            await self._record(
                ReplaceRequested(
                    client_order_id=client_order_id,
                    ts=self._clock.now(),
                    quantity=quantity,
                    limit_price=limit_price,
                ),
                durable=True,
            )
        try:
            await self._venue.amend_order(
                client_order_id, symbol, quantity=quantity, limit_price=limit_price
            )
        except VenueError as exc:
            return await self._record(
                ReplaceRejected(
                    client_order_id=client_order_id,
                    ts=self._clock.now(),
                    origin=EventOrigin.VENUE,
                    reason=str(exc),
                    code=exc.code,
                )
            )
        except (TransportError, AuthError, RateLimited) as exc:
            return await self._mark_unknown(client_order_id, f"amend: {exc}")
        return self.order(client_order_id)

    # Venue events

    async def _on_venue_event(self, event: OrderEvent) -> None:
        cid = event.client_order_id
        if cid not in self._orders:
            detail = f"{event.kind} from venue for an order ismet never sent"
            await self._report_break(ReconBreak(BreakKind.ORPHAN_ORDER, cid, detail))
            return
        await self._record(event)

    async def _stream_loop(self) -> None:
        backoff = ExponentialBackoff(base=0.1, maximum=10.0)
        attempt = 0
        while True:
            try:
                stream = self._venue.stream_executions()
                self.stream_healthy = True
                if attempt:
                    await self.reconcile()
                async for event in stream:
                    attempt = 0
                    await self._on_venue_event(event)
                raise TransportError("execution stream ended")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.stream_healthy = False
                logger.warning("execution stream lost: %s", exc)
            await asyncio.sleep(backoff.delay(attempt))
            attempt += 1

    # Reconciliation

    async def _reconcile_loop(self) -> None:
        interval = min(self._reconcile_interval, max(self._unknown_grace, 0.01))
        elapsed = 0.0
        while True:
            await asyncio.sleep(interval)
            elapsed += interval
            has_unknown = any(o.status in _RESOLVE for o in self._open.values())
            if has_unknown or elapsed >= self._reconcile_interval:
                elapsed = 0.0
                try:
                    await self.reconcile()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("reconciliation failed")

    async def _report_break(self, item: ReconBreak) -> None:
        logger.error(
            "reconciliation break: %s %s: %s",
            item.kind.value,
            item.subject,
            item.detail,
        )
        with contextlib.suppress(IsmetError):
            await self._append(
                RecordKind.RECON_BREAK,
                {
                    "kind": item.kind.value,
                    "subject": item.subject,
                    "detail": item.detail,
                },
            )
        if self._on_break is not None:
            try:
                self._on_break(item)
            except Exception:
                logger.exception("on_break hook failed")
        if item.is_critical and self._halt_on_break and not self._kill.engaged:
            await self.engage_kill(
                f"reconciliation break: {item.kind.value} {item.subject}",
                operator="oms",
                cancel_open=False,
            )

    async def reconcile(self) -> ReconReport:
        """Compare every open order, the venue's open orders, and positions."""
        async with self._recon_lock:
            report = ReconReport(at=self._clock.now())
            for state in self.open_orders():
                if state.client_order_id in self._in_flight:
                    # Its send has not returned, so "not found" would mean nothing.
                    continue
                report.orders_checked += 1
                await self._reconcile_order(state, report)
            await self._find_orphans(report)
            await self._check_positions(report)
            for item in report.breaks:
                await self._report_break(item)
            return report

    async def _reconcile_order(self, state: OrderState, report: ReconReport) -> None:
        cid = state.client_order_id
        symbol = state.request.symbol
        try:
            snapshot = await self._venue.order_status(cid, symbol)
        except IsmetError as exc:
            report.complete = False
            report.breaks.append(ReconBreak(BreakKind.QUERY_FAILED, cid, str(exc)))
            return
        if snapshot is None:
            await self._resolve_not_found(state, report)
            return
        if snapshot.filled_quantity != state.filled_quantity:
            await self._sync_fills(state, snapshot, report)
        current = self._orders[cid]
        if self._status_differs(current, snapshot):
            await self._record(
                Restated(
                    client_order_id=cid,
                    ts=self._clock.now(),
                    origin=EventOrigin.RECONCILE,
                    status=snapshot.status,
                    venue_order_id=snapshot.venue_order_id,
                    venue_filled_quantity=snapshot.filled_quantity,
                    reason="reconciliation",
                )
            )
            report.resolved.append(cid)

    @staticmethod
    def _status_differs(state: OrderState, snapshot: VenueOrderSnapshot) -> bool:
        if state.status is snapshot.status:
            return False
        # A cancel or replace the venue has not processed yet is not a disagreement.
        return not (state.status in _PENDING and snapshot.status in _WORKING)

    async def _sync_fills(
        self, state: OrderState, snapshot: VenueOrderSnapshot, report: ReconReport
    ) -> None:
        cid = state.client_order_id
        since = self._created_at.get(cid, report.at)
        try:
            fills = await self._venue.fills_since(since, state.request.symbol)
        except IsmetError as exc:
            report.complete = False
            report.breaks.append(ReconBreak(BreakKind.QUERY_FAILED, cid, str(exc)))
            return
        known = {f.fill_id for f in state.fills}
        for fill in fills:
            if fill.client_order_id == cid and fill.fill_id not in known:
                await self._record_recovered_fill(fill)
        current = self._orders[cid]
        if current.filled_quantity != snapshot.filled_quantity:
            report.breaks.append(
                ReconBreak(
                    BreakKind.FILL_MISMATCH,
                    cid,
                    f"ismet has {current.filled_quantity} filled, venue reports "
                    f"{snapshot.filled_quantity}",
                )
            )
        else:
            report.resolved.append(cid)

    async def _record_recovered_fill(self, fill: Fill) -> None:
        await self._record(
            Filled(
                client_order_id=fill.client_order_id,
                ts=self._clock.now(),
                origin=EventOrigin.RECONCILE,
                fill=fill,
            )
        )

    async def _resolve_not_found(self, state: OrderState, report: ReconReport) -> None:
        """The venue says it has no such order.

        An order still in flight can look missing, so wait out ``unknown_grace``, then
        send a cancel by client order id to kill anything arriving late, and ask once
        more. Only then is the order declared never accepted.
        """
        cid = state.client_order_id
        symbol = state.request.symbol
        now = self._clock.monotonic()
        first_seen = self._unknown_since.setdefault(cid, now)
        if now - first_seen < self._unknown_grace:
            report.complete = False
            return
        with contextlib.suppress(IsmetError):
            await self._venue.cancel_order(cid, symbol)
        try:
            snapshot = await self._venue.order_status(cid, symbol)
        except IsmetError as exc:
            report.complete = False
            report.breaks.append(ReconBreak(BreakKind.QUERY_FAILED, cid, str(exc)))
            return
        if snapshot is not None:
            return  # it did arrive; the next pass reconciles it
        if state.status in _RESOLVE:
            await self._record(
                Rejected(
                    client_order_id=cid,
                    ts=self._clock.now(),
                    origin=EventOrigin.RECONCILE,
                    reason="venue has no record of this order after the grace period",
                    code="NOT_FOUND_AT_VENUE",
                )
            )
            report.resolved.append(cid)
            return
        await self._record(
            Restated(
                client_order_id=cid,
                ts=self._clock.now(),
                origin=EventOrigin.RECONCILE,
                status=OrderStatus.CANCELED,
                reason="venue no longer has this working order",
            )
        )
        report.breaks.append(
            ReconBreak(
                BreakKind.MISSING_AT_VENUE,
                cid,
                f"was {state.status.value} with {state.filled_quantity} filled",
            )
        )

    async def _find_orphans(self, report: ReconReport) -> None:
        try:
            venue_open = await self._venue.open_orders()
        except IsmetError as exc:
            report.complete = False
            report.breaks.append(
                ReconBreak(BreakKind.QUERY_FAILED, "open_orders", str(exc))
            )
            return
        for snapshot in venue_open:
            state = self._orders.get(snapshot.client_order_id)
            if state is None or state.is_terminal:
                recorded = "not in the journal" if state is None else state.status.value
                report.breaks.append(
                    ReconBreak(
                        BreakKind.ORPHAN_ORDER,
                        snapshot.client_order_id,
                        f"open at venue, {snapshot.side.value} {snapshot.quantity} "
                        f"{snapshot.symbol}; ismet has it as {recorded}",
                    )
                )

    async def _check_positions(self, report: ReconReport) -> None:
        if self._baseline is None or not isinstance(self._provider, AccountCapability):
            return
        try:
            positions = await self._provider.positions()
        except IsmetError as exc:
            report.complete = False
            report.breaks.append(
                ReconBreak(BreakKind.QUERY_FAILED, "positions", str(exc))
            )
            return
        venue = {p.symbol: p.quantity for p in positions}
        for symbol in sorted({*venue, *self._tracked_symbols()}, key=str):
            ours = self.net_position(symbol) or Decimal(0)
            theirs = venue.get(symbol, Decimal(0))
            if ours == theirs:
                continue
            await self._recover_missing_fills(symbol, report)
            ours = self.net_position(symbol) or Decimal(0)
            if ours != theirs:
                report.breaks.append(
                    ReconBreak(
                        BreakKind.POSITION_MISMATCH,
                        str(symbol),
                        f"ismet {ours}, venue {theirs}, difference {sub(ours, theirs)}",
                    )
                )

    async def _recover_missing_fills(self, symbol: Symbol, report: ReconReport) -> None:
        # Rare by design: this runs only after a position disagreement.
        times = [
            self._created_at[o.client_order_id]
            for o in self._orders.values()
            if o.request.symbol == symbol and o.client_order_id in self._created_at
        ]
        since = min(times) if times else report.at
        try:
            fills = await self._venue.fills_since(since, symbol)
        except IsmetError as exc:
            report.complete = False
            report.breaks.append(
                ReconBreak(BreakKind.QUERY_FAILED, str(symbol), str(exc))
            )
            return
        for fill in fills:
            state = self._orders.get(fill.client_order_id)
            if state is None:
                report.breaks.append(
                    ReconBreak(
                        BreakKind.ORPHAN_FILL,
                        fill.fill_id,
                        f"fill for {fill.client_order_id}, which ismet never sent",
                    )
                )
            elif all(f.fill_id != fill.fill_id for f in state.fills):
                await self._record_recovered_fill(fill)
                report.resolved.append(fill.client_order_id)


def events_of(journal: Journal) -> Iterable[OrderEvent]:
    """Every order event in a journal, in order, for audit and replay."""
    for record in journal.records():
        if record.kind is RecordKind.ORDER_EVENT:
            yield parse_order_event(record.payload)


__all__ = ["OrderManager", "events_of"]
