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

New orders are refused whenever exposure cannot be trusted: the kill switch is on,
the journal has failed, the latest reconciliation could not query the venue, or the
execution stream is down.

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
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Iterable,
    Mapping,
)
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
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
    DuplicateOrderId,
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
from ismet.execution.journal import (
    Journal,
    JournalRecord,
    KillState,
    RecordKind,
    chain_contains,
    names_operator,
)
from ismet.execution.reconcile import BreakKind, ReconBreak, ReconReport
from ismet.execution.risk import RiskConfig, RiskContext, RiskEngine
from ismet.execution.state import (
    PENDING_STATUSES,
    WORKING_STATUSES,
    OrderState,
    apply_event,
    fill_adds_detail,
    fill_contradicts,
    new_order,
)
from ismet.models import Instrument, Quote, Symbol, to_decimal
from ismet.models.decimals import absolute, add, mul, sub
from ismet.models.orders import (
    Acknowledged,
    CancelRejected,
    CancelRequested,
    Environment,
    EventOrigin,
    Fill,
    FillBusted,
    Filled,
    OrderEvent,
    OrderRequest,
    OrderStatus,
    OutcomeUnknown,
    Position,
    Rejected,
    Replaced,
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
_RESOLVE = (OrderStatus.SUBMITTING, OrderStatus.UNKNOWN)
# A fill after a cancel is an ordinary race, recorded but not a reason to stop. A fill
# on an order believed never accepted or never sent means the books were wrong.
_HALTING_ANOMALIES = (
    "overfill",
    "re-reported with different terms",
    "after terminal status rejected",
    "before the send started",
)
_MAX_CONCURRENT_CANCELS = 32
_FILL_SYNC_ATTEMPTS = 3


def _net_quantities(positions: Iterable[Position]) -> dict[Symbol, Decimal]:
    """One quantity per symbol, whatever number of rows a venue reports it in."""
    net: dict[Symbol, Decimal] = {}
    for position in positions:
        net[position.symbol] = add(
            net.get(position.symbol, Decimal(0)), position.quantity
        )
    return net


def _orphan_fill_subject(fill: Fill) -> str:
    """A fill id is unique only per order, so an orphan is named by both."""
    return f"{fill.client_order_id}:{fill.fill_id}"


def _is_halting(anomaly: str) -> bool:
    return any(tag in anomaly for tag in _HALTING_ANOMALIES)


def _fill_terms(fill: Fill) -> tuple[str, str, str, str, str, str]:
    """A fill report's identity and what it says traded, as one hashable key."""
    return (
        fill.client_order_id,
        fill.fill_id,
        str(fill.symbol),
        fill.side.value,
        str(fill.price),
        str(fill.quantity),
    )


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

    ``instrument_ttl`` bounds how long reference data is trusted, so a halt or a
    delisting is seen; a failed refresh refuses the order. ``cancel_retry_interval`` is
    how long a sent cancel may stay unanswered before it is sent again.
    ``fill_lookback`` widens every fill query, so a venue clock behind the local one
    cannot hide a fill. Reconciliation also compares all recent fills, including those
    on closed orders, because a position check misses offsetting losses and a venue with
    no account API has no position check at all. Positions must disagree for longer than
    ``unknown_grace`` to count, since brokers' position views lag their fills.
    ``stream_settle`` is how long a newly connected execution stream must survive, or
    deliver an event, before it counts as up. New orders are refused until then, and
    :meth:`start` waits out the first one. A follow-up reconciliation then covers the
    time the subscription took to go live. ``None`` trusts the stream once subscribed
    and skips the follow-up.

    A gross limit values positions at current market marks, never at cost basis. Each
    new order refreshes the marks of every held symbol older than the risk config's
    ``max_reference_age_seconds``, one reference quote per symbol, and a symbol with no
    fresh mark makes gross exposure unknown, which refuses the order.

    Risk limits changed through :meth:`update_risk_config` are journalled. A start with
    limits different from the latest journalled change is refused, so an incident's
    tightened limits cannot silently revert: start with the journalled limits, then
    change them through :meth:`update_risk_config`.
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
        instrument_ttl: float = 60.0,
        cancel_retry_interval: float = 1.0,
        fill_lookback: float = 3600.0,
        stream_settle: float | None = 2.0,
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
        self._instrument_ttl = instrument_ttl
        self._cancel_retry_interval = cancel_retry_interval
        self._fill_lookback = timedelta(seconds=fill_lookback)
        self._stream_settle = stream_settle
        self._locate_check = locate_check
        self._on_event = on_event
        self._on_break = on_break
        self._on_anomaly = on_anomaly
        self._checkpoint = checkpoint

        self._orders: dict[str, OrderState] = {}
        # Derived indexes, kept so submit does not slow as finished orders accumulate.
        self._open: dict[str, OrderState] = {}
        self._filled_net: dict[Symbol, Decimal] = {}
        self._marks: dict[Symbol, tuple[Decimal, float]] = {}
        self._symbols_seen: set[Symbol] = set()
        self._created_at: dict[str, datetime] = {}
        self._baseline: dict[Symbol, Decimal] | None = None
        self._unknown_since: dict[str, float] = {}
        self._submitting: set[str] = set()
        self._placing: set[str] = set()
        self._cancel_before_send: set[str] = set()
        self._instruments: dict[Symbol, Instrument] = {}
        self._instruments_at: dict[Symbol, float] = {}
        self._change_requested_at: dict[str, float] = {}
        self._kill = KillState(False)
        self._kill_unrecorded: dict[str, Any] | None = None
        self._reconcile_blind: str | None = None
        self._last_sweep: datetime | None = None
        self._durable_sweep: datetime | None = None
        self._reported_orphan_fills: set[str] = set()
        self._reported_corrections: set[tuple[str, str, str, str, str, str]] = set()
        self._sweep_floor: datetime | None = None
        self._adjustments_seen = 0
        self._journalled_config: RiskConfig | None = None
        self._unjournalled: list[OrderEvent] = []
        self._position_suspect_since: dict[Symbol, float] = {}
        self._journal_failed: str | None = None
        self.stream_healthy = False
        self._stream_judged = asyncio.Event()

        self._executor: ThreadPoolExecutor | None = None
        self._state_lock = asyncio.Lock()
        self._submit_lock = asyncio.Lock()
        self._recon_lock = asyncio.Lock()
        self._adjust_lock = asyncio.Lock()
        self._changed = asyncio.Condition()
        self._tasks: list[asyncio.Task[None]] = []
        self._started = False

    # Lifecycle

    async def __aenter__(self) -> OrderManager:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def start(
        self,
        *,
        verify_journal: bool = True,
        expected_head_hash: str | None = None,
    ) -> ReconReport:
        """Verify and replay the journal, reconcile, and start background work.

        Returns the first reconciliation, once the execution stream has held up for
        ``stream_settle`` or failed. A journal that fails verification raises
        :class:`~ismet.errors.JournalError`, and nothing starts. Pass the head hash
        saved from an earlier run as ``expected_head_hash`` to also detect a truncated
        or replaced journal, which is otherwise self-consistent.
        """
        if self._started:
            raise ConfigError("order manager already started")
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="ismet-journal"
        )
        try:
            return await self._start(verify_journal, expected_head_hash)
        except BaseException:
            await self.abort()
            raise

    async def _start(
        self, verify_journal: bool, expected_head_hash: str | None
    ) -> ReconReport:
        if verify_journal:
            result = await self._call_journal(self._journal.verify)
            if not result.ok:
                raise JournalError(
                    f"journal failed verification at record {result.first_bad_seq}: "
                    f"{result.detail}"
                )
        if expected_head_hash is not None:
            found = await self._call_journal(
                lambda: chain_contains(self._journal, expected_head_hash)
            )
            if not found:
                raise JournalError(
                    f"journal does not contain the expected head {expected_head_hash}; "
                    "it was truncated or replaced"
                )
        await self._recover()
        if (
            self._journalled_config is not None
            and self._journalled_config != self._risk.config
        ):
            raise ConfigError(
                "risk limits differ from the latest journalled change; start with the "
                "journalled limits, then change them with update_risk_config"
            )
        await self._establish_baseline()
        self._started = True
        self.stream_healthy = False
        self._stream_judged.clear()
        for symbol in self._symbols_with_exposure():
            await self._instrument(symbol)
        report = await self.reconcile()
        self._tasks = [
            asyncio.create_task(self._stream_loop(), name="ismet-oms-stream"),
            asyncio.create_task(self._reconcile_loop(), name="ismet-oms-reconcile"),
            asyncio.create_task(self._kill_loop(), name="ismet-oms-kill"),
        ]
        await self._stream_judged.wait()
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
        await self._record_kill()
        if self._kill_unrecorded is not None:
            logger.critical(
                "stopping with a kill switch the journal never recorded: it will not "
                "survive a restart. Engage it again with `ismet kill engage`"
            )
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
    ) -> JournalRecord:
        text = payload if isinstance(payload, str) else json.dumps(payload, default=str)
        ts = self._clock.now()
        return await self._call_journal(
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
        # A restarted instance rebuilds from the journal, not on top of old memory.
        self._orders.clear()
        self._open.clear()
        self._filled_net.clear()
        self._marks.clear()
        self._symbols_seen.clear()
        self._created_at.clear()
        self._baseline = None
        self._reported_orphan_fills.clear()
        self._reported_corrections.clear()
        self._sweep_floor = None
        self._adjustments_seen = 0
        self._journalled_config = None
        self._unjournalled.clear()
        records = await self._call_journal(lambda: list(self._journal.records()))
        self._durable_sweep = None
        # With no clean sweep on record, every fill since the first record is unproven.
        self._last_sweep = records[0].ts if records else None
        halting: tuple[int, str] | None = None
        for record in records:
            if record.kind is RecordKind.SWEEP_MARK:
                self._durable_sweep = datetime.fromisoformat(
                    json.loads(record.payload)["at"]
                )
                self._last_sweep = self._durable_sweep
            elif record.kind is RecordKind.CONFIG_CHANGE:
                self._journalled_config = RiskConfig.model_validate(
                    json.loads(record.payload)["risk"]
                )
            elif record.kind is RecordKind.RECON_BREAK:
                detail = json.loads(record.payload)
                if detail["kind"] == BreakKind.ORPHAN_FILL.value:
                    self._reported_orphan_fills.add(detail["subject"])
            elif record.kind is RecordKind.POSITION_ADJUSTED:
                self._apply_adjustment(record)
            elif record.kind is RecordKind.ORDER_EVENT:
                event = parse_order_event(record.payload)
                cid = event.client_order_id
                if isinstance(event, SubmitIntent) and cid not in self._orders:
                    self._store(new_order(event))
                    self._created_at[cid] = event.ts
                elif cid in self._orders:
                    prior = self._orders[cid]
                    if isinstance(event, Filled) and fill_contradicts(
                        prior, event.fill
                    ):
                        # Raised below if it never was; a sweep must not halt again.
                        self._reported_corrections.add(_fill_terms(event.fill))
                    state = apply_event(prior, event)
                    self._store(state)
                    for message in state.anomalies[len(prior.anomalies) :]:
                        if _is_halting(message):
                            halting = (record.seq, f"order {cid}: {message}")
                else:
                    raise JournalError(f"event {event.kind} for {cid} has no intent")
            elif record.kind is RecordKind.POSITION_BASELINE:
                self._baseline = {
                    Symbol.parse(key): Decimal(value["quantity"])
                    for key, value in json.loads(record.payload).items()
                }
                self._sweep_floor = self._baseline_floor(record.ts)
        self._adjustments_seen = records[-1].seq if records else 0
        self._marks.clear()  # replayed fills are history, not the market now
        self._kill = await self._call_journal(self._journal.kill_state)
        if halting is not None and halting[0] > self._kill.seq:
            # The process died between recording the anomaly and recording its halt.
            await self._halt(halting[1])
        for state in list(self._open.values()):
            if state.status is OrderStatus.PENDING_SUBMIT:
                await self._never_sent(state.client_order_id)
            elif state.status is OrderStatus.SUBMITTING:
                # The send's result died with the old process.
                await self._mark_unknown(
                    state.client_order_id, "process stopped during the send"
                )

    async def _establish_baseline(self) -> None:
        if self._baseline is not None:
            return
        if isinstance(self._provider, AccountCapability):
            positions = await self._provider.positions()
        elif self._assume_flat:
            positions = []
        else:
            return
        quantities = _net_quantities(positions)
        record = await self._append(
            RecordKind.POSITION_BASELINE,
            {
                str(symbol): {"quantity": str(quantity)}
                for symbol, quantity in quantities.items()
            },
        )
        self._baseline = quantities
        self._sweep_floor = self._baseline_floor(record.ts)

    def _baseline_floor(self, captured_at: datetime) -> datetime | None:
        """Where fill queries may start, once a baseline is on record.

        A baseline taken before ismet sent any order already holds every fill dated
        before it, and none of those can be on an order ismet sent, so reporting them
        as orphans would halt a first start on an account that traded that day. Only a
        venue clock behind the local one can date a later fill before the baseline, and
        such a fill still shows as a position difference.
        """
        return None if self._orders else captured_at

    def _fill_window(self, since: datetime) -> datetime:
        if self._sweep_floor is None:
            return since
        return max(since, self._sweep_floor)

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
        fills_before = len(prior.fills) if prior is not None else 0
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
        if len(state.fills) > fills_before:
            self._set_mark(symbol, state.fills[-1].price)

    def net_position(self, symbol: Symbol) -> Decimal | None:
        """Baseline plus every recorded fill, or ``None`` if the baseline is unknown."""
        if self._baseline is None:
            return None
        return add(
            self._baseline.get(symbol, Decimal(0)),
            self._filled_net.get(symbol, Decimal(0)),
        )

    def _symbols_with_exposure(self) -> set[Symbol]:
        symbols = {o.request.symbol for o in self._open.values()}
        for symbol in self._tracked_symbols():
            if self.net_position(symbol):
                symbols.add(symbol)
        return symbols

    def _tracked_symbols(self) -> set[Symbol]:
        symbols = set(self._symbols_seen)
        if self._baseline:
            symbols |= set(self._baseline)
        return symbols

    async def _record(
        self,
        event: OrderEvent,
        *,
        durable: bool = False,
        only_if: OrderState | None = None,
    ) -> OrderState:
        """Journal and apply one event, atomically with respect to other events.

        ``durable=True`` is for events that must be on disk before the next step,
        and raises if the journal fails. Otherwise a journal failure stops new
        orders, and the event still reaches memory.

        A cancelled caller cannot split the journal from memory: the write and the
        update finish together before the cancellation is honoured.

        ``only_if`` drops the event unless the order is still in that state, checked
        atomically with other events. A conclusion drawn from a query is out of date
        once a newer event has landed.
        """
        return await self._uncancellable(
            self._record_now(event, durable=durable, only_if=only_if)
        )

    @staticmethod
    async def _uncancellable(work: Coroutine[Any, Any, T]) -> T:
        """Finish ``work`` before honouring a cancellation of the caller."""
        task = asyncio.create_task(work)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if not task.done():
                await asyncio.wait({task})
            if task.done() and not task.cancelled():
                task.exception()  # retrieved, so it is not reported as unobserved
            raise

    async def _record_now(
        self, event: OrderEvent, *, durable: bool, only_if: OrderState | None
    ) -> OrderState:
        anomalies: tuple[str, ...]
        async with self._state_lock:
            if only_if is not None:
                latest = self._orders[event.client_order_id]
                if latest is not only_if:
                    return latest
            try:
                await self._write_unjournalled()
                await self._write_event(event)
            except (JournalError, ConfigError, OSError) as exc:
                if durable:
                    raise
                self._unjournalled.append(event)
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

    async def _write_event(self, event: OrderEvent) -> None:
        await self._append(
            RecordKind.ORDER_EVENT,
            dump_order_event(event),
            client_order_id=event.client_order_id,
            unique_intent=isinstance(event, SubmitIntent),
        )

    async def _write_unjournalled(self) -> None:
        """Write events a failed journal missed, first and in order, so replay holds.

        New orders stay refused until a restart even once this succeeds: the failure
        means this process's record of the outage cannot be fully trusted.
        """
        while self._unjournalled:
            await self._write_event(self._unjournalled[0])
            self._unjournalled.pop(0)

    async def _retry_unjournalled(self) -> None:
        if not self._unjournalled:
            return
        async with self._state_lock:
            try:
                await self._write_unjournalled()
            except (JournalError, ConfigError, OSError) as exc:
                logger.critical("journal still failing: %s", exc)

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
        if self._started and _is_halting(message):
            await self._halt(f"order {cid}: {message}")

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

        try:
            return await asyncio.wait_for(waiter(), timeout)
        except asyncio.TimeoutError:
            # On Python 3.10 this is not the builtin TimeoutError.
            raise TimeoutError(
                f"order {client_order_id} did not reach the state within {timeout}s"
            ) from None

    # Kill switch

    @property
    def kill_state(self) -> KillState:
        return self._kill

    async def engage_kill(
        self, reason: str, *, operator: str = "api", cancel_open: bool = True
    ) -> KillState:
        """Stop new orders, durably. With ``cancel_open``, also cancel what is open.

        In-memory state changes first, so the switch works even if the journal is
        down, and the record is retried until it lands. Cancels are retried by a
        background sweep until nothing is open. Engaging when already engaged keeps the
        original reason and can only widen the action: a later automatic halt cannot
        stop a cancel sweep an operator asked for. That judgment reads the journal
        first, since memory may predate a release or an engage from another process.
        """
        await self._refresh_kill()
        current = self._kill
        if current.engaged:
            if current.cancel_open or not cancel_open:
                if cancel_open and self._started:
                    await self.cancel_all()
                return current
            reason, operator = current.reason, current.operator
        self._kill = KillState(True, reason, operator, current.seq, cancel_open)
        self._kill_unrecorded = {
            "reason": reason,
            "operator": operator,
            "cancel_open": cancel_open,
        }
        logger.critical("kill switch engaged by %s: %s", operator, reason)
        await self._record_kill()
        if cancel_open and self._started:
            await self.cancel_all()
        return self._kill

    async def _halt(self, reason: str) -> None:
        """Stop new orders without touching what is open, unless already engaged."""
        await self._refresh_kill()
        if not self._kill.engaged:
            await self.engage_kill(reason, operator="oms", cancel_open=False)

    async def _refresh_kill(self) -> None:
        """Adopt the journal's kill state. With the journal down, memory stands."""
        try:
            await self._merge_kill(await self._call_journal(self._journal.kill_state))
        except Exception:
            logger.exception("could not read the kill switch from the journal")

    async def _record_kill(self) -> None:
        """Try to make an engaged kill durable. A restart must not lose it."""
        payload = self._kill_unrecorded
        if payload is None:
            return
        try:
            record = await self._append(RecordKind.KILL_ENGAGED, payload)
        except (IsmetError, OSError) as exc:
            self._journal_failed = str(exc)
            logger.critical("kill switch could not be journalled: %s", exc)
            return
        if self._kill_unrecorded is not payload:
            return  # superseded during the write, by a wider engage or a release
        self._kill_unrecorded = None
        self._kill = KillState(
            True,
            payload["reason"],
            payload["operator"],
            record.seq,
            payload["cancel_open"],
        )

    async def release_kill(self, reason: str, *, operator: str) -> KillState:
        """Allow new orders again. Requires a named operator; journalled."""
        if not operator.strip() or not reason.strip():
            raise ValidationError(
                "releasing the kill switch requires an operator and reason"
            )
        record = await self._append(
            RecordKind.KILL_RELEASED, {"reason": reason, "operator": operator}
        )
        self._kill_unrecorded = None
        self._kill = KillState(False, reason, operator, record.seq)
        logger.warning("kill switch released by %s: %s", operator, reason)
        return self._kill

    async def _merge_kill(self, durable: KillState) -> None:
        """Adopt a kill state read from the journal only if it is newer and believable.

        A read can race a local engage, and a local engage may not have reached the
        journal yet. In both cases the journal looks "not engaged", and believing it
        would silently switch the kill switch off. A release from elsewhere must sit on
        an intact chain: anyone can insert a row in a SQLite file. The journal has
        already ignored one naming no operator.
        """
        if self._kill_unrecorded is not None or durable.seq <= self._kill.seq:
            return
        if durable.engaged:
            self._kill = durable
            return
        if self._kill.engaged:
            result = await self._call_journal(self._journal.verify)
            if not result.ok:
                logger.critical(
                    "ignoring a kill switch release: journal failed verification at "
                    "record %s: %s",
                    result.first_bad_seq,
                    result.detail,
                )
                return
        self._kill = durable

    async def _check_can_trade(self) -> None:
        """Refuse a new order if exposure cannot be trusted right now."""
        await self._merge_kill(await self._call_journal(self._journal.kill_state))
        if self._kill.engaged:
            raise KillSwitchEngaged(self._kill.reason)
        if self._journal_failed is not None:
            raise KillSwitchEngaged(f"journal failure: {self._journal_failed}")
        if self._reconcile_blind is not None:
            raise RiskRejected("reconciliation", self._reconcile_blind)
        if not self.stream_healthy:
            raise RiskRejected("execution_stream", "the execution stream is down")

    async def _kill_loop(self) -> None:
        while True:
            await asyncio.sleep(self._kill_poll_interval)
            try:
                await self._record_kill()
                await self._retry_unjournalled()
                await self._merge_kill(
                    await self._call_journal(self._journal.kill_state)
                )
                if self._kill.engaged and self._kill.cancel_open and self.open_orders():
                    await self.cancel_all()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("kill switch sweep failed")

    # Submit

    async def _instrument(self, symbol: Symbol) -> Instrument | None:
        """Reference data for ``symbol``, refreshed after ``instrument_ttl``.

        A failed refresh returns ``None``, which refuses the order. Terms such as the
        multiplier stay available for exposure on orders already open.
        """
        now = self._clock.monotonic()
        cached = self._instruments.get(symbol)
        fetched_at = self._instruments_at.get(symbol, now)
        if cached is not None and now - fetched_at < self._instrument_ttl:
            return cached
        try:
            if self._instrument_source is not None:
                found = await self._instrument_source(symbol)
            elif isinstance(self._provider, ReferenceDataCapability):
                found = await self._provider.instrument(symbol)
            else:
                return None
        except Exception:
            logger.exception("instrument lookup failed for %s", symbol)
            return None
        self._instruments[symbol] = found
        self._instruments_at[symbol] = now
        return found

    async def _reference(self, symbol: Symbol) -> Quote | None:
        try:
            if self._reference_source is not None:
                return await self._reference_source(symbol)
            if isinstance(self._provider, MarketDataCapability):
                return await self._provider.quote(symbol)
        except Exception:
            logger.exception("reference quote failed for %s", symbol)
        return None

    def _gross_notional(
        self, exclude: str | None = None, currency: str | None = None
    ) -> Decimal | None:
        """Absolute value of positions and open orders at current marks.

        ``None`` if holdings, a mark or an instrument's multiplier is unknown, or if
        exposure spans currencies, since adding them would need a rate nobody declared.
        All of these fail a gross limit closed. A multiplier of 1 is never assumed.
        """
        if self._baseline is None:
            return None
        currencies = {currency} if currency is not None else set()
        gross = Decimal(0)
        for symbol in self._tracked_symbols():
            position = self.net_position(symbol)
            if position is None:
                return None
            if position == 0:
                continue
            instrument = self._instruments.get(symbol)
            mark = self._mark(symbol)
            if mark is None or instrument is None:
                return None
            currencies.add(instrument.currency)
            value = mul(mul(position, mark), instrument.multiplier)
            gross = add(gross, absolute(value))
        for order in self.open_orders():
            if order.client_order_id == exclude:
                continue
            symbol = order.request.symbol
            prices = [
                absolute(price)
                for price in (order.limit_price, order.pending_limit_price)
                if price is not None
            ]
            price = (
                max(prices)
                if prices
                else order.request.stop_price
                if order.request.stop_price is not None
                else self._mark(symbol)
            )
            instrument = self._instruments.get(symbol)
            if price is None or instrument is None:
                return None
            currencies.add(instrument.currency)
            value = mul(mul(order.leaves_quantity, price), instrument.multiplier)
            gross = add(gross, absolute(value))
        return gross if len(currencies) <= 1 else None

    def _mark(self, symbol: Symbol) -> Decimal | None:
        entry = self._marks.get(symbol)
        return None if entry is None else entry[0]

    def _set_mark(self, symbol: Symbol, price: Decimal) -> None:
        if price == 0:
            self._marks.pop(symbol, None)  # a value of zero would pass any gross limit
        else:
            self._marks[symbol] = (price, self._clock.monotonic())

    def _mark_quote(self, symbol: Symbol, reference: Quote | None) -> None:
        """Adopt a quote as the mark. One that is missing or old leaves no mark."""
        limit = float(self._risk.config.max_reference_age_seconds)
        price = None
        if reference is not None and (
            abs((self._clock.now() - reference.exchange_ts).total_seconds()) <= limit
        ):
            price = reference.mid if reference.mid is not None else reference.last
        if price is None:
            self._marks.pop(symbol, None)
        else:
            self._set_mark(symbol, price)

    async def _refresh_marks(self, request: OrderRequest) -> None:
        """Bring marks up to date, if a gross limit applies to this order.

        A held symbol whose reference data is still missing, after a failed lookup at
        start or a position adjustment, is looked up again here, or its gross exposure
        stays unknown for good.
        """
        scopes = self._risk.config.limits_for(request)
        if all(limits.max_gross_notional is None for _, limits in scopes):
            return
        limit = float(self._risk.config.max_reference_age_seconds)
        now = self._clock.monotonic()
        exposed = self._symbols_with_exposure() - {request.symbol}
        stale = [
            symbol
            for symbol in exposed
            if symbol not in self._marks or now - self._marks[symbol][1] > limit
        ]
        missing = [symbol for symbol in exposed if symbol not in self._instruments]
        quotes = await asyncio.gather(*(self._reference(symbol) for symbol in stale))
        for symbol, quote in zip(stale, quotes, strict=True):
            self._mark_quote(symbol, quote)
        await asyncio.gather(*(self._instrument(symbol) for symbol in missing))

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
            gross_notional=self._gross_notional(
                exclude, None if instrument is None else instrument.currency
            ),
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
        existing = self._existing_order(request)
        if existing is not None:
            return existing
        instrument = await self._instrument(request.symbol)
        reference = await self._reference(request.symbol)
        self._mark_quote(request.symbol, reference)
        await self._refresh_marks(request)
        async with self._submit_lock:
            existing = self._existing_order(request)
            if existing is not None:
                return existing
            await self._check_can_trade()
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
            self._submitting.add(cid)
            try:
                await self._record(
                    SubmitIntent(
                        client_order_id=cid, ts=self._clock.now(), request=request
                    ),
                    durable=True,
                )
            except asyncio.CancelledError:
                # The cancelled write still finished, so the intent is on record.
                await self._close_unsent(cid, "CANCELED_BEFORE_SEND")
                self._submitting.discard(cid)
                raise
            except BaseException:
                self._submitting.discard(cid)
                raise
        try:
            self._pass("after_intent", cid)
            return await self._send(request)
        except asyncio.CancelledError:
            await self._close_unsent(cid, "CANCELED_BEFORE_SEND")
            raise
        except Exception:
            await self._close_unsent(cid, "NEVER_SENT")
            raise
        finally:
            self._submitting.discard(cid)
            self._cancel_before_send.discard(cid)

    async def _close_unsent(self, cid: str, code: str) -> None:
        """The send path stopped short and nothing went out, so record that."""
        state = self._orders.get(cid)
        if state is not None and state.status is OrderStatus.PENDING_SUBMIT:
            await self._refuse_send(cid, code)

    def _existing_order(self, request: OrderRequest) -> OrderState | None:
        existing = self._orders.get(request.client_order_id)
        if existing is not None and existing.request != request:
            raise ValidationError(
                f"client order id {request.client_order_id} already used for a "
                "different order"
            )
        return existing

    async def _send(self, request: OrderRequest) -> OrderState:
        cid = request.client_order_id
        try:
            if self._orders[cid].status is not OrderStatus.PENDING_SUBMIT:
                return self._orders[cid]
            refusal = self._send_refusal(cid)
            if refusal is not None:
                return await self._refuse_send(cid, refusal)
            state = await self._record(
                SendStarted(client_order_id=cid, ts=self._clock.now()), durable=True
            )
            self._pass("after_send_started", cid)
            # Writing that record took time: the order may have been cancelled or the
            # kill switch engaged meanwhile, and nothing has gone to the venue yet.
            if state.status is not OrderStatus.SUBMITTING:
                return state
            refusal = self._send_refusal(cid)
            if refusal is not None:
                return await self._refuse_send(cid, refusal)
            self._placing.add(cid)
            return await self._place(request)
        finally:
            self._placing.discard(cid)

    def _send_refusal(self, cid: str) -> str | None:
        if cid in self._cancel_before_send:
            return "CANCELED_BEFORE_SEND"
        return "KILL_SWITCH" if self._kill.engaged else None

    async def _refuse_send(self, cid: str, code: str) -> OrderState:
        return await self._record(
            Rejected(
                client_order_id=cid,
                ts=self._clock.now(),
                reason="stopped before the order was sent",
                code=code,
            )
        )

    async def _place(self, request: OrderRequest) -> OrderState:
        cid = request.client_order_id
        try:
            ack = await self._venue.place_order(request)
        except AmbiguousOutcome as exc:
            return await self._mark_unknown(cid, str(exc))
        except DuplicateOrderId as exc:
            # An order with this id is at the venue, whatever became of this request.
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
        self._pass("after_venue_accept", cid)
        return await self._record(
            Acknowledged(
                client_order_id=cid,
                ts=self._clock.now(),
                origin=EventOrigin.VENUE,
                venue_order_id=ack.venue_order_id,
            )
        )

    async def _never_sent(self, cid: str) -> OrderState:
        return await self._record(
            Rejected(
                client_order_id=cid,
                ts=self._clock.now(),
                origin=EventOrigin.RECONCILE,
                reason="the send never started; never sent",
                code="NEVER_SENT",
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
        if client_order_id in self._submitting and (
            client_order_id not in self._placing
        ):
            # The send path has not called the venue, and decides what happens next, so
            # the choice between sending and cancelling is made in one place.
            self._cancel_before_send.add(client_order_id)
            return state
        if state.status is OrderStatus.PENDING_SUBMIT:
            return await self._refuse_send(client_order_id, "CANCELED_BEFORE_SEND")
        self._change_requested_at[client_order_id] = self._clock.monotonic()
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
        except Exception as exc:
            # A failure, or an adapter bug, says nothing about whether it was applied.
            return await self._mark_unknown(client_order_id, f"cancel: {exc!r}")
        return self.order(client_order_id)

    async def cancel_all(self, symbol: Symbol | None = None) -> list[OrderState]:
        """Cancel every open order, optionally in one symbol. Never stops early.

        A cancel already sent is left alone until ``cancel_retry_interval`` passes
        unanswered, then sent again.
        """
        now = self._clock.monotonic()
        slots = asyncio.Semaphore(_MAX_CONCURRENT_CANCELS)

        async def cancel_one(order: OrderState) -> OrderState:
            cid = order.client_order_id
            if order.status is OrderStatus.PENDING_CANCEL:
                sent = self._change_requested_at.get(cid)
                if sent is not None and now - sent < self._cancel_retry_interval:
                    return order
            async with slots:
                try:
                    return await self.cancel(cid)
                except Exception:
                    logger.exception("cancel failed for %s", cid)
                    return order

        # One slow answer must not hold back the cancels behind it.
        orders = self.open_orders(symbol)
        return list(await asyncio.gather(*(cancel_one(o) for o in orders)))

    async def amend(
        self,
        client_order_id: str,
        *,
        quantity: Decimal | None = None,
        limit_price: Decimal | None = None,
    ) -> OrderState:
        """Change a working order's terms, after risk checks on the result."""
        self._require_started()
        if not self._venue.trading_rules().supports_amend:
            raise NotSupported(self._provider.name, "amend_order")
        amended = self._amended_request(client_order_id, quantity, limit_price)
        symbol = amended.symbol
        instrument = await self._instrument(symbol)
        reference = await self._reference(symbol)
        self._mark_quote(symbol, reference)
        await self._refresh_marks(amended)
        async with self._submit_lock:
            # Those awaits may have let fills or a cancel land.
            amended = self._amended_request(client_order_id, quantity, limit_price)
            await self._check_can_trade()
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
            self._change_requested_at[client_order_id] = self._clock.monotonic()
            state = await self._record(
                ReplaceRequested(
                    client_order_id=client_order_id,
                    ts=self._clock.now(),
                    quantity=quantity,
                    limit_price=limit_price,
                ),
                durable=True,
            )
        if state.status is not OrderStatus.PENDING_REPLACE:
            return state  # a cancel or a last fill won while the record was written
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
        except Exception as exc:
            return await self._mark_unknown(client_order_id, f"amend: {exc!r}")
        return self.order(client_order_id)

    def _amended_request(
        self,
        client_order_id: str,
        quantity: Decimal | None,
        limit_price: Decimal | None,
    ) -> OrderRequest:
        """What would be live after the amend: the unfilled part, on the new terms."""
        state = self.order(client_order_id)
        if state.status not in WORKING_STATUSES:
            raise OrderStateError(
                f"cannot amend {client_order_id} in {state.status.value}"
            )
        if quantity is not None and quantity <= state.filled_quantity:
            raise ValidationError(
                f"new quantity {quantity} is not above filled {state.filled_quantity}"
            )
        new_total = quantity if quantity is not None else state.quantity
        terms = state.request.model_dump()
        terms["quantity"] = sub(new_total, state.filled_quantity)
        if limit_price is not None:
            terms["limit_price"] = limit_price
        elif state.limit_price is not None:
            terms["limit_price"] = state.limit_price
        try:
            return OrderRequest.model_validate(terms)
        except ValueError as exc:
            raise ValidationError(f"invalid amend: {exc}") from exc

    async def adjust_position(
        self, symbol: Symbol, delta: Decimal, *, operator: str, reason: str
    ) -> None:
        """Record a position change no order explains, such as a split or a transfer.

        The next reconciliation compares against the adjusted position, which is how a
        ``POSITION_MISMATCH`` with an outside cause is cleared. Journalled, and also
        accepted from another process through ``ismet position adjust``.
        """
        if not operator.strip() or not reason.strip():
            raise ValidationError(
                "adjusting a position requires an operator and reason"
            )
        if self._baseline is None:
            raise ConfigError(
                "this venue's position is unknown, so it cannot be adjusted"
            )
        try:
            delta = to_decimal(delta)
        except (TypeError, ValueError) as exc:
            raise ValidationError(f"invalid position delta: {exc}") from exc
        await self._append(
            RecordKind.POSITION_ADJUSTED,
            {
                "symbol": str(symbol),
                "delta": str(delta),
                "operator": operator,
                "reason": reason,
            },
        )
        await self._merge_adjustments()

    def _apply_adjustment(self, record: JournalRecord) -> None:
        detail = json.loads(record.payload)
        if self._baseline is None or not names_operator(detail):
            logger.critical("ignoring position adjustment at record %s", record.seq)
            return
        symbol = Symbol.parse(detail["symbol"])
        self._baseline[symbol] = add(
            self._baseline.get(symbol, Decimal(0)), Decimal(detail["delta"])
        )
        logger.warning(
            "position of %s adjusted by %s: %s, %s",
            symbol,
            detail["delta"],
            detail["operator"],
            detail.get("reason", ""),
        )

    async def _merge_adjustments(self) -> None:
        """Adopt adjustments journalled since the last look, if the chain is intact.

        One look at a time: a second caller reading the same records during the
        verification would apply each adjustment twice.
        """
        async with self._adjust_lock:
            seen = self._adjustments_seen
            records = await self._call_journal(
                lambda: list(self._journal.records(after_seq=seen))
            )
            adjustments = [r for r in records if r.kind is RecordKind.POSITION_ADJUSTED]
            if adjustments:
                result = await self._call_journal(self._journal.verify)
                if not result.ok:
                    logger.critical(
                        "ignoring position adjustments: journal failed verification "
                        "at record %s: %s",
                        result.first_bad_seq,
                        result.detail,
                    )
                    return
            for record in adjustments:
                self._apply_adjustment(record)
                self._adjustments_seen = record.seq
            if records:
                self._adjustments_seen = records[-1].seq

    async def update_risk_config(self, config: RiskConfig, *, operator: str) -> None:
        """Replace risk limits, recording the change durably first."""
        if not operator.strip():
            raise ValidationError("changing risk limits requires an operator")
        await self._append(
            RecordKind.CONFIG_CHANGE,
            {"operator": operator, "risk": config.model_dump(mode="json")},
        )
        self._risk.update_config(config)

    # Venue events

    async def _on_venue_event(self, event: OrderEvent) -> None:
        cid = event.client_order_id
        if cid not in self._orders:
            # Keep the whole report: it may be the only record of a real execution.
            payload = dump_order_event(event)
            with contextlib.suppress(IsmetError, OSError):
                await self._append(
                    RecordKind.ANOMALY,
                    {"message": "event for an unknown order", "event": payload},
                    client_order_id=cid,
                )
            detail = f"{event.kind} from venue for an order ismet never sent: {payload}"
            if isinstance(event, Filled):
                subject = _orphan_fill_subject(event.fill)
                item = ReconBreak(BreakKind.ORPHAN_FILL, subject, detail)
            else:
                item = ReconBreak(BreakKind.ORPHAN_ORDER, cid, detail)
            await self._report_break(item)
            return
        if isinstance(event, FillBusted) and (
            event.fill_id in self._orders[cid].busted_fill_ids
        ):
            return  # a venue may repeat a bust, and the record already has it
        if isinstance(event, Filled) and fill_contradicts(
            self._orders[cid], event.fill
        ):
            # Recorded and halted on below; a sweep finding it again must not halt.
            self._reported_corrections.add(_fill_terms(event.fill))
        await self._record(event)

    async def _consume_stream(self, stream: AsyncIterator[OrderEvent]) -> None:
        async for event in stream:
            self.stream_healthy = True  # it delivered, so it exists
            self._stream_judged.set()
            await self._on_venue_event(event)

    async def _reconcile_quietly(self) -> None:
        try:
            await self.reconcile()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._reconcile_blind = f"reconciliation failed: {exc!r}"
            logger.exception("reconciliation failed")

    async def _stream_loop(self) -> None:
        backoff = ExponentialBackoff(base=0.1, maximum=10.0)
        attempt = 0
        while True:
            consumer: asyncio.Task[None] | None = None
            settled: asyncio.Task[None] | None = None
            try:
                stream = self._venue.stream_executions()
                # Consume first, then reconcile, so nothing falls between the two.
                consumer = asyncio.create_task(self._consume_stream(stream))
                await asyncio.sleep(0)
                if attempt:
                    await self._reconcile_quietly()
                await asyncio.wait({consumer}, timeout=self._stream_settle or 0)
                if not consumer.done():
                    self.stream_healthy = True
                    self._stream_judged.set()
                    if self._stream_settle is not None:
                        settled = asyncio.create_task(self._reconcile_quietly())
                await consumer
                raise TransportError("execution stream ended")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.stream_healthy:
                    attempt = 0  # only a stream that held up restarts the backoff
                self.stream_healthy = False
                self._stream_judged.set()
                logger.warning("execution stream lost: %s", exc)
            finally:
                for task in (consumer, settled):
                    if task is not None:
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError, Exception):
                            await task
            await asyncio.sleep(backoff.delay(attempt))
            attempt += 1

    # Reconciliation

    async def _reconcile_loop(self) -> None:
        interval = min(self._reconcile_interval, max(self._unknown_grace, 0.01))
        elapsed = 0.0
        while True:
            await asyncio.sleep(interval)
            elapsed += interval
            has_unknown = any(
                o.status in _RESOLVE and o.client_order_id not in self._submitting
                for o in self._open.values()
            )
            due = elapsed >= self._reconcile_interval
            if has_unknown or due or self._reconcile_blind is not None:
                elapsed = 0.0
                await self._reconcile_quietly()

    async def _report_break(self, item: ReconBreak) -> None:
        await self._uncancellable(self._report_break_now(item))

    async def _report_break_now(self, item: ReconBreak) -> None:
        """Report one break: the halt first, since a crash may stop what follows.

        An orphan fill counts as reported only here. Until then every pass finds it
        again, and once its record is durable a restart does not.
        """
        if item.kind is BreakKind.ORPHAN_FILL:
            if item.subject in self._reported_orphan_fills:
                return
            self._reported_orphan_fills.add(item.subject)
        logger.error(
            "reconciliation break: %s %s: %s",
            item.kind.value,
            item.subject,
            item.detail,
        )
        if item.is_critical and self._halt_on_break:
            await self._halt(f"reconciliation break: {item.kind.value} {item.subject}")
        with contextlib.suppress(IsmetError, OSError):
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

    async def reconcile(self) -> ReconReport:
        """Compare every open order, the venue's open orders, and positions.

        If any query failed, exposure is unverified, so new orders are refused until a
        later pass gets through. Background passes run frequently while that lasts.
        """
        async with self._recon_lock:
            report = ReconReport(at=self._clock.now())
            await self._merge_adjustments()
            for state in self.open_orders():
                cid = state.client_order_id
                if cid in self._submitting:
                    # Its send has not returned, so "not found" would mean nothing.
                    continue
                report.orders_checked += 1
                try:
                    await self._reconcile_order(state, report)
                except Exception as exc:
                    self._query_failed(report, state.client_order_id, exc)
            await self._find_orphans(report)
            await self._sweep_fills(report)
            await self._check_positions(report)
            failed = [b for b in report.breaks if b.kind is BreakKind.QUERY_FAILED]
            self._reconcile_blind = (
                f"could not verify exposure with the venue: {failed[0].detail}"
                if failed
                else None
            )
            for item in report.breaks:
                await self._report_break(item)
            return report

    @staticmethod
    def _query_failed(report: ReconReport, subject: str, exc: Exception) -> None:
        """Any exception, including an adapter bug, means this pass is not proof."""
        report.complete = False
        report.breaks.append(ReconBreak(BreakKind.QUERY_FAILED, subject, repr(exc)))

    async def _reconcile_order(self, state: OrderState, report: ReconReport) -> None:
        cid = state.client_order_id
        if state.status is OrderStatus.PENDING_SUBMIT:
            await self._never_sent(cid)  # nothing went out, so the venue cannot help
            report.resolved.append(cid)
            return
        snapshot = await self._venue.order_status(cid, state.request.symbol)
        if self._orders[cid] is not state:
            # The answer may predate what just landed, so ask again.
            state = self._orders[cid]
            snapshot = await self._venue.order_status(cid, state.request.symbol)
            if self._orders[cid] is not state:
                report.complete = False  # still moving: the next pass judges it
                return
        if state.is_terminal:
            return
        if snapshot is None:
            await self._resolve_not_found(state, report)
            return
        self._unknown_since.pop(cid, None)
        request = state.request
        if (snapshot.symbol, snapshot.side) != (request.symbol, request.side):
            report.breaks.append(
                ReconBreak(
                    BreakKind.ORPHAN_ORDER,
                    cid,
                    f"the venue's order under this id is {snapshot.side.value} "
                    f"{snapshot.symbol}; ismet sent {request.side.value} "
                    f"{request.symbol}",
                )
            )
            return
        await self._sync_terms(snapshot, report)
        if snapshot.filled_quantity != state.filled_quantity:
            latest = await self._sync_fills(state, snapshot, report)
            if latest is None:
                return
            snapshot = latest
        current = self._orders[cid]
        if current.is_terminal and not snapshot.status.is_terminal:
            return  # closed while the fills were fetched, which is newer than the query
        # Never close an order while its fills disagree: closing would hide the
        # difference from every later pass.
        fills_agree = current.filled_quantity == snapshot.filled_quantity
        if not fills_agree and snapshot.status.is_terminal:
            return
        if not self._status_differs(current, snapshot):
            return
        if current.status is OrderStatus.PENDING_REPLACE:
            await self._record(
                ReplaceRejected(
                    client_order_id=cid,
                    ts=self._clock.now(),
                    origin=EventOrigin.RECONCILE,
                    reason="the venue did not apply the change",
                ),
                only_if=current,
            )
        else:
            await self._record(
                Restated(
                    client_order_id=cid,
                    ts=self._clock.now(),
                    origin=EventOrigin.RECONCILE,
                    status=snapshot.status,
                    venue_order_id=snapshot.venue_order_id,
                    venue_filled_quantity=snapshot.filled_quantity,
                    reason="reconciliation",
                ),
                only_if=current,
            )
        report.resolved.append(cid)

    async def _sync_terms(
        self, snapshot: VenueOrderSnapshot, report: ReconReport
    ) -> None:
        """Adopt the venue's quantity and price, so exposure uses what is live."""
        cid = snapshot.client_order_id
        state = self._orders[cid]
        if snapshot.quantity == state.quantity and (
            snapshot.limit_price is None or snapshot.limit_price == state.limit_price
        ):
            return
        await self._record(
            Replaced(
                client_order_id=cid,
                ts=self._clock.now(),
                origin=EventOrigin.RECONCILE,
                quantity=snapshot.quantity,
                limit_price=(
                    snapshot.limit_price
                    if snapshot.limit_price is not None
                    else state.limit_price
                ),
                venue_order_id=snapshot.venue_order_id,
            ),
            only_if=state,
        )
        report.resolved.append(cid)

    def _status_differs(self, state: OrderState, snapshot: VenueOrderSnapshot) -> bool:
        if state.status is snapshot.status:
            return False
        if state.status in PENDING_STATUSES and snapshot.status in WORKING_STATUSES:
            # A cancel or replace the venue has not processed yet is not a disagreement,
            # until it has gone unanswered for too long.
            sent = self._change_requested_at.get(state.client_order_id)
            now = self._clock.monotonic()
            return sent is None or now - sent >= self._unknown_grace
        return True

    async def _sync_fills(
        self, state: OrderState, snapshot: VenueOrderSnapshot, report: ReconReport
    ) -> VenueOrderSnapshot | None:
        """Book the venue's fills. Returns the latest answer, or ``None`` if unsettled.

        A difference counts only when the venue's count held still across a fill query:
        any other difference can be a fill landing between the two questions.
        """
        cid = state.client_order_id
        for _ in range(_FILL_SYNC_ATTEMPTS):
            if not await self._recover_order_fills(state, report):
                return None
            current = self._orders[cid]
            if current.filled_quantity == snapshot.filled_quantity:
                report.resolved.append(cid)
                return snapshot
            fresh = await self._venue.order_status(cid, state.request.symbol)
            if fresh is None:
                return None
            if fresh.filled_quantity == snapshot.filled_quantity:
                report.breaks.append(
                    ReconBreak(
                        BreakKind.FILL_MISMATCH,
                        cid,
                        f"ismet has {self._orders[cid].filled_quantity} filled, venue "
                        f"reports {fresh.filled_quantity}",
                    )
                )
                return fresh
            snapshot = fresh
        report.complete = False  # still trading: the next pass judges it
        return None

    async def _recover_order_fills(
        self, state: OrderState, report: ReconReport
    ) -> bool:
        """Book any fills the venue has for this order. False if it could not ask."""
        cid = state.client_order_id
        since = self._created_at.get(cid, report.at) - self._fill_lookback
        try:
            fills = await self._venue.fills_since(since, state.request.symbol)
        except Exception as exc:
            self._query_failed(report, cid, exc)
            return False
        await self._book_fills([f for f in fills if f.client_order_id == cid], report)
        return True

    async def _sweep_fills(self, report: ReconReport) -> None:
        """Book every recent fill not already recorded, on any order.

        This is what catches a fill on an order already closed. Only a clean sweep moves
        the window forward, so a failure leaves the fills for the next pass to find, and
        the journal holds where the window stands so a restart resumes from there.
        """
        began = self._last_sweep if self._last_sweep is not None else report.at
        since = self._fill_window(began - self._fill_lookback)
        try:
            fills = await self._venue.fills_since(since)
        except Exception as exc:
            self._query_failed(report, "fills", exc)
            return
        await self._book_fills(fills, report)
        if not self._unjournalled:
            self._last_sweep = report.at
            await self._save_sweep(report.at)

    async def _save_sweep(self, at: datetime) -> None:
        """Note the window now and then: an older note only widens a restart's sweep."""
        saved = self._durable_sweep
        if saved is not None and at - saved < self._fill_lookback / 4:
            return
        with contextlib.suppress(IsmetError, OSError):
            await self._append(RecordKind.SWEEP_MARK, {"at": at.isoformat()})
            self._durable_sweep = at

    async def _book_fills(self, fills: Iterable[Fill], report: ReconReport) -> None:
        """Record what a venue query knows and this manager does not.

        A fill on an unknown order is reported once, per order and fill id, since a fill
        id is unique only per order. It reaches the journal, so an
        operator's release of the halt it causes is final, even across a restart. A
        known fill reported with other terms is recorded too, which raises the same
        halting anomaly the stream would, once per fill and set of terms, so the
        sweeps inside the lookback window do not halt again after a release.
        """
        for fill in fills:
            state = self._orders.get(fill.client_order_id)
            if state is None:
                subject = _orphan_fill_subject(fill)
                found = {b.subject for b in report.breaks}
                if subject not in self._reported_orphan_fills | found:
                    report.breaks.append(
                        ReconBreak(
                            BreakKind.ORPHAN_FILL,
                            subject,
                            f"{fill.side.value} {fill.quantity} {fill.symbol} at "
                            f"{fill.price} on {fill.client_order_id}, which ismet "
                            "never sent",
                        )
                    )
                continue
            if fill.fill_id in state.busted_fill_ids:
                continue
            known = any(f.fill_id == fill.fill_id for f in state.fills)
            if not known or fill_adds_detail(state, fill):
                await self._record_recovered_fill(fill)
                report.resolved.append(fill.client_order_id)
            elif fill_contradicts(state, fill):
                terms = _fill_terms(fill)
                if terms not in self._reported_corrections:
                    self._reported_corrections.add(terms)
                    await self._record_recovered_fill(fill)

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
        """The venue's status query says it has no such order.

        An order still in flight can look missing, so wait out ``unknown_grace``, then
        send a cancel by client order id to kill anything arriving late, and ask once
        more. An order the venue later purged can look missing too, so check for fills
        before closing it. Only an order with no cancel failure, no status, and no fills
        is declared never accepted.
        """
        cid = state.client_order_id
        symbol = state.request.symbol
        now = self._clock.monotonic()
        first_seen = self._unknown_since.setdefault(cid, now)
        if now - first_seen < self._unknown_grace:
            report.complete = False
            return
        await self._record(
            CancelRequested(
                client_order_id=cid, ts=self._clock.now(), origin=EventOrigin.RECONCILE
            )
        )
        try:
            await self._venue.cancel_order(cid, symbol)
        except VenueError:
            pass  # the expected answer for an order the venue does not have
        except Exception as exc:
            # The protective cancel did not go through, so a late arrival is still live.
            self._query_failed(report, cid, exc)
            return
        snapshot = await self._venue.order_status(cid, symbol)
        if snapshot is not None:
            return  # it did arrive; the next pass reconciles it
        if not await self._recover_order_fills(state, report):
            return
        current = self._orders[cid]
        if current.is_terminal:
            report.resolved.append(cid)
            return
        if state.status in _RESOLVE and current.filled_quantity == 0:
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
                reason="venue no longer has this order",
            )
        )
        report.breaks.append(
            ReconBreak(
                BreakKind.MISSING_AT_VENUE,
                cid,
                f"was {state.status.value}; {current.filled_quantity} filled",
            )
        )

    async def _find_orphans(self, report: ReconReport) -> None:
        try:
            venue_open = await self._venue.open_orders()
        except Exception as exc:
            self._query_failed(report, "open_orders", exc)
            return
        for snapshot in venue_open:
            state = self._orders.get(snapshot.client_order_id)
            if state is not None and state.is_terminal:
                # The list may predate an event already recorded here, so ask again.
                current = await self._current_snapshot(snapshot, report)
                if current is None or current.status.is_terminal:
                    continue
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

    async def _current_snapshot(
        self, snapshot: VenueOrderSnapshot, report: ReconReport
    ) -> VenueOrderSnapshot | None:
        try:
            return await self._venue.order_status(
                snapshot.client_order_id, snapshot.symbol
            )
        except Exception as exc:
            self._query_failed(report, snapshot.client_order_id, exc)
            return None

    async def _check_positions(self, report: ReconReport) -> None:
        if self._baseline is None or not isinstance(self._provider, AccountCapability):
            report.positions_verified = False
            return
        try:
            positions = await self._provider.positions()
        except Exception as exc:
            report.positions_verified = False
            self._query_failed(report, "positions", exc)
            return
        venue = _net_quantities(positions)
        now = self._clock.monotonic()
        for symbol in sorted({*venue, *self._tracked_symbols()}, key=str):
            ours = self.net_position(symbol) or Decimal(0)
            theirs = venue.get(symbol, Decimal(0))
            if ours == theirs:
                self._position_suspect_since.pop(symbol, None)
                continue
            await self._recover_missing_fills(symbol, report)
            ours = self.net_position(symbol) or Decimal(0)
            # The first answer may predate a fill recorded while it was in flight.
            try:
                fresh = await self._provider.positions()
            except Exception as exc:
                report.positions_verified = False
                self._query_failed(report, str(symbol), exc)
                continue
            theirs = _net_quantities(fresh).get(symbol, Decimal(0))
            if ours == theirs:
                self._position_suspect_since.pop(symbol, None)
                continue
            first_seen = self._position_suspect_since.setdefault(symbol, now)
            if now - first_seen < self._unknown_grace:
                report.complete = False  # brokers' positions can lag their fills
                continue
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
        since = (min(times) if times else report.at) - self._fill_lookback
        try:
            fills = await self._venue.fills_since(self._fill_window(since), symbol)
        except Exception as exc:
            self._query_failed(report, str(symbol), exc)
            return
        await self._book_fills(fills, report)


def events_of(journal: Journal) -> Iterable[OrderEvent]:
    """Every order event in a journal, in order, for audit and replay."""
    for record in journal.records():
        if record.kind is RecordKind.ORDER_EVENT:
            yield parse_order_event(record.payload)


__all__ = ["OrderManager", "events_of"]
