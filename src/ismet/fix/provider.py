"""Order entry over FIX: :class:`TradingCapability` on a :class:`FixSession`.

Spec references: FIX 4.2 with errata (messages, Appendix D scenarios), FIX 4.4 and FIX
5.0 SP2 via fiximate.fixtrading.org for ExecType F, G, H, and I, and the "FIX Transport"
document for PossResend.

What FIX does and does not offer, and how this provider handles each:

- ``place_order``: at most one NewOrderSingle per client order id, enforced by the
  durable store, under one sequence number. If the session is down it raises
  :class:`~ismet.errors.TransportError` having stored nothing. Once the message is
  stored, a failure raises :class:`~ismet.errors.AmbiguousOutcome`, including a timeout
  or a disconnect before the ack. A new sequence number is never used for an order, and
  PossResend is never set. The session may retransmit a stored order under its original
  number and PossDupFlag=Y if the venue asks, which the spec requires, and the venue
  deduplicates by ClOrdID. ``FixSessionConfig.stale_resend_after`` gap-fills aged
  orders.
- A reject with OrdRejReason=6 means the venue already holds the ClOrdID, which is what
  a resent order can get. It raises :class:`~ismet.errors.DuplicateOrderId` and does not
  reach the stream, because it says nothing against the original order.
- ``order_status`` sends OrderStatusRequest(H). FIX has no universal "not found".
  Appendix D scenario D24 answers an unknown order with an ExecutionReport with
  OrdStatus=Rejected, ExecType=Order Status (ExecTransType=Status in 4.2), and
  OrdRejReason=Unknown Order. A BusinessMessageReject with BusinessRejectReason=Unknown
  ID is treated the same, since that is what its code says. **Assumption:** the venue
  uses one of those; if not, override :meth:`FixDialect.is_unknown_order`. A timeout, a
  session Reject, or any other reject raises and never returns ``None``. The one other
  case that cannot be asked is an order with no row in the store: FIX requires Side,
  which only that row holds. It raises rather than returning ``None``, because a wiped
  or misconfigured store must not make a live order look dead. An order whose process
  died between the order manager's ``send_started`` and this row stays ``UNKNOWN``
  until an operator resolves it.
- ``cancel_order`` and ``amend_order`` send F and G under a fresh ClOrdID, as FIX
  requires, with OrigClOrdID naming the last accepted one. The chain is durable, so a
  report under any of its ClOrdIDs maps back to the caller's client order id. They wait
  for the first reply: OrderCancelReject(9) raises ``VenueError``, and any execution
  report returns. A timeout or disconnect raises ``AmbiguousOutcome``.
- ``open_orders`` and ``fills_since``: FIX has no universal query for these, since
  OrderMassStatusRequest and TradeCaptureReportRequest are optional and venue-specific.
  Both answer from the durable local record of what this session sent and was told, so
  **an order placed outside this session is not listed** until a report arrives.
- ``stream_executions``: execution reports are durable in the store before the inbound
  sequence number advances. After a crash, ones the order manager never saw are found by
  reconciliation through ``order_status`` and ``fills_since``.
- An execution report that cannot be applied still consumes its sequence number, as
  the session test cases require, and could have been a fill. So it is quarantined
  durably, its order is restated as ``UNKNOWN``, and the stream stops raising
  :class:`ReportQuarantinedError` until an operator calls
  :meth:`FixProvider.release_quarantine`. The order manager then refuses new orders and
  its reconciliation compares CumQty with the venue. This fails closed: one malformed
  report stops new orders on this provider. A report with only a field defect such as
  an empty Text is applied and then rejected.
- Fill identity: ``fill_id`` is ExecID(17). ExecID is unique within a day or the life
  of a multi-day order (FIX 4.2 field definition), so duplicates are detected per order.
  A repeat with the same content is dropped. A repeated fill with different content is
  forwarded, so the order manager flags it. PossResend=Y reports are forwarded to this
  same check, as "Standard Message Header" says to.
- ExecType G/H, or ExecTransType 2/1 in 4.2, name the affected execution in ExecRefID:
  H becomes :class:`FillBusted`, and G becomes a bust plus a fill under the new ExecID.

Credentials stay in :class:`FixSessionConfig`, as :class:`pydantic.SecretStr`.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
from collections import OrderedDict, deque
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal
from enum import Enum
from functools import partial
from pathlib import Path
from typing import Any, ClassVar, TypeVar

import pydantic
from pydantic import BaseModel, ConfigDict

from ismet.config import ProviderSettings
from ismet.errors import (
    AmbiguousOutcome,
    ConfigError,
    DuplicateOrderId,
    IsmetError,
    TransportError,
    VenueError,
)
from ismet.fix.message import (
    FieldFormatError,
    FixMessage,
    format_utc_timestamp,
    parse_decimal,
    parse_seq_num,
    parse_utc_timestamp,
)
from ismet.fix.session import (
    ApplicationRejectError,
    FieldValue,
    FixSession,
    FixSessionConfig,
    SessionDownError,
    SessionFailedError,
)
from ismet.fix.store import (
    FixStore,
    FixStoreError,
    QuarantinedMessage,
    SqliteFixStore,
    StoredExecution,
)
from ismet.models import Symbol
from ismet.models.orders import (
    Acknowledged,
    Canceled,
    CancelRejected,
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
    Rejected,
    Replaced,
    ReplaceRejected,
    Restated,
    ShortSaleMark,
    TimeInForce,
    TradingRules,
    VenueOrderSnapshot,
)
from ismet.providers.base import Provider
from ismet.transport.clock import SYSTEM_CLOCK, Clock

logger = logging.getLogger("ismet.fix")

T = TypeVar("T")
Fields = list[tuple[int, FieldValue]]

_REJECT_REQUIRED_TAG = 1
_REJECT_BAD_VALUE = 5
_REJECT_BAD_FORMAT = 6

_HEADER_TAGS = frozenset({34, 43, 49, 50, 52, 56, 57, 97, 115, 116, 122, 128, 129})
_BACKLOG_LIMIT = 100_000
_MULTI_DAY = frozenset({TimeInForce.GTC, TimeInForce.GTD})
_SENT_INDEX_LIMIT = 10_000


@dataclass(frozen=True)
class FixTags:
    """Business tags, with the FIX standard numbers. A venue can replace any of them."""

    account: int = 1
    avg_px: int = 6
    client_order_id: int = 11
    commission: int = 12
    commission_type: int = 13
    cum_qty: int = 14
    currency: int = 15
    exec_id: int = 17
    exec_inst: int = 18
    exec_ref_id: int = 19
    exec_trans_type: int = 20
    handl_inst: int = 21
    last_px: int = 31
    last_qty: int = 32
    ord_status_req_id: int = 790
    order_id: int = 37
    order_qty: int = 38
    ord_status: int = 39
    ord_type: int = 40
    orig_client_order_id: int = 41
    price: int = 44
    ref_seq_num: int = 45
    side: int = 54
    symbol: int = 55
    text: int = 58
    time_in_force: int = 59
    transact_time: int = 60
    stop_px: int = 99
    cxl_rej_reason: int = 102
    ord_rej_reason: int = 103
    expire_time: int = 126
    exec_type: int = 150
    ref_msg_type: int = 372
    business_reject_ref_id: int = 379
    business_reject_reason: int = 380
    session_reject_reason: int = 373
    cxl_rej_response_to: int = 434
    last_liquidity_ind: int = 851


_ORD_STATUS: Mapping[str, OrderStatus] = {
    "0": OrderStatus.ACCEPTED,
    "1": OrderStatus.PARTIALLY_FILLED,
    "2": OrderStatus.FILLED,
    "3": OrderStatus.EXPIRED,  # Done for day: no further executions today.
    "4": OrderStatus.CANCELED,
    "5": OrderStatus.ACCEPTED,  # Replaced, which 4.2 reports in OrdStatus.
    "6": OrderStatus.PENDING_CANCEL,
    "7": OrderStatus.ACCEPTED,  # Stopped: still working, at a guaranteed price.
    "8": OrderStatus.REJECTED,
    "9": OrderStatus.ACCEPTED,  # Suspended: held by the venue, not ended.
    "A": OrderStatus.SUBMITTING,  # Pending new: received, not yet accepted.
    "B": OrderStatus.ACCEPTED,  # Calculated.
    "C": OrderStatus.EXPIRED,
    "D": OrderStatus.ACCEPTED,  # Accepted for bidding.
    "E": OrderStatus.PENDING_REPLACE,
}


@dataclass(frozen=True)
class FixDialect:
    """How one venue's FIX differs from the standard. Override fields or subclass.

    ``handl_inst`` is required on D and G in FIX 4.2 and optional later. ``None`` omits
    it. ``status_request_ids`` adds OrdStatusReqID(790), which arrived in FIX 4.3, and
    ``None`` decides from the BeginString. ``ignored_msg_types`` are application
    messages accepted without action. Anything else unrecognised gets a
    BusinessMessageReject, as test case 2r requires.
    """

    tags: FixTags = field(default_factory=FixTags)
    side_codes: Mapping[OrderSide, str] = field(
        default_factory=lambda: {OrderSide.BUY: "1", OrderSide.SELL: "2"}
    )
    short_side_codes: Mapping[ShortSaleMark, str] = field(
        default_factory=lambda: {
            ShortSaleMark.SHORT: "5",
            ShortSaleMark.SHORT_EXEMPT: "6",
        }
    )
    ord_type_codes: Mapping[OrderType, str] = field(
        default_factory=lambda: {
            OrderType.MARKET: "1",
            OrderType.LIMIT: "2",
            OrderType.STOP: "3",
            OrderType.STOP_LIMIT: "4",
        }
    )
    tif_codes: Mapping[TimeInForce, str] = field(
        default_factory=lambda: {
            TimeInForce.DAY: "0",
            TimeInForce.GTC: "1",
            TimeInForce.OPG: "2",
            TimeInForce.IOC: "3",
            TimeInForce.FOK: "4",
            TimeInForce.GTD: "6",
            TimeInForce.CLS: "7",
        }
    )
    ord_status: Mapping[str, OrderStatus] = field(default_factory=lambda: _ORD_STATUS)
    post_only_exec_inst: str | None = "6"
    handl_inst: str | None = "1"
    status_request_ids: bool | None = None
    unknown_order_reasons: frozenset[str] = frozenset({"5"})
    duplicate_order_reasons: frozenset[str] = frozenset({"6"})
    unknown_id_business_reasons: frozenset[str] = frozenset({"1"})
    ignored_msg_types: frozenset[str] = frozenset({"B", "C"})
    default_venue: str | None = None
    supports_amend: bool = True
    supports_short: bool = True
    client_order_id_max_length: int = 60

    def instrument_fields(self, symbol: Symbol) -> Fields:
        """How an instrument is named. Override to add SecurityID or an exchange."""
        return [(self.tags.symbol, symbol.ticker)]

    def side_of(self, code: str) -> OrderSide | None:
        """A Side value, reduced to its direction. ``None`` if it has none."""
        if code in ("1", "3"):
            return OrderSide.BUY
        if code in ("2", "4", "5", "6"):
            return OrderSide.SELL
        return None

    def wire_id(self, client_order_id: str, sequence: int) -> str:
        """The ClOrdID of the ``sequence``-th cancel or replace on an order.

        ``~`` cannot occur in a client order id, so a chained id cannot collide with
        another order's. It must fit the venue's limit, which is why
        ``client_order_id_max_length`` is lower than the 64 ismet allows.
        """
        return f"{client_order_id}~{sequence}"

    def is_unknown_order(self, report: FixMessage) -> bool:
        """Whether a status reply means the venue has no such order (scenario D24)."""
        tags = self.tags
        return (
            report.get(tags.ord_status) == "8"
            and report.get(tags.ord_rej_reason) in self.unknown_order_reasons
        )

    def is_duplicate_reject(self, report: FixMessage) -> bool:
        """A reject saying the venue already holds this ClOrdID, as a resent order gets.

        It says nothing against the original order, which may be live.
        """
        tags = self.tags
        return (
            report.get(tags.exec_type) == "8"
            and report.get(tags.ord_rej_reason) in self.duplicate_order_reasons
        )

    def is_fill_report(self, report: FixMessage) -> bool:
        """4.4 and later report a trade as F, and 4.2 as a partial fill or fill."""
        return report.get(self.tags.exec_type) in ("F", "1", "2")

    def trading_rules(self) -> TradingRules:
        return TradingRules(
            order_types=frozenset(self.ord_type_codes),
            time_in_force=frozenset(self.tif_codes),
            supports_amend=self.supports_amend,
            supports_short=self.supports_short,
            supports_post_only=self.post_only_exec_inst is not None,
            supports_cancel_on_disconnect=False,
            client_order_id_max_length=self.client_order_id_max_length,
        )


class ReportQuarantinedError(TransportError):
    """An execution report could not be applied. See :meth:`FixProvider.quarantined`."""

    def __init__(self, message: str) -> None:
        super().__init__(message, retryable=False)


class _Record(BaseModel):
    """What this provider remembers about an order, durable in the store."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    client_order_id: str
    symbol: Symbol
    side: OrderSide
    short_sale: ShortSaleMark
    quantity: Decimal
    order_type: OrderType
    time_in_force: TimeInForce
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    expire_at: datetime | None = None
    account: str | None = None
    post_only: bool = False
    wire_id: str
    chain_length: int = 0
    acknowledged: bool = False
    venue_order_id: str | None = None
    status: OrderStatus = OrderStatus.SUBMITTING
    filled: Decimal = Decimal(0)
    updated_at: datetime


class _Kind(str, Enum):
    NEW = "new"
    CANCEL = "cancel"
    REPLACE = "replace"
    STATUS = "status"


@dataclass
class _Pending:
    """A request waiting for its first reply."""

    kind: _Kind
    client_order_id: str
    wire_id: str
    future: asyncio.Future[VenueOrderSnapshot | OrderAck | None]
    key: str
    seq: int | None = None


_TYPE_OF: Mapping[_Kind, str] = {
    _Kind.NEW: "D",
    _Kind.CANCEL: "F",
    _Kind.REPLACE: "G",
    _Kind.STATUS: "H",
}


@dataclass(frozen=True)
class _Sent:
    """What a sequence number carried, so a late session Reject can be understood."""

    kind: _Kind
    client_order_id: str
    wire_id: str


class FixProvider(Provider):
    """Trading over one FIX session. See the module docstring for the semantics.

    Subclass to name a venue (``name`` and ``venues``) or to pass a :class:`FixDialect`.
    ``environment`` stays ``LIVE`` unless the caller says otherwise, so the order
    manager demands ``allow_live`` by default. A store outlives its provider, and the
    caller closes it, unless it came from :meth:`from_settings`.
    """

    name: ClassVar[str] = "fix"

    def __init__(
        self,
        config: FixSessionConfig,
        store: FixStore,
        *,
        dialect: FixDialect | None = None,
        ack_timeout: float = 10.0,
        query_timeout: float = 10.0,
        logon_timeout: float = 30.0,
        environment: Environment = Environment.LIVE,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        super().__init__(clock=clock)
        self.environment = environment
        chosen = dialect or FixDialect()
        if config.begin_string == "FIX.4.2":
            # TimeInForce 7 (at the close) was added in FIX 4.3.
            codes: dict[TimeInForce, str] = {
                tif: code
                for tif, code in chosen.tif_codes.items()
                if tif is not TimeInForce.CLS
            }
            chosen = replace(chosen, tif_codes=codes)
        self.dialect = chosen
        self._store = store
        self._owns_store = False
        self._session = FixSession(config, store, self, clock=clock)
        self._ack_timeout = ack_timeout
        self._query_timeout = query_timeout
        self._logon_timeout = logon_timeout
        self._pending: dict[str, _Pending] = {}
        self._pending_by_seq: dict[int, _Pending] = {}
        self._sent: OrderedDict[int, _Sent] = OrderedDict()
        self._subscribers: list[asyncio.Queue[OrderEvent | Exception]] = []
        self._backlog: deque[OrderEvent | ReportQuarantinedError] = deque()
        self._records_lock = asyncio.Lock()
        self._failure: Exception | None = None
        self._request_counter = 0
        self._begin_string = config.begin_string
        self.dropped_reports = 0
        """Reports that could not be mapped. Reconciliation is what recovers them."""

    @classmethod
    def from_settings(cls, settings: ProviderSettings) -> FixProvider:
        """Build from settings.

        Options: ``host``, ``port``, ``begin_string``, ``sender_comp_id``,
        ``target_comp_id``, ``store_path``, and optionally ``heartbeat_interval``,
        ``default_appl_ver_id``. Credentials: ``username`` and ``password``, if the
        venue uses them. ``environment`` may be ``simulated`` or ``paper`` to opt out
        of LIVE.
        """
        missing = [
            key
            for key in (
                "host",
                "port",
                "begin_string",
                "sender_comp_id",
                "target_comp_id",
                "store_path",
            )
            if settings.option(key) is None
        ]
        if missing:
            raise ConfigError(
                f"provider {settings.name!r} is missing options: {', '.join(missing)}"
            )
        config = FixSessionConfig(
            host=str(settings.option("host")),
            port=int(settings.option("port")),
            begin_string=str(settings.option("begin_string")),
            sender_comp_id=str(settings.option("sender_comp_id")),
            target_comp_id=str(settings.option("target_comp_id")),
            heartbeat_interval=int(settings.option("heartbeat_interval", 30)),
            default_appl_ver_id=settings.option("default_appl_ver_id"),
            username=settings.credentials.get("username"),
            password=settings.credentials.get("password"),
        )
        store = SqliteFixStore(Path(settings.option("store_path")), config.session_id)
        provider = cls(
            config,
            store,
            environment=Environment(settings.option("environment", "live")),
        )
        provider._owns_store = True
        return provider

    @property
    def session(self) -> FixSession:
        return self._session

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(name={self.name!r}, "
            f"session={self._session.config.session_id!r}, "
            f"state={self._session.state.value!r})"
        )

    # Lifecycle

    async def open(self) -> None:
        """Connect and log on. Raises if the session cannot become ready in time."""
        self._failure = None
        await self._session.start()
        await self._session.wait_ready(self._logon_timeout)

    async def close(self) -> None:
        """Log out and stop. Open orders stay working at the venue. Idempotent."""
        await self._session.stop()
        for queue in self._subscribers:
            queue.put_nowait(TransportError("FIX provider closed"))
        if self._owns_store:
            self._store.close()

    # SessionListener

    def on_session_ready(self) -> None:
        logger.info("FIX provider %s is ready", self.name)

    def on_session_down(self, reason: str) -> None:
        for pending in list(self._pending.values()):
            if pending.future.done():
                continue
            if pending.kind is _Kind.STATUS:
                error: Exception = TransportError(
                    f"FIX session lost during a status query: {reason}"
                )
            else:
                error = AmbiguousOutcome(
                    f"FIX session lost before the {pending.kind.value} of "
                    f"{pending.client_order_id} was answered: {reason}"
                )
            pending.future.set_exception(error)

    def on_session_failed(self, error: Exception) -> None:
        self._failure = error
        self.on_session_down(str(error))
        for queue in self._subscribers:
            queue.put_nowait(error)

    def on_sequence_reset(self) -> None:
        self._sent.clear()
        self._pending_by_seq.clear()

    async def on_discarded_message(self, message: FixMessage, reason: str) -> None:
        await self._quarantine(message, reason)

    async def on_session_reject(self, message: FixMessage) -> None:
        tags = self.dialect.tags
        seq_text = message.get(tags.ref_seq_num)
        code = f"SESSION_{message.get(tags.session_reject_reason, 'REJECT')}"
        text = self._peer_text(message) or "session-level reject"
        seq = _reference(seq_text)
        sent = None if seq is None else self._sent.get(seq)
        ref_type = message.get(tags.ref_msg_type)
        if (
            sent is not None
            and ref_type is not None
            and ref_type != _TYPE_OF[sent.kind]
        ):
            sent = None
        if sent is None:
            # An admin message we sent, or an application message from before a restart.
            logger.warning("FIX session Reject for message %s: %s", seq_text, text)
            return
        pending = None if seq is None else self._pending_by_seq.get(seq)
        self._reject_sent(pending, sent, f"session reject: {text}", code)

    async def on_application_message(self, message: FixMessage) -> None:
        msg_type = message.msg_type
        try:
            if msg_type == "8":
                await self._on_execution_report(message)
            elif msg_type == "9":
                await self._on_cancel_reject(message)
            elif msg_type == "j":
                await self._on_business_reject(message)
            else:
                await self._on_other_message(message)
        except ApplicationRejectError as exc:
            await self._quarantine(message, exc.text)
            raise

    async def _quarantine(self, message: FixMessage, reason: str) -> None:
        """Record a message that will never be redelivered, and stop if it mattered."""
        self.dropped_reports += 1
        text = self._session.scrub(reason)
        logger.critical("FIX message type %s quarantined: %s", message.msg_type, text)
        record = QuarantinedMessage(
            parse_seq_num(message.get(34) or "0"),
            message.msg_type,
            self._session.scrub(repr(message)),
            text,
            self.clock.now().isoformat(),
        )
        await self._session.run_store(lambda: self._store.quarantine(record))
        if message.msg_type != "8" or self._is_status(message):
            return
        tags = self.dialect.tags
        wire_id = message.get(tags.client_order_id)
        root = await self._resolve(wire_id, message.get(tags.orig_client_order_id))
        target = root or wire_id
        if target is not None:
            # A pending report that may hold a fill: only a query can settle it.
            with contextlib.suppress(pydantic.ValidationError):
                self._publish(
                    Restated(
                        client_order_id=target,
                        ts=self.clock.now(),
                        origin=EventOrigin.VENUE,
                        status=OrderStatus.UNKNOWN,
                        reason=f"unreadable execution report: {text}",
                    )
                )
        self._halt(ReportQuarantinedError(f"unreadable execution report: {text}"))

    def _halt(self, error: ReportQuarantinedError) -> None:
        if self._subscribers:
            for queue in self._subscribers:
                queue.put_nowait(error)
        else:
            self._backlog.append(error)

    async def quarantined(self) -> list[QuarantinedMessage]:
        """Every message that consumed a sequence number without being applied."""
        return await self._store_read(self._store.quarantined)

    def release_quarantine(self) -> None:
        """An operator has dealt with the quarantine, so the stream may run again."""
        kept = [e for e in self._backlog if not isinstance(e, ReportQuarantinedError)]
        self._backlog = deque(kept)

    async def _on_other_message(self, message: FixMessage) -> None:
        msg_type = message.msg_type
        if msg_type in self.dialect.ignored_msg_types:
            logger.info("FIX ignored application message type %s", msg_type)
        else:
            await self._reject_unsupported(message)

    async def _reject_unsupported(self, message: FixMessage) -> None:
        tags = self.dialect.tags
        logger.warning("FIX message type %s is not supported", message.msg_type)
        with contextlib.suppress(SessionDownError, AmbiguousOutcome, FixStoreError):
            await self._session.send_application(
                "j",
                [
                    (tags.ref_seq_num, message.get(34) or "0"),
                    (tags.ref_msg_type, message.msg_type),
                    (tags.business_reject_reason, "3"),
                    (tags.text, "Unsupported Message Type"),
                ],
            )

    # Publishing

    def _publish(self, event: OrderEvent) -> None:
        if self._subscribers:
            for queue in self._subscribers:
                queue.put_nowait(event)
            return
        if len(self._backlog) >= _BACKLOG_LIMIT:
            oldest = next(
                (e for e in self._backlog if not isinstance(e, ReportQuarantinedError)),
                None,
            )
            if oldest is not None:
                self._backlog.remove(oldest)
                logger.critical(
                    "FIX event backlog is full with no subscriber; dropped %s for %s. "
                    "Reconciliation recovers it from the store.",
                    oldest.kind,
                    oldest.client_order_id,
                )
        self._backlog.append(event)

    # Store helpers

    async def _load(self, client_order_id: str) -> _Record | None:
        payload = await self._session.run_store(
            lambda: self._store.get_order(client_order_id)
        )
        return None if payload is None else _Record.model_validate_json(payload)

    async def _save(self, record: _Record) -> None:
        payload = record.model_dump_json()
        await self._session.run_store(
            lambda: self._store.put_order(record.client_order_id, payload)
        )

    async def _resolve(self, *wire_ids: str | None) -> str | None:
        for wire_id in wire_ids:
            if wire_id is None:
                continue
            root = await self._session.run_store(
                partial(self._store.resolve_wire_id, wire_id)
            )
            if root is not None:
                return root
        return None

    # Inbound

    def _decimal(self, message: FixMessage, tag: int) -> Decimal | None:
        raw = message.get(tag)
        if raw is None:
            return None
        try:
            return parse_decimal(raw, tag)
        except FieldFormatError as exc:
            raise ApplicationRejectError(
                _REJECT_BAD_FORMAT, f"tag {tag} is not a decimal", tag
            ) from exc

    def _transact_time(self, message: FixMessage) -> datetime:
        raw = message.get(self.dialect.tags.transact_time)
        if raw is None:
            return self.clock.now()
        try:
            return parse_utc_timestamp(raw, self.dialect.tags.transact_time)
        except FieldFormatError as exc:
            raise ApplicationRejectError(
                _REJECT_BAD_FORMAT,
                "TransactTime is not a timestamp",
                self.dialect.tags.transact_time,
            ) from exc

    def _peer_text(self, message: FixMessage) -> str:
        """Text(58), which can carry a credential if the venue echoes one."""
        return self._session.scrub(message.get(self.dialect.tags.text) or "")

    def _require(self, message: FixMessage, tag: int) -> str:
        value = message.get(tag)
        if value is None:
            raise ApplicationRejectError(
                _REJECT_REQUIRED_TAG, f"required tag {tag} is missing", tag
            )
        return value

    def _is_status(self, message: FixMessage) -> bool:
        tags = self.dialect.tags
        return (
            message.get(tags.exec_type) == "I"
            or message.get(tags.exec_trans_type) == "3"
        )

    def _digest(self, message: FixMessage) -> str:
        material = "\x01".join(
            f"{tag}={value}" for tag, value in message if tag not in _HEADER_TAGS
        )
        return hashlib.sha256(material.encode("latin-1", "replace")).hexdigest()

    async def _on_execution_report(self, message: FixMessage) -> None:
        tags = self.dialect.tags
        wire_id = self._require(message, tags.client_order_id)
        root = await self._resolve(wire_id, message.get(tags.orig_client_order_id))
        if self._is_status(message):
            await self._on_status_report(message, root or wire_id, wire_id)
            return
        client_order_id = root or wire_id
        exec_id = self._require(message, tags.exec_id)
        self._require(message, tags.exec_type)
        async with self._records_lock:
            await self._apply_report(message, client_order_id, wire_id, exec_id)

    async def _apply_report(
        self, message: FixMessage, client_order_id: str, wire_id: str, exec_id: str
    ) -> None:
        digest = self._digest(message)
        known = await self._session.run_store(
            lambda: self._store.execution(client_order_id, exec_id)
        )
        conflicting = known is not None and known.digest != digest
        if known is not None and not conflicting:
            logger.info("FIX dropped a repeat of ExecID %s", exec_id)
            return
        record = await self._load(client_order_id)
        if record is None:
            logger.error(
                "FIX report for %s, which this session never sent", client_order_id
            )
        events = self._events_for(message, client_order_id, exec_id, record)
        if conflicting:
            logger.error(
                "FIX ExecID %s was reported again with different content", exec_id
            )
            for event in events:
                if isinstance(event, Filled):
                    self._publish(event)
            return
        fill = next((e.fill for e in events if isinstance(e, Filled)), None)
        bust = next((e.fill_id for e in events if isinstance(e, FillBusted)), None)
        updated = self._updated_record(record, message, wire_id, fill, bust)
        execution = StoredExecution(
            client_order_id,
            exec_id,
            digest,
            fill.model_dump_json() if fill is not None else None,
            False,
            self.clock.now().isoformat(),
        )
        payload = updated.model_dump_json() if updated is not None else None
        commit = asyncio.ensure_future(
            self._session.run_store(
                lambda: self._store.record_report(
                    client_order_id,
                    order_payload=payload,
                    execution=execution,
                    bust_exec_id=bust,
                )
            )
        )
        cancelled: asyncio.CancelledError | None = None
        try:
            await asyncio.shield(commit)
        except asyncio.CancelledError as exc:
            # A commit that lands marks this ExecID as seen, so its redelivery would be
            # dropped as a repeat. The events go out whether or not we are stopping.
            cancelled = exc
            await asyncio.wait({commit})
            if commit.exception() is not None:
                raise
        for event in events:
            self._publish(event)
        self._answer_waiters(message, wire_id)
        if cancelled is not None:
            raise cancelled

    def _updated_record(
        self,
        record: _Record | None,
        message: FixMessage,
        wire_id: str,
        fill: Fill | None,
        bust: str | None,
    ) -> _Record | None:
        if record is None:
            return None
        tags = self.dialect.tags
        update: dict[str, object] = {"updated_at": self.clock.now()}
        if self.dialect.is_duplicate_reject(message):
            return record.model_copy(update=update)
        exec_type = message.get(tags.exec_type)
        status_code = message.get(tags.ord_status)
        status = self._status_from(status_code or "", record)
        if status is not None:
            update["status"] = status
        cum_qty = self._decimal(message, tags.cum_qty)
        if cum_qty is not None:
            update["filled"] = cum_qty
        elif fill is not None and bust is None:
            update["filled"] = record.filled + fill.quantity
        venue_order_id = _venue_order_id(message.get(tags.order_id))
        if venue_order_id is not None:
            update["venue_order_id"] = venue_order_id
        if exec_type != "A" and exec_type != "8":
            update["acknowledged"] = True
        if exec_type == "5":
            update["wire_id"] = wire_id
            quantity = self._decimal(message, tags.order_qty)
            if quantity is not None:
                update["quantity"] = quantity
            price = self._decimal(message, tags.price)
            if price is not None:
                update["limit_price"] = price
        return record.model_copy(update=update)

    def _events_for(
        self,
        message: FixMessage,
        client_order_id: str,
        exec_id: str,
        record: _Record | None,
    ) -> list[OrderEvent]:
        tags = self.dialect.tags
        exec_type = message.get(tags.exec_type)
        trans = message.get(tags.exec_trans_type, "0")
        text = self._peer_text(message)
        ts = self._transact_time(message)
        common: dict[str, Any] = {
            "client_order_id": client_order_id,
            "ts": ts,
            "origin": EventOrigin.VENUE,
        }
        try:
            if exec_type == "H" or trans == "1":
                return [self._bust_event(message, client_order_id, ts)]
            if exec_type == "G" or trans == "2":
                events: list[OrderEvent] = [
                    self._bust_event(message, client_order_id, ts)
                ]
                fill = self._fill(message, client_order_id, exec_id, record, ts)
                if fill is not None:
                    events.append(Filled(**common, fill=fill))
                return events
            if self.dialect.is_fill_report(message):
                fill = self._fill(message, client_order_id, exec_id, record, ts)
                if fill is None:
                    return []
                return [Filled(**common, fill=fill)]
            venue_order_id = _venue_order_id(message.get(tags.order_id))
            if exec_type == "0":
                return [
                    Acknowledged(
                        **common,
                        venue_order_id=self._require(message, tags.order_id),
                    )
                ]
            if exec_type == "4":
                return [Canceled(**common, reason=text)]
            if exec_type == "3" and not self._done_for_day_ends(record):
                return []
            if exec_type in ("C", "3"):
                return [Expired(**common)]
            if exec_type == "8":
                if self.dialect.is_duplicate_reject(message):
                    logger.warning(
                        "FIX venue already holds %s; leaving it to reconciliation",
                        client_order_id,
                    )
                    return []
                return [
                    Rejected(
                        **common,
                        reason=text or "rejected by the venue",
                        code=message.get(tags.ord_rej_reason),
                    )
                ]
            if exec_type == "5":
                return [
                    Replaced(
                        **common,
                        quantity=self._decimal(message, tags.order_qty),
                        limit_price=self._decimal(message, tags.price),
                        venue_order_id=venue_order_id,
                    )
                ]
            if exec_type == "D":
                status = self.dialect.ord_status.get(
                    self._require(message, tags.ord_status)
                )
                if status is None:
                    raise ApplicationRejectError(
                        _REJECT_BAD_VALUE, "unknown OrdStatus", tags.ord_status
                    )
                return [
                    Restated(
                        **common,
                        status=status,
                        venue_order_id=venue_order_id,
                        venue_filled_quantity=self._decimal(message, tags.cum_qty),
                        reason=text,
                    )
                ]
        except pydantic.ValidationError as exc:
            raise ApplicationRejectError(
                _REJECT_BAD_VALUE, "report fails validation"
            ) from exc
        if exec_type not in ("A", "6", "E", "7", "9", "B"):
            logger.warning("FIX ExecType %s has no order event", exec_type)
        return []

    @staticmethod
    def _done_for_day_ends(record: _Record | None) -> bool:
        """Done for day ends a day order. A multi-day order works again tomorrow."""
        return record is not None and record.time_in_force not in _MULTI_DAY

    def _status_from(self, code: str, record: _Record | None) -> OrderStatus | None:
        status = self.dialect.ord_status.get(code)
        if status is OrderStatus.EXPIRED and code == "3":
            if self._done_for_day_ends(record):
                return status
            filled = record is not None and record.filled > 0
            return OrderStatus.PARTIALLY_FILLED if filled else OrderStatus.ACCEPTED
        return status

    def _bust_event(
        self, message: FixMessage, client_order_id: str, ts: datetime
    ) -> FillBusted:
        tags = self.dialect.tags
        return FillBusted(
            client_order_id=client_order_id,
            ts=ts,
            origin=EventOrigin.VENUE,
            fill_id=self._require(message, tags.exec_ref_id),
            reason=self._peer_text(message) or "execution cancelled by the venue",
        )

    def _fill(
        self,
        message: FixMessage,
        client_order_id: str,
        exec_id: str,
        record: _Record | None,
        ts: datetime,
    ) -> Fill | None:
        tags = self.dialect.tags
        quantity = self._decimal(message, tags.last_qty)
        if quantity is None:
            raise ApplicationRejectError(
                _REJECT_REQUIRED_TAG, "LastQty missing on a fill", tags.last_qty
            )
        if quantity == 0:
            return None
        price = self._decimal(message, tags.last_px)
        if price is None:
            raise ApplicationRejectError(
                _REJECT_REQUIRED_TAG, "LastPx missing on a fill", tags.last_px
            )
        side = self.dialect.side_of(message.get(tags.side, "") or "")
        if side is None and record is not None:
            side = record.side
        if side is None:
            raise ApplicationRejectError(
                _REJECT_BAD_VALUE, "Side has no direction", tags.side
            )
        symbol = self._symbol_of(message, record)
        fee: Decimal | None = None
        fee_currency: str | None = None
        if message.get(tags.commission_type) == "3":  # absolute, not per unit or bps
            fee = self._decimal(message, tags.commission)
            if fee is not None:
                fee_currency = message.get(tags.currency)
        return Fill(
            fill_id=exec_id,
            client_order_id=client_order_id,
            symbol=symbol,
            side=side,
            price=price,
            quantity=quantity,
            ts=ts,
            venue_order_id=_venue_order_id(message.get(tags.order_id)),
            fee=fee,
            fee_currency=fee_currency,
            liquidity={"1": Liquidity.MAKER, "2": Liquidity.TAKER}.get(
                message.get(tags.last_liquidity_ind) or "", Liquidity.UNKNOWN
            ),
        )

    def _symbol_of(self, message: FixMessage, record: _Record | None) -> Symbol:
        if record is not None:
            return record.symbol
        tags = self.dialect.tags
        venue = self.dialect.default_venue
        if venue is None:
            raise ApplicationRejectError(
                _REJECT_BAD_VALUE,
                "fill for an unknown order and no default venue",
                tags.client_order_id,
            )
        return Symbol(venue=venue, ticker=self._require(message, tags.symbol))

    def _answer_waiters(self, message: FixMessage, wire_id: str) -> None:
        tags = self.dialect.tags
        pending = self._pending.get(wire_id)
        if pending is None or pending.future.done() or pending.kind is _Kind.STATUS:
            return
        exec_type = message.get(tags.exec_type)
        if exec_type == "8":
            reason = self._peer_text(message) or "rejected by the venue"
            code = message.get(tags.ord_rej_reason)
            duplicate = pending.kind is _Kind.NEW and self.dialect.is_duplicate_reject(
                message
            )
            pending.future.set_exception(
                DuplicateOrderId(reason, code=code)
                if duplicate
                else VenueError(reason, code=code)
            )
            return
        if pending.kind is _Kind.NEW:
            if exec_type == "A":
                return  # Pending New is not an acceptance.
            pending.future.set_result(
                OrderAck(
                    client_order_id=pending.client_order_id,
                    venue_order_id=message.get(tags.order_id) or "NONE",
                    ts=self._transact_time(message),
                )
            )
            return
        pending.future.set_result(None)

    async def _on_status_report(
        self, message: FixMessage, client_order_id: str, wire_id: str
    ) -> None:
        tags = self.dialect.tags
        request_id = message.get(tags.ord_status_req_id)
        pending = self._pending.get(request_id or "")
        if pending is None:
            pending = next(
                (
                    p
                    for p in self._pending.values()
                    if p.kind is _Kind.STATUS
                    and not p.future.done()
                    and (p.wire_id == wire_id or p.client_order_id == client_order_id)
                ),
                None,
            )
        if self.dialect.is_unknown_order(message):
            if pending is not None and not pending.future.done():
                pending.future.set_result(None)
            return
        async with self._records_lock:
            await self._apply_status(message, client_order_id, pending)

    async def _apply_status(
        self, message: FixMessage, client_order_id: str, pending: _Pending | None
    ) -> None:
        record = await self._load(client_order_id)
        try:
            snapshot = self._snapshot(message, client_order_id, record)
        except (ApplicationRejectError, pydantic.ValidationError) as exc:
            detail = (
                exc.text
                if isinstance(exc, ApplicationRejectError)
                else f"{exc.error_count()} validation errors"
            )
            if pending is not None and not pending.future.done():
                pending.future.set_exception(
                    TransportError(f"FIX status report is malformed: {detail}")
                )
            raise ApplicationRejectError(
                _REJECT_BAD_VALUE, "status report is malformed"
            ) from exc
        if record is not None:
            updated = record.model_copy(
                update={
                    "status": snapshot.status,
                    "filled": snapshot.filled_quantity,
                    "venue_order_id": snapshot.venue_order_id or record.venue_order_id,
                    "acknowledged": record.acknowledged
                    or snapshot.status is not OrderStatus.SUBMITTING,
                    "updated_at": self.clock.now(),
                }
            )
            await self._save(updated)
        if pending is not None and not pending.future.done():
            pending.future.set_result(snapshot)

    def _snapshot(
        self, message: FixMessage, client_order_id: str, record: _Record | None
    ) -> VenueOrderSnapshot:
        tags = self.dialect.tags
        code = self._require(message, tags.ord_status)
        status = self._status_from(code, record)
        if status is None:
            raise ApplicationRejectError(
                _REJECT_BAD_VALUE, "unknown OrdStatus", tags.ord_status
            )
        quantity = self._decimal(message, tags.order_qty)
        if quantity is None or quantity <= 0:
            if record is None:
                raise ApplicationRejectError(
                    _REJECT_REQUIRED_TAG, "OrderQty missing", tags.order_qty
                )
            quantity = record.quantity
        filled = self._decimal(message, tags.cum_qty)
        if filled is None:
            raise ApplicationRejectError(
                _REJECT_REQUIRED_TAG, "CumQty missing", tags.cum_qty
            )
        side = self.dialect.side_of(message.get(tags.side) or "")
        if side is None:
            if record is None:
                raise ApplicationRejectError(
                    _REJECT_BAD_VALUE, "Side has no direction", tags.side
                )
            side = record.side
        price = self._decimal(message, tags.price)
        return VenueOrderSnapshot(
            client_order_id=client_order_id,
            venue_order_id=_venue_order_id(message.get(tags.order_id)),
            symbol=self._symbol_of(message, record),
            side=side,
            quantity=quantity,
            filled_quantity=filled,
            status=status,
            limit_price=price
            if price is not None
            else (record.limit_price if record is not None else None),
            ts=self._transact_time(message),
        )

    async def _on_cancel_reject(self, message: FixMessage) -> None:
        tags = self.dialect.tags
        wire_id = self._require(message, tags.client_order_id)
        reason = self._peer_text(message) or "cancel or replace rejected by the venue"
        code = message.get(tags.cxl_rej_reason)
        pending = self._pending.get(wire_id)
        if pending is not None and not pending.future.done():
            pending.future.set_exception(VenueError(reason, code=code))
            return
        root = await self._resolve(wire_id, message.get(tags.orig_client_order_id))
        if root is None:
            logger.error("FIX OrderCancelReject for an unknown ClOrdID")
            self.dropped_reports += 1
            return
        replace = message.get(tags.cxl_rej_response_to) == "2"
        self._publish_reject(
            _Kind.REPLACE if replace else _Kind.CANCEL, root, reason, code
        )

    async def _on_business_reject(self, message: FixMessage) -> None:
        tags = self.dialect.tags
        ref_id = message.get(tags.business_reject_ref_id)
        seq_text = message.get(tags.ref_seq_num)
        reason_code = message.get(tags.business_reject_reason)
        text = self._peer_text(message) or "business message reject"
        seq = _reference(seq_text)
        pending = self._find_pending(ref_id, seq)
        code = f"BUSINESS_{reason_code}"
        if pending is not None and pending.kind is _Kind.STATUS:
            if not pending.future.done():
                if reason_code in self.dialect.unknown_id_business_reasons:
                    pending.future.set_result(None)
                else:
                    pending.future.set_exception(VenueError(text, code=code))
            return
        sent = None if seq is None else self._sent.get(seq)
        if pending is not None:
            sent = _Sent(pending.kind, pending.client_order_id, pending.wire_id)
        elif sent is None:
            root = await self._resolve(ref_id)
            kind = {"D": _Kind.NEW, "F": _Kind.CANCEL, "G": _Kind.REPLACE}.get(
                message.get(tags.ref_msg_type) or ""
            )
            if root is None or kind is None:
                logger.error("FIX BusinessMessageReject matches no request: %s", text)
                self.dropped_reports += 1
                return
            sent = _Sent(kind, root, ref_id or root)
        self._reject_sent(pending, sent, text, code)

    def _find_pending(self, ref_id: str | None, seq: int | None) -> _Pending | None:
        if seq is not None and seq in self._pending_by_seq:
            return self._pending_by_seq[seq]
        if ref_id is None:
            return None
        found = self._pending.get(ref_id)
        if found is not None:
            return found
        return next((p for p in self._pending.values() if p.wire_id == ref_id), None)

    def _reject_sent(
        self, pending: _Pending | None, sent: _Sent, reason: str, code: str | None
    ) -> None:
        """Fail the waiter if one remains. Otherwise tell the stream.

        A rejected new order always reaches the stream too, because the order manager
        may have stopped waiting and marked it unknown, and the reject resolves that.
        """
        waiting = pending is not None and not pending.future.done()
        if pending is not None and waiting:
            pending.future.set_exception(VenueError(reason, code=code))
        if sent.kind is _Kind.STATUS:
            return
        if sent.kind is _Kind.NEW or not waiting:
            self._publish_reject(sent.kind, sent.client_order_id, reason, code)

    def _publish_reject(
        self, kind: _Kind, client_order_id: str, reason: str, code: str | None
    ) -> None:
        now = self.clock.now()
        event: OrderEvent
        if kind is _Kind.NEW:
            event = Rejected(
                client_order_id=client_order_id,
                ts=now,
                origin=EventOrigin.VENUE,
                reason=reason,
                code=code,
            )
        elif kind is _Kind.REPLACE:
            event = ReplaceRejected(
                client_order_id=client_order_id,
                ts=now,
                origin=EventOrigin.VENUE,
                reason=reason,
                code=code,
            )
        else:
            event = CancelRejected(
                client_order_id=client_order_id,
                ts=now,
                origin=EventOrigin.VENUE,
                reason=reason,
                code=code,
            )
        self._publish(event)

    # Outbound

    def _wants_status_request_id(self) -> bool:
        wanted = self.dialect.status_request_ids
        return self._begin_string != "FIX.4.2" if wanted is None else wanted

    def _transact_field(self) -> tuple[int, FieldValue]:
        return (
            self.dialect.tags.transact_time,
            format_utc_timestamp(self.clock.now()),
        )

    def _side_code(self, record: _Record) -> str:
        if record.short_sale is not ShortSaleMark.NOT_SHORT:
            return self.dialect.short_side_codes[record.short_sale]
        return self.dialect.side_codes[record.side]

    def _order_terms(
        self, record: _Record, quantity: Decimal, limit_price: Decimal | None
    ) -> Fields:
        tags = self.dialect.tags
        fields: Fields = []
        if record.account is not None:
            fields.append((tags.account, record.account))
        if self.dialect.handl_inst is not None:
            fields.append((tags.handl_inst, self.dialect.handl_inst))
        if record.post_only and self.dialect.post_only_exec_inst is not None:
            fields.append((tags.exec_inst, self.dialect.post_only_exec_inst))
        fields.extend(self.dialect.instrument_fields(record.symbol))
        fields.append((tags.side, self._side_code(record)))
        fields.append(self._transact_field())
        fields.append((tags.order_qty, quantity))
        fields.append((tags.ord_type, self.dialect.ord_type_codes[record.order_type]))
        if limit_price is not None:
            fields.append((tags.price, limit_price))
        if record.stop_price is not None:
            fields.append((tags.stop_px, record.stop_price))
        fields.append(
            (tags.time_in_force, self.dialect.tif_codes[record.time_in_force])
        )
        if record.expire_at is not None:
            fields.append((tags.expire_time, format_utc_timestamp(record.expire_at)))
        return fields

    def _check_supported(self, request: OrderRequest) -> None:
        dialect = self.dialect
        problem: str | None = None
        if request.order_type not in dialect.ord_type_codes:
            problem = f"order type {request.order_type.value}"
        elif request.time_in_force not in dialect.tif_codes:
            problem = f"time in force {request.time_in_force.value}"
        elif request.reduce_only:
            problem = "reduce_only, which FIX has no standard field for"
        elif request.post_only and dialect.post_only_exec_inst is None:
            problem = "post_only"
        elif (
            request.short_sale is not ShortSaleMark.NOT_SHORT
            and request.short_sale not in dialect.short_side_codes
        ):
            problem = f"short sale mark {request.short_sale.value}"
        if problem is not None:
            raise VenueError(
                f"not sent: {problem} is not supported", code="UNSUPPORTED"
            )

    def _require_session(self) -> None:
        if self._failure is not None:
            raise SessionFailedError(f"FIX session failed: {self._failure}")
        if not self._session.is_ready:
            raise SessionDownError(f"FIX session is {self._session.state.value}")

    def _new_pending(
        self, kind: _Kind, client_order_id: str, wire_id: str, key: str
    ) -> _Pending:
        pending = _Pending(
            kind,
            client_order_id,
            wire_id,
            asyncio.get_running_loop().create_future(),
            key,
        )
        self._pending[key] = pending
        return pending

    def _drop_pending(self, pending: _Pending) -> None:
        self._pending.pop(pending.key, None)
        if pending.seq is not None:
            self._pending_by_seq.pop(pending.seq, None)
        if pending.future.done() and not pending.future.cancelled():
            pending.future.exception()
        else:
            pending.future.cancel()

    def _note_sent(self, pending: _Pending, seq: int) -> None:
        pending.seq = seq
        self._pending_by_seq[seq] = pending
        self._sent[seq] = _Sent(pending.kind, pending.client_order_id, pending.wire_id)
        while len(self._sent) > _SENT_INDEX_LIMIT:
            self._sent.popitem(last=False)

    async def _send_and_wait(
        self, pending: _Pending, msg_type: str, fields: Fields
    ) -> VenueOrderSnapshot | OrderAck | None:
        await self._session.send_application(
            msg_type, fields, on_allocated=lambda seq: self._note_sent(pending, seq)
        )
        return await pending.future

    # TradingCapability

    def trading_rules(self) -> TradingRules:
        return self.dialect.trading_rules()

    async def place_order(self, request: OrderRequest) -> OrderAck:
        """Send one NewOrderSingle and wait for acceptance. See the module docstring."""
        cid = request.client_order_id
        self._check_supported(request)
        self._require_session()
        if await self._load_or_transport_error(cid) is not None:
            raise DuplicateOrderId(
                f"client order id {cid} was already sent on this session; not resent",
                code="DUPLICATE_CLIENT_ORDER_ID",
            )
        record = _Record(
            client_order_id=cid,
            symbol=request.symbol,
            side=request.side,
            short_sale=request.short_sale,
            quantity=request.quantity,
            order_type=request.order_type,
            time_in_force=request.time_in_force,
            limit_price=request.limit_price,
            stop_price=request.stop_price,
            expire_at=request.expire_at,
            account=request.account,
            post_only=request.post_only,
            wire_id=cid,
            updated_at=self.clock.now(),
        )
        fields: Fields = [
            (self.dialect.tags.client_order_id, cid),
            *self._order_terms(record, request.quantity, request.limit_price),
        ]
        payload = record.model_dump_json()

        def remember() -> None:
            self._store.put_order(cid, payload)
            self._store.link_wire_id(cid, cid)

        try:
            await self._session.run_store(remember)
        except FixStoreError as exc:
            raise TransportError(
                f"FIX store failed, order not sent: {exc}", retryable=False
            ) from exc
        pending = self._new_pending(_Kind.NEW, cid, cid, cid)
        try:
            result = await asyncio.wait_for(
                self._send_and_wait(pending, "D", fields), self._ack_timeout
            )
        except (SessionDownError, SessionFailedError, FixStoreError) as exc:
            # Nothing reached the store's outbound log, so nothing can ever be resent.
            with contextlib.suppress(FixStoreError):
                await self._session.run_store(lambda: self._store.delete_order(cid))
            if isinstance(exc, FixStoreError):
                raise TransportError(
                    f"FIX store failed, order not sent: {exc}", retryable=False
                ) from exc
            raise
        except asyncio.TimeoutError:
            raise AmbiguousOutcome(
                f"no acknowledgement for {cid} within {self._ack_timeout}s"
            ) from None
        finally:
            self._drop_pending(pending)
        assert isinstance(result, OrderAck)
        return result

    async def _load_or_transport_error(self, client_order_id: str) -> _Record | None:
        try:
            return await self._load(client_order_id)
        except FixStoreError as exc:
            raise TransportError(f"FIX store failed: {exc}", retryable=False) from exc

    async def _next_wire_id(self, client_order_id: str) -> tuple[_Record, str]:
        """Take the order's next ClOrdID, atomically with respect to its reports."""
        async with self._records_lock:
            record = await self._load_or_transport_error(client_order_id)
            if record is None:
                raise _no_record(client_order_id)
            sequence = record.chain_length + 1
            wire_id = self.dialect.wire_id(client_order_id, sequence)
            updated = record.model_copy(update={"chain_length": sequence})
            payload = updated.model_dump_json()

            def remember() -> None:
                self._store.put_order(client_order_id, payload)
                self._store.link_wire_id(wire_id, client_order_id)

            try:
                await self._session.run_store(remember)
            except FixStoreError as exc:
                raise TransportError(
                    f"FIX store failed, change not sent: {exc}", retryable=False
                ) from exc
        return updated, wire_id

    async def _request_change(
        self, kind: _Kind, msg_type: str, wire_id: str, root: str, fields: Fields
    ) -> None:
        pending = self._new_pending(kind, root, wire_id, wire_id)
        try:
            await asyncio.wait_for(
                self._send_and_wait(pending, msg_type, fields), self._ack_timeout
            )
        except asyncio.TimeoutError:
            raise AmbiguousOutcome(
                f"no answer to the {kind.value} of {root} within {self._ack_timeout}s"
            ) from None
        except FixStoreError as exc:
            raise TransportError(
                f"FIX store failed, {kind.value} not sent: {exc}", retryable=False
            ) from exc
        finally:
            self._drop_pending(pending)

    async def cancel_order(self, client_order_id: str, symbol: Symbol) -> None:
        """Send OrderCancelRequest(F) and wait for the venue's first answer."""
        self._require_session()
        tags = self.dialect.tags
        record, wire_id = await self._next_wire_id(client_order_id)
        fields: Fields = [
            (tags.orig_client_order_id, record.wire_id),
            (tags.client_order_id, wire_id),
        ]
        if record.venue_order_id is not None:
            fields.append((tags.order_id, record.venue_order_id))
        if record.account is not None:
            fields.append((tags.account, record.account))
        fields.extend(self.dialect.instrument_fields(symbol))
        fields.append((tags.side, self._side_code(record)))
        fields.append(self._transact_field())
        fields.append((tags.order_qty, record.quantity))
        await self._request_change(_Kind.CANCEL, "F", wire_id, client_order_id, fields)

    async def amend_order(
        self,
        client_order_id: str,
        symbol: Symbol,  # noqa: ARG002 - the stored order names the instrument
        *,
        quantity: Decimal | None = None,
        limit_price: Decimal | None = None,
    ) -> None:
        """Send OrderCancelReplaceRequest(G) and wait for the first answer."""
        self._require_session()
        tags = self.dialect.tags
        record, wire_id = await self._next_wire_id(client_order_id)
        fields: Fields = [
            (tags.orig_client_order_id, record.wire_id),
            (tags.client_order_id, wire_id),
        ]
        if record.venue_order_id is not None:
            fields.append((tags.order_id, record.venue_order_id))
        fields.extend(
            self._order_terms(
                record,
                quantity if quantity is not None else record.quantity,
                limit_price if limit_price is not None else record.limit_price,
            )
        )
        await self._request_change(_Kind.REPLACE, "G", wire_id, client_order_id, fields)

    async def order_status(
        self, client_order_id: str, symbol: Symbol
    ) -> VenueOrderSnapshot | None:
        """Ask the venue about an order. See the module docstring for ``None``."""
        record = await self._load_or_transport_error(client_order_id)
        if record is None:
            raise _no_record(client_order_id)
        self._require_session()
        tags = self.dialect.tags
        self._request_counter += 1
        key = f"S{self._request_counter}-{client_order_id}"
        fields: Fields = [(tags.client_order_id, record.wire_id)]
        if record.account is not None:
            fields.append((tags.account, record.account))
        fields.extend(self.dialect.instrument_fields(symbol))
        fields.append((tags.side, self._side_code(record)))
        if self._wants_status_request_id():
            fields.append((tags.ord_status_req_id, key))
        pending = self._new_pending(_Kind.STATUS, client_order_id, record.wire_id, key)
        try:
            result = await asyncio.wait_for(
                self._send_and_wait(pending, "H", fields), self._query_timeout
            )
        except asyncio.TimeoutError:
            raise TransportError(
                f"no answer to the status query for {client_order_id} within "
                f"{self._query_timeout}s"
            ) from None
        except (AmbiguousOutcome, FixStoreError) as exc:
            raise TransportError(f"status query failed: {exc}") from exc
        finally:
            self._drop_pending(pending)
        assert result is None or isinstance(result, VenueOrderSnapshot)
        return result

    async def open_orders(
        self, symbol: Symbol | None = None
    ) -> list[VenueOrderSnapshot]:
        """Working orders from the local record. See the module docstring."""
        payloads = await self._store_read(self._store.all_orders)
        snapshots: list[VenueOrderSnapshot] = []
        for payload in payloads:
            record = _Record.model_validate_json(payload)
            if (
                not record.acknowledged
                or record.status.is_terminal
                or (symbol is not None and record.symbol != symbol)
            ):
                continue
            snapshots.append(
                VenueOrderSnapshot(
                    client_order_id=record.client_order_id,
                    venue_order_id=record.venue_order_id,
                    symbol=record.symbol,
                    side=record.side,
                    quantity=record.quantity,
                    filled_quantity=record.filled,
                    status=record.status,
                    limit_price=record.limit_price,
                    ts=record.updated_at,
                )
            )
        return snapshots

    async def fills_since(
        self, since: datetime, symbol: Symbol | None = None
    ) -> list[Fill]:
        """Unbusted fills from the local record.

        A fill counts if either the venue's time or our receipt time is at or after
        ``since``, so a venue clock behind ours cannot hide a recovered fill.
        """
        executions = await self._store_read(self._store.executions)
        fills: list[Fill] = []
        for execution in executions:
            if execution.fill is None or execution.busted:
                continue
            fill = Fill.model_validate_json(execution.fill)
            received = datetime.fromisoformat(execution.received_at)
            if (fill.ts >= since or received >= since) and (
                symbol is None or fill.symbol == symbol
            ):
                fills.append(fill)
        fills.sort(key=lambda f: f.ts)
        return fills

    async def _store_read(self, fn: Callable[[], T]) -> T:
        try:
            return await self._session.run_store(fn)
        except IsmetError as exc:
            raise TransportError(f"FIX store read failed: {exc}") from exc

    async def stream_executions(self) -> AsyncIterator[OrderEvent]:
        """Yield venue events. A disconnect the session can restore does not end it.

        Raises once the session has failed for good. Events that arrive with no
        subscriber wait in a backlog, which the first subscriber drains.
        """
        if self._failure is not None:
            raise self._failure
        queue: asyncio.Queue[OrderEvent | Exception] = asyncio.Queue()
        while self._backlog:
            queue.put_nowait(self._backlog.popleft())
        self._subscribers.append(queue)
        try:
            while True:
                item = await queue.get()
                if isinstance(item, ReportQuarantinedError):
                    # Stays queued, so every resubscribe stops here until released.
                    self._backlog.append(item)
                    raise item
                if isinstance(item, Exception):
                    raise item
                yield item
        finally:
            self._subscribers.remove(queue)
            while not queue.empty():
                leftover = queue.get_nowait()
                if isinstance(leftover, ReportQuarantinedError):
                    if not any(
                        isinstance(e, ReportQuarantinedError) for e in self._backlog
                    ):
                        self._backlog.append(leftover)
                elif not isinstance(leftover, Exception):
                    self._backlog.append(leftover)


def _reference(text: str | None) -> int | None:
    """A RefSeqNum from a peer, or ``None`` if it cannot be one."""
    if text is None:
        return None
    try:
        return parse_seq_num(text)
    except FieldFormatError:
        return None


def _no_record(client_order_id: str) -> TransportError:
    return TransportError(
        f"no local record of {client_order_id}, so FIX cannot name its Side. The store "
        "may be new or misconfigured; an operator must resolve this order.",
        retryable=False,
    )


def _venue_order_id(value: str | None) -> str | None:
    """OrderID, or ``None`` for the literal "NONE" FIX uses when there is none."""
    return None if value in (None, "NONE") else value


__all__ = ["FixDialect", "FixProvider", "FixTags", "ReportQuarantinedError"]
