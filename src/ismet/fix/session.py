"""An asyncio FIX initiator session for FIX 4.2, FIX 4.4, and FIXT.1.1/FIX 5.0 SP2.

Behaviour follows the FIXT 1.1 "FIX Transport" document, whose session rules match the
FIX 4.2 and 4.4 session chapters. Section names below are from that document, and
"case N" means its "FIX Session-level Test Cases and Expected Behaviors". Field
definitions are on fiximate.fixtrading.org.

- "Logon" and cases 1B: the initiator waits for the confirming Logon. A first message
  that is not a Logon gets a Logout and a disconnect.
- "Logon": after the Logon, send a TestRequest and wait for the answer before new
  messages, so both sides finish resend processing first. Until then the session is
  ``SYNCING`` and refuses application messages.
- "Heartbeats" and cases 4 and 6: send a Heartbeat after a quiet HeartBtInt. After
  HeartBtInt plus 20% with nothing received, send a TestRequest. After another such
  period the link is declared lost.
- "Message Recovery" and case 2: a number above the expected one triggers a
  ResendRequest through infinity (EndSeqNo 0), and later messages are ignored until the
  gap closes, the first option in "Ordered Message Processing". A number below the
  expected one without PossDupFlag means Logout and terminate, and the session does not
  reconnect because the spec calls for manual intervention. With PossDupFlag it is
  ignored.
- "Response by Message Type": a ResendRequest above the expected number is served
  first, and then the gap is requested, which avoids a deadlock when both sides have
  gaps. A Logout with a gap gets a ResendRequest, then the confirming Logout.
- "Message Recovery": resent application messages keep their number and carry
  PossDupFlag=Y and OrigSendingTime. Administrative messages become
  SequenceReset-GapFill, including Reject, which the spec permits a sender to gap-fill.
  A resent administrative message is checked for sequence only, except Reject.
- Cases 10 and 11: SequenceReset handling. In both modes a lowering NewSeqNo gets a
  Reject and the expected number does not move. Case 10e does not say whether to
  increment, so this follows 11c and does not, which is also what QuickFIX does.
- Cases 2f, 2g, 2i, 2k, 2o: OrigSendingTime, BeginString, CompID, and SendingTime
  checks. Cases 2d, 2m, 3b: garbled messages are ignored and consume no number.
- Cases 12 and 13: the Logout handshake, in both directions.
- Standard Message Header: PossResend is forwarded to the application, which checks
  its business identifiers. This session never sets PossResend itself.

An outbound application message is durable before it is written, and its number is
allocated in the same transaction, so it can be resent after a crash. The inbound number
advances only after the application has handled the message. A crash between the two
causes redelivery, which the application removes by ExecID.

Credentials are held as :class:`pydantic.SecretStr` and are unwrapped only when a Logon
is framed. Frames are never logged, Logon is never stored, and text from the peer is
scrubbed of the configured secrets before it is logged or raised.

Not implemented: NextExpectedMsgSeqNum(789) resynchronization, FIXT per-message
ApplVerID, encryption (EncryptMethod is always 0; pass ``ssl`` for TLS), and scheduled
session times. A daily reset is done with ``reset_on_logon``, which resets once per
:attr:`FixSessionConfig.session_period` and not on a reconnect within it, or with
:meth:`FixStore.reset_sequences`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import ssl as ssl_module
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import NoReturn, Protocol, TypeVar

from pydantic import SecretStr

from ismet.errors import AmbiguousOutcome, AuthError, ConfigError, TransportError
from ismet.fix.message import (
    DEFAULT_MAX_BODY_LENGTH,
    SENSITIVE_TAGS,
    FieldFormatError,
    FixMessage,
    FixStreamParser,
    GarbledFrame,
    encode_fields,
    encode_frame,
    format_utc_timestamp,
    parse_seq_num,
    parse_utc_timestamp,
)
from ismet.fix.store import FixStore, FixStoreError, StoredMessage
from ismet.transport.backoff import ExponentialBackoff
from ismet.transport.clock import SYSTEM_CLOCK, Clock

logger = logging.getLogger("ismet.fix")

T = TypeVar("T")
FieldValue = str | int | bool | Decimal | SecretStr

BEGIN_STRINGS = frozenset({"FIX.4.2", "FIX.4.4", "FIXT.1.1"})

APPL_VER_FIX50SP2 = "9"
"""DefaultApplVerID(1137) for FIX 5.0 SP2."""

ADMIN_TYPES = frozenset({"0", "1", "2", "3", "4", "5", "A"})

_SYNC_RETRY_SECONDS = 1.0

REJECT_REQUIRED_TAG_MISSING = 1
REJECT_VALUE_INCORRECT = 5
REJECT_INCORRECT_FORMAT = 6
REJECT_COMP_ID = 9
REJECT_SENDING_TIME = 10


class SessionState(str, Enum):
    STOPPED = "stopped"
    CONNECTING = "connecting"
    LOGON_SENT = "logon_sent"
    SYNCING = "syncing"
    READY = "ready"
    LOGOUT_SENT = "logout_sent"
    DISCONNECTED = "disconnected"
    FAILED = "failed"


class SessionDownError(TransportError):
    """The session cannot take a message. Nothing was stored or written."""

    def __init__(self, message: str) -> None:
        super().__init__(message, retryable=True)


class SessionFailedError(TransportError):
    """The session stopped for good and will not reconnect by itself."""

    def __init__(self, message: str) -> None:
        super().__init__(message, retryable=False)


class SequenceTooLowError(SessionFailedError):
    """The peer's MsgSeqNum was too low without PossDupFlag. Needs an operator."""


class LogonRejectedError(AuthError):
    """The peer answered our Logon with a Logout. Retrying could lock the account."""


class ApplicationRejectError(Exception):
    """Raised by a listener to answer a malformed message with a session Reject."""

    def __init__(self, reason: int, text: str, tag: int | None = None) -> None:
        self.reason = reason
        self.text = text
        self.tag = tag
        super().__init__(text)


class SessionListener(Protocol):
    """What a session reports to its application."""

    async def on_application_message(self, message: FixMessage) -> None:
        """Handle one in-order message. Return only once its effects are durable.

        Raise :class:`ApplicationRejectError` to reject it. Any other exception leaves
        the inbound number unmoved and drops the connection, so it is redelivered.
        """
        ...

    async def on_session_reject(self, message: FixMessage) -> None:
        """A session-level Reject(3) arrived."""
        ...

    async def on_discarded_message(self, message: FixMessage, reason: str) -> None:
        """An application message is about to be rejected unread by the session.

        It consumes a sequence number, so it can never be redelivered, and it could
        have been a fill. Return only once that is durably recorded and surfaced.
        """
        ...

    async def on_possible_message_loss(
        self, first_seq: int, last_seq: int | None, reason: str
    ) -> None:
        """The peer moved its numbers past messages never read here, which it may not
        resend, and they could have been fills. ``last_seq`` is ``None`` if unknown.
        Return only once that is durably recorded and surfaced.
        """
        ...

    def on_sequence_reset(self) -> None:
        """Numbers restart at 1, so an older number no longer names the same message."""
        ...

    def on_session_ready(self) -> None: ...

    def on_session_down(self, reason: str) -> None: ...

    def on_session_failed(self, error: Exception) -> None: ...


def _utc_date(now: datetime) -> str:
    return now.date().isoformat()


@dataclass(frozen=True)
class FixSessionConfig:
    """One FIX session. ``repr`` cannot show ``password``, which is a ``SecretStr``.

    ``stale_resend_after`` gap-fills application messages older than this many seconds
    instead of resending them, which "Message Recovery" allows for aged orders.

    ``reset_on_logon`` sends ResetSeqNumFlag on the first connection of each
    ``session_period``, a label computed from the current UTC time, by default the UTC
    date. Its confirmation is recorded in the store, so a reconnect later in the period
    keeps the numbers and can recover what it missed by ResendRequest. Orders that were
    open across the reset are checked by the order manager's reconciliation, through
    CumQty.
    """

    host: str
    port: int
    begin_string: str
    sender_comp_id: str
    target_comp_id: str
    heartbeat_interval: int = 30
    default_appl_ver_id: str | None = None
    username: SecretStr | None = None
    password: SecretStr | None = None
    logon_fields: tuple[tuple[int, str | SecretStr], ...] = ()
    reset_on_logon: bool = False
    session_period: Callable[[datetime], str] = _utc_date
    ssl: ssl_module.SSLContext | None = None
    connect_timeout: float = 10.0
    logon_timeout: float = 10.0
    logout_timeout: float = 10.0
    termination_wait: float = 2.0
    write_timeout: float = 10.0
    max_sending_time_skew: float | None = 120.0
    stale_resend_after: float = 60.0
    max_body_length: int = DEFAULT_MAX_BODY_LENGTH
    resend_timeout: float | None = None
    max_resend_requests: int = 3
    reconnect_backoff: ExponentialBackoff = field(
        default_factory=lambda: ExponentialBackoff(base=1.0, maximum=30.0)
    )
    max_reconnect_attempts: int | None = 60

    def __post_init__(self) -> None:
        if self.begin_string not in BEGIN_STRINGS:
            raise ConfigError(
                f"unsupported BeginString {self.begin_string!r}; expected one of "
                f"{sorted(BEGIN_STRINGS)}"
            )
        if self.begin_string == "FIXT.1.1" and not self.default_appl_ver_id:
            raise ConfigError(
                "FIXT.1.1 requires default_appl_ver_id; use APPL_VER_FIX50SP2 for "
                "FIX 5.0 SP2"
            )
        if self.begin_string != "FIXT.1.1" and self.default_appl_ver_id:
            raise ConfigError("default_appl_ver_id applies only to FIXT.1.1")
        if self.heartbeat_interval <= 0:
            raise ConfigError("heartbeat_interval must be positive")
        for name, value in (("username", self.username), ("password", self.password)):
            if value is not None and not isinstance(value, SecretStr):
                raise ConfigError(f"{name} must be a SecretStr")
        for tag, field_value in self.logon_fields:
            if tag in SENSITIVE_TAGS and not isinstance(field_value, SecretStr):
                raise ConfigError(f"logon field {tag} must be a SecretStr")

    @property
    def session_id(self) -> str:
        return f"{self.begin_string}:{self.sender_comp_id}->{self.target_comp_id}"


class _DisconnectError(Exception):
    """End this connection; the runner decides whether to reconnect."""


class _TerminalError(Exception):
    """End the session for good."""

    def __init__(self, error: Exception) -> None:
        self.error = error
        super().__init__(str(error))


class FixSession:
    """One initiator session across any number of physical connections."""

    def __init__(
        self,
        config: FixSessionConfig,
        store: FixStore,
        listener: SessionListener | None = None,
        *,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        self.config = config
        self.store = store
        self.listener = listener
        self._clock = clock
        self._state = SessionState.STOPPED
        self._secrets = self._collect_secrets(config)
        self._executor: ThreadPoolExecutor | None = None
        self._runner: asyncio.Task[None] | None = None
        self._settled = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._writer: asyncio.StreamWriter | None = None
        self._failure: Exception | None = None
        self._stopping = False
        self._next_in = 1

        self._last_sent = 0.0
        self._last_received = 0.0
        self._test_request_at: float | None = None
        self._deadline: float | None = None
        self._test_counter = 0
        self._sync_ids: set[str] = set()
        self._sync_sent_at = 0.0
        self._synced = False
        self._resend_target: int | None = None
        self._resend_requested_at = 0.0
        self._resend_requests = 0
        self._reset_awaiting_confirmation: str | None = None

    @staticmethod
    def _collect_secrets(config: FixSessionConfig) -> tuple[str, ...]:
        found = [
            value.get_secret_value()
            for _, value in config.logon_fields
            if isinstance(value, SecretStr)
        ]
        for credential in (config.password, config.username):
            if credential is not None:
                found.append(credential.get_secret_value())
        return tuple(s for s in found if s)

    def scrub(self, text: str) -> str:
        """Remove configured credentials from text a peer may have echoed them in."""
        for secret in self._secrets:
            text = text.replace(secret, "**********")
        return text

    def __repr__(self) -> str:
        return f"FixSession({self.config.session_id}, state={self._state.value})"

    # State

    @property
    def state(self) -> SessionState:
        return self._state

    @property
    def is_ready(self) -> bool:
        return self._state is SessionState.READY

    @property
    def failure(self) -> Exception | None:
        return self._failure

    @property
    def next_inbound(self) -> int:
        return self._next_in

    def _set_state(self, state: SessionState) -> None:
        if state is not self._state:
            logger.info(
                "FIX session %s: %s -> %s",
                self.config.session_id,
                self._state.value,
                state.value,
            )
        self._state = state
        if state in (SessionState.READY, SessionState.FAILED, SessionState.STOPPED):
            self._settled.set()
        else:
            self._settled.clear()

    # Lifecycle

    async def start(self) -> None:
        """Begin connecting in the background. Idempotent."""
        if self._runner is not None:
            return
        self._failure = None
        self._stopping = False
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="ismet-fix-store"
        )
        self._next_in = await self.run_store(self.store.next_inbound)
        self._set_state(SessionState.DISCONNECTED)
        self._runner = asyncio.create_task(
            self._run(), name=f"ismet-fix-{self.config.session_id}"
        )

    async def wait_ready(self, timeout: float) -> None:
        """Wait until application messages can flow.

        Raises the session's failure if it stopped for good, and
        :class:`SessionDownError` if it is not ready in time.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            if self._failure is not None:
                raise self._failure
            if self.is_ready:
                return
            if self._state is SessionState.STOPPED:
                raise SessionDownError("FIX session is stopped")
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise SessionDownError(
                    f"FIX session not ready after {timeout}s "
                    f"(state {self._state.value})"
                )
            # The state can change again before a woken waiter runs, so recheck it.
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._settled.wait(), remaining)

    async def stop(self) -> None:
        """Log out if connected and stop. Sequence numbers remain. Idempotent."""
        runner, self._stopping = self._runner, True
        if runner is None:
            return
        if self._state in (SessionState.READY, SessionState.SYNCING):
            with contextlib.suppress(_DisconnectError, FixStoreError):
                await self._begin_logout("session stopping")
        logging_out = self._state is SessionState.LOGOUT_SENT
        await self._cancel_after(
            runner, self.config.logout_timeout if logging_out else 0
        )
        self._runner = None
        self._writer = None
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None
        self._set_state(SessionState.STOPPED)
        if self.listener is not None:
            self.listener.on_session_down("session stopped")

    @staticmethod
    async def _cancel_after(task: asyncio.Task[None], timeout: float) -> None:
        done, _ = await asyncio.wait({task}, timeout=timeout)
        if not done:
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task

    async def run_store(self, fn: Callable[[], T]) -> T:
        """Run a store call off the event loop, in submission order."""
        if self._executor is None:
            raise SessionDownError("FIX session is not started")
        return await asyncio.get_running_loop().run_in_executor(self._executor, fn)

    # Connection management

    def _should_run(self) -> bool:
        """Read again after each await: ``stop`` flips the flag from another task."""
        return not self._stopping

    async def _run(self) -> None:
        attempt = 0
        cfg = self.config
        while self._should_run():
            self._set_state(SessionState.CONNECTING)
            was_ready = False
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(cfg.host, cfg.port, ssl=cfg.ssl),
                    cfg.connect_timeout,
                )
            except (OSError, asyncio.TimeoutError) as exc:
                reason = f"connect failed: {type(exc).__name__}"
            else:
                try:
                    reason = await self._serve(reader, writer)
                except _TerminalError as exc:
                    self._fail(exc.error)
                    return
                finally:
                    was_ready = self._synced
                    await self._close_writer(writer)
            self._on_disconnected(reason)
            if not self._should_run():
                return
            attempt = 0 if was_ready else attempt + 1
            if (
                cfg.max_reconnect_attempts is not None
                and attempt > cfg.max_reconnect_attempts
            ):
                self._fail(
                    SessionFailedError(
                        f"gave up after {attempt - 1} reconnect attempts: {reason}"
                    )
                )
                return
            await asyncio.sleep(cfg.reconnect_backoff.delay(max(attempt - 1, 0)))

    def _on_disconnected(self, reason: str) -> None:
        logger.warning(
            "FIX session %s disconnected: %s", self.config.session_id, reason
        )
        self._writer = None
        self._set_state(SessionState.DISCONNECTED)
        if self.listener is not None:
            self.listener.on_session_down(reason)

    def _fail(self, error: Exception) -> None:
        logger.critical("FIX session %s failed: %s", self.config.session_id, error)
        self._writer = None
        self._failure = error
        self._set_state(SessionState.FAILED)
        if self.listener is not None:
            self.listener.on_session_failed(error)

    @staticmethod
    async def _close_writer(writer: asyncio.StreamWriter) -> None:
        writer.close()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(writer.wait_closed(), 2.0)

    async def _serve(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> str:
        """Run one connection. Returns why it ended; raises :class:`_TerminalError`."""
        self._writer = writer
        now = self._clock.monotonic()
        self._last_received = now
        self._last_sent = now
        self._test_request_at = None
        self._sync_ids = set()
        self._synced = False
        self._resend_target = None
        self._resend_requests = 0
        self._deadline = now + self.config.logon_timeout
        tasks: list[asyncio.Task[None]] = []
        try:
            await self._send_logon()
            tasks = [
                asyncio.create_task(self._read_loop(reader), name="ismet-fix-read"),
                asyncio.create_task(self._timer_loop(), name="ismet-fix-timer"),
            ]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
            return "connection ended"
        except _DisconnectError as exc:
            return str(exc)
        except (OSError, EOFError) as exc:
            return f"connection error: {type(exc).__name__}"
        except FixStoreError as exc:
            raise _TerminalError(
                SessionFailedError(f"FIX store failed: {exc}")
            ) from exc
        except _TerminalError:
            raise
        except Exception as exc:
            logger.exception("FIX session %s internal error", self.config.session_id)
            return f"internal error: {type(exc).__name__}"
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
        parser = FixStreamParser(max_body_length=self.config.max_body_length)
        while True:
            data = await reader.read(65536)
            if not data:
                raise _DisconnectError("peer closed the connection")
            for item in parser.feed(data):
                if isinstance(item, GarbledFrame):
                    logger.warning(
                        "FIX session %s ignored a garbled message: %s",
                        self.config.session_id,
                        item.reason,
                    )
                    continue
                await self._handle(item)

    async def _timer_loop(self) -> None:
        cfg = self.config
        interval = float(cfg.heartbeat_interval)
        silence_limit = interval * 1.2
        tick = min(0.1, interval / 10)
        resend_timeout = (
            cfg.resend_timeout
            if cfg.resend_timeout is not None
            else max(interval * 2, 5.0)
        )
        while True:
            await asyncio.sleep(tick)
            now = self._clock.monotonic()
            state = self._state
            if (
                state in (SessionState.LOGON_SENT, SessionState.SYNCING)
                and self._deadline is not None
                and now > self._deadline
            ):
                raise _DisconnectError(f"no answer during {state.value}")
            if state is SessionState.LOGOUT_SENT:
                if self._deadline is not None and now > self._deadline:
                    raise _DisconnectError("no Logout confirmation in time")
                continue
            if state not in (SessionState.SYNCING, SessionState.READY):
                continue
            if (
                state is SessionState.SYNCING
                and self._sync_ids
                and now - self._sync_sent_at > _SYNC_RETRY_SECONDS
            ):
                # A peer that drops messages above a gap, as "Ordered Message
                # Processing" allows, gap-fills our TestRequest and never answers it.
                await self._send_sync_request()
            if self._test_request_at is not None:
                if now - self._test_request_at > silence_limit:
                    with contextlib.suppress(_DisconnectError, FixStoreError):
                        await self._send_admin(
                            "5", [(58, "no response to TestRequest")]
                        )
                    raise _DisconnectError("missed heartbeats")
            elif now - self._last_received > silence_limit:
                self._test_counter += 1
                self._test_request_at = now
                await self._send_admin("1", [(112, f"TEST{self._test_counter}")])
            if now - self._last_sent >= interval:
                await self._send_admin("0", [])
            if (
                self._resend_target is not None
                and now - self._resend_requested_at > resend_timeout
            ):
                if self._resend_requests >= cfg.max_resend_requests:
                    raise _DisconnectError("message gap was not filled")
                await self._send_resend_request()

    # Outbound

    def _frame(
        self,
        seq: int,
        msg_type: str,
        body: bytes,
        *,
        poss_dup: bool = False,
        orig_sending_time: str | None = None,
        sending_time: str | None = None,
    ) -> bytes:
        header: list[tuple[int, FieldValue]] = [
            (35, msg_type),
            (49, self.config.sender_comp_id),
            (56, self.config.target_comp_id),
            (34, seq),
        ]
        if poss_dup:
            header.append((43, "Y"))
        header.append((52, sending_time or format_utc_timestamp(self._clock.now())))
        if orig_sending_time is not None:
            header.append((122, orig_sending_time))
        return encode_frame(self.config.begin_string, encode_fields(header) + body)

    async def _write(self, frame: bytes) -> None:
        writer = self._writer
        if writer is None or writer.is_closing():
            raise _DisconnectError("connection is closed")
        try:
            writer.write(frame)
            await asyncio.wait_for(writer.drain(), self.config.write_timeout)
        except (OSError, asyncio.TimeoutError) as exc:
            raise _DisconnectError(f"write failed: {type(exc).__name__}") from exc
        self._last_sent = self._clock.monotonic()

    async def _send_admin(
        self,
        msg_type: str,
        fields: Sequence[tuple[int, FieldValue]],
    ) -> int:
        async with self._send_lock:
            return await self._send_admin_locked(msg_type, fields)

    async def _send_admin_locked(
        self,
        msg_type: str,
        fields: Sequence[tuple[int, FieldValue]],
    ) -> int:
        seq = await self.run_store(self.store.allocate_outbound)
        await self._write(self._frame(seq, msg_type, encode_fields(fields)))
        return seq

    async def send_application(
        self,
        msg_type: str,
        fields: Sequence[tuple[int, FieldValue]],
        *,
        on_allocated: Callable[[int], None] | None = None,
    ) -> int:
        """Persist, then write, one application message. Returns its MsgSeqNum.

        Raises :class:`SessionDownError` or :class:`SessionFailedError` if the session
        could not take it, or :class:`FixStoreError` if storing failed. Nothing was
        stored or written in those cases. Raises
        :class:`~ismet.errors.AmbiguousOutcome` once the message is stored but its write
        did not complete, because it may have reached the peer and may yet be resent
        under the same number. ``on_allocated`` runs
        before the write, so a caller can match a reply that arrives during it.
        """
        body = encode_fields(fields)
        async with self._send_lock:
            self._require_ready()
            sending_time = format_utc_timestamp(self._clock.now())
            record = StoredMessage(0, msg_type, sending_time, body)
            seq = await self.run_store(lambda: self.store.allocate_outbound(record))
            if on_allocated is not None:
                on_allocated(seq)
            try:
                # The stored time, so a later OrigSendingTime equals this SendingTime.
                frame = self._frame(seq, msg_type, body, sending_time=sending_time)
                await self._write(frame)
            except _DisconnectError as exc:
                raise AmbiguousOutcome(
                    f"FIX message {seq} stored but its write did not complete: {exc}"
                ) from exc
            return seq

    def _require_ready(self) -> None:
        if self._failure is not None:
            raise SessionFailedError(f"FIX session failed: {self._failure}")
        if not self.is_ready:
            raise SessionDownError(f"FIX session is {self._state.value}")

    async def send_reject(
        self, ref_seq: int, reason: int, text: str, *, tag: int | None = None
    ) -> None:
        fields: list[tuple[int, FieldValue]] = [(45, ref_seq)]
        if tag is not None:
            fields.append((371, tag))
        fields.append((373, reason))
        fields.append((58, text))
        await self._send_admin("3", fields)

    def _logon_fields(self, *, reset: bool) -> list[tuple[int, FieldValue]]:
        cfg = self.config
        fields: list[tuple[int, FieldValue]] = [
            (98, 0),
            (108, cfg.heartbeat_interval),
        ]
        if reset:
            fields.append((141, "Y"))
        if cfg.username is not None:
            fields.append((553, cfg.username))
        if cfg.password is not None:
            fields.append((554, cfg.password))
        if cfg.default_appl_ver_id is not None:
            fields.append((1137, cfg.default_appl_ver_id))
        fields.extend(cfg.logon_fields)
        return fields

    async def _send_logon(self) -> None:
        cfg = self.config
        self._reset_awaiting_confirmation = None
        async with self._send_lock:
            if cfg.reset_on_logon:
                period = cfg.session_period(self._clock.now())
                if period != await self.run_store(self.store.reset_period):
                    await self._reset_numbers()
                    self._reset_awaiting_confirmation = period
            self._set_state(SessionState.LOGON_SENT)
            reset = self._reset_awaiting_confirmation is not None
            await self._send_admin_locked("A", self._logon_fields(reset=reset))

    async def _reset_numbers(self) -> None:
        """Both numbers back to 1. The caller holds the send lock."""
        await self.run_store(self.store.reset_sequences)
        self._next_in = 1
        self._resend_target = None
        if self.listener is not None:
            self.listener.on_sequence_reset()

    async def _begin_logout(self, text: str) -> None:
        self._deadline = self._clock.monotonic() + self.config.logout_timeout
        self._set_state(SessionState.LOGOUT_SENT)
        await self._send_admin("5", [(58, text)])

    async def _send_resend_request(self) -> None:
        self._resend_requested_at = self._clock.monotonic()
        self._resend_requests += 1
        await self._send_admin("2", [(7, self._next_in), (16, 0)])

    async def _terminate(self, text: str) -> NoReturn:
        """Send a Logout, give the peer a moment to answer, and drop the connection."""
        with contextlib.suppress(_DisconnectError, FixStoreError):
            await self._send_admin("5", [(58, text)])
        raise _DisconnectError(text)

    # Inbound

    async def _advance(self, to: int | None = None) -> None:
        value = to if to is not None else self._next_in + 1
        await self.run_store(lambda: self.store.set_next_inbound(value))
        self._next_in = value
        if self._resend_target is not None:
            # Catching up is progress, however long the history is.
            now = self._clock.monotonic()
            self._resend_requested_at = now
            self._resend_requests = min(self._resend_requests, 1)
            if self._state is SessionState.SYNCING:
                self._deadline = now + self.config.logon_timeout
        if self._resend_target is not None and self._next_in > self._resend_target:
            self._resend_target = None
            self._resend_requests = 0
            if self._state is SessionState.SYNCING and not self._synced:
                # The answer to the first TestRequest may have been gap-filled away.
                await self._send_sync_request()
        self._maybe_ready()

    async def _send_sync_request(self) -> None:
        self._test_counter += 1
        sync_id = f"SYNC{self._test_counter}"
        self._sync_ids.add(sync_id)
        self._sync_sent_at = self._clock.monotonic()
        await self._send_admin("1", [(112, sync_id)])

    async def _handle(self, message: FixMessage) -> None:
        """Apply the session checks, in the order of test case 2."""
        self._last_received = self._clock.monotonic()
        self._test_request_at = None
        cfg = self.config
        expected = self._next_in
        if message.begin_string != cfg.begin_string:
            await self._terminate(f"Incorrect BeginString {message.begin_string}")
        try:
            # SeqNum is a positive int, and the int datatype allows leading zeros.
            seq = parse_seq_num(message.get(34) or "", 34)
        except FieldFormatError:
            seq = 0
        if seq == 0:
            await self._terminate("MsgSeqNum missing or invalid")
        msg_type = message.msg_type
        if message.get(49) != cfg.target_comp_id or message.get(56) != (
            cfg.sender_comp_id
        ):
            await self._discard(message, seq, "CompID problem")
            await self.send_reject(seq, REJECT_COMP_ID, "CompID problem")
            if seq == expected:
                await self._advance()
            await self._terminate("Incorrect SenderCompID or TargetCompID")
        sending_time = await self._check_sending_time(message, seq, expected)
        if sending_time is None:
            return
        if self._state is SessionState.LOGOUT_SENT and msg_type == "5":
            # Case 13a. A confirming Logout ends this connection whatever its number.
            if seq == expected:
                await self._advance()
            raise _DisconnectError("Logout confirmed")
        if self._state is SessionState.LOGON_SENT:
            if msg_type == "5":
                await self._on_logon_refused(message)
            if msg_type != "A":
                await self._terminate("first message not a logon")
        if msg_type == "A" and message.get(141) == "Y":
            await self._on_logon_with_reset(message, seq)
            return
        if msg_type == "4" and message.get(123) != "Y":
            await self._on_sequence_reset_reset(message, seq)
            return
        poss_dup = message.get(43) == "Y"
        if seq < expected:
            if not poss_dup:
                await self._fail_too_low(expected, seq)
            await self._check_orig_sending_time(message, seq, sending_time, advance=0)
            return
        if seq > expected:
            await self._on_gap(message, seq)
            return
        if (
            poss_dup
            and msg_type != "4"
            and not await self._check_orig_sending_time(
                message, seq, sending_time, advance=1
            )
        ):
            return
        if message.defect is not None and msg_type in ADMIN_TYPES:
            await self.send_reject(
                seq, message.defect.reason, message.defect.text, tag=message.defect.tag
            )
            await self._advance()
            return
        if msg_type in ADMIN_TYPES:
            await self._on_admin(message, seq, poss_dup)
            return
        await self._deliver(message, seq)

    async def _discard(self, message: FixMessage, seq: int, reason: str) -> None:
        if (
            self.listener is not None
            and seq == self._next_in
            and message.msg_type not in ADMIN_TYPES
        ):
            await self.listener.on_discarded_message(message, reason)

    async def _check_sending_time(
        self, message: FixMessage, seq: int, expected: int
    ) -> datetime | None:
        raw = message.get(52)
        if raw is None:
            await self._discard(message, seq, "SendingTime missing")
            await self.send_reject(
                seq, REJECT_REQUIRED_TAG_MISSING, "SendingTime missing", tag=52
            )
            if seq == expected:
                await self._advance()
            return None
        problem = "SendingTime accuracy problem"
        try:
            parsed = parse_utc_timestamp(raw, 52)
        except FieldFormatError:
            parsed = None
        limit = self.config.max_sending_time_skew
        if parsed is None or (
            limit is not None
            and abs(self._clock.now() - parsed) > timedelta(seconds=limit)
        ):
            await self._discard(message, seq, problem)
            await self.send_reject(seq, REJECT_SENDING_TIME, problem, tag=52)
            if seq == expected:
                await self._advance()
            await self._terminate(problem)
        return parsed

    async def _check_orig_sending_time(
        self, message: FixMessage, seq: int, sending_time: datetime, *, advance: int
    ) -> bool:
        """Cases 2e, 2f, 2g. True if the PossDup's times are consistent."""
        raw = message.get(122)
        if raw is None:
            if message.msg_type == "4":
                return True
            await self._discard(message, seq, "OrigSendingTime missing")
            await self.send_reject(
                seq, REJECT_REQUIRED_TAG_MISSING, "OrigSendingTime missing", tag=122
            )
            if advance:
                await self._advance()
            return False
        try:
            original = parse_utc_timestamp(raw, 122)
        except FieldFormatError:
            await self._discard(message, seq, "bad OrigSendingTime")
            await self.send_reject(
                seq, REJECT_INCORRECT_FORMAT, "bad OrigSendingTime", tag=122
            )
            if advance:
                await self._advance()
            return False
        if original > sending_time:
            await self._discard(message, seq, "OrigSendingTime after SendingTime")
            await self.send_reject(
                seq, REJECT_SENDING_TIME, "OrigSendingTime after SendingTime", tag=122
            )
            if advance:
                await self._advance()
            await self._terminate("SendingTime accuracy problem")
        return True

    async def _fail_too_low(self, expected: int, seq: int) -> NoReturn:
        text = f"MsgSeqNum too low, expecting {expected} but received {seq}"
        with contextlib.suppress(_DisconnectError, FixStoreError):
            await self._send_admin("5", [(58, text)])
        raise _TerminalError(SequenceTooLowError(text))

    async def _on_logon_refused(self, message: FixMessage) -> NoReturn:
        text = self.scrub(message.get(58) or "no reason given")
        raise _TerminalError(LogonRejectedError(f"FIX logon refused: {text}"))

    async def _on_gap(self, message: FixMessage, seq: int) -> None:
        msg_type = message.msg_type
        if msg_type == "A":
            self._on_logon_confirmed(message)
        elif msg_type == "2" and message.get(43) != "Y":
            await self._on_resend_request(message, seq)
        target = self._resend_target
        self._resend_target = seq if target is None else max(target, seq)
        if target is None:
            await self._send_resend_request()
        if msg_type == "A":
            await self._after_logon()
        elif msg_type == "5":
            await self._begin_logout("Logout confirmed")

    async def _on_admin(self, message: FixMessage, seq: int, poss_dup: bool) -> None:
        msg_type = message.msg_type
        if msg_type == "4":
            await self._on_gap_fill(message, seq)
            return
        if msg_type == "3":
            if self.listener is not None:
                await self.listener.on_session_reject(message)
            await self._advance()
            return
        if poss_dup:
            # "Message Recovery": a resent admin message counts for sequence only.
            await self._advance()
            return
        if msg_type == "A":
            if self._state is SessionState.LOGON_SENT:
                self._on_logon_confirmed(message)
                await self._advance()
                await self._after_logon()
            else:
                logger.warning("FIX session ignored an unexpected Logon")
                await self._advance()
        elif msg_type == "0":
            await self._on_heartbeat(message)
        elif msg_type == "1":
            await self._on_test_request(message, seq)
        elif msg_type == "2":
            await self._on_resend_request(message, seq)
            await self._advance()
        elif msg_type == "5":
            await self._advance()
            logger.warning("FIX peer logged out: %s", self.scrub(message.get(58) or ""))
            await self._begin_logout("Logout confirmed")

    def _on_logon_confirmed(self, message: FixMessage) -> None:
        echoed = message.get(108)
        if echoed != str(self.config.heartbeat_interval):
            logger.warning(
                "FIX peer echoed HeartBtInt %s, ours is %s",
                echoed,
                self.config.heartbeat_interval,
            )
        self._deadline = self._clock.monotonic() + self.config.logon_timeout
        self._set_state(SessionState.SYNCING)

    async def _after_logon(self) -> None:
        period = self._reset_awaiting_confirmation
        if period is not None:
            self._reset_awaiting_confirmation = None
            await self.run_store(lambda: self.store.set_reset_period(period))
        await self._send_sync_request()

    def _maybe_ready(self) -> None:
        if (
            self._state is SessionState.SYNCING
            and self._synced
            and self._resend_target is None
        ):
            self._set_state(SessionState.READY)
            if self.listener is not None:
                self.listener.on_session_ready()

    async def _on_logon_with_reset(self, message: FixMessage, seq: int) -> None:
        if seq != 1:
            await self.send_reject(
                seq, REJECT_VALUE_INCORRECT, "ResetSeqNumFlag Logon must be 1", tag=34
            )
            await self._terminate("ResetSeqNumFlag Logon with MsgSeqNum other than 1")
        if (
            self._reset_awaiting_confirmation is not None
            and self._state is SessionState.LOGON_SENT
        ):
            await self._advance(2)
            self._on_logon_confirmed(message)
        else:
            await self._obey_peer_reset(message)
        await self._after_logon()

    async def _obey_peer_reset(self, message: FixMessage) -> None:
        """ "Logon": the receiver must obey a reset, and it answers in kind at 1.

        Application messages stop first, so none can precede our Logon or be numbered
        under the old counters. Whatever was not yet read can no longer be requested.
        """
        logger.warning("FIX peer reset the sequence numbers at Logon")
        unread_from = self._next_in
        if self._state in (
            SessionState.LOGON_SENT,
            SessionState.READY,
            SessionState.SYNCING,
        ):
            self._on_logon_confirmed(message)
        self._synced = False
        self._sync_ids = set()
        if unread_from > 1 and self.listener is not None:
            await self.listener.on_possible_message_loss(
                unread_from, None, "peer reset the sequence numbers at Logon"
            )
        async with self._send_lock:
            await self._reset_numbers()
            await self._advance(2)
            await self._send_admin_locked("A", self._logon_fields(reset=True))

    async def _on_heartbeat(self, message: FixMessage) -> None:
        if message.get(112) in self._sync_ids:
            self._sync_ids.clear()
            self._synced = True
        await self._advance()

    async def _on_test_request(self, message: FixMessage, seq: int) -> None:
        test_id = message.get(112)
        if test_id is None:
            await self.send_reject(
                seq, REJECT_REQUIRED_TAG_MISSING, "TestReqID missing", tag=112
            )
        else:
            await self._send_admin("0", [(112, test_id)])
        await self._advance()

    async def _on_gap_fill(self, message: FixMessage, seq: int) -> None:
        try:
            new_seq = parse_seq_num(message.require(36), 36)
        except FieldFormatError:
            await self.send_reject(
                seq, REJECT_REQUIRED_TAG_MISSING, "NewSeqNo missing or invalid", tag=36
            )
            await self._advance()
            return
        if new_seq <= seq:
            await self.send_reject(
                seq,
                REJECT_VALUE_INCORRECT,
                f"attempt to lower sequence number, invalid value NewSeqNum={new_seq}",
                tag=36,
            )
            return
        await self._advance(new_seq)

    async def _on_sequence_reset_reset(self, message: FixMessage, seq: int) -> None:
        try:
            new_seq = parse_seq_num(message.require(36), 36)
        except FieldFormatError:
            await self.send_reject(
                seq, REJECT_REQUIRED_TAG_MISSING, "NewSeqNo missing or invalid", tag=36
            )
            return
        if new_seq < self._next_in:
            await self.send_reject(
                seq,
                REJECT_VALUE_INCORRECT,
                f"attempt to lower sequence number, invalid value NewSeqNum={new_seq}",
                tag=36,
            )
            return
        if new_seq == self._next_in:
            logger.warning("FIX SequenceReset-Reset to the number already expected")
            return
        logger.warning("FIX SequenceReset-Reset moves inbound to %d", new_seq)
        if self.listener is not None:
            await self.listener.on_possible_message_loss(
                self._next_in, new_seq - 1, "SequenceReset-Reset skipped these numbers"
            )
        await self._advance(new_seq)

    async def _on_resend_request(self, message: FixMessage, seq: int) -> None:
        try:
            begin = parse_seq_num(message.require(7), 7)
            end = parse_seq_num(message.require(16), 16)
        except FieldFormatError as exc:
            await self.send_reject(
                seq,
                REJECT_REQUIRED_TAG_MISSING,
                "BeginSeqNo or EndSeqNo missing or invalid",
                tag=exc.tag,
            )
            return
        await self._resend(begin, end)

    async def _resend(self, begin: int, end: int) -> None:
        """Answer a ResendRequest. The lock keeps new messages out of the sequence."""
        async with self._send_lock:
            last = await self.run_store(self.store.next_outbound) - 1
            # EndSeqNo 0 means infinity in 4.2 and later; 999999 did in 4.1 and earlier.
            if end == 0 or end > last:
                end = last
            if begin < 1 or begin > end:
                logger.warning("FIX ResendRequest has nothing to resend")
                return
            stored = {
                record.seq: record
                for record in await self.run_store(
                    lambda: self.store.messages(begin, end)
                )
            }
            gap_start: int | None = None
            for seq in range(begin, end + 1):
                record = stored.get(seq)
                if record is None or self._is_stale(record):
                    gap_start = seq if gap_start is None else gap_start
                    continue
                if gap_start is not None:
                    await self._write_gap_fill(gap_start, seq)
                    gap_start = None
                frame = self._frame(
                    seq,
                    record.msg_type,
                    record.body,
                    poss_dup=True,
                    orig_sending_time=record.sending_time,
                )
                await self._write(frame)
            if gap_start is not None:
                await self._write_gap_fill(gap_start, end + 1)
            if self._state is SessionState.SYNCING:
                self._deadline = self._clock.monotonic() + self.config.logon_timeout

    def _is_stale(self, record: StoredMessage) -> bool:
        sent = parse_utc_timestamp(record.sending_time, 52)
        stale = self._clock.now() - sent > timedelta(
            seconds=self.config.stale_resend_after
        )
        if stale:
            logger.warning(
                "FIX gap-filling aged message %d instead of resending it", record.seq
            )
        return stale

    async def _write_gap_fill(self, seq: int, new_seq: int) -> None:
        now = format_utc_timestamp(self._clock.now())
        body = encode_fields([(123, "Y"), (36, new_seq)])
        await self._write(
            self._frame(seq, "4", body, poss_dup=True, orig_sending_time=now)
        )

    async def _deliver(self, message: FixMessage, seq: int) -> None:
        if self.listener is None:
            await self._advance()
            return
        try:
            await self.listener.on_application_message(message)
        except ApplicationRejectError as exc:
            await self.send_reject(seq, exc.reason, exc.text, tag=exc.tag)
        except (_DisconnectError, _TerminalError, FixStoreError):
            raise
        except Exception as exc:
            logger.exception("FIX application handler failed for message %d", seq)
            raise _DisconnectError(
                f"application handler failed: {type(exc).__name__}"
            ) from exc
        else:
            # Case 14 rejects a field defect, but a readable fill must not be lost.
            defect = message.defect
            if defect is not None:
                await self.send_reject(seq, defect.reason, defect.text, tag=defect.tag)
        await self._advance()


__all__ = [
    "ADMIN_TYPES",
    "APPL_VER_FIX50SP2",
    "BEGIN_STRINGS",
    "ApplicationRejectError",
    "FixSession",
    "FixSessionConfig",
    "LogonRejectedError",
    "SequenceTooLowError",
    "SessionDownError",
    "SessionFailedError",
    "SessionListener",
    "SessionState",
]
