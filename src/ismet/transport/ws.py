"""WebSocket transport with reconnect, heartbeat, resubscribe, backpressure.

The transport owns a background task that keeps a connection alive. Incoming
messages are decoded (floats as ``Decimal``) and placed on a bounded queue
that :meth:`WebSocketTransport.messages` drains. When the connection drops the
task backs off, reconnects, and calls ``on_connect`` again so the provider can
re-authenticate and resubscribe. Every reconnect is logged, so an outage is visible
even to a consumer blocked in ``messages``, and ``on_state`` reports each state.
``max_reconnects`` bounds consecutive attempts that deliver no data, however the
connection ended; it resets once a data message arrives, and ``is_data`` says which
messages count, so a server that acknowledges and closes still reaches the bound. A
retryable :class:`TransportError` raised by ``on_connect`` (for
example a send on a socket that just dropped) counts as one more attempt; any
other error outside the socket layer (decoding, a non-retryable error from
``on_connect``) is terminal: the transport moves to ``FAILED`` and surfaces it
from ``wait_connected`` and ``messages``.

Handshake failures that cannot succeed on retry are terminal too, because the
URL and connect options are fixed at construction: a malformed URL raises
:class:`TransportError` (not retryable), and a 4xx rejection other than 429
raises :class:`AuthError` for 401/403 or a non-retryable
:class:`TransportError` otherwise, each naming the URL and status with the
``websockets`` exception as ``__cause__``. 429 and 5xx rejections reconnect
with backoff like any socket error. ``start`` never leaks its task: if the
first connect fails or times out the task is cancelled before the error is
raised, and a fresh ``start`` after ``close`` begins with clean failure state
and stats and an emptied queue; the queue object itself never changes, so a
consumer already waiting in ``messages`` keeps receiving across restarts.

``before_connect`` runs before every handshake, whether or not the handshake then
succeeds, so a provider can charge a connection budget there. ``on_rejected`` sees the
status, body and lower-cased headers of each refused handshake, and an error it returns
ends the stream with that error. A 429 rejection waits at least its ``Retry-After``, and
the reconnect log line states the wait.
Under a dropping policy only data messages are dropped: a message ``is_data`` rejects,
such as a venue's refusal, evicts the oldest instead, and the first drop is logged at
WARNING. ``stats.dropped`` counts every message lost, and :meth:`lost_before` says where
in the delivered order a loss belongs.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any

import websockets
from websockets.asyncio.client import ClientConnection, connect

from ismet import _aio
from ismet.errors import AuthError, IsmetError, RateLimited, TransportError
from ismet.transport.backoff import ExponentialBackoff
from ismet.transport.clock import SYSTEM_CLOCK, Clock
from ismet.transport.http import retry_after_seconds

logger = logging.getLogger("ismet.transport.ws")

DEFAULT_WS_BACKOFF = ExponentialBackoff(base=0.5, maximum=30.0)


class BackpressurePolicy(str, Enum):
    """What to do when the consumer falls behind and the queue is full."""

    BLOCK = "block"
    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"


class ConnectionState(str, Enum):
    IDLE = "idle"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    CLOSED = "closed"
    FAILED = "failed"


@dataclass
class WsStats:
    connects: int = 0
    reconnects: int = 0
    received: int = 0
    dropped: int = 0
    evicted: int = 0
    sent: int = 0

    def reset(self) -> None:
        """Zero every counter in place so held references stay live."""
        self.connects = 0
        self.reconnects = 0
        self.received = 0
        self.dropped = 0
        self.evicted = 0
        self.sent = 0


class _Closed:
    pass


_CLOSED = _Closed()

OnConnect = Callable[["WebSocketTransport"], Awaitable[None]]
OnState = Callable[[ConnectionState], None]
BeforeConnect = Callable[[], Awaitable[None]]
OnRejected = Callable[[int, bytes, Mapping[str, str]], IsmetError | None]


class SequenceOutcome(str, Enum):
    """How a sequence number relates to what was already seen."""

    FIRST = "first"
    IN_ORDER = "in_order"
    GAP = "gap"
    DUPLICATE = "duplicate"
    OUT_OF_ORDER = "out_of_order"


@dataclass(frozen=True)
class SequenceObservation:
    """Result of observing one sequence number.

    ``missing`` is how many values were skipped on a gap, otherwise zero.
    ``expected`` is the value that would have continued the sequence.
    """

    outcome: SequenceOutcome
    sequence: int
    expected: int | None
    missing: int = 0


class SequenceTracker:
    """Classify a stream of sequence numbers for one feed or symbol.

    A value at or below the high-water mark is a duplicate, unless a gap reported it
    missing: then it is a late, out-of-order delivery of real data. A value below the
    first one seen counts the same way, once, and everything between it and that first
    value is treated as a gap. The high-water mark never moves down. Missing ranges are
    kept in a bounded list, so memory stays bounded however many gaps a feed has; a very
    late delivery past that bound counts as a duplicate.
    """

    MAX_TRACKED_GAPS = 1024

    def __init__(self) -> None:
        self.last: int | None = None
        self.gaps = 0
        self.missing = 0
        self.duplicates = 0
        self.out_of_order = 0
        self._missing: list[tuple[int, int]] = []
        self._first = 0

    def _remember_missing(self, low: int, high: int) -> None:
        if low <= high:
            self._missing.append((low, high))
            del self._missing[: -self.MAX_TRACKED_GAPS]

    def _take_missing(self, sequence: int) -> bool:
        for index, (low, high) in enumerate(self._missing):
            if low <= sequence <= high:
                pieces = []
                if low < sequence:
                    pieces.append((low, sequence - 1))
                if sequence < high:
                    pieces.append((sequence + 1, high))
                self._missing[index : index + 1] = pieces
                return True
        return False

    def classify(self, sequence: int) -> SequenceObservation:
        """Record ``sequence`` and return how it relates to the prior ones."""
        last = self.last
        if last is None:
            self.last = sequence
            self._first = sequence
            return SequenceObservation(SequenceOutcome.FIRST, sequence, None)
        expected = last + 1
        if sequence == expected:
            self.last = sequence
            return SequenceObservation(SequenceOutcome.IN_ORDER, sequence, expected)
        if sequence <= last:
            if sequence < self._first:
                self._remember_missing(sequence + 1, self._first - 1)
                self._first = sequence
                late = True
            else:
                late = self._take_missing(sequence)
            if late:
                self.out_of_order += 1
                return SequenceObservation(
                    SequenceOutcome.OUT_OF_ORDER, sequence, expected
                )
            self.duplicates += 1
            return SequenceObservation(SequenceOutcome.DUPLICATE, sequence, expected)
        missing = sequence - expected
        self._remember_missing(expected, sequence - 1)
        self.last = sequence
        self.gaps += 1
        self.missing += missing
        return SequenceObservation(SequenceOutcome.GAP, sequence, expected, missing)

    def reset(self) -> None:
        """Forget the position, for a venue that restarts sequences on reconnect."""
        self.last = None
        self._missing.clear()


class WebSocketTransport:
    """A self-healing WebSocket connection producing decoded JSON messages."""

    def __init__(
        self,
        url: str,
        *,
        on_connect: OnConnect | None = None,
        on_state: OnState | None = None,
        before_connect: BeforeConnect | None = None,
        on_rejected: OnRejected | None = None,
        backoff: ExponentialBackoff = DEFAULT_WS_BACKOFF,
        max_reconnects: int | None = None,
        heartbeat_interval: float | None = 20.0,
        heartbeat_timeout: float | None = 20.0,
        queue_size: int = 10_000,
        backpressure: BackpressurePolicy = BackpressurePolicy.BLOCK,
        decode: Callable[[str | bytes], Any] | None = None,
        is_data: Callable[[Any], bool] | None = None,
        connect_kwargs: dict[str, Any] | None = None,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        self.url = url
        self.stats = WsStats()
        self.state = ConnectionState.IDLE
        self._on_connect = on_connect
        self._on_state = on_state
        self._before_connect = before_connect
        self._on_rejected = on_rejected
        self._backoff = backoff
        self._max_reconnects = max_reconnects
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout
        self._policy = backpressure
        self._decode = decode or (lambda raw: json.loads(raw, parse_float=Decimal))
        self._is_data = is_data
        self._connect_kwargs = connect_kwargs or {}
        self._clock = clock
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=queue_size)
        self._conn: ClientConnection | None = None
        self._task: asyncio.Task[None] | None = None
        self._connected = asyncio.Event()
        self._stop = asyncio.Event()
        self._failure: IsmetError | None = None
        self._last_error: BaseException | None = None
        self._told_dropped = 0
        self._told_evicted = 0

    def _set_state(self, state: ConnectionState) -> None:
        self.state = state
        if self._on_state is not None:
            try:
                self._on_state(state)
            except Exception:
                logger.exception("on_state hook failed")

    async def __aenter__(self) -> WebSocketTransport:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def start(
        self, *, wait_connected: bool = True, timeout: float = 30.0
    ) -> None:
        """Start the connection loop; by default wait for the first connect.

        If the first connect fails or does not complete within ``timeout`` the
        background task is stopped before the error propagates, so a failed
        start never leaves a reconnect loop running.
        """
        if self._task is not None:
            return
        self._stop.clear()
        self._connected.clear()
        self._failure = None
        self._last_error = None
        self.stats.reset()
        self._told_dropped = self._told_evicted = 0
        self._drain_queue()
        self._task = asyncio.create_task(self._run(), name=f"ismet-ws:{self.url}")
        if not wait_connected:
            return
        try:
            await self.wait_connected(timeout)
        except BaseException:
            await self._shutdown()
            if self.state is not ConnectionState.FAILED:
                self._set_state(ConnectionState.CLOSED)
            self._enqueue(_CLOSED, force=True)
            raise

    async def wait_connected(self, timeout: float = 30.0) -> None:
        try:
            await _aio.wait_for(self._connected.wait(), timeout)
        except asyncio.TimeoutError as exc:
            if self._failure is not None:
                raise self._failure from self._failure.__cause__
            cause = self._last_error if self._last_error is not None else exc
            raise TransportError(
                f"websocket {self.url} not connected within {timeout}s"
            ) from cause
        if self._failure is not None:
            raise self._failure

    async def _shutdown(self) -> None:
        self._stop.set()
        task, self._task = self._task, None
        if self._conn is not None:
            with contextlib.suppress(Exception):
                await self._conn.close()
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    async def close(self) -> None:
        """Stop reconnecting, close the socket, and end :meth:`messages`."""
        await self._shutdown()
        self._set_state(ConnectionState.CLOSED)
        self._enqueue(_CLOSED, force=True)

    async def send_json(self, message: Any) -> None:
        """Send ``message`` as JSON on the current connection."""
        conn = self._conn
        if conn is None:
            raise TransportError(f"websocket {self.url} is not connected")
        try:
            await conn.send(json.dumps(message, default=str))
        except websockets.exceptions.ConnectionClosed as exc:
            raise TransportError(
                f"websocket {self.url} closed while sending: {exc}"
            ) from exc
        self.stats.sent += 1

    async def messages(self) -> AsyncIterator[Any]:
        """Yield decoded messages until the transport is closed or fails."""
        while True:
            item = await self._queue.get()
            if item is _CLOSED:
                if self._failure is not None:
                    raise self._failure
                return
            yield item

    def _drain_queue(self) -> None:
        while True:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return

    def _enqueue(self, item: Any, *, force: bool = False) -> bool:
        if force or self._policy is BackpressurePolicy.BLOCK:
            if self._queue.full() and force:
                with contextlib.suppress(asyncio.QueueEmpty):
                    self._queue.get_nowait()
                    self._count_drop()
                    self.stats.evicted += 1
            try:
                self._queue.put_nowait(item)
                return True
            except asyncio.QueueFull:
                return False
        if self._queue.full():
            if self._policy is BackpressurePolicy.DROP_NEWEST:
                self._count_drop()
                return False
            with contextlib.suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
                self._count_drop()
        self._queue.put_nowait(item)
        return True

    def lost_before(self, stamped: int) -> bool:
        """Whether a message was lost ahead of this one, and not yet reported.

        ``stamped`` is ``stats.dropped`` read when this message was decoded. Call once
        per message, in delivery order. A newest-dropping queue discards only what
        arrives after everything queued, so the stamp places the loss exactly. A message
        evicted from the front is reported on the next one delivered, and the stamp
        still places any drop after that. A later stamp also counts the eviction, so a
        message can be told of a loss already reported, which is safer than missing one.
        """
        evicted = self.stats.evicted != self._told_evicted
        self._told_evicted = self.stats.evicted
        if self._policy is BackpressurePolicy.DROP_NEWEST:
            lost = evicted or stamped > self._told_dropped
            self._told_dropped = max(self._told_dropped, stamped)
            return lost
        if self.stats.dropped <= self._told_dropped:
            return False
        self._told_dropped = self.stats.dropped
        return True

    def _count_drop(self) -> None:
        self.stats.dropped += 1
        if self.stats.dropped == 1:
            logger.warning(
                "websocket %s dropped a message: the consumer is behind a full queue "
                "(%s)",
                self.url,
                self._policy.value,
            )

    async def _put(self, item: Any, *, is_data: bool) -> None:
        if self._policy is BackpressurePolicy.BLOCK:
            await self._queue.put(item)
        else:
            self._enqueue(item, force=not is_data)

    def _fail(self, failure: IsmetError) -> None:
        self._failure = failure
        self._set_state(ConnectionState.FAILED)
        self._connected.set()

    def _handshake_failure(
        self, exc: websockets.exceptions.InvalidStatus
    ) -> IsmetError | None:
        status = exc.response.status_code
        if not 400 <= status < 500 or status == 429:
            return None
        message = f"websocket {self.url} handshake rejected with HTTP {status}"
        failure: IsmetError
        if status in (401, 403):
            failure = AuthError(message, status=status)
        else:
            failure = TransportError(message, retryable=False, status=status)
        failure.__cause__ = exc
        return failure

    @staticmethod
    def _headers(exc: websockets.exceptions.InvalidStatus) -> dict[str, str]:
        return {name.lower(): v for name, v in exc.response.headers.raw_items()}

    def _stated_wait(self, exc: websockets.exceptions.InvalidStatus) -> float:
        return retry_after_seconds(self._headers(exc), clock=self._clock) or 0.0

    def _rejected(self, exc: websockets.exceptions.InvalidStatus) -> IsmetError | None:
        if self._on_rejected is None:
            return None
        try:
            verdict = self._on_rejected(
                exc.response.status_code,
                bytes(exc.response.body or b""),
                self._headers(exc),
            )
        except Exception:
            logger.exception("on_rejected hook failed")
            return None
        if verdict is not None:
            verdict.__cause__ = exc
        return verdict

    async def _run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            self._set_state(
                ConnectionState.RECONNECTING if attempt else ConnectionState.CONNECTING
            )
            reason: object = "server closed the connection"
            fatal: IsmetError | None = None
            stated_wait = 0.0
            try:
                if self._before_connect is not None:
                    await self._before_connect()
                async with connect(
                    self.url,
                    ping_interval=self._heartbeat_interval,
                    ping_timeout=self._heartbeat_timeout,
                    **self._connect_kwargs,
                ) as conn:
                    self._conn = conn
                    self.stats.connects += 1
                    if attempt:
                        self.stats.reconnects += 1
                    if self._on_connect is not None:
                        await self._on_connect(self)
                    self._set_state(ConnectionState.CONNECTED)
                    self._connected.set()
                    async for raw in conn:
                        self.stats.received += 1
                        item = self._decode(raw)
                        is_data = self._is_data is None or self._is_data(item)
                        if is_data:
                            attempt = 0
                        await self._put(item, is_data=is_data)
            except asyncio.CancelledError:
                raise
            except websockets.exceptions.InvalidURI as exc:
                fatal = TransportError(
                    f"websocket {self.url} has an invalid URI: {exc}",
                    retryable=False,
                )
                fatal.__cause__ = exc
            except websockets.exceptions.InvalidStatus as exc:
                self._last_error = exc
                fatal = self._rejected(exc) or self._handshake_failure(exc)
                if fatal is None:
                    reason = exc
                    stated_wait = self._stated_wait(exc)
            except (
                websockets.exceptions.WebSocketException,
                OSError,
                TimeoutError,
                asyncio.TimeoutError,
            ) as exc:
                self._last_error = exc
                reason = exc
            except RateLimited as exc:
                fatal = (
                    exc  # before_connect refusing to wait; more attempts cannot help
                )
            except TransportError as exc:
                if exc.retryable:
                    self._last_error = exc
                    reason = exc
                else:
                    fatal = exc
            except Exception as exc:
                fatal = TransportError(
                    f"websocket {self.url} failed: {exc!r}", retryable=False
                )
                fatal.__cause__ = exc
            finally:
                self._conn = None
                self._connected.clear()
            if self._stop.is_set():
                break
            if fatal is None:
                attempt += 1
                if self._max_reconnects is not None and attempt > self._max_reconnects:
                    fatal = TransportError(
                        f"websocket {self.url} failed after {attempt - 1} "
                        f"reconnects: {reason}",
                        retryable=False,
                    )
                    if isinstance(reason, BaseException):
                        fatal.__cause__ = reason
            if fatal is not None:
                self._fail(fatal)
                break
            delay = max(self._backoff.delay(attempt - 1), stated_wait)
            logger.warning(
                "websocket %s lost (%s); reconnect attempt %d in %.1fs",
                self.url,
                reason,
                attempt,
                delay,
            )
            await asyncio.sleep(delay)
        self._enqueue(_CLOSED, force=True)


__all__ = [
    "DEFAULT_WS_BACKOFF",
    "BackpressurePolicy",
    "BeforeConnect",
    "ConnectionState",
    "OnRejected",
    "OnState",
    "SequenceObservation",
    "SequenceOutcome",
    "SequenceTracker",
    "WebSocketTransport",
    "WsStats",
]
