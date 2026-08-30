"""WebSocket transport with reconnect, heartbeat, resubscribe, backpressure.

The transport owns a background task that keeps a connection alive. Incoming
messages are decoded (floats as ``Decimal``) and placed on a bounded queue
that :meth:`WebSocketTransport.messages` drains. When the connection drops the
task backs off, reconnects, and calls ``on_connect`` again so the provider can
re-authenticate and resubscribe. ``max_reconnects`` bounds consecutive attempts
that deliver no message, however the connection ended; it resets once a message
arrives. A retryable :class:`TransportError` raised by ``on_connect`` (for
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
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any

import websockets
from websockets.asyncio.client import ClientConnection, connect

from ismet.errors import AuthError, IsmetError, TransportError
from ismet.transport.backoff import ExponentialBackoff

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
    sent: int = 0


class _Closed:
    pass


_CLOSED = _Closed()

OnConnect = Callable[["WebSocketTransport"], Awaitable[None]]
OnState = Callable[[ConnectionState], None]


class SequenceTracker:
    """Detect gaps in a monotonically increasing sequence number."""

    def __init__(self) -> None:
        self.last: int | None = None
        self.gaps = 0

    def observe(self, sequence: int) -> int:
        """Record ``sequence``; return the number of skipped values (0 if none)."""
        if self.last is None:
            self.last = sequence
            return 0
        gap = sequence - self.last - 1
        self.last = max(self.last, sequence)
        if gap > 0:
            self.gaps += 1
            return gap
        return 0


class WebSocketTransport:
    """A self-healing WebSocket connection producing decoded JSON messages."""

    def __init__(
        self,
        url: str,
        *,
        on_connect: OnConnect | None = None,
        on_state: OnState | None = None,
        backoff: ExponentialBackoff = DEFAULT_WS_BACKOFF,
        max_reconnects: int | None = None,
        heartbeat_interval: float | None = 20.0,
        heartbeat_timeout: float | None = 20.0,
        queue_size: int = 10_000,
        backpressure: BackpressurePolicy = BackpressurePolicy.BLOCK,
        decode: Callable[[str | bytes], Any] | None = None,
        connect_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self.url = url
        self.stats = WsStats()
        self.state = ConnectionState.IDLE
        self._on_connect = on_connect
        self._on_state = on_state
        self._backoff = backoff
        self._max_reconnects = max_reconnects
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout
        self._policy = backpressure
        self._decode = decode or (lambda raw: json.loads(raw, parse_float=Decimal))
        self._connect_kwargs = connect_kwargs or {}
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=queue_size)
        self._conn: ClientConnection | None = None
        self._task: asyncio.Task[None] | None = None
        self._connected = asyncio.Event()
        self._stop = asyncio.Event()
        self._failure: IsmetError | None = None
        self._last_error: BaseException | None = None

    def _set_state(self, state: ConnectionState) -> None:
        self.state = state
        if self._on_state is not None:
            self._on_state(state)

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
        self.stats = WsStats()
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
            await asyncio.wait_for(self._connected.wait(), timeout)
        except asyncio.TimeoutError as exc:
            if self._failure is not None:
                raise self._failure from None
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
            try:
                self._queue.put_nowait(item)
                return True
            except asyncio.QueueFull:
                return False
        if self._queue.full():
            if self._policy is BackpressurePolicy.DROP_NEWEST:
                self.stats.dropped += 1
                return False
            with contextlib.suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
                self.stats.dropped += 1
        self._queue.put_nowait(item)
        return True

    async def _put(self, item: Any) -> None:
        if self._policy is BackpressurePolicy.BLOCK:
            await self._queue.put(item)
        else:
            self._enqueue(item)

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

    async def _run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            self._set_state(
                ConnectionState.RECONNECTING if attempt else ConnectionState.CONNECTING
            )
            reason: object = "server closed the connection"
            fatal: IsmetError | None = None
            try:
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
                        attempt = 0
                        self.stats.received += 1
                        await self._put(self._decode(raw))
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
                fatal = self._handshake_failure(exc)
                if fatal is None:
                    reason = exc
            except (
                websockets.exceptions.WebSocketException,
                OSError,
                TimeoutError,
                asyncio.TimeoutError,
            ) as exc:
                self._last_error = exc
                reason = exc
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
            await asyncio.sleep(self._backoff.delay(attempt - 1))
        self._enqueue(_CLOSED, force=True)


__all__ = [
    "DEFAULT_WS_BACKOFF",
    "BackpressurePolicy",
    "ConnectionState",
    "SequenceTracker",
    "WebSocketTransport",
    "WsStats",
]
