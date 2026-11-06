"""Record and replay vendor traffic, so adapter tests use real payloads offline.

HTTP goes through :class:`ReplayTransport`, an httpx transport that serves recorded
exchanges by method and URL with query and raises :class:`UnrecordedRequestError` for
anything else. WebSocket goes through :class:`WsReplayServer`, a localhost
``websockets`` server that replays recorded frames.

Recording keeps response headers on an allow-list, so a cookie or tracing header
cannot reach a fixture by default. Request headers are never stored.
:func:`find_secrets` is the independent check on the result.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
from collections import defaultdict, deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit, urlunsplit

import httpx
from websockets.asyncio.client import connect
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from ismet import _aio

ALLOWED_RESPONSE_HEADERS = frozenset(
    {
        "age",
        "cache-control",
        "cb-after",
        "cb-before",
        "content-type",
        "date",
        "retry-after",
        "x-mbx-used-weight",
        "x-mbx-used-weight-1m",
    }
)

_SECRET_PATTERNS: Mapping[str, re.Pattern[str]] = {
    "credential header": re.compile(
        r"(?i)\b(authorization|set-cookie|cookie|x-mbx-apikey|cb-access-[a-z]+)\b"
    ),
    "tracking cookie": re.compile(r"__cf[a-z_]*="),
    "credential field": re.compile(
        r"(?i)\b(api[_-]?key|secret|passphrase|signature|password|access[_-]?token)"
        r"\b\"?\s*[:=]"
    ),
    "bearer token": re.compile(r"(?i)\bbearer\s+[a-z0-9._~+/-]{8,}"),
    "aws key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "json web token": re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"),
    "private key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "ip address": re.compile(r"(?<![\d.])\d{1,3}(?:\.\d{1,3}){3}(?![\d.])"),
}


class UnrecordedRequestError(AssertionError):
    """A request reached replay with no matching recording.

    An ``AssertionError`` so no retry or error mapping in the transport swallows it.
    """


def exchange_key(method: str, url: str) -> tuple[str, str]:
    """Method and URL with query parameters sorted, the lookup key for replay."""
    parts = urlsplit(url)
    query = "&".join(f"{k}={v}" for k, v in sorted(parse_qsl(parts.query)))
    return method.upper(), urlunsplit(parts._replace(query=query))


@dataclass(frozen=True)
class RecordedExchange:
    """One HTTP request and its response, the body stored as text exactly as sent."""

    method: str
    url: str
    status: int
    headers: Mapping[str, str]
    body: str
    note: str | None = None


@dataclass(frozen=True)
class RecordedSession:
    """One WebSocket subscription.

    ``sent`` is what the client sends. ``control`` holds the leading frames such as
    acknowledgements, sent on every connection. ``frames`` holds the data frames.
    """

    url: str
    sent: tuple[Any, ...]
    control: tuple[str, ...]
    frames: tuple[str, ...]


def save_exchanges(
    path: Path, exchanges: Iterable[RecordedExchange], recorded_at: str
) -> None:
    """Write a cassette of HTTP exchanges."""
    document = {
        "recorded_at": recorded_at,
        "exchanges": [
            {
                "method": e.method,
                "url": e.url,
                "status": e.status,
                "headers": dict(e.headers),
                "body": e.body,
                **({"note": e.note} if e.note else {}),
            }
            for e in exchanges
        ],
    }
    _write_json(path, document)


def load_exchanges(path: Path) -> list[RecordedExchange]:
    """Read a cassette written by :func:`save_exchanges`."""
    document = json.loads(path.read_text(encoding="utf-8"))
    return [
        RecordedExchange(
            method=e["method"],
            url=e["url"],
            status=e["status"],
            headers=e["headers"],
            body=e["body"],
            note=e.get("note"),
        )
        for e in document["exchanges"]
    ]


def save_session(path: Path, session: RecordedSession, recorded_at: str) -> None:
    """Write a WebSocket recording."""
    _write_json(
        path,
        {
            "recorded_at": recorded_at,
            "url": session.url,
            "sent": list(session.sent),
            "control": list(session.control),
            "frames": list(session.frames),
        },
    )


def load_session(path: Path) -> RecordedSession:
    """Read a recording written by :func:`save_session`."""
    document = json.loads(path.read_text(encoding="utf-8"))
    return RecordedSession(
        url=document["url"],
        sent=tuple(document["sent"]),
        control=tuple(document["control"]),
        frames=tuple(document["frames"]),
    )


def _write_json(path: Path, document: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(document, indent=1, ensure_ascii=False, sort_keys=False)
    path.write_text(text + "\n", encoding="utf-8")


class RecordingTransport(httpx.AsyncBaseTransport):
    """Forward to the real network and keep a scrubbed copy of each exchange."""

    def __init__(self, inner: httpx.AsyncBaseTransport | None = None) -> None:
        self._inner = inner or httpx.AsyncHTTPTransport()
        self.exchanges: list[RecordedExchange] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self._inner.handle_async_request(request)
        # httpx decodes content-encoding on read; the stored body is the decoded one.
        response.request = request
        content = await response.aread()
        await response.aclose()
        headers = {
            name.lower(): value
            for name, value in response.headers.items()
            if name.lower() in ALLOWED_RESPONSE_HEADERS
        }
        self.exchanges.append(
            RecordedExchange(
                method=request.method,
                url=str(request.url),
                status=response.status_code,
                headers=headers,
                body=content.decode("utf-8"),
            )
        )
        return httpx.Response(
            response.status_code, headers=headers, content=content, request=request
        )

    async def aclose(self) -> None:
        await self._inner.aclose()


class ReplayTransport(httpx.AsyncBaseTransport):
    """Serve recorded exchanges and never touch the network.

    Repeated requests for one key are served in recorded order, and the last is
    served again once they run out, so an adapter may repeat an idempotent read.
    ``requests`` lists the key of every request seen, for assertions.
    """

    def __init__(self, exchanges: Iterable[RecordedExchange]) -> None:
        self._queues: dict[tuple[str, str], deque[RecordedExchange]] = defaultdict(
            deque
        )
        for exchange in exchanges:
            self._queues[exchange_key(exchange.method, exchange.url)].append(exchange)
        self.requests: list[tuple[str, str]] = []

    @classmethod
    def from_dir(cls, directory: Path, pattern: str = "rest_*.json") -> ReplayTransport:
        """Replay every cassette matching ``pattern``, in file name order."""
        exchanges: list[RecordedExchange] = []
        for path in sorted(directory.glob(pattern)):
            exchanges.extend(load_exchanges(path))
        return cls(exchanges)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        key = exchange_key(request.method, str(request.url))
        self.requests.append(key)
        queue = self._queues.get(key)
        if not queue:
            raise UnrecordedRequestError(
                f"no recording for {key[0]} {key[1]}; recorded: {sorted(self._queues)}"
            )
        exchange = queue.popleft() if len(queue) > 1 else queue[0]
        return httpx.Response(
            exchange.status,
            headers=dict(exchange.headers),
            content=exchange.body.encode("utf-8"),
            request=request,
        )


@dataclass
class ReplayConnection:
    """What one client connection to :class:`WsReplayServer` sent."""

    received: list[Any] = field(default_factory=list)


class WsReplayServer:
    """Replay recorded frames over a real WebSocket on localhost.

    The first message on a connection selects the session whose ``sent`` contains
    it. Anything unmatched is added to ``unexpected`` and the connection is closed
    with code 1008. ``plan`` gives the slice of ``frames`` per connection: the server
    closes after every planned slice but the last, then stays open like a quiet
    market. With no plan a connection receives every frame and stays open.
    """

    def __init__(
        self,
        sessions: Sequence[RecordedSession],
        *,
        plan: Sequence[slice] | None = None,
    ) -> None:
        self._sessions = tuple(sessions)
        self._plan = tuple(plan) if plan else (slice(None),)
        self._server: Server | None = None
        self.connections: list[ReplayConnection] = []
        self.unexpected: list[Any] = []
        self.url = ""

    async def __aenter__(self) -> WsReplayServer:
        self._server = await serve(self._handle, "127.0.0.1", 0)
        host, port = next(iter(self._server.sockets)).getsockname()[:2]
        self.url = f"ws://{host}:{port}/"
        return self

    async def __aexit__(self, *exc: object) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    async def _handle(self, connection: ServerConnection) -> None:
        record = ReplayConnection()
        index = len(self.connections)
        self.connections.append(record)
        with contextlib.suppress(ConnectionClosed):
            message = json.loads(await connection.recv())
            record.received.append(message)
            session = next((s for s in self._sessions if message in s.sent), None)
            if session is None:
                self.unexpected.append(message)
                await connection.close(1008, "unrecorded subscription")
                return
            for frame in session.control:
                await connection.send(frame)
            for frame in session.frames[self._plan[min(index, len(self._plan) - 1)]]:
                await connection.send(frame)
            if index < len(self._plan) - 1:
                await connection.close(1012, "replay plan: reconnect")
                return
            async for later in connection:
                record.received.append(json.loads(later))


async def record_session(
    url: str,
    sent: Sequence[Any],
    is_data: Callable[[Mapping[str, Any]], bool],
    *,
    frames: int,
    control: int,
    timeout: float = 60.0,
) -> RecordedSession:
    """Subscribe on the real network and capture ``frames`` data frames.

    At most ``control`` non-data frames are kept, because a feed may send more than
    the leading acknowledgement. Frames are stored as the exact text received.
    """
    captured: list[str] = []
    controls: list[str] = []

    async def capture() -> None:
        async with connect(url) as connection:
            for message in sent:
                await connection.send(json.dumps(message))
            async for raw in connection:
                text = raw if isinstance(raw, str) else raw.decode("utf-8")
                if is_data(json.loads(text)):
                    captured.append(text)
                    if len(captured) >= frames:
                        return
                elif len(controls) < control:
                    controls.append(text)

    with contextlib.suppress(asyncio.TimeoutError):
        await _aio.wait_for(capture(), timeout)
    return RecordedSession(url, tuple(sent), tuple(controls), tuple(captured))


def find_secrets(text: str) -> list[str]:
    """Names of the secret patterns matching ``text``. Empty means clean."""
    return [name for name, pattern in _SECRET_PATTERNS.items() if pattern.search(text)]


__all__ = [
    "ALLOWED_RESPONSE_HEADERS",
    "RecordedExchange",
    "RecordedSession",
    "RecordingTransport",
    "ReplayConnection",
    "ReplayTransport",
    "UnrecordedRequestError",
    "WsReplayServer",
    "exchange_key",
    "find_secrets",
    "load_exchanges",
    "load_session",
    "record_session",
    "save_exchanges",
    "save_session",
]
