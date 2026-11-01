"""Binance Spot public market data. Venue MIC ``BNCX``. No trading, no keys.

Vendor documentation, retrieved 2026-11-01:

- https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md
  (endpoints, weights, HTTP codes, IP limits)
- https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
  (streams, subscribing, connection limits)
- https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md
  (``data-api.binance.vision`` and ``data-stream.binance.vision``)
- https://github.com/binance/binance-spot-api-docs/blob/master/filters.md and
  ``enums.md``, ``errors.md`` in the same repository

The market-data-only hosts are the default because ``api.binance.com`` answered
HTTP 451 from where this adapter was built; both serve the same public data.

Rate limits. ``exchangeInfo`` reported, on 2026-11-01, ``REQUEST_WEIGHT`` 6000 per
minute and ``RAW_REQUESTS`` 300000 per 5 minutes per IP. The cheapest endpoint here
weighs 2, so the weight limit always binds first and is the only one enforced.
Weights are ``exchangeInfo`` 20, ``klines`` 2, ``depth`` 5, 25, 50 or 250 by limit.
Binance counts a fixed minute, while a token bucket admits ``capacity + rate * 60``
in any 60 seconds, so the bucket is 4800 per minute with a burst of 1200, which can
never exceed 6000. Streams: 5 incoming messages per second per connection, 1024
streams per connection, 300 connections per 5 minutes per IP. This adapter sends
one message per connection. A bucket of 150 plus 0.5 per second is charged before
every handshake, accepted or not, so it bounds connection attempts as well as
connections.

Timestamps. REST ``depth`` and the ``bookTicker`` stream carry no time at all, so
``exchange_ts`` on those quotes and books is the received time: an upper bound, with
``latency`` zero by construction. Do not use it to measure staleness. ``source.feed``
names the endpoint, so this is detectable. ``/ticker/24hr`` does have a time,
``closeTime``, and was rejected: it is the end of the rolling statistics window, not
the book's time. Measured on 2026-11-01, ``ZILTRY`` returned the same live book as
``bookTicker`` with a ``closeTime`` 126 seconds old, and a ``closeTime`` over 25 days
old on a suspended pair. Stamping a live book with it would misdate the book.

Streams. Each stream call opens its own connection and resubscribes on reconnect
from ``on_connect``. Trade ids are consecutive per symbol (0 breaks in 325 live
trades), so a jump is a real loss and :class:`~ismet.normalize._common.StreamGate`
flags the next trade ``AFTER_GAP`` and logs the count. ``bookTicker`` ``u`` jumps in
normal operation, so it only orders quotes, and the first quote per symbol after a
reconnect is flagged instead. Repeats are dropped; a lower number is delivered
flagged ``OUT_OF_ORDER``. Nothing is backfilled, because neither stream can replay
and ``/api/v3/trades`` only holds recent trades. A consumer that must have every
trade should act on ``AFTER_GAP``. A frame a dropping ``ws_backpressure`` policy
discarded counts the same as a reconnect, so the next quote per symbol is flagged.
``IsmetClient`` takes its gap policy from those flags, because this adapter declares
``gates_streams``.

Outages. Reconnects never stop by default, since a long-lived stream should survive a
network outage. Each one is logged at WARNING; pass ``ws_on_state`` to watch them, or
``ws_max_reconnects`` to fail instead. Only data frames count as progress.

Vendor quirks:

- Subscribing to a stream that does not exist is acknowledged with success and then
  stays silent. Symbols are therefore checked through ``exchangeInfo`` before
  connecting, which also supplies the event currency.
- Symbols cannot be split into assets. Asset codes include ``T``, ``U`` and the
  non-ASCII ``币安人生``, which ismet codes cannot hold; see
  :func:`~ismet.normalize.binance.instrument_from_symbol_info`.
- Full ``exchangeInfo`` was 6.8 MB and 3742 symbols, 2369 of them ``BREAK``; it is
  cached for ``search_cache_seconds`` for that reason.
- HTTP 403 is a WAF block, possibly from rate limits, and surfaces as
  :class:`~ismet.errors.AuthError` by the transport's mapping even though this
  adapter sends no credentials.
- HTTP 418 is an IP ban for continuing after 429s. It is raised as
  :class:`~ismet.errors.RateLimited` and never retried, because sending more lengthens
  it. The ban lasts the longest of ``Retry-After``, the time stated in the body, and
  the 2 minutes the docs give as the shortest ban, so a host clock running fast cannot
  shorten it. A 429's ``Retry-After`` is held the same way, and requests are refused
  locally until the wait ends. A 429 whose ``Retry-After`` exceeds the retry policy's
  cap is not retried, because retrying before it elapses is what earns the ban.
- ``serverShutdown`` frames are logged; the close that follows triggers the
  reconnect. A connection is valid for 24 hours and then dropped, handled the same
  way.
"""

from __future__ import annotations

import logging
import re
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar, TypeVar

import httpx

from ismet.config import ProviderSettings
from ismet.errors import IsmetError, RateLimited, TransportError, ValidationError
from ismet.models import (
    Bar,
    Instrument,
    Interval,
    MarketEvent,
    OrderBook,
    Quote,
    Symbol,
)
from ismet.models import Trade as TradeEvent
from ismet.normalize import binance as normalize
from ismet.normalize._common import (
    StreamGate,
    check_range,
    copy_made_at,
    merge_bars,
    search_rank,
)
from ismet.providers.base import Provider, max_reconnects_option
from ismet.transport.backoff import (
    ExponentialBackoff,
    RetryPolicy,
    should_retry_after_stated_wait,
)
from ismet.transport.circuit import CircuitBreaker
from ismet.transport.clock import SYSTEM_CLOCK, Clock
from ismet.transport.http import (
    Hook,
    HttpTransport,
    RequestInfo,
    Response,
    loads_decimal,
    map_response_error,
    retry_after_seconds,
)
from ismet.transport.ratelimit import RateLimiter, RateLimitSpec
from ismet.transport.ws import (
    DEFAULT_WS_BACKOFF,
    BackpressurePolicy,
    OnState,
    WebSocketTransport,
)

logger = logging.getLogger("ismet.providers.binance")

BINANCE_VENUE = "BNCX"
REST_URL = "https://data-api.binance.vision"
WS_URL = "wss://data-stream.binance.vision/ws"

MAX_STREAMS_PER_CONNECTION = 1024
MAX_KLINES_PER_REQUEST = 1000
MAX_DEPTH = 5000
MAX_BARS = 100_000
DEFAULT_TIMEOUT = 30.0
_MS_PER_SECOND = 1000
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_BANNED_UNTIL = re.compile(r"banned until (\d+)")
_SHORTEST_BAN_SECONDS = 120.0

_WEIGHT = "weight"
_WS_CONNECT = "ws_connect"
_EXCHANGE_INFO = "exchange_info"
_KLINES = "klines"
_DEPTH_TIERS: tuple[tuple[int, str, int], ...] = (
    (100, "depth_100", 5),
    (500, "depth_500", 25),
    (1000, "depth_1000", 50),
    (5000, "depth_5000", 250),
)
ENDPOINT_WEIGHTS: Mapping[str, int] = {
    _EXCHANGE_INFO: 20,
    _KLINES: 2,
    **{key: weight for _, key, weight in _DEPTH_TIERS},
}
RATE_LIMITS: Mapping[str, RateLimitSpec] = {
    _WEIGHT: RateLimitSpec.per_minute(4800, burst=1200),
    _WS_CONNECT: RateLimitSpec(rate=0.5, capacity=150),
}

E = TypeVar("E", bound=MarketEvent)


class _WeightLimiter(RateLimiter):
    """Charge each endpoint's documented weight against one shared IP budget.

    ``HttpTransport`` takes a single token per ``rate_key``, while Binance's limit is
    one budget shared by endpoints of different weights.
    """

    def __init__(self, clock: Clock) -> None:
        super().__init__(dict(RATE_LIMITS), clock=clock)

    async def acquire(self, key: str | None = None, tokens: int = 1) -> float:
        if key is not None and key in ENDPOINT_WEIGHTS:
            return await super().acquire(_WEIGHT, tokens * ENDPOINT_WEIGHTS[key])
        return await super().acquire(key, tokens)


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, RateLimited) and exc.retry_after is None:
        return False
    return should_retry_after_stated_wait(exc)


BINANCE_RETRY = RetryPolicy(should_retry=_should_retry)


def _ceil_ms(moment: datetime) -> int:
    micros = (moment - _EPOCH) // timedelta(microseconds=1)
    return -(-micros // _MS_PER_SECOND)


def _depth_rate_key(depth: int) -> str:
    return next(key for limit, key, _ in _DEPTH_TIERS if depth <= limit)


class BinanceProvider(Provider):
    """Quotes, books, bars, streams and instruments from Binance Spot."""

    name: ClassVar[str] = "binance"
    gates_streams: ClassVar[bool] = True
    venues: ClassVar[frozenset[str]] = frozenset({BINANCE_VENUE})

    def __init__(
        self,
        *,
        rest_url: str = REST_URL,
        ws_url: str = WS_URL,
        timeout: float = DEFAULT_TIMEOUT,
        klines_per_request: int = MAX_KLINES_PER_REQUEST,
        search_cache_seconds: float = 3600.0,
        retry_policy: RetryPolicy = BINANCE_RETRY,
        hooks: list[Hook] | None = None,
        ws_backoff: ExponentialBackoff = DEFAULT_WS_BACKOFF,
        ws_max_reconnects: int | None = None,
        ws_queue_size: int = 10_000,
        ws_backpressure: BackpressurePolicy = BackpressurePolicy.BLOCK,
        ws_on_state: OnState | None = None,
        clock: Clock = SYSTEM_CLOCK,
        http_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        super().__init__(clock=clock)
        if not 1 <= klines_per_request <= MAX_KLINES_PER_REQUEST:
            raise ValidationError(
                f"klines_per_request must be 1..{MAX_KLINES_PER_REQUEST}"
            )
        self._ws_url = ws_url
        self._klines_per_request = klines_per_request
        self._search_cache_seconds = search_cache_seconds
        self._ws_backoff = ws_backoff
        self._ws_max_reconnects = ws_max_reconnects
        self._ws_queue_size = ws_queue_size
        self._ws_backpressure = ws_backpressure
        self._ws_on_state = ws_on_state
        self._limiter = _WeightLimiter(clock)
        self._http = HttpTransport(
            rest_url,
            timeout=timeout,
            retry_policy=retry_policy,
            rate_limiter=self._limiter,
            breaker=CircuitBreaker(self.name, clock=clock),
            hooks=[self._hold_when_limited, *(hooks or [])],
            clock=clock,
            transport=http_transport,
            map_error=self._map_error,
        )
        self._currencies: dict[str, str | None] = {}
        self._all_symbols: list[Mapping[str, Any]] | None = None
        self._all_symbols_at = 0.0
        self._streams: set[WebSocketTransport] = set()
        self._hold_until = 0.0

    @classmethod
    def from_settings(cls, settings: ProviderSettings) -> BinanceProvider:
        """Options: ``rest_url``, ``ws_url``, ``ws_max_reconnects``. No credentials.

        With no ``ws_max_reconnects``, a stream in an outage keeps reconnecting.
        """
        return cls(
            rest_url=settings.option("rest_url", REST_URL),
            ws_url=settings.option("ws_url", WS_URL),
            ws_max_reconnects=max_reconnects_option(settings),
        )

    async def open(self) -> None:
        await self._http.open()

    async def close(self) -> None:
        for stream in tuple(self._streams):
            await stream.close()
        await self._http.close()

    def _native(self, symbol: Symbol) -> str:
        if symbol.venue != BINANCE_VENUE:
            raise ValidationError(
                f"binance serves venue {BINANCE_VENUE} only, got {symbol.venue}"
            )
        if symbol.ticker != symbol.ticker.upper():
            raise ValidationError(
                f"binance tickers are upper case native symbols, got {symbol.ticker!r}"
            )
        return symbol.ticker

    async def _get(self, path: str, params: Mapping[str, Any], rate_key: str) -> Any:
        return (await self._request(path, params, rate_key)).json()

    async def _request(
        self, path: str, params: Mapping[str, Any], rate_key: str
    ) -> Response:
        remaining = self._hold_until - self.clock.monotonic()
        if remaining > 0:
            raise RateLimited(
                f"binance told this IP to wait another {remaining:.0f}s; not sending",
                retry_after=remaining,
            )
        return await self._http.request("GET", path, params=params, rate_key=rate_key)

    def _map_error(
        self, status: int, body: bytes, headers: Mapping[str, str]
    ) -> IsmetError:
        if status != 418:
            return map_response_error(status, body, headers)
        text = body.decode("utf-8", errors="replace")[:500]
        waits = [_SHORTEST_BAN_SECONDS, retry_after_seconds(headers) or 0.0]
        found = _BANNED_UNTIL.search(text)
        if found:
            waits.append(
                int(found.group(1)) / _MS_PER_SECOND - self.clock.now().timestamp()
            )
        return RateLimited(
            f"binance has banned this IP: HTTP 418: {text}", retry_after=max(waits)
        )

    def _hold_when_limited(self, info: RequestInfo) -> None:
        if isinstance(info.error, RateLimited) and info.error.retry_after is not None:
            self._hold_until = max(
                self._hold_until, self.clock.monotonic() + info.error.retry_after
            )

    async def _symbol_info(self, ticker: str) -> Mapping[str, Any]:
        payload = await self._get(
            "/api/v3/exchangeInfo",
            {"symbol": ticker, "showPermissionSets": "false"},
            _EXCHANGE_INFO,
        )
        info: Mapping[str, Any] = payload["symbols"][0]
        self._currencies[ticker] = normalize.quote_asset_of(info)
        return info

    async def _currency(self, ticker: str) -> str | None:
        if ticker not in self._currencies:
            await self._symbol_info(ticker)
        return self._currencies[ticker]

    # MarketDataCapability

    async def quote(self, symbol: Symbol) -> Quote:
        """Top of book from ``depth?limit=1``. ``last`` is ``None``.

        ``depth`` over ``bookTicker``: weight 5 against 2, but it has
        ``lastUpdateId``. ``exchange_ts`` is the received time; see the module
        docstring.
        """
        ticker = self._native(symbol)
        currency = await self._currency(ticker)
        payload = await self._get(
            "/api/v3/depth", {"symbol": ticker, "limit": 1}, _depth_rate_key(1)
        )
        return normalize.quote_from_depth(payload, symbol, self.clock.now(), currency)

    async def order_book(self, symbol: Symbol, depth: int = 10) -> OrderBook:
        """Snapshot of ``depth`` levels per side, 1..5000.

        ``exchange_ts`` is the received time; see the module docstring.
        """
        ticker = self._native(symbol)
        if not 1 <= depth <= MAX_DEPTH:
            raise ValidationError(f"depth must be 1..{MAX_DEPTH}, got {depth}")
        currency = await self._currency(ticker)
        payload = await self._get(
            "/api/v3/depth", {"symbol": ticker, "limit": depth}, _depth_rate_key(depth)
        )
        return normalize.order_book_from_depth(
            payload, symbol, self.clock.now(), currency
        )

    # HistoricalCapability

    async def bars(
        self, symbol: Symbol, interval: Interval, start: datetime, end: datetime
    ) -> list[Bar]:
        """Closed bars with an open time in ``[start, end)``, ascending.

        ``startTime`` and ``endTime`` are inclusive milliseconds, so the request
        asks up to the last millisecond before ``end``. Pages continue from the
        latest open time until a short page. A range that would exceed
        ``MAX_BARS`` raises :class:`ValidationError` before any request. A kline counts
        as closed by the server's ``Date``, so a fast host clock cannot pass off a
        partial one as final.
        """
        ticker = self._native(symbol)
        check_range(start, end)
        vendor_interval = normalize.KLINE_INTERVALS.get(interval)
        if vendor_interval is None:
            raise ValidationError(f"binance has no {interval.value} klines")
        seconds = interval.nominal_seconds
        if seconds is not None:
            count = (end - start) // timedelta(seconds=seconds)
            if count > MAX_BARS:
                raise ValidationError(
                    f"range would produce {count} bars (max {MAX_BARS})"
                )
        currency = await self._currency(ticker)
        cursor, last = _ceil_ms(start), _ceil_ms(end) - 1
        bars: list[Bar] = []
        while cursor <= last:
            response = await self._request(
                "/api/v3/klines",
                {
                    "symbol": ticker,
                    "interval": vendor_interval,
                    "startTime": cursor,
                    "endTime": last,
                    "limit": self._klines_per_request,
                },
                _KLINES,
            )
            rows = response.json()
            received = self.clock.now()
            bars.extend(
                normalize.bars_from_klines(
                    rows,
                    symbol,
                    interval,
                    received,
                    currency,
                    as_of=copy_made_at(response.headers, received),
                )
            )
            if len(rows) < self._klines_per_request:
                break
            following = normalize.next_kline_start(rows)
            if following <= cursor:
                raise TransportError(
                    f"binance klines did not advance past {cursor}", retryable=False
                )
            cursor = following
        return merge_bars(bars, start, end)

    # StreamingCapability

    def stream_quotes(self, symbols: Sequence[Symbol]) -> AsyncIterator[Quote]:
        """Quotes from ``<symbol>@bookTicker``. See the module docstring on gaps."""
        return self._stream(
            symbols,
            "bookTicker",
            number_field="u",
            contiguous=False,
            parse=normalize.quote_from_book_ticker,
        )

    def stream_trades(self, symbols: Sequence[Symbol]) -> AsyncIterator[TradeEvent]:
        """Trades from ``<symbol>@trade``. See the module docstring on gaps."""
        return self._stream(
            symbols,
            "trade",
            number_field="t",
            contiguous=True,
            parse=normalize.trade_from_frame,
        )

    async def _stream(
        self,
        symbols: Sequence[Symbol],
        channel: str,
        *,
        number_field: str,
        contiguous: bool,
        parse: Callable[[Mapping[str, Any], Symbol, datetime, str | None], E],
    ) -> AsyncIterator[E]:
        by_ticker = {self._native(symbol): symbol for symbol in symbols}
        if not by_ticker:
            raise ValidationError("at least one symbol is required")
        if len(by_ticker) > MAX_STREAMS_PER_CONNECTION:
            raise ValidationError(
                f"binance allows {MAX_STREAMS_PER_CONNECTION} streams per connection"
            )
        currencies = {t: await self._currency(t) for t in by_ticker}
        request = {
            "method": "SUBSCRIBE",
            "params": [f"{t.lower()}@{channel}" for t in by_ticker],
            "id": 1,
        }
        generation = 0

        def decode(raw: str | bytes) -> tuple[datetime, int, int, Any]:
            return self.clock.now(), generation, ws.stats.dropped, loads_decimal(raw)

        async def before_connect() -> None:
            await self._limiter.acquire(_WS_CONNECT)

        async def on_connect(ws: WebSocketTransport) -> None:
            nonlocal generation
            generation += 1
            await ws.send_json(request)

        ws = WebSocketTransport(
            self._ws_url,
            on_connect=on_connect,
            before_connect=before_connect,
            on_state=self._ws_on_state,
            backoff=self._ws_backoff,
            max_reconnects=self._ws_max_reconnects,
            queue_size=self._ws_queue_size,
            backpressure=self._ws_backpressure,
            decode=decode,
            is_data=lambda item: number_field in item[3],
        )
        gate = StreamGate(contiguous=contiguous)
        seen_generation = 1
        seen_dropped = 0
        newest_drop = self._ws_backpressure is BackpressurePolicy.DROP_NEWEST
        self._streams.add(ws)
        try:
            await ws.start()
            async for (
                received,
                frame_generation,
                dropped_before,
                frame,
            ) in ws.messages():
                dropped = dropped_before if newest_drop else ws.stats.dropped
                if frame_generation != seen_generation or dropped != seen_dropped:
                    seen_generation, seen_dropped = frame_generation, dropped
                    gate.mark_reconnected()
                if number_field not in frame:
                    self._control(frame)
                    continue
                symbol = by_ticker.get(frame["s"])
                if symbol is None:
                    logger.warning("frame for unsubscribed %s ignored", frame["s"])
                    continue
                event = parse(frame, symbol, received, currencies[frame["s"]])
                admitted = gate.admit(frame["s"], frame[number_field], event)
                if admitted is not None:
                    yield admitted
        finally:
            self._streams.discard(ws)
            await ws.close()

    @staticmethod
    def _control(frame: Mapping[str, Any]) -> None:
        error = normalize.control_error(frame)
        if error is not None:
            raise error
        if frame.get("e") == "serverShutdown":
            logger.warning("binance announced a server shutdown; expect a reconnect")

    # ReferenceDataCapability

    async def instrument(self, symbol: Symbol) -> Instrument:
        """Current reference data, read fresh from ``exchangeInfo``."""
        info = await self._symbol_info(self._native(symbol))
        return normalize.instrument_from_symbol_info(info, symbol)

    async def _symbols(self) -> list[Mapping[str, Any]]:
        now = self.clock.monotonic()
        if (
            self._all_symbols is None
            or now - self._all_symbols_at >= self._search_cache_seconds
        ):
            payload = await self._get(
                "/api/v3/exchangeInfo", {"showPermissionSets": "false"}, _EXCHANGE_INFO
            )
            self._all_symbols = payload["symbols"]
            self._all_symbols_at = now
        return self._all_symbols

    async def search(
        self, query: str, *, venue: str | None = None, limit: int = 20
    ) -> list[Instrument]:
        """Match ticker and asset codes, active listings first.

        Listings without a valid :class:`Instrument` are left out and counted in a
        log line, as described on ``instrument_from_symbol_info``.
        """
        if venue is not None and not self.serves(venue):
            return []
        if limit <= 0:
            return []
        needle = query.strip().upper()
        ranked: list[tuple[bool, int, str, Instrument]] = []
        skipped = 0
        for info in await self._symbols():
            ticker = info["symbol"]
            rank = search_rank(needle, ticker, info["baseAsset"], info["quoteAsset"])
            if rank is None:
                continue
            try:
                target = Symbol(venue=BINANCE_VENUE, ticker=ticker)
                instrument = normalize.instrument_from_symbol_info(info, target)
            except (ValidationError, TransportError, ValueError):
                skipped += 1
                continue
            ranked.append((not instrument.active, rank, ticker, instrument))
        if skipped:
            logger.info(
                "search %r left out %d unrepresentable listings", query, skipped
            )
        ranked.sort(key=lambda hit: hit[:3])
        return [hit[3] for hit in ranked[:limit]]


__all__ = [
    "BINANCE_RETRY",
    "BINANCE_VENUE",
    "ENDPOINT_WEIGHTS",
    "MAX_BARS",
    "MAX_DEPTH",
    "MAX_KLINES_PER_REQUEST",
    "RATE_LIMITS",
    "REST_URL",
    "WS_URL",
    "BinanceProvider",
]
