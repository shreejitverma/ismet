"""Coinbase Exchange public market data. Venue code ``ZCBS``. No trading, no keys.

Vendor documentation, retrieved 2026-11-01:

- https://docs.cdp.coinbase.com/exchange/rest-api/rate-limits
- https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-single-product
- https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-book
- https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles
- https://docs.cdp.coinbase.com/exchange/websocket-feed/overview
- https://docs.cdp.coinbase.com/exchange/websocket-feed/channels
- https://docs.cdp.coinbase.com/exchange/websocket-feed/rate-limits

Rate limits. Public REST: 10 requests per second per IP, bursts to 15. Feed: 8
requests per second per IP, bursts to 20, and 10 subscriptions per product per
channel, so at most 10 concurrent stream calls per product here. Each is declared in
``RATE_LIMITS``. The feed bucket is charged before every handshake, accepted or not. A
rate limit that states a wait longer than the retry policy's cap is not retried,
because a retry at the cap would come before the time the venue stated.

Timestamps. ``time`` on ``/book`` and on feed frames has up to nanosecond digits,
kept in ``exchange_ts_ns``. REST market data is served through a CDN: responses said
``cache-control: public, max-age=2`` with ``cf-cache-status: HIT``, so ``/book`` can
be about 2 seconds old on arrival. ``time`` tells the truth about that, and
``latency`` shows it. Candles have no such time and ``max-age=300``, so
:meth:`CoinbaseProvider.bars` judges them by when the copy was made, ``Date`` minus
``Age``. It also means ``sequence`` can go backwards between REST calls:
measured live, a ``/book?level=2`` response carried a lower sequence than the
``level=1`` response fetched just before it. Use the feed for anything fresher.

Streams. Each stream call opens its own connection and resubscribes on reconnect from
``on_connect``. The documented rule that ``sequence`` rises by exactly one per message
holds only for the unfiltered feed: across 42 consecutive live ``ticker`` frames no
step was 1, ``matches`` jumps likewise, and the docs say ``matches`` can drop messages.
``sequence`` is therefore delivered but not used to find loss. For trades, gaps are
found with ``trade_id``, consecutive per product in the same capture: a jump flags the
next trade ``AFTER_GAP`` and logs the count. Quotes come from ``ticker``, which batches
cascades, so nothing can show loss there, and the first quote per product after a
reconnect is flagged. A frame a dropping ``ws_backpressure`` policy discarded counts
the same as a reconnect. Repeats are dropped; a lower number is delivered flagged
``OUT_OF_ORDER``. Nothing is backfilled, though the docs suggest REST for it: this
adapter reports the gap and a consumer that needs every trade should act on it.
``IsmetClient`` takes its gap policy from those flags, because this adapter declares
``gates_streams``.

Outages. Reconnects never stop by default, since a long-lived stream should survive a
network outage. Each one is logged at WARNING; pass ``ws_on_state`` to watch them, or
``ws_max_reconnects`` to fail instead. Only data frames count as progress.

Vendor quirks:

- ``matches`` opens with one ``last_match``, an older trade. On first connect it only
  sets the position. After a reconnect it passes through the gate: the same trade is
  dropped as a repeat, a newer one is delivered, flagged if trades were missed.
- ``side`` means the maker's side on ``matches`` and ``/trades`` but the taker's on
  ``ticker``. Verified live: ``ticker`` ``buy`` printed at the best ask, ``matches``
  ``buy`` at the best bid. :attr:`Trade.side` is always the aggressor.
- ``ticker`` fires only on matches, so stream quotes do not move between trades.
- Candles are ``[time, low, high, open, close, volume]`` newest first as JSON numbers.
  ``end`` is inclusive of a bucket opening at it. The 300 limit is on the range divided
  by granularity, so a range of exactly 300 returns 301 rows; the docs add that rows
  may start before ``start``. Requests here ask for 300 aligned rows and results are
  filtered to ``[start, end)``.
- ``/book?level=2`` has no depth parameter and returned the whole book, 40000 levels
  and 1.2 MB, whatever ``depth`` is requested. ``depth=1`` uses ``level=1``. Level 3
  is not used because the docs warn against polling it and it is not aggregated.
- Errors have no code, only ``{"message": ...}``, so
  :attr:`VenueError.code` is ``None`` and ``payload`` has the text. A rejected feed
  subscription sends ``{"type": "error"}`` and then an empty ``subscriptions``.
- Unknown product ids in the feed are rejected, unlike Binance, so no reference data
  request is needed before connecting. A quote asset is only registered once the venue
  has answered for the product, so a mistyped ticker adds no currency.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar, TypeVar
from urllib.parse import quote as url_quote

import httpx

from ismet.config import ProviderSettings
from ismet.errors import TransportError, ValidationError
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
from ismet.normalize import coinbase as normalize
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
from ismet.transport.http import Hook, HttpTransport, loads_decimal
from ismet.transport.ratelimit import RateLimiter, RateLimitSpec
from ismet.transport.ws import (
    DEFAULT_WS_BACKOFF,
    BackpressurePolicy,
    OnState,
    WebSocketTransport,
)

logger = logging.getLogger("ismet.providers.coinbase")

COINBASE_VENUE = "ZCBS"
REST_URL = "https://api.exchange.coinbase.com"
WS_URL = "wss://ws-feed.exchange.coinbase.com"

MAX_CANDLES_PER_REQUEST = 300
MAX_BARS = 100_000
DEFAULT_TIMEOUT = 30.0

_PUBLIC = "public"
_WS_SUBSCRIBE = "ws_subscribe"
RATE_LIMITS: Mapping[str, RateLimitSpec] = {
    _PUBLIC: RateLimitSpec(rate=10.0, capacity=15),
    _WS_SUBSCRIBE: RateLimitSpec(rate=8.0, capacity=20),
}

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_ONE_SECOND = timedelta(seconds=1)

COINBASE_RETRY = RetryPolicy(should_retry=should_retry_after_stated_wait)

E = TypeVar("E", bound=MarketEvent)


def _ceil_seconds(moment: datetime, granularity: int) -> int:
    micros = (moment - _EPOCH) // timedelta(microseconds=1)
    step = granularity * 1_000_000
    return -(-micros // step) * granularity


def _iso(seconds: int) -> str:
    return (_EPOCH + seconds * _ONE_SECOND).strftime("%Y-%m-%dT%H:%M:%SZ")


def _outdated(
    as_of: datetime,
    received: datetime,
    first_open: int,
    last_open: int,
    granularity: int,
) -> bool:
    """Whether a bucket in the window has closed since this copy was made."""
    closed_by_now = (received - _EPOCH) // timedelta(seconds=granularity) * granularity
    closed_through = min(last_open + granularity, closed_by_now)
    if closed_through < first_open + granularity:
        return False
    return as_of < _EPOCH + closed_through * _ONE_SECOND


class CoinbaseProvider(Provider):
    """Quotes, books, bars, streams and instruments from Coinbase Exchange."""

    name: ClassVar[str] = "coinbase"
    gates_streams: ClassVar[bool] = True
    venues: ClassVar[frozenset[str]] = frozenset({COINBASE_VENUE})

    def __init__(
        self,
        *,
        rest_url: str = REST_URL,
        ws_url: str = WS_URL,
        timeout: float = DEFAULT_TIMEOUT,
        candles_per_request: int = MAX_CANDLES_PER_REQUEST,
        search_cache_seconds: float = 3600.0,
        retry_policy: RetryPolicy = COINBASE_RETRY,
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
        if not 1 <= candles_per_request <= MAX_CANDLES_PER_REQUEST:
            raise ValidationError(
                f"candles_per_request must be 1..{MAX_CANDLES_PER_REQUEST}"
            )
        self._ws_url = ws_url
        self._candles_per_request = candles_per_request
        self._search_cache_seconds = search_cache_seconds
        self._ws_backoff = ws_backoff
        self._ws_max_reconnects = ws_max_reconnects
        self._ws_queue_size = ws_queue_size
        self._ws_backpressure = ws_backpressure
        self._ws_on_state = ws_on_state
        self._limiter = RateLimiter(dict(RATE_LIMITS), clock=clock)
        self._http = HttpTransport(
            rest_url,
            timeout=timeout,
            retry_policy=retry_policy,
            rate_limiter=self._limiter,
            breaker=CircuitBreaker(self.name, clock=clock),
            hooks=hooks,
            clock=clock,
            transport=http_transport,
        )
        self._all_products: list[Mapping[str, Any]] | None = None
        self._all_products_at = 0.0
        self._streams: set[WebSocketTransport] = set()

    @classmethod
    def from_settings(cls, settings: ProviderSettings) -> CoinbaseProvider:
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
        if symbol.venue != COINBASE_VENUE:
            raise ValidationError(
                f"coinbase serves venue {COINBASE_VENUE} only, got {symbol.venue}"
            )
        if symbol.ticker != symbol.ticker.upper():
            raise ValidationError(
                f"coinbase tickers are upper case product ids, got {symbol.ticker!r}"
            )
        return symbol.ticker

    def _path(self, ticker: str, *parts: str) -> str:
        return "/".join(("/products", url_quote(ticker, safe=""), *parts))

    async def _get(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        return await self._http.get_json(path, params=params, rate_key=_PUBLIC)

    # MarketDataCapability

    async def quote(self, symbol: Symbol) -> Quote:
        """Top of book with its own time and sequence. ``last`` is ``None``."""
        ticker = self._native(symbol)
        payload = await self._get(self._path(ticker, "book"), {"level": 1})
        return normalize.quote_from_book(
            payload, symbol, self.clock.now(), normalize.quote_currency_of(ticker)
        )

    async def order_book(self, symbol: Symbol, depth: int = 10) -> OrderBook:
        """The best ``depth`` levels per side.

        Coinbase has no depth parameter, so any ``depth`` above 1 downloads the
        whole aggregated book, over 1 MB on BTC-USD.
        """
        ticker = self._native(symbol)
        if depth < 1:
            raise ValidationError(f"depth must be >= 1, got {depth}")
        level = 1 if depth == 1 else 2
        payload = await self._get(self._path(ticker, "book"), {"level": level})
        return normalize.order_book_from_book(
            payload,
            symbol,
            self.clock.now(),
            depth,
            normalize.quote_currency_of(ticker),
        )

    # HistoricalCapability

    async def bars(
        self, symbol: Symbol, interval: Interval, start: datetime, end: datetime
    ) -> list[Bar]:
        """Closed bars with an open time in ``[start, end)``, ascending.

        Buckets open on multiples of the granularity, so windows run between aligned
        bucket opens, inclusive at both ends: an unaligned window could fall between
        two buckets and silently lose one. An empty window is normal, as Coinbase
        publishes no candle without trades, so every window is requested. A range
        that would exceed ``MAX_BARS`` raises before any request.

        Responses come through a CDN and can be minutes old, so a bucket counts as
        closed only by the time the copy was made. A copy too old to show every bucket
        that has closed by now cannot tell a missing bucket from one with no trades, so
        its bars carry :attr:`QualityFlag.STALE`, and the same request can be repeated
        once the copy expires. Such a copy with no bars for a window has nothing to
        carry the flag, and would read as no trades, so it raises a retryable
        :class:`~ismet.errors.TransportError` instead.
        """
        ticker = self._native(symbol)
        check_range(start, end)
        granularity = normalize.CANDLE_GRANULARITY.get(interval)
        if granularity is None:
            raise ValidationError(f"coinbase has no {interval.value} candles")
        count = (end - start) // timedelta(seconds=granularity)
        if count > MAX_BARS:
            raise ValidationError(f"range would produce {count} bars (max {MAX_BARS})")
        cursor = _ceil_seconds(start, granularity)
        final = _ceil_seconds(end, granularity) - granularity
        path = self._path(ticker, "candles")
        bars: list[Bar] = []
        while cursor <= final:
            window_end = min(
                cursor + (self._candles_per_request - 1) * granularity, final
            )
            response = await self._http.request(
                "GET",
                path,
                params={
                    "granularity": granularity,
                    "start": _iso(cursor),
                    "end": _iso(window_end),
                },
                rate_key=_PUBLIC,
            )
            received = self.clock.now()
            as_of = copy_made_at(response.headers, received)
            outdated = _outdated(as_of, received, cursor, window_end, granularity)
            page = normalize.bars_from_candles(
                response.json(),
                symbol,
                interval,
                received,
                normalize.quote_currency_of(ticker),
                as_of=as_of,
                outdated=outdated,
            )
            if outdated and not page:
                raise TransportError(
                    f"coinbase served a copy made at {as_of.isoformat()}, which "
                    f"predates the window from {_iso(cursor)} to {_iso(window_end)}; "
                    "repeat the request once the copy expires",
                    retryable=True,
                )
            bars.extend(page)
            cursor = window_end + granularity
        return merge_bars(bars, start, end)

    # StreamingCapability

    def stream_quotes(self, symbols: Sequence[Symbol]) -> AsyncIterator[Quote]:
        """Quotes from the ``ticker`` channel. See the module docstring on gaps."""
        return self._stream(
            symbols,
            "ticker",
            number_field="sequence",
            contiguous=False,
            data_types=frozenset({"ticker"}),
            parse=normalize.quote_from_ticker,
        )

    def stream_trades(self, symbols: Sequence[Symbol]) -> AsyncIterator[TradeEvent]:
        """Trades from the ``matches`` channel. See the module docstring on gaps."""
        return self._stream(
            symbols,
            "matches",
            number_field="trade_id",
            contiguous=True,
            data_types=frozenset({"match", "last_match"}),
            parse=normalize.trade_from_match,
        )

    async def _stream(
        self,
        symbols: Sequence[Symbol],
        channel: str,
        *,
        number_field: str,
        contiguous: bool,
        data_types: frozenset[str],
        parse: Callable[[Mapping[str, Any], Symbol, datetime, str | None], E],
    ) -> AsyncIterator[E]:
        by_ticker = {self._native(symbol): symbol for symbol in symbols}
        if not by_ticker:
            raise ValidationError("at least one symbol is required")
        currencies: dict[str, str | None] = {}
        request = {
            "type": "subscribe",
            "product_ids": list(by_ticker),
            "channels": [channel],
        }
        generation = 0

        def decode(raw: str | bytes) -> tuple[datetime, int, int, Any]:
            return self.clock.now(), generation, ws.stats.dropped, loads_decimal(raw)

        async def before_connect() -> None:
            await self._limiter.acquire(_WS_SUBSCRIBE)

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
            is_data=lambda item: item[3].get("type") in data_types,
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
                kind = frame.get("type")
                if kind not in data_types:
                    error = normalize.control_error(frame)
                    if error is not None:
                        raise error
                    continue
                ticker = frame["product_id"]
                symbol = by_ticker.get(ticker)
                if symbol is None:
                    logger.warning("frame for unsubscribed %s ignored", ticker)
                    continue
                if ticker not in currencies:
                    currencies[ticker] = normalize.quote_currency_of(ticker)
                event = parse(frame, symbol, received, currencies[ticker])
                if kind == "last_match" and gate.is_fresh(ticker):
                    gate.seed(ticker, frame[number_field])
                    continue
                admitted = gate.admit(ticker, frame[number_field], event)
                if admitted is not None:
                    yield admitted
        finally:
            self._streams.discard(ws)
            await ws.close()

    # ReferenceDataCapability

    async def instrument(self, symbol: Symbol) -> Instrument:
        """Current reference data from ``GET /products/{id}``."""
        ticker = self._native(symbol)
        payload = await self._get(self._path(ticker))
        return normalize.instrument_from_product(payload, symbol)

    async def _products(self) -> list[Mapping[str, Any]]:
        now = self.clock.monotonic()
        if (
            self._all_products is None
            or now - self._all_products_at >= self._search_cache_seconds
        ):
            self._all_products = await self._get("/products")
            self._all_products_at = now
        return self._all_products

    async def search(
        self, query: str, *, venue: str | None = None, limit: int = 20
    ) -> list[Instrument]:
        """Match product id and asset codes, active listings first.

        Listings without a valid :class:`Instrument` are left out and counted in a
        log line.
        """
        if venue is not None and not self.serves(venue):
            return []
        if limit <= 0:
            return []
        needle = query.strip().upper()
        ranked: list[tuple[bool, int, str, Instrument]] = []
        skipped = 0
        for product in await self._products():
            ticker = product["id"]
            rank = search_rank(
                needle, ticker, product["base_currency"], product["quote_currency"]
            )
            if rank is None:
                continue
            try:
                target = Symbol(venue=COINBASE_VENUE, ticker=ticker)
                instrument = normalize.instrument_from_product(product, target)
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
    "COINBASE_RETRY",
    "COINBASE_VENUE",
    "MAX_BARS",
    "MAX_CANDLES_PER_REQUEST",
    "RATE_LIMITS",
    "REST_URL",
    "WS_URL",
    "CoinbaseProvider",
]
