"""A blocking facade over :class:`~ismet.client.IsmetClient`.

The async client runs on its own event loop in one dedicated background thread, so
this works in a script, in a thread pool, and inside a Jupyter cell that already has
a running loop, none of which may call ``asyncio.run``. :meth:`IsmetSyncClient.close`
stops and joins the thread; the context manager calls it for you. The thread is a
daemon, so a forgotten ``close`` cannot stop the interpreter from exiting.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine, Iterable, Iterator, Sequence
from datetime import date, datetime, time, timezone
from queue import Full, Queue
from typing import Any, TypeVar

from ismet.calendars import TradingCalendar, get_calendar
from ismet.capabilities import Capability
from ismet.client import IsmetClient
from ismet.config import Settings
from ismet.errors import ConfigError, ValidationError
from ismet.export import BarSeries
from ismet.marketdata.quality import GapHandler, GapPolicy
from ismet.models import Instrument, Interval, OrderBook, Quote, Symbol, Trade
from ismet.providers import Provider, ProviderRegistry

T = TypeVar("T")
_DONE = object()


def _as_datetime(value: datetime | date | str | None, default: datetime) -> datetime:
    if value is None:
        return default
    if isinstance(value, str):
        value = (
            date.fromisoformat(value)
            if len(value) == 10
            else datetime.fromisoformat(value)
        )
    if not isinstance(value, datetime):
        value = datetime.combine(value, time.min, tzinfo=timezone.utc)
    if value.tzinfo is None:
        raise ValidationError(f"datetime {value} must be timezone-aware")
    return value


class IsmetSyncClient:
    """Blocking access to every provider. Use as a context manager."""

    def __init__(self, providers: Iterable[Provider] = ()) -> None:
        self._client = IsmetClient(providers)
        self._init()

    @classmethod
    def from_env(
        cls,
        *,
        settings: Settings | None = None,
        registry: ProviderRegistry | None = None,
    ) -> IsmetSyncClient:
        """Build providers named by ``ISMET_PROVIDERS`` or the config file."""
        instance = cls.__new__(cls)
        instance._client = IsmetClient.from_env(settings=settings, registry=registry)
        instance._init()
        return instance

    def _init(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._opened = False

    # Lifecycle

    def __enter__(self) -> IsmetSyncClient:
        self.open()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def open(self) -> None:
        """Start the background loop and open every provider."""
        if self._opened:
            return
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, name="ismet-sync", daemon=True
        )
        self._thread.start()
        try:
            self._run(self._client.open())
        except BaseException:
            self._stop_thread()
            raise
        self._opened = True

    def close(self) -> None:
        """Close providers, stop the loop, and join the thread. Idempotent."""
        if self._thread is None:
            return
        try:
            if self._opened:
                self._run(self._client.close())
        finally:
            self._opened = False
            self._stop_thread()

    def _stop_thread(self) -> None:
        loop, thread = self._loop, self._thread
        self._loop, self._thread = None, None
        if loop is None or thread is None:
            return

        async def cancel_rest() -> None:
            tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        asyncio.run_coroutine_threadsafe(cancel_rest(), loop).result()
        loop.call_soon_threadsafe(loop.stop)
        thread.join()
        loop.close()

    def _run(self, coroutine: Coroutine[Any, Any, T]) -> T:
        if self._loop is None:
            coroutine.close()
            raise ConfigError("client is not open; use it in a 'with' block")
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop).result()

    # Registry

    def capabilities(self, provider: str) -> frozenset[Capability]:
        return self._client.capabilities(provider)

    def calendar(self, venue: str) -> TradingCalendar:
        return get_calendar(venue)

    # Calls

    def quote(
        self,
        symbol: str | Symbol,
        *,
        venue: str | None = None,
        provider: str | None = None,
    ) -> Quote:
        return self._run(self._client.quote(symbol, venue=venue, provider=provider))

    def order_book(
        self,
        symbol: str | Symbol,
        *,
        venue: str | None = None,
        provider: str | None = None,
        depth: int = 10,
    ) -> OrderBook:
        return self._run(
            self._client.order_book(symbol, venue=venue, provider=provider, depth=depth)
        )

    def bars(
        self,
        symbol: str | Symbol,
        *,
        interval: Interval | str,
        start: datetime | date | str,
        end: datetime | date | str | None = None,
        venue: str | None = None,
        provider: str | None = None,
    ) -> BarSeries:
        """Bars in ``[start, end)``. ``end`` defaults to now; dates are UTC midnight."""
        now = datetime.now(timezone.utc)
        return self._run(
            self._client.bars(
                symbol,
                interval=interval,
                start=_as_datetime(start, now),
                end=_as_datetime(end, now),
                venue=venue,
                provider=provider,
            )
        )

    def instrument(
        self,
        symbol: str | Symbol,
        *,
        venue: str | None = None,
        provider: str | None = None,
    ) -> Instrument:
        return self._run(
            self._client.instrument(symbol, venue=venue, provider=provider)
        )

    def search(
        self,
        query: str,
        *,
        venue: str | None = None,
        provider: str | None = None,
        limit: int = 20,
    ) -> list[Instrument]:
        return self._run(
            self._client.search(query, venue=venue, provider=provider, limit=limit)
        )

    # Streams

    def _iterate(self, stream: Any, *, maxsize: int) -> Iterator[Any]:
        """Bridge an async iterator to this thread, with a bounded queue.

        Leaving the ``for`` loop for any reason cancels the producer.
        """
        if self._loop is None:
            raise ConfigError("client is not open; use it in a 'with' block")
        loop = self._loop
        queue: Queue[Any] = Queue(maxsize=maxsize)
        stop = threading.Event()

        def put(item: Any) -> None:
            while not stop.is_set():
                try:
                    queue.put(item, timeout=0.05)
                    return
                except Full:
                    continue

        async def pump() -> None:
            try:
                async for item in stream:
                    await asyncio.to_thread(put, item)
                    if stop.is_set():
                        return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await asyncio.to_thread(put, exc)
            finally:
                aclose = getattr(stream, "aclose", None)
                if aclose is not None:
                    await aclose()
                put(_DONE)

        future = asyncio.run_coroutine_threadsafe(pump(), loop)
        try:
            while True:
                item = queue.get()
                if item is _DONE:
                    return
                if isinstance(item, Exception):
                    raise item
                yield item
        finally:
            stop.set()
            future.cancel()

    def stream_quotes(
        self,
        symbols: Sequence[str | Symbol],
        *,
        venue: str | None = None,
        provider: str | None = None,
        gap_policy: GapPolicy = GapPolicy.REPORT,
        on_gap: GapHandler | None = None,
        buffer: int = 1000,
    ) -> Iterator[Quote]:
        """Blocking iterator of quotes. A slow consumer applies backpressure."""
        return self._iterate(
            self._client.stream_quotes(
                symbols,
                venue=venue,
                provider=provider,
                gap_policy=gap_policy,
                on_gap=on_gap,
            ),
            maxsize=buffer,
        )

    def stream_trades(
        self,
        symbols: Sequence[str | Symbol],
        *,
        venue: str | None = None,
        provider: str | None = None,
        gap_policy: GapPolicy = GapPolicy.REPORT,
        on_gap: GapHandler | None = None,
        buffer: int = 1000,
    ) -> Iterator[Trade]:
        """Blocking iterator of trades. A slow consumer applies backpressure."""
        return self._iterate(
            self._client.stream_trades(
                symbols,
                venue=venue,
                provider=provider,
                gap_policy=gap_policy,
                on_gap=on_gap,
            ),
            maxsize=buffer,
        )


__all__ = ["IsmetSyncClient"]
