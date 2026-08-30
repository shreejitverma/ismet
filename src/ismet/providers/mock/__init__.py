"""Deterministic in-memory provider for tests, demos, and conformance runs.

Prices follow a seeded random walk in whole ticks, so every price is tick
aligned and every run with the same seed produces the same data. Venue MIC is
``XMOK``.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import random
from collections.abc import AsyncIterator, Callable, Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, ClassVar, TypeVar

from ismet.config import ENV_PREFIX, ProviderSettings
from ismet.errors import ConfigError, ValidationError, VenueError
from ismet.models import (
    AssetClass,
    Bar,
    Instrument,
    Interval,
    Level,
    OrderBook,
    Quote,
    Side,
    Symbol,
    Trade,
)
from ismet.providers.base import Provider
from ismet.transport.clock import SYSTEM_CLOCK, Clock

MOCK_VENUE = "XMOK"
TICK = Decimal("0.01")
DEFAULT_UNIVERSE: tuple[tuple[str, str], ...] = (
    ("ACME", "Acme Corporation"),
    ("GLOBEX", "Globex Industries"),
    ("INITECH", "Initech Software"),
    ("UMBRELLA", "Umbrella Holdings"),
    ("WAYNE", "Wayne Enterprises"),
)
MAX_BARS = 100_000
T = TypeVar("T")


def _as_seed(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError(value)
        return int(value)
    return int(str(value).strip())


def _as_interval(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError(value)
    seconds = float(value)
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError(value)
    return seconds


def _option(
    settings: ProviderSettings,
    key: str,
    default: T,
    cast: Callable[[Any], T],
    expected: str,
) -> T:
    value = settings.option(key, default)
    try:
        return cast(value)
    except (TypeError, ValueError):
        env = f"{ENV_PREFIX}{settings.name.upper()}_{key.upper()}"
        raise ConfigError(
            f"provider {settings.name!r} option {key!r} must be {expected}, "
            f"got {value!r}; check {env} or [provider.{settings.name}.options] "
            f"{key} in the config file"
        ) from None


class _Walk:
    """Random walk in integer ticks for one ticker."""

    def __init__(self, ticker: str, seed: int) -> None:
        digest = hashlib.sha256(f"{seed}:{ticker}".encode()).digest()
        self.rng = random.Random(int.from_bytes(digest[:8], "big"))
        self.ticks = 5_000 + int.from_bytes(digest[8:12], "big") % 45_000
        self.sequence = 0

    def step(self) -> int:
        self.ticks = max(100, self.ticks + self.rng.randint(-5, 5))
        self.sequence += 1
        return self.ticks


class MockProvider(Provider):
    """Implements market data, historical, streaming, and reference data."""

    name: ClassVar[str] = "mock"
    venues: ClassVar[frozenset[str]] = frozenset({MOCK_VENUE})

    def __init__(
        self,
        *,
        seed: int = 42,
        tick_interval: float = 0.01,
        universe: Sequence[tuple[str, str]] = DEFAULT_UNIVERSE,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        super().__init__(clock=clock)
        self.seed = seed
        self.tick_interval = tick_interval
        self._universe = {t.upper(): n for t, n in universe}
        self._walks: dict[str, _Walk] = {}
        self.opened = False

    @classmethod
    def from_settings(cls, settings: ProviderSettings) -> MockProvider:
        return cls(
            seed=_option(settings, "seed", 42, _as_seed, "an integer"),
            tick_interval=_option(
                settings,
                "tick_interval",
                0.01,
                _as_interval,
                "a finite number of seconds, zero or more",
            ),
        )

    async def open(self) -> None:
        self.opened = True

    async def close(self) -> None:
        self.opened = False

    def _check(self, symbol: Symbol) -> None:
        if symbol.venue != MOCK_VENUE:
            raise ValidationError(
                f"mock provider serves venue {MOCK_VENUE} only, got {symbol.venue}"
            )
        if symbol.ticker.upper() not in self._universe:
            raise VenueError(
                f"unknown symbol {symbol}", code="UNKNOWN_SYMBOL", payload=None
            )

    def _walk(self, symbol: Symbol) -> _Walk:
        key = symbol.ticker.upper()
        if key not in self._walks:
            self._walks[key] = _Walk(key, self.seed)
        return self._walks[key]

    def _now(self) -> datetime:
        return self.clock.now()

    # MarketDataCapability

    async def quote(self, symbol: Symbol) -> Quote:
        self._check(symbol)
        walk = self._walk(symbol)
        px = walk.step() * TICK
        now = self._now()
        return Quote(
            symbol=symbol,
            exchange_ts=now,
            received_ts=now,
            sequence=walk.sequence,
            bid=px - TICK,
            ask=px + TICK,
            bid_size=Decimal(walk.rng.randint(1, 50) * 100),
            ask_size=Decimal(walk.rng.randint(1, 50) * 100),
            last=px,
            last_size=Decimal(walk.rng.randint(1, 10) * 100),
            currency="USD",
        )

    async def order_book(self, symbol: Symbol, depth: int = 10) -> OrderBook:
        self._check(symbol)
        if depth < 1:
            raise ValidationError("depth must be >= 1")
        walk = self._walk(symbol)
        px = walk.step() * TICK
        now = self._now()
        bids = tuple(
            Level(
                price=px - TICK * (i + 1), size=Decimal(walk.rng.randint(1, 50) * 100)
            )
            for i in range(depth)
        )
        asks = tuple(
            Level(
                price=px + TICK * (i + 1), size=Decimal(walk.rng.randint(1, 50) * 100)
            )
            for i in range(depth)
        )
        return OrderBook(
            symbol=symbol,
            exchange_ts=now,
            received_ts=now,
            sequence=walk.sequence,
            bids=bids,
            asks=asks,
            currency="USD",
        )

    # HistoricalCapability

    async def bars(
        self, symbol: Symbol, interval: Interval, start: datetime, end: datetime
    ) -> list[Bar]:
        self._check(symbol)
        if start.tzinfo is None or end.tzinfo is None:
            raise ValidationError("start and end must be timezone-aware")
        if start >= end:
            raise ValidationError("start must be before end")
        seconds = interval.seconds
        if seconds is None:
            raise ValidationError(f"interval {interval.value} is not supported")
        count = int((end - start).total_seconds() // seconds)
        if count > MAX_BARS:
            raise ValidationError(f"range would produce {count} bars (max {MAX_BARS})")
        digest = hashlib.sha256(
            f"{self.seed}:{symbol.ticker.upper()}:{start.timestamp()}".encode()
        ).digest()
        rng = random.Random(int.from_bytes(digest[:8], "big"))
        ticks = 5_000 + int.from_bytes(digest[8:12], "big") % 45_000
        received = self._now()
        out: list[Bar] = []
        step = timedelta(seconds=seconds)
        for i in range(count):
            open_t = ticks
            path = [open_t]
            for _ in range(4):
                ticks = max(100, ticks + rng.randint(-20, 20))
                path.append(ticks)
            ts = start + step * i
            out.append(
                Bar(
                    symbol=symbol,
                    exchange_ts=ts,
                    received_ts=max(received, ts),
                    interval=interval,
                    open=path[0] * TICK,
                    high=max(path) * TICK,
                    low=min(path) * TICK,
                    close=path[-1] * TICK,
                    volume=Decimal(rng.randint(1, 1_000) * 100),
                    trade_count=rng.randint(1, 500),
                    currency="USD",
                )
            )
        return out

    # StreamingCapability

    async def stream_quotes(self, symbols: Sequence[Symbol]) -> AsyncIterator[Quote]:
        for symbol in symbols:
            self._check(symbol)
        while True:
            for symbol in symbols:
                yield await self.quote(symbol)
            await asyncio.sleep(self.tick_interval)

    async def stream_trades(self, symbols: Sequence[Symbol]) -> AsyncIterator[Trade]:
        for symbol in symbols:
            self._check(symbol)
        while True:
            for symbol in symbols:
                walk = self._walk(symbol)
                px = walk.step() * TICK
                now = self._now()
                yield Trade(
                    symbol=symbol,
                    exchange_ts=now,
                    received_ts=now,
                    sequence=walk.sequence,
                    price=px,
                    size=Decimal(walk.rng.randint(1, 20) * 100),
                    side=walk.rng.choice((Side.BUY, Side.SELL)),
                    trade_id=f"{symbol.ticker.upper()}-{walk.sequence}",
                    currency="USD",
                )
            await asyncio.sleep(self.tick_interval)

    # ReferenceDataCapability

    async def instrument(self, symbol: Symbol) -> Instrument:
        self._check(symbol)
        return Instrument(
            symbol=symbol,
            name=self._universe[symbol.ticker.upper()],
            asset_class=AssetClass.EQUITY,
            currency="USD",
            tick_size=TICK,
            lot_size=Decimal(1),
        )

    async def search(
        self, query: str, *, venue: str | None = None, limit: int = 20
    ) -> list[Instrument]:
        if venue is not None and venue.upper() != MOCK_VENUE:
            return []
        needle = query.strip().upper()
        hits = [
            t for t, n in self._universe.items() if needle in t or needle in n.upper()
        ]
        return [
            await self.instrument(Symbol(venue=MOCK_VENUE, ticker=t))
            for t in sorted(hits)[: max(0, limit)]
        ]


__all__ = ["DEFAULT_UNIVERSE", "MOCK_VENUE", "TICK", "MockProvider"]
