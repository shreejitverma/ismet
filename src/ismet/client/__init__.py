"""The unified client: one entry point over every registered provider."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from datetime import datetime
from types import MappingProxyType
from typing import cast

from ismet.capabilities import (
    Capability,
    HistoricalCapability,
    MarketDataCapability,
    ReferenceDataCapability,
    StreamingCapability,
)
from ismet.config import Settings
from ismet.errors import ConfigError, ValidationError
from ismet.models import Bar, Instrument, Interval, OrderBook, Quote, Symbol, Trade
from ismet.providers import Provider, ProviderRegistry


class IsmetClient:
    """Async facade that routes each call to the right provider.

    Routing: an explicit ``provider=`` wins; otherwise the venue of the symbol
    selects the unique registered provider that serves it. Zero or several
    matches is an error, never a guess.
    """

    def __init__(self, providers: Iterable[Provider] = ()) -> None:
        self._providers: dict[str, Provider] = {}
        self._opened = False
        for provider in providers:
            self.register(provider)

    @classmethod
    def from_env(
        cls,
        *,
        settings: Settings | None = None,
        registry: ProviderRegistry | None = None,
    ) -> IsmetClient:
        """Build providers named by ``ISMET_PROVIDERS`` (or the config file)."""
        resolved = settings or Settings.load()
        if not resolved.providers:
            raise ConfigError(
                "no providers configured; set ISMET_PROVIDERS (for example "
                "ISMET_PROVIDERS=mock) or 'providers' in the config file"
            )
        reg = registry or ProviderRegistry.discover()
        client = cls()
        for name in resolved.providers:
            provider_cls = reg.get(name)
            client.register(provider_cls.from_settings(resolved.for_provider(name)))
        return client

    # Lifecycle

    async def __aenter__(self) -> IsmetClient:
        await self.open()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def open(self) -> None:
        """Open every provider; on failure close the ones already opened."""
        if self._opened:
            return
        opened: list[Provider] = []
        try:
            for provider in self._providers.values():
                await provider.open()
                opened.append(provider)
        except BaseException:
            for provider in reversed(opened):
                with contextlib.suppress(Exception):
                    await provider.close()
            raise
        self._opened = True

    async def close(self) -> None:
        """Close every provider opened by :meth:`open`; no-op otherwise."""
        if not self._opened:
            return
        self._opened = False
        for provider in reversed(list(self._providers.values())):
            await provider.close()

    # Registry

    def register(self, provider: Provider) -> None:
        key = provider.name.lower()
        if key in self._providers:
            raise ConfigError(f"provider {key!r} is already registered")
        self._providers[key] = provider

    @property
    def providers(self) -> Mapping[str, Provider]:
        return MappingProxyType(self._providers)

    def provider(self, name: str) -> Provider:
        try:
            return self._providers[name.lower()]
        except KeyError:
            raise ConfigError(
                f"provider {name!r} is not registered; have {sorted(self._providers)}"
            ) from None

    def capabilities(self, provider: str) -> frozenset[Capability]:
        return self.provider(provider).capabilities()

    def resolve(
        self, venue: str | None = None, provider: str | None = None
    ) -> Provider:
        """Pick the provider for ``venue`` unless ``provider`` is given."""
        if provider is not None:
            chosen = self.provider(provider)
            if venue is not None and not chosen.serves(venue):
                raise ValidationError(
                    f"provider {chosen.name!r} does not serve venue {venue.upper()!r}"
                )
            return chosen
        if venue is None:
            raise ValidationError("either venue or provider is required")
        matches = [p for p in self._providers.values() if p.serves(venue)]
        if not matches:
            raise ValidationError(
                f"no registered provider serves venue {venue.upper()!r}; "
                f"registered: {sorted(self._providers)}"
            )
        if len(matches) > 1:
            raise ValidationError(
                f"venue {venue.upper()!r} is served by several providers "
                f"{sorted(p.name for p in matches)}; pass provider= to choose"
            )
        return matches[0]

    def _symbol(self, symbol: str | Symbol, venue: str | None) -> Symbol:
        if isinstance(symbol, Symbol):
            if venue is not None and symbol.venue != venue.upper():
                raise ValidationError(
                    f"symbol venue {symbol.venue} does not match venue= {venue.upper()}"
                )
            return symbol
        try:
            return Symbol.parse(symbol, venue=venue)
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc

    def _route(
        self,
        symbol: str | Symbol,
        venue: str | None,
        provider: str | None,
        cap: Capability,
    ) -> tuple[Symbol, Provider]:
        sym = self._symbol(symbol, venue)
        chosen = self.resolve(sym.venue, provider)
        chosen.require(cap)
        return sym, chosen

    # Market data

    async def quote(
        self,
        symbol: str | Symbol,
        *,
        venue: str | None = None,
        provider: str | None = None,
    ) -> Quote:
        sym, chosen = self._route(symbol, venue, provider, Capability.MARKET_DATA)
        return await cast(MarketDataCapability, chosen).quote(sym)

    async def order_book(
        self,
        symbol: str | Symbol,
        *,
        venue: str | None = None,
        provider: str | None = None,
        depth: int = 10,
    ) -> OrderBook:
        sym, chosen = self._route(symbol, venue, provider, Capability.MARKET_DATA)
        return await cast(MarketDataCapability, chosen).order_book(sym, depth)

    async def bars(
        self,
        symbol: str | Symbol,
        *,
        interval: Interval | str,
        start: datetime,
        end: datetime,
        venue: str | None = None,
        provider: str | None = None,
    ) -> list[Bar]:
        sym, chosen = self._route(symbol, venue, provider, Capability.HISTORICAL)
        try:
            iv = Interval(interval)
        except ValueError:
            raise ValidationError(
                f"unknown interval {interval!r}; one of {[i.value for i in Interval]}"
            ) from None
        return await cast(HistoricalCapability, chosen).bars(sym, iv, start, end)

    # Streaming

    def _route_many(
        self, symbols: Sequence[str | Symbol], venue: str | None, provider: str | None
    ) -> tuple[list[Symbol], Provider]:
        if not symbols:
            raise ValidationError("at least one symbol is required")
        syms = [self._symbol(s, venue) for s in symbols]
        venues = {s.venue for s in syms}
        if len(venues) > 1 and provider is None:
            raise ValidationError(
                f"symbols span venues {sorted(venues)}; pass provider= to stream them"
            )
        chosen = self.resolve(next(iter(venues)), provider)
        for s in syms:
            if not chosen.serves(s.venue):
                raise ValidationError(
                    f"provider {chosen.name!r} does not serve {s.venue}"
                )
        chosen.require(Capability.STREAMING)
        return syms, chosen

    async def stream_quotes(
        self,
        symbols: Sequence[str | Symbol],
        *,
        venue: str | None = None,
        provider: str | None = None,
    ) -> AsyncIterator[Quote]:
        syms, chosen = self._route_many(symbols, venue, provider)
        async for quote in cast(StreamingCapability, chosen).stream_quotes(syms):
            yield quote

    async def stream_trades(
        self,
        symbols: Sequence[str | Symbol],
        *,
        venue: str | None = None,
        provider: str | None = None,
    ) -> AsyncIterator[Trade]:
        syms, chosen = self._route_many(symbols, venue, provider)
        async for trade in cast(StreamingCapability, chosen).stream_trades(syms):
            yield trade

    # Reference data

    async def instrument(
        self,
        symbol: str | Symbol,
        *,
        venue: str | None = None,
        provider: str | None = None,
    ) -> Instrument:
        sym, chosen = self._route(symbol, venue, provider, Capability.REFERENCE_DATA)
        return await cast(ReferenceDataCapability, chosen).instrument(sym)

    async def search(
        self,
        query: str,
        *,
        venue: str | None = None,
        provider: str | None = None,
        limit: int = 20,
    ) -> list[Instrument]:
        chosen = self.resolve(venue, provider)
        chosen.require(Capability.REFERENCE_DATA)
        return await cast(ReferenceDataCapability, chosen).search(
            query, venue=venue, limit=limit
        )


__all__ = ["IsmetClient"]
