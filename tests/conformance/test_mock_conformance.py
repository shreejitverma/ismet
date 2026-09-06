from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from ismet.capabilities import Capability
from ismet.models import Quote, Symbol
from ismet.providers import Provider
from ismet.providers.mock import MockProvider
from ismet.testing import assert_conformant, run_conformance

UTC = timezone.utc


async def test_mock_provider_is_conformant(sym: Symbol) -> None:
    report = await run_conformance(MockProvider(tick_interval=0), symbol=sym)
    assert_conformant(report)
    names = {(c.capability, c.name) for c in report.checks}
    assert (Capability.TRADING, "not_supported_raises") not in names
    assert (Capability.STREAMING, "stream_quotes") in names
    assert "PASS" in report.summary()


async def test_symbol_discovered_via_search_when_omitted() -> None:
    report = await run_conformance(MockProvider(tick_interval=0), search_query="wayne")
    assert report.passed, report.summary()
    assert any(c.detail == "WAYNE@XMOK" for c in report.checks)


async def test_report_fails_without_symbol_or_venues() -> None:
    class NoVenues(Provider):
        name = "novenues"

    r = await run_conformance(NoVenues())
    assert not r.passed and r.checks[0].name == "venues"

    r2 = await run_conformance(MockProvider(), search_query="zzz")
    assert not r2.passed and r2.checks[0].name == "symbol"
    with pytest.raises(AssertionError, match="FAIL"):
        assert_conformant(r2)


async def test_invariant_violations_are_reported(sym: Symbol) -> None:
    class Sloppy(Provider):
        name = "sloppy"
        venues = frozenset({"XMOK"})

        async def quote(self, symbol: Symbol) -> Quote:
            now = datetime.now(UTC)
            return Quote(
                symbol=symbol, exchange_ts=now, received_ts=now, last=Decimal("1.005")
            )

        async def order_book(self, symbol: Symbol, depth: int = 10):  # type: ignore[no-untyped-def]
            return await MockProvider().order_book(symbol, depth)

        async def instrument(self, symbol: Symbol):  # type: ignore[no-untyped-def]
            return await MockProvider().instrument(symbol)

        async def search(
            self, query: str, *, venue: str | None = None, limit: int = 20
        ):  # type: ignore[no-untyped-def]
            return await MockProvider().search(query, venue=venue, limit=limit)

        async def stream_quotes(self, symbols):  # type: ignore[no-untyped-def]
            for s in symbols:
                yield await self.quote(s)

        async def stream_trades(self, symbols):  # type: ignore[no-untyped-def]
            raise RuntimeError("stream broke")
            yield

        def require(self, capability: Capability) -> None:  # never raises
            return None

    report = await run_conformance(Sloppy(), symbol=sym, stream_messages=1)
    assert Capability.HISTORICAL not in report.capabilities
    failed = {c.name: c.detail for c in report.failures}
    assert "not aligned to tick" in failed["quote_invariants"]
    assert failed["not_supported_raises"] == "require() did not raise"
    assert "RuntimeError" in failed["stream_trades"]
    assert "not aligned" in failed["stream_quotes"]
    assert "FAIL" in report.summary()


async def test_timeouts_are_reported(sym: Symbol) -> None:
    class Slow(MockProvider):
        name = "slow"

        async def instrument(self, symbol: Symbol):  # type: ignore[no-untyped-def, override]
            import asyncio

            await asyncio.sleep(10)

    report = await run_conformance(
        Slow(tick_interval=1.0), symbol=sym, timeout=0.05, stream_messages=5
    )
    failed = {c.name: c.detail for c in report.failures}
    assert "timed out" in failed["instrument"]
    assert "within 0.05s" in failed["stream_quotes"]
