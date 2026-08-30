from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ismet import IsmetClient, Symbol
from ismet.capabilities import Capability
from ismet.config import Settings
from ismet.errors import ConfigError, NotSupported, ValidationError
from ismet.models import Quote
from ismet.providers import Provider, ProviderRegistry
from ismet.providers.mock import MockProvider

UTC = timezone.utc


class QuoteOnly(Provider):
    name = "quoteonly"
    venues = frozenset({"XQOT"})

    async def quote(self, symbol: Symbol) -> Quote:
        now = datetime.now(UTC)
        return Quote(symbol=symbol, exchange_ts=now, received_ts=now, last="1")

    async def order_book(self, symbol: Symbol, depth: int = 10):  # type: ignore[no-untyped-def]
        raise AssertionError("unused")


class MockTwin(MockProvider):
    name = "twin"


class Counting(Provider):
    name = "counting"
    venues = frozenset({"XCNT"})

    def __init__(self) -> None:
        super().__init__()
        self.opens = 0
        self.closes = 0

    async def open(self) -> None:
        self.opens += 1

    async def close(self) -> None:
        self.closes += 1


class Broken(Provider):
    name = "broken"
    venues = frozenset({"XBRK"})

    async def open(self) -> None:
        raise RuntimeError("boom")


class Last(Counting):
    name = "last"
    venues = frozenset({"XLST"})


class BrokenClose(Counting):
    name = "brokenclose"
    venues = frozenset({"XBRC"})

    async def close(self) -> None:
        await super().close()
        raise RuntimeError(f"close failed {self.closes}")


async def test_open_and_close_are_idempotent_and_paired() -> None:
    counting = Counting()
    client = IsmetClient([counting])
    await client.close()
    assert counting.closes == 0
    await client.open()
    await client.open()
    assert counting.opens == 1
    await client.close()
    await client.close()
    assert counting.closes == 1
    async with client:
        assert counting.opens == 2
    assert counting.closes == 2


async def test_open_failure_closes_already_opened_providers() -> None:
    counting = Counting()
    client = IsmetClient([counting, Broken()])
    with pytest.raises(RuntimeError, match="boom"):
        await client.open()
    assert (counting.opens, counting.closes) == (1, 1)
    await client.close()
    assert counting.closes == 1
    with pytest.raises(RuntimeError, match="boom"):
        async with client:
            raise AssertionError("unreachable")
    assert (counting.opens, counting.closes) == (2, 2)


async def test_close_attempts_every_provider_and_reraises_first_error() -> None:
    first, middle, last = Counting(), BrokenClose(), Last()
    client = IsmetClient([first, middle, last])
    await client.open()
    with pytest.raises(RuntimeError, match="close failed 1"):
        await client.close()
    assert (first.closes, middle.closes, last.closes) == (1, 1, 1)
    await client.close()
    assert (first.closes, middle.closes, last.closes) == (1, 1, 1)
    with pytest.raises(RuntimeError, match="close failed 2"):
        async with client:
            pass
    assert (first.closes, middle.closes, last.closes) == (2, 2, 2)


async def test_quick_start_against_mock() -> None:
    async with IsmetClient([MockProvider()]) as client:
        q = await client.quote("ACME", venue="XMOK")
        assert q.bid is not None and q.symbol.key == ("XMOK", "ACME")
        q2 = await client.quote("ACME@xmok")
        assert q2.sequence == 2
        q3 = await client.quote(Symbol(venue="XMOK", ticker="ACME"), provider="mock")
        assert q3.sequence == 3
        book = await client.order_book("ACME@XMOK", depth=2)
        assert len(book.bids) == 2
        end = datetime(2026, 1, 5, tzinfo=UTC)
        bars = await client.bars(
            "ACME@XMOK", interval="1h", start=end - timedelta(hours=3), end=end
        )
        assert len(bars) == 3
        inst = await client.instrument("ACME", venue="XMOK")
        assert inst.name == "Acme Corporation"
        assert [i.symbol.ticker for i in await client.search("acme", venue="XMOK")] == [
            "ACME"
        ]
        n = 0
        async for _ in client.stream_quotes(["ACME", "WAYNE"], venue="XMOK"):
            n += 1
            if n == 2:
                break
        async for _ in client.stream_trades(["ACME@XMOK"], provider="mock"):
            break
        assert client.capabilities("mock") == MockProvider().capabilities()
        assert client.providers["mock"].opened
    assert not client.providers["mock"].opened


async def test_routing_errors() -> None:
    client = IsmetClient([MockProvider(), QuoteOnly()])
    with pytest.raises(ValidationError, match="no registered provider serves"):
        await client.quote("AAPL", venue="XNAS")
    with pytest.raises(ValidationError, match="either venue or provider"):
        client.resolve()
    with pytest.raises(ValidationError, match="has no venue"):
        await client.quote("ACME")
    with pytest.raises(ValidationError, match="does not serve venue"):
        await client.quote("ACME", venue="XMOK", provider="quoteonly")
    with pytest.raises(ValidationError, match="does not match venue="):
        await client.quote(Symbol(venue="XMOK", ticker="ACME"), venue="XQOT")
    with pytest.raises(ConfigError, match="not registered"):
        client.provider("nope")
    with pytest.raises(ConfigError, match="already registered"):
        client.register(MockProvider())
    with pytest.raises(ValidationError, match="unknown interval"):
        await client.bars(
            "ACME@XMOK", interval="2m", start=datetime.now(UTC), end=datetime.now(UTC)
        )
    with pytest.raises(ValidationError, match="at least one symbol"):
        async for _ in client.stream_quotes([], venue="XMOK"):
            pass
    with pytest.raises(ValidationError, match="span venues"):
        async for _ in client.stream_quotes(["ACME@XMOK", "X@XQOT"]):
            pass
    with pytest.raises(ValidationError, match="does not serve"):
        async for _ in client.stream_quotes(["ACME@XMOK", "X@XQOT"], provider="mock"):
            pass


async def test_ambiguous_venue_requires_explicit_provider() -> None:
    client = IsmetClient([MockProvider(), MockTwin()])
    with pytest.raises(ValidationError, match="several providers"):
        await client.quote("ACME@XMOK")
    assert (await client.quote("ACME@XMOK", provider="twin")).symbol.ticker == "ACME"


async def test_missing_capability_raises_not_supported() -> None:
    client = IsmetClient([QuoteOnly()])
    assert client.capabilities("quoteonly") == {Capability.MARKET_DATA}
    assert (await client.quote("X@XQOT")).last is not None
    with pytest.raises(NotSupported, match="historical"):
        await client.bars(
            "X@XQOT", interval="1d", start=datetime.now(UTC), end=datetime.now(UTC)
        )
    with pytest.raises(NotSupported, match="reference_data"):
        await client.search("x", venue="XQOT")
    with pytest.raises(NotSupported, match="streaming"):
        async for _ in client.stream_trades(["X@XQOT"]):
            pass


async def test_from_env_builds_configured_providers() -> None:
    settings = Settings.load(
        env={"ISMET_PROVIDERS": "mock", "ISMET_MOCK_SEED": "9"}, use_file=False
    )
    client = IsmetClient.from_env(
        settings=settings, registry=ProviderRegistry.discover()
    )
    mock = client.providers["mock"]
    assert isinstance(mock, MockProvider) and mock.seed == 9
    with pytest.raises(ConfigError, match="no providers configured"):
        IsmetClient.from_env(settings=Settings.load(env={}, use_file=False))
    with pytest.raises(ConfigError, match="unknown provider"):
        IsmetClient.from_env(
            settings=Settings.load(env={"ISMET_PROVIDERS": "ghost"}, use_file=False),
            registry=ProviderRegistry(),
        )
