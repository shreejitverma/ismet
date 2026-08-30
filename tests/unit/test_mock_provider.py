from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pydantic import SecretStr

from ismet.capabilities import Capability
from ismet.config import ProviderSettings
from ismet.errors import ConfigError, NotSupported, ValidationError, VenueError
from ismet.models import Interval, Symbol
from ismet.providers.mock import MAX_DEPTH, TICK, MockProvider
from ismet.transport.clock import ManualClock

UTC = timezone.utc


async def test_capabilities_and_lifecycle() -> None:
    p = MockProvider()
    assert p.capabilities() == {
        Capability.MARKET_DATA,
        Capability.HISTORICAL,
        Capability.STREAMING,
        Capability.REFERENCE_DATA,
    }
    assert p.supports(Capability.MARKET_DATA) and not p.supports(Capability.TRADING)
    with pytest.raises(NotSupported):
        p.require(Capability.TRADING)
    async with p:
        assert p.opened
    assert not p.opened
    assert "mock" in repr(p) and p.serves("xmok")


async def test_deterministic_for_same_seed(sym: Symbol) -> None:
    a = [
        (q.bid, q.ask)
        for q in [await MockProvider(seed=7).quote(sym) for _ in range(3)]
    ]
    b = [
        (q.bid, q.ask)
        for q in [await MockProvider(seed=7).quote(sym) for _ in range(3)]
    ]
    c = [
        (q.bid, q.ask)
        for q in [await MockProvider(seed=8).quote(sym) for _ in range(3)]
    ]
    assert a == b and a != c


async def test_quote_and_book_are_tick_aligned(sym: Symbol) -> None:
    clock = ManualClock()
    p = MockProvider(clock=clock)
    q = await p.quote(sym)
    assert q.exchange_ts == clock.now() == q.received_ts
    assert q.bid is not None and q.ask is not None and q.last is not None
    assert q.bid % TICK == 0 and q.ask - q.bid == 2 * TICK
    assert q.sequence == 1
    book = await p.order_book(sym, depth=3)
    assert len(book.bids) == len(book.asks) == 3
    assert book.best_bid is not None and book.best_ask is not None
    assert book.best_bid.price < book.best_ask.price
    assert book.sequence == 2
    with pytest.raises(ValidationError, match="depth"):
        await p.order_book(sym, depth=0)


async def test_order_book_depth_is_capped_and_prices_stay_positive(
    sym: Symbol,
) -> None:
    p = MockProvider()
    with pytest.raises(ValidationError, match=f"depth must be <= {MAX_DEPTH}"):
        await p.order_book(sym, depth=MAX_DEPTH + 1)
    p._walk(sym).ticks = 0
    book = await p.order_book(sym, depth=MAX_DEPTH)
    assert len(book.bids) == len(book.asks) == MAX_DEPTH
    assert all(lvl.price > 0 for lvl in book.bids)
    assert book.bids[-1].price == TICK
    assert len({lvl.price for lvl in book.bids}) == MAX_DEPTH


async def test_symbol_validation(sym: Symbol) -> None:
    p = MockProvider()
    with pytest.raises(ValidationError, match="XMOK only"):
        await p.quote(Symbol(venue="XNAS", ticker="ACME"))
    with pytest.raises(VenueError) as info:
        await p.quote(Symbol(venue="XMOK", ticker="NOPE"))
    assert info.value.code == "UNKNOWN_SYMBOL"


async def test_bars_shape_and_validation(sym: Symbol) -> None:
    p = MockProvider()
    start = datetime(2026, 1, 5, 9, 30, tzinfo=UTC)
    end = start + timedelta(minutes=10)
    bars = await p.bars(sym, Interval.M1, start, end)
    assert len(bars) == 10
    assert [b.exchange_ts for b in bars] == [
        start + timedelta(minutes=i) for i in range(10)
    ]
    assert all(b.low <= b.open <= b.high and b.low <= b.close <= b.high for b in bars)
    again = await p.bars(sym, Interval.M1, start, end)
    assert [(b.exchange_ts, b.open, b.close) for b in bars] == [
        (b.exchange_ts, b.open, b.close) for b in again
    ]
    with pytest.raises(ValidationError, match="before end"):
        await p.bars(sym, Interval.M1, end, start)
    with pytest.raises(ValidationError, match="timezone-aware"):
        await p.bars(sym, Interval.M1, datetime(2026, 1, 1), end)
    with pytest.raises(ValidationError, match="not supported"):
        await p.bars(sym, Interval.MO1, start, end)
    with pytest.raises(ValidationError, match="max"):
        await p.bars(sym, Interval.S1, start, start + timedelta(days=30))


async def test_streams_and_reference_data(sym: Symbol) -> None:
    p = MockProvider(tick_interval=0)
    other = Symbol(venue="XMOK", ticker="wayne")
    quotes = []
    async for q in p.stream_quotes([sym, other]):
        quotes.append(q)
        if len(quotes) == 4:
            break
    assert [q.symbol.ticker for q in quotes] == ["ACME", "wayne", "ACME", "wayne"]
    trades = []
    async for t in MockProvider(tick_interval=0).stream_trades([sym]):
        trades.append(t)
        if len(trades) == 2:
            break
    assert trades[0].trade_id == "ACME-1" and trades[1].sequence == 2
    inst = await p.instrument(other)
    assert inst.name == "Wayne Enterprises" and inst.tick_size == TICK
    assert [i.symbol.ticker for i in await p.search("e")] == [
        "ACME",
        "GLOBEX",
        "INITECH",
        "UMBRELLA",
        "WAYNE",
    ]
    assert [i.symbol.ticker for i in await p.search("acme", limit=1)] == ["ACME"]
    assert await p.search("acme", venue="XNAS") == []
    with pytest.raises(VenueError):
        async for _ in p.stream_trades([Symbol(venue="XMOK", ticker="NOPE")]):
            pass


def test_from_settings() -> None:
    s = ProviderSettings(
        name="mock",
        credentials={"k": SecretStr("v")},
        options={"seed": "5", "tick_interval": "0.5"},
    )
    p = MockProvider.from_settings(s)
    assert p.seed == 5 and p.tick_interval == 0.5
    assert MockProvider.from_settings(ProviderSettings(name="mock")).seed == 42
    assert isinstance(Decimal(p.seed), Decimal)


@pytest.mark.parametrize(
    ("options", "seed", "tick_interval"),
    [
        ({"seed": 5.0, "tick_interval": 0}, 5, 0.0),
        ({"seed": " 7 ", "tick_interval": "0.25"}, 7, 0.25),
        ({"seed": Decimal(9), "tick_interval": Decimal("0.5")}, 9, 0.5),
    ],
)
def test_from_settings_accepts_integral_seed_and_finite_interval(
    options: dict[str, object], seed: int, tick_interval: float
) -> None:
    p = MockProvider.from_settings(ProviderSettings(name="mock", options=options))
    assert (p.seed, p.tick_interval) == (seed, tick_interval)
    assert type(p.seed) is int and type(p.tick_interval) is float


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"seed": "abc"}, "option 'seed' must be an integer, got 'abc'"),
        ({"seed": None}, "option 'seed' must be an integer, got None"),
        ({"seed": True}, "option 'seed' must be an integer, got True"),
        ({"seed": 3.7}, "option 'seed' must be an integer, got 3.7"),
        ({"seed": "3.7"}, "option 'seed' must be an integer, got '3.7'"),
        ({"tick_interval": "fast"}, "option 'tick_interval' must be a finite"),
        ({"tick_interval": "inf"}, "got 'inf'"),
        ({"tick_interval": "nan"}, "got 'nan'"),
        ({"tick_interval": float("inf")}, "got inf"),
        ({"tick_interval": -1}, "got -1"),
        ({"tick_interval": True}, "got True"),
    ],
)
def test_from_settings_rejects_bad_options_with_config_error(
    options: dict[str, object], message: str
) -> None:
    with pytest.raises(ConfigError, match=message) as info:
        MockProvider.from_settings(ProviderSettings(name="mock", options=options))
    key = next(iter(options)).upper()
    assert f"ISMET_MOCK_{key}" in str(info.value)
