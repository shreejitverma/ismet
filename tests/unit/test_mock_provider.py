from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pydantic import SecretStr

from ismet.capabilities import Capability
from ismet.config import ProviderSettings
from ismet.errors import ConfigError, NotSupported, ValidationError, VenueError
from ismet.models import Bar, Interval, Symbol
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


async def test_concurrent_streams_each_see_an_unbroken_sequence() -> None:
    """One ticker, two streams and a snapshot, with nothing lost: no gap anywhere."""
    from ismet import IsmetClient
    from ismet.marketdata.quality import GapPolicy

    async with IsmetClient([MockProvider(tick_interval=0)]) as client:
        quotes = client.stream_quotes(["ACME@XMOK"], gap_policy=GapPolicy.RAISE)
        trades = client.stream_trades(["ACME@XMOK"], gap_policy=GapPolicy.RAISE)
        seen_quotes, seen_trades = [], []
        for _ in range(5):
            seen_quotes.append(await quotes.__anext__())
            seen_trades.append(await trades.__anext__())
            await client.quote("ACME@XMOK")
        await quotes.aclose()
        await trades.aclose()
    assert [q.sequence for q in seen_quotes] == [1, 2, 3, 4, 5]
    assert [t.sequence for t in seen_trades] == [1, 2, 3, 4, 5]
    assert not any(q.flags for q in seen_quotes)
    assert not any(t.flags for t in seen_trades)


async def test_trade_ids_stay_unique_across_streams_on_one_ticker() -> None:
    provider = MockProvider(tick_interval=0)
    symbol = Symbol(venue="XMOK", ticker="ACME")
    first, second = provider.stream_trades([symbol]), provider.stream_trades([symbol])
    ids = [
        (await stream.__anext__()).trade_id  # type: ignore[attr-defined]
        for stream in (first, second, first, second)
    ]
    assert len(set(ids)) == 4


ACME = Symbol(venue="XMOK", ticker="ACME")


async def _first_prices(provider: MockProvider, count: int = 5) -> list[Decimal]:
    stream = provider.stream_quotes([ACME])
    prices = [(await stream.__anext__()).last for _ in range(count)]  # type: ignore[attr-defined]
    await stream.aclose()  # type: ignore[attr-defined]
    return prices


async def test_fresh_providers_with_one_seed_stream_the_same_prices() -> None:
    assert await _first_prices(MockProvider(tick_interval=0)) == await _first_prices(
        MockProvider(tick_interval=0)
    )


async def test_a_concurrent_stream_on_one_provider_keeps_the_seeded_prices() -> None:
    solo = await _first_prices(MockProvider(tick_interval=0))
    provider = MockProvider(tick_interval=0)
    order: list[str] = []

    async def take(name: str, stream) -> list[Decimal]:  # type: ignore[no-untyped-def]
        prices = []
        async for event in stream:
            order.append(name)
            prices.append(event.last if name != "trades" else event.price)
            if len(prices) == 5:
                break
        await stream.aclose()
        return prices

    first, _, _ = await asyncio.gather(
        take("first", provider.stream_quotes([ACME])),
        take("second", provider.stream_quotes([ACME])),
        take("trades", provider.stream_trades([ACME])),
    )
    assert order[:3] == ["first", "second", "trades"]  # They really did interleave.
    assert order[3:6] == ["first", "second", "trades"]
    assert first == solo


async def test_a_snapshot_during_a_stream_leaves_its_prices_alone() -> None:
    solo = await _first_prices(MockProvider(tick_interval=0))
    provider = MockProvider(tick_interval=0)
    stream = provider.stream_quotes([ACME])
    prices = []
    for _ in range(5):
        prices.append((await stream.__anext__()).last)  # type: ignore[attr-defined]
        await provider.quote(ACME)
        await provider.order_book(ACME, 3)
    await stream.aclose()  # type: ignore[attr-defined]
    assert prices == solo


async def test_a_restarted_stream_starts_over_from_the_seed() -> None:
    provider = MockProvider(tick_interval=0)
    assert await _first_prices(provider) == await _first_prices(provider)


async def test_trade_streams_keep_their_own_prices_too() -> None:
    async def trades(provider: MockProvider) -> list[tuple[Decimal, Decimal]]:
        stream = provider.stream_trades([ACME])
        seen = [await stream.__anext__() for _ in range(4)]  # type: ignore[attr-defined]
        await stream.aclose()  # type: ignore[attr-defined]
        return [(t.price, t.size) for t in seen]

    solo = await trades(MockProvider(tick_interval=0))
    provider = MockProvider(tick_interval=0)
    other = provider.stream_trades([ACME])
    await other.__anext__()  # type: ignore[attr-defined]
    assert await trades(provider) == solo
    await other.aclose()  # type: ignore[attr-defined]


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


async def _opens(
    provider: MockProvider, start: float, end: float, interval: Interval = Interval.M1
) -> list[float]:
    bars = await provider.bars(
        ACME,
        interval,
        EPOCH + timedelta(seconds=start),
        EPOCH + timedelta(seconds=end),
    )
    return [(b.exchange_ts - EPOCH).total_seconds() for b in bars]


async def test_bars_cover_every_open_time_in_the_range() -> None:
    p = MockProvider()
    assert await _opens(p, 0, 90) == [0, 60]
    assert await _opens(p, 0, 60) == [0]
    assert await _opens(p, 0, 60.5) == [0, 60]


async def test_bars_open_on_the_interval_grid_whatever_the_start() -> None:
    p = MockProvider()
    assert await _opens(p, 30, 90) == [60]
    assert await _opens(p, 90, 180) == [120]
    assert await _opens(p, 61, 119) == []


async def test_a_bar_is_the_same_however_the_range_is_paged() -> None:
    p = MockProvider()
    start = datetime(2026, 1, 5, 9, 30, tzinfo=UTC)

    async def rows(lo: int, hi: int) -> list[Bar]:
        return await p.bars(
            ACME,
            Interval.M1,
            start + timedelta(minutes=lo),
            start + timedelta(minutes=hi),
        )

    def prices(bars: list[Bar]) -> list[tuple[datetime, Decimal, Decimal, Decimal]]:
        return [(b.exchange_ts, b.open, b.high, b.close) for b in bars]

    whole = prices(await rows(0, 10))
    assert prices(await rows(0, 5)) + prices(await rows(5, 10)) == whole
    assert prices(await rows(3, 7)) == whole[3:7]
    assert len({row[1:] for row in whole}) > 1  # Not one bar repeated.


async def test_bar_prices_stay_near_one_level() -> None:
    bars = await MockProvider().bars(
        ACME,
        Interval.M1,
        datetime(2026, 1, 5, tzinfo=UTC),
        datetime(2026, 1, 5, 3, tzinfo=UTC),
    )
    assert max(b.high for b in bars) - min(b.low for b in bars) < 100 * TICK * 10


async def test_the_bar_limit_counts_the_bars_that_would_be_returned() -> None:
    p = MockProvider()
    start = datetime(2026, 1, 5, tzinfo=UTC)
    assert (
        len(await p.bars(ACME, Interval.S1, start, start + timedelta(seconds=100_000)))
        == 100_000
    )
    with pytest.raises(ValidationError, match="100001 bars"):
        await p.bars(
            ACME, Interval.S1, start, start + timedelta(seconds=100_000, milliseconds=1)
        )


async def test_bars_stop_at_the_last_closed_bar(sym: Symbol) -> None:
    now = datetime(2026, 1, 5, 9, 30, 40, tzinfo=UTC)
    p = MockProvider(clock=ManualClock(now))
    bars = await p.bars(
        sym, Interval.M1, now - timedelta(minutes=3), now + timedelta(hours=1)
    )
    assert [b.exchange_ts for b in bars] == [
        datetime(2026, 1, 5, 9, 28, tzinfo=UTC),
        datetime(2026, 1, 5, 9, 29, tzinfo=UTC),
    ]
    assert all(b.exchange_ts + timedelta(minutes=1) <= now for b in bars)
    assert await p.bars(sym, Interval.M1, now, now + timedelta(hours=1)) == []
