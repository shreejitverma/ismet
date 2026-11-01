from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from ismet.execution import (
    Journal,
    KillState,
    OrderManager,
    RiskConfig,
    RiskEngine,
    RiskLimits,
    SqliteJournal,
)
from ismet.models import Symbol
from ismet.models.orders import OrderRequest, OrderSide, OrderType
from ismet.testing.simvenue import SIM_VENUE, SimVenue, sim_instrument
from ismet.transport.clock import ManualClock

ACME = Symbol(venue=SIM_VENUE, ticker="ACME")
START = datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)


async def until(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    """Wait for ``condition``, failing the test instead of hanging CI."""

    async def poll() -> None:
        while not condition():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(poll(), timeout)


def count_kill_polls(journal: Journal) -> dict[str, int]:
    """Count kill switch reads, so a test can tell that polls actually ran."""
    counts = {"reads": 0}
    read = journal.kill_state

    def counted() -> KillState:
        counts["reads"] += 1
        return read()

    journal.kill_state = counted  # type: ignore[method-assign]
    return counts


def permissive_config(**overrides: object) -> RiskConfig:
    fields: dict[str, object] = {
        "default": RiskLimits(
            max_order_quantity=Decimal(1_000_000),
            max_order_notional=Decimal(100_000_000),
            max_position=Decimal(10_000_000),
            max_open_orders=10_000,
        ),
        "duplicate_window_seconds": Decimal(0),
        "allow_short": True,
        "prevent_self_match": False,
    }
    fields.update(overrides)
    return RiskConfig.model_validate(fields)


_sequence = {"n": 0}


def order(
    cid: str | None = None,
    *,
    side: OrderSide = OrderSide.BUY,
    quantity: str = "100",
    price: str | None = "10.00",
    symbol: Symbol = ACME,
    **kw: object,
) -> OrderRequest:
    _sequence["n"] += 1
    fields: dict[str, object] = {
        "client_order_id": cid or f"C{_sequence['n']}",
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
    }
    if price is None:
        fields["order_type"] = OrderType.MARKET
    else:
        fields["limit_price"] = price
    fields.update(kw)
    return OrderRequest.model_validate(fields)


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(START)


@pytest.fixture
def venue(clock: ManualClock) -> SimVenue:
    sim = SimVenue(clock=clock, instruments=[sim_instrument("ACME")])
    sim.set_quote(ACME, "9.99", "10.01")
    return sim


@pytest.fixture
def journal_path(tmp_path: Path) -> Path:
    return tmp_path / "journal.db"


@pytest.fixture
async def journal(journal_path: Path) -> AsyncIterator[Journal]:
    instance = SqliteJournal(journal_path)
    yield instance
    instance.close()


Factory = Callable[..., OrderManager]


@pytest.fixture
def make_manager(venue: SimVenue, journal: Journal, clock: ManualClock) -> Factory:
    def build(**kw: object) -> OrderManager:
        options: dict[str, object] = {
            "clock": clock,
            "unknown_grace": 0.0,
            "reconcile_interval": 3600.0,
            "kill_poll_interval": 0.01,
            "stream_settle": None,
        }
        risk = kw.pop("risk", None) or RiskEngine(permissive_config())
        options.update(kw)
        return OrderManager(venue, journal, risk, **options)  # type: ignore[arg-type]

    return build


@pytest.fixture
async def oms(make_manager: Factory) -> AsyncIterator[OrderManager]:
    manager = make_manager()
    await manager.start()
    yield manager
    await manager.abort()
