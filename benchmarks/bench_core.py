#!/usr/bin/env python3
"""Measure the hot paths and print per-operation latency percentiles.

Usage: ``uv run python benchmarks/bench_core.py [--check benchmarks/budgets.json]``

Each operation is timed individually with ``perf_counter_ns``, one call after another
with no target rate, so a slow call delays the ones behind it and shows up in the
tail instead of being averaged away. These figures describe sequential service time.
They do not predict latency under an open arrival rate, which also includes queueing.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import sys
import tempfile
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from time import perf_counter_ns

from ismet.execution import (
    OrderManager,
    RecordKind,
    RiskConfig,
    RiskContext,
    RiskEngine,
    RiskLimits,
    SqliteJournal,
)
from ismet.marketdata import BookBuilder, BookDelta
from ismet.models import Quote, Symbol, Trade
from ismet.models.decimals import Rounding, midpoint, round_to_step
from ismet.models.orders import (
    OrderRequest,
    OrderSide,
    OrderType,
    TimeInForce,
    TradingRules,
)
from ismet.testing.simvenue import SIM_VENUE, SimVenue, sim_instrument

NOW = datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)
SYM = Symbol(venue=SIM_VENUE, ticker="ACME")
LIMITS = RiskLimits(
    max_order_quantity=Decimal(1_000_000),
    max_order_notional=Decimal(1_000_000_000),
    max_position=Decimal(1_000_000_000),
    price_collar_bps=Decimal(100),
)


def percentiles(samples_ns: list[int]) -> dict[str, float]:
    ordered = sorted(samples_ns)

    def at(q: float) -> float:
        return ordered[min(int(q * len(ordered)), len(ordered) - 1)] / 1000

    return {
        "n": len(ordered),
        "p50_us": round(at(0.50), 2),
        "p99_us": round(at(0.99), 2),
        "p999_us": round(at(0.999), 2),
        "max_us": round(ordered[-1] / 1000, 2),
        "mean_us": round(statistics.fmean(ordered) / 1000, 2),
    }


def timed(fn: Callable[[], object], count: int) -> dict[str, float]:
    samples = []
    for _ in range(count):
        started = perf_counter_ns()
        fn()
        samples.append(perf_counter_ns() - started)
    return percentiles(samples)


async def timed_async(
    fn: Callable[[int], Awaitable[object]], count: int
) -> dict[str, float]:
    samples = []
    for index in range(count):
        started = perf_counter_ns()
        await fn(index)
        samples.append(perf_counter_ns() - started)
    return percentiles(samples)


def bench_decimals() -> dict[str, dict[str, float]]:
    a, b = Decimal("100.01"), Decimal("100.03")
    step = Decimal("0.01")
    return {
        "decimal_midpoint": timed(lambda: midpoint(a, b), 100_000),
        "decimal_round_to_step": timed(
            lambda: round_to_step(Decimal("100.017"), step, Rounding.FLOOR), 100_000
        ),
    }


def bench_models() -> dict[str, dict[str, float]]:
    return {
        "model_trade_construct": timed(
            lambda: Trade(
                symbol=SYM,
                exchange_ts=NOW,
                received_ts=NOW,
                price="100.01",
                size="100",
            ),
            20_000,
        ),
    }


def bench_book() -> dict[str, dict[str, float]]:
    book = BookBuilder(SYM)
    book.apply_snapshot(
        0,
        [(Decimal(1000 - i), Decimal(10)) for i in range(500)],
        [(Decimal(1001 + i), Decimal(10)) for i in range(500)],
    )
    state = {"seq": 0}

    def update() -> None:
        state["seq"] += 1
        price = Decimal(1000 - state["seq"] % 500)
        book.apply_delta(BookDelta(OrderSide.BUY, price, Decimal(5), state["seq"]))

    return {"book_apply_delta_1000_levels": timed(update, 100_000)}


def bench_risk() -> dict[str, dict[str, float]]:
    instrument = sim_instrument("ACME")
    quote = Quote(
        symbol=SYM, exchange_ts=NOW, received_ts=NOW, bid="99.99", ask="100.01"
    )
    request = OrderRequest(
        client_order_id="B1",
        symbol=SYM,
        side=OrderSide.BUY,
        quantity="10",
        limit_price="100.00",
    )
    rules = TradingRules(
        order_types=frozenset(OrderType), time_in_force=frozenset(TimeInForce)
    )
    engine = RiskEngine(
        RiskConfig(
            default=LIMITS,
            enforce_session=False,
            duplicate_window_seconds=Decimal(0),
        )
    )
    context = RiskContext(
        request=request,
        instrument=instrument,
        reference=quote,
        now=NOW,
        position=Decimal(0),
        rules=rules,
        gross_notional=Decimal(0),
    )
    return {"risk_check_full": timed(lambda: engine.check(context), 50_000)}


def bench_journal(directory: Path) -> dict[str, dict[str, float]]:
    journal = SqliteJournal(directory / "bench.db")
    payload = json.dumps({"reason": "benchmark"})
    append = timed(lambda: journal.append(RecordKind.ANOMALY, payload), 2_000)
    kill = timed(journal.kill_state, 5_000)
    journal.close()
    return {"journal_append_fsync": append, "journal_kill_state": kill}


async def bench_submit(directory: Path) -> dict[str, dict[str, float]]:
    venue = SimVenue(instruments=[sim_instrument("ACME")])
    venue.set_quote(SYM, "99.99", "100.01")
    journal = SqliteJournal(directory / "oms.db")
    risk = RiskEngine(
        RiskConfig(
            default=LIMITS,
            duplicate_window_seconds=Decimal(0),
            prevent_self_match=False,
        )
    )
    async with OrderManager(venue, journal, risk, reconcile_interval=3600) as oms:
        await venue.drain()

        async def submit(index: int) -> None:
            await oms.submit(
                OrderRequest(
                    client_order_id=f"S{index}",
                    symbol=SYM,
                    side=OrderSide.BUY,
                    quantity="1",
                    limit_price="99.00",
                )
            )

        result = await timed_async(submit, 500)
    journal.close()
    return {"oms_submit_end_to_end_sim": result}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path, help="fail if p99 exceeds a budget file")
    parser.add_argument("--json", type=Path, help="write results to this file")
    args = parser.parse_args()
    results: dict[str, dict[str, float]] = {}
    results.update(bench_decimals())
    results.update(bench_models())
    results.update(bench_book())
    results.update(bench_risk())
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        results.update(bench_journal(Path(tmp)))
        results.update(asyncio.run(bench_submit(Path(tmp))))
    print(platform.platform(), platform.processor(), platform.python_version())
    header = ("p50 us", "p99 us", "p99.9 us", "max us")
    print(f"{'operation':34} {'n':>7}", *(f"{title:>10}" for title in header))
    for name, row in results.items():
        cells = [row[key] for key in ("p50_us", "p99_us", "p999_us", "max_us")]
        print(f"{name:34} {row['n']:>7}", *(f"{cell:>10}" for cell in cells))
    if args.json:
        args.json.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    if args.check:
        budgets = json.loads(args.check.read_text(encoding="utf-8"))
        over = {
            name: (results[name]["p99_us"], limit)
            for name, limit in budgets.items()
            if results[name]["p99_us"] > limit
        }
        for name, (actual, limit) in over.items():
            print(f"OVER BUDGET {name}: p99 {actual} us > {limit} us")
        return 1 if over else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
