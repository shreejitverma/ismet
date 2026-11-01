from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from ismet.errors import SequenceGap
from ismet.marketdata import (
    GapPolicy,
    Issue,
    QualityMonitor,
    SequenceGuard,
)
from ismet.models import QualityFlag, Quote, Symbol, Trade

D = Decimal
TS = datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)
SYM = Symbol(venue="XSIM", ticker="ACME")
OTHER = Symbol(venue="XSIM", ticker="OTHER")


def trade(sequence: int | None, symbol: Symbol = SYM, price: str = "10") -> Trade:
    return Trade(
        symbol=symbol,
        exchange_ts=TS,
        received_ts=TS,
        sequence=sequence,
        price=price,
        size="1",
    )


def test_guard_passes_in_order_events_untouched() -> None:
    guard = SequenceGuard()
    first, second = trade(1), trade(2)
    assert guard.admit(first) is first and guard.admit(second) is second
    assert guard.stats.delivered == 2 and guard.stats.gaps == 0


def test_guard_passes_unsequenced_events() -> None:
    guard = SequenceGuard()
    event = trade(None)
    assert guard.admit(event) is event and guard.admit(event) is event


def test_guard_flags_gaps_and_reports_them() -> None:
    seen = []
    guard = SequenceGuard(on_gap=lambda s, o: seen.append((s, o.missing)))
    guard.admit(trade(1))
    after = guard.admit(trade(5))
    assert after is not None and QualityFlag.AFTER_GAP in after.quality
    assert guard.stats.gaps == 1 and guard.stats.missing == 3
    assert seen == [(SYM, 3)]


def test_guard_can_treat_a_gap_as_fatal() -> None:
    guard = SequenceGuard(GapPolicy.RAISE)
    guard.admit(trade(1))
    with pytest.raises(SequenceGap) as caught:
        guard.admit(trade(3))
    assert caught.value.expected == 2 and caught.value.received == 3


def test_guard_drops_duplicates_and_flags_late_events() -> None:
    guard = SequenceGuard()
    guard.admit(trade(10))
    assert guard.admit(trade(10)) is None
    assert guard.stats.duplicates_dropped == 1
    guard.admit(trade(12))
    late = guard.admit(trade(11))
    assert late is not None and QualityFlag.OUT_OF_ORDER in late.quality
    assert guard.stats.out_of_order == 1


def test_guard_tracks_each_symbol_separately_and_can_reset() -> None:
    guard = SequenceGuard()
    guard.admit(trade(100, SYM))
    guard.admit(trade(1, OTHER))
    assert guard.stats.gaps == 0
    guard.reset(SYM)
    guard.admit(trade(1, SYM))
    assert guard.stats.gaps == 0 and guard.stats.out_of_order == 0
    guard.reset()
    guard.admit(trade(50, SYM))
    assert guard.stats.gaps == 0


def test_monitor_flags_outliers_without_changing_data() -> None:
    monitor = QualityMonitor()
    assert monitor.observe(trade(1, price="100")) == []
    assert monitor.observe(trade(2, price="101")) == []
    issues = monitor.observe(trade(3, price="150"))
    assert [i.issue for i in issues] == [Issue.OUTLIER]


def test_monitor_flags_skew_and_crossed_markets() -> None:
    monitor = QualityMonitor()
    skewed = Trade(
        symbol=SYM,
        exchange_ts=TS + timedelta(seconds=5),
        received_ts=TS,
        price="10",
        size="1",
    )
    assert [i.issue for i in monitor.observe(skewed)] == [Issue.CLOCK_SKEW]
    crossed = Quote(symbol=SYM, exchange_ts=TS, received_ts=TS, bid="11", ask="10")
    assert Issue.CROSSED in [i.issue for i in monitor.observe(crossed)]
    no_price = Quote(symbol=OTHER, exchange_ts=TS, received_ts=TS)
    assert monitor.observe(no_price) == []


def test_monitor_finds_stale_symbols() -> None:
    monitor = QualityMonitor()
    monitor.observe(trade(1))
    assert monitor.stale_symbols(TS + timedelta(seconds=10)) == []
    stale = monitor.stale_symbols(TS + timedelta(seconds=31))
    assert [i.issue for i in stale] == [Issue.STALE]


def test_monitor_compares_sources() -> None:
    monitor = QualityMonitor()
    assert monitor.compare_sources(SYM, D(100), D("100.5")) is None
    far = monitor.compare_sources(SYM, D(100), D(110))
    assert far is not None and far.issue is Issue.SOURCE_DIVERGENCE
    assert monitor.compare_sources(SYM, D(1), D(0)) is not None
