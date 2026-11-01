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
from ismet.models import (
    Bar,
    Interval,
    Level,
    OrderBook,
    QualityFlag,
    Quote,
    Symbol,
    Trade,
)

D = Decimal
TS = datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)
SYM = Symbol(venue="ZSIM", ticker="ACME")
OTHER = Symbol(venue="ZSIM", ticker="OTHER")


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


# The first bucket opens at TS, and the oracle is its length: M1 closes 60s later.


def bar(interval: Interval, received: datetime) -> Bar:
    return Bar(
        symbol=SYM,
        exchange_ts=TS,
        received_ts=received,
        interval=interval,
        open="10",
        high="10",
        low="10",
        close="10",
        volume="1",
    )


def test_a_bar_is_as_old_as_its_close_not_its_open() -> None:
    monitor = QualityMonitor()
    just_closed = bar(Interval.M1, TS + timedelta(seconds=61))
    assert monitor.observe(just_closed) == []
    late = bar(Interval.M1, TS + timedelta(seconds=91))
    assert [i.issue for i in monitor.observe(late)] == [Issue.STALE]


def test_a_month_has_no_length_so_its_bar_has_no_staleness_verdict() -> None:
    bars = bar(Interval.MO1, TS + timedelta(days=400))
    assert QualityMonitor().observe(bars) == []


def test_a_real_move_is_confirmed_by_nearby_prices_that_never_repeat() -> None:
    monitor = QualityMonitor()
    assert monitor.observe(trade(1, price="100")) == []
    issues = [
        monitor.observe(trade(1, price=p)) for p in ("120.01", "120.03", "119.98")
    ]
    assert [[i.issue for i in found] for found in issues] == [[Issue.OUTLIER]] * 3
    assert monitor.observe(trade(1, price="120.10")) == []


def test_scattered_bad_ticks_never_become_the_level() -> None:
    monitor = QualityMonitor()
    monitor.observe(trade(1, price="100"))
    for price in ("1000", "5000", "20", "900", "4000", "30"):
        assert [i.issue for i in monitor.observe(trade(1, price=price))] == [
            Issue.OUTLIER
        ]
    assert monitor.observe(trade(1, price="100.5")) == []


def test_a_one_sided_quote_is_still_checked_for_an_outlier() -> None:
    monitor = QualityMonitor()
    monitor.observe(trade(1, price="100"))
    one_sided = Quote(symbol=SYM, exchange_ts=TS, received_ts=TS, bid="1000")
    assert [i.issue for i in monitor.observe(one_sided)] == [Issue.OUTLIER]


def flagged(sequence: int, *flags: QualityFlag) -> Trade:
    return trade(sequence).model_copy(update={"quality": frozenset(flags)})


def test_a_gated_guard_reads_the_flags_and_never_a_jump() -> None:
    guard = SequenceGuard(GapPolicy.RAISE, gated=True)
    first, jumped = trade(100), trade(900)
    assert guard.admit(first) is first
    assert guard.admit(jumped) is jumped
    assert guard.stats.gaps == 0
    with pytest.raises(SequenceGap) as caught:
        guard.admit(flagged(1000, QualityFlag.AFTER_GAP))
    # The adapter counted the loss by its own numbering, so nothing here can say which
    # number was expected. A figure derived from the sequence would be invented.
    assert (caught.value.expected, caught.value.received) == (1000, 1000)
    assert guard.stats.gaps == 1


def test_a_gated_guard_counts_what_the_adapter_flagged_and_drops_nothing() -> None:
    seen: list[int] = []
    guard = SequenceGuard(on_gap=lambda _, o: seen.append(o.sequence), gated=True)
    assert guard.admit(flagged(5, QualityFlag.AFTER_GAP)) is not None
    assert guard.admit(flagged(3, QualityFlag.OUT_OF_ORDER)) is not None
    assert guard.admit(trade(5)) is not None
    assert seen == [5]
    assert guard.stats.out_of_order == 1
    assert guard.stats.delivered == 3


def _bar(close: str, *flags: QualityFlag, received: datetime = TS) -> Bar:
    return Bar(
        symbol=SYM,
        exchange_ts=TS - timedelta(minutes=1),
        received_ts=received,
        interval=Interval.M1,
        open=close,
        high=close,
        low=close,
        close=close,
        volume="1",
        quality=frozenset(flags),
    )


def test_a_bar_close_far_from_the_last_one_is_an_outlier() -> None:
    monitor = QualityMonitor()
    assert monitor.observe(_bar("100")) == []
    assert [i.issue for i in monitor.observe(_bar("1000"))] == [Issue.OUTLIER]


def test_a_book_mid_far_from_the_last_price_is_an_outlier() -> None:
    monitor = QualityMonitor()
    monitor.observe(trade(1, price="100"))
    book = OrderBook(
        symbol=SYM,
        exchange_ts=TS,
        received_ts=TS,
        bids=(Level(price="999", size="1"),),
        asks=(Level(price="1001", size="1"),),
    )
    issues = monitor.observe(book)
    assert [i.issue for i in issues] == [Issue.OUTLIER]
    assert "1000" in issues[0].detail


def _book(bid: str, ask: str) -> OrderBook:
    return OrderBook(
        symbol=SYM,
        exchange_ts=TS,
        received_ts=TS,
        bids=(Level(price=bid, size="1"),),
        asks=(Level(price=ask, size="1"),),
    )


def test_a_zero_bid_prices_a_book_at_the_same_mid_as_the_equal_quote() -> None:
    monitor = QualityMonitor()
    quote = Quote(
        symbol=SYM,
        exchange_ts=TS,
        received_ts=TS,
        bid="0",
        bid_size="1",
        ask="0.05",
        ask_size="1",
    )
    assert monitor.observe(quote) == []
    assert monitor.observe(_book("0", "0.05")) == []


def test_a_move_off_a_zero_price_is_reported_and_must_be_confirmed() -> None:
    monitor = QualityMonitor()
    assert monitor.observe(_book("0", "0")) == []
    jumped = monitor.observe(trade(1, price="1000"))
    assert [i.issue for i in jumped] == [Issue.OUTLIER]
    assert "1000" in jumped[0].detail
    assert [i.issue for i in monitor.observe(trade(1, price="1"))] == [Issue.OUTLIER]


def test_a_steady_zero_price_is_not_an_outlier() -> None:
    monitor = QualityMonitor()
    assert monitor.observe(trade(1, price="0")) == []
    assert monitor.observe(trade(1, price="0")) == []


def test_a_guard_delivers_a_number_below_the_first_once() -> None:
    guard = SequenceGuard()
    guard.admit(trade(100))
    assert QualityFlag.OUT_OF_ORDER in guard.admit(trade(50)).quality  # type: ignore[union-attr]
    assert guard.admit(trade(50)) is None
    assert guard.admit(trade(50)) is None
    assert (guard.stats.out_of_order, guard.stats.duplicates_dropped) == (1, 2)


def test_a_one_sided_book_and_an_empty_book_are_handled() -> None:
    monitor = QualityMonitor()
    monitor.observe(trade(1, price="100"))
    one_sided = OrderBook(
        symbol=SYM,
        exchange_ts=TS,
        received_ts=TS,
        asks=(Level(price="1001", size="1"),),
    )
    assert [i.issue for i in monitor.observe(one_sided)] == [Issue.OUTLIER]
    empty = OrderBook(symbol=SYM, exchange_ts=TS, received_ts=TS)
    assert monitor.observe(empty) == []


def test_a_bar_the_adapter_declared_stale_is_reported_stale() -> None:
    monitor = QualityMonitor()
    issues = monitor.observe(_bar("100", QualityFlag.STALE))
    assert [i.issue for i in issues] == [Issue.STALE]
    assert issues[0].detail == "the source declared this event stale"


def test_an_event_both_old_and_declared_stale_is_reported_once() -> None:
    monitor = QualityMonitor()
    old = _bar("100", QualityFlag.STALE, received=TS + timedelta(minutes=5))
    assert [i.issue for i in monitor.observe(old)] == [Issue.STALE]
