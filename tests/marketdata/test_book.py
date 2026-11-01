from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ismet.errors import SequenceGap
from ismet.marketdata import BookBuilder, BookDelta
from ismet.models import QualityFlag, Symbol
from ismet.models.orders import OrderSide

D = Decimal
TS = datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)
SYM = Symbol(venue="XSIM", ticker="ACME")
BUY, SELL = OrderSide.BUY, OrderSide.SELL


def snap(book: BookBuilder, depth: int | None = None):  # type: ignore[no-untyped-def]
    return book.snapshot(depth=depth, exchange_ts=TS, received_ts=TS)


def synced() -> BookBuilder:
    book = BookBuilder(SYM)
    book.apply_snapshot(
        100, [(D(10), D(5)), (D(9), D(7))], [(D(11), D(3)), (D(12), D(4))]
    )
    return book


def test_snapshot_orders_levels_and_limits_depth() -> None:
    result = snap(synced())
    assert [lvl.price for lvl in result.bids] == [D(10), D(9)]
    assert [lvl.price for lvl in result.asks] == [D(11), D(12)]
    assert result.sequence == 100
    shallow = snap(synced(), 1)
    assert len(shallow.bids) == len(shallow.asks) == 1
    assert synced().depth_counts() == (2, 2)


def test_deltas_insert_update_and_remove() -> None:
    book = synced()
    assert book.apply_delta(BookDelta(BUY, D("9.5"), D(1), 101))
    assert book.apply_delta(BookDelta(SELL, D(11), D(9), 102))
    assert book.apply_delta(BookDelta(BUY, D(10), D(0), 103))
    assert book.apply_delta(
        BookDelta(BUY, D("1"), D(0), 104)
    )  # removing nothing is fine
    result = snap(book)
    assert [lvl.price for lvl in result.bids] == [D("9.5"), D(9)]
    assert result.asks[0].size == 9
    assert result.sequence == 104


def test_old_deltas_are_ignored_and_counted() -> None:
    book = synced()
    assert not book.apply_delta(BookDelta(BUY, D(10), D(99), 100))
    assert book.stale_deltas == 1
    assert snap(book).bids[0].size == 5


def test_gap_makes_the_book_refuse_to_answer_until_resynced() -> None:
    book = synced()
    assert not book.apply_delta(BookDelta(BUY, D(10), D(1), 105))
    assert not book.is_synced and book.gaps == 1
    with pytest.raises(SequenceGap) as caught:
        snap(book)
    assert caught.value.expected == 101 and caught.value.received == 105
    book.apply_delta(BookDelta(SELL, D(11), D(1), 106))
    book.apply_snapshot(105, [(D(10), D(8))], [(D(11), D(8))])
    assert book.is_synced and book.last_sequence == 106
    result = snap(book)
    assert result.bids[0].size == 8  # delta 105 is covered by the snapshot
    assert result.asks[0].size == 1  # delta 106 is newer and is replayed
    assert QualityFlag.AFTER_GAP in result.quality


def test_book_before_any_snapshot_refuses_to_answer() -> None:
    book = BookBuilder(SYM)
    book.apply_delta(BookDelta(BUY, D(10), D(1), 1))
    assert not book.is_synced
    with pytest.raises(SequenceGap):
        snap(book)


def test_buffer_is_bounded_and_reports_overflow() -> None:
    book = BookBuilder(SYM, max_buffered=2)
    for sequence in range(1, 5):
        book.apply_delta(BookDelta(BUY, D(10), D(1), sequence))
    assert book.overflowed
    book.apply_snapshot(0, [], [])
    assert not book.overflowed


def test_batched_deltas_use_the_covered_range() -> None:
    book = synced()
    assert book.apply_delta(BookDelta(BUY, D(10), D(1), 110, first_sequence=95))
    assert book.is_synced and book.last_sequence == 110
    book.apply_delta(BookDelta(BUY, D(10), D(1), 120, first_sequence=115))
    assert not book.is_synced


def test_crossed_book_is_served_and_flagged() -> None:
    book = BookBuilder(SYM)
    book.apply_snapshot(1, [(D(11), D(1))], [(D(10), D(1))])
    assert QualityFlag.CROSSED in snap(book).flags


@given(
    st.lists(
        st.tuples(
            st.sampled_from([BUY, SELL]),
            st.integers(min_value=1, max_value=50),
            st.integers(min_value=0, max_value=5),
        ),
        max_size=200,
    )
)
def test_book_matches_a_naive_model(updates: list[tuple[OrderSide, int, int]]) -> None:
    book = BookBuilder(SYM)
    book.apply_snapshot(0, [], [])
    model: dict[OrderSide, dict[int, int]] = {BUY: {}, SELL: {}}
    for sequence, (side, price, size) in enumerate(updates, start=1):
        book.apply_delta(BookDelta(side, D(price), D(size), sequence))
        if size == 0:
            model[side].pop(price, None)
        else:
            model[side][price] = size
    result = snap(book)
    assert [(lvl.price, lvl.size) for lvl in result.bids] == [
        (D(p), D(s)) for p, s in sorted(model[BUY].items(), reverse=True)
    ]
    assert [(lvl.price, lvl.size) for lvl in result.asks] == [
        (D(p), D(s)) for p, s in sorted(model[SELL].items())
    ]
