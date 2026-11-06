from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ismet.errors import SequenceGap, ValidationError
from ismet.marketdata import BookBuilder, BookDelta
from ismet.models import QualityFlag, Symbol
from ismet.models.orders import OrderSide

D = Decimal
TS = datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)
SYM = Symbol(venue="ZSIM", ticker="ACME")
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


def _raising_rows():  # type: ignore[no-untyped-def]
    yield (D(8), D(1))
    raise RuntimeError("feed broke mid-iteration")


@pytest.mark.parametrize(
    ("bids", "error"),
    [
        (_raising_rows, RuntimeError),
        (lambda: [(D(8),)], ValidationError),
        (lambda: [(D("NaN"), D(1))], ValidationError),
        (lambda: [(D(8), D(-1))], ValidationError),
        (lambda: [(D(8), D("NaN"))], ValidationError),
    ],
)
def test_a_bad_snapshot_leaves_the_book_exactly_as_it_was(bids, error) -> None:  # type: ignore[no-untyped-def]
    book = synced()
    before = snap(book)
    with pytest.raises(error):
        book.apply_snapshot(200, bids(), [])
    assert book.is_synced
    assert book.last_sequence == 100
    assert snap(book) == before


@pytest.mark.parametrize(
    "asks",
    [_raising_rows, lambda: [(D(11),)], lambda: [(D("NaN"), D(1))]],
)
def test_bad_asks_leave_the_bids_of_the_rejected_snapshot_out(asks) -> None:  # type: ignore[no-untyped-def]
    book = synced()
    before = snap(book)
    with pytest.raises((RuntimeError, ValidationError)):
        book.apply_snapshot(200, [(D(50), D(9))], asks())
    assert book.last_sequence == 100
    assert snap(book) == before


def test_a_snapshot_sequence_that_is_not_an_int_is_a_validation_error() -> None:
    book = synced()
    with pytest.raises(ValidationError):
        book.apply_snapshot("200", [], [])  # type: ignore[arg-type]


def test_every_level_of_a_default_range_update_is_applied() -> None:
    book = BookBuilder(SYM)
    book.apply_snapshot(1, [], [])
    assert book.apply_delta(BookDelta(BUY, D(10), D(1), 2))
    assert book.apply_delta(BookDelta(BUY, D(9), D(1), 2))
    assert book.depth_counts() == (2, 0)
    assert book.stale_deltas == 0
    assert not book.apply_delta(BookDelta(BUY, D(8), D(1), 1))
    assert book.stale_deltas == 1


@pytest.mark.parametrize(
    "row", [("100.5", D(2)), (D("100.5"), "2"), (100, D(2)), (D(100), 2.5)]
)
def test_book_levels_must_be_decimal(row) -> None:  # type: ignore[no-untyped-def]
    book = synced()
    before = snap(book)
    with pytest.raises(ValidationError):
        book.apply_snapshot(200, [row], [])
    with pytest.raises(ValidationError):
        BookDelta(BUY, row[0], row[1], 101)
    assert snap(book) == before


def test_a_bad_snapshot_keeps_buffered_deltas_for_the_next_one() -> None:
    book = BookBuilder(SYM)
    book.apply_delta(BookDelta(BUY, D(10), D(5), 101))
    with pytest.raises(ValidationError):
        book.apply_snapshot(100, [(D("NaN"), D(1))], [])
    assert not book.is_synced
    assert book.apply_snapshot(100, [], [])
    assert snap(book).bids[0].price == 10


@pytest.mark.parametrize(
    "fields",
    [
        {"price": D("NaN"), "size": D(1), "sequence": 1},
        {"price": D(1), "size": D("NaN"), "sequence": 1},
        {"price": D(1), "size": D(-1), "sequence": 1},
        {"price": D(1), "size": D(1), "sequence": "1"},
        {"price": D(1), "size": D(1), "sequence": 5, "first_sequence": 6},
    ],
)
def test_an_impossible_delta_cannot_be_built(fields) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(ValidationError):
        BookDelta(BUY, **fields)


def test_a_negative_price_is_a_real_level() -> None:
    delta = BookDelta(BUY, D("-37.63"), D(1), 1)
    assert delta.price == D("-37.63")


def test_depth_below_one_is_refused_not_read_as_a_slice() -> None:
    with pytest.raises(ValidationError):
        snap(synced(), -1)
    with pytest.raises(ValidationError):
        snap(synced(), 0)


def test_a_zero_buffer_is_refused_at_construction() -> None:
    with pytest.raises(ValidationError):
        BookBuilder(SYM, max_buffered=0)


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
    assert not result.quality  # fully resynced, so nothing is wrong with this book


def test_missing_exactly_one_update_is_a_gap() -> None:
    book = synced()
    assert not book.apply_delta(BookDelta(BUY, D(10), D(1), 102))
    assert not book.is_synced and book.gaps == 1


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
    assert book.apply_snapshot(4, [], [])
    assert not book.overflowed
    assert book.is_synced


def test_redelivered_batch_cannot_roll_a_new_snapshot_back() -> None:
    book = BookBuilder(SYM)
    book.apply_snapshot(10, [(D(100), D(9))], [])
    delta = BookDelta(BUY, D(100), D(1), 15, first_sequence=11)
    assert book.apply_delta(delta)
    assert book.apply_snapshot(20, [(D(100), D(5))], [])
    assert not book.apply_delta(delta)
    assert book.last_sequence == 20
    assert snap(book).bids[0].size == D(5)


def test_levels_of_one_batch_still_apply_together() -> None:
    book = BookBuilder(SYM)
    book.apply_snapshot(10, [], [])
    assert book.apply_delta(BookDelta(BUY, D(100), D(1), 13, first_sequence=11))
    assert book.apply_delta(BookDelta(BUY, D(99), D(2), 13, first_sequence=11))
    assert [(x.price, x.size) for x in snap(book).bids] == [
        (D(100), D(1)),
        (D(99), D(2)),
    ]


def test_a_batch_cut_by_overflow_cannot_sync_from_an_older_snapshot() -> None:
    book = BookBuilder(SYM, max_buffered=2)
    for price in (D(100), D(99), D(98)):
        book.apply_delta(BookDelta(BUY, price, D(1), 13, first_sequence=11))
    assert not book.apply_snapshot(10, [], [])
    assert not book.is_synced
    assert book.overflowed
    with pytest.raises(SequenceGap):
        snap(book)
    assert book.apply_snapshot(13, [(D(100), D(1))], [])
    assert book.is_synced and not book.overflowed


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
