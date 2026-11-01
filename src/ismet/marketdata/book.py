"""Build a full order book from a snapshot and sequenced deltas.

A book built from deltas is only right if no delta was missed. A missed delta
corrupts it silently, and the result looks like a normal book, so
:class:`BookBuilder` refuses to serve data once it knows it may be wrong. After a
gap, :attr:`BookBuilder.is_synced` is false and :meth:`snapshot` raises until a new
snapshot arrives. Deltas that arrive during that time are buffered and replayed over
the new snapshot, the sequence most venue specifications prescribe.
"""

from __future__ import annotations

from bisect import bisect_left, insort
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from ismet.errors import SequenceGap
from ismet.models import Level, OrderBook, Symbol
from ismet.models.decimals import neg
from ismet.models.orders import OrderSide


@dataclass(frozen=True)
class BookDelta:
    """Set a price level to an absolute size. A size of zero removes the level.

    ``first_sequence`` to ``sequence`` is the range this delta covers, for venues
    that batch updates (such as Binance's ``U`` and ``u``). Every level of one venue
    update carries the same range. Single-update feeds use the default, where the
    range is one number.
    """

    side: OrderSide
    price: Decimal
    size: Decimal
    sequence: int
    first_sequence: int | None = None

    @property
    def start(self) -> int:
        return self.sequence if self.first_sequence is None else self.first_sequence


class _Side:
    def __init__(self, *, descending: bool) -> None:
        self._descending = descending
        self._levels: dict[Decimal, Decimal] = {}
        self._keys: list[Decimal] = []

    def _key(self, price: Decimal) -> Decimal:
        return neg(price) if self._descending else price

    def set(self, price: Decimal, size: Decimal) -> None:
        key = self._key(price)
        if size == 0:
            if price in self._levels:
                del self._levels[price]
                del self._keys[bisect_left(self._keys, key)]
            return
        if price not in self._levels:
            insort(self._keys, key)
        self._levels[price] = size

    def clear(self) -> None:
        self._levels.clear()
        self._keys.clear()

    def top(self, depth: int | None) -> tuple[Level, ...]:
        keys = self._keys if depth is None else self._keys[:depth]
        prices = [self._key(key) for key in keys]
        return tuple(Level(price=p, size=self._levels[p]) for p in prices)

    def __len__(self) -> int:
        return len(self._levels)


class BookBuilder:
    """One instrument's book. Not thread-safe; own it from one task."""

    def __init__(self, symbol: Symbol, *, max_buffered: int = 100_000) -> None:
        self.symbol = symbol
        self._bids = _Side(descending=True)
        self._asks = _Side(descending=False)
        self._last: int | None = None
        self._synced = False
        self._buffer: deque[BookDelta] = deque()
        self._max_buffered = max_buffered
        self.gaps = 0
        self.stale_deltas = 0
        self.overflowed = False
        self.stale_snapshots = 0
        self._batch: tuple[int, int] | None = None

    @property
    def is_synced(self) -> bool:
        """Whether the book reflects every update up to :attr:`last_sequence`."""
        return self._synced

    @property
    def last_sequence(self) -> int | None:
        return self._last

    def apply_snapshot(
        self,
        sequence: int,
        bids: Iterable[tuple[Decimal, Decimal]],
        asks: Iterable[tuple[Decimal, Decimal]],
    ) -> bool:
        """Replace the book, then apply any buffered deltas newer than ``sequence``.

        A snapshot older than a book in sync is ignored and counted, because applying
        it would roll the book back. Returns whether it was applied.
        """
        if self._synced and self._last is not None and sequence < self._last:
            self.stale_snapshots += 1
            return False
        self._bids.clear()
        self._asks.clear()
        for price, size in bids:
            self._bids.set(price, size)
        for price, size in asks:
            self._asks.set(price, size)
        self._last = sequence
        self._synced = True
        self.overflowed = False
        buffered = list(self._buffer)
        self._buffer.clear()
        for delta in buffered:
            if delta.sequence > sequence:
                self.apply_delta(delta)
        return True

    def apply_delta(self, delta: BookDelta) -> bool:
        """Apply one delta. Returns false if it was buffered or ignored as old.

        A gap flips the book out of sync. It does not raise, since a gap is expected
        operation; check :attr:`is_synced` and fetch a new snapshot.
        """
        if not self._synced:
            self._buffer_delta(delta)
            return False
        assert self._last is not None
        batch = (delta.start, delta.sequence)
        same_batch = delta.first_sequence is not None and batch == self._batch
        if delta.sequence <= self._last and not same_batch:
            self.stale_deltas += 1
            return False
        if delta.start > self._last + 1:
            self.gaps += 1
            self._synced = False
            self._buffer_delta(delta)
            return False
        side = self._bids if delta.side is OrderSide.BUY else self._asks
        side.set(delta.price, delta.size)
        self._last = delta.sequence
        self._batch = batch
        return True

    def _buffer_delta(self, delta: BookDelta) -> None:
        if len(self._buffer) >= self._max_buffered:
            self._buffer.popleft()
            self.overflowed = True
        self._buffer.append(delta)

    def snapshot(
        self,
        *,
        depth: int | None = None,
        exchange_ts: datetime,
        received_ts: datetime,
    ) -> OrderBook:
        """The current book. Raises if it may be wrong; see :attr:`is_synced`."""
        if not self._synced or self._last is None:
            expected = 0 if self._last is None else self._last + 1
            newest = self._buffer[-1].sequence if self._buffer else expected
            raise SequenceGap(self.symbol, expected, newest)
        return OrderBook(
            symbol=self.symbol,
            exchange_ts=exchange_ts,
            received_ts=received_ts,
            sequence=self._last,
            bids=self._bids.top(depth),
            asks=self._asks.top(depth),
        )

    def depth_counts(self) -> tuple[int, int]:
        """Number of bid and ask levels held."""
        return (len(self._bids), len(self._asks))


__all__ = ["BookBuilder", "BookDelta"]
