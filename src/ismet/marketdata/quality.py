"""Stream guards and data-quality checks.

:class:`SequenceGuard` watches a stream's sequence numbers, per symbol, and applies a
caller-chosen policy. :class:`QualityMonitor` flags stale feeds, clock skew, outlier
prices, and disagreement between sources. Both only observe and report. Neither
edits a price, because a quietly "repaired" price is worse than a visibly bad one.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum

from ismet.errors import SequenceGap
from ismet.models import MarketEvent, Quote, Symbol, Trade
from ismet.models.decimals import absolute, mul, ratio, sub
from ismet.models.types import QualityFlag
from ismet.transport.ws import SequenceObservation, SequenceOutcome, SequenceTracker

logger = logging.getLogger("ismet.marketdata")


class GapPolicy(str, Enum):
    """What to do when a stream skips sequence numbers."""

    REPORT = "report"
    """Deliver the event, flagged, and log and count the gap."""
    RAISE = "raise"
    """Raise :class:`~ismet.errors.SequenceGap`, ending the stream."""


GapHandler = Callable[[Symbol, SequenceObservation], None]


@dataclass
class StreamStats:
    """Counters for one stream, safe to read at any time."""

    delivered: int = 0
    gaps: int = 0
    missing: int = 0
    duplicates_dropped: int = 0
    out_of_order: int = 0


class SequenceGuard:
    """Tracks per-symbol sequences. Events with no sequence pass through unchecked.

    Duplicates are dropped and counted. A late event that a gap reported missing is
    delivered with :attr:`QualityFlag.OUT_OF_ORDER`. Events after a gap carry
    :attr:`QualityFlag.AFTER_GAP`.
    """

    def __init__(
        self,
        policy: GapPolicy = GapPolicy.REPORT,
        *,
        on_gap: GapHandler | None = None,
    ) -> None:
        self.policy = policy
        self.stats = StreamStats()
        self._on_gap = on_gap
        self._trackers: dict[Symbol, SequenceTracker] = {}

    def admit(self, event: MarketEvent) -> MarketEvent | None:
        """Return the event to deliver, or ``None`` to drop a duplicate."""
        if event.sequence is None:
            self.stats.delivered += 1
            return event
        tracker = self._trackers.setdefault(event.symbol, SequenceTracker())
        observed = tracker.classify(event.sequence)
        if observed.outcome is SequenceOutcome.DUPLICATE:
            self.stats.duplicates_dropped += 1
            return None
        flag: QualityFlag | None = None
        if observed.outcome is SequenceOutcome.GAP:
            self.stats.gaps += 1
            self.stats.missing += observed.missing
            self._report_gap(event.symbol, observed)
            flag = QualityFlag.AFTER_GAP
        elif observed.outcome is SequenceOutcome.OUT_OF_ORDER:
            self.stats.out_of_order += 1
            flag = QualityFlag.OUT_OF_ORDER
        self.stats.delivered += 1
        if flag is None:
            return event
        return event.model_copy(update={"quality": event.quality | {flag}})

    def _report_gap(self, symbol: Symbol, observed: SequenceObservation) -> None:
        logger.warning(
            "sequence gap on %s: expected %s, got %s (%s missing)",
            symbol,
            observed.expected,
            observed.sequence,
            observed.missing,
        )
        if self._on_gap is not None:
            self._on_gap(symbol, observed)
        if self.policy is GapPolicy.RAISE:
            assert observed.expected is not None
            raise SequenceGap(symbol, observed.expected, observed.sequence)

    def reset(self, symbol: Symbol | None = None) -> None:
        """Forget positions, for a venue that restarts sequences on reconnect."""
        if symbol is None:
            self._trackers.clear()
        else:
            self._trackers.pop(symbol, None)


class Issue(str, Enum):
    STALE = "stale"
    CLOCK_SKEW = "clock_skew"
    OUTLIER = "outlier"
    CROSSED = "crossed"
    SOURCE_DIVERGENCE = "source_divergence"


@dataclass(frozen=True)
class QualityIssue:
    symbol: Symbol
    issue: Issue
    detail: str


@dataclass
class QualityThresholds:
    """Thresholds; all have conservative defaults a desk can tighten."""

    max_age: timedelta = timedelta(seconds=30)
    max_clock_skew: timedelta = timedelta(seconds=1)
    outlier_bps: Decimal = Decimal(1000)
    divergence_bps: Decimal = Decimal(100)
    outlier_confirmations: int = 3
    """Repeats of an outlying price needed before it becomes the new level."""


@dataclass
class QualityMonitor:
    """Keeps the last good price per symbol and reports what looks wrong."""

    thresholds: QualityThresholds = field(default_factory=QualityThresholds)
    _last_price: dict[Symbol, Decimal] = field(default_factory=dict)
    _last_seen: dict[Symbol, datetime] = field(default_factory=dict)
    _candidate: dict[Symbol, tuple[Decimal, int]] = field(default_factory=dict)

    @staticmethod
    def _price_of(event: MarketEvent) -> Decimal | None:
        if isinstance(event, Trade):
            return event.price
        if isinstance(event, Quote):
            return event.mid if event.mid is not None else event.last
        return None

    def observe(self, event: MarketEvent) -> list[QualityIssue]:
        """Record ``event`` and return anything wrong with it."""
        issues: list[QualityIssue] = []
        symbol = event.symbol
        limits = self.thresholds
        self._last_seen[symbol] = event.received_ts
        if event.latency > limits.max_age:
            issues.append(
                QualityIssue(
                    symbol,
                    Issue.STALE,
                    f"event is {event.latency.total_seconds():.1f}s old on arrival",
                )
            )
        if event.clock_skew > limits.max_clock_skew:
            issues.append(
                QualityIssue(
                    symbol,
                    Issue.CLOCK_SKEW,
                    f"venue clock is {event.clock_skew} ahead of local",
                )
            )
        if QualityFlag.CROSSED in event.flags:
            issues.append(QualityIssue(symbol, Issue.CROSSED, "crossed market"))
        price = self._price_of(event)
        if price is None:
            return issues
        previous = self._last_price.get(symbol)
        if previous is not None and previous != 0:
            move = _bps_between(price, previous)
            if move > limits.outlier_bps:
                issues.append(
                    QualityIssue(
                        symbol,
                        Issue.OUTLIER,
                        f"{price} moved {move:.1f} bps from {previous}",
                    )
                )
                if not self._confirmed(symbol, price):
                    return issues  # the good level stands until this is confirmed
        self._candidate.pop(symbol, None)
        self._last_price[symbol] = price
        return issues

    def _confirmed(self, symbol: Symbol, price: Decimal) -> bool:
        held, count = self._candidate.get(symbol, (price, 0))
        count = count + 1 if held == price else 1
        self._candidate[symbol] = (price, count)
        return count >= self.thresholds.outlier_confirmations

    def stale_symbols(
        self, now: datetime, *, expected: Iterable[Symbol] = ()
    ) -> list[QualityIssue]:
        """Symbols with no event within ``max_age``.

        Pass ``expected`` to also report symbols that have never produced an event,
        which otherwise stay invisible.
        """
        never_seen = [
            QualityIssue(symbol, Issue.STALE, "no event received")
            for symbol in expected
            if symbol not in self._last_seen
        ]
        return never_seen + [
            QualityIssue(
                symbol,
                Issue.STALE,
                f"last event {(now - seen).total_seconds():.1f}s ago",
            )
            for symbol, seen in sorted(
                self._last_seen.items(), key=lambda kv: str(kv[0])
            )
            if now - seen > self.thresholds.max_age
        ]

    def compare_sources(
        self, symbol: Symbol, price_a: Decimal, price_b: Decimal
    ) -> QualityIssue | None:
        """Flag two sources disagreeing on a price by more than ``divergence_bps``."""
        if price_a == price_b:
            return None
        if price_b == 0:
            return QualityIssue(symbol, Issue.SOURCE_DIVERGENCE, "second price is zero")
        gap = _bps_between(price_a, price_b)
        if gap <= self.thresholds.divergence_bps:
            return None
        return QualityIssue(
            symbol,
            Issue.SOURCE_DIVERGENCE,
            f"{price_a} and {price_b} differ by {gap:.1f} bps",
        )


def _bps_between(price: Decimal, reference: Decimal) -> Decimal:
    return ratio(
        mul(absolute(sub(price, reference)), Decimal(10000)), absolute(reference)
    )


__all__ = [
    "GapHandler",
    "GapPolicy",
    "Issue",
    "QualityIssue",
    "QualityMonitor",
    "QualityThresholds",
    "SequenceGuard",
    "StreamStats",
]
