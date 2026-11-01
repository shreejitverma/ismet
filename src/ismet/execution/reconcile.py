"""Reconciliation results: where the venue and the journal disagree."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class BreakKind(str, Enum):
    """A disagreement between what ismet recorded and what the venue reports."""

    ORPHAN_ORDER = "orphan_order"
    ORPHAN_FILL = "orphan_fill"
    MISSING_AT_VENUE = "missing_at_venue"
    FILL_MISMATCH = "fill_mismatch"
    POSITION_MISMATCH = "position_mismatch"
    QUERY_FAILED = "query_failed"


CRITICAL_BREAKS = frozenset(
    {
        BreakKind.ORPHAN_ORDER,
        BreakKind.ORPHAN_FILL,
        BreakKind.FILL_MISMATCH,
        BreakKind.MISSING_AT_VENUE,
        BreakKind.POSITION_MISMATCH,
    }
)
"""Breaks that mean exposure may be wrong, which stop new orders by default."""


@dataclass(frozen=True)
class ReconBreak:
    kind: BreakKind
    subject: str
    detail: str

    @property
    def is_critical(self) -> bool:
        return self.kind in CRITICAL_BREAKS


@dataclass
class ReconReport:
    """One reconciliation pass.

    ``resolved`` lists client order ids whose state changed to match the venue.
    ``complete`` is false if any query failed, so a clean ``breaks`` list is not
    proof of agreement unless ``complete`` is true. ``positions_verified`` is false
    when positions could not be compared, for example on a venue with no account API,
    which leaves a real class of error undetectable by this pass.
    """

    at: datetime
    orders_checked: int = 0
    resolved: list[str] = field(default_factory=list)
    breaks: list[ReconBreak] = field(default_factory=list)
    complete: bool = True
    positions_verified: bool = True

    @property
    def clean(self) -> bool:
        return self.complete and not self.breaks


__all__ = ["CRITICAL_BREAKS", "BreakKind", "ReconBreak", "ReconReport"]
