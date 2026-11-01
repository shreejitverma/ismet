"""Helpers shared by the vendor normalizers."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from functools import wraps
from typing import ParamSpec, TypeVar

from ismet.errors import TransportError, ValidationError
from ismet.models import (
    Bar,
    DataSource,
    FeedKind,
    MarketEvent,
    QualityFlag,
    Timeliness,
    datetime_from_ns,
)
from ismet.refdata import register_crypto_asset, validate_currency
from ismet.transport.ws import SequenceOutcome, SequenceTracker

logger = logging.getLogger("ismet.normalize")

P = ParamSpec("P")
R = TypeVar("R")
E = TypeVar("E", bound=MarketEvent)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_ISO_8601_UTC = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$"
)
_NANOS_PER_MILLI = 1_000_000
_NANOS_PER_SECOND = 1_000_000_000
_MICROS_DIGITS = 6
_NANOS_DIGITS = 9


def normalizer(vendor: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Report a payload of the wrong shape as a non-retryable protocol error.

    ``KeyError``, ``pydantic.ValidationError`` and friends would otherwise escape
    the ismet error hierarchy. A vendor sending a payload ismet cannot read will
    not fix itself on retry.
    """

    def decorate(fn: Callable[P, R]) -> Callable[P, R]:
        @wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            try:
                return fn(*args, **kwargs)
            except (
                LookupError,
                AttributeError,
                TypeError,
                ValueError,
                ArithmeticError,
            ) as exc:
                raise TransportError(
                    f"{vendor}: unreadable payload in {fn.__name__}: {exc!r}",
                    retryable=False,
                ) from exc

        return wrapper

    return decorate


def venue_source(provider: str, feed: str) -> DataSource:
    """Provenance of an event read directly from the venue's public API."""
    return DataSource(
        provider=provider,
        feed=feed,
        kind=FeedKind.VENUE_API,
        timeliness=Timeliness.REALTIME,
    )


def strict_int(value: object) -> int:
    """``value`` if it is a JSON integer; anything else is an unreadable payload."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"expected an integer, got {value!r}")
    return value


def strict_bool(value: object) -> bool:
    """``value`` if it is a JSON boolean; anything else is an unreadable payload."""
    if not isinstance(value, bool):
        raise TypeError(f"expected a boolean, got {value!r}")
    return value


def datetime_from_ms(ms: int) -> datetime:
    """UTC datetime for integer epoch milliseconds."""
    return datetime_from_ns(int(ms) * _NANOS_PER_MILLI)


def parse_iso8601_utc(text: str) -> tuple[datetime, int | None]:
    """Parse a ``...Z`` timestamp without going through ``float``.

    Returns the datetime, floored to microseconds, and the exact epoch
    nanoseconds when the vendor published more digits than a datetime can hold.
    ``datetime.fromisoformat`` cannot be used because Python 3.10 rejects both
    ``Z`` and fractions that are not exactly 3 or 6 digits long.
    """
    match = _ISO_8601_UTC.match(text)
    if match is None:
        raise ValueError(f"not a UTC ISO 8601 timestamp: {text!r}")
    year, month, day, hour, minute, second = (int(g) for g in match.groups()[:6])
    fraction = match.group(7) or ""
    whole = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    seconds = (whole - _EPOCH) // timedelta(seconds=1)
    nanos = seconds * _NANOS_PER_SECOND + int(fraction.ljust(_NANOS_DIGITS, "0"))
    return datetime_from_ns(nanos), nanos if len(fraction) > _MICROS_DIGITS else None


def copy_made_at(headers: Mapping[str, str], received: datetime) -> datetime:
    """When a response was made, by the server's clock: ``Date`` minus any CDN ``Age``.

    Never later than ``received``, so a fast host clock cannot make a bar look closed.
    With no readable ``Date`` the response can only be taken as fresh, so this is
    ``received``.
    """
    try:
        served = parsedate_to_datetime(headers["date"])
        age = timedelta(seconds=max(int(headers.get("age", 0)), 0))
    except (KeyError, TypeError, ValueError, OverflowError):
        return received
    if served.tzinfo is None:
        served = served.replace(tzinfo=timezone.utc)
    return min(received, served - age)


def register_asset(code: str) -> str | None:
    """Register a venue asset; ``None`` if ismet cannot represent the code.

    Real venues list single-letter and non-Latin assets, such as Binance's ``U``
    and ``币安人生``, and those are valid. A code that is not representable, such as
    one with punctuation or a reserved ISO code like ``XTS``, must not break
    everything else on the venue, so the caller gets ``None`` and decides whether the
    field is optional.
    """
    try:
        register_crypto_asset(code)
        return validate_currency(code)
    except ValueError:
        return None


def check_range(start: datetime, end: datetime) -> None:
    """Require an aware, non-empty ``[start, end)``."""
    if start.tzinfo is None or end.tzinfo is None:
        raise ValidationError("start and end must be timezone-aware")
    if start >= end:
        raise ValidationError("start must be before end")


def search_rank(needle: str, ticker: str, *names: str) -> int | None:
    """Rank a search hit, lower first, or ``None`` when it does not match.

    Exact ticker, then ticker prefix, then exact name, then any substring.
    """
    if ticker == needle:
        return 0
    if ticker.startswith(needle):
        return 1
    if needle in names:
        return 2
    if needle in ticker or any(needle in name for name in names):
        return 3
    return None


def merge_bars(bars: Iterable[Bar], start: datetime, end: datetime) -> list[Bar]:
    """Ascending bars within ``[start, end)`` with one bar per open time.

    A later page wins a duplicate, because it was read later.
    """
    by_open = {b.exchange_ts: b for b in bars if start <= b.exchange_ts < end}
    return [by_open[ts] for ts in sorted(by_open)]


class StreamGate:
    """Decide what a stream does with each event, from its venue number.

    One gate serves one stream for its whole life and survives reconnects, so
    an event replayed after a reconnect is recognised. ``contiguous`` says whether
    the number increases by exactly one per event at this subscription:

    - Contiguous numbers (trade ids): a jump is a real loss. The first event after
      it is delivered with ``AFTER_GAP`` and the count is logged.
    - Sparse numbers (Binance bookTicker ``u``, Coinbase ticker ``sequence``): the
      subscription shows a subset, so a jump means nothing. After a reconnect the
      next event for each key is delivered with ``AFTER_GAP``, because updates were
      certainly unobserved while disconnected.

    Either way a repeat is dropped, and a number below the high-water mark seen for
    the first time is delivered with ``OUT_OF_ORDER``. Nothing is backfilled.
    """

    def __init__(self, *, contiguous: bool) -> None:
        self._contiguous = contiguous
        self._trackers: dict[str, SequenceTracker] = {}
        self._reconnected: set[str] = set()

    def _tracker(self, key: str) -> SequenceTracker:
        return self._trackers.setdefault(key, SequenceTracker())

    def is_fresh(self, key: str) -> bool:
        """Whether nothing has been seen for ``key`` yet."""
        return self._tracker(key).last is None

    def seed(self, key: str, number: int) -> None:
        """Set the position for ``key`` without delivering an event."""
        self._tracker(key).classify(number)

    def mark_reconnected(self) -> None:
        """Note that a new connection replaced the old one."""
        self._reconnected = {k for k, t in self._trackers.items() if t.last is not None}

    def admit(self, key: str, number: int, event: E) -> E | None:
        """``event`` with any quality flags it earned, or ``None`` to drop it."""
        observed = self._tracker(key).classify(number)
        flags: set[QualityFlag] = set()
        if observed.outcome is SequenceOutcome.DUPLICATE:
            return None
        if observed.outcome is SequenceOutcome.OUT_OF_ORDER:
            flags.add(QualityFlag.OUT_OF_ORDER)
        if observed.outcome is SequenceOutcome.GAP and self._contiguous:
            logger.warning(
                "stream gap on %s: %d events missing before %d",
                key,
                observed.missing,
                number,
            )
            flags.add(QualityFlag.AFTER_GAP)
        if key in self._reconnected:
            self._reconnected.discard(key)
            if not self._contiguous:
                flags.add(QualityFlag.AFTER_GAP)
        if not flags:
            return event
        return event.model_copy(update={"quality": event.quality | flags})


__all__ = [
    "StreamGate",
    "check_range",
    "copy_made_at",
    "datetime_from_ms",
    "merge_bars",
    "normalizer",
    "parse_iso8601_utc",
    "register_asset",
    "search_rank",
    "strict_bool",
    "strict_int",
    "venue_source",
]
