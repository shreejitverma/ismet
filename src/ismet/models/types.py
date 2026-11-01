"""Scalar types shared by all domain models.

Money and quantity fields are ``Decimal`` only. Floats are rejected at the
model boundary so precision loss cannot enter silently; parse vendor JSON with
``parse_float=Decimal`` (the HTTP and WebSocket transports do) or pass strings.

Validation rejects what cannot be real and admits what merely looks unusual.
Prices may be zero or negative because real markets produce both: a zero option
bid, negative crude futures in April 2020, negative calendar spreads. Sizes,
tick sizes, and lot sizes have narrower domains and their own types.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Annotated, Any

from pydantic import AfterValidator, AwareDatetime, BeforeValidator, PlainSerializer

from ismet.models.decimals import MAX_ADJUSTED_EXPONENT, MAX_SIGNIFICANT_DIGITS
from ismet.refdata.codes import validate_currency, validate_venue
from ismet.refdata.identifiers import (
    validate_cusip,
    validate_figi,
    validate_isin,
    validate_lei,
    validate_sedol,
)


def to_decimal(value: Any) -> Decimal:
    """Coerce ``value`` to a finite ``Decimal`` without going through float.

    Accepts ``Decimal``, ``int`` (not ``bool``), and numeric strings.
    Rejects ``float``, ``bool``, NaN, infinities, and magnitudes outside the
    range exact arithmetic is guaranteed for (see :mod:`ismet.models.decimals`).
    """
    if isinstance(value, bool):
        raise TypeError("bool is not a valid decimal value")
    if isinstance(value, float):
        raise TypeError(
            "float is not accepted for money or quantity fields; pass str, int, "
            "or Decimal (parse JSON with parse_float=Decimal)"
        )
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, int):
        result = Decimal(value)
    elif isinstance(value, str):
        try:
            result = Decimal(value.strip())
        except InvalidOperation as exc:
            raise ValueError(f"not a decimal number: {value!r}") from exc
    else:
        raise TypeError(f"cannot convert {type(value).__name__} to Decimal")
    if not result.is_finite():
        raise ValueError(f"decimal must be finite, got {result}")
    if len(result.as_tuple().digits) > MAX_SIGNIFICANT_DIGITS:
        raise ValueError(
            f"decimal has more than {MAX_SIGNIFICANT_DIGITS} significant digits"
        )
    if abs(result.adjusted()) > MAX_ADJUSTED_EXPONENT:
        raise ValueError(f"decimal magnitude out of range: {result}")
    return result


def _validate_decimal(value: Any) -> Decimal:
    try:
        return to_decimal(value)
    except TypeError as exc:
        raise ValueError(str(exc)) from None


def _positive(value: Decimal) -> Decimal:
    if value <= 0:
        raise ValueError(f"must be > 0, got {value}")
    return value


def _non_negative(value: Decimal) -> Decimal:
    if value < 0:
        raise ValueError(f"must be >= 0, got {value}")
    return value


_decimal_serializer = PlainSerializer(str, return_type=str, when_used="json")

DecimalStrict = Annotated[
    Decimal, BeforeValidator(_validate_decimal), _decimal_serializer
]
"""Any finite Decimal; floats rejected."""

Price = DecimalStrict
"""A price in quote units. Finite and signed: zero and negative are real."""

PositiveDecimal = Annotated[DecimalStrict, AfterValidator(_positive)]
"""A strictly positive Decimal, for tick sizes, lot sizes, and multipliers."""

Quantity = Annotated[DecimalStrict, AfterValidator(_non_negative)]
"""A non-negative Decimal."""

Ratio = DecimalStrict
"""A signed Decimal such as a multiplier or fee rate."""


def _utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc)


Timestamp = Annotated[AwareDatetime, AfterValidator(_utc)]
"""A timezone-aware datetime, normalised to UTC. Naive datetimes are rejected."""


Currency = Annotated[str, AfterValidator(validate_currency)]
"""ISO 4217 code, a venue quote unit such as ``GBX``, or a registered crypto asset.

Upper-cased. Unknown codes are rejected; see
:func:`ismet.refdata.register_crypto_asset`.
"""

VenueCode = Annotated[str, AfterValidator(validate_venue)]
"""ISO 10383 MIC, for example ``XNAS``, or a registered private venue code."""

Isin = Annotated[str, AfterValidator(validate_isin)]
Cusip = Annotated[str, AfterValidator(validate_cusip)]
Sedol = Annotated[str, AfterValidator(validate_sedol)]
Figi = Annotated[str, AfterValidator(validate_figi)]
Lei = Annotated[str, AfterValidator(validate_lei)]


class Side(str, Enum):
    """Aggressor side of a trade, or direction of an order."""

    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"


class AssetClass(str, Enum):
    """Coarse instrument classification."""

    EQUITY = "equity"
    ETF = "etf"
    INDEX = "index"
    FUTURE = "future"
    OPTION = "option"
    FX = "fx"
    CRYPTO = "crypto"
    BOND = "bond"
    FUND = "fund"
    UNKNOWN = "unknown"


class Interval(str, Enum):
    """Bar interval.

    Intraday intervals have a fixed length. ``1d``, ``1w``, and ``1M`` are venue
    sessions and calendar periods, whose real length varies with half-days,
    holidays, and DST, so :attr:`seconds` is ``None`` for them.
    """

    S1 = "1s"
    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"
    W1 = "1w"
    MO1 = "1M"

    @property
    def seconds(self) -> int | None:
        """Exact length in seconds, or ``None`` for session-based intervals."""
        return {
            Interval.S1: 1,
            Interval.M1: 60,
            Interval.M5: 300,
            Interval.M15: 900,
            Interval.M30: 1800,
            Interval.H1: 3600,
            Interval.H4: 14400,
        }.get(self)

    @property
    def nominal_seconds(self) -> int | None:
        """Wall-clock length on a 24x7 venue; ``None`` for the irregular ``1M``."""
        if self is Interval.D1:
            return 86400
        if self is Interval.W1:
            return 604800
        return self.seconds

    @property
    def is_session_based(self) -> bool:
        return self.seconds is None


class BookState(str, Enum):
    """Relationship between the best bid and best ask."""

    EMPTY = "empty"
    ONE_SIDED = "one_sided"
    NORMAL = "normal"
    LOCKED = "locked"
    CROSSED = "crossed"


class QualityFlag(str, Enum):
    """Data-quality conditions attached to an event, never silently repaired."""

    CLOCK_SKEW = "clock_skew"
    CROSSED = "crossed"
    LOCKED = "locked"
    STALE = "stale"
    AFTER_GAP = "after_gap"
    OUT_OF_ORDER = "out_of_order"
    OUTLIER = "outlier"
    RESTATED = "restated"


class Timeliness(str, Enum):
    """How current a feed is by contract, which decides what it may be used for."""

    REALTIME = "realtime"
    DELAYED = "delayed"
    INDICATIVE = "indicative"
    END_OF_DAY = "end_of_day"
    UNKNOWN = "unknown"


class FeedKind(str, Enum):
    CONSOLIDATED = "consolidated"
    DIRECT = "direct"
    VENUE_API = "venue_api"
    SIMULATED = "simulated"
    UNKNOWN = "unknown"


class Adjustment(str, Enum):
    """What corporate actions have been applied to a bar's prices."""

    RAW = "raw"
    SPLIT = "split"
    SPLIT_DIVIDEND = "split_dividend"
    UNKNOWN = "unknown"


class TradeAction(str, Enum):
    """A print, or a venue correction of an earlier print."""

    NEW = "new"
    CORRECT = "correct"
    CANCEL = "cancel"


__all__ = [
    "Adjustment",
    "AssetClass",
    "BookState",
    "Currency",
    "Cusip",
    "DecimalStrict",
    "FeedKind",
    "Figi",
    "Interval",
    "Isin",
    "Lei",
    "PositiveDecimal",
    "Price",
    "QualityFlag",
    "Quantity",
    "Ratio",
    "Sedol",
    "Side",
    "Timeliness",
    "Timestamp",
    "TradeAction",
    "VenueCode",
    "to_decimal",
]
