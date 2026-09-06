"""Scalar types shared by all domain models.

Money and quantity fields are ``Decimal`` only. Floats are rejected at the
model boundary so precision loss cannot enter silently; parse vendor JSON with
``parse_float=Decimal`` (the HTTP and WebSocket transports do) or pass strings.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Annotated, Any

from pydantic import AfterValidator, AwareDatetime, BeforeValidator, PlainSerializer

_CURRENCY_RE = re.compile(r"^[A-Z0-9]{3,10}$")
_MIC_RE = re.compile(r"^[A-Z0-9]{4}$")


def to_decimal(value: Any) -> Decimal:
    """Coerce ``value`` to a finite ``Decimal`` without going through float.

    Accepts ``Decimal``, ``int`` (not ``bool``), and numeric strings.
    Rejects ``float``, ``bool``, NaN, and infinities.
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

Price = Annotated[DecimalStrict, AfterValidator(_positive)]
"""A strictly positive Decimal."""

Quantity = Annotated[DecimalStrict, AfterValidator(_non_negative)]
"""A non-negative Decimal."""

Ratio = DecimalStrict
"""A signed Decimal such as a multiplier or fee rate."""


def _utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc)


Timestamp = Annotated[AwareDatetime, AfterValidator(_utc)]
"""A timezone-aware datetime, normalised to UTC. Naive datetimes are rejected."""


def _currency(value: str) -> str:
    value = value.strip().upper()
    if not _CURRENCY_RE.match(value):
        raise ValueError(f"invalid currency code: {value!r}")
    return value


Currency = Annotated[str, AfterValidator(_currency)]
"""ISO 4217 code, or a crypto asset code such as ``USDT``. Upper-cased."""


def _mic(value: str) -> str:
    value = value.strip().upper()
    if not _MIC_RE.match(value):
        raise ValueError(f"invalid venue MIC: {value!r} (expected 4 characters)")
    return value


VenueCode = Annotated[str, AfterValidator(_mic)]
"""ISO 10383 Market Identifier Code, for example ``XNAS`` or ``XNSE``."""


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
    """Bar interval. ``duration`` is ``None`` for calendar-irregular intervals."""

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
        """Fixed length in seconds, or ``None`` for ``1M``."""
        table = {
            Interval.S1: 1,
            Interval.M1: 60,
            Interval.M5: 300,
            Interval.M15: 900,
            Interval.M30: 1800,
            Interval.H1: 3600,
            Interval.H4: 14400,
            Interval.D1: 86400,
            Interval.W1: 604800,
        }
        return table.get(self)


__all__ = [
    "AssetClass",
    "Currency",
    "DecimalStrict",
    "Interval",
    "Price",
    "Quantity",
    "Ratio",
    "Side",
    "Timestamp",
    "VenueCode",
    "to_decimal",
]
