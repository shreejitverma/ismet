"""Exact decimal arithmetic, independent of the caller's decimal context.

Python's ambient context defaults to 28 significant digits and is mutable per
thread, so ``a + b`` can silently round a large crypto notional and ``a // b``
can raise ``DivisionImpossible``. Money code in ismet therefore never uses the
ambient context. These helpers run in a private context whose precision exceeds
anything :func:`ismet.models.types.to_decimal` admits (64 significant digits,
exponents within 64), and which traps ``Inexact``: an operation either returns
the exact answer or raises ``decimal.Inexact``. Only :func:`ratio` rounds, and it
says so.
"""

from __future__ import annotations

from decimal import (
    ROUND_HALF_EVEN,
    Context,
    Decimal,
    DivisionByZero,
    Inexact,
    InvalidOperation,
    Overflow,
    Underflow,
    localcontext,
)
from enum import Enum

MAX_SIGNIFICANT_DIGITS = 64
MAX_ADJUSTED_EXPONENT = 64

_EXACT = Context(
    prec=512,
    rounding=ROUND_HALF_EVEN,
    traps=[InvalidOperation, DivisionByZero, Overflow, Underflow, Inexact],
)
_ROUNDED = Context(
    prec=512,
    rounding=ROUND_HALF_EVEN,
    traps=[InvalidOperation, DivisionByZero, Overflow, Underflow],
)
RATIO_PRECISION = 50


class Rounding(str, Enum):
    """Direction for rounding to a step.

    ``FLOOR`` and ``CEILING`` are toward negative and positive infinity, so they
    stay correct for negative prices.
    """

    FLOOR = "floor"
    CEILING = "ceiling"
    TOWARD_ZERO = "toward_zero"
    AWAY_FROM_ZERO = "away_from_zero"
    HALF_EVEN = "half_even"


def add(a: Decimal, b: Decimal) -> Decimal:
    with localcontext(_EXACT):
        return a + b


def sub(a: Decimal, b: Decimal) -> Decimal:
    with localcontext(_EXACT):
        return a - b


def mul(a: Decimal, b: Decimal) -> Decimal:
    with localcontext(_EXACT):
        return a * b


def neg(a: Decimal) -> Decimal:
    with localcontext(_EXACT):
        return -a


def total(values: list[Decimal] | tuple[Decimal, ...]) -> Decimal:
    with localcontext(_EXACT):
        result = Decimal(0)
        for value in values:
            result += value
        return result


def midpoint(a: Decimal, b: Decimal) -> Decimal:
    """``(a + b) / 2``, always exact in decimal."""
    with localcontext(_EXACT):
        return (a + b) / 2


def ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    """``numerator / denominator`` rounded half-even to 50 significant digits.

    The one deliberately rounding operation, for values such as average price
    that have no finite decimal form. Callers keep the exact numerator and
    denominator as the source of truth.
    """
    with localcontext(_ROUNDED) as ctx:
        ctx.prec = RATIO_PRECISION
        return numerator / denominator


def is_multiple(value: Decimal, step: Decimal) -> bool:
    """Whether ``value`` is an exact integer multiple of ``step``."""
    if step <= 0:
        raise ValueError(f"step must be > 0, got {step}")
    with localcontext(_EXACT):
        return value % step == 0


def round_to_step(value: Decimal, step: Decimal, rounding: Rounding) -> Decimal:
    """Round ``value`` to a multiple of ``step`` in the stated direction."""
    if step <= 0:
        raise ValueError(f"step must be > 0, got {step}")
    with localcontext(_EXACT):
        quotient = value // step
        remainder = value - quotient * step
        if remainder == 0:
            return (quotient * step).quantize(step)
        # ``//`` truncates toward zero, so quotient is the toward-zero multiple.
        toward_zero = quotient * step
        away = toward_zero + (step if value > 0 else -step)
        if rounding is Rounding.TOWARD_ZERO:
            result = toward_zero
        elif rounding is Rounding.AWAY_FROM_ZERO:
            result = away
        elif rounding is Rounding.FLOOR:
            result = toward_zero if value > 0 else away
        elif rounding is Rounding.CEILING:
            result = away if value > 0 else toward_zero
        else:
            twice = abs(remainder) * 2
            if twice < step:
                result = toward_zero
            elif twice > step:
                result = away
            else:
                result = toward_zero if quotient % 2 == 0 else away
        return result.quantize(step)


__all__ = [
    "MAX_ADJUSTED_EXPONENT",
    "MAX_SIGNIFICANT_DIGITS",
    "DivisionByZero",
    "Inexact",
    "Rounding",
    "add",
    "is_multiple",
    "midpoint",
    "mul",
    "neg",
    "ratio",
    "round_to_step",
    "sub",
    "total",
]
