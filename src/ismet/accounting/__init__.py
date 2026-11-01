"""Post-trade books: tax lots, realized and unrealized P&L, fees, and slippage.

A :class:`PositionLedger` is rebuilt from fills, so a bust or a late correction
changes history by removing or replacing a fill, never by adjusting a running total
that nobody can then check. All arithmetic is exact. Everything is in the
instrument's quote currency; :func:`convert` moves a value between currencies at a
rate the caller declares, because a hidden rate source is how P&L stops being
reproducible.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

from ismet.errors import ValidationError
from ismet.models.decimals import absolute, add, mul, ratio, sub
from ismet.models.orders import Fill, OrderSide
from ismet.models.symbol import Symbol

_ZERO = Decimal(0)


class LotMethod(str, Enum):
    """Which open lot a closing trade consumes. Tax treatment varies by country."""

    FIFO = "fifo"
    LIFO = "lifo"
    AVERAGE = "average"


@dataclass(frozen=True)
class Lot:
    """An open position slice. ``quantity`` is signed: short lots are negative."""

    quantity: Decimal
    price: Decimal
    opened_by: str


@dataclass(frozen=True)
class RealizedTrade:
    """One closing match."""

    quantity: Decimal
    open_price: Decimal
    close_price: Decimal
    pnl: Decimal
    opened_by: str
    closed_by: str


@dataclass
class PositionLedger:
    """The books for one instrument. Apply fills in execution order.

    ``multiplier`` converts price points to money, for options and futures. ``fees``
    are kept per currency, because a fee in BNB and a fee in USDT cannot be added.
    ``fills_with_unknown_fee`` counts fills that reported no fee, which is not the same
    as a zero fee.
    """

    method: LotMethod = LotMethod.FIFO
    multiplier: Decimal = Decimal(1)
    lots: list[Lot] = field(default_factory=list)
    realized_trades: list[RealizedTrade] = field(default_factory=list)
    fees: dict[str, Decimal] = field(default_factory=dict)
    fills_with_unknown_fee: int = 0
    symbol: Symbol | None = None
    _seen: set[str] = field(default_factory=set)

    @property
    def quantity(self) -> Decimal:
        """Net position. Short is negative."""
        result = _ZERO
        for lot in self.lots:
            result = add(result, lot.quantity)
        return result

    @property
    def realized_pnl(self) -> Decimal:
        """Realized P&L before fees."""
        result = _ZERO
        for trade in self.realized_trades:
            result = add(result, trade.pnl)
        return result

    @property
    def cost_basis(self) -> Decimal:
        """Signed sum of quantity times open price for the open lots."""
        result = _ZERO
        for lot in self.lots:
            result = add(result, mul(lot.quantity, lot.price))
        return result

    @property
    def average_open_price(self) -> Decimal | None:
        quantity = self.quantity
        return None if quantity == 0 else ratio(self.cost_basis, quantity)

    def unrealized_pnl(self, mark: Decimal) -> Decimal:
        """Mark-to-market P&L on the open lots at ``mark``."""
        value = mul(self.quantity, mark)
        return mul(sub(value, self.cost_basis), self.multiplier)

    def apply(self, fill: Fill) -> list[RealizedTrade]:
        """Book ``fill``; returns the closing matches it produced.

        A repeated ``fill_id`` is ignored, so replaying reports cannot double count.
        """
        if fill.fill_id in self._seen:
            return []
        if self.symbol is None:
            self.symbol = fill.symbol
        elif fill.symbol != self.symbol:
            raise ValidationError(
                f"ledger for {self.symbol} was given a fill in {fill.symbol}"
            )
        if fill.fee is not None and fill.fee != 0 and fill.fee_currency is None:
            raise ValidationError(f"fill {fill.fill_id} has a fee with no currency")
        self._seen.add(fill.fill_id)
        if fill.fee is None:
            self.fills_with_unknown_fee += 1
        elif fill.fee_currency is not None:
            self.fees[fill.fee_currency] = add(
                self.fees.get(fill.fee_currency, _ZERO), fill.fee
            )
        signed = mul(Decimal(fill.side.sign), fill.quantity)
        closing: list[RealizedTrade] = []
        remaining = signed
        if self.method is LotMethod.AVERAGE and self.lots:
            self._collapse_to_average()
        while remaining != 0 and self.lots and self._opposes(remaining):
            index = -1 if self.method is LotMethod.LIFO else 0
            lot = self.lots[index]
            matched = min(absolute(remaining), absolute(lot.quantity))
            sign = 1 if lot.quantity > 0 else -1
            pnl = mul(
                mul(mul(Decimal(sign), matched), sub(fill.price, lot.price)),
                self.multiplier,
            )
            closing.append(
                RealizedTrade(
                    matched, lot.price, fill.price, pnl, lot.opened_by, fill.fill_id
                )
            )
            left = sub(absolute(lot.quantity), matched)
            if left == 0:
                self.lots.pop(index)
            else:
                self.lots[index] = Lot(
                    mul(Decimal(sign), left), lot.price, lot.opened_by
                )
            remaining = (
                add(remaining, matched) if remaining < 0 else sub(remaining, matched)
            )
        if remaining != 0:
            self.lots.append(Lot(remaining, fill.price, fill.fill_id))
        self.realized_trades.extend(closing)
        return closing

    def _opposes(self, signed: Decimal) -> bool:
        return (self.lots[0].quantity > 0) != (signed > 0)

    def _collapse_to_average(self) -> None:
        quantity = self.quantity
        if quantity == 0 or len(self.lots) == 1:
            return
        average = ratio(self.cost_basis, quantity)
        self.lots = [Lot(quantity, average, self.lots[0].opened_by)]


def build_ledger(
    fills: Iterable[Fill],
    *,
    method: LotMethod = LotMethod.FIFO,
    multiplier: Decimal = Decimal(1),
) -> PositionLedger:
    """Build the books from fills, in execution-time order then ``fill_id``."""
    ledger = PositionLedger(method=method, multiplier=multiplier)
    for fill in sorted(fills, key=lambda f: (f.ts, f.fill_id)):
        ledger.apply(fill)
    return ledger


@dataclass(frozen=True)
class Valuation:
    """Total P&L in one currency, with the rates that produced it recorded."""

    currency: str
    realized: Decimal
    unrealized: Decimal
    fees: Decimal
    rates_used: Mapping[str, Decimal]

    @property
    def net(self) -> Decimal:
        return sub(add(self.realized, self.unrealized), self.fees)


def convert(
    amount: Decimal, from_currency: str, to_currency: str, rates: Mapping[str, Decimal]
) -> Decimal:
    """Convert ``amount``. ``rates`` maps ``"EURUSD"`` to USD per one EUR.

    Uses the direct pair, or the inverse. A missing rate raises rather than
    assuming parity.
    """
    if from_currency == to_currency:
        return amount
    direct = rates.get(f"{from_currency}{to_currency}")
    if direct is not None:
        return mul(amount, direct)
    inverse = rates.get(f"{to_currency}{from_currency}")
    if inverse is not None:
        return ratio(amount, inverse)
    raise ValidationError(f"no rate to convert {from_currency} to {to_currency}")


def value_books(
    books: Mapping[str, tuple[PositionLedger, str, Decimal | None]],
    *,
    currency: str,
    rates: Mapping[str, Decimal],
) -> Valuation:
    """Sum several ledgers in ``currency``.

    ``books`` maps a name to (ledger, quote currency, mark). An open position with no
    mark raises: an unpriced position must not read as zero P&L.
    """
    realized = unrealized = fees = _ZERO
    for name, (ledger, quote_currency, mark) in books.items():
        if mark is None and ledger.quantity != 0:
            raise ValidationError(f"{name} has an open position and no mark")
        realized = add(
            realized, convert(ledger.realized_pnl, quote_currency, currency, rates)
        )
        for fee_currency, amount in ledger.fees.items():
            fees = add(fees, convert(amount, fee_currency, currency, rates))
        if mark is not None:
            unrealized = add(
                unrealized,
                convert(ledger.unrealized_pnl(mark), quote_currency, currency, rates),
            )
    return Valuation(currency, realized, unrealized, fees, dict(rates))


def slippage_bps(side: OrderSide, arrival: Decimal, average: Decimal) -> Decimal:
    """Implementation shortfall against the arrival price, in basis points.

    Positive means the execution cost money relative to arrival, on either side.
    """
    if arrival == 0:
        raise ValidationError("arrival price is zero; slippage in bps is undefined")
    cost = mul(Decimal(side.sign), sub(average, arrival))
    return ratio(mul(cost, Decimal(10000)), absolute(arrival))


__all__ = [
    "Lot",
    "LotMethod",
    "PositionLedger",
    "RealizedTrade",
    "Valuation",
    "build_ledger",
    "convert",
    "slippage_bps",
    "value_books",
]
