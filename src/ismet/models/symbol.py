"""Instrument identity."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from ismet.models.types import Cusip, Figi, Isin, Sedol, VenueCode


class Symbol(BaseModel):
    """Identity of an instrument on one venue.

    The pair ``(venue, ticker)`` is the primary key. Global identifiers are
    optional and checksum-validated when present. ``vendor_codes`` holds
    provider-specific handles keyed by provider name, so a provider can
    round-trip its own identifier without polluting the public key.

    Equality and hashing use the primary key only: the same listing described
    with and without an ISIN is the same symbol. Use :meth:`same_identifiers` to
    detect two sources disagreeing about a listing's identifiers.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    venue: VenueCode
    ticker: str = Field(min_length=1)
    isin: Isin | None = None
    figi: Figi | None = None
    cusip: Cusip | None = None
    sedol: Sedol | None = None
    vendor_codes: dict[str, str] = Field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.ticker}@{self.venue}"

    def __hash__(self) -> int:
        return hash(self.key)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Symbol):
            return NotImplemented
        return self.key == other.key

    def same_identifiers(self, other: Symbol) -> bool:
        """Whether no global identifier conflicts between the two descriptions."""
        for name in ("isin", "figi", "cusip", "sedol"):
            mine, theirs = getattr(self, name), getattr(other, name)
            if mine is not None and theirs is not None and mine != theirs:
                return False
        return True

    @property
    def key(self) -> tuple[str, str]:
        """The ``(venue, ticker)`` primary key."""
        return (self.venue, self.ticker)

    @classmethod
    def parse(cls, text: str, *, venue: str | None = None) -> Symbol:
        """Parse ``"TICKER@MIC"`` or ``"TICKER"`` with an explicit ``venue``."""
        ticker, sep, parsed_venue = text.partition("@")
        if sep:
            if venue is not None and venue.upper() != parsed_venue.upper():
                raise ValueError(
                    f"venue mismatch: {parsed_venue!r} in {text!r} vs {venue!r}"
                )
            return cls(venue=parsed_venue, ticker=ticker)
        if venue is None:
            raise ValueError(
                f"symbol {text!r} has no venue; use 'TICKER@MIC' or pass venue="
            )
        return cls(venue=venue, ticker=ticker)


__all__ = ["Symbol"]
