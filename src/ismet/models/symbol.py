"""Instrument identity."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from ismet.models.types import VenueCode


class Symbol(BaseModel):
    """Identity of an instrument on one venue.

    The pair ``(venue, ticker)`` is the primary key. Global identifiers are
    optional and carried through when a provider supplies them. ``vendor_codes``
    holds provider-specific handles keyed by provider name, so a provider can
    round-trip its own identifier without polluting the public key.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    venue: VenueCode
    ticker: str = Field(min_length=1)
    isin: str | None = None
    figi: str | None = None
    cusip: str | None = None
    sedol: str | None = None
    vendor_codes: dict[str, str] = Field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.ticker}@{self.venue}"

    def __hash__(self) -> int:
        return hash(self.key)

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
