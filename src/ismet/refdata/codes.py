"""ISO venue and currency code registries backed by bundled snapshots.

The snapshots live in ``ismet/refdata/data`` and are regenerated from the
publishers by ``scripts/update_refdata.py``. Unknown codes are errors, not
guesses, with two explicit, append-only escape hatches for things ISO does not
cover: :func:`register_private_venue` for venues with no MIC, and
:func:`register_crypto_asset` for crypto assets, which have no ISO code at all.
Both registries are process-wide because a code's validity must not depend on
which object validated it. They only ever grow, so readers need no lock.

Expired MICs remain valid: historical data legitimately refers to them.
"""

from __future__ import annotations

import csv
import re
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from types import MappingProxyType

_MIC_FORMAT = re.compile(r"^[A-Z0-9]{4}$")
# Crypto tickers include single letters and non-Latin names, so only shape is checked.
_ASSET_FORMAT = re.compile(r"^[^\W_]{1,20}$")

NON_MONETARY_ISO_CODES = frozenset({"XTS", "XXX"})
"""ISO 4217 codes for testing and for "no currency"; never valid on a price."""

QUOTE_MINOR_UNITS: Mapping[str, tuple[str, int]] = MappingProxyType(
    {
        "GBX": ("GBP", 100),
        "GBP": ("GBP", 1),
        "ZAC": ("ZAR", 100),
        "ILA": ("ILS", 100),
        "USC": ("USD", 100),
    }
)
"""Non-ISO quotation units used by venues, mapped to (ISO currency, divisor)."""

_BUILTIN_CRYPTO = frozenset(
    [
        "AAVE",
        "ADA",
        "ALGO",
        "APT",
        "ARB",
        "ATOM",
        "AVAX",
        "BCH",
        "BNB",
        "BONK",
        "BTC",
        "BUSD",
        "DAI",
        "DOGE",
        "DOT",
        "ETC",
        "ETH",
        "EURC",
        "FDUSD",
        "FIL",
        "HBAR",
        "ICP",
        "INJ",
        "LDO",
        "LINK",
        "LTC",
        "MKR",
        "NEAR",
        "OP",
        "PEPE",
        "POL",
        "RNDR",
        "SEI",
        "SHIB",
        "SOL",
        "STX",
        "SUI",
        "TON",
        "TRX",
        "TUSD",
        "UNI",
        "USDC",
        "USDP",
        "USDT",
        "WBTC",
        "XLM",
        "XMR",
        "XRP",
    ]
)
_crypto_assets: set[str] = set(_BUILTIN_CRYPTO)
_private_venues: dict[str, str] = {
    "XMOK": "ismet deterministic mock venue",
    "XSIM": "ismet simulated matching venue",
    "ZCBS": "Coinbase Exchange spot (no ISO 10383 MIC is published)",
}


@dataclass(frozen=True)
class Venue:
    """One entry of the MIC registry, or a registered private venue."""

    mic: str
    operating_mic: str
    status: str
    country: str
    name: str

    @property
    def is_private(self) -> bool:
        return self.status == "PRIVATE"

    @property
    def is_active(self) -> bool:
        return self.status in ("ACTIVE", "UPDATED", "PRIVATE")


@dataclass(frozen=True)
class CurrencyInfo:
    """One ISO 4217 entry. ``minor_units`` is ``None`` where ISO says N.A."""

    code: str
    minor_units: int | None
    name: str


def _read_rows(filename: str) -> list[dict[str, str]]:
    text = (
        resources.files("ismet.refdata")
        .joinpath("data")
        .joinpath(filename)
        .read_text(encoding="utf-8")
    )
    lines = [line for line in text.splitlines() if not line.startswith("#")]
    return list(csv.DictReader(lines))


@lru_cache(maxsize=1)
def _iso_venues() -> Mapping[str, Venue]:
    return MappingProxyType(
        {
            row["mic"]: Venue(
                row["mic"],
                row["operating_mic"],
                row["status"],
                row["country"],
                row["name"],
            )
            for row in _read_rows("mics.csv")
        }
    )


@lru_cache(maxsize=1)
def _iso_currencies() -> Mapping[str, CurrencyInfo]:
    table: dict[str, CurrencyInfo] = {}
    for row in _read_rows("currencies.csv"):
        minor = row["minor_units"]
        table[row["code"]] = CurrencyInfo(
            row["code"], int(minor) if minor.isdigit() else None, row["name"]
        )
    return MappingProxyType(table)


def venue_info(code: str) -> Venue | None:
    """The registry entry for ``code``, or ``None`` if it is not a known venue."""
    upper = code.strip().upper()
    if upper in _private_venues:
        return Venue(upper, upper, "PRIVATE", "", _private_venues[upper])
    return _iso_venues().get(upper)


def is_known_venue(code: str) -> bool:
    return venue_info(code) is not None


def register_private_venue(code: str, name: str) -> str:
    """Register a venue that has no MIC. Colliding with a real MIC is an error.

    Idempotent for the same code. Returns the normalised code.
    """
    upper = code.strip().upper()
    if not _MIC_FORMAT.match(upper):
        raise ValueError(f"invalid venue code {code!r}: expected 4 characters")
    if upper in _iso_venues():
        raise ValueError(f"{upper} is an ISO 10383 MIC and cannot be made private")
    _private_venues.setdefault(upper, name)
    return upper


def currency_info(code: str) -> CurrencyInfo | None:
    return _iso_currencies().get(code.strip().upper())


def is_iso_currency(code: str) -> bool:
    upper = code.strip().upper()
    return upper in _iso_currencies() and upper not in NON_MONETARY_ISO_CODES


def is_crypto_asset(code: str) -> bool:
    return code.strip().upper() in _crypto_assets


def register_crypto_asset(code: str) -> str:
    """Declare a crypto asset code. Idempotent. Returns the normalised code.

    Adapters call this with the assets a venue actually lists. A code already
    assigned by ISO 4217 stays a currency and is not added.
    """
    upper = code.strip().upper()
    if not _ASSET_FORMAT.match(upper):
        raise ValueError(f"invalid asset code {code!r}")
    if upper not in _iso_currencies():
        _crypto_assets.add(upper)
    return upper


def validate_venue(value: str) -> str:
    """Normalise ``value`` and require it to be a known venue."""
    upper = value.strip().upper()
    if not _MIC_FORMAT.match(upper):
        raise ValueError(f"invalid venue MIC: {upper!r} (expected 4 characters)")
    if venue_info(upper) is None:
        raise ValueError(
            f"unknown venue MIC: {upper!r} is not in the ISO 10383 snapshot; "
            "use the operating or segment MIC, or register_private_venue() for a "
            "venue with no MIC"
        )
    return upper


def validate_currency(value: str) -> str:
    """Normalise ``value`` and require an ISO, quote-unit, or crypto code."""
    upper = value.strip().upper()
    if not _ASSET_FORMAT.match(upper) and upper not in QUOTE_MINOR_UNITS:
        raise ValueError(f"invalid currency code: {upper!r}")
    if upper in NON_MONETARY_ISO_CODES:
        raise ValueError(f"{upper} is a non-monetary ISO 4217 code")
    if is_iso_currency(upper) or upper in QUOTE_MINOR_UNITS or is_crypto_asset(upper):
        return upper
    raise ValueError(
        f"unknown currency code: {upper!r} is not ISO 4217, a known quote unit, or a "
        "registered crypto asset; call register_crypto_asset() if it is a real asset"
    )


__all__ = [
    "NON_MONETARY_ISO_CODES",
    "QUOTE_MINOR_UNITS",
    "CurrencyInfo",
    "Venue",
    "currency_info",
    "is_crypto_asset",
    "is_iso_currency",
    "is_known_venue",
    "register_crypto_asset",
    "register_private_venue",
    "validate_currency",
    "validate_venue",
    "venue_info",
]
