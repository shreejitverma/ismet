"""Check-digit validation for securities and legal-entity identifiers.

Algorithms: ISIN (ISO 6166, Luhn over expanded digits), CUSIP (CUSIP Global
Services modulus-10 double-add-double), SEDOL (LSEG weighted modulus-10), FIGI
(OMG FIGI 1.0 modified Luhn), LEI (ISO 17442, ISO 7064 MOD 97-10).

Each ``validate_*`` returns the upper-cased identifier or raises ``ValueError``.
"""

from __future__ import annotations

import re

_ISIN = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
_CUSIP = re.compile(r"^[A-Z0-9*@#]{8}[0-9]$")
_SEDOL = re.compile(r"^[B-DF-HJ-NP-TV-Z0-9]{6}[0-9]$")
_FIGI = re.compile(r"^[B-DF-HJ-NP-TV-Z]{2}G[B-DF-HJ-NP-TV-Z0-9]{8}[0-9]$")
_LEI = re.compile(r"^[A-Z0-9]{18}[0-9]{2}$")
_FIGI_BANNED_PREFIXES = frozenset({"BS", "BM", "GG", "GB", "GH", "KY", "VG"})
_SEDOL_WEIGHTS = (1, 3, 1, 7, 3, 9, 1)


def _char_value(ch: str) -> int:
    return int(ch) if ch.isdigit() else ord(ch) - ord("A") + 10


def validate_isin(value: str) -> str:
    code = value.strip().upper()
    if not _ISIN.match(code):
        raise ValueError(f"invalid ISIN format: {value!r}")
    digits = "".join(str(_char_value(ch)) for ch in code)
    total = 0
    for index, digit in enumerate(reversed(digits)):
        number = int(digit)
        if index % 2 == 1:
            number *= 2
        total += number // 10 + number % 10
    if total % 10 != 0:
        raise ValueError(f"invalid ISIN check digit: {value!r}")
    return code


def validate_cusip(value: str) -> str:
    code = value.strip().upper()
    if not _CUSIP.match(code):
        raise ValueError(f"invalid CUSIP format: {value!r}")
    special = {"*": 36, "@": 37, "#": 38}
    total = 0
    for index, ch in enumerate(code[:8]):
        number = special.get(ch) if ch in special else _char_value(ch)
        assert number is not None
        if index % 2 == 1:
            number *= 2
        total += number // 10 + number % 10
    if (10 - total % 10) % 10 != int(code[8]):
        raise ValueError(f"invalid CUSIP check digit: {value!r}")
    return code


def validate_sedol(value: str) -> str:
    code = value.strip().upper()
    if not _SEDOL.match(code):
        raise ValueError(f"invalid SEDOL format: {value!r}")
    total = sum(
        _char_value(ch) * weight
        for ch, weight in zip(code, _SEDOL_WEIGHTS, strict=True)
    )
    if total % 10 != 0:
        raise ValueError(f"invalid SEDOL check digit: {value!r}")
    return code


def validate_figi(value: str) -> str:
    code = value.strip().upper()
    if not _FIGI.match(code) or code[:2] in _FIGI_BANNED_PREFIXES:
        raise ValueError(f"invalid FIGI format: {value!r}")
    total = 0
    for index, ch in enumerate(code[:11]):
        number = _char_value(ch)
        if index % 2 == 1:
            number *= 2
        total += number // 10 + number % 10
    if (10 - total % 10) % 10 != int(code[11]):
        raise ValueError(f"invalid FIGI check digit: {value!r}")
    return code


def validate_lei(value: str) -> str:
    code = value.strip().upper()
    if not _LEI.match(code):
        raise ValueError(f"invalid LEI format: {value!r}")
    if int("".join(str(_char_value(ch)) for ch in code)) % 97 != 1:
        raise ValueError(f"invalid LEI check digits: {value!r}")
    return code


__all__ = [
    "validate_cusip",
    "validate_figi",
    "validate_isin",
    "validate_lei",
    "validate_sedol",
]
