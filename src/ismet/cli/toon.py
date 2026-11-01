"""A small encoder for the subset of TOON that the ismet CLI prints.

See https://toonformat.dev/reference/spec.html. Objects are ``key: value`` lines with
two-space nesting, and uniform lists are ``name[N]{a,b}:`` tables.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

_SAFE = re.compile(r"^[^\s,:\"\\\[\]{}#-][^,:\"\\\[\]{}\n\r\t]*(?<!\s)$")
_NUMBER_LIKE = re.compile(r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$")
_RESERVED = {"true", "false", "null"}


def scalar(value: Any) -> str:
    """Encode one scalar. Text that could read as another type is quoted."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal | int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, Enum):
        return scalar(value.value)
    if isinstance(value, datetime | date):
        return f'"{value.isoformat()}"'
    text = str(value)
    if _SAFE.match(text) and text not in _RESERVED and not _NUMBER_LIKE.match(text):
        return text
    escaped = (
        text.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'


def _is_table(items: Sequence[Any]) -> bool:
    if not items or not all(isinstance(item, Mapping) for item in items):
        return False
    keys = list(items[0])
    return all(
        list(item) == keys
        and not any(isinstance(v, Mapping | list | tuple) for v in item.values())
        for item in items
    )


def encode(value: Mapping[str, Any], indent: int = 0) -> str:
    """Encode a mapping as TOON text."""
    pad = "  " * indent
    lines: list[str] = []
    for key, item in value.items():
        if isinstance(item, Mapping):
            if item:
                lines.append(f"{pad}{key}:")
                lines.append(encode(item, indent + 1))
            else:
                lines.append(f"{pad}{key}: {{}}")
        elif isinstance(item, list | tuple):
            if _is_table(item):
                fields = ",".join(item[0])
                lines.append(f"{pad}{key}[{len(item)}]{{{fields}}}:")
                lines.extend(
                    f"{pad}  " + ",".join(scalar(v) for v in row.values())
                    for row in item
                )
            elif item and not any(isinstance(v, Mapping | list | tuple) for v in item):
                lines.append(f"{pad}{key}[{len(item)}]:")
                lines.extend(f"{pad}  {scalar(v)}" for v in item)
            else:
                lines.append(f"{pad}{key}[0]:")
        else:
            lines.append(f"{pad}{key}: {scalar(item)}")
    return "\n".join(lines)


__all__ = ["encode", "scalar"]
