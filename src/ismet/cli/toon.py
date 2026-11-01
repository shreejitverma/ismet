"""A small encoder for the subset of TOON that the ismet CLI prints.

See https://toonformat.dev/reference/spec.html. Objects are ``key: value`` lines with
two-space nesting, uniform lists are ``name[N]{a,b}:`` tables, and other lists are
``name[N]:`` followed by ``- `` items.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

_SAFE = re.compile(r"[^\s,:\"\\\[\]{}#-][^,:\"\\\[\]{}\n\r\t]*(?<!\s)")
_NUMBER_LIKE = re.compile(r"-?\d+(\.\d+)?([eE][+-]?\d+)?")
_RESERVED = {"true", "false", "null"}
_BARE_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_.]*")


def _quoted(text: str) -> str:
    escaped = (
        text.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'


def _key(name: object) -> str:
    text = str(name)
    return text if _BARE_KEY.fullmatch(text) else _quoted(text)


def scalar(value: Any) -> str:
    """Encode one scalar. Text that could read as another type is quoted."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, Enum):
        return scalar(value.value)
    if isinstance(value, datetime | date):
        return f'"{value.isoformat()}"'
    text = str(value)
    if (
        _SAFE.fullmatch(text)
        and text not in _RESERVED
        and not _NUMBER_LIKE.fullmatch(text)
    ):
        return text
    return _quoted(text)


def _is_table(items: Sequence[Any]) -> bool:
    if not items or not all(isinstance(item, Mapping) for item in items):
        return False
    keys = list(items[0])
    return bool(keys) and all(
        list(item) == keys
        and not any(isinstance(v, Mapping | list | tuple) for v in item.values())
        for item in items
    )


def _is_nested(value: Any) -> bool:
    return isinstance(value, Mapping | list | tuple)


def _item_lines(item: Any, depth: int) -> list[str]:
    pad = "  " * depth
    if not _is_nested(item):
        return [f"{pad}- {scalar(item)}"]
    if isinstance(item, Mapping):
        lines = _mapping_lines(item, depth + 1)
    else:
        lines = _field_lines("", item, depth + 1)
    if not lines:
        return [f"{pad}-"]
    return [f"{pad}- {lines[0].lstrip()}", *lines[1:]]


def _field_lines(head: str, item: Any, depth: int) -> list[str]:
    pad = "  " * depth
    if isinstance(item, Mapping):
        return [f"{pad}{head}:", *_mapping_lines(item, depth + 1)]
    if not isinstance(item, list | tuple):
        return [f"{pad}{head}: {scalar(item)}"]
    if _is_table(item):
        fields = ",".join(_key(name) for name in item[0])
        return [
            f"{pad}{head}[{len(item)}]{{{fields}}}:",
            *(f"{pad}  " + ",".join(scalar(v) for v in row.values()) for row in item),
        ]
    lines = [f"{pad}{head}[{len(item)}]:"]
    for element in item:
        lines.extend(_item_lines(element, depth + 1))
    return lines


def _mapping_lines(value: Mapping[Any, Any], depth: int) -> list[str]:
    lines: list[str] = []
    for key, item in value.items():
        lines.extend(_field_lines(_key(key), item, depth))
    return lines


def encode(value: Mapping[str, Any]) -> str:
    """Encode a mapping as TOON text."""
    return "\n".join(_mapping_lines(value, 0))


__all__ = ["encode", "scalar"]
