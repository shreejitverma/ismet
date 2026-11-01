"""Hand data to dataframes without silently losing precision.

Prices stay ``Decimal`` by default: an object column in pandas, a decimal column in
polars. A float column loses money precision, so it is available only when asked for
by name. ``pandas`` and ``polars`` are optional extras, imported on use.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterable, Iterator
from decimal import Decimal
from typing import Any

from pydantic import BaseModel

from ismet.errors import ConfigError, ValidationError
from ismet.models import Bar

_DROPPED = {"symbol"}
_POLARS_DECIMAL_DIGITS = 38


def to_records(items: Iterable[BaseModel]) -> list[dict[str, Any]]:
    """Flat dictionaries, one per model, with ``ticker`` and ``venue`` as strings.

    ``quality`` is every flag sorted and comma-joined, including those derived from the
    data such as ``crossed`` and ``clock_skew``, and empty when clean. ``source`` is
    the provider's name and ``timeliness`` what it contracts to, so a flagged or delayed
    row stays visibly so.
    """
    rows: list[dict[str, Any]] = []
    for item in items:
        row = {k: v for k, v in item.model_dump().items() if k not in _DROPPED}
        symbol = getattr(item, "symbol", None)
        if symbol is not None:
            row = {"ticker": symbol.ticker, "venue": symbol.venue, **row}
        if "quality" in row:
            flags = getattr(item, "flags", row["quality"])
            row["quality"] = ",".join(sorted(flag.value for flag in flags))
        if "source" in row:
            source = getattr(item, "source", None)
            row["source"] = None if source is None else source.provider
            row["timeliness"] = None if source is None else source.timeliness.value
        rows.append(row)
    return rows


def _optional(module: str) -> Any:
    try:
        return importlib.import_module(module)
    except ImportError as exc:
        raise ConfigError(
            f"{module} is not installed; pip install ismet[{module}]"
        ) from exc


def _floated(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: _floated(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_floated(item) for item in value]
    return value


def _floatify(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        for key, value in row.items():
            row[key] = _floated(value)


def _decimals(value: Any, path: str) -> Iterator[tuple[str, Decimal]]:
    """Every decimal in a value, named by column and, inside a book, by field."""
    if isinstance(value, Decimal):
        yield path, value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from _decimals(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _decimals(item, path)


def _require_polars_width(rows: list[dict[str, Any]]) -> None:
    """Polars gives a decimal column one scale, so digits are counted per column."""
    integers: dict[str, int] = {}
    scales: dict[str, int] = {}
    for row in rows:
        for column, cell in row.items():
            for key, value in _decimals(cell, column):
                _, digits, exponent = value.as_tuple()
                assert isinstance(exponent, int)
                integers[key] = max(integers.get(key, 0), len(digits) + exponent)
                scales[key] = max(scales.get(key, 0), -exponent)
    for key, integer in integers.items():
        if integer + scales[key] > _POLARS_DECIMAL_DIGITS:
            raise ValidationError(
                f"polars cannot hold column {key} exactly: {integer} integer and "
                f"{scales[key]} fractional digits exceed {_POLARS_DECIMAL_DIGITS}; "
                "pass lossy_float=True for floats, or use to_pandas to keep every digit"
            )


def _has_integer_gap(values: list[Any]) -> bool:
    kinds = {type(value) for value in values}
    return kinds == {int, type(None)}


def to_pandas(items: Iterable[BaseModel], *, lossy_float: bool = False) -> Any:
    """A pandas DataFrame. Decimals stay exact unless ``lossy_float`` is set.

    An integer column with a missing value stays exact too, as an object column: pandas
    would otherwise make it float, which cannot hold a nanosecond timestamp.
    """
    pd = _optional("pandas")
    rows = to_records(items)
    if lossy_float:
        _floatify(rows)
    frame = pd.DataFrame(rows)
    for column in frame.columns:
        values = [row.get(column) for row in rows]
        if _has_integer_gap(values):
            frame[column] = pd.Series(values, dtype=object)
    return frame


def to_polars(items: Iterable[BaseModel], *, lossy_float: bool = False) -> Any:
    """A polars DataFrame. Decimals stay exact unless ``lossy_float`` is set.

    Polars holds at most 38 digits in a decimal column, counting the column's widest
    integer part and its widest fraction together, and an adjusted price can have up to
    50. Such a column raises :class:`~ismet.errors.ValidationError`, rather than being
    rounded or nulled unasked: pass ``lossy_float`` to take floats, or use pandas, which
    keeps every digit.
    """
    pl = _optional("polars")
    rows = to_records(items)
    if lossy_float:
        _floatify(rows)
    _require_polars_width(rows)
    return pl.DataFrame(rows, infer_schema_length=None)


class BarSeries(list[Bar]):
    """A list of bars that can convert itself to a dataframe."""

    def to_records(self) -> list[dict[str, Any]]:
        return to_records(self)

    def to_pandas(self, *, lossy_float: bool = False) -> Any:
        return to_pandas(self, lossy_float=lossy_float)

    def to_polars(self, *, lossy_float: bool = False) -> Any:
        return to_polars(self, lossy_float=lossy_float)


__all__ = ["BarSeries", "to_pandas", "to_polars", "to_records"]
