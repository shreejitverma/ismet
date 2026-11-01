"""Hand data to dataframes without silently losing precision.

Prices stay ``Decimal`` by default: an object column in pandas, a decimal column in
polars. A float column loses money precision, so it is available only when asked for
by name. ``pandas`` and ``polars`` are optional extras, imported on use.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterable
from decimal import Decimal
from typing import Any

from pydantic import BaseModel

from ismet.errors import ConfigError, ValidationError
from ismet.models import Bar

_DROPPED = {"symbol"}


def to_records(items: Iterable[BaseModel]) -> list[dict[str, Any]]:
    """Flat dictionaries, one per model, with ``symbol`` and ``venue`` as strings.

    ``quality`` is its flags sorted and comma-joined, empty when clean. ``source`` is
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
            row["quality"] = ",".join(sorted(flag.value for flag in row["quality"]))
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


def _floatify(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        for key, value in row.items():
            if isinstance(value, Decimal):
                row[key] = float(value)


def to_pandas(items: Iterable[BaseModel], *, lossy_float: bool = False) -> Any:
    """A pandas DataFrame. Decimals stay exact unless ``lossy_float`` is set."""
    pd = _optional("pandas")
    rows = to_records(items)
    if lossy_float:
        _floatify(rows)
    return pd.DataFrame(rows)


def to_polars(items: Iterable[BaseModel], *, lossy_float: bool = False) -> Any:
    """A polars DataFrame. Decimals stay exact unless ``lossy_float`` is set.

    Polars holds at most 38 digits in a decimal column, and an adjusted price can have
    up to 50. Such a value raises :class:`~ismet.errors.ValidationError`, rather than
    being rounded unasked: pass ``lossy_float`` to take floats, or use pandas, which
    keeps every digit.
    """
    pl = _optional("polars")
    rows = to_records(items)
    if lossy_float:
        _floatify(rows)
    try:
        return pl.DataFrame(rows, infer_schema_length=None)
    except (RuntimeError, pl.exceptions.PolarsError) as exc:
        if "decimal" not in str(exc).lower():
            raise
        raise ValidationError(
            f"polars cannot hold these decimals exactly ({exc}); pass lossy_float=True "
            "for floats, or use to_pandas to keep every digit"
        ) from exc


class BarSeries(list[Bar]):
    """A list of bars that can convert itself to a dataframe."""

    def to_records(self) -> list[dict[str, Any]]:
        return to_records(self)

    def to_pandas(self, *, lossy_float: bool = False) -> Any:
        return to_pandas(self, lossy_float=lossy_float)

    def to_polars(self, *, lossy_float: bool = False) -> Any:
        return to_polars(self, lossy_float=lossy_float)


__all__ = ["BarSeries", "to_pandas", "to_polars", "to_records"]
