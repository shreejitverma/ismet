"""ismet metrics recorded through OpenTelemetry. Needs ``pip install ismet[otel]``.

:class:`OpenTelemetryMetrics` implements :class:`~ismet.observability.Metrics`, so
the hooks in :mod:`ismet.observability` feed whatever exporter the application has
configured on its meter provider. Metric names stay as ismet documents them in
``docs/OPERATIONS.md``, so dashboards keep working whichever backend records them.
ismet configures no provider or exporter: without one the OpenTelemetry API records
nothing, which is its documented default.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any, Protocol

from opentelemetry import metrics as otel

from ismet import __version__
from ismet.observability import Labels


class _Instrument(Protocol):
    def __call__(self, value: float, attributes: dict[str, str]) -> None: ...


def _unit(name: str) -> str:
    """The UCUM unit OpenTelemetry expects, read from ismet's name suffix."""
    if name.endswith("_seconds"):
        return "s"
    if name.endswith("_bytes"):
        return "By"
    return ""


class OpenTelemetryMetrics:
    """Record ismet's counters, gauges, and histograms on an OpenTelemetry meter.

    ``meter`` defaults to the global provider's meter named ``ismet``. Instruments
    are created on first use and reused, so a name always maps to one instrument.
    Counters only go up, as OpenTelemetry requires; a negative value is refused.
    """

    def __init__(self, meter: otel.Meter | None = None) -> None:
        self._meter = meter or otel.get_meter("ismet", __version__)
        self._instruments: dict[tuple[str, str], _Instrument] = {}
        self._lock = threading.Lock()

    def _instrument(
        self, kind: str, name: str, create: Callable[..., Any], record: str
    ) -> _Instrument:
        key = (kind, name)
        found = self._instruments.get(key)
        if found is None:
            with self._lock:
                found = self._instruments.get(key)
                if found is None:
                    made = create(name, unit=_unit(name))
                    found = getattr(made, record)
                    self._instruments[key] = found
        return found

    def counter(
        self, name: str, value: float = 1, labels: Labels | None = None
    ) -> None:
        if value < 0:
            raise ValueError(f"counter {name} cannot go down by {value}")
        add = self._instrument("counter", name, self._meter.create_counter, "add")
        add(value, dict(labels or {}))

    def gauge(self, name: str, value: float, labels: Labels | None = None) -> None:
        set_ = self._instrument("gauge", name, self._meter.create_gauge, "set")
        set_(value, dict(labels or {}))

    def histogram(self, name: str, value: float, labels: Labels | None = None) -> None:
        record = self._instrument(
            "histogram", name, self._meter.create_histogram, "record"
        )
        record(value, dict(labels or {}))


__all__ = ["OpenTelemetryMetrics"]
