"""Metrics hooks. ismet emits; the application decides where numbers go.

:class:`Metrics` is small enough to bind to Prometheus, StatsD, or OpenTelemetry in a
few lines. Metric names are stable and documented in ``docs/OPERATIONS.md``.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from typing import Protocol, runtime_checkable

from ismet.execution.reconcile import ReconBreak
from ismet.execution.state import OrderState
from ismet.models.orders import OrderEvent
from ismet.transport.http import Hook, RequestInfo

Labels = Mapping[str, str]


@runtime_checkable
class Metrics(Protocol):
    def counter(
        self, name: str, value: float = 1, labels: Labels | None = None
    ) -> None: ...

    def gauge(self, name: str, value: float, labels: Labels | None = None) -> None: ...

    def histogram(
        self, name: str, value: float, labels: Labels | None = None
    ) -> None: ...


def _key(name: str, labels: Labels | None) -> tuple[str, tuple[tuple[str, str], ...]]:
    return (name, tuple(sorted((labels or {}).items())))


class InMemoryMetrics:
    """Collects metrics in dictionaries, for tests and small deployments."""

    def __init__(self) -> None:
        self.counters: defaultdict[tuple[str, tuple[tuple[str, str], ...]], float] = (
            defaultdict(float)
        )
        self.gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self.histograms: defaultdict[
            tuple[str, tuple[tuple[str, str], ...]], list[float]
        ] = defaultdict(list)

    def counter(
        self, name: str, value: float = 1, labels: Labels | None = None
    ) -> None:
        self.counters[_key(name, labels)] += value

    def gauge(self, name: str, value: float, labels: Labels | None = None) -> None:
        self.gauges[_key(name, labels)] = value

    def histogram(self, name: str, value: float, labels: Labels | None = None) -> None:
        self.histograms[_key(name, labels)].append(value)

    def count(self, name: str, **labels: str) -> float:
        return self.counters.get(_key(name, labels), 0.0)


def http_hook(metrics: Metrics, *, provider: str) -> Hook:
    """A hook for :class:`~ismet.transport.http.HttpTransport`.

    Paths are left out of the labels, since they can carry identifiers that would
    make metric cardinality unbounded.
    """

    def hook(info: RequestInfo) -> None:
        labels = {
            "provider": provider,
            "method": info.method,
            "outcome": "error" if info.error is not None else "ok",
        }
        metrics.counter("ismet_http_requests_total", labels=labels)
        metrics.histogram("ismet_http_latency_seconds", info.latency, labels=labels)

    return hook


def order_event_hook(metrics: Metrics):  # type: ignore[no-untyped-def]
    """An ``on_event`` hook for :class:`~ismet.execution.OrderManager`."""

    def hook(event: OrderEvent, state: OrderState) -> None:
        metrics.counter("ismet_order_events_total", labels={"kind": event.kind})
        if state.is_terminal:
            metrics.counter(
                "ismet_orders_terminal_total", labels={"status": state.status.value}
            )

    return hook


def break_hook(metrics: Metrics):  # type: ignore[no-untyped-def]
    """An ``on_break`` hook for :class:`~ismet.execution.OrderManager`."""

    def hook(item: ReconBreak) -> None:
        metrics.counter("ismet_recon_breaks_total", labels={"kind": item.kind.value})

    return hook


__all__ = [
    "InMemoryMetrics",
    "Metrics",
    "break_hook",
    "http_hook",
    "order_event_hook",
]
