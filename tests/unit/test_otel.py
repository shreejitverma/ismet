"""ismet metrics reach an OpenTelemetry exporter with their names, units, and labels.

The oracle is the OpenTelemetry SDK's own in-memory reader, which sees what any
configured exporter would be handed.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("opentelemetry.sdk.metrics")

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from ismet.execution.reconcile import BreakKind, ReconBreak
from ismet.observability import Metrics, break_hook
from ismet.otel import OpenTelemetryMetrics


def recorder() -> tuple[OpenTelemetryMetrics, InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    return OpenTelemetryMetrics(provider.get_meter("test")), reader


def exported(reader: InMemoryMetricReader) -> dict[str, Any]:
    data = reader.get_metrics_data()
    assert data is not None
    return {
        metric.name: metric
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }


def points(metric: Any) -> dict[tuple[tuple[str, str], ...], Any]:
    return {
        tuple(sorted(point.attributes.items())): point
        for point in metric.data.data_points
    }


def test_it_is_an_ismet_metrics_sink() -> None:
    metrics, _ = recorder()
    assert isinstance(metrics, Metrics)


def test_counters_add_up_per_label_set() -> None:
    metrics, reader = recorder()
    metrics.counter("ismet_http_requests_total", labels={"outcome": "ok"})
    metrics.counter("ismet_http_requests_total", 2, labels={"outcome": "ok"})
    metrics.counter("ismet_http_requests_total", labels={"outcome": "error"})
    found = points(exported(reader)["ismet_http_requests_total"])
    assert found[(("outcome", "ok"),)].value == 3
    assert found[(("outcome", "error"),)].value == 1


def test_a_gauge_keeps_the_last_value() -> None:
    metrics, reader = recorder()
    metrics.gauge("ismet_open_orders", 4)
    metrics.gauge("ismet_open_orders", 2)
    assert points(exported(reader)["ismet_open_orders"])[()].value == 2


def test_a_histogram_in_seconds_is_exported_in_seconds() -> None:
    metrics, reader = recorder()
    for latency in (0.1, 0.3):
        metrics.histogram("ismet_http_latency_seconds", latency)
    metric = exported(reader)["ismet_http_latency_seconds"]
    assert metric.unit == "s"
    point = points(metric)[()]
    assert point.count == 2
    assert point.sum == pytest.approx(0.4)


def test_a_counter_cannot_go_down() -> None:
    metrics, _ = recorder()
    with pytest.raises(ValueError, match="cannot go down"):
        metrics.counter("ismet_recon_breaks_total", -1)


def test_the_ismet_hooks_feed_it() -> None:
    metrics, reader = recorder()
    hook = break_hook(metrics)
    hook(ReconBreak(BreakKind.FILL_MISMATCH, "C1", "two fills, one booked"))
    found = points(exported(reader)["ismet_recon_breaks_total"])
    assert found[(("kind", BreakKind.FILL_MISMATCH.value),)].value == 1


def test_without_a_configured_provider_nothing_is_raised() -> None:
    metrics = OpenTelemetryMetrics()
    metrics.counter("ismet_order_events_total", labels={"kind": "filled"})
    metrics.gauge("ismet_open_orders", 1)
    metrics.histogram("ismet_http_latency_seconds", 0.2)
