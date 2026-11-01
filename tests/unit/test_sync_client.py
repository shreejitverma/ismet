from __future__ import annotations

import asyncio
import threading
from datetime import date, datetime, timedelta, timezone

import pytest

from ismet import IsmetSyncClient, Symbol
from ismet.errors import ConfigError, TransportError, ValidationError
from ismet.providers.mock import MockProvider
from ismet.sync import _as_datetime


def sync_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == "ismet-sync"]


def test_context_manager_serves_calls_and_leaves_no_thread() -> None:
    with IsmetSyncClient([MockProvider()]) as client:
        assert len(sync_threads()) == 1
        assert client.quote("ACME", venue="XMOK").bid is not None
        assert len(client.order_book("ACME@XMOK", depth=2).bids) == 2
        assert client.instrument("ACME@XMOK").tick_size
        assert [i.symbol.ticker for i in client.search("acme", venue="XMOK")] == [
            "ACME"
        ]
        assert client.capabilities("mock")
        assert client.calendar("XNYS").venue == "XNYS"
    assert sync_threads() == []


def test_bars_accept_dates_strings_and_datetimes_and_export() -> None:
    with IsmetSyncClient([MockProvider()]) as client:
        bars = client.bars(
            "ACME@XMOK", interval="1h", start="2026-03-09", end=date(2026, 3, 10)
        )
        assert len(bars) == 24
        assert bars.to_records()[0]["ticker"] == "ACME"
        recent = client.bars(
            "ACME@XMOK",
            interval="1h",
            start=datetime.now(timezone.utc) - timedelta(hours=3),
        )
        assert len(recent) >= 2


def test_naive_datetimes_are_rejected() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        _as_datetime("2026-03-09T10:00:00", datetime.now(timezone.utc))


def test_works_from_inside_a_running_event_loop() -> None:
    """Jupyter has a running loop, so the facade must not call asyncio.run."""

    async def inside() -> bool:
        with IsmetSyncClient([MockProvider()]) as client:
            return client.quote("ACME@XMOK").bid is not None

    assert asyncio.run(inside())


def test_stream_is_a_blocking_iterator_and_stops_when_abandoned() -> None:
    with IsmetSyncClient([MockProvider(tick_interval=0)]) as client:
        taken = []
        for quote in client.stream_quotes(["ACME"], venue="XMOK", buffer=2):
            taken.append(quote)
            if len(taken) == 5:
                break
        assert [q.sequence for q in taken] == sorted(q.sequence for q in taken)
        for trade in client.stream_trades([Symbol(venue="XMOK", ticker="ACME")]):
            assert trade.price is not None
            break
    assert sync_threads() == []


def test_stream_errors_reach_the_caller() -> None:
    class Broken(MockProvider):
        name = "broken"

        async def stream_quotes(self, symbols):  # type: ignore[no-untyped-def,override]
            yield await self.quote(symbols[0])
            raise TransportError("feed died")

    with (
        IsmetSyncClient([Broken()]) as client,
        pytest.raises(TransportError, match="feed died"),
    ):
        list(client.stream_quotes(["ACME"], venue="XMOK"))


def test_finite_stream_ends_cleanly() -> None:
    class Finite(MockProvider):
        name = "finite"

        async def stream_trades(self, symbols):  # type: ignore[no-untyped-def,override]
            return
            yield

    with IsmetSyncClient([Finite()]) as client:
        assert list(client.stream_trades(["ACME"], venue="XMOK")) == []


def test_use_before_open_is_refused_and_close_is_idempotent() -> None:
    client = IsmetSyncClient([MockProvider()])
    with pytest.raises(ConfigError, match="not open"):
        client.quote("ACME@XMOK")
    with pytest.raises(ConfigError, match="not open"):
        next(client.stream_quotes(["ACME"], venue="XMOK"))
    client.close()
    client.open()
    client.open()
    client.close()
    client.close()
    assert sync_threads() == []


def test_failed_open_leaves_no_thread() -> None:
    class CannotOpen(MockProvider):
        name = "cannotopen"

        async def open(self) -> None:
            raise TransportError("no route")

    client = IsmetSyncClient([CannotOpen()])
    with pytest.raises(TransportError):
        client.open()
    assert sync_threads() == []


def test_from_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("ISMET_PROVIDERS", "mock")
    monkeypatch.setenv("ISMET_CONFIG_FILE", str(tmp_path / "none.toml"))
    with IsmetSyncClient.from_env() as client:
        assert client.quote("ACME@XMOK").last is not None
