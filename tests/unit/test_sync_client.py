from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time
from datetime import date, datetime, timedelta, timezone

import pytest

from ismet import IsmetSyncClient, Symbol
from ismet.errors import ConfigError, TransportError, ValidationError
from ismet.providers.mock import MockProvider
from ismet.sync import as_datetime


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
        as_datetime("2026-03-09T10:00:00", datetime.now(timezone.utc))


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
    empty = tmp_path / "empty.toml"
    empty.write_text("", encoding="utf-8")
    monkeypatch.setenv("ISMET_CONFIG_FILE", str(empty))
    with IsmetSyncClient.from_env() as client:
        assert client.quote("ACME@XMOK").last is not None


def test_a_stream_held_across_close_and_reopen_raises_rather_than_ending() -> None:
    client = IsmetSyncClient([MockProvider()])
    client.open()
    stream = client.stream_quotes(["ACME"], venue="XMOK")
    next(stream)
    client.close()
    client.open()
    try:
        with pytest.raises(ConfigError, match="closed during this stream"):
            list(stream)
    finally:
        client.close()


def test_a_stream_the_consumer_left_does_not_fail_a_later_close() -> None:
    client = IsmetSyncClient([MockProvider()])
    client.open()
    stream = client.stream_quotes(["ACME"], venue="XMOK")
    next(stream)
    stream.close()
    client.close()
    assert client._streams == {}


def test_a_stop_that_times_out_still_stops_the_loop(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("ismet.sync._JOIN_TIMEOUT", 0.2)
    client = IsmetSyncClient([MockProvider()])
    client.open()
    thread = client._thread
    assert thread is not None

    async def stubborn() -> None:
        while True:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                await asyncio.sleep(0.05)
                continue

    assert client._loop is not None
    pending = asyncio.run_coroutine_threadsafe(stubborn(), client._loop)
    with pytest.raises(TimeoutError):
        client.close()
    assert client._thread is None and client._loop is None
    thread.join(3)
    assert not thread.is_alive()
    pending.cancel()


class _FailsWhileTheConsumerLooks(MockProvider):
    """A stream that fails only once the test says so, with nothing delivered."""

    def __init__(self, release: threading.Event) -> None:
        super().__init__()
        self._release = release

    async def stream_trades(self, symbols):  # type: ignore[no-untyped-def,override]
        await asyncio.to_thread(self._release.wait, 10)
        raise TransportError("the feed died", retryable=False)
        yield  # makes this an async generator


def test_a_stream_failing_just_as_the_consumer_checks_still_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The failure lands after the consumer saw no failure, but before it saw an end."""
    release = threading.Event()
    consumer = threading.current_thread()

    class Interleaved(threading.Event):
        def is_set(self) -> bool:
            if threading.current_thread() is consumer:
                release.set()
                self.wait(10)
            return super().is_set()

    with IsmetSyncClient([_FailsWhileTheConsumerLooks(release)]) as client:
        monkeypatch.setattr(threading, "Event", Interleaved)
        stream = client.stream_trades(["ACME@XMOK"])
        monkeypatch.undo()
        with pytest.raises(TransportError, match="the feed died"):
            next(stream)


@pytest.mark.parametrize("buffer", [0, -1])
def test_a_buffer_that_would_be_unbounded_is_refused(buffer: int) -> None:
    # queue.Queue treats maxsize <= 0 as unbounded, which would drop the backpressure.
    with IsmetSyncClient([MockProvider()]) as client:
        with pytest.raises(ValidationError, match="buffer"):
            client.stream_quotes(["ACME"], venue="XMOK", buffer=buffer)
        with pytest.raises(ValidationError, match="buffer"):
            client.stream_trades(["ACME"], venue="XMOK", buffer=buffer)


def test_an_iterator_never_consumed_starts_nothing() -> None:
    started: list[object] = []

    class Recording(MockProvider):
        async def stream_quotes(self, symbols):  # type: ignore[no-untyped-def,override]
            started.append(symbols)
            async for quote in super().stream_quotes(symbols):
                yield quote

    with IsmetSyncClient([Recording()]) as client:
        unused = client.stream_quotes(["ACME"], venue="XMOK", buffer=1)
        time.sleep(0.1)
        assert started == []
        del unused
        used = client.stream_quotes(["ACME"], venue="XMOK", buffer=1)
        assert next(used).symbol.ticker == "ACME"
        used.close()
    assert len(started) == 1


class _SlowToClose(MockProvider):
    """A provider whose close waits for the test, so a close can be caught midway."""

    def __init__(self) -> None:
        super().__init__(tick_interval=0)
        self.closing = threading.Event()
        self.may_finish = threading.Event()

    async def close(self) -> None:
        self.closing.set()
        await asyncio.to_thread(self.may_finish.wait)
        await super().close()


def _closing_client() -> tuple[IsmetSyncClient, _SlowToClose, threading.Thread]:
    provider = _SlowToClose()
    client = IsmetSyncClient([provider])
    client.open()
    closer = threading.Thread(target=client.close)
    return client, provider, closer


def test_a_stream_first_consumed_during_a_close_raises() -> None:
    client, provider, closer = _closing_client()
    stream = client.stream_quotes(["ACME@XMOK"])
    closer.start()
    try:
        assert provider.closing.wait(10)
        with pytest.raises(ConfigError, match="not open"):
            next(stream)
    finally:
        provider.may_finish.set()
        closer.join(10)
    assert sync_threads() == []


def test_a_call_made_during_a_close_raises_and_does_not_hang() -> None:
    client, provider, closer = _closing_client()
    closer.start()
    try:
        assert provider.closing.wait(10)
        with pytest.raises(ConfigError, match="not open"):
            client.quote("ACME@XMOK")
    finally:
        provider.may_finish.set()
        closer.join(10)
    assert sync_threads() == []


def test_a_stream_already_running_when_close_begins_raises() -> None:
    client, provider, closer = _closing_client()
    stream = client.stream_quotes(["ACME@XMOK"])
    assert next(stream).symbol.ticker == "ACME"
    closer.start()
    assert provider.closing.wait(10)
    provider.may_finish.set()
    with pytest.raises(ConfigError, match="closed"):
        for _ in stream:
            pass
    closer.join(10)
    assert sync_threads() == []


def test_a_producer_that_died_before_it_ran_does_not_hang_the_consumer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Its cleanup never ran, so nothing else would ever end the consumer."""
    outcome: list[object] = []

    def never_ran(coroutine, loop):  # type: ignore[no-untyped-def]
        coroutine.close()
        future: concurrent.futures.Future[None] = concurrent.futures.Future()
        future.cancel()
        return future

    def consume(stream) -> None:  # type: ignore[no-untyped-def]
        try:
            outcome.append(list(stream))
        except ConfigError as exc:
            outcome.append(exc)

    with IsmetSyncClient([MockProvider()]) as client:
        stream = client.stream_quotes(["ACME@XMOK"])
        monkeypatch.setattr("ismet.sync.asyncio.run_coroutine_threadsafe", never_ran)
        consumer = threading.Thread(target=consume, args=(stream,), daemon=True)
        consumer.start()
        consumer.join(10)
        monkeypatch.undo()
        assert not consumer.is_alive()
    assert len(outcome) == 1 and isinstance(outcome[0], ConfigError)


def test_closing_twice_at_once_is_safe() -> None:
    client, provider, closer = _closing_client()
    closer.start()
    assert provider.closing.wait(10)
    client.close()
    provider.may_finish.set()
    closer.join(10)
    client.close()
    assert sync_threads() == []


def test_a_closed_client_can_be_opened_again() -> None:
    client = IsmetSyncClient([MockProvider()])
    with client:
        client.quote("ACME@XMOK")
    with client:
        assert client.quote("ACME@XMOK").bid is not None
        assert next(client.stream_quotes(["ACME@XMOK"])).bid is not None
    assert sync_threads() == []


def test_a_call_blocked_during_close_raises_a_config_error() -> None:
    entered = threading.Event()

    class Blocks(MockProvider):
        async def quote(self, symbol: Symbol):  # type: ignore[no-untyped-def]
            entered.set()
            await asyncio.Event().wait()

    client = IsmetSyncClient([Blocks()])
    client.open()
    outcome: list[BaseException] = []

    def call() -> None:
        try:
            client.quote("ACME@XMOK")
        except BaseException as exc:
            outcome.append(exc)

    thread = threading.Thread(target=call)
    thread.start()
    assert entered.wait(10)
    client.close()
    thread.join(10)
    assert not thread.is_alive()
    assert len(outcome) == 1 and isinstance(outcome[0], ConfigError)
    assert "closed" in str(outcome[0])
