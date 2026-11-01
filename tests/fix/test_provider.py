from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar

import pytest
from pydantic import SecretStr

from ismet.capabilities import Capability, TradingCapability
from ismet.config import ProviderSettings
from ismet.errors import (
    AmbiguousOutcome,
    AuthError,
    ConfigError,
    DuplicateOrderId,
    TransportError,
    VenueError,
)
from ismet.fix.message import encode_fields, format_utc_timestamp
from ismet.fix.provider import FixDialect, FixProvider, FixTags, ReportQuarantinedError
from ismet.fix.session import SessionState
from ismet.fix.store import FixStore, FixStoreError, MemoryFixStore, SqliteFixStore
from ismet.models import Symbol
from ismet.models.orders import (
    Acknowledged,
    Canceled,
    CancelRejected,
    Environment,
    EventOrigin,
    Expired,
    FillBusted,
    Filled,
    Liquidity,
    OrderEvent,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    Rejected,
    Replaced,
    ReplaceRejected,
    Restated,
    ShortSaleMark,
    TimeInForce,
)

from .acceptor import FixAcceptor
from .conftest import eventually, session_config

ACME = Symbol(venue="XNAS", ticker="ACME")


def _now_text() -> str:
    return format_utc_timestamp(datetime.now(timezone.utc))


ProviderFactory = Callable[..., Awaitable[FixProvider]]


def order(cid: str = "C1", **kw: Any) -> OrderRequest:
    fields: dict[str, Any] = {
        "client_order_id": cid,
        "symbol": ACME,
        "side": OrderSide.BUY,
        "quantity": "100",
        "limit_price": "10.25",
    }
    fields.update(kw)
    return OrderRequest.model_validate(fields)


@pytest.fixture
async def make_provider(
    acceptor: FixAcceptor,
) -> AsyncIterator[ProviderFactory]:
    providers: list[FixProvider] = []

    async def build(
        *,
        store: FixStore | None = None,
        dialect: FixDialect | None = None,
        target: FixAcceptor | None = None,
        open_now: bool = True,
        ack_timeout: float = 2.0,
        query_timeout: float = 2.0,
        **session_options: Any,
    ) -> FixProvider:
        provider = FixProvider(
            session_config(target or acceptor, **session_options),
            store if store is not None else MemoryFixStore(),
            dialect=dialect,
            ack_timeout=ack_timeout,
            query_timeout=query_timeout,
            logon_timeout=5.0,
        )
        providers.append(provider)
        if open_now:
            await provider.open()
        return provider

    yield build
    for provider in providers:
        await provider.close()


class Stream:
    """Collects a provider's execution stream in a background task."""

    def __init__(self, provider: FixProvider) -> None:
        self.events: list[OrderEvent] = []
        self.error: BaseException | None = None
        self._task = asyncio.create_task(self._run(provider))

    async def _run(self, provider: FixProvider) -> None:
        try:
            async for event in provider.stream_executions():
                self.events.append(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.error = exc

    async def close(self) -> None:
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task

    def of(self, kind: type[Any]) -> list[Any]:
        return [e for e in self.events if isinstance(e, kind)]


@pytest.fixture
async def stream_of() -> AsyncIterator[Callable[[FixProvider], Stream]]:
    streams: list[Stream] = []

    def build(provider: FixProvider) -> Stream:
        stream = Stream(provider)
        streams.append(stream)
        return stream

    yield build
    for stream in streams:
        await stream.close()


class TestContract:
    async def test_implements_trading_capability(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(open_now=False)
        assert isinstance(provider, TradingCapability)
        assert provider.supports(Capability.TRADING)

    async def test_environment_is_live_by_default_and_opt_out_is_explicit(
        self, acceptor: FixAcceptor
    ) -> None:
        store = MemoryFixStore()
        config = session_config(acceptor)
        assert FixProvider(config, store).environment is Environment.LIVE
        paper = FixProvider(config, store, environment=Environment.PAPER)
        assert paper.environment is Environment.PAPER

    def test_a_venue_subclass_declares_itself(self) -> None:
        class Broker(FixProvider):
            name: ClassVar[str] = "broker"
            venues: ClassVar[frozenset[str]] = frozenset({"xnas"})

        assert Broker.venues == frozenset({"XNAS"})

    async def test_rules_come_from_the_dialect(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(open_now=False)
        rules = provider.trading_rules()
        assert OrderType.STOP_LIMIT in rules.order_types
        assert TimeInForce.CLS in rules.time_in_force
        assert rules.supports_amend
        assert rules.supports_short
        assert rules.client_order_id_max_length < 64

    async def test_at_the_close_is_not_offered_on_fix_42(
        self, make_provider: ProviderFactory
    ) -> None:
        old = FixAcceptor("FIX.4.2")
        await old.start()
        provider = await make_provider(target=old, open_now=False)
        assert TimeInForce.CLS not in provider.trading_rules().time_in_force
        assert TimeInForce.DAY in provider.trading_rules().time_in_force
        await old.stop()


class TestFromSettings:
    def test_requires_its_options(self) -> None:
        with pytest.raises(ConfigError, match="missing options"):
            FixProvider.from_settings(ProviderSettings(name="fix"))

    async def test_builds_with_a_secret_password(self, tmp_path: Path) -> None:
        settings = ProviderSettings(
            name="fix",
            credentials={"password": SecretStr("hunter2")},
            options={
                "host": "127.0.0.1",
                "port": 9,
                "begin_string": "FIX.4.4",
                "sender_comp_id": "A",
                "target_comp_id": "B",
                "store_path": str(tmp_path / "fix.db"),
            },
        )
        provider = FixProvider.from_settings(settings)
        assert provider.environment is Environment.LIVE
        assert "hunter2" not in repr(provider)
        assert "hunter2" not in repr(provider.session.config)
        await provider.close()

    async def test_environment_can_opt_out(self, tmp_path: Path) -> None:
        provider = FixProvider.from_settings(
            ProviderSettings(
                name="fix",
                options={
                    "host": "127.0.0.1",
                    "port": 9,
                    "begin_string": "FIX.4.4",
                    "sender_comp_id": "A",
                    "target_comp_id": "B",
                    "store_path": str(tmp_path / "fix.db"),
                    "environment": "paper",
                },
            )
        )
        assert provider.environment is Environment.PAPER
        await provider.close()


class TestNewOrderMapping:
    async def test_limit_order_fields(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        ack = await provider.place_order(order(account="ACC1"))
        assert ack.client_order_id == "C1"
        assert ack.venue_order_id == "O1"
        wire = acceptor.of_type("D")[0]
        assert wire.get(11) == "C1"
        assert wire.get(1) == "ACC1"
        assert wire.get(21) == "1"
        assert wire.get(55) == "ACME"
        assert wire.get(54) == "1"
        assert wire.get(38) == "100"
        assert wire.get(40) == "2"
        assert wire.get(44) == "10.25"
        assert wire.get(59) == "0"
        assert wire.get(60) is not None
        assert wire.get(97) is None

    @pytest.mark.parametrize(
        ("request_fields", "expected"),
        [
            (
                {"order_type": OrderType.MARKET, "limit_price": None},
                {40: "1", 44: None},
            ),
            (
                {
                    "order_type": OrderType.STOP,
                    "limit_price": None,
                    "stop_price": "9.5",
                },
                {40: "3", 99: "9.5", 44: None},
            ),
            (
                {"order_type": OrderType.STOP_LIMIT, "stop_price": "9.5"},
                {40: "4", 99: "9.5", 44: "10.25"},
            ),
            ({"time_in_force": TimeInForce.GTC}, {59: "1"}),
            ({"time_in_force": TimeInForce.IOC}, {59: "3"}),
            ({"time_in_force": TimeInForce.FOK}, {59: "4"}),
            ({"time_in_force": TimeInForce.OPG}, {59: "2"}),
            ({"time_in_force": TimeInForce.CLS}, {59: "7"}),
            ({"post_only": True}, {18: "6"}),
            ({"side": OrderSide.SELL}, {54: "2"}),
            ({"side": OrderSide.SELL, "short_sale": ShortSaleMark.SHORT}, {54: "5"}),
            (
                {"side": OrderSide.SELL, "short_sale": ShortSaleMark.SHORT_EXEMPT},
                {54: "6"},
            ),
        ],
    )
    async def test_field_mapping(
        self,
        acceptor: FixAcceptor,
        make_provider: ProviderFactory,
        request_fields: dict[str, Any],
        expected: dict[int, str | None],
    ) -> None:
        provider = await make_provider()
        await provider.place_order(order(**request_fields))
        wire = acceptor.of_type("D")[0]
        for tag, value in expected.items():
            assert wire.get(tag) == value, tag

    async def test_gtd_sends_expire_time(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        expiry = datetime(2030, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        await provider.place_order(
            order(time_in_force=TimeInForce.GTD, expire_at=expiry)
        )
        wire = acceptor.of_type("D")[0]
        assert wire.get(59) == "6"
        assert wire.get(126) == "20300102-03:04:05.000"

    async def test_a_decimal_reaches_the_wire_exactly(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await provider.place_order(
            order(quantity="0.123456789012345678", limit_price="1234567.000000000001")
        )
        wire = acceptor.of_type("D")[0]
        assert wire.get(38) == "0.123456789012345678"
        assert wire.get(44) == "1234567.000000000001"

    async def test_reduce_only_cannot_be_silently_dropped(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        with pytest.raises(VenueError) as info:
            await provider.place_order(order(reduce_only=True))
        assert info.value.code == "UNSUPPORTED"
        assert acceptor.of_type("D") == []

    async def test_per_version_order_flow(self, begin_string: str) -> None:
        acceptor = FixAcceptor(begin_string)
        await acceptor.start()
        provider = FixProvider(
            session_config(acceptor), MemoryFixStore(), logon_timeout=5.0
        )
        await provider.open()
        stream = Stream(provider)
        ack = await provider.place_order(order())
        assert ack.venue_order_id == "O1"
        exec_id = await acceptor.fill("C1", "40", "10.25")
        await eventually(lambda: bool(stream.of(Filled)))
        fill = stream.of(Filled)[0].fill
        assert fill.fill_id == exec_id
        assert fill.quantity == Decimal(40)
        assert fill.price == Decimal("10.25")
        snapshot = await provider.order_status("C1", ACME)
        assert snapshot is not None
        assert snapshot.status is OrderStatus.PARTIALLY_FILLED
        assert snapshot.filled_quantity == Decimal(40)
        assert await provider.order_status("C1", ACME) is not None
        assert acceptor.of_type("D")[0].begin_string == begin_string
        await stream.close()
        await provider.close()
        await acceptor.stop()


class TestPlaceOrderContract:
    async def test_session_down_before_writing_raises_transport_error(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        store = MemoryFixStore()
        provider = await make_provider(store=store)
        await acceptor.stop()
        await eventually(lambda: not provider.session.is_ready)
        with pytest.raises(TransportError) as info:
            await provider.place_order(order())
        assert not isinstance(info.value, AmbiguousOutcome)
        assert store.get_order("C1") is None
        assert [m.msg_type for m in store.messages(1, 99)] == []

    async def test_never_opened_raises_transport_error(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(open_now=False)
        with pytest.raises(TransportError) as info:
            await provider.place_order(order())
        assert not isinstance(info.value, AmbiguousOutcome)

    async def test_no_ack_before_the_timeout_is_ambiguous_and_never_resent(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        store = MemoryFixStore()
        provider = await make_provider(store=store, ack_timeout=0.1)
        acceptor.faults.lose_ack = 1
        with pytest.raises(AmbiguousOutcome):
            await provider.place_order(order())
        await asyncio.sleep(0.15)
        wires = acceptor.of_type("D")
        assert len(wires) == 1
        assert wires[0].get(43) is None
        assert wires[0].get(97) is None
        assert [m.msg_type for m in store.messages(1, 99)] == ["D"]

    async def test_disconnect_while_waiting_is_ambiguous_at_once(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(ack_timeout=30.0)
        acceptor.faults.lose_ack = 1
        acceptor.faults.drop_after_type["D"] = 1
        with pytest.raises(AmbiguousOutcome):
            await asyncio.wait_for(provider.place_order(order()), 5.0)

    async def test_dropped_before_the_venue_saw_it_is_recovered_by_session_resend(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        """The socket is cut mid-send, so the venue sees the order on the resend."""
        provider = await make_provider(ack_timeout=0.05)
        stream = Stream(provider)
        assert provider.session._writer is not None
        session_writer = provider.session._writer
        real = session_writer.write

        def half_write(data: bytes) -> None:
            real(data[: len(data) // 2])
            session_writer.transport.abort()

        session_writer.write = half_write  # type: ignore[method-assign]
        with pytest.raises(AmbiguousOutcome):
            await provider.place_order(order())
        await provider.session.wait_ready(5.0)
        await eventually(lambda: "C1" in acceptor.orders)
        assert acceptor.orders["C1"].arrivals == 1
        wire = acceptor.of_type("D")[0]
        assert wire.get(43) == "Y"
        assert wire.get(122) is not None
        await eventually(lambda: bool(stream.of(Acknowledged)))
        await stream.close()

    async def test_duplicate_client_order_id_is_refused_locally(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await provider.place_order(order())
        with pytest.raises(VenueError) as info:
            await provider.place_order(order())
        assert info.value.code == "DUPLICATE_CLIENT_ORDER_ID"
        assert len(acceptor.of_type("D")) == 1

    async def test_venue_duplicate_reject_is_typed_and_not_a_rejection(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        acceptor.faults.order_reject = "6"
        with pytest.raises(DuplicateOrderId) as info:
            await provider.place_order(order())
        assert info.value.code == "6"
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        assert stream.of(Rejected) == []
        await stream.close()

    async def test_late_duplicate_reject_of_a_resent_order_spares_the_live_order(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        await acceptor.exec_report("C1", "8", "8", rej_reason="6", text="duplicate")
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        assert stream.of(Rejected) == []
        assert [o.client_order_id for o in await provider.open_orders()] == ["C1"]
        await stream.close()

    async def test_duplicate_after_an_ambiguous_outcome_is_still_refused(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(ack_timeout=0.05)
        acceptor.faults.lose_ack = 1
        with pytest.raises(AmbiguousOutcome):
            await provider.place_order(order())
        with pytest.raises(VenueError):
            await provider.place_order(order())
        assert len(acceptor.of_type("D")) == 1

    async def test_reject_raises_venue_error_with_the_raw_code(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        acceptor.faults.order_reject = "3"
        with pytest.raises(VenueError) as info:
            await provider.place_order(order())
        assert info.value.code == "3"
        await eventually(lambda: bool(stream.of(Rejected)))
        assert stream.of(Rejected)[0].code == "3"
        await stream.close()

    async def test_business_reject_raises_venue_error(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        acceptor.faults.business_reject_orders = "3"
        with pytest.raises(VenueError) as info:
            await provider.place_order(order())
        assert info.value.code == "BUSINESS_3"
        await eventually(lambda: bool(stream.of(Rejected)))
        await stream.close()

    async def test_session_reject_raises_venue_error(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        acceptor.faults.session_reject_orders = "5"
        with pytest.raises(VenueError) as info:
            await provider.place_order(order())
        assert info.value.code == "SESSION_5"
        await eventually(lambda: bool(stream.of(Rejected)))
        await stream.close()

    async def test_late_session_reject_reaches_the_stream(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(ack_timeout=0.05)
        stream = Stream(provider)
        acceptor.faults.lose_ack = 1
        with pytest.raises(AmbiguousOutcome):
            await provider.place_order(order())
        seq = acceptor.of_type("D")[0].require(34)
        await acceptor.send("3", [(45, seq), (373, "5"), (58, "late")])
        await eventually(lambda: bool(stream.of(Rejected)))
        await stream.close()

    async def test_session_reject_of_an_admin_message_is_harmless(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await acceptor.send("3", [(45, "1"), (373, "5")])
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        assert provider.session.is_ready

    async def test_a_fill_acknowledges_the_order(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        acceptor.faults.lose_ack = 1
        task = asyncio.create_task(provider.place_order(order()))
        await acceptor.wait_until(lambda: "C1" in acceptor.orders)
        await acceptor.fill("C1", "100", "10.25")
        ack = await task
        assert ack.venue_order_id == "O1"

    async def test_pending_new_is_not_an_acknowledgement(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(ack_timeout=0.1)
        acceptor.faults.lose_ack = 1
        task = asyncio.create_task(provider.place_order(order()))
        await acceptor.wait_until(lambda: "C1" in acceptor.orders)
        await acceptor.exec_report("C1", "A", "A")
        with pytest.raises(AmbiguousOutcome):
            await task

    async def test_order_accepted_after_pending_new(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        acceptor.faults.lose_ack = 1
        task = asyncio.create_task(provider.place_order(order()))
        await acceptor.wait_until(lambda: "C1" in acceptor.orders)
        await acceptor.exec_report("C1", "A", "A")
        await acceptor.exec_report("C1", "0", "0")
        assert (await task).venue_order_id == "O1"

    async def test_store_failure_means_nothing_was_sent(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory, tmp_path: Path
    ) -> None:
        store = SqliteFixStore(tmp_path / "fix.db", "s")
        provider = await make_provider(store=store)
        store._conn.execute("DROP TABLE fix_outbound")
        with pytest.raises(TransportError) as info:
            await provider.place_order(order())
        assert not isinstance(info.value, AmbiguousOutcome)
        assert acceptor.of_type("D") == []
        assert store.get_order("C1") is None
        await provider.close()
        store.close()


class TestExecutionMapping:
    async def _placed(
        self, make_provider: ProviderFactory, **kw: Any
    ) -> tuple[FixProvider, Stream]:
        provider = await make_provider(**kw)
        stream = Stream(provider)
        await provider.place_order(order())
        await eventually(lambda: bool(stream.of(Acknowledged)))
        return provider, stream

    async def test_new_maps_to_acknowledged(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        _, stream = await self._placed(make_provider)
        event = stream.of(Acknowledged)[0]
        assert event.venue_order_id == "O1"
        assert event.origin is EventOrigin.VENUE
        await stream.close()

    async def test_fill_id_is_exec_id_and_fields_map(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        _, stream = await self._placed(make_provider)
        exec_id = await acceptor.fill(
            "C1",
            "40",
            "10.2000000001",
            extra=[(851, "2"), (13, "3"), (12, "1.5"), (15, "USD")],
        )
        await eventually(lambda: bool(stream.of(Filled)))
        fill = stream.of(Filled)[0].fill
        assert fill.fill_id == exec_id
        assert fill.client_order_id == "C1"
        assert fill.symbol == ACME
        assert fill.side is OrderSide.BUY
        assert fill.price == Decimal("10.2000000001")
        assert fill.quantity == Decimal(40)
        assert fill.venue_order_id == "O1"
        assert fill.liquidity is Liquidity.TAKER
        assert fill.fee == Decimal("1.5")
        assert fill.fee_currency == "USD"
        await stream.close()

    async def test_commission_that_is_not_absolute_is_not_a_fee(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        _, stream = await self._placed(make_provider)
        await acceptor.fill("C1", "40", "10", extra=[(13, "2"), (12, "1.5")])
        await eventually(lambda: bool(stream.of(Filled)))
        assert stream.of(Filled)[0].fill.fee is None
        assert stream.of(Filled)[0].fill.liquidity is Liquidity.UNKNOWN
        await stream.close()

    async def test_maker_liquidity(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        _, stream = await self._placed(make_provider)
        await acceptor.fill("C1", "40", "10", extra=[(851, "1")])
        await eventually(lambda: bool(stream.of(Filled)))
        assert stream.of(Filled)[0].fill.liquidity is Liquidity.MAKER
        await stream.close()

    async def test_duplicate_exec_report_is_dropped(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider, stream = await self._placed(make_provider)
        order_state = acceptor.orders["C1"]
        exec_id = await acceptor.fill("C1", "40", "10")
        await eventually(lambda: bool(stream.of(Filled)))
        # The same report under a new sequence number, as a venue might repeat it.
        seq = max(acceptor.sent)
        fields = acceptor.sent[seq][1]
        await acceptor.send("8", list(fields))
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        assert len(stream.of(Filled)) == 1
        assert exec_id
        assert order_state.filled == Decimal(40)
        await stream.close()

    async def test_possresend_with_a_seen_exec_id_is_dropped_and_new_is_kept(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider, stream = await self._placed(make_provider)
        exec_id = await acceptor.fill("C1", "40", "10")
        await eventually(lambda: bool(stream.of(Filled)))
        seq = max(acceptor.sent)
        await acceptor.send("8", [*acceptor.sent[seq][1], (97, "Y")])
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        assert len(stream.of(Filled)) == 1
        await acceptor.fill("C1", "10", "10", extra=[(97, "Y")])
        await eventually(lambda: len(stream.of(Filled)) == 2)
        assert stream.of(Filled)[0].fill.fill_id == exec_id
        await stream.close()

    async def test_fill_reported_again_with_different_terms_is_forwarded(
        self,
        acceptor: FixAcceptor,
        make_provider: ProviderFactory,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _, stream = await self._placed(make_provider)
        exec_id = await acceptor.fill("C1", "40", "10")
        await eventually(lambda: len(stream.of(Filled)) == 1)
        await acceptor.exec_report(
            "C1",
            "F",
            "1",
            last_qty=Decimal(40),
            last_px=Decimal(11),
            exec_id=exec_id,
        )
        await eventually(lambda: len(stream.of(Filled)) == 2)
        assert stream.of(Filled)[1].fill.price == Decimal(11)
        assert "different content" in caplog.text
        await stream.close()

    async def test_a_duplicate_delivered_by_session_resend_is_dropped_there(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider, stream = await self._placed(make_provider)
        await acceptor.fill("C1", "40", "10")
        await eventually(lambda: bool(stream.of(Filled)))
        seq = max(acceptor.sent)
        await acceptor.send_raw_seq(
            seq,
            "8",
            acceptor.sent[seq][1],
            poss_dup=True,
            orig_sending_time=acceptor.sent[seq][2],
        )
        await acceptor.send("0", [])
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        assert len(stream.of(Filled)) == 1
        await stream.close()

    async def test_trade_cancel_maps_to_fill_busted(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider, stream = await self._placed(make_provider)
        exec_id = await acceptor.fill("C1", "40", "10")
        await eventually(lambda: bool(stream.of(Filled)))
        await acceptor.bust("C1", exec_id, "40")
        await eventually(lambda: bool(stream.of(FillBusted)))
        assert stream.of(FillBusted)[0].fill_id == exec_id
        since = datetime(2000, 1, 1, tzinfo=timezone.utc)
        assert await provider.fills_since(since) == []
        await stream.close()

    async def test_trade_correct_busts_then_fills_under_the_new_exec_id(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider, stream = await self._placed(make_provider)
        first = await acceptor.fill("C1", "40", "10")
        await eventually(lambda: bool(stream.of(Filled)))
        second = await acceptor.correct("C1", first, "40", "30", "9.5")
        await eventually(lambda: len(stream.of(Filled)) == 2)
        assert stream.of(FillBusted)[0].fill_id == first
        corrected = stream.of(Filled)[1].fill
        assert corrected.fill_id == second
        assert corrected.quantity == Decimal(30)
        assert corrected.price == Decimal("9.5")
        since = datetime(2000, 1, 1, tzinfo=timezone.utc)
        assert [f.fill_id for f in await provider.fills_since(since)] == [second]
        await stream.close()

    async def test_fix_42_trade_cancel_and_correct(
        self, make_provider: ProviderFactory
    ) -> None:
        old = FixAcceptor("FIX.4.2")
        await old.start()
        provider = await make_provider(target=old)
        stream = Stream(provider)
        await provider.place_order(order())
        first = await old.fill("C1", "40", "10")
        await eventually(lambda: bool(stream.of(Filled)))
        assert stream.of(Filled)[0].fill.fill_id == first
        second = await old.correct("C1", first, "40", "30", "9")
        await eventually(lambda: len(stream.of(Filled)) == 2)
        await old.bust("C1", second, "30")
        await eventually(lambda: len(stream.of(FillBusted)) == 2)
        assert [e.fill_id for e in stream.of(FillBusted)] == [first, second]
        await stream.close()
        await provider.close()
        await old.stop()

    @pytest.mark.parametrize(
        ("exec_type", "ord_status", "kind"),
        [
            ("4", "4", Canceled),
            ("C", "C", Expired),
            ("3", "3", Expired),
        ],
    )
    async def test_terminal_reports(
        self,
        acceptor: FixAcceptor,
        make_provider: ProviderFactory,
        exec_type: str,
        ord_status: str,
        kind: type[Any],
    ) -> None:
        provider, stream = await self._placed(make_provider)
        await acceptor.exec_report("C1", exec_type, ord_status, text="why")
        await eventually(lambda: bool(stream.of(kind)))
        assert await provider.open_orders() == []
        await stream.close()

    async def test_restated_report(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        _, stream = await self._placed(make_provider)
        await acceptor.exec_report("C1", "D", "0", text="corporate action")
        await eventually(lambda: bool(stream.of(Restated)))
        event = stream.of(Restated)[0]
        assert event.status is OrderStatus.ACCEPTED
        assert event.reason == "corporate action"
        await stream.close()

    @pytest.mark.parametrize("exec_type", ["A", "6", "E", "7", "9", "B", "M"])
    async def test_reports_with_no_order_event(
        self,
        acceptor: FixAcceptor,
        make_provider: ProviderFactory,
        exec_type: str,
    ) -> None:
        provider, stream = await self._placed(make_provider)
        count = len(stream.events)
        await acceptor.exec_report("C1", exec_type, "0")
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        assert len(stream.events) == count
        await stream.close()

    async def test_report_for_an_order_never_sent_still_reaches_the_stream(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        acceptor.orders["ALIEN"] = _alien_order()
        await acceptor.exec_report("ALIEN", "0", "0")
        await eventually(lambda: bool(stream.of(Acknowledged)))
        assert stream.of(Acknowledged)[0].client_order_id == "ALIEN"
        await stream.close()

    async def test_fill_for_an_order_never_sent_uses_the_default_venue(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(dialect=FixDialect(default_venue="XNAS"))
        stream = Stream(provider)
        acceptor.orders["ALIEN"] = _alien_order()
        await acceptor.fill("ALIEN", "1", "2")
        await eventually(lambda: bool(stream.of(Filled)))
        assert stream.of(Filled)[0].fill.symbol == Symbol(venue="XNAS", ticker="ZZZ")
        await stream.close()

    async def test_fill_for_an_order_never_sent_without_a_venue_is_rejected(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        acceptor.orders["ALIEN"] = _alien_order()
        await acceptor.fill("ALIEN", "1", "2")
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        assert provider.dropped_reports == 1
        assert (await provider.quarantined())[0].msg_type == "8"

    @pytest.mark.parametrize(
        ("fields", "reject_reason", "reject_tag"),
        [
            ([(37, "O1"), (150, "0")], "1", "11"),
            ([(11, "C1"), (37, "O1"), (150, "0")], "1", "17"),
            ([(11, "C1"), (17, "X1"), (150, "0")], "1", "37"),
            ([(11, "C1"), (17, "X2"), (150, "F"), (31, "10")], "1", "32"),
            ([(11, "C1"), (17, "X3"), (150, "F"), (32, "10")], "1", "31"),
            ([(11, "C1"), (17, "X4"), (150, "F"), (32, "abc"), (31, "1")], "6", "32"),
            ([(11, "C1"), (17, "X5"), (150, "H")], "1", "19"),
            (
                [(11, "C1"), (17, "X6"), (150, "F"), (32, "1"), (31, "1"), (60, "x")],
                "6",
                "60",
            ),
            ([(11, "C1"), (17, "X7"), (150, "D"), (39, "?")], "5", "39"),
        ],
    )
    async def test_malformed_reports_are_rejected_and_counted(
        self,
        acceptor: FixAcceptor,
        make_provider: ProviderFactory,
        fields: list[tuple[int, str]],
        reject_reason: str,
        reject_tag: str,
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        await eventually(lambda: bool(stream.of(Acknowledged)))
        await acceptor.send("8", fields)
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        reject = acceptor.of_type("3")[0]
        assert reject.get(373) == reject_reason
        assert reject.get(371) == reject_tag
        await eventually(lambda: stream.error is not None)
        assert isinstance(stream.error, ReportQuarantinedError)
        assert provider.dropped_reports == 1
        assert provider.session.is_ready
        assert not stream.of(Filled)
        assert len(await provider.quarantined()) == 1
        await stream.close()

    async def test_unsupported_application_message_gets_a_business_reject(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        await make_provider()
        seq = await acceptor.send("AE", [(58, "trade capture")])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("j")))
        reject = acceptor.of_type("j")[0]
        assert reject.get(45) == str(seq)
        assert reject.get(372) == "AE"
        assert reject.get(380) == "3"

    async def test_news_is_accepted_quietly(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await acceptor.send("B", [(148, "headline")])
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        assert acceptor.of_type("j") == []


def _alien_order() -> Any:
    from .acceptor import VenueOrder

    return VenueOrder("ALIEN", "ALIEN", "ZZZ", "1", Decimal(1), None, "O99")


class TestUnreadableReports:
    async def test_a_fill_with_an_empty_text_is_applied_and_rejected(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        seq = acceptor.next_out
        fields = (
            f"35=8\x0149=VENUE\x0156=CLIENT\x0134={seq}\x0152={_now_text()}"
            "\x0137=O1\x0111=C1\x0117=X1\x01150=F\x0139=1\x0154=1\x0155=ACME"
            "\x0132=40\x0131=10\x0114=40\x0158=\x01"
        ).encode()
        head = f"8=FIX.4.4\x019={len(fields)}\x01".encode() + fields
        acceptor.next_out += 1
        await acceptor.write(head + f"10={sum(head) % 256:03d}\x01".encode())
        await eventually(lambda: bool(stream.of(Filled)))
        assert stream.of(Filled)[0].fill.fill_id == "X1"
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        assert acceptor.of_type("3")[0].get(373) == "4"
        assert stream.error is None
        assert await provider.quarantined() == []
        await stream.close()

    async def test_an_unreadable_fill_restates_the_order_and_stops_the_stream(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        await eventually(lambda: bool(stream.of(Acknowledged)))
        await acceptor.exec_report(
            "C1",
            "F",
            "1",
            last_qty=Decimal(40),
            last_px=Decimal(10),
            extra=[(13, "3"), (12, "bad")],
        )
        await eventually(lambda: stream.error is not None)
        restated = stream.of(Restated)[0]
        assert restated.status is OrderStatus.UNKNOWN
        assert not stream.of(Filled)
        held = await provider.quarantined()
        assert held[0].msg_type == "8"
        assert "12=bad" in held[0].content
        assert (
            await provider.session.run_store(
                lambda: provider.session.store.next_inbound()
            )
            == acceptor.next_out
        )

    async def test_the_stream_stays_stopped_until_released(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        first = Stream(provider)
        await provider.place_order(order())
        await eventually(lambda: bool(first.of(Acknowledged)))
        await acceptor.send("8", [(11, "C1"), (150, "0")])
        await eventually(lambda: first.error is not None)
        await acceptor.exec_report("C1", "4", "4")
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)

        second = Stream(provider)
        await eventually(lambda: second.error is not None)
        assert isinstance(second.error, ReportQuarantinedError)
        assert second.of(Canceled) == [], "a later close must not hide the problem"

        provider.release_quarantine()
        third = Stream(provider)
        await eventually(lambda: bool(third.of(Canceled)))
        assert third.error is None
        await third.close()

    async def test_quarantine_with_no_subscriber_stops_the_first_one(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await acceptor.send("8", [(11, "ZZZ"), (150, "0")])
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        stream = Stream(provider)
        await eventually(lambda: stream.error is not None)
        assert isinstance(stream.error, ReportQuarantinedError)

    async def test_a_report_with_no_exec_type_is_quarantined(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await provider.place_order(order())
        await acceptor.send("8", [(37, "O1"), (11, "C1"), (17, "X"), (32, "1")])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        assert acceptor.of_type("3")[0].get(371) == "150"
        assert len(await provider.quarantined()) == 1

    async def test_a_discarded_report_is_quarantined(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        await eventually(lambda: bool(stream.of(Acknowledged)))
        seq = acceptor.next_out
        acceptor.next_out += 1
        await acceptor.send_raw_seq(seq, "8", [(11, "C1"), (150, "F")], poss_dup=True)
        await eventually(lambda: stream.error is not None)
        assert (await provider.quarantined())[0].reason == "OrigSendingTime missing"
        assert stream.of(Restated)[0].status is OrderStatus.UNKNOWN

    async def test_the_quarantine_never_holds_a_credential(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        password = "hunter2-do-not-leak"
        provider = await make_provider(password=SecretStr(password))
        await acceptor.send("8", [(11, "C1"), (150, "0"), (58, f"echo {password}")])
        await eventually(lambda: provider.dropped_reports == 1)
        held = await provider.quarantined()
        assert password not in held[0].content
        assert password not in held[0].reason

    @pytest.mark.parametrize("text", ["9" * 19, "9" * 5000, "x"])
    async def test_an_oversized_reference_cannot_break_the_session(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory, text: str
    ) -> None:
        provider = await make_provider()
        await acceptor.send("3", [(45, text), (373, "5")])
        await acceptor.send("j", [(45, text), (380, "0")])
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        assert provider.session.is_ready
        assert acceptor.connections == 1


class TestDoneForDay:
    @pytest.mark.parametrize("tif", [TimeInForce.GTC, TimeInForce.GTD])
    async def test_a_multi_day_order_is_not_dead(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory, tif: TimeInForce
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        extra: dict[str, Any] = {"time_in_force": tif}
        if tif is TimeInForce.GTD:
            extra["expire_at"] = datetime(2030, 1, 1, tzinfo=timezone.utc)
        await provider.place_order(order(**extra))
        await eventually(lambda: bool(stream.of(Acknowledged)))
        await acceptor.exec_report("C1", "3", "3")
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        assert stream.of(Expired) == []
        assert [o.client_order_id for o in await provider.open_orders()] == ["C1"]
        acceptor.orders["C1"].status = "3"
        snapshot = await provider.order_status("C1", ACME)
        assert snapshot is not None
        assert snapshot.status is OrderStatus.ACCEPTED
        await stream.close()

    async def test_a_partly_filled_multi_day_order_stays_partly_filled(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await provider.place_order(order(time_in_force=TimeInForce.GTC))
        await acceptor.fill("C1", "40", "10")
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        acceptor.orders["C1"].status = "3"
        snapshot = await provider.order_status("C1", ACME)
        assert snapshot is not None
        assert snapshot.status is OrderStatus.PARTIALLY_FILLED

    async def test_a_day_order_ends(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        await acceptor.exec_report("C1", "3", "3")
        await eventually(lambda: bool(stream.of(Expired)))
        await stream.close()


class TestSequenceResetForgetsWhatNumbersMeant:
    async def test_a_reject_of_a_reused_number_does_not_touch_the_old_order(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        seq = acceptor.of_type("D")[0].require(34)
        provider.on_sequence_reset()
        await acceptor.send("3", [(45, seq), (373, "5"), (58, "about a heartbeat")])
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        assert stream.of(Rejected) == []
        await stream.close()

    async def test_a_reject_naming_another_message_type_is_ignored(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        seq = acceptor.of_type("D")[0].require(34)
        await acceptor.send("3", [(45, seq), (372, "0"), (373, "5")])
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        assert stream.of(Rejected) == []
        await stream.close()


class TestConcurrency:
    async def test_a_report_during_a_cancel_is_not_overwritten(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        """Replace reports racing new requests keep the ClOrdID chain coherent."""
        provider = await make_provider()
        await provider.place_order(order())
        await asyncio.gather(
            provider.amend_order("C1", ACME, quantity=Decimal(90)),
            provider.amend_order("C1", ACME, quantity=Decimal(80)),
            provider.amend_order("C1", ACME, quantity=Decimal(70)),
        )
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        ids = [m.get(11) for m in acceptor.of_type("G")]
        assert len(set(ids)) == 3, "a ClOrdID was reused"
        payload = provider.session.store.get_order("C1")
        assert payload is not None
        assert '"chain_length":3' in payload

    async def test_events_go_out_when_a_commit_is_cancelled(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        """A commit that lands marks the ExecID seen, so the event must not be lost."""
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        await eventually(lambda: bool(stream.of(Acknowledged)))
        store = provider.session.store
        real = store.record_report
        started = threading.Event()
        release = threading.Event()

        def slow(*args: Any, **kw: Any) -> None:
            started.set()
            release.wait(5.0)
            real(*args, **kw)

        store.record_report = slow  # type: ignore[method-assign]
        await acceptor.fill("C1", "40", "10", exec_id="X9")
        await asyncio.to_thread(started.wait, 5.0)
        await acceptor.drop_connection()
        await provider.session.stop()
        release.set()
        await eventually(lambda: bool(stream.of(Filled)))
        assert stream.of(Filled)[0].fill.fill_id == "X9"
        store.record_report = real  # type: ignore[method-assign]
        await stream.close()


class TestCancel:
    async def test_cancel_fields_and_chain(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order(side=OrderSide.SELL))
        await provider.cancel_order("C1", ACME)
        wire = acceptor.of_type("F")[0]
        assert wire.get(41) == "C1"
        assert wire.get(11) == "C1~1"
        assert wire.get(37) == "O1"
        assert wire.get(55) == "ACME"
        assert wire.get(54) == "2"
        assert wire.get(38) == "100"
        assert wire.get(60) is not None
        await eventually(lambda: bool(stream.of(Canceled)))
        assert stream.of(Canceled)[0].client_order_id == "C1"
        await stream.close()

    async def test_cancel_reject_raises_venue_error_and_skips_the_stream(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        acceptor.faults.cancel_reject = "0"
        with pytest.raises(VenueError) as info:
            await provider.cancel_order("C1", ACME)
        assert info.value.code == "0"
        assert "cannot cancel" in str(info.value)
        await asyncio.sleep(0.01)
        assert stream.of(CancelRejected) == []
        await stream.close()

    async def test_late_cancel_reject_reaches_the_stream(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        await acceptor.send(
            "9",
            [
                (37, "O1"),
                (11, "C1~9"),
                (41, "C1"),
                (39, "0"),
                (434, "1"),
                (102, "1"),
            ],
        )
        await eventually(lambda: bool(stream.of(CancelRejected)))
        assert stream.of(CancelRejected)[0].client_order_id == "C1"
        await stream.close()

    async def test_cancel_reject_for_nothing_we_know_is_counted(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await acceptor.send(
            "9",
            [(37, "NONE"), (11, "ZZ"), (41, "YY"), (39, "8"), (434, "1")],
        )
        await eventually(lambda: provider.dropped_reports == 1)

    async def test_cancel_works_for_an_order_that_was_never_acknowledged(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(ack_timeout=0.05)
        acceptor.faults.lose_ack = 1
        with pytest.raises(AmbiguousOutcome):
            await provider.place_order(order())
        await asyncio.sleep(0)
        provider._ack_timeout = 2.0
        await provider.cancel_order("C1", ACME)
        assert acceptor.of_type("F")[0].get(41) == "C1"
        assert acceptor.of_type("F")[0].get(37) is None

    async def test_cancel_of_an_order_with_no_record_cannot_be_sent(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        with pytest.raises(TransportError, match="no local record"):
            await provider.cancel_order("NOPE", ACME)
        assert acceptor.of_type("F") == []

    async def test_no_answer_is_ambiguous(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(ack_timeout=0.1)
        await provider.place_order(order())
        acceptor.faults.silent = True
        with pytest.raises(AmbiguousOutcome):
            await provider.cancel_order("C1", ACME)

    async def test_session_down_is_transport_error(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await provider.place_order(order())
        await acceptor.stop()
        await eventually(lambda: not provider.session.is_ready)
        with pytest.raises(TransportError) as info:
            await provider.cancel_order("C1", ACME)
        assert not isinstance(info.value, AmbiguousOutcome)


class TestAmend:
    async def test_replace_fields_chain_and_report(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order(side=OrderSide.SELL))
        await provider.amend_order(
            "C1", ACME, quantity=Decimal(80), limit_price=Decimal("11.5")
        )
        wire = acceptor.of_type("G")[0]
        assert wire.get(41) == "C1"
        assert wire.get(11) == "C1~1"
        assert wire.get(38) == "80"
        assert wire.get(44) == "11.5"
        assert wire.get(40) == "2"
        assert wire.get(54) == "2"
        assert wire.get(21) == "1"
        await eventually(lambda: bool(stream.of(Replaced)))
        event = stream.of(Replaced)[0]
        assert event.client_order_id == "C1"
        assert event.quantity == Decimal(80)
        assert event.limit_price == Decimal("11.5")
        await stream.close()

    async def test_the_next_change_chains_from_the_replacement(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        await provider.amend_order("C1", ACME, limit_price=Decimal(11))
        await eventually(lambda: bool(stream.of(Replaced)))
        await provider.amend_order("C1", ACME, quantity=Decimal(50))
        second = acceptor.of_type("G")[1]
        assert second.get(41) == "C1~1"
        assert second.get(11) == "C1~2"
        assert second.get(38) == "50"
        assert second.get(44) == "11"
        await eventually(lambda: len(stream.of(Replaced)) == 2)
        await provider.cancel_order("C1", ACME)
        cancel = acceptor.of_type("F")[0]
        assert cancel.get(41) == "C1~2"
        assert cancel.get(11) == "C1~3"
        await eventually(lambda: bool(stream.of(Canceled)))
        assert stream.of(Canceled)[0].client_order_id == "C1"
        await stream.close()

    async def test_a_fill_under_a_replaced_id_maps_to_the_original(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        await provider.amend_order("C1", ACME, limit_price=Decimal(11))
        await acceptor.fill("C1", "10", "11")
        await eventually(lambda: bool(stream.of(Filled)))
        assert stream.of(Filled)[0].fill.client_order_id == "C1"
        await stream.close()

    async def test_reject_raises_venue_error(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        acceptor.faults.replace_reject = "0"
        with pytest.raises(VenueError) as info:
            await provider.amend_order("C1", ACME, limit_price=Decimal(11))
        assert info.value.code == "0"
        assert stream.of(ReplaceRejected) == []
        await stream.close()

    async def test_late_reject_reaches_the_stream(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        await acceptor.send(
            "9",
            [(37, "O1"), (11, "C1~9"), (41, "C1"), (39, "0"), (434, "2")],
        )
        await eventually(lambda: bool(stream.of(ReplaceRejected)))
        await stream.close()

    async def test_amend_of_an_order_with_no_record_cannot_be_sent(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        with pytest.raises(TransportError, match="no local record"):
            await provider.amend_order("NOPE", ACME, quantity=Decimal(1))

    async def test_no_answer_is_ambiguous(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(ack_timeout=0.1)
        await provider.place_order(order())
        acceptor.faults.silent = True
        with pytest.raises(AmbiguousOutcome):
            await provider.amend_order("C1", ACME, quantity=Decimal(1))


class TestOrderStatus:
    async def test_found(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await provider.place_order(order())
        await acceptor.fill("C1", "40", "10.25")
        snapshot = await provider.order_status("C1", ACME)
        assert snapshot is not None
        assert snapshot.client_order_id == "C1"
        assert snapshot.venue_order_id == "O1"
        assert snapshot.symbol == ACME
        assert snapshot.side is OrderSide.BUY
        assert snapshot.quantity == Decimal(100)
        assert snapshot.filled_quantity == Decimal(40)
        assert snapshot.status is OrderStatus.PARTIALLY_FILLED
        assert snapshot.limit_price == Decimal("10.25")
        wire = acceptor.of_type("H")[0]
        assert wire.get(11) == "C1"
        assert wire.get(55) == "ACME"
        assert wire.get(54) == "1"
        assert wire.get(790) is not None

    async def test_status_reports_do_not_reach_the_stream(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        await eventually(lambda: bool(stream.of(Acknowledged)))
        await provider.order_status("C1", ACME)
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        await asyncio.sleep(0.01)
        assert [type(e) for e in stream.events] == [Acknowledged]
        await stream.close()

    async def test_unknown_order_per_scenario_d24_is_none(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await provider.place_order(order())
        acceptor.wire.clear()
        assert await provider.order_status("C1", ACME) is None

    async def test_unknown_order_on_fix_42_is_none(
        self, make_provider: ProviderFactory
    ) -> None:
        old = FixAcceptor("FIX.4.2")
        await old.start()
        provider = await make_provider(target=old)
        await provider.place_order(order())
        old.wire.clear()
        assert await provider.order_status("C1", ACME) is None
        assert old.of_type("H")[0].get(790) is None
        await provider.close()
        await old.stop()

    async def test_unknown_id_business_reject_is_none(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await provider.place_order(order())
        acceptor.faults.status_unknown_via_business_reject = True
        assert await provider.order_status("C1", ACME) is None

    @pytest.mark.parametrize("reason", ["0", "3", "4", "6"])
    async def test_other_business_rejects_raise(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory, reason: str
    ) -> None:
        provider = await make_provider()
        await provider.place_order(order())
        acceptor.wire.clear()
        acceptor.faults.status_business_reject_reason = reason
        with pytest.raises(VenueError) as info:
            await provider.order_status("C1", ACME)
        assert info.value.code == f"BUSINESS_{reason}"

    async def test_a_genuinely_rejected_order_is_not_not_found(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        acceptor.faults.order_reject = "3"
        with pytest.raises(VenueError):
            await provider.place_order(order())
        acceptor.wire["C1"] = "C1"
        snapshot = await provider.order_status("C1", ACME)
        assert snapshot is not None
        assert snapshot.status is OrderStatus.REJECTED

    async def test_timeout_raises_and_is_never_none(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(query_timeout=0.05)
        await provider.place_order(order())
        acceptor.faults.ignore_status_requests = True
        with pytest.raises(TransportError):
            await provider.order_status("C1", ACME)

    async def test_session_down_raises_and_is_never_none(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await provider.place_order(order())
        await acceptor.stop()
        await eventually(lambda: not provider.session.is_ready)
        with pytest.raises(TransportError):
            await provider.order_status("C1", ACME)

    async def test_disconnect_during_the_query_raises(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(query_timeout=5.0)
        await provider.place_order(order())
        acceptor.faults.ignore_status_requests = True
        acceptor.faults.drop_after_type["H"] = 1
        with pytest.raises(TransportError):
            await provider.order_status("C1", ACME)

    async def test_session_reject_of_the_query_raises(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await provider.place_order(order())
        acceptor.faults.ignore_status_requests = True
        task = asyncio.create_task(provider.order_status("C1", ACME))
        await acceptor.wait_until(lambda: bool(acceptor.of_type("H")))
        seq = acceptor.of_type("H")[0].require(34)
        await acceptor.send("3", [(45, seq), (373, "1")])
        with pytest.raises(VenueError):
            await task

    async def test_no_local_record_raises_and_is_never_none(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        """A wiped or misconfigured store must not make a live order look dead."""
        provider = await make_provider()
        with pytest.raises(TransportError, match="no local record"):
            await provider.order_status("NEVER", ACME)
        assert acceptor.of_type("H") == []

    async def test_malformed_status_report_raises(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await provider.place_order(order())
        acceptor.faults.ignore_status_requests = True
        task = asyncio.create_task(provider.order_status("C1", ACME))
        await acceptor.wait_until(lambda: bool(acceptor.of_type("H")))
        await acceptor.send(
            "8", [(37, "O1"), (11, "C1"), (17, "0"), (150, "I"), (39, "?")]
        )
        with pytest.raises(TransportError, match="malformed"):
            await task

    async def test_concurrent_queries_each_get_an_answer(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        for cid in ("C1", "C2", "C3"):
            await provider.place_order(order(cid))
        results = await asyncio.gather(
            *(provider.order_status(c, ACME) for c in ("C1", "C2", "C3", "C1"))
        )
        assert [r.client_order_id for r in results if r] == ["C1", "C2", "C3", "C1"]


class TestLocalQueries:
    async def test_open_orders_tracks_reports(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        other = Symbol(venue="XNAS", ticker="OTHER")
        await provider.place_order(order("C1"))
        await provider.place_order(order("C2", symbol=other))
        snapshot_ids = {o.client_order_id for o in await provider.open_orders()}
        assert snapshot_ids == {"C1", "C2"}
        assert [o.client_order_id for o in await provider.open_orders(other)] == ["C2"]
        await acceptor.fill("C1", "100", "10")
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        assert [o.client_order_id for o in await provider.open_orders()] == ["C2"]

    async def test_unacknowledged_orders_are_not_open(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(ack_timeout=0.05)
        acceptor.faults.lose_ack = 1
        with pytest.raises(AmbiguousOutcome):
            await provider.place_order(order())
        assert await provider.open_orders() == []

    async def test_fills_since_filters_by_time_and_symbol(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        other = Symbol(venue="XNAS", ticker="OTHER")
        await provider.place_order(order("C1"))
        await provider.place_order(order("C2", symbol=other))
        await acceptor.fill("C1", "1", "10", exec_id="F1")
        await acceptor.fill("C2", "1", "10", exec_id="F2")
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        past = datetime(2000, 1, 1, tzinfo=timezone.utc)
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        assert {f.fill_id for f in await provider.fills_since(past)} == {"F1", "F2"}
        assert [f.fill_id for f in await provider.fills_since(past, other)] == ["F2"]
        assert await provider.fills_since(future) == []

    async def test_a_fill_timestamped_early_by_the_venue_is_still_found(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        """A venue clock behind ours must not hide a recovered fill."""
        provider = await make_provider()
        await provider.place_order(order())
        since = datetime.now(timezone.utc)
        await acceptor.send(
            "8",
            [
                (37, "O1"),
                (11, "C1"),
                (17, "EARLY"),
                (150, "F"),
                (39, "1"),
                (54, "1"),
                (55, "ACME"),
                (32, "1"),
                (31, "10"),
                (14, "1"),
                (60, "20000101-00:00:00"),
            ],
        )
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        assert [f.fill_id for f in await provider.fills_since(since)] == ["EARLY"]

    async def test_a_failed_store_read_is_a_transport_error(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory, tmp_path: Path
    ) -> None:
        store = SqliteFixStore(tmp_path / "fix.db", "s")
        provider = await make_provider(store=store)
        store._conn.execute("DROP TABLE fix_execution")
        with pytest.raises(TransportError):
            await provider.fills_since(datetime(2000, 1, 1, tzinfo=timezone.utc))
        store._conn.execute("DROP TABLE fix_order")
        with pytest.raises(TransportError):
            await provider.open_orders()
        with pytest.raises(TransportError):
            await provider.cancel_order("C1", ACME)
        await provider.close()
        store.close()


class TestStream:
    async def test_events_with_no_subscriber_wait_in_the_backlog(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        await provider.place_order(order())
        stream = Stream(provider)
        await eventually(lambda: bool(stream.of(Acknowledged)))
        await stream.close()

    async def test_events_survive_a_resubscribe(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        first = Stream(provider)
        await provider.place_order(order())
        await eventually(lambda: bool(first.of(Acknowledged)))
        await first.close()
        await acceptor.fill("C1", "1", "10")
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        second = Stream(provider)
        await eventually(lambda: bool(second.of(Filled)))
        await second.close()

    async def test_a_reconnect_does_not_end_the_stream(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        await acceptor.drop_connection()
        await eventually(lambda: not provider.session.is_ready)
        await provider.session.wait_ready(5.0)
        await acceptor.fill("C1", "1", "10")
        await eventually(lambda: bool(stream.of(Filled)))
        assert stream.error is None
        await stream.close()

    async def test_reports_during_an_outage_are_replayed_by_the_session(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await provider.place_order(order())
        acceptor.faults.truncate_next = 1
        await acceptor.fill("C1", "1", "10")
        await eventually(lambda: bool(stream.of(Filled)))
        assert stream.of(Filled)[0].fill.quantity == Decimal(1)
        await stream.close()

    async def test_a_terminal_failure_ends_the_stream_with_an_error(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await asyncio.sleep(0)
        await acceptor.send_raw_seq(1, "0", [])
        await eventually(lambda: stream.error is not None)
        assert isinstance(stream.error, TransportError)
        assert provider.session.state is SessionState.FAILED
        with pytest.raises(TransportError):
            await provider.place_order(order())
        with pytest.raises(TransportError):
            async for _ in provider.stream_executions():
                pass
        await stream.close()

    async def test_close_ends_the_stream(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider()
        stream = Stream(provider)
        await asyncio.sleep(0)
        await provider.close()
        await eventually(lambda: stream.error is not None)


class TestRestart:
    async def test_terms_and_chain_survive_a_restart(
        self, acceptor: FixAcceptor, tmp_path: Path
    ) -> None:
        path = tmp_path / "fix.db"
        store = SqliteFixStore(path, "s")
        provider = FixProvider(session_config(acceptor), store, logon_timeout=5.0)
        await provider.open()
        await provider.place_order(order(side=OrderSide.SELL))
        await provider.amend_order("C1", ACME, limit_price=Decimal(11))
        await provider.close()
        store.close()

        reopened = SqliteFixStore(path, "s")
        again = FixProvider(session_config(acceptor), reopened, logon_timeout=5.0)
        await again.open()
        stream = Stream(again)
        snapshot = await again.order_status("C1", ACME)
        assert snapshot is not None
        assert acceptor.of_type("H")[0].get(11) == "C1~1"
        assert acceptor.of_type("H")[0].get(54) == "2"
        await again.cancel_order("C1", ACME)
        assert acceptor.of_type("F")[0].get(41) == "C1~1"
        assert acceptor.of_type("F")[0].get(11) == "C1~2"
        await eventually(lambda: bool(stream.of(Canceled)))
        assert stream.of(Canceled)[0].client_order_id == "C1"
        await stream.close()
        await again.close()
        reopened.close()

    async def test_seen_exec_ids_and_fills_survive_a_restart(
        self, acceptor: FixAcceptor, tmp_path: Path
    ) -> None:
        path = tmp_path / "fix.db"
        store = SqliteFixStore(path, "s")
        provider = FixProvider(session_config(acceptor), store, logon_timeout=5.0)
        await provider.open()
        await provider.place_order(order())
        await acceptor.fill("C1", "40", "10", exec_id="F1")
        await eventually(lambda: provider.session.next_inbound == acceptor.next_out)
        fields = acceptor.sent[max(acceptor.sent)][1]
        await provider.close()
        store.close()

        reopened = SqliteFixStore(path, "s")
        again = FixProvider(session_config(acceptor), reopened, logon_timeout=5.0)
        await again.open()
        stream = Stream(again)
        await acceptor.send("8", list(fields))
        await eventually(lambda: again.session.next_inbound == acceptor.next_out)
        assert stream.of(Filled) == []
        past = datetime(2000, 1, 1, tzinfo=timezone.utc)
        assert [f.fill_id for f in await again.fills_since(past)] == ["F1"]
        await stream.close()
        await again.close()
        reopened.close()

    async def test_order_status_after_restart_of_an_ambiguous_order(
        self, acceptor: FixAcceptor, tmp_path: Path
    ) -> None:
        path = tmp_path / "fix.db"
        store = SqliteFixStore(path, "s")
        provider = FixProvider(
            session_config(acceptor), store, ack_timeout=0.05, logon_timeout=5.0
        )
        await provider.open()
        acceptor.faults.lose_ack = 1
        with pytest.raises(AmbiguousOutcome):
            await provider.place_order(order())
        await provider.close()
        store.close()

        reopened = SqliteFixStore(path, "s")
        again = FixProvider(session_config(acceptor), reopened, logon_timeout=5.0)
        await again.open()
        snapshot = await again.order_status("C1", ACME)
        assert snapshot is not None
        assert snapshot.status is OrderStatus.ACCEPTED
        assert len(acceptor.orders) == 1
        await again.close()
        reopened.close()


class TestDialect:
    async def test_a_venue_can_override_tags_and_fields(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        class Venue(FixDialect):
            def instrument_fields(self, symbol: Symbol) -> list[Any]:
                return [(55, symbol.ticker), (207, symbol.venue)]

        dialect = Venue(
            tags=FixTags(account=6666),
            handl_inst=None,
            post_only_exec_inst=None,
            supports_short=False,
        )
        provider = await make_provider(dialect=dialect)
        await provider.place_order(order(account="ACC"))
        wire = acceptor.of_type("D")[0]
        assert wire.get(6666) == "ACC"
        assert wire.get(1) is None
        assert wire.get(21) is None
        assert wire.get(207) == "XNAS"
        rules = provider.trading_rules()
        assert not rules.supports_post_only
        assert not rules.supports_short

    async def test_post_only_without_a_mapping_is_refused(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(dialect=FixDialect(post_only_exec_inst=None))
        with pytest.raises(VenueError) as info:
            await provider.place_order(order(post_only=True))
        assert info.value.code == "UNSUPPORTED"

    async def test_custom_inbound_tags(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        provider = await make_provider(
            dialect=FixDialect(tags=FixTags(exec_ref_id=7777))
        )
        stream = Stream(provider)
        await provider.place_order(order())
        first = await acceptor.fill("C1", "1", "10")
        await eventually(lambda: bool(stream.of(Filled)))
        await acceptor.exec_report(
            "C1", "H", "0", extra=[(7777, first)], last_qty=Decimal(0)
        )
        await eventually(lambda: bool(stream.of(FillBusted)))
        assert stream.of(FillBusted)[0].fill_id == first
        await stream.close()

    async def test_a_venue_can_define_its_own_not_found(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        class Venue(FixDialect):
            def is_unknown_order(self, report: Any) -> bool:
                return report.get(58) == "gone"

        provider = await make_provider(dialect=Venue())
        await provider.place_order(order())
        acceptor.faults.ignore_status_requests = True
        task = asyncio.create_task(provider.order_status("C1", ACME))
        await acceptor.wait_until(lambda: bool(acceptor.of_type("H")))
        await acceptor.exec_report("C1", "I", "0", text="gone", status_reply=True)
        assert await task is None

    async def test_unsupported_terms_are_refused(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        dialect = FixDialect(
            tif_codes={TimeInForce.DAY: "0"},
            ord_type_codes={OrderType.LIMIT: "2"},
            short_side_codes={},
        )
        provider = await make_provider(dialect=dialect)
        for request in (
            order("A", time_in_force=TimeInForce.GTC),
            order("B", order_type=OrderType.MARKET, limit_price=None),
            order("C", side=OrderSide.SELL, short_sale=ShortSaleMark.SHORT),
        ):
            with pytest.raises(VenueError) as info:
                await provider.place_order(request)
            assert info.value.code == "UNSUPPORTED"
        assert acceptor.of_type("D") == []


class TestSecrets:
    async def test_password_is_not_in_any_provider_surface(
        self,
        acceptor: FixAcceptor,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(logging.DEBUG)
        password = "hunter2-do-not-leak"
        path = tmp_path / "fix.db"
        store = SqliteFixStore(path, "s")
        provider = FixProvider(
            session_config(
                acceptor, username=SecretStr("alice"), password=SecretStr(password)
            ),
            store,
            logon_timeout=5.0,
        )
        await provider.open()
        await provider.place_order(order())
        assert acceptor.logons[0].get(554) == password
        assert password not in repr(provider)
        assert password not in repr(provider.session)
        assert password not in repr(provider.session.config)
        assert password not in str(provider.session.config)
        await provider.close()
        store.close()
        assert password not in caplog.text
        for file in tmp_path.iterdir():
            assert password.encode() not in file.read_bytes()

    @pytest.mark.parametrize("kind", ["cancel", "business", "session", "report"])
    async def test_rejects_echoing_the_password_are_scrubbed(
        self,
        acceptor: FixAcceptor,
        make_provider: ProviderFactory,
        caplog: pytest.LogCaptureFixture,
        kind: str,
    ) -> None:
        caplog.set_level(logging.DEBUG)
        password = "hunter2-do-not-leak"
        provider = await make_provider(password=SecretStr(password), ack_timeout=0.3)
        stream = Stream(provider)
        await provider.place_order(order())
        await eventually(lambda: bool(stream.of(Acknowledged)))
        echo = f"you sent {password}"
        if kind == "report":
            await acceptor.exec_report("C1", "4", "4", text=echo)
            await eventually(lambda: bool(stream.of(Canceled)))
            rendered = repr(stream.of(Canceled)[0])
        else:
            acceptor.faults.silent = kind != "cancel"
            acceptor.faults.cancel_reject = "1" if kind == "cancel" else None
            task = asyncio.create_task(provider.cancel_order("C1", ACME))
            await asyncio.sleep(0.05)
            seq = str(int(acceptor.received[-1].require(34)))
            if kind == "cancel":
                await acceptor.send(
                    "9",
                    [
                        (37, "O1"),
                        (11, "C1~1"),
                        (41, "C1"),
                        (39, "0"),
                        (434, "1"),
                        (58, echo),
                    ],
                )
            elif kind == "business":
                await acceptor.send(
                    "j", [(45, seq), (372, "F"), (380, "0"), (58, echo)]
                )
            else:
                await acceptor.send("3", [(45, seq), (373, "5"), (58, echo)])
            with pytest.raises(VenueError) as info:
                await task
            rendered = str(info.value)
        assert password not in rendered
        assert password not in caplog.text
        await stream.close()

    async def test_a_failed_store_on_cancel_is_a_transport_error(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory, tmp_path: Path
    ) -> None:
        store = SqliteFixStore(tmp_path / "fix.db", "s")
        provider = await make_provider(store=store)
        await provider.place_order(order())
        store._conn.execute("DROP TABLE fix_outbound")
        with pytest.raises(TransportError):
            await provider.cancel_order("C1", ACME)
        store._conn.execute("DROP TABLE fix_wire_id")
        with pytest.raises(TransportError):
            await provider.cancel_order("C1", ACME)
        await provider.close()
        store.close()

    async def test_logon_refusal_is_an_auth_error_without_the_password(
        self, acceptor: FixAcceptor, make_provider: ProviderFactory
    ) -> None:
        password = "hunter2-do-not-leak"
        acceptor.faults.refuse_logon = f"wrong: {password}"
        with pytest.raises(AuthError) as info:
            await make_provider(password=SecretStr(password))
        assert password not in str(info.value)
        assert acceptor.connections == 1


class TestEncoding:
    def test_stored_bodies_keep_decimal_text(self) -> None:
        assert encode_fields([(44, Decimal("1.10"))]) == b"44=1.10\x01"

    async def test_fix_store_error_is_typed(self, tmp_path: Path) -> None:
        store = SqliteFixStore(tmp_path / "fix.db", "s")
        store._conn.close()
        with pytest.raises(FixStoreError):
            store.next_outbound()
