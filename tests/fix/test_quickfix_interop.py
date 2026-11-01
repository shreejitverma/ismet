"""This initiator against a real QuickFIX acceptor, with data dictionary validation on.

Skipped where ``quickfix`` is not installed. It is a C++ engine with no relationship to
this code, so a pass here is independent evidence for framing, the session protocol,
and the order message fields. To run it::

    uv venv --python 3.12 /tmp/qf
    VIRTUAL_ENV=/tmp/qf uv pip install quickfix pytest pytest-asyncio hypothesis \\
        httpx websockets pydantic platformdirs tzdata
    /tmp/qf/bin/python -m pytest tests/fix/test_quickfix_interop.py -q
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import sys
import threading
import warnings
from collections.abc import AsyncIterator
from dataclasses import dataclass, field, replace
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from pydantic import SecretStr

from ismet.errors import AmbiguousOutcome, VenueError
from ismet.fix.message import encode_fields, format_utc_timestamp
from ismet.fix.provider import FixProvider, _Kind
from ismet.fix.session import (
    APPL_VER_FIX50SP2,
    FixSessionConfig,
    LogonRejectedError,
    SessionState,
)
from ismet.fix.store import FixStore, SqliteFixStore, StoredMessage
from ismet.models import Symbol
from ismet.models.orders import (
    Acknowledged,
    Canceled,
    FillBusted,
    Filled,
    OrderEvent,
    OrderRequest,
    OrderSide,
    OrderStatus,
    Replaced,
)
from ismet.transport.backoff import ExponentialBackoff

with warnings.catch_warnings():
    # SWIG's generated module warns on import under Python 3.12 and later.
    warnings.simplefilter("ignore", DeprecationWarning)
    fix: Any = pytest.importorskip("quickfix")

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")

ACME = Symbol(venue="XNAS", ticker="ACME")
VERSIONS = ["FIX.4.2", "FIX.4.4", "FIXT.1.1"]
DICTIONARY = {"FIX.4.2": "FIX42", "FIX.4.4": "FIX44", "FIXT.1.1": "FIX50SP2"}


def dictionary(name: str) -> Path:
    path = Path(sys.prefix) / "share" / "quickfix" / f"{name}.xml"
    if not path.exists():
        pytest.skip(f"QuickFIX data dictionary {name} is not installed")
    return path


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@dataclass
class Venue:
    """Order state in the QuickFIX application. Touched from its own thread."""

    orders: dict[str, dict[str, str]] = field(default_factory=dict)
    received: list[tuple[str, dict[int, str]]] = field(default_factory=list)
    rejects: list[str] = field(default_factory=list)
    rejects_from_client: list[str] = field(default_factory=list)
    resend_requests: int = 0
    logon_fields: dict[int, str] = field(default_factory=dict)
    logons: int = 0
    next_exec: int = 0
    silent_orders: bool = False
    unknown_status: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)


_TAGS = (11, 41, 37, 38, 44, 54, 55, 40, 59, 60, 21, 790)


class Application(fix.Application):  # type: ignore[misc]
    """A small venue, using QuickFIX's own session handling."""

    def __init__(self, venue: Venue, begin_string: str) -> None:
        super().__init__()
        self.venue = venue
        self.begin_string = begin_string
        self.session_id: Any = None

    def onCreate(self, session_id: Any) -> None:  # noqa: N802
        self.session_id = session_id

    def onLogon(self, session_id: Any) -> None:  # noqa: N802
        self.venue.logons += 1

    def onLogout(self, session_id: Any) -> None:  # noqa: N802
        pass

    def toAdmin(self, message: Any, session_id: Any) -> None:  # noqa: N802
        # A Reject leaving QuickFIX means it refused something this initiator sent.
        msg_type = message.getHeader().getField(35)
        if msg_type == "3":
            text = message.getField(58) if message.isSetField(58) else "no text"
            with self.venue.lock:
                self.venue.rejects.append(str(text))
        elif msg_type == "2":
            with self.venue.lock:
                self.venue.resend_requests += 1

    def fromAdmin(self, message: Any, session_id: Any) -> None:  # noqa: N802
        # A Reject arriving at QuickFIX means this initiator refused something of its.
        if message.getHeader().getField(35) == "A":
            with self.venue.lock:
                for tag in (553, 554):
                    if message.isSetField(tag):
                        self.venue.logon_fields[tag] = message.getField(tag)
        if message.getHeader().getField(35) == "3":
            text = message.getField(58) if message.isSetField(58) else "no text"
            with self.venue.lock:
                self.venue.rejects_from_client.append(str(text))

    def toApp(self, message: Any, session_id: Any) -> None:  # noqa: N802
        if message.getHeader().getField(35) == "j":
            with self.venue.lock:
                self.venue.rejects.append("business message reject")

    def fromApp(self, message: Any, session_id: Any) -> None:  # noqa: N802
        header = message.getHeader()
        msg_type = header.getField(35)
        fields = {
            tag: message.getField(tag) for tag in _TAGS if message.isSetField(tag)
        }
        fields[34] = header.getField(34)
        if header.isSetField(43):
            fields[43] = header.getField(43)
        with self.venue.lock:
            self.venue.received.append((msg_type, fields))
        if msg_type == "D":
            self._on_new(fields)
        elif msg_type == "F":
            self._on_cancel(fields)
        elif msg_type == "G":
            self._on_replace(fields)
        elif msg_type == "H":
            self._on_status(fields)

    def report(self, order: dict[str, str], exec_type: str, **extra: Any) -> str:
        with self.venue.lock:
            self.venue.next_exec += 1
            exec_id = f"QF{self.venue.next_exec}"
        return self.send_report(order, exec_type, exec_id, **extra)

    def send_report(
        self,
        order: dict[str, str],
        exec_type: str,
        exec_id: str,
        *,
        wire: str | None = None,
        orig: str | None = None,
        status: str | None = None,
        last_qty: str | None = None,
        last_px: str | None = None,
        ref: str | None = None,
        trans: str = "0",
        request_id: str | None = None,
    ) -> str:
        msg = fix.Message()
        msg.getHeader().setField(35, "8")
        msg.setField(37, order["order_id"])
        msg.setField(11, wire or order["wire"])
        msg.setField(17, exec_id)
        msg.setField(39, status or order["status"])
        msg.setField(54, order["side"])
        msg.setField(55, order["symbol"])
        msg.setField(38, order["qty"])
        msg.setField(14, order["cum"])
        msg.setField(151, str(Decimal(order["qty"]) - Decimal(order["cum"])))
        msg.setField(6, order["avg"])
        msg.setField(60, format_utc_timestamp(_now()))
        if self.begin_string == "FIX.4.2":
            msg.setField(20, trans)
            msg.setField(150, exec_type)
            msg.setField(32, last_qty or "0")
            msg.setField(31, last_px or "0")
        else:
            msg.setField(150, exec_type)
            if last_qty is not None:
                msg.setField(32, last_qty)
                msg.setField(31, last_px or "0")
        if orig is not None:
            msg.setField(41, orig)
        if ref is not None:
            msg.setField(19, ref)
        if request_id is not None:
            msg.setField(790, request_id)
        fix.Session.sendToTarget(msg, self.session_id)
        return exec_id

    def _on_new(self, fields: dict[int, str]) -> None:
        cid = fields[11]
        with self.venue.lock:
            if cid in self.venue.orders:
                return
            order = {
                "order_id": f"Q{len(self.venue.orders) + 1}",
                "wire": cid,
                "root": cid,
                "side": fields[54],
                "symbol": fields[55],
                "qty": fields.get(38, "0"),
                "cum": "0",
                "avg": "0",
                "status": "0",
            }
            self.venue.orders[cid] = order
        if not self.venue.silent_orders:
            self.report(order, "0")

    def fill(self, cid: str, quantity: str, price: str) -> str:
        order = self.venue.orders[cid]
        order["cum"] = str(Decimal(order["cum"]) + Decimal(quantity))
        order["avg"] = price
        full = Decimal(order["cum"]) >= Decimal(order["qty"])
        order["status"] = "2" if full else "1"
        if self.begin_string == "FIX.4.2":
            return self.report(order, order["status"], last_qty=quantity, last_px=price)
        return self.report(order, "F", last_qty=quantity, last_px=price)

    def bust(self, cid: str, exec_ref: str, quantity: str) -> str:
        order = self.venue.orders[cid]
        order["cum"] = str(Decimal(order["cum"]) - Decimal(quantity))
        order["status"] = "0"
        if self.begin_string == "FIX.4.2":
            return self.report(order, "0", ref=exec_ref, trans="1")
        return self.report(order, "H", ref=exec_ref, last_qty="0", last_px="0")

    def _find(self, wire: str) -> dict[str, str] | None:
        for order in self.venue.orders.values():
            if order["wire"] == wire:
                return order
        return None

    def _on_cancel(self, fields: dict[int, str]) -> None:
        order = self._find(fields[41])
        if order is None:
            self._cancel_reject(fields, "1")
            return
        order["wire"] = fields[11]
        order["status"] = "4"
        self.report(order, "4", wire=fields[11], orig=fields[41])

    def _on_replace(self, fields: dict[int, str]) -> None:
        order = self._find(fields[41])
        if order is None:
            self._cancel_reject(fields, "1")
            return
        order["wire"] = fields[11]
        order["qty"] = fields.get(38, order["qty"])
        self.report(order, "5", wire=fields[11], orig=fields[41])

    def _cancel_reject(self, fields: dict[int, str], reason: str) -> None:
        msg = fix.Message()
        msg.getHeader().setField(35, "9")
        msg.setField(37, "NONE")
        msg.setField(11, fields[11])
        msg.setField(41, fields[41])
        msg.setField(39, "8")
        msg.setField(434, "1")
        msg.setField(102, reason)
        fix.Session.sendToTarget(msg, self.session_id)

    def _on_status(self, fields: dict[int, str]) -> None:
        order = None if self.venue.unknown_status else self._find(fields[11])
        request_id = fields.get(790)
        fix42 = self.begin_string == "FIX.4.2"
        if order is None:
            missing = {
                "order_id": "NONE",
                "wire": fields[11],
                "side": fields[54],
                "symbol": fields[55],
                "qty": "0",
                "cum": "0",
                "avg": "0",
                "status": "8",
            }
            msg_type = "8" if fix42 else "I"
            self._unknown(missing, msg_type, request_id)
            return
        self.send_report(
            order,
            order["status"] if fix42 else "I",
            "0",
            trans="3",
            request_id=request_id,
        )

    def _unknown(
        self, order: dict[str, str], exec_type: str, request_id: str | None
    ) -> None:
        msg = fix.Message()
        msg.getHeader().setField(35, "8")
        msg.setField(37, "NONE")
        msg.setField(11, order["wire"])
        msg.setField(17, "0")
        msg.setField(39, "8")
        msg.setField(54, order["side"])
        msg.setField(55, order["symbol"])
        msg.setField(14, "0")
        msg.setField(151, "0")
        msg.setField(6, "0")
        msg.setField(38, "0")
        msg.setField(103, "5")
        msg.setField(150, exec_type)
        if self.begin_string == "FIX.4.2":
            msg.setField(20, "3")
            msg.setField(32, "0")
            msg.setField(31, "0")
        if request_id is not None:
            msg.setField(790, request_id)
        fix.Session.sendToTarget(msg, self.session_id)


def _now() -> Any:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc)


class QuickFixVenue:
    """A running QuickFIX SocketAcceptor."""

    def __init__(self, begin_string: str, root: Path) -> None:
        self.begin_string = begin_string
        self.root = root
        self.port = free_port()
        self.venue = Venue()
        self.application = Application(self.venue, begin_string)
        self._acceptor: Any = None
        self.settings_path = root / "acceptor.cfg"
        self.settings_path.write_text(self._settings())

    def _settings(self) -> str:
        name = DICTIONARY[self.begin_string]
        lines = [
            "[DEFAULT]",
            "ConnectionType=acceptor",
            f"FileStorePath={self.root / 'store'}",
            f"FileLogPath={self.root / 'log'}",
            "StartTime=00:00:00",
            "EndTime=00:00:00",
            f"SocketAcceptPort={self.port}",
            "SocketReuseAddress=Y",
            "UseDataDictionary=Y",
            "ValidateUserDefinedFields=N",
            "ValidateFieldsOutOfOrder=Y",
            "ResetOnLogon=N",
            "ResetOnLogout=N",
            "ResetOnDisconnect=N",
            "",
            "[SESSION]",
            f"BeginString={self.begin_string}",
            "SenderCompID=VENUE",
            "TargetCompID=CLIENT",
            "HeartBtInt=30",
        ]
        if self.begin_string == "FIXT.1.1":
            lines += [
                "DefaultApplVerID=FIX.5.0SP2",
                f"TransportDataDictionary={dictionary('FIXT11')}",
                f"AppDataDictionary={dictionary(name)}",
            ]
        else:
            lines.append(f"DataDictionary={dictionary(name)}")
        return "\n".join(lines) + "\n"

    def start(self) -> None:
        settings = fix.SessionSettings(str(self.settings_path))
        self._acceptor = fix.SocketAcceptor(
            self.application,
            fix.FileStoreFactory(settings),
            settings,
            fix.FileLogFactory(settings),
        )
        self._acceptor.start()

    def stop(self) -> None:
        if self._acceptor is not None:
            self._acceptor.stop()
            self._acceptor = None

    def messages(self, msg_type: str) -> list[dict[int, str]]:
        with self.venue.lock:
            return [fields for kind, fields in self.venue.received if kind == msg_type]

    def session_log(self) -> str:
        log_dir = self.root / "log"
        return "\n".join(
            path.read_text(errors="replace")
            for path in log_dir.glob("*.event.current.log")
        )


async def eventually(predicate: Any, timeout: float = 10.0, what: str = "") -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(what or "condition not met in time")
        await asyncio.sleep(0.01)


def _assert_no_rejects(venue: QuickFixVenue) -> None:
    assert venue.venue.rejects == [], venue.session_log()
    assert venue.venue.rejects_from_client == [], venue.session_log()


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


class Client:
    """A provider, its store, and a collected execution stream."""

    def __init__(self, venue: QuickFixVenue, root: Path) -> None:
        self.venue = venue
        self.path = root / "client.db"
        self.store: FixStore | None = None
        self.provider: FixProvider | None = None
        self.events: list[OrderEvent] = []
        self._task: asyncio.Task[None] | None = None

    def config(self) -> FixSessionConfig:
        begin = self.venue.begin_string
        return FixSessionConfig(
            host="127.0.0.1",
            port=self.venue.port,
            begin_string=begin,
            sender_comp_id="CLIENT",
            target_comp_id="VENUE",
            heartbeat_interval=30,
            default_appl_ver_id=APPL_VER_FIX50SP2 if begin == "FIXT.1.1" else None,
            logon_timeout=5.0,
            logout_timeout=5.0,
            reconnect_backoff=ExponentialBackoff(base=0.05, maximum=0.2, jitter=False),
        )

    def open_store(self) -> SqliteFixStore:
        store = SqliteFixStore(self.path, self.config().session_id)
        self.store = store
        return store

    async def connect(self, *, ack_timeout: float = 5.0) -> FixProvider:
        store = self.store if self.store is not None else self.open_store()
        self.provider = FixProvider(
            self.config(),
            store,
            ack_timeout=ack_timeout,
            query_timeout=5.0,
            logon_timeout=10.0,
        )
        await self.provider.open()
        self._task = asyncio.create_task(self._collect(self.provider))
        return self.provider

    async def _collect(self, provider: FixProvider) -> None:
        with contextlib.suppress(Exception):
            async for event in provider.stream_executions():
                self.events.append(event)

    def of(self, kind: type[Any]) -> list[Any]:
        return [e for e in self.events if isinstance(e, kind)]

    async def disconnect(self, *, close_store: bool = True) -> None:
        if self.provider is not None:
            await self.provider.close()
            self.provider = None
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if close_store and self.store is not None:
            self.store.close()
            self.store = None


@pytest.fixture(params=VERSIONS)
async def venue(
    request: pytest.FixtureRequest, tmp_path: Path
) -> AsyncIterator[QuickFixVenue]:
    instance = QuickFixVenue(str(request.param), tmp_path)
    await asyncio.to_thread(instance.start)
    yield instance
    await asyncio.to_thread(instance.stop)


@pytest.fixture
async def client(venue: QuickFixVenue, tmp_path: Path) -> AsyncIterator[Client]:
    instance = Client(venue, tmp_path)
    yield instance
    await instance.disconnect()


class TestOrderFlow:
    async def test_logon_and_logout(self, venue: QuickFixVenue, client: Client) -> None:
        provider = await client.connect()
        assert provider.session.state is SessionState.READY
        await eventually(lambda: venue.venue.logons == 1)
        await client.disconnect()
        _assert_no_rejects(venue)

    async def test_order_life(self, venue: QuickFixVenue, client: Client) -> None:
        provider = await client.connect()
        ack = await provider.place_order(order())
        assert ack.venue_order_id == "Q1"
        assert venue.venue.orders["C1"]["qty"] == "100"
        wire = venue.messages("D")[0]
        assert wire[44] == "10.25"
        assert wire[54] == "1"

        snapshot = await provider.order_status("C1", ACME)
        assert snapshot is not None
        assert snapshot.status is OrderStatus.ACCEPTED

        first = venue.application.fill("C1", "40", "10.25")
        await eventually(lambda: bool(client.of(Filled)))
        fill = client.of(Filled)[0].fill
        assert fill.fill_id == first
        assert fill.quantity == Decimal(40)
        assert fill.price == Decimal("10.25")

        await provider.amend_order("C1", ACME, quantity=Decimal(90))
        await eventually(lambda: bool(client.of(Replaced)))
        assert venue.venue.orders["C1"]["qty"] == "90"
        assert venue.messages("G")[0][41] == "C1"
        assert venue.messages("G")[0][11] == "C1~1"

        await provider.cancel_order("C1", ACME)
        await eventually(lambda: bool(client.of(Canceled)))
        assert client.of(Canceled)[0].client_order_id == "C1"
        assert venue.messages("F")[0][41] == "C1~1"

        _assert_no_rejects(venue)
        assert any(isinstance(e, Acknowledged) for e in client.events)

    async def test_trade_bust(self, venue: QuickFixVenue, client: Client) -> None:
        provider = await client.connect()
        await provider.place_order(order())
        exec_id = venue.application.fill("C1", "40", "10")
        await eventually(lambda: bool(client.of(Filled)))
        venue.application.bust("C1", exec_id, "40")
        await eventually(lambda: bool(client.of(FillBusted)))
        assert client.of(FillBusted)[0].fill_id == exec_id
        _assert_no_rejects(venue)

    async def test_status_of_an_order_the_venue_does_not_have(
        self, venue: QuickFixVenue, client: Client
    ) -> None:
        provider = await client.connect()
        await provider.place_order(order())
        venue.venue.unknown_status = True
        assert await provider.order_status("C1", ACME) is None
        _assert_no_rejects(venue)

    async def test_cancel_of_an_order_the_venue_does_not_have(
        self, venue: QuickFixVenue, client: Client
    ) -> None:
        provider = await client.connect()
        await provider.place_order(order())
        venue.venue.orders.clear()
        with pytest.raises(VenueError) as info:
            await provider.cancel_order("C1", ACME)
        assert info.value.code == "1"

    async def test_lost_ack_is_ambiguous_and_the_venue_has_one_order(
        self, venue: QuickFixVenue, client: Client
    ) -> None:
        provider = await client.connect(ack_timeout=0.2)
        venue.venue.silent_orders = True
        with pytest.raises(AmbiguousOutcome):
            await provider.place_order(order())
        snapshot = await provider.order_status("C1", ACME)
        assert snapshot is not None
        assert snapshot.status is OrderStatus.ACCEPTED
        assert len(venue.messages("D")) == 1
        _assert_no_rejects(venue)


class TestCredentials:
    async def test_quickfix_accepts_username_and_password_at_logon(
        self, venue: QuickFixVenue, client: Client
    ) -> None:
        if venue.begin_string == "FIX.4.2":
            pytest.skip("FIX 4.2 defines no Username or Password in Logon")
        config = replace(
            client.config(),
            username=SecretStr("alice"),
            password=SecretStr("hunter2-do-not-leak"),
        )
        provider = FixProvider(config, client.open_store(), logon_timeout=10.0)
        client.provider = provider
        await provider.open()
        await eventually(lambda: venue.venue.logons == 1)
        with venue.venue.lock:
            assert venue.venue.logon_fields[554] == "hunter2-do-not-leak"
            assert venue.venue.logon_fields[553] == "alice"
        _assert_no_rejects(venue)


class TestSequenceRecovery:
    async def test_numbers_continue_across_a_restart(
        self, venue: QuickFixVenue, client: Client
    ) -> None:
        provider = await client.connect()
        await provider.place_order(order())
        await client.disconnect()
        store = client.open_store()
        assert store.next_outbound() > 2
        provider = await client.connect()
        await eventually(lambda: venue.venue.logons == 2)
        await provider.place_order(order("C2"))
        assert len(venue.messages("D")) == 2
        _assert_no_rejects(venue)

    async def test_we_recover_a_gap_from_quickfix(
        self, venue: QuickFixVenue, client: Client
    ) -> None:
        """QuickFIX retransmits with PossDup, and gap-fills its admin messages."""
        provider = await client.connect()
        await provider.place_order(order())
        first = venue.application.fill("C1", "40", "10")
        await eventually(lambda: bool(client.of(Filled)))
        await client.disconnect()

        store = client.open_store()
        expected = store.next_inbound()
        assert expected > 3
        store.set_next_inbound(1)
        client.events.clear()

        provider = await client.connect()
        await eventually(lambda: provider.session.next_inbound >= expected)
        # The fill came back as PossDup, and ExecID removed it as a repeat.
        assert client.of(Filled) == []
        since = _now().replace(year=2000)
        assert [f.fill_id for f in await provider.fills_since(since)] == [first]
        assert provider.session.state is SessionState.READY
        _assert_no_rejects(venue)

    async def test_an_execution_never_seen_is_replayed_by_quickfix(
        self, venue: QuickFixVenue, client: Client
    ) -> None:
        """QuickFIX sends a fill to a dead socket, then replays it after the Logon."""
        provider = await client.connect()
        await provider.place_order(order())
        await client.disconnect()
        store = client.open_store()
        seen = store.next_inbound()
        store.close()
        client.store = None

        # Wait for QuickFIX to see the disconnect, then have it store a fill to send.
        await eventually(
            lambda: (
                not fix.Session.lookupSession(venue.application.session_id).isLoggedOn()
            )
        )
        exec_id = venue.application.fill("C1", "40", "10")

        client.open_store()
        client.events.clear()
        provider = await client.connect()
        await eventually(lambda: bool(client.of(Filled)), what="fill was not replayed")
        fill = client.of(Filled)[0].fill
        assert fill.fill_id == exec_id
        assert provider.session.next_inbound > seen
        assert client.store is not None
        replayed = [e for e in client.store.fills_since(0) if e.exec_id == exec_id]
        assert len(replayed) == 1
        _assert_no_rejects(venue)

    async def test_quickfix_recovers_a_gap_from_us(
        self, venue: QuickFixVenue, client: Client
    ) -> None:
        """A number we skipped: QuickFIX asks for it, and we gap-fill it."""
        provider = await client.connect()
        await provider.place_order(order())
        await client.disconnect()

        store = client.open_store()
        store.set_next_outbound(store.next_outbound() + 3)

        provider = await client.connect()
        await provider.place_order(order("C2"))
        assert list(venue.venue.orders) == ["C1", "C2"]
        _assert_no_rejects(venue)
        assert venue.venue.resend_requests >= 1

    async def test_an_order_stored_but_never_written_reaches_quickfix_by_resend(
        self, venue: QuickFixVenue, client: Client
    ) -> None:
        """A crash after persisting and before the socket, then a restart."""
        provider = await client.connect()
        await provider.place_order(order())
        await client.disconnect()

        store = client.open_store()
        now = format_utc_timestamp(_now())
        fields: list[tuple[int, Any]] = [
            (11, "CRASHED"),
            (55, "ACME"),
            (54, "1"),
            (60, now),
            (38, "5"),
            (40, "2"),
            (44, "10"),
            (59, "0"),
        ]
        if venue.begin_string == "FIX.4.2":
            fields.insert(0, (21, "1"))
        store.allocate_outbound(StoredMessage(0, "D", now, encode_fields(fields)))

        await client.connect()
        await eventually(lambda: "CRASHED" in venue.venue.orders)
        copies = [m for m in venue.messages("D") if m[11] == "CRASHED"]
        assert len(copies) == 1
        assert copies[0][43] == "Y"
        _assert_no_rejects(venue)

    async def test_our_number_too_low_is_refused_by_quickfix(
        self, venue: QuickFixVenue, client: Client
    ) -> None:
        provider = await client.connect()
        await provider.place_order(order())
        await client.disconnect()

        store = client.open_store()
        store.set_next_outbound(1)
        config = client.config()
        provider = FixProvider(config, store, logon_timeout=5.0)
        client.provider = provider
        with pytest.raises(LogonRejectedError, match=r"(?i)too low"):
            await provider.open()
        assert provider.session.state is SessionState.FAILED
        assert venue.venue.logons == 1


class TestValidationIsReallyOn:
    async def test_quickfix_rejects_an_order_missing_a_required_field(
        self, venue: QuickFixVenue, client: Client
    ) -> None:
        """Proves the dictionary is enforced, so a clean run above means something."""
        provider = await client.connect()
        await provider.session.send_application("D", [(11, "BAD"), (55, "ACME")])
        await eventually(lambda: bool(venue.venue.rejects))
        assert venue.messages("D") == []

    async def test_a_quickfix_session_reject_fails_the_request(
        self, venue: QuickFixVenue, client: Client
    ) -> None:
        provider = await client.connect()
        fields = [(11, "BADTYPE"), (55, "ACME"), (54, "1"), (60, "not a time")]
        waiter = provider._new_pending(_Kind.NEW, "BADTYPE", "BADTYPE", "BADTYPE")
        try:
            await provider._send_and_wait(waiter, "D", list(fields))
        except VenueError as exc:
            assert (exc.code or "").startswith("SESSION_")
        else:
            raise AssertionError("expected a session reject")
        finally:
            provider._drop_pending(waiter)
