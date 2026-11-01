"""An in-process FIX acceptor over a real localhost socket, with scriptable faults.

It behaves like a small venue: sequence numbers and sent messages persist across
connections, it answers ResendRequest with PossDup and gap fills, and it holds orders
keyed by ClOrdID so a retransmitted order cannot be accepted twice. ``orders`` and
``received`` are what actually reached it, independent of what the initiator believes.

Framing goes through :mod:`ismet.fix.message`. The codec is checked independently by
golden vectors in ``test_message.py`` and against real QuickFIX in the interop test.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from ismet.fix.message import (
    FixMessage,
    FixStreamParser,
    GarbledFrame,
    encode,
    format_utc_timestamp,
)

FieldList = list[tuple[int, str]]
ADMIN = {"0", "1", "2", "3", "4", "5", "A"}
CLOSED = {"2", "4", "8", "C"}


@dataclass
class VenueOrder:
    cl_ord_id: str
    wire_id: str
    symbol: str
    side: str
    quantity: Decimal
    price: Decimal | None
    order_id: str
    status: str = "0"
    filled: Decimal = Decimal(0)
    avg_px: Decimal = Decimal(0)
    arrivals: int = 1


@dataclass
class Faults:
    """Faults consumed by the next matching event."""

    drop_after_type: dict[str, int] = field(default_factory=dict)
    lose_ack: int = 0
    garble_next: int = 0
    truncate_next: int = 0
    skip_seq_next: int = 0
    silent: bool = False
    ignore_status_requests: bool = False
    status_unknown_via_business_reject: bool = False
    status_business_reject_reason: str | None = None
    cancel_reject: str | None = None
    replace_reject: str | None = None
    order_reject: str | None = None
    business_reject_orders: str | None = None
    session_reject_orders: str | None = None
    refuse_logon: str | None = None
    ignore_logon: bool = False
    first_message_type: str | None = None
    suppress_test_response: bool = False
    bad_sending_time_next: int = 0
    wrong_comp_id_next: int = 0
    wrong_begin_string_next: int = 0
    no_logout_reply: bool = False
    reject_even_if_known: bool = False


class FixAcceptor:
    """Listens on an ephemeral port for one initiator at a time."""

    def __init__(
        self,
        begin_string: str = "FIX.4.4",
        *,
        sender: str = "VENUE",
        target: str = "CLIENT",
        fix42_style: bool | None = None,
    ) -> None:
        self.begin_string = begin_string
        self.sender = sender
        self.target = target
        self.fix42_style = (
            begin_string == "FIX.4.2" if fix42_style is None else fix42_style
        )
        self.faults = Faults()
        self.next_out = 1
        self.next_in = 1
        self.sent: dict[int, tuple[str, FieldList, str]] = {}
        self.received: list[FixMessage] = []
        self.garbled_seen: list[str] = []
        self.orders: dict[str, VenueOrder] = {}
        self.wire: dict[str, str] = {}
        self.connections = 0
        self.logons: list[FixMessage] = []
        self.disconnects = 0
        self._server: asyncio.AbstractServer | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._order_counter = 0
        self._exec_counter = 0
        self._changed = asyncio.Condition()
        self._logged_on = False
        self.port = 0
        self.on_message: Callable[[FixMessage], None] | None = None
        self.resend_requests_answered = 0
        self.logout_text: str | None = None
        self.heartbeat_interval = 30
        self._reset_answer_due = False
        self._hb_task: asyncio.Task[None] | None = None

    # Lifecycle

    async def start(self, *, port: int = 0) -> None:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", port)
        self.port = self._server.sockets[0].getsockname()[1]

    async def restart(self) -> None:
        await self.start(port=self.port)

    async def stop(self) -> None:
        server, self._server = self._server, None
        if server is not None:
            server.close()
        await self.drop_connection()
        for task in list(self._tasks):
            task.cancel()
        for task in list(self._tasks):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if server is not None:
            # On 3.12 and later this waits for every connection to finish.
            await server.wait_closed()

    async def drop_connection(self) -> None:
        writer, self._writer = self._writer, None
        self._logged_on = False
        if writer is not None:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    @property
    def connected(self) -> bool:
        return self._writer is not None

    async def wait_until(
        self, predicate: Callable[[], bool], timeout: float = 5.0
    ) -> None:
        async def waiter() -> None:
            async with self._changed:
                while not predicate():
                    await self._changed.wait()

        await asyncio.wait_for(waiter(), timeout)

    async def _notify(self) -> None:
        async with self._changed:
            self._changed.notify_all()

    # What arrived

    def of_type(self, msg_type: str) -> list[FixMessage]:
        return [m for m in self.received if m.msg_type == msg_type]

    # Sending

    def _now(self) -> str:
        return format_utc_timestamp(datetime.now(timezone.utc))

    def frame(
        self,
        seq: int,
        msg_type: str,
        fields: FieldList,
        *,
        poss_dup: bool = False,
        orig_sending_time: str | None = None,
    ) -> bytes:
        faults = self.faults
        sending_time = self._now()
        sender, target, begin = self.sender, self.target, self.begin_string
        if faults.bad_sending_time_next:
            faults.bad_sending_time_next -= 1
            stale = datetime.now(timezone.utc) - timedelta(hours=1)
            sending_time = format_utc_timestamp(stale)
        if faults.wrong_comp_id_next:
            faults.wrong_comp_id_next -= 1
            sender = "IMPOSTOR"
        if faults.wrong_begin_string_next:
            faults.wrong_begin_string_next -= 1
            begin = "FIX.4.1"
        header: FieldList = [
            (35, msg_type),
            (49, sender),
            (56, target),
            (34, str(seq)),
        ]
        if poss_dup:
            header.append((43, "Y"))
        header.append((52, sending_time))
        if orig_sending_time is not None:
            header.append((122, orig_sending_time))
        return encode(begin, [*header, *fields])

    async def write(self, data: bytes) -> None:
        if self._writer is None:
            raise ConnectionError("no connection")
        self._writer.write(data)
        await self._writer.drain()

    async def send(self, msg_type: str, fields: FieldList) -> int:
        """Send and remember a message, applying any scripted framing fault."""
        faults = self.faults
        if faults.skip_seq_next:
            self.next_out += faults.skip_seq_next
            faults.skip_seq_next = 0
        seq = self.next_out
        self.next_out += 1
        sending_time = self._now()
        self.sent[seq] = (msg_type, list(fields), sending_time)
        data = self.frame(seq, msg_type, fields)
        if faults.garble_next:
            faults.garble_next -= 1
            data = _corrupt_checksum(data)
        if faults.truncate_next:
            faults.truncate_next -= 1
            await self.write(data[: len(data) // 2])
            await self.drop_connection()
            return seq
        await self.write(data)
        return seq

    async def send_raw_seq(
        self, seq: int, msg_type: str, fields: FieldList, **kw: object
    ) -> None:
        """Send with an exact sequence number and no bookkeeping."""
        await self.write(self.frame(seq, msg_type, fields, **kw))  # type: ignore[arg-type]

    async def reset_sequences_at_logon(self) -> None:
        """Start a new FIX session from this side: a Logon at 1 with ResetSeqNumFlag."""
        self.next_in = 1
        self.next_out = 2
        self.sent.clear()
        self._reset_answer_due = True
        await self.write(self.frame(1, "A", [(98, "0"), (108, "30"), (141, "Y")]))

    # Accepting

    async def _accept(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        assert task is not None
        self._tasks.add(task)
        self.connections += 1
        self._writer = writer
        parser = FixStreamParser()
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                for item in parser.feed(data):
                    if isinstance(item, GarbledFrame):
                        self.garbled_seen.append(item.reason)
                        continue
                    self.received.append(item)
                    if self.on_message is not None:
                        self.on_message(item)
                    await self._handle(item)
                    await self._notify()
                    remaining = self.faults.drop_after_type.get(item.msg_type, 0)
                    if remaining:
                        self.faults.drop_after_type[item.msg_type] = remaining - 1
                        await self.drop_connection()
                        return
        except (ConnectionError, OSError):
            pass
        finally:
            self.disconnects += 1
            self._logged_on = False
            if self._writer is writer:
                self._writer = None
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            self._tasks.discard(task)
            await self._notify()

    async def _handle(self, message: FixMessage) -> None:
        msg_type = message.msg_type
        seq = int(message.require(34))
        if self.faults.silent and msg_type != "A":
            if seq == self.next_in:
                self.next_in += 1
            return
        if msg_type == "A":
            await self._on_logon(message, seq)
            return
        if msg_type == "4" and message.get(123) != "Y":
            self.next_in = int(message.require(36))
            return
        if seq < self.next_in:
            if message.get(43) == "Y":
                return
            await self.send(
                "5",
                [(58, f"MsgSeqNum too low, expecting {self.next_in} received {seq}")],
            )
            await self.drop_connection()
            return
        if seq > self.next_in:
            if msg_type == "2":
                await self._on_resend_request(message)
            await self.send("2", [(7, str(self.next_in)), (16, "0")])
            return
        if msg_type == "4":
            self.next_in = int(message.require(36))
            return
        self.next_in = seq + 1
        if msg_type in ADMIN:
            await self._on_admin(message)
        else:
            await self._on_application(message)

    async def _on_logon(self, message: FixMessage, seq: int) -> None:
        self.logons.append(message)
        if self._reset_answer_due:
            self._reset_answer_due = False
            self.next_in = seq + 1
            return
        faults = self.faults
        self.heartbeat_interval = int(message.require(108))
        if faults.ignore_logon:
            return
        if faults.refuse_logon is not None:
            await self.send("5", [(58, faults.refuse_logon)])
            await self.drop_connection()
            return
        if faults.first_message_type is not None:
            await self.send(faults.first_message_type, [(112, "NOPE")])
            return
        if message.get(141) == "Y":
            self.next_in = 1
            self.next_out = 1
            self.sent.clear()
        gap = seq > self.next_in
        if seq < self.next_in:
            await self.send(
                "5",
                [(58, f"MsgSeqNum too low, expecting {self.next_in} received {seq}")],
            )
            await self.drop_connection()
            return
        if not gap:
            self.next_in = seq + 1
        fields: FieldList = [(98, "0"), (108, message.require(108))]
        if message.get(141) == "Y":
            fields.append((141, "Y"))
        if message.get(1137) is not None:
            fields.append((1137, message.require(1137)))
        await self.send("A", fields)
        self._logged_on = True
        if gap:
            await self.send("2", [(7, str(self.next_in)), (16, "0")])

    async def _on_admin(self, message: FixMessage) -> None:
        msg_type = message.msg_type
        if msg_type == "1":
            if not self.faults.suppress_test_response:
                await self.send("0", [(112, message.require(112))])
        elif msg_type == "2":
            if message.get(43) != "Y":
                await self._on_resend_request(message)
        elif msg_type == "5":
            self.logout_text = message.get(58)
            if not self.faults.no_logout_reply:
                await self.send("5", [(58, "bye")])
            await self.drop_connection()

    async def _on_resend_request(self, message: FixMessage) -> None:
        self.resend_requests_answered += 1
        begin = int(message.require(7))
        end = int(message.require(16))
        last = self.next_out - 1
        if end == 0 or end > last:
            end = last
        gap_start: int | None = None
        for seq in range(begin, end + 1):
            sent = self.sent.get(seq)
            if sent is None or sent[0] in ADMIN:
                gap_start = seq if gap_start is None else gap_start
                continue
            if gap_start is not None:
                await self._gap_fill(gap_start, seq)
                gap_start = None
            await self.write(
                self.frame(
                    seq, sent[0], sent[1], poss_dup=True, orig_sending_time=sent[2]
                )
            )
        if gap_start is not None:
            await self._gap_fill(gap_start, end + 1)

    async def _gap_fill(self, begin: int, new_seq: int) -> None:
        await self.write(
            self.frame(
                begin,
                "4",
                [(123, "Y"), (36, str(new_seq))],
                poss_dup=True,
                orig_sending_time=self._now(),
            )
        )

    # Application

    async def _on_application(self, message: FixMessage) -> None:
        msg_type = message.msg_type
        if msg_type == "D":
            await self._on_new_order(message)
        elif msg_type == "F":
            await self._on_cancel(message)
        elif msg_type == "G":
            await self._on_replace(message)
        elif msg_type == "H":
            await self._on_status(message)

    def next_exec_id(self) -> str:
        self._exec_counter += 1
        return f"E{self._exec_counter}"

    async def _on_new_order(self, message: FixMessage) -> None:
        faults = self.faults
        cid = message.require(11)
        if faults.business_reject_orders is not None:
            await self.send(
                "j",
                [
                    (45, message.require(34)),
                    (372, "D"),
                    (379, cid),
                    (380, faults.business_reject_orders),
                    (58, "business says no"),
                ],
            )
            return
        if faults.session_reject_orders is not None:
            await self.send(
                "3",
                [
                    (45, message.require(34)),
                    (373, faults.session_reject_orders),
                    (58, "session says no"),
                ],
            )
            return
        existing = self.orders.get(cid)
        if existing is not None:
            existing.arrivals += 1
            if faults.reject_even_if_known:
                await self.exec_report(
                    cid, "8", "8", rej_reason=faults.order_reject, text="duplicate"
                )
            return
        if faults.order_reject is not None:
            self._order_counter += 1
            self.orders[cid] = VenueOrder(
                cid,
                cid,
                message.get(55) or "UNKNOWN",
                message.get(54) or "1",
                Decimal(message.get(38) or "1"),
                None,
                f"O{self._order_counter}",
                status="8",
            )
            await self.exec_report(cid, "8", "8", rej_reason=faults.order_reject)
            return
        price = message.get(44)
        self._order_counter += 1
        self.orders[cid] = VenueOrder(
            cid,
            cid,
            message.get(55) or "UNKNOWN",
            message.get(54) or "1",
            Decimal(message.get(38) or "1"),
            None if price is None else Decimal(price),
            f"O{self._order_counter}",
        )
        self.wire[cid] = cid
        if faults.lose_ack:
            faults.lose_ack -= 1
            return
        await self.exec_report(cid, "0", "0")

    def _order_for(self, wire_id: str) -> VenueOrder | None:
        root = self.wire.get(wire_id)
        return None if root is None else self.orders.get(root)

    async def _on_cancel(self, message: FixMessage) -> None:
        wire_id = message.require(11)
        orig = message.require(41)
        order = self._order_for(orig)
        reason = self.faults.cancel_reject
        if reason is None and order is not None and order.status in CLOSED:
            reason = "0"  # too late to cancel, as a venue answers for a closed order
        if order is None or reason is not None:
            await self.send(
                "9",
                [
                    (37, order.order_id if order else "NONE"),
                    (11, wire_id),
                    (41, orig),
                    (39, order.status if order else "8"),
                    (434, "1"),
                    (102, reason if reason is not None else "1"),
                    (58, "cannot cancel"),
                ],
            )
            return
        self.wire[wire_id] = order.cl_ord_id
        order.wire_id = wire_id
        order.status = "4"
        await self.exec_report(order.cl_ord_id, "4", "4", wire_id=wire_id, orig=orig)

    async def _on_replace(self, message: FixMessage) -> None:
        wire_id = message.require(11)
        orig = message.require(41)
        order = self._order_for(orig)
        if order is None or self.faults.replace_reject is not None:
            await self.send(
                "9",
                [
                    (37, order.order_id if order else "NONE"),
                    (11, wire_id),
                    (41, orig),
                    (39, order.status if order else "8"),
                    (434, "2"),
                    (102, self.faults.replace_reject or "1"),
                    (58, "cannot replace"),
                ],
            )
            return
        self.wire[wire_id] = order.cl_ord_id
        order.wire_id = wire_id
        order.quantity = Decimal(message.require(38))
        price = message.get(44)
        order.price = None if price is None else Decimal(price)
        await self.exec_report(
            order.cl_ord_id, "5", order.status, wire_id=wire_id, orig=orig
        )

    async def _on_status(self, message: FixMessage) -> None:
        faults = self.faults
        if faults.ignore_status_requests:
            return
        wire_id = message.require(11)
        order = self._order_for(wire_id)
        request_id = message.get(790)
        if order is None or faults.status_unknown_via_business_reject:
            if faults.status_unknown_via_business_reject or (
                faults.status_business_reject_reason is not None
            ):
                await self.send(
                    "j",
                    [
                        (45, message.require(34)),
                        (372, "H"),
                        (379, wire_id),
                        (380, faults.status_business_reject_reason or "1"),
                        (58, "no such order"),
                    ],
                )
                return
            fields: FieldList = [
                (37, "NONE"),
                (11, wire_id),
                (17, "0"),
                (39, "8"),
                (54, message.require(54)),
                (55, message.require(55)),
                (38, "0"),
                (14, "0"),
                (151, "0"),
                (6, "0"),
                (103, "5"),
                (58, "unknown order"),
            ]
            if self.fix42_style:
                fields.extend([(20, "3"), (150, "8")])
            else:
                fields.append((150, "I"))
            if request_id is not None:
                fields.append((790, request_id))
            await self.send("8", fields)
            return
        await self.exec_report(
            order.cl_ord_id,
            "I",
            order.status,
            wire_id=wire_id,
            request_id=request_id,
            status_reply=True,
        )

    async def exec_report(
        self,
        cl_ord_id: str,
        exec_type: str,
        ord_status: str,
        *,
        wire_id: str | None = None,
        orig: str | None = None,
        last_qty: Decimal | None = None,
        last_px: Decimal | None = None,
        exec_id: str | None = None,
        exec_ref_id: str | None = None,
        rej_reason: str | None = None,
        request_id: str | None = None,
        status_reply: bool = False,
        text: str | None = None,
        extra: FieldList | None = None,
    ) -> str:
        """Report on an order, in the form this acceptor's FIX version uses."""
        order = self.orders[cl_ord_id]
        identifier = exec_id or ("0" if status_reply else self.next_exec_id())
        leaves = Decimal(0) if ord_status in CLOSED else order.quantity - order.filled
        fields: FieldList = [
            (37, order.order_id),
            (11, wire_id or order.wire_id),
            (17, identifier),
            (39, ord_status),
            (54, order.side),
            (55, order.symbol),
            (38, str(order.quantity)),
            (14, str(order.filled)),
            (151, str(leaves)),
            (6, str(order.avg_px)),
            (60, self._now()),
        ]
        if orig is not None:
            fields.append((41, orig))
        if order.price is not None:
            fields.append((44, str(order.price)))
        if self.fix42_style:
            if status_reply:
                wire_exec, trans = ord_status, "3"
            elif exec_type == "F":
                wire_exec = "2" if ord_status == "2" else "1"
                trans = "0"
            elif exec_type == "H":
                wire_exec, trans = ord_status, "1"
            elif exec_type == "G":
                wire_exec, trans = ord_status, "2"
            else:
                wire_exec, trans = exec_type, "0"
            fields.append((20, trans))
            fields.append((150, wire_exec))
        else:
            fields.append((150, exec_type))
        if last_qty is not None:
            fields.append((32, str(last_qty)))
        if last_px is not None:
            fields.append((31, str(last_px)))
        if exec_ref_id is not None:
            fields.append((19, exec_ref_id))
        if rej_reason is not None:
            fields.append((103, rej_reason))
        if request_id is not None:
            fields.append((790, request_id))
        if text is not None:
            fields.append((58, text))
        if extra:
            fields.extend(extra)
        await self.send("8", fields)
        return identifier

    async def fill(
        self,
        cl_ord_id: str,
        quantity: str,
        price: str,
        *,
        exec_id: str | None = None,
        extra: FieldList | None = None,
    ) -> str:
        order = self.orders[cl_ord_id]
        qty, px = Decimal(quantity), Decimal(price)
        order.avg_px = (order.avg_px * order.filled + px * qty) / (order.filled + qty)
        order.filled += qty
        order.status = "2" if order.filled >= order.quantity else "1"
        return await self.exec_report(
            cl_ord_id,
            "F",
            order.status,
            last_qty=qty,
            last_px=px,
            exec_id=exec_id,
            extra=extra,
        )

    async def bust(self, cl_ord_id: str, exec_ref_id: str, quantity: str) -> str:
        order = self.orders[cl_ord_id]
        order.filled -= Decimal(quantity)
        order.status = "1" if order.filled > 0 else "0"
        return await self.exec_report(
            cl_ord_id,
            "H",
            order.status,
            exec_ref_id=exec_ref_id,
            last_qty=Decimal(0),
            last_px=Decimal(0),
        )

    async def correct(
        self, cl_ord_id: str, exec_ref_id: str, old_qty: str, quantity: str, price: str
    ) -> str:
        order = self.orders[cl_ord_id]
        order.filled += Decimal(quantity) - Decimal(old_qty)
        order.status = "2" if order.filled >= order.quantity else "1"
        return await self.exec_report(
            cl_ord_id,
            "G",
            order.status,
            exec_ref_id=exec_ref_id,
            last_qty=Decimal(quantity),
            last_px=Decimal(price),
        )


def _corrupt_checksum(frame: bytes) -> bytes:
    digits = frame[-4:-1]
    wrong = (int(digits) + 1) % 256
    return frame[:-4] + f"{wrong:03d}".encode() + frame[-1:]


__all__ = ["Faults", "FixAcceptor", "VenueOrder"]
