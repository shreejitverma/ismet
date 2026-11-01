"""Durable FIX session state: sequence numbers, outbound messages, and order context.

Spec references (FIXT 1.1 "FIX Transport" document):

- "Session Protocol": a FIX session spans physical connections, and only resetting both
  counters to 1 begins a new one, so the counters must survive a restart.
- "Message Recovery": a sender must answer a ResendRequest with the original
  application messages or a gap fill, so an outbound application message is durable
  before it reaches the socket. Administrative messages only consume a sequence number,
  because they are gap-filled on resend.

Allocating a sequence number and storing its message is one transaction, so after a
crash a number is either unused or recoverable. :class:`SqliteFixStore` commits with
``synchronous=FULL``, so a write has been fsynced when the call returns.

The store also keeps what the provider must not lose: the terms of each order (FIX
requires Side on a cancel or status request, which the capability's signature does not
supply), the chain of ClOrdIDs, and each execution by ExecID. A report's order update
and its execution record commit together, so a crash cannot apply half of a report.
Credentials never reach this module, because Logon is not stored.
"""

from __future__ import annotations

import dataclasses
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from ismet.errors import IsmetError


class FixStoreError(IsmetError):
    """The FIX store failed. Nothing further is sent until it can write."""


@dataclass(frozen=True)
class StoredMessage:
    """An application message as it was first sent, without its session header."""

    seq: int
    msg_type: str
    sending_time: str
    body: bytes


@dataclass(frozen=True)
class StoredExecution:
    """One non-status execution report. ``fill`` is JSON if the report was a fill."""

    client_order_id: str
    exec_id: str
    digest: str
    fill: str | None
    busted: bool
    received_at: str
    latest_us: int = 0
    """The later of the venue's time and receipt, in microseconds since the epoch."""


@dataclass(frozen=True)
class QuarantinedMessage:
    """A message rejected or discarded unapplied. ``content`` has credentials masked.

    ``halted`` means it may have been a fill, so the stream stays stopped until an
    operator's release is recorded here. A row whose ``msg_type`` is ``*`` stands for a
    range of numbers the peer skipped or reset away, which may have held fills.
    """

    seq: int
    msg_type: str
    content: str
    reason: str
    received_at: str
    halted: bool = False
    released_by: str | None = None
    release_reason: str | None = None
    released_at: str | None = None


class FixStore(Protocol):
    """Session and order state. Every mutating call is durable when it returns."""

    def next_outbound(self) -> int: ...

    def next_inbound(self) -> int: ...

    def allocate_outbound(self, message: StoredMessage | None = None) -> int:
        """Consume the next outbound number, storing ``message`` under it.

        ``message.seq`` is ignored; the allocated number is returned.
        """
        ...

    def set_next_inbound(self, value: int) -> None: ...

    def messages(self, begin: int, end: int) -> list[StoredMessage]: ...

    def reset_sequences(self) -> None:
        """Begin a new FIX session: both numbers become 1, stored messages go."""
        ...

    def reset_period(self) -> str | None:
        """The session period of the last confirmed ``reset_on_logon``."""
        ...

    def set_reset_period(self, period: str) -> None: ...

    def put_order(
        self, client_order_id: str, payload: str, *, is_open: bool = True
    ) -> None: ...

    def delete_order(self, client_order_id: str) -> None: ...

    def get_order(self, client_order_id: str) -> str | None: ...

    def open_orders(self) -> list[str]:
        """Payloads last stored with ``is_open``, found through an index."""
        ...

    def link_wire_id(self, wire_id: str, client_order_id: str) -> None: ...

    def resolve_wire_id(self, wire_id: str) -> str | None: ...

    def execution(self, client_order_id: str, exec_id: str) -> StoredExecution | None:
        """A recorded report, for deduplication."""
        ...

    def record_report(
        self,
        client_order_id: str,
        *,
        order_payload: str | None,
        order_open: bool = True,
        execution: StoredExecution | None = None,
        bust_exec_id: str | None = None,
    ) -> None:
        """Atomically update the order, add the execution, and mark a bust."""
        ...

    def fills_since(self, since_us: int) -> list[StoredExecution]:
        """Unbusted fills with ``latest_us`` at or after ``since_us``, by index."""
        ...

    def busted_executions(self) -> list[StoredExecution]:
        """Every execution marked busted, found through an index."""
        ...

    def quarantine(self, record: QuarantinedMessage) -> None:
        """Keep a message that consumed a sequence number but could not be applied."""
        ...

    def quarantined(self) -> list[QuarantinedMessage]: ...

    def release_quarantine(
        self, operator: str, reason: str, at: str, *, expected: int
    ) -> int:
        """Record an operator's release of every unreleased row, in one transaction.

        Returns how many unreleased rows there are. Nothing changes unless that equals
        ``expected``, the count the operator saw, so a row that arrived meanwhile is
        never released unseen.
        """
        ...

    def close(self) -> None: ...


class MemoryFixStore:
    """The same behaviour in memory, for tests. Not durable."""

    def __init__(self) -> None:
        self._next_out = 1
        self._next_in = 1
        self._messages: dict[int, StoredMessage] = {}
        self._orders: dict[str, str] = {}
        self._open_orders: set[str] = set()
        self._reset_period: str | None = None
        self._wire: dict[str, str] = {}
        self._executions: dict[tuple[str, str], StoredExecution] = {}
        self._quarantine: list[QuarantinedMessage] = []

    def next_outbound(self) -> int:
        return self._next_out

    def next_inbound(self) -> int:
        return self._next_in

    def allocate_outbound(self, message: StoredMessage | None = None) -> int:
        seq = self._next_out
        if message is not None:
            self._messages[seq] = StoredMessage(
                seq, message.msg_type, message.sending_time, message.body
            )
        self._next_out = seq + 1
        return seq

    def set_next_inbound(self, value: int) -> None:
        self._next_in = value

    def messages(self, begin: int, end: int) -> list[StoredMessage]:
        return [
            self._messages[seq] for seq in sorted(self._messages) if begin <= seq <= end
        ]

    def reset_sequences(self) -> None:
        self._next_out = 1
        self._next_in = 1
        self._messages.clear()

    def reset_period(self) -> str | None:
        return self._reset_period

    def set_reset_period(self, period: str) -> None:
        self._reset_period = period

    def put_order(
        self, client_order_id: str, payload: str, *, is_open: bool = True
    ) -> None:
        self._orders[client_order_id] = payload
        if is_open:
            self._open_orders.add(client_order_id)
        else:
            self._open_orders.discard(client_order_id)

    def delete_order(self, client_order_id: str) -> None:
        self._orders.pop(client_order_id, None)
        self._open_orders.discard(client_order_id)
        for wire in [w for w, c in self._wire.items() if c == client_order_id]:
            del self._wire[wire]

    def get_order(self, client_order_id: str) -> str | None:
        return self._orders.get(client_order_id)

    def open_orders(self) -> list[str]:
        return [self._orders[cid] for cid in self._open_orders]

    def link_wire_id(self, wire_id: str, client_order_id: str) -> None:
        self._wire[wire_id] = client_order_id

    def resolve_wire_id(self, wire_id: str) -> str | None:
        return self._wire.get(wire_id)

    def execution(self, client_order_id: str, exec_id: str) -> StoredExecution | None:
        return self._executions.get((client_order_id, exec_id))

    def record_report(
        self,
        client_order_id: str,
        *,
        order_payload: str | None,
        order_open: bool = True,
        execution: StoredExecution | None = None,
        bust_exec_id: str | None = None,
    ) -> None:
        if order_payload is not None:
            self.put_order(client_order_id, order_payload, is_open=order_open)
        if execution is not None:
            key = (execution.client_order_id, execution.exec_id)
            self._executions.setdefault(key, execution)
        if bust_exec_id is not None:
            known = self._executions.get((client_order_id, bust_exec_id))
            if known is not None:
                self._executions[(client_order_id, bust_exec_id)] = dataclasses.replace(
                    known, busted=True
                )

    def fills_since(self, since_us: int) -> list[StoredExecution]:
        return [
            e
            for e in self._executions.values()
            if e.fill is not None and not e.busted and e.latest_us >= since_us
        ]

    def busted_executions(self) -> list[StoredExecution]:
        return [e for e in self._executions.values() if e.busted]

    def quarantine(self, record: QuarantinedMessage) -> None:
        self._quarantine.append(record)

    def quarantined(self) -> list[QuarantinedMessage]:
        return list(self._quarantine)

    def release_quarantine(
        self, operator: str, reason: str, at: str, *, expected: int
    ) -> int:
        unreleased = [
            index
            for index, record in enumerate(self._quarantine)
            if record.released_by is None
        ]
        if len(unreleased) != expected:
            return len(unreleased)
        for index in unreleased:
            self._quarantine[index] = dataclasses.replace(
                self._quarantine[index],
                released_by=operator,
                release_reason=reason,
                released_at=at,
            )
        return len(unreleased)

    def close(self) -> None:
        """Nothing to release."""


_EXECUTION_COLUMNS = (
    "client_order_id, exec_id, digest, fill, busted, received_at, latest_us"
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS fix_sequence (
    session TEXT PRIMARY KEY,
    next_out INTEGER NOT NULL,
    next_in INTEGER NOT NULL,
    reset_period TEXT
);
CREATE TABLE IF NOT EXISTS fix_outbound (
    session TEXT NOT NULL,
    seq INTEGER NOT NULL,
    msg_type TEXT NOT NULL,
    sending_time TEXT NOT NULL,
    body BLOB NOT NULL,
    PRIMARY KEY (session, seq)
);
CREATE TABLE IF NOT EXISTS fix_order (
    session TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    is_open INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (session, client_order_id)
);
CREATE INDEX IF NOT EXISTS fix_order_open ON fix_order (session) WHERE is_open = 1;
CREATE TABLE IF NOT EXISTS fix_wire_id (
    session TEXT NOT NULL,
    wire_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    PRIMARY KEY (session, wire_id)
);
CREATE TABLE IF NOT EXISTS fix_execution (
    session TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    exec_id TEXT NOT NULL,
    digest TEXT NOT NULL,
    fill TEXT,
    busted INTEGER NOT NULL DEFAULT 0,
    received_at TEXT NOT NULL,
    latest_us INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session, client_order_id, exec_id)
);
CREATE INDEX IF NOT EXISTS fix_execution_latest ON fix_execution (session, latest_us);
CREATE INDEX IF NOT EXISTS fix_execution_busted ON fix_execution (session)
    WHERE busted = 1;
CREATE TABLE IF NOT EXISTS fix_quarantine (
    session TEXT NOT NULL,
    seq INTEGER NOT NULL,
    msg_type TEXT NOT NULL,
    content TEXT NOT NULL,
    reason TEXT NOT NULL,
    received_at TEXT NOT NULL,
    halted INTEGER NOT NULL DEFAULT 0,
    released_by TEXT,
    release_reason TEXT,
    released_at TEXT
);
"""


class SqliteFixStore:
    """One SQLite file, which may hold several sessions keyed by ``session``.

    ``session`` is usually :attr:`ismet.fix.session.FixSessionConfig.session_id`. A
    connection is guarded by a lock and may be used from any thread, so the session can
    fsync off the event loop. Use one store instance per session.
    """

    def __init__(
        self, path: str | Path, session: str, *, busy_timeout: float = 10.0
    ) -> None:
        self.path = Path(path)
        self.session = session
        self._lock = threading.Lock()
        self._closed = False
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(
                self.path,
                timeout=busy_timeout,
                check_same_thread=False,
                isolation_level=None,
            )
        except (sqlite3.Error, OSError) as exc:
            raise FixStoreError(f"cannot open FIX store {self.path}: {exc}") from exc
        try:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.executescript(_SCHEMA)
            self._conn.execute(
                "INSERT OR IGNORE INTO fix_sequence (session, next_out, next_in) "
                "VALUES (?, 1, 1)",
                (session,),
            )
        except sqlite3.Error as exc:
            self._conn.close()
            raise FixStoreError(f"cannot open FIX store {self.path}: {exc}") from exc

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    yield self._conn
                    self._conn.execute("COMMIT")
                except BaseException:
                    self._conn.execute("ROLLBACK")
                    raise
            except (sqlite3.Error, OverflowError) as exc:
                raise FixStoreError(f"FIX store write failed: {exc}") from exc

    def _query(self, sql: str, *args: object) -> list[tuple[object, ...]]:
        with self._lock:
            try:
                return self._conn.execute(sql, args).fetchall()
            except sqlite3.Error as exc:
                raise FixStoreError(f"FIX store read failed: {exc}") from exc

    def next_outbound(self) -> int:
        rows = self._query(
            "SELECT next_out FROM fix_sequence WHERE session = ?", self.session
        )
        return int(str(rows[0][0]))

    def next_inbound(self) -> int:
        rows = self._query(
            "SELECT next_in FROM fix_sequence WHERE session = ?", self.session
        )
        return int(str(rows[0][0]))

    def allocate_outbound(self, message: StoredMessage | None = None) -> int:
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT next_out FROM fix_sequence WHERE session = ?", (self.session,)
            ).fetchone()
            seq = int(row[0])
            if message is not None:
                conn.execute(
                    "INSERT OR REPLACE INTO fix_outbound "
                    "(session, seq, msg_type, sending_time, body) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        self.session,
                        seq,
                        message.msg_type,
                        message.sending_time,
                        message.body,
                    ),
                )
            conn.execute(
                "UPDATE fix_sequence SET next_out = ? WHERE session = ?",
                (seq + 1, self.session),
            )
        return seq

    def set_next_inbound(self, value: int) -> None:
        with self._transaction() as conn:
            conn.execute(
                "UPDATE fix_sequence SET next_in = ? WHERE session = ?",
                (value, self.session),
            )

    def messages(self, begin: int, end: int) -> list[StoredMessage]:
        rows = self._query(
            "SELECT seq, msg_type, sending_time, body FROM fix_outbound "
            "WHERE session = ? AND seq BETWEEN ? AND ? ORDER BY seq",
            self.session,
            begin,
            end,
        )
        return [
            StoredMessage(int(str(r[0])), str(r[1]), str(r[2]), bytes(_blob(r[3])))
            for r in rows
        ]

    def reset_sequences(self) -> None:
        with self._transaction() as conn:
            conn.execute(
                "UPDATE fix_sequence SET next_out = 1, next_in = 1 WHERE session = ?",
                (self.session,),
            )
            conn.execute("DELETE FROM fix_outbound WHERE session = ?", (self.session,))

    def reset_period(self) -> str | None:
        rows = self._query(
            "SELECT reset_period FROM fix_sequence WHERE session = ?", self.session
        )
        return None if rows[0][0] is None else str(rows[0][0])

    def set_reset_period(self, period: str) -> None:
        with self._transaction() as conn:
            conn.execute(
                "UPDATE fix_sequence SET reset_period = ? WHERE session = ?",
                (period, self.session),
            )

    def put_order(
        self, client_order_id: str, payload: str, *, is_open: bool = True
    ) -> None:
        with self._transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO fix_order "
                "(session, client_order_id, payload, is_open) VALUES (?, ?, ?, ?)",
                (self.session, client_order_id, payload, int(is_open)),
            )

    def delete_order(self, client_order_id: str) -> None:
        with self._transaction() as conn:
            conn.execute(
                "DELETE FROM fix_order WHERE session = ? AND client_order_id = ?",
                (self.session, client_order_id),
            )
            conn.execute(
                "DELETE FROM fix_wire_id WHERE session = ? AND client_order_id = ?",
                (self.session, client_order_id),
            )

    def get_order(self, client_order_id: str) -> str | None:
        rows = self._query(
            "SELECT payload FROM fix_order WHERE session = ? AND client_order_id = ?",
            self.session,
            client_order_id,
        )
        return str(rows[0][0]) if rows else None

    def open_orders(self) -> list[str]:
        rows = self._query(
            "SELECT payload FROM fix_order WHERE session = ? AND is_open = 1",
            self.session,
        )
        return [str(r[0]) for r in rows]

    def link_wire_id(self, wire_id: str, client_order_id: str) -> None:
        with self._transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO fix_wire_id "
                "(session, wire_id, client_order_id) VALUES (?, ?, ?)",
                (self.session, wire_id, client_order_id),
            )

    def resolve_wire_id(self, wire_id: str) -> str | None:
        rows = self._query(
            "SELECT client_order_id FROM fix_wire_id WHERE session = ? AND wire_id = ?",
            self.session,
            wire_id,
        )
        return str(rows[0][0]) if rows else None

    def execution(self, client_order_id: str, exec_id: str) -> StoredExecution | None:
        rows = self._query(
            f"SELECT {_EXECUTION_COLUMNS} FROM fix_execution "
            "WHERE session = ? AND client_order_id = ? AND exec_id = ?",
            self.session,
            client_order_id,
            exec_id,
        )
        return _execution(rows[0]) if rows else None

    def record_report(
        self,
        client_order_id: str,
        *,
        order_payload: str | None,
        order_open: bool = True,
        execution: StoredExecution | None = None,
        bust_exec_id: str | None = None,
    ) -> None:
        with self._transaction() as conn:
            if order_payload is not None:
                conn.execute(
                    "INSERT OR REPLACE INTO fix_order "
                    "(session, client_order_id, payload, is_open) VALUES (?, ?, ?, ?)",
                    (self.session, client_order_id, order_payload, int(order_open)),
                )
            if execution is not None:
                conn.execute(
                    "INSERT OR IGNORE INTO fix_execution "
                    "(session, client_order_id, exec_id, digest, fill, busted, "
                    "received_at, latest_us) VALUES (?, ?, ?, ?, ?, 0, ?, ?)",
                    (
                        self.session,
                        execution.client_order_id,
                        execution.exec_id,
                        execution.digest,
                        execution.fill,
                        execution.received_at,
                        execution.latest_us,
                    ),
                )
            if bust_exec_id is not None:
                conn.execute(
                    "UPDATE fix_execution SET busted = 1 WHERE session = ? "
                    "AND client_order_id = ? AND exec_id = ?",
                    (self.session, client_order_id, bust_exec_id),
                )

    def fills_since(self, since_us: int) -> list[StoredExecution]:
        rows = self._query(
            f"SELECT {_EXECUTION_COLUMNS} FROM fix_execution WHERE session = ? "
            "AND latest_us >= ? AND fill IS NOT NULL AND busted = 0",
            self.session,
            since_us,
        )
        return [_execution(r) for r in rows]

    def busted_executions(self) -> list[StoredExecution]:
        rows = self._query(
            f"SELECT {_EXECUTION_COLUMNS} FROM fix_execution "
            "WHERE session = ? AND busted = 1",
            self.session,
        )
        return [_execution(r) for r in rows]

    def quarantine(self, record: QuarantinedMessage) -> None:
        with self._transaction() as conn:
            conn.execute(
                "INSERT INTO fix_quarantine "
                "(session, seq, msg_type, content, reason, received_at, halted, "
                "released_by, release_reason, released_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    self.session,
                    record.seq,
                    record.msg_type,
                    record.content,
                    record.reason,
                    record.received_at,
                    int(record.halted),
                    record.released_by,
                    record.release_reason,
                    record.released_at,
                ),
            )

    def quarantined(self) -> list[QuarantinedMessage]:
        rows = self._query(
            "SELECT seq, msg_type, content, reason, received_at, halted, released_by, "
            "release_reason, released_at FROM fix_quarantine "
            "WHERE session = ? ORDER BY rowid",
            self.session,
        )
        return [
            QuarantinedMessage(
                int(str(r[0])),
                str(r[1]),
                str(r[2]),
                str(r[3]),
                str(r[4]),
                bool(r[5]),
                None if r[6] is None else str(r[6]),
                None if r[7] is None else str(r[7]),
                None if r[8] is None else str(r[8]),
            )
            for r in rows
        ]

    def release_quarantine(
        self, operator: str, reason: str, at: str, *, expected: int
    ) -> int:
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM fix_quarantine "
                "WHERE session = ? AND released_by IS NULL",
                (self.session,),
            ).fetchone()
            unreleased = int(row[0])
            if unreleased != expected:
                return unreleased
            conn.execute(
                "UPDATE fix_quarantine SET released_by = ?, release_reason = ?, "
                "released_at = ? WHERE session = ? AND released_by IS NULL",
                (operator, reason, at, self.session),
            )
            return unreleased

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                self._conn.close()


def _blob(value: object) -> bytes:
    if isinstance(value, bytes | bytearray | memoryview):
        return bytes(value)
    raise FixStoreError("stored message body is not a blob")


def _execution(row: tuple[object, ...]) -> StoredExecution:
    return StoredExecution(
        str(row[0]),
        str(row[1]),
        str(row[2]),
        None if row[3] is None else str(row[3]),
        bool(row[4]),
        str(row[5]),
        int(str(row[6])),
    )


__all__ = [
    "FixStore",
    "FixStoreError",
    "MemoryFixStore",
    "QuarantinedMessage",
    "SqliteFixStore",
    "StoredExecution",
    "StoredMessage",
]
