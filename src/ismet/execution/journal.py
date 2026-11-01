"""The durable, append-only, tamper-evident order journal.

Before an order is sent, its intent is on disk. After a crash, the journal says
what was attempted, and for each order whether a send could have started. It is
also the audit trail: every record carries the SHA-256 of its content and of the
previous record, so editing, removing from the middle, or reordering history breaks
the chain and :meth:`Journal.verify` reports where. Removing the newest records leaves
a shorter chain that is still valid, so detecting that requires a head hash saved
elsewhere: see ``expected_head_hash`` on :meth:`OrderManager.start`.

:class:`SqliteJournal` commits with ``synchronous=FULL`` and ``fullfsync``, so a
record has reached stable storage when ``append`` returns, as far as the drive honours
flush requests. SQLite triggers forbid UPDATE and DELETE. Those
stop accidents; the hash chain is what detects a deliberate edit, unless the editor
recomputes the whole chain, which only an anchored :attr:`Journal.head_hash`
copied elsewhere can catch. It uses only the standard library, so it works the same
on every OS.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import IO, Protocol

from ismet.errors import JournalError

GENESIS_HASH = "0" * 64


class RecordKind(str, Enum):
    """What a journal record holds."""

    ORDER_EVENT = "order_event"
    RISK_REJECT = "risk_reject"
    KILL_ENGAGED = "kill_engaged"
    KILL_RELEASED = "kill_released"
    RECON_BREAK = "recon_break"
    POSITION_BASELINE = "position_baseline"
    CONFIG_CHANGE = "config_change"
    ANOMALY = "anomaly"


@dataclass(frozen=True)
class JournalRecord:
    seq: int
    ts: datetime
    kind: RecordKind
    client_order_id: str | None
    payload: str
    prev_hash: str
    hash: str


@dataclass(frozen=True)
class KillState:
    engaged: bool
    reason: str = ""
    operator: str = ""
    seq: int = 0
    cancel_open: bool = False


@dataclass(frozen=True)
class VerifyResult:
    ok: bool
    records: int
    head_hash: str
    first_bad_seq: int | None = None
    detail: str = ""


def _record_hash(
    seq: int,
    ts: str,
    kind: str,
    client_order_id: str | None,
    payload: str,
    prev_hash: str,
) -> str:
    material = json.dumps(
        [seq, ts, kind, client_order_id, payload, prev_hash],
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


class Journal(Protocol):
    """Durable event storage. ``append`` is durable when it returns."""

    def append(
        self,
        kind: RecordKind,
        payload: str,
        *,
        client_order_id: str | None = None,
        ts: datetime | None = None,
        unique_intent: bool = False,
    ) -> JournalRecord: ...

    def records(self, *, after_seq: int = 0) -> Iterator[JournalRecord]: ...

    def kill_state(self) -> KillState: ...

    def verify(self) -> VerifyResult: ...

    def head_hash(self) -> str: ...

    def close(self) -> None: ...


def chain_contains(journal: Journal, record_hash: str) -> bool:
    """Whether a previously saved head hash is still part of the journal's chain."""
    if record_hash == GENESIS_HASH:
        return True
    return any(record.hash == record_hash for record in journal.records())


def _verify_chain(records: Iterator[JournalRecord]) -> VerifyResult:
    prev = GENESIS_HASH
    expected_seq = 1
    count = 0
    for record in records:
        count += 1
        if record.seq != expected_seq:
            return VerifyResult(
                False,
                count,
                prev,
                record.seq,
                f"sequence jumps from {expected_seq - 1} to {record.seq}",
            )
        if record.prev_hash != prev:
            return VerifyResult(
                False, count, prev, record.seq, "previous hash mismatch"
            )
        computed = _record_hash(
            record.seq,
            record.ts.isoformat(),
            record.kind.value,
            record.client_order_id,
            record.payload,
            record.prev_hash,
        )
        if computed != record.hash:
            return VerifyResult(
                False, count, prev, record.seq, "record content altered"
            )
        prev = record.hash
        expected_seq += 1
    return VerifyResult(True, count, prev)


def _kill_state_from(records: Iterator[JournalRecord]) -> KillState:
    state = KillState(False)
    for record in records:
        if record.kind in (RecordKind.KILL_ENGAGED, RecordKind.KILL_RELEASED):
            detail = json.loads(record.payload)
            state = KillState(
                record.kind is RecordKind.KILL_ENGAGED,
                detail.get("reason", ""),
                detail.get("operator", ""),
                record.seq,
                bool(detail.get("cancel_open", False)),
            )
    return state


class MemoryJournal:
    """A chain-compatible journal in memory, for tests and demos. Not durable."""

    def __init__(self) -> None:
        self._records: list[JournalRecord] = []
        self._intents: set[str] = set()

    def append(
        self,
        kind: RecordKind,
        payload: str,
        *,
        client_order_id: str | None = None,
        ts: datetime | None = None,
        unique_intent: bool = False,
    ) -> JournalRecord:
        if unique_intent:
            if client_order_id in self._intents:
                raise JournalError(
                    f"client order id {client_order_id} already journalled"
                )
            self._intents.add(str(client_order_id))
        moment = (ts or datetime.now(timezone.utc)).astimezone(timezone.utc)
        prev = self._records[-1].hash if self._records else GENESIS_HASH
        seq = len(self._records) + 1
        stamp = moment.isoformat()
        digest = _record_hash(seq, stamp, kind.value, client_order_id, payload, prev)
        record = JournalRecord(
            seq, moment, kind, client_order_id, payload, prev, digest
        )
        self._records.append(record)
        return record

    def records(self, *, after_seq: int = 0) -> Iterator[JournalRecord]:
        return iter(self._records[after_seq:])

    def kill_state(self) -> KillState:
        return _kill_state_from(iter(self._records))

    def verify(self) -> VerifyResult:
        return _verify_chain(iter(self._records))

    def head_hash(self) -> str:
        return self._records[-1].hash if self._records else GENESIS_HASH

    def close(self) -> None:
        """Nothing to release."""


_SCHEMA = """
CREATE TABLE IF NOT EXISTS journal (
    seq INTEGER PRIMARY KEY,
    ts TEXT NOT NULL,
    kind TEXT NOT NULL,
    client_order_id TEXT,
    payload TEXT NOT NULL,
    unique_intent INTEGER NOT NULL DEFAULT 0,
    prev_hash TEXT NOT NULL,
    hash TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS journal_one_intent_per_order
    ON journal (client_order_id) WHERE unique_intent = 1;
CREATE INDEX IF NOT EXISTS journal_kill ON journal (kind)
    WHERE kind IN ('kill_engaged', 'kill_released');
CREATE TRIGGER IF NOT EXISTS journal_no_update BEFORE UPDATE ON journal
    BEGIN SELECT RAISE(ABORT, 'journal is append-only'); END;
CREATE TRIGGER IF NOT EXISTS journal_no_delete BEFORE DELETE ON journal
    BEGIN SELECT RAISE(ABORT, 'journal is append-only'); END;
"""


class SqliteJournal:
    """A journal file safe to share between processes, such as an OMS and the CLI.

    One writer at a time is enforced by SQLite's write lock, and each append reads
    the chain head inside the same transaction, so concurrent processes extend the
    chain correctly. A connection is guarded by a lock and may be used from any
    thread, which lets the order manager fsync off the event loop.
    """

    def __init__(self, path: str | Path, *, busy_timeout: float = 10.0) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(
                self.path,
                timeout=busy_timeout,
                check_same_thread=False,
                isolation_level=None,
            )
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            # On macOS fsync() does not flush the drive's cache, so without these a
            # power cut can lose a committed record. Ignored on other platforms.
            self._conn.execute("PRAGMA fullfsync=ON")
            self._conn.execute("PRAGMA checkpoint_fullfsync=ON")
            self._conn.executescript(_SCHEMA)
        except (sqlite3.Error, OSError) as exc:
            raise JournalError(f"cannot open journal {self.path}: {exc}") from exc

    def append(
        self,
        kind: RecordKind,
        payload: str,
        *,
        client_order_id: str | None = None,
        ts: datetime | None = None,
        unique_intent: bool = False,
    ) -> JournalRecord:
        moment = (ts or datetime.now(timezone.utc)).astimezone(timezone.utc)
        stamp = moment.isoformat()
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    head = self._conn.execute(
                        "SELECT seq, hash FROM journal ORDER BY seq DESC LIMIT 1"
                    ).fetchone()
                    seq = head[0] + 1 if head else 1
                    prev = head[1] if head else GENESIS_HASH
                    digest = _record_hash(
                        seq, stamp, kind.value, client_order_id, payload, prev
                    )
                    self._conn.execute(
                        "INSERT INTO journal "
                        "(seq, ts, kind, client_order_id, payload, unique_intent, "
                        "prev_hash, hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            seq,
                            stamp,
                            kind.value,
                            client_order_id,
                            payload,
                            int(unique_intent),
                            prev,
                            digest,
                        ),
                    )
                    self._conn.execute("COMMIT")
                except BaseException:
                    self._conn.execute("ROLLBACK")
                    raise
            except sqlite3.IntegrityError as exc:
                raise JournalError(
                    f"client order id {client_order_id} already journalled"
                ) from exc
            except sqlite3.Error as exc:
                raise JournalError(f"journal write failed: {exc}") from exc
        return JournalRecord(seq, moment, kind, client_order_id, payload, prev, digest)

    @staticmethod
    def _row(row: tuple[int, str, str, str | None, str, str, str]) -> JournalRecord:
        return JournalRecord(
            row[0],
            datetime.fromisoformat(row[1]),
            RecordKind(row[2]),
            row[3],
            row[4],
            row[5],
            row[6],
        )

    def records(self, *, after_seq: int = 0) -> Iterator[JournalRecord]:
        with self._lock:
            try:
                rows = self._conn.execute(
                    "SELECT seq, ts, kind, client_order_id, payload, prev_hash, hash "
                    "FROM journal WHERE seq > ? ORDER BY seq",
                    (after_seq,),
                ).fetchall()
            except sqlite3.Error as exc:
                raise JournalError(f"journal read failed: {exc}") from exc
        return iter([self._row(row) for row in rows])

    def kill_state(self) -> KillState:
        with self._lock:
            try:
                row = self._conn.execute(
                    "SELECT seq, kind, payload FROM journal WHERE kind IN "
                    "('kill_engaged', 'kill_released') ORDER BY seq DESC LIMIT 1"
                ).fetchone()
            except sqlite3.Error as exc:
                raise JournalError(f"journal read failed: {exc}") from exc
        if row is None:
            return KillState(False)
        detail = json.loads(row[2])
        return KillState(
            row[1] == RecordKind.KILL_ENGAGED.value,
            detail.get("reason", ""),
            detail.get("operator", ""),
            row[0],
            bool(detail.get("cancel_open", False)),
        )

    def verify(self) -> VerifyResult:
        return _verify_chain(self.records())

    def durability_settings(self) -> dict[str, int]:
        """The pragmas durability depends on, for tests and ``ismet doctor``."""
        with self._lock:
            return {
                name: int(self._conn.execute(f"PRAGMA {name}").fetchone()[0])
                for name in ("synchronous", "fullfsync", "checkpoint_fullfsync")
            }

    def head_hash(self) -> str:
        with self._lock:
            row = self._conn.execute(
                "SELECT hash FROM journal ORDER BY seq DESC LIMIT 1"
            ).fetchone()
        return row[0] if row else GENESIS_HASH

    def close(self) -> None:
        with self._lock:
            self._conn.close()


def export_jsonl(journal: Journal, stream: IO[str]) -> int:
    """Write the journal as JSON lines for regulators and archives. Returns a count."""
    count = 0
    for record in journal.records():
        stream.write(
            json.dumps(
                {
                    "seq": record.seq,
                    "ts": record.ts.isoformat(),
                    "kind": record.kind.value,
                    "client_order_id": record.client_order_id,
                    "payload": json.loads(record.payload),
                    "prev_hash": record.prev_hash,
                    "hash": record.hash,
                },
                separators=(",", ":"),
            )
            + "\n"
        )
        count += 1
    return count


__all__ = [
    "GENESIS_HASH",
    "Journal",
    "JournalRecord",
    "KillState",
    "MemoryJournal",
    "RecordKind",
    "SqliteJournal",
    "VerifyResult",
    "chain_contains",
    "export_jsonl",
]
