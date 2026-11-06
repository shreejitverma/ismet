"""The durable, append-only, tamper-evident order journal.

Before an order is sent, its intent is on disk. After a crash, the journal says
what was attempted, and for each order whether a send could have started. It is
also the audit trail: every record carries the SHA-256 of its content and of the
previous record, so editing, removing from the middle, or reordering history breaks
the chain and :meth:`Journal.verify` reports where. Removing the newest records would
leave a shorter chain that is still valid, so each commit also updates a small anchor
file beside the journal (``<journal>.head``), and verification fails if the journal is
behind its anchor. Deleting both defeats that, which is why
``expected_head_hash`` on :meth:`OrderManager.start` exists for a hash saved elsewhere.

:class:`SqliteJournal` commits with ``synchronous=FULL`` and ``fullfsync``, so a
record has reached stable storage when ``append`` returns, as far as the drive honours
flush requests. SQLite triggers forbid UPDATE and DELETE. Those
stop accidents; the hash chain is what detects a deliberate edit, unless the editor
recomputes the whole chain, which only an anchored :attr:`Journal.head_hash`
copied elsewhere can catch. It uses only the standard library, so it works the same
on every OS.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import sqlite3
import threading
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import IO, Protocol

from ismet.errors import JournalError, ValidationError

logger = logging.getLogger("ismet.execution")

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
    POSITION_ADJUSTED = "position_adjusted"
    SWEEP_MARK = "sweep_mark"
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
    unique_intent: bool = False


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
    unique_intent: bool = False,
) -> str:
    material = json.dumps(
        [seq, ts, kind, client_order_id, payload, prev_hash, int(unique_intent)],
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


_RawRow = tuple[int, str, str, str | None, str, str, str, int]


def _verify_chain(rows: Iterable[_RawRow]) -> VerifyResult:
    """Check stored text as written, so a corrupt field is a failure, not a crash."""
    prev = GENESIS_HASH
    expected_seq = 1
    count = 0
    for seq, ts, kind, client_order_id, payload, prev_hash, digest, unique in rows:
        count += 1
        if seq != expected_seq:
            return VerifyResult(
                False,
                count,
                prev,
                seq,
                f"sequence jumps from {expected_seq - 1} to {seq}",
            )
        if prev_hash != prev:
            return VerifyResult(False, count, prev, seq, "previous hash mismatch")
        computed = _record_hash(
            seq, ts, kind, client_order_id, payload, prev_hash, bool(unique)
        )
        if computed != digest:
            return VerifyResult(False, count, prev, seq, "record content altered")
        prev = digest
        expected_seq += 1
    return VerifyResult(True, count, prev)


def names_operator(detail: dict[str, object]) -> bool:
    """Whether a journalled operator action says who did it."""
    operator = detail.get("operator")
    return isinstance(operator, str) and bool(operator.strip())


def _kill_state_from(rows: Iterable[tuple[int, str, str]]) -> KillState:
    """The kill state of ``(seq, kind, payload)`` rows, oldest first.

    A release naming no operator is ignored, so it leaves the switch as it was: every
    reader, including a process starting up, must agree that the halt still holds. A row
    that cannot be read could have been either kind, so it leaves the switch engaged
    until a later readable row says otherwise. Engages only widen until a release: two
    can race, and the later one's narrower terms must not stop the earlier's cancels.
    """
    state = KillState(False)
    for seq, kind, payload in rows:
        try:
            detail = json.loads(payload)
        except ValueError:
            detail = None
        if not isinstance(detail, dict):
            logger.critical("kill switch record %s cannot be read", seq)
            state = KillState(
                True,
                f"kill switch record {seq} cannot be read",
                "",
                seq,
                state.cancel_open,
            )
            continue
        engaged = kind == RecordKind.KILL_ENGAGED.value
        if not engaged and not names_operator(detail):
            logger.critical("ignoring a kill switch release with no operator: %s", seq)
            continue
        state = KillState(
            engaged,
            detail.get("reason", ""),
            detail.get("operator", ""),
            seq,
            bool(detail.get("cancel_open", False))
            or (engaged and state.engaged and state.cancel_open),
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
        digest = _record_hash(
            seq, stamp, kind.value, client_order_id, payload, prev, unique_intent
        )
        record = JournalRecord(
            seq, moment, kind, client_order_id, payload, prev, digest, unique_intent
        )
        self._records.append(record)
        return record

    def records(self, *, after_seq: int = 0) -> Iterator[JournalRecord]:
        return iter(self._records[after_seq:])

    def kill_state(self) -> KillState:
        return _kill_state_from(
            (r.seq, r.kind.value, r.payload)
            for r in self._records
            if r.kind in (RecordKind.KILL_ENGAGED, RecordKind.KILL_RELEASED)
        )

    def verify(self) -> VerifyResult:
        return _verify_chain(
            (
                r.seq,
                r.ts.isoformat(),
                r.kind.value,
                r.client_order_id,
                r.payload,
                r.prev_hash,
                r.hash,
                int(r.unique_intent),
            )
            for r in self._records
        )

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


def _connect_read_only(path: Path) -> sqlite3.Connection:
    """Read a journal without writing to it, even where nothing can be written.

    SQLite cannot use a WAL database in a directory it cannot write to, as an audit copy
    may be. With no WAL file to miss, such a file is safe to read as immutable.
    """
    uri = path.resolve().as_uri()
    wal = path.with_name(path.name + "-wal")
    has_wal = wal.exists() and wal.stat().st_size > 0
    mode = "mode=ro"
    if not has_wal and not os.access(path.parent, os.W_OK):
        mode = "immutable=1"
    return sqlite3.connect(
        f"{uri}?{mode}", uri=True, check_same_thread=False, isolation_level=None
    )


def _check_is_journal(connection: sqlite3.Connection, path: Path) -> None:
    """Only a file that is no database, or has no journal table, is not a journal."""
    try:
        row = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'journal'"
        ).fetchone()
    except sqlite3.DatabaseError as exc:
        if "file is not a database" in str(exc):
            raise ValidationError(f"{path} is not an ismet journal") from exc
        raise JournalError(f"cannot read journal {path}: {exc}") from exc
    if row is None:
        raise ValidationError(f"{path} is not an ismet journal")


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
            self._warn_if_triggers_dropped()
            self._conn.executescript(_SCHEMA)
        except (sqlite3.Error, OSError) as exc:
            raise JournalError(f"cannot open journal {self.path}: {exc}") from exc

    def _warn_if_triggers_dropped(self) -> None:
        """The schema script puts a dropped trigger back, so say so before it does.

        Only tampering removes one, and recreating it quietly would hide that.
        """
        names = {
            row[0]
            for row in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'trigger')"
            )
        }
        if "journal" not in names:
            return
        missing = {"journal_no_update", "journal_no_delete"} - names
        if missing:
            logger.critical(
                "journal %s had its append-only trigger%s %s removed: treat the file "
                "as tampered with. They are being recreated",
                self.path,
                "s" if len(missing) > 1 else "",
                ", ".join(sorted(missing)),
            )

    @classmethod
    def open_read_only(cls, path: str | Path) -> SqliteJournal:
        """Open for reading only: no schema script, no pragmas.

        Reading must not change the evidence. Recreating a dropped trigger would hide
        the tampering, so this skips everything opening for writing does.
        """
        journal = cls.__new__(cls)
        journal.path = Path(path)
        journal._lock = threading.Lock()
        try:
            journal._conn = _connect_read_only(journal.path)
        except sqlite3.Error as exc:
            raise JournalError(f"cannot read journal {path}: {exc}") from exc
        try:
            _check_is_journal(journal._conn, journal.path)
        except BaseException:
            journal._conn.close()
            raise
        return journal

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
                        seq,
                        stamp,
                        kind.value,
                        client_order_id,
                        payload,
                        prev,
                        unique_intent,
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
            self._write_anchor(seq, digest)
        return JournalRecord(
            seq, moment, kind, client_order_id, payload, prev, digest, unique_intent
        )

    @property
    def anchor_path(self) -> Path:
        return self.path.with_name(self.path.name + ".head")

    def _write_anchor(self, seq: int, digest: str) -> None:
        """Best effort: a missing or older anchor only weakens truncation detection.

        An anchor that names a record the chain no longer has, or has with another
        digest, is evidence of truncation or replacement. Appending must not move it,
        or an emergency ``kill engage`` would make the journal verify clean again. An
        anchor already past ``seq`` on an intact row was written by another process
        and is kept, quietly.

        Runs after the record commits, so a crash in between leaves the anchor behind,
        never ahead. It takes the database write lock for the read and the replace,
        so writers in different processes update the anchor one at a time and a slower
        one cannot move it back past a faster one. Called with the connection lock
        held.
        """
        try:
            self._conn.execute("BEGIN IMMEDIATE")
        except sqlite3.Error as exc:
            logger.warning("could not lock the journal to update its anchor: %s", exc)
            return
        try:
            anchor = self._read_anchor()
            if anchor is not None and self._anchor_disputes_chain(anchor):
                logger.critical(
                    "journal %s is behind its anchor at record %s: keeping the anchor",
                    self.path,
                    anchor[0],
                )
                return
            if anchor is not None and anchor[0] >= seq:
                return
            temporary = self.anchor_path.with_name(
                f"{self.anchor_path.name}.{os.getpid()}.{threading.get_ident()}"
            )
            try:
                temporary.write_text(f"{seq} {digest}", encoding="ascii")
                temporary.replace(self.anchor_path)
            except OSError as exc:
                logger.warning("could not update the journal anchor: %s", exc)
        finally:
            try:
                self._conn.execute("COMMIT")
            except sqlite3.Error as exc:
                logger.warning("could not release the journal anchor lock: %s", exc)
                with contextlib.suppress(sqlite3.Error):
                    self._conn.execute("ROLLBACK")

    def _read_anchor(self) -> tuple[int, str] | None:
        try:
            seq, digest = self.anchor_path.read_text(encoding="ascii").split()
            return int(seq), digest
        except (OSError, ValueError):
            return None

    def _anchor_disputes_chain(self, anchor: tuple[int, str]) -> bool:
        """Whether the anchored record is gone or no longer has the anchored digest.

        A truncated chain lacks the row, and a replaced one has another digest there.
        An anchor ahead of this append on an intact row is another process's, not a
        dispute.
        """
        anchored_seq, anchored_digest = anchor
        try:
            row = self._conn.execute(
                "SELECT hash FROM journal WHERE seq = ?", (anchored_seq,)
            ).fetchone()
        except sqlite3.Error:
            return True  # unreadable now; the next append looks again
        return row is None or row[0] != anchored_digest

    @staticmethod
    def _row(
        row: tuple[int, str, str, str | None, str, str, str, int],
    ) -> JournalRecord:
        return JournalRecord(
            row[0],
            datetime.fromisoformat(row[1]),
            RecordKind(row[2]),
            row[3],
            row[4],
            row[5],
            row[6],
            bool(row[7]),
        )

    def records(self, *, after_seq: int = 0) -> Iterator[JournalRecord]:
        with self._lock:
            try:
                rows = self._conn.execute(
                    "SELECT seq, ts, kind, client_order_id, payload, prev_hash, hash, "
                    "unique_intent FROM journal WHERE seq > ? ORDER BY seq",
                    (after_seq,),
                ).fetchall()
            except sqlite3.Error as exc:
                raise JournalError(f"journal read failed: {exc}") from exc
        return iter([self._row(row) for row in rows])

    def kill_state(self) -> KillState:
        with self._lock:
            try:
                rows = self._conn.execute(
                    "SELECT seq, kind, payload FROM journal WHERE kind IN "
                    "('kill_engaged', 'kill_released') ORDER BY seq"
                ).fetchall()
            except sqlite3.Error as exc:
                raise JournalError(f"journal read failed: {exc}") from exc
        return _kill_state_from(rows)

    def verify(self) -> VerifyResult:
        """Check the chain, then that nothing newer than the anchor has been removed.

        The anchor is read first: each commit precedes its anchor update, so an anchor
        read before the rows can never be ahead of them, whatever another process does.
        """
        anchor = self._read_anchor()
        with self._lock:
            try:
                rows = self._conn.execute(
                    "SELECT seq, ts, kind, client_order_id, payload, prev_hash, hash, "
                    "unique_intent FROM journal ORDER BY seq"
                ).fetchall()
            except sqlite3.Error as exc:
                raise JournalError(f"journal read failed: {exc}") from exc
        result = _verify_chain(rows)
        if not result.ok:
            return result
        if anchor is None:
            return result
        seq, digest = anchor
        if seq > len(rows):
            return VerifyResult(
                False,
                result.records,
                result.head_hash,
                seq,
                f"journal ends at {len(rows)} but its anchor saw {seq}: truncated",
            )
        if rows[seq - 1][6] != digest:
            return VerifyResult(
                False,
                result.records,
                result.head_hash,
                seq,
                f"record {seq} does not match the anchor: the journal was replaced",
            )
        return result

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
    """Write the journal as JSON lines for regulators and archives. Returns a count.

    ``payload`` is the parsed record for reading, and ``raw_payload`` the exact stored
    text the hash covers, so :func:`verify_jsonl` can check an archive on its own.
    """
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
                    "raw_payload": record.payload,
                    "unique_intent": record.unique_intent,
                    "prev_hash": record.prev_hash,
                    "hash": record.hash,
                },
                separators=(",", ":"),
            )
            + "\n"
        )
        count += 1
    return count


def verify_jsonl(lines: Iterable[str]) -> VerifyResult:
    """Check the hash chain of an archive written by :func:`export_jsonl`."""
    rows: list[_RawRow] = []
    for line in lines:
        item = json.loads(line)
        rows.append(
            (
                item["seq"],
                item["ts"],
                item["kind"],
                item["client_order_id"],
                item["raw_payload"],
                item["prev_hash"],
                item["hash"],
                int(item["unique_intent"]),
            )
        )
    return _verify_chain(rows)


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
    "names_operator",
    "verify_jsonl",
]
