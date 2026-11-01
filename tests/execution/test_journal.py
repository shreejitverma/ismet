"""Durability, append-only enforcement, and tamper evidence."""

from __future__ import annotations

import io
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ismet.errors import JournalError
from ismet.execution.journal import (
    GENESIS_HASH,
    MemoryJournal,
    RecordKind,
    SqliteJournal,
    export_jsonl,
)

TS = datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)


@pytest.fixture(params=["sqlite", "memory"])
def journal(request: pytest.FixtureRequest, tmp_path: Path):  # type: ignore[no-untyped-def]
    instance = (
        SqliteJournal(tmp_path / "j.db")
        if request.param == "sqlite"
        else MemoryJournal()
    )
    yield instance
    instance.close()


def test_append_chains_hashes_and_verifies(journal) -> None:  # type: ignore[no-untyped-def]
    assert journal.head_hash() == GENESIS_HASH
    first = journal.append(RecordKind.ANOMALY, '{"a":1}', client_order_id="C1", ts=TS)
    second = journal.append(RecordKind.ANOMALY, '{"a":2}', ts=TS)
    assert (first.seq, second.seq) == (1, 2)
    assert first.prev_hash == GENESIS_HASH
    assert second.prev_hash == first.hash
    assert journal.head_hash() == second.hash
    result = journal.verify()
    assert result.ok and result.records == 2 and result.head_hash == second.hash
    assert [r.seq for r in journal.records(after_seq=1)] == [2]


def test_one_intent_per_client_order_id(journal) -> None:  # type: ignore[no-untyped-def]
    journal.append(
        RecordKind.ORDER_EVENT, "{}", client_order_id="C1", unique_intent=True
    )
    with pytest.raises(JournalError, match="already journalled"):
        journal.append(
            RecordKind.ORDER_EVENT, "{}", client_order_id="C1", unique_intent=True
        )
    journal.append(RecordKind.ORDER_EVENT, "{}", client_order_id="C1")
    assert journal.verify().ok


def test_kill_state_tracks_the_latest_record(journal) -> None:  # type: ignore[no-untyped-def]
    assert not journal.kill_state().engaged
    journal.append(
        RecordKind.KILL_ENGAGED,
        json.dumps({"reason": "halt", "operator": "ops", "cancel_open": True}),
    )
    state = journal.kill_state()
    assert state.engaged and state.reason == "halt" and state.operator == "ops"
    assert state.cancel_open
    journal.append(
        RecordKind.KILL_RELEASED, json.dumps({"reason": "ok", "operator": "o"})
    )
    assert not journal.kill_state().engaged


def test_export_writes_one_json_object_per_record(journal) -> None:  # type: ignore[no-untyped-def]
    journal.append(RecordKind.ANOMALY, '{"a":1}', client_order_id="C1", ts=TS)
    out = io.StringIO()
    assert export_jsonl(journal, out) == 1
    row = json.loads(out.getvalue())
    assert row["payload"] == {"a": 1} and row["kind"] == "anomaly"


def test_records_survive_reopening_and_chain_continues(tmp_path: Path) -> None:
    path = tmp_path / "j.db"
    first = SqliteJournal(path)
    record = first.append(RecordKind.ANOMALY, "{}", ts=TS)
    first.close()
    second = SqliteJournal(path)
    next_record = second.append(RecordKind.ANOMALY, "{}", ts=TS)
    assert next_record.prev_hash == record.hash and next_record.seq == 2
    assert second.verify().ok
    second.close()


def test_two_connections_extend_one_chain(tmp_path: Path) -> None:
    path = tmp_path / "j.db"
    oms, cli = SqliteJournal(path), SqliteJournal(path)
    oms.append(RecordKind.ANOMALY, "{}")
    cli.append(RecordKind.KILL_ENGAGED, json.dumps({"reason": "from cli"}))
    oms.append(RecordKind.ANOMALY, "{}")
    assert oms.kill_state().engaged and oms.kill_state().reason == "from cli"
    assert cli.verify().ok and cli.verify().records == 3
    oms.close()
    cli.close()


def test_update_and_delete_are_refused_by_the_database(tmp_path: Path) -> None:
    path = tmp_path / "j.db"
    journal = SqliteJournal(path)
    journal.append(RecordKind.ANOMALY, "{}", ts=TS)
    journal.close()
    conn = sqlite3.connect(path)
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        conn.execute("UPDATE journal SET payload = 'x'")
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        conn.execute("DELETE FROM journal")
    conn.close()


def _tamper(path: Path, sql: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute("DROP TRIGGER journal_no_update")
    conn.execute("DROP TRIGGER journal_no_delete")
    conn.execute(sql)
    conn.commit()
    conn.close()


@pytest.mark.parametrize(
    ("sql", "bad_seq", "detail"),
    [
        ("UPDATE journal SET payload = '{\"a\":9}' WHERE seq = 2", 2, "altered"),
        (
            "UPDATE journal SET ts = '2020-01-01T00:00:00+00:00' WHERE seq = 2",
            2,
            "altered",
        ),
        ("UPDATE journal SET kind = 'kill_released' WHERE seq = 2", 2, "altered"),
        ("DELETE FROM journal WHERE seq = 2", 3, "jumps"),
        ("UPDATE journal SET hash = 'x', payload = 'y' WHERE seq = 1", 1, "altered"),
    ],
)
def test_tampering_is_detected_and_located(
    tmp_path: Path, sql: str, bad_seq: int, detail: str
) -> None:
    path = tmp_path / "j.db"
    journal = SqliteJournal(path)
    for value in range(3):
        journal.append(RecordKind.ANOMALY, json.dumps({"a": value}), ts=TS)
    journal.close()
    _tamper(path, sql)
    reopened = SqliteJournal(path)
    result = reopened.verify()
    reopened.close()
    assert not result.ok
    assert result.first_bad_seq == bad_seq
    assert detail in result.detail


def test_deleting_the_tail_is_visible_through_an_anchored_head_hash(
    tmp_path: Path,
) -> None:
    path = tmp_path / "j.db"
    journal = SqliteJournal(path)
    journal.append(RecordKind.ANOMALY, "{}", ts=TS)
    journal.append(RecordKind.ANOMALY, "{}", ts=TS)
    anchor = journal.head_hash()
    journal.close()
    _tamper(path, "DELETE FROM journal WHERE seq = 2")
    reopened = SqliteJournal(path)
    assert reopened.verify().ok  # a truncated chain is still self-consistent
    assert reopened.head_hash() != anchor
    reopened.close()


def test_unusable_path_raises_journal_error(tmp_path: Path) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("x")
    with pytest.raises(JournalError, match="cannot open"):
        SqliteJournal(blocker / "j.db")


def test_writing_after_close_raises_journal_error(tmp_path: Path) -> None:
    journal = SqliteJournal(tmp_path / "j.db")
    journal.close()
    with pytest.raises(JournalError):
        journal.append(RecordKind.ANOMALY, "{}")
    with pytest.raises(JournalError):
        list(journal.records())
    with pytest.raises(JournalError):
        journal.kill_state()
