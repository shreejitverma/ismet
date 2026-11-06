"""Durability, append-only enforcement, and tamper evidence."""

from __future__ import annotations

import io
import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ismet.errors import JournalError
from ismet.execution.journal import (
    GENESIS_HASH,
    MemoryJournal,
    RecordKind,
    SqliteJournal,
    chain_contains,
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


@pytest.mark.parametrize("operator", ["", "   ", None, 7])
def test_a_release_naming_no_operator_leaves_the_kill_engaged(  # type: ignore[no-untyped-def]
    journal, operator
) -> None:
    journal.append(
        RecordKind.KILL_ENGAGED, json.dumps({"reason": "halt", "operator": "ops"})
    )
    journal.append(
        RecordKind.KILL_RELEASED, json.dumps({"reason": "x", "operator": operator})
    )
    state = journal.kill_state()
    assert state.engaged and state.reason == "halt" and state.operator == "ops"


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


def test_commits_ask_for_a_real_flush_to_disk(tmp_path: Path) -> None:
    journal = SqliteJournal(tmp_path / "j.db")
    settings = journal.durability_settings()
    journal.close()
    assert settings["synchronous"] == 2  # FULL
    assert settings["fullfsync"] == 1
    assert settings["checkpoint_fullfsync"] == 1


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
    result = reopened.verify()
    assert not result.ok and "truncated" in result.detail
    assert reopened.head_hash() != anchor
    reopened.close()
    # Without the anchor file the shorter chain is self-consistent, which is why a head
    # saved elsewhere exists as a stronger check.
    (tmp_path / "j.db.head").unlink()
    bare = SqliteJournal(path)
    assert bare.verify().ok
    assert not chain_contains(bare, anchor)
    bare.close()


def _three_records(path: Path) -> None:
    journal = SqliteJournal(path)
    for value in range(3):
        journal.append(
            RecordKind.ANOMALY, json.dumps({"a": value}), client_order_id="C", ts=TS
        )
    journal.close()


def test_a_record_rewritten_with_its_own_hash_recomputed_breaks_the_link(
    tmp_path: Path,
) -> None:
    path = tmp_path / "j.db"
    _three_records(path)
    from ismet.execution.journal import _record_hash

    conn = sqlite3.connect(path)
    seq, ts, kind, cid, _, prev = conn.execute(
        "SELECT seq, ts, kind, client_order_id, payload, prev_hash FROM journal "
        "WHERE seq = 2"
    ).fetchone()
    forged = _record_hash(seq, ts, kind, cid, '{"a":9}', prev)
    _tamper(
        path, f"UPDATE journal SET payload='{{\"a\":9}}', hash='{forged}' WHERE seq=2"
    )
    conn.close()
    journal = SqliteJournal(path)
    result = journal.verify()
    journal.close()
    assert not result.ok
    assert result.first_bad_seq == 3 and "previous hash" in result.detail


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE journal SET client_order_id = 'X' WHERE seq = 2",
        "UPDATE journal SET prev_hash = 'abc' WHERE seq = 2",
        "UPDATE journal SET unique_intent = 1 WHERE seq = 2",
        "UPDATE journal SET kind = 'bogus' WHERE seq = 2",
        "UPDATE journal SET ts = 'garbage' WHERE seq = 2",
        "UPDATE journal SET seq = CASE seq WHEN 2 THEN 30 WHEN 3 THEN 2 END "
        "WHERE seq IN (2, 3)",
    ],
)
def test_every_stored_field_is_covered_and_corruption_is_a_failure_not_a_crash(
    tmp_path: Path, sql: str
) -> None:
    path = tmp_path / "j.db"
    _three_records(path)
    _tamper(path, sql)
    journal = SqliteJournal(path)
    result = journal.verify()
    journal.close()
    assert not result.ok


def test_an_anchor_that_disagrees_with_the_chain_means_the_journal_was_replaced(
    tmp_path: Path,
) -> None:
    path = tmp_path / "j.db"
    _three_records(path)
    (tmp_path / "j.db.head").write_text("2 " + "e" * 64, encoding="ascii")
    journal = SqliteJournal(path)
    result = journal.verify()
    assert not result.ok and "replaced" in result.detail
    (tmp_path / "j.db.head").write_text("garbage", encoding="ascii")
    assert journal.verify().ok  # an unreadable anchor weakens the check, nothing more
    journal.close()


def test_an_intact_anchor_ahead_of_this_append_stays_quiet(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Another process already anchored a later intact record. Do not cry tamper."""
    journal = SqliteJournal(tmp_path / "j.db")
    journal.append(RecordKind.ANOMALY, '{"n": 1}', ts=TS)
    second = journal.append(RecordKind.ANOMALY, '{"n": 2}', ts=TS)
    first = next(journal.records())
    with caplog.at_level("CRITICAL", logger="ismet.execution"):
        journal._write_anchor(first.seq, first.hash)
    assert not [
        record for record in caplog.records if "behind its anchor" in record.message
    ]
    text = journal.anchor_path.read_text(encoding="ascii")
    assert text.startswith(f"{second.seq} ")
    assert journal.verify().ok
    journal.close()


def test_a_later_append_cannot_move_the_anchor_backward(tmp_path: Path) -> None:
    """The check and the replace share the write, so the anchor only grows."""
    path = tmp_path / "j.db"
    first = SqliteJournal(path)
    first.append(RecordKind.ANOMALY, '{"n": 1}', ts=TS)
    read_anchor = first._read_anchor
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def stalled() -> tuple[int, str] | None:
        value = read_anchor()
        entered.set()
        assert release.wait(5)
        return value

    first._read_anchor = stalled  # type: ignore[method-assign]

    def append_first() -> None:
        first.append(RecordKind.ANOMALY, '{"n": 2}', ts=TS)

    def append_second() -> None:
        second = SqliteJournal(path)
        second.append(RecordKind.ANOMALY, '{"n": 3}', ts=TS)
        second.close()
        finished.set()

    writer = threading.Thread(target=append_first)
    other = threading.Thread(target=append_second)
    writer.start()
    assert entered.wait(5)
    other.start()
    other.join(2.0)
    release.set()
    writer.join(15)
    other.join(15)
    first.close()
    check = SqliteJournal(path)
    count = sum(1 for _ in check.records())
    anchor_seq = int(check.anchor_path.read_text(encoding="ascii").split()[0])
    result = check.verify()
    check.close()
    assert anchor_seq == count
    assert result.ok
    assert finished.is_set()


def test_appending_after_truncation_keeps_the_evidence(tmp_path: Path) -> None:
    """An engage on a truncated journal must not move the anchor onto the short one."""
    path = tmp_path / "j.db"
    journal = SqliteJournal(path)
    for value in range(6):
        journal.append(RecordKind.ANOMALY, json.dumps({"a": value}), ts=TS)
    journal.close()
    _tamper(path, "DELETE FROM journal WHERE seq > 3")
    reopened = SqliteJournal(path)
    reopened.append(RecordKind.KILL_ENGAGED, '{"reason": "r"}', ts=TS)
    result = reopened.verify()
    assert not result.ok and "truncated" in result.detail
    for value in range(3):  # the chain grows back past where the anchor saw it
        reopened.append(RecordKind.ANOMALY, json.dumps({"b": value}), ts=TS)
    reopened.close()
    restarted = SqliteJournal(path)
    result = restarted.verify()
    assert not result.ok and "replaced" in result.detail
    assert result.first_bad_seq == 6
    restarted.append(RecordKind.ANOMALY, "{}", ts=TS)
    assert not restarted.verify().ok
    restarted.close()


def test_reopening_a_journal_whose_triggers_were_dropped_is_logged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    path = tmp_path / "j.db"
    _three_records(path)
    _tamper(path, "SELECT 1")
    with caplog.at_level("CRITICAL", logger="ismet.execution"):
        journal = SqliteJournal(path)
    journal.close()
    assert any("append-only trigger" in r.message for r in caplog.records)
    caplog.clear()
    with caplog.at_level("CRITICAL", logger="ismet.execution"):
        SqliteJournal(path).close()
    assert not caplog.records  # the triggers are back, and that is not news twice


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


def test_a_narrower_engage_does_not_stop_a_cancel_sweep(journal) -> None:  # type: ignore[no-untyped-def]
    """Two engages can race, and the later must not undo the earlier one's cancels."""
    journal.append(
        RecordKind.KILL_ENGAGED,
        json.dumps({"reason": "desk", "operator": "ops", "cancel_open": True}),
    )
    journal.append(
        RecordKind.KILL_ENGAGED,
        json.dumps({"reason": "overfill", "operator": "oms", "cancel_open": False}),
    )
    state = journal.kill_state()
    assert state.engaged and state.cancel_open
    assert state.reason == "overfill"


def test_a_release_ends_the_wider_engage(journal) -> None:  # type: ignore[no-untyped-def]
    journal.append(
        RecordKind.KILL_ENGAGED,
        json.dumps({"reason": "desk", "operator": "ops", "cancel_open": True}),
    )
    journal.append(
        RecordKind.KILL_RELEASED, json.dumps({"reason": "clear", "operator": "ops"})
    )
    journal.append(
        RecordKind.KILL_ENGAGED,
        json.dumps({"reason": "overfill", "operator": "oms", "cancel_open": False}),
    )
    state = journal.kill_state()
    assert state.engaged and not state.cancel_open
