from __future__ import annotations

import subprocess
import sys
import textwrap
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from ismet.fix.store import (
    FixStore,
    FixStoreError,
    MemoryFixStore,
    QuarantinedMessage,
    SqliteFixStore,
    StoredExecution,
    StoredMessage,
)

SESSION = "FIX.4.4:CLIENT->VENUE"


def message(body: bytes = b"11=A\x01", msg_type: str = "D") -> StoredMessage:
    return StoredMessage(0, msg_type, "20260309-14:30:15.123", body)


def execution(
    exec_id: str = "E1", fill: str | None = None, cid: str = "C1"
) -> StoredExecution:
    return StoredExecution(cid, exec_id, "digest", fill, False, "2026-03-09T14:30:15")


@pytest.fixture(params=["memory", "sqlite"])
def store(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[FixStore]:
    instance: FixStore
    if request.param == "memory":
        instance = MemoryFixStore()
    else:
        instance = SqliteFixStore(tmp_path / "fix.db", SESSION)
    yield instance
    instance.close()


class TestBothStores:
    def test_numbers_start_at_one(self, store: FixStore) -> None:
        assert store.next_outbound() == 1
        assert store.next_inbound() == 1

    def test_allocation_is_sequential_and_stores_the_message(
        self, store: FixStore
    ) -> None:
        assert store.allocate_outbound() == 1
        assert store.allocate_outbound(message(b"11=B\x01")) == 2
        assert store.allocate_outbound() == 3
        assert store.next_outbound() == 4
        stored = store.messages(1, 10)
        assert [(m.seq, m.body) for m in stored] == [(2, b"11=B\x01")]

    def test_messages_honour_their_range(self, store: FixStore) -> None:
        for _ in range(5):
            store.allocate_outbound(message())
        assert [m.seq for m in store.messages(2, 4)] == [2, 3, 4]

    def test_inbound_number_moves(self, store: FixStore) -> None:
        store.set_next_inbound(42)
        assert store.next_inbound() == 42

    def test_reset_starts_a_new_session(self, store: FixStore) -> None:
        store.allocate_outbound(message())
        store.set_next_inbound(9)
        store.put_order("C1", "{}")
        store.reset_sequences()
        assert store.next_outbound() == 1
        assert store.next_inbound() == 1
        assert store.messages(1, 10) == []
        assert store.get_order("C1") == "{}"

    def test_orders_and_wire_ids(self, store: FixStore) -> None:
        store.put_order("C1", "one")
        store.put_order("C1", "two")
        store.link_wire_id("C1", "C1")
        store.link_wire_id("C1~1", "C1")
        assert store.get_order("C1") == "two"
        assert store.open_orders() == ["two"]
        store.put_order("C1", "closed", is_open=False)
        assert store.open_orders() == []
        assert store.get_order("C1") == "closed"
        assert store.resolve_wire_id("C1~1") == "C1"
        assert store.resolve_wire_id("nope") is None
        assert store.get_order("nope") is None
        store.delete_order("C1")
        assert store.get_order("C1") is None
        assert store.resolve_wire_id("C1~1") is None

    def test_record_report_is_keyed_by_order_and_exec_id(self, store: FixStore) -> None:
        store.record_report("C1", order_payload="o", execution=execution())
        store.record_report(
            "C2", order_payload=None, execution=execution(cid="C2", fill="f")
        )
        assert store.get_order("C1") == "o"
        assert store.execution("C1", "E1") is not None
        found = store.execution("C2", "E1")
        assert found is not None
        assert found.fill == "f"
        assert store.execution("C3", "E1") is None

    def test_first_record_of_an_exec_id_wins(self, store: FixStore) -> None:
        store.record_report("C1", order_payload=None, execution=execution(fill="a"))
        store.record_report("C1", order_payload=None, execution=execution(fill="b"))
        found = store.execution("C1", "E1")
        assert found is not None
        assert found.fill == "a"

    def test_bust_marks_the_execution(self, store: FixStore) -> None:
        store.record_report("C1", order_payload=None, execution=execution(fill="a"))
        store.record_report("C1", order_payload=None, bust_exec_id="E1")
        found = store.execution("C1", "E1")
        assert found is not None
        assert found.busted
        assert [e.exec_id for e in store.busted_executions()] == ["E1"]
        assert store.fills_since(0) == []

    def test_a_closing_report_leaves_the_open_index(self, store: FixStore) -> None:
        store.record_report("C1", order_payload="working")
        assert store.open_orders() == ["working"]
        store.record_report("C1", order_payload="filled", order_open=False)
        assert store.open_orders() == []

    def test_fills_since_uses_the_later_of_venue_time_and_receipt(
        self, store: FixStore
    ) -> None:
        for exec_id, latest in (("OLD", 99), ("EDGE", 100), ("NEW", 101)):
            store.record_report(
                "C1",
                order_payload=None,
                execution=StoredExecution(
                    "C1", exec_id, "d", "fill", False, "t", latest
                ),
            )
        store.record_report(
            "C1",
            order_payload=None,
            execution=StoredExecution("C1", "ACK", "d", None, False, "t", 500),
        )
        found = {e.exec_id for e in store.fills_since(100)}
        assert found == {"EDGE", "NEW"}

    def test_the_reset_period_survives_a_sequence_reset(self, store: FixStore) -> None:
        assert store.reset_period() is None
        store.set_reset_period("2026-03-09")
        store.reset_sequences()
        assert store.reset_period() == "2026-03-09"


class TestQuarantine:
    def test_round_trip(self, store: FixStore) -> None:
        record = QuarantinedMessage(5, "8", "content", "why", "now")
        store.quarantine(record)
        assert store.quarantined() == [record]

    def test_release_records_the_operator_once(self, store: FixStore) -> None:
        store.quarantine(QuarantinedMessage(5, "8", "c", "r", "t", halted=True))
        assert store.release_quarantine("alice", "checked", "now", expected=1) == 1
        store.quarantine(QuarantinedMessage(6, "8", "c", "r", "t", halted=True))
        assert store.release_quarantine("bob", "checked too", "later", expected=1) == 1
        first, second = store.quarantined()
        assert (first.released_by, first.release_reason) == ("alice", "checked")
        assert first.released_at == "now"
        assert first.halted
        assert second.released_by == "bob"
        assert (
            store.release_quarantine("carol", "nothing left", "last", expected=0) == 0
        )

    def test_release_changes_nothing_unless_the_count_matches(
        self, store: FixStore
    ) -> None:
        store.quarantine(QuarantinedMessage(5, "8", "c", "r", "t", halted=True))
        store.quarantine(QuarantinedMessage(6, "*", "c", "r", "t", halted=True))
        assert store.release_quarantine("alice", "saw one", "now", expected=1) == 2
        assert [q.released_by for q in store.quarantined()] == [None, None]
        assert store.release_quarantine("alice", "saw both", "now", expected=2) == 2
        assert [q.released_by for q in store.quarantined()] == ["alice", "alice"]

    def test_it_survives_a_reset_and_a_reopen(self, tmp_path: Path) -> None:
        path = tmp_path / "fix.db"
        store = SqliteFixStore(path, SESSION)
        store.quarantine(QuarantinedMessage(5, "8", "c", "r", "t"))
        store.reset_sequences()
        store.close()
        again = SqliteFixStore(path, SESSION)
        assert len(again.quarantined()) == 1
        again.close()

    def test_an_oversized_number_is_a_typed_error(self, tmp_path: Path) -> None:
        store = SqliteFixStore(tmp_path / "fix.db", SESSION)
        with pytest.raises(FixStoreError):
            store.set_next_inbound(2**70)
        store.close()


class TestSqlite:
    def test_synchronous_is_full_and_wal_is_on(self, tmp_path: Path) -> None:
        store = SqliteFixStore(tmp_path / "fix.db", SESSION)
        assert store._conn.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert store._conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        store.close()

    def test_numbers_and_messages_survive_a_clean_reopen(self, tmp_path: Path) -> None:
        path = tmp_path / "fix.db"
        first = SqliteFixStore(path, SESSION)
        first.allocate_outbound(message())
        first.allocate_outbound()
        first.set_next_inbound(7)
        first.put_order("C1", "payload")
        first.link_wire_id("C1", "C1")
        first.record_report("C1", order_payload="o2", execution=execution(fill="f"))
        first.close()
        second = SqliteFixStore(path, SESSION)
        assert second.next_outbound() == 3
        assert second.next_inbound() == 7
        assert [m.seq for m in second.messages(1, 9)] == [1]
        assert second.get_order("C1") == "o2"
        assert second.resolve_wire_id("C1") == "C1"
        assert second.execution("C1", "E1") is not None
        second.close()

    def test_numbers_survive_a_process_that_dies_without_closing(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "fix.db"
        script = textwrap.dedent(
            f"""
            import os
            from ismet.fix.store import SqliteFixStore, StoredMessage
            store = SqliteFixStore({str(path)!r}, {SESSION!r})
            store.allocate_outbound(StoredMessage(0, "D", "t", b"11=A\\x01"))
            store.allocate_outbound()
            store.set_next_inbound(5)
            os._exit(9)
            """
        )
        src = Path(__file__).resolve().parents[2] / "src"
        result = subprocess.run(
            [sys.executable, "-c", script],
            env={"PYTHONPATH": str(src)},
            check=False,
        )
        assert result.returncode == 9
        store = SqliteFixStore(path, SESSION)
        assert store.next_outbound() == 3
        assert store.next_inbound() == 5
        assert [m.body for m in store.messages(1, 9)] == [b"11=A\x01"]
        store.close()

    def test_sessions_in_one_file_are_independent(self, tmp_path: Path) -> None:
        path = tmp_path / "fix.db"
        first = SqliteFixStore(path, "one")
        second = SqliteFixStore(path, "two")
        first.allocate_outbound(message())
        first.put_order("C1", "x")
        assert second.next_outbound() == 1
        assert second.messages(1, 9) == []
        assert second.get_order("C1") is None
        first.close()
        second.close()

    def test_close_is_idempotent(self, tmp_path: Path) -> None:
        store = SqliteFixStore(tmp_path / "fix.db", SESSION)
        store.close()
        store.close()

    def test_an_unopenable_path_raises_a_typed_error(self, tmp_path: Path) -> None:
        blocker = tmp_path / "file"
        blocker.write_text("x")
        with pytest.raises(FixStoreError):
            SqliteFixStore(blocker / "fix.db", SESSION)

    def test_a_not_a_database_file_raises_a_typed_error(self, tmp_path: Path) -> None:
        path = tmp_path / "fix.db"
        path.write_bytes(b"this is not sqlite" * 100)
        with pytest.raises(FixStoreError):
            SqliteFixStore(path, SESSION)

    def _broken(self, tmp_path: Path) -> SqliteFixStore:
        store = SqliteFixStore(tmp_path / "fix.db", SESSION)
        store._conn.close()
        return store

    @pytest.mark.parametrize(
        "call",
        [
            lambda s: s.allocate_outbound(),
            lambda s: s.set_next_inbound(1),
            lambda s: s.messages(1, 2),
            lambda s: s.put_order("C", "x"),
            lambda s: s.record_report("C", order_payload="x"),
        ],
    )
    def test_a_failing_database_raises_a_typed_error(
        self, tmp_path: Path, call: Callable[[SqliteFixStore], object]
    ) -> None:
        with pytest.raises(FixStoreError):
            call(self._broken(tmp_path))

    def test_a_failed_allocation_rolls_back(self, tmp_path: Path) -> None:
        store = SqliteFixStore(tmp_path / "fix.db", SESSION)
        store._conn.execute("DROP TABLE fix_outbound")
        with pytest.raises(FixStoreError):
            store.allocate_outbound(message())
        assert store.next_outbound() == 1
        store.close()

    def test_a_report_commits_all_or_nothing(self, tmp_path: Path) -> None:
        store = SqliteFixStore(tmp_path / "fix.db", SESSION)
        store.put_order("C1", "before")
        store._conn.execute("DROP TABLE fix_execution")
        with pytest.raises(FixStoreError):
            store.record_report("C1", order_payload="after", execution=execution())
        assert store.get_order("C1") == "before"
        store.close()

    def test_the_quarantine_and_reset_period_survive_a_reopen(
        self, tmp_path: Path
    ) -> None:
        store = SqliteFixStore(tmp_path / "fix.db", SESSION)
        store.quarantine(QuarantinedMessage(5, "8", "c", "r", "t", halted=True))
        store.set_reset_period("2026-03-09")
        store.close()
        again = SqliteFixStore(tmp_path / "fix.db", SESSION)
        assert again.quarantined()[0].halted
        assert again.quarantined()[0].released_by is None
        assert again.reset_period() == "2026-03-09"
        again.close()
