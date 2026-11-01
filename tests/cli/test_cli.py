"""The CLI through its real entry point, in-process, with TOON output checked."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ismet.cli import main
from ismet.cli.toon import encode, scalar
from ismet.execution import RecordKind, SqliteJournal
from ismet.models import Symbol
from ismet.models.orders import (
    OrderRequest,
    OrderSide,
    SendStarted,
    SubmitIntent,
    dump_order_event,
)

TS = datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for name in list(__import__("os").environ):
        if name.startswith("ISMET_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("ISMET_CONFIG_FILE", str(tmp_path / "absent.toml"))


@pytest.fixture(autouse=True)
def fixed_now(monkeypatch: pytest.MonkeyPatch) -> None:
    """The bundled calendars end, so these tests must not depend on today's date."""
    monkeypatch.setattr(
        "ismet.cli._now", lambda: datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)
    )


def run(capsys: pytest.CaptureFixture[str], *args: str) -> tuple[int, str]:
    code = main(list(args))
    return code, capsys.readouterr().out


def test_no_arguments_shows_live_state_not_a_manual(capsys) -> None:  # type: ignore[no-untyped-def]
    code, out = run(capsys)
    assert code == 0
    assert out.startswith("bin: ") and "description:" in out
    assert "venues[6]{venue,phase,next_open}:" in out
    assert "providers_configured: none" in out
    assert "help[4]:" in out


@pytest.mark.parametrize("flag", ["-v", "-V", "--version"])
def test_version_prints_bare_version(capsys, flag: str) -> None:  # type: ignore[no-untyped-def]
    code, out = run(capsys, flag)
    assert code == 0
    assert out.strip().count(".") == 2 and ":" not in out


def test_unknown_flag_is_a_structured_usage_error(capsys) -> None:  # type: ignore[no-untyped-def]
    code, out = run(capsys, "quote", "ACME@XMOK", "--bogus")
    assert code == 2
    assert "--bogus" in out and "ismet quote --help" in out


def test_abbreviated_flags_are_rejected(capsys) -> None:  # type: ignore[no-untyped-def]
    code, _ = run(capsys, "book", "ACME@XMOK", "--dep", "3")
    assert code == 2


def test_missing_configuration_tells_the_user_what_to_do(capsys) -> None:  # type: ignore[no-untyped-def]
    code, out = run(capsys, "quote", "ACME@XMOK")
    assert code == 1
    assert "no providers configured" in out and "ISMET_PROVIDERS=mock" in out


def test_quote_book_bars_through_the_mock_provider(capsys, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("ISMET_PROVIDERS", "mock")
    code, out = run(capsys, "quote", "ACME", "--venue", "XMOK")
    assert code == 0 and "symbol: ACME@XMOK" in out and "state: normal" in out
    code, out = run(capsys, "book", "ACME@XMOK", "--depth", "2")
    assert code == 0 and "bids[2]{price,size}:" in out and "asks[2]" in out
    code, out = run(
        capsys,
        "bars",
        "ACME@XMOK",
        "--interval",
        "1h",
        "--start",
        "2026-03-09",
        "--end",
        "2026-03-14",
    )
    assert code == 0
    assert "count: 50 of 120 total" in out and "--full" in out
    code, out = run(
        capsys,
        "bars",
        "ACME@XMOK",
        "--interval",
        "1h",
        "--start",
        "2026-03-09",
        "--end",
        "2026-03-09T02:00:00+00:00",
        "--full",
    )
    assert code == 0 and "count: 2 of 2 total" in out and "--full" not in out


def test_provider_errors_are_translated(capsys, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("ISMET_PROVIDERS", "mock")
    code, out = run(capsys, "quote", "ACME@XNYS")
    assert code == 1 and "no registered provider serves" in out
    code, out = run(capsys, "quote", "ACME@NOTAMIC")
    assert code == 1 and "error:" in out
    assert "Traceback" not in out


def test_unknown_provider_is_a_doctor_failure(capsys, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("ISMET_PROVIDERS", "nosuch")
    code, out = run(capsys, "doctor")
    assert code == 1 and "problems: 1" in out and "fail" in out


def test_doctor_reports_presence_of_credentials_never_values(
    capsys, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("ISMET_PROVIDERS", "mock")
    monkeypatch.setenv("ISMET_MOCK_API_KEY", "s3cret-value")
    code, out = run(capsys, "doctor")
    assert code == 0
    assert "api_key" in out and "s3cret-value" not in out


def test_doctor_without_providers_says_how_to_configure(capsys) -> None:  # type: ignore[no-untyped-def]
    code, out = run(capsys, "doctor")
    assert code == 0 and "ISMET_PROVIDERS=mock" in out


def test_calendar_views(capsys) -> None:  # type: ignore[no-untyped-def]
    code, out = run(capsys, "calendar")
    assert code == 0 and "XNSE" in out
    code, out = run(capsys, "calendar", "XNYS")
    assert code == 0 and "timezone: America/New_York" in out and "data_covers" in out
    code, out = run(capsys, "calendar", "XNYS", "--on", "2026-11-26")
    assert code == 0 and "session: closed" in out
    code, out = run(capsys, "calendar", "XNYS", "--on", "2026-11-27")
    assert "early_close: true" in out
    code, out = run(capsys, "calendar", "XNYS", "--next-open")
    assert code == 0 and "next_open:" in out
    code, out = run(capsys, "calendar", "XOFF")
    assert code == 1 and "no trading calendar" in out
    code, out = run(capsys, "calendar", "XNYS", "--on", "1990-01-02")
    assert code == 1 and "Update ismet" in out
    code, out = run(capsys, "calendar", "XNYS", "--on", "garbage")
    assert code == 1 and "error:" in out


def test_providers_lists_capabilities(capsys) -> None:  # type: ignore[no-untyped-def]
    code, out = run(capsys, "providers")
    assert code == 0 and "mock" in out and "market_data" in out


def test_kill_switch_round_trip_is_idempotent_and_audited(
    capsys, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    who = ("--reason", "fat finger", "--operator", "ops")
    code, out = run(capsys, "kill", "engage", "--journal", path, "--create", *who)
    assert code == 0 and "kill: engaged" in out and "cancel_open: true" in out
    code, out = run(capsys, "kill", "engage", "--journal", path, *who)
    assert code == 0 and "no-op" in out
    code, out = run(capsys, "kill", "status", "--journal", path)
    assert code == 0 and "reason: fat finger" in out
    code, out = run(capsys, "kill", "release", "--journal", path, *who)
    assert code == 0 and "kill: not engaged" in out
    code, out = run(capsys, "kill", "release", "--journal", path, *who)
    assert code == 0 and "no-op" in out
    code, out = run(capsys, "kill", "engage", "--journal", path, *who, "--keep-orders")
    assert "cancel_open: false" in out
    code, out = run(capsys, "kill", "engage", "--journal", path, *who)
    assert code == 0 and "cancel_open: true" in out  # widening is recorded, not a no-op
    journal = SqliteJournal(path)
    kinds = [r.kind for r in journal.records()]
    journal.close()
    assert kinds == [
        RecordKind.KILL_ENGAGED,
        RecordKind.KILL_RELEASED,
        RecordKind.KILL_ENGAGED,
        RecordKind.KILL_ENGAGED,
    ]


def test_kill_requires_flags_and_action_and_existing_journal(
    capsys, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    code, out = run(capsys, "kill", "engage", "--journal", str(tmp_path / "j.db"))
    assert code == 2 and "--reason" in out
    who = ("--reason", "r", "--operator", "o")
    typo = tmp_path / "typo.db"
    code, out = run(capsys, "kill", "engage", "--journal", str(typo), *who)
    assert code == 1 and "no journal" in out and not typo.exists()
    code, out = run(capsys, "kill", "release", "--journal", str(typo), *who)
    assert code == 1 and not typo.exists()
    code, out = run(capsys, "kill")
    assert code == 2
    code, out = run(capsys, "kill", "status", "--journal", str(tmp_path / "none.db"))
    assert code == 1 and "no journal" in out
    assert not (tmp_path / "none.db").exists()


def _journal_with_order(path: Path) -> None:
    journal = SqliteJournal(path)
    request = OrderRequest(
        client_order_id="C1",
        symbol=Symbol(venue="XSIM", ticker="ACME"),
        side=OrderSide.BUY,
        quantity="10",
        limit_price="1",
    )
    for event in (
        SubmitIntent(client_order_id="C1", ts=TS, request=request),
        SendStarted(client_order_id="C1", ts=TS),
    ):
        journal.append(
            RecordKind.ORDER_EVENT, dump_order_event(event), client_order_id="C1"
        )
    journal.close()


def test_journal_verify_orders_export(capsys, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    _journal_with_order(path)
    code, out = run(capsys, "journal", "verify", str(path))
    assert code == 0 and "ok: true" in out and "records: 2" in out
    head = next(line for line in out.splitlines() if "head_hash" in line).split()[-1]
    code, out = run(capsys, "journal", "verify", str(path), "--expect-head", head)
    assert code == 0
    code, out = run(capsys, "journal", "verify", str(path), "--expect-head", "f" * 64)
    assert code == 1 and "truncated or replaced" in out
    code, out = run(capsys, "journal", "orders", str(path))
    assert code == 0 and "C1,ACME@XSIM,buy,submitting,0/10" in out
    target = tmp_path / "audit.jsonl"
    code, out = run(capsys, "journal", "export", str(path), "--out", str(target))
    assert code == 0 and "exported: 2" in out
    assert json.loads(target.read_text().splitlines()[0])["kind"] == "order_event"
    code, out = run(capsys, "journal")
    assert code == 2


def test_journal_orders_empty_state_is_definitive(capsys, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    SqliteJournal(path).close()
    code, out = run(capsys, "journal", "orders", str(path), "--all")
    assert code == 0 and "0 orders of 0" in out
    code, out = run(capsys, "journal", "orders", str(path))
    assert "0 open orders of 0" in out


def test_journal_verify_fails_with_exit_code_on_tampering(
    capsys, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    _journal_with_order(path)
    conn = sqlite3.connect(path)
    conn.execute("DROP TRIGGER journal_no_update")
    conn.execute("UPDATE journal SET payload = '{}' WHERE seq = 1")
    conn.commit()
    conn.close()
    code, out = run(capsys, "journal", "verify", str(path))
    assert code == 1 and "ok: false" in out and "first_bad_seq: 1" in out


def test_internal_errors_go_to_stderr_not_stdout(capsys, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    import ismet.cli as cli

    def boom() -> int:
        raise RuntimeError("bug")

    monkeypatch.setattr(cli, "_providers", boom)
    code = main(["providers"])
    captured = capsys.readouterr()
    assert code == 1
    assert "internal error: RuntimeError" in captured.out
    assert "Traceback" in captured.err and "Traceback" not in captured.out


def test_iso_times_with_a_z_suffix_are_accepted(capsys, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("ISMET_PROVIDERS", "mock")
    code, out = run(
        capsys,
        "bars",
        "ACME@XMOK",
        "--interval",
        "1h",
        "--start",
        "2026-03-09T00:00:00Z",
        "--end",
        "2026-03-09T02:00:00Z",
    )
    assert code == 0 and "count: 2 of 2 total" in out


def test_toon_scalars_quote_ambiguous_text() -> None:
    assert scalar("abc\n") == '"abc\\n"'
    assert scalar("plain text") == "plain text"
    assert scalar("true") == '"true"' and scalar("123") == '"123"'
    assert scalar("a,b") == '"a,b"' and scalar("a:b") == '"a:b"'
    assert scalar("") == '""' and scalar(" x") == '" x"'
    assert scalar('say "hi"\n') == '"say \\"hi\\"\\n"'
    assert scalar(None) == "null" and scalar(True) == "true" and scalar(1.5) == "1.5"


def test_toon_structures() -> None:
    text = encode(
        {
            "a": {"b": 1},
            "empty": {},
            "rows": [{"x": 1, "y": "z"}],
            "items": ["p", "q"],
            "none": [],
        }
    )
    assert text.splitlines() == [
        "a:",
        "  b: 1",
        "empty: {}",
        "rows[1]{x,y}:",
        "  1,z",
        "items[2]:",
        "  p",
        "  q",
        "none[0]:",
    ]
