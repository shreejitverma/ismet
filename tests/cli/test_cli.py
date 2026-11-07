"""The CLI through its real entry point, in-process, with TOON output checked."""

from __future__ import annotations

import errno
import json
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import textwrap
from datetime import datetime, timezone
from decimal import Decimal
from importlib.metadata import EntryPoint
from pathlib import Path
from typing import Any

import pytest

from ismet.cli import main
from ismet.cli.toon import encode, scalar
from ismet.errors import JournalError
from ismet.execution import RecordKind, SqliteJournal
from ismet.execution.journal import verify_jsonl
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
    for name in list(os.environ):
        if name.startswith("ISMET_"):
            monkeypatch.delenv(name)
    empty = tmp_path / "empty.toml"
    empty.write_text("", encoding="utf-8")
    monkeypatch.setenv("ISMET_CONFIG_FILE", str(empty))


@pytest.fixture(autouse=True)
def fixed_now(monkeypatch: pytest.MonkeyPatch) -> None:
    """The bundled calendars end, so these tests must not depend on today's date."""
    monkeypatch.setattr(
        "ismet.cli._now", lambda: datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)
    )


_HEADER = re.compile(
    r'(?P<key>"(?:[^"\\\\]|\\\\.)*"|[A-Za-z_][A-Za-z0-9_.]*)?'
    r"(?:\[(?P<count>\d+)\](?:\{(?P<fields>[^}]*)\})?)?:(?: (?P<value>.*))?"
)
_NUMBER = re.compile(r"-?\d+(\.\d+)?([eE][+-]?\d+)?")
_ESCAPES = {"\\": "\\", '"': '"', "n": "\n", "r": "\r", "t": "\t"}


def _toon_scalar(text: str) -> Any:
    """A TOON primitive per the spec: quoted strings, or bare tokens that are safe."""
    if text.startswith('"'):
        if len(text) < 2 or not text.endswith('"'):
            raise ValueError(f"unterminated string: {text}")
        out, body, i = [], text[1:-1], 0
        while i < len(body):
            if body[i] == '"':
                raise ValueError(f"unescaped quote: {text}")
            if body[i] == "\\":
                out.append(_ESCAPES[body[i + 1]])
                i += 2
            else:
                out.append(body[i])
                i += 1
        return "".join(out)
    if text in ("true", "false"):
        return text == "true"
    if text == "null":
        return None
    if _NUMBER.fullmatch(text):
        return Decimal(text)
    if not text or text != text.strip() or text.startswith("-"):
        raise ValueError(f"string must be quoted: {text!r}")
    if any(c in text for c in ':"\\[]{}'):
        raise ValueError(f"string must be quoted: {text!r}")
    return text


def _toon_split(row: str) -> list[str]:
    cells, cell, quoted, escape = [], "", False, False
    for char in row:
        if escape:
            escape = False
        elif char == "\\" and quoted:
            escape = True
        elif char == '"':
            quoted = not quoted
        elif char == "," and not quoted:
            cells.append(cell)
            cell = ""
            continue
        cell += char
    cells.append(cell)
    return cells


def _toon_block(lines: list[tuple[int, str]], at: int, depth: int) -> tuple[dict, int]:  # type: ignore[type-arg]
    result: dict[str, Any] = {}
    while at < len(lines) and lines[at][0] == depth:
        match = _HEADER.fullmatch(lines[at][1])
        if match is None or match["key"] is None:
            raise ValueError(f"not a field: {lines[at][1]!r}")
        key = match["key"]
        key = _toon_scalar(key) if key.startswith('"') else key
        at += 1
        if match["count"] is None:
            if match["value"] is not None:
                result[key] = _toon_scalar(match["value"])
            else:
                result[key], at = _toon_block(lines, at, depth + 1)
            continue
        count = int(match["count"])
        rows: list[Any] = []
        if match["value"] is not None:
            rows = [_toon_scalar(v) for v in _toon_split(match["value"])]
        elif match["fields"] is not None:
            names = match["fields"].split(",")
            while at < len(lines) and lines[at][0] == depth + 1:
                cells = _toon_split(lines[at][1])
                if len(cells) != len(names):
                    raise ValueError(f"row width: {lines[at][1]!r}")
                rows.append(dict(zip(names, map(_toon_scalar, cells), strict=True)))
                at += 1
        else:
            while at < len(lines) and lines[at][0] == depth + 1:
                text = lines[at][1]
                if not text.startswith("- "):
                    raise ValueError(f"list item needs a hyphen: {text!r}")
                rows.append(_toon_scalar(text[2:]))
                at += 1
        if len(rows) != count:
            raise ValueError(f"{key} declares {count} items, has {len(rows)}")
        result[key] = rows
    return result, at


def decode_toon(text: str) -> dict[str, Any]:
    """A strict decoder written from the TOON spec, independent of the encoder."""
    lines: list[tuple[int, str]] = []
    for raw in text.splitlines():
        stripped = raw.lstrip(" ")
        indent = len(raw) - len(stripped)
        if indent % 2:
            raise ValueError(f"odd indent: {raw!r}")
        lines.append((indent // 2, stripped))
    result, at = _toon_block(lines, 0, 0)
    if at != len(lines):
        raise ValueError(f"unexpected line: {lines[at]}")
    return result


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
    decoded = decode_toon(out)
    assert decoded["count"] == "50 of 120 total"
    hint = re.fullmatch(r"Run `ismet (.*)` for all rows", decoded["help"][0])
    assert hint is not None
    code, out = run(capsys, *shlex.split(hint[1]))
    assert code == 0 and decode_toon(out)["count"] == "120 of 120 total"
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


def test_providers_lists_names_and_names_one_for_capabilities(capsys) -> None:  # type: ignore[no-untyped-def]
    code, out = run(capsys, "providers")
    assert code == 0 and "mock" in decode_toon(out)["providers"]
    code, out = run(capsys, "providers", "mock")
    assert code == 0 and "market_data" in decode_toon(out)["capabilities"]
    code, out = run(capsys, "providers", "nonesuch")
    assert code == 1 and "unknown provider" in out


def test_listing_providers_imports_nothing(capsys, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    broken = EntryPoint(
        name="broken",
        value="ismet_no_such_module:Provider",
        group="ismet.providers",
    )
    monkeypatch.setattr("ismet.providers.registry.entry_points", lambda **_: [broken])
    code, out = run(capsys, "providers")
    assert code == 0 and decode_toon(out)["providers"] == ["broken"]
    code, out = run(capsys, "providers", "broken")
    assert code == 1 and "failed to import" in out


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


@pytest.mark.parametrize(
    ("reason", "operator"), [("r", ""), ("r", "   "), ("", "ops"), ("  ", "ops")]
)
def test_kill_release_refuses_a_blank_name_or_reason(
    capsys, tmp_path: Path, reason: str, operator: str
) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    who = ("--reason", "fat finger", "--operator", "ops")
    run(capsys, "kill", "engage", "--journal", path, "--create", *who)
    code, out = run(
        capsys,
        "kill",
        "release",
        "--journal",
        path,
        "--reason",
        reason,
        "--operator",
        operator,
    )
    assert code == 2 and "is blank" in out
    code, out = run(capsys, "kill", "status", "--journal", path)
    assert code == 0 and "kill: engaged" in out
    journal = SqliteJournal(path)
    kinds = [r.kind for r in journal.records()]
    journal.close()
    assert kinds == [RecordKind.KILL_ENGAGED]


def test_kill_status_stays_engaged_after_a_nameless_release_row(
    capsys, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    who = ("--reason", "fat finger", "--operator", "ops")
    run(capsys, "kill", "engage", "--journal", path, "--create", *who)
    journal = SqliteJournal(path)
    journal.append(RecordKind.KILL_RELEASED, '{"reason": "r", "operator": " "}')
    journal.close()
    code, out = run(capsys, "kill", "status", "--journal", path)
    assert code == 0 and "kill: engaged" in out and "reason: fat finger" in out


def test_a_halt_is_not_refused_for_want_of_a_name(capsys, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    code, out = run(
        capsys,
        "kill",
        "engage",
        "--journal",
        path,
        "--create",
        "--reason",
        "",
        "--operator",
        "",
    )
    assert code == 0 and "kill: engaged" in out


@pytest.mark.parametrize(
    ("reason", "operator"), [("split", ""), ("split", " "), ("", "ops"), (" ", "ops")]
)
def test_position_adjust_refuses_a_blank_name_or_reason(
    capsys, tmp_path: Path, reason: str, operator: str
) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    SqliteJournal(path).close()
    code, out = run(
        capsys,
        "position",
        "adjust",
        "ACME@XNYS",
        "--journal",
        path,
        "--delta",
        "10",
        "--reason",
        reason,
        "--operator",
        operator,
    )
    assert code == 2 and "is blank" in out
    journal = SqliteJournal(path)
    records = list(journal.records())
    journal.close()
    assert records == []


def _journal_with_order(path: Path) -> None:
    journal = SqliteJournal(path)
    request = OrderRequest(
        client_order_id="C1",
        symbol=Symbol(venue="ZSIM", ticker="ACME"),
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
    assert code == 0 and "C1,ACME@ZSIM,buy,submitting,0/10" in out
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

    def boom(args: object) -> int:
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
        "empty:",
        "rows[1]{x,y}:",
        "  1,z",
        "items[2]:",
        "  - p",
        "  - q",
        "none[0]:",
    ]


def test_toon_round_trips_through_a_spec_decoder() -> None:
    value = {
        "text": 'a, b: [c] "d" \\ e',
        "keyword": "true",
        "numeric_text": "007",
        "empty_text": "",
        "number": Decimal("1E+2"),
        "flag": False,
        "nothing": None,
        "nested": {"inner": {"x": 1}},
        "empty": {},
        "rows": [{"a": "x,y", "b": 2}, {"a": "z", "b": 3}],
        "words": ["one two", "three: four", "-five"],
        "none": [],
        "odd key": 1,
    }
    decoded = decode_toon(encode(value))
    assert decoded["text"] == value["text"]
    assert decoded["keyword"] == "true" and decoded["numeric_text"] == "007"
    assert decoded["empty_text"] == "" and decoded["number"] == 100
    assert decoded["flag"] is False and decoded["nothing"] is None
    assert decoded["nested"] == {"inner": {"x": 1}} and decoded["empty"] == {}
    assert decoded["rows"] == [{"a": "x,y", "b": 2}, {"a": "z", "b": 3}]
    assert decoded["words"] == value["words"]
    assert decoded["none"] == [] and decoded["odd key"] == 1


def test_a_list_holding_structures_is_refused_rather_than_flattened() -> None:
    with pytest.raises(TypeError, match="scalars or uniform rows"):
        encode({"mixed": [1, {"nested": 2}]})


@pytest.mark.parametrize(
    "command",
    [(), ("calendar",), ("calendar", "XNYS"), ("doctor",), ("providers",)],
    ids=lambda c: " ".join(c) or "home",
)
def test_real_command_output_is_valid_toon(capsys, command: tuple[str, ...]) -> None:  # type: ignore[no-untyped-def]
    _, out = run(capsys, *command)
    decoded = decode_toon(out)
    assert decoded


def test_doctor_hint_and_venue_list_decode_as_lists(capsys) -> None:  # type: ignore[no-untyped-def]
    _, out = run(capsys, "doctor")
    assert "ISMET_PROVIDERS=mock" in decode_toon(out)["help"][0]
    _, out = run(capsys, "calendar")
    assert "XNYS" in decode_toon(out)["calendars"]


def _plain_sqlite(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE bars (x)")
    conn.commit()
    conn.close()


def _tables(path: Path) -> list[str]:
    conn = sqlite3.connect(path)
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    conn.close()
    return [row[0] for row in rows]


@pytest.mark.parametrize(
    "command",
    [
        ("kill", "engage", "--journal", "{}", "--reason", "r", "--operator", "o"),
        (
            "kill",
            "engage",
            "--journal",
            "{}",
            "--create",
            "--reason",
            "r",
            "--operator",
            "o",
        ),
        ("kill", "status", "--journal", "{}"),
        ("journal", "verify", "{}"),
        ("journal", "orders", "{}"),
    ],
    ids=lambda c: " ".join(
        w
        for w in c
        if w in ("kill", "journal", "engage", "status", "verify", "orders", "--create")
    ),
)
def test_a_file_that_is_not_a_journal_is_refused_and_left_alone(
    capsys, tmp_path: Path, command: tuple[str, ...]
) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "bars.db"
    _plain_sqlite(path)
    code, out = run(capsys, *(w.replace("{}", str(path)) for w in command))
    assert code == 1 and "not an ismet journal" in out
    assert _tables(path) == ["bars"]


def test_a_file_that_is_not_a_database_is_refused(capsys, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "notes.txt"
    path.write_text("this is not a database, just some words", encoding="utf-8")
    code, out = run(capsys, "kill", "status", "--journal", str(path))
    assert code == 1 and "not an ismet journal" in out
    assert path.read_text(encoding="utf-8").startswith("this is not")


def test_export_never_overwrites_and_cannot_destroy_the_journal(
    capsys, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    _journal_with_order(path)
    code, out = run(capsys, "journal", "export", str(path), "--out", str(path))
    assert code == 1 and "already exists" in out
    code, out = run(capsys, "journal", "verify", str(path))
    assert code == 0 and "records: 2" in out
    other = tmp_path / "keep.txt"
    other.write_text("keep", encoding="utf-8")
    code, _ = run(capsys, "journal", "export", str(path), "--out", str(other))
    assert code == 1 and other.read_text(encoding="utf-8") == "keep"
    target = tmp_path / "audit.jsonl"
    code, out = run(capsys, "journal", "export", str(path), "--out", str(target))
    assert code == 0 and "exported: 2" in out
    assert sorted(p.name for p in tmp_path.iterdir() if p.name.startswith(".")) == []
    assert len(target.read_text(encoding="utf-8").splitlines()) == 2


def test_an_empty_expected_head_is_a_usage_error(capsys, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    _journal_with_order(path)
    code, out = run(capsys, "journal", "verify", str(path), "--expect-head", "")
    assert code == 2 and "--expect-head" in out and "chain intact" not in out


@pytest.mark.parametrize(
    "arguments",
    [
        ("kill", "engage", "--journal", "j.db", "--reason", "r", "--op", "o", "--k"),
        ("journal", "export", "j.db", "--o", "out"),
    ],
)
def test_safety_commands_refuse_abbreviated_flags(capsys, arguments) -> None:  # type: ignore[no-untyped-def]
    code, _ = run(capsys, *arguments)
    assert code == 2


def test_a_naive_time_is_refused_as_the_sync_client_refuses_it(
    capsys, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("ISMET_PROVIDERS", "mock")
    code, out = run(
        capsys,
        "bars",
        "ACME@XMOK",
        "--start",
        "2026-03-09T09:30:00",
        "--end",
        "2026-03-10",
    )
    assert code == 1 and "timezone-aware" in out


def _triggers(path: Path) -> set[str]:
    conn = sqlite3.connect(path)
    found = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'")
    }
    conn.close()
    return found


@pytest.mark.parametrize(
    "command",
    [
        ("journal", "verify", "{}"),
        ("journal", "orders", "{}", "--all"),
        ("kill", "status", "--journal", "{}"),
    ],
)
def test_read_commands_leave_tampering_evidence_where_it_is(
    capsys, tmp_path: Path, command: tuple[str, ...]
) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    _journal_with_order(path)
    conn = sqlite3.connect(path)
    conn.execute("DROP TRIGGER journal_no_update")
    conn.commit()
    conn.close()
    assert "journal_no_update" not in _triggers(path)
    code, _ = run(capsys, *(word.format(path) for word in command))
    assert code == 0
    assert "journal_no_update" not in _triggers(path)


def test_journal_export_also_leaves_the_journal_as_it_found_it(
    capsys, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    _journal_with_order(path)
    conn = sqlite3.connect(path)
    conn.execute("DROP TRIGGER journal_no_update")
    conn.commit()
    conn.close()
    code, _ = run(capsys, "journal", "export", str(path), "--out", str(tmp_path / "o"))
    assert code == 0 and "journal_no_update" not in _triggers(path)


def test_a_corrupted_kind_with_an_expected_head_still_names_the_bad_record(
    capsys, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    _journal_with_order(path)
    journal = SqliteJournal(path)
    head = journal.head_hash()
    journal.close()
    conn = sqlite3.connect(path)
    conn.execute("DROP TRIGGER journal_no_update")
    conn.execute("UPDATE journal SET kind = 'nonsense' WHERE seq = 1")
    conn.commit()
    conn.close()
    code, out = run(capsys, "journal", "verify", str(path), "--expect-head", head)
    assert code == 1 and "ok: false" in out and "first_bad_seq: 1" in out


read_only_directories_work = os.name != "nt" and os.geteuid() != 0


@pytest.fixture
def read_only_copy(tmp_path: Path):  # type: ignore[no-untyped-def]
    if not read_only_directories_work:
        pytest.skip("needs a directory this user cannot write")
    audit = tmp_path / "audit"
    audit.mkdir()
    yield audit
    audit.chmod(0o755)


def test_an_audit_copy_in_a_read_only_directory_can_be_verified(
    capsys, tmp_path: Path, read_only_copy: Path
) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    _journal_with_order(path)
    copied = read_only_copy / "j.db"
    shutil.copy(path, copied)
    read_only_copy.chmod(0o555)
    code, out = run(capsys, "journal", "verify", str(copied))
    assert code == 0 and "ok: true" in out and "records: 2" in out


def test_an_unreadable_journal_is_not_called_something_else(
    capsys, tmp_path: Path, read_only_copy: Path
) -> None:  # type: ignore[no-untyped-def]
    """A copy with an unmerged WAL cannot be read here, and that is what it must say."""
    path = tmp_path / "j.db"
    journal = SqliteJournal(path)
    journal.append(RecordKind.KILL_ENGAGED, '{"reason": "r", "operator": "o"}')
    copied = read_only_copy / "j.db"
    shutil.copy(path, copied)
    shutil.copy(path.with_name("j.db-wal"), read_only_copy / "j.db-wal")
    journal.close()
    read_only_copy.chmod(0o555)
    code, out = run(capsys, "journal", "verify", str(copied))
    assert code == 1
    assert "not an ismet journal" not in out and "cannot read journal" in out


def test_files_that_are_not_journals_are_still_called_that(
    capsys, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    text = tmp_path / "notes.txt"
    text.write_text("this is not a database, not by a long way " * 50)
    code, out = run(capsys, "journal", "verify", str(text))
    assert code == 1 and "not an ismet journal" in out
    other = tmp_path / "other.db"
    conn = sqlite3.connect(other)
    conn.execute("CREATE TABLE something (x)")
    conn.commit()
    conn.close()
    code, out = run(capsys, "kill", "status", "--journal", str(other))
    assert code == 1 and "not an ismet journal" in out


def test_a_journal_left_by_a_killed_process_can_be_read(capsys, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """After a crash the record is only in the WAL, and this is when people look."""
    path = tmp_path / "j.db"
    crash = textwrap.dedent(
        f"""
        import os
        from ismet.execution import RecordKind, SqliteJournal
        journal = SqliteJournal({str(path)!r})
        journal.append(RecordKind.KILL_ENGAGED, '{{"reason": "r", "operator": "o"}}')
        os._exit(0)
        """
    )
    subprocess.run([sys.executable, "-c", crash], check=True)
    assert path.with_name("j.db-wal").stat().st_size > 0
    code, out = run(capsys, "journal", "verify", str(path))
    assert code == 0 and "ok: true" in out and "records: 1" in out
    code, out = run(capsys, "kill", "status", "--journal", str(path))
    assert code == 0 and "kill: engaged" in out


def test_a_journal_held_open_by_a_running_process_can_be_read(
    capsys, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    journal = SqliteJournal(path)
    journal.append(RecordKind.KILL_ENGAGED, '{"reason": "r", "operator": "o"}')
    code, out = run(capsys, "kill", "status", "--journal", str(path))
    assert code == 0 and "kill: engaged" in out
    journal.append(RecordKind.KILL_RELEASED, '{"reason": "r", "operator": "o"}')
    code, out = run(capsys, "kill", "status", "--journal", str(path))
    assert code == 0 and "kill: not engaged" in out
    journal.close()


def broken_journal(path: str, *rows: tuple[RecordKind, str]) -> None:
    journal = SqliteJournal(path)
    journal.append(RecordKind.ANOMALY, "{}")
    for kind, payload in rows:
        journal.append(kind, payload)
    journal.close()
    connection = sqlite3.connect(path)
    connection.execute("DROP TRIGGER journal_no_update")
    connection.execute("UPDATE journal SET payload = '{\"x\": 1}' WHERE seq = 1")
    connection.commit()
    connection.close()


KILLED = '{"reason": "r", "operator": "o", "cancel_open": false}'
WHO = ("--reason", "fat finger", "--operator", "ops")


def test_kill_release_refuses_a_broken_chain(capsys, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    broken_journal(path, (RecordKind.KILL_ENGAGED, KILLED))
    code, out = run(capsys, "kill", "release", "--journal", path, *WHO)
    assert code == 1 and "failed verification" in out
    journal = SqliteJournal(path)
    kinds = [r.kind for r in journal.records()]
    journal.close()
    assert RecordKind.KILL_RELEASED not in kinds


def test_kill_status_shows_a_broken_chain(capsys, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    broken_journal(path)
    code, out = run(capsys, "kill", "status", "--journal", path)
    assert code == 1 and "broken at record 1" in out


def test_kill_engage_says_when_the_halt_was_not_recorded(
    capsys, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "j.db"
    journal = SqliteJournal(path)
    journal.append(RecordKind.ANOMALY, "{}")
    journal.close()

    def fail(self, *args: object, **kwargs: object) -> None:
        raise JournalError("journal write failed: disk is full")

    monkeypatch.setattr(SqliteJournal, "append", fail)
    code, out = run(capsys, "kill", "engage", "--journal", str(path), *WHO)
    assert code == 1
    assert "halt NOT recorded" in out
    assert "cancel at the venue directly" in out
    assert "tampered" not in out


def test_kill_engage_records_the_halt_before_it_reads_the_chain(
    capsys, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "j.db"
    journal = SqliteJournal(path)
    journal.append(RecordKind.ANOMALY, "{}")
    journal.close()
    calls: list[str] = []
    real_append = SqliteJournal.append
    real_verify = SqliteJournal.verify

    def append(self: SqliteJournal, kind: RecordKind, *args: Any, **kwargs: Any) -> Any:
        calls.append(f"append {kind.value}")
        return real_append(self, kind, *args, **kwargs)

    def verify(self: SqliteJournal) -> Any:
        calls.append("verify")
        return real_verify(self)

    monkeypatch.setattr(SqliteJournal, "append", append)
    monkeypatch.setattr(SqliteJournal, "verify", verify)
    code, out = run(capsys, "kill", "engage", "--journal", str(path), *WHO)
    assert code == 0 and "kill: engaged" in out and "chain:" in out
    assert calls[0] == f"append {RecordKind.KILL_ENGAGED.value}"
    assert "verify" in calls


def test_a_no_op_engage_on_a_broken_chain_reports_the_chain(
    capsys, tmp_path: Path
) -> None:
    path = tmp_path / "j.db"
    journal = SqliteJournal(path)
    journal.append(RecordKind.ANOMALY, "{}")
    journal.append(
        RecordKind.KILL_ENGAGED,
        json.dumps({"reason": "r", "operator": "o", "cancel_open": True}),
    )
    journal.close()
    connection = sqlite3.connect(path)
    connection.execute("DROP TRIGGER journal_no_update")
    connection.execute("UPDATE journal SET payload = '{\"x\": 1}' WHERE seq = 1")
    connection.commit()
    connection.close()
    code, out = run(capsys, "kill", "engage", "--journal", str(path), *WHO)
    assert code == 0 and "no-op" in out and "chain:" in out


def test_kill_engage_still_works_on_a_broken_chain(capsys, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    broken_journal(path)
    code, out = run(capsys, "kill", "engage", "--journal", path, *WHO)
    assert code == 0 and "kill: engaged" in out


def test_kill_engage_keeps_the_evidence_of_a_truncated_journal(
    capsys, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    journal = SqliteJournal(path)
    for value in range(6):
        journal.append(RecordKind.ANOMALY, json.dumps({"a": value}))
    journal.close()
    connection = sqlite3.connect(path)
    connection.execute("DROP TRIGGER journal_no_delete")
    connection.execute("DELETE FROM journal WHERE seq > 3")
    connection.commit()
    connection.close()
    code, out = run(capsys, "kill", "engage", "--journal", path, *WHO)
    assert code == 0 and "kill: engaged" in out
    assert "broken at record 6" in out and "truncated" in out
    code, out = run(capsys, "journal", "verify", path)
    assert code == 1 and "truncated" in out
    code, out = run(capsys, "kill", "release", "--journal", path, *WHO)
    assert code == 1 and "failed verification" in out
    journal = SqliteJournal(path)
    assert journal.kill_state().engaged
    journal.close()


def adjust(capsys, path: str, delta: str = "10"):  # type: ignore[no-untyped-def]
    return run(
        capsys,
        "position",
        "adjust",
        "ACME@XNYS",
        "--journal",
        path,
        "--delta",
        delta,
        *WHO,
    )


def test_position_adjust_refuses_a_broken_chain(capsys, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    broken_journal(path, (RecordKind.POSITION_BASELINE, "{}"))
    code, out = adjust(capsys, path)
    assert code == 1 and "failed verification" in out


def test_position_adjust_refuses_a_journal_with_no_baseline(
    capsys, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    SqliteJournal(path).close()
    code, out = adjust(capsys, path)
    assert code == 1 and "no position baseline" in out


@pytest.mark.parametrize("delta", ["NaN", "sNaN", "Infinity", "1E+1000", "ten"])
def test_position_adjust_refuses_a_delta_out_of_range(
    capsys, tmp_path: Path, delta: str
) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    journal = SqliteJournal(path)
    journal.append(RecordKind.POSITION_BASELINE, "{}")
    journal.close()
    code, out = adjust(capsys, path, delta)
    assert code == 2 and "--delta" in out
    journal = SqliteJournal(path)
    kinds = [r.kind for r in journal.records()]
    journal.close()
    assert kinds == [RecordKind.POSITION_BASELINE]


def test_position_adjust_writes_on_a_journal_with_a_baseline(
    capsys, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    journal = SqliteJournal(path)
    journal.append(RecordKind.POSITION_BASELINE, "{}")
    journal.close()
    code, out = adjust(capsys, path, "1.5")
    assert code == 0 and "adjusted: ACME@XNYS" in out and 'delta: "1.5"' in out
    journal = SqliteJournal(path)
    record = list(journal.records())[-1]
    journal.close()
    assert record.kind is RecordKind.POSITION_ADJUSTED
    assert json.loads(record.payload) == {
        "symbol": "ACME@XNYS",
        "delta": "1.5",
        "operator": "ops",
        "reason": "fat finger",
    }


@pytest.mark.parametrize("payload", ["{not json", "[1]"])
@pytest.mark.parametrize("flags", [(), ("--keep-orders",)])
def test_kill_engage_survives_a_corrupt_kill_record(
    capsys, tmp_path: Path, payload: str, flags: tuple[str, ...]
) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    journal = SqliteJournal(path)
    journal.append(RecordKind.KILL_RELEASED, '{"reason": "r", "operator": "o"}')
    journal.close()
    connection = sqlite3.connect(path)
    connection.execute("DROP TRIGGER journal_no_update")
    connection.execute("UPDATE journal SET payload = ? WHERE seq = 1", (payload,))
    connection.commit()
    connection.close()
    code, out = run(capsys, "kill", "engage", "--journal", path, *WHO, *flags)
    assert code == 0 and "engaged" in out
    journal = SqliteJournal(path)
    state = journal.kill_state()
    journal.close()
    assert state.engaged


def test_a_later_readable_release_is_not_hidden_by_a_corrupt_record(
    capsys, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    broken_journal(
        path,
        (RecordKind.KILL_ENGAGED, KILLED),
        (RecordKind.KILL_RELEASED, '{"reason": "r", "operator": "o"}'),
    )
    connection = sqlite3.connect(path)
    connection.execute("UPDATE journal SET payload = '{bad' WHERE seq = 2")
    connection.commit()
    connection.close()
    journal = SqliteJournal(path)
    state = journal.kill_state()
    journal.close()
    assert not state.engaged


def order_journal_broken(path: Path) -> None:
    _journal_with_order(path)
    connection = sqlite3.connect(path)
    connection.execute("DROP TRIGGER journal_no_update")
    connection.execute(
        "UPDATE journal SET payload = replace(payload, '\"10\"', '\"99999\"') "
        "WHERE seq = 1"
    )
    connection.commit()
    connection.close()


def test_journal_orders_reports_a_broken_chain(capsys, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    order_journal_broken(path)
    code, out = run(capsys, "journal", "orders", str(path), "--all")
    assert code == 1 and "broken at record 1" in out


def test_journal_orders_reports_an_intact_chain(capsys, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    _journal_with_order(path)
    code, out = run(capsys, "journal", "orders", str(path), "--all")
    assert code == 0 and "chain: intact" in out


def test_journal_export_reports_a_broken_chain(capsys, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    order_journal_broken(path)
    out_path = tmp_path / "out.jsonl"
    code, out = run(capsys, "journal", "export", str(path), "--out", str(out_path))
    assert code == 1 and "broken at record 1" in out
    assert out_path.exists()


def test_export_is_on_disk_before_it_takes_its_name(
    capsys, tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    _journal_with_order(path)
    out_path = tmp_path / "out.jsonl"
    steps: list[str] = []
    fsync, link = os.fsync, os.link

    def spy_fsync(descriptor: int) -> None:
        steps.append("fsync")
        fsync(descriptor)

    def spy_link(*args: Any, **kw: Any) -> None:
        steps.append("link")
        link(*args, **kw)

    monkeypatch.setattr(os, "fsync", spy_fsync)
    monkeypatch.setattr(os, "link", spy_link)
    code, _ = run(capsys, "journal", "export", str(path), "--out", str(out_path))
    assert code == 0 and steps[:2] == ["fsync", "link"]
    # Windows cannot sync a directory, so the name's own fsync is POSIX only.
    assert steps[2:] == ([] if os.name == "nt" else ["fsync"])
    assert [p.name for p in tmp_path.glob(".out.jsonl.*")] == []


def test_export_never_replaces_a_file_that_appeared_meanwhile(
    capsys, tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    import ismet.cli as cli

    path = tmp_path / "j.db"
    _journal_with_order(path)
    out_path = tmp_path / "out.jsonl"
    real = cli.export_jsonl

    def racing(journal: object, stream: object) -> int:
        out_path.write_text("someone else's audit file", encoding="utf-8")
        return real(journal, stream)  # type: ignore[arg-type]

    monkeypatch.setattr(cli, "export_jsonl", racing)
    code, out = run(capsys, "journal", "export", str(path), "--out", str(out_path))
    assert code == 1 and "already exists" in out
    assert out_path.read_text(encoding="utf-8") == "someone else's audit file"
    assert [p.name for p in tmp_path.glob(".out.jsonl.*")] == []


def test_doctor_names_the_config_file_it_read(
    capsys, tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    override = tmp_path / "x.toml"
    override.write_text('providers = ["mock"]\n', encoding="utf-8")
    monkeypatch.setenv("ISMET_CONFIG_FILE", str(override))
    _, out = run(capsys, "doctor")
    assert decode_toon(out)["config_file"] == str(override)


def test_doctor_fails_when_the_config_override_names_no_file(
    capsys, tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    typo = tmp_path / "typo.toml"
    monkeypatch.setenv("ISMET_CONFIG_FILE", str(typo))
    code, out = run(capsys, "doctor")
    assert code == 1
    assert "problems: 1" in out
    assert "fail" in out and "typo.toml" in out


def test_home_reports_a_malformed_config_rather_than_no_providers(
    capsys, tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    override = tmp_path / "x.toml"
    override.write_text("not toml [[[", encoding="utf-8")
    monkeypatch.setenv("ISMET_CONFIG_FILE", str(override))
    monkeypatch.setenv("ISMET_PROVIDERS", "mock")
    code, out = run(capsys)
    assert code == 1
    assert "providers_configured: unknown" in out
    assert "config_error" in out and "ismet doctor" in out


def test_bars_show_flags_and_adjustment(capsys, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from ismet.client import IsmetClient
    from ismet.models import Adjustment, Bar, Interval, QualityFlag

    ts = datetime(2026, 3, 9, 14, 30, tzinfo=timezone.utc)
    flagged = Bar(
        symbol=Symbol(venue="XMOK", ticker="ACME"),
        exchange_ts=ts,
        received_ts=ts,
        interval=Interval.D1,
        open="1",
        high="1",
        low="1",
        close="1",
        volume="1",
        adjustment=Adjustment.SPLIT,
        quality=frozenset({QualityFlag.OUTLIER}),
    )

    async def bars(self: object, *args: object, **kw: object) -> list[Bar]:
        return [flagged]

    monkeypatch.setattr(IsmetClient, "bars", bars)
    monkeypatch.setenv("ISMET_PROVIDERS", "mock")
    code, out = run(capsys, "bars", "ACME@XMOK")
    assert code == 0 and "outlier" in out and "split" in out


def _no_hard_links(*args: Any, **kw: Any) -> None:
    raise OSError(errno.ENOTSUP, "this filesystem has no hard links")


def test_export_works_on_a_filesystem_without_hard_links(
    capsys, tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    _journal_with_order(path)
    target = tmp_path / "out.jsonl"
    monkeypatch.setattr(os, "link", _no_hard_links)
    code, out = run(capsys, "journal", "export", str(path), "--out", str(target))
    assert code == 0 and "exported: 2" in out
    assert verify_jsonl(target.read_text("utf-8").splitlines()).records == 2
    assert [p.name for p in tmp_path.glob(".out.jsonl.*")] == []


def test_export_without_hard_links_still_never_replaces_a_file(
    capsys, tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    import ismet.cli as cli

    path = tmp_path / "j.db"
    _journal_with_order(path)
    target = tmp_path / "out.jsonl"
    real = cli.export_jsonl

    def racing(journal: object, stream: object) -> int:
        target.write_text("someone else's audit file", encoding="utf-8")
        return real(journal, stream)  # type: ignore[arg-type]

    monkeypatch.setattr(cli, "export_jsonl", racing)
    monkeypatch.setattr(os, "link", _no_hard_links)
    code, out = run(capsys, "journal", "export", str(path), "--out", str(target))
    assert code == 1 and "already exists" in out
    assert target.read_text("utf-8") == "someone else's audit file"
    assert [p.name for p in tmp_path.glob(".out.jsonl.*")] == []


def test_export_failing_to_take_its_name_leaves_nothing_behind(
    capsys, tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    _journal_with_order(path)
    target = tmp_path / "out.jsonl"

    def no_rename(*args: Any, **kw: Any) -> None:
        raise OSError(errno.EIO, "disk error")

    monkeypatch.setattr(os, "link", _no_hard_links)
    monkeypatch.setattr(Path, "replace", no_rename)
    code, out = run(capsys, "journal", "export", str(path), "--out", str(target))
    assert code == 1 and "chain: intact" in out
    assert not target.exists()
    assert [p.name for p in tmp_path.glob(".out.jsonl.*")] == []


@pytest.mark.skipif(
    os.name == "nt" or (hasattr(os, "geteuid") and os.geteuid() == 0),
    reason="needs POSIX directory permissions, which root ignores",
)
def test_an_export_whose_directory_cannot_be_synced_says_so(
    capsys, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    _journal_with_order(path)
    write_only = tmp_path / "drop"
    write_only.mkdir()
    write_only.chmod(0o300)
    target = write_only / "out.jsonl"
    try:
        code, out = run(capsys, "journal", "export", str(path), "--out", str(target))
        assert code == 0 and "exported: 2" in out and "warning:" in out
        assert verify_jsonl(target.read_text("utf-8").splitlines()).records == 2
        code, out = run(capsys, "journal", "export", str(path), "--out", str(target))
        assert code == 1 and "already exists" in out
    finally:
        write_only.chmod(0o700)


def _corrupt(path: Path, column: str, value: str) -> None:
    connection = sqlite3.connect(path)
    connection.execute("DROP TRIGGER journal_no_update")
    connection.execute(f"UPDATE journal SET {column} = ? WHERE seq = 2", (value,))
    connection.commit()
    connection.close()


@pytest.mark.parametrize(
    ("column", "value"),
    [("payload", "{bad"), ("kind", "junk"), ("ts", "nope")],
)
@pytest.mark.parametrize("action", ["orders", "export"])
def test_an_unreadable_row_does_not_cost_the_chain_result(
    capsys, tmp_path: Path, column: str, value: str, action: str
) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "j.db"
    _journal_with_order(path)
    _corrupt(path, column, value)
    target = tmp_path / "out.jsonl"
    extra = ("--out", str(target)) if action == "export" else ()
    code, out = run(capsys, "journal", action, str(path), *extra)
    assert code == 1
    assert 'chain: "broken at record 2' in out
    assert "error:" in out and "tampered" in out
    assert "doctor" not in out
    assert not target.exists()


def test_a_journal_error_on_a_write_points_at_the_journal_not_the_config(
    capsys, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "j.db")
    broken_journal(path, (RecordKind.KILL_ENGAGED, KILLED))
    for args in (
        ("kill", "release", "--journal", path, *WHO),
        (
            "position",
            "adjust",
            "ACME@XNYS",
            "--journal",
            path,
            "--delta",
            "1",
            *WHO,
        ),
    ):
        code, out = run(capsys, *args)
        assert code == 1 and "tampered" in out and "doctor" not in out


def _in_fresh_process(script: str, *args: str) -> str:
    """Run in a new interpreter, whose process-wide code registries start empty."""
    done = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    return done.stdout


def _cli_in_fresh_process(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "ismet", *args],
        capture_output=True,
        text=True,
        check=False,
    )


JOURNAL_ON_A_PRIVATE_VENUE = """
    import sys
    from datetime import datetime, timezone
    from ismet.execution import RecordKind, SqliteJournal
    from ismet.models import Symbol
    from ismet.models.orders import (
        OrderRequest, OrderSide, SubmitIntent, dump_order_event
    )
    from ismet.refdata.codes import register_private_venue

    register_private_venue("ZABC", "A private venue")
    journal = SqliteJournal(sys.argv[1])
    journal.append(RecordKind.POSITION_BASELINE, "{}")
    event = SubmitIntent(
        client_order_id="C1",
        ts=datetime(2026, 3, 9, tzinfo=timezone.utc),
        request=OrderRequest(
            client_order_id="C1",
            symbol=Symbol(venue="ZABC", ticker="ACME"),
            side=OrderSide.BUY,
            quantity="10",
            limit_price="1",
        ),
    )
    journal.append(
        RecordKind.ORDER_EVENT, dump_order_event(event), client_order_id="C1"
    )
    journal.close()
"""


def test_a_new_process_reads_orders_on_a_venue_it_never_registered(
    tmp_path: Path,
) -> None:
    path = str(tmp_path / "j.db")
    _in_fresh_process(JOURNAL_ON_A_PRIVATE_VENUE, path)
    done = _cli_in_fresh_process("journal", "orders", path)
    assert done.returncode == 0, done.stdout
    assert "C1,ACME@ZABC" in done.stdout and "chain: intact" in done.stdout


def test_a_new_process_can_adjust_a_position_on_a_venue_in_the_journal(
    tmp_path: Path,
) -> None:
    path = str(tmp_path / "j.db")
    _in_fresh_process(JOURNAL_ON_A_PRIVATE_VENUE, path)
    done = _cli_in_fresh_process(
        "position", "adjust", "ACME@ZABC", "--journal", path, "--delta", "5", *WHO
    )
    assert done.returncode == 0, done.stdout
    assert "adjusted: ACME@ZABC" in done.stdout
    journal = SqliteJournal(path)
    last = list(journal.records())[-1]
    journal.close()
    assert last.kind is RecordKind.POSITION_ADJUSTED
    assert json.loads(last.payload)["symbol"] == "ACME@ZABC"


def test_a_mistyped_venue_cannot_be_written_into_the_journal(tmp_path: Path) -> None:
    """A manager could not read it back, and a journal cannot lose a record."""
    path = str(tmp_path / "j.db")
    _in_fresh_process(JOURNAL_ON_A_PRIVATE_VENUE, path)
    done = _cli_in_fresh_process(
        "position", "adjust", "ACME@ZABD", "--journal", path, "--delta", "5", *WHO
    )
    assert done.returncode == 1 and "ZABD" in done.stdout
    journal = SqliteJournal(path)
    kinds = [r.kind for r in journal.records()]
    journal.close()
    assert RecordKind.POSITION_ADJUSTED not in kinds


def test_a_venue_named_only_by_the_baseline_can_be_adjusted(tmp_path: Path) -> None:
    path = tmp_path / "j.db"
    journal = SqliteJournal(path)
    journal.append(
        RecordKind.POSITION_BASELINE, json.dumps({"ACME@ZABC": {"quantity": "1"}})
    )
    journal.close()
    done = _cli_in_fresh_process(
        "position", "adjust", "ACME@ZABC", "--journal", str(path), "--delta", "5", *WHO
    )
    assert done.returncode == 0, done.stdout
