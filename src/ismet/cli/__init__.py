"""The ``ismet`` command, built to the AXI principles for agent-facing CLIs.

Output is TOON on stdout, including errors. Exit codes: 0 success or no-op, 1 failed,
2 usage. Nothing prompts. Unknown flags are errors. With no arguments it shows live
state rather than a manual.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import importlib.util
import json
import os
import platform
import shlex
import sys
import tempfile
import traceback
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, NoReturn

from pydantic import ValidationError as PydanticValidationError

from ismet.calendars import available_calendars, get_calendar
from ismet.capabilities import capabilities_of
from ismet.cli.toon import encode
from ismet.client import IsmetClient
from ismet.config import ENV_PREFIX, Settings, config_file_path
from ismet.errors import (
    AuthError,
    CalendarOutOfRange,
    ConfigError,
    IsmetError,
    JournalError,
    NotSupported,
    ValidationError,
)
from ismet.execution.journal import (
    RecordKind,
    SqliteJournal,
    VerifyResult,
    chain_contains,
    export_jsonl,
)
from ismet.execution.manager import events_of
from ismet.execution.state import replay
from ismet.models import Symbol, to_decimal
from ismet.models.orders import SubmitIntent
from ismet.providers import ProviderRegistry
from ismet.refdata.codes import accepting_stored_codes, is_known_venue
from ismet.sync import as_datetime

DESCRIPTION = (
    "Typed access to market data, trading calendars, and the safety controls "
    "(journal and kill switch) of the ismet order path"
)
HOME_VENUES = ("XNYS", "XNSE", "XLON", "XTKS", "XHKG", "BNCX")
EXIT_OK, EXIT_FAILED, EXIT_USAGE = 0, 1, 2
_MAX_ROWS = 50
_COMMANDS = (
    "quote",
    "book",
    "bars",
    "calendar",
    "providers",
    "doctor",
    "journal",
    "kill",
    "position",
)
Out = Mapping[str, Any]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _version() -> str:
    try:
        return importlib.metadata.version("ismet")
    except importlib.metadata.PackageNotFoundError:
        from ismet import __version__

        return __version__


def _print(payload: Out) -> None:
    print(encode(payload))


def _bin_path() -> str:
    path = str(Path(sys.argv[0]).resolve())
    home = str(Path.home())
    return "~" + path[len(home) :] if path.startswith(home) else path


class _UsageError(Exception):
    def __init__(self, message: str, help_text: str) -> None:
        super().__init__(message)
        self.help_text = help_text


class _Parser(argparse.ArgumentParser):
    """argparse that reports usage errors in the CLI's structured form."""

    def error(self, message: str) -> NoReturn:
        raise _UsageError(message, f"{self.prog} --help")


def _build_parser() -> _Parser:
    parser = _Parser(prog="ismet", description=DESCRIPTION, allow_abbrev=False)
    parser.add_argument("-v", "-V", "--version", action="store_true")
    commands = parser.add_subparsers(dest="command", parser_class=_Parser)

    def command(name: str, summary: str) -> _Parser:
        sub: _Parser = commands.add_parser(
            name, help=summary, description=summary, allow_abbrev=False
        )
        return sub

    def market(sub: _Parser) -> None:
        sub.add_argument("symbol", help="TICKER@MIC, or TICKER with --venue")
        sub.add_argument("--venue", help="venue MIC")
        sub.add_argument("--provider", help="provider, when several serve the venue")

    market(command("quote", "Show the current quote"))

    book = command("book", "Show the order book")
    market(book)
    book.add_argument("--depth", type=int, default=5, help="levels per side (5)")

    bars = command("bars", "Show historical bars")
    market(bars)
    bars.add_argument("--interval", default="1d", help="1m, 5m, 1h, 1d, ... (1d)")
    bars.add_argument("--start", help="ISO date or time (30 days ago)")
    bars.add_argument("--end", help="ISO date or time, exclusive (now)")
    bars.add_argument("--full", action="store_true", help="show every row")

    calendar = command("calendar", "Show a venue's state, or list venues")
    calendar.add_argument("venue", nargs="?", help="venue MIC; omit to list")
    calendar.add_argument("--on", help="show the session on this date")
    calendar.add_argument(
        "--next-open", action="store_true", help="print only the next open"
    )

    providers = command(
        "providers", "List installed providers, without importing any of them"
    )
    providers.add_argument(
        "name", nargs="?", help="import this provider and show what it can do"
    )
    command("doctor", "Check the installation and configuration")

    journal = command("journal", "Inspect an order journal")
    journal_commands = journal.add_subparsers(dest="action", parser_class=_Parser)
    for name, summary in (
        ("verify", "Verify the journal's hash chain"),
        ("orders", "Rebuild order state from the journal"),
        ("export", "Write the journal as JSON lines"),
    ):
        action = journal_commands.add_parser(
            name, help=summary, description=summary, allow_abbrev=False
        )
        action.add_argument("path", help="journal file")
        if name == "export":
            action.add_argument("--out", required=True, help="output file")
        if name == "verify":
            action.add_argument(
                "--expect-head", help="a saved head hash, to detect truncation"
            )
        if name == "orders":
            action.add_argument("--all", action="store_true", help="include closed")

    kill = command("kill", "Read or set the kill switch in a journal")
    kill_commands = kill.add_subparsers(dest="action", parser_class=_Parser)
    for name, summary in (
        ("status", "Show the kill switch"),
        ("engage", "Stop new orders in every process using this journal"),
        ("release", "Allow new orders again"),
    ):
        action = kill_commands.add_parser(
            name, help=summary, description=summary, allow_abbrev=False
        )
        action.add_argument("--journal", required=True, help="journal file")
        if name == "engage":
            action.add_argument(
                "--create",
                action="store_true",
                help="create the journal if absent; otherwise a mistyped path errors",
            )
        if name != "status":
            action.add_argument("--reason", required=True, help="recorded in the audit")
            action.add_argument("--operator", required=True, help="who is doing this")
        if name == "engage":
            action.add_argument(
                "--keep-orders",
                action="store_true",
                help="stop new orders but leave open orders working",
            )

    position = command("position", "Record position changes no order explains")
    position_commands = position.add_subparsers(dest="action", parser_class=_Parser)
    adjust = position_commands.add_parser(
        "adjust",
        help="Clear a position mismatch with an outside cause, such as a split",
        description="Clear a position mismatch with an outside cause, such as a split",
        allow_abbrev=False,
    )
    adjust.add_argument("symbol", help="TICKER@MIC")
    adjust.add_argument("--journal", required=True, help="journal file")
    adjust.add_argument(
        "--delta", required=True, help="signed quantity to add to ismet's position"
    )
    adjust.add_argument("--reason", required=True, help="recorded in the audit")
    adjust.add_argument("--operator", required=True, help="who is doing this")
    return parser


# Commands


def _home() -> int:
    now = _now()
    rows: list[dict[str, Any]] = []
    for code in HOME_VENUES:
        calendar = get_calendar(code)
        try:
            rows.append(
                {
                    "venue": code,
                    "phase": calendar.phase_at(now).value,
                    "next_open": calendar.next_open(now),
                }
            )
        except CalendarOutOfRange:
            rows.append({"venue": code, "phase": "calendar_expired", "next_open": None})
    configured: Sequence[str] = ()
    config_error: dict[str, str] = {}
    try:
        configured = Settings.load().providers
    except ConfigError as exc:
        config_error = {"config_error": f"{exc}; run `ismet doctor`"}
    _print(
        {
            "bin": _bin_path(),
            "description": DESCRIPTION,
            "version": _version(),
            "providers_configured": (
                "unknown" if config_error else ",".join(configured) or "none"
            ),
            **config_error,
            "venues": rows,
            "help": [
                "Run `ismet quote <TICKER@MIC>` for a quote",
                "Run `ismet calendar <MIC>` for a venue's sessions",
                "Run `ismet doctor` to check configuration",
                "Run `ismet kill status --journal <path>` for the kill switch",
            ],
        }
    )
    return EXIT_FAILED if config_error else EXIT_OK


async def _with_client(call: Callable[[IsmetClient], Awaitable[int]]) -> int:
    async with IsmetClient.from_env() as client:
        return await call(client)


async def _quote(args: argparse.Namespace, client: IsmetClient) -> int:
    quote = await client.quote(args.symbol, venue=args.venue, provider=args.provider)
    _print(
        {
            "quote": {
                "symbol": str(quote.symbol),
                "bid": quote.bid,
                "ask": quote.ask,
                "last": quote.last,
                "state": quote.book_state.value,
                "exchange_ts": quote.exchange_ts,
                "flags": ",".join(sorted(f.value for f in quote.flags)) or "none",
            }
        }
    )
    return EXIT_OK


async def _book(args: argparse.Namespace, client: IsmetClient) -> int:
    book = await client.order_book(
        args.symbol, venue=args.venue, provider=args.provider, depth=args.depth
    )
    _print(
        {
            "symbol": str(book.symbol),
            "state": book.book_state.value,
            "exchange_ts": book.exchange_ts,
            "bids": [{"price": x.price, "size": x.size} for x in book.bids],
            "asks": [{"price": x.price, "size": x.size} for x in book.asks],
        }
    )
    return EXIT_OK


async def _bars(args: argparse.Namespace, client: IsmetClient) -> int:
    now = _now()
    start = as_datetime(args.start, now - timedelta(days=30))
    end = as_datetime(args.end, now)
    bars = await client.bars(
        args.symbol,
        interval=args.interval,
        start=start,
        end=end,
        venue=args.venue,
        provider=args.provider,
    )
    if not bars:
        _print({"bars": f"0 bars for {args.symbol} in that range"})
        return EXIT_OK
    shown = bars if args.full else bars[-_MAX_ROWS:]
    payload: dict[str, Any] = {
        "count": f"{len(shown)} of {len(bars)} total",
        "bars": [
            {
                "ts": bar.exchange_ts,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "flags": ",".join(sorted(f.value for f in bar.flags)) or "none",
                "adjustment": bar.adjustment.value,
            }
            for bar in shown
        ],
    }
    if len(shown) < len(bars):
        words = ["ismet", "bars", args.symbol]
        if args.venue:
            words += ["--venue", args.venue]
        if args.provider:
            words += ["--provider", args.provider]
        words += ["--interval", args.interval]
        words += ["--start", start.isoformat(), "--end", end.isoformat(), "--full"]
        payload["help"] = [f"Run `{shlex.join(words)}` for all rows"]
    _print(payload)
    return EXIT_OK


def _calendar(args: argparse.Namespace) -> int:
    if args.venue is None:
        names = available_calendars()
        _print({"count": len(names), "calendars": names})
        return EXIT_OK
    calendar = get_calendar(args.venue)
    now = _now()
    if args.next_open:
        _print({"venue": calendar.venue, "next_open": calendar.next_open(now)})
        return EXIT_OK
    if args.on:
        day = date.fromisoformat(args.on)
        session = calendar.session(day)
        if session is None:
            _print({"venue": calendar.venue, "date": day, "session": "closed"})
            return EXIT_OK
        _print(
            {
                "venue": calendar.venue,
                "date": day,
                "open": session.open,
                "close": session.close,
                "break_start": session.break_start,
                "break_end": session.break_end,
                "early_close": session.early_close,
            }
        )
        return EXIT_OK
    _print(
        {
            "venue": calendar.venue,
            "timezone": calendar.tz.key,
            "phase": calendar.phase_at(now).value,
            "next_open": calendar.next_open(now),
            "next_close": calendar.next_close(now),
            "data_covers": f"{calendar.first_date} to {calendar.last_date}",
            "help": [f"Run `ismet calendar {calendar.venue} --on <YYYY-MM-DD>`"],
        }
    )
    return EXIT_OK


def _providers(args: argparse.Namespace) -> int:
    registry = ProviderRegistry.discover()
    if args.name is not None:
        provider = registry.get(args.name)
        _print(
            {
                "name": args.name.lower(),
                "venues": " ".join(sorted(provider.venues)),
                "capabilities": " ".join(
                    sorted(c.value for c in capabilities_of(provider))
                ),
            }
        )
        return EXIT_OK
    names = registry.names()
    if not names:
        _print({"providers": "0 providers installed"})
        return EXIT_OK
    _print(
        {
            "count": len(names),
            "providers": names,
            "help": ["Run `ismet providers <name>` for its venues and capabilities"],
        }
    )
    return EXIT_OK


def _doctor() -> int:
    checks: list[dict[str, Any]] = []
    problems = 0
    try:
        settings = Settings.load()
        checks.append({"check": "config", "status": "ok", "detail": "loaded"})
    except ConfigError as exc:
        settings = None
        problems += 1
        checks.append({"check": "config", "status": "fail", "detail": str(exc)})
    registry = ProviderRegistry.discover()
    configured: Sequence[str] = settings.providers if settings else ()
    for name in configured:
        try:
            registry.get(name)
        except ConfigError as exc:
            problems += 1
            checks.append(
                {"check": f"provider {name}", "status": "fail", "detail": str(exc)}
            )
            continue
        assert settings is not None
        credentials = sorted(settings.for_provider(name).credentials)
        detail = f"credentials set: {','.join(credentials) or 'none'}"
        checks.append({"check": f"provider {name}", "status": "ok", "detail": detail})
    for extra in ("pandas", "polars"):
        found = importlib.util.find_spec(extra) is not None
        checks.append(
            {
                "check": f"extra {extra}",
                "status": "ok" if found else "absent",
                "detail": "installed" if found else f"pip install ismet[{extra}]",
            }
        )
    today = _now().date()
    for code in available_calendars():
        calendar = get_calendar(code)
        remaining = (calendar.last_date - today).days
        if remaining < 90:
            checks.append(
                {
                    "check": f"calendar {code}",
                    "status": "warn",
                    "detail": (
                        f"data ends {calendar.last_date}; update ismet, or extend it "
                        "from the venue's holiday list (docs/OPERATIONS.md)"
                    ),
                }
            )
    payload: dict[str, Any] = {
        "version": _version(),
        "python": platform.python_version(),
        "os": f"{platform.system()} {platform.machine()}",
        "config_file": str(config_file_path()),
        "providers_configured": ",".join(configured) or "none",
        "problems": problems,
        "checks": checks,
    }
    if not configured:
        example = f"{ENV_PREFIX}PROVIDERS=mock"
        payload["help"] = [f"Set {ENV_PREFIX}PROVIDERS, for example {example}"]
    _print(payload)
    return EXIT_FAILED if problems else EXIT_OK


def _reading_journal(path: str) -> SqliteJournal:
    file_path = Path(path)
    if not file_path.exists():
        raise ValidationError(f"no journal at {file_path}")
    return SqliteJournal.open_read_only(file_path)


def _existing_journal(path: str) -> SqliteJournal:
    _reading_journal(path).close()
    return SqliteJournal(path)


def _sync_directory(directory: Path) -> None:
    """Make a new name in ``directory`` survive a power cut. Windows cannot do this."""
    if os.name == "nt":
        return
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _take_name(temporary: Path, out: Path) -> None:
    """Give the finished file its name, refusing one that already exists."""
    try:
        os.link(temporary, out)
    except FileExistsError:
        raise
    except OSError:
        # This filesystem has no hard links. Claim the name, then rename over the claim,
        # so a crash leaves an empty file rather than a truncated audit that verifies.
        os.close(os.open(out, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
        try:
            temporary.replace(out)
        except BaseException:
            out.unlink(missing_ok=True)
            raise


def _export(journal: SqliteJournal, out: Path) -> tuple[int, str | None]:
    """Write the export beside its destination, so a failure leaves nothing behind.

    The file is on disk before it takes its name, and taking the name refuses a file
    that appeared in the meantime, so an existing audit file is never replaced. Returns
    the count and a warning if the name itself could not be made durable: the complete
    file is there, so that is reported rather than undone.
    """
    if out.exists():
        raise ValidationError(f"{out} already exists; choose a new --out")
    descriptor, name = tempfile.mkstemp(dir=out.parent, prefix=f".{out.name}.")
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            count = export_jsonl(journal, stream)
            stream.flush()
            os.fsync(stream.fileno())
        _take_name(temporary, out)
    except FileExistsError:
        raise ValidationError(f"{out} already exists; choose a new --out") from None
    finally:
        temporary.unlink(missing_ok=True)
    try:
        _sync_directory(out.parent)
    except OSError as exc:
        return (
            count,
            f"{out.parent} could not be synced, so a power cut may lose "
            f"{out.name}: {exc}",
        )
    return count, None


def _chain_text(result: VerifyResult) -> str:
    if result.ok:
        return "intact"
    return f"broken at record {result.first_bad_seq}: {result.detail}"


def _journal(args: argparse.Namespace) -> int:
    if args.action is None:
        raise _UsageError("journal needs an action", "ismet journal --help")
    if args.action == "verify" and args.expect_head == "":
        raise _UsageError(
            "--expect-head is empty; was the variable holding it set?",
            "ismet journal --help",
        )
    journal = _reading_journal(args.path)
    try:
        if args.action == "verify":
            result = journal.verify()
            # A broken chain is the stronger answer, and it names the bad record.
            if (
                result.ok
                and args.expect_head is not None
                and not chain_contains(journal, args.expect_head)
            ):
                _print(
                    {
                        "ok": False,
                        "records": result.records,
                        "head_hash": result.head_hash,
                        "detail": "expected head not found: truncated or replaced",
                    }
                )
                return EXIT_FAILED
            _print(
                {
                    "ok": result.ok,
                    "records": result.records,
                    "head_hash": result.head_hash,
                    "first_bad_seq": result.first_bad_seq,
                    "detail": result.detail or "chain intact",
                    "help": [
                        "Copy head_hash somewhere else to detect later truncation"
                    ],
                }
            )
            return EXIT_OK if result.ok else EXIT_FAILED
        chain = journal.verify()
        try:
            _journal_contents(args, journal, chain)
        except (IsmetError, ValueError, OSError) as exc:
            # The chain was worked out first, and it is the answer that matters most.
            _print(
                {
                    "error": _error_text(exc),
                    "chain": _chain_text(chain),
                    "help": _JOURNAL_HINT if not chain.ok else _error_hint(exc),
                }
            )
            return EXIT_FAILED
        return EXIT_OK if chain.ok else EXIT_FAILED
    finally:
        journal.close()


def _journal_contents(
    args: argparse.Namespace, journal: SqliteJournal, chain: VerifyResult
) -> None:
    if args.action == "export":
        count, warning = _export(journal, Path(args.out))
        _print(
            {
                "exported": count,
                "path": args.out,
                "chain": _chain_text(chain),
                **({} if warning is None else {"warning": warning}),
            }
        )
        return
    with accepting_stored_codes():
        orders = replay(events_of(journal))
    states = [o for o in orders.values() if args.all or o.is_open]
    if not states:
        scope = "" if args.all else "open "
        _print(
            {
                "orders": f"0 {scope}orders of {len(orders)} in this journal",
                "chain": _chain_text(chain),
            }
        )
        return
    _print(
        {
            "chain": _chain_text(chain),
            "count": f"{len(states)} of {len(orders)} total",
            "orders": [
                {
                    "id": o.client_order_id,
                    "symbol": str(o.request.symbol),
                    "side": o.request.side,
                    "status": o.status,
                    "filled": f"{o.filled_quantity}/{o.quantity}",
                }
                for o in states
            ],
        }
    )


def _require_named(args: argparse.Namespace, help_text: str) -> None:
    """An unset shell variable must not pass for an operator's name or reason."""
    for flag in ("reason", "operator"):
        if not getattr(args, flag).strip():
            raise _UsageError(
                f"--{flag} is blank; was the variable holding it set?", help_text
            )


def _require_intact(result: VerifyResult, refused: str) -> None:
    """A running manager ignores this write on a broken chain, so do not pretend."""
    if not result.ok:
        raise JournalError(
            f"{refused}: journal failed verification at record "
            f"{result.first_bad_seq}: {result.detail}"
        )


def _kill(args: argparse.Namespace) -> int:
    if args.action is None:
        raise _UsageError("kill needs an action", "ismet kill --help")
    # Engaging is exempt: a missing name must never stop a halt.
    if args.action == "release":
        _require_named(args, "ismet kill --help")
    creating = (
        args.action == "engage" and args.create and not Path(args.journal).exists()
    )
    # A mistyped path must not look like a successful halt while trading continues.
    journal: SqliteJournal
    if creating:
        journal = SqliteJournal(args.journal)
    elif args.action == "status":
        journal = _reading_journal(args.journal)
    else:
        journal = _existing_journal(args.journal)
    try:
        # Engaging reports the chain only after the halt is durable: the evidence of a
        # broken chain survives the append, so `kill release` still refuses on it.
        chain = None if args.action == "engage" else journal.verify()
        if args.action == "release" and chain is not None:
            _require_intact(chain, "kill switch not released")
        state = journal.kill_state()
        if args.action == "engage":
            wants_cancel = not args.keep_orders
            if state.engaged and (state.cancel_open or not wants_cancel):
                _print(
                    {
                        "kill": "already engaged (no-op)",
                        "reason": state.reason,
                        "chain": _lenient_chain_text(journal),
                    }
                )
                return EXIT_OK
            try:
                journal.append(
                    RecordKind.KILL_ENGAGED,
                    json.dumps(
                        {
                            "reason": args.reason,
                            "operator": args.operator,
                            "cancel_open": wants_cancel,
                        }
                    ),
                )
            except (JournalError, OSError) as exc:
                _print(
                    {
                        "error": _error_text(exc),
                        "help": "halt NOT recorded; cancel at the venue directly",
                    }
                )
                return EXIT_FAILED
            state = journal.kill_state()
        elif args.action == "release":
            if not state.engaged:
                _print({"kill": "not engaged (no-op)"})
                return EXIT_OK
            journal.append(
                RecordKind.KILL_RELEASED,
                json.dumps({"reason": args.reason, "operator": args.operator}),
            )
            state = journal.kill_state()
        _print(
            {
                "kill": "engaged" if state.engaged else "not engaged",
                "reason": state.reason or "none",
                "operator": state.operator or "none",
                "cancel_open": state.cancel_open,
                "chain": (
                    _lenient_chain_text(journal)
                    if chain is None
                    else _chain_text(chain)
                ),
            }
        )
        return EXIT_OK if chain is None or chain.ok else EXIT_FAILED
    finally:
        journal.close()


def _lenient_chain_text(journal: SqliteJournal) -> str:
    """The chain check as a diagnostic: nothing here may stop an emergency halt."""
    try:
        return _chain_text(journal.verify())
    except Exception as exc:
        return f"could not be checked: {_error_text(exc)}"


def _journal_names_venue(journal: SqliteJournal, venue: str) -> bool:
    """Whether a baseline, an adjustment or an order in the journal is on ``venue``."""
    for record in journal.records():
        if record.kind is RecordKind.POSITION_BASELINE:
            symbols = list(json.loads(record.payload))
        elif record.kind is RecordKind.POSITION_ADJUSTED:
            symbols = [json.loads(record.payload)["symbol"]]
        else:
            continue
        if any(text.rpartition("@")[2].upper() == venue for text in symbols):
            return True
    with accepting_stored_codes():
        return any(
            isinstance(event, SubmitIntent) and event.request.symbol.venue == venue
            for event in events_of(journal)
        )


def _position(args: argparse.Namespace) -> int:
    if args.action is None:
        raise _UsageError("position needs an action", "ismet position --help")
    _require_named(args, "ismet position --help")
    # A venue may have been registered only by the process that traded it. It is
    # checked against the journal below, once the journal is open.
    with accepting_stored_codes():
        symbol = Symbol.parse(args.symbol)
    try:
        delta = to_decimal(args.delta)
    except ValueError as exc:
        raise _UsageError(f"--delta: {exc}", "ismet position --help") from None
    journal = _existing_journal(args.journal)
    try:
        _require_intact(journal.verify(), "position not adjusted")
        if not any(r.kind is RecordKind.POSITION_BASELINE for r in journal.records()):
            raise ConfigError(
                "this journal has no position baseline, so a running manager would "
                "ignore the adjustment"
            )
        if not is_known_venue(symbol.venue) and not _journal_names_venue(
            journal, symbol.venue
        ):
            # A record cannot be taken back, and a manager could not parse this symbol.
            raise ValidationError(
                f"venue {symbol.venue} is not a known MIC and nothing in this journal "
                "is on it, so this looks like a typing mistake; check the symbol, or "
                "use OrderManager.adjust_position in a process that registered it"
            )
        record = journal.append(
            RecordKind.POSITION_ADJUSTED,
            json.dumps(
                {
                    "symbol": str(symbol),
                    "delta": str(delta),
                    "operator": args.operator,
                    "reason": args.reason,
                }
            ),
        )
    finally:
        journal.close()
    _print(
        {
            "adjusted": str(symbol),
            "delta": str(delta),
            "record": record.seq,
            "help": ["The next reconciliation compares against the new position"],
        }
    )
    return EXIT_OK


def _dispatch(args: argparse.Namespace) -> int:
    command = args.command
    if command == "calendar":
        return _calendar(args)
    if command == "providers":
        return _providers(args)
    if command == "doctor":
        return _doctor()
    if command == "journal":
        return _journal(args)
    if command == "kill":
        return _kill(args)
    if command == "position":
        return _position(args)
    handler = {"quote": _quote, "book": _book, "bars": _bars}[command]
    return asyncio.run(_with_client(lambda client: handler(args, client)))


_JOURNAL_HINT = (
    "Check the path. If it is the journal, treat it as tampered with: preserve it and "
    "its .head file, as in docs/OPERATIONS.md"
)


def _error_text(exc: Exception) -> str:
    if isinstance(exc, PydanticValidationError):
        return "; ".join(e["msg"] for e in exc.errors())
    return str(exc)


def _error_hint(exc: Exception) -> str:
    if isinstance(exc, JournalError):
        return _JOURNAL_HINT
    if isinstance(exc, ConfigError):
        return f"Set {ENV_PREFIX}PROVIDERS=mock to try it, or run `ismet doctor`"
    if isinstance(exc, AuthError):
        return "Check the provider's credentials with `ismet doctor`"
    if isinstance(exc, NotSupported):
        return "Run `ismet providers <name>` to see a provider's capabilities"
    if isinstance(exc, CalendarOutOfRange):
        return "Update ismet for newer calendar data"
    return "Run `ismet doctor` to check configuration"


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point. Returns the process exit code."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments:
        return _home()
    try:
        args = _build_parser().parse_args(arguments)
        if args.version:
            print(_version())
            return EXIT_OK
        if args.command is None:
            return _home()
        return _dispatch(args)
    except _UsageError as exc:
        scope = next(
            (a for a in arguments if not a.startswith("-") and a in _COMMANDS), None
        )
        help_text = f"ismet {scope} --help" if scope else exc.help_text
        _print({"error": str(exc), "help": help_text})
        return EXIT_USAGE
    except (IsmetError, ValueError, OSError) as exc:
        _print({"error": _error_text(exc), "help": _error_hint(exc)})
        return EXIT_FAILED
    except Exception as exc:
        traceback.print_exc(file=sys.stderr)
        _print({"error": f"internal error: {type(exc).__name__}", "help": "See stderr"})
        return EXIT_FAILED


__all__ = ["main"]
