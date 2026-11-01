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
import platform
import sys
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
from ismet.config import ENV_PREFIX, Settings, default_config_path
from ismet.errors import (
    AuthError,
    CalendarOutOfRange,
    ConfigError,
    IsmetError,
    NotSupported,
    ValidationError,
)
from ismet.execution.journal import RecordKind, SqliteJournal, export_jsonl
from ismet.execution.manager import events_of
from ismet.execution.state import replay
from ismet.providers import ProviderRegistry

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
)
Out = Mapping[str, Any]


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

    command("providers", "List installed providers and their capabilities")
    command("doctor", "Check the installation and configuration")

    journal = command("journal", "Inspect an order journal")
    journal_commands = journal.add_subparsers(dest="action", parser_class=_Parser)
    for name, summary in (
        ("verify", "Verify the journal's hash chain"),
        ("orders", "Rebuild order state from the journal"),
        ("export", "Write the journal as JSON lines"),
    ):
        action = journal_commands.add_parser(name, help=summary, description=summary)
        action.add_argument("path", help="journal file")
        if name == "export":
            action.add_argument("--out", required=True, help="output file")
        if name == "orders":
            action.add_argument("--all", action="store_true", help="include closed")

    kill = command("kill", "Read or set the kill switch in a journal")
    kill_commands = kill.add_subparsers(dest="action", parser_class=_Parser)
    for name, summary in (
        ("status", "Show the kill switch"),
        ("engage", "Stop new orders in every process using this journal"),
        ("release", "Allow new orders again"),
    ):
        action = kill_commands.add_parser(name, help=summary, description=summary)
        action.add_argument("--journal", required=True, help="journal file")
        if name != "status":
            action.add_argument("--reason", required=True, help="recorded in the audit")
            action.add_argument("--operator", required=True, help="who is doing this")
        if name == "engage":
            action.add_argument(
                "--keep-orders",
                action="store_true",
                help="stop new orders but leave open orders working",
            )
    return parser


# Commands


def _home() -> int:
    now = datetime.now(timezone.utc)
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
    configured: Sequence[str]
    try:
        configured = Settings.load().providers
    except ConfigError:
        configured = ()
    _print(
        {
            "bin": _bin_path(),
            "description": DESCRIPTION,
            "version": _version(),
            "providers_configured": ",".join(configured) or "none",
            "venues": rows,
            "help": [
                "Run `ismet quote <TICKER@MIC>` for a quote",
                "Run `ismet calendar <MIC>` for a venue's sessions",
                "Run `ismet doctor` to check configuration",
                "Run `ismet kill status --journal <path>` for the kill switch",
            ],
        }
    )
    return EXIT_OK


async def _with_client(call: Callable[[IsmetClient], Awaitable[int]]) -> int:
    async with IsmetClient.from_env() as client:
        return await call(client)


def _parse_time(text: str | None, default: datetime) -> datetime:
    if text is None:
        return default
    value = datetime.fromisoformat(text)
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


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
    now = datetime.now(timezone.utc)
    bars = await client.bars(
        args.symbol,
        interval=args.interval,
        start=_parse_time(args.start, now - timedelta(days=30)),
        end=_parse_time(args.end, now),
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
            }
            for bar in shown
        ],
    }
    if len(shown) < len(bars):
        payload["help"] = [f"Run `ismet bars {args.symbol} --full` for all rows"]
    _print(payload)
    return EXIT_OK


def _calendar(args: argparse.Namespace) -> int:
    if args.venue is None:
        names = available_calendars()
        _print({"count": len(names), "calendars": names})
        return EXIT_OK
    calendar = get_calendar(args.venue)
    now = datetime.now(timezone.utc)
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


def _providers() -> int:
    registry = ProviderRegistry.discover()
    rows = []
    for name in registry.names():
        provider = registry.get(name)
        capabilities = sorted(c.value for c in capabilities_of(provider))
        rows.append(
            {
                "name": name,
                "venues": " ".join(sorted(provider.venues)),
                "capabilities": " ".join(capabilities),
            }
        )
    if not rows:
        _print({"providers": "0 providers installed"})
        return EXIT_OK
    _print({"count": len(rows), "providers": rows})
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
    today = datetime.now(timezone.utc).date()
    for code in available_calendars():
        calendar = get_calendar(code)
        remaining = (calendar.last_date - today).days
        if remaining < 90:
            checks.append(
                {
                    "check": f"calendar {code}",
                    "status": "warn",
                    "detail": f"data ends {calendar.last_date}; update ismet",
                }
            )
    _print(
        {
            "version": _version(),
            "python": platform.python_version(),
            "os": f"{platform.system()} {platform.machine()}",
            "config_file": str(default_config_path()),
            "providers_configured": ",".join(configured) or "none",
            "problems": problems,
            "checks": checks,
        }
    )
    if not configured:
        example = f"{ENV_PREFIX}PROVIDERS=mock"
        print(f"help[1]:\n  Set {ENV_PREFIX}PROVIDERS, for example {example}")
    return EXIT_FAILED if problems else EXIT_OK


def _existing_journal(path: str) -> SqliteJournal:
    file_path = Path(path)
    if not file_path.exists():
        raise ValidationError(f"no journal at {file_path}")
    return SqliteJournal(file_path)


def _journal(args: argparse.Namespace) -> int:
    if args.action is None:
        raise _UsageError("journal needs an action", "ismet journal --help")
    journal = _existing_journal(args.path)
    try:
        if args.action == "verify":
            result = journal.verify()
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
        if args.action == "export":
            with Path(args.out).open("w", encoding="utf-8") as stream:
                count = export_jsonl(journal, stream)
            _print({"exported": count, "path": args.out})
            return EXIT_OK
        orders = replay(events_of(journal))
        states = [o for o in orders.values() if args.all or o.is_open]
        if not states:
            scope = "" if args.all else "open "
            _print({"orders": f"0 {scope}orders of {len(orders)} in this journal"})
            return EXIT_OK
        _print(
            {
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
        return EXIT_OK
    finally:
        journal.close()


def _kill(args: argparse.Namespace) -> int:
    if args.action is None:
        raise _UsageError("kill needs an action", "ismet kill --help")
    if args.action == "status":
        journal = _existing_journal(args.journal)
    else:
        journal = SqliteJournal(args.journal)
    try:
        state = journal.kill_state()
        if args.action == "engage":
            if state.engaged:
                _print({"kill": "already engaged (no-op)", "reason": state.reason})
                return EXIT_OK
            journal.append(
                RecordKind.KILL_ENGAGED,
                json.dumps(
                    {
                        "reason": args.reason,
                        "operator": args.operator,
                        "cancel_open": not args.keep_orders,
                    }
                ),
            )
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
            }
        )
        return EXIT_OK
    finally:
        journal.close()


def _dispatch(args: argparse.Namespace) -> int:
    command = args.command
    if command == "calendar":
        return _calendar(args)
    if command == "providers":
        return _providers()
    if command == "doctor":
        return _doctor()
    if command == "journal":
        return _journal(args)
    if command == "kill":
        return _kill(args)
    handler = {"quote": _quote, "book": _book, "bars": _bars}[command]
    return asyncio.run(_with_client(lambda client: handler(args, client)))


def _error_hint(exc: Exception) -> str:
    if isinstance(exc, ConfigError):
        return f"Set {ENV_PREFIX}PROVIDERS=mock to try it, or run `ismet doctor`"
    if isinstance(exc, AuthError):
        return "Check the provider's credentials with `ismet doctor`"
    if isinstance(exc, NotSupported):
        return "Run `ismet providers` to see each provider's capabilities"
    if isinstance(exc, CalendarOutOfRange):
        return "Update ismet for newer calendar data"
    return "Run `ismet doctor` to check configuration"


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point. Returns the process exit code."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments in (["-v"], ["-V"], ["--version"]):
        print(_version())
        return EXIT_OK
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
        message = (
            "; ".join(e["msg"] for e in exc.errors())
            if isinstance(exc, PydanticValidationError)
            else str(exc)
        )
        _print({"error": message, "help": _error_hint(exc)})
        return EXIT_FAILED
    except Exception as exc:
        traceback.print_exc(file=sys.stderr)
        _print({"error": f"internal error: {type(exc).__name__}", "help": "See stderr"})
        return EXIT_FAILED


__all__ = ["main"]
