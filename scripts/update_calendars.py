#!/usr/bin/env python3
"""Regenerate the bundled trading calendar snapshot from ``exchange_calendars``.

ismet has no pandas or numpy runtime dependency, so instead of importing
exchange_calendars (Apache-2.0, https://github.com/gerrymanoim/exchange_calendars)
it ships a compact derived snapshot and refuses queries outside the snapshot's
dates. Run this in a throwaway environment:

    uv venv /tmp/ec && VIRTUAL_ENV=/tmp/ec uv pip install exchange_calendars
    /tmp/ec/bin/python scripts/update_calendars.py

Encoding per venue: ``eras`` are regular hours from a date onward, ``closed`` are
weekdays with no session, ``extra`` are weekend sessions, ``special`` are sessions
whose hours differ from their era. tests/differential proves the round trip.
"""

from __future__ import annotations

import datetime as dt
import json
from collections import Counter
from pathlib import Path
from zoneinfo import ZoneInfo

import exchange_calendars as ec
import pandas as pd

VENUES = (
    "XNYS", "XBOM", "XLON", "XTKS", "XHKG", "XSHG", "XETR", "XFRA", "XPAR", "XAMS",
    "XBRU", "XMIL", "XMAD", "XSWX", "XSTO", "XTSE", "XASX", "XKRX", "XSES", "XTAI",
    "XJSE", "BVMF", "XMEX", "XNZE",
)  # fmt: skip
ALIASES = {
    "XNAS": "XNYS",  # NYSE and Nasdaq keep the same regular session and holidays
    "XNSE": "XBOM",  # NSE and BSE publish a common equity trading holiday list
    "XJPX": "XTKS",  # JPX operates the Tokyo Stock Exchange
    "XSHE": "XSHG",  # SSE and SZSE keep the same regular session and holidays
}
START = pd.Timestamp("2007-01-01")
OUT = (
    Path(__file__).resolve().parent.parent
    / "src" / "ismet" / "calendars" / "data" / "calendars.json"
)  # fmt: skip
MIN_ERA_SESSIONS = 15


def hhmm(ts: pd.Timestamp | None, tz: ZoneInfo) -> str | None:
    if ts is None or pd.isna(ts):
        return None
    return ts.to_pydatetime().astimezone(tz).strftime("%H:%M")


def encode(name: str) -> dict[str, object]:
    cal = ec.get_calendar(name)
    tz = ZoneInfo(str(cal.tz))
    breaks_start = cal.break_starts
    breaks_end = cal.break_ends
    rows: list[tuple[str, tuple[str | None, ...]]] = []
    for session in cal.sessions[cal.sessions >= START]:
        hours = (
            hhmm(cal.session_open(session), tz),
            hhmm(cal.session_close(session), tz),
            hhmm(breaks_start.loc[session], tz) if breaks_start is not None else None,
            hhmm(breaks_end.loc[session], tz) if breaks_end is not None else None,
        )
        rows.append((session.strftime("%Y-%m-%d"), hours))

    eras: list[list[object]] = []
    special: dict[str, list[str | None]] = {}
    index = 0
    while index < len(rows):
        window = Counter(h for _, h in rows[index : index + MIN_ERA_SESSIONS])
        regular = window.most_common(1)[0][0]
        if not eras or eras[-1][1:] != list(regular):
            if eras and window[tuple(eras[-1][1:])] * 2 > MIN_ERA_SESSIONS:
                regular = tuple(eras[-1][1:])  # type: ignore[assignment]
            else:
                eras.append([rows[index][0], *regular])
        current = tuple(eras[-1][1:])
        while index < len(rows):
            date, hours = rows[index]
            if hours != current:
                ahead = Counter(h for _, h in rows[index : index + MIN_ERA_SESSIONS])
                if ahead[current] * 2 <= len(rows[index : index + MIN_ERA_SESSIONS]):
                    break
                special[date] = list(hours)
            index += 1

    sessions = {date for date, _ in rows}
    first, last = rows[0][0], rows[-1][0]
    closed = [
        d.strftime("%Y-%m-%d")
        for d in pd.bdate_range(first, last)
        if d.strftime("%Y-%m-%d") not in sessions
    ]
    extra = sorted(d for d in sessions if dt.date.fromisoformat(d).weekday() >= 5)
    return {
        "tz": str(cal.tz),
        "first": first,
        "last": last,
        "eras": eras,
        "closed": closed,
        "extra": extra,
        "special": special,
    }


def main() -> None:
    payload = {
        "source": f"exchange_calendars {ec.__version__} (Apache-2.0)",
        "generated": dt.datetime.now(dt.timezone.utc).date().isoformat(),
        "aliases": ALIASES,
        "venues": {name: encode(name) for name in VENUES},
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, separators=(",", ":")) + "\n", encoding="utf-8")
    print(f"{OUT}: {OUT.stat().st_size} bytes, {len(VENUES)} venues")


if __name__ == "__main__":
    main()
