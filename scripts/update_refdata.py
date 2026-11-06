#!/usr/bin/env python3
"""Regenerate ismet's bundled ISO reference snapshots from their publishers.

Usage: ``uv run python scripts/update_refdata.py``

Sources:
- ISO 10383 MIC: https://www.iso20022.org/market-identifier-codes
- ISO 4217 list one: https://www.six-group.com/en/products-services/financial-information/data-standards.html

The script keeps only the columns ismet reads, sorted, so a refresh produces a
small reviewable diff. It needs network access and is never run at import time.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

MIC_URL = "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"
CCY_URL = (
    "https://www.six-group.com/dam/download/financial-information/data-center/"
    "iso-currrency/lists/list-one.xml"
)
DATA = Path(__file__).resolve().parent.parent / "src" / "ismet" / "refdata" / "data"


def fetch(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "ismet-refdata"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def update_mics(today: str) -> int:
    text = fetch(MIC_URL).decode("utf-8-sig")
    rows = sorted(csv.DictReader(io.StringIO(text)), key=lambda r: r["MIC"])
    out = io.StringIO(newline="")
    out.write(f"# ISO 10383 snapshot retrieved {today} from {MIC_URL}\n")
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(["mic", "operating_mic", "status", "country", "name"])
    for row in rows:
        writer.writerow(
            [
                row["MIC"],
                row["OPERATING MIC"],
                row["STATUS"],
                row["ISO COUNTRY CODE (ISO 3166)"],
                row["MARKET NAME-INSTITUTION DESCRIPTION"].strip(),
            ]
        )
    (DATA / "mics.csv").write_text(out.getvalue(), encoding="utf-8", newline="\n")
    return len(rows)


def update_currencies(today: str) -> int:
    root = ET.fromstring(fetch(CCY_URL))
    published = root.get("Pblshd", "unknown")
    table: dict[str, tuple[str, str]] = {}
    for entry in root.iter("CcyNtry"):
        code = entry.findtext("Ccy")
        if not code:
            continue
        minor = entry.findtext("CcyMnrUnts") or "N.A."
        table[code] = (minor, (entry.findtext("CcyNm") or "").strip())
    out = io.StringIO(newline="")
    out.write(f"# ISO 4217 list one published {published}, retrieved {today}\n")
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(["code", "minor_units", "name"])
    for code in sorted(table):
        writer.writerow([code, *table[code]])
    (DATA / "currencies.csv").write_text(out.getvalue(), encoding="utf-8", newline="\n")
    return len(table)


def main() -> None:
    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    DATA.mkdir(parents=True, exist_ok=True)
    print(f"mics: {update_mics(today)}")
    print(f"currencies: {update_currencies(today)}")


if __name__ == "__main__":
    main()
