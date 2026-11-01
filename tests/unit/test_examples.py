"""Every published example runs, offline, against what the package ships."""

from __future__ import annotations

import re
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

EXAMPLES = sorted((Path(__file__).parents[2] / "examples").glob("*.py"))


def test_examples_exist() -> None:
    assert len(EXAMPLES) >= 3


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.stem)
def test_example_runs(path: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(path)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def _output_of(stem: str) -> str:
    path = next(p for p in EXAMPLES if p.stem == stem)
    return subprocess.run(
        [sys.executable, str(path)],
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    ).stdout


def test_quickstart_shows_real_data() -> None:
    quote, bars, opens = _output_of("quickstart_market_data").splitlines()
    parsed = re.fullmatch(r"ACME@XMOK bid (\S+) ask (\S+) \((\S+)\)", quote)
    assert parsed is not None
    assert Decimal(parsed[1]) < Decimal(parsed[2])
    assert parsed[3] == "normal"
    # Thirty one-minute intervals hold thirty bars.
    count, close = re.fullmatch(r"(\d+) bars, last close (\S+)", bars).groups()  # type: ignore[union-attr]
    assert int(count) == 30 and Decimal(close) > 0
    # After Saturday 2026-03-07, NYSE next opens Monday 09:30 New York, on EDT by then.
    moment = datetime.fromisoformat(opens.removeprefix("NYSE next opens at "))
    assert moment == datetime(2026, 3, 9, 13, 30, tzinfo=timezone.utc)


def test_sync_client_shows_real_data() -> None:
    bars, close, quote = _output_of("sync_client").splitlines()
    assert bars == "24 bars"  # one-hour bars across one UTC day
    assert Decimal(close) > 0
    bid, ask = quote.split()
    assert Decimal(bid) < Decimal(ask)


def test_safe_order_path_shows_what_it_claims() -> None:
    output = _output_of("safe_order_path")
    assert "refused before sending: price_collar" in output
    assert "filled at 100.01" in output
    assert "status is unknown" in output
    assert "after asking the venue: accepted" in output
    assert "times sent: 1" in output
    assert "cancelled by the switch: canceled" in output
    assert "position from fills: 10" in output
    assert "journal chain intact: True" in output
