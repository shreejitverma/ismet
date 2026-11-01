"""Every published example runs, offline, against what the package ships."""

from __future__ import annotations

import subprocess
import sys
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


def test_safe_order_path_shows_what_it_claims() -> None:
    path = next(p for p in EXAMPLES if p.stem == "safe_order_path")
    output = subprocess.run(
        [sys.executable, str(path)],
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    ).stdout
    assert "refused before sending: price_collar" in output
    assert "filled at 100.01" in output
    assert "status is unknown" in output
    assert "after asking the venue: accepted" in output
    assert "times sent: 1" in output
    assert "cancelled by the switch: canceled" in output
    assert "position from fills: 10" in output
    assert "journal chain intact: True" in output
