#!/usr/bin/env python3
"""Run the committed mutant catalogue against a throwaway copy of the repository.

Each mutant in ``mutation/mutants.toml`` replaces one exact snippet in one source
file with a plausible bug, and names the tests that must fail when it is applied. A
mutant those tests do not fail survives: the risk it stands for is not covered.
The working tree is never touched; every mutant runs in a temporary copy.

    uv run --python 3.10 --extra dev python scripts/mutation_check.py   # every mutant
    uv run --python 3.10 --extra dev python scripts/mutation_check.py --only ID ...
    uv run --python 3.10 --extra dev python scripts/mutation_check.py --list

A mutant with ``before_python = "3.12"`` can only be killed on an older interpreter.
On that version or later it is not run and is reported as not applicable, counting
as neither killed nor survived; on Python 3.12 or later one mutant does not apply.

Exit status: 0 when every mutant is killed, or survives with a recorded
``equivalent`` reason; 1 when one survives without one, or an ``equivalent`` mutant
is now killed so its reason is stale; 2 when the catalogue does not apply or the
unmutated tests fail, since then no mutant can be judged.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - exercised on 3.10 only
    import tomli as tomllib

ROOT = Path(__file__).resolve().parent.parent
CATALOGUE = ROOT / "mutation" / "mutants.toml"
CATALOGUE_TEST = "tests/unit/test_mutation_catalogue.py"
TIMEOUT_SECONDS = 900
IGNORED = (".git", ".venv", "dist", "build", "__pycache__", "*.egg-info", ".*_cache")


@dataclass(frozen=True)
class Mutant:
    id: str
    file: str
    find: str
    replace: str
    risk: str
    tests: tuple[str, ...]
    equivalent: str | None = None
    before_python: str | None = None


def load(path: Path = CATALOGUE) -> list[Mutant]:
    with path.open("rb") as handle:
        data = tomllib.load(handle)
    mutants = [
        Mutant(
            id=entry["id"],
            file=entry["file"],
            find=entry["find"],
            replace=entry["replace"],
            risk=entry["risk"],
            tests=tuple(entry["tests"]),
            equivalent=entry.get("equivalent"),
            before_python=entry.get("before_python"),
        )
        for entry in data["mutant"]
    ]
    ids = [m.id for m in mutants]
    duplicates = sorted({i for i in ids if ids.count(i) > 1})
    if duplicates:
        raise ValueError(f"duplicate mutant ids: {duplicates}")
    return mutants


def python_version(text: object) -> tuple[int, int]:
    """``"3.12"`` as ``(3, 12)``; ValueError for anything that is not MAJOR.MINOR."""
    if not isinstance(text, str) or not re.fullmatch(r"\d+\.\d+", text):
        raise ValueError(f"{text!r} is not a Python version like '3.12'")
    major, minor = text.split(".")
    return int(major), int(minor)


def applies_here(m: Mutant, version: tuple[int, int] = sys.version_info[:2]) -> bool:
    """Whether ``m`` can be killed on Python ``version``."""
    return m.before_python is None or version < python_version(m.before_python)


def problems(mutants: list[Mutant], root: Path = ROOT) -> list[str]:
    """Why a mutant cannot be applied to the current source, one line each."""
    found = []
    for m in mutants:
        source = root / m.file
        if not source.is_file():
            found.append(f"{m.id}: {m.file} does not exist")
            continue
        text = source.read_text(encoding="utf-8")
        count = text.count(m.find)
        if count != 1:
            found.append(f"{m.id}: snippet occurs {count} times in {m.file}, not once")
        elif m.file.endswith(".py"):
            try:
                compile(text.replace(m.find, m.replace, 1), m.file, "exec")
            except SyntaxError as exc:
                # A mutant that cannot import fails every test for the wrong reason.
                found.append(f"{m.id}: the mutated file does not compile: {exc.msg}")
        if m.before_python is not None:
            try:
                python_version(m.before_python)
            except ValueError as exc:
                found.append(f"{m.id}: before_python {exc}")
        if m.find == m.replace:
            found.append(f"{m.id}: replace is the same as find")
        if not m.tests:
            found.append(f"{m.id}: names no tests")
        for test in m.tests:
            if not (root / test.split("::")[0]).exists():
                found.append(f"{m.id}: test path {test} does not exist")
    return found


def environment(copy: Path) -> dict[str, str]:
    return {
        **os.environ,
        "PYTHONPATH": str(copy / "src"),
        "PYTHONDONTWRITEBYTECODE": "1",
    }


def run_tests(copy: Path, tests: tuple[str, ...]) -> tuple[bool, float, str]:
    """Whether the tests pass in ``copy``, how long they took, and the last line."""
    command = [
        sys.executable, "-B", "-m", "pytest", "-x", "-q",
        "-p", "no:cacheprovider", "--no-cov",
        # It checks the catalogue against the source, so it fails for every mutant
        # and would make each one look killed.
        f"--ignore={CATALOGUE_TEST}", *tests,
    ]  # fmt: skip
    started = time.monotonic()
    try:
        done = subprocess.run(
            command,
            cwd=copy,
            env=environment(copy),
            capture_output=True,
            text=True,
            timeout=TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, time.monotonic() - started, f"timed out after {TIMEOUT_SECONDS}s"
    # pytest reports on stdout; a run that cannot start, such as one without
    # pytest installed, says why only on stderr.
    lines = [line for line in done.stdout.splitlines() if line.strip()]
    lines = lines or [line for line in done.stderr.splitlines() if line.strip()]
    return done.returncode == 0, time.monotonic() - started, lines[-1] if lines else ""


def imports_the_copy(copy: Path) -> bool:
    found = subprocess.run(
        [sys.executable, "-B", "-c", "import ismet; print(ismet.__file__)"],
        cwd=copy,
        env=environment(copy),
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    return Path(found).resolve().is_relative_to(copy.resolve())


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--only", nargs="+", metavar="ID", help="run these mutants")
    parser.add_argument("--list", action="store_true", help="list mutants and exit")
    args = parser.parse_args(argv)

    mutants = load()
    if args.only:
        unknown = sorted(set(args.only) - {m.id for m in mutants})
        if unknown:
            print(f"unknown mutant ids: {unknown}", file=sys.stderr)
            return 2
        mutants = [m for m in mutants if m.id in args.only]
    if args.list:
        for m in mutants:
            print(f"{m.id}\t{m.file}\t{m.risk}")
        return 0
    broken = problems(mutants)
    if broken:
        print("catalogue does not apply:", *broken, sep="\n  ", file=sys.stderr)
        return 2
    here = "{}.{}".format(*sys.version_info[:2])
    skipped = [m for m in mutants if not applies_here(m)]
    for m in skipped:
        print(f"{m.id:<44} not applicable on Python {here}: {m.risk}")
    mutants = [m for m in mutants if applies_here(m)]
    if not mutants:
        print(f"\n0 mutants run, {len(skipped)} not applicable on Python {here}")
        return 0

    with tempfile.TemporaryDirectory(prefix="ismet-mutation-") as tmp:
        copy = Path(tmp) / "repo"
        shutil.copytree(ROOT, copy, ignore=shutil.ignore_patterns(*IGNORED))
        if not imports_the_copy(copy):
            print("the copy's tests would import another ismet", file=sys.stderr)
            return 2
        selections = sorted({t for m in mutants for t in m.tests})
        passed, seconds, last = run_tests(copy, tuple(selections))
        if not passed:
            print(f"unmutated tests fail, so nothing can be judged: {last}")
            return 2
        print(f"baseline: {len(selections)} test selections pass ({seconds:.0f}s)")

        survivors, stale = [], []
        for m in mutants:
            target = copy / m.file
            original = target.read_text(encoding="utf-8")
            target.write_text(original.replace(m.find, m.replace, 1), encoding="utf-8")
            try:
                passed, seconds, last = run_tests(copy, m.tests)
            finally:
                target.write_text(original, encoding="utf-8")
            if passed and m.equivalent:
                verdict = "equivalent"
            elif passed:
                verdict = "SURVIVED"
                survivors.append(m)
            elif m.equivalent:
                verdict = "KILLED, equivalent note is stale"
                stale.append(m)
            else:
                verdict = "killed"
            print(f"{m.id:<44} {verdict:<10} {seconds:6.1f}s  {m.risk}")

    killed = len(mutants) - len(survivors) - sum(1 for m in mutants if m.equivalent)
    killed += len(stale)
    print(
        f"\n{killed} of {len(mutants)} mutants killed, {len(survivors)} survived, "
        f"{sum(1 for m in mutants if m.equivalent) - len(stale)} recorded equivalent, "
        f"{len(skipped)} not applicable on Python {here}"
    )
    for m in survivors:
        print(f"survived: {m.id}: {m.risk}")
    for m in stale:
        print(f"stale equivalent note: {m.id}")
    return 1 if survivors or stale else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
