"""The committed mutant catalogue still applies to the source it mutates.

The mutants themselves run weekly and on demand (scripts/mutation_check.py), since
the whole catalogue takes minutes. This keeps a refactor from silently turning a
mutant into one that no longer applies, which would quietly drop a covered risk.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "mutation_check", ROOT / "scripts" / "mutation_check.py"
)
assert _spec is not None and _spec.loader is not None
mutation_check = importlib.util.module_from_spec(_spec)
sys.modules["mutation_check"] = mutation_check
_spec.loader.exec_module(mutation_check)


def test_every_mutant_applies_to_the_current_source() -> None:
    mutants = mutation_check.load()
    assert mutants
    assert mutation_check.problems(mutants) == []


def test_every_mutant_says_what_a_survivor_would_mean() -> None:
    for mutant in mutation_check.load():
        assert mutant.risk.strip(), mutant.id
        if mutant.equivalent is not None:
            assert mutant.equivalent.strip(), mutant.id


def catalogue(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "mutants.toml"
    path.write_text(body, encoding="utf-8")
    return path


ENTRY = """
[[mutant]]
id = "{id}"
file = "src/x.py"
find = "{find}"
replace = "{replace}"
risk = "a risk"
tests = ["{test}"]
"""


def write_tree(tmp_path: Path, source: str) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "x.py").write_text(source, encoding="utf-8")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_x.py").write_text("", encoding="utf-8")


def test_duplicate_ids_are_refused(tmp_path: Path) -> None:
    entry = ENTRY.format(id="a", find="x", replace="y", test="tests/test_x.py")
    with pytest.raises(ValueError, match="duplicate"):
        mutation_check.load(catalogue(tmp_path, entry + entry))


@pytest.mark.parametrize(
    ("source", "find", "replace", "test", "named"),
    [
        ("a = 1\n", "b = 1", "b = 2", "tests/test_x.py", "occurs 0 times"),
        ("a = 1\na = 1\n", "a = 1", "a = 2", "tests/test_x.py", "occurs 2 times"),
        ("a = 1\n", "a = 1", "a = 1", "tests/test_x.py", "same as find"),
        ("a = 1\n", "a = 1", "a = 2", "tests/test_y.py", "does not exist"),
        ("a = 1\n", "a = 1", "a = (", "tests/test_x.py", "does not compile"),
    ],
    ids=["missing", "ambiguous", "unchanged", "no-test", "syntax-error"],
)
def test_a_mutant_that_cannot_apply_is_reported(
    tmp_path: Path, source: str, find: str, replace: str, test: str, named: str
) -> None:
    write_tree(tmp_path, source)
    entry = ENTRY.format(id="m", find=find, replace=replace, test=test)
    mutants = mutation_check.load(catalogue(tmp_path, entry))
    found = mutation_check.problems(mutants, tmp_path)
    assert any(named in line for line in found), found


@pytest.mark.parametrize("version", ["3", "3.12.1", "three.twelve", "3.x", 3.12])
def test_an_unparseable_before_python_is_reported(
    tmp_path: Path, version: object
) -> None:
    write_tree(tmp_path, "a = 1\n")
    entry = ENTRY.format(id="m", find="a = 1", replace="a = 2", test="tests/test_x.py")
    value = f'"{version}"' if isinstance(version, str) else str(version)
    mutants = mutation_check.load(
        catalogue(tmp_path, entry + f"before_python = {value}\n")
    )
    found = mutation_check.problems(mutants, tmp_path)
    assert any("before_python" in line for line in found), found


def test_a_mutant_only_killable_before_a_version_does_not_apply_from_it(
    tmp_path: Path,
) -> None:
    entry = ENTRY.format(id="m", find="a = 1", replace="a = 2", test="tests/test_x.py")
    (gated,) = mutation_check.load(
        catalogue(tmp_path, entry + 'before_python = "3.12"\n')
    )
    (plain,) = mutation_check.load(catalogue(tmp_path, entry))
    assert mutation_check.applies_here(gated, (3, 11))
    assert not mutation_check.applies_here(gated, (3, 12))
    assert not mutation_check.applies_here(gated, (3, 13))
    assert mutation_check.applies_here(plain, (3, 13))

    here = "{}.{}".format(*sys.version_info[:2])
    (running,) = mutation_check.load(
        catalogue(tmp_path, entry + f'before_python = "{here}"\n')
    )
    assert not mutation_check.applies_here(running)


def test_a_mutant_whose_restored_file_matches_its_size_and_mtime_is_not_cached(
    tmp_path: Path,
) -> None:
    (tmp_path / "src").mkdir()
    module = tmp_path / "src" / "target.py"
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_target.py").write_text(
        "import target\n\n\ndef test_value():\n    assert target.VALUE == 1\n",
        encoding="utf-8",
    )
    tests = ("tests/test_target.py",)
    stamp = 1_700_000_000

    module.write_text("VALUE = 2\n", encoding="utf-8")
    os.utime(module, (stamp, stamp))
    passed, _, _ = mutation_check.run_tests(tmp_path, tests)
    assert not passed

    module.write_text("VALUE = 1\n", encoding="utf-8")
    os.utime(module, (stamp, stamp))
    passed, _, last = mutation_check.run_tests(tmp_path, tests)
    assert passed, last
    assert not list(tmp_path.rglob("*.pyc"))
