"""What the release workflows promise, read through a YAML parser.

Public behaviour: a release reaches PyPI only through the gates, and only the steps that
need an OIDC token hold one. The oracle is the supply-chain policy in
docs/THREAT_MODEL.md. A publish job that stopped depending on CI, or a build job holding
id-token, would fail here.
"""

from __future__ import annotations

import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path
from typing import Any

import pytest

import ismet

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[2] / ".github" / "workflows"


def workflow(name: str) -> dict[Any, Any]:
    loaded: dict[Any, Any] = yaml.safe_load((ROOT / name).read_text(encoding="utf-8"))
    return loaded


def ancestors(jobs: dict[str, Any], name: str) -> set[str]:
    needs = jobs[name].get("needs", [])
    direct = {needs} if isinstance(needs, str) else set(needs)
    found = set(direct)
    for job in direct:
        found |= ancestors(jobs, job)
    return found


def test_publishing_requires_the_version_check_ci_and_attestation() -> None:
    jobs = workflow("publish.yml")["jobs"]
    assert {"version", "ci", "build", "attest"} <= ancestors(jobs, "publish")
    assert jobs["ci"]["uses"] == "./.github/workflows/workflow.yml"
    # The YAML 1.1 parser reads the key ``on`` as true.
    assert "workflow_call" in workflow("workflow.yml")[True]


def test_only_a_version_tag_can_start_a_release() -> None:
    jobs = workflow("publish.yml")["jobs"]
    assert jobs["version"]["if"] == "startsWith(github.ref, 'refs/tags/v')"


def test_only_jobs_that_install_nothing_hold_an_oidc_token() -> None:
    jobs = workflow("publish.yml")["jobs"]
    holders = {
        name
        for name, job in jobs.items()
        if job.get("permissions", {}).get("id-token") == "write"
    }
    assert holders == {"attest", "publish"}
    for name in holders:
        for step in jobs[name]["steps"]:
            assert "run" not in step, f"{name} runs a command while holding a token"


@pytest.mark.parametrize("path", sorted(ROOT.glob("*.yml")), ids=lambda p: p.name)
def test_every_third_party_action_is_pinned_to_a_commit(path: Path) -> None:
    for job in yaml.safe_load(path.read_text(encoding="utf-8"))["jobs"].values():
        for step in job.get("steps", []):
            uses = step.get("uses")
            if uses is not None and not uses.startswith("./"):
                _, _, ref = uses.partition("@")
                assert len(ref) == 40, uses
                assert all(c in "0123456789abcdef" for c in ref), uses


SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"


def run_script(name: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPTS / name), *args],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def declare_version(tmp_path: Path, version: str) -> str:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(f'[project]\nversion = "{version}"\n', encoding="utf-8")
    return str(pyproject)


def test_a_release_is_allowed_when_tag_pyproject_and_package_agree(
    tmp_path: Path,
) -> None:
    version = ismet.__version__
    result = run_script(
        "check_release_version.py", f"v{version}", declare_version(tmp_path, version)
    )
    assert result.returncode == 0, result.stderr


def test_a_release_is_refused_when_the_tag_names_another_version(
    tmp_path: Path,
) -> None:
    result = run_script(
        "check_release_version.py",
        "v99.0.0",
        declare_version(tmp_path, ismet.__version__),
    )
    assert result.returncode == 1 and "v99.0.0" in result.stderr


def test_a_release_is_refused_when_the_package_disagrees_with_both(
    tmp_path: Path,
) -> None:
    result = run_script(
        "check_release_version.py", "v99.0.0", declare_version(tmp_path, "99.0.0")
    )
    assert result.returncode == 1


def steps_running(job: dict[str, Any], script: str) -> list[str]:
    return [s["run"] for s in job["steps"] if script in s.get("run", "")]


def test_the_version_job_runs_the_tested_check() -> None:
    job = workflow("publish.yml")["jobs"]["version"]
    assert steps_running(job, "scripts/check_release_version.py")


@pytest.mark.parametrize(
    ("name", "job"), [("publish.yml", "build"), ("workflow.yml", "build")]
)
def test_what_is_built_is_checked_and_smoke_tested_in_the_same_job(
    name: str, job: str
) -> None:
    steps = workflow(name)["jobs"][job]["steps"]
    runs = [step.get("run", "") for step in steps]
    built = next(i for i, run in enumerate(runs) if "python -m build" in run)
    checked = next(
        i for i, run in enumerate(runs) if "check_dist.py dist --smoke" in run
    )
    assert built < checked


def make_dist(
    dist: Path,
    *,
    wheel: str = "ismet-1-py3-none-any.whl",
    sdist_member: str = "ismet-1/src/ismet/__init__.py",
    wheel_member: str = "ismet/__init__.py",
) -> None:
    dist.mkdir()
    with zipfile.ZipFile(dist / wheel, "w") as archive:
        archive.writestr(wheel_member, "")
    source = dist / "member"
    source.write_text("", encoding="utf-8")
    with tarfile.open(dist / "ismet-1.tar.gz", "w:gz") as archive:
        archive.add(source, arcname=sdist_member)
    source.unlink()


def test_clean_distributions_pass(tmp_path: Path) -> None:
    dist = tmp_path / "dist"
    make_dist(dist)
    assert run_script("check_dist.py", str(dist)).returncode == 0


@pytest.mark.parametrize(
    ("terms", "named"),
    [
        ({"wheel": "ismet-1-cp312-cp312-linux_x86_64.whl"}, "not a pure wheel"),
        (
            {"sdist_member": "ismet-1/tests/fixtures/binance/ws_trade.json"},
            "venue data",
        ),
        ({"wheel_member": "tests/fixtures/coinbase/ws_ticker.json"}, "venue data"),
    ],
)
def test_impure_or_data_carrying_distributions_are_refused(
    tmp_path: Path, terms: dict[str, str], named: str
) -> None:
    dist = tmp_path / "dist"
    make_dist(dist, **terms)
    result = run_script("check_dist.py", str(dist))
    assert result.returncode == 1 and named in result.stderr


def test_a_missing_sdist_is_refused(tmp_path: Path) -> None:
    dist = tmp_path / "dist"
    make_dist(dist)
    (dist / "ismet-1.tar.gz").unlink()
    assert run_script("check_dist.py", str(dist)).returncode == 1
