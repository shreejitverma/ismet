"""What the release workflows promise, read through a YAML parser.

Public behaviour: a release reaches PyPI only through the gates, and only the steps that
need an OIDC token hold one. The oracle is the supply-chain policy in
docs/THREAT_MODEL.md. A publish job that stopped depending on CI, or a build job holding
id-token, would fail here.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

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
