#!/usr/bin/env python3
"""Check the distributions a build produced, the same way in CI and in a release.

Usage: check_dist.py DIST [--smoke]

There must be one pure wheel and one source tarball, neither holding recorded venue
data. ``--smoke`` also installs the wheel into a fresh environment and runs it outside
the source tree.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tarfile
import tempfile
import venv
import zipfile
from pathlib import Path

_RECORDED_DATA = "tests/fixtures/"


def problems(dist: Path) -> list[str]:
    wheels = sorted(dist.glob("*.whl"))
    sdists = sorted(dist.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        return [f"expected one wheel and one sdist, found {wheels + sdists}"]
    wheel, sdist = wheels[0], sdists[0]
    found: list[str] = []
    if not wheel.name.endswith("-py3-none-any.whl"):
        found.append(f"{wheel.name} is not a pure wheel")
    with zipfile.ZipFile(wheel) as archive:
        wheel_members = archive.namelist()
    with tarfile.open(sdist) as archive:
        sdist_members = archive.getnames()
    for name, members in ((wheel.name, wheel_members), (sdist.name, sdist_members)):
        found += [
            f"{name} holds recorded venue data: {member}"
            for member in members
            if _RECORDED_DATA in member
        ]
    return found


def smoke(wheel: Path) -> None:
    with tempfile.TemporaryDirectory() as directory:
        fresh = Path(directory) / "fresh"
        venv.create(fresh, with_pip=True)
        bin_dir = fresh / "bin"
        subprocess.run([bin_dir / "pip", "install", wheel.resolve()], check=True)
        ismet = bin_dir / "ismet"
        mock = {**os.environ, "ISMET_PROVIDERS": "mock"}
        for command, env in (
            ([ismet, "--version"], None),
            ([ismet, "calendar", "XNYS", "--on", "2026-11-27"], None),
            ([ismet, "quote", "ACME@XMOK"], mock),
        ):
            subprocess.run(command, check=True, cwd=directory, env=env)


def main(argv: list[str]) -> int:
    dist = Path(argv[0])
    found = problems(dist)
    for problem in found:
        print(problem, file=sys.stderr)
    if found:
        return 1
    if "--smoke" in argv[1:]:
        smoke(next(dist.glob("*.whl")))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
