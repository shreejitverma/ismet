#!/usr/bin/env python3
"""Refuse a release unless the tag, pyproject.toml, and ismet.__version__ all agree.

Usage: check_release_version.py TAG [PYPROJECT]
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

import ismet


def main(argv: list[str]) -> int:
    tag = argv[0]
    pyproject = Path(argv[1]) if len(argv) > 1 else Path("pyproject.toml")
    declared = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"][
        "version"
    ]
    if tag != f"v{declared}" or ismet.__version__ != declared:
        print(
            f"tag {tag}, pyproject {declared}, package {ismet.__version__}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
