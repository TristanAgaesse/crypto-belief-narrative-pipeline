from __future__ import annotations

import sys
from pathlib import Path


def pytest_configure() -> None:
    """Ensure tests exercise the workspace `src/` tree.

    Some developer environments may have an editable install of this package
    pointing at a different checkout; tests should always run against the
    current worktree.
    """

    root = Path(__file__).resolve().parents[1]
    src = root / "src"
    if src.exists():
        sys.path.insert(0, str(src))

