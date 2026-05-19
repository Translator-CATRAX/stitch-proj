#!/usr/bin/env python3
"""Remove build artifacts produced by ``python -m build`` and friends.

Run this from the project root (e.g., ``venv/bin/python tools/clean.py``)
to delete:

* ``build/``                -- staging directory created by setuptools
* ``dist/``                 -- output directory holding ``.whl`` / ``.tar.gz``
* ``*.egg-info/``           -- top-level egg-info directory (if any)
* ``stitch.egg-info/``,
  ``stitch_proj.egg-info/`` -- per-package egg-info directories

It is normally used immediately before re-running ``python -m build`` for a
fresh release (see the "Versioning workflow" section in ``README.md``), so
that the new wheel and source distribution are assembled from a clean
staging tree. It is safe to run any time; if the target directories are
already absent it is a no-op.
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TARGETS: tuple[str, ...] = (
    "build",
    "dist",
    "*.egg-info",
)


def _main() -> int:
    removed_any = False
    for pattern in TARGETS:
        for path in REPO_ROOT.glob(pattern):
            if path.is_dir():
                print(f"removing directory: {path.relative_to(REPO_ROOT)}/")
                shutil.rmtree(path)
                removed_any = True
            elif path.exists():
                print(f"removing file: {path.relative_to(REPO_ROOT)}")
                path.unlink()
                removed_any = True
    if not removed_any:
        print("nothing to clean; build artifacts already absent")
    return 0


if __name__ == "__main__":
    sys.exit(_main())
