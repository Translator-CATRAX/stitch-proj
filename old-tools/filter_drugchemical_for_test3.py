"""Throwaway: produce a DrugChemical.txt restricted to CURIEs present in a
given set of compendia files (Drug.txt, ChemicalEntity.txt, SmallMolecule.txt.01).

The DrugChemical conflation file ships clusters that reference SmallMolecule
CURIEs from shards .02, .03, ... which test 3 of the stitch integration suite
does not load.  This script keeps only those clusters whose every CURIE is
present in the compendia you point it at.

Usage
-----
    python filter_drugchemical_for_test3.py \
        DrugChemical-test3.txt \
        DrugChemical.txt \
        Drug.txt ChemicalEntity.txt SmallMolecule.txt.01

The compendia files can be fetched from
    https://stars.renci.org/var/babel_outputs/2025mar31/compendia/
and the conflation file from
    https://stars.renci.org/var/babel_outputs/2025mar31/conflation/
"""

from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path

# Reasonable CURIE shape: prefix (alpha-starting), colon, non-whitespace local.
_CURIE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9._-]*:[^\s]+$")


def _walk(node: object, found: set[str]) -> None:
    """Add every CURIE-shaped string in ``node`` (recursively) to ``found``."""
    if isinstance(node, dict):
        for value in node.values():
            _walk(value, found)
    elif isinstance(node, list):
        for value in node:
            _walk(value, found)
    elif isinstance(node, str) and _CURIE_RE.match(node):
        found.add(node)


def collect_curies(compendium_path: Path) -> set[str]:
    """Return every CURIE-shaped string value anywhere in a Babel compendium."""
    found: set[str] = set()
    with compendium_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            _walk(json.loads(line), found)
    return found


def main(argv: list[str]) -> int:
    if len(argv) < 4:
        print(
            "usage: filter_drugchemical_for_test3.py "
            "OUT.txt DrugChemical.txt COMPENDIUM [COMPENDIUM ...]",
            file=sys.stderr,
        )
        return 2

    out_path = Path(argv[1])
    conflation_path = Path(argv[2])
    compendia = [Path(p) for p in argv[3:]]

    available: set[str] = set()
    for path in compendia:
        before = len(available)
        print(f"scanning {path} ...", file=sys.stderr)
        available |= collect_curies(path)
        print(
            f"  +{len(available) - before} new CURIEs "
            f"(running total: {len(available)})",
            file=sys.stderr,
        )

    kept = dropped = 0
    with (
        conflation_path.open("r", encoding="utf-8") as src,
        out_path.open("w", encoding="utf-8") as dst,
    ):
        for line in src:
            stripped = line.strip()
            if not stripped:
                continue
            curies = ast.literal_eval(stripped)
            if all(c in available for c in curies):
                dst.write(stripped + "\n")
                kept += 1
            else:
                dropped += 1

    print(
        f"wrote {kept} clusters to {out_path}; dropped {dropped} "
        f"({100 * dropped / (kept + dropped):.1f}% of input)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
