"""
refresh_test3_fixture.py

Regenerate the hand-trimmed ``test-artifacts/DrugChemical-test.txt`` conflation
fixture used by integration test 3 (``ingest_babel.py --test-type=3``) so that
every cluster it contains references only CURIEs that test 3 will actually have
loaded into the ``identifiers`` table.

Why this exists
---------------
Test 3 ingests only a *subset* of the Babel compendia
(``ingest_babel.TEST_3_COMPENDIA``) and then ingests the
``DrugChemical-test.txt`` fixture. The conflation ingest raises ``ValueError``
if a cluster references a CURIE that is
not present in ``identifiers``. Because the fixture is a static file while the
Babel release churns (clique membership and ``SmallMolecule`` partition
boundaries shift), a fixture built against one release goes stale against the
next and the ingest fails mid-run (e.g. "canonical CURIE not found in
identifiers: CHEBI:8310").

The only thing that determines whether a cluster is valid is the set of CURIEs
loaded into ``identifiers``. For test 3 that set is *exactly* the union of every
``identifiers[*].i`` value across the ``TEST_3_COMPENDIA`` files (test 3 runs
with ``insrt_msng_taxa=False``, so no taxon CURIEs are added). We can therefore
compute that set by *streaming-parsing* those compendia -- no sqlite database,
no index build, no VACUUM -- and drop any fixture cluster with a member outside
it. Runtime is dominated by downloading the (multi-GB) ``SmallMolecule.txt.01``
partition, so it is bandwidth-bound rather than fast, but it still beats a full
test-3 ingest, writes no database, and cannot crash on a stale fixture -- so it
is safe and cheap to run before doing any real ingest.

Modes
-----
Default: filter the *existing* fixture in place (downloads the 3 compendia
only). Keeps the curated subset stable, just dropping newly-stale clusters.

``--from-release``: instead filter the new release's full
``conflation/DrugChemical.txt`` (downloads that large file too). Picks up
brand-new clusters so the fixture tracks the release rather than only shrinking.

Run from the repository root with ``python tools/refresh_test3_fixture.py``
(imports the ``stitch`` package, which must be installed -- e.g. the project's
editable venv install). See ``--help`` for options.
"""
import argparse
import ast
import json
import sys
import tempfile
from pathlib import Path
from typing import Iterable, Iterator
from urllib.parse import urljoin

from stitch import stitchutils as su
from stitch.ingest_babel import (
    DEFAULT_BABEL_COMPENDIA_URL,
    DEFAULT_BABEL_CONFLATION_URL,
    TEST_3_COMPENDIA,
)

# Large chunks keep the pandas/line-chunk overhead down; we only scan, never
# hold a whole file in memory at once.
_LINES_PER_CHUNK = 10_000
_DEFAULT_FIXTURE = Path("test-artifacts") / "DrugChemical-test.txt"
# The real Babel conflation file that the fixture is trimmed from; this is the
# download source in --from-release mode (distinct from the fixture's name).
_RELEASE_CONFLATION_FILE = "DrugChemical.txt"
# The fixture historically included a few clusters with a duplicated member
# (e.g. the leading ``["CHEBI:8310", "CHEBI:8310", ...]``) to exercise the
# dedup branch in ingest_babel.process_conflation_chunk. Guarantee at least
# this many survive so that coverage is not silently lost.
_DEFAULT_MIN_DUP_LINES = 3


def _log(msg: str) -> None:
    print(f"{su.cur_datetime_local_str()}: {msg}", file=sys.stderr)


def _ensure_trailing_slash(url: str) -> str:
    return url if url.endswith("/") else url + "/"


def _collect_valid_curies(compendia_base_url: str,
                          filenames: Iterable[str],
                          temp_dir: Path) -> set[str]:
    """Union of every ``identifiers[*].i`` across the given compendia files.

    This is exactly the set of CURIEs that end up in the ``identifiers`` table
    after test 3's compendia ingest (see
    ``ingest_babel._make_compendia_chunk_processor``).
    """
    valid: set[str] = set()
    for name in filenames:
        url = urljoin(compendia_base_url, name)
        _log(f"scanning compendium for identifier CURIEs: {url}")
        n_before = len(valid)
        for chunk_df in su.read_json_lines_from_url(url, _LINES_PER_CHUNK,
                                                    temp_dir):
            for identifiers in chunk_df["identifiers"]:
                for ident in identifiers:
                    valid.add(ident["i"])
        _log(f"  added {len(valid) - n_before} new CURIEs "
             f"(running total {len(valid)})")
    return valid


def _iter_cluster_lines(source: str, temp_dir: Path) -> Iterator[str]:
    """Yield raw (unstripped) cluster lines from a local path or a URL."""
    if "://" in source:
        for chunk in su.read_line_chunks_from_url(source, _LINES_PER_CHUNK,
                                                  temp_dir):
            yield from chunk
    else:
        with open(source, encoding="utf-8") as file_obj:
            yield from file_obj


def _has_duplicate_member(curie_list: list[str]) -> bool:
    return len(curie_list) != len(set(curie_list))


def _ensure_dup_coverage(lines: list[str],
                         dup_line_count: int,
                         min_dup_lines: int) -> list[str]:
    """Guarantee at least ``min_dup_lines`` clusters exercise the dedup branch.

    If too few kept clusters already contain a duplicate member, inject a
    duplicated canonical CURIE into the leading position of some kept clusters
    (mirroring the historical ``["CHEBI:8310", "CHEBI:8310", ...]`` shape). The
    canonical is already known-valid, so this never introduces a missing CURIE.
    """
    if dup_line_count >= min_dup_lines:
        return lines
    need = min_dup_lines - dup_line_count
    _log(f"only {dup_line_count} kept cluster(s) exercise the dedup branch; "
         f"injecting a duplicated canonical into {need} more to preserve "
         f"coverage")
    out = list(lines)
    injected = 0
    for i, line in enumerate(out):
        if injected >= need:
            break
        curie_list = ast.literal_eval(line)
        if not _has_duplicate_member(curie_list):
            out[i] = json.dumps([curie_list[0]] + curie_list)
            injected += 1
    if injected < need:
        _log(f"warning: could inject only {injected} of {need} duplicate "
             f"cluster(s); dedup-branch coverage may be limited")
    return out


def _write_fixture(fixture_path: Path, lines: list[str]) -> None:
    """Atomically overwrite ``fixture_path`` with the kept cluster lines."""
    tmp_path = fixture_path.with_suffix(fixture_path.suffix + ".tmp")
    tmp_path.write_text("".join(line + "\n" for line in lines),
                        encoding="utf-8")
    tmp_path.replace(fixture_path)
    _log(f"wrote {len(lines)} clusters to {fixture_path}")


def refresh_fixture(compendia_base_url: str,
                    source: str,
                    fixture_path: Path,
                    min_dup_lines: int,
                    temp_dir: Path) -> None:
    valid = _collect_valid_curies(compendia_base_url, TEST_3_COMPENDIA,
                                  temp_dir)
    _log(f"valid CURIE set size: {len(valid)}")

    kept: list[str] = []
    total = dropped = dup_lines = 0
    for raw_line in _iter_cluster_lines(source, temp_dir):
        line = raw_line.strip()
        if not line:
            continue
        total += 1
        curie_list = ast.literal_eval(line)
        su.validate_curie_list(curie_list)
        # A cluster survives iff every (deduplicated) member is loadable; the
        # canonical is curie_list[0], which is included in this subset check.
        if set(curie_list) <= valid:
            kept.append(line)  # keep verbatim, preserving any duplicates
            if _has_duplicate_member(curie_list):
                dup_lines += 1
        else:
            dropped += 1

    _log(f"clusters: total={total} kept={len(kept)} dropped={dropped}")
    _log(f"kept clusters exercising the dedup branch: {dup_lines}")
    if not kept:
        raise ValueError("no clusters survived filtering; check that "
                         "--babel-compendia-url points at the intended release")

    kept = _ensure_dup_coverage(kept, dup_lines, min_dup_lines)
    _write_fixture(fixture_path, kept)


def _get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Regenerate test-artifacts/DrugChemical-test.txt for "
                    "integration test 3 without a full Babel ingest.")
    parser.add_argument("--babel-compendia-url", type=str,
                        dest="babel_compendia_url",
                        default=DEFAULT_BABEL_COMPENDIA_URL,
                        help="base URL of the release's compendia/ directory "
                             "(default: %(default)s)")
    parser.add_argument("--fixture-file", type=Path, dest="fixture_file",
                        default=_DEFAULT_FIXTURE,
                        help="path to the DrugChemical-test.txt fixture to "
                             "write (default: %(default)s)")
    parser.add_argument("--from-release", action="store_true",
                        dest="from_release",
                        help="filter the release's full conflation "
                             "DrugChemical.txt instead of the existing "
                             "fixture (downloads the large conflation file)")
    parser.add_argument("--babel-conflation-url", type=str,
                        dest="babel_conflation_url",
                        default=DEFAULT_BABEL_CONFLATION_URL,
                        help="base URL of the release's conflation/ directory; "
                             "used only with --from-release "
                             "(default: %(default)s)")
    parser.add_argument("--min-dup-lines", type=int, dest="min_dup_lines",
                        default=_DEFAULT_MIN_DUP_LINES,
                        help="minimum number of kept clusters that must contain "
                             "a duplicated member, to preserve dedup-branch "
                             "coverage (default: %(default)s)")
    return parser.parse_args()


def _main() -> None:
    args = _get_args()
    compendia_base = _ensure_trailing_slash(args.babel_compendia_url)
    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        if args.from_release:
            conflation_base = _ensure_trailing_slash(args.babel_conflation_url)
            source = urljoin(conflation_base, _RELEASE_CONFLATION_FILE)
            _log(f"regenerating fixture from release conflation file: {source}")
        else:
            source = str(args.fixture_file)
            _log(f"filtering existing fixture in place: {source}")
        refresh_fixture(compendia_base, source, args.fixture_file,
                        args.min_dup_lines, temp_dir)


if __name__ == "__main__":
    _main()
