# Changelog

All notable changes to the `stitch-proj` **PyPI distribution package** are
documented in this file. (Releases of the Babel sqlite *database* artifacts
are tracked separately on the
[GitHub Releases page](https://github.com/Translator-CATRAX/stitch-proj/releases)
under the `babel-*` tags.)

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Note on historical entries.** Per-version changelog tracking began with the
> `0.1.4` development cycle. Entries for `0.1.3` and earlier were reconstructed
> after the fact from git history and the issue tracker. Only `v0.1.2` and
> `v0.1.3` were git-tagged; the `0.0.1`, `0.1.0`, and `0.1.1` PyPI releases were
> published without tags, so their boundaries were reconstructed from PyPI
> upload dates. Version `0.1.2` was tagged but never published to PyPI — the
> work accumulated under it shipped in `0.1.3`.

## [Unreleased]

### Added
- `local_babel.get_label_for_curie(conn, curie)`: return the `identifiers.label`
  for a CURIE (`None` if absent), with unit tests.

### Changed
- `document-dependencies.sh` now supports two modes: `--fresh` (default) builds
  a throwaway venv from `pyproject.toml` and freezes that, so `dependencies.txt`
  is exactly the declared dependency closure by construction; `--from-venv`
  preserves the previous behavior of freezing the existing `./venv` as-is.
- Documentation: clarified that the `pyproject.toml` `version` is a bare PEP 440
  triple (no `v` prefix); documented the venv hygiene around `dependencies.txt`
  and the rule to regenerate it whenever `pyproject.toml` dependencies change.
- Regenerated `dependencies.txt` from a clean environment, removing an orphaned
  `ray` pin (and bumping `urllib3`/`idna`/`requests`/`pytest` past their
  advisories), clearing the repository's Dependabot alerts.

## [0.1.3] - 2026-06-10

This release supersedes the unpublished `v0.1.2` tag; it bundles all the work
accumulated since `0.1.1`.

### Added
- `local_babel.map_name_to_curie`: resolve a name to a CURIE (#89).
- `tools/clean.sh` for cleaning up build artifacts (#88).
- `build-release.sh`, a tag-gated release-build wrapper, with documentation
  (#94).
- Database indexes: on `descriptions.desc` (#87) and a `COLLATE NOCASE` index on
  `identifiers.label` (#92).
- Uniqueness constraint on the `identifiers_taxa` link table (#70).
- Guard so `ingest_babel.py` fails clearly if run as a plain script (#91).
- Type hints for the `Callable` usages in `ingest_babel.py` (#28).

### Changed
- Packaging modernized to use `pyproject.toml` for PyPI dependencies and pytest
  configuration (#93); development-only dependencies separated into a `dev`
  extra (#86).
- `ingest_babel.py` index handling improved (#90), deferring creation of
  inessential indexes until the end of the ingest for faster builds (#74).
- Documented the PyPI build-and-publish process; overhauled and reorganized the
  README (#81).

### Fixed
- Resolved `twine` warnings on upload (#95).

## [0.1.1] - 2026-04-15

### Changed
- `row_counts.py` now connects to the sqlite database in read-only mode (#62).
- `_get_biolink_type_pkids_from_curies` accepts a `Sequence[str]` and guards
  against empty input.
- `run-integration-tests.sh` made flexible as to its script location.
- README updated with PyPI package details.

### Fixed
- Inconsistent result ordering from `local_babel.map_any_curie_to_cliques`,
  with a regression test (#79).
- Invalid `test_type` now raises `ValueError` instead of `AssertionError`.
- Missing-taxon error now includes the offending row id.
- Package-compatibility issue where `pipreqs` pulled in `ipython`, breaking use
  in Google Colab.

## [0.1.0] - 2026-02-20

### Added
- First functional PyPI distribution of `stitch-proj`: an installable package
  exposing the Babel ingest (`ingest_babel.py`) and local-query (`local_babel.py`)
  modules, plus `row_counts.py`, `babel_schema.py`, and `stitchutils.py`.
- README install/usage instructions for the PyPI package.

## [0.0.1] - 2026-02-20

### Added
- Initial upload of the `stitch-proj` package to PyPI.

[Unreleased]: https://github.com/Translator-CATRAX/stitch-proj/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/Translator-CATRAX/stitch-proj/releases/tag/v0.1.3
[0.1.1]: https://pypi.org/project/stitch-proj/0.1.1/
[0.1.0]: https://pypi.org/project/stitch-proj/0.1.0/
[0.0.1]: https://pypi.org/project/stitch-proj/0.0.1/
