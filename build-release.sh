#!/usr/bin/env bash
#
# build-release.sh -- build the PyPI distribution, gated on a release tag.
#
# Refuses to build unless ALL of the following hold:
#   - HEAD is exactly at a vX.Y.Z tag,
#   - the working tree is clean, and
#   - the tag's version matches `version` in pyproject.toml.
# This makes it impossible to produce (and then upload) a distribution
# that has no corresponding source-repository tag, or whose version
# disagrees with that tag. Intended to be run from a fresh checkout of
# the release tag (see the versioning workflow in README.md).
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

# Gate 1: HEAD must be exactly at a vX.Y.Z release tag.
if ! tag="$(git describe --exact-match --tags --match 'v[0-9]*' HEAD 2>/dev/null)"; then
    echo "ERROR: HEAD is not at a vX.Y.Z release tag; refusing to build." >&2
    echo "Tag the release first, then build from a fresh checkout of the tag." >&2
    exit 1
fi

# Gate 2: the working tree must be clean, so the build matches the tag.
if ! git diff --quiet HEAD; then
    echo "ERROR: working tree has uncommitted changes; refusing to build." >&2
    exit 1
fi

# Gate 3: the tag must match the version declared in pyproject.toml.
tag_version="${tag#v}"
pyproject_version="$(python -c 'import tomllib; print(tomllib.load(open("pyproject.toml","rb"))["project"]["version"])')"
if [ "${tag_version}" != "${pyproject_version}" ]; then
    echo "ERROR: tag ${tag} does not match pyproject.toml version ${pyproject_version}." >&2
    echo "The git tag (vX.Y.Z) and the pyproject.toml version (X.Y.Z) must agree." >&2
    exit 1
fi

echo "Building release ${tag} from a clean, tagged checkout ..."
python -m build
echo "Done: release ${tag} built into dist/"
