#!/usr/bin/env bash
# Remove build artifacts produced by `python -m build` and `pip install -e .`.
#
# Deletes the following at the repository root (no-op if absent):
#   - build/         (staging directory created by setuptools)
#   - dist/          (output directory holding `.whl` / `.tar.gz`)
#   - *.egg-info/    (top-level egg-info directories)
#
# See the "Cleaning build artifacts" section of README.md for when to run.
# Safe to run any time; if nothing is present it just reports that.

set -o nounset -o pipefail -o errexit

# Operate from the repository root, regardless of where this script was invoked.
cd "$(dirname "$0")/.."

# Avoid literal "*.egg-info" being passed to rm when no match exists.
shopt -s nullglob

removed_any=0
for path in build dist ./*.egg-info; do
    if [ -e "${path}" ]; then
        echo "removing: ${path}"
        rm -rf "${path}"
        removed_any=1
    fi
done

if [ "${removed_any}" -eq 0 ]; then
    echo "nothing to clean; build artifacts already absent"
fi
