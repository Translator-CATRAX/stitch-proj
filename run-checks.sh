#!/usr/bin/env bash
set -o nounset -o pipefail -o errexit

echo "Running type checks"
mypy --ignore-missing-imports local_babel.py

echo "Running lint checks"
ruff check local_babel.py

