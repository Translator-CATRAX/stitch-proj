#!/usr/bin/env bash
set -o nounset -o pipefail -o errexit

run_mypy_cmd="mypy --ignore-missing-imports"
run_ruff_cmd="ruff check"

echo "Running type checks on local_babel.py"
${run_mypy_cmd} local_babel.py

echo "Running ruff checks on local_babel.py"
${run_ruff_cmd} local_babel.py

echo "Running type checks on ingest_babel.py"
${run_mypy_cmd} ingest_babel.py

echo "Running ruff checks on ingest_babel.py"
${run_ruff_cmd} ingest_babel.py
