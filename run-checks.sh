#!/usr/bin/env bash
set -o nounset -o pipefail -o errexit

run_mypy_cmd="mypy --ignore-missing-imports"
run_ruff_cmd="ruff check"

files_to_check=(stitch/local_babel.py
                stitch/ingest_babel.py
                tests/conftest.py
                tests/test_local_babel.py
                tests/test_ingest_babel.py)

for file in "${files_to_check[@]}"; do
    echo "Running ruff checks on file ${file}"
    ${run_ruff_cmd} ${file}

    echo "Running type checks on file ${file}"
    ${run_mypy_cmd} ${file}
done

echo "Running full pytest unit test suite"
pytest -v
