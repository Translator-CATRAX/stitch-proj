#!/usr/bin/env bash -x
set -o nounset -o pipefail -o errexit

run_mypy_cmd="venv/bin/mypy --ignore-missing-imports"
run_ruff_cmd="venv/bin/ruff check"
run_vulture_cmd="venv/bin/vulture `find stitch tests -name \*.py ! -name '.#*.py'`"

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

    echo "Running dead code checks on file ${file}"
    ${run_vulture_cmd} ${file}
done

echo "Running full pytest unit test suite"
venv/bin/pytest -v
