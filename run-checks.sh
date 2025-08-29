#!/usr/bin/env bash -x
set -o nounset -o pipefail -o errexit

run_mypy_cmd="venv/bin/mypy --ignore-missing-imports"
run_ruff_cmd="venv/bin/ruff check"
run_vulture_cmd="venv/bin/vulture `find stitch tests -name \*.py ! -name '.#*.py'`"
run_pylint_cmd="venv/bin/pylint"
files_to_check=(stitch/local_babel.py
                stitch/ingest_babel.py
                stitch/row_counts.py
                conftest.py
                tests/test_local_babel.py
                tests/test_ingest_babel.py)

for file in "${files_to_check[@]}"; do
    echo "Running ruff checks on file ${file}"
    ${run_ruff_cmd} ${file}

    echo "Running type checks on file ${file}"
    ${run_mypy_cmd} ${file}

    echo "Running dead code checks on file ${file}"
    ${run_vulture_cmd} ${file}

    # Skip pylint for unit test modules in tests/ that start with test_
    if [[ ! "${file}" == tests/test_*.py ]]; then
        echo "Running pylint checks on file ${file}"
        ${run_pylint_cmd} "${file}"
    else
        echo "Skipping pylint for test file ${file}"
    fi    
done

echo "Running full pytest unit test suite"
venv/bin/pytest -v
