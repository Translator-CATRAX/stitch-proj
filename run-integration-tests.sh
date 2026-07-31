#!/usr/bin/env bash
set -euo pipefail

STITCH_DIR=.
# This next line is commented out because I think we have successfully
# resolved issue 17, with the temp dir being able to be specified only
# via the "--temp-dir" command-line option for ingest_babel.py:
STITCH_LOG_FILE=${STITCH_DIR}/run-integration-tests.log
STITCH_SQLITE_FILE=${STITCH_DIR}/babel.sqlite
BABEL_BASE_URL=https://stars.renci.org/var/babel_outputs/2026jul22
BABEL_COMPENDIA_BASE_URL=${BABEL_BASE_URL}/compendia/
BABEL_CONFLATION_BASE_URL=${BABEL_BASE_URL}/conflation/
# Directory holding a hand-trimmed DrugChemical-test.txt for test 3 (must
# contain a file literally named "DrugChemical-test.txt" -- the filename is
# hardcoded in ingest_babel.py:TEST_3_CONFLATION. Regenerate it for a new
# Babel release with: python tools/refresh_test3_fixture.py).
TEST_ARTIFACTS_DIR=${STITCH_DIR}/test-artifacts
INGEST_BABEL_CMD=venv/bin/ingest-babel

rm -f ${STITCH_LOG_FILE}

${INGEST_BABEL_CMD} \
             --babel-compendia-url ${BABEL_COMPENDIA_BASE_URL} \
             --babel-conflation-url ${BABEL_CONFLATION_BASE_URL} \
             --database-file-name ${STITCH_SQLITE_FILE} \
             --test-compendia-file ${TEST_ARTIFACTS_DIR}/test-tiny.jsonl \
             --test-type=1 \
             >>${STITCH_LOG_FILE} 2>&1

${INGEST_BABEL_CMD} \
             --babel-compendia-url ${BABEL_COMPENDIA_BASE_URL} \
             --babel-conflation-url ${BABEL_CONFLATION_BASE_URL} \
             --database-file-name ${STITCH_SQLITE_FILE} \
             --test-type=2 \
             >>${STITCH_LOG_FILE} 2>&1

${INGEST_BABEL_CMD} \
             --babel-compendia-url ${BABEL_COMPENDIA_BASE_URL} \
             --babel-conflation-url "file://$(cd "${TEST_ARTIFACTS_DIR}" && pwd)/" \
             --database-file-name ${STITCH_SQLITE_FILE} \
             --test-type=3 \
             >>${STITCH_LOG_FILE} 2>&1

# Test 4 ingests umls.txt (ingest_babel.py:TEST_4_COMPENDIA). That file is not
# covered by tests 1-3, and its JSON key order has changed across Babel
# releases, so it is worth exercising on its own before a full ingest.
${INGEST_BABEL_CMD} \
             --babel-compendia-url ${BABEL_COMPENDIA_BASE_URL} \
             --babel-conflation-url ${BABEL_CONFLATION_BASE_URL} \
             --database-file-name ${STITCH_SQLITE_FILE} \
             --test-type=4 \
             >>${STITCH_LOG_FILE} 2>&1
