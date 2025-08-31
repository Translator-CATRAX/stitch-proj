#!/usr/bin/env bash
set -o nounset -o pipefail -o errexit

STITCH_DIR=.
# This next line is commented out because I think we have successfully
# resolved issue 17, with the temp dir being able to be specified only
# via the "--temp-dir" command-line option for ingest_babel.py:
STITCH_LOG_FILE=${STITCH_DIR}/run-integration-tests.log
STITCH_SQLITE_FILE=${STITCH_DIR}/babel.sqlite
BABEL_BASE_URL=https://stars.renci.org/var/babel_outputs/2025mar31
BABEL_COMPENDIA_BASE_URL=${BABEL_BASE_URL}/compendia/
BABEL_CONFLATION_BASE_URL=${BABEL_BASE_URL}/conflation/
INGEST_BABEL_CMD=${STITCH_DIR}/venv/bin/ingest-babel

rm -f ${STITCH_LOG_FILE}

${INGEST_BABEL_CMD} \
             --babel-compendia-url ${BABEL_COMPENDIA_BASE_URL} \
             --babel-conflation-url ${BABEL_CONFLATION_BASE_URL} \
             --database-file-name ${STITCH_SQLITE_FILE} \
             --test-type=1
             >>${STITCH_LOG_FILE} 2>&1

${INGEST_BABEL_CMD} \
             --babel-compendia-url ${BABEL_COMPENDIA_BASE_URL} \
             --babel-conflation-url ${BABEL_CONFLATION_BASE_URL} \
             --database-file-name ${STITCH_SQLITE_FILE} \
             --test-type=2
             >>${STITCH_LOG_FILE} 2>&1

${INGEST_BABEL_CMD} \
             --babel-compendia-url ${BABEL_COMPENDIA_BASE_URL} \
             --babel-conflation-url ${BABEL_CONFLATION_BASE_URL} \
             --database-file-name ${STITCH_SQLITE_FILE} \
             --test-type=3
             >>${STITCH_LOG_FILE} 2>&1
