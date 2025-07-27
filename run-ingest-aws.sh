#!/usr/bin/env bash
set -o nounset -o pipefail -o errexit

STITCH_DIR=/home/ubuntu/stitch
# This next line is commented out because I think we have successfully
# resolved issue 17, with the temp dir being able to be specified only
# via the "--temp-dir" command-line option for ingest_babel.py:
STITCH_TMP_DIR=/home/ubuntu/stitch/tmp
STITCH_LOG_FILE=${STITCH_DIR}/ingest-babel.log
STITCH_SQLITE_FILE=${STITCH_DIR}/babel.sqlite
BABEL_BASE_URL=https://stars.renci.org/var/babel_outputs/2025mar31
BABEL_COMPENDIA_BASE_URL=${BABEL_BASE_URL}/compendia/
BABEL_CONFLATION_BASE_URL=${BABEL_BASE_URL}/conflation/

export SQLITE_TMPDIR=${STITCH_TMP_DIR}
rm -r -f ${STITCH_TMP_DIR}
mkdir -p ${STITCH_TMP_DIR}

${STITCH_DIR}/venv/bin/python3.12 -u ${STITCH_DIR}/stitch/ingest_babel.py \
             --babel-compendia-url ${BABEL_COMPENDIA_BASE_URL} \
             --babel-conflation-url ${BABEL_CONFLATION_BASE_URL} \
             --database-file-name ${STITCH_SQLITE_FILE} \
             --temp-dir ${STITCH_TMP_DIR} \
             >${STITCH_LOG_FILE} 2>&1
