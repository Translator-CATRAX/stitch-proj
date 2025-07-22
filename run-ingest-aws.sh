#!/usr/bin/env -S bash -x

set -o nounset -o pipefail -o errexit

STITCH_DIR=/home/ubuntu/stitch
STITCH_TMP_DIR=/home/ubuntu/stitch/tmp
STITCH_LOG_FILE=${STITCH_DIR}/ingest-babel.log
STITCH_SQLITE_FILE=${STITCH_DIR}/babel.sqlite
BABEL_COMPENDIA_BASE_URL=https://stars.renci.org/var/babel_outputs/2025mar31/compendia/
BABEL_CONFLATION_BASE_URL=https://stars.renci.org/var/babel_outputs/2025mar31/conflation/

export SQLITE_TMPDIR=${STITCH_TMP_DIR}
rm -r -f ${STITCH_TMP_DIR}
mkdir -p ${STITCH_TMP_DIR}

${STITCH_DIR}/venv/bin/python3.12 -u ${STITCH_DIR}/stitch/ingest_babel.py \
             --babel-compendia-url ${BABEL_COMPENDIA_BASE_URL} \
             --babel-conflation-url ${BABEL_CONFLATION_BASE_URL} \
             --database-file-name ${STITCH_SQLITE_FILE} \
             --temp-dir ${STITCH_TMP_DIR} \
             >${STITCH_LOG_FILE} 2>&1
