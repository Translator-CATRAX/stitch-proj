#!/usr/bin/env bash
set -o nounset -o pipefail -o errexit

STITCH_DIR=/home/ubuntu/stitch-proj
STITCH_TMP_DIR=${STITCH_DIR}/tmp
STITCH_LOG_FILE=${STITCH_DIR}/ingest-babel.log
STITCH_SQLITE_FILE=${STITCH_DIR}/babel.sqlite

BABEL_BASE_URL=https://stars.renci.org/var/babel_outputs/2025aug17
BABEL_COMPENDIA_BASE_URL=${BABEL_BASE_URL}/compendia/
BABEL_CONFLATION_BASE_URL=${BABEL_BASE_URL}/conflation/

export SQLITE_TMPDIR=${STITCH_TMP_DIR}
rm -r -f ${STITCH_TMP_DIR}
mkdir -p ${STITCH_TMP_DIR}

INGEST_BABEL_CMD=${STITCH_DIR}/venv/bin/ingest-babel

${INGEST_BABEL_CMD} \
             --babel-compendia-url ${BABEL_COMPENDIA_BASE_URL} \
             --babel-conflation-url ${BABEL_CONFLATION_BASE_URL} \
             --database-file-name ${STITCH_SQLITE_FILE} \
             --temp-dir ${STITCH_TMP_DIR} \
             >${STITCH_LOG_FILE} 2>&1
