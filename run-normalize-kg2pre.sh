#!/usr/bin/env bash
# run-normalize-kg2pre.sh: normalize KG2pre using a local SQLite store of Babel
# Copyright 2025 Stephen A. Ramsey

set -o nounset -o pipefail -o errexit

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    echo Usage: "$0 <kg2-nodes> <kg2-edges> <babel-sqlite> <output>"
    exit 2
fi

# Usage: run-normalize-kg2pre.sh <kg2-nodes> <kg2-edges> <babel-sqlite> <output>

echo "==============starting run-normalize-kg2pre.sh==============="
date

config_dir=`dirname $0`
source ${config_dir}/master-config.shinc

kg2pre_nodes=${1:-"${BUILD_DIR}/${kg2pre_nodes_filename}"}
kg2pre_edges=${2:-"${BUILD_DIR}/${kg2pre_edges_filename}"}
babel_sqlite=${3:-"babel-20250331.sqlite"}
edges_output=${4:-"${BUILD_DIR}/stitch-edges-output-split.jsonl"}

# ${s3_cp_cmd} s3://${s3_bucket_public}/${babel_sqlite} ${BUILD_DIR}/${babel_sqlite}

date

split -n 16 --numeric-suffixes --additional-suffix .jsonl ${kg2pre_edges} ${BUILD_DIR}/kg2-edges_

date

${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_00.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.00 &
${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_01.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.01 &
${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_02.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.02 &
${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_03.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.03 &
${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_04.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.04 &
${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_05.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.05 &
${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_06.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.06 &
${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_07.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.07 &
${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_08.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.08 &
${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_09.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.09 &
${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_10.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.10 &
${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_11.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.11 &
${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_12.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.12 &
${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_13.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.13 &
${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_14.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.14 &
${python_command} ${CODE_DIR}/stitch/normalize_kg2pre_jsonlines.py ${kg2pre_nodes} ${BUILD_DIR}/kg2-edges_15.jsonl ${BUILD_DIR}/${babel_sqlite} ${edges_output}.15 &
wait

date

cat ${edges_output}.00 > ${edges_output}
cat ${edges_output}.01 >> ${edges_output}
cat ${edges_output}.02 >> ${edges_output}
cat ${edges_output}.03 >> ${edges_output}
cat ${edges_output}.04 >> ${edges_output}
cat ${edges_output}.05 >> ${edges_output}
cat ${edges_output}.06 >> ${edges_output}
cat ${edges_output}.07 >> ${edges_output}
cat ${edges_output}.08 >> ${edges_output}
cat ${edges_output}.09 >> ${edges_output}
cat ${edges_output}.10 >> ${edges_output}
cat ${edges_output}.11 >> ${edges_output}
cat ${edges_output}.12 >> ${edges_output}
cat ${edges_output}.13 >> ${edges_output}
cat ${edges_output}.14 >> ${edges_output}
cat ${edges_output}.15 >> ${edges_output}

date
echo "==============finished run-normalize-kg2pre.sh==============="
