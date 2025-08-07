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
edges_output=${4:-"${BUILD_DIR}/stitch-edges-output.jsonl"}

${s3_cp_cmd} s3://${s3_bucket_public}/${babel_sqlite} ${BUILD_DIR}/${babel_sqlite}

date

${python_command} ${CODE_DIR}/stitch/kg2pre_to_kg2c_edges.py \
                  ${kg2pre_edges} \
                  ${BUILD_DIR}/${babel_sqlite} \
                  ${edges_output}

date
echo "==============finished run-normalize-kg2pre.sh==============="
