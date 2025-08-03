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

kg2_nodes=${1:-"db/kg2-simplified-2.10.2-nodes.jsonl.gz"}
kg2_edges=${2:-"db/kg2-simplified-2.10.2-edges.jsonl.gz"}
babel_sqlite=${3:-"db/babel-20250331.sqlite"}
edges_output=${4:-"edges-output.jsonl"}

${python_command} ${CODE_DIR}/normalize_kg2pre.py \
                  ${kg2_nodes} \
                  ${kg2_edges} \
                  ${babel_sqlite} \
                  ${edges_output}

date
echo "==============finished run-normalize-kg2pre.sh==============="
