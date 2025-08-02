#!/usr/bin/env bash
set -o nounset -o pipefail -o errexit

python3.12 -u stitch/normalize_kg2pre.py \
           db/kg2-simplified-2.10.2-nodes.jsonl.gz \
           db/kg2-simplified-2.10.2-edges.jsonl.gz \
           db/babel-20250331.sqlite \
           edges-output.jsonl

