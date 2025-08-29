#/usr/bin/env bash
set -o nounset -o pipefail -o errexit

python3.12 -m venv venv
venv/bin/pip3 install -r requirements.txt
venv/bin/pip3 install -e .

