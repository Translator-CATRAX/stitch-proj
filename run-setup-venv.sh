#/usr/bin/env bash
set -o nounset -o pipefail -o errexit

install_dev=false
for arg in "$@"; do
    case "${arg}" in
        --dev) install_dev=true ;;
        *) echo "Unknown argument: ${arg}" >&2; echo "Usage: $0 [--dev]" >&2; exit 1 ;;
    esac
done

rm -r -f venv
python3.12 -m venv venv
if [ "${install_dev}" = true ]; then
    venv/bin/pip3 install -e ".[dev]"
else
    venv/bin/pip3 install -e .
fi
