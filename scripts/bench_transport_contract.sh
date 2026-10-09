#!/usr/bin/env bash
# Explicit paired experiments replace the historical five-run median summary.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -eq 0 ]]; then
    echo "Usage: $0 --config experiment.json --output /absolute/evidence/directory" >&2
    echo "See scripts/README.md. No implicit builds, retries, or acceptance claims." >&2
    exit 2
fi
exec python3 "$ROOT_DIR/scripts/run_ab.py" "$@"
