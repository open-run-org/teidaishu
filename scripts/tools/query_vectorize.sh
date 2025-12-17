#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PY="$ROOT_DIR/.venv/bin/python3"
[[ -x "$PY" ]] || PY="python3"

exec "$PY" "$ROOT_DIR/apps/reddit/index/cmd/query/main.py" "$@"
