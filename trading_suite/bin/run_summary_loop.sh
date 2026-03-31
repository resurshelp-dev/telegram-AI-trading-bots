#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SUITE_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
ROOT_DIR=$(cd "$SUITE_DIR/.." && pwd)
PYTHON_BIN="${TRADING_PYTHON:-python3}"

"$SCRIPT_DIR/prepare_runtime.sh" >/dev/null
cd "$ROOT_DIR"

exec "$PYTHON_BIN" "$SUITE_DIR/bin/summary_collector.py" --loop-seconds 60
