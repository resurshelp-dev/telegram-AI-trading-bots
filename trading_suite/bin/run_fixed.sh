#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SUITE_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
ROOT_DIR=$(cd "$SUITE_DIR/.." && pwd)
PYTHON_BIN="${TRADING_PYTHON:-python3}"

"$SCRIPT_DIR/prepare_runtime.sh" >/dev/null
cd "$ROOT_DIR"

export FIXED_RUNTIME_ROOT="$SUITE_DIR/runtime/fixed"
export FIXED_ENV_PATH="$SUITE_DIR/configs/fixed.env"

exec "$PYTHON_BIN" "$ROOT_DIR/fixed/contrarian_bot.py" --mode live --paper true
