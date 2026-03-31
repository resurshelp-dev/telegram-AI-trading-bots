#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SUITE_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
ROOT_DIR=$(cd "$SUITE_DIR/.." && pwd)
PYTHON_BIN="${TRADING_PYTHON:-python3}"

"$SCRIPT_DIR/prepare_runtime.sh" >/dev/null
cd "$ROOT_DIR"

export THREE_BAR_RUNTIME_ROOT="$SUITE_DIR/runtime/3bar"
export THREE_BAR_ENV_PATH="$SUITE_DIR/configs/3bar.env"

exec "$PYTHON_BIN" "$ROOT_DIR/3bar/three_bar_system_daemon.py"
