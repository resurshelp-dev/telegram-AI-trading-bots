#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SUITE_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
ROOT_DIR=$(cd "$SUITE_DIR/.." && pwd)
PYTHON_BIN="${TRADING_PYTHON:-python3}"

"$SCRIPT_DIR/prepare_runtime.sh" >/dev/null
cd "$ROOT_DIR"

export KAKTAK_RUNTIME_ROOT="$SUITE_DIR/runtime/kaktak"
export KAKTAK_CONFIG_PATH="$SUITE_DIR/configs/kaktak.bot_config.json"

exec "$PYTHON_BIN" "$ROOT_DIR/kaktak/contrarian_bot.py" --config "$SUITE_DIR/configs/kaktak.bot_config.json" --mode live --paper true
