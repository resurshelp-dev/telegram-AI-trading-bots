#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SUITE_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
ROOT_DIR=$(cd "$SUITE_DIR/.." && pwd)
PYTHON_BIN="${TRADING_PYTHON:-python3}"

"$SCRIPT_DIR/prepare_runtime.sh" >/dev/null
cd "$ROOT_DIR"

export SVECHI_RUNTIME_ROOT="$SUITE_DIR/runtime/svechi"
export SVECHI_ENV_PATH="$SUITE_DIR/configs/svechi.env"
export SVECHI_ALLOW_FALLBACK_ENV=false

exec "$PYTHON_BIN" "$ROOT_DIR/svechi/svechi_final_automation.py"
