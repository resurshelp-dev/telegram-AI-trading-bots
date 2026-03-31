#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SUITE_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
RUNTIME_DIR="$SUITE_DIR/runtime"

mkdir -p "$RUNTIME_DIR/correction/logs" "$RUNTIME_DIR/correction/state" "$RUNTIME_DIR/correction/reports"
mkdir -p "$RUNTIME_DIR/3bar/logs" "$RUNTIME_DIR/3bar/state" "$RUNTIME_DIR/3bar/reports"
mkdir -p "$RUNTIME_DIR/svechi/logs" "$RUNTIME_DIR/svechi/reports"
mkdir -p "$RUNTIME_DIR/kaktak"
mkdir -p "$RUNTIME_DIR/fixed/logs" "$RUNTIME_DIR/fixed/state" "$RUNTIME_DIR/fixed/reports"
mkdir -p "$RUNTIME_DIR/summary"

echo "runtime_prepared=$RUNTIME_DIR"
