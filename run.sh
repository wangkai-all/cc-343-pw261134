#!/usr/bin/env bash
# Start the full stack: supplier process + web process (which supervises the
# worker child process). Everything binds localhost; ports are env-overridable.
set -euo pipefail
cd "$(dirname "$0")"

export SUPPLIER_PORT="${SUPPLIER_PORT:-8091}"
export WEB_PORT="${WEB_PORT:-8090}"
export WORKER_CONTROL_PORT="${WORKER_CONTROL_PORT:-8092}"
export RUNTIME_DIR="${RUNTIME_DIR:-$(pwd)/runtime}"

mkdir -p "$RUNTIME_DIR"

echo "[start] supplier on :$SUPPLIER_PORT, web on :$WEB_PORT, runtime=$RUNTIME_DIR"
python3 -I scripts/run.py app.supplier &
SUPPLIER_PID=$!
python3 -I scripts/run.py app.api &
WEB_PID=$!

echo "$SUPPLIER_PID" > "$RUNTIME_DIR/supplier.pid"
echo "$WEB_PID" > "$RUNTIME_DIR/web.pid"

trap 'echo; echo "[start] shutting down…"; kill $WEB_PID $SUPPLIER_PID 2>/dev/null || true' INT TERM
echo "[start] open http://127.0.0.1:$WEB_PORT  (Ctrl-C stops everything)"
wait
