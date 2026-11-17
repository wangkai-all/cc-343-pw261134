#!/usr/bin/env bash
# Stop a stack started by run.sh (plus any lingering worker child).
set -euo pipefail
cd "$(dirname "$0")"
RUNTIME_DIR="${RUNTIME_DIR:-$(pwd)/runtime}"

for name in web supplier; do
  pidfile="$RUNTIME_DIR/$name.pid"
  if [[ -f "$pidfile" ]]; then
    pid="$(cat "$pidfile")"
    kill "$pid" 2>/dev/null && echo "[stop] $name ($pid) terminated" || true
    rm -f "$pidfile"
  fi
done
pkill -f "app.worker" 2>/dev/null && echo "[stop] worker terminated" || true
