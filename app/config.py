"""Central configuration. Every value can be overridden by an env var so the
local source (ports, directories, timings) stays fully controllable."""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUNTIME_DIR = Path(os.environ.get("RUNTIME_DIR", ROOT / "runtime"))
DATA_DIR = RUNTIME_DIR / "data"          # raw package cache + sealed evidence
RAW_CACHE_DIR = DATA_DIR / "raw_cache"
SEALED_DIR = DATA_DIR / "sealed"         # append-only audit evidence
DB_PATH = RUNTIME_DIR / "state.sqlite3"

SUPPLIER_HOST = os.environ.get("SUPPLIER_HOST", "127.0.0.1")
SUPPLIER_PORT = int(os.environ.get("SUPPLIER_PORT", "8091"))
SUPPLIER_BASE = f"http://{SUPPLIER_HOST}:{SUPPLIER_PORT}"

WEB_HOST = os.environ.get("WEB_HOST", "127.0.0.1")
WEB_PORT = int(os.environ.get("WEB_PORT", "8090"))

# Control endpoint the web process uses to suspend/resume the worker process.
WORKER_CONTROL_PORT = int(os.environ.get("WORKER_CONTROL_PORT", "8092"))
WORKER_HEARTBEAT_SECONDS = float(os.environ.get("WORKER_HEARTBEAT_SECONDS", "2"))
# A worker whose heartbeat is older than this is considered offline.
WORKER_OFFLINE_AFTER = float(os.environ.get("WORKER_OFFLINE_AFTER", "6"))

# How long the license service waits between notification pulls.
POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "0.5"))
# Worker poll intervals.
EXECUTOR_POLL_SECONDS = float(os.environ.get("EXECUTOR_POLL_SECONDS", "0.3"))
CONSUMER_POLL_SECONDS = float(os.environ.get("CONSUMER_POLL_SECONDS", "0.3"))

# Simulated supplier latency, so tests/demos can widen the revoke race window.
SUPPLIER_LATENCY_SECONDS = float(os.environ.get("SUPPLIER_LATENCY_SECONDS", "1.2"))

# Directory of the web UI static files.
STATIC_DIR = ROOT / "web"
