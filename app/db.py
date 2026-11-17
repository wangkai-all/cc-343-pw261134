"""SQLite storage shared by the web process and the worker process.

WAL mode + a busy timeout make cross-process access safe; every connection is
thread-local. All state the demo needs lives here so a reset is simply
deleting the runtime directory.
"""
from __future__ import annotations

import sqlite3
import threading
from typing import Any, Iterable, Sequence

from . import config

_local = threading.local()


def get_conn() -> sqlite3.Connection:
    conn = getattr(_local, "conn", None)
    if conn is None:
        config.RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            config.DB_PATH, timeout=30, isolation_level=None, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        _local.conn = conn
    return conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS license_ledger (
    field          TEXT PRIMARY KEY,
    permitted      INTEGER NOT NULL,           -- 1 = licensed, 0 = revoked
    effective_time REAL NOT NULL,              -- unix ts when the state applies
    notification_id TEXT,                      -- last notification that changed it
    updated_at     REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS notifications (
    notification_id TEXT PRIMARY KEY,          -- stable id, survives redelivery
    seq             INTEGER NOT NULL,          -- outbox sequence (new per redelivery)
    field           TEXT NOT NULL,
    permitted       INTEGER NOT NULL,
    effective_time  REAL NOT NULL,
    received_at     REAL NOT NULL,
    processed_at    REAL
);

CREATE TABLE IF NOT EXISTS tasks (
    id              INTEGER PRIMARY KEY,
    task_uid        TEXT UNIQUE NOT NULL,
    package_id      TEXT NOT NULL,
    requested_fields TEXT NOT NULL,            -- JSON array
    status          TEXT NOT NULL,             -- see TASK_STATUSES
    cancel_reason   TEXT,
    created_at      REAL NOT NULL,
    leased_at       REAL,
    downloaded_at   REAL,
    ingested_at     REAL,
    finished_at     REAL
);

CREATE TABLE IF NOT EXISTS packages (
    id            TEXT PRIMARY KEY,
    content_hash  TEXT NOT NULL,
    cache_path    TEXT,                        -- NULL after eviction
    sealed_path   TEXT,                        -- set once evidence is sealed
    size_bytes    INTEGER NOT NULL,
    downloaded_at REAL NOT NULL,
    task_id       INTEGER
);

CREATE TABLE IF NOT EXISTS package_fields (
    package_id TEXT NOT NULL,
    field      TEXT NOT NULL,
    value_hash TEXT NOT NULL,                  -- hash of the raw value (evidence)
    PRIMARY KEY (package_id, field)
);

CREATE TABLE IF NOT EXISTS results (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id       INTEGER NOT NULL,
    package_id    TEXT NOT NULL,
    field         TEXT NOT NULL,
    status        TEXT NOT NULL,              -- published | filtered | withdrawn
    value         TEXT,                        -- distributable copy, NULL if not public
    value_hash    TEXT NOT NULL,
    filtered_reason TEXT,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_id    TEXT NOT NULL,
    ts          REAL NOT NULL,
    actor       TEXT NOT NULL,
    action      TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    prev_hash   TEXT NOT NULL,
    entry_hash  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS seals (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    package_id  TEXT NOT NULL,
    sealed_path TEXT NOT NULL,
    sha256      TEXT NOT NULL,
    reason      TEXT NOT NULL,
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS app_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    level TEXT NOT NULL,
    component TEXT NOT NULL,
    message TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS supplier_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    detail TEXT
);

CREATE TABLE IF NOT EXISTS worker_heartbeat (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    worker_pid INTEGER,
    suspended INTEGER NOT NULL DEFAULT 0,
    ts REAL
);

CREATE INDEX IF NOT EXISTS idx_results_task ON results(task_id);
CREATE INDEX IF NOT EXISTS idx_results_field ON results(field);
CREATE INDEX IF NOT EXISTS idx_results_package ON results(package_id);
CREATE INDEX IF NOT EXISTS idx_notif_nid ON notifications(notification_id);
"""

TASK_STATUSES = {
    "QUEUED",        # waiting to be leased
    "DOWNLOADING",   # leased, HTTP GET in progress / done, awaiting consumer
    "INGESTING",     # consumer is processing the package
    "PUBLISHED",     # finished, at least one field published
    "FILTERED",      # finished, everything filtered/cancelled at reception
    "CANCELLED",     # cancelled while still queued (never downloaded)
    "FAILED",
}


def init_db() -> None:
    conn = get_conn()
    conn.executescript(SCHEMA)


def query(sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
    return get_conn().execute(sql, params).fetchall()


def query_one(sql: str, params: Sequence[Any] = ()) -> sqlite3.Row | None:
    return get_conn().execute(sql, params).fetchone()


def execute(sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
    return get_conn().execute(sql, params)


def log_event(level: str, component: str, message: str) -> None:
    import time

    execute(
        "INSERT INTO app_log(ts, level, component, message) VALUES (?,?,?,?)",
        (time.time(), level, component, message),
    )
