"""Collection worker — a separate OS process so it can genuinely go offline.

Two loops share the SQLite state:

* executor loop: leases QUEUED tasks, performs a real HTTP GET against the
  supplier, writes the raw package into the cache (raw package cache);
* consumer loop: takes downloaded packages through the ingestion boundary in
  :mod:`app.lifecycle`, where per-field reception filtering happens.

A small HTTP control server (separate port) supports ``suspend`` / ``resume``
so the UI can simulate a worker that is alive but not processing, distinct
from a fully stopped process. Heartbeats let the UI show online/offline.
"""
from __future__ import annotations

import json
import os
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import audit, config, db, httpclient, license as license_service, lifecycle

STOP = threading.Event()
SUSPENDED = threading.Event()


def heartbeat_loop() -> None:
    while not STOP.wait(config.WORKER_HEARTBEAT_SECONDS):
        db.execute(
            """INSERT INTO worker_heartbeat(id, worker_pid, suspended, ts)
               VALUES (1, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET worker_pid=excluded.worker_pid,
                   suspended=excluded.suspended, ts=excluded.ts""",
            (os.getpid(), 1 if SUSPENDED.is_set() else 0, time.time()),
        )


def executor_loop() -> None:
    """Lease queued tasks and download their packages over real HTTP."""
    while not STOP.wait(config.EXECUTOR_POLL_SECONDS):
        if SUSPENDED.is_set():
            continue
        task = lifecycle.lease_next_task()
        if task is None:
            continue
        db.log_event("INFO", "worker",
                     f"leased {task['task_uid']} (task {task['id']}), downloading "
                     f"{task['package_id']} from supplier")
        try:
            status, body = httpclient.request(
                "GET", f"{config.SUPPLIER_BASE}/packages/{task['package_id']}",
                timeout=30,
            )
            if status != 200:
                raise RuntimeError(f"supplier returned {status}: {body}")
            package_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
            lifecycle.record_download(task, package_bytes)
            db.log_event("INFO", "worker",
                         f"task {task['id']}: raw package cached, awaiting consumer")
        except Exception as exc:
            db.execute(
                "UPDATE tasks SET status='FAILED', cancel_reason=?, finished_at=? WHERE id=?",
                (f"download error: {exc}", time.time(), task["id"]),
            )
            audit.append_audit("worker", "task.failed",
                               {"task_id": task["id"], "error": str(exc)})
            db.log_event("ERROR", "worker", f"task {task['id']} failed: {exc}")


def consumer_loop() -> None:
    """Run downloaded packages through the reception filter."""
    while not STOP.wait(config.CONSUMER_POLL_SECONDS):
        if SUSPENDED.is_set():
            continue
        row = db.query_one(
            "SELECT * FROM tasks WHERE status='DOWNLOADING' "
            "AND downloaded_at IS NOT NULL ORDER BY id LIMIT 1"
        )
        if row is None:
            continue
        try:
            lifecycle.ingest_package(row["id"])
        except Exception as exc:
            db.log_event("ERROR", "worker", f"ingest task {row['id']} failed: {exc}")
            audit.append_audit("worker", "ingest.failed",
                               {"task_id": row["id"], "error": str(exc)})


class ControlHandler(BaseHTTPRequestHandler):
    server_version = "WorkerControl/1.0"

    def log_message(self, fmt: str, *args) -> None:
        return

    def _send(self, status: int, body: object) -> None:
        data = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if self.path.startswith("/control"):
            return self._send(200, {"pid": os.getpid(),
                                    "suspended": SUSPENDED.is_set()})
        self._send(404, {"error": "not found"})

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}
        if self.path == "/control/suspend":
            SUSPENDED.set()
            db.execute("UPDATE worker_heartbeat SET suspended=1 WHERE id=1")
            db.log_event("WARN", "worker", "worker suspended via control API")
            return self._send(200, {"ok": True, "suspended": True})
        if self.path == "/control/resume":
            SUSPENDED.clear()
            db.execute("UPDATE worker_heartbeat SET suspended=0 WHERE id=1")
            db.log_event("INFO", "worker", "worker resumed via control API")
            return self._send(200, {"ok": True, "suspended": False})
        self._send(404, {"error": "not found"})


def _shutdown(signum, frame) -> None:
    db.log_event("WARN", "worker", f"received signal {signum}, stopping")
    STOP.set()


def run() -> None:
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    db.init_db()
    # Catch up on anything that happened while this worker was offline.
    summary = license_service.catch_up()
    db.log_event("INFO", "worker",
                 f"worker {os.getpid()} started; catch-up: {summary}")
    db.execute(
        """INSERT INTO worker_heartbeat(id, worker_pid, suspended, ts)
           VALUES (1, ?, 0, ?)
           ON CONFLICT(id) DO UPDATE SET worker_pid=excluded.worker_pid,
               suspended=0, ts=excluded.ts""",
        (os.getpid(), time.time()),
    )

    control = ThreadingHTTPServer(("127.0.0.1", config.WORKER_CONTROL_PORT), ControlHandler)
    threads = [
        threading.Thread(target=heartbeat_loop, name="hb", daemon=True),
        threading.Thread(target=executor_loop, name="executor", daemon=True),
        threading.Thread(target=consumer_loop, name="consumer", daemon=True),
        threading.Thread(target=control.serve_forever, name="control", daemon=True),
    ]
    for t in threads:
        t.start()
    print(f"[worker] pid={os.getpid()} control port={config.WORKER_CONTROL_PORT}", flush=True)
    while not STOP.is_set():
        time.sleep(0.2)
    control.shutdown()


if __name__ == "__main__":
    run()
