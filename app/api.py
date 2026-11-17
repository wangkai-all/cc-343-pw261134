"""Web process: REST API, static UI hosting, license poller and worker
process supervision.

The worker is a child process spawned here, so "offline" is real: stopping it
kills the OS process (heartbeat goes stale, downloads stop), and restarting it
runs the startup catch-up against any notifications that arrived meanwhile.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from . import audit, config, db, httpclient, license as license_service, lifecycle

WORKER_PROC: subprocess.Popen | None = None
WORKER_LOCK = __import__("threading").Lock()


# --------------------------------------------------------------------------
# Worker process supervision
# --------------------------------------------------------------------------
def worker_status() -> dict:
    hb = db.query_one("SELECT * FROM worker_heartbeat WHERE id=1")
    now = time.time()
    if hb is None or hb["ts"] is None:
        return {"state": "stopped", "pid": None, "suspended": False,
                "last_heartbeat": None, "seconds_since_heartbeat": None}
    age = now - hb["ts"]
    if hb["suspended"]:
        state = "suspended"
    elif age <= config.WORKER_OFFLINE_AFTER:
        state = "online"
    else:
        state = "offline"
    return {"state": state, "pid": hb["worker_pid"],
            "suspended": bool(hb["suspended"]),
            "last_heartbeat": hb["ts"], "seconds_since_heartbeat": round(age, 2),
            "offline_after": config.WORKER_OFFLINE_AFTER}


def start_worker() -> dict:
    global WORKER_PROC
    with WORKER_LOCK:
        if WORKER_PROC is not None and WORKER_PROC.poll() is None:
            return {"ok": False, "reason": "already running", "pid": WORKER_PROC.pid}
        WORKER_PROC = subprocess.Popen(
            [sys.executable, "-I", str(config.ROOT / "scripts" / "run.py"),
             "app.worker"],
            cwd=str(config.ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        db.log_event("INFO", "api", f"worker process started (pid={WORKER_PROC.pid})")
        return {"ok": True, "pid": WORKER_PROC.pid}


def stop_worker() -> dict:
    global WORKER_PROC
    with WORKER_LOCK:
        if WORKER_PROC is None or WORKER_PROC.poll() is not None:
            WORKER_PROC = None
            return {"ok": False, "reason": "not running"}
        pid = WORKER_PROC.pid
        WORKER_PROC.terminate()  # graceful SIGTERM; worker flips its stop event
        try:
            WORKER_PROC.wait(timeout=5)
        except subprocess.TimeoutExpired:
            WORKER_PROC.kill()
            WORKER_PROC.wait(timeout=5)
        WORKER_PROC = None
        db.log_event("WARN", "api", f"worker process stopped (pid={pid})")
        return {"ok": True, "pid": pid}


def worker_control(action: str) -> tuple[int, dict]:
    return httpclient.request(
        "POST", f"http://127.0.0.1:{config.WORKER_CONTROL_PORT}/control/{action}",
        body={}, timeout=5,
    )


# --------------------------------------------------------------------------
# Read models
# --------------------------------------------------------------------------
def overview() -> dict:
    fields = {}
    for f in ("email", "phone", "age", "city"):
        row = db.query_one("SELECT * FROM license_ledger WHERE field=?", (f,))
        results = db.query(
            "SELECT status, COUNT(*) c FROM results WHERE field=? GROUP BY status", (f,))
        fields[f] = {
            "license": dict(row) if row else {"field": f, "permitted": 1,
                                              "effective_time": None},
            "result_counts": {r["status"]: r["c"] for r in results},
        }
    tasks = [dict(r) for r in db.query("SELECT * FROM tasks ORDER BY id")]
    packages = [dict(r) for r in db.query("SELECT * FROM packages ORDER BY downloaded_at")]
    seals = audit.list_seals()
    chain = audit.verify_chain()
    return {"fields": fields, "tasks": tasks, "packages": packages,
            "seals": seals, "audit_chain": chain, "worker": worker_status(),
            "supplier": config.SUPPLIER_BASE}


def reset_all() -> dict:
    """Full reset to the seeded state: stop worker, wipe runtime, reset supplier.

    The license poller thread is intentionally left alone: a threading.Event
    that has fired cannot be re-armed to keep an old waiter alive. The poller
    simply keeps polling against the freshly recreated database.
    """
    stop_worker()
    time.sleep(0.2)
    import shutil
    if config.RUNTIME_DIR.exists():
        shutil.rmtree(config.RUNTIME_DIR)
    config.RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    config.SEALED_DIR.mkdir(parents=True, exist_ok=True)
    db.init_db()
    status, body = httpclient.request("POST", f"{config.SUPPLIER_BASE}/admin/reset", body={})
    audit.append_audit("api", "system.reset", {"supplier_reset_status": status})
    start_worker()
    return {"ok": True, "supplier": body}


# --------------------------------------------------------------------------
# HTTP handler
# --------------------------------------------------------------------------
class ApiHandler(BaseHTTPRequestHandler):
    server_version = "PointwiseWeb/1.0"

    def log_message(self, fmt: str, *args) -> None:
        return

    def _send_json(self, status: int, body: object) -> None:
        data = json.dumps(body, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _send_file(self, path: Path, content_type: str) -> None:
        if not path.exists():
            return self._send_json(404, {"error": "not found"})
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    # -- GET ---------------------------------------------------------------
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path, qs = parsed.path, parse_qs(parsed.query)
        try:
            if path == "/api/overview":
                return self._send_json(200, overview())
            if path == "/api/lineage":
                field = qs.get("field", [""])[0]
                if not field:
                    return self._send_json(400, {"error": "field required"})
                return self._send_json(200, lifecycle.field_lineage(field))
            if path == "/api/notifications":
                rows = db.query("SELECT * FROM notifications ORDER BY seq")
                return self._send_json(200, {"notifications": [dict(r) for r in rows]})
            if path == "/api/audit":
                rows = db.query("SELECT * FROM audit_log ORDER BY seq")
                return self._send_json(200, {
                    "entries": [dict(r) for r in rows],
                    "verification": audit.verify_chain(),
                })
            if path == "/api/logs":
                rows = db.query("SELECT * FROM app_log ORDER BY id DESC LIMIT 200")
                return self._send_json(200, {"logs": [dict(r) for r in rows]})
            if path == "/api/supplier/requests":
                status, body = httpclient.request(
                    "GET", f"{config.SUPPLIER_BASE}/admin/requests", timeout=5)
                return self._send_json(status, body)
            if path == "/api/supplier/packages":
                status, body = httpclient.request(
                    "GET", f"{config.SUPPLIER_BASE}/packages", timeout=5)
                return self._send_json(status, body)
            if path == "/api/worker":
                return self._send_json(200, worker_status())
            if path in ("/", "/index.html"):
                return self._send_file(config.STATIC_DIR / "index.html", "text/html; charset=utf-8")
            if path == "/app.js":
                return self._send_file(config.STATIC_DIR / "app.js",
                                       "application/javascript; charset=utf-8")
            if path == "/styles.css":
                return self._send_file(config.STATIC_DIR / "styles.css", "text/css; charset=utf-8")
            return self._send_json(404, {"error": "not found"})
        except Exception as exc:
            db.log_event("ERROR", "api", f"GET {path} failed: {exc}")
            return self._send_json(500, {"error": str(exc)})

    # -- POST --------------------------------------------------------------
    def do_POST(self) -> None:
        path = urlparse(self.path).path
        try:
            body = self._body()
            if path == "/api/tasks":
                task = lifecycle.create_task(body["package_id"], list(body["requested_fields"]))
                return self._send_json(201, task)
            if path == "/api/tasks/cancel":
                result = lifecycle.cancel_task(int(body["task_id"]),
                                               body.get("reason", "manual cancel"))
                return self._send_json(200, result)
            if path == "/api/revoke":
                status, resp = httpclient.request(
                    "POST", f"{config.SUPPLIER_BASE}/admin/revoke",
                    body={"field": body["field"],
                          "effective_in": float(body.get("effective_in", 0.0))},
                    timeout=5)
                return self._send_json(status, resp)
            if path == "/api/restore":
                status, resp = httpclient.request(
                    "POST", f"{config.SUPPLIER_BASE}/admin/restore",
                    body={"field": body["field"]}, timeout=5)
                return self._send_json(status, resp)
            if path == "/api/redeliver":
                status, resp = httpclient.request(
                    "POST", f"{config.SUPPLIER_BASE}/notifications/"
                            f"{int(body['seq'])}/redeliver", body={}, timeout=5)
                return self._send_json(status, resp)
            if path == "/api/poll":
                return self._send_json(200, {"ingested": license_service.poll_once()})
            if path == "/api/cache/evict":
                return self._send_json(200, lifecycle.evict_cache())
            if path == "/api/audit/verify":
                return self._send_json(200, audit.verify_chain())
            if path == "/api/worker/start":
                return self._send_json(200, start_worker())
            if path == "/api/worker/stop":
                return self._send_json(200, stop_worker())
            if path == "/api/worker/suspend":
                status, resp = worker_control("suspend")
                return self._send_json(status, resp)
            if path == "/api/worker/resume":
                status, resp = worker_control("resume")
                return self._send_json(status, resp)
            if path == "/api/reset":
                return self._send_json(200, reset_all())
            return self._send_json(404, {"error": "not found"})
        except KeyError as exc:
            return self._send_json(400, {"error": f"missing parameter: {exc.args[0]}"})
        except (ValueError, LookupError) as exc:
            return self._send_json(400, {"error": str(exc)})
        except Exception as exc:
            db.log_event("ERROR", "api", f"POST {path} failed: {exc}")
            return self._send_json(500, {"error": str(exc)})


def run() -> None:
    config.RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    config.SEALED_DIR.mkdir(parents=True, exist_ok=True)
    db.init_db()
    license_service.start_poller()
    start_worker()
    server = ThreadingHTTPServer((config.WEB_HOST, config.WEB_PORT), ApiHandler)
    print(f"[web] UI on http://{config.WEB_HOST}:{config.WEB_PORT}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop_worker()
        license_service.stop_poller()
        server.server_close()


if __name__ == "__main__":
    run()
