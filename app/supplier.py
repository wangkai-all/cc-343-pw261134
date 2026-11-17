"""The supplier: a controllable local source speaking real HTTP.

Endpoints
---------
GET  /health
GET  /packages                       -> list package metadata + fields
GET  /packages/{id}                  -> the raw package (latency is simulated)
POST /admin/revoke                   {field, effective_in} issue a revocation
POST /admin/restore                  {field}                 re-license a field
GET  /notifications?cursor=N         -> outbox poll (at-least-once delivery)
POST /notifications/{seq}/redeliver  -> re-emit one notification with a new seq
GET  /admin/requests                 -> every request this process has served
POST /admin/reset                    -> back to the seeded state

Notifications carry a stable ``notification_id`` (per field + version) and an
outbox ``seq`` that changes on redelivery, so consumers can dedupe by
notification_id while tracking delivery position by seq.
"""
from __future__ import annotations

import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import config

# Deterministic seed data. Values are structured records; the collector stores
# whole packages and later decides per field what may be published.
SEED_PACKAGES = {
    "pkg-alpha": {
        "id": "pkg-alpha",
        "issued_at": 1763000000.0,
        "records": [
            {"entity": "cust-1", "fields": {
                "email": "cust-1@example.test",
                "phone": "+1-555-0101",
                "age": 31,
                "city": "Berlin",
            }},
            {"entity": "cust-2", "fields": {
                "email": "cust-2@example.test",
                "phone": "+1-555-0102",
                "age": 47,
                "city": "Lisbon",
            }},
        ],
    },
    "pkg-bravo": {
        "id": "pkg-bravo",
        "issued_at": 1763000100.0,
        "records": [
            {"entity": "cust-3", "fields": {
                "email": "cust-3@example.test",
                "phone": "+1-555-0103",
                "age": 28,
                "city": "Madrid",
            }},
        ],
    },
    "pkg-charlie": {
        "id": "pkg-charlie",
        "issued_at": 1763000200.0,
        "records": [
            {"entity": "cust-4", "fields": {
                "email": "cust-4@example.test",
                "phone": "+1-555-0104",
                "age": 62,
                "city": "Oslo",
            }},
            {"entity": "cust-5", "fields": {
                "email": "cust-5@example.test",
                "phone": "+1-555-0105",
                "age": 35,
                "city": "Oslo",
            }},
        ],
    },
}

ALL_FIELDS = ["email", "phone", "age", "city"]


class SupplierState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        with self.lock:
            self.packages = json.loads(json.dumps(SEED_PACKAGES))
            # field -> {"permitted": bool, "version": int}
            self.licenses = {f: {"permitted": True, "version": 1} for f in ALL_FIELDS}
            self.outbox: list[dict] = []
            self.request_log: list[dict] = []
            self.field_versions = {f: 1 for f in ALL_FIELDS}

    def _emit(self, field: str, permitted: bool, effective_time: float) -> dict:
        self.field_versions[field] += 1
        nid = f"notif-{field}-v{self.field_versions[field]}"
        notification = {
            "notification_id": nid,
            "seq": len(self.outbox) + 1,
            "field": field,
            "permitted": permitted,
            "effective_time": effective_time,
            "issued_at": time.time(),
        }
        self.outbox.append(notification)
        return notification

    def revoke(self, field: str, effective_in: float) -> dict:
        with self.lock:
            if field not in self.licenses:
                raise KeyError(field)
            effective_time = time.time() + max(effective_in, 0.0)
            self.licenses[field] = {"permitted": False, "version": self.field_versions[field] + 1}
            return self._emit(field, False, effective_time)

    def restore(self, field: str) -> dict:
        with self.lock:
            if field not in self.licenses:
                raise KeyError(field)
            effective_time = time.time()
            self.licenses[field] = {"permitted": True, "version": self.field_versions[field] + 1}
            return self._emit(field, True, effective_time)

    def redeliver(self, seq: int) -> dict:
        with self.lock:
            original = next((n for n in self.outbox if n["seq"] == seq), None)
            if original is None:
                raise IndexError(seq)
            redelivered = dict(original)
            redelivered["seq"] = len(self.outbox) + 1
            redelivered["redelivered_from"] = original["seq"]
            redelivered["issued_at"] = time.time()
            self.outbox.append(redelivered)
            return redelivered

    def poll(self, cursor: int) -> list[dict]:
        with self.lock:
            return [dict(n) for n in self.outbox if n["seq"] > cursor]

    def record_request(self, method: str, path: str, status: int, detail: str = "") -> None:
        with self.lock:
            self.request_log.append(
                {"id": len(self.request_log) + 1, "ts": time.time(),
                 "method": method, "path": path, "status_code": status, "detail": detail}
            )


STATE = SupplierState()


class SupplierHandler(BaseHTTPRequestHandler):
    server_version = "PointwiseSupplier/1.0"

    def log_message(self, fmt: str, *args) -> None:  # quiet default stderr logging
        return

    # -- helpers -----------------------------------------------------------
    def _send(self, status: int, body: object = None) -> None:
        data = b"" if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if data:
            self.wfile.write(data)
        STATE.record_request(self.command, self.path, status)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    # -- GET ---------------------------------------------------------------
    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        query = self.path.split("?", 1)[1] if "?" in self.path else ""
        if path == "/health":
            return self._send(200, {"ok": True})
        if path == "/packages":
            with STATE.lock:
                summary = [
                    {"id": p["id"], "issued_at": p["issued_at"],
                     "fields": sorted({f for r in p["records"] for f in r["fields"]})}
                    for p in STATE.packages.values()
                ]
            return self._send(200, {"packages": summary, "licenses": dict(STATE.licenses)})
        if path.startswith("/packages/"):
            pkg_id = path.rsplit("/", 1)[1]
            with STATE.lock:
                pkg = STATE.packages.get(pkg_id)
            if pkg is None:
                return self._send(404, {"error": "unknown package"})
            # Real, configurable latency: the download is genuinely in flight,
            # which is what makes the revoke-during-download race demonstrable.
            time.sleep(config.SUPPLIER_LATENCY_SECONDS)
            return self._send(200, pkg)
        if path == "/notifications":
            cursor = 0
            for part in query.split("&"):
                if part.startswith("cursor="):
                    cursor = int(part.split("=", 1)[1])
            return self._send(200, {"notifications": STATE.poll(cursor)})
        if path == "/admin/requests":
            with STATE.lock:
                return self._send(200, {"requests": list(STATE.request_log)})
        return self._send(404, {"error": "not found"})

    # -- POST --------------------------------------------------------------
    def do_POST(self) -> None:
        path = self.path.split("?", 1)[0]
        try:
            body = self._body()
            if path == "/admin/revoke":
                notification = STATE.revoke(body["field"], float(body.get("effective_in", 0.0)))
                return self._send(200, {"ok": True, "notification": notification})
            if path == "/admin/restore":
                notification = STATE.restore(body["field"])
                return self._send(200, {"ok": True, "notification": notification})
            if path.startswith("/notifications/") and path.endswith("/redeliver"):
                seq = int(path.split("/")[2])
                return self._send(200, {"ok": True, "notification": STATE.redeliver(seq)})
            if path == "/admin/reset":
                STATE.reset()
                return self._send(200, {"ok": True})
            return self._send(404, {"error": "not found"})
        except KeyError as exc:
            return self._send(400, {"error": f"unknown field: {exc.args[0]}"})
        except IndexError as exc:
            return self._send(404, {"error": f"unknown seq: {exc.args[0]}"})
        except (json.JSONDecodeError, ValueError) as exc:
            return self._send(400, {"error": f"bad request: {exc}"})


def run() -> None:
    server = ThreadingHTTPServer((config.SUPPLIER_HOST, config.SUPPLIER_PORT), SupplierHandler)
    print(f"[supplier] listening on {config.SUPPLIER_HOST}:{config.SUPPLIER_PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    run()
