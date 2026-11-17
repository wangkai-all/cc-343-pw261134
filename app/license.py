"""License service.

Responsibilities
----------------
* Poll the supplier notification outbox and ingest every notification
  exactly once (dedupe on the stable ``notification_id``; the outbox ``seq``
  only tracks delivery position, so redelivery with a fresh seq is harmless).
* Maintain the license ledger (per-field permitted flag + effective time).
* On revocation, orchestrate the takedown:
    - queued tasks whose *whole* requested scope is revoked  -> CANCELLED
    - already published results carrying the field           -> withdrawn,
      distributable value deleted, raw package sealed first
    - in-flight / downloaded-but-not-ingested work is left alone here; the
      consumer's reception filter (:mod:`app.lifecycle`) is the authoritative
      boundary and re-checks the ledger at ingestion time.
* Survive worker outages: notifications are ingested here regardless, and a
  restarted worker reconciles via :func:`catch_up`.
"""
from __future__ import annotations

import threading
import time

from . import audit, config, db, httpclient, lifecycle

POLLER_STATE = {"stop": threading.Event()}


# --------------------------------------------------------------------------
# Ingestion (idempotent)
# --------------------------------------------------------------------------
def ingest_notification(n: dict) -> dict:
    """Apply one supplier notification. Safe to call repeatedly."""
    conn = db.get_conn()
    now = time.time()
    existing = conn.execute(
        "SELECT notification_id FROM notifications WHERE notification_id = ?",
        (n["notification_id"],),
    ).fetchone()
    if existing:
        # Redelivery: remember the new delivery seq but do not re-apply.
        conn.execute(
            "UPDATE notifications SET seq = ? WHERE notification_id = ?",
            (n["seq"], n["notification_id"]),
        )
        db.log_event("INFO", "license",
                     f"duplicate notification {n['notification_id']} ignored (redelivery)")
        return {"notification_id": n["notification_id"], "applied": False, "reason": "duplicate"}

    conn.execute(
        """INSERT INTO notifications(notification_id, seq, field, permitted,
                                     effective_time, received_at, processed_at)
           VALUES (?,?,?,?,?,?,?)""",
        (n["notification_id"], n["seq"], n["field"], int(n["permitted"]),
         n["effective_time"], now, now),
    )
    conn.execute(
        """INSERT INTO license_ledger(field, permitted, effective_time,
                                      notification_id, updated_at)
           VALUES (?,?,?,?,?)
           ON CONFLICT(field) DO UPDATE SET
               permitted=excluded.permitted,
               effective_time=excluded.effective_time,
               notification_id=excluded.notification_id,
               updated_at=excluded.updated_at""",
        (n["field"], int(n["permitted"]), n["effective_time"],
         n["notification_id"], now),
    )
    audit.append_audit(
        "license-service",
        "license.revoked" if not n["permitted"] else "license.restored",
        {"field": n["field"], "permitted": bool(n["permitted"]),
         "effective_time": n["effective_time"],
         "notification_id": n["notification_id"], "seq": n["seq"]},
    )
    db.log_event("INFO", "license",
                 f"field {n['field']} -> {'PERMITTED' if n['permitted'] else 'REVOKED'} "
                 f"(effective {n['effective_time']:.3f})")

    if not n["permitted"]:
        lifecycle.cancel_fully_revoked_queued_tasks(n["field"])
        lifecycle.take_down_published_field(n["field"], n["notification_id"])
    return {"notification_id": n["notification_id"], "applied": True}


def catch_up() -> dict:
    """Replay every received notification locally.

    Used by a worker that was offline while revocations arrived. Ingestion is
    idempotent, and per-field reception filtering is also idempotent, so this
    is safe to run on every worker startup.
    """
    rows = db.query("SELECT * FROM notifications ORDER BY seq")
    applied_filters = 0
    for row in rows:
        if not row["permitted"]:
            applied_filters += lifecycle.reapply_reception_filter(
                row["field"], row["notification_id"]
            )
    return {"notifications_known": len(rows), "filter_actions": applied_filters}


# --------------------------------------------------------------------------
# Polling thread (runs inside the web process)
# --------------------------------------------------------------------------
def poll_once() -> int:
    last = db.query_one("SELECT MAX(seq) AS m FROM notifications")
    cursor = last["m"] if last and last["m"] is not None else 0
    try:
        status, body = httpclient.request(
            "GET", f"{config.SUPPLIER_BASE}/notifications?cursor={cursor}", timeout=5
        )
    except Exception as exc:  # supplier down: retry next tick
        db.log_event("WARN", "license", f"poll failed: {exc}")
        return 0
    if status != 200:
        return 0
    count = 0
    for n in body.get("notifications", []):
        ingest_notification(n)
        count += 1
    return count


def _poll_loop() -> None:
    while not POLLER_STATE["stop"].wait(config.POLL_INTERVAL_SECONDS):
        try:
            poll_once()
        except Exception as exc:  # never let the thread die silently
            db.log_event("ERROR", "license", f"poll loop error: {exc}")


def start_poller() -> threading.Thread:
    thread = threading.Thread(target=_poll_loop, name="license-poller", daemon=True)
    thread.start()
    return thread


def stop_poller() -> None:
    POLLER_STATE["stop"].set()
