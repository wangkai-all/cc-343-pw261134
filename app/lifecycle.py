"""Domain lifecycle: the reception filter, takedown, cancellation and eviction.

Design decision (see README "为什么是接收后过滤而不是任务取消")
----------------------------------------------------------------
Cancellation is only an *optimization*. A task can be mid-download or have
finished downloading in the same instant the revocation arrives, so no
cancel signal can guarantee the revoked field never reaches storage. The
correctness anchor is therefore the ingestion boundary: :func:`ingest_package`
re-reads the license ledger for every field at the moment of ingestion and
drops anything no longer permitted. Cancellation simply saves bandwidth for
queued tasks whose entire requested scope is already revoked.
"""
from __future__ import annotations

import json
import os
import time
import uuid

from . import audit, config, db

ACTIVE_TASK_STATES = ("QUEUED", "DOWNLOADING", "INGESTING")


# --------------------------------------------------------------------------
# License lookups
# --------------------------------------------------------------------------
def current_license(field: str) -> dict | None:
    row = db.query_one("SELECT * FROM license_ledger WHERE field = ?", (field,))
    return dict(row) if row else None


def is_permitted(field: str, at_time: float) -> tuple[bool, dict | None]:
    """Return (permitted, ledger_row).

    A revocation whose ``effective_time`` is still in the future does not bite
    yet. Absence of any ledger entry means the field is still licensed.
    """
    row = current_license(field)
    if row is None or row["permitted"]:
        return True, row
    if row["effective_time"] > at_time:
        return True, row
    return False, row


# --------------------------------------------------------------------------
# Task creation / leasing (used by the API and the worker)
# --------------------------------------------------------------------------
def create_task(package_id: str, requested_fields: list[str]) -> dict:
    task_uid = f"task-{uuid.uuid4().hex[:10]}"
    now = time.time()
    cur = db.execute(
        """INSERT INTO tasks(task_uid, package_id, requested_fields, status, created_at)
           VALUES (?,?,?,'QUEUED',?)""",
        (task_uid, package_id, json.dumps(requested_fields), now),
    )
    task_id = cur.lastrowid
    audit.append_audit("api", "task.created",
                       {"task_id": task_id, "task_uid": task_uid,
                        "package_id": package_id, "requested_fields": requested_fields})
    db.log_event("INFO", "api", f"created {task_uid} for {package_id} fields={requested_fields}")
    return {"task_id": task_id, "task_uid": task_uid}


def lease_next_task() -> dict | None:
    """Atomically claim one QUEUED task. Cross-process safe via BEGIN IMMEDIATE."""
    conn = db.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM tasks WHERE status = 'QUEUED' ORDER BY id LIMIT 1"
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return None
        conn.execute(
            "UPDATE tasks SET status='DOWNLOADING', leased_at=? WHERE id=?",
            (time.time(), row["id"]),
        )
        conn.execute("COMMIT")
        return dict(row)
    except Exception:
        conn.execute("ROLLBACK")
        raise


def cancel_task(task_id: int, reason: str, actor: str = "api") -> dict:
    row = db.query_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
    if row is None:
        raise LookupError(task_id)
    if row["status"] != "QUEUED":
        raise ValueError(f"only QUEUED tasks can be cancelled (is {row['status']})")
    db.execute(
        "UPDATE tasks SET status='CANCELLED', cancel_reason=?, finished_at=? WHERE id=?",
        (reason, time.time(), task_id),
    )
    audit.append_audit(actor, "task.cancelled",
                       {"task_id": task_id, "task_uid": row["task_uid"], "reason": reason})
    return {"task_id": task_id, "status": "CANCELLED"}


# --------------------------------------------------------------------------
# Revocation orchestration (called by the license service)
# --------------------------------------------------------------------------
def cancel_fully_revoked_queued_tasks(trigger_field: str) -> list[dict]:
    """Cancel QUEUED tasks whose *entire* requested scope is now revoked.

    A task that still has at least one licensed field must run — the reception
    filter will strip the revoked field while preserving the rest.
    """
    cancelled = []
    for row in db.query("SELECT * FROM tasks WHERE status = 'QUEUED' ORDER BY id"):
        fields = json.loads(row["requested_fields"])
        now = time.time()
        if fields and all(not is_permitted(f, now)[0] for f in fields):
            reason = (f"all requested fields revoked "
                      f"(triggered by revocation of '{trigger_field}')")
            cancel_task(row["id"], reason, actor="license-service")
            cancelled.append({"task_id": row["id"], "reason": reason})
    return cancelled


def take_down_published_field(field: str, notification_id: str) -> dict:
    """Withdraw every published result for a revoked field.

    Deletes the distributable copy (``results.value``) but keeps the row, the
    value hash and all lineage. The underlying raw package is sealed first so
    the evidence survives cache eviction.
    """
    published = db.query(
        "SELECT * FROM results WHERE field = ? AND status = 'published'", (field,)
    )
    package_ids = sorted({r["package_id"] for r in published})
    for pkg_id in package_ids:
        pkg = db.query_one("SELECT * FROM packages WHERE id = ?", (pkg_id,))
        if pkg and pkg["cache_path"] and os.path.exists(pkg["cache_path"]):
            audit.seal_package(pkg_id, pkg["cache_path"],
                               reason=f"revocation of '{field}' ({notification_id})")
    now = time.time()
    for r in published:
        db.execute(
            """UPDATE results SET status='withdrawn', value=NULL,
                   filtered_reason=?, updated_at=? WHERE id=?""",
            (f"withdrawn after revocation ({notification_id})", now, r["id"]),
        )
    if published:
        audit.append_audit("license-service", "results.withdrawn",
                           {"field": field, "notification_id": notification_id,
                            "result_ids": [r["id"] for r in published],
                            "packages": package_ids})
        db.log_event("WARN", "license",
                     f"withdrew {len(published)} published result(s) for '{field}'")
    return {"withdrawn": len(published), "packages_sealed": package_ids}


def reapply_reception_filter(field: str, notification_id: str) -> int:
    """Idempotent catch-up for a worker that was offline during revocation."""
    outcome = take_down_published_field(field, notification_id)
    return outcome["withdrawn"]


# --------------------------------------------------------------------------
# The ingestion boundary (worker consumer side)
# --------------------------------------------------------------------------
def _field_values(package: dict, field: str) -> list:
    return [r["fields"][field] for r in package["records"] if field in r["fields"]]


def record_download(task: dict, package_bytes: bytes) -> dict:
    """Persist the raw package into the cache (worker executor side)."""
    config.RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    package = json.loads(package_bytes.decode("utf-8"))
    pkg_id = package["id"]
    digest = audit.sha256_bytes(package_bytes)
    cache_path = config.RAW_CACHE_DIR / f"{pkg_id}.json"
    if not cache_path.exists():  # another task may already have fetched it
        with open(cache_path, "wb") as fh:
            fh.write(package_bytes)
    now = time.time()
    db.execute(
        """INSERT INTO packages(id, content_hash, cache_path, sealed_path,
                                size_bytes, downloaded_at, task_id)
           VALUES (?,?,?,?,?,?,?)
           ON CONFLICT(id) DO UPDATE SET cache_path=excluded.cache_path,
                                         task_id=excluded.task_id""",
        (pkg_id, digest, str(cache_path), None, len(package_bytes), now, task["id"]),
    )
    db.execute("UPDATE tasks SET downloaded_at=? WHERE id=?", (now, task["id"]))
    audit.append_audit("worker", "package.downloaded",
                       {"task_id": task["id"], "package_id": pkg_id,
                        "sha256": digest, "size_bytes": len(package_bytes),
                        "cache_path": str(cache_path)})
    return {"package_id": pkg_id, "sha256": digest, "cache_path": str(cache_path)}


def ingest_package(task_id: int) -> dict:
    """Consumer boundary: decide per field what may be published.

    This is the authoritative enforcement point — the license ledger is read
    *here*, not when the task was created, so a revocation arriving while the
    download was in flight is always honored.
    """
    conn = db.get_conn()
    task = db.query_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
    if task is None:
        raise LookupError(task_id)
    if task["status"] not in ("DOWNLOADING", "INGESTING"):
        raise ValueError(f"task {task_id} not awaiting ingestion (status={task['status']})")
    pkg_row = db.query_one("SELECT * FROM packages WHERE task_id = ?", (task_id,))
    if pkg_row is None or not pkg_row["cache_path"]:
        raise ValueError(f"no cached package for task {task_id}")
    with open(pkg_row["cache_path"], "rb") as fh:
        package_bytes = fh.read()
    package = json.loads(package_bytes.decode("utf-8"))
    requested = json.loads(task["requested_fields"])
    now = time.time()

    conn.execute("BEGIN IMMEDIATE")
    try:
        db.execute("UPDATE tasks SET status='INGESTING' WHERE id=?", (task_id,))
        decisions = []
        any_published = False
        for field in requested:
            values = _field_values(package, field)
            if not values:
                decisions.append({"field": field, "decision": "absent", "reason": "not in package"})
                continue
            raw_json = json.dumps(values, sort_keys=True, ensure_ascii=False)
            value_hash = audit.sha256_bytes(raw_json.encode("utf-8"))
            # Evidence row for every field the package actually carries —
            # lineage works even for fields we never publish.
            db.execute(
                """INSERT INTO package_fields(package_id, field, value_hash)
                   VALUES (?,?,?) ON CONFLICT(package_id, field) DO NOTHING""",
                (package["id"], field, value_hash),
            )
            permitted, ledger = is_permitted(field, now)
            if permitted:
                status, value, reason = "published", raw_json, None
                any_published = True
            else:
                nid = ledger["notification_id"] if ledger else "unknown"
                status = "filtered"
                value = None
                reason = (f"reception filter: '{field}' revoked at "
                          f"{ledger['effective_time']:.3f} ({nid})")
            db.execute(
                """INSERT INTO results(task_id, package_id, field, status, value,
                                      value_hash, filtered_reason, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (task_id, package["id"], field, status, value, value_hash,
                 reason, now, now),
            )
            decisions.append({"field": field, "decision": status, "reason": reason})
        db.execute(
            "UPDATE tasks SET status=?, ingested_at=?, finished_at=? WHERE id=?",
            ("PUBLISHED" if any_published else "FILTERED", now, now, task_id),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    # If anything was filtered, seal the raw package as audit evidence.
    filtered = [d for d in decisions if d["decision"] == "filtered"]
    sealed = None
    if filtered:
        sealed = audit.seal_package(
            package["id"], pkg_row["cache_path"],
            reason="reception filter: " + ", ".join(d["field"] for d in filtered),
        )
    audit.append_audit("worker", "package.ingested",
                       {"task_id": task_id, "package_id": package["id"],
                        "decisions": decisions, "sealed": sealed is not None})
    db.log_event("INFO", "worker",
                 f"task {task_id} ingested: "
                 f"{sum(d['decision'] == 'published' for d in decisions)} published, "
                 f"{len(filtered)} filtered")
    return {"task_id": task_id, "decisions": decisions, "sealed": sealed}


# --------------------------------------------------------------------------
# Cache eviction — never touches sealed evidence
# --------------------------------------------------------------------------
def evict_cache() -> dict:
    """Delete distributable raw-cache copies.

    Hard guarantees:
      * only files under ``runtime/data/raw_cache/`` are ever removed;
      * packages still referenced by an active task are kept;
      * ``runtime/data/sealed/`` is not referenced here at all, so sealed
        audit evidence cannot be deleted by cache cleanup by construction.
    """
    evicted, skipped, missing = [], [], []
    for pkg in db.query("SELECT * FROM packages ORDER BY id"):
        if not pkg["cache_path"]:
            continue
        task = db.query_one("SELECT * FROM tasks WHERE id = ?", (pkg["task_id"],))
        if task and task["status"] in ACTIVE_TASK_STATES:
            skipped.append({"package_id": pkg["id"], "reason": f"task {task['status']}"})
            continue
        # Defense in depth: refuse anything that resolves outside raw_cache.
        path = os.path.realpath(pkg["cache_path"])
        if not path.startswith(os.path.realpath(config.RAW_CACHE_DIR) + os.sep):
            skipped.append({"package_id": pkg["id"], "reason": "path outside raw_cache"})
            continue
        if os.path.exists(path):
            os.remove(path)
        else:
            missing.append(pkg["id"])
        db.execute("UPDATE packages SET cache_path=NULL WHERE id=?", (pkg["id"],))
        evicted.append({"package_id": pkg["id"], "path": path,
                        "sealed_evidence_preserved": bool(pkg["sealed_path"])})
        audit.append_audit("api", "cache.evicted",
                           {"package_id": pkg["id"], "path": path,
                            "sealed_path": pkg["sealed_path"]})
    db.log_event("INFO", "api", f"cache eviction: {len(evicted)} removed, {len(skipped)} kept")
    return {"evicted": evicted, "skipped": skipped, "missing": missing}


# --------------------------------------------------------------------------
# Field lineage: which packages / tasks / results mention a field
# --------------------------------------------------------------------------
def field_lineage(field: str) -> dict:
    ledger = current_license(field)
    notifications = [dict(r) for r in db.query(
        "SELECT * FROM notifications WHERE field=? ORDER BY seq", (field,))]
    packages = [dict(r) for r in db.query(
        """SELECT pf.package_id, pf.value_hash, p.content_hash, p.cache_path,
                  p.sealed_path, p.task_id
           FROM package_fields pf JOIN packages p ON p.id = pf.package_id
           WHERE pf.field = ? ORDER BY p.downloaded_at""", (field,))]
    results = [dict(r) for r in db.query(
        """SELECT r.id, r.task_id, r.package_id, r.field, r.status, r.value,
                  r.value_hash, r.filtered_reason, r.created_at, r.updated_at,
                  t.task_uid
           FROM results r JOIN tasks t ON t.id = r.task_id
           WHERE r.field = ? ORDER BY r.id""", (field,))]
    task_ids = sorted({r["task_id"] for r in results} |
                      {p["task_id"] for p in packages if p["task_id"] is not None})
    tasks = [dict(r) for r in db.query(
        "SELECT * FROM tasks WHERE id IN (%s) ORDER BY id"
        % ",".join("?" * len(task_ids)), task_ids)] if task_ids else []
    return {"field": field, "license": ledger, "notifications": notifications,
            "packages": packages, "tasks": tasks, "results": results}
