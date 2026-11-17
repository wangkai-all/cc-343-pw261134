"""Sealed audit evidence.

Two mechanisms:

1. A hash-chained append-only ``audit_log`` table. Every entry stores the
   previous entry's hash, so any tampering (including row deletion) breaks the
   chain and is detected by :func:`verify_chain`.
2. Sealed raw-package copies under ``runtime/data/sealed/``. That directory is
   append-only: the application never deletes from it, and cache eviction
   (:mod:`app.lifecycle`) is physically incapable of touching it.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import time
import uuid
from pathlib import Path
from typing import Any

from . import config, db

GENESIS_HASH = "0" * 64


def _canonical(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def append_audit(actor: str, action: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Append one entry to the hash chain. Returns the stored entry."""
    conn = db.get_conn()
    entry_id = uuid.uuid4().hex
    ts = time.time()
    prev = conn.execute("SELECT entry_hash FROM audit_log ORDER BY seq DESC LIMIT 1").fetchone()
    prev_hash = prev["entry_hash"] if prev else GENESIS_HASH
    body = {
        "entry_id": entry_id,
        "ts": ts,
        "actor": actor,
        "action": action,
        "payload": payload,
        "prev_hash": prev_hash,
    }
    entry_hash = sha256_bytes(_canonical(body).encode("utf-8"))
    payload_json = json.dumps(payload, ensure_ascii=False)
    conn.execute(
        """INSERT INTO audit_log(entry_id, ts, actor, action, payload_json,
                                 prev_hash, entry_hash)
           VALUES (?,?,?,?,?,?,?)""",
        (entry_id, ts, actor, action, payload_json, prev_hash, entry_hash),
    )
    body["entry_hash"] = entry_hash
    return body


def verify_chain() -> dict[str, Any]:
    """Recompute every hash and report the first broken link, if any."""
    rows = db.query("SELECT * FROM audit_log ORDER BY seq")
    prev_hash = GENESIS_HASH
    for row in rows:
        payload = json.loads(row["payload_json"])
        body = {
            "entry_id": row["entry_id"],
            "ts": row["ts"],
            "actor": row["actor"],
            "action": row["action"],
            "payload": payload,
            "prev_hash": prev_hash,
        }
        expected = sha256_bytes(_canonical(body).encode("utf-8"))
        if row["prev_hash"] != prev_hash:
            return {
                "ok": False,
                "broken_at_seq": row["seq"],
                "reason": "prev_hash mismatch (row deleted or inserted out of band)",
            }
        if row["entry_hash"] != expected:
            return {
                "ok": False,
                "broken_at_seq": row["seq"],
                "reason": "entry_hash mismatch (payload tampered with)",
            }
        prev_hash = row["entry_hash"]
    return {"ok": True, "entries": len(rows), "tip_hash": prev_hash}


def seal_package(package_id: str, source_cache_path: str | Path, reason: str) -> dict[str, Any]:
    """Copy a raw package into the append-only sealed store and record it.

    Idempotent per package: a second call for the same package returns the
    existing seal instead of duplicating evidence.
    """
    existing = db.query_one("SELECT * FROM seals WHERE package_id = ?", (package_id,))
    if existing:
        return {
            "package_id": package_id,
            "sealed_path": existing["sealed_path"],
            "sha256": existing["sha256"],
            "reason": existing["reason"],
            "already_sealed": True,
        }

    config.SEALED_DIR.mkdir(parents=True, exist_ok=True)
    dest = config.SEALED_DIR / f"{package_id}.sealed.json"
    shutil.copy2(source_cache_path, dest)
    digest = sha256_file(dest)
    ts = time.time()
    db.execute(
        """INSERT INTO seals(package_id, sealed_path, sha256, reason, created_at)
           VALUES (?,?,?,?,?)""",
        (package_id, str(dest), digest, reason, ts),
    )
    db.execute("UPDATE packages SET sealed_path = ? WHERE id = ?", (str(dest), package_id))
    append_audit(
        "audit",
        "evidence.sealed",
        {
            "package_id": package_id,
            "sealed_path": str(dest),
            "sha256": digest,
            "reason": reason,
        },
    )
    return {
        "package_id": package_id,
        "sealed_path": str(dest),
        "sha256": digest,
        "reason": reason,
        "already_sealed": False,
    }


def list_seals() -> list[dict[str, Any]]:
    return [dict(r) for r in db.query("SELECT * FROM seals ORDER BY id")]
