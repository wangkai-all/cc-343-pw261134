"""End-to-end tests for the pointwise revocation stack.

Spawns the supplier and web (which spawns the worker) against an isolated
runtime directory and randomized localhost ports, drives every scenario over
real HTTP, and asserts the safety properties:

  1. normal task -> all requested fields published
  2. revoke during an in-flight download -> nothing revoked gets published
  3. notification redelivery -> applied once (idempotent)
  4. worker offline during revoke -> catch-up filters after restart
  5. fully-revoked queued task -> CANCELLED; partially revoked -> filtered
  6. cache eviction never deletes sealed evidence; audit chain verifies
  7. tampering with the audit table breaks chain verification

Run:  python3 -I tests/test_e2e.py
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(method: str, url: str, body: dict | None = None, timeout: int = 20):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


class Stack:
    def __init__(self) -> None:
        self.runtime = Path(tempfile.mkdtemp(prefix="pointwise-e2e-"))
        self.supplier_port = free_port()
        self.web_port = free_port()
        self.worker_port = free_port()
        self.procs: list[subprocess.Popen] = []

    def start(self) -> None:
        base_env = dict(os.environ,
                        RUNTIME_DIR=str(self.runtime),
                        SUPPLIER_PORT=str(self.supplier_port),
                        WEB_PORT=str(self.web_port),
                        WORKER_CONTROL_PORT=str(self.worker_port),
                        SUPPLIER_LATENCY_SECONDS="1.0",
                        WORKER_OFFLINE_AFTER="3",
                        POLL_INTERVAL_SECONDS="0.2")
        supplier = subprocess.Popen(
            [sys.executable, "-I", str(ROOT / "scripts" / "run.py"),
             "app.supplier"], cwd=ROOT,
            env=base_env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.procs.append(supplier)
        web = subprocess.Popen(
            [sys.executable, "-I", str(ROOT / "scripts" / "run.py"),
             "app.api"], cwd=ROOT,
            env=base_env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.procs.append(web)
        self.web = f"http://127.0.0.1:{self.web_port}"
        self._wait_ready()

    def _wait_ready(self) -> None:
        deadline = time.time() + 25
        while time.time() < deadline:
            try:
                status, _ = http("GET", f"{self.web}/api/overview", timeout=2)
                if status == 200:
                    return
            except Exception:
                pass
            time.sleep(0.3)
        raise RuntimeError("stack did not become ready")

    def stop(self) -> None:
        for proc in reversed(self.procs):
            proc.send_signal(signal.SIGTERM)
        for proc in self.procs:
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
        # Worker is a grandchild spawned by the web process.
        subprocess.run(["pkill", "-f", "app.worker"], capture_output=True)
        shutil.rmtree(self.runtime, ignore_errors=True)


PASS, FAIL = 0, 0


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def wait_task(web: str, task_id: int, timeout: float = 20.0) -> dict:
    deadline = time.time() + timeout
    terminal = {"PUBLISHED", "FILTERED", "CANCELLED", "FAILED"}
    while time.time() < deadline:
        _, ov = http("GET", f"{web}/api/overview")
        task = next((t for t in ov["tasks"] if t["id"] == task_id), None)
        if task and task["status"] in terminal:
            return task
        time.sleep(0.3)
    raise AssertionError(f"task {task_id} never reached a terminal state")


def field_results(web: str, field: str, task_id: int) -> list[dict]:
    _, lin = http("GET", f"{web}/api/lineage?field={field}")
    return [r for r in lin["results"] if r["task_id"] == task_id]


def main() -> int:
    stack = Stack()
    stack.start()
    web = stack.web
    try:
        print("\n[1] 正常采集：请求的字段全部发布")
        _, t = http("POST", f"{web}/api/tasks",
                    {"package_id": "pkg-alpha", "requested_fields": ["email", "age"]})
        task = wait_task(web, t["task_id"])
        check("任务终态 PUBLISHED", task["status"] == "PUBLISHED", task["status"])
        for field in ("email", "age"):
            rows = field_results(web, field, t["task_id"])
            check(f"{field} 已发布且带可分发值",
                  rows and rows[0]["status"] == "published" and rows[0]["value"] is not None)

        print("\n[2] 在途下载竞态：下载刚完成即收回 phone")
        _, t2 = http("POST", f"{web}/api/tasks",
                     {"package_id": "pkg-bravo", "requested_fields": ["phone"]})
        time.sleep(0.3)  # inside the supplier's simulated download latency
        _, rev = http("POST", f"{web}/api/revoke",
                      {"field": "phone", "effective_in": 0})
        check("收回通知被许可服务摄入", "notification" in rev)
        task2 = wait_task(web, t2["task_id"])
        check("任务未被硬杀，由消费者过滤收尾", task2["status"] in ("FILTERED", "PUBLISHED"))
        rows = field_results(web, "phone", t2["task_id"])
        check("phone 无已发布值", rows and all(r["status"] != "published" for r in rows))
        check("phone 值被标记 filtered", rows and all(r["status"] == "filtered" for r in rows))
        _, ov = http("GET", f"{web}/api/overview")
        check("原始包已封存为审计证据",
              any(s["package_id"] == "pkg-bravo" for s in ov["seals"]))
        check("raw cache 中仍有原始包（封存之外可清理）",
              any(p["id"] == "pkg-bravo" and p["cache_path"] for p in ov["packages"]))

        print("\n[3] 通知重发：相同 notification_id 只应用一次")
        _, rev2 = http("POST", f"{web}/api/revoke", {"field": "age", "effective_in": 0})
        time.sleep(0.8)
        _, audit_before = http("GET", f"{web}/api/audit")
        revoke_count_before = sum(1 for e in audit_before["entries"]
                                  if e["action"] == "license.revoked"
                                  and json.loads(e["payload_json"]).get("field") == "age")
        http("POST", f"{web}/api/redeliver", {"seq": rev2["notification"]["seq"]})
        time.sleep(0.8)
        _, audit_after = http("GET", f"{web}/api/audit")
        revoke_count_after = sum(1 for e in audit_after["entries"]
                                 if e["action"] == "license.revoked"
                                 and json.loads(e["payload_json"]).get("field") == "age")
        check("重发不产生第二条 revoke 审计",
              revoke_count_before == revoke_count_after == 1,
              f"{revoke_count_before} -> {revoke_count_after}")
        _, notifs = http("GET", f"{web}/api/notifications")
        ids = [n["notification_id"] for n in notifs["notifications"]]
        check("notification_id 去重（行数不增加）", len(ids) == len(set(ids)))

        print("\n[4] worker 离线期间收回 city，重启后补齐")
        http("POST", f"{web}/api/worker/stop")
        time.sleep(4)  # exceed WORKER_OFFLINE_AFTER
        _, ov = http("GET", f"{web}/api/overview")
        check("worker 显示离线/停止", ov["worker"]["state"] in ("offline", "stopped"),
              ov["worker"]["state"])
        http("POST", f"{web}/api/revoke", {"field": "city", "effective_in": 0})
        _, t3 = http("POST", f"{web}/api/tasks",
                     {"package_id": "pkg-charlie",
                      "requested_fields": ["city", "email"]})
        time.sleep(1.0)  # worker is gone: task must stay queued
        _, ov = http("GET", f"{web}/api/overview")
        queued = next(x for x in ov["tasks"] if x["id"] == t3["task_id"])
        check("离线期间任务保持 QUEUED", queued["status"] == "QUEUED", queued["status"])
        http("POST", f"{web}/api/worker/start")
        task3 = wait_task(web, t3["task_id"], timeout=25)
        check("重启后任务被处理（catch-up 生效）", task3["status"] == "PUBLISHED")
        city_rows = field_results(web, "city", t3["task_id"])
        email_rows = field_results(web, "email", t3["task_id"])
        check("city 被过滤未发布", all(r["status"] == "filtered" for r in city_rows))
        check("email 正常发布", any(r["status"] == "published" for r in email_rows))

        print("\n[5] 全量收回 -> 取消；部分收回 -> 过滤保留")
        # Restore fields revoked by earlier scenarios so this test stays
        # order-independent (also exercises the restore/notification path).
        http("POST", f"{web}/api/restore", {"field": "phone"})
        http("POST", f"{web}/api/restore", {"field": "email"})
        time.sleep(0.6)
        http("POST", f"{web}/api/worker/suspend")
        _, t4 = http("POST", f"{web}/api/tasks",
                     {"package_id": "pkg-alpha", "requested_fields": ["email"]})
        _, t5 = http("POST", f"{web}/api/tasks",
                     {"package_id": "pkg-alpha", "requested_fields": ["email", "phone"]})
        time.sleep(0.5)
        http("POST", f"{web}/api/revoke", {"field": "email", "effective_in": 0})
        time.sleep(1.0)
        http("POST", f"{web}/api/worker/resume")
        task4 = wait_task(web, t4["task_id"])
        task5 = wait_task(web, t5["task_id"])
        check("全字段收回的队列任务被 CANCELLED",
              task4["status"] == "CANCELLED", task4["status"])
        check("部分收回的任务保留并完成", task5["status"] in ("PUBLISHED", "FILTERED"),
              task5["status"])
        phone_rows = field_results(web, "phone", t5["task_id"])
        check("任务内未收回字段 phone 仍发布",
              any(r["status"] == "published" for r in phone_rows))

        print("\n[6] 缓存清理与审计证据隔离")
        _, evict = http("POST", f"{web}/api/cache/evict")
        check("清理只报告 raw_cache 内路径",
              all("raw_cache" in e["path"] for e in evict["evicted"]))
        sealed_root = stack.runtime / "data" / "sealed"
        sealed_files = list(sealed_root.glob("*.sealed.json"))
        check("sealed 目录证据全部保留", len(sealed_files) >= 2,
              f"found {len(sealed_files)}")
        _, ov = http("GET", f"{web}/api/overview")
        check("封存元数据仍可查询", len(ov["seals"]) == len(sealed_files))
        check("哈希链校验通过", ov["audit_chain"]["ok"] is True,
              str(ov["audit_chain"]))
        # Eviction must not delete evidence even when run a second time.
        http("POST", f"{web}/api/cache/evict")
        check("再次清理后 sealed 依然完整",
              len(list(sealed_root.glob("*.sealed.json"))) == len(sealed_files))

        print("\n[7] 篡改审计表 -> 哈希链校验失败")
        import sqlite3
        conn = sqlite3.connect(stack.runtime / "state.sqlite3")
        conn.execute("DELETE FROM audit_log WHERE seq = (SELECT MIN(seq) FROM audit_log)")
        conn.commit()
        conn.close()
        _, verify = http("POST", f"{web}/api/audit/verify")
        check("删除首条审计记录被检测到", verify["ok"] is False, str(verify))

        print("\n[8] 供应商真实请求可核对")
        _, reqs = http("GET", f"{web}/api/supplier/requests")
        paths = [r["path"] for r in reqs["requests"]]
        check("存在真实下载请求 /packages/*",
              any(p.startswith("/packages/") for p in paths))
        check("存在通知轮询请求", any(p.startswith("/notifications") for p in paths))
        check("存在收回管理请求", any(p == "/admin/revoke" for p in paths))

    finally:
        stack.stop()

    print(f"\n结果：{PASS} 通过，{FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
