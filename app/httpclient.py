"""Tiny JSON HTTP client on top of the stdlib (no third-party dependencies)."""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any


def request(method: str, url: str, body: dict[str, Any] | None = None,
            timeout: float = 15.0) -> tuple[int, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))
