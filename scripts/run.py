#!/usr/bin/env python3
"""Process launcher for isolated (-I) runs.

`python -I` implies -P, so the working directory is not placed on sys.path and
the `app` package cannot be found for `-m` invocations. This launcher lives
outside any data directory, explicitly puts the repository root on sys.path
and then runs the requested module. All untrusted data still lives under
runtime/ and is only ever parsed as JSON, never imported.

Usage: python3 -I scripts/run.py <module.name>
"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

if len(sys.argv) < 2:
    raise SystemExit("usage: run.py <module.name> [args...]")
module = sys.argv[1]
sys.argv = [module] + sys.argv[2:]
runpy.run_module(module, run_name="__main__", alter_sys=True)
