"""Vercel entrypoint wrapper for Flask dashboard."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
DASHBOARD_FILE = ROOT_DIR / "src" / "dashboard.py"

spec = importlib.util.spec_from_file_location("dashboard_entry", DASHBOARD_FILE)
if spec is None or spec.loader is None:
    raise RuntimeError(f"Unable to load dashboard module from {DASHBOARD_FILE}")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

app = module.app

