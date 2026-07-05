"""Compatibility shim for runtimes importing `data.kalshi_trade_api` from repo root."""

from __future__ import annotations

import importlib.util
from pathlib import Path

try:
    from src.data.kalshi_trade_api import *  # type: ignore # noqa: F401,F403
except Exception:
    _src_file = Path(__file__).resolve().parents[1] / "src" / "data" / "kalshi_trade_api.py"
    _spec = importlib.util.spec_from_file_location("src_data_kalshi_trade_api", _src_file)
    if _spec is None or _spec.loader is None:
        raise RuntimeError(f"Unable to load Kalshi trade API module from {_src_file}")
    _module = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_module)
    for _name in dir(_module):
        if _name.startswith("_"):
            continue
        globals()[_name] = getattr(_module, _name)

