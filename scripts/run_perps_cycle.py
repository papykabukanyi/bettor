"""Run one Kalshi Perps strategy cycle manually (scans all instruments).

Dry-run unless KALSHI_PERPS_LIVE_TRADING_ENABLED=1 is set in the environment
AND dry_run=False is passed here. Safe to run repeatedly to watch what the
strategy would decide before ever enabling live trading.

Usage:
    python scripts/run_perps_cycle.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def _load_dotenv() -> None:
    import os

    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        return
    lines = env_path.read_text(encoding="utf-8").splitlines()
    idx = 0
    while idx < len(lines):
        raw = lines[idx].strip()
        idx += 1
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key, value = key.strip(), value.strip()
        if not key:
            continue
        if key == "KALSHI_PRIVATE_KEY" and "BEGIN RSA PRIVATE KEY" in value and "END RSA PRIVATE KEY" not in value:
            chunks = [value]
            while idx < len(lines):
                part = lines[idx].rstrip("\r")
                chunks.append(part)
                idx += 1
                if "END RSA PRIVATE KEY" in part:
                    break
            value = "\n".join(chunks)
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def main() -> int:
    _load_dotenv()
    from data.perps_strategy import LIVE_TRADING_ENABLED, run_cycle

    result = run_cycle()
    print(json.dumps(result, indent=2, default=str))
    print("\n--- Summary ---")
    print(f"Live trading enabled: {LIVE_TRADING_ENABLED}")
    print(f"Action taken: {result.get('action')}")
    if not result.get("ok", True):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
