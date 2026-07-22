"""One-shot, READ-ONLY connectivity + eligibility check for Kalshi Perps
against the real account configured in .env / the environment.

Places zero orders. Only makes GET requests. Safe to run repeatedly.

Usage:
    python scripts/test_kalshi_perps_connectivity.py
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
    """Minimal .env loader (mirrors dashboard.py's) so this script works
    standalone without needing the full dashboard import."""
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
    from data.kalshi_perps import run_connectivity_check

    result = run_connectivity_check()
    print(json.dumps(result, indent=2, default=str))

    checks = result.get("checks", {})
    enabled = (checks.get("margin_enabled") or {}).get("data", {}).get("enabled")
    print("\n--- Summary ---")
    if enabled is True:
        print("Margin/Perps trading IS enabled on this account.")
    elif enabled is False:
        print("Margin/Perps trading is NOT yet enabled on this account (phased rollout).")
    else:
        err = (checks.get("margin_enabled") or {}).get("error")
        print(f"Could not determine margin-enabled status: {err}")

    failed = [name for name, c in checks.items() if not c.get("ok")]
    if failed:
        print(f"Checks that failed: {failed}")
        return 1
    print("All read-only checks succeeded. No orders were placed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
