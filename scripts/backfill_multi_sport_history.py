"""Backfill unified multi-sport historical depth into training tables.

Usage:
  python scripts/backfill_multi_sport_history.py 180 nfl,nba,nhl,soccer
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
sys.path.insert(0, ROOT)
sys.path.insert(0, SRC)


def _parse_days(argv) -> int:
    try:
        if len(argv) > 1:
            return max(7, int(argv[1]))
    except Exception:
        pass
    return max(7, int(os.getenv("MULTI_SPORT_HISTORY_DAYS", "180") or "180"))


def _parse_sports(argv) -> list[str]:
    if len(argv) > 2:
        parts = [p.strip().lower() for p in str(argv[2]).split(",") if p.strip()]
        if parts:
            return parts
    env = os.getenv(
        "MULTI_SPORT_HISTORY_SPORTS",
        "nfl,nba,nhl,soccer,baseball,tennis,boxing,mma,golf,motorsports,cricket",
    )
    return [p.strip().lower() for p in env.split(",") if p.strip()]


def main() -> None:
    from data.multi_sport_history import ingest_multi_sport_history

    days = _parse_days(sys.argv)
    sports = _parse_sports(sys.argv)
    print(f"[multi-history] days_back={days} sports={sports}")

    result = ingest_multi_sport_history(days_back=days, sports=sports)
    print("[multi-history] result:")
    print(result)


if __name__ == "__main__":
    main()
