"""Backfill historical news + injury data using free sources.
Usage:
  python scripts/backfill_history.py 30
"""

import os
import sys
import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
sys.path.insert(0, ROOT)
sys.path.insert(0, SRC)


def _parse_days(argv) -> int:
    try:
        if len(argv) > 1:
            return int(argv[1])
    except Exception:
        pass
    return int(os.getenv("HIST_DAYS", "30"))


def main():
    from data.history_ingest import backfill_news, backfill_injuries
    days = _parse_days(sys.argv)
    print(f"[backfill] days_back={days}")

    n_news = backfill_news(days_back=days)
    print(f"[backfill] news rows: {n_news}")

    n_inj = backfill_injuries(days_back=days)
    print(f"[backfill] injuries rows: {n_inj}")


if __name__ == "__main__":
    main()
