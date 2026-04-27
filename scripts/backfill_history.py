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

    # Retrain the MLB model with collected sentiment + injury data
    print("[backfill] Retraining model with historical data...")
    try:
        from data.mlb_fetcher import build_game_dataset
        from models.mlb_model import retrain_with_history
        from config import MLB_SEASONS
        team_stats = build_game_dataset(MLB_SEASONS[:3])
        retrain_with_history(team_stats, verbose=True)
        print("[backfill] Model retrained and saved.")
    except Exception as e:
        print(f"[backfill] Model retrain failed (non-fatal): {e}")


if __name__ == "__main__":
    main()
