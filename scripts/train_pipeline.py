"""
Full Training Pipeline
======================
One-time (or weekly) script that:

  1. Downloads Lahman DB CSVs → player/team career history
  2. Fetches GDELT news sentiment → per-team historical sentiment
  3. Loads Retrosheet / MLB Stats API game logs → ground-truth outcomes
  4. Builds an enhanced training DataFrame
  5. Runs retrain_with_history() to produce mlb_model.joblib
  6. Prints calibration stats

Usage:
  python scripts/train_pipeline.py [--seasons 2023 2024 2025] [--gdelt-days 60]

The pipeline is safe to re-run; all downloads are cached locally.
"""

import os
import sys
import argparse
import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC  = os.path.join(ROOT, "src")
sys.path.insert(0, ROOT)
sys.path.insert(0, SRC)


def _parse_args():
    p = argparse.ArgumentParser(description="MLB model training pipeline")
    p.add_argument("--seasons",    nargs="+", type=int,
                   default=[2023, 2024, 2025],
                   help="MLB seasons to include (default: 2023 2024 2025)")
    p.add_argument("--gdelt-days", type=int, default=60,
                   help="Days of GDELT news to fetch (default: 60)")
    p.add_argument("--skip-gdelt", action="store_true",
                   help="Skip GDELT fetch (use DB sentiment already stored)")
    p.add_argument("--skip-lahman", action="store_true",
                   help="Skip Lahman download")
    p.add_argument("--skip-retro",  action="store_true",
                   help="Skip Retrosheet/Stats-API game log pull")
    return p.parse_args()


def main():
    args = _parse_args()

    print(f"\n{'='*60}")
    print(f"  MLB Training Pipeline — {datetime.datetime.now():%Y-%m-%d %H:%M}")
    print(f"  Seasons: {args.seasons}")
    print(f"{'='*60}\n")

    # ── Step 1: Lahman team records ────────────────────────────────────────
    lahman_teams = None
    if not args.skip_lahman:
        print("[pipeline] Step 1/5 — Downloading Lahman team records…")
        try:
            from data.lahman_fetcher import get_team_records
            lahman_teams = get_team_records(seasons=args.seasons)
            print(f"[pipeline]   Lahman: {len(lahman_teams)} team-season rows")
        except Exception as e:
            print(f"[pipeline]   Lahman error: {e}")
    else:
        print("[pipeline] Step 1/5 — Lahman SKIPPED")

    # ── Step 2: GDELT historical sentiment ────────────────────────────────
    if not args.skip_gdelt:
        print(f"\n[pipeline] Step 2/5 — Fetching GDELT sentiment ({args.gdelt_days} days)…")
        try:
            from data.gdelt_fetcher import fetch_gdelt_sentiment
            n = fetch_gdelt_sentiment(days_back=args.gdelt_days, verbose=True)
            print(f"[pipeline]   GDELT: {n} articles saved")
        except Exception as e:
            print(f"[pipeline]   GDELT error: {e}")
    else:
        print("[pipeline] Step 2/5 — GDELT SKIPPED")

    # ── Step 3: Game-log outcomes (Retrosheet / MLB Stats API) ────────────
    game_results = None
    if not args.skip_retro:
        print(f"\n[pipeline] Step 3/5 — Loading game outcomes for {args.seasons}…")
        try:
            from data.retrosheet_fetcher import get_game_results, build_team_win_pct_by_season
            game_results = get_game_results(args.seasons, verbose=True)
            win_pct_df   = build_team_win_pct_by_season(args.seasons)
            print(f"[pipeline]   Outcomes: {len(game_results)} games")
            print(f"[pipeline]   Win-pct coverage: {win_pct_df['team'].nunique()} teams")
        except Exception as e:
            print(f"[pipeline]   Retrosheet error: {e}")
    else:
        print("[pipeline] Step 3/5 — Retrosheet SKIPPED")

    # ── Step 4: Build training feature set from MLB Stats API ─────────────
    print(f"\n[pipeline] Step 4/5 — Building team stats feature set…")
    try:
        from data.mlb_fetcher import build_game_dataset
        from config import MLB_SEASONS
        # Use pipeline seasons; fall back to config
        use_seasons = args.seasons or MLB_SEASONS[:3]
        team_stats  = build_game_dataset(use_seasons)
        print(f"[pipeline]   Team stats: {len(team_stats)} rows across "
              f"{team_stats['season'].nunique() if not team_stats.empty else 0} seasons")

        # If Lahman team records are available, merge win_pct as an extra feature
        if lahman_teams is not None and not lahman_teams.empty and not team_stats.empty:
            try:
                import pandas as pd
                merge_src = lahman_teams[["season","team_name","win_pct","era"]].rename(
                    columns={"team_name": "team", "era": "lahman_era"}
                )
                team_stats = team_stats.merge(merge_src, on=["season","team"], how="left")
                print(f"[pipeline]   Merged Lahman win_pct for "
                      f"{team_stats['win_pct'].notna().sum()} rows")
            except Exception as e:
                print(f"[pipeline]   Lahman merge skipped: {e}")

    except Exception as e:
        print(f"[pipeline]   Team stats error: {e}")
        team_stats = None  # type: ignore

    # ── Step 5: Retrain model ─────────────────────────────────────────────
    if team_stats is not None and not team_stats.empty:
        print(f"\n[pipeline] Step 5/5 — Retraining model with sentiment + injury features…")
        try:
            from models.mlb_model import retrain_with_history
            retrain_with_history(team_stats, verbose=True)
            print("[pipeline]   Model saved → models/mlb_model.joblib")
        except Exception as e:
            print(f"[pipeline]   Retrain error: {e}")
    else:
        print("[pipeline] Step 5/5 — SKIPPED (no team stats)")

    # ── Bonus: Calibration check ──────────────────────────────────────────
    print(f"\n[pipeline] Calibration check…")
    try:
        from data.db import get_calibration_data
        cal = get_calibration_data(days_back=90)
        if cal.get("total_resolved", 0) > 0:
            print(f"[pipeline]   Resolved predictions: {cal['total_resolved']}")
            print(f"[pipeline]   Expected Calibration Error (ECE): {cal.get('ece')}")
            if cal.get("bins"):
                print("[pipeline]   Calibration bins:")
                for b in cal["bins"]:
                    gap_bar = "█" * int(b["gap"] * 20)
                    print(f"    {b['bin']:10s}  n={b['n']:4d}  "
                          f"pred={b['avg_pred']:.2f} actual={b['avg_actual']:.2f}  "
                          f"gap={b['gap']:.3f} {gap_bar}")
        else:
            print("[pipeline]   No resolved predictions yet — calibration unavailable")
    except Exception as e:
        print(f"[pipeline]   Calibration check error: {e}")

    print(f"\n{'='*60}")
    print(f"  Pipeline complete — {datetime.datetime.now():%H:%M:%S}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
