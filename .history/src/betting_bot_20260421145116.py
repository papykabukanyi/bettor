#!/usr/bin/env python3
"""
Betting Bot  –  MLB + Soccer Value Bet Finder
=============================================

Pipeline:
  1. [TRAIN]   Pull historical data → train MLB + Soccer models
  2. [ANALYZE] Fetch today's games + live odds → generate predictions
  3. [REPORT]  Find value bets (edge > threshold) → Kelly stake sizes
  4. [PROPS]   MLB player prop edge checker

Run modes:
  python src/betting_bot.py              # full daily run (step 2-3-4)
  python src/betting_bot.py --train      # re-train models (step 1, slow)
  python src/betting_bot.py --props "Aaron Judge" 0.5 hits
"""

import sys
import os
import argparse
import warnings

warnings.filterwarnings("ignore")

# Make src/ importable regardless of launch directory
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SRC_DIR)

from config import BANKROLL, MLB_SEASONS, FOOTBALL_DATA_UK_LEAGUES

# ------------------------------------------------------------------
# Lazy imports (only load heavy libs when needed)
# ------------------------------------------------------------------

def _train_mlb():
    """Download MLB stats and (re)train the win-probability model."""
    from data.mlb_fetcher import build_game_dataset
    from models.mlb_model import train

    print("\n[TRAIN] Fetching MLB team stats …")
    stats = build_game_dataset(MLB_SEASONS)
    if stats.empty:
        print("[TRAIN] No MLB data fetched – check pybaseball installation.")
        return None
    print(f"[TRAIN] {len(stats)} team-season rows fetched.")
    model = train(stats, verbose=True)
    return model


def _train_soccer(league_keys: list[str] | None = None):
    """Download soccer historical data and (re)train the Poisson+ML model."""
    from data.soccer_fetcher import get_historical_matches, compute_team_strength
    from models.soccer_model import SoccerModel

    use_keys = league_keys or ["EPL", "ESP", "GER"]
    print(f"\n[TRAIN] Fetching soccer data for: {use_keys} …")

    all_frames = []
    import pandas as pd
    for lk in use_keys:
        df = get_historical_matches(lk)
        if not df.empty:
            all_frames.append(df)
            print(f"  {lk}: {len(df)} matches loaded")

    if not all_frames:
        print("[TRAIN] No soccer data fetched.")
        return None

    matches = pd.concat(all_frames, ignore_index=True)
    model = SoccerModel()
    model.fit(matches)
    return model


def _analyze_mlb():
    """Fetch today's MLB games, predict outcomes, return prediction list."""
    from data.mlb_fetcher import get_schedule_today, build_game_dataset
    from models.mlb_model import load_model, predict_from_season_stats

    print("\n[MLB] Fetching today's schedule …")
    games = get_schedule_today()
    if not games:
        print("[MLB] No games scheduled today or API unavailable.")
        return []

    # Load team stats for the most recent season
    from config import MLB_SEASONS
    stats = build_game_dataset([MLB_SEASONS[0]])

    model = load_model()

    predictions = []
    for g in games:
        if g.get("status", "") not in ("Preview", "Pre-Game", "Scheduled", "Warmup", ""):
            continue
        pred = predict_from_season_stats(
            g["home_team"], g["away_team"], stats, model
        )
        pred["home_team"]        = g["home_team"]
        pred["away_team"]        = g["away_team"]
        pred["home_starter"]     = g.get("home_starter", "TBD")
        pred["away_starter"]     = g.get("away_starter", "TBD")
        # Attach expected run total for over/under analysis
        from data.mlb_fetcher import estimate_game_total
        pred["predicted_total"]  = estimate_game_total(g["home_team"], g["away_team"], stats)
        predictions.append(pred)
        print(f"  {g['away_team']} @ {g['home_team']}"
              f"  →  Home win: {pred.get('home_win_prob',0):.1%} | "
              f"Away win: {pred.get('away_win_prob',0):.1%} | "
              f"Predicted total: {pred['predicted_total']:.1f} runs")

    return predictions


def _analyze_soccer(league_keys: list[str] | None = None):
    """Fetch today's soccer fixtures, predict outcomes, return prediction list."""
    from data.soccer_fetcher import get_todays_fixtures
    from models.soccer_model import load_model

    use_keys = league_keys or ["EPL", "ESP", "GER", "MLS"]
    print(f"\n[SOCCER] Fetching today's fixtures for: {use_keys} …")
    fixtures = get_todays_fixtures(use_keys)

    if not fixtures:
        print("[SOCCER] No fixtures today or API key not set.")
        return []

    model = load_model()
    if model is None:
        print("[SOCCER] Model not trained – run with --train first.")
        return []

    predictions = []
    for f in fixtures:
        pred = model.predict(f["home_team"], f["away_team"])
        pred["home_team"] = f["home_team"]
        pred["away_team"] = f["away_team"]
        pred["league"]    = f.get("league", "")
        predictions.append(pred)
        print(f"  [{pred['league']}] {f['away_team']} @ {f['home_team']}"
              f"  →  H:{pred.get('home_win',0):.1%}"
              f" D:{pred.get('draw',0):.1%}"
              f" A:{pred.get('away_win',0):.1%}")

    return predictions


def _get_odds(sports: list[str]) -> "tuple[pd.DataFrame, pd.DataFrame]":
    """Fetch live moneyline odds AND totals for a list of sport keys.
    Returns (moneyline_df, totals_df).
    """
    import pandas as pd
    from data.odds_fetcher import get_live_odds, odds_to_dataframe, get_totals_odds, totals_to_dataframe

    ml_rows, tot_rows = [], []
    for sport in sports:
        raw_ml = get_live_odds(sport, markets="h2h")
        if raw_ml:
            df = odds_to_dataframe(raw_ml)
            df["sport_key"] = sport
            ml_rows.append(df)

        raw_tot = get_totals_odds(sport)
        if raw_tot:
            df2 = totals_to_dataframe(raw_tot)
            df2["sport_key"] = sport
            tot_rows.append(df2)

    ml_df  = pd.concat(ml_rows,  ignore_index=True) if ml_rows  else pd.DataFrame()
    tot_df = pd.concat(tot_rows, ignore_index=True) if tot_rows else pd.DataFrame()
    return ml_df, tot_df


def _scan_props_today(games: list[dict], season: int) -> list[dict]:
    """
    Auto-scan all of today's probable starters for strikeout prop value.
    Returns a list of prop analysis dicts for display in the daily report.
    """
    from data.mlb_fetcher import get_starters_props_batch

    if not games:
        return []

    print(f"\n[PROPS] Scanning {len(games)} games for starter prop analysis …")
    props = get_starters_props_batch(games, season)
    for p in props:
        over_p = p.get("over_prob", 0)
        under_p = p.get("under_prob", 0)
        if over_p >= 0.58:
            lean = f"LEAN OVER  {p['line']:.1f} Ks ({over_p:.0%})"
        elif under_p >= 0.58:
            lean = f"LEAN UNDER {p['line']:.1f} Ks ({under_p:.0%})"
        else:
            lean = f"No clear lean  ({over_p:.0%} over / {under_p:.0%} under)"
        print(f"  {p['name']:20s}  avg {p['avg_per_game']:.1f} Ks  →  {lean}")
    return props


def _check_player_props(player_name: str, line: float, stat_type: str, season: int):
    """Evaluate a player prop bet."""
    from data.mlb_fetcher import get_player_prop_stats
    from models.mlb_model import evaluate_player_prop

    print(f"\n[PROPS] Looking up {player_name} ({season}) …")
    stats = get_player_prop_stats(player_name, season)
    if not stats:
        print(f"  Player '{player_name}' not found.")
        return

    print(f"  Type    : {stats.get('type', '?')}")
    print(f"  Team    : {stats.get('team', '?')}")
    print(f"  Season  : {stats.get('season', '?')}")

    # Map stat type to per-game average
    stat_map = {
        "hits":       stats.get("H_per_game"),
        "hr":         stats.get("HR_per_game"),
        "rbi":        stats.get("RBI_per_game"),
        "strikeouts": stats.get("K_per_game"),
    }
    avg = stat_map.get(stat_type.lower())
    if avg is None:
        print(f"  Stat type '{stat_type}' not found for this player.")
        print(f"  Available: {[k for k, v in stat_map.items() if v is not None]}")
        return

    result = evaluate_player_prop(avg, line)
    print(f"\n  Prop: {player_name} Over/Under {line} {stat_type}")
    print(f"  Historical avg/game : {result['avg_per_game']}")
    print(f"  Over probability    : {result['over_prob']:.1%}")
    print(f"  Under probability   : {result['under_prob']:.1%}")
    print(f"  Recommendation      : {result['recommendation']}")


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="MLB + Soccer Value Bet Bot")
    parser.add_argument("--train",   action="store_true", help="Re-train models from historical data")
    parser.add_argument("--props",   nargs=3, metavar=("PLAYER", "LINE", "STAT"),
                        help="Check MLB player prop: --props 'Aaron Judge' 0.5 hits")
    parser.add_argument("--season",  type=int, default=2024, help="Season for props lookup")
    parser.add_argument("--leagues", nargs="+", default=["EPL", "ESP", "GER"],
                        help="Soccer leagues to analyze")
    parser.add_argument("--bankroll", type=float, default=BANKROLL)
    args = parser.parse_args()

    print("=" * 60)
    print("  MLB + SOCCER VALUE BET BOT")
    print("=" * 60)

    # --- Props mode ------------------------------------------------
    if args.props:
        player, line_str, stat = args.props
        _check_player_props(player, float(line_str), stat, args.season)
        return

    # --- Training mode --------------------------------------------
    if args.train:
        _train_mlb()
        _train_soccer(args.leagues)
        print("\n[TRAIN] Done. Run without --train to analyze today's games.")
        return

    # --- Daily analysis mode --------------------------------------
    mlb_preds    = _analyze_mlb()
    soccer_preds = _analyze_soccer(args.leagues)

    all_predictions = (
        [dict(p, _sport="mlb")    for p in mlb_preds] +
        [dict(p, _sport="soccer") for p in soccer_preds]
    )

    if not all_predictions:
        print("\nNo games to analyze today.")
        return

    # Fetch live odds (moneyline + totals)
    print("\n[ODDS] Fetching live moneyline and totals odds …")
    ml_df, tot_df = _get_odds(["mlb", "epl", "laliga", "bundesliga"])

    # Split odds by sport
    def _sport_filter(df, is_mlb: bool):
        if df.empty:
            return df
        mlb_keys = {"mlb", "baseball_mlb"}
        mask = df.get("sport_key", pd.Series(dtype=str)).isin(mlb_keys)
        return df[mask] if is_mlb else df[~mask]

    mlb_ml_df     = _sport_filter(ml_df,  is_mlb=True)
    soccer_ml_df  = _sport_filter(ml_df,  is_mlb=False)
    mlb_tot_df    = _sport_filter(tot_df, is_mlb=True)
    soccer_tot_df = _sport_filter(tot_df, is_mlb=False)

    # Scan today's starters for prop analysis
    from config import MLB_SEASONS
    prop_stats = _scan_props_today(
        get_schedule_today() if mlb_preds else [], MLB_SEASONS[0]
    )

    from analysis.value_finder import find_value_bets, find_totals_bets, build_parlay, summarise_suggestions

    # ── Win bets ────────────────────────────────────────────────────────
    value_bets = (
        find_value_bets(mlb_preds,    mlb_ml_df,    sport="mlb")
      + find_value_bets(soccer_preds, soccer_ml_df, sport="soccer")
    )

    # ── Totals bets ─────────────────────────────────────────────────────
    totals_bets = (
        find_totals_bets(mlb_preds,    mlb_tot_df,    sport="mlb")
      + find_totals_bets(soccer_preds, soccer_tot_df, sport="soccer")
    )

    parlays = build_parlay(value_bets + totals_bets)

    print("\n" + summarise_suggestions(
        value_bets,
        parlays=parlays,
        totals_bets=totals_bets,
        prop_stats=prop_stats,
    ))


if __name__ == "__main__":
    # Allow 'import pandas as pd' inside main without circular issues
    import pandas as pd
    main()
