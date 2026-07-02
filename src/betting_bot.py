#!/usr/bin/env python3
"""
Betting Bot  –  MLB + Soccer Value Bet Finder
=============================================

Pipeline:
  1. [TRAIN]   Pull historical data → train MLB + Soccer models
  2. [ANALYZE] Fetch today's games + live odds → generate predictions
  3. [REPORT]  Find value bets (edge > threshold) → Kelly stake sizes
  4. [PROPS]   MLB player prop edge checker
  5. [HF]      HF-first mode: bootstrap dataset, append daily results,
               retrain/publish model, and run HF-based inference

Run modes:
  python src/betting_bot.py              # full daily run (step 2-3-4)
  python src/betting_bot.py --train      # re-train models (step 1, slow)
  python src/betting_bot.py --props "Aaron Judge" 0.5 hits
  python src/betting_bot.py --hf-bootstrap --hf-retrain-publish
"""

import sys
import os
import argparse
import warnings
import json
import datetime

warnings.filterwarnings("ignore")

# Make src/ importable regardless of launch directory
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SRC_DIR)

from config import BANKROLL, MLB_SEASONS

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
    from data.soccer_fetcher import get_historical_matches
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
        # Enrich with multi-source sentiment signals
        try:
            from data.sentiment import get_team_sentiment
            home_s = get_team_sentiment(g["home_team"], "baseball")
            away_s = get_team_sentiment(g["away_team"], "baseball")
            pred["sentiment_score"] = home_s.get("combined", 0.0)
            pred["signal_type"]     = home_s.get("signal_type") or away_s.get("signal_type") or "neutral"
            pred["injury_flag"]     = bool(home_s.get("injury_flag") or away_s.get("injury_flag"))
            pred["momentum_flag"]   = bool(home_s.get("momentum_flag") or away_s.get("momentum_flag"))
            pred["lineup_flag"]     = bool(home_s.get("lineup_flag") or away_s.get("lineup_flag"))
            pred["active_sources"]  = list(set(
                (home_s.get("active_sources") or []) + (away_s.get("active_sources") or [])
            ))
        except Exception as _se:
            print(f"[MLB] Sentiment fetch skipped: {_se}")
        predictions.append(pred)
        home_pct = pred.get('home_win_prob', 0.5)
        away_pct = pred.get('away_win_prob', 0.5)
        fav = g['home_team'] if home_pct >= away_pct else g['away_team']
        fav_pct = max(home_pct, away_pct)
        print(f"  {g['away_team']} @ {g['home_team']}"
              f"  →  Fav: {fav} ({fav_pct:.0%})"
              f"  |  Total: {pred['predicted_total']:.1f} runs")

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


def _norm_team_token(name: str) -> str:
    return "".join(ch for ch in str(name or "").lower() if ch.isalnum())


def _norm_matchup_key(home_team: str, away_team: str) -> str:
    return f"{_norm_team_token(away_team)}@{_norm_team_token(home_team)}"


def _build_prediction_index(predictions: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for pred in predictions or []:
        home = str(pred.get("home_team") or "").strip()
        away = str(pred.get("away_team") or "").strip()
        if not home or not away:
            continue
        out[_norm_matchup_key(home, away)] = pred
    return out


def _combined_feature_vector(pred: dict | None, bet: dict) -> dict:
    pred = pred or {}
    model_prob = float(bet.get("model_prob") or 0.0)
    book_prob = float(bet.get("book_prob") or 0.0)
    edge = float(bet.get("edge") or 0.0)
    ev = float(bet.get("ev") or 0.0)
    sentiment = float(pred.get("sentiment_score") or 0.0)
    injury_penalty = 0.10 if pred.get("injury_flag") else 0.0
    lineup_boost = 0.04 if pred.get("lineup_flag") else 0.0
    momentum_boost = 0.05 if pred.get("momentum_flag") else 0.0
    source_count = len(pred.get("active_sources") or [])

    signal_score = (
        0.52 * model_prob
        + 0.22 * max(edge, 0.0)
        + 0.16 * max(sentiment, 0.0)
        + 0.06 * min(max(ev, 0.0), 1.0)
        + lineup_boost
        + momentum_boost
        - injury_penalty
    )
    signal_score = max(0.0, min(1.0, signal_score))

    return {
        "model_prob": round(model_prob, 4),
        "book_prob": round(book_prob, 4),
        "edge": round(edge, 4),
        "ev": round(ev, 4),
        "sentiment": round(sentiment, 4),
        "injury_flag": bool(pred.get("injury_flag")),
        "lineup_flag": bool(pred.get("lineup_flag")),
        "momentum_flag": bool(pred.get("momentum_flag")),
        "source_count": source_count,
        "signal_score": round(signal_score, 4),
    }


def _attach_pipeline_features(bets: list[dict], prediction_index: dict[str, dict]) -> list[dict]:
    enriched: list[dict] = []
    for bet in bets or []:
        matchup = str(bet.get("matchup") or "").strip()
        parts = matchup.split(" vs ")
        home = parts[0].strip() if len(parts) == 2 else ""
        away = parts[1].strip() if len(parts) == 2 else ""
        pred = prediction_index.get(_norm_matchup_key(home, away)) if home and away else None
        vector = _combined_feature_vector(pred, bet)
        item = dict(bet)
        item["combined_features"] = vector
        item["signal_score"] = vector["signal_score"]
        item["data_sources"] = sorted(set((pred or {}).get("active_sources") or []) | {"odds"})
        enriched.append(item)
    return enriched


def _ev_signal_filter(bets: list[dict], min_ev: float, min_signal: float) -> list[dict]:
    return [
        b for b in (bets or [])
        if float(b.get("ev") or 0.0) >= min_ev
        and float(b.get("signal_score") or 0.0) >= min_signal
    ]


def _export_signal_log(path: str, bets: list[dict]) -> None:
    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "count": len(bets or []),
        "signals": bets or [],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _run_hf_pipeline_mode(args) -> bool:
    hf_mode = any([
        args.hf_bootstrap,
        args.hf_append_daily,
        args.hf_retrain_publish,
        args.hf_daily_run,
        bool(args.hf_predict_matchup),
    ])
    if not hf_mode:
        return False

    from data.hf_pipeline import HFDirectPipeline
    from config import HF_INFERENCE_ENDPOINT, HF_INFERENCE_MODEL

    pipeline = HFDirectPipeline(
        dataset_repo=args.hf_dataset_repo,
        model_repo=args.hf_model_repo,
    )
    if not pipeline.ok:
        print("[HF] Pipeline is not configured. Set HF_API_KEY and retry.")
        return True

    print(f"[HF] Dataset repo: {pipeline.dataset_repo_id}")
    print(f"[HF] Model repo:   {pipeline.model_repo_id}")

    if args.hf_bootstrap:
        result = pipeline.bootstrap_one_year_history(days_back=args.hf_days_back)
        print("[HF][BOOTSTRAP]", json.dumps(result, indent=2))

    if args.hf_append_daily:
        result = pipeline.append_daily_results()
        print("[HF][APPEND]", json.dumps(result, indent=2))

    if args.hf_retrain_publish:
        summary = pipeline.train_and_publish_best_model(
            min_rows=args.hf_min_train_rows,
            forced_model=args.hf_custom_model,
        )
        print(
            "[HF][TRAIN] "
            f"rows={summary.rows} best={summary.best_model} "
            f"cv_roc_auc={summary.cv_roc_auc:.4f} repo={summary.repo_id}"
        )

    if args.hf_daily_run:
        endpoint = args.hf_endpoint_url or HF_INFERENCE_ENDPOINT or ""
        model_id = args.hf_inference_model or HF_INFERENCE_MODEL or pipeline.model_repo_id
        daily = pipeline.run_daily_pipeline(
            custom_model=args.hf_custom_model,
            min_rows=args.hf_min_train_rows,
            predictions_output_path=args.hf_predictions_output,
            via_api=bool(args.hf_predict_via_api),
            model_id=model_id,
            endpoint_url=endpoint,
        )
        print("[HF][DAILY]", json.dumps(daily, indent=2))

    if args.hf_predict_matchup:
        home_team, away_team = args.hf_predict_matchup
        if args.hf_predict_via_api:
            endpoint = args.hf_endpoint_url or HF_INFERENCE_ENDPOINT or ""
            model_id = args.hf_inference_model or HF_INFERENCE_MODEL or pipeline.model_repo_id
            result = pipeline.predict_via_hf_api(
                home_team=home_team,
                away_team=away_team,
                season=args.hf_predict_season,
                model_id=model_id,
                endpoint_url=endpoint,
            )
            print("[HF][PREDICT_API]", json.dumps(result, indent=2))
        else:
            result = pipeline.predict_from_model_repo(
                home_team=home_team,
                away_team=away_team,
                season=args.hf_predict_season,
            )
            print("[HF][PREDICT_MODEL]", json.dumps(result, indent=2))

    return True


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
    parser.add_argument("--min-ev", type=float, default=0.0,
                        help="Minimum EV threshold for final signals")
    parser.add_argument("--min-signal", type=float, default=0.52,
                        help="Minimum combined signal score (0-1)")
    parser.add_argument("--signal-log", default=os.path.join("data", "latest_signal_log.json"),
                        help="Path to write combined feature/signal log JSON")
    parser.add_argument("--hf-bootstrap", action="store_true",
                        help="One-time load ~1 year of completed games to HF dataset")
    parser.add_argument("--hf-append-daily", action="store_true",
                        help="Append latest completed game results to HF dataset")
    parser.add_argument("--hf-retrain-publish", action="store_true",
                        help="Retrain best model from HF dataset and publish to HF model repo")
    parser.add_argument("--hf-daily-run", action="store_true",
                        help="Run daily clean+append, retrain custom model, and generate daily predictions")
    parser.add_argument("--hf-days-back", type=int, default=365,
                        help="Historical lookback for HF bootstrap")
    parser.add_argument("--hf-min-train-rows", type=int, default=200,
                        help="Minimum rows required before HF retrain")
    parser.add_argument("--hf-custom-model", default="auto",
                        choices=["auto", "gradient_boosting", "random_forest", "logistic_regression"],
                        help="Custom model choice for HF retraining")
    parser.add_argument("--hf-dataset-repo", default="",
                        help="Override HF dataset repo name/id")
    parser.add_argument("--hf-model-repo", default="",
                        help="Override HF model repo name/id")
    parser.add_argument("--hf-predictions-output", default=os.path.join("data", "hf_daily_predictions.json"),
                        help="File path for generated daily HF predictions JSON")
    parser.add_argument("--hf-predict-matchup", nargs=2, metavar=("HOME_TEAM", "AWAY_TEAM"),
                        help="Predict one matchup from HF model")
    parser.add_argument("--hf-predict-season", type=int, default=0,
                        help="Season/year feature for HF matchup prediction")
    parser.add_argument("--hf-predict-via-api", action="store_true",
                        help="Call HF inference API instead of downloading model artifact")
    parser.add_argument("--hf-endpoint-url", default="",
                        help="Custom HF inference endpoint URL")
    parser.add_argument("--hf-inference-model", default="",
                        help="HF model id for API inference calls (optional model swap)")
    args = parser.parse_args()

    print("=" * 60)
    print("  MLB + SOCCER VALUE BET BOT")
    print("=" * 60)

    if _run_hf_pipeline_mode(args):
        return

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
    from data.mlb_fetcher import get_schedule_today
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

    prediction_index = _build_prediction_index(all_predictions)
    total_candidates = len(value_bets) + len(totals_bets)
    value_bets = _attach_pipeline_features(value_bets, prediction_index)
    totals_bets = _attach_pipeline_features(totals_bets, prediction_index)
    value_bets = _ev_signal_filter(value_bets, min_ev=args.min_ev, min_signal=args.min_signal)
    totals_bets = _ev_signal_filter(totals_bets, min_ev=args.min_ev, min_signal=args.min_signal)
    combined_bets = value_bets + totals_bets

    parlays = build_parlay(value_bets + totals_bets)
    os.makedirs(os.path.dirname(args.signal_log) or ".", exist_ok=True)
    _export_signal_log(args.signal_log, combined_bets)
    print(f"[PIPELINE] Signals kept: {len(combined_bets)}/{total_candidates}"
          f"  |  min_ev={args.min_ev:.3f} min_signal={args.min_signal:.2f}")
    print(f"[PIPELINE] Signal log: {args.signal_log}")

    # ── Kalshi ticker + investor grade enrichment ──────────────────────────
    _all_bets = value_bets + totals_bets
    try:
        from data.kalshi import attach_kalshi_to_bets
        _all_bets = attach_kalshi_to_bets(_all_bets)
        value_bets  = _all_bets[:len(value_bets)]
        totals_bets = _all_bets[len(value_bets):]
    except Exception as _ke:
        print(f"[main] Kalshi enrichment skipped: {_ke}")
    try:
        from analysis.investor import investor_grade, build_daily_portfolio
        for _p in _all_bets:
            _p.update(investor_grade(_p, args.bankroll))
        portfolio = build_daily_portfolio(_all_bets, bankroll=args.bankroll)
        _grade_counts: dict[str, int] = {}
        for _p in _all_bets:
            _g = _p.get("grade", "X")
            _grade_counts[_g] = _grade_counts.get(_g, 0) + 1
        if _grade_counts:
            print(f"\n[GRADES] {_grade_counts}")
        if portfolio:
            print(f"[PORTFOLIO] {len(portfolio)} bets recommended today"
                  f"  |  Total stake: ${sum(float(b.get('recommended_stake', 0)) for b in portfolio):.2f}")
    except Exception as _ie:
        print(f"[main] Investor grade skipped: {_ie}")

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
