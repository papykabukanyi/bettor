"""
Betting Bot  –  Web Dashboard
==============================
Run locally : python src/dashboard.py
Deploy      : Railway picks this up via Procfile  (gunicorn src.dashboard:app)

Routes:
  GET  /              → main dashboard (shows cached results)
  POST /api/run       → kick off a fresh analysis in a background thread
  GET  /api/status    → polling endpoint: {status, last_updated, results}
"""

import os
import sys
import json
import datetime
import threading
import traceback

from flask import Flask, render_template, jsonify, request

# Make src/ importable
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SRC_DIR)

from config import BANKROLL, MIN_VALUE_EDGE, KELLY_FRACTION, MLB_SEASONS

app = Flask(__name__, template_folder="templates")

# ──────────────────────────────────────────────────────────────────────────────
# Shared analysis state (in-memory; safe for single-worker gunicorn / dev)
# ──────────────────────────────────────────────────────────────────────────────

_state = {
    "status":       "idle",          # idle | running | done | error
    "last_updated": None,
    "error":        None,
    "win_bets":     [],
    "totals_bets":  [],
    "prop_stats":   [],
    "parlays":      [],
    "parlays_2":    [],
    "parlays_3":    [],
    "parlays_4":    [],
    "mlb_games":    0,
    "soccer_games": 0,
    "api_remaining": None,
}
_lock = threading.Lock()


# ──────────────────────────────────────────────────────────────────────────────
# Analysis worker
# ──────────────────────────────────────────────────────────────────────────────

def _run_analysis():
    """Full pipeline; runs in a background thread."""
    import warnings
    warnings.filterwarnings("ignore")
    import pandas as pd

    with _lock:
        _state["status"] = "running"
        _state["error"]  = None

    try:
        # ── MLB ────────────────────────────────────────────────────────────
        from data.mlb_fetcher import (
            get_schedule_today, build_game_dataset,
            estimate_game_total, get_starters_props_batch,
        )
        from models.mlb_model import load_model, predict_from_season_stats

        games     = get_schedule_today()
        stats     = build_game_dataset([MLB_SEASONS[0]])
        mlb_model = load_model()
        mlb_preds = []

        for g in games:
            if g.get("status", "") not in ("Preview", "Pre-Game", "Scheduled", "Warmup", ""):
                continue
            pred = predict_from_season_stats(g["home_team"], g["away_team"], stats, mlb_model)
            pred["home_team"]       = g["home_team"]
            pred["away_team"]       = g["away_team"]
            pred["home_starter"]    = g.get("home_starter", "TBD")
            pred["away_starter"]    = g.get("away_starter", "TBD")
            pred["predicted_total"] = estimate_game_total(g["home_team"], g["away_team"], stats)
            mlb_preds.append(pred)

        # ── Soccer ────────────────────────────────────────────────────────
        from data.soccer_fetcher import get_todays_fixtures
        from models.soccer_model import load_model as load_soccer

        soccer_model = load_soccer()
        soccer_preds = []
        leagues      = ["EPL", "ESP", "GER"]
        fixtures     = get_todays_fixtures(leagues)

        if soccer_model:
            for f in fixtures:
                pred = soccer_model.predict(f["home_team"], f["away_team"])
                pred["home_team"] = f["home_team"]
                pred["away_team"] = f["away_team"]
                pred["league"]    = f.get("league", "")
                soccer_preds.append(pred)

        # ── Odds ──────────────────────────────────────────────────────────
        from data.odds_fetcher import (
            get_live_odds, odds_to_dataframe,
            get_totals_odds, totals_to_dataframe,
        )

        api_remaining = None
        ml_rows, tot_rows = [], []
        for sport_key in ["mlb", "epl", "laliga", "bundesliga"]:
            raw_ml = get_live_odds(sport_key, markets="h2h")
            if raw_ml:
                df = odds_to_dataframe(raw_ml)
                df["sport_key"] = sport_key
                ml_rows.append(df)
            raw_tot = get_totals_odds(sport_key)
            if raw_tot:
                df2 = totals_to_dataframe(raw_tot)
                df2["sport_key"] = sport_key
                tot_rows.append(df2)

        ml_df  = pd.concat(ml_rows,  ignore_index=True) if ml_rows  else pd.DataFrame()
        tot_df = pd.concat(tot_rows, ignore_index=True) if tot_rows else pd.DataFrame()

        def sport_filter(df, is_mlb):
            if df.empty:
                return df
            mlb_keys = {"mlb", "baseball_mlb"}
            mask = df.get("sport_key", pd.Series(dtype=str)).isin(mlb_keys)
            return df[mask] if is_mlb else df[~mask]

        # ── Value bets ────────────────────────────────────────────────────
        from analysis.value_finder import find_value_bets, find_totals_bets, build_parlay

        win_bets = (
            find_value_bets(mlb_preds,    sport_filter(ml_df,  True),  sport="mlb")
          + find_value_bets(soccer_preds, sport_filter(ml_df,  False), sport="soccer")
        )
        totals_bets = (
            find_totals_bets(mlb_preds,    sport_filter(tot_df, True),  sport="mlb")
          + find_totals_bets(soccer_preds, sport_filter(tot_df, False), sport="soccer")
        )
        parlays = build_parlay(win_bets + totals_bets)

        # ── Starter props ─────────────────────────────────────────────────
        prop_stats = get_starters_props_batch(games, MLB_SEASONS[0])

        # ── Serialise for JSON  (convert numpy floats etc.) ───────────────
        def _clean(obj):
            if isinstance(obj, list):
                return [_clean(x) for x in obj]
            if isinstance(obj, dict):
                return {k: _clean(v) for k, v in obj.items()}
            try:
                import numpy as np
                if isinstance(obj, (np.integer,)):
                    return int(obj)
                if isinstance(obj, (np.floating,)):
                    return float(obj)
                if isinstance(obj, (np.bool_,)):
                    return bool(obj)
            except ImportError:
                pass
            return obj

        # Add sequential ids so the template / JS can reference each bet
        for i, b in enumerate(win_bets):
            b["_id"] = f"win_{i}"
        for i, b in enumerate(totals_bets):
            b["_id"] = f"tot_{i}"
        for i, p in enumerate(prop_stats):
            p["_id"] = f"prop_{i}"

        # Keep top-10 per leg-size for parlays
        parlays_2 = [p for p in parlays if p["num_legs"] == 2][:10]
        parlays_3 = [p for p in parlays if p["num_legs"] == 3][:10]
        parlays_4 = [p for p in parlays if p["num_legs"] == 4][:10]

        with _lock:
            _state["status"]        = "done"
            _state["last_updated"]  = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _state["win_bets"]      = _clean(win_bets)
            _state["totals_bets"]   = _clean(totals_bets)
            _state["prop_stats"]    = _clean(prop_stats)
            _state["parlays"]       = _clean(parlays[:5])   # top-5 all sizes (legacy)
            _state["parlays_2"]     = _clean(parlays_2)
            _state["parlays_3"]     = _clean(parlays_3)
            _state["parlays_4"]     = _clean(parlays_4)
            _state["mlb_games"]     = len(mlb_preds)
            _state["soccer_games"]  = len(soccer_preds)
            _state["api_remaining"] = api_remaining

    except Exception:
        err = traceback.format_exc()
        with _lock:
            _state["status"] = "error"
            _state["error"]  = err


# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    with _lock:
        state = dict(_state)
    return render_template("dashboard.html", state=state, bankroll=BANKROLL)


@app.route("/api/run", methods=["POST"])
def api_run():
    with _lock:
        if _state["status"] == "running":
            return jsonify({"ok": False, "msg": "Analysis already running"}), 409
        _state["status"] = "running"

    t = threading.Thread(target=_run_analysis, daemon=True)
    t.start()
    return jsonify({"ok": True, "msg": "Analysis started"})


@app.route("/api/status")
def api_status():
    with _lock:
        return jsonify(dict(_state))


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
