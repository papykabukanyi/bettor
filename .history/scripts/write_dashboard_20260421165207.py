import os

content = """\
\"\"\"
Betting Bot - Web Dashboard
Routes:
  GET  /            -> main dashboard
  POST /api/run     -> kick off analysis in background thread
  GET  /api/status  -> polling: {status, phase, phase_idx, phase_total, last_updated}
  GET  /api/games   -> upcoming games (today + tomorrow) from DB
\"\"\"

import os, sys, datetime, threading, traceback
from flask import Flask, render_template, jsonify, request

SRC_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SRC_DIR)
from config import BANKROLL, MIN_VALUE_EDGE, KELLY_FRACTION, MLB_SEASONS

app = Flask(__name__, template_folder="templates")

_PHASES = [
    "Fetching MLB schedule",
    "Fetching soccer fixtures",
    "Loading team stats & model",
    "Fetching injury reports",
    "Fetching live odds",
    "Running value models",
    "Building parlays",
    "Saving to database",
]

_state = {
    "status": "idle", "phase": "", "phase_idx": 0, "phase_total": len(_PHASES),
    "last_updated": None, "error": None,
    "win_bets": [], "totals_bets": [], "prop_stats": [],
    "parlays": [], "parlays_2": [], "parlays_3": [], "parlays_4": [],
    "mlb_games": 0, "soccer_games": 0,
    "upcoming_games": [], "injuries": [], "api_remaining": None,
}
_lock = threading.Lock()

def _phase(idx, name=""):
    with _lock:
        _state["phase"]     = name or (_PHASES[idx] if idx < len(_PHASES) else name)
        _state["phase_idx"] = idx

def _run_analysis():
    import warnings; warnings.filterwarnings("ignore")
    import pandas as pd
    with _lock:
        _state["status"] = "running"; _state["error"] = None
        _state["phase"] = _PHASES[0]; _state["phase_idx"] = 0
    try:
        _phase(0)
        from data.mlb_fetcher import (get_schedule_range, build_game_dataset,
                                       estimate_game_total, get_starters_props_batch)
        from models.mlb_model import load_model, predict_from_season_stats
        games = get_schedule_range(days_ahead=1)
        today_str = datetime.date.today().isoformat()
        today_games = [g for g in games if g.get("date", "") == today_str]

        _phase(1)
        from data.soccer_fetcher import get_todays_fixtures
        from models.soccer_model import load_model as load_soccer
        soccer_model = load_soccer()
        soccer_preds = []
        fixtures = get_todays_fixtures(["EPL", "ESP", "GER"])
        if soccer_model:
            for f in fixtures:
                pred = soccer_model.predict(f["home_team"], f["away_team"])
                pred.update({"home_team": f["home_team"], "away_team": f["away_team"],
                              "league": f.get("league", ""), "game_time": f.get("game_time"),
                              "date": f.get("date", today_str)})
                soccer_preds.append(pred)

        _phase(2)
        stats = build_game_dataset([MLB_SEASONS[0]])
        mlb_model = load_model()
        mlb_preds = []
        for g in today_games:
            if g.get("status", "") not in ("Preview", "Pre-Game", "Scheduled", "Warmup", ""):
                continue
            pred = predict_from_season_stats(g["home_team"], g["away_team"], stats, mlb_model)
            pred.update({"home_team": g["home_team"], "away_team": g["away_team"],
                          "home_starter": g.get("home_starter", "TBD"),
                          "away_starter": g.get("away_starter", "TBD"),
                          "predicted_total": estimate_game_total(g["home_team"], g["away_team"], stats),
                          "game_time": g.get("game_time"), "date": g.get("date", today_str)})
            mlb_preds.append(pred)

        _phase(3)
        all_injuries = []
        try:
            from data.injury_fetcher import fetch_all_injuries
            from data.db import save_injuries, get_injuries
            raw_inj = fetch_all_injuries()
            for lk, lst in raw_inj.items():
                sport = "mlb" if lk == "mlb" else "soccer"
                save_injuries(sport, lst)
                all_injuries.extend(lst)
            if not all_injuries:
                all_injuries = get_injuries()
        except Exception as e:
            print(f"[dashboard] injuries: {e}")

        _phase(4)
        from data.odds_fetcher import (get_live_odds, odds_to_dataframe,
                                        get_totals_odds, totals_to_dataframe)
        ml_rows, tot_rows = [], []
        for sk in ["mlb", "epl", "laliga", "bundesliga"]:
            raw = get_live_odds(sk, markets="h2h")
            if raw:
                d = odds_to_dataframe(raw); d["sport_key"] = sk; ml_rows.append(d)
            raw2 = get_totals_odds(sk)
            if raw2:
                d2 = totals_to_dataframe(raw2); d2["sport_key"] = sk; tot_rows.append(d2)
        ml_df  = pd.concat(ml_rows,  ignore_index=True) if ml_rows  else pd.DataFrame()
        tot_df = pd.concat(tot_rows, ignore_index=True) if tot_rows else pd.DataFrame()

        def sfilt(df, is_mlb):
            if df.empty: return df
            mask = df.get("sport_key", pd.Series(dtype=str)).isin({"mlb", "baseball_mlb"})
            return df[mask] if is_mlb else df[~mask]

        _phase(5)
        from analysis.value_finder import find_value_bets, find_totals_bets, build_parlay
        win_bets    = (find_value_bets(mlb_preds, sfilt(ml_df, True),  sport="mlb")
                     + find_value_bets(soccer_preds, sfilt(ml_df, False), sport="soccer"))
        totals_bets = (find_totals_bets(mlb_preds, sfilt(tot_df, True),  sport="mlb")
                     + find_totals_bets(soccer_preds, sfilt(tot_df, False), sport="soccer"))

        _phase(6)
        parlays    = build_parlay(win_bets + totals_bets)
        prop_stats = get_starters_props_batch(today_games, MLB_SEASONS[0])

        _phase(7)
        upcoming_games = []
        try:
            from data.db import save_value_bets, get_upcoming_games
            save_value_bets(win_bets, "win")
            save_value_bets(totals_bets, "totals")
            upcoming_games = get_upcoming_games(days_ahead=1)
        except Exception as e:
            print(f"[dashboard] db: {e}")
            upcoming_games = _build_upcoming(games, fixtures)

        def _clean(obj):
            if isinstance(obj, list):  return [_clean(x) for x in obj]
            if isinstance(obj, dict):  return {k: _clean(v) for k, v in obj.items()}
            try:
                import numpy as np
                if isinstance(obj, np.integer):  return int(obj)
                if isinstance(obj, np.floating): return float(obj)
                if isinstance(obj, np.bool_):    return bool(obj)
            except ImportError: pass
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()
            return obj

        for i, b in enumerate(win_bets):    b["_id"] = f"win_{i}"
        for i, b in enumerate(totals_bets): b["_id"] = f"tot_{i}"
        for i, p in enumerate(prop_stats):  p["_id"] = f"prop_{i}"

        p2 = [p for p in parlays if p["num_legs"] == 2][:10]
        p3 = [p for p in parlays if p["num_legs"] == 3][:10]
        p4 = [p for p in parlays if p["num_legs"] == 4][:10]

        with _lock:
            _state.update({
                "status": "done", "phase": "Complete",
                "last_updated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
                "win_bets": _clean(win_bets), "totals_bets": _clean(totals_bets),
                "prop_stats": _clean(prop_stats), "parlays": _clean(parlays[:5]),
                "parlays_2": _clean(p2), "parlays_3": _clean(p3), "parlays_4": _clean(p4),
                "mlb_games": len(mlb_preds), "soccer_games": len(soccer_preds),
                "upcoming_games": _clean(upcoming_games),
                "injuries": _clean(all_injuries[:100]),
            })
    except Exception:
        err = traceback.format_exc()
        with _lock:
            _state["status"] = "error"; _state["phase"] = "Error"; _state["error"] = err


def _build_upcoming(mlb_games, soccer_fixtures):
    today    = datetime.date.today().isoformat()
    tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()
    result = []
    for g in mlb_games:
        d = g.get("date", today)
        if d in (today, tomorrow):
            result.append({"sport": "mlb", "league": "MLB",
                "home_team": g["home_team"], "away_team": g["away_team"],
                "game_date": d, "game_time": g.get("game_time"),
                "status": g.get("status", "Scheduled"),
                "home_starter": g.get("home_starter"),
                "away_starter": g.get("away_starter")})
    for f in soccer_fixtures:
        d = f.get("date", today)
        if d in (today, tomorrow):
            result.append({"sport": "soccer", "league": f.get("league", ""),
                "home_team": f["home_team"], "away_team": f["away_team"],
                "game_date": d, "game_time": f.get("game_time"),
                "status": f.get("status", "Scheduled")})
    result.sort(key=lambda x: (x.get("game_date", ""), x.get("game_time") or "99:99"))
    return result


@app.route("/")
def index():
    with _lock: state = dict(_state)
    return render_template("dashboard.html", state=state, bankroll=BANKROLL)

@app.route("/api/run", methods=["POST"])
def api_run():
    with _lock:
        if _state["status"] == "running":
            return jsonify({"ok": False, "msg": "Analysis already running"}), 409
        _state["status"] = "running"
        _state["phase"]  = _PHASES[0]
        _state["phase_idx"] = 0
    threading.Thread(target=_run_analysis, daemon=True).start()
    return jsonify({"ok": True, "msg": "Analysis started"})

@app.route("/api/status")
def api_status():
    with _lock:
        return jsonify({k: _state[k] for k in
            ("status", "phase", "phase_idx", "phase_total", "last_updated", "error")})

@app.route("/api/games")
def api_games():
    try:
        from data.db import get_upcoming_games
        return jsonify(get_upcoming_games(days_ahead=1))
    except Exception:
        return jsonify([])

if __name__ == "__main__":
    try:
        from data.db import init_schema
        init_schema()
    except Exception as e:
        print(f"[dashboard] DB init: {e}")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
"""

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "dashboard.py")
out = os.path.normpath(out)
with open(out, "w", encoding="utf-8") as fh:
    fh.write(content)
print(f"Written {len(content.splitlines())} lines to {out}")
