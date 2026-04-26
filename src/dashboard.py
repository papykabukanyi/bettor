"""
Betting Bot — Web Dashboard (MLB)
==================================
Routes:
  GET  /                      → main dashboard (SSR empty arrays + phases)
  POST /api/run               → kick off analysis in background thread
  GET  /api/status            → {status, phase, phase_idx, phase_total, last_updated, error}
  GET  /api/cached-state      → {ok, game_cards_today, game_cards_tomorrow,
                                   best_parlays, player_props, last_updated, status}
  GET  /api/logs              → {logs: [...]}
  GET  /api/performance       → {ok, stats}
  GET  /api/predictions       → {ok, predictions}
  POST /api/resolve-outcomes  → {ok, resolved}
  POST /api/parlay/save       → {ok}
  GET  /api/parlay/list       → {ok, parlays}
  POST /api/parlay/resolve    → {ok}
  GET  /api/phone-numbers     → {numbers}
  POST /api/phone-numbers/add → {ok, msg}
  POST /api/phone-numbers/remove → {ok}
  POST /api/sms/send          → {ok} / {error}
"""

import os
import sys
import json
import datetime
import threading
import traceback
import warnings

from flask import Flask, render_template, jsonify, request

SRC_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SRC_DIR)
from config import BANKROLL, MIN_VALUE_EDGE, KELLY_FRACTION, MLB_SEASONS

app = Flask(__name__, template_folder="templates")

_PHASES = [
    "Fetching MLB schedule",
    "Loading team stats & model",
    "Fetching injuries",
    "Fetching live odds",
    "Running game predictions",
    "Building player props",
    "Building parlays",
    "Fetching sentiment",
    "Saving to database",
]

_state = {
    "status":           "idle",
    "phase":            "",
    "phase_idx":        0,
    "phase_total":      len(_PHASES),
    "last_updated":     None,
    "error":            None,
    "game_cards_today":    [],
    "game_cards_tomorrow": [],
    "best_parlays":        [],
    "player_props":        [],
    "logs":                [],
}
_lock = threading.Lock()


def _log(msg):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with _lock:
        _state["logs"].append(line)
        if len(_state["logs"]) > 200:
            _state["logs"] = _state["logs"][-200:]


def _phase(idx, name=""):
    with _lock:
        _state["phase"]     = name or (_PHASES[idx] if idx < len(_PHASES) else name)
        _state["phase_idx"] = idx


def _clean(obj):
    if isinstance(obj, list):
        return [_clean(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    try:
        import numpy as np
        if isinstance(obj, np.integer):  return int(obj)
        if isinstance(obj, np.floating): return float(obj)
        if isinstance(obj, np.bool_):    return bool(obj)
        if isinstance(obj, np.ndarray):  return obj.tolist()
    except ImportError:
        pass
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    return obj


def _build_card(game, bets, props, when):
    ht  = game.get("home_team", "")
    at  = game.get("away_team", "")
    gk  = f"{at}@{ht}"
    alt_gk = game.get("game_key", gk)

    card = {
        "game_key":     gk,
        "when":         when,
        "when_label":   when,
        "home_team":    ht,
        "away_team":    at,
        "home_starter": game.get("home_starter", "TBD"),
        "away_starter": game.get("away_starter", "TBD"),
        "game_time":    game.get("game_time", ""),
        "moneyline":       None,
        "run_line":        None,
        "total":           None,
        "f5_moneyline":    None,
        "f5_total":        None,
        "home_team_total": None,
        "away_team_total": None,
        "home_props":  [],
        "away_props":  [],
    }

    _GAME_BET_TYPES = ("moneyline", "run_line", "total", "f5_moneyline",
                        "f5_total", "home_team_total", "away_team_total")
    for bet in bets:
        bk = bet.get("game_key", bet.get("game", ""))
        if bk not in (gk, alt_gk):
            continue
        bt = bet.get("bet_type", "")
        if bt in _GAME_BET_TYPES:
            current = card[bt]
            if current is None or bet.get("safety", 0) > current.get("safety", 0):
                card[bt] = bet

    ht_lc = ht.lower()
    at_lc = at.lower()
    for p in props:
        pk = p.get("game_key", p.get("game", ""))
        if pk not in (gk, alt_gk) and gk not in pk and alt_gk not in pk:
            continue
        team_lc = (p.get("team", "")).lower()
        if any(part in team_lc for part in ht_lc.split() if len(part) > 3):
            card["home_props"].append(p)
        else:
            card["away_props"].append(p)

    safety_scores = [b["safety"] for b in
                     [card["moneyline"], card["run_line"], card["total"]] if b]
    avg = sum(safety_scores) / len(safety_scores) if safety_scores else 0.45
    if avg >= 0.72:   card["overall_safety_label"] = "ELITE"
    elif avg >= 0.60: card["overall_safety_label"] = "SAFE"
    elif avg >= 0.50: card["overall_safety_label"] = "MODERATE"
    else:             card["overall_safety_label"] = "RISKY"

    return card


def _run_analysis():
    warnings.filterwarnings("ignore")
    import pandas as pd

    with _lock:
        _state["status"]    = "running"
        _state["error"]     = None
        _state["logs"]      = []
        _state["phase"]     = _PHASES[0]
        _state["phase_idx"] = 0

    try:
        today_str    = datetime.date.today().isoformat()
        tomorrow_str = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()

        _phase(0)
        _log("Fetching MLB schedule...")
        from data.mlb_fetcher import get_schedule_range
        all_games = get_schedule_range(days_ahead=2)
        today_games    = [g for g in all_games if g.get("date", "") == today_str]
        tomorrow_games = [g for g in all_games if g.get("date", "") == tomorrow_str]
        _log(f"Schedule: {len(today_games)} today, {len(tomorrow_games)} tomorrow")

        _phase(1)
        _log("Loading team stats and model...")
        from data.mlb_fetcher import build_game_dataset
        from models.mlb_model import load_model
        team_stats = build_game_dataset([MLB_SEASONS[0]])
        model      = load_model()
        _log(f"Team stats rows: {len(team_stats)}")

        _phase(2)
        _log("Fetching injury reports...")
        injuries = []
        injured_names = set()
        try:
            from data.injury_fetcher import fetch_all_injuries
            from data.db import save_injuries
            raw_inj = fetch_all_injuries()
            mlb_inj = raw_inj.get("mlb", [])
            save_injuries("mlb", mlb_inj)
            injuries = mlb_inj
            injured_names = {i.get("player_name", "") for i in injuries
                             if i.get("status", "").lower() in ("out", "il", "dl", "dtd")}
        except Exception as e:
            _log(f"Injuries skipped: {e}")

        _phase(3)
        _log("Fetching live odds...")
        odds_by_game = {}
        try:
            from data.odds_fetcher import get_live_odds, odds_to_dataframe
            raw_odds = get_live_odds("baseball_mlb", markets="h2h,totals")
            if raw_odds:
                odds_df = odds_to_dataframe(raw_odds)
                for _, row in odds_df.iterrows():
                    key = f"{row.get('away_team','')}@{row.get('home_team','')}"
                    odds_by_game[key] = row.to_dict()
                _log(f"Odds loaded for {len(odds_by_game)} games")
        except Exception as e:
            _log(f"Odds skipped: {e}")

        _phase(4)
        _log("Running game predictions...")
        from models.mlb_predictor import predict_game, build_game_bets

        all_bets = []
        for g in today_games + tomorrow_games:
            ht = g.get("home_team", "")
            at = g.get("away_team", "")
            if not ht or not at:
                continue
            st = g.get("status", "")
            if st and st not in ("Preview", "Pre-Game", "Scheduled", "Warmup", ""):
                continue
            try:
                pred = predict_game(ht, at, team_stats, model, injuries=injuries)
                gk   = pred["game_key"]
                orow = odds_by_game.get(gk) or odds_by_game.get(f"{at}@{ht}")
                gb   = build_game_bets(g, pred, orow)
                all_bets.extend(gb)
            except Exception as e:
                _log(f"Prediction error {ht} vs {at}: {e}")
        _log(f"Game bets generated: {len(all_bets)}")

        _phase(5)
        _log("Building player prop bets...")
        all_props = []
        try:
            from data.mlb_fetcher import get_starters_props_batch, get_hitter_props_batch
            from models.mlb_predictor import build_player_prop_bets

            prop_odds = {}
            try:
                from data.odds_fetcher import get_player_prop_odds
                prop_odds = get_player_prop_odds("baseball_mlb") or {}
            except Exception as e:
                _log(f"Prop odds skipped: {e}")

            starter_props = get_starters_props_batch(today_games + tomorrow_games, MLB_SEASONS[0])
            try:
                hitter_props = get_hitter_props_batch(today_games + tomorrow_games, MLB_SEASONS[0])
            except Exception:
                hitter_props = []

            raw_props = starter_props + hitter_props
            _log(f"Raw props fetched: {len(raw_props)}")
            all_props = build_player_prop_bets(raw_props,
                                               injured_players=injured_names,
                                               odds_lines=prop_odds)
            _log(f"Prop bets built: {len(all_props)}")
        except Exception as e:
            _log(f"Props error: {e}")

        _phase(6)
        _log("Building parlays...")
        from models.mlb_predictor import build_parlays
        best_parlays = build_parlays(all_bets + all_props, max_legs=5, top_n=5)
        _log(f"Parlays built: {len(best_parlays)}")

        _phase(7)
        _log("Fetching sentiment (non-blocking)...")
        try:
            from data.sentiment import get_game_sentiments
            for g in today_games[:5]:
                get_game_sentiments(g.get("home_team", ""), g.get("away_team", ""))
        except Exception as e:
            _log(f"Sentiment skipped: {e}")

        _phase(8)
        _log("Saving to database and building cards...")
        from data.db import save_predictions, save_prop_picks, save_analysis_cache

        try:
            pred_rows = []
            for b in all_bets:
                pred_rows.append({
                    "game_key":     b.get("game_key", ""),
                    "sport":        "mlb",
                    "bet_type":     b.get("bet_type", ""),
                    "pick":         b.get("pick", ""),
                    "line":         b.get("line"),
                    "odds_am":      b.get("odds_am"),
                    "dec_odds":     b.get("dec_odds", 2.0),
                    "confidence":   b.get("confidence", 50),
                    "safety_label": b.get("safety_label", "MODERATE"),
                    "edge":         b.get("edge", 0.0),
                    "stake_usd":    b.get("stake_usd", 0.0),
                    "ev":           b.get("ev", 0.0),
                    "game_date":    b.get("game_date", today_str),
                    "matchup":      b.get("matchup", ""),
                })
            save_predictions(pred_rows)
            save_prop_picks(all_props)
        except Exception as e:
            _log(f"DB save error: {e}")

        today_cards    = [_build_card(g, all_bets, all_props, "TODAY")    for g in today_games]
        tomorrow_cards = [_build_card(g, all_bets, all_props, "TOMORROW") for g in tomorrow_games]

        def _card_score(c):
            s = [b["safety"] for b in [c.get("moneyline"), c.get("run_line"), c.get("total")] if b]
            return sum(s) / len(s) if s else 0

        today_cards.sort(key=_card_score, reverse=True)
        tomorrow_cards.sort(key=_card_score, reverse=True)
        all_props_flat = sorted(all_props, key=lambda x: x.get("safety", 0), reverse=True)

        last_updated = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

        try:
            save_analysis_cache({
                "game_cards_today":    today_cards,
                "game_cards_tomorrow": tomorrow_cards,
                "best_parlays":        best_parlays,
                "player_props":        all_props_flat,
                "last_updated":        last_updated,
            })
        except Exception as e:
            _log(f"Cache save error: {e}")

        with _lock:
            _state.update({
                "status":              "done",
                "phase":               "Complete",
                "last_updated":        last_updated,
                "game_cards_today":    _clean(today_cards),
                "game_cards_tomorrow": _clean(tomorrow_cards),
                "best_parlays":        _clean(best_parlays),
                "player_props":        _clean(all_props_flat),
            })

        _log(f"Analysis complete — {len(today_cards)} today, {len(tomorrow_cards)} tomorrow, {len(all_props_flat)} props")

    except Exception:
        err = traceback.format_exc()
        _log(f"Analysis FAILED:\n{err}")
        with _lock:
            _state["status"] = "error"
            _state["phase"]  = "Error"
            _state["error"]  = err


@app.route("/")
def index():
    with _lock:
        state = dict(_state)
    return render_template(
        "dashboard.html",
        state=state,
        bankroll=BANKROLL,
        phases=_PHASES,
        today_cards=json.dumps([]),
        tomorrow_cards=json.dumps([]),
        best_parlays=json.dumps([]),
        all_props=json.dumps([]),
    )


@app.route("/api/run", methods=["POST"])
def api_run():
    with _lock:
        if _state["status"] == "running":
            return jsonify({"ok": False, "msg": "Analysis already running"}), 409
        _state["status"]    = "running"
        _state["phase"]     = _PHASES[0]
        _state["phase_idx"] = 0
    threading.Thread(target=_run_analysis, daemon=True).start()
    return jsonify({"ok": True, "msg": "Analysis started"})


@app.route("/api/status")
def api_status():
    with _lock:
        return jsonify({k: _state[k] for k in
            ("status", "phase", "phase_idx", "phase_total", "last_updated", "error")})


@app.route("/api/cached-state")
def api_cached_state():
    with _lock:
        if _state.get("game_cards_today") or _state.get("player_props"):
            return jsonify({
                "ok":                  True,
                "status":              _state["status"],
                "last_updated":        _state["last_updated"],
                "game_cards_today":    _state["game_cards_today"],
                "game_cards_tomorrow": _state["game_cards_tomorrow"],
                "best_parlays":        _state["best_parlays"],
                "player_props":        _state["player_props"],
            })

    try:
        from data.db import get_analysis_cache
        cached = get_analysis_cache(max_age_hours=22)
        if cached:
            cached["ok"] = True
            return jsonify(cached)
    except Exception:
        pass

    return jsonify({"ok": False, "status": "idle",
                    "game_cards_today": [], "game_cards_tomorrow": [],
                    "best_parlays": [], "player_props": []})


@app.route("/api/logs")
def api_logs():
    with _lock:
        return jsonify({"logs": list(_state.get("logs", []))})


@app.route("/api/performance")
def api_performance():
    try:
        from data.db import get_performance_stats
        return jsonify({"ok": True, "stats": get_performance_stats()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/predictions")
def api_predictions():
    days    = int(request.args.get("days", 30))
    outcome = request.args.get("outcome")
    try:
        from data.db import get_predictions
        preds = get_predictions(days=days, outcome=outcome or None)
        return jsonify({"ok": True, "predictions": _clean(preds)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "predictions": []})


@app.route("/api/resolve-outcomes", methods=["POST"])
def api_resolve_outcomes():
    try:
        from models.mlb_predictor import resolve_game_outcomes
        n = resolve_game_outcomes(days_back=3)
        return jsonify({"ok": True, "resolved": n, "msg": f"Resolved {n} predictions"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/parlay/save", methods=["POST"])
def api_parlay_save():
    data = request.get_json(force=True) or {}
    try:
        from data.db import save_tracked_parlay
        save_tracked_parlay(
            name=data.get("name", "My Parlay"),
            legs=data.get("legs", []),
            combined_odds=float(data.get("combined_odds", 0)),
            stake_usd=float(data.get("stake_usd", 0)),
        )
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/parlay/list")
def api_parlay_list():
    try:
        from data.db import get_tracked_parlays
        return jsonify({"ok": True, "parlays": _clean(get_tracked_parlays(include_resolved=True))})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "parlays": []})


@app.route("/api/parlay/resolve", methods=["POST"])
def api_parlay_resolve():
    data = request.get_json(force=True) or {}
    try:
        from data.db import resolve_tracked_parlay
        resolve_tracked_parlay(
            parlay_id=int(data.get("id", 0)),
            outcome=data.get("outcome", "WIN"),
            payout=float(data.get("payout", 0)),
        )
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/phone-numbers")
def api_phone_numbers():
    try:
        from data.db import get_phone_numbers
        return jsonify({"ok": True, "numbers": get_phone_numbers()})
    except Exception as e:
        return jsonify({"ok": False, "numbers": [], "error": str(e)})


@app.route("/api/phone-numbers/add", methods=["POST"])
def api_phone_add():
    data  = request.get_json(force=True) or {}
    phone = (data.get("phone") or "").strip()
    label = (data.get("label") or "").strip()
    if not phone:
        return jsonify({"ok": False, "msg": "Phone number required"}), 400
    try:
        from data.db import add_phone_number
        ok, msg = add_phone_number(phone, label)
        return jsonify({"ok": ok, "msg": msg})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)})


@app.route("/api/phone-numbers/remove", methods=["POST"])
def api_phone_remove():
    data  = request.get_json(force=True) or {}
    phone = (data.get("phone") or "").strip()
    if not phone:
        return jsonify({"ok": False, "msg": "Phone number required"}), 400
    try:
        from data.db import remove_phone_number
        return jsonify({"ok": remove_phone_number(phone)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/sms/send", methods=["POST"])
def api_sms_send():
    try:
        from sms import send_daily_picks
        with _lock:
            props = list(_state.get("player_props", []))
        send_daily_picks(props)
        return jsonify({"ok": True, "msg": "SMS sent"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


if __name__ == "__main__":
    try:
        from data.db import init_schema
        init_schema()
    except Exception as e:
        print(f"[dashboard] DB init: {e}")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
