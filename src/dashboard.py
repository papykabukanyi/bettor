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
import queue
import datetime
import threading
import traceback
import warnings
import atexit
import tempfile
import re

from flask import Flask, render_template, jsonify, request, Response

SRC_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SRC_DIR)
from config import BANKROLL, MIN_VALUE_EDGE, KELLY_FRACTION, MLB_SEASONS, et_today

# Dashboard uses a lower edge threshold to show more picks
# (bot tracks accuracy; high-edge filter is for real-money staking only)
_DASH_MIN_EDGE = 0.02
_DAILY_LOCK_HOUR_ET = int(os.getenv("DAILY_LOCK_HOUR_ET", "5"))
_DAILY_LOCK_MINUTE_ET = int(os.getenv("DAILY_LOCK_MINUTE_ET", "0"))

app = Flask(__name__, template_folder="templates")

# ─── Gunicorn / production: init once per worker ─────────────────────────────
_worker_initialized = False
_worker_init_lock   = threading.Lock()
_scheduler          = None

_BG_LOCK_PATH = os.path.join(tempfile.gettempdir(), "bettor_bg.lock")
_BG_LOCK_FD = None
_BG_IS_LEADER = False


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _release_bg_lock():
    global _BG_LOCK_FD
    try:
        if _BG_LOCK_FD is not None:
            os.close(_BG_LOCK_FD)
            _BG_LOCK_FD = None
        if os.path.exists(_BG_LOCK_PATH):
            os.remove(_BG_LOCK_PATH)
    except Exception:
        pass


def _acquire_bg_lock() -> bool:
    """Return True if this process becomes the background-job leader."""
    global _BG_LOCK_FD
    try:
        fd = os.open(_BG_LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        _BG_LOCK_FD = fd
        atexit.register(_release_bg_lock)
        return True
    except FileExistsError:
        try:
            with open(_BG_LOCK_PATH, "r", encoding="utf-8") as f:
                pid = int((f.read() or "0").strip() or "0")
            if _pid_alive(pid):
                return False
        except Exception:
            return False
        try:
            os.remove(_BG_LOCK_PATH)
        except Exception:
            return False
        return _acquire_bg_lock()


def _init_worker():
    global _worker_initialized, _scheduler, _BG_IS_LEADER
    with _worker_init_lock:
        if _worker_initialized:
            return
        _worker_initialized = True
    try:
        from data.db import init_schema
        init_schema()
    except Exception as e:
        print(f"[worker-init] DB init: {e}")
    _BG_IS_LEADER = _acquire_bg_lock()
    if _BG_IS_LEADER:
        _scheduler = _start_scheduler()
        _start_live_scores()
        _auto_boot_analysis()
    else:
        _scheduler = None
        _start_cache_poller()


@app.before_request
def _lazy_init():
    """Triggered once per-worker on the very first request."""
    if not _worker_initialized:
        _init_worker()

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
    "last_updated_ts":  None,
    "error":            None,
    "game_cards_today":    [],
    "game_cards_tomorrow": [],
    "best_parlays":        [],
    "player_props":        [],
    "elite_parlay":        None,
    "live_scores":         {},
    "logs":                [],
}
_lock = threading.Lock()

# ─── Server-Sent Events broadcast ────────────────────────────────────────────
_sse_clients: list[queue.Queue] = []
_sse_lock = threading.Lock()


def _sse_broadcast(event: str, data: dict):
    """Push an SSE message to every connected browser tab."""
    msg = f"event: {event}\ndata: {json.dumps(data)}\n\n"
    with _sse_lock:
        dead = []
        for q in _sse_clients:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _sse_clients.remove(q)


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


def _norm_gk(s: str) -> str:
    """Normalize game key so 'Away @ Home' == 'Away@Home'."""
    return s.replace(" @ ", "@").replace(" @", "@").replace("@ ", "@").strip()


def _et_calendar_today() -> datetime.date:
    """Return calendar date in America/New_York (no 10 PM cutover)."""
    try:
        import zoneinfo

        eastern = zoneinfo.ZoneInfo("America/New_York")
        return datetime.datetime.now(tz=eastern).date()
    except Exception:
        try:
            import pytz

            eastern = pytz.timezone("America/New_York")
            return datetime.datetime.now(tz=eastern).date()
        except Exception:
            return datetime.date.today()


def _team_words(full_name: str) -> list:
    """Return meaningful words from a team name (skip short/common words)."""
    return [w for w in full_name.lower().split() if len(w) > 3]


def _line_value(val) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        s = str(val).strip()
    except Exception:
        return None
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        pass
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def _is_public_prop(p: dict) -> bool:
    if (p.get("direction") or "").upper() != "OVER":
        return False
    lv = _line_value(p.get("line"))
    if lv is not None and lv <= 0.5:
        return False
    return True


def _build_card(game, bets, props, when):
    ht  = game.get("home_team", "")
    at  = game.get("away_team", "")
    gk      = f"{at}@{ht}"
    gk_norm = _norm_gk(gk)
    alt_gk  = game.get("game_key", gk)
    alt_norm = _norm_gk(alt_gk)

    # Also store a reversed form for reverse-key matches
    rev_gk  = _norm_gk(f"{ht}@{at}")

    card = {
        "game_key":     gk,
        "game_date":    game.get("date") or game.get("game_date"),
        "game_datetime": game.get("game_datetime"),
        "when":         when,
        "when_label":   when,
        "home_team":    ht,
        "away_team":    at,
        "home_starter": game.get("home_starter", "TBD"),
        "away_starter": game.get("away_starter", "TBD"),
        "game_time":    game.get("game_time", ""),
        "status":       game.get("status", ""),
        "home_score":   game.get("home_score"),
        "away_score":   game.get("away_score"),
        "inning":       game.get("inning", ""),
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

    def _key_matches(k: str) -> bool:
        kn = _norm_gk(k)
        return kn in (gk_norm, alt_norm, rev_gk) or gk_norm in kn or alt_norm in kn

    for bet in bets:
        bk = bet.get("game_key", bet.get("game", ""))
        if not _key_matches(bk):
            continue
        bt = bet.get("bet_type", "")
        if bt in _GAME_BET_TYPES:
            current = card[bt]
            if current is None or bet.get("safety", 0) > current.get("safety", 0):
                card[bt] = bet

    ht_words = _team_words(ht)
    at_words = _team_words(at)
    for p in props:
        pk = p.get("game_key", p.get("game", ""))
        if not _key_matches(pk):
            # fallback: check if team name appears in prop team field
            pt = (p.get("team", "")).lower()
            if not any(w in pt for w in ht_words + at_words):
                continue
        team_lc = (p.get("team", "")).lower()
        # Assign to home or away side
        if any(w in team_lc for w in ht_words):
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


def _run_analysis(lock_date: datetime.date | None = None):
    warnings.filterwarnings("ignore")
    import pandas as pd

    with _lock:
        _state["status"]    = "running"
        _state["error"]     = None
        _state["logs"]      = []
        _state["phase"]     = _PHASES[0]
        _state["phase_idx"] = 0

    try:
        # Decide whether this run should lock/save today's picks
        need_preds = False
        need_props = False
        try:
            from data.db import has_predictions_for_date, has_prop_picks_for_date
            if lock_date is None:
                today = et_today()
                need_preds = not has_predictions_for_date(today)
                need_props = not has_prop_picks_for_date(today)
                if need_preds or need_props:
                    lock_date = today
                    _log(f"[lock] Daily picks missing for {today} - locking this run")
            else:
                need_preds = not has_predictions_for_date(lock_date)
                need_props = not has_prop_picks_for_date(lock_date)
        except Exception as _le:
            _log(f"[lock] Daily lock check failed: {_le}")
            if lock_date is None:
                lock_date = et_today()
            need_preds = True
            need_props = True

        # Dashboard display should follow calendar ET date (no betting-day cutover).
        today_date = _et_calendar_today()
        today_str  = today_date.isoformat()
        tomorrow_str = (today_date + datetime.timedelta(days=1)).isoformat()

        # ── Auto-backfill 30 days of data and retrain model ──────────────────
        _log("[backfill] Running 30-day backfill before analysis...")
        try:
            from data.history_ingest import backfill_news, backfill_injuries, backfill_game_results
            n_news = backfill_news(days_back=30)
            _log(f"[backfill] News rows: {n_news}")
            n_inj = backfill_injuries(days_back=30)
            _log(f"[backfill] Injury rows: {n_inj}")
            n_games = backfill_game_results(days_back=30)
            _log(f"[backfill] Game results saved: {n_games}")
        except Exception as _bf_e:
            _log(f"[backfill] Backfill error (continuing): {_bf_e}")
        # Retrain deferred to after team_stats is loaded (later in pipeline)

        # Statuses that should be hidden from Today cards because they're fully done/cancelled.
        _DONE_STATUSES = {
            "final", "game over", "f", "completed early", "completed",
            "cancelled", "suspended", "postponed",
        }

        _phase(0)
        _log("Fetching MLB schedule...")
        from data.mlb_fetcher import get_schedule_range
        all_games = get_schedule_range(days_ahead=2)
        today_games    = [g for g in all_games if g.get("date", "") == today_str]
        tomorrow_games = [g for g in all_games if g.get("date", "") == tomorrow_str]

        # Display all still-relevant today games, including active/in-progress.
        upcoming_today    = [g for g in today_games
                     if g.get("status", "").lower() not in _DONE_STATUSES]
        upcoming_tomorrow = tomorrow_games  # tomorrow games can't be past
        _log(f"Schedule: {len(today_games)} today ({len(upcoming_today)} upcoming), "
             f"{len(tomorrow_games)} tomorrow")

        _phase(1)
        _log("Loading team stats and model...")
        from data.mlb_fetcher import build_game_dataset
        from models.mlb_model import load_model, train as train_model
        # Use 3 seasons for robust team differentiation (early-season 2026 data is sparse)
        team_stats = build_game_dataset(MLB_SEASONS[:3])
        model      = load_model()
        _log(f"Team stats rows: {len(team_stats)} (seasons: {sorted(team_stats['season'].unique().tolist(), reverse=True) if not team_stats.empty else 'none'})")
        # Auto-train model if not found or team_stats updated
        if model is None and not team_stats.empty:
            _log("No saved model — training now...")
            try:
                model = train_model(team_stats, verbose=False)
                _log("Model trained and saved.")
            except Exception as e:
                _log(f"Model training failed: {e}")

        # Retrain enhanced model with backfilled game results
        if lock_date:
            try:
                from models.mlb_model import retrain_with_history
                retrain_with_history(team_stats)
                model = load_model()  # reload after retrain
                _log("[backfill] Enhanced model retrained and reloaded.")
            except Exception as _rt_e:
                _log(f"[backfill] Retrain skipped: {_rt_e}")

        _phase(2)
        _log("Fetching injury reports...")
        injuries = []
        injured_names = set()
        def _is_out(status: str) -> bool:
            s = (status or "").lower()
            return any(k in s for k in (
                "out", "il", "dl", "inj", "dtd", "day-to-day",
                "suspended", "inactive", "placed", "covid",
            ))
        try:
            from data.injury_fetcher import fetch_all_injuries
            from data.db import save_injuries
            raw_inj = fetch_all_injuries()
            mlb_inj = raw_inj.get("mlb", [])
            save_injuries("mlb", mlb_inj)
            injuries = mlb_inj
        except Exception as e:
            _log(f"Injuries skipped: {e}")

        if not injuries:
            try:
                from data.db import get_injuries
                injuries = get_injuries(sport="mlb")
                _log(f"Injuries loaded from DB: {len(injuries)}")
            except Exception as e:
                _log(f"Injuries DB fallback skipped: {e}")

        injured_names = {i.get("player_name", "") for i in injuries if _is_out(i.get("status", ""))}

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
        import models.mlb_predictor as _mp
        from models.mlb_predictor import predict_game, build_game_bets
        # Lower edge threshold so dashboard shows all value picks (accuracy tracking)
        _orig_edge = _mp.MIN_VALUE_EDGE
        _mp.MIN_VALUE_EDGE = _DASH_MIN_EDGE

        all_bets = []
        def _is_terminal_status(s: str) -> bool:
            sl = (s or "").lower()
            return any(k in sl for k in (
                "final", "game over", "completed", "cancelled", "suspended", "postponed"
            ))

        for g in today_games + tomorrow_games:
            ht = g.get("home_team", "")
            at = g.get("away_team", "")
            if not ht or not at:
                continue
            st = g.get("status", "")
            if _is_terminal_status(st):
                _log(f"Skip {at}@{ht} status={st!r}")
                continue
            try:
                pred = predict_game(ht, at, team_stats, model, injuries=injuries)
                hw   = pred.get("home_win_prob", 0.5)
                _log(f"  {at}@{ht}: home win prob={hw:.1%}")
                gk   = pred["game_key"]
                # Try exact key then reversed
                orow = (odds_by_game.get(gk)
                        or odds_by_game.get(f"{at}@{ht}")
                        or odds_by_game.get(f"{ht}@{at}"))
                gb   = build_game_bets(g, pred, orow)
                all_bets.extend(gb)
                _log(f"  bets for {gk}: {len(gb)}")
            except Exception as e:
                _log(f"Prediction error {ht} vs {at}: {e}")
        _mp.MIN_VALUE_EDGE = _orig_edge
        _log(f"Game bets generated: {len(all_bets)}")

        _phase(5)
        _log("Building player prop bets...")
        all_props = []
        try:
            from data.mlb_fetcher import get_starters_props_batch, get_hitter_props_batch
            from models.mlb_predictor import build_player_prop_bets

            prop_odds = {}
            try:
                from data.odds_fetcher import get_player_props_odds
                prop_odds = get_player_props_odds("baseball_mlb") or {}
            except Exception as e:
                _log(f"Prop odds skipped: {e}")

            starter_props = get_starters_props_batch(today_games + tomorrow_games, MLB_SEASONS[0])
            try:
                hitter_props = get_hitter_props_batch(today_games + tomorrow_games, MLB_SEASONS[0])
            except Exception:
                hitter_props = []

            raw_props = starter_props + hitter_props
            _log(f"Raw props fetched: {len(raw_props)}")
            all_props = build_player_prop_bets(
                raw_props,
                injured_players=injured_names,
                odds_lines=prop_odds,
                min_prob=0.60,
                only_over=True,
            )
            if not all_props:
                _log("No qualifying props at 0.60 - relaxing to 0.55")
                all_props = build_player_prop_bets(
                    raw_props,
                    injured_players=injured_names,
                    odds_lines=prop_odds,
                    min_prob=0.55,
                    only_over=True,
                )
            _log(f"Prop bets built: {len(all_props)}")
            raw_props_count = len(all_props)
            all_props = [p for p in all_props if _is_public_prop(p)]
            if len(all_props) != raw_props_count:
                _log(f"Public props tracked: {len(all_props)}/{raw_props_count}")
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

        def _date_str(val) -> str:
            if isinstance(val, datetime.datetime):
                return val.date().isoformat()
            if isinstance(val, datetime.date):
                return val.isoformat()
            return str(val) if val is not None else ""

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
            for p in all_props:
                game_str = p.get("game") or p.get("game_key") or ""
                away_team = ""
                home_team = ""
                if "@" in game_str:
                    parts = [s.strip() for s in game_str.split("@")]
                    if len(parts) == 2:
                        away_team, home_team = parts[0], parts[1]
                pick_label = f"{p.get('name','')} {p.get('direction','')} {p.get('line','')} {p.get('prop_label','')}".strip()
                pred_rows.append({
                    "game_key":     p.get("game_key", p.get("game", "")),
                    "sport":        "mlb",
                    "bet_type":     "player_prop",
                    "pick":         pick_label,
                    "line":         p.get("line"),
                    "odds_am":      p.get("odds_am"),
                    "dec_odds":     p.get("dec_odds", 2.0),
                    "confidence":   p.get("confidence", p.get("conf", 50)),
                    "model_prob":   p.get("model_prob", 0.0),
                    "safety_label": p.get("safety_label", "MODERATE"),
                    "edge":         p.get("edge", 0.0),
                    "stake_usd":    0.0,
                    "ev":           p.get("ev", 0.0),
                    "game_date":    p.get("date", today_str),
                    "game_time":    p.get("game_time", ""),
                    "home_team":    home_team,
                    "away_team":    away_team,
                    "matchup":      game_str,
                    "sentiment_score": p.get("signal_sentiment"),
                })
            if lock_date:
                lock_str = _date_str(lock_date)
                if need_preds:
                    pred_rows_locked = [p for p in pred_rows if _date_str(p.get("game_date")) == lock_str]
                    save_predictions(pred_rows_locked)
                else:
                    _log(f"[lock] Predictions already locked for {lock_str} - skipping DB save")

                if need_props:
                    props_locked = [p for p in all_props if _date_str(p.get("date")) == lock_str]
                    save_prop_picks(props_locked, game_date=lock_date)
                else:
                    _log(f"[lock] Props already locked for {lock_str} - skipping DB save")
            else:
                _log("[lock] Daily picks already locked - skipping DB save")
        except Exception as e:
            _log(f"DB save error: {e}")

        # Display only upcoming games; past/live ones were predicted & saved for accuracy tracking
        today_cards    = [_build_card(g, all_bets, all_props, "TODAY")    for g in upcoming_today]
        tomorrow_cards = [_build_card(g, all_bets, all_props, "TOMORROW") for g in upcoming_tomorrow]

        def _card_score(c):
            s = [b["safety"] for b in [c.get("moneyline"), c.get("run_line"), c.get("total")] if b]
            return sum(s) / len(s) if s else 0

        today_cards.sort(key=_card_score, reverse=True)
        tomorrow_cards.sort(key=_card_score, reverse=True)
        all_props_flat = sorted(all_props, key=lambda x: x.get("safety", 0), reverse=True)

        now_ts = datetime.datetime.now(datetime.timezone.utc)
        last_updated = now_ts.strftime("%Y-%m-%d %H:%M")

        try:
            save_analysis_cache({
                "game_cards_today":    today_cards,
                "game_cards_tomorrow": tomorrow_cards,
                "best_parlays":        best_parlays,
                "player_props":        all_props_flat,
                "last_updated":        last_updated,
            }, cache_date=et_today())
        except Exception as e:
            _log(f"Cache save error: {e}")

        with _lock:
            _state.update({
                "status":              "done",
                "phase":               "Complete",
                "last_updated":        last_updated,
                "last_updated_ts":     now_ts.isoformat(),
                "game_cards_today":    _clean(today_cards),
                "game_cards_tomorrow": _clean(tomorrow_cards),
                "best_parlays":        _clean(best_parlays),
                "player_props":        _clean(all_props_flat),
            })

        # Auto-resolve outcomes for recent past predictions + props + tracked parlays
        try:
            from models.mlb_predictor import (
                resolve_game_outcomes, resolve_prop_outcomes, resolve_tracked_parlays
            )
            n_games  = resolve_game_outcomes(days_back=3)
            n_props  = resolve_prop_outcomes(days_back=3)
            n_parlay = resolve_tracked_parlays(days_back=3)
            _log(f"Auto-resolved: {n_games} game preds, {n_props} props, {n_parlay} parlays")
        except Exception as e:
            _log(f"Auto-resolve skipped: {e}")

        # Build elite parlay and store in state
        try:
            from models.mlb_predictor import build_elite_parlay
            elite = build_elite_parlay(all_bets + all_props)
            with _lock:
                _state["elite_parlay"] = _clean(elite)
            if elite:
                _log(f"Elite parlay built: {elite['n_legs']} legs, "
                     f"combined prob={elite['combined_prob']}%, "
                     f"EV={elite['combined_ev']:.3f}")
            else:
                _log("Elite parlay: no qualifying legs (need 80%+ prob + positive EV + ELITE)")
        except Exception as e:
            _log(f"Elite parlay skipped: {e}")

        # Self-improvement: check calibration and retrain if needed
        try:
            from models.mlb_model import auto_improve
            improve_result = auto_improve(team_stats, min_resolved=50, verbose=False)
            _log(f"[calibration] {improve_result.get('msg', '')}  "
                 f"(ECE={improve_result.get('ece')}, resolved={improve_result.get('total_resolved')})")
        except Exception as e:
            _log(f"Auto-improve skipped: {e}")

        _log(f"Analysis complete — {len(today_cards)} today (upcoming), "
             f"{len(tomorrow_cards)} tomorrow, {len(all_props_flat)} props")

        # Broadcast full state update to all SSE clients
        _sse_broadcast("state_update", {
            "status":              "done",
            "last_updated":        last_updated,
            "game_cards_today":    _clean(today_cards),
            "game_cards_tomorrow": _clean(tomorrow_cards),
            "best_parlays":        _clean(best_parlays),
            "player_props":        _clean(all_props_flat),
            "elite_parlay":        _clean(_state.get("elite_parlay")),
        })

    except Exception:
        err = traceback.format_exc()
        _log(f"Analysis FAILED:\n{err}")
        with _lock:
            _state["status"] = "error"
            _state["phase"]  = "Error"
            _state["error"]  = err
        _sse_broadcast("status", {"status": "error", "error": err[:300]})


@app.route("/")
def index():
    with _lock:
        state = dict(_state)
    return render_template(
        "dashboard.html",
        state=state,
        bankroll=BANKROLL,
        phases=_PHASES,
        today_cards=[],
        tomorrow_cards=[],
        best_parlays=[],
        all_props=[],
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
            cache_updated_at_iso = _state.get("last_updated_ts")
            cache_age_min = None
            if cache_updated_at_iso:
                try:
                    dt = datetime.datetime.fromisoformat(cache_updated_at_iso)
                    now = datetime.datetime.now(datetime.timezone.utc) if dt.tzinfo else datetime.datetime.utcnow()
                    cache_age_min = max(0, int((now - dt).total_seconds() / 60))
                except Exception:
                    cache_age_min = None
            return jsonify({
                "ok":                  True,
                "status":              _state["status"],
                "last_updated":        _state["last_updated"],
                "cache_updated_at_iso": cache_updated_at_iso,
                "cache_age_min":        cache_age_min,
                "game_cards_today":    _state["game_cards_today"],
                "game_cards_tomorrow": _state["game_cards_tomorrow"],
                "best_parlays":        _state["best_parlays"],
                "player_props":        _state["player_props"],
                "elite_parlay":        _state.get("elite_parlay"),
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
                    "best_parlays": [], "player_props": [],
                    "elite_parlay": None})


@app.route("/api/logs")
def api_logs():
    with _lock:
        return jsonify({"logs": list(_state.get("logs", []))})


@app.route("/api/parlay/build-elite", methods=["POST"])
def api_parlay_build_elite():
    """Build and save one elite parlay from the current in-memory state."""
    with _lock:
        all_bets  = list(_state.get("best_parlays", []))   # fallback
        all_props = list(_state.get("player_props", []))
        # Reconstruct from game cards for bet-level picks
        raw_picks = []
        for card in (_state.get("game_cards_today", []) +
                     _state.get("game_cards_tomorrow", [])):
            for slot in ("moneyline", "run_line", "total", "f5_moneyline",
                         "f5_total", "home_team_total", "away_team_total"):
                b = card.get(slot)
                if b:
                    raw_picks.append(b)
        raw_picks += all_props

    if not raw_picks:
        return jsonify({"ok": False, "msg": "No picks available — run analysis first"})

    try:
        from models.mlb_predictor import build_elite_parlay
        parlay = build_elite_parlay(raw_picks)
        if parlay:
            with _lock:
                _state["elite_parlay"] = _clean(parlay)
            return jsonify({"ok": True, "parlay": _clean(parlay)})
        else:
            return jsonify({
                "ok":  False,
                "msg": "No qualifying legs found. Need model_prob ≥ 80%, positive EV, and ELITE safety.",
            })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/calibration")
def api_calibration():
    """Return model calibration stats (ECE + per-bin breakdown)."""
    days = int(request.args.get("days", 90))
    try:
        from data.db import get_calibration_data
        return jsonify({"ok": True, "calibration": get_calibration_data(days_back=days)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/parlay/performance")
def api_parlay_performance():
    """Win/loss/ROI stats for all tracked parlays."""
    try:
        from data.db import get_parlay_performance_stats
        return jsonify({"ok": True, "stats": get_parlay_performance_stats()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/parlay/auto-resolve", methods=["POST"])
def api_parlay_auto_resolve():
    """Auto-resolve pending tracked parlays based on leg prediction outcomes."""
    try:
        from models.mlb_predictor import resolve_tracked_parlays
        n = resolve_tracked_parlays(days_back=7)
        return jsonify({"ok": True, "resolved": n,
                        "msg": f"Resolved {n} tracked parlay(s)"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/auto-improve", methods=["POST"])
def api_auto_improve():
    """Trigger calibration check + conditional model retrain."""
    try:
        import pandas as pd
        from data.mlb_fetcher import build_game_dataset
        from models.mlb_model import auto_improve
        team_stats = build_game_dataset(MLB_SEASONS[:3])
        result = auto_improve(team_stats, min_resolved=50, verbose=True)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/backfill", methods=["POST"])
def api_backfill():
    """
    Run the full backfill pipeline (news → injuries → game scores → retrain).
    Accepts optional JSON body: {"days_back": 3}
    Runs in a background thread; returns immediately.
    """
    with _lock:
        if _state["status"] == "running":
            return jsonify({"ok": False, "msg": "Analysis already running"}), 409

    days_back = int((request.get_json(silent=True) or {}).get("days_back", 3))

    def _run_backfill():
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
        try:
            with _lock:
                _state["status"] = "running"
                _state["phase"]  = "Backfilling data"
            _log(f"[backfill] Starting backfill (days_back={days_back})")

            # News
            try:
                from data.history_ingest import backfill_news
                n = backfill_news(days_back=days_back)
                _log(f"[backfill] News rows ingested: {n}")
            except Exception as e:
                _log(f"[backfill] News error: {e}")

            # Injuries
            try:
                from data.history_ingest import backfill_injuries
                inj = backfill_injuries(days_back=days_back)
                _log(f"[backfill] Injury rows: {inj}")
            except Exception as e:
                _log(f"[backfill] Injury error: {e}")

            # Game results
            try:
                from data.history_ingest import backfill_game_results
                n_games = backfill_game_results(days_back=days_back)
                _log(f"[backfill] Completed games saved: {n_games}")
            except Exception as e:
                _log(f"[backfill] Game results error: {e}")

            # Retrain
            try:
                import pandas as pd
                from data.mlb_fetcher import build_game_dataset
                from models.mlb_model import retrain_with_history
                team_stats = build_game_dataset(MLB_SEASONS[:3])
                retrain_with_history(team_stats)
                _log("[backfill] Model retrained and saved.")
            except Exception as e:
                _log(f"[backfill] Retrain error: {e}")

            with _lock:
                _state["status"] = "idle"
                _state["phase"]  = "Backfill complete"
        except Exception as e:
            _log(f"[backfill] Fatal error: {e}")
            with _lock:
                _state["status"] = "idle"
                _state["error"]  = str(e)

    threading.Thread(target=_run_backfill, daemon=True).start()
    return jsonify({"ok": True, "msg": f"Backfill started (days_back={days_back})"})


@app.route("/api/performance")
def api_performance():
    try:
        from data.db import get_performance_stats
        return jsonify({"ok": True, "stats": get_performance_stats()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/prop-performance")
def api_prop_performance():
    try:
        from data.db import get_prop_performance_stats
        return jsonify({"ok": True, "stats": get_prop_performance_stats()})
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
        from models.mlb_predictor import (
            resolve_game_outcomes, resolve_prop_outcomes, resolve_tracked_parlays
        )
        n_games  = resolve_game_outcomes(days_back=3)
        n_props  = resolve_prop_outcomes(days_back=3)
        n_parlay = resolve_tracked_parlays(days_back=7)
        return jsonify({
            "ok": True,
            "resolved_games":  n_games,
            "resolved_props":  n_props,
            "resolved_parlays": n_parlay,
            "msg": f"Resolved {n_games} game preds + {n_props} props + {n_parlay} parlays",
        })
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
        return jsonify({"ok": True, "parlays": _clean(get_tracked_parlays(include_resolved=False))})
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
    data = request.get_json(force=True, silent=True) or {}
    numbers = data.get("numbers") or []  # list of {phone, label} from localStorage
    try:
        from sms import send_daily_picks_to_all
        with _lock:
            state = {
                "best_parlays": list(_state.get("best_parlays", [])),
                "game_cards_today": list(_state.get("game_cards_today", [])),
            }
        result = send_daily_picks_to_all(state, numbers=numbers if numbers else None)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/sms/send-parlay", methods=["POST"])
def api_sms_send_parlay():
    data = request.get_json(force=True) or {}
    try:
        from sms import send_parlay_to_all
        result = send_parlay_to_all(data)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/live-scores")
def api_live_scores():
    """Poll MLB Stats API for today's in-progress / final game scores."""
    try:
        import statsapi as mlbstatsapi
        today = _et_calendar_today().strftime("%m/%d/%Y")
        raw = mlbstatsapi.schedule(start_date=today, end_date=today) or []
        games = []
        for g in raw:
            status = g.get("status", "")
            games.append({
                "game_pk":     g.get("game_id"),
                "home_team":   g.get("home_name", ""),
                "away_team":   g.get("away_name", ""),
                "home_score":  g.get("home_score"),
                "away_score":  g.get("away_score"),
                "status":      status,
                "inning":      g.get("current_inning", ""),
                "inning_half": g.get("inning_state", ""),
                "game_key":    f"{g.get('away_name','')}@{g.get('home_name','')}",
            })
        return jsonify({"ok": True, "games": games})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "games": []})


# ─── Live-score background watcher ───────────────────────────────────────────
_live_score_timer = None
_LIVE_SCORE_INTERVAL = 120  # seconds (2 min)
_cache_poll_timer = None
_CACHE_POLL_INTERVAL = int(os.getenv("CACHE_POLL_INTERVAL_SEC", "120"))


def _sync_state_from_cache(broadcast: bool = False) -> bool:
    """Refresh in-memory state from DB cache when available."""
    try:
        from data.db import get_analysis_cache
        cached = get_analysis_cache(max_age_hours=22)
        if not cached:
            return False
        cache_iso = cached.get("cache_updated_at_iso")
        with _lock:
            if cache_iso and cache_iso == _state.get("last_updated_ts"):
                return False
            if _state.get("status") == "running":
                return False
            _state.update({
                "game_cards_today":    cached.get("game_cards_today", []),
                "game_cards_tomorrow": cached.get("game_cards_tomorrow", []),
                "best_parlays":        cached.get("best_parlays", []),
                "player_props":        cached.get("player_props", []),
                "elite_parlay":        cached.get("elite_parlay"),
                "last_updated":        cached.get("last_updated"),
            })
            if cache_iso:
                _state["last_updated_ts"] = cache_iso
        if broadcast:
            _sse_broadcast("state_update", {
                "status":              "done",
                "last_updated":        cached.get("last_updated"),
                "game_cards_today":    cached.get("game_cards_today", []),
                "game_cards_tomorrow": cached.get("game_cards_tomorrow", []),
                "best_parlays":        cached.get("best_parlays", []),
                "player_props":        cached.get("player_props", []),
                "elite_parlay":        cached.get("elite_parlay"),
            })
        return True
    except Exception:
        return False


def _start_cache_poller():
    """Non-leader workers poll DB cache and broadcast updates to their SSE clients."""
    global _cache_poll_timer
    if _cache_poll_timer is not None:
        return

    def _tick():
        global _cache_poll_timer
        _sync_state_from_cache(broadcast=True)
        _cache_poll_timer = threading.Timer(_CACHE_POLL_INTERVAL, _tick)
        _cache_poll_timer.daemon = True
        _cache_poll_timer.start()

    _tick()


def _start_live_scores():
    if _live_score_timer is None:
        _poll_live_scores()


def _poll_live_scores():
    """Runs in background: updates live scores and auto-resolves completed games."""
    global _live_score_timer
    try:
        import statsapi as mlbstatsapi
        # Use calendar ET date (no 10 PM cutover) to avoid dropping late live games.
        today = _et_calendar_today()
        today_str = today.strftime("%m/%d/%Y")
        raw = mlbstatsapi.schedule(start_date=today_str, end_date=today_str) or []
        
        # More robust status matching - include any active/live game statuses
        def _is_active_game(status):
            s = (status or "").lower()
            # Include: live games, warmup, pre-game, final/completed (for scores)
            return any(k in s for k in (
                "progress", "live", "warmup", "pre-game", "pregame",
                "final", "game over", "completed", "delay", "challenge",
                "postpon", "cancel", "suspend"
            ))
        
        live = [g for g in raw if _is_active_game(g.get("status", ""))]
        live_map = {
            _norm_gk(f"{g.get('away_name','')}@{g.get('home_name','')}"): {
                "home_score":  g.get("home_score"),
                "away_score":  g.get("away_score"),
                "status":      g.get("status"),
                "inning":      g.get("current_inning", ""),
                "inning_half": g.get("inning_state", ""),
            }
            for g in live
        }
        with _lock:
            _state["live_scores"] = live_map

        # Broadcast full live map every poll (including empty) so clients can clear stale entries.
        _sse_broadcast("live_scores", {"scores": live_map})

        # Auto-resolve finished games (non-blocking, errors suppressed)
        def _is_final_status(status):
            s = (status or "").lower()
            return any(k in s for k in ("final", "game over", "completed"))
        
        if any(_is_final_status(g.get("status", "")) for g in live):
            try:
                from models.mlb_predictor import resolve_game_outcomes, resolve_prop_outcomes
                n_g = resolve_game_outcomes(days_back=1)
                n_p = resolve_prop_outcomes(days_back=1)
                if n_g or n_p:
                    print(f"[live-scores] Auto-resolved {n_g} predictions, {n_p} props")
                    # Push updated performance to clients
                    try:
                        from data.db import get_performance_stats, get_parlay_performance_stats
                        _sse_broadcast("performance_update", {
                            "stats":        get_performance_stats(),
                            "parlay_stats": get_parlay_performance_stats(),
                        })
                    except Exception:
                        pass
            except Exception as exc:
                print(f"[live-scores] resolve error: {exc}")
    except Exception as exc:
        print(f"[live-scores] poll error: {exc}")
    finally:
        _live_score_timer = threading.Timer(_LIVE_SCORE_INTERVAL, _poll_live_scores)
        _live_score_timer.daemon = True
        _live_score_timer.start()


# Live-score polling is started by the leader worker.


# ─── SSE stream endpoint ─────────────────────────────────────────────────────
@app.route("/api/stream")
def api_stream():
    """Long-lived SSE connection. Each browser tab connects once."""
    q: queue.Queue = queue.Queue(maxsize=50)
    with _sse_lock:
        _sse_clients.append(q)

    # Immediately send current state so fresh page loads fill in fast
    with _lock:
        hello = {
            "status":              _state.get("status", "idle"),
            "last_updated":        _state.get("last_updated"),
            "game_cards_today":    _state.get("game_cards_today", []),
            "game_cards_tomorrow": _state.get("game_cards_tomorrow", []),
            "best_parlays":        _state.get("best_parlays", []),
            "player_props":        _state.get("player_props", []),
            "elite_parlay":        _state.get("elite_parlay"),
            "live_scores":         _state.get("live_scores", {}),
        }
    try:
        q.put_nowait(f"event: state_update\ndata: {json.dumps(hello)}\n\n")
    except queue.Full:
        pass

    def _generate():
        yield ": connected\n\n"
        while True:
            try:
                msg = q.get(timeout=25)
                yield msg
            except queue.Empty:
                yield ": ping\n\n"   # keep-alive

    def _cleanup(resp):
        with _sse_lock:
            if q in _sse_clients:
                _sse_clients.remove(q)
        return resp

    response = Response(
        _generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
    response.call_on_close(lambda: _cleanup(None))
    return response


# ─── APScheduler: auto-run analysis every 5 hours ───────────────────────────
def _scheduled_analysis(force: bool = False, lock_today: bool = False):
    """Called by APScheduler. Skips if already running or cache is very fresh."""
    with _lock:
        if _state["status"] == "running":
            return
        last_ts = _state.get("last_updated_ts")

    # Skip if ran within the last ~5 hours
    if not force and last_ts:
        try:
            dt = datetime.datetime.fromisoformat(last_ts)
            now = datetime.datetime.now(datetime.timezone.utc) if dt.tzinfo else datetime.datetime.utcnow()
            age_min = (now - dt).total_seconds() / 60
            if age_min < 295:
                return
        except Exception:
            pass

    print(f"[scheduler] Auto-running analysis at {datetime.datetime.now().strftime('%H:%M')}")
    with _lock:
        _state["status"]    = "running"
        _state["phase"]     = _PHASES[0]
        _state["phase_idx"] = 0
    _sse_broadcast("status", {"status": "running", "phase": _PHASES[0]})
    lock_date = et_today() if lock_today else None
    threading.Thread(target=_run_analysis, args=(lock_date,), daemon=True).start()


def _start_scheduler():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.interval import IntervalTrigger
        from apscheduler.triggers.cron import CronTrigger
        sched = BackgroundScheduler(daemon=True)
        # Run every 5 hours
        sched.add_job(_scheduled_analysis, IntervalTrigger(minutes=300), id="auto_analysis",
                      max_instances=1, coalesce=True)
        # Daily lock run (ET morning)
        sched.add_job(
            lambda: _scheduled_analysis(force=True, lock_today=True),
            CronTrigger(hour=_DAILY_LOCK_HOUR_ET, minute=_DAILY_LOCK_MINUTE_ET,
                        timezone="America/New_York"),
            id="daily_lock",
            max_instances=1,
            coalesce=True,
        )
        sched.start()
        print("[scheduler] APScheduler started — analysis every 5 hours")
        return sched
    except Exception as e:
        print(f"[scheduler] Could not start APScheduler: {e}")
        return None


def _auto_boot_analysis():
    """On startup: load cache, and if it's stale (>30 min) kick off analysis immediately."""
    try:
        from data.db import get_analysis_cache
        cached = get_analysis_cache(max_age_hours=22)
        if cached:
            with _lock:
                _state.update({
                    "game_cards_today":    cached.get("game_cards_today", []),
                    "game_cards_tomorrow": cached.get("game_cards_tomorrow", []),
                    "best_parlays":        cached.get("best_parlays", []),
                    "player_props":        cached.get("player_props", []),
                    "last_updated":        cached.get("last_updated"),
                })
            print(f"[boot] Loaded cache from DB (last updated: {cached.get('last_updated')})")
            # Check age (treat cache as fresh for ~5 hours)
            lu = cached.get("last_updated", "")
            age_min = cached.get("cache_age_min")
            stale = True
            if age_min is not None:
                stale = age_min > 300
            elif lu:
                try:
                    dt = datetime.datetime.strptime(lu, "%Y-%m-%d %H:%M")
                    age_min = (datetime.datetime.utcnow() - dt).total_seconds() / 60
                    stale = age_min > 300
                except Exception:
                    pass
            if stale:
                print("[boot] Cache is stale — triggering fresh analysis...")
                threading.Thread(target=_run_analysis, daemon=True).start()
            elif not cached.get("player_props"):
                print("[boot] Cache missing props - triggering fresh analysis...")
                threading.Thread(target=_run_analysis, daemon=True).start()
        else:
            print("[boot] No DB cache — triggering fresh analysis...")
            threading.Thread(target=_run_analysis, daemon=True).start()
    except Exception as e:
        print(f"[boot] Auto-boot error: {e}")
        threading.Thread(target=_run_analysis, daemon=True).start()


if __name__ == "__main__":
    try:
        from data.db import init_schema
        init_schema()
    except Exception as e:
        print(f"[dashboard] DB init: {e}")
    _BG_IS_LEADER = _acquire_bg_lock()
    if _BG_IS_LEADER:
        _scheduler = _start_scheduler()
        _start_live_scores()
        _auto_boot_analysis()
    else:
        _scheduler = None
        _start_cache_poller()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
