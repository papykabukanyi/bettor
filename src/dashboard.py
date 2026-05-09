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
  GET  /api/email/recipients  → {recipients}
  POST /api/email/send        → {ok} / {error}
  POST /api/email/send-parlay → {ok}
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
from config import BANKROLL, MIN_VALUE_EDGE, KELLY_FRACTION, MLB_SEASONS, et_today, SPORT as CONFIG_SPORT

# Dashboard uses a lower edge threshold to show more picks
# (bot tracks accuracy; high-edge filter is for real-money staking only)
_DASH_MIN_EDGE = 0.02
_DAILY_LOCK_HOUR_ET = int(os.getenv("DAILY_LOCK_HOUR_ET", "5"))
_DAILY_LOCK_MINUTE_ET = int(os.getenv("DAILY_LOCK_MINUTE_ET", "0"))
_AUTO_ANALYSIS_INTERVAL_MIN = int(os.getenv("AUTO_ANALYSIS_INTERVAL_MIN", "0"))
_ACTIVE_SPORT = str(CONFIG_SPORT or os.getenv("SPORT", "all") or "all").strip().lower()
if _ACTIVE_SPORT not in {"mlb", "soccer", "all"}:
    _ACTIVE_SPORT = "all"

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

_MLB_PHASES = [
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

_SOCCER_PHASES = [
    "Fetching tournament fixtures",
    "Running soccer model + sentiment",
    "Building player props",
    "Building parlays",
    "Saving to database",
]

_ALL_SPORTS_PHASES = [
    "Discovering available sports",
    "Fetching live odds feed",
    "Ranking best available bets",
    "Building cards",
]

if _ACTIVE_SPORT == "soccer":
    _PHASES = _SOCCER_PHASES
elif _ACTIVE_SPORT == "mlb":
    _PHASES = _MLB_PHASES
else:
    _PHASES = _ALL_SPORTS_PHASES


_MULTI_SPORT_CACHE = {
    "snapshot": None,
    "ts": 0.0,
}
_MULTI_SPORT_CACHE_TTL_SEC = int(os.getenv("MULTI_SPORT_CACHE_TTL_SEC", "180"))
_MAX_ODDS_SPORTS = int(os.getenv("MAX_ODDS_SPORTS", "12"))


def _env_flag(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


_ALL_SPORTS_SENTIMENT_MAX_GAMES = max(1, int(os.getenv("ALL_SPORTS_SENTIMENT_MAX_GAMES", "24")))
_ALL_SPORTS_SENTIMENT_PLAYERS_PER_GAME = max(1, int(os.getenv("ALL_SPORTS_SENTIMENT_PLAYERS_PER_GAME", "8")))
_ALL_SPORTS_SENTIMENT_INCLUDE_NEWS = _env_flag("ALL_SPORTS_SENTIMENT_INCLUDE_NEWS", "0")
_ALL_SPORTS_STRICT_SENTIMENT_ONLY = _env_flag("ALL_SPORTS_STRICT_SENTIMENT_ONLY", "0")

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


def _compose_game_key(away_team: str, home_team: str,
                      game_datetime=None, game_date=None, game_time=None) -> str:
    """Build a stable unique key for a scheduled game instance."""
    match_key = _norm_gk(f"{away_team}@{home_team}")
    suffix = str(game_datetime or "").strip()
    if not suffix:
        gd = str(game_date or "").strip()
        gt = str(game_time or "").strip()
        suffix = f"{gd}T{gt}".strip("T")
    return f"{match_key}#{suffix}" if suffix else match_key


def _card_date_from_iso(game_datetime) -> str:
    try:
        raw = str(game_datetime or "").strip()
        if not raw:
            return ""
        return datetime.datetime.fromisoformat(raw).date().isoformat()
    except Exception:
        return ""


def _normalize_card_list(cards, expected_date: str | None = None) -> list:
    out = []
    seen = set()
    for raw in cards or []:
        if not isinstance(raw, dict):
            continue
        card = dict(raw)
        away = card.get("away_team", "")
        home = card.get("home_team", "")
        match_key = card.get("match_key") or _norm_gk(f"{away}@{home}")
        game_date = card.get("game_date") or _card_date_from_iso(card.get("game_datetime"))
        if expected_date and game_date and game_date != expected_date:
            continue
        card["match_key"] = match_key
        if game_date and not card.get("game_date"):
            card["game_date"] = game_date
        card["game_key"] = _compose_game_key(
            away,
            home,
            card.get("game_datetime"),
            card.get("game_date"),
            card.get("game_time"),
        )
        dedupe_key = card.get("game_key") or match_key
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(card)
    return out


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
    if _ACTIVE_SPORT in {"soccer", "all"}:
        return True
    if (p.get("direction") or "").upper() != "OVER":
        return False
    lv = _line_value(p.get("line"))
    if lv is not None and lv <= 0.5:
        return False
    return True


def _build_card(game, bets, props, when):
    ht  = game.get("home_team", "")
    at  = game.get("away_team", "")
    sport_group = _infer_sport_group(
        game.get("sport") or game.get("competition") or game.get("league") or _ACTIVE_SPORT
    )
    competition_name = str(game.get("competition_name") or game.get("league") or "").strip()
    league_name = str(game.get("league") or competition_name or sport_group.upper() or "SPORT").strip()
    match_key = _norm_gk(game.get("match_key") or f"{at}@{ht}")
    unique_gk = _compose_game_key(
        at,
        ht,
        game.get("game_datetime"),
        game.get("date") or game.get("game_date"),
        game.get("game_time"),
    )
    gk_norm = _norm_gk(match_key)
    unique_norm = _norm_gk(unique_gk)
    alt_gk  = game.get("match_key", match_key)
    alt_norm = _norm_gk(alt_gk)

    # Also store a reversed form for reverse-key matches
    rev_gk  = _norm_gk(f"{ht}@{at}")

    card = {
        "game_key":     unique_gk,
        "match_key":    match_key,
        "sport":        sport_group,
        "league":       league_name,
        "competition":  str(game.get("competition") or "").strip(),
        "competition_name": competition_name or league_name,
        "game_pk":      game.get("game_pk") or game.get("game_id") or game.get("external_id"),
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

    def _slot_for_bet(bet: dict) -> str | None:
        bt = str(bet.get("bet_type", ""))
        if bt in _GAME_BET_TYPES:
            return bt
        if _ACTIVE_SPORT in {"soccer", "all"}:
            if bt in {"1X2", "Draw No Bet"}:
                return "moneyline"
            if bt == "Goals O/U":
                return "total"
            if bt == "BTTS":
                return "run_line"
        return None

    def _key_matches(k: str) -> bool:
        kn = _norm_gk(k)
        return (
            kn in (gk_norm, alt_norm, rev_gk, unique_norm)
            or gk_norm in kn
            or alt_norm in kn
            or kn in unique_norm
        )

    for bet in bets:
        bk = bet.get("game_key", bet.get("game", ""))
        if not _key_matches(bk):
            continue
        slot = _slot_for_bet(bet)
        if slot:
            current = card[slot]
            if current is None or bet.get("safety", 0) > current.get("safety", 0):
                card[slot] = bet

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


def _safety_label_from_prob(prob: float) -> str:
    p = float(prob or 0.5)
    if p >= 0.72:
        return "ELITE"
    if p >= 0.60:
        return "SAFE"
    if p >= 0.50:
        return "MODERATE"
    return "RISKY"


def _safety_score_from_label(label: str | None) -> float:
    v = str(label or "MODERATE").upper()
    if v == "ELITE":
        return 0.80
    if v == "SAFE":
        return 0.65
    if v == "MODERATE":
        return 0.52
    return 0.45


def _normalize_soccer_bet(game: dict, bet: dict, default_date: str) -> dict:
    row = dict(bet or {})
    home = game.get("home_team", "")
    away = game.get("away_team", "")
    match_key = _norm_gk(game.get("match_key") or row.get("match_key") or f"{away}@{home}")
    game_key = row.get("game_key") or _compose_game_key(
        away,
        home,
        game.get("game_datetime"),
        game.get("date") or game.get("game_date"),
        game.get("game_time"),
    )

    try:
        odds_am = int(float(row.get("odds_am", row.get("odds", -110)) or -110))
    except (TypeError, ValueError):
        odds_am = -110
    try:
        dec_odds = float(row.get("dec_odds") or (1 + (odds_am / 100.0) if odds_am > 0 else 1 + (100.0 / abs(odds_am))))
    except Exception:
        dec_odds = 1.91

    model_prob = float(row.get("model_prob", row.get("probability", 0.5)) or 0.5)
    safety_label = row.get("safety_label") or _safety_label_from_prob(model_prob)
    confidence = int(row.get("confidence") or round(model_prob * 100))

    row.update({
        "sport": "soccer",
        "pick": row.get("pick") or row.get("pick_label") or row.get("bet_type") or "Soccer Market",
        "match_key": match_key,
        "game_key": game_key,
        "game_date": row.get("game_date") or game.get("date") or game.get("game_date") or default_date,
        "game_time": row.get("game_time") or game.get("game_time") or "",
        "home_team": row.get("home_team") or home,
        "away_team": row.get("away_team") or away,
        "odds_am": odds_am,
        "dec_odds": round(dec_odds, 4),
        "model_prob": max(0.01, min(0.99, model_prob)),
        "confidence": confidence,
        "safety_label": safety_label,
        "safety": float(row.get("safety", _safety_score_from_label(safety_label))),
    })
    row.setdefault("worth_score", 0.0)
    row.setdefault("worth_it", False)
    row.setdefault("worth_reason", "")
    row.setdefault("market_popularity", 0.0)
    row.setdefault("market_mentions", 0)
    return row


def _normalize_soccer_prop(game: dict, prop: dict, default_date: str) -> dict:
    row = dict(prop or {})
    home = game.get("home_team", "")
    away = game.get("away_team", "")
    match_key = _norm_gk(game.get("match_key") or row.get("match_key") or f"{away}@{home}")
    game_key = row.get("game_key") or _compose_game_key(
        away,
        home,
        game.get("game_datetime"),
        game.get("date") or game.get("game_date"),
        game.get("game_time"),
    )
    try:
        odds_am = int(float(row.get("odds_am", -110) or -110))
    except (TypeError, ValueError):
        odds_am = -110
    try:
        dec_odds = float(row.get("dec_odds") or (1 + (odds_am / 100.0) if odds_am > 0 else 1 + (100.0 / abs(odds_am))))
    except Exception:
        dec_odds = 1.91

    model_prob = float(row.get("model_prob", 0.5) or 0.5)
    safety_label = row.get("safety_label") or _safety_label_from_prob(model_prob)

    row.update({
        "sport": "soccer",
        "game": row.get("game") or f"{away} @ {home}",
        "match_key": match_key,
        "game_key": game_key,
        "date": row.get("date") or game.get("date") or game.get("game_date") or default_date,
        "game_date": row.get("game_date") or game.get("date") or game.get("game_date") or default_date,
        "game_time": row.get("game_time") or game.get("game_time") or "",
        "home_team": row.get("home_team") or home,
        "away_team": row.get("away_team") or away,
        "direction": str(row.get("direction") or "OVER").upper(),
        "odds_am": odds_am,
        "dec_odds": round(dec_odds, 4),
        "model_prob": max(0.01, min(0.99, model_prob)),
        "confidence": int(row.get("confidence") or round(model_prob * 100)),
        "safety_label": safety_label,
        "safety": float(row.get("safety", _safety_score_from_label(safety_label))),
    })
    row.setdefault("worth_score", 0.0)
    row.setdefault("worth_it", False)
    row.setdefault("worth_reason", "")
    row.setdefault("market_popularity", 0.0)
    row.setdefault("market_mentions", 0)
    return row


def _infer_sport_group(sport_key: str) -> str:
    raw = str(sport_key or "").strip().lower()
    if not raw:
        return "other"
    if "_" in raw:
        return raw.split("_", 1)[0]
    return raw


def _prob_from_american(odds) -> float | None:
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    if o > 0:
        return 100.0 / (o + 100.0)
    return abs(o) / (abs(o) + 100.0)


def _datetime_to_et_parts(iso_value: str) -> tuple[str, str]:
    raw = str(iso_value or "").strip()
    if not raw:
        return "", ""
    try:
        dt = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        try:
            import zoneinfo

            eastern = zoneinfo.ZoneInfo("America/New_York")
            et = dt.astimezone(eastern)
        except Exception:
            et = dt
        return et.date().isoformat(), et.strftime("%H:%M")
    except Exception:
        return "", ""


def _rank_label(prob: float) -> str:
    if prob >= 0.72:
        return "ELITE"
    if prob >= 0.60:
        return "SAFE"
    if prob >= 0.50:
        return "MODERATE"
    return "RISKY"


def _slug_token(text: str) -> str:
    raw = re.sub(r"[^a-z0-9]+", "_", str(text or "").strip().lower())
    return raw.strip("_") or "unknown"


def _time_hhmm(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    m = re.match(r"^(\d{1,2}):(\d{2})", s)
    if not m:
        return ""
    return f"{int(m.group(1)):02d}:{m.group(2)}"


def _collect_fallback_games_for_all_sports(today: datetime.date, tomorrow: datetime.date) -> list[dict]:
    rows: list[dict] = []
    allowed_dates = {today.isoformat(), tomorrow.isoformat()}

    def _push_game(
        *,
        sport_group: str,
        league: str,
        competition: str,
        competition_name: str,
        home: str,
        away: str,
        game_date: str,
        game_time: str = "",
        game_datetime: str = "",
        status: str = "Scheduled",
        home_score=None,
        away_score=None,
    ):
        if not home or not away:
            return
        gd = str(game_date or "").strip()
        if gd and gd not in allowed_dates:
            return
        gt = _time_hhmm(game_time)
        match_key = _norm_gk(f"{away}@{home}")
        game_key = _compose_game_key(away, home, game_datetime, gd, gt)
        rows.append({
            "sport": _infer_sport_group(sport_group),
            "league": league or competition_name or competition,
            "competition": competition,
            "competition_name": competition_name or league or competition,
            "home_team": home,
            "away_team": away,
            "date": gd,
            "game_date": gd,
            "game_time": gt,
            "game_datetime": game_datetime or "",
            "status": status or "Scheduled",
            "home_score": home_score,
            "away_score": away_score,
            "match_key": match_key,
            "game_key": game_key,
        })

    # 0) Prefer DB-cached schedule first for speed.
    has_mlb = False
    has_soccer = False
    try:
        from data.db import get_upcoming_games

        for g in (get_upcoming_games(days_ahead=2) or []):
            sport_group = _infer_sport_group(g.get("sport") or "")
            league = str(g.get("league") or sport_group.upper() or "SPORT")
            comp_code = f"db_{_slug_token(sport_group)}_{_slug_token(league)}".upper()[:64]
            if sport_group in {"baseball", "mlb"} and "mlb" in league.lower():
                comp_code = "baseball_mlb"
                has_mlb = True
            if sport_group == "soccer":
                has_soccer = True
            _push_game(
                sport_group=sport_group,
                league=league,
                competition=comp_code,
                competition_name=league,
                home=str(g.get("home_team") or "").strip(),
                away=str(g.get("away_team") or "").strip(),
                game_date=str(g.get("game_date") or "").strip(),
                game_time=str(g.get("game_time") or ""),
                game_datetime=str(g.get("game_datetime") or ""),
                status=str(g.get("status") or "Scheduled"),
                home_score=g.get("home_score"),
                away_score=g.get("away_score"),
            )
    except Exception as e:
        _log(f"[all-sports] DB schedule fallback unavailable: {e}")

    # 1) MLB official schedule (free)
    if not has_mlb:
        try:
            from data.mlb_fetcher import get_schedule_range

            for g in (get_schedule_range(days_ahead=2) or []):
                _push_game(
                    sport_group="baseball",
                    league="MLB",
                    competition="baseball_mlb",
                    competition_name="MLB",
                    home=str(g.get("home_team") or "").strip(),
                    away=str(g.get("away_team") or "").strip(),
                    game_date=str(g.get("date") or g.get("game_date") or "").strip(),
                    game_time=str(g.get("game_time") or ""),
                    game_datetime=str(g.get("game_datetime") or ""),
                    status=str(g.get("status") or "Scheduled"),
                    home_score=g.get("home_score"),
                    away_score=g.get("away_score"),
                )
        except Exception as e:
            _log(f"[all-sports] MLB fallback fetch failed: {e}")

    # 2) Soccer tournaments via fast ESPN path to avoid football-data 429 backoff delays.
    if not has_soccer:
        try:
            from data.soccer_fetcher import _fetch_matches_espn_range

            start = today.isoformat()
            end = tomorrow.isoformat()
            raw_codes = os.getenv("SOCCER_FALLBACK_COMPETITIONS", "PL,MLS,CL")
            codes = [c.strip().upper() for c in str(raw_codes).split(",") if c.strip()]
            if not codes:
                codes = ["PL", "MLS", "CL"]

            for code in codes:
                for g in (_fetch_matches_espn_range(code, start, end) or []):
                    comp = str(g.get("competition") or code).strip().upper()
                    comp_name = str(g.get("competition_name") or g.get("comp_name") or g.get("league") or comp)
                    _push_game(
                        sport_group="soccer",
                        league=comp_name,
                        competition=comp,
                        competition_name=comp_name,
                        home=str(g.get("home_team") or "").strip(),
                        away=str(g.get("away_team") or "").strip(),
                        game_date=str(g.get("date") or g.get("game_date") or "").strip(),
                        game_time=str(g.get("game_time") or ""),
                        game_datetime=str(g.get("game_datetime") or ""),
                        status=str(g.get("status") or "Scheduled"),
                        home_score=g.get("home_score"),
                        away_score=g.get("away_score"),
                    )
        except Exception as e:
            _log(f"[all-sports] Soccer fallback fetch failed: {e}")

    # 3) TheSportsDB (multi-sport free fixture feed) - opt-in due endpoint variability.
    tsdb_enabled = str(os.getenv("ENABLE_TSDB_FALLBACK", "0")).strip().lower() in {"1", "true", "yes", "on"}
    if tsdb_enabled:
        try:
            from data.thesportsdb_fetcher import get_events_by_date

            tsdb_sports = [
                ("Soccer", "soccer"),
                ("Baseball", "baseball"),
                ("Basketball", "basketball"),
                ("Ice Hockey", "icehockey"),
                ("American Football", "americanfootball"),
                ("Tennis", "tennis"),
                ("MMA", "mma"),
            ]
            for d in (today, tomorrow):
                for tsdb_name, sport_group in tsdb_sports:
                    events = get_events_by_date(d, sport=tsdb_name) or []
                    for ev in events:
                        home = str(ev.get("strHomeTeam") or "").strip()
                        away = str(ev.get("strAwayTeam") or "").strip()
                        if not home or not away:
                            continue
                        league = str(ev.get("strLeague") or tsdb_name)
                        sport_name = str(ev.get("strSport") or tsdb_name)
                        group = _infer_sport_group(sport_name)
                        comp_code = f"tsdb_{_slug_token(group)}_{_slug_token(league)}".upper()[:64]
                        status = str(ev.get("strStatus") or "").strip()
                        hs = ev.get("intHomeScore")
                        aw = ev.get("intAwayScore")
                        if not status:
                            status = "Final" if hs is not None and aw is not None else "Scheduled"
                        _push_game(
                            sport_group=group,
                            league=league,
                            competition=comp_code,
                            competition_name=league,
                            home=home,
                            away=away,
                            game_date=str(ev.get("dateEvent") or d.isoformat()),
                            game_time=str(ev.get("strTime") or ""),
                            game_datetime=str(ev.get("strTimestamp") or ""),
                            status=status,
                            home_score=hs,
                            away_score=aw,
                        )
        except Exception as e:
            _log(f"[all-sports] TheSportsDB fallback fetch failed: {e}")

    # Dedupe by competition + matchup + schedule slot.
    deduped: list[dict] = []
    seen = set()
    for g in rows:
        key = (
            str(g.get("competition") or ""),
            str(g.get("match_key") or ""),
            str(g.get("game_date") or ""),
            str(g.get("game_time") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(g)

    deduped.sort(key=lambda x: (
        str(x.get("game_date") or ""),
        str(x.get("game_time") or ""),
        str(x.get("competition_name") or x.get("league") or ""),
    ))
    return deduped


def _build_model_fallback_bets(games: list[dict]) -> list[dict]:
    bets: list[dict] = []
    today_str = _et_calendar_today().isoformat()

    def _prob_to_american(prob: float) -> int:
        p = max(0.01, min(0.99, float(prob or 0.5)))
        if p >= 0.5:
            return int(round(-p / (1.0 - p) * 100))
        return int(round((1.0 - p) / p * 100))

    # Deterministic, zero-network fallback pick generation.
    # Used only when sportsbook/model feeds are unavailable.
    for g in (games or [])[:120]:
        home = str(g.get("home_team") or "").strip()
        away = str(g.get("away_team") or "").strip()
        if not home or not away:
            continue

        sport = _infer_sport_group(g.get("sport") or g.get("competition") or "")
        if sport == "soccer":
            home_prob = 0.45
            bet_type = "1X2"
            pick = f"{home} to Win"
        elif sport in {"baseball", "mlb"}:
            home_prob = 0.55
            bet_type = "moneyline"
            pick = f"{home} ML"
        else:
            home_prob = 0.53
            bet_type = "moneyline"
            pick = f"{home} ML"

        odds_am = _prob_to_american(home_prob)
        label = _rank_label(home_prob)
        game_date = g.get("game_date") or g.get("date") or today_str
        game_key = g.get("game_key") or _compose_game_key(
            away,
            home,
            g.get("game_datetime"),
            game_date,
            g.get("game_time"),
        )
        bets.append({
            "sport": sport,
            "league": g.get("league") or g.get("competition_name") or sport.upper(),
            "competition": g.get("competition") or sport.upper(),
            "competition_name": g.get("competition_name") or g.get("league") or sport.upper(),
            "bet_type": bet_type,
            "pick": pick,
            "line": None,
            "odds_am": odds_am,
            "dec_odds": round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4),
            "model_prob": round(home_prob, 4),
            "confidence": int(round(home_prob * 100)),
            "safety_label": label,
            "safety": _safety_score_from_label(label),
            "game_date": game_date,
            "game_time": g.get("game_time") or "",
            "home_team": home,
            "away_team": away,
            "match_key": g.get("match_key") or _norm_gk(f"{away}@{home}"),
            "game_key": game_key,
            "worth_it": home_prob >= 0.53,
            "worth_score": round(home_prob * 100.0, 2),
            "worth_reason": "Fallback baseline pick while live odds are unavailable",
        })

    # Dedupe similar bets.
    deduped: list[dict] = []
    seen = set()
    for b in bets:
        key = (
            str(b.get("game_key") or ""),
            str(b.get("bet_type") or ""),
            str(b.get("pick") or b.get("pick_label") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(b)
    deduped.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
    return deduped


def _multi_sport_best_bets_rows(bets: list[dict]) -> list[dict]:
    """Convert ranked game bets into rows consumed by the Best Bets table."""
    rows: list[dict] = []
    seen = set()

    for b in (bets or []):
        if not isinstance(b, dict):
            continue

        pick = str(b.get("pick") or b.get("pick_label") or "").strip()
        if not pick:
            continue

        market = str(b.get("bet_type") or "best_bet").strip() or "best_bet"
        sport = _infer_sport_group(b.get("sport") or b.get("competition") or b.get("league") or "")
        home = str(b.get("home_team") or "").strip()
        away = str(b.get("away_team") or "").strip()
        game_date = str(b.get("game_date") or b.get("date") or "").strip()
        game_time = str(b.get("game_time") or "")
        game_key = str(b.get("game_key") or "").strip()
        if not game_key and (home and away):
            game_key = _compose_game_key(away, home, b.get("game_datetime"), game_date, game_time)

        # Keep a stable, compact stat token for table filters and sorting.
        stat_type = re.sub(r"[^a-z0-9_]+", "_", market.lower()).strip("_") or "best_bet"
        prop_label = market.replace("_", " ").strip().title() or "Best Bet"

        prob_raw = b.get("model_prob", b.get("probability", 0.5))
        try:
            prob = float(prob_raw)
        except (TypeError, ValueError):
            prob = 0.5
        prob = max(0.01, min(0.99, prob))

        direction = ""
        pick_up = pick.upper()
        if "OVER" in pick_up:
            direction = "OVER"
        elif "UNDER" in pick_up:
            direction = "UNDER"

        team = str(b.get("team") or "").strip()
        if not team:
            if home and home.upper() in pick_up:
                team = home
            elif away and away.upper() in pick_up:
                team = away
            else:
                team = str(b.get("competition_name") or b.get("league") or sport.upper() or "SPORT")

        dec_odds = b.get("dec_odds")
        try:
            dec_odds_f = float(dec_odds)
        except (TypeError, ValueError):
            dec_odds_f = None

        ev_val = b.get("ev")
        try:
            ev = float(ev_val)
        except (TypeError, ValueError):
            if dec_odds_f and dec_odds_f > 1:
                ev = (dec_odds_f - 1.0) * prob - (1.0 - prob)
            else:
                ev = 0.0

        row = {
            "sport": sport,
            "name": pick,
            "team": team,
            "prop_label": prop_label,
            "stat_type": stat_type,
            "line": b.get("line"),
            "direction": direction,
            "model_prob": prob,
            "safety_label": str(b.get("safety_label") or _safety_label_from_prob(prob)).upper(),
            "ev": ev,
            "odds_am": b.get("odds_am"),
            "dec_odds": dec_odds_f,
            "confidence": int(b.get("confidence") or round(prob * 100)),
            "pick": pick,
            "game": b.get("game") or (f"{away} @ {home}" if (away and home) else ""),
            "game_key": game_key,
            "match_key": b.get("match_key") or _norm_gk(f"{away}@{home}") if (away and home) else "",
            "game_date": game_date,
            "game_time": game_time,
            "league": b.get("league"),
            "competition": b.get("competition"),
            "competition_name": b.get("competition_name") or b.get("league"),
        }

        dedupe_key = (
            str(row.get("game_key") or ""),
            str(row.get("stat_type") or ""),
            str(row.get("pick") or ""),
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        rows.append(row)

    rows.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
    return rows[:300]


def _build_all_sport_sentiment_props(games: list[dict], bets: list[dict]) -> list[dict]:
    """Build all-sports player rows strictly from sentiment-mentioned players."""
    try:
        from data.sentiment import get_game_player_sentiment_props
    except Exception as e:
        _log(f"[all-sports] sentiment player extractor unavailable: {e}")
        return []

    if not games:
        return []

    today_str = _et_calendar_today().isoformat()
    tomorrow_str = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()
    allowed = {today_str, tomorrow_str}

    target_games = [g for g in (games or []) if str(g.get("game_date") or "") in allowed]
    target_games = target_games[:_ALL_SPORTS_SENTIMENT_MAX_GAMES]
    if not target_games:
        return []

    # Index ranked bets by game/match key to extract an odds anchor per game.
    idx: dict[str, list[dict]] = {}

    def _index_bet_key(k: str, row: dict):
        nk = _norm_gk(k or "")
        if not nk:
            return
        idx.setdefault(nk, []).append(row)

    for b in (bets or []):
        if not isinstance(b, dict):
            continue
        _index_bet_key(str(b.get("game_key") or ""), b)
        _index_bet_key(str(b.get("match_key") or ""), b)
        home = str(b.get("home_team") or "").strip()
        away = str(b.get("away_team") or "").strip()
        if home and away:
            _index_bet_key(f"{away}@{home}", b)

    rows: list[dict] = []
    seen = set()
    include_news = _ALL_SPORTS_SENTIMENT_INCLUDE_NEWS and len(target_games) <= 12

    for g in target_games:
        home = str(g.get("home_team") or "").strip()
        away = str(g.get("away_team") or "").strip()
        if not home or not away:
            continue

        game_date = str(g.get("game_date") or g.get("date") or "")
        game_time = str(g.get("game_time") or "")
        match_key = _norm_gk(g.get("match_key") or f"{away}@{home}")
        game_key = str(g.get("game_key") or _compose_game_key(away, home, g.get("game_datetime"), game_date, game_time))
        sport = _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "other")

        keys = {
            _norm_gk(game_key),
            match_key,
            _norm_gk(f"{home}@{away}"),
        }
        candidate_bets: list[dict] = []
        for k in keys:
            if not k:
                continue
            candidate_bets.extend(idx.get(k, []))
        candidate_bets.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)

        odds_hint = None
        for cb in candidate_bets:
            if cb.get("odds_am") is not None:
                odds_hint = cb.get("odds_am")
                break

        try:
            per_game = get_game_player_sentiment_props(
                home_team=home,
                away_team=away,
                sport=sport,
                game_key=game_key,
                game_date=game_date,
                game_time=game_time,
                max_players=_ALL_SPORTS_SENTIMENT_PLAYERS_PER_GAME,
                odds_hint=odds_hint,
                include_news=include_news,
            ) or []
        except Exception as e:
            _log(f"[all-sports] sentiment extraction failed for {away}@{home}: {e}")
            continue

        for r in per_game:
            row = dict(r)
            row["sport"] = _infer_sport_group(row.get("sport") or sport)
            row["league"] = row.get("league") or g.get("league") or g.get("competition_name")
            row["competition"] = row.get("competition") or g.get("competition")
            row["competition_name"] = row.get("competition_name") or g.get("competition_name") or g.get("league")
            row["game_key"] = row.get("game_key") or game_key
            row["match_key"] = row.get("match_key") or match_key
            row["game_date"] = row.get("game_date") or game_date
            row["game_time"] = row.get("game_time") or game_time
            row["home_team"] = row.get("home_team") or home
            row["away_team"] = row.get("away_team") or away

            dedupe_key = (
                str(row.get("game_key") or ""),
                str(row.get("name") or "").strip().lower(),
                str(row.get("stat_type") or "").strip().lower(),
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            rows.append(row)

    rows.sort(
        key=lambda x: (
            float(x.get("model_prob") or 0.0),
            int(x.get("sentiment_mentions") or 0),
        ),
        reverse=True,
    )
    return rows[:400]


def _build_multi_sport_snapshot(force_refresh: bool = False) -> dict:
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    if not force_refresh:
        cached = _MULTI_SPORT_CACHE.get("snapshot")
        cache_ts = float(_MULTI_SPORT_CACHE.get("ts") or 0.0)
        if cached and (now - cache_ts) < _MULTI_SPORT_CACHE_TTL_SEC:
            return cached

    snapshot = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "tournaments": [],
        "games": [],
        "bets": [],
    }

    try:
        from data.odds_fetcher import get_available_sports, get_live_odds
    except Exception as e:
        _log(f"[all-sports] odds fetcher unavailable: {e}")
        return snapshot

    sports = get_available_sports() or []
    if not sports:
        _log("[all-sports] No sports returned by odds API (missing key or quota exhausted) — trying free-source fallbacks")

    active = []
    for s in sports:
        if s.get("has_outrights"):
            continue
        key = str(s.get("key") or "").strip()
        if not key:
            continue
        active.append(s)

    active = active[: max(1, _MAX_ODDS_SPORTS)] if active else []
    today = _et_calendar_today()
    tomorrow = today + datetime.timedelta(days=1)
    allowed_dates = {today.isoformat(), tomorrow.isoformat()}

    games: list[dict] = []
    bets: list[dict] = []
    empty_streak = 0

    for sport in active:
        sport_key = str(sport.get("key") or "").strip()
        title = str(sport.get("title") or sport_key)
        if not sport_key:
            continue

        try:
            events = get_live_odds(sport_key, markets="h2h") or []
        except Exception as e:
            _log(f"[all-sports] {sport_key} odds error: {e}")
            empty_streak += 1
            if empty_streak >= 5 and not games:
                _log("[all-sports] Stopping fetch early (likely API auth/quota issue)")
                break
            continue

        if not events:
            empty_streak += 1
            if empty_streak >= 5 and not games:
                _log("[all-sports] Stopping fetch early (no events returned across multiple sports)")
                break
            continue
        empty_streak = 0

        for ev in events:
            home = str(ev.get("home_team") or "").strip()
            away = str(ev.get("away_team") or "").strip()
            if not home or not away:
                continue

            game_datetime = str(ev.get("commence_time") or "").strip()
            game_date, game_time = _datetime_to_et_parts(game_datetime)
            if game_date and game_date not in allowed_dates:
                continue

            match_key = _norm_gk(f"{away}@{home}")
            game_key = _compose_game_key(away, home, game_datetime, game_date, game_time)
            status = str(ev.get("status") or "Scheduled")
            sport_group = _infer_sport_group(sport_key)

            games.append({
                "sport": sport_group,
                "league": title,
                "competition": sport_key,
                "competition_name": title,
                "home_team": home,
                "away_team": away,
                "date": game_date,
                "game_date": game_date,
                "game_time": game_time,
                "game_datetime": game_datetime,
                "status": status,
                "match_key": match_key,
                "game_key": game_key,
            })

            books = ev.get("bookmakers") or []
            if not books:
                continue
            market = None
            for m in (books[0].get("markets") or []):
                if str(m.get("key") or "") == "h2h":
                    market = m
                    break
            if not market:
                continue

            outcomes = market.get("outcomes") or []
            priced = []
            for out in outcomes:
                name = str(out.get("name") or "").strip()
                odds_am = out.get("price")
                implied = _prob_from_american(odds_am)
                if not name or implied is None:
                    continue
                priced.append((name, odds_am, implied))
            if len(priced) < 2:
                continue

            total = sum(x[2] for x in priced)
            norm = []
            for name, odds_am, implied in priced:
                true_prob = implied / total if total > 0 else implied
                norm.append((name, odds_am, max(0.01, min(0.99, true_prob))))

            pick_name, pick_odds, model_prob = max(norm, key=lambda x: x[2])
            label = _rank_label(model_prob)
            bet = {
                "sport": sport_group,
                "league": title,
                "competition": sport_key,
                "competition_name": title,
                "bet_type": "moneyline",
                "pick": pick_name,
                "line": None,
                "odds_am": int(float(pick_odds)),
                "dec_odds": round((1 + (pick_odds / 100.0)) if float(pick_odds) > 0 else (1 + 100.0 / abs(float(pick_odds))), 4),
                "model_prob": float(model_prob),
                "confidence": int(round(model_prob * 100)),
                "safety_label": label,
                "safety": _safety_score_from_label(label),
                "game_date": game_date,
                "game_time": game_time,
                "home_team": home,
                "away_team": away,
                "match_key": match_key,
                "game_key": game_key,
                "worth_it": model_prob >= 0.56,
                "worth_score": round(model_prob * 100.0, 2),
                "worth_reason": f"Best no-vig side from live h2h market ({title})",
            }
            bets.append(bet)

    # If odds feed failed, backfill games from free sources.
    if not games:
        fallback_games = _collect_fallback_games_for_all_sports(today, tomorrow)
        if fallback_games:
            _log(f"[all-sports] Fallback feeds supplied {len(fallback_games)} games")
            games = fallback_games

    # If we still have no ranked bets, derive baseline bets from MLB/soccer models.
    if games and not bets:
        fallback_bets = _build_model_fallback_bets(games)
        if fallback_bets:
            _log(f"[all-sports] Fallback models supplied {len(fallback_bets)} ranked bets")
            bets = fallback_bets

    # Build tournaments from final game pool (odds + fallback merged).
    tournament_counts: dict[str, int] = {}
    tournament_meta: dict[str, dict] = {}
    for g in games:
        code = str(g.get("competition") or "UNKNOWN").strip()
        if not code:
            continue
        tournament_counts[code] = tournament_counts.get(code, 0) + 1
        if code not in tournament_meta:
            tournament_meta[code] = {
                "code": code,
                "name": g.get("competition_name") or g.get("league") or code,
                "country": "Global",
                "type": _infer_sport_group(g.get("sport") or code),
            }

    tournaments = []
    for code, meta in tournament_meta.items():
        row = dict(meta)
        row["match_count"] = int(tournament_counts.get(code, 0))
        tournaments.append(row)
    tournaments.sort(key=lambda x: (x.get("match_count", 0), x.get("name", "")), reverse=True)

    games.sort(key=lambda x: (
        str(x.get("game_date") or x.get("date") or ""),
        str(x.get("game_time") or ""),
        str(x.get("competition_name") or x.get("league") or ""),
    ))

    snapshot["tournaments"] = tournaments
    snapshot["games"] = games
    snapshot["bets"] = sorted(bets, key=lambda x: (x.get("model_prob") or 0), reverse=True)
    _MULTI_SPORT_CACHE["snapshot"] = snapshot
    _MULTI_SPORT_CACHE["ts"] = now
    return snapshot


def _run_all_sports_analysis():
    with _lock:
        _state["status"] = "running"
        _state["error"] = None
        _state["logs"] = []
        _state["phase"] = _PHASES[0]
        _state["phase_idx"] = 0

    try:
        _phase(0)
        _log("[all-sports] Discovering online sportsbooks and events...")
        snapshot = _build_multi_sport_snapshot(force_refresh=True)

        _phase(1)
        games = snapshot.get("games") or []
        bets = snapshot.get("bets") or []
        best_bet_rows = _multi_sport_best_bets_rows(bets)
        sentiment_prop_rows = _build_all_sport_sentiment_props(games, bets)
        table_rows = sentiment_prop_rows if (sentiment_prop_rows or _ALL_SPORTS_STRICT_SENTIMENT_ONLY) else best_bet_rows
        _log(f"[all-sports] Pulled {len(games)} games and {len(bets)} ranked bets")
        if best_bet_rows:
            _log(f"[all-sports] Best-bets table rows prepared: {len(best_bet_rows)}")
        if sentiment_prop_rows:
            _log(f"[all-sports] Sentiment player rows prepared: {len(sentiment_prop_rows)}")

        today_str = _et_calendar_today().isoformat()
        tomorrow_str = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()

        _phase(2)
        today_games = [g for g in games if str(g.get("game_date") or "") == today_str]
        tomorrow_games = [g for g in games if str(g.get("game_date") or "") == tomorrow_str]

        _phase(3)
        today_cards = [_build_card(g, bets, table_rows, "TODAY") for g in today_games]
        tomorrow_cards = [_build_card(g, bets, table_rows, "TOMORROW") for g in tomorrow_games]

        def _card_score(card: dict) -> float:
            s = [b["safety"] for b in [card.get("moneyline"), card.get("run_line"), card.get("total")] if b]
            return sum(s) / len(s) if s else 0.45

        today_cards.sort(key=_card_score, reverse=True)
        tomorrow_cards.sort(key=_card_score, reverse=True)
        best_parlays = []
        try:
            from models.mlb_predictor import build_parlays
            best_parlays = build_parlays(bets, max_legs=5, top_n=5)
        except Exception as parlay_exc:
            _log(f"[all-sports] parlay builder skipped: {parlay_exc}")

        last_updated = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M")
        with _lock:
            _state.update({
                "status": "done",
                "phase": "Complete",
                "last_updated": last_updated,
                "last_updated_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "game_cards_today": _clean(today_cards),
                "game_cards_tomorrow": _clean(tomorrow_cards),
                "best_parlays": _clean(best_parlays),
                "player_props": _clean(table_rows),
                "elite_parlay": None,
            })

        _sse_broadcast("state_update", {
            "status": "done",
            "last_updated": last_updated,
            "game_cards_today": _clean(today_cards),
            "game_cards_tomorrow": _clean(tomorrow_cards),
            "best_parlays": _clean(best_parlays),
            "player_props": _clean(table_rows),
            "elite_parlay": None,
        })
        _log(f"[all-sports] Complete — {len(today_cards)} today, {len(tomorrow_cards)} tomorrow")
    except Exception:
        err = traceback.format_exc()
        _log(f"[all-sports] FAILED:\n{err}")
        with _lock:
            _state["status"] = "error"
            _state["phase"] = "Error"
            _state["error"] = err
        _sse_broadcast("status", {"status": "error", "error": err[:300]})


def _run_soccer_analysis(lock_date: datetime.date | None = None):
    warnings.filterwarnings("ignore")

    with _lock:
        _state["status"] = "running"
        _state["error"] = None
        _state["logs"] = []
        _state["phase"] = _PHASES[0]
        _state["phase_idx"] = 0

    try:
        today_date = _et_calendar_today()
        today_str = today_date.isoformat()
        tomorrow_str = (today_date + datetime.timedelta(days=1)).isoformat()
        run_id = f"SOCCER-{today_str}"

        need_preds = False
        need_props = False
        try:
            from data.db import has_predictions_for_date, has_prop_picks_for_date, upsert_daily_run
            upsert_daily_run(run_id, today_date, status="RUNNING")
            if lock_date is None:
                need_preds = not has_predictions_for_date(today_date, sport="soccer")
                need_props = not has_prop_picks_for_date(today_date, sport="soccer")
                if need_preds or need_props:
                    lock_date = today_date
                    _log(f"[lock] No soccer picks for {today_date} yet — this run will lock picks")
                else:
                    _log(f"[lock] Soccer picks already saved for {today_date} — updating cards only")
            else:
                need_preds = not has_predictions_for_date(lock_date, sport="soccer")
                need_props = not has_prop_picks_for_date(lock_date, sport="soccer")
        except Exception as lock_exc:
            _log(f"[lock] Soccer lock check failed: {lock_exc}")
            if lock_date is None:
                lock_date = today_date
            need_preds = True
            need_props = True

        _phase(0)
        _log("Fetching soccer fixtures across tournaments...")
        from data.soccer_fetcher import get_matches_today_all, get_matches_tomorrow_all

        today_games = get_matches_today_all() or []
        tomorrow_games = get_matches_tomorrow_all() or []
        display_today = today_games
        display_tomorrow = tomorrow_games
        all_games = display_today + display_tomorrow
        _log(f"Fixtures: {len(display_today)} today, {len(display_tomorrow)} tomorrow")

        _phase(1)
        _log("Running soccer model + sentiment analysis...")
        from models.soccer_predictor import analyze_matches

        analyzed = analyze_matches(all_games, use_sentiment=True) or []
        by_match: dict[str, dict] = {}
        by_game: dict[str, dict] = {}
        for card in analyzed:
            mk = _norm_gk(card.get("match_key") or "")
            gk = _norm_gk(card.get("game_key") or "")
            if mk:
                by_match[mk] = card
            if gk:
                by_game[gk] = card

        all_bets: list[dict] = []
        all_props: list[dict] = []

        for g in all_games:
            home = g.get("home_team", "")
            away = g.get("away_team", "")
            match_key = _norm_gk(f"{away}@{home}")
            game_key = _norm_gk(_compose_game_key(
                away,
                home,
                g.get("game_datetime"),
                g.get("date") or g.get("game_date"),
                g.get("game_time"),
            ))
            card = by_game.get(game_key) or by_match.get(match_key)
            if not card:
                continue
            for bet in card.get("suggested_bets", []) or []:
                all_bets.append(_normalize_soccer_bet(g, bet, today_str))
            for prop in card.get("suggested_props", []) or []:
                all_props.append(_normalize_soccer_prop(g, prop, today_str))

        _log(f"Soccer bets generated: {len(all_bets)}")
        _phase(2)
        _log(f"Soccer player props generated: {len(all_props)}")

        _phase(3)
        _log("Building soccer parlays...")
        try:
            from models.mlb_predictor import build_parlays
            best_parlays = build_parlays(all_bets + all_props, max_legs=5, top_n=5)
        except Exception as parlay_exc:
            _log(f"Parlay builder fallback: {parlay_exc}")
            best_parlays = []

        _phase(4)
        _log("Saving soccer analysis and building cards...")
        from data.db import save_predictions, save_prop_picks, save_analysis_cache

        def _date_str(value) -> str:
            if isinstance(value, datetime.datetime):
                return value.date().isoformat()
            if isinstance(value, datetime.date):
                return value.isoformat()
            return str(value or "")

        pred_rows = []
        for b in all_bets:
            pred_rows.append({
                "game_key": b.get("game_key", ""),
                "run_id": run_id,
                "run_date": today_str,
                "sport": "soccer",
                "bet_type": b.get("bet_type", "soccer_market"),
                "pick": b.get("pick") or b.get("pick_label") or "Soccer Market",
                "line": b.get("line"),
                "odds_am": b.get("odds_am"),
                "dec_odds": b.get("dec_odds", 1.91),
                "model_prob": b.get("model_prob", 0.5),
                "confidence": b.get("confidence", 50),
                "safety_label": b.get("safety_label", "MODERATE"),
                "game_date": b.get("game_date", today_str),
                "game_time": b.get("game_time", ""),
                "home_team": b.get("home_team", ""),
                "away_team": b.get("away_team", ""),
                "home_starter": "",
                "away_starter": "",
                "sentiment_score": b.get("market_popularity"),
                "news_snippet": (b.get("worth_reason") or "")[:500],
            })

        if lock_date:
            lock_str = _date_str(lock_date)
            if need_preds:
                pred_rows_locked = [p for p in pred_rows if _date_str(p.get("game_date")) == lock_str]
                save_predictions(pred_rows_locked)
            else:
                _log(f"[lock] Soccer predictions already saved for {lock_str} — cards updating")

            if need_props:
                props_locked = [p for p in all_props if _date_str(p.get("date") or p.get("game_date")) == lock_str]
                for pp in props_locked:
                    pp["run_id"] = run_id
                save_prop_picks(props_locked, game_date=lock_date)
            else:
                _log(f"[lock] Soccer props already saved for {lock_str} — tracking only")
        else:
            _log("[lock] No lock_date — updating analysis cards without re-saving soccer picks")

        today_cards = [_build_card(g, all_bets, all_props, "TODAY") for g in display_today]
        tomorrow_cards = [_build_card(g, all_bets, all_props, "TOMORROW") for g in display_tomorrow]

        def _card_score(card: dict) -> float:
            s = [b["safety"] for b in [card.get("moneyline"), card.get("run_line"), card.get("total")] if b]
            return sum(s) / len(s) if s else 0.45

        today_cards.sort(key=_card_score, reverse=True)
        tomorrow_cards.sort(key=_card_score, reverse=True)
        all_props_flat = sorted(all_props, key=lambda x: x.get("safety", 0), reverse=True)

        now_ts = datetime.datetime.now(datetime.timezone.utc)
        last_updated = now_ts.strftime("%Y-%m-%d %H:%M")
        try:
            save_analysis_cache({
                "game_cards_today": today_cards,
                "game_cards_tomorrow": tomorrow_cards,
                "best_parlays": best_parlays,
                "player_props": all_props_flat,
                "last_updated": last_updated,
            }, cache_date=today_date)
        except Exception as cache_exc:
            _log(f"Soccer cache save error: {cache_exc}")

        with _lock:
            _state.update({
                "status": "done",
                "phase": "Complete",
                "last_updated": last_updated,
                "last_updated_ts": now_ts.isoformat(),
                "game_cards_today": _clean(today_cards),
                "game_cards_tomorrow": _clean(tomorrow_cards),
                "best_parlays": _clean(best_parlays),
                "player_props": _clean(all_props_flat),
                "elite_parlay": None,
            })

        try:
            from data.db import upsert_daily_run
            upsert_daily_run(
                run_id,
                today_date,
                status="DONE",
                games_today=len(today_cards),
                games_tmrw=len(tomorrow_cards),
                props_count=len(all_props_flat),
                parlays_count=len(best_parlays),
                finished=True,
            )
        except Exception as run_exc:
            _log(f"[run-log] {run_exc}")

        if need_preds or need_props:
            try:
                from email_notify import send_daily_picks
                mail_state = {
                    "best_parlays": _clean(best_parlays),
                    "game_cards_today": _clean(today_cards),
                    "player_props": _clean(all_props_flat),
                }
                mail_result = send_daily_picks(mail_state)
                _log(f"[email] Sent daily picks — {mail_result.get('sent',0)} delivered, {mail_result.get('failed',0)} failed")
            except Exception as mail_exc:
                _log(f"[email] Soccer send failed: {mail_exc}")

        _sse_broadcast("state_update", {
            "status": "done",
            "last_updated": last_updated,
            "game_cards_today": _clean(today_cards),
            "game_cards_tomorrow": _clean(tomorrow_cards),
            "best_parlays": _clean(best_parlays),
            "player_props": _clean(all_props_flat),
            "elite_parlay": None,
        })

        _log(
            f"Soccer analysis complete — {len(today_cards)} today, "
            f"{len(tomorrow_cards)} tomorrow, {len(all_props_flat)} props"
        )
    except Exception:
        err = traceback.format_exc()
        _log(f"Soccer analysis FAILED:\n{err}")
        with _lock:
            _state["status"] = "error"
            _state["phase"] = "Error"
            _state["error"] = err
        _sse_broadcast("status", {"status": "error", "error": err[:300]})


def _run_analysis(lock_date: datetime.date | None = None):
    if _ACTIVE_SPORT == "all":
        return _run_all_sports_analysis()
    if _ACTIVE_SPORT == "soccer":
        return _run_soccer_analysis(lock_date)

    warnings.filterwarnings("ignore")
    import pandas as pd

    with _lock:
        _state["status"]    = "running"
        _state["error"]     = None
        _state["logs"]      = []
        _state["phase"]     = _PHASES[0]
        _state["phase_idx"] = 0

    try:
        today_date = _et_calendar_today()
        today_str  = today_date.isoformat()
        tomorrow_str = (today_date + datetime.timedelta(days=1)).isoformat()
        run_id = f"MLB-{today_str}"

        # ── Step 0: Archive previous-day PENDING picks so lock check is fresh ──
        try:
            from data.db import archive_previous_day_data, upsert_daily_run
            arch = archive_previous_day_data(today_date)
            if arch.get("predictions_archived") or arch.get("props_archived"):
                _log(f"[archive] Archived {arch.get('predictions_archived',0)} preds, "
                     f"{arch.get('props_archived',0)} props from prior days for training")
            upsert_daily_run(run_id, today_date, status="RUNNING")
        except Exception as _ae:
            _log(f"[archive] Archive step skipped: {_ae}")

        # Decide whether this run should lock/save today's picks
        need_preds = False
        need_props = False
        try:
            from data.db import has_predictions_for_date, has_prop_picks_for_date
            if lock_date is None:
                today = today_date
                need_preds = not has_predictions_for_date(today)
                need_props = not has_prop_picks_for_date(today)
                if need_preds or need_props:
                    lock_date = today
                    _log(f"[lock] No picks for {today} yet — this run will lock picks")
                else:
                    _log(f"[lock] Today's picks already saved for {today} — updating cards only")
            else:
                need_preds = not has_predictions_for_date(lock_date)
                need_props = not has_prop_picks_for_date(lock_date)
        except Exception as _le:
            _log(f"[lock] Daily lock check failed: {_le}")
            if lock_date is None:
                lock_date = today_date
            need_preds = True
            need_props = True

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

        _phase(0)
        _log("Fetching MLB schedule...")
        from data.mlb_fetcher import get_schedule_range
        all_games = get_schedule_range(days_ahead=2)
        today_games    = [g for g in all_games if g.get("date", "") == today_str]
        tomorrow_games = [g for g in all_games if g.get("date", "") == tomorrow_str]

        # Keep all calendar-today games visible on the Today tab, including finals,
        # until the next refresh/day boundary removes them naturally.
        display_today = today_games
        display_tomorrow = tomorrow_games
        _log(f"Schedule: {len(display_today)} today, {len(display_tomorrow)} tomorrow")

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
        from data.sentiment import get_game_sentiments
        from models.mlb_predictor import predict_game, build_game_bets
        # Lower edge threshold so dashboard shows all value picks (accuracy tracking)
        _orig_edge = _mp.MIN_VALUE_EDGE
        _mp.MIN_VALUE_EDGE = _DASH_MIN_EDGE

        all_bets = []
        sentiment_cache = {}
        def _is_terminal_status(s: str) -> bool:
            sl = (s or "").lower()
            return any(k in sl for k in (
                "final", "game over", "completed", "cancelled", "suspended", "postponed"
            ))

        # Identify which games are still upcoming (not yet final)
        _today_upcoming   = [g for g in today_games    if not _is_terminal_status(g.get("status", ""))]
        _tomorrow_upcoming = [g for g in tomorrow_games if not _is_terminal_status(g.get("status", ""))]
        _all_today_final   = len(today_games) > 0 and len(_today_upcoming) == 0

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
                match_key = _norm_gk(f"{at}@{ht}")
                matchup_sentiment = sentiment_cache.get(match_key)
                if matchup_sentiment is None:
                    try:
                        matchup_sentiment = get_game_sentiments(ht, at)
                    except Exception as sentiment_exc:
                        _log(f"Sentiment skipped for {at}@{ht}: {sentiment_exc}")
                        matchup_sentiment = {}
                    sentiment_cache[match_key] = matchup_sentiment
                pred = predict_game(ht, at, team_stats, model, sentiment=matchup_sentiment, injuries=injuries)
                pred["game_key"] = _compose_game_key(
                    at,
                    ht,
                    g.get("game_datetime"),
                    g.get("date"),
                    g.get("game_time"),
                )
                pred["match_key"] = match_key
                hw   = pred.get("home_win_prob", 0.5)
                _log(f"  {at}@{ht}: home win prob={hw:.1%}")
                gk   = pred["match_key"]
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

        # ── DB fallback: restore today's saved predictions when all today games are Final ──
        if _all_today_final:
            _log(f"[fallback] All {len(today_games)} today games are Final — loading saved predictions from DB for today's cards...")
            try:
                from data.db import get_predictions_for_date
                saved_today = get_predictions_for_date(today_str)
                if saved_today:
                    all_bets.extend(saved_today)
                    _log(f"[fallback] Restored {len(saved_today)} saved picks for today's cards")
                else:
                    _log("[fallback] No saved predictions found in DB for today")
            except Exception as _fb_e:
                _log(f"[fallback] DB prediction restore failed: {_fb_e}")

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
            scheduled_keys_by_slot = {}
            scheduled_keys_by_match_day = {}
            for sg in today_games + tomorrow_games:
                match_key = _norm_gk(f"{sg.get('away_team','')}@{sg.get('home_team','')}")
                slot = (match_key, str(sg.get("date") or ""), str(sg.get("game_time") or "").strip())
                unique_key = _compose_game_key(
                    sg.get("away_team", ""),
                    sg.get("home_team", ""),
                    sg.get("game_datetime"),
                    sg.get("date"),
                    sg.get("game_time"),
                )
                scheduled_keys_by_slot[slot] = unique_key
                scheduled_keys_by_match_day.setdefault((match_key, str(sg.get("date") or "")), []).append(unique_key)

            for raw_prop in raw_props:
                game_str = _norm_gk(str(raw_prop.get("game") or ""))
                raw_date = str(raw_prop.get("date") or "")
                raw_time = str(raw_prop.get("game_time") or "").strip()
                unique_prop_key = scheduled_keys_by_slot.get((game_str, raw_date, raw_time))
                if not unique_prop_key:
                    day_matches = scheduled_keys_by_match_day.get((game_str, raw_date), [])
                    if len(day_matches) == 1:
                        unique_prop_key = day_matches[0]
                raw_prop["match_key"] = game_str
                raw_prop["game_key"] = unique_prop_key or game_str

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
        _log(f"Sentiment snapshot ready for {len(sentiment_cache)} matchups")

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
                    "run_id":       run_id,
                    "run_date":     today_str,
                    "sport":        "mlb",
                    "bet_type":     b.get("bet_type", ""),
                    "pick":         b.get("pick", ""),
                    "line":         b.get("line"),
                    "odds_am":      b.get("odds_am"),
                    "dec_odds":     b.get("dec_odds", 2.0),
                    "model_prob":   b.get("model_prob", 0.0),
                    "confidence":   b.get("confidence", 50),
                    "safety_label": b.get("safety_label", "MODERATE"),
                    "game_date":    b.get("game_date", today_str),
                    "game_time":    b.get("game_time", ""),
                    "home_team":    b.get("home_team", ""),
                    "away_team":    b.get("away_team", ""),
                    "home_starter": b.get("home_starter", ""),
                    "away_starter": b.get("away_starter", ""),
                    "sentiment_score": (sentiment_cache.get(b.get("match_key", ""), {}).get("home", {}) or {}).get("combined"),
                    "news_snippet": "",
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
                    "run_id":       run_id,
                    "run_date":     today_str,
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
                    _log(f"[lock] Predictions already saved for {lock_str} — cards updating")

                if need_props:
                    props_locked = [p for p in all_props if _date_str(p.get("date")) == lock_str]
                    # Stamp run_id on each prop pick
                    for _pp in props_locked:
                        _pp["run_id"] = run_id
                    save_prop_picks(props_locked, game_date=lock_date)
                else:
                    _log(f"[lock] Props already saved for {lock_str} — tracking only")
            else:
                _log("[lock] No lock_date — updating analysis cards without re-saving picks")
        except Exception as e:
            _log(f"DB save error: {e}")

        # Display only upcoming games; past/live ones were predicted & saved for accuracy tracking
        today_cards    = [_build_card(g, all_bets, all_props, "TODAY")    for g in display_today]
        tomorrow_cards = [_build_card(g, all_bets, all_props, "TOMORROW") for g in display_tomorrow]

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
            }, cache_date=today_date)
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

        # Mark the daily run as finished in DB
        try:
            from data.db import upsert_daily_run
            upsert_daily_run(run_id, today_date, status="DONE",
                             games_today=len(today_cards),
                             games_tmrw=len(tomorrow_cards),
                             props_count=len(all_props_flat),
                             parlays_count=len(best_parlays),
                             finished=True)
        except Exception as _re:
            _log(f"[run-log] {_re}")

        # ── Send email notification when new picks are saved ──────────────────
        if need_preds or need_props:
            try:
                from email_notify import send_daily_picks
                _mail_state = {
                    "best_parlays":     _clean(best_parlays),
                    "game_cards_today": _clean(today_cards),
                    "player_props":     _clean(all_props_flat),
                }
                _mail_result = send_daily_picks(_mail_state)
                _log(f"[email] Sent daily picks — {_mail_result.get('sent',0)} delivered, "
                     f"{_mail_result.get('failed',0)} failed")
            except Exception as _me:
                _log(f"[email] Send failed: {_me}")

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
    today_str = _et_calendar_today().isoformat()
    tomorrow_str = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()
    today_cards = _normalize_card_list(state.get("game_cards_today", []), expected_date=today_str)
    tomorrow_cards = _normalize_card_list(state.get("game_cards_tomorrow", []), expected_date=tomorrow_str)
    return render_template(
        "dashboard.html",
        state=state,
        bankroll=BANKROLL,
        active_sport=_ACTIVE_SPORT,
        phases=_PHASES,
        today_cards=today_cards,
        tomorrow_cards=tomorrow_cards,
        best_parlays=state.get("best_parlays", []),
        all_props=state.get("player_props", []),
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
        if (
            _state.get("game_cards_today")
            or _state.get("game_cards_tomorrow")
            or _state.get("player_props")
            or _state.get("best_parlays")
        ):
            today_str = _et_calendar_today().isoformat()
            tomorrow_str = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()
            today_cards = _normalize_card_list(_state.get("game_cards_today", []), expected_date=today_str)
            tomorrow_cards = _normalize_card_list(_state.get("game_cards_tomorrow", []), expected_date=tomorrow_str)
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
                "sport":               _ACTIVE_SPORT,
                "status":              _state["status"],
                "last_updated":        _state["last_updated"],
                "cache_updated_at_iso": cache_updated_at_iso,
                "cache_age_min":        cache_age_min,
                "game_cards_today":    today_cards,
                "game_cards_tomorrow": tomorrow_cards,
                "best_parlays":        _state["best_parlays"],
                "player_props":        _state["player_props"],
                "elite_parlay":        _state.get("elite_parlay"),
            })

    if _ACTIVE_SPORT != "all":
        try:
            from data.db import get_analysis_cache
            cached = get_analysis_cache(max_age_hours=22)
            if cached:
                today_str = _et_calendar_today().isoformat()
                tomorrow_str = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()
                cached["game_cards_today"] = _normalize_card_list(cached.get("game_cards_today", []), expected_date=today_str)
                cached["game_cards_tomorrow"] = _normalize_card_list(cached.get("game_cards_tomorrow", []), expected_date=tomorrow_str)
                if not (
                    cached.get("game_cards_today")
                    or cached.get("game_cards_tomorrow")
                    or cached.get("player_props")
                    or cached.get("best_parlays")
                ):
                    cached = None
            if cached:
                cached["ok"] = True
                cached["sport"] = _ACTIVE_SPORT
                return jsonify(cached)
        except Exception:
            pass

    # Fallback: build schedule-only cards so tabs are never blank while analysis/cache is unavailable.
    try:
        today_date = _et_calendar_today()
        today_str = today_date.isoformat()
        tomorrow_str = (today_date + datetime.timedelta(days=1)).isoformat()
        if _ACTIVE_SPORT == "all":
            snap = _build_multi_sport_snapshot(force_refresh=False)
            all_games = snap.get("games") or []
            today_games = [g for g in all_games if str(g.get("game_date") or "") == today_str]
            tomorrow_games = [g for g in all_games if str(g.get("game_date") or "") == tomorrow_str]
            all_bets = snap.get("bets") or []
            fallback_props = _build_all_sport_sentiment_props(all_games, all_bets)
            if not fallback_props and not _ALL_SPORTS_STRICT_SENTIMENT_ONLY:
                fallback_props = _multi_sport_best_bets_rows(all_bets)
            fallback_today = [_build_card(g, all_bets, fallback_props, "TODAY") for g in today_games]
            fallback_tomorrow = [_build_card(g, all_bets, fallback_props, "TOMORROW") for g in tomorrow_games]
        elif _ACTIVE_SPORT == "soccer":
            from data.soccer_fetcher import get_matches_today_all, get_matches_tomorrow_all

            today_games = get_matches_today_all() or []
            tomorrow_games = get_matches_tomorrow_all() or []
            fallback_today = [_build_card(g, [], [], "TODAY") for g in today_games]
            fallback_tomorrow = [_build_card(g, [], [], "TOMORROW") for g in tomorrow_games]
        else:
            from data.mlb_fetcher import get_schedule_range

            all_games = get_schedule_range(days_ahead=2) or []
            today_games = [g for g in all_games if g.get("date", "") == today_str]
            tomorrow_games = [g for g in all_games if g.get("date", "") == tomorrow_str]
            fallback_today = [_build_card(g, [], [], "TODAY") for g in today_games]
            fallback_tomorrow = [_build_card(g, [], [], "TOMORROW") for g in tomorrow_games]

        if fallback_today or fallback_tomorrow:
            return jsonify({
                "ok": True,
                "sport": _ACTIVE_SPORT,
                "status": "idle",
                "last_updated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
                "game_cards_today": fallback_today,
                "game_cards_tomorrow": fallback_tomorrow,
                "best_parlays": [],
                "player_props": _clean(fallback_props) if _ACTIVE_SPORT == "all" else [],
                "elite_parlay": None,
            })
    except Exception:
        pass

    return jsonify({
        "ok": True if _ACTIVE_SPORT == "all" else False,
        "sport": _ACTIVE_SPORT,
        "status": "idle",
        "game_cards_today": [],
        "game_cards_tomorrow": [],
        "best_parlays": [],
        "player_props": [],
        "elite_parlay": None,
    })


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
        db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
        return jsonify({"ok": True, "stats": get_parlay_performance_stats(sport=db_sport)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/parlay/auto-resolve", methods=["POST"])
def api_parlay_auto_resolve():
    """Auto-resolve pending tracked parlays based on leg prediction outcomes."""
    if _ACTIVE_SPORT != "mlb":
        return jsonify({"ok": True, "resolved": 0,
                        "msg": "Auto-resolve is currently available for MLB mode only"})
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
        db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
        return jsonify({"ok": True, "stats": get_performance_stats(sport=db_sport)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/prop-performance")
def api_prop_performance():
    try:
        from data.db import get_prop_performance_stats
        db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
        return jsonify({"ok": True, "stats": get_prop_performance_stats(sport=db_sport)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/predictions")
def api_predictions():
    days    = int(request.args.get("days", 30))
    outcome = request.args.get("outcome")
    try:
        from data.db import get_predictions
        db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
        preds = get_predictions(days=days, outcome=outcome or None, sport=db_sport)
        return jsonify({"ok": True, "predictions": _clean(preds)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "predictions": []})


def _match_status_bucket(status: str) -> str:
    s = str(status or "").lower()
    if any(k in s for k in ("in progress", "in_play", "live", "halftime", "paused")):
        return "live"
    if any(k in s for k in ("final", "finished", "completed")):
        return "finished"
    return "scheduled"


@app.route("/api/tournaments")
def api_tournaments():
    if _ACTIVE_SPORT == "all":
        try:
            snap = _build_multi_sport_snapshot(force_refresh=False)
            return jsonify({"ok": True, "tournaments": snap.get("tournaments", [])})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e), "tournaments": []})
    if _ACTIVE_SPORT != "soccer":
        return jsonify({"ok": True, "tournaments": []})
    try:
        from data.soccer_fetcher import get_tournaments, get_matches_today_all, get_matches_tomorrow_all

        tournaments = get_tournaments() or []
        counts: dict[str, int] = {}
        for m in (get_matches_today_all() or []) + (get_matches_tomorrow_all() or []):
            code = str(m.get("competition") or "").strip().upper()
            if not code:
                continue
            counts[code] = counts.get(code, 0) + 1

        payload = []
        for t in tournaments:
            code = str(t.get("code") or "").upper()
            row = dict(t)
            row["match_count"] = int(counts.get(code, 0))
            payload.append(row)

        payload.sort(key=lambda x: (x.get("match_count", 0), x.get("name", "")), reverse=True)
        return jsonify({"ok": True, "tournaments": payload})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "tournaments": []})


@app.route("/api/tournament/data")
def api_tournament_data():
    if _ACTIVE_SPORT == "all":
        code = str(request.args.get("code", "") or "").strip()
        if not code:
            return jsonify({"ok": False, "error": "Missing sport code", "matches": [], "standings": [], "top_scorers": []}), 400
        query = str(request.args.get("q", "") or "").strip().lower()
        status_filter = str(request.args.get("status", "all") or "all").strip().lower()
        try:
            snap = _build_multi_sport_snapshot(force_refresh=bool(request.args.get("refresh")))
            tournaments = {str(t.get("code") or ""): t for t in (snap.get("tournaments") or [])}
            if code not in tournaments:
                return jsonify({"ok": False, "error": f"Unsupported sport code: {code}", "matches": [], "standings": [], "top_scorers": []}), 404

            matches = []
            for m in (snap.get("games") or []):
                if str(m.get("competition") or "") != code:
                    continue
                bucket = _match_status_bucket(m.get("status", ""))
                if status_filter in {"scheduled", "live", "finished"} and bucket != status_filter:
                    continue
                if query:
                    home = str(m.get("home_team") or "").lower()
                    away = str(m.get("away_team") or "").lower()
                    if query not in home and query not in away:
                        continue
                row = dict(m)
                row["status_bucket"] = bucket
                matches.append(row)

            matches.sort(key=lambda x: (x.get("game_date") or x.get("date") or "", x.get("game_time") or ""))
            return jsonify({
                "ok": True,
                "code": code,
                "competition": tournaments.get(code, {"code": code, "name": code}),
                "matches": matches,
                "standings": [],
                "top_scorers": [],
            })
        except Exception as e:
            return jsonify({"ok": False, "error": str(e), "matches": [], "standings": [], "top_scorers": []})

    if _ACTIVE_SPORT != "soccer":
        return jsonify({"ok": False, "error": "Tournament data is available only in soccer mode"}), 400

    code = str(request.args.get("code", "PL") or "PL").strip().upper()
    try:
        days = int(request.args.get("days", 7) or 7)
    except Exception:
        days = 7
    days = max(1, min(days, 14))
    query = str(request.args.get("q", "") or "").strip().lower()
    status_filter = str(request.args.get("status", "all") or "all").strip().lower()

    try:
        from data.soccer_fetcher import (
            TOURNAMENTS,
            get_competition_info,
            get_matches_in_range,
            get_standings,
            get_top_scorers,
        )

        if code not in TOURNAMENTS:
            return jsonify({"ok": False, "error": f"Unsupported tournament code: {code}"}), 404

        start_date = _et_calendar_today()
        end_date = start_date + datetime.timedelta(days=days)
        matches = get_matches_in_range(code, start_date.isoformat(), end_date.isoformat()) or []

        filtered = []
        for m in matches:
            bucket = _match_status_bucket(m.get("status", ""))
            if status_filter in {"scheduled", "live", "finished"} and bucket != status_filter:
                continue
            if query:
                home = str(m.get("home_team") or "").lower()
                away = str(m.get("away_team") or "").lower()
                if query not in home and query not in away:
                    continue
            row = dict(m)
            row["status_bucket"] = bucket
            filtered.append(row)

        filtered.sort(key=lambda x: (x.get("game_date") or x.get("date") or "", x.get("game_time") or ""))
        standings = get_standings(code) or []
        top_scorers = get_top_scorers(code, limit=20) or []
        return jsonify({
            "ok": True,
            "code": code,
            "competition": get_competition_info(code),
            "matches": filtered,
            "standings": standings,
            "top_scorers": top_scorers,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "matches": [], "standings": [], "top_scorers": []})


@app.route("/api/resolve-outcomes", methods=["POST"])
def api_resolve_outcomes():
    if _ACTIVE_SPORT != "mlb":
        return jsonify({
            "ok": True,
            "resolved_games": 0,
            "resolved_props": 0,
            "resolved_parlays": 0,
            "msg": "Auto-resolve is currently available for MLB mode only",
        })
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
        dedupe_raw = str(data.get("dedupe_pending", "0")).strip().lower()
        dedupe_pending = dedupe_raw in {"1", "true", "yes", "on"}
        raw_legs = data.get("legs", [])
        norm_legs = []
        if isinstance(raw_legs, list):
            for leg in raw_legs:
                if isinstance(leg, dict):
                    leg_payload = dict(leg)
                    leg_payload.setdefault("sport", _ACTIVE_SPORT)
                    norm_legs.append(leg_payload)
        pid = save_tracked_parlay(
            name=data.get("name", "My Parlay"),
            legs=norm_legs,
            combined_odds=float(data.get("combined_odds", 0)),
            stake_usd=float(data.get("stake_usd", 0)),
            dedupe_pending=dedupe_pending,
        )
        return jsonify({"ok": True, "id": pid})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/parlay/list")
def api_parlay_list():
    try:
        from data.db import get_tracked_parlays
        inc = str(request.args.get("include_resolved", "1")).strip().lower()
        include_resolved = inc in {"1", "true", "yes", "on"}
        current_only_raw = str(request.args.get("current_only", "1")).strip().lower()
        current_only = current_only_raw in {"1", "true", "yes", "on"}
        target_date = _et_calendar_today() if current_only else None
        db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
        return jsonify({
            "ok": True,
            "parlays": _clean(get_tracked_parlays(
                include_resolved=include_resolved,
                target_date=target_date,
                sport=db_sport,
            )),
        })
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


@app.route("/api/email/recipients")
def api_email_recipients():
    """Return the configured email recipients from ENV."""
    try:
        raw = os.getenv("EMAIL_TO", "")
        recipients = [e.strip() for e in raw.split(",") if e.strip()]
        return jsonify({"ok": True, "recipients": recipients})
    except Exception as e:
        return jsonify({"ok": False, "recipients": [], "error": str(e)})


@app.route("/api/email/send", methods=["POST"])
def api_email_send():
    """Manually trigger a daily picks email to all configured recipients."""
    try:
        from email_notify import send_daily_picks
        with _lock:
            state = {
                "best_parlays":     list(_state.get("best_parlays", [])),
                "game_cards_today": list(_state.get("game_cards_today", [])),
                "player_props":     list(_state.get("player_props", [])),
            }
        result = send_daily_picks(state)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/email/send-parlay", methods=["POST"])
def api_email_send_parlay():
    """Send a parlay alert email."""
    data = request.get_json(force=True) or {}
    try:
        from email_notify import send_parlay_alert
        result = send_parlay_alert(data)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/live-scores")
def api_live_scores():
    """Poll MLB Stats API for today's game statuses/scores."""
    if _ACTIVE_SPORT == "soccer":
        try:
            from data.soccer_fetcher import get_live_matches
            live = get_live_matches() or []
            games = []
            for g in live:
                away = g.get("away_team", "")
                home = g.get("home_team", "")
                game_key = _compose_game_key(
                    away,
                    home,
                    g.get("game_datetime"),
                    g.get("game_date") or g.get("date"),
                    g.get("game_time"),
                )
                games.append({
                    "home_team": home,
                    "away_team": away,
                    "home_score": g.get("home_score"),
                    "away_score": g.get("away_score"),
                    "status": g.get("status"),
                    "inning": g.get("minute", ""),
                    "inning_half": "",
                    "match_key": _norm_gk(g.get("match_key") or f"{away}@{home}"),
                    "game_key": game_key,
                })
            return jsonify({"ok": True, "games": games})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e), "games": []})
    try:
        import statsapi as mlbstatsapi
        from data.mlb_fetcher import _parse_mlb_game
        today = _et_calendar_today().strftime("%m/%d/%Y")
        raw = mlbstatsapi.schedule(start_date=today, end_date=today) or []
        games = []
        for g in raw:
            status = g.get("status", "")
            parsed = _parse_mlb_game(g, _et_calendar_today().isoformat())
            match_key = _norm_gk(f"{g.get('away_name','')}@{g.get('home_name','')}")
            games.append({
                "game_pk":     g.get("game_id"),
                "home_team":   g.get("home_name", ""),
                "away_team":   g.get("away_name", ""),
                "home_score":  g.get("home_score"),
                "away_score":  g.get("away_score"),
                "status":      status,
                "inning":      g.get("current_inning", ""),
                "inning_half": g.get("inning_state", ""),
                "match_key":   match_key,
                "game_key":    _compose_game_key(
                    g.get("away_name", ""),
                    g.get("home_name", ""),
                    parsed.get("game_datetime"),
                    parsed.get("date"),
                    parsed.get("game_time"),
                ),
            })
        return jsonify({"ok": True, "games": games})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "games": []})


# ─── Live-score background watcher ───────────────────────────────────────────
_live_score_timer = None
_LIVE_SCORE_INTERVAL = 120  # seconds (2 min)
_eod_email_sent_dates: set = set()  # track which dates have had EOD results email sent
_cache_poll_timer = None
_CACHE_POLL_INTERVAL = int(os.getenv("CACHE_POLL_INTERVAL_SEC", "120"))


def _sync_state_from_cache(broadcast: bool = False) -> bool:
    """Refresh in-memory state from DB cache when available."""
    if _ACTIVE_SPORT == "all":
        return False
    try:
        from data.db import get_analysis_cache
        cached = get_analysis_cache(max_age_hours=22)
        if not cached:
            return False
        today_str = _et_calendar_today().isoformat()
        tomorrow_str = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()
        today_cards = _normalize_card_list(cached.get("game_cards_today", []), expected_date=today_str)
        tomorrow_cards = _normalize_card_list(cached.get("game_cards_tomorrow", []), expected_date=tomorrow_str)
        cache_iso = cached.get("cache_updated_at_iso")
        with _lock:
            if cache_iso and cache_iso == _state.get("last_updated_ts"):
                return False
            if _state.get("status") == "running":
                return False
            _state.update({
                "game_cards_today":    today_cards,
                "game_cards_tomorrow": tomorrow_cards,
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
                "game_cards_today":    today_cards,
                "game_cards_tomorrow": tomorrow_cards,
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
    if _ACTIVE_SPORT != "mlb":
        return
    if _live_score_timer is None:
        _poll_live_scores()


def _poll_live_scores():
    """Runs in background: updates live scores and auto-resolves completed games."""
    global _live_score_timer
    try:
        import statsapi as mlbstatsapi
        from data.mlb_fetcher import _parse_mlb_game
        # Use calendar ET date (no 10 PM cutover) to avoid dropping late live games.
        today = _et_calendar_today()
        today_str = today.strftime("%m/%d/%Y")
        raw = mlbstatsapi.schedule(start_date=today_str, end_date=today_str) or []

        state_map = {}
        for g in raw:
            parsed = _parse_mlb_game(g, today.isoformat())
            match_key = _norm_gk(f"{g.get('away_name','')}@{g.get('home_name','')}")
            key = _compose_game_key(
                g.get("away_name", ""),
                g.get("home_name", ""),
                parsed.get("game_datetime"),
                parsed.get("date"),
                parsed.get("game_time"),
            )
            state_map[key] = {
                "game_pk":     g.get("game_id"),
                "match_key":   match_key,
                "game_key":    key,
                "home_score":  g.get("home_score"),
                "away_score":  g.get("away_score"),
                "status":      g.get("status"),
                "inning":      g.get("current_inning", ""),
                "inning_half": g.get("inning_state", ""),
            }
        with _lock:
            _state["live_scores"] = state_map

        # Broadcast full status map every poll (including empty) so clients can clear stale entries.
        _sse_broadcast("live_scores", {"scores": state_map})

        # Auto-resolve finished games (non-blocking, errors suppressed)
        def _is_final_status(status):
            s = (status or "").lower()
            return any(k in s for k in ("final", "game over", "completed"))
        
        if any(_is_final_status(g.get("status", "")) for g in raw):
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
                            "stats":        get_performance_stats(sport="mlb"),
                            "parlay_stats": get_parlay_performance_stats(sport="mlb"),
                        })
                    except Exception:
                        pass
            except Exception as exc:
                print(f"[live-scores] resolve error: {exc}")

        # ── EOD results email — fire once when all today's games are Final ──
        today_key = today.isoformat()
        all_today_final = bool(raw) and all(_is_final_status(g.get("status","")) for g in raw)
        if all_today_final and today_key not in _eod_email_sent_dates:
            print(f"[live-scores] All today's games final — building EOD results email for {today_key}")
            try:
                from data.db import get_conn
                import psycopg2.extras
                conn = get_conn()
                rows = []
                if conn:
                    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    cur.execute("""
                        SELECT bet_type, pick, odds_am, model_prob, confidence,
                               home_team, away_team, outcome
                        FROM predictions
                        WHERE game_date = %s AND sport = 'mlb' AND outcome IN ('WIN','LOSS','PUSH')
                        ORDER BY outcome, model_prob DESC
                    """, (today_key,))
                    rows = [dict(r) for r in cur.fetchall()]
                    conn.close()

                prop_rows = []
                try:
                    conn2 = get_conn()
                    if conn2:
                        cur2 = conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                        cur2.execute("""
                            SELECT name, team, stat_type, prop_label, line, direction, outcome, actual
                            FROM prop_history
                            WHERE game_date = %s AND sport = 'mlb' AND outcome IN ('WIN','LOSS','PUSH')
                            ORDER BY outcome
                        """, (today_key,))
                        prop_rows = [dict(r) for r in cur2.fetchall()]
                        conn2.close()
                except Exception:
                    pass

                wins   = sum(1 for r in rows if r.get("outcome") == "WIN")
                losses = sum(1 for r in rows if r.get("outcome") == "LOSS")
                pushes = sum(1 for r in rows if r.get("outcome") == "PUSH")
                total  = wins + losses + pushes
                hit_rate = round(wins / total * 100, 1) if total > 0 else 0.0

                picks_formatted = [
                    {
                        "pick":      r.get("pick",""),
                        "bet_type":  r.get("bet_type",""),
                        "outcome":   r.get("outcome",""),
                        "game":      f"{r.get('away_team','')} @ {r.get('home_team','')}",
                        "odds_am":   r.get("odds_am"),
                        "model_prob":r.get("model_prob",0),
                    }
                    for r in rows
                ]

                import datetime as _dt
                results_payload = {
                    "date_str": _dt.date.today().strftime("%A, %B %d, %Y"),
                    "total":    total,
                    "wins":     wins,
                    "losses":   losses,
                    "pushes":   pushes,
                    "hit_rate": hit_rate,
                    "picks":    picks_formatted,
                    "props":    prop_rows,
                    "parlays":  [],
                }

                from email_notify import send_daily_results
                result = send_daily_results(results_payload)
                if result.get("sent", 0) > 0:
                    _eod_email_sent_dates.add(today_key)
                    print(f"[live-scores] EOD results email sent ({wins}W/{losses}L/{pushes}P)")
                else:
                    print(f"[live-scores] EOD email failed: {result.get('errors')}")
            except Exception as _eod_e:
                print(f"[live-scores] EOD email error: {_eod_e}")
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
    lock_date = _et_calendar_today() if lock_today else None
    threading.Thread(target=_run_analysis, args=(lock_date,), daemon=True).start()


def _start_scheduler():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.interval import IntervalTrigger
        from apscheduler.triggers.cron import CronTrigger
        sched = BackgroundScheduler(daemon=True)
        if _AUTO_ANALYSIS_INTERVAL_MIN > 0:
            sched.add_job(
                _scheduled_analysis,
                IntervalTrigger(minutes=_AUTO_ANALYSIS_INTERVAL_MIN),
                id="auto_analysis",
                max_instances=1,
                coalesce=True,
            )
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
        if _AUTO_ANALYSIS_INTERVAL_MIN > 0:
            print(f"[scheduler] APScheduler started — analysis every {_AUTO_ANALYSIS_INTERVAL_MIN} minutes")
        else:
            print("[scheduler] APScheduler started — daily morning snapshot only")
        return sched
    except Exception as e:
        print(f"[scheduler] Could not start APScheduler: {e}")
        return None


def _load_boot_schedule_fallback() -> bool:
    """Populate state with schedule-only cards so UI isn't blank when cache is absent."""
    try:
        today_date = _et_calendar_today()
        today_str = today_date.isoformat()
        tomorrow_str = (today_date + datetime.timedelta(days=1)).isoformat()
        if _ACTIVE_SPORT == "all":
            snap = _build_multi_sport_snapshot(force_refresh=False)
            all_games = snap.get("games") or []
            all_bets = snap.get("bets") or []
            boot_props = _build_all_sport_sentiment_props(all_games, all_bets)
            if not boot_props and not _ALL_SPORTS_STRICT_SENTIMENT_ONLY:
                boot_props = _multi_sport_best_bets_rows(all_bets)
            today_games = [g for g in all_games if str(g.get("game_date") or "") == today_str]
            tomorrow_games = [g for g in all_games if str(g.get("game_date") or "") == tomorrow_str]
            today_cards = [_build_card(g, all_bets, boot_props, "TODAY") for g in today_games]
            tomorrow_cards = [_build_card(g, all_bets, boot_props, "TOMORROW") for g in tomorrow_games]
        elif _ACTIVE_SPORT == "soccer":
            from data.soccer_fetcher import get_matches_today_all, get_matches_tomorrow_all

            today_games = get_matches_today_all() or []
            tomorrow_games = get_matches_tomorrow_all() or []
            today_cards = [_build_card(g, [], [], "TODAY") for g in today_games]
            tomorrow_cards = [_build_card(g, [], [], "TOMORROW") for g in tomorrow_games]
        else:
            from data.mlb_fetcher import get_schedule_range

            all_games = get_schedule_range(days_ahead=2) or []
            today_games = [g for g in all_games if g.get("date", "") == today_str]
            tomorrow_games = [g for g in all_games if g.get("date", "") == tomorrow_str]
            today_cards = [_build_card(g, [], [], "TODAY") for g in today_games]
            tomorrow_cards = [_build_card(g, [], [], "TOMORROW") for g in tomorrow_games]

        with _lock:
            _state.update({
                "status": "idle",
                "game_cards_today": today_cards,
                "game_cards_tomorrow": tomorrow_cards,
                "best_parlays": [],
                "player_props": _clean(boot_props) if _ACTIVE_SPORT == "all" else [],
                "last_updated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
            })

        print(f"[boot] Loaded schedule fallback ({len(today_cards)} today, {len(tomorrow_cards)} tomorrow)")
        return True
    except Exception as exc:
        print(f"[boot] Schedule fallback load failed: {exc}")
        return False


def _auto_boot_analysis():
    """On startup: load today's DB snapshot, or generate one if today's snapshot is missing/stale."""
    if _ACTIVE_SPORT == "all":
        _load_boot_schedule_fallback()
        threading.Thread(target=_run_analysis, daemon=True).start()
        return
    try:
        from data.db import get_analysis_cache
        today_str    = _et_calendar_today().isoformat()
        tomorrow_str = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()
        cached = get_analysis_cache(max_age_hours=22)

        if cached:
            raw_today    = cached.get("game_cards_today", [])
            raw_tomorrow = cached.get("game_cards_tomorrow", [])

            # Validate that cached cards actually match today's calendar date
            today_dates    = {c.get("game_date") for c in raw_today    if isinstance(c, dict)}
            tomorrow_dates = {c.get("game_date") for c in raw_tomorrow if isinstance(c, dict)}
            cache_is_fresh = (
                (not raw_today    or today_str    in today_dates)
                and
                (not raw_tomorrow or tomorrow_str in tomorrow_dates)
            )

            if cache_is_fresh:
                with _lock:
                    _state.update({
                        "game_cards_today":    _normalize_card_list(raw_today,    expected_date=today_str),
                        "game_cards_tomorrow": _normalize_card_list(raw_tomorrow, expected_date=tomorrow_str),
                        "best_parlays":        cached.get("best_parlays", []),
                        "player_props":        cached.get("player_props", []),
                        "last_updated":        cached.get("last_updated"),
                    })
                n_today = len(_state["game_cards_today"])
                n_tmrw  = len(_state["game_cards_tomorrow"])
                print(f"[boot] Loaded valid today cache — {n_today} today, {n_tmrw} tomorrow "
                      f"(last updated: {cached.get('last_updated')})")
                # If cached cards are empty (no games at all), still trigger a refresh
                if n_today == 0 and n_tmrw == 0:
                    print("[boot] Cache has 0 games — triggering fresh analysis...")
                    threading.Thread(target=_run_analysis, daemon=True).start()
                return
            else:
                stale_dates = today_dates | tomorrow_dates
                print(f"[boot] Cache has stale game dates {stale_dates} (expected {today_str}) "
                      f"— triggering fresh analysis to replace stale data...")
        else:
            _load_boot_schedule_fallback()
            print(f"[boot] No cache for {today_str} — triggering fresh analysis...")

        # Always run fresh analysis when cache is missing or stale
        threading.Thread(target=_run_analysis, daemon=True).start()

    except Exception as e:
        print(f"[boot] Auto-boot error: {e}")
        if not _load_boot_schedule_fallback():
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
