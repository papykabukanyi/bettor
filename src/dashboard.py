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
import math

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
        _start_outcome_resolver()
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


def _attach_tracking_uids(game_bets: list[dict], prop_rows: list[dict]):
    """Attach deterministic IDs to every displayed/saved bet row."""
    try:
        from data.db import _prediction_uid, _prop_uid
    except Exception:
        return

    for bet in game_bets or []:
        if not isinstance(bet, dict):
            continue
        game_date = (
            bet.get("game_date")
            or bet.get("date")
            or _et_calendar_today().isoformat()
        )
        payload = {
            "sport": bet.get("sport") or _ACTIVE_SPORT,
            "game_date": game_date,
            "game_key": bet.get("game_key") or bet.get("game") or bet.get("match_key") or "",
            "bet_type": bet.get("bet_type") or "",
            "pick": bet.get("pick") or "",
            "line": bet.get("line"),
        }
        uid = bet.get("bet_uid") or bet.get("prediction_uid") or _prediction_uid(payload)
        if uid:
            bet["bet_uid"] = uid
            bet.setdefault("prediction_uid", uid)

    for prop in prop_rows or []:
        if not isinstance(prop, dict):
            continue
        game_date = (
            prop.get("date")
            or prop.get("game_date")
            or _et_calendar_today().isoformat()
        )
        payload = {
            "sport": prop.get("sport") or _ACTIVE_SPORT,
            "game_date": game_date,
            "date": game_date,
            "game_key": prop.get("game_key") or prop.get("game") or prop.get("match_key") or "",
            "name": prop.get("name") or prop.get("player_name") or "",
            "player_name": prop.get("player_name") or prop.get("name") or "",
            "team": prop.get("team") or "",
            "stat_type": prop.get("stat_type") or prop.get("prop_type") or "",
            "prop_type": prop.get("prop_type") or prop.get("stat_type") or "",
            "line": prop.get("line"),
            "direction": prop.get("direction") or prop.get("recommendation") or "",
            "recommendation": prop.get("recommendation") or prop.get("direction") or "",
        }
        uid = prop.get("bet_uid") or prop.get("prediction_uid") or _prop_uid(payload, game_date=game_date)
        if uid:
            prop["bet_uid"] = uid
            prop.setdefault("prediction_uid", uid)


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
        "suggested_bets": [],
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
        kn = _norm_gk(str(k or ""))
        if not kn:
            return False

        # Exact key match first (supports fully-qualified unique keys).
        if kn in (gk_norm, alt_norm, rev_gk, unique_norm):
            return True

        # If both keys include a unique suffix, require exact match to avoid
        # leaking props/bets across different scheduled instances.
        if "#" in kn and "#" in unique_norm:
            return False

        base = kn.split("#", 1)[0]
        return base in (gk_norm, alt_norm, rev_gk)

    def _team_token(name: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())

    def _team_aliases(name: str) -> set[str]:
        words = [w for w in re.findall(r"[a-z0-9]+", str(name or "").lower()) if w]
        aliases: set[str] = set()
        full = "".join(words)
        if full:
            aliases.add(full)
        if words:
            aliases.add(words[-1])
            aliases.add("".join(w[0] for w in words))
        if len(words) >= 2:
            aliases.add("".join(words[-2:]))
        return {a for a in aliases if len(a) >= 2}

    home_token = _team_token(ht)
    away_token = _team_token(at)
    home_aliases = _team_aliases(ht)
    away_aliases = _team_aliases(at)

    def _same_matchup(row: dict) -> bool:
        rh = _team_token(row.get("home_team") or "")
        ra = _team_token(row.get("away_team") or "")
        if not rh or not ra:
            return False
        return (rh == home_token and ra == away_token) or (rh == away_token and ra == home_token)

    card_date = str(card.get("game_date") or "").strip()

    matched_bets = {}

    for bet in bets:
        bet_sport = _infer_sport_group(
            bet.get("sport") or bet.get("competition") or bet.get("league") or ""
        )
        if bet_sport not in {"other", ""} and sport_group not in {"other", ""} and bet_sport != sport_group:
            continue

        bet_date = str(bet.get("game_date") or bet.get("date") or "").strip()
        if card_date and bet_date and bet_date != card_date:
            continue

        bk = bet.get("game_key", bet.get("game", ""))
        bm = bet.get("match_key", "")
        if not (_key_matches(bk) or _key_matches(bm)):
            continue

        # Keep every matched market for modal/details rendering.
        sig = "|".join([
            str(bet.get("bet_type") or ""),
            str(bet.get("pick") or ""),
            str(bet.get("line") if bet.get("line") is not None else ""),
            str(bet.get("odds_am") if bet.get("odds_am") is not None else ""),
        ])
        prev = matched_bets.get(sig)
        if prev is None or float(bet.get("model_prob") or 0.0) > float(prev.get("model_prob") or 0.0):
            matched_bets[sig] = bet

        slot = _slot_for_bet(bet)
        if slot:
            current = card[slot]
            if current is None or bet.get("safety", 0) > current.get("safety", 0):
                card[slot] = bet

    card["suggested_bets"] = sorted(
        matched_bets.values(),
        key=lambda b: float(b.get("model_prob") or 0.0),
        reverse=True,
    )

    for p in props:
        prop_sport = _infer_sport_group(
            p.get("sport") or p.get("competition") or p.get("league") or ""
        )
        if prop_sport not in {"other", ""} and sport_group not in {"other", ""} and prop_sport != sport_group:
            continue

        prop_date = str(p.get("game_date") or p.get("date") or "").strip()
        if card_date and prop_date and prop_date != card_date:
            continue

        pk = p.get("game_key", p.get("game", ""))
        pm = p.get("match_key", "")
        key_match = _key_matches(pk) or _key_matches(pm)
        if not key_match and not _same_matchup(p):
            continue

        team_token = _team_token(p.get("team") or "")
        is_home = team_token in home_aliases if team_token else False
        is_away = team_token in away_aliases if team_token else False

        # Skip ambiguous/unmapped props instead of attaching to the wrong team.
        if is_home == is_away:
            continue

        if is_home:
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

    token = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    compact = token.replace("_", "")
    exact = {
        "mlb": "baseball",
        "baseball": "baseball",
        "baseball_mlb": "baseball",
        "soccer": "soccer",
        "football": "soccer",
        "basketball": "basketball",
        "nba": "basketball",
        "wnba": "basketball",
        "americanfootball": "americanfootball",
        "american_football": "americanfootball",
        "nfl": "americanfootball",
        "ncaaf": "americanfootball",
        "icehockey": "icehockey",
        "ice_hockey": "icehockey",
        "hockey": "icehockey",
        "nhl": "icehockey",
        "tennis": "tennis",
        "mma": "mma",
    }
    if token in exact:
        return exact[token]
    if compact in exact:
        return exact[compact]

    if any(k in token for k in ("american_football", "americanfootball", "nfl", "ncaaf", "college_football", "xfl", "ufl", "cfl")):
        return "americanfootball"
    if any(k in token for k in ("ice_hockey", "icehockey", "nhl", "hockey")):
        return "icehockey"
    if any(k in token for k in ("basketball", "nba", "wnba", "ncaab", "euroleague")):
        return "basketball"
    if any(k in token for k in ("baseball", "mlb", "npb", "kbo")):
        return "baseball"
    if any(k in token for k in ("soccer", "mls", "epl", "bundesliga", "la_liga", "ligue", "serie_a", "uefa", "fifa", "eng_1", "ger_1", "ita_1", "esp_1", "fra_1", "ned_1", "por_1", "champions")):
        return "soccer"
    if any(k in token for k in ("tennis", "atp", "wta")):
        return "tennis"
    if any(k in token for k in ("mma", "ufc", "bellator", "pfl")):
        return "mma"

    return token or "other"


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

    def _as_score_int(value):
        try:
            if value is None:
                return None
            if isinstance(value, str) and not value.strip():
                return None
            return int(float(value))
        except Exception:
            return None

    def _record_summary(comp: dict) -> str:
        recs = comp.get("records") or []
        if not recs:
            return ""
        for rec in recs:
            rtype = str(rec.get("type") or "").strip().lower()
            rname = str(rec.get("name") or "").strip().lower()
            if rtype in {"total", "overall"} or rname in {"overall", "total"}:
                return str(rec.get("summary") or "")
        return str(recs[0].get("summary") or "")

    def _record_win_pct(summary: str) -> float | None:
        s = str(summary or "").strip()
        if not s:
            return None
        nums = [int(x) for x in re.findall(r"\d+", s)]
        if len(nums) < 2:
            return None
        wins = float(nums[0])
        losses = float(nums[1])
        draws = float(nums[2]) if len(nums) >= 3 else 0.0
        total = wins + losses + draws
        if total <= 0:
            return None
        return round((wins + 0.5 * draws) / total, 4)

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

    # 3) ESPN multi-sport scoreboards (free) for non-MLB/soccer coverage.
    espn_enabled = str(os.getenv("ENABLE_ESPN_MULTI_SPORT_FALLBACK", "1")).strip().lower() in {"1", "true", "yes", "on"}
    if espn_enabled:
        try:
            import requests

            espn_sources = [
                ("basketball", "nba", "basketball", "NBA"),
                ("basketball", "wnba", "basketball", "WNBA"),
                ("hockey", "nhl", "icehockey", "NHL"),
                ("football", "nfl", "americanfootball", "NFL"),
                ("football", "college-football", "americanfootball", "NCAAF"),
                ("basketball", "mens-college-basketball", "basketball", "NCAAB"),
                ("basketball", "womens-college-basketball", "basketball", "WNCAAB"),
                ("tennis", "atp", "tennis", "ATP"),
                ("tennis", "wta", "tennis", "WTA"),
                ("mma", "ufc", "mma", "UFC"),
            ]
            for d in (today, tomorrow):
                dates_token = d.strftime("%Y%m%d")
                for sport_path, league_path, sport_group, league_label in espn_sources:
                    url = f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/{league_path}/scoreboard"
                    try:
                        resp = requests.get(url, params={"dates": dates_token, "limit": 200}, timeout=8)
                        if resp.status_code != 200:
                            continue
                        data = resp.json() or {}
                    except Exception:
                        continue

                    for ev in (data.get("events") or []):
                        comp = (ev.get("competitions") or [{}])[0]
                        competitors = comp.get("competitors") or []
                        if len(competitors) < 2:
                            continue

                        home_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "home"), competitors[0])
                        away_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "away"), competitors[1] if len(competitors) > 1 else competitors[0])
                        home = str(((home_c.get("team") or {}).get("displayName") or "")).strip()
                        away = str(((away_c.get("team") or {}).get("displayName") or "")).strip()
                        if not home or not away:
                            continue

                        iso_dt = str(ev.get("date") or comp.get("date") or "").strip()
                        game_date, game_time = _datetime_to_et_parts(iso_dt)
                        if not game_date:
                            game_date = d.isoformat()

                        status_desc = str((((ev.get("status") or {}).get("type") or {}).get("description")) or "").strip()
                        status_state = str((((ev.get("status") or {}).get("type") or {}).get("state")) or "").strip().lower()
                        status_low = status_desc.lower()
                        if status_state in {"post", "final", "finished"} or "final" in status_low:
                            status = "Final"
                        elif status_state in {"in", "in_progress", "live"} or "progress" in status_low or "halftime" in status_low:
                            status = "In Progress"
                        else:
                            status = "Scheduled"

                        competition_name = str((comp.get("league") or {}).get("name") or league_label).strip()
                        comp_code = f"espn_{_slug_token(sport_group)}_{_slug_token(league_path)}".upper()[:64]
                        before = len(rows)
                        _push_game(
                            sport_group=sport_group,
                            league=competition_name or league_label,
                            competition=comp_code,
                            competition_name=competition_name or league_label,
                            home=home,
                            away=away,
                            game_date=game_date,
                            game_time=game_time,
                            game_datetime=iso_dt,
                            status=status,
                            home_score=_as_score_int(home_c.get("score")),
                            away_score=_as_score_int(away_c.get("score")),
                        )
                        if len(rows) > before:
                            home_rec = _record_summary(home_c)
                            away_rec = _record_summary(away_c)
                            rows[-1].update({
                                "source": "espn",
                                "espn_event_id": str(ev.get("id") or "").strip(),
                                "espn_sport_path": sport_path,
                                "espn_league_path": league_path,
                                "home_record": home_rec,
                                "away_record": away_rec,
                                "home_record_pct": _record_win_pct(home_rec),
                                "away_record_pct": _record_win_pct(away_rec),
                                "home_rank": home_c.get("curatedRank") or (home_c.get("team") or {}).get("rank"),
                                "away_rank": away_c.get("curatedRank") or (away_c.get("team") or {}).get("rank"),
                            })
        except Exception as e:
            _log(f"[all-sports] ESPN multi-sport fallback fetch failed: {e}")

    # 4) TheSportsDB (multi-sport free fixture feed) - opt-in due endpoint variability.
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

    def _record_win_pct(summary: str) -> float | None:
        s = str(summary or "").strip()
        if not s:
            return None
        nums = [int(x) for x in re.findall(r"\d+", s)]
        if len(nums) < 2:
            return None
        wins = float(nums[0])
        losses = float(nums[1])
        draws = float(nums[2]) if len(nums) >= 3 else 0.0
        total = wins + losses + draws
        if total <= 0:
            return None
        return (wins + (0.5 * draws)) / total

    def _to_float(value) -> float | None:
        try:
            if value is None:
                return None
            if isinstance(value, str) and not value.strip():
                return None
            return float(value)
        except Exception:
            return None

    def _to_int(value) -> int | None:
        try:
            if value is None:
                return None
            if isinstance(value, str) and not value.strip():
                return None
            return int(float(value))
        except Exception:
            return None

    def _estimate_home_prob(game: dict, sport: str) -> float:
        if sport == "soccer":
            base = 0.45
        elif sport in {"baseball", "mlb"}:
            base = 0.55
        else:
            base = 0.53

        hp = _to_float(game.get("home_record_pct"))
        ap = _to_float(game.get("away_record_pct"))
        if hp is None:
            hp = _record_win_pct(game.get("home_record") or "")
        if ap is None:
            ap = _record_win_pct(game.get("away_record") or "")
        if hp is not None and ap is not None:
            base = 0.5 + ((hp - ap) * 0.75)
            if sport != "soccer":
                base += 0.02  # modest home-edge prior

        hr = _to_int(game.get("home_rank"))
        ar = _to_int(game.get("away_rank"))
        if hr and ar and hr > 0 and ar > 0:
            # Lower rank number is stronger.
            rank_edge = (ar - hr) / max(10.0, float(ar + hr))
            base += max(-0.08, min(0.08, rank_edge * 0.6))

        hs = _to_int(game.get("home_score"))
        aw = _to_int(game.get("away_score"))
        status = str(game.get("status") or "").strip().lower()
        if hs is not None and aw is not None:
            diff = hs - aw
            if "final" in status:
                if diff > 0:
                    return 0.99
                if diff < 0:
                    return 0.01
                return 0.50
            if "progress" in status or "live" in status:
                base += max(-0.22, min(0.22, diff * 0.035))

        return max(0.05, min(0.95, float(base)))

    # Deterministic, zero-network fallback pick generation.
    # Used only when sportsbook/model feeds are unavailable.
    for g in (games or [])[:120]:
        home = str(g.get("home_team") or "").strip()
        away = str(g.get("away_team") or "").strip()
        if not home or not away:
            continue

        sport = _infer_sport_group(g.get("sport") or g.get("competition") or "")
        home_prob = _estimate_home_prob(g, sport)
        pick_home = home_prob >= 0.5
        pick_prob = home_prob if pick_home else (1.0 - home_prob)
        pick_team = home if pick_home else away
        if sport == "soccer":
            bet_type = "1X2"
            pick = f"{pick_team} to Win"
        else:
            bet_type = "moneyline"
            pick = f"{pick_team} ML"

        odds_am = _prob_to_american(pick_prob)
        label = _rank_label(pick_prob)
        game_date = g.get("game_date") or g.get("date") or today_str
        game_key = g.get("game_key") or _compose_game_key(
            away,
            home,
            g.get("game_datetime"),
            game_date,
            g.get("game_time"),
        )

        reason = "Fallback baseline pick while live odds are unavailable"
        home_rec = str(g.get("home_record") or "").strip()
        away_rec = str(g.get("away_record") or "").strip()
        if home_rec or away_rec:
            reason = f"Record-based fallback ({away} {away_rec or '?'} at {home} {home_rec or '?'})"

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
            "model_prob": round(pick_prob, 4),
            "confidence": int(round(pick_prob * 100)),
            "safety_label": label,
            "safety": _safety_score_from_label(label),
            "game_date": game_date,
            "game_time": g.get("game_time") or "",
            "home_team": home,
            "away_team": away,
            "match_key": g.get("match_key") or _norm_gk(f"{away}@{home}"),
            "game_key": game_key,
            "worth_it": pick_prob >= 0.53,
            "worth_score": round(pick_prob * 100.0, 2),
            "worth_reason": reason,
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


def _merge_all_sports_table_rows(sentiment_rows: list[dict], best_bet_rows: list[dict]) -> list[dict]:
    """Combine sentiment/player rows with best-bet rows for sports not covered by props."""
    s_rows = list(sentiment_rows or [])
    b_rows = list(best_bet_rows or [])

    if _ALL_SPORTS_STRICT_SENTIMENT_ONLY:
        return s_rows[:400]
    if not s_rows:
        return b_rows[:400]

    covered_sports = {
        _infer_sport_group(r.get("sport") or r.get("competition") or r.get("league") or "")
        for r in s_rows
    }
    merged = list(s_rows)
    seen = {
        (
            str(r.get("game_key") or ""),
            str(r.get("name") or r.get("pick") or "").strip().lower(),
            str(r.get("stat_type") or "").strip().lower(),
            str(r.get("line") if r.get("line") is not None else ""),
            str(r.get("direction") or "").strip().upper(),
        )
        for r in merged
    }

    for row in b_rows:
        sport = _infer_sport_group(row.get("sport") or row.get("competition") or row.get("league") or "")
        if sport in covered_sports:
            continue

        key = (
            str(row.get("game_key") or ""),
            str(row.get("name") or row.get("pick") or "").strip().lower(),
            str(row.get("stat_type") or "").strip().lower(),
            str(row.get("line") if row.get("line") is not None else ""),
            str(row.get("direction") or "").strip().upper(),
        )
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)

    merged.sort(
        key=lambda x: (
            float(x.get("model_prob") or 0.0),
            int(x.get("sentiment_mentions") or 0),
        ),
        reverse=True,
    )
    return merged[:400]


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

    # If social mention volume is low, backfill with model-generated player props.
    fallback_threshold = max(6, min(60, len(target_games)))
    game_sports = {
        _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "")
        for g in target_games
    }
    row_sports = {
        _infer_sport_group(r.get("sport") or r.get("competition") or r.get("league") or "")
        for r in rows
    }
    missing_sports = sorted(s for s in game_sports if s not in {"", "other"} and s not in row_sports)
    should_backfill = len(rows) < fallback_threshold or bool(missing_sports)

    if should_backfill:
        model_rows = _build_model_player_props_fallback(target_games, max_per_game=_ALL_SPORTS_SENTIMENT_PLAYERS_PER_GAME)
        if model_rows and missing_sports:
            model_rows = [
                r for r in model_rows
                if _infer_sport_group(r.get("sport") or r.get("competition") or r.get("league") or "") in set(missing_sports)
            ]
        if model_rows:
            seen = {
                (
                    str(r.get("game_key") or ""),
                    str(r.get("name") or "").strip().lower(),
                    str(r.get("stat_type") or "").strip().lower(),
                )
                for r in rows
            }
            for r in model_rows:
                key = (
                    str(r.get("game_key") or ""),
                    str(r.get("name") or "").strip().lower(),
                    str(r.get("stat_type") or "").strip().lower(),
                )
                if key in seen:
                    continue
                seen.add(key)
                rows.append(r)
            rows.sort(
                key=lambda x: (
                    float(x.get("model_prob") or 0.0),
                    int(x.get("sentiment_mentions") or 0),
                ),
                reverse=True,
            )
    return rows[:400]


def _build_model_player_props_fallback(games: list[dict], max_per_game: int = 6) -> list[dict]:
    """Fallback player props from model/history sources when social mentions are sparse."""
    rows: list[dict] = []
    max_per_game = max(1, min(int(max_per_game or 6), 12))
    today_str = _et_calendar_today().isoformat()
    season = _et_calendar_today().year

    def _prob_to_american(prob: float) -> int:
        p = max(0.01, min(0.99, float(prob or 0.5)))
        if p >= 0.5:
            return int(round(-p / (1.0 - p) * 100))
        return int(round((1.0 - p) / p * 100))

    # Soccer model props (uses squad/market context).
    try:
        from models.soccer_predictor import get_player_props as get_soccer_player_props

        soccer_games = [
            g for g in (games or [])
            if _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "") == "soccer"
        ]
        for g in soccer_games[:36]:
            props = get_soccer_player_props(g) or []
            per_game = []
            for prop in props:
                norm = _normalize_soccer_prop(g, prop, today_str)
                norm["prop_label"] = norm.get("prop_label") or str(norm.get("stat_type") or "soccer_prop").replace("_", " ").title()
                norm.setdefault("sentiment_mentions", int(norm.get("market_mentions") or 0))
                norm.setdefault("sentiment_sources", "soccer_model")
                norm.setdefault("worth_reason", "Soccer model + market popularity")
                per_game.append(norm)
            per_game.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
            rows.extend(per_game[:max_per_game])
    except Exception as e:
        _log(f"[all-sports] soccer player-prop fallback skipped: {e}")

    # Basketball player props from ESPN summary leaders/boxscore (free endpoint).
    try:
        import requests

        basketball_games = [
            g for g in (games or [])
            if _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "") == "basketball"
        ]

        if basketball_games:
            stat_meta = {
                "points": ("points", "Points", 8.5),
                "rebounds": ("rebounds", "Rebounds", 3.5),
                "assists": ("assists", "Assists", 2.5),
            }
            scoreboard_cache: dict[tuple[str, str], list[dict]] = {}

            def _b_num(value):
                if value is None:
                    return None
                if isinstance(value, (int, float)):
                    return float(value)
                s = str(value).strip()
                if not s:
                    return None
                m = re.search(r"-?\d+(?:\.\d+)?", s)
                if not m:
                    return None
                try:
                    return float(m.group(0))
                except Exception:
                    return None

            def _team_token(name: str) -> str:
                return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())

            def _b_poisson_over(rate: float, line: float) -> float:
                lam = max(0.01, float(rate or 0.01))
                target = int(math.floor(float(line or 0.5)) + 1)
                cdf = 0.0
                for k in range(max(0, target)):
                    try:
                        cdf += math.exp(-lam) * (lam ** k) / math.factorial(k)
                    except Exception:
                        pass
                return max(0.05, min(0.95, 1.0 - cdf))

            def _league_slug(game: dict) -> str:
                src_slug = str(game.get("espn_league_path") or "").strip().lower()
                if src_slug:
                    return src_slug
                comp = str(game.get("competition") or "").lower()
                league = str(game.get("league") or game.get("competition_name") or "").lower()
                if "wnba" in comp or "wnba" in league:
                    return "wnba"
                if "womens-college-basketball" in comp or "womens_college_basketball" in comp:
                    return "womens-college-basketball"
                if "mens-college-basketball" in comp or "mens_college_basketball" in comp or "ncaab" in comp:
                    return "mens-college-basketball"
                if "nba" in comp or "nba" in league:
                    return "nba"
                return "nba"

            def _scoreboard_events(slug: str, date_token: str) -> list[dict]:
                key = (slug, date_token)
                if key in scoreboard_cache:
                    return scoreboard_cache[key]
                url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/{slug}/scoreboard"
                try:
                    resp = requests.get(url, params={"dates": date_token, "limit": 200}, timeout=8)
                    if resp.status_code != 200:
                        scoreboard_cache[key] = []
                        return []
                    events = (resp.json() or {}).get("events") or []
                    scoreboard_cache[key] = events
                    return events
                except Exception:
                    scoreboard_cache[key] = []
                    return []

            def _resolve_event_id(game: dict, slug: str) -> str:
                eid = str(game.get("espn_event_id") or game.get("event_id") or "").strip()
                if eid:
                    return eid

                gd = str(game.get("game_date") or game.get("date") or "").strip()
                date_token = gd.replace("-", "") if re.match(r"^\d{4}-\d{2}-\d{2}$", gd) else _et_calendar_today().strftime("%Y%m%d")

                away_tok = _team_token(game.get("away_team") or "")
                home_tok = _team_token(game.get("home_team") or "")
                if not away_tok or not home_tok:
                    return ""

                for ev in _scoreboard_events(slug, date_token):
                    comp = (ev.get("competitions") or [{}])[0]
                    competitors = comp.get("competitors") or []
                    if len(competitors) < 2:
                        continue
                    home_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "home"), competitors[0])
                    away_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "away"), competitors[1] if len(competitors) > 1 else competitors[0])
                    eh = _team_token((home_c.get("team") or {}).get("displayName") or "")
                    ea = _team_token((away_c.get("team") or {}).get("displayName") or "")
                    if eh == home_tok and ea == away_tok:
                        return str(ev.get("id") or "").strip()
                    if eh == away_tok and ea == home_tok:
                        return str(ev.get("id") or "").strip()
                return ""

            def _mk_bball_row(game: dict, team_name: str, player_name: str, stat_name: str, raw_value: float, source: str) -> dict:
                stat_type, stat_label, min_line = stat_meta.get(stat_name, ("points", "Points", 8.5))
                base_rate = max(0.1, float(raw_value or min_line))
                line_val = max(min_line, round(max(min_line, base_rate * 0.88) * 2.0) / 2.0)
                over_prob = _b_poisson_over(base_rate, line_val)
                model_prob = max(0.52, min(0.88, over_prob))
                odds_am = _prob_to_american(model_prob)
                dec_odds = round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4)
                ev = (dec_odds - 1.0) * model_prob - (1.0 - model_prob)

                home = str(game.get("home_team") or "").strip()
                away = str(game.get("away_team") or "").strip()
                game_date = str(game.get("game_date") or game.get("date") or today_str)
                game_time = str(game.get("game_time") or "")
                game_key = str(game.get("game_key") or _compose_game_key(away, home, game.get("game_datetime"), game_date, game_time))
                match_key = _norm_gk(game.get("match_key") or f"{away}@{home}")

                return {
                    "sport": "basketball",
                    "name": player_name,
                    "team": team_name or home,
                    "prop_label": f"Projected {stat_label}",
                    "stat_type": stat_type,
                    "line": line_val,
                    "direction": "OVER",
                    "model_prob": round(model_prob, 4),
                    "confidence": int(round(model_prob * 100)),
                    "safety_label": _safety_label_from_prob(model_prob),
                    "ev": round(ev, 4),
                    "odds_am": odds_am,
                    "dec_odds": dec_odds,
                    "game": f"{away} @ {home}",
                    "game_key": game_key,
                    "match_key": match_key,
                    "game_date": game_date,
                    "game_time": game_time,
                    "home_team": home,
                    "away_team": away,
                    "sentiment_score": 0.0,
                    "sentiment_mentions": 0,
                    "sentiment_sources": source,
                    "worth_it": model_prob >= 0.57,
                    "worth_score": round(model_prob * 100.0, 2),
                    "worth_reason": "ESPN team leaders + boxscore trend",
                }

            for g in basketball_games[:60]:
                home = str(g.get("home_team") or "").strip()
                away = str(g.get("away_team") or "").strip()
                if not home or not away:
                    continue

                league_slug = _league_slug(g)
                event_id = _resolve_event_id(g, league_slug)
                if not event_id:
                    continue

                url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/{league_slug}/summary"
                try:
                    resp = requests.get(url, params={"event": event_id}, timeout=8)
                    if resp.status_code != 200:
                        continue
                    summary = resp.json() or {}
                except Exception:
                    continue

                game_rows: list[dict] = []

                for team_bucket in (summary.get("leaders") or []):
                    team_name = str(((team_bucket.get("team") or {}).get("displayName") or "")).strip()
                    for cat in (team_bucket.get("leaders") or []):
                        stat_name = str(cat.get("name") or "").strip().lower()
                        if stat_name not in stat_meta:
                            continue
                        leader_list = cat.get("leaders") or []
                        if not leader_list:
                            continue
                        lead = leader_list[0] or {}
                        athlete = lead.get("athlete") or {}
                        player_name = str(athlete.get("displayName") or athlete.get("fullName") or "").strip()
                        if not player_name:
                            continue
                        raw_val = _b_num(lead.get("value"))
                        if raw_val is None:
                            raw_val = _b_num(lead.get("displayValue"))
                        if raw_val is None:
                            stats_arr = lead.get("statistics") or []
                            raw_val = _b_num((stats_arr[0] or {}).get("value")) if stats_arr else None
                        if raw_val is None or raw_val <= 0:
                            continue
                        game_rows.append(_mk_bball_row(g, team_name, player_name, stat_name, raw_val, "espn_basketball_leaders"))

                if not game_rows:
                    for team_box in ((summary.get("boxscore") or {}).get("players") or []):
                        team_name = str(((team_box.get("team") or {}).get("displayName") or "")).strip()
                        top_by_stat: dict[str, tuple[str, float]] = {}
                        for stat_block in (team_box.get("statistics") or []):
                            keys = [str(k or "").strip().lower() for k in (stat_block.get("keys") or [])]
                            key_idx = {k: i for i, k in enumerate(keys)}
                            for arow in (stat_block.get("athletes") or []):
                                athlete = arow.get("athlete") or {}
                                player_name = str(athlete.get("displayName") or athlete.get("fullName") or "").strip()
                                if not player_name:
                                    continue
                                vals = arow.get("stats") or []
                                for stat_name in stat_meta:
                                    idx = key_idx.get(stat_name)
                                    if idx is None or idx >= len(vals):
                                        continue
                                    raw_val = _b_num(vals[idx])
                                    if raw_val is None or raw_val <= 0:
                                        continue
                                    prev = top_by_stat.get(stat_name)
                                    if (not prev) or raw_val > prev[1]:
                                        top_by_stat[stat_name] = (player_name, raw_val)

                        for stat_name, payload in top_by_stat.items():
                            game_rows.append(_mk_bball_row(g, team_name, payload[0], stat_name, payload[1], "espn_basketball_boxscore"))

                deduped_game_rows: list[dict] = []
                seen_game = set()
                for row in game_rows:
                    key = (
                        str(row.get("game_key") or ""),
                        str(row.get("name") or "").strip().lower(),
                        str(row.get("stat_type") or "").strip().lower(),
                    )
                    if key in seen_game:
                        continue
                    seen_game.add(key)
                    deduped_game_rows.append(row)

                deduped_game_rows.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
                rows.extend(deduped_game_rows[:max_per_game])
    except Exception as e:
        _log(f"[all-sports] basketball player-prop fallback skipped: {e}")

    # Hockey player props from ESPN summary leaders (free endpoint).
    try:
        import requests

        hockey_games = [
            g for g in (games or [])
            if _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "") == "icehockey"
        ]

        if hockey_games:
            stat_meta = {
                "goals": ("goals", "Goals", 0.5),
                "assists": ("assists", "Assists", 0.5),
                "shots": ("shots_on_goal", "Shots on Goal", 1.5),
                "saves": ("saves", "Saves", 20.5),
            }
            scoreboard_cache: dict[tuple[str, str], list[dict]] = {}

            def _h_num(value):
                if value is None:
                    return None
                if isinstance(value, (int, float)):
                    return float(value)
                s = str(value).strip()
                if not s:
                    return None
                m = re.search(r"-?\d+(?:\.\d+)?", s)
                if not m:
                    return None
                try:
                    return float(m.group(0))
                except Exception:
                    return None

            def _team_token(name: str) -> str:
                return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())

            def _h_poisson_over(rate: float, line: float) -> float:
                lam = max(0.01, float(rate or 0.01))
                target = int(math.floor(float(line or 0.5)) + 1)
                cdf = 0.0
                for k in range(max(0, target)):
                    try:
                        cdf += math.exp(-lam) * (lam ** k) / math.factorial(k)
                    except Exception:
                        pass
                return max(0.05, min(0.95, 1.0 - cdf))

            def _scoreboard_events(slug: str, date_token: str) -> list[dict]:
                key = (slug, date_token)
                if key in scoreboard_cache:
                    return scoreboard_cache[key]
                url = f"https://site.api.espn.com/apis/site/v2/sports/hockey/{slug}/scoreboard"
                try:
                    resp = requests.get(url, params={"dates": date_token, "limit": 200}, timeout=8)
                    if resp.status_code != 200:
                        scoreboard_cache[key] = []
                        return []
                    events = (resp.json() or {}).get("events") or []
                    scoreboard_cache[key] = events
                    return events
                except Exception:
                    scoreboard_cache[key] = []
                    return []

            def _resolve_event_id(game: dict, slug: str) -> str:
                eid = str(game.get("espn_event_id") or game.get("event_id") or "").strip()
                if eid:
                    return eid
                gd = str(game.get("game_date") or game.get("date") or "").strip()
                date_token = gd.replace("-", "") if re.match(r"^\d{4}-\d{2}-\d{2}$", gd) else _et_calendar_today().strftime("%Y%m%d")

                away_tok = _team_token(game.get("away_team") or "")
                home_tok = _team_token(game.get("home_team") or "")
                if not away_tok or not home_tok:
                    return ""

                for ev in _scoreboard_events(slug, date_token):
                    comp = (ev.get("competitions") or [{}])[0]
                    competitors = comp.get("competitors") or []
                    if len(competitors) < 2:
                        continue
                    home_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "home"), competitors[0])
                    away_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "away"), competitors[1] if len(competitors) > 1 else competitors[0])
                    eh = _team_token((home_c.get("team") or {}).get("displayName") or "")
                    ea = _team_token((away_c.get("team") or {}).get("displayName") or "")
                    if eh == home_tok and ea == away_tok:
                        return str(ev.get("id") or "").strip()
                    if eh == away_tok and ea == home_tok:
                        return str(ev.get("id") or "").strip()
                return ""

            def _mk_hockey_row(game: dict, team_name: str, player_name: str, stat_name: str, raw_value: float, source: str) -> dict:
                stat_type, stat_label, min_line = stat_meta.get(stat_name, ("goals", "Goals", 0.5))

                # ESPN leader values can be season totals for pregame cards.
                if stat_name in {"goals", "assists", "shots"}:
                    base_rate = float(raw_value) / 82.0 if raw_value > 8 else float(raw_value)
                elif stat_name == "saves":
                    base_rate = float(raw_value) / 60.0 if raw_value > 70 else float(raw_value)
                else:
                    base_rate = float(raw_value)

                base_rate = max(0.05, base_rate)
                line_val = max(min_line, round(max(min_line, base_rate * 0.9) * 2.0) / 2.0)
                over_prob = _h_poisson_over(base_rate, line_val)
                model_prob = max(0.52, min(0.86, over_prob))
                odds_am = _prob_to_american(model_prob)
                dec_odds = round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4)
                ev = (dec_odds - 1.0) * model_prob - (1.0 - model_prob)

                home = str(game.get("home_team") or "").strip()
                away = str(game.get("away_team") or "").strip()
                game_date = str(game.get("game_date") or game.get("date") or today_str)
                game_time = str(game.get("game_time") or "")
                game_key = str(game.get("game_key") or _compose_game_key(away, home, game.get("game_datetime"), game_date, game_time))
                match_key = _norm_gk(game.get("match_key") or f"{away}@{home}")

                return {
                    "sport": "icehockey",
                    "name": player_name,
                    "team": team_name or home,
                    "prop_label": f"Projected {stat_label}",
                    "stat_type": stat_type,
                    "line": line_val,
                    "direction": "OVER",
                    "model_prob": round(model_prob, 4),
                    "confidence": int(round(model_prob * 100)),
                    "safety_label": _safety_label_from_prob(model_prob),
                    "ev": round(ev, 4),
                    "odds_am": odds_am,
                    "dec_odds": dec_odds,
                    "game": f"{away} @ {home}",
                    "game_key": game_key,
                    "match_key": match_key,
                    "game_date": game_date,
                    "game_time": game_time,
                    "home_team": home,
                    "away_team": away,
                    "sentiment_score": 0.0,
                    "sentiment_mentions": 0,
                    "sentiment_sources": source,
                    "worth_it": model_prob >= 0.57,
                    "worth_score": round(model_prob * 100.0, 2),
                    "worth_reason": "ESPN hockey leaders trend",
                }

            for g in hockey_games[:50]:
                home = str(g.get("home_team") or "").strip()
                away = str(g.get("away_team") or "").strip()
                if not home or not away:
                    continue

                league_slug = str(g.get("espn_league_path") or "nhl").strip().lower() or "nhl"
                event_id = _resolve_event_id(g, league_slug)
                if not event_id:
                    continue

                url = f"https://site.api.espn.com/apis/site/v2/sports/hockey/{league_slug}/summary"
                try:
                    resp = requests.get(url, params={"event": event_id}, timeout=8)
                    if resp.status_code != 200:
                        continue
                    summary = resp.json() or {}
                except Exception:
                    continue

                game_rows: list[dict] = []
                for team_bucket in (summary.get("leaders") or []):
                    team_name = str(((team_bucket.get("team") or {}).get("displayName") or "")).strip()
                    for cat in (team_bucket.get("leaders") or []):
                        stat_name = str(cat.get("name") or "").strip().lower()
                        if stat_name not in stat_meta:
                            continue
                        leader_list = cat.get("leaders") or []
                        if not leader_list:
                            continue
                        lead = leader_list[0] or {}
                        athlete = lead.get("athlete") or {}
                        player_name = str(athlete.get("displayName") or athlete.get("fullName") or "").strip()
                        if not player_name:
                            continue
                        raw_val = _h_num(lead.get("value"))
                        if raw_val is None:
                            raw_val = _h_num(lead.get("displayValue"))
                        if raw_val is None or raw_val <= 0:
                            continue
                        game_rows.append(_mk_hockey_row(g, team_name, player_name, stat_name, raw_val, "espn_hockey_leaders"))

                deduped_game_rows: list[dict] = []
                seen_game = set()
                for row in game_rows:
                    key = (
                        str(row.get("game_key") or ""),
                        str(row.get("name") or "").strip().lower(),
                        str(row.get("stat_type") or "").strip().lower(),
                    )
                    if key in seen_game:
                        continue
                    seen_game.add(key)
                    deduped_game_rows.append(row)

                deduped_game_rows.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
                rows.extend(deduped_game_rows[:max_per_game])
    except Exception as e:
        _log(f"[all-sports] hockey player-prop fallback skipped: {e}")

    # MLB historical hitter props across recent seasons.
    try:
        from data import mlb_fetcher as _mlb_fetcher

        mlb_games = [
            g for g in (games or [])
            if _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "") in {"baseball", "mlb"}
        ]
        if mlb_games and getattr(_mlb_fetcher, "PYBASEBALL_OK", False):
            by_match = {}
            batch_games = []
            for g in mlb_games[:40]:
                home = str(g.get("home_team") or "").strip()
                away = str(g.get("away_team") or "").strip()
                if not home or not away:
                    continue
                match_key = _norm_gk(g.get("match_key") or f"{away}@{home}")
                by_match[match_key] = g
                batch_games.append({
                    "home_team": home,
                    "away_team": away,
                    "game_time": g.get("game_time") or "",
                    "date": g.get("game_date") or g.get("date") or today_str,
                    "game_date": g.get("game_date") or g.get("date") or today_str,
                    "game_datetime": g.get("game_datetime") or "",
                    "match_key": match_key,
                })

            raw = _mlb_fetcher.get_hitter_props_batch(batch_games, season=season) or []
            per_game: dict[str, list[dict]] = {}
            for p in raw:
                game_txt = str(p.get("game") or "").strip()
                mk = _norm_gk(game_txt.replace(" @ ", "@")) if game_txt else ""
                g = by_match.get(mk)
                if not g:
                    continue

                over_p = float(p.get("over_prob") or 0.5)
                under_p = float(p.get("under_prob") or (1.0 - over_p))
                direction = "OVER" if over_p >= under_p else "UNDER"
                model_prob = max(over_p, under_p)
                model_prob = max(0.01, min(0.99, model_prob))
                odds_am = _prob_to_american(model_prob)
                dec_odds = round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4)
                ev = (dec_odds - 1.0) * model_prob - (1.0 - model_prob)

                home = str(g.get("home_team") or "").strip()
                away = str(g.get("away_team") or "").strip()
                game_date = str(g.get("game_date") or g.get("date") or today_str)
                game_time = str(g.get("game_time") or "")
                game_key = str(g.get("game_key") or _compose_game_key(away, home, g.get("game_datetime"), game_date, game_time))
                stat_type = str(p.get("stat_type") or "hits")

                row = {
                    "sport": "baseball",
                    "name": p.get("name"),
                    "team": p.get("team") or home,
                    "prop_label": f"Historical {stat_type.replace('_', ' ').title()}",
                    "stat_type": stat_type,
                    "line": p.get("line"),
                    "direction": direction,
                    "model_prob": round(model_prob, 4),
                    "confidence": int(round(model_prob * 100)),
                    "safety_label": _safety_label_from_prob(model_prob),
                    "ev": round(ev, 4),
                    "odds_am": odds_am,
                    "dec_odds": dec_odds,
                    "game": f"{away} @ {home}",
                    "game_key": game_key,
                    "match_key": mk,
                    "game_date": game_date,
                    "game_time": game_time,
                    "home_team": home,
                    "away_team": away,
                    "sentiment_score": 0.0,
                    "sentiment_mentions": 0,
                    "sentiment_sources": "mlb_historical_model",
                    "worth_it": model_prob >= 0.57,
                    "worth_score": round(model_prob * 100.0, 2),
                    "worth_reason": "Historical multi-season batter profile",
                }
                per_game.setdefault(game_key, []).append(row)

            for gk, arr in per_game.items():
                arr.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
                rows.extend(arr[:max_per_game])

        existing_mlb_rows = [r for r in rows if _infer_sport_group(r.get("sport") or "") in {"baseball", "mlb"}]
        if mlb_games and len(existing_mlb_rows) < max(8, len(mlb_games)):
            try:
                from data.sportsdata_fetcher import get_mlb_player_season_stats, get_mlb_teams

                def _norm_team_name(name: str) -> str:
                    return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())

                def _poisson_over_prob(rate: float, line: float) -> float:
                    lam = max(0.01, float(rate or 0.01))
                    target = int(math.floor(float(line or 0.5)) + 1)
                    cdf = 0.0
                    for k in range(max(0, target)):
                        try:
                            cdf += math.exp(-lam) * (lam ** k) / math.factorial(k)
                        except Exception:
                            pass
                    return max(0.05, min(0.95, 1.0 - cdf))

                team_rows = get_mlb_teams() or []
                alias_to_key: dict[str, str] = {}
                for t in team_rows:
                    key = str(t.get("Key") or "").strip().upper()
                    city = str(t.get("City") or "").strip()
                    name = str(t.get("Name") or "").strip()
                    full = f"{city} {name}".strip()
                    for alias in (full, name):
                        a = _norm_team_name(alias)
                        if a and key:
                            alias_to_key[a] = key

                def _resolve_team_key(team_name: str) -> str:
                    n = _norm_team_name(team_name)
                    if not n:
                        return ""
                    if n in alias_to_key:
                        return alias_to_key[n]
                    for alias, key in alias_to_key.items():
                        if n and (n in alias or alias in n):
                            return key
                    return ""

                player_stats = get_mlb_player_season_stats(season=season) or []
                if not player_stats:
                    player_stats = get_mlb_player_season_stats() or []

                by_team: dict[str, list[dict]] = {}
                for p in player_stats:
                    tkey = str(p.get("Team") or "").strip().upper()
                    if not tkey:
                        continue
                    by_team.setdefault(tkey, []).append(p)

                def _mk_prop_row(game: dict, team_name: str, player_name: str, stat_type: str, prop_label: str,
                                 line_val: float, over_prob: float, direction: str, source: str) -> dict:
                    model_prob = over_prob if direction == "OVER" else (1.0 - over_prob)
                    model_prob = max(0.01, min(0.99, model_prob))
                    odds_am = _prob_to_american(model_prob)
                    dec_odds = round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4)
                    ev = (dec_odds - 1.0) * model_prob - (1.0 - model_prob)
                    home = str(game.get("home_team") or "").strip()
                    away = str(game.get("away_team") or "").strip()
                    game_date = str(game.get("game_date") or game.get("date") or today_str)
                    game_time = str(game.get("game_time") or "")
                    game_key = str(game.get("game_key") or _compose_game_key(away, home, game.get("game_datetime"), game_date, game_time))
                    match_key = _norm_gk(game.get("match_key") or f"{away}@{home}")
                    return {
                        "sport": "baseball",
                        "name": player_name,
                        "team": team_name,
                        "prop_label": prop_label,
                        "stat_type": stat_type,
                        "line": line_val,
                        "direction": direction,
                        "model_prob": round(model_prob, 4),
                        "confidence": int(round(model_prob * 100)),
                        "safety_label": _safety_label_from_prob(model_prob),
                        "ev": round(ev, 4),
                        "odds_am": odds_am,
                        "dec_odds": dec_odds,
                        "game": f"{away} @ {home}",
                        "game_key": game_key,
                        "match_key": match_key,
                        "game_date": game_date,
                        "game_time": game_time,
                        "home_team": home,
                        "away_team": away,
                        "sentiment_score": 0.0,
                        "sentiment_mentions": 0,
                        "sentiment_sources": source,
                        "worth_it": model_prob >= 0.57,
                        "worth_score": round(model_prob * 100.0, 2),
                        "worth_reason": "Historical MLB season profile",
                    }

                for g in mlb_games[:40]:
                    home = str(g.get("home_team") or "").strip()
                    away = str(g.get("away_team") or "").strip()
                    if not home or not away:
                        continue
                    hk = _resolve_team_key(home)
                    ak = _resolve_team_key(away)
                    game_rows: list[dict] = []

                    for team_name, tkey in ((home, hk), (away, ak)):
                        if not tkey:
                            continue
                        candidates = by_team.get(tkey, [])
                        if not candidates:
                            continue

                        hitters = [p for p in candidates if str(p.get("PositionCategory") or "").upper() != "P"]
                        pitchers = [p for p in candidates if str(p.get("PositionCategory") or "").upper() == "P"]

                        hitters.sort(key=lambda x: float(x.get("FantasyPoints") or 0.0), reverse=True)
                        for p in hitters[:3]:
                            pname = str(p.get("Name") or "").strip()
                            games_played = float(p.get("Games") or 0.0)
                            if not pname or games_played < 5:
                                continue
                            stats = [
                                ("hits", "Historical Hits", float(p.get("Hits") or 0.0), 0.5),
                                ("home_runs", "Historical Home Runs", float(p.get("HomeRuns") or 0.0), 0.5),
                                ("rbi", "Historical RBI", float(p.get("RunsBattedIn") or 0.0), 0.5),
                                ("runs", "Historical Runs", float(p.get("Runs") or 0.0), 0.5),
                                ("stolen_bases", "Historical Stolen Bases", float(p.get("StolenBases") or 0.0), 0.5),
                                ("total_bases", "Historical Total Bases", float(p.get("TotalBases") or 0.0), 1.5),
                            ]
                            best_prop = None
                            best_prob = 0.0
                            for stat_type, label, total_val, line_val in stats:
                                if total_val <= 0:
                                    continue
                                rate = total_val / max(games_played, 1.0)
                                over_prob = _poisson_over_prob(rate, line_val)
                                if over_prob >= 0.52 and over_prob > best_prob:
                                    best_prob = over_prob
                                    best_prop = (stat_type, label, line_val, over_prob)
                            if best_prop:
                                game_rows.append(
                                    _mk_prop_row(
                                        g,
                                        team_name,
                                        pname,
                                        best_prop[0],
                                        best_prop[1],
                                        best_prop[2],
                                        best_prop[3],
                                        "OVER",
                                        "mlb_sportsdata_historical",
                                    )
                                )

                        pitchers.sort(key=lambda x: float(x.get("PitchingStrikeouts") or 0.0), reverse=True)
                        for p in pitchers[:1]:
                            pname = str(p.get("Name") or "").strip()
                            games_played = float(p.get("Games") or p.get("Started") or 0.0)
                            strikeouts = float(p.get("PitchingStrikeouts") or 0.0)
                            if not pname or games_played < 3 or strikeouts <= 0:
                                continue
                            k_rate = strikeouts / max(games_played, 1.0)
                            line_val = max(3.5, round(k_rate * 0.85 * 2.0) / 2.0)
                            over_prob = _poisson_over_prob(k_rate, line_val)
                            if over_prob >= 0.52:
                                game_rows.append(
                                    _mk_prop_row(
                                        g,
                                        team_name,
                                        pname,
                                        "strikeouts",
                                        "Historical Pitcher Strikeouts",
                                        line_val,
                                        over_prob,
                                        "OVER",
                                        "mlb_sportsdata_historical",
                                    )
                                )

                    game_rows.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
                    rows.extend(game_rows[:max_per_game])
            except Exception as se:
                _log(f"[all-sports] sportsdata MLB fallback skipped: {se}")
        elif mlb_games:
            _log("[all-sports] pybaseball unavailable — skipping bulk hitter fallback")
    except Exception as e:
        _log(f"[all-sports] mlb player-prop fallback skipped: {e}")

    # Last-resort MLB starter props (does not require pybaseball dependencies).
    if not rows:
        try:
            from data.sentiment import get_player_prop_signal

            starter_rows: list[dict] = []
            for g in (games or []):
                sport = _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "")
                if sport not in {"baseball", "mlb"}:
                    continue
                home = str(g.get("home_team") or "").strip()
                away = str(g.get("away_team") or "").strip()
                game_date = str(g.get("game_date") or g.get("date") or today_str)
                game_time = str(g.get("game_time") or "")
                game_key = str(g.get("game_key") or _compose_game_key(away, home, g.get("game_datetime"), game_date, game_time))
                match_key = _norm_gk(g.get("match_key") or f"{away}@{home}")

                starters = [
                    (str(g.get("home_starter") or "").strip(), home),
                    (str(g.get("away_starter") or "").strip(), away),
                ]
                for starter_name, team in starters:
                    if not starter_name or starter_name.upper() == "TBD":
                        continue

                    line = 4.5
                    direction = "OVER"
                    chosen_prob = 0.55
                    sent_score = 0.0
                    reason = "Starter historical + sentiment strikeout profile"

                    try:
                        signal = get_player_prop_signal(starter_name, "strikeouts", line)
                        over_prob = float(signal.get("probability") or 0.5)
                        direction = str(signal.get("direction") or "OVER").upper()
                        chosen_prob = over_prob if direction == "OVER" else (1.0 - over_prob)
                        sent_score = float(signal.get("sentiment_score") or 0.0)
                        if signal.get("rationale"):
                            reason = str(signal.get("rationale"))[:220]
                    except Exception:
                        pass

                    chosen_prob = max(0.51, min(0.88, float(chosen_prob)))
                    odds_am = _prob_to_american(chosen_prob)
                    dec_odds = round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4)
                    ev = (dec_odds - 1.0) * chosen_prob - (1.0 - chosen_prob)

                    starter_rows.append({
                        "sport": "baseball",
                        "name": starter_name,
                        "team": team,
                        "prop_label": "Pitcher Strikeouts",
                        "stat_type": "strikeouts",
                        "line": line,
                        "direction": direction,
                        "model_prob": round(chosen_prob, 4),
                        "confidence": int(round(chosen_prob * 100)),
                        "safety_label": _safety_label_from_prob(chosen_prob),
                        "ev": round(ev, 4),
                        "odds_am": odds_am,
                        "dec_odds": dec_odds,
                        "game": f"{away} @ {home}",
                        "game_key": game_key,
                        "match_key": match_key,
                        "game_date": game_date,
                        "game_time": game_time,
                        "home_team": home,
                        "away_team": away,
                        "sentiment_score": round(sent_score, 4),
                        "sentiment_mentions": 0,
                        "sentiment_sources": "historical_trends,sentiment",
                        "worth_it": chosen_prob >= 0.56,
                        "worth_score": round(chosen_prob * 100.0, 2),
                        "worth_reason": reason,
                    })

            starter_rows.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
            rows.extend(starter_rows)
        except Exception as e:
            _log(f"[all-sports] starter-prop fallback skipped: {e}")

    deduped: list[dict] = []
    seen = set()
    for r in rows:
        key = (
            str(r.get("game_key") or ""),
            str(r.get("name") or "").strip().lower(),
            str(r.get("stat_type") or "").strip().lower(),
            str(r.get("line") if r.get("line") is not None else ""),
            str(r.get("direction") or "").strip().upper(),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    deduped.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
    return deduped[:350]


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
        table_rows = _merge_all_sports_table_rows(sentiment_prop_rows, best_bet_rows)
        _attach_tracking_uids(bets, table_rows)
        card_prop_rows = sentiment_prop_rows if sentiment_prop_rows else []
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
        today_cards = [_build_card(g, bets, card_prop_rows, "TODAY") for g in today_games]
        tomorrow_cards = [_build_card(g, bets, card_prop_rows, "TOMORROW") for g in tomorrow_games]

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

        _attach_tracking_uids(all_bets, all_props)

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

        _attach_tracking_uids(all_bets, all_props)

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
            fallback_player_props = _build_all_sport_sentiment_props(all_games, all_bets)
            fallback_best_bets = _multi_sport_best_bets_rows(all_bets)
            fallback_props = _merge_all_sports_table_rows(fallback_player_props, fallback_best_bets)
            fallback_today = [_build_card(g, all_bets, fallback_player_props, "TODAY") for g in today_games]
            fallback_tomorrow = [_build_card(g, all_bets, fallback_player_props, "TOMORROW") for g in tomorrow_games]
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
    try:
        # First run the universal resolver so game/prop outcomes are up-to-date
        result   = _resolve_all_sports_outcomes(days_back=21)
        n_parlay = result.get("parlays", 0)
        n_other  = result.get("games", 0) + result.get("props", 0)
        return jsonify({
            "ok":      True,
            "resolved": n_parlay,
            "msg":     f"Resolved {n_parlay} parlay(s) + {n_other} game/prop bet(s)",
        })
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
        sport_filter = _infer_sport_group(str(request.args.get("sport", "all") or "all").strip())
        if sport_filter in {"", "other", "all"}:
            sport_filter = "all"
        query = str(request.args.get("q", "") or "").strip().lower()
        status_filter = str(request.args.get("status", "all") or "all").strip().lower()
        try:
            snap = _build_multi_sport_snapshot(force_refresh=bool(request.args.get("refresh")))
            tournaments = {str(t.get("code") or ""): t for t in (snap.get("tournaments") or [])}
            if code and code not in tournaments:
                return jsonify({"ok": False, "error": f"Unsupported sport code: {code}", "matches": [], "standings": [], "top_scorers": []}), 404

            matches = []
            for m in (snap.get("games") or []):
                if code and str(m.get("competition") or "") != code:
                    continue
                ms = _infer_sport_group(m.get("sport") or m.get("competition") or m.get("league") or "")
                if sport_filter != "all" and ms != sport_filter:
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

            match_keys = {
                _norm_gk(str(m.get("match_key") or f"{m.get('away_team','')}@{m.get('home_team','')}"))
                for m in matches
            }
            team_map = {}
            for m in matches:
                for t in (m.get("home_team"), m.get("away_team")):
                    token = re.sub(r"[^a-z0-9]+", "", str(t or "").strip().lower())
                    if token:
                        team_map[token] = str(t or "").strip()

            def _resolve_team_name(name: str) -> str:
                token = re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())
                if not token:
                    return ""
                if token in team_map:
                    return team_map[token]
                for tk, label in team_map.items():
                    if token in tk or tk in token:
                        return label
                return ""

            selected_bets = []
            if match_keys:
                for b in (snap.get("bets") or []):
                    bm = _norm_gk(str(b.get("match_key") or f"{b.get('away_team','')}@{b.get('home_team','')}"))
                    if bm not in match_keys:
                        continue
                    selected_bets.append(b)

            with _lock:
                state_props = list(_state.get("player_props") or [])
            selected_props = []
            if match_keys:
                for p in state_props:
                    pm = _norm_gk(str(p.get("match_key") or ""))
                    if not pm:
                        pm = _norm_gk(str(p.get("game") or "").replace(" @ ", "@"))
                    if pm not in match_keys:
                        continue
                    ps = _infer_sport_group(p.get("sport") or p.get("competition") or p.get("league") or "")
                    if sport_filter != "all" and ps != sport_filter:
                        continue
                    selected_props.append(p)

            team_stats: dict[str, dict] = {}
            for m in matches:
                for label in (str(m.get("home_team") or "").strip(), str(m.get("away_team") or "").strip()):
                    if not label:
                        continue
                    team_stats.setdefault(label, {
                        "team": label,
                        "games": 0,
                        "team_picks": 0,
                        "against_picks": 0,
                        "prop_count": 0,
                        "prob_sum": 0.0,
                        "prob_n": 0,
                        "markets": {},
                    })
                    team_stats[label]["games"] += 1

            for b in selected_bets:
                home = str(b.get("home_team") or "").strip()
                away = str(b.get("away_team") or "").strip()
                pick = str(b.get("pick") or "").strip().lower()
                bet_type = str(b.get("bet_type") or "moneyline").strip().lower()
                prob = float(b.get("model_prob") or 0.0)

                pick_team = ""
                if home and home.lower() in pick:
                    pick_team = home
                elif away and away.lower() in pick:
                    pick_team = away
                elif "home" in pick and home:
                    pick_team = home
                elif "away" in pick and away:
                    pick_team = away

                if pick_team and pick_team in team_stats:
                    row = team_stats[pick_team]
                    row["team_picks"] += 1
                    row["prob_sum"] += prob
                    row["prob_n"] += 1
                    row["markets"][bet_type] = row["markets"].get(bet_type, 0) + 1
                    opp = away if pick_team == home else home
                    if opp in team_stats:
                        team_stats[opp]["against_picks"] += 1

            for p in selected_props:
                team_label = _resolve_team_name(p.get("team") or "")
                if team_label and team_label in team_stats:
                    team_stats[team_label]["prop_count"] += 1

            team_rows = []
            for row in team_stats.values():
                avg_prob = (row["prob_sum"] / row["prob_n"]) if row["prob_n"] else 0.0
                markets = row.get("markets") or {}
                top_market = max(markets.items(), key=lambda kv: kv[1])[0] if markets else "—"
                team_rows.append({
                    "team": row["team"],
                    "games": int(row["games"]),
                    "team_picks": int(row["team_picks"]),
                    "against_picks": int(row["against_picks"]),
                    "prop_count": int(row["prop_count"]),
                    "avg_model": round(avg_prob * 100.0, 1),
                    "top_market": top_market.replace("_", " "),
                })

            team_rows.sort(key=lambda x: (x.get("team_picks", 0), x.get("prop_count", 0), x.get("avg_model", 0.0)), reverse=True)

            market_stats: dict[str, dict] = {}
            for b in selected_bets:
                market = str(b.get("bet_type") or "moneyline").strip().lower() or "moneyline"
                entry = market_stats.setdefault(market, {"count": 0, "prob_sum": 0.0, "best_prob": 0.0, "best_pick": ""})
                prob = float(b.get("model_prob") or 0.0)
                entry["count"] += 1
                entry["prob_sum"] += prob
                if prob > entry["best_prob"]:
                    entry["best_prob"] = prob
                    entry["best_pick"] = str(b.get("pick") or "")

            for p in selected_props:
                market = str(p.get("stat_type") or "player_prop").strip().lower() or "player_prop"
                entry = market_stats.setdefault(market, {"count": 0, "prob_sum": 0.0, "best_prob": 0.0, "best_pick": ""})
                prob = float(p.get("model_prob") or 0.0)
                entry["count"] += 1
                entry["prob_sum"] += prob
                if prob > entry["best_prob"]:
                    entry["best_prob"] = prob
                    entry["best_pick"] = f"{p.get('name','')} {str(p.get('direction') or '').upper()} {p.get('line')}"

            market_rows = []
            for name, stat in market_stats.items():
                cnt = int(stat.get("count") or 0)
                avg_prob = (float(stat.get("prob_sum") or 0.0) / cnt) if cnt else 0.0
                market_rows.append({
                    "name": name.replace("_", " "),
                    "count": cnt,
                    "avg_model": round(avg_prob * 100.0, 1),
                    "best_pick": str(stat.get("best_pick") or ""),
                    "best_model": round(float(stat.get("best_prob") or 0.0) * 100.0, 1),
                })
            market_rows.sort(key=lambda x: (x.get("count", 0), x.get("avg_model", 0.0)), reverse=True)

            return jsonify({
                "ok": True,
                "code": code,
                "sport": sport_filter,
                "competition": tournaments.get(code, {"code": code, "name": code or "All Sports"}),
                "matches": matches,
                "standings": [{"group": "Team Bet Analysis", "mode": "team_bets", "table": team_rows}],
                "top_scorers": market_rows,
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


# ─── Universal all-sport outcome resolver ────────────────────────────────────
_SOCCER_STAT_MAP = {
    "goals": "goals",
    "assists": "assists",
    "shotsontarget": "shots_on_target",
    "shotsongoal": "shots_on_target",
    "keypasses": "key_passes",
    "tackles": "tackles",
    "saves": "saves",
}


_ESPN_RESOLVE_CONFIGS = [
    # (espn_sport_path, espn_league_path, sport_label, boxscore_stat_map)
    ("basketball", "nba",                       "basketball",      {"points": "points", "rebounds": "totalRebounds", "assists": "assists", "steals": "steals", "blocks": "blocks", "threePointFieldGoalsMade": "threes"}),
    ("basketball", "wnba",                      "basketball",      {"points": "points", "rebounds": "totalRebounds", "assists": "assists"}),
    ("hockey",     "nhl",                       "icehockey",       {"goals": "goals", "assists": "assists", "shots": "shots_on_goal", "saves": "saves"}),
    ("football",   "nfl",                       "americanfootball", {"passingYards": "passing_yards", "rushingYards": "rushing_yards", "receivingYards": "receiving_yards", "touchdowns": "touchdowns", "receptions": "receptions"}),
    ("baseball",   "mlb",                       "baseball",        {"hits": "hits", "homeRuns": "home_runs", "rbi": "rbi", "strikeouts": "strikeouts"}),
    ("soccer",     "usa.1",                     "soccer",          _SOCCER_STAT_MAP),
    ("soccer",     "eng.1",                     "soccer",          _SOCCER_STAT_MAP),
    ("soccer",     "esp.1",                     "soccer",          _SOCCER_STAT_MAP),
    ("soccer",     "ger.1",                     "soccer",          _SOCCER_STAT_MAP),
]


def _resolve_all_sports_outcomes(days_back: int = 3) -> dict:
    """
    Universal outcome resolver.  For each sport/day with PENDING bets:
      1. Fetches completed ESPN scoreboards.
      2. Resolves pending game predictions (moneyline / spread / total).
      3. Resolves pending player props using ESPN boxscore stats.
      4. Resolves pending tracked parlays once all their legs are settled.
    Works in 'all' mode and every single-sport mode.
    Returns {"games": N, "props": N, "parlays": N}.
    """
    import requests as _req
    try:
        from data.db import (
            get_conn, get_pending_props, update_prop_outcome,
            get_tracked_parlays, resolve_tracked_parlay,
        )
        import psycopg2.extras as _dba
    except Exception:
        return {"games": 0, "props": 0, "parlays": 0}

    today        = _et_calendar_today()
    n_games      = 0
    n_props      = 0
    n_parlays    = 0

    def _num(v):
        if v is None:
            return None
        try:
            return float(str(v).strip().replace(",", ""))
        except Exception:
            return None

    def _teams_match(espn_name: str, pick_fragment: str) -> bool:
        en = espn_name.lower()
        pf = pick_fragment.lower()
        # direct substring or any two-char+ word overlap
        if en in pf or pf in en:
            return True
        for word in en.split():
            if len(word) > 2 and word in pf:
                return True
        return False

    def _stat_key_norm(v: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(v or "").lower())

    def _prop_type_aliases(v: str):
        compact = _stat_key_norm(v)
        base = re.sub(r"[^a-z0-9]+", "_", str(v or "").lower()).strip("_")
        aliases = {base, compact}
        alias_map = {
            "keypasses": {"key_passes", "keypasses"},
            "shotsontarget": {"shots_on_target", "shotsontarget", "shots_on_goal", "shotsongoal"},
            "shotsongoal": {"shots_on_target", "shots_on_goal", "shotsontarget", "shotsongoal"},
            "tackles": {"tackles", "tackles_won", "tackleswon"},
            "saves": {"saves"},
            "goals": {"goals"},
            "assists": {"assists"},
            "strikeouts": {"strikeouts"},
        }
        aliases.update(alias_map.get(compact, set()))
        return [a for a in aliases if a]

    # ── 1. Collect all ESPN completed games grouped by (sport_path, league_path) ──
    # Prefer only dates that still have pending records to keep resolver fast while
    # still allowing wider backfill windows.
    completed_games_by_config = {}
    pending_dates = set()
    conn_dates = get_conn()
    if conn_dates:
        try:
            cdates = conn_dates.cursor()
            cdates.execute("""
                SELECT DISTINCT game_date::date
                FROM predictions
                WHERE outcome = 'PENDING'
                  AND game_date >= CURRENT_DATE - INTERVAL '%s days'
                  AND game_date <= CURRENT_DATE
            """ % int(days_back + 1))
            for row in (cdates.fetchall() or []):
                d = row[0] if row else None
                if d:
                    pending_dates.add(d)

            cdates.execute("""
                SELECT DISTINCT game_date::date
                FROM prop_history
                WHERE outcome = 'PENDING'
                  AND game_date >= CURRENT_DATE - INTERVAL '%s days'
                  AND game_date <= CURRENT_DATE
            """ % int(days_back + 1))
            for row in (cdates.fetchall() or []):
                d = row[0] if row else None
                if d:
                    pending_dates.add(d)
        except Exception:
            pass
        finally:
            try:
                conn_dates.close()
            except Exception:
                pass

    pending_dates.add(today)

    if pending_dates:
        check_dates = sorted(pending_dates, reverse=True)
    else:
        # Include today (days_ago=0) so same-day finals resolve immediately.
        check_dates = [today - datetime.timedelta(days=days_ago) for days_ago in range(0, days_back + 1)]

    for check_date in check_dates:
        dates_token = check_date.strftime("%Y%m%d")
        for cfg in _ESPN_RESOLVE_CONFIGS:
            sport_path, league_path, sport_group, _stat_map = cfg
            url = f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/{league_path}/scoreboard"
            try:
                resp = _req.get(url, params={"dates": dates_token, "limit": 200}, timeout=8)
                if resp.status_code != 200:
                    continue
                data = resp.json() or {}
            except Exception:
                continue
            for ev in (data.get("events") or []):
                comp   = (ev.get("competitions") or [{}])[0]
                status = str(((comp.get("status") or {}).get("type") or {}).get("name") or "").lower()
                if not any(k in status for k in ("final", "complete", "finished", "ended", "postgame")):
                    continue
                competitors = comp.get("competitors") or []
                if len(competitors) < 2:
                    continue
                home_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "home"), competitors[0])
                away_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "away"), competitors[1] if len(competitors) > 1 else competitors[0])
                home   = str(((home_c.get("team") or {}).get("displayName") or "")).strip()
                away   = str(((away_c.get("team") or {}).get("displayName") or "")).strip()
                h_sc   = _num(home_c.get("score"))
                a_sc   = _num(away_c.get("score"))
                if not home or not away or h_sc is None or a_sc is None:
                    continue
                key = (sport_path, league_path)
                completed_games_by_config.setdefault(key, []).append({
                    "event_id":   str(ev.get("id") or ""),
                    "sport_path": sport_path,
                    "league_path": league_path,
                    "sport_group": sport_group,
                    "home":       home,
                    "away":       away,
                    "home_score": h_sc,
                    "away_score": a_sc,
                    "total":      h_sc + a_sc,
                    "game_date":  check_date.isoformat(),
                    "stat_map":   _stat_map,
                })

    all_completed = [g for games in completed_games_by_config.values() for g in games]

    # ── 2. Resolve pending game predictions ───────────────────────────────────
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor(cursor_factory=_dba.RealDictCursor)
            cur.execute("""
                SELECT id, sport, bet_type, pick, line, game_key, game_date::text
                FROM predictions
                WHERE outcome = 'PENDING'
                  AND game_date >= CURRENT_DATE - INTERVAL '%s days'
                                    AND game_date <= CURRENT_DATE
            """ % int(days_back + 1))
            pending_preds = cur.fetchall()

            for pred in pending_preds:
                pick     = str(pred.get("pick") or "")
                line_val = _num(pred.get("line")) or 0.0
                bet_type = str(pred.get("bet_type") or "").lower()
                game_key = str(pred.get("game_key") or "")
                pred_sport = str(pred.get("sport") or "").lower()

                for g in all_completed:
                    sport_group = g["sport_group"]
                    # sport filter
                    if pred_sport and pred_sport not in (sport_group, "all", ""):
                        if pred_sport == "mlb" and sport_group != "baseball":
                            continue
                        if pred_sport not in ("all", ""):
                            if pred_sport == "basketball" and sport_group != "basketball":
                                continue
                            if pred_sport == "icehockey" and sport_group != "icehockey":
                                continue
                            if pred_sport == "soccer" and sport_group != "soccer":
                                continue

                    home, away = g["home"], g["away"]
                    h_sc, a_sc = g["home_score"], g["away_score"]
                    total = g["total"]

                    # Verify this game matches the prediction's game_key
                    if game_key and home not in game_key and away not in game_key:
                        if not (any(w in game_key.lower() for w in home.lower().split() if len(w) > 3) or
                                any(w in game_key.lower() for w in away.lower().split() if len(w) > 3)):
                            continue

                    outcome = None
                    result_str = f"{away} {int(a_sc)} @ {home} {int(h_sc)}"

                    if "moneyline" in bet_type or bet_type == "money_line":
                        winner = home if h_sc > a_sc else away
                        if h_sc == a_sc:
                            outcome = "PUSH"
                        elif _teams_match(winner, pick):
                            outcome = "WIN"
                        else:
                            outcome = "LOSS"

                    elif bet_type in ("run_line", "puck_line", "spread", "point_spread"):
                        margin = h_sc - a_sc
                        # '+' / '-' convention in pick
                        if f"-1.5" in pick or f"-2.5" in pick:
                            fav_margin = float(next((p for p in pick.split() if p.startswith("-")), "-1.5"))
                            fav_team   = pick.split()[0] if pick else ""
                            if _teams_match(home, fav_team):
                                outcome = "WIN" if margin > abs(fav_margin) else "LOSS"
                            else:
                                outcome = "WIN" if -margin > abs(fav_margin) else "LOSS"
                        elif "+1.5" in pick or "+2.5" in pick:
                            dog_margin = float(next((p for p in pick.split() if p.startswith("+")), "+1.5"))
                            dog_team   = pick.split()[0] if pick else ""
                            if _teams_match(home, dog_team):
                                outcome = "WIN" if margin > -abs(dog_margin) else "LOSS"
                            else:
                                outcome = "WIN" if -margin > -abs(dog_margin) else "LOSS"

                    elif bet_type in ("total", "f5_total", "game_total"):
                        if "OVER" in pick.upper():
                            if total > line_val:
                                outcome = "WIN"
                            elif total == line_val:
                                outcome = "PUSH"
                            else:
                                outcome = "LOSS"
                        elif "UNDER" in pick.upper():
                            if total < line_val:
                                outcome = "WIN"
                            elif total == line_val:
                                outcome = "PUSH"
                            else:
                                outcome = "LOSS"

                    if outcome:
                        try:
                            cur.execute("""
                                UPDATE predictions
                                SET outcome = %s, actual_result = %s, resolved_at = NOW()
                                WHERE id = %s AND outcome = 'PENDING'
                            """, (outcome, result_str, pred["id"]))
                            n_games += cur.rowcount
                        except Exception:
                            pass
                        break  # matched — stop searching completed games for this pred

            conn.commit()
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[resolve] game predictions error: {exc}")
        finally:
            conn.close()

    # ── 3. Resolve pending player props via ESPN boxscore ─────────────────────
    pending_props = get_pending_props(days_back=days_back)
    if pending_props:
        # Cache boxscore player stats per event_id so we don't re-fetch
        _boxscore_cache: dict[str, dict[str, float]] = {}

        for prop in pending_props:
            prop_sport  = str(prop.get("sport") or "").lower()
            player_name = str(prop.get("player_name") or "").strip().lower()
            team_name   = str(prop.get("team") or "").strip().lower()
            prop_type_raw = str(prop.get("prop_type") or "").strip().lower()
            prop_type = re.sub(r"[^a-z0-9]+", "_", prop_type_raw).strip("_")
            line_val    = _num(prop.get("line")) or 0.0
            rec         = str(prop.get("recommendation") or "OVER").upper()
            game_key    = str(prop.get("game_key") or "")

            for g in all_completed:
                sport_group = g["sport_group"]
                stat_map    = g["stat_map"]

                # Sport matching
                def _sport_matches(ps: str, sg: str) -> bool:
                    if not ps or ps in ("all", ""):
                        return True
                    if ps == sg:
                        return True
                    if ps == "mlb" and sg == "baseball":
                        return True
                    if ps == "basketball" and sg == "basketball":
                        return True
                    if ps == "icehockey" and sg == "icehockey":
                        return True
                    if ps == "soccer" and sg == "soccer":
                        return True
                    return False

                if not _sport_matches(prop_sport, sport_group):
                    continue

                # Game matching
                home, away = g["home"], g["away"]
                if game_key:
                    if not (any(w in game_key.lower() for w in home.lower().split() if len(w) > 3) or
                            any(w in game_key.lower() for w in away.lower().split() if len(w) > 3)):
                        if not any(w in game_key.lower() for w in team_name.split() if len(w) > 3):
                            continue
                else:
                    if team_name and not (_teams_match(home, team_name) or _teams_match(away, team_name)):
                        continue

                event_id    = g["event_id"]
                sport_path  = g["sport_path"]
                league_path = g["league_path"]
                cache_key   = f"{sport_path}/{league_path}/{event_id}"

                # Fetch + cache boxscore
                if cache_key not in _boxscore_cache:
                    player_stats = {}
                    try:
                        url = f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/{league_path}/summary"
                        resp = _req.get(url, params={"event": event_id}, timeout=8)
                        if resp.status_code == 200:
                            summary = resp.json() or {}
                            # Boxscore players block
                            for team_box in ((summary.get("boxscore") or {}).get("players") or []):
                                for stat_block in (team_box.get("statistics") or []):
                                    keys = [str(k or "").strip() for k in (stat_block.get("keys") or [])]
                                    key_idx = {_stat_key_norm(k): i for i, k in enumerate(keys)}
                                    for arow in (stat_block.get("athletes") or []):
                                        athlete  = arow.get("athlete") or {}
                                        pname    = str(athlete.get("displayName") or athlete.get("fullName") or "").strip().lower()
                                        if not pname:
                                            continue
                                        vals = arow.get("stats") or []
                                        for raw_k, mapped_k in stat_map.items():
                                            idx = key_idx.get(_stat_key_norm(raw_k))
                                            if idx is not None and idx < len(vals):
                                                v = _num(vals[idx])
                                                if v is not None:
                                                    player_stats.setdefault(pname, {})[mapped_k] = v
                            # Leaders block as fallback
                            for team_bucket in (summary.get("leaders") or []):
                                for cat in (team_bucket.get("leaders") or []):
                                    cat_name = str(cat.get("name") or "").strip()
                                    for raw_k, mapped_k in stat_map.items():
                                        if (_stat_key_norm(cat_name) == _stat_key_norm(raw_k)
                                                or _stat_key_norm(mapped_k) == _stat_key_norm(cat_name)):
                                            for lead in (cat.get("leaders") or []):
                                                athlete = lead.get("athlete") or {}
                                                pname   = str(athlete.get("displayName") or athlete.get("fullName") or "").strip().lower()
                                                v       = _num(lead.get("value"))
                                                if pname and v is not None:
                                                    player_stats.setdefault(pname, {})[mapped_k] = v
                    except Exception:
                        pass
                    _boxscore_cache[cache_key] = player_stats

                player_stats = _boxscore_cache[cache_key]
                if not player_stats:
                    continue

                # Find player in boxscore (exact or partial)
                matched_stats = player_stats.get(player_name)
                if matched_stats is None:
                    parts = [w for w in player_name.split() if len(w) > 2]
                    matched_stats = next(
                        (v for k, v in player_stats.items() if all(p in k for p in parts)),
                        None,
                    )
                if matched_stats is None:
                    continue

                # Find the actual stat value
                # prop_type in db can be "points", "rebounds", "assists", "goals", "shots_on_goal" etc.
                prop_aliases = _prop_type_aliases(prop_type)
                prop_alias_norms = {_stat_key_norm(a) for a in prop_aliases}
                actual_val = None
                for alias in prop_aliases:
                    actual_val = matched_stats.get(alias)
                    if actual_val is not None:
                        break
                if actual_val is None:
                    # Try mapped alias
                    for raw_k, mapped_k in stat_map.items():
                        if (_stat_key_norm(mapped_k) in prop_alias_norms
                                or _stat_key_norm(raw_k) in prop_alias_norms):
                            actual_val = matched_stats.get(mapped_k)
                            if actual_val is not None:
                                break
                if actual_val is None:
                    continue

                # Resolve
                if actual_val > line_val:
                    outcome = "WIN" if rec == "OVER" else "LOSS"
                elif actual_val < line_val:
                    outcome = "LOSS" if rec == "OVER" else "WIN"
                else:
                    outcome = "PUSH"

                update_prop_outcome(prop["id"], actual_val, outcome)
                n_props += 1
                break  # matched this prop — move to next

    # ── 4. MLB-specific prop + game resolution (statsapi) ─────────────────────
    try:
        from models.mlb_predictor import (
            resolve_game_outcomes as _mlb_game_res,
            resolve_prop_outcomes as _mlb_prop_res,
        )
        ng = _mlb_game_res(days_back=days_back)
        np_ = _mlb_prop_res(days_back=days_back)
        n_games += ng
        n_props  += np_
    except Exception:
        pass

    # ── 5. Resolve parlays ─────────────────────────────────────────────────────
    try:
        from models.mlb_predictor import resolve_tracked_parlays as _rtp
        n_parlays = _rtp(days_back=days_back + 4)
    except Exception:
        n_parlays = 0

    total = n_games + n_props + n_parlays
    if total:
        print(f"[resolve_all] Resolved {n_games} game preds + {n_props} props + {n_parlays} parlays")
        try:
            from data.db import get_performance_stats, get_parlay_performance_stats
            db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
            _sse_broadcast("performance_update", {
                "stats":        get_performance_stats(sport=db_sport),
                "parlay_stats": get_parlay_performance_stats(sport=db_sport),
            })
        except Exception:
            pass

    return {"games": n_games, "props": n_props, "parlays": n_parlays}


@app.route("/api/resolve-outcomes", methods=["POST"])
def api_resolve_outcomes():
    try:
        result = _resolve_all_sports_outcomes(days_back=21)
        n_games  = result["games"]
        n_props  = result["props"]
        n_parlay = result["parlays"]
        return jsonify({
            "ok": True,
            "resolved_games":   n_games,
            "resolved_props":   n_props,
            "resolved_parlays": n_parlay,
            "msg": f"Resolved {n_games} game preds + {n_props} props + {n_parlay} parlays",
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/parlay/save", methods=["POST"])
def api_parlay_save():
    data = request.get_json(force=True) or {}
    try:
        from data.db import save_tracked_parlay, _prediction_uid, _prop_uid
        # Default to duplicate prevention for all saves unless explicitly disabled.
        dedupe_raw = str(data.get("dedupe_pending", "1")).strip().lower()
        dedupe_pending = dedupe_raw in {"1", "true", "yes", "on"}
        raw_legs = data.get("legs", [])
        norm_legs = []
        if isinstance(raw_legs, list):
            for leg in raw_legs:
                if isinstance(leg, dict):
                    leg_payload = dict(leg)
                    leg_payload.setdefault("sport", _ACTIVE_SPORT)
                    leg_payload.setdefault("game_key", leg_payload.get("game", ""))
                    leg_payload.setdefault("game", leg_payload.get("game_key", ""))

                    # Attach deterministic prediction UID per leg for exact outcome lookups.
                    if not leg_payload.get("prediction_uid") and not leg_payload.get("bet_uid"):
                        game_date = leg_payload.get("game_date") or _et_calendar_today().isoformat()
                        source = str(leg_payload.get("source") or "").strip().lower()
                        bet_type = str(leg_payload.get("bet_type") or "").strip().lower()

                        is_prop_leg = (
                            source == "prop"
                            or bet_type == "player_prop"
                            or bool(leg_payload.get("prop_type") or leg_payload.get("stat_type"))
                        )

                        if is_prop_leg:
                            leg_uid = _prop_uid({
                                "sport": leg_payload.get("sport", _ACTIVE_SPORT),
                                "game_date": game_date,
                                "game_key": leg_payload.get("game_key") or leg_payload.get("game") or "",
                                "name": leg_payload.get("name") or leg_payload.get("player_name") or "",
                                "player_name": leg_payload.get("player_name") or leg_payload.get("name") or "",
                                "team": leg_payload.get("team") or "",
                                "stat_type": leg_payload.get("stat_type") or leg_payload.get("prop_type") or "",
                                "prop_type": leg_payload.get("prop_type") or leg_payload.get("stat_type") or "",
                                "line": leg_payload.get("line"),
                                "direction": leg_payload.get("direction") or leg_payload.get("recommendation") or "",
                                "recommendation": leg_payload.get("recommendation") or leg_payload.get("direction") or "",
                            }, game_date=game_date)
                        else:
                            pick_label = (
                                leg_payload.get("pick")
                                or leg_payload.get("label")
                                or ""
                            )
                            leg_uid = _prediction_uid({
                                "sport": leg_payload.get("sport", _ACTIVE_SPORT),
                                "game_date": game_date,
                                "game_key": leg_payload.get("game_key") or leg_payload.get("game") or "",
                                "bet_type": leg_payload.get("bet_type") or "",
                                "pick": pick_label,
                                "line": leg_payload.get("line"),
                            })

                        leg_payload["prediction_uid"] = leg_uid
                        leg_payload["bet_uid"] = leg_uid
                    elif leg_payload.get("prediction_uid") and not leg_payload.get("bet_uid"):
                        leg_payload["bet_uid"] = leg_payload.get("prediction_uid")
                    elif leg_payload.get("bet_uid") and not leg_payload.get("prediction_uid"):
                        leg_payload["prediction_uid"] = leg_payload.get("bet_uid")
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


@app.route("/api/kalshi/markets")
def api_kalshi_markets():
    """Public Kalshi market data (no auth required on Kalshi side)."""
    try:
        from data.kalshi import list_markets

        limit = int(request.args.get("limit", 200))
        cursor = (request.args.get("cursor") or "").strip() or None
        status = (request.args.get("status") or "open").strip() or None
        event_ticker = (request.args.get("event_ticker") or "").strip() or None
        series_ticker = (request.args.get("series_ticker") or "").strip() or None

        data = list_markets(
            limit=limit,
            cursor=cursor,
            status=status,
            event_ticker=event_ticker,
            series_ticker=series_ticker,
        )
        markets = data.get("markets") or []
        return jsonify({
            "ok": True,
            "markets": _clean(markets),
            "cursor": data.get("cursor"),
            "count": len(markets),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "markets": [], "count": 0})


@app.route("/api/kalshi/events")
def api_kalshi_events():
    """Public Kalshi event data (for market timing context)."""
    try:
        from data.kalshi import list_events

        limit = int(request.args.get("limit", 200))
        cursor = (request.args.get("cursor") or "").strip() or None
        status = (request.args.get("status") or "").strip() or None
        series_ticker = (request.args.get("series_ticker") or "").strip() or None

        data = list_events(
            limit=limit,
            cursor=cursor,
            status=status,
            series_ticker=series_ticker,
        )
        events = data.get("events") or []
        return jsonify({
            "ok": True,
            "events": _clean(events),
            "cursor": data.get("cursor"),
            "count": len(events),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "events": [], "count": 0})


@app.route("/api/kalshi/order", methods=["POST"])
def api_kalshi_order():
    """Execute a Kalshi order using API credentials from environment variables."""
    data = request.get_json(force=True) or {}
    try:
        from data.kalshi import place_order

        ticker = str(data.get("market_ticker") or data.get("ticker") or "").strip()
        if not ticker:
            return jsonify({"ok": False, "error": "market_ticker is required"}), 400

        side = str(data.get("side") or "yes").strip().lower()
        if side not in {"yes", "no"}:
            side = "yes"

        action = str(data.get("action") or "buy").strip().lower()
        if action not in {"buy", "sell"}:
            action = "buy"

        try:
            amount_usd = float(data.get("amount_usd", 0) or 0)
        except Exception:
            amount_usd = 0.0
        try:
            price_cents = int(float(data.get("limit_price_cents", data.get("price_cents", 50)) or 50))
        except Exception:
            price_cents = 50
        price_cents = max(1, min(price_cents, 99))

        count = data.get("count")
        if count is None:
            if amount_usd > 0:
                count = max(1, int((amount_usd * 100.0) // max(price_cents, 1)))
            else:
                count = 1
        else:
            count = max(1, int(count))

        client_order_id = (
            str(data.get("client_order_id") or "").strip()
            or f"bettor_{_et_calendar_today().strftime('%Y%m%d')}_{abs(hash((ticker, side, count, price_cents))) % 1000000:06d}"
        )

        payload = {
            "ticker": ticker,
            "side": side,
            "action": action,
            "type": str(data.get("type") or "limit"),
            "count": count,
            "client_order_id": client_order_id,
        }
        if side == "yes":
            payload["yes_price"] = price_cents
        else:
            payload["no_price"] = price_cents

        # Allow advanced callers to override/append raw order fields.
        raw_order = data.get("order")
        if isinstance(raw_order, dict):
            payload.update(raw_order)

        response = place_order(payload)
        return jsonify({
            "ok": True,
            "client_order_id": client_order_id,
            "request": _clean(payload),
            "response": _clean(response),
        })
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


# Interval for the periodic background outcome resolver (5 minutes)
_RESOLVE_INTERVAL = 5 * 60
_resolve_poller_timer: threading.Timer | None = None


def _start_outcome_resolver():
    """Periodically resolve pending bets for ALL sports every 5 minutes.
    Uses threading.Timer — no APScheduler needed."""
    global _resolve_poller_timer
    if _resolve_poller_timer is not None:
        return

    def _tick():
        global _resolve_poller_timer
        try:
            print("[auto-resolve] Running periodic all-sport resolver…")
            _resolve_all_sports_outcomes(days_back=21)
        except Exception as exc:
            print(f"[auto-resolve] error: {exc}")
        _resolve_poller_timer = threading.Timer(_RESOLVE_INTERVAL, _tick)
        _resolve_poller_timer.daemon = True
        _resolve_poller_timer.start()

    # First run after 2 minutes so startup completes first
    _resolve_poller_timer = threading.Timer(120, _tick)
    _resolve_poller_timer.daemon = True
    _resolve_poller_timer.start()
    print(f"[auto-resolve] Periodic resolver started (every {_RESOLVE_INTERVAL // 60} min)")


def _start_live_scores():
    # Start live-score + auto-resolve polling for all sports
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
                # Universal resolver handles all sports (MLB statsapi path included)
                res = _resolve_all_sports_outcomes(days_back=1)
                n_g = res.get("games", 0)
                n_p = res.get("props", 0)
                if n_g or n_p:
                    print(f"[live-scores] Auto-resolved {n_g} predictions, {n_p} props")
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
            boot_player_props = _build_all_sport_sentiment_props(all_games, all_bets)
            boot_best_bets = _multi_sport_best_bets_rows(all_bets)
            boot_props = _merge_all_sports_table_rows(boot_player_props, boot_best_bets)
            today_games = [g for g in all_games if str(g.get("game_date") or "") == today_str]
            tomorrow_games = [g for g in all_games if str(g.get("game_date") or "") == tomorrow_str]
            today_cards = [_build_card(g, all_bets, boot_player_props, "TODAY") for g in today_games]
            tomorrow_cards = [_build_card(g, all_bets, boot_player_props, "TOMORROW") for g in tomorrow_games]
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
        _start_outcome_resolver()
        _auto_boot_analysis()
    else:
        _scheduler = None
        _start_cache_poller()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
