"""Betting Bot - Web Dashboard v3"""
import os, sys, datetime, threading, traceback
from collections import deque
from flask import Flask, render_template, jsonify, request

try:
    import pytz
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    _SCHEDULER_OK = True
except ImportError:
    _SCHEDULER_OK = False

SRC_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SRC_DIR)
from config import MIN_VALUE_EDGE, KELLY_FRACTION, MLB_SEASONS, et_today

app = Flask(__name__, template_folder="templates")

try:
    from data.db import init_schema
    init_schema()
except Exception as e:
    print(f"[dashboard] DB init: {e}")

_PHASES = [
    "Fetching MLB schedule",
    "Fetching soccer fixtures",
    "Loading stats & models",
    "Fetching injury reports",
    "Fetching live odds",
    "Running team win algorithm",
    "Analyzing player props",
    "Building game cards & parlays",
    "Saving to database",
]

_state = {
    "status": "idle", "phase": "", "phase_idx": 0, "phase_total": len(_PHASES),
    "last_updated": None, "error": None,
    "today_team_picks": [],
    "tomorrow_team_picks": [],
    "player_props": [],
    "prop_parlays": {},
    "team_parlays": {},
    "upcoming_games": [],
    "game_cards_today": [],
    "game_cards_tomorrow": [],
    "best_parlays": [],
}
_lock = threading.Lock()

_logs = deque(maxlen=400)
_logs_lock = threading.Lock()

_ANALYSIS_LOCK_KEY = 98243751


def _try_analysis_lock():
    """Return a DB connection holding the advisory lock, or None if unavailable."""
    try:
        from data.db import get_conn
        conn = get_conn()
        if conn is None:
            return None
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (_ANALYSIS_LOCK_KEY,))
        ok = cur.fetchone()[0]
        if not ok:
            conn.close()
            return None
        return conn
    except Exception as e:
        print(f"[analysis_lock] acquire error: {e}")
        return None


def _release_analysis_lock(conn):
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_advisory_unlock(%s)", (_ANALYSIS_LOCK_KEY,))
        conn.commit()
    except Exception as e:
        print(f"[analysis_lock] release error: {e}")
    finally:
        conn.close()


def _load_cache_into_state(max_age_hours: int = 22) -> bool:
    """Load DB cache into memory for the current worker. Returns True if loaded."""
    try:
        from data.db import get_analysis_cache, get_upcoming_games
        cached = get_analysis_cache(max_age_hours=max_age_hours)
        if not cached:
            return False
        upcoming_games = []
        try:
            upcoming_games = get_upcoming_games(days_ahead=1)
        except Exception:
            pass
        with _lock:
            _state.update({
                "status":              "done",
                "phase":               "Complete (from DB cache)",
                "last_updated":        cached.get("last_updated",
                                          cached.get("_updated_at",
                                              datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))),
                "game_cards_today":    cached.get("game_cards_today", []),
                "game_cards_tomorrow": cached.get("game_cards_tomorrow", []),
                "best_parlays":        cached.get("best_parlays", []),
                "today_team_picks":    cached.get("today_team_picks", []),
                "tomorrow_team_picks": cached.get("tomorrow_team_picks", []),
                "player_props":        cached.get("player_props", []),
                "prop_parlays":        cached.get("prop_parlays", {}),
                "team_parlays":        cached.get("team_parlays", {}),
                "upcoming_games":      upcoming_games,
                "error":               None,
            })
        return True
    except Exception as e:
        _log(f"Cache load error: {e}")
        return False


def _log(msg):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    with _logs_lock:
        _logs.append(entry)
    print(entry, flush=True)


class _StdoutCapture:
    def __init__(self, real):
        self._real = real

    def write(self, msg):
        s = msg.rstrip()
        if s:
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            with _logs_lock:
                _logs.append(f"[{ts}] {s}")
        self._real.write(msg)

    def flush(self):
        self._real.flush()

    def fileno(self):
        return self._real.fileno()


def _safety_score(model_prob, edge, book_prob=None):
    prob_score  = min(float(model_prob or 0.5), 0.92)
    edge_norm   = min(max(float(edge or 0), 0.0), 0.30) / 0.30
    consistency = 1.0 - abs(float(model_prob or 0.5) - float(book_prob or model_prob or 0.5)) * 2.0
    consistency = max(0.0, min(1.0, consistency))
    return round(prob_score * 0.50 + edge_norm * 0.30 + consistency * 0.20, 4)


def _safety_label(score):
    if score >= 0.72:
        return "ELITE"
    if score >= 0.60:
        return "SAFE"
    if score >= 0.50:
        return "MODERATE"
    return "RISKY"


_FINAL_STATUSES = {
    # MLB
    "final", "game over", "f", "completed early", "postponed",
    # soccer
    "finished", "ft", "aet", "pen",
}


def _game_is_over(status: str) -> bool:
    """Return True if game has already completed."""
    st = (status or "").strip().lower()
    if not st:
        return False
    return any(st == s or st.startswith(s) for s in _FINAL_STATUSES)


def _fmt_time_12h(gtime: str) -> str:
    """Convert '15:05' or '15:05:00' to '3:05 PM'."""
    if not gtime:
        return ""
    try:
        fmt = "%H:%M:%S" if gtime.count(":") == 2 else "%H:%M"
        t = datetime.datetime.strptime(gtime, fmt)
        return t.strftime("%I:%M %p").lstrip("0")
    except Exception:
        return gtime


def _when_str(gdate, gtime, status, today, tomorrow):
    st = (status or "").upper()
    if _game_is_over(status):
        return "FINAL", "FINAL"
    if "IN PROGRESS" in st or "LIVE" in st or "IN_PLAY" in st or "HALFTIME" in st or "PAUSED" in st:
        return "LIVE NOW", "LIVE"
    t12 = _fmt_time_12h(gtime)
    if gdate == today and t12:
        return f"TODAY {t12} ET", "TODAY"
    if gdate == today:
        return "TODAY", "TODAY"
    if gdate == tomorrow:
        t = f" {t12} ET" if t12 else ""
        return f"TOMORROW{t}", "TOMORROW"
    return gdate or "TBD", "UPCOMING"


def _build_team_pick(bet, today, tomorrow):
    parts     = (bet.get("matchup") or "").split(" vs ")
    ht        = parts[0] if len(parts) == 2 else bet.get("matchup", "?")
    at        = parts[1] if len(parts) == 2 else "?"
    side      = bet.get("bet", "HOME")
    pick_team = at if side == "AWAY" else (ht if side == "HOME" else "DRAW")
    opp_team  = ht if side == "AWAY" else (at if side == "HOME" else "—")
    odds_am   = bet.get("odds_am", 0)
    safety    = _safety_score(bet.get("model_prob", 0.5), bet.get("edge", 0), bet.get("book_prob"))
    when, when_label = _when_str(bet.get("date", today), bet.get("game_time"), bet.get("status"), today, tomorrow)
    return {
        "_id":          bet.get("_id", ""),
        "sport":        (bet.get("sport") or "MLB").upper(),
        "league":       bet.get("league", ""),
        "home_team":    ht,
        "away_team":    at,
        "pick_team":    pick_team,
        "opp_team":     opp_team,
        "side":         side,
        "conf":         round(float(bet.get("model_prob", 0.5)) * 100),
        "edge":         round(float(bet.get("edge", 0)) * 100, 1),
        "odds_am":      odds_am,
        "odds_str":     (f"+{odds_am}" if odds_am and odds_am > 0 else str(odds_am or "N/A")),
        "dec_odds":     float(bet.get("dec_odds", 2.0)),
        "safety":       safety,
        "safety_label": _safety_label(safety),
        "when":         when,
        "when_label":   when_label,
        "home_starter": bet.get("home_starter", ""),
        "away_starter": bet.get("away_starter", ""),
        "ev":           round(float(bet.get("ev", 0)), 4),
    }


def _build_prop_pick(p, today, tomorrow, odds_lines: dict | None = None):
    """
    Build a unified prop pick dict for display.
    Handles both pitcher props (strikeouts) and hitter props (hits, HR, total_bases).
    odds_lines: optional dict {player_name: {market: {line, over_odds, under_odds}}}
                from The Odds API — used to replace estimated lines with real book lines.
    """
    ov, un    = float(p.get("over_prob", 0.5)), float(p.get("under_prob", 0.5))
    stat_type = p.get("stat_type", "strikeouts")
    direction = "OVER" if ov >= un else "UNDER"
    conf      = round(max(ov, un) * 100)

    # Map stat_type to readable labels
    _PROP_LABELS = {
        # MLB pitcher
        "strikeouts":         "Pitcher Strikeouts",
        # MLB hitter
        "hits":               "Batter Hits",
        "home_runs":          "Batter Home Runs",
        "total_bases":        "Total Bases",
        "rbi":                "RBI",
        "runs":               "Runs Scored",
        "walks":              "Walks (BB)",
        "stolen_bases":       "Stolen Bases",
        "batter_strikeouts":  "Batter Strikeouts",
        "doubles":            "Doubles (2B)",
        # Soccer
        "goals_scored":       "Goal Scored (Anytime)",
        "assists":            "Assist",
        "shots_total":        "Shots",
        "shots_on_target":    "Shots on Target",
        "goal_or_assist":     "Goal or Assist",
        "cards":              "Carded (Yellow/Red)",
    }
    _MARKET_MAP = {
        "strikeouts":         "pitcher_strikeouts",
        "hits":               "batter_hits",
        "home_runs":          "batter_home_runs",
        "total_bases":        "batter_total_bases",
        "rbi":                "batter_rbis",
        "runs":               "batter_runs_scored",
        "walks":              "batter_walks",
        "stolen_bases":       "batter_stolen_bases",
        "batter_strikeouts":  "batter_strikeouts",
        "doubles":            "batter_doubles",
    }
    _RATE_LABELS = {
        "strikeouts":         "K/Start",
        "hits":               "H/Game",
        "home_runs":          "HR/Game",
        "total_bases":        "TB/Game",
        "rbi":                "RBI/Gm",
        "runs":               "R/Game",
        "walks":              "BB/Game",
        "stolen_bases":       "SB/Game",
        "batter_strikeouts":  "K/Game",
        "doubles":            "2B/Game",
        "goals_scored":       "xG/Match",
        "assists":            "xA/Match",
        "shots_total":        "Sh/Match",
        "shots_on_target":    "SOT/Match",
        "goal_or_assist":     "xG+A",
        "cards":              "Card/M",
    }

    prop_label = _PROP_LABELS.get(stat_type, stat_type.replace("_", " ").title())
    rate_label = _RATE_LABELS.get(stat_type, "Avg/Game")
    line       = p.get("line", "?")

    # If we have real book odds, use their line and recalculate confidence
    real_over_odds  = None
    real_under_odds = None
    if odds_lines:
        pname   = p.get("name", "")
        mkey    = _MARKET_MAP.get(stat_type, "")
        pdata   = odds_lines.get(pname, {}).get(mkey, {})
        if pdata:
            line            = pdata.get("line", line)
            real_over_odds  = pdata.get("over_odds")
            real_under_odds = pdata.get("under_odds")

    # Dec odds: from real book if available, else estimated
    if real_over_odds and direction == "OVER":
        dec_odds = round((real_over_odds / 100 + 1) if real_over_odds > 0
                         else (100 / abs(real_over_odds) + 1), 3)
    elif real_under_odds and direction == "UNDER":
        dec_odds = round((real_under_odds / 100 + 1) if real_under_odds > 0
                         else (100 / abs(real_under_odds) + 1), 3)
    else:
        dec_odds = round(1.0 / max(ov, un), 3) if max(ov, un) > 0 else 2.0

    edge   = max(ov, un) - 0.5
    safety = _safety_score(max(ov, un), edge)
    when, when_label = _when_str(p.get("date", today), p.get("game_time"), "", today, tomorrow)

    return {
        "_id":          p.get("_id", ""),
        "name":         p.get("name", ""),
        "team":         p.get("team", ""),
        "game":         p.get("game", ""),
        "sport":        p.get("sport", "mlb"),
        "league":       p.get("league", ""),
        "stat_type":    stat_type,
        "prop_label":   prop_label,
        "direction":    direction,
        "line":         line,
        "conf":         conf,
        "safety":       safety,
        "safety_label": _safety_label(safety),
        "dec_odds":     dec_odds,
        "when":         when,
        "when_label":   when_label,
        # pitcher-specific
        "era":          round(float(p.get("era",  0)), 2),
        "xfip":         round(float(p.get("xfip", p.get("era", 0))), 2),
        "k9":           round(float(p.get("k9",   0)), 1),
        "k_pct":        round(float(p.get("k_pct",0)), 1),
        "whip":         round(float(p.get("whip", 0)), 2),
        "avg_ks":       round(float(p.get("avg_per_game", 0)), 1),
        # hitter-specific
        "avg":          round(float(p.get("avg",      0)), 3),
        "ops":          round(float(p.get("ops",      0)), 3),
        "wrc_plus":     round(float(p.get("wrc_plus", 0))),
        "avg_per_game": round(float(p.get("avg_per_game", 0)), 2),
        # soccer-specific
        "xg":           round(float(p.get("xg",         0)), 2),
        "xa":           round(float(p.get("xa",         0)), 2),
        "goals_pg":     round(float(p.get("goals_pg",   0)), 3),
        "assists_pg":   round(float(p.get("assists_pg", 0)), 3),
        "card_pg":      round(float(p.get("card_pg",    0)), 3),
        "mp":           round(float(p.get("mp",         0))),
        "over_pct":     round(ov * 100),
        "under_pct":    round(un * 100),
        # bet label context
        "rate_label":   rate_label,
        "ip_per_start": round(float(p.get("ip_per_start", 0)), 1),
        # real odds from book
        "over_odds_am":  real_over_odds,
        "under_odds_am": real_under_odds,
        "_source":       p.get("_source", ""),
    }


def _build_parlays(picks, max_legs=10, top_n=3):
    from itertools import combinations
    result = {}
    pool = sorted(picks, key=lambda x: x.get("safety", 0), reverse=True)[:18]
    for n in range(2, min(max_legs + 1, len(pool) + 1)):
        scored = []
        for combo in combinations(pool, n):
            comb_p = 1.0
            for c in combo:
                comb_p *= (c.get("conf", 50) / 100.0)
            comb_d = 1.0
            for c in combo:
                comb_d *= float(c.get("dec_odds", 2.0))
            avg_safety = sum(c.get("safety", 0) for c in combo) / n
            scored.append({
                "legs":          [c.get("label", "?") for c in combo],
                "combined_prob": round(comb_p * 100, 1),
                "combined_dec":  round(comb_d, 2),
                "avg_safety":    round(avg_safety, 3),
                "safety_label":  _safety_label(avg_safety),
                "score":         round(comb_p * avg_safety, 5),
            })
        scored.sort(key=lambda x: x["score"], reverse=True)
        result[str(n)] = scored[:top_n]
    return result


# ── Game-card helpers ─────────────────────────────────────────────────────────

def _norm_cdf(z):
    """Standard normal CDF approximation (Abramowitz & Stegun, error < 7.5e-8)."""
    import math
    sign = 1 if z >= 0 else -1
    z = abs(z)
    t = 1.0 / (1.0 + 0.2316419 * z)
    d = 0.3989422820 * math.exp(-z * z / 2.0)
    p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.7814779 + t * (-1.8212560 + t * 1.3302744))))
    return 0.5 + sign * (0.5 - p)


def _compute_game_prop(pred, sport, predicted_total=None):
    """
    Return the best single game prop for display.
      Soccer → BTTS (both teams to score).
      MLB    → Total Runs OVER/UNDER.
    Returns a dict or None.
    """
    import math
    try:
        if sport == "soccer":
            hxg = float(pred.get("home_xg") or pred.get("predicted_home_goals") or 0)
            axg = float(pred.get("away_xg") or pred.get("predicted_away_goals") or 0)
            if hxg > 0.1 and axg > 0.1:
                p = (1 - math.exp(-hxg)) * (1 - math.exp(-axg))
            else:
                hw = float(pred.get("home_win_prob") or pred.get("home_win") or 0.38)
                aw = float(pred.get("away_win_prob") or pred.get("away_win") or 0.28)
                p = max(0.28, min(0.72, 0.28 + (1.0 - abs(hw - aw)) * 0.42))
            p = round(p, 3)
            if p < 0.50:
                return None
            s = _safety_score(p, p - 0.5)
            return {"type": "btts", "label": "Both Teams to Score",
                    "prob": p, "conf": round(p * 100),
                    "safety": s, "safety_label": _safety_label(s),
                    "dec_odds": round(1.0 / max(p, 0.01), 2), "line": None}
        elif sport == "mlb":
            total = float(predicted_total or 8.5)
            line  = round(total * 2) / 2          # nearest 0.5
            z     = (line - total) / 3.2
            p_ov  = 1.0 - _norm_cdf(z)
            direction, prob = ("OVER", round(p_ov, 3)) if p_ov >= 0.5 \
                               else ("UNDER", round(1.0 - p_ov, 3))
            if prob < 0.52:
                return None
            label = f"Total {direction} {line} Runs"
            s = _safety_score(prob, prob - 0.5)
            return {"type": f"total_{direction.lower()}", "label": label,
                    "direction": direction, "prob": prob, "conf": round(prob * 100),
                    "safety": s, "safety_label": _safety_label(s),
                    "dec_odds": round(1.0 / max(prob, 0.01), 2), "line": line}
    except Exception:
        pass
    return None


def _build_game_cards(mlb_preds, soccer_preds, win_bets, player_props, today, tomorrow):
    """
    Group predictions by game into clean cards.
    Each card has: win_pick, player_prop, game_prop.
    Returns (today_cards, tomorrow_cards).
    """
    # win lookup: (home_team, away_team) → best win bet (highest edge)
    win_by_game = {}
    for b in win_bets:
        parts = (b.get("matchup") or "").split(" vs ")
        if len(parts) == 2:
            key = (parts[0].strip(), parts[1].strip())
            if key not in win_by_game or b.get("edge", 0) > win_by_game[key].get("edge", 0):
                win_by_game[key] = b

    # prop lookup: pick best prop for each game by scanning player_props
    prop_by_game = {}  # (home, away) → prop dict
    for p in sorted(player_props, key=lambda x: x.get("safety", 0), reverse=True):
        gs = p.get("game", "")
        matched = False
        for (ht, at) in win_by_game:
            if (ht in gs or at in gs) and (ht, at) not in prop_by_game:
                prop_by_game[(ht, at)] = p
                matched = True
                break
        if not matched:
            # Try by team field
            pt = p.get("team", "")
            for (ht, at) in win_by_game:
                if pt and (pt == ht or pt == at) and (ht, at) not in prop_by_game:
                    prop_by_game[(ht, at)] = p
                    break

    today_cards, tomorrow_cards = [], []
    seen = set()

    for pred, sport in ([(p, "mlb") for p in mlb_preds] +
                        [(p, "soccer") for p in soccer_preds]):
        ht = pred.get("home_team", "")
        at = pred.get("away_team", "")
        if not ht or not at:
            continue
        key = (ht, at)
        if key in seen:
            continue
        seen.add(key)

        gdate  = pred.get("date", today)
        gtime  = pred.get("game_time")
        status = pred.get("status", "")
        if _game_is_over(status):
            continue

        when, when_label = _when_str(gdate, gtime, status, today, tomorrow)
        if when_label not in ("TODAY", "TOMORROW", "LIVE", "UPCOMING"):
            continue

        # ── Win pick ──────────────────────────────────────────────
        win_bet  = win_by_game.get(key)
        win_pick = _build_team_pick(win_bet, today, tomorrow) if win_bet else None

        # ── Player prop ───────────────────────────────────────────
        raw_p = prop_by_game.get(key)
        if not raw_p:
            for p in player_props:
                gs = p.get("game", "")
                if ht in gs or at in gs:
                    raw_p = p
                    break
        player_prop = None
        if raw_p:
            player_prop = {
                "name":         raw_p.get("name", ""),
                "team":         raw_p.get("team", ""),
                "direction":    raw_p.get("direction", "OVER"),
                "line":         raw_p.get("line", "?"),
                "prop_label":   raw_p.get("prop_label", "Prop"),
                "conf":         raw_p.get("conf", 50),
                "safety":       raw_p.get("safety", 0.5),
                "safety_label": raw_p.get("safety_label", "MODERATE"),
                "sport":        raw_p.get("sport", sport),
                "over_pct":     raw_p.get("over_pct", 50),
                "under_pct":    raw_p.get("under_pct", 50),
            }

        # ── Game prop ─────────────────────────────────────────────
        game_prop = _compute_game_prop(pred, sport, pred.get("predicted_total"))

        if not win_pick and not player_prop and not game_prop:
            continue

        safeties    = [x["safety"] for x in [win_pick, player_prop, game_prop] if x]
        overall_s   = round(sum(safeties) / len(safeties), 3) if safeties else 0.5

        card = {
            "game_key":              f"{at}@{ht}",
            "sport":                 sport.upper(),
            "league":                pred.get("league", sport.upper()),
            "home_team":             ht,
            "away_team":             at,
            "when":                  when,
            "when_label":            when_label,
            "date":                  gdate,
            "overall_safety":        overall_s,
            "overall_safety_label":  _safety_label(overall_s),
            "win_pick":              win_pick,
            "player_prop":           player_prop,
            "game_prop":             game_prop,
            "home_starter":          pred.get("home_starter", ""),
            "away_starter":          pred.get("away_starter", ""),
        }

        if gdate == today or when_label == "LIVE":
            today_cards.append(card)
        elif gdate == tomorrow:
            tomorrow_cards.append(card)

    today_cards.sort(key=lambda x: x["overall_safety"], reverse=True)
    tomorrow_cards.sort(key=lambda x: x["overall_safety"], reverse=True)
    return today_cards, tomorrow_cards


def _build_best_parlays(all_cards, max_legs=10):
    """
    For each leg count 2..max_legs, find the safest parlay.
    One pick per game (best safety across win/prop/game_prop).
    Returns list of parlay dicts.
    """
    from itertools import combinations

    pool = []
    for card in all_cards:
        candidates = []
        for src, mk in [(card.get("win_pick"), "win"),
                        (card.get("player_prop"), "prop"),
                        (card.get("game_prop"), "game")]:
            if not src:
                continue
            if mk == "win":
                label   = f"{src['pick_team']} WIN"
                dec     = max(float(src.get("dec_odds", 2.0)), 1.01)
                conf_f  = src.get("conf", 50) / 100.0
            elif mk == "prop":
                conf_f  = src.get("conf", 50) / 100.0
                label   = f"{src['name']} {src['direction']} {src['line']} {src['prop_label']}"
                dec     = max(round(1.0 / max(conf_f, 0.01), 2), 1.01)
            else:
                conf_f  = src.get("conf", 50) / 100.0
                label   = src.get("label", "Game Prop")
                dec     = max(src.get("dec_odds", 2.0), 1.01)
            candidates.append({
                "label": label, "conf": conf_f, "dec_odds": dec,
                "safety": src.get("safety", 0.5),
                "badge":  src.get("safety_label", "MODERATE"),
                "game":   card["game_key"],
                "when":   card.get("when_label", "TODAY"),
            })
        if candidates:
            pool.append(max(candidates, key=lambda x: x["safety"]))

    if len(pool) < 2:
        return []

    results = []
    for n in range(2, min(max_legs + 1, len(pool) + 1)):
        best, best_score = None, -1.0
        for combo in combinations(pool, n):
            cp = 1.0
            for c in combo: cp *= c["conf"]
            cd = 1.0
            for c in combo: cd *= c["dec_odds"]
            avgs = sum(c["safety"] for c in combo) / n
            score = cp * avgs
            if score > best_score:
                best_score = score
                best = {
                    "n_legs": n,
                    "legs": [{"label": c["label"], "conf": round(c["conf"] * 100),
                              "badge": c["badge"], "game": c["game"],
                              "when": c["when"]} for c in combo],
                    "combined_prob": round(cp * 100, 1),
                    "combined_dec":  round(cd, 2),
                    "avg_safety":    round(avgs, 3),
                    "safety_label":  _safety_label(avgs),
                    "score":         round(score, 5),
                }
        if best:
            results.append(best)
    return results


def _phase(idx, name=""):
    label = name or (_PHASES[idx] if idx < len(_PHASES) else name)
    with _lock:
        _state["phase"]     = label
        _state["phase_idx"] = idx
    _log(f"Phase {idx+1}/{len(_PHASES)}: {label}")


def _run_from_cache():
    """
    Reload analysis state from the DB cache without making any external API calls.
    Falls through to a full _run_analysis() if no fresh cache exists.
    """
    _real = sys.stdout
    sys.stdout = _StdoutCapture(_real)
    with _lock:
        _state["status"] = "running"; _state["error"] = None
        _state["phase"]  = "Loading from database"; _state["phase_idx"] = 0
    with _logs_lock:
        _logs.clear()
    _log("=== Loading from database cache ===")
    try:
        from data.db import get_analysis_cache, get_upcoming_games
        cached = get_analysis_cache(max_age_hours=22)
        if not cached:
            _log("No fresh cache found — running full analysis instead...")
            sys.stdout = _real
            _run_analysis()
            return
        _log(f"Cache hit — last updated {cached.get('_updated_at', 'today')}")
        upcoming_games = []
        try:
            upcoming_games = get_upcoming_games(days_ahead=1)
        except Exception:
            pass
        _log("=== Loaded from cache ===")
        with _lock:
            _state.update({
                "status":              "done",
                "phase":               "Complete (from DB cache)",
                "last_updated":        cached.get("last_updated",
                                          cached.get("_updated_at",
                                              datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))),
                "game_cards_today":    cached.get("game_cards_today", []),
                "game_cards_tomorrow": cached.get("game_cards_tomorrow", []),
                "best_parlays":        cached.get("best_parlays", []),
                "today_team_picks":    cached.get("today_team_picks", []),
                "tomorrow_team_picks": cached.get("tomorrow_team_picks", []),
                "player_props":        cached.get("player_props", []),
                "prop_parlays":        cached.get("prop_parlays", {}),
                "team_parlays":        cached.get("team_parlays", {}),
                "upcoming_games":      upcoming_games,
                "error":               None,
            })
    except Exception:
        err = traceback.format_exc()
        _log(f"Cache load error — falling back to full analysis: {err}")
        sys.stdout = _real
        _run_analysis()
    finally:
        sys.stdout = _real


def _run_analysis():
    import warnings; warnings.filterwarnings("ignore")
    import pandas as pd
    _real = sys.stdout
    sys.stdout = _StdoutCapture(_real)
    lock_conn = _try_analysis_lock()
    if lock_conn is None:
        _log("Another analysis run is active — skipping duplicate run")
        if not _load_cache_into_state():
            with _lock:
                _state["status"] = "running"
                _state["phase"] = "Waiting for active run"
        sys.stdout = _real
        return False
    ok = False
    with _lock:
        _state["status"] = "running"; _state["error"] = None
        _state["phase"]  = _PHASES[0]; _state["phase_idx"] = 0
    with _logs_lock:
        _logs.clear()
    _log("=== Analysis started ===")
    try:
        today    = et_today().isoformat()
        tomorrow = (et_today() + datetime.timedelta(days=1)).isoformat()

        _phase(0)
        from data.mlb_fetcher import (get_schedule_range, build_game_dataset,
                                       estimate_game_total, get_starters_props_batch)
        from models.mlb_model import load_model, predict_from_season_stats
        games          = get_schedule_range(days_ahead=1)
        today_games    = [g for g in games if g.get("date", "") == today]
        tomorrow_games = [g for g in games if g.get("date", "") == tomorrow]
        _log(f"MLB: {len(today_games)} today, {len(tomorrow_games)} tomorrow")

        _phase(1)
        from data.soccer_fetcher import (get_todays_fixtures, get_historical_matches,
                                          compute_team_strength)
        from models.soccer_model import load_model as load_soccer, SoccerModel
        soccer_model = load_soccer()
        if soccer_model is None:
            _log("No saved soccer model — training on historical match data...")
            try:
                all_matches = []
                for lk in ["EPL", "ESP", "GER"]:
                    m = get_historical_matches(lk)
                    if not m.empty:
                        all_matches.append(m)
                        _log(f"  {lk}: {len(m)} historical matches loaded")
                if all_matches:
                    combined = pd.concat(all_matches, ignore_index=True)
                    sm = SoccerModel()
                    sm.fit(combined)
                    soccer_model = sm
                    _log(f"Soccer model trained: {len(combined)} matches, {combined['home_team'].nunique()} teams")
                else:
                    _log("WARNING: No historical soccer data — soccer model unavailable")
            except Exception as _e:
                _log(f"Soccer model training failed: {_e}")
        else:
            _log("Soccer model loaded from disk")
        soccer_preds = []
        fixtures = get_todays_fixtures(["EPL", "ESP", "GER", "ITA", "FRA", "MLS"])
        # Only predict on live or upcoming fixtures — skip finished games
        fixtures = [f for f in fixtures if not _game_is_over(f.get("status", ""))]
        _log(f"Soccer: {len(fixtures)} fixtures")
        if soccer_model and getattr(soccer_model, 'fitted', False):
            for f in fixtures:
                try:
                    pred = soccer_model.predict(f["home_team"], f["away_team"])
                    pred.update({"home_team": f["home_team"], "away_team": f["away_team"],
                                  "league": f.get("league", ""), "game_time": f.get("game_time"),
                                  "date": f.get("date", today)})
                    soccer_preds.append(pred)
                except Exception:
                    pass
        else:
            _log("WARNING: Soccer model not available — soccer predictions skipped")

        _phase(2)
        _log("Loading season stats for all teams...")
        stats     = build_game_dataset(MLB_SEASONS)   # use all configured seasons
        mlb_model = load_model()
        if mlb_model is None:
            if not stats.empty:
                _log(f"No saved MLB model — training on {stats['team'].nunique()} teams "
                     f"across {stats['season'].nunique()} season(s)...")
                from models.mlb_model import train as _train_mlb
                mlb_model = _train_mlb(stats, verbose=False)
                _log("MLB model trained and cached for future runs")
            else:
                _log("WARNING: No season stats available — all MLB confidence will show 50%")
        else:
            _log(f"MLB model loaded — {stats['team'].nunique() if not stats.empty else 0} teams in stats")
        mlb_preds = []
        for g in games:
            if g.get("status", "") not in ("Preview", "Pre-Game", "Scheduled", "Warmup", ""):
                continue
            pred = predict_from_season_stats(g["home_team"], g["away_team"], stats, mlb_model)
            pred.update({
                "home_team":       g["home_team"],
                "away_team":       g["away_team"],
                "home_starter":    g.get("home_starter", "TBD"),
                "away_starter":    g.get("away_starter", "TBD"),
                "predicted_total": estimate_game_total(g["home_team"], g["away_team"], stats),
                "game_time":       g.get("game_time"),
                "date":            g.get("date", today),
            })
            mlb_preds.append(pred)
        _log(f"Predictions ready: {len(mlb_preds)} MLB, {len(soccer_preds)} soccer")

        _phase(3)
        all_injuries = []
        try:
            from data.injury_fetcher import fetch_all_injuries
            from data.db import save_injuries
            raw_inj = fetch_all_injuries()
            for lk, lst in raw_inj.items():
                save_injuries("mlb" if lk == "mlb" else "soccer", lst)
                all_injuries.extend(lst)
                if lst:
                    _log(f"Injuries [{lk}]: {len(lst)} players affected")
        except Exception as e:
            _log(f"WARNING injuries skipped: {e}")

        # ── Populate DB from all extended data sources (background-safe) ──
        _log("Populating DB from extended sources (SportsData, TheSportsDB, RapidAPI)...")
        try:
            import threading as _t
            def _populate_extended():
                try:
                    from data.sportsdata_fetcher import populate_mlb, populate_soccer
                    populate_mlb()
                    for _comp in [5, 12, 10, 11, 8]:
                        try:
                            populate_soccer(competition=_comp)
                        except Exception:
                            pass
                except Exception as _e:
                    print(f"[dashboard] sportsdata populate error: {_e}")
                try:
                    from data.thesportsdb_fetcher import populate_soccer_standings, populate_today_events
                    populate_soccer_standings()
                    populate_today_events("soccer")
                except Exception as _e:
                    print(f"[dashboard] thesportsdb populate error: {_e}")
                try:
                    from data.rapidapi_football_fetcher import populate_live_scores
                    populate_live_scores()
                except Exception as _e:
                    print(f"[dashboard] rapidapi_football populate error: {_e}")
            _t.Thread(target=_populate_extended, daemon=True).start()
            _log("Extended data collection running in background...")
        except Exception as _ext_e:
            _log(f"Extended data collection skipped: {_ext_e}")


        _phase(4)
        _log("Fetching live odds...")
        from data.odds_fetcher import (get_live_odds, odds_to_dataframe,
                                        get_totals_odds, totals_to_dataframe)
        ml_rows = []
        for sk in ["mlb", "epl", "laliga", "bundesliga"]:
            raw = get_live_odds(sk, markets="h2h")
            if raw:
                d = odds_to_dataframe(raw); d["sport_key"] = sk; ml_rows.append(d)
                _log(f"Odds [{sk}]: {len(raw)} games")
            else:
                _log(f"No odds yet [{sk}]")
        ml_df = pd.concat(ml_rows, ignore_index=True) if ml_rows else pd.DataFrame()

        def sfilt(df, is_mlb):
            if df.empty: return df
            mask = df.get("sport_key", pd.Series(dtype=str)).isin({"mlb", "baseball_mlb"})
            return df[mask] if is_mlb else df[~mask]

        _phase(5)
        _log("Running team win algorithm (safety scores)...")
        from analysis.value_finder import find_value_bets
        win_bets = (find_value_bets(mlb_preds, sfilt(ml_df, True),  sport="mlb") +
                    find_value_bets(soccer_preds, sfilt(ml_df, False), sport="soccer"))
        _log(f"Team value bets: {len(win_bets)}")

        # ── Enrich with multi-source news/form/injury signals ────────────
        try:
            from models.news_model import enrich_prediction
            _log("Enriching predictions with news/form/injury signals...")
            for b in win_bets:
                pts  = (b.get("matchup") or "").split(" vs ")
                ht   = pts[0] if len(pts) == 2 else ""
                at   = pts[1] if len(pts) == 2 else ""
                lk   = b.get("league", "")
                sp   = b.get("sport", "mlb")
                if ht and at:
                    enrich_prediction(b, sport=sp, league=lk)
        except Exception as _e:
            _log(f"[news_model] skipped: {_e}")

        game_lookup = {(g["home_team"], g["away_team"]): g for g in games}
        for i, b in enumerate(win_bets):
            b["_id"] = f"win_{i}"
            pts = (b.get("matchup") or "").split(" vs ")
            ht, at = (pts[0], pts[1]) if len(pts) == 2 else ("", "")
            g = game_lookup.get((ht, at)) or {}
            if not b.get("date"):      b["date"]      = g.get("date", today)
            if not b.get("game_time"): b["game_time"] = g.get("game_time")
            b["home_starter"] = g.get("home_starter", "")
            b["away_starter"] = g.get("away_starter", "")
            b["status"]       = g.get("status", "")

        # Drop bets for games that are already over
        win_bets      = [b for b in win_bets if not _game_is_over(b.get("status", ""))]
        today_wins    = [b for b in win_bets if b.get("date", today) == today]
        tomorrow_wins = [b for b in win_bets if b.get("date", "") == tomorrow]

        _phase(6)
        _log("Analyzing player props (MLB pitchers + hitters + soccer)...")
        # ── Fetch real book player-prop lines (uses Odds API credits) ────
        odds_lines: dict = {}   # {player_name: {market_key: {line, over_odds, under_odds}}}
        try:
            from data.odds_fetcher import get_player_props_odds
            _all_mlb_markets = (
                "pitcher_strikeouts,"
                "batter_hits,batter_home_runs,batter_total_bases,"
                "batter_rbis,batter_runs_scored,batter_walks,"
                "batter_stolen_bases,batter_strikeouts,batter_doubles"
            )
            raw_lines = get_player_props_odds("mlb", markets=_all_mlb_markets, max_events=15)
            for ol in raw_lines:
                pname = ol.get("player", "")
                mkey  = ol.get("market", "")
                if pname and mkey:
                    odds_lines.setdefault(pname, {})[mkey] = {
                        "line":       ol.get("line"),
                        "over_odds":  ol.get("over_odds"),
                        "under_odds": ol.get("under_odds"),
                    }
            _log(f"Real prop lines: {len(raw_lines)} from Odds API")
        except Exception as _e:
            _log(f"Prop odds skipped (non-critical): {_e}")

        # ── MLB Pitcher strikeout props ───────────────────────────────────
        # Only fetch props for games that haven't ended yet
        _active_mlb = [g for g in today_games + tomorrow_games
                       if not _game_is_over(g.get("status", ""))]
        pitcher_raw = get_starters_props_batch(_active_mlb, MLB_SEASONS[0])
        for i, p in enumerate(pitcher_raw): p["_id"] = f"prop_p_{i}"

        # ── MLB Hitter props (H, HR, TB, RBI, R, BB, SB, K, 2B) ─────────
        from data.mlb_fetcher import get_hitter_props_batch
        hitter_raw = get_hitter_props_batch(_active_mlb, MLB_SEASONS[0])
        for i, p in enumerate(hitter_raw): p["_id"] = f"prop_h_{i}"

        # ── Soccer player props (goals, assists, shots, cards, G+A) ──────
        # Primary: FBRef scrape → Fallback: The Odds API soccer player props
        soccer_props_raw: list[dict] = []
        try:
            from data.soccer_fetcher import (get_soccer_player_props_batch,
                                              get_fixtures_range, _FBREF_BLOCKED)
            import datetime as _sdt
            _cur_year = et_today().year
            _soccer_season = f"{_cur_year - 1}-{_cur_year}"
            if not _FBREF_BLOCKED:
                try:
                    _all_soccer_fixtures = get_fixtures_range(["EPL", "ESP", "GER"])
                except Exception:
                    _all_soccer_fixtures = fixtures
                soccer_props_raw = get_soccer_player_props_batch(
                    _all_soccer_fixtures,
                    season=_soccer_season,
                )
            if not soccer_props_raw:
                # FBRef blocked or returned nothing — use The Odds API directly
                _log("FBRef player stats unavailable — fetching soccer props from Odds API")
                from data.odds_fetcher import get_soccer_player_props_from_odds
                soccer_props_raw = get_soccer_player_props_from_odds(
                    league_keys=["EPL", "ESP", "GER"],
                    max_events_per_league=4,
                )
            for i, p in enumerate(soccer_props_raw):
                p["_id"] = f"prop_s_{i}"
        except Exception as _se:
            _log(f"Soccer props skipped (non-critical): {_se}")

        _log(f"Raw props: {len(pitcher_raw)} pitcher, {len(hitter_raw)} hitter, "
             f"{len(soccer_props_raw)} soccer")

        # ── Attach game date/time and build display picks ─────────────────
        # Only include props for games that are live or upcoming (not finished)
        active_sched = [g for g in today_games + tomorrow_games
                        if not _game_is_over(g.get("status", ""))]
        # Also filter soccer props whose fixture is over
        soccer_props_raw = [p for p in soccer_props_raw
                            if not _game_is_over(p.get("status", ""))]

        mlb_raw   = pitcher_raw + hitter_raw
        all_sched = active_sched
        for p in mlb_raw:
            for g in all_sched:
                if (g.get("home_team", "") in p.get("game", "") or
                        g.get("away_team", "") in p.get("game", "")):
                    p["date"]      = g.get("date", today)
                    p["game_time"] = g.get("game_time")
                    break

        # Soccer props already have date/game_time from fixture
        prop_raw = mlb_raw + soccer_props_raw

        # Confidence thresholds per sport/stat
        player_props = []
        for p in prop_raw:
            ov = float(p.get("over_prob", 0.5))
            un = float(p.get("under_prob", 0.5))
            st = p.get("stat_type", "strikeouts")
            sp = p.get("sport", "mlb")
            if sp == "soccer":
                thresh = 0.55   # show more soccer props
            elif st == "strikeouts":
                thresh = 0.52
            else:
                thresh = 0.51   # show all MLB hitter props with any edge
            if max(ov, un) >= thresh:
                player_props.append(_build_prop_pick(p, today, tomorrow, odds_lines))

        # Drop any picks for games that ended up marked FINAL
        player_props = [p for p in player_props if p.get("when_label") != "FINAL"]
        player_props.sort(key=lambda x: x["safety"], reverse=True)
        _log(f"Player props: {len(player_props)} qualifying picks")

        _phase(7)
        _log("Building game cards and parlays...")
        today_team    = sorted([_build_team_pick(b, today, tomorrow) for b in today_wins],
                               key=lambda x: x["safety"], reverse=True)
        tomorrow_team = sorted([_build_team_pick(b, today, tomorrow) for b in tomorrow_wins],
                               key=lambda x: x["safety"], reverse=True)

        team_pool = [{"_id": p["_id"],
                      "label": f"{p['pick_team']} to beat {p['opp_team']}",
                      "dec_odds": p["dec_odds"], "conf": p["conf"], "safety": p["safety"]}
                     for p in (today_team + tomorrow_team)]
        prop_pool = [{"_id": p["_id"],
                      "label": f"{p['name']} {p['direction']} {p['line']} {p.get('prop_label', 'Ks')}",
                      "dec_odds": p["dec_odds"], "conf": p["conf"], "safety": p["safety"]}
                     for p in player_props]

        prop_parlays = _build_parlays(prop_pool, max_legs=10)
        team_parlays = _build_parlays(team_pool, max_legs=6)

        # ── New: per-game cards + best combined parlays ──────────────────
        today_cards, tomorrow_cards = _build_game_cards(
            mlb_preds, soccer_preds, win_bets, player_props, today, tomorrow)
        best_parlays = _build_best_parlays(today_cards + tomorrow_cards, max_legs=10)
        _log(f"Game cards: {len(today_cards)} today, {len(tomorrow_cards)} tomorrow | "
             f"Best parlays: {len(best_parlays)} options (2-{min(10, len(best_parlays)+1)} legs)")

        _phase(8)
        upcoming_games = []
        try:
            from data.db import save_value_bets, get_upcoming_games, save_prop_picks, save_analysis_cache
            save_value_bets(win_bets, "win")
            # Save today's qualified prop picks to prop_history
            save_prop_picks(player_props)
            upcoming_games = get_upcoming_games(days_ahead=1)
            _log(f"DB saved: {len(win_bets)} bets, {len(player_props)} props, "
                 f"{len(upcoming_games)} schedule rows")
        except Exception as e:
            _log(f"DB warning: {e}")
            upcoming_games = _build_upcoming(games, fixtures)

        def _clean(obj):
            if isinstance(obj, list):  return [_clean(x) for x in obj]
            if isinstance(obj, dict):  return {k: _clean(v) for k, v in obj.items()}
            try:
                import numpy as np
                if isinstance(obj, np.integer):  return int(obj)
                if isinstance(obj, np.floating): return float(obj)
                if isinstance(obj, np.bool_):    return bool(obj)
            except ImportError:
                pass
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()
            return obj

        _log("=== Done! ===")
        _last_updated = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        _clean_cards_today    = _clean(today_cards)
        _clean_cards_tomorrow = _clean(tomorrow_cards)
        _clean_parlays        = _clean(best_parlays)
        _clean_today_team     = _clean(today_team)
        _clean_tomorrow_team  = _clean(tomorrow_team)
        _clean_props          = _clean(player_props)

        # ── Save full result to DB so reruns can skip all API calls ──
        try:
            from data.db import save_analysis_cache
            save_analysis_cache({
                "game_cards_today":    _clean_cards_today,
                "game_cards_tomorrow": _clean_cards_tomorrow,
                "best_parlays":        _clean_parlays,
                "today_team_picks":    _clean_today_team,
                "tomorrow_team_picks": _clean_tomorrow_team,
                "player_props":        _clean_props,
                "prop_parlays":        _clean(prop_parlays),
                "team_parlays":        _clean(team_parlays),
                "last_updated":        _last_updated,
                "raw_games":           _clean(games),
                "raw_fixtures":        _clean(fixtures),
                "raw_mlb_preds":       _clean(mlb_preds),
                "raw_soccer_preds":    _clean(soccer_preds),
                "raw_win_bets":        _clean(win_bets),
            })
            _log("Analysis cache saved to DB — next run will load from DB")
        except Exception as _ce:
            _log(f"Cache save skipped: {_ce}")

        with _lock:
            _state.update({
                "status": "done", "phase": "Complete",
                "last_updated":          _last_updated,
                "today_team_picks":      _clean_today_team,
                "tomorrow_team_picks":   _clean_tomorrow_team,
                "player_props":          _clean_props,
                "prop_parlays":          _clean(prop_parlays),
                "team_parlays":          _clean(team_parlays),
                "upcoming_games":        _clean(upcoming_games),
                "game_cards_today":      _clean_cards_today,
                "game_cards_tomorrow":   _clean_cards_tomorrow,
                "best_parlays":          _clean_parlays,
            })
        ok = True
    except Exception:
        err = traceback.format_exc()
        _log(f"ERROR: {err}")
        with _lock:
            _state["status"] = "error"; _state["phase"] = "Error"; _state["error"] = err
    finally:
        sys.stdout = _real
        _release_analysis_lock(lock_conn)
    return ok


def _build_upcoming(mlb_games, soccer_fixtures):
    today    = et_today().isoformat()
    tomorrow = (et_today() + datetime.timedelta(days=1)).isoformat()
    result   = []
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
    with _lock:
        state  = dict(_state)
        status = state.get("status", "idle")
    # Load DB cache for this worker to avoid empty screens and double loads.
    if status != "running":
        _load_cache_into_state(max_age_hours=22)
        with _lock:
            state  = dict(_state)
            status = state.get("status", "idle")
    # If no cache exists, kick off a single background analysis.
    if status not in ("running", "done"):
        with _lock:
            _state["status"]    = "running"
            _state["phase"]     = _PHASES[0]
            _state["phase_idx"] = 0
        threading.Thread(target=_run_analysis, daemon=True).start()
        with _lock:
            state = dict(_state)
    today    = et_today().isoformat()
    tomorrow = (et_today() + datetime.timedelta(days=1)).isoformat()
    return render_template("dashboard.html", state=state, today=today, tomorrow=tomorrow)


@app.route("/api/run", methods=["POST"])
def api_run():
    data  = request.get_json(silent=True) or {}
    force = bool(data.get("force", False))
    with _lock:
        if _state["status"] == "running":
            return jsonify({"ok": False, "msg": "Analysis already running"}), 409
        _state["status"] = "running"
        _state["phase"]  = _PHASES[0]
        _state["phase_idx"] = 0
    target = _run_analysis if force else _run_from_cache
    threading.Thread(target=target, daemon=True).start()
    return jsonify({"ok": True, "msg": "Analysis started", "from_cache": not force})


@app.route("/api/status")
def api_status():
    with _lock:
        st = _state.get("status", "idle")
    if st == "idle":
        _load_cache_into_state(max_age_hours=22)
    with _lock:
        return jsonify({k: _state[k] for k in
            ("status", "phase", "phase_idx", "phase_total", "last_updated", "error")})


@app.route("/api/logs")
def api_logs():
    since = int(request.args.get("since", 0))
    with _logs_lock:
        all_logs = list(_logs)
    return jsonify({"logs": all_logs[since:], "total": len(all_logs)})


@app.route("/api/games")
def api_games():
    try:
        from data.db import get_upcoming_games
        return jsonify(get_upcoming_games(days_ahead=1))
    except Exception:
        return jsonify([])


@app.route("/api/cached-state")
def api_cached_state():
    """
    Return the last-saved bets + props from the database so the loading
    screen can show real data while the app warms up.
    Returns a JSON object mirroring the _state structure.
    """
    today    = et_today().isoformat()
    tomorrow = (et_today() + datetime.timedelta(days=1)).isoformat()
    result = {
        "has_cache":         False,
        "last_updated":      None,
        "today_team_picks":  [],
        "tomorrow_team_picks": [],
        "player_props":      [],
        "upcoming_games":    [],
        "db_stats": {
            "total_games": 0,
            "total_bets":  0,
            "total_props": 0,
        }
    }
    try:
        from data.db import get_conn, get_upcoming_games, get_todays_prop_picks
        import psycopg2.extras

        # ── 1. Upcoming games ──
        games = get_upcoming_games(days_ahead=1)
        result["upcoming_games"] = games
        result["db_stats"]["total_games"] = len(games)

        # ── 2. Today's cached prop picks (up to 4h old) ──
        cached_props = get_todays_prop_picks(max_age_hours=4)
        if cached_props:
            result["player_props"] = cached_props
            result["db_stats"]["total_props"] = len(cached_props)
            result["has_cache"] = True

        # ── 3. Today's + tomorrow's cached value bets ──
        conn = get_conn()
        if conn:
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("""
                    SELECT sport, matchup, game_date, bet, model_prob, book_prob,
                           edge, odds_am, dec_odds, stake_usd, ev, bet_type, detected_at
                    FROM   value_bets
                    WHERE  game_date IN (%s::date, %s::date)
                      AND  detected_at > NOW() - INTERVAL '6 hours'
                    ORDER BY edge DESC NULLS LAST
                    LIMIT 60
                """, (today, tomorrow))
                bets = []
                for r in cur.fetchall():
                    d = dict(r)
                    if isinstance(d.get("game_date"), datetime.date):
                        d["game_date"] = d["game_date"].isoformat()
                    if d.get("detected_at"):
                        d["detected_at"] = d["detected_at"].isoformat()
                    # Determine when label
                    d["when_label"] = "TODAY" if d.get("game_date") == today else "TOMORROW"
                    bets.append(d)
                result["today_team_picks"]    = [b for b in bets if b.get("game_date") == today]
                result["tomorrow_team_picks"] = [b for b in bets if b.get("game_date") == tomorrow]
                result["db_stats"]["total_bets"] = len(bets)
                if bets:
                    result["has_cache"] = True
                # Get last detected_at as last_updated
                cur.execute("SELECT MAX(detected_at) FROM value_bets WHERE game_date >= CURRENT_DATE - 1")
                row = cur.fetchone()
                if row and row.get("max"):
                    ts = row["max"]
                    if hasattr(ts, "strftime"):
                        result["last_updated"] = ts.strftime("%b %d %I:%M %p ET")
                    else:
                        result["last_updated"] = str(ts)[:16]
            except Exception as e:
                print(f"[api/cached-state] value_bets query: {e}")
            finally:
                conn.close()
    except Exception as e:
        print(f"[api/cached-state] error: {e}")

    return jsonify(result)



if __name__ == "__main__":
    try:
        from data.db import init_schema
        init_schema()
    except Exception as e:
        print(f"[dashboard] DB init: {e}")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)


# ─────────────────────────────────────────────────────────────────────────────
# Scheduled morning run (6 AM ET every day)
# ─────────────────────────────────────────────────────────────────────────────

def _morning_job():
    """Full analysis + SMS blast at 6 AM ET."""
    _log("[scheduler] 6 AM ET — starting daily analysis + SMS")
    with _lock:
        if _state["status"] == "running":
            _log("[scheduler] Already running — skipping scheduled run")
            return
        _state["status"]    = "running"
        _state["phase"]     = _PHASES[0]
        _state["phase_idx"] = 0

    def _run_and_notify():
        ran = _run_analysis()
        if not ran:
            _log("[scheduler] Analysis skipped — no SMS sent")
            return
        try:
            from sms import send_daily_picks_to_all
            with _lock:
                st = dict(_state)
            result = send_daily_picks_to_all(st)
            _log(f"[scheduler] SMS sent: {result.get('sent',0)} ok, "
                 f"{result.get('failed',0)} failed")
        except Exception as _e:
            _log(f"[scheduler] SMS error: {_e}")

    threading.Thread(target=_run_and_notify, daemon=True).start()


# ─────────────────────────────────────────────────────────────────────────────
# Phone-number management endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/phone-numbers", methods=["GET"])
def api_get_phones():
    try:
        from data.db import get_phone_numbers
        return jsonify(get_phone_numbers(active_only=False))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/phone-numbers", methods=["POST"])
def api_add_phone():
    data  = request.get_json(silent=True) or {}
    phone = (data.get("phone") or "").strip()
    label = (data.get("label") or "").strip()
    if not phone:
        return jsonify({"ok": False, "msg": "phone is required"}), 400
    # Validate: only digits, +, -, spaces, parentheses allowed
    import re
    if not re.match(r"^\+?[\d\s\-().]{7,20}$", phone):
        return jsonify({"ok": False, "msg": "Invalid phone number format"}), 400
    from data.db import add_phone_number
    ok, msg = add_phone_number(phone, label)
    if not ok:
        return jsonify({"ok": False, "msg": msg or "Could not add number"}), 500
    return jsonify({"ok": True})


@app.route("/api/phone-numbers/<path:phone>", methods=["DELETE"])
def api_delete_phone(phone):
    from data.db import remove_phone_number
    ok = remove_phone_number(phone)
    return jsonify({"ok": ok})


# ─────────────────────────────────────────────────────────────────────────────
# SMS send endpoint
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/sms/send", methods=["POST"])
def api_sms_send():
    """Send the current picks via SMS to all registered numbers immediately."""
    with _lock:
        st = dict(_state)
    if st.get("status") == "running" and not st.get("game_cards_today") and not st.get("player_props"):
        # Try the DB cache if this worker doesn't have in-memory data yet
        try:
            from data.db import get_analysis_cache
            cached = get_analysis_cache(max_age_hours=22)
            if cached:
                st = cached
        except Exception as e:
            return jsonify({"ok": False, "msg": str(e)}), 500
    if st.get("status") not in ("done", "running") and not st.get("game_cards_today"):
        try:
            from data.db import get_analysis_cache
            cached = get_analysis_cache(max_age_hours=22)
            if cached:
                st = cached
            else:
                return jsonify({"ok": False, "msg": "No cached analysis yet — wait for auto-run to finish"}), 400
        except Exception as e:
            return jsonify({"ok": False, "msg": str(e)}), 500
    try:
        from sms import send_daily_picks_to_all
        result = send_daily_picks_to_all(st)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Start APScheduler (fires 6 AM ET every day)
# ─────────────────────────────────────────────────────────────────────────────

def _start_scheduler():
    if not _SCHEDULER_OK:
        print("[scheduler] APScheduler not installed — daily SMS disabled")
        return
    try:
        eastern   = pytz.timezone("America/New_York")
        scheduler = BackgroundScheduler(timezone=eastern)
        scheduler.add_job(
            _morning_job,
            CronTrigger(hour=6, minute=0, timezone=eastern),
            id="morning_analysis",
            replace_existing=True,
            misfire_grace_time=3600,
        )
        scheduler.start()
        print("[scheduler] Daily 6 AM ET job registered")
    except Exception as exc:
        print(f"[scheduler] Failed to start: {exc}")


_start_scheduler()