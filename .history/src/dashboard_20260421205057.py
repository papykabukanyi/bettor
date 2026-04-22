"""Betting Bot - Web Dashboard v3"""
import os, sys, datetime, threading, traceback
from collections import deque
from flask import Flask, render_template, jsonify, request

SRC_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SRC_DIR)
from config import MIN_VALUE_EDGE, KELLY_FRACTION, MLB_SEASONS

app = Flask(__name__, template_folder="templates")

_PHASES = [
    "Fetching MLB schedule",
    "Fetching soccer fixtures",
    "Loading stats & models",
    "Fetching injury reports",
    "Fetching live odds",
    "Running team win algorithm",
    "Analyzing player props",
    "Building parlays",
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
}
_lock = threading.Lock()

_logs = deque(maxlen=400)
_logs_lock = threading.Lock()


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


def _phase(idx, name=""):
    label = name or (_PHASES[idx] if idx < len(_PHASES) else name)
    with _lock:
        _state["phase"]     = label
        _state["phase_idx"] = idx
    _log(f"Phase {idx+1}/{len(_PHASES)}: {label}")


def _run_analysis():
    import warnings; warnings.filterwarnings("ignore")
    import pandas as pd
    _real = sys.stdout
    sys.stdout = _StdoutCapture(_real)
    with _lock:
        _state["status"] = "running"; _state["error"] = None
        _state["phase"]  = _PHASES[0]; _state["phase_idx"] = 0
    with _logs_lock:
        _logs.clear()
    _log("=== Analysis started ===")
    try:
        today    = datetime.date.today().isoformat()
        tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()

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
        fixtures = get_todays_fixtures(["EPL", "ESP", "GER"])
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
            raw_lines = get_player_props_odds("mlb", max_events=15)
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
        soccer_props_raw: list[dict] = []
        try:
            from data.soccer_fetcher import get_soccer_player_props_batch, get_fixtures_range
            import datetime as _sdt
            _cur_year = _sdt.date.today().year
            _soccer_season = f"{_cur_year - 1}-{_cur_year}"
            # Use today+tomorrow range; fallback to today-only fixtures
            try:
                _all_soccer_fixtures = get_fixtures_range(["EPL", "ESP", "GER"])
            except Exception:
                _all_soccer_fixtures = fixtures  # today only
            soccer_props_raw = get_soccer_player_props_batch(
                _all_soccer_fixtures,
                season=_soccer_season,
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
                thresh = 0.60   # stricter for soccer (binomial near-certainty props)
            elif st == "strikeouts":
                thresh = 0.54
            else:
                thresh = 0.52
            if max(ov, un) >= thresh:
                player_props.append(_build_prop_pick(p, today, tomorrow, odds_lines))

        # Drop any picks for games that ended up marked FINAL
        player_props = [p for p in player_props if p.get("when_label") != "FINAL"]
        player_props.sort(key=lambda x: x["safety"], reverse=True)
        _log(f"Player props: {len(player_props)} qualifying picks")

        _phase(7)
        _log("Building 2-10 leg parlays...")
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
        _log(f"Parlays: {sum(len(v) for v in prop_parlays.values())} prop, "
             f"{sum(len(v) for v in team_parlays.values())} team")

        _phase(8)
        upcoming_games = []
        try:
            from data.db import save_value_bets, get_upcoming_games, save_prop_picks
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
        with _lock:
            _state.update({
                "status": "done", "phase": "Complete",
                "last_updated":        datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
                "today_team_picks":    _clean(today_team),
                "tomorrow_team_picks": _clean(tomorrow_team),
                "player_props":        _clean(player_props),
                "prop_parlays":        _clean(prop_parlays),
                "team_parlays":        _clean(team_parlays),
                "upcoming_games":      _clean(upcoming_games),
            })
    except Exception:
        err = traceback.format_exc()
        _log(f"ERROR: {err}")
        with _lock:
            _state["status"] = "error"; _state["phase"] = "Error"; _state["error"] = err
    finally:
        sys.stdout = _real


def _build_upcoming(mlb_games, soccer_fixtures):
    today    = datetime.date.today().isoformat()
    tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()
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
    with _lock: state = dict(_state)
    today    = datetime.date.today().isoformat()
    tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()
    return render_template("dashboard.html", state=state, today=today, tomorrow=tomorrow)


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


if __name__ == "__main__":
    try:
        from data.db import init_schema
        init_schema()
    except Exception as e:
        print(f"[dashboard] DB init: {e}")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
