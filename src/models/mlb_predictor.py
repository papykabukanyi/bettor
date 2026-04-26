"""
MLB Predictor — All Bet Types
==============================
Generates predictions for every MLB bet type:
  - Moneyline (home/away win)
  - Run line (spread ±1.5)
  - Game total (over/under)
  - First 5 innings (F5) moneyline + total
  - Team totals (over/under)
  - Player props: HR, Hits, Total Bases, RBI, Runs, Walks, SB, K (batter), K (pitcher)
  - Parlays: auto-generated best combinations

Integrates:
  - MLB Stats API / pybaseball stats
  - The Odds API real book lines
  - Sentiment scores (Reddit + News + HF)
  - Injury report filter (removes injured players)
  - Historical player trends from DB
  - GBM model for game win probability
"""

import os
import sys
import math
import datetime
import json
import warnings
from typing import Optional

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SRC)

from config import MIN_VALUE_EDGE, KELLY_FRACTION, BANKROLL, MLB_SEASONS, et_today


# ─── Safety helpers ──────────────────────────────────────────────────────────

def _safety_score(model_prob: float, edge: float, book_prob: float = None) -> float:
    prob_score  = min(float(model_prob or 0.5), 0.92)
    edge_norm   = min(max(float(edge or 0), 0.0), 0.30) / 0.30
    consistency = 1.0 - abs(float(model_prob or 0.5) -
                             float(book_prob or model_prob or 0.5)) * 2.0
    consistency = max(0.0, min(1.0, consistency))
    return round(prob_score * 0.50 + edge_norm * 0.30 + consistency * 0.20, 4)


def _safety_label(score: float) -> str:
    if score >= 0.72: return "ELITE"
    if score >= 0.60: return "SAFE"
    if score >= 0.50: return "MODERATE"
    return "RISKY"


def _am_odds_to_dec(am: int) -> float:
    if am is None: return 2.0
    try:
        am = int(am)
        return round(am / 100 + 1, 4) if am > 0 else round(100 / abs(am) + 1, 4)
    except Exception:
        return 2.0


def _dec_to_prob(dec: float) -> float:
    if not dec or dec <= 1: return 0.5
    return round(1.0 / dec, 4)


def _remove_vig(p1: float, p2: float) -> tuple:
    total = p1 + p2
    if total <= 0: return 0.5, 0.5
    return round(p1 / total, 4), round(p2 / total, 4)


def _kelly(p: float, dec_odds: float, fraction: float = KELLY_FRACTION) -> float:
    """Kelly criterion stake as fraction of bankroll."""
    q = 1.0 - p
    b = dec_odds - 1.0
    if b <= 0: return 0.0
    k = (b * p - q) / b
    return round(max(0.0, k * fraction), 4)


def _norm_sf(line: float, mean: float, std: float) -> float:
    """P(X > line) using normal approximation."""
    from scipy.stats import norm
    return float(norm.sf(line, loc=mean, scale=max(std, 0.01)))


# ─── Game-level predictions ───────────────────────────────────────────────────

def predict_game(home_team: str, away_team: str, team_stats: pd.DataFrame,
                 model, sentiment: dict = None,
                 injuries: list = None) -> dict:
    """
    Generate all game-level bets for one matchup.

    Returns dict with:
      moneyline, run_line, total, f5_moneyline, f5_total,
      home_team_total, away_team_total, game_key, home_team, away_team
    """
    from models.mlb_model import predict_from_season_stats

    pred   = predict_from_season_stats(home_team, away_team, team_stats, model)
    hw_raw = float(pred.get("home_win_prob", 0.5))
    aw_raw = 1.0 - hw_raw

    # Sentiment adjustment (max ±3% swing)
    if sentiment:
        home_sent = float((sentiment.get("home") or {}).get("combined", 0))
        away_sent = float((sentiment.get("away") or {}).get("combined", 0))
        adjust    = (home_sent - away_sent) * 0.03
        hw_prob   = max(0.05, min(0.95, hw_raw + adjust))
        aw_prob   = 1.0 - hw_prob
    else:
        hw_prob, aw_prob = hw_raw, aw_raw

    # Injury adjustment: reduce confidence if key players are out
    inj_penalty = 0.0
    if injuries:
        home_injured = [i for i in injuries
                        if home_team.lower() in i.get("team", "").lower()
                        and i.get("status", "").lower() in ("out", "il", "dl", "dtd")]
        away_injured = [i for i in injuries
                        if away_team.lower() in i.get("team", "").lower()
                        and i.get("status", "").lower() in ("out", "il", "dl", "dtd")]
        # Roughly 1% per key player out (max 5%)
        inj_penalty = min(0.05, len(home_injured) * 0.01 - len(away_injured) * 0.01)
        hw_prob = max(0.05, min(0.95, hw_prob - inj_penalty))
        aw_prob = 1.0 - hw_prob

    # Expected total (runs)
    def _row(t):
        mask = team_stats["team"].str.contains(t, case=False, na=False)
        rows = team_stats[mask]
        return rows.sort_values("season", ascending=False).iloc[0] if not rows.empty else None

    home_row = _row(home_team)
    away_row = _row(away_team)
    MLB_GAMES = 162
    MLB_AVG   = 9.0
    if home_row is not None and away_row is not None:
        home_rpg = float(home_row.get("runs_scored", 700) or 700) / MLB_GAMES
        away_rpg = float(away_row.get("runs_scored", 700) or 700) / MLB_GAMES
        home_era = float(home_row.get("era", 4.5) or 4.5)
        away_era = float(away_row.get("era", 4.5) or 4.5)
        home_exp = (home_rpg + away_era * 9 / MLB_GAMES) / 2.0
        away_exp = (away_rpg + home_era * 9 / MLB_GAMES) / 2.0
        exp_total = round(home_exp + away_exp, 2)
    else:
        exp_total = MLB_AVG
        home_exp  = exp_total / 2.0
        away_exp  = exp_total / 2.0

    # Run-line: home -1.5 win probability ≈ hw_prob - 0.12 (covers spread)
    home_rl_prob = max(0.05, hw_prob - 0.12)
    away_rl_prob = max(0.05, aw_prob - 0.12)

    # F5 win probability ≈ 80% of full-game (5 innings is tighter variance)
    f5_hw = max(0.10, 0.50 + (hw_prob - 0.50) * 0.80)
    f5_aw = 1.0 - f5_hw
    f5_exp_total = round(exp_total * 5 / 9, 2)

    return {
        "game_key":       f"{away_team}@{home_team}",
        "home_team":      home_team,
        "away_team":      away_team,
        "home_win_prob":  round(hw_prob, 4),
        "away_win_prob":  round(aw_prob, 4),
        "exp_total":      exp_total,
        "home_exp_runs":  round(home_exp, 2),
        "away_exp_runs":  round(away_exp, 2),
        # Spread
        "home_rl_prob":   round(home_rl_prob, 4),
        "away_rl_prob":   round(away_rl_prob, 4),
        # F5
        "f5_home_prob":   round(f5_hw, 4),
        "f5_away_prob":   round(f5_aw, 4),
        "f5_exp_total":   f5_exp_total,
        # Meta
        "sentiment":      sentiment or {},
        "inj_penalty":    round(inj_penalty, 3),
    }


# ─── Build bet picks from game prediction + odds ─────────────────────────────

_BET_TYPES = ["moneyline", "run_line", "total", "f5_moneyline", "f5_total",
              "home_team_total", "away_team_total"]

_BET_LABELS = {
    "moneyline":        "Moneyline",
    "run_line":         "Run Line (±1.5)",
    "total":            "Total Runs",
    "f5_moneyline":     "F5 Moneyline",
    "f5_total":         "F5 Total",
    "home_team_total":  "Home Team Total",
    "away_team_total":  "Away Team Total",
    "player_prop":      "Player Prop",
}


def build_game_bets(game: dict, pred: dict, odds_row: dict = None) -> list[dict]:
    """
    Given game metadata + model prediction + optional odds row,
    produce a list of bet dicts for each market type.

    odds_row: dict from odds_to_dataframe or None → uses implied fair odds
    """
    bets = []
    ht   = pred["home_team"]
    at   = pred["away_team"]
    gk   = pred["game_key"]
    gd   = game.get("date", str(et_today()))
    gt   = game.get("game_time", "")
    hs   = game.get("home_starter", "TBD")
    aws  = game.get("away_starter", "TBD")

    def _bet(bet_type: str, pick: str, model_prob: float, book_prob: float,
              odds_am: int = None, line: float = None) -> dict:
        edge       = model_prob - book_prob
        if edge < MIN_VALUE_EDGE:
            return None
        dec_odds   = (_am_odds_to_dec(odds_am) if odds_am
                      else round(1.0 / max(book_prob, 0.01), 3))
        safety     = _safety_score(model_prob, edge, book_prob)
        stake_pct  = _kelly(model_prob, dec_odds)
        return {
            "game_key":       gk,
            "sport":          "mlb",
            "bet_type":       bet_type,
            "pick":           pick,
            "line":           line,
            "odds_am":        odds_am,
            "dec_odds":       dec_odds,
            "model_prob":     round(model_prob, 4),
            "book_prob":      round(book_prob, 4),
            "edge":           round(edge, 4),
            "confidence":     round(model_prob * 100),
            "safety":         safety,
            "safety_label":   _safety_label(safety),
            "stake_pct":      stake_pct,
            "stake_usd":      round(BANKROLL * stake_pct, 2),
            "ev":             round((dec_odds - 1) * model_prob - (1 - model_prob), 4),
            "game_date":      gd,
            "game_time":      gt,
            "home_team":      ht,
            "away_team":      at,
            "home_starter":   hs,
            "away_starter":   aws,
            "matchup":        f"{ht} vs {at}",
            "_bet_label":     _BET_LABELS.get(bet_type, bet_type),
        }

    # ── Extract real odds ────────────────────────────────────────────────
    if odds_row and not (isinstance(odds_row, pd.DataFrame) and odds_row.empty):
        # Moneyline
        if isinstance(odds_row, dict):
            h_ml = odds_row.get("home_odds_am") or odds_row.get("home")
            a_ml = odds_row.get("away_odds_am") or odds_row.get("away")
        else:
            try:
                h_ml = float(odds_row.get("home_odds_am", 0))
                a_ml = float(odds_row.get("away_odds_am", 0))
            except Exception:
                h_ml = a_ml = None

        if h_ml and a_ml:
            h_bp, a_bp = _remove_vig(_dec_to_prob(_am_odds_to_dec(int(h_ml))),
                                      _dec_to_prob(_am_odds_to_dec(int(a_ml))))
            b = _bet("moneyline", f"{ht} ML", pred["home_win_prob"],
                     h_bp, int(h_ml))
            if b: bets.append(b)
            b = _bet("moneyline", f"{at} ML", pred["away_win_prob"],
                     a_bp, int(a_ml))
            if b: bets.append(b)

        # Total
        total_line = odds_row.get("total_line") if isinstance(odds_row, dict) else None
        if total_line:
            exp    = pred["exp_total"]
            ov_p   = _norm_sf(float(total_line), exp, 2.5)
            un_p   = 1.0 - ov_p
            b = _bet("total", f"OVER {total_line}", ov_p, 0.476, line=float(total_line))
            if b: bets.append(b)
            b = _bet("total", f"UNDER {total_line}", un_p, 0.476, line=float(total_line))
            if b: bets.append(b)
    else:
        # No real odds — use model-implied probabilities with vig estimate
        bp_h = 0.476  # ~52.4 implied (vig subtracted)
        bp_a = 0.476
        b = _bet("moneyline", f"{ht} ML", pred["home_win_prob"], bp_h)
        if b: bets.append(b)
        b = _bet("moneyline", f"{at} ML", pred["away_win_prob"], bp_a)
        if b: bets.append(b)

        # Total
        exp   = pred["exp_total"]
        line  = round(exp * 2) / 2  # nearest 0.5
        ov_p  = _norm_sf(line, exp, 2.5)
        un_p  = 1.0 - ov_p
        b = _bet("total", f"OVER {line}", ov_p, 0.476, line=line)
        if b: bets.append(b)
        b = _bet("total", f"UNDER {line}", un_p, 0.476, line=line)
        if b: bets.append(b)

    # ── Run line (always model-based unless odds provided) ───────────────
    rl_bp = 0.476
    b = _bet("run_line", f"{ht} -1.5", pred["home_rl_prob"], rl_bp)
    if b: bets.append(b)
    b = _bet("run_line", f"{at} +1.5", pred["away_rl_prob"], rl_bp)
    if b: bets.append(b)

    # ── F5 Moneyline ──────────────────────────────────────────────────────
    b = _bet("f5_moneyline", f"{ht} F5", pred["f5_home_prob"], 0.476)
    if b: bets.append(b)
    b = _bet("f5_moneyline", f"{at} F5", pred["f5_away_prob"], 0.476)
    if b: bets.append(b)

    # ── F5 Total ──────────────────────────────────────────────────────────
    f5exp  = pred["f5_exp_total"]
    f5line = round(f5exp * 2) / 2
    f5ov   = _norm_sf(f5line, f5exp, 1.5)
    f5un   = 1.0 - f5ov
    b = _bet("f5_total", f"F5 OVER {f5line}", f5ov, 0.476, line=f5line)
    if b: bets.append(b)
    b = _bet("f5_total", f"F5 UNDER {f5line}", f5un, 0.476, line=f5line)
    if b: bets.append(b)

    # ── Team totals ───────────────────────────────────────────────────────
    for side, exp_r in [("home", pred["home_exp_runs"]), ("away", pred["away_exp_runs"])]:
        team_name = ht if side == "home" else at
        t_line    = round(exp_r * 2) / 2
        t_ov      = _norm_sf(t_line, exp_r, 1.5)
        t_un      = 1.0 - t_ov
        bt        = f"{side}_team_total"
        b = _bet(bt, f"{team_name} OVER {t_line}", t_ov, 0.476, line=t_line)
        if b: bets.append(b)

    return bets


# ─── Player prop predictions ─────────────────────────────────────────────────

def build_player_prop_bets(raw_props: list[dict], injured_players: set = None,
                            odds_lines: dict = None) -> list[dict]:
    """
    Convert raw props from mlb_fetcher into full bet dicts.

    raw_props: output of get_starters_props_batch + get_hitter_props_batch
    injured_players: set of player names to exclude
    odds_lines: {player_name: {market_key: {line, over_odds, under_odds}}}
    Returns list of bet dicts.
    """
    injured = injured_players or set()
    bets    = []

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
    _PROP_LABELS = {
        "strikeouts":         "Pitcher Strikeouts",
        "hits":               "Batter Hits",
        "home_runs":          "Home Runs",
        "total_bases":        "Total Bases",
        "rbi":                "RBI",
        "runs":               "Runs Scored",
        "walks":              "Walks (BB)",
        "stolen_bases":       "Stolen Bases",
        "batter_strikeouts":  "Batter Strikeouts",
        "doubles":            "Doubles",
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
    }

    # Import signal generator (lazy — avoid circular import at module level)
    try:
        from data.sentiment import get_player_prop_signal as _prop_signal
        _SIGNAL_OK = True
    except Exception:
        _SIGNAL_OK = False

    for p in raw_props:
        name = p.get("name", "")
        # Skip injured players
        if any(name.lower() in inj.lower() or inj.lower() in name.lower()
               for inj in injured if inj):
            continue

        st        = p.get("stat_type", "strikeouts")
        raw_ov    = float(p.get("over_prob",  0.5))
        raw_un    = float(p.get("under_prob", 0.5))

        # ── Get real book line/odds first so we pass the real line to signal ──
        real_line      = p.get("line", "?")
        real_over_odds = real_under_odds = None
        if odds_lines:
            mkey  = _MARKET_MAP.get(st, "")
            pdata = odds_lines.get(name, {}).get(mkey, {})
            if pdata:
                real_line        = pdata.get("line", real_line)
                real_over_odds   = pdata.get("over_odds")
                real_under_odds  = pdata.get("under_odds")

        # ── Run historical + sentiment signal ─────────────────────────────
        signal: dict = {}
        if _SIGNAL_OK:
            try:
                numeric_line = float(real_line) if real_line not in ("?", None) else float(p.get("line", 0.5))
                signal = _prop_signal(
                    player_name  = name,
                    stat_type    = st,
                    line         = numeric_line,
                    prop_data    = p,
                    pitcher_hand = p.get("pitcher_hand"),   # "L"/"R" if known
                    venue        = p.get("venue"),           # "home"/"away" if known
                )
            except Exception as _se:
                print(f"[predictor] prop signal error for {name}: {_se}")

        # ── Use signal probability when available, else raw model ─────────
        if signal:
            # Signal is the primary source — it already blends history + sentiment
            sig_ov    = float(signal["probability"])          # P(OVER)
            sig_un    = 1.0 - sig_ov
            direction = signal["direction"]
            conf      = signal["confidence"]
            ov        = sig_ov
            un        = sig_un
        else:
            ov        = raw_ov
            un        = raw_un
            direction = "OVER" if ov >= un else "UNDER"
            conf      = round(max(ov, un) * 100)

        edge = max(ov, un) - 0.5
        if max(ov, un) < 0.51:
            continue

        # ── Book probability from real odds (or model-implied) ────────────
        if real_over_odds and direction == "OVER":
            book_odds_am = int(real_over_odds)
            dec          = _am_odds_to_dec(book_odds_am)
            book_prob    = _dec_to_prob(dec)
        elif real_under_odds and direction == "UNDER":
            book_odds_am = int(real_under_odds)
            dec          = _am_odds_to_dec(book_odds_am)
            book_prob    = _dec_to_prob(dec)
        else:
            book_odds_am = None
            book_prob    = 0.476
            dec          = round(1.0 / max(max(ov, un), 0.01), 3)

        safety = _safety_score(max(ov, un), edge, book_prob)

        bets.append({
            # Identity
            "game_key":          p.get("game", ""),
            "sport":             "mlb",
            "bet_type":          "player_prop",
            # Display
            "name":              name,
            "team":              p.get("team", ""),
            "game":              p.get("game", ""),
            "stat_type":         st,
            "prop_label":        _PROP_LABELS.get(st, st.replace("_", " ").title()),
            "rate_label":        _RATE_LABELS.get(st, "Avg/Game"),
            "direction":         direction,
            "line":              real_line,
            "avg_per_game":      round(float(p.get("avg_per_game", 0)), 3),
            # Odds
            "odds_am":           book_odds_am,
            "dec_odds":          dec,
            "over_odds_am":      real_over_odds,
            "under_odds_am":     real_under_odds,
            # Probabilities (signal-adjusted when available)
            "over_prob":         round(ov, 4),
            "under_prob":        round(un, 4),
            "over_pct":          round(ov * 100),
            "under_pct":         round(un * 100),
            "raw_over_prob":     round(raw_ov, 4),   # original model before signal
            "raw_under_prob":    round(raw_un, 4),
            "conf":              conf,
            "model_prob":        round(max(ov, un), 4),
            "confidence":        conf,
            "edge":              round(edge, 4),
            "safety":            safety,
            "safety_label":      _safety_label(safety),
            "ev":                round((dec - 1) * max(ov, un) - (1 - max(ov, un)), 4),
            # Signal details (what drove the recommendation)
            "signal_rationale":  signal.get("rationale", ""),
            "signal_hist_prob":  signal.get("hist_prob", round(raw_ov, 4)),
            "signal_sentiment":  signal.get("sentiment_score", 0.0),
            "signal_sources":    signal.get("data_sources", []),
            # Stats for display
            "era":               round(float(p.get("era", 0)), 2),
            "xfip":              round(float(p.get("xfip", p.get("era", 0))), 2),
            "k9":                round(float(p.get("k9", 0)), 1),
            "k_pct":             round(float(p.get("k_pct", 0)), 1),
            "whip":              round(float(p.get("whip", 0)), 2),
            "avg_ks":            round(float(p.get("avg_per_game", 0)), 1),
            "avg":               round(float(p.get("avg", 0)), 3),
            "ops":               round(float(p.get("ops", 0)), 3),
            "wrc_plus":          round(float(p.get("wrc_plus", 0))),
            "ip_per_start":      round(float(p.get("ip_per_start", 0)), 1),
            # Timing
            "date":              p.get("date", str(et_today())),
            "game_time":         p.get("game_time", ""),
        })

    bets.sort(key=lambda x: x["safety"], reverse=True)
    return bets


# ─── Parlay builder ──────────────────────────────────────────────────────────

def build_parlays(all_picks: list[dict], max_legs: int = 8, top_n: int = 5) -> list[dict]:
    """
    Build best parlays from all picks (game + prop bets).
    One pick per game (avoids correlated legs within the same game).
    Returns list sorted by combined expected value.
    """
    from itertools import combinations

    # De-duplicate: one best pick per game_key
    by_game: dict[str, dict] = {}
    for pick in all_picks:
        gk = pick.get("game_key", pick.get("game", ""))
        if gk not in by_game or pick.get("safety", 0) > by_game[gk].get("safety", 0):
            by_game[gk] = pick

    pool = sorted(by_game.values(), key=lambda x: x.get("safety", 0), reverse=True)[:20]
    if len(pool) < 2:
        return []

    results = []
    for n in range(2, min(max_legs + 1, len(pool) + 1)):
        best_combos = []
        for combo in combinations(pool, n):
            comb_p = 1.0
            for c in combo: comb_p *= float(c.get("model_prob", 0.5))
            comb_d = 1.0
            for c in combo: comb_d *= float(c.get("dec_odds", 2.0))
            avg_s  = sum(c.get("safety", 0.5) for c in combo) / n
            score  = comb_p * avg_s
            best_combos.append({
                "n_legs":        n,
                "legs": [{
                    "label":     c.get("pick") or f"{c.get('name','')} {c.get('direction','')} {c.get('line','')} {c.get('prop_label','')}".strip(),
                    "bet_type":  c.get("bet_type", ""),
                    "conf":      c.get("confidence", round(float(c.get("model_prob",0.5))*100)),
                    "badge":     c.get("safety_label", "MODERATE"),
                    "game":      c.get("game_key", c.get("game", "")),
                    "dec_odds":  round(float(c.get("dec_odds", 2.0)), 2),
                } for c in combo],
                "combined_prob": round(comb_p * 100, 1),
                "combined_dec":  round(comb_d, 2),
                "avg_safety":    round(avg_s, 3),
                "safety_label":  _safety_label(avg_s),
                "score":         round(score, 5),
                "payout_100":    round(comb_d * 100, 0),
            })
        best_combos.sort(key=lambda x: x["score"], reverse=True)
        results.extend(best_combos[:top_n])

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_n * max_legs]


# ─── Outcome resolution ──────────────────────────────────────────────────────

def resolve_game_outcomes(days_back: int = 3) -> int:
    """
    Check completed games and update PENDING predictions to WIN/LOSS.
    Called by the scheduled job (daily, a few hours after last game).
    Returns count of resolved predictions.
    """
    try:
        import statsapi as mlbstatsapi
    except ImportError:
        return 0

    try:
        from data.db import get_predictions, update_prediction_outcome, get_conn
    except Exception:
        return 0

    resolved = 0
    today = et_today()
    for delta in range(1, days_back + 1):
        check_date = today - datetime.timedelta(days=delta)
        date_str   = check_date.isoformat()
        try:
            schedule = mlbstatsapi.schedule(start_date=date_str, end_date=date_str)
        except Exception:
            continue

        for game in schedule:
            status = game.get("status", "")
            if "final" not in status.lower() and "completed" not in status.lower():
                continue
            ht   = game.get("home_name", "")
            at   = game.get("away_name", "")
            gk   = f"{at}@{ht}"
            h_sc = int(game.get("home_score", 0) or 0)
            a_sc = int(game.get("away_score", 0) or 0)
            total = h_sc + a_sc
            result_str = f"{at} {a_sc} @ {ht} {h_sc}"

            # Fetch pending predictions for this game
            conn = get_conn()
            if not conn:
                continue
            try:
                import psycopg2.extras
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("""
                    SELECT id, bet_type, pick, line FROM predictions
                    WHERE game_key = %s AND game_date = %s AND outcome = 'PENDING'
                """, (gk, date_str))
                pending = cur.fetchall()
            except Exception:
                conn.close()
                continue

            for pred in pending:
                try:
                    bet_type = pred["bet_type"]
                    pick     = pred["pick"] or ""
                    line     = float(pred["line"] or 0)
                    outcome  = "LOSS"

                    if bet_type == "moneyline":
                        winner = ht if h_sc > a_sc else at
                        outcome = "WIN" if winner in pick else "LOSS"

                    elif bet_type == "run_line":
                        if f"{ht} -1.5" in pick:
                            outcome = "WIN" if (h_sc - a_sc) > 1.5 else "LOSS"
                        elif f"{at} +1.5" in pick:
                            outcome = "WIN" if (a_sc - h_sc) > -1.5 else "LOSS"

                    elif bet_type in ("total", "f5_total"):
                        if "OVER" in pick:
                            outcome = "WIN" if total > line else ("PUSH" if total == line else "LOSS")
                        else:
                            outcome = "WIN" if total < line else ("PUSH" if total == line else "LOSS")

                    cur.execute("""
                        UPDATE predictions
                        SET outcome = %s, actual_result = %s, resolved_at = NOW()
                        WHERE id = %s
                    """, (outcome, result_str, pred["id"]))
                    resolved += 1
                except Exception:
                    pass

            try:
                conn.commit()
            except Exception:
                conn.rollback()
            conn.close()

    print(f"[mlb_predictor] Resolved {resolved} predictions")
    return resolved
