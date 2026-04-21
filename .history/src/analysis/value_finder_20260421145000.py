"""
Value Bet Finder + Kelly Criterion Staking
==========================================

Value bet: a bet where model probability > bookmaker implied probability.
Edge     : model_prob – implied_prob
Kelly %  : f* = (b*p - q) / b  where b = decimal_odds-1, p = model_prob, q = 1-p

Usage:
    from analysis.value_finder import find_value_bets, kelly_stake, summarise_suggestions
"""

import sys
import os
import pandas as pd
import numpy as np
from tabulate import tabulate

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import MIN_VALUE_EDGE, KELLY_FRACTION, BANKROLL
from data.odds_fetcher import american_to_prob, remove_vig


# ---------------------------------------------------------------------------
# Core math
# ---------------------------------------------------------------------------

def american_to_decimal(american_odds: float) -> float:
    """Convert American moneyline odds to decimal odds."""
    if american_odds is None or pd.isna(american_odds):
        return float("nan")
    if american_odds > 0:
        return (american_odds / 100.0) + 1
    else:
        return (100.0 / abs(american_odds)) + 1


def kelly_stake(model_prob: float, decimal_odds: float, fraction: float = KELLY_FRACTION) -> float:
    """
    Fractional Kelly criterion bet size (as fraction of bankroll).

    model_prob   : predicted win probability (0–1)
    decimal_odds : payout per unit staked (e.g. 2.50)
    fraction     : Kelly fraction (0.25 = quarter Kelly, more conservative)

    Returns: fraction of bankroll to bet (0 if negative edge)
    """
    b = decimal_odds - 1.0  # net profit per unit stake
    p = model_prob
    q = 1.0 - p
    full_kelly = (b * p - q) / b if b > 0 else 0.0
    staked = max(0.0, fraction * full_kelly)
    return round(staked, 4)


def find_value_bets(
    predictions: list[dict],
    odds_df: pd.DataFrame,
    sport: str = "mlb",
    min_edge: float = MIN_VALUE_EDGE,
) -> list[dict]:
    """
    Cross-reference model predictions with live odds to find value bets.

    predictions : list of dicts from mlb_model.predict_game() or soccer_model.predict()
      Each must have: home_team, away_team, home_win_prob (or home_win), away_win_prob (or away_win)
      Soccer also includes: draw_prob (or draw)

    odds_df : DataFrame from odds_fetcher.odds_to_dataframe()
      Columns: home_team, away_team, home_odds, away_odds, draw_odds

    min_edge : minimum probability edge to flag as a value bet

    Returns: list of value bet dicts sorted by edge descending
    """
    suggestions = []

    for pred in predictions:
        home = pred.get("home_team", "")
        away = pred.get("away_team", "")

        # Match to odds (fuzzy – find row where team names overlap)
        odds_row = _match_odds_row(home, away, odds_df)
        if odds_row is None:
            continue

        home_odds_am = odds_row.get("home_odds")
        away_odds_am = odds_row.get("away_odds")
        draw_odds_am = odds_row.get("draw_odds")

        # Convert to probabilities
        raw_home = american_to_prob(home_odds_am) if home_odds_am else None
        raw_away = american_to_prob(away_odds_am) if away_odds_am else None
        raw_draw = american_to_prob(draw_odds_am) if draw_odds_am else None

        if raw_home is None or raw_away is None:
            continue

        true_home, true_away, true_draw = remove_vig(
            raw_home, raw_away, raw_draw or 0.0
        )

        # Get model probabilities (handle both naming conventions)
        model_home = pred.get("home_win_prob") or pred.get("home_win") or 0
        model_away = pred.get("away_win_prob") or pred.get("away_win") or 0
        model_draw = pred.get("draw_prob") or pred.get("draw") or 0

        edge_home = model_home - true_home
        edge_away = model_away - true_away
        edge_draw = model_draw - true_draw if true_draw > 0 else -1

        for side, model_p, book_p, odds_am in [
            ("HOME", model_home, true_home, home_odds_am),
            ("AWAY", model_away, true_away, away_odds_am),
            ("DRAW", model_draw, true_draw, draw_odds_am),
        ]:
            if odds_am is None or pd.isna(odds_am):
                continue
            edge = model_p - book_p
            if edge < min_edge:
                continue

            dec_odds = american_to_decimal(odds_am)
            stake_frac = kelly_stake(model_p, dec_odds)
            stake_usd = round(stake_frac * BANKROLL, 2)
            expected_value = round(model_p * (dec_odds - 1) - (1 - model_p), 4)

            suggestions.append({
                "sport":       sport.upper(),
                "matchup":     f"{home} vs {away}",
                "bet":         side,
                "model_prob":  round(model_p, 4),
                "book_prob":   round(book_p, 4),
                "edge":        round(edge, 4),
                "odds_am":     odds_am,
                "dec_odds":    round(dec_odds, 2),
                "kelly_frac":  stake_frac,
                "stake_usd":   stake_usd,
                "ev":          expected_value,
            })

    return sorted(suggestions, key=lambda x: x["edge"], reverse=True)


def find_totals_bets(
    predictions: list[dict],
    totals_df: pd.DataFrame,
    sport: str = "mlb",
    min_edge: float = MIN_VALUE_EDGE,
) -> list[dict]:
    """
    Find value in over/under game total lines.

    MLB: predictions must include 'predicted_total' (expected runs from estimate_game_total).
    Soccer: predictions must include 'over_2_5' / 'under_2_5' (from Poisson model).
    """
    from scipy import stats as scipy_stats

    suggestions = []

    for pred in predictions:
        home = pred.get("home_team", "")
        away = pred.get("away_team", "")

        odds_row = _match_odds_row(home, away, totals_df)
        if odds_row is None:
            continue

        try:
            total_line    = float(odds_row.get("total_line") or 0)
            over_odds_am  = odds_row.get("over_odds")
            under_odds_am = odds_row.get("under_odds")
        except (TypeError, ValueError):
            continue

        if not total_line or over_odds_am is None or under_odds_am is None:
            continue

        # ── Derive model over/under probabilities ──────────────────────
        if sport == "mlb":
            predicted_total = pred.get("predicted_total")
            if not predicted_total:
                continue
            # Normal approximation: MLB game totals have ~2.8 run std dev
            model_over  = float(scipy_stats.norm.sf(total_line, loc=predicted_total, scale=2.8))
            model_under = 1.0 - model_over
            line_desc   = f"{total_line:.1f} runs"
            pred_desc   = f"{predicted_total:.1f} runs"

        elif sport == "soccer":
            if abs(total_line - 2.5) < 0.1:
                model_over  = float(pred.get("over_2_5", 0))
                model_under = float(pred.get("under_2_5", 1 - model_over))
            else:
                # Non-2.5 lines require full Poisson re-integration – skip for now
                continue
            line_desc = "2.5 goals"
            lh = pred.get("lambda_home", 1.5)
            la = pred.get("lambda_away", 1.2)
            pred_desc = f"{(lh + la):.1f} goals"
        else:
            continue

        # ── Compare to book probabilities ──────────────────────────────
        raw_over, raw_under = american_to_prob(over_odds_am), american_to_prob(under_odds_am)
        true_over, true_under, _ = remove_vig(raw_over, raw_under, 0.0)

        for side, model_p, book_p, odds_am in [
            ("OVER",  model_over,  true_over,  over_odds_am),
            ("UNDER", model_under, true_under, under_odds_am),
        ]:
            edge = model_p - book_p
            if edge < min_edge:
                continue

            dec_odds  = american_to_decimal(odds_am)
            stake_frac = kelly_stake(model_p, dec_odds)
            stake_usd  = round(stake_frac * BANKROLL, 2)
            ev         = round(model_p * (dec_odds - 1) - (1 - model_p), 4)

            suggestions.append({
                "sport":           sport.upper(),
                "matchup":         f"{home} vs {away}",
                "bet":             side,
                "bet_desc":        f"{side} {line_desc}",
                "model_prob":      round(model_p, 4),
                "book_prob":       round(book_p, 4),
                "edge":            round(edge, 4),
                "total_line":      total_line,
                "predicted_total": pred_desc,
                "odds_am":         odds_am,
                "dec_odds":        round(dec_odds, 2),
                "kelly_frac":      stake_frac,
                "stake_usd":       stake_usd,
                "ev":              ev,
            })

    return sorted(suggestions, key=lambda x: x["edge"], reverse=True)



    """Fuzzy-match a prediction's teams to a row in the odds DataFrame."""
    if odds_df.empty:
        return None
    for _, row in odds_df.iterrows():
        r_home = str(row.get("home_team", "")).lower()
        r_away = str(row.get("away_team", "")).lower()
        h = home.lower()
        a = away.lower()
        # Check if any part of the name matches
        if (_partial(h, r_home) and _partial(a, r_away)) or \
           (_partial(h, r_away) and _partial(a, r_home)):
            return row.to_dict()
    return None


def _partial(a: str, b: str) -> bool:
    """Returns True if either string is contained in the other."""
    a_words = set(a.split())
    b_words = set(b.split())
    return bool(a_words & b_words) or a in b or b in a


# ---------------------------------------------------------------------------
# Parlay / Same-Game Multi builder
# ---------------------------------------------------------------------------

def build_parlay(
    value_bets: list[dict],
    max_legs: int = 4,
    min_leg_prob: float = 0.55,
) -> list[dict]:
    """
    Suggest multi-leg parlays from the top value bets.

    Filters legs where model_prob >= min_leg_prob, then combines up to
    max_legs legs. Assumes independence (simplified).

    Returns list of parlay dicts:
      {legs, combined_prob, combined_dec_odds, ev}
    """
    eligible = [b for b in value_bets if b["model_prob"] >= min_leg_prob]
    if len(eligible) < 2:
        return []

    from itertools import combinations
    parlays = []
    for r in range(2, min(max_legs, len(eligible)) + 1):
        for combo in combinations(eligible, r):
            combined_prob = 1.0
            combined_dec = 1.0
            legs = []
            for leg in combo:
                combined_prob *= leg["model_prob"]
                combined_dec *= leg["dec_odds"]
                legs.append(f"{leg['matchup']} [{leg['bet']}]")
            ev = round(combined_prob * (combined_dec - 1) - (1 - combined_prob), 4)
            parlays.append({
                "legs": legs,
                "num_legs": r,
                "combined_prob": round(combined_prob, 4),
                "combined_dec_odds": round(combined_dec, 2),
                "ev": ev,
                "positive_ev": ev > 0,
            })

    return sorted(parlays, key=lambda x: x["ev"], reverse=True)


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def summarise_suggestions(value_bets: list[dict], parlays: list[dict] | None = None) -> str:
    """Format value bets and optional parlays as a readable report."""
    lines = []
    lines.append("=" * 70)
    lines.append("  VALUE BETS REPORT")
    lines.append("=" * 70)

    if not value_bets:
        lines.append("  No value bets found with current settings.")
    else:
        table = [
            [
                b["sport"],
                b["matchup"][:35],
                b["bet"],
                f"{b['model_prob']:.1%}",
                f"{b['book_prob']:.1%}",
                f"+{b['edge']:.1%}",
                b["odds_am"],
                f"${b['stake_usd']:.2f}",
                f"{b['ev']:+.4f}",
            ]
            for b in value_bets
        ]
        headers = ["Sport", "Matchup", "Bet", "Model%", "Book%", "Edge", "Odds", f"Stake(${BANKROLL:.0f})", "EV"]
        lines.append(tabulate(table, headers=headers, tablefmt="rounded_outline"))

    if parlays:
        lines.append("")
        lines.append("=" * 70)
        lines.append("  PARLAY SUGGESTIONS  (top 5 by EV)")
        lines.append("=" * 70)
        for p in parlays[:5]:
            ev_str = f"+{p['ev']:.4f}" if p["ev"] > 0 else f"{p['ev']:.4f}"
            lines.append(f"  {p['num_legs']}-leg | Prob: {p['combined_prob']:.1%} | Odds: {p['combined_dec_odds']:.2f}x | EV: {ev_str}")
            for leg in p["legs"]:
                lines.append(f"    - {leg}")
            lines.append("")

    lines.append("=" * 70)
    lines.append(f"  Bankroll: ${BANKROLL:.2f}  |  Min edge: {MIN_VALUE_EDGE:.1%}  |  Kelly fraction: {KELLY_FRACTION}")
    lines.append("=" * 70)
    return "\n".join(lines)
