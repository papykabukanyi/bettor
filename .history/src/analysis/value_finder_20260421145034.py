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

def summarise_suggestions(
    value_bets: list[dict],
    parlays: list[dict] | None = None,
    totals_bets: list[dict] | None = None,
    prop_stats: list[dict] | None = None,
) -> str:
    """
    Format value bets, totals, player props and parlays as a clear daily report.

    value_bets   : from find_value_bets()
    parlays      : from build_parlay()
    totals_bets  : from find_totals_bets()
    prop_stats   : from get_starters_props_batch() – pitcher analysis for today's starters
    """
    import datetime

    W = 70  # line width

    def _line(char="─"):
        return char * W

    def _grade(edge: float) -> str:
        if edge >= 0.15: return "A+"
        if edge >= 0.10: return "A "
        if edge >= 0.07: return "B "
        return "C "

    def _bet_block(b: dict, mode: str = "win") -> list[str]:
        """Render one bet as a readable block."""
        g      = _grade(b["edge"])
        sport  = b["sport"]
        odds_i = int(b["odds_am"])
        odds_s = f"+{odds_i}" if odds_i > 0 else str(odds_i)

        dec    = b["dec_odds"]
        payout = round((dec - 1) * b["stake_usd"], 2)
        ev_s   = f"{b['ev']:+.3f}"

        if mode == "win":
            parts  = b["matchup"].split(" vs ")
            home_t, away_t = (parts[0], parts[1]) if len(parts) == 2 else (b["matchup"], "?")
            if b["bet"] == "HOME":
                pick = f"  BET:  {home_t}  to WIN  (home)"
            elif b["bet"] == "AWAY":
                pick = f"  BET:  {away_t}  to WIN  (away)"
            else:
                pick = f"  BET:  DRAW  in  {b['matchup']}"
        else:
            pick = f"  BET:  {b['matchup']}  →  {b.get('bet_desc', b['bet'])}"
            if mode == "total":
                pick += f"   (model predicts {b.get('predicted_total', '?')})"

        return [
            f"  ★  Grade: [{g}]  {sport}",
            pick,
            f"     Model: {b['model_prob']:.0%}  vs  Book: {b['book_prob']:.0%}  →  Edge: +{b['edge']:.0%}",
            f"     Odds: {odds_s}  |  Stake: ${b['stake_usd']:.0f}  to win ${payout:.0f}  |  EV: {ev_s}",
        ]

    out: list[str] = []
    today_s = datetime.date.today().strftime("%a %b %d %Y")

    # ══ Header ══════════════════════════════════════════════════════════
    out.append(_line("═"))
    out.append(f"    MLB + SOCCER BETTING REPORT  –  {today_s}")
    out.append(f"    Bankroll: ${BANKROLL:,.0f}   Min edge: {MIN_VALUE_EDGE:.0%}   Kelly: {KELLY_FRACTION:.0%}")
    out.append(_line("═"))

    # ══ Team Win Bets ════════════════════════════════════════════════════
    out.append("")
    out.append("  ─── TEAM WIN BETS " + "─" * (W - 18))
    out.append("")
    if not value_bets:
        out.append("  No team win value bets found today. Models are in line with the books.")
    else:
        for b in value_bets:
            for ln in _bet_block(b, "win"):
                out.append(ln)
            out.append("")

    # ══ Over / Under ─────────────────────────────────────────────────────
    if totals_bets:
        out.append("  ─── OVER / UNDER  (GAME TOTALS) " + "─" * (W - 32))
        out.append("")
        for b in totals_bets:
            for ln in _bet_block(b, "total"):
                out.append(ln)
            out.append("")
    else:
        out.append("  ─── OVER / UNDER  (GAME TOTALS) " + "─" * (W - 32))
        out.append("")
        out.append("  No totals value found today, or totals odds not yet available.")
        out.append("")

    # ══ Player Props ─────────────────────────────────────────────────────
    out.append("  ─── PLAYER PROPS  (TODAY'S STARTERS – STRIKEOUTS) " + "─" * (W - 50))
    out.append("")
    if not prop_stats:
        out.append("  No starter data available (Baseball Reference may be updating).")
        out.append("")
    else:
        for p in prop_stats:
            over_p  = p.get("over_prob", 0.0)
            under_p = p.get("under_prob", 0.0)
            line    = p.get("line", 5.5)
            k_avg   = p.get("avg_per_game", 0.0)

            if over_p >= 0.68:
                rec = f"STRONG OVER  →  take OVER {line:.1f} Ks"
            elif over_p >= 0.58:
                rec = f"LEAN OVER    →  consider OVER {line:.1f} Ks"
            elif under_p >= 0.68:
                rec = f"STRONG UNDER →  take UNDER {line:.1f} Ks"
            elif under_p >= 0.58:
                rec = f"LEAN UNDER   →  consider UNDER {line:.1f} Ks"
            else:
                rec = f"COIN FLIP    →  skip ({over_p:.0%} over / {under_p:.0%} under)"

            out.append(f"  ┌─  {p['name']}  ({p.get('team', '?')})   {p.get('game', '')}")
            out.append(f"  │   ERA: {p.get('era', 0):.2f}   K/9: {p.get('k9', 0):.1f}   WHIP: {p.get('whip', 0):.2f}   IP/start: {p.get('ip_per_start', 0):.1f}")
            out.append(f"  │   Season avg: {k_avg:.1f} Ks/start   →   Prop line: {line:.1f}")
            out.append(f"  │   {rec}")
            out.append(f"  └" + "─" * (W - 3))
            out.append("")

    # ══ Parlays ──────────────────────────────────────────────────────────
    if parlays:
        out.append("  ─── PARLAY OPTIONS " + "─" * (W - 19))
        out.append("")
        for p in parlays[:3]:
            ev_s        = f"{p['ev']:+.3f}"
            parlay_stake = round(BANKROLL * 0.02, 0)
            parlay_win   = round(parlay_stake * p["combined_dec_odds"], 2)
            out.append(
                f"  {p['num_legs']}-LEG  |  Odds: {p['combined_dec_odds']:.2f}x  |  "
                f"Prob: {p['combined_prob']:.0%}  |  EV: {ev_s}"
            )
            out.append(f"  Stake ${parlay_stake:.0f} → potential win ${parlay_win:.0f}")
            for i, leg in enumerate(p["legs"], 1):
                out.append(f"    {i}. {leg}")
            out.append("")

    # ══ Footer ═══════════════════════════════════════════════════════════
    n_win    = len(value_bets)
    n_totals = len(totals_bets or [])
    n_props  = len(prop_stats or [])
    total_stake = sum(b["stake_usd"] for b in value_bets + (totals_bets or []))

    out.append(_line("═"))
    out.append(
        f"  SUMMARY:  {n_win} win bet(s)  |  {n_totals} totals bet(s)  |  "
        f"{n_props} props analyzed  |  Total staked: ${total_stake:.0f}"
    )
    out.append(_line("═"))

    return "\n".join(out)

