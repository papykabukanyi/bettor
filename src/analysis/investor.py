"""
Investor-Grade Bet Scoring and Portfolio Management
====================================================
Think like a professional sports bettor with decades of experience.

Every bet must earn its place through a rigorous quality gate:
  - Real edge over the closing line, not just any positive number
  - Model confidence backed by multi-source sentiment confirmation
  - Clean injury/lineup status (never fade a compromised side on thin info)
  - Live Kalshi market availability (confirms market liquidity exists)
  - Conservative bankroll sizing — survive a bad run, profit over time

Grade tiers:
  A+  (75-100)  Bankable play — maximum conviction, full recommended stake
  A   (60-74)   Solid play    — high quality, 2.5% of bankroll
  B   (45-59)   Moderate play — use carefully, 1.2% stake
  C   (30-44)   Speculative   — 0.4% token position only
  X   (<30)     Skip          — not worth the risk capital
"""
from __future__ import annotations

import os
from typing import Any

# ── Grade configuration ───────────────────────────────────────────────────────

# (min_score, grade_code, display_label)
_GRADE_THRESHOLDS: list[tuple[int, str, str]] = [
    (75, "A+", "A+ PLAY — BANKABLE"),
    (60, "A",  "A PLAY — SOLID"),
    (45, "B",  "B PLAY — MODERATE"),
    (30, "C",  "C PLAY — SPECULATIVE"),
    (0,  "X",  "SKIP — INSUFFICIENT EDGE"),
]

# Stake as % of bankroll per grade (institutional-style, conservative sizing)
_GRADE_STAKE_PCT: dict[str, float] = {
    "A+": 0.040,
    "A":  0.025,
    "B":  0.012,
    "C":  0.004,
    "X":  0.000,
}

# Minimum EV required to keep a grade (prevents high-score low-EV promotions)
_GRADE_MIN_EV: dict[str, float] = {
    "A+": 0.08,
    "A":  0.04,
    "B":  0.02,
    "C":  0.005,
    "X":  -999.0,
}

# Default daily risk cap
_DEFAULT_MAX_DAILY_RISK = float(os.getenv("DAILY_MAX_RISK_FRACTION", "0.12"))


# ── Core scoring ──────────────────────────────────────────────────────────────

def investor_score(bet: dict[str, Any]) -> float:
    """
    Compute a composite investor score (0–100) for a single bet.

    Weights:
      35 pts  Expected value (EV), scaled over 0–30% range
      30 pts  Model probability, scaled 0.50–0.95
      20 pts  Safety score (0–1)
      15 pts  Sentiment quality (multi-source, no injury)
      +3 pts  Bonus when already matched to a live Kalshi market
    """
    try:
        ev          = float(bet.get("ev") or 0)
        model_prob  = float(bet.get("model_prob") or 0.5)
        safety      = float(bet.get("safety") or 0.5)
        signal_type = str(bet.get("signal_type") or "neutral")
        sources     = bet.get("active_sources") or []
        if isinstance(sources, str):
            sources = [s for s in sources.split(",") if s.strip()]
        n_srcs      = len(sources)
        injury_flag = bool(bet.get("injury_flag"))
        momentum_flag = bool(bet.get("momentum_flag"))
        kalshi_ok   = str(bet.get("kalshi_status") or "") == "matched"

        # EV component (0–35 pts): full marks at 30 % EV
        ev_pts   = min(max(ev, 0.0), 0.30) / 0.30 * 35.0

        # Probability component (0–30 pts): linear from 0.50 to 0.95
        prob_pts = max(0.0, min((model_prob - 0.50) / 0.45, 1.0)) * 30.0

        # Safety component (0–20 pts)
        safe_pts = min(safety, 1.0) * 20.0

        # Sentiment component (can be negative for injury risk)
        if injury_flag:
            sent_pts = -12.0          # professional never bets injury-risk side on thin info
        elif signal_type == "positive_momentum" and n_srcs >= 3:
            sent_pts = 15.0
        elif signal_type == "positive_momentum" and n_srcs >= 2:
            sent_pts = 12.0
        elif signal_type == "positive_momentum":
            sent_pts = 8.0
        elif signal_type == "lineup_change" and momentum_flag:
            sent_pts = 6.0
        elif signal_type == "neutral":
            sent_pts = 5.0            # silence is OK — absence of bad news counts for something
        elif signal_type == "negative_momentum":
            sent_pts = 2.0
        elif signal_type == "lineup_change":
            sent_pts = 3.0
        else:
            sent_pts = 1.0

        # Kalshi liquidity bonus
        kalshi_bonus = 3.0 if kalshi_ok else 0.0

        raw = ev_pts + prob_pts + safe_pts + sent_pts + kalshi_bonus
        return round(max(0.0, min(100.0, raw)), 2)
    except Exception:
        return 0.0


def investor_grade(bet: dict[str, Any], bankroll: float | None = None) -> dict[str, Any]:
    """
    Return a dict of grade metadata for a single bet.

    Keys added:
      grade             str    — "A+", "A", "B", "C", "X"
      grade_label       str    — human-readable tier name
      investor_score    float  — 0–100 composite quality score
      recommended_stake float  — $ amount to wager (based on bankroll × grade %)
      grade_rationale   str    — concise reason string for display
    """
    if bankroll is None:
        try:
            from config import BANKROLL
            bankroll = float(BANKROLL or 1000)
        except Exception:
            bankroll = 1000.0

    score = investor_score(bet)

    # Map score → grade
    grade = "X"
    label = _GRADE_THRESHOLDS[-1][2]
    for threshold, g, l in _GRADE_THRESHOLDS:
        if score >= threshold:
            grade, label = g, l
            break

    # EV gate: downgrade if EV doesn't meet the bar for this tier
    ev = float(bet.get("ev") or 0)
    grade_order = ["A+", "A", "B", "C", "X"]
    while grade in ("A+", "A", "B") and ev < _GRADE_MIN_EV.get(grade, 0.0):
        idx = grade_order.index(grade)
        grade = grade_order[idx + 1]
        label = next(l for t, g, l in _GRADE_THRESHOLDS if g == grade)

    stake_pct = _GRADE_STAKE_PCT.get(grade, 0.0)
    recommended_stake = round(float(bankroll) * stake_pct, 2)

    # Build human-readable rationale
    parts: list[str] = []
    if ev > 0:
        parts.append(f"EV {ev:+.1%}")
    mp = float(bet.get("model_prob") or 0)
    if mp >= 0.68:
        parts.append(f"Model {mp:.0%}")
    srcs = bet.get("active_sources") or []
    if isinstance(srcs, str):
        srcs = [s for s in srcs.split(",") if s.strip()]
    if srcs:
        parts.append(f"{len(srcs)} signal sources")
    sig = str(bet.get("signal_type") or "neutral")
    if sig not in ("neutral", ""):
        parts.append(sig.replace("_", " ").title())
    kt = str(bet.get("kalshi_ticker") or "")
    if kt:
        parts.append(f"Kalshi {kt}")

    rationale = " · ".join(parts) if parts else "Model edge only"

    return {
        "grade":             grade,
        "grade_label":       label,
        "investor_score":    score,
        "recommended_stake": recommended_stake,
        "grade_rationale":   rationale,
    }


# ── Portfolio management ──────────────────────────────────────────────────────

def build_daily_portfolio(
    bets: list[dict[str, Any]],
    bankroll: float | None = None,
    max_daily_risk: float | None = None,
) -> list[dict[str, Any]]:
    """
    Filter, grade, and rank bets like a disciplined professional building a daily book.

    Rules:
      1. Grade every bet; skip X-grade entirely
      2. Sort by investor_score descending (highest conviction first)
      3. Cap total daily stake at max_daily_risk × bankroll (default 12%)
      4. Max 3 bets per game-key to avoid over-concentration on a single event
      5. Flag correlated legs (same game, different markets) for display

    Returns a new enriched list; originals are never mutated.
    """
    if bankroll is None:
        try:
            from config import BANKROLL
            bankroll = float(BANKROLL or 1000)
        except Exception:
            bankroll = 1000.0

    cap_fraction = max_daily_risk if max_daily_risk is not None else _DEFAULT_MAX_DAILY_RISK
    daily_cap    = float(bankroll) * cap_fraction

    # Enrich
    enriched: list[dict[str, Any]] = []
    for bet in (bets or []):
        if not isinstance(bet, dict):
            continue
        g = investor_grade(bet, bankroll)
        enriched.append({**bet, **g})

    # Sort highest conviction first
    enriched.sort(key=lambda x: float(x.get("investor_score") or 0), reverse=True)

    # Remove X-grade bets
    enriched = [b for b in enriched if b.get("grade", "X") != "X"]

    # Apply budget + per-game cap
    portfolio: list[dict[str, Any]] = []
    game_counts: dict[str, int] = {}
    budget_used = 0.0

    for b in enriched:
        gk     = str(b.get("game_key") or b.get("match_key") or b.get("matchup") or "unknown")
        stake  = float(b.get("recommended_stake") or 0)
        if game_counts.get(gk, 0) >= 3:
            continue
        if stake > 0 and (budget_used + stake) > daily_cap:
            continue
        game_counts[gk] = game_counts.get(gk, 0) + 1
        budget_used += stake
        portfolio.append(b)

    # Flag correlated legs (same game, >1 bet)
    game_idx: dict[str, list[int]] = {}
    for i, b in enumerate(portfolio):
        gk = str(b.get("game_key") or "x")
        game_idx.setdefault(gk, []).append(i)
    for gk, indices in game_idx.items():
        if len(indices) > 1:
            for i in indices:
                portfolio[i]["correlated_with_game"] = True

    return portfolio
