"""
soccer_predictor.py — Multi-tournament soccer bet builder
=========================================================
Builds structured bet dicts from model predictions + odds + news sentiment,
matching the same schema as MLB bets (for dashboard compatibility).

Sentiment integration:
  - News/sentiment signal from soccer_news.py (cached 2h)
  - Adjusts base model probabilities by up to ±3% based on team news
  - Strong positive news (injury return, winning run) → small prob boost
  - Strong negative news (key injury, suspension) → small prob reduction
"""

from __future__ import annotations

import os
from typing import Any

from models.soccer_model import predict as _model_predict, STAGE_MAP
from data.club_stats_fetcher import get_squad_props
from data.wc2026_fetcher import TEAM_ELO, get_wc_odds

try:
    from data.soccer_news import (
        get_match_news_signal,
        get_injury_alerts,
        get_market_popularity_signal,
    )
    _NEWS_AVAILABLE = True
except ImportError:
    _NEWS_AVAILABLE = False
    def get_match_news_signal(h, a): return {}
    def get_injury_alerts(t): return []
    def get_market_popularity_signal(h, a): return {}

try:
    from data.soccer_fetcher import (
        get_competition_odds,
        FD_TO_ODDS_SPORT,
        TOURNAMENTS,
        get_team_recent_form,
    )
except ImportError:
    def get_competition_odds(code): return []
    def get_team_recent_form(team_name, days_back=140, max_matches=12, competition_codes=None):
        return {
            "sample_size": 0,
            "goals_for_per_match": 0.0,
            "goals_against_per_match": 0.0,
            "points_per_match": 0.0,
            "win_rate": 0.0,
        }
    FD_TO_ODDS_SPORT = {}
    TOURNAMENTS = {}

try:
    from data.soccer_data_sources import build_soccer_prediction_context
    _DS_CONTEXT_AVAILABLE = True
except Exception:
    _DS_CONTEXT_AVAILABLE = False
    def build_soccer_prediction_context(home_team: str, away_team: str, *, league_hint: str = "EPL", match_date: str = "") -> dict:
        return {}


# ── Safety thresholds (same as MLB) ──────────────────────────────────────────
def _safety(prob: float) -> str:
    if prob >= 0.78: return "ELITE"
    if prob >= 0.62: return "SAFE"
    if prob >= 0.50: return "MODERATE"
    return "RISKY"


def _edge(model_prob: float, implied_prob: float) -> float:
    return round(model_prob - implied_prob, 4)


def _american_to_implied(odds: int | float | None) -> float:
    if not odds:
        return 0.33
    try:
        odds = float(odds)
        if odds > 0:
            return round(100 / (odds + 100), 4)
        else:
            return round(abs(odds) / (abs(odds) + 100), 4)
    except (TypeError, ValueError):
        return 0.33


def _implied_to_american(prob: float) -> int:
    if prob <= 0:
        return +999
    if prob >= 1:
        return -999
    if prob >= 0.5:
        return int(round(-prob / (1 - prob) * 100))
    else:
        return int(round((1 - prob) / prob * 100))


def _american_to_decimal(odds: int | float | None) -> float:
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return 1.91
    if o > 0:
        return round(1 + (o / 100.0), 4)
    if o < 0:
        return round(1 + (100.0 / abs(o)), 4)
    return 1.91


def _safety_score(label: str | None) -> float:
    v = str(label or "MODERATE").upper()
    if v == "ELITE":
        return 0.80
    if v == "SAFE":
        return 0.65
    if v == "MODERATE":
        return 0.52
    return 0.45


def _worth_eval(prob: float, odds_am: int | float | None, edge: float, popularity: float) -> tuple[float, bool, str]:
    implied = _american_to_implied(odds_am)
    base_delta = float(prob or 0.0) - float(implied or 0.0)
    score = round(base_delta + max(0.0, float(edge or 0.0)) * 0.35 + max(0.0, float(popularity or 0.0)) * 0.12, 4)
    worth_it = score >= 0.03 and float(prob or 0.0) >= 0.5
    reason = (
        f"Model {prob:.1%} vs implied {implied:.1%}; edge {edge:+.1%}; chatter {popularity:.2f}"
        if isinstance(prob, float) else "Model/value comparison unavailable"
    )
    return score, worth_it, reason


# ── Team statistics lookup ────────────────────────────────────────────────────
_TEAM_STATS_DEFAULTS = {
    # goals_for, goals_ag, xg_for, xg_ag (per 90 min averages)
    "France":        (2.10, 0.80, 2.05, 0.82),
    "Brazil":        (2.05, 0.85, 2.00, 0.88),
    "Argentina":     (2.00, 0.90, 1.95, 0.92),
    "England":       (2.00, 0.85, 1.95, 0.88),
    "Spain":         (1.95, 0.78, 1.90, 0.80),
    "Germany":       (1.95, 0.95, 1.88, 0.98),
    "Portugal":      (1.90, 0.90, 1.85, 0.92),
    "Netherlands":   (1.88, 0.88, 1.82, 0.90),
    "Belgium":       (1.85, 0.88, 1.78, 0.90),
    "Croatia":       (1.55, 0.85, 1.52, 0.88),
    "Uruguay":       (1.65, 0.90, 1.60, 0.92),
    "Colombia":      (1.62, 0.92, 1.58, 0.95),
    "Japan":         (1.55, 0.98, 1.50, 1.00),
    "Morocco":       (1.40, 0.75, 1.38, 0.78),
    "Senegal":       (1.45, 0.95, 1.42, 0.98),
    "Mexico":        (1.55, 1.05, 1.52, 1.08),
    "United States": (1.48, 1.05, 1.45, 1.08),
    "South Korea":   (1.45, 1.02, 1.42, 1.05),
    "Canada":        (1.55, 0.98, 1.52, 1.00),
    "Australia":     (1.38, 1.10, 1.35, 1.12),
    "Turkey":        (1.55, 1.05, 1.52, 1.08),
    "Serbia":        (1.62, 1.05, 1.58, 1.08),
    "Ecuador":       (1.45, 0.98, 1.42, 1.00),
    "Chile":         (1.42, 1.05, 1.38, 1.08),
    "Peru":          (1.35, 1.05, 1.32, 1.08),
    "Iran":          (1.32, 1.10, 1.28, 1.12),
    "New Zealand":   (1.25, 1.20, 1.22, 1.22),
    "Saudi Arabia":  (1.30, 1.15, 1.28, 1.18),
    "Costa Rica":    (1.25, 1.20, 1.22, 1.22),
    "Panama":        (1.20, 1.25, 1.18, 1.28),
    "Venezuela":     (1.35, 1.10, 1.32, 1.12),
    "Honduras":      (1.18, 1.28, 1.15, 1.30),
    "Bolivia":       (1.10, 1.42, 1.08, 1.45),
    "Paraguay":      (1.25, 1.12, 1.22, 1.15),
}

def _team_stats(team: str) -> tuple[float, float, float, float]:
    return _TEAM_STATS_DEFAULTS.get(team, (1.40, 1.05, 1.35, 1.08))


def _blend_team_stats_with_recent_form(team: str, base_stats: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
    """Blend static priors with recent season form from historical finished matches."""
    g_for, g_against, xg_for, xg_against = base_stats
    try:
        form = get_team_recent_form(team, days_back=140, max_matches=12)
    except Exception:
        return base_stats

    sample = int(form.get("sample_size", 0) or 0)
    if sample < 3:
        return base_stats

    form_weight = min(0.45, 0.18 + (sample * 0.02))
    hist_for = float(form.get("goals_for_per_match", g_for) or g_for)
    hist_against = float(form.get("goals_against_per_match", g_against) or g_against)
    blended_for = (g_for * (1.0 - form_weight)) + (hist_for * form_weight)
    blended_against = (g_against * (1.0 - form_weight)) + (hist_against * form_weight)

    # Keep xG tied to historical goal tendencies for stability.
    blended_xg_for = (xg_for * (1.0 - form_weight)) + (hist_for * form_weight)
    blended_xg_against = (xg_against * (1.0 - form_weight)) + (hist_against * form_weight)
    return (blended_for, blended_against, blended_xg_for, blended_xg_against)


def _get_live_odds_for_match(home: str, away: str, all_odds: list[dict]) -> dict:
    """Find odds for a specific match from the odds API response."""
    h_low = home.lower()
    a_low = away.lower()
    for event in (all_odds or []):
        eh = event.get("home_team","").lower()
        ea = event.get("away_team","").lower()
        if eh in h_low or h_low in eh or ea in a_low or a_low in ea:
            result: dict = {"h2h_home": None, "h2h_draw": None, "h2h_away": None,
                            "over25": None, "under25": None}
            for bk in event.get("bookmakers", [])[:3]:
                for market in bk.get("markets", []):
                    if market["key"] == "h2h":
                        for outcome in market.get("outcomes", []):
                            if outcome["name"].lower() in (eh, h_low):
                                result["h2h_home"] = result["h2h_home"] or outcome.get("price")
                            elif outcome["name"].lower() == "draw":
                                result["h2h_draw"] = result["h2h_draw"] or outcome.get("price")
                            else:
                                result["h2h_away"] = result["h2h_away"] or outcome.get("price")
                    elif market["key"] == "totals":
                        for outcome in market.get("outcomes", []):
                            if outcome.get("point") in (2.5, 2) and outcome["name"] == "Over":
                                result["over25"] = result["over25"] or outcome.get("price")
                            elif outcome.get("point") in (2.5, 2) and outcome["name"] == "Under":
                                result["under25"] = result["under25"] or outcome.get("price")
            return result
    return {}


# ── Core bet builder ──────────────────────────────────────────────────────────
def predict_match(
    home: str, away: str, stage: str = "group",
    home_elo: float | None = None, away_elo: float | None = None,
    use_sentiment: bool = True,
) -> dict[str, float]:
    """
    Call soccer_model.predict with team stats lookup.
    Optionally adjusts probabilities with news sentiment signal.
    """
    h_elo = home_elo or TEAM_ELO.get(home, 1850.0)
    a_elo = away_elo or TEAM_ELO.get(away, 1850.0)
    gfh, gah, xfh, xah = _blend_team_stats_with_recent_form(home, _team_stats(home))
    gfa, gaa, xfa, xaa = _blend_team_stats_with_recent_form(away, _team_stats(away))
    stage_id = STAGE_MAP.get(stage.lower(), 0)

    probs = _model_predict(
        home_elo=h_elo,    away_elo=a_elo,
        goals_for_h=gfh,   goals_ag_h=gah,
        goals_for_a=gfa,   goals_ag_a=gaa,
        xg_for_h=xfh,      xg_ag_h=xah,
        xg_for_a=xfa,      xg_ag_a=xaa,
        stage=stage_id,
    )

    # ── Multi-source data context (Understat/Transfermarkt/API-Football) ──
    if _DS_CONTEXT_AVAILABLE and str(os.getenv("SOCCER_MULTI_SOURCE_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}:
        try:
            ds_ctx = build_soccer_prediction_context(
                home_team=home,
                away_team=away,
                league_hint=str(os.getenv("SOCCER_UNDERSTAT_LEAGUE", "EPL") or "EPL"),
            )

            hxg = float(ds_ctx.get("home_understat_xg") or 0.0)
            axg = float(ds_ctx.get("away_understat_xg") or 0.0)
            hxga = float(ds_ctx.get("home_understat_xga") or 0.0)
            axga = float(ds_ctx.get("away_understat_xga") or 0.0)
            home_inj = int(ds_ctx.get("home_transfermarkt_injuries") or 0)
            away_inj = int(ds_ctx.get("away_transfermarkt_injuries") or 0)
            lineups_confirmed = bool(ds_ctx.get("lineups_confirmed"))
            api_home = ds_ctx.get("api_home_prob")
            api_away = ds_ctx.get("api_away_prob")

            # Small bounded shift from external context to avoid overfitting noisy feeds.
            shift = 0.0
            xg_delta = (hxg - axg) + (axga - hxga)
            shift += max(-0.03, min(0.03, xg_delta * 0.012))
            shift += max(-0.02, min(0.02, (away_inj - home_inj) * 0.006))
            if lineups_confirmed:
                shift += 0.004

            if api_home is not None and api_away is not None:
                api_delta = float(api_home) - float(api_away)
                model_delta = float(probs.get("home_prob", 0.33)) - float(probs.get("away_prob", 0.33))
                shift += max(-0.02, min(0.02, (api_delta - model_delta) * 0.35))

            shift = max(-0.05, min(0.05, shift))
            hp = float(probs.get("home_prob", 0.33)) + shift
            ap = float(probs.get("away_prob", 0.33)) - shift
            dp = float(probs.get("draw_prob", 0.34))
            total = hp + dp + ap
            if total > 0:
                probs["home_prob"] = round(max(0.02, hp / total), 4)
                probs["away_prob"] = round(max(0.02, ap / total), 4)
                probs["draw_prob"] = round(max(0.02, dp / total), 4)

            probs["context_shift"] = round(shift, 4)
            probs["context_home_injuries"] = home_inj
            probs["context_away_injuries"] = away_inj
            probs["context_lineups_confirmed"] = lineups_confirmed
            probs["context_sources"] = ds_ctx.get("sources") or {}
        except Exception as e:
            print(f"[soccer_predictor] Data-source context error: {e}")

    # ── Sentiment adjustment (coverage-aware nudge) ─────────────────────────
    if use_sentiment and _NEWS_AVAILABLE:
        try:
            signal = get_match_news_signal(home, away)
            combined = signal.get("combined_signal", 0.0)  # home - away, -2 to +2
            src_cov = float(signal.get("source_coverage", 0) or 0)
            max_shift = 0.03 + min(0.03, src_cov * 0.003)
            adj = max(-1.0, min(1.0, combined)) * max_shift
            hp  = probs["home_prob"] + adj
            ap  = probs["away_prob"] - adj
            dp  = probs["draw_prob"]
            # Renormalise
            total = hp + dp + ap
            if total > 0:
                probs["home_prob"] = round(max(0.02, hp / total), 4)
                probs["away_prob"] = round(max(0.02, ap / total), 4)
                probs["draw_prob"] = round(max(0.02, dp / total), 4)
            # Carry sentiment through
            probs["home_sentiment"] = signal.get("home_sentiment", 0.0)
            probs["away_sentiment"] = signal.get("away_sentiment", 0.0)
            probs["sentiment_signal"] = combined
            probs["home_sentiment_label"] = signal.get("home_label", "neutral")
            probs["away_sentiment_label"] = signal.get("away_label", "neutral")
            probs["home_headlines"] = signal.get("home_headlines", [])
            probs["away_headlines"] = signal.get("away_headlines", [])
            probs["home_sources"] = signal.get("home_sources", [])
            probs["away_sources"] = signal.get("away_sources", [])
            probs["source_coverage"] = int(src_cov)
            probs["market_popularity"] = signal.get("market_popularity", {})
        except Exception as e:
            print(f"[soccer_predictor] Sentiment error: {e}")

    return probs


def build_match_bets(match: dict, live_odds: list[dict] | None = None) -> list[dict]:
    """
    Build bet dicts for a WC match.
    match: internal match dict from wc2026_fetcher
    Returns list of bet dicts (same schema as MLB side/total/parlay bets).
    """
    home  = match["home_team"]
    away  = match["away_team"]
    stage = match.get("stage", "GROUP_STAGE").lower()
    # Normalize stage string
    for key in STAGE_MAP:
        if key in stage:
            stage = key
            break
    else:
        stage = "group"

    probs = predict_match(home, away, stage)
    hp    = probs["home_prob"]
    dp    = probs["draw_prob"]
    ap    = probs["away_prob"]
    o25   = probs["over25_prob"]
    btts  = probs["btts_prob"]

    match_key = match.get("match_key", f"{away[:3]}@{home[:3]}".upper())
    game_key  = match.get("game_key", f"{match.get('date','')}#{away}@{home}")
    date_str  = match.get("date", "")

    # Get live odds
    raw_odds = _get_live_odds_for_match(home, away, live_odds or [])
    oh   = raw_odds.get("h2h_home")
    od   = raw_odds.get("h2h_draw")
    oa   = raw_odds.get("h2h_away")
    oov  = raw_odds.get("over25")

    ih = _american_to_implied(oh) or 0.38
    id_ = _american_to_implied(od) or 0.28
    ia  = _american_to_implied(oa) or 0.34
    iov = _american_to_implied(oov) or 0.48

    # Fall back to model-implied odds when market unavailable
    if not oh: oh = _implied_to_american(hp)
    if not od: od = _implied_to_american(dp)
    if not oa: oa = _implied_to_american(ap)
    if not oov: oov = _implied_to_american(o25)

    bets: list[dict] = []

    # ── 1. 1X2 — Home Win ─────────────────────────────────────────────────────
    if hp >= 0.45:
        bets.append({
            "bet_type":    "1X2",
            "pick_label":  f"{home} to Win",
            "pick_side":   "home",
            "probability": hp,
            "model_prob":  hp,
            "edge":        _edge(hp, ih),
            "odds":        oh,
            "odds_am":     oh,
            "dec_odds":    _american_to_decimal(oh),
            "safety_label": _safety(hp),
            "match_key":   match_key,
            "game_key":    game_key,
            "date":        date_str,
            "sport":       "soccer",
            "home_team":   home,
            "away_team":   away,
        })

    # ── 2. 1X2 — Draw ─────────────────────────────────────────────────────────
    if dp >= 0.28:
        bets.append({
            "bet_type":    "1X2",
            "pick_label":  "Draw",
            "pick_side":   "draw",
            "probability": dp,
            "model_prob":  dp,
            "edge":        _edge(dp, id_),
            "odds":        od,
            "odds_am":     od,
            "dec_odds":    _american_to_decimal(od),
            "safety_label": _safety(dp),
            "match_key":   match_key,
            "game_key":    game_key,
            "date":        date_str,
            "sport":       "soccer",
            "home_team":   home,
            "away_team":   away,
        })

    # ── 3. 1X2 — Away Win ─────────────────────────────────────────────────────
    if ap >= 0.40:
        bets.append({
            "bet_type":    "1X2",
            "pick_label":  f"{away} to Win",
            "pick_side":   "away",
            "probability": ap,
            "model_prob":  ap,
            "edge":        _edge(ap, ia),
            "odds":        oa,
            "odds_am":     oa,
            "dec_odds":    _american_to_decimal(oa),
            "safety_label": _safety(ap),
            "match_key":   match_key,
            "game_key":    game_key,
            "date":        date_str,
            "sport":       "soccer",
            "home_team":   home,
            "away_team":   away,
        })

    # ── 4. Goals Over 2.5 ─────────────────────────────────────────────────────
    if o25 >= 0.50:
        bets.append({
            "bet_type":    "Goals O/U",
            "pick_label":  f"Over 2.5 Goals",
            "pick_side":   "over",
            "probability": o25,
            "model_prob":  o25,
            "edge":        _edge(o25, iov),
            "odds":        oov,
            "odds_am":     oov,
            "dec_odds":    _american_to_decimal(oov),
            "safety_label": _safety(o25),
            "match_key":   match_key,
            "game_key":    game_key,
            "date":        date_str,
            "sport":       "soccer",
            "home_team":   home,
            "away_team":   away,
        })

    # ── 5. Goals Under 2.5 ────────────────────────────────────────────────────
    u25 = 1 - o25
    if u25 >= 0.52:
        iund = 1 - iov
        ound = raw_odds.get("under25") or _implied_to_american(u25)
        bets.append({
            "bet_type":    "Goals O/U",
            "pick_label":  "Under 2.5 Goals",
            "pick_side":   "under",
            "probability": u25,
            "model_prob":  u25,
            "edge":        _edge(u25, iund),
            "odds":        ound,
            "odds_am":     ound,
            "dec_odds":    _american_to_decimal(ound),
            "safety_label": _safety(u25),
            "match_key":   match_key,
            "game_key":    game_key,
            "date":        date_str,
            "sport":       "soccer",
            "home_team":   home,
            "away_team":   away,
        })

    # ── 6. BTTS (Both Teams to Score) ─────────────────────────────────────────
    if btts >= 0.50:
        # Estimate BTTS odds (often around -110 to +120)
        ibtts = btts
        obtts = _implied_to_american(ibtts)
        bets.append({
            "bet_type":    "BTTS",
            "pick_label":  "Both Teams to Score — Yes",
            "pick_side":   "yes",
            "probability": btts,
            "model_prob":  btts,
            "edge":        _edge(btts, ibtts),
            "odds":        obtts,
            "odds_am":     obtts,
            "dec_odds":    _american_to_decimal(obtts),
            "safety_label": _safety(btts),
            "match_key":   match_key,
            "game_key":    game_key,
            "date":        date_str,
            "sport":       "soccer",
            "home_team":   home,
            "away_team":   away,
        })

    # ── 7. Draw No Bet (DNB) — if one side is clear favourite ─────────────────
    dnb_side = None
    dnb_prob = 0.0
    if hp > ap + 0.15:
        dnb_side = home
        dnb_prob = round(hp / (hp + ap), 4)
    elif ap > hp + 0.15:
        dnb_side = away
        dnb_prob = round(ap / (hp + ap), 4)

    if dnb_side and dnb_prob >= 0.58:
        idnb = _american_to_implied(-115)  # typical DNB juice
        bets.append({
            "bet_type":    "Draw No Bet",
            "pick_label":  f"{dnb_side} (DNB)",
            "pick_side":   "home" if dnb_side == home else "away",
            "probability": dnb_prob,
            "model_prob":  dnb_prob,
            "edge":        _edge(dnb_prob, idnb),
            "odds":        -115,
            "odds_am":     -115,
            "dec_odds":    _american_to_decimal(-115),
            "safety_label": _safety(dnb_prob),
            "match_key":   match_key,
            "game_key":    game_key,
            "date":        date_str,
            "sport":       "soccer",
            "home_team":   home,
            "away_team":   away,
        })

    market_signal = get_market_popularity_signal(home, away) if _NEWS_AVAILABLE else {}
    market_scores = (market_signal or {}).get("market_scores", {}) or {}
    market_counts = (market_signal or {}).get("market_counts", {}) or {}

    def _market_key_for_bet(bet: dict) -> str:
        bt = str(bet.get("bet_type") or "").lower()
        side = str(bet.get("pick_side") or "").lower()
        if bt == "1x2" and side == "home":
            return "home_win"
        if bt == "1x2" and side == "away":
            return "away_win"
        if bt == "1x2" and side == "draw":
            return "draw"
        if bt == "goals o/u" and side == "over":
            return "over_2_5"
        if bt == "goals o/u" and side == "under":
            return "under_2_5"
        if bt == "btts":
            return "btts_yes"
        if bt == "draw no bet" and side == "home":
            return "home_win"
        if bt == "draw no bet" and side == "away":
            return "away_win"
        return "home_win"

    for bet in bets:
        key = _market_key_for_bet(bet)
        pop = float(market_scores.get(key, 0.0) or 0.0)
        mentions = int(market_counts.get(key, 0) or 0)
        score, worth_it, reason = _worth_eval(
            float(bet.get("probability", 0.5) or 0.5),
            bet.get("odds_am"),
            float(bet.get("edge", 0.0) or 0.0),
            pop,
        )
        bet["market_popularity"] = round(pop, 4)
        bet["market_mentions"] = mentions
        bet["worth_score"] = score
        bet["worth_it"] = worth_it
        bet["worth_reason"] = reason
        bet["safety"] = _safety_score(bet.get("safety_label"))

    return bets


def get_player_props(match: dict) -> list[dict]:
    """
    Build player prop bets for a WC match.
    Uses club_stats_fetcher to get top players for each national team.
    """
    home = match["home_team"]
    away = match["away_team"]
    game_key = match.get("game_key", "")
    date_str = match.get("date", "")

    home_props = get_squad_props(home, top_n=5)
    away_props = get_squad_props(away, top_n=5)
    all_props  = home_props + away_props

    market_signal = get_market_popularity_signal(home, away) if _NEWS_AVAILABLE else {}
    prop_pop = float(((market_signal or {}).get("market_scores") or {}).get("player_props", 0.0) or 0.0)
    prop_mentions = int(((market_signal or {}).get("market_counts") or {}).get("player_props", 0) or 0)

    # Attach match context
    for p in all_props:
        p["game_key"]  = game_key
        p["date"]      = date_str
        p["home_team"] = home
        p["away_team"] = away
        p["match_key"] = match.get("match_key", "")
        p["sport"]     = "soccer"
        p.setdefault("odds_am", -110)
        p.setdefault("dec_odds", _american_to_decimal(p.get("odds_am")))
        p.setdefault("model_prob", 0.5)
        p["safety"] = _safety_score(p.get("safety_label"))

        prob = float(p.get("model_prob", 0.5) or 0.5)
        implied = _american_to_implied(p.get("odds_am", -110))
        score = round((prob - implied) + (prop_pop * 0.10), 4)
        p["market_popularity"] = round(prop_pop, 4)
        p["market_mentions"] = prop_mentions
        p["worth_score"] = score
        p["worth_it"] = bool(score >= 0.025 and prob >= 0.5)
        p["worth_reason"] = (
            f"Player prop model {prob:.1%} vs implied {implied:.1%}; chatter {prop_pop:.2f}"
        )

    return all_props


def build_parlay(bets: list[dict], max_legs: int = 4) -> dict | None:
    """
    Build a parlay from the highest-edge SAFE+ bets across matches.
    Returns parlay dict or None if not enough legs.
    """
    # Filter ELITE + SAFE bets with positive edge
    eligible = [b for b in bets
                if b.get("safety_label") in ("ELITE", "SAFE")
                and b.get("edge", 0) > 0.04
                and b.get("probability", 0) >= 0.58]

    if len(eligible) < 2:
        return None

    # Sort by edge desc, dedupe by game_key (one bet per match)
    seen_games: set[str] = set()
    legs: list[dict] = []
    for bet in sorted(eligible, key=lambda b: b.get("edge", 0), reverse=True):
        gk = bet.get("game_key", "")
        if gk not in seen_games:
            seen_games.add(gk)
            legs.append(bet)
        if len(legs) >= max_legs:
            break

    if len(legs) < 2:
        return None

    # Parlay probability = product of individual probs
    parlay_prob = 1.0
    for leg in legs:
        parlay_prob *= leg.get("probability", 0.5)

    # Parlay payout (approximate)
    payout_mult = 1.0
    for leg in legs:
        odds = float(leg.get("odds", -110) or -110)
        if odds > 0:
            payout_mult *= (1 + odds / 100)
        else:
            payout_mult *= (1 + 100 / abs(odds))

    # Convert to American odds
    payout_am = _implied_to_american(1 / payout_mult) if payout_mult > 1 else +500

    return {
        "parlay_type":  "Best Value",
        "legs":         legs,
        "leg_count":    len(legs),
        "probability":  round(parlay_prob, 4),
        "payout":       payout_am,
        "payout_mult":  round(payout_mult, 2),
        "safety_label": "SAFE" if parlay_prob >= 0.35 else "RISKY",
        "sport":        "soccer",
        "date":         legs[0].get("date", ""),
    }


def analyze_matches(matches: list[dict], competition_code: str = "WC") -> dict[str, Any]:
    """
    Run full analysis on a list of soccer matches from any competition.
    Returns state dict compatible with dashboard / email_notify.
    """
    # Get live odds for this competition
    try:
        live_odds = get_competition_odds(competition_code) or get_wc_odds()
    except Exception:
        live_odds = get_wc_odds()

    all_bets:  list[dict] = []
    all_props: list[dict] = []
    cards:     list[dict] = []

    for match in matches:
        bets  = build_match_bets(match, live_odds)
        props = get_player_props(match)
        all_bets.extend(bets)
        all_props.extend(props)

        home  = match["home_team"]
        away  = match["away_team"]
        stage = match.get("stage", "group")
        probs = predict_match(home, away, stage, use_sentiment=True)

        # Injury alerts for UI display
        home_injuries = []
        away_injuries = []
        if _NEWS_AVAILABLE:
            try:
                home_injuries = get_injury_alerts(home)
                away_injuries = get_injury_alerts(away)
            except Exception:
                pass

        comp_code = match.get("competition", competition_code)
        comp_info = TOURNAMENTS.get(comp_code, {"name": comp_code, "emoji": "⚽"})

        cards.append({
            "game_key":           match.get("game_key", ""),
            "match_key":          match.get("match_key", ""),
            "game_date":          match.get("date", match.get("game_date", "")),
            "game_time":          match.get("game_time", "TBD"),
            "home_team":          home,
            "away_team":          away,
            "home_crest":         match.get("home_crest", ""),
            "away_crest":         match.get("away_crest", ""),
            "venue":              match.get("venue", ""),
            "group":              match.get("group", ""),
            "stage":              stage,
            "status":             match.get("status", "Scheduled"),
            "home_score":         match.get("home_score"),
            "away_score":         match.get("away_score"),
            "home_prob":          probs["home_prob"],
            "draw_prob":          probs["draw_prob"],
            "away_prob":          probs["away_prob"],
            "over25":             probs["over25_prob"],
            "btts":               probs["btts_prob"],
            # Sentiment
            "home_sentiment":     probs.get("home_sentiment", 0.0),
            "away_sentiment":     probs.get("away_sentiment", 0.0),
            "sentiment_signal":   probs.get("sentiment_signal", 0.0),
            "home_sentiment_label": probs.get("home_sentiment_label", "neutral"),
            "away_sentiment_label": probs.get("away_sentiment_label", "neutral"),
            "home_headlines":     probs.get("home_headlines", []),
            "away_headlines":     probs.get("away_headlines", []),
            "source_coverage":    probs.get("source_coverage", 0),
            "market_popularity":  probs.get("market_popularity", {}),
            "home_injuries":      home_injuries,
            "away_injuries":      away_injuries,
            # Competition info
            "competition":        comp_code,
            "comp_name":          comp_info.get("name", comp_code),
            "comp_emoji":         comp_info.get("emoji", "⚽"),
            # Bets
            "bets":               bets,
            "home_props":         [p for p in props if p.get("team") == home],
            "away_props":         [p for p in props if p.get("team") == away],
            "sport":              "soccer",
            # Dashboard compatibility fields
            "overall_safety_label": _safety(probs["home_prob"]),
            "when_label":         "TODAY",
        })

    parlay = build_parlay(all_bets)
    parlays = [parlay] if parlay else []

    return {
        "game_cards_today":    cards,
        "game_cards_tomorrow": [],
        "games":               cards,
        "bets":                all_bets,
        "player_props":        all_props,
        "best_parlays":        parlays,
        "sport":               "soccer",
        "competition":         competition_code,
        "date":                matches[0].get("date", "") if matches else "",
    }
