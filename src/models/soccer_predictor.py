"""
soccer_predictor.py — WC 2026 match bet builder
================================================
Builds structured bet dicts from model predictions + odds,
matching the same schema as MLB bets (for dashboard compatibility).
"""

from __future__ import annotations

import datetime
from typing import Any

from models.soccer_model import predict as _model_predict, STAGE_MAP
from data.club_stats_fetcher import get_squad_props, get_wc_player_stats
from data.wc2026_fetcher import TEAM_ELO, get_wc_odds


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
    home: str, away: str, stage: str = "group"
) -> dict[str, float]:
    """Call soccer_model.predict with team stats lookup."""
    h_elo = TEAM_ELO.get(home, 1850.0)
    a_elo = TEAM_ELO.get(away, 1850.0)
    gfh, gah, xfh, xah = _team_stats(home)
    gfa, gaa, xfa, xaa = _team_stats(away)
    stage_id = STAGE_MAP.get(stage.lower(), 0)

    return _model_predict(
        home_elo=h_elo,    away_elo=a_elo,
        goals_for_h=gfh,   goals_ag_h=gah,
        goals_for_a=gfa,   goals_ag_a=gaa,
        xg_for_h=xfh,      xg_ag_h=xah,
        xg_for_a=xfa,      xg_ag_a=xaa,
        stage=stage_id,
    )


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
            "edge":        _edge(hp, ih),
            "odds":        oh,
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
            "edge":        _edge(dp, id_),
            "odds":        od,
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
            "edge":        _edge(ap, ia),
            "odds":        oa,
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
            "edge":        _edge(o25, iov),
            "odds":        oov,
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
            "edge":        _edge(u25, iund),
            "odds":        ound,
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
            "edge":        _edge(btts, ibtts),
            "odds":        obtts,
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
            "edge":        _edge(dnb_prob, idnb),
            "odds":        -115,
            "safety_label": _safety(dnb_prob),
            "match_key":   match_key,
            "game_key":    game_key,
            "date":        date_str,
            "sport":       "soccer",
            "home_team":   home,
            "away_team":   away,
        })

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

    # Attach match context
    for p in all_props:
        p["game_key"]  = game_key
        p["date"]      = date_str
        p["home_team"] = home
        p["away_team"] = away
        p["match_key"] = match.get("match_key", "")
        p["sport"]     = "soccer"

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


def analyze_matches(matches: list[dict]) -> dict[str, Any]:
    """
    Run full analysis on a list of WC matches.
    Returns state dict compatible with dashboard / email_notify.
    """
    live_odds = get_wc_odds()
    all_bets:  list[dict] = []
    all_props: list[dict] = []
    cards:     list[dict] = []

    for match in matches:
        bets  = build_match_bets(match, live_odds)
        props = get_player_props(match)
        all_bets.extend(bets)
        all_props.extend(props)

        home = match["home_team"]
        away = match["away_team"]
        probs = predict_match(home, away, match.get("stage","group"))

        cards.append({
            "game_key":   match.get("game_key",""),
            "match_key":  match.get("match_key",""),
            "home_team":  home,
            "away_team":  away,
            "home_flag":  match.get("home_flag","🏳"),
            "away_flag":  match.get("away_flag","🏳"),
            "game_time":  match.get("game_time","TBD"),
            "venue":      match.get("venue",""),
            "city":       match.get("city",""),
            "group":      match.get("group",""),
            "status":     match.get("status","Scheduled"),
            "home_score": match.get("home_score"),
            "away_score": match.get("away_score"),
            "home_prob":  probs["home_prob"],
            "draw_prob":  probs["draw_prob"],
            "away_prob":  probs["away_prob"],
            "over25":     probs["over25_prob"],
            "btts":       probs["btts_prob"],
            "bets":       bets,
            "props":      props,
            "sport":      "soccer",
        })

    parlay = build_parlay(all_bets)
    parlays = [parlay] if parlay else []

    return {
        "games":   cards,
        "bets":    all_bets,
        "props":   all_props,
        "parlays": parlays,
        "sport":   "soccer",
        "date":    matches[0].get("date","") if matches else "",
    }
