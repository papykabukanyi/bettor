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

import datetime
import re
from typing import Any

try:
    import requests
except ImportError:
    requests = None

from models.soccer_model import predict as _model_predict, STAGE_MAP
from data.club_stats_fetcher import get_squad_props, get_wc_player_stats
from data.wc2026_fetcher import TEAM_ELO, get_wc_odds

try:
    from data.soccer_news import get_match_news_signal, get_injury_alerts
    _NEWS_AVAILABLE = True
except ImportError:
    _NEWS_AVAILABLE = False
    def get_match_news_signal(h, a): return {}
    def get_injury_alerts(t): return []

try:
    from data.soccer_fetcher import get_competition_odds, FD_TO_ODDS_SPORT, TOURNAMENTS, get_team_squad
except ImportError:
    def get_competition_odds(code): return []
    def get_team_squad(team_id=None, team_name=None, competition_code=None): return []
    FD_TO_ODDS_SPORT = {}
    TOURNAMENTS = {}


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


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _norm_name(name: str | None) -> str:
    return " ".join(str(name or "").strip().lower().split())


def _position_bucket(position: str | None) -> str:
    p = str(position or "").strip().lower()
    if not p:
        return "mid"
    if "goal" in p or p in {"gk", "keeper"}:
        return "gk"
    if "def" in p or "back" in p:
        return "def"
    if "for" in p or "striker" in p or "attack" in p or "wing" in p:
        return "att"
    if "mid" in p:
        return "mid"
    return "mid"


def _competition_candidates(match_competition: str | None) -> list[str]:
    seen = set()
    out: list[str] = []

    def _add(code: str | None):
        c = str(code or "").strip().upper()
        if not c or c in seen:
            return
        seen.add(c)
        out.append(c)

    # Prefer the declared competition if it is one of our supported codes.
    _add(match_competition)

    # Core football-data competitions used by this project.
    for code in ("WC", "CL", "PL", "BL1", "SA", "PD", "FL1", "DED", "PPL", "BSA", "ELC"):
        _add(code)

    for code in TOURNAMENTS.keys():
        _add(code)

    return out


_ESPN_LEAGUE_MAP = {
    "PL": "eng.1",
    "BL1": "ger.1",
    "SA": "ita.1",
    "PD": "esp.1",
    "FL1": "fra.1",
    "DED": "ned.1",
    "PPL": "por.1",
    "CL": "uefa.champions",
    "ELC": "eng.2",
    "BSA": "bra.1",
    "MLS": "usa.1",
}


def _espn_slug_for_comp(code: str | None) -> str:
    return _ESPN_LEAGUE_MAP.get(str(code or "").strip().upper(), "")


def _extract_espn_team_id(crest_url: str | None) -> str:
    url = str(crest_url or "")
    if not url:
        return ""
    m = re.search(r"/(\d+)\.(?:png|svg|jpg|jpeg)(?:\?|$)", url, flags=re.IGNORECASE)
    return m.group(1) if m else ""


def _fetch_espn_roster(team_id: str, league_slug: str) -> list[dict]:
    if not requests or not team_id or not league_slug:
        return []
    url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{league_slug}/teams/{team_id}/roster"
    try:
        resp = requests.get(url, timeout=8)
        if resp.status_code != 200:
            return []
        data = resp.json() or {}
    except Exception:
        return []

    roster: list[dict] = []
    seen = set()
    athletes = data.get("athletes", []) or []

    # ESPN currently returns either:
    # 1) flat list of athletes, or
    # 2) grouped list where each item has an `items` array.
    flat_items: list[dict] = []
    if athletes and isinstance(athletes, list) and isinstance(athletes[0], dict) and isinstance(athletes[0].get("items"), list):
        for group in athletes:
            for item in group.get("items", []) or []:
                if isinstance(item, dict):
                    flat_items.append(item)
    else:
        flat_items = [a for a in athletes if isinstance(a, dict)]

    for item in flat_items:
        name = str(item.get("displayName") or item.get("fullName") or item.get("shortName") or "").strip()
        if not name:
            continue
        key = _norm_name(name)
        if key in seen:
            continue
        seen.add(key)

        pos_obj = item.get("position") or item.get("defaultPosition") or {}
        if isinstance(pos_obj, dict):
            position = pos_obj.get("displayName") or pos_obj.get("abbreviation") or ""
        else:
            position = str(pos_obj or "")

        roster.append({
            "id": str(item.get("id") or ""),
            "name": name,
            "position": position,
            "nationality": item.get("citizenship") or item.get("nationality") or "",
        })
    return roster


def _fallback_profile_players(team_name: str) -> list[dict]:
    norm_team = _norm_name(team_name)
    if not norm_team:
        return []
    players = get_wc_player_stats() or []
    out: list[dict] = []
    seen = set()
    for p in players:
        club = _norm_name(p.get("team"))
        if not club:
            continue
        if club == norm_team or norm_team in club or club in norm_team:
            name = str(p.get("name") or "").strip()
            if not name:
                continue
            key = _norm_name(name)
            if key in seen:
                continue
            seen.add(key)
            out.append(p)
    return out


def _resolve_team_squad(
    team_name: str,
    team_id: Any,
    match_competition: str | None,
    team_crest: str | None = None,
) -> list[dict]:
    if team_id:
        squad = get_team_squad(team_id=team_id)
        if squad:
            return squad

    for comp_code in _competition_candidates(match_competition):
        squad = get_team_squad(team_name=team_name, competition_code=comp_code)
        if squad:
            return squad

    # Fallback: ESPN roster endpoint using crest-derived team id.
    espn_team_id = _extract_espn_team_id(team_crest)
    if espn_team_id:
        slugs = []
        for comp_code in _competition_candidates(match_competition):
            slug = _espn_slug_for_comp(comp_code)
            if slug and slug not in slugs:
                slugs.append(slug)
        if not slugs:
            slugs = list(dict.fromkeys(_ESPN_LEAGUE_MAP.values()))

        for slug in slugs:
            squad = _fetch_espn_roster(espn_team_id, slug)
            if squad:
                return squad

    # Final fallback: match known club profiles.
    fallback = _fallback_profile_players(team_name)
    if fallback:
        return fallback

    return []


def _side_win_prob(team_name: str, match: dict, match_probs: dict | None) -> float:
    probs = match_probs or {}
    if str(team_name or "") == str(match.get("home_team") or ""):
        return float(probs.get("home_prob", 0.5) or 0.5)
    if str(team_name or "") == str(match.get("away_team") or ""):
        return float(probs.get("away_prob", 0.5) or 0.5)
    return 0.5


def _player_stat_row(player_name: str) -> dict:
    rows = get_wc_player_stats(player_name=player_name) or []
    return rows[0] if rows else {}


def _build_player_prop(player: dict, team_name: str, match: dict, team_win_prob: float) -> dict | None:
    name = str(player.get("name") or "").strip()
    if not name:
        return None

    pos_raw = player.get("position")
    pos_bucket = _position_bucket(pos_raw)
    stat_row = _player_stat_row(name)

    stat_type = "key_passes"
    prop_label = "Key Passes"
    line = 1.5
    season_avg = None
    model_prob = 0.55
    rationale = "Position-based baseline projection."

    if pos_bucket == "gk":
        stat_type = "saves"
        prop_label = "Saves"
        line = 2.5
        model_prob = 0.55 + max(0.0, (0.58 - team_win_prob)) * 0.45
        rationale = "Goalkeeper save volume projection from expected shot pressure."
    elif pos_bucket == "def":
        stat_type = "tackles"
        prop_label = "Tackles"
        line = 1.5
        model_prob = 0.54 + max(0.0, (0.55 - team_win_prob)) * 0.25
        rationale = "Defender tackling projection from expected defensive workload."
    elif pos_bucket == "mid":
        stat_type = "key_passes"
        prop_label = "Key Passes"
        line = 1.5
        model_prob = 0.54 + (team_win_prob - 0.5) * 0.16
        rationale = "Midfield creativity projection adjusted by match control expectation."
    elif pos_bucket == "att":
        stat_type = "shots_on_target"
        prop_label = "Shots on Target"
        line = 1.5
        model_prob = 0.56 + (team_win_prob - 0.5) * 0.20
        rationale = "Attacker shot-on-target projection adjusted by team win probability."

    try:
        mins_90 = float(stat_row.get("minutes_90s", 0) or 0)
    except (TypeError, ValueError):
        mins_90 = 0.0
    try:
        goals_p90 = float(stat_row.get("goals_per90", 0) or 0)
    except (TypeError, ValueError):
        goals_p90 = 0.0
    try:
        xg_p90 = float(stat_row.get("xg_per90", 0) or 0)
    except (TypeError, ValueError):
        xg_p90 = 0.0
    try:
        sot_total = float(stat_row.get("shots_on_target", 0) or 0)
    except (TypeError, ValueError):
        sot_total = 0.0
    try:
        xa_total = float(stat_row.get("xa", 0) or 0)
    except (TypeError, ValueError):
        xa_total = 0.0

    sot_p90 = (sot_total / mins_90) if mins_90 > 0 else 0.0
    xa_p90 = (xa_total / mins_90) if mins_90 > 0 else 0.0

    if pos_bucket == "att":
        if sot_p90 > 0:
            season_avg = round(sot_p90, 2)
            model_prob = 0.44 + min(0.34, sot_p90 * 0.24) + (team_win_prob - 0.5) * 0.15
            rationale = f"{sot_p90:.2f} shots on target per 90 with {xg_p90:.2f} xG per 90."
        elif goals_p90 > 0:
            season_avg = round(goals_p90, 2)
            model_prob = 0.40 + min(0.30, goals_p90 * 0.50) + (team_win_prob - 0.5) * 0.14
            rationale = f"{goals_p90:.2f} goals per 90 profile driving attacking projection."
    elif pos_bucket == "mid":
        if xa_p90 > 0:
            season_avg = round(max(xa_p90 * 3.0, 0.1), 2)
            model_prob = 0.46 + min(0.25, xa_p90 * 0.80) + (team_win_prob - 0.5) * 0.12
            rationale = f"Creative midfield profile ({xa_p90:.2f} xA per 90)."
    elif pos_bucket == "gk" and mins_90 > 0:
        season_avg = 3.0
    elif pos_bucket == "def":
        season_avg = 1.8

    model_prob = round(_clamp(model_prob, 0.22, 0.86), 3)
    dec_odds = 1.91
    implied = round(1.0 / dec_odds, 4)
    edge = _edge(model_prob, implied)
    ev = round((dec_odds - 1.0) * model_prob - (1.0 - model_prob), 4)

    prop = {
        "name": name,
        "team": team_name,
        "nation": team_name,
        "position": pos_raw or "",
        "stat_type": stat_type,
        "prop_label": prop_label,
        "line": line,
        "direction": "OVER",
        "model_prob": model_prob,
        "confidence": int(round(model_prob * 100)),
        "safety_label": _safety(model_prob),
        "season_avg": season_avg,
        "signal_rationale": rationale,
        "odds_am": -110,
        "dec_odds": dec_odds,
        "edge": edge,
        "ev": ev,
        "sport": "soccer",
    }

    club_team = str(stat_row.get("team") or "").strip()
    if club_team:
        prop["club_team"] = club_team

    return prop


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
    gfh, gah, xfh, xah = _team_stats(home)
    gfa, gaa, xfa, xaa = _team_stats(away)
    stage_id = STAGE_MAP.get(stage.lower(), 0)

    probs = _model_predict(
        home_elo=h_elo,    away_elo=a_elo,
        goals_for_h=gfh,   goals_ag_h=gah,
        goals_for_a=gfa,   goals_ag_a=gaa,
        xg_for_h=xfh,      xg_ag_h=xah,
        xg_for_a=xfa,      xg_ag_a=xaa,
        stage=stage_id,
    )

    # ── Sentiment adjustment (small nudge, max ±3%) ────────────────────────
    if use_sentiment and _NEWS_AVAILABLE:
        try:
            signal = get_match_news_signal(home, away)
            combined = signal.get("combined_signal", 0.0)  # home - away, -2 to +2
            # Scale: clamp to ±1, multiply by 0.03 → max ±0.03 shift
            adj = max(-1.0, min(1.0, combined)) * 0.03
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


def get_player_props(
    match: dict,
    include_all_players: bool = True,
    match_probs: dict | None = None,
) -> list[dict]:
    """
    Build player prop bets for a WC match.
    Uses club_stats_fetcher to get top players for each national team.
    """
    home = match["home_team"]
    away = match["away_team"]
    competition = match.get("competition", "")
    game_key = match.get("game_key") or f"{match.get('date','')}#{away}@{home}"
    date_str = match.get("date") or match.get("game_date") or ""
    match_key = match.get("match_key", "")

    home_props: list[dict] = []
    away_props: list[dict] = []

    if include_all_players:
        home_team_id = match.get("home_id")
        away_team_id = match.get("away_id")

        home_squad = _resolve_team_squad(home, home_team_id, competition, match.get("home_crest"))
        away_squad = _resolve_team_squad(away, away_team_id, competition, match.get("away_crest"))

        home_prob = _side_win_prob(home, match, match_probs)
        away_prob = _side_win_prob(away, match, match_probs)

        for player in home_squad:
            prop = _build_player_prop(player, home, match, home_prob)
            if prop:
                home_props.append(prop)
        for player in away_squad:
            prop = _build_player_prop(player, away, match, away_prob)
            if prop:
                away_props.append(prop)

        # Fallback for national-team contexts where squad endpoints are incomplete.
        if not home_props:
            for player in (get_wc_player_stats(nation=home) or []):
                prop = _build_player_prop(player, home, match, home_prob)
                if prop:
                    home_props.append(prop)
        if not away_props:
            for player in (get_wc_player_stats(nation=away) or []):
                prop = _build_player_prop(player, away, match, away_prob)
                if prop:
                    away_props.append(prop)
    else:
        home_props = get_squad_props(home, top_n=5)
        away_props = get_squad_props(away, top_n=5)

    all_props = home_props + away_props

    dedup: dict[tuple[str, str, str], dict] = {}
    for prop in all_props:
        key = (
            _norm_name(prop.get("name")),
            str(prop.get("team") or ""),
            str(prop.get("stat_type") or ""),
        )
        prev = dedup.get(key)
        if prev is None or float(prop.get("model_prob", 0) or 0) > float(prev.get("model_prob", 0) or 0):
            dedup[key] = prop

    all_props = list(dedup.values())
    all_props.sort(key=lambda p: (
        0 if str(p.get("team") or "") == away else 1,
        -(float(p.get("model_prob", 0) or 0)),
        str(p.get("name") or ""),
    ))

    for p in all_props:
        p["game"] = p.get("game") or f"{away}@{home}"
        p["game_key"] = game_key
        p["date"] = date_str
        p["home_team"] = home
        p["away_team"] = away
        p["match_key"] = match_key
        p["competition"] = competition
        p["sport"] = "soccer"

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
    odds_cache: dict[str, list[dict]] = {}

    def _odds_for_comp(comp_code: str) -> list[dict]:
        cc = str(comp_code or competition_code or "WC")
        if cc in odds_cache:
            return odds_cache[cc]
        try:
            odds = get_competition_odds(cc) or []
        except Exception:
            odds = []
        if not odds and cc == "WC":
            try:
                odds = get_wc_odds()
            except Exception:
                odds = []
        odds_cache[cc] = odds
        return odds

    all_bets:  list[dict] = []
    all_props: list[dict] = []
    cards:     list[dict] = []

    for match in matches:
        home  = match["home_team"]
        away  = match["away_team"]
        stage = match.get("stage", "group")
        comp_code = match.get("competition", competition_code)

        probs = predict_match(home, away, stage, use_sentiment=True)
        bets = build_match_bets(match, _odds_for_comp(comp_code))
        props = get_player_props(match, include_all_players=True, match_probs=probs)
        all_bets.extend(bets)
        all_props.extend(props)


        # Injury alerts for UI display
        home_injuries = []
        away_injuries = []
        if _NEWS_AVAILABLE:
            try:
                home_injuries = get_injury_alerts(home)
                away_injuries = get_injury_alerts(away)
            except Exception:
                pass

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
