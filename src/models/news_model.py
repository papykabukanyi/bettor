"""
Multi-Source Signal Aggregator
================================
Enriches model predictions with signals from multiple free APIs:

  1. NewsAPI          – sports news sentiment (free tier: 100 req/day)
  2. ESPN API         – unofficial, no key, team/player injury + recent form
  3. API-Football     – RapidAPI free tier (100 req/day), live standings + H2H
  4. TheSportsDB      – free multi-sport form (last 5 results)
  5. DB-backed        – standings / injuries stored by sportsdata/balldontlie fetchers

All sources fail gracefully – the pipeline works without any of them.

Public interface:
    get_news_signals(home_team, away_team, sport) → dict
    get_team_form_signals(team, sport, league)    → dict
    get_injury_signals(team, sport)               → dict
    enrich_prediction(pred_dict, sport)           → pred_dict (in-place + returns)
    aggregate_signals(home, away, sport, league)  → dict
"""

import os
import sys
import time
import json
import hashlib
import datetime
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config import NEWS_API_KEY, RAPIDAPI_KEY

# ---------------------------------------------------------------------------
# Config / env keys (all optional – graceful fallback)
# ---------------------------------------------------------------------------
_NEWSAPI_KEY      = NEWS_API_KEY
_RAPIDAPI_KEY     = RAPIDAPI_KEY       # for API-Football
_NEWS_CACHE: dict = {}          # in-process TTL cache
_CACHE_TTL        = 3600        # 1 hour

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _cache_get(key: str):
    entry = _NEWS_CACHE.get(key)
    if entry and time.time() - entry["ts"] < _CACHE_TTL:
        return entry["v"]
    return None

def _cache_set(key: str, value):
    _NEWS_CACHE[key] = {"ts": time.time(), "v": value}

def _safe_get(url: str, params: dict = None, headers: dict = None,
              timeout: int = 6) -> dict | list | None:
    """requests.get with hard timeout + silent exception."""
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

def _norm(s: str) -> str:
    return s.lower().replace(" fc","").replace(" cf","").replace(".", "").strip()

# ---------------------------------------------------------------------------
# 1. NewsAPI – sports sentiment
# ---------------------------------------------------------------------------
_NEWSAPI_BASE = "https://newsapi.org/v2/everything"

def _newsapi_articles(query: str, page_size: int = 5,
                       sport: str = "", team: str = "") -> list[dict]:
    if not _NEWSAPI_KEY:
        return []
    ck = f"news:{hashlib.md5(query.encode()).hexdigest()}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached
    data = _safe_get(_NEWSAPI_BASE, params={
        "q": query, "language": "en", "sortBy": "publishedAt",
        "pageSize": page_size, "apiKey": _NEWSAPI_KEY,
    })
    articles = data.get("articles", []) if data else []
    _cache_set(ck, articles)
    # Persist to DB in background (best-effort)
    if articles and (sport or team):
        try:
            from src.data.db import save_news_articles
            rows = []
            for a in articles:
                rows.append({
                    "sport":       sport,
                    "team":        team,
                    "headline":    (a.get("title") or "")[:500],
                    "description": (a.get("description") or "")[:1000],
                    "url":         (a.get("url") or "")[:500],
                    "source_name": ((a.get("source") or {}).get("name") or "")[:100],
                    "sentiment":   None,   # filled after scoring
                    "published_at": a.get("publishedAt"),
                })
            save_news_articles(rows)
        except Exception:
            pass
    return articles

def _sentiment_score(articles: list[dict]) -> float:
    """
    Naïve keyword sentiment on article titles/descriptions.
    Returns float in [-1.0, +1.0] (positive = good news for the team).
    """
    pos_words = {"win","won","victory","dominat","clinch","comeback","strong",
                 "healthy","return","hot","streak","form"}
    neg_words = {"loss","lost","injur","suspend","fired","crisis","poor",
                 "slump","relegat","blow","out","doubt","miss","absent"}
    score = 0.0
    count = 0
    for a in articles:
        text = ((a.get("title") or "") + " " + (a.get("description") or "")).lower()
        for w in pos_words:
            if w in text:
                score += 1
        for w in neg_words:
            if w in text:
                score -= 1
        count += 1
    if count == 0:
        return 0.0
    return max(-1.0, min(1.0, score / (count * 3)))

def get_news_signals(home_team: str, away_team: str, sport: str = "mlb") -> dict:
    """
    Returns dict with sentiment scores for home / away teams and matchup.
    Keys: home_sentiment, away_sentiment, matchup_sentiment (all -1..+1)
    """
    ck = f"news_sig:{_norm(home_team)}:{_norm(away_team)}:{sport}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    home_arts     = _newsapi_articles(f"{home_team} {sport}", sport=sport, team=home_team)
    away_arts     = _newsapi_articles(f"{away_team} {sport}", sport=sport, team=away_team)
    matchup_arts  = _newsapi_articles(f"{home_team} vs {away_team}", sport=sport)

    result = {
        "home_sentiment":    _sentiment_score(home_arts),
        "away_sentiment":    _sentiment_score(away_arts),
        "matchup_sentiment": _sentiment_score(matchup_arts),
        "home_article_count": len(home_arts),
        "away_article_count": len(away_arts),
    }
    _cache_set(ck, result)
    return result

# ---------------------------------------------------------------------------
# 2. ESPN API (unofficial / no key)
# ---------------------------------------------------------------------------
_ESPN_MLB_BASE  = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb"
_ESPN_SOC_BASE  = "https://site.api.espn.com/apis/site/v2/sports/soccer"

_ESPN_LEAGUE_IDS = {
    "EPL": "eng.1", "ESP": "esp.1", "GER": "ger.1",
    "ITA": "ita.1", "FRA": "fra.1", "MLS": "usa.1",
}

def _espn_team_id(team_name: str, sport: str) -> str | None:
    """Search ESPN for a team slug."""
    ck = f"espn_id:{_norm(team_name)}:{sport}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    if sport == "mlb":
        data = _safe_get(f"{_ESPN_MLB_BASE}/teams")
    else:
        league = "eng.1"   # default
        data = _safe_get(f"{_ESPN_SOC_BASE}/{league}/teams")

    if not data:
        return None

    sports = data.get("sports", [{}])
    leagues = sports[0].get("leagues", [{}]) if sports else [{}]
    teams   = leagues[0].get("teams", []) if leagues else []

    norm_query = _norm(team_name)
    for entry in teams:
        t = entry.get("team", {})
        if norm_query in _norm(t.get("displayName", "")) or \
           norm_query in _norm(t.get("shortDisplayName", "")) or \
           norm_query in _norm(t.get("abbreviation", "")):
            tid = t.get("id")
            _cache_set(ck, tid)
            return tid
    return None

def get_team_form_signals(team: str, sport: str = "mlb", league: str = "EPL") -> dict:
    """
    Fetch recent form (last 5 results) from ESPN API.
    Returns dict: wins, losses, draws, form_pct, streak
    """
    ck = f"form:{_norm(team)}:{sport}:{league}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    default = {"wins": 0, "losses": 0, "draws": 0, "form_pct": 0.5, "streak": 0,
               "source": "default"}

    try:
        if sport == "mlb":
            # ESPN scoreboard – parse last 5 game outcomes
            data = _safe_get(f"{_ESPN_MLB_BASE}/scoreboard",
                             params={"limit": 10, "dates": ""})
        else:
            league_id = _ESPN_LEAGUE_IDS.get(league, "eng.1")
            data = _safe_get(f"{_ESPN_SOC_BASE}/{league_id}/scoreboard",
                             params={"limit": 10})

        if not data:
            return default

        events = data.get("events", [])
        norm_t = _norm(team)
        wins = losses = draws = 0
        streak = 0

        for ev in events[:10]:
            comps = ev.get("competitions", [{}])
            comp  = comps[0] if comps else {}
            competitors = comp.get("competitors", [])
            team_comp = None
            for c in competitors:
                cn = _norm(c.get("team", {}).get("displayName", ""))
                if norm_t in cn:
                    team_comp = c
                    break
            if not team_comp:
                continue
            winner = team_comp.get("winner", False)
            score1 = int(team_comp.get("score", 0) or 0)
            score2 = 0
            for c in competitors:
                if c is not team_comp:
                    score2 = int(c.get("score", 0) or 0)
            if winner:
                wins   += 1
                streak  = max(0, streak) + 1
            elif score1 == score2:
                draws  += 1
                streak  = 0
            else:
                losses += 1
                streak  = min(0, streak) - 1

        total = wins + losses + draws
        form_pct = wins / total if total > 0 else 0.5
        result = {"wins": wins, "losses": losses, "draws": draws,
                  "form_pct": form_pct, "streak": streak, "source": "espn"}
        _cache_set(ck, result)
        return result
    except Exception:
        return default

# ---------------------------------------------------------------------------
# 3. API-Football (RapidAPI, 100 free req/day)
# ---------------------------------------------------------------------------
_APIFOOTBALL_BASE = "https://api-football-v1.p.rapidapi.com/v3"
_APIFOOTBALL_HEADERS = lambda: {
    "X-RapidAPI-Key":  _RAPIDAPI_KEY,
    "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com",
}

_APIFOOTBALL_LEAGUE_IDS = {
    "EPL": 39, "ESP": 140, "GER": 78,
    "ITA": 135, "FRA": 61, "MLS": 253,
}

def _apifootball_h2h(home_id: int, away_id: int, last: int = 5) -> list[dict]:
    if not _RAPIDAPI_KEY:
        return []
    ck = f"h2h:{home_id}:{away_id}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached
    data = _safe_get(f"{_APIFOOTBALL_BASE}/fixtures/headtohead",
                     params={"h2h": f"{home_id}-{away_id}", "last": last},
                     headers=_APIFOOTBALL_HEADERS())
    results = (data or {}).get("response", [])
    _cache_set(ck, results)
    return results

def _apifootball_standings(league: str) -> list[dict]:
    if not _RAPIDAPI_KEY:
        return []
    league_id = _APIFOOTBALL_LEAGUE_IDS.get(league)
    if not league_id:
        return []
    ck = f"standings:{league}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached
    season = datetime.date.today().year
    data = _safe_get(f"{_APIFOOTBALL_BASE}/standings",
                     params={"league": league_id, "season": season},
                     headers=_APIFOOTBALL_HEADERS())
    resp = (data or {}).get("response", [])
    flat = []
    for r in resp:
        for group in r.get("league", {}).get("standings", []):
            flat.extend(group)
    _cache_set(ck, flat)
    return flat

def _apifootball_standing_for_team(team_name: str, standings: list) -> dict | None:
    norm_t = _norm(team_name)
    for entry in standings:
        tn = _norm(entry.get("team", {}).get("name", ""))
        if norm_t in tn or tn in norm_t:
            return entry
    return None

# ---------------------------------------------------------------------------
# 4. Injury signals (DB-backed + ESPN + NewsAPI)
# ---------------------------------------------------------------------------
def get_injury_signals(team: str, sport: str = "mlb") -> dict:
    """
    Returns dict: injured_key_players (list), injury_severity (0..1)
    0 = no injuries, 1 = multiple key players out
    """
    ck = f"injuries:{_norm(team)}:{sport}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    injured = []
    severity = 0.0

    # ── DB-backed injuries (populated by sportsdata/balldontlie fetchers) ──
    try:
        from src.data.db import get_injuries
        db_injuries = get_injuries(sport)
        norm_t = _norm(team)
        for inj in db_injuries:
            if norm_t in _norm(inj.get("team","")) or \
               norm_t in _norm(inj.get("player_name","")):
                status = inj.get("status","").lower()
                if any(s in status for s in ["out","doubtful","questionable","ir","injured"]):
                    injured.append(inj.get("player_name",""))
                    if "out" in status or "ir" in status:
                        severity = min(1.0, severity + 0.30)
                    elif "doubtful" in status:
                        severity = min(1.0, severity + 0.20)
                    else:
                        severity = min(1.0, severity + 0.10)
    except Exception:
        pass

    # NewsAPI injury scan (secondary source)
    articles = _newsapi_articles(f"{team} injury out ruled", page_size=8,
                                  sport=sport, team=team)
    injury_kw = ["injur","ruled out","doubtful","out for","misses","sidelined","suspended"]
    for a in articles:
        text = ((a.get("title") or "") + " " + (a.get("description") or "")).lower()
        if any(k in text for k in injury_kw):
            injured.append(a.get("title", ""))
            severity = min(1.0, severity + 0.15)

    result = {
        "injured_reports": [str(i) for i in injured[:5]],
        "injury_severity": round(severity, 2),
        "source": "db+newsapi",
    }
    _cache_set(ck, result)
    return result


# ---------------------------------------------------------------------------
# 5. TheSportsDB form signals (multi-sport, free, no key)
# ---------------------------------------------------------------------------

def _thesportsdb_form(team: str) -> dict:
    """Get last-5 form from TheSportsDB for any team."""
    ck = f"tsdb_form:{_norm(team)}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached
    default = {"wins": 0, "losses": 0, "draws": 0, "form_pct": 0.5,
               "form": "", "streak": 0, "source": "tsdb"}
    try:
        from src.data.thesportsdb_fetcher import get_team_form
        result = get_team_form(team)
        if result:
            result["source"] = "thesportsdb"
            _cache_set(ck, result)
            return result
    except Exception:
        pass
    return default


# ---------------------------------------------------------------------------
# 6. DB-backed standings helper
# ---------------------------------------------------------------------------

def _db_standing_for_team(team: str, sport: str, league: str) -> dict | None:
    """Return DB standings row for a team (normalised match)."""
    try:
        from src.data.db import get_standings
        rows = get_standings(sport, league=league)
        norm_t = _norm(team)
        for r in rows:
            if norm_t in _norm(r.get("team","")):
                return r
    except Exception:
        pass
    return None


def aggregate_signals(home_team: str, away_team: str,
                      sport: str = "mlb", league: str = "") -> dict:
    """
    Pull all available signals and aggregate into probability adjustments.

    Returns:
        home_adj   : float delta to add to home win probability (-0.1..+0.1)
        away_adj   : float delta to add to away win probability (-0.1..+0.1)
        confidence_boost : float delta to add to safety score
        signals    : raw signal dict for logging
    """
    ck = f"agg:{_norm(home_team)}:{_norm(away_team)}:{sport}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    signals = {}
    home_adj = 0.0
    away_adj = 0.0
    confidence_boost = 0.0

    # ── News sentiment ──────────────────────────────────────────────
    try:
        news = get_news_signals(home_team, away_team, sport)
        signals["news"] = news
        home_adj  += news["home_sentiment"]  * 0.025
        away_adj  += news["away_sentiment"]  * 0.025
        confidence_boost += abs(news["matchup_sentiment"]) * 0.01
    except Exception:
        pass

    # ── ESPN form ───────────────────────────────────────────────────
    try:
        hform = get_team_form_signals(home_team, sport, league)
        aform = get_team_form_signals(away_team, sport, league)
        signals["home_form"] = hform
        signals["away_form"] = aform
        home_adj  += (hform["form_pct"] - 0.5) * 0.04
        away_adj  += (aform["form_pct"] - 0.5) * 0.04
        if hform.get("streak", 0) > 2:
            confidence_boost += 0.02
        if aform.get("streak", 0) < -2:
            confidence_boost += 0.01
    except Exception:
        pass

    # ── TheSportsDB form (multi-sport fallback / secondary confirmation) ──
    try:
        h_tsdb = _thesportsdb_form(home_team)
        a_tsdb = _thesportsdb_form(away_team)
        if h_tsdb.get("form"):
            signals["home_form_tsdb"] = h_tsdb
            home_adj += (h_tsdb["form_pct"] - 0.5) * 0.03
            if h_tsdb.get("streak", 0) > 2:
                confidence_boost += 0.01
        if a_tsdb.get("form"):
            signals["away_form_tsdb"] = a_tsdb
            away_adj += (a_tsdb["form_pct"] - 0.5) * 0.03
    except Exception:
        pass

    # ── Injury signals (DB-backed + NewsAPI) ──────────────────────
    try:
        hinj = get_injury_signals(home_team, sport)
        ainj = get_injury_signals(away_team, sport)
        signals["home_injuries"] = hinj
        signals["away_injuries"] = ainj
        home_adj -= hinj["injury_severity"] * 0.04
        away_adj -= ainj["injury_severity"] * 0.04
        if hinj["injury_severity"] > 0.3 or ainj["injury_severity"] > 0.3:
            confidence_boost += 0.01   # key injury info improves model
    except Exception:
        pass

    # ── DB standings (populated by sportsdata / balldontlie fetchers) ──
    try:
        hst = _db_standing_for_team(home_team, sport, league)
        ast = _db_standing_for_team(away_team, sport, league)
        if hst and ast:
            hr  = hst.get("rank") or 10
            ar  = ast.get("rank") or 10
            hpt = hst.get("points") or hst.get("wins") or 0
            apt = ast.get("points") or ast.get("wins") or 0
            rank_diff = (ar - hr) / 20.0
            pts_diff  = (hpt - apt) / 30.0
            home_adj  += rank_diff * 0.025
            home_adj  += pts_diff  * 0.015
            away_adj  -= rank_diff * 0.025
            confidence_boost += 0.015
            signals["db_standings"] = {"home_rank": hr, "away_rank": ar,
                                       "home_pts": hpt, "away_pts": apt,
                                       "source": hst.get("source","")}
    except Exception:
        pass

    # ── API-Football standings (soccer only, live) ──────────────────
    if sport == "soccer" and league and _RAPIDAPI_KEY:
        try:
            standings = _apifootball_standings(league)
            if standings:
                hst2 = _apifootball_standing_for_team(home_team, standings)
                ast2 = _apifootball_standing_for_team(away_team, standings)
                if hst2 and ast2:
                    hr, ar = hst2.get("rank", 10), ast2.get("rank", 10)
                    rank_diff = (ar - hr) / 20.0
                    home_adj  += rank_diff * 0.03
                    away_adj  -= rank_diff * 0.03
                    hpts = hst2.get("points", 0)
                    apts = ast2.get("points", 0)
                    pts_diff = (hpts - apts) / 30.0
                    home_adj  += pts_diff * 0.02
                    confidence_boost += 0.01
                    signals["standings"] = {"home_rank": hr, "away_rank": ar,
                                            "home_pts": hpts, "away_pts": apts}
        except Exception:
            pass

    # ── Clamp adjustments ─────────────────────────────────────────
    home_adj         = max(-0.10, min(0.10, home_adj))
    away_adj         = max(-0.10, min(0.10, away_adj))
    confidence_boost = max(0.0,  min(0.05, confidence_boost))

    result = {
        "home_adj":         home_adj,
        "away_adj":         away_adj,
        "confidence_boost": confidence_boost,
        "signals":          signals,
    }
    _cache_set(ck, result)
    return result


def enrich_prediction(pred: dict, sport: str = "mlb", league: str = "") -> dict:
    """
    Apply multi-source signal adjustments to an existing prediction dict.
    Mutates pred in-place (also returns it for chaining).

    Expected pred keys: home_team, away_team,
                        home_win_prob / home_win, away_win_prob / away_win
    Adds: home_win_adj, away_win_adj, signal_boost, signal_sources
    """
    home = pred.get("home_team", "")
    away = pred.get("away_team", "")
    if not home or not away:
        return pred

    try:
        agg = aggregate_signals(home, away, sport, league)

        # Normalised base probs
        hp = float(pred.get("home_win_prob") or pred.get("home_win") or 0.5)
        ap = float(pred.get("away_win_prob") or pred.get("away_win") or 0.5)

        hp_adj = min(0.97, max(0.03, hp + agg["home_adj"]))
        ap_adj = min(0.97, max(0.03, ap + agg["away_adj"]))

        # Renormalise if 1X2
        dp = float(pred.get("draw_prob") or pred.get("draw") or 0.0)
        if dp > 0:
            total = hp_adj + dp + ap_adj
            hp_adj /= total
            ap_adj /= total
            dp      = dp / total
            pred["draw_prob"] = round(dp, 4)

        pred["home_win_adj"]   = round(hp_adj, 4)
        pred["away_win_adj"]   = round(ap_adj, 4)
        pred["signal_boost"]   = round(agg["confidence_boost"], 4)
        pred["signal_sources"] = list(agg["signals"].keys())

        # Convenience: also update primary keys so downstream uses enriched values
        if "home_win_prob" in pred:
            pred["home_win_prob"] = hp_adj
            pred["away_win_prob"] = ap_adj
        if "home_win" in pred:
            pred["home_win"] = hp_adj
            pred["away_win"] = ap_adj
    except Exception as exc:
        print(f"[news_model] enrich_prediction error: {exc}")

    return pred
