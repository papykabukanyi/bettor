"""
RapidAPI – free-api-live-football-data fetcher
Host : free-api-live-football-data.p.rapidapi.com
Key  : b65cec1d35msh240f423a84de0abp19075ejsn7f2de12fbc00 (RAPIDAPI_KEY env var)

Documented endpoints (free tier):
  /football-get-all-leagues
  /football-get-all-teams-by-league-by-season?leagueId={id}&season={year}
  /football-get-all-fixtures-by-team-by-season?teamId={id}&season={year}
  /football-get-live-scores
  /football-players-search?playerName={name}
  /football-get-team-info?teamId={id}
  /football-get-fixture-info-by-id?fixtureId={id}
"""

import requests
import time
from datetime import date, datetime

from src.config import RAPIDAPI_KEY

_HOST    = "free-api-live-football-data.p.rapidapi.com"
_BASE    = f"https://{_HOST}"
_HEADERS = {
    "x-rapidapi-key":  RAPIDAPI_KEY,
    "x-rapidapi-host": _HOST,
}

# Map football-data.org competition ids to RapidAPI league ids where known
# (RapidAPI free tier may have limited league availability)
_LEAGUE_IDS = {
    "premier_league":   1,
    "la_liga":          2,
    "bundesliga":       3,
    "serie_a":          4,
    "ligue_1":          5,
    "champions_league": 6,
}

# ─── helpers ─────────────────────────────────────────────────────────────────

def _get(path: str, params: dict = None, timeout: int = 10):
    if not RAPIDAPI_KEY:
        return None
    url = f"{_BASE}/{path.lstrip('/')}"
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=_HEADERS,
                                params=params or {}, timeout=timeout)
            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            if resp.status_code in (401, 403):
                print(f"[rapidapi_football] {resp.status_code} – check key/subscription")
                return None
            resp.raise_for_status()
            data = resp.json()
            # API returns {"status":"success","response":{...}} or
            # {"status":"success","response":[...]}
            if isinstance(data, dict):
                inner = data.get("response", data)
                return inner
            return data
        except requests.exceptions.Timeout:
            print(f"[rapidapi_football] timeout {path}")
            return None
        except Exception as e:
            print(f"[rapidapi_football] {path} error: {e}")
            return None
    return None


# ─── leagues ─────────────────────────────────────────────────────────────────

def get_all_leagues() -> list[dict]:
    data = _get("/football-get-all-leagues")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("leagues", [])
    return []


# ─── live scores ─────────────────────────────────────────────────────────────

def get_live_scores() -> list[dict]:
    """Return currently live matches."""
    data = _get("/football-get-live-scores")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # may be {"matches": [...]} or similar
        for key in ("matches", "events", "games", "data"):
            if key in data and isinstance(data[key], list):
                return data[key]
    return []


# ─── fixtures ────────────────────────────────────────────────────────────────

def get_fixtures_by_team_season(team_id: int, season: int = None) -> list[dict]:
    season = season or date.today().year
    data = _get("/football-get-all-fixtures-by-team-by-season",
                {"teamId": team_id, "season": season})
    return data if isinstance(data, list) else []


def get_fixture_info(fixture_id: int) -> dict | None:
    data = _get("/football-get-fixture-info-by-id", {"fixtureId": fixture_id})
    if isinstance(data, list) and data:
        return data[0]
    return data if isinstance(data, dict) else None


# ─── teams ────────────────────────────────────────────────────────────────────

def get_team_info(team_id: int) -> dict | None:
    data = _get("/football-get-team-info", {"teamId": team_id})
    if isinstance(data, list) and data:
        return data[0]
    return data if isinstance(data, dict) else None


def get_teams_by_league_season(league_id: int, season: int = None) -> list[dict]:
    season = season or date.today().year
    data = _get("/football-get-all-teams-by-league-by-season",
                {"leagueId": league_id, "season": season})
    return data if isinstance(data, list) else []


# ─── players ─────────────────────────────────────────────────────────────────

def search_player(name: str) -> list[dict]:
    data = _get("/football-players-search", {"playerName": name})
    return data if isinstance(data, list) else []


# ─── form helpers ─────────────────────────────────────────────────────────────

def get_team_recent_form(team_id: int, n: int = 5) -> dict:
    """Return recent form dict for a team from their last N fixtures."""
    fixtures = get_fixtures_by_team_season(team_id)
    # Filter to finished matches only
    finished = [f for f in fixtures
                if (f.get("status","") or "").lower() in
                   ("finished","ft","aet","pens","full time")]
    # Sort by date descending
    def _parse_date(f):
        raw = f.get("date","") or f.get("event_date","") or ""
        try:
            return datetime.fromisoformat(raw[:10])
        except Exception:
            return datetime.min
    finished.sort(key=_parse_date, reverse=True)
    recent = finished[:n]
    form_chars = []
    for f in recent:
        hs = f.get("goals_home") or f.get("homeScore") or 0
        vs = f.get("goals_away") or f.get("awayScore") or 0
        try:
            hs, vs = int(hs), int(vs)
        except Exception:
            continue
        home_id = (f.get("home_team") or {}).get("id") or f.get("home_team_id")
        is_home = str(home_id) == str(team_id)
        if hs == vs:
            form_chars.append("D")
        elif (is_home and hs > vs) or (not is_home and vs > hs):
            form_chars.append("W")
        else:
            form_chars.append("L")
    wins   = form_chars.count("W")
    losses = form_chars.count("L")
    draws  = form_chars.count("D")
    total  = len(form_chars)
    return {
        "team_id":  team_id,
        "form":     "".join(form_chars),
        "wins":     wins,
        "losses":   losses,
        "draws":    draws,
        "form_pct": wins / total if total else 0.5,
    }


# ─── DB population helpers ────────────────────────────────────────────────────

def populate_live_scores():
    """Save live scores → match_events table."""
    from src.data.db import save_match_events
    live = get_live_scores()
    events = []
    for m in live:
        ht = m.get("home_team","") or m.get("home","")
        vt = m.get("away_team","") or m.get("away","")
        gd_raw = m.get("date","") or ""
        try:
            gd = date.fromisoformat(gd_raw[:10])
        except Exception:
            gd = date.today()
        events.append({
            "sport":      "soccer",
            "league":     m.get("league","") or m.get("competition",""),
            "home_team":  ht,
            "away_team":  vt,
            "game_date":  gd,
            "event_type": "live_score",
            "minute":     m.get("elapsed") or m.get("minute"),
            "player_name": None,
            "team":       None,
            "detail":     f"{m.get('goals_home',0)}-{m.get('goals_away',0)}",
            "source":     "rapidapi_football",
        })
    if events:
        save_match_events(events)
    print(f"[rapidapi_football] live scores: {len(events)} saved")
