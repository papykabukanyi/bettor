"""
TheSportsDB fetcher  –  free tier (key=1) – multi-sport.
Base: https://www.thesportsdb.com/api/v1/json/{key}/

Free tier endpoints:
  /eventsday.php?d=YYYY-MM-DD&s=Soccer          – events by date + sport
  /eventsnext.php?id={team_id}                  – next events for a team
  /eventslast.php?id={team_id}                  – last 5 events for a team
  /searchteams.php?t={team_name}                – search team
  /searchplayers.php?t={team_id}                – players by team
  /searchplayers.php?p={player_name}            – search by name
  /lookupleague.php?id={league_id}              – league details
  /lookuptable.php?l={league_id}&s={season}     – standings table
  /searchevents.php?e={event}                   – search events

Common league IDs:
  4328 = English Premier League
  4335 = La Liga
  4331 = Bundesliga
  4332 = Serie A
  4334 = Ligue 1
  4346 = MLS
  4387 = NBA
  4424 = MLB
"""

import requests
import time
from datetime import date, datetime

from src.config import THESPORTSDB_API_KEY

_KEY  = THESPORTSDB_API_KEY or "1"
_BASE = f"https://www.thesportsdb.com/api/v1/json/{_KEY}"

_SOCCER_LEAGUES = {
    "premier_league": 4328,
    "la_liga":        4335,
    "bundesliga":     4331,
    "serie_a":        4332,
    "ligue_1":        4334,
    "mls":            4346,
    "champions_league": 4480,
}

_SPORT_TO_TSDB = {
    "soccer": "Soccer",
    "mlb":    "Baseball",
}

# ─── helpers ─────────────────────────────────────────────────────────────────

def _get(endpoint: str, params: dict = None, timeout: int = 8):
    url = f"{_BASE}/{endpoint}"
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params or {}, timeout=timeout)
            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            print(f"[thesportsdb] timeout {endpoint}")
            return None
        except Exception as e:
            print(f"[thesportsdb] error {endpoint}: {e}")
            return None
    return None


# ─── events / fixtures ────────────────────────────────────────────────────────

def get_events_by_date(d: date = None, sport: str = "Soccer") -> list[dict]:
    """Return all events on a date for a given sport."""
    d = d or date.today()
    tsdb_sport = _SPORT_TO_TSDB.get(sport.lower(), sport)
    # Try with sport filter first; fall back to date-only (free tier compat)
    data = _get("eventsday.php", {"d": d.isoformat(), "s": tsdb_sport})
    events = (data or {}).get("events")
    if not events:
        data = _get("eventsday.php", {"d": d.isoformat()})
        events = (data or {}).get("events") or []
    if sport.lower() not in ("", "all"):
        events = [e for e in events
                  if e.get("strSport","").lower() == tsdb_sport.lower()]
    return events


def get_team_next_events(team_id: int) -> list[dict]:
    data = _get("eventsnext.php", {"id": team_id})
    return (data or {}).get("events") or []


def get_team_last_events(team_id: int) -> list[dict]:
    data = _get("eventslast.php", {"id": team_id})
    return (data or {}).get("events") or []


# ─── teams ────────────────────────────────────────────────────────────────────

def search_team(name: str) -> list[dict]:
    data = _get("searchteams.php", {"t": name})
    return (data or {}).get("teams") or []


def get_team_id(name: str) -> int | None:
    teams = search_team(name)
    return int(teams[0]["idTeam"]) if teams else None


# ─── players ─────────────────────────────────────────────────────────────────

def get_players_by_team(team_id: int) -> list[dict]:
    data = _get("lookup_all_players.php", {"id": team_id})
    return (data or {}).get("player") or []


def search_player(name: str) -> list[dict]:
    data = _get("searchplayers.php", {"p": name})
    return (data or {}).get("player") or []


# ─── standings ────────────────────────────────────────────────────────────────

def get_standings(league_id: int, season: str = None) -> list[dict]:
    season = season or str(date.today().year)
    data = _get("lookuptable.php", {"l": league_id, "s": season})
    return (data or {}).get("table") or []


# ─── form / recent results ────────────────────────────────────────────────────

def get_team_form(team_name: str) -> dict:
    """Return dict with form string (e.g. 'WWLDW') for a team."""
    team_id = get_team_id(team_name)
    if not team_id:
        return {}
    events = get_team_last_events(team_id)
    form_chars = []
    for ev in events[:5]:
        home  = ev.get("strHomeTeam","")
        away  = ev.get("strAwayTeam","")
        hs    = ev.get("intHomeScore")
        vs    = ev.get("intAwayScore")
        if hs is None or vs is None:
            continue
        hs, vs = int(hs), int(vs)
        is_home = home.lower() == team_name.lower()
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
        "form":       "".join(form_chars),
        "wins":       wins,
        "losses":     losses,
        "draws":      draws,
        "form_pct":   wins / total if total else 0.5,
        "streak":     _streak(form_chars),
        "raw_events": events[:5],
    }


def _streak(chars: list) -> int:
    """Positive streak = consecutive Ws, negative = consecutive Ls."""
    if not chars:
        return 0
    last = chars[0]
    n = 0
    for c in chars:
        if c == last:
            n += 1
        else:
            break
    return n if last == "W" else -n


# ─── DB population helpers ────────────────────────────────────────────────────

def _soccer_season() -> str:
    """Return current soccer season as 'YYYY-YYYY' (e.g. '2025-2026')."""
    today = date.today()
    if today.month >= 8:
        return f"{today.year}-{today.year + 1}"
    return f"{today.year - 1}-{today.year}"


def populate_soccer_standings():
    """Save standings for all major soccer leagues → DB."""
    from src.data.db import save_standings
    season = _soccer_season()
    for league_name, league_id in _SOCCER_LEAGUES.items():
        try:
            rows_raw = get_standings(league_id, season)
            if not rows_raw:
                continue
            rows = []
            for r in rows_raw:
                rows.append({
                    "sport":      "soccer",
                    "league":     league_name,
                    "season":     int(season),
                    "team":       r.get("name",""),
                    "rank":       int(r.get("intRank",0) or 0),
                    "wins":       int(r.get("intWin",0) or 0),
                    "losses":     int(r.get("intLoss",0) or 0),
                    "draws":      int(r.get("intDraw",0) or 0),
                    "points":     int(r.get("intPoints",0) or 0),
                    "gf":         int(r.get("intGoalsFor",0) or 0),
                    "ga":         int(r.get("intGoalsAgainst",0) or 0),
                    "gd":         int(r.get("intGoalDifference",0) or 0),
                    "form":       r.get("strForm",""),
                    "stats_json": r,
                    "source":     "thesportsdb",
                })
            if rows:
                save_standings(rows)
                print(f"[thesportsdb] {league_name}: {len(rows)} standings saved")
        except Exception as e:
            print(f"[thesportsdb] standings error {league_name}: {e}")


def populate_today_events(sport: str = "soccer"):
    """Save today's events for a sport → games table."""
    from src.data.db import upsert_game
    events = get_events_by_date(date.today(), sport)
    saved = 0
    for ev in events:
        try:
            raw_date = ev.get("dateEvent","")
            try:
                gd = date.fromisoformat(raw_date) if raw_date else date.today()
            except Exception:
                gd = date.today()
            upsert_game(
                sport       = sport,
                league      = ev.get("strLeague",""),
                home_team   = ev.get("strHomeTeam",""),
                away_team   = ev.get("strAwayTeam",""),
                game_date   = gd,
                status      = ev.get("strStatus","Scheduled"),
                home_score  = ev.get("intHomeScore"),
                away_score  = ev.get("intAwayScore"),
                external_id = ev.get("idEvent"),
            )
            saved += 1
        except Exception as e:
            print(f"[thesportsdb] event upsert error: {e}")
    print(f"[thesportsdb] {sport} today events: {saved} saved")
