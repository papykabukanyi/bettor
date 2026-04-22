"""
SportsData.io fetcher  –  MLB, NBA, NCAA, NFL, NHL, Soccer.
Key:  3228e5282150459182dc4bbd731a330a  (trial tier)

SportsData.io uses sport-specific base URLs:
  MLB:    https://api.sportsdata.io/v3/mlb/scores/json/...
  NBA:    https://api.sportsdata.io/v3/nba/scores/json/...
  SOCCER: https://api.sportsdata.io/v3/soccer/scores/json/...
  NFL:    https://api.sportsdata.io/v3/nfl/scores/json/...

Auth:    ?key=<api_key>  OR  Ocp-Apim-Subscription-Key header
"""

import os
import time
import requests
from datetime import date, datetime

from src.config import SPORTSDATA_API_KEY

_KEY = SPORTSDATA_API_KEY
_BASES = {
    "mlb":    "https://api.sportsdata.io/v3/mlb",
    "nba":    "https://api.sportsdata.io/v3/nba",
    "nfl":    "https://api.sportsdata.io/v3/nfl",
    "soccer": "https://api.sportsdata.io/v3/soccer",
}
_HEADERS = {"Ocp-Apim-Subscription-Key": _KEY}

# ─── helpers ─────────────────────────────────────────────────────────────────

def _get(sport: str, path: str, timeout: int = 10):
    """GET request to SportsData.io.  Returns parsed JSON or None."""
    if not _KEY:
        return None
    base = _BASES.get(sport.lower())
    if not base:
        return None
    url = f"{base}{path}?key={_KEY}"
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=timeout)
            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            if resp.status_code in (401, 403):
                print(f"[sportsdata] {resp.status_code} – key may not cover {sport} endpoint")
                return None          # fail-fast, no retry
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            print(f"[sportsdata] timeout {url}")
            return None
        except Exception as e:
            print(f"[sportsdata] {sport} {path} error: {e}")
            return None
    return None


# ─── MLB ─────────────────────────────────────────────────────────────────────

def get_mlb_games_by_date(d: date = None) -> list[dict]:
    d = d or date.today()
    data = _get("mlb", f"/scores/json/GamesByDate/{d.strftime('%Y-%b-%d').upper()}")
    return data or []


def get_mlb_standings(season: int = None) -> list[dict]:
    season = season or date.today().year
    data = _get("mlb", f"/scores/json/Standings/{season}")
    return data or []


def get_mlb_player_season_stats(season: int = None) -> list[dict]:
    season = season or date.today().year
    data = _get("mlb", f"/stats/json/PlayerSeasonStats/{season}")
    return (data or [])[:500]


def get_mlb_injuries() -> list[dict]:
    data = _get("mlb", "/scores/json/Injuries")
    return data or []


def get_mlb_teams() -> list[dict]:
    data = _get("mlb", "/scores/json/teams")
    return data or []


def get_mlb_player_props_by_date(d: date = None) -> list[dict]:
    d = d or date.today()
    data = _get("mlb", f"/projections/json/DfsSlatesByDate/{d.strftime('%Y-%b-%d').upper()}")
    return data or []


# ─── NBA ─────────────────────────────────────────────────────────────────────

def get_nba_games_by_date(d: date = None) -> list[dict]:
    d = d or date.today()
    data = _get("nba", f"/scores/json/GamesByDate/{d.strftime('%Y-%b-%d').upper()}")
    return data or []


def get_nba_standings(season: str = None) -> list[dict]:
    season = season or f"{date.today().year}"
    data = _get("nba", f"/scores/json/Standings/{season}")
    return data or []


def get_nba_player_season_stats(season: str = None) -> list[dict]:
    season = season or f"{date.today().year}"
    data = _get("nba", f"/stats/json/PlayerSeasonStats/{season}")
    return (data or [])[:500]


def get_nba_injuries() -> list[dict]:
    data = _get("nba", "/scores/json/Injuries")
    return data or []


# ─── Soccer ──────────────────────────────────────────────────────────────────

def get_soccer_games_by_date(d: date = None, competition: int = None) -> list[dict]:
    d = d or date.today()
    comp = competition or 5  # 5=Premier League default
    data = _get("soccer", f"/scores/json/GamesByDate/{comp}/{d.strftime('%Y-%b-%d').upper()}")
    return data or []


def get_soccer_standings(competition: int = None, season: int = None) -> list[dict]:
    competition = competition or 5
    season      = season or date.today().year
    data = _get("soccer", f"/scores/json/Standings/{competition}/{season}")
    return data or []


def get_soccer_injuries(competition: int = None) -> list[dict]:
    competition = competition or 5
    data = _get("soccer", f"/scores/json/Injuries/{competition}")
    return data or []


# ─── DB population helpers ────────────────────────────────────────────────────

def _norm_injury(raw: dict, sport: str, source: str = "sportsdata") -> dict:
    return {
        "team":        raw.get("Team",""),
        "player_name": raw.get("Name","") or raw.get("ShortName",""),
        "status":      raw.get("Status","") or raw.get("InjuryStatus",""),
        "description": raw.get("InjuryBodyPart","") or raw.get("InjuryDescription",""),
        "injury_type": raw.get("InjuryBodyPart",""),
        "source":      source,
    }


def populate_mlb(season: int = None):
    """Collect MLB standings, player stats, injuries → DB."""
    from src.data.db import save_standings, save_player_season_stats, save_injuries

    season = season or date.today().year
    print("[sportsdata] MLB populate …")

    # Standings
    standings_raw = get_mlb_standings(season)
    rows = []
    for s in standings_raw:
        rows.append({
            "sport":      "mlb",
            "league":     s.get("League","MLB"),
            "season":     season,
            "team":       s.get("Name","") or s.get("City",""),
            "rank":       s.get("DivisionRank"),
            "wins":       s.get("Wins"),
            "losses":     s.get("Losses"),
            "draws":      0,
            "points":     s.get("Wins",0),
            "form":       "",
            "stats_json": {k:v for k,v in s.items()},
            "source":     "sportsdata",
        })
    if rows:
        save_standings(rows)
        print(f"[sportsdata] MLB standings: {len(rows)} saved")

    # Player stats
    pstats_raw = get_mlb_player_season_stats(season)
    prows = []
    for p in pstats_raw:
        name = p.get("Name","") or p.get("ShortName","")
        if not name:
            continue
        prows.append({
            "sport":       "mlb",
            "player_name": name,
            "team":        p.get("Team",""),
            "season":      season,
            "stat_group":  "mlb_batting" if p.get("AtBats") else "mlb_pitching",
            "stats_json":  p,
            "source":      "sportsdata",
        })
    if prows:
        save_player_season_stats(prows)
        print(f"[sportsdata] MLB player stats: {len(prows)} saved")

    # Injuries
    inj_raw = get_mlb_injuries()
    inj_list = [_norm_injury(i, "mlb") for i in inj_raw]
    if inj_list:
        save_injuries("mlb", inj_list)
        print(f"[sportsdata] MLB injuries: {len(inj_list)} saved")


def populate_nba(season: str = None):
    """Collect NBA standings, player stats, injuries → DB."""
    from src.data.db import save_standings, save_player_season_stats, save_injuries

    season_str = season or str(date.today().year)
    print("[sportsdata] NBA populate …")

    # Standings
    standings_raw = get_nba_standings(season_str)
    rows = []
    for s in standings_raw:
        rows.append({
            "sport":      "nba",
            "league":     "NBA",
            "season":     int(season_str[:4]),
            "team":       s.get("Name","") or s.get("City",""),
            "rank":       s.get("ConferenceRank") or s.get("DivisionRank"),
            "wins":       s.get("Wins"),
            "losses":     s.get("Losses"),
            "draws":      0,
            "points":     s.get("Wins",0),
            "form":       s.get("L10",""),
            "stats_json": s,
            "source":     "sportsdata",
        })
    if rows:
        save_standings(rows)
        print(f"[sportsdata] NBA standings: {len(rows)} saved")

    # Player stats
    pstats_raw = get_nba_player_season_stats(season_str)
    prows = []
    for p in pstats_raw:
        name = p.get("Name","") or p.get("ShortName","")
        if not name:
            continue
        prows.append({
            "sport":       "nba",
            "player_name": name,
            "team":        p.get("Team",""),
            "season":      int(season_str[:4]),
            "stat_group":  "nba_season",
            "stats_json":  p,
            "source":      "sportsdata",
        })
    if prows:
        save_player_season_stats(prows)
        print(f"[sportsdata] NBA player stats: {len(prows)} saved")

    # Injuries
    inj_raw = get_nba_injuries()
    inj_list = [_norm_injury(i, "nba") for i in inj_raw]
    if inj_list:
        save_injuries("nba", inj_list)
        print(f"[sportsdata] NBA injuries: {len(inj_list)} saved")


def populate_soccer(competition: int = 5, season: int = None):
    """Collect soccer standings, injuries → DB (Premier League default)."""
    from src.data.db import save_standings, save_injuries

    season = season or date.today().year
    comp_names = {5: "premier_league", 12: "ligue_1", 10: "bundesliga",
                  11: "serie_a", 8: "la_liga", 6: "mls"}
    league_name = comp_names.get(competition, f"soccer_{competition}")
    print(f"[sportsdata] soccer ({league_name}) populate …")

    standings_raw = get_soccer_standings(competition, season)
    rows = []
    for s in standings_raw:
        rows.append({
            "sport":      "soccer",
            "league":     league_name,
            "season":     season,
            "team":       s.get("Name",""),
            "rank":       s.get("Overall",{}).get("Rank"),
            "wins":       s.get("Overall",{}).get("Wins"),
            "losses":     s.get("Overall",{}).get("Losses"),
            "draws":      s.get("Overall",{}).get("Draws"),
            "points":     s.get("Overall",{}).get("Points"),
            "gf":         s.get("Overall",{}).get("GoalsScored"),
            "ga":         s.get("Overall",{}).get("GoalsAgainst"),
            "gd":         s.get("Overall",{}).get("GoalDifferential"),
            "form":       "",
            "stats_json": s,
            "source":     "sportsdata",
        })
    if rows:
        save_standings(rows)
        print(f"[sportsdata] soccer standings: {len(rows)} saved")

    inj_raw = get_soccer_injuries(competition)
    inj_list = [_norm_injury(i, "soccer") for i in inj_raw]
    if inj_list:
        save_injuries("soccer", inj_list)
        print(f"[sportsdata] soccer injuries: {len(inj_list)} saved")


def populate_all():
    """Run all sport population functions."""
    populate_mlb()
    populate_nba()
    for comp in [5, 12, 10, 11, 8]:   # EPL, Ligue 1, Bundesliga, Serie A, La Liga
        try:
            populate_soccer(competition=comp)
        except Exception as e:
            print(f"[sportsdata] soccer comp {comp} error: {e}")
