"""
BallDontLie API v1 fetcher  –  NBA players, teams, season stats, injuries.
Base URL : https://api.balldontlie.io/v1
Auth     : Authorization: <api_key> header
Free tier: 60 req/min, unlimited per day
"""

import os
import time
import requests
from datetime import date, datetime

from src.config import BALLDONTLIE_API_KEY

_BASE = "https://api.balldontlie.io/v1"
_HEADERS = {"Authorization": BALLDONTLIE_API_KEY}
_SEASON = 2024          # current NBA season year (2024–25)

# ─── helpers ─────────────────────────────────────────────────────────────────

def _get(path: str, params: dict = None, timeout: int = 10):
    """GET request with basic retry on 429."""
    url = f"{_BASE}{path}"
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=_HEADERS, params=params or {}, timeout=timeout)
            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            print(f"[balldontlie] timeout {url}")
            return None
        except Exception as e:
            print(f"[balldontlie] error {url}: {e}")
            return None
    return None


def _paginate(path: str, params: dict = None, max_pages: int = 10) -> list:
    """Paginate through all pages and accumulate .data[]."""
    params = dict(params or {})
    params.setdefault("per_page", 100)
    results = []
    cursor  = None
    for _ in range(max_pages):
        if cursor:
            params["cursor"] = cursor
        data = _get(path, params)
        if not data:
            break
        results.extend(data.get("data") or [])
        meta   = data.get("meta") or {}
        cursor = meta.get("next_cursor")
        if not cursor:
            break
    return results


# ─── teams ────────────────────────────────────────────────────────────────────

def get_teams() -> list[dict]:
    """Return all NBA teams."""
    data = _get("/teams", {"per_page": 100})
    return (data or {}).get("data", [])


# ─── players ──────────────────────────────────────────────────────────────────

def search_player(name: str) -> list[dict]:
    """Search for a player by (partial) name."""
    data = _get("/players", {"search": name, "per_page": 25})
    return (data or {}).get("data", [])


def get_active_players(season: int = _SEASON) -> list[dict]:
    """Return all players who have stats in a given season (paginated)."""
    return _paginate("/players", {"seasons[]": season, "per_page": 100})


# ─── season averages ──────────────────────────────────────────────────────────

def get_season_averages(player_ids: list[int], season: int = _SEASON) -> list[dict]:
    """Return season averages for a list of player IDs."""
    if not player_ids:
        return []
    params = {"season": season, "per_page": 100}
    for pid in player_ids:
        params[f"player_ids[]"] = pid          # requests encodes multiple values
    # requests encodes list params correctly when passed as list
    params2 = {"season": season}
    params2["player_ids[]"] = player_ids
    # Build manually for correct multi-value serialization
    from urllib.parse import urlencode
    qs = urlencode([("season", season)] +
                   [("player_ids[]", pid) for pid in player_ids[:50]])
    try:
        resp = requests.get(f"{_BASE}/season_averages?{qs}",
                            headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception as e:
        print(f"[balldontlie] season_averages error: {e}")
        return []


# ─── live / today box scores ──────────────────────────────────────────────────

def get_live_box_scores() -> list[dict]:
    """Return live box scores for today."""
    data = _get("/live_box_scores")
    return (data or {}).get("data", [])


def get_games_by_date(d: date = None) -> list[dict]:
    """Return games for a specific date (defaults to today)."""
    d = d or date.today()
    data = _get("/games", {"dates[]": d.isoformat(), "per_page": 100})
    return (data or {}).get("data", [])


def get_box_scores_by_date(d: date = None) -> list[dict]:
    """Return box scores for a specific date."""
    d = d or date.today()
    data = _get("/box_scores", {"date": d.isoformat()})
    return (data or {}).get("data", [])


# ─── standings ────────────────────────────────────────────────────────────────

def get_standings(season: int = _SEASON) -> list[dict]:
    """Return NBA standings for a season."""
    data = _get("/standings", {"season": season})
    return (data or {}).get("data", [])


# ─── injuries ─────────────────────────────────────────────────────────────────

def get_injuries() -> list[dict]:
    """Return current NBA injury report."""
    return _paginate("/player_injuries", {"per_page": 100}, max_pages=5)


# ─── DB population helpers ────────────────────────────────────────────────────

def populate_db(season: int = _SEASON):
    """
    Full data collection run:
      1. Fetch all teams
      2. Fetch standings → save to DB
      3. Fetch active players + season averages → save player profiles + stats
      4. Fetch injuries → save injury reports
      5. Fetch today's games → save to games table
    """
    from src.data.db import (bulk_upsert_player_profiles, save_player_season_stats,
                              save_standings, save_injuries, upsert_game)

    print("[balldontlie] starting populate_db …")

    # 1. Teams (used for name mapping)
    teams_raw = get_teams()
    team_map  = {t["id"]: t for t in teams_raw}   # id → team dict

    # 2. Standings
    standings_raw = get_standings(season)
    if standings_raw:
        rows = []
        for s in standings_raw:
            team = s.get("team", {})
            rows.append({
                "sport":      "nba",
                "league":     "NBA",
                "season":     season,
                "team":       team.get("full_name", team.get("name","")),
                "rank":       s.get("conference_rank") or s.get("division_rank"),
                "wins":       s.get("wins"),
                "losses":     s.get("losses"),
                "draws":      0,
                "points":     s.get("wins", 0),   # NBA uses W-L, not points
                "form":       s.get("l10_record",""),
                "stats_json": {k: v for k, v in s.items()
                               if k not in ("team","league")},
                "source":     "balldontlie",
            })
        save_standings(rows)
        print(f"[balldontlie] standings: {len(rows)} teams saved")

    # 3. Players + season averages  (sample: 200 players by roster pages)
    print("[balldontlie] fetching active players …")
    players = get_active_players(season)[:200]     # cap to save quota
    if players:
        # Save profiles
        profiles = []
        for p in players:
            team = p.get("team") or {}
            profiles.append({
                "sport":       "nba",
                "source":      "balldontlie",
                "external_id": str(p["id"]),
                "player_name": f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
                "team":        team.get("full_name",""),
                "position":    p.get("position",""),
                "height":      p.get("height",""),
                "weight":      p.get("weight",""),
                "jersey_number": str(p.get("jersey_number","") or ""),
                "profile_json": p,
            })
        bulk_upsert_player_profiles(profiles)
        print(f"[balldontlie] profiles: {len(profiles)} saved")

        # Season averages
        ids = [p["id"] for p in players]
        avgs = get_season_averages(ids[:50], season)   # first 50 in one batch
        stat_rows = []
        for a in avgs:
            pid    = a.get("player_id")
            pdata  = next((p for p in players if p["id"] == pid), {})
            team   = (pdata.get("team") or {}).get("full_name", "")
            name   = f"{pdata.get('first_name','')} {pdata.get('last_name','')}".strip()
            stat_rows.append({
                "sport":       "nba",
                "player_name": name,
                "team":        team,
                "season":      season,
                "stat_group":  "nba_season_avg",
                "stats_json":  a,
                "source":      "balldontlie",
            })
        if stat_rows:
            save_player_season_stats(stat_rows)
            print(f"[balldontlie] season averages: {len(stat_rows)} saved")

    # 4. Injuries
    print("[balldontlie] fetching injuries …")
    injuries_raw = get_injuries()
    if injuries_raw:
        inj_list = []
        for i in injuries_raw:
            player = i.get("player") or {}
            team   = player.get("team") or {}
            inj_list.append({
                "team":        team.get("full_name",""),
                "player_name": f"{player.get('first_name','')} {player.get('last_name','')}".strip(),
                "status":      i.get("status",""),
                "description": i.get("description","") or i.get("return_date",""),
                "injury_type": i.get("type","") or "",
                "source":      "balldontlie",
            })
        save_injuries("nba", inj_list)
        print(f"[balldontlie] injuries: {len(inj_list)} saved")

    # 5. Today's games
    print("[balldontlie] fetching today's games …")
    games_raw = get_games_by_date()
    for g in games_raw:
        ht = g.get("home_team", {})
        vt = g.get("visitor_team", {})
        dt = g.get("date","")[:10] if g.get("date") else None
        try:
            gd = date.fromisoformat(dt) if dt else date.today()
        except Exception:
            gd = date.today()
        upsert_game(
            sport        = "nba",
            league       = "NBA",
            home_team    = ht.get("full_name",""),
            away_team    = vt.get("full_name",""),
            game_date    = gd,
            status       = g.get("status","Scheduled"),
            home_score   = g.get("home_team_score"),
            away_score   = g.get("visitor_team_score"),
            season       = season,
            external_id  = str(g.get("id","")),
        )
    print(f"[balldontlie] today's games: {len(games_raw)} saved")
    print("[balldontlie] populate_db complete")
