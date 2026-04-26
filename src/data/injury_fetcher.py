"""
Injury Reports Fetcher
======================
Uses ESPN's public (no-key) sports API to fetch current injury reports for:
  - MLB teams
  - Soccer (EPL, La Liga, Bundesliga)

Data is saved to PostgreSQL and used to annotate game cards on the dashboard.
"""

import sys
import os
import datetime
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"

_ENDPOINTS = {
    "mlb":         f"{_ESPN_BASE}/baseball/mlb/injuries",
    "epl":         f"{_ESPN_BASE}/soccer/eng.1/injuries",
    "laliga":      f"{_ESPN_BASE}/soccer/esp.1/injuries",
    "bundesliga":  f"{_ESPN_BASE}/soccer/ger.1/injuries",
    "serie_a":     f"{_ESPN_BASE}/soccer/ita.1/injuries",
    "ligue_1":     f"{_ESPN_BASE}/soccer/fra.1/injuries",
}

_SPORT_LABELS = {
    "mlb": "mlb",
    "epl": "soccer", "laliga": "soccer",
    "bundesliga": "soccer", "serie_a": "soccer", "ligue_1": "soccer",
}


def fetch_injuries(league_key: str) -> list[dict]:
    """
    Fetch injury list from ESPN for a given league key.
    Returns list of dicts: {sport, team, player_name, status, description, injury_type}
    """
    url = _ENDPOINTS.get(league_key)
    if not url:
        return []
    try:
        resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return []
        data = resp.json()
        sport = _SPORT_LABELS.get(league_key, "unknown")
        injuries = []
        for team_entry in data.get("injuries", []):
            team_name = (team_entry.get("team") or {}).get("displayName", "Unknown")
            for item in team_entry.get("items", []):
                athlete = item.get("athlete") or {}
                inj_type = item.get("type") or {}
                injuries.append({
                    "sport":       sport,
                    "team":        team_name,
                    "player_name": athlete.get("displayName", "Unknown"),
                    "status":      item.get("status", ""),
                    "description": item.get("shortComment") or item.get("longComment") or "",
                    "injury_type": inj_type.get("text", "") if isinstance(inj_type, dict) else str(inj_type),
                })
        return injuries
    except Exception as e:
        print(f"[injury_fetcher] {league_key} error: {e}")
        return []


def fetch_mlb_transactions(start_date: str, end_date: str) -> list[dict]:
    """
    Fetch MLB transactions from StatsAPI and extract injury-related moves.
    start_date/end_date: YYYY-MM-DD
    Returns list of injury dicts with fetched_at set to transaction date.
    """
    try:
        import statsapi as mlbstatsapi
    except Exception:
        return []

    params = {
        "sportId": 1,
        "startDate": start_date,
        "endDate": end_date,
    }
    try:
        data = mlbstatsapi.get("transactions", params) or {}
        items = data.get("transactions") if isinstance(data, dict) else data
        if not items:
            return []
    except Exception as e:
        print(f"[injury_fetcher] MLB transactions error: {e}")
        return []

    injuries = []
    for t in items:
        desc = t.get("description", "") or ""
        ttype = t.get("typeDesc") or t.get("typeCode") or t.get("transactionType") or ""
        blob = f"{ttype} {desc}".lower()
        if not any(k in blob for k in ("injured", "injury", "il", "disabled list", "placed on")):
            continue

        person = t.get("person") or t.get("player") or {}
        team = t.get("team") or t.get("toTeam") or {}
        player_name = person.get("fullName") or person.get("name") or "Unknown"
        team_name = team.get("name") or team.get("teamName") or "Unknown"
        status = ttype or "Injury"
        tdate = t.get("date") or t.get("transactionDate") or t.get("effectiveDate")

        injuries.append({
            "sport": "mlb",
            "team": team_name,
            "player_name": player_name,
            "status": status,
            "description": desc,
            "injury_type": "transaction",
            "fetched_at": tdate,
        })

    return injuries


def fetch_injury_history(start_date: str, end_date: str) -> list[dict]:
    """
    Build a historical injury list using MLB transactions and ESPN snapshots.
    start_date/end_date: YYYY-MM-DD
    """
    injuries = []
    injuries.extend(fetch_mlb_transactions(start_date, end_date))

    # Also snapshot current ESPN injury list with end_date timestamp
    try:
        today_snapshot = fetch_injuries("mlb")
        for inj in today_snapshot:
            inj["fetched_at"] = end_date
        injuries.extend(today_snapshot)
    except Exception:
        pass

    return injuries


def fetch_all_injuries() -> dict[str, list[dict]]:
    """
    Fetch injuries for all supported leagues.
    Returns dict: {league_key: [injury_dicts]}
    """
    results = {}
    for key in _ENDPOINTS:
        injuries = fetch_injuries(key)
        if injuries:
            results[key] = injuries
            print(f"[injury_fetcher] {key}: {len(injuries)} injured players")
    return results


def get_injuries_for_game(home_team: str, away_team: str,
                           all_injuries: list[dict]) -> list[dict]:
    """
    Filter injury list to players on the two teams in a game.
    Fuzzy match on team name (lowercase substring).
    """
    ht = home_team.lower()
    at = away_team.lower()
    matched = []
    for inj in all_injuries:
        team = (inj.get("team") or "").lower()
        if any(word in team for word in ht.split() if len(word) > 3) or \
           any(word in team for word in at.split() if len(word) > 3):
            matched.append(inj)
    return matched
