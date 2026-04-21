"""
Soccer Data Fetcher
===================
Data sources (all free / free-tier):

1. football-data.co.uk
   - Free CSV downloads with historical match results AND closing odds
   - URL: https://www.football-data.co.uk/mmz4281/{season}/{league_code}.csv
   - Columns used: HomeTeam, AwayTeam, FTHG, FTAG, FTR,
                   B365H, B365D, B365A (Bet365 closing odds)

2. football-data.org API  (free tier: 10 leagues, 10 req/min)
   - Requires free registration at https://www.football-data.org/client/register
   - Returns today's fixtures with teams
   - Set FOOTBALL_DATA_API_KEY in .env

Provides:
  - Historical match results with odds (for model training + back-testing)
  - Team attack/defence strength ratings (Poisson model inputs)
  - Today's fixtures for selected leagues
  - Player stats scraped from fbref via requests (no key needed)
"""

import io
import sys
import os
import time
import warnings
import requests
import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import FOOTBALL_DATA_API_KEY, FOOTBALL_DATA_BASE, FOOTBALL_DATA_UK_LEAGUES

# ---------------------------------------------------------------------------
# football-data.co.uk  (free historical CSVs)
# ---------------------------------------------------------------------------

_FDUK_BASE = "https://www.football-data.co.uk/mmz4281/{season}/{code}.csv"

# MLS uses a different URL pattern
_FDUK_MLS_BASE = "https://www.football-data.co.uk/new/{code}{season}.csv"

def _fetch_fduk_csv(season_code: str, league_code: str) -> pd.DataFrame:
    """Download one season CSV from football-data.co.uk."""
    if league_code == "USA":
        url = f"https://www.football-data.co.uk/new/MLS{season_code}.csv"
    else:
        url = _FDUK_BASE.format(season=season_code, code=league_code)
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text), low_memory=False, on_bad_lines="skip")
        df["season_code"] = season_code
        df["league_code"] = league_code
        return df
    except Exception as e:
        print(f"[soccer_fetcher] CSV fetch failed {url}: {e}")
        return pd.DataFrame()


def get_historical_matches(league_key: str = "EPL", seasons: list[str] | None = None) -> pd.DataFrame:
    """
    Download and clean historical match results + Bet365 odds for a league.

    league_key: one of EPL, ELC, ESP, GER, ITA, FRA, MLS
    Returns DataFrame with columns:
        home_team, away_team, fthg, ftag, ftr,
        b365h, b365d, b365a, season_code, league
    """
    info = FOOTBALL_DATA_UK_LEAGUES.get(league_key)
    if info is None:
        print(f"[soccer_fetcher] Unknown league key: {league_key}")
        return pd.DataFrame()

    league_label, default_seasons, code = info
    use_seasons = seasons if seasons else default_seasons

    frames = []
    for sc in use_seasons:
        df = _fetch_fduk_csv(sc, code)
        if df.empty:
            continue
        # Standardise column names (some seasons use different casing)
        df.columns = [c.strip() for c in df.columns]
        rename = {
            "HomeTeam": "home_team", "AwayTeam": "away_team",
            "FTHG": "fthg", "FTAG": "ftag", "FTR": "ftr",
            "B365H": "b365h", "B365D": "b365d", "B365A": "b365a",
            "Date": "match_date",
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        df["league"] = league_label

        keep = ["home_team", "away_team", "fthg", "ftag", "ftr",
                "b365h", "b365d", "b365a", "match_date", "season_code", "league"]
        df = df[[c for c in keep if c in df.columns]].dropna(subset=["home_team", "away_team", "fthg", "ftag"])
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result["fthg"] = pd.to_numeric(result["fthg"], errors="coerce")
    result["ftag"] = pd.to_numeric(result["ftag"], errors="coerce")
    result["b365h"] = pd.to_numeric(result.get("b365h"), errors="coerce")
    result["b365d"] = pd.to_numeric(result.get("b365d"), errors="coerce")
    result["b365a"] = pd.to_numeric(result.get("b365a"), errors="coerce")
    return result


def compute_team_strength(matches: pd.DataFrame) -> pd.DataFrame:
    """
    Compute attack and defence strength ratings per team using the
    Dixon-Coles / Poisson strength method:

        attack_strength  = avg goals scored / league avg goals per game
        defence_strength = avg goals conceded / league avg goals per game

    Returns DataFrame: team, attack_strength, defence_strength, home_adv
    """
    if matches.empty:
        return pd.DataFrame()

    avg_goals = (matches["fthg"].mean() + matches["ftag"].mean()) / 2

    records = []
    teams = set(matches["home_team"].tolist() + matches["away_team"].tolist())
    for team in teams:
        home_g = matches[matches["home_team"] == team]["fthg"]
        away_g = matches[matches["away_team"] == team]["ftag"]
        home_c = matches[matches["home_team"] == team]["ftag"]
        away_c = matches[matches["away_team"] == team]["fthg"]

        scored = pd.concat([home_g, away_g]).mean()
        conceded = pd.concat([home_c, away_c]).mean()

        # Home advantage: ratio of goals scored at home vs away
        home_adv = home_g.mean() / away_g.mean() if away_g.mean() > 0 else 1.0

        records.append({
            "team": team,
            "attack_strength": round(scored / avg_goals, 4) if avg_goals > 0 else 1.0,
            "defence_strength": round(conceded / avg_goals, 4) if avg_goals > 0 else 1.0,
            "home_adv": round(home_adv, 4),
            "games_played": len(home_g) + len(away_g),
        })

    return pd.DataFrame(records).sort_values("team")


# ---------------------------------------------------------------------------
# football-data.org API  (today's fixtures)
# ---------------------------------------------------------------------------

_FDORG_LEAGUE_MAP = {
    "EPL": "PL",     # Premier League
    "ESP": "PD",     # La Liga
    "GER": "BL1",    # Bundesliga
    "ITA": "SA",     # Serie A
    "FRA": "FL1",    # Ligue 1
    "MLS": "MLS",    # MLS
}


def get_todays_fixtures(league_keys: list[str] | None = None) -> list[dict]:
    """
    Fetch today's soccer fixtures from football-data.org API.
    Requires FOOTBALL_DATA_API_KEY in .env (free registration).
    Falls back to empty list if key not set.
    """
    if not FOOTBALL_DATA_API_KEY or FOOTBALL_DATA_API_KEY == "your_football_data_key_here":
        print("[soccer_fetcher] FOOTBALL_DATA_API_KEY not set – skipping live fixtures.")
        return []

    headers = {"X-Auth-Token": FOOTBALL_DATA_API_KEY}
    use_keys = league_keys if league_keys else list(_FDORG_LEAGUE_MAP.keys())
    fixtures = []

    for lk in use_keys:
        comp_id = _FDORG_LEAGUE_MAP.get(lk)
        if not comp_id:
            continue
        url = f"{FOOTBALL_DATA_BASE}/competitions/{comp_id}/matches?status=SCHEDULED"
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 429:
                print("[soccer_fetcher] Rate limit hit – sleeping 12s")
                time.sleep(12)
                resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            for m in data.get("matches", []):
                utc_date = m.get("utcDate", "")[:10]
                import datetime
                today = datetime.date.today().isoformat()
                if utc_date == today:
                    fixtures.append({
                        "league": lk,
                        "date": utc_date,
                        "home_team": m["homeTeam"]["name"],
                        "away_team": m["awayTeam"]["name"],
                        "match_id": m["id"],
                    })
            time.sleep(1)  # stay under 10 req/min
        except Exception as e:
            print(f"[soccer_fetcher] fixture fetch error for {lk}: {e}")

    return fixtures


# ---------------------------------------------------------------------------
# FBRef player stats scraper (no API key required)
# ---------------------------------------------------------------------------

def get_player_stats_fbref(league_key: str = "EPL", season: str = "2024-2025") -> pd.DataFrame:
    """
    Scrape player shooting/goal stats from FBRef for a given league and season.
    Uses pandas read_html (parses tables from FBRef's public pages).
    Returns DataFrame with: player, squad, goals, assists, xg, xa, npxg, minutes
    """
    _league_fbref = {
        "EPL": "9",
        "ESP": "12",
        "GER": "20",
        "ITA": "11",
        "FRA": "13",
    }
    league_id = _league_fbref.get(league_key)
    if not league_id:
        print(f"[soccer_fetcher] FBRef scraping not supported for league: {league_key}")
        return pd.DataFrame()

    url = (
        f"https://fbref.com/en/comps/{league_id}/{season}/stats/"
        f"{season}-{league_key}-Stats"
    )
    headers = {"User-Agent": "Mozilla/5.0 (research project; educational use)"}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        tables = pd.read_html(io.StringIO(resp.text))
        # Standard stats table is usually the first large one
        df = tables[0] if tables else pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [" ".join(c).strip() for c in df.columns]
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        return df
    except Exception as e:
        print(f"[soccer_fetcher] FBRef scrape error: {e}")
        return pd.DataFrame()
