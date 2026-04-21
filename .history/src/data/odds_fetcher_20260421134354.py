"""
Odds Fetcher
============
Source: The Odds API  (https://the-odds-api.com)
Free tier: 500 requests / month  (resets monthly)
No commitment – just register for a key.

Supported sports:
  - baseball_mlb   : MLB moneyline, runline, totals
  - soccer_*       : MLS, EPL, La Liga, etc.

Also provides:
  - american_to_prob()    : convert American odds → implied probability
  - decimal_to_prob()     : convert decimal odds → implied probability
  - remove_vig()          : strip bookmaker margin from raw implied probs
"""

import os
import sys
import requests
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import ODDS_API_KEY, ODDS_API_BASE, ODDS_REGIONS

# Sports codes accepted by The Odds API
SPORT_MAP = {
    "mlb":      "baseball_mlb",
    "mls":      "soccer_usa_mls",
    "epl":      "soccer_epl",
    "laliga":   "soccer_spain_la_liga",
    "bundesliga": "soccer_germany_bundesliga",
    "seriea":   "soccer_italy_serie_a",
    "ligue1":   "soccer_france_ligue_1",
    "ucl":      "soccer_uefa_champs_league",
}


def _headers() -> dict:
    return {}  # API key is passed as a query param


def get_live_odds(sport_key: str = "mlb", markets: str = "h2h") -> list[dict]:
    """
    Fetch live / upcoming odds for a sport.

    sport_key : one of keys in SPORT_MAP or a raw odds-api sport key
    markets   : comma-separated, e.g. 'h2h' | 'spreads' | 'totals'

    Returns list of game dicts:
      {id, sport, commence_time, home_team, away_team,
       bookmakers: [{key, title, markets: [{key, outcomes: [{name, price}]}]}]}
    """
    if not ODDS_API_KEY or ODDS_API_KEY == "your_odds_api_key_here":
        print("[odds_fetcher] ODDS_API_KEY not set in .env – returning empty.")
        return []

    raw_sport = SPORT_MAP.get(sport_key.lower(), sport_key)
    url = f"{ODDS_API_BASE}/sports/{raw_sport}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGIONS,
        "markets": markets,
        "oddsFormat": "american",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        remaining = resp.headers.get("x-requests-remaining", "?")
        print(f"[odds_fetcher] Requests remaining this month: {remaining}")
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[odds_fetcher] fetch error: {e}")
        return []


def odds_to_dataframe(games: list[dict], preferred_book: str = "draftkings") -> pd.DataFrame:
    """
    Flatten the nested odds response into a clean DataFrame.
    Picks the preferred bookmaker; falls back to the first available.

    Columns: sport, home_team, away_team, commence_time,
             home_odds, away_odds, draw_odds (soccer only)
    """
    rows = []
    for game in games:
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        sport = game.get("sport_key", "")
        commence = game.get("commence_time", "")
        books = game.get("bookmakers", [])
        if not books:
            continue

        # prefer a specific book, else use first
        book = next((b for b in books if b["key"] == preferred_book), books[0])
        for market in book.get("markets", []):
            if market["key"] != "h2h":
                continue
            outcomes = {o["name"]: o["price"] for o in market.get("outcomes", [])}
            rows.append({
                "sport": sport,
                "home_team": home,
                "away_team": away,
                "commence_time": commence,
                "home_odds": outcomes.get(home),
                "away_odds": outcomes.get(away),
                "draw_odds": outcomes.get("Draw"),
                "book": book["title"],
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Odds math utilities
# ---------------------------------------------------------------------------

def american_to_prob(odds: float) -> float:
    """Convert American moneyline odds to raw implied probability (includes vig)."""
    if odds is None or pd.isna(odds):
        return float("nan")
    if odds > 0:
        return 100.0 / (odds + 100.0)
    else:
        return abs(odds) / (abs(odds) + 100.0)


def decimal_to_prob(odds: float) -> float:
    """Convert decimal odds (European) to implied probability."""
    if odds is None or pd.isna(odds) or odds <= 0:
        return float("nan")
    return 1.0 / odds


def remove_vig(home_prob: float, away_prob: float, draw_prob: float = 0.0) -> tuple[float, float, float]:
    """
    Normalise implied probabilities by removing bookmaker overround (vig).
    Returns (true_home_prob, true_away_prob, true_draw_prob) that sum to 1.
    """
    total = home_prob + away_prob + draw_prob
    if total <= 0:
        return home_prob, away_prob, draw_prob
    return home_prob / total, away_prob / total, draw_prob / total


def get_available_sports() -> list[dict]:
    """List all sports currently available on The Odds API."""
    if not ODDS_API_KEY or ODDS_API_KEY == "your_odds_api_key_here":
        return []
    url = f"{ODDS_API_BASE}/sports"
    try:
        resp = requests.get(url, params={"apiKey": ODDS_API_KEY}, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[odds_fetcher] sports list error: {e}")
        return []
