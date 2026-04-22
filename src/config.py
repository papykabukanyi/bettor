"""
Configuration loader for the betting bot.
Reads settings from .env file or environment variables.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "")

# API Keys
ODDS_API_KEY          = os.getenv("ODDS_API_KEY", "")
FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "")
NEWS_API_KEY          = os.getenv("NEWS_API_KEY", "")
BALLDONTLIE_API_KEY   = os.getenv("BALLDONTLIE_API_KEY", "")
RAPIDAPI_KEY          = os.getenv("RAPIDAPI_KEY", "")
THESPORTSDB_API_KEY   = os.getenv("THESPORTSDB_API_KEY", "1")   # "1" = free tier
SPORTSDATA_API_KEY    = os.getenv("SPORTSDATA_API_KEY", "")

# Strategy settings
MIN_VALUE_EDGE = float(os.getenv("MIN_VALUE_EDGE", "0.05"))
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.25"))
BANKROLL = float(os.getenv("BANKROLL", "1000"))

# The Odds API endpoints
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
ODDS_REGIONS = "us"
ODDS_MARKETS = "h2h,spreads,totals"

# Football-data.org endpoint
FOOTBALL_DATA_BASE = "https://api.football-data.org/v4"

# Football-data.co.uk (free historical CSVs with odds baked in)
# Keys: (league_label, season_code, url_code)
FOOTBALL_DATA_UK_LEAGUES = {
    "EPL":  ("English Premier League", ["2425", "2324", "2223"], "E0"),
    "ELC":  ("English Championship",   ["2425", "2324", "2223"], "E1"),
    "ESP":  ("Spanish La Liga",        ["2425", "2324", "2223"], "SP1"),
    "GER":  ("German Bundesliga",      ["2425", "2324", "2223"], "D1"),
    "ITA":  ("Italian Serie A",        ["2425", "2324", "2223"], "I1"),
    "FRA":  ("French Ligue 1",         ["2425", "2324", "2223"], "F1"),
    # MLS is NOT available on football-data.co.uk; live MLS odds use The Odds API
}

# MLB seasons to pull historical data (most recent first)
MLB_SEASONS = [2026, 2025, 2024, 2023]

# ---------------------------------------------------------------------------
# Eastern-time date helper
# ---------------------------------------------------------------------------
import datetime as _dt

def et_today() -> _dt.date:
    """
    Return the 'effective today' in US Eastern time.

    Railway runs UTC; games are scheduled in Eastern time.
    Late-night cutover: after 22:00 ET (10 PM) we treat the NEXT calendar
    day as today so that finished games drop off and tomorrow's slate appears.
    """
    try:
        import zoneinfo
        eastern = zoneinfo.ZoneInfo("America/New_York")
    except Exception:
        try:
            import pytz
            eastern = pytz.timezone("America/New_York")
        except Exception:
            # Last resort: UTC-4 fixed offset (EDT)
            return _dt.date.today()

    now_et = _dt.datetime.now(tz=eastern)
    # After 10 PM ET, roll forward to the next day so the new day's games load
    if now_et.hour >= 22:
        return (now_et + _dt.timedelta(days=1)).date()
    return now_et.date()


def et_today_str() -> str:
    """Return et_today() formatted as 'YYYY-MM-DD'."""
    return et_today().isoformat()
