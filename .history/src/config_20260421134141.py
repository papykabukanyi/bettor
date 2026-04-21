"""
Configuration loader for the betting bot.
Reads settings from .env file or environment variables.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# API Keys
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "")

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
    "MLS":  ("USA MLS",                ["2024", "2023", "2022"], "USA"),
}

# MLB seasons to pull historical data
MLB_SEASONS = [2024, 2023, 2022]
