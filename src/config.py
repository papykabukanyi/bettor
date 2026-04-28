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
ODDS_API_KEY       = os.getenv("ODDS_API_KEY", "")
NEWS_API_KEY       = os.getenv("NEWS_API_KEY", "")
SPORTSDATA_API_KEY = os.getenv("SPORTSDATA_API_KEY", "")

# Reddit API (sentiment analysis)
REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT", "bettor-bot/1.0")

# Hugging Face (sentiment scoring via Inference API)
HF_API_KEY = os.getenv("HF_API_KEY", "")

# Discord (sentiment)
DISCORD_BOT_TOKEN      = os.getenv("DISCORD_BOT_TOKEN", "")
DISCORD_CHANNELS       = os.getenv("DISCORD_CHANNELS", "")
DISCORD_LOOKBACK_HOURS = int(os.getenv("DISCORD_LOOKBACK_HOURS", "12"))
DISCORD_MAX_MESSAGES   = int(os.getenv("DISCORD_MAX_MESSAGES", "300"))
DISCORD_CACHE_MINUTES  = int(os.getenv("DISCORD_CACHE_MINUTES", "15"))

# ClickSend SMS
CLICKSEND_USERNAME = os.getenv("CLICKSEND_USERNAME", "")
CLICKSEND_API_KEY  = os.getenv("CLICKSEND_API_KEY", "")

# Strategy settings
MIN_VALUE_EDGE = float(os.getenv("MIN_VALUE_EDGE", "0.05"))
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.25"))
BANKROLL       = float(os.getenv("BANKROLL", "1000"))

# The Odds API
ODDS_API_BASE   = "https://api.the-odds-api.com/v4"
ODDS_REGIONS    = "us"
ODDS_MARKETS    = "h2h,spreads,totals"

# MLB seasons to pull historical data (most recent first)
MLB_SEASONS = [2026, 2025, 2024, 2023]

# ---------------------------------------------------------------------------
# Eastern-time date helper
# ---------------------------------------------------------------------------
import datetime as _dt

def et_today() -> _dt.date:
    """
    Return the 'effective today' in US Eastern time.
    After 10 PM ET rolls forward to the next calendar day.
    """
    try:
        import zoneinfo
        eastern = zoneinfo.ZoneInfo("America/New_York")
    except Exception:
        try:
            import pytz
            eastern = pytz.timezone("America/New_York")
        except Exception:
            return _dt.date.today()

    now_et = _dt.datetime.now(tz=eastern)
    if now_et.hour >= 22:
        return (now_et + _dt.timedelta(days=1)).date()
    return now_et.date()


def et_today_str() -> str:
    return et_today().isoformat()
