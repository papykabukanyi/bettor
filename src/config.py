"""
Configuration loader for the betting bot.
Reads settings from .env file or environment variables.
"""
import os
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from dotenv import load_dotenv

load_dotenv()


def _env_bool(name: str, default: str = "false") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def _first_env(*names: str, default: str = "") -> str:
    for name in names:
        value = str(os.getenv(name, "") or "").strip()
        if value:
            return value
    return default


def _normalize_pg_url(url: str) -> str:
    raw = str(url or "").strip()
    if raw.startswith("postgres://"):
        raw = "postgresql://" + raw[len("postgres://"):]
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
        if not parts.scheme.startswith("postgres"):
            return raw
        allowed = {
            "sslmode", "connect_timeout", "application_name", "options",
            "target_session_attrs", "keepalives", "keepalives_idle",
            "keepalives_interval", "keepalives_count", "channel_binding",
            "gssencmode", "krbsrvname", "service",
        }
        filtered_qs = [(k, v) for (k, v) in parse_qsl(parts.query, keep_blank_values=True) if k in allowed]
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(filtered_qs), parts.fragment))
    except Exception:
        return raw

# Database
DATABASE_URL = _normalize_pg_url(_first_env(
    "POSTGRES_URL",
    "POSTGRES_PRISMA_URL",
    "POSTGRES_URL_NON_POOLING",
    "DATABASE_URL",
    default="",
))

# API Keys
ODDS_API_KEY       = os.getenv("ODDS_API_KEY", "")
NEWS_API_KEY       = os.getenv("NEWS_API_KEY", "")
SPORTSDATA_API_KEY = os.getenv("SPORTSDATA_API_KEY", "")
THESPORTSDB_API_KEY = os.getenv("THESPORTSDB_API_KEY", "1")

# Reddit API (sentiment analysis)
REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT", "bettor-bot/1.0")

# Hugging Face (sentiment scoring via Inference API)
HF_API_KEY = os.getenv("HF_API_KEY", "")

# Discord (sentiment)
DISCORD_BOT_TOKEN      = os.getenv("DISCORD_BOT_TOKEN", "")
DISCORD_CHANNELS       = os.getenv("DISCORD_CHANNELS", "")
DISCORD_CHANNELS_BY_SPORT = os.getenv("DISCORD_CHANNELS_BY_SPORT", "")
DISCORD_LOOKBACK_HOURS = int(os.getenv("DISCORD_LOOKBACK_HOURS", "12"))
DISCORD_MAX_MESSAGES   = int(os.getenv("DISCORD_MAX_MESSAGES", "300"))
DISCORD_CACHE_MINUTES  = int(os.getenv("DISCORD_CACHE_MINUTES", "15"))
DISCORD_ENABLE_ATTACHMENT_OCR = _env_bool("DISCORD_ENABLE_ATTACHMENT_OCR", "true")
DISCORD_MAX_IMAGE_ATTACHMENTS = int(os.getenv("DISCORD_MAX_IMAGE_ATTACHMENTS", "40"))
DISCORD_OCR_TIMEOUT_SECONDS = int(os.getenv("DISCORD_OCR_TIMEOUT_SECONDS", "10"))

# newsdata.io (free tier: 200 req/day, no credit card)
NEWSDATA_API_KEY         = os.getenv("NEWSDATA_API_KEY", "")

# Social player sentiment settings
SOCIAL_PLAYER_MIN_MENTIONS = int(os.getenv("SOCIAL_PLAYER_MIN_MENTIONS", "1"))
SOCIAL_MAX_PLAYERS_PER_GAME = int(os.getenv("SOCIAL_MAX_PLAYERS_PER_GAME", "8"))

# Deprecated TikTok config — kept for backward compat with existing .env files but not used
TIKTOK_ENABLED           = False
TIKTOK_HASHTAGS          = ""
TIKTOK_MAX_VIDEOS        = 0
TIKTOK_MAX_COMMENTS      = 0
TIKTOK_COMMENTS_PER_VIDEO = 0
TIKTOK_FETCH_COMMENTS    = False
TIKTOK_CACHE_MINUTES     = 0

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
# Soccer / World Cup 2026
# ---------------------------------------------------------------------------
FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "")
SPORT                 = os.getenv("SPORT", "all")   # "all", "mlb", or "soccer"
WC_START_DATE         = "2026-06-11"
WC_END_DATE           = "2026-07-19"

# Soccer external data sources
SOCCER_DS_CACHE_TTL_SEC = int(os.getenv("SOCCER_DS_CACHE_TTL_SEC", "900"))

# API-Football (RapidAPI)
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")
API_FOOTBALL_HOST = os.getenv("API_FOOTBALL_HOST", "api-football-v1.p.rapidapi.com")
API_FOOTBALL_BASE = os.getenv("API_FOOTBALL_BASE", "https://api-football-v1.p.rapidapi.com/v3")

# BSD live odds endpoint (provider-specific)
BSD_API_BASE = os.getenv("BSD_API_BASE", "")
BSD_API_KEY = os.getenv("BSD_API_KEY", "")

# Transfermarkt adapter endpoint (proxy/scraper service)
TRANSFERMARKT_PROXY_URL = os.getenv("TRANSFERMARKT_PROXY_URL", "")

# WNBA data-source integration
WNBA_DATA_CACHE_TTL_SEC = int(os.getenv("WNBA_DATA_CACHE_TTL_SEC", "300"))
WNBA_STATS_API_BASE = os.getenv("WNBA_STATS_API_BASE", "https://stats.wnba.com/stats")
WNBA_STATS_API_TIMEOUT_SEC = int(os.getenv("WNBA_STATS_API_TIMEOUT_SEC", "10"))
WNBA_BREF_START_YEAR = int(os.getenv("WNBA_BREF_START_YEAR", "1997"))
WNBA_KAGGLE_DATA_DIR = os.getenv("WNBA_KAGGLE_DATA_DIR", "")

# Tennis data-source integration
TENNIS_DATA_CACHE_TTL_SEC = int(os.getenv("TENNIS_DATA_CACHE_TTL_SEC", "300"))
TENNIS_REFERENCE_YEARS = int(os.getenv("TENNIS_REFERENCE_YEARS", "8"))
TENNIS_SACKMANN_START_YEAR = int(os.getenv("TENNIS_SACKMANN_START_YEAR", "2016"))
TENNIS_SACKMANN_END_YEAR = int(os.getenv("TENNIS_SACKMANN_END_YEAR", "2026"))
TENNIS_JEFF_SACKMANN_DIR = os.getenv("TENNIS_JEFF_SACKMANN_DIR", "")
TENNIS_TENNIS_DATA_CO_UK_DIR = os.getenv("TENNIS_TENNIS_DATA_CO_UK_DIR", "")
TENNIS_SLAM_POINTBYP_PBP_DIR = os.getenv("TENNIS_SLAM_POINTBYP_PBP_DIR", "")
TENNIS_API_BASE = os.getenv("TENNIS_API_BASE", "")
TENNIS_API_KEY = os.getenv("TENNIS_API_KEY", "")

# Golf data-source integration
GOLF_DATA_CACHE_TTL_SEC = int(os.getenv("GOLF_DATA_CACHE_TTL_SEC", "300"))
GOLF_REFERENCE_YEARS = int(os.getenv("GOLF_REFERENCE_YEARS", "8"))
GOLF_DATAGOLF_API_BASE = os.getenv("GOLF_DATAGOLF_API_BASE", "")
GOLF_DATAGOLF_API_KEY = os.getenv("GOLF_DATAGOLF_API_KEY", "")
GOLF_PGA_STATDATA_BASE = os.getenv("GOLF_PGA_STATDATA_BASE", "")
GOLF_GOLFAPI_BASE = os.getenv("GOLF_GOLFAPI_BASE", "")
GOLF_GOLFAPI_KEY = os.getenv("GOLF_GOLFAPI_KEY", "")
GOLF_KAGGLE_DATA_DIR = os.getenv("GOLF_KAGGLE_DATA_DIR", "")

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
