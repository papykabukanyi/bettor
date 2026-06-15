"""Kalshi API helpers.

Public market data is fetched from Kalshi's external API without auth.
Order execution uses RSA key-based authentication (PKCS1v15 + SHA256).

Required environment variables for order execution:
  KALSHI_API_KEY       - Your Kalshi API key ID (UUID)
  KALSHI_PRIVATE_KEY   - PEM-encoded RSA private key (multi-line OK in .env)
"""

from __future__ import annotations

import base64
import datetime
import json
import os
import re
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import combinations
from typing import Any
from urllib.parse import urlparse

import requests

KALSHI_BASE_URL = os.getenv(
    "KALSHI_BASE_URL",
    "https://external-api.kalshi.com/trade-api/v2",
).rstrip("/")
KALSHI_TIMEOUT_SEC = int(os.getenv("KALSHI_TIMEOUT_SEC", "15"))

# Path prefix used when signing (everything after the hostname).
_KALSHI_BASE_PATH = urlparse(KALSHI_BASE_URL).path.rstrip("/")  # e.g. /trade-api/v2
_KALSHI_MARKET_CACHE_TTL_SEC = max(
    60, int(os.getenv("KALSHI_MARKET_CACHE_TTL_SEC", "600") or "600")
)
_KALSHI_SERIES_DISCOVERY_TTL_SEC = max(
    300, int(os.getenv("KALSHI_SERIES_DISCOVERY_TTL_SEC", "1800") or "1800")
)
_KALSHI_SERIES_DISCOVERY_PAGES = max(
    1, min(int(os.getenv("KALSHI_SERIES_DISCOVERY_PAGES", "2") or "2"), 10)
)
_KALSHI_MAX_DISCOVERED_SERIES = max(
    10, min(int(os.getenv("KALSHI_MAX_DISCOVERED_SERIES", "25") or "25"), 400)
)
_KALSHI_SERIES_FETCH_WORKERS = max(
    1, min(int(os.getenv("KALSHI_SERIES_FETCH_WORKERS", "6") or "6"), 16)
)
_KALSHI_COMBO_MARKET_PAGES = max(
    1, min(int(os.getenv("KALSHI_COMBO_MARKET_PAGES", "4") or "4"), 12)
)

# Series ticker prefix → sport (updated per Kalshi documentation)
_SERIES_SPORT_MAP: dict[str, str] = {
    # ── Baseball (MLB) — confirmed live series ──
    "KXMLBGAME": "baseball",  # game winners / moneylines
    "KXMLBSPREAD": "baseball",  # run line / spreads
    "KXMLBF5": "baseball",  # first-five derivatives
    "KXMLBHRR": "baseball",   # player hits+runs+RBIs combined props
    "KXMLBHIT": "baseball",   # player hits props
    "KXMLBRBI": "baseball",   # player RBI props
    "KXMLBTOTAL": "baseball", # game run totals
    "KXMLBTB": "baseball",    # total bases props
    "KXKBOGAME": "baseball",
    "KXNPBGAME": "baseball",
    "KXNCAABBGAME": "baseball",
    "KXMLB": "baseball",      # futures / misc
    "MLBWIN": "baseball", "MLBOU": "baseball", "MLBHR": "baseball",
    "MLBRUNS": "baseball", "MLBK": "baseball", "MLBHITS": "baseball",
    "MLBWSERIES": "baseball",
    # ── Basketball (NBA/WNBA) — confirmed live series ──
    "KXNBAPTS": "basketball",   # player points props
    "KXNBATOTAL": "basketball", # game point totals
    "KXNBAREB": "basketball",   # player rebounds props
    "KXNBAAST": "basketball",   # player assists props
    "KXNBARA": "basketball",    # player rebounds+assists props
    "KXNBAGAME": "basketball",
    "KXNBASPREAD": "basketball",
    "KXNBATEAMTOTAL": "basketball",
    "KXNBA1H": "basketball",
    "KXNBA1HWINNER": "basketball",
    "KXNBA1HSPREAD": "basketball",
    "KXNBA1QSPREAD": "basketball",
    "KXNBA2HSPREAD": "basketball",
    "KXNBA": "basketball",      # futures / championship
    "KXWNBA": "basketball",
    "KXWNBAGAME": "basketball",
    "KXWNBASPREAD": "basketball",
    "KXWNBATOTAL": "basketball",
    "KXWNBAREB": "basketball",
    "KXWNBAAST": "basketball",
    "KXWNBAPTS": "basketball",
    "KXWNBA1H": "basketball",
    "KXWNBA1HSPREAD": "basketball",
    "KXWNBA1HTOTAL": "basketball",
    "NBAPTSO": "basketball", "NBAMVP": "basketball", "NBACHAMP": "basketball",
    # ── Hockey (NHL) — confirmed live series ──
    "KXNHLPTS": "hockey",   # player points (goals+assists) props
    "KXNHLTOTAL": "hockey", # game goal totals
    "KXNHLAST": "hockey",   # player assists props
    "KXNHLSPREAD": "hockey",
    "KXNHLGOAL": "hockey",
    "KXNHLANYGOAL": "hockey",
    "KXAHLGAME": "hockey",
    "KXWOMHOCKEYGOAL": "hockey",
    "KXWOMHOCKEYTOTAL": "hockey",
    "KXWOMHOCKEYFIRSTGOAL": "hockey",
    "KXWOMHOCKEYSPREAD": "hockey",
    "KXNHL": "hockey",      # futures / misc
    "NHLWIN": "hockey", "NHLOU": "hockey", "NHLCHAMP": "hockey", "NHLIN": "hockey",
    # ── Football (NFL) ──
    "KXNFL": "football",
    "NFLWIN": "football", "NFLOU": "football", "NFLTD": "football",
    "NFLMVP": "football", "NFLSB": "football",
    # ── Soccer ──
    "KXMLS": "soccer", "KXMLSGAME": "soccer", "KXMLSSPREAD": "soccer",
    "KXEPL": "soccer", "KXEPLSPREAD": "soccer", "KXFIFA": "soccer",
    "MSLWIN": "soccer", "MLSWIN": "soccer", "EPLWIN": "soccer", "UCLWIN": "soccer",
    # ── Tennis / Combat / Motorsports / Golf (generic series families) ──
    "KXTENNIS": "tennis", "ATP": "tennis", "WTA": "tennis", "TENNIS": "tennis",
    "KXBOX": "boxing", "BOXING": "boxing",
    "KXMMA": "mma", "MMA": "mma", "UFC": "mma", "PFL": "mma", "BELLATOR": "mma",
    "KXF1": "motorsports", "F1": "motorsports", "FORMULA1": "motorsports", "NASCAR": "motorsports",
    "KXGOLF": "golf", "GOLF": "golf", "PGA": "golf", "LPGA": "golf",
    "KXCRICKET": "cricket", "CRICKET": "cricket",
}

# Confirmed live Kalshi sports series to fetch explicitly (bypasses pagination ordering issues).
# The general /markets endpoint returns 25k+ non-sports markets first, so we target these directly.
# Add new confirmed series here — they will be persisted to the DB series registry automatically.
_SPORTS_SERIES_TO_FETCH: list[str] = [
    # NBA — player props + game markets
    "KXNBAGAME",   # game winners / moneylines
    "KXNBASPREAD", # game spreads
    "KXNBAPTS",    # player points
    "KXNBATOTAL",  # game totals
    "KXNBATEAMTOTAL", # team totals
    "KXNBAREB",    # player rebounds
    "KXNBAAST",    # player assists
    "KXNBARA",     # player rebounds + assists
    "KXNBA1H",     # first-half winner
    "KXNBA1HWINNER",
    "KXNBA1HSPREAD", # first-half spread
    "KXNBA1QSPREAD",
    "KXNBA2HSPREAD",
    # WNBA
    "KXWNBAGAME",
    "KXWNBASPREAD",
    "KXWNBATOTAL",
    "KXWNBAPTS",
    "KXWNBAREB",
    "KXWNBAAST",
    "KXWNBA1H",
    "KXWNBA1HSPREAD",
    "KXWNBA1HTOTAL",
    # MLB — player props + game markets
    "MLBWIN",      # game winners / moneylines
    "MLBOU",       # game totals/over-under
    "KXMLBGAME",
    "KXMLBSPREAD",
    "KXMLBHRR",    # hits + runs + RBIs combined
    "KXMLBHIT",    # hits only
    "KXMLBRBI",
    "KXMLBTOTAL",  # game run totals
    "KXMLBF5",     # first-five winner
    "KXMLBF5TOTAL",
    "KXMLBF5SPREAD",
    "KXMLBTB",
    "KXKBOGAME",
    "KXNPBGAME",
    "KXNCAABBGAME",
    # NHL — player props + game markets
    "NHLWIN",      # game winners / moneylines
    "NHLOU",       # game totals/over-under
    "KXNHLPTS",    # player points (goals+assists)
    "KXNHLTOTAL",  # game goal totals
    "KXNHLAST",    # player assists
    "KXNHLSPREAD",
    "KXNHLGOAL",
    "KXNHLANYGOAL",
    "KXAHLGAME",
    # NFL (off-season — will return empty but keeps the door open)
    "NFLWIN", "NFLOU", "NFLTD",
    # Soccer
    "KXMLS", "KXMLSGAME", "KXMLSSPREAD", "KXEPL", "KXEPLSPREAD", "KXFIFA", "MSLWIN", "MLSWIN", "EPLWIN", "UCLWIN",
    # Tennis / Combat / Motorsports / Golf / Cricket (newer Kalshi sports families)
    "KXTENNIS", "ATP", "WTA",
    "KXBOX", "BOXING",
    "KXMMA", "UFC", "PFL", "BELLATOR",
    "KXF1", "F1", "NASCAR",
    "KXGOLF", "PGA", "LPGA",
    "KXCRICKET", "CRICKET",
]

# Per-series stat-type hints used to disambiguate player prop events.
# Maps a substring that appears in the series/event ticker to the bet stat types it covers.
# Keys are upper-case substrings; values are lower-case prop_type keywords.
_SERIES_STAT_HINTS: dict[str, list[str]] = {
    "NBAPTS":  ["point", "pts"],
    "WNBAPTS": ["point", "pts"],
    "NHLPTS":  ["point", "pts"],
    "NBAREB":  ["rebound", "reb"],
    "WNBAREB": ["rebound", "reb"],
    "NBAAST":  ["assist", "ast"],
    "WNBAAST": ["assist", "ast"],
    "NHLAST":  ["assist", "ast"],
    "NHLGLS":  ["goal", "goals"],
    "MLBHRR":  ["hit", "run", "rbi", "hrr"],
    "MLBHIT":  ["hit", "hits"],
    "MLBHR":   ["home run", "hr"],
    "NBATOTAL": ["total"],
    "WNBATOTAL": ["total"],
    "NHLTOTAL": ["total", "goal"],
    "MLBTOTAL": ["total", "run"],
}

_DISCOVERY_SUPPORTED_SPORTS = {
    "basketball", "baseball", "hockey", "football", "soccer",
    "tennis", "boxing", "mma", "golf", "motorsports", "cricket",
}
_DISCOVERY_INCLUDE_TEXT = (
    "game",
    "match",
    "spread",
    "total",
    "team total",
    "first half",
    "1st half",
    "points",
    "pra",
    "rebounds",
    "assists",
    "hits",
    "runs",
    "rbi",
    "home run",
    "goals",
    "goal",
    "set",
    "round",
    "fight",
    "knockout",
    "submission",
    "winner",
    "pole",
    "lap",
    "birdie",
    "bogey",
    "wicket",
)
_DISCOVERY_INCLUDE_TICKER = (
    "GAME",
    "MATCH",
    "SPREAD",
    "TOTAL",
    "1H",
    "TEAMTOTAL",
    "REB",
    "HIT",
    "HRR",
    "RBI",
    "GOAL",
    "GLS",
    "WIN",
    "SET",
    "ROUND",
    "FIGHT",
    "MMA",
    "UFC",
    "BOX",
    "TENNIS",
    "ATP",
    "WTA",
    "F1",
    "NASCAR",
    "GOLF",
    "PGA",
    "CRICKET",
)
_DISCOVERY_EXCLUDE_TEXT = (
    "draft",
    "mvp",
    "rookie",
    "coach",
    "manager",
    "next team",
    "transfer",
    "leader",
    "all star",
    "seed",
    "record",
    "debut",
    "of the year",
    "most improved",
    "pick",
    "combine",
    "season delay",
    "division",
    "rank",
)
_DISCOVERY_EXCLUDE_TICKER = (
    "DRAFT",
    "MVP",
    "ROY",
    "ROTY",
    "OPOY",
    "DPOTY",
    "COTY",
    "MIMP",
    "COACH",
    "MANAGER",
    "LEADER",
    "SEED",
    "RECORD",
    "DEBUT",
    "PICK",
    "COMBINE",
    "RETURN",
    "DELAY",
    "PREPACK",
    "WINS",
)

_VALID_SPORT_TAGS = {
    "baseball", "basketball", "football", "hockey", "soccer",
    "tennis", "boxing", "mma", "golf", "motorsports", "cricket",
}
_ACTIONABLE_SERIES_TICKER_INCLUDE = _DISCOVERY_INCLUDE_TICKER + (
    "WIN",
    "WINNER",
    "MONEYLINE",
    "F5",
    "1Q",
    "2Q",
    "3Q",
    "4Q",
    "2H",
    "ANYGOAL",
    "SHOT",
    "SAVE",
    "CARD",
    "CORNER",
    "BTTS",
    "TB",
    "KO",
    "SUB",
    "DECISION",
    "MATCHWINNER",
    "RACEWINNER",
    "TOURNAMENT",
    "OPEN",
)
_ACTIONABLE_SERIES_TICKER_EXCLUDE = _DISCOVERY_EXCLUDE_TICKER + (
    "POY",
    "6POY",
    "TOPAPRANK",
    "TOP25",
    "VIEWER",
    "GAMESPLAYED",
    "ALEAST",
    "ALWEST",
    "NLEAST",
    "NLWEST",
    "EAST",
    "WEST",
    "NORTH",
    "SOUTH",
    "PACIFIC",
    "ATLANTIC",
    "CENTRAL",
    "SOUTHEAST",
    "NORTHEAST",
    "NORTHWEST",
    "SOUTHWEST",
    "DIV",
    "DIVISION",
    "RANK",
)
_ACTIONABLE_SERIES_FETCH_SET = {series.upper() for series in _SPORTS_SERIES_TO_FETCH}

# ── Series registry persistence ─────────────────────────────────────────────────────
# Keeps track of all confirmed working series tickers across restarts.  Written to a
# local JSON file so the bot can recover known-good series without a full Kalshi scan.
_SERIES_REGISTRY_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "kalshi_series_registry.json",
)
_SERIES_REGISTRY_LOCK = threading.Lock()


def _load_series_registry() -> dict[str, str]:
    """Load persisted {series_ticker: sport} mapping from disk."""
    try:
        if os.path.exists(_SERIES_REGISTRY_PATH):
            with open(_SERIES_REGISTRY_PATH, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return {str(k): str(v) for k, v in data.items()}
    except Exception:
        pass
    return {}


def _save_series_registry(registry: dict[str, str]) -> None:
    """Persist series registry to disk."""
    try:
        os.makedirs(os.path.dirname(_SERIES_REGISTRY_PATH), exist_ok=True)
        with open(_SERIES_REGISTRY_PATH, "w") as f:
            json.dump(registry, f, indent=2, sort_keys=True)
    except Exception:
        pass


def _update_series_registry(new_series: dict[str, str]) -> None:
    """Merge newly confirmed series into the persistent registry."""
    with _SERIES_REGISTRY_LOCK:
        registry = _load_series_registry()
        updated = False
        for ticker, sport in new_series.items():
            if ticker and sport and registry.get(ticker) != sport:
                registry[ticker] = sport
                updated = True
        if updated:
            _save_series_registry(registry)


def _detect_sport_from_series(series_ticker: str) -> str:
    """Best-effort sport detection from a series ticker not in _SERIES_SPORT_MAP."""
    t = series_ticker.upper()
    if any(token in t for token in ("MLB", "BASEBALL", "KBO", "NPB", "WBC")):
        return "baseball"
    if any(
        token in t
        for token in (
            "NBA",
            "BASKETBALL",
            "WNBA",
            "NCAAMB",
            "NCAAWB",
            "EUROLEAGUE",
            "NBL",
            "FIBA",
            "JBLEAGUE",
            "ABA",
            "GBL",
        )
    ):
        return "basketball"
    if any(token in t for token in ("NHL", "HOCKEY", "IIHF")):
        return "hockey"
    if "NFL" in t or "FOOTBALL" in t:
        return "football"
    if any(token in t for token in ("MLS", "EPL", "FIFA", "SOCCER", "UEFA", "LALIGA", "BUNDESLIGA", "LIGUE")):
        return "soccer"
    if any(token in t for token in ("TENNIS", "ATP", "WTA", "WIMBLEDON", "ROLAND", "USOPEN", "AUSTRALIANOPEN")):
        return "tennis"
    if any(token in t for token in ("BOX", "BOXING")):
        return "boxing"
    if any(token in t for token in ("MMA", "UFC", "PFL", "BELLATOR")):
        return "mma"
    if any(token in t for token in ("GOLF", "PGA", "LPGA", "MASTERS", "RYDER")):
        return "golf"
    if any(token in t for token in ("F1", "FORMULA1", "FORMULA", "NASCAR", "INDYCAR", "MOTOGP")):
        return "motorsports"
    if any(token in t for token in ("CRICKET", "IPL", "T20", "ODI")):
        return "cricket"
    return ""


def _detect_sport_from_series_row(series: dict[str, Any]) -> str:
    ticker = str(series.get("ticker") or "")
    tags = " ".join(str(tag or "") for tag in (series.get("tags") or []))
    title = str(series.get("title") or "")
    text = _norm_text(" ".join((ticker, title, tags)))

    if any(token in text for token in (
        "women s pro basketball",
        "women s basketball",
        "wnba",
        "pro basketball",
        "college basketball",
        "march madness",
        "euroleague",
        "fiba",
        "basketball",
    )):
        return "basketball"
    if any(token in text for token in ("pro baseball", "college baseball", "baseball", "mlb", "kbo", "npb", "world baseball")):
        return "baseball"
    if any(token in text for token in ("pro hockey", "college hockey", "hockey", "nhl", "iihf", "olympic hockey")):
        return "hockey"
    if any(token in text for token in ("pro football", "college football", "football", "nfl", "super bowl")):
        return "football"
    if any(token in text for token in ("soccer", "mls", "premier league", "epl", "uefa", "fifa", "liga", "bundesliga", "ligue", "cup")):
        return "soccer"
    if any(token in text for token in ("tennis", "atp", "wta", "wimbledon", "us open", "australian open", "roland garros")):
        return "tennis"
    if any(token in text for token in ("boxing", "box", "heavyweight", "welterweight", "middleweight")):
        return "boxing"
    if any(token in text for token in ("mma", "ufc", "pfl", "bellator", "mixed martial arts", "octagon")):
        return "mma"
    if any(token in text for token in ("golf", "pga", "lpga", "masters", "open championship", "ryder cup")):
        return "golf"
    if any(token in text for token in ("formula 1", "f1", "nascar", "indycar", "moto gp", "motorsport", "motorsports")):
        return "motorsports"
    if any(token in text for token in ("cricket", "ipl", "test match", "t20", "odi")):
        return "cricket"
    return _detect_sport_from_series(ticker)


def _is_actionable_sports_series(series: dict[str, Any]) -> bool:
    ticker = str(series.get("ticker") or "").strip().upper()
    if ticker and _is_actionable_series_ticker(ticker):
        return True
    raw = " ".join(
        [
            str(series.get("ticker") or ""),
            str(series.get("title") or ""),
            " ".join(str(tag or "") for tag in (series.get("tags") or [])),
        ]
    )
    raw_upper = raw.upper()
    text = _norm_text(raw)
    padded_text = f" {text} "
    if any(token in raw_upper for token in _DISCOVERY_EXCLUDE_TICKER):
        return False
    if any(f" {token} " in padded_text for token in _DISCOVERY_EXCLUDE_TEXT):
        return False
    return any(token in raw_upper for token in _DISCOVERY_INCLUDE_TICKER) or any(
        f" {token} " in padded_text for token in _DISCOVERY_INCLUDE_TEXT
    )


def _is_actionable_series_ticker(series_ticker: str) -> bool:
    raw_upper = str(series_ticker or "").strip().upper()
    if not raw_upper:
        return False
    if raw_upper in _ACTIONABLE_SERIES_FETCH_SET:
        return True
    if any(token in raw_upper for token in _ACTIONABLE_SERIES_TICKER_EXCLUDE):
        return False
    if any(token in raw_upper for token in _ACTIONABLE_SERIES_TICKER_INCLUDE):
        return True
    return raw_upper.endswith(("GAME", "SPREAD", "TOTAL"))


def _discover_actionable_sports_series(*, force_refresh: bool = False) -> dict[str, str]:
    now = time.time()
    with _KALSHI_SERIES_CACHE_LOCK:
        age = now - float(_KALSHI_SERIES_CACHE.get("ts") or 0.0)
        if (
            not force_refresh
            and _KALSHI_SERIES_CACHE.get("series")
            and age < _KALSHI_SERIES_DISCOVERY_TTL_SEC
        ):
            return dict(_KALSHI_SERIES_CACHE.get("series") or {})

    discovered: dict[str, str] = {}
    cursor: str | None = None
    for _ in range(_KALSHI_SERIES_DISCOVERY_PAGES):
        try:
            data = list_series(limit=200, cursor=cursor, category="Sports")
        except Exception:
            break
        rows = data.get("series") or []
        for row in rows:
            if not isinstance(row, dict):
                continue
            sport = _detect_sport_from_series_row(row)
            if sport not in _DISCOVERY_SUPPORTED_SPORTS:
                continue
            if not _is_actionable_sports_series(row):
                continue
            ticker = str(row.get("ticker") or "").strip().upper()
            if not ticker:
                continue
            discovered.setdefault(ticker, sport)
            if len(discovered) >= _KALSHI_MAX_DISCOVERED_SERIES:
                break
        if len(discovered) >= _KALSHI_MAX_DISCOVERED_SERIES:
            break
        cursor = data.get("cursor")
        if not cursor:
            break

    with _KALSHI_SERIES_CACHE_LOCK:
        _KALSHI_SERIES_CACHE.update({"ts": time.time(), "series": dict(discovered)})
    return discovered

_KALSHI_MARKET_CACHE_LOCK = threading.Lock()
_KALSHI_MARKET_CACHE: dict[str, Any] = {
    "ts": 0.0,
    "markets": [],
    "combo_markets": [],
    "count": 0,
}
_KALSHI_SERIES_CACHE_LOCK = threading.Lock()
_KALSHI_SERIES_CACHE: dict[str, Any] = {
    "ts": 0.0,
    "series": {},
}

# Module-level resolution cache: keyed by bet_signature, valid for current catalog epoch.
# Survives across HTTP requests — avoids re-scoring every market on every auto-poll.
_RESOLUTION_CACHE: dict[str, dict[str, Any]] = {}
_RESOLUTION_CACHE_CATALOG_TS: float = 0.0  # catalog timestamp when cache was built

_ENTITY_STOPWORDS = {
    "ac",
    "afc",
    "cf",
    "club",
    "de",
    "fc",
    "sc",
    "the",
    "united",
}
_AMBIGUOUS_TEAM_ALIASES = {
    "new york",
    "los angeles",
    "san antonio",
    "san francisco",
    "san diego",
    "kansas city",
    "oklahoma city",
}
_SPORT_DONE_HOURS = {
    "baseball": 4.5,
    "basketball": 3.5,
    "football": 4.5,
    "hockey": 3.25,
    "soccer": 2.5,
    "tennis": 4.0,
    "boxing": 3.0,
    "mma": 3.0,
    "golf": 10.0,
    "motorsports": 5.0,
    "cricket": 8.0,
}
_SPECIAL_ENTITY_ALIASES: dict[str, set[str]] = {
    # ── NBA ──────────────────────────────────────────────────────────────────
    "atlanta hawks": {"atl", "hawks"},
    "boston celtics": {"bos", "celtics"},
    "brooklyn nets": {"bkn", "nets"},
    "charlotte hornets": {"cha", "clt", "hornets"},
    "chicago bulls": {"chi", "bulls"},
    "cleveland cavaliers": {"cle", "cavaliers", "cavs"},
    "dallas mavericks": {"dal", "mavericks", "mavs"},
    "denver nuggets": {"den", "nuggets"},
    "detroit pistons": {"det", "pistons"},
    "golden state warriors": {"gsw", "warriors"},
    "houston rockets": {"hou", "rockets"},
    "indiana pacers": {"ind", "pacers"},
    "los angeles clippers": {"lac", "clippers"},
    "los angeles lakers": {"lal", "lakers"},
    "memphis grizzlies": {"mem", "grizzlies"},
    "miami heat": {"mia", "heat"},
    "milwaukee bucks": {"mil", "bucks"},
    "minnesota timberwolves": {"min", "timberwolves", "wolves"},
    "new orleans pelicans": {"nop", "pelicans"},
    "new york knicks": {"nyk", "knicks"},
    "oklahoma city thunder": {"okc", "thunder"},
    "orlando magic": {"orl", "magic"},
    "philadelphia 76ers": {"phi", "76ers", "sixers"},
    "phoenix suns": {"phx", "pho", "suns"},
    "portland trail blazers": {"por", "blazers", "trail blazers"},
    "sacramento kings": {"sac", "kings"},
    "san antonio spurs": {"sas", "spurs"},
    "toronto raptors": {"tor", "raptors"},
    "utah jazz": {"uta", "jazz"},
    "washington wizards": {"was", "wiz", "wizards"},
    # ── WNBA ─────────────────────────────────────────────────────────────────
    "las vegas aces": {"lva", "aces"},
    "new york liberty": {"nyl", "liberty"},
    "seattle storm": {"sea", "storm"},
    "connecticut sun": {"con", "sun"},
    "washington mystics": {"wsh", "mystics"},
    "los angeles sparks": {"las", "sparks"},
    "chicago sky": {"chi", "sky"},
    "dallas wings": {"dal", "wings"},
    "indiana fever": {"ind", "fever"},
    "atlanta dream": {"atl", "dream"},
    "phoenix mercury": {"phx", "mercury"},
    "minnesota lynx": {"min", "lynx"},
    # ── NHL ──────────────────────────────────────────────────────────────────
    "anaheim ducks": {"ana", "ducks"},
    "boston bruins": {"bos", "bruins"},
    "buffalo sabres": {"buf", "sabres"},
    "calgary flames": {"cgy", "flames"},
    "carolina hurricanes": {"car", "hurricanes", "canes"},
    "chicago blackhawks": {"chi", "blackhawks", "hawks"},
    "colorado avalanche": {"col", "avalanche", "avs"},
    "columbus blue jackets": {"cbj", "blue jackets", "jackets"},
    "dallas stars": {"dal", "stars"},
    "detroit red wings": {"det", "red wings", "wings"},
    "edmonton oilers": {"edm", "oilers"},
    "florida panthers": {"fla", "panthers"},
    "los angeles kings": {"lak", "kings"},
    "minnesota wild": {"min", "wild"},
    "montreal canadiens": {"mtl", "canadiens", "habs"},
    "nashville predators": {"nsh", "predators", "preds"},
    "new jersey devils": {"njd", "devils"},
    "new york islanders": {"nyi", "islanders", "isles"},
    "new york rangers": {"nyr", "rangers"},
    "ottawa senators": {"ott", "senators", "sens"},
    "philadelphia flyers": {"phi", "flyers"},
    "pittsburgh penguins": {"pit", "penguins", "pens"},
    "san jose sharks": {"sjs", "sharks"},
    "seattle kraken": {"sea", "kraken"},
    "st. louis blues": {"stl", "blues"},
    "tampa bay lightning": {"tbl", "tbl", "lightning"},
    "toronto maple leafs": {"tor", "maple leafs", "leafs"},
    "utah hockey club": {"uta", "utah"},
    "vancouver canucks": {"van", "canucks"},
    "vegas golden knights": {"vgk", "golden knights"},
    "washington capitals": {"wsh", "caps", "capitals"},
    "winnipeg jets": {"wpg", "jets"},
    # ── MLB ──────────────────────────────────────────────────────────────────
    "arizona diamondbacks": {"ari", "dbacks", "diamondbacks"},
    "atlanta braves": {"atl", "braves"},
    "baltimore orioles": {"bal", "orioles", "birds"},
    "boston red sox": {"bos", "red sox"},
    "chicago cubs": {"chc", "cubs"},
    "chicago white sox": {"cws", "chw", "white sox"},
    "cincinnati reds": {"cin", "reds"},
    "cleveland guardians": {"cle", "guardians"},
    "colorado rockies": {"col", "rockies"},
    "detroit tigers": {"det", "tigers"},
    "houston astros": {"hou", "astros"},
    "kansas city royals": {"kc", "kcr", "royals"},
    "los angeles angels": {"laa", "angels"},
    "los angeles dodgers": {"lad", "dodgers"},
    "miami marlins": {"mia", "mar", "marlins"},
    "milwaukee brewers": {"mil", "brewers"},
    "minnesota twins": {"min", "twi", "twins"},
    "new york mets": {"nym", "mets"},
    "new york yankees": {"nyy", "yankees"},
    "oakland athletics": {"oak", "ath", "athletics"},
    "philadelphia phillies": {"phi", "phillies"},
    "pittsburgh pirates": {"pit", "pirates"},
    "san diego padres": {"sd", "sdp", "padres"},
    "san francisco giants": {"sf", "sfg", "giants"},
    "seattle mariners": {"sea", "mar", "mariners"},
    "st. louis cardinals": {"stl", "cardinals"},
    "tampa bay rays": {"tb", "tbr", "rays"},
    "texas rangers": {"tex", "rangers"},
    "toronto blue jays": {"tor", "bluejays", "blue jays"},
    "washington nationals": {"was", "wsh", "nationals", "nats"},
    # ── MLS ──────────────────────────────────────────────────────────────────
    "new york city fc": {"nycfc", "new york city"},
    "columbus crew": {"clb", "crew"},
    "west ham united": {"whu", "west ham"},
    "arsenal": {"afc", "arsenal"},
    "fc barcelona": {"bar", "barcelona", "barca"},
    "real madrid": {"rma", "real madrid"},
    "manchester city": {"mci", "man city"},
    "manchester united": {"mun", "man utd"},
    "liverpool": {"liv", "liverpool"},
    "chelsea": {"che", "chelsea"},
    "tottenham hotspur": {"tot", "spurs"},
    "atletico madrid": {"atm", "atletico"},
    "paris saint germain": {"psg"},
    "paris saint-germain": {"psg"},
    "inter milan": {"int", "inter"},
    "juventus": {"juv", "juve"},
    "ac milan": {"acm", "milan"},
}
_PROP_HINTS = {
    "points",
    "rebounds",
    "assists",
    "pra",
    "goals",
    "shots",
    "shots on target",
    "saves",
    "hits",
    "runs",
    "rbi",
    "strikeouts",
    "home runs",
    "total bases",
    "stolen bases",
    "cards",
    "corners",
}

_PROP_TYPE_ALIASES: dict[str, tuple[str, ...]] = {
    "points": ("point", "points", "pts"),
    "rebounds": ("rebound", "rebounds", "reb"),
    "assists": ("assist", "assists", "ast"),
    "pra": (
        "pra",
        "points rebounds assists",
        "points rebounds and assists",
        "rebounds assists",
        "ra",
    ),
    "goals": ("goal", "goals", "anytime goal", "first goal"),
    "hits": ("hit", "hits"),
    "runs": ("run", "runs"),
    "rbi": ("rbi", "rbis", "runs batted in"),
    "total_bases": ("total base", "total bases", "tb"),
    "home_runs": ("home run", "home runs", "hr", "hrs"),
    "strikeouts": ("strikeout", "strikeouts", " k ", " ks ", " k+"),
}

_TICKER_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
_MONTH_ABBR_BY_NUM = {v: k for k, v in _TICKER_MONTHS.items()}


def _load_private_key():
    """Load and cache the RSA private key from environment."""
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    pem = os.getenv("KALSHI_PRIVATE_KEY", "").strip()

    # Handle case where dotenv stores with literal \n instead of real newlines
    if pem and "\\n" in pem and "\n" not in pem:
        pem = pem.replace("\\n", "\n")

    # Fallback: load from a .pem file path if env var is a file path
    if not pem or not pem.startswith("-----"):
        key_file = os.getenv("KALSHI_PRIVATE_KEY_FILE", "").strip()
        if key_file and os.path.exists(key_file):
            with open(key_file, "r") as f:
                pem = f.read().strip()

    if not pem:
        raise RuntimeError(
            "Kalshi private key is missing. Set KALSHI_PRIVATE_KEY in environment."
        )

    return load_pem_private_key(pem.encode("ascii"), password=None)


def _auth_headers(method: str, path: str) -> dict[str, str]:
    """Build Kalshi RSA-signed request headers.

    Signing message: {timestamp_ms}{METHOD}{/full/path}
    Algorithm: RSA-PSS + SHA256 (MAX_LENGTH salt) for 2048-bit keys;
               PKCS1v15 + SHA256 for other key sizes.
    """
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding

    api_key_id = os.getenv("KALSHI_API_KEY", "").strip()
    if not api_key_id:
        raise RuntimeError(
            "Kalshi API key ID is missing. Set KALSHI_API_KEY in environment."
        )

    private_key = _load_private_key()

    timestamp_ms = str(int(time.time() * 1000))
    msg = (timestamp_ms + method.upper() + path).encode("ascii")

    # Kalshi uses RSA-PSS with DIGEST_LENGTH salt (per official docs)
    signature = private_key.sign(
        msg,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )

    sig_b64 = base64.b64encode(signature).decode("ascii")

    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        "KALSHI-ACCESS-SIGNATURE": sig_b64,
        "Content-Type": "application/json",
    }


def _request_json(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    auth: bool = False,
) -> dict[str, Any]:
    url = f"{KALSHI_BASE_URL}/{path.lstrip('/')}"

    if auth:
        # The signing path must include the full URL path from /
        sign_path = _KALSHI_BASE_PATH + "/" + path.lstrip("/")
        request_headers = _auth_headers(method, sign_path)
        if headers:
            request_headers.update(headers)
    else:
        request_headers = dict(headers or {})

    resp = requests.request(
        method=method.upper(),
        url=url,
        params=params,
        json=payload,
        headers=request_headers,
        timeout=KALSHI_TIMEOUT_SEC,
    )

    data: dict[str, Any]
    try:
        data = resp.json() if resp.text else {}
    except Exception:
        data = {"raw": (resp.text or "")[:4000]}

    if resp.status_code >= 400:
        msg = (
            data.get("error")
            or data.get("message")
            or data.get("detail")
            or data.get("raw")
            or f"HTTP {resp.status_code}"
        )
        raise RuntimeError(f"Kalshi API error ({resp.status_code}): {msg}")
    return data


def _parse_list_response(
    data: dict[str, Any], preferred_key: str
) -> tuple[list[dict[str, Any]], str | None]:
    rows = data.get(preferred_key)
    if not isinstance(rows, list):
        rows = data.get("data")
    if not isinstance(rows, list):
        rows = []

    cursor = data.get("cursor") or data.get("next_cursor")
    if cursor is None:
        pagination = data.get("pagination")
        if isinstance(pagination, dict):
            cursor = pagination.get("cursor") or pagination.get("next_cursor")

    clean_rows = [r for r in rows if isinstance(r, dict)]
    return clean_rows, (str(cursor) if cursor else None)


def list_markets(
    *,
    limit: int = 200,
    cursor: str | None = None,
    status: str | None = "open",
    event_ticker: str | None = None,
    series_ticker: str | None = None,
    mve_filter: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": max(1, min(int(limit or 200), 1000))}
    if cursor:
        params["cursor"] = cursor
    if status:
        params["status"] = status
    if event_ticker:
        params["event_ticker"] = event_ticker
    if series_ticker:
        params["series_ticker"] = series_ticker
    if mve_filter in {"only", "exclude"}:
        params["mve_filter"] = mve_filter

    data = _request_json("GET", "/markets", params=params)
    markets, next_cursor = _parse_list_response(data, "markets")
    return {"markets": markets, "cursor": next_cursor, "raw": data}


def list_events(
    *,
    limit: int = 200,
    cursor: str | None = None,
    status: str | None = None,
    series_ticker: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": max(1, min(int(limit or 200), 500))}
    if cursor:
        params["cursor"] = cursor
    if status:
        params["status"] = status
    if series_ticker:
        params["series_ticker"] = series_ticker

    data = _request_json("GET", "/events", params=params)
    events, next_cursor = _parse_list_response(data, "events")
    return {"events": events, "cursor": next_cursor, "raw": data}


def list_series(
    *,
    limit: int = 200,
    cursor: str | None = None,
    category: str | None = None,
    tags: str | None = None,
    include_product_metadata: bool | None = None,
    include_volume: bool | None = None,
    min_updated_ts: int | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": max(1, min(int(limit or 200), 200))}
    if cursor:
        params["cursor"] = cursor
    if category:
        params["category"] = category
    if tags:
        params["tags"] = tags
    if include_product_metadata is not None:
        params["include_product_metadata"] = bool(include_product_metadata)
    if include_volume is not None:
        params["include_volume"] = bool(include_volume)
    if min_updated_ts is not None:
        params["min_updated_ts"] = int(min_updated_ts)

    data = _request_json("GET", "/series", params=params)
    series, next_cursor = _parse_list_response(data, "series")
    return {"series": series, "cursor": next_cursor, "raw": data}


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _norm_text(value: Any) -> str:
    return re.sub(
        r"\s+",
        " ",
        re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()),
    ).strip()


def _as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _fmt_num(value: float) -> str:
    """Format a number for display: remove trailing zeros after decimal point."""
    if float(value).is_integer():
        return str(int(value))
    return f"{value:g}"


def _parse_iso_dt(value: Any) -> datetime.datetime | None:
    if isinstance(value, datetime.datetime):
        dt = value
    else:
        raw = str(value or "").strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        elif re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$", raw):
            raw += "+00:00"
        try:
            dt = datetime.datetime.fromisoformat(raw)
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)


def _parse_et_local_dt(value: Any) -> datetime.datetime | None:
    raw = str(value or "").strip()
    if not raw or not re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?$", raw):
        return None
    try:
        dt = datetime.datetime.fromisoformat(raw)
    except Exception:
        return None
    try:
        import zoneinfo

        eastern = zoneinfo.ZoneInfo("America/New_York")
        return dt.replace(tzinfo=eastern).astimezone(datetime.timezone.utc)
    except Exception:
        try:
            import pytz

            eastern = pytz.timezone("America/New_York")
            return eastern.localize(dt).astimezone(datetime.timezone.utc)
        except Exception:
            return dt.replace(tzinfo=datetime.timezone.utc)


def _market_time(market: dict[str, Any]) -> datetime.datetime | None:
    for key in (
        "occurrence_datetime",
        "close_time",
        "expiration_time",
        "expected_expiration_time",
        "latest_expiration_time",
        "open_time",
    ):
        dt = _parse_iso_dt(market.get(key))
        if dt is not None:
            return dt
    for key in ("ticker", "event_ticker"):
        dt = _ticker_time(str(market.get(key) or ""))
        if dt is not None:
            return dt
    return None


def _ticker_time(value: str) -> datetime.datetime | None:
    text = str(value or "").upper().strip()
    if not text:
        return None
    match = re.search(r"(?<!\d)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})(\d{4})?", text)
    if not match:
        return None
    year = 2000 + int(match.group(1))
    month = _TICKER_MONTHS.get(match.group(2), 0)
    day = int(match.group(3))
    hhmm = match.group(4) or ""
    if not month:
        return None
    if len(hhmm) == 4:
        hour = int(hhmm[:2])
        minute = int(hhmm[2:])
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return _parse_et_local_dt(f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}")
    return _parse_iso_dt(f"{year:04d}-{month:02d}-{day:02d}")


def _bet_start_dt(bet: dict[str, Any]) -> datetime.datetime | None:
    for value in (
        bet.get("scheduled_start"),
        bet.get("start_time"),
        bet.get("game_datetime"),
    ):
        dt = _parse_iso_dt(value)
        if dt is not None:
            return dt

    game_date = str(bet.get("game_date") or "").strip()
    game_time = str(bet.get("game_time") or "").strip()
    if game_date and game_time:
        dt = _parse_et_local_dt(f"{game_date}T{game_time}")
        if dt is not None:
            return dt

    for key in ("game", "game_key"):
        raw = str(bet.get(key) or "")
        if "#" in raw:
            suffix = raw.rsplit("#", 1)[-1].strip()
            dt = _parse_et_local_dt(suffix) or _parse_iso_dt(suffix)
            if dt is not None:
                return dt

    for value in (bet.get("game_date"),):
        dt = _parse_iso_dt(value)
        if dt is not None:
            return dt
    return None


def _bet_date_ticker_tokens(bet: dict[str, Any]) -> list[str]:
    """Build date-shaped tokens often present in Kalshi tickers/event ids."""
    dt = _bet_start_dt(bet)
    if dt is None:
        return []
    dt_utc = dt.astimezone(datetime.timezone.utc)
    mon = _MONTH_ABBR_BY_NUM.get(int(dt_utc.month), "")
    if not mon:
        return []

    yy = dt_utc.year % 100
    tokens = [
        f"{yy:02d}{mon}{dt_utc.day:02d}",
        f"{mon}{dt_utc.day:02d}",
        f"{dt_utc.year:04d}{dt_utc.month:02d}{dt_utc.day:02d}",
    ]
    if dt_utc.hour or dt_utc.minute:
        tokens.append(f"{yy:02d}{mon}{dt_utc.day:02d}{dt_utc.hour:02d}{dt_utc.minute:02d}")

    out: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        t = str(token or "").strip().upper()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _bet_identity(bet: dict[str, Any], index: int = 0) -> str:
    for key in ("uid", "bet_uid", "prediction_uid"):
        value = str(bet.get(key) or "").strip()
        if value:
            return value
    return f"ready_{index}"


def _split_matchup_teams(value: Any) -> tuple[str, str]:
    raw = str(value or "").strip()
    if not raw:
        return "", ""
    if "#" in raw:
        raw = raw.split("#", 1)[0]
    raw = raw.replace(" vs. ", " vs ")
    for delim in ("@", " vs ", " v "):
        if delim in raw:
            parts = [p.strip() for p in raw.split(delim, 1)]
            if len(parts) == 2:
                away, home = parts[0], parts[1]
                return away, home
    return "", ""


def _normalize_bet_type(value: Any, pick_text: str = "") -> str:
    bt = _norm_text(value)
    pick_norm = _norm_text(pick_text)
    if any(token in bt for token in ("player prop", "prop")):
        return "player_prop"
    if any(token in bt for token in ("f5", "first 5", "first five")):
        return "f5_moneyline" if "total" not in bt else "f5_total"
    if any(token in bt for token in ("moneyline", "money line", "winner", "1x2", "match winner")):
        return "moneyline"
    if any(token in bt for token in ("run line", "spread", "handicap")):
        return "spread"
    if "team total" in bt:
        if "home" in bt:
            return "home_team_total"
        if "away" in bt:
            return "away_team_total"
        return "team_total"
    if any(token in bt for token in ("total", "over under", "goals o u", "btts")):
        return "total"
    if any(token in pick_norm for token in (" over ", " under ")):
        return "total"
    return bt.replace(" ", "_") if bt else "single"


def _canonical_prop_type(*values: Any) -> str:
    """Infer a canonical prop type token from free-form bet fields."""
    text = " ".join(_norm_text(v) for v in values if str(v or "").strip())
    if not text:
        return ""
    padded = f" {text} "
    for canonical, aliases in _PROP_TYPE_ALIASES.items():
        for alias in aliases:
            alias_norm = _norm_text(alias)
            if not alias_norm:
                continue
            if f" {alias_norm} " in padded or alias_norm in text:
                return canonical
    return ""


def _normalize_ready_bet(bet: dict[str, Any]) -> dict[str, Any]:
    """Canonicalize a ready-bet row so matcher scoring is stable across sources."""
    if not isinstance(bet, dict):
        return {}

    clean: dict[str, Any] = dict(bet)
    for key in (
        "uid", "bet_uid", "prediction_uid", "kind", "label", "pick", "game", "game_key",
        "game_date", "game_time", "scheduled_start", "start_time", "game_datetime", "sport",
        "bet_type", "raw_bet_type", "prop_type", "player_name", "name", "team", "home_team", "away_team",
        "direction", "recommendation", "side_default",
    ):
        if key in clean and clean.get(key) is not None:
            clean[key] = str(clean.get(key) or "").strip()

    pick_text = str(clean.get("pick") or clean.get("label") or "")
    clean["bet_type"] = _normalize_bet_type(clean.get("bet_type") or clean.get("raw_bet_type"), pick_text)

    game_key = str(clean.get("game_key") or clean.get("game") or "")
    away_from_game, home_from_game = _split_matchup_teams(game_key)
    if not clean.get("away_team") and away_from_game:
        clean["away_team"] = away_from_game
    if not clean.get("home_team") and home_from_game:
        clean["home_team"] = home_from_game

    if not clean.get("team"):
        picked = _picked_team_name(clean)
        if picked:
            clean["team"] = picked

    if not clean.get("player_name"):
        clean["player_name"] = str(clean.get("name") or "").strip()

    prop_type = _canonical_prop_type(
        clean.get("prop_type"),
        clean.get("stat_type"),
        clean.get("bet_type"),
        clean.get("pick"),
        clean.get("label"),
    )
    if prop_type:
        clean["prop_type"] = prop_type

    if not clean.get("direction"):
        dir_text = _norm_text(" ".join(
            str(clean.get(k) or "")
            for k in ("direction", "recommendation", "pick", "label")
        ))
        if "under" in dir_text:
            clean["direction"] = "UNDER"
        elif "over" in dir_text:
            clean["direction"] = "OVER"

    if clean.get("line") is not None:
        line_num = _as_float(clean.get("line"))
        clean["line"] = line_num if line_num is not None else clean.get("line")

    if not clean.get("game") and clean.get("game_key"):
        clean["game"] = clean.get("game_key")
    if not clean.get("game_key") and clean.get("game"):
        clean["game_key"] = clean.get("game")

    if not clean.get("sport"):
        clean["sport"] = _bet_sport_tag(clean)

    side_default = str(clean.get("side_default") or "").lower()
    if side_default not in {"yes", "no"}:
        clean["side_default"] = "no" if "under" in _norm_text(pick_text) else "yes"

    # Ensure combo legs are normalized in backend before matching.
    if _bet_kind_tag(clean) == "combo":
        clean_legs: list[dict[str, Any]] = []
        for leg in (clean.get("legs") or []):
            if not isinstance(leg, dict):
                continue
            n_leg = _normalize_ready_bet(leg)
            if n_leg:
                clean_legs.append(n_leg)
        clean["legs"] = clean_legs

    return clean


def _bet_signature(bet: dict[str, Any]) -> str:
    parts = [
        str(bet.get("kind") or ""),
        str(bet.get("label") or bet.get("pick") or ""),
        str(bet.get("bet_type") or bet.get("prop_type") or ""),
        str(bet.get("player_name") or ""),
        str(bet.get("team") or ""),
        str(bet.get("home_team") or ""),
        str(bet.get("away_team") or ""),
        str(bet.get("line") if bet.get("line") is not None else ""),
        str(bet.get("direction") or ""),
        str(bet.get("game") or bet.get("game_key") or ""),
        str(bet.get("game_date") or ""),
        str(bet.get("scheduled_start") or bet.get("start_time") or bet.get("game_time") or ""),
    ]
    return "|".join(parts)


def _entity_aliases(name: Any) -> set[str]:
    norm = _norm_text(name)
    if not norm:
        return set()

    tokens = [tok for tok in norm.split() if tok]
    no_stop = [tok for tok in tokens if tok not in _ENTITY_STOPWORDS]
    aliases = {norm}

    if no_stop:
        aliases.add(" ".join(no_stop))
        aliases.add(no_stop[-1])
        if len(no_stop[0]) >= 4:
            aliases.add(no_stop[0])
        if len(no_stop) >= 2:
            first_two = " ".join(no_stop[:2])
            last_two = " ".join(no_stop[-2:])
            if first_two not in _AMBIGUOUS_TEAM_ALIASES:
                aliases.add(first_two)
            if last_two not in _AMBIGUOUS_TEAM_ALIASES:
                aliases.add(last_two)
            aliases.add(" ".join(no_stop[:-1] + [no_stop[-1][:1]]))
    if len(tokens) >= 2:
        token_last_two = " ".join(tokens[-2:])
        if token_last_two not in _AMBIGUOUS_TEAM_ALIASES:
            aliases.add(token_last_two)
    if "76ers" in str(name):
        aliases.update({"76ers", "sixers"})

    aliases.update(_SPECIAL_ENTITY_ALIASES.get(norm, set()))
    return {alias for alias in aliases if len(alias) >= 3}


def _entity_match_score(text: str, name: Any) -> float:
    norm_name = _norm_text(name)
    if not text or not norm_name:
        return 0.0

    aliases = _entity_aliases(name)
    name_tokens = [tok for tok in norm_name.split() if len(tok) >= 3 and tok not in _ENTITY_STOPWORDS]
    best = 0.0

    if norm_name in text:
        best = max(best, 7.0)
    if name_tokens and all(tok in text for tok in name_tokens):
        best = max(best, 5.5 if len(name_tokens) >= 2 else 2.4)

    last_token = name_tokens[-1] if name_tokens else ""
    for alias in aliases:
        if alias in text:
            if " " in alias:
                score = 1.6 if alias in _AMBIGUOUS_TEAM_ALIASES else 5.2
            else:
                score = 2.2
            if alias == last_token and len(name_tokens) >= 2:
                score = 3.2
            best = max(best, score)
    return best


def _token_overlap_score(text: str, *values: Any) -> float:
    score = 0.0
    seen: set[str] = set()
    for value in values:
        norm = _norm_text(value)
        for token in norm.split():
            if len(token) < 4 or token in _ENTITY_STOPWORDS or token in seen:
                continue
            seen.add(token)
            if token in text:
                score += 0.75
    return score


def _line_match_score(text: str, line: Any, *, allow_half_step_integer: bool = False) -> float:
    num = _as_float(line)
    if num is None:
        return 0.0
    candidates = {
        _norm_text(str(num).rstrip("0").rstrip(".")),
        _norm_text(f"{num:.1f}"),
    }
    if float(num).is_integer():
        candidates.add(_norm_text(str(int(num))))
        candidates.add(_norm_text(f"{int(num)}.0"))
    elif allow_half_step_integer:
        rounded_half = round(num * 2.0)
        if abs(num * 2.0 - rounded_half) <= 1e-9 and rounded_half % 2 == 1:
            # Kalshi player props often phrase 2.5 as "3+" and similar threshold markets.
            candidates.add(_norm_text(str(int(num + 0.5))))
    return 2.8 if any(candidate and candidate in text for candidate in candidates) else 0.0


def _line_candidate_numbers(text: str, *, bet_num: float | None = None) -> list[float]:
    import re as _re

    nums: list[float] = []
    max_diff = None
    if bet_num is not None:
        scale = abs(float(bet_num or 0.0))
        # Wider tolerance helps map model lines to nearby listed Kalshi thresholds.
        max_diff = max(10.0, min(40.0, scale * 0.25))

    for tok in _re.findall(r'\b\d+(?:[._]\d+)?\b', text):
        val = _as_float(tok.replace("_", "."))
        if val is None:
            continue
        if val < 0.25 or val > 600.0:
            continue
        if 1900.0 <= val <= 2100.0:
            continue
        if max_diff is not None and abs(val - bet_num) > max_diff:
            continue
        nums.append(val)
    return nums


def _line_proximity_score(text: str, line: Any, *, direction: str = "") -> float:
    """Return a score based on how close the nearest number in text is to the bet line.

    Used as a fallback when _line_match_score returns 0.  Kalshi prop/total markets
    have discrete thresholds (32, 35, 40 points; 195.5, 197 total) that often differ
    from a model's predicted line.  Proximity scoring lets us pick the closest Kalshi
    market without requiring exact line equality.

    When ``direction`` is provided ("over"/"under"), direction-aligned thresholds score
    higher than direction-opposing thresholds.  This breaks the tie between markets
    equidistant from the bet line (e.g. "35 pts" and "30 pts" when bet line=32.5 over).
    """
    bet_num = _as_float(line)
    if bet_num is None:
        return 0.0
    nums = _line_candidate_numbers(text, bet_num=bet_num)
    if not nums:
        return 0.0

    moderate_window = max(5.0, min(12.0, abs(bet_num) * 0.05))
    far_window = max(8.0, min(20.0, abs(bet_num) * 0.10))

    direction_norm = _norm_text(str(direction))
    is_over = "over" in direction_norm
    is_under = "under" in direction_norm

    if is_over:
        favorable = [n for n in nums if n <= bet_num]
        stretch = [n for n in nums if n > bet_num]
    elif is_under:
        favorable = [n for n in nums if n >= bet_num]
        stretch = [n for n in nums if n < bet_num]
    else:
        # No direction info — proximity only.
        # ±1 is treated as essentially exact (Kalshi thresholds are often ±0.5–1 from model).
        closest_diff = min(abs(n - bet_num) for n in nums)
        if closest_diff <= 1.0:
            return 2.8   # ±1 → essentially exact
        if closest_diff <= 2.5:
            return 2.0   # very close (was 1.2)
        if closest_diff <= moderate_window:
            return 1.0   # close enough to prefer over nothing
        if closest_diff <= far_window:
            return 0.4
        return 0.0

    # For OVER picks, a slightly lower Kalshi threshold is still a valid match and usually
    # easier to clear; for UNDER picks, the mirror image applies.  Also allow a nearby
    # tougher threshold on the opposite side when it is the closest open market.
    closest_favorable_diff = min((abs(n - bet_num) for n in favorable), default=float("inf"))
    closest_stretch_diff = min((abs(n - bet_num) for n in stretch), default=float("inf"))

    if closest_favorable_diff <= 1.0:
        return 2.8
    if closest_stretch_diff <= 1.0:
        return 2.7
    if closest_favorable_diff <= 3.0:
        return 2.6
    if closest_stretch_diff <= 3.0:
        return 2.4
    if closest_favorable_diff <= far_window:
        return 1.7
    if closest_stretch_diff <= far_window:
        return 1.4
    return 0.0


def _series_stat_alignment_score(
    *,
    series_hint_text: str,
    prop_type: Any,
    mismatch_penalty: float = -6.0,
    match_bonus: float = 4.0,
) -> float:
    """Score whether a series/event ticker aligns with the player's stat type."""
    hint_upper = str(series_hint_text or "").upper()
    prop_type_norm = _norm_text(prop_type)
    if not hint_upper or not prop_type_norm:
        return 0.0
    for series_suffix, prop_hints in _SERIES_STAT_HINTS.items():
        if series_suffix in hint_upper:
            if any(h in prop_type_norm for h in prop_hints):
                return match_bonus
            return mismatch_penalty
    return 0.0


def _combined_market_text(markets: list[dict[str, Any]]) -> str:
    return _norm_text(
        " ".join(
            " ".join(
                str(market.get(key) or "")
                for key in (
                    "ticker",
                    "event_ticker",
                    "title",
                    "subtitle",
                    "yes_sub_title",
                    "no_sub_title",
                    "question",
                )
            )
            for market in markets
            if isinstance(market, dict)
        )
    )


def _closest_num_in_text(text: str, bet_num: float) -> float | None:
    """Return the number in *text* that is closest to *bet_num*.

    Used to extract the actual Kalshi threshold (e.g. 35) from a matched market's
    text so we can annotate 'Adjusted: your line 32.5 → Kalshi threshold 35'.
    Only considers numbers in the range [3, 600] to avoid jersey numbers / years.
    Returns None if no candidates found.
    """
    nums = _line_candidate_numbers(text, bet_num=bet_num)
    if not nums:
        return None
    return min(nums, key=lambda n: abs(n - bet_num))


def _build_event_index(markets: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    event_index: dict[str, dict[str, Any]] = {}
    for market in markets:
        if not isinstance(market, dict):
            continue
        event_ticker = str(market.get("event_ticker") or market.get("ticker") or "").strip()
        if not event_ticker:
            continue
        bucket = event_index.setdefault(
            event_ticker,
            {
                "event_ticker": event_ticker,
                "series_ticker": _series_ticker_from_market(market),
                "markets": [],
                "sport": "",
                "market_kinds": set(),
            },
        )
        bucket["markets"].append(market)
        if not bucket["sport"]:
            bucket["sport"] = str(market.get("sport") or "").strip().lower() or _market_sport_tag(market)
        market_kind = _market_kind_tag(market)
        if market_kind:
            bucket["market_kinds"].add(market_kind)

    for bucket in event_index.values():
        markets_in_event = bucket.get("markets") or []
        if isinstance(bucket.get("market_kinds"), set):
            bucket["market_kinds"] = sorted(bucket["market_kinds"])
        bucket["text"] = _combined_market_text(markets_in_event)
        times = [dt for dt in (_market_time(m) for m in markets_in_event) if dt is not None]
        bucket["occurrence_datetime"] = (
            min(times).isoformat().replace("+00:00", "Z") if times else None
        )
    return event_index


def _score_event_group(bet: dict[str, Any], event_group: dict[str, Any]) -> float:
    text = str(event_group.get("text") or "")
    if not text:
        return 0.0

    bet_sport = _bet_sport_tag(bet)
    market_sport = str(event_group.get("sport") or "")
    if bet_sport and market_sport and bet_sport != market_sport:
        return 0.0

    pseudo_market = {"occurrence_datetime": event_group.get("occurrence_datetime")}
    time_score = _time_match_score(bet, pseudo_market)
    if time_score < -1.5:
        return 0.0

    bet_kind = _bet_kind_tag(bet)
    event_kinds = {str(kind) for kind in (event_group.get("market_kinds") or []) if kind}
    if bet_kind == "moneyline" and event_kinds and "moneyline" not in event_kinds:
        return 0.0
    if bet_kind == "spread" and event_kinds and "spread" not in event_kinds:
        return 0.0
    if bet_kind in {"total", "team_total"} and event_kinds and "total" not in event_kinds:
        return 0.0

    if bet_kind == "player_prop":
        player_score = _entity_match_score(text, bet.get("player_name") or bet.get("name"))
        if player_score < 2.0:
            return 0.0
        score = player_score * 2.1
        score += max(
            _entity_match_score(text, bet.get("team")),
            _entity_match_score(text, bet.get("home_team")),
            _entity_match_score(text, bet.get("away_team")),
        ) * 0.35
        score += _token_overlap_score(text, bet.get("prop_type"), bet.get("direction"), bet.get("label"))
        line_score = _line_match_score(text, bet.get("line"), allow_half_step_integer=True)
        if line_score <= 0.0:
            direction_hint = str(
                bet.get("direction") or bet.get("pick") or bet.get("label") or ""
            )
            line_score = _line_proximity_score(text, bet.get("line"), direction=direction_hint)
        score += line_score
        score += time_score

        # Strongly prefer the event whose series ticker matches the bet's stat type.
        # This disambiguates KXNBAPTS vs KXNBAREB vs KXNBAAST for the same player.
        event_ticker_upper = str(event_group.get("event_ticker") or "").upper()
        stat_alignment = _series_stat_alignment_score(
            series_hint_text=f"{event_ticker_upper} {event_group.get('series_ticker') or ''}",
            prop_type=bet.get("prop_type"),
            mismatch_penalty=-7.0,
            match_bonus=4.2,
        )
        if stat_alignment <= -6.0:
            return 0.0
        score += stat_alignment

        score += _series_hint_score(
            bet,
            str(event_group.get("series_ticker") or event_ticker_upper),
            weight=1.15,
            search_text=f"{event_group.get('event_ticker') or ''} {text}",
        )
        score += _date_hint_score(
            bet,
            event_group.get("event_ticker"),
            event_group.get("series_ticker"),
            text,
        )

        return score

    home_score = _entity_match_score(text, bet.get("home_team"))
    away_score = _entity_match_score(text, bet.get("away_team"))
    picked_team = _picked_team_name(bet)
    picked_team_score = _entity_match_score(text, picked_team) if picked_team else 0.0
    pick_score = max(
        picked_team_score,
        _entity_match_score(text, bet.get("team")),
        _entity_match_score(text, bet.get("pick")),
        _entity_match_score(text, bet.get("label")),
    )

    if max(home_score, away_score, pick_score) < 1.7:
        return 0.0
    if bet_kind == "moneyline" and picked_team and picked_team_score < 2.0:
        return 0.0
    if (bet.get("home_team") or bet.get("away_team")) and home_score < 1.5 and away_score < 1.5:
        return 0.0

    score = time_score
    score += home_score + away_score
    score += pick_score * 1.2
    score += _token_overlap_score(text, bet.get("label"), bet.get("pick"), bet.get("bet_type"))
    score += _series_hint_score(
        bet,
        str(event_group.get("series_ticker") or ""),
        weight=0.9,
        search_text=f"{event_group.get('event_ticker') or ''} {text}",
    )
    score += _date_hint_score(
        bet,
        event_group.get("event_ticker"),
        event_group.get("series_ticker"),
        text,
    )

    if bet_kind in {"spread", "total", "team_total"}:
        line_score = _line_match_score(text, bet.get("line"))
        if line_score <= 0.0:
            # Kalshi total/spread lines differ from model lines; use proximity fallback
            # so that team/time matching still identifies the correct event.
            direction_hint = str(
                bet.get("direction") or bet.get("pick") or bet.get("label") or ""
            )
            line_score = _line_proximity_score(text, bet.get("line"), direction=direction_hint)
        score += line_score
    return score


def _picked_team_name(bet: dict[str, Any]) -> str:
    pick_text = _norm_text(" ".join(str(bet.get(key) or "") for key in ("pick", "label")))
    home_team = str(bet.get("home_team") or "").strip()
    away_team = str(bet.get("away_team") or "").strip()

    if pick_text and (home_team or away_team):
        home_score = _entity_match_score(pick_text, home_team)
        away_score = _entity_match_score(pick_text, away_team)
        if max(home_score, away_score) >= 2.2:
            return home_team if home_score >= away_score else away_team

    team = str(bet.get("team") or "").strip()
    if team:
        return team

    return ""


def _resolve_market_side(bet: dict[str, Any], market: dict[str, Any]) -> str:
    direction_text = _norm_text(
        " ".join(str(bet.get(key) or "") for key in ("direction", "pick", "label", "bet_type"))
    )
    market_text = _market_text(market)
    bet_kind = _bet_kind_tag(bet)

    if bet_kind == "player_prop":
        return "no" if "under" in direction_text else "yes"

    if bet_kind in {"total", "team_total"}:
        if "under" in direction_text:
            return "no"
        if "over" in direction_text:
            return "yes"

    picked_team = _picked_team_name(bet)
    if picked_team:
        selected_side_text = _market_selected_side_text(market)
        if selected_side_text:
            selected_score = _entity_match_score(selected_side_text, picked_team)
            home_team = str(bet.get("home_team") or "").strip()
            away_team = str(bet.get("away_team") or "").strip()
            other_team = away_team if picked_team == home_team else home_team if picked_team == away_team else ""
            other_score = _entity_match_score(selected_side_text, other_team) if other_team else 0.0
            if selected_score >= 2.2 and selected_score >= other_score:
                return "yes"
            if other_score >= 2.2 and other_score > selected_score:
                return "no"

        picked_score = _entity_match_score(market_text, picked_team)
        if picked_score >= 2.2:
            return "yes"

        home_team = str(bet.get("home_team") or "").strip()
        away_team = str(bet.get("away_team") or "").strip()
        other_team = away_team if picked_team == home_team else home_team if picked_team == away_team else ""
        if other_team and _entity_match_score(market_text, other_team) >= 2.2:
            return "no"

    return "no" if str(bet.get("side_default") or "yes").lower() == "no" else "yes"


def _bet_sport_tag(bet: dict[str, Any]) -> str:
    text = _norm_text(
        " ".join(
            str(bet.get(key) or "")
            for key in ("sport", "bet_type", "prop_type", "game", "label")
        )
    )
    if any(token in text for token in ("basketball", "nba", "wnba")):
        return "basketball"
    if any(token in text for token in ("baseball", "mlb")):
        return "baseball"
    if any(token in text for token in ("football", "americanfootball", "american football", "nfl", "ncaaf", "cfl", "xfl", "ufl")):
        return "football"
    if any(token in text for token in ("hockey", "nhl", "icehockey", "ice hockey", "ahl")):
        return "hockey"
    if any(token in text for token in ("soccer", "mls", "premier", "bundesliga", "serie a", "laliga", "ligue 1", "uefa", "fifa", "1x2", "btts", "goals o u")):
        return "soccer"
    if any(token in text for token in ("tennis", "atp", "wta", "set", "aces", "double fault")):
        return "tennis"
    if any(token in text for token in ("boxing", "box", "knockout", "ko", "decision", "round")):
        return "boxing"
    if any(token in text for token in ("mma", "ufc", "bellator", "pfl", "submission", "tko")):
        return "mma"
    if any(token in text for token in ("golf", "pga", "birdie", "bogey", "stroke")):
        return "golf"
    if any(token in text for token in ("f1", "formula", "nascar", "race", "lap", "qualifying", "pole")):
        return "motorsports"
    if any(token in text for token in ("cricket", "wicket", "innings", "run line", "t20", "odi")):
        return "cricket"
    return ""


def _series_ticker_from_market(market: dict[str, Any]) -> str:
    explicit = str(market.get("series_ticker") or "").strip()
    if explicit:
        return explicit.upper()
    for key in ("event_ticker", "ticker"):
        value = str(market.get(key) or "").strip()
        if not value:
            continue
        if "-" in value:
            return value.split("-", 1)[0].upper()
        return value.upper()
    return ""


def _bet_series_hints(bet: dict[str, Any]) -> tuple[list[str], list[str]]:
    """Return (preferred_series, avoid_series) hints for Kalshi matching."""
    sport = _bet_sport_tag(bet)
    bet_kind = _bet_kind_tag(bet)
    bet_type = _norm_text(bet.get("bet_type") or "")
    prop_type = _norm_text(bet.get("prop_type") or bet.get("stat_type") or "")
    label = _norm_text(bet.get("label") or bet.get("pick") or "")
    league = _norm_text(bet.get("league") or bet.get("competition") or "")
    text = f"{bet_type} {prop_type} {label}"

    preferred: list[str] = []
    avoid: list[str] = []

    def _set_family(pref: list[str], bad: list[str]):
        preferred.extend(pref)
        avoid.extend(bad)

    # Honor explicit series tickers from upstream payload first.
    explicit_sources = [
        bet.get("kalshi_series_ticker"),
        bet.get("series_ticker"),
    ]
    for raw in explicit_sources:
        token = str(raw or "").strip().upper()
        if not token:
            continue
        token = re.sub(r"[^A-Z0-9_]+", "_", token).strip("_")
        if token:
            preferred.append(token)

    if league:
        if "wnba" in league:
            preferred.extend(["KXWNBAGAME", "KXWNBASPREAD", "KXWNBATOTAL", "KXWNBAPTS", "KXWNBAREB", "KXWNBAAST"])
        elif "nba" in league:
            preferred.extend(["KXNBAGAME", "KXNBASPREAD", "KXNBATOTAL", "KXNBAPTS", "KXNBAREB", "KXNBAAST"])
        elif "mlb" in league:
            preferred.extend(["KXMLBGAME", "KXMLBSPREAD", "KXMLBTOTAL", "KXMLBF5", "MLBWIN", "MLBOU"])
        elif "nhl" in league:
            preferred.extend(["NHLWIN", "KXNHLSPREAD", "KXNHLTOTAL", "NHLOU"])
        elif "nfl" in league:
            preferred.extend(["NFLWIN", "KXNFL", "NFLOU", "NFLTD"])
        elif any(tok in league for tok in ("epl", "premier", "mls", "champions league", "uefa", "fifa")):
            preferred.extend(["KXEPL", "KXEPLSPREAD", "KXMLS", "KXMLSGAME", "KXMLSSPREAD", "EPLWIN", "MLSWIN", "UCLWIN", "SOCCER", "MATCH"])

    if sport == "baseball":
        if "f5" in bet_type or "first 5" in text or "first five" in text:
            _set_family(["KXMLBF5"], ["KXMLBGAME", "KXMLBTOTAL", "KXMLBSPREAD"])
        elif bet_kind == "moneyline":
            _set_family(["KXMLBGAME", "MLBWIN"], ["KXMLBSPREAD", "KXMLBTOTAL"])
        elif bet_kind == "spread":
            _set_family(["KXMLBSPREAD"], ["KXMLBGAME", "KXMLBTOTAL"])
        elif bet_kind in {"total", "team_total"}:
            _set_family(["KXMLBTOTAL", "MLBOU"], ["KXMLBGAME", "KXMLBSPREAD"])
        elif bet_kind == "player_prop":
            if any(tok in prop_type for tok in ("hit",)):
                _set_family(["KXMLBHIT", "MLBHITS"], ["KXMLBRBI", "KXMLBTB", "MLBHR"])
            elif "rbi" in prop_type:
                _set_family(["KXMLBRBI"], ["KXMLBHIT", "KXMLBTB", "MLBHR"])
            elif any(tok in prop_type for tok in ("total_bases", "total base", "tb")):
                _set_family(["KXMLBTB"], ["KXMLBHIT", "KXMLBRBI", "MLBHR"])
            elif any(tok in prop_type for tok in ("home_run", "home run", "hr")):
                _set_family(["MLBHR"], ["KXMLBHIT", "KXMLBRBI", "KXMLBTB"])
            elif any(tok in prop_type for tok in ("strikeout", " k ")) or prop_type == "k":
                _set_family(["MLBK"], ["KXMLBHIT", "KXMLBRBI", "KXMLBTB", "MLBHR"])
            elif "run" in prop_type:
                _set_family(["MLBRUNS", "KXMLBHRR"], ["KXMLBHIT", "KXMLBRBI", "KXMLBTB"])
    elif sport == "basketball":
        if bet_kind == "moneyline":
            _set_family(["KXNBAGAME", "KXWNBAGAME"], ["KXNBASPREAD", "KXNBATOTAL", "KXNBATEAMTOTAL"])
        elif bet_kind == "spread":
            _set_family(["KXNBASPREAD", "KXWNBASPREAD"], ["KXNBAGAME", "KXNBATOTAL", "KXNBATEAMTOTAL"])
        elif bet_kind == "team_total":
            _set_family(["KXNBATEAMTOTAL"], ["KXNBAGAME", "KXNBASPREAD", "KXNBATOTAL"])
        elif bet_kind == "total":
            _set_family(["KXNBATOTAL", "KXWNBATOTAL"], ["KXNBAGAME", "KXNBASPREAD", "KXNBATEAMTOTAL"])
        elif bet_kind == "player_prop":
            if any(tok in prop_type for tok in ("point", "pts")):
                _set_family(["KXNBAPTS", "KXWNBAPTS"], ["KXNBAREB", "KXNBAAST", "KXNBARA"])
            elif any(tok in prop_type for tok in ("rebound", "reb")):
                _set_family(["KXNBAREB", "KXWNBAREB"], ["KXNBAPTS", "KXNBAAST", "KXNBARA"])
            elif any(tok in prop_type for tok in ("assist", "ast")):
                _set_family(["KXNBAAST", "KXWNBAAST"], ["KXNBAPTS", "KXNBAREB", "KXNBARA"])
    elif sport == "hockey":
        if bet_kind == "moneyline":
            _set_family(["NHLWIN", "KXAHLGAME"], ["KXNHLTOTAL", "KXNHLSPREAD"])
        elif bet_kind == "spread":
            _set_family(["KXNHLSPREAD"], ["NHLWIN", "KXNHLTOTAL"])
        elif bet_kind == "total":
            _set_family(["KXNHLTOTAL", "NHLOU"], ["NHLWIN", "KXNHLSPREAD"])
        elif bet_kind == "player_prop":
            if any(tok in prop_type for tok in ("assist", "ast")):
                _set_family(["KXNHLAST"], ["KXNHLPTS", "KXNHLGOAL", "KXNHLANYGOAL"])
            elif "goal" in prop_type:
                _set_family(["KXNHLGOAL", "KXNHLANYGOAL"], ["KXNHLPTS", "KXNHLAST"])
            elif any(tok in prop_type for tok in ("point", "pts")):
                _set_family(["KXNHLPTS"], ["KXNHLAST", "KXNHLGOAL", "KXNHLANYGOAL"])
    elif sport == "football":
        if bet_kind == "moneyline":
            _set_family(["NFLWIN", "KXNFL"], ["NFLOU", "NFLTD"])
        elif bet_kind == "spread":
            _set_family(["KXNFL"], ["NFLWIN", "NFLOU"])
        elif bet_kind in {"total", "team_total"}:
            _set_family(["NFLOU", "KXNFL"], ["NFLWIN", "NFLTD"])
        elif bet_kind == "player_prop":
            if any(tok in prop_type for tok in ("touchdown", "td")):
                _set_family(["NFLTD"], ["NFLWIN", "NFLOU"])
            elif any(tok in prop_type for tok in ("yard", "passing", "rushing", "receiving", "reception")):
                _set_family(["KXNFL", "NFLOU"], ["NFLWIN"])
    elif sport == "soccer":
        if bet_kind == "moneyline":
            _set_family(["KXMLSGAME", "KXEPL", "MLSWIN", "EPLWIN", "UCLWIN"], ["KXMLSSPREAD"])
        elif bet_kind == "spread":
            _set_family(["KXMLSSPREAD", "KXEPLSPREAD"], ["KXMLSGAME", "MLSWIN", "EPLWIN"])
        elif bet_kind in {"total", "team_total"}:
            _set_family(["KXMLS", "KXEPL", "UCLWIN"], ["KXMLSGAME", "KXMLSSPREAD"])
    elif sport == "tennis":
        _set_family(["KXTENNIS", "ATP", "WTA", "TENNIS", "MATCH", "SET"], [])
    elif sport == "boxing":
        _set_family(["KXBOX", "BOXING", "FIGHT", "ROUND"], [])
    elif sport == "mma":
        _set_family(["KXMMA", "UFC", "PFL", "BELLATOR", "FIGHT", "ROUND"], [])
    elif sport == "golf":
        _set_family(["KXGOLF", "PGA", "LPGA", "GOLF", "TOURNAMENT", "WINNER"], [])
    elif sport == "motorsports":
        _set_family(["KXF1", "F1", "NASCAR"], [])
    elif sport == "cricket":
        _set_family(["KXCRICKET", "CRICKET"], [])

    preferred = list(dict.fromkeys([p.upper() for p in preferred if p]))
    for token in _bet_date_ticker_tokens(bet):
        if token not in preferred:
            preferred.append(token)
    avoid = [a.upper() for a in dict.fromkeys([a for a in avoid if a]) if a.upper() not in preferred]
    return preferred, avoid


def _series_hint_score(
    bet: dict[str, Any],
    series_ticker: str,
    *,
    weight: float = 1.0,
    search_text: str = "",
) -> float:
    series_upper = f"{str(series_ticker or '').upper()} {str(search_text or '').upper()}"
    if not series_upper:
        return 0.0
    preferred, avoid = _bet_series_hints(bet)
    score = 0.0
    if preferred:
        if any(pref in series_upper for pref in preferred):
            score += 3.8 * weight
        elif _bet_sport_tag(bet):
            score -= 1.3 * weight
    if avoid and any(bad in series_upper for bad in avoid):
        score -= 3.2 * weight
    return score


def _date_hint_score(bet: dict[str, Any], *texts: Any) -> float:
    hints = _bet_date_ticker_tokens(bet)
    if not hints:
        return 0.0
    hay = " ".join(str(value or "") for value in texts).upper()
    if not hay:
        return 0.0
    return 1.7 if any(hint in hay for hint in hints) else 0.0


def _market_sport_tag(market: dict[str, Any]) -> str:
    explicit_sport = str(market.get("sport") or "").strip().lower()
    if explicit_sport in _VALID_SPORT_TAGS:
        return explicit_sport
    text = _norm_text(
        " ".join(
            str(market.get(key) or "")
            for key in (
                "ticker",
                "event_ticker",
                "title",
                "yes_sub_title",
                "no_sub_title",
                "category",
            )
        )
    )
    if "kxmve" in text or "crosscategory" in text or "multigame" in text:
        return "multi"
    # Check known series ticker prefixes first (most reliable)
    ticker_upper = str(market.get("ticker") or "").upper()
    event_upper = str(market.get("event_ticker") or "").upper()
    series_upper = _series_ticker_from_market(market)
    for prefix, sport in _SERIES_SPORT_MAP.items():
        if (
            ticker_upper.startswith(prefix)
            or event_upper.startswith(prefix)
            or series_upper.startswith(prefix)
        ):
            return sport
    # Fallback to text-based detection
    if any(token in text for token in ("kxnba", "kxwnba", "nbaptso", "nbamvp", "nbachamp", "basketball", "nba", "wnba")):
        return "basketball"
    if any(token in text for token in ("mlbwin", "mlbou", "mlbhr", "mlbruns", "mlbk", "kxmlb", "baseball", "mlb")):
        return "baseball"
    if any(token in text for token in ("nflwin", "nflou", "nfltd", "nflmvp", "nflsb", "kxnfl", "football", "nfl")):
        return "football"
    if any(token in text for token in ("nhlwin", "nhlou", "nhlchamp", "nhlin", "kxnhl", "hockey", "nhl")):
        return "hockey"
    if any(token in text for token in ("mslwin", "mlswin", "eplwin", "uclwin", "kxmls", "kxepl", "kxfifa", "soccer", "mls", "premier", "bundesliga", "serie a", "laliga", "ligue 1", "uefa", "fifa")):
        return "soccer"
    if any(token in text for token in ("tennis", "atp", "wta", "wimbledon", "roland", "us open", "australian open")):
        return "tennis"
    if any(token in text for token in ("boxing", "box", "heavyweight", "welterweight", "middleweight")):
        return "boxing"
    if any(token in text for token in ("mma", "ufc", "pfl", "bellator", "mixed martial arts")):
        return "mma"
    if any(token in text for token in ("golf", "pga", "lpga", "masters", "open championship", "ryder cup")):
        return "golf"
    if any(token in text for token in ("f1", "formula", "nascar", "indycar", "motogp", "race winner", "pole")):
        return "motorsports"
    if any(token in text for token in ("cricket", "ipl", "t20", "odi", "test match")):
        return "cricket"
    return ""


def _bet_kind_tag(bet: dict[str, Any]) -> str:
    kind = _norm_text(bet.get("kind"))
    bet_type = _norm_text(bet.get("bet_type") or bet.get("prop_type"))
    if kind == "combo":
        return "combo"
    if kind == "player prop" or bet.get("player_name"):
        return "player_prop"
    if any(token in bet_type for token in ("moneyline", "1x2", "draw no bet")):
        return "moneyline"
    if any(token in bet_type for token in ("run line", "spread")):
        return "spread"
    if "team total" in bet_type:
        return "team_total"
    if any(token in bet_type for token in ("total", "goals o u", "btts")):
        return "total"
    return "single"


def _market_text(market: dict[str, Any]) -> str:
    return _norm_text(
        " ".join(
            str(market.get(key) or "")
            for key in (
                "ticker",
                "event_ticker",
                "title",
                "yes_sub_title",
                "no_sub_title",
                "subtitle",
                "question",
            )
        )
    )


def _market_selected_side_text(market: dict[str, Any]) -> str:
    return _norm_text(
        " ".join(
            str(market.get(key) or "")
            for key in (
                "yes_sub_title",
                "subtitle",
                "ticker",
            )
        )
    )


def _is_combo_market(market: dict[str, Any]) -> bool:
    legs = market.get("mve_selected_legs")
    if isinstance(legs, list) and legs:
        return True
    ticker = str(market.get("ticker") or "").upper()
    event_ticker = str(market.get("event_ticker") or "").upper()
    return "KXMVE" in ticker or "KXMVE" in event_ticker


def _market_kind_tag(market: dict[str, Any]) -> str:
    if _is_combo_market(market):
        return "combo"

    text = _market_text(market)
    raw_title = str(market.get("title") or "")
    title = _norm_text(raw_title)
    event = _norm_text(market.get("event_ticker"))
    series_upper = _series_ticker_from_market(market)
    primary_participant = _norm_text(market.get("primary_participant_key"))

    # Player prop: use primary_participant_key first (most reliable for KX* series).
    # Fallback: title pattern "PlayerName: N+" where digit or "+" follows the colon.
    is_player_prop = "player" in primary_participant
    if not is_player_prop and ": " in raw_title:
        after_colon = raw_title[raw_title.index(": ") + 2:]
        if after_colon and (after_colon[0].isdigit() or after_colon.startswith("+")):
            is_player_prop = True
    if is_player_prop and any(hint in text for hint in _PROP_HINTS):
        return "player_prop"

    if any(token in series_upper for token in ("SPREAD", "RUNLINE")):
        return "spread"
    if any(token in series_upper for token in ("TEAMTOTAL", "TOTAL", "BTTS")):
        return "total"
    if any(token in series_upper for token in ("GAME", "MATCH", "WINNER", "MONEYLINE", "1HWINNER")) or series_upper.endswith(("1H", "2H")):
        return "moneyline"

    if any(token in text for token in ("spread", "run line")) or "spread" in event:
        return "spread"
    if any(token in text for token in ("total", "over", "under", "btts")) or "total" in event:
        return "total"
    if any(token in text for token in ("wins", "winner", "to win", "moneyline")) or "match" in event:
        return "moneyline"
    if any(token in title for token in ("yes ", "no ")) and "scored" not in text:
        return "moneyline"
    return ""


def _market_price_cents(market: dict[str, Any], side: str = "yes") -> int:
    side = "no" if str(side or "yes").lower() == "no" else "yes"

    def _value_to_cents(value: Any) -> int | None:
        num = _as_float(value)
        if num is None:
            return None
        if num <= 1.0:
            return max(1, min(99, int(round(num * 100))))
        return max(1, min(99, int(round(num))))

    field_order = [
        f"{side}_ask_dollars",
        f"{side}_bid_dollars",
        f"previous_{side}_ask_dollars",
        f"{side}_ask",
        f"{side}_bid",
    ]
    if side == "yes":
        field_order.extend(["last_price_dollars", "last_price"])

    for field in field_order:
        cents = _value_to_cents(market.get(field))
        if cents is not None:
            return cents

    if side == "no":
        return max(1, min(99, 100 - _market_price_cents(market, "yes")))
    return 50


def _time_match_score(bet: dict[str, Any], market: dict[str, Any]) -> float:
    bet_dt = _bet_start_dt(bet)
    market_dt = _market_time(market)
    if bet_dt is None or market_dt is None:
        return 0.0

    # Date-only bets (midnight UTC) are lenient: many US games start after midnight UTC.
    # Allow same date or adjacent date (e.g., 10:30 PM ET crosses midnight to next UTC day).
    bet_is_date_only = (bet_dt.hour == 0 and bet_dt.minute == 0 and bet_dt.second == 0)
    if bet_is_date_only:
        diff_days = abs((market_dt.date() - bet_dt.date()).days)
        if diff_days == 0:
            return 1.5   # same calendar date
        if diff_days == 1:
            return 0.5   # adjacent date — late-night US game crossing UTC midnight
        return -2.0

    diff_minutes = abs((market_dt - bet_dt).total_seconds()) / 60.0
    if diff_minutes <= 30:
        return 6.0
    if diff_minutes <= 120:
        return 4.5
    if diff_minutes <= 360:
        return 2.5
    if diff_minutes <= 720:
        return 1.0
    if bet_dt.date() == market_dt.date():
        return 0.25
    # Allow adjacent dates for late-night US games
    diff_days = abs((market_dt.date() - bet_dt.date()).days)
    if diff_days == 1 and diff_minutes <= 1500:
        return -0.5
    return -2.0


def _bet_schedule_state(bet: dict[str, Any]) -> dict[str, Any]:
    start_dt = _bet_start_dt(bet)
    if start_dt is None:
        return {"state": "unknown", "scheduled_start": None}

    now = _utc_now()

    # If start_dt has no time component (midnight UTC from a date-only string like "2026-05-10"),
    # treat the entire calendar day as "upcoming" and only mark "done" after the next calendar
    # day plus the sport's typical game duration.  This prevents bets being discarded early in
    # the morning before games have actually started.
    is_date_only = (
        start_dt.hour == 0
        and start_dt.minute == 0
        and start_dt.second == 0
    )

    if is_date_only:
        sport = _bet_sport_tag(bet)
        done_after_hours = _SPORT_DONE_HOURS.get(sport, 3.5)
        # "done" = start of next day UTC + sport duration, so games on "today" are never
        # prematurely closed.
        next_day_midnight = (start_dt + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        done_cutoff = next_day_midnight + datetime.timedelta(hours=done_after_hours)
        if now >= done_cutoff:
            return {
                "state": "done",
                "scheduled_start": start_dt.isoformat().replace("+00:00", "Z"),
                "seconds_since_start": int((now - start_dt).total_seconds()),
            }
        # On the same day or not yet past done_cutoff: leave as upcoming so the resolver
        # continues searching for an open Kalshi market.
        return {
            "state": "upcoming",
            "scheduled_start": start_dt.isoformat().replace("+00:00", "Z"),
            "seconds_to_start": 0,
        }

    if now < start_dt:
        return {
            "state": "upcoming",
            "scheduled_start": start_dt.isoformat().replace("+00:00", "Z"),
            "seconds_to_start": int((start_dt - now).total_seconds()),
        }

    sport = _bet_sport_tag(bet)
    done_after_hours = _SPORT_DONE_HOURS.get(sport, 3.5)
    done_cutoff = start_dt + datetime.timedelta(hours=done_after_hours)
    if now >= done_cutoff:
        return {
            "state": "done",
            "scheduled_start": start_dt.isoformat().replace("+00:00", "Z"),
            "seconds_since_start": int((now - start_dt).total_seconds()),
        }

    return {
        "state": "started",
        "scheduled_start": start_dt.isoformat().replace("+00:00", "Z"),
        "seconds_since_start": int((now - start_dt).total_seconds()),
    }


def _score_single_market(bet: dict[str, Any], market: dict[str, Any]) -> float:
    if _is_combo_market(market):
        return 0.0

    bet_sport = _bet_sport_tag(bet)
    market_sport = _market_sport_tag(market)
    if market_sport == "multi":
        return 0.0
    if bet_sport and market_sport and bet_sport != market_sport:
        return 0.0

    bet_kind = _bet_kind_tag(bet)
    market_kind = _market_kind_tag(market)
    if bet_kind == "player_prop" and market_kind != "player_prop":
        return 0.0
    if bet_kind == "moneyline" and market_kind in {"player_prop", "spread", "total"}:
        return 0.0
    if bet_kind == "spread" and market_kind not in {"spread", ""}:
        return 0.0
    if bet_kind in {"total", "team_total"} and market_kind not in {"total", ""}:
        return 0.0

    text = _market_text(market)
    if not text:
        return 0.0
    series_upper = _series_ticker_from_market(market)

    time_score = _time_match_score(bet, market)
    if time_score < -1.5:
        return 0.0

    if bet_kind == "player_prop":
        player_score = _entity_match_score(text, bet.get("player_name") or bet.get("name"))
        if player_score < 2.0:
            return 0.0
        line_score = _line_match_score(text, bet.get("line"), allow_half_step_integer=True)
        if line_score <= 0.0:
            # Kalshi has discrete prop thresholds (32, 35, 40 pts); use proximity to
            # pick the closest market without requiring exact line equality.
            # Pass direction so the closest direction-aligned threshold wins.
            direction_hint = str(
                bet.get("direction") or bet.get("pick") or bet.get("label") or ""
            )
            line_score = _line_proximity_score(text, bet.get("line"), direction=direction_hint)
        score = player_score * 1.8
        score += max(
            _entity_match_score(text, bet.get("team")),
            _entity_match_score(text, bet.get("home_team")),
            _entity_match_score(text, bet.get("away_team")),
        ) * 0.6
        score += _token_overlap_score(
            text,
            bet.get("prop_type"),
            bet.get("direction"),
            bet.get("label"),
        )
        score += line_score
        score += time_score
        stat_alignment = _series_stat_alignment_score(
            series_hint_text=f"{series_upper} {market.get('event_ticker') or ''}",
            prop_type=bet.get("prop_type"),
            mismatch_penalty=-7.0,
            match_bonus=4.0,
        )
        if stat_alignment <= -6.0:
            return 0.0
        score += stat_alignment
        score += _series_hint_score(
            bet,
            series_upper,
            weight=1.0,
            search_text=f"{market.get('event_ticker') or ''} {text}",
        )
        score += _date_hint_score(bet, market.get("event_ticker"), series_upper, text)
        return score

    pick_score = max(
        _entity_match_score(text, bet.get("team")),
        _entity_match_score(text, bet.get("pick")),
        _entity_match_score(text, bet.get("label")),
    )
    picked_team = _picked_team_name(bet)
    home_score = _entity_match_score(text, bet.get("home_team"))
    away_score = _entity_match_score(text, bet.get("away_team"))
    team_hits = sum(1 for value in (home_score, away_score) if value >= 3.0)
    if max(pick_score, home_score, away_score) < 2.2:
        return 0.0

    score = time_score
    score += pick_score * 1.6
    if team_hits == 2:
        score += home_score + away_score + 4.0
    else:
        score += max(home_score, away_score)
    score += _token_overlap_score(text, bet.get("label"), bet.get("pick"), bet.get("bet_type"))
    score += _series_hint_score(
        bet,
        series_upper,
        weight=0.85,
        search_text=f"{market.get('event_ticker') or ''} {text}",
    )
    score += _date_hint_score(bet, market.get("event_ticker"), series_upper, text)

    if bet_kind == "moneyline" and picked_team:
        selected_side_text = _market_selected_side_text(market)
        selected_score = _entity_match_score(selected_side_text, picked_team)
        home_team = str(bet.get("home_team") or "").strip()
        away_team = str(bet.get("away_team") or "").strip()
        other_team = away_team if picked_team == home_team else home_team if picked_team == away_team else ""
        other_selected_score = _entity_match_score(selected_side_text, other_team) if other_team else 0.0
        if selected_score >= 2.2 and selected_score >= other_selected_score:
            score += 4.0
        elif other_selected_score >= 2.2 and other_selected_score > selected_score:
            return 0.0

    if bet_kind in {"total", "team_total", "spread"}:
        line_score = _line_match_score(text, bet.get("line"))
        if line_score <= 0.0:
            # Use proximity so we pick the closest available Kalshi threshold rather
            # than failing when model line (e.g. 211.5) differs from Kalshi (195.5).
            direction_hint = str(
                bet.get("direction") or bet.get("pick") or bet.get("label") or ""
            )
            line_score = _line_proximity_score(text, bet.get("line"), direction=direction_hint)
        score += line_score
        direction = _norm_text(bet.get("pick") or bet.get("direction") or bet.get("label"))
        if "over" in direction and "over" in text:
            score += 1.5
        if "under" in direction and "under" in text:
            score += 1.5

    if bet_kind == "team_total":
        team_score = _entity_match_score(text, bet.get("team"))
        if team_score < 3.0:
            return 0.0
        score += team_score * 0.9

    return score


def _single_resolution_payload(
    status: str,
    *,
    message: str,
    scheduled_start: str | None = None,
    market: dict[str, Any] | None = None,
    side: str = "yes",
    score: float = 0.0,
    bet_line: float | None = None,
    kalshi_line: float | None = None,
) -> dict[str, Any]:
    payload = {
        "status": status,
        "message": message,
        "scheduled_start": scheduled_start,
        "side": "no" if side == "no" else "yes",
        "score": round(float(score or 0.0), 3),
    }
    if market is not None:
        payload.update(
            {
                "market_ticker": str(market.get("ticker") or ""),
                "market_title": str(
                    market.get("title") or market.get("question") or market.get("ticker") or ""
                ),
                "event_ticker": str(market.get("event_ticker") or ""),
                "series_ticker": _series_ticker_from_market(market),
                "price_cents": _market_price_cents(market, side),
                "close_time": str(market.get("close_time") or market.get("expiration_time") or ""),
            }
        )
        # Annotate when the Kalshi threshold differs from our predicted line
        if kalshi_line is not None and bet_line is not None:
            payload["kalshi_line"] = kalshi_line
            payload["bet_line"] = bet_line
            diff = abs(kalshi_line - bet_line)
            if diff > 0.25:
                payload["line_note"] = (
                    f"Adjusted: your line {_fmt_num(bet_line)} → "
                    f"Kalshi threshold {_fmt_num(kalshi_line)} "
                    f"(±{_fmt_num(diff)})"
                )
            else:
                payload["line_note"] = ""
        else:
            payload["line_note"] = ""
    return payload


def _resolve_single_bet(
    bet: dict[str, Any],
    markets: list[dict[str, Any]],
    event_index: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    schedule = _bet_schedule_state(bet)
    if schedule["state"] == "done":
        return _single_resolution_payload(
            "done",
            message="Game is done.",
            scheduled_start=schedule.get("scheduled_start"),
        )
    if schedule["state"] == "started":
        return _single_resolution_payload(
            "started",
            message="Game already started.",
            scheduled_start=schedule.get("scheduled_start"),
        )

    local_event_index = event_index or _build_event_index(markets)
    bet_kind = _bet_kind_tag(bet)
    sport_tag = _bet_sport_tag(bet)
    min_event_score = 5.4 if bet_kind == "player_prop" else 6.0
    min_market_score = 5.9 if bet_kind == "player_prop" else 6.6

    # Allow env-based tuning without code changes in production.
    if bet_kind == "player_prop":
        min_event_score = max(3.8, float(os.getenv("KALSHI_MIN_EVENT_SCORE_PROP", str(min_event_score)) or min_event_score))
        min_market_score = max(4.0, float(os.getenv("KALSHI_MIN_MARKET_SCORE_PROP", str(min_market_score)) or min_market_score))
    else:
        min_event_score = max(4.2, float(os.getenv("KALSHI_MIN_EVENT_SCORE", str(min_event_score)) or min_event_score))
        min_market_score = max(4.4, float(os.getenv("KALSHI_MIN_MARKET_SCORE", str(min_market_score)) or min_market_score))
    if sport_tag in {"soccer", "tennis", "boxing", "mma", "golf", "motorsports", "cricket"}:
        min_event_score = max(3.8, min_event_score - 0.85)
        min_market_score = max(4.2, min_market_score - 0.95)
    scored_events: list[tuple[float, dict[str, Any]]] = []
    for event_group in local_event_index.values():
        score = _score_event_group(bet, event_group)
        if score > 0.0:
            scored_events.append((score, event_group))
    scored_events.sort(key=lambda item: item[0], reverse=True)

    if scored_events:
        preferred_series, avoid_series = _bet_series_hints(bet)
        if preferred_series:
            pref_hits = [
                item
                for item in scored_events
                if any(
                    pref in str(item[1].get("event_ticker") or "").upper()
                    or pref in str(item[1].get("series_ticker") or "").upper()
                    for pref in preferred_series
                )
            ]
            if pref_hits:
                scored_events = pref_hits
        if avoid_series and len(scored_events) > 1:
            filtered_events = [
                item
                for item in scored_events
                if not any(
                    bad in str(item[1].get("event_ticker") or "").upper()
                    or bad in str(item[1].get("series_ticker") or "").upper()
                    for bad in avoid_series
                )
            ]
            if filtered_events:
                scored_events = filtered_events

    if not scored_events or scored_events[0][0] < min_event_score:
        return _single_resolution_payload(
            "unavailable",
            message="No exact Kalshi event matches this prediction.",
            scheduled_start=schedule.get("scheduled_start"),
        )

    if len(scored_events) > 1:
        top_event_score = scored_events[0][0]
        next_event_score = scored_events[1][0]
        default_event_gap = 0.55 if bet_kind == "moneyline" else 0.75
        if sport_tag in {"soccer", "tennis", "boxing", "mma", "golf", "motorsports", "cricket"}:
            default_event_gap = max(0.3, default_event_gap - 0.12)
        event_ambiguity_gap = max(
            0.2,
            float(os.getenv("KALSHI_EVENT_AMBIGUITY_GAP", str(default_event_gap)) or default_event_gap),
        )
        if next_event_score >= min_event_score and (top_event_score - next_event_score) < event_ambiguity_gap:
            return _single_resolution_payload(
                "unavailable",
                message="Kalshi event match is ambiguous; refusing to guess a ticker.",
                scheduled_start=schedule.get("scheduled_start"),
            )

    candidate_markets = list(scored_events[0][1].get("markets") or [])
    best_market: dict[str, Any] | None = None
    best_score = 0.0
    second_best_score = 0.0
    for market in candidate_markets:
        score = _score_single_market(bet, market)
        if score > best_score:
            second_best_score = best_score
            best_market = market
            best_score = score
        elif score > second_best_score:
            second_best_score = score

    if best_market is None or best_score < min_market_score:
        # Relaxed fallback: use the best market within the best event when signal is
        # still reasonably strong, to improve actionable coverage for near-threshold lines.
        if scored_events and scored_events[0][0] >= 4.8:
            relaxed_best = None
            relaxed_score = 0.0
            for market in candidate_markets:
                score = _score_single_market(bet, market)
                if score > relaxed_score:
                    relaxed_score = score
                    relaxed_best = market
            if relaxed_best is not None and relaxed_score >= 5.2:
                side = _resolve_market_side(bet, relaxed_best)
                bet_line_val = _as_float(bet.get("line"))
                market_text_for_line = _market_text(relaxed_best)
                kalshi_line_val: float | None = None
                if bet_line_val is not None:
                    kalshi_line_val = _closest_num_in_text(market_text_for_line, bet_line_val)
                return _single_resolution_payload(
                    "matched",
                    message="Matched to closest open Kalshi market.",
                    scheduled_start=schedule.get("scheduled_start"),
                    market=relaxed_best,
                    side=side,
                    score=relaxed_score,
                    bet_line=bet_line_val,
                    kalshi_line=kalshi_line_val,
                )
        return _single_resolution_payload(
            "unavailable",
            message="No exact Kalshi market is open for this bet.",
            scheduled_start=schedule.get("scheduled_start"),
        )

    market_ambiguity_gap = max(
        0.2,
        float(os.getenv("KALSHI_MARKET_AMBIGUITY_GAP", "0.5") or "0.5"),
    )
    if sport_tag in {"soccer", "tennis", "boxing", "mma", "golf", "motorsports", "cricket"}:
        market_ambiguity_gap = max(0.25, market_ambiguity_gap - 0.1)
    if second_best_score >= (min_market_score + 1.0) and (best_score - second_best_score) < market_ambiguity_gap:
        return _single_resolution_payload(
            "unavailable",
            message="Kalshi market match is ambiguous within the event; refusing to guess.",
            scheduled_start=schedule.get("scheduled_start"),
        )

    side = _resolve_market_side(bet, best_market)
    # Extract the actual Kalshi threshold from the matched market's text so we can
    # annotate "Adjusted: your line X → Kalshi threshold Y" in the UI.
    bet_line_val = _as_float(bet.get("line"))
    market_text_for_line = _market_text(best_market)
    kalshi_line_val: float | None = None
    if bet_line_val is not None:
        kalshi_line_val = _closest_num_in_text(market_text_for_line, bet_line_val)
    return _single_resolution_payload(
        "matched",
        message="Matched to an open Kalshi market.",
        scheduled_start=schedule.get("scheduled_start"),
        market=best_market,
        side=side,
        score=best_score,
        bet_line=bet_line_val,
        kalshi_line=kalshi_line_val,
    )


def _resolve_combo_bet(
    bet: dict[str, Any],
    markets: list[dict[str, Any]],
    combo_markets: list[dict[str, Any]],
    single_cache: dict[str, dict[str, Any]],
    event_index: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    legs = [
        _normalize_ready_bet(leg)
        for leg in (bet.get("legs") or [])
        if isinstance(leg, dict)
    ]
    legs = [leg for leg in legs if leg]
    if len(legs) < 2:
        return _single_resolution_payload("unavailable", message="Combo needs at least 2 legs.")

    # Reject duplicated legs in a combo at backend level.
    seen_leg_sigs: set[str] = set()
    for leg in legs:
        sig = _bet_signature(leg)
        if sig in seen_leg_sigs:
            return {
                "status": "unavailable",
                "message": "Combo blocked: duplicate legs detected.",
                "legs": [],
            }
        seen_leg_sigs.add(sig)

    resolved_legs: list[dict[str, Any]] = []
    for leg in legs:
        sig = _bet_signature(leg)
        if sig not in single_cache:
            single_cache[sig] = _resolve_single_bet(leg, markets, event_index)
        leg_result = single_cache[sig]
        resolved_legs.append(leg_result)
        if leg_result.get("status") in {"done", "started"}:
            return {
                "status": leg_result["status"],
                "message": f"Combo blocked: {leg_result.get('message', 'A leg already started.')}",
                "legs": resolved_legs,
            }
        if leg_result.get("status") != "matched":
            return {
                "status": "unavailable",
                "message": "Combo blocked: at least one leg has no exact open Kalshi market.",
                "legs": resolved_legs,
            }

    target_legs = Counter(
        (
            str(leg.get("market_ticker") or ""),
            str(leg.get("side") or "yes").lower(),
        )
        for leg in resolved_legs
    )

    exact_matches: list[dict[str, Any]] = []
    for market in combo_markets:
        selected = market.get("mve_selected_legs") or []
        market_legs = Counter(
            (
                str(leg.get("market_ticker") or ""),
                str(leg.get("side") or "yes").lower(),
            )
            for leg in selected
            if isinstance(leg, dict)
        )
        if market_legs and market_legs == target_legs:
            exact_matches.append(market)

    if not exact_matches:
        return {
            "status": "unavailable",
            "message": "No exact Kalshi combo market exists for these legs.",
            "legs": resolved_legs,
        }

    exact_matches.sort(
        key=lambda market: (
            _time_match_score(legs[0], market),
            _as_float(market.get("liquidity_dollars")) or 0.0,
        ),
        reverse=True,
    )
    matched_market = exact_matches[0]
    return {
        **_single_resolution_payload(
            "matched",
            message="Matched to an exact Kalshi combo market.",
            market=matched_market,
            side="yes",
            score=len(target_legs),
        ),
        "legs": resolved_legs,
    }


def _fetch_open_markets_for_series(series: str, sport_hint: str = "") -> tuple[str, list[dict[str, Any]], str]:
    rows_out: list[dict[str, Any]] = []
    cursor: str | None = None
    pages = 0
    while True:
        pages += 1
        try:
            data = list_markets(
                limit=1000,
                cursor=cursor,
                status="open",
                series_ticker=series,
                mve_filter="exclude",
            )
        except Exception:
            break
        rows = data.get("markets") or []
        for row in rows:
            if not isinstance(row, dict):
                continue
            normalized_row = dict(row)
            normalized_row["series_ticker"] = str(
                normalized_row.get("series_ticker") or series
            ).upper()
            detected_sport = sport_hint or _market_sport_tag(normalized_row)
            if detected_sport:
                normalized_row["sport"] = str(normalized_row.get("sport") or detected_sport)
            rows_out.append(normalized_row)
        cursor = data.get("cursor")
        if not cursor or pages >= 20:
            break
    return series, rows_out, (sport_hint or _detect_sport_from_series(series))


def _fetch_open_combo_markets() -> list[dict[str, Any]]:
    rows_out: list[dict[str, Any]] = []
    seen_tickers: set[str] = set()
    cursor: str | None = None
    pages = 0
    while True:
        pages += 1
        try:
            data = list_markets(limit=1000, cursor=cursor, status="open", mve_filter="only")
        except Exception:
            break
        rows = data.get("markets") or []
        for row in rows:
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker") or "")
            if ticker and ticker in seen_tickers:
                continue
            if ticker:
                seen_tickers.add(ticker)
            rows_out.append(dict(row))
        cursor = data.get("cursor")
        if not cursor or pages >= _KALSHI_COMBO_MARKET_PAGES:
            break
    return rows_out


def get_open_market_catalog(
    *,
    force_refresh: bool = False,
    include_combo_markets: bool = False,
) -> dict[str, Any]:
    now = time.time()
    cached_markets: list[dict[str, Any]] = []
    cached_combo_markets: list[dict[str, Any]] = []
    markets_cache_fresh = False
    combo_cache_fresh = False
    with _KALSHI_MARKET_CACHE_LOCK:
        age = now - float(_KALSHI_MARKET_CACHE.get("ts") or 0.0)
        cached_markets = list(_KALSHI_MARKET_CACHE.get("markets") or [])
        cached_combo_markets = list(_KALSHI_MARKET_CACHE.get("combo_markets") or [])
        markets_cache_fresh = bool(
            not force_refresh
            and cached_markets
            and age < _KALSHI_MARKET_CACHE_TTL_SEC
        )
        combo_cache_fresh = bool(
            not force_refresh
            and cached_combo_markets
            and age < _KALSHI_MARKET_CACHE_TTL_SEC
        )
        if markets_cache_fresh and (not include_combo_markets or combo_cache_fresh):
            combo_rows = list(cached_combo_markets) if include_combo_markets else []
            return {
                "markets": list(cached_markets),
                "combo_markets": combo_rows,
                "count": len(cached_markets) + len(combo_rows),
                "cache_age_sec": age,
            }

    # Fetch markets from known sports series directly — the general /markets endpoint
    # returns non-sports (politics/crypto/economics) first and the 25k cap is reached
    # before any NBA/MLB markets appear.  Targeting specific series tickers is fast and precise.
    seen_tickers: set[str] = {
        str(market.get("ticker") or "")
        for market in (cached_markets if markets_cache_fresh else [])
        if isinstance(market, dict) and str(market.get("ticker") or "")
    }
    markets: list[dict[str, Any]] = list(cached_markets) if markets_cache_fresh else []

    # Merge hardcoded series with anything previously confirmed in the persistent registry
    # plus dynamically discovered actionable sports series from Kalshi's public /series feed.
    registry = {
        ticker: sport
        for ticker, sport in _load_series_registry().items()
        if _is_actionable_series_ticker(ticker)
    }
    discovered_series = {
        ticker: sport
        for ticker, sport in _discover_actionable_sports_series(force_refresh=not registry).items()
        if _is_actionable_series_ticker(ticker)
    }
    series_to_fetch = list(
        dict.fromkeys(
            list(_SPORTS_SERIES_TO_FETCH)
            + list(discovered_series.keys())
            + list(registry.keys())
        )
    )
    newly_confirmed: dict[str, str] = {}

    series_hints = {
        **{series: _SERIES_SPORT_MAP.get(series, "") for series in series_to_fetch},
        **registry,
        **discovered_series,
    }
    if not markets:
        max_workers = min(max(1, _KALSHI_SERIES_FETCH_WORKERS), max(1, len(series_to_fetch)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(
                    _fetch_open_markets_for_series,
                    series,
                    series_hints.get(series) or _detect_sport_from_series(series),
                ): series
                for series in series_to_fetch
            }
            for future in as_completed(future_map):
                series = future_map[future]
                try:
                    _, fetched_rows, sport_hint = future.result()
                except Exception:
                    continue
                series_count = 0
                for normalized_row in fetched_rows:
                    ticker = str(normalized_row.get("ticker") or "")
                    if ticker and ticker not in seen_tickers:
                        seen_tickers.add(ticker)
                        markets.append(normalized_row)
                        series_count += 1
                if series_count > 0:
                    sport = sport_hint or _detect_sport_from_series(series)
                    if sport:
                        newly_confirmed[series] = sport

    # If targeted series fetch returns nothing (transient API issues),
    # fall back to broad sports scan to avoid "0 markets scanned" responses.
    if not markets:
        cursor = None
        scan_pages = max(2, min(30, int(os.getenv("KALSHI_FALLBACK_SCAN_PAGES", "12") or "12")))
        pages = 0
        while pages < scan_pages:
            pages += 1
            try:
                data = list_markets(limit=500, cursor=cursor, status="open")
            except Exception:
                break
            rows = data.get("markets") or []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                normalized_row = dict(row)
                normalized_row["series_ticker"] = _series_ticker_from_market(normalized_row)
                sport = _market_sport_tag(normalized_row)
                if not sport or sport == "multi":
                    continue
                normalized_row["sport"] = sport
                ticker = str(normalized_row.get("ticker") or "")
                if ticker and ticker not in seen_tickers:
                    seen_tickers.add(ticker)
                    markets.append(normalized_row)
            cursor = data.get("cursor")
            if not cursor:
                break

    # Persist any newly confirmed or registry-missing series
    if newly_confirmed:
        _update_series_registry(newly_confirmed)

    # If fetch still failed but we have a previous non-empty cache, return stale cache
    # instead of dropping to zero markets.
    if not markets and cached_markets:
        stale_age = now - float(_KALSHI_MARKET_CACHE.get("ts") or 0.0)
        combo_rows = list(cached_combo_markets) if include_combo_markets else []
        return {
            "markets": list(cached_markets),
            "combo_markets": combo_rows,
            "count": len(cached_markets) + len(combo_rows),
            "cache_age_sec": max(0.0, stale_age),
        }

    combo_markets = list(cached_combo_markets) if (include_combo_markets and combo_cache_fresh) else []
    if include_combo_markets and not combo_markets:
        combo_markets = _fetch_open_combo_markets()

    if markets:
        with _KALSHI_MARKET_CACHE_LOCK:
            updated_cache = {
                "ts": time.time(),
                "markets": markets,
                "count": len(markets) + len(combo_markets),
            }
            if include_combo_markets:
                updated_cache["combo_markets"] = combo_markets
            _KALSHI_MARKET_CACHE.update(updated_cache)

    return {
        "markets": list(markets),
        "combo_markets": list(combo_markets) if include_combo_markets else [],
        "count": len(markets) + len(combo_markets),
        "cache_age_sec": 0.0,
    }


def resolve_ready_bets(
    bets: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    force_refresh: bool = False,
) -> dict[str, Any]:
    global _RESOLUTION_CACHE, _RESOLUTION_CACHE_CATALOG_TS

    needs_combo_markets = any(
        isinstance(bet, dict) and _bet_kind_tag(bet) == "combo"
        for bet in (bets or [])
    )
    catalog = get_open_market_catalog(
        force_refresh=force_refresh,
        include_combo_markets=needs_combo_markets,
    )
    if not int(catalog.get("count") or 0) and not force_refresh:
        # Retry once with hard refresh before accepting a zero-market catalog.
        catalog = get_open_market_catalog(
            force_refresh=True,
            include_combo_markets=needs_combo_markets,
        )
    catalog_ts = float(_KALSHI_MARKET_CACHE.get("ts") or 0.0)

    # Invalidate resolution cache whenever the market catalog has been refreshed
    if catalog_ts != _RESOLUTION_CACHE_CATALOG_TS or force_refresh:
        _RESOLUTION_CACHE = {}
        _RESOLUTION_CACHE_CATALOG_TS = catalog_ts

    markets = [market for market in catalog["markets"] if not _is_combo_market(market)]
    combo_markets = list(catalog["combo_markets"])
    event_index = _build_event_index(markets)

    resolutions: dict[str, dict[str, Any]] = {}
    summary = {
        "matched": 0,
        "started": 0,
        "done": 0,
        "unavailable": 0,
        "count": 0,
        "market_count": int(catalog.get("count") or 0),
    }
    single_cache: dict[str, dict[str, Any]] = {}


    # Deduplicate singles/props and combos by signature/uid before resolving
    seen_singles = set()
    seen_combos = set()
    deduped_bets = []
    for index, bet in enumerate(bets or []):
        if not isinstance(bet, dict):
            continue
        normalized_bet = _normalize_ready_bet(bet)
        if not normalized_bet:
            continue
        kind = _bet_kind_tag(normalized_bet)
        if kind == "combo":
            uid = str(normalized_bet.get("uid") or normalized_bet.get("bet_uid") or normalized_bet.get("prediction_uid") or f"combo_{index}")
            if uid in seen_combos:
                continue
            seen_combos.add(uid)
            deduped_bets.append((index, normalized_bet, uid, kind))
        else:
            sig = _bet_signature(normalized_bet)
            if sig in seen_singles:
                continue
            seen_singles.add(sig)
            deduped_bets.append((index, normalized_bet, sig, kind))

    for index, normalized_bet, unique_id, kind in deduped_bets:
        uid = _bet_identity(normalized_bet, index)
        if kind == "combo":
            result = _resolve_combo_bet(normalized_bet, markets, combo_markets, single_cache, event_index)
        else:
            sig = _bet_signature(normalized_bet)
            if sig not in single_cache:
                if sig in _RESOLUTION_CACHE:
                    single_cache[sig] = _RESOLUTION_CACHE[sig]
                else:
                    single_cache[sig] = _resolve_single_bet(normalized_bet, markets, event_index)
                    _RESOLUTION_CACHE[sig] = single_cache[sig]
            result = dict(single_cache[sig])
        resolutions[uid] = result
        summary["count"] += 1
        status = str(result.get("status") or "unavailable")
        if status not in summary:
            status = "unavailable"
        summary[status] += 1

    return {
        "resolutions": resolutions,
        "summary": summary,
        "market_count": int(catalog.get("count") or 0),
        "cache_age_sec": float(catalog.get("cache_age_sec") or 0.0),
    }


def suggest_combo_bets(
    bets: list[dict[str, Any]],
    *,
    resolutions: dict[str, dict[str, Any]] | None = None,
    max_legs: int = 4,
    min_legs: int = 2,
    min_combined_prob: float = 0.20,
    min_ev: float = -0.10,
    max_combos: int = 30,
) -> list[dict[str, Any]]:
    """Analyze a set of ready bets and suggest the best multi-leg combo parlays.

    Args:
        bets:              List of ready-bet dicts (same format sent to resolve_ready_bets).
        resolutions:       Optional pre-computed Kalshi resolution map {uid: resolution}.
        max_legs:          Maximum legs per combo (default 4).
        min_legs:          Minimum legs (default 2).
        min_combined_prob: Minimum parlay hit probability to include (default 20%).
        min_ev:            Minimum expected value (default -10% — allows slightly -EV to learn).
        max_combos:        Cap on returned suggestions (sorted by EV descending).

    Returns:
        List of combo suggestion dicts, each containing:
          - legs: list of individual bet dicts with their Kalshi ticker if matched
          - combined_prob: float (product of individual probs)
          - combined_dec_odds: float (product of individual decimal odds)
          - ev: float (combined_prob * combined_dec_odds - 1)
          - quality: float 0-1
          - label: str
          - all_matched: bool (all legs have a Kalshi market ticker)
    """
    if not bets:
        return []

    resolutions = resolutions or {}

    # Filter to bets with a meaningful probability
    candidates: list[dict[str, Any]] = []
    for i, bet in enumerate(bets):
        if not isinstance(bet, dict):
            continue
        uid = str(bet.get("bet_uid") or bet.get("uid") or bet.get("prediction_uid") or f"bet_{i}")
        prob = float(bet.get("model_prob") or bet.get("probability") or 0.0)
        dec = float(bet.get("dec_odds") or bet.get("decimal_odds") or 1.9)
        if prob < 0.50 or dec < 1.01:
            continue
        # Single-bet EV gate
        single_ev = prob * dec - 1.0
        if single_ev < -0.15:
            continue
        res = resolutions.get(uid) or {}
        matched_ticker = str(res.get("market_ticker") or "") if str(res.get("status") or "") == "matched" else ""
        candidates.append({
            **bet,
            "uid": uid,
            "model_prob": prob,
            "dec_odds": dec,
            "single_ev": single_ev,
            "_matched_ticker": matched_ticker,
        })

    if len(candidates) < min_legs:
        return []

    # Hard cap candidate pool to keep combo search bounded under heavy ready-bet payloads.
    # Without this, combinations(N, k) can explode and fail the whole resolve path.
    max_candidates = max(8, int(os.getenv("KALSHI_COMBO_MAX_CANDIDATES", "26") or "26"))
    if len(candidates) > max_candidates:
        candidates.sort(
            key=lambda c: (
                not bool(c.get("_matched_ticker")),
                -(float(c.get("single_ev") or 0.0)),
                -(float(c.get("model_prob") or 0.0)),
            )
        )
        candidates = candidates[:max_candidates]

    combos: list[dict[str, Any]] = []
    for n_legs in range(min_legs, min(max_legs + 1, len(candidates) + 1)):
        for leg_set in combinations(candidates, n_legs):
            # Prefer diversification: penalise same-game combos (not illegal, just lower value signal)
            game_keys = [str(l.get("game") or l.get("game_key") or "") for l in leg_set]
            distinct_games = len(set(g for g in game_keys if g))

            # Precision guard: avoid duplicate player-prop legs on same player/stat/game.
            prop_leg_keys = [
                (
                    _norm_text(l.get("player_name") or l.get("name") or ""),
                    _norm_text(l.get("prop_type") or l.get("bet_type") or ""),
                    _norm_text(l.get("game") or l.get("game_key") or ""),
                    _norm_text(l.get("direction") or l.get("pick") or ""),
                )
                for l in leg_set
                if _bet_kind_tag(l) == "player_prop"
            ]
            if len(prop_leg_keys) != len(set(prop_leg_keys)):
                continue

            prob = 1.0
            dec = 1.0
            for leg in leg_set:
                prob *= leg["model_prob"]
                dec *= leg["dec_odds"]

            if prob < min_combined_prob:
                continue

            ev = prob * dec - 1.0
            if ev < min_ev:
                continue

            # Quality score: weigh EV + prob + diversity
            diversity_bonus = (distinct_games / max(n_legs, 1)) * 0.1
            quality = min(1.0, max(0.0, (0.4 * prob + 0.4 * max(0.0, ev) + 0.2 * (n_legs / max_legs)) + diversity_bonus))

            all_matched = all(bool(l.get("_matched_ticker")) for l in leg_set)
            tickers = [l["_matched_ticker"] for l in leg_set if l.get("_matched_ticker")]

            # Precision guard: if all legs are matched, market tickers should be unique.
            if all_matched and len(set(tickers)) != len(tickers):
                continue

            combo = {
                "uid": "combo_" + "_".join(l["uid"][:8] for l in leg_set),
                "kind": "combo",
                "label": f"{n_legs}-Leg Combo {'✓' if all_matched else '~'}",
                "n_legs": n_legs,
                "legs": [
                    {
                        "uid": l["uid"],
                        "label": l.get("label") or l.get("pick") or l.get("uid"),
                        "sport": l.get("sport") or "",
                        "game": l.get("game") or l.get("game_key") or "",
                        "game_date": l.get("game_date") or "",
                        "player_name": l.get("player_name") or "",
                        "prop_type": l.get("prop_type") or l.get("bet_type") or "",
                        "model_prob": l["model_prob"],
                        "dec_odds": l["dec_odds"],
                        "ev": l["single_ev"],
                        "kalshi_ticker": l.get("_matched_ticker") or "",
                    }
                    for l in leg_set
                ],
                "combined_prob": round(prob, 4),
                "combined_dec_odds": round(dec, 3),
                "ev": round(ev, 4),
                "quality": round(quality, 3),
                "all_matched": all_matched,
                "matched_tickers": tickers,
                "distinct_games": distinct_games,
            }
            combos.append(combo)

    # Sort: all-matched first, then by EV descending
    combos.sort(key=lambda c: (not c["all_matched"], -c["ev"]))
    return combos[:max_combos]


def get_today_kalshi_tickers() -> dict[str, Any]:
    """Reverse-match approach: fetch all open sports markets available today/upcoming,
    grouped by sport. Use to discover what Kalshi has available today before matching to games."""
    catalog = get_open_market_catalog()
    markets = [m for m in catalog["markets"] if not _is_combo_market(m)]

    today_utc = _utc_now().date()
    result: dict[str, list[dict[str, Any]]] = {}

    for market in markets:
        sport = _market_sport_tag(market)
        if not sport or sport == "multi":
            continue
        # Only include upcoming / today markets
        close_dt = _market_time(market)
        if close_dt is not None and close_dt.date() < today_utc:
            continue  # already expired

        entry = {
            "ticker": market.get("ticker"),
            "event_ticker": str(market.get("event_ticker") or ""),
            "title": str(market.get("title") or market.get("question") or ""),
            "close_time": str(market.get("close_time") or ""),
            "yes_price": _market_price_cents(market, "yes"),
            "no_price": _market_price_cents(market, "no"),
        }
        result.setdefault(sport, []).append(entry)

    return {
        "sports": result,
        "total": sum(len(v) for v in result.values()),
        "date": today_utc.isoformat(),
        "market_count": catalog.get("count", 0),
    }


def attach_kalshi_to_bets(
    bets: list[dict[str, Any]],
    *,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    """
    Enrich each bet dict with the best-matching Kalshi market info.

    Every prediction the bot generates should have a Kalshi ticker so it can
    be placed directly.  This function resolves the full catalog once (cached
    for 10 min), then attaches per-bet:

        kalshi_ticker        str — matched market ticker
        kalshi_event_ticker  str — parent event ticker
        kalshi_side          str — "yes" | "no"
        kalshi_price_cents   int — ask price in cents (0-100) for the chosen side
        kalshi_status        str — "matched" | "unavailable" | "started" | "done"

    Returns a new list of dicts; originals are never mutated.
    """
    if not bets:
        return bets
    try:
        result       = resolve_ready_bets(list(bets), force_refresh=force_refresh)
        resolutions: dict[str, dict[str, Any]] = result.get("resolutions") or {}
        enriched: list[dict[str, Any]] = []
        for i, bet in enumerate(bets):
            if not isinstance(bet, dict):
                enriched.append(bet)
                continue
            # resolve_ready_bets keys resolutions by _bet_identity(normalized_bet, idx)
            normalized = _normalize_ready_bet(bet) or {}
            uid = _bet_identity(normalized, i) if normalized else _bet_identity(bet, i)
            res = resolutions.get(uid) or {}
            eb  = dict(bet)
            eb["kalshi_ticker"]       = str(res.get("market_ticker") or "")
            eb["kalshi_event_ticker"] = str(res.get("event_ticker")  or "")
            eb["kalshi_series_ticker"] = str(res.get("series_ticker") or "")
            eb["kalshi_side"]         = str(res.get("side")          or "")
            eb["kalshi_price_cents"]  = int(res.get("price_cents")   or 0)
            eb["kalshi_status"]       = str(res.get("status")        or "unavailable")
            enriched.append(eb)
        return enriched
    except Exception as exc:
        print(f"[kalshi] attach_kalshi_to_bets error: {exc}")
        return [dict(b) if isinstance(b, dict) else b for b in bets]


def get_balance() -> dict[str, Any]:
    """Get the authenticated user's Kalshi portfolio balance."""
    data = _request_json("GET", "/portfolio/balance", auth=True)
    return data


def place_order(order_payload: dict[str, Any]) -> dict[str, Any]:
    """Place a Kalshi order using RSA-signed authentication.

    Tries the primary portfolio/orders endpoint, falls back to /orders.
    """
    last_error: Exception | None = None

    for path in ("/portfolio/orders", "/orders"):
        try:
            return _request_json("POST", path, payload=order_payload, auth=True)
        except Exception as exc:
            last_error = exc
            if "(404)" in str(exc):
                continue
            raise

    if last_error:
        raise last_error
    raise RuntimeError("Kalshi order placement failed")


def get_order(order_id: str) -> dict[str, Any]:
    """Fetch a single authenticated Kalshi order by ID."""
    clean_order_id = str(order_id or "").strip()
    if not clean_order_id:
        raise RuntimeError("order_id is required")

    last_error: Exception | None = None
    for path in (f"/portfolio/orders/{clean_order_id}", f"/orders/{clean_order_id}"):
        try:
            return _request_json("GET", path, auth=True)
        except Exception as exc:
            last_error = exc
            if "(404)" in str(exc):
                continue
            raise

    if last_error:
        raise last_error
    raise RuntimeError("Kalshi order lookup failed")


# ─── Kalshi WebSocket Manager ─────────────────────────────────────────────────
# Maintains a single persistent WebSocket connection to Kalshi.
# Subscribes to the 'ticker' channel so every open-market price update arrives
# in real time.  The `websockets` library automatically responds to the 10 s
# Kalshi ping (0x9 / body='heartbeat') with a pong, so no manual keep-alive is
# required.  The manager runs its asyncio event-loop in a daemon thread so it
# doesn't interfere with Flask's thread-based server.
# ---------------------------------------------------------------------------

import asyncio as _asyncio

KALSHI_WS_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
_KALSHI_WS_SIGN_PATH = "/trade-api/ws/v2"


class KalshiWebSocketManager:
    """Persistent WebSocket client for the Kalshi ticker feed.

    Usage::

        mgr = KalshiWebSocketManager()
        mgr.start()
        # later …
        tickers = mgr.get_tickers()   # {market_ticker: {yes_bid, yes_ask, …}}
    """

    def __init__(self, url: str = KALSHI_WS_URL) -> None:
        self._url = url
        self._loop: "_asyncio.AbstractEventLoop | None" = None
        self._thread: "threading.Thread | None" = None
        self._running = False
        self._connected = False
        self._ticker_cache: "dict[str, dict]" = {}
        self._lock = threading.Lock()
        self._msg_id = 0
        # Callbacks called with (market_ticker: str, data: dict) on every update
        self._on_update: "list" = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the WebSocket manager in a background daemon thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            name="kalshi-ws",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Signal the manager to shut down."""
        self._running = False
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)

    def get_tickers(self) -> "dict[str, dict]":
        """Thread-safe snapshot of the current ticker cache."""
        with self._lock:
            return dict(self._ticker_cache)

    def is_connected(self) -> bool:
        return self._connected

    def add_update_callback(self, fn: "callable") -> None:
        """Register fn(market_ticker, data) to be called on every ticker msg."""
        self._on_update.append(fn)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        self._loop = _asyncio.new_event_loop()
        _asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._connect_loop())
        except Exception as exc:
            print(f"[kalshi-ws] Event-loop exited: {exc}")
        finally:
            self._loop.close()

    async def _connect_loop(self) -> None:
        """Outer reconnect loop with exponential back-off."""
        delay = 3.0
        while self._running:
            try:
                await self._connect_once()
                delay = 3.0  # reset after clean disconnect
            except Exception as exc:
                print(f"[kalshi-ws] Connection error: {exc} — retry in {delay:.0f}s")
                self._connected = False
                if self._running:
                    await _asyncio.sleep(delay)
                delay = min(delay * 1.5, 60.0)

    async def _connect_once(self) -> None:
        """Open one WebSocket session and read until close/error."""
        try:
            import websockets  # type: ignore[import]
        except ImportError:
            print("[kalshi-ws] 'websockets' package not installed — stopping")
            self._running = False
            return

        # Build signed auth headers (fresh timestamp for each connection)
        try:
            headers = _auth_headers("GET", _KALSHI_WS_SIGN_PATH)
        except Exception as exc:
            print(f"[kalshi-ws] Auth failed (no credentials?): {exc}")
            self._running = False
            return

        print("[kalshi-ws] Connecting …")
        async with websockets.connect(
            self._url,
            additional_headers=headers,
            ping_interval=20,   # library sends WS-level pings every 20 s
            ping_timeout=30,    # wait 30 s for pong before treating as dead
            close_timeout=10,
        ) as ws:
            self._connected = True
            print("[kalshi-ws] Connected — subscribing to ticker channel")

            self._msg_id += 1
            await ws.send(json.dumps({
                "id": self._msg_id,
                "cmd": "subscribe",
                "params": {"channels": ["ticker"]},
            }))

            async for raw in ws:
                if not self._running:
                    break
                try:
                    self._handle_msg(json.loads(raw))
                except Exception:
                    pass

        self._connected = False
        print("[kalshi-ws] Disconnected")

    def _handle_msg(self, msg: dict) -> None:
        msg_type = msg.get("type", "")
        data = msg.get("msg", {})

        if msg_type == "ticker":
            ticker = data.get("market_ticker", "")
            if ticker:
                with self._lock:
                    self._ticker_cache[ticker] = data
                for fn in self._on_update:
                    try:
                        fn(ticker, data)
                    except Exception:
                        pass

        elif msg_type == "subscribed":
            sid = msg.get("sid")
            channel = data.get("channel")
            print(f"[kalshi-ws] Subscribed sid={sid} channel={channel}")

        elif msg_type == "error":
            print(f"[kalshi-ws] Server error {data.get('code')}: {data.get('msg')}")


# ------------------------------------------------------------------
# Module-level singleton helpers
# ------------------------------------------------------------------

_kalshi_ws_manager: "KalshiWebSocketManager | None" = None


def start_kalshi_ws(url: str = KALSHI_WS_URL) -> KalshiWebSocketManager:
    """Ensure the singleton WebSocket manager is started and return it.

    Safe to call multiple times — only starts once.
    If the Kalshi credentials are absent the manager will detect this on
    first connect and stop itself gracefully.
    """
    global _kalshi_ws_manager
    if _kalshi_ws_manager is None:
        _kalshi_ws_manager = KalshiWebSocketManager(url=url)
    if not _kalshi_ws_manager._running:
        _kalshi_ws_manager.start()
    return _kalshi_ws_manager


def get_kalshi_ws_manager() -> "KalshiWebSocketManager | None":
    """Return the singleton manager (may be None if never started)."""
    return _kalshi_ws_manager


def get_live_tickers() -> "dict[str, dict]":
    """Return a snapshot of the WebSocket ticker cache.

    Returns an empty dict if the WS manager hasn't started or has no data yet.
    The REST ``/api/kalshi/today-tickers`` endpoint falls back to REST when this
    is empty so there is no user-visible disruption during cold-start.
    """
    mgr = _kalshi_ws_manager
    if mgr is None:
        return {}
    return mgr.get_tickers()

