"""Polymarket market matching and execution helpers.

This module resolves normalized ready-bet rows to open Polymarket markets using
the public Gamma API and can optionally place orders through the CLOB client
when credentials are configured.
"""

from __future__ import annotations

import datetime
import json
import os
import re
import threading
import time
import urllib.parse
from typing import Any

from cryptography.hazmat.primitives.asymmetric import ed25519
import requests

POLYMARKET_BASE_URL = os.getenv("POLYMARKET_BASE_URL", "https://gamma-api.polymarket.com").rstrip("/")
POLYMARKET_US_API_BASE = os.getenv("POLYMARKET_US_API_BASE", "https://api.polymarket.us").rstrip("/")
POLYMARKET_GATEWAY_BASE = os.getenv("POLYMARKET_GATEWAY_BASE", "https://gateway.polymarket.us").rstrip("/")
POLYMARKET_CLOB_HOST = os.getenv("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com").rstrip("/")
POLYMARKET_CHAIN_ID = int(os.getenv("POLYMARKET_CHAIN_ID", "137") or "137")
POLYMARKET_SIGNATURE_TYPE = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "1") or "1")
POLYMARKET_PRIVATE_KEY = str(os.getenv("POLYMARKET_PRIVATE_KEY", "") or "").strip()
POLYMARKET_FUNDER = str(os.getenv("POLYMARKET_FUNDER", "") or "").strip()
POLYMARKET_API_KEY = str(os.getenv("POLYMARKET_API_KEY", "") or "").strip()
POLYMARKET_API_SECRET = str(os.getenv("POLYMARKET_API_SECRET", "") or "").strip()
POLYMARKET_KEY_ID = str(os.getenv("POLYMARKET_KEY_ID", "") or POLYMARKET_API_KEY).strip()
POLYMARKET_SECRET_KEY = str(os.getenv("POLYMARKET_SECRET_KEY", "") or POLYMARKET_API_SECRET).strip()
POLYMARKET_API_PASSPHRASE = str(os.getenv("POLYMARKET_API_PASSPHRASE", "") or "").strip()
POLYMARKET_TIMEOUT_SEC = int(os.getenv("POLYMARKET_TIMEOUT_SEC", "15"))
POLYMARKET_MARKET_CACHE_TTL_SEC = max(120, int(os.getenv("POLYMARKET_MARKET_CACHE_TTL_SEC", "900") or "900"))
POLYMARKET_MARKET_PAGES = max(1, min(int(os.getenv("POLYMARKET_MARKET_PAGES", "4") or "4"), 20))
POLYMARKET_PAGE_LIMIT = max(50, min(int(os.getenv("POLYMARKET_PAGE_LIMIT", "200") or "200"), 500))
POLYMARKET_US_COOLDOWN_SEC = max(120, int(os.getenv("POLYMARKET_US_COOLDOWN_SEC", "900") or "900"))

_MARKET_CACHE_LOCK = threading.Lock()
_MARKET_CACHE: dict[str, Any] = {"ts": 0.0, "payload": None}
_BALANCE_CACHE_LOCK = threading.Lock()
_BALANCE_CACHE: dict[str, Any] = {"ts": 0.0, "payload": None}
_BALANCE_COOLDOWN_UNTIL = 0.0


def _norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())).strip()


def _parse_jsonish_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = __import__("json").loads(raw)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
    return []


def _as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _as_decimal_str(value: Any, *, default: str = "0") -> str:
    num = _as_float(value)
    if num is None:
        return default
    return f"{num:.8f}".rstrip("0").rstrip(".") or "0"


def _has_us_api_creds() -> bool:
    return bool(POLYMARKET_KEY_ID and POLYMARKET_SECRET_KEY)


def _us_auth_headers(method: str, path: str) -> dict[str, str]:
    if not _has_us_api_creds():
        raise RuntimeError("POLYMARKET_KEY_ID and POLYMARKET_SECRET_KEY are required for Polymarket US authenticated API.")

    try:
        import base64

        secret_bytes = base64.b64decode(POLYMARKET_SECRET_KEY)
        # Docs show using first 32 bytes as Ed25519 private key bytes.
        private_key = ed25519.Ed25519PrivateKey.from_private_bytes(secret_bytes[:32])
    except Exception as exc:
        raise RuntimeError("POLYMARKET_SECRET_KEY is invalid for Ed25519 signing.") from exc

    ts = str(int(time.time() * 1000))
    message = f"{ts}{str(method or '').upper()}{path}"
    signature = base64.b64encode(private_key.sign(message.encode("utf-8"))).decode("utf-8")
    return {
        "X-PM-Access-Key": POLYMARKET_KEY_ID,
        "X-PM-Timestamp": ts,
        "X-PM-Signature": signature,
        "Content-Type": "application/json",
    }


def _us_request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    headers = _us_auth_headers(method, path)
    resp = requests.request(
        str(method or "GET").upper(),
        f"{POLYMARKET_US_API_BASE}{path}",
        headers=headers,
        params=params or None,
        data=(json.dumps(payload) if payload is not None else None),
        timeout=POLYMARKET_TIMEOUT_SEC,
    )
    try:
        data = resp.json() if resp.content else {}
    except Exception:
        data = {"raw": resp.text}

    if not resp.ok:
        msg = ""
        if isinstance(data, dict):
            msg = str(data.get("message") or data.get("error") or data.get("details") or data)
        if resp.status_code in {429, 403}:
            raw_text = ""
            if isinstance(data, dict):
                raw_text = str(data.get("raw") or "")
            if not raw_text and getattr(resp, "text", None):
                raw_text = str(resp.text)
            if any(token in raw_text.lower() for token in ("rate limited", "cloudflare", "error 1015", "enable cookies")):
                raise RuntimeError(f"Polymarket US API {resp.status_code}: rate limited by Cloudflare")
        raise RuntimeError(f"Polymarket US API {resp.status_code}: {msg or resp.reason}")

    return data if isinstance(data, dict) else {"raw": data}


def _parse_iso_dt(value: Any) -> datetime.datetime | None:
    if isinstance(value, datetime.datetime):
        dt = value
    else:
        raw = str(value or "").strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.datetime.fromisoformat(raw)
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)


def _bet_start_dt(bet: dict[str, Any]) -> datetime.datetime | None:
    for value in (
        bet.get("scheduled_start"),
        bet.get("game_datetime"),
        bet.get("start_time"),
    ):
        dt = _parse_iso_dt(value)
        if dt is not None:
            return dt

    game_date = str(bet.get("game_date") or "").strip()
    game_time = str(bet.get("game_time") or "").strip()
    if game_date and game_time:
        dt = _parse_iso_dt(f"{game_date}T{game_time}")
        if dt is not None:
            return dt
    return _parse_iso_dt(bet.get("game_date"))


def _bet_date_hints(bet: dict[str, Any]) -> list[str]:
    dt = _bet_start_dt(bet)
    if dt is None:
        return []
    dt_utc = dt.astimezone(datetime.timezone.utc)
    mmdd = f"{dt_utc.month:02d}{dt_utc.day:02d}"
    yyyymmdd = f"{dt_utc.year:04d}{dt_utc.month:02d}{dt_utc.day:02d}"
    yyyy_mm_dd = f"{dt_utc.year:04d}-{dt_utc.month:02d}-{dt_utc.day:02d}"
    yy_mm_dd = f"{dt_utc.year % 100:02d}-{dt_utc.month:02d}-{dt_utc.day:02d}"
    out: list[str] = []
    for token in (mmdd, yyyymmdd, yyyy_mm_dd, yy_mm_dd):
        if token and token not in out:
            out.append(token)
    return out


def _bet_kind_tag(bet: dict[str, Any]) -> str:
    text = _norm_text(" ".join(str(bet.get(key) or "") for key in ("kind", "bet_type", "prop_type", "label", "pick")))
    if "combo" in text:
        return "combo"
    if any(token in text for token in ("player prop", "prop", "points", "rebounds", "assists", "hits", "runs", "rbi", "strikeouts", "shots on target", "goals", "corners", "cards")):
        return "player_prop"
    if any(token in text for token in ("moneyline", "money line", "winner", "match winner", "1x2", "draw no bet", "btts", "win")):
        return "moneyline"
    if any(token in text for token in ("run line", "spread", "handicap")):
        return "spread"
    if any(token in text for token in ("team total", "total", "over under", "goals o u")):
        return "total"
    return "single"


def _bet_sport_tag(bet: dict[str, Any]) -> str:
    text = _norm_text(" ".join(str(bet.get(key) or "") for key in ("sport", "bet_type", "prop_type", "label", "pick", "game")))
    if any(token in text for token in ("basketball", "nba", "wnba")):
        return "basketball"
    if any(token in text for token in ("baseball", "mlb")):
        return "baseball"
    if any(token in text for token in ("football", "nfl", "ncaaf", "american football")):
        return "football"
    if any(token in text for token in ("hockey", "nhl")):
        return "hockey"
    if any(token in text for token in ("soccer", "mls", "premier", "champions league", "1x2", "btts", "goals o u")):
        return "soccer"
    if any(token in text for token in ("tennis", "atp", "wta")):
        return "tennis"
    if any(token in text for token in ("boxing", "mma", "ufc", "fight", "submission", "knockout")):
        return "combat"
    if any(token in text for token in ("golf", "pga", "lpga")):
        return "golf"
    if any(token in text for token in ("f1", "nascar", "motorsport", "race")):
        return "motorsports"
    if any(token in text for token in ("cricket", "wicket", "innings")):
        return "cricket"
    return "other"



# ── Team / player alias expansion ─────────────────────────────────────────────
# Maps abbreviations, nicknames, city-only references → normalized full name tokens
# Used to expand both bet picks and market text before matching.
_ALIASES: dict[str, str] = {
    # NBA
    "gsw": "golden state warriors", "warriors": "golden state warriors",
    "lal": "los angeles lakers", "lakers": "los angeles lakers",
    "lac": "los angeles clippers", "clippers": "los angeles clippers",
    "bos": "boston celtics", "celtics": "boston celtics",
    "mia": "miami heat", "heat": "miami heat",
    "nyk": "new york knicks", "knicks": "new york knicks",
    "chi": "chicago bulls", "bulls": "chicago bulls",
    "den": "denver nuggets", "nuggets": "denver nuggets",
    "phx": "phoenix suns", "suns": "phoenix suns",
    "mil": "milwaukee bucks", "bucks": "milwaukee bucks",
    "okc": "oklahoma city thunder", "thunder": "oklahoma city thunder",
    "ind": "indiana pacers", "pacers": "indiana pacers",
    "min": "minnesota timberwolves", "wolves": "minnesota timberwolves",
    "cle": "cleveland cavaliers", "cavs": "cleveland cavaliers",
    "atl": "atlanta hawks", "hawks": "atlanta hawks",
    "mem": "memphis grizzlies", "grizzlies": "memphis grizzlies",
    "dal": "dallas mavericks", "mavs": "dallas mavericks",
    "hou": "houston rockets", "rockets": "houston rockets",
    "sas": "san antonio spurs", "spurs": "san antonio spurs",
    "nop": "new orleans pelicans", "pelicans": "new orleans pelicans",
    "por": "portland trail blazers", "blazers": "portland trail blazers",
    "sac": "sacramento kings", "kings": "sacramento kings",
    "uta": "utah jazz", "jazz": "utah jazz",
    "tor": "toronto raptors", "raptors": "toronto raptors",
    "cha": "charlotte hornets", "hornets": "charlotte hornets",
    "det": "detroit pistons", "pistons": "detroit pistons",
    "was": "washington wizards", "wizards": "washington wizards",
    "orl": "orlando magic", "magic": "orlando magic",
    "bkn": "brooklyn nets", "nets": "brooklyn nets",
    # MLB
    "nyy": "new york yankees", "yankees": "new york yankees",
    "bos_mlb": "boston red sox", "red sox": "boston red sox",
    "lad": "los angeles dodgers", "dodgers": "los angeles dodgers",
    "hou_mlb": "houston astros", "astros": "houston astros",
    "atl_mlb": "atlanta braves", "braves": "atlanta braves",
    "chc": "chicago cubs", "cubs": "chicago cubs",
    "cws": "chicago white sox", "white sox": "chicago white sox",
    "stl": "st louis cardinals", "cardinals": "st louis cardinals",
    "sf": "san francisco giants", "giants": "san francisco giants",
    "sd": "san diego padres", "padres": "san diego padres",
    "sea": "seattle mariners", "mariners": "seattle mariners",
    "tex": "texas rangers", "rangers": "texas rangers",
    "tor_mlb": "toronto blue jays", "blue jays": "toronto blue jays",
    "min_mlb": "minnesota twins", "twins": "minnesota twins",
    "cle_mlb": "cleveland guardians", "guardians": "cleveland guardians",
    "det_mlb": "detroit tigers", "tigers": "detroit tigers",
    "phi": "philadelphia phillies", "phillies": "philadelphia phillies",
    "nym": "new york mets", "mets": "new york mets",
    "mia_mlb": "miami marlins", "marlins": "miami marlins",
    "mil_mlb": "milwaukee brewers", "brewers": "milwaukee brewers",
    "cin": "cincinnati reds", "reds": "cincinnati reds",
    "pit": "pittsburgh pirates", "pirates": "pittsburgh pirates",
    "was_mlb": "washington nationals", "nationals": "washington nationals",
    "col": "colorado rockies", "rockies": "colorado rockies",
    "ari": "arizona diamondbacks", "dbacks": "arizona diamondbacks",
    "oak": "oakland athletics", "athletics": "oakland athletics",
    "kc": "kansas city royals", "royals": "kansas city royals",
    "tb": "tampa bay rays", "rays": "tampa bay rays",
    "bal": "baltimore orioles", "orioles": "baltimore orioles",
    # NFL
    "ne": "new england patriots", "patriots": "new england patriots",
    "kc_nfl": "kansas city chiefs", "chiefs": "kansas city chiefs",
    "sf_nfl": "san francisco 49ers", "49ers": "san francisco 49ers", "niners": "san francisco 49ers",
    "dal_nfl": "dallas cowboys", "cowboys": "dallas cowboys",
    "gb": "green bay packers", "packers": "green bay packers",
    "buf": "buffalo bills", "bills": "buffalo bills",
    "phi_nfl": "philadelphia eagles", "eagles": "philadelphia eagles",
    "sea_nfl": "seattle seahawks", "seahawks": "seattle seahawks",
    "lac_nfl": "los angeles chargers", "chargers": "los angeles chargers",
    "lar": "los angeles rams", "rams": "los angeles rams",
    "den_nfl": "denver broncos", "broncos": "denver broncos",
    "min_nfl": "minnesota vikings", "vikings": "minnesota vikings",
    "chi_nfl": "chicago bears", "bears": "chicago bears",
    "det_nfl": "detroit lions", "lions": "detroit lions",
    "bal_nfl": "baltimore ravens", "ravens": "baltimore ravens",
    "cin_nfl": "cincinnati bengals", "bengals": "cincinnati bengals",
    "pit_nfl": "pittsburgh steelers", "steelers": "pittsburgh steelers",
    "cle_nfl": "cleveland browns", "browns": "cleveland browns",
    "ten": "tennessee titans", "titans": "tennessee titans",
    "ind_nfl": "indianapolis colts", "colts": "indianapolis colts",
    "jax": "jacksonville jaguars", "jaguars": "jaguars",
    "hou_nfl": "houston texans", "texans": "houston texans",
    "nyg": "new york giants",
    "nyj": "new york jets", "jets": "new york jets",
    "mia_nfl": "miami dolphins", "dolphins": "miami dolphins",
    "atl_nfl": "atlanta falcons", "falcons": "atlanta falcons",
    "car": "carolina panthers", "panthers": "carolina panthers",
    "no": "new orleans saints", "saints": "new orleans saints",
    "tb_nfl": "tampa bay buccaneers", "buccaneers": "tampa bay buccaneers", "bucs": "tampa bay buccaneers",
    "ari_nfl": "arizona cardinals",
    "lar_nfl": "los angeles rams",
    "lv": "las vegas raiders", "raiders": "las vegas raiders",
    # NHL
    "bos_nhl": "boston bruins", "bruins": "boston bruins",
    "tor_nhl": "toronto maple leafs", "maple leafs": "toronto maple leafs", "leafs": "toronto maple leafs",
    "mtl": "montreal canadiens", "canadiens": "montreal canadiens", "habs": "montreal canadiens",
    "nyc": "new york rangers", "nyr": "new york rangers",
    "nyi": "new york islanders", "islanders": "new york islanders",
    "nj": "new jersey devils", "devils": "new jersey devils",
    "phi_nhl": "philadelphia flyers", "flyers": "philadelphia flyers",
    "pit_nhl": "pittsburgh penguins", "penguins": "pittsburgh penguins",
    "was_nhl": "washington capitals", "capitals": "washington capitals", "caps": "washington capitals",
    "car_nhl": "carolina hurricanes", "hurricanes": "carolina hurricanes",
    "fla": "florida panthers",
    "tb_nhl": "tampa bay lightning", "lightning": "tampa bay lightning",
    "chi_nhl": "chicago blackhawks", "blackhawks": "chicago blackhawks",
    "det_nhl": "detroit red wings", "red wings": "detroit red wings",
    "stl_nhl": "st louis blues", "blues": "st louis blues",
    "min_nhl": "minnesota wild", "wild": "minnesota wild",
    "wpg": "winnipeg jets",
    "col_nhl": "colorado avalanche", "avalanche": "colorado avalanche",
    "edm": "edmonton oilers", "oilers": "edmonton oilers",
    "cgy": "calgary flames", "flames": "calgary flames",
    "van": "vancouver canucks", "canucks": "vancouver canucks",
    "sea_nhl": "seattle kraken", "kraken": "seattle kraken",
    "ari_nhl": "arizona coyotes", "coyotes": "arizona coyotes",
    "lak": "los angeles kings",
    "sjs": "san jose sharks", "sharks": "san jose sharks",
    "ana": "anaheim ducks", "ducks": "anaheim ducks",
    "dal_nhl": "dallas stars", "stars": "dallas stars",
    "nsh": "nashville predators", "predators": "nashville predators",
    "cbj": "columbus blue jackets",
    "buf_nhl": "buffalo sabres", "sabres": "buffalo sabres",
    "ott": "ottawa senators", "senators": "ottawa senators",
}

def _expand_aliases(text: str) -> str:
    """Expand known abbreviations/nicknames in normalised text to full names."""
    tokens = text.split()
    expanded = []
    i = 0
    while i < len(tokens):
        # try 2-token phrases first (e.g., "red sox", "white sox")
        if i + 1 < len(tokens):
            two = tokens[i] + " " + tokens[i + 1]
            if two in _ALIASES:
                expanded.append(_ALIASES[two])
                i += 2
                continue
        tok = tokens[i]
        expanded.append(_ALIASES.get(tok, tok))
        i += 1
    return " ".join(expanded)


def _char_ngrams(text: str, n: int = 3) -> list[str]:
    """Return character n-grams from text (no spaces)."""
    t = text.replace(" ", "")
    return [t[i:i+n] for i in range(len(t) - n + 1)] if len(t) >= n else [t] if t else []


def _fuzzy_name_score(text: str, name: Any) -> float:
    """
    Multi-signal entity match: exact > all-tokens > last-name > token-ratio > trigram.
    Returns a 0-5.5 score.
    """
    norm = _norm_text(name)
    if not text or not norm:
        return 0.0

    # Expand aliases in both sides
    text_exp = _expand_aliases(text)
    norm_exp = _expand_aliases(norm)

    # Exact substring match (after alias expansion)
    if norm_exp in text_exp or norm in text:
        return 5.5

    tokens = [tok for tok in norm_exp.split() if len(tok) >= 3]

    if not tokens:
        return 0.0

    # All tokens present
    if all(tok in text_exp for tok in tokens):
        return 4.2

    # Last name only (common for tennis, combat, golf)
    last = tokens[-1]
    if len(last) >= 4 and last in text_exp:
        return 3.8

    # Token ratio
    matched = sum(1 for tok in tokens if tok in text_exp)
    ratio = matched / len(tokens)
    if ratio >= 0.75:
        return 3.5
    if ratio >= 0.5:
        return 2.8

    # Character trigram similarity (Jaccard)
    ng_name = set(_char_ngrams(norm_exp.replace(" ", ""), 3))
    ng_text = set(_char_ngrams(text_exp.replace(" ", ""), 3))
    if ng_name and ng_text:
        jaccard = len(ng_name & ng_text) / len(ng_name | ng_text)
        if jaccard >= 0.5:
            return 2.5 * jaccard
        if jaccard >= 0.3:
            return 1.5 * jaccard

    return 0.0


def _picked_team_name(bet: dict[str, Any]) -> str:
    pick = _norm_text(" ".join(str(bet.get(key) or "") for key in ("pick", "label")))
    home = str(bet.get("home_team") or "").strip()
    away = str(bet.get("away_team") or "").strip()
    for team in (home, away):
        if team and _norm_text(team) in pick:
            return team
    return str(bet.get("team") or "").strip()


def _entity_match_score(text: str, name: Any) -> float:
    return _fuzzy_name_score(text, name)


def _extract_vs_teams(market_text: str) -> tuple[str, str]:
    """Extract 'Team A' and 'Team B' from patterns like 'team a vs team b' or 'team a beats team b'."""
    for sep in (" vs ", " versus ", " v ", " beat ", " beats ", " defeats ", " at ", " host "):
        idx = market_text.find(sep)
        if idx > 0:
            left = market_text[:idx].strip().split()[-4:]
            right = market_text[idx + len(sep):].strip().split()[:4]
            return " ".join(left), " ".join(right)
    return "", ""


def _token_overlap_score(text: str, *values: Any) -> float:
    score = 0.0
    seen: set[str] = set()
    for value in values:
        for token in _norm_text(value).split():
            if len(token) < 3 or token in seen:
                continue
            seen.add(token)
            if token in text:
                score += 0.6
    return score


def _line_match_score(text: str, line: Any) -> float:
    num = _as_float(line)
    if num is None:
        return 0.0
    candidates = {
        _norm_text(f"{num:g}"),
        _norm_text(f"{num:.1f}"),
        _norm_text(str(int(num))) if float(num).is_integer() else "",
    }
    return 2.2 if any(candidate and candidate in text for candidate in candidates) else 0.0


def _line_proximity_score(text: str, line: Any, *, direction: str = "") -> float:
    bet_num = _as_float(line)
    if bet_num is None:
        return 0.0
    nums = [num for num in (_as_float(tok.replace("_", ".")) for tok in re.findall(r"\b\d+(?:[._]\d+)?\b", text)) if num is not None]
    if not nums:
        return 0.0
    closest = min(abs(num - bet_num) for num in nums)
    direction_norm = _norm_text(direction)
    if closest <= 1.0:
        return 2.4 if any(token in direction_norm for token in ("over", "under")) else 2.2
    if closest <= 3.0:
        return 1.6
    if closest <= 8.0:
        return 0.8
    return 0.0


def _market_text(market: dict[str, Any]) -> str:
    event = market.get("events")
    event_obj = event[0] if isinstance(event, list) and event and isinstance(event[0], dict) else (event if isinstance(event, dict) else {})
    parts = [
        market.get("title"),
        market.get("question"),
        market.get("description"),
        market.get("subtitle"),
        market.get("event_slug"),
        market.get("slug"),
        market.get("category"),
        market.get("endDateIso"),
        market.get("endDate"),
        market.get("start_date"),
        event_obj.get("title") if isinstance(event_obj, dict) else None,
        event_obj.get("slug") if isinstance(event_obj, dict) else None,
        event_obj.get("description") if isinstance(event_obj, dict) else None,
    ]
    outcomes = market.get("outcomes")
    if isinstance(outcomes, list):
        parts.extend(outcomes)
    return _norm_text(" ".join(str(part or "") for part in parts))


def _market_event(market: dict[str, Any]) -> dict[str, Any]:
    events = market.get("events")
    if isinstance(events, list):
        for event in events:
            if isinstance(event, dict):
                return event
    if isinstance(events, dict):
        return events
    return {}


def _market_sport_tag(market: dict[str, Any]) -> str:
    text = _market_text(market)
    category = _norm_text(market.get("category") or market.get("market_category") or "")
    tag = _norm_text(market.get("tag") or market.get("topic") or "")
    search = f"{text} {category} {tag}"
    if any(token in search for token in ("basketball", "nba", "wnba", "ncaab", "euroleague")):
        return "basketball"
    if any(token in search for token in ("baseball", "mlb", "kbo", "npb")):
        return "baseball"
    if any(token in search for token in ("football", "nfl", "ncaaf", "american football", "americanfootball", "super bowl", "college football")):
        return "football"
    if any(token in search for token in ("hockey", "nhl", "icehockey", "ice hockey", "stanley cup")):
        return "hockey"
    if any(token in search for token in ("soccer", "mls", "premier", "champions league", "1x2", "btts", "goals o u", "bundesliga", "la liga", "serie a", "ligue 1")):
        return "soccer"
    if any(token in search for token in ("tennis", "atp", "wta", "wimbledon", "roland garros", "us open", "australian open")):
        return "tennis"
    if any(token in search for token in ("boxing", "mma", "ufc", "fight", "submission", "knockout", "bellator", "pfl")):
        return "combat"
    if any(token in search for token in ("golf", "pga", "lpga", "masters", "open championship", "ryder cup")):
        return "golf"
    if any(token in search for token in ("f1", "nascar", "motorsport", "race", "formula 1", "indycar")):
        return "motorsports"
    if any(token in search for token in ("cricket", "wicket", "innings", "ipl", "world cup")):
        return "cricket"
    # Non-sports domains to avoid "unknown" buckets and improve observability.
    if any(token in search for token in ("election", "president", "senate", "house", "governor", "democrat", "republican", "trump", "biden", "politic")):
        return "politics"
    if any(token in search for token in ("bitcoin", "ethereum", "solana", "crypto", "token", "airdrop", "defi", "btc", "eth")):
        return "crypto"
    if any(token in search for token in ("cpi", "inflation", "fed", "interest rate", "gdp", "recession", "economy", "jobs report", "unemployment")):
        return "macro"
    if any(token in search for token in ("gta", "movie", "album", "music", "celebrity", "oscar", "grammy", "tv show", "netflix")):
        return "entertainment"
    if any(token in search for token in ("openai", "ai", "chatgpt", "tesla", "apple", "microsoft", "google", "meta")):
        return "tech"
    if any(token in search for token in ("china", "taiwan", "war", "israel", "ukraine", "nato", "geopolit")):
        return "geopolitics"
    return "other"


def _canonical_golf_series_ticker(value: Any) -> str:
    text = _norm_text(value)
    if not text:
        return ""
    if "lpga" in text or "womens open" in text or "solheim" in text or "chevron" in text:
        return "LPGA"
    if "masters" in text:
        return "PGA_MASTERS"
    if "us open" in text and "tennis" not in text:
        return "PGA_US_OPEN"
    if "open championship" in text or "british open" in text:
        return "PGA_OPEN_CHAMPIONSHIP"
    if "pga championship" in text:
        return "PGA_CHAMPIONSHIP"
    if "players championship" in text or "the players" in text:
        return "PGA_PLAYERS"
    if "ryder cup" in text:
        return "PGA_RYDER_CUP"
    if "golf" in text or "pga" in text:
        return "PGA"
    return ""


def _canonical_tennis_series_ticker(value: Any) -> str:
    text = _norm_text(value)
    if not text:
        return ""
    if "wta" in text or "women" in text:
        return "WTA"
    if "atp" in text or "men" in text:
        return "ATP"
    if "wimbledon" in text:
        return "TENNIS_WIMBLEDON"
    if "roland" in text or "french open" in text:
        return "TENNIS_ROLAND_GARROS"
    if "us open" in text and "golf" not in text:
        return "TENNIS_US_OPEN"
    if "australian open" in text:
        return "TENNIS_AUSTRALIAN_OPEN"
    if "tennis" in text:
        return "TENNIS"
    return ""


def _canonical_combat_series_ticker(value: Any) -> str:
    text = _norm_text(value)
    if not text:
        return ""
    if "ufc" in text:
        return "UFC"
    if "pfl" in text:
        return "PFL"
    if "bellator" in text:
        return "BELLATOR"
    if "boxing" in text or "box" in text:
        return "BOXING"
    if "mma" in text or "fight" in text:
        return "MMA"
    return ""


def _market_series_ticker(market: dict[str, Any]) -> str:
    sport = _market_sport_tag(market)
    source_text = " ".join(
        [
            str(market.get("market_event_title") or ""),
            str(market.get("market_event_slug") or ""),
            str(market.get("market_title") or ""),
            str(market.get("question") or ""),
            str(market.get("market_slug") or ""),
            str(market.get("event_slug") or ""),
            str(market.get("slug") or ""),
        ]
    )
    if sport == "golf":
        canon = _canonical_golf_series_ticker(source_text)
        if canon:
            return canon
        return "GOLF"
    if sport == "tennis":
        canon = _canonical_tennis_series_ticker(source_text)
        if canon:
            return canon
        return "TENNIS"
    if sport == "combat":
        canon = _canonical_combat_series_ticker(source_text)
        if canon:
            return canon
        return "MMA"

    event = _market_event(market)
    candidates = [
        str(event.get("series_ticker") or "").strip(),
        str(market.get("series_ticker") or "").strip(),
        str(event.get("slug") or "").strip(),
        str(market.get("event_slug") or "").strip(),
    ]
    for cand in candidates:
        if cand:
            return re.sub(r"[^A-Z0-9_]+", "_", cand.upper()).strip("_")[:120]
    return ""


def _golf_series_family(series_ticker: Any) -> str:
    upper = str(series_ticker or "").strip().upper()
    if not upper:
        return ""
    if "LPGA" in upper:
        return "LPGA"
    if any(token in upper for token in ("PGA", "GOLF", "MASTERS", "US_OPEN", "OPEN_CHAMPIONSHIP", "RYDER")):
        return "PGA"
    return ""


def _tennis_series_family(series_ticker: Any) -> str:
    upper = str(series_ticker or "").strip().upper()
    if not upper:
        return ""
    if "WTA" in upper:
        return "WTA"
    if "ATP" in upper:
        return "ATP"
    if any(token in upper for token in ("TENNIS", "WIMBLEDON", "ROLAND", "AUSTRALIAN_OPEN", "US_OPEN")):
        return "TENNIS"
    return ""


def _combat_series_family(series_ticker: Any) -> str:
    upper = str(series_ticker or "").strip().upper()
    if not upper:
        return ""
    if "UFC" in upper:
        return "UFC"
    if "PFL" in upper:
        return "PFL"
    if "BELLATOR" in upper:
        return "BELLATOR"
    if "BOX" in upper:
        return "BOXING"
    if "MMA" in upper or "FIGHT" in upper:
        return "MMA"
    return ""


def _bet_series_hints(bet: dict[str, Any]) -> list[str]:
    hints: list[str] = []
    explicit = [
        bet.get("polymarket_series_ticker"),
        bet.get("series_ticker"),
        bet.get("kalshi_series_ticker"),
    ]
    for raw in explicit:
        token = str(raw or "").strip().upper()
        if not token:
            continue
        token = re.sub(r"[^A-Z0-9_]+", "_", token).strip("_")
        if token:
            hints.append(token)

    league = _norm_text(bet.get("league") or bet.get("competition") or "")
    if "wnba" in league:
        hints.extend(["WNBA", "BASKETBALL"])
    elif "nba" in league:
        hints.extend(["NBA", "BASKETBALL"])
    elif "mlb" in league:
        hints.extend(["MLB", "BASEBALL"])
    elif "nhl" in league:
        hints.extend(["NHL", "HOCKEY"])
    elif "nfl" in league:
        hints.extend(["NFL", "FOOTBALL"])
    elif any(tok in league for tok in ("epl", "premier", "mls", "champions", "uefa", "fifa")):
        hints.extend(["SOCCER", "FOOTBALL"])

    sport = _bet_sport_tag(bet)
    if sport:
        hints.append(sport.upper())

    for token in _bet_date_hints(bet):
        hints.append(token.upper())

    return list(dict.fromkeys([h for h in hints if h]))


def _series_alignment_score(bet: dict[str, Any], market: dict[str, Any], market_text: str) -> float:
    hints = _bet_series_hints(bet)
    if not hints:
        return 0.0

    series = str(market.get("market_series_ticker") or _market_series_ticker(market) or "").upper()
    search_space = f"{series} {market_text.upper()}"
    if any(hint and hint in search_space for hint in hints):
        return 2.0

    date_hints = _bet_date_hints(bet)
    if date_hints and any(hint.upper() in search_space for hint in date_hints):
        return 1.2

    return -0.8


def _market_start_dt(market: dict[str, Any]) -> datetime.datetime | None:
    event = _market_event(market)
    for key in ("start_date", "startDateIso", "startDate", "close_time", "endDateIso", "end_date", "created_at", "updated_at"):
        dt = _parse_iso_dt(market.get(key))
        if dt is not None:
            return dt
    for key in ("startDate", "endDate"):
        dt = _parse_iso_dt(event.get(key))
        if dt is not None:
            return dt
    return None


def _time_score(bet: dict[str, Any], market: dict[str, Any]) -> float:
    bet_dt = _bet_start_dt(bet)
    market_dt = _market_start_dt(market)
    if bet_dt is None or market_dt is None:
        # If explicit datetimes are missing, still reward same-day date-token alignment.
        search_space = _market_text(market).upper()
        if any(hint.upper() in search_space for hint in _bet_date_hints(bet)):
            return 0.8
        return 0.0
    delta_hours = abs((market_dt - bet_dt).total_seconds()) / 3600.0
    if delta_hours <= 3:
        return 2.0
    if delta_hours <= 12:
        return 1.0
    if delta_hours <= 36:
        return 0.4
    return 0.0


def _market_kind_tag(market: dict[str, Any]) -> str:
    text = _market_text(market)
    title = _norm_text(" ".join(str(market.get(k) or "") for k in ("market_title", "title", "question", "market_slug", "event_slug", "slug")))
    search = f"{text} {title}"
    if any(token in search for token in ("player prop", "points", "rebounds", "assists", "hits", "runs", "strikeouts", "shots", "saves", "goals", "cards", "corners", "to record")):
        return "player_prop"
    if any(token in search for token in ("moneyline", "winner", "match winner", "1x2", "to win", "beats", "defeats", "champion")):
        return "moneyline"
    if any(token in search for token in ("spread", "handicap", "by more than", "by at least")):
        return "spread"
    if any(token in search for token in ("total", "over under", "over", "under", "goals")):
        return "total"
    return "single"


def _market_identifier(market: dict[str, Any]) -> str:
    for key in ("condition_id", "id", "slug", "event_slug"):
        value = str(market.get(key) or "").strip()
        if value:
            return value
    return ""


def _market_side(bet: dict[str, Any], market: dict[str, Any]) -> str:
    """Determine YES/NO side based on which team the bet picks, using market title parsing."""
    direction_text = _norm_text(" ".join(str(bet.get(key) or "") for key in ("direction", "pick", "label", "bet_type")))

    # Player props: OVER = YES, UNDER = NO
    if _bet_kind_tag(bet) == "player_prop":
        return "no" if "under" in direction_text else "yes"

    # Totals
    if "under" in direction_text and "over" not in direction_text:
        return "no"
    if "against" in direction_text:
        return "no"

    # For moneyline: check if the picked team is the YES outcome in the market title
    # Market titles like "Will the Warriors beat the Lakers?" → YES = Warriors
    picked = _picked_team_name(bet)
    if picked:
        market_title = _norm_text(str(market.get("market_title") or market.get("title") or market.get("question") or ""))

        # Extract the team in the YES position from "team_a vs team_b" or "will team_a beat team_b"
        team_a, team_b = _extract_vs_teams(market_title)
        if team_a and team_b:
            picked_norm = _norm_text(picked)
            # Check alias expansion
            picked_exp = _expand_aliases(picked_norm)
            team_a_exp = _expand_aliases(team_a)
            team_b_exp = _expand_aliases(team_b)
            # YES side is typically the first named team (home team in polymarket)
            score_a = _fuzzy_name_score(team_a_exp, picked_exp)
            score_b = _fuzzy_name_score(team_b_exp, picked_exp)
            if score_a >= 2.0 and score_a > score_b:
                return "yes"
            if score_b >= 2.0 and score_b > score_a:
                return "no"

    return "yes"


def _score_market(bet: dict[str, Any], market: dict[str, Any]) -> float:
    market_text = _market_text(market)
    if not market_text:
        return 0.0

    bet_sport = _bet_sport_tag(bet)
    market_sport = _market_sport_tag(market)

    # Hard sport mismatch → immediate reject
    if bet_sport and market_sport and bet_sport != market_sport:
        return 0.0

    # Cross-sport sanity guard when bet sport is unknown
    if not bet_sport and market_sport in {"hockey", "basketball", "baseball", "football", "soccer", "tennis", "combat", "golf", "motorsports", "cricket"}:
        if _bet_kind_tag(bet) in {"moneyline", "spread", "player_prop"}:
            label_text = _norm_text(" ".join(str(bet.get(key) or "") for key in ("label", "pick", "team", "player_name", "name", "game")))
            if market_sport == "hockey" and any(t in label_text for t in ("nba", "wnba", "basketball", "thunder", "knicks", "warriors", "celtics", "lakers")):
                return 0.0
            if market_sport == "basketball" and any(t in label_text for t in ("nhl", "hockey", "stanley", "goalie")):
                return 0.0

    # Build alias-expanded market text for matching
    market_text_exp = _expand_aliases(market_text)

    score = _time_score(bet, market)
    score += _series_alignment_score(bet, market, market_text)

    kind = _bet_kind_tag(bet)
    market_kind = _market_kind_tag(market)

    # ── Player prop ──────────────────────────────────────────────────────────
    if kind == "player_prop":
        if market_kind != "player_prop":
            return 0.0
        player_name = str(bet.get("player_name") or bet.get("name") or "")
        player_score = _entity_match_score(market_text_exp, player_name)
        if player_score < 2.0:
            return 0.0
        score += player_score * 2.2
        score += max(
            _entity_match_score(market_text_exp, bet.get("team")),
            _entity_match_score(market_text_exp, bet.get("home_team")),
            _entity_match_score(market_text_exp, bet.get("away_team")),
        ) * 0.3
        score += _token_overlap_score(market_text_exp, bet.get("prop_type"), bet.get("bet_type"), bet.get("label"))
        score += _line_match_score(market_text_exp, bet.get("line")) or _line_proximity_score(market_text_exp, bet.get("line"), direction=str(bet.get("direction") or bet.get("pick") or ""))
        return score

    # ── Team-based bets ──────────────────────────────────────────────────────
    if kind == "moneyline" and market_kind not in {"moneyline", "single"}:
        return 0.0
    if kind == "spread" and market_kind not in {"spread", "single"}:
        return 0.0
    if kind == "total" and market_kind not in {"total", "single"}:
        return 0.0

    picked = _picked_team_name(bet)

    # Extract both teams from the market title for bidirectional matching
    market_title_norm = _norm_text(str(market.get("market_title") or market.get("title") or market.get("question") or ""))
    team_a, team_b = _extract_vs_teams(market_title_norm)

    # Score each team candidate with alias expansion
    candidates = [
        bet.get("home_team"), bet.get("away_team"),
        bet.get("team"), picked,
    ]
    team_score = max(
        (_entity_match_score(market_text_exp, c) for c in candidates),
        default=0.0,
    )

    # Bonus: picked team matches one side of a vs-market exactly
    if team_a and team_b and picked:
        picked_exp = _expand_aliases(_norm_text(picked))
        score_a = _fuzzy_name_score(_expand_aliases(team_a), picked_exp)
        score_b = _fuzzy_name_score(_expand_aliases(team_b), picked_exp)
        if max(score_a, score_b) >= 3.0:
            team_score = max(team_score, max(score_a, score_b) + 0.8)

    # Both teams in market (game integrity check)
    if team_a and team_b:
        home_in_market = max(
            _entity_match_score(market_text_exp, bet.get("home_team")),
            _entity_match_score(market_text_exp, bet.get("away_team")),
        )
        if home_in_market >= 3.0:
            score += 1.2  # Both teams confirmed present

    if team_score < 3.0 and kind in {"moneyline", "spread"}:
        return 0.0

    score += team_score * 1.5
    score += _token_overlap_score(market_text_exp, bet.get("label"), bet.get("pick"), bet.get("bet_type"), bet.get("prop_type"))
    score += _line_match_score(market_text_exp, bet.get("line")) or _line_proximity_score(market_text_exp, bet.get("line"), direction=str(bet.get("direction") or bet.get("pick") or ""))

    if kind in {"moneyline", "spread", "total"} and score < 4.0:
        return 0.0
    return score


def _clean_market(market: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(market, dict):
        return {}
    title = str(market.get("title") or market.get("question") or market.get("slug") or "").strip()
    identifier = _market_identifier(market)
    event = _market_event(market)
    outcomes = [str(v or "").strip() for v in _parse_jsonish_list(market.get("outcomes"))]
    token_ids = [str(v or "").strip() for v in _parse_jsonish_list(market.get("clobTokenIds"))]
    outcome_prices_raw = _parse_jsonish_list(market.get("outcomePrices"))
    outcome_prices: list[float | None] = []
    for val in outcome_prices_raw:
        outcome_prices.append(_as_float(val))

    yes_token_id = ""
    no_token_id = ""
    yes_price = None
    no_price = None

    for idx, outcome in enumerate(outcomes):
        token_id = token_ids[idx] if idx < len(token_ids) else ""
        price = outcome_prices[idx] if idx < len(outcome_prices) else None
        norm_outcome = _norm_text(outcome)
        if norm_outcome in {"yes", "true", "up", "over"}:
            yes_token_id = token_id or yes_token_id
            yes_price = price if price is not None else yes_price
        elif norm_outcome in {"no", "false", "down", "under"}:
            no_token_id = token_id or no_token_id
            no_price = price if price is not None else no_price

    if not yes_token_id and token_ids:
        yes_token_id = token_ids[0]
    if not no_token_id and len(token_ids) > 1:
        no_token_id = token_ids[1]
    if yes_price is None and outcome_prices:
        yes_price = outcome_prices[0]
    if no_price is None and len(outcome_prices) > 1:
        no_price = outcome_prices[1]

    cleaned = {
        "market_id": str(market.get("id") or identifier or title).strip(),
        "market_ticker": identifier or title,
        "market_slug": str(market.get("slug") or "").strip(),
        "market_title": title,
        "question": str(market.get("question") or "").strip(),
        "exchange": "polymarket",
        "status": str(market.get("status") or "active").strip().lower(),
        "start_date": str(market.get("startDateIso") or event.get("startDate") or market.get("start_date") or market.get("endDateIso") or market.get("end_date") or event.get("endDate") or "").strip(),
        "market_start_date": str(market.get("startDateIso") or event.get("startDate") or market.get("start_date") or "").strip(),
        "market_end_date": str(market.get("endDateIso") or market.get("end_date") or event.get("endDate") or "").strip(),
        "market_event_title": str(event.get("title") or "").strip(),
        "market_event_slug": str(event.get("slug") or "").strip(),
        "market_category": str(market.get("category") or market.get("marketCategory") or "").strip(),
        "market_sport": _market_sport_tag(market),
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id,
        "yes_price": yes_price,
        "no_price": no_price,
        "minimum_tick_size": _as_float(market.get("minimum_tick_size")) or 0.01,
        "neg_risk": bool(market.get("neg_risk")),
    }
    cleaned["market_series_ticker"] = _market_series_ticker(cleaned)
    return cleaned


def _fetch_markets_page(offset: int) -> list[dict[str, Any]]:
    params_list = [
        {"limit": POLYMARKET_PAGE_LIMIT, "offset": offset, "active": "true", "closed": "false"},
        {"limit": POLYMARKET_PAGE_LIMIT, "offset": offset, "status": "active"},
        {"limit": POLYMARKET_PAGE_LIMIT, "offset": offset},
    ]
    url = f"{POLYMARKET_BASE_URL}/markets"
    last_error: Exception | None = None
    for params in params_list:
        try:
            resp = requests.get(url, params=params, timeout=POLYMARKET_TIMEOUT_SEC)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return [row for row in data if isinstance(row, dict)]
            if isinstance(data, dict):
                for key in ("markets", "data", "results"):
                    rows = data.get(key)
                    if isinstance(rows, list):
                        return [row for row in rows if isinstance(row, dict)]
            return []
        except Exception as exc:
            last_error = exc
            continue
    if last_error:
        raise last_error
    return []


def _fetch_markets(force_refresh: bool = False) -> list[dict[str, Any]]:
    now = time.time()
    with _MARKET_CACHE_LOCK:
        cache_ts = float(_MARKET_CACHE.get("ts") or 0.0)
        payload = _MARKET_CACHE.get("payload")
        if not force_refresh and payload and (now - cache_ts) < POLYMARKET_MARKET_CACHE_TTL_SEC:
            return list(payload)

    # Fetch all pages concurrently for speed
    import concurrent.futures
    offsets = [page_idx * POLYMARKET_PAGE_LIMIT for page_idx in range(POLYMARKET_MARKET_PAGES)]
    all_rows: list[dict[str, Any]] = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(offsets), 6)) as ex:
            futures = {ex.submit(_fetch_markets_page, off): off for off in offsets}
            for fut in concurrent.futures.as_completed(futures):
                try:
                    page_rows = fut.result()
                    all_rows.extend(page_rows)
                except Exception:
                    pass
    except Exception:
        # Fallback: sequential
        for off in offsets:
            try:
                page_rows = _fetch_markets_page(off)
                if page_rows:
                    all_rows.extend(page_rows)
            except Exception:
                break

    cleaned = [_clean_market(row) for row in all_rows]
    cleaned = [row for row in cleaned if row.get("market_ticker") or row.get("market_title")]
    # Deduplicate by market_id
    seen_ids: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for row in cleaned:
        mid = str(row.get("market_id") or row.get("market_slug") or "")
        if mid and mid in seen_ids:
            continue
        if mid:
            seen_ids.add(mid)
        deduped.append(row)

    with _MARKET_CACHE_LOCK:
        _MARKET_CACHE["ts"] = now
        _MARKET_CACHE["payload"] = deduped
    return deduped


def resolve_ready_bets(bets: list[dict[str, Any]], *, force_refresh: bool = False) -> dict[str, Any]:
    """Match ready-bet rows against open Polymarket markets."""
    clean_bets = [bet for bet in bets or [] if isinstance(bet, dict)]
    markets = _fetch_markets(force_refresh=force_refresh)
    resolutions: dict[str, dict[str, Any]] = {}
    matched = started = done = unavailable = 0

    for idx, bet in enumerate(clean_bets):
        uid = str(bet.get("uid") or bet.get("bet_uid") or bet.get("prediction_uid") or f"ready_{idx}").strip()
        if not uid:
            continue

        best_market: dict[str, Any] | None = None
        best_score = 0.0
        second_best_score = 0.0
        for market in markets:
            score = _score_market(bet, market)
            if score > best_score:
                second_best_score = best_score
                best_score = score
                best_market = market
            elif score > second_best_score:
                second_best_score = score

        sport_tag = _bet_sport_tag(bet)
        primary_threshold = 3.4
        relaxed_threshold = 3.05
        ambiguity_gap = 0.85
        if sport_tag in {"soccer", "tennis", "combat", "golf", "motorsports", "cricket"}:
            primary_threshold = 3.15
            relaxed_threshold = 2.85
            ambiguity_gap = 0.75
        is_match = bool(best_market and best_score >= primary_threshold)
        if not is_match and best_market and best_score >= relaxed_threshold:
            # Guardrail: only relax when the top market is clearly better than runner-up.
            if (best_score - second_best_score) >= ambiguity_gap:
                is_match = True

        if is_match:
            matched += 1
            side = _market_side(bet, best_market)
            token_id = str(best_market.get("yes_token_id") if side == "yes" else best_market.get("no_token_id") or "").strip()
            price = _as_float(best_market.get("yes_price") if side == "yes" else best_market.get("no_price"))
            scheduled_start = best_market.get("market_start_date") or best_market.get("start_date") or best_market.get("market_end_date") or ""
            resolutions[uid] = {
                "uid": uid,
                "status": "matched",
                "exchange": "polymarket",
                "market_ticker": best_market.get("market_ticker") or "",
                "market_title": best_market.get("market_title") or "",
                "market_slug": best_market.get("market_slug") or "",
                "market_id": best_market.get("market_id") or "",
                "market_event_title": best_market.get("market_event_title") or "",
                "market_event_slug": best_market.get("market_event_slug") or "",
                "market_sport": best_market.get("market_sport") or "",
                "series_ticker": best_market.get("market_series_ticker") or "",
                "side": side,
                "token_id": token_id,
                "yes_token_id": best_market.get("yes_token_id") or "",
                "no_token_id": best_market.get("no_token_id") or "",
                "price": price,
                "minimum_tick_size": best_market.get("minimum_tick_size") or 0.01,
                "neg_risk": bool(best_market.get("neg_risk")),
                "scheduled_start": scheduled_start,
                "score": round(best_score, 3),
            }
            continue

        bet_dt = _bet_start_dt(bet)
        if bet_dt and bet_dt < datetime.datetime.now(datetime.timezone.utc):
            done += 1
            status = "done"
            message = "Event already started or passed."
        else:
            unavailable += 1
            status = "unavailable"
            message = "No exact Polymarket market found."
        resolutions[uid] = {
            "uid": uid,
            "status": status,
            "exchange": "polymarket",
            "market_ticker": "",
            "market_title": "",
            "market_slug": "",
            "market_id": "",
            "market_event_title": "",
            "market_event_slug": "",
            "market_sport": "",
            "series_ticker": "",
            "side": "yes",
            "token_id": "",
            "message": message,
        }

    return {
        "exchange": "polymarket",
        "count": len(clean_bets),
        "matched": matched,
        "started": started,
        "done": done,
        "unavailable": unavailable,
        "market_count": len(markets),
        "summary": {
            "exchange": "polymarket",
            "count": len(clean_bets),
            "matched": matched,
            "started": started,
            "done": done,
            "unavailable": unavailable,
            "market_count": len(markets),
        },
        "resolutions": resolutions,
    }


def attach_polymarket_to_bets(
    bets: list[dict[str, Any]],
    *,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    """Enrich bet rows with best-matching Polymarket market metadata."""
    if not bets:
        return bets
    try:
        result = resolve_ready_bets(list(bets), force_refresh=force_refresh)
        resolutions: dict[str, dict[str, Any]] = result.get("resolutions") or {}
        enriched: list[dict[str, Any]] = []
        for i, bet in enumerate(bets):
            if not isinstance(bet, dict):
                enriched.append(bet)
                continue
            uid = str(
                bet.get("uid")
                or bet.get("bet_uid")
                or bet.get("prediction_uid")
                or f"ready_{i}"
            ).strip()
            res = resolutions.get(uid) or {}
            eb = dict(bet)
            raw_series = str(res.get("series_ticker") or "")
            sport_tag = _bet_sport_tag(eb)
            kalshi_series = str(eb.get("kalshi_series_ticker") or "")
            aligned_series = raw_series
            series_match = None
            if sport_tag == "golf":
                k_family = _golf_series_family(kalshi_series)
                p_family = _golf_series_family(raw_series)
                if k_family and p_family:
                    series_match = k_family == p_family
                    if not series_match:
                        aligned_series = k_family
                elif k_family and not p_family:
                    aligned_series = k_family
                    series_match = True
                elif p_family:
                    series_match = True
            elif sport_tag == "tennis":
                k_family = _tennis_series_family(kalshi_series)
                p_family = _tennis_series_family(raw_series)
                if k_family and p_family:
                    series_match = k_family == p_family
                    if not series_match:
                        aligned_series = k_family
                elif k_family and not p_family:
                    aligned_series = k_family
                    series_match = True
                elif p_family:
                    series_match = True
            elif sport_tag == "combat":
                k_family = _combat_series_family(kalshi_series)
                p_family = _combat_series_family(raw_series)
                if k_family and p_family:
                    series_match = k_family == p_family
                    if not series_match:
                        aligned_series = k_family
                elif k_family and not p_family:
                    aligned_series = k_family
                    series_match = True
                elif p_family:
                    series_match = True
            eb["polymarket_ticker"] = str(res.get("market_ticker") or "")
            eb["polymarket_market_slug"] = str(res.get("market_slug") or "")
            eb["polymarket_event_slug"] = str(res.get("market_event_slug") or "")
            eb["polymarket_series_ticker_raw"] = raw_series
            eb["polymarket_series_ticker"] = aligned_series
            if series_match is not None:
                eb["polymarket_series_match"] = bool(series_match)
            eb["polymarket_side"] = str(res.get("side") or "")
            eb["polymarket_price"] = _as_float(res.get("price"))
            eb["polymarket_status"] = str(res.get("status") or "unavailable")
            enriched.append(eb)
        return enriched
    except Exception:
        return [dict(b) if isinstance(b, dict) else b for b in bets]


def _get_clob_client():
    if not POLYMARKET_PRIVATE_KEY:
        raise RuntimeError(
            "POLYMARKET_PRIVATE_KEY is not configured. "
            "Polymarket order signing requires a wallet private key. "
            "If you only have API key/secret, add POLYMARKET_API_PASSPHRASE and a signing wallet key/funder."
        )
    if POLYMARKET_SIGNATURE_TYPE in {1, 2, 3} and not POLYMARKET_FUNDER:
        raise RuntimeError("POLYMARKET_FUNDER is required for signature types 1/2/3.")

    try:
        from py_clob_client.client import ClobClient
    except Exception as exc:
        raise RuntimeError("py-clob-client is not installed. Add it to requirements and install dependencies.") from exc

    kwargs: dict[str, Any] = {
        "key": POLYMARKET_PRIVATE_KEY,
        "chain_id": POLYMARKET_CHAIN_ID,
        "signature_type": POLYMARKET_SIGNATURE_TYPE,
    }
    if POLYMARKET_FUNDER:
        kwargs["funder"] = POLYMARKET_FUNDER
    client = ClobClient(POLYMARKET_CLOB_HOST, **kwargs)
    client.set_api_creds(client.create_or_derive_api_creds())
    return client


def _coerce_usd(value: Any) -> float | None:
    num = _as_float(value)
    if num is None:
        return None
    if num > 1_000_000:
        return num / 1_000_000.0
    return num


def _normalize_px_value(value: Any) -> float | None:
    px = _as_float(value)
    if px is None:
        return None
    if px > 1.0 and px <= 100.0:
        return px / 100.0
    return px


def get_market_bbo(market_slug: str) -> dict[str, Any]:
    """Fetch lightweight market price snapshot for a market slug."""
    slug = str(market_slug or "").strip()
    if not slug:
        raise RuntimeError("market_slug is required.")

    encoded_slug = urllib.parse.quote(slug, safe="")
    resp = requests.get(f"{POLYMARKET_GATEWAY_BASE}/v1/markets/{encoded_slug}/bbo", timeout=POLYMARKET_TIMEOUT_SEC)
    resp.raise_for_status()
    payload = resp.json() if resp.content else {}
    market_data = payload.get("marketData") if isinstance(payload, dict) else {}
    market_data = market_data if isinstance(market_data, dict) else {}

    current_px = _normalize_px_value(((market_data.get("currentPx") or {}).get("value") if isinstance(market_data.get("currentPx"), dict) else market_data.get("currentPx")))
    best_bid = _normalize_px_value(((market_data.get("bestBid") or {}).get("value") if isinstance(market_data.get("bestBid"), dict) else market_data.get("bestBid")))
    best_ask = _normalize_px_value(((market_data.get("bestAsk") or {}).get("value") if isinstance(market_data.get("bestAsk"), dict) else market_data.get("bestAsk")))
    last_sample = market_data.get("lastPriceSample") if isinstance(market_data.get("lastPriceSample"), dict) else {}
    long_px = _normalize_px_value(((last_sample.get("longPx") or {}).get("value") if isinstance(last_sample.get("longPx"), dict) else last_sample.get("longPx")))
    short_px = _normalize_px_value(((last_sample.get("shortPx") or {}).get("value") if isinstance(last_sample.get("shortPx"), dict) else last_sample.get("shortPx")))

    return {
        "ok": True,
        "market_slug": slug,
        "current_px": current_px,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "long_px": long_px,
        "short_px": short_px,
        "raw": payload,
    }


def get_order(order_id: str) -> dict[str, Any]:
    """Fetch a Polymarket order by exchange order id."""
    oid = str(order_id or "").strip()
    if not oid:
        raise RuntimeError("order_id is required.")
    payload = _us_request("GET", f"/v1/order/{oid}")
    order = payload.get("order") if isinstance(payload, dict) and isinstance(payload.get("order"), dict) else {}
    return {"ok": True, "order_id": oid, "order": order, "raw": payload}


def close_position_order(
    *,
    market_slug: str,
    synchronous_execution: bool = False,
    max_block_time: int = 5,
    slippage_bips: int = 50,
) -> dict[str, Any]:
    """Close an existing position in a market using Polymarket close-position API."""
    slug = str(market_slug or "").strip()
    if not slug:
        raise RuntimeError("market_slug is required.")

    body: dict[str, Any] = {
        "marketSlug": slug,
        "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
        "synchronousExecution": bool(synchronous_execution),
    }
    if synchronous_execution:
        body["maxBlockTime"] = str(max(1, int(max_block_time or 5)))
    if slippage_bips and int(slippage_bips) > 0:
        body["slippageTolerance"] = {"bips": int(slippage_bips)}

    response = _us_request("POST", "/v1/order/close-position", payload=body)
    return {
        "ok": True,
        "market_slug": slug,
        "close_order_id": str(response.get("id") or "").strip(),
        "response": response,
    }


def get_balance() -> dict[str, Any]:
    """Return Polymarket balance in USD.

    Prefers Polymarket US authenticated API when key/secret are configured.
    """
    global _BALANCE_COOLDOWN_UNTIL

    now = time.time()
    with _BALANCE_CACHE_LOCK:
        cached_payload = dict(_BALANCE_CACHE.get("payload") or {})
        cached_ts = float(_BALANCE_CACHE.get("ts") or 0.0)

    if now < _BALANCE_COOLDOWN_UNTIL and cached_payload:
        cached_payload["ok"] = False
        cached_payload["source"] = "polymarket_us_cached"
        cached_payload["cached"] = True
        cached_payload["cooldown_active"] = True
        cached_payload["cache_age_sec"] = round(now - cached_ts, 2)
        return cached_payload

    if _has_us_api_creds():
        try:
            payload = _us_request("GET", "/v1/account/balances")
            balances = payload.get("balances") if isinstance(payload, dict) else None
            balances = balances if isinstance(balances, list) else []
            usd_row = next(
                (
                    row for row in balances
                    if isinstance(row, dict) and str(row.get("currency") or "").upper() == "USD"
                ),
                (balances[0] if balances and isinstance(balances[0], dict) else {}),
            )
            current_balance = float(_as_float((usd_row or {}).get("currentBalance")) or 0.0)
            buying_power = float(_as_float((usd_row or {}).get("buyingPower")) or current_balance)
            asset_notional = float(_as_float((usd_row or {}).get("assetNotional")) or 0.0)
            portfolio_value = current_balance + asset_notional
            result = {
                "ok": True,
                "balance_usd": round(current_balance, 6),
                "buying_power_usd": round(buying_power, 6),
                "portfolio_usd": round(portfolio_value, 6),
                "raw": payload,
                "source": "polymarket_us",
            }
            with _BALANCE_CACHE_LOCK:
                _BALANCE_CACHE["ts"] = time.time()
                _BALANCE_CACHE["payload"] = dict(result)
            return result
        except Exception as exc:
            msg = str(exc)
            if any(token in msg.lower() for token in ("rate limited", "cloudflare", "error 1015", "429")):
                _BALANCE_COOLDOWN_UNTIL = time.time() + POLYMARKET_US_COOLDOWN_SEC
                fallback = {
                    "ok": False,
                    "balance_usd": float(cached_payload.get("balance_usd") or 0.0),
                    "buying_power_usd": float(cached_payload.get("buying_power_usd") or cached_payload.get("balance_usd") or 0.0),
                    "portfolio_usd": float(cached_payload.get("portfolio_usd") or cached_payload.get("balance_usd") or 0.0),
                    "raw": {"error": msg},
                    "source": "polymarket_us_cached",
                    "cached": bool(cached_payload),
                    "cooldown_active": True,
                    "cooldown_sec": POLYMARKET_US_COOLDOWN_SEC,
                    "message": "Polymarket US balance is rate limited; using cached balance if available.",
                }
                if cached_payload:
                    fallback.update({k: v for k, v in cached_payload.items() if k not in {"ok", "raw", "source"}})
                return fallback
            raise

    # Legacy CLOB fallback path.
    client = _get_clob_client()

    candidates: list[Any] = []
    try:
        if hasattr(client, "get_balance_allowance"):
            try:
                candidates.append(client.get_balance_allowance({"asset_type": "COLLATERAL"}))
            except Exception:
                candidates.append(client.get_balance_allowance())
    except Exception:
        pass

    if hasattr(client, "get_balance"):
        try:
            candidates.append(client.get_balance())
        except Exception:
            pass

    if hasattr(client, "get_collateral"):
        try:
            candidates.append(client.get_collateral())
        except Exception:
            pass

    payload = next((c for c in candidates if c is not None), {})
    if not isinstance(payload, dict):
        payload = {"raw": payload}

    balance_usd = None
    for key in (
        "balance",
        "available",
        "available_balance",
        "availableBalance",
        "collateral",
        "total",
        "amount",
    ):
        if key in payload:
            balance_usd = _coerce_usd(payload.get(key))
            if balance_usd is not None:
                break

    nested = payload.get("balance") if isinstance(payload.get("balance"), dict) else None
    if balance_usd is None and isinstance(nested, dict):
        for key in ("available", "total", "amount"):
            balance_usd = _coerce_usd(nested.get(key))
            if balance_usd is not None:
                break

    return {
        "ok": balance_usd is not None,
        "balance_usd": round(float(balance_usd or 0.0), 6),
        "portfolio_usd": round(float(balance_usd or 0.0), 6),
        "raw": payload,
        "source": "clob_legacy",
    }


def place_order(
    *,
    market_slug: str,
    amount_usd: float,
    side: str = "yes",
    price: float | None = None,
    order_type: str = "ORDER_TYPE_MARKET",
) -> dict[str, Any]:
    """Place a Polymarket order.

    Uses Polymarket US authenticated API when key/secret are configured.
    """
    slug = str(market_slug or "").strip()
    if not slug:
        raise RuntimeError("market_slug is required to place a Polymarket order.")

    usd = float(amount_usd or 0.0)
    if usd <= 0:
        raise RuntimeError("amount_usd must be > 0.")

    side_norm = str(side or "yes").strip().lower()
    side_norm = "yes" if side_norm not in {"yes", "no"} else side_norm
    limit_price = float(price if price is not None else 0.50)
    limit_price = max(0.01, min(limit_price, 0.99))

    if _has_us_api_creds():
        req_type = str(order_type or "ORDER_TYPE_MARKET").strip().upper()
        if req_type not in {"ORDER_TYPE_LIMIT", "ORDER_TYPE_MARKET"}:
            req_type = "ORDER_TYPE_MARKET"

        body: dict[str, Any] = {
            "marketSlug": slug,
            "type": req_type,
            "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_MANUAL",
            "outcomeSide": "OUTCOME_SIDE_YES" if side_norm == "yes" else "OUTCOME_SIDE_NO",
            "action": "ORDER_ACTION_BUY",
        }
        if req_type == "ORDER_TYPE_LIMIT":
            body["price"] = {"value": _as_decimal_str(limit_price, default="0.50"), "currency": "USD"}
            body["quantity"] = float(max(1.0, round(usd, 8)))
            body["tif"] = "TIME_IN_FORCE_GOOD_TILL_CANCEL"
        else:
            body["cashOrderQty"] = {"value": _as_decimal_str(usd, default="1.00"), "currency": "USD"}
            body["tif"] = "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"

        response = _us_request("POST", "/v1/orders", payload=body)
        return {
            "ok": True,
            "exchange": "polymarket",
            "market_slug": slug,
            "side": side_norm,
            "amount_usd": usd,
            "limit_price": limit_price,
            "response": response,
            "source": "polymarket_us",
        }

    # Legacy CLOB fallback path.
    token = str(os.getenv("POLYMARKET_TOKEN_ID", "") or "").strip()
    if not token:
        raise RuntimeError("POLYMARKET_TOKEN_ID is required for legacy CLOB order fallback.")
    client = _get_clob_client()
    try:
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY
    except Exception as exc:
        raise RuntimeError("py-clob-client order types are unavailable. Verify py-clob-client installation.") from exc

    order_type_val = getattr(OrderType, str(order_type or "FOK").upper(), None) or OrderType.FOK
    try:
        mo = MarketOrderArgs(token_id=token, amount=float(usd), side=BUY, order_type=order_type_val, price=limit_price)
    except TypeError:
        try:
            mo = MarketOrderArgs(token_id=token, amount=float(usd), side=BUY, order_type=order_type_val)
        except TypeError:
            mo = MarketOrderArgs(token_id=token, amount=float(usd), side=BUY)
    signed = client.create_market_order(mo)
    response = client.post_order(signed, order_type_val)
    if not isinstance(response, dict):
        response = {"raw": response}

    return {
        "ok": bool(response.get("success", True)),
        "exchange": "polymarket",
        "market_slug": slug,
        "token_id": token,
        "side": side_norm,
        "amount_usd": usd,
        "limit_price": limit_price,
        "response": response,
        "source": "clob_legacy",
    }
