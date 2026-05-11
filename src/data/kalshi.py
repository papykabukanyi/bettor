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
import os
import re
import threading
import time
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

# Series ticker prefix → sport (updated per Kalshi documentation)
_SERIES_SPORT_MAP: dict[str, str] = {
    # Baseball (MLB)
    "MLBWIN": "baseball", "MLBOU": "baseball", "MLBHR": "baseball",
    "MLBRUNS": "baseball", "MLBK": "baseball", "MLBHITS": "baseball",
    "MLBWSERIES": "baseball", "KXMLB": "baseball",
    # Basketball (NBA/WNBA)
    "KXNBA": "basketball", "NBAPTSO": "basketball", "NBAMVP": "basketball",
    "NBACHAMP": "basketball", "KXWNBA": "basketball",
    # Football (NFL)
    "NFLWIN": "football", "NFLOU": "football", "NFLTD": "football",
    "NFLMVP": "football", "NFLSB": "football", "KXNFL": "football",
    # Hockey (NHL)
    "NHLWIN": "hockey", "NHLOU": "hockey", "NHLCHAMP": "hockey",
    "NHLIN": "hockey", "KXNHL": "hockey",
    # Soccer
    "MSLWIN": "soccer", "EPLWIN": "soccer", "UCLWIN": "soccer",
    "KXMLS": "soccer", "KXEPL": "soccer", "KXFIFA": "soccer",
}
_KALSHI_MARKET_CACHE_LOCK = threading.Lock()
_KALSHI_MARKET_CACHE: dict[str, Any] = {
    "ts": 0.0,
    "markets": [],
    "combo_markets": [],
    "count": 0,
}

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
_SPORT_DONE_HOURS = {
    "baseball": 4.5,
    "basketball": 3.5,
    "football": 4.5,
    "hockey": 3.25,
    "soccer": 2.5,
}
_SPECIAL_ENTITY_ALIASES: dict[str, set[str]] = {
    "montreal canadiens": {"mtl canadiens", "canadiens"},
    "new york city fc": {"nycfc", "new york city"},
    "philadelphia 76ers": {"76ers", "sixers"},
    "minnesota timberwolves": {"timberwolves", "wolves"},
    "new york knicks": {"knicks"},
    "new york liberty": {"liberty"},
    "las vegas aces": {"aces"},
    "seattle storm": {"storm"},
    "west ham united": {"west ham"},
    "washington mystics": {"mystics"},
    "connecticut sun": {"sun"},
    "los angeles sparks": {"sparks"},
    "columbus crew": {"crew"},
    "arsenal": {"arsenal"},
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
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": max(1, min(int(limit or 200), 500))}
    if cursor:
        params["cursor"] = cursor
    if status:
        params["status"] = status
    if event_ticker:
        params["event_ticker"] = event_ticker
    if series_ticker:
        params["series_ticker"] = series_ticker

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


def _market_time(market: dict[str, Any]) -> datetime.datetime | None:
    for key in ("close_time", "expiration_time", "expected_expiration_time", "open_time"):
        dt = _parse_iso_dt(market.get(key))
        if dt is not None:
            return dt
    return None


def _bet_start_dt(bet: dict[str, Any]) -> datetime.datetime | None:
    candidates = [
        bet.get("scheduled_start"),
        bet.get("start_time"),
        bet.get("game_time"),
        bet.get("game_date"),
    ]
    for key in ("game", "game_key"):
        raw = str(bet.get(key) or "")
        if "#" in raw:
            candidates.append(raw.rsplit("#", 1)[-1])
    for value in candidates:
        dt = _parse_iso_dt(value)
        if dt is not None:
            return dt
    return None


def _bet_identity(bet: dict[str, Any], index: int = 0) -> str:
    for key in ("uid", "bet_uid", "prediction_uid"):
        value = str(bet.get(key) or "").strip()
        if value:
            return value
    return f"ready_{index}"


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
        if len(no_stop) >= 2:
            aliases.add(" ".join(no_stop[-2:]))
    if len(tokens) >= 2:
        aliases.add(" ".join(tokens[-2:]))
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
            score = 5.8 if " " in alias else 2.2
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


def _line_match_score(text: str, line: Any) -> float:
    num = _as_float(line)
    if num is None:
        return 0.0
    candidates = {str(num).rstrip("0").rstrip("."), f"{num:.1f}"}
    if float(num).is_integer():
        candidates.add(str(int(num)))
        candidates.add(f"{int(num)}.0")
    return 2.8 if any(candidate and candidate in text for candidate in candidates) else 0.0


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
    if any(token in text for token in ("football", "nfl")):
        return "football"
    if any(token in text for token in ("hockey", "nhl", "icehockey")):
        return "hockey"
    if any(token in text for token in ("soccer", "mls", "premier", "bundesliga", "serie a", "laliga", "ligue 1", "uefa", "fifa", "1x2", "btts", "goals o u")):
        return "soccer"
    return ""


def _market_sport_tag(market: dict[str, Any]) -> str:
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
    for prefix, sport in _SERIES_SPORT_MAP.items():
        if ticker_upper.startswith(prefix) or event_upper.startswith(prefix):
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
    if any(token in text for token in ("mslwin", "eplwin", "uclwin", "kxmls", "kxepl", "kxfifa", "soccer", "mls", "premier", "bundesliga", "serie a", "laliga", "ligue 1", "uefa", "fifa")):
        return "soccer"
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
    title = _norm_text(market.get("title"))
    event = _norm_text(market.get("event_ticker"))
    if any(hint in text for hint in _PROP_HINTS) or ":" in str(market.get("title") or ""):
        return "player_prop"
    if any(token in text for token in ("spread", "run line")) or "spread" in event:
        return "spread"
    if any(token in text for token in ("total", "over", "under", "btts")) or "total" in event:
        return "total"
    if any(token in text for token in ("wins", "to win", "moneyline")) or "match" in event:
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

    time_score = _time_match_score(bet, market)
    if time_score < -1.5:
        return 0.0

    if bet_kind == "player_prop":
        player_score = _entity_match_score(text, bet.get("player_name") or bet.get("name"))
        if player_score < 3.0:
            return 0.0
        score = player_score * 1.8
        score += _entity_match_score(text, bet.get("team")) * 0.6
        score += _token_overlap_score(
            text,
            bet.get("prop_type"),
            bet.get("direction"),
            bet.get("label"),
        )
        score += _line_match_score(text, bet.get("line"))
        score += time_score
        return score

    pick_score = max(
        _entity_match_score(text, bet.get("team")),
        _entity_match_score(text, bet.get("pick")),
        _entity_match_score(text, bet.get("label")),
    )
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

    if bet_kind in {"total", "team_total", "spread"}:
        score += _line_match_score(text, bet.get("line"))
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
                "price_cents": _market_price_cents(market, side),
                "close_time": str(market.get("close_time") or market.get("expiration_time") or ""),
            }
        )
    return payload


def _resolve_single_bet(bet: dict[str, Any], markets: list[dict[str, Any]]) -> dict[str, Any]:
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

    best_market: dict[str, Any] | None = None
    best_score = 0.0
    for market in markets:
        score = _score_single_market(bet, market)
        if score > best_score:
            best_market = market
            best_score = score

    if best_market is None or best_score < 10.0:
        return _single_resolution_payload(
            "unavailable",
            message="No exact Kalshi market is open for this bet.",
            scheduled_start=schedule.get("scheduled_start"),
        )

    side = "no" if str(bet.get("side_default") or "yes").lower() == "no" else "yes"
    return _single_resolution_payload(
        "matched",
        message="Matched to a live Kalshi market.",
        scheduled_start=schedule.get("scheduled_start"),
        market=best_market,
        side=side,
        score=best_score,
    )


def _resolve_combo_bet(
    bet: dict[str, Any],
    markets: list[dict[str, Any]],
    combo_markets: list[dict[str, Any]],
    single_cache: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    legs = [leg for leg in (bet.get("legs") or []) if isinstance(leg, dict)]
    if len(legs) < 2:
        return _single_resolution_payload("unavailable", message="Combo needs at least 2 legs.")

    resolved_legs: list[dict[str, Any]] = []
    for leg in legs:
        sig = _bet_signature(leg)
        if sig not in single_cache:
            single_cache[sig] = _resolve_single_bet(leg, markets)
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

    target_legs = {
        (
            str(leg.get("market_ticker") or ""),
            str(leg.get("side") or "yes").lower(),
        )
        for leg in resolved_legs
    }

    exact_matches: list[dict[str, Any]] = []
    for market in combo_markets:
        selected = market.get("mve_selected_legs") or []
        market_legs = {
            (
                str(leg.get("market_ticker") or ""),
                str(leg.get("side") or "yes").lower(),
            )
            for leg in selected
            if isinstance(leg, dict)
        }
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


def get_open_market_catalog(*, force_refresh: bool = False) -> dict[str, Any]:
    now = time.time()
    with _KALSHI_MARKET_CACHE_LOCK:
        age = now - float(_KALSHI_MARKET_CACHE.get("ts") or 0.0)
        if (
            not force_refresh
            and _KALSHI_MARKET_CACHE.get("markets")
            and age < _KALSHI_MARKET_CACHE_TTL_SEC
        ):
            return {
                "markets": list(_KALSHI_MARKET_CACHE.get("markets") or []),
                "combo_markets": list(_KALSHI_MARKET_CACHE.get("combo_markets") or []),
                "count": int(_KALSHI_MARKET_CACHE.get("count") or 0),
                "cache_age_sec": age,
            }

    markets: list[dict[str, Any]] = []
    cursor: str | None = None
    pages = 0
    while True:
        pages += 1
        data = list_markets(limit=500, cursor=cursor, status="open")
        rows = data.get("markets") or []
        markets.extend(row for row in rows if isinstance(row, dict))
        cursor = data.get("cursor")
        if not cursor or pages >= 60 or len(markets) >= 25000:
            break

    combo_markets = [market for market in markets if _is_combo_market(market)]
    with _KALSHI_MARKET_CACHE_LOCK:
        _KALSHI_MARKET_CACHE.update(
            {
                "ts": time.time(),
                "markets": markets,
                "combo_markets": combo_markets,
                "count": len(markets),
            }
        )

    return {
        "markets": list(markets),
        "combo_markets": list(combo_markets),
        "count": len(markets),
        "cache_age_sec": 0.0,
    }


def resolve_ready_bets(
    bets: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    force_refresh: bool = False,
) -> dict[str, Any]:
    catalog = get_open_market_catalog(force_refresh=force_refresh)
    markets = [market for market in catalog["markets"] if not _is_combo_market(market)]
    combo_markets = list(catalog["combo_markets"])

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

    for index, bet in enumerate(bets or []):
        if not isinstance(bet, dict):
            continue
        uid = _bet_identity(bet, index)
        if _bet_kind_tag(bet) == "combo":
            result = _resolve_combo_bet(bet, markets, combo_markets, single_cache)
        else:
            sig = _bet_signature(bet)
            if sig not in single_cache:
                single_cache[sig] = _resolve_single_bet(bet, markets)
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
        if close_dt is None:
            continue
        if close_dt.date() < today_utc:
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

