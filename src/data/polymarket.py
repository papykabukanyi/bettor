"""Polymarket market matching and execution helpers.

This module resolves normalized ready-bet rows to open Polymarket markets using
the public Gamma API and can optionally place orders through the CLOB client
when credentials are configured.
"""

from __future__ import annotations

import datetime
import os
import re
import threading
import time
from typing import Any

import requests

POLYMARKET_BASE_URL = os.getenv("POLYMARKET_BASE_URL", "https://gamma-api.polymarket.com").rstrip("/")
POLYMARKET_CLOB_HOST = os.getenv("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com").rstrip("/")
POLYMARKET_CHAIN_ID = int(os.getenv("POLYMARKET_CHAIN_ID", "137") or "137")
POLYMARKET_SIGNATURE_TYPE = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "1") or "1")
POLYMARKET_PRIVATE_KEY = str(os.getenv("POLYMARKET_PRIVATE_KEY", "") or "").strip()
POLYMARKET_FUNDER = str(os.getenv("POLYMARKET_FUNDER", "") or "").strip()
POLYMARKET_API_KEY = str(os.getenv("POLYMARKET_API_KEY", "") or "").strip()
POLYMARKET_API_SECRET = str(os.getenv("POLYMARKET_API_SECRET", "") or "").strip()
POLYMARKET_API_PASSPHRASE = str(os.getenv("POLYMARKET_API_PASSPHRASE", "") or "").strip()
POLYMARKET_TIMEOUT_SEC = int(os.getenv("POLYMARKET_TIMEOUT_SEC", "15"))
POLYMARKET_MARKET_CACHE_TTL_SEC = max(120, int(os.getenv("POLYMARKET_MARKET_CACHE_TTL_SEC", "900") or "900"))
POLYMARKET_MARKET_PAGES = max(1, min(int(os.getenv("POLYMARKET_MARKET_PAGES", "4") or "4"), 20))
POLYMARKET_PAGE_LIMIT = max(50, min(int(os.getenv("POLYMARKET_PAGE_LIMIT", "200") or "200"), 500))

_MARKET_CACHE_LOCK = threading.Lock()
_MARKET_CACHE: dict[str, Any] = {"ts": 0.0, "payload": None}


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
    return ""


def _picked_team_name(bet: dict[str, Any]) -> str:
    pick = _norm_text(" ".join(str(bet.get(key) or "") for key in ("pick", "label")))
    home = str(bet.get("home_team") or "").strip()
    away = str(bet.get("away_team") or "").strip()
    for team in (home, away):
        if team and _norm_text(team) in pick:
            return team
    return str(bet.get("team") or "").strip()


def _entity_match_score(text: str, name: Any) -> float:
    norm = _norm_text(name)
    if not text or not norm:
        return 0.0
    if norm in text:
        return 5.5
    tokens = [tok for tok in norm.split() if len(tok) >= 3]
    if tokens and all(tok in text for tok in tokens):
        return 4.0
    if tokens and any(tok in text for tok in tokens):
        return 2.0
    return 0.0


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
    parts = [
        market.get("title"),
        market.get("question"),
        market.get("description"),
        market.get("subtitle"),
        market.get("event_slug"),
        market.get("slug"),
        market.get("category"),
    ]
    outcomes = market.get("outcomes")
    if isinstance(outcomes, list):
        parts.extend(outcomes)
    return _norm_text(" ".join(str(part or "") for part in parts))


def _market_start_dt(market: dict[str, Any]) -> datetime.datetime | None:
    for key in ("start_date", "close_time", "end_date", "created_at", "updated_at"):
        dt = _parse_iso_dt(market.get(key))
        if dt is not None:
            return dt
    return None


def _time_score(bet: dict[str, Any], market: dict[str, Any]) -> float:
    bet_dt = _bet_start_dt(bet)
    market_dt = _market_start_dt(market)
    if bet_dt is None or market_dt is None:
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
    if any(token in text for token in ("player prop", "points", "rebounds", "assists", "hits", "runs", "strikeouts", "goals", "cards", "corners")):
        return "player_prop"
    if any(token in text for token in ("moneyline", "winner", "match winner", "1x2")):
        return "moneyline"
    if any(token in text for token in ("spread", "handicap")):
        return "spread"
    if any(token in text for token in ("total", "over under", "goals")):
        return "total"
    return "single"


def _market_identifier(market: dict[str, Any]) -> str:
    for key in ("condition_id", "id", "slug", "event_slug"):
        value = str(market.get(key) or "").strip()
        if value:
            return value
    return ""


def _market_side(bet: dict[str, Any], market: dict[str, Any]) -> str:
    direction_text = _norm_text(" ".join(str(bet.get(key) or "") for key in ("direction", "pick", "label", "bet_type")))
    if _bet_kind_tag(bet) == "player_prop":
        return "no" if "under" in direction_text else "yes"
    if any(token in direction_text for token in ("under", "against")):
        return "no"
    return "yes"


def _score_market(bet: dict[str, Any], market: dict[str, Any]) -> float:
    market_text = _market_text(market)
    if not market_text:
        return 0.0

    bet_sport = _bet_sport_tag(bet)
    market_sport = _bet_sport_tag({"sport": market.get("category") or market.get("subtitle") or market.get("title") or ""})
    if bet_sport and market_sport and bet_sport != market_sport:
        return 0.0

    score = _time_score(bet, market)

    kind = _bet_kind_tag(bet)
    market_kind = _market_kind_tag(market)
    if kind == "player_prop":
        if market_kind != "player_prop":
            return 0.0
        player_score = _entity_match_score(market_text, bet.get("player_name") or bet.get("name"))
        if player_score < 2.0:
            return 0.0
        score += player_score * 2.0
        score += max(
            _entity_match_score(market_text, bet.get("team")),
            _entity_match_score(market_text, bet.get("home_team")),
            _entity_match_score(market_text, bet.get("away_team")),
        ) * 0.3
        score += _token_overlap_score(market_text, bet.get("prop_type"), bet.get("bet_type"), bet.get("label"))
        score += _line_match_score(market_text, bet.get("line")) or _line_proximity_score(market_text, bet.get("line"), direction=str(bet.get("direction") or bet.get("pick") or ""))
        return score

    picked = _picked_team_name(bet)
    if kind == "moneyline" and market_kind not in {"moneyline", "single"}:
        return 0.0
    if kind == "spread" and market_kind not in {"spread", "single"}:
        return 0.0
    if kind == "total" and market_kind not in {"total", "single"}:
        return 0.0

    team_score = max(
        _entity_match_score(market_text, bet.get("home_team")),
        _entity_match_score(market_text, bet.get("away_team")),
        _entity_match_score(market_text, bet.get("team")),
        _entity_match_score(market_text, picked),
    )
    if team_score < 1.8 and kind in {"moneyline", "spread"}:
        return 0.0
    score += team_score * 1.4
    score += _token_overlap_score(market_text, bet.get("label"), bet.get("pick"), bet.get("bet_type"), bet.get("prop_type"))
    score += _line_match_score(market_text, bet.get("line")) or _line_proximity_score(market_text, bet.get("line"), direction=str(bet.get("direction") or bet.get("pick") or ""))
    return score


def _clean_market(market: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(market, dict):
        return {}
    title = str(market.get("title") or market.get("question") or market.get("slug") or "").strip()
    identifier = _market_identifier(market)
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

    return {
        "market_id": str(market.get("id") or identifier or title).strip(),
        "market_ticker": identifier or title,
        "market_slug": str(market.get("slug") or "").strip(),
        "market_title": title,
        "question": str(market.get("question") or "").strip(),
        "exchange": "polymarket",
        "status": str(market.get("status") or "active").strip().lower(),
        "start_date": str(market.get("start_date") or market.get("end_date") or "").strip(),
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id,
        "yes_price": yes_price,
        "no_price": no_price,
        "minimum_tick_size": _as_float(market.get("minimum_tick_size")) or 0.01,
        "neg_risk": bool(market.get("neg_risk")),
    }


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

    rows: list[dict[str, Any]] = []
    for page_idx in range(POLYMARKET_MARKET_PAGES):
        page_rows = _fetch_markets_page(page_idx * POLYMARKET_PAGE_LIMIT)
        if not page_rows:
            break
        rows.extend(page_rows)
        if len(page_rows) < POLYMARKET_PAGE_LIMIT:
            break

    cleaned = [_clean_market(row) for row in rows]
    cleaned = [row for row in cleaned if row.get("market_ticker") or row.get("market_title")]
    with _MARKET_CACHE_LOCK:
        _MARKET_CACHE["ts"] = now
        _MARKET_CACHE["payload"] = list(cleaned)
    return cleaned


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
        for market in markets:
            score = _score_market(bet, market)
            if score > best_score:
                best_score = score
                best_market = market

        if best_market and best_score >= 3.4:
            matched += 1
            side = _market_side(bet, best_market)
            token_id = str(best_market.get("yes_token_id") if side == "yes" else best_market.get("no_token_id") or "").strip()
            price = _as_float(best_market.get("yes_price") if side == "yes" else best_market.get("no_price"))
            resolutions[uid] = {
                "uid": uid,
                "status": "matched",
                "exchange": "polymarket",
                "market_ticker": best_market.get("market_ticker") or "",
                "market_title": best_market.get("market_title") or "",
                "market_slug": best_market.get("market_slug") or "",
                "market_id": best_market.get("market_id") or "",
                "side": side,
                "token_id": token_id,
                "yes_token_id": best_market.get("yes_token_id") or "",
                "no_token_id": best_market.get("no_token_id") or "",
                "price": price,
                "minimum_tick_size": best_market.get("minimum_tick_size") or 0.01,
                "neg_risk": bool(best_market.get("neg_risk")),
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


def get_balance() -> dict[str, Any]:
    """Return Polymarket collateral balance in USD when available."""
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
    }


def place_order(
    *,
    token_id: str,
    amount_usd: float,
    side: str = "yes",
    price: float | None = None,
    order_type: str = "FOK",
) -> dict[str, Any]:
    """Place a Polymarket market order by dollar amount."""
    token = str(token_id or "").strip()
    if not token:
        raise RuntimeError("token_id is required to place a Polymarket order.")

    usd = float(amount_usd or 0.0)
    if usd <= 0:
        raise RuntimeError("amount_usd must be > 0.")

    side_norm = str(side or "yes").strip().lower()
    side_norm = "yes" if side_norm not in {"yes", "no"} else side_norm
    limit_price = float(price if price is not None else 0.50)
    limit_price = max(0.01, min(limit_price, 0.99))

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
        "token_id": token,
        "side": side_norm,
        "amount_usd": usd,
        "limit_price": limit_price,
        "response": response,
    }
