"""Alpaca Trading + Market Data API client -- simple API-key/secret auth,
unlike Schwab's OAuth 2.0 authorization-code flow. A static
ALPACA_API_KEY_ID + ALPACA_API_SECRET_KEY pair (generated once from the
Alpaca dashboard) authenticates every call directly via headers -- there's
no human login step, no authorization code, no access/refresh token pair,
and therefore no durable token storage on HF needed either. is_configured()
is the entire "is this account usable" check.

Two separate base URLs (confirmed via Alpaca's own docs):
  ALPACA_TRADING_BASE_URL -- account/positions/orders/assets/clock. Paper
                             by default (https://paper-api.alpaca.markets);
                             a real funded account switches this to
                             https://api.alpaca.markets and swaps the key
                             pair for a live one -- nothing else in this
                             module changes.
  ALPACA_DATA_BASE_URL    -- market data (https://data.alpaca.markets),
                             identical for paper and live per Alpaca's docs
                             ("Market Data API works identically").
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

TRADING_BASE_URL = os.getenv("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets").rstrip("/")
DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
API_KEY_ID = os.getenv("ALPACA_API_KEY_ID", "")
API_SECRET_KEY = os.getenv("ALPACA_API_SECRET_KEY", "")
# Alpaca's free market-data plan only entitles the IEX feed -- requesting
# "sip" (full consolidated tape) without the paid subscription is a real
# 403, so IEX is the safe default; a real account with that add-on can
# override this via env var.
DATA_FEED = os.getenv("ALPACA_DATA_FEED", "iex")
TIMEOUT_SEC = 15


def is_configured() -> bool:
    """True once both credentials are set. Unlike Schwab there's no
    interactive login step -- this IS the whole "logged in" check, and it
    never changes at runtime (no token to expire/refresh)."""
    return bool(API_KEY_ID and API_SECRET_KEY)


def _auth_headers() -> dict[str, str]:
    if not is_configured():
        raise RuntimeError(
            "Alpaca API key/secret not configured -- set ALPACA_API_KEY_ID "
            "and ALPACA_API_SECRET_KEY."
        )
    return {"APCA-API-KEY-ID": API_KEY_ID, "APCA-API-SECRET-KEY": API_SECRET_KEY}


# Alpaca's own free-tier ceiling is 200 req/min (Basic plan) -- a 429 here is
# expected occasionally under normal load (not just a bug symptom), so it's
# worth a short retry instead of failing the whole caller outright. Respects
# Retry-After when Alpaca sends one; otherwise backs off a fixed short
# interval. Mirrors schwab_client.py's own identical retry shape.
_RATE_LIMIT_RETRY_ATTEMPTS = int(os.getenv("ALPACA_RATE_LIMIT_RETRY_ATTEMPTS", "3") or "3")
_RATE_LIMIT_RETRY_BACKOFF_SEC = float(os.getenv("ALPACA_RATE_LIMIT_RETRY_BACKOFF_SEC", "2.0") or "2.0")


def _get_with_retry(url: str, *, headers: dict[str, str], params: dict[str, Any]) -> requests.Response:
    last_resp: requests.Response | None = None
    for attempt in range(_RATE_LIMIT_RETRY_ATTEMPTS):
        resp = requests.get(url, headers=headers, params=params, timeout=TIMEOUT_SEC)
        if resp.status_code != 429:
            return resp
        last_resp = resp
        if attempt == _RATE_LIMIT_RETRY_ATTEMPTS - 1:
            break
        retry_after = resp.headers.get("Retry-After")
        wait_sec = float(retry_after) if retry_after and retry_after.isdigit() else _RATE_LIMIT_RETRY_BACKOFF_SEC * (attempt + 1)
        time.sleep(wait_sec)
    return last_resp


def _trading_get(path: str, *, params: dict[str, Any] | None = None) -> Any:
    resp = _get_with_retry(f"{TRADING_BASE_URL}{path}", headers=_auth_headers(), params=params or {})
    resp.raise_for_status()
    return resp.json() if resp.content else {}


def _trading_post(path: str, *, json_body: dict[str, Any]) -> dict[str, Any]:
    resp = requests.post(
        f"{TRADING_BASE_URL}{path}", headers={**_auth_headers(), "Content-Type": "application/json"},
        json=json_body, timeout=TIMEOUT_SEC,
    )
    resp.raise_for_status()
    return resp.json() if resp.content else {}


def _trading_delete(path: str) -> dict[str, Any]:
    resp = requests.delete(f"{TRADING_BASE_URL}{path}", headers=_auth_headers(), timeout=TIMEOUT_SEC)
    resp.raise_for_status()
    return resp.json() if resp.content else {}


def _data_get(path: str, *, params: dict[str, Any] | None = None) -> Any:
    resp = _get_with_retry(f"{DATA_BASE_URL}{path}", headers=_auth_headers(), params=params or {})
    resp.raise_for_status()
    return resp.json() if resp.content else {}


def get_account() -> dict[str, Any]:
    """Real cash/buying_power/equity for the linked account -- used only in
    "live" mode; "simulate" mode tracks its own virtual balance in local
    state instead (see alpaca_strategy.py)."""
    return _trading_get("/v2/account")


def get_clock() -> dict[str, Any]:
    """{"timestamp", "is_open", "next_open", "next_close"} -- Alpaca's own
    authoritative market-hours endpoint (accounts for holidays natively),
    replacing Schwab's markets endpoint + hand-computed ET fallback for the
    regular-session open/closed boundary specifically."""
    return _trading_get("/v2/clock")


def get_positions() -> list[dict[str, Any]]:
    result = _trading_get("/v2/positions")
    return result if isinstance(result, list) else []


def get_position(symbol: str) -> dict[str, Any] | None:
    """None (not an error) if there is no open position in `symbol` --
    Alpaca returns 404 in that case."""
    try:
        return _trading_get(f"/v2/positions/{symbol}")
    except requests.exceptions.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return None
        raise


def close_position(symbol: str) -> dict[str, Any]:
    """Liquidates the entire position in `symbol` at market and cancels any
    of its still-open bracket child orders (take-profit/stop-loss)
    automatically -- Alpaca's dedicated endpoint for a forced early exit
    (e.g. max-hold-time), simpler and safer than Schwab's cancel-then-
    place-a-new-market-sell dance (which risked a double-sell if the
    bracket's own TP/SL had already filled moments earlier)."""
    return _trading_delete(f"/v2/positions/{symbol}")


def get_assets(*, status: str = "active", asset_class: str = "us_equity") -> list[dict[str, Any]]:
    """The tradable universe straight from Alpaca itself. Unlike Schwab
    (which has no bulk symbol-listing endpoint and needed NASDAQ's own free
    public symbol directory as a workaround), Alpaca's /v2/assets IS the
    authoritative list of what this account can actually trade -- a better
    semantic fit, not just a convenience."""
    result = _trading_get("/v2/assets", params={"status": status, "asset_class": asset_class})
    return result if isinstance(result, list) else []


_MAX_BAR_PAGES = 20  # defensive cap -- a live API should never actually loop this many times


def get_bars(
    symbols: list[str], *, timeframe: str = "1Min", start: str | None = None,
    end: str | None = None, limit: int = 10000, feed: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Historical OHLCV bars for one or MORE symbols in a single call
    (comma-separated `symbols`) -- a real structural advantage over
    Schwab's per-symbol-only pricehistory endpoint. Returns
    {"SYMBOL": [bar, ...], ...}; a symbol with no bars in range is simply
    absent from the dict, not an error. Paginates internally via
    next_page_token so callers never have to."""
    params: dict[str, Any] = {
        "symbols": ",".join(symbols), "timeframe": timeframe, "limit": limit,
        "feed": feed or DATA_FEED, "adjustment": "raw",
    }
    if start:
        params["start"] = start
    if end:
        params["end"] = end

    bars_by_symbol: dict[str, list[dict[str, Any]]] = {}
    page_token = None
    for _ in range(_MAX_BAR_PAGES):
        if page_token:
            params["page_token"] = page_token
        data = _data_get("/v2/stocks/bars", params=params)
        for symbol, bars in (data.get("bars") or {}).items():
            bars_by_symbol.setdefault(symbol, []).extend(bars)
        page_token = data.get("next_page_token")
        if not page_token:
            break
    return bars_by_symbol


def get_latest_quote(symbol: str) -> dict[str, Any]:
    """A single real-time-ish bid/ask quote -- much lighter than pulling a
    bars window just to read the current price, used by the fast
    position-management poll (mirrors Schwab's get_quote)."""
    data = _data_get(f"/v2/stocks/{symbol}/quotes/latest", params={"feed": DATA_FEED})
    return data.get("quote") or {}


# ---------------------------------------------------------------------------
# Order placement. Alpaca supports bracket orders NATIVELY in a single
# order submission (order_class="bracket") -- simpler than Schwab's
# TRIGGER+OCO nested child-order structure, and Alpaca itself keeps the
# take-profit/stop-loss legs live on the exchange the instant the entry
# fills, same safety property Schwab's OCO gave.
# ---------------------------------------------------------------------------
def build_bracket_order(
    *, symbol: str, quantity: float, side: str, take_profit_price: float,
    stop_loss_price: float, stop_loss_limit_price: float | None = None,
) -> dict[str, Any]:
    """side: "buy" (open long) or "sell" (open short, needs margin
    approval -- unused by the long-only strategy this ships with).
    Bracket orders require WHOLE-share quantities (Alpaca does not support
    fractional-share bracket/OCO orders), so `quantity` must already be an
    integer count -- see alpaca_strategy.compute_position_size."""
    return {
        "symbol": symbol, "qty": str(int(quantity)), "side": side, "type": "market",
        "time_in_force": "day", "order_class": "bracket",
        "take_profit": {"limit_price": f"{take_profit_price:.2f}"},
        "stop_loss": {
            "stop_price": f"{stop_loss_price:.2f}",
            "limit_price": f"{(stop_loss_limit_price if stop_loss_limit_price is not None else stop_loss_price):.2f}",
        },
    }


def place_order(order_spec: dict[str, Any]) -> str:
    """Places a real order and returns its order id."""
    resp = _trading_post("/v2/orders", json_body=order_spec)
    order_id = resp.get("id", "")
    if not order_id:
        raise RuntimeError(f"Order placed but no order id in response: {resp!r}")
    return order_id


def get_order(order_id: str) -> dict[str, Any]:
    return _trading_get(f"/v2/orders/{order_id}")


def get_orders(*, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"limit": limit}
    if status:
        params["status"] = status
    result = _trading_get("/v2/orders", params=params)
    return result if isinstance(result, list) else []


def cancel_order(order_id: str) -> None:
    _trading_delete(f"/v2/orders/{order_id}")
