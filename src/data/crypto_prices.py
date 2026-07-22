"""Live crypto price cross-check, independent of Kalshi's own quote.

Live-tested (2026-07-22) against all 16 Kalshi perp underlyings before
picking these sources:
  1. Coinbase Exchange public ticker (api.exchange.coinbase.com) -- no key,
     no auth, full 16/16 coverage including HYPE, 10 req/s steady / 15
     burst per IP.
  2. Kraken public ticker (api.kraken.com) -- no key, full 16/16 coverage.
  3. API Ninjas (requires API_NINJAS_API_KEY) -- last-resort fallback ONLY.
     The free/non-premium tier this key uses returns prices delayed by
     roughly 15 minutes (confirmed: this key gets a 403 on API Ninjas' own
     premium-only /v1/cryptosymbols endpoint, and their docs state
     non-premium cryptoprice responses are the delayed feed). This is
     deliberately never used to decide entry/exit timing -- only as a
     sanity check when both real-time sources are down, and every quote
     from it is flagged `delayed: True` so callers can tell.

Binance was evaluated and deliberately excluded: its main domain
(api.binance.com) geo-blocks US IPs (HTTP 451), and even via its official
market-data mirror (data-api.binance.vision) it does not list Hyperliquid's
HYPE at all (it lists an unrelated token called "HYPER" instead). Coinbase +
Kraken alone already give clean, verified, no-key coverage of all 16
instruments without either problem, so a third exchange wasn't worth the
extra complexity.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

_TIMEOUT_SEC = 5
_CACHE_TTL_SEC = 5
_cache: dict[str, tuple[dict[str, Any], float]] = {}

API_NINJAS_API_KEY = os.getenv("API_NINJAS_API_KEY", "")

# Coinbase/Kraken/API-Ninjas symbol formats, keyed by the same short coin
# symbol perps_data.coin_for_ticker() produces (BTC, ETH, SOL, ...).
_COINBASE_PRODUCTS = {
    "BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD", "XRP": "XRP-USD",
    "DOGE": "DOGE-USD", "LTC": "LTC-USD", "BCH": "BCH-USD", "LINK": "LINK-USD",
    "SUI": "SUI-USD", "NEAR": "NEAR-USD", "DOT": "DOT-USD", "HBAR": "HBAR-USD",
    "HYPE": "HYPE-USD", "SHIB": "SHIB-USD", "KSHIB": "SHIB-USD", "XLM": "XLM-USD", "ZEC": "ZEC-USD",
}
_KRAKEN_PAIRS = {
    "BTC": "XBTUSD", "ETH": "ETHUSD", "SOL": "SOLUSD", "XRP": "XRPUSD",
    "DOGE": "DOGEUSD", "LTC": "LTCUSD", "BCH": "BCHUSD", "LINK": "LINKUSD",
    "SUI": "SUIUSD", "NEAR": "NEARUSD", "DOT": "DOTUSD", "HBAR": "HBARUSD",
    "HYPE": "HYPEUSD", "SHIB": "SHIBUSD", "KSHIB": "SHIBUSD", "XLM": "XLMUSD", "ZEC": "ZECUSD",
}
_API_NINJAS_PAIRS = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "XRP": "XRPUSDT",
    "DOGE": "DOGEUSDT", "LTC": "LTCUSDT", "BCH": "BCHUSDT", "LINK": "LINKUSDT",
    "SUI": "SUIUSDT", "NEAR": "NEARUSDT", "DOT": "DOTUSDT", "HBAR": "HBARUSDT",
    "HYPE": "HYPEUSDT", "SHIB": "SHIBUSDT", "KSHIB": "SHIBUSDT", "XLM": "XLMUSDT", "ZEC": "ZECUSDT",
}


def _fetch_coinbase(coin: str) -> float | None:
    product = _COINBASE_PRODUCTS.get(coin)
    if not product:
        return None
    try:
        resp = requests.get(f"https://api.exchange.coinbase.com/products/{product}/ticker", timeout=_TIMEOUT_SEC)
        resp.raise_for_status()
        price = resp.json().get("price")
        return float(price) if price is not None else None
    except Exception as exc:
        logger.debug("[crypto_prices] coinbase fetch failed for %s: %s", coin, exc)
        return None


def _fetch_kraken(coin: str) -> float | None:
    pair = _KRAKEN_PAIRS.get(coin)
    if not pair:
        return None
    try:
        resp = requests.get("https://api.kraken.com/0/public/Ticker", params={"pair": pair}, timeout=_TIMEOUT_SEC)
        resp.raise_for_status()
        data = resp.json()
        if data.get("error"):
            return None
        result = data.get("result") or {}
        row = next(iter(result.values()), None)
        if not row:
            return None
        last_trade = row.get("c") or []
        return float(last_trade[0]) if last_trade else None
    except Exception as exc:
        logger.debug("[crypto_prices] kraken fetch failed for %s: %s", coin, exc)
        return None


def _fetch_api_ninjas(coin: str) -> float | None:
    """Last-resort ONLY -- see module docstring: this key's tier returns
    prices delayed by ~15 minutes, never a live quote."""
    if not API_NINJAS_API_KEY:
        return None
    symbol = _API_NINJAS_PAIRS.get(coin)
    if not symbol:
        return None
    try:
        resp = requests.get(
            "https://api.api-ninjas.com/v1/cryptoprice", params={"symbol": symbol},
            headers={"X-Api-Key": API_NINJAS_API_KEY}, timeout=_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        price = resp.json().get("price")
        return float(price) if price is not None else None
    except Exception as exc:
        logger.debug("[crypto_prices] api ninjas fetch failed for %s: %s", coin, exc)
        return None


def get_fast_price(coin: str) -> dict[str, Any] | None:
    """Best available live price for a coin symbol (BTC, ETH, ...), trying
    Coinbase then Kraken then (delayed) API Ninjas. Cached for a few seconds
    so a burst of calls across several open positions doesn't multiply
    outbound requests. Returns None if every source fails."""
    symbol = str(coin or "").upper().strip()
    cached = _cache.get(symbol)
    now = time.monotonic()
    if cached and (now - cached[1]) < _CACHE_TTL_SEC:
        return cached[0]

    price = _fetch_coinbase(symbol)
    if price is not None:
        result = {"price": price, "source": "coinbase", "delayed": False}
        _cache[symbol] = (result, now)
        return result

    price = _fetch_kraken(symbol)
    if price is not None:
        result = {"price": price, "source": "kraken", "delayed": False}
        _cache[symbol] = (result, now)
        return result

    price = _fetch_api_ninjas(symbol)
    if price is not None:
        result = {"price": price, "source": "api_ninjas", "delayed": True}
        _cache[symbol] = (result, now)
        return result

    return None
