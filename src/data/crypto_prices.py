"""Live crypto price cross-check, independent of Kalshi's own quote -- from
Alpaca only.

1. The Alpaca crypto stream's live quote at the chart venue (Kraken US via
   Alpaca, alpaca_client.CHART_CRYPTO_LOC): the bid/ask mid, seconds old.
2. Alpaca's REST latest quote at the same venue, when the stream has
   nothing fresh.

Covers every coin in kalshi_15m_spot.SPOT_PRODUCTS (kSHIB reads SHIB, whose
price units differ -- callers compare it only where units match). Coins
Alpaca doesn't list return None, as do non-crypto underlyings (a gold perp
has no crypto quote to cross-check against).
"""
from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

_CACHE_TTL_SEC = 3
STREAM_MAX_AGE_SEC = 10.0
_cache: dict[str, tuple[dict[str, Any], float]] = {}


def _pair(coin: str) -> str | None:
    from data import kalshi_15m_spot
    c = kalshi_15m_spot.chart_coin(coin)
    return kalshi_15m_spot.SPOT_PRODUCTS.get(c)


def _fetch_alpaca_latest(pair: str) -> float | None:
    from data import alpaca_client
    try:
        data = alpaca_client._crypto_data_get(  # noqa: SLF001
            f"/v1beta3/crypto/{alpaca_client.CHART_CRYPTO_LOC}/latest/quotes", params={"symbols": pair})
        q = (data.get("quotes") or {}).get(pair) or {}
        bid, ask = float(q.get("bp") or 0), float(q.get("ap") or 0)
        return (bid + ask) / 2.0 if 0 < bid <= ask else None
    except Exception as exc:
        logger.debug("[crypto_prices] alpaca latest quote failed for %s: %s", pair, exc)
        return None


def get_fast_price(coin: str) -> dict[str, Any] | None:
    """Best available live price for a coin symbol (BTC, ETH, ...) from
    Alpaca: the stream's quote, else the REST latest quote. Cached a few
    seconds so a burst of calls across open positions doesn't multiply
    requests. None if Alpaca has no quote for it."""
    symbol = str(coin or "").upper().strip()
    cached = _cache.get(symbol)
    now = time.monotonic()
    if cached and (now - cached[1]) < _CACHE_TTL_SEC:
        return cached[0]
    pair = _pair(symbol)
    if not pair:
        return None
    from data import alpaca_stream
    tick = alpaca_stream.latest_price("crypto", pair, max_age_sec=STREAM_MAX_AGE_SEC)
    if tick is not None:
        result = {"price": float(tick["price"]), "source": "alpaca_stream", "delayed": False, "age_sec": tick["age_sec"]}
    else:
        price = _fetch_alpaca_latest(pair)
        if price is None:
            return None
        result = {"price": price, "source": "alpaca_latest_quote", "delayed": False}
    _cache[symbol] = (result, now)
    return result
