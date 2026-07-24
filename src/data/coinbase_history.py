"""Deep historical crypto price data from Coinbase Exchange's public API --
free, no key required, no geo-block for US-based servers (unlike Binance,
which returns HTTP 451 "restricted location" from this account's servers).

Why this exists: Kalshi's crypto perps only launched 2026-06-04, so Kalshi's
own archive can never hold more than a few weeks of history. The underlying
coins themselves have traded for years, and Kalshi's perp price tracks that
same underlying spot price closely (that's the whole point of a perp), so a
model pretrained on the coin's own multi-year Coinbase price history is a
reasonable, honest way to give it far more market experience than Kalshi's
own archive can offer -- then it's applied live against Kalshi's own 1-minute
feed (see perps_data.py), not against Coinbase prices.

Confirmed live against the real API (2026-07-23):
  - All 16 Kalshi perp coins are listed on Coinbase under a "<SYM>-USD"
    product (including HYPE and SHIB, the underlying real coin for kSHIB).
  - 1-minute candles retain at least 2 years of real history (not just a
    recent window, unlike several other free exchange APIs).
  - Daily candles retain 5+ years.
  - No rate-limit errors observed at ~6 requests/second sustained.

Endpoint: GET /products/{product_id}/candles?granularity=<seconds>&start=&end=
Returns up to 300 candles per call as [time, low, high, open, close, volume]
arrays, so multi-year history needs many chained calls -- see
fetch_coinbase_history() below.
"""
from __future__ import annotations

import datetime as dt
import logging
import time
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)

COINBASE_BASE_URL = "https://api.exchange.coinbase.com"
_TIMEOUT_SEC = 15
_MAX_CANDLES_PER_CALL = 300
_REQUEST_DELAY_SEC = 0.12  # ~8/sec -- comfortably under the observed safe rate

# Kalshi coin symbol -> Coinbase product ID. kSHIB is Kalshi's own scaled
# representation of Shiba Inu -- the underlying real coin is just SHIB.
COINBASE_PRODUCT_BY_COIN = {
    "BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD", "XRP": "XRP-USD",
    "DOGE": "DOGE-USD", "LTC": "LTC-USD", "BCH": "BCH-USD", "LINK": "LINK-USD",
    "SUI": "SUI-USD", "NEAR": "NEAR-USD", "DOT": "DOT-USD", "HBAR": "HBAR-USD",
    "HYPE": "HYPE-USD", "KSHIB": "SHIB-USD", "XLM": "XLM-USD", "ZEC": "ZEC-USD",
}


def _fetch_candle_page(product_id: str, *, granularity_sec: int, start: dt.datetime, end: dt.datetime) -> list[list[float]]:
    resp = requests.get(
        f"{COINBASE_BASE_URL}/products/{product_id}/candles",
        params={"granularity": granularity_sec, "start": start.isoformat(), "end": end.isoformat()},
        timeout=_TIMEOUT_SEC, headers={"User-Agent": "Mozilla/5.0"},
    )
    if resp.status_code == 429:
        time.sleep(2.0)
        resp = requests.get(
            f"{COINBASE_BASE_URL}/products/{product_id}/candles",
            params={"granularity": granularity_sec, "start": start.isoformat(), "end": end.isoformat()},
            timeout=_TIMEOUT_SEC, headers={"User-Agent": "Mozilla/5.0"},
        )
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def fetch_coinbase_history(
    product_id: str, *, days: int, granularity_sec: int, end: dt.datetime | None = None,
) -> pd.DataFrame:
    """Chains paginated calls backward from `end` (default now) to cover
    `days` of history at `granularity_sec` resolution. Returns a DataFrame
    with columns [ts, close] -- same shape as perps_data._candles_to_frame,
    so it plugs directly into engineer_features() unchanged. Stops early
    (rather than erroring) once a ticker's actual listing history runs out
    -- newer coins (e.g. HYPE, SUI) legitimately don't have 4 years."""
    end = end or dt.datetime.now(dt.timezone.utc)
    span_per_call = dt.timedelta(seconds=granularity_sec * _MAX_CANDLES_PER_CALL)
    total_span = dt.timedelta(days=days)
    window_end = end
    earliest = end - total_span
    frames = []
    consecutive_empty = 0
    while window_end > earliest:
        window_start = max(earliest, window_end - span_per_call)
        try:
            page = _fetch_candle_page(product_id, granularity_sec=granularity_sec, start=window_start, end=window_end)
        except Exception as exc:
            logger.warning("[coinbase_history] page fetch failed for %s: %s", product_id, exc)
            break
        time.sleep(_REQUEST_DELAY_SEC)
        if not page:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                # Three empty pages in a row -- almost certainly walked past
                # this coin's actual listing date, not a transient gap.
                break
        else:
            consecutive_empty = 0
            frames.append(pd.DataFrame({
                "ts": [int(row[0]) for row in page],
                "close": [float(row[4]) for row in page],
            }))
        window_end = window_start
    if not frames:
        return pd.DataFrame({"ts": pd.Series(dtype="int64"), "close": pd.Series(dtype="float64")})
    combined = pd.concat(frames, ignore_index=True)
    return combined.drop_duplicates(subset="ts").sort_values("ts").reset_index(drop=True)
