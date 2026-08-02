"""Stock news sentiment -- the equities equivalent of crypto_news.py, wired
into alpaca_data.py's engineer_features the exact same way perps_data.py
feeds crypto_news.get_sentiment() into its own feature frame (a single
`sentiment_score` column, broadcast as a constant across the batch that was
fetched together).

Only source: Google News RSS, free and unlimited, queried per symbol.
Unlike crypto (a small, hand-curated list of ~15 coins with hardcoded
match terms), the Alpaca watchlist can be any of thousands of US equities
chosen dynamically each cycle -- a hardcoded per-symbol keyword dict the
way crypto_news._COIN_MATCH_TERMS works doesn't scale here. Instead, the
query itself is built from Alpaca's own asset `name` field (e.g. "Apple
Inc. Common Stock" for AAPL, already available from alpaca_client.get_assets()
with zero extra API calls), which is both a genuine per-company search term
AND avoids the ticker-ambiguity problem (a bare ticker like "MA" is also
just the word "ma"; "Mastercard Incorporated" is not).

Deliberately does NOT add newsdata.io here: NEWSDATA_API_KEY's daily quota
is shared with crypto_news.py, which already exhausts it within an hour or
two on crypto tickers alone (see that module's own docstring) -- adding
stock tickers on the same key would just guarantee both sides run dry
sooner, for no net signal gain. If a stock-specific news source is wanted
later, give it its own separate API key rather than contending for this one.

Same lightweight keyword-polarity approach as crypto_news.py (no ML model) --
just enough signal to feed the direction classifier one more feature.
"""
from __future__ import annotations

import logging
import re
import time
import xml.etree.ElementTree as ET
from typing import Any

import requests

logger = logging.getLogger(__name__)

_TIMEOUT_SEC = 8
_CACHE_TTL_SEC = 600
_cache: dict[str, tuple[dict[str, Any], float]] = {}

# General finance/equity vocabulary -- overlaps with crypto_news.py's lexicon
# where the words apply equally well to stocks (surge, rally, crash, ...),
# swapped out for equity-specific idioms crypto headlines rarely use
# (earnings beats/misses, guidance, downgrades) in place of pure crypto terms
# (adoption, etf, inflows) that would mostly just fail to match here anyway.
_POSITIVE_WORDS = {
    "surge", "rally", "bullish", "gain", "gains", "soar", "soars", "high", "highs",
    "beat", "beats", "upgrade", "upgraded", "outperform", "breakout", "record",
    "buy", "buying", "positive", "recover", "recovery", "boom", "jump", "jumps",
    "rise", "rises", "rising", "profit", "profits", "growth", "strong", "buyback",
}
_NEGATIVE_WORDS = {
    "crash", "crashes", "plunge", "plunges", "bearish", "miss", "misses", "downgrade",
    "downgraded", "underperform", "lawsuit", "dump", "dumps", "sell-off", "selloff",
    "layoffs", "layoff", "fear", "loss", "losses", "decline", "declines", "drop",
    "drops", "falling", "fell", "fine", "investigation", "recall", "weak", "cut",
    "cuts", "warning",
}


def _score_headlines(headlines: list[str]) -> tuple[float, int]:
    total = 0.0
    scored = 0
    for headline in headlines:
        words = set(re.findall(r"[a-z]+", headline.lower()))
        pos = len(words & _POSITIVE_WORDS)
        neg = len(words & _NEGATIVE_WORDS)
        if pos == 0 and neg == 0:
            continue
        total += (pos - neg) / max(1, pos + neg)
        scored += 1
    if scored == 0:
        return 0.0, len(headlines)
    return max(-1.0, min(1.0, total / scored)), len(headlines)


def _fetch_google_news_rss(query: str) -> list[str]:
    url = "https://news.google.com/rss/search"
    try:
        resp = requests.get(url, params={"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"}, timeout=_TIMEOUT_SEC)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        return [item.findtext("title") or "" for item in root.iter("item")][:30]
    except Exception as exc:
        logger.warning("[stock_news] google news rss failed for %r: %s", query, exc)
        return []


def _clean_company_query(symbol: str, company_name: str | None) -> str:
    """A bare ticker is often ambiguous or a common word ("MA", "ALL", "IT"),
    so prefer the company name when available -- stripped of the generic
    "Common Stock"/share-class suffixes Alpaca's asset names carry, which
    only dilute the search rather than helping it."""
    if not company_name:
        return f"{symbol} stock"
    cleaned = re.sub(
        r"\b(common stock|ordinary shares|class [a-z]|depositary shares|inc\.?|corp\.?|corporation|co\.?|ltd\.?|plc)\b",
        "", company_name, flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.")
    return f"{cleaned or symbol} stock"


def get_sentiment(symbol: str, *, company_name: str | None = None) -> dict[str, Any]:
    """Sentiment for one equity symbol. Cached per-symbol for
    _CACHE_TTL_SEC since news doesn't meaningfully change minute to minute --
    same TTL as crypto_news.get_sentiment()."""
    symbol = str(symbol or "").upper().strip()
    cached = _cache.get(symbol)
    now = time.time()
    if cached and (now - cached[1]) < _CACHE_TTL_SEC:
        return cached[0]

    query = _clean_company_query(symbol, company_name)
    headlines = _fetch_google_news_rss(query)
    score, volume = _score_headlines(headlines)
    result = {
        "symbol": symbol, "sentiment_score": score, "headline_volume": volume,
        "computed_at": now,
    }
    _cache[symbol] = (result, now)
    return result
