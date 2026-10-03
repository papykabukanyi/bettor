"""Crypto news and sentiment -- from Alpaca only (Benzinga, alpaca_news).

The same functions every caller has always used (the Kalshi and Alpaca bots'
data collectors and models, the Threads posts), now reading Alpaca's news:
articles tagged with the coin's pair ticker (BTCUSD, ...), from the live
news stream plus the REST endpoint. No other news source is read.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable

from data import alpaca_news

logger = logging.getLogger(__name__)

_CACHE_TTL_SEC = 120
_cache: dict[str, tuple[dict[str, Any], float]] = {}


def _score_headlines(headlines: list[str]) -> tuple[float, int]:
    return alpaca_news.score_headlines(headlines)


def get_sentiment(coin_symbol: str, *, use_limited_sources: bool = True) -> dict[str, Any]:
    """Sentiment for one coin (e.g. "BTC") from Alpaca news over the last
    alpaca_news.DEFAULT_HOURS. use_limited_sources is kept for callers and
    ignored (there is one source)."""
    symbol = str(coin_symbol or "").upper().strip()
    cached = _cache.get(symbol)
    now = time.time()
    if cached and (now - cached[1]) < _CACHE_TTL_SEC:
        return cached[0]
    info = alpaca_news.sentiment(symbol)
    result = {"coin": symbol, "sentiment_score": info["sentiment_score"], "headline_volume": info["headline_volume"],
              "headline_sentiment_score": info["sentiment_score"], "source": info["source"], "computed_at": now}
    _cache[symbol] = (result, now)
    return result


def get_generic_sentiment(query: str, *, cache_key: str) -> dict[str, Any]:
    """Sentiment for an asset that isn't a coin (e.g. a commodity), named by
    cache_key ("GOLD", "WTI", ...), from the Alpaca news on its tickers."""
    key = str(cache_key or query or "").upper().strip()
    info = alpaca_news.sentiment(key)
    return {"query": query, "sentiment_score": info["sentiment_score"], "headline_volume": info["headline_volume"],
            "source": info["source"], "computed_at": time.time()}


def prewarm_sentiment(coins: list[str], *, use_limited_sources: bool = True, max_workers: int = 8) -> None:
    """One bulk Alpaca news pull for every coin a scan is about to read."""
    alpaca_news.prefetch([c for c in coins if c])


def _crypto_tickers() -> list[str]:
    from data import kalshi_15m_spot
    return [f"{c}USD" for c in kalshi_15m_spot.SPOT_PRODUCTS]


def get_trending_headlines(*, limit: int = 5) -> list[str]:
    """The newest crypto headlines on Alpaca news."""
    try:
        alpaca_news.fetch(_crypto_tickers(), hours=12)
    except Exception as exc:
        logger.debug("[crypto_news] headline fetch failed: %s", exc)
    return [a["headline"] for a in alpaca_news.articles(_crypto_tickers(), hours=12) if a["headline"]][:limit]


def get_trending_story(*, exclude: Callable[[str], bool] | None = None) -> dict[str, Any] | None:
    """The lead crypto story for the Threads trending-news post, from Alpaca
    news (see alpaca_news.latest_story)."""
    return alpaca_news.latest_story(_crypto_tickers(), exclude=exclude)
