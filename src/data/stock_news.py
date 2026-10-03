"""Stock news and sentiment -- from Alpaca only (Benzinga, alpaca_news).

The same functions every caller has always used (the stock and options
bots' data collectors and models, the Threads posts), now reading Alpaca's
news for the ticker itself. No other news source is read.
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


def get_sentiment(symbol: str, *, company_name: str | None = None, use_limited_sources: bool = True) -> dict[str, Any]:
    """Sentiment for one equity symbol from Alpaca news over the last
    alpaca_news.DEFAULT_HOURS (company_name / use_limited_sources are kept
    for callers and not needed: Alpaca tags articles with the ticker)."""
    symbol = str(symbol or "").upper().strip()
    cached = _cache.get(symbol)
    now = time.time()
    if cached and (now - cached[1]) < _CACHE_TTL_SEC:
        return cached[0]
    info = alpaca_news.sentiment(symbol)
    result = {"symbol": symbol, "sentiment_score": info["sentiment_score"], "headline_volume": info["headline_volume"],
              "source": info["source"], "computed_at": now}
    _cache[symbol] = (result, now)
    return result


def prewarm_sentiment(items: list[tuple[str, str | None]], *, use_limited_sources: bool = True, max_workers: int = 8) -> None:
    """One bulk Alpaca news pull for every (symbol, company_name) a scan is
    about to read."""
    alpaca_news.prefetch([s for s, _ in items if s])


_MARKET_TICKERS = ("SPY", "QQQ", "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA")


def get_trending_headlines(*, limit: int = 5) -> list[str]:
    """The newest market headlines on Alpaca news (index ETFs and megacaps)."""
    try:
        alpaca_news.fetch(list(_MARKET_TICKERS), hours=12)
    except Exception as exc:
        logger.debug("[stock_news] headline fetch failed: %s", exc)
    return [a["headline"] for a in alpaca_news.articles(list(_MARKET_TICKERS), hours=12) if a["headline"]][:limit]


def get_trending_story(*, query: str | None = None, exclude: Callable[[str], bool] | None = None) -> dict[str, Any] | None:
    """The lead market story for the Threads trending-news post, from Alpaca
    news. `query`, when it is a ticker, narrows the story to that ticker."""
    tickers = [query.upper()] if query and query.replace(".", "").isalpha() and len(query) <= 5 else list(_MARKET_TICKERS)
    return alpaca_news.latest_story(tickers, exclude=exclude)
