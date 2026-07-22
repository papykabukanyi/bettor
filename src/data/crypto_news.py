"""Crypto news sentiment -- free, no-key sources only.

Three independent free feeds, each degrading silently if unreachable:
  1. Google News RSS   -- broad coverage, per-coin query
  2. CoinTelegraph RSS -- crypto-specific newsroom
  3. Reddit JSON API   -- r/CryptoCurrency + r/{coin} real-time discussion

Produces a simple keyword-polarity sentiment score in [-1, 1] plus a headline
volume count. This is intentionally lightweight (no ML sentiment model) --
just enough signal to feed as one more feature into the direction classifier,
not a system of its own.
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

_POSITIVE_WORDS = {
    "surge", "rally", "bullish", "gain", "gains", "soar", "soars", "high", "highs",
    "adopt", "adoption", "approve", "approval", "partnership", "breakout", "record",
    "inflow", "inflows", "buy", "buying", "upgrade", "positive", "recover", "recovery",
    "boom", "jump", "jumps", "rise", "rises", "rising", "milestone", "etf",
}
_NEGATIVE_WORDS = {
    "crash", "crashes", "plunge", "plunges", "bearish", "hack", "hacked", "ban",
    "banned", "lawsuit", "dump", "dumps", "sell-off", "selloff", "crackdown",
    "regulation", "regulatory", "fear", "loss", "losses", "outflow", "outflows",
    "collapse", "liquidation", "liquidated", "scam", "fraud", "decline", "drop",
    "drops", "falling", "fell", "fine", "investigation",
}

_COIN_QUERIES = {
    "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "XRP": "xrp ripple",
    "DOGE": "dogecoin", "LTC": "litecoin", "BCH": "bitcoin cash", "LINK": "chainlink",
    "SUI": "sui crypto", "NEAR": "near protocol crypto", "DOT": "polkadot crypto",
    "HBAR": "hedera hbar", "HYPE": "hyperliquid crypto", "KSHIB": "shiba inu",
    "XLM": "stellar lumens", "ZEC": "zcash",
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
    try:
        url = "https://news.google.com/rss/search"
        params = {"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"}
        resp = requests.get(url, params=params, timeout=_TIMEOUT_SEC)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        return [item.findtext("title") or "" for item in root.iter("item")][:30]
    except Exception as exc:
        logger.warning("[crypto_news] google news rss failed for %r: %s", query, exc)
        return []


def _fetch_cointelegraph_rss() -> list[str]:
    try:
        resp = requests.get("https://cointelegraph.com/rss", timeout=_TIMEOUT_SEC)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        return [item.findtext("title") or "" for item in root.iter("item")][:40]
    except Exception as exc:
        logger.warning("[crypto_news] cointelegraph rss failed: %s", exc)
        return []


def _fetch_reddit_json(subreddit: str) -> list[str]:
    try:
        url = f"https://www.reddit.com/r/{subreddit}/new.json"
        headers = {"User-Agent": "bettor-perps-bot/1.0"}
        resp = requests.get(url, params={"limit": 30}, headers=headers, timeout=_TIMEOUT_SEC)
        resp.raise_for_status()
        data = resp.json()
        children = (data.get("data") or {}).get("children") or []
        return [c.get("data", {}).get("title", "") for c in children]
    except Exception as exc:
        logger.warning("[crypto_news] reddit fetch failed for r/%s: %s", subreddit, exc)
        return []


def get_sentiment(coin_symbol: str) -> dict[str, Any]:
    """Sentiment for one coin symbol (e.g. "BTC"). Cached per-coin for
    _CACHE_TTL_SEC since news doesn't meaningfully change minute to minute."""
    symbol = str(coin_symbol or "").upper().strip()
    cached = _cache.get(symbol)
    now = time.time()
    if cached and (now - cached[1]) < _CACHE_TTL_SEC:
        return cached[0]

    query = _COIN_QUERIES.get(symbol, symbol.lower())
    headlines: list[str] = []
    headlines.extend(_fetch_google_news_rss(query))
    if symbol == "BTC":
        headlines.extend(_fetch_cointelegraph_rss())
        headlines.extend(_fetch_reddit_json("Bitcoin"))
    headlines.extend(_fetch_reddit_json("CryptoCurrency"))

    score, volume = _score_headlines(headlines)
    result = {
        "coin": symbol, "sentiment_score": score, "headline_volume": volume,
        "computed_at": time.time(),
    }
    _cache[symbol] = (result, now)
    return result
