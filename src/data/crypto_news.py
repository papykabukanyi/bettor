"""Crypto news sentiment.

Free, no-key sources (live-tested 2026-07-22, all return real content):
  1. Google News RSS   -- broad coverage, per-coin query
  2. CoinTelegraph RSS -- crypto-specific newsroom (general feed)
  3. CryptoSlate RSS   -- crypto-specific newsroom (general feed)
  4. Decrypt RSS       -- crypto-specific newsroom (general feed)

Reddit's JSON API (r/CryptoCurrency, r/Bitcoin) used to be a source here but
now returns HTTP 403 on every request -- confirmed from multiple networks,
not just this server, so it's a deliberate block rather than a transient
rate limit. Removed rather than left silently failing on every cycle.

Optional paid-tier-adjacent source: CryptoPanic's public API now sits behind
Cloudflare bot protection and returns a 400 challenge page without a real
auth token (their old fully-open endpoint is gone). If you want it anyway --
it's a crypto-native aggregator with per-currency tagging, generally the best
signal-to-noise of any source here -- sign up for a free token at
https://cryptopanic.com/developers/api/ (free tier, no credit card) and set
CRYPTOPANIC_API_KEY. Without it, this source is simply skipped (not
required -- the four free sources above already cover it).

Produces a simple keyword-polarity sentiment score in [-1, 1] plus a headline
volume count. This is intentionally lightweight (no ML sentiment model) --
just enough signal to feed as one more feature into the direction classifier,
not a system of its own.
"""
from __future__ import annotations

import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from typing import Any

import requests

logger = logging.getLogger(__name__)

_TIMEOUT_SEC = 8
_CACHE_TTL_SEC = 600
_cache: dict[str, tuple[dict[str, Any], float]] = {}

CRYPTOPANIC_API_KEY = os.getenv("CRYPTOPANIC_API_KEY", "")

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
# CryptoPanic currency-tag codes differ slightly from Kalshi's coin symbols.
_CRYPTOPANIC_CODES = {"KSHIB": "SHIB"}


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


def _fetch_rss_titles(url: str, *, source_name: str, limit: int = 40, headers: dict[str, str] | None = None) -> list[str]:
    try:
        resp = requests.get(url, timeout=_TIMEOUT_SEC, headers=headers or {"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        return [item.findtext("title") or "" for item in root.iter("item")][:limit]
    except Exception as exc:
        logger.warning("[crypto_news] %s rss failed: %s", source_name, exc)
        return []


def _fetch_google_news_rss(query: str) -> list[str]:
    url = "https://news.google.com/rss/search"
    try:
        resp = requests.get(url, params={"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"}, timeout=_TIMEOUT_SEC)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        return [item.findtext("title") or "" for item in root.iter("item")][:30]
    except Exception as exc:
        logger.warning("[crypto_news] google news rss failed for %r: %s", query, exc)
        return []


def _fetch_cryptopanic(coin_symbol: str) -> list[str]:
    """Optional: only runs if CRYPTOPANIC_API_KEY is set. See module
    docstring for how to get a free token -- skipped silently otherwise."""
    if not CRYPTOPANIC_API_KEY:
        return []
    code = _CRYPTOPANIC_CODES.get(coin_symbol, coin_symbol)
    try:
        resp = requests.get(
            "https://cryptopanic.com/api/v1/posts/",
            params={"auth_token": CRYPTOPANIC_API_KEY, "currencies": code, "public": "true"},
            timeout=_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        results = resp.json().get("results") or []
        return [r.get("title", "") for r in results if r.get("title")]
    except Exception as exc:
        logger.warning("[crypto_news] cryptopanic fetch failed for %s: %s", coin_symbol, exc)
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
    headlines.extend(_fetch_cryptopanic(symbol))
    if symbol == "BTC":
        headlines.extend(_fetch_rss_titles("https://cointelegraph.com/rss", source_name="cointelegraph"))
        headlines.extend(_fetch_rss_titles("https://cryptoslate.com/feed/", source_name="cryptoslate"))
        headlines.extend(_fetch_rss_titles("https://decrypt.co/feed", source_name="decrypt"))

    score, volume = _score_headlines(headlines)
    result = {
        "coin": symbol, "sentiment_score": score, "headline_volume": volume,
        "computed_at": time.time(),
    }
    _cache[symbol] = (result, now)
    return result
