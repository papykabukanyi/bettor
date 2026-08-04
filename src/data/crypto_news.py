"""Crypto news sentiment.

Free, no-key sources (live-tested 2026-07-22, all return real content):
  1. Google News RSS   -- broad coverage, per-coin query
  2. CoinTelegraph RSS -- crypto-specific newsroom (general feed)
  3. CryptoSlate RSS   -- crypto-specific newsroom (general feed)
  4. Decrypt RSS       -- crypto-specific newsroom (general feed)

The three newsroom feeds are general (not per-coin) but free and unlimited,
so every coin gets its OWN individual slice of them: each feed is fetched
ONCE (shared, long-TTL cache) and its headlines are matched against each
coin's own keyword terms (see _COIN_MATCH_TERMS) -- not just dumped onto
BTC by default. A quieter altcoin will genuinely get fewer matches than
BTC/ETH; that's an honest reflection of real news coverage, not a bug.

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

Optional additional source: newsdata.io's `/api/1/latest` endpoint (confirmed
live 2026-07-22 with a free key), general news filtered by a per-coin query --
their free tier is a few hundred requests/day account-wide, shared across
every coin this bot watches. Confirmed live: with every active ticker polled
every ~10 minutes, this quota is gone within the first hour or two of any
given day, and every later call that same day also 429s -- so a 429 is
treated as "today's quota is gone," not a transient blip (see
_NEWSDATA_COOLDOWN_SEC). CryptoPanic and this source are also both gated to
ONLY the coins currently in get_watchlist() (top volume+volatility) via
get_sentiment()'s use_limited_sources flag -- the limited/quota-constrained
sources are worth spending on the coins actually being traded right now, not
spread thin across every active-but-untraded instrument. Sign up free at
https://newsdata.io and set NEWSDATA_API_KEY. Skipped silently without it.

Optional additional source: SerpApi's Google News engine (serpapi_client.py),
gated the same watchlist-only way as CryptoPanic/newsdata.io above. Its own
250-searches/month free tier is shared with stock_news.py and the 30-minute
trending-news Threads post, so serpapi_client.py enforces a real cooldown
between ANY two calls across all of them -- see that module's own docstring
for the exact budget math. Set SERPAPI_API_KEY to enable; skipped silently
without it.

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

from data import news_sources, serpapi_client

logger = logging.getLogger(__name__)

_TIMEOUT_SEC = 8
_CACHE_TTL_SEC = 600
_cache: dict[str, tuple[dict[str, Any], float]] = {}

# The three BTC-only general newsroom feeds below are a single shared feed
# (not per-coin), so they're cached separately with a longer TTL -- cuts
# their call rate ~3x versus riding the per-coin 600s cache, after CryptoSlate
# started returning 429 (Too Many Requests) under normal traffic. Render's
# outbound IPs are shared across many unrelated apps, so this is about being
# a lighter neighbor, not a bug in our own request volume.
_GENERAL_FEED_CACHE_TTL_SEC = 1800
_general_feed_cache: dict[str, tuple[list[str], float]] = {}

CRYPTOPANIC_API_KEY = os.getenv("CRYPTOPANIC_API_KEY", "")
NEWSDATA_API_KEY = os.getenv("NEWSDATA_API_KEY", "")

# Real gap found in review: every OTHER limited source here (newsdata.io
# below, serpapi_client.py's own shared budget, news_sources.py's three
# sources) proactively rations calls against its free-tier daily cap --
# CryptoPanic had none at all, just fired a real request every single time
# it was asked, for every ticker, every cycle. Same "manage the budget,
# don't just react to a failure" discipline as news_sources.py's own
# _cooldown_ok: a real minimum interval sized from CryptoPanic's stated
# free-tier daily cap with a safety margin, not a race to the limit.
_CRYPTOPANIC_CALLS_PER_DAY = float(os.getenv("CRYPTOPANIC_CALLS_PER_DAY", "1000") or "1000")
_CRYPTOPANIC_SAFETY_MARGIN = 0.8
_cryptopanic_last_call_ts = 0.0
# Belt-and-suspenders, same as newsdata.io below: if a 429 slips through
# anyway (the daily cap turned out to be lower than assumed, or shared
# across other apps on the same account), stop hammering it for the day.
_CRYPTOPANIC_429_COOLDOWN_SEC = 24 * 3600
_cryptopanic_cooldown_until = 0.0

# Confirmed live: with every active ticker each checked roughly every 10
# minutes, newsdata.io's free-tier DAILY quota gets exhausted within the
# first hour or two, and every subsequent call for the REST OF THAT DAY also
# 429s. A 1-hour cooldown used to just retry-and-fail once an hour for the
# rest of the day, spamming the log with an already-known outcome -- a 429
# means the daily budget is gone, so the cooldown needs to cover roughly a
# day, not an hour.
_NEWSDATA_COOLDOWN_SEC = 24 * 3600
_newsdata_cooldown_until = 0.0

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

# Used to match each coin's own headlines out of the SHARED general
# newsroom feeds (CoinTelegraph/CryptoSlate/Decrypt cover all of crypto, not
# one coin). Deliberately full names/distinctive phrases rather than bare
# tickers for the short/ambiguous symbols (near/link/dot/sui are all common
# English words or substrings -- "near" would match almost any headline,
# "link" matches any article that merely links to something) -- a false
# match here would inject noise into that coin's sentiment score, not signal.
_COIN_MATCH_TERMS = {
    "BTC": ["bitcoin"], "ETH": ["ethereum"], "SOL": ["solana"],
    "XRP": ["xrp", "ripple"], "DOGE": ["dogecoin"], "LTC": ["litecoin"],
    "BCH": ["bitcoin cash"], "LINK": ["chainlink"], "SUI": ["sui network", "sui blockchain", "$sui"],
    "NEAR": ["near protocol", "$near"], "DOT": ["polkadot"], "HBAR": ["hedera"],
    "HYPE": ["hyperliquid"], "KSHIB": ["shiba inu"], "XLM": ["stellar", "lumens"],
    "ZEC": ["zcash"],
}


def _match_headlines_for_coin(headlines: list[str], symbol: str) -> list[str]:
    """Filters a batch of general (not coin-specific) headlines down to the
    ones actually about this coin, using safe/distinctive terms rather than
    the bare ticker (see _COIN_MATCH_TERMS)."""
    terms = _COIN_MATCH_TERMS.get(symbol) or [_COIN_QUERIES.get(symbol, symbol.lower())]
    return [h for h in headlines if any(term in h.lower() for term in terms)]


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


def _fetch_rss_titles_cached(url: str, *, source_name: str, limit: int = 40) -> list[str]:
    """Same as _fetch_rss_titles but with its own longer-lived cache -- for
    the shared (not per-coin) general newsroom feeds only."""
    now = time.time()
    cached = _general_feed_cache.get(source_name)
    if cached and (now - cached[1]) < _GENERAL_FEED_CACHE_TTL_SEC:
        return cached[0]
    titles = _fetch_rss_titles(url, source_name=source_name, limit=limit)
    if titles:  # don't cache a transient failure's empty result over a good one
        _general_feed_cache[source_name] = (titles, now)
    return titles


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
    docstring for how to get a free token -- skipped silently otherwise.
    Proactively rationed (see _CRYPTOPANIC_CALLS_PER_DAY above) PLUS a
    reactive 429 cooldown as a second line of defense."""
    global _cryptopanic_last_call_ts, _cryptopanic_cooldown_until
    if not CRYPTOPANIC_API_KEY:
        return []
    now = time.time()
    if now < _cryptopanic_cooldown_until:
        return []
    min_interval = 86400.0 / max(1.0, _CRYPTOPANIC_CALLS_PER_DAY * _CRYPTOPANIC_SAFETY_MARGIN)
    if (now - _cryptopanic_last_call_ts) < min_interval:
        return []
    _cryptopanic_last_call_ts = now
    code = _CRYPTOPANIC_CODES.get(coin_symbol, coin_symbol)
    try:
        resp = requests.get(
            "https://cryptopanic.com/api/v1/posts/",
            params={"auth_token": CRYPTOPANIC_API_KEY, "currencies": code, "public": "true"},
            timeout=_TIMEOUT_SEC,
        )
        if resp.status_code == 429:
            _cryptopanic_cooldown_until = now + _CRYPTOPANIC_429_COOLDOWN_SEC
            logger.warning("[crypto_news] cryptopanic rate-limited (429) -- daily quota likely exhausted, pausing this source for 24.0 hours")
            return []
        resp.raise_for_status()
        results = resp.json().get("results") or []
        return [r.get("title", "") for r in results if r.get("title")]
    except Exception as exc:
        logger.warning("[crypto_news] cryptopanic fetch failed for %s: %s", coin_symbol, exc)
        return []


def _fetch_newsdata_io(coin_symbol: str) -> list[str]:
    """Optional: only runs if NEWSDATA_API_KEY is set. See module docstring
    for how to get a free token -- skipped silently otherwise. Their
    `sentiment`/`ai_*` fields are paid-plan-only (confirmed live), so this
    only uses the free `title` field, same as every other source here."""
    global _newsdata_cooldown_until
    if not NEWSDATA_API_KEY:
        return []
    now = time.time()
    if now < _newsdata_cooldown_until:
        return []
    query = _COIN_QUERIES.get(coin_symbol, coin_symbol.lower())
    try:
        resp = requests.get(
            "https://newsdata.io/api/1/latest",
            params={"apikey": NEWSDATA_API_KEY, "q": query, "language": "en"},
            timeout=_TIMEOUT_SEC,
        )
        if resp.status_code == 429:
            _newsdata_cooldown_until = now + _NEWSDATA_COOLDOWN_SEC
            logger.warning(
                "[crypto_news] newsdata.io rate-limited (429) -- daily quota likely exhausted, "
                "pausing this source for %.1f hours",
                _NEWSDATA_COOLDOWN_SEC / 3600,
            )
            return []
        resp.raise_for_status()
        results = resp.json().get("results") or []
        return [r.get("title", "") for r in results if r.get("title")]
    except Exception as exc:
        logger.warning("[crypto_news] newsdata.io fetch failed for %s: %s", coin_symbol, exc)
        return []


def _fetch_serpapi(coin_symbol: str) -> list[str]:
    """Optional: only runs if SERPAPI_API_KEY is set (see serpapi_client.py
    for the shared 250/month-budget cooldown every caller of that module
    respects). Gated the same way as CryptoPanic/newsdata.io -- a paid,
    quota-constrained source reserved for watchlist coins."""
    query = _COIN_QUERIES.get(coin_symbol, coin_symbol.lower())
    return serpapi_client.search_news(query)


def get_sentiment(coin_symbol: str, *, use_limited_sources: bool = True) -> dict[str, Any]:
    """Sentiment for one coin symbol (e.g. "BTC"). Cached per-coin for
    _CACHE_TTL_SEC since news doesn't meaningfully change minute to minute.

    use_limited_sources gates CryptoPanic + newsdata.io + SerpApi -- all
    quota/quality-constrained, unlike the free/unlimited RSS sources below.
    Callers should set this False for coins outside the current watchlist
    (see perps_data.get_watchlist()) so the limited quota is spent on the
    coins actually meeting the volume+volatility bar right now, not spread
    across every active-but-untraded instrument."""
    symbol = str(coin_symbol or "").upper().strip()
    cached = _cache.get(symbol)
    now = time.time()
    if cached and (now - cached[1]) < _CACHE_TTL_SEC:
        return cached[0]

    query = _COIN_QUERIES.get(symbol, symbol.lower())
    headlines: list[str] = []
    headlines.extend(_fetch_google_news_rss(query))
    if use_limited_sources:
        headlines.extend(_fetch_cryptopanic(symbol))
        headlines.extend(_fetch_newsdata_io(symbol))
        headlines.extend(_fetch_serpapi(symbol))
        headlines.extend(news_sources.fetch_all(query))

    # These three feeds cover all of crypto, not one coin -- fetched once
    # (shared, long-TTL cache) regardless of which coin asked, then matched
    # down to headlines actually about THIS coin. Runs for every coin, not
    # just BTC, so altcoins get real individual coverage from these free,
    # unlimited sources too.
    general_feed_headlines: list[str] = []
    general_feed_headlines.extend(_fetch_rss_titles_cached("https://cointelegraph.com/rss", source_name="cointelegraph"))
    general_feed_headlines.extend(_fetch_rss_titles_cached("https://cryptoslate.com/feed/", source_name="cryptoslate"))
    general_feed_headlines.extend(_fetch_rss_titles_cached("https://decrypt.co/feed", source_name="decrypt"))
    headlines.extend(_match_headlines_for_coin(general_feed_headlines, symbol))

    score, volume = _score_headlines(headlines)
    result = {
        "coin": symbol, "sentiment_score": score, "headline_volume": volume,
        "computed_at": time.time(),
    }
    _cache[symbol] = (result, now)
    return result


def get_trending_headlines(*, limit: int = 5) -> list[str]:
    """General crypto-market trending headlines -- NOT one coin's own
    sentiment feed, just "what's happening in crypto right now." Powers
    the 30-minute Threads trending-news post (see threads_post.py). Reuses
    the same free/unlimited general newsroom feeds get_sentiment() already
    shares across every coin, so this costs zero extra real network calls
    beyond their own long (30-min) TTL cache."""
    headlines: list[str] = []
    headlines.extend(_fetch_rss_titles_cached("https://cointelegraph.com/rss", source_name="cointelegraph"))
    headlines.extend(_fetch_rss_titles_cached("https://cryptoslate.com/feed/", source_name="cryptoslate"))
    headlines.extend(_fetch_rss_titles_cached("https://decrypt.co/feed", source_name="decrypt"))
    return [h for h in headlines if h][:limit]
