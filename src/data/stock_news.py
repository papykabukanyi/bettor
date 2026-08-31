"""Stock news sentiment -- the equities equivalent of crypto_news.py, wired
into alpaca_data.py's engineer_features the exact same way perps_data.py
feeds crypto_news.get_sentiment() into its own feature frame (a single
`sentiment_score` column, broadcast as a constant across the batch that was
fetched together).

Primary source: Google News RSS, free and unlimited, queried per symbol.
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
sooner, for no net signal gain.

Optional additional source: SerpApi's Google News engine (serpapi_client.py),
gated to watchlist symbols only via get_sentiment()'s use_limited_sources
flag -- same discipline as crypto_news.py, since its 250-searches/month
free tier is shared across BOTH sentiment modules and the 30-minute
trending-news Threads post. Set SERPAPI_API_KEY to enable; skipped silently
without it.

Same lightweight keyword-polarity approach as crypto_news.py (no ML model) --
just enough signal to feed the direction classifier one more feature.
"""
from __future__ import annotations

import logging
import re
import time
import xml.etree.ElementTree as ET
from typing import Any, Callable

import requests

from data import news_sources, serpapi_client

logger = logging.getLogger(__name__)

_TIMEOUT_SEC = 8
_CACHE_TTL_SEC = 600
_cache: dict[str, tuple[dict[str, Any], float]] = {}

# Real gap found in review, confirmed live: 70+ consecutive 503s over 90
# minutes on this exact endpoint (one per watchlist symbol per cycle, every
# single one silently caught and retried next cycle anyway) -- Google News
# RSS had no reactive backoff at all, unlike every rationed/keyed source in
# crypto_news.py. Same "Render's outbound IPs are shared across many
# unrelated apps" cause crypto_news.py's own CryptoSlate 429 comment
# documents -- not our own request volume, an external condition a
# per-symbol cache can't help with. 30 minutes (not a 24h quota-style
# pattern -- there's no fixed daily reset here, this is a soft
# availability signal that can clear anytime) stops hammering a
# currently-failing endpoint without risking a full day of degraded
# sentiment if it recovers sooner. Shared across both RSS fetchers below
# (same endpoint, same failure mode).
_GOOGLE_NEWS_RSS_COOLDOWN_SEC = 30 * 60
_google_news_rss_cooldown_until = 0.0

# General finance/equity vocabulary -- overlaps with crypto_news.py's lexicon
# where the words apply equally well to stocks (surge, rally, crash, ...),
# swapped out for equity-specific idioms crypto headlines rarely use
# (earnings beats/misses, guidance, downgrades) in place of pure crypto terms
# (adoption, etf, inflows) that would mostly just fail to match here anyway.
### "high"/"highs" removed from this list -- real bug found and fixed in
# review, same as crypto_news.py's own identical word list: `_score_headlines`
# is plain bag-of-words matching with no negation/context handling, and
# "high"/"highs" fired standalone (no offsetting negative word) on real
# headlines with the OPPOSITE of bullish meaning -- e.g. "Why Hasn't XRP
# Hit New Highs Like Bitcoin and Ethereum..." (an underperformance
# headline) scored purely positive from "highs" alone. Confirmed live via
# crypto_news.py's own archived sentiment_score history: heavily skewed
# positive (mean +0.50, 100% nonzero) across a real 66-day sample.
_POSITIVE_WORDS = {
    "surge", "rally", "bullish", "gain", "gains", "soar", "soars",
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


def _note_google_news_rss_cooldown(status_code: int) -> None:
    global _google_news_rss_cooldown_until
    _google_news_rss_cooldown_until = time.time() + _GOOGLE_NEWS_RSS_COOLDOWN_SEC
    logger.warning(
        "[stock_news] google news rss returned %d -- pausing this source for %.1f minutes",
        status_code, _GOOGLE_NEWS_RSS_COOLDOWN_SEC / 60,
    )


def _fetch_google_news_rss(query: str) -> list[str]:
    if time.time() < _google_news_rss_cooldown_until:
        return []
    url = "https://news.google.com/rss/search"
    try:
        resp = requests.get(url, params={"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"}, timeout=_TIMEOUT_SEC)
        if resp.status_code in (429, 503):
            _note_google_news_rss_cooldown(resp.status_code)
            return []
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        return [item.findtext("title") or "" for item in root.iter("item")][:30]
    except Exception as exc:
        logger.warning("[stock_news] google news rss failed for %r: %s", query, exc)
        return []


def _fetch_google_news_rss_items(query: str, *, limit: int = 10) -> list[dict[str, Any]]:
    """Same feed as _fetch_google_news_rss, but keeps title + link (a Google
    News redirect URL -- see threads_post.py's own OG-image resolver, which
    follows it to the real article to pull a photo, since this RSS feed
    itself carries no enclosure/media image the way crypto_news.py's direct
    newsroom feeds do) + source outlet name."""
    if time.time() < _google_news_rss_cooldown_until:
        return []
    url = "https://news.google.com/rss/search"
    try:
        resp = requests.get(url, params={"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"}, timeout=_TIMEOUT_SEC)
        if resp.status_code in (429, 503):
            _note_google_news_rss_cooldown(resp.status_code)
            return []
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        items = []
        for item in root.iter("item"):
            title = item.findtext("title") or ""
            if not title:
                continue
            source_el = item.find("source")
            items.append({
                "title": title, "link": item.findtext("link") or "",
                "source": (source_el.text if source_el is not None else None) or "Google News",
            })
        return items[:limit]
    except Exception as exc:
        logger.warning("[stock_news] google news rich rss failed for %r: %s", query, exc)
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


def get_sentiment(symbol: str, *, company_name: str | None = None, use_limited_sources: bool = True) -> dict[str, Any]:
    """Sentiment for one equity symbol. Cached per-symbol for
    _CACHE_TTL_SEC since news doesn't meaningfully change minute to minute --
    same TTL as crypto_news.get_sentiment().

    use_limited_sources gates SerpApi -- its 250-searches/month free tier is
    shared with crypto_news.py and the 30-minute trending-news Threads post
    (see serpapi_client.py's own cooldown). Callers should set this False
    for symbols outside the current watchlist, same discipline as
    crypto_news.get_sentiment()'s identical flag."""
    symbol = str(symbol or "").upper().strip()
    cached = _cache.get(symbol)
    now = time.time()
    if cached and (now - cached[1]) < _CACHE_TTL_SEC:
        return cached[0]

    query = _clean_company_query(symbol, company_name)
    headlines = _fetch_google_news_rss(query)
    if use_limited_sources:
        headlines.extend(serpapi_client.search_news(query))
        headlines.extend(news_sources.fetch_all(query))
    score, volume = _score_headlines(headlines)
    result = {
        "symbol": symbol, "sentiment_score": score, "headline_volume": volume,
        "computed_at": now,
    }
    _cache[symbol] = (result, now)
    return result


def prewarm_sentiment(
    symbols_and_names: list[tuple[str, str | None]], *, use_limited_sources: bool = True, max_workers: int = 8,
) -> None:
    """Fetches sentiment for every (symbol, company_name) pair CONCURRENTLY
    via a thread pool, populating the SAME per-symbol cache get_sentiment()
    itself reads -- every sequential get_sentiment() call made afterward in
    the same cycle becomes a cache hit instead of its own blocking fetch.
    Same real fix, same rationale, as crypto_news.prewarm_sentiment (see
    its own docstring for the full, confirmed root cause on the crypto
    side) -- this module shares the identical sequential-per-symbol-fetch
    shape via alpaca_data.py's collect_dataset_rows/latest_feature_row,
    used by both the stocks and options services.

    Best-effort: any symbol whose fetch fails or times out inside the pool
    just falls through to its own normal (slower) get_sentiment() call
    later in the sequential loop."""
    import concurrent.futures

    unique_pairs = list(dict.fromkeys((s, n) for s, n in symbols_and_names if s))  # de-dupe, preserve order
    if not unique_pairs:
        return
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(get_sentiment, symbol, company_name=name, use_limited_sources=use_limited_sources)
                for symbol, name in unique_pairs
            ]
            concurrent.futures.wait(futures, timeout=_TIMEOUT_SEC * 3)
    except Exception as exc:
        logger.debug("[stock_news] sentiment prewarm failed (non-fatal, per-symbol fetch will still run): %s", exc)


# General "what's happening in the stock market right now" query -- NOT
# per-symbol, so it's fetched once and cached on its own longer TTL, same
# shared-general-feed discipline as crypto_news.py's newsroom feeds.
_TRENDING_QUERY = "stock market"
_TRENDING_CACHE_TTL_SEC = 1800
_trending_cache: tuple[list[str], float] | None = None

# Real, confirmed pattern: a single fixed query's TOP result on a broad
# aggregator like Google News is often the SAME story for hours (the
# market's dominant narrative doesn't turn over every 30 minutes) -- so a
# caller that only ever looks at the top result runs dry (see
# get_trending_story's own exclude-based fix below) far more often than
# the underlying feed actually being empty. Rotating the query itself,
# once per cache window, gives each fetch a genuinely different angle on
# "what's happening" instead of re-asking the exact same question and
# getting the exact same sticky answer. Index by cache window (not
# fetch count) so this rotates in lockstep with the cache TTL below --
# every fresh fetch is automatically a different topic.
_TRENDING_QUERIES = (
    "stock market", "Wall Street stocks", "S&P 500", "Nasdaq stocks",
    "earnings report stocks", "Federal Reserve interest rate", "IPO stock market", "Dow Jones",
)


def _current_trending_query() -> str:
    idx = int(time.time() // _TRENDING_CACHE_TTL_SEC) % len(_TRENDING_QUERIES)
    return _TRENDING_QUERIES[idx]


def get_trending_headlines(*, limit: int = 5) -> list[str]:
    """General stock-market trending headlines -- NOT one symbol's own
    sentiment feed, just "what's happening in the market right now."
    Powers the 30-minute Threads trending-news post (see threads_post.py)."""
    global _trending_cache
    now = time.time()
    if _trending_cache and (now - _trending_cache[1]) < _TRENDING_CACHE_TTL_SEC:
        return _trending_cache[0][:limit]
    headlines = _fetch_google_news_rss(_TRENDING_QUERY)
    if headlines:  # don't cache a transient failure's empty result over a good one
        _trending_cache = (headlines, now)
    return headlines[:limit]


_trending_story_cache: tuple[list[dict[str, Any]], float, str] | None = None


def get_trending_story(
    *, query: str | None = None, exclude: Callable[[str], bool] | None = None,
) -> dict[str, Any] | None:
    """Picks ONE lead story for the Threads trending-news post. Unlike
    crypto_news.get_trending_story() (which corroborates across 3 distinct
    newsroom feeds), this is a single aggregator query -- Google News' own
    relevance ranking already puts its best-covered story first for a
    broad query, so the top result IS the popularity signal here. `query`
    defaults to the rotating topic (see _current_trending_query) rather
    than one fixed string, so consecutive fetches surface genuinely
    different real-world news instead of re-asking the same question.

    `exclude`, when given, is a predicate (e.g. "has this title already
    been posted recently?") applied in feed order across all fetched
    items -- the first item NOT excluded becomes the lead. Real, confirmed
    bug this replaces: the old version always took items[0] and gave up
    entirely (returning it as-is, letting the CALLER discover it's a
    duplicate and fall through to filler) even when items[1..9] held a
    genuinely fresh, unposted story the very next line down. Returns None
    only when every fetched item is excluded (or the feed failed) --
    i.e. there is truly nothing new to say, not just that the single
    top-ranked item happens to be stale.

    `link` is a Google redirect, not the real article URL -- see
    threads_post.py's OG-image resolver, which follows it and pulls a
    real photo from the actual page. Returns {"title", "link",
    "image_url": None, "source", "secondary": [titles...]}. Never raises
    -- same best-effort contract as the rest of this module."""
    global _trending_story_cache
    resolved_query = query or _current_trending_query()
    now = time.time()
    if (
        _trending_story_cache
        and _trending_story_cache[2] == resolved_query
        and (now - _trending_story_cache[1]) < _TRENDING_CACHE_TTL_SEC
    ):
        items = _trending_story_cache[0]
    else:
        items = _fetch_google_news_rss_items(resolved_query, limit=10)
        if items:
            _trending_story_cache = (items, now, resolved_query)
    if not items:
        return None
    lead = next((it for it in items if exclude is None or not exclude(it["title"])), None)
    if lead is None:
        return None
    secondary = [it["title"] for it in items if it is not lead][:3]
    return {
        "title": lead["title"], "link": lead["link"], "image_url": None,
        "source": lead["source"], "secondary": secondary,
    }
