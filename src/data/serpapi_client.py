"""SerpApi (serpapi.com) Google News search -- a shared, budget-aware client
used by BOTH crypto_news.py and stock_news.py as an additional optional
sentiment source, plus the 30-minute "trending news" Threads post job.

SerpApi's free tier is 250 searches/month TOTAL for this account, shared
across every caller above -- nowhere near enough for a per-symbol,
per-cycle call (100 watchlist symbols x every 2-15 minutes would burn the
whole month's budget in under an hour). This module is the single
chokepoint that enforces a real, conservative minimum interval BETWEEN any
two actual network calls (_SERPAPI_MIN_INTERVAL_SEC, default 4 hours -> at
most 6 calls/day -> ~180/month, a comfortable margin under 250 that still
leaves room for ad-hoc/manual use). Every caller shares this SAME cooldown
clock -- whichever one asks first in a given window gets the real call;
everyone else in that window gets the cached result instead of a fresh one.

This is a soft, in-memory-only budget guard (resets on process restart),
not a hard billing enforcement -- the goal is "make 250 credits/month last
comfortably under normal operation," not "guarantee never exceeding it
under adversarial conditions."
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY", "")
_TIMEOUT_SEC = 10
_SEARCH_URL = "https://serpapi.com/search.json"

# See module docstring for the budget math behind this default.
_SERPAPI_MIN_INTERVAL_SEC = int(os.getenv("SERPAPI_MIN_INTERVAL_SEC", str(4 * 3600)) or str(4 * 3600))
_last_call_ts = 0.0
_last_result_cache: dict[str, list[str]] = {}

# Purely observational -- logged so usage is visible in the logs well
# before 250/month is ever actually at risk; not itself an enforcement
# mechanism (the interval cooldown above is what actually limits calls).
_calls_this_month = 0
_month_key = ""


def _bump_month_counter() -> None:
    global _calls_this_month, _month_key
    current_month = time.strftime("%Y-%m")
    if current_month != _month_key:
        _month_key = current_month
        _calls_this_month = 0
    _calls_this_month += 1
    if _calls_this_month in (200, 230, 250):
        logger.warning(
            "[serpapi_client] %d SerpApi calls so far this month (free tier is 250/month total)",
            _calls_this_month,
        )


def search_news(query: str, *, num: int = 10) -> list[str]:
    """Headline titles for `query` via SerpApi's google_news engine. Returns
    the LAST cached result (possibly for a different query -- see below) if
    still within the cooldown window, [] if never configured, and never
    raises. Skipped silently without SERPAPI_API_KEY set.

    Deliberately caches by "most recent call" rather than per-query: the
    cooldown exists specifically to cap TOTAL calls across every caller
    sharing this one 250/month budget, not to give each query its own
    independent allowance -- a per-query cache would let N distinct queries
    each get their own fresh call every cooldown window, defeating the
    whole point."""
    global _last_call_ts
    if not SERPAPI_API_KEY:
        return []
    now = time.time()
    if (now - _last_call_ts) < _SERPAPI_MIN_INTERVAL_SEC:
        return _last_result_cache.get(query, next(iter(_last_result_cache.values()), []))

    # Real, confirmed bug found in review: this used to only update
    # _last_call_ts on a SUCCESSFUL response (after raise_for_status()), so
    # once the account's real quota was exhausted (or any transient
    # failure occurred), the cooldown gate above never actually engaged --
    # EVERY subsequent call, for every symbol in whatever loop is calling
    # this, made its own real network attempt instead of being skipped,
    # forever, until a call happened to succeed again. Confirmed live: a
    # single sentiment-fetch cycle burned through 30+ consecutive 429s
    # (one full network round-trip per coin in crypto's own universe),
    # tying up the single-threaded worker long enough to make the
    # dashboard itself intermittently hang. Recording the ATTEMPT (not
    # just success) here means a failure still starts the real cooldown,
    # so the very next call in the same loop is skipped immediately
    # instead of also making -- and also failing -- its own network call.
    _last_call_ts = now
    try:
        resp = requests.get(
            _SEARCH_URL,
            params={"engine": "google_news", "q": query, "gl": "us", "hl": "en", "api_key": SERPAPI_API_KEY, "num": num},
            timeout=_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        _bump_month_counter()
        data: dict[str, Any] = resp.json()
        results = data.get("news_results") or []
        titles = [r.get("title", "") for r in results if r.get("title")][:num]
        _last_result_cache.clear()
        _last_result_cache[query] = titles
        return titles
    except Exception as exc:
        logger.warning("[serpapi_client] search failed for %r: %s", query, exc)
        return []
