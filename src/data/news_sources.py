"""Three more general-purpose news search APIs -- newsapi.org, TheNewsAPI,
and World News API -- each with their own free-tier DAILY quota, shared as
additional optional sentiment sources by BOTH crypto_news.py and
stock_news.py (and the 30-minute trending-news Threads post).

Same "manage the budget, don't just react to a failure" discipline as
newsdata.io/serpapi_client.py: a real minimum interval between calls to
EACH source, sized from that source's own stated daily cap with a safety
margin -- not a race to the exact limit followed by a day-long retry-and-
fail loop once it 429s. Each source has its OWN independent cooldown clock
(unlike serpapi_client.py's single shared clock -- these are three
separate accounts/keys, not one tightly-shared 250/month budget), so all
three can genuinely be called back-to-back without contending with each
other.

  newsapi.org   -- https://newsapi.org/v2/everything, apiKey param.
                   Free "Developer" plan is commonly 100 req/day (not
                   independently confirmed for this account -- override
                   via NEWSAPI_ORG_CALLS_PER_DAY if different).
  TheNewsAPI    -- https://api.thenewsapi.com/v1/news/all, api_token param.
                   Confirmed 100/day free tier for this account.
  World News API -- https://api.worldnewsapi.com/search-news, x-api-key
                   header. Confirmed 50 points/day for this account
                   (assumed 1 point per search-news call here).

Set NEWSAPI_ORG_KEY / THENEWSAPI_TOKEN / WORLDNEWSAPI_KEY to enable each;
skipped silently without its key, same as every other optional source.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

_TIMEOUT_SEC = 8

NEWSAPI_ORG_KEY = os.getenv("NEWSAPI_ORG_KEY", "")
THENEWSAPI_TOKEN = os.getenv("THENEWSAPI_TOKEN", "")
WORLDNEWSAPI_KEY = os.getenv("WORLDNEWSAPI_KEY", "")

# Stay comfortably under each source's own stated daily cap rather than
# riding it exactly to the edge -- traffic isn't perfectly even across a
# day, and a few extra ad-hoc/manual calls should still fit.
_SAFETY_MARGIN = 0.8

_NEWSAPI_ORG_CALLS_PER_DAY = float(os.getenv("NEWSAPI_ORG_CALLS_PER_DAY", "100") or "100")
_THENEWSAPI_CALLS_PER_DAY = float(os.getenv("THENEWSAPI_CALLS_PER_DAY", "100") or "100")
_WORLDNEWSAPI_CALLS_PER_DAY = float(os.getenv("WORLDNEWSAPI_CALLS_PER_DAY", "50") or "50")

_last_call_ts: dict[str, float] = {}


def _min_interval_sec(calls_per_day: float) -> float:
    return 86400.0 / max(1.0, calls_per_day * _SAFETY_MARGIN)


def _cooldown_ok(source_name: str, calls_per_day: float) -> bool:
    """True (and marks the call as spent) if `source_name` is outside its
    own cooldown window. Marks the slot as used the moment a real call is
    ABOUT to be issued -- not only after a successful response -- so a
    request that used real quota but then failed to parse still counts
    against the budget, same conservative posture as newsdata.io's own
    429-triggered cooldown."""
    now = time.time()
    last = _last_call_ts.get(source_name, 0.0)
    if (now - last) < _min_interval_sec(calls_per_day):
        return False
    _last_call_ts[source_name] = now
    return True


def fetch_newsapi_org(query: str) -> list[str]:
    if not NEWSAPI_ORG_KEY or not _cooldown_ok("newsapi_org", _NEWSAPI_ORG_CALLS_PER_DAY):
        return []
    try:
        resp = requests.get(
            "https://newsapi.org/v2/everything",
            params={"q": query, "apiKey": NEWSAPI_ORG_KEY, "language": "en", "sortBy": "publishedAt", "pageSize": 10},
            timeout=_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        articles: list[dict[str, Any]] = resp.json().get("articles") or []
        return [a.get("title", "") for a in articles if a.get("title")]
    except Exception as exc:
        logger.warning("[news_sources] newsapi.org fetch failed for %r: %s", query, exc)
        return []


def fetch_thenewsapi(query: str) -> list[str]:
    if not THENEWSAPI_TOKEN or not _cooldown_ok("thenewsapi", _THENEWSAPI_CALLS_PER_DAY):
        return []
    try:
        resp = requests.get(
            "https://api.thenewsapi.com/v1/news/all",
            params={"search": query, "api_token": THENEWSAPI_TOKEN, "language": "en", "limit": 10},
            timeout=_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        articles: list[dict[str, Any]] = resp.json().get("data") or []
        return [a.get("title", "") for a in articles if a.get("title")]
    except Exception as exc:
        logger.warning("[news_sources] thenewsapi fetch failed for %r: %s", query, exc)
        return []


def fetch_worldnewsapi(query: str) -> list[str]:
    if not WORLDNEWSAPI_KEY or not _cooldown_ok("worldnewsapi", _WORLDNEWSAPI_CALLS_PER_DAY):
        return []
    try:
        resp = requests.get(
            "https://api.worldnewsapi.com/search-news",
            params={"text": query, "number": 10},
            headers={"x-api-key": WORLDNEWSAPI_KEY},
            timeout=_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        articles: list[dict[str, Any]] = resp.json().get("news") or []
        return [a.get("title", "") for a in articles if a.get("title")]
    except Exception as exc:
        logger.warning("[news_sources] worldnewsapi fetch failed for %r: %s", query, exc)
        return []


def fetch_all(query: str) -> list[str]:
    """All three sources' headlines for one query, each independently
    cooldown-gated and skipped silently if unconfigured or still cooling
    down. The single call site crypto_news.py/stock_news.py actually use."""
    headlines: list[str] = []
    headlines.extend(fetch_newsapi_org(query))
    headlines.extend(fetch_thenewsapi(query))
    headlines.extend(fetch_worldnewsapi(query))
    return headlines
