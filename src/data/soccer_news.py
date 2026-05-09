"""
soccer_news.py — Soccer news + sentiment engine
================================================
Aggregates news from multiple free sources, scores sentiment per team/player,
and produces a combined signal (-1.0 to +1.0) used by the betting model.

Sources:
  1. NewsAPI (free tier, 100 req/day) — filtered by team/competition
    2. Multi-feed RSS (ESPN, BBC, Goal, Sky Sports, Guardian, Reuters)
    3. GDELT DOC API (free, no auth)
    4. Hugging Face inference API (BART/DistilBERT sentiment) OR keyword fallback

Caching: 2-hour TTL to respect NewsAPI's 100 req/day limit.
"""

from __future__ import annotations

import datetime
import os
import re
import time
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Any

import requests

# ── Config ────────────────────────────────────────────────────────────────────
_NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")
_HF_API_KEY   = os.getenv("HF_API_KEY", "")
_HF_MODEL     = "distilbert-base-uncased-finetuned-sst-2-english"

_CACHE_TTL = 7200  # 2 hours
_cache: dict[str, tuple[Any, float]] = {}

# RSS feeds (no auth required)
RSS_FEEDS = {
    "espn":    "https://www.espn.com/espn/rss/soccer/news",
    "bbc":     "https://feeds.bbci.co.uk/sport/football/rss.xml",
    "goal":    "https://www.goal.com/en/news/soccer/rss",
    "sky":     "https://www.skysports.com/rss/12040",
    "guardian": "https://www.theguardian.com/football/rss",
    "reuters": "https://feeds.reuters.com/reuters/sportsNews",
}

_GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

# Keywords: injury/suspension/controversy (negative) vs form/win/comeback (positive)
_NEGATIVE_SIGNALS = [
    "injured", "injury", "suspended", "suspension", "banned", "ban",
    "doubt", "doubtful", "sidelined", "ruled out", "limping", "fitness concern",
    "knocked out", "relegated", "loss", "losses", "defeated", "slump",
    "poor form", "crisis", "controversy", "scandal", "row", "dispute",
    "red card", "covid", "illness", "strain", "fracture", "tear",
    "hamstring", "knee", "ankle", "surgery", "operation",
]

_POSITIVE_SIGNALS = [
    "win", "won", "victory", "scored", "goal", "hat-trick",
    "comeback", "unbeaten", "form", "fit", "returns", "returned",
    "back in training", "confident", "motivated", "record",
    "champion", "champions", "trophy", "title", "best",
    "outstanding", "brilliant", "impressive", "impressive",
    "penalty", "assist", "brace", "good form", "flying",
]


# ── Caching helper ─────────────────────────────────────────────────────────────
def _cached(key: str, fn, *args, ttl: int = _CACHE_TTL, **kwargs) -> Any:
    now = time.time()
    if key in _cache:
        val, ts = _cache[key]
        if now - ts < ttl:
            return val
    val = fn(*args, **kwargs)
    _cache[key] = (val, now)
    return val


# ── NewsAPI ───────────────────────────────────────────────────────────────────
def _fetch_newsapi(query: str, max_results: int = 10) -> list[dict]:
    """Query NewsAPI for soccer news."""
    if not _NEWS_API_KEY:
        return []
    try:
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params={
                "q":        f"{query} soccer",
                "language": "en",
                "sortBy":   "publishedAt",
                "pageSize": max_results,
                "apiKey":   _NEWS_API_KEY,
            },
            timeout=8,
        )
        if r.status_code == 200:
            articles = r.json().get("articles", [])
            return [
                {
                    "title":       a.get("title", ""),
                    "description": a.get("description", ""),
                    "url":         a.get("url", ""),
                    "source":      a.get("source", {}).get("name", "NewsAPI"),
                    "published":   a.get("publishedAt", ""),
                    "text":        f"{a.get('title','')} {a.get('description','')}",
                }
                for a in articles
                if a.get("title")
            ]
    except Exception as e:
        print(f"[soccer_news] NewsAPI error: {e}")
    return []


# ── RSS feeds ─────────────────────────────────────────────────────────────────
def _fetch_rss(feed_url: str, team_filter: str | None = None) -> list[dict]:
    """Parse RSS feed and optionally filter by team name."""
    try:
        r = requests.get(feed_url, timeout=8,
                         headers={"User-Agent": "Mozilla/5.0 BettorBot/1.0"})
        if r.status_code != 200:
            return []
        root = ET.fromstring(r.content)
        items = root.findall(".//item")
        results = []
        for item in items[:30]:
            title       = (item.findtext("title") or "").strip()
            description = (item.findtext("description") or "").strip()
            link        = (item.findtext("link") or "").strip()
            pub_date    = (item.findtext("pubDate") or "").strip()
            text        = f"{title} {description}"
            if team_filter and team_filter.lower() not in text.lower():
                continue
            results.append({
                "title":       title,
                "description": description,
                "url":         link,
                "source":      feed_url.split("/")[2],
                "published":   pub_date,
                "text":        text,
            })
        return results
    except Exception as e:
        print(f"[soccer_news] RSS error for {feed_url}: {e}")
        return []


def _fetch_all_rss(team_filter: str | None = None) -> list[dict]:
    """Aggregate all RSS feeds."""
    all_articles: list[dict] = []
    for name, url in RSS_FEEDS.items():
        try:
            articles = _fetch_rss(url, team_filter=team_filter)
            all_articles.extend(articles)
        except Exception:
            pass
    # Deduplicate by title
    seen = set()
    unique = []
    for a in all_articles:
        t = a.get("title", "").lower().strip()[:80]
        if t not in seen:
            seen.add(t)
            unique.append(a)
    return unique


def _fetch_gdelt(query: str, max_results: int = 20) -> list[dict]:
    """Fetch free soccer headlines from GDELT DOC API."""
    q = (query or "soccer").strip()
    if not q:
        q = "soccer"
    try:
        r = requests.get(
            _GDELT_DOC_URL,
            params={
                "query": f'"{q}" soccer',
                "mode": "artlist",
                "maxrecords": max(5, min(max_results, 50)),
                "format": "json",
                "sourcelang": "english",
                "sort": "datedesc",
            },
            timeout=8,
        )
        if r.status_code != 200:
            return []
        articles = (r.json() or {}).get("articles", []) or []
        rows: list[dict] = []
        for a in articles:
            title = (a.get("title") or "").strip()
            if not title:
                continue
            domain = (a.get("domain") or "gdelt").strip() or "gdelt"
            rows.append({
                "title": title,
                "description": "",
                "url": (a.get("url") or "").strip(),
                "source": domain,
                "published": (a.get("seendate") or "").strip(),
                "text": title,
            })
        return rows
    except Exception as e:
        print(f"[soccer_news] GDELT error: {e}")
        return []


def _parse_published_dt(value: str | None) -> datetime.datetime | None:
    """Parse common RSS/ISO/GDELT timestamp formats into aware UTC datetimes."""
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        pass
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        pass
    for fmt in ("%Y%m%d%H%M%S", "%Y%m%dT%H%M%SZ", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.datetime.strptime(raw, fmt).replace(tzinfo=datetime.timezone.utc)
            return dt
        except Exception:
            continue
    return None


def _filter_recent_articles(articles: list[dict], max_age_hours: int) -> list[dict]:
    if max_age_hours <= 0:
        return list(articles)
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=max_age_hours)
    recent: list[dict] = []
    for a in articles:
        published_dt = _parse_published_dt(a.get("published"))
        if published_dt is None or published_dt >= cutoff:
            recent.append(a)
    recent.sort(
        key=lambda x: (_parse_published_dt(x.get("published")) or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)),
        reverse=True,
    )
    return recent


# ── Hugging Face sentiment ────────────────────────────────────────────────────
def _hf_sentiment(text: str) -> float:
    """
    Use HF Inference API for sentiment.
    Returns +1.0 (positive) to -1.0 (negative).
    Falls back to keyword scoring on error.
    """
    if not _HF_API_KEY or not text:
        return _keyword_sentiment(text)
    try:
        r = requests.post(
            f"https://api-inference.huggingface.co/models/{_HF_MODEL}",
            headers={"Authorization": f"Bearer {_HF_API_KEY}"},
            json={"inputs": text[:512]},
            timeout=6,
        )
        if r.status_code == 200:
            results = r.json()
            if isinstance(results, list) and results:
                label_scores = {item["label"]: item["score"] for item in results[0]}
                pos = label_scores.get("POSITIVE", 0.5)
                neg = label_scores.get("NEGATIVE", 0.5)
                return round(pos - neg, 4)
    except Exception:
        pass
    return _keyword_sentiment(text)


def _keyword_sentiment(text: str) -> float:
    """Fast keyword-based sentiment fallback. Returns -1.0 to +1.0."""
    if not text:
        return 0.0
    text_lower = text.lower()
    pos_score = sum(1 for kw in _POSITIVE_SIGNALS if kw in text_lower)
    neg_score = sum(1 for kw in _NEGATIVE_SIGNALS if kw in text_lower)
    total = pos_score + neg_score
    if total == 0:
        return 0.0
    raw = (pos_score - neg_score) / total
    return round(max(-1.0, min(1.0, raw)), 4)


# ── Public API ────────────────────────────────────────────────────────────────
def get_soccer_news(team: str | None = None,
                    competition: str | None = None,
                    max_age_hours: int = 24,
                    max_results: int = 15) -> list[dict]:
    """
    Get soccer news articles, optionally filtered by team or competition name.
    Articles older than max_age_hours are excluded.
    """
    query = team or competition or "soccer"
    cache_key = f"news_{query.lower().replace(' ', '_')}_{max_age_hours}_{max_results}"
    articles = _cached(cache_key, _do_fetch_news, query, max_results)
    return _filter_recent_articles(articles or [], max_age_hours)[:max_results]


def _do_fetch_news(query: str, max_results: int) -> list[dict]:
    """Internal: fetch and merge news from all sources."""
    results: list[dict] = []
    # 1. NewsAPI
    results.extend(_fetch_newsapi(query, max_results))
    # 2. RSS feeds filtered by team/competition query
    results.extend(_fetch_all_rss(team_filter=query if query != "soccer" else None))
    # 3. GDELT free global news monitor
    results.extend(_fetch_gdelt(query, max_results=max_results * 2))

    # Deduplicate
    seen: set[str] = set()
    unique: list[dict] = []
    for a in results:
        title = a.get("title", "").lower().strip()
        url = a.get("url", "").lower().strip()
        key = f"{title[:120]}|{url[:200]}"
        if not title or key in seen:
            continue
        seen.add(key)
        unique.append(a)

    unique.sort(
        key=lambda x: (_parse_published_dt(x.get("published")) or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)),
        reverse=True,
    )
    return unique[: max(max_results * 3, 30)]


def get_team_sentiment(team_name: str) -> dict:
    """
    Compute sentiment score for a team based on recent news.
    Returns:
        {
          "team": str,
          "score": float (-1.0 to +1.0),
          "label": "positive"|"negative"|"neutral",
          "article_count": int,
          "headlines": [str],
        }
    """
    cache_key = f"sentiment_{team_name.lower().replace(' ', '_')}"
    return _cached(cache_key, _compute_team_sentiment, team_name)


def _compute_team_sentiment(team_name: str) -> dict:
    articles = get_soccer_news(team=team_name)
    if not articles:
        return {
            "team":          team_name,
            "score":         0.0,
            "label":         "neutral",
            "article_count": 0,
            "headlines":     [],
            "sources":       [],
            "source_counts": {},
        }

    weighted_sum = 0.0
    total_weight = 0.0
    source_counts: dict[str, int] = {}
    for a in articles:
        source = str(a.get("source") or "unknown").strip().lower() or "unknown"
        idx = source_counts.get(source, 0)
        weight = 1.0 / (1.0 + (idx * 0.35))
        source_counts[source] = idx + 1
        score = _hf_sentiment(a.get("text", a.get("title", "")))
        weighted_sum += score * weight
        total_weight += weight

    avg_score = round(weighted_sum / total_weight, 4) if total_weight > 0 else 0.0
    label = "positive" if avg_score > 0.1 else "negative" if avg_score < -0.1 else "neutral"
    return {
        "team":          team_name,
        "score":         avg_score,
        "label":         label,
        "article_count": len(articles),
        "headlines":     [a.get("title", "") for a in articles[:5]],
        "sources":       sorted(source_counts.keys()),
        "source_counts": source_counts,
    }


def get_player_news(player_name: str) -> list[dict]:
    """Soccer news about a specific player."""
    cache_key = f"player_{player_name.lower().replace(' ', '_')}"
    return _cached(cache_key, _fetch_newsapi, player_name, 8)


def get_match_news_signal(home_team: str, away_team: str) -> dict:
    """
    Get combined news/sentiment signal for a specific match.
    Returns:
        {
          "home_team":         str,
          "away_team":         str,
          "home_sentiment":    float,
          "away_sentiment":    float,
          "home_label":        str,
          "away_label":        str,
          "combined_signal":   float,  # home - away, range -2 to +2
          "home_headlines":    [str],
          "away_headlines":    [str],
          "signal_strength":   "strong"|"moderate"|"weak",
        }
    """
    cache_key = f"match_{home_team[:8]}_{away_team[:8]}".lower().replace(" ", "_")
    return _cached(cache_key, _compute_match_signal, home_team, away_team)


def _compute_match_signal(home_team: str, away_team: str) -> dict:
    home_s = _compute_team_sentiment(home_team)
    away_s = _compute_team_sentiment(away_team)
    source_coverage = len(home_s.get("sources", [])) + len(away_s.get("sources", []))
    coverage_scale = 1.0 + min(0.20, max(0.0, (source_coverage - 2) * 0.03))
    combined = round((home_s["score"] - away_s["score"]) * coverage_scale, 4)
    strength = "strong" if abs(combined) > 0.4 else "moderate" if abs(combined) > 0.15 else "weak"
    return {
        "home_team":       home_team,
        "away_team":       away_team,
        "home_sentiment":  home_s["score"],
        "away_sentiment":  away_s["score"],
        "home_label":      home_s["label"],
        "away_label":      away_s["label"],
        "combined_signal": combined,
        "home_headlines":  home_s.get("headlines", []),
        "away_headlines":  away_s.get("headlines", []),
        "home_sources":    home_s.get("sources", []),
        "away_sources":    away_s.get("sources", []),
        "source_coverage": source_coverage,
        "signal_strength": strength,
    }


def get_competition_news(competition_name: str, max_results: int = 20) -> list[dict]:
    """News about an entire competition (e.g. 'Champions League', 'Premier League')."""
    cache_key = f"comp_news_{competition_name.lower().replace(' ', '_')}_{max_results}"
    articles = _cached(cache_key, _do_fetch_news, competition_name, max_results)
    return (articles or [])[:max_results]


def batch_team_sentiments(teams: list[str]) -> dict[str, dict]:
    """Compute sentiment for multiple teams at once. Useful for squad analysis."""
    results: dict[str, dict] = {}
    for team in teams:
        results[team] = get_team_sentiment(team)
    return results


def get_injury_alerts(team_name: str) -> list[str]:
    """Extract injury/suspension news snippets for a team."""
    articles = get_soccer_news(team=team_name)
    alerts: list[str] = []
    for a in articles:
        text = a.get("text", "").lower()
        for kw in ["injured", "suspended", "doubt", "ruled out", "sidelined",
                   "hamstring", "knee", "ankle", "surgery"]:
            if kw in text:
                alerts.append(a.get("title", ""))
                break
    return alerts[:5]


def clear_cache():
    """Clear all cached news/sentiment data."""
    global _cache
    _cache.clear()
