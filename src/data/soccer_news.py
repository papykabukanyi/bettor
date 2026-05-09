"""
soccer_news.py — Free soccer sentiment and market chatter signals
=================================================================
Aggregates free sources (RSS, Reddit, GDELT, optional NewsAPI) and
computes team sentiment + popular market discussion signals.

No paid API is required for baseline operation.
"""

from __future__ import annotations

import datetime
import re
import time
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from config import NEWS_API_KEY, REDDIT_USER_AGENT

_CACHE_TTL_SEC = 2 * 60 * 60
_cache: dict[str, tuple[Any, float]] = {}

RSS_FEEDS = {
    "espn": "https://www.espn.com/espn/rss/soccer/news",
    "bbc": "https://feeds.bbci.co.uk/sport/football/rss.xml",
    "goal": "https://www.goal.com/en/news/soccer/rss",
    "sky": "https://www.skysports.com/rss/12040",
    "guardian": "https://www.theguardian.com/football/rss",
    "reuters": "https://feeds.reuters.com/reuters/sportsNews",
}

REDDIT_SUBS = ("soccer", "sportsbook", "football", "PremierLeague")
_GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

_POSITIVE_TERMS = {
    "win", "winning", "dominant", "in form", "hot streak", "healthy",
    "fit", "returns", "back from injury", "clean sheet", "top form",
    "strong", "confident", "clinical", "momentum",
}
_NEGATIVE_TERMS = {
    "injury", "injured", "out", "doubt", "suspended", "ban", "banned",
    "fatigue", "tired", "poor form", "struggling", "slump", "crisis",
    "missing", "absence", "knock", "hamstring", "ankle", "knee",
}
_INJURY_TERMS = {
    "injury", "injured", "out", "doubt", "suspended", "ban", "banned",
    "hamstring", "ankle", "knee", "illness", "muscle", "strain",
}

_HOME_WIN_WORDS = {"to win", "moneyline", "ml", "1x2", "back", "pick"}
_DRAW_WORDS = {"draw", "stalemate", "tie", "x"}
_OVER_WORDS = {"over 2.5", "goals over", "high scoring", "over2.5"}
_UNDER_WORDS = {"under 2.5", "goals under", "low scoring", "under2.5"}
_BTTS_YES_WORDS = {"btts", "both teams to score", "gg"}
_BTTS_NO_WORDS = {"btts no", "both teams not to score", "clean sheet"}
_PLAYER_PROP_WORDS = {
    "anytime scorer", "goal scorer", "shots on target", "assist", "player prop",
    "prop bet", "cards", "corners", "passes", "tackles",
}


def _cached(key: str, fn, *args, **kwargs):
    now = time.time()
    existing = _cache.get(key)
    if existing and (now - existing[1]) < _CACHE_TTL_SEC:
        return existing[0]
    value = fn(*args, **kwargs)
    _cache[key] = (value, now)
    return value


def _parse_published_dt(value: str | None) -> datetime.datetime | None:
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


def _is_recent(article: dict, max_age_hours: int) -> bool:
    if max_age_hours <= 0:
        return True
    dt = _parse_published_dt(article.get("published"))
    if dt is None:
        return True
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=max_age_hours)
    return dt >= cutoff


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _news_article(title: str, description: str, url: str, source: str, published: str = "") -> dict:
    title_clean = _normalize_space(title)
    desc_clean = _normalize_space(description)
    return {
        "title": title_clean,
        "description": desc_clean,
        "url": (url or "").strip(),
        "source": (source or "unknown").strip().lower(),
        "published": (published or "").strip(),
        "text": _normalize_space(f"{title_clean}. {desc_clean}"),
    }


def _dedupe_articles(rows: list[dict]) -> list[dict]:
    seen: set[str] = set()
    unique: list[dict] = []
    for a in rows:
        title = (a.get("title") or "").lower().strip()
        url = (a.get("url") or "").lower().strip()
        key = f"{title[:140]}|{url[:220]}"
        if not title or key in seen:
            continue
        seen.add(key)
        unique.append(a)
    unique.sort(
        key=lambda x: (_parse_published_dt(x.get("published")) or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)),
        reverse=True,
    )
    return unique


def _fetch_newsapi(query: str, max_results: int = 25) -> list[dict]:
    if not NEWS_API_KEY:
        return []
    try:
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params={
                "q": query,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": max(5, min(max_results, 50)),
                "apiKey": NEWS_API_KEY,
            },
            timeout=8,
        )
        if r.status_code != 200:
            return []
        items = (r.json() or {}).get("articles", []) or []
        out: list[dict] = []
        for it in items:
            out.append(
                _news_article(
                    it.get("title", ""),
                    it.get("description", ""),
                    it.get("url", ""),
                    (it.get("source") or {}).get("name", "newsapi"),
                    it.get("publishedAt", ""),
                )
            )
        return out
    except Exception:
        return []


def _fetch_rss_feed(source: str, url: str, query_terms: list[str] | None = None) -> list[dict]:
    try:
        r = requests.get(url, timeout=8)
        if r.status_code != 200:
            return []
        root = ET.fromstring(r.content)
        out: list[dict] = []
        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            desc = (item.findtext("description") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub = (item.findtext("pubDate") or "").strip()
            if not title:
                continue
            txt = f"{title} {desc}".lower()
            if query_terms and not any(t in txt for t in query_terms):
                continue
            out.append(_news_article(title, desc, link, source, pub))
        return out
    except Exception:
        return []


def _fetch_all_rss(query: str, max_results: int = 30) -> list[dict]:
    terms = [t.lower() for t in re.findall(r"[a-zA-Z0-9]+", query or "") if len(t) >= 3]
    rows: list[dict] = []
    for source, url in RSS_FEEDS.items():
        rows.extend(_fetch_rss_feed(source, url, terms if terms else None))
    return rows[: max(max_results * 2, 40)]


def _fetch_gdelt(query: str, max_results: int = 30) -> list[dict]:
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
            rows.append(
                _news_article(
                    title,
                    "",
                    (a.get("url") or "").strip(),
                    (a.get("domain") or "gdelt").strip(),
                    (a.get("seendate") or "").strip(),
                )
            )
        return rows
    except Exception:
        return []


def _fetch_reddit(query: str, max_results: int = 30) -> list[dict]:
    out: list[dict] = []
    headers = {"User-Agent": REDDIT_USER_AGENT or "bettor-bot/1.0"}
    q = (query or "soccer").strip() or "soccer"
    for sub in REDDIT_SUBS:
        try:
            r = requests.get(
                f"https://www.reddit.com/r/{sub}/search.json",
                params={"q": q, "restrict_sr": 1, "sort": "new", "limit": max(8, min(max_results, 30))},
                headers=headers,
                timeout=8,
            )
            if r.status_code != 200:
                continue
            children = (((r.json() or {}).get("data") or {}).get("children") or [])
            for child in children:
                data = child.get("data") or {}
                title = (data.get("title") or "").strip()
                text = (data.get("selftext") or "").strip()
                if not title:
                    continue
                utc = data.get("created_utc")
                published = ""
                if utc:
                    try:
                        published = datetime.datetime.fromtimestamp(float(utc), tz=datetime.timezone.utc).isoformat()
                    except Exception:
                        published = ""
                out.append(
                    _news_article(
                        title,
                        text,
                        f"https://www.reddit.com{data.get('permalink', '')}",
                        f"reddit:{sub}",
                        published,
                    )
                )
        except Exception:
            continue
    return out


def _sentiment_score(text: str) -> float:
    low = (text or "").lower()
    if not low:
        return 0.0
    pos_hits = sum(1 for w in _POSITIVE_TERMS if w in low)
    neg_hits = sum(1 for w in _NEGATIVE_TERMS if w in low)
    total = pos_hits + neg_hits
    if total == 0:
        return 0.0
    score = (pos_hits - neg_hits) / max(total, 1)
    return round(max(-1.0, min(1.0, score)), 4)


def get_soccer_news(query: str = "soccer", max_results: int = 40, max_age_hours: int = 48) -> list[dict]:
    """Return merged free-source soccer headlines sorted by recency."""
    q = (query or "soccer").strip() or "soccer"
    key = f"news::{q.lower()}::{max_results}::{max_age_hours}"

    def _do_fetch() -> list[dict]:
        rows: list[dict] = []
        rows.extend(_fetch_newsapi(q, max_results=max_results))
        rows.extend(_fetch_all_rss(q, max_results=max_results))
        rows.extend(_fetch_gdelt(q, max_results=max_results * 2))
        rows.extend(_fetch_reddit(q, max_results=max_results))
        merged = _dedupe_articles(rows)
        return [a for a in merged if _is_recent(a, max_age_hours)]

    return _cached(key, _do_fetch)[:max_results]


def _compute_team_sentiment(team_name: str) -> dict:
    articles = get_soccer_news(query=team_name, max_results=40, max_age_hours=72)
    if not articles:
        return {
            "team": team_name,
            "score": 0.0,
            "label": "neutral",
            "article_count": 0,
            "headlines": [],
            "sources": [],
            "source_counts": {},
        }

    source_counts: dict[str, int] = {}
    weighted_sum = 0.0
    total_weight = 0.0

    for a in articles:
        source = str(a.get("source") or "unknown").strip().lower() or "unknown"
        idx = source_counts.get(source, 0)
        weight = 1.0 / (1.0 + (idx * 0.35))
        source_counts[source] = idx + 1
        s = _sentiment_score(a.get("text", ""))
        weighted_sum += s * weight
        total_weight += weight

    avg = round(weighted_sum / total_weight, 4) if total_weight > 0 else 0.0
    label = "positive" if avg > 0.12 else "negative" if avg < -0.12 else "neutral"

    return {
        "team": team_name,
        "score": avg,
        "label": label,
        "article_count": len(articles),
        "headlines": [a.get("title", "") for a in articles[:6]],
        "sources": sorted(source_counts.keys()),
        "source_counts": source_counts,
    }


def get_team_sentiment(team_name: str) -> dict:
    key = f"team_sent::{(team_name or '').strip().lower()}"
    return _cached(key, _compute_team_sentiment, team_name)


def get_market_popularity_signal(home_team: str, away_team: str) -> dict:
    """
    Estimate which markets are most discussed in free soccer chatter.
    Returns normalized scores in [0, 1].
    """
    query = f"{home_team} {away_team} betting odds"
    articles = get_soccer_news(query=query, max_results=80, max_age_hours=96)

    counts = {
        "home_win": 0,
        "draw": 0,
        "away_win": 0,
        "over_2_5": 0,
        "under_2_5": 0,
        "btts_yes": 0,
        "btts_no": 0,
        "player_props": 0,
    }

    home_low = (home_team or "").lower()
    away_low = (away_team or "").lower()

    for a in articles:
        text = (a.get("text") or "").lower()
        if not text:
            continue

        if any(k in text for k in _DRAW_WORDS):
            counts["draw"] += 1

        if any(k in text for k in _OVER_WORDS):
            counts["over_2_5"] += 1
        if any(k in text for k in _UNDER_WORDS):
            counts["under_2_5"] += 1

        if any(k in text for k in _BTTS_YES_WORDS):
            counts["btts_yes"] += 1
        if any(k in text for k in _BTTS_NO_WORDS):
            counts["btts_no"] += 1

        if any(k in text for k in _PLAYER_PROP_WORDS):
            counts["player_props"] += 1

        if home_low and home_low in text and any(k in text for k in _HOME_WIN_WORDS):
            counts["home_win"] += 1
        if away_low and away_low in text and any(k in text for k in _HOME_WIN_WORDS):
            counts["away_win"] += 1

    max_count = max(1, max(counts.values()) if counts else 1)
    scores = {k: round(v / max_count, 4) for k, v in counts.items()}

    top_sources = []
    seen_sources = set()
    for a in articles:
        source = (a.get("source") or "").strip()
        if source and source not in seen_sources:
            seen_sources.add(source)
            top_sources.append(source)
        if len(top_sources) >= 6:
            break

    return {
        "market_counts": counts,
        "market_scores": scores,
        "sample_size": len(articles),
        "top_sources": top_sources,
    }


def get_match_news_signal(home_team: str, away_team: str) -> dict:
    """Return combined sentiment signal for a matchup."""
    home_s = get_team_sentiment(home_team)
    away_s = get_team_sentiment(away_team)

    source_coverage = len(home_s.get("sources", [])) + len(away_s.get("sources", []))
    coverage_scale = 1.0 + min(0.20, max(0.0, (source_coverage - 2) * 0.03))
    combined = round((home_s["score"] - away_s["score"]) * coverage_scale, 4)
    strength = "strong" if abs(combined) > 0.4 else "moderate" if abs(combined) > 0.15 else "weak"

    market_signal = get_market_popularity_signal(home_team, away_team)

    return {
        "home_team": home_team,
        "away_team": away_team,
        "home_sentiment": home_s["score"],
        "away_sentiment": away_s["score"],
        "home_label": home_s["label"],
        "away_label": away_s["label"],
        "combined_signal": combined,
        "home_headlines": home_s.get("headlines", []),
        "away_headlines": away_s.get("headlines", []),
        "home_sources": home_s.get("sources", []),
        "away_sources": away_s.get("sources", []),
        "source_coverage": source_coverage,
        "signal_strength": strength,
        "market_popularity": market_signal,
    }


def get_injury_alerts(team_name: str, max_results: int = 8) -> list[dict]:
    """Return likely injury/suspension headlines for a team from free sources."""
    articles = get_soccer_news(query=team_name, max_results=60, max_age_hours=120)
    alerts: list[dict] = []
    low_team = (team_name or "").lower()

    for a in articles:
        text = (a.get("text") or "").lower()
        if low_team and low_team not in text:
            continue
        if not any(k in text for k in _INJURY_TERMS):
            continue

        severity = "medium"
        if any(k in text for k in ("out", "ruled out", "suspended", "ban", "banned")):
            severity = "high"
        elif any(k in text for k in ("doubt", "questionable", "knock")):
            severity = "low"

        alerts.append(
            {
                "team": team_name,
                "headline": a.get("title", ""),
                "source": a.get("source", ""),
                "url": a.get("url", ""),
                "published": a.get("published", ""),
                "severity": severity,
            }
        )

    return alerts[:max_results]
