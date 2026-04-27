"""
GDELT Project — Free Historical News Sentiment Fetcher
======================================================
Uses the GDELT 2.0 DOC API (no API key required).
Fetches news articles about each MLB team for a date range,
scores sentiment, and persists to the news_articles DB table.

API docs: https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
Endpoint: https://api.gdeltproject.org/api/v2/doc/doc
  ?query=<team>+baseball
  &mode=artlist
  &maxrecords=250
  &format=json
  &startdatetime=YYYYMMDDHHMMSS
  &enddatetime=YYYYMMDDHHMMSS

Free — no auth required — rate limit: ~1 req/sec.
"""

import os
import sys
import time
import datetime
import re

import requests

SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SRC)

_GDELT_URL  = "https://api.gdeltproject.org/api/v2/doc/doc"
_TIMEOUT    = 20
_RATE_SLEEP = 1.2  # seconds between requests (polite crawl)

# All 30 MLB franchise city/nickname pairs for better queries
_MLB_TEAMS = [
    ("Yankees",    "New York Yankees"),
    ("Red Sox",    "Boston Red Sox"),
    ("Blue Jays",  "Toronto Blue Jays"),
    ("Orioles",    "Baltimore Orioles"),
    ("Rays",       "Tampa Bay Rays"),
    ("White Sox",  "Chicago White Sox"),
    ("Indians",    "Cleveland Guardians"),
    ("Tigers",     "Detroit Tigers"),
    ("Royals",     "Kansas City Royals"),
    ("Twins",      "Minnesota Twins"),
    ("Athletics",  "Oakland Athletics"),
    ("Astros",     "Houston Astros"),
    ("Angels",     "Los Angeles Angels"),
    ("Mariners",   "Seattle Mariners"),
    ("Rangers",    "Texas Rangers"),
    ("Mets",       "New York Mets"),
    ("Phillies",   "Philadelphia Phillies"),
    ("Marlins",    "Miami Marlins"),
    ("Braves",     "Atlanta Braves"),
    ("Nationals",  "Washington Nationals"),
    ("Cubs",       "Chicago Cubs"),
    ("Brewers",    "Milwaukee Brewers"),
    ("Cardinals",  "St. Louis Cardinals"),
    ("Pirates",    "Pittsburgh Pirates"),
    ("Reds",       "Cincinnati Reds"),
    ("Dodgers",    "Los Angeles Dodgers"),
    ("Giants",     "San Francisco Giants"),
    ("Padres",     "San Diego Padres"),
    ("Diamondbacks", "Arizona Diamondbacks"),
    ("Rockies",    "Colorado Rockies"),
]


def _gdelt_fetch_articles(team_keyword: str, start_dt: datetime.datetime,
                           end_dt: datetime.datetime) -> list[dict]:
    """
    Query GDELT DOC API for one team + date window.
    Returns list of raw article dicts {url, title, seendate, sourcecountry, …}.
    """
    start_s = start_dt.strftime("%Y%m%d%H%M%S")
    end_s   = end_dt.strftime("%Y%m%d%H%M%S")
    query   = f'"{team_keyword}" baseball'
    params  = {
        "query":         query,
        "mode":          "artlist",
        "maxrecords":    250,
        "format":        "json",
        "startdatetime": start_s,
        "enddatetime":   end_s,
        "sourcelang":    "english",
    }
    try:
        r = requests.get(_GDELT_URL, params=params, timeout=_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        return data.get("articles") or []
    except Exception as e:
        print(f"[gdelt] fetch error for '{team_keyword}': {e}")
        return []


def fetch_gdelt_sentiment(
    days_back: int = 30,
    teams: list[tuple] | None = None,
    verbose: bool = True,
) -> int:
    """
    Fetch GDELT news for all MLB teams over the last `days_back` days,
    score sentiment, and save to news_articles.

    Returns total rows persisted.
    """
    from data.sentiment import score_texts
    from data.db import save_news_articles

    if teams is None:
        teams = _MLB_TEAMS

    end_dt   = datetime.datetime.utcnow()
    start_dt = end_dt - datetime.timedelta(days=days_back)

    total_saved = 0
    for nickname, full_name in teams:
        if verbose:
            print(f"[gdelt] fetching '{nickname}' articles {start_dt.date()} → {end_dt.date()}")

        articles = _gdelt_fetch_articles(nickname, start_dt, end_dt)
        time.sleep(_RATE_SLEEP)

        if not articles:
            continue

        # Build text corpus for sentiment scoring
        texts = []
        for a in articles:
            title = (a.get("title") or "")
            texts.append(title[:512])

        scores = score_texts(texts)

        rows = []
        for a, s in zip(articles, scores):
            seen_raw = a.get("seendate", "")
            pub_at   = None
            if seen_raw:
                try:
                    pub_at = datetime.datetime.strptime(seen_raw[:8], "%Y%m%d")
                except Exception:
                    pass

            rows.append({
                "sport":        "mlb",
                "team":         full_name,
                "headline":     (a.get("title") or "")[:500],
                "description":  "",
                "url":          (a.get("url") or "")[:500],
                "source_name":  (a.get("domain") or "gdelt")[:100],
                "sentiment":    round(float(s), 3),
                "published_at": pub_at,
            })

        if rows:
            try:
                save_news_articles(rows)
                total_saved += len(rows)
                if verbose:
                    print(f"[gdelt]   {full_name}: {len(rows)} articles saved")
            except Exception as e:
                print(f"[gdelt]   save error for {full_name}: {e}")

    if verbose:
        print(f"[gdelt] Done — {total_saved} total articles persisted")
    return total_saved
