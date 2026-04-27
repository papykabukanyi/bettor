"""
Sentiment Analysis Engine
=========================
Sources:
  1. Reddit API (praw)   — r/baseball, r/mlb, team subreddits
  2. News API            — team/player news headlines
  3. Hugging Face        — DistilBERT SST-2 sentiment scoring via Inference API

Usage:
    from data.sentiment import get_team_sentiment, get_player_sentiment, score_texts
"""

import os
import sys
import re
import json
import datetime
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import NEWS_API_KEY, REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT, HF_API_KEY

# ─── Hugging Face Inference API ──────────────────────────────────────────────
# HF changed to a new router endpoint in 2025; try both bases in order
_HF_MODELS = [
    "cardiffnlp/twitter-roberta-base-sentiment-latest",
    "distilbert-base-uncased-finetuned-sst-2-english",
    "ProsusAI/finbert",
]
_HF_API_BASES = [
    "https://router.huggingface.co/hf-inference/models",
    "https://api-inference.huggingface.co/models",
]

# ─── Circuit breakers ────────────────────────────────────────────────────────
_REDDIT_FAILED   = False
_HF_FAILED       = False
_NEWS_FAILED     = False

# ─── MLB team → subreddit mapping ────────────────────────────────────────────
_TEAM_SUBREDDITS = {
    "yankees":      "NYYankees",
    "red sox":      "redsox",
    "blue jays":    "TorontoBlueJays",
    "rays":         "TampaBayRays",
    "orioles":      "orioles",
    "white sox":    "whitesox",
    "guardians":    "clevelandguardians",
    "tigers":       "motorcitykitties",
    "royals":       "KCRoyals",
    "twins":        "minnesotatwins",
    "astros":       "Astros",
    "athletics":    "OaklandAthletics",
    "mariners":     "Mariners",
    "angels":       "angelsbaseball",
    "rangers":      "TexasRangers",
    "braves":       "Braves",
    "phillies":     "phillies",
    "mets":         "NewYorkMets",
    "marlins":      "letsgofish",
    "nationals":    "Nationals",
    "cubs":         "CHICubs",
    "cardinals":    "Cardinals",
    "brewers":      "Brewers",
    "reds":         "reds",
    "pirates":      "buccos",
    "dodgers":      "Dodgers",
    "giants":       "SFGiants",
    "padres":       "Padres",
    "rockies":      "ColoradoRockies",
    "diamondbacks": "azdiamondbacks",
}


def _team_subreddit(team_name: str) -> str:
    """Return the team-specific subreddit name."""
    lower = team_name.lower()
    for keyword, sub in _TEAM_SUBREDDITS.items():
        if keyword in lower:
            return sub
    return ""


# ─── Hugging Face sentiment scoring ─────────────────────────────────────────

def _hf_parse_scores(results: list, model: str) -> list[float]:
    """Parse HF inference API response into [-1, +1] scores."""
    scores = []
    for r in results:
        if isinstance(r, list):
            # SST-2 / finbert style: [{label, score}, ...]
            pos_labels = {"POSITIVE", "positive", "POS", "pos", "LABEL_2", "label_2"}
            neg_labels = {"NEGATIVE", "negative", "NEG", "neg", "LABEL_0", "label_0"}
            pos = next((x["score"] for x in r if x["label"] in pos_labels), None)
            neg = next((x["score"] for x in r if x["label"] in neg_labels), None)
            if pos is not None:
                scores.append(pos * 2 - 1)
            elif neg is not None:
                scores.append(-(neg * 2 - 1))
            else:
                scores.append(0.0)
        elif isinstance(r, dict) and "label" in r:
            lbl = r.get("label", "").upper()
            sc  = r.get("score", 0.5)
            if lbl in ("POSITIVE", "POS", "LABEL_2"):
                scores.append(sc * 2 - 1)
            elif lbl in ("NEGATIVE", "NEG", "LABEL_0"):
                scores.append(-(sc * 2 - 1))
            else:
                scores.append(0.0)
        else:
            scores.append(0.0)
    return scores


def score_texts(texts: list[str], batch_size: int = 16) -> list[float]:
    """
    Score texts via HF Inference API (tries multiple models).
    Returns scores in [-1, +1].  Falls back to keyword scoring.
    """
    global _HF_FAILED
    if not texts:
        return []

    if HF_API_KEY and not _HF_FAILED:
        headers = {"Authorization": f"Bearer {HF_API_KEY}"}
        for base in _HF_API_BASES:
            for model in _HF_MODELS:
                url = f"{base}/{model}"
                try:
                    all_scores: list[float] = []
                    failed = False
                    for i in range(0, len(texts), batch_size):
                        batch = [t[:512] for t in texts[i:i + batch_size]]
                        payload = {"inputs": batch, "options": {"wait_for_model": True}}
                        resp = requests.post(url, headers=headers, json=payload, timeout=30)
                        if resp.status_code == 200:
                            raw = resp.json()
                            # Normalize flat format: [[d1, d2, ...]] → [[d1], [d2], ...]
                            if (isinstance(raw, list) and len(raw) == 1
                                    and isinstance(raw[0], list)
                                    and len(raw[0]) == len(batch)
                                    and isinstance(raw[0][0], dict)
                                    and "label" in raw[0][0]):
                                raw = [[x] for x in raw[0]]
                            parsed = _hf_parse_scores(raw, model)
                            all_scores.extend(parsed)
                        elif resp.status_code == 503:
                            print(f"[sentiment] HF model {model} still loading — trying next")
                            failed = True
                            break
                        else:
                            failed = True
                            break
                    if not failed and len(all_scores) == len(texts):
                        return all_scores
                except Exception:
                    pass

        print("[sentiment] HF unavailable — using keyword fallback")
        _HF_FAILED = True

    return [_keyword_sentiment(t) for t in texts]


def _keyword_sentiment(text: str) -> float:
    """Extended MLB-specific lexicon-based sentiment as fallback."""
    positive = {
        # performance
        "win", "wins", "won", "great", "amazing", "excellent", "elite",
        "hot", "fire", "streak", "dominant", "dominates", "crushing",
        "beast", "clutch", "ace", "comeback", "solid", "rolling", "strong",
        "shutdown", "perfect", "lights out", "no hitter", "no-hitter",
        "homer", "blast", "bomb", "dinger", "grand slam", "cycle",
        "rbi", "walk-off", "walkoff", "saves", "efficient", "dominant",
        # form
        "on fire", "in form", "top form", "prime", "healthy", "activated",
        "returns", "back", "cleared", "ready", "available", "confident",
        "extension", "career high", "record", "breakout",
        # stats
        "strikeout", "strikeouts", "shutout", "complete game",
        "batting average", "home run", "home runs",
    }
    negative = {
        # results
        "loss", "losses", "lost", "terrible", "awful", "slump", "cold",
        "injury", "injured", "out", "disabled", "struggling", "bad",
        "horrible", "worst", "giving up", "collapse", "blown", "blew",
        "ejected", "benched", "scratched", "day to day", "dl", "il",
        # health
        "strained", "strain", "sprained", "sprain", "fracture", "fractured",
        "surgery", "procedure", "rehab", "rehabbing", "shut down", "placed on",
        "placed", "hamstring", "oblique", "elbow", "shoulder", "wrist",
        "back pain", "side session", "limited", "questionable", "doubtful",
        "missed", "skipped", "unable", "won't pitch", "not available",
        # form
        "rough", "blown save", "walks", "wild", "inconsistent", "struggling",
        "early exit", "knocked out", "rocked", "shelled", "ineffective",
    }
    lower = text.lower()
    pos   = sum(1 for w in positive if w in lower)
    neg   = sum(1 for w in negative if w in lower)
    total = pos + neg
    if total == 0:
        return 0.0
    return round((pos - neg) / total, 3)


# ─── Reddit sentiment ────────────────────────────────────────────────────────

def _get_reddit_instance():
    """Return a praw Reddit instance, or None if credentials are missing."""
    global _REDDIT_FAILED
    if _REDDIT_FAILED:
        return None
    if not REDDIT_CLIENT_ID or REDDIT_CLIENT_ID.startswith("YOUR_"):
        return None
    try:
        import praw
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT,
            read_only=True,
        )
        return reddit
    except Exception as e:
        print(f"[sentiment] Reddit init error: {e}")
        _REDDIT_FAILED = True
        return None


def get_reddit_sentiment(entity: str, entity_type: str = "team",
                          post_limit: int = 30) -> dict:
    """
    Fetch recent Reddit posts/comments mentioning an entity, score them.
    Returns {score, volume, keywords, source}.
    """
    global _REDDIT_FAILED
    reddit = _get_reddit_instance()
    if not reddit:
        return {}

    try:
        texts = []
        # Search r/baseball and r/mlb for the entity
        for sub_name in ["baseball", "mlb"]:
            try:
                sub = reddit.subreddit(sub_name)
                for post in sub.search(entity, limit=post_limit // 2, time_filter="day",
                                       sort="new"):
                    if post.selftext:
                        texts.append(post.title + " " + post.selftext[:200])
                    else:
                        texts.append(post.title)
            except Exception:
                pass

        # Also search team-specific subreddit if available
        if entity_type == "team":
            team_sub = _team_subreddit(entity)
            if team_sub:
                try:
                    sub = reddit.subreddit(team_sub)
                    for post in sub.hot(limit=20):
                        texts.append(post.title)
                except Exception:
                    pass

        if not texts:
            return {}

        scores = score_texts(texts)
        avg_score = round(sum(scores) / len(scores), 4) if scores else 0.0

        # Extract top keywords
        all_text = " ".join(texts).lower()
        words    = re.findall(r"\b[a-z]{4,}\b", all_text)
        from collections import Counter
        stop     = {"that", "this", "they", "with", "have", "from", "will", "game",
                    "baseball", "team", "player", "said", "their", "just", "been",
                    "would", "could", "should", "when", "what", "about", "more"}
        freq     = Counter(w for w in words if w not in stop)
        keywords = ", ".join(w for w, _ in freq.most_common(8))

        return {
            "score":    avg_score,
            "volume":   len(texts),
            "keywords": keywords,
            "source":   "reddit",
        }
    except Exception as e:
        print(f"[sentiment] Reddit sentiment error for {entity}: {e}")
        _REDDIT_FAILED = True
        return {}


# ─── News API sentiment ───────────────────────────────────────────────────────

_NEWS_STOP = {
    "that", "this", "they", "with", "have", "from", "will", "game",
    "baseball", "team", "player", "said", "their", "just", "been",
    "would", "could", "should", "when", "what", "about", "more",
    "also", "after", "into", "over", "first", "season", "last",
    "year", "week", "time", "some", "were", "been", "than",
}


def _news_fetch_articles(query: str, days_back: int = 7,
                          page_size: int = 100,
                          start_date: str = None,
                          end_date: str = None) -> list[dict]:
    """
    Single NewsAPI /everything call.  Returns raw article dicts or [].
    Handles 426 (plan limit) and other errors gracefully.
    """
    global _NEWS_FAILED
    url    = "https://newsapi.org/v2/everything"
    params = {
        "q":          query,
        "language":   "en",
        "searchIn":   "title,description",
        "sortBy":     "publishedAt",
        "pageSize":   min(int(page_size), 100),
        "apiKey":     NEWS_API_KEY,
    }
    if start_date and end_date:
        params["from"] = start_date
        params["to"] = end_date
    else:
        params["from"] = (datetime.date.today() - datetime.timedelta(days=days_back)).isoformat()
    try:
        resp = requests.get(url, params=params, timeout=12)
        if resp.status_code == 426:   # paid plan required
            _NEWS_FAILED = True
            return []
        if resp.status_code == 429:   # rate limited
            return []
        if resp.status_code != 200:
            print(f"[sentiment] NewsAPI status {resp.status_code} for: {query[:60]}")
            return []
        return resp.json().get("articles", [])
    except Exception as e:
        print(f"[sentiment] NewsAPI fetch error: {e}")
        return []


def get_news_sentiment(entity: str, entity_type: str = "team") -> dict:
    """
    Fetch recent news via NewsAPI with multiple targeted queries and score
    sentiment using HuggingFace DistilBERT (or keyword fallback).

    Strategy (no Reddit, News-only):
      Query 1 — general performance/results:  "<entity>" AND (MLB OR baseball)
      Query 2 — injury / availability news:   "<entity>" AND (injury OR IL OR scratch)
      Query 3 — stats / prop context (players only):
                 "<entity>" AND (strikeout OR "home run" OR hits OR RBI OR stats)

    Up to 100 articles per query, de-duplicated by URL.
    Lookback: 7 days (vs prior 3).
    """
    global _NEWS_FAILED
    if _NEWS_FAILED or not NEWS_API_KEY or NEWS_API_KEY.startswith("YOUR_"):
        return {}

    from collections import Counter

    # Build entity token — for teams use last word ("Yankees"), for players use full name
    if entity_type == "team":
        short = entity.split()[-1]   # "New York Yankees" → "Yankees"
        q_general  = f'"{short}" AND (MLB OR baseball)'
        q_injury   = f'"{short}" AND (injury OR injured OR IL OR scratched OR "day to day")'
        queries    = [q_general, q_injury]
    else:
        # Player: use quoted full name for precision
        q_general  = f'"{entity}" AND (MLB OR baseball)'
        q_injury   = f'"{entity}" AND (injury OR injured OR IL OR scratched OR limited)'
        q_stats    = (f'"{entity}" AND '
                      f'(strikeout OR "home run" OR hits OR RBI OR stats OR performance)')
        queries    = [q_general, q_injury, q_stats]

    # ── Fetch + de-duplicate ──────────────────────────────────────────────
    seen_urls: set[str] = set()
    all_articles: list[dict] = []

    for q in queries:
        if _NEWS_FAILED:
            break
        raw = _news_fetch_articles(q, days_back=7, page_size=100)
        for a in raw:
            url_ = a.get("url", "")
            if url_ and url_ not in seen_urls:
                seen_urls.add(url_)
                all_articles.append(a)

    if not all_articles:
        return {}

    # ── Score all texts ───────────────────────────────────────────────────
    texts  = [
        f"{a.get('title') or ''} {a.get('description') or ''}"
        for a in all_articles
    ]
    scores     = score_texts(texts)
    avg_score  = round(sum(scores) / len(scores), 4) if scores else 0.0

    # ── Persist articles to DB ────────────────────────────────────────────
    article_rows = []
    for a, s in zip(all_articles, scores):
        article_rows.append({
            "sport":       "mlb",
            "team":        entity,
            "headline":    (a.get("title") or "")[:500],
            "description": (a.get("description") or "")[:1000],
            "url":         (a.get("url") or "")[:500],
            "source_name": (a.get("source") or {}).get("name", "")[:100],
            "sentiment":   round(s, 3),
            "published_at": a.get("publishedAt"),
        })
    try:
        from data.db import save_news_articles
        save_news_articles(article_rows)
    except Exception:
        pass

    # ── Top keywords ──────────────────────────────────────────────────────
    all_text = " ".join(texts).lower()
    words    = re.findall(r"\b[a-z]{4,}\b", all_text)
    freq     = Counter(w for w in words if w not in _NEWS_STOP)
    keywords = ", ".join(w for w, _ in freq.most_common(10))

    print(f"[sentiment] News: {entity!r} → {len(all_articles)} articles "
          f"({len(queries)} queries), score={avg_score:+.3f}")

    return {
        "score":    avg_score,
        "volume":   len(all_articles),
        "keywords": keywords,
        "source":   "news",
        "articles": article_rows[:8],   # top-8 for display
    }


def _parse_rss_items(xml_text: str) -> list[dict]:
    """Basic RSS parser (title/description/link/pubDate)."""
    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml_text)
        items = []
        for item in root.findall(".//item"):
            items.append({
                "title": (item.findtext("title") or "").strip(),
                "description": (item.findtext("description") or "").strip(),
                "url": (item.findtext("link") or "").strip(),
                "publishedAt": (item.findtext("pubDate") or "").strip(),
                "source": {"name": "rss"},
            })
        return items
    except Exception:
        return []


def _google_news_rss(query: str) -> list[dict]:
    """Fallback: Google News RSS (recent headlines only)."""
    try:
        from urllib.parse import quote
        q = quote(query)
        url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
        resp = requests.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return []
        return _parse_rss_items(resp.text)
    except Exception:
        return []


def fetch_news_history(entity: str, start_date: str, end_date: str,
                       entity_type: str = "team") -> list[dict]:
    """
    Fetch historical news for an entity between start_date and end_date.
    Uses NewsAPI /everything when available; falls back to Google News RSS.
    Returns article rows (also saved to DB).
    """
    global _NEWS_FAILED

    if entity_type == "team":
        short = entity.split()[-1]
        q_general = f'"{short}" AND (MLB OR baseball)'
        q_injury  = f'"{short}" AND (injury OR injured OR IL OR scratched OR "day to day")'
        queries   = [q_general, q_injury]
    else:
        q_general = f'"{entity}" AND (MLB OR baseball)'
        q_injury  = f'"{entity}" AND (injury OR injured OR IL OR scratched OR limited)'
        q_stats   = (f'"{entity}" AND '
                     f'(strikeout OR "home run" OR hits OR RBI OR stats OR performance)')
        queries   = [q_general, q_injury, q_stats]

    seen_urls: set[str] = set()
    all_articles: list[dict] = []

    for q in queries:
        raw = []
        if NEWS_API_KEY and not _NEWS_FAILED:
            raw = _news_fetch_articles(q, page_size=100,
                                       start_date=start_date, end_date=end_date)
        if not raw:
            raw = _google_news_rss(q)
        for a in raw:
            url_ = a.get("url", "")
            if url_ and url_ not in seen_urls:
                seen_urls.add(url_)
                all_articles.append(a)

    if not all_articles:
        return []

    texts  = [f"{a.get('title') or ''} {a.get('description') or ''}" for a in all_articles]
    scores = score_texts(texts)

    article_rows = []
    for a, s in zip(all_articles, scores):
        article_rows.append({
            "sport":       "mlb",
            "team":        entity,
            "headline":    (a.get("title") or "")[:500],
            "description": (a.get("description") or "")[:1000],
            "url":         (a.get("url") or "")[:500],
            "source_name": (a.get("source") or {}).get("name", "")[:100],
            "sentiment":   round(float(s), 3),
            "published_at": a.get("publishedAt") or None,
        })

    try:
        from data.db import save_news_articles
        save_news_articles(article_rows)
    except Exception:
        pass

    return article_rows


# ─── Combined team / player sentiment ────────────────────────────────────────

def get_team_sentiment(team_name: str) -> dict:
    """
    Get sentiment for a team using NewsAPI (Reddit used automatically when
    credentials are configured; silently skipped otherwise).
    Returns {reddit, news, combined} score dict.
    """
    # Reddit: only runs when real credentials are set
    reddit_data = get_reddit_sentiment(team_name, entity_type="team")
    news_data   = get_news_sentiment(team_name, entity_type="team")

    scores  = []
    weights = []
    if reddit_data.get("score") is not None and reddit_data.get("volume", 0) > 0:
        scores.append(float(reddit_data["score"]))
        weights.append(reddit_data["volume"])
    if news_data.get("score") is not None and news_data.get("volume", 0) > 0:
        scores.append(float(news_data["score"]))
        weights.append(news_data["volume"])

    combined = 0.0
    if scores:
        total_w  = sum(weights)
        combined = round(sum(s * w for s, w in zip(scores, weights)) / total_w, 4)

    try:
        from data.db import save_sentiment
        if reddit_data:
            save_sentiment(team_name, "team", "reddit",
                           reddit_data.get("score", 0.0),
                           reddit_data.get("volume", 0),
                           reddit_data.get("keywords", ""))
        if news_data:
            save_sentiment(team_name, "team", "news",
                           news_data.get("score", 0.0),
                           news_data.get("volume", 0),
                           news_data.get("keywords", ""))
        if scores:
            save_sentiment(team_name, "team", "combined", combined,
                           sum(weights), "")
    except Exception as e:
        print(f"[sentiment] DB save error: {e}")

    return {
        "team":     team_name,
        "reddit":   reddit_data,
        "news":     news_data,
        "combined": combined,
        "volume":   sum(weights),
    }


def get_player_sentiment(player_name: str) -> dict:
    """Get combined sentiment for a player from NewsAPI (+ Reddit when configured)."""
    reddit_data = get_reddit_sentiment(player_name, entity_type="player", post_limit=20)
    news_data   = get_news_sentiment(player_name, entity_type="player")

    scores  = []
    weights = []
    if reddit_data.get("score") is not None and reddit_data.get("volume", 0) > 0:
        scores.append(float(reddit_data["score"]))
        weights.append(reddit_data["volume"])
    if news_data.get("score") is not None and news_data.get("volume", 0) > 0:
        scores.append(float(news_data["score"]))
        weights.append(news_data["volume"])

    combined = 0.0
    if scores:
        total_w   = sum(weights)
        combined  = round(sum(s * w for s, w in zip(scores, weights)) / total_w, 4)

    try:
        from data.db import save_sentiment
        if scores:
            save_sentiment(player_name, "player", "combined", combined,
                           sum(weights), "")
    except Exception:
        pass

    return {
        "player":   player_name,
        "reddit":   reddit_data,
        "news":     news_data,
        "combined": combined,
    }


def get_game_sentiments(home_team: str, away_team: str) -> dict:
    """
    Get sentiment for both teams in a game.
    Returns {home: {...}, away: {...}} with combined scores.
    """
    # Check DB cache first (within last 12h)
    try:
        from data.db import get_sentiment as db_sentiment
        home_cached = db_sentiment(home_team, hours=12)
        away_cached = db_sentiment(away_team, hours=12)
        if home_cached.get("combined") and away_cached.get("combined"):
            return {
                "home": {"combined": home_cached["combined"]["score"],
                         "news_keywords": home_cached.get("news", {}).get("keywords", "")},
                "away": {"combined": away_cached["combined"]["score"],
                         "news_keywords": away_cached.get("news", {}).get("keywords", "")},
            }
    except Exception:
        pass

    home_sent = get_team_sentiment(home_team)
    away_sent = get_team_sentiment(away_team)
    return {
        "home": {
            "combined":      home_sent["combined"],
            "reddit_score":  home_sent["reddit"].get("score", 0),
            "news_score":    home_sent["news"].get("score", 0),
            "news_keywords": home_sent["news"].get("keywords", ""),
            "volume":        home_sent["volume"],
        },
        "away": {
            "combined":      away_sent["combined"],
            "reddit_score":  away_sent["reddit"].get("score", 0),
            "news_score":    away_sent["news"].get("score", 0),
            "news_keywords": away_sent["news"].get("keywords", ""),
            "volume":        away_sent["volume"],
        },
    }


# ─── Player prop signal (historical + sentiment) ──────────────────────────────

# How volatile each stat is: higher = wider normal distribution = more uncertainty
_STAT_STD_FACTORS = {
    "strikeouts":         0.35,  # K/start: moderate variance
    "hits":               0.55,  # H/game: fairly volatile
    "home_runs":          0.80,  # HR: rare/bursty
    "total_bases":        0.50,
    "rbi":                0.65,
    "runs":               0.60,
    "walks":              0.60,
    "stolen_bases":       0.75,
    "batter_strikeouts":  0.55,
    "doubles":            0.70,
}

_STAT_UNITS = {
    "strikeouts":         "Ks",
    "hits":               "H",
    "home_runs":          "HR",
    "total_bases":        "TB",
    "rbi":                "RBI",
    "runs":               "R",
    "walks":              "BB",
    "stolen_bases":       "SB",
    "batter_strikeouts":  "K",
    "doubles":            "2B",
}

# Keys to try when reading per-game avg from DB trend JSONB blobs
_STAT_TREND_KEYS = {
    "strikeouts":         ["strikeouts", "k", "so", "avg"],
    "hits":               ["hits", "h", "avg"],
    "home_runs":          ["home_runs", "hr", "avg"],
    "total_bases":        ["total_bases", "tb", "avg"],
    "rbi":                ["rbi", "avg"],
    "runs":               ["runs", "r", "avg"],
    "walks":              ["walks", "bb", "avg"],
    "stolen_bases":       ["stolen_bases", "sb", "avg"],
    "batter_strikeouts":  ["batter_strikeouts", "k", "so", "avg"],
    "doubles":            ["doubles", "2b", "d", "avg"],
}


def _extract_trend_avg(blob, stat_type: str):
    """Pull per-game average out of a trend JSONB blob (dict or raw float)."""
    if blob is None:
        return None
    if isinstance(blob, (int, float)):
        return float(blob)
    if isinstance(blob, dict):
        for key in _STAT_TREND_KEYS.get(stat_type, ["avg"]):
            if key in blob:
                try:
                    return float(blob[key])
                except (TypeError, ValueError):
                    pass
    return None


def _over_prob_norm(avg_val, line: float, std_factor: float) -> float:
    """P(X > line) using a normal approximation centred on avg_val."""
    if avg_val is None or avg_val <= 0:
        return 0.5
    try:
        from scipy.stats import norm
        std = max(float(avg_val) * std_factor, 0.1)
        return float(norm.sf(line, loc=float(avg_val), scale=std))
    except Exception:
        return 0.65 if float(avg_val) > line else 0.35


def get_player_prop_signal(
    player_name:  str,
    stat_type:    str,
    line:         float,
    prop_data:    dict = None,
    pitcher_hand: str  = None,   # "L" or "R" for batter split
    venue:        str  = None,   # "home" or "away"
) -> dict:
    """
    Generate a directional OVER/UNDER signal for a player prop by combining:
      1. Historical player trends from DB (last_5, last_10, season_avg, splits)
      2. Reddit + News sentiment scored by HuggingFace DistilBERT

    Weights: 85 % historical performance / 15 % sentiment.

    Returns:
        direction      — "OVER" or "UNDER"
        probability    — P(OVER) as a float [0, 1]
        confidence     — % confidence in the chosen direction (int)
        rationale      — human-readable explanation e.g.
                         "OVER 6.5 Ks · L5 avg 7.2 ↑ · season 6.8 · positive buzz (+0.31)"
        hist_prob      — blended historical probability before sentiment
        sentiment_score — combined sentiment score [-1, +1]
        data_sources   — list of data layers actually used
    """
    std_factor   = _STAT_STD_FACTORS.get(stat_type, 0.50)
    stat_unit    = _STAT_UNITS.get(stat_type, stat_type[:3].upper())
    rationale_parts: list[str] = []
    data_sources:    list[str] = []

    # ── 1. Pull historical trends from DB ─────────────────────────────────
    season_prob  = None
    last5_prob   = None
    last10_prob  = None
    matchup_prob = None
    venue_prob   = None

    season_avg_val = None
    last5_avg_val  = None
    last10_avg_val = None

    try:
        from data.db import get_player_trends
        cur_year = datetime.date.today().year
        trend_rows = get_player_trends(player_name, season=cur_year)
        if not trend_rows:
            trend_rows = get_player_trends(player_name, season=cur_year - 1)

        # Find the trend row whose stat_type overlaps with the requested prop
        _pitch_props = {"strikeouts"}
        _bat_props   = {"hits", "home_runs", "total_bases", "rbi", "runs",
                        "walks", "stolen_bases", "batter_strikeouts", "doubles"}

        for t in trend_rows:
            ttype = (t.get("stat_type") or "").lower()
            is_hit = (
                stat_type == ttype
                or (stat_type in _pitch_props and ttype == "pitching")
                or (stat_type in _bat_props   and ttype == "batting")
            )
            if not is_hit:
                continue

            sa = t.get("season_avg")
            if sa is not None:
                season_avg_val = float(sa)
                season_prob    = _over_prob_norm(season_avg_val, line, std_factor)
                data_sources.append("season_avg")

            l5_avg = _extract_trend_avg(t.get("last_5"), stat_type)
            if l5_avg is not None:
                last5_avg_val = l5_avg
                last5_prob    = _over_prob_norm(l5_avg, line, std_factor)
                data_sources.append("last_5")

            l10_avg = _extract_trend_avg(t.get("last_10"), stat_type)
            if l10_avg is not None:
                last10_avg_val = l10_avg
                last10_prob    = _over_prob_norm(l10_avg, line, std_factor)
                data_sources.append("last_10")

            # Pitcher handedness split (for batters)
            if pitcher_hand:
                split_key = "vs_lefty" if pitcher_hand.upper() == "L" else "vs_righty"
                split_val = t.get(split_key)
                if split_val is not None:
                    matchup_prob = _over_prob_norm(float(split_val), line, std_factor)
                    data_sources.append(f"vs_{pitcher_hand.upper()}")

            # Home/away split
            if venue:
                venue_key = f"{venue.lower()}_avg"
                venue_val = t.get(venue_key)
                if venue_val is not None:
                    venue_prob = _over_prob_norm(float(venue_val), line, std_factor)
                    data_sources.append(f"{venue}_split")

            break  # use first matching trend row

    except Exception as e:
        print(f"[sentiment] prop_signal DB error: {e}")

    # ── 2. Fall back to raw prop data when no DB trends ───────────────────
    if prop_data and season_avg_val is None:
        avg_pg = prop_data.get("avg_per_game")
        if avg_pg:
            season_avg_val = float(avg_pg)
            season_prob    = _over_prob_norm(season_avg_val, line, std_factor)
            data_sources.append("model_avg")

    # ── 3. Weighted historical probability ────────────────────────────────
    # Recent form weighs the most; season average is the baseline anchor.
    hist_components = []
    hist_weights    = []
    if last5_prob   is not None: hist_components.append(last5_prob);   hist_weights.append(0.40)
    if last10_prob  is not None: hist_components.append(last10_prob);  hist_weights.append(0.25)
    if season_prob  is not None: hist_components.append(season_prob);  hist_weights.append(0.20)
    if matchup_prob is not None: hist_components.append(matchup_prob); hist_weights.append(0.10)
    if venue_prob   is not None: hist_components.append(venue_prob);   hist_weights.append(0.05)

    if hist_components:
        total_w   = sum(hist_weights)
        hist_prob = sum(p * w for p, w in zip(hist_components, hist_weights)) / total_w
    elif prop_data:
        hist_prob = float(prop_data.get("over_prob", 0.5))
    else:
        hist_prob = 0.5

    # ── 4. Sentiment score (check DB cache, then live fetch) ──────────────
    sentiment_score = 0.0
    try:
        from data.db import get_sentiment as _db_sent
        cached = _db_sent(player_name, hours=12)
        if cached.get("combined"):
            sentiment_score = float(cached["combined"]["score"])
            data_sources.append("sentiment_cached")
        elif not _HF_FAILED and not _NEWS_FAILED:
            sent = get_player_sentiment(player_name)
            sentiment_score = float(sent.get("combined", 0))
            if abs(sentiment_score) > 0.05:
                data_sources.append("sentiment_live")
    except Exception as e:
        print(f"[sentiment] prop_signal sentiment fetch error: {e}")

    # ── 5. Blend: 85 % historical + 15 % sentiment nudge ─────────────────
    # Positive sentiment → slightly more likely OVER.
    # Scale: sentiment score ±1.0 maps to ±12 % swing (boosted: News is sole source).
    sentiment_adj = sentiment_score * 0.12
    final_prob    = max(0.10, min(0.90, hist_prob + sentiment_adj))

    # ── 6. Build human-readable rationale ────────────────────────────────
    direction  = "OVER" if final_prob >= 0.5 else "UNDER"
    conf_prob  = final_prob if direction == "OVER" else 1.0 - final_prob
    confidence = round(conf_prob * 100)

    if last5_avg_val is not None and last10_avg_val is not None:
        trend_arrow = "↑" if last5_avg_val > last10_avg_val else "↓"
        rationale_parts.append(f"L5 {last5_avg_val:.1f} {stat_unit} {trend_arrow}")
    elif last5_avg_val is not None:
        rationale_parts.append(f"L5 avg {last5_avg_val:.1f} {stat_unit}")

    if last10_avg_val is not None:
        rationale_parts.append(f"L10 {last10_avg_val:.1f}")

    if season_avg_val is not None and "season_avg" in data_sources:
        rationale_parts.append(f"season {season_avg_val:.1f}")

    if matchup_prob is not None:
        hand_str = "vs LHP" if (pitcher_hand or "").upper() == "L" else "vs RHP"
        rationale_parts.append(hand_str)

    if abs(sentiment_score) > 0.10:
        buzz = "positive buzz" if sentiment_score > 0 else "negative buzz"
        rationale_parts.append(f"{buzz} ({sentiment_score:+.2f})")

    if not rationale_parts:
        # Nothing from DB — just show the model average
        if season_avg_val is not None:
            rationale_parts.append(f"avg {season_avg_val:.1f} {stat_unit}/game")
        else:
            rationale_parts.append("model projection")

    rationale = f"{direction} {line} {stat_unit} · " + " · ".join(rationale_parts)

    return {
        "direction":       direction,
        "probability":     round(final_prob, 4),   # P(OVER)
        "confidence":      confidence,             # % confidence in chosen direction
        "rationale":       rationale,
        "hist_prob":       round(hist_prob, 4),
        "sentiment_score": round(sentiment_score, 4),
        "data_sources":    data_sources,
    }
