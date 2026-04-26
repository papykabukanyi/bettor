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
_HF_MODEL   = "distilbert-base-uncased-finetuned-sst-2-english"
_HF_API_URL = f"https://api-inference.huggingface.co/models/{_HF_MODEL}"

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

def score_texts(texts: list[str], batch_size: int = 16) -> list[float]:
    """
    Score a list of texts using DistilBERT SST-2 via HF Inference API.
    Returns a list of sentiment scores in [-1, 1].
    +1 = very positive, -1 = very negative.
    Falls back to simple keyword scoring if HF API is unavailable.
    """
    global _HF_FAILED
    if not texts:
        return []

    # Try HF Inference API
    if HF_API_KEY and not _HF_FAILED:
        try:
            scores = []
            headers = {"Authorization": f"Bearer {HF_API_KEY}"}
            for i in range(0, len(texts), batch_size):
                batch = [t[:512] for t in texts[i:i + batch_size]]
                resp  = requests.post(_HF_API_URL, headers=headers,
                                      json={"inputs": batch}, timeout=15)
                if resp.status_code == 200:
                    results = resp.json()
                    for r in results:
                        if isinstance(r, list):
                            pos = next((x["score"] for x in r if x["label"] == "POSITIVE"), 0.5)
                            scores.append(pos * 2 - 1)  # [0,1] → [-1,+1]
                        else:
                            scores.append(0.0)
                else:
                    print(f"[sentiment] HF API status {resp.status_code} — falling back to keyword scoring")
                    _HF_FAILED = True
                    break
            if len(scores) == len(texts):
                return scores
        except Exception as e:
            print(f"[sentiment] HF API error: {e} — using keyword fallback")
            _HF_FAILED = True

    # ── Keyword fallback (no external API needed) ─────────────────────────
    return [_keyword_sentiment(t) for t in texts]


def _keyword_sentiment(text: str) -> float:
    """Simple lexicon-based sentiment as fallback."""
    positive = {"win", "wins", "great", "amazing", "excellent", "hot", "fire",
                "streak", "dominant", "crushing", "beast", "clutch", "ace",
                "comeback", "solid", "on fire", "rolling", "strong", "shutdown",
                "perfect", "lights out", "no hitter", "homer", "blast", "bomb"}
    negative = {"loss", "losses", "terrible", "awful", "slump", "cold", "injury",
                "injured", "out", "disabled", "struggling", "bad", "horrible",
                "worst", "can't hit", "giving up", "collapse", "blown", "blew",
                "ejected", "benched", "scratched", "day to day", "dl", "il"}
    lower  = text.lower()
    words  = set(re.findall(r"\b\w+\b", lower))
    pos    = sum(1 for w in positive if w in lower)
    neg    = sum(1 for w in negative if w in lower)
    total  = pos + neg
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

def get_news_sentiment(entity: str, entity_type: str = "team") -> dict:
    """
    Fetch recent news headlines via NewsAPI and score their sentiment.
    Returns {score, volume, keywords, source, articles}.
    """
    global _NEWS_FAILED
    if _NEWS_FAILED or not NEWS_API_KEY or NEWS_API_KEY.startswith("YOUR_"):
        return {}

    # MLB team names work better with "Yankees" vs "New York Yankees" in queries
    search_q = entity.split()[-1] if entity_type == "team" else entity
    # Add "MLB" context to avoid cross-sport confusion
    query = f"{search_q} MLB baseball"

    url    = "https://newsapi.org/v2/everything"
    params = {
        "q":          query,
        "language":   "en",
        "sortBy":     "publishedAt",
        "pageSize":   20,
        "from":       (datetime.date.today() - datetime.timedelta(days=3)).isoformat(),
        "apiKey":     NEWS_API_KEY,
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 426:  # Plan upgrade required
            _NEWS_FAILED = True
            return {}
        if resp.status_code != 200:
            return {}
        data = resp.json()
        articles = data.get("articles", [])
        if not articles:
            return {}

        texts = [
            f"{a.get('title','')} {a.get('description','')}"
            for a in articles
        ]
        scores   = score_texts(texts)
        avg_score = round(sum(scores) / len(scores), 4) if scores else 0.0

        # Collect rich article data for DB
        article_rows = []
        for a, s in zip(articles, scores):
            article_rows.append({
                "sport":       "mlb",
                "team":        entity,
                "headline":    a.get("title", "")[:500],
                "description": a.get("description", "")[:1000],
                "url":         a.get("url", "")[:500],
                "source_name": (a.get("source") or {}).get("name", "")[:100],
                "sentiment":   round(s, 3),
                "published_at": a.get("publishedAt"),
            })

        # Save to DB
        try:
            from data.db import save_news_articles
            save_news_articles(article_rows)
        except Exception:
            pass

        all_text = " ".join(texts).lower()
        words    = re.findall(r"\b[a-z]{4,}\b", all_text)
        from collections import Counter
        stop     = {"that", "this", "they", "with", "have", "from", "will", "game",
                    "baseball", "team", "player", "said", "their", "just", "been"}
        freq     = Counter(w for w in words if w not in stop)
        keywords = ", ".join(w for w, _ in freq.most_common(8))

        return {
            "score":    avg_score,
            "volume":   len(articles),
            "keywords": keywords,
            "source":   "news",
            "articles": article_rows[:5],
        }
    except Exception as e:
        print(f"[sentiment] News sentiment error for {entity}: {e}")
        return {}


# ─── Combined team / player sentiment ────────────────────────────────────────

def get_team_sentiment(team_name: str) -> dict:
    """
    Get combined sentiment for a team from Reddit + News.
    Saves results to DB. Returns {reddit, news, combined} score dict.
    """
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
        total_w   = sum(weights)
        combined  = round(sum(s * w for s, w in zip(scores, weights)) / total_w, 4)

    # Persist
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
    """Get combined sentiment for a player from Reddit + News."""
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
