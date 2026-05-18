"""
Sentiment Analysis Engine  —  Multi-Source, All Sports
=======================================================
Active sources (ordered by reliability / no-key preference):

  1. ESPN News Feed       — free, no key, official; covers all major sports
  2. Google News RSS      — free, no key; broad coverage
  3. Yahoo Sports RSS     — free, no key; sport-specific feeds
  4. CBS Sports RSS       — free, no key; sport-specific feeds
  5. Bleacher Report RSS  — free, no key; sport-specific feeds
  6. RotoBaller RSS       — free, no key; prop/injury focus
  7. Reddit JSON API      — free, no key; real-time fan discussion
  8. GDELT DOC API        — free, no key; global news corpus
  9. NewsAPI              — optional key; broader search
 10. newsdata.io          — optional key (free tier 200/day)
 11. Discord              — optional bot token; real-time channels
 12. Reddit praw          — optional OAuth; higher rate limits

REMOVED: TikTok (undocumented internal API, always blocked server-side)

Each source:
  - Degrades silently if unavailable
  - Returns a consistent {score, volume, keywords, source, signal_type,
    injury_flag, momentum_flag, lineup_flag, articles} dict
  - Has a per-source in-memory TTL cache to avoid redundant fetches
"""

from __future__ import annotations

import os
import sys
import re
import json
import datetime
import math
import unicodedata
import time
from collections import Counter
from typing import Any

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import (
    NEWS_API_KEY,
    NEWSDATA_API_KEY,
    REDDIT_CLIENT_ID,
    REDDIT_CLIENT_SECRET,
    REDDIT_USER_AGENT,
    HF_API_KEY,
    DISCORD_BOT_TOKEN,
    DISCORD_CHANNELS,
    DISCORD_CHANNELS_BY_SPORT,
    DISCORD_LOOKBACK_HOURS,
    DISCORD_MAX_MESSAGES,
    DISCORD_CACHE_MINUTES,
    DISCORD_ENABLE_ATTACHMENT_OCR,
    DISCORD_MAX_IMAGE_ATTACHMENTS,
    DISCORD_OCR_TIMEOUT_SECONDS,
    SOCIAL_PLAYER_MIN_MENTIONS,
    SOCIAL_MAX_PLAYERS_PER_GAME,
)

# --- Hugging Face models (tried in order) ---
_HF_MODELS = [
    "cardiffnlp/twitter-roberta-base-sentiment-latest",
    "distilbert-base-uncased-finetuned-sst-2-english",
    "ProsusAI/finbert",
]
_HF_OCR_MODELS = [
    "microsoft/trocr-base-printed",
    "Salesforce/blip-image-captioning-base",
]
_HF_API_BASES = [
    "https://router.huggingface.co/hf-inference/models",
    "https://api-inference.huggingface.co/models",
]

# --- Per-source weight caps ---
_SENTIMENT_WEIGHT_CAPS = {
    "espn_news":  10.0,   # official sport news - highest trust
    "newsdata":    6.0,   # newsdata.io sport API
    "news":        8.0,   # NewsAPI
    "rss":         6.0,   # Yahoo/CBS/BR/RotoBaller RSS
    "reddit":      5.0,   # Reddit (praw or JSON)
    "gdelt":       3.0,   # GDELT historical corpus
    "discord":     4.0,   # Discord channels
}

# --- Circuit breakers ---
_REDDIT_FAILED        = False
_HF_FAILED            = False
_NEWS_FAILED          = False
_NEWSDATA_FAILED      = False
_DISCORD_FAILED       = False
_DISCORD_AUTH_LOGGED  = False

# --- In-memory TTL caches ---
_DISCORD_CACHE:           dict = {}
_DISCORD_IMAGE_OCR_CACHE: dict = {}
_ESPN_NEWS_CACHE:         dict = {}
_RSS_CACHE:               dict = {}
_REDDIT_JSON_CACHE:       dict = {}
_GDELT_CACHE:             dict = {}
_NEWSDATA_CACHE:          dict = {}
_SOCIAL_PLAYER_CACHE:     dict = {}
_HIST_PLAYER_CACHE:       dict = {}

_CACHE_TTL_SHORT = 300    # 5 min  - live feeds
_CACHE_TTL_MED   = 1800   # 30 min - news
_CACHE_TTL_LONG  = 3600   # 1 h    - historical


def _cache_fresh(store: dict, key: str, ttl: float):
    row = store.get(key)
    if row and (time.time() - row.get("_ts", 0)) < ttl:
        return row.get("data")
    return None


def _cache_store(store: dict, key: str, data) -> None:
    store[key] = {"data": data, "_ts": time.time()}


# --- Sport -> ESPN slug / subreddit / RSS mapping ---
_ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"

_ESPN_NEWS_SLUGS = {
    "baseball":         ["baseball/mlb"],
    "mlb":              ["baseball/mlb"],
    "basketball":       ["basketball/nba"],
    "nba":              ["basketball/nba"],
    "wnba":             ["basketball/wnba"],
    "americanfootball": ["football/nfl"],
    "nfl":              ["football/nfl"],
    "icehockey":        ["hockey/nhl"],
    "nhl":              ["hockey/nhl"],
    "soccer":           ["soccer/eng.1", "soccer/usa.1", "soccer/esp.1"],
    "mma":              [],
}

_SPORT_SUBREDDITS = {
    "baseball":         ["baseball", "mlb"],
    "mlb":              ["baseball", "mlb"],
    "basketball":       ["nba", "basketball"],
    "nba":              ["nba"],
    "wnba":             ["wnba"],
    "americanfootball": ["nfl", "footballstrategy"],
    "nfl":              ["nfl"],
    "icehockey":        ["hockey", "nhl"],
    "nhl":              ["nhl"],
    "soccer":           ["soccer", "mls", "PremierLeague"],
    "mma":              ["MMA", "ufc"],
    "all":              ["baseball", "nba", "nfl", "nhl", "soccer"],
}

_TEAM_SUBREDDITS = {
    # MLB
    "yankees":       "NYYankees",       "red sox":     "redsox",
    "blue jays":     "TorontoBlueJays", "rays":        "TampaBayRays",
    "orioles":       "orioles",          "white sox":  "whitesox",
    "guardians":     "clevelandguardians", "tigers":   "motorcitykitties",
    "royals":        "KCRoyals",         "twins":      "minnesotatwins",
    "astros":        "Astros",           "athletics":  "OaklandAthletics",
    "mariners":      "Mariners",         "angels":     "angelsbaseball",
    "rangers":       "TexasRangers",     "braves":     "Braves",
    "phillies":      "phillies",         "mets":       "NewYorkMets",
    "marlins":       "letsgofish",       "nationals":  "Nationals",
    "cubs":          "CHICubs",          "cardinals":  "Cardinals",
    "brewers":       "Brewers",          "reds":       "reds",
    "pirates":       "buccos",           "dodgers":    "Dodgers",
    "giants":        "SFGiants",         "padres":     "Padres",
    "rockies":       "ColoradoRockies",  "diamondbacks": "azdiamondbacks",
    # NBA
    "lakers":        "lakers",           "celtics":    "bostonceltics",
    "warriors":      "warriors",         "bucks":      "mkebucks",
    "knicks":        "NYKnicks",         "heat":       "heat",
    "76ers":         "sixers",           "nuggets":    "denvernuggets",
    "suns":          "suns",             "nets":       "GoNets",
    "bulls":         "chicagobulls",     "clippers":   "LAClippers",
    "mavericks":     "Mavericks",        "thunder":    "thunder",
    "spurs":         "NBASpurs",         "jazz":       "UtahJazz",
    "hawks":         "AtlantaHawks",     "hornets":    "CharlotteHornets",
    "magic":         "OrlandoMagic",     "wizards":    "washingtonwizards",
    "cavaliers":     "clevelandcavs",    "pistons":    "DetroitPistons",
    "pacers":        "pacers",           "raptors":    "torontoraptors",
    "76ers":         "sixers",           "timberwolves": "timberwolves",
    # NFL
    "patriots":      "Patriots",         "cowboys":    "cowboys",
    "chiefs":        "KansasCityChiefs", "packers":    "GreenBayPackers",
    "49ers":         "49ers",            "eagles":     "eagles",
    "steelers":      "steelers",         "ravens":     "ravens",
    "bears":         "CHIBears",         "bills":      "buffalobills",
    "dolphins":      "miamidolphins",    "jets":       "nyjets",
    "raiders":       "raiders",          "chargers":   "Chargers",
    "broncos":       "DenverBroncos",    "seahawks":   "Seahawks",
    "rams":          "losangelesrams",   "cardinals":  "AZCardinals",
    "falcons":       "falcons",          "saints":     "Saints",
    "buccaneers":    "buccaneers",       "panthers":   "panthers",
    "lions":         "detroitlions",     "bears":      "CHIBears",
    "vikings":       "minnesotavikings", "giants":     "NYGiants",
    # NHL
    "maple leafs":   "leafs",            "bruins":     "bostonbruins",
    "penguins":      "penguins",         "capitals":   "caps",
    "oilers":        "EdmontonOilers",   "flames":     "calgaryflames",
    "canucks":       "canucks",          "jets":       "winnipegjets",
    "canadiens":     "Habs",             "senators":   "OttawaSenators",
    "lightning":     "TampaBayLightning","panthers":   "FloridaPanthers",
    "hurricanes":    "canes",            "islanders":  "NewYorkIslanders",
    "devils":        "devils",           "flyers":     "hockeyflyers",
    "blackhawks":    "hawks",            "red wings":  "detroitredwings",
    "blues":         "stlouisblues",     "predators":  "predators",
    "wild":          "wildhockey",       "avalanche":  "coloradoavalanche",
    "sharks":        "SanJoseSharks",    "kings":      "losangeleskings",
    "ducks":         "AnaheimDucks",     "coyotes":    "Coyotes",
    "golden knights": "goldenknights",   "kraken":     "SeattleKraken",
}

_TEAM_ABBR = {
    "yankees":"NYY","red sox":"BOS","blue jays":"TOR","rays":"TBR",
    "orioles":"BAL","white sox":"CHW","guardians":"CLE","tigers":"DET",
    "royals":"KCR","twins":"MIN","astros":"HOU","athletics":"OAK",
    "mariners":"SEA","angels":"LAA","rangers":"TEX","braves":"ATL",
    "phillies":"PHI","mets":"NYM","marlins":"MIA","nationals":"WSN",
    "cubs":"CHC","cardinals":"STL","brewers":"MIL","reds":"CIN",
    "pirates":"PIT","dodgers":"LAD","giants":"SFG","padres":"SDP",
    "rockies":"COL","diamondbacks":"ARI",
}

# Sport-specific RSS feed collections
_RSS_FEEDS = {
    "baseball": [
        "https://sports.yahoo.com/mlb/rss.xml",
        "https://www.cbssports.com/rss/headlines/mlb/",
        "https://bleacherreport.com/articles/feed?tag=mlb",
        "https://www.rotoballer.com/feed/?tag=mlb-news",
    ],
    "basketball": [
        "https://sports.yahoo.com/nba/rss.xml",
        "https://www.cbssports.com/rss/headlines/nba/",
        "https://bleacherreport.com/articles/feed?tag=nba",
        "https://www.rotoballer.com/feed/?tag=nba-news",
    ],
    "americanfootball": [
        "https://sports.yahoo.com/nfl/rss.xml",
        "https://www.cbssports.com/rss/headlines/nfl/",
        "https://bleacherreport.com/articles/feed?tag=nfl",
    ],
    "icehockey": [
        "https://sports.yahoo.com/nhl/rss.xml",
        "https://www.cbssports.com/rss/headlines/nhl/",
        "https://bleacherreport.com/articles/feed?tag=nhl",
    ],
    "soccer": [
        "https://sports.yahoo.com/soccer/rss.xml",
        "https://www.cbssports.com/rss/headlines/soccer/",
        "https://bleacherreport.com/articles/feed?tag=soccer",
    ],
    "all": [
        "https://sports.yahoo.com/rss.xml",
        "https://www.cbssports.com/rss/headlines/",
        "https://bleacherreport.com/articles/feed",
    ],
}

_NEWS_STOP = {
    "that","this","they","with","have","from","will","game","baseball",
    "basketball","football","hockey","soccer","team","player","said",
    "their","just","been","would","could","should","when","what",
    "about","more","also","after","into","over","first","season",
    "last","year","week","time","some","were","than","very","most",
}

_DISCORD_SLANG = {
    "raking":       "hitting excellently",
    "dealing":      "pitching excellently",
    "got lit up":   "pitched terribly",
    "cooked":       "performing poorly",
    "dog water":    "terrible",
    "w player":     "great player",
    "l take":       "bad opinion",
    "no cap":       "honestly",
    "glazing":      "over-praising",
    "brickin":      "missing shots badly",
    "cold game":    "bad game",
    "fire game":    "great game",
    "top diff":     "top player made the difference",
}


# --- Text normalization helpers ---
def _normalize_text(text: str) -> str:
    raw = unicodedata.normalize("NFKD", str(text))
    ascii_txt = raw.encode("ascii", "ignore").decode("ascii").lower()
    return f" {re.sub(r'[^a-z0-9]+', ' ', ascii_txt).strip()} "


def _normalize_discord_slang(text: str) -> str:
    t = (text or "").lower()
    for slang, meaning in _DISCORD_SLANG.items():
        t = t.replace(slang, meaning)
    return t


def _team_subreddit(team_name: str) -> str:
    lower = team_name.lower()
    for keyword, sub in _TEAM_SUBREDDITS.items():
        if keyword in lower:
            return sub
    return ""


def _infer_sport_group(sport: str) -> str:
    raw = re.sub(r"[^a-z0-9]+", "_", str(sport or "").lower()).strip("_")
    if any(k in raw for k in ("baseball", "mlb")):
        return "baseball"
    if any(k in raw for k in ("basketball", "nba", "wnba")):
        return "basketball"
    if any(k in raw for k in ("americanfootball", "nfl", "ncaaf")):
        return "americanfootball"
    if any(k in raw for k in ("icehockey", "hockey", "nhl")):
        return "icehockey"
    if any(k in raw for k in ("soccer", "football")):
        return "soccer"
    if any(k in raw for k in ("mma", "ufc", "boxing")):
        return "mma"
    return raw or "all"


# --- Signal classifier ---
_INJURY_WORDS = {
    "injury","injured","out","doubtful","questionable","day-to-day","dtd",
    "il","dl","disabled","strained","strain","sprained","sprain","fracture",
    "fractured","surgery","procedure","rehab","hamstring","oblique","elbow",
    "shoulder","wrist","ankle","concussion","scratch","scratched","limited",
    "shut down","placed on","wont play","not available","miss","missed",
    "back pain","side session","listed","inactive","sit out","sat out",
}
_LINEUP_WORDS = {
    "starting","starter","lineup","confirmed","activated","returns","back",
    "cleared","available","ready","healthy","active","called up","promoted",
    "recalled","roster","rotation","extension","signed","traded","released",
}
_MOMENTUM_WORDS = {
    "streak","hot","cold","slump","dominant","dominates","crushing","beast",
    "clutch","fire","on fire","rolling","strong","form","prime","breakout",
    "career high","record","elite","ace","shutdown","lights out","bounce back",
}


def _classify_signal(score: float, texts: list) -> dict:
    """Return signal_type + injury/lineup/momentum flags from text corpus."""
    combined = " ".join(t.lower() for t in texts)
    inj   = sum(1 for w in _INJURY_WORDS   if w in combined)
    lin   = sum(1 for w in _LINEUP_WORDS   if w in combined)
    mom   = sum(1 for w in _MOMENTUM_WORDS if w in combined)
    if inj >= 2:
        sig_type = "injury_concern"
    elif lin >= 2:
        sig_type = "lineup_change"
    elif abs(score) >= 0.25 and mom >= 2:
        sig_type = "positive_momentum" if score > 0 else "negative_momentum"
    else:
        sig_type = "neutral"
    return {
        "signal_type":   sig_type,
        "injury_flag":   inj >= 1,
        "lineup_flag":   lin >= 1,
        "momentum_flag": mom >= 1,
    }


# --- RSS helper ---
def _parse_rss_items(xml_text: str, max_items: int = 60) -> list:
    """Parse RSS/Atom XML to list of {title, description, url, source_name}."""
    try:
        import xml.etree.ElementTree as ET
        root  = ET.fromstring(xml_text)
        items = []
        for item in root.findall(".//item")[:max_items]:
            title = (item.findtext("title") or "").strip()
            desc  = re.sub(r"<[^>]+>", " ", item.findtext("description") or "").strip()
            items.append({
                "title":       title,
                "description": desc,
                "url":         (item.findtext("link") or "").strip(),
            })
        return items
    except Exception:
        return []


# =========================================================================
# SOURCE 1 — ESPN News (free, no key, official)
# =========================================================================
def _fetch_espn_news_raw(sport: str = "all", limit: int = 50) -> list:
    sg     = _infer_sport_group(sport)
    slugs  = _ESPN_NEWS_SLUGS.get(sg) or []
    if sg == "all" or not slugs:
        slugs = ["baseball/mlb", "basketball/nba", "football/nfl",
                 "hockey/nhl", "soccer/eng.1"]

    ck = f"espn:{sg}"
    cached = _cache_fresh(_ESPN_NEWS_CACHE, ck, _CACHE_TTL_SHORT)
    if cached is not None:
        return cached

    articles   = []
    seen_urls  = set()
    for slug in slugs:
        url = f"{_ESPN_BASE}/{slug}/news"
        try:
            resp = requests.get(url, params={"limit": limit},
                                headers={"User-Agent": "bettor-sentiment/1.0"},
                                timeout=10)
            if resp.status_code != 200:
                continue
            data = resp.json()
            for art in (data.get("articles") or []):
                headline = str(art.get("headline") or art.get("title") or "").strip()
                desc     = str(art.get("description") or art.get("story") or "")[:300].strip()
                art_url  = str((art.get("links") or {}).get("web", {}).get("href") or "").strip()
                if not headline or art_url in seen_urls:
                    continue
                seen_urls.add(art_url)
                articles.append({
                    "title":       headline,
                    "description": desc,
                    "url":         art_url,
                    "source_name": f"ESPN/{slug.split('/')[0].upper()}",
                })
        except Exception:
            continue

    _cache_store(_ESPN_NEWS_CACHE, ck, articles)
    return articles


def get_espn_news_sentiment(entity: str, entity_type: str = "team",
                             sport=None) -> dict:
    """Sentiment from ESPN official news feed (free, no key)."""
    articles = _fetch_espn_news_raw(sport or "all", limit=60)
    if not articles:
        return {}

    tokens  = _entity_tokens(entity, entity_type)
    matched = []
    for a in articles:
        text = f"{a.get('title','')} {a.get('description','')}".strip()
        nt   = _normalize_text(text)
        ntns = nt.replace(" ", "")
        if any(_text_has_token(nt, ntns, t) for t in tokens):
            matched.append(a)

    if not matched:
        return {}

    texts  = [f"{a['title']} {a.get('description','')}" for a in matched]
    scores = score_texts(texts)
    avg    = round(sum(scores) / len(scores), 4) if scores else 0.0
    sigs   = _classify_signal(avg, texts)
    return {
        "score":    avg,
        "volume":   len(matched),
        "keywords": _top_keywords(texts),
        "source":   "espn_news",
        "articles": [{"title": a["title"], "url": a["url"],
                      "source_name": a.get("source_name","ESPN")}
                     for a in matched[:6]],
        **sigs,
    }


# =========================================================================
# SOURCE 2 — Multi-sport RSS (Yahoo / CBS / Bleacher Report / RotoBaller)
# =========================================================================
def _rss_source_name(url: str) -> str:
    if "yahoo"  in url: return "Yahoo Sports"
    if "cbs"    in url: return "CBS Sports"
    if "bleach" in url: return "Bleacher Report"
    if "rotob"  in url: return "RotoBaller"
    return "RSS"


def _fetch_rss_articles_raw(sport: str = "all") -> list:
    sg     = _infer_sport_group(sport)
    feeds  = _RSS_FEEDS.get(sg) or _RSS_FEEDS.get("all") or []
    ck     = f"rss:{sg}"
    cached = _cache_fresh(_RSS_CACHE, ck, _CACHE_TTL_SHORT)
    if cached is not None:
        return cached

    articles  = []
    seen_urls = set()
    headers   = {"User-Agent": "Mozilla/5.0 (compatible; bettor-bot/1.0)"}
    for feed_url in feeds:
        try:
            resp = requests.get(feed_url, headers=headers, timeout=10)
            if resp.status_code != 200:
                continue
            for item in _parse_rss_items(resp.text, max_items=40):
                u = item.get("url", "")
                if not item.get("title") or u in seen_urls:
                    continue
                seen_urls.add(u)
                item["source_name"] = _rss_source_name(feed_url)
                articles.append(item)
        except Exception:
            continue

    _cache_store(_RSS_CACHE, ck, articles)
    return articles


def get_rss_sentiment(entity: str, entity_type: str = "team", sport=None) -> dict:
    """Sentiment from Yahoo/CBS/BR/RotoBaller RSS feeds (all free, no key)."""
    articles = _fetch_rss_articles_raw(sport or "all")
    if not articles:
        return {}

    tokens  = _entity_tokens(entity, entity_type)
    matched = []
    for a in articles:
        text = f"{a.get('title','')} {a.get('description','')}".strip()
        nt   = _normalize_text(text)
        ntns = nt.replace(" ", "")
        if any(_text_has_token(nt, ntns, t) for t in tokens):
            matched.append(a)

    if not matched:
        return {}

    texts  = [f"{a['title']} {a.get('description','')}" for a in matched]
    scores = score_texts(texts)
    avg    = round(sum(scores) / len(scores), 4) if scores else 0.0
    sigs   = _classify_signal(avg, texts)
    return {
        "score":    avg,
        "volume":   len(matched),
        "keywords": _top_keywords(texts),
        "source":   "rss",
        "articles": [{"title": a["title"], "url": a.get("url",""),
                      "source_name": a.get("source_name","RSS")}
                     for a in matched[:6]],
        **sigs,
    }


# =========================================================================
# SOURCE 3 — Reddit public JSON API (no praw, no OAuth required)
# =========================================================================
_REDDIT_JSON_HEADERS = {
    "User-Agent": "bettor-sentiment/1.0 (+https://github.com/bettor)",
    "Accept":     "application/json",
}


def _fetch_reddit_json_posts(subreddits: list, query: str,
                              limit: int = 50, days_back: int = 1) -> list:
    """
    Fetch Reddit posts via public JSON endpoint — no credentials needed.
    Reddit allows ~1 unauthenticated req/sec per IP.
    """
    if not subreddits:
        return []
    texts    = []
    seen     = set()
    t_filter = "day" if days_back <= 1 else ("week" if days_back <= 7 else "month")

    for sub in subreddits[:4]:
        url    = f"https://www.reddit.com/r/{sub}/search.json"
        params = {"q": query, "sort": "new", "t": t_filter,
                  "limit": min(limit, 25), "restrict_sr": "true"}
        try:
            resp = requests.get(url, params=params,
                                headers=_REDDIT_JSON_HEADERS, timeout=12)
            if resp.status_code == 429:
                time.sleep(1.5)
                continue
            if resp.status_code != 200:
                continue
            posts = ((resp.json().get("data") or {}).get("children") or [])
            for p in posts:
                pd    = (p.get("data") or {})
                title = str(pd.get("title") or "").strip()
                body  = str(pd.get("selftext") or "")[:200].strip()
                combined = f"{title} {body}".strip()
                if combined and combined not in seen:
                    seen.add(combined)
                    texts.append(combined)
        except Exception:
            continue
        time.sleep(0.6)

    return texts


def get_reddit_json_sentiment(entity: str, entity_type: str = "team",
                               sport=None) -> dict:
    """Sentiment from Reddit JSON API (no credentials needed)."""
    sg   = _infer_sport_group(sport or "all")
    subs = list(_SPORT_SUBREDDITS.get(sg) or _SPORT_SUBREDDITS.get("all") or [])
    if entity_type == "team":
        ts = _team_subreddit(entity)
        if ts and ts not in subs:
            subs.insert(0, ts)

    ck     = f"reddit_json:{sg}:{_normalize_text(entity).strip()}"
    cached = _cache_fresh(_REDDIT_JSON_CACHE, ck, _CACHE_TTL_SHORT)
    if cached is not None:
        return cached if cached else {}

    texts = _fetch_reddit_json_posts(subs, entity, limit=30)
    if not texts:
        _cache_store(_REDDIT_JSON_CACHE, ck, {})
        return {}

    scores = score_texts(texts)
    avg    = round(sum(scores) / len(scores), 4) if scores else 0.0
    sigs   = _classify_signal(avg, texts)
    result = {"score": avg, "volume": len(texts),
              "keywords": _top_keywords(texts), "source": "reddit", **sigs}
    _cache_store(_REDDIT_JSON_CACHE, ck, result)
    return result


# =========================================================================
# SOURCE 4 — Reddit praw (optional; higher rate limits with credentials)
# =========================================================================
def _get_reddit_instance():
    global _REDDIT_FAILED
    if _REDDIT_FAILED:
        return None
    if not REDDIT_CLIENT_ID or REDDIT_CLIENT_ID.startswith("YOUR_"):
        return None
    try:
        import praw
        return praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT,
            read_only=True,
        )
    except Exception as e:
        print(f"[sentiment] Reddit praw init error: {e}")
        _REDDIT_FAILED = True
        return None


def get_reddit_praw_sentiment(entity: str, entity_type: str = "team",
                               sport=None) -> dict:
    """Sentiment from Reddit via praw (needs REDDIT_CLIENT_ID + SECRET)."""
    global _REDDIT_FAILED
    reddit = _get_reddit_instance()
    if not reddit:
        return {}
    sg   = _infer_sport_group(sport or "all")
    subs = list(_SPORT_SUBREDDITS.get(sg) or ["baseball", "mlb"])
    if entity_type == "team":
        ts = _team_subreddit(entity)
        if ts:
            subs.insert(0, ts)
    try:
        texts = []
        for sub_name in subs[:3]:
            try:
                sub = reddit.subreddit(sub_name)
                for post in sub.search(entity, limit=15, time_filter="day", sort="new"):
                    body = str(post.selftext or "")[:200]
                    texts.append(f"{post.title} {body}".strip())
            except Exception:
                pass
        if not texts:
            return {}
        scores = score_texts(texts)
        avg    = round(sum(scores) / len(scores), 4) if scores else 0.0
        sigs   = _classify_signal(avg, texts)
        return {"score": avg, "volume": len(texts),
                "keywords": _top_keywords(texts), "source": "reddit_praw", **sigs}
    except Exception as e:
        print(f"[sentiment] Reddit praw error for {entity}: {e}")
        _REDDIT_FAILED = True
        return {}


# =========================================================================
# SOURCE 5 — GDELT DOC API (free, no key, global news corpus)
# =========================================================================
_GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"


def get_gdelt_sentiment(entity: str, entity_type: str = "team", sport=None) -> dict:
    """
    Sentiment from GDELT 2.0 DOC API — free, no key, ~1 req/sec.
    Searches last 24 h of global English-language news.
    """
    sg = _infer_sport_group(sport or "all")
    sport_term = {"baseball":"baseball","basketball":"basketball",
                  "americanfootball":"football","icehockey":"hockey",
                  "soccer":"soccer"}.get(sg, "sports")
    q  = f'"{entity}" {sport_term}' if entity_type == "team" else f'"{entity}" sports'
    ck = f"gdelt:{_normalize_text(q).strip()}"
    cached = _cache_fresh(_GDELT_CACHE, ck, _CACHE_TTL_MED)
    if cached is not None:
        return cached if cached else {}

    now   = datetime.datetime.utcnow()
    start = (now - datetime.timedelta(hours=24)).strftime("%Y%m%d%H%M%S")
    end   = now.strftime("%Y%m%d%H%M%S")
    try:
        resp = requests.get(_GDELT_URL, params={
            "query": q, "mode": "artlist", "maxrecords": 50,
            "format": "json", "startdatetime": start, "enddatetime": end,
        }, timeout=15, headers={"User-Agent": "bettor-sentiment/1.0"})
        if resp.status_code != 200:
            _cache_store(_GDELT_CACHE, ck, {})
            return {}
        arts = (resp.json() or {}).get("articles") or []
        if not arts:
            _cache_store(_GDELT_CACHE, ck, {})
            return {}
        texts  = [str(a.get("title","")).strip() for a in arts if a.get("title")]
        scores = score_texts(texts)
        avg    = round(sum(scores) / len(scores), 4) if scores else 0.0
        sigs   = _classify_signal(avg, texts)
        result = {"score": avg, "volume": len(texts),
                  "keywords": _top_keywords(texts), "source": "gdelt", **sigs}
        _cache_store(_GDELT_CACHE, ck, result)
        return result
    except Exception as e:
        print(f"[sentiment] GDELT error for {entity}: {e}")
        _cache_store(_GDELT_CACHE, ck, {})
        return {}


# =========================================================================
# SOURCE 6 — NewsAPI + Google News RSS fallback
# =========================================================================
def _news_fetch_articles(query: str, days_back: int = 7, page_size: int = 100,
                          start_date=None, end_date=None) -> list:
    global _NEWS_FAILED
    if not NEWS_API_KEY:
        return []
    url    = "https://newsapi.org/v2/everything"
    params = {
        "q": query, "language": "en", "searchIn": "title,description",
        "sortBy": "publishedAt", "pageSize": min(int(page_size), 100),
        "apiKey": NEWS_API_KEY,
    }
    if start_date and end_date:
        params["from"] = start_date
        params["to"]   = end_date
    else:
        params["from"] = (datetime.date.today() - datetime.timedelta(days=days_back)).isoformat()
    try:
        resp = requests.get(url, params=params, timeout=12)
        if resp.status_code == 426:
            _NEWS_FAILED = True
            return []
        if resp.status_code in (429, 401):
            return []
        if resp.status_code != 200:
            return []
        return resp.json().get("articles", [])
    except Exception:
        return []


def _google_news_rss(query: str) -> list:
    """Free fallback — Google News RSS; no key required."""
    try:
        from urllib.parse import quote
        url  = f"https://news.google.com/rss/search?q={quote(query)}&hl=en-US&gl=US&ceid=US:en"
        resp = requests.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return []
        return _parse_rss_items(resp.text, max_items=50)
    except Exception:
        return []


def get_news_sentiment(entity: str, entity_type: str = "team", sport=None) -> dict:
    """Sentiment from NewsAPI with Google News RSS fallback."""
    global _NEWS_FAILED
    sg = _infer_sport_group(sport or "all")
    sport_term = {
        "baseball":         "MLB OR baseball",
        "basketball":       "NBA OR basketball",
        "americanfootball": "NFL OR football",
        "icehockey":        "NHL OR hockey",
        "soccer":           "soccer OR football",
    }.get(sg, "sports")

    if entity_type == "team":
        short   = entity.split()[-1]
        queries = [f'"{short}" ({sport_term})',
                   f'"{short}" (injury OR injured OR scratched OR "day to day")']
    else:
        queries = [f'"{entity}" ({sport_term})',
                   f'"{entity}" (injury OR injured OR scratched OR limited)',
                   f'"{entity}" (stats OR performance OR prop OR over OR under)']

    seen_urls    = set()
    all_articles = []
    for q in queries:
        if _NEWS_FAILED:
            break
        raw = _news_fetch_articles(q, days_back=7) if NEWS_API_KEY and not _NEWS_FAILED else []
        if not raw:
            raw = _google_news_rss(q)
        for a in raw:
            u = a.get("url", "")
            if u and u not in seen_urls:
                seen_urls.add(u)
                all_articles.append(a)

    if not all_articles:
        return {}

    texts  = [f"{a.get('title','')}" + (" " + a.get("description","") if a.get("description") else "")
              for a in all_articles]
    scores = score_texts(texts)
    avg    = round(sum(scores) / len(scores), 4) if scores else 0.0
    sigs   = _classify_signal(avg, texts)

    try:
        from data.db import save_news_articles
        article_rows = [{
            "sport":       sg,
            "team":        entity,
            "headline":    (a.get("title") or "")[:500],
            "description": (a.get("description") or "")[:1000],
            "url":         (a.get("url") or "")[:500],
            "source_name": ((a.get("source") or {}).get("name") or a.get("source_name",""))[:100],
            "sentiment":   round(float(s), 3),
            "published_at": a.get("publishedAt"),
        } for a, s in zip(all_articles, scores)]
        save_news_articles(article_rows)
    except Exception:
        pass

    return {
        "score":    avg,
        "volume":   len(all_articles),
        "keywords": _top_keywords(texts),
        "source":   "news",
        "articles": [{"title": a.get("title",""), "url": a.get("url",""),
                      "source_name": (a.get("source") or {}).get("name","NewsAPI")}
                     for a in all_articles[:6]],
        **sigs,
    }


# =========================================================================
# SOURCE 7 — newsdata.io (free tier 200 req/day, no credit card)
# =========================================================================
def get_newsdata_sentiment(entity: str, entity_type: str = "team", sport=None) -> dict:
    """
    Sentiment from newsdata.io.
    Set NEWSDATA_API_KEY env var to enable (free tier: 200 req/day).
    """
    global _NEWSDATA_FAILED
    if _NEWSDATA_FAILED or not NEWSDATA_API_KEY:
        return {}

    sg = _infer_sport_group(sport or "all")
    ck = f"newsdata:{sg}:{_normalize_text(entity).strip()}"
    cached = _cache_fresh(_NEWSDATA_CACHE, ck, _CACHE_TTL_MED)
    if cached is not None:
        return cached if cached else {}

    try:
        resp = requests.get("https://newsdata.io/api/1/news", params={
            "apikey":   NEWSDATA_API_KEY,
            "q":        entity,
            "language": "en",
            "category": "sports",
            "size":     50,
        }, timeout=12)
        if resp.status_code in (401, 403):
            _NEWSDATA_FAILED = True
            _cache_store(_NEWSDATA_CACHE, ck, {})
            return {}
        if resp.status_code in (429, 422):
            _cache_store(_NEWSDATA_CACHE, ck, {})
            return {}
        if resp.status_code != 200:
            _cache_store(_NEWSDATA_CACHE, ck, {})
            return {}
        results = (resp.json() or {}).get("results") or []
        if not results:
            _cache_store(_NEWSDATA_CACHE, ck, {})
            return {}
        texts  = [f"{a.get('title','')} {a.get('description') or ''}".strip() for a in results]
        scores = score_texts(texts)
        avg    = round(sum(scores) / len(scores), 4) if scores else 0.0
        sigs   = _classify_signal(avg, texts)
        result = {"score": avg, "volume": len(texts),
                  "keywords": _top_keywords(texts), "source": "newsdata", **sigs}
        _cache_store(_NEWSDATA_CACHE, ck, result)
        return result
    except Exception as e:
        print(f"[sentiment] newsdata.io error for {entity}: {e}")
        _cache_store(_NEWSDATA_CACHE, ck, {})
        return {}


# =========================================================================
# SOURCE 8 — Discord (requires DISCORD_BOT_TOKEN)
# =========================================================================
def _parse_discord_channels(raw: str) -> list:
    if not raw:
        return []
    parsed = []
    for t in re.split(r"[\s,|]+", raw.strip()):
        if not t:
            continue
        ids = re.findall(r"\d{5,}", t)
        if len(ids) >= 2:
            guild_id, channel_id = ids[-2], ids[-1]
        elif len(ids) == 1:
            guild_id, channel_id = "", ids[0]
        else:
            continue
        parsed.append({"guild_id": guild_id, "channel_id": channel_id})
    return parsed


def _parse_discord_channel_map(raw: str) -> dict:
    out = {}
    for block in re.split(r"[;\n]+", str(raw or "").strip()):
        part = block.strip()
        if not part:
            continue
        if "=" in part:
            sport_key, values = part.split("=", 1)
        elif ":" in part:
            sport_key, values = part.split(":", 1)
        else:
            continue
        key = str(sport_key or "").strip().lower()
        if not key or not re.match(r"^[a-z_]+$", key):
            continue
        channels = _parse_discord_channels(values)
        if channels:
            out[key] = channels
    return out


def _resolve_discord_channels_for_sport(sport=None) -> list:
    channels = _parse_discord_channels(DISCORD_CHANNELS)
    by_sport = _parse_discord_channel_map(DISCORD_CHANNELS_BY_SPORT)
    sg       = _infer_sport_group(str(sport or "all"))
    channels.extend(by_sport.get("all", []))
    if sg and sg != "all":
        for k in [sg, sport or ""]:
            channels.extend(by_sport.get(str(k).lower(), []))
    deduped = []
    seen    = set()
    for ch in channels:
        cid = str(ch.get("channel_id") or "")
        if cid and cid not in seen:
            seen.add(cid)
            deduped.append(ch)
    return deduped


def _discord_headers() -> dict:
    return {"Authorization": f"Bot {DISCORD_BOT_TOKEN}"}


def _is_image_attachment(att: dict) -> bool:
    ct = str(att.get("content_type") or "").lower()
    if ct.startswith("image/"):
        return True
    return str(att.get("filename") or "").lower().endswith(
        (".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".tiff"))


def _ocr_text_from_image_url(url: str) -> str:
    if not DISCORD_ENABLE_ATTACHMENT_OCR or not HF_API_KEY:
        return ""
    cached = _DISCORD_IMAGE_OCR_CACHE.get(url)
    if cached is not None:
        return cached
    try:
        img_bytes = requests.get(url, timeout=max(3, int(DISCORD_OCR_TIMEOUT_SECONDS))).content or b""
    except Exception:
        _DISCORD_IMAGE_OCR_CACHE[url] = ""
        return ""
    headers = {"Authorization": f"Bearer {HF_API_KEY}", "Content-Type": "application/octet-stream"}
    for base in _HF_API_BASES:
        for model in _HF_OCR_MODELS:
            try:
                r = requests.post(f"{base}/{model}", headers=headers, data=img_bytes,
                                  timeout=max(3, int(DISCORD_OCR_TIMEOUT_SECONDS)))
                if r.status_code != 200:
                    continue
                payload = r.json() if r.text else []
                text = ""
                if isinstance(payload, list) and payload:
                    first = payload[0]
                    text  = str((first or {}).get("generated_text") or
                                (first or {}).get("caption") or first or "").strip()
                elif isinstance(payload, dict):
                    text = str(payload.get("generated_text") or payload.get("caption") or "").strip()
                if text:
                    _DISCORD_IMAGE_OCR_CACHE[url] = text
                    return text
            except Exception:
                continue
    _DISCORD_IMAGE_OCR_CACHE[url] = ""
    return ""


def _fetch_discord_messages(sport=None) -> list:
    global _DISCORD_FAILED, _DISCORD_AUTH_LOGGED
    if _DISCORD_FAILED or not DISCORD_BOT_TOKEN:
        return []
    channels = _resolve_discord_channels_for_sport(sport)
    if not channels:
        return []

    sg  = _infer_sport_group(str(sport or "all"))
    ck  = f"discord:{sg}:{'|'.join(sorted(c.get('channel_id','') for c in channels))}"
    now = datetime.datetime.now(datetime.timezone.utc)
    cached_row = _DISCORD_CACHE.get(ck, {})
    cached_at  = cached_row.get("fetched_at")
    if cached_at and (now - cached_at).total_seconds() / 60 <= float(DISCORD_CACHE_MINUTES):
        return list(cached_row.get("messages", []))

    cutoff        = now - datetime.timedelta(hours=float(DISCORD_LOOKBACK_HOURS))
    max_messages  = max(1, int(DISCORD_MAX_MESSAGES))
    remaining_ocr = max(0, int(DISCORD_MAX_IMAGE_ATTACHMENTS))
    all_msgs      = []

    for ch in channels:
        channel_id = ch.get("channel_id")
        if not channel_id:
            continue
        url    = f"https://discord.com/api/v10/channels/{channel_id}/messages"
        before = None
        fetched = 0
        while fetched < max_messages:
            params = {"limit": min(100, max_messages - fetched)}
            if before:
                params["before"] = before
            try:
                resp = requests.get(url, headers=_discord_headers(), params=params, timeout=15)
                if resp.status_code == 401:
                    if not _DISCORD_AUTH_LOGGED:
                        print("[sentiment] Discord: invalid bot token")
                        _DISCORD_AUTH_LOGGED = True
                    _DISCORD_FAILED = True
                    return []
                if resp.status_code in (403, 429):
                    break
                if resp.status_code != 200:
                    break
                batch = resp.json() or []
            except Exception:
                break
            if not batch:
                break
            stop_early = False
            for msg in batch:
                ts = msg.get("timestamp")
                try:
                    if ts and datetime.datetime.fromisoformat(ts.replace("Z","+00:00")) < cutoff:
                        stop_early = True
                        break
                except Exception:
                    pass
                if (msg.get("author") or {}).get("bot"):
                    continue
                chunks = [str(msg.get("content") or "").strip()]
                for att in (msg.get("attachments") or []):
                    desc = str(att.get("description") or "").strip()
                    if desc:
                        chunks.append(desc)
                    if remaining_ocr > 0 and _is_image_attachment(att):
                        ocr = _ocr_text_from_image_url(att.get("url") or att.get("proxy_url") or "")
                        if ocr:
                            chunks.append(ocr)
                        remaining_ocr -= 1
                combined = " ".join(x for x in chunks if x).strip()
                if not combined:
                    continue
                all_msgs.append({"content": combined, "timestamp": ts, "channel_id": channel_id})
                fetched += 1
                if fetched >= max_messages:
                    break
            before = batch[-1].get("id")
            if stop_early or not before:
                break

    _DISCORD_CACHE[ck] = {"fetched_at": now, "messages": list(all_msgs)}
    return all_msgs


def _discord_texts_for_entity(entity: str, entity_type: str, sport=None) -> list:
    msgs   = _fetch_discord_messages(sport=sport)
    tokens = _entity_tokens(entity, entity_type)
    if not msgs or not tokens:
        return []
    hits = []
    seen = set()
    for m in msgs:
        text = m.get("content", "")
        if not text:
            continue
        nt   = _normalize_text(text)
        ntns = nt.replace(" ", "")
        if any(_text_has_token(nt, ntns, t) for t in tokens) and text not in seen:
            hits.append(text)
            seen.add(text)
    return hits


def get_discord_sentiment(entity: str, entity_type: str = "team", sport=None) -> dict:
    if not DISCORD_BOT_TOKEN:
        return {}
    texts = [_normalize_discord_slang(t) for t in
             _discord_texts_for_entity(entity, entity_type, sport=sport) if t.strip()]
    if not texts:
        return {}
    scores = score_texts(texts)
    avg    = round(sum(scores) / len(scores), 4) if scores else 0.0
    sigs   = _classify_signal(avg, texts)
    return {"score": avg, "volume": len(texts),
            "keywords": _top_keywords(texts), "source": "discord", **sigs}


# =========================================================================
# HuggingFace scoring + multi-sport keyword fallback
# =========================================================================
def _hf_parse_scores(results: list, model: str) -> list:
    scores = []
    for r in results:
        if isinstance(r, list):
            pos = next((x["score"] for x in r
                        if x["label"] in {"POSITIVE","positive","POS","pos","LABEL_2","label_2"}), None)
            neg = next((x["score"] for x in r
                        if x["label"] in {"NEGATIVE","negative","NEG","neg","LABEL_0","label_0"}), None)
            scores.append((pos * 2 - 1) if pos is not None else
                          (-(neg * 2 - 1)) if neg is not None else 0.0)
        elif isinstance(r, dict) and "label" in r:
            lbl = r["label"].upper()
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


def score_texts(texts: list, batch_size: int = 16) -> list:
    """Score texts in [-1, +1].  HuggingFace Inference API -> keyword fallback."""
    global _HF_FAILED
    if not texts:
        return []
    if HF_API_KEY and not _HF_FAILED:
        headers = {"Authorization": f"Bearer {HF_API_KEY}"}
        for base in _HF_API_BASES:
            for model in _HF_MODELS:
                url = f"{base}/{model}"
                try:
                    all_scores = []
                    failed     = False
                    for i in range(0, len(texts), batch_size):
                        batch   = [t[:512] for t in texts[i:i + batch_size]]
                        payload = {"inputs": batch, "options": {"wait_for_model": True}}
                        resp    = requests.post(url, headers=headers, json=payload, timeout=30)
                        if resp.status_code == 200:
                            raw = resp.json()
                            if (isinstance(raw, list) and len(raw) == 1
                                    and isinstance(raw[0], list)
                                    and len(raw[0]) == len(batch)
                                    and isinstance(raw[0][0], dict)
                                    and "label" in raw[0][0]):
                                raw = [[x] for x in raw[0]]
                            all_scores.extend(_hf_parse_scores(raw, model))
                        elif resp.status_code == 503:
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
    """
    Multi-sport lexicon-based sentiment scorer.
    Covers MLB, NBA, NFL, NHL, Soccer, MMA.
    Returns a score in [-1.0, +1.0].
    """
    _POS = {
        # Universal positives
        "win","wins","won","victory","champion","championship","dominant",
        "dominates","outstanding","excellent","great","amazing","elite",
        "clutch","efficient","strong","solid","healthy","activated","cleared",
        "returns","comeback","streak","form","prime","breakout","record",
        "career high","extension","signed",
        # MLB
        "homer","home run","blast","dinger","grand slam","no-hitter",
        "no hitter","strikeout","strikeouts","shutout","complete game",
        "walk-off","walkoff","lights out","ace","raking","dealing",
        # NBA
        "triple double","double double","buzzer beater","dunk","block",
        "steal","mvp","all-star","all star","three pointer","three-pointer",
        # NFL
        "touchdown","interception","sack","field goal",
        "rushing yard","receiving yard","quarterback","pro bowl",
        # NHL
        "hat trick","save","power play","penalty kill","clean game",
        # Soccer
        "clean sheet","assist","penalty saved","brace","man of the match",
        # MMA
        "knockout","ko","submission","finish","champion",
        "title defence","performance bonus",
    }
    _NEG = {
        # Universal injury/availability negatives
        "injury","injured","out","doubtful","questionable","inactive",
        "disabled","scratched","day-to-day","dtd","il","dl",
        "strained","strain","sprained","sprain","fracture","fractured",
        "surgery","procedure","rehab","hamstring","oblique","elbow",
        "shoulder","wrist","ankle","concussion","limited","shut down",
        "missed","miss","not available","sit out","sat out",
        # Performance negatives
        "loss","losses","lost","slump","cold","terrible","awful",
        "struggling","struggle","collapse","blown","blew",
        "benched","ejected","released","cut",
        # MLB
        "got lit up","rocked","shelled","blown save","early exit",
        "knocked out","ineffective","rough outing",
        # NBA
        "cold shooting","brick","turnover","foul trouble","fouled out",
        # NFL
        "fumble","penalty","flag","turnovers","poor decision",
        # NHL
        "goal against","missed shot","pulled","icing","giveaway",
        # Soccer
        "red card","yellow card","penalty conceded","own goal",
        "missed penalty","off target","poor form","suspended",
        # MMA
        "tko loss","submission loss","decision loss","injury stoppage",
        "dominated","outclassed",
    }
    lower = text.lower()
    pos   = sum(1 for w in _POS if w in lower)
    neg   = sum(1 for w in _NEG if w in lower)
    total = pos + neg
    if total == 0:
        return 0.0
    return round((pos - neg) / total, 3)


# =========================================================================
# Entity token helpers
# =========================================================================
def _entity_tokens(entity: str, entity_type: str) -> list:
    base  = (entity or "").strip()
    if not base:
        return []
    norm  = _normalize_text(base).strip()
    tokens = {norm, norm.replace(" ", "")}
    parts  = [p for p in norm.split() if p]
    tokens.update(parts)
    if entity_type == "team" and parts:
        last = parts[-1]
        tokens.add(last)
        abbr = _TEAM_ABBR.get(last)
        if abbr:
            tokens.add(abbr.lower())
    if entity_type == "player" and parts:
        suffixes = {"jr","sr","ii","iii","iv","v"}
        last = parts[-1] if parts[-1] not in suffixes else (parts[-2] if len(parts) >= 2 else parts[-1])
        tokens.add(last)
        tokens.add(parts[0])
    return sorted(tokens, key=len, reverse=True)


def _text_has_token(norm_text: str, norm_nospace: str, token: str) -> bool:
    t = token.strip().lower()
    if not t:
        return False
    if " " in t or len(t) <= 3:
        return f" {t} " in norm_text
    return (t in norm_text) or (t in norm_nospace)


def _team_alias_tokens(team_name: str) -> list:
    base   = _normalize_text(team_name).strip()
    parts  = [p for p in base.split() if p]
    stop   = {"club","city","fc","cf","sc","the","de","and"}
    tokens = {base, base.replace(" ", "")}
    for p in parts:
        if len(p) >= 3 and p not in stop:
            tokens.add(p)
    if parts:
        last = parts[-1]
        if last not in stop:
            tokens.add(last)
        abbr = _TEAM_ABBR.get(last)
        if abbr:
            tokens.add(abbr.lower())
    return sorted(tokens, key=len, reverse=True)


def _text_mentions_team(text: str, aliases: list) -> bool:
    nt   = _normalize_text(text)
    ntns = nt.replace(" ", "")
    return any(_text_has_token(nt, ntns, t) for t in aliases)


def _infer_team_from_text(text: str, home_team: str, away_team: str) -> str:
    home_hit = _text_mentions_team(text, _team_alias_tokens(home_team))
    away_hit = _text_mentions_team(text, _team_alias_tokens(away_team))
    if home_hit and not away_hit:
        return home_team
    if away_hit and not home_hit:
        return away_team
    return ""


def _top_keywords(texts: list, n: int = 10) -> str:
    words = re.findall(r"\b[a-z]{4,}\b", " ".join(texts).lower())
    freq  = Counter(w for w in words if w not in _NEWS_STOP)
    return ", ".join(w for w, _ in freq.most_common(n))


# =========================================================================
# Source weight combination
# =========================================================================
def _source_weight(name: str, data: dict) -> float:
    if not data or data.get("score") is None:
        return 0.0
    vol = int(data.get("volume", 0) or 0)
    if vol <= 0:
        return 0.0
    cap = _SENTIMENT_WEIGHT_CAPS.get(name, 4.0)
    return min(cap, math.sqrt(vol))


def _combine_sources(*named_sources) -> tuple:
    """
    Weighted-average combination of (name, data) pairs.
    Returns (combined_score, total_volume, active_source_names).
    """
    scores  = []
    weights = []
    total_vol = 0
    active    = []
    for name, data in named_sources:
        w = _source_weight(name, data)
        if w <= 0:
            continue
        scores.append(float(data.get("score", 0.0)))
        weights.append(w)
        total_vol += int(data.get("volume", 0) or 0)
        active.append(name)
    if not scores:
        return 0.0, total_vol, active
    total_w  = sum(weights)
    combined = round(sum(s * w for s, w in zip(scores, weights)) / total_w, 4)
    return combined, total_vol, active


# =========================================================================
# Player name extraction (for social feeds)
# =========================================================================
def _extract_player_name_candidates(text: str) -> list:
    raw = str(text or "")
    block_words = {
        "Major League","Premier League","World Cup","Champions League",
        "New York","Los Angeles","Golden State","Manchester United",
        "First Half","Second Half","Full Time","Game Total",
    }
    lowered_block = {
        "sportsbook","odds","moneyline","parlay","spread","under","over",
        "today","tomorrow","daily","betting","market","props","team",
    }
    out  = []
    seen = set()
    for m in re.finditer(r"\b[A-Z][a-z]{1,}(?:\s+[A-Z][a-z]{1,}){1,2}\b", raw):
        cand  = m.group(0).strip()
        parts = cand.split()
        if (not cand or cand in block_words or len(parts) < 2
                or any(ch.isdigit() for ch in cand)
                or any(p.lower() in lowered_block for p in parts)):
            continue
        key = cand.lower()
        if key not in seen:
            seen.add(key)
            out.append(cand)
    for tag in re.findall(r"#([A-Za-z][A-Za-z0-9]{5,})", raw):
        split = re.sub(r"([a-z])([A-Z])", r"\1 \2", tag).strip()
        if " " in split and not any(ch.isdigit() for ch in split):
            if split.lower() not in seen:
                seen.add(split.lower())
                out.append(split)
    return out


def _is_team_like_name(name: str, home_team: str, away_team: str) -> bool:
    n = _normalize_text(name).strip()
    if not n:
        return True
    for team in (home_team, away_team):
        t = _normalize_text(team).strip()
        if not t:
            continue
        if n == t or n in t or t in n:
            return True
        if len(set(n.split()) & set(t.split())) >= min(len(n.split()), 2):
            return True
    return False


# =========================================================================
# Math helpers
# =========================================================================
def _american_from_prob(prob: float) -> int:
    p = max(0.01, min(0.99, float(prob or 0.5)))
    if p >= 0.5:
        return int(round(-p / (1.0 - p) * 100))
    return int(round((1.0 - p) / p * 100))


def _implied_prob_from_american(odds) -> float:
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    return 100.0 / (o + 100.0) if o > 0 else abs(o) / (abs(o) + 100.0)


def _blend_prob_with_odds(sentiment_prob: float, odds_hint) -> float:
    implied = _implied_prob_from_american(odds_hint)
    if implied is None:
        return sentiment_prob
    return max(0.05, min(0.95, sentiment_prob * 0.72 + implied * 0.28))


def _safety_label_from_prob(prob: float) -> str:
    p = float(prob or 0.5)
    if p >= 0.72:  return "ELITE"
    if p >= 0.60:  return "SAFE"
    if p >= 0.50:  return "MODERATE"
    return "RISKY"


def _sport_for_stats(sport: str) -> str:
    sk = str(sport or "").strip().lower()
    if sk in {"baseball","mlb","baseball_mlb"}:    return "mlb"
    if sk in {"soccer","football"}:                 return "soccer"
    if sk in {"basketball","nba"}:                  return "basketball"
    if sk in {"americanfootball","nfl"}:             return "americanfootball"
    if sk in {"icehockey","nhl","hockey"}:          return "icehockey"
    return sk or "mlb"


def _to_float(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(str(value).strip().replace(",", ""))
    except Exception:
        return None


def _first_numeric(stats: dict, keys: list):
    if not isinstance(stats, dict):
        return None
    lowered = {str(k).lower(): v for k, v in stats.items()}
    for key in keys:
        val = _to_float(lowered.get(key))
        if val is not None:
            return val
    return None


def _poisson_over_prob(rate: float, line: float) -> float:
    lam    = max(0.01, float(rate or 0.01))
    target = int(math.floor(float(line or 0.5)) + 1)
    if target <= 1:
        return max(0.05, min(0.95, 1.0 - math.exp(-lam)))
    cdf = 0.0
    for k in range(target):
        try:
            cdf += math.exp(-lam) * (lam ** k) / math.factorial(k)
        except Exception:
            pass
    return max(0.05, min(0.95, 1.0 - cdf))


def _metric_from_stats_json(stats: dict, sport: str):
    if not isinstance(stats, dict):
        return None
    sk    = _sport_for_stats(sport)
    games = _first_numeric(stats, ["games","g","gp","appearances","matches","played"])
    if not games or games <= 0:
        return None
    if sk == "mlb":
        for stat_key, label in [("hits","Hits"),("home_runs","Home Runs"),
                                  ("rbi","RBI"),("runs","Runs")]:
            v = _first_numeric(stats, [stat_key, stat_key.split("_")[0]])
            if v is not None:
                return (stat_key, label, v / games, 0.5)
    if sk == "soccer":
        for stat_key, label in [("goals","Goals"),("assists","Assists"),
                                  ("shots_on_target","Shots on Target")]:
            v = _first_numeric(stats, [stat_key])
            if v is not None:
                return (stat_key, label, v / games, 0.5)
    if sk == "basketball":
        for stat_key, label in [("points","Points"),("assists","Assists"),
                                  ("rebounds","Rebounds")]:
            v = _first_numeric(stats, [stat_key, stat_key[:3]])
            if v is not None:
                avg = v / games
                return (stat_key, label, avg, max(8.5, round(avg * 0.85, 1)))
    if sk == "americanfootball":
        v = _first_numeric(stats, ["touchdowns","td","passing_tds","rushing_tds","receiving_tds"])
        if v is not None:
            return ("touchdowns", "Touchdowns", v / games, 0.5)
    return None


def _historical_team_player_candidates(team_name: str, sport: str,
                                        max_rows: int = 8) -> list:
    max_rows     = max(1, min(int(max_rows or 8), 50))
    stats_sport  = _sport_for_stats(sport)
    ck           = f"{stats_sport}|{_normalize_text(team_name).strip()}|{max_rows}"
    now          = datetime.datetime.now(datetime.timezone.utc)
    cached_row   = _HIST_PLAYER_CACHE.get(ck, {})
    cached_at    = cached_row.get("fetched_at")
    if cached_at and isinstance(cached_at, datetime.datetime):
        if (now - cached_at).total_seconds() / 60 <= 90:
            return list(cached_row.get("rows", []))
    try:
        from data.db import get_player_season_stats
    except Exception:
        return []
    rows = get_player_season_stats(stats_sport) or []
    if not rows:
        _HIST_PLAYER_CACHE[ck] = {"fetched_at": now, "rows": []}
        return []

    aliases     = _team_alias_tokens(team_name)
    by_player   = {}
    current_year = datetime.date.today().year

    for row in rows:
        rteam = str(row.get("team") or "").strip()
        if rteam and not (_text_mentions_team(rteam, aliases) or
                          _text_mentions_team(team_name, _team_alias_tokens(rteam))):
            continue
        pname  = str(row.get("player_name") or "").strip()
        if not pname:
            continue
        metric = _metric_from_stats_json(row.get("stats_json") or {}, sport)
        if not metric:
            continue
        stat_type, prop_label, rate, line = metric
        if rate <= 0:
            continue
        try:
            season = int(row.get("season") or 0)
        except Exception:
            season = 0
        age    = max(0, current_year - season) if season else 3
        if age > 8:
            continue
        weight = 1.0 / (1.0 + 0.45 * age)
        pentry = by_player.setdefault(pname, {"player": pname, "team": rteam or team_name, "metrics": {}})
        mkey   = f"{stat_type}|{line}|{prop_label}"
        mentry = pentry["metrics"].setdefault(mkey, {
            "stat_type": stat_type, "prop_label": prop_label,
            "line": float(line), "weighted_rate": 0.0,
            "weight_total": 0.0, "seasons": set(), "last_season": 0,
        })
        mentry["weighted_rate"] += float(rate) * weight
        mentry["weight_total"]  += weight
        if season:
            mentry["seasons"].add(season)
            if season > mentry["last_season"]:
                mentry["last_season"] = season

    candidates = []
    for player_name, pdata in by_player.items():
        best_metric = None
        best_rate   = 0.0
        for m in pdata["metrics"].values():
            tw = float(m.get("weight_total") or 0.0)
            if tw <= 0:
                continue
            avg_rate = float(m.get("weighted_rate") or 0.0) / tw
            if avg_rate > best_rate:
                best_rate   = avg_rate
                best_metric = m
        if not best_metric or best_rate <= 0:
            continue
        line      = float(best_metric.get("line") or 0.5)
        hist_prob = _poisson_over_prob(best_rate, line)
        candidates.append({
            "name":          player_name,
            "team":          pdata.get("team") or team_name,
            "stat_type":     best_metric["stat_type"],
            "prop_label":    f"Historical {best_metric['prop_label']}",
            "line":          line,
            "hist_rate":     round(best_rate, 4),
            "hist_prob":     round(hist_prob, 4),
            "seasons_count": len(best_metric.get("seasons") or []),
            "last_season":   int(best_metric.get("last_season") or 0),
        })

    candidates.sort(key=lambda x: (float(x.get("hist_prob") or 0.0),
                                   int(x.get("seasons_count") or 0),
                                   int(x.get("last_season") or 0)), reverse=True)
    candidates = candidates[:max_rows]
    _HIST_PLAYER_CACHE[ck] = {"fetched_at": now, "rows": candidates}
    return candidates


# =========================================================================
# Public combined sentiment APIs
# =========================================================================
def _get_all_sources(entity: str, entity_type: str, sport=None) -> dict:
    """Run all available sources for entity. Free sources run first."""
    sg = _infer_sport_group(str(sport or "all"))
    return {
        "espn_news": get_espn_news_sentiment(entity, entity_type, sport=sg),
        "rss":       get_rss_sentiment(entity, entity_type, sport=sg),
        "reddit":    get_reddit_json_sentiment(entity, entity_type, sport=sg)
                     or get_reddit_praw_sentiment(entity, entity_type, sport=sg),
        "gdelt":     get_gdelt_sentiment(entity, entity_type, sport=sg),
        "news":      get_news_sentiment(entity, entity_type, sport=sg),
        "newsdata":  get_newsdata_sentiment(entity, entity_type, sport=sg),
        "discord":   get_discord_sentiment(entity, entity_type, sport=sg),
    }


def get_team_sentiment(team_name: str, sport=None) -> dict:
    """Aggregate sentiment for a team from all active sources."""
    cached = _cached_sentiment_payload(team_name, "team")
    if cached:
        return cached

    sources  = _get_all_sources(team_name, "team", sport)
    combined, total_vol, active = _combine_sources(*sources.items())

    all_signal_types = [s.get("signal_type","neutral") for s in sources.values() if s]
    dominant_signal  = "neutral"
    for sig in ("injury_concern","lineup_change","positive_momentum","negative_momentum"):
        if all_signal_types.count(sig) >= 1:
            dominant_signal = sig
            break

    injury_flag   = any(s.get("injury_flag",  False) for s in sources.values() if s)
    momentum_flag = any(s.get("momentum_flag", False) for s in sources.values() if s)
    lineup_flag   = any(s.get("lineup_flag",   False) for s in sources.values() if s)
    all_keywords  = ", ".join(filter(None, [s.get("keywords","") for s in sources.values() if s]))

    try:
        from data.db import save_sentiment
        for src_name, src_data in sources.items():
            if src_data:
                save_sentiment(team_name, "team", src_name,
                               src_data.get("score", 0.0),
                               src_data.get("volume", 0),
                               src_data.get("keywords", ""))
        if total_vol > 0:
            save_sentiment(team_name, "team", "combined", combined, total_vol, "")
    except Exception as e:
        print(f"[sentiment] DB save error for {team_name}: {e}")

    return {
        "team":           team_name,
        "combined":       combined,
        "volume":         total_vol,
        "active_sources": active,
        "signal_type":    dominant_signal,
        "injury_flag":    injury_flag,
        "momentum_flag":  momentum_flag,
        "lineup_flag":    lineup_flag,
        "top_keywords":   all_keywords[:200],
        "espn_news":      sources.get("espn_news") or {},
        "rss":            sources.get("rss") or {},
        "reddit":         sources.get("reddit") or {},
        "gdelt":          sources.get("gdelt") or {},
        "news":           sources.get("news") or {},
        "newsdata":       sources.get("newsdata") or {},
        "discord":        sources.get("discord") or {},
    }


def get_player_sentiment(player_name: str, sport=None) -> dict:
    """Aggregate sentiment for a player from all active sources."""
    cached = _cached_sentiment_payload(player_name, "player")
    if cached:
        return cached

    sources  = _get_all_sources(player_name, "player", sport)
    combined, total_vol, active = _combine_sources(*sources.items())

    injury_flag   = any(s.get("injury_flag",  False) for s in sources.values() if s)
    momentum_flag = any(s.get("momentum_flag", False) for s in sources.values() if s)
    lineup_flag   = any(s.get("lineup_flag",   False) for s in sources.values() if s)
    all_signal_types = [s.get("signal_type","neutral") for s in sources.values() if s]
    dominant_signal  = "neutral"
    for sig in ("injury_concern","lineup_change","positive_momentum","negative_momentum"):
        if all_signal_types.count(sig) >= 1:
            dominant_signal = sig
            break

    try:
        from data.db import save_sentiment
        for src_name, src_data in sources.items():
            if src_data:
                save_sentiment(player_name, "player", src_name,
                               src_data.get("score", 0.0),
                               src_data.get("volume", 0),
                               src_data.get("keywords", ""))
        if total_vol > 0:
            save_sentiment(player_name, "player", "combined", combined, total_vol, "")
    except Exception:
        pass

    return {
        "player":         player_name,
        "combined":       combined,
        "volume":         total_vol,
        "active_sources": active,
        "signal_type":    dominant_signal,
        "injury_flag":    injury_flag,
        "momentum_flag":  momentum_flag,
        "lineup_flag":    lineup_flag,
        **{k: v for k, v in sources.items()},
    }


def _cached_sentiment_payload(entity: str, entity_kind: str):
    try:
        from data.db import get_sentiment as db_get_sentiment
        cached = db_get_sentiment(entity, hours=18)
    except Exception:
        return None
    if not cached or "combined" not in cached:
        return None
    combined_score = float((cached.get("combined") or {}).get("score", 0.0) or 0.0)
    total_vol = sum(int((cached.get(src) or {}).get("volume", 0) or 0)
                   for src in ("espn_news","rss","reddit","gdelt","news","newsdata","discord"))
    return {
        entity_kind:      entity,
        "combined":       combined_score,
        "volume":         total_vol,
        "active_sources": [s for s in ("espn_news","rss","reddit","gdelt","news","newsdata","discord")
                           if cached.get(s)],
        **{s: cached.get(s) or {} for s in
           ("espn_news","rss","reddit","gdelt","news","newsdata","discord")},
    }


def get_game_sentiments(home_team: str, away_team: str, sport=None) -> dict:
    """Get sentiment for both teams in a game."""
    try:
        from data.db import get_sentiment as db_sentiment
        home_c = db_sentiment(home_team, hours=12)
        away_c = db_sentiment(away_team, hours=12)
        if home_c.get("combined") and away_c.get("combined"):
            return {
                "home": {"combined": home_c["combined"]["score"], "signal_type": "neutral",
                         "top_keywords": (home_c.get("news") or {}).get("keywords","")},
                "away": {"combined": away_c["combined"]["score"], "signal_type": "neutral",
                         "top_keywords": (away_c.get("news") or {}).get("keywords","")},
            }
    except Exception:
        pass

    home_sent = get_team_sentiment(home_team, sport=sport)
    away_sent = get_team_sentiment(away_team, sport=sport)

    def _team_dict(s: dict) -> dict:
        return {
            "combined":       s.get("combined", 0.0),
            "volume":         s.get("volume", 0),
            "active_sources": s.get("active_sources", []),
            "signal_type":    s.get("signal_type","neutral"),
            "injury_flag":    s.get("injury_flag", False),
            "momentum_flag":  s.get("momentum_flag", False),
            "lineup_flag":    s.get("lineup_flag", False),
            "top_keywords":   s.get("top_keywords",""),
            **{f"{src}_score": s.get(src,{}).get("score",0)
               for src in ("espn_news","rss","reddit","news","gdelt")},
        }

    return {"home": _team_dict(home_sent), "away": _team_dict(away_sent)}


# =========================================================================
# Historical news backfill
# =========================================================================
def fetch_news_history(entity: str, start_date: str, end_date: str,
                       entity_type: str = "team") -> list:
    """Fetch historical news for a date range (backfill use)."""
    global _NEWS_FAILED
    if entity_type == "team":
        short   = entity.split()[-1]
        queries = [f'"{short}" (MLB OR baseball)',
                   f'"{short}" (injury OR injured OR "day to day")']
    else:
        queries = [f'"{entity}" (MLB OR baseball)',
                   f'"{entity}" (injury OR injured OR IL)',
                   f'"{entity}" (strikeout OR "home run" OR stats OR performance)']

    seen_urls    = set()
    all_articles = []
    for q in queries:
        raw = (_news_fetch_articles(q, page_size=100, start_date=start_date, end_date=end_date)
               if NEWS_API_KEY and not _NEWS_FAILED else []) or _google_news_rss(q)
        for a in raw:
            u = a.get("url","")
            if u and u not in seen_urls:
                seen_urls.add(u)
                all_articles.append(a)

    if not all_articles:
        return []
    texts  = [f"{a.get('title','')} {a.get('description','')}" for a in all_articles]
    scores = score_texts(texts)
    rows   = [{
        "sport": "mlb", "team": entity,
        "headline":    (a.get("title") or "")[:500],
        "description": (a.get("description") or "")[:1000],
        "url":         (a.get("url") or "")[:500],
        "source_name": ((a.get("source") or {}).get("name") or a.get("source_name",""))[:100],
        "sentiment":   round(float(s), 3),
        "published_at": a.get("publishedAt"),
    } for a, s in zip(all_articles, scores)]
    try:
        from data.db import save_news_articles
        save_news_articles(rows)
    except Exception:
        pass
    return rows


# =========================================================================
# Game player sentiment props
# =========================================================================
def get_game_player_sentiment_props(
    home_team:   str,
    away_team:   str,
    sport:       str = "all",
    game_key:    str = "",
    game_date:   str = "",
    game_time:   str = "",
    max_players  = None,
    odds_hint    = None,
    include_news: bool = True,
) -> list:
    """
    Build player prop rows from multi-source sentiment + historical stats.
    Sources used: ESPN, RSS, Discord, Reddit JSON API, NewsAPI/Google RSS.
    """
    home = str(home_team or "").strip()
    away = str(away_team or "").strip()
    if not home or not away:
        return []

    max_rows     = max(1, min(int(max_players or SOCIAL_MAX_PLAYERS_PER_GAME or 8), 24))
    min_mentions = max(1, int(SOCIAL_PLAYER_MIN_MENTIONS or 1))
    sg           = _infer_sport_group(str(sport or "all"))

    ck = json.dumps({"home": home.lower(), "away": away.lower(), "sport": sg,
                     "gk": str(game_key or ""), "inc_news": bool(include_news),
                     "max_rows": max_rows, "odds": odds_hint}, sort_keys=True)
    now = datetime.datetime.now(datetime.timezone.utc)
    cached_row = _SOCIAL_PLAYER_CACHE.get(ck, {})
    cached_at  = cached_row.get("fetched_at")
    if cached_at and isinstance(cached_at, datetime.datetime):
        if (now - cached_at).total_seconds() / 60 <= 20:
            return list(cached_row.get("rows", []))

    home_aliases = _team_alias_tokens(home)
    away_aliases = _team_alias_tokens(away)
    source_rows  = []
    seen_text    = set()

    def _add_texts(texts, src):
        for t in texts:
            if t and t not in seen_text:
                seen_text.add(t)
                source_rows.append((src, t))

    # ESPN news (free, always run)
    for art in _fetch_espn_news_raw(sg, limit=60):
        text = f"{art.get('title','')} {art.get('description','')}".strip()
        if _text_mentions_team(text, home_aliases) or _text_mentions_team(text, away_aliases):
            _add_texts([text], "espn_news")

    # RSS feeds (free, always run)
    for art in _fetch_rss_articles_raw(sg):
        text = f"{art.get('title','')} {art.get('description','')}".strip()
        if _text_mentions_team(text, home_aliases) or _text_mentions_team(text, away_aliases):
            _add_texts([text], "rss")

    # Discord
    _add_texts(_discord_texts_for_entity(home, "team", sport=sg), "discord")
    _add_texts(_discord_texts_for_entity(away, "team", sport=sg), "discord")

    # Reddit JSON API (free)
    subs = list(_SPORT_SUBREDDITS.get(sg) or _SPORT_SUBREDDITS.get("all") or [])
    for team in [home, away]:
        ts = _team_subreddit(team)
        if ts and ts not in subs:
            subs.insert(0, ts)
    reddit_query = f"{home.split()[-1]} OR {away.split()[-1]}"
    _add_texts(_fetch_reddit_json_posts(subs, reddit_query, limit=30), "reddit")

    # NewsAPI / Google RSS
    if include_news:
        home_key = home.split()[-1]
        away_key = away.split()[-1]
        q = f'("{home_key}" OR "{away_key}") AND (player OR lineup OR prop OR odds OR injury)'
        raw_news = _news_fetch_articles(q, days_back=3, page_size=30) if NEWS_API_KEY and not _NEWS_FAILED else []
        if not raw_news:
            raw_news = _google_news_rss(q)
        for a in raw_news:
            text = f"{a.get('title','')} {a.get('description','')}".strip()
            if _text_mentions_team(text, home_aliases) or _text_mentions_team(text, away_aliases):
                _add_texts([text], "news")

    # Historical candidates (gap-fill)
    hist_candidates = (
        _historical_team_player_candidates(home, sport, max_rows=max_rows * 2) +
        _historical_team_player_candidates(away, sport, max_rows=max_rows * 2)
    )
    hist_by_name = {
        str(c.get("name","")).strip().lower(): c for c in hist_candidates if c.get("name")
    }

    if not source_rows and not hist_candidates:
        _SOCIAL_PLAYER_CACHE[ck] = {"fetched_at": now, "rows": []}
        return []

    # Score and bucket by player name
    buckets = {}
    if source_rows:
        source_rows = source_rows[:200]
        score_inputs = [_normalize_discord_slang(t) if s == "discord" else t
                        for s, t in source_rows]
        scores = score_texts(score_inputs)
        if len(scores) != len(source_rows):
            scores = [_keyword_sentiment(t) for _, t in source_rows]

        for (src, text), sc in zip(source_rows, scores):
            names     = list(set(_extract_player_name_candidates(text)))
            team_hint = _infer_team_from_text(text, home, away)
            for name in names:
                if _is_team_like_name(name, home, away):
                    continue
                row = buckets.setdefault(name, {
                    "mentions": 0, "sentiment_sum": 0.0,
                    "sources": set(), "team_hits": {},
                })
                row["mentions"]      += 1
                row["sentiment_sum"] += float(sc or 0.0)
                row["sources"].add(src)
                if team_hint:
                    row["team_hits"][team_hint] = row["team_hits"].get(team_hint, 0) + 1

    rows           = []
    existing_names = set()

    for player_name, data in buckets.items():
        mentions = int(data.get("mentions", 0) or 0)
        if mentions < min_mentions or _is_team_like_name(player_name, home, away):
            continue
        avg_sent  = float(data.get("sentiment_sum", 0.0)) / max(mentions, 1)
        hist      = hist_by_name.get(player_name.strip().lower())
        hist_prob = float((hist or {}).get("hist_prob") or 0.0) or None

        over_prob = 0.50 + max(-0.30, min(0.30, avg_sent * 0.22)) + min(0.10, math.log1p(mentions) * 0.03)
        over_prob = max(0.05, min(0.95, over_prob))
        if hist_prob is not None:
            over_prob = over_prob * 0.58 + hist_prob * 0.42
        over_prob = max(0.05, min(0.95, over_prob))

        direction  = "OVER" if over_prob >= 0.5 else "UNDER"
        pick_prob  = over_prob if direction == "OVER" else (1.0 - over_prob)
        model_prob = max(0.51, min(0.92, _blend_prob_with_odds(pick_prob, odds_hint)))
        odds_am    = _american_from_prob(model_prob)
        dec_odds   = round((1 + odds_am / 100.0) if odds_am > 0 else (1 + 100.0 / abs(odds_am)), 4)
        ev         = (dec_odds - 1.0) * model_prob - (1.0 - model_prob)

        team_hits = data.get("team_hits") or {}
        team_name = max(team_hits.items(), key=lambda kv: kv[1])[0] if team_hits else (
            (hist or {}).get("team") or home)

        sources_str = ",".join(sorted(data.get("sources") or []))
        rows.append(_build_prop_row(
            player_name=player_name, team_name=str(team_name),
            home=home, away=away, sport=sg, hist=hist,
            model_prob=model_prob, over_prob=over_prob, direction=direction,
            odds_am=odds_am, dec_odds=dec_odds, ev=ev,
            sentiment_score=avg_sent, mentions=mentions, sources_str=sources_str,
            game_key=game_key, game_date=game_date, game_time=game_time,
            worth_reason=f"Sentiment ({sources_str}) + historical profile",
        ))
        existing_names.add(player_name.strip().lower())

    # Gap-fill with historical performers not yet mentioned in social text
    for cand in hist_candidates:
        if len(rows) >= max_rows:
            break
        player_name = str(cand.get("name") or "").strip()
        if not player_name or player_name.lower() in existing_names:
            continue
        if _is_team_like_name(player_name, home, away):
            continue
        hist_prob   = float(cand.get("hist_prob") or 0.5)
        cached_sent = _cached_sentiment_payload(player_name, "player")
        sent_score  = float((cached_sent or {}).get("combined") or 0.0)
        over_prob   = max(0.05, min(0.95, hist_prob + sent_score * 0.08))
        direction   = "OVER" if over_prob >= 0.5 else "UNDER"
        pick_prob   = over_prob if direction == "OVER" else (1.0 - over_prob)
        model_prob  = max(0.51, min(0.92, _blend_prob_with_odds(pick_prob, odds_hint)))
        odds_am     = _american_from_prob(model_prob)
        dec_odds    = round((1 + odds_am / 100.0) if odds_am > 0 else (1 + 100.0 / abs(odds_am)), 4)
        ev          = (dec_odds - 1.0) * model_prob - (1.0 - model_prob)
        src         = "history,sentiment_cached" if cached_sent else "history"
        rows.append(_build_prop_row(
            player_name=player_name, team_name=str(cand.get("team") or home),
            home=home, away=away, sport=sg, hist=cand,
            model_prob=model_prob, over_prob=over_prob, direction=direction,
            odds_am=odds_am, dec_odds=dec_odds, ev=ev,
            sentiment_score=sent_score, mentions=0, sources_str=src,
            game_key=game_key, game_date=game_date, game_time=game_time,
            worth_reason="Historical multi-season trend with sentiment adjustment",
        ))
        existing_names.add(player_name.lower())

    # Deduplicate + sort
    deduped   = []
    seen_keys = set()
    for row in rows:
        key = (str(row.get("game_key","")), str(row.get("name","")).lower(),
               str(row.get("stat_type","")), str(row.get("line","")),
               str(row.get("direction","")))
        if key not in seen_keys:
            seen_keys.add(key)
            deduped.append(row)

    deduped.sort(key=lambda r: (float(r.get("model_prob",0)),
                                int(r.get("sentiment_mentions",0))), reverse=True)
    result = deduped[:max_rows]
    _SOCIAL_PLAYER_CACHE[ck] = {"fetched_at": now, "rows": result}
    return result


def _build_prop_row(*, player_name, team_name, home, away, sport, hist,
                    model_prob, over_prob, direction, odds_am, dec_odds, ev,
                    sentiment_score, mentions, sources_str, game_key,
                    game_date, game_time, worth_reason) -> dict:
    stat_type  = str((hist or {}).get("stat_type") or f"{sport}_sentiment")
    prop_label = str((hist or {}).get("prop_label") or "Sentiment Edge")
    line       = float((hist or {}).get("line") or 0.5)
    return {
        "sport":               sport,
        "name":                player_name,
        "team":                team_name,
        "prop_label":          prop_label,
        "stat_type":           stat_type,
        "line":                line,
        "direction":           direction,
        "model_prob":          round(model_prob, 4),
        "confidence":          int(round(model_prob * 100)),
        "safety_label":        _safety_label_from_prob(model_prob),
        "ev":                  round(ev, 4),
        "odds_am":             odds_am,
        "dec_odds":            dec_odds,
        "game":                f"{away} @ {home}",
        "game_key":            game_key,
        "match_key":           f"{away}@{home}",
        "game_date":           game_date,
        "game_time":           game_time,
        "home_team":           home,
        "away_team":           away,
        "sentiment_score":     round(sentiment_score, 4),
        "sentiment_mentions":  mentions,
        "sentiment_sources":   sources_str,
        "worth_it":            model_prob >= 0.57,
        "worth_score":         round(model_prob * 100.0, 2),
        "worth_reason":        worth_reason,
    }


# =========================================================================
# Player prop signal — historical + sentiment
# =========================================================================
_STAT_STD_FACTORS = {
    "strikeouts":0.35,"hits":0.55,"home_runs":0.80,"total_bases":0.50,
    "rbi":0.65,"runs":0.60,"walks":0.60,"stolen_bases":0.75,
    "batter_strikeouts":0.55,"doubles":0.70,"points":0.40,
    "assists":0.55,"rebounds":0.50,"goals":0.80,
}
_STAT_UNITS = {
    "strikeouts":"Ks","hits":"H","home_runs":"HR","total_bases":"TB",
    "rbi":"RBI","runs":"R","walks":"BB","stolen_bases":"SB",
    "batter_strikeouts":"K","doubles":"2B","points":"PTS",
    "assists":"AST","rebounds":"REB","goals":"G",
}
_STAT_TREND_KEYS = {
    "strikeouts":["strikeouts","k","so","avg"],"hits":["hits","h","avg"],
    "home_runs":["home_runs","hr","avg"],"total_bases":["total_bases","tb","avg"],
    "rbi":["rbi","avg"],"runs":["runs","r","avg"],"walks":["walks","bb","avg"],
    "stolen_bases":["stolen_bases","sb","avg"],"batter_strikeouts":["batter_strikeouts","k","so","avg"],
    "doubles":["doubles","2b","d","avg"],"points":["points","pts","avg"],
    "assists":["assists","ast","avg"],"rebounds":["rebounds","reb","avg"],
}


def _extract_trend_avg(blob, stat_type: str):
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
    if avg_val is None or avg_val <= 0:
        return 0.5
    try:
        from scipy.stats import norm
        std = max(float(avg_val) * std_factor, 0.1)
        return float(norm.sf(line, loc=float(avg_val), scale=std))
    except Exception:
        return 0.65 if float(avg_val) > line else 0.35


def get_player_prop_signal(player_name: str, stat_type: str, line: float,
                            prop_data: dict = None, pitcher_hand: str = None,
                            venue: str = None) -> dict:
    """
    Directional OVER/UNDER signal: 85% historical performance + 15% sentiment.
    Returns: direction, probability, confidence, rationale, hist_prob,
             sentiment_score, data_sources.
    """
    std_factor   = _STAT_STD_FACTORS.get(stat_type, 0.50)
    stat_unit    = _STAT_UNITS.get(stat_type, stat_type[:3].upper())
    rationale_parts = []
    data_sources    = []

    season_prob = last5_prob = last10_prob = matchup_prob = venue_prob = None
    season_avg_val = last5_avg_val = last10_avg_val = None

    # Apply pitcher-handedness split only to baseball hitter/pitching props.
    _mlb_pitch_props = {"strikeouts"}
    _mlb_hit_props = {
        "hits", "home_runs", "total_bases", "rbi", "runs",
        "walks", "stolen_bases", "batter_strikeouts", "doubles",
    }
    _use_pitcher_split = stat_type in (_mlb_pitch_props | _mlb_hit_props)

    try:
        from data.db import get_player_trends
        cur_year   = datetime.date.today().year
        trend_rows = get_player_trends(player_name, season=cur_year) or \
                     get_player_trends(player_name, season=cur_year - 1)
        _pitch_props = {"strikeouts"}
        _bat_props   = {"hits","home_runs","total_bases","rbi","runs","walks","stolen_bases",
                        "batter_strikeouts","doubles"}
        for t in trend_rows:
            ttype  = (t.get("stat_type") or "").lower()
            is_hit = (stat_type == ttype
                      or (stat_type in _pitch_props and ttype == "pitching")
                      or (stat_type in _bat_props   and ttype == "batting"))
            if not is_hit:
                continue
            sa = t.get("season_avg")
            if sa is not None:
                season_avg_val = float(sa)
                season_prob    = _over_prob_norm(season_avg_val, line, std_factor)
                data_sources.append("season_avg")
            l5 = _extract_trend_avg(t.get("last_5"), stat_type)
            if l5 is not None:
                last5_avg_val = l5
                last5_prob    = _over_prob_norm(l5, line, std_factor)
                data_sources.append("last_5")
            l10 = _extract_trend_avg(t.get("last_10"), stat_type)
            if l10 is not None:
                last10_avg_val = l10
                last10_prob    = _over_prob_norm(l10, line, std_factor)
                data_sources.append("last_10")
            if _use_pitcher_split and pitcher_hand:
                split_key = "vs_lefty" if pitcher_hand.upper() == "L" else "vs_righty"
                sv = t.get(split_key)
                if sv is not None:
                    matchup_prob = _over_prob_norm(float(sv), line, std_factor)
                    data_sources.append(f"vs_{pitcher_hand.upper()}")
            if venue:
                vv = t.get(f"{venue.lower()}_avg")
                if vv is not None:
                    venue_prob = _over_prob_norm(float(vv), line, std_factor)
                    data_sources.append(f"{venue}_split")
            break
    except Exception as e:
        print(f"[sentiment] prop_signal DB error: {e}")

    if prop_data and season_avg_val is None:
        avg_pg = prop_data.get("avg_per_game")
        if avg_pg:
            season_avg_val = float(avg_pg)
            season_prob    = _over_prob_norm(season_avg_val, line, std_factor)
            data_sources.append("model_avg")

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

    sentiment_score = 0.0
    try:
        from data.db import get_sentiment as _db_sent
        cached_s = _db_sent(player_name, hours=12)
        if cached_s.get("combined"):
            sentiment_score = float(cached_s["combined"]["score"])
            data_sources.append("sentiment_cached")
        elif not _HF_FAILED and not _NEWS_FAILED:
            sent = get_player_sentiment(player_name)
            sentiment_score = float(sent.get("combined", 0))
            if abs(sentiment_score) > 0.05:
                data_sources.append("sentiment_live")
    except Exception:
        pass

    final_prob = max(0.10, min(0.90, hist_prob + sentiment_score * 0.12))
    direction  = "OVER" if final_prob >= 0.5 else "UNDER"
    conf_prob  = final_prob if direction == "OVER" else 1.0 - final_prob
    confidence = round(conf_prob * 100)

    if last5_avg_val is not None and last10_avg_val is not None:
        arrow = "up" if last5_avg_val > last10_avg_val else "down"
        rationale_parts.append(f"L5 {last5_avg_val:.1f} {stat_unit} ({arrow})")
    elif last5_avg_val is not None:
        rationale_parts.append(f"L5 avg {last5_avg_val:.1f} {stat_unit}")
    if last10_avg_val is not None:
        rationale_parts.append(f"L10 {last10_avg_val:.1f}")
    if season_avg_val is not None and "season_avg" in data_sources:
        rationale_parts.append(f"season {season_avg_val:.1f}")
    if matchup_prob is not None and _use_pitcher_split:
        if stat_type in _mlb_hit_props:
            rationale_parts.append("AVG vs LHP" if (pitcher_hand or "").upper() == "L" else "AVG vs RHP")
        else:
            rationale_parts.append("vs LHP" if (pitcher_hand or "").upper() == "L" else "vs RHP")
    if abs(sentiment_score) > 0.10:
        buzz = "positive buzz" if sentiment_score > 0 else "negative buzz"
        rationale_parts.append(f"{buzz} ({sentiment_score:+.2f})")
    if not rationale_parts:
        rationale_parts.append(
            f"avg {season_avg_val:.1f} {stat_unit}/game" if season_avg_val else "model projection")

    return {
        "direction":       direction,
        "probability":     round(final_prob, 4),
        "confidence":      confidence,
        "rationale":       f"{direction} {line} {stat_unit} - " + " - ".join(rationale_parts),
        "hist_prob":       round(hist_prob, 4),
        "sentiment_score": round(sentiment_score, 4),
        "data_sources":    data_sources,
    }
