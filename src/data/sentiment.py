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
import math
import unicodedata
import requests
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import (
    NEWS_API_KEY,
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
    TIKTOK_ENABLED,
    TIKTOK_HASHTAGS,
    TIKTOK_MAX_VIDEOS,
    TIKTOK_CACHE_MINUTES,
    SOCIAL_PLAYER_MIN_MENTIONS,
    SOCIAL_MAX_PLAYERS_PER_GAME,
)

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

_SENTIMENT_WEIGHT_CAPS = {
    "news": 8.0,
    "reddit": 5.0,
    "discord": 4.0,
    "tiktok": 4.0,
}

# ─── Circuit breakers ────────────────────────────────────────────────────────
_REDDIT_FAILED   = False
_HF_FAILED       = False
_NEWS_FAILED     = False
_DISCORD_FAILED  = False
_DISCORD_AUTH_LOGGED = False
_DISCORD_CACHE: dict[str, dict] = {}
_TIKTOK_CACHE: dict[str, dict] = {}
_SOCIAL_PLAYER_CACHE: dict[str, dict] = {}

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

# Common MLB team abbreviations used in chat
_TEAM_ABBR = {
    "yankees": "NYY",
    "red sox": "BOS",
    "blue jays": "TOR",
    "rays": "TBR",
    "orioles": "BAL",
    "white sox": "CHW",
    "guardians": "CLE",
    "tigers": "DET",
    "royals": "KCR",
    "twins": "MIN",
    "astros": "HOU",
    "athletics": "OAK",
    "mariners": "SEA",
    "angels": "LAA",
    "rangers": "TEX",
    "braves": "ATL",
    "phillies": "PHI",
    "mets": "NYM",
    "marlins": "MIA",
    "nationals": "WSN",
    "cubs": "CHC",
    "cardinals": "STL",
    "brewers": "MIL",
    "reds": "CIN",
    "pirates": "PIT",
    "dodgers": "LAD",
    "giants": "SFG",
    "padres": "SDP",
    "rockies": "COL",
    "diamondbacks": "ARI",
}

_DISCORD_SLANG = {
    "raking": "hitting excellently",
    "dealing": "pitching excellently",
    "got lit up": "pitched terribly",
    "cooked": "performing poorly",
    "dog water": "terrible",
    "w player": "great player",
    "l take": "bad opinion",
    "no cap": "honestly",
}


def _team_subreddit(team_name: str) -> str:
    """Return the team-specific subreddit name."""
    lower = team_name.lower()
    for keyword, sub in _TEAM_SUBREDDITS.items():
        if keyword in lower:
            return sub
    return ""


def _normalize_text(text: str) -> str:
    """Lowercase + strip accents/punct for robust token matching."""
    raw = unicodedata.normalize("NFKD", str(text))
    ascii_txt = raw.encode("ascii", "ignore").decode("ascii")
    ascii_txt = ascii_txt.lower()
    ascii_txt = re.sub(r"[^a-z0-9]+", " ", ascii_txt)
    return f" {ascii_txt.strip()} "


def _normalize_discord_slang(text: str) -> str:
    t = (text or "").lower()
    for slang, meaning in _DISCORD_SLANG.items():
        t = t.replace(slang, meaning)
    return t


# ─── Discord sentiment ─────────────────────────────────────────────────────

def _parse_discord_channels(raw: str) -> list[dict]:
    """Parse DISCORD_CHANNELS into a list of {guild_id, channel_id} dicts."""
    if not raw:
        return []
    tokens = re.split(r"[\s,|]+", raw.strip())
    parsed = []
    for t in tokens:
        if not t:
            continue
        ids = re.findall(r"\d{5,}", t)
        if len(ids) >= 2:
            guild_id, channel_id = ids[-2], ids[-1]
        elif len(ids) == 1:
            guild_id, channel_id = "", ids[0]
        else:
            continue
        parsed.append({"guild_id": guild_id, "channel_id": channel_id, "raw": t})
    return parsed


def _parse_discord_channel_map(raw: str) -> dict[str, list[dict]]:
    """
    Parse DISCORD_CHANNELS_BY_SPORT from entries like:
      mlb=123:456|123:789; soccer=234:567; all=999:111
    """
    out: dict[str, list[dict]] = {}
    if not raw:
        return out

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
        if not key:
            continue
        if not re.match(r"^[a-z_]+$", key):
            continue
        channels = _parse_discord_channels(values)
        if channels:
            out[key] = channels
    return out


def _resolve_discord_channels_for_sport(sport: str | None = None) -> list[dict]:
    channels = _parse_discord_channels(DISCORD_CHANNELS)
    by_sport = _parse_discord_channel_map(DISCORD_CHANNELS_BY_SPORT)
    sport_key = str(sport or "all").strip().lower()

    channels.extend(by_sport.get("all", []))
    if sport_key and sport_key != "all":
        channels.extend(by_sport.get(sport_key, []))

    deduped: list[dict] = []
    seen = set()
    for ch in channels:
        cid = str(ch.get("channel_id") or "")
        if not cid or cid in seen:
            continue
        seen.add(cid)
        deduped.append(ch)
    return deduped


def _discord_cache_key(sport: str | None, channels: list[dict]) -> str:
    sport_key = str(sport or "all").strip().lower()
    ids = sorted(str(c.get("channel_id") or "") for c in channels if c.get("channel_id"))
    joined = "-".join(ids)
    return f"{sport_key}|{joined}"


def _discord_headers() -> dict:
    return {"Authorization": f"Bot {DISCORD_BOT_TOKEN}"}


def _parse_discord_ts(ts: str) -> "datetime.datetime | None":
    if not ts:
        return None
    try:
        return datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _fetch_discord_messages(sport: str | None = None) -> list[dict]:
    """Fetch recent Discord messages from configured channels (cached)."""
    global _DISCORD_FAILED, _DISCORD_CACHE, _DISCORD_AUTH_LOGGED
    if _DISCORD_FAILED:
        return []
    if not DISCORD_BOT_TOKEN:
        return []

    channels = _resolve_discord_channels_for_sport(sport)
    if not channels:
        return []

    cache_key = _discord_cache_key(sport, channels)

    now = datetime.datetime.now(datetime.timezone.utc)
    cached_row = _DISCORD_CACHE.get(cache_key, {})
    cached_at = cached_row.get("fetched_at")
    if cached_at and isinstance(cached_at, datetime.datetime):
        age_min = (now - cached_at).total_seconds() / 60
        if age_min <= float(DISCORD_CACHE_MINUTES):
            return list(cached_row.get("messages", []))

    cutoff = now - datetime.timedelta(hours=float(DISCORD_LOOKBACK_HOURS))
    max_messages = max(1, int(DISCORD_MAX_MESSAGES))
    all_msgs: list[dict] = []

    for ch in channels:
        channel_id = ch.get("channel_id")
        if not channel_id:
            continue
        url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
        before = None
        fetched = 0
        while fetched < max_messages:
            params = {"limit": min(100, max_messages - fetched)}
            if before:
                params["before"] = before
            try:
                resp = requests.get(url, headers=_discord_headers(), params=params, timeout=15)
                if resp.status_code == 401:
                    # Invalid token: stop Discord sentiment attempts for this process.
                    if not _DISCORD_AUTH_LOGGED:
                        print("[sentiment] Discord auth error (invalid bot token)")
                        _DISCORD_AUTH_LOGGED = True
                    _DISCORD_FAILED = True
                    return []
                if resp.status_code == 403:
                    # Missing channel permission: skip this channel, continue others.
                    if not _DISCORD_AUTH_LOGGED:
                        print("[sentiment] Discord auth error (check channel permissions)")
                        _DISCORD_AUTH_LOGGED = True
                    break
                if resp.status_code == 429:
                    print("[sentiment] Discord rate limited - skipping")
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
                ts = _parse_discord_ts(msg.get("timestamp"))
                if ts and ts < cutoff:
                    stop_early = True
                    break
                content = (msg.get("content") or "").strip()
                if not content:
                    continue
                if msg.get("author", {}).get("bot"):
                    continue
                all_msgs.append({
                    "content": content,
                    "timestamp": msg.get("timestamp"),
                    "channel_id": channel_id,
                })
                fetched += 1
                if fetched >= max_messages:
                    break

            before = batch[-1].get("id")
            if stop_early or not before:
                break

    _DISCORD_CACHE[cache_key] = {"fetched_at": now, "messages": list(all_msgs)}
    return all_msgs


def _entity_tokens(entity: str, entity_type: str) -> list[str]:
    base = (entity or "").strip()
    if not base:
        return []
    norm = _normalize_text(base).strip()
    tokens = set()
    if norm:
        tokens.add(norm)
        tokens.add(norm.replace(" ", ""))
    parts = [p for p in norm.split() if p]
    tokens.update(parts)

    if entity_type == "team" and parts:
        last = parts[-1]
        tokens.add(last)
        abbr = _TEAM_ABBR.get(last)
        if abbr:
            tokens.add(abbr.lower())

    if entity_type == "player" and parts:
        suffixes = {"jr", "sr", "ii", "iii", "iv", "v"}
        last = parts[-1]
        if last in suffixes and len(parts) >= 2:
            last = parts[-2]
        tokens.add(last)
        tokens.add(parts[0])

    # Keep short tokens, but only match them with word boundaries later.
    return sorted(tokens, key=len, reverse=True)


def _discord_texts_for_entity(entity: str, entity_type: str, sport: str | None = None) -> list[str]:
    msgs = _fetch_discord_messages(sport=sport)
    if not msgs:
        return []
    tokens = _entity_tokens(entity, entity_type)
    if not tokens:
        return []
    hits: list[str] = []
    seen = set()
    for m in msgs:
        text = m.get("content", "")
        if not text:
            continue
        norm_text = _normalize_text(text)
        norm_nospace = norm_text.replace(" ", "")
        matched = False
        for t in tokens:
            if not t:
                continue
            if " " in t:
                if f" {t} " in norm_text:
                    matched = True
                    break
            elif len(t) <= 3:
                if f" {t} " in norm_text:
                    matched = True
                    break
            else:
                if t in norm_text or t in norm_nospace:
                    matched = True
                    break
        if matched and text not in seen:
            hits.append(text)
            seen.add(text)
    return hits


def get_discord_sentiment(entity: str, entity_type: str = "team", sport: str | None = None) -> dict:
    """Fetch recent Discord messages, score sentiment, return summary."""
    if not DISCORD_BOT_TOKEN:
        return {}
    texts = _discord_texts_for_entity(entity, entity_type, sport=sport)
    if not texts:
        return {}

    norm_texts = [_normalize_discord_slang(t) for t in texts if t.strip()]
    if not norm_texts:
        return {}

    scores = score_texts(norm_texts)
    avg_score = round(sum(scores) / len(scores), 4) if scores else 0.0

    all_text = " ".join(norm_texts).lower()
    words = re.findall(r"\b[a-z]{4,}\b", all_text)
    stop = {"that", "this", "they", "with", "have", "from", "will", "game",
            "baseball", "team", "player", "said", "their", "just", "been",
            "would", "could", "should", "when", "what", "about", "more"}
    freq = Counter(w for w in words if w not in stop)
    keywords = ", ".join(w for w, _ in freq.most_common(8))

    return {
        "score":    avg_score,
        "volume":   len(texts),
        "keywords": keywords,
        "source":   "discord",
    }


def _parse_tiktok_hashtags(raw: str) -> list[str]:
    if not raw:
        return []
    tags: list[str] = []
    seen = set()
    for token in re.split(r"[\s,|]+", str(raw or "").strip()):
        t = str(token or "").strip().lstrip("#").lower()
        if not t:
            continue
        t = re.sub(r"[^a-z0-9_]", "", t)
        if not t or t in seen:
            continue
        seen.add(t)
        tags.append(t)
    return tags


def _default_tiktok_hashtags_for_sport(sport: str) -> list[str]:
    mapping = {
        "baseball": ["mlb", "mlbpicks", "mlbprops", "baseballbetting"],
        "soccer": ["soccer", "soccerbets", "footballbetting", "soccerprops"],
        "basketball": ["nba", "nbabets", "basketballbets", "nbaprops"],
        "americanfootball": ["nfl", "nflbets", "footballbets", "nflprops"],
        "icehockey": ["nhl", "nhlbets", "hockeybets"],
        "tennis": ["tennis", "tennisbets"],
        "mma": ["mma", "ufc", "ufcbets"],
    }
    sk = str(sport or "all").strip().lower()
    if sk == "all":
        merged = []
        for arr in mapping.values():
            merged.extend(arr)
        return merged
    return mapping.get(sk, [])


def _tiktok_cache_key(sport: str, hashtags: list[str]) -> str:
    sk = str(sport or "all").strip().lower()
    return f"{sk}|{'-'.join(sorted(hashtags))}"


def _fetch_tiktok_hashtag_texts(sport: str = "all", limit: int = 60) -> list[str]:
    if not TIKTOK_ENABLED:
        return []

    env_tags = _parse_tiktok_hashtags(TIKTOK_HASHTAGS)
    tags = env_tags + [t for t in _default_tiktok_hashtags_for_sport(sport) if t not in env_tags]
    if not tags:
        return []

    max_items = max(1, min(int(limit or TIKTOK_MAX_VIDEOS or 40), int(TIKTOK_MAX_VIDEOS or 40)))
    cache_key = _tiktok_cache_key(sport, tags)
    now = datetime.datetime.now(datetime.timezone.utc)
    cached = _TIKTOK_CACHE.get(cache_key, {})
    cached_at = cached.get("fetched_at")
    if cached_at and isinstance(cached_at, datetime.datetime):
        age_min = (now - cached_at).total_seconds() / 60
        if age_min <= float(TIKTOK_CACHE_MINUTES):
            return list(cached.get("texts", []))[:max_items]

    rows: list[str] = []
    seen = set()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
    }

    for tag in tags:
        if len(rows) >= max_items:
            break
        remaining = max_items - len(rows)
        url = "https://www.tiktok.com/api/challenge/item_list/"
        params = {
            "challengeName": tag,
            "count": min(35, max(1, remaining)),
            "cursor": 0,
        }
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=12)
            if resp.status_code != 200:
                continue
            payload = resp.json() if resp.text else {}
            items = payload.get("itemList") or payload.get("items") or []
            for item in items:
                text = str(item.get("desc") or "").strip()
                if not text or text in seen:
                    continue
                seen.add(text)
                rows.append(text)
                if len(rows) >= max_items:
                    break
        except Exception:
            continue

    _TIKTOK_CACHE[cache_key] = {"fetched_at": now, "texts": rows}
    return rows


def _team_alias_tokens(team_name: str) -> list[str]:
    base = _normalize_text(team_name).strip()
    if not base:
        return []
    parts = [p for p in base.split() if p]
    stop = {"club", "city", "fc", "cf", "sc", "the", "de", "and"}
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


def _text_has_token(norm_text: str, norm_nospace: str, token: str) -> bool:
    if not token:
        return False
    t = token.strip().lower()
    if not t:
        return False
    if " " in t:
        return f" {t} " in norm_text
    if len(t) <= 3:
        return f" {t} " in norm_text
    return (t in norm_text) or (t in norm_nospace)


def _text_mentions_team(text: str, aliases: list[str]) -> bool:
    norm_text = _normalize_text(text)
    norm_nospace = norm_text.replace(" ", "")
    for token in aliases:
        if _text_has_token(norm_text, norm_nospace, token):
            return True
    return False


def _infer_team_from_text(text: str, home_team: str, away_team: str) -> str:
    home_alias = _team_alias_tokens(home_team)
    away_alias = _team_alias_tokens(away_team)
    home_hit = _text_mentions_team(text, home_alias)
    away_hit = _text_mentions_team(text, away_alias)
    if home_hit and not away_hit:
        return home_team
    if away_hit and not home_hit:
        return away_team
    return ""


def _extract_player_name_candidates(text: str) -> list[str]:
    raw = str(text or "")
    if not raw:
        return []

    block_words = {
        "Major League", "Premier League", "World Cup", "Champions League",
        "New York", "Los Angeles", "Golden State", "Manchester United",
        "First Half", "Second Half", "Full Time", "Game Total",
    }
    lowered_block = {
        "sportsbook", "odds", "moneyline", "parlay", "spread", "under", "over",
        "today", "tomorrow", "daily", "betting", "market", "props", "team",
    }

    out: list[str] = []
    seen = set()

    for m in re.finditer(r"\b[A-Z][a-z]{1,}(?:\s+[A-Z][a-z]{1,}){1,2}\b", raw):
        cand = m.group(0).strip()
        if not cand or cand in block_words:
            continue
        parts = cand.split()
        if len(parts) < 2:
            continue
        if any(ch.isdigit() for ch in cand):
            continue
        if any(p.lower() in lowered_block for p in parts):
            continue
        key = cand.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(cand)

    # Hashtag fallback (#ShoheiOhtani -> Shohei Ohtani)
    for tag in re.findall(r"#([A-Za-z][A-Za-z0-9]{5,})", raw):
        split = re.sub(r"([a-z])([A-Z])", r"\1 \2", tag).strip()
        if " " not in split:
            continue
        if any(ch.isdigit() for ch in split):
            continue
        if split.lower() in seen:
            continue
        seen.add(split.lower())
        out.append(split)

    return out


def _american_from_prob(prob: float) -> int:
    p = max(0.01, min(0.99, float(prob or 0.5)))
    if p >= 0.5:
        return int(round(-p / (1.0 - p) * 100))
    return int(round((1.0 - p) / p * 100))


def _implied_prob_from_american(odds) -> float | None:
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    if o > 0:
        return 100.0 / (o + 100.0)
    return abs(o) / (abs(o) + 100.0)


def _blend_prob_with_odds(sentiment_prob: float, odds_hint) -> float:
    implied = _implied_prob_from_american(odds_hint)
    if implied is None:
        return sentiment_prob
    blended = sentiment_prob * 0.72 + implied * 0.28
    return max(0.05, min(0.95, blended))


def _safety_label_from_prob(prob: float) -> str:
    p = float(prob or 0.5)
    if p >= 0.72:
        return "ELITE"
    if p >= 0.60:
        return "SAFE"
    if p >= 0.50:
        return "MODERATE"
    return "RISKY"


def get_game_player_sentiment_props(
    home_team: str,
    away_team: str,
    sport: str = "all",
    game_key: str = "",
    game_date: str = "",
    game_time: str = "",
    max_players: int | None = None,
    odds_hint=None,
    include_news: bool = True,
) -> list[dict]:
    """
    Build player rows from social sentiment mentions only.
    Output rows are compatible with dashboard player_props table.
    """
    home = str(home_team or "").strip()
    away = str(away_team or "").strip()
    if not home or not away:
        return []

    max_rows = int(max_players or SOCIAL_MAX_PLAYERS_PER_GAME or 8)
    max_rows = max(1, min(max_rows, 24))
    min_mentions = max(1, int(SOCIAL_PLAYER_MIN_MENTIONS or 1))

    cache_key = json.dumps({
        "home": home.lower(),
        "away": away.lower(),
        "sport": str(sport or "all").lower(),
        "gk": str(game_key or ""),
        "include_news": bool(include_news),
        "max_rows": max_rows,
        "odds_hint": odds_hint,
    }, sort_keys=True)
    now = datetime.datetime.now(datetime.timezone.utc)
    cached = _SOCIAL_PLAYER_CACHE.get(cache_key, {})
    cached_at = cached.get("fetched_at")
    if cached_at and isinstance(cached_at, datetime.datetime):
        age_min = (now - cached_at).total_seconds() / 60
        max_age = max(float(DISCORD_CACHE_MINUTES), float(TIKTOK_CACHE_MINUTES), 10.0)
        if age_min <= max_age:
            return list(cached.get("rows", []))

    source_rows: list[tuple[str, str]] = []
    seen_text = set()

    for txt in _discord_texts_for_entity(home, "team", sport=sport):
        if txt and txt not in seen_text:
            seen_text.add(txt)
            source_rows.append(("discord", txt))
    for txt in _discord_texts_for_entity(away, "team", sport=sport):
        if txt and txt not in seen_text:
            seen_text.add(txt)
            source_rows.append(("discord", txt))

    home_aliases = _team_alias_tokens(home)
    away_aliases = _team_alias_tokens(away)

    for txt in _fetch_tiktok_hashtag_texts(sport=sport, limit=80):
        if not txt:
            continue
        if not (_text_mentions_team(txt, home_aliases) or _text_mentions_team(txt, away_aliases)):
            continue
        if txt in seen_text:
            continue
        seen_text.add(txt)
        source_rows.append(("tiktok", txt))

    if include_news:
        home_key = home.split()[-1]
        away_key = away.split()[-1]
        query = f'("{home_key}" OR "{away_key}") AND (player OR lineup OR prop OR odds OR injury)'
        news_rows: list[dict] = []
        if NEWS_API_KEY and not _NEWS_FAILED:
            news_rows = _news_fetch_articles(query, days_back=3, page_size=30)
        if not news_rows:
            news_rows = _google_news_rss(query)
        for article in news_rows:
            text = f"{article.get('title') or ''} {article.get('description') or ''}".strip()
            if not text:
                continue
            if not (_text_mentions_team(text, home_aliases) or _text_mentions_team(text, away_aliases)):
                continue
            if text in seen_text:
                continue
            seen_text.add(text)
            source_rows.append(("news", text))

    if not source_rows:
        _SOCIAL_PLAYER_CACHE[cache_key] = {"fetched_at": now, "rows": []}
        return []

    source_rows = source_rows[:180]
    score_inputs = [
        _normalize_discord_slang(text) if src == "discord" else text
        for src, text in source_rows
    ]
    scores = score_texts(score_inputs)
    if len(scores) != len(source_rows):
        scores = [_keyword_sentiment(text) for _, text in source_rows]

    buckets: dict[str, dict] = {}
    for (src, text), sc in zip(source_rows, scores):
        names = list(set(_extract_player_name_candidates(text)))
        if not names:
            continue
        team_hit = _infer_team_from_text(text, home, away)
        for name in names:
            row = buckets.setdefault(name, {
                "mentions": 0,
                "sentiment_sum": 0.0,
                "sources": set(),
                "team_hits": {},
            })
            row["mentions"] += 1
            row["sentiment_sum"] += float(sc or 0.0)
            row["sources"].add(src)
            if team_hit:
                team_counts = row["team_hits"]
                team_counts[team_hit] = team_counts.get(team_hit, 0) + 1

    rows: list[dict] = []
    for player_name, data in buckets.items():
        mentions = int(data.get("mentions", 0) or 0)
        if mentions < min_mentions:
            continue

        avg_sent = float(data.get("sentiment_sum", 0.0) or 0.0) / max(mentions, 1)
        direction = "OVER" if avg_sent >= 0 else "UNDER"

        sentiment_strength = min(0.30, abs(avg_sent) * 0.22 + min(0.12, math.log1p(mentions) * 0.06))
        model_prob = 0.50 + sentiment_strength
        model_prob = _blend_prob_with_odds(model_prob, odds_hint)
        model_prob = max(0.51, min(0.92, model_prob))

        odds_am = _american_from_prob(model_prob)
        dec_odds = round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4)
        ev = (dec_odds - 1.0) * model_prob - (1.0 - model_prob)

        team_hits = data.get("team_hits", {}) or {}
        if team_hits:
            team_name = max(team_hits.items(), key=lambda kv: kv[1])[0]
        else:
            team_name = home

        sport_key = re.sub(r"[^a-z0-9_]+", "_", str(sport or "all").lower()).strip("_") or "all"
        rows.append({
            "sport": sport_key,
            "name": player_name,
            "team": team_name,
            "prop_label": "Sentiment -Odds Edge",
            "stat_type": f"{sport_key}_sentiment",
            "line": 0.5,
            "direction": direction,
            "model_prob": round(model_prob, 4),
            "confidence": int(round(model_prob * 100)),
            "safety_label": _safety_label_from_prob(model_prob),
            "ev": round(ev, 4),
            "odds_am": odds_am,
            "dec_odds": dec_odds,
            "game": f"{away} @ {home}",
            "game_key": game_key,
            "match_key": f"{away}@{home}",
            "game_date": game_date,
            "game_time": game_time,
            "home_team": home,
            "away_team": away,
            "sentiment_score": round(avg_sent, 4),
            "sentiment_mentions": mentions,
            "sentiment_sources": ",".join(sorted(data.get("sources") or [])),
            "worth_it": model_prob >= 0.57,
            "worth_score": round(model_prob * 100.0, 2),
            "worth_reason": "Only players mentioned in social sentiment feeds are included",
        })

    rows.sort(key=lambda r: (int(r.get("sentiment_mentions") or 0), float(r.get("model_prob") or 0.0)), reverse=True)
    rows = rows[:max_rows]
    _SOCIAL_PLAYER_CACHE[cache_key] = {"fetched_at": now, "rows": rows}
    return rows


def get_tiktok_sentiment(entity: str, entity_type: str = "team", sport: str | None = None) -> dict:
    """Estimate sentiment for an entity from TikTok hashtag descriptions."""
    if not TIKTOK_ENABLED:
        return {}
    texts = _fetch_tiktok_hashtag_texts(sport=sport or "all", limit=90)
    if not texts:
        return {}

    tokens = _entity_tokens(entity, entity_type)
    if not tokens:
        return {}

    hits: list[str] = []
    seen = set()
    for text in texts:
        norm_text = _normalize_text(text)
        norm_nospace = norm_text.replace(" ", "")
        matched = False
        for t in tokens:
            if _text_has_token(norm_text, norm_nospace, t):
                matched = True
                break
        if matched and text not in seen:
            seen.add(text)
            hits.append(text)

    if not hits:
        return {}

    scores = score_texts(hits)
    avg_score = round(sum(scores) / len(scores), 4) if scores else 0.0
    words = re.findall(r"\b[a-z]{4,}\b", " ".join(hits).lower())
    stop = {
        "that", "this", "they", "with", "have", "from", "will", "game",
        "team", "player", "props", "odds", "betting", "more", "today",
    }
    freq = Counter(w for w in words if w not in stop)
    keywords = ", ".join(w for w, _ in freq.most_common(8))

    return {
        "score": avg_score,
        "volume": len(hits),
        "keywords": keywords,
        "source": "tiktok",
    }


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


def _cached_sentiment_payload(entity: str, entity_kind: str) -> dict | None:
    try:
        from data.db import get_sentiment as db_get_sentiment

        cached = db_get_sentiment(entity, hours=18)
    except Exception:
        return None
    if not cached or "combined" not in cached:
        return None
    return {
        entity_kind: entity,
        "reddit": cached.get("reddit", {}),
        "news": cached.get("news", {}),
        "discord": cached.get("discord", {}),
        "tiktok": cached.get("tiktok", {}),
        "combined": float((cached.get("combined") or {}).get("score", 0.0) or 0.0),
        "volume": sum(int((cached.get(src) or {}).get("volume", 0) or 0) for src in ("reddit", "news", "discord", "tiktok")),
    }


def _sentiment_weight(source_name: str, source_data: dict) -> float:
    score = source_data.get("score")
    volume = int(source_data.get("volume", 0) or 0)
    if score is None or volume <= 0:
        return 0.0
    return min(_SENTIMENT_WEIGHT_CAPS.get(source_name, 4.0), math.sqrt(volume))


def _combine_sentiment_sources(
    reddit_data: dict,
    news_data: dict,
    discord_data: dict,
    tiktok_data: dict | None = None,
) -> tuple[float, int]:
    scores = []
    weights = []
    raw_volume = 0
    for source_name, source_data in (
        ("reddit", reddit_data),
        ("news", news_data),
        ("discord", discord_data),
        ("tiktok", tiktok_data or {}),
    ):
        raw_volume += int(source_data.get("volume", 0) or 0)
        weight = _sentiment_weight(source_name, source_data)
        if weight <= 0:
            continue
        scores.append(float(source_data.get("score", 0.0)))
        weights.append(weight)
    if not scores:
        return 0.0, raw_volume
    total_weight = sum(weights)
    combined = round(sum(score * weight for score, weight in zip(scores, weights)) / total_weight, 4)
    return combined, raw_volume


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

def get_team_sentiment(team_name: str, sport: str | None = None) -> dict:
    """
    Get sentiment for a team using NewsAPI (Reddit used automatically when
    credentials are configured; silently skipped otherwise).
    Returns {reddit, news, combined} score dict.
    """
    cached = _cached_sentiment_payload(team_name, "team")
    if cached:
        return cached

    # Reddit: only runs when real credentials are set
    reddit_data  = get_reddit_sentiment(team_name, entity_type="team")
    news_data    = get_news_sentiment(team_name, entity_type="team")
    discord_data = get_discord_sentiment(team_name, entity_type="team", sport=sport)
    tiktok_data  = get_tiktok_sentiment(team_name, entity_type="team", sport=sport)

    combined, raw_volume = _combine_sentiment_sources(reddit_data, news_data, discord_data, tiktok_data)

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
        if discord_data:
            save_sentiment(team_name, "team", "discord",
                           discord_data.get("score", 0.0),
                           discord_data.get("volume", 0),
                           discord_data.get("keywords", ""))
        if tiktok_data:
            save_sentiment(team_name, "team", "tiktok",
                           tiktok_data.get("score", 0.0),
                           tiktok_data.get("volume", 0),
                           tiktok_data.get("keywords", ""))
        if raw_volume > 0:
            save_sentiment(team_name, "team", "combined", combined,
                           raw_volume, "")
    except Exception as e:
        print(f"[sentiment] DB save error: {e}")

    return {
        "team":     team_name,
        "reddit":   reddit_data,
        "news":     news_data,
        "discord":  discord_data,
        "tiktok":   tiktok_data,
        "combined": combined,
        "volume":   raw_volume,
    }


def get_player_sentiment(player_name: str, sport: str | None = None) -> dict:
    """Get combined sentiment for a player from NewsAPI (+ Reddit when configured)."""
    cached = _cached_sentiment_payload(player_name, "player")
    if cached:
        return cached

    reddit_data  = get_reddit_sentiment(player_name, entity_type="player", post_limit=20)
    news_data    = get_news_sentiment(player_name, entity_type="player")
    discord_data = get_discord_sentiment(player_name, entity_type="player", sport=sport)
    tiktok_data  = get_tiktok_sentiment(player_name, entity_type="player", sport=sport)

    combined, raw_volume = _combine_sentiment_sources(reddit_data, news_data, discord_data, tiktok_data)

    try:
        from data.db import save_sentiment
        if reddit_data:
            save_sentiment(player_name, "player", "reddit",
                           reddit_data.get("score", 0.0),
                           reddit_data.get("volume", 0),
                           reddit_data.get("keywords", ""))
        if news_data:
            save_sentiment(player_name, "player", "news",
                           news_data.get("score", 0.0),
                           news_data.get("volume", 0),
                           news_data.get("keywords", ""))
        if discord_data:
            save_sentiment(player_name, "player", "discord",
                           discord_data.get("score", 0.0),
                           discord_data.get("volume", 0),
                           discord_data.get("keywords", ""))
        if tiktok_data:
            save_sentiment(player_name, "player", "tiktok",
                           tiktok_data.get("score", 0.0),
                           tiktok_data.get("volume", 0),
                           tiktok_data.get("keywords", ""))
        if raw_volume > 0:
            save_sentiment(player_name, "player", "combined", combined,
                           raw_volume, "")
    except Exception:
        pass

    return {
        "player":   player_name,
        "reddit":   reddit_data,
        "news":     news_data,
        "discord":  discord_data,
        "tiktok":   tiktok_data,
        "combined": combined,
        "volume":   raw_volume,
    }


def get_game_sentiments(home_team: str, away_team: str, sport: str | None = None) -> dict:
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

    home_sent = get_team_sentiment(home_team, sport=sport)
    away_sent = get_team_sentiment(away_team, sport=sport)
    return {
        "home": {
            "combined":      home_sent["combined"],
            "reddit_score":  home_sent["reddit"].get("score", 0),
            "news_score":    home_sent["news"].get("score", 0),
            "discord_score": home_sent.get("discord", {}).get("score", 0),
            "tiktok_score":  home_sent.get("tiktok", {}).get("score", 0),
            "news_keywords": home_sent["news"].get("keywords", ""),
            "discord_keywords": home_sent.get("discord", {}).get("keywords", ""),
            "tiktok_keywords": home_sent.get("tiktok", {}).get("keywords", ""),
            "volume":        home_sent["volume"],
        },
        "away": {
            "combined":      away_sent["combined"],
            "reddit_score":  away_sent["reddit"].get("score", 0),
            "news_score":    away_sent["news"].get("score", 0),
            "discord_score": away_sent.get("discord", {}).get("score", 0),
            "tiktok_score":  away_sent.get("tiktok", {}).get("score", 0),
            "news_keywords": away_sent["news"].get("keywords", ""),
            "discord_keywords": away_sent.get("discord", {}).get("keywords", ""),
            "tiktok_keywords": away_sent.get("tiktok", {}).get("keywords", ""),
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
