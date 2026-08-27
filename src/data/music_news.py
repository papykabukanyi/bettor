"""Trending music/entertainment news -- the fallback content source for
Threads' 30-minute trending-news post (see threads_post.post_trending_news)
when there's genuinely nothing fresh in the account's own market (stocks/
crypto/perps/options) AND no real story to comment on AND no reply-worthy
thread to jump into. Real feedback: publishing a bland "nothing notable
right now" filler in that situation is worse than posting something
genuinely engaging, even off-topic -- a real music headline with a real
artist photo keeps the account active and interesting on a slow news day
instead of visibly admitting it has nothing to say.

Three dedicated music newsrooms, same free/unlimited RSS approach as
crypto_news.py/stock_news.py's own newsroom feeds -- no API key needed.
Pitchfork and NME carry real photos directly in the feed (media:thumbnail
or an <img> inside the article body); Billboard's photo is embedded the
same way. Rolling Stone's feed carries no image at all (confirmed live
2026-08) and is deliberately NOT included here -- every story this module
returns is expected to have a genuine picture, since that's the entire
point of using it as a fallback over plain text.
"""
from __future__ import annotations

import logging
import re
import time
import xml.etree.ElementTree as ET
from typing import Any

import requests

logger = logging.getLogger(__name__)

_TIMEOUT_SEC = 8
_CACHE_TTL_SEC = 1800  # same "not minute-to-minute" cadence as stock_news/crypto_news trending feeds

_FEEDS = (
    ("https://pitchfork.com/feed/feed-news/rss", "Pitchfork"),
    ("https://www.nme.com/news/music/feed", "NME"),
    ("https://www.billboard.com/feed/", "Billboard"),
)

_MEDIA_TAGS = ("enclosure", "{http://search.yahoo.com/mrss/}content", "{http://search.yahoo.com/mrss/}thumbnail")
_IMG_SRC_RE = re.compile(r'<img[^>]+src="([^"]+)"')

_rich_feed_cache: dict[str, tuple[list[dict[str, Any]], float]] = {}


def _extract_image_url(item: ET.Element) -> str | None:
    for tag in _MEDIA_TAGS:
        el = item.find(tag)
        if el is not None and el.get("url"):
            return el.get("url")
    # Fallback: some newsrooms (Billboard, NME) embed the lead photo as a
    # plain <img> inside the full article body or the RSS description
    # rather than a proper media/enclosure tag -- same "at least try to
    # find a real picture" spirit as threads_post.py's own OG-image
    # resolution for scraped article links.
    for tag in ("{http://purl.org/rss/1.0/modules/content/}encoded", "description"):
        el = item.find(tag)
        if el is not None and el.text:
            match = _IMG_SRC_RE.search(el.text)
            if match:
                return match.group(1)
    return None


def _fetch_rss_items(url: str, *, source_name: str, limit: int = 15) -> list[dict[str, Any]]:
    now = time.time()
    cached = _rich_feed_cache.get(source_name)
    if cached and (now - cached[1]) < _CACHE_TTL_SEC:
        return cached[0]
    try:
        resp = requests.get(url, timeout=_TIMEOUT_SEC, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as exc:
        logger.warning("[music_news] feed fetch failed for %s: %s", source_name, exc)
        return cached[0] if cached else []
    items = []
    for item in root.iter("item"):
        title = item.findtext("title") or ""
        if not title:
            continue
        items.append({
            "title": title, "link": item.findtext("link") or "",
            "image_url": _extract_image_url(item), "source": source_name,
        })
    items = items[:limit]
    if items:
        _rich_feed_cache[source_name] = (items, now)
    return items


def get_trending_story() -> dict[str, Any] | None:
    """Picks ONE lead story -- the freshest item, across all 3 newsrooms,
    that actually carries a real photo (each feed is already newest-first,
    so the first qualifying item found is the freshest picture-bearing
    story overall). Falls back to the single freshest item with no photo
    only if literally none of them had one this cycle -- still real music
    news, just without the "artist pic" the caller asked for. Returns
    {"title", "link", "image_url", "source", "secondary": [titles...]} or
    None if every feed failed. Never raises."""
    items: list[dict[str, Any]] = []
    for url, name in _FEEDS:
        items.extend(_fetch_rss_items(url, source_name=name))
    if not items:
        return None

    with_image = [it for it in items if it.get("image_url")]
    lead = with_image[0] if with_image else items[0]

    seen_titles = {lead["title"]}
    secondary = []
    for it in items:
        if it["title"] in seen_titles or len(secondary) >= 3:
            continue
        seen_titles.add(it["title"])
        secondary.append(it["title"])

    return {
        "title": lead["title"], "link": lead["link"], "image_url": lead.get("image_url"),
        "source": lead["source"], "secondary": secondary,
    }
