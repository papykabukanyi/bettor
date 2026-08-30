"""Posts a real-time note to Meta Threads every time the Kalshi Perps bot
(or the Alpaca stocks bot) enters a real trade -- ticker, side, entry
price, take-profit/stop-loss targets, and why the model/technical signal
triggered the entry. Also posts a short notice on every process boot, an
hourly status update (open positions, or "flat", plus today's realized
P&L), and a 30-minute trending-news digest (see post_trending_news) meant
to surface whatever news might be influencing the bot's own decisions
right now, not just report positions after the fact.

Best-effort only, by design: a failure here must NEVER block or delay real
trade execution, mirroring how a failed news-sentiment fetch never blocks
the trading loop either (see crypto_news.py's own established discipline).
Every function in this module catches its own exceptions and returns a
plain bool rather than raising.

Posting requires a completed interactive Threads login (see
threads_client.get_authorization_url()) -- THREADS_APP_ID/THREADS_APP_SECRET
alone cannot post anything on their own.
"""
from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import threading
import time
from typing import Any

from data import threads_client

logger = logging.getLogger(__name__)

THREADS_POST_ENABLED = str(os.getenv("THREADS_POST_ENABLED", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}

_THREADS_POST_MAX_CHARS = 500

# ── Recently-posted-story dedup (real feedback: the same trending headline
# was posting again on a later 30-minute cycle whenever a slow news day left
# it still the top/freshest item) ────────────────────────────────────────────
# Shared via HF (same durable-state pattern threads_client.py already uses
# for OAuth tokens) since crypto's and stocks' trending-news jobs each run
# in their own separate Render service/process with no shared memory
# otherwise. Keyed by `market` (crypto/stocks/perps/options each have an
# independent news pool, so a story posted for one market says nothing
# about another). A story "ages out" after _RECENT_NEWS_MAX_AGE_SEC so a
# genuinely recurring story (e.g. a multi-day market event) can eventually
# be posted again once it's no longer a same-cycle repeat.
HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_MODEL_REPO = os.getenv("HF_MODEL_REPO", "papylove/kalshi-perps-model")
_RECENT_NEWS_HF_FILENAME = "threads_recent_news.json"
_RECENT_NEWS_MAX_AGE_SEC = float(os.getenv("THREADS_RECENT_NEWS_MAX_AGE_HOURS", "48") or "48") * 3600
_RECENT_NEWS_MAX_PER_MARKET = 100
_RECENT_NEWS_HF_TIMEOUT_SEC = int(os.getenv("THREADS_RECENT_NEWS_HF_TIMEOUT_SEC", "10") or "10")
_recent_news_lock = threading.RLock()  # reentrant: _record_posted_story holds this while calling _load_recent_news, which also acquires it
_recent_news_cache: dict[str, list[dict[str, Any]]] | None = None


def _significant_words(title: str) -> set[str]:
    """Same technique crypto_news.py's own cross-outlet corroboration
    already uses -- lets a paraphrased repost of the same real-world story
    (different outlet, slightly different wording) still count as a
    duplicate, not just a byte-for-byte identical title."""
    stopwords = {
        "the", "a", "an", "of", "to", "in", "on", "for", "and", "or", "is", "are",
        "with", "at", "by", "as", "its", "it", "this", "that", "after", "amid",
        "over", "into", "new", "why", "how", "what", "will", "could", "may",
        "says", "said", "vs", "than", "up", "down", "out", "now",
    }
    return {w for w in re.findall(r"[a-z0-9']+", title.lower()) if len(w) > 2 and w not in stopwords}


def _pull_json_from_hf(filename: str, *, timeout_sec: int) -> Any:
    """Generic small-JSON-file pull from HF_MODEL_REPO -- shared by every
    Threads-side durable store in this module (recently-posted stories,
    already-replied-to post ids) so the download/hard-timeout plumbing
    exists in exactly one place. Same pattern threads_client.py's own token
    pull uses. Returns None (never raises) on any failure, including "no
    such file yet" (the normal first-ever-call state)."""
    if not HF_API_KEY:
        return None

    def _download() -> Any:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(repo_id=HF_MODEL_REPO, filename=filename, repo_type="model", token=HF_API_KEY)
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    try:
        from server_common import call_with_hard_timeout
        return call_with_hard_timeout(_download, timeout_sec=timeout_sec)
    except Exception as exc:
        logger.info("[threads_post] no %s on HF yet (or fetch failed): %s", filename, exc)
        return None


def _push_json_to_hf(filename: str, data: Any, *, timeout_sec: int, commit_message: str) -> None:
    if not HF_API_KEY:
        return

    def _upload() -> None:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            json.dump(data, tmp, indent=2)
            tmp_path = tmp.name
        try:
            api.upload_file(
                path_or_fileobj=tmp_path, path_in_repo=filename,
                repo_id=HF_MODEL_REPO, repo_type="model", commit_message=commit_message,
            )
        finally:
            os.unlink(tmp_path)

    try:
        from server_common import call_with_hard_timeout
        call_with_hard_timeout(_upload, timeout_sec=timeout_sec)
    except Exception as exc:
        logger.warning("[threads_post] %s push to HF failed: %s", filename, exc)


def _load_recent_news() -> dict[str, list[dict[str, Any]]]:
    global _recent_news_cache
    with _recent_news_lock:
        if _recent_news_cache is None:
            _recent_news_cache = _pull_json_from_hf(_RECENT_NEWS_HF_FILENAME, timeout_sec=_RECENT_NEWS_HF_TIMEOUT_SEC) or {}
        return _recent_news_cache


def _is_recent_duplicate_story(market: str, title: str) -> bool:
    """True if `title` matches (exactly or as a near-duplicate -- see
    _significant_words) any story already recorded as posted for this
    market within _RECENT_NEWS_MAX_AGE_SEC."""
    now = time.time()
    entries = _load_recent_news().get(market, [])
    words = _significant_words(title)
    for entry in entries:
        if now - float(entry.get("posted_at", 0)) >= _RECENT_NEWS_MAX_AGE_SEC:
            continue
        entry_title = entry.get("title", "")
        if entry_title == title:
            return True
        if len(words & _significant_words(entry_title)) >= 3:
            return True
    return False


def _record_posted_story(market: str, title: str, *, source: str | None = None, secondary: list[str] | None = None) -> None:
    """Best-effort -- a failure here means a duplicate might slip through on
    a later cycle, not that the post itself (already sent) fails. Also the
    source of "last known story" for this market (see _last_known_story) --
    stores enough context (source/secondary, not just the title) to serve
    as a genuine commentary SUBJECT later, not just a dedup key."""
    try:
        with _recent_news_lock:
            store = dict(_load_recent_news())
            now = time.time()
            entries = [
                e for e in store.get(market, [])
                if now - float(e.get("posted_at", 0)) < _RECENT_NEWS_MAX_AGE_SEC
            ]
            entries.append({"title": title, "posted_at": now, "source": source, "secondary": secondary or []})
            store[market] = entries[-_RECENT_NEWS_MAX_PER_MARKET:]
            global _recent_news_cache
            _recent_news_cache = store
        _push_json_to_hf(
            _RECENT_NEWS_HF_FILENAME, store, timeout_sec=_RECENT_NEWS_HF_TIMEOUT_SEC,
            commit_message="update recently-posted news store",
        )
    except Exception as exc:
        logger.warning("[threads_post] failed to record posted story for dedup: %s", exc)


def _last_known_story(market: str) -> dict[str, Any] | None:
    """The most recently posted real story for this market (regardless of
    how long ago -- NOT filtered by _RECENT_NEWS_MAX_AGE_SEC, unlike the
    dedup check itself), reconstructed as a story-like dict. Used as a
    genuine commentary SUBJECT when a cycle's feed comes back completely
    empty (no story at all, not just a duplicate -- see
    post_trending_news's own no-fresh-story fallback), so "nothing new
    right now" still has something real and on-topic to say instead of
    admitting defeat. None only if this market has never recorded a real
    post at all (a fresh deploy, or HF unreachable)."""
    entries = _load_recent_news().get(market, [])
    if not entries:
        return None
    latest = entries[-1]
    return {
        "title": latest.get("title", ""), "source": latest.get("source"),
        "secondary": latest.get("secondary") or [], "link": "", "image_url": None,
    }


# ── Keyword-search auto-reply (real, working capability -- see module note
# below on its current reach limitation) ─────────────────────────────────────
_REPLIED_POSTS_HF_FILENAME = "threads_replied_posts.json"
_REPLIED_POSTS_MAX_AGE_SEC = 30 * 24 * 3600  # 30 days -- plenty long to never re-reply to the same post
_REPLIED_POSTS_MAX_STORED = 500
_REPLIED_POSTS_HF_TIMEOUT_SEC = int(os.getenv("THREADS_REPLIED_POSTS_HF_TIMEOUT_SEC", "10") or "10")
_replied_posts_lock = threading.RLock()  # reentrant: _record_replied_post holds this while calling _load_replied_posts, which also acquires it
_replied_posts_cache: dict[str, float] | None = None
# Small, deliberately conservative cap -- this reaches OTHER accounts'
# timelines, unlike every other function in this module (which only ever
# posts as this account). A real spam/platform-policy risk if run
# aggressively; kept low regardless of how many qualifying posts a search
# returns.
MAX_AUTO_REPLIES_PER_RUN = int(os.getenv("THREADS_MAX_AUTO_REPLIES_PER_RUN", "2") or "2")


def _load_replied_posts() -> dict[str, float]:
    global _replied_posts_cache
    with _replied_posts_lock:
        if _replied_posts_cache is None:
            _replied_posts_cache = _pull_json_from_hf(_REPLIED_POSTS_HF_FILENAME, timeout_sec=_REPLIED_POSTS_HF_TIMEOUT_SEC) or {}
        return _replied_posts_cache


def _record_replied_post(post_id: str) -> None:
    try:
        with _replied_posts_lock:
            store = dict(_load_replied_posts())
            now = time.time()
            store = {pid: ts for pid, ts in store.items() if now - float(ts) < _REPLIED_POSTS_MAX_AGE_SEC}
            store[post_id] = now
            # Oldest-first trim once over the cap -- same unbounded-growth
            # discipline every other durable store in this codebase applies.
            if len(store) > _REPLIED_POSTS_MAX_STORED:
                for pid in sorted(store, key=store.get)[: len(store) - _REPLIED_POSTS_MAX_STORED]:
                    del store[pid]
            global _replied_posts_cache
            _replied_posts_cache = store
        _push_json_to_hf(
            _REPLIED_POSTS_HF_FILENAME, store, timeout_sec=_REPLIED_POSTS_HF_TIMEOUT_SEC,
            commit_message="update already-replied-to posts store",
        )
    except Exception as exc:
        logger.warning("[threads_post] failed to record replied post for dedup: %s", exc)


def reply_to_trending_keyword_posts(query: str, *, market: str = "perps", max_replies: int | None = None) -> dict[str, Any]:
    """Searches Threads for public posts matching `query` (see
    threads_client.search_keyword_posts) and replies to up to
    `max_replies` (default MAX_AUTO_REPLIES_PER_RUN) of them that haven't
    already been replied to, in the "news anchor" persona (see
    threads_persona.anchor_draft_reply) -- real engagement/marketing, not a
    boilerplate template, and the persona is instructed to mention this
    bot's own site only when it's actually relevant, not on every reply.

    IMPORTANT, CONFIRMED LIVE LIMITATION (2026-08-23): threads_keyword_search
    is functionally self-only under Meta's default "Standard Access" grant
    for this app -- `query` will only ever match THIS account's own past
    posts until Meta's App Review grants Advanced Access for that specific
    permission. This function is real and fully wired (so it starts working
    the moment that access is granted, no code change needed), but its
    PRACTICAL reach today is "reply to my own old posts matching `query`",
    not genuinely popular third-party posts -- do not expect broad
    engagement from this until that review clears.

    Never raises -- returns {"ok", "candidates_found", "replied": [...],
    "skipped_already_replied": N} even on a total failure (empty result,
    `ok: False` with `error`), matching every other Threads function's
    best-effort contract. Deliberately capped low (see
    MAX_AUTO_REPLIES_PER_RUN's own comment) since, unlike every other post
    this module makes, a reply reaches someone ELSE's timeline."""
    if not THREADS_POST_ENABLED:
        return {"ok": False, "reason": "threads_post_disabled", "replied": []}
    cap = max_replies if max_replies is not None else MAX_AUTO_REPLIES_PER_RUN
    try:
        candidates = threads_client.search_keyword_posts(query, search_type="TOP", limit=25)
    except Exception as exc:
        logger.warning("[threads_post] keyword search for %r failed: %s", query, exc)
        return {"ok": False, "error": str(exc), "replied": []}

    already_replied = _load_replied_posts()
    replied: list[dict[str, Any]] = []
    skipped = 0
    for post in candidates:
        if len(replied) >= cap:
            break
        post_id = post.get("id")
        if not post_id or post_id in already_replied:
            if post_id:
                skipped += 1
            continue
        try:
            from data import threads_persona
            reply_text = threads_persona.anchor_draft_reply(post.get("text") or "", author_username=post.get("username"))
        except Exception as exc:
            logger.warning("[threads_post] reply drafting failed for post %s: %s", post_id, exc)
            reply_text = None
        if not reply_text:
            continue  # never post a generic fallback reply -- a low-quality reply is worse than no reply
        try:
            threads_client.create_and_publish_post(reply_text, reply_to_id=post_id)
            _record_replied_post(post_id)
            replied.append({"post_id": post_id, "username": post.get("username"), "reply_text": reply_text})
        except Exception as exc:
            logger.warning("[threads_post] failed to post reply to %s: %s", post_id, exc)

    return {
        "ok": True, "query": query, "market": market, "candidates_found": len(candidates),
        "replied": replied, "skipped_already_replied": skipped,
    }


# Every post used to say "Kalshi Perps" regardless of which of the four
# asset-class services actually sent it -- a real mislabeling bug once
# stocks/crypto/options started sharing this same module. `market` now
# drives both the label and a set of contextual hashtags meant to help
# each post actually get found/gain attention on Threads, not just report
# the trade.
_MARKET_LABELS = {
    "perps": "Kalshi Perps", "stocks": "Alpaca Stocks", "crypto": "Alpaca Crypto", "options": "Alpaca Options",
}
_MARKET_HASHTAGS = {
    "perps": "#Kalshi #PredictionMarkets #Crypto #Trading",
    "stocks": "#StockMarket #Stocks #Trading #Investing",
    "crypto": "#Crypto #Bitcoin #CryptoTrading #Altcoins",
    "options": "#OptionsTrading #Stocks #Calls #Puts",
}


def _market_label(market: str) -> str:
    return _MARKET_LABELS.get(market, _MARKET_LABELS["perps"])


def _hashtags_for_market(market: str) -> str:
    return _MARKET_HASHTAGS.get(market, _MARKET_HASHTAGS["perps"])


# Short form of _MARKET_LABELS (no "Alpaca"/"Kalshi" prefix) for the
# trending-news digest specifically, which has always used this shorter
# style ("Stocks trending news...", not "Alpaca Stocks trending news...").
_SHORT_MARKET_LABELS = {"perps": "Perps", "stocks": "Stocks", "crypto": "Crypto", "options": "Options"}


def _short_market_label(market: str) -> str:
    return _SHORT_MARKET_LABELS.get(market, "Perps")


# Real feedback: a bland "nothing notable right now" filler post is low-
# value on its own, and got noticeably WORSE once dedup started skipping
# genuine repeats -- it could now fire on essentially every cycle a slow
# news day produced only a duplicate, instead of just occasionally. This
# is what post_trending_news's own no-fresh-story branch searches with
# when it goes for real engagement (a reply round) instead of filler text.
_REPLY_KEYWORDS_BY_MARKET = {"perps": "crypto", "crypto": "crypto", "stocks": "stocks", "options": "stocks"}


def _reply_keyword_for_market(market: str) -> str:
    return _REPLY_KEYWORDS_BY_MARKET.get(market, "trading")


# Discovery hashtags extracted straight from the headline's own text --
# specific/topical tags (a coin name, a ticker, "Fed", "earnings", ...) get
# found by people searching or browsing THAT topic, on top of (not instead
# of) the generic per-market tag set above. Several keys deliberately map to
# the same tag (btc/bitcoin) since a headline might use either form.
_KEYWORD_HASHTAGS = {
    "bitcoin": "#Bitcoin", "btc": "#Bitcoin", "ethereum": "#Ethereum", "eth": "#Ethereum",
    "solana": "#Solana", "xrp": "#XRP", "ripple": "#XRP", "dogecoin": "#Dogecoin",
    "litecoin": "#Litecoin", "chainlink": "#Chainlink", "polkadot": "#Polkadot",
    "hedera": "#Hedera", "hyperliquid": "#Hyperliquid", "shiba inu": "#SHIB",
    "stellar": "#Stellar", "zcash": "#Zcash", "stablecoin": "#Stablecoins",
    "altcoin": "#Altcoins", "defi": "#DeFi", "nft": "#NFT",
    "etf": "#ETF", "sec": "#SEC", "fed": "#FederalReserve", "federal reserve": "#FederalReserve",
    "inflation": "#Inflation", "rate cut": "#RateCuts", "interest rate": "#InterestRates",
    "earnings": "#Earnings", "ipo": "#IPO", "recession": "#Recession",
    "s&p": "#SPX", "nasdaq": "#Nasdaq", "dow jones": "#DowJones",
    "apple": "#Apple", "tesla": "#Tesla", "nvidia": "#Nvidia", "microsoft": "#Microsoft",
    "amazon": "#Amazon", "google": "#Google", "meta": "#Meta",
    "regulation": "#Regulation", "hack": "#CryptoSecurity", "lawsuit": "#Lawsuit",
    "record": "#RecordHigh", "rally": "#MarketRally", "crash": "#MarketCrash",
}


def _extract_keyword_hashtags(text: str, *, max_tags: int = 3) -> list[str]:
    lowered = text.lower()
    found: list[str] = []
    for keyword, tag in _KEYWORD_HASHTAGS.items():
        if keyword in lowered and tag not in found:
            found.append(tag)
        if len(found) >= max_tags:
            break
    return found


def _clean_headline(title: str) -> str:
    """Google News titles carry a trailing " - source.com" suffix that
    reads as noise once the source is already shown separately below."""
    return re.sub(r"\s+-\s+[\w.]+\.\w{2,}$", "", title).strip()


def _hashtags_for_story(story: dict, *, market: str) -> str:
    """Market's own base hashtags + up to 3 extracted from the headline's
    own topic (see _extract_keyword_hashtags), deduped. Shared by both
    caption builders below -- the ONLY caption text posted alongside a
    generated news-card IMAGE (see _hashtags_only_caption) and the
    headline extraction used to build the card's own hashtag pills."""
    title = _clean_headline(story["title"])
    hashtags = _hashtags_for_market(market)
    extra_tags = _extract_keyword_hashtags(title)
    extra_tags = [t for t in extra_tags if t.lower() not in hashtags.lower()]
    if extra_tags:
        hashtags = f"{hashtags} {' '.join(extra_tags)}"
    return hashtags


def _hashtags_only_caption(story: dict, *, market: str) -> str:
    """The Threads post TEXT for a trending-news post that HAS an image --
    hashtags only. The headline/source/secondary-headlines content already
    lives ON the generated news card itself (see chart_snapshot.
    generate_news_card) -- repeating all of that again as caption text next
    to the picture was pure duplication, real feedback confirmed live."""
    return _hashtags_for_story(story, market=market)


def _format_trending_story_caption(story: dict, *, market: str, headline_override: str | None = None) -> str:
    """Full headline + source + hashtags text -- used ONLY as the text-only
    fallback when no image could be generated/posted at all (see
    post_trending_news). In that case the TEXT is the only thing carrying
    the actual news content, so it needs the full story, not just hashtags.
    One story only -- no "also trending" bullets (real feedback: bundling
    several unrelated headlines into one post read as confusing; see
    post_trending_news's own comment for the fuller "one post, one story"
    rationale this shares).

    `headline_override` (see threads_persona.anchor_rewrite_headline) lets
    the caller swap in the anchor-rewritten lead headline instead of the
    plain, cleaned one -- defaults to the plain headline when omitted/None
    (e.g. the rewrite failed) so this still works standalone."""
    label = _short_market_label(market)
    title = headline_override or _clean_headline(story["title"])
    source = story.get("source") or ""
    lines = [f"\U0001F4F0 {label} news: {title}"]
    if source:
        lines.append(f"(via {source})")
    lines.append("")
    lines.append(_hashtags_for_story(story, market=market))
    text = "\n".join(lines)
    if len(text) > _THREADS_POST_MAX_CHARS:
        text = text[: _THREADS_POST_MAX_CHARS - 1] + "…"
    return text


_MUSIC_NEWS_HASHTAGS = "#Music #MusicNews #NewMusic #Entertainment #Trending"


def _format_music_news_caption(story: dict) -> str:
    title = _clean_headline(story["title"])
    source = story.get("source") or ""
    lines = [f"\U0001F3B5 {title}"]
    if source:
        lines.append(f"(via {source})")
    lines.append("")
    lines.append(_MUSIC_NEWS_HASHTAGS)
    text = "\n".join(lines)
    if len(text) > _THREADS_POST_MAX_CHARS:
        text = text[: _THREADS_POST_MAX_CHARS - 1] + "…"
    return text


def _post_music_news_fallback() -> bool:
    """Real fallback for a slow news day (see post_trending_news's own
    empty-feed branch): a genuine music/entertainment headline with a real
    artist photo instead of a bland "nothing notable" filler -- real
    feedback confirmed that filler kept showing up in practice (the
    commentary/reply attempts right above this one both depend on this
    market already having real news history, or on Meta's search access,
    neither of which is guaranteed). Namespaced under its own "music" dedup
    market (see _is_recent_duplicate_story) so it never collides with, or
    gets starved by, any real trading market's own recent-news pool. Only
    posts as an IMAGE (see music_news.get_trending_story's own preference
    for picture-bearing stories) when a real photo came back -- falls back
    to plain text with the same hashtags otherwise, still real content
    either way. Best-effort, never raises."""
    try:
        from data import music_news
        story = music_news.get_trending_story()
        if not story or not story.get("title"):
            logger.info("[threads_post] music-news fallback: no story available from any feed")
            return False
        if _is_recent_duplicate_story("music", story["title"]):
            logger.info("[threads_post] music-news fallback: %r is a recent duplicate, skipping", story["title"])
            return False
        logger.info("[threads_post] music-news fallback: posting %r (image=%s)", story["title"], bool(story.get("image_url")))
        caption = _format_music_news_caption(story)
        image_url = story.get("image_url")
        if image_url:
            try:
                threads_client.create_and_publish_image_post(image_url, caption)
                _record_posted_story("music", story["title"], source=story.get("source"))
                return True
            except Exception as exc:
                logger.warning("[threads_post] failed to post music news as an image, falling back to text: %s", exc)
        threads_client.create_and_publish_post(caption)
        _record_posted_story("music", story["title"], source=story.get("source"))
        return True
    except Exception as exc:
        logger.warning("[threads_post] music-news fallback failed: %s", exc)
        return False


def is_configured() -> bool:
    """True once a real login has actually completed (a token is present)
    -- surfaced to /api/status so the dashboard can show whether this is
    wired up yet without needing to check server logs."""
    return bool(threads_client.get_valid_access_token())


def _format_trade_entry_text(
    *, ticker: str, side: str, entry_price: float, take_profit_price: float,
    stop_loss_price: float, reason: str, dry_run: bool, market: str = "perps",
) -> str:
    tag = "[SIMULATED] " if dry_run else ""
    direction = "SHORT" if side == "short" else "LONG"
    text = (
        f"{tag}{_market_label(market)}: {direction} {ticker} @ {entry_price:.4f}\n"
        f"Take-profit: {take_profit_price:.4f} | Stop-loss: {stop_loss_price:.4f}\n"
        f"Why: {reason}\n"
        f"{_hashtags_for_market(market)}"
    )
    if len(text) > _THREADS_POST_MAX_CHARS:
        text = text[: _THREADS_POST_MAX_CHARS - 1] + "…"
    return text


def post_trade_entry(
    *, ticker: str, side: str, entry_price: float, take_profit_price: float,
    stop_loss_price: float, reason: str, dry_run: bool, market: str = "perps",
) -> bool:
    """Posts one Threads post describing a just-opened position. Returns
    whether it actually posted (False for "not configured" and for any
    real failure alike -- callers should never branch on the failure
    reason, only log it, since this must never affect trading logic)."""
    if not THREADS_POST_ENABLED:
        return False
    text = _format_trade_entry_text(
        ticker=ticker, side=side, entry_price=entry_price, take_profit_price=take_profit_price,
        stop_loss_price=stop_loss_price, reason=reason, dry_run=dry_run, market=market,
    )
    try:
        threads_client.create_and_publish_post(text)
        return True
    except Exception as exc:
        logger.warning("[threads_post] failed to post trade entry for %s: %s", ticker, exc)
        return False


def _format_trade_exit_text(
    *, ticker: str, side: str, entry_price: float, exit_price: float,
    pnl_usd: float, reason: str, dry_run: bool, market: str = "perps",
) -> str:
    tag = "[SIMULATED] " if dry_run else ""
    direction = "SHORT" if side == "short" else "LONG"
    result_word = "WIN" if pnl_usd > 0 else "LOSS" if pnl_usd < 0 else "FLAT"
    text = (
        f"{tag}{_market_label(market)}: CLOSED {direction} {ticker} -- {result_word} {pnl_usd:+.2f}\n"
        f"Entry {entry_price:.4f} -> Exit {exit_price:.4f}\n"
        f"Why: {reason}\n"
        f"{_hashtags_for_market(market)}"
    )
    if len(text) > _THREADS_POST_MAX_CHARS:
        text = text[: _THREADS_POST_MAX_CHARS - 1] + "…"
    return text


def post_trade_exit(
    *, ticker: str, side: str, entry_price: float, exit_price: float,
    pnl_usd: float, reason: str, dry_run: bool, market: str = "perps",
) -> bool:
    """Posts one Threads post describing a just-closed position -- the
    counterpart to post_trade_entry() so followers see the full round trip
    (entry AND exit/result), not just entries. Same best-effort, never-
    raise contract as every other post here."""
    if not THREADS_POST_ENABLED:
        return False
    text = _format_trade_exit_text(
        ticker=ticker, side=side, entry_price=entry_price, exit_price=exit_price,
        pnl_usd=pnl_usd, reason=reason, dry_run=dry_run, market=market,
    )
    try:
        threads_client.create_and_publish_post(text)
        return True
    except Exception as exc:
        logger.warning("[threads_post] failed to post trade exit for %s: %s", ticker, exc)
        return False


def _format_scale_in_text(
    *, ticker: str, side: str, add_price: float, add_count: float, new_count: float,
    new_entry_price: float, reason: str, dry_run: bool, market: str = "perps",
) -> str:
    tag = "[SIMULATED] " if dry_run else ""
    direction = "SHORT" if side == "short" else "LONG"
    text = (
        f"{tag}{_market_label(market)}: ADDED to {direction} {ticker} @ {add_price:.4f} "
        f"(+{add_count:g}, now {new_count:g} @ avg {new_entry_price:.4f})\n"
        f"Why: {reason}\n"
        f"{_hashtags_for_market(market)}"
    )
    if len(text) > _THREADS_POST_MAX_CHARS:
        text = text[: _THREADS_POST_MAX_CHARS - 1] + "…"
    return text


def post_scale_in(
    *, ticker: str, side: str, add_price: float, add_count: float, new_count: float,
    new_entry_price: float, reason: str, dry_run: bool, market: str = "perps",
) -> bool:
    """Posts one Threads post describing an ADD to an already-open,
    already-winning position (see perps_strategy.USE_SCALE_IN). Same best-
    effort, never-raise contract as post_trade_entry()."""
    if not THREADS_POST_ENABLED:
        return False
    text = _format_scale_in_text(
        ticker=ticker, side=side, add_price=add_price, add_count=add_count, new_count=new_count,
        new_entry_price=new_entry_price, reason=reason, dry_run=dry_run, market=market,
    )
    try:
        threads_client.create_and_publish_post(text)
        return True
    except Exception as exc:
        logger.warning("[threads_post] failed to post scale-in for %s: %s", ticker, exc)
        return False


def _format_partial_exit_text(
    *, ticker: str, side: str, exit_price: float, closed_count: float, remaining_count: float | None,
    pnl_usd: float, reason: str, dry_run: bool, market: str = "perps",
) -> str:
    tag = "[SIMULATED] " if dry_run else ""
    direction = "SHORT" if side == "short" else "LONG"
    remaining_note = f", {remaining_count:g} still riding" if remaining_count else ""
    text = (
        f"{tag}{_market_label(market)}: Partial profit on {direction} {ticker} -- "
        f"closed {closed_count:g} @ {exit_price:.4f} for {pnl_usd:+.2f}{remaining_note} (stop now locked in)\n"
        f"Why: {reason}\n"
        f"{_hashtags_for_market(market)}"
    )
    if len(text) > _THREADS_POST_MAX_CHARS:
        text = text[: _THREADS_POST_MAX_CHARS - 1] + "…"
    return text


def post_partial_exit(
    *, ticker: str, side: str, exit_price: float, closed_count: float, remaining_count: float | None,
    pnl_usd: float, reason: str, dry_run: bool, market: str = "perps",
) -> bool:
    """Posts one Threads post describing a PARTIAL close -- some of the
    position banked as real profit, the rest still open with a tightened,
    locked-in-profit stop (see perps_strategy.USE_PARTIAL_EXIT). Deliberately
    NOT post_trade_exit() -- that would misleadingly read as the whole
    position having closed. Same best-effort, never-raise contract."""
    if not THREADS_POST_ENABLED:
        return False
    text = _format_partial_exit_text(
        ticker=ticker, side=side, exit_price=exit_price, closed_count=closed_count,
        remaining_count=remaining_count, pnl_usd=pnl_usd, reason=reason, dry_run=dry_run, market=market,
    )
    try:
        threads_client.create_and_publish_post(text)
        return True
    except Exception as exc:
        logger.warning("[threads_post] failed to post partial exit for %s: %s", ticker, exc)
        return False


def post_restart_notice(message: str = "Money Bot has restarted!") -> bool:
    """Posts a short note once per process boot -- see app_kalshi.py's
    `_ensure_background_jobs_started` for the once-per-boot call site. Same
    best-effort, never-raise contract as post_trade_entry()."""
    if not THREADS_POST_ENABLED:
        return False
    try:
        threads_client.create_and_publish_post(message[:_THREADS_POST_MAX_CHARS])
        return True
    except Exception as exc:
        logger.warning("[threads_post] failed to post restart notice: %s", exc)
        return False


def post_trade_analysis_summary(summary_text: str, *, market: str = "perps") -> bool:
    """Posts the periodic post-trade analysis digest (see
    perps_trade_analysis.format_analysis_summary_text) -- what the
    account's own real trade history shows: win rate by exit reason/
    confidence level, and any evidence-gated confidence-threshold
    adjustment that got applied off it. Same best-effort, never-raise
    contract as every other post here."""
    if not THREADS_POST_ENABLED:
        return False
    text = summary_text
    hashtags = _hashtags_for_market(market)
    if hashtags and hashtags not in text:
        text = f"{text}\n{hashtags}"
    if len(text) > _THREADS_POST_MAX_CHARS:
        text = text[: _THREADS_POST_MAX_CHARS - 1] + "…"
    try:
        threads_client.create_and_publish_post(text)
        return True
    except Exception as exc:
        logger.warning("[threads_post] failed to post trade analysis summary: %s", exc)
        return False


def _format_held_duration(minutes: float | None) -> str:
    """"42min" under an hour, "17h3m" (or "17h" on the dot) beyond it --
    the old flat `.0f`min` formatting read fine at the old ~3-hour max_hold
    ceiling but turns into an unreadable "1020min" now that
    USE_TREND_TRAILING_STRATEGY can hold a position most of a day."""
    if minutes is None:
        return "?"
    total_minutes = int(minutes)
    if total_minutes < 60:
        return f"{total_minutes}min"
    hours, mins = divmod(total_minutes, 60)
    return f"{hours}h{mins}m" if mins else f"{hours}h"


def _format_hourly_status_text(
    *, positions: list[dict], today_realized_pnl_usd: float | None, market: str = "perps",
) -> str:
    """Informal, visual snapshot with a real follow-growth hook -- the old
    version was a flat, mechanical status dump with no reason for a reader
    to stick around or follow. Every exact substring the existing test
    suite already checks for ("N open position(s)", "LONG"/"SHORT
    <TICKER>", "held 42min", "Today's P&L: +0.00") is preserved verbatim,
    just wrapped in a friendlier format around it."""
    if not positions:
        lines = ["\U0001F634 Flat right now -- scanning for the next move..."]
    else:
        count = len(positions)
        lines = [f"\U0001F4CA {count} open position{'s' if count != 1 else ''}"]
        for p in positions:
            is_short = p.get("side") == "short"
            direction = "SHORT" if is_short else "LONG"
            direction_emoji = "\U0001F4C9" if is_short else "\U0001F4C8"
            held_str = _format_held_duration(p.get("held_minutes"))
            entry_price = p.get("entry_price", 0.0)
            take_profit_price = p.get("take_profit_price", entry_price)
            stop_loss_price = p.get("stop_loss_price", entry_price)
            lines.append(f"{direction_emoji} {direction} {p.get('ticker', '?')} @ {entry_price:.4f} (held {held_str})")
            lines.append(f"\U0001F3AF TP {take_profit_price:.4f}  \U0001F6D1 SL {stop_loss_price:.4f}")
    if today_realized_pnl_usd is not None:
        pnl_emoji = "\U0001F7E2" if today_realized_pnl_usd >= 0 else "\U0001F534"
        lines.append(f"{pnl_emoji} Today's P&L: {today_realized_pnl_usd:+.2f}")
    lines.append("")
    lines.append("\U0001F916 Fully automated, runs 24/7 -- follow for real-time trade alerts \U0001F514")
    lines.append(f"{_hashtags_for_market(market)} #AlgoTrading #FollowForMore")
    text = "\n".join(lines)
    if len(text) > _THREADS_POST_MAX_CHARS:
        text = text[: _THREADS_POST_MAX_CHARS - 1] + "…"
    return text


def post_hourly_status(
    *, positions: list[dict], today_realized_pnl_usd: float | None = None, market: str = "perps",
) -> bool:
    """Posts a status update every hour regardless of whether a trade
    happened -- what position(s) the bot is currently holding (or that
    it's flat), plus today's realized P&L. Same best-effort, never-raise
    contract as the other posts here."""
    if not THREADS_POST_ENABLED:
        return False
    text = _format_hourly_status_text(positions=positions, today_realized_pnl_usd=today_realized_pnl_usd, market=market)
    try:
        threads_client.create_and_publish_post(text)
        return True
    except Exception as exc:
        logger.warning("[threads_post] failed to post hourly status: %s", exc)
        return False


def post_trending_news(story: dict | None, *, market: str) -> bool:
    """Posts the current trending story's LEAD headline -- one post, one
    story, always -- as a GENERATED image card -- the headline, source,
    and hashtags are rendered directly onto the picture itself (see
    chart_snapshot.generate_news_card), not attached as a caption next to a
    scraped photo. The Threads post TEXT next to the image is hashtags ONLY
    (see _hashtags_only_caption) -- real feedback confirmed the old caption
    duplicated everything already visible on the card itself, which read as
    redundant.

    Real feedback: this used to also bundle `story`'s own up-to-3 "also
    trending" secondary headlines into the same post -- a multi-card
    carousel when there was an image, or a bullet list when there wasn't --
    which read as confusing (several unrelated stories under one post).
    `secondary` is still fetched/stored (see _record_posted_story) as
    grounding context for a later commentary post via _last_known_story,
    but never rendered as part of THIS post's own content anymore. Falls
    back, in order: image card (chart_snapshot.generate_news_card) -> plain
    text (see _format_trending_story_caption) -- each step only triggers if
    the one before it failed for any reason, so a real post still beats
    none. `story` comes from crypto_news.get_trending_story() /
    stock_news.get_trending_story() -- {"title", "link", "image_url",
    "source", "secondary"}, or None if every feed failed.

    Real, confirmed bug the image-card approach (any of the 3 fallback
    tiers) replaces: the original version attached the story's own RSS
    image, or one scraped from its article link's og:image tag. For
    Google-News-sourced stories (stocks/options), that scrape returns the
    SAME static Google News branding image for every article regardless of
    headline (confirmed live: 6 different real articles all resolved to
    one identical og:image URL, since Google's interstitial redirect page
    never carries the real publisher's own image) -- exactly the "same
    picture every time" behavior reported live. Generating the card
    instead guarantees every post's image is genuinely distinct (it's
    rendered from that story's own text) and never depends on a
    third-party page's markup staying scrapeable.

    Runs every 30 minutes (see app_kalshi.py's/alpaca_server.py's own
    scheduled job) independent of whether any trade happened. Same
    best-effort, never-raise contract as every other post here."""
    if not THREADS_POST_ENABLED:
        return False
    # Real feedback: the same trending headline was posting again on a later
    # 30-minute cycle whenever a slow news day left it still the top/
    # freshest item -- see this module's own recent-news dedup comment.
    # Checked against the RAW title (before any anchor rewrite below), since
    # dedup must key off the real underlying story, not this cycle's own
    # restyled wording of it. `duplicate_story` keeps the ORIGINAL story
    # around after `story` itself gets nulled out, specifically so the
    # commentary fallback right below still has something real to comment
    # ON, instead of only ever seeing "there's nothing."
    duplicate_story = None
    if story and _is_recent_duplicate_story(market, story["title"]):
        logger.info("[threads_post] skipping trending news for %s -- already posted recently: %s", market, story["title"])
        duplicate_story = story
        story = None
    if not story:
        # Real feedback: a reply-search round (see reply_to_trending_keyword_posts's
        # own module note) is functionally self-only under Meta's current
        # Standard Access grant, so it routinely finds nothing to reply to
        # -- falling all the way through to a bland "nothing notable" post
        # anyway in practice. Genuine commentary/analysis on a story the
        # account already knows about doesn't depend on that crippled
        # search at all, so it's tried FIRST. Subject is the in-window
        # duplicate if there is one, else the last real story this market
        # ever posted (see _last_known_story) -- a genuinely empty feed
        # this cycle still leaves something real and on-topic to say,
        # instead of only ever falling through to filler once there's no
        # in-window duplicate specifically.
        commentary_subject = duplicate_story or _last_known_story(market)
        if commentary_subject and commentary_subject.get("title"):
            try:
                from data import threads_persona
                subject_title = _clean_headline(commentary_subject["title"])
                subject_secondary = [_clean_headline(s) for s in (commentary_subject.get("secondary") or []) if s]
                commentary = threads_persona.anchor_commentary(
                    subject_title, source=commentary_subject.get("source"), secondary=subject_secondary,
                )
                if commentary:
                    hashtags = _hashtags_for_story(commentary_subject, market=market)
                    # Real feedback: this fallback path was always plain
                    # text, unlike a fresh headline (always a generated
                    # card) -- turn commentary into a real picture too,
                    # same generate_news_card this module already trusts
                    # for headlines, reusing `commentary` as its "headline"
                    # slot. Falls back to a hashtags-only-caption image
                    # post, then plain text, same 3-tier discipline
                    # post_trending_news's own fresh-story path already
                    # uses below.
                    image_url = None
                    try:
                        from data import chart_snapshot
                        chart_path = chart_snapshot.generate_news_card(
                            market=market, headline=commentary, source=commentary_subject.get("source") or "",
                            secondary=[], hashtags=hashtags,
                        )
                        if chart_path is not None:
                            image_url = chart_snapshot.public_url_for(chart_path)
                    except Exception as exc:
                        logger.warning("[threads_post] commentary card generation failed, falling back to text: %s", exc)
                    if image_url:
                        try:
                            threads_client.create_and_publish_image_post(image_url, _hashtags_only_caption(commentary_subject, market=market))
                            return True
                        except Exception as exc:
                            logger.warning("[threads_post] failed to post commentary as an image, falling back to text: %s", exc)
                    text = f"{commentary}\n\n{hashtags}"
                    if len(text) > _THREADS_POST_MAX_CHARS:
                        text = text[: _THREADS_POST_MAX_CHARS - 1] + "…"
                    threads_client.create_and_publish_post(text)
                    return True
            except Exception as exc:
                logger.warning("[threads_post] commentary fallback failed: %s", exc)
        # Real engagement instead of filler: reply to trending conversation
        # in this market -- see _REPLY_KEYWORDS_BY_MARKET's own comment.
        # Reached only when there was truly nothing to comment on (this
        # market has never posted a real story at all) or the commentary
        # attempt itself failed end to end.
        try:
            reply_result = reply_to_trending_keyword_posts(_reply_keyword_for_market(market), market=market)
            if reply_result.get("replied"):
                return True
        except Exception as exc:
            logger.warning("[threads_post] fallback reply round failed: %s", exc)
        # Last real-content resort before the bland filler below: a genuine
        # music/entertainment headline with a real artist photo -- see
        # _post_music_news_fallback's own docstring for why this beats
        # admitting "nothing notable" outright.
        if _post_music_news_fallback():
            return True
        text = f"{_short_market_label(market)} trending news: nothing notable right now.\n{_hashtags_for_market(market)} #Trends #News"
        try:
            threads_client.create_and_publish_post(text)
            return True
        except Exception as exc:
            logger.warning("[threads_post] failed to post trending news (no story): %s", exc)
            return False

    raw_title = story["title"]
    title = _clean_headline(raw_title)
    secondary = [_clean_headline(s) for s in (story.get("secondary") or []) if s]
    # "News anchor" persona rewrite -- see threads_persona.py's own
    # docstring. Only the LEAD headline (one LLM call per cycle, not one
    # per headline); falls back to the plain, cleaned headline unchanged on
    # any failure (no API key configured, network error, empty completion)
    # -- this must never block a real post.
    try:
        from data import threads_persona
        anchor_title = threads_persona.anchor_rewrite_headline(title, source=story.get("source"), secondary=secondary)
        if anchor_title:
            title = anchor_title
    except Exception as exc:
        logger.warning("[threads_post] anchor rewrite failed, using the plain headline: %s", exc)
    hashtags = _hashtags_for_story(story, market=market)

    # Real feedback: bundling the lead headline plus up to 3 unrelated
    # "also trending" headlines into one post (a multi-card carousel, or a
    # bullet list in the text fallback below) read as confusing -- several
    # different stories under one caption/post. One post is now always
    # exactly one story: the lead headline only. `secondary` is still
    # fetched and stored (see _record_posted_story) since _last_known_story
    # uses it as extra grounding for a later commentary post, just never
    # rendered as part of THIS post's own content.
    image_url = None
    try:
        from data import chart_snapshot

        chart_path = chart_snapshot.generate_news_card(
            market=market, headline=title, source=story.get("source") or "", secondary=[], hashtags=hashtags,
        )
        if chart_path is not None:
            image_url = chart_snapshot.public_url_for(chart_path)
    except Exception as exc:
        logger.warning("[threads_post] news card generation failed, falling back to text: %s", exc)

    if image_url:
        try:
            # Hashtags only -- the headline/source/secondary-headlines
            # content already lives ON the card itself (see
            # _hashtags_only_caption's own docstring for why this must not
            # repeat what the picture already shows).
            threads_client.create_and_publish_image_post(image_url, _hashtags_only_caption(story, market=market))
            _record_posted_story(market, raw_title, source=story.get("source"), secondary=story.get("secondary"))
            return True
        except Exception as exc:
            logger.warning("[threads_post] failed to post trending news as an image, falling back to text: %s", exc)
    try:
        # No image at all -- the text is the only thing carrying the real
        # story now, so it needs the full headline/source/secondary, not
        # just hashtags. headline_override carries through the anchor
        # rewrite (or the plain headline, if that failed) rather than
        # re-deriving the plain one from scratch.
        threads_client.create_and_publish_post(_format_trending_story_caption(story, market=market, headline_override=title))
        _record_posted_story(market, raw_title, source=story.get("source"), secondary=story.get("secondary"))
        return True
    except Exception as exc:
        logger.warning("[threads_post] failed to post trending news: %s", exc)
        return False


def post_trade_entry_chart(
    *, ticker: str, market: str, candles: list[dict], entry_price: float | None = None,
    take_profit_price: float | None = None, stop_loss_price: float | None = None,
    entry_index: int | None = None, side: str = "long", dry_run: bool, subtitle: str | None = None,
    indicators: dict | None = None,
) -> bool:
    """Posts a candlestick-chart image of a just-opened trade -- real OHLC
    price action plus the entry/take-profit/stop-loss levels, so followers
    see the actual "whole idea" of the trade, not just a text line. Always
    attempted now (every trade, not a random subset -- see this module's
    git history for the old CHART_SNAPSHOT_PROBABILITY gate this replaced).
    Skips (returns False, not an error) whenever: disabled, not enough
    candle history to chart, Pillow/rendering fails, or this service
    doesn't know its own public URL (RENDER_EXTERNAL_URL unset -- e.g.
    local dev, where Threads' servers could never reach the image anyway).
    Same best-effort, never-raise contract as every other post here -- a
    chart is a nice-to-have, never allowed to affect trading."""
    if not THREADS_POST_ENABLED:
        return False
    try:
        from data import chart_snapshot
        chart_path = chart_snapshot.generate_candlestick_chart(
            ticker=ticker, market=market, candles=candles, side=side,
            entry_price=entry_price, take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price, entry_index=entry_index, subtitle=subtitle,
            indicators=indicators,
        )
        if chart_path is None:
            return False
        image_url = chart_snapshot.public_url_for(chart_path)
        if image_url is None:
            return False

        tag = "[SIMULATED] " if dry_run else ""
        direction = "SHORT" if side == "short" else "LONG"
        entry_line = f" @ {entry_price:.4f}" if entry_price is not None else ""
        caption = (
            f"{tag}{_market_label(market)}: {direction} {ticker}{entry_line}\n"
            f"{_hashtags_for_market(market)}"
        )
        threads_client.create_and_publish_image_post(image_url, caption)
        return True
    except Exception as exc:
        logger.warning("[threads_post] failed to post trade entry chart for %s: %s", ticker, exc)
        return False


def post_trade_exit_chart(
    *, ticker: str, market: str, candles: list[dict], entry_price: float | None = None,
    exit_price: float | None = None, take_profit_price: float | None = None,
    stop_loss_price: float | None = None, entry_index: int | None = None, exit_index: int | None = None,
    side: str = "long", pnl_usd: float, dry_run: bool, subtitle: str | None = None,
    indicators: dict | None = None,
) -> bool:
    """Posts a candlestick-chart image of a just-CLOSED trade -- the same
    "whole idea" snapshot as post_trade_entry_chart, but for the round
    trip: entry through exit, colored/labeled by the real win/loss result.
    Always attempted (see post_trade_entry_chart's own docstring). Same
    best-effort, never-raise contract as every other post here."""
    if not THREADS_POST_ENABLED:
        return False
    try:
        from data import chart_snapshot
        chart_path = chart_snapshot.generate_candlestick_chart(
            ticker=ticker, market=market, candles=candles, side=side,
            entry_price=entry_price, exit_price=exit_price, take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price, entry_index=entry_index, exit_index=exit_index,
            pnl_usd=pnl_usd, subtitle=subtitle, indicators=indicators,
        )
        if chart_path is None:
            return False
        image_url = chart_snapshot.public_url_for(chart_path)
        if image_url is None:
            return False

        tag = "[SIMULATED] " if dry_run else ""
        direction = "SHORT" if side == "short" else "LONG"
        result_word = "WIN" if pnl_usd > 0 else "LOSS" if pnl_usd < 0 else "FLAT"
        caption = (
            f"{tag}{_market_label(market)}: CLOSED {direction} {ticker} -- {result_word} {pnl_usd:+.2f}\n"
            f"{_hashtags_for_market(market)}"
        )
        threads_client.create_and_publish_image_post(image_url, caption)
        return True
    except Exception as exc:
        logger.warning("[threads_post] failed to post trade exit chart for %s: %s", ticker, exc)
        return False


def post_sentiment_snapshot(*, market: str, ticker_sentiments: list[dict]) -> bool:
    """Posts a per-ticker sentiment bar-chart image -- every ticker this
    service actually tracks (its dataset/watchlist), each with its own
    real news sentiment (not a single shared/aggregate score), refreshed
    every time this runs since *_news.get_sentiment() itself re-checks the
    news on its own short TTL. Genuinely different from post_trending_news
    (headlines text) -- this is the per-ticker SCORES, as a picture.
    Same best-effort, never-raise contract as every other post here."""
    if not THREADS_POST_ENABLED:
        return False
    try:
        from data import chart_snapshot
        chart_path = chart_snapshot.generate_sentiment_snapshot(market=market, ticker_sentiments=ticker_sentiments)
        if chart_path is None:
            return False
        image_url = chart_snapshot.public_url_for(chart_path)
        if image_url is None:
            return False

        # #AlgoTrading #FollowForMore added per real feedback: this caption
        # was missing the same follower-growth hashtags the hourly status
        # post already carries (see that job's own comment).
        caption = f"{_market_label(market)}: per-ticker sentiment snapshot\n{_hashtags_for_market(market)} #AlgoTrading #FollowForMore"
        threads_client.create_and_publish_image_post(image_url, caption)
        return True
    except Exception as exc:
        logger.warning("[threads_post] failed to post sentiment snapshot for %s: %s", market, exc)
        return False
