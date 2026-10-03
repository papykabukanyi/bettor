"""News for every bot's setup rule, from Alpaca's market-data news (Benzinga,
real time, tagged with tickers for stocks, ETFs and crypto pairs).

Two feeds, merged: the live news stream (wss .../v1beta1/news, every
article, kept KEEP_HOURS in memory) and the REST endpoint
(/v1beta1/news?symbols=..., cached CACHE_SEC per symbol set) for the hours
before the stream started. Each bot asks for the tickers of what it trades
(news_symbols): stocks and options their own ticker, crypto the pair as
Benzinga tags it (BTCUSD), commodities their ETFs.

sentiment() scores the headlines (and summaries) of the last `hours` with
the same word lists the bots used before, so the rule's threshold
(<= -NEWS_BLOCK blocks a trade against the news) keeps its meaning. No
articles is a neutral 0.0, never a block.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

STREAM_URL = os.getenv("ALPACA_NEWS_STREAM_URL", "wss://stream.data.alpaca.markets/v1beta1/news")
KEEP_HOURS = 48
CACHE_SEC = 120
DEFAULT_HOURS = float(os.getenv("ALPACA_NEWS_HOURS", "6") or "6")

# Commodity -> the tickers its news is tagged with (the ETF the chart reads
# and its closest peer).
COMMODITY_NEWS_SYMBOLS = {
    "GOLD": ["GLD", "IAU"], "SILVER": ["SLV", "SIVR"], "COPPER": ["CPER", "FCX"], "PLATINUM": ["PPLT"],
    "PALLADIUM": ["PALL"], "WTI": ["USO", "XLE"], "NATGAS": ["UNG"], "BRENT": ["BNO"],
}

_lock = threading.Lock()
_articles: dict[int, dict[str, Any]] = {}  # id -> article (stream and REST)
_rest_cache: dict[str, float] = {}  # symbol-set key -> fetched at
_stream_thread: threading.Thread | None = None
_stream_stats: dict[str, Any] = {"connected": False, "connects": 0, "articles": 0, "last_article_at": None, "last_error": None}


def news_symbols(asset: str) -> list[str]:
    """The tickers an asset's news is tagged with."""
    a = str(asset or "").upper().strip()
    if a in COMMODITY_NEWS_SYMBOLS:
        return list(COMMODITY_NEWS_SYMBOLS[a])
    if "/" in a:  # crypto pair, e.g. BTC/USD
        return [a.replace("/", "")]
    from data import kalshi_15m_spot
    coin = kalshi_15m_spot.chart_coin(a)
    if coin in kalshi_15m_spot.SPOT_PRODUCTS:
        return [f"{coin}USD"]
    return [a]


def _parse_ts(value: Any) -> float | None:
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _store(article: dict[str, Any]) -> None:
    try:
        aid = int(article["id"])
    except (KeyError, TypeError, ValueError):
        return
    created = _parse_ts(article.get("created_at")) or time.time()
    row = {"id": aid, "headline": str(article.get("headline") or ""), "summary": str(article.get("summary") or ""),
           "symbols": [str(s).upper() for s in article.get("symbols") or []], "created_at": created,
           "source": article.get("source") or "benzinga", "url": article.get("url")}
    cutoff = time.time() - KEEP_HOURS * 3600
    with _lock:
        _articles[aid] = row
        if len(_articles) > 20000:
            for old in [k for k, v in _articles.items() if v["created_at"] < cutoff]:
                del _articles[old]


def fetch(symbols: list[str], *, hours: float = DEFAULT_HOURS, max_pages: int = 4) -> int:
    """Pull the last `hours` of articles over REST for the tickers not
    fetched within CACHE_SEC (40 tickers per request, up to max_pages of 50
    articles); returns how many came back."""
    from data import alpaca_client
    now = time.time()
    stale = sorted({s.upper() for s in symbols if now - _rest_cache.get(s.upper(), 0.0) >= CACHE_SEC})
    if not stale or not alpaca_client.is_configured():
        return 0
    start = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    count = 0
    for i in range(0, len(stale), 40):
        chunk = stale[i:i + 40]
        for s in chunk:
            _rest_cache[s] = now
        params: dict[str, Any] = {"symbols": ",".join(chunk), "start": start, "limit": 50, "sort": "desc",
                                  "include_content": "false"}
        for _ in range(max_pages):
            data = alpaca_client._data_get("/v1beta1/news", params=params)  # noqa: SLF001
            for article in data.get("news") or []:
                _store(article)
                count += 1
            if not data.get("next_page_token"):
                break
            params["page_token"] = data["next_page_token"]
    return count


def prefetch(assets: list[str], *, hours: float = DEFAULT_HOURS) -> int:
    """One bulk REST pull for every asset a scan is about to read."""
    symbols = sorted({s for a in assets for s in news_symbols(a)})
    try:
        return fetch(symbols, hours=hours)
    except Exception as exc:
        logger.debug("[alpaca_news] prefetch failed: %s", exc)
        return 0


def articles(symbols: list[str], *, hours: float = DEFAULT_HOURS, now: float | None = None) -> list[dict[str, Any]]:
    """Stored articles tagged with any of these tickers in the last `hours`,
    newest first."""
    now = time.time() if now is None else now
    wanted = {s.upper() for s in symbols}
    since = now - hours * 3600
    with _lock:
        rows = [a for a in _articles.values() if a["created_at"] >= since and wanted & set(a["symbols"])]
    return sorted(rows, key=lambda a: a["created_at"], reverse=True)


def sentiment(asset: str, *, hours: float = DEFAULT_HOURS, now: float | None = None) -> dict[str, Any]:
    """{"sentiment_score" in [-1, 1], "headline_volume", "symbols",
    "latest_headline"} for an asset (see news_symbols)."""
    from data.crypto_news import _score_headlines
    symbols = news_symbols(asset)
    try:
        fetch(symbols, hours=hours)
    except Exception as exc:
        logger.debug("[alpaca_news] fetch failed for %s: %s", symbols, exc)
    rows = articles(symbols, hours=hours, now=now)
    score, volume = _score_headlines([f"{a['headline']} {a['summary']}" for a in rows])
    return {"sentiment_score": score if rows else 0.0, "headline_volume": volume, "symbols": symbols,
            "latest_headline": rows[0]["headline"] if rows else None, "source": "Alpaca news (Benzinga)",
            "computed_at": time.time()}


# ---------------------------------------------------------------------------
# Live stream
# ---------------------------------------------------------------------------
def _stream_session() -> None:
    import websocket
    ws = websocket.create_connection(STREAM_URL, timeout=30)
    try:
        def expect(msg: str) -> None:
            for _ in range(5):
                for m in json.loads(ws.recv()):
                    if m.get("T") == "error":
                        raise ConnectionError(f"{m.get('code')}: {m.get('msg')}")
                    if m.get("T") == "success" and m.get("msg") == msg:
                        return
            raise ConnectionError(f"no '{msg}' from the news stream")

        expect("connected")
        ws.send(json.dumps({"action": "auth", "key": os.getenv("ALPACA_API_KEY_ID", ""),
                            "secret": os.getenv("ALPACA_API_SECRET_KEY", "")}))
        expect("authenticated")
        ws.send(json.dumps({"action": "subscribe", "news": ["*"]}))
        _stream_stats.update(connected=True, last_error=None)
        _stream_stats["connects"] += 1
        ws.settimeout(120)
        while True:
            try:
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                ws.ping()
                continue
            if not raw:
                raise ConnectionError("news stream closed by server")
            for m in json.loads(raw):
                if m.get("T") == "n":
                    _store(m)
                    _stream_stats["articles"] += 1
                    _stream_stats["last_article_at"] = time.time()
                elif m.get("T") == "error":
                    _stream_stats["last_error"] = f"{m.get('code')}: {m.get('msg')}"
    finally:
        _stream_stats["connected"] = False
        try:
            ws.close()
        except Exception:
            pass


def _stream_loop() -> None:
    backoff = 1.0
    while True:
        try:
            _stream_session()
            backoff = 1.0
        except Exception as exc:
            _stream_stats["last_error"] = f"{type(exc).__name__}: {exc}"[:300]
            logger.warning("[alpaca_news] stream dropped: %s", exc)
        time.sleep(backoff)
        backoff = min(60.0, backoff * 2)


def start_stream() -> dict[str, Any]:
    """Start the news stream once per process (needs the Alpaca keys)."""
    global _stream_thread
    from data import alpaca_stream
    if not alpaca_stream.enabled():
        return {"ok": False, "reason": "disabled_or_no_keys"}
    if _stream_thread is None or not _stream_thread.is_alive():
        _stream_thread = threading.Thread(target=_stream_loop, name="alpaca-news-stream", daemon=True)
        _stream_thread.start()
    return {"ok": True}


def status() -> dict[str, Any]:
    s = dict(_stream_stats)
    s["last_article_age_sec"] = None if s["last_article_at"] is None else round(time.time() - s["last_article_at"], 1)
    with _lock:
        s["stored_articles"] = len(_articles)
    return s
