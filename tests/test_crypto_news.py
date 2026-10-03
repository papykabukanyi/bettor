"""crypto_news: the same functions every caller uses, now Alpaca news only."""
from __future__ import annotations

import time

import pytest

from data import alpaca_client, alpaca_news, crypto_news


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    monkeypatch.setattr(alpaca_news, "_articles", {})
    monkeypatch.setattr(alpaca_news, "_rest_cache", {})
    monkeypatch.setattr(crypto_news, "_cache", {})
    monkeypatch.setattr(alpaca_client, "is_configured", lambda: False)


def _store(aid, headline, symbols, minutes_ago=5, image=None):
    created = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - minutes_ago * 60))
    alpaca_news._store({"id": aid, "headline": headline, "summary": "", "symbols": symbols, "created_at": created,  # noqa: SLF001
                        "url": f"https://x/{aid}", "images": [{"size": "large", "url": image}] if image else []})


def test_coin_sentiment_reads_alpaca_news_for_the_pair():
    _store(1, "Bitcoin rally extends on record inflows", ["BTCUSD"])
    s = crypto_news.get_sentiment("BTC")
    assert s["coin"] == "BTC" and s["sentiment_score"] > 0 and s["headline_volume"] == 1
    assert s["source"] == "Alpaca news (Benzinga)"
    assert crypto_news.get_sentiment("ETH")["sentiment_score"] == 0.0


def test_commodity_sentiment_reads_its_etf_news():
    _store(2, "Gold plunges as dollar jumps", ["GLD"])
    s = crypto_news.get_generic_sentiment("GOLD", cache_key="GOLD")
    assert s["headline_volume"] == 1


def test_prewarm_is_one_bulk_alpaca_pull(monkeypatch):
    seen = []
    monkeypatch.setattr(alpaca_news, "prefetch", lambda assets: seen.append(list(assets)))
    crypto_news.prewarm_sentiment(["BTC", "ETH", ""])
    assert seen == [["BTC", "ETH"]]


def test_trending_story_is_alpaca_news_with_an_image_first():
    _store(3, "Ethereum upgrade lands", ["ETHUSD"], minutes_ago=2)
    _store(4, "Bitcoin and Solana lead crypto rally", ["BTCUSD", "SOLUSD"], minutes_ago=30, image="https://img/4.jpg")
    story = crypto_news.get_trending_story()
    assert story["title"] == "Bitcoin and Solana lead crypto rally" and story["image_url"] == "https://img/4.jpg"
    assert story["source"] == "Benzinga via Alpaca" and story["secondary"] == ["Ethereum upgrade lands"]
    assert crypto_news.get_trending_story(exclude=lambda t: "Solana" in t)["title"] == "Ethereum upgrade lands"
    assert crypto_news.get_trending_headlines(limit=1) == ["Ethereum upgrade lands"]


def test_no_other_news_source_is_left():
    import inspect
    src = inspect.getsource(crypto_news)
    for host in ("google", "cryptopanic", "newsdata", "serpapi", "alternative.me", "cointelegraph", "rss"):
        assert host not in src.lower()
