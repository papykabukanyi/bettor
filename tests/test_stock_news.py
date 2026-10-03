"""stock_news: the same functions every caller uses, now Alpaca news only."""
from __future__ import annotations

import time

import pytest

from data import alpaca_client, alpaca_news, stock_news


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    monkeypatch.setattr(alpaca_news, "_articles", {})
    monkeypatch.setattr(alpaca_news, "_rest_cache", {})
    monkeypatch.setattr(stock_news, "_cache", {})
    monkeypatch.setattr(alpaca_client, "is_configured", lambda: False)


def _store(aid, headline, symbols, minutes_ago=5):
    created = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - minutes_ago * 60))
    alpaca_news._store({"id": aid, "headline": headline, "summary": "", "symbols": symbols, "created_at": created})  # noqa: SLF001


def test_symbol_sentiment_reads_alpaca_news_for_the_ticker():
    _store(1, "Apple misses estimates, shares drop", ["AAPL"])
    s = stock_news.get_sentiment("aapl", company_name="Apple Inc.")
    assert s["symbol"] == "AAPL" and s["sentiment_score"] < 0 and s["headline_volume"] == 1
    assert stock_news.get_sentiment("MSFT")["sentiment_score"] == 0.0


def test_prewarm_is_one_bulk_alpaca_pull(monkeypatch):
    seen = []
    monkeypatch.setattr(alpaca_news, "prefetch", lambda assets: seen.append(list(assets)))
    stock_news.prewarm_sentiment([("AAPL", "Apple"), ("NVDA", None)])
    assert seen == [["AAPL", "NVDA"]]


def test_trending_story_reads_market_news_or_one_ticker():
    _store(2, "Stocks rally as Nvidia beats", ["NVDA", "SPY"])
    _store(3, "Tesla recall widens", ["TSLA"], minutes_ago=1)
    assert stock_news.get_trending_story()["title"] == "Stocks rally as Nvidia beats"
    assert stock_news.get_trending_story(query="TSLA")["title"] == "Tesla recall widens"
    assert stock_news.get_trending_story(query="stock market")["title"] == "Stocks rally as Nvidia beats"


def test_no_other_news_source_is_left():
    import inspect
    src = inspect.getsource(stock_news).lower()
    for host in ("google", "serpapi", "newsapi", "rss"):
        assert host not in src
