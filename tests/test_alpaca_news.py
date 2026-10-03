"""Alpaca news (Benzinga) behind every bot's news rule."""
from __future__ import annotations

import time

import pytest

from data import alpaca_client, alpaca_news as n


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    monkeypatch.setattr(n, "_articles", {})
    monkeypatch.setattr(n, "_rest_cache", {})


def _article(aid, headline, symbols, minutes_ago=10):
    created = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - minutes_ago * 60))
    return {"id": aid, "headline": headline, "summary": "", "symbols": symbols, "created_at": created}


def test_each_asset_reads_the_tickers_its_news_is_tagged_with():
    assert n.news_symbols("AAPL") == ["AAPL"]
    assert n.news_symbols("BTC/USD") == ["BTCUSD"]
    assert n.news_symbols("BTC") == ["BTCUSD"] and n.news_symbols("KSHIB") == ["SHIBUSD"]
    assert n.news_symbols("GOLD") == ["GLD", "IAU"] and n.news_symbols("WTI")[0] == "USO"


def test_sentiment_scores_recent_tagged_headlines(monkeypatch):
    monkeypatch.setattr(alpaca_client, "is_configured", lambda: False)
    n._store(_article(1, "Bitcoin surges to record high on strong inflows", ["BTCUSD"]))  # noqa: SLF001
    n._store(_article(2, "Apple plunges after weak guidance", ["AAPL"]))  # noqa: SLF001
    n._store(_article(3, "Bitcoin crash fears", ["BTCUSD"], minutes_ago=60 * 30))  # noqa: SLF001 (too old)
    btc = n.sentiment("BTC")
    assert btc["sentiment_score"] > 0 and btc["headline_volume"] == 1 and btc["latest_headline"].startswith("Bitcoin surges")
    assert n.sentiment("AAPL")["sentiment_score"] < 0
    quiet = n.sentiment("ETH")
    assert quiet["sentiment_score"] == 0.0 and quiet["headline_volume"] == 0  # no news is neutral, never a block


def test_one_bulk_rest_pull_covers_a_whole_scan(monkeypatch):
    calls = []
    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)

    def fake_get(path, *, params):
        calls.append((path, params["symbols"]))
        return {"news": [_article(9, "Gold rallies", ["GLD"])], "next_page_token": None}

    monkeypatch.setattr(alpaca_client, "_data_get", fake_get)
    n.prefetch(["GOLD", "AAPL", "BTC/USD"])
    assert calls == [("/v1beta1/news", "AAPL,BTCUSD,GLD,IAU")]
    n.sentiment("GOLD")  # every ticker fresh: no second request
    assert len(calls) == 1 and n.articles(["GLD"])[0]["id"] == 9
