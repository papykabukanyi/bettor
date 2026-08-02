"""Stock news sentiment -- the equities counterpart to test_crypto_news.py.
Only one source (Google News RSS, free/unlimited), so this focuses on the
company-name-based query construction (the one genuinely different piece
from crypto_news.py, which uses a small hardcoded per-coin dict instead)
plus the same keyword-scoring and per-symbol caching discipline."""
from __future__ import annotations

import pytest

from data import stock_news as news


def test_clean_company_query_strips_common_stock_suffix():
    query = news._clean_company_query("AAPL", "Apple Inc. Common Stock")  # noqa: SLF001
    assert "common stock" not in query.lower()
    assert "apple" in query.lower()
    assert query.endswith("stock")


def test_clean_company_query_strips_corp_and_class_suffixes():
    query = news._clean_company_query("GOOGL", "Alphabet Inc. Class A Common Stock")  # noqa: SLF001
    assert "class a" not in query.lower()
    assert "alphabet" in query.lower()


def test_clean_company_query_falls_back_to_ticker_without_a_company_name():
    query = news._clean_company_query("AAPL", None)  # noqa: SLF001
    assert query == "AAPL stock"


def test_clean_company_query_falls_back_to_ticker_when_name_is_only_boilerplate():
    query = news._clean_company_query("XYZ", "Inc. Common Stock")  # noqa: SLF001
    assert query == "XYZ stock"


def test_score_headlines_positive_and_negative_words():
    score, volume = news._score_headlines(["Stock surges to record high after earnings beat"])  # noqa: SLF001
    assert score > 0
    assert volume == 1


def test_score_headlines_negative():
    score, volume = news._score_headlines(["Shares plunge after guidance cut and lawsuit filed"])  # noqa: SLF001
    assert score < 0


def test_score_headlines_neutral_when_no_scored_words_match():
    score, volume = news._score_headlines(["Company announces quarterly meeting date"])  # noqa: SLF001
    assert score == 0.0
    assert volume == 1


def test_score_headlines_empty_list():
    score, volume = news._score_headlines([])  # noqa: SLF001
    assert score == 0.0
    assert volume == 0


def test_fetch_google_news_rss_returns_empty_on_failure(monkeypatch):
    def fail(*a, **k):
        raise RuntimeError("network down")

    monkeypatch.setattr(news.requests, "get", fail)
    assert news._fetch_google_news_rss("Apple stock") == []  # noqa: SLF001


@pytest.fixture
def _isolated_sentiment_cache(monkeypatch):
    monkeypatch.setattr(news, "_cache", {})
    yield


def test_get_sentiment_uses_the_company_name_in_its_query(monkeypatch, _isolated_sentiment_cache):
    captured = {}

    def fake_fetch(query):
        captured["query"] = query
        return ["Apple surges on strong iPhone sales"]

    monkeypatch.setattr(news, "_fetch_google_news_rss", fake_fetch)
    result = news.get_sentiment("AAPL", company_name="Apple Inc. Common Stock")
    assert "apple" in captured["query"].lower()
    assert result["symbol"] == "AAPL"
    assert result["sentiment_score"] > 0


def test_get_sentiment_falls_back_to_ticker_without_a_company_name(monkeypatch, _isolated_sentiment_cache):
    captured = {}

    def fake_fetch(query):
        captured["query"] = query
        return []

    monkeypatch.setattr(news, "_fetch_google_news_rss", fake_fetch)
    news.get_sentiment("MSFT")
    assert captured["query"] == "MSFT stock"


def test_get_sentiment_is_cached_within_the_ttl(monkeypatch, _isolated_sentiment_cache):
    calls = {"n": 0}

    def fake_fetch(query):
        calls["n"] += 1
        return ["some headline"]

    monkeypatch.setattr(news, "_fetch_google_news_rss", fake_fetch)
    first = news.get_sentiment("AAPL", company_name="Apple Inc.")
    second = news.get_sentiment("AAPL", company_name="Apple Inc.")
    assert first == second
    assert calls["n"] == 1
