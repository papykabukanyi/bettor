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


@pytest.fixture
def _isolated_trending_cache(monkeypatch):
    monkeypatch.setattr(news, "_trending_cache", None)
    yield


def test_get_trending_headlines_queries_the_general_market(monkeypatch, _isolated_trending_cache):
    captured = {}

    def fake_fetch(query):
        captured["query"] = query
        return ["Market rallies broadly", "Fed signals rate cuts", "Tech leads gains", "extra", "another", "one more"]

    monkeypatch.setattr(news, "_fetch_google_news_rss", fake_fetch)
    headlines = news.get_trending_headlines(limit=3)
    assert captured["query"] == news._TRENDING_QUERY  # noqa: SLF001
    assert len(headlines) == 3


def test_get_trending_headlines_is_cached_within_the_ttl(monkeypatch, _isolated_trending_cache):
    calls = {"n": 0}

    def fake_fetch(query):
        calls["n"] += 1
        return ["headline"]

    monkeypatch.setattr(news, "_fetch_google_news_rss", fake_fetch)
    news.get_trending_headlines()
    news.get_trending_headlines()
    assert calls["n"] == 1


def test_get_trending_headlines_does_not_cache_a_transient_empty_failure(monkeypatch, _isolated_trending_cache):
    monkeypatch.setattr(news, "_fetch_google_news_rss", lambda query: [])
    result = news.get_trending_headlines()
    assert result == []
    assert news._trending_cache is None  # noqa: SLF001


@pytest.fixture
def _isolated_trending_story_cache(monkeypatch):
    monkeypatch.setattr(news, "_trending_story_cache", None)
    yield


def test_get_trending_story_uses_the_top_result_as_the_lead(monkeypatch, _isolated_trending_story_cache):
    """Google News' own relevance ranking already puts its best-covered
    story first for a broad query -- the top result IS the popularity
    signal here, unlike crypto's own cross-outlet corroboration."""
    items = [
        {"title": "S&P 500 hits record high - cnbc.com", "link": "https://news.google.com/a", "source": "cnbc.com"},
        {"title": "Bond yields tick up - ft.com", "link": "https://news.google.com/b", "source": "ft.com"},
        {"title": "Tech earnings beat expectations - barrons.com", "link": "https://news.google.com/c", "source": "barrons.com"},
    ]
    monkeypatch.setattr(news, "_fetch_google_news_rss_items", lambda query, limit=10: items)
    story = news.get_trending_story()
    assert story is not None
    assert story["title"] == "S&P 500 hits record high - cnbc.com"
    assert story["source"] == "cnbc.com"
    assert story["image_url"] is None
    assert story["secondary"] == ["Bond yields tick up - ft.com", "Tech earnings beat expectations - barrons.com"]


def test_get_trending_story_returns_none_when_the_feed_fails(monkeypatch, _isolated_trending_story_cache):
    monkeypatch.setattr(news, "_fetch_google_news_rss_items", lambda query, limit=10: [])
    assert news.get_trending_story() is None


def test_get_trending_story_is_cached_within_the_ttl(monkeypatch, _isolated_trending_story_cache):
    calls = {"n": 0}

    def fake_fetch(query, limit=10):
        calls["n"] += 1
        return [{"title": "headline", "link": "https://news.google.com/a", "source": "cnbc.com"}]

    monkeypatch.setattr(news, "_fetch_google_news_rss_items", fake_fetch)
    news.get_trending_story()
    news.get_trending_story()
    assert calls["n"] == 1
