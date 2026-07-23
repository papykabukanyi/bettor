"""Crypto news sentiment sources -- gating (skip silently without a key) and
basic response parsing for the optional API-key-gated sources."""
from __future__ import annotations

from data import crypto_news as news


def test_fetch_newsdata_io_skips_silently_without_a_key(monkeypatch):
    monkeypatch.setattr(news, "NEWSDATA_API_KEY", "")

    def fail_if_called(*a, **k):
        raise AssertionError("must not call the network without an API key set")

    monkeypatch.setattr(news.requests, "get", fail_if_called)
    assert news._fetch_newsdata_io("BTC") == []  # noqa: SLF001


def test_fetch_newsdata_io_extracts_titles(monkeypatch):
    monkeypatch.setattr(news, "NEWSDATA_API_KEY", "fake-key")

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"results": [{"title": "Bitcoin surges to new high"}, {"title": ""}, {"no_title": True}]}

    monkeypatch.setattr(news.requests, "get", lambda *a, **k: FakeResponse())
    titles = news._fetch_newsdata_io("BTC")  # noqa: SLF001
    assert titles == ["Bitcoin surges to new high"]


def test_fetch_newsdata_io_returns_empty_on_failure(monkeypatch):
    monkeypatch.setattr(news, "NEWSDATA_API_KEY", "fake-key")

    def raise_error(*a, **k):
        raise RuntimeError("429 rate limited")

    monkeypatch.setattr(news.requests, "get", raise_error)
    assert news._fetch_newsdata_io("BTC") == []  # noqa: SLF001


def test_fetch_rss_titles_cached_reuses_cache_within_ttl(monkeypatch):
    monkeypatch.setattr(news, "_general_feed_cache", {})
    calls = {"n": 0}

    def fake_fetch(url, *, source_name, limit=40):
        calls["n"] += 1
        return ["headline one"]

    monkeypatch.setattr(news, "_fetch_rss_titles", fake_fetch)
    first = news._fetch_rss_titles_cached("https://example.com/feed", source_name="test_source")  # noqa: SLF001
    second = news._fetch_rss_titles_cached("https://example.com/feed", source_name="test_source")  # noqa: SLF001
    assert first == second == ["headline one"]
    assert calls["n"] == 1  # second call served from cache, not a real fetch


def test_fetch_rss_titles_cached_does_not_cache_a_transient_empty_failure(monkeypatch):
    monkeypatch.setattr(news, "_general_feed_cache", {})
    monkeypatch.setattr(news, "_fetch_rss_titles", lambda url, *, source_name, limit=40: [])
    result = news._fetch_rss_titles_cached("https://example.com/feed", source_name="test_source")  # noqa: SLF001
    assert result == []
    assert "test_source" not in news._general_feed_cache
