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
        status_code = 200

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


def test_fetch_newsdata_io_enters_cooldown_after_a_429_and_stops_calling(monkeypatch):
    """Confirmed live: with 16 tickers polled every ~10 minutes, the free
    quota gets exhausted fast and every subsequent call for the rest of the
    day also 429s. A cooldown must make it stop calling the network at all
    for a while, instead of retrying (and logging a failure) every cycle."""
    monkeypatch.setattr(news, "NEWSDATA_API_KEY", "fake-key")
    monkeypatch.setattr(news, "_newsdata_cooldown_until", 0.0)
    calls = {"n": 0}

    class _RateLimitedResponse:
        status_code = 429

    def fake_get(*a, **k):
        calls["n"] += 1
        return _RateLimitedResponse()

    monkeypatch.setattr(news.requests, "get", fake_get)
    assert news._fetch_newsdata_io("BTC") == []  # noqa: SLF001
    assert calls["n"] == 1

    # Immediately after: still in cooldown, must NOT call the network again.
    assert news._fetch_newsdata_io("ETH") == []  # noqa: SLF001
    assert calls["n"] == 1


def test_fetch_newsdata_io_calls_again_once_cooldown_expires(monkeypatch):
    monkeypatch.setattr(news, "NEWSDATA_API_KEY", "fake-key")
    monkeypatch.setattr(news, "_newsdata_cooldown_until", news.time.time() - 1)  # already expired

    class FakeResponse:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {"results": [{"title": "some headline"}]}

    calls = {"n": 0}

    def fake_get(*a, **k):
        calls["n"] += 1
        return FakeResponse()

    monkeypatch.setattr(news.requests, "get", fake_get)
    assert news._fetch_newsdata_io("BTC") == ["some headline"]  # noqa: SLF001
    assert calls["n"] == 1


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
