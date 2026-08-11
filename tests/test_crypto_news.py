"""Crypto news sentiment sources -- gating (skip silently without a key) and
basic response parsing for the optional API-key-gated sources."""
from __future__ import annotations

import pytest

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


def test_fetch_google_news_rss_enters_cooldown_after_a_503_and_stops_calling(monkeypatch):
    """Confirmed live: 70+ consecutive 503s over 90 minutes on this exact
    endpoint (options), one per symbol per cycle, every one silently caught
    and retried next cycle anyway before this fix -- a cooldown must make
    it stop calling the network at all for a while."""
    monkeypatch.setattr(news, "_google_news_rss_cooldown_until", 0.0)
    calls = {"n": 0}

    class _UnavailableResponse:
        status_code = 503

    def fake_get(*a, **k):
        calls["n"] += 1
        return _UnavailableResponse()

    monkeypatch.setattr(news.requests, "get", fake_get)
    assert news._fetch_google_news_rss("bitcoin") == []  # noqa: SLF001
    assert calls["n"] == 1

    # Immediately after: still in cooldown, must NOT call the network again.
    assert news._fetch_google_news_rss("ethereum") == []  # noqa: SLF001
    assert calls["n"] == 1


def test_fetch_google_news_rss_enters_cooldown_after_a_429_too(monkeypatch):
    monkeypatch.setattr(news, "_google_news_rss_cooldown_until", 0.0)

    class _RateLimitedResponse:
        status_code = 429

    monkeypatch.setattr(news.requests, "get", lambda *a, **k: _RateLimitedResponse())
    assert news._fetch_google_news_rss("bitcoin") == []  # noqa: SLF001
    assert news.time.time() < news._google_news_rss_cooldown_until  # noqa: SLF001


def test_fetch_google_news_rss_calls_again_once_cooldown_expires(monkeypatch):
    monkeypatch.setattr(news, "_google_news_rss_cooldown_until", news.time.time() - 1)  # already expired

    class FakeResponse:
        status_code = 200
        content = b"<rss><channel></channel></rss>"

        def raise_for_status(self):
            pass

    calls = {"n": 0}

    def fake_get(*a, **k):
        calls["n"] += 1
        return FakeResponse()

    monkeypatch.setattr(news.requests, "get", fake_get)
    assert news._fetch_google_news_rss("bitcoin") == []  # noqa: SLF001
    assert calls["n"] == 1


def test_fetch_cryptopanic_skips_silently_without_a_key(monkeypatch):
    monkeypatch.setattr(news, "CRYPTOPANIC_API_KEY", "")

    def fail_if_called(*a, **k):
        raise AssertionError("must not call the network without an API key set")

    monkeypatch.setattr(news.requests, "get", fail_if_called)
    assert news._fetch_cryptopanic("BTC") == []  # noqa: SLF001


def test_fetch_cryptopanic_extracts_titles(monkeypatch):
    monkeypatch.setattr(news, "CRYPTOPANIC_API_KEY", "fake-key")
    monkeypatch.setattr(news, "_cryptopanic_last_call_ts", 0.0)
    monkeypatch.setattr(news, "_cryptopanic_cooldown_until", 0.0)

    class FakeResponse:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {"results": [{"title": "Bitcoin surges to new high"}, {"title": ""}, {"no_title": True}]}

    monkeypatch.setattr(news.requests, "get", lambda *a, **k: FakeResponse())
    titles = news._fetch_cryptopanic("BTC")  # noqa: SLF001
    assert titles == ["Bitcoin surges to new high"]


def test_fetch_cryptopanic_proactively_rations_calls_before_ever_hitting_a_429(monkeypatch):
    """Real gap found in review: every OTHER limited source proactively
    rations itself against its free-tier daily cap; CryptoPanic had no
    protection at all and would fire a real request every single call.
    Back-to-back calls (well inside the computed minimum interval for the
    default 1000/day cap) must skip the network entirely after the first."""
    monkeypatch.setattr(news, "CRYPTOPANIC_API_KEY", "fake-key")
    monkeypatch.setattr(news, "_cryptopanic_last_call_ts", 0.0)
    monkeypatch.setattr(news, "_cryptopanic_cooldown_until", 0.0)

    class FakeResponse:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {"results": [{"title": "headline"}]}

    calls = {"n": 0}

    def fake_get(*a, **k):
        calls["n"] += 1
        return FakeResponse()

    monkeypatch.setattr(news.requests, "get", fake_get)
    assert news._fetch_cryptopanic("BTC") == ["headline"]  # noqa: SLF001
    assert calls["n"] == 1
    # Immediately after, for a DIFFERENT coin -- still inside the shared
    # per-source cooldown window, must not call the network again.
    assert news._fetch_cryptopanic("ETH") == []  # noqa: SLF001
    assert calls["n"] == 1


def test_fetch_cryptopanic_enters_cooldown_after_a_429_and_stops_calling(monkeypatch):
    monkeypatch.setattr(news, "CRYPTOPANIC_API_KEY", "fake-key")
    monkeypatch.setattr(news, "_cryptopanic_last_call_ts", 0.0)
    monkeypatch.setattr(news, "_cryptopanic_cooldown_until", 0.0)

    class _RateLimitedResponse:
        status_code = 429

    calls = {"n": 0}

    def fake_get(*a, **k):
        calls["n"] += 1
        return _RateLimitedResponse()

    monkeypatch.setattr(news.requests, "get", fake_get)
    assert news._fetch_cryptopanic("BTC") == []  # noqa: SLF001
    assert calls["n"] == 1

    # Bypass the proactive per-call cooldown (simulate enough time passing
    # for THAT), but the 429-triggered daily cooldown must still block it.
    monkeypatch.setattr(news, "_cryptopanic_last_call_ts", 0.0)
    assert news._fetch_cryptopanic("ETH") == []  # noqa: SLF001
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


def test_newsdata_cooldown_is_roughly_a_day():
    """Confirmed live: with every active ticker polled every ~10 minutes,
    newsdata.io's free-tier DAILY quota is gone within the first hour or
    two, so every later call that same day also 429s. A short (1-hour)
    cooldown used to just retry-and-fail once an hour for the rest of the
    day; it must now cover roughly a full day instead."""
    assert news._NEWSDATA_COOLDOWN_SEC >= 20 * 3600  # noqa: SLF001


def test_match_headlines_for_coin_matches_distinctive_terms(monkeypatch):
    headlines = [
        "Bitcoin surges past resistance", "Ethereum gas fees drop", "Random unrelated headline",
    ]
    assert news._match_headlines_for_coin(headlines, "BTC") == ["Bitcoin surges past resistance"]  # noqa: SLF001
    assert news._match_headlines_for_coin(headlines, "ETH") == ["Ethereum gas fees drop"]  # noqa: SLF001


def test_match_headlines_for_coin_avoids_false_positives_on_common_words():
    """NEAR/LINK/DOT/SUI are all common English words or substrings -- a bare
    substring match would inject noise (e.g. "near" matching almost any
    headline) into that coin's sentiment score instead of real signal."""
    headlines = [
        "Prices climb near a new high", "Read more: link in bio", "Fed to dot the i's on policy",
        "Analysts suit up for earnings season",
    ]
    assert news._match_headlines_for_coin(headlines, "NEAR") == []  # noqa: SLF001
    assert news._match_headlines_for_coin(headlines, "LINK") == []  # noqa: SLF001
    assert news._match_headlines_for_coin(headlines, "DOT") == []  # noqa: SLF001
    assert news._match_headlines_for_coin(headlines, "SUI") == []  # noqa: SLF001
    assert news._match_headlines_for_coin(["NEAR Protocol launches new upgrade"], "NEAR") == [
        "NEAR Protocol launches new upgrade"
    ]


@pytest.fixture
def _isolated_sentiment_caches(monkeypatch):
    """NOT autouse -- only the get_sentiment() tests below need this; the
    _fetch_rss_titles_cached-specific tests above test that function's real
    caching behavior and must not have it stubbed out from under them."""
    monkeypatch.setattr(news, "_cache", {})
    monkeypatch.setattr(news, "_general_feed_cache", {})
    monkeypatch.setattr(news, "_fetch_google_news_rss", lambda query: [])
    monkeypatch.setattr(news, "_fetch_rss_titles_cached", lambda url, *, source_name, limit=40: [])
    yield


def test_get_sentiment_skips_limited_sources_when_disabled(monkeypatch, _isolated_sentiment_caches):
    def fail_if_called(*a, **k):
        raise AssertionError("must not call a quota-limited source when use_limited_sources=False")

    monkeypatch.setattr(news, "_fetch_cryptopanic", fail_if_called)
    monkeypatch.setattr(news, "_fetch_newsdata_io", fail_if_called)
    result = news.get_sentiment("ZEC", use_limited_sources=False)
    assert result["coin"] == "ZEC"


def test_get_sentiment_uses_limited_sources_by_default(monkeypatch, _isolated_sentiment_caches):
    calls = {"cryptopanic": 0, "newsdata": 0}
    monkeypatch.setattr(news, "_fetch_cryptopanic", lambda symbol: calls.__setitem__("cryptopanic", calls["cryptopanic"] + 1) or [])
    monkeypatch.setattr(news, "_fetch_newsdata_io", lambda symbol: calls.__setitem__("newsdata", calls["newsdata"] + 1) or [])
    news.get_sentiment("BTC")
    assert calls == {"cryptopanic": 1, "newsdata": 1}


def test_get_sentiment_matches_general_feed_headlines_for_any_coin_not_just_btc(monkeypatch, _isolated_sentiment_caches):
    """The three newsroom feeds used to only ever get attached to BTC's
    sentiment -- every other coin got zero coverage from them. They're
    general (cover all of crypto), free, and shared/cached, so every coin
    should get its own matched slice."""
    monkeypatch.setattr(news, "_fetch_cryptopanic", lambda symbol: [])
    monkeypatch.setattr(news, "_fetch_newsdata_io", lambda symbol: [])

    def fake_fetch(url, *, source_name, limit=40):
        # Only one of the three (real) feeds happens to carry the story --
        # distinguishing by source_name mimics that instead of tripling the
        # same headlines across all three shared-feed calls.
        if source_name == "cointelegraph":
            return ["Zcash privacy upgrade ships", "Unrelated headline"]
        return []

    monkeypatch.setattr(news, "_fetch_rss_titles_cached", fake_fetch)
    result = news.get_sentiment("ZEC")
    assert result["headline_volume"] == 1  # only the ZEC-relevant headline matched


def test_get_trending_headlines_combines_the_general_feeds_and_respects_the_limit(monkeypatch):
    def fake_fetch(url, *, source_name, limit=40):
        return {"cointelegraph": ["a", "b"], "cryptoslate": ["c"], "decrypt": ["d", "e"]}.get(source_name, [])

    monkeypatch.setattr(news, "_fetch_rss_titles_cached", fake_fetch)
    result = news.get_trending_headlines(limit=3)
    assert len(result) == 3


def test_get_trending_headlines_drops_empty_titles(monkeypatch):
    monkeypatch.setattr(news, "_fetch_rss_titles_cached", lambda url, *, source_name, limit=40: ["real headline", "", None] if source_name == "cointelegraph" else [])
    result = news.get_trending_headlines()
    assert result == ["real headline"]


def _rich_item(title, source, image_url="https://x.com/i.jpg"):
    return {"title": title, "link": f"https://{source}.example/a", "pub_date": "", "image_url": image_url, "source": source}


def test_get_trending_story_picks_the_cross_outlet_corroborated_story(monkeypatch):
    """Real popularity signal: the same story independently covered by 2+
    outlets is the lead, even if it isn't the very first item fetched."""
    def fake_fetch(url, *, source_name, limit=40):
        return {
            "cointelegraph": [_rich_item("Quiet single-source update", "cointelegraph"), _rich_item("Bitcoin ETF inflows hit record high this week", "cointelegraph")],
            "cryptoslate": [_rich_item("Bitcoin ETF inflows hit a record high this week", "cryptoslate", image_url=None)],
            "decrypt": [_rich_item("Unrelated NFT drop announced today", "decrypt")],
        }.get(source_name, [])

    monkeypatch.setattr(news, "_fetch_rss_items_cached", fake_fetch)
    story = news.get_trending_story()
    assert story is not None
    assert "ETF inflows" in story["title"]
    # Prefers the version WITH an image when both sides of the corroborated pair are candidates.
    assert story["image_url"] == "https://x.com/i.jpg"


def test_get_trending_story_falls_back_to_freshest_item_without_corroboration(monkeypatch):
    def fake_fetch(url, *, source_name, limit=40):
        return {
            "cointelegraph": [_rich_item("First distinct story from cointelegraph", "cointelegraph")],
            "cryptoslate": [_rich_item("Second distinct story from cryptoslate", "cryptoslate")],
            "decrypt": [_rich_item("Third distinct story from decrypt", "decrypt")],
        }.get(source_name, [])

    monkeypatch.setattr(news, "_fetch_rss_items_cached", fake_fetch)
    story = news.get_trending_story()
    assert story is not None
    assert story["title"] == "First distinct story from cointelegraph"


def test_get_trending_story_returns_none_when_every_feed_fails(monkeypatch):
    monkeypatch.setattr(news, "_fetch_rss_items_cached", lambda url, *, source_name, limit=40: [])
    assert news.get_trending_story() is None


def test_get_trending_story_secondary_excludes_the_lead_and_caps_at_three(monkeypatch):
    def fake_fetch(url, *, source_name, limit=40):
        if source_name != "cointelegraph":
            return []
        return [_rich_item(f"Story {i}", "cointelegraph") for i in range(6)]

    monkeypatch.setattr(news, "_fetch_rss_items_cached", fake_fetch)
    story = news.get_trending_story()
    assert story is not None
    assert story["title"] not in story["secondary"]
    assert len(story["secondary"]) == 3
