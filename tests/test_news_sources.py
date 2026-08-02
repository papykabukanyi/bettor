"""newsapi.org / TheNewsAPI / World News API -- gating (skip without a key),
response parsing, and the per-source daily-quota cooldown that keeps each
free tier from being burned through in the first hour."""
from __future__ import annotations

import pytest

from data import news_sources as news


@pytest.fixture(autouse=True)
def _isolated_news_sources_state(monkeypatch):
    monkeypatch.setattr(news, "NEWSAPI_ORG_KEY", "fake-key")
    monkeypatch.setattr(news, "THENEWSAPI_TOKEN", "fake-token")
    monkeypatch.setattr(news, "WORLDNEWSAPI_KEY", "fake-key")
    monkeypatch.setattr(news, "_last_call_ts", {})
    yield


class _FakeResponse:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self._body


def test_fetch_newsapi_org_skips_silently_without_a_key(monkeypatch):
    monkeypatch.setattr(news, "NEWSAPI_ORG_KEY", "")

    def fail_if_called(*a, **k):
        raise AssertionError("must not call the network without an API key set")

    monkeypatch.setattr(news.requests, "get", fail_if_called)
    assert news.fetch_newsapi_org("apple stock") == []


def test_fetch_newsapi_org_extracts_titles(monkeypatch):
    captured = {}

    def fake_get(url, *, params, timeout):
        captured["url"] = url
        captured["params"] = params
        return _FakeResponse({"articles": [{"title": "Apple beats earnings"}, {"title": ""}]})

    monkeypatch.setattr(news.requests, "get", fake_get)
    titles = news.fetch_newsapi_org("apple stock")
    assert titles == ["Apple beats earnings"]
    assert captured["params"]["apiKey"] == "fake-key"
    assert captured["params"]["q"] == "apple stock"


def test_fetch_thenewsapi_extracts_titles(monkeypatch):
    captured = {}

    def fake_get(url, *, params, timeout):
        captured["url"] = url
        captured["params"] = params
        return _FakeResponse({"data": [{"title": "Bitcoin rallies"}]})

    monkeypatch.setattr(news.requests, "get", fake_get)
    titles = news.fetch_thenewsapi("bitcoin")
    assert titles == ["Bitcoin rallies"]
    assert captured["url"] == "https://api.thenewsapi.com/v1/news/all"
    assert captured["params"]["api_token"] == "fake-token"
    assert captured["params"]["search"] == "bitcoin"


def test_fetch_worldnewsapi_uses_the_x_api_key_header(monkeypatch):
    captured = {}

    def fake_get(url, *, params, headers, timeout):
        captured["url"] = url
        captured["params"] = params
        captured["headers"] = headers
        return _FakeResponse({"news": [{"title": "Markets rise on rate cut hopes"}]})

    monkeypatch.setattr(news.requests, "get", fake_get)
    titles = news.fetch_worldnewsapi("stock market")
    assert titles == ["Markets rise on rate cut hopes"]
    assert captured["url"] == "https://api.worldnewsapi.com/search-news"
    assert captured["headers"]["x-api-key"] == "fake-key"
    assert captured["params"]["text"] == "stock market"


def test_each_source_returns_empty_on_a_network_failure(monkeypatch):
    def fail(*a, **k):
        raise RuntimeError("network down")

    monkeypatch.setattr(news.requests, "get", fail)
    assert news.fetch_newsapi_org("q") == []
    assert news.fetch_thenewsapi("q") == []
    assert news.fetch_worldnewsapi("q") == []


def test_cooldown_ok_blocks_a_second_call_within_the_window():
    assert news._cooldown_ok("test_source", calls_per_day=100) is True  # noqa: SLF001
    assert news._cooldown_ok("test_source", calls_per_day=100) is False  # noqa: SLF001


def test_cooldown_ok_allows_a_call_once_the_interval_has_passed(monkeypatch):
    monkeypatch.setattr(news, "_last_call_ts", {"test_source": news.time.time() - 999999})
    assert news._cooldown_ok("test_source", calls_per_day=100) is True  # noqa: SLF001


def test_cooldown_is_independent_per_source():
    """Unlike serpapi_client.py's single shared clock, these three sources
    each have their own account/key/quota -- one being on cooldown must not
    block the others."""
    assert news._cooldown_ok("newsapi_org", calls_per_day=100) is True  # noqa: SLF001
    assert news._cooldown_ok("newsapi_org", calls_per_day=100) is False  # noqa: SLF001
    assert news._cooldown_ok("thenewsapi", calls_per_day=100) is True  # noqa: SLF001
    assert news._cooldown_ok("worldnewsapi", calls_per_day=50) is True  # noqa: SLF001


def test_min_interval_reflects_the_daily_cap_with_a_safety_margin():
    # 50/day at an 80% safety margin -> 40 effective/day -> 2160s apart.
    interval = news._min_interval_sec(50)  # noqa: SLF001
    assert 2000 < interval < 2300


def test_fetch_all_combines_every_source(monkeypatch):
    monkeypatch.setattr(news, "fetch_newsapi_org", lambda q: ["from newsapi"])
    monkeypatch.setattr(news, "fetch_thenewsapi", lambda q: ["from thenewsapi"])
    monkeypatch.setattr(news, "fetch_worldnewsapi", lambda q: ["from worldnewsapi"])
    assert news.fetch_all("apple stock") == ["from newsapi", "from thenewsapi", "from worldnewsapi"]
