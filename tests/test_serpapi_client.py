"""SerpApi shared client -- gating (skip without a key), response parsing,
and the cross-caller cooldown that keeps the 250/month free-tier budget
from being exhausted in minutes."""
from __future__ import annotations

import pytest

from data import serpapi_client as serp


@pytest.fixture(autouse=True)
def _isolated_serpapi_state(monkeypatch):
    monkeypatch.setattr(serp, "SERPAPI_API_KEY", "fake-key")
    monkeypatch.setattr(serp, "_last_call_ts", 0.0)
    monkeypatch.setattr(serp, "_last_result_cache", {})
    monkeypatch.setattr(serp, "_calls_this_month", 0)
    monkeypatch.setattr(serp, "_month_key", "")
    yield


def test_search_news_skips_silently_without_a_key(monkeypatch):
    monkeypatch.setattr(serp, "SERPAPI_API_KEY", "")

    def fail_if_called(*a, **k):
        raise AssertionError("must not call the network without an API key set")

    monkeypatch.setattr(serp.requests, "get", fail_if_called)
    assert serp.search_news("apple stock") == []


class _FakeResponse:
    def __init__(self, results):
        self._results = results

    def raise_for_status(self):
        pass

    def json(self):
        return {"news_results": self._results}


def test_search_news_extracts_titles(monkeypatch):
    monkeypatch.setattr(serp.requests, "get", lambda *a, **k: _FakeResponse([
        {"title": "Apple surges on strong earnings"}, {"title": ""}, {"no_title": True},
    ]))
    titles = serp.search_news("apple stock")
    assert titles == ["Apple surges on strong earnings"]


def test_search_news_uses_the_google_news_engine(monkeypatch):
    captured = {}

    def fake_get(url, *, params, timeout):
        captured["url"] = url
        captured["params"] = params
        return _FakeResponse([])

    monkeypatch.setattr(serp.requests, "get", fake_get)
    serp.search_news("bitcoin")
    assert captured["url"] == serp._SEARCH_URL  # noqa: SLF001
    assert captured["params"]["engine"] == "google_news"
    assert captured["params"]["api_key"] == "fake-key"
    assert captured["params"]["q"] == "bitcoin"


def test_search_news_returns_empty_on_failure(monkeypatch):
    def raise_error(*a, **k):
        raise RuntimeError("network down")

    monkeypatch.setattr(serp.requests, "get", raise_error)
    assert serp.search_news("bitcoin") == []


def test_search_news_respects_the_cooldown_across_callers(monkeypatch):
    """The whole point of the shared cooldown: ANY second call within the
    window -- even for a totally different query -- must reuse the cached
    result instead of hitting the network again, since every caller shares
    the SAME 250/month budget."""
    calls = {"n": 0}

    def fake_get(url, *, params, timeout):
        calls["n"] += 1
        return _FakeResponse([{"title": "first query result"}])

    monkeypatch.setattr(serp.requests, "get", fake_get)
    first = serp.search_news("apple stock")
    second = serp.search_news("bitcoin")  # a different query, still within cooldown
    assert first == second == ["first query result"]
    assert calls["n"] == 1


def test_search_news_calls_again_once_cooldown_expires(monkeypatch):
    monkeypatch.setattr(serp, "_last_call_ts", serp.time.time() - serp._SERPAPI_MIN_INTERVAL_SEC - 1)  # noqa: SLF001
    calls = {"n": 0}

    def fake_get(url, *, params, timeout):
        calls["n"] += 1
        return _FakeResponse([{"title": "fresh result"}])

    monkeypatch.setattr(serp.requests, "get", fake_get)
    result = serp.search_news("apple stock")
    assert result == ["fresh result"]
    assert calls["n"] == 1


def test_search_news_never_raises_on_a_malformed_response(monkeypatch):
    class _BadResponse:
        def raise_for_status(self):
            pass

        def json(self):
            raise ValueError("not json")

    monkeypatch.setattr(serp.requests, "get", lambda *a, **k: _BadResponse())
    assert serp.search_news("apple stock") == []


def test_month_counter_resets_across_month_boundaries(monkeypatch):
    monkeypatch.setattr(serp, "_month_key", "2020-01")
    monkeypatch.setattr(serp, "_calls_this_month", 249)
    serp._bump_month_counter()  # noqa: SLF001
    assert serp._calls_this_month == 1  # noqa: SLF001 -- reset for the new month, not accumulated to 250
