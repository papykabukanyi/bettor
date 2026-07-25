"""Schwab OAuth client -- authorization URL construction, token exchange/
refresh, and the durable HF token mirror (Render's disk is ephemeral, so a
restart must not force a fresh interactive login every time). All network
calls mocked; never touches the real Schwab API or HF."""
from __future__ import annotations

import pytest

from data import schwab_client


class _FakeResponse:
    def __init__(self, json_body, status_code=200):
        self._json_body = json_body
        self.status_code = status_code

    def json(self):
        return self._json_body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


@pytest.fixture(autouse=True)
def _isolated_token_state(monkeypatch):
    monkeypatch.setattr(schwab_client, "_token_cache", {})
    monkeypatch.setattr(schwab_client, "CLIENT_ID", "test-client-id")
    monkeypatch.setattr(schwab_client, "CLIENT_SECRET", "test-client-secret")
    monkeypatch.setattr(schwab_client, "REDIRECT_URI", "https://example.com/schcallback")
    monkeypatch.setattr(schwab_client, "HF_API_KEY", "")  # no real network for the HF mirror by default
    yield


def test_get_authorization_url_includes_client_id_and_redirect_uri():
    url = schwab_client.get_authorization_url()
    assert url.startswith(schwab_client.AUTHORIZE_URL)
    assert "client_id=test-client-id" in url
    assert "redirect_uri=" in url


def test_exchange_code_for_tokens_posts_authorization_code_grant(monkeypatch):
    captured = {}

    def fake_post(url, *, headers, data, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["data"] = data
        return _FakeResponse({"access_token": "at-1", "refresh_token": "rt-1", "expires_in": 1800})

    monkeypatch.setattr(schwab_client.requests, "post", fake_post)
    tokens = schwab_client.exchange_code_for_tokens("auth-code-123")

    assert tokens["access_token"] == "at-1"
    assert captured["url"] == schwab_client.TOKEN_URL
    assert captured["data"]["grant_type"] == "authorization_code"
    assert captured["data"]["code"] == "auth-code-123"
    assert captured["data"]["redirect_uri"] == "https://example.com/schcallback"
    assert captured["headers"]["Authorization"].startswith("Basic ")
    assert schwab_client._token_cache["access_token"] == "at-1"  # noqa: SLF001


def test_get_valid_access_token_reuses_cached_token_before_expiry(monkeypatch):
    def fail_if_called(*a, **k):
        raise AssertionError("must not refresh a token that isn't expired yet")

    monkeypatch.setattr(schwab_client.requests, "post", fail_if_called)
    schwab_client._token_cache.update({  # noqa: SLF001
        "access_token": "still-good", "refresh_token": "rt-1",
        "obtained_at": schwab_client.time.time(), "expires_at": schwab_client.time.time() + 1000,
    })
    assert schwab_client.get_valid_access_token() == "still-good"


def test_get_valid_access_token_refreshes_when_expired(monkeypatch):
    def fake_post(url, *, headers, data, timeout):
        assert data["grant_type"] == "refresh_token"
        assert data["refresh_token"] == "rt-1"
        return _FakeResponse({"access_token": "at-refreshed", "refresh_token": "rt-2", "expires_in": 1800})

    monkeypatch.setattr(schwab_client.requests, "post", fake_post)
    schwab_client._token_cache.update({  # noqa: SLF001
        "access_token": "stale", "refresh_token": "rt-1",
        "obtained_at": schwab_client.time.time() - 2000, "expires_at": schwab_client.time.time() - 100,
    })
    assert schwab_client.get_valid_access_token() == "at-refreshed"
    assert schwab_client._token_cache["refresh_token"] == "rt-2"  # noqa: SLF001


def test_get_valid_access_token_returns_none_without_ever_logging_in():
    assert schwab_client.get_valid_access_token() is None


def test_get_valid_access_token_returns_none_when_refresh_fails(monkeypatch):
    def fake_post(url, *, headers, data, timeout):
        return _FakeResponse({"error": "invalid_grant"}, status_code=400)

    monkeypatch.setattr(schwab_client.requests, "post", fake_post)
    schwab_client._token_cache.update({  # noqa: SLF001
        "access_token": "stale", "refresh_token": "rt-expired",
        "obtained_at": 0.0, "expires_at": 0.0,
    })
    assert schwab_client.get_valid_access_token() is None


def test_get_raises_without_a_valid_token():
    with pytest.raises(RuntimeError, match="No valid Schwab access token"):
        schwab_client.get("/marketdata/v1/pricehistory")


def test_get_sends_bearer_auth_header(monkeypatch):
    schwab_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "refresh_token": "rt-1",
        "obtained_at": schwab_client.time.time(), "expires_at": schwab_client.time.time() + 1000,
    })
    captured = {}

    def fake_get(url, *, headers, params, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["params"] = params
        return _FakeResponse({"candles": []})

    monkeypatch.setattr(schwab_client.requests, "get", fake_get)
    result = schwab_client.get("/marketdata/v1/pricehistory", params={"symbol": "AAPL"})

    assert result == {"candles": []}
    assert captured["url"] == f"{schwab_client.API_BASE_URL}/marketdata/v1/pricehistory"
    assert captured["headers"]["Authorization"] == "Bearer at-1"
    assert captured["params"] == {"symbol": "AAPL"}
