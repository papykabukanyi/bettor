"""Meta Threads OAuth client -- authorization URL construction, the
two-step short-lived-then-long-lived token exchange, refresh gating (must
be near expiry AND at least 24h old), and the durable HF token mirror.
All network calls mocked; never touches the real Threads API or HF."""
from __future__ import annotations

import pytest

from data import threads_client


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
    monkeypatch.setattr(threads_client, "_token_cache", {})
    monkeypatch.setattr(threads_client, "APP_ID", "test-app-id")
    monkeypatch.setattr(threads_client, "APP_SECRET", "test-app-secret")
    monkeypatch.setattr(threads_client, "REDIRECT_URI", "https://example.com/threadscallback")
    monkeypatch.setattr(threads_client, "HF_API_KEY", "")  # no real network for the HF mirror by default
    yield


def test_get_authorization_url_includes_client_id_redirect_and_scope():
    url = threads_client.get_authorization_url()
    assert url.startswith(threads_client.AUTHORIZE_URL)
    assert "client_id=test-app-id" in url
    assert "redirect_uri=" in url
    assert "response_type=code" in url
    assert "threads_content_publish" in url


def test_exchange_code_for_tokens_does_the_short_then_long_lived_round_trip(monkeypatch):
    captured = {"post": None, "get": None}

    def fake_post(url, *, data, timeout):
        captured["post"] = {"url": url, "data": data}
        return _FakeResponse({"access_token": "short-lived-1", "user_id": "12345"})

    def fake_get(url, *, params, timeout):
        captured["get"] = {"url": url, "params": params}
        return _FakeResponse({"access_token": "long-lived-1", "token_type": "bearer", "expires_in": 5184000})

    monkeypatch.setattr(threads_client.requests, "post", fake_post)
    monkeypatch.setattr(threads_client.requests, "get", fake_get)

    result = threads_client.exchange_code_for_tokens("auth-code-123")

    assert result["access_token"] == "long-lived-1"
    assert captured["post"]["url"] == threads_client.TOKEN_URL
    assert captured["post"]["data"]["grant_type"] == "authorization_code"
    assert captured["post"]["data"]["code"] == "auth-code-123"
    assert captured["post"]["data"]["redirect_uri"] == "https://example.com/threadscallback"
    assert captured["get"]["url"] == threads_client.EXCHANGE_URL
    assert captured["get"]["params"]["grant_type"] == "th_exchange_token"
    assert captured["get"]["params"]["access_token"] == "short-lived-1"
    # The long-lived token (not the short-lived one) is what actually gets
    # cached/used going forward.
    assert threads_client._token_cache["access_token"] == "long-lived-1"  # noqa: SLF001
    assert threads_client._token_cache["user_id"] == "12345"  # noqa: SLF001


def test_get_valid_access_token_reuses_cached_token_before_expiry(monkeypatch):
    def fail_if_called(*a, **k):
        raise AssertionError("must not refresh a token that isn't near expiry")

    monkeypatch.setattr(threads_client.requests, "get", fail_if_called)
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "still-good", "user_id": "1",
        "obtained_at": threads_client.time.time() - 30 * 86400,
        "expires_at": threads_client.time.time() + 30 * 86400,
    })
    assert threads_client.get_valid_access_token() == "still-good"


def test_get_valid_access_token_refreshes_when_near_expiry_and_old_enough(monkeypatch):
    def fake_get(url, *, params, timeout):
        assert url == threads_client.REFRESH_URL
        assert params["grant_type"] == "th_refresh_token"
        assert params["access_token"] == "aging"
        return _FakeResponse({"access_token": "refreshed", "expires_in": 5184000})

    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "aging", "user_id": "1",
        "obtained_at": threads_client.time.time() - 56 * 86400,  # well over the 24h floor
        "expires_at": threads_client.time.time() + 1 * 86400,  # inside the 5-day refresh margin
    })
    assert threads_client.get_valid_access_token() == "refreshed"
    assert threads_client._token_cache["access_token"] == "refreshed"  # noqa: SLF001


def test_get_valid_access_token_does_not_refresh_a_token_younger_than_24h(monkeypatch):
    """Threads' own refresh endpoint requires the token to be at least 24h
    old -- refreshing earlier would just fail, so the still-valid current
    token must be returned as-is instead of attempting (and failing) a
    premature refresh."""
    def fail_if_called(*a, **k):
        raise AssertionError("must not attempt a refresh before the 24h floor")

    monkeypatch.setattr(threads_client.requests, "get", fail_if_called)
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "brand-new", "user_id": "1",
        "obtained_at": threads_client.time.time() - 3600,  # 1h old, under the 25h floor
        "expires_at": threads_client.time.time() + 1 * 86400,  # still inside the refresh margin
    })
    assert threads_client.get_valid_access_token() == "brand-new"


def test_get_valid_access_token_returns_none_without_ever_logging_in():
    assert threads_client.get_valid_access_token() is None


def test_get_valid_access_token_keeps_current_token_when_refresh_fails(monkeypatch):
    def fake_get(url, *, params, timeout):
        return _FakeResponse({"error": "invalid_token"}, status_code=400)

    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "still-usable", "user_id": "1",
        "obtained_at": threads_client.time.time() - 56 * 86400,
        "expires_at": threads_client.time.time() + 1 * 86400,
    })
    # A failed refresh attempt must not throw away the still-valid token --
    # keep using it until it actually expires.
    assert threads_client.get_valid_access_token() == "still-usable"


def test_get_user_id_returns_the_cached_value():
    threads_client._token_cache.update({"user_id": "999"})  # noqa: SLF001
    assert threads_client.get_user_id() == "999"


def test_create_and_publish_post_raises_without_a_valid_token():
    with pytest.raises(RuntimeError, match="No valid Threads access token"):
        threads_client.create_and_publish_post("hello")


def test_create_and_publish_post_does_the_container_then_publish_round_trip(monkeypatch):
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "user_id": "user-42",
        "obtained_at": threads_client.time.time(), "expires_at": threads_client.time.time() + 1000000,
    })
    captured = []

    def fake_post(url, *, params, timeout):
        captured.append({"url": url, "params": params})
        if "threads_publish" in url:
            return _FakeResponse({"id": "post-999"})
        return _FakeResponse({"id": "creation-123"})

    monkeypatch.setattr(threads_client.requests, "post", fake_post)
    post_id = threads_client.create_and_publish_post("hello world")

    assert post_id == "post-999"
    assert captured[0]["url"] == f"{threads_client.API_BASE_URL}/{threads_client.API_VERSION}/user-42/threads"
    assert captured[0]["params"]["media_type"] == "TEXT"
    assert captured[0]["params"]["text"] == "hello world"
    assert captured[1]["url"] == f"{threads_client.API_BASE_URL}/{threads_client.API_VERSION}/user-42/threads_publish"
    assert captured[1]["params"]["creation_id"] == "creation-123"
