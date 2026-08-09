"""Meta Threads OAuth client -- authorization URL construction, the
two-step short-lived-then-long-lived token exchange, refresh gating (must
be near expiry AND at least 24h old), and the durable HF token mirror.
All network calls mocked; never touches the real Threads API or HF."""
from __future__ import annotations

import pytest

from data import threads_client


class _FakeResponse:
    def __init__(self, json_body, status_code=200, text=None, url="https://example.com/fake"):
        self._json_body = json_body
        self.status_code = status_code
        self.text = text if text is not None else str(json_body)
        self.url = url

    def json(self):
        return self._json_body


@pytest.fixture(autouse=True)
def _isolated_token_state(monkeypatch):
    monkeypatch.setattr(threads_client, "_token_cache", {})
    monkeypatch.setattr(threads_client, "APP_ID", "test-app-id")
    monkeypatch.setattr(threads_client, "APP_SECRET", "test-app-secret")
    monkeypatch.setattr(threads_client, "REDIRECT_URI", "https://example.com/threadscallback")
    monkeypatch.setattr(threads_client, "HF_API_KEY", "")  # no real network for the HF mirror by default
    monkeypatch.setattr(threads_client, "_rate_limited_until", 0.0)  # no leftover cooldown between tests
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


def test_get_valid_access_token_does_not_refresh_a_token_younger_than_the_minimum_age(monkeypatch):
    """Threads' refresh endpoint can reject a too-fresh token -- refreshing
    earlier would just fail, so the still-valid current token must be
    returned as-is instead of attempting (and failing) a premature refresh."""
    def fail_if_called(*a, **k):
        raise AssertionError("must not attempt a refresh before the minimum-age floor")

    monkeypatch.setattr(threads_client.requests, "get", fail_if_called)
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "brand-new", "user_id": "1",
        "obtained_at": threads_client.time.time() - 3600,  # 1h old, under the 2h floor
        "expires_at": threads_client.time.time() + 1 * 86400,  # still inside the refresh margin
    })
    assert threads_client.get_valid_access_token() == "brand-new"


def test_get_valid_access_token_refreshes_a_token_that_only_has_a_24h_lifetime(monkeypatch):
    """Real, confirmed production incident: a stored token was observed
    with only a 24h total lifetime (not the ~60 days Meta's docs describe
    for a genuine long-lived token). The OLD 25h minimum-age floor could
    NEVER be satisfied before such a token expired -- permanently breaking
    Threads posting with no automatic recovery, confirmed live when calling
    Meta's refresh endpoint directly on a 21h-old token succeeded
    immediately. This locks in the fix: a token old enough to clear the
    (now much lower) minimum-age floor, but with a short total lifetime,
    must still get refreshed well before it expires."""
    def fake_get(url, *, params, timeout):
        return _FakeResponse({"access_token": "refreshed", "expires_in": 5107617})

    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    obtained_at = threads_client.time.time() - 21 * 3600  # 21h old
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "aging-short-lived", "user_id": "1",
        "obtained_at": obtained_at, "expires_at": obtained_at + 24 * 3600,  # only a 24h lifetime
    })
    assert threads_client.get_valid_access_token() == "refreshed"


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


def test_raise_for_status_with_body_includes_the_response_body(monkeypatch):
    """Real gap found in review: plain resp.raise_for_status() raises an
    HTTPError with just "400 Client Error" -- Meta's own rejection reason
    (bad creation_id, rate limit, content policy, etc.) lives in the
    response body, which never made it into the logs. A real, repeated
    threads_publish 400 was undiagnosable from Render's logs alone before
    this fix."""
    resp = _FakeResponse({"error": {"message": "Invalid creation_id"}}, status_code=400, url="https://graph.threads.net/v1.0/x/threads_publish")
    with pytest.raises(threads_client.requests.exceptions.HTTPError, match="Invalid creation_id"):
        threads_client._raise_for_status_with_body(resp)  # noqa: SLF001


def test_raise_for_status_with_body_does_not_raise_below_400():
    resp = _FakeResponse({"id": "ok"}, status_code=200)
    threads_client._raise_for_status_with_body(resp)  # noqa: SLF001 -- must not raise


# ---------------------------------------------------------------------------
# Rate-limit cooldown -- real, confirmed production incident: all 4
# services share one Threads account, each posting several content types
# on its own schedule. Every post (text or image) is create->poll->publish,
# so real call volume against the shared account is a multiple of the
# visible post count. Confirmed live: "Threads API Rate Limit Exceeded"
# (error_subcode 4279002) fired across all 4 services within the same
# ~40-second window, for multiple different post types each -- i.e. once
# rate-limited, every remaining scheduled post kept re-discovering the
# same failure instead of backing off.
# ---------------------------------------------------------------------------

def test_raise_for_status_with_body_sets_the_cooldown_on_a_rate_limit_error():
    assert threads_client.is_rate_limited() is False
    resp = _FakeResponse(
        {"error": {"message": "Calls to this api have exceeded the rate limit.", "code": 613, "error_subcode": 4279002}},
        status_code=400,
    )
    with pytest.raises(threads_client.requests.exceptions.HTTPError):
        threads_client._raise_for_status_with_body(resp)  # noqa: SLF001
    assert threads_client.is_rate_limited() is True


def test_raise_for_status_with_body_does_not_set_the_cooldown_for_other_errors():
    resp = _FakeResponse({"error": {"message": "Invalid creation_id", "code": 100}}, status_code=400)
    with pytest.raises(threads_client.requests.exceptions.HTTPError):
        threads_client._raise_for_status_with_body(resp)  # noqa: SLF001
    assert threads_client.is_rate_limited() is False


def test_is_rate_limited_clears_after_the_cooldown_expires(monkeypatch):
    monkeypatch.setattr(threads_client, "_rate_limited_until", threads_client.time.monotonic() - 1)
    assert threads_client.is_rate_limited() is False


def test_create_and_publish_post_skips_the_network_call_while_rate_limited(monkeypatch):
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "user_id": "user-42",
        "obtained_at": threads_client.time.time(), "expires_at": threads_client.time.time() + 1000000,
    })
    monkeypatch.setattr(threads_client, "_rate_limited_until", threads_client.time.monotonic() + 1000)

    def fail_if_called(*a, **k):
        raise AssertionError("must not make a network call while the rate-limit cooldown is active")

    monkeypatch.setattr(threads_client.requests, "post", fail_if_called)
    with pytest.raises(RuntimeError, match="rate limit cooldown"):
        threads_client.create_and_publish_post("hello world")


def test_create_and_publish_image_post_skips_the_network_call_while_rate_limited(monkeypatch):
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "user_id": "user-42",
        "obtained_at": threads_client.time.time(), "expires_at": threads_client.time.time() + 1000000,
    })
    monkeypatch.setattr(threads_client, "_rate_limited_until", threads_client.time.monotonic() + 1000)

    def fail_if_called(*a, **k):
        raise AssertionError("must not make a network call while the rate-limit cooldown is active")

    monkeypatch.setattr(threads_client.requests, "post", fail_if_called)
    with pytest.raises(RuntimeError, match="rate limit cooldown"):
        threads_client.create_and_publish_image_post("https://example.com/chart.png", "caption")


def test_pull_tokens_from_hf_gives_up_after_a_hard_timeout(monkeypatch):
    """Real, confirmed production incident: huggingface_hub's own internal
    shared-session lock can hang for minutes, and this call runs WHILE
    _STATE_LOCK is held (from get_valid_access_token/get_user_id) -- a
    single stuck call here doesn't just freeze itself, it blocks every
    other caller of those two functions too, including a plain /api/status
    health check. Confirmed live: gunicorn's own 300s WORKER TIMEOUT
    killed the perps worker mid-request, stuck acquiring _STATE_LOCK,
    because this call never had the same hard-timeout protection every
    other HF pull in this codebase already has."""
    monkeypatch.setattr(threads_client, "HF_API_KEY", "test-key")
    monkeypatch.setattr(threads_client, "_PULL_TOKENS_HF_TIMEOUT_SEC", 0.05)

    def hang_forever(*a, **k):
        import time as t
        t.sleep(0.5)  # comfortably longer than the 0.05s timeout below, short enough not to stall pytest's own exit

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", hang_forever, raising=False)

    import time as real_time
    start = real_time.monotonic()
    result = threads_client._pull_tokens_from_hf()  # noqa: SLF001
    elapsed = real_time.monotonic() - start

    assert result is None
    assert elapsed < 2.0  # bounded by the hard timeout, not the 5s hang


def test_get_user_id_returns_the_cached_value():
    threads_client._token_cache.update({"user_id": "999"})  # noqa: SLF001
    assert threads_client.get_user_id() == "999"


def test_wait_for_container_ready_returns_immediately_on_finished(monkeypatch):
    calls = []

    def fake_get(url, *, params, timeout):
        calls.append(params)
        return _FakeResponse({"status": "FINISHED"})

    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    threads_client._wait_for_container_ready("creation-1", "token-1")  # noqa: SLF001
    assert len(calls) == 1
    assert calls[0]["access_token"] == "token-1"


def test_wait_for_container_ready_polls_until_finished(monkeypatch):
    statuses = iter(["IN_PROGRESS", "IN_PROGRESS", "FINISHED"])
    monkeypatch.setattr(threads_client, "_CONTAINER_STATUS_POLL_INTERVAL_SEC", 0.01)
    monkeypatch.setattr(threads_client.requests, "get", lambda url, *, params, timeout: _FakeResponse({"status": next(statuses)}))
    threads_client._wait_for_container_ready("creation-1", "token-1")  # noqa: SLF001
    assert next(statuses, "exhausted") == "exhausted"  # confirms all 3 statuses were actually consumed


def test_wait_for_container_ready_stops_on_a_terminal_error_status(monkeypatch):
    calls = {"n": 0}

    def fake_get(url, *, params, timeout):
        calls["n"] += 1
        return _FakeResponse({"status": "ERROR"})

    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    threads_client._wait_for_container_ready("creation-1", "token-1")  # noqa: SLF001
    assert calls["n"] == 1  # stopped polling immediately, didn't keep retrying a terminal state


def test_wait_for_container_ready_gives_up_after_the_max_wait(monkeypatch):
    monkeypatch.setattr(threads_client, "_CONTAINER_STATUS_MAX_WAIT_SEC", 0.05)
    monkeypatch.setattr(threads_client, "_CONTAINER_STATUS_POLL_INTERVAL_SEC", 0.01)
    monkeypatch.setattr(threads_client.requests, "get", lambda url, *, params, timeout: _FakeResponse({"status": "IN_PROGRESS"}))

    import time as real_time
    start = real_time.monotonic()
    threads_client._wait_for_container_ready("creation-1", "token-1")  # noqa: SLF001
    elapsed = real_time.monotonic() - start
    assert elapsed < 1.0  # bounded by _CONTAINER_STATUS_MAX_WAIT_SEC, not an infinite loop


def test_wait_for_container_ready_never_raises_on_a_status_check_failure(monkeypatch):
    monkeypatch.setattr(threads_client, "_CONTAINER_STATUS_MAX_WAIT_SEC", 0.05)
    monkeypatch.setattr(threads_client, "_CONTAINER_STATUS_POLL_INTERVAL_SEC", 0.01)

    def raise_error(url, *, params, timeout):
        raise RuntimeError("network down")

    monkeypatch.setattr(threads_client.requests, "get", raise_error)
    threads_client._wait_for_container_ready("creation-1", "token-1")  # noqa: SLF001  # must not raise


def test_create_and_publish_post_waits_for_the_container_before_publishing(monkeypatch):
    """The real bug this whole fix addresses: publish must not fire until
    the container reports FINISHED."""
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "user_id": "user-42",
        "obtained_at": threads_client.time.time(), "expires_at": threads_client.time.time() + 1000000,
    })
    call_order = []

    def fake_post(url, *, params, timeout):
        call_order.append(("post", url))
        if "threads_publish" in url:
            return _FakeResponse({"id": "post-999"})
        return _FakeResponse({"id": "creation-123"})

    def fake_get(url, *, params, timeout):
        call_order.append(("get", url))
        return _FakeResponse({"status": "FINISHED"})

    monkeypatch.setattr(threads_client.requests, "post", fake_post)
    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    threads_client.create_and_publish_post("hello world")

    kinds = [k for k, _ in call_order]
    assert kinds == ["post", "get", "post"]  # create -> status check -> publish, in that order


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

    def fake_get(url, *, params, timeout):
        return _FakeResponse({"status": "FINISHED"})

    monkeypatch.setattr(threads_client.requests, "post", fake_post)
    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    post_id = threads_client.create_and_publish_post("hello world")

    assert post_id == "post-999"
    assert captured[0]["url"] == f"{threads_client.API_BASE_URL}/{threads_client.API_VERSION}/user-42/threads"
    assert captured[0]["params"]["media_type"] == "TEXT"
    assert captured[0]["params"]["text"] == "hello world"
    assert captured[1]["url"] == f"{threads_client.API_BASE_URL}/{threads_client.API_VERSION}/user-42/threads_publish"
    assert captured[1]["params"]["creation_id"] == "creation-123"


def test_create_and_publish_image_post_raises_without_a_valid_token():
    with pytest.raises(RuntimeError, match="No valid Threads access token"):
        threads_client.create_and_publish_image_post("https://example.com/chart.png", "caption")


def test_create_and_publish_image_post_does_the_container_then_publish_round_trip(monkeypatch):
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

    def fake_get(url, *, params, timeout):
        return _FakeResponse({"status": "FINISHED"})

    monkeypatch.setattr(threads_client.requests, "post", fake_post)
    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    post_id = threads_client.create_and_publish_image_post("https://example.com/chart.png", "caption text")

    assert post_id == "post-999"
    assert captured[0]["url"] == f"{threads_client.API_BASE_URL}/{threads_client.API_VERSION}/user-42/threads"
    assert captured[0]["params"]["media_type"] == "IMAGE"
    assert captured[0]["params"]["image_url"] == "https://example.com/chart.png"
    assert captured[0]["params"]["text"] == "caption text"
    assert captured[1]["params"]["creation_id"] == "creation-123"
