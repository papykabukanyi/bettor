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
    monkeypatch.setattr(threads_client, "_list_posts_cache", None)
    monkeypatch.setattr(threads_client, "_posts_archive_cache", None)
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
    monkeypatch.setattr(threads_client, "_rate_limited_until", threads_client.time.time() - 1)
    assert threads_client.is_rate_limited() is False


def test_note_rate_limited_pushes_the_cooldown_to_hf_merged_with_existing_tokens(monkeypatch):
    """Real, confirmed gap: the cooldown used to live only in this
    process's own memory -- each of the 4 services sharing one Threads
    account discovered a rate limit independently instead of the instant
    ANY of them hit it. Must push the FULL token record (not just the new
    field) so this never clobbers the stored access_token/user_id."""
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "user_id": "user-42", "obtained_at": 1000.0, "expires_at": 2000.0,
    })
    pushed = {}
    monkeypatch.setattr(threads_client, "_push_tokens_to_hf", lambda record: pushed.update(record))

    threads_client._note_rate_limited()  # noqa: SLF001

    assert pushed["access_token"] == "at-1"
    assert pushed["user_id"] == "user-42"
    assert pushed["rate_limited_until"] == pytest.approx(threads_client.time.time() + threads_client._RATE_LIMIT_COOLDOWN_SEC, abs=2)  # noqa: SLF001


def test_note_rate_limited_does_not_push_when_there_is_no_token_yet(monkeypatch):
    pushed = {"called": False}
    monkeypatch.setattr(threads_client, "_push_tokens_to_hf", lambda record: pushed.update(called=True))

    threads_client._note_rate_limited()  # noqa: SLF001

    assert pushed["called"] is False


def test_is_rate_limited_adopts_a_cooldown_set_by_another_process_via_hf(monkeypatch):
    """Not locally rate-limited, but another one of the 4 services already
    hit the shared limit and pushed a cooldown to HF -- this process must
    back off too instead of finding out the hard way."""
    monkeypatch.setattr(threads_client, "_last_rate_limit_hf_check_ts", 0.0)
    future = threads_client.time.time() + 500
    monkeypatch.setattr(threads_client, "_pull_tokens_from_hf", lambda: {"access_token": "at-1", "rate_limited_until": future})

    assert threads_client.is_rate_limited() is True
    assert threads_client._rate_limited_until == pytest.approx(future)  # noqa: SLF001


def test_is_rate_limited_does_not_recheck_hf_within_the_cooldown_window(monkeypatch):
    monkeypatch.setattr(threads_client, "_last_rate_limit_hf_check_ts", threads_client.time.time())
    calls = {"n": 0}

    def fake_pull():
        calls["n"] += 1
        return {"rate_limited_until": threads_client.time.time() + 500}

    monkeypatch.setattr(threads_client, "_pull_tokens_from_hf", fake_pull)

    assert threads_client.is_rate_limited() is False  # too soon since the last HF check -- must not call it again
    assert calls["n"] == 0


def test_create_and_publish_post_skips_the_network_call_while_rate_limited(monkeypatch):
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "user_id": "user-42",
        "obtained_at": threads_client.time.time(), "expires_at": threads_client.time.time() + 1000000,
    })
    monkeypatch.setattr(threads_client, "_rate_limited_until", threads_client.time.time() + 1000)

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
    monkeypatch.setattr(threads_client, "_rate_limited_until", threads_client.time.time() + 1000)

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
    status = threads_client._wait_for_container_ready("creation-1", "token-1")  # noqa: SLF001
    assert len(calls) == 1
    assert calls[0]["access_token"] == "token-1"
    assert status == "FINISHED"


def test_wait_for_container_ready_polls_until_finished(monkeypatch):
    statuses = iter(["IN_PROGRESS", "IN_PROGRESS", "FINISHED"])
    monkeypatch.setattr(threads_client, "_CONTAINER_STATUS_POLL_INTERVAL_SEC", 0.01)
    monkeypatch.setattr(threads_client.requests, "get", lambda url, *, params, timeout: _FakeResponse({"status": next(statuses)}))
    status = threads_client._wait_for_container_ready("creation-1", "token-1")  # noqa: SLF001
    assert next(statuses, "exhausted") == "exhausted"  # confirms all 3 statuses were actually consumed
    assert status == "FINISHED"


def test_wait_for_container_ready_stops_on_a_terminal_error_status(monkeypatch):
    calls = {"n": 0}

    def fake_get(url, *, params, timeout):
        calls["n"] += 1
        return _FakeResponse({"status": "ERROR"})

    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    status = threads_client._wait_for_container_ready("creation-1", "token-1")  # noqa: SLF001
    assert calls["n"] == 1  # stopped polling immediately, didn't keep retrying a terminal state
    assert status == "ERROR"


def test_wait_for_container_ready_gives_up_after_the_max_wait(monkeypatch):
    monkeypatch.setattr(threads_client, "_CONTAINER_STATUS_MAX_WAIT_SEC", 0.05)
    monkeypatch.setattr(threads_client, "_CONTAINER_STATUS_POLL_INTERVAL_SEC", 0.01)
    monkeypatch.setattr(threads_client.requests, "get", lambda url, *, params, timeout: _FakeResponse({"status": "IN_PROGRESS"}))

    import time as real_time
    start = real_time.monotonic()
    status = threads_client._wait_for_container_ready("creation-1", "token-1")  # noqa: SLF001
    elapsed = real_time.monotonic() - start
    assert elapsed < 1.0  # bounded by _CONTAINER_STATUS_MAX_WAIT_SEC, not an infinite loop
    assert status == "IN_PROGRESS"  # never reached a terminal state -- caller still gets to decide what to do


def test_wait_for_container_ready_never_raises_on_a_status_check_failure(monkeypatch):
    monkeypatch.setattr(threads_client, "_CONTAINER_STATUS_MAX_WAIT_SEC", 0.05)
    monkeypatch.setattr(threads_client, "_CONTAINER_STATUS_POLL_INTERVAL_SEC", 0.01)

    def raise_error(url, *, params, timeout):
        raise RuntimeError("network down")

    monkeypatch.setattr(threads_client.requests, "get", raise_error)
    status = threads_client._wait_for_container_ready("creation-1", "token-1")  # noqa: SLF001  # must not raise
    assert status is None


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
    assert captured[0]["params"]["text"] == f"hello world{threads_client._PROMO_TAG}"  # noqa: SLF001
    assert captured[1]["url"] == f"{threads_client.API_BASE_URL}/{threads_client.API_VERSION}/user-42/threads_publish"
    assert captured[1]["params"]["creation_id"] == "creation-123"


def test_create_and_publish_post_refuses_to_publish_a_container_in_a_terminal_error_state(monkeypatch):
    """Real, confirmed live bug: publishing a container that already
    reached ERROR/EXPIRED always got rejected by Meta's API with
    error_subcode 4279009 ("the requested resource does not exist") -- a
    wasted call against an already rate-limit-sensitive shared account,
    18-30 occurrences per service across 5 days. Must fail fast locally
    instead, with a clear reason, and never even attempt the publish call."""
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "user_id": "user-42",
        "obtained_at": threads_client.time.time(), "expires_at": threads_client.time.time() + 1000000,
    })

    def fake_post(url, *, params, timeout):
        if "threads_publish" in url:
            raise AssertionError("must not attempt to publish a container in a terminal ERROR/EXPIRED state")
        return _FakeResponse({"id": "creation-123"})

    monkeypatch.setattr(threads_client.requests, "post", fake_post)
    monkeypatch.setattr(threads_client.requests, "get", lambda url, *, params, timeout: _FakeResponse({"status": "ERROR"}))

    with pytest.raises(RuntimeError, match="terminal status ERROR"):
        threads_client.create_and_publish_post("hello world")


def test_create_and_publish_post_still_attempts_publish_when_status_is_unknown(monkeypatch):
    """A status-check timeout/failure (status stays None) is NOT a
    confirmed-bad state -- unlike ERROR/EXPIRED, still worth a real
    attempt rather than giving up locally on an uncertain read."""
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "user_id": "user-42",
        "obtained_at": threads_client.time.time(), "expires_at": threads_client.time.time() + 1000000,
    })
    monkeypatch.setattr(threads_client, "_CONTAINER_STATUS_MAX_WAIT_SEC", 0.05)
    monkeypatch.setattr(threads_client, "_CONTAINER_STATUS_POLL_INTERVAL_SEC", 0.01)

    def fake_post(url, *, params, timeout):
        if "threads_publish" in url:
            return _FakeResponse({"id": "post-999"})
        return _FakeResponse({"id": "creation-123"})

    def raise_error(url, *, params, timeout):
        raise RuntimeError("network down")

    monkeypatch.setattr(threads_client.requests, "post", fake_post)
    monkeypatch.setattr(threads_client.requests, "get", raise_error)

    post_id = threads_client.create_and_publish_post("hello world")
    assert post_id == "post-999"


def test_create_and_publish_image_post_refuses_to_publish_a_container_in_a_terminal_error_state(monkeypatch):
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "user_id": "user-42",
        "obtained_at": threads_client.time.time(), "expires_at": threads_client.time.time() + 1000000,
    })

    def fake_post(url, *, params, timeout):
        if "threads_publish" in url:
            raise AssertionError("must not attempt to publish a container in a terminal ERROR/EXPIRED state")
        return _FakeResponse({"id": "creation-123"})

    monkeypatch.setattr(threads_client.requests, "post", fake_post)
    monkeypatch.setattr(threads_client.requests, "get", lambda url, *, params, timeout: _FakeResponse({"status": "EXPIRED"}))

    with pytest.raises(RuntimeError, match="terminal status EXPIRED"):
        threads_client.create_and_publish_image_post("https://example.com/chart.png", "caption")


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
    assert captured[0]["params"]["text"] == f"caption text{threads_client._PROMO_TAG}"  # noqa: SLF001
    assert captured[1]["params"]["creation_id"] == "creation-123"


# ---------------------------------------------------------------------------
# create_and_publish_carousel_post -- multiple images under ONE Threads
# post. 3-step flow: item containers (is_carousel_item=true) -> carousel
# container (children=<ids>) -> publish.
# ---------------------------------------------------------------------------

def test_create_and_publish_carousel_post_rejects_too_few_images():
    with pytest.raises(ValueError, match="at least"):
        threads_client.create_and_publish_carousel_post(["https://example.com/a.png"])


def test_create_and_publish_carousel_post_rejects_too_many_images():
    urls = [f"https://example.com/{i}.png" for i in range(threads_client.CAROUSEL_MAX_ITEMS + 1)]
    with pytest.raises(ValueError, match="at most"):
        threads_client.create_and_publish_carousel_post(urls)


def test_create_and_publish_carousel_post_skips_the_network_call_while_rate_limited(monkeypatch):
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "user_id": "user-42",
        "obtained_at": threads_client.time.time(), "expires_at": threads_client.time.time() + 1000000,
    })
    monkeypatch.setattr(threads_client, "_rate_limited_until", threads_client.time.time() + 1000)

    def fail_if_called(*a, **k):
        raise AssertionError("must not make a network call while the rate-limit cooldown is active")

    monkeypatch.setattr(threads_client.requests, "post", fail_if_called)
    with pytest.raises(RuntimeError, match="rate limit cooldown"):
        threads_client.create_and_publish_carousel_post(["https://example.com/a.png", "https://example.com/b.png"])


def test_create_and_publish_carousel_post_raises_without_a_valid_token():
    with pytest.raises(RuntimeError, match="No valid Threads access token"):
        threads_client.create_and_publish_carousel_post(["https://example.com/a.png", "https://example.com/b.png"])


def test_create_and_publish_carousel_post_creates_one_item_container_per_image_then_the_carousel(monkeypatch):
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "user_id": "user-42",
        "obtained_at": threads_client.time.time(), "expires_at": threads_client.time.time() + 1000000,
    })
    urls = ["https://example.com/a.png", "https://example.com/b.png", "https://example.com/c.png"]
    captured = []
    creation_counter = {"n": 0}

    def fake_post(url, *, params, timeout):
        captured.append({"url": url, "params": dict(params)})
        if "threads_publish" in url:
            return _FakeResponse({"id": "post-999"})
        if params.get("media_type") == "CAROUSEL":
            return _FakeResponse({"id": "carousel-container-1"})
        creation_counter["n"] += 1
        return _FakeResponse({"id": f"item-{creation_counter['n']}"})

    monkeypatch.setattr(threads_client.requests, "post", fake_post)
    monkeypatch.setattr(threads_client.requests, "get", lambda url, *, params, timeout: _FakeResponse({"status": "FINISHED"}))

    post_id = threads_client.create_and_publish_carousel_post(urls, "check out these 3 stories")

    assert post_id == "post-999"
    item_calls = [c for c in captured if c["params"].get("media_type") == "IMAGE"]
    assert len(item_calls) == 3
    for call, url in zip(item_calls, urls):
        assert call["params"]["image_url"] == url
        assert call["params"]["is_carousel_item"] == "true"
        assert "text" not in call["params"]  # caption lives on the carousel container, not its children

    carousel_calls = [c for c in captured if c["params"].get("media_type") == "CAROUSEL"]
    assert len(carousel_calls) == 1
    assert carousel_calls[0]["params"]["children"] == "item-1,item-2,item-3"
    assert carousel_calls[0]["params"]["text"] == f"check out these 3 stories{threads_client._PROMO_TAG}"  # noqa: SLF001

    publish_calls = [c for c in captured if "threads_publish" in c["url"]]
    assert len(publish_calls) == 1
    assert publish_calls[0]["params"]["creation_id"] == "carousel-container-1"


def test_create_and_publish_carousel_post_fails_if_any_item_container_errors(monkeypatch):
    """A partially-broken carousel must not be published at all -- same
    discipline as the plain image/text post's own ERROR/EXPIRED check."""
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "user_id": "user-42",
        "obtained_at": threads_client.time.time(), "expires_at": threads_client.time.time() + 1000000,
    })
    urls = ["https://example.com/a.png", "https://example.com/b.png"]
    creation_counter = {"n": 0}

    def fake_post(url, *, params, timeout):
        if "threads_publish" in url:
            raise AssertionError("must not attempt to publish when an item container failed")
        creation_counter["n"] += 1
        return _FakeResponse({"id": f"item-{creation_counter['n']}"})

    statuses = iter(["FINISHED", "ERROR"])
    monkeypatch.setattr(threads_client.requests, "post", fake_post)
    monkeypatch.setattr(threads_client.requests, "get", lambda url, *, params, timeout: _FakeResponse({"status": next(statuses)}))

    with pytest.raises(RuntimeError, match="terminal status ERROR"):
        threads_client.create_and_publish_carousel_post(urls)


def test_create_and_publish_carousel_post_refuses_to_publish_a_carousel_container_in_error(monkeypatch):
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "user_id": "user-42",
        "obtained_at": threads_client.time.time(), "expires_at": threads_client.time.time() + 1000000,
    })
    urls = ["https://example.com/a.png", "https://example.com/b.png"]

    def fake_post(url, *, params, timeout):
        if "threads_publish" in url:
            raise AssertionError("must not attempt to publish a carousel container in a terminal ERROR/EXPIRED state")
        return _FakeResponse({"id": "some-id"})

    monkeypatch.setattr(threads_client.requests, "post", fake_post)
    # Every status check (item x2, then carousel) reports FINISHED except
    # the LAST one (the carousel container itself), which is EXPIRED.
    statuses = iter(["FINISHED", "FINISHED", "EXPIRED"])
    monkeypatch.setattr(threads_client.requests, "get", lambda url, *, params, timeout: _FakeResponse({"status": next(statuses)}))

    with pytest.raises(RuntimeError, match="carousel container.*terminal status EXPIRED"):
        threads_client.create_and_publish_carousel_post(urls)


def test_create_and_publish_carousel_post_passes_through_reply_params(monkeypatch):
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "user_id": "user-42",
        "obtained_at": threads_client.time.time(), "expires_at": threads_client.time.time() + 1000000,
    })
    urls = ["https://example.com/a.png", "https://example.com/b.png"]
    captured = []

    def fake_post(url, *, params, timeout):
        captured.append(dict(params))
        if "threads_publish" in url:
            return _FakeResponse({"id": "post-999"})
        if params.get("media_type") == "CAROUSEL":
            return _FakeResponse({"id": "carousel-container-1"})
        return _FakeResponse({"id": "item-1"})

    monkeypatch.setattr(threads_client.requests, "post", fake_post)
    monkeypatch.setattr(threads_client.requests, "get", lambda url, *, params, timeout: _FakeResponse({"status": "FINISHED"}))

    threads_client.create_and_publish_carousel_post(urls, reply_to_id="thread-1", reply_control="followers_only")

    carousel_call = next(c for c in captured if c.get("media_type") == "CAROUSEL")
    assert carousel_call["reply_to_id"] == "thread-1"
    assert carousel_call["reply_control"] == "followers_only"
    # The reply params belong on the carousel container, not the item containers.
    item_calls = [c for c in captured if c.get("media_type") == "IMAGE"]
    for call in item_calls:
        assert "reply_to_id" not in call
        assert "reply_control" not in call


# ---------------------------------------------------------------------------
# search_keyword_posts / search_locations / lookup_public_profile /
# get_pending_replies / manage_reply -- the 4 additional-permission
# capability functions.
# ---------------------------------------------------------------------------

def _with_valid_token():
    threads_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "user_id": "user-42",
        "obtained_at": threads_client.time.time(), "expires_at": threads_client.time.time() + 1000000,
    })


def test_search_keyword_posts_raises_without_a_valid_token():
    with pytest.raises(RuntimeError, match="No valid Threads access token"):
        threads_client.search_keyword_posts("bitcoin")


def test_search_keyword_posts_builds_the_expected_request_and_returns_data(monkeypatch):
    _with_valid_token()
    captured = {}

    def fake_get(url, *, params, timeout):
        captured["url"] = url
        captured["params"] = params
        return _FakeResponse({"data": [{"id": "post-1", "text": "bitcoin is up"}]})

    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    result = threads_client.search_keyword_posts(
        "bitcoin", search_type="RECENT", media_type="TEXT", author_username="cumdev", limit=10,
    )

    assert captured["url"] == f"{threads_client.API_BASE_URL}/{threads_client.API_VERSION}/keyword_search"
    assert captured["params"]["q"] == "bitcoin"
    assert captured["params"]["search_type"] == "RECENT"
    assert captured["params"]["media_type"] == "TEXT"
    assert captured["params"]["author_username"] == "cumdev"
    assert captured["params"]["limit"] == 10
    assert result == [{"id": "post-1", "text": "bitcoin is up"}]


def test_search_keyword_posts_raises_on_an_error_response(monkeypatch):
    _with_valid_token()
    monkeypatch.setattr(threads_client.requests, "get", lambda url, *, params, timeout: _FakeResponse({"error": "bad"}, status_code=400))
    with pytest.raises(Exception):
        threads_client.search_keyword_posts("bitcoin")


def test_search_locations_requires_a_query_or_coordinates():
    with pytest.raises(ValueError, match="query.*latitude.*longitude"):
        threads_client.search_locations()


def test_search_locations_raises_without_a_valid_token():
    with pytest.raises(RuntimeError, match="No valid Threads access token"):
        threads_client.search_locations(query="New York")


def test_search_locations_builds_the_expected_request_with_a_text_query(monkeypatch):
    _with_valid_token()
    captured = {}

    def fake_get(url, *, params, timeout):
        captured["url"] = url
        captured["params"] = params
        return _FakeResponse({"data": [{"id": 1, "name": "New York"}]})

    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    result = threads_client.search_locations(query="New York")

    assert captured["url"] == f"{threads_client.API_BASE_URL}/{threads_client.API_VERSION}/location_search"
    assert captured["params"]["q"] == "New York"
    assert "latitude" not in captured["params"]
    assert result == [{"id": 1, "name": "New York"}]


def test_search_locations_builds_the_expected_request_with_coordinates(monkeypatch):
    _with_valid_token()
    captured = {}

    def fake_get(url, *, params, timeout):
        captured["params"] = params
        return _FakeResponse({"data": []})

    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    threads_client.search_locations(latitude=40.7, longitude=-74.0)

    assert captured["params"]["latitude"] == 40.7
    assert captured["params"]["longitude"] == -74.0
    assert "q" not in captured["params"]


def test_lookup_public_profile_raises_without_a_valid_token():
    with pytest.raises(RuntimeError, match="No valid Threads access token"):
        threads_client.lookup_public_profile("meta")


def test_lookup_public_profile_returns_the_profile_data(monkeypatch):
    _with_valid_token()
    captured = {}

    def fake_get(url, *, params, timeout):
        captured["url"] = url
        captured["params"] = params
        return _FakeResponse({"username": "meta", "follower_count": 1000000})

    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    result = threads_client.lookup_public_profile("meta")

    assert captured["url"] == f"{threads_client.API_BASE_URL}/{threads_client.API_VERSION}/profile_lookup"
    assert captured["params"]["username"] == "meta"
    assert result == {"username": "meta", "follower_count": 1000000}


def test_lookup_public_profile_returns_none_on_404(monkeypatch):
    _with_valid_token()
    monkeypatch.setattr(threads_client.requests, "get", lambda url, *, params, timeout: _FakeResponse({}, status_code=404))
    assert threads_client.lookup_public_profile("nobody-real") is None


def test_get_pending_replies_raises_without_a_valid_token():
    with pytest.raises(RuntimeError, match="No valid Threads access token"):
        threads_client.get_pending_replies("media-1")


def test_get_pending_replies_builds_the_expected_request_and_returns_data(monkeypatch):
    _with_valid_token()
    captured = {}

    def fake_get(url, *, params, timeout):
        captured["url"] = url
        captured["params"] = params
        return _FakeResponse({"data": [{"id": "reply-1", "hide_status": "NOT_HIDDEN"}]})

    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    result = threads_client.get_pending_replies("media-1", approval_status="pending")

    assert captured["url"] == f"{threads_client.API_BASE_URL}/{threads_client.API_VERSION}/media-1/pending_replies"
    assert captured["params"]["approval_status"] == "pending"
    assert captured["params"]["reverse"] == "true"
    assert result == [{"id": "reply-1", "hide_status": "NOT_HIDDEN"}]


def test_manage_reply_raises_without_a_valid_token():
    with pytest.raises(RuntimeError, match="No valid Threads access token"):
        threads_client.manage_reply("reply-1", hide=True)


def test_manage_reply_hides_a_reply(monkeypatch):
    _with_valid_token()
    captured = {}

    def fake_post(url, *, params, timeout):
        captured["url"] = url
        captured["params"] = params
        return _FakeResponse({"success": True})

    monkeypatch.setattr(threads_client.requests, "post", fake_post)
    result = threads_client.manage_reply("reply-1", hide=True)

    assert captured["url"] == f"{threads_client.API_BASE_URL}/{threads_client.API_VERSION}/reply-1/manage_reply"
    assert captured["params"]["hide"] == "true"
    assert result is True


def test_manage_reply_unhides_a_reply(monkeypatch):
    _with_valid_token()
    captured = {}

    def fake_post(url, *, params, timeout):
        captured["params"] = params
        return _FakeResponse({"success": True})

    monkeypatch.setattr(threads_client.requests, "post", fake_post)
    threads_client.manage_reply("reply-1", hide=False)

    assert captured["params"]["hide"] == "false"


# ---------------------------------------------------------------------------
# _with_promo_tag -- every post through this client tags the bot's own
# site, see this module's own PROMO_URL comment.
# ---------------------------------------------------------------------------

def test_with_promo_tag_appends_the_url_to_normal_text():
    result = threads_client._with_promo_tag("real trade update")  # noqa: SLF001
    assert result == f"real trade update{threads_client._PROMO_TAG}"  # noqa: SLF001
    assert result.endswith(threads_client.PROMO_URL)


def test_with_promo_tag_on_empty_text_returns_just_the_url():
    assert threads_client._with_promo_tag("") == threads_client.PROMO_URL  # noqa: SLF001
    assert threads_client._with_promo_tag(None) == threads_client.PROMO_URL  # noqa: SLF001


def test_with_promo_tag_does_not_duplicate_an_already_tagged_caption():
    already_tagged = f"see the site: {threads_client.PROMO_URL}"
    assert threads_client._with_promo_tag(already_tagged) == already_tagged  # noqa: SLF001


def test_with_promo_tag_trims_long_text_so_the_tag_still_fits():
    long_text = "x" * 600
    result = threads_client._with_promo_tag(long_text)  # noqa: SLF001
    assert len(result) <= threads_client._THREADS_MAX_CHARS  # noqa: SLF001
    assert result.endswith(threads_client.PROMO_URL)
    assert result.startswith("x")


# ---------------------------------------------------------------------------
# list_recent_posts / durable posts archive -- backs the public
# /api/threads/posts (+ /api/threads/posts/sync) routes in app_kalshi.py.
# ---------------------------------------------------------------------------

def _post(post_id: str, text: str = "some post") -> dict:
    return {"id": post_id, "media_type": "TEXT_POST", "text": text, "timestamp": "2026-08-27T00:00:00+0000"}


def test_list_recent_posts_raises_without_a_valid_token():
    with pytest.raises(RuntimeError, match="No valid Threads access token"):
        threads_client.list_recent_posts()


def test_list_recent_posts_builds_the_expected_request_and_returns_data(monkeypatch):
    _with_valid_token()
    captured = {}

    def fake_get(url, *, params, timeout):
        captured["url"] = url
        captured["params"] = params
        return _FakeResponse({"data": [_post("p3"), _post("p2"), _post("p1")]})

    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    result = threads_client.list_recent_posts(limit=2)

    assert captured["url"] == f"{threads_client.API_BASE_URL}/{threads_client.API_VERSION}/user-42/threads"
    assert captured["params"]["fields"] == threads_client.LIST_POSTS_FIELDS
    assert [p["id"] for p in result] == ["p3", "p2"]  # capped at limit, newest-first order preserved


def test_list_recent_posts_caps_limit_at_50(monkeypatch):
    _with_valid_token()
    monkeypatch.setattr(threads_client.requests, "get", lambda url, *, params, timeout: _FakeResponse({"data": [_post(str(i)) for i in range(60)]}))
    result = threads_client.list_recent_posts(limit=999)
    assert len(result) == 50


def test_list_recent_posts_since_id_truncates_to_newer_posts(monkeypatch):
    _with_valid_token()
    monkeypatch.setattr(
        threads_client.requests, "get",
        lambda url, *, params, timeout: _FakeResponse({"data": [_post("p4"), _post("p3"), _post("p2"), _post("p1")]}),
    )
    result = threads_client.list_recent_posts(limit=50, since_id="p2")
    assert [p["id"] for p in result] == ["p4", "p3"]


def test_list_recent_posts_reuses_the_cache_within_the_ttl(monkeypatch):
    _with_valid_token()
    calls = {"n": 0}

    def fake_get(url, *, params, timeout):
        calls["n"] += 1
        return _FakeResponse({"data": [_post("p1")]})

    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    threads_client.list_recent_posts()
    threads_client.list_recent_posts()
    assert calls["n"] == 1


def test_list_recent_posts_refetches_after_the_cache_expires(monkeypatch):
    _with_valid_token()
    monkeypatch.setattr(threads_client, "_LIST_POSTS_CACHE_TTL_SEC", 0.0)
    calls = {"n": 0}

    def fake_get(url, *, params, timeout):
        calls["n"] += 1
        return _FakeResponse({"data": [_post("p1")]})

    monkeypatch.setattr(threads_client.requests, "get", fake_get)
    threads_client.list_recent_posts()
    threads_client.list_recent_posts()
    assert calls["n"] == 2


def test_get_posts_archive_returns_empty_without_hf_configured():
    assert threads_client.get_posts_archive() == []


def test_get_posts_archive_pulls_from_hf_once_and_caches(monkeypatch):
    monkeypatch.setattr(threads_client, "HF_API_KEY", "test-key")
    calls = {"n": 0}

    def fake_pull():
        calls["n"] += 1
        return [_post("p1")]

    monkeypatch.setattr(threads_client, "_pull_posts_archive_from_hf", fake_pull)
    assert threads_client.get_posts_archive() == [_post("p1")]
    assert threads_client.get_posts_archive() == [_post("p1")]
    assert calls["n"] == 1


def test_sync_posts_archive_merges_new_posts_ahead_of_the_existing_archive(monkeypatch):
    _with_valid_token()
    monkeypatch.setattr(threads_client, "HF_API_KEY", "test-key")
    monkeypatch.setattr(threads_client, "_pull_posts_archive_from_hf", lambda: [_post("p1")])
    pushed = {}
    monkeypatch.setattr(threads_client, "_push_posts_archive_to_hf", lambda archive: pushed.update(archive=archive))
    monkeypatch.setattr(
        threads_client.requests, "get",
        lambda url, *, params, timeout: _FakeResponse({"data": [_post("p3"), _post("p2"), _post("p1")]}),
    )

    result = threads_client.sync_posts_archive()

    assert result == {"new_posts": 2, "total_archived": 3}
    assert [p["id"] for p in pushed["archive"]] == ["p3", "p2", "p1"]
    assert [p["id"] for p in threads_client.get_posts_archive()] == ["p3", "p2", "p1"]


def test_sync_posts_archive_does_not_push_when_nothing_is_new(monkeypatch):
    _with_valid_token()
    monkeypatch.setattr(threads_client, "HF_API_KEY", "test-key")
    monkeypatch.setattr(threads_client, "_pull_posts_archive_from_hf", lambda: [_post("p1")])

    def fail_if_called(archive):
        raise AssertionError("must not push to HF when nothing changed")

    monkeypatch.setattr(threads_client, "_push_posts_archive_to_hf", fail_if_called)
    monkeypatch.setattr(threads_client.requests, "get", lambda url, *, params, timeout: _FakeResponse({"data": [_post("p1")]}))

    result = threads_client.sync_posts_archive()
    assert result == {"new_posts": 0, "total_archived": 1}


def test_sync_posts_archive_trims_to_the_max_size(monkeypatch):
    _with_valid_token()
    monkeypatch.setattr(threads_client, "HF_API_KEY", "test-key")
    monkeypatch.setattr(threads_client, "_POSTS_ARCHIVE_MAX_SIZE", 3)
    monkeypatch.setattr(threads_client, "_pull_posts_archive_from_hf", lambda: [_post("old1"), _post("old2")])
    monkeypatch.setattr(threads_client, "_push_posts_archive_to_hf", lambda archive: None)
    monkeypatch.setattr(
        threads_client.requests, "get",
        lambda url, *, params, timeout: _FakeResponse({"data": [_post("new2"), _post("new1")]}),
    )

    result = threads_client.sync_posts_archive()
    assert result["total_archived"] == 3
    assert [p["id"] for p in threads_client.get_posts_archive()] == ["new2", "new1", "old1"]


# ---------------------------------------------------------------------------
# _STATE_LOCK bounded acquisition -- real, confirmed production incident:
# a plain `with _STATE_LOCK:` (no timeout) let ONE stuck lock-holder freeze
# an entire gunicorn worker for 300s until Render's own watchdog SIGKILLed
# it, taking down every other request AND the background scheduler on that
# process. get_valid_access_token/get_user_id/_adopt_shared_rate_limit_from_hf/
# _note_rate_limited must all degrade to a cached/best-effort fallback
# instead of hanging when the lock is contended.
# ---------------------------------------------------------------------------
import threading  # noqa: E402


def _hold_state_lock_for(seconds: float) -> threading.Thread:
    """Simulates another thread stuck holding _STATE_LOCK -- starts a
    background thread that acquires it immediately and only releases after
    `seconds`, so a test can exercise the "lock busy" path deterministically."""
    ready = threading.Event()

    def _hold():
        threads_client._STATE_LOCK.acquire()  # noqa: SLF001
        ready.set()
        threads_client.time.sleep(seconds)
        threads_client._STATE_LOCK.release()  # noqa: SLF001

    t = threading.Thread(target=_hold, daemon=True)
    t.start()
    ready.wait(timeout=2)
    return t


def test_get_valid_access_token_falls_back_to_cache_when_the_lock_is_busy(monkeypatch):
    _with_valid_token()
    monkeypatch.setattr(threads_client, "_STATE_LOCK_ACQUIRE_TIMEOUT_SEC", 0.1)
    holder = _hold_state_lock_for(0.5)
    try:
        assert threads_client.get_valid_access_token() == "at-1"  # cached value, not a hang
    finally:
        holder.join(timeout=2)


def test_get_valid_access_token_returns_none_when_the_lock_is_busy_and_nothing_is_cached(monkeypatch):
    monkeypatch.setattr(threads_client, "_STATE_LOCK_ACQUIRE_TIMEOUT_SEC", 0.1)
    holder = _hold_state_lock_for(0.5)
    try:
        assert threads_client.get_valid_access_token() is None
    finally:
        holder.join(timeout=2)


def test_get_user_id_falls_back_to_cache_when_the_lock_is_busy(monkeypatch):
    _with_valid_token()
    monkeypatch.setattr(threads_client, "_STATE_LOCK_ACQUIRE_TIMEOUT_SEC", 0.1)
    holder = _hold_state_lock_for(0.5)
    try:
        assert threads_client.get_user_id() == "user-42"
    finally:
        holder.join(timeout=2)


def test_note_rate_limited_never_raises_when_the_lock_is_busy(monkeypatch):
    monkeypatch.setattr(threads_client, "_STATE_LOCK_ACQUIRE_TIMEOUT_SEC", 0.1)
    holder = _hold_state_lock_for(0.5)
    try:
        threads_client._note_rate_limited()  # noqa: SLF001 -- must not hang or raise
    finally:
        holder.join(timeout=2)


def test_adopt_shared_rate_limit_never_raises_when_the_lock_is_busy(monkeypatch):
    monkeypatch.setattr(threads_client, "_STATE_LOCK_ACQUIRE_TIMEOUT_SEC", 0.1)
    holder = _hold_state_lock_for(0.5)
    try:
        threads_client._adopt_shared_rate_limit_from_hf()  # noqa: SLF001 -- must not hang or raise
    finally:
        holder.join(timeout=2)


def test_get_valid_access_token_still_works_normally_once_the_lock_is_free(monkeypatch):
    """The bounded-acquire change must not break the ordinary, uncontended
    path -- a real regression risk any time a plain `with lock:` gets
    rewritten as an explicit acquire/release."""
    _with_valid_token()
    monkeypatch.setattr(threads_client, "_STATE_LOCK_ACQUIRE_TIMEOUT_SEC", 5.0)
    assert threads_client.get_valid_access_token() == "at-1"
    assert not threads_client._STATE_LOCK.locked()  # noqa: SLF001 -- released properly
