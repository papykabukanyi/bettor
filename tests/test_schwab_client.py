"""Schwab OAuth client -- authorization URL construction, token exchange/
refresh, and the durable HF token mirror (Render's disk is ephemeral, so a
restart must not force a fresh interactive login every time). All network
calls mocked; never touches the real Schwab API or HF."""
from __future__ import annotations

import pytest

from data import schwab_client


class _FakeResponse:
    def __init__(self, json_body, status_code=200, headers=None):
        self._json_body = json_body
        self.status_code = status_code
        self.headers = headers or {}

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


# ── 429 retry/backoff -- Schwab enforces a real 120 req/min ceiling, and a
# 429 there is expected occasionally under normal load, not just a bug
# symptom, so `get()` retries with backoff instead of failing the caller
# outright the first time it happens.
def test_get_retries_after_a_429_and_succeeds(monkeypatch):
    schwab_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "refresh_token": "rt-1",
        "obtained_at": schwab_client.time.time(), "expires_at": schwab_client.time.time() + 1000,
    })
    monkeypatch.setattr(schwab_client, "_RATE_LIMIT_RETRY_BACKOFF_SEC", 0.0)
    monkeypatch.setattr(schwab_client.time, "sleep", lambda _s: None)
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append(1)
        if len(calls) == 1:
            return _FakeResponse({}, status_code=429)
        return _FakeResponse({"candles": []}, status_code=200)

    monkeypatch.setattr(schwab_client.requests, "get", fake_get)
    result = schwab_client.get("/marketdata/v1/pricehistory", params={"symbol": "AAPL"})
    assert result == {"candles": []}
    assert len(calls) == 2


def test_get_respects_retry_after_header(monkeypatch):
    schwab_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "refresh_token": "rt-1",
        "obtained_at": schwab_client.time.time(), "expires_at": schwab_client.time.time() + 1000,
    })
    slept = []
    monkeypatch.setattr(schwab_client.time, "sleep", lambda s: slept.append(s))
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append(1)
        if len(calls) == 1:
            return _FakeResponse({}, status_code=429, headers={"Retry-After": "7"})
        return _FakeResponse({"candles": []}, status_code=200)

    monkeypatch.setattr(schwab_client.requests, "get", fake_get)
    schwab_client.get("/marketdata/v1/pricehistory")
    assert slept == [7.0]


def test_get_raises_after_exhausting_retries_on_persistent_429(monkeypatch):
    schwab_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "refresh_token": "rt-1",
        "obtained_at": schwab_client.time.time(), "expires_at": schwab_client.time.time() + 1000,
    })
    monkeypatch.setattr(schwab_client, "_RATE_LIMIT_RETRY_BACKOFF_SEC", 0.0)
    monkeypatch.setattr(schwab_client.time, "sleep", lambda _s: None)
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append(1)
        return _FakeResponse({}, status_code=429)

    monkeypatch.setattr(schwab_client.requests, "get", fake_get)
    with pytest.raises(RuntimeError, match="HTTP 429"):
        schwab_client.get("/marketdata/v1/pricehistory")
    assert len(calls) == schwab_client._RATE_LIMIT_RETRY_ATTEMPTS  # noqa: SLF001


def _authenticate():
    schwab_client._token_cache.update({  # noqa: SLF001
        "access_token": "at-1", "refresh_token": "rt-1",
        "obtained_at": schwab_client.time.time(), "expires_at": schwab_client.time.time() + 1000,
    })


class _FakePostResponse:
    def __init__(self, location, status_code=201):
        self.headers = {"Location": location}
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_build_bracket_order_shape_for_a_long():
    order = schwab_client.build_bracket_order(
        symbol="AAPL", quantity=10, entry_instruction="BUY", exit_instruction="SELL",
        take_profit_price=101.0, stop_loss_price=98.0,
    )
    assert order["orderType"] == "MARKET"
    assert order["orderStrategyType"] == "TRIGGER"
    assert order["orderLegCollection"][0]["instruction"] == "BUY"
    assert order["orderLegCollection"][0]["instrument"]["symbol"] == "AAPL"

    oco = order["childOrderStrategies"][0]
    assert oco["orderStrategyType"] == "OCO"
    tp, sl = oco["childOrderStrategies"]
    assert tp["orderType"] == "LIMIT" and tp["price"] == "101.00"
    assert sl["orderType"] == "STOP" and sl["stopPrice"] == "98.00"
    assert tp["orderLegCollection"][0]["instruction"] == "SELL"
    assert sl["orderLegCollection"][0]["instruction"] == "SELL"


def test_build_bracket_order_uses_limit_entry_when_a_price_is_given():
    order = schwab_client.build_bracket_order(
        symbol="AAPL", quantity=10, entry_instruction="BUY", exit_instruction="SELL",
        take_profit_price=101.0, stop_loss_price=98.0, entry_price=99.5,
    )
    assert order["orderType"] == "LIMIT"
    assert order["price"] == "99.50"


def test_get_account_hash_fetches_and_caches(monkeypatch):
    _authenticate()
    monkeypatch.setattr(schwab_client, "_account_hash_cache", {"hash": None, "computed_at": 0.0})
    calls = {"n": 0}

    def fake_get(path, *, params=None):
        calls["n"] += 1
        assert path == "/trader/v1/accounts/accountNumbers"
        return [{"accountNumber": "123", "hashValue": "hash-abc"}]

    monkeypatch.setattr(schwab_client, "get", fake_get)
    assert schwab_client.get_account_hash() == "hash-abc"
    assert schwab_client.get_account_hash() == "hash-abc"
    assert calls["n"] == 1  # second call served from cache


def test_get_account_hash_selects_the_configured_account_number(monkeypatch):
    _authenticate()
    monkeypatch.setattr(schwab_client, "_account_hash_cache", {"hash": None, "computed_at": 0.0})
    monkeypatch.setattr(schwab_client, "SCHWAB_ACCOUNT_NUMBER", "456")
    monkeypatch.setattr(schwab_client, "get", lambda path, params=None: [
        {"accountNumber": "123", "hashValue": "hash-first"},
        {"accountNumber": "456", "hashValue": "hash-chosen"},
    ])
    assert schwab_client.get_account_hash() == "hash-chosen"


def test_get_account_hash_raises_with_no_linked_accounts(monkeypatch):
    _authenticate()
    monkeypatch.setattr(schwab_client, "_account_hash_cache", {"hash": None, "computed_at": 0.0})
    monkeypatch.setattr(schwab_client, "get", lambda path, params=None: [])
    with pytest.raises(RuntimeError, match="no linked accounts"):
        schwab_client.get_account_hash()


def test_place_order_extracts_order_id_from_location_header(monkeypatch):
    _authenticate()
    monkeypatch.setattr(schwab_client, "_account_hash_cache", {"hash": "hash-abc", "computed_at": schwab_client.time.time()})
    captured = {}

    def fake_post(url, *, headers, json, timeout):
        captured["url"] = url
        captured["json"] = json
        return _FakePostResponse("https://api.schwabapi.com/trader/v1/accounts/hash-abc/orders/9999")

    monkeypatch.setattr(schwab_client.requests, "post", fake_post)
    order_id = schwab_client.place_order({"orderType": "MARKET"})

    assert order_id == "9999"
    assert captured["url"] == f"{schwab_client.API_BASE_URL}/trader/v1/accounts/hash-abc/orders"
    assert captured["json"] == {"orderType": "MARKET"}


def test_place_order_raises_if_no_order_id_in_location(monkeypatch):
    _authenticate()
    monkeypatch.setattr(schwab_client, "_account_hash_cache", {"hash": "hash-abc", "computed_at": schwab_client.time.time()})
    monkeypatch.setattr(schwab_client.requests, "post", lambda url, **kw: _FakePostResponse(""))
    with pytest.raises(RuntimeError, match="no order ID"):
        schwab_client.place_order({"orderType": "MARKET"})


def test_get_order_and_cancel_order_use_the_account_hash(monkeypatch):
    _authenticate()
    monkeypatch.setattr(schwab_client, "_account_hash_cache", {"hash": "hash-abc", "computed_at": schwab_client.time.time()})
    captured = {}

    def fake_get(url, *, headers, params, timeout):
        captured["get_url"] = url
        return _FakeResponse({"status": "FILLED"})

    def fake_delete(url, *, headers, timeout):
        captured["delete_url"] = url
        return _FakePostResponse("", status_code=200)

    monkeypatch.setattr(schwab_client.requests, "get", fake_get)
    monkeypatch.setattr(schwab_client.requests, "delete", fake_delete)

    order = schwab_client.get_order("555")
    assert order == {"status": "FILLED"}
    assert captured["get_url"] == f"{schwab_client.API_BASE_URL}/trader/v1/accounts/hash-abc/orders/555"

    schwab_client.cancel_order("555")
    assert captured["delete_url"] == f"{schwab_client.API_BASE_URL}/trader/v1/accounts/hash-abc/orders/555"


def test_get_quote_unwraps_the_symbols_own_quote_object(monkeypatch):
    _authenticate()

    def fake_get(url, *, headers, params, timeout):
        return _FakeResponse({"AAPL": {"quote": {"lastPrice": 150.25}}})

    monkeypatch.setattr(schwab_client.requests, "get", fake_get)
    quote = schwab_client.get_quote("AAPL")
    assert quote == {"lastPrice": 150.25}


def test_get_quote_returns_empty_dict_for_an_unknown_symbol(monkeypatch):
    _authenticate()
    monkeypatch.setattr(schwab_client.requests, "get", lambda url, **kw: _FakeResponse({}))
    assert schwab_client.get_quote("ZZZZ") == {}


def test_get_account_balance_uses_the_account_hash(monkeypatch):
    _authenticate()
    monkeypatch.setattr(schwab_client, "_account_hash_cache", {"hash": "hash-abc", "computed_at": schwab_client.time.time()})
    captured = {}

    def fake_get(url, *, headers, params, timeout):
        captured["url"] = url
        return _FakeResponse({"securitiesAccount": {"currentBalances": {"cashBalance": 100.0}}})

    monkeypatch.setattr(schwab_client.requests, "get", fake_get)
    balance = schwab_client.get_account_balance()
    assert balance == {"cashBalance": 100.0}
    assert captured["url"] == f"{schwab_client.API_BASE_URL}/trader/v1/accounts/hash-abc"
