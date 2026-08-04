"""Alpaca API client -- simple API-key/secret auth (no OAuth, unlike the
former Schwab client), trading endpoints (account/positions/orders/assets/
clock) and market-data endpoints (bars/quotes). All network calls mocked;
never touches the real Alpaca API or HF."""
from __future__ import annotations

import requests
import pytest

from data import alpaca_client


class _FakeResponse:
    def __init__(self, json_body, status_code=200, headers=None):
        self._json_body = json_body
        self.status_code = status_code
        self.headers = headers or {}
        self.content = b"1" if json_body is not None else b""

    def json(self):
        return self._json_body

    def raise_for_status(self):
        if self.status_code >= 400:
            err = requests.exceptions.HTTPError(f"HTTP {self.status_code}")
            err.response = self
            raise err


@pytest.fixture(autouse=True)
def _isolated_client_state(monkeypatch):
    monkeypatch.setattr(alpaca_client, "API_KEY_ID", "test-key-id")
    monkeypatch.setattr(alpaca_client, "API_SECRET_KEY", "test-secret-key")
    yield


def test_is_configured_true_when_both_credentials_present():
    assert alpaca_client.is_configured() is True


def test_is_configured_false_when_either_credential_missing(monkeypatch):
    monkeypatch.setattr(alpaca_client, "API_SECRET_KEY", "")
    assert alpaca_client.is_configured() is False


def test_auth_headers_raises_when_not_configured(monkeypatch):
    monkeypatch.setattr(alpaca_client, "API_KEY_ID", "")
    with pytest.raises(RuntimeError, match="not configured"):
        alpaca_client._auth_headers()  # noqa: SLF001


def test_auth_headers_shape():
    headers = alpaca_client._auth_headers()  # noqa: SLF001
    assert headers == {"APCA-API-KEY-ID": "test-key-id", "APCA-API-SECRET-KEY": "test-secret-key"}


def test_get_account_hits_trading_base_url(monkeypatch):
    captured = {}

    def fake_get(url, *, headers, params, timeout):
        captured["url"] = url
        captured["headers"] = headers
        return _FakeResponse({"cash": "100.00", "buying_power": "200.00"})

    monkeypatch.setattr(alpaca_client.requests, "get", fake_get)
    account = alpaca_client.get_account()
    assert account["cash"] == "100.00"
    assert captured["url"] == f"{alpaca_client.TRADING_BASE_URL}/v2/account"
    assert captured["headers"]["APCA-API-KEY-ID"] == "test-key-id"


def test_get_clock(monkeypatch):
    monkeypatch.setattr(alpaca_client.requests, "get", lambda url, **kw: _FakeResponse({"is_open": True, "next_close": "x"}))
    clock = alpaca_client.get_clock()
    assert clock["is_open"] is True


def test_get_positions_returns_list(monkeypatch):
    monkeypatch.setattr(alpaca_client.requests, "get", lambda url, **kw: _FakeResponse([{"symbol": "AAPL"}]))
    assert alpaca_client.get_positions() == [{"symbol": "AAPL"}]


def test_get_positions_defensive_against_non_list_response(monkeypatch):
    monkeypatch.setattr(alpaca_client.requests, "get", lambda url, **kw: _FakeResponse({"unexpected": "shape"}))
    assert alpaca_client.get_positions() == []


def test_get_position_returns_none_on_404(monkeypatch):
    monkeypatch.setattr(alpaca_client.requests, "get", lambda url, **kw: _FakeResponse({"code": 40410000}, status_code=404))
    assert alpaca_client.get_position("AAPL") is None


def test_get_position_reraises_non_404_errors(monkeypatch):
    monkeypatch.setattr(alpaca_client.requests, "get", lambda url, **kw: _FakeResponse({}, status_code=500))
    with pytest.raises(requests.exceptions.HTTPError):
        alpaca_client.get_position("AAPL")


def test_get_position_returns_position_when_open(monkeypatch):
    monkeypatch.setattr(alpaca_client.requests, "get", lambda url, **kw: _FakeResponse({"symbol": "AAPL", "qty": "10"}))
    assert alpaca_client.get_position("AAPL") == {"symbol": "AAPL", "qty": "10"}


def test_close_position_deletes_the_position(monkeypatch):
    captured = {}

    def fake_delete(url, *, headers, timeout):
        captured["url"] = url
        return _FakeResponse({"id": "order-1"})

    monkeypatch.setattr(alpaca_client.requests, "delete", fake_delete)
    result = alpaca_client.close_position("AAPL")
    assert result == {"id": "order-1"}
    assert captured["url"] == f"{alpaca_client.TRADING_BASE_URL}/v2/positions/AAPL"


def test_get_assets_filters_by_status_and_class(monkeypatch):
    captured = {}

    def fake_get(url, *, headers, params, timeout):
        captured["params"] = params
        return _FakeResponse([{"symbol": "AAPL", "tradable": True}])

    monkeypatch.setattr(alpaca_client.requests, "get", fake_get)
    assets = alpaca_client.get_assets()
    assert assets == [{"symbol": "AAPL", "tradable": True}]
    assert captured["params"] == {"status": "active", "asset_class": "us_equity"}


def test_get_bars_single_symbol_single_page(monkeypatch):
    captured = {}

    def fake_get(url, *, headers, params, timeout):
        captured["url"] = url
        captured["params"] = dict(params)
        return _FakeResponse({"bars": {"AAPL": [{"t": "2024-01-01T00:00:00Z", "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 100}]}, "next_page_token": None})

    monkeypatch.setattr(alpaca_client.requests, "get", fake_get)
    bars = alpaca_client.get_bars(["AAPL"], timeframe="1Min")
    assert bars == {"AAPL": [{"t": "2024-01-01T00:00:00Z", "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 100}]}
    assert captured["url"] == f"{alpaca_client.DATA_BASE_URL}/v2/stocks/bars"
    assert captured["params"]["symbols"] == "AAPL"
    assert captured["params"]["feed"] == alpaca_client.DATA_FEED


def test_get_bars_multi_symbol_joins_comma_separated(monkeypatch):
    captured = {}

    def fake_get(url, *, headers, params, timeout):
        captured["params"] = dict(params)
        return _FakeResponse({"bars": {"AAPL": [], "MSFT": []}, "next_page_token": None})

    monkeypatch.setattr(alpaca_client.requests, "get", fake_get)
    alpaca_client.get_bars(["AAPL", "MSFT"])
    assert captured["params"]["symbols"] == "AAPL,MSFT"


def test_get_bars_paginates_via_next_page_token(monkeypatch):
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append(dict(params))
        if "page_token" not in params:
            return _FakeResponse({"bars": {"AAPL": [{"t": "2024-01-01T00:00:00Z", "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}]}, "next_page_token": "tok-2"})
        return _FakeResponse({"bars": {"AAPL": [{"t": "2024-01-02T00:00:00Z", "o": 2, "h": 2, "l": 2, "c": 2, "v": 2}]}, "next_page_token": None})

    monkeypatch.setattr(alpaca_client.requests, "get", fake_get)
    bars = alpaca_client.get_bars(["AAPL"])
    assert len(bars["AAPL"]) == 2
    assert len(calls) == 2
    assert calls[1]["page_token"] == "tok-2"


def test_get_latest_quote_unwraps_the_quote_object(monkeypatch):
    monkeypatch.setattr(alpaca_client.requests, "get", lambda url, **kw: _FakeResponse({"quote": {"ap": 150.5, "bp": 150.25}}))
    quote = alpaca_client.get_latest_quote("AAPL")
    assert quote == {"ap": 150.5, "bp": 150.25}


def test_get_latest_quote_returns_empty_dict_when_absent(monkeypatch):
    monkeypatch.setattr(alpaca_client.requests, "get", lambda url, **kw: _FakeResponse({}))
    assert alpaca_client.get_latest_quote("ZZZZ") == {}


def test_get_crypto_bars_uses_the_v1beta3_crypto_path(monkeypatch):
    captured = {}

    def fake_get(url, *, headers, params, timeout):
        captured["url"] = url
        captured["params"] = dict(params)
        return _FakeResponse({"bars": {"BTC/USD": [{"t": "2024-01-01T00:00:00Z", "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}]}, "next_page_token": None})

    monkeypatch.setattr(alpaca_client.requests, "get", fake_get)
    bars = alpaca_client.get_crypto_bars(["BTC/USD"], timeframe="1Min")
    assert captured["url"] == f"{alpaca_client.DATA_BASE_URL}/v1beta3/crypto/us/bars"
    assert captured["params"]["symbols"] == "BTC/USD"
    assert "feed" not in captured["params"]  # crypto has no separate feed tiers, unlike stocks
    assert len(bars["BTC/USD"]) == 1


def test_get_crypto_bars_paginates(monkeypatch):
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append(dict(params))
        if "page_token" not in params:
            return _FakeResponse({"bars": {"BTC/USD": [{"t": "2024-01-01T00:00:00Z", "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}]}, "next_page_token": "tok-2"})
        return _FakeResponse({"bars": {"BTC/USD": [{"t": "2024-01-02T00:00:00Z", "o": 2, "h": 2, "l": 2, "c": 2, "v": 2}]}, "next_page_token": None})

    monkeypatch.setattr(alpaca_client.requests, "get", fake_get)
    bars = alpaca_client.get_crypto_bars(["BTC/USD"])
    assert len(bars["BTC/USD"]) == 2
    assert len(calls) == 2


def test_get_crypto_latest_quote_unwraps_the_symbol(monkeypatch):
    captured = {}

    def fake_get(url, *, headers, params, timeout):
        captured["url"] = url
        captured["params"] = params
        return _FakeResponse({"quotes": {"BTC/USD": {"ap": 65000.5, "bp": 64999.0}}})

    monkeypatch.setattr(alpaca_client.requests, "get", fake_get)
    quote = alpaca_client.get_crypto_latest_quote("BTC/USD")
    assert quote == {"ap": 65000.5, "bp": 64999.0}
    assert captured["url"] == f"{alpaca_client.DATA_BASE_URL}/v1beta3/crypto/us/latest/quotes"
    assert captured["params"]["symbols"] == "BTC/USD"


def test_get_crypto_latest_quote_returns_empty_dict_when_absent(monkeypatch):
    monkeypatch.setattr(alpaca_client.requests, "get", lambda url, **kw: _FakeResponse({"quotes": {}}))
    assert alpaca_client.get_crypto_latest_quote("ZZZ/USD") == {}


def test_build_crypto_order_uses_notional_by_default():
    order = alpaca_client.build_crypto_order(symbol="BTC/USD", side="buy", notional=50.0)
    assert order == {"symbol": "BTC/USD", "side": "buy", "type": "market", "time_in_force": "gtc", "notional": "50.00"}


def test_build_crypto_order_uses_qty_when_notional_not_given():
    order = alpaca_client.build_crypto_order(symbol="BTC/USD", side="sell", qty=0.001234567)
    assert order["qty"] == "0.001234567"
    assert "notional" not in order


def test_get_option_contracts_filters_by_underlying_and_expiration(monkeypatch):
    captured = {}

    def fake_get(url, *, headers, params, timeout):
        captured["url"] = url
        captured["params"] = dict(params)
        return _FakeResponse({"option_contracts": [{"symbol": "AAPL250620C00100000"}], "next_page_token": None})

    monkeypatch.setattr(alpaca_client.requests, "get", fake_get)
    contracts = alpaca_client.get_option_contracts(
        underlying_symbols=["AAPL"], expiration_date_gte="2025-06-01", expiration_date_lte="2025-06-30", option_type="call",
    )
    assert contracts == [{"symbol": "AAPL250620C00100000"}]
    assert captured["url"] == f"{alpaca_client.TRADING_BASE_URL}/v2/options/contracts"
    assert captured["params"]["underlying_symbols"] == "AAPL"
    assert captured["params"]["type"] == "call"
    assert captured["params"]["expiration_date_gte"] == "2025-06-01"


def test_get_option_contracts_paginates(monkeypatch):
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append(dict(params))
        if "page_token" not in params:
            return _FakeResponse({"option_contracts": [{"symbol": "A"}], "next_page_token": "tok-2"})
        return _FakeResponse({"option_contracts": [{"symbol": "B"}], "next_page_token": None})

    monkeypatch.setattr(alpaca_client.requests, "get", fake_get)
    contracts = alpaca_client.get_option_contracts(underlying_symbols=["AAPL"])
    assert contracts == [{"symbol": "A"}, {"symbol": "B"}]
    assert len(calls) == 2


def test_get_option_latest_quote_unwraps_the_symbol(monkeypatch):
    monkeypatch.setattr(alpaca_client.requests, "get", lambda url, **kw: _FakeResponse({"quotes": {"AAPL250620C00100000": {"ap": 2.5, "bp": 2.3}}}))
    quote = alpaca_client.get_option_latest_quote("AAPL250620C00100000")
    assert quote == {"ap": 2.5, "bp": 2.3}


def test_build_option_order_shape():
    order = alpaca_client.build_option_order(symbol="AAPL250620C00100000", side="buy", qty=2)
    assert order == {"symbol": "AAPL250620C00100000", "qty": "2", "side": "buy", "type": "market", "time_in_force": "day"}


def test_build_bracket_order_shape_for_a_buy():
    order = alpaca_client.build_bracket_order(
        symbol="AAPL", quantity=10, side="buy", take_profit_price=101.0, stop_loss_price=98.0,
    )
    assert order["symbol"] == "AAPL"
    assert order["qty"] == "10"
    assert order["side"] == "buy"
    assert order["type"] == "market"
    assert order["order_class"] == "bracket"
    assert order["take_profit"]["limit_price"] == "101.00"
    assert order["stop_loss"]["stop_price"] == "98.00"
    assert order["stop_loss"]["limit_price"] == "98.00"  # defaults to stop price when not given


def test_build_bracket_order_honors_explicit_stop_limit_price():
    order = alpaca_client.build_bracket_order(
        symbol="AAPL", quantity=10, side="buy", take_profit_price=101.0,
        stop_loss_price=98.0, stop_loss_limit_price=97.5,
    )
    assert order["stop_loss"]["limit_price"] == "97.50"


def test_build_bracket_order_truncates_fractional_qty_to_whole_shares():
    order = alpaca_client.build_bracket_order(
        symbol="AAPL", quantity=10.9, side="buy", take_profit_price=101.0, stop_loss_price=98.0,
    )
    assert order["qty"] == "10"


def test_build_extended_hours_limit_order_shape_for_a_buy():
    order = alpaca_client.build_extended_hours_limit_order(
        symbol="AAPL", quantity=10, side="buy", limit_price=100.25,
    )
    assert order["symbol"] == "AAPL"
    assert order["qty"] == "10"
    assert order["side"] == "buy"
    assert order["type"] == "limit"
    assert order["extended_hours"] is True
    assert order["time_in_force"] == "day"
    assert order["limit_price"] == "100.25"
    assert "order_class" not in order  # Alpaca rejects brackets outside regular hours


def test_build_extended_hours_limit_order_truncates_fractional_qty_to_whole_shares():
    order = alpaca_client.build_extended_hours_limit_order(
        symbol="AAPL", quantity=10.9, side="buy", limit_price=100.0,
    )
    assert order["qty"] == "10"


def test_place_order_extracts_id_from_json_body(monkeypatch):
    captured = {}

    def fake_post(url, *, headers, json, timeout):
        captured["url"] = url
        captured["json"] = json
        return _FakeResponse({"id": "order-123", "status": "accepted"})

    monkeypatch.setattr(alpaca_client.requests, "post", fake_post)
    order_id = alpaca_client.place_order({"symbol": "AAPL"})
    assert order_id == "order-123"
    assert captured["url"] == f"{alpaca_client.TRADING_BASE_URL}/v2/orders"
    assert captured["json"] == {"symbol": "AAPL"}


def test_place_order_raises_if_no_id_in_response(monkeypatch):
    monkeypatch.setattr(alpaca_client.requests, "post", lambda url, **kw: _FakeResponse({}))
    with pytest.raises(RuntimeError, match="no order id"):
        alpaca_client.place_order({"symbol": "AAPL"})


def test_get_order_and_cancel_order(monkeypatch):
    captured = {}

    def fake_get(url, *, headers, params, timeout):
        captured["get_url"] = url
        return _FakeResponse({"id": "555", "status": "filled"})

    def fake_delete(url, *, headers, timeout):
        captured["delete_url"] = url
        return _FakeResponse(None, status_code=204)

    monkeypatch.setattr(alpaca_client.requests, "get", fake_get)
    monkeypatch.setattr(alpaca_client.requests, "delete", fake_delete)

    order = alpaca_client.get_order("555")
    assert order["status"] == "filled"
    assert captured["get_url"] == f"{alpaca_client.TRADING_BASE_URL}/v2/orders/555"

    alpaca_client.cancel_order("555")
    assert captured["delete_url"] == f"{alpaca_client.TRADING_BASE_URL}/v2/orders/555"


def test_get_orders_passes_status_filter(monkeypatch):
    captured = {}

    def fake_get(url, *, headers, params, timeout):
        captured["params"] = params
        return _FakeResponse([{"id": "1"}])

    monkeypatch.setattr(alpaca_client.requests, "get", fake_get)
    orders = alpaca_client.get_orders(status="open")
    assert orders == [{"id": "1"}]
    assert captured["params"]["status"] == "open"


# ── 429 retry/backoff -- Alpaca's free tier enforces a real 200 req/min
# ceiling, and a 429 there is expected occasionally under normal load, not
# just a bug symptom, so the retry helper backs off instead of failing the
# caller outright the first time it happens.
def test_get_retries_after_a_429_and_succeeds(monkeypatch):
    monkeypatch.setattr(alpaca_client, "_RATE_LIMIT_RETRY_BACKOFF_SEC", 0.0)
    monkeypatch.setattr(alpaca_client.time, "sleep", lambda _s: None)
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append(1)
        if len(calls) == 1:
            return _FakeResponse({}, status_code=429)
        return _FakeResponse({"cash": "1.0"}, status_code=200)

    monkeypatch.setattr(alpaca_client.requests, "get", fake_get)
    result = alpaca_client.get_account()
    assert result == {"cash": "1.0"}
    assert len(calls) == 2


def test_get_respects_retry_after_header(monkeypatch):
    slept = []
    monkeypatch.setattr(alpaca_client.time, "sleep", lambda s: slept.append(s))
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append(1)
        if len(calls) == 1:
            return _FakeResponse({}, status_code=429, headers={"Retry-After": "7"})
        return _FakeResponse({"cash": "1.0"}, status_code=200)

    monkeypatch.setattr(alpaca_client.requests, "get", fake_get)
    alpaca_client.get_account()
    assert slept == [7.0]


def test_get_raises_after_exhausting_retries_on_persistent_429(monkeypatch):
    monkeypatch.setattr(alpaca_client, "_RATE_LIMIT_RETRY_BACKOFF_SEC", 0.0)
    monkeypatch.setattr(alpaca_client.time, "sleep", lambda _s: None)
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append(1)
        return _FakeResponse({}, status_code=429)

    monkeypatch.setattr(alpaca_client.requests, "get", fake_get)
    with pytest.raises(requests.exceptions.HTTPError):
        alpaca_client.get_account()
    assert len(calls) == alpaca_client._RATE_LIMIT_RETRY_ATTEMPTS  # noqa: SLF001
