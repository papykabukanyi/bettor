"""Kalshi 15-minute event-contract markets client -- see kalshi_15m.py's
own module docstring for the product and the live-confirmed API surface
this mirrors. Mocks _request_json (the same low-level signed-request core
kalshi_perps.py already relies on, see test_kalshi_client.py for its own
coverage) rather than the network directly."""
from __future__ import annotations

import datetime as dt

import pytest

from data import kalshi_15m


def test_known_15m_series_covers_the_5_perps_coins():
    assert set(kalshi_15m.KNOWN_15M_SERIES.keys()) == {"BTC", "ETH", "SOL", "XRP", "DOGE"}
    assert kalshi_15m.KNOWN_15M_SERIES["BTC"] == "KXBTC15M"


def test_list_series_passes_the_category_filter(monkeypatch):
    captured = {}

    def fake(method, path, **kw):
        captured["method"], captured["path"], captured["params"] = method, path, kw.get("params")
        return {"series": [{"ticker": "KXBTC15M"}]}

    monkeypatch.setattr(kalshi_15m, "_request_json", fake)
    result = kalshi_15m.list_series(category="Crypto")
    assert captured["method"] == "GET"
    assert captured["path"] == "/series"
    assert captured["params"] == {"category": "Crypto"}
    assert result == [{"ticker": "KXBTC15M"}]


def test_get_series_returns_empty_dict_when_missing(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "_request_json", lambda *a, **kw: {})
    assert kalshi_15m.get_series("KXBTC15M") == {}


def test_list_open_markets_filters_by_series_and_status(monkeypatch):
    captured = {}

    def fake(method, path, **kw):
        captured["params"] = kw.get("params")
        return {"markets": [{"ticker": "KXBTC15M-1"}]}

    monkeypatch.setattr(kalshi_15m, "_request_json", fake)
    result = kalshi_15m.list_open_markets("KXBTC15M")
    assert captured["params"]["series_ticker"] == "KXBTC15M"
    assert captured["params"]["status"] == "open"
    assert result == [{"ticker": "KXBTC15M-1"}]


def test_get_current_window_market_returns_none_when_nothing_is_open(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "list_open_markets", lambda series_ticker: [])
    assert kalshi_15m.get_current_window_market("KXBTC15M") is None


def test_get_current_window_market_picks_the_soonest_closing_one(monkeypatch):
    """Defensive against a rare overlap right at a window boundary -- see
    this function's own docstring."""
    markets = [
        {"ticker": "later", "close_time": "2026-09-19T19:00:00Z"},
        {"ticker": "sooner", "close_time": "2026-09-19T18:45:00Z"},
    ]
    monkeypatch.setattr(kalshi_15m, "list_open_markets", lambda series_ticker: markets)
    result = kalshi_15m.get_current_window_market("KXBTC15M")
    assert result["ticker"] == "sooner"


def test_seconds_to_close_returns_none_without_a_close_time():
    assert kalshi_15m.seconds_to_close({}) is None
    assert kalshi_15m.seconds_to_close({"close_time": "not-a-date"}) is None


def test_seconds_to_close_computes_real_remaining_time():
    future = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)
    market = {"close_time": future.isoformat().replace("+00:00", "Z")}
    seconds = kalshi_15m.seconds_to_close(market)
    assert 290 < seconds < 310  # ~5 minutes, allowing for test execution time


def test_seconds_to_close_is_negative_for_an_already_closed_market():
    past = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)
    market = {"close_time": past.isoformat().replace("+00:00", "Z")}
    assert kalshi_15m.seconds_to_close(market) < 0


def test_get_portfolio_balance_calls_the_authenticated_endpoint(monkeypatch):
    captured = {}

    def fake(method, path, **kw):
        captured.update(method=method, path=path, auth=kw.get("auth"))
        return {"balance_dollars": "100.00"}

    monkeypatch.setattr(kalshi_15m, "_request_json", fake)
    result = kalshi_15m.get_portfolio_balance()
    assert captured == {"method": "GET", "path": "/portfolio/balance", "auth": True}
    assert result["balance_dollars"] == "100.00"


def test_get_balance_by_shard_omits_exchange_index_when_not_given(monkeypatch):
    captured = {}

    def fake(method, path, **kw):
        captured.update(method=method, path=path, params=kw.get("params"), auth=kw.get("auth"))
        return {"balance_dollars": "0.00"}

    monkeypatch.setattr(kalshi_15m, "_request_json", fake)
    kalshi_15m.get_balance_by_shard()
    assert captured == {"method": "GET", "path": "/portfolio/balance", "params": {}, "auth": True}


def test_get_balance_by_shard_passes_exchange_index_when_given(monkeypatch):
    captured = {}

    def fake(method, path, **kw):
        captured["params"] = kw.get("params")
        return {"balance_dollars": "0.00"}

    monkeypatch.setattr(kalshi_15m, "_request_json", fake)
    kalshi_15m.get_balance_by_shard(exchange_index=2)
    assert captured["params"] == {"exchange_index": 2}


def test_get_subaccount_balances_returns_the_balances_list(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "_request_json", lambda *a, **kw: {"balances": [{"exchange_index": 2, "balance": "0"}]})
    result = kalshi_15m.get_subaccount_balances()
    assert result == [{"exchange_index": 2, "balance": "0"}]


def test_get_subaccount_balances_handles_a_bare_list_response(monkeypatch):
    # Real, disclosed uncertainty in the docstring -- fall back to treating
    # the response itself as the list if it isn't wrapped in a "balances" key.
    monkeypatch.setattr(kalshi_15m, "_request_json", lambda *a, **kw: [{"exchange_index": 0, "balance": "6929"}])
    result = kalshi_15m.get_subaccount_balances()
    assert result == [{"exchange_index": 0, "balance": "6929"}]


def test_get_portfolio_positions_filters_by_ticker_when_given(monkeypatch):
    captured = {}

    def fake(method, path, **kw):
        captured["params"] = kw.get("params")
        return {"market_positions": []}

    monkeypatch.setattr(kalshi_15m, "_request_json", fake)
    kalshi_15m.get_portfolio_positions(ticker="KXBTC15M-1")
    assert captured["params"] == {"ticker": "KXBTC15M-1"}

    kalshi_15m.get_portfolio_positions()
    assert captured["params"] == {}


def test_get_orders_filters_by_ticker_and_status(monkeypatch):
    captured = {}

    def fake(method, path, **kw):
        captured["params"] = kw.get("params")
        return {"orders": []}

    monkeypatch.setattr(kalshi_15m, "_request_json", fake)
    kalshi_15m.get_orders(ticker="KXBTC15M-1", status="resting")
    assert captured["params"] == {"ticker": "KXBTC15M-1", "status": "resting"}


def test_create_order_rejects_an_invalid_side():
    with pytest.raises(ValueError, match="side must be"):
        kalshi_15m.create_order(ticker="KXBTC15M-1", side="yes", count=1, price=0.5, client_order_id="abc")


def test_create_order_builds_the_correct_payload_and_path(monkeypatch):
    captured = {}

    def fake(method, path, **kw):
        captured.update(method=method, path=path, payload=kw.get("payload"), auth=kw.get("auth"))
        return {"order_id": "o1"}

    monkeypatch.setattr(kalshi_15m, "_request_json", fake)
    result = kalshi_15m.create_order(
        ticker="KXBTC15M-1", side="bid", count=2, price=0.46, client_order_id="abc-123",
    )
    assert captured["method"] == "POST"
    assert captured["path"] == "/portfolio/events/orders"
    assert captured["auth"] is True
    payload = captured["payload"]
    assert payload["ticker"] == "KXBTC15M-1"
    assert payload["side"] == "bid"
    assert payload["count"] == "2.00"
    assert payload["price"] == "0.4600"
    assert payload["client_order_id"] == "abc-123"
    assert payload["time_in_force"] == "immediate_or_cancel"
    assert payload["self_trade_prevention_type"] == "taker_at_cross"
    assert payload["post_only"] is False
    assert payload["reduce_only"] is False
    assert result["order_id"] == "o1"


def test_cancel_order_hits_the_correct_path(monkeypatch):
    captured = {}

    def fake(method, path, **kw):
        captured.update(method=method, path=path, auth=kw.get("auth"))
        return {"order": {"status": "canceled"}}

    monkeypatch.setattr(kalshi_15m, "_request_json", fake)
    kalshi_15m.cancel_order("o1")
    assert captured == {"method": "DELETE", "path": "/portfolio/events/orders/o1", "auth": True}


def test_get_market_returns_none_when_no_market_matches(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "_request_json", lambda *a, **kw: {"markets": []})
    assert kalshi_15m.get_market("KXBTC15M-1") is None


def test_get_market_returns_the_single_matching_market(monkeypatch):
    captured = {}

    def fake(method, path, **kw):
        captured["params"] = kw.get("params")
        return {"markets": [{"ticker": "KXBTC15M-1", "result": "yes"}]}

    monkeypatch.setattr(kalshi_15m, "_request_json", fake)
    result = kalshi_15m.get_market("KXBTC15M-1")
    assert captured["params"] == {"tickers": "KXBTC15M-1"}
    assert result["result"] == "yes"


def test_known_15m_metals_series_covers_gold_silver_copper():
    assert set(kalshi_15m.KNOWN_15M_METALS_SERIES.keys()) == {"GOLD", "SILVER", "COPPER"}
    assert kalshi_15m.KNOWN_15M_METALS_SERIES["GOLD"] == "KXGOLD15M"
    assert kalshi_15m.KNOWN_15M_METALS_SERIES["SILVER"] == "KXSILVER15M"
    assert kalshi_15m.KNOWN_15M_METALS_SERIES["COPPER"] == "KXCOPPER15M"
    # No overlap with the crypto universe -- coin/series identity must
    # stay unambiguous when the strategy layer merges both mappings.
    assert not (set(kalshi_15m.KNOWN_15M_METALS_SERIES) & set(kalshi_15m.KNOWN_15M_SERIES))
