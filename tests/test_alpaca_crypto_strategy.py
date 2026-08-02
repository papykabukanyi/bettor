"""Alpaca crypto strategy decision logic. Mirrors test_alpaca_strategy.py's
discipline; focuses on what's genuinely different here: notional-based
position sizing (no whole-share affordability problem), no market-hours
gating, and plain market close orders instead of a bracket-order
reconciliation (crypto has no bracket orders on Alpaca at all, so there's
no "did the broker already close this" race to check for)."""
from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from data import alpaca_client, alpaca_crypto_data, alpaca_crypto_model, threads_post, alpaca_crypto_strategy as strat


def _row(**overrides):
    base = {
        "symbol": "BTC/USD", "current_price": 65000.0, "short_ma": 65200.0,
        "dollar_volume_z": 2.0, "volatility_5": 0.002, "volatility_30": 0.001,
    }
    base.update(overrides)
    return base


def test_entry_requires_an_unusual_volume_spike():
    should_enter, reason = strat.decide_entry_technical(_row(dollar_volume_z=0.5))
    assert not should_enter
    assert "volume" in reason


def test_entry_fires_when_all_gates_pass():
    should_enter, reason = strat.decide_entry_technical(_row())
    assert should_enter


def test_evaluate_candidate_technical_only_fallback_without_a_model():
    result = strat.evaluate_candidate(_row(), model_prediction=None)
    assert result["should_enter"]
    assert result["model_ok"] is False


def test_evaluate_candidate_requires_model_confidence_when_a_model_exists():
    low_confidence = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.51})
    assert not low_confidence["should_enter"]
    high_confidence = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.7})
    assert high_confidence["should_enter"]


def _position(entry_price=65000.0, minutes_ago=0):
    opened = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=minutes_ago)
    return {"entry_price": entry_price, "opened_at": opened.isoformat()}


def test_decide_exit_take_profit():
    pos = _position()
    should_exit, reason = strat.decide_exit(pos, 65000.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001))
    assert should_exit and "take_profit" in reason


def test_decide_exit_stop_loss():
    pos = _position()
    should_exit, reason = strat.decide_exit(pos, 65000.0 * (1 - strat.STOP_LOSS_PCT - 0.001))
    assert should_exit and "stop_loss" in reason


def test_decide_exit_max_hold_time():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(pos, 65000.0)
    assert should_exit and "max_hold_time" in reason


def test_position_exit_levels():
    levels = strat.position_exit_levels({"entry_price": 65000.0})
    assert levels["take_profit_price"] == round(65000.0 * (1 + strat.TAKE_PROFIT_PCT), 6)
    assert levels["stop_loss_price"] == round(65000.0 * (1 - strat.STOP_LOSS_PCT), 6)


def test_compute_position_notional_is_a_dollar_amount_not_a_share_count():
    """Crypto never has the "can't afford one whole share" problem
    equities have -- notional sizing works at any price, including a
    $65,000 coin against a $500 account."""
    notional = strat.compute_position_notional(500.0)
    assert notional == round(500.0 * strat.POSITION_SIZE_PCT, 2)
    assert notional > 0


def test_compute_position_notional_floors_at_zero_for_a_negative_balance():
    assert strat.compute_position_notional(-10.0) == 0.0


# ---------------------------------------------------------------------------
# Simulate/live position lifecycle -- state persistence, entry, exit.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _isolated_state(tmp_path, monkeypatch):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "alpaca_crypto_state.json")
    monkeypatch.setattr(strat, "HF_API_KEY", "")
    monkeypatch.setattr(strat, "MODE", "simulate")
    yield


def test_load_state_defaults_to_the_simulate_starting_balance():
    state = strat._load_state()  # noqa: SLF001
    assert state["balance"] == strat.SIMULATE_STARTING_BALANCE
    assert state["positions"] == []


def test_get_current_price_averages_bid_and_ask(monkeypatch):
    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", lambda symbol: {"ap": 65010.0, "bp": 64990.0})
    assert strat.get_current_price("BTC/USD") == pytest.approx(65000.0)


def test_get_current_price_returns_none_on_a_failed_quote(monkeypatch):
    def fail(symbol):
        raise RuntimeError("network down")

    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", fail)
    assert strat.get_current_price("BTC/USD") is None


def _entry_row(**overrides):
    base = {
        "symbol": "BTC/USD", "current_price": 65000.0, "short_ma": 65200.0,
        "dollar_volume_z": 2.0, "volatility_5": 0.002, "volatility_30": 0.001,
    }
    base.update(overrides)
    return base


def test_scan_and_enter_simulate_mode_opens_a_paper_position_without_any_real_order(monkeypatch):
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BTC/USD"])
    monkeypatch.setattr(alpaca_crypto_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_crypto_model, "predict_direction", lambda symbol: {"model_ok": False})

    def fail_if_called(*a, **k):
        raise AssertionError("simulate mode must never place a real order")

    monkeypatch.setattr(alpaca_client, "place_order", fail_if_called)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    assert result["opened"][0]["dry_run"] is True

    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1
    assert state["positions"][0]["symbol"] == "BTC/USD"
    assert state["positions"][0]["count"] > 0  # a fractional coin count, computed from notional/entry_price


def test_scan_and_enter_skips_a_symbol_already_held(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 500.0, "positions": [{"symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001}],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BTC/USD"])

    def fail_if_called(symbol):
        raise AssertionError("must not re-evaluate a symbol that's already held")

    monkeypatch.setattr(alpaca_crypto_data, "latest_feature_row", fail_if_called)
    result = strat.scan_and_enter()
    assert result["opened"] == []


def test_scan_and_enter_respects_the_daily_loss_cap(monkeypatch):
    today = strat._today_str()  # noqa: SLF001
    strat._save_state({  # noqa: SLF001
        "balance": 500.0, "positions": [], "trade_log": [],
        "realized_pnl_by_date": {today: -100.0},  # -20% of $500, breaches the 10% default cap
    })
    result = strat.scan_and_enter()
    assert result["action"] == "daily_loss_cap_breached"


def test_scan_and_enter_posts_to_threads_on_a_simulated_entry(monkeypatch):
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BTC/USD"])
    monkeypatch.setattr(alpaca_crypto_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_crypto_model, "predict_direction", lambda symbol: {"model_ok": False})

    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_entry", lambda **kw: posted.update(kw) or True)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    assert posted["ticker"] == "BTC/USD"
    assert posted["side"] == "long"
    assert posted["dry_run"] is True


def test_scan_and_enter_still_opens_the_position_even_if_threads_post_raises(monkeypatch):
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BTC/USD"])
    monkeypatch.setattr(alpaca_crypto_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_crypto_model, "predict_direction", lambda symbol: {"model_ok": False})

    def raise_error(**kwargs):
        raise RuntimeError("simulated Threads API outage")

    monkeypatch.setattr(threads_post, "post_trade_entry", raise_error)
    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1


def test_scan_and_enter_one_symbol_failing_does_not_block_the_others(monkeypatch):
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BAD/USD", "BTC/USD"])

    def fake_feature_row(symbol):
        if symbol == "BAD/USD":
            raise RuntimeError("data fetch failed")
        return _entry_row(symbol=symbol)

    monkeypatch.setattr(alpaca_crypto_data, "latest_feature_row", fake_feature_row)
    monkeypatch.setattr(alpaca_crypto_model, "predict_direction", lambda symbol: {"model_ok": False})

    result = strat.scan_and_enter()
    outcomes = {o["symbol"]: o for o in result["opened"]}
    assert outcomes["BAD/USD"]["ok"] is False
    assert outcomes["BTC/USD"]["ok"] is True and outcomes["BTC/USD"]["action"] == "opened"


def test_manage_open_positions_posts_a_threads_exit_on_close(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 500.0, "positions": [{
            "symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 65000.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_exit", lambda **kw: posted.update(kw) or True)

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert posted["ticker"] == "BTC/USD"
    assert posted["market"] == "crypto"
    assert posted["pnl_usd"] > 0


def test_manage_open_positions_returns_no_position_without_any_state():
    assert strat.manage_open_positions()["action"] == "no_position"


def test_manage_open_positions_simulate_mode_closes_on_take_profit(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 500.0, "positions": [{
            "symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 65000.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert "take_profit" in result["closed"][0]["reason"]
    state = strat._load_state()  # noqa: SLF001
    assert state["positions"] == []


def test_manage_open_positions_leaves_position_open_when_nothing_triggers(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 500.0, "positions": [{
            "symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", lambda symbol: {"ap": 65005.0, "bp": 65005.0})
    result = strat.manage_open_positions()
    assert result["action"] == "no_change"


def test_manage_open_positions_never_places_a_real_order_in_simulate_mode(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 500.0, "positions": [{
            "symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 65000.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    def fail_if_called(*a, **k):
        raise AssertionError("simulate mode must never place a real order")

    monkeypatch.setattr(alpaca_client, "place_order", fail_if_called)
    result = strat.manage_open_positions()
    assert result["action"] == "closed"


def test_manage_open_positions_live_mode_places_a_plain_market_sell(monkeypatch):
    """Unlike the equities strategy, there's no get_position/close_position
    reconciliation dance here -- crypto has no broker-native bracket order
    that could have already closed the position, so a triggered exit is
    always a fresh, plain market sell."""
    monkeypatch.setattr(strat, "MODE", "live")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "balance": 500.0, "positions": [{
            "symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 65000.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    captured = {}
    monkeypatch.setattr(alpaca_client, "build_crypto_order", lambda **kw: captured.update(kw) or {"symbol": kw["symbol"]})
    monkeypatch.setattr(alpaca_client, "place_order", lambda spec: "order-2")

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert captured["side"] == "sell"
    assert captured["qty"] == 0.001
