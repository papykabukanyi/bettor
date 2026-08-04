"""Alpaca equities strategy decision logic -- separate from and parallel to
test_perps_strategy.py. Pure-function tests: volume/volatility entry gate,
take-profit/stop-loss/max-hold exits, position sizing, and the simulate/
live position-lifecycle engine (state persistence, entry, exit)."""
from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from data import alpaca_client, alpaca_data, alpaca_model, threads_post, alpaca_strategy as strat


@pytest.fixture(autouse=True)
def _regular_market_session(monkeypatch):
    """scan_and_enter/manage_open_positions are now session-aware (see
    alpaca_strategy.py's own docstrings) -- default every test to the
    regular session, matching what these tests already assumed implicitly
    before that awareness existed. Tests that actually exercise pre/post
    market behavior override this explicitly."""
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "regular", "is_open": True, "source": "test"})
    yield


def _row(**overrides):
    base = {
        "symbol": "AAPL", "current_price": 100.0, "short_ma": 100.3,
        "dollar_volume_z": 2.0, "volatility_5": 0.002, "volatility_30": 0.001,
    }
    base.update(overrides)
    return base


def test_entry_does_not_require_an_unusual_volume_spike(monkeypatch):
    """Real gap found in review: requiring a volume spike AND a
    volatility-ratio jump AND a dip, all three simultaneously, made real
    entries rare in practice -- perps_strategy.py's own
    decide_entry_technical (this one's model) has no such gate at all.
    Low/negative volume must no longer block an otherwise-valid dip entry
    at the new defaults."""
    should_enter, reason = strat.decide_entry_technical(_row(dollar_volume_z=-2.0))
    assert should_enter
    assert "dip" in reason


def test_entry_does_not_require_elevated_volatility_relative_to_its_own_baseline():
    should_enter, reason = strat.decide_entry_technical(_row(volatility_5=0.0005, volatility_30=0.002))
    assert should_enter


def test_entry_volume_gate_can_still_be_re_enabled_via_env(monkeypatch):
    """Confirms this is a real, working opt-in override, not dead config."""
    monkeypatch.setattr(strat, "MIN_VOLUME_Z", 1.5)
    should_enter, reason = strat.decide_entry_technical(_row(dollar_volume_z=0.5))
    assert not should_enter
    assert "volume" in reason


def test_entry_requires_a_real_dip():
    should_enter, reason = strat.decide_entry_technical(_row(short_ma=100.0, current_price=100.0))
    assert not should_enter
    assert "dip" in reason


def test_entry_fires_when_all_gates_pass():
    should_enter, reason = strat.decide_entry_technical(_row())
    assert should_enter


def test_evaluate_candidate_technical_only_fallback_without_a_model():
    result = strat.evaluate_candidate(_row(), model_prediction=None)
    assert result["technical_ok"]
    assert result["should_enter"]
    assert result["model_ok"] is False


def test_evaluate_candidate_requires_model_confidence_when_a_model_exists():
    low_confidence = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.51})
    assert not low_confidence["should_enter"]

    high_confidence = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.7})
    assert high_confidence["should_enter"]


def test_evaluate_candidate_skips_model_check_when_technical_gate_fails():
    no_dip_row = _row(short_ma=100.0, current_price=100.0)
    result = strat.evaluate_candidate(no_dip_row, {"model_ok": True, "probability_up": 0.9})
    assert not result["should_enter"]
    assert result["model_ok"] is False


def _position(entry_price=100.0, minutes_ago=0):
    opened = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=minutes_ago)
    return {"entry_price": entry_price, "opened_at": opened.isoformat()}


def test_decide_exit_take_profit():
    pos = _position(entry_price=100.0)
    should_exit, reason = strat.decide_exit(pos, 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001))
    assert should_exit and "take_profit" in reason


def test_decide_exit_stop_loss():
    pos = _position(entry_price=100.0)
    should_exit, reason = strat.decide_exit(pos, 100.0 * (1 - strat.STOP_LOSS_PCT - 0.001))
    assert should_exit and "stop_loss" in reason


def test_decide_exit_max_hold_time():
    pos = _position(entry_price=100.0, minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(pos, 100.0)  # flat price -- neither TP nor SL
    assert should_exit and "max_hold_time" in reason


def test_decide_exit_holds_when_nothing_triggers():
    pos = _position(entry_price=100.0)
    should_exit, reason = strat.decide_exit(pos, 100.1)
    assert not should_exit and "holding" in reason


def test_decide_exit_respects_explicit_simulated_now():
    """A backtest replaying historical rows must use the SIMULATED current
    time, not real wall-clock time -- the exact bug that was found and
    fixed in the Kalshi perps backtest earlier this session."""
    opened = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
    pos = {"entry_price": 100.0, "opened_at": opened.isoformat()}
    sim_now = opened + dt.timedelta(minutes=5)  # well under MAX_HOLD_MINUTES
    should_exit, reason = strat.decide_exit(pos, 100.1, now=sim_now)
    assert not should_exit  # would be a false max_hold_time if real wall-clock time leaked in


def test_position_exit_levels():
    levels = strat.position_exit_levels({"entry_price": 100.0})
    assert levels["take_profit_price"] == round(100.0 * (1 + strat.TAKE_PROFIT_PCT), 6)
    assert levels["stop_loss_price"] == round(100.0 * (1 - strat.STOP_LOSS_PCT), 6)


def test_compute_position_size_floors_to_whole_shares():
    count = strat.compute_position_size(1000.0, 30.0)
    assert count == int((1000.0 * strat.POSITION_SIZE_PCT) // 30.0)


def test_compute_position_size_zero_at_zero_price():
    assert strat.compute_position_size(1000.0, 0.0) == 0


# ---------------------------------------------------------------------------
# Simulate/live position lifecycle -- state persistence, entry, exit.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _isolated_state(tmp_path, monkeypatch):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "alpaca_state.json")
    monkeypatch.setattr(strat, "HF_API_KEY", "")  # no real network for the HF durable-state mirror by default
    monkeypatch.setattr(strat, "MODE", "simulate")
    yield


def test_load_state_defaults_to_the_simulate_starting_balance():
    state = strat._load_state()  # noqa: SLF001
    assert state["balance"] == strat.SIMULATE_STARTING_BALANCE
    assert state["positions"] == []


def test_get_available_balance_subtracts_committed_positions(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 100.0, "positions": [{"symbol": "AAPL", "entry_price": 50.0, "count": 1.0}],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    assert strat.get_available_balance() == 50.0


def test_get_current_price_averages_bid_and_ask_from_the_latest_quote(monkeypatch):
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": 124.0, "bp": 122.9})
    assert strat.get_current_price("AAPL") == pytest.approx(123.45)


def test_get_current_price_falls_back_to_whichever_side_is_present(monkeypatch):
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": 124.0, "bp": 0})
    assert strat.get_current_price("AAPL") == 124.0


def test_get_current_price_returns_none_on_a_failed_quote(monkeypatch):
    def fail(symbol):
        raise RuntimeError("network down")

    monkeypatch.setattr(alpaca_client, "get_latest_quote", fail)
    assert strat.get_current_price("AAPL") is None


def _entry_row(**overrides):
    # A $100 simulate balance at the default 45% position size needs an
    # affordable-in-whole-shares current_price.
    base = {
        "symbol": "AAPL", "current_price": 5.0, "short_ma": 5.015,
        "dollar_volume_z": 2.0, "volatility_5": 0.002, "volatility_30": 0.001,
    }
    base.update(overrides)
    return base


def test_scan_and_enter_simulate_mode_opens_a_paper_position_without_any_real_order(monkeypatch):
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})

    def fail_if_called(*a, **k):
        raise AssertionError("simulate mode must never place a real order")

    monkeypatch.setattr(alpaca_client, "place_order", fail_if_called)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    assert result["opened"][0]["dry_run"] is True

    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1
    assert state["positions"][0]["symbol"] == "AAPL"
    assert state["positions"][0]["order_id"] is None


def test_scan_and_enter_places_an_extended_hours_limit_order_in_pre_market(monkeypatch):
    """A bracket order is regular-hours only -- pre/post-market must place
    a plain limit order with extended_hours=true instead."""
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "pre_market", "is_open": False, "source": "test"})
    monkeypatch.setattr(strat, "MODE", "live")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "100.0"})

    captured = {}
    monkeypatch.setattr(alpaca_client, "place_order", lambda order_spec: captured.update(order_spec) or "order-1")

    result = strat.scan_and_enter(dry_run=False)
    assert result["opened"][0]["action"] == "opened"
    assert captured["type"] == "limit"
    assert captured["extended_hours"] is True
    assert captured["side"] == "buy"
    assert "order_class" not in captured


def test_scan_and_enter_places_a_bracket_order_during_regular_hours(monkeypatch):
    monkeypatch.setattr(strat, "MODE", "live")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "100.0"})

    captured = {}
    monkeypatch.setattr(alpaca_client, "place_order", lambda order_spec: captured.update(order_spec) or "order-1")

    result = strat.scan_and_enter(dry_run=False)
    assert result["opened"][0]["action"] == "opened"
    assert captured["order_class"] == "bracket"


def test_scan_and_enter_skips_entirely_when_market_is_fully_closed(monkeypatch):
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "closed", "is_open": False, "source": "test"})

    def fail_if_called(*a, **kw):
        raise AssertionError("must not evaluate any symbol when the market is fully closed")

    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", fail_if_called)
    monkeypatch.setattr(alpaca_data, "latest_feature_row", fail_if_called)

    result = strat.scan_and_enter()
    assert result == {"opened": [], "action": "market_closed"}


def test_scan_and_enter_skips_a_symbol_already_held(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 100.0, "positions": [{"symbol": "AAPL", "entry_price": 100.0, "count": 1.0}],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())

    def fail_if_called(symbol):
        raise AssertionError("must not re-evaluate a symbol that's already held")

    monkeypatch.setattr(alpaca_data, "latest_feature_row", fail_if_called)

    result = strat.scan_and_enter()
    assert result["opened"] == []


def test_scan_and_enter_respects_the_daily_loss_cap(monkeypatch):
    today = strat._today_str()  # noqa: SLF001
    strat._save_state({  # noqa: SLF001
        "balance": 100.0, "positions": [], "trade_log": [],
        "realized_pnl_by_date": {today: -20.0},  # -20% of $100, breaches the 10% default cap
    })
    result = strat.scan_and_enter()
    assert result["action"] == "daily_loss_cap_breached"


def test_scan_and_enter_posts_to_threads_on_a_simulated_entry(monkeypatch):
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})

    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_entry", lambda **kw: posted.update(kw) or True)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    assert posted["ticker"] == "AAPL"
    assert posted["side"] == "long"
    assert posted["dry_run"] is True  # simulate mode -- must be flagged [SIMULATED]
    assert posted["take_profit_price"] > posted["entry_price"]
    assert posted["stop_loss_price"] < posted["entry_price"]


def test_scan_and_enter_still_opens_the_position_even_if_threads_post_raises(monkeypatch):
    """A Threads failure must never be allowed to affect a real/simulated
    entry, since post_trade_entry() is called AFTER the position is
    already saved to state."""
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})

    def raise_error(**kwargs):
        raise RuntimeError("simulated Threads API outage")

    monkeypatch.setattr(threads_post, "post_trade_entry", raise_error)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1


def test_scan_and_enter_one_symbol_failing_does_not_block_the_others(monkeypatch):
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["BAD", "AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())

    def fake_feature_row(symbol):
        if symbol == "BAD":
            raise RuntimeError("data fetch failed")
        return _entry_row(symbol=symbol)

    monkeypatch.setattr(alpaca_data, "latest_feature_row", fake_feature_row)
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})

    result = strat.scan_and_enter()
    outcomes = {o["symbol"]: o for o in result["opened"]}
    assert outcomes["BAD"]["ok"] is False
    assert outcomes["AAPL"]["ok"] is True and outcomes["AAPL"]["action"] == "opened"


def test_manage_open_positions_returns_no_position_without_any_state():
    result = strat.manage_open_positions()
    assert result["action"] == "no_position"


def test_manage_open_positions_posts_a_threads_exit_on_close(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 100.0, "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_exit", lambda **kw: posted.update(kw) or True)

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert posted["ticker"] == "AAPL"
    assert posted["market"] == "stocks"
    assert posted["pnl_usd"] > 0


def test_manage_open_positions_simulate_mode_closes_on_take_profit_and_updates_virtual_balance(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 100.0, "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert "take_profit" in result["closed"][0]["reason"]

    state = strat._load_state()  # noqa: SLF001
    assert state["positions"] == []
    expected_gain = round(take_profit_price - 100.0, 6)
    assert state["balance"] == round(100.0 + expected_gain, 6)
    assert state["trade_log"][0]["realized_pnl_usd"] == expected_gain


def test_manage_open_positions_leaves_position_open_when_nothing_triggers(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 100.0, "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": 100.05, "bp": 100.05})
    result = strat.manage_open_positions()
    assert result["action"] == "no_change"
    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1


def test_manage_open_positions_one_bad_quote_does_not_block_the_others(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 100.0,
        "positions": [
            {"symbol": "BAD", "entry_price": 100.0, "count": 1.0, "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None},
            {"symbol": "AAPL", "entry_price": 100.0, "count": 1.0, "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None},
        ],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)

    def fake_quote(symbol):
        if symbol == "BAD":
            raise RuntimeError("quote service down")
        return {"ap": take_profit_price, "bp": take_profit_price}

    monkeypatch.setattr(alpaca_client, "get_latest_quote", fake_quote)
    result = strat.manage_open_positions()
    assert any(c["symbol"] == "BAD" and c["ok"] is False for c in result["checks"])
    assert any(t["symbol"] == "AAPL" for t in result["closed"])
    state = strat._load_state()  # noqa: SLF001
    assert [p["symbol"] for p in state["positions"]] == ["BAD"]  # AAPL closed, BAD stayed open


def test_manage_open_positions_never_places_a_real_order_in_simulate_mode(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 100.0, "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    def fail_if_called(*a, **k):
        raise AssertionError("simulate mode must never place or cancel a real order")

    monkeypatch.setattr(alpaca_client, "place_order", fail_if_called)
    monkeypatch.setattr(alpaca_client, "close_position", fail_if_called)

    result = strat.manage_open_positions()
    assert result["action"] == "closed"


# ---------------------------------------------------------------------------
# Live-mode exit reconciliation -- new relative to the former Schwab
# strategy: before forcing a live exit, check whether Alpaca's own bracket
# take-profit/stop-loss already closed the position moments earlier (a real
# double-sell risk if not checked).
# ---------------------------------------------------------------------------
def test_manage_open_positions_live_mode_closes_via_close_position_when_still_open(monkeypatch):
    monkeypatch.setattr(strat, "MODE", "live")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "balance": 100.0, "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})
    monkeypatch.setattr(alpaca_client, "get_position", lambda symbol: {"symbol": symbol, "qty": "1"})
    closed_calls = []
    monkeypatch.setattr(alpaca_client, "close_position", lambda symbol: closed_calls.append(symbol))

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert closed_calls == ["AAPL"]


def test_manage_open_positions_live_mode_reconciles_without_double_selling_when_bracket_already_closed_it(monkeypatch):
    monkeypatch.setattr(strat, "MODE", "live")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "balance": 100.0, "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
            "take_profit_price": 101.0, "stop_loss_price": 99.0,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})
    # Alpaca's own bracket order already closed this position -- no position exists anymore.
    monkeypatch.setattr(alpaca_client, "get_position", lambda symbol: None)

    def fail_if_called(symbol):
        raise AssertionError("must not attempt to close a position that's already gone (double-sell risk)")

    monkeypatch.setattr(alpaca_client, "close_position", fail_if_called)

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    # Reconciled using the position's OWN stored take-profit level, not the
    # (possibly stale) live quote fetched after the bracket already fired.
    assert result["closed"][0]["exit_price"] == 101.0


def test_manage_open_positions_uses_an_extended_hours_limit_order_during_pre_market(monkeypatch):
    """close_position() liquidates at market -- Alpaca rejects that outside
    9:30-4:00 ET. Pre/post-market must use a plain limit sell with
    extended_hours=true instead, checked at EXIT time (not entry time)."""
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "pre_market", "is_open": False, "source": "test"})
    monkeypatch.setattr(strat, "MODE", "live")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "balance": 100.0, "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})
    monkeypatch.setattr(alpaca_client, "get_position", lambda symbol: {"symbol": symbol, "qty": "1"})

    def fail_if_called(symbol):
        raise AssertionError("close_position() is a market order -- must not be used outside regular hours")

    monkeypatch.setattr(alpaca_client, "close_position", fail_if_called)
    captured = {}
    monkeypatch.setattr(alpaca_client, "place_order", lambda order_spec: captured.update(order_spec) or "order-2")

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert captured["type"] == "limit"
    assert captured["extended_hours"] is True
    assert captured["side"] == "sell"


def test_manage_open_positions_defers_a_live_exit_when_market_is_fully_closed(monkeypatch):
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "closed", "is_open": False, "source": "test"})
    monkeypatch.setattr(strat, "MODE", "live")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "balance": 100.0, "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    def fail_if_called(*a, **kw):
        raise AssertionError("must not attempt any order when the market is fully closed")

    monkeypatch.setattr(alpaca_client, "get_position", fail_if_called)
    monkeypatch.setattr(alpaca_client, "close_position", fail_if_called)
    monkeypatch.setattr(alpaca_client, "place_order", fail_if_called)

    result = strat.manage_open_positions()
    assert result["action"] == "no_change"
    # The position must still be there, untouched, to retry next cycle.
    with strat._STATE_LOCK:  # noqa: SLF001
        state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1
