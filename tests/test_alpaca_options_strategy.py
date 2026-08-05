"""Alpaca options strategy decision logic. Mirrors test_alpaca_crypto_strategy.py's
discipline; focuses on what's genuinely different here: no technical-only
cold-start fallback (a real trained model is required before any entry),
directional bet expressed via contract choice (call vs put) rather than
order side, whole-contract quantity sizing, and forced exit near
expiration on top of the usual take-profit/stop-loss/max-hold checks."""
from __future__ import annotations

import datetime as dt

import pytest

from data import alpaca_client, alpaca_data, alpaca_options_data, alpaca_options_model, threads_post, alpaca_options_strategy as strat


@pytest.fixture(autouse=True)
def _regular_market_session(monkeypatch):
    """scan_and_enter is regular-hours-only now (Alpaca doesn't support
    extended-hours options trading at all -- see alpaca_options_strategy.py's
    own docstring) -- default every test to the regular session, matching
    what these tests already assumed implicitly before that gate existed."""
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "regular", "is_open": True, "source": "test"})
    # Also comfortably clear of the open/close-edge gate ("pick the best
    # times" -- see alpaca_options_strategy.py's own AVOID_SESSION_EDGE_MINUTES).
    monkeypatch.setattr(strat, "_minutes_from_session_edge", lambda: 999.0)
    yield


def _row(**overrides):
    base = {"symbol": "AAPL", "current_price": 190.0, "short_ma": 191.0}
    base.update(overrides)
    return base


def test_evaluate_candidate_never_enters_without_a_trained_model():
    result = strat.evaluate_candidate(_row(), model_prediction=None)
    assert not result["should_enter"]
    assert result["model_ok"] is False

    result = strat.evaluate_candidate(_row(), model_prediction={"model_ok": False})
    assert not result["should_enter"]


def test_evaluate_candidate_buys_a_call_on_confident_up_prediction():
    result = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.7})
    assert result["should_enter"]
    assert result["direction"] == "up"


def test_evaluate_candidate_buys_a_put_on_confident_down_prediction():
    result = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.2})
    assert result["should_enter"]
    assert result["direction"] == "down"


def test_evaluate_candidate_skips_when_the_model_is_not_confident_either_way():
    result = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.52})
    assert not result["should_enter"]
    assert result["direction"] is None


def _position(entry_price=5.0, minutes_ago=0, expiration_days_out=30):
    opened = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=minutes_ago)
    expiration = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=expiration_days_out)).date().isoformat()
    return {"entry_price": entry_price, "opened_at": opened.isoformat(), "expiration_date": expiration}


def test_decide_exit_take_profit():
    pos = _position()
    should_exit, reason = strat.decide_exit(pos, 5.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001))
    assert should_exit and "take_profit" in reason


def test_decide_exit_stop_loss():
    pos = _position()
    should_exit, reason = strat.decide_exit(pos, 5.0 * (1 - strat.STOP_LOSS_PCT - 0.001))
    assert should_exit and "stop_loss" in reason


def test_decide_exit_max_hold_time():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(pos, 5.0)
    assert should_exit and "max_hold_time" in reason


def test_decide_exit_forces_a_close_near_expiration_regardless_of_pnl():
    """A held-flat contract (no TP/SL/max-hold trigger) still must be
    force-closed once expiration is imminent -- avoids assignment risk and
    a contract decaying to worthless for reasons unrelated to this
    strategy's own exit signal."""
    pos = _position(expiration_days_out=1)
    should_exit, reason = strat.decide_exit(pos, 5.0)
    assert should_exit and reason == "near_expiration"


def test_decide_exit_does_not_force_close_when_expiration_is_far_out():
    pos = _position(expiration_days_out=30)
    should_exit, reason = strat.decide_exit(pos, 5.0)
    assert not should_exit


def test_position_exit_levels():
    levels = strat.position_exit_levels({"entry_price": 5.0})
    assert levels["take_profit_price"] == round(5.0 * (1 + strat.TAKE_PROFIT_PCT), 6)
    assert levels["stop_loss_price"] == round(5.0 * (1 - strat.STOP_LOSS_PCT), 6)


def test_compute_contract_qty_is_a_whole_number_sized_off_the_100x_multiplier():
    qty = strat.compute_contract_qty(500.0, 1.0)
    budget = 500.0 * strat.POSITION_SIZE_PCT
    assert qty == int(budget // (1.0 * 100))
    assert qty >= 1


def test_compute_contract_qty_is_zero_when_a_single_contract_is_unaffordable():
    assert strat.compute_contract_qty(500.0, 100.0) == 0


def test_compute_contract_qty_is_zero_for_a_non_positive_contract_price():
    assert strat.compute_contract_qty(500.0, 0.0) == 0


# ---------------------------------------------------------------------------
# Simulate/live position lifecycle -- state persistence, entry, exit.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _isolated_state(tmp_path, monkeypatch):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "alpaca_options_state.json")
    monkeypatch.setattr(strat, "HF_API_KEY", "")
    monkeypatch.setattr(strat, "MODE", "simulate")
    yield


def test_load_state_defaults_to_the_simulate_starting_balance():
    state = strat._load_state()  # noqa: SLF001
    assert state["balance"] == strat.SIMULATE_STARTING_BALANCE
    assert state["positions"] == []


def test_get_current_option_price_averages_bid_and_ask(monkeypatch):
    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", lambda symbol: {"ap": 5.1, "bp": 4.9})
    assert strat.get_current_option_price("AAPL240223C00195000") == pytest.approx(5.0)


def test_get_current_option_price_returns_none_on_a_failed_quote(monkeypatch):
    def fail(symbol):
        raise RuntimeError("network down")

    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", fail)
    assert strat.get_current_option_price("AAPL240223C00195000") is None


def _contract(**overrides):
    base = {
        "symbol": "AAPL240223C00195000", "type": "call", "strike_price": 195.0,
        "expiration_date": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=30)).date().isoformat(),
    }
    base.update(overrides)
    return base


def test_scan_and_enter_simulate_mode_opens_a_paper_position_without_any_real_order(monkeypatch):
    monkeypatch.setattr(strat, "ENTRY_STRATEGY", "naked")
    monkeypatch.setattr(alpaca_options_data, "get_options_universe", lambda: ["AAPL"])
    monkeypatch.setattr(alpaca_options_data, "latest_feature_row", lambda symbol: _row())
    monkeypatch.setattr(alpaca_options_model, "predict_direction", lambda symbol: {"model_ok": True, "probability_up": 0.7})
    monkeypatch.setattr(alpaca_options_data, "select_contract", lambda underlying, *, direction, current_price: _contract())
    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", lambda symbol: {"ap": 1.05, "bp": 0.95})

    def fail_if_called(*a, **k):
        raise AssertionError("simulate mode must never place a real order")

    monkeypatch.setattr(alpaca_client, "place_order", fail_if_called)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    assert result["opened"][0]["dry_run"] is True
    assert result["opened"][0]["option_type"] == "call"

    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1
    assert state["positions"][0]["underlying_symbol"] == "AAPL"
    assert state["positions"][0]["symbol"] == "AAPL240223C00195000"
    assert state["positions"][0]["count"] >= 1


def test_scan_and_enter_skips_without_a_trained_model(monkeypatch):
    monkeypatch.setattr(alpaca_options_data, "get_options_universe", lambda: ["AAPL"])
    monkeypatch.setattr(alpaca_options_data, "latest_feature_row", lambda symbol: _row())
    monkeypatch.setattr(alpaca_options_model, "predict_direction", lambda symbol: {"model_ok": False})

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "skipped"
    state = strat._load_state()  # noqa: SLF001
    assert state["positions"] == []


def test_scan_and_enter_skips_entirely_outside_regular_hours(monkeypatch):
    """Alpaca does not support extended-hours options trading at all --
    unlike equities, there's no limit+extended_hours order path to fall
    back to, so this must skip outright, not just gate the order type."""
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "pre_market", "is_open": False, "source": "test"})

    def fail_if_called(*a, **kw):
        raise AssertionError("must not evaluate any underlying outside regular hours")

    monkeypatch.setattr(alpaca_options_data, "get_options_universe", fail_if_called)
    monkeypatch.setattr(alpaca_options_data, "latest_feature_row", fail_if_called)

    result = strat.scan_and_enter()
    assert result == {"opened": [], "action": "market_not_regular_hours"}


def test_scan_and_enter_skips_entirely_right_at_the_session_open(monkeypatch):
    """"Pick the best times": real options spreads are consistently widest
    right at the open/close -- a technically-valid signal there is priced
    worse than the exact same signal AVOID_SESSION_EDGE_MINUTES later."""
    monkeypatch.setattr(strat, "_minutes_from_session_edge", lambda: 3.0)

    def fail_if_called(*a, **kw):
        raise AssertionError("must not evaluate any underlying right at the session edge")

    monkeypatch.setattr(alpaca_options_data, "get_options_universe", fail_if_called)
    monkeypatch.setattr(alpaca_options_data, "latest_feature_row", fail_if_called)

    result = strat.scan_and_enter()
    assert result == {"opened": [], "action": "too_close_to_session_edge"}


def test_scan_and_enter_skips_a_symbol_already_held(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 500.0,
        "positions": [{
            "symbol": "AAPL240223C00195000", "underlying_symbol": "AAPL", "entry_price": 5.0, "count": 1,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "expiration_date": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=30)).date().isoformat(),
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_options_data, "get_options_universe", lambda: ["AAPL"])

    def fail_if_called(symbol):
        raise AssertionError("must not re-evaluate a symbol that's already held")

    monkeypatch.setattr(alpaca_options_data, "latest_feature_row", fail_if_called)
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


def test_scan_and_enter_respects_max_concurrent_positions(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 500.0,
        "positions": [
            {
                "symbol": f"SYM{i}240223C00195000", "underlying_symbol": f"SYM{i}", "entry_price": 5.0, "count": 1,
                "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "expiration_date": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=30)).date().isoformat(),
            }
            for i in range(strat.MAX_CONCURRENT_POSITIONS)
        ],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_options_data, "get_options_universe", lambda: ["AAPL"])
    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "skipped_slot_taken"


def test_scan_and_enter_posts_to_threads_on_a_simulated_entry(monkeypatch):
    monkeypatch.setattr(strat, "ENTRY_STRATEGY", "naked")
    monkeypatch.setattr(alpaca_options_data, "get_options_universe", lambda: ["AAPL"])
    monkeypatch.setattr(alpaca_options_data, "latest_feature_row", lambda symbol: _row())
    monkeypatch.setattr(alpaca_options_model, "predict_direction", lambda symbol: {"model_ok": True, "probability_up": 0.7})
    monkeypatch.setattr(alpaca_options_data, "select_contract", lambda underlying, *, direction, current_price: _contract())
    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", lambda symbol: {"ap": 1.05, "bp": 0.95})

    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_entry", lambda **kw: posted.update(kw) or True)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    assert posted["ticker"] == "AAPL240223C00195000"
    assert posted["side"] == "long"
    assert posted["dry_run"] is True


def test_scan_and_enter_still_opens_the_position_even_if_threads_post_raises(monkeypatch):
    monkeypatch.setattr(strat, "ENTRY_STRATEGY", "naked")
    monkeypatch.setattr(alpaca_options_data, "get_options_universe", lambda: ["AAPL"])
    monkeypatch.setattr(alpaca_options_data, "latest_feature_row", lambda symbol: _row())
    monkeypatch.setattr(alpaca_options_model, "predict_direction", lambda symbol: {"model_ok": True, "probability_up": 0.7})
    monkeypatch.setattr(alpaca_options_data, "select_contract", lambda underlying, *, direction, current_price: _contract())
    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", lambda symbol: {"ap": 1.05, "bp": 0.95})

    def raise_error(**kwargs):
        raise RuntimeError("simulated Threads API outage")

    monkeypatch.setattr(threads_post, "post_trade_entry", raise_error)
    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1


def test_scan_and_enter_one_symbol_failing_does_not_block_the_others(monkeypatch):
    monkeypatch.setattr(strat, "ENTRY_STRATEGY", "naked")
    monkeypatch.setattr(alpaca_options_data, "get_options_universe", lambda: ["BAD", "AAPL"])

    def fake_feature_row(symbol):
        if symbol == "BAD":
            raise RuntimeError("data fetch failed")
        return _row(symbol=symbol)

    monkeypatch.setattr(alpaca_options_data, "latest_feature_row", fake_feature_row)
    monkeypatch.setattr(alpaca_options_model, "predict_direction", lambda symbol: {"model_ok": True, "probability_up": 0.7})
    monkeypatch.setattr(alpaca_options_data, "select_contract", lambda underlying, *, direction, current_price: _contract())
    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", lambda symbol: {"ap": 1.05, "bp": 0.95})

    result = strat.scan_and_enter()
    outcomes = {o["symbol"]: o for o in result["opened"]}
    assert outcomes["BAD"]["ok"] is False
    assert outcomes["AAPL"]["ok"] is True and outcomes["AAPL"]["action"] == "opened"


def test_manage_open_positions_returns_no_position_without_any_state():
    assert strat.manage_open_positions()["action"] == "no_position"


def test_manage_open_positions_posts_a_threads_exit_on_close(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 500.0, "positions": [{
            "symbol": "AAPL240223C00195000", "underlying_symbol": "AAPL", "entry_price": 5.0, "count": 1,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
            "expiration_date": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=30)).date().isoformat(),
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 5.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_exit", lambda **kw: posted.update(kw) or True)

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert posted["ticker"] == "AAPL240223C00195000"
    assert posted["market"] == "options"
    assert posted["pnl_usd"] > 0


def test_manage_open_positions_simulate_mode_closes_on_take_profit(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 500.0, "positions": [{
            "symbol": "AAPL240223C00195000", "underlying_symbol": "AAPL", "entry_price": 5.0, "count": 1,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
            "expiration_date": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=30)).date().isoformat(),
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 5.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert "take_profit" in result["closed"][0]["reason"]
    state = strat._load_state()  # noqa: SLF001
    assert state["positions"] == []


def test_manage_open_positions_leaves_position_open_when_nothing_triggers(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 500.0, "positions": [{
            "symbol": "AAPL240223C00195000", "underlying_symbol": "AAPL", "entry_price": 5.0, "count": 1,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
            "expiration_date": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=30)).date().isoformat(),
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", lambda symbol: {"ap": 5.01, "bp": 5.01})
    result = strat.manage_open_positions()
    assert result["action"] == "no_change"


def test_manage_open_positions_never_places_a_real_order_in_simulate_mode(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 500.0, "positions": [{
            "symbol": "AAPL240223C00195000", "underlying_symbol": "AAPL", "entry_price": 5.0, "count": 1,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
            "expiration_date": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=30)).date().isoformat(),
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 5.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    def fail_if_called(*a, **k):
        raise AssertionError("simulate mode must never place a real order")

    monkeypatch.setattr(alpaca_client, "place_order", fail_if_called)
    result = strat.manage_open_positions()
    assert result["action"] == "closed"


def test_manage_open_positions_live_mode_places_a_plain_market_sell(monkeypatch):
    monkeypatch.setattr(strat, "MODE", "live")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "balance": 500.0, "positions": [{
            "symbol": "AAPL240223C00195000", "underlying_symbol": "AAPL", "entry_price": 5.0, "count": 1,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
            "expiration_date": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=30)).date().isoformat(),
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 5.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    captured = {}
    monkeypatch.setattr(alpaca_client, "build_option_order", lambda **kw: captured.update(kw) or {"symbol": kw["symbol"]})
    monkeypatch.setattr(alpaca_client, "place_order", lambda spec: "order-2")

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert captured["side"] == "sell"
    assert captured["qty"] == 1


def test_manage_open_positions_force_closes_near_expiration(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 500.0, "positions": [{
            "symbol": "AAPL240223C00195000", "underlying_symbol": "AAPL", "entry_price": 5.0, "count": 1,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
            "expiration_date": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)).date().isoformat(),
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", lambda symbol: {"ap": 5.0, "bp": 5.0})
    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert result["closed"][0]["reason"] == "near_expiration"


# ---------------------------------------------------------------------------
# Vertical debit spreads -- ENTRY_STRATEGY's default. Confirmed via
# Alpaca's own docs (and this account's real options_approved_level, 3)
# that "Buy a call spread"/"Buy a put spread" is a genuine order_class=
# "mleg" order, not just naked long calls/puts (Level 2).
# ---------------------------------------------------------------------------
def _spread_contracts(**overrides):
    long_contract = {
        "symbol": "AAPL240223C00195000", "type": "call", "strike_price": 195.0,
        "expiration_date": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=30)).date().isoformat(),
    }
    short_contract = {
        "symbol": "AAPL240223C00200000", "type": "call", "strike_price": 200.0,
        "expiration_date": long_contract["expiration_date"],
    }
    long_contract.update(overrides.get("long", {}))
    short_contract.update(overrides.get("short", {}))
    return long_contract, short_contract


def test_get_current_spread_price_is_the_net_of_both_legs_mid_quotes(monkeypatch):
    def fake_quote(symbol):
        return {"AAPL240223C00195000": {"ap": 2.1, "bp": 1.9}, "AAPL240223C00200000": {"ap": 1.1, "bp": 0.9}}[symbol]

    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", fake_quote)
    net = strat.get_current_spread_price("AAPL240223C00195000", "AAPL240223C00200000")
    assert net == pytest.approx(2.0 - 1.0)  # long mid (2.0) minus short mid (1.0)


def test_get_current_spread_price_returns_none_if_either_leg_has_no_quote(monkeypatch):
    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", lambda symbol: {})
    assert strat.get_current_spread_price("A", "B") is None


def test_scan_and_enter_debit_spread_is_the_default_entry_strategy():
    assert strat.ENTRY_STRATEGY == "debit_spread"


def test_scan_and_enter_opens_a_debit_spread_position_by_default(monkeypatch):
    monkeypatch.setattr(alpaca_options_data, "get_options_universe", lambda: ["AAPL"])
    monkeypatch.setattr(alpaca_options_data, "latest_feature_row", lambda symbol: _row())
    monkeypatch.setattr(alpaca_options_model, "predict_direction", lambda symbol: {"model_ok": True, "probability_up": 0.7})
    monkeypatch.setattr(alpaca_options_data, "select_spread_contracts", lambda underlying, *, direction, current_price: _spread_contracts())

    def fake_quote(symbol):
        return {"AAPL240223C00195000": {"ap": 2.1, "bp": 1.9}, "AAPL240223C00200000": {"ap": 1.1, "bp": 0.9}}[symbol]

    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", fake_quote)

    def fail_if_called(*a, **k):
        raise AssertionError("simulate mode must never place a real order")

    monkeypatch.setattr(alpaca_client, "place_order", fail_if_called)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    assert result["opened"][0]["strategy"] == "debit_spread"
    assert result["opened"][0]["entry_price"] == pytest.approx(1.0)  # net debit: long mid 2.0 - short mid 1.0

    state = strat._load_state()  # noqa: SLF001
    position = state["positions"][0]
    assert position["symbol"] == "AAPL240223C00195000"  # the long leg
    assert position["short_symbol"] == "AAPL240223C00200000"
    assert position["strategy"] == "debit_spread"
    assert position["entry_price"] == pytest.approx(1.0)


def test_scan_and_enter_skips_a_spread_when_no_short_leg_is_available(monkeypatch):
    monkeypatch.setattr(alpaca_options_data, "get_options_universe", lambda: ["AAPL"])
    monkeypatch.setattr(alpaca_options_data, "latest_feature_row", lambda symbol: _row())
    monkeypatch.setattr(alpaca_options_model, "predict_direction", lambda symbol: {"model_ok": True, "probability_up": 0.7})
    monkeypatch.setattr(alpaca_options_data, "select_spread_contracts", lambda underlying, *, direction, current_price: None)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "skipped_no_contract"


def test_scan_and_enter_live_mode_places_a_real_mleg_order_for_a_spread(monkeypatch):
    monkeypatch.setattr(strat, "MODE", "live")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(alpaca_options_data, "get_options_universe", lambda: ["AAPL"])
    monkeypatch.setattr(alpaca_options_data, "latest_feature_row", lambda symbol: _row())
    monkeypatch.setattr(alpaca_options_model, "predict_direction", lambda symbol: {"model_ok": True, "probability_up": 0.7})
    monkeypatch.setattr(alpaca_options_data, "select_spread_contracts", lambda underlying, *, direction, current_price: _spread_contracts())
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0"})

    def fake_quote(symbol):
        return {"AAPL240223C00195000": {"ap": 2.1, "bp": 1.9}, "AAPL240223C00200000": {"ap": 1.1, "bp": 0.9}}[symbol]

    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", fake_quote)

    captured = {}
    monkeypatch.setattr(alpaca_client, "build_option_spread_order", lambda **kw: captured.update(kw) or {"order_class": "mleg"})
    monkeypatch.setattr(alpaca_client, "place_order", lambda spec: "order-1")

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    assert captured["long_symbol"] == "AAPL240223C00195000"
    assert captured["short_symbol"] == "AAPL240223C00200000"
    assert captured["limit_price"] == pytest.approx(1.0 * (1 + strat.SPREAD_LIMIT_SLIPPAGE_PCT))


def test_manage_open_positions_closes_a_debit_spread_on_take_profit(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "balance": 500.0, "positions": [{
            "symbol": "AAPL240223C00195000", "underlying_symbol": "AAPL", "strategy": "debit_spread",
            "short_symbol": "AAPL240223C00200000", "entry_price": 1.0, "count": 1,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
            "expiration_date": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=30)).date().isoformat(),
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    # Net spread value rises to well past TAKE_PROFIT_PCT above the 1.0 entry debit.
    target = 1.0 * (1 + strat.TAKE_PROFIT_PCT + 0.05)

    def fake_quote(symbol):
        return {"AAPL240223C00195000": {"ap": target + 1.0, "bp": target + 1.0}, "AAPL240223C00200000": {"ap": 1.0, "bp": 1.0}}[symbol]

    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", fake_quote)

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert result["closed"][0]["strategy"] == "debit_spread"
    assert "take_profit" in result["closed"][0]["reason"]


def test_manage_open_positions_live_mode_closes_a_spread_with_a_reversed_mleg_order(monkeypatch):
    monkeypatch.setattr(strat, "MODE", "live")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "balance": 500.0, "positions": [{
            "symbol": "AAPL240223C00195000", "underlying_symbol": "AAPL", "strategy": "debit_spread",
            "short_symbol": "AAPL240223C00200000", "entry_price": 1.0, "count": 1,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
            "expiration_date": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=30)).date().isoformat(),
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    target = 1.0 * (1 + strat.TAKE_PROFIT_PCT + 0.05)

    def fake_quote(symbol):
        return {"AAPL240223C00195000": {"ap": target + 1.0, "bp": target + 1.0}, "AAPL240223C00200000": {"ap": 1.0, "bp": 1.0}}[symbol]

    monkeypatch.setattr(alpaca_client, "get_option_latest_quote", fake_quote)

    captured = {}
    monkeypatch.setattr(alpaca_client, "build_option_spread_order", lambda **kw: captured.update(kw) or {"order_class": "mleg"})
    monkeypatch.setattr(alpaca_client, "place_order", lambda spec: "order-2")

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert captured["closing"] is True
    assert captured["long_symbol"] == "AAPL240223C00195000"
    assert captured["short_symbol"] == "AAPL240223C00200000"
