"""Schwab equities strategy decision logic -- separate from and parallel to
test_perps_strategy.py. Pure-function tests: volume/volatility entry gate,
take-profit/stop-loss/max-hold exits, position sizing."""
from __future__ import annotations

import datetime as dt

from data import schwab_strategy as strat


def _row(**overrides):
    base = {
        "symbol": "AAPL", "current_price": 100.0, "short_ma": 100.3,
        "dollar_volume_z": 2.0, "volatility_5": 0.002, "volatility_30": 0.001,
    }
    base.update(overrides)
    return base


def test_entry_requires_an_unusual_volume_spike():
    should_enter, reason = strat.decide_entry_technical(_row(dollar_volume_z=0.5))
    assert not should_enter
    assert "volume" in reason


def test_entry_requires_elevated_volatility_relative_to_its_own_baseline():
    should_enter, reason = strat.decide_entry_technical(_row(volatility_5=0.001, volatility_30=0.001))
    assert not should_enter
    assert "volatile" in reason


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
    result = strat.evaluate_candidate(_row(dollar_volume_z=0.1), {"model_ok": True, "probability_up": 0.9})
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
    # $1000 budget * 10% = $100, at $30/share -> 3 whole shares, not 3.33
    count = strat.compute_position_size(1000.0, 30.0)
    assert count == int((1000.0 * strat.POSITION_SIZE_PCT) // 30.0)


def test_compute_position_size_zero_at_zero_price():
    assert strat.compute_position_size(1000.0, 0.0) == 0
