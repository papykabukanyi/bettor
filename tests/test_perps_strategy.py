"""Kalshi Perps strategy decision logic. Pure-function / mocked-network
tests covering the actual entry/exit rules: dip detection, the bearish trend
filter, the direction-model override, take-profit, stop-loss, max-hold-time
exits, dry-run gating, and the daily loss cap. If any of these regress, a
live (even dry-run) cycle could silently do the wrong thing with a leveraged
product -- so the core decisions are locked down here."""
from __future__ import annotations

import datetime as dt

from data import perps_strategy as strat


def _row(**overrides):
    base = {"ticker": "KXBTCPERP", "current_price": 6.60, "short_ma": 6.63, "trend_pct": 0.0}
    base.update(overrides)
    return base


def test_dip_in_flat_trend_triggers_technical_entry():
    should_enter, reason = strat.decide_entry_technical(_row())
    assert should_enter
    assert "dip" in reason


def test_dip_in_strong_downtrend_is_filtered_out():
    should_enter, reason = strat.decide_entry_technical(_row(trend_pct=-0.05))
    assert not should_enter
    assert "trend filter" in reason


def test_negligible_move_does_not_trigger_technical_entry():
    should_enter, _ = strat.decide_entry_technical(_row(current_price=6.632, short_ma=6.633))
    assert not should_enter


def _position(entry_price=6.60, minutes_ago=0):
    opened = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=minutes_ago)
    return {"entry_price": entry_price, "count": 1.0, "opened_at": opened.isoformat()}


def test_take_profit_exit():
    pos = _position()
    should_exit, reason = strat.decide_exit(pos, 6.60 * (1 + strat.TAKE_PROFIT_PCT + 0.001))
    assert should_exit and "take_profit" in reason


def test_stop_loss_exit():
    pos = _position()
    should_exit, reason = strat.decide_exit(pos, 6.60 * (1 - strat.STOP_LOSS_PCT - 0.001))
    assert should_exit and "stop_loss" in reason


def test_max_hold_time_forces_exit_even_at_small_gain():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(pos, 6.601)
    assert should_exit and "max_hold_time" in reason


def test_holds_when_nothing_triggered():
    pos = _position(minutes_ago=1)
    should_exit, reason = strat.decide_exit(pos, 6.605)
    assert not should_exit
    assert "holding" in reason


def test_evaluate_candidate_falls_back_to_technical_when_model_not_trained(monkeypatch):
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False, "ticker": ticker})
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True
    assert result["model_ok"] is False
    assert "fallback" in result["reason"]


def test_evaluate_candidate_model_blocks_entry_when_predicting_down(monkeypatch):
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "down", "probability_up": 0.3,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is False
    assert result["model_ok"] is True


def test_evaluate_candidate_model_confirms_entry_with_high_confidence(monkeypatch):
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.9,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True
    assert result["score"] == 0.9


def _market_response(price=6.60, tick_size=0.0001):
    return {"market": {"price": price, "tick_size": tick_size}}


def test_dry_run_never_places_a_real_order(monkeypatch, tmp_path):
    """Regardless of the trading-enabled flag's value, passing dry_run=True
    (or the module default when the env flag is off) must never call the
    real order-placement function."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)

    def fail_if_called(*a, **k):
        raise AssertionError("create_margin_order must not be called while dry-run is in effect")

    monkeypatch.setattr(strat, "create_margin_order", fail_if_called)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response())
    monkeypatch.setattr(
        strat, "scan_for_best_entry",
        lambda tickers=None: ({"ticker": "KXBTCPERP", "current_price": 6.60, "reason": "test dip"}, []),
    )

    result = strat.run_cycle()
    assert result["dry_run"] is True
    assert result["action"] == "opened"


def test_daily_loss_cap_blocks_new_entries(monkeypatch, tmp_path):
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(strat, "STATE_FILE", state_file)
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    strat._save_state({
        "position": None,
        "realized_pnl_by_date": {strat._today_str(): -abs(strat.DAILY_LOSS_CAP_USD) - 1.0},
        "trade_log": [],
    })
    result = strat.run_cycle()
    assert result["action"] == "skipped_daily_loss_cap"


def test_only_one_position_at_a_time(monkeypatch, tmp_path):
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(strat, "STATE_FILE", state_file)
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=6.605))
    strat._save_state({
        "position": {"ticker": "KXBTCPERP", "entry_price": 6.55, "count": 1.0,
                      "opened_at": dt.datetime.now(dt.timezone.utc).isoformat()},
        "realized_pnl_by_date": {}, "trade_log": [],
    })
    result = strat.run_cycle()
    # With an existing position open, a cycle must manage THAT position
    # (check its exit), never open a second one alongside it.
    assert result["action"] in ("none", "closed")
    assert result["ticker"] == "KXBTCPERP"
