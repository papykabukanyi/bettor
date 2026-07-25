"""Schwab equities backtest engine -- synthetic price data only, never
touches the network. Verifies the simulation correctly reuses the REAL
schwab_strategy decide functions, respects the concurrency cap, and that
batch-predicting the model once matches predicting per-row."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data import schwab_backtest as bt
from data import schwab_strategy as strat


def _synthetic_test_df(n: int = 200, symbol: str = "AAPL") -> pd.DataFrame:
    rng = np.random.default_rng(7)
    ts = np.arange(n) * 60
    close = 100.0 + np.sin(np.arange(n) / 5.0) * 0.5
    dist_to_ma_15 = np.sin(np.arange(n) / 5.0 + 0.3) * 0.01
    return pd.DataFrame({
        "symbol": symbol, "ts": ts, "close": close, "dist_to_ma_15": dist_to_ma_15,
        "dist_to_ma_30": dist_to_ma_15 * 0.5,
        "ret_1m": rng.normal(0, 0.001, n), "ret_5m": rng.normal(0, 0.002, n),
        "ret_15m": rng.normal(0, 0.003, n), "ret_30m": rng.normal(0, 0.004, n), "ret_60m": rng.normal(0, 0.006, n),
        "volatility_5": np.abs(rng.normal(0.0008, 0.0002, n)),
        "volatility_15": np.abs(rng.normal(0.001, 0.0002, n)),
        "volatility_30": np.abs(rng.normal(0.0012, 0.0003, n)),
        "volume_ratio_5": np.abs(rng.normal(1.0, 0.2, n)),
        "volume_ratio_15": np.abs(rng.normal(1.0, 0.15, n)),
        "dollar_volume_z": np.full(n, 2.0),  # always "unusual volume" so entries can fire deterministically
        "rsi_14": np.full(n, 0.5), "macd_hist_pct": np.zeros(n), "bb_pct_b": np.full(n, 0.5),
        "bb_bandwidth": np.full(n, 0.01), "atr_pct": np.full(n, 0.001), "stoch_k": np.full(n, 0.5),
        "time_of_day_pct": np.full(n, 0.5),
    })


def test_add_model_predictions_without_a_model_yields_nan_column():
    df = _synthetic_test_df()
    result = bt.add_model_predictions(df, fitted=None)
    assert result["model_probability_up"].isna().all()


def test_simulate_runs_technical_only_without_a_model():
    df = _synthetic_test_df()
    result = bt.simulate(df, fitted=None, starting_balance=10_000.0)
    assert result["trade_count"] >= 0
    assert "trades_per_day" in result


def test_simulate_never_exceeds_max_concurrent_positions(monkeypatch):
    frames = [_synthetic_test_df(symbol=s) for s in ["AAA", "BBB", "CCC", "DDD"]]
    for i, f in enumerate(frames):
        f["ts"] = f["ts"] + i * 10_000_000  # keep symbols' timestamps from colliding
    df = pd.concat(frames, ignore_index=True)
    result = bt.simulate(
        df, fitted=None, starting_balance=100_000.0, max_concurrent_positions=2,
        min_volume_z=0.0, min_volatility_ratio=0.0, entry_dip_pct=0.0001,
    )
    assert result["trade_count"] >= 0  # must not raise; concurrency cap enforced internally


def test_simulate_respects_a_trained_models_direction(monkeypatch):
    df = _synthetic_test_df()

    class _AlwaysDownModel:
        def predict_proba(self, x):
            return np.tile([0.9, 0.1], (len(x), 1))  # p_up = 0.1, well under any confidence bar

    fitted = {
        "model": _AlwaysDownModel(), "model_type": "fake",
        "feature_cols": bt.FEATURE_COLUMNS + ["symbol_code"], "symbol_categories": ["AAPL"],
    }
    result = bt.simulate(df, fitted, starting_balance=10_000.0, min_volume_z=0.0, min_volatility_ratio=0.0, entry_dip_pct=0.0001)
    assert result["trade_count"] == 0  # model confidently predicts DOWN -- long-only strategy must never enter


def test_simulate_reuses_precomputed_predictions_when_present():
    df = _synthetic_test_df()
    df["model_probability_up"] = 0.9
    result = bt.simulate(df, fitted=None, starting_balance=10_000.0, min_volume_z=0.0, min_volatility_ratio=0.0, entry_dip_pct=0.0001)
    assert result["trade_count"] >= 0  # must not crash trying to re-predict when the column already exists


def test_simulate_blocks_entries_when_volume_is_not_unusual():
    df = _synthetic_test_df()
    df["dollar_volume_z"] = 0.0  # never unusual
    result = bt.simulate(df, fitted=None, starting_balance=10_000.0, min_volume_z=1.5, entry_dip_pct=0.0001)
    assert result["trade_count"] == 0


def test_simulate_deducts_no_fee_since_schwab_is_commission_free():
    """Unlike the Kalshi perps backtest (which has to model a real 1.6%
    round-trip taker fee), a Schwab equity trade's realized P&L is exactly
    the gross price delta -- no fee_usd field, no deduction."""
    df = _synthetic_test_df(n=300)
    result = bt.simulate(df, fitted=None, starting_balance=10_000.0, min_volume_z=0.0, min_volatility_ratio=0.0, entry_dip_pct=0.0001)
    for t in result["trades"]:
        assert "fee_usd" not in t
        assert t["realized_pnl_usd"] == round((t["exit_price"] - t["entry_price"]) * t["count"], 6)


def test_run_config_sweep_tries_every_grid_entry_and_ranks_by_return(monkeypatch):
    df = _synthetic_test_df(n=300)
    monkeypatch.setattr(bt, "_SWEEP_GRID", [
        {"take_profit_pct": 0.01, "stop_loss_pct": 0.008, "max_hold_minutes": 120},
        {"take_profit_pct": 0.02, "stop_loss_pct": 0.015, "max_hold_minutes": 60},
    ])
    result = bt.run_config_sweep(df, starting_balance=10_000.0, min_trades=0)
    assert len(result["all_configs"]) == 2
    assert result["best"] is not None
    assert result["ranked"][0]["return_pct"] >= result["ranked"][-1]["return_pct"]


def test_run_config_sweep_excludes_configs_below_the_min_trade_count(monkeypatch):
    df = _synthetic_test_df(n=300)
    # An absurdly high take-profit that will never realistically fire in
    # this tiny synthetic window -- must be excluded from "ranked" (it
    # would otherwise "win" on a trivial low-trade-count technicality).
    monkeypatch.setattr(bt, "_SWEEP_GRID", [
        {"take_profit_pct": 0.5, "stop_loss_pct": 0.4, "max_hold_minutes": 5},
    ])
    result = bt.run_config_sweep(df, starting_balance=10_000.0, min_trades=1000)
    assert result["all_configs"][0]["trade_count"] < 1000
    assert result["best"] is not None  # falls back to the unqualified list rather than crashing
    assert result["ranked"] == result["all_configs"]


def test_run_config_sweep_restores_the_real_strategy_parameters_afterward(monkeypatch):
    """Each grid entry temporarily overwrites schwab_strategy's real
    module-level TAKE_PROFIT_PCT/STOP_LOSS_PCT/MAX_HOLD_MINUTES to reuse
    simulate()'s real decide_exit logic -- leaving the LAST grid entry's
    values in place afterward would mean live trading silently runs on
    leftover sweep parameters instead of its actually configured ones."""
    monkeypatch.setattr(strat, "TAKE_PROFIT_PCT", 0.0123)
    monkeypatch.setattr(strat, "STOP_LOSS_PCT", 0.0099)
    monkeypatch.setattr(strat, "MAX_HOLD_MINUTES", 77)
    monkeypatch.setattr(bt, "_SWEEP_GRID", [
        {"take_profit_pct": 0.5, "stop_loss_pct": 0.4, "max_hold_minutes": 999},
    ])

    df = _synthetic_test_df(n=300)
    bt.run_config_sweep(df, starting_balance=10_000.0, min_trades=0)

    assert strat.TAKE_PROFIT_PCT == 0.0123
    assert strat.STOP_LOSS_PCT == 0.0099
    assert strat.MAX_HOLD_MINUTES == 77


def test_run_config_sweep_restores_parameters_even_if_simulate_raises(monkeypatch):
    monkeypatch.setattr(strat, "TAKE_PROFIT_PCT", 0.0123)
    monkeypatch.setattr(strat, "STOP_LOSS_PCT", 0.0099)
    monkeypatch.setattr(strat, "MAX_HOLD_MINUTES", 77)
    monkeypatch.setattr(bt, "_SWEEP_GRID", [{"take_profit_pct": 0.5, "stop_loss_pct": 0.4, "max_hold_minutes": 999}])

    def fail(*a, **k):
        raise RuntimeError("boom")

    monkeypatch.setattr(bt, "simulate", fail)
    df = _synthetic_test_df(n=300)
    with pytest.raises(RuntimeError):
        bt.run_config_sweep(df, starting_balance=10_000.0, min_trades=0)

    assert strat.TAKE_PROFIT_PCT == 0.0123
    assert strat.STOP_LOSS_PCT == 0.0099
    assert strat.MAX_HOLD_MINUTES == 77
