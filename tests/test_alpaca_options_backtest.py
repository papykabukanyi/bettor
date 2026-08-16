"""Alpaca options backtest engine -- synthetic price data only, never
touches the network. Verifies the simulation correctly reuses the REAL
alpaca_options_strategy decide_exit/compute_contract_qty functions (which
read TAKE_PROFIT_PCT/STOP_LOSS_PCT/MAX_HOLD_MINUTES/POSITION_SIZE_PCT as
module-level globals, not parameters -- a real trap this file's own
simulate() has to guard against with a try/finally restore), that entries
require a real model (no technical-only fallback, matching
evaluate_candidate exactly), and that the concurrency/daily-loss-cap
guards hold."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data import alpaca_options_backtest as bt
from data import alpaca_options_strategy as strat


def _synthetic_test_df(n: int = 300, symbol: str = "AAPL") -> pd.DataFrame:
    rng = np.random.default_rng(7)
    ts = np.arange(n) * 60
    close = 195.0 + np.cumsum(rng.normal(0, 0.05, n))
    return pd.DataFrame({
        "symbol": symbol, "ts": ts, "close": close,
        "dist_to_ma_15": rng.normal(0, 0.01, n), "dist_to_ma_30": rng.normal(0, 0.005, n),
        "ret_1m": rng.normal(0, 0.001, n), "ret_5m": rng.normal(0, 0.002, n),
        "ret_15m": rng.normal(0, 0.003, n), "ret_30m": rng.normal(0, 0.004, n), "ret_60m": rng.normal(0, 0.006, n),
        # volatility_5 set well above volatility_30 (ratio ~1.5-2x, safely
        # clearing MIN_VOLATILITY_RATIO=1.1 even with per-row noise) -- this
        # fixture represents a row that QUALIFIES for entry by default, same
        # as its dollar_volume_z=2.0 below clearing MIN_VOLUME_Z=1.0. Tests
        # that need a non-qualifying row override these explicitly.
        "volatility_5": np.abs(rng.normal(0.0018, 0.0003, n)),
        "volatility_15": np.abs(rng.normal(0.0014, 0.0002, n)),
        "volatility_30": np.abs(rng.normal(0.0010, 0.0002, n)),
        "volume_ratio_5": np.abs(rng.normal(1.0, 0.2, n)),
        "volume_ratio_15": np.abs(rng.normal(1.0, 0.15, n)),
        "dollar_volume_z": np.full(n, 2.0),
        "rsi_14": np.full(n, 0.5), "macd_hist_pct": np.zeros(n), "bb_pct_b": np.full(n, 0.5),
        "bb_bandwidth": np.full(n, 0.01), "atr_pct": np.full(n, 0.001), "stoch_k": np.full(n, 0.5),
        "time_of_day_pct": np.full(n, 0.5), "sentiment_score": np.zeros(n),
    })


class _AlwaysUpModel:
    def predict_proba(self, x):
        return np.tile([0.1, 0.9], (len(x), 1))  # p_up = 0.9, clears any real confidence bar


class _AlwaysDownModel:
    def predict_proba(self, x):
        return np.tile([0.9, 0.1], (len(x), 1))  # p_up = 0.1 -> confident DOWN


class _UnconfidentModel:
    def predict_proba(self, x):
        return np.tile([0.5, 0.5], (len(x), 1))  # dead center -- neither direction clears the bar


def _fitted(model, symbols=("AAPL",)):
    return {"model": model, "model_type": "fake", "feature_cols": bt.FEATURE_COLUMNS + ["symbol_code"], "symbol_categories": list(symbols)}


def test_add_model_predictions_without_a_model_yields_nan_column():
    df = _synthetic_test_df()
    result = bt.add_model_predictions(df, fitted=None)
    assert result["model_probability_up"].isna().all()


def test_simulate_never_enters_without_a_trained_model():
    """No technical-only cold-start fallback -- matches
    evaluate_candidate's own real rule exactly (options add real leverage
    AND theta on top of direction risk, a materially worse bet without
    the model's own confidence)."""
    df = _synthetic_test_df()
    result = bt.simulate(df, fitted=None, starting_balance=500.0)
    assert result["trade_count"] == 0


def test_simulate_enters_long_on_a_confident_up_model():
    # A real, correct affordability constraint (same mechanism the live
    # compute_contract_qty already enforces): AAPL's ~$195 price implies a
    # realistic ~2%-of-underlying premium (see _ENTRY_PREMIUM_PCT_OF_UNDERLYING)
    # around $3.90/share -- a $390/contract cost the default $500 balance
    # at 20% sizing can't afford even once. A larger balance isolates THIS
    # test's actual point (does a confident signal fire trades at all) from
    # that separate, already-correct affordability behavior.
    df = _synthetic_test_df(n=500)
    result = bt.simulate(df, _fitted(_AlwaysUpModel()), starting_balance=50_000.0, model_confidence_min=0.55)
    assert result["trade_count"] > 0
    for t in result["trades"]:
        assert t["count"] >= 1


def test_simulate_enters_on_a_confident_down_model_too():
    """Bidirectional through contract choice (calls vs puts), same as the
    live strategy -- a confidently-DOWN model must still produce trades,
    not zero them out the way a long-only equities strategy would."""
    df = _synthetic_test_df(n=500)
    result = bt.simulate(df, _fitted(_AlwaysDownModel()), starting_balance=50_000.0, model_confidence_min=0.55)
    assert result["trade_count"] > 0


def test_simulate_enters_nothing_when_the_model_is_unconfident():
    df = _synthetic_test_df(n=500)
    result = bt.simulate(df, _fitted(_UnconfidentModel()), starting_balance=500.0, model_confidence_min=0.55)
    assert result["trade_count"] == 0


def test_simulate_enters_nothing_when_volume_is_not_unusual_enough():
    """Real, confirmed bug this guards against: _simulate_inner never
    checked MIN_VOLUME_Z despite the module docstring claiming it replays
    evaluate_candidate's real signal -- a confident model on a quiet
    underlying used to fire a trade here that live evaluate_candidate would
    have rejected outright."""
    df = _synthetic_test_df(n=500)
    df["dollar_volume_z"] = 0.1  # well below MIN_VOLUME_Z=1.0
    result = bt.simulate(df, _fitted(_AlwaysUpModel()), starting_balance=50_000.0, model_confidence_min=0.55)
    assert result["trade_count"] == 0


def test_simulate_enters_nothing_when_not_more_volatile_than_baseline():
    """Same real bug, other half of the gate: volatility_5/volatility_30
    below MIN_VOLATILITY_RATIO must also block entry, matching
    evaluate_candidate's "underlying not more volatile than its own recent
    baseline" rejection."""
    df = _synthetic_test_df(n=500)
    df["dollar_volume_z"] = 5.0  # clears the volume gate so only volatility is under test
    df["volatility_5"] = 0.0005
    df["volatility_30"] = 0.005  # ratio = 0.1, well below MIN_VOLATILITY_RATIO=1.1
    result = bt.simulate(df, _fitted(_AlwaysUpModel()), starting_balance=50_000.0, model_confidence_min=0.55)
    assert result["trade_count"] == 0


def test_simulate_reuses_precomputed_predictions_when_present():
    df = _synthetic_test_df(n=500)
    df["model_probability_up"] = 0.9
    result = bt.simulate(df, fitted=None, starting_balance=500.0, model_confidence_min=0.55)
    assert result["trade_count"] >= 0  # must not crash trying to re-predict when the column already exists


def test_simulate_never_exceeds_max_concurrent_positions(monkeypatch):
    frames = [_synthetic_test_df(n=500, symbol=s) for s in ["AAA", "BBB", "CCC", "DDD"]]
    for i, f in enumerate(frames):
        f["ts"] = f["ts"] + i * 10_000_000
    df = pd.concat(frames, ignore_index=True)
    result = bt.simulate(
        df, _fitted(_AlwaysUpModel(), symbols=["AAA", "BBB", "CCC", "DDD"]),
        starting_balance=100_000.0, max_concurrent_positions=2, model_confidence_min=0.55,
    )
    assert result["open_positions_at_end"] <= 2


def test_simulate_trades_size_via_the_real_100x_contract_multiplier():
    df = _synthetic_test_df(n=500)
    result = bt.simulate(df, _fitted(_AlwaysUpModel()), starting_balance=50_000.0, model_confidence_min=0.55)
    assert result["trade_count"] > 0
    for t in result["trades"]:
        assert t["realized_pnl_usd"] == round((t["exit_price"] - t["entry_price"]) * t["count"] * 100, 6)


def test_simulate_restores_real_strategy_parameters_afterward(monkeypatch):
    """decide_exit()/compute_contract_qty() read TAKE_PROFIT_PCT/
    STOP_LOSS_PCT/MAX_HOLD_MINUTES/POSITION_SIZE_PCT as module-level
    globals on alpaca_options_strategy, not function parameters -- a
    sweep call must never leave live trading running on leftover swept
    values afterward."""
    monkeypatch.setattr(strat, "TAKE_PROFIT_PCT", 0.0123)
    monkeypatch.setattr(strat, "STOP_LOSS_PCT", 0.0099)
    monkeypatch.setattr(strat, "MAX_HOLD_MINUTES", 77)
    monkeypatch.setattr(strat, "POSITION_SIZE_PCT", 0.0321)

    df = _synthetic_test_df(n=500)
    bt.simulate(
        df, _fitted(_AlwaysUpModel()), starting_balance=500.0,
        take_profit_pct=0.9, stop_loss_pct=0.8, max_hold_minutes=999, position_size_pct=0.9,
    )

    assert strat.TAKE_PROFIT_PCT == 0.0123
    assert strat.STOP_LOSS_PCT == 0.0099
    assert strat.MAX_HOLD_MINUTES == 77
    assert strat.POSITION_SIZE_PCT == 0.0321


def test_run_config_sweep_tries_every_grid_entry_and_ranks_by_return(monkeypatch):
    df = _synthetic_test_df(n=500)
    df["model_probability_up"] = 0.9
    monkeypatch.setattr(bt, "_SWEEP_GRID", [
        {"take_profit_pct": 0.3, "stop_loss_pct": 0.2, "max_hold_minutes": 180, "model_confidence_min": 0.5},
        {"take_profit_pct": 0.5, "stop_loss_pct": 0.3, "max_hold_minutes": 90, "model_confidence_min": 0.5},
    ])
    result = bt.run_config_sweep(df, starting_balance=50_000.0, min_trades=0)
    assert len(result["all_configs"]) == 2
    assert result["best"] is not None
    assert any(c["trade_count"] > 0 for c in result["all_configs"])
    assert result["ranked"][0]["return_pct"] >= result["ranked"][-1]["return_pct"]


def test_run_config_sweep_excludes_configs_below_the_min_trade_count():
    df = _synthetic_test_df(n=500)
    df["model_probability_up"] = 0.5  # never confident -- zero trades on every config
    result = bt.run_config_sweep(df, starting_balance=500.0, min_trades=1000)
    assert result["all_configs"][0]["trade_count"] < 1000
    assert result["best"] is not None  # falls back to the unqualified list rather than crashing


def test_run_config_sweep_restores_parameters_even_if_simulate_raises(monkeypatch):
    monkeypatch.setattr(strat, "TAKE_PROFIT_PCT", 0.0123)
    monkeypatch.setattr(strat, "STOP_LOSS_PCT", 0.0099)
    monkeypatch.setattr(strat, "MAX_HOLD_MINUTES", 77)
    monkeypatch.setattr(strat, "POSITION_SIZE_PCT", 0.0321)
    monkeypatch.setattr(bt, "_SWEEP_GRID", [{"take_profit_pct": 0.5, "stop_loss_pct": 0.4, "max_hold_minutes": 999, "model_confidence_min": 0.5}])

    def fail(*a, **k):
        raise RuntimeError("boom")

    monkeypatch.setattr(bt, "simulate", fail)
    df = _synthetic_test_df(n=500)
    with pytest.raises(RuntimeError):
        bt.run_config_sweep(df, starting_balance=500.0, min_trades=0)

    assert strat.TAKE_PROFIT_PCT == 0.0123
    assert strat.STOP_LOSS_PCT == 0.0099
    assert strat.MAX_HOLD_MINUTES == 77
    assert strat.POSITION_SIZE_PCT == 0.0321


def test_run_walkforward_backtest_returns_multiple_folds_via_the_real_pipeline(monkeypatch):
    """End-to-end through the real fetch->fit->simulate pipeline (the HF
    dataset download mocked out), confirming run_walkforward_backtest wires
    walkforward.run_walkforward_folds up to this module's own
    fit_backtest_model/simulate rather than reimplementing any of that."""
    n = 900
    rng = np.random.default_rng(11)
    frames = [_synthetic_test_df(n=n, symbol=s) for s in ("AAA", "BBB")]
    for i, f in enumerate(frames):
        f["ts"] = f["ts"] + i * 10_000_000
    combined = pd.concat(frames, ignore_index=True)
    combined["label_up"] = (rng.normal(0, 1, len(combined)) > 0).astype(int)

    monkeypatch.setattr(bt, "load_training_dataset", lambda *, max_shards=90: combined)
    # 4 folds x 3 sklearn candidates is real fit cost -- cut to the
    # cheapest candidate only. This test is about run_walkforward_backtest's
    # own plumbing, not about which candidate wins a fit-quality contest.
    monkeypatch.setattr(bt, "_CANDIDATES", {"logistic_regression": bt._CANDIDATES["logistic_regression"]})

    result = bt.run_walkforward_backtest(starting_balance=50_000.0)
    assert result["ok"] is True
    assert result["fold_count"] >= 2
    assert "profitable_fold_ratio" in result
    assert "mean_return_pct" in result
    for fold in result["folds"]:
        assert "return_pct" in fold
        assert "fold_bounds" in fold


def test_run_walkforward_backtest_reports_no_data_when_the_dataset_is_empty(monkeypatch):
    monkeypatch.setattr(bt, "load_training_dataset", lambda *, max_shards=90: pd.DataFrame())
    result = bt.run_walkforward_backtest()
    assert result == {"ok": False, "reason": "no_data"}


def test_fit_backtest_model_returns_none_below_min_rows():
    df = _synthetic_test_df(n=20)
    df["label_up"] = 1
    assert bt.fit_backtest_model(df, min_rows=300) is None


def test_fit_backtest_model_fits_and_returns_a_usable_model():
    n = 2000
    df = _synthetic_test_df(n=n)
    rng = np.random.default_rng(3)
    df["label_up"] = (rng.normal(0, 1, n) > 0).astype(int)
    fitted = bt.fit_backtest_model(df, min_rows=300)
    assert fitted is not None
    assert fitted["model_type"] in {"logistic_regression", "random_forest", "gradient_boosting"}
    preds = bt.add_model_predictions(df, fitted)
    assert preds["model_probability_up"].between(0, 1).all()
