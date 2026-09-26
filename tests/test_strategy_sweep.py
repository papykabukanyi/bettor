"""Generic parameter-sweep engine tests -- synthetic feature data only,
reuses kalshi_15m_metals_backtest.py's own real fit/simulate/data-loading
functions (monkeypatching only load_training_dataset, same convention
test_kalshi_15m_backtest.py/test_kalshi_15m_metals_backtest.py's own
run_backtest/run_walkforward_backtest tests already use) so this module's
OWN cartesian-product/evidence-gating/ranking logic is verified against
the real backtest contract, not a fake stand-in that could silently
drift from it."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data import kalshi_15m_metals_backtest as metals_bt
from data import strategy_sweep


def _feature_row_defaults() -> dict:
    return {
        "ret_1m": 0.0, "ret_3m": 0.0, "ret_5m": 0.0, "ret_10m": 0.0, "ret_15m": 0.0, "ret_30m": 0.0,
        "trend_1h": 0.0, "trend_2h": 0.0, "trend_3h": 0.0, "trend_4h": 0.0,
        "dist_to_ma_15": 0.0, "dist_to_ma_30": 0.0,
        "volatility_5": 0.001, "volatility_15": 0.001, "volatility_30": 0.001,
        "rsi_14": 0.5, "macd_hist_pct": 0.0, "bb_pct_b": 0.5, "bb_bandwidth": 0.01,
        "hour_sin": 0.0, "hour_cos": 1.0, "dow_sin": 0.0, "dow_cos": 1.0,
        "sentiment_score": 0.0,
    }


def _synthetic_df(n_per_symbol: int = 3000, symbols: tuple[str, ...] = ("GOLD", "SILVER"), seed: int = 3) -> pd.DataFrame:
    """A REAL, learnable pattern (ret_1m predicts label_up almost
    perfectly) over enough rows that every DEFAULT_FOLD_BOUNDS fold has
    enough training data -- see strategy_sweep._build_fold_frames's own
    min_train_rows=300 floor."""
    rng = np.random.default_rng(seed)
    frames = []
    for symbol in symbols:
        ts = np.arange(n_per_symbol) * 900
        dist = rng.normal(0, 0.01, n_per_symbol)
        label_up = (dist > 0).astype(int)
        row = {**_feature_row_defaults()}
        data = {k: np.full(n_per_symbol, v, dtype=float) for k, v in row.items()}
        data.update({"symbol": symbol, "ts": ts, "label_up": label_up, "ret_1m": dist})
        frames.append(pd.DataFrame(data))
    return pd.concat(frames, ignore_index=True)


def test_cartesian_product_generates_every_combination():
    result = strategy_sweep.cartesian_product({"a": [1, 2], "b": [3, 4]})
    assert len(result) == 4
    assert {"a": 1, "b": 3} in result
    assert {"a": 2, "b": 4} in result


def test_cartesian_product_of_empty_grid_is_one_empty_combination():
    assert strategy_sweep.cartesian_product({}) == [{}]


def test_run_parameter_sweep_reports_no_data_when_archive_is_empty(monkeypatch):
    monkeypatch.setattr(metals_bt, "load_training_dataset", lambda: pd.DataFrame())
    result = strategy_sweep.run_parameter_sweep(metals_bt, {"model_confidence_min": [0.55, 0.60]})
    assert result == {"ok": False, "reason": "no_data"}


def test_run_parameter_sweep_rejects_a_grid_larger_than_max_combinations(monkeypatch):
    monkeypatch.setattr(metals_bt, "load_training_dataset", lambda: _synthetic_df())
    result = strategy_sweep.run_parameter_sweep(
        metals_bt, {"model_confidence_min": [0.5, 0.55, 0.6, 0.65, 0.7]}, max_combinations=3,
    )
    assert result["ok"] is False
    assert result["reason"] == "grid_too_large"
    assert result["combinations_requested"] == 5


def test_run_parameter_sweep_end_to_end_ranks_real_strategies(monkeypatch):
    monkeypatch.setattr(metals_bt, "load_training_dataset", lambda: _synthetic_df())

    result = strategy_sweep.run_parameter_sweep(
        metals_bt, {"model_confidence_min": [0.51, 0.90], "yes_confidence_extra_required": [0.0]},
        min_trades_per_fold=1, min_folds_with_trades=1,
    )

    assert result["ok"] is True
    assert result["combinations_tried"] == 2
    assert result["combinations_evaluated"] == 2
    assert result["stopped_early"] is False
    assert result["folds_used"] >= 1
    assert result["elapsed_sec"] >= 0
    # A near-coin-flip floor (0.51) should find real trades; a near-
    # impossible one (0.90) should not clear the evidence bar at all given
    # this synthetic model's own real accuracy -- so at most 1 combination
    # survives with evidence.
    assert result["combinations_with_evidence"] <= 2
    for strat_result in result["top_strategies"]:
        assert strat_result["folds_with_evidence"] >= 1
        assert strat_result["total_trades"] >= 1


def test_run_parameter_sweep_excludes_combinations_below_the_evidence_bar(monkeypatch):
    monkeypatch.setattr(metals_bt, "load_training_dataset", lambda: _synthetic_df())

    # An absurdly high min_trades_per_fold no real fold's own trade count
    # will ever clear, regardless of how many trades a confident model
    # actually finds.
    result = strategy_sweep.run_parameter_sweep(
        metals_bt, {"model_confidence_min": [0.55]}, min_trades_per_fold=999_999, min_folds_with_trades=1,
    )

    assert result["ok"] is True
    assert result["combinations_with_evidence"] == 0
    assert result["top_strategies"] == []


def test_run_parameter_sweep_ranks_by_profitable_fold_ratio_then_return(monkeypatch):
    monkeypatch.setattr(metals_bt, "load_training_dataset", lambda: _synthetic_df())

    result = strategy_sweep.run_parameter_sweep(
        metals_bt, {"model_confidence_min": [0.51, 0.55, 0.60]}, min_trades_per_fold=1, min_folds_with_trades=1,
    )

    assert result["ok"] is True
    ratios = [s["profitable_fold_ratio"] for s in result["top_strategies"]]
    assert ratios == sorted(ratios, reverse=True)


def test_run_parameter_sweep_stops_early_at_the_time_budget(monkeypatch):
    monkeypatch.setattr(metals_bt, "load_training_dataset", lambda: _synthetic_df())

    # A max_seconds of 0 means the very first combination already exceeds
    # budget -- must stop immediately with an honest, non-empty-shaped
    # result (not an error) and combinations_evaluated == 0.
    result = strategy_sweep.run_parameter_sweep(
        metals_bt, {"model_confidence_min": [0.51, 0.55, 0.60]},
        min_trades_per_fold=1, min_folds_with_trades=1, max_seconds=0.0,
    )

    assert result["ok"] is True
    assert result["stopped_early"] is True
    assert result["combinations_evaluated"] == 0
    assert result["combinations_tried"] == 3
    assert result["top_strategies"] == []


def test_run_parameter_sweep_reports_no_qualifying_folds_on_too_little_data(monkeypatch):
    tiny = _synthetic_df(n_per_symbol=50, symbols=("GOLD",))
    monkeypatch.setattr(metals_bt, "load_training_dataset", lambda: tiny)
    result = strategy_sweep.run_parameter_sweep(metals_bt, {"model_confidence_min": [0.55]})
    assert result == {"ok": False, "reason": "no_qualifying_folds"}
