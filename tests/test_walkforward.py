"""Shared walk-forward multi-fold backtest driver -- pure logic tests using
fake fit_fn/simulate_fn (the fold-slicing/aggregation logic is generic
across all 4 services, so this file never touches any real strategy code
or synthetic feature dataframes -- see each service's own test_*_backtest.py
for end-to-end tests wired to its real fit_backtest_model/simulate)."""
from __future__ import annotations

import pandas as pd
import pytest

from data import walkforward as wf


def _combined_df(n: int = 1000) -> pd.DataFrame:
    return pd.DataFrame({"ts": range(n), "value": range(n)})


def _fake_fit(train_df):
    return {"model_type": "fake", "train_len": len(train_df)}


def _fake_simulate(test_df, fitted, **kwargs):
    return {
        "trade_count": len(test_df),
        "win_count": len(test_df) // 2,
        "win_rate": 0.5,
        "return_pct": 0.1,
        "total_realized_pnl_usd": 10.0,
    }


def test_run_walkforward_folds_returns_no_data_for_an_empty_df():
    result = wf.run_walkforward_folds(pd.DataFrame(), fit_fn=_fake_fit, simulate_fn=_fake_simulate)
    assert result == {"ok": False, "reason": "no_data"}


def test_run_walkforward_folds_uses_default_fold_bounds_by_default():
    df = _combined_df()
    result = wf.run_walkforward_folds(df, fit_fn=_fake_fit, simulate_fn=_fake_simulate)
    assert result["ok"] is True
    assert result["fold_count"] == len(wf.DEFAULT_FOLD_BOUNDS)
    for fold, bounds in zip(result["folds"], wf.DEFAULT_FOLD_BOUNDS):
        assert fold["fold_bounds"] == list(bounds)


def test_run_walkforward_folds_accepts_custom_fold_bounds():
    df = _combined_df()
    result = wf.run_walkforward_folds(df, fit_fn=_fake_fit, simulate_fn=_fake_simulate, fold_bounds=[(0.0, 0.5, 1.0)])
    assert result["fold_count"] == 1
    assert result["folds"][0]["fold_bounds"] == [0.0, 0.5, 1.0]


def test_run_walkforward_folds_expanding_window_trains_on_strictly_more_rows_each_fold():
    captured_train_lens = []

    def capture_fit(train_df):
        captured_train_lens.append(len(train_df))
        return {"model_type": "fake"}

    wf.run_walkforward_folds(_combined_df(), fit_fn=capture_fit, simulate_fn=_fake_simulate)
    assert captured_train_lens == sorted(captured_train_lens)
    assert len(set(captured_train_lens)) == len(captured_train_lens)  # strictly increasing, no ties


def test_run_walkforward_folds_test_slice_matches_the_configured_quantile_window():
    captured_test_lens = []

    def capture_simulate(test_df, fitted, **kwargs):
        captured_test_lens.append(len(test_df))
        return _fake_simulate(test_df, fitted, **kwargs)

    wf.run_walkforward_folds(_combined_df(n=1000), fit_fn=_fake_fit, simulate_fn=capture_simulate)
    # Every default fold's test window spans 15 points of the timeline (e.g. 0.40-0.55).
    for count in captured_test_lens:
        assert 100 <= count <= 200


def test_run_walkforward_folds_skips_a_fold_with_too_few_training_rows():
    df = _combined_df(n=100)  # every fold's train slice is far below the 300-row default gate
    result = wf.run_walkforward_folds(df, fit_fn=_fake_fit, simulate_fn=_fake_simulate, min_train_rows=300)
    assert result == {"ok": False, "reason": "no_fold_had_enough_data"}


def test_run_walkforward_folds_skips_only_the_folds_below_the_row_threshold():
    # First custom fold has a tiny train slice (below min_train_rows); the
    # second has plenty -- only the first should be skipped, not the whole run.
    df = _combined_df(n=1000)
    result = wf.run_walkforward_folds(
        df, fit_fn=_fake_fit, simulate_fn=_fake_simulate,
        fold_bounds=[(0.0, 0.01, 0.02), (0.0, 0.5, 1.0)], min_train_rows=300,
    )
    assert result["ok"] is True
    assert result["fold_count"] == 1
    assert result["folds"][0]["fold_bounds"] == [0.0, 0.5, 1.0]


def test_run_walkforward_folds_continues_past_a_fold_that_raises():
    calls = [0]

    def flaky_simulate(test_df, fitted, **kwargs):
        calls[0] += 1
        if calls[0] == 1:
            raise RuntimeError("boom")
        return _fake_simulate(test_df, fitted, **kwargs)

    result = wf.run_walkforward_folds(_combined_df(), fit_fn=_fake_fit, simulate_fn=flaky_simulate)
    assert result["ok"] is True
    assert result["fold_count"] == len(wf.DEFAULT_FOLD_BOUNDS) - 1


def test_run_walkforward_folds_passes_simulate_kwargs_through_to_every_fold():
    received = []

    def capture_simulate(test_df, fitted, **kwargs):
        received.append(kwargs)
        return _fake_simulate(test_df, fitted, **kwargs)

    wf.run_walkforward_folds(
        _combined_df(), fit_fn=_fake_fit, simulate_fn=capture_simulate, simulate_kwargs={"starting_balance": 42.0},
    )
    assert received
    assert all(kwargs == {"starting_balance": 42.0} for kwargs in received)


def test_run_walkforward_folds_records_model_used_from_the_fitted_dict():
    result = wf.run_walkforward_folds(_combined_df(), fit_fn=_fake_fit, simulate_fn=_fake_simulate)
    assert all(f["model_used"] == "fake" for f in result["folds"])


def test_run_walkforward_folds_handles_a_none_fitted_model_gracefully():
    result = wf.run_walkforward_folds(_combined_df(), fit_fn=lambda train_df: None, simulate_fn=_fake_simulate)
    assert result["ok"] is True
    assert all(f["model_used"] is None for f in result["folds"])


def test_summarize_folds_computes_profitable_ratio_and_aggregate_stats():
    folds = [
        {"return_pct": 0.10, "trade_count": 10, "win_rate": 0.6, "total_realized_pnl_usd": 5.0},
        {"return_pct": -0.05, "trade_count": 8, "win_rate": 0.3, "total_realized_pnl_usd": -3.0},
        {"return_pct": 0.20, "trade_count": 12, "win_rate": 0.7, "total_realized_pnl_usd": 8.0},
        {"return_pct": 0.0, "trade_count": 0, "win_rate": 0.0, "total_realized_pnl_usd": 0.0},
    ]
    summary = wf.summarize_folds(folds)
    assert summary["fold_count"] == 4
    assert summary["profitable_fold_count"] == 2
    assert summary["profitable_fold_ratio"] == 0.5
    assert summary["total_trade_count"] == 30
    assert summary["total_realized_pnl_usd"] == 10.0
    assert summary["min_return_pct"] == -0.05
    assert summary["max_return_pct"] == 0.20
    # The zero-trade fold is excluded from mean_win_rate (nothing traded, nothing to average in).
    assert summary["mean_win_rate"] == round((0.6 + 0.3 + 0.7) / 3, 4)


def test_summarize_folds_std_return_is_zero_when_every_fold_matches():
    folds = [{"return_pct": 0.1, "trade_count": 5, "win_rate": 0.5, "total_realized_pnl_usd": 1.0} for _ in range(3)]
    summary = wf.summarize_folds(folds)
    assert summary["std_return_pct"] == 0.0
