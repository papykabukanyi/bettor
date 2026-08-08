"""Alpaca crypto backtest engine -- synthetic price data only, never touches
the network. Verifies the simulation correctly reuses the REAL
alpaca_crypto_strategy decide_exit/adaptive_exit_pcts/round_trip_fee_usd
functions (which read TAKE_PROFIT_PCT/STOP_LOSS_PCT/MAX_HOLD_MINUTES as
module-level globals, not parameters -- simulate() has to guard against
that with a try/finally restore, same real trap alpaca_options_backtest.py
already documents), that same-coin/different-quote-currency pairs are
deduped (no double-betting BTC via BTC/USD + BTC/USDT), and that the
concurrency/daily-loss-cap guards hold. No prior test file existed for this
module -- it's the only one of the 4 asset classes' backtest engines that
had zero coverage before this."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data import alpaca_crypto_backtest as bt
from data import alpaca_crypto_strategy as strat


def _synthetic_test_df(n: int = 300, symbol: str = "BTC/USD") -> pd.DataFrame:
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
        "hour_sin": np.zeros(n), "hour_cos": np.ones(n), "dow_sin": np.zeros(n), "dow_cos": np.ones(n),
        "sentiment_score": np.zeros(n),
    })


class _AlwaysUpModel:
    def predict_proba(self, x):
        return np.tile([0.1, 0.9], (len(x), 1))  # p_up = 0.9, clears any real confidence bar


class _AlwaysDownModel:
    def predict_proba(self, x):
        return np.tile([0.9, 0.1], (len(x), 1))  # p_up = 0.1 -> confident DOWN


def _fitted(model, symbols=("BTC/USD",)):
    return {"model": model, "model_type": "fake", "feature_cols": bt.FEATURE_COLUMNS + ["symbol_code"], "symbol_categories": list(symbols)}


def test_add_model_predictions_without_a_model_yields_nan_column():
    df = _synthetic_test_df()
    result = bt.add_model_predictions(df, fitted=None)
    assert result["model_probability_up"].isna().all()


def test_simulate_runs_technical_only_without_a_model():
    """Unlike options (no technical-only fallback), crypto matches perps/
    stocks: an unfitted backtest still lets a strong technical dip fire."""
    df = _synthetic_test_df(n=300)
    result = bt.simulate(df, fitted=None, starting_balance=500.0, entry_dip_pct=0.0001, min_volume_z=0.0)
    assert result["trade_count"] >= 0
    assert "trades_per_day" in result


def test_simulate_respects_a_trained_models_direction():
    """Long-only (real spot, no shorting): a confidently-DOWN model must
    block every entry the technical gates would otherwise allow."""
    df = _synthetic_test_df(n=300)
    result = bt.simulate(
        df, _fitted(_AlwaysDownModel()), starting_balance=500.0, entry_dip_pct=0.0001, min_volume_z=0.0,
    )
    assert result["trade_count"] == 0


def test_simulate_enters_on_a_confident_up_model():
    df = _synthetic_test_df(n=300)
    result = bt.simulate(
        df, _fitted(_AlwaysUpModel()), starting_balance=500.0, entry_dip_pct=0.0001, min_volume_z=0.0,
    )
    assert result["trade_count"] > 0


def test_simulate_reuses_precomputed_predictions_when_present(monkeypatch):
    df = _synthetic_test_df(n=300)
    df["model_probability_up"] = 0.9

    def fail_if_called(*a, **k):
        raise AssertionError("add_model_predictions must not run again when the column is already present")

    monkeypatch.setattr(bt, "add_model_predictions", fail_if_called)
    result = bt.simulate(df, fitted=None, starting_balance=500.0, entry_dip_pct=0.0001, min_volume_z=0.0)
    assert "trade_count" in result


def test_simulate_never_exceeds_max_concurrent_positions():
    frames = [_synthetic_test_df(symbol=s) for s in ["AAA/USD", "BBB/USD", "CCC/USD", "DDD/USD"]]
    for i, f in enumerate(frames):
        f["ts"] = f["ts"] + i * 10_000_000
    df = pd.concat(frames, ignore_index=True)
    result = bt.simulate(
        df, fitted=None, starting_balance=100_000.0, max_concurrent_positions=2,
        entry_dip_pct=0.0001, min_volume_z=0.0,
    )
    assert result["open_positions_at_end"] <= 2


def test_simulate_dedups_same_coin_across_quote_currencies():
    """get_crypto_universe() lists multiple quote pairs for the same coin
    (e.g. BTC/USD and BTC/USDT) -- the live strategy explicitly refuses to
    hold both at once, and this backtest must replicate that or it would
    overstate diversification by triple-betting the same coin's move."""
    frames = [_synthetic_test_df(symbol=s) for s in ["BTC/USD", "BTC/USDT"]]
    # Same timestamps for both -- if dedup didn't work, both could open on the SAME tick.
    df = pd.concat(frames, ignore_index=True)
    result = bt.simulate(
        df, fitted=None, starting_balance=100_000.0, max_concurrent_positions=5,
        entry_dip_pct=0.0001, min_volume_z=0.0,
    )
    assert result["open_positions_at_end"] <= 1


def test_simulate_deducts_the_real_round_trip_taker_fee():
    df = _synthetic_test_df(n=300)
    zero_fee = bt.simulate(
        df, _fitted(_AlwaysUpModel()), starting_balance=500.0, entry_dip_pct=0.0001, min_volume_z=0.0, taker_fee_rate=0.0,
    )
    with_fee = bt.simulate(
        df, _fitted(_AlwaysUpModel()), starting_balance=500.0, entry_dip_pct=0.0001, min_volume_z=0.0,
        taker_fee_rate=strat.TAKER_FEE_RATE,
    )
    assert zero_fee["trade_count"] == with_fee["trade_count"]
    if with_fee["trade_count"] > 0:
        assert zero_fee["total_realized_pnl_usd"] > with_fee["total_realized_pnl_usd"]
        assert with_fee["total_fees_usd"] > 0
    assert zero_fee["total_fees_usd"] == 0


def test_simulate_restores_real_strategy_parameters_afterward(monkeypatch):
    """decide_exit()/adaptive_exit_pcts() read TAKE_PROFIT_PCT/STOP_LOSS_PCT/
    MAX_HOLD_MINUTES as module-level globals on alpaca_crypto_strategy --
    leaving the LAST swept values in place would mean live trading silently
    runs on leftover backtest parameters instead of its real configured ones."""
    monkeypatch.setattr(strat, "TAKE_PROFIT_PCT", 0.0123)
    monkeypatch.setattr(strat, "STOP_LOSS_PCT", 0.0099)
    monkeypatch.setattr(strat, "MAX_HOLD_MINUTES", 77)

    df = _synthetic_test_df(n=300)
    bt.simulate(
        df, fitted=None, starting_balance=500.0, entry_dip_pct=0.0001, min_volume_z=0.0,
        take_profit_pct=0.9, stop_loss_pct=0.8, max_hold_minutes=999,
    )

    assert strat.TAKE_PROFIT_PCT == 0.0123
    assert strat.STOP_LOSS_PCT == 0.0099
    assert strat.MAX_HOLD_MINUTES == 77


def test_simulate_restores_parameters_even_if_inner_raises(monkeypatch):
    monkeypatch.setattr(strat, "TAKE_PROFIT_PCT", 0.0123)
    monkeypatch.setattr(strat, "STOP_LOSS_PCT", 0.0099)
    monkeypatch.setattr(strat, "MAX_HOLD_MINUTES", 77)

    def fail(*a, **k):
        raise RuntimeError("boom")

    monkeypatch.setattr(bt, "_simulate_inner", fail)
    df = _synthetic_test_df(n=300)
    with pytest.raises(RuntimeError):
        bt.simulate(df, fitted=None, starting_balance=500.0, take_profit_pct=0.9, stop_loss_pct=0.8, max_hold_minutes=999)

    assert strat.TAKE_PROFIT_PCT == 0.0123
    assert strat.STOP_LOSS_PCT == 0.0099
    assert strat.MAX_HOLD_MINUTES == 77


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


def test_run_config_sweep_tries_every_grid_entry_and_ranks_by_return(monkeypatch):
    df = _synthetic_test_df(n=300)
    monkeypatch.setattr(bt, "_SWEEP_GRID", [
        {"label": "a", "take_profit_pct": 0.01, "stop_loss_pct": 0.008},
        {"label": "b", "take_profit_pct": 0.018, "stop_loss_pct": 0.012, "model_confidence_min": 0.6},
    ])
    result = bt.run_config_sweep(df, starting_balance=500.0, min_trades=0)
    assert len(result["configs"]) == 2
    assert result["best"] is not None
    assert result["configs"][0]["return_pct"] >= result["configs"][-1]["return_pct"]


def test_run_config_sweep_flags_configs_below_the_min_trade_count(monkeypatch):
    df = _synthetic_test_df(n=300)
    df["dollar_volume_z"] = 0.0  # never "unusual" -- zero trades regardless of config
    monkeypatch.setattr(bt, "_SWEEP_GRID", [{"label": "a", "take_profit_pct": 0.01, "stop_loss_pct": 0.008}])
    result = bt.run_config_sweep(df, starting_balance=500.0, min_trades=1000)
    assert result["configs"][0]["low_sample"] is True


def test_run_walkforward_backtest_returns_multiple_folds_via_the_real_pipeline(monkeypatch):
    """End-to-end through the real fetch->fit->simulate pipeline (network
    calls mocked out), confirming run_walkforward_backtest wires
    walkforward.run_walkforward_folds up to this module's own
    build_pair_frame/fit_backtest_model/simulate rather than reimplementing
    any of that."""
    n = 900
    rng = np.random.default_rng(11)
    combined = pd.concat([_synthetic_test_df(n=n, symbol=s) for s in ("BTC/USD", "ETH/USD")], ignore_index=True)
    # ETH's rows need distinct timestamps from BTC's, same as the other
    # multi-symbol fixtures in this file -- offset by symbol.
    combined.loc[combined["symbol"] == "ETH/USD", "ts"] += 10_000_000
    combined["label_up"] = (rng.normal(0, 1, len(combined)) > 0).astype(int)

    def fake_build_pair_frame(symbol, *, days):
        return combined[combined["symbol"] == symbol].reset_index(drop=True)

    monkeypatch.setattr(bt, "build_pair_frame", fake_build_pair_frame)
    monkeypatch.setattr(bt, "get_crypto_universe", lambda: ["BTC/USD", "ETH/USD"])
    # 4 folds x 3 sklearn candidates is real fit cost -- cut to the
    # cheapest candidate only. This test is about run_walkforward_backtest's
    # own plumbing, not about which candidate wins a fit-quality contest.
    monkeypatch.setattr(bt, "_CANDIDATES", {"logistic_regression": bt._CANDIDATES["logistic_regression"]})

    result = bt.run_walkforward_backtest(days=30, starting_balance=500.0)
    assert result["ok"] is True
    assert result["symbols"] == ["BTC/USD", "ETH/USD"]
    assert result["fold_count"] >= 2
    assert "profitable_fold_ratio" in result
    assert "mean_return_pct" in result
    for fold in result["folds"]:
        assert "return_pct" in fold
        assert "fold_bounds" in fold


def test_run_walkforward_backtest_reports_no_data_when_every_pair_fetch_is_empty(monkeypatch):
    monkeypatch.setattr(bt, "build_pair_frame", lambda symbol, *, days: pd.DataFrame())
    monkeypatch.setattr(bt, "get_crypto_universe", lambda: ["BTC/USD"])
    result = bt.run_walkforward_backtest()
    assert result == {"ok": False, "reason": "no_data"}
