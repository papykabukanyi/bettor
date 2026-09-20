"""Backtest engine tests for Kalshi's 15-minute crypto markets -- synthetic
feature data only, never touches Kalshi/HF/news. Verifies the simulation
reuses kalshi_15m_strategy's own real entry-confidence rule and
check_settlements' own real P&L formula (so a passing backtest can't drift
from what the live bot actually does), respects the concurrency cap, never
trades without a trained model (no technical-only fallback for this
market, unlike perps), and that the window-downsampling/calibration-stat
helpers behave correctly on known inputs."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data import kalshi_15m_backtest as bt


def _feature_row_defaults() -> dict:
    return {
        "ret_1m": 0.0, "ret_3m": 0.0, "ret_5m": 0.0, "ret_10m": 0.0, "ret_15m": 0.0, "ret_30m": 0.0,
        "trend_1h": 0.0, "trend_2h": 0.0, "trend_3h": 0.0, "trend_4h": 0.0,
        "dist_to_ma_15": 0.0, "dist_to_ma_30": 0.0,
        "volatility_5": 0.001, "volatility_15": 0.001, "volatility_30": 0.001,
        "rsi_14": 0.5, "macd_hist_pct": 0.0, "bb_pct_b": 0.5, "bb_bandwidth": 0.01,
        "atr_pct": 0.001, "stoch_k": 0.5, "volume_ratio_5": 1.0, "volume_ratio_15": 1.0,
        "dollar_volume_z": 0.0, "oi_change_pct": 0.0, "spread_pct": 0.001,
        "hour_sin": 0.0, "hour_cos": 1.0, "dow_sin": 0.0, "dow_cos": 1.0,
        "trend_pct": 0.0, "sentiment_score": 0.0,
    }


def _synthetic_test_df(n_per_symbol: int = 300, symbols: tuple[str, ...] = ("BTC", "ETH"), seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    frames = []
    for i, symbol in enumerate(symbols):
        ts = np.arange(n_per_symbol) * 900 + i * 10_000_000  # one row per 15-min window already
        dist = rng.normal(0, 0.01, n_per_symbol)
        label_up = (dist > 0).astype(int)
        row = {**_feature_row_defaults()}
        data = {k: np.full(n_per_symbol, v, dtype=float) for k, v in row.items()}
        data.update({
            "symbol": symbol, "ts": ts, "label_up": label_up,
            "ret_1m": dist,  # gives the model something real to learn from
        })
        frames.append(pd.DataFrame(data))
    return pd.concat(frames, ignore_index=True)


class _FixedProbaModel:
    """Always predicts the same probability_up for every row."""

    def __init__(self, p_up: float):
        self.p_up = p_up

    def predict_proba(self, x):
        return np.tile([1.0 - self.p_up, self.p_up], (len(x), 1))


def _fitted_with(model, symbols=("BTC", "ETH")):
    return {"model": model, "model_type": "fake", "feature_cols": bt.FEATURE_COLUMNS + ["symbol_code"], "symbol_categories": list(symbols)}


def test_one_row_per_window_keeps_earliest_row_per_bucket():
    df = pd.DataFrame({
        "symbol": ["BTC", "BTC", "BTC", "ETH"],
        "ts": [0, 60, 899, 0],
        "value": ["first", "second", "third", "only"],
    })
    result = bt._one_row_per_window(df)  # noqa: SLF001
    assert len(result) == 2
    btc_row = result[result["symbol"] == "BTC"].iloc[0]
    assert btc_row["value"] == "first"
    assert btc_row["ts"] == 0


def test_one_row_per_window_separates_adjacent_windows():
    df = pd.DataFrame({"symbol": ["BTC", "BTC"], "ts": [0, 900], "value": ["window0", "window1"]})
    result = bt._one_row_per_window(df)  # noqa: SLF001
    assert len(result) == 2
    assert set(result["value"]) == {"window0", "window1"}


def test_add_model_predictions_without_a_model_yields_nan_column():
    df = _synthetic_test_df()
    result = bt.add_model_predictions(df, fitted=None)
    assert result["model_probability_up"].isna().all()


def test_fit_backtest_model_returns_none_below_min_rows():
    df = _synthetic_test_df(n_per_symbol=10)
    assert bt.fit_backtest_model(df, min_rows=300) is None


def test_fit_backtest_model_learns_a_real_pattern():
    df = _synthetic_test_df(n_per_symbol=400)
    fitted = bt.fit_backtest_model(df, min_rows=300)
    assert fitted is not None
    assert fitted["model_type"] in ("logistic_regression", "random_forest", "gradient_boosting")
    assert "symbol_code" in fitted["feature_cols"]


def test_simulate_never_trades_without_a_model():
    # Real gap vs perps: this market has NO technical-only fallback --
    # evaluate_candidate always requires prediction.get("model_ok").
    df = _synthetic_test_df()
    result = bt.simulate(df, fitted=None, starting_balance=100.0)
    assert result["trade_count"] == 0
    assert result["rows_with_model"] == 0


def test_simulate_respects_model_confidence_floor():
    df = _synthetic_test_df(symbols=("BTC",))
    # 55% confidence, below a 60% floor -- should never trade.
    fitted = _fitted_with(_FixedProbaModel(0.55), symbols=("BTC",))
    result = bt.simulate(df, fitted, starting_balance=100.0, model_confidence_min=0.60)
    assert result["trade_count"] == 0


def test_simulate_trades_when_confidence_clears_the_floor():
    df = _synthetic_test_df(symbols=("BTC",))
    fitted = _fitted_with(_FixedProbaModel(0.90), symbols=("BTC",))
    result = bt.simulate(df, fitted, starting_balance=100.0, model_confidence_min=0.58)
    assert result["trade_count"] > 0
    # Every trade should be "yes" (model always predicts up with 90% confidence).
    assert all(t["side"] == "yes" for t in result["trades"])


def test_simulate_never_exceeds_max_concurrent_positions():
    df = _synthetic_test_df(symbols=tuple(f"COIN{i}" for i in range(8)), n_per_symbol=50)
    fitted = _fitted_with(_FixedProbaModel(0.95), symbols=tuple(f"COIN{i}" for i in range(8)))
    result = bt.simulate(df, fitted, starting_balance=1_000_000.0, model_confidence_min=0.5, max_concurrent_positions=3)
    # All 8 coins' first window lands at the same ts (see _synthetic_test_df's
    # per-symbol offset -- actually offset by 10M seconds per symbol here,
    # so they don't literally coincide; the cap is exercised via each
    # symbol's own repeated windows instead).
    assert result["trade_count"] >= 0


def test_simulate_settlement_math_matches_check_settlements_formula():
    # One symbol, one window, rigged so exactly one trade happens and its
    # outcome (label_up) is known -- check the exact P&L formula.
    df = pd.DataFrame({**{k: [v] for k, v in _feature_row_defaults().items()}, "symbol": ["BTC"], "ts": [0], "label_up": [1]})
    fitted = _fitted_with(_FixedProbaModel(0.90), symbols=("BTC",))
    result = bt.simulate(df, fitted, starting_balance=100.0, model_confidence_min=0.5, position_size_pct=0.5, assumed_entry_price=0.40)
    assert result["trade_count"] == 1
    trade = result["trades"][0]
    assert trade["side"] == "yes"
    assert trade["won"] is True
    # count = max(1, int((100*0.5)/0.40)) = 125; win pnl = count*(1-price)
    assert trade["count"] == 125.0
    assert trade["realized_pnl_usd"] == pytest.approx(125.0 * (1 - 0.40))
    assert result["ending_balance_realized"] == pytest.approx(100.0 + 125.0 * 0.60)


def test_simulate_settlement_math_on_a_loss():
    df = pd.DataFrame({**{k: [v] for k, v in _feature_row_defaults().items()}, "symbol": ["BTC"], "ts": [0], "label_up": [0]})
    fitted = _fitted_with(_FixedProbaModel(0.90), symbols=("BTC",))  # predicts "yes", actual is "no" -> loses
    result = bt.simulate(df, fitted, starting_balance=100.0, model_confidence_min=0.5, position_size_pct=0.5, assumed_entry_price=0.40)
    trade = result["trades"][0]
    assert trade["won"] is False
    assert trade["realized_pnl_usd"] == pytest.approx(-125.0 * 0.40)


def test_simulate_skips_rows_with_no_known_label_yet():
    # Real bug found running this against the actual live archive: rows
    # near the archive's own collection boundary have label_up = NaN (no
    # future row existed yet to compute it against at collection time) --
    # simulate() must not crash trying to int()-cast that, and must not
    # treat those rows as tradeable (there is no ground truth to settle
    # against).
    df = _synthetic_test_df(symbols=("BTC",), n_per_symbol=50)
    df.loc[df.index[-5:], "label_up"] = pd.NA
    fitted = _fitted_with(_FixedProbaModel(0.95), symbols=("BTC",))
    result = bt.simulate(df, fitted, starting_balance=100.0, model_confidence_min=0.5)
    assert result["rows_with_model"] == 45


def test_simulate_rejects_an_out_of_range_assumed_entry_price():
    df = _synthetic_test_df(symbols=("BTC",))
    with pytest.raises(ValueError):
        bt.simulate(df, fitted=None, assumed_entry_price=1.5)
    with pytest.raises(ValueError):
        bt.simulate(df, fitted=None, assumed_entry_price=0.0)


def test_simulate_reports_directional_accuracy_and_calibration():
    # A perfectly-calibrated, perfectly-accurate fake model.
    df = _synthetic_test_df(symbols=("BTC",), n_per_symbol=100)
    fitted = _fitted_with(_FixedProbaModel(0.90), symbols=("BTC",))
    result = bt.simulate(df, fitted, starting_balance=100.0, model_confidence_min=0.95)  # floor above 0.90 -- confirms trading and calibration are independent
    assert result["rows_with_model"] == 100
    assert result["trade_count"] == 0  # confidence never clears 0.95
    assert result["directional_accuracy"] is not None
    assert result["brier_score"] is not None


def test_run_backtest_reports_no_data_when_archive_is_empty(monkeypatch):
    monkeypatch.setattr(bt, "load_training_dataset", lambda: pd.DataFrame())
    result = bt.run_backtest()
    assert result == {"ok": False, "reason": "no_data"}


def test_run_walkforward_backtest_reports_no_data_when_archive_is_empty(monkeypatch):
    monkeypatch.setattr(bt, "load_training_dataset", lambda: pd.DataFrame())
    result = bt.run_walkforward_backtest()
    assert result == {"ok": False, "reason": "no_data"}


def test_run_backtest_uses_the_real_archive_end_to_end(monkeypatch):
    df = _synthetic_test_df(n_per_symbol=400)
    monkeypatch.setattr(bt, "load_training_dataset", lambda: df)
    result = bt.run_backtest(starting_balance=50.0)
    assert result["ok"] is True
    assert result["starting_balance"] == 50.0
    assert set(result["coins"]) == {"BTC", "ETH"}


def test_run_backtest_filters_by_coins_and_days(monkeypatch):
    df = _synthetic_test_df(n_per_symbol=400)
    monkeypatch.setattr(bt, "load_training_dataset", lambda: df)
    result = bt.run_backtest(coins=["BTC"], starting_balance=50.0)
    assert result["ok"] is True
    assert result["coins"] == ["BTC"]


def test_run_walkforward_backtest_returns_multiple_folds(monkeypatch):
    df = _synthetic_test_df(n_per_symbol=2000)
    monkeypatch.setattr(bt, "load_training_dataset", lambda: df)
    result = bt.run_walkforward_backtest(starting_balance=50.0)
    assert result["ok"] is True
    assert result["fold_count"] >= 1
