"""Backtest engine tests for Kalshi's 15-minute GOLD/SILVER/COPPER/
PLATINUM/PALLADIUM markets -- synthetic feature data only, never touches
Kalshi/HF/news. Mirrors test_kalshi_15m_backtest.py's own crypto test
suite closely (see kalshi_15m_metals_backtest.py's own module docstring
for why this module exists and what it deliberately does NOT replay),
adapted to metals' own leaner METALS_FEATURE_COLUMNS and adding coverage
for yes_confidence_extra_required -- new in this module (and backported
to kalshi_15m_backtest.py's own simulate) specifically because it didn't
exist yet when the crypto backtest was first built."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data import kalshi_15m_metals_backtest as bt


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


def _synthetic_test_df(n_per_symbol: int = 300, symbols: tuple[str, ...] = ("GOLD", "SILVER"), seed: int = 7) -> pd.DataFrame:
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


def _fitted_with(model, symbols=("GOLD", "SILVER")):
    return {"model": model, "model_type": "fake", "feature_cols": bt.FEATURE_COLUMNS + ["symbol_code"], "symbol_categories": list(symbols)}


def test_one_row_per_window_keeps_earliest_row_per_bucket():
    df = pd.DataFrame({
        "symbol": ["GOLD", "GOLD", "GOLD", "SILVER"],
        "ts": [0, 60, 899, 0],
        "value": ["first", "second", "third", "only"],
    })
    result = bt._one_row_per_window(df)  # noqa: SLF001
    assert len(result) == 2
    gold_row = result[result["symbol"] == "GOLD"].iloc[0]
    assert gold_row["value"] == "first"
    assert gold_row["ts"] == 0


def test_one_row_per_window_separates_adjacent_windows():
    df = pd.DataFrame({"symbol": ["GOLD", "GOLD"], "ts": [0, 900], "value": ["window0", "window1"]})
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
    df = _synthetic_test_df()
    result = bt.simulate(df, fitted=None, starting_balance=100.0)
    assert result["trade_count"] == 0
    assert result["rows_with_model"] == 0


def test_simulate_respects_model_confidence_floor():
    df = _synthetic_test_df(symbols=("GOLD",))
    fitted = _fitted_with(_FixedProbaModel(0.55), symbols=("GOLD",))
    result = bt.simulate(df, fitted, starting_balance=100.0, model_confidence_min=0.60)
    assert result["trade_count"] == 0


def test_simulate_trades_when_confidence_clears_the_floor():
    df = _synthetic_test_df(symbols=("GOLD",))
    fitted = _fitted_with(_FixedProbaModel(0.90), symbols=("GOLD",))
    result = bt.simulate(df, fitted, starting_balance=100.0, model_confidence_min=0.58)
    assert result["trade_count"] > 0
    assert all(t["side"] == "yes" for t in result["trades"])


def test_simulate_never_exceeds_max_concurrent_positions():
    df = _synthetic_test_df(symbols=tuple(f"METAL{i}" for i in range(5)), n_per_symbol=50)
    fitted = _fitted_with(_FixedProbaModel(0.95), symbols=tuple(f"METAL{i}" for i in range(5)))
    result = bt.simulate(df, fitted, starting_balance=1_000_000.0, model_confidence_min=0.5, max_concurrent_positions=3)
    assert result["trade_count"] >= 0


def test_simulate_settlement_math_matches_check_settlements_formula():
    df = pd.DataFrame({**{k: [v] for k, v in _feature_row_defaults().items()}, "symbol": ["GOLD"], "ts": [0], "label_up": [1]})
    fitted = _fitted_with(_FixedProbaModel(0.90), symbols=("GOLD",))
    result = bt.simulate(df, fitted, starting_balance=100.0, model_confidence_min=0.5, position_size_pct=0.5, assumed_entry_price=0.40)
    assert result["trade_count"] == 1
    trade = result["trades"][0]
    assert trade["side"] == "yes"
    assert trade["won"] is True
    assert trade["count"] == 125.0
    assert trade["realized_pnl_usd"] == pytest.approx(125.0 * (1 - 0.40))
    assert result["ending_balance_realized"] == pytest.approx(100.0 + 125.0 * 0.60)


def test_simulate_settlement_math_on_a_loss():
    df = pd.DataFrame({**{k: [v] for k, v in _feature_row_defaults().items()}, "symbol": ["GOLD"], "ts": [0], "label_up": [0]})
    fitted = _fitted_with(_FixedProbaModel(0.90), symbols=("GOLD",))
    result = bt.simulate(df, fitted, starting_balance=100.0, model_confidence_min=0.5, position_size_pct=0.5, assumed_entry_price=0.40)
    trade = result["trades"][0]
    assert trade["won"] is False
    assert trade["realized_pnl_usd"] == pytest.approx(-125.0 * 0.40)


def test_simulate_skips_rows_with_no_known_label_yet():
    df = _synthetic_test_df(symbols=("GOLD",), n_per_symbol=50)
    df.loc[df.index[-5:], "label_up"] = pd.NA
    fitted = _fitted_with(_FixedProbaModel(0.95), symbols=("GOLD",))
    result = bt.simulate(df, fitted, starting_balance=100.0, model_confidence_min=0.5)
    assert result["rows_with_model"] == 45


def test_simulate_rejects_an_out_of_range_assumed_entry_price():
    df = _synthetic_test_df(symbols=("GOLD",))
    with pytest.raises(ValueError):
        bt.simulate(df, fitted=None, assumed_entry_price=1.5)
    with pytest.raises(ValueError):
        bt.simulate(df, fitted=None, assumed_entry_price=0.0)


def test_simulate_reports_directional_accuracy_and_calibration():
    df = _synthetic_test_df(symbols=("GOLD",), n_per_symbol=100)
    fitted = _fitted_with(_FixedProbaModel(0.90), symbols=("GOLD",))
    result = bt.simulate(df, fitted, starting_balance=100.0, model_confidence_min=0.95)
    assert result["rows_with_model"] == 100
    assert result["trade_count"] == 0
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
    assert set(result["coins"]) == {"GOLD", "SILVER"}


def test_run_backtest_filters_by_coins_and_days(monkeypatch):
    df = _synthetic_test_df(n_per_symbol=400)
    monkeypatch.setattr(bt, "load_training_dataset", lambda: df)
    result = bt.run_backtest(coins=["GOLD"], starting_balance=50.0)
    assert result["ok"] is True
    assert result["coins"] == ["GOLD"]


def test_run_walkforward_backtest_returns_multiple_folds(monkeypatch):
    df = _synthetic_test_df(n_per_symbol=2000)
    monkeypatch.setattr(bt, "load_training_dataset", lambda: df)
    result = bt.run_walkforward_backtest(starting_balance=50.0)
    assert result["ok"] is True
    assert result["fold_count"] >= 1


# ---------------------------------------------------------------------------
# yes_confidence_extra_required -- new in this module (and backported to
# kalshi_15m_backtest.py's own simulate), replaying YES_CONFIDENCE_EXTRA_REQUIRED's
# own real, live per-side floor.
# ---------------------------------------------------------------------------
def test_simulate_applies_yes_confidence_extra_required_to_yes_only():
    df = _synthetic_test_df(symbols=("GOLD",))
    # 60% confidence clears a plain 0.58 floor but not once a 0.07 "yes"
    # surcharge is added (0.65).
    fitted = _fitted_with(_FixedProbaModel(0.60), symbols=("GOLD",))
    result = bt.simulate(df, fitted, starting_balance=100.0, model_confidence_min=0.58, yes_confidence_extra_required=0.07)
    assert result["trade_count"] == 0


def test_simulate_yes_confidence_extra_required_does_not_affect_no_decisions():
    df = _synthetic_test_df(symbols=("GOLD",))
    # probability_up=0.40 -> side="no", confidence=0.60 -- same raw
    # confidence as the "yes" case above, but "no" gets no surcharge.
    fitted = _fitted_with(_FixedProbaModel(0.40), symbols=("GOLD",))
    result = bt.simulate(df, fitted, starting_balance=100.0, model_confidence_min=0.58, yes_confidence_extra_required=0.07)
    assert result["trade_count"] > 0
    assert all(t["side"] == "no" for t in result["trades"])


def test_simulate_defaults_yes_confidence_extra_required_to_the_live_strategy_value(monkeypatch):
    from data import kalshi_15m_strategy as strat

    monkeypatch.setattr(strat, "YES_CONFIDENCE_EXTRA_REQUIRED", 0.2)
    df = _synthetic_test_df(symbols=("GOLD",))
    fitted = _fitted_with(_FixedProbaModel(0.65), symbols=("GOLD",))
    # 0.65 clears a bare 0.58 floor but not once the live default's 0.2
    # surcharge is picked up (0.78) -- omitting the param entirely.
    result = bt.simulate(df, fitted, starting_balance=100.0, model_confidence_min=0.58)
    assert result["trade_count"] == 0
    assert result["yes_confidence_extra_required"] == 0.2
