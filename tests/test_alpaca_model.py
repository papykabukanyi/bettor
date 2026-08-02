"""Alpaca stock direction-classifier training + prediction -- structurally
identical tests to test_perps_model.py, but for the separate Alpaca
pipeline. Synthetic feature data only; never touches Alpaca, HF, or the
network."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data import alpaca_model


@pytest.fixture(autouse=True)
def _isolated_model_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(alpaca_model, "MODEL_PATH", tmp_path / "model.joblib")
    monkeypatch.setattr(alpaca_model, "MODEL_META_PATH", tmp_path / "model_meta.json")
    monkeypatch.setattr(alpaca_model, "HF_API_KEY", "")
    alpaca_model._model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001
    yield
    alpaca_model._model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001


def _synthetic_training_frame(n: int = 500, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dist = rng.normal(0, 0.01, n)
    label = (dist > 0).astype(int)
    return pd.DataFrame({
        "symbol": ["AAPL"] * n,
        "ts": np.arange(n),
        "ret_1m": rng.normal(0, 0.001, n),
        "ret_5m": rng.normal(0, 0.002, n),
        "ret_15m": rng.normal(0, 0.003, n),
        "ret_30m": rng.normal(0, 0.004, n),
        "ret_60m": rng.normal(0, 0.006, n),
        "dist_to_ma_15": dist,
        "dist_to_ma_30": dist * 0.5,
        "volatility_5": np.abs(rng.normal(0.0008, 0.0003, n)),
        "volatility_15": np.abs(rng.normal(0.001, 0.0005, n)),
        "volatility_30": np.abs(rng.normal(0.0012, 0.0006, n)),
        "volume_ratio_5": np.abs(rng.normal(1.0, 0.3, n)),
        "volume_ratio_15": np.abs(rng.normal(1.0, 0.2, n)),
        "dollar_volume_z": rng.normal(0, 1.0, n),
        "rsi_14": np.clip(rng.normal(0.5, 0.15, n), 0.0, 1.0),
        "macd_hist_pct": rng.normal(0, 0.001, n),
        "bb_pct_b": np.clip(rng.normal(0.5, 0.2, n), 0.0, 1.0),
        "bb_bandwidth": np.abs(rng.normal(0.01, 0.003, n)),
        "atr_pct": np.abs(rng.normal(0.001, 0.0003, n)),
        "stoch_k": np.clip(rng.normal(0.5, 0.2, n), 0.0, 1.0),
        "time_of_day_pct": np.clip(rng.normal(0.5, 0.3, n), -0.2, 1.2),
        "sentiment_score": np.clip(rng.normal(0.0, 0.3, n), -1.0, 1.0),
        "label_up": label,
    })


def test_train_model_with_no_data_returns_not_ok():
    result = alpaca_model.train_model(df=pd.DataFrame())
    assert result["ok"] is False
    assert result["reason"] == "no_data"


def test_train_model_with_too_few_rows_returns_not_ok():
    small_df = _synthetic_training_frame(n=20)
    result = alpaca_model.train_model(df=small_df)
    assert result["ok"] is False
    assert result["reason"] == "insufficient_rows"


def test_train_model_succeeds_with_enough_signal_rows():
    df = _synthetic_training_frame(n=500)
    result = alpaca_model.train_model(df=df)
    assert result["ok"] is True
    assert result["rows"] > 0
    assert result["model_type"] in {"logistic_regression", "random_forest", "gradient_boosting"}
    assert alpaca_model.MODEL_PATH.exists()
    assert alpaca_model.MODEL_META_PATH.exists()


def test_train_model_surfaces_sentiment_scores_feature_importance():
    """sentiment_score must be a real, verifiable input to the trained
    model -- not just fed in and hoped to help -- so its learned weight
    shows up in meta.feature_importances the same as every other feature."""
    df = _synthetic_training_frame(n=500)
    result = alpaca_model.train_model(df=df)
    assert result["ok"] is True
    importances = result["feature_importances"]
    assert importances is not None
    assert "sentiment_score" in importances


def test_feature_importance_map_uses_coef_for_logistic_regression():
    from sklearn.linear_model import LogisticRegression
    import numpy as np

    model = LogisticRegression().fit(np.array([[0.0, 1.0], [1.0, 0.0], [0.0, 0.0], [1.0, 1.0]]), [0, 1, 0, 1])
    importances = alpaca_model._feature_importance_map(model, ["a", "b"])  # noqa: SLF001
    assert set(importances.keys()) == {"a", "b"}


def test_predict_direction_reports_model_ok_false_without_a_trained_model():
    result = alpaca_model.predict_direction("AAPL")
    assert result["model_ok"] is False


def test_predict_direction_uses_trained_model(monkeypatch):
    df = _synthetic_training_frame(n=500)
    train_result = alpaca_model.train_model(df=df)
    assert train_result["ok"] is True

    monkeypatch.setattr(alpaca_model, "latest_feature_row", lambda symbol: {
        "symbol": symbol, "current_price": 100.0, "short_ma": 99.0,
        "ret_1m": 0.0, "ret_5m": 0.0, "ret_15m": 0.0, "ret_30m": 0.0, "ret_60m": 0.0,
        "dist_to_ma_15": 0.03, "dist_to_ma_30": 0.015,
        "volatility_5": 0.001, "volatility_15": 0.001, "volatility_30": 0.001,
        "volume_ratio_5": 1.0, "volume_ratio_15": 1.0, "dollar_volume_z": 0.0,
        "rsi_14": 0.5, "macd_hist_pct": 0.0, "bb_pct_b": 0.5, "bb_bandwidth": 0.01, "atr_pct": 0.001, "stoch_k": 0.5,
        "time_of_day_pct": 0.5, "sentiment_score": 0.0,
    })
    prediction = alpaca_model.predict_direction("AAPL")
    assert prediction["model_ok"] is True
    assert prediction["direction"] in {"up", "down"}
    assert 0.0 <= prediction["probability_up"] <= 1.0
