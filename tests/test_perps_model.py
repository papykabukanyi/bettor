"""Direction-classifier training + prediction. Uses synthetic feature data
only -- never touches Kalshi, Hugging Face, or news feeds. Verifies the
"not enough data yet" cold-start path (so the strategy's technical-only
fallback is exercised correctly) and that a trained model can actually
produce a usable prediction from a feature row."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data import perps_model


@pytest.fixture(autouse=True)
def _isolated_model_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(perps_model, "MODEL_PATH", tmp_path / "model.joblib")
    monkeypatch.setattr(perps_model, "MODEL_META_PATH", tmp_path / "model_meta.json")
    monkeypatch.setattr(perps_model, "HF_API_KEY", "")
    perps_model._model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001
    yield
    perps_model._model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001


def _synthetic_training_frame(n: int = 500, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dist = rng.normal(0, 0.01, n)
    label = (dist > 0).astype(int)
    return pd.DataFrame({
        "ticker": ["KXBTCPERP"] * n,
        "ts": np.arange(n),
        "ret_1m": rng.normal(0, 0.001, n),
        "ret_3m": rng.normal(0, 0.0015, n),
        "ret_5m": rng.normal(0, 0.002, n),
        "ret_10m": rng.normal(0, 0.0025, n),
        "ret_15m": rng.normal(0, 0.003, n),
        "ret_30m": rng.normal(0, 0.004, n),
        "trend_1h": rng.normal(0, 0.006, n),
        "trend_2h": rng.normal(0, 0.008, n),
        "trend_3h": rng.normal(0, 0.009, n),
        "trend_4h": rng.normal(0, 0.01, n),
        "dist_to_ma_15": dist,
        "dist_to_ma_30": dist * 0.5,
        "volatility_5": np.abs(rng.normal(0.0008, 0.0003, n)),
        "volatility_15": np.abs(rng.normal(0.001, 0.0005, n)),
        "volatility_30": np.abs(rng.normal(0.0012, 0.0006, n)),
        "trend_pct": rng.normal(0, 0.01, n),
        "sentiment_score": rng.normal(0, 0.2, n),
        "label_up": label,
    })


def test_train_model_with_no_data_returns_not_ok():
    result = perps_model.train_model(df=pd.DataFrame())
    assert result["ok"] is False
    assert result["reason"] == "no_data"


def test_train_model_with_too_few_rows_returns_not_ok():
    small_df = _synthetic_training_frame(n=20)
    result = perps_model.train_model(df=small_df)
    assert result["ok"] is False
    assert result["reason"] == "insufficient_rows"


def test_train_model_succeeds_with_enough_signal_rows():
    df = _synthetic_training_frame(n=500)
    result = perps_model.train_model(df=df)
    assert result["ok"] is True
    assert result["rows"] > 0
    assert result["model_type"] in {"logistic_regression", "random_forest", "gradient_boosting"}
    assert perps_model.MODEL_PATH.exists()
    assert perps_model.MODEL_META_PATH.exists()


def test_predict_direction_reports_model_ok_false_without_a_trained_model():
    result = perps_model.predict_direction("KXBTCPERP")
    assert result["model_ok"] is False


def test_predict_direction_uses_trained_model(monkeypatch):
    df = _synthetic_training_frame(n=500)
    train_result = perps_model.train_model(df=df)
    assert train_result["ok"] is True

    monkeypatch.setattr(perps_model, "latest_feature_row", lambda ticker: {
        "ticker": ticker, "current_price": 100.0, "short_ma": 99.0, "trend_pct": 0.0,
        "ret_1m": 0.0, "ret_3m": 0.0, "ret_5m": 0.0, "ret_10m": 0.0, "ret_15m": 0.0, "ret_30m": 0.0,
        "trend_1h": 0.0, "trend_2h": 0.0, "trend_3h": 0.0, "trend_4h": 0.0,
        "dist_to_ma_15": 0.03, "dist_to_ma_30": 0.015,
        "volatility_5": 0.001, "volatility_15": 0.001, "volatility_30": 0.001, "sentiment_score": 0.0,
    })
    prediction = perps_model.predict_direction("KXBTCPERP")
    assert prediction["model_ok"] is True
    assert prediction["direction"] in {"up", "down"}
    assert 0.0 <= prediction["probability_up"] <= 1.0
