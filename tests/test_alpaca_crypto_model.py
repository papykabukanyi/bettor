"""Alpaca crypto direction-classifier training + prediction -- structurally
identical tests to test_alpaca_model.py/test_perps_model.py, but for the
separate crypto pipeline. Synthetic feature data only; never touches
Alpaca, HF, or the network."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from data import alpaca_crypto_model


@pytest.fixture(autouse=True)
def _isolated_model_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(alpaca_crypto_model, "MODEL_PATH", tmp_path / "model.joblib")
    monkeypatch.setattr(alpaca_crypto_model, "MODEL_META_PATH", tmp_path / "model_meta.json")
    monkeypatch.setattr(alpaca_crypto_model, "HF_API_KEY", "")
    alpaca_crypto_model._model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001
    yield
    alpaca_crypto_model._model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001


def _synthetic_training_frame(n: int = 500, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dist = rng.normal(0, 0.01, n)
    label = (dist > 0).astype(int)
    return pd.DataFrame({
        "symbol": ["BTC/USD"] * n,
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
        "hour_sin": np.sin(rng.uniform(0, 2 * np.pi, n)),
        "hour_cos": np.cos(rng.uniform(0, 2 * np.pi, n)),
        "dow_sin": np.sin(rng.uniform(0, 2 * np.pi, n)),
        "dow_cos": np.cos(rng.uniform(0, 2 * np.pi, n)),
        "sentiment_score": np.clip(rng.normal(0.0, 0.3, n), -1.0, 1.0),
        "label_up": label,
    })


def test_train_model_with_no_data_returns_not_ok():
    result = alpaca_crypto_model.train_model(df=pd.DataFrame())
    assert result["ok"] is False
    assert result["reason"] == "no_data"


def test_train_model_with_too_few_rows_returns_not_ok():
    result = alpaca_crypto_model.train_model(df=_synthetic_training_frame(n=20))
    assert result["ok"] is False
    assert result["reason"] == "insufficient_rows"


def test_train_model_succeeds_with_enough_signal_rows():
    df = _synthetic_training_frame(n=500)
    result = alpaca_crypto_model.train_model(df=df)
    assert result["ok"] is True
    assert result["rows"] > 0
    assert result["model_type"] in {"logistic_regression", "random_forest", "gradient_boosting"}
    assert alpaca_crypto_model.MODEL_PATH.exists()
    assert alpaca_crypto_model.MODEL_META_PATH.exists()


def test_train_model_surfaces_sentiment_scores_feature_importance():
    result = alpaca_crypto_model.train_model(df=_synthetic_training_frame(n=500))
    assert result["ok"] is True
    importances = result["feature_importances"]
    assert importances is not None
    assert "sentiment_score" in importances


def test_predict_direction_reports_model_ok_false_without_a_trained_model():
    result = alpaca_crypto_model.predict_direction("BTC/USD")
    assert result["model_ok"] is False


def test_predict_direction_uses_trained_model(monkeypatch):
    df = _synthetic_training_frame(n=500)
    train_result = alpaca_crypto_model.train_model(df=df)
    assert train_result["ok"] is True

    monkeypatch.setattr(alpaca_crypto_model, "latest_feature_row", lambda symbol: {
        "symbol": symbol, "current_price": 65000.0, "short_ma": 64800.0,
        "ret_1m": 0.0, "ret_5m": 0.0, "ret_15m": 0.0, "ret_30m": 0.0, "ret_60m": 0.0,
        "dist_to_ma_15": 0.03, "dist_to_ma_30": 0.015,
        "volatility_5": 0.001, "volatility_15": 0.001, "volatility_30": 0.001,
        "volume_ratio_5": 1.0, "volume_ratio_15": 1.0, "dollar_volume_z": 0.0,
        "rsi_14": 0.5, "macd_hist_pct": 0.0, "bb_pct_b": 0.5, "bb_bandwidth": 0.01, "atr_pct": 0.001, "stoch_k": 0.5,
        "hour_sin": 0.0, "hour_cos": 1.0, "dow_sin": 0.0, "dow_cos": 1.0, "sentiment_score": 0.0,
    })
    prediction = alpaca_crypto_model.predict_direction("BTC/USD")
    assert prediction["model_ok"] is True
    assert prediction["direction"] in {"up", "down"}
    assert 0.0 <= prediction["probability_up"] <= 1.0


# ---------------------------------------------------------------------------
# _TorchMLPClassifier / train_torch_candidate_model -- the custom PyTorch
# candidate, trained in complete isolation from train_model()'s existing
# sklearn candidates (see both docstrings for why). Real torch, not mocked.
# ---------------------------------------------------------------------------
def test_torch_mlp_classifier_fit_predict_proba_shape_and_range():
    rng = np.random.default_rng(0)
    x = rng.normal(size=(200, 5))
    y = (x[:, 0] > 0).astype(int)

    clf = alpaca_crypto_model._TorchMLPClassifier(input_dim=5, epochs=5)  # noqa: SLF001
    clf.fit(x, y)
    proba = clf.predict_proba(x)
    assert proba.shape == (200, 2)
    assert np.all((proba >= 0.0) & (proba <= 1.0))
    assert np.allclose(proba.sum(axis=1), 1.0, atol=1e-5)
    preds = clf.predict(x)
    assert set(np.unique(preds)).issubset({0, 1})


def test_torch_mlp_classifier_survives_a_joblib_pickle_round_trip(tmp_path):
    import joblib

    rng = np.random.default_rng(1)
    x = rng.normal(size=(200, 5))
    y = (x[:, 0] > 0).astype(int)

    clf = alpaca_crypto_model._TorchMLPClassifier(input_dim=5, epochs=5)  # noqa: SLF001
    clf.fit(x, y)
    proba_before = clf.predict_proba(x)

    path = tmp_path / "torch_clf.joblib"
    joblib.dump(clf, path)
    reloaded = joblib.load(path)
    proba_after = reloaded.predict_proba(x)
    assert np.allclose(proba_before, proba_after)


def test_train_torch_candidate_model_with_no_data_returns_not_ok():
    result = alpaca_crypto_model.train_torch_candidate_model(df=pd.DataFrame())
    assert result["ok"] is False
    assert result["reason"] == "no_data"


def test_train_torch_candidate_model_with_too_few_rows_returns_not_ok():
    result = alpaca_crypto_model.train_torch_candidate_model(df=_synthetic_training_frame(n=20))
    assert result["ok"] is False
    assert result["reason"] == "insufficient_rows"


def test_train_torch_candidate_model_promotes_unconditionally_with_no_current_model(monkeypatch):
    monkeypatch.setattr(alpaca_crypto_model, "load_model", lambda: (None, None))
    df = _synthetic_training_frame(n=500)

    result = alpaca_crypto_model.train_torch_candidate_model(df=df)

    assert result["ok"] is True
    assert result["promoted"] is True
    assert result["current_score"] is None
    assert alpaca_crypto_model.MODEL_PATH.exists()
    meta = json.loads(alpaca_crypto_model.MODEL_META_PATH.read_text(encoding="utf-8"))
    assert meta["model_type"] == "torch_mlp"


def test_train_torch_candidate_model_does_not_promote_a_worse_candidate(monkeypatch):
    """A current model that scores perfectly on the holdout must never be
    displaced by a torch candidate that (being a small net on noisy
    synthetic data) can't realistically match it."""

    class _PerfectModel:
        def predict(self, x):
            return (x[:, 5] > 0).astype(int)  # column 5 is dist_to_ma_15, == label_up's own generator

        def predict_proba(self, x):
            preds = self.predict(x)
            return np.column_stack([1.0 - preds, preds]).astype(float)

    monkeypatch.setattr(alpaca_crypto_model, "load_model", lambda: (_PerfectModel(), {"model_type": "perfect_stub"}))
    before = alpaca_crypto_model.MODEL_PATH.exists()
    df = _synthetic_training_frame(n=500)

    result = alpaca_crypto_model.train_torch_candidate_model(df=df)

    assert result["ok"] is True
    assert result["promoted"] is False
    assert result["current_score"] == 1.0
    assert alpaca_crypto_model.MODEL_PATH.exists() == before


def test_train_torch_candidate_model_promoted_model_is_usable_via_predict_direction(monkeypatch):
    real_load_model = alpaca_crypto_model.load_model
    monkeypatch.setattr(alpaca_crypto_model, "load_model", lambda: (None, None))
    df = _synthetic_training_frame(n=500)
    train_result = alpaca_crypto_model.train_torch_candidate_model(df=df)
    assert train_result["ok"] is True and train_result["promoted"] is True

    monkeypatch.setattr(alpaca_crypto_model, "load_model", real_load_model)
    alpaca_crypto_model._model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001
    monkeypatch.setattr(alpaca_crypto_model, "latest_feature_row", lambda symbol: {
        "symbol": symbol, "current_price": 65000.0, "short_ma": 64800.0,
        "ret_1m": 0.0, "ret_5m": 0.0, "ret_15m": 0.0, "ret_30m": 0.0, "ret_60m": 0.0,
        "dist_to_ma_15": 0.03, "dist_to_ma_30": 0.015,
        "volatility_5": 0.001, "volatility_15": 0.001, "volatility_30": 0.001,
        "volume_ratio_5": 1.0, "volume_ratio_15": 1.0, "dollar_volume_z": 0.0,
        "rsi_14": 0.5, "macd_hist_pct": 0.0, "bb_pct_b": 0.5, "bb_bandwidth": 0.01, "atr_pct": 0.001, "stoch_k": 0.5,
        "hour_sin": 0.0, "hour_cos": 1.0, "dow_sin": 0.0, "dow_cos": 1.0, "sentiment_score": 0.0,
    })

    prediction = alpaca_crypto_model.predict_direction("BTC/USD")
    assert prediction["model_ok"] is True
    assert prediction["model_type"] == "torch_mlp"
    assert prediction["direction"] in {"up", "down"}
