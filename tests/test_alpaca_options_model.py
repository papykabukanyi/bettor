"""Alpaca options direction-classifier training + prediction -- structurally
identical tests to test_alpaca_model.py (the options model reuses the
equities feature recipe exactly -- predicting whether a stock goes up or
down is the same problem whether the result buys shares or an option on
it). Synthetic feature data only; never touches Alpaca, HF, or the
network."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data import alpaca_options_model


@pytest.fixture(autouse=True)
def _isolated_model_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(alpaca_options_model, "MODEL_PATH", tmp_path / "model.joblib")
    monkeypatch.setattr(alpaca_options_model, "MODEL_META_PATH", tmp_path / "model_meta.json")
    monkeypatch.setattr(alpaca_options_model, "HF_API_KEY", "")
    alpaca_options_model._model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001
    yield
    alpaca_options_model._model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001


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
    result = alpaca_options_model.train_model(df=pd.DataFrame())
    assert result["ok"] is False
    assert result["reason"] == "no_data"


def test_train_model_with_too_few_rows_returns_not_ok():
    result = alpaca_options_model.train_model(df=_synthetic_training_frame(n=20))
    assert result["ok"] is False
    assert result["reason"] == "insufficient_rows"


def test_train_model_succeeds_with_enough_signal_rows():
    df = _synthetic_training_frame(n=500)
    result = alpaca_options_model.train_model(df=df)
    assert result["ok"] is True
    assert result["rows"] > 0
    assert result["model_type"] in {"logistic_regression", "random_forest", "gradient_boosting"}
    assert alpaca_options_model.MODEL_PATH.exists()
    assert alpaca_options_model.MODEL_META_PATH.exists()


def test_train_model_stays_uncalibrated_below_the_holdout_floor():
    """n=500 is exactly the pre-existing regression fixture -- its last
    walk-forward fold's test slice (~100 rows) falls below
    ALPACA_OPTIONS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS (200), so this MUST
    fall back to the old, uncalibrated, single-candidate contract exactly."""
    df = _synthetic_training_frame(n=500)
    result = alpaca_options_model.train_model(df=df)
    assert result["ok"] is True
    assert result["calibrated"] is False
    assert result["ensemble_members"] is None
    assert result["model_type"] in {"logistic_regression", "random_forest", "gradient_boosting"}


def test_train_model_calibrates_above_the_holdout_floor():
    """A large enough fixture that the last walk-forward fold's test slice
    clears ALPACA_OPTIONS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS -- must ship a
    calibrated model (single candidate or ensemble, either is fine), and
    surfaces feature importances keyed by model name (not the old flat
    shape)."""
    df = _synthetic_training_frame(n=3000)
    result = alpaca_options_model.train_model(df=df)
    assert result["ok"] is True
    assert result["calibrated"] is True
    assert result["model_type"] in {"logistic_regression", "random_forest", "gradient_boosting", "ensemble"}
    if result["model_type"] == "ensemble":
        assert set(result["ensemble_members"]) <= {"logistic_regression", "random_forest", "gradient_boosting"}
        assert len(result["ensemble_members"]) >= 2
    else:
        assert result["ensemble_members"] is None
    assert result["feature_importances"]
    for name, importances in result["feature_importances"].items():
        assert importances
        assert "sentiment_score" in importances
        assert all(isinstance(v, float) for v in importances.values())


def test_train_model_cv_detail_reflects_the_walk_forward_folds():
    df = _synthetic_training_frame(n=3000)
    result = alpaca_options_model.train_model(df=df)
    assert result["ok"] is True
    assert len(result["cv_detail"]) <= alpaca_options_model.WALK_FORWARD_SPLITS
    assert all("test_rows" in fold for fold in result["cv_detail"])
    for name in {"logistic_regression", "random_forest", "gradient_boosting"}:
        assert "walk_forward_mean_score" in result["scores"][name]


def test_averaged_ensemble_predict_proba_is_the_mean_of_its_members():
    class _FakeModel:
        def __init__(self, up_proba):
            self._up_proba = up_proba

        def predict_proba(self, x):
            return np.array([[1.0 - self._up_proba, self._up_proba]] * len(x))

    ensemble = alpaca_options_model._AveragedEnsemble(  # noqa: SLF001
        [_FakeModel(0.6), _FakeModel(0.8)], ["model_a", "model_b"],
    )
    result = ensemble.predict_proba(np.zeros((2, 1)))
    assert result.shape == (2, 2)
    assert result[0][1] == pytest.approx(0.7)
    assert result[0][0] == pytest.approx(0.3)


def test_recency_sample_weight_favors_more_recent_rows():
    ts = np.array([0, 43200, 86400])  # 0, 0.5, 1 day (in seconds) before "now"
    weights = alpaca_options_model._recency_sample_weight(ts, half_life_days=1.0)  # noqa: SLF001
    assert weights[-1] == pytest.approx(1.0)  # most recent row: zero age, full weight
    assert weights[0] < weights[1] < weights[-1]
    assert weights[0] == pytest.approx(0.5)  # exactly one half-life old


def test_predict_direction_reports_model_ok_false_without_a_trained_model():
    result = alpaca_options_model.predict_direction("AAPL")
    assert result["model_ok"] is False


def test_predict_direction_uses_trained_model(monkeypatch):
    df = _synthetic_training_frame(n=500)
    train_result = alpaca_options_model.train_model(df=df)
    assert train_result["ok"] is True

    monkeypatch.setattr(alpaca_options_model, "latest_feature_row", lambda symbol: {
        "symbol": symbol, "current_price": 195.0, "short_ma": 194.0,
        "ret_1m": 0.0, "ret_5m": 0.0, "ret_15m": 0.0, "ret_30m": 0.0, "ret_60m": 0.0,
        "dist_to_ma_15": 0.03, "dist_to_ma_30": 0.015,
        "volatility_5": 0.001, "volatility_15": 0.001, "volatility_30": 0.001,
        "volume_ratio_5": 1.0, "volume_ratio_15": 1.0, "dollar_volume_z": 0.0,
        "rsi_14": 0.5, "macd_hist_pct": 0.0, "bb_pct_b": 0.5, "bb_bandwidth": 0.01, "atr_pct": 0.001, "stoch_k": 0.5,
        "time_of_day_pct": 0.5, "sentiment_score": 0.0,
    })
    prediction = alpaca_options_model.predict_direction("AAPL")
    assert prediction["model_ok"] is True
    assert prediction["direction"] in {"up", "down"}
    assert 0.0 <= prediction["probability_up"] <= 1.0


def test_predict_direction_works_with_a_calibrated_model(monkeypatch):
    df = _synthetic_training_frame(n=3000)
    train_result = alpaca_options_model.train_model(df=df)
    assert train_result["ok"] is True
    assert train_result["calibrated"] is True

    monkeypatch.setattr(alpaca_options_model, "latest_feature_row", lambda symbol: {
        "symbol": symbol, "current_price": 195.0, "short_ma": 194.0,
        "ret_1m": 0.0, "ret_5m": 0.0, "ret_15m": 0.0, "ret_30m": 0.0, "ret_60m": 0.0,
        "dist_to_ma_15": 0.03, "dist_to_ma_30": 0.015,
        "volatility_5": 0.001, "volatility_15": 0.001, "volatility_30": 0.001,
        "volume_ratio_5": 1.0, "volume_ratio_15": 1.0, "dollar_volume_z": 0.0,
        "rsi_14": 0.5, "macd_hist_pct": 0.0, "bb_pct_b": 0.5, "bb_bandwidth": 0.01, "atr_pct": 0.001, "stoch_k": 0.5,
        "time_of_day_pct": 0.5, "sentiment_score": 0.0,
    })
    prediction = alpaca_options_model.predict_direction("AAPL")
    assert prediction["model_ok"] is True
    assert prediction["direction"] in {"up", "down"}
    assert 0.0 <= prediction["probability_up"] <= 1.0


def test_download_model_from_hf_bounds_a_hang_instead_of_freezing(monkeypatch):
    """Real, confirmed production incident (same call shape, on perps):
    called directly from /api/alpaca/options/status on every cold boot,
    unbounded this can hang on huggingface_hub's own internal session lock
    long enough to freeze the whole --workers 1 process until gunicorn's
    worker timeout SIGKILLs it. Locks in the fix: a hang must degrade to
    "no model this one time", not an unbounded freeze."""
    import time as time_module

    monkeypatch.setattr(alpaca_options_model, "HF_API_KEY", "fake-key")
    monkeypatch.setattr(alpaca_options_model, "_MODEL_DOWNLOAD_HF_TIMEOUT_SEC", 0.2)

    def hangs_forever(*a, **k):
        time_module.sleep(30)
        raise AssertionError("should never get here")

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", hangs_forever)

    start = time_module.monotonic()
    result = alpaca_options_model._download_model_from_hf()  # noqa: SLF001
    elapsed = time_module.monotonic() - start

    assert result is False
    assert elapsed < 5  # must return promptly, not wait out the full 30s hang
