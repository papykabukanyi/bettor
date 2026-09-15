"""Alpaca stock direction-classifier training + prediction -- structurally
identical tests to test_perps_model.py, but for the separate Alpaca
pipeline. Synthetic feature data only; never touches Alpaca, HF, or the
network."""
from __future__ import annotations

import json
import time as time_module

import numpy as np
import pandas as pd
import pytest

import server_common
from data import alpaca_model


@pytest.fixture(autouse=True)
def _isolated_model_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(alpaca_model, "MODEL_PATH", tmp_path / "model.joblib")
    monkeypatch.setattr(alpaca_model, "MODEL_META_PATH", tmp_path / "model_meta.json")
    monkeypatch.setattr(alpaca_model, "HF_API_KEY", "")
    alpaca_model._model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001
    # _hf_recheck_state is ALSO module-level global state, shared with
    # _model_cache's own real risk here: any OTHER test's incidental
    # load_model() cache-hit already sets last_checked_at, which would
    # then silently rate-limit every later test's own recheck attempt for
    # the rest of the whole pytest process -- reset the same way.
    alpaca_model._hf_recheck_state.update({"last_checked_at": 0.0, "checking": False})  # noqa: SLF001
    yield
    alpaca_model._model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001
    alpaca_model._hf_recheck_state.update({"last_checked_at": 0.0, "checking": False})  # noqa: SLF001


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
    shows up in meta.feature_importances the same as every other feature.
    Keyed by candidate name (winning candidate only below the calibration
    holdout floor, every candidate once above it -- see
    test_train_model_calibrates_above_the_holdout_floor) -- matches
    perps_model.py's/alpaca_crypto_model.py's own contract exactly."""
    df = _synthetic_training_frame(n=500)
    result = alpaca_model.train_model(df=df)
    assert result["ok"] is True
    importances = result["feature_importances"]
    assert importances
    for name, feature_map in importances.items():
        assert feature_map
        assert "sentiment_score" in feature_map


def test_feature_importance_map_uses_coef_for_logistic_regression():
    from sklearn.linear_model import LogisticRegression
    import numpy as np

    model = LogisticRegression().fit(np.array([[0.0, 1.0], [1.0, 0.0], [0.0, 0.0], [1.0, 1.0]]), [0, 1, 0, 1])
    importances = alpaca_model._feature_importance_map(model, ["a", "b"])  # noqa: SLF001
    assert set(importances.keys()) == {"a", "b"}


# ── Walk-forward CV / calibration / outcome-aware weighting -- ported from
# perps_model.py's/alpaca_crypto_model.py's own design (see this module's
# docstring for why). ─────────────────────────────────────────────────────

def test_train_model_stays_uncalibrated_below_the_holdout_floor():
    """n=500's last walk-forward fold test slice (~100 rows) falls below
    ALPACA_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS (200) -- must fall back to
    the old, uncalibrated, single-candidate contract exactly, same
    regression fixture shape as perps_model.py's/alpaca_crypto_model.py's
    own identical test."""
    result = alpaca_model.train_model(df=_synthetic_training_frame(n=500))
    assert result["ok"] is True
    assert result["calibrated"] is False
    assert result["ensemble_members"] is None
    assert result["model_type"] in {"logistic_regression", "random_forest", "gradient_boosting"}


def test_train_model_calibrates_above_the_holdout_floor():
    """A large enough fixture that the last walk-forward fold's test slice
    clears ALPACA_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS -- must ship a
    calibrated model (single candidate or ensemble, either is fine)."""
    result = alpaca_model.train_model(df=_synthetic_training_frame(n=3000))
    assert result["ok"] is True
    assert result["calibrated"] is True
    assert result["model_type"] in {"logistic_regression", "random_forest", "gradient_boosting", "ensemble"}
    if result["model_type"] == "ensemble":
        assert set(result["ensemble_members"]) <= {"logistic_regression", "random_forest", "gradient_boosting"}
        assert len(result["ensemble_members"]) >= 2
    else:
        assert result["ensemble_members"] is None


def test_train_model_cv_detail_reflects_the_walk_forward_folds():
    result = alpaca_model.train_model(df=_synthetic_training_frame(n=3000))
    assert result["ok"] is True
    cv_detail = result["cv_detail"]
    assert len(cv_detail) <= alpaca_model.WALK_FORWARD_SPLITS
    assert all("test_rows" in fold for fold in cv_detail)


def test_recency_sample_weight_favors_more_recent_rows():
    ts = np.array([0, 30 * 86400, 60 * 86400], dtype=float)  # oldest, mid, newest (60 days apart)
    weights = alpaca_model._recency_sample_weight(ts, half_life_days=14)  # noqa: SLF001
    assert weights[2] == 1.0  # newest row relative to itself -- no decay
    assert weights[0] < weights[1] < weights[2]


def test_trade_outcome_sample_weight_returns_all_ones_without_a_trade_log():
    symbols = np.array(["AAPL", "MSFT"])
    ts = np.array([1000.0, 2000.0])
    weights = alpaca_model._trade_outcome_sample_weight(symbols, ts, None)  # noqa: SLF001
    assert (weights == 1.0).all()


def test_trade_outcome_sample_weight_upweights_a_matching_real_win():
    minute_ts = 120.0
    symbols = np.array(["AAPL"])
    ts = np.array([minute_ts])
    trade_log = [{"symbol": "AAPL", "opened_at": "1970-01-01T00:02:00+00:00", "realized_pnl_usd": 5.0, "dry_run": False}]
    weights = alpaca_model._trade_outcome_sample_weight(symbols, ts, trade_log)  # noqa: SLF001
    assert weights[0] == alpaca_model.ALPACA_MODEL_TRADE_OUTCOME_WIN_WEIGHT


def test_trade_outcome_sample_weight_upweights_a_loss_more_than_a_win():
    ts = np.array([60.0, 120.0])
    symbols = np.array(["AAPL", "MSFT"])
    trade_log = [
        {"symbol": "AAPL", "opened_at": "1970-01-01T00:01:00+00:00", "realized_pnl_usd": 5.0, "dry_run": False},
        {"symbol": "MSFT", "opened_at": "1970-01-01T00:02:00+00:00", "realized_pnl_usd": -5.0, "dry_run": False},
    ]
    weights = alpaca_model._trade_outcome_sample_weight(symbols, ts, trade_log)  # noqa: SLF001
    assert weights[1] > weights[0] > 1.0


def test_trade_outcome_sample_weight_ignores_dry_run_trades():
    ts = np.array([60.0])
    symbols = np.array(["AAPL"])
    trade_log = [{"symbol": "AAPL", "opened_at": "1970-01-01T00:01:00+00:00", "realized_pnl_usd": 5.0, "dry_run": True}]
    weights = alpaca_model._trade_outcome_sample_weight(symbols, ts, trade_log)  # noqa: SLF001
    assert weights[0] == 1.0


def test_train_model_accepts_a_trade_log_and_reports_how_many_rows_matched():
    df = _synthetic_training_frame(n=3000)
    # Row 0 has ts=0 -- match it to a real winning trade at the same minute.
    trade_log = [{"symbol": "AAPL", "opened_at": "1970-01-01T00:00:00+00:00", "realized_pnl_usd": 5.0, "dry_run": False}]
    result = alpaca_model.train_model(df=df, trade_log=trade_log)
    assert result["ok"] is True
    assert result["trade_outcome_rows_matched"] >= 1


def test_train_model_with_no_trade_log_matches_zero_rows():
    result = alpaca_model.train_model(df=_synthetic_training_frame(n=3000))
    assert result["ok"] is True
    assert result["trade_outcome_rows_matched"] == 0


def test_averaged_ensemble_predict_proba_is_the_mean_of_its_members():
    class _Fake:
        def __init__(self, proba):
            self._proba = proba

        def predict_proba(self, x):
            return np.tile(self._proba, (len(x), 1))

    ensemble = alpaca_model._AveragedEnsemble(  # noqa: SLF001
        [_Fake([0.2, 0.8]), _Fake([0.6, 0.4])], ["a", "b"],
    )
    proba = ensemble.predict_proba(np.zeros((1, 3)))
    assert proba[0][1] == pytest.approx(0.6)


def test_averaged_ensemble_predict_thresholds_at_half():
    class _Fake:
        def __init__(self, proba):
            self._proba = proba

        def predict_proba(self, x):
            return np.tile(self._proba, (len(x), 1))

    ensemble = alpaca_model._AveragedEnsemble([_Fake([0.9, 0.1])], ["a"])  # noqa: SLF001
    assert ensemble.predict(np.zeros((1, 3)))[0] == 0


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


# ---------------------------------------------------------------------------
# _TorchMLPClassifier / train_torch_candidate_model -- the custom PyTorch
# candidate, trained in complete isolation from train_model()'s existing
# sklearn candidates (see both docstrings for why: measured locally,
# `import torch` alone costs ~154MB RSS on a 512MB-ceiling service with real
# OOM history). Real torch, not mocked -- these are the same correctness
# guarantees as the sklearn candidates, just for a hand-built architecture.
# ---------------------------------------------------------------------------
def test_torch_mlp_classifier_fit_predict_proba_shape_and_range():
    rng = np.random.default_rng(0)
    x = rng.normal(size=(200, 5))
    y = (x[:, 0] > 0).astype(int)

    clf = alpaca_model._TorchMLPClassifier(input_dim=5, epochs=5)  # noqa: SLF001
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

    clf = alpaca_model._TorchMLPClassifier(input_dim=5, epochs=5)  # noqa: SLF001
    clf.fit(x, y)
    proba_before = clf.predict_proba(x)

    path = tmp_path / "torch_clf.joblib"
    joblib.dump(clf, path)
    reloaded = joblib.load(path)
    proba_after = reloaded.predict_proba(x)
    assert np.allclose(proba_before, proba_after)


def test_train_torch_candidate_model_with_no_data_returns_not_ok():
    result = alpaca_model.train_torch_candidate_model(df=pd.DataFrame())
    assert result["ok"] is False
    assert result["reason"] == "no_data"


def test_train_torch_candidate_model_with_too_few_rows_returns_not_ok():
    small_df = _synthetic_training_frame(n=20)
    result = alpaca_model.train_torch_candidate_model(df=small_df)
    assert result["ok"] is False
    assert result["reason"] == "insufficient_rows"


def test_train_torch_candidate_model_promotes_unconditionally_with_no_current_model(monkeypatch):
    monkeypatch.setattr(alpaca_model, "load_model", lambda: (None, None))
    df = _synthetic_training_frame(n=500)

    result = alpaca_model.train_torch_candidate_model(df=df)

    assert result["ok"] is True
    assert result["promoted"] is True
    assert result["current_score"] is None
    assert alpaca_model.MODEL_PATH.exists()
    meta = json.loads(alpaca_model.MODEL_META_PATH.read_text(encoding="utf-8"))
    assert meta["model_type"] == "torch_mlp"


def test_train_torch_candidate_model_does_not_promote_a_worse_candidate(monkeypatch):
    """A current model that scores perfectly on the holdout must never be
    displaced by a torch candidate that (being a small net on noisy
    synthetic data) can't realistically match it -- the "only ship if the
    evidence says so" discipline this whole codebase already applies to
    every other candidate comparison."""

    class _PerfectModel:
        def predict(self, x):
            return (x[:, 5] > 0).astype(int)  # column 5 is dist_to_ma_15, == label_up's own generator

        def predict_proba(self, x):
            preds = self.predict(x)
            return np.column_stack([1.0 - preds, preds]).astype(float)

    monkeypatch.setattr(alpaca_model, "load_model", lambda: (_PerfectModel(), {"model_type": "perfect_stub"}))
    before = alpaca_model.MODEL_PATH.exists()
    df = _synthetic_training_frame(n=500)

    result = alpaca_model.train_torch_candidate_model(df=df)

    assert result["ok"] is True
    assert result["promoted"] is False
    assert result["current_score"] == 1.0
    assert alpaca_model.MODEL_PATH.exists() == before  # untouched -- nothing was written


def test_train_torch_candidate_model_promoted_model_is_usable_via_predict_direction(monkeypatch):
    real_load_model = alpaca_model.load_model
    monkeypatch.setattr(alpaca_model, "load_model", lambda: (None, None))
    df = _synthetic_training_frame(n=500)
    train_result = alpaca_model.train_torch_candidate_model(df=df)
    assert train_result["ok"] is True and train_result["promoted"] is True

    monkeypatch.setattr(alpaca_model, "load_model", real_load_model)  # predict_direction below needs the real one to read the just-promoted model back off disk
    alpaca_model._model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001 -- force a real reload from disk
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
    assert prediction["model_type"] == "torch_mlp"
    assert prediction["direction"] in {"up", "down"}


# ── Real, confirmed bug this closes: load_model() never used to re-check
# HF once a local copy existed, only re-downloading if the file was
# missing entirely -- see server_common.maybe_schedule_hf_model_recheck's
# own docstring / test_perps_model.py's identical coverage for the full
# motivation (training moving off this process onto a scheduled Hugging
# Face Job). ───────────────────────────────────────────────────────────

def test_load_model_schedules_a_background_hf_recheck_when_something_is_already_cached(monkeypatch):
    alpaca_model._model_cache.update({"model": object(), "meta": {"trained_at": "t1"}, "loaded_at": time_module.time()})  # noqa: SLF001
    fired = server_common.threading.Event()
    monkeypatch.setattr(alpaca_model, "_schedule_hf_model_recheck", lambda: fired.set())

    alpaca_model.load_model()

    assert fired.is_set()


def test_load_model_does_not_schedule_a_recheck_with_nothing_cached_yet(monkeypatch):
    def fail_if_called():
        raise AssertionError("must not schedule a recheck before ever having a model")

    monkeypatch.setattr(alpaca_model, "_schedule_hf_model_recheck", fail_if_called)
    alpaca_model.load_model()  # no model file on disk (isolated tmp_path) -- falls through to the None, None path


def test_schedule_hf_model_recheck_picks_up_a_newer_model_from_hf(monkeypatch):
    perps_loaded_at = time_module.time()
    alpaca_model._model_cache.update({"model": "old-model-object", "meta": {"trained_at": "2026-01-01T00:00:00+00:00"}, "loaded_at": perps_loaded_at})  # noqa: SLF001
    monkeypatch.setattr(alpaca_model, "HF_API_KEY", "fake-key")
    monkeypatch.setattr(server_common, "pull_json_from_hf", lambda *a, **k: {"trained_at": "2026-02-01T00:00:00+00:00"})
    monkeypatch.setattr(alpaca_model, "_download_model_from_hf", lambda: True)

    alpaca_model._schedule_hf_model_recheck()  # noqa: SLF001
    for _ in range(50):
        if alpaca_model._model_cache["loaded_at"] == 0.0:  # noqa: SLF001
            break
        time_module.sleep(0.02)
    assert alpaca_model._model_cache["loaded_at"] == 0.0  # noqa: SLF001


def test_schedule_hf_model_recheck_leaves_the_cache_alone_when_hf_has_nothing_newer(monkeypatch):
    original_loaded_at = time_module.time()
    alpaca_model._model_cache.update({"model": "current-model-object", "meta": {"trained_at": "2026-01-01T00:00:00+00:00"}, "loaded_at": original_loaded_at})  # noqa: SLF001
    monkeypatch.setattr(alpaca_model, "HF_API_KEY", "fake-key")
    monkeypatch.setattr(server_common, "pull_json_from_hf", lambda *a, **k: {"trained_at": "2026-01-01T00:00:00+00:00"})

    def fail_if_called():
        raise AssertionError("must not re-download when HF's trained_at matches what's already cached")

    monkeypatch.setattr(alpaca_model, "_download_model_from_hf", fail_if_called)

    alpaca_model._schedule_hf_model_recheck()  # noqa: SLF001
    time_module.sleep(0.1)
    assert alpaca_model._model_cache["loaded_at"] == original_loaded_at  # noqa: SLF001
