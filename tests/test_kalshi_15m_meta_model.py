"""Meta-labeling module: predicts whether kalshi_15m_model.py's own
primary direction classifier should be trusted in the current context.
Uses synthetic feature data only -- never touches Kalshi, Hugging Face,
or news feeds. Mirrors test_perps_meta_model.py's own test suite in
full (see its docstring) -- verifies the leakage-free out-of-fold
construction, the cold-start / not-enough-data paths, that a trained
meta-model actually produces a usable trust score, and the same
stale-cache HF-recheck wiring kalshi_15m_model.py's own tests already
lock in for the primary model."""
from __future__ import annotations

import time as time_module

import numpy as np
import pandas as pd
import pytest

import server_common
from data import kalshi_15m_meta_model, kalshi_15m_model


@pytest.fixture(autouse=True)
def _isolated_meta_model_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(kalshi_15m_meta_model, "META_MODEL_PATH", tmp_path / "meta_model.joblib")
    monkeypatch.setattr(kalshi_15m_meta_model, "META_MODEL_META_PATH", tmp_path / "meta_model_meta.json")
    monkeypatch.setattr(kalshi_15m_meta_model, "HF_API_KEY", "")
    kalshi_15m_meta_model._meta_model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001
    kalshi_15m_meta_model._hf_meta_recheck_state.update({"last_checked_at": 0.0, "checking": False})  # noqa: SLF001
    yield
    kalshi_15m_meta_model._meta_model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001
    kalshi_15m_meta_model._hf_meta_recheck_state.update({"last_checked_at": 0.0, "checking": False})  # noqa: SLF001


def _synthetic_training_frame(n: int = 1000, seed: int = 42) -> pd.DataFrame:
    """Same shape as test_kalshi_15m_model.py's own fixture -- a real,
    learnable label_up (driven by dist_to_ma_15) so the primary model's
    own per-fold accuracy is neither trivial nor random, giving the
    meta-model's own was_correct label some genuine structure to find."""
    rng = np.random.default_rng(seed)
    dist = rng.normal(0, 0.01, n)
    label = (dist > 0).astype(int)
    return pd.DataFrame({
        "symbol": ["BTC"] * n,
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
        "rsi_14": np.clip(rng.normal(0.5, 0.15, n), 0.0, 1.0),
        "macd_hist_pct": rng.normal(0, 0.001, n),
        "bb_pct_b": np.clip(rng.normal(0.5, 0.2, n), 0.0, 1.0),
        "bb_bandwidth": np.abs(rng.normal(0.01, 0.003, n)),
        "atr_pct": np.abs(rng.normal(0.001, 0.0003, n)),
        "stoch_k": np.clip(rng.normal(0.5, 0.2, n), 0.0, 1.0),
        "volume_ratio_5": np.abs(rng.normal(1.0, 0.3, n)),
        "volume_ratio_15": np.abs(rng.normal(1.0, 0.3, n)),
        "dollar_volume_z": rng.normal(0, 1.0, n),
        "oi_change_pct": rng.normal(0, 0.01, n),
        "spread_pct": np.abs(rng.normal(0.001, 0.0003, n)),
        "hour_sin": rng.uniform(-1, 1, n),
        "hour_cos": rng.uniform(-1, 1, n),
        "dow_sin": rng.uniform(-1, 1, n),
        "dow_cos": rng.uniform(-1, 1, n),
        "trend_pct": rng.normal(0, 0.01, n),
        "sentiment_score": rng.normal(0, 0.2, n),
        "label_up": label,
    })


# ── _build_oof_dataset ──────────────────────────────────────────────────

def test_build_oof_dataset_returns_none_for_an_unknown_model_type():
    df = _synthetic_training_frame(n=1000)
    assert kalshi_15m_meta_model._build_oof_dataset(df, model_type="not_a_real_candidate") is None  # noqa: SLF001


def test_build_oof_dataset_returns_none_below_the_minimum_row_floor():
    df = _synthetic_training_frame(n=50)
    assert kalshi_15m_meta_model._build_oof_dataset(df, model_type="logistic_regression") is None  # noqa: SLF001


def test_build_oof_dataset_produces_one_row_per_out_of_fold_test_sample():
    df = _synthetic_training_frame(n=1000)
    oof = kalshi_15m_meta_model._build_oof_dataset(df, model_type="logistic_regression")  # noqa: SLF001
    assert oof is not None
    assert 0 < len(oof) < 1000
    assert set(kalshi_15m_meta_model.META_FEATURE_COLUMNS + ["was_correct"]).issubset(oof.columns)
    assert oof["was_correct"].isin([0, 1]).all()
    assert ((oof["primary_confidence"] >= 0.0) & (oof["primary_confidence"] <= 1.0)).all()


def test_build_oof_dataset_is_leakage_free_ordering():
    df = _synthetic_training_frame(n=1000)
    oof = kalshi_15m_meta_model._build_oof_dataset(df, model_type="logistic_regression")  # noqa: SLF001
    assert oof is not None
    assert oof["ts"].is_monotonic_increasing


# ── train_meta_model ─────────────────────────────────────────────────────

def test_train_meta_model_with_no_data_returns_not_ok():
    result = kalshi_15m_meta_model.train_meta_model(df=pd.DataFrame(), primary_model_type="logistic_regression")
    assert result["ok"] is False
    assert result["reason"] == "no_data"


def test_train_meta_model_with_too_few_rows_returns_not_ok():
    df = _synthetic_training_frame(n=50)
    result = kalshi_15m_meta_model.train_meta_model(df=df, primary_model_type="logistic_regression")
    assert result["ok"] is False
    assert result["reason"] == "insufficient_oof_rows"


def test_train_meta_model_requires_a_primary_model_type_when_none_is_trained_yet(monkeypatch):
    monkeypatch.setattr(kalshi_15m_model, "load_model", lambda: (None, None))
    df = _synthetic_training_frame(n=1000)
    result = kalshi_15m_meta_model.train_meta_model(df=df)
    assert result["ok"] is False
    assert result["reason"] == "no_primary_model_type"


def test_train_meta_model_succeeds_with_enough_signal_rows():
    df = _synthetic_training_frame(n=1000)
    result = kalshi_15m_meta_model.train_meta_model(df=df, primary_model_type="logistic_regression")
    assert result["ok"] is True
    assert result["oof_rows"] > 0
    assert result["primary_model_type"] == "logistic_regression"
    assert kalshi_15m_meta_model.META_MODEL_PATH.exists()
    assert kalshi_15m_meta_model.META_MODEL_META_PATH.exists()


def test_train_meta_model_falls_back_to_the_best_base_candidate_for_an_ensemble_primary(monkeypatch):
    monkeypatch.setattr(kalshi_15m_model, "load_model", lambda: (object(), {
        "model_type": "ensemble",
        "scores": {
            "logistic_regression": {"walk_forward_mean_score": 0.55},
            "random_forest": {"walk_forward_mean_score": 0.61},
            "gradient_boosting": {"walk_forward_mean_score": 0.58},
        },
    }))
    df = _synthetic_training_frame(n=1000)
    result = kalshi_15m_meta_model.train_meta_model(df=df)
    assert result["ok"] is True
    assert result["primary_model_type"] == "random_forest"


def test_train_meta_model_reads_the_primary_model_type_when_not_given_explicitly(monkeypatch):
    monkeypatch.setattr(kalshi_15m_model, "load_model", lambda: (object(), {"model_type": "gradient_boosting"}))
    df = _synthetic_training_frame(n=1000)
    result = kalshi_15m_meta_model.train_meta_model(df=df)
    assert result["ok"] is True
    assert result["primary_model_type"] == "gradient_boosting"


# ── trust_score ──────────────────────────────────────────────────────────

def test_trust_score_returns_none_without_a_trained_meta_model():
    row = {"volatility_30": 0.001, "atr_pct": 0.001, "bb_bandwidth": 0.01,
           "dollar_volume_z": 0.0, "oi_change_pct": 0.0, "hour_sin": 0.0, "hour_cos": 1.0}
    assert kalshi_15m_meta_model.trust_score(row, primary_probability_up=0.7) is None


def test_trust_score_returns_none_for_a_missing_row():
    assert kalshi_15m_meta_model.trust_score(None, primary_probability_up=0.7) is None


def test_trust_score_never_raises_on_a_row_missing_every_context_column():
    df = _synthetic_training_frame(n=1000)
    assert kalshi_15m_meta_model.train_meta_model(df=df, primary_model_type="logistic_regression")["ok"] is True
    score = kalshi_15m_meta_model.trust_score({}, primary_probability_up=0.7)
    assert score is not None
    assert 0.0 <= score <= 1.0


def test_trust_score_produces_a_valid_probability_after_training():
    df = _synthetic_training_frame(n=1000)
    train_result = kalshi_15m_meta_model.train_meta_model(df=df, primary_model_type="logistic_regression")
    assert train_result["ok"] is True

    row = {"volatility_30": 0.0012, "atr_pct": 0.001, "bb_bandwidth": 0.01,
           "dollar_volume_z": 0.0, "oi_change_pct": 0.0, "hour_sin": 0.0, "hour_cos": 1.0}
    score = kalshi_15m_meta_model.trust_score(row, primary_probability_up=0.8)
    assert score is not None
    assert 0.0 <= score <= 1.0


# ── HF stale-cache recheck wiring -- same bug class kalshi_15m_model.py's
# own tests already lock in. ────────────────────────────────────────────

def test_load_meta_model_schedules_a_background_hf_recheck_when_something_is_already_cached(monkeypatch):
    kalshi_15m_meta_model._meta_model_cache.update({"model": object(), "meta": {"trained_at": "t1"}, "loaded_at": time_module.time()})  # noqa: SLF001
    fired = server_common.threading.Event()
    monkeypatch.setattr(kalshi_15m_meta_model, "_schedule_hf_meta_model_recheck", lambda: fired.set())

    kalshi_15m_meta_model.load_meta_model()

    assert fired.is_set()


def test_load_meta_model_does_not_schedule_a_recheck_with_nothing_cached_yet(monkeypatch):
    def fail_if_called():
        raise AssertionError("must not schedule a recheck before ever having a meta-model")

    monkeypatch.setattr(kalshi_15m_meta_model, "_schedule_hf_meta_model_recheck", fail_if_called)
    kalshi_15m_meta_model.load_meta_model()  # no meta-model file on disk (isolated tmp_path) -- falls through to the None, None path


def test_schedule_hf_meta_model_recheck_picks_up_a_newer_meta_model_from_hf(monkeypatch):
    kalshi_15m_meta_model._meta_model_cache.update({"model": "old-meta-model", "meta": {"trained_at": "2026-01-01T00:00:00+00:00"}, "loaded_at": time_module.time()})  # noqa: SLF001
    monkeypatch.setattr(kalshi_15m_meta_model, "HF_API_KEY", "fake-key")
    monkeypatch.setattr(server_common, "pull_json_from_hf", lambda *a, **k: {"trained_at": "2026-02-01T00:00:00+00:00"})
    monkeypatch.setattr(kalshi_15m_meta_model, "_download_meta_model_from_hf", lambda: True)

    kalshi_15m_meta_model._schedule_hf_meta_model_recheck()  # noqa: SLF001
    for _ in range(50):
        if kalshi_15m_meta_model._meta_model_cache["loaded_at"] == 0.0:  # noqa: SLF001
            break
        time_module.sleep(0.02)
    assert kalshi_15m_meta_model._meta_model_cache["loaded_at"] == 0.0  # noqa: SLF001


def test_schedule_hf_meta_model_recheck_leaves_the_cache_alone_when_hf_has_nothing_newer(monkeypatch):
    original_loaded_at = time_module.time()
    kalshi_15m_meta_model._meta_model_cache.update({"model": "current-meta-model", "meta": {"trained_at": "2026-01-01T00:00:00+00:00"}, "loaded_at": original_loaded_at})  # noqa: SLF001
    monkeypatch.setattr(kalshi_15m_meta_model, "HF_API_KEY", "fake-key")
    monkeypatch.setattr(server_common, "pull_json_from_hf", lambda *a, **k: {"trained_at": "2026-01-01T00:00:00+00:00"})

    def fail_if_called():
        raise AssertionError("must not re-download when HF's trained_at matches what's already cached")

    monkeypatch.setattr(kalshi_15m_meta_model, "_download_meta_model_from_hf", fail_if_called)

    kalshi_15m_meta_model._schedule_hf_meta_model_recheck()  # noqa: SLF001
    time_module.sleep(0.1)
    assert kalshi_15m_meta_model._meta_model_cache["loaded_at"] == original_loaded_at  # noqa: SLF001


def test_download_meta_model_from_hf_bounds_a_hang_instead_of_freezing(monkeypatch):
    monkeypatch.setattr(kalshi_15m_meta_model, "HF_API_KEY", "fake-key")
    monkeypatch.setattr(kalshi_15m_meta_model, "_META_MODEL_DOWNLOAD_HF_TIMEOUT_SEC", 0.2)

    def hangs_forever(*a, **k):
        time_module.sleep(30)
        raise AssertionError("should never get here")

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", hangs_forever)

    start = time_module.monotonic()
    result = kalshi_15m_meta_model._download_meta_model_from_hf()  # noqa: SLF001
    elapsed = time_module.monotonic() - start

    assert result is False
    assert elapsed < 5
