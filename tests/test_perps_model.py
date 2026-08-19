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


def test_train_model_stays_uncalibrated_below_the_holdout_floor():
    """n=500 is exactly the pre-existing regression fixture -- its last
    walk-forward fold's test slice (~100 rows) falls below
    PERPS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS (200), so this MUST fall back
    to the old, uncalibrated, single-candidate contract exactly."""
    df = _synthetic_training_frame(n=500)
    result = perps_model.train_model(df=df)
    assert result["ok"] is True
    assert result["calibrated"] is False
    assert result["ensemble_members"] is None
    assert result["model_type"] in {"logistic_regression", "random_forest", "gradient_boosting"}


# ---------------------------------------------------------------------------
# _trade_outcome_sample_weight -- the bot's own real trade wins/losses
# folded into training, not just abstract next-minute price direction with
# no connection to its actual trading history. A heuristic proxy (see its
# own docstring), so these tests verify the MECHANICS (matching, weighting,
# dry-run exclusion) rather than claiming any particular predictive lift.
# ---------------------------------------------------------------------------
def _iso(epoch_seconds: int) -> str:
    import datetime as dt
    return dt.datetime.fromtimestamp(epoch_seconds, tz=dt.timezone.utc).isoformat()


def test_trade_outcome_sample_weight_returns_all_ones_without_a_trade_log():
    tickers = np.array(["KXBTCPERP", "KXETHPERP"])
    ts = np.array([0, 60])
    weights = perps_model._trade_outcome_sample_weight(tickers, ts, None)  # noqa: SLF001
    assert list(weights) == [1.0, 1.0]


def test_trade_outcome_sample_weight_upweights_a_matching_real_win():
    tickers = np.array(["KXBTCPERP", "KXETHPERP"])
    ts = np.array([0, 6000])
    trade_log = [{"ticker": "KXBTCPERP", "opened_at": _iso(0), "realized_pnl_usd": 1.0, "dry_run": False}]
    weights = perps_model._trade_outcome_sample_weight(tickers, ts, trade_log)  # noqa: SLF001
    assert weights[0] == perps_model.PERPS_MODEL_TRADE_OUTCOME_WIN_WEIGHT
    assert weights[1] == 1.0  # unmatched row untouched


def test_trade_outcome_sample_weight_upweights_a_loss_more_than_a_win():
    tickers = np.array(["KXBTCPERP"])
    ts = np.array([0])
    win = perps_model._trade_outcome_sample_weight(  # noqa: SLF001
        tickers, ts, [{"ticker": "KXBTCPERP", "opened_at": _iso(0), "realized_pnl_usd": 1.0, "dry_run": False}],
    )
    loss = perps_model._trade_outcome_sample_weight(  # noqa: SLF001
        tickers, ts, [{"ticker": "KXBTCPERP", "opened_at": _iso(0), "realized_pnl_usd": -1.0, "dry_run": False}],
    )
    assert loss[0] > win[0]
    assert win[0] == perps_model.PERPS_MODEL_TRADE_OUTCOME_WIN_WEIGHT
    assert loss[0] == perps_model.PERPS_MODEL_TRADE_OUTCOME_LOSS_WEIGHT


def test_trade_outcome_sample_weight_ignores_dry_run_trades():
    tickers = np.array(["KXBTCPERP"])
    ts = np.array([0])
    trade_log = [{"ticker": "KXBTCPERP", "opened_at": _iso(0), "realized_pnl_usd": 1.0, "dry_run": True}]
    weights = perps_model._trade_outcome_sample_weight(tickers, ts, trade_log)  # noqa: SLF001
    assert weights[0] == 1.0


def test_trade_outcome_sample_weight_requires_matching_ticker_not_just_time():
    tickers = np.array(["KXETHPERP"])
    ts = np.array([0])
    trade_log = [{"ticker": "KXBTCPERP", "opened_at": _iso(0), "realized_pnl_usd": 1.0, "dry_run": False}]
    weights = perps_model._trade_outcome_sample_weight(tickers, ts, trade_log)  # noqa: SLF001
    assert weights[0] == 1.0  # different ticker at the same time -- must not match


def test_train_model_accepts_a_trade_log_and_reports_how_many_rows_matched():
    df = _synthetic_training_frame(n=500)
    # Rows 0-59 all share minute_ts=0 with this trade's opened_at -- see
    # this module's own minute-aligned matching in _trade_outcome_sample_weight.
    trade_log = [{"ticker": "KXBTCPERP", "opened_at": _iso(0), "realized_pnl_usd": 1.0, "dry_run": False}]
    result = perps_model.train_model(df=df, trade_log=trade_log)
    assert result["ok"] is True
    assert result["trade_outcome_rows_matched"] > 0


def test_train_model_with_no_trade_log_matches_zero_rows():
    df = _synthetic_training_frame(n=500)
    result = perps_model.train_model(df=df, trade_log=None)
    assert result["ok"] is True
    assert result["trade_outcome_rows_matched"] == 0


def test_train_model_calibrates_above_the_holdout_floor():
    """A large enough fixture that the last walk-forward fold's test slice
    clears PERPS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS -- must ship a
    calibrated model (single candidate or ensemble, either is fine)."""
    df = _synthetic_training_frame(n=3000)
    result = perps_model.train_model(df=df)
    assert result["ok"] is True
    assert result["calibrated"] is True
    assert result["model_type"] in {"logistic_regression", "random_forest", "gradient_boosting", "ensemble"}
    if result["model_type"] == "ensemble":
        assert set(result["ensemble_members"]) <= {"logistic_regression", "random_forest", "gradient_boosting"}
        assert len(result["ensemble_members"]) >= 2
    else:
        assert result["ensemble_members"] is None
    # Feature-importance observability: at least the winning candidate (or
    # every ensemble member) has a real, non-empty importance map.
    assert result["feature_importances"]
    for name, importances in result["feature_importances"].items():
        assert importances
        assert all(isinstance(v, float) for v in importances.values())


def test_train_model_cv_detail_reflects_the_walk_forward_folds():
    df = _synthetic_training_frame(n=3000)
    result = perps_model.train_model(df=df)
    assert result["ok"] is True
    assert len(result["cv_detail"]) <= perps_model.WALK_FORWARD_SPLITS
    assert all("test_rows" in fold for fold in result["cv_detail"])
    for name in {"logistic_regression", "random_forest", "gradient_boosting"}:
        assert "walk_forward_mean_score" in result["scores"][name]


def test_predict_direction_works_with_a_calibrated_model(monkeypatch):
    df = _synthetic_training_frame(n=3000)
    train_result = perps_model.train_model(df=df)
    assert train_result["ok"] is True
    assert train_result["calibrated"] is True

    monkeypatch.setattr(perps_model, "latest_feature_row", lambda ticker: {
        "ticker": ticker, "current_price": 100.0, "short_ma": 99.0, "trend_pct": 0.0,
        "ret_1m": 0.0, "ret_3m": 0.0, "ret_5m": 0.0, "ret_10m": 0.0, "ret_15m": 0.0, "ret_30m": 0.0,
        "trend_1h": 0.0, "trend_2h": 0.0, "trend_3h": 0.0, "trend_4h": 0.0,
        "dist_to_ma_15": 0.03, "dist_to_ma_30": 0.015,
        "volatility_5": 0.001, "volatility_15": 0.001, "volatility_30": 0.001,
        "rsi_14": 0.5, "macd_hist_pct": 0.0, "bb_pct_b": 0.5, "bb_bandwidth": 0.01, "atr_pct": 0.001, "stoch_k": 0.5,
        "volume_ratio_5": 1.0, "volume_ratio_15": 1.0, "dollar_volume_z": 0.0, "oi_change_pct": 0.0, "spread_pct": 0.001,
        "hour_sin": 0.0, "hour_cos": 1.0, "dow_sin": 0.0, "dow_cos": 1.0,
        "sentiment_score": 0.0,
    })
    prediction = perps_model.predict_direction("KXBTCPERP")
    assert prediction["model_ok"] is True
    assert prediction["direction"] in {"up", "down"}
    assert 0.0 <= prediction["probability_up"] <= 1.0


def test_averaged_ensemble_predict_proba_is_the_mean_of_its_members():
    class _FakeModel:
        def __init__(self, up_proba):
            self._up_proba = up_proba

        def predict_proba(self, x):
            return np.array([[1.0 - self._up_proba, self._up_proba]] * len(x))

    ensemble = perps_model._AveragedEnsemble(  # noqa: SLF001
        [_FakeModel(0.6), _FakeModel(0.8)], ["model_a", "model_b"],
    )
    result = ensemble.predict_proba(np.zeros((2, 1)))
    assert result.shape == (2, 2)
    assert result[0][1] == pytest.approx(0.7)
    assert result[0][0] == pytest.approx(0.3)


def test_averaged_ensemble_predict_thresholds_at_half():
    """Real, confirmed live bug on the equities-options sibling of this
    exact class (identical copy): this class used to have no predict() at
    all, so any caller expecting standard sklearn predict()/predict_proba()
    symmetry (e.g. a torch-vs-current-model re-score comparison) raised
    AttributeError whenever an averaged ensemble was the live model."""
    class _FakeModel:
        def __init__(self, up_proba):
            self._up_proba = up_proba

        def predict_proba(self, x):
            return np.array([[1.0 - self._up_proba, self._up_proba]] * len(x))

    ensemble = perps_model._AveragedEnsemble(  # noqa: SLF001
        [_FakeModel(0.7), _FakeModel(0.9)], ["model_a", "model_b"],  # mean up_proba = 0.8
    )
    up_preds = ensemble.predict(np.zeros((2, 1)))
    assert list(up_preds) == [1, 1]

    down_ensemble = perps_model._AveragedEnsemble(  # noqa: SLF001
        [_FakeModel(0.1), _FakeModel(0.3)], ["model_a", "model_b"],  # mean up_proba = 0.2
    )
    down_preds = down_ensemble.predict(np.zeros((2, 1)))
    assert list(down_preds) == [0, 0]


def test_recency_sample_weight_favors_more_recent_rows():
    ts = np.array([0, 43200, 86400])  # 0, 0.5, 1 day (in seconds) before "now"
    weights = perps_model._recency_sample_weight(ts, half_life_days=1.0)  # noqa: SLF001
    assert weights[-1] == pytest.approx(1.0)  # most recent row: zero age, full weight
    assert weights[0] < weights[1] < weights[-1]
    assert weights[0] == pytest.approx(0.5)  # exactly one half-life old


def test_predict_direction_uses_trained_model(monkeypatch):
    df = _synthetic_training_frame(n=500)
    train_result = perps_model.train_model(df=df)
    assert train_result["ok"] is True

    monkeypatch.setattr(perps_model, "latest_feature_row", lambda ticker: {
        "ticker": ticker, "current_price": 100.0, "short_ma": 99.0, "trend_pct": 0.0,
        "ret_1m": 0.0, "ret_3m": 0.0, "ret_5m": 0.0, "ret_10m": 0.0, "ret_15m": 0.0, "ret_30m": 0.0,
        "trend_1h": 0.0, "trend_2h": 0.0, "trend_3h": 0.0, "trend_4h": 0.0,
        "dist_to_ma_15": 0.03, "dist_to_ma_30": 0.015,
        "volatility_5": 0.001, "volatility_15": 0.001, "volatility_30": 0.001,
        "rsi_14": 0.5, "macd_hist_pct": 0.0, "bb_pct_b": 0.5, "bb_bandwidth": 0.01, "atr_pct": 0.001, "stoch_k": 0.5,
        "volume_ratio_5": 1.0, "volume_ratio_15": 1.0, "dollar_volume_z": 0.0, "oi_change_pct": 0.0, "spread_pct": 0.001,
        "hour_sin": 0.0, "hour_cos": 1.0, "dow_sin": 0.0, "dow_cos": 1.0,
        "sentiment_score": 0.0,
    })
    prediction = perps_model.predict_direction("KXBTCPERP")
    assert prediction["model_ok"] is True
    assert prediction["direction"] in {"up", "down"}
    assert 0.0 <= prediction["probability_up"] <= 1.0


def test_download_model_from_hf_bounds_a_hang_instead_of_freezing(monkeypatch):
    """Real, confirmed production incident: this is called directly from
    /api/status on every cold boot (MODEL_PATH/MODEL_META_PATH only exist
    locally after the first successful download, wiped by every restart).
    Unbounded, a hang here (huggingface_hub's own internal session lock,
    not a slow response) froze the entire --workers 1 process until
    gunicorn's own worker timeout SIGKILLed it -- which wipes local disk
    and guarantees the next boot hits this same call again: a
    self-sustaining crash loop. This locks in the fix: a hang must degrade
    to "no model this one time", not an unbounded freeze."""
    import time as time_module

    monkeypatch.setattr(perps_model, "HF_API_KEY", "fake-key")
    monkeypatch.setattr(perps_model, "_MODEL_DOWNLOAD_HF_TIMEOUT_SEC", 0.2)

    def hangs_forever(*a, **k):
        time_module.sleep(30)
        raise AssertionError("should never get here")

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", hangs_forever)

    start = time_module.monotonic()
    result = perps_model._download_model_from_hf()  # noqa: SLF001
    elapsed = time_module.monotonic() - start

    assert result is False
    assert elapsed < 5  # must return promptly, not wait out the full 30s hang
