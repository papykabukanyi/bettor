"""Direction-classifier training + prediction for Kalshi's 15-minute
GOLD/SILVER/COPPER markets. Synthetic feature data only -- never touches
Kalshi, Hugging Face, or the gold-api.com price source. Mirrors
test_kalshi_15m_model.py's own coverage (itself mirroring
test_perps_model.py's, see its docstring) since kalshi_15m_metals_model.py
is a faithful adaptation of the same proven architecture -- the one real
difference exercised here is the LEANER feature set (see
kalshi_15m_metals_data.METALS_FEATURE_COLUMNS' own docstring for exactly
which crypto/perps indicators are dropped and why)."""
from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd
import pytest

from data import kalshi_15m_metals_model


@pytest.fixture(autouse=True)
def _isolated_model_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(kalshi_15m_metals_model, "MODEL_PATH", tmp_path / "model.joblib")
    monkeypatch.setattr(kalshi_15m_metals_model, "MODEL_META_PATH", tmp_path / "model_meta.json")
    monkeypatch.setattr(kalshi_15m_metals_model, "HF_API_KEY", "")
    kalshi_15m_metals_model._model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001
    kalshi_15m_metals_model._hf_recheck_state.update({"last_checked_at": 0.0, "checking": False})  # noqa: SLF001
    yield
    kalshi_15m_metals_model._model_cache.update({"model": None, "meta": None, "loaded_at": 0.0})  # noqa: SLF001
    kalshi_15m_metals_model._hf_recheck_state.update({"last_checked_at": 0.0, "checking": False})  # noqa: SLF001


def _synthetic_training_frame(n: int = 500, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dist = rng.normal(0, 0.01, n)
    label = (dist > 0).astype(int)
    return pd.DataFrame({
        "symbol": ["GOLD"] * n,
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
        "hour_sin": rng.uniform(-1, 1, n),
        "hour_cos": rng.uniform(-1, 1, n),
        "dow_sin": rng.uniform(-1, 1, n),
        "dow_cos": rng.uniform(-1, 1, n),
        "label_up": label,
    })


def test_synthetic_frame_matches_the_real_leaner_feature_set():
    """Confirms this test file's own fixture stays in sync with
    METALS_FEATURE_COLUMNS -- a real drift here would make every other
    test in this file pass for the wrong reason (training on a feature
    set that doesn't match production)."""
    from data.kalshi_15m_metals_data import METALS_FEATURE_COLUMNS
    df = _synthetic_training_frame(n=5)
    for col in METALS_FEATURE_COLUMNS:
        assert col in df.columns


def test_train_model_with_no_data_returns_not_ok():
    result = kalshi_15m_metals_model.train_model(df=pd.DataFrame())
    assert result["ok"] is False
    assert result["reason"] == "no_data"


def test_train_model_with_too_few_rows_returns_not_ok():
    small_df = _synthetic_training_frame(n=20)
    result = kalshi_15m_metals_model.train_model(df=small_df)
    assert result["ok"] is False
    assert result["reason"] == "insufficient_rows"


def test_train_model_succeeds_with_enough_signal_rows():
    df = _synthetic_training_frame(n=500)
    result = kalshi_15m_metals_model.train_model(df=df)
    assert result["ok"] is True
    assert result["rows"] > 0
    assert result["model_type"] in {"logistic_regression", "random_forest", "gradient_boosting"}
    assert kalshi_15m_metals_model.MODEL_PATH.exists()
    assert kalshi_15m_metals_model.MODEL_META_PATH.exists()


def test_predict_direction_reports_model_ok_false_without_a_trained_model():
    result = kalshi_15m_metals_model.predict_direction("GOLD")
    assert result["model_ok"] is False


def test_train_model_stays_uncalibrated_below_the_holdout_floor():
    df = _synthetic_training_frame(n=500)
    result = kalshi_15m_metals_model.train_model(df=df)
    assert result["ok"] is True
    assert result["calibrated"] is False
    assert result["ensemble_members"] is None
    assert result["model_type"] in {"logistic_regression", "random_forest", "gradient_boosting"}


# ---------------------------------------------------------------------------
# _trade_outcome_sample_weight -- mechanics-only coverage (matching,
# weighting, dry-run exclusion), same as perps_model's/kalshi_15m_model's
# own identical tests. A genuine no-op in production until this brand new
# market has real closed trades.
# ---------------------------------------------------------------------------
def _iso(epoch_seconds: int) -> str:
    return dt.datetime.fromtimestamp(epoch_seconds, tz=dt.timezone.utc).isoformat()


def test_trade_outcome_sample_weight_returns_all_ones_without_a_trade_log():
    symbols = np.array(["GOLD", "SILVER"])
    ts = np.array([0, 60])
    weights = kalshi_15m_metals_model._trade_outcome_sample_weight(symbols, ts, None)  # noqa: SLF001
    assert list(weights) == [1.0, 1.0]


def test_trade_outcome_sample_weight_upweights_a_matching_real_win():
    symbols = np.array(["GOLD", "SILVER"])
    ts = np.array([0, 6000])
    trade_log = [{"symbol": "GOLD", "opened_at": _iso(0), "realized_pnl_usd": 1.0, "dry_run": False}]
    weights = kalshi_15m_metals_model._trade_outcome_sample_weight(symbols, ts, trade_log)  # noqa: SLF001
    assert weights[0] == kalshi_15m_metals_model.KALSHI_15M_METALS_MODEL_TRADE_OUTCOME_WIN_WEIGHT
    assert weights[1] == 1.0


def test_trade_outcome_sample_weight_upweights_a_loss_more_than_a_win():
    symbols = np.array(["GOLD"])
    ts = np.array([0])
    win = kalshi_15m_metals_model._trade_outcome_sample_weight(  # noqa: SLF001
        symbols, ts, [{"symbol": "GOLD", "opened_at": _iso(0), "realized_pnl_usd": 1.0, "dry_run": False}],
    )
    loss = kalshi_15m_metals_model._trade_outcome_sample_weight(  # noqa: SLF001
        symbols, ts, [{"symbol": "GOLD", "opened_at": _iso(0), "realized_pnl_usd": -1.0, "dry_run": False}],
    )
    assert loss[0] > win[0]
    assert win[0] == kalshi_15m_metals_model.KALSHI_15M_METALS_MODEL_TRADE_OUTCOME_WIN_WEIGHT
    assert loss[0] == kalshi_15m_metals_model.KALSHI_15M_METALS_MODEL_TRADE_OUTCOME_LOSS_WEIGHT


def test_trade_outcome_sample_weight_ignores_dry_run_trades():
    symbols = np.array(["GOLD"])
    ts = np.array([0])
    trade_log = [{"symbol": "GOLD", "opened_at": _iso(0), "realized_pnl_usd": 1.0, "dry_run": True}]
    weights = kalshi_15m_metals_model._trade_outcome_sample_weight(symbols, ts, trade_log)  # noqa: SLF001
    assert weights[0] == 1.0


def test_trade_outcome_sample_weight_requires_matching_symbol_not_just_time():
    symbols = np.array(["SILVER"])
    ts = np.array([0])
    trade_log = [{"symbol": "GOLD", "opened_at": _iso(0), "realized_pnl_usd": 1.0, "dry_run": False}]
    weights = kalshi_15m_metals_model._trade_outcome_sample_weight(symbols, ts, trade_log)  # noqa: SLF001
    assert weights[0] == 1.0


def test_train_model_accepts_a_trade_log_and_reports_how_many_rows_matched():
    df = _synthetic_training_frame(n=500)
    trade_log = [{"symbol": "GOLD", "opened_at": _iso(0), "realized_pnl_usd": 1.0, "dry_run": False}]
    result = kalshi_15m_metals_model.train_model(df=df, trade_log=trade_log)
    assert result["ok"] is True
    assert result["trade_outcome_rows_matched"] > 0


def test_train_model_with_no_trade_log_matches_zero_rows():
    df = _synthetic_training_frame(n=500)
    result = kalshi_15m_metals_model.train_model(df=df, trade_log=None)
    assert result["ok"] is True
    assert result["trade_outcome_rows_matched"] == 0


def test_train_model_calibrates_above_the_holdout_floor():
    df = _synthetic_training_frame(n=3000)
    result = kalshi_15m_metals_model.train_model(df=df)
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
        assert all(isinstance(v, float) for v in importances.values())


def test_train_model_cv_detail_reflects_the_walk_forward_folds():
    df = _synthetic_training_frame(n=3000)
    result = kalshi_15m_metals_model.train_model(df=df)
    assert result["ok"] is True
    assert len(result["cv_detail"]) <= kalshi_15m_metals_model.WALK_FORWARD_SPLITS
    assert all("test_rows" in fold for fold in result["cv_detail"])
    for name in {"logistic_regression", "random_forest", "gradient_boosting"}:
        assert "walk_forward_mean_score" in result["scores"][name]


def _fake_feature_row(symbol: str) -> dict:
    return {
        "symbol": symbol, "current_price": 4379.0,
        "ret_1m": 0.0, "ret_3m": 0.0, "ret_5m": 0.0, "ret_10m": 0.0, "ret_15m": 0.0, "ret_30m": 0.0,
        "trend_1h": 0.0, "trend_2h": 0.0, "trend_3h": 0.0, "trend_4h": 0.0,
        "dist_to_ma_15": 0.03, "dist_to_ma_30": 0.015,
        "volatility_5": 0.001, "volatility_15": 0.001, "volatility_30": 0.001,
        "rsi_14": 0.5, "macd_hist_pct": 0.0, "bb_pct_b": 0.5, "bb_bandwidth": 0.01,
        "hour_sin": 0.0, "hour_cos": 1.0, "dow_sin": 0.0, "dow_cos": 1.0,
    }


def test_predict_direction_works_with_a_calibrated_model(monkeypatch):
    df = _synthetic_training_frame(n=3000)
    train_result = kalshi_15m_metals_model.train_model(df=df)
    assert train_result["ok"] is True
    assert train_result["calibrated"] is True

    monkeypatch.setattr(kalshi_15m_metals_model, "latest_feature_row", _fake_feature_row)
    result = kalshi_15m_metals_model.predict_direction("GOLD")
    assert result["model_ok"] is True
    assert result["direction"] in {"up", "down"}
    assert 0.0 <= result["probability_up"] <= 1.0
    assert result["current_price"] == 4379.0


def test_predict_direction_reports_no_feature_data_when_unavailable(monkeypatch):
    df = _synthetic_training_frame(n=500)
    kalshi_15m_metals_model.train_model(df=df)
    monkeypatch.setattr(kalshi_15m_metals_model, "latest_feature_row", lambda symbol: None)
    result = kalshi_15m_metals_model.predict_direction("GOLD")
    assert result["model_ok"] is False
    assert result["reason"] == "no_feature_data"


def test_predict_direction_handles_an_unknown_symbol_gracefully(monkeypatch):
    """A symbol never seen during training (symbol_code falls back to -1)
    must not crash prediction -- just a real, if lower-quality, guess."""
    df = _synthetic_training_frame(n=500)
    kalshi_15m_metals_model.train_model(df=df)
    monkeypatch.setattr(kalshi_15m_metals_model, "latest_feature_row", _fake_feature_row)
    result = kalshi_15m_metals_model.predict_direction("SILVER")  # not in training data (only "GOLD")
    assert result["model_ok"] is True
