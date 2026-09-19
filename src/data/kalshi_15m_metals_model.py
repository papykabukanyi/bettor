"""Direction classifier for Kalshi's 15-minute GOLD/SILVER/COPPER markets:
given current technical features for one of the 3 traded metals, predict
whether its price will be higher or lower
kalshi_15m_metals_data.LABEL_HORIZON_MINUTES (15) minutes from now --
matching what these markets actually settle on (see kalshi_15m.py's own
module docstring). No news sentiment here (no metals-news source exists
in this codebase, unlike crypto's), and a leaner feature set overall --
see kalshi_15m_metals_data.METALS_FEATURE_COLUMNS' own module docstring
for exactly which crypto/perps indicators are dropped and why (all need
volume/OI/bid-ask/high-low, none of which this market's plain spot-price
source has).

Faithfully mirrors perps_model.py's own proven architecture (walk-forward
CV across 3 candidates, sigmoid calibration, ensembling only when a real
fold-level comparison supports it, recency + trade-outcome sample
weighting) rather than a simpler one-off -- see perps_model.train_model's
own docstring for the full design rationale behind each piece, all
identical here except "ticker" -> "symbol" (this module's own column
name, see kalshi_15m_metals_data.py) and this module's own HF_MODEL_REPO/
MODEL_PATH. Same "independent per-market module" convention as every
other *_model.py here.
"""
from __future__ import annotations

import datetime as dt
import gc
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.frozen import FrozenEstimator
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.model_selection import TimeSeriesSplit

from data.kalshi_15m_metals_data import LABEL_HORIZON_MINUTES, METALS_FEATURE_COLUMNS as FEATURE_COLUMNS, latest_feature_row, load_training_dataset

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MODEL_PATH = DATA_DIR / "kalshi_15m_metals_model.joblib"
MODEL_META_PATH = DATA_DIR / "kalshi_15m_metals_model_meta.json"

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_KALSHI_15M_METALS_MODEL_REPO = os.getenv("HF_KALSHI_15M_METALS_MODEL_REPO", "papylove/kalshi-15m-metals-model")

MIN_TRAIN_ROWS = int(os.getenv("KALSHI_15M_METALS_MIN_TRAIN_ROWS", "300") or "300")
MODEL_CACHE_TTL_SEC = int(os.getenv("KALSHI_15M_METALS_MODEL_CACHE_TTL_SEC", "1800") or "1800")

WALK_FORWARD_SPLITS = 4
KALSHI_15M_METALS_MODEL_RECENCY_HALFLIFE_DAYS = float(os.getenv("KALSHI_15M_METALS_MODEL_RECENCY_HALFLIFE_DAYS", "14") or "14")
KALSHI_15M_METALS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS = int(os.getenv("KALSHI_15M_METALS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS", "200") or "200")

_model_cache: dict[str, Any] = {"model": None, "meta": None, "loaded_at": 0.0}

HF_MODEL_RECHECK_INTERVAL_SEC = int(os.getenv("KALSHI_15M_METALS_MODEL_HF_RECHECK_INTERVAL_SEC", "600") or "600")
_hf_recheck_state: dict[str, Any] = {"last_checked_at": 0.0, "checking": False}
_hf_recheck_lock = threading.Lock()

_CANDIDATES = {
    "logistic_regression": lambda: LogisticRegression(max_iter=1000, class_weight="balanced"),
    "random_forest": lambda: RandomForestClassifier(
        n_estimators=150, max_depth=6, min_samples_leaf=20, class_weight="balanced", random_state=42, n_jobs=4,
    ),
    "gradient_boosting": lambda: GradientBoostingClassifier(
        n_estimators=150, max_depth=3, learning_rate=0.05, random_state=42,
    ),
}


class _AveragedEnsemble:
    """Unweighted mean of predict_proba across 2-3 already-calibrated
    candidates -- see perps_model._AveragedEnsemble's own docstring for
    why this is module-level (joblib/pickle path resolution across the
    train-here/load-there round trip) and why predict() exists alongside
    predict_proba() (a real bug found on this exact class elsewhere)."""

    def __init__(self, models: list[Any], names: list[str]):
        self.models = models
        self.names = names

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        return np.mean([m.predict_proba(x) for m in self.models], axis=0)

    def predict(self, x: np.ndarray) -> np.ndarray:
        return (self.predict_proba(x)[:, 1] >= 0.5).astype(int)


def _recency_sample_weight(ts: np.ndarray, *, half_life_days: float) -> np.ndarray:
    if half_life_days <= 0 or len(ts) == 0:
        return np.ones(len(ts), dtype=float)
    age_days = (ts.max() - ts) / 86400.0
    return np.power(0.5, age_days / half_life_days)


KALSHI_15M_METALS_MODEL_TRADE_OUTCOME_WIN_WEIGHT = float(os.getenv("KALSHI_15M_METALS_MODEL_TRADE_OUTCOME_WIN_WEIGHT", "1.3") or "1.3")
KALSHI_15M_METALS_MODEL_TRADE_OUTCOME_LOSS_WEIGHT = float(os.getenv("KALSHI_15M_METALS_MODEL_TRADE_OUTCOME_LOSS_WEIGHT", "1.8") or "1.8")


def _trade_outcome_sample_weight(symbols: np.ndarray, ts: np.ndarray, trade_log: list[dict[str, Any]] | None) -> np.ndarray:
    """See perps_model._trade_outcome_sample_weight's own docstring for
    the full rationale -- identical here, keyed on `symbol` (this coin,
    e.g. "BTC") instead of `ticker`. A genuine no-op until this market has
    real closed trades (trade_log empty/None), which is expected/correct
    during the first days after this strategy goes live."""
    weights = np.ones(len(symbols), dtype=float)
    if not trade_log:
        return weights
    outcome_by_key: dict[tuple[str, int], bool] = {}
    for t in trade_log:
        if t.get("dry_run") or not t.get("opened_at") or not t.get("symbol"):
            continue
        try:
            opened_ts = int(dt.datetime.fromisoformat(t["opened_at"]).timestamp())
        except Exception:
            continue
        minute_ts = (opened_ts // 60) * 60
        outcome_by_key[(t["symbol"], minute_ts)] = float(t.get("realized_pnl_usd") or 0.0) > 0
    if not outcome_by_key:
        return weights

    minute_ts_values = (ts.astype(np.int64) // 60) * 60
    for i in range(len(symbols)):
        won = outcome_by_key.get((symbols[i], int(minute_ts_values[i])))
        if won is not None:
            weights[i] *= KALSHI_15M_METALS_MODEL_TRADE_OUTCOME_WIN_WEIGHT if won else KALSHI_15M_METALS_MODEL_TRADE_OUTCOME_LOSS_WEIGHT
    return weights


def _feature_importance_map(model: Any, feature_cols: list[str]) -> dict[str, float] | None:
    try:
        if hasattr(model, "feature_importances_"):
            return dict(zip(feature_cols, model.feature_importances_.tolist()))
        if hasattr(model, "coef_"):
            return dict(zip(feature_cols, model.coef_[0].tolist()))
    except Exception:
        pass
    return None


def _prepare_training_frame(df: pd.DataFrame) -> pd.DataFrame:
    labeled = df.dropna(subset=["label_up"] + FEATURE_COLUMNS).copy()
    labeled["label_up"] = labeled["label_up"].astype(int)
    labeled["symbol_code"] = labeled["symbol"].astype("category").cat.codes
    return labeled.sort_values("ts").reset_index(drop=True)


def train_model(df: pd.DataFrame | None = None, trade_log: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """See perps_model.train_model's own docstring for the full design
    rationale -- identical walk-forward/calibration/ensembling logic,
    this module's own data/HF repo. Never raises on ordinary "not enough
    data yet" conditions -- expected for weeks after this market's own
    data collection first goes live."""
    frame = df if df is not None else load_training_dataset()
    if frame.empty:
        return {"ok": False, "reason": "no_data"}

    labeled = _prepare_training_frame(frame)
    del frame
    if len(labeled) < MIN_TRAIN_ROWS:
        return {"ok": False, "reason": "insufficient_rows", "rows": len(labeled), "need": MIN_TRAIN_ROWS}

    feature_cols = FEATURE_COLUMNS + ["symbol_code"]
    n_rows = len(labeled)
    x_all = labeled[feature_cols].values
    y_all = labeled["label_up"].values
    ts_all = labeled["ts"].values
    oldest_row_age_days = float((ts_all.max() - ts_all.min()) / 86400.0) if n_rows else 0.0
    symbol_categories = list(labeled["symbol"].astype("category").cat.categories)
    outcome_weight_all = _trade_outcome_sample_weight(labeled["symbol"].values, ts_all, trade_log)
    del labeled

    tscv = TimeSeriesSplit(n_splits=WALK_FORWARD_SPLITS)
    splits = list(tscv.split(x_all))
    fold_scores: dict[str, list[float]] = {name: [] for name in _CANDIDATES}
    ensemble_fold_scores: list[float] = []
    cv_detail: list[dict[str, Any]] = []
    last_fold_models: dict[str, Any] = {}

    for fold_idx, (train_idx, test_idx) in enumerate(splits):
        x_tr, y_tr, ts_tr = x_all[train_idx], y_all[train_idx], ts_all[train_idx]
        x_te, y_te = x_all[test_idx], y_all[test_idx]
        if len(set(y_te)) < 2:
            continue
        sample_weight = _recency_sample_weight(ts_tr, half_life_days=KALSHI_15M_METALS_MODEL_RECENCY_HALFLIFE_DAYS)
        sample_weight = sample_weight * outcome_weight_all[train_idx]

        fold_probas: list[np.ndarray] = []
        fold_models: dict[str, Any] = {}
        fold_result = {"fold": fold_idx, "test_rows": int(len(test_idx))}
        for name, factory in _CANDIDATES.items():
            try:
                model = factory()
                model.fit(x_tr, y_tr, sample_weight=sample_weight)
                preds = model.predict(x_te)
                proba = model.predict_proba(x_te)[:, 1]
                acc = float(accuracy_score(y_te, preds))
                auc = float(roc_auc_score(y_te, proba))
                combined = (acc + auc) / 2.0
                fold_scores[name].append(combined)
                fold_result[name] = combined
                fold_models[name] = model
                fold_probas.append(proba)
            except Exception as exc:
                logger.warning("[kalshi_15m_metals_model] candidate %s failed on fold %d: %s", name, fold_idx, exc)

        if len(fold_probas) >= 2:
            ensemble_proba = np.mean(fold_probas, axis=0)
            ensemble_preds = (ensemble_proba >= 0.5).astype(int)
            ensemble_score = (
                accuracy_score(y_te, ensemble_preds) + roc_auc_score(y_te, ensemble_proba)
            ) / 2.0
            ensemble_fold_scores.append(float(ensemble_score))
            fold_result["ensemble"] = float(ensemble_score)

        cv_detail.append(fold_result)
        if fold_idx == len(splits) - 1:
            last_fold_models = fold_models

    mean_scores = {name: (sum(s) / len(s) if s else -1.0) for name, s in fold_scores.items()}
    best_name = max(mean_scores, key=mean_scores.get)
    if mean_scores[best_name] < 0:
        return {"ok": False, "reason": "all_candidates_failed"}

    mean_ensemble_score = sum(ensemble_fold_scores) / len(ensemble_fold_scores) if ensemble_fold_scores else None
    use_ensemble = mean_ensemble_score is not None and mean_ensemble_score > mean_scores[best_name] and len(last_fold_models) >= 2

    last_train_idx, last_test_idx = splits[-1]
    x_last_test, y_last_test = x_all[last_test_idx], y_all[last_test_idx]
    can_calibrate = (
        bool(last_fold_models)
        and len(last_test_idx) >= KALSHI_15M_METALS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS
        and len(set(y_last_test)) >= 2
    )

    feature_importances: dict[str, dict[str, float]] = {}
    ensemble_members: list[str] | None = None

    if can_calibrate:
        calibrated_models: dict[str, Any] = {}
        for name, model in last_fold_models.items():
            importance = _feature_importance_map(model, feature_cols)
            if importance is not None:
                feature_importances[name] = importance
            calibrated = CalibratedClassifierCV(estimator=FrozenEstimator(model), method="sigmoid")
            calibrated.fit(x_last_test, y_last_test)
            calibrated_models[name] = calibrated

        if use_ensemble:
            best_model = _AveragedEnsemble(list(calibrated_models.values()), list(calibrated_models.keys()))
            model_type = "ensemble"
            ensemble_members = list(calibrated_models.keys())
        else:
            best_model = calibrated_models[best_name]
            model_type = best_name
        calibrated_flag = True
    else:
        best_model = _CANDIDATES[best_name]()
        full_sample_weight = _recency_sample_weight(ts_all, half_life_days=KALSHI_15M_METALS_MODEL_RECENCY_HALFLIFE_DAYS)
        full_sample_weight = full_sample_weight * outcome_weight_all
        best_model.fit(x_all, y_all, sample_weight=full_sample_weight)
        model_type = best_name
        calibrated_flag = False
        importance = _feature_importance_map(best_model, feature_cols)
        if importance is not None:
            feature_importances[best_name] = importance

    meta = {
        "trained_at": time.time(),
        "model_type": model_type,
        "calibrated": calibrated_flag,
        "ensemble_members": ensemble_members,
        "scores": {name: {"walk_forward_mean_score": mean_scores[name]} for name in _CANDIDATES},
        "mean_ensemble_score": mean_ensemble_score,
        "cv_detail": cv_detail,
        "feature_importances": feature_importances,
        "rows": n_rows,
        "oldest_row_age_days": round(oldest_row_age_days, 2),
        "recency_halflife_days": KALSHI_15M_METALS_MODEL_RECENCY_HALFLIFE_DAYS,
        "calibration_min_holdout_rows": KALSHI_15M_METALS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS,
        "feature_columns": feature_cols,
        "symbol_categories": symbol_categories,
        "label_horizon_minutes": LABEL_HORIZON_MINUTES,
        "trade_outcome_rows_matched": int(np.sum(outcome_weight_all != 1.0)),
    }

    joblib.dump(best_model, MODEL_PATH)
    MODEL_META_PATH.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    _model_cache.update({"model": best_model, "meta": meta, "loaded_at": time.time()})

    _push_model_to_hf()
    gc.collect()
    return {"ok": True, **meta}


def _push_model_to_hf() -> None:
    if not HF_API_KEY:
        return
    try:
        from huggingface_hub import HfApi
        from data.kalshi_15m_metals_data import retry_on_rate_limit
        api = HfApi(token=HF_API_KEY)
        try:
            api.repo_info(repo_id=HF_KALSHI_15M_METALS_MODEL_REPO, repo_type="model")
        except Exception:
            api.create_repo(repo_id=HF_KALSHI_15M_METALS_MODEL_REPO, repo_type="model", exist_ok=True, private=False)
        retry_on_rate_limit(lambda: api.upload_file(
            path_or_fileobj=str(MODEL_PATH), path_in_repo="kalshi_15m_metals_model.joblib",
            repo_id=HF_KALSHI_15M_METALS_MODEL_REPO, repo_type="model", commit_message="update kalshi 15m direction model",
        ))
        retry_on_rate_limit(lambda: api.upload_file(
            path_or_fileobj=str(MODEL_META_PATH), path_in_repo="kalshi_15m_metals_model_meta.json",
            repo_id=HF_KALSHI_15M_METALS_MODEL_REPO, repo_type="model", commit_message="update kalshi 15m model metadata",
        ))
    except Exception as exc:
        logger.warning("[kalshi_15m_metals_model] HF model push failed: %s", exc)


_MODEL_DOWNLOAD_HF_TIMEOUT_SEC = int(os.getenv("KALSHI_15M_METALS_MODEL_DOWNLOAD_HF_TIMEOUT_SEC", "15") or "15")


def _download_model_from_hf() -> bool:
    if not HF_API_KEY:
        return False

    def _download() -> bool:
        from huggingface_hub import hf_hub_download
        model_path = hf_hub_download(
            repo_id=HF_KALSHI_15M_METALS_MODEL_REPO, filename="kalshi_15m_metals_model.joblib", repo_type="model", token=HF_API_KEY,
        )
        meta_path = hf_hub_download(
            repo_id=HF_KALSHI_15M_METALS_MODEL_REPO, filename="kalshi_15m_metals_model_meta.json", repo_type="model", token=HF_API_KEY,
        )
        MODEL_PATH.write_bytes(Path(model_path).read_bytes())
        MODEL_META_PATH.write_text(Path(meta_path).read_text(encoding="utf-8"), encoding="utf-8")
        return True

    try:
        from server_common import call_with_hard_timeout
        return bool(call_with_hard_timeout(_download, timeout_sec=_MODEL_DOWNLOAD_HF_TIMEOUT_SEC, on_timeout=False))
    except Exception as exc:
        logger.info("[kalshi_15m_metals_model] no model available on HF yet: %s", exc)
        return False


def _schedule_hf_model_recheck() -> None:
    from server_common import maybe_schedule_hf_model_recheck, refresh_model_if_hf_has_a_newer_one

    def _check() -> None:
        refresh_model_if_hf_has_a_newer_one(
            model_repo=HF_KALSHI_15M_METALS_MODEL_REPO, token=HF_API_KEY, meta_filename="kalshi_15m_metals_model_meta.json",
            timeout_sec=_MODEL_DOWNLOAD_HF_TIMEOUT_SEC,
            current_trained_at=(_model_cache["meta"] or {}).get("trained_at"),
            download_fn=_download_model_from_hf,
            invalidate_fn=lambda: _model_cache.update(loaded_at=0.0),
        )

    maybe_schedule_hf_model_recheck(
        refresh_state=_hf_recheck_state, lock=_hf_recheck_lock,
        recheck_interval_sec=HF_MODEL_RECHECK_INTERVAL_SEC, check_fn=_check,
    )


def load_model() -> tuple[Any | None, dict[str, Any] | None]:
    now = time.time()
    if _model_cache["model"] is not None:
        _schedule_hf_model_recheck()
        if (now - _model_cache["loaded_at"]) < MODEL_CACHE_TTL_SEC:
            return _model_cache["model"], _model_cache["meta"]

    if not MODEL_PATH.exists() or not MODEL_META_PATH.exists():
        _download_model_from_hf()

    if not MODEL_PATH.exists() or not MODEL_META_PATH.exists():
        return None, None

    try:
        model = joblib.load(MODEL_PATH)
        meta = json.loads(MODEL_META_PATH.read_text(encoding="utf-8"))
        _model_cache.update({"model": model, "meta": meta, "loaded_at": now})
        return model, meta
    except Exception as exc:
        logger.warning("[kalshi_15m_metals_model] failed to load cached model: %s", exc)
        return None, None


def predict_direction(symbol: str) -> dict[str, Any]:
    """{"model_ok": False} if no trained model exists yet (expected for
    weeks after this market's own data collection first goes live --
    callers fall back to a technical-only or no-trade posture in that
    case). Otherwise returns direction + probability_up + the raw feature
    row used, for observability."""
    model, meta = load_model()
    if model is None or meta is None:
        return {"model_ok": False, "symbol": symbol}

    row = latest_feature_row(symbol)
    if row is None:
        return {"model_ok": False, "symbol": symbol, "reason": "no_feature_data"}

    categories = meta.get("symbol_categories") or []
    symbol_code = float(categories.index(symbol)) if symbol in categories else -1.0
    feature_cols = meta.get("feature_columns") or (FEATURE_COLUMNS + ["symbol_code"])
    x = np.array([[row.get(col, symbol_code if col == "symbol_code" else 0.0) for col in feature_cols]])

    try:
        proba_up = float(model.predict_proba(x)[0][1])
    except Exception as exc:
        return {"model_ok": False, "symbol": symbol, "reason": f"predict_failed: {exc}"}

    direction = "up" if proba_up >= 0.5 else "down"
    return {
        "model_ok": True, "symbol": symbol, "direction": direction, "probability_up": proba_up,
        "current_price": row.get("current_price"), "model_type": meta.get("model_type"),
    }
