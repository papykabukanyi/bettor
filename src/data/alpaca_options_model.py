"""Direction classifier for Alpaca options underlyings -- structurally
identical to perps_model.py's own walk-forward-CV + calibration +
evidence-based-ensembling design (ported here, not reinvented, once that
methodology was already proven live on perps): same chronological-
holdout-never-random-shuffle discipline, same candidate comparison, same
feature-importance surfacing. Trained on alpaca_options_data's
features/labels (the UNDERLYING's own direction) and persisted to
HF_ALPACA_OPTIONS_MODEL_REPO -- its own model, as explicitly requested,
even though the feature recipe matches the equities model exactly
(predicting whether a stock goes up or down is the same problem whether
the result is used to buy shares or buy an option on it).

"Train on all the dataset possible, learn patterns robustly" -- walk-
forward CV (4 sequential chronological folds, never a random shuffle)
picks a candidate based on how it performs across the WHOLE history in
sequence, not one lucky/unlucky single 80/20 split; recency weighting
lets the model favor the CURRENT market regime over stale history without
needing a narrower MAX_TRAIN_ROWS window; calibration turns probability_up
into a genuinely meaningful confidence instead of a raw sklearn score;
evidence-based ensembling ships an averaged blend of calibrated candidates
only when THIS retrain's own folds show it actually beats every individual
one, never as a fixed a-priori rule.
"""
from __future__ import annotations

import gc
import json
import logging
import os
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

from data.alpaca_options_data import FEATURE_COLUMNS, latest_feature_row, load_training_dataset

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MODEL_PATH = DATA_DIR / "alpaca_options_model.joblib"
MODEL_META_PATH = DATA_DIR / "alpaca_options_model_meta.json"

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_ALPACA_OPTIONS_MODEL_REPO = os.getenv("HF_ALPACA_OPTIONS_MODEL_REPO", "papylove/alpaca-options-model")

MIN_TRAIN_ROWS = int(os.getenv("ALPACA_OPTIONS_MIN_TRAIN_ROWS", "300") or "300")
MODEL_CACHE_TTL_SEC = int(os.getenv("ALPACA_OPTIONS_MODEL_CACHE_TTL_SEC", "1800") or "1800")

WALK_FORWARD_SPLITS = 4
ALPACA_OPTIONS_MODEL_RECENCY_HALFLIFE_DAYS = float(os.getenv("ALPACA_OPTIONS_MODEL_RECENCY_HALFLIFE_DAYS", "14") or "14")
# Calibration/ensembling both need a real held-out slice to fit against on
# top of the walk-forward split itself -- below this floor (e.g. right at
# the MIN_TRAIN_ROWS=300 cold-start edge), skip both and fall back to
# EXACTLY the old contract (single best walk-forward candidate, refit on
# 100% of rows, uncalibrated) rather than risk fitting a calibrator on too
# few rows to mean anything.
ALPACA_OPTIONS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS = int(os.getenv("ALPACA_OPTIONS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS", "200") or "200")

_model_cache: dict[str, Any] = {"model": None, "meta": None, "loaded_at": 0.0}

# n_estimators=60 (not perps' pre-OOM-fix 100, nor this file's own
# original 100): perps' walk-forward loop fits up to 4 folds x 3
# candidates (12 models total) in sequence -- a materially different
# memory profile from the OLD single-split shape that fit once. Perps hit
# two real container-level OOM kills discovering that the hard way this
# session; applying that lesson proactively here rather than waiting for
# options to relearn it via its own crash.
_CANDIDATES = {
    "logistic_regression": lambda: LogisticRegression(max_iter=1000, class_weight="balanced"),
    "random_forest": lambda: RandomForestClassifier(
        n_estimators=60, max_depth=6, min_samples_leaf=20, class_weight="balanced", random_state=42, n_jobs=1,
    ),
    "gradient_boosting": lambda: GradientBoostingClassifier(
        n_estimators=60, max_depth=3, learning_rate=0.05, random_state=42,
    ),
}


class _AveragedEnsemble:
    """Unweighted mean of predict_proba across 2-3 already-calibrated
    candidates -- module-level (not a closure) so joblib/pickle can resolve
    it by qualified import path across the train-here/load-there (HF
    download into a possibly different worker process) round trip. Shipped
    only when a real walk-forward comparison shows the average actually
    beats every individual candidate (see train_model()) -- not a fixed
    a-priori rule. Identical to perps_model.py's own copy of this class."""

    def __init__(self, models: list[Any], names: list[str]):
        self.models = models
        self.names = names

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        return np.mean([m.predict_proba(x) for m in self.models], axis=0)


def _recency_sample_weight(ts: np.ndarray, *, half_life_days: float) -> np.ndarray:
    """Exponential half-life decay relative to THIS SLICE's own max
    timestamp -- not the global dataset's max, so an early walk-forward
    fold doesn't get penalized against a "now" that's actually in its own
    future. Combines multiplicatively with class_weight="balanced" via
    sklearn's own sample_weight handling."""
    if half_life_days <= 0 or len(ts) == 0:
        return np.ones(len(ts), dtype=float)
    age_days = (ts.max() - ts) / 86400.0
    return np.power(0.5, age_days / half_life_days)


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


def train_model(df: pd.DataFrame | None = None) -> dict[str, Any]:
    """Train, compare candidates via walk-forward (chronological, never
    randomly-shuffled) cross-validation, keep the best -- calibrated, and
    ensembled if the evidence from THIS retrain's own folds actually
    supports it -- persist locally + to HF. Returns a summary dict either
    way (never raises on ordinary "not enough data yet" conditions --
    that's expected during the first days of data collection). Ported
    directly from perps_model.py's own already-proven-live version of
    this function -- see that module's docstring for the real profiling
    numbers (walk-forward costs ~2x wall time / +3.5% peak RSS vs the old
    single-split version, trivial against this container's 512MB ceiling
    and nowhere near this daily-off-hours-interval job's own stale-lock
    ceiling)."""
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
            # Can't score AUC meaningfully on a single-class test fold --
            # skip this ONE fold, don't abort the whole retrain over it.
            continue
        sample_weight = _recency_sample_weight(ts_tr, half_life_days=ALPACA_OPTIONS_MODEL_RECENCY_HALFLIFE_DAYS)

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
                logger.warning("[alpaca_options_model] candidate %s failed on fold %d: %s", name, fold_idx, exc)

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
        and len(last_test_idx) >= ALPACA_OPTIONS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS
        and len(set(y_last_test)) >= 2
    )

    feature_importances: dict[str, dict[str, float]] = {}
    ensemble_members: list[str] | None = None

    if can_calibrate:
        # Fold-3's train slice (~80% in steady state, identical to the old
        # single 80/20 split) already produced these fits during the loop
        # above -- calibrating here fits ONLY the sigmoid mapping on
        # fold-3's own test slice, no new base-model fit needed.
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
        # Thin-data fallback: EXACTLY the old contract -- refit the single
        # best walk-forward candidate fresh on 100% of rows, uncalibrated.
        # Never regresses cold-start behavior below the calibration floor.
        best_model = _CANDIDATES[best_name]()
        full_sample_weight = _recency_sample_weight(ts_all, half_life_days=ALPACA_OPTIONS_MODEL_RECENCY_HALFLIFE_DAYS)
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
        "recency_halflife_days": ALPACA_OPTIONS_MODEL_RECENCY_HALFLIFE_DAYS,
        "calibration_min_holdout_rows": ALPACA_OPTIONS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS,
        "feature_columns": feature_cols,
        "symbol_categories": symbol_categories,
    }

    joblib.dump(best_model, MODEL_PATH)
    MODEL_META_PATH.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    _model_cache.update({"model": best_model, "meta": meta, "loaded_at": time.time()})

    _push_model_to_hf()
    # Same real OOM-mitigation discipline proven necessary across every
    # other train_model() here -- the walk-forward loop above fits up to
    # 4 folds x 3 candidates (12 models total) in sequence, most going
    # out of scope naturally, but an explicit collect() helps CPython
    # consolidate/reuse that churn's freed memory sooner.
    gc.collect()
    return {"ok": True, **meta}


def _push_model_to_hf() -> None:
    if not HF_API_KEY:
        return
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
        try:
            api.repo_info(repo_id=HF_ALPACA_OPTIONS_MODEL_REPO, repo_type="model")
        except Exception:
            api.create_repo(repo_id=HF_ALPACA_OPTIONS_MODEL_REPO, repo_type="model", exist_ok=True, private=False)
        api.upload_file(
            path_or_fileobj=str(MODEL_PATH), path_in_repo="alpaca_options_model.joblib",
            repo_id=HF_ALPACA_OPTIONS_MODEL_REPO, repo_type="model", commit_message="update alpaca options model",
        )
        api.upload_file(
            path_or_fileobj=str(MODEL_META_PATH), path_in_repo="alpaca_options_model_meta.json",
            repo_id=HF_ALPACA_OPTIONS_MODEL_REPO, repo_type="model", commit_message="update alpaca options model metadata",
        )
    except Exception as exc:
        logger.warning("[alpaca_options_model] HF model push failed: %s", exc)


_MODEL_DOWNLOAD_HF_TIMEOUT_SEC = int(os.getenv("ALPACA_OPTIONS_MODEL_DOWNLOAD_HF_TIMEOUT_SEC", "15") or "15")


def _download_model_from_hf() -> bool:
    if not HF_API_KEY:
        return False

    def _download() -> bool:
        from huggingface_hub import hf_hub_download
        model_path = hf_hub_download(
            repo_id=HF_ALPACA_OPTIONS_MODEL_REPO, filename="alpaca_options_model.joblib", repo_type="model", token=HF_API_KEY,
        )
        meta_path = hf_hub_download(
            repo_id=HF_ALPACA_OPTIONS_MODEL_REPO, filename="alpaca_options_model_meta.json", repo_type="model", token=HF_API_KEY,
        )
        MODEL_PATH.write_bytes(Path(model_path).read_bytes())
        MODEL_META_PATH.write_text(Path(meta_path).read_text(encoding="utf-8"), encoding="utf-8")
        return True

    try:
        # Real, confirmed production incident (same call shape, on perps):
        # called directly from /api/alpaca/options/status on every cold
        # boot, unbounded this can hang on huggingface_hub's own internal
        # session lock long enough to freeze this --workers 1 process
        # until gunicorn's worker timeout SIGKILLs it -- which wipes local
        # disk and guarantees the next boot hits this same unconditional
        # call again: a self-sustaining crash loop.
        from server_common import call_with_hard_timeout
        return bool(call_with_hard_timeout(_download, timeout_sec=_MODEL_DOWNLOAD_HF_TIMEOUT_SEC, on_timeout=False))
    except Exception as exc:
        logger.info("[alpaca_options_model] no model available on HF yet: %s", exc)
        return False


def load_model() -> tuple[Any | None, dict[str, Any] | None]:
    now = time.time()
    if _model_cache["model"] is not None and (now - _model_cache["loaded_at"]) < MODEL_CACHE_TTL_SEC:
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
        logger.warning("[alpaca_options_model] failed to load cached model: %s", exc)
        return None, None


def predict_direction(symbol: str) -> dict[str, Any]:
    """{"model_ok": False} if no trained model exists yet (expected during
    the first days of data collection -- callers fall back to
    technical-only signals in that case). Unchanged by the walk-forward/
    calibration/ensembling upgrade above -- every possible shipped
    artifact (plain model / CalibratedClassifierCV / _AveragedEnsemble)
    exposes .predict_proba(x) the same way."""
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

    return {
        "model_ok": True, "symbol": symbol, "probability_up": proba_up,
        "direction": "up" if proba_up >= 0.5 else "down",
        "current_price": row["current_price"], "short_ma": row["short_ma"],
        "model_type": meta.get("model_type"), "trained_at": meta.get("trained_at"),
    }
