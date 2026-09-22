"""Meta-labeling for kalshi_15m_model.py's own primary direction
classifier -- mirrors perps_meta_model.py's own structure and reasoning
in full (see its module docstring for the complete design rationale): a
secondary, deliberately simple classifier predicting whether the
PRIMARY model's prediction is trustworthy IN THE CURRENT CONTEXT (time
of day, volatility/liquidity regime, and the primary model's own
confidence) -- NOT a second opinion on direction, only on whether to
trust the first opinion.

Crypto only (BTC/ETH/SOL/XRP/DOGE) -- kalshi_15m_metals_model.py has its
own, much smaller, differently-shaped feature set (no atr_pct/
dollar_volume_z/oi_change_pct, the exact context columns this module
needs) and, per KALSHI_15M_TORCH_TRAIN_HOUR_ET's own comment, "hasn't
even completed one regular walk-forward training run yet" -- a smaller,
newer dataset isn't the place to add a second model layer before the
first one has real footing. Reuses perps_data.FEATURE_COLUMNS directly,
same as kalshi_15m_model.py's own primary training already does (this
market's crypto model proxies off perps' own data pipeline -- see
kalshi_15m_data.latest_feature_row's own docstring).

Trained on the primary model's own OUT-OF-FOLD walk-forward predictions,
one fresh fit per TimeSeriesSplit fold, so every "was this prediction
right" label comes from a model that never saw that row during ITS OWN
training. Deliberately does NOT call or modify kalshi_15m_model.train_model()
itself -- fully separate, additive, zero risk to the live-critical
primary training pipeline.

Off by default (KALSHI_15M_USE_META_MODEL, gated in kalshi_15m_strategy.py)
pending real backtest validation via kalshi_15m_backtest.py -- same
evidence-gated rollout discipline as perps' own identical USE_META_MODEL."""
from __future__ import annotations

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
from sklearn.frozen import FrozenEstimator
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.model_selection import TimeSeriesSplit

from data.kalshi_15m_data import load_training_dataset
from data.kalshi_15m_model import (
    KALSHI_15M_MODEL_RECENCY_HALFLIFE_DAYS,
    _CANDIDATES,
    _prepare_training_frame,
    _recency_sample_weight,
)
from data.perps_data import FEATURE_COLUMNS

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
META_MODEL_PATH = DATA_DIR / "kalshi_15m_meta_model.joblib"
META_MODEL_META_PATH = DATA_DIR / "kalshi_15m_meta_model_meta.json"

HF_API_KEY = os.getenv("HF_API_KEY", "")
# Same repo the primary crypto model already uses (kalshi_15m_model.py's
# own HF_KALSHI_15M_MODEL_REPO) -- one more file in the "this market's
# models" bucket, not a new dedicated repo.
HF_MODEL_REPO = os.getenv("HF_KALSHI_15M_MODEL_REPO", "papylove/kalshi-15m-model")

# Context features -- deliberately NOT the primary model's own full
# FEATURE_COLUMNS list (that would just re-litigate direction, which is
# not this model's job): a compact "regime + liquidity + time + how
# confident was the primary model" vector. Identical column NAMES to
# perps_meta_model.py's own META_CONTEXT_COLUMNS since both primary
# models share the exact same perps_data.FEATURE_COLUMNS schema.
META_CONTEXT_COLUMNS = [
    "volatility_30", "atr_pct", "bb_bandwidth",
    "dollar_volume_z", "oi_change_pct",
    "hour_sin", "hour_cos",
]
META_FEATURE_COLUMNS = META_CONTEXT_COLUMNS + ["primary_confidence"]

MIN_META_TRAIN_ROWS = int(os.getenv("KALSHI_15M_META_MODEL_MIN_TRAIN_ROWS", "300") or "300")
META_WALK_FORWARD_SPLITS = int(os.getenv("KALSHI_15M_META_MODEL_WALK_FORWARD_SPLITS", "4") or "4")
META_HOLDOUT_FRACTION = float(os.getenv("KALSHI_15M_META_MODEL_HOLDOUT_FRACTION", "0.2") or "0.2")

_meta_model_cache: dict[str, Any] = {"model": None, "meta": None, "loaded_at": 0.0}
META_MODEL_CACHE_TTL_SEC = int(os.getenv("KALSHI_15M_META_MODEL_CACHE_TTL_SEC", "1800") or "1800")
HF_META_MODEL_RECHECK_INTERVAL_SEC = int(os.getenv("KALSHI_15M_META_MODEL_HF_RECHECK_INTERVAL_SEC", "600") or "600")
_hf_meta_recheck_state: dict[str, Any] = {"checking": False, "last_checked_at": 0.0}
_hf_meta_recheck_lock = threading.Lock()


def _build_oof_dataset(df: pd.DataFrame, *, model_type: str) -> pd.DataFrame | None:
    """Runs its own fresh walk-forward pass (same TimeSeriesSplit shape as
    kalshi_15m_model.train_model, but entirely standalone) refitting
    `model_type` per fold, and returns one row per out-of-fold test
    sample: this row's own context features plus whether the primary
    model's call on it was actually correct."""
    if model_type not in _CANDIDATES:
        logger.warning("[kalshi_15m_meta_model] unknown primary model_type %r, skipping", model_type)
        return None
    labeled = _prepare_training_frame(df)
    if len(labeled) < MIN_META_TRAIN_ROWS:
        return None

    feature_cols = FEATURE_COLUMNS + ["symbol_code"]
    x_all = labeled[feature_cols].values
    y_all = labeled["label_up"].values
    ts_all = labeled["ts"].values

    tscv = TimeSeriesSplit(n_splits=META_WALK_FORWARD_SPLITS)
    oof_frames: list[pd.DataFrame] = []
    for train_idx, test_idx in tscv.split(x_all):
        x_tr, y_tr, ts_tr = x_all[train_idx], y_all[train_idx], ts_all[train_idx]
        x_te, y_te = x_all[test_idx], y_all[test_idx]
        if len(set(y_tr)) < 2:
            continue
        sample_weight = _recency_sample_weight(ts_tr, half_life_days=KALSHI_15M_MODEL_RECENCY_HALFLIFE_DAYS)
        try:
            model = _CANDIDATES[model_type]()
            model.fit(x_tr, y_tr, sample_weight=sample_weight)
            proba = model.predict_proba(x_te)[:, 1]
        except Exception as exc:
            logger.warning("[kalshi_15m_meta_model] fold fit failed: %s", exc)
            continue
        primary_pred = (proba >= 0.5).astype(int)
        was_correct = (primary_pred == y_te).astype(int)
        fold_frame = labeled.iloc[test_idx][META_CONTEXT_COLUMNS].copy()
        fold_frame["primary_confidence"] = np.abs(proba - 0.5) * 2.0
        fold_frame["was_correct"] = was_correct
        fold_frame["ts"] = ts_all[test_idx]
        oof_frames.append(fold_frame)

    if not oof_frames:
        return None
    return pd.concat(oof_frames, ignore_index=True).sort_values("ts").reset_index(drop=True)


def train_meta_model(df: pd.DataFrame | None = None, *, primary_model_type: str | None = None) -> dict[str, Any]:
    """Builds the out-of-fold dataset and fits a single calibrated
    LogisticRegression on it -- deliberately simple, same rationale as
    perps_meta_model.train_meta_model's own docstring. Never raises on
    ordinary "not enough data yet" conditions."""
    frame = df if df is not None else load_training_dataset()
    if frame.empty:
        return {"ok": False, "reason": "no_data"}

    if primary_model_type is None:
        from data.kalshi_15m_model import load_model as _load_primary_model
        _, primary_meta = _load_primary_model()
        primary_model_type = (primary_meta or {}).get("model_type")
        if primary_model_type == "ensemble":
            scores = (primary_meta or {}).get("scores") or {}
            candidates = {k: v.get("walk_forward_mean_score", -1.0) for k, v in scores.items()}
            primary_model_type = max(candidates, key=candidates.get) if candidates else None
    if not primary_model_type:
        return {"ok": False, "reason": "no_primary_model_type"}

    oof = _build_oof_dataset(frame, model_type=primary_model_type)
    del frame
    if oof is None or len(oof) < MIN_META_TRAIN_ROWS:
        return {"ok": False, "reason": "insufficient_oof_rows", "rows": 0 if oof is None else len(oof), "need": MIN_META_TRAIN_ROWS}

    split_at = int(len(oof) * (1.0 - META_HOLDOUT_FRACTION))
    train_part, holdout_part = oof.iloc[:split_at], oof.iloc[split_at:]
    if len(set(train_part["was_correct"])) < 2:
        return {"ok": False, "reason": "single_class_train_split"}

    x_train = train_part[META_FEATURE_COLUMNS].values
    y_train = train_part["was_correct"].values
    base_model = LogisticRegression(max_iter=1000, class_weight="balanced")
    base_model.fit(x_train, y_train)

    holdout_metrics: dict[str, float] | None = None
    if len(holdout_part) >= 20 and len(set(holdout_part["was_correct"])) >= 2:
        x_hold = holdout_part[META_FEATURE_COLUMNS].values
        y_hold = holdout_part["was_correct"].values
        calibrated = CalibratedClassifierCV(estimator=FrozenEstimator(base_model), method="sigmoid")
        calibrated.fit(x_hold, y_hold)
        hold_proba = calibrated.predict_proba(x_hold)[:, 1]
        holdout_metrics = {
            "accuracy": float(accuracy_score(y_hold, (hold_proba >= 0.5).astype(int))),
            "auc": float(roc_auc_score(y_hold, hold_proba)),
        }
        final_model = calibrated
        calibrated_flag = True
    else:
        x_all = oof[META_FEATURE_COLUMNS].values
        y_all = oof["was_correct"].values
        final_model = LogisticRegression(max_iter=1000, class_weight="balanced")
        final_model.fit(x_all, y_all)
        calibrated_flag = False

    meta = {
        "trained_at": time.time(),
        "primary_model_type": primary_model_type,
        "feature_columns": META_FEATURE_COLUMNS,
        "calibrated": calibrated_flag,
        "holdout_metrics": holdout_metrics,
        "oof_rows": int(len(oof)),
        "positive_rate": float(oof["was_correct"].mean()),
    }

    joblib.dump(final_model, META_MODEL_PATH)
    META_MODEL_META_PATH.write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
    _meta_model_cache.update({"model": final_model, "meta": meta, "loaded_at": time.time()})
    _push_meta_model_to_hf()
    return {"ok": True, **meta}


def _push_meta_model_to_hf() -> None:
    if not HF_API_KEY:
        return
    try:
        from huggingface_hub import HfApi
        from data.kalshi_15m_data import retry_on_rate_limit
        api = HfApi(token=HF_API_KEY)
        try:
            api.repo_info(repo_id=HF_MODEL_REPO, repo_type="model")
        except Exception:
            api.create_repo(repo_id=HF_MODEL_REPO, repo_type="model", exist_ok=True, private=False)
        retry_on_rate_limit(lambda: api.upload_file(
            path_or_fileobj=str(META_MODEL_PATH), path_in_repo="kalshi_15m_meta_model.joblib",
            repo_id=HF_MODEL_REPO, repo_type="model", commit_message="update kalshi 15m meta-model",
        ))
        retry_on_rate_limit(lambda: api.upload_file(
            path_or_fileobj=str(META_MODEL_META_PATH), path_in_repo="kalshi_15m_meta_model_meta.json",
            repo_id=HF_MODEL_REPO, repo_type="model", commit_message="update kalshi 15m meta-model metadata",
        ))
    except Exception as exc:
        logger.warning("[kalshi_15m_meta_model] HF push failed: %s", exc)


_META_MODEL_DOWNLOAD_HF_TIMEOUT_SEC = int(os.getenv("KALSHI_15M_META_MODEL_DOWNLOAD_HF_TIMEOUT_SEC", "15") or "15")


def _download_meta_model_from_hf() -> bool:
    if not HF_API_KEY:
        return False

    def _download() -> bool:
        from huggingface_hub import hf_hub_download
        model_path = hf_hub_download(repo_id=HF_MODEL_REPO, filename="kalshi_15m_meta_model.joblib", repo_type="model", token=HF_API_KEY)
        meta_path = hf_hub_download(repo_id=HF_MODEL_REPO, filename="kalshi_15m_meta_model_meta.json", repo_type="model", token=HF_API_KEY)
        META_MODEL_PATH.write_bytes(Path(model_path).read_bytes())
        META_MODEL_META_PATH.write_text(Path(meta_path).read_text(encoding="utf-8"), encoding="utf-8")
        return True

    try:
        from server_common import call_with_hard_timeout
        return bool(call_with_hard_timeout(_download, timeout_sec=_META_MODEL_DOWNLOAD_HF_TIMEOUT_SEC, on_timeout=False))
    except Exception as exc:
        logger.info("[kalshi_15m_meta_model] no meta-model available on HF yet: %s", exc)
        return False


def _schedule_hf_meta_model_recheck() -> None:
    from server_common import maybe_schedule_hf_model_recheck, refresh_model_if_hf_has_a_newer_one

    def _check() -> None:
        refresh_model_if_hf_has_a_newer_one(
            model_repo=HF_MODEL_REPO, token=HF_API_KEY, meta_filename="kalshi_15m_meta_model_meta.json",
            timeout_sec=_META_MODEL_DOWNLOAD_HF_TIMEOUT_SEC,
            current_trained_at=(_meta_model_cache["meta"] or {}).get("trained_at"),
            download_fn=_download_meta_model_from_hf,
            invalidate_fn=lambda: _meta_model_cache.update(loaded_at=0.0),
        )

    maybe_schedule_hf_model_recheck(
        refresh_state=_hf_meta_recheck_state, lock=_hf_meta_recheck_lock,
        recheck_interval_sec=HF_META_MODEL_RECHECK_INTERVAL_SEC, check_fn=_check,
    )


def load_meta_model() -> tuple[Any | None, dict[str, Any] | None]:
    """Same cache/download/staleness-recheck shape as kalshi_15m_model.load_model."""
    now = time.time()
    if _meta_model_cache["model"] is not None:
        _schedule_hf_meta_model_recheck()
        if (now - _meta_model_cache["loaded_at"]) < META_MODEL_CACHE_TTL_SEC:
            return _meta_model_cache["model"], _meta_model_cache["meta"]

    if not META_MODEL_PATH.exists() or not META_MODEL_META_PATH.exists():
        _download_meta_model_from_hf()
    if not META_MODEL_PATH.exists() or not META_MODEL_META_PATH.exists():
        return None, None

    try:
        model = joblib.load(META_MODEL_PATH)
        meta = json.loads(META_MODEL_META_PATH.read_text(encoding="utf-8"))
        _meta_model_cache.update({"model": model, "meta": meta, "loaded_at": now})
        return model, meta
    except Exception as exc:
        logger.warning("[kalshi_15m_meta_model] failed to load cached meta-model: %s", exc)
        return None, None


def trust_score(row: dict[str, Any] | None, *, primary_probability_up: float) -> float | None:
    """The meta-model's own predicted probability that the primary
    model's directional call on `row` (already made,
    `primary_probability_up` already computed by
    kalshi_15m_model.predict_direction) is CORRECT -- None if no meta-
    model is trained yet or `row` is missing the context features this
    needs. Never raises -- same best-effort contract as every other
    prediction path in this codebase."""
    if row is None:
        return None
    model, meta = load_meta_model()
    if model is None or meta is None:
        return None
    feature_cols = meta.get("feature_columns") or META_FEATURE_COLUMNS
    try:
        values = []
        for col in feature_cols:
            if col == "primary_confidence":
                values.append(abs(primary_probability_up - 0.5) * 2.0)
            else:
                values.append(float(row.get(col, 0.0)))
        x = np.array([values])
        return float(model.predict_proba(x)[0][1])
    except Exception as exc:
        logger.debug("[kalshi_15m_meta_model] trust_score failed: %s", exc)
        return None
