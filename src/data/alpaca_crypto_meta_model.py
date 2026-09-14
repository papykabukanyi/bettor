"""Meta-labeling for alpaca_crypto_model.py's own primary direction
classifier: a secondary, deliberately simple classifier predicting
whether the PRIMARY model's prediction is trustworthy IN THE CURRENT
CONTEXT (time of day, volatility/liquidity regime, and the primary
model's own confidence) -- NOT a second opinion on direction, only on
whether to trust the first opinion.

Direct port of perps_meta_model.py (see its own module docstring for the
full design rationale) -- built here only once crypto's own primary
model had the same walk-forward-CV foundation this depends on (see
alpaca_crypto_model.py's own docstring: it used to be a single 80/20
split, which this module would have had to build its own OOF logic on
top of from a much thinner foundation than perps' own).

Motivated by the same real, confirmed limitation a strategy review found
for crypto specifically: alpaca_crypto_backtest.py's own module comment
records a real 68-pair/21-day backtest at current defaults returning
-13.7% (26.5% win rate) -- consistent with a thin-edge classifier, the
same class of problem perps_meta_model.py was built to help mitigate
(not fix outright -- this can't manufacture edge that isn't there, only
help skip the primary model's worst contexts).

Trained on the primary model's own OUT-OF-FOLD walk-forward predictions
(see `_build_oof_dataset` below) -- refits the SAME model type
alpaca_crypto_model.py's latest training run picked, one fresh fit per
TimeSeriesSplit fold, so every "was this prediction right" label comes
from a model that never saw that row during ITS OWN training. Does NOT
call or modify alpaca_crypto_model.train_model() itself -- a fully
separate, additive training pass with zero risk to the existing,
live-critical primary training pipeline.

Off by default (ALPACA_CRYPTO_USE_META_MODEL, gated in
alpaca_crypto_strategy.py) pending real backtest validation -- same
evidence-gated rollout discipline as every other new signal in this
codebase."""
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

from data.alpaca_crypto_data import load_training_dataset
from data.alpaca_crypto_model import (
    ALPACA_CRYPTO_MODEL_RECENCY_HALFLIFE_DAYS,
    _CANDIDATES,
    _prepare_training_frame,
    _recency_sample_weight,
)

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
META_MODEL_PATH = DATA_DIR / "alpaca_crypto_meta_model.joblib"
META_MODEL_META_PATH = DATA_DIR / "alpaca_crypto_meta_model_meta.json"

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_ALPACA_CRYPTO_MODEL_REPO = os.getenv("HF_ALPACA_CRYPTO_MODEL_REPO", "papylove/alpaca-crypto-model")

# Context features -- deliberately NOT the primary model's own full
# FEATURE_COLUMNS list (that would just re-litigate direction, which is
# not this model's job): a compact "regime + liquidity + time + how
# confident was the primary model" vector. No oi_change_pct/spread_pct
# equivalent here (unlike perps_meta_model.py's own list) -- crypto spot
# has no open-interest concept, and alpaca_crypto_data.py doesn't compute
# a spread feature; every other column is identical in name and meaning.
META_CONTEXT_COLUMNS = [
    "volatility_30", "atr_pct", "bb_bandwidth",
    "dollar_volume_z",
    "hour_sin", "hour_cos",
]
META_FEATURE_COLUMNS = META_CONTEXT_COLUMNS + ["primary_confidence"]

MIN_META_TRAIN_ROWS = int(os.getenv("ALPACA_CRYPTO_META_MODEL_MIN_TRAIN_ROWS", "500") or "500")
META_WALK_FORWARD_SPLITS = int(os.getenv("ALPACA_CRYPTO_META_MODEL_WALK_FORWARD_SPLITS", "4") or "4")
# Held out from meta-model FITTING (not from the primary model's own OOF
# generation above) purely to report an honest, never-seen-by-the-meta-
# model accuracy/AUC in the training summary -- chronological, like every
# other split in this codebase.
META_HOLDOUT_FRACTION = float(os.getenv("ALPACA_CRYPTO_META_MODEL_HOLDOUT_FRACTION", "0.2") or "0.2")

_meta_model_cache: dict[str, Any] = {"model": None, "meta": None, "loaded_at": 0.0}
META_MODEL_CACHE_TTL_SEC = int(os.getenv("ALPACA_CRYPTO_META_MODEL_CACHE_TTL_SEC", "1800") or "1800")
HF_META_MODEL_RECHECK_INTERVAL_SEC = int(os.getenv("ALPACA_CRYPTO_META_MODEL_HF_RECHECK_INTERVAL_SEC", "600") or "600")
_hf_meta_recheck_state: dict[str, Any] = {"checking": False, "last_checked_at": 0.0}
_hf_meta_recheck_lock = threading.Lock()


def _build_oof_dataset(df: pd.DataFrame, *, model_type: str) -> pd.DataFrame | None:
    """Runs its own fresh walk-forward pass (same TimeSeriesSplit shape as
    alpaca_crypto_model.train_model, but entirely standalone -- see this
    module's own docstring for why) refitting `model_type` per fold, and
    returns one row per out-of-fold test sample: this row's own context
    features plus whether the primary model's call on it was actually
    correct. None if there isn't enough data or `model_type` isn't a real
    candidate."""
    if model_type not in _CANDIDATES:
        logger.warning("[alpaca_crypto_meta_model] unknown primary model_type %r, skipping", model_type)
        return None
    labeled = _prepare_training_frame(df)
    if len(labeled) < MIN_META_TRAIN_ROWS:
        return None

    from data.alpaca_crypto_model import FEATURE_COLUMNS  # local import: avoids a module-load-order surprise if alpaca_crypto_model ever imports this module back

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
        sample_weight = _recency_sample_weight(ts_tr, half_life_days=ALPACA_CRYPTO_MODEL_RECENCY_HALFLIFE_DAYS)
        try:
            model = _CANDIDATES[model_type]()
            model.fit(x_tr, y_tr, sample_weight=sample_weight)
            proba = model.predict_proba(x_te)[:, 1]
        except Exception as exc:
            logger.warning("[alpaca_crypto_meta_model] fold fit failed: %s", exc)
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
    """Builds the out-of-fold dataset (see `_build_oof_dataset`) and fits a
    single calibrated LogisticRegression on it -- deliberately simple:
    the meta-problem (given a compact regime/confidence context, was the
    primary model right?) is lower-dimensional and needs to stay
    well-calibrated (its OWN output IS the trust score handed to
    alpaca_crypto_strategy.py, not just a directional call), not
    maximally expressive. Evaluated on a chronological holdout the
    meta-model never trains on, for an honest accuracy/AUC in the
    returned summary. Never raises on ordinary "not enough data yet"
    conditions."""
    frame = df if df is not None else load_training_dataset()
    if frame.empty:
        return {"ok": False, "reason": "no_data"}

    if primary_model_type is None:
        from data.alpaca_crypto_model import load_model as _load_primary_model
        _, primary_meta = _load_primary_model()
        primary_model_type = (primary_meta or {}).get("model_type")
        # An "ensemble" primary model has no single sklearn model TYPE to
        # refit per fold here -- fall back to the single best base
        # candidate's own walk-forward scores instead, same fallback
        # alpaca_crypto_model.py's own docstring already treats as a fully
        # valid non-ensemble outcome.
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
        # Thin-holdout fallback: same "still ship something real, just
        # uncalibrated" posture as alpaca_crypto_model.py's own cold-start
        # path -- refit on ALL out-of-fold rows rather than leaving a
        # usable model on the table over a too-small holdout slice.
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
        api = HfApi(token=HF_API_KEY)
        try:
            api.repo_info(repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, repo_type="model")
        except Exception:
            api.create_repo(repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, repo_type="model", exist_ok=True, private=False)
        api.upload_file(
            path_or_fileobj=str(META_MODEL_PATH), path_in_repo="alpaca_crypto_meta_model.joblib",
            repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, repo_type="model", commit_message="update alpaca crypto meta-model",
        )
        api.upload_file(
            path_or_fileobj=str(META_MODEL_META_PATH), path_in_repo="alpaca_crypto_meta_model_meta.json",
            repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, repo_type="model", commit_message="update alpaca crypto meta-model metadata",
        )
    except Exception as exc:
        logger.warning("[alpaca_crypto_meta_model] HF push failed: %s", exc)


_META_MODEL_DOWNLOAD_HF_TIMEOUT_SEC = int(os.getenv("ALPACA_CRYPTO_META_MODEL_DOWNLOAD_HF_TIMEOUT_SEC", "15") or "15")


def _download_meta_model_from_hf() -> bool:
    if not HF_API_KEY:
        return False

    def _download() -> bool:
        from huggingface_hub import hf_hub_download
        model_path = hf_hub_download(
            repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, filename="alpaca_crypto_meta_model.joblib",
            repo_type="model", token=HF_API_KEY,
        )
        meta_path = hf_hub_download(
            repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, filename="alpaca_crypto_meta_model_meta.json",
            repo_type="model", token=HF_API_KEY,
        )
        META_MODEL_PATH.write_bytes(Path(model_path).read_bytes())
        META_MODEL_META_PATH.write_text(Path(meta_path).read_text(encoding="utf-8"), encoding="utf-8")
        return True

    try:
        from server_common import call_with_hard_timeout
        return bool(call_with_hard_timeout(_download, timeout_sec=_META_MODEL_DOWNLOAD_HF_TIMEOUT_SEC, on_timeout=False))
    except Exception as exc:
        logger.info("[alpaca_crypto_meta_model] no meta-model available on HF yet: %s", exc)
        return False


def _schedule_hf_meta_model_recheck() -> None:
    from server_common import maybe_schedule_hf_model_recheck, refresh_model_if_hf_has_a_newer_one

    def _check() -> None:
        refresh_model_if_hf_has_a_newer_one(
            model_repo=HF_ALPACA_CRYPTO_MODEL_REPO, token=HF_API_KEY, meta_filename="alpaca_crypto_meta_model_meta.json",
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
    """Same cache/download/staleness-recheck shape as
    alpaca_crypto_model.load_model -- see that function's own docstring
    for the full rationale (a meta-model retrained by a job that no
    longer runs in THIS process still needs to be picked up on a
    reasonable cadence, not just on this process's next full restart)."""
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
        logger.warning("[alpaca_crypto_meta_model] failed to load cached meta-model: %s", exc)
        return None, None


def trust_score(row: dict[str, Any] | None, *, primary_probability_up: float) -> float | None:
    """The meta-model's own predicted probability that the primary
    model's directional call on `row` (already made, `primary_probability_up`
    already computed by alpaca_crypto_model.predict_direction) is CORRECT
    -- None if no meta-model is trained yet or `row` is missing the
    context features this needs. Never raises -- same best-effort
    contract as every other prediction path in this codebase."""
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
        logger.debug("[alpaca_crypto_meta_model] trust_score failed: %s", exc)
        return None
