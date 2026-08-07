"""Direction classifier: given current multi-timeframe technical features +
news sentiment for a perp instrument, predict whether its price will be
higher or lower `PERPS_LABEL_HORIZON_MINUTES` (default 1, see perps_data.py)
from now.

Training data comes from `perps_data.load_training_dataset()` (local shards,
falling back to downloading the full history from the HF dataset repo).
Several sklearn model types are compared on a chronological holdout split
(never a random shuffle -- this is time series, and a random split would leak
future information into training) and the best one is kept.

The trained model is cached locally (data/perps_model.joblib) and pushed to
the HF model repo (HF_MODEL_REPO) so a fresh deploy can load a working model
immediately instead of needing to retrain from zero first.
"""
from __future__ import annotations

import datetime as dt
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

from data.perps_data import FEATURE_COLUMNS, LABEL_HORIZON_MINUTES, latest_feature_row, load_training_dataset, retry_on_rate_limit

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MODEL_PATH = DATA_DIR / "perps_model.joblib"
MODEL_META_PATH = DATA_DIR / "perps_model_meta.json"

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_MODEL_REPO = os.getenv("HF_MODEL_REPO", "papylove/kalshi-perps-model")

MIN_TRAIN_ROWS = int(os.getenv("PERPS_MIN_TRAIN_ROWS", "300") or "300")
MODEL_CACHE_TTL_SEC = int(os.getenv("PERPS_MODEL_CACHE_TTL_SEC", "1800") or "1800")

# "Think well, not just react": walk-forward CV (below) picks a candidate
# type based on 4 sequential looks instead of one single lucky/unlucky
# holdout split; recency weighting lets the model favor the CURRENT market
# regime over stale history without needing a narrower MAX_TRAIN_ROWS
# window; calibration (below) makes probability_up a genuinely meaningful
# confidence instead of a raw, uninterpreted sklearn score.
WALK_FORWARD_SPLITS = 4
PERPS_MODEL_RECENCY_HALFLIFE_DAYS = float(os.getenv("PERPS_MODEL_RECENCY_HALFLIFE_DAYS", "14") or "14")
# Calibration/ensembling both need a real held-out slice to fit against on
# top of the walk-forward split itself -- below this floor (e.g. right at
# the MIN_TRAIN_ROWS=300 cold-start edge), skip both and fall back to
# EXACTLY the old contract (single best walk-forward candidate, refit on
# 100% of rows, uncalibrated) rather than risk fitting a calibrator on too
# few rows to mean anything.
PERPS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS = int(os.getenv("PERPS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS", "200") or "200")

_model_cache: dict[str, Any] = {"model": None, "meta": None, "loaded_at": 0.0}

# n_jobs=1 (not -1) is deliberate regardless of container size: RandomForest's
# default joblib backend forks a separate OS process per worker, each holding
# its own copy of the training arrays -- a multiple of peak memory for a
# speed trade this isn't worth taking on a shared web dyno either way.
#
# n_estimators history: 100 -> 80 -> 60 across two real OOM incidents on the
# old 512MB-capped container (confirmed live: a container-level OOM kill and
# a separate worker SIGKILL during the daily cron retrain). Raised back up to
# 150 after migrating to a 2GB (standard plan) container -- same value
# already proven safe locally in this codebase's own backtest modules
# (perps_backtest.py etc., which never had this dyno's memory ceiling to
# begin with), not a new guess. Still n_jobs=1 -- the walk-forward CV shape
# some of this codebase's siblings use fits multiple candidates in sequence
# per call regardless of container size, and forking per-tree on top of that
# would multiply peak memory for no accuracy benefit.
_CANDIDATES = {
    "logistic_regression": lambda: LogisticRegression(max_iter=1000, class_weight="balanced"),
    "random_forest": lambda: RandomForestClassifier(
        n_estimators=150, max_depth=6, min_samples_leaf=20, class_weight="balanced", random_state=42, n_jobs=1,
    ),
    "gradient_boosting": lambda: GradientBoostingClassifier(
        n_estimators=150, max_depth=3, learning_rate=0.05, random_state=42,
    ),
}


class _AveragedEnsemble:
    """Unweighted mean of predict_proba across 2-3 already-calibrated
    candidates -- module-level (not a closure) so joblib/pickle can resolve
    it by qualified import path across the train-here/load-there (HF
    download into a possibly different worker process) round trip. Shipped
    only when a real walk-forward comparison shows the average actually
    beats every individual candidate (see train_model()) -- not a fixed
    a-priori rule."""

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


PERPS_MODEL_TRADE_OUTCOME_WIN_WEIGHT = float(os.getenv("PERPS_MODEL_TRADE_OUTCOME_WIN_WEIGHT", "1.3") or "1.3")
PERPS_MODEL_TRADE_OUTCOME_LOSS_WEIGHT = float(os.getenv("PERPS_MODEL_TRADE_OUTCOME_LOSS_WEIGHT", "1.8") or "1.8")


def _trade_outcome_sample_weight(tickers: np.ndarray, ts: np.ndarray, trade_log: list[dict[str, Any]] | None) -> np.ndarray:
    """Extra multiplicative weight for training rows that correspond to a
    REAL past trade entry from the bot's own trade_log -- losses upweighted
    more than wins (a well-established hard-example-weighting technique:
    the model most needs to learn from situations that led to a real loss,
    not just from abstract next-minute price direction with no connection
    to the bot's own trading history). Combines multiplicatively with the
    existing recency weight, same as class_weight="balanced" already does.

    A heuristic proxy, not a perfect causal signal -- label_up is still the
    same 1-minute-ahead price direction target every candidate is fit
    against; only the WEIGHT assigned to a matching row changes based on
    how that specific real trade eventually turned out (which can resolve
    many minutes later, via take_profit/stop_loss/max_hold_time -- not
    necessarily correlated with the 1-minute-ahead label itself). Every row
    not matching a real trade entry keeps weight 1.0, unaffected -- this
    never overrides the recency-weighted, market-data-only training signal
    that already exists, it only sharpens it around the bot's own real
    history where that history exists."""
    weights = np.ones(len(tickers), dtype=float)
    if not trade_log:
        return weights
    outcome_by_key: dict[tuple[str, int], bool] = {}
    for t in trade_log:
        if t.get("dry_run") or not t.get("opened_at") or not t.get("ticker"):
            continue
        try:
            opened_ts = int(dt.datetime.fromisoformat(t["opened_at"]).timestamp())
        except Exception:
            continue
        minute_ts = (opened_ts // 60) * 60
        outcome_by_key[(t["ticker"], minute_ts)] = float(t.get("realized_pnl_usd") or 0.0) > 0
    if not outcome_by_key:
        return weights

    minute_ts_values = (ts.astype(np.int64) // 60) * 60
    for i in range(len(tickers)):
        won = outcome_by_key.get((tickers[i], int(minute_ts_values[i])))
        if won is not None:
            weights[i] *= PERPS_MODEL_TRADE_OUTCOME_WIN_WEIGHT if won else PERPS_MODEL_TRADE_OUTCOME_LOSS_WEIGHT
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
    labeled["ticker_code"] = labeled["ticker"].astype("category").cat.codes
    return labeled.sort_values("ts").reset_index(drop=True)


def train_model(df: pd.DataFrame | None = None, trade_log: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Train, compare candidates via walk-forward (chronological, never
    randomly-shuffled) cross-validation, keep the best -- calibrated, and
    ensembled if the evidence from THIS retrain's own folds actually
    supports it -- persist locally + to HF. Returns a summary dict either
    way (never raises on ordinary "not enough data yet" conditions --
    that's expected during the first days of data collection).

    trade_log (perps_strategy state's own trade_log, passed in by the
    caller -- this module never imports perps_strategy directly, which
    would create a circular import) feeds _trade_outcome_sample_weight: see
    its own docstring for why the bot's real past wins/losses get folded
    into training, not just raw market-data labels.

    Real profiling (this session) against real production data: this
    walk-forward design costs ~2x the wall-clock time of the old single-
    split version (~50s vs ~24s locally) for +8MB peak RSS (+3.5%) --
    trivial against the 512MB ceiling and nowhere near _run_perps_train's
    30-minute stale-lock ceiling (a daily cron, not a hot path). The mean
    walk-forward winner picked a DIFFERENT candidate than the old single-
    split baseline did on the same real data -- exactly the single-split
    fragility this exists to fix."""
    frame = df if df is not None else load_training_dataset()
    if frame.empty:
        return {"ok": False, "reason": "no_data"}

    labeled = _prepare_training_frame(frame)
    # _prepare_training_frame() already .copy()'d everything it needs into
    # `labeled` -- `frame` itself is 100% dead weight from here on, and
    # freeing it now (rather than letting it ride until the function
    # returns) matters on a 512MB-capped container that has ALSO already
    # crashed from this exact function's own peak memory once this session.
    del frame
    if len(labeled) < MIN_TRAIN_ROWS:
        return {"ok": False, "reason": "insufficient_rows", "rows": len(labeled), "need": MIN_TRAIN_ROWS}

    feature_cols = FEATURE_COLUMNS + ["ticker_code"]
    n_rows = len(labeled)
    x_all = labeled[feature_cols].values
    y_all = labeled["label_up"].values
    ts_all = labeled["ts"].values
    oldest_row_age_days = float((ts_all.max() - ts_all.min()) / 86400.0) if n_rows else 0.0
    ticker_categories = list(labeled["ticker"].astype("category").cat.categories)
    outcome_weight_all = _trade_outcome_sample_weight(labeled["ticker"].values, ts_all, trade_log)
    # `labeled` is now redundant with the numpy arrays above -- unlike the
    # old single-split shape, the walk-forward loop below holds these arrays
    # open across several fit calls in a row, so freeing the heavier
    # DataFrame representation early only helps.
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
        sample_weight = _recency_sample_weight(ts_tr, half_life_days=PERPS_MODEL_RECENCY_HALFLIFE_DAYS)
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
                logger.warning("[perps_model] candidate %s failed on fold %d: %s", name, fold_idx, exc)

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
        and len(last_test_idx) >= PERPS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS
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
        full_sample_weight = _recency_sample_weight(ts_all, half_life_days=PERPS_MODEL_RECENCY_HALFLIFE_DAYS)
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
        "recency_halflife_days": PERPS_MODEL_RECENCY_HALFLIFE_DAYS,
        "calibration_min_holdout_rows": PERPS_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS,
        "feature_columns": feature_cols,
        "ticker_categories": ticker_categories,
        "label_horizon_minutes": LABEL_HORIZON_MINUTES,
        # How many training rows actually matched a real past trade entry
        # (see _trade_outcome_sample_weight) -- 0 is expected/fine (falls
        # back to pure recency weighting), useful to see this grow over time
        # as the account trades more.
        "trade_outcome_rows_matched": int(np.sum(outcome_weight_all != 1.0)),
    }

    joblib.dump(best_model, MODEL_PATH)
    MODEL_META_PATH.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    _model_cache.update({"model": best_model, "meta": meta, "loaded_at": time.time()})

    _push_model_to_hf()
    # The walk-forward loop above fits up to 4 folds x 3 candidates (12
    # models total) in sequence -- most go out of scope naturally, but an
    # explicit collect() here helps CPython consolidate/reuse that churn's
    # freed memory sooner rather than later, on the SAME 512MB-capped
    # container that has already had two real OOM incidents this session.
    # A real, if modest, additional safety margin -- not a substitute for
    # the row-cap/estimator-count/lookback-window fixes already in place.
    gc.collect()
    return {"ok": True, **meta}


def _push_model_to_hf() -> None:
    if not HF_API_KEY:
        return
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
        try:
            api.repo_info(repo_id=HF_MODEL_REPO, repo_type="model")
        except Exception:
            api.create_repo(repo_id=HF_MODEL_REPO, repo_type="model", exist_ok=True, private=False)
        retry_on_rate_limit(lambda: api.upload_file(
            path_or_fileobj=str(MODEL_PATH), path_in_repo="perps_model.joblib",
            repo_id=HF_MODEL_REPO, repo_type="model", commit_message="update perps direction model",
        ))
        retry_on_rate_limit(lambda: api.upload_file(
            path_or_fileobj=str(MODEL_META_PATH), path_in_repo="perps_model_meta.json",
            repo_id=HF_MODEL_REPO, repo_type="model", commit_message="update perps model metadata",
        ))
    except Exception as exc:
        logger.warning("[perps_model] HF model push failed: %s", exc)


_MODEL_DOWNLOAD_HF_TIMEOUT_SEC = int(os.getenv("PERPS_MODEL_DOWNLOAD_HF_TIMEOUT_SEC", "15") or "15")


def _download_model_from_hf() -> bool:
    if not HF_API_KEY:
        return False

    def _download() -> bool:
        from huggingface_hub import hf_hub_download
        model_path = hf_hub_download(
            repo_id=HF_MODEL_REPO, filename="perps_model.joblib", repo_type="model", token=HF_API_KEY,
        )
        meta_path = hf_hub_download(
            repo_id=HF_MODEL_REPO, filename="perps_model_meta.json", repo_type="model", token=HF_API_KEY,
        )
        MODEL_PATH.write_bytes(Path(model_path).read_bytes())
        MODEL_META_PATH.write_text(Path(meta_path).read_text(encoding="utf-8"), encoding="utf-8")
        return True

    try:
        # Real, confirmed production incident (Render's own logs): this is
        # called directly from app_kalshi.py's /api/status on every cold
        # boot (MODEL_PATH/MODEL_META_PATH only exist locally AFTER the
        # first successful download, wiped by every restart) -- unbounded,
        # it hung on huggingface_hub's own internal session lock long
        # enough to freeze this --workers 1 process until gunicorn's
        # worker timeout SIGKILLed it, which wipes local disk and
        # guarantees the NEXT boot hits the exact same unconditional call
        # again: a self-sustaining crash loop, the same class of bug
        # already fixed in _pull_durable_state_from_hf but missed here.
        from server_common import call_with_hard_timeout
        return bool(call_with_hard_timeout(_download, timeout_sec=_MODEL_DOWNLOAD_HF_TIMEOUT_SEC, on_timeout=False))
    except Exception as exc:
        logger.info("[perps_model] no model available on HF yet: %s", exc)
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
        logger.warning("[perps_model] failed to load cached model: %s", exc)
        return None, None


def predict_direction(ticker: str) -> dict[str, Any]:
    """{"model_ok": False} if no trained model exists yet (expected during
    the first days of data collection -- callers should fall back to
    technical-only signals in that case). Otherwise returns direction +
    probability_up + the raw feature row used, for observability."""
    model, meta = load_model()
    if model is None or meta is None:
        return {"model_ok": False, "ticker": ticker}

    row = latest_feature_row(ticker)
    if row is None:
        return {"model_ok": False, "ticker": ticker, "reason": "no_feature_data"}

    categories = meta.get("ticker_categories") or []
    ticker_code = float(categories.index(ticker)) if ticker in categories else -1.0
    feature_cols = meta.get("feature_columns") or (FEATURE_COLUMNS + ["ticker_code"])
    x = np.array([[row.get(col, ticker_code if col == "ticker_code" else 0.0) for col in feature_cols]])

    try:
        proba_up = float(model.predict_proba(x)[0][1])
    except Exception as exc:
        return {"model_ok": False, "ticker": ticker, "reason": f"predict_failed: {exc}"}

    return {
        "model_ok": True, "ticker": ticker, "probability_up": proba_up,
        "direction": "up" if proba_up >= 0.5 else "down",
        "current_price": row["current_price"], "short_ma": row["short_ma"], "trend_pct": row["trend_pct"],
        "model_type": meta.get("model_type"), "trained_at": meta.get("trained_at"),
    }
