"""Direction classifier for Kalshi's 15-minute event-contract markets:
given current multi-timeframe technical features + news sentiment for one
of the 5 traded coins, predict whether its price will be higher or lower
kalshi_15m_data.LABEL_HORIZON_MINUTES (15) minutes from now -- matching
what these markets actually settle on (see kalshi_15m.py's own module
docstring).

Faithfully mirrors perps_model.py's own proven architecture (walk-forward
CV across 3 candidates, sigmoid calibration, ensembling only when a real
fold-level comparison supports it, recency + trade-outcome sample
weighting) rather than a simpler one-off -- see perps_model.train_model's
own docstring for the full design rationale behind each piece, all
identical here except "ticker" -> "symbol" (this module's own column
name, see kalshi_15m_data.py) and this module's own HF_MODEL_REPO/
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

from data.kalshi_15m_data import LABEL_HORIZON_MINUTES, latest_feature_row, load_training_dataset
from data.perps_data import FEATURE_COLUMNS

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MODEL_PATH = DATA_DIR / "kalshi_15m_model.joblib"
MODEL_META_PATH = DATA_DIR / "kalshi_15m_model_meta.json"

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_KALSHI_15M_MODEL_REPO = os.getenv("HF_KALSHI_15M_MODEL_REPO", "papylove/kalshi-15m-model")

MIN_TRAIN_ROWS = int(os.getenv("KALSHI_15M_MIN_TRAIN_ROWS", "300") or "300")
MODEL_CACHE_TTL_SEC = int(os.getenv("KALSHI_15M_MODEL_CACHE_TTL_SEC", "1800") or "1800")

WALK_FORWARD_SPLITS = 4
KALSHI_15M_MODEL_RECENCY_HALFLIFE_DAYS = float(os.getenv("KALSHI_15M_MODEL_RECENCY_HALFLIFE_DAYS", "14") or "14")
KALSHI_15M_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS = int(os.getenv("KALSHI_15M_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS", "200") or "200")

_model_cache: dict[str, Any] = {"model": None, "meta": None, "loaded_at": 0.0}

HF_MODEL_RECHECK_INTERVAL_SEC = int(os.getenv("KALSHI_15M_MODEL_HF_RECHECK_INTERVAL_SEC", "600") or "600")
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


def _suppress_torch_numpy_warning() -> None:
    """Identical to alpaca_options_model's own copy of this function --
    some torch CPU wheels (built against numpy 1.x) emit a noisy
    UserWarning on first import under this project's numpy 2.x pin, real
    and confirmed harmless (see predict_proba below's own .tolist() use),
    but would otherwise spam every training-job log line looking exactly
    like a crash traceback."""
    import warnings
    warnings.filterwarnings("ignore", message=".*NumPy 1.x.*", category=UserWarning)
    warnings.filterwarnings("ignore", message=".*Failed to initialize NumPy.*", category=UserWarning)


class _TorchMLPClassifier:
    """Hand-built feedforward neural net (3 hidden layers, ReLU + dropout,
    sigmoid output via BCEWithLogitsLoss) wrapped in a scikit-learn-
    compatible interface (.fit/.predict/.predict_proba) -- a genuinely
    custom model, not one of sklearn's canned classifiers, per explicit
    user direction ("change the model... to a more powerful model...
    trained on finance and prediction"). Line-for-line identical
    architecture to alpaca_options_model.py's own _TorchMLPClassifier
    (64->32->16->1, dropout 0.2, Adam weight_decay, chronological-
    validation-slice early stopping) -- same "independent per-market
    module, not a shared base class" convention this whole codebase
    already follows elsewhere, not a missed opportunity to share code.

    Deliberately NOT added to _CANDIDATES / train_model()'s existing
    walk-forward loop (which already fits up to 12 models per call across
    4 folds x 3 candidates) -- see train_torch_candidate_model() below,
    which trains this one candidate in complete isolation, on its own
    low-frequency daily schedule, using a single chronological split
    rather than the full 4-fold walk-forward. `torch` is imported lazily
    inside these methods, never at module level: measured elsewhere in
    this codebase, `import torch` alone costs ~154MB RSS -- merely
    importing this file (done on every predict_direction call) must
    never pay that cost.

    Persists as a plain state_dict + numpy normalization stats rather
    than a live nn.Module/optimizer -- keeps the joblib-pickled object's
    own footprint to just small tensors + arrays."""

    def __init__(self, input_dim: int, hidden_dim: int = 64, epochs: int = 100,
                 lr: float = 1e-3, batch_size: int = 256, random_state: int = 42,
                 dropout: float = 0.2, weight_decay: float = 1e-5,
                 early_stopping_patience: int = 8, validation_fraction: float = 0.15):
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.epochs = epochs
        self.lr = lr
        self.batch_size = batch_size
        self.random_state = random_state
        self.dropout = dropout
        self.weight_decay = weight_decay
        self.early_stopping_patience = early_stopping_patience
        self.validation_fraction = validation_fraction
        self._state_dict: dict[str, Any] | None = None
        self._x_mean: np.ndarray | None = None
        self._x_std: np.ndarray | None = None

    def _build_net(self):
        from torch import nn
        mid = max(self.hidden_dim // 2, 8)
        small = max(self.hidden_dim // 4, 4)
        return nn.Sequential(
            nn.Linear(self.input_dim, self.hidden_dim), nn.ReLU(), nn.Dropout(self.dropout),
            nn.Linear(self.hidden_dim, mid), nn.ReLU(), nn.Dropout(self.dropout),
            nn.Linear(mid, small), nn.ReLU(),
            nn.Linear(small, 1),
        )

    def fit(self, x: np.ndarray, y: np.ndarray, sample_weight: np.ndarray | None = None) -> "_TorchMLPClassifier":
        _suppress_torch_numpy_warning()
        import torch
        from torch import nn

        torch.manual_seed(self.random_state)
        torch.set_num_threads(1)

        x_mean, x_std = x.mean(axis=0), x.std(axis=0)
        x_std[x_std == 0] = 1.0
        self._x_mean, self._x_std = x_mean, x_std
        x_norm = (x - x_mean) / x_std

        n_total = len(x_norm)
        n_val = int(n_total * self.validation_fraction)
        use_early_stopping = n_val >= 20 and (n_total - n_val) >= 20
        if use_early_stopping:
            x_fit, y_fit = x_norm[:-n_val], y[:-n_val]
            x_val, y_val = x_norm[-n_val:], y[-n_val:]
            w_fit = sample_weight[:-n_val] if sample_weight is not None else None
            w_val = sample_weight[-n_val:] if sample_weight is not None else None
        else:
            x_fit, y_fit, w_fit = x_norm, y, sample_weight
            x_val = y_val = w_val = None

        net = self._build_net()
        x_t = torch.tensor(x_fit, dtype=torch.float32)
        y_t = torch.tensor(y_fit, dtype=torch.float32).view(-1, 1)
        w_t = torch.tensor(w_fit, dtype=torch.float32).view(-1, 1) if w_fit is not None else None

        opt = torch.optim.Adam(net.parameters(), lr=self.lr, weight_decay=self.weight_decay)
        loss_fn = nn.BCEWithLogitsLoss(reduction="none" if w_t is not None else "mean")
        n = len(x_t)
        best_val_loss = float("inf")
        best_state: dict[str, Any] | None = None
        epochs_without_improvement = 0
        x_val_t = torch.tensor(x_val, dtype=torch.float32) if x_val is not None else None
        y_val_t = torch.tensor(y_val, dtype=torch.float32).view(-1, 1) if y_val is not None else None
        w_val_t = torch.tensor(w_val, dtype=torch.float32).view(-1, 1) if w_val is not None else None
        val_loss_fn = nn.BCEWithLogitsLoss(reduction="none" if w_val_t is not None else "mean")

        for _epoch in range(self.epochs):
            net.train()
            perm = torch.randperm(n)
            for start in range(0, n, self.batch_size):
                idx = perm[start:start + self.batch_size]
                opt.zero_grad()
                logits = net(x_t[idx])
                loss = loss_fn(logits, y_t[idx])
                if w_t is not None:
                    loss = (loss * w_t[idx]).mean()
                loss.backward()
                opt.step()

            if not use_early_stopping:
                continue
            net.eval()
            with torch.no_grad():
                val_logits = net(x_val_t)
                val_loss_raw = val_loss_fn(val_logits, y_val_t)
                val_loss = float((val_loss_raw * w_val_t).mean() if w_val_t is not None else val_loss_raw)
            if val_loss < best_val_loss - 1e-4:
                best_val_loss = val_loss
                best_state = {k: v.clone() for k, v in net.state_dict().items()}
                epochs_without_improvement = 0
            else:
                epochs_without_improvement += 1
                if epochs_without_improvement >= self.early_stopping_patience:
                    break

        self._state_dict = best_state if best_state is not None else {k: v.clone() for k, v in net.state_dict().items()}
        del net, x_t, y_t, opt
        return self

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        _suppress_torch_numpy_warning()
        import torch

        torch.set_num_threads(1)
        net = self._build_net()
        net.load_state_dict(self._state_dict)
        net.eval()
        x_norm = (x - self._x_mean) / self._x_std
        with torch.no_grad():
            logits = net(torch.tensor(x_norm, dtype=torch.float32))
            proba_up = np.asarray(torch.sigmoid(logits).view(-1).tolist())
        return np.column_stack([1.0 - proba_up, proba_up])

    def predict(self, x: np.ndarray) -> np.ndarray:
        return (self.predict_proba(x)[:, 1] >= 0.5).astype(int)


def _recency_sample_weight(ts: np.ndarray, *, half_life_days: float) -> np.ndarray:
    if half_life_days <= 0 or len(ts) == 0:
        return np.ones(len(ts), dtype=float)
    age_days = (ts.max() - ts) / 86400.0
    return np.power(0.5, age_days / half_life_days)


KALSHI_15M_MODEL_TRADE_OUTCOME_WIN_WEIGHT = float(os.getenv("KALSHI_15M_MODEL_TRADE_OUTCOME_WIN_WEIGHT", "1.3") or "1.3")
KALSHI_15M_MODEL_TRADE_OUTCOME_LOSS_WEIGHT = float(os.getenv("KALSHI_15M_MODEL_TRADE_OUTCOME_LOSS_WEIGHT", "1.8") or "1.8")


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
            weights[i] *= KALSHI_15M_MODEL_TRADE_OUTCOME_WIN_WEIGHT if won else KALSHI_15M_MODEL_TRADE_OUTCOME_LOSS_WEIGHT
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
        sample_weight = _recency_sample_weight(ts_tr, half_life_days=KALSHI_15M_MODEL_RECENCY_HALFLIFE_DAYS)
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
                logger.warning("[kalshi_15m_model] candidate %s failed on fold %d: %s", name, fold_idx, exc)

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
        and len(last_test_idx) >= KALSHI_15M_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS
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
        full_sample_weight = _recency_sample_weight(ts_all, half_life_days=KALSHI_15M_MODEL_RECENCY_HALFLIFE_DAYS)
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
        "recency_halflife_days": KALSHI_15M_MODEL_RECENCY_HALFLIFE_DAYS,
        "calibration_min_holdout_rows": KALSHI_15M_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS,
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


def train_torch_candidate_model(df: pd.DataFrame | None = None, trade_log: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Trains the custom PyTorch MLP candidate in complete isolation from
    train_model()'s existing walk-forward sklearn/ensemble candidates --
    own data load, own single chronological split (not the full 4-fold
    walk-forward -- see _TorchMLPClassifier's own docstring for why), own
    fit, with the SAME recency + trade-outcome sample weighting
    train_model's own walk-forward loop uses -- and promotes it to the
    live model ONLY if it actually beats the currently-persisted model's
    freshly-recomputed score on the SAME holdout. Never raises on
    ordinary "not enough data yet" conditions. Identical promotion logic
    to alpaca_options_model.train_torch_candidate_model."""
    frame = df if df is not None else load_training_dataset()
    if frame.empty:
        return {"ok": False, "reason": "no_data"}

    labeled = _prepare_training_frame(frame)
    del frame
    if len(labeled) < MIN_TRAIN_ROWS:
        return {"ok": False, "reason": "insufficient_rows", "rows": len(labeled), "need": MIN_TRAIN_ROWS}

    feature_cols = FEATURE_COLUMNS + ["symbol_code"]
    split_idx = int(len(labeled) * 0.8)
    train_df, test_df = labeled.iloc[:split_idx], labeled.iloc[split_idx:]
    del labeled
    if train_df.empty or test_df.empty or test_df["label_up"].nunique() < 2:
        return {"ok": False, "reason": "insufficient_class_variety"}

    x_train, y_train, ts_train = train_df[feature_cols].values, train_df["label_up"].values, train_df["ts"].values
    x_test, y_test = test_df[feature_cols].values, test_df["label_up"].values
    symbol_categories = list(train_df["symbol"].astype("category").cat.categories)
    n_rows = len(train_df) + len(test_df)
    outcome_weight = _trade_outcome_sample_weight(train_df["symbol"].values, ts_train, trade_log)
    del train_df, test_df
    sample_weight = _recency_sample_weight(ts_train, half_life_days=KALSHI_15M_MODEL_RECENCY_HALFLIFE_DAYS) * outcome_weight

    try:
        torch_model = _TorchMLPClassifier(input_dim=len(feature_cols))
        torch_model.fit(x_train, y_train, sample_weight=sample_weight)
        torch_preds = torch_model.predict(x_test)
        torch_proba = torch_model.predict_proba(x_test)[:, 1]
        torch_score = (float(accuracy_score(y_test, torch_preds)) + float(roc_auc_score(y_test, torch_proba))) / 2.0
    except Exception as exc:
        logger.warning("[kalshi_15m_model] torch candidate training failed: %s", exc)
        del x_train, x_test
        gc.collect()
        return {"ok": False, "reason": "torch_training_failed", "error": str(exc)}
    del x_train

    current_model, current_meta = load_model()
    current_score: float | None = None
    if current_model is not None:
        try:
            current_preds = current_model.predict(x_test)
            current_proba = current_model.predict_proba(x_test)[:, 1]
            current_score = (float(accuracy_score(y_test, current_preds)) + float(roc_auc_score(y_test, current_proba))) / 2.0
        except Exception as exc:
            logger.warning("[kalshi_15m_model] could not re-score current model against torch's holdout: %s", exc)

    promoted = current_score is None or torch_score > current_score
    result = {
        "ok": True, "promoted": promoted, "torch_score": torch_score,
        "current_score": current_score, "current_model_type": (current_meta or {}).get("model_type"),
        "rows": n_rows,
    }

    if promoted:
        meta = {
            "trained_at": time.time(), "model_type": "torch_mlp", "calibrated": False, "ensemble_members": None,
            "scores": {"torch_mlp": {"combined": torch_score}, "previous": {"combined": current_score}},
            "rows": n_rows, "feature_columns": feature_cols, "symbol_categories": symbol_categories,
            "feature_importances": {}, "recency_halflife_days": KALSHI_15M_MODEL_RECENCY_HALFLIFE_DAYS,
            "label_horizon_minutes": LABEL_HORIZON_MINUTES,
            "trade_outcome_rows_matched": int(np.sum(outcome_weight != 1.0)),
        }
        joblib.dump(torch_model, MODEL_PATH)
        MODEL_META_PATH.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        _model_cache.update({"model": torch_model, "meta": meta, "loaded_at": time.time()})
        _push_model_to_hf()

    gc.collect()
    return result


def _push_model_to_hf() -> None:
    if not HF_API_KEY:
        return
    try:
        from huggingface_hub import HfApi
        from data.kalshi_15m_data import retry_on_rate_limit
        api = HfApi(token=HF_API_KEY)
        try:
            api.repo_info(repo_id=HF_KALSHI_15M_MODEL_REPO, repo_type="model")
        except Exception:
            api.create_repo(repo_id=HF_KALSHI_15M_MODEL_REPO, repo_type="model", exist_ok=True, private=False)
        retry_on_rate_limit(lambda: api.upload_file(
            path_or_fileobj=str(MODEL_PATH), path_in_repo="kalshi_15m_model.joblib",
            repo_id=HF_KALSHI_15M_MODEL_REPO, repo_type="model", commit_message="update kalshi 15m direction model",
        ))
        retry_on_rate_limit(lambda: api.upload_file(
            path_or_fileobj=str(MODEL_META_PATH), path_in_repo="kalshi_15m_model_meta.json",
            repo_id=HF_KALSHI_15M_MODEL_REPO, repo_type="model", commit_message="update kalshi 15m model metadata",
        ))
    except Exception as exc:
        logger.warning("[kalshi_15m_model] HF model push failed: %s", exc)


_MODEL_DOWNLOAD_HF_TIMEOUT_SEC = int(os.getenv("KALSHI_15M_MODEL_DOWNLOAD_HF_TIMEOUT_SEC", "15") or "15")


def _download_model_from_hf() -> bool:
    if not HF_API_KEY:
        return False

    def _download() -> bool:
        from huggingface_hub import hf_hub_download
        model_path = hf_hub_download(
            repo_id=HF_KALSHI_15M_MODEL_REPO, filename="kalshi_15m_model.joblib", repo_type="model", token=HF_API_KEY,
        )
        meta_path = hf_hub_download(
            repo_id=HF_KALSHI_15M_MODEL_REPO, filename="kalshi_15m_model_meta.json", repo_type="model", token=HF_API_KEY,
        )
        MODEL_PATH.write_bytes(Path(model_path).read_bytes())
        MODEL_META_PATH.write_text(Path(meta_path).read_text(encoding="utf-8"), encoding="utf-8")
        return True

    try:
        from server_common import call_with_hard_timeout
        return bool(call_with_hard_timeout(_download, timeout_sec=_MODEL_DOWNLOAD_HF_TIMEOUT_SEC, on_timeout=False))
    except Exception as exc:
        logger.info("[kalshi_15m_model] no model available on HF yet: %s", exc)
        return False


def _schedule_hf_model_recheck() -> None:
    from server_common import maybe_schedule_hf_model_recheck, refresh_model_if_hf_has_a_newer_one

    def _check() -> None:
        refresh_model_if_hf_has_a_newer_one(
            model_repo=HF_KALSHI_15M_MODEL_REPO, token=HF_API_KEY, meta_filename="kalshi_15m_model_meta.json",
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
        logger.warning("[kalshi_15m_model] failed to load cached model: %s", exc)
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
        # The full feature row, for kalshi_15m_strategy.evaluate_candidate's
        # own correlation-study confidence layer (multi_timeframe_bullishness
        # needs the same ret_5m/trend_1h/etc. fields this row already has --
        # see crypto_correlation.py's own docstring) -- avoids a SECOND,
        # real, non-free latest_feature_row call (candle fetch + sentiment +
        # feature engineering) for the exact same coin on the exact same
        # cycle.
        "feature_row": row,
    }
