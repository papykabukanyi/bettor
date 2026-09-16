"""Direction classifier for Alpaca-traded equities/ETFs -- trained on
alpaca_data's stock features/labels and persisted to HF_ALPACA_MODEL_REPO
instead of the Kalshi perps bot's own model repo. Never touches any Kalshi
perps state or files.

Shares perps_model.py's own walk-forward-CV + recency/trade-outcome
sample weighting + calibration/ensembling design (see train_model()'s own
docstring) -- ported here after a strategy review found this file still
on the older single-chronological-split contract perps_model.py/
alpaca_crypto_model.py/alpaca_options_model.py all already moved past.
This matters more here, not less: a real broad backtest this same review
ran (~915K rows, ~11 months, the live 10-symbol watchlist) found the
current live config barely breaks even (-0.34% over the period) -- exactly
the kind of thin, fragile edge a single lucky-or-unlucky 80/20 split could
overstate, and MODEL_CONFIDENCE_MIN=0.52 is compared directly against
probability_up, a raw, uncalibrated sklearn score before this change, same
gap the other 3 services' own calibration steps already closed.
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

from data.alpaca_data import FEATURE_COLUMNS, latest_feature_row, load_training_dataset

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MODEL_PATH = DATA_DIR / "alpaca_model.joblib"
MODEL_META_PATH = DATA_DIR / "alpaca_model_meta.json"

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_ALPACA_MODEL_REPO = os.getenv("HF_ALPACA_MODEL_REPO", "papylove/alpaca-model")

MIN_TRAIN_ROWS = int(os.getenv("ALPACA_MIN_TRAIN_ROWS", "300") or "300")
MODEL_CACHE_TTL_SEC = int(os.getenv("ALPACA_MODEL_CACHE_TTL_SEC", "1800") or "1800")

# See perps_model.py's own identical constants for the full rationale:
# walk-forward CV picks a candidate based on 4 sequential looks instead of
# one single lucky/unlucky holdout split; recency weighting lets the model
# favor the CURRENT market regime over stale history; calibration makes
# probability_up -- the exact number MODEL_CONFIDENCE_MIN is compared
# against every entry -- a genuinely meaningful confidence instead of a
# raw, uninterpreted sklearn score.
WALK_FORWARD_SPLITS = 4
ALPACA_MODEL_RECENCY_HALFLIFE_DAYS = float(os.getenv("ALPACA_MODEL_RECENCY_HALFLIFE_DAYS", "30") or "30")
# Calibration/ensembling both need a real held-out slice to fit against on
# top of the walk-forward split itself -- below this floor (e.g. right at
# the MIN_TRAIN_ROWS=300 cold-start edge), skip both and fall back to
# EXACTLY the old contract (single best walk-forward candidate, refit on
# 100% of rows, uncalibrated) rather than risk fitting a calibrator on too
# few rows to mean anything.
ALPACA_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS = int(os.getenv("ALPACA_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS", "200") or "200")

_model_cache: dict[str, Any] = {"model": None, "meta": None, "loaded_at": 0.0}

# Bounded, background re-check that a NEWER model exists on HF than
# whatever this process already has cached -- see
# server_common.maybe_schedule_hf_model_recheck's own docstring (and
# perps_model.py's identical wiring) for the real bug this fixes: this
# cache never used to re-check HF once a local copy existed, only
# re-downloading if the file was missing entirely. Matters once training
# can run somewhere other than this same process.
HF_MODEL_RECHECK_INTERVAL_SEC = int(os.getenv("ALPACA_MODEL_HF_RECHECK_INTERVAL_SEC", "600") or "600")
_hf_recheck_state: dict[str, Any] = {"last_checked_at": 0.0, "checking": False}
_hf_recheck_lock = threading.Lock()

# n_jobs=1->4: see perps_model.py's own comment for the full reasoning --
# real process-forking memory multiplication, but negligible in absolute
# terms against this app's current 32GB HF Docker Space ceiling. Bounded
# at 4 (not -1/8) since 4 markets share these 8 vCPUs in one process now.
#
# n_estimators history: 100 -> 60 after real, confirmed production OOM
# incidents on this exact service's old 512MB container (two oomKilled
# restarts roughly ALPACA_INTENSIVE_TRAINING_MINUTES apart -- that job fits
# THIS model AND alpaca_backtest.py's own candidates in the same call, up to
# 6 total). Raised back up to 150 after migrating to a 2GB (standard plan)
# container -- same value already proven safe locally in this codebase's own
# backtest modules, not a new guess.
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
    candidates -- module-level (not a closure) so joblib/pickle can resolve
    it by qualified import path across the train-here/load-there (HF
    download into a possibly different worker process) round trip. Shipped
    only when a real walk-forward comparison shows the average actually
    beats every individual candidate (see train_model()) -- not a fixed
    a-priori rule. Identical to perps_model.py's/alpaca_crypto_model.py's
    own class of the same name -- replicated, not imported, matching this
    codebase's own per-market isolation convention (no cross-market
    imports)."""

    def __init__(self, models: list[Any], names: list[str]):
        self.models = models
        self.names = names

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        return np.mean([m.predict_proba(x) for m in self.models], axis=0)

    def predict(self, x: np.ndarray) -> np.ndarray:
        return (self.predict_proba(x)[:, 1] >= 0.5).astype(int)


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


ALPACA_MODEL_TRADE_OUTCOME_WIN_WEIGHT = float(os.getenv("ALPACA_MODEL_TRADE_OUTCOME_WIN_WEIGHT", "1.3") or "1.3")
ALPACA_MODEL_TRADE_OUTCOME_LOSS_WEIGHT = float(os.getenv("ALPACA_MODEL_TRADE_OUTCOME_LOSS_WEIGHT", "1.8") or "1.8")


def _trade_outcome_sample_weight(symbols: np.ndarray, ts: np.ndarray, trade_log: list[dict[str, Any]] | None) -> np.ndarray:
    """Extra multiplicative weight for training rows that correspond to a
    REAL past trade entry from the bot's own trade_log -- losses upweighted
    more than wins. Identical design to perps_model.py's/
    alpaca_crypto_model.py's own function of the same name (see either's
    docstring for the full rationale) -- keyed on symbol, otherwise
    unchanged. Every row not matching a real trade entry keeps weight 1.0,
    unaffected."""
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
            weights[i] *= ALPACA_MODEL_TRADE_OUTCOME_WIN_WEIGHT if won else ALPACA_MODEL_TRADE_OUTCOME_LOSS_WEIGHT
    return weights


def _feature_importance_map(model: Any, feature_cols: list[str]) -> dict[str, float] | None:
    """Surfaces which features the trained model actually leaned on --
    e.g. how much weight sentiment_score carried relative to the technical
    features -- via /api/alpaca/status's model.feature_importances, rather
    than sentiment_score just being one more opaque input nobody can
    verify is doing anything. Same helper already proven on the perps
    side (perps_model.py)."""
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
    """Train, compare candidates via walk-forward (chronological, never
    randomly-shuffled) cross-validation, keep the best -- calibrated, and
    ensembled if the evidence from THIS retrain's own folds actually
    supports it -- persist locally + to HF_ALPACA_MODEL_REPO. Returns a
    summary dict either way (never raises on ordinary "not enough data
    yet" conditions -- expected during the first days of stock data
    collection). See this module's own docstring for why this now mirrors
    perps_model.py's/alpaca_crypto_model.py's design exactly instead of
    the older single 80/20-split contract.

    trade_log (alpaca_strategy state's own trade_log, passed in by the
    caller -- this module never imports alpaca_strategy directly, which
    would create a circular import) feeds _trade_outcome_sample_weight:
    see its own docstring for why the bot's real past wins/losses get
    folded into training, not just raw market-data labels."""
    frame = df if df is not None else load_training_dataset()
    if frame.empty:
        return {"ok": False, "reason": "no_data"}

    labeled = _prepare_training_frame(frame)
    # _prepare_training_frame() already .copy()'d everything it needs into
    # `labeled` -- `frame` itself is 100% dead weight from here on, and
    # freeing it now (rather than letting it ride until the function
    # returns) mirrors perps_model.py's/alpaca_crypto_model.py's own real
    # OOM-avoidance fix.
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
            # Can't score AUC meaningfully on a single-class test fold --
            # skip this ONE fold, don't abort the whole retrain over it.
            continue
        sample_weight = _recency_sample_weight(ts_tr, half_life_days=ALPACA_MODEL_RECENCY_HALFLIFE_DAYS)
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
                logger.warning("[alpaca_model] candidate %s failed on fold %d: %s", name, fold_idx, exc)

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
        and len(last_test_idx) >= ALPACA_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS
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
        full_sample_weight = _recency_sample_weight(ts_all, half_life_days=ALPACA_MODEL_RECENCY_HALFLIFE_DAYS)
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
        "recency_halflife_days": ALPACA_MODEL_RECENCY_HALFLIFE_DAYS,
        "calibration_min_holdout_rows": ALPACA_MODEL_CALIBRATION_MIN_HOLDOUT_ROWS,
        "feature_columns": feature_cols,
        "symbol_categories": symbol_categories,
        # How many training rows actually matched a real past trade entry
        # (see _trade_outcome_sample_weight) -- 0 is expected/fine (falls
        # back to pure recency weighting), useful to see this grow over
        # time as the account trades more.
        "trade_outcome_rows_matched": int(np.sum(outcome_weight_all != 1.0)),
    }

    joblib.dump(best_model, MODEL_PATH)
    MODEL_META_PATH.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    _model_cache.update({"model": best_model, "meta": meta, "loaded_at": time.time()})

    _push_model_to_hf()
    # Confirmed real recurring OOM on this exact service (512MB, running
    # this training job for BOTH the stock and crypto strategies in the
    # same process) -- freeing the walk-forward loop's own churn (up to 4
    # folds x 3 candidates fit in sequence) mirrors the same real fix
    # already proven on the perps side, not a guess.
    gc.collect()
    return {"ok": True, **meta}


def _suppress_torch_numpy_warning() -> None:
    """Some torch CPU wheels (built against numpy 1.x) emit a noisy
    UserWarning on first import under this project's numpy 2.x pin -- real,
    confirmed harmless locally (training/inference both complete correctly
    once .tolist() is used instead of .numpy(), see predict_proba below),
    but would otherwise spam every single training-job log line on Render
    looking exactly like a crash traceback."""
    import warnings
    warnings.filterwarnings("ignore", message=".*NumPy 1.x.*", category=UserWarning)
    warnings.filterwarnings("ignore", message=".*Failed to initialize NumPy.*", category=UserWarning)


class _TorchMLPClassifier:
    """Hand-built feedforward neural net (2 hidden layers, ReLU, sigmoid
    output via BCEWithLogitsLoss) wrapped in a scikit-learn-compatible
    interface (.fit/.predict/.predict_proba) so it drops into the same
    scoring/persistence machinery as every sklearn candidate in this file --
    a genuinely custom model, not one of sklearn's canned classifiers, per
    the user's explicit "fully custom model" request.

    `torch` is imported lazily inside these methods, never at module level:
    measured locally, `import torch` alone costs ~154MB RSS -- a real bite
    out of this service's 512MB ceiling given its own documented OOM
    history (see the n_estimators/n_jobs comments above). Merely importing
    this file (done on every request path that touches predict_direction)
    must never pay that cost -- only actually training or predicting with
    THIS specific candidate does. For the same reason this candidate is
    deliberately NOT added to _CANDIDATES / train_model()'s existing
    multi-candidate loop (which already fits 3 models in one call and has
    its own real OOM history) -- see train_torch_candidate_model() below,
    which trains this one candidate in complete isolation, on its own
    low-frequency schedule.

    Persists as a plain state_dict + numpy normalization stats rather than
    a live nn.Module/optimizer -- the standard PyTorch persistence idiom
    (avoids pickling optimizer/autograd state that doesn't need to survive
    the train-here/load-there HF round trip), and keeps the joblib-pickled
    object's own footprint to just small tensors + arrays."""

    def __init__(self, input_dim: int, hidden_dim: int = 32, epochs: int = 30,
                 lr: float = 1e-3, batch_size: int = 256, random_state: int = 42):
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.epochs = epochs
        self.lr = lr
        self.batch_size = batch_size
        self.random_state = random_state
        self._state_dict: dict[str, Any] | None = None
        self._x_mean: np.ndarray | None = None
        self._x_std: np.ndarray | None = None

    def _build_net(self):
        from torch import nn
        return nn.Sequential(
            nn.Linear(self.input_dim, self.hidden_dim), nn.ReLU(),
            nn.Linear(self.hidden_dim, max(self.hidden_dim // 2, 4)), nn.ReLU(),
            nn.Linear(max(self.hidden_dim // 2, 4), 1),
        )

    def fit(self, x: np.ndarray, y: np.ndarray, sample_weight: np.ndarray | None = None) -> "_TorchMLPClassifier":
        _suppress_torch_numpy_warning()
        import torch
        from torch import nn

        torch.manual_seed(self.random_state)
        torch.set_num_threads(1)  # avoid thread-multiplication memory -- this candidate stays isolated regardless of the sklearn candidates' own n_jobs

        x_mean, x_std = x.mean(axis=0), x.std(axis=0)
        x_std[x_std == 0] = 1.0
        self._x_mean, self._x_std = x_mean, x_std
        x_norm = (x - x_mean) / x_std

        net = self._build_net()
        x_t = torch.tensor(x_norm, dtype=torch.float32)
        y_t = torch.tensor(y, dtype=torch.float32).view(-1, 1)
        w_t = torch.tensor(sample_weight, dtype=torch.float32).view(-1, 1) if sample_weight is not None else None

        opt = torch.optim.Adam(net.parameters(), lr=self.lr)
        loss_fn = nn.BCEWithLogitsLoss(reduction="none" if w_t is not None else "mean")
        n = len(x_t)
        for _epoch in range(self.epochs):
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

        self._state_dict = {k: v.clone() for k, v in net.state_dict().items()}
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
            # .tolist() rather than .numpy(): real, confirmed bug hit locally
            # -- some torch CPU wheels (built against numpy 1.x) hard-refuse
            # the zero-copy numpy bridge under this project's numpy 2.x
            # ("RuntimeError: Numpy is not available"), not just a warning.
            # .tolist() converts via pybind11 directly, independent of numpy
            # ABI compatibility, so this works regardless of which torch/numpy
            # build combination actually lands on a given deploy.
            proba_up = np.asarray(torch.sigmoid(logits).view(-1).tolist())
        return np.column_stack([1.0 - proba_up, proba_up])

    def predict(self, x: np.ndarray) -> np.ndarray:
        return (self.predict_proba(x)[:, 1] >= 0.5).astype(int)


def train_torch_candidate_model(df: pd.DataFrame | None = None) -> dict[str, Any]:
    """Trains the custom PyTorch MLP candidate in complete isolation from
    train_model()'s existing sklearn candidates -- own data load, own
    chronological split, own fit -- and promotes it to the live model ONLY
    if it actually beats the currently-persisted model's freshly-recomputed
    score on the SAME holdout (the same "add as a candidate, let the
    evidence decide" discipline as _CANDIDATES, just run as a separate,
    low-frequency job instead of being stacked into the existing
    multi-candidate call -- see _TorchMLPClassifier's own docstring for why
    that isolation matters on this exact 512MB service). Never raises on
    ordinary "not enough data yet" conditions."""
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

    x_train, y_train = train_df[feature_cols].values, train_df["label_up"].values
    x_test, y_test = test_df[feature_cols].values, test_df["label_up"].values
    symbol_categories = list(train_df["symbol"].astype("category").cat.categories)
    n_rows = len(train_df) + len(test_df)
    del train_df, test_df

    try:
        torch_model = _TorchMLPClassifier(input_dim=len(feature_cols))
        torch_model.fit(x_train, y_train)
        torch_preds = torch_model.predict(x_test)
        torch_proba = torch_model.predict_proba(x_test)[:, 1]
        torch_score = (float(accuracy_score(y_test, torch_preds)) + float(roc_auc_score(y_test, torch_proba))) / 2.0
    except Exception as exc:
        logger.warning("[alpaca_model] torch candidate training failed: %s", exc)
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
            logger.warning("[alpaca_model] could not re-score current model against torch's holdout: %s", exc)

    promoted = current_score is None or torch_score > current_score
    result = {
        "ok": True, "promoted": promoted, "torch_score": torch_score,
        "current_score": current_score, "current_model_type": (current_meta or {}).get("model_type"),
        "rows": n_rows,
    }

    if promoted:
        meta = {
            "trained_at": time.time(), "model_type": "torch_mlp",
            "scores": {"torch_mlp": {"combined": torch_score}, "previous": {"combined": current_score}},
            "rows": n_rows, "feature_columns": feature_cols, "symbol_categories": symbol_categories,
            "feature_importances": None,
        }
        joblib.dump(torch_model, MODEL_PATH)
        MODEL_META_PATH.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        _model_cache.update({"model": torch_model, "meta": meta, "loaded_at": time.time()})
        _push_model_to_hf()
        result["meta"] = meta

    del x_test, torch_model
    gc.collect()
    return result


def _push_model_to_hf() -> None:
    if not HF_API_KEY:
        return
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
        try:
            api.repo_info(repo_id=HF_ALPACA_MODEL_REPO, repo_type="model")
        except Exception:
            api.create_repo(repo_id=HF_ALPACA_MODEL_REPO, repo_type="model", exist_ok=True, private=False)
        api.upload_file(
            path_or_fileobj=str(MODEL_PATH), path_in_repo="alpaca_model.joblib",
            repo_id=HF_ALPACA_MODEL_REPO, repo_type="model", commit_message="update alpaca direction model",
        )
        api.upload_file(
            path_or_fileobj=str(MODEL_META_PATH), path_in_repo="alpaca_model_meta.json",
            repo_id=HF_ALPACA_MODEL_REPO, repo_type="model", commit_message="update alpaca model metadata",
        )
    except Exception as exc:
        logger.warning("[alpaca_model] HF model push failed: %s", exc)


_MODEL_DOWNLOAD_HF_TIMEOUT_SEC = int(os.getenv("ALPACA_MODEL_DOWNLOAD_HF_TIMEOUT_SEC", "15") or "15")


def _download_model_from_hf() -> bool:
    if not HF_API_KEY:
        return False

    def _download() -> bool:
        from huggingface_hub import hf_hub_download
        model_path = hf_hub_download(
            repo_id=HF_ALPACA_MODEL_REPO, filename="alpaca_model.joblib", repo_type="model", token=HF_API_KEY,
        )
        meta_path = hf_hub_download(
            repo_id=HF_ALPACA_MODEL_REPO, filename="alpaca_model_meta.json", repo_type="model", token=HF_API_KEY,
        )
        MODEL_PATH.write_bytes(Path(model_path).read_bytes())
        MODEL_META_PATH.write_text(Path(meta_path).read_text(encoding="utf-8"), encoding="utf-8")
        return True

    try:
        # Real, confirmed production incident (same call shape, on perps):
        # called directly from /api/alpaca/status on every cold boot,
        # unbounded this can hang on huggingface_hub's own internal
        # session lock long enough to freeze this --workers 1 process
        # until gunicorn's worker timeout SIGKILLs it -- which wipes local
        # disk and guarantees the next boot hits this same unconditional
        # call again: a self-sustaining crash loop.
        from server_common import call_with_hard_timeout
        return bool(call_with_hard_timeout(_download, timeout_sec=_MODEL_DOWNLOAD_HF_TIMEOUT_SEC, on_timeout=False))
    except Exception as exc:
        logger.info("[alpaca_model] no model available on HF yet: %s", exc)
        return False


def _schedule_hf_model_recheck() -> None:
    from server_common import maybe_schedule_hf_model_recheck, refresh_model_if_hf_has_a_newer_one

    def _check() -> None:
        refresh_model_if_hf_has_a_newer_one(
            model_repo=HF_ALPACA_MODEL_REPO, token=HF_API_KEY, meta_filename="alpaca_model_meta.json",
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
        logger.warning("[alpaca_model] failed to load cached model: %s", exc)
        return None, None


def predict_direction(symbol: str) -> dict[str, Any]:
    """{"model_ok": False} if no trained model exists yet (expected during
    the first days of data collection -- callers fall back to
    technical-only signals in that case)."""
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
