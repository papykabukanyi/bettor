"""Direction classifier for Alpaca-traded crypto pairs -- structurally
identical to alpaca_model.py (equities) and perps_model.py (Kalshi perps):
same chronological-holdout-never-random-shuffle discipline, same candidate
comparison, same feature-importance surfacing. Trained on
alpaca_crypto_data's features/labels and persisted to
HF_ALPACA_CRYPTO_MODEL_REPO. Never touches any Kalshi perps or equities
state.
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
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score

from data.alpaca_crypto_data import FEATURE_COLUMNS, latest_feature_row, load_training_dataset

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MODEL_PATH = DATA_DIR / "alpaca_crypto_model.joblib"
MODEL_META_PATH = DATA_DIR / "alpaca_crypto_model_meta.json"

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_ALPACA_CRYPTO_MODEL_REPO = os.getenv("HF_ALPACA_CRYPTO_MODEL_REPO", "papylove/alpaca-crypto-model")

MIN_TRAIN_ROWS = int(os.getenv("ALPACA_CRYPTO_MIN_TRAIN_ROWS", "300") or "300")
MODEL_CACHE_TTL_SEC = int(os.getenv("ALPACA_CRYPTO_MODEL_CACHE_TTL_SEC", "1800") or "1800")

_model_cache: dict[str, Any] = {"model": None, "meta": None, "loaded_at": 0.0}

# n_jobs=1 (not -1): same reasoning as every other model here -- avoid
# multiplying peak memory via RandomForest's per-worker process forking on
# a memory-constrained deployment.
#
# n_estimators=60 (not 100): real, confirmed production OOM incidents on
# this exact service (Render's own events: 3 oomKilled restarts, roughly
# a day apart) -- this file had never picked up the same n_estimators=
# 100->60 reduction perps_model.py/alpaca_options_model.py/alpaca_model.py
# already proved necessary for an identical multi-candidate-fit-in-one-call
# shape on the same 512MB ceiling.
_CANDIDATES = {
    "logistic_regression": lambda: LogisticRegression(max_iter=1000, class_weight="balanced"),
    "random_forest": lambda: RandomForestClassifier(
        n_estimators=60, max_depth=6, min_samples_leaf=20, class_weight="balanced", random_state=42, n_jobs=1,
    ),
    "gradient_boosting": lambda: GradientBoostingClassifier(
        n_estimators=60, max_depth=3, learning_rate=0.05, random_state=42,
    ),
}


def _feature_importance_map(model: Any, feature_cols: list[str]) -> dict[str, float] | None:
    """Surfaces which features the trained model actually leaned on --
    e.g. how much weight sentiment_score carried relative to the technical
    features -- same helper already proven on the perps and Alpaca-equity
    sides."""
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
    """Train, compare candidates on a chronological holdout, keep the best,
    persist locally + to HF_ALPACA_CRYPTO_MODEL_REPO. Never raises on
    ordinary "not enough data yet" conditions."""
    frame = df if df is not None else load_training_dataset()
    if frame.empty:
        return {"ok": False, "reason": "no_data"}

    labeled = _prepare_training_frame(frame)
    if len(labeled) < MIN_TRAIN_ROWS:
        return {"ok": False, "reason": "insufficient_rows", "rows": len(labeled), "need": MIN_TRAIN_ROWS}

    feature_cols = FEATURE_COLUMNS + ["symbol_code"]
    split_idx = int(len(labeled) * 0.8)
    train_df, test_df = labeled.iloc[:split_idx], labeled.iloc[split_idx:]
    if train_df.empty or test_df.empty or test_df["label_up"].nunique() < 2:
        return {"ok": False, "reason": "insufficient_class_variety", "rows": len(labeled)}

    x_train, y_train = train_df[feature_cols].values, train_df["label_up"].values
    x_test, y_test = test_df[feature_cols].values, test_df["label_up"].values

    best_name, best_model, best_score = None, None, -1.0
    scores: dict[str, dict[str, float]] = {}
    for name, factory in _CANDIDATES.items():
        try:
            model = factory()
            model.fit(x_train, y_train)
            preds = model.predict(x_test)
            proba = model.predict_proba(x_test)[:, 1]
            acc = float(accuracy_score(y_test, preds))
            auc = float(roc_auc_score(y_test, proba)) if len(set(y_test)) > 1 else 0.5
            scores[name] = {"accuracy": acc, "auc": auc}
            combined = (acc + auc) / 2.0
            if combined > best_score:
                best_name, best_model, best_score = name, model, combined
        except Exception as exc:
            logger.warning("[alpaca_crypto_model] candidate %s failed: %s", name, exc)

    if best_model is None:
        return {"ok": False, "reason": "all_candidates_failed"}

    best_model.fit(labeled[feature_cols].values, labeled["label_up"].values)

    symbol_categories = list(labeled["symbol"].astype("category").cat.categories)
    meta = {
        "trained_at": time.time(), "model_type": best_name, "scores": scores,
        "rows": len(labeled), "feature_columns": feature_cols, "symbol_categories": symbol_categories,
        "feature_importances": _feature_importance_map(best_model, feature_cols),
    }

    joblib.dump(best_model, MODEL_PATH)
    MODEL_META_PATH.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    _model_cache.update({"model": best_model, "meta": meta, "loaded_at": time.time()})

    _push_model_to_hf()
    # Confirmed real recurring OOM on this exact service (512MB, running
    # this training job for BOTH the stock and crypto strategies in the
    # same process) -- freeing the training frame/arrays and forcing a
    # collection here mirrors the same real fix already proven on the
    # perps side, not a guess.
    del frame, labeled, train_df, test_df, x_train, x_test
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
        torch.set_num_threads(1)  # avoid thread-multiplication memory, same discipline as sklearn's n_jobs=1 above

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
        logger.warning("[alpaca_crypto_model] torch candidate training failed: %s", exc)
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
            logger.warning("[alpaca_crypto_model] could not re-score current model against torch's holdout: %s", exc)

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
            api.repo_info(repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, repo_type="model")
        except Exception:
            api.create_repo(repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, repo_type="model", exist_ok=True, private=False)
        api.upload_file(
            path_or_fileobj=str(MODEL_PATH), path_in_repo="alpaca_crypto_model.joblib",
            repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, repo_type="model", commit_message="update alpaca crypto direction model",
        )
        api.upload_file(
            path_or_fileobj=str(MODEL_META_PATH), path_in_repo="alpaca_crypto_model_meta.json",
            repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, repo_type="model", commit_message="update alpaca crypto model metadata",
        )
    except Exception as exc:
        logger.warning("[alpaca_crypto_model] HF model push failed: %s", exc)


_MODEL_DOWNLOAD_HF_TIMEOUT_SEC = int(os.getenv("ALPACA_CRYPTO_MODEL_DOWNLOAD_HF_TIMEOUT_SEC", "15") or "15")


def _download_model_from_hf() -> bool:
    if not HF_API_KEY:
        return False

    def _download() -> bool:
        from huggingface_hub import hf_hub_download
        model_path = hf_hub_download(
            repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, filename="alpaca_crypto_model.joblib", repo_type="model", token=HF_API_KEY,
        )
        meta_path = hf_hub_download(
            repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, filename="alpaca_crypto_model_meta.json", repo_type="model", token=HF_API_KEY,
        )
        MODEL_PATH.write_bytes(Path(model_path).read_bytes())
        MODEL_META_PATH.write_text(Path(meta_path).read_text(encoding="utf-8"), encoding="utf-8")
        return True

    try:
        # Real, confirmed production incident (same call shape, on perps):
        # called directly from /api/alpaca/crypto/status on every cold
        # boot, unbounded this can hang on huggingface_hub's own internal
        # session lock long enough to freeze this --workers 1 process
        # until gunicorn's worker timeout SIGKILLs it -- which wipes local
        # disk and guarantees the next boot hits this same unconditional
        # call again: a self-sustaining crash loop.
        from server_common import call_with_hard_timeout
        return bool(call_with_hard_timeout(_download, timeout_sec=_MODEL_DOWNLOAD_HF_TIMEOUT_SEC, on_timeout=False))
    except Exception as exc:
        logger.info("[alpaca_crypto_model] no model available on HF yet: %s", exc)
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
        logger.warning("[alpaca_crypto_model] failed to load cached model: %s", exc)
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
