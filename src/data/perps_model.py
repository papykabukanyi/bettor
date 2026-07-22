"""Direction classifier: given current multi-timeframe technical features +
news sentiment for a perp instrument, predict whether its price will be
higher or lower `PERPS_LABEL_HORIZON_MINUTES` (default 30) from now.

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

from data.perps_data import FEATURE_COLUMNS, latest_feature_row, load_training_dataset

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

_model_cache: dict[str, Any] = {"model": None, "meta": None, "loaded_at": 0.0}

_CANDIDATES = {
    "logistic_regression": lambda: LogisticRegression(max_iter=1000, class_weight="balanced"),
    "random_forest": lambda: RandomForestClassifier(
        n_estimators=200, max_depth=6, min_samples_leaf=20, class_weight="balanced", random_state=42,
    ),
    "gradient_boosting": lambda: GradientBoostingClassifier(
        n_estimators=150, max_depth=3, learning_rate=0.05, random_state=42,
    ),
}


def _prepare_training_frame(df: pd.DataFrame) -> pd.DataFrame:
    labeled = df.dropna(subset=["label_up"] + FEATURE_COLUMNS).copy()
    labeled["label_up"] = labeled["label_up"].astype(int)
    labeled["ticker_code"] = labeled["ticker"].astype("category").cat.codes
    return labeled.sort_values("ts").reset_index(drop=True)


def train_model(df: pd.DataFrame | None = None) -> dict[str, Any]:
    """Train, compare candidates on a chronological holdout, keep the best,
    persist locally + to HF. Returns a summary dict either way (never raises
    on ordinary "not enough data yet" conditions -- that's expected during
    the first days of data collection)."""
    frame = df if df is not None else load_training_dataset()
    if frame.empty:
        return {"ok": False, "reason": "no_data"}

    labeled = _prepare_training_frame(frame)
    if len(labeled) < MIN_TRAIN_ROWS:
        return {"ok": False, "reason": "insufficient_rows", "rows": len(labeled), "need": MIN_TRAIN_ROWS}

    feature_cols = FEATURE_COLUMNS + ["ticker_code"]
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
            logger.warning("[perps_model] candidate %s failed: %s", name, exc)

    if best_model is None:
        return {"ok": False, "reason": "all_candidates_failed"}

    # Refit the winner on the full labeled dataset before shipping it.
    best_model.fit(labeled[feature_cols].values, labeled["label_up"].values)

    ticker_categories = list(labeled["ticker"].astype("category").cat.categories)
    meta = {
        "trained_at": time.time(),
        "model_type": best_name,
        "scores": scores,
        "rows": len(labeled),
        "feature_columns": feature_cols,
        "ticker_categories": ticker_categories,
        "label_horizon_minutes": int(os.getenv("PERPS_LABEL_HORIZON_MINUTES", "30") or "30"),
    }

    joblib.dump(best_model, MODEL_PATH)
    MODEL_META_PATH.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    _model_cache.update({"model": best_model, "meta": meta, "loaded_at": time.time()})

    _push_model_to_hf()
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
            api.create_repo(repo_id=HF_MODEL_REPO, repo_type="model", exist_ok=True, private=True)
        api.upload_file(
            path_or_fileobj=str(MODEL_PATH), path_in_repo="perps_model.joblib",
            repo_id=HF_MODEL_REPO, repo_type="model", commit_message="update perps direction model",
        )
        api.upload_file(
            path_or_fileobj=str(MODEL_META_PATH), path_in_repo="perps_model_meta.json",
            repo_id=HF_MODEL_REPO, repo_type="model", commit_message="update perps model metadata",
        )
    except Exception as exc:
        logger.warning("[perps_model] HF model push failed: %s", exc)


def _download_model_from_hf() -> bool:
    if not HF_API_KEY:
        return False
    try:
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
