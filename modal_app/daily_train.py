from __future__ import annotations

import json
from dataclasses import dataclass

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from modal_app import common

app = common.make_app("bettor-daily-train")


@dataclass
class TrainingArtifact:
    pipeline: Pipeline
    metadata: dict


def _build_training_frame(records: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(records)
    if frame.empty:
        return frame
    frame["home_score"] = pd.to_numeric(frame.get("home_score"), errors="coerce")
    frame["away_score"] = pd.to_numeric(frame.get("away_score"), errors="coerce")
    frame = frame.dropna(subset=["home_team", "away_team", "home_score", "away_score", "game_date"]).copy()
    frame = frame[frame["home_score"] != frame["away_score"]].copy()
    frame["sport"] = frame.get("sport", "mlb").fillna("mlb").astype(str).str.lower()
    frame["league"] = frame.get("league", "").fillna("").astype(str)
    game_dates = pd.to_datetime(frame["game_date"], errors="coerce")
    frame["season"] = pd.to_numeric(frame.get("season"), errors="coerce").fillna(game_dates.dt.year.fillna(0)).astype(int)
    frame["month"] = game_dates.dt.month.fillna(6).astype(int)
    frame["day_of_week"] = game_dates.dt.dayofweek.fillna(0).astype(int)
    frame = frame.dropna(subset=["home_team", "away_team"]).copy()
    return frame


def run_daily_train() -> dict:
    common.ensure_directories()
    records = common.load_history_records()
    frame = _build_training_frame(records)
    if frame.empty:
        raise RuntimeError("No completed games available for training.")

    y = (frame["home_score"] > frame["away_score"]).astype(int)
    class_counts = y.value_counts()
    if len(class_counts) < 2:
        raise RuntimeError("Training data only contains one class; collect more historical results.")

    split_count = int(min(5, class_counts.min()))
    if split_count < 2:
        raise RuntimeError("Not enough class balance for 5-fold training; collect more completed games.")

    X = frame[common.FEATURE_COLUMNS].copy()
    preprocessor = ColumnTransformer(
        transformers=[
            ("categorical", OneHotEncoder(handle_unknown="ignore"), common.CAT_COLUMNS),
            ("numerical", "passthrough", common.NUM_COLUMNS),
        ]
    )
    candidates = {
        "logistic_regression": LogisticRegression(max_iter=2500, random_state=42),
        "random_forest": RandomForestClassifier(n_estimators=350, random_state=42, n_jobs=-1),
        "gradient_boosting": GradientBoostingClassifier(random_state=42),
    }
    cv = StratifiedKFold(n_splits=split_count, shuffle=True, random_state=42)

    best_name = ""
    best_score = -1.0
    best_pipeline: Pipeline | None = None
    candidate_scores: dict[str, float] = {}

    for name, estimator in candidates.items():
        pipeline = Pipeline([("preprocessor", preprocessor), ("model", estimator)])
        scores = cross_val_score(pipeline, X, y, cv=cv, scoring="roc_auc")
        score = float(scores.mean())
        candidate_scores[name] = round(score, 6)
        if score > best_score:
            best_score = score
            best_name = name
            best_pipeline = pipeline

    if best_pipeline is None:
        raise RuntimeError("No training candidate succeeded.")

    best_pipeline.fit(X, y)
    trained_at = common.now_utc_iso()
    version = trained_at[:19].replace(":", "-").replace("T", "_")
    metadata = {
        "ok": True,
        "trained_at": trained_at,
        "version": version,
        "rows": int(len(frame)),
        "best_model": best_name,
        "best_score": round(best_score, 6),
        "candidate_scores": candidate_scores,
        "candidate_count": len(candidates),
        "sports_covered": sorted(frame["sport"].dropna().astype(str).unique().tolist()),
        "features": common.FEATURE_COLUMNS,
        "cv_folds": split_count,
    }
    artifact = TrainingArtifact(pipeline=best_pipeline, metadata=metadata)
    joblib.dump({"pipeline": artifact.pipeline, "metadata": artifact.metadata}, common.path_for("models", "best_model.joblib"))
    common.save_model_stats(metadata)

    history = common.load_training_history()
    history.append(metadata)
    common.save_training_history(history)

    summary = {
        **metadata,
        "updated_at": common.now_utc_iso(),
    }
    common.save_json(common.path_for("pipeline", "train_summary.json"), summary)
    common.update_pipeline_status("train", summary)
    return summary


@app.function(
    image=common.image,
    volumes={common.REMOTE_VOLUME_MOUNT: common.volume},
    secrets=common.modal_secrets(),
    schedule=common.modal.Cron("0 3 * * *", timezone="America/New_York"),
    timeout=60 * 30,
)
def daily_train() -> dict:
    result = run_daily_train()
    common.commit_volume()
    return result


@app.local_entrypoint()
def main():
    print(json.dumps(run_daily_train(), indent=2))
