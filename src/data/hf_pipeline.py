"""
HF-first pipeline (no DB dependency):
1) One-time bootstrap of historical game results -> HF dataset
2) Daily append of new results -> same HF dataset
3) Daily retrain on full HF dataset -> HF model repo
4) Predict from HF model artifact or HF inference API
"""

from __future__ import annotations

import datetime
import json
import os
import tempfile
from dataclasses import dataclass

import requests

from data.hf_uploader import HFUploader


@dataclass
class TrainSummary:
    repo_id: str
    rows: int
    best_model: str
    cv_roc_auc: float
    trained_at: str


class HFDirectPipeline:
    FINAL_STATES = {"Final", "Game Over", "Completed Early", "Completed"}
    UPCOMING_STATES = {"Preview", "Pre-Game", "Scheduled", "Warmup"}

    def __init__(self, token: str | None = None, dataset_repo: str | None = None, model_repo: str | None = None):
        try:
            from config import HF_API_KEY, HF_DATASET_REPO, HF_MODEL_REPO
        except Exception:
            HF_API_KEY = os.getenv("HF_API_KEY", "")
            HF_DATASET_REPO = os.getenv("HF_DATASET_REPO", "sportprediction")
            HF_MODEL_REPO = os.getenv("HF_MODEL_REPO", "sports-win-model")

        self.token = str(token or HF_API_KEY or "").strip()
        self.uploader = HFUploader(token=self.token, repo_name=dataset_repo or HF_DATASET_REPO)
        self.dataset_repo_id = getattr(self.uploader, "_repo_id", "")
        self.model_repo_name = str(model_repo or HF_MODEL_REPO or "sports-win-model").strip()
        self.model_repo_id = self.model_repo_name
        self._data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data")
        self._status_file = os.getenv(
            "HF_PIPELINE_STATUS_FILE",
            os.path.join(self._data_dir, "hf_pipeline_status.json"),
        )
        self._api = None
        self._ok = bool(self.token and self.uploader and getattr(self.uploader, "_ok", False))

        if self._ok:
            try:
                from huggingface_hub import HfApi
                self._api = HfApi(token=self.token)
                who = self._api.whoami() or {}
                user = str(who.get("name") or "").strip()
                if user and "/" not in self.model_repo_id:
                    self.model_repo_id = f"{user}/{self.model_repo_id}"
            except Exception:
                self._ok = False

    @property
    def ok(self) -> bool:
        return self._ok

    def bootstrap_one_year_history(self, days_back: int = 365) -> dict:
        end = datetime.date.today()
        start = end - datetime.timedelta(days=max(1, int(days_back)))
        records = self._clean_game_records(self._fetch_completed_games(start, end))
        if not records:
            self._write_status({"last_step": "bootstrap", "ok": False, "message": "No historical records found"})
            return {"ok": False, "msg": "No historical records found", "records": 0}
        self.uploader.push_records("games", records)
        self.uploader.flush_all()
        result = {"ok": True, "msg": "Historical data uploaded", "records": len(records), "dataset_repo": self.dataset_repo_id}
        self._write_status({"last_step": "bootstrap", "ok": True, "historical_records": len(records), "dataset_repo": self.dataset_repo_id})
        return result

    def append_daily_results(self, day: datetime.date | None = None) -> dict:
        target = day or datetime.date.today()
        records = self._clean_game_records(self._fetch_completed_games(target, target))
        if not records:
            self._write_status({"last_step": "append_daily", "ok": True, "append_records": 0, "append_date": target.isoformat()})
            return {"ok": True, "msg": "No completed games yet for day", "records": 0, "date": target.isoformat()}
        self.uploader.push_records("games", records)
        self.uploader.flush_all()
        result = {"ok": True, "msg": "Daily results appended", "records": len(records), "date": target.isoformat()}
        self._write_status({"last_step": "append_daily", "ok": True, "append_records": len(records), "append_date": target.isoformat()})
        return result

    def train_and_publish_best_model(self, min_rows: int = 200, forced_model: str = "auto") -> TrainSummary:
        from datasets import load_dataset
        import joblib
        import pandas as pd
        from sklearn.compose import ColumnTransformer
        from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import StratifiedKFold, cross_val_score
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder

        if not self._ok or not self._api:
            raise RuntimeError("HF pipeline is not configured. Set HF_API_KEY.")
        ds = load_dataset(self.dataset_repo_id, "games", split="train")
        df = ds.to_pandas()

        if df.empty:
            raise RuntimeError("HF dataset has no rows in games/train")

        for col in ("home_score", "away_score"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["home_team", "away_team", "home_score", "away_score"])
        df = df[df["home_score"] != df["away_score"]].copy()
        if len(df) < min_rows:
            raise RuntimeError(f"Not enough rows to train: {len(df)} < {min_rows}")

        df["season"] = pd.to_numeric(df.get("season"), errors="coerce").fillna(0).astype(int)
        y = (df["home_score"] > df["away_score"]).astype(int)
        X = df[["home_team", "away_team", "season"]].copy()

        pre = ColumnTransformer(
            transformers=[
                ("teams", OneHotEncoder(handle_unknown="ignore"), ["home_team", "away_team"]),
                ("season", "passthrough", ["season"]),
            ]
        )
        candidates = {
            "logistic_regression": LogisticRegression(max_iter=2000),
            "random_forest": RandomForestClassifier(n_estimators=300, random_state=42),
            "gradient_boosting": GradientBoostingClassifier(random_state=42),
        }
        forced_key = str(forced_model or "auto").strip().lower()
        if forced_key and forced_key != "auto":
            if forced_key not in candidates:
                raise RuntimeError(f"Unknown custom model: {forced_key}")
            candidates = {forced_key: candidates[forced_key]}
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

        best_name = ""
        best_score = -1.0
        best_pipeline = None
        for name, model in candidates.items():
            pipe = Pipeline([("pre", pre), ("model", model)])
            scores = cross_val_score(pipe, X, y, cv=cv, scoring="roc_auc")
            mean_score = float(scores.mean())
            if mean_score > best_score:
                best_score = mean_score
                best_name = name
                best_pipeline = pipe

        if best_pipeline is None:
            raise RuntimeError("Could not select a model candidate")
        best_pipeline.fit(X, y)

        trained_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        metadata = {
            "trained_at": trained_at,
            "rows": int(len(df)),
            "best_model": best_name,
            "cv_roc_auc": round(best_score, 6),
            "dataset_repo": self.dataset_repo_id,
            "features": ["home_team", "away_team", "season"],
            "target": "home_win",
            "forced_model": forced_key if forced_key != "auto" else "",
        }

        with tempfile.TemporaryDirectory(prefix="hf_model_") as td:
            model_path = os.path.join(td, "model.joblib")
            meta_path = os.path.join(td, "metadata.json")
            readme_path = os.path.join(td, "README.md")
            joblib.dump(best_pipeline, model_path)
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)
            with open(readme_path, "w", encoding="utf-8") as f:
                f.write(
                    "# Sports Win Model\n\n"
                    f"- best_model: {best_name}\n"
                    f"- cv_roc_auc: {best_score:.4f}\n"
                    f"- rows: {len(df)}\n"
                    f"- trained_at: {trained_at}\n"
                )

            self._api.create_repo(repo_id=self.model_repo_id, repo_type="model", exist_ok=True, private=False)
            self._api.upload_file(
                path_or_fileobj=model_path,
                path_in_repo="model.joblib",
                repo_id=self.model_repo_id,
                repo_type="model",
                commit_message=f"Update model ({best_name})",
            )
            self._api.upload_file(
                path_or_fileobj=meta_path,
                path_in_repo="metadata.json",
                repo_id=self.model_repo_id,
                repo_type="model",
                commit_message="Update model metadata",
            )
            self._api.upload_file(
                path_or_fileobj=readme_path,
                path_in_repo="README.md",
                repo_id=self.model_repo_id,
                repo_type="model",
                commit_message="Update model card",
            )

        summary = TrainSummary(
            repo_id=self.model_repo_id,
            rows=int(len(df)),
            best_model=best_name,
            cv_roc_auc=float(best_score),
            trained_at=trained_at,
        )
        self._write_status(
            {
                "last_step": "train_publish",
                "ok": True,
                "trained_rows": int(len(df)),
                "best_model": best_name,
                "cv_roc_auc": round(float(best_score), 6),
                "model_repo": self.model_repo_id,
                "trained_at": trained_at,
            }
        )
        return summary

    def predict_from_model_repo(self, home_team: str, away_team: str, season: int | None = None) -> dict:
        import joblib
        import pandas as pd
        from huggingface_hub import hf_hub_download

        season_val = int(season or datetime.date.today().year)
        model_path = hf_hub_download(repo_id=self.model_repo_id, filename="model.joblib", repo_type="model", token=self.token)
        model = joblib.load(model_path)
        row = pd.DataFrame([{"home_team": home_team, "away_team": away_team, "season": season_val}])
        probs = model.predict_proba(row)[0]
        home_prob = float(probs[1])
        return {
            "home_team": home_team,
            "away_team": away_team,
            "season": season_val,
            "home_win_prob": round(home_prob, 4),
            "away_win_prob": round(1.0 - home_prob, 4),
            "model_repo": self.model_repo_id,
        }

    def predict_daily_schedule(
        self,
        day: datetime.date | None = None,
        output_path: str | None = None,
        via_api: bool = False,
        model_id: str | None = None,
        endpoint_url: str | None = None,
    ) -> dict:
        target = day or datetime.date.today()
        games = self._fetch_upcoming_games(target)
        predictions: list[dict] = []
        for g in games:
            home_team = str(g.get("home_team") or "").strip()
            away_team = str(g.get("away_team") or "").strip()
            if not home_team or not away_team:
                continue
            try:
                if via_api:
                    pred = self.predict_via_hf_api(
                        home_team=home_team,
                        away_team=away_team,
                        season=int(target.year),
                        model_id=model_id,
                        endpoint_url=endpoint_url,
                    )
                    pred_payload = {"home_team": home_team, "away_team": away_team, "api_response": pred.get("response")}
                else:
                    pred_payload = self.predict_from_model_repo(home_team=home_team, away_team=away_team, season=target.year)
                pred_payload["game_date"] = target.isoformat()
                pred_payload["game_time"] = str(g.get("game_time") or "")
                pred_payload["status"] = str(g.get("status") or "")
                predictions.append(pred_payload)
            except Exception as exc:
                predictions.append(
                    {
                        "home_team": home_team,
                        "away_team": away_team,
                        "game_date": target.isoformat(),
                        "error": str(exc),
                    }
                )

        out_path = output_path or os.path.join(self._data_dir, "hf_daily_predictions.json")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        payload = {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "date": target.isoformat(),
            "prediction_count": len(predictions),
            "predictions": predictions,
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        self._write_status(
            {
                "last_step": "predict_daily",
                "ok": True,
                "prediction_date": target.isoformat(),
                "prediction_count": len(predictions),
                "prediction_file": out_path,
                "predict_mode": "api" if via_api else "model_artifact",
            }
        )
        return {"ok": True, "prediction_count": len(predictions), "output_file": out_path, "date": target.isoformat()}

    def run_daily_pipeline(
        self,
        custom_model: str = "auto",
        min_rows: int = 200,
        predictions_output_path: str | None = None,
        via_api: bool = False,
        model_id: str | None = None,
        endpoint_url: str | None = None,
    ) -> dict:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        today = datetime.date.today()
        append_y = self.append_daily_results(yesterday)
        append_t = self.append_daily_results(today)
        summary = self.train_and_publish_best_model(min_rows=min_rows, forced_model=custom_model)
        preds = self.predict_daily_schedule(
            day=today,
            output_path=predictions_output_path,
            via_api=via_api,
            model_id=model_id,
            endpoint_url=endpoint_url,
        )
        result = {
            "ok": True,
            "append_yesterday": append_y,
            "append_today": append_t,
            "train": {
                "repo_id": summary.repo_id,
                "rows": summary.rows,
                "best_model": summary.best_model,
                "cv_roc_auc": summary.cv_roc_auc,
                "trained_at": summary.trained_at,
            },
            "predictions": preds,
        }
        self._write_status({"last_step": "daily_pipeline", "ok": True, "daily_result": result})
        return result

    def predict_via_hf_api(
        self,
        home_team: str,
        away_team: str,
        season: int | None = None,
        model_id: str | None = None,
        endpoint_url: str | None = None,
    ) -> dict:
        url = str(endpoint_url or "").strip()
        if not url:
            model_ref = str(model_id or self.model_repo_id).strip()
            url = f"https://api-inference.huggingface.co/models/{model_ref}"

        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        payload = {
            "inputs": {
                "home_team": home_team,
                "away_team": away_team,
                "season": int(season or datetime.date.today().year),
            }
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return {"url": url, "response": data}

    def _fetch_completed_games(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        current = start
        while current <= end:
            day = current.isoformat()
            try:
                resp = requests.get(
                    "https://statsapi.mlb.com/api/v1/schedule",
                    params={"sportId": 1, "date": day, "hydrate": "linescore", "gameType": "R"},
                    timeout=25,
                )
                resp.raise_for_status()
                payload = resp.json() or {}
                for date_entry in payload.get("dates", []):
                    for game in date_entry.get("games", []):
                        status = str(
                            (game.get("status", {}) or {}).get("detailedState")
                            or (game.get("status", {}) or {}).get("abstractGameState")
                            or ""
                        )
                        if status not in self.FINAL_STATES:
                            continue
                        teams = game.get("teams") or {}
                        home = teams.get("home") or {}
                        away = teams.get("away") or {}
                        home_team = str(((home.get("team") or {}).get("name")) or "").strip()
                        away_team = str(((away.get("team") or {}).get("name")) or "").strip()
                        home_score = home.get("score")
                        away_score = away.get("score")
                        if not home_team or not away_team or home_score is None or away_score is None:
                            continue
                        rows.append(
                            {
                                "game_id": str(game.get("gamePk") or ""),
                                "sport": "mlb",
                                "league": "MLB",
                                "game_date": day,
                                "game_datetime": str(game.get("gameDate") or ""),
                                "status": status,
                                "home_team": home_team,
                                "away_team": away_team,
                                "home_score": float(home_score),
                                "away_score": float(away_score),
                                "home_starter": "",
                                "away_starter": "",
                                "season": int(day[:4]),
                                "metadata": "{}",
                                "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                            }
                        )
            except Exception:
                pass
            current += datetime.timedelta(days=1)
        return rows

    def _fetch_upcoming_games(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        try:
            resp = requests.get(
                "https://statsapi.mlb.com/api/v1/schedule",
                params={"sportId": 1, "date": day.isoformat(), "gameType": "R"},
                timeout=25,
            )
            resp.raise_for_status()
            payload = resp.json() or {}
            for date_entry in payload.get("dates", []):
                for game in date_entry.get("games", []):
                    status = str(
                        (game.get("status", {}) or {}).get("detailedState")
                        or (game.get("status", {}) or {}).get("abstractGameState")
                        or ""
                    )
                    if status not in self.UPCOMING_STATES:
                        continue
                    teams = game.get("teams") or {}
                    home_team = str((((teams.get("home") or {}).get("team") or {}).get("name") or "")).strip()
                    away_team = str((((teams.get("away") or {}).get("team") or {}).get("name") or "")).strip()
                    if not home_team or not away_team:
                        continue
                    rows.append(
                        {
                            "home_team": home_team,
                            "away_team": away_team,
                            "status": status,
                            "game_time": str(game.get("gameDate") or ""),
                        }
                    )
        except Exception:
            pass
        return rows

    def _clean_game_records(self, records: list[dict]) -> list[dict]:
        cleaned: list[dict] = []
        seen: set[str] = set()
        for raw in records or []:
            if not isinstance(raw, dict):
                continue
            game_id = str(raw.get("game_id") or "").strip()
            home_team = " ".join(str(raw.get("home_team") or "").strip().split())
            away_team = " ".join(str(raw.get("away_team") or "").strip().split())
            game_date = str(raw.get("game_date") or "").strip()
            if not home_team or not away_team or not game_date:
                continue
            if home_team.lower() == away_team.lower():
                continue
            dedupe_key = game_id or f"{game_date}|{away_team}|{home_team}"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            row = dict(raw)
            row["game_id"] = game_id
            row["home_team"] = home_team[:120]
            row["away_team"] = away_team[:120]
            row["league"] = str(row.get("league") or "MLB").strip()[:80]
            row["sport"] = str(row.get("sport") or "mlb").strip().lower()[:32]
            row["status"] = str(row.get("status") or "").strip()[:80]
            row["game_date"] = game_date[:10]
            row["game_datetime"] = str(row.get("game_datetime") or "").strip()[:40]
            row["season"] = int(str(row.get("season") or game_date[:4] or datetime.date.today().year))
            row["metadata"] = str(row.get("metadata") or "{}")
            row["created_at"] = str(row.get("created_at") or datetime.datetime.now(datetime.timezone.utc).isoformat())
            try:
                row["home_score"] = float(row.get("home_score"))
                row["away_score"] = float(row.get("away_score"))
            except Exception:
                continue
            cleaned.append(row)
        return cleaned

    def _write_status(self, patch: dict) -> None:
        os.makedirs(os.path.dirname(self._status_file), exist_ok=True)
        base = {}
        try:
            if os.path.exists(self._status_file):
                with open(self._status_file, "r", encoding="utf-8") as f:
                    base = json.load(f) or {}
        except Exception:
            base = {}
        base.update(patch or {})
        base["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        base["dataset_repo"] = self.dataset_repo_id
        base["model_repo"] = self.model_repo_id
        with open(self._status_file, "w", encoding="utf-8") as f:
            json.dump(base, f, indent=2)
