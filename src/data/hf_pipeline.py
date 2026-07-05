"""
HF-first pipeline: automatic multi-sport predictions via HuggingFace.

Pipeline stages (all automatic from deployment):
  1. bootstrap_one_year_history() — One-time: load 1yr MLB/NBA/NHL/Soccer -> HF Dataset
  2. append_daily_results()       — Daily: append completed games -> HF Dataset
  3. train_and_publish_best_model() — Daily: retrain on full dataset -> HF Model Hub
  4. predict_daily_schedule()     — Daily: generate today+tomorrow predictions
  5. run_daily_pipeline()         — Orchestrates steps 2-4

Record IDs: every game record gets a UUID4 `record_id`.
Prediction IDs: every prediction gets a UUID4 `prediction_id`.
Sports: MLB (statsapi.mlb.com free), NBA (balldontlie.io free),
        NHL (api-web.nhle.com free), Soccer (thesportsdb free key "1").
Features: home_team, away_team, sport, season, month, day_of_week.
"""

from __future__ import annotations

import datetime
import io
import json
import logging
import os
import re
import tempfile
import time
from dataclasses import dataclass, field
from uuid import uuid4

import requests

from data.hf_uploader import HFUploader

logger = logging.getLogger(__name__)


def _now_utc() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


@dataclass
class TrainSummary:
    repo_id: str
    rows: int
    best_model: str
    cv_roc_auc: float
    trained_at: str
    version: str = ""
    features: list = field(default_factory=list)
    sports_covered: list = field(default_factory=list)


class HFDirectPipeline:
    FINAL_STATES = frozenset(
        {"Final", "Game Over", "Completed Early", "Completed", "F", "STATUS_FINAL", "OFF", "FINAL", "OVER"}
    )
    UPCOMING_STATES = frozenset(
        {"Preview", "Pre-Game", "Scheduled", "Warmup", "Pre-game", "Sched", "FUT", "NS", "Pre-Preview"}
    )
    _CONFIDENCE_TIERS = [(0.70, "elite"), (0.60, "solid"), (0.55, "lean"), (0.0, "uncertain")]
    _TRAIN_FEATURES = ["home_team", "away_team", "sport", "season", "month", "day_of_week"]
    _CAT_FEATURES = ["home_team", "away_team", "sport"]
    _NUM_FEATURES = ["season", "month", "day_of_week"]
    _POLYMARKET_SPORT_ALIASES = {
        "baseball": "mlb",
        "mlb": "mlb",
        "basketball": "nba",
        "nba": "nba",
        "hockey": "nhl",
        "nhl": "nhl",
        "soccer": "soccer",
        "football": "soccer",
        "tennis": "tennis",
        "golf": "golf",
    }

    def __init__(
        self,
        token: str | None = None,
        dataset_repo: str | None = None,
        model_repo: str | None = None,
    ):
        try:
            from config import (
                HF_API_KEY,
                HF_DATASET_REPO,
                HF_MODEL_REPO,
                FOOTBALL_DATA_API_KEY,
                TENNIS_JEFF_SACKMANN_DIR,
            )
        except Exception:
            HF_API_KEY = os.getenv("HF_API_KEY", "")
            HF_DATASET_REPO = os.getenv("HF_DATASET_REPO", "sportprediction")
            HF_MODEL_REPO = os.getenv("HF_MODEL_REPO", "sports-win-model")
            FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "")
            TENNIS_JEFF_SACKMANN_DIR = os.getenv("TENNIS_JEFF_SACKMANN_DIR", "")

        self.token = str(token or HF_API_KEY or "").strip()
        self.football_data_api_key = str(FOOTBALL_DATA_API_KEY or "").strip()
        self.tennis_sackmann_dir = str(TENNIS_JEFF_SACKMANN_DIR or "").strip()
        self.uploader = HFUploader(token=self.token, repo_name=dataset_repo or HF_DATASET_REPO)
        self.dataset_repo_id = getattr(self.uploader, "_repo_id", "")
        self.model_repo_name = str(model_repo or HF_MODEL_REPO or "sports-win-model").strip()
        self.model_repo_id = self.model_repo_name
        self._data_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data"
        )
        self._status_file = os.getenv(
            "HF_PIPELINE_STATUS_FILE",
            os.path.join(self._data_dir, "hf_pipeline_status.json"),
        )
        self._predictions_file = os.getenv(
            "HF_DAILY_PREDICTIONS_FILE",
            os.path.join(self._data_dir, "hf_daily_predictions.json"),
        )
        self._training_history_file = os.path.join(self._data_dir, "training_history.json")
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
            except Exception as exc:
                logger.warning("[hf_pipeline] HF API init failed: %s", exc)
                self._ok = False

    @property
    def ok(self) -> bool:
        return self._ok

    # ──────────────────────────────────────────────────────────
    # Public pipeline methods
    # ──────────────────────────────────────────────────────────

    def bootstrap_one_year_history(self, days_back: int = 365) -> dict:
        """One-time: fetch and upload ~1yr of multi-sport game data to HF Dataset."""
        end = datetime.date.today()
        start = end - datetime.timedelta(days=max(1, int(days_back)))
        logger.info("[hf_pipeline] Bootstrapping %d days (%s to %s)", days_back, start, end)
        records = self._clean_game_records(self._fetch_completed_games(start, end))
        if not records:
            self._write_status({"last_step": "bootstrap", "ok": False, "message": "No records found"})
            return {"ok": False, "msg": "No historical records found", "records": 0}
        self.uploader.push_records("games", records)
        self.uploader.flush_all()
        sports = sorted({r.get("sport", "") for r in records})
        self._write_status({
            "last_step": "bootstrap",
            "ok": True,
            "bootstrap_records": len(records),
            "bootstrap_sports": sports,
            "bootstrap_date_range": f"{start} to {end}",
            "bootstrap_completed_at": _now_utc(),
        })
        logger.info("[hf_pipeline] Bootstrap done: %d records, sports=%s", len(records), sports)
        return {"ok": True, "records": len(records), "sports": sports, "date_range": f"{start} to {end}"}

    def append_daily_results(self, day: datetime.date | None = None) -> dict:
        """Daily: append completed games for `day` to HF Dataset."""
        target = day or datetime.date.today()
        logger.info("[hf_pipeline] Appending daily results for %s", target)
        records = self._clean_game_records(self._fetch_completed_games(target, target))
        if not records:
            self._write_status({
                "last_step": "append_daily", "ok": True,
                "append_records": 0, "append_date": target.isoformat(), "append_sports": [],
            })
            return {"ok": True, "records": 0, "date": target.isoformat()}
        self.uploader.push_records("games", records)
        self.uploader.flush_all()
        sports = sorted({r.get("sport", "") for r in records})
        self._write_status({
            "last_step": "append_daily", "ok": True,
            "append_records": len(records), "append_date": target.isoformat(),
            "append_sports": sports, "append_completed_at": _now_utc(),
        })
        return {"ok": True, "records": len(records), "date": target.isoformat(), "sports": sports}

    def train_and_publish_best_model(self, min_rows: int = 200, forced_model: str = "auto") -> TrainSummary:
        """Daily: train best classifier on full HF dataset and publish to HF Model Hub."""
        import joblib
        import pandas as pd
        from sklearn.compose import ColumnTransformer
        from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import StratifiedKFold, cross_val_score
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder

        if not self._ok or not self._api:
            raise RuntimeError("HF pipeline not configured. Set HF_API_KEY.")

        logger.info("[hf_pipeline] Loading games parquet shards from %s", self.dataset_repo_id)
        df = self._load_games_dataframe_from_hub()
        if df.empty:
            raise RuntimeError("HF dataset has no rows in games/train split")

        df = self._build_training_df(df)
        if len(df) < min_rows:
            raise RuntimeError(f"Not enough rows: {len(df)} < {min_rows}")

        y = (df["home_score"] > df["away_score"]).astype(int)
        X = df[self._TRAIN_FEATURES].copy()
        sports_covered = sorted(df["sport"].dropna().astype(str).unique().tolist())

        pre = ColumnTransformer(
            transformers=[
                ("cats", OneHotEncoder(handle_unknown="ignore"), self._CAT_FEATURES),
                ("nums", "passthrough", self._NUM_FEATURES),
            ]
        )
        candidates = {
            "logistic_regression": LogisticRegression(max_iter=2000, random_state=42),
            "random_forest": RandomForestClassifier(n_estimators=300, random_state=42, n_jobs=-1),
            "gradient_boosting": GradientBoostingClassifier(n_estimators=200, random_state=42),
        }
        forced_key = str(forced_model or "auto").strip().lower()
        if forced_key and forced_key != "auto" and forced_key in candidates:
            candidates = {forced_key: candidates[forced_key]}

        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        best_name, best_score, best_pipeline = "", -1.0, None
        for name, model in candidates.items():
            pipe = Pipeline([("pre", pre), ("model", model)])
            try:
                scores = cross_val_score(pipe, X, y, cv=cv, scoring="roc_auc")
                score = float(scores.mean())
                logger.info("[hf_pipeline] %s CV AUC=%.4f", name, score)
                if score > best_score:
                    best_score, best_name, best_pipeline = score, name, pipe
            except Exception as exc:
                logger.warning("[hf_pipeline] %s failed: %s", name, exc)

        if best_pipeline is None:
            raise RuntimeError("No model candidate succeeded")
        best_pipeline.fit(X, y)

        trained_at = _now_utc()
        version = trained_at[:19].replace(":", "-").replace("T", "_")
        metadata = {
            "version": version, "trained_at": trained_at,
            "rows": int(len(df)), "best_model": best_name,
            "cv_roc_auc": round(best_score, 6),
            "dataset_repo": self.dataset_repo_id,
            "features": self._TRAIN_FEATURES,
            "categorical_features": self._CAT_FEATURES,
            "numerical_features": self._NUM_FEATURES,
            "target": "home_win", "sports_covered": sports_covered,
        }

        with tempfile.TemporaryDirectory(prefix="hf_model_") as td:
            model_path = os.path.join(td, "model.joblib")
            meta_path = os.path.join(td, "metadata.json")
            readme_path = os.path.join(td, "README.md")
            joblib.dump(best_pipeline, model_path)
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)
            readme_content = (
                "---\n"
                "license: mit\n"
                "library_name: scikit-learn\n"
                "pipeline_tag: tabular-classification\n"
                "tags:\n"
                "- sports\n"
                "- betting\n"
                "- scikit-learn\n"
                "- auto-training\n"
                "metrics:\n"
                "- roc_auc\n"
                "datasets:\n"
                f"- {self.dataset_repo_id}\n"
                "---\n\n"
                "# Sports Win Prediction Model\n\n"
                f"Auto-trained {trained_at[:10]} by bettor HF pipeline.\n\n"
                "| Field | Value |\n|---|---|\n"
                f"| best_model | {best_name} |\n"
                f"| cv_roc_auc | {best_score:.4f} |\n"
                f"| rows | {len(df):,} |\n"
                f"| sports | {', '.join(sports_covered)} |\n"
                f"| version | {version} |\n"
            )
            with open(readme_path, "w", encoding="utf-8") as f:
                f.write(readme_content)
            self._api.create_repo(repo_id=self.model_repo_id, repo_type="model", exist_ok=True, private=False)
            for fname, fpath in [("model.joblib", model_path), ("metadata.json", meta_path), ("README.md", readme_path)]:
                self._api.upload_file(
                    path_or_fileobj=fpath, path_in_repo=fname,
                    repo_id=self.model_repo_id, repo_type="model",
                    commit_message=f"v{version}: update {fname}",
                )
        logger.info("[hf_pipeline] Published model v%s to %s (AUC=%.4f)", version, self.model_repo_id, best_score)

        summary = TrainSummary(
            repo_id=self.model_repo_id, rows=int(len(df)),
            best_model=best_name, cv_roc_auc=float(best_score),
            trained_at=trained_at, version=version,
            features=self._TRAIN_FEATURES, sports_covered=sports_covered,
        )
        self._append_training_history(summary)
        self._write_status({
            "last_step": "train_publish", "ok": True,
            "trained_rows": int(len(df)), "best_model": best_name,
            "cv_roc_auc": round(float(best_score), 6),
            "model_repo": self.model_repo_id,
            "trained_at": trained_at, "model_version": version,
            "sports_covered": sports_covered, "train_completed_at": _now_utc(),
        })
        return summary

    def predict_daily_schedule(
        self,
        day: datetime.date | None = None,
        output_path: str | None = None,
        via_api: bool = False,
        model_id: str | None = None,
        endpoint_url: str | None = None,
    ) -> dict:
        """Daily: generate predictions for today and tomorrow, save to JSON."""
        today = day or datetime.date.today()
        tomorrow = today + datetime.timedelta(days=1)
        meta = self._get_model_metadata()
        model_version = meta.get("version", "unknown")
        model_type = meta.get("best_model", "unknown")
        model_auc = float(meta.get("cv_roc_auc") or 0.0)

        all_predictions: list[dict] = []
        for target_date in [today, tomorrow]:
            games = self._fetch_upcoming_games(target_date)
            for g in games:
                home_team = str(g.get("home_team") or "").strip()
                away_team = str(g.get("away_team") or "").strip()
                if not home_team or not away_team:
                    continue
                sport = str(g.get("sport") or "mlb")
                league = str(g.get("league") or sport.upper())
                game_date = str(g.get("game_date") or target_date.isoformat())
                game_time = str(g.get("game_time") or "")
                game_id = str(g.get("game_id") or "")
                try:
                    if via_api:
                        resp = self.predict_via_hf_api(home_team, away_team, target_date.year, model_id, endpoint_url)
                        api_resp = resp.get("response") or {}
                        home_prob = float(api_resp[0].get("score", 0.5) if isinstance(api_resp, list) else 0.5)
                        away_prob = 1.0 - home_prob
                    else:
                        pred = self.predict_from_model_repo(
                            home_team=home_team, away_team=away_team,
                            sport=sport, season=target_date.year,
                        )
                        home_prob = float(pred.get("home_win_prob", 0.5))
                        away_prob = float(pred.get("away_win_prob", 0.5))
                    confidence = max(home_prob, away_prob)
                    tier = self._confidence_tier(confidence)
                    all_predictions.append({
                        "prediction_id": str(uuid4()),
                        "game_id": game_id,
                        "sport": sport,
                        "league": league,
                        "home_team": home_team,
                        "away_team": away_team,
                        "game_date": game_date,
                        "game_time": game_time,
                        "home_win_prob": round(home_prob, 4),
                        "away_win_prob": round(away_prob, 4),
                        "confidence": round(confidence, 4),
                        "confidence_tier": tier,
                        "model_version": model_version,
                        "model_type": model_type,
                        "model_auc": model_auc,
                        "predicted_at": _now_utc(),
                        "predict_mode": "api" if via_api else "artifact",
                    })
                except Exception as exc:
                    logger.warning("[hf_pipeline] predict error %s vs %s: %s", home_team, away_team, exc)
                    all_predictions.append({
                        "prediction_id": str(uuid4()),
                        "game_id": game_id, "sport": sport, "league": league,
                        "home_team": home_team, "away_team": away_team,
                        "game_date": game_date, "game_time": game_time,
                        "error": str(exc), "predicted_at": _now_utc(),
                    })

        out_path = output_path or self._predictions_file
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        good = [p for p in all_predictions if not p.get("error")]
        payload = {
            "generated_at": _now_utc(),
            "today": today.isoformat(),
            "tomorrow": tomorrow.isoformat(),
            "prediction_count": len(good),
            "error_count": len(all_predictions) - len(good),
            "model_version": model_version,
            "model_type": model_type,
            "model_auc": model_auc,
            "predictions": all_predictions,
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        self._write_status({
            "last_step": "predict_daily", "ok": True,
            "prediction_date": today.isoformat(),
            "prediction_count": len(good),
            "prediction_file": out_path,
            "model_version": model_version,
            "predict_completed_at": _now_utc(),
        })
        logger.info("[hf_pipeline] %d predictions generated (%s + %s)", len(good), today, tomorrow)
        return {"ok": True, "prediction_count": len(good), "output_file": out_path, "date": today.isoformat()}

    def run_daily_pipeline(
        self,
        custom_model: str = "auto",
        min_rows: int = 200,
        predictions_output_path: str | None = None,
        via_api: bool = False,
        model_id: str | None = None,
        endpoint_url: str | None = None,
    ) -> dict:
        """Orchestrate full daily pipeline: append results -> train -> predict."""
        logger.info("[hf_pipeline] Starting daily pipeline")
        self._write_status({"last_step": "daily_pipeline_started", "ok": True, "started_at": _now_utc()})
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        today = datetime.date.today()
        append_y = self.append_daily_results(yesterday)
        append_t = self.append_daily_results(today)
        try:
            summary = self.train_and_publish_best_model(min_rows=min_rows, forced_model=custom_model)
            train_result = {
                "ok": True, "repo_id": summary.repo_id, "rows": summary.rows,
                "best_model": summary.best_model, "cv_roc_auc": summary.cv_roc_auc,
                "trained_at": summary.trained_at, "version": summary.version,
                "sports_covered": summary.sports_covered,
            }
        except Exception as exc:
            logger.warning("[hf_pipeline] Training failed: %s", exc)
            train_result = {"ok": False, "error": str(exc)}
        preds = self.predict_daily_schedule(
            output_path=predictions_output_path,
            via_api=via_api, model_id=model_id, endpoint_url=endpoint_url,
        )
        result = {
            "ok": True,
            "append_yesterday": append_y, "append_today": append_t,
            "train": train_result, "predictions": preds,
            "completed_at": _now_utc(),
        }
        self._write_status({"last_step": "daily_pipeline", "ok": True, "daily_completed_at": _now_utc()})
        logger.info("[hf_pipeline] Daily pipeline complete")
        return result

    def predict_from_model_repo(
        self,
        home_team: str,
        away_team: str,
        sport: str = "mlb",
        season: int | None = None,
    ) -> dict:
        """Download model artifact from HF Hub and return win probabilities."""
        import joblib
        import pandas as pd
        from huggingface_hub import hf_hub_download

        today = datetime.date.today()
        model_path = hf_hub_download(
            repo_id=self.model_repo_id, filename="model.joblib",
            repo_type="model", token=self.token,
        )
        model = joblib.load(model_path)
        row = pd.DataFrame([{
            "home_team": home_team,
            "away_team": away_team,
            "sport": str(sport).lower(),
            "season": int(season or today.year),
            "month": today.month,
            "day_of_week": today.weekday(),
        }])
        probs = model.predict_proba(row)[0]
        home_prob = float(probs[1])
        meta = self._get_model_metadata()
        return {
            "home_team": home_team, "away_team": away_team,
            "sport": sport, "season": int(season or today.year),
            "home_win_prob": round(home_prob, 4),
            "away_win_prob": round(1.0 - home_prob, 4),
            "model_repo": self.model_repo_id,
            "model_version": meta.get("version", ""),
        }

    def predict_via_hf_api(
        self,
        home_team: str,
        away_team: str,
        season: int | None = None,
        model_id: str | None = None,
        endpoint_url: str | None = None,
    ) -> dict:
        """Call HF Inference API or custom endpoint."""
        url = str(endpoint_url or "").strip()
        if not url:
            url = f"https://api-inference.huggingface.co/models/{str(model_id or self.model_repo_id).strip()}"
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        payload = {"inputs": {"home_team": home_team, "away_team": away_team,
                               "season": int(season or datetime.date.today().year)}}
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        return {"url": url, "response": resp.json()}

    # ──────────────────────────────────────────────────────────
    # Private sport-specific completed game fetchers
    # ──────────────────────────────────────────────────────────

    def _fetch_completed_games(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        rows += self._fetch_mlb_games(start, end)
        rows += self._fetch_nba_games(start, end)
        rows += self._fetch_nhl_games(start, end)
        rows += self._fetch_soccer_games(start, end)
        rows += self._fetch_tennis_games_jeff_sackmann(start, end)
        return rows

    def _fetch_mlb_games(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        current = start
        while current <= end:
            day = current.isoformat()
            try:
                resp = requests.get(
                    "https://statsapi.mlb.com/api/v1/schedule",
                    params={"sportId": 1, "date": day, "hydrate": "linescore", "gameType": "R"},
                    timeout=20,
                )
                resp.raise_for_status()
                payload = resp.json() or {}
                for de in payload.get("dates", []):
                    for game in de.get("games", []):
                        status = str(
                            (game.get("status") or {}).get("detailedState")
                            or (game.get("status") or {}).get("abstractGameState") or ""
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
                        rows.append(self._make_game_record(
                            game_id=str(game.get("gamePk") or ""),
                            sport="mlb", league="MLB", game_date=day,
                            game_datetime=str(game.get("gameDate") or ""),
                            status=status, home_team=home_team, away_team=away_team,
                            home_score=float(home_score), away_score=float(away_score),
                            season=int(day[:4]),
                        ))
            except Exception as exc:
                logger.debug("[hf_pipeline] MLB %s: %s", day, exc)
            current += datetime.timedelta(days=1)
        return rows

    def _fetch_nba_games(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        current = start
        while current <= end:
            day = current.isoformat()
            try:
                resp = requests.get(
                    "https://www.balldontlie.io/api/v1/games",
                    params={"start_date": day, "end_date": day, "per_page": 100},
                    timeout=20,
                )
                resp.raise_for_status()
                for g in (resp.json() or {}).get("data", []):
                    if str(g.get("status") or "").strip() != "Final":
                        continue
                    home_team = str((g.get("home_team") or {}).get("full_name") or "").strip()
                    away_team = str((g.get("visitor_team") or {}).get("full_name") or "").strip()
                    hs = g.get("home_team_score")
                    as_ = g.get("visitor_team_score")
                    if not home_team or not away_team or hs is None or as_ is None:
                        continue
                    gd = str(g.get("date") or day)[:10]
                    rows.append(self._make_game_record(
                        game_id=str(g.get("id") or ""), sport="nba", league="NBA",
                        game_date=gd, game_datetime=str(g.get("date") or ""),
                        status="Final", home_team=home_team, away_team=away_team,
                        home_score=float(hs), away_score=float(as_), season=int(gd[:4]),
                    ))
                time.sleep(0.25)
            except Exception as exc:
                logger.debug("[hf_pipeline] NBA %s: %s", day, exc)
            current += datetime.timedelta(days=1)
        return rows

    def _fetch_nhl_games(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        current = start
        while current <= end:
            day = current.isoformat()
            try:
                resp = requests.get(f"https://api-web.nhle.com/v1/schedule/{day}", timeout=20)
                resp.raise_for_status()
                for week in (resp.json() or {}).get("gameWeek", []):
                    for game in week.get("games", []):
                        state = str(game.get("gameState") or "")
                        if state not in ("OFF", "FINAL", "OVER"):
                            continue
                        hd = game.get("homeTeam") or {}
                        ad = game.get("awayTeam") or {}
                        ht = str((hd.get("commonName") or {}).get("default") or hd.get("abbrev") or "").strip()
                        at = str((ad.get("commonName") or {}).get("default") or ad.get("abbrev") or "").strip()
                        hs = hd.get("score")
                        as_ = ad.get("score")
                        if not ht or not at or hs is None or as_ is None:
                            continue
                        gd = str(game.get("gameDate") or day)[:10]
                        rows.append(self._make_game_record(
                            game_id=str(game.get("id") or ""), sport="nhl", league="NHL",
                            game_date=gd, game_datetime=str(game.get("startTimeUTC") or ""),
                            status="Final", home_team=ht, away_team=at,
                            home_score=float(hs), away_score=float(as_), season=int(gd[:4]),
                        ))
            except Exception as exc:
                logger.debug("[hf_pipeline] NHL %s: %s", day, exc)
            current += datetime.timedelta(days=1)
        return rows

    def _fetch_soccer_games(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows = self._fetch_soccer_games_football_data(start, end)
        if rows:
            return rows
        return self._fetch_soccer_games_thesportsdb(start, end)

    def _fetch_soccer_games_football_data(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        if not self.football_data_api_key:
            return rows
        headers = {"X-Auth-Token": self.football_data_api_key}
        competitions = ("PL", "PD", "SA", "BL1", "FL1", "PPL")
        for comp in competitions:
            try:
                resp = requests.get(
                    f"https://api.football-data.org/v4/competitions/{comp}/matches",
                    params={"dateFrom": start.isoformat(), "dateTo": end.isoformat(), "status": "FINISHED"},
                    headers=headers,
                    timeout=25,
                )
                resp.raise_for_status()
                for m in (resp.json() or {}).get("matches", []):
                    score = m.get("score") or {}
                    full = score.get("fullTime") or {}
                    hs = full.get("home")
                    as_ = full.get("away")
                    ht = str(((m.get("homeTeam") or {}).get("name")) or "").strip()
                    at = str(((m.get("awayTeam") or {}).get("name")) or "").strip()
                    gd = str(m.get("utcDate") or "")[:10]
                    if hs is None or as_ is None or not ht or not at or not gd:
                        continue
                    rows.append(
                        self._make_game_record(
                            game_id=str(m.get("id") or ""),
                            sport="soccer",
                            league=str((m.get("competition") or {}).get("name") or comp),
                            game_date=gd,
                            game_datetime=str(m.get("utcDate") or ""),
                            status="Final",
                            home_team=ht,
                            away_team=at,
                            home_score=float(hs),
                            away_score=float(as_),
                            season=int(gd[:4]),
                        )
                    )
                time.sleep(0.2)
            except Exception as exc:
                logger.debug("[hf_pipeline] football-data %s: %s", comp, exc)
        return rows

    def _fetch_soccer_games_thesportsdb(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        current = start
        while current <= end:
            day = current.isoformat()
            try:
                resp = requests.get(
                    "https://www.thesportsdb.com/api/v1/json/1/eventsday.php",
                    params={"d": day, "s": "Soccer"}, timeout=20,
                )
                resp.raise_for_status()
                for ev in ((resp.json() or {}).get("events") or []):
                    hs = ev.get("intHomeScore")
                    as_ = ev.get("intAwayScore")
                    if hs is None or as_ is None:
                        continue
                    ht = str(ev.get("strHomeTeam") or "").strip()
                    at = str(ev.get("strAwayTeam") or "").strip()
                    if not ht or not at:
                        continue
                    rows.append(self._make_game_record(
                        game_id=str(ev.get("idEvent") or ""), sport="soccer",
                        league=str(ev.get("strLeague") or "Soccer"),
                        game_date=day, game_datetime=day, status="Final",
                        home_team=ht, away_team=at,
                        home_score=float(hs), away_score=float(as_), season=current.year,
                    ))
            except Exception as exc:
                logger.debug("[hf_pipeline] Soccer %s: %s", day, exc)
            current += datetime.timedelta(days=1)
        return rows

    # ──────────────────────────────────────────────────────────
    # Private sport-specific upcoming fetchers
    # ──────────────────────────────────────────────────────────

    def _fetch_upcoming_games(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        rows += self._fetch_mlb_upcoming(day)
        rows += self._fetch_nba_upcoming(day)
        rows += self._fetch_nhl_upcoming(day)
        rows += self._fetch_soccer_upcoming(day)
        rows += self._fetch_polymarket_upcoming(day)

        deduped = []
        seen = set()
        for row in rows:
            key = (
                str(row.get("sport") or "").lower(),
                str(row.get("game_date") or ""),
                str(row.get("away_team") or "").lower(),
                str(row.get("home_team") or "").lower(),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        return deduped

    def _fetch_mlb_upcoming(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        try:
            resp = requests.get(
                "https://statsapi.mlb.com/api/v1/schedule",
                params={"sportId": 1, "date": day.isoformat(), "gameType": "R"}, timeout=20,
            )
            resp.raise_for_status()
            for de in (resp.json() or {}).get("dates", []):
                for game in de.get("games", []):
                    teams = game.get("teams") or {}
                    ht = str((((teams.get("home") or {}).get("team") or {}).get("name") or "")).strip()
                    at = str((((teams.get("away") or {}).get("team") or {}).get("name") or "")).strip()
                    if not ht or not at:
                        continue
                    rows.append({"sport": "mlb", "league": "MLB", "home_team": ht, "away_team": at,
                                 "game_date": day.isoformat(), "game_time": str(game.get("gameDate") or ""),
                                 "game_id": str(game.get("gamePk") or "")})
        except Exception:
            pass
        return rows

    def _fetch_nba_upcoming(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        try:
            resp = requests.get(
                "https://www.balldontlie.io/api/v1/games",
                params={"start_date": day.isoformat(), "end_date": day.isoformat(), "per_page": 100},
                timeout=20,
            )
            resp.raise_for_status()
            for g in (resp.json() or {}).get("data", []):
                ht = str((g.get("home_team") or {}).get("full_name") or "").strip()
                at = str((g.get("visitor_team") or {}).get("full_name") or "").strip()
                if not ht or not at:
                    continue
                rows.append({"sport": "nba", "league": "NBA", "home_team": ht, "away_team": at,
                             "game_date": day.isoformat(), "game_time": str(g.get("date") or ""),
                             "game_id": str(g.get("id") or "")})
        except Exception:
            pass
        return rows

    def _fetch_nhl_upcoming(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        try:
            resp = requests.get(f"https://api-web.nhle.com/v1/schedule/{day.isoformat()}", timeout=20)
            resp.raise_for_status()
            for week in (resp.json() or {}).get("gameWeek", []):
                for game in week.get("games", []):
                    hd = game.get("homeTeam") or {}
                    ad = game.get("awayTeam") or {}
                    ht = str((hd.get("commonName") or {}).get("default") or hd.get("abbrev") or "").strip()
                    at = str((ad.get("commonName") or {}).get("default") or ad.get("abbrev") or "").strip()
                    if not ht or not at:
                        continue
                    rows.append({"sport": "nhl", "league": "NHL", "home_team": ht, "away_team": at,
                                 "game_date": str(game.get("gameDate") or day.isoformat()),
                                 "game_time": str(game.get("startTimeUTC") or ""),
                                 "game_id": str(game.get("id") or "")})
        except Exception:
            pass
        return rows

    def _fetch_soccer_upcoming(self, day: datetime.date) -> list[dict]:
        rows = self._fetch_soccer_upcoming_football_data(day)
        if rows:
            return rows
        return self._fetch_soccer_upcoming_thesportsdb(day)

    def _fetch_soccer_upcoming_football_data(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        if not self.football_data_api_key:
            return rows
        headers = {"X-Auth-Token": self.football_data_api_key}
        competitions = ("PL", "PD", "SA", "BL1", "FL1", "PPL")
        for comp in competitions:
            try:
                resp = requests.get(
                    f"https://api.football-data.org/v4/competitions/{comp}/matches",
                    params={"dateFrom": day.isoformat(), "dateTo": day.isoformat(), "status": "SCHEDULED"},
                    headers=headers,
                    timeout=25,
                )
                resp.raise_for_status()
                for m in (resp.json() or {}).get("matches", []):
                    ht = str(((m.get("homeTeam") or {}).get("name")) or "").strip()
                    at = str(((m.get("awayTeam") or {}).get("name")) or "").strip()
                    if not ht or not at:
                        continue
                    rows.append(
                        {
                            "sport": "soccer",
                            "league": str((m.get("competition") or {}).get("name") or comp),
                            "home_team": ht,
                            "away_team": at,
                            "game_date": day.isoformat(),
                            "game_time": str(m.get("utcDate") or ""),
                            "game_id": str(m.get("id") or ""),
                        }
                    )
                time.sleep(0.2)
            except Exception as exc:
                logger.debug("[hf_pipeline] football-data upcoming %s: %s", comp, exc)
        return rows

    def _fetch_soccer_upcoming_thesportsdb(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        try:
            resp = requests.get(
                "https://www.thesportsdb.com/api/v1/json/1/eventsday.php",
                params={"d": day.isoformat(), "s": "Soccer"}, timeout=20,
            )
            resp.raise_for_status()
            for ev in ((resp.json() or {}).get("events") or []):
                if ev.get("intHomeScore") is not None:
                    continue
                ht = str(ev.get("strHomeTeam") or "").strip()
                at = str(ev.get("strAwayTeam") or "").strip()
                if not ht or not at:
                    continue
                rows.append({"sport": "soccer", "league": str(ev.get("strLeague") or "Soccer"),
                             "home_team": ht, "away_team": at,
                             "game_date": day.isoformat(), "game_time": str(ev.get("strTime") or ""),
                             "game_id": str(ev.get("idEvent") or "")})
        except Exception:
            pass
        return rows

    def _fetch_tennis_games_jeff_sackmann(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        base_dir = str(self.tennis_sackmann_dir or "").strip()
        if not base_dir or not os.path.isdir(base_dir):
            return rows
        try:
            import csv
        except Exception:
            return rows

        for year in range(start.year, end.year + 1):
            csv_path = os.path.join(base_dir, f"atp_matches_{year}.csv")
            if not os.path.exists(csv_path):
                continue
            try:
                with open(csv_path, "r", encoding="utf-8", newline="") as handle:
                    reader = csv.DictReader(handle)
                    for row in reader:
                        date_raw = str(row.get("tourney_date") or "").strip()
                        if len(date_raw) != 8 or not date_raw.isdigit():
                            continue
                        game_date = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
                        try:
                            parsed = datetime.date.fromisoformat(game_date)
                        except Exception:
                            continue
                        if parsed < start or parsed > end:
                            continue
                        winner = str(row.get("winner_name") or "").strip()
                        loser = str(row.get("loser_name") or "").strip()
                        if not winner or not loser:
                            continue
                        rows.append(
                            self._make_game_record(
                                game_id=str(row.get("match_num") or f"{year}-{winner}-{loser}-{game_date}"),
                                sport="tennis",
                                league=str(row.get("tourney_name") or "ATP"),
                                game_date=game_date,
                                game_datetime=game_date,
                                status="Final",
                                home_team=winner,
                                away_team=loser,
                                home_score=1.0,
                                away_score=0.0,
                                season=parsed.year,
                            )
                        )
            except Exception as exc:
                logger.debug("[hf_pipeline] Jeff Sackmann %s: %s", year, exc)
        return rows

    def _fetch_polymarket_upcoming(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        try:
            resp = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={"active": "true", "closed": "false", "limit": 300},
                timeout=25,
            )
            resp.raise_for_status()
            markets = resp.json() or []
        except Exception as exc:
            logger.debug("[hf_pipeline] polymarket upcoming fetch failed: %s", exc)
            return rows

        for m in markets:
            if not isinstance(m, dict):
                continue
            event_iso = str(m.get("startDate") or m.get("endDate") or "").strip()
            if not event_iso:
                continue
            event_day = event_iso[:10]
            if event_day != day.isoformat():
                continue
            home, away = self._parse_polymarket_matchup(str(m.get("question") or m.get("title") or ""))
            if not home or not away:
                continue
            sport_raw = str(m.get("sport") or m.get("category") or "").strip().lower()
            sport = self._POLYMARKET_SPORT_ALIASES.get(sport_raw, "")
            if not sport:
                for k, v in self._POLYMARKET_SPORT_ALIASES.items():
                    if k in sport_raw:
                        sport = v
                        break
            if not sport:
                continue
            rows.append(
                {
                    "sport": sport,
                    "league": str(m.get("seriesTicker") or m.get("eventSlug") or "Polymarket"),
                    "home_team": home,
                    "away_team": away,
                    "game_date": event_day,
                    "game_time": event_iso,
                    "game_id": str(m.get("id") or m.get("slug") or ""),
                }
            )
        return rows

    def _parse_polymarket_matchup(self, text: str) -> tuple[str, str]:
        raw = " ".join(str(text or "").split())
        if not raw:
            return "", ""
        patterns = [
            r"(?P<a>.+?)\s+vs\.?\s+(?P<b>.+)",
            r"(?P<a>.+?)\s+v\.?\s+(?P<b>.+)",
            r"(?P<a>.+?)\s+@\s+(?P<b>.+)",
            r"(?P<a>.+?)\s+at\s+(?P<b>.+)",
        ]
        for pat in patterns:
            m = re.search(pat, raw, flags=re.IGNORECASE)
            if not m:
                continue
            a = str(m.group("a") or "").strip(" ?!,:;")
            b = str(m.group("b") or "").strip(" ?!,:;")
            if a and b and a.lower() != b.lower():
                return b, a
        return "", ""

    # ──────────────────────────────────────────────────────────
    # Private helpers
    # ──────────────────────────────────────────────────────────

    def _make_game_record(self, **kwargs) -> dict:
        now = _now_utc()
        record = {
            "record_id": str(uuid4()),
            "game_id": "", "sport": "mlb", "league": "",
            "game_date": "", "game_datetime": "", "status": "",
            "home_team": "", "away_team": "",
            "home_score": 0.0, "away_score": 0.0,
            "home_starter": "", "away_starter": "",
            "season": datetime.date.today().year,
            "metadata": "{}", "created_at": now,
        }
        record.update(kwargs)
        return record

    def _confidence_tier(self, prob: float) -> str:
        for threshold, tier in self._CONFIDENCE_TIERS:
            if prob >= threshold:
                return tier
        return "uncertain"

    def _clean_game_records(self, records: list[dict]) -> list[dict]:
        cleaned: list[dict] = []
        seen: set[str] = set()
        for raw in (records or []):
            if not isinstance(raw, dict):
                continue
            game_id = str(raw.get("game_id") or "").strip()
            ht = " ".join(str(raw.get("home_team") or "").strip().split())
            at = " ".join(str(raw.get("away_team") or "").strip().split())
            gd = str(raw.get("game_date") or "").strip()
            if not ht or not at or not gd:
                continue
            if ht.lower() == at.lower():
                continue
            key = game_id or f"{gd}|{raw.get('sport','?')}|{at}|{ht}"
            if key in seen:
                continue
            seen.add(key)
            row = dict(raw)
            row["record_id"] = str(row.get("record_id") or uuid4())
            row["home_team"] = ht[:120]
            row["away_team"] = at[:120]
            row["league"] = str(row.get("league") or "").strip()[:80]
            row["sport"] = str(row.get("sport") or "mlb").strip().lower()[:32]
            row["status"] = str(row.get("status") or "").strip()[:80]
            row["game_date"] = gd[:10]
            row["game_datetime"] = str(row.get("game_datetime") or "").strip()[:40]
            row["season"] = int(str(row.get("season") or gd[:4] or datetime.date.today().year))
            row["metadata"] = str(row.get("metadata") or "{}")
            row["created_at"] = str(row.get("created_at") or _now_utc())
            try:
                row["home_score"] = float(row.get("home_score"))
                row["away_score"] = float(row.get("away_score"))
            except Exception:
                continue
            cleaned.append(row)
        return cleaned

    def _build_training_df(self, df):
        import pandas as pd
        for col in ("home_score", "away_score"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["home_team", "away_team", "home_score", "away_score"])
        df = df[df["home_score"] != df["away_score"]].copy()
        if "sport" not in df.columns:
            df["sport"] = "mlb"
        df["sport"] = df["sport"].fillna("mlb").astype(str)
        df["season"] = pd.to_numeric(df.get("season"), errors="coerce").fillna(0).astype(int)
        if "game_date" in df.columns:
            gd = pd.to_datetime(df["game_date"], errors="coerce")
            df["month"] = gd.dt.month.fillna(6).astype(int)
            df["day_of_week"] = gd.dt.dayofweek.fillna(0).astype(int)
        else:
            df["month"] = 6
            df["day_of_week"] = 0
        return df

    def _load_games_dataframe_from_hub(self):
        import pandas as pd
        from huggingface_hub import hf_hub_download

        if not self._ok or not self._api:
            return pd.DataFrame()

        try:
            files = self._api.list_repo_files(repo_id=self.dataset_repo_id, repo_type="dataset")
        except Exception as exc:
            logger.warning("[hf_pipeline] list_repo_files failed: %s", exc)
            return pd.DataFrame()

        game_files = [f for f in files if str(f).startswith("data/games/") and str(f).endswith(".parquet")]
        if not game_files:
            return pd.DataFrame()

        frames = []
        for path_in_repo in game_files:
            try:
                local_path = hf_hub_download(
                    repo_id=self.dataset_repo_id,
                    repo_type="dataset",
                    filename=path_in_repo,
                    token=self.token,
                )
                frames.append(pd.read_parquet(local_path))
            except Exception as exc:
                logger.debug("[hf_pipeline] parquet read failed for %s: %s", path_in_repo, exc)
                continue
        if not frames:
            return pd.DataFrame()
        df = pd.concat(frames, ignore_index=True, sort=False)
        return df

    def _write_status(self, patch: dict) -> None:
        os.makedirs(os.path.dirname(self._status_file), exist_ok=True)
        base: dict = {}
        try:
            if os.path.exists(self._status_file):
                with open(self._status_file, "r", encoding="utf-8") as f:
                    base = json.load(f) or {}
        except Exception:
            base = {}
        base.update(patch or {})
        base["updated_at"] = _now_utc()
        base["dataset_repo"] = self.dataset_repo_id
        base["model_repo"] = self.model_repo_id
        with open(self._status_file, "w", encoding="utf-8") as f:
            json.dump(base, f, indent=2)

    def _append_training_history(self, summary: TrainSummary) -> None:
        os.makedirs(os.path.dirname(self._training_history_file), exist_ok=True)
        history: list[dict] = []
        try:
            if os.path.exists(self._training_history_file):
                with open(self._training_history_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        history = data
        except Exception:
            history = []
        history.append({
            "version": summary.version,
            "trained_at": summary.trained_at,
            "rows": summary.rows,
            "best_model": summary.best_model,
            "cv_roc_auc": summary.cv_roc_auc,
            "sports_covered": summary.sports_covered,
            "repo_id": summary.repo_id,
        })
        history = history[-100:]
        with open(self._training_history_file, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2)
        if self._ok and self._api:
            try:
                buf = io.BytesIO(json.dumps(history, indent=2).encode("utf-8"))
                self._api.upload_file(
                    path_or_fileobj=buf, path_in_repo="training_history.json",
                    repo_id=self.model_repo_id, repo_type="model",
                    commit_message=f"v{summary.version}: update training history",
                )
            except Exception as exc:
                logger.debug("[hf_pipeline] training_history push: %s", exc)

    def _get_model_metadata(self) -> dict:
        if not self._ok or not self._api:
            return {}
        try:
            from huggingface_hub import hf_hub_download
            path = hf_hub_download(
                repo_id=self.model_repo_id, filename="metadata.json",
                repo_type="model", token=self.token,
            )
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
        except Exception:
            return {}
