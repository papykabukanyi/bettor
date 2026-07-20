from __future__ import annotations

import datetime
import io
import json
import logging
import os
import tempfile
import threading
from dataclasses import dataclass, field
from pathlib import Path
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
    _append_lock = threading.Lock()
    _append_inflight_days: set[str] = set()
    _whoami_cache_lock = threading.Lock()
    _whoami_cache_name: str = ""
    _whoami_cache_token: str = ""
    _broken_model_versions: set[str] = set()

    _TRAIN_FEATURES = ["home_team", "away_team", "sport", "season", "month", "day_of_week"]
    _CAT_FEATURES = ["home_team", "away_team", "sport"]
    _NUM_FEATURES = ["season", "month", "day_of_week"]

    def __init__(self, token: str | None = None, dataset_repo: str | None = None, model_repo: str | None = None):
        hf_api_key = str(os.getenv("HF_API_KEY", "")).strip()
        hf_dataset_repo = str(os.getenv("HF_DATASET_REPO", "papylove/sportprediction")).strip()
        hf_model_repo = str(os.getenv("HF_MODEL_REPO", "papylove/sportprediction")).strip()

        self.token = str(token or hf_api_key).strip()
        self.uploader = HFUploader(token=self.token, repo_name=dataset_repo or hf_dataset_repo)
        self.dataset_repo_id = getattr(self.uploader, "_repo_id", "")
        self.model_repo_id = str(model_repo or hf_model_repo).strip()

        root_dir = Path(__file__).resolve().parents[2]
        self._data_dir = root_dir / "data"
        self._status_file = Path(os.getenv("HF_PIPELINE_STATUS_FILE", str(self._data_dir / "hf_pipeline_status.json")))
        self._predictions_file = Path(os.getenv("HF_DAILY_PREDICTIONS_FILE", str(self._data_dir / "hf_daily_predictions.json")))
        self._training_history_file = self._data_dir / "training_history.json"

        self._api = None
        self._cached_model = None
        self._cached_model_version = ""
        self._ok = bool(self.token and getattr(self.uploader, "_ok", False))

        if self._ok:
            try:
                from huggingface_hub import HfApi
                self._api = HfApi(token=self.token)
                user = ""
                with HFDirectPipeline._whoami_cache_lock:
                    if (
                        HFDirectPipeline._whoami_cache_token == self.token
                        and HFDirectPipeline._whoami_cache_name
                    ):
                        user = HFDirectPipeline._whoami_cache_name
                if not user:
                    who = self._api.whoami() or {}
                    user = str(who.get("name") or "").strip()
                    if user:
                        with HFDirectPipeline._whoami_cache_lock:
                            HFDirectPipeline._whoami_cache_token = self.token
                            HFDirectPipeline._whoami_cache_name = user
                if user and "/" not in self.model_repo_id:
                    self.model_repo_id = f"{user}/{self.model_repo_id}"
            except Exception as exc:
                logger.warning("[hf_pipeline] HF API init failed: %s", exc)
                self._ok = False

    @property
    def ok(self) -> bool:
        return self._ok

    def append_daily_results(self, day: datetime.date | None = None) -> dict:
        target = day or datetime.date.today()
        target_key = target.isoformat()
        with HFDirectPipeline._append_lock:
            if target_key in HFDirectPipeline._append_inflight_days:
                logger.info("[hf_pipeline] append_daily_results skipped; already running for %s", target_key)
                return {"ok": True, "records": 0, "date": target_key, "skipped": True, "reason": "inflight_same_day"}
            HFDirectPipeline._append_inflight_days.add(target_key)

        try:
            logger.info("[hf_pipeline] Appending daily results for %s", target_key)
            records = []
            self._write_status(
                {
                    "last_step": "append_daily",
                    "ok": True,
                    "append_records": len(records),
                    "append_date": target_key,
                    "append_sports": [],
                    "append_completed_at": _now_utc(),
                }
            )
            return {"ok": True, "records": len(records), "date": target_key, "sports": []}
        finally:
            with HFDirectPipeline._append_lock:
                HFDirectPipeline._append_inflight_days.discard(target_key)

    def train_and_publish_best_model(self, min_rows: int = 200, forced_model: str = "auto") -> TrainSummary:
        if not self._ok or not self._api:
            return TrainSummary(
                repo_id="",
                rows=0,
                best_model="skipped",
                cv_roc_auc=0.0,
                trained_at=_now_utc(),
                version="",
                sports_covered=[],
            )
        return TrainSummary(
            repo_id=self.model_repo_id,
            rows=0,
            best_model="skipped",
            cv_roc_auc=0.0,
            trained_at=_now_utc(),
            version="",
            features=self._TRAIN_FEATURES,
            sports_covered=[],
        )

    def predict_daily_schedule(
        self,
        day: datetime.date | None = None,
        output_path: str | None = None,
        via_api: bool = False,
        model_id: str | None = None,
        endpoint_url: str | None = None,
    ) -> dict:
        today = day or datetime.date.today()
        tomorrow = today + datetime.timedelta(days=1)
        payload = {
            "generated_at": _now_utc(),
            "today": today.isoformat(),
            "tomorrow": tomorrow.isoformat(),
            "prediction_count": 0,
            "error_count": 0,
            "model_version": "",
            "model_type": "",
            "model_auc": 0.0,
            "predictions": [],
        }
        out_path = Path(output_path or str(self._predictions_file))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        self._write_status(
            {
                "last_step": "predict_daily",
                "ok": True,
                "prediction_date": today.isoformat(),
                "prediction_count": 0,
                "prediction_sports": [],
                "prediction_file": str(out_path),
                "predict_completed_at": _now_utc(),
            }
        )
        return {"ok": True, "prediction_count": 0, "output_file": str(out_path), "date": today.isoformat()}

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
        append_y = self.append_daily_results(yesterday)
        append_t = self.append_daily_results(datetime.date.today())
        train = self.train_and_publish_best_model(min_rows=min_rows, forced_model=custom_model)
        preds = self.predict_daily_schedule(
            output_path=predictions_output_path, via_api=via_api, model_id=model_id, endpoint_url=endpoint_url
        )
        return {
            "ok": True,
            "append_yesterday": append_y,
            "append_today": append_t,
            "train": {
                "ok": True,
                "repo_id": train.repo_id,
                "rows": train.rows,
                "best_model": train.best_model,
                "cv_roc_auc": train.cv_roc_auc,
                "trained_at": train.trained_at,
                "version": train.version,
                "sports_covered": train.sports_covered,
            },
            "predictions": preds,
            "completed_at": _now_utc(),
        }

    def publish_runtime_artifacts(self) -> dict:
        if not self._ok or not self._api:
            return {"ok": False, "uploaded": 0, "reason": "hf_not_configured"}

        artifacts = {
            "artifacts/hf_pipeline_status.json": self._status_file,
            "artifacts/hf_daily_predictions.json": self._predictions_file,
            "artifacts/training_history.json": self._training_history_file,
        }
        uploaded = 0
        for repo_path, local_path in artifacts.items():
            if not Path(local_path).exists():
                continue
            try:
                with open(local_path, "rb") as f:
                    self._api.upload_file(
                        path_or_fileobj=f.read(),
                        path_in_repo=repo_path,
                        repo_id=self.model_repo_id,
                        repo_type="model",
                        commit_message=f"Update {repo_path}",
                    )
                uploaded += 1
            except Exception as exc:
                logger.debug("[hf_pipeline] publish artifact failed %s: %s", repo_path, exc)
        return {"ok": True, "uploaded": uploaded}

    def _write_status(self, patch: dict) -> None:
        self._status_file.parent.mkdir(parents=True, exist_ok=True)
        base: dict = {}
        try:
            if self._status_file.exists():
                base = json.loads(self._status_file.read_text(encoding="utf-8"))
        except Exception:
            base = {}
        base.update(patch or {})
        base["updated_at"] = _now_utc()
        base["dataset_repo"] = self.dataset_repo_id
        base["model_repo"] = self.model_repo_id
        self._status_file.write_text(json.dumps(base, indent=2), encoding="utf-8")
