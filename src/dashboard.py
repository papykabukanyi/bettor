"""
HF-first web dashboard.

Routes:
  GET /                     -> HF dashboard UI
  GET /api/hf/pipeline-status
  GET /api/hf/predictions?date=today|tomorrow|YYYY-MM-DD
  GET /api/hf/dataset-stats
  GET /api/hf/model-history
"""

from __future__ import annotations

import atexit
import datetime
import json
import logging
import os
import threading
import time
from typing import Any

from flask import Flask, jsonify, render_template, request

SRC_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(SRC_DIR), "data")
HF_PIPELINE_STATUS_FILE = os.getenv(
    "HF_PIPELINE_STATUS_FILE",
    os.path.join(DATA_DIR, "hf_pipeline_status.json"),
)
HF_DAILY_PREDICTIONS_FILE = os.getenv(
    "HF_DAILY_PREDICTIONS_FILE",
    os.path.join(DATA_DIR, "hf_daily_predictions.json"),
)
HF_TRAINING_HISTORY_FILE = os.getenv(
    "HF_TRAINING_HISTORY_FILE",
    os.path.join(DATA_DIR, "training_history.json"),
)
HF_DAILY_RUN_HOUR_ET = int(os.getenv("HF_DAILY_RUN_HOUR_ET", "4") or "4")
HF_DAILY_RUN_MINUTE_ET = int(os.getenv("HF_DAILY_RUN_MINUTE_ET", "15") or "15")
HF_DAILY_CUSTOM_MODEL = str(
    os.getenv("HF_DAILY_CUSTOM_MODEL", "gradient_boosting") or "gradient_boosting"
).strip().lower()
HF_DAILY_MIN_TRAIN_ROWS = max(50, int(os.getenv("HF_DAILY_MIN_TRAIN_ROWS", "200") or "200"))
HF_DAILY_USE_INFERENCE_API = str(os.getenv("HF_DAILY_USE_INFERENCE_API", "0")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
HF_AUTORUN_ON_DEPLOY = str(os.getenv("HF_AUTORUN_ON_DEPLOY", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
HF_AUTORUN_DELAY_SEC = max(0, int(os.getenv("HF_AUTORUN_DELAY_SEC", "30") or "30"))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder="templates")

_run_lock = threading.Lock()
_run_state: dict[str, Any] = {
    "running": False,
    "last_reason": "",
    "last_started_at": "",
    "last_completed_at": "",
    "last_error": "",
}
_scheduler = None
_autorun_started = False
_autorun_lock = threading.Lock()



def _utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()



def _load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default



def _write_status_patch(patch: dict[str, Any]) -> dict[str, Any]:
    os.makedirs(os.path.dirname(HF_PIPELINE_STATUS_FILE), exist_ok=True)
    payload = _load_json(HF_PIPELINE_STATUS_FILE, {})
    if not isinstance(payload, dict):
        payload = {}
    payload.update(patch or {})
    payload["updated_at"] = _utc_now_iso()
    with open(HF_PIPELINE_STATUS_FILE, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return payload



def _run_daily_pipeline(reason: str) -> None:
    from config import HF_INFERENCE_ENDPOINT, HF_INFERENCE_MODEL
    from data.hf_pipeline import HFDirectPipeline

    _run_state.update(
        {
            "running": True,
            "last_reason": reason,
            "last_started_at": _utc_now_iso(),
            "last_error": "",
        }
    )
    try:
        pipeline = HFDirectPipeline()
        if not pipeline.ok:
            _write_status_patch(
                {
                    "ok": False,
                    "last_step": "daily_pipeline_skipped",
                    "message": "HF pipeline is not configured. Set HF_API_KEY.",
                }
            )
            return

        result = pipeline.run_daily_pipeline(
            custom_model=HF_DAILY_CUSTOM_MODEL,
            min_rows=HF_DAILY_MIN_TRAIN_ROWS,
            predictions_output_path=HF_DAILY_PREDICTIONS_FILE,
            via_api=HF_DAILY_USE_INFERENCE_API,
            model_id=HF_INFERENCE_MODEL or pipeline.model_repo_id,
            endpoint_url=HF_INFERENCE_ENDPOINT or "",
        )
        _write_status_patch(
            {
                "ok": bool(result.get("ok", True)),
                "last_step": "daily_pipeline",
                "last_reason": reason,
                "last_completed_at": _utc_now_iso(),
            }
        )
    except Exception as exc:
        logger.exception("HF daily pipeline failed")
        _run_state["last_error"] = str(exc)
        _write_status_patch(
            {
                "ok": False,
                "last_step": "daily_pipeline_error",
                "last_reason": reason,
                "error": str(exc),
            }
        )
    finally:
        _run_state["running"] = False
        _run_state["last_completed_at"] = _utc_now_iso()
        _run_lock.release()



def _trigger_daily_pipeline(reason: str) -> bool:
    if not _run_lock.acquire(blocking=False):
        return False
    thread = threading.Thread(
        target=_run_daily_pipeline,
        args=(reason,),
        daemon=True,
        name=f"hf-pipeline-{reason}",
    )
    thread.start()
    return True



def _autorun_hf_pipeline_on_deploy() -> None:
    global _autorun_started
    if not HF_AUTORUN_ON_DEPLOY:
        return
    with _autorun_lock:
        if _autorun_started:
            return
        _autorun_started = True

    def _runner() -> None:
        try:
            if HF_AUTORUN_DELAY_SEC > 0:
                time.sleep(HF_AUTORUN_DELAY_SEC)
            started = _trigger_daily_pipeline("deploy_autorun")
            if started:
                logger.info("HF deploy autorun triggered")
        except Exception:
            logger.exception("HF deploy autorun failed")

    threading.Thread(target=_runner, daemon=True, name="hf-deploy-autorun").start()



def _start_scheduler():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except Exception as exc:
        logger.warning("APScheduler unavailable: %s", exc)
        return None

    scheduler = BackgroundScheduler(timezone="America/New_York")
    scheduler.add_job(
        lambda: _trigger_daily_pipeline("scheduled_daily_run"),
        CronTrigger(
            hour=HF_DAILY_RUN_HOUR_ET,
            minute=HF_DAILY_RUN_MINUTE_ET,
            timezone="America/New_York",
        ),
        id="hf_daily_pipeline",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    logger.info(
        "HF scheduler started for %02d:%02d ET",
        HF_DAILY_RUN_HOUR_ET,
        HF_DAILY_RUN_MINUTE_ET,
    )
    return scheduler



def _ensure_background_services() -> None:
    global _scheduler
    if _scheduler is None:
        _scheduler = _start_scheduler()
        if _scheduler is not None:
            atexit.register(lambda: _scheduler.shutdown(wait=False))
    _autorun_hf_pipeline_on_deploy()



def _load_predictions_payload() -> dict[str, Any]:
    payload = _load_json(HF_DAILY_PREDICTIONS_FILE, {})
    return payload if isinstance(payload, dict) else {}



def _resolve_prediction_date(requested: str, payload: dict[str, Any]) -> str:
    requested_value = str(requested or "today").strip().lower()
    if requested_value == "tomorrow":
        return str(payload.get("tomorrow") or "")
    if requested_value and requested_value != "today":
        return requested
    return str(payload.get("today") or "")



def _filter_predictions_for_date(payload: dict[str, Any], target_date: str) -> list[dict[str, Any]]:
    predictions = payload.get("predictions") or []
    if not isinstance(predictions, list):
        return []
    return [
        row
        for row in predictions
        if isinstance(row, dict) and str(row.get("game_date") or "") == str(target_date or "")
    ]


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/hf/pipeline-status")
def hf_pipeline_status():
    status = _load_json(HF_PIPELINE_STATUS_FILE, {})
    if not isinstance(status, dict):
        status = {}
    scheduler_running = False
    if _scheduler is not None:
        try:
            scheduler_running = bool(_scheduler.running)
        except Exception:
            scheduler_running = False
    payload = dict(status)
    payload.update(
        {
            "running": _run_state["running"],
            "last_reason": _run_state["last_reason"] or status.get("last_reason", ""),
            "last_started_at": _run_state["last_started_at"],
            "last_completed_at": _run_state["last_completed_at"] or status.get("last_completed_at", ""),
            "last_error": _run_state["last_error"] or status.get("error", ""),
            "scheduler": {
                "enabled": _scheduler is not None,
                "running": scheduler_running,
                "daily_run_hour_et": HF_DAILY_RUN_HOUR_ET,
                "daily_run_minute_et": HF_DAILY_RUN_MINUTE_ET,
            },
            "autorun": {
                "enabled": HF_AUTORUN_ON_DEPLOY,
                "delay_sec": HF_AUTORUN_DELAY_SEC,
                "started": _autorun_started,
            },
        }
    )
    return jsonify(payload)


@app.route("/api/hf/predictions")
def hf_predictions():
    payload = _load_predictions_payload()
    requested = request.args.get("date", "today")
    target_date = _resolve_prediction_date(requested, payload)
    predictions = _filter_predictions_for_date(payload, target_date)
    good_predictions = [row for row in predictions if not row.get("error")]
    return jsonify(
        {
            "date": target_date,
            "generated_at": payload.get("generated_at", ""),
            "prediction_count": len(good_predictions),
            "error_count": len(predictions) - len(good_predictions),
            "model_version": payload.get("model_version", ""),
            "model_type": payload.get("model_type", ""),
            "model_auc": payload.get("model_auc"),
            "predictions": predictions,
        }
    )


@app.route("/api/hf/dataset-stats")
def hf_dataset_stats():
    status = _load_json(HF_PIPELINE_STATUS_FILE, {})
    if not isinstance(status, dict):
        status = {}
    return jsonify(
        {
            "dataset_repo": status.get("dataset_repo", ""),
            "bootstrap_records": status.get("bootstrap_records", 0),
            "bootstrap_sports": status.get("bootstrap_sports", []),
            "bootstrap_date_range": status.get("bootstrap_date_range", ""),
            "bootstrap_completed_at": status.get("bootstrap_completed_at", ""),
            "last_append_records": status.get("last_append_records", status.get("append_records", 0)),
            "last_append_date": status.get("last_append_date", status.get("append_date", "")),
            "last_append_sports": status.get("last_append_sports", status.get("append_sports", [])),
            "last_append_completed_at": status.get(
                "last_append_completed_at", status.get("append_completed_at", "")
            ),
            "updated_at": status.get("updated_at", ""),
        }
    )


@app.route("/api/hf/model-history")
def hf_model_history():
    status = _load_json(HF_PIPELINE_STATUS_FILE, {})
    history = _load_json(HF_TRAINING_HISTORY_FILE, [])
    if not isinstance(status, dict):
        status = {}
    if not isinstance(history, list):
        history = []
    latest = history[-1] if history else {}
    latest = latest if isinstance(latest, dict) else {}
    return jsonify(
        {
            "model_repo": status.get("model_repo", ""),
            "current_version": latest.get("version") or status.get("model_version", ""),
            "current_model": latest.get("best_model") or status.get("best_model", ""),
            "current_auc": latest.get("cv_roc_auc") if history else status.get("cv_roc_auc"),
            "trained_rows": latest.get("rows") if history else status.get("trained_rows", 0),
            "sports_covered": latest.get("sports_covered") or status.get("sports_covered", []),
            "last_trained_at": latest.get("trained_at") or status.get("trained_at", ""),
            "history_count": len(history),
            "history": history,
        }
    )


_ensure_background_services()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000") or "5000")
    app.run(host="0.0.0.0", port=port, debug=False)
