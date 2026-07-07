"""HF-backed web dashboard with optional HF Space API proxy."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, render_template, request

SRC_DIR = Path(__file__).resolve().parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from data.hf_pipeline import HFDirectPipeline
from data.kalshi_trade_api import build_live_snapshot, submit_prediction_orders
from data.pregame_timing import run_pregame_timing_cycle
from data.multi_sport_scheduler import (
    run_multi_sport_live_fetch,
    get_multi_sport_scheduler_status,
)

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
HF_STATUS_FILE = DATA_DIR / "hf_pipeline_status.json"
HF_PREDICTIONS_FILE = DATA_DIR / "hf_daily_predictions.json"
HF_HISTORY_FILE = DATA_DIR / "training_history.json"
HF_MARKETS_FILE = DATA_DIR / "hf_daily_prediction_markets.json"
KALSHI_AUTOMATION_STATE_FILE = DATA_DIR / "kalshi_automation_status.json"
KALSHI_SCHEDULE_STATE_FILE = DATA_DIR / "pregame_schedule.json"
REQUEST_TIMEOUT = int(os.getenv("HF_PROXY_TIMEOUT", "15") or "15")
HF_MODEL_REPO = str(os.getenv("HF_MODEL_REPO", "papylove/sportprediction") or "").strip()
HF_AUTORUN_ON_STARTUP = str(os.getenv("HF_AUTORUN_ON_STARTUP", "0")).strip().lower() in {"1", "true", "yes", "on"}
HF_DAILY_RUN_HOUR_ET = int(os.getenv("HF_DAILY_RUN_HOUR_ET", "4") or "4")
HF_DAILY_RUN_MINUTE_ET = int(os.getenv("HF_DAILY_RUN_MINUTE_ET", "15") or "15")
HF_DAILY_CUSTOM_MODEL = str(os.getenv("HF_DAILY_CUSTOM_MODEL", "auto") or "auto").strip().lower()
HF_DAILY_MIN_TRAIN_ROWS = int(os.getenv("HF_DAILY_MIN_TRAIN_ROWS", "200") or "200")
HF_BOOTSTRAP_ON_EMPTY = str(os.getenv("HF_BOOTSTRAP_ON_EMPTY", "1")).strip().lower() in {"1", "true", "yes", "on"}
HF_BOOTSTRAP_DAYS = int(os.getenv("HF_BOOTSTRAP_DAYS", "365") or "365")
HF_ACTIVE_SCAN_MINUTES = int(os.getenv("HF_ACTIVE_SCAN_MINUTES", "30") or "30")
HF_ACTIVE_APPEND_DAYS = int(os.getenv("HF_ACTIVE_APPEND_DAYS", "3") or "3")
HF_RETRAIN_INTERVAL_MINUTES = int(os.getenv("HF_RETRAIN_INTERVAL_MINUTES", "180") or "180")
HF_ATTACH_KALSHI = str(os.getenv("HF_ATTACH_KALSHI", "1")).strip().lower() in {"1", "true", "yes", "on"}
KALSHI_AUTOBET_ENABLED = str(os.getenv("KALSHI_AUTOBET_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
KALSHI_LIVE_TRADING_ENABLED = str(os.getenv("KALSHI_LIVE_TRADING_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "on"}
KALSHI_AUTOBET_DRY_RUN = str(os.getenv("AUTOBET_DRY_RUN", "1" if not KALSHI_LIVE_TRADING_ENABLED else "0")).strip().lower() in {"1", "true", "yes", "on"}
KALSHI_AUTOBET_STAKE_USD = float(os.getenv("KALSHI_AUTOBET_STAKE_USD", "1.0") or "1.0")
KALSHI_AUTOBET_MAX_SINGLE_ORDERS = int(os.getenv("KALSHI_AUTOBET_MAX_SINGLE_ORDERS", "1") or "1")
KALSHI_AUTOBET_MAX_COMBO_ORDERS = int(os.getenv("KALSHI_AUTOBET_MAX_COMBO_ORDERS", "1") or "1")
PREGAME_TIMING_MINUTES = int(os.getenv("PREGAME_TIMING_MINUTES", "5") or "5")
DASHBOARD_LOCAL_AUTORUN = str(os.getenv("DASHBOARD_LOCAL_AUTORUN", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
app = Flask(__name__, template_folder="templates")
scheduler = BackgroundScheduler(timezone="America/New_York")
_startup_lock = threading.Lock()
_startup_done = False
_prediction_refresh_lock = threading.Lock()
_prediction_refresh_last_ts = 0.0
_prediction_refresh_running = False
_KALSHI_LIVE_CACHE: dict[str, Any] = {}
_KALSHI_LIVE_CACHE_TS = 0.0
_KALSHI_LIVE_CACHE_LOCK = threading.Lock()
_KALSHI_LIVE_CACHE_TTL_SEC = max(5, int(os.getenv("KALSHI_LIVE_CACHE_TTL_SEC", "20") or "20"))


def _bootstrap_env_from_dotenv() -> None:
    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        return
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return
    idx = 0
    while idx < len(lines):
        raw = lines[idx].strip()
        idx += 1
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if key == "KALSHI_PRIVATE_KEY" and "BEGIN RSA PRIVATE KEY" in value and "END RSA PRIVATE KEY" not in value:
            chunks = [value]
            while idx < len(lines):
                part = lines[idx].rstrip("\r")
                chunks.append(part)
                idx += 1
                if "END RSA PRIVATE KEY" in part:
                    break
            value = "\n".join(chunks)
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


_bootstrap_env_from_dotenv()


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        raw = str(os.getenv(name, str(default)) or str(default)).strip()
        return float(raw)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        raw = str(os.getenv(name, str(default)) or str(default)).strip()
        return int(raw)
    except Exception:
        return int(default)


def _is_cron_authorized() -> bool:
    secret = str(os.getenv("CRON_SECRET", "") or "").strip()
    if not secret:
        return True
    auth = str(request.headers.get("authorization") or "")
    return auth == f"Bearer {secret}"




def _discover_provider_api_url() -> tuple[str, str]:
    # Only use an explicitly-configured provider URL; no auto-probing HF Spaces.
    explicit = str(os.getenv("HF_SPACE_API_URL", "") or os.getenv("PREDICTIONS_API_URL", "")).strip().rstrip("/")
    if explicit:
        return explicit, "explicit_env"
    return "", "none"


PROVIDER_API_URL, PROVIDER_API_SOURCE = _discover_provider_api_url()
HF_PREFER_LOCAL_SNAPSHOT = str(os.getenv("HF_PREFER_LOCAL_SNAPSHOT", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}


def _load_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def _save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _normalize_hf_repo_id(value: str) -> str:
    raw = str(value or "").strip().strip("/")
    if not raw or "/" not in raw:
        return ""
    owner, name = raw.split("/", 1)
    owner = owner.strip()
    name = name.strip()
    if not owner or not name:
        return ""
    return f"{owner}/{name}"


def _fetch_hf_model_artifact_json(path_in_repo: str) -> Any:
    repo_id = _normalize_hf_repo_id(HF_MODEL_REPO)
    if not repo_id:
        return None
    url = f"https://huggingface.co/{repo_id}/resolve/main/{path_in_repo.lstrip('/')}"
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception:
        return None


def _fetch_first_hf_json(paths_in_repo: list[str]) -> Any:
    for candidate in paths_in_repo:
        payload = _fetch_hf_model_artifact_json(candidate)
        if isinstance(payload, dict) and payload:
            return payload
    return None


def _provider_or_local(path: str, local_file: Path, *, default: Any) -> tuple[Any, str, str]:
    def _rows_and_sports(payload: Any) -> tuple[int, int]:
        if not isinstance(payload, dict):
            return 0, 0
        rows = payload.get("predictions")
        if not isinstance(rows, list):
            return 0, 0
        sports: set[str] = set()
        count = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            count += 1
            sport = str(row.get("sport") or "").strip().lower()
            if sport:
                sports.add(sport)
        return count, len(sports)

    def _merge_prediction_payloads(candidates: list[tuple[Any, str]]) -> tuple[Any, str]:
        if not candidates:
            return default, "hf_local_snapshot_no_space_url"

        merged_rows: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        selected_source = ""
        selected_score = (-1, -1)
        today_value = ""
        tomorrow_value = ""
        model_version = ""
        model_type = ""
        model_auc = 0.0
        generated_at = ""

        for payload, source in candidates:
            if not isinstance(payload, dict):
                continue
            score = _rows_and_sports(payload)
            if score[1] > selected_score[1] or (score[1] == selected_score[1] and score[0] > selected_score[0]):
                selected_source = source
                selected_score = score
            if not today_value:
                today_value = str(payload.get("today") or "").strip()
            if not tomorrow_value:
                tomorrow_value = str(payload.get("tomorrow") or "").strip()
            if not generated_at:
                generated_at = str(payload.get("generated_at") or "").strip()
            if not model_version:
                model_version = str(payload.get("model_version") or "").strip()
            if not model_type:
                model_type = str(payload.get("model_type") or payload.get("model_name") or "").strip()
            if not model_auc:
                try:
                    model_auc = float(payload.get("model_auc") or 0.0)
                except Exception:
                    model_auc = 0.0

            for row in (payload.get("predictions") or []):
                if not isinstance(row, dict):
                    continue
                row_id = str(row.get("prediction_id") or row.get("prediction_uid") or "").strip()
                if not row_id:
                    row_id = "|".join(
                        [
                            str(row.get("sport") or "").strip().lower(),
                            str(row.get("league") or "").strip().lower(),
                            str(row.get("game_date") or "").strip(),
                            str(row.get("game_time") or "").strip(),
                            str(row.get("away_team") or "").strip().lower(),
                            str(row.get("home_team") or "").strip().lower(),
                            str(row.get("market_type") or "").strip().lower(),
                            str(row.get("player_name") or "").strip().lower(),
                            str(row.get("predicted_team") or row.get("predicted_label") or row.get("predicted_outcome") or "").strip().lower(),
                        ]
                    )
                if row_id in seen_ids:
                    continue
                seen_ids.add(row_id)
                merged_rows.append(row)

        if not merged_rows:
            return default, "hf_local_snapshot_no_space_url"

        if not today_value:
            today_value = dt.date.today().isoformat()
        if not tomorrow_value:
            tomorrow_value = (dt.date.today() + dt.timedelta(days=1)).isoformat()

        merged_payload = {
            "ok": True,
            "generated_at": generated_at,
            "today": today_value,
            "tomorrow": tomorrow_value,
            "prediction_count": len([row for row in merged_rows if not row.get("error")]),
            "model_version": model_version,
            "model_type": model_type,
            "model_auc": model_auc,
            "predictions": merged_rows,
        }
        return merged_payload, (selected_source or "merged_sources")

    error = ""
    is_prediction_path = path in {"/predictions/today", "/predictions/tomorrow"}
    prediction_candidates: list[tuple[Any, str]] = []

    if local_file.exists():
        local_payload = _load_json(local_file, default)
        if isinstance(local_payload, dict) and local_payload:
            if HF_PREFER_LOCAL_SNAPSHOT and not is_prediction_path:
                return local_payload, "hf_local_snapshot", ""
            if is_prediction_path:
                prediction_candidates.append((local_payload, "hf_local_snapshot"))
    if PROVIDER_API_URL:
        try:
            response = requests.get(urljoin(PROVIDER_API_URL + "/", path.lstrip("/")), timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            provider_source = "hf_space_auto" if PROVIDER_API_SOURCE.startswith("auto:") else "hf_space"
            provider_payload = response.json()
            if is_prediction_path and isinstance(provider_payload, dict) and provider_payload:
                prediction_candidates.append((provider_payload, provider_source))
            else:
                return provider_payload, provider_source, ""
        except Exception as exc:
            error = str(exc)
            logger.warning("Provider proxy failed for %s: %s", path, exc)

    hub_artifact_map = {
        "/status": ["artifacts/hf_pipeline_status.json", "hf_pipeline_status.json"],
        "/model/stats": ["artifacts/hf_pipeline_status.json", "hf_pipeline_status.json"],
        "/kalshi/submissions": ["artifacts/hf_daily_prediction_markets.json", "hf_daily_prediction_markets.json"],
        "/kalshi/positions": ["artifacts/hf_daily_prediction_markets.json", "hf_daily_prediction_markets.json"],
    }
    # Prediction paths use ONLY local file (scheduler updates it; HF artifact is stale)
    artifact_paths = hub_artifact_map.get(path)
    if artifact_paths:
        payload = _fetch_first_hf_json(artifact_paths)
        if isinstance(payload, dict) and payload:
            if is_prediction_path:
                prediction_candidates.append((payload, "hf_hub_artifact"))
            else:
                return payload, "hf_hub_artifact", error

    if is_prediction_path and prediction_candidates:
        merged_payload, merged_source = _merge_prediction_payloads(prediction_candidates)
        return merged_payload, merged_source, error

    if local_file.exists():
        return _load_json(local_file, default), "hf_local_snapshot", error
    return default, "hf_local_snapshot_no_space_url", error or "HF provider API URL is not configured and no local HF snapshot exists."


def _envelope(payload: Any, source: str, error: str = "") -> dict[str, Any]:
    wrapped = dict(payload) if isinstance(payload, dict) else {"data": payload}
    wrapped.setdefault("ok", not bool(error))
    wrapped.setdefault("source", source)
    wrapped.setdefault("provider_configured", bool(PROVIDER_API_URL) or bool(_normalize_hf_repo_id(HF_MODEL_REPO)))
    wrapped.setdefault("provider_url", PROVIDER_API_URL)
    wrapped.setdefault("provider_source", PROVIDER_API_SOURCE)
    wrapped.setdefault("hf_model_repo", _normalize_hf_repo_id(HF_MODEL_REPO))
    if error:
        wrapped.setdefault("warning", error)
    return wrapped


def _sanitize_kalshi_warning(message: str | None) -> str:
    text = str(message or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if "kalshi private key" in lowered or "set kalshi_private_key" in lowered or "kalshi api key" in lowered:
        return "Kalshi credentials are not loading on the server."
    return text


def _refresh_combo_artifact(predictions_payload: dict[str, Any]) -> dict[str, Any]:
    from data.kalshi_trade_api import build_combo_suggestions_from_predictions

    combos = build_combo_suggestions_from_predictions(
        predictions_payload,
        max_combos=max(1, int(os.getenv("KALSHI_COMBO_ARTIFACT_MAX", "50") or "50")),
    )
    payload = {
        "ok": True,
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "combo_count": len(combos),
        "combos": combos,
    }
    _save_json(DATA_DIR / "hf_daily_prediction_combos.json", payload)
    return payload


def _auto_place_kalshi_from_predictions(predictions_payload: dict[str, Any]) -> dict[str, Any] | None:
    if not KALSHI_AUTOBET_ENABLED:
        return None
    try:
        return submit_prediction_orders(
            predictions_payload,
            dry_run=KALSHI_AUTOBET_DRY_RUN,
            stake_usd=max(1.0, KALSHI_AUTOBET_STAKE_USD),
            max_orders=max(1, KALSHI_AUTOBET_MAX_SINGLE_ORDERS),
            include_combos=True,
            max_combos=max(0, KALSHI_AUTOBET_MAX_COMBO_ORDERS),
        )
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _run_hf_daily_pipeline() -> dict[str, Any]:
    pipeline = HFDirectPipeline()
    if not pipeline.ok:
        raise RuntimeError("HF pipeline not configured. Set HF_API_KEY, HF_DATASET_REPO, HF_MODEL_REPO.")

    status_payload = _load_json(HF_STATUS_FILE, {})
    if HF_BOOTSTRAP_ON_EMPTY and not status_payload.get("bootstrap_completed_at"):
        pipeline.bootstrap_one_year_history(days_back=HF_BOOTSTRAP_DAYS)

    result = pipeline.run_daily_pipeline(
        custom_model=HF_DAILY_CUSTOM_MODEL,
        min_rows=HF_DAILY_MIN_TRAIN_ROWS,
    )

    if HF_ATTACH_KALSHI:
        from betting_bot import _attach_market_context

        _attach_market_context(
            predictions_path=str(HF_PREDICTIONS_FILE),
            output_path=str(HF_MARKETS_FILE),
        )

    predictions_payload = _load_json(HF_PREDICTIONS_FILE, {})
    sync_pregame_schedule(predictions_payload if isinstance(predictions_payload, dict) else {})
    combo_artifact = {"ok": False, "combo_count": 0, "combos": []}
    if isinstance(predictions_payload, dict) and isinstance(predictions_payload.get("predictions"), list):
        combo_artifact = _refresh_combo_artifact(predictions_payload)
    kalshi_placement = _auto_place_kalshi_from_predictions(predictions_payload)
    if isinstance(result, dict):
        result["kalshi_combo_artifact"] = combo_artifact
        result["kalshi_placement"] = kalshi_placement
    return result


def _run_hf_active_cycle() -> dict[str, Any]:
    pipeline = HFDirectPipeline()
    if not pipeline.ok:
        raise RuntimeError("HF pipeline not configured. Set HF_API_KEY, HF_DATASET_REPO, HF_MODEL_REPO.")

    today = dt.date.today()
    append_runs: list[dict[str, Any]] = []
    for offset in range(max(1, HF_ACTIVE_APPEND_DAYS)):
        target_day = today - dt.timedelta(days=offset)
        append_runs.append(pipeline.append_daily_results(target_day))

    append_t = append_runs[0] if append_runs else {"ok": True, "records": 0}
    append_y = append_runs[1] if len(append_runs) > 1 else {"ok": True, "records": 0}
    new_records_total = sum(int(r.get("records") or 0) for r in append_runs)

    try:
        meta = pipeline._get_model_metadata()  # noqa: SLF001
    except Exception:
        meta = {}
    retrain_needed = not meta.get("version")
    trained_at = str(meta.get("trained_at") or "").strip()
    if trained_at:
        try:
            trained_dt = dt.datetime.fromisoformat(trained_at.replace("Z", "+00:00"))
            elapsed_min = (dt.datetime.now(dt.timezone.utc) - trained_dt).total_seconds() / 60.0
            if elapsed_min >= max(15, HF_RETRAIN_INTERVAL_MINUTES):
                retrain_needed = True
        except Exception:
            retrain_needed = True
    if new_records_total > 0 and HF_RETRAIN_INTERVAL_MINUTES <= 60:
        retrain_needed = True
    if retrain_needed:
        pipeline.train_and_publish_best_model(
            min_rows=HF_DAILY_MIN_TRAIN_ROWS,
            forced_model=HF_DAILY_CUSTOM_MODEL,
        )

    pipeline.ensure_model_card_metadata()
    preds = pipeline.predict_daily_schedule()
    if HF_ATTACH_KALSHI:
        from betting_bot import _attach_market_context

        _attach_market_context(
            predictions_path=str(HF_PREDICTIONS_FILE),
            output_path=str(HF_MARKETS_FILE),
        )
    predictions_payload = _load_json(HF_PREDICTIONS_FILE, {})
    sync_pregame_schedule(predictions_payload if isinstance(predictions_payload, dict) else {})
    combo_artifact = {"ok": False, "combo_count": 0, "combos": []}
    if isinstance(predictions_payload, dict) and isinstance(predictions_payload.get("predictions"), list):
        combo_artifact = _refresh_combo_artifact(predictions_payload)
    kalshi_placement = _auto_place_kalshi_from_predictions(predictions_payload)
    pipeline.publish_runtime_artifacts()
    return {
        "ok": True,
        "append_yesterday": append_y,
        "append_today": append_t,
        "append_runs": append_runs,
        "new_records_total": new_records_total,
        "retrained": retrain_needed,
        "predictions": preds,
        "kalshi_combo_artifact": combo_artifact,
        "kalshi_placement": kalshi_placement,
    }


def _run_kalshi_automation_background() -> dict[str, Any]:
    result = run_kalshi_automation_cycle({})
    _save_json(KALSHI_AUTOMATION_STATE_FILE, result)
    return result


def _ensure_background_jobs_started() -> None:
    global _startup_done
    if _startup_done:
        return
    if not DASHBOARD_LOCAL_AUTORUN:
        return
    with _startup_lock:
        if _startup_done:
            return
        if not scheduler.running:
            _first_active = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)
            scheduler.add_job(
                _run_hf_active_cycle,
                "interval",
                minutes=max(5, HF_ACTIVE_SCAN_MINUTES),
                id="hf_active_cycle",
                replace_existing=True,
                next_run_time=_first_active,
            )
            scheduler.add_job(
                _run_hf_daily_pipeline,
                "cron",
                hour=HF_DAILY_RUN_HOUR_ET,
                minute=HF_DAILY_RUN_MINUTE_ET,
                id="hf_daily_pipeline",
                replace_existing=True,
            )
            scheduler.add_job(
                _run_kalshi_automation_background,
                "interval",
                minutes=max(1, PREGAME_TIMING_MINUTES),
                id="kalshi_automation_cycle",
                replace_existing=True,
            )
            # Multi-sport data pipeline: fetch live games every 30 minutes
            scheduler.add_job(
                run_multi_sport_live_fetch,
                "interval",
                minutes=30,
                id="multi_sport_live_fetch",
                replace_existing=True,
            )
            scheduler.start()
            logger.info(
                "Dashboard scheduler started: active every %d min, pregames every %d min, daily at %02d:%02d ET, multi-sport every 30 min",
                max(5, HF_ACTIVE_SCAN_MINUTES),
                max(1, PREGAME_TIMING_MINUTES),
                HF_DAILY_RUN_HOUR_ET,
                HF_DAILY_RUN_MINUTE_ET,
            )

            # Always run a lightweight predictions refresh on startup (no training, no HF push).
            # This ensures the predictions file is fresh for all sports without risking OOM.
            def _startup_predict_only() -> None:
                try:
                    pipeline = HFDirectPipeline()
                    if pipeline.ok:
                        pipeline.predict_daily_schedule()
                        logger.info("Dashboard startup predictions generated")
                except Exception as exc:
                    logger.warning("Dashboard startup predictions failed: %s", exc)

            threading.Thread(target=_startup_predict_only, daemon=True, name="dashboard-startup-predict").start()

        if HF_AUTORUN_ON_STARTUP:
            def _runner() -> None:
                # Full active cycle (append + train + predict + Kalshi) — only if explicitly enabled
                try:
                    _run_hf_active_cycle()
                    logger.info("Dashboard startup active cycle completed")
                except Exception as exc:
                    logger.exception("Dashboard startup active cycle failed: %s", exc)
                try:
                    _run_kalshi_automation_background()
                    logger.info("Dashboard startup Kalshi automation cycle completed")
                except Exception as exc:
                    logger.exception("Dashboard startup Kalshi automation cycle failed: %s", exc)

            threading.Thread(target=_runner, daemon=True, name="dashboard-hf-startup-autorun").start()
        _startup_done = True


@app.before_request
def _bootstrap_background_jobs() -> None:
    _ensure_background_jobs_started()


def _predictions_for_date(payload: dict[str, Any], target_date: str) -> dict[str, Any]:
    rows = [p for p in (payload.get("predictions") or []) if str((p or {}).get("game_date") or "") == target_date]
    return {
        "ok": True,
        "date": target_date,
        "generated_at": payload.get("generated_at", ""),
        "prediction_count": len(rows),
        "model_version": payload.get("model_version", ""),
        "model_name": payload.get("model_type", ""),
        "predictions": rows,
    }


def _extract_prediction_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    rows = payload.get("predictions")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _prediction_sports(payload: dict[str, Any]) -> set[str]:
    sports: set[str] = set()
    for row in _extract_prediction_rows(payload):
        sport = str(row.get("sport") or "").strip().lower()
        if sport:
            sports.add(sport)
    return sports


def _prediction_payload_needs_multisport_refresh(payload: dict[str, Any]) -> bool:
    rows = _extract_prediction_rows(payload)
    if not rows:
        return True
    sports = _prediction_sports(payload)
    return bool(sports) and sports.issubset({"cricket"})


def _trigger_background_multisport_refresh_if_needed(payload: dict[str, Any], *, reason: str) -> None:
    global _prediction_refresh_last_ts, _prediction_refresh_running
    if not isinstance(payload, dict):
        return
    if not _prediction_payload_needs_multisport_refresh(payload):
        return
    now_ts = time.time()
    refresh_cooldown = max(120, int(os.getenv("HF_PREDICTION_REFRESH_COOLDOWN_SEC", "300") or "300"))
    with _prediction_refresh_lock:
        if _prediction_refresh_running:
            return
        if (now_ts - _prediction_refresh_last_ts) < refresh_cooldown:
            return
        _prediction_refresh_last_ts = now_ts
        _prediction_refresh_running = True

    def _runner() -> None:
        global _prediction_refresh_running
        try:
            _run_hf_active_cycle()
            logger.info("Triggered background multi-sport refresh (%s)", reason)
        except Exception as exc:
            logger.warning("Background multi-sport refresh failed (%s): %s", reason, exc)
        finally:
            with _prediction_refresh_lock:
                _prediction_refresh_running = False

    threading.Thread(target=_runner, daemon=True, name="dashboard-multisport-refresh").start()


def _prediction_metrics() -> tuple[int, int, int, dict[str, int], list[str]]:
    today_payload, _, _ = _provider_or_local("/predictions/today", HF_PREDICTIONS_FILE, default={})
    tomorrow_payload, _, _ = _provider_or_local("/predictions/tomorrow", HF_PREDICTIONS_FILE, default={})

    today_count = int((today_payload or {}).get("prediction_count") or 0) if isinstance(today_payload, dict) else 0
    tomorrow_count = int((tomorrow_payload or {}).get("prediction_count") or 0) if isinstance(tomorrow_payload, dict) else 0
    all_rows = _extract_prediction_rows(today_payload if isinstance(today_payload, dict) else {})
    all_rows += _extract_prediction_rows(tomorrow_payload if isinstance(tomorrow_payload, dict) else {})

    sport_counts: dict[str, int] = {}
    for row in all_rows:
        sport = str(row.get("sport") or "").strip().lower()
        if not sport:
            continue
        sport_counts[sport] = sport_counts.get(sport, 0) + 1
    sports = sorted(sport_counts.keys())
    return today_count + tomorrow_count, today_count, tomorrow_count, sport_counts, sports


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/predictions/status")
def predictions_status():
    payload, source, error = _provider_or_local("/status", HF_STATUS_FILE, default={})
    if isinstance(payload, dict) and "pipeline" in payload and "metrics" in payload:
        metrics = payload.get("metrics") or {}
        total_predictions, today_predictions, tomorrow_predictions, sport_counts, sports = _prediction_metrics()
        metrics["total_predictions"] = int(metrics.get("total_predictions") or total_predictions)
        metrics["today_predictions"] = int(metrics.get("today_predictions") or today_predictions)
        metrics["tomorrow_predictions"] = int(metrics.get("tomorrow_predictions") or tomorrow_predictions)
        metrics["sport_breakdown"] = sport_counts
        payload["metrics"] = metrics
        model_payload = payload.get("model") or {}
        if isinstance(model_payload, dict):
            model_payload["sports_covered"] = model_payload.get("sports_covered") or sports
            payload["model"] = model_payload
        return jsonify(_envelope(payload, source, error))

    status = payload if isinstance(payload, dict) else {}
    total_predictions, today_predictions, tomorrow_predictions, sport_counts, sports = _prediction_metrics()
    model = {
        "best_model": status.get("best_model", ""),
        "version": status.get("model_version", ""),
        "best_score": status.get("cv_roc_auc", 0),
        "rows": status.get("trained_rows", 0),
        "sports_covered": status.get("sports_covered", []) or sports,
    }
    transformed = {
        "ok": bool(status),
        "updated_at": status.get("updated_at", ""),
        "pipeline": {
            "fetch": {
                "ok": status.get("ok", False),
                "updated_at": status.get("append_completed_at", ""),
                "completed_games_total": status.get("append_records", 0),
            },
            "train": {
                "ok": bool(status.get("best_model")),
                "trained_at": status.get("trained_at", ""),
                "rows": status.get("trained_rows", 0),
                "best_model": status.get("best_model", ""),
            },
            "predict": {
                "ok": status.get("prediction_count", 0) >= 0,
                "generated_at": status.get("predict_completed_at", ""),
                "prediction_count": status.get("prediction_count", 0),
            },
            "kalshi": {"ok": True},
        },
        "metrics": {
            "total_predictions": total_predictions,
            "today_predictions": today_predictions,
            "tomorrow_predictions": tomorrow_predictions,
            "active_models": 1 if model.get("best_model") else 0,
            "win_rate": float(model.get("best_score") or 0),
            "sport_breakdown": sport_counts,
        },
        "model": model,
        "kalshi": {"submissions": {}, "positions": {}},
    }
    return jsonify(_envelope(transformed, source, error))


@app.route("/api/predictions/today")
def predictions_today():
    payload, source, error = _provider_or_local("/predictions/today", HF_PREDICTIONS_FILE, default={})
    if isinstance(payload, dict):
        _trigger_background_multisport_refresh_if_needed(payload, reason="api/predictions/today")
    if isinstance(payload, dict) and "date" in payload and "predictions" in payload:
        return jsonify(_envelope(payload, source, error))
    if not isinstance(payload, dict):
        return jsonify(_envelope({}, source, error))
    return jsonify(_envelope(_predictions_for_date(payload, str(payload.get("today") or "")), source, error))


@app.route("/api/predictions/tomorrow")
def predictions_tomorrow():
    payload, source, error = _provider_or_local("/predictions/tomorrow", HF_PREDICTIONS_FILE, default={})
    if isinstance(payload, dict):
        _trigger_background_multisport_refresh_if_needed(payload, reason="api/predictions/tomorrow")
    if isinstance(payload, dict) and "date" in payload and "predictions" in payload:
        return jsonify(_envelope(payload, source, error))
    if not isinstance(payload, dict):
        return jsonify(_envelope({}, source, error))
    return jsonify(_envelope(_predictions_for_date(payload, str(payload.get("tomorrow") or "")), source, error))


@app.route("/api/model/stats")
def model_stats():
    payload, source, error = _provider_or_local("/model/stats", HF_STATUS_FILE, default={})
    if isinstance(payload, dict) and "current_model" in payload:
        return jsonify(_envelope(payload, source, error))
    status = payload if isinstance(payload, dict) else {}
    transformed = {
        "ok": bool(status),
        "updated_at": status.get("trained_at", ""),
        "current_model": {
            "best_model": status.get("best_model", ""),
            "version": status.get("model_version", ""),
            "best_score": status.get("cv_roc_auc", 0),
            "rows": status.get("trained_rows", 0),
            "sports_covered": status.get("sports_covered", []),
            "candidate_count": 3,
        },
        "history": _load_json(HF_HISTORY_FILE, []),
    }
    return jsonify(_envelope(transformed, source, error))


def _local_submissions_payload() -> dict[str, Any]:
    markets = _load_json(HF_MARKETS_FILE, {})
    rows = []
    summary = {"evaluated": 0, "placed": 0, "dry_run": 0, "failed": 0, "skipped": 0, "available_buying_power_usd": 0.0}
    for market in (markets.get("markets") or []):
        if not isinstance(market, dict):
            continue
        status = str(market.get("kalshi_status") or "unavailable").strip().lower()
        rows.append(
            {
                "submitted_at": market.get("detected_at") or "",
                "game": market.get("game") or "",
                "pick": market.get("pick") or "",
                "status": status or "unavailable",
                "price": market.get("kalshi_price_cents"),
                "amount_usd": market.get("stake_usd") or 0,
                "reason": market.get("kalshi_message") or "",
                "ticker": market.get("kalshi_ticker") or "",
            }
        )
        summary["evaluated"] += 1
        if status == "matched":
            summary["placed"] += 1
        elif status in {"unavailable", "done"}:
            summary["skipped"] += 1
        elif status in {"error", "failed"}:
            summary["failed"] += 1
    return {"ok": True, "updated_at": markets.get("generated_at", ""), "summary": summary, "submissions": rows}


def _merge_prediction_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_id = str(row.get("prediction_id") or row.get("prediction_uid") or "").strip()
        if not row_id:
            row_id = "|".join(
                [
                    str(row.get("sport") or ""),
                    str(row.get("game_date") or ""),
                    str(row.get("game_time") or ""),
                    str(row.get("home_team") or ""),
                    str(row.get("away_team") or ""),
                    str(row.get("market_type") or ""),
                    str(row.get("predicted_team") or row.get("predicted_label") or row.get("predicted_outcome") or ""),
                ]
            )
        if row_id in seen:
            continue
        seen.add(row_id)
        merged.append(row)
    return merged


def _automation_predictions_payload() -> dict[str, Any]:
    local_payload = _load_json(HF_PREDICTIONS_FILE, {})
    today_payload, _, _ = _provider_or_local("/predictions/today", HF_PREDICTIONS_FILE, default={})
    tomorrow_payload, _, _ = _provider_or_local("/predictions/tomorrow", HF_PREDICTIONS_FILE, default={})
    rows: list[dict[str, Any]] = []
    for payload in (today_payload, tomorrow_payload, local_payload):
        if isinstance(payload, dict):
            rows.extend([row for row in (payload.get("predictions") or []) if isinstance(row, dict)])
    merged = _merge_prediction_rows(rows)
    model_version = ""
    for payload in (today_payload, tomorrow_payload, local_payload):
        if isinstance(payload, dict):
            candidate = str(payload.get("model_version") or "").strip()
            if candidate:
                model_version = candidate
                break
    return {
        "ok": True,
        "source": "automation_merged",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "model_version": model_version,
        "predictions": merged,
    }


def _live_kalshi_snapshot() -> dict[str, Any]:
    global _KALSHI_LIVE_CACHE_TS, _KALSHI_LIVE_CACHE
    now = time.monotonic()
    with _KALSHI_LIVE_CACHE_LOCK:
        if _KALSHI_LIVE_CACHE and (now - _KALSHI_LIVE_CACHE_TS) < _KALSHI_LIVE_CACHE_TTL_SEC:
            return dict(_KALSHI_LIVE_CACHE)
    try:
        snapshot = build_live_snapshot()
    except Exception as exc:
        snapshot = {
            "ok": False,
            "updated_at": "",
            "error": str(exc),
            "balance": {
                "balance_cents": 0,
                "balance_usd": 0.0,
                "balance_dollars": "0",
                "portfolio_value_cents": 0,
                "portfolio_value_usd": 0.0,
                "updated_ts": None,
            },
            "open_orders": [],
            "open_orders_count": 0,
            "open_notional_usd": 0.0,
        }
    with _KALSHI_LIVE_CACHE_LOCK:
        _KALSHI_LIVE_CACHE = dict(snapshot)
        _KALSHI_LIVE_CACHE_TS = time.monotonic()
    return snapshot


@app.route("/api/kalshi/submissions")
def kalshi_submissions():
    payload, source, error = _provider_or_local("/kalshi/submissions", HF_MARKETS_FILE, default={})
    base = payload if isinstance(payload, dict) and "summary" in payload and "submissions" in payload else _local_submissions_payload()
    live = _live_kalshi_snapshot()
    if live.get("ok"):
        base_summary = base.get("summary") or {}
        account = live.get("account") or live.get("balance") or {}
        base_summary["available_buying_power_usd"] = float((account or {}).get("buying_power_usd") or (account or {}).get("balance_usd") or 0.0)
        base["summary"] = base_summary
    wrapped = _envelope(base, "kalshi_live" if live.get("ok") else source, error or str(live.get("error") or ""))
    wrapped["live"] = live
    return jsonify(wrapped)


@app.route("/api/kalshi/positions")
def kalshi_positions():
    payload, source, error = _provider_or_local("/kalshi/positions", HF_MARKETS_FILE, default={})
    live = _live_kalshi_snapshot()
    if live.get("ok"):
        account = live.get("account") or live.get("balance") or {}
        positions = live.get("all_positions") or live.get("positions") or live.get("market_positions") or live.get("open_orders") or []
        transformed = {
            "ok": True,
            "updated_at": live.get("updated_at") or "",
            "summary": {
                "active_positions": int(live.get("position_count") or 0),
                "open_notional_usd": float(live.get("open_notional_usd") or 0.0),
                "estimated_pnl_usd": sum(float((row or {}).get("realized_pnl_dollars") or 0.0) for row in positions if isinstance(row, dict)),
                "available_buying_power_usd": float((account or {}).get("buying_power_usd") or (account or {}).get("balance_usd") or 0.0),
                "portfolio_value_usd": float((account or {}).get("portfolio_value_usd") or 0.0),
            },
            "positions": positions,
            "balance": account,
            "account": account,
        }
        wrapped = _envelope(transformed, "kalshi_live", error)
        wrapped["live"] = live
        return jsonify(wrapped)
    if isinstance(payload, dict) and "summary" in payload and "positions" in payload:
        wrapped = _envelope(payload, source, error or str(live.get("error") or ""))
        wrapped["live"] = live
        return jsonify(wrapped)
    local = {"ok": False, "updated_at": "", "summary": {"active_positions": 0, "open_notional_usd": 0, "estimated_pnl_usd": 0}, "positions": [], "balance": {}, "account": {}}
    wrapped = _envelope(local, source, error or str(live.get("error") or ""))
    wrapped["live"] = live
    return jsonify(wrapped)


@app.route("/api/kalshi/status")
def kalshi_status():
    live = _live_kalshi_snapshot()
    submissions = _local_submissions_payload()
    all_positions = live.get("all_positions") or live.get("positions") or live.get("market_positions") or []
    account_snapshot = live.get("account") or live.get("balance") or {}
    positions = {
        "ok": bool(live.get("ok")),
        "updated_at": live.get("updated_at") or "",
        "summary": {
            "active_positions": int(live.get("position_count") or 0),
            "open_notional_usd": float(live.get("open_notional_usd") or 0.0),
            "estimated_pnl_usd": sum(
                float((row or {}).get("realized_pnl_dollars") or 0.0)
                for row in all_positions
                if isinstance(row, dict)
            ),
            "available_buying_power_usd": float((account_snapshot or {}).get("buying_power_usd") or (account_snapshot or {}).get("balance_usd") or 0.0),
            "portfolio_value_usd": float((account_snapshot or {}).get("portfolio_value_usd") or 0.0),
        },
        "positions": all_positions,
        "balance": account_snapshot,
        "account": account_snapshot,
        "live": live,
    }
    if live.get("ok"):
        account = live.get("account") or live.get("balance") or {}
        submissions["summary"]["available_buying_power_usd"] = float((account or {}).get("buying_power_usd") or (account or {}).get("balance_usd") or 0.0)
    source = "kalshi_live" if live.get("ok") else "hf_local_snapshot"
    payload = {
        "ok": bool(live.get("ok")),
        "source": source,
        "provider_configured": bool(PROVIDER_API_URL),
        "provider_url": PROVIDER_API_URL,
        "provider_source": PROVIDER_API_SOURCE,
        "warning": _sanitize_kalshi_warning(str(live.get("error") or "")),
        "updated_at": live.get("updated_at") or "",
        "submissions": submissions,
        "positions": positions,
        "live": live,
    }
    return jsonify(payload)


@app.route("/api/kalshi/live")
def kalshi_live():
    live = _live_kalshi_snapshot()
    return jsonify(live)


@app.route("/api/kalshi/place-from-predictions", methods=["POST"])
def kalshi_place_from_predictions():
    body = request.get_json(silent=True) or {}
    dry_run = bool(body.get("dry_run", True))
    stake_usd = float(body.get("stake_usd", 1.0) or 1.0)
    max_orders = int(body.get("max_orders", 1) or 1)
    include_combos = bool(body.get("include_combos", _env_flag("KALSHI_AUTO_CREATE_COMBOS", default=True)))
    max_combos = int(body.get("max_combos", _env_int("KALSHI_AUTOBET_MAX_COMBO_ORDERS", 1)) or 1)
    if not dry_run and not _env_flag("KALSHI_LIVE_TRADING_ENABLED", default=False):
        return jsonify(
            {
                "ok": False,
                "error": "Live trading is disabled. Set KALSHI_LIVE_TRADING_ENABLED=1 to allow order placement.",
            }
        ), 400

    payload, _, _ = _provider_or_local("/predictions/today", HF_PREDICTIONS_FILE, default={})
    if not isinstance(payload, dict) or not isinstance(payload.get("predictions"), list):
        payload = _load_json(HF_PREDICTIONS_FILE, {})
    if not isinstance(payload, dict) or not isinstance(payload.get("predictions"), list):
        return jsonify({"ok": False, "error": "Prediction data unavailable."}), 400
    try:
        result = submit_prediction_orders(
            payload,
            stake_usd=stake_usd,
            max_orders=max_orders,
            dry_run=dry_run,
            include_combos=include_combos,
            max_combos=max(0, max_combos),
        )
        return jsonify(result)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/kalshi/automation/status")
def kalshi_automation_status():
    state = _load_json(KALSHI_AUTOMATION_STATE_FILE, {})
    if not isinstance(state, dict):
        state = {}
    current_feed_payload = _automation_predictions_payload()
    current_feed = {
        "source": str(current_feed_payload.get("source") or ""),
        "prediction_count": len(current_feed_payload.get("predictions") or []),
        "model_version": str(current_feed_payload.get("model_version") or ""),
    }
    state_feed = state.get("predictions_feed")
    if not isinstance(state_feed, dict):
        state_feed = {}
    state_count = int(state_feed.get("prediction_count") or 0)
    current_count = int(current_feed.get("prediction_count") or 0)
    effective_feed = dict(state_feed)
    if current_count >= state_count:
        effective_feed.update(current_feed)
    state["predictions_feed"] = effective_feed
    return jsonify(
        {
            "ok": True,
            "automation": state,
            "predictions_feed": effective_feed,
            "settings": {
                "analysis_lead_minutes": _env_int("PREGAME_ANALYSIS_LEAD_MINUTES", 90),
                "bet_lead_minutes": _env_int("PREGAME_BET_LEAD_MINUTES", 60),
                "min_confidence": _env_int("AUTOBET_MIN_CONFIDENCE", 52),
                "stake_usd": _env_float("KALSHI_AUTOBET_STAKE_USD", 1.0),
                "max_single_orders": _env_int("KALSHI_AUTOBET_MAX_SINGLE_ORDERS", 1),
                "max_combo_orders": _env_int("KALSHI_AUTOBET_MAX_COMBO_ORDERS", 1),
            },
        }
    )


def run_kalshi_automation_cycle(body: dict[str, Any] | None = None) -> dict[str, Any]:
    body = body or {}
    predictions_payload = _automation_predictions_payload()
    result = run_pregame_timing_cycle(
        dry_run=body.get("dry_run"),
        stake_usd=body.get("stake_usd"),
        max_single_orders=body.get("max_single_orders"),
        max_combo_orders=body.get("max_combo_orders"),
        include_combos=body.get("include_combos"),
        predictions_payload=predictions_payload,
    )
    result["predictions_feed"] = {
        "source": str(predictions_payload.get("source") or ""),
        "prediction_count": len(predictions_payload.get("predictions") or []),
        "model_version": str(predictions_payload.get("model_version") or ""),
    }
    _save_json(KALSHI_AUTOMATION_STATE_FILE, result)
    return result


@app.route("/api/kalshi/automation/tick", methods=["GET", "POST"])
def kalshi_automation_tick():
    if not _is_cron_authorized():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    body = request.get_json(silent=True) or {}

    try:
        state = run_kalshi_automation_cycle(body)
        return jsonify(state)
    except ValueError as exc:
        state = {"ok": False, "updated_at": "", "error": str(exc)}
        _save_json(KALSHI_AUTOMATION_STATE_FILE, state)
        return jsonify(state), 400
    except Exception as exc:
        state = {"ok": False, "updated_at": "", "error": str(exc)}
        _save_json(KALSHI_AUTOMATION_STATE_FILE, state)
        return jsonify(state), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000") or "5000")
    _ensure_background_jobs_started()
    app.run(host="0.0.0.0", port=port, debug=False)
