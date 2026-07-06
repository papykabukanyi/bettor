"""HF-backed web dashboard with optional HF Space API proxy."""

from __future__ import annotations

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

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
HF_STATUS_FILE = DATA_DIR / "hf_pipeline_status.json"
HF_PREDICTIONS_FILE = DATA_DIR / "hf_daily_predictions.json"
HF_HISTORY_FILE = DATA_DIR / "training_history.json"
HF_MARKETS_FILE = DATA_DIR / "hf_daily_prediction_markets.json"
KALSHI_AUTOMATION_STATE_FILE = DATA_DIR / "kalshi_automation_status.json"
KALSHI_SCHEDULE_STATE_FILE = DATA_DIR / "pregame_schedule.json"
REQUEST_TIMEOUT = int(os.getenv("HF_PROXY_TIMEOUT", "15") or "15")
DISCOVERY_TIMEOUT = int(os.getenv("HF_PROXY_DISCOVERY_TIMEOUT", "4") or "4")
HF_MODEL_REPO = str(os.getenv("HF_MODEL_REPO", "papylove/sportprediction") or "").strip()
HF_AUTORUN_ON_STARTUP = str(os.getenv("HF_AUTORUN_ON_STARTUP", "1")).strip().lower() in {"1", "true", "yes", "on"}
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
KALSHI_AUTOBET_DRY_RUN = str(os.getenv("AUTOBET_DRY_RUN", "1")).strip().lower() in {"1", "true", "yes", "on"}
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
_KALSHI_LIVE_CACHE: dict[str, Any] = {}
_KALSHI_LIVE_CACHE_TS = 0.0
_KALSHI_LIVE_CACHE_LOCK = threading.Lock()
_KALSHI_LIVE_CACHE_TTL_SEC = max(5, int(os.getenv("KALSHI_LIVE_CACHE_TTL_SEC", "20") or "20"))


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


def _space_repo_to_url(repo_id: str) -> str:
    value = str(repo_id or "").strip().strip("/")
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value.rstrip("/")
    if "/" not in value:
        return ""
    owner, space = value.split("/", 1)
    owner = owner.strip()
    space = space.strip()
    if not owner or not space:
        return ""
    return f"https://{owner}-{space}.hf.space"


def _discover_provider_api_url() -> tuple[str, str]:
    explicit = str(os.getenv("HF_SPACE_API_URL", "") or os.getenv("PREDICTIONS_API_URL", "")).strip().rstrip("/")
    if explicit:
        return explicit, "explicit_env"

    candidates: list[tuple[str, str]] = []
    for env_name in ("HF_SPACE_REPO", "HF_SPACE_ID", "SPACE_ID", "HF_MODEL_REPO", "HF_DATASET_REPO"):
        raw = str(os.getenv(env_name, "") or "").strip()
        url = _space_repo_to_url(raw)
        if url:
            candidates.append((url.rstrip("/"), env_name))

    seen: set[str] = set()
    for base_url, source in candidates:
        if base_url in seen:
            continue
        seen.add(base_url)
        try:
            health = requests.get(urljoin(base_url + "/", "health"), timeout=DISCOVERY_TIMEOUT)
            if health.ok:
                return base_url, f"auto:{source}"
        except Exception:
            pass
        try:
            status = requests.get(urljoin(base_url + "/", "status"), timeout=DISCOVERY_TIMEOUT)
            if status.ok:
                return base_url, f"auto:{source}"
        except Exception:
            pass
    return "", "none"


PROVIDER_API_URL, PROVIDER_API_SOURCE = _discover_provider_api_url()


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
    error = ""
    if PROVIDER_API_URL:
        try:
            response = requests.get(urljoin(PROVIDER_API_URL + "/", path.lstrip("/")), timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json(), "hf_space_auto" if PROVIDER_API_SOURCE.startswith("auto:") else "hf_space", ""
        except Exception as exc:
            error = str(exc)
            logger.warning("Provider proxy failed for %s: %s", path, exc)

    hub_artifact_map = {
        "/status": ["artifacts/hf_pipeline_status.json", "hf_pipeline_status.json"],
        "/predictions/today": ["artifacts/hf_daily_predictions.json", "hf_daily_predictions.json"],
        "/predictions/tomorrow": ["artifacts/hf_daily_predictions.json", "hf_daily_predictions.json"],
        "/model/stats": ["artifacts/hf_pipeline_status.json", "hf_pipeline_status.json"],
        "/kalshi/submissions": ["artifacts/hf_daily_prediction_markets.json", "hf_daily_prediction_markets.json"],
        "/kalshi/positions": ["artifacts/hf_daily_prediction_markets.json", "hf_daily_prediction_markets.json"],
    }
    artifact_paths = hub_artifact_map.get(path)
    if artifact_paths:
        payload = _fetch_first_hf_json(artifact_paths)
        if isinstance(payload, dict) and payload:
            return payload, "hf_hub_artifact", error

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
    if "kalshi private key" in lowered or "set kalshi_private_key" in lowered:
        return ""
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
    return run_kalshi_automation_cycle({})


def _ensure_background_jobs_started() -> None:
    global _startup_done
    if _startup_done:
        return
    if not DASHBOARD_LOCAL_AUTORUN:
        return
    if PROVIDER_API_URL and not _env_flag("DASHBOARD_FORCE_LOCAL_AUTORUN", default=False):
        return
    with _startup_lock:
        if _startup_done:
            return
        if not scheduler.running:
            scheduler.add_job(
                _run_hf_active_cycle,
                "interval",
                minutes=max(5, HF_ACTIVE_SCAN_MINUTES),
                id="hf_active_cycle",
                replace_existing=True,
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
            scheduler.start()
            logger.info(
                "Dashboard scheduler started: active every %d min, pregames every %d min, daily at %02d:%02d ET",
                max(5, HF_ACTIVE_SCAN_MINUTES),
                max(1, PREGAME_TIMING_MINUTES),
                HF_DAILY_RUN_HOUR_ET,
                HF_DAILY_RUN_MINUTE_ET,
            )
        if HF_AUTORUN_ON_STARTUP:
            def _runner() -> None:
                try:
                    _run_hf_active_cycle()
                    logger.info("Dashboard startup active cycle completed")
                except Exception as exc:
                    logger.exception("Dashboard startup active cycle failed: %s", exc)

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


def _prediction_counts_for_metrics() -> tuple[int, int, int]:
    today_payload, _, _ = _provider_or_local("/predictions/today", HF_PREDICTIONS_FILE, default={})
    tomorrow_payload, _, _ = _provider_or_local("/predictions/tomorrow", HF_PREDICTIONS_FILE, default={})

    today_count = int((today_payload or {}).get("prediction_count") or 0) if isinstance(today_payload, dict) else 0
    tomorrow_count = int((tomorrow_payload or {}).get("prediction_count") or 0) if isinstance(tomorrow_payload, dict) else 0
    return today_count + tomorrow_count, today_count, tomorrow_count


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/predictions/status")
def predictions_status():
    payload, source, error = _provider_or_local("/status", HF_STATUS_FILE, default={})
    if isinstance(payload, dict) and "pipeline" in payload and "metrics" in payload:
        metrics = payload.get("metrics") or {}
        if not int(metrics.get("total_predictions") or 0):
            total_predictions, today_predictions, tomorrow_predictions = _prediction_counts_for_metrics()
            metrics["total_predictions"] = total_predictions
            metrics["today_predictions"] = today_predictions
            metrics["tomorrow_predictions"] = tomorrow_predictions
            payload["metrics"] = metrics
        return jsonify(_envelope(payload, source, error))

    status = payload if isinstance(payload, dict) else {}
    total_predictions, today_predictions, tomorrow_predictions = _prediction_counts_for_metrics()
    model = {
        "best_model": status.get("best_model", ""),
        "version": status.get("model_version", ""),
        "best_score": status.get("cv_roc_auc", 0),
        "rows": status.get("trained_rows", 0),
        "sports_covered": status.get("sports_covered", []),
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
        },
        "model": model,
        "kalshi": {"submissions": {}, "positions": {}},
    }
    return jsonify(_envelope(transformed, source, error))


@app.route("/api/predictions/today")
def predictions_today():
    payload, source, error = _provider_or_local("/predictions/today", HF_PREDICTIONS_FILE, default={})
    if isinstance(payload, dict) and "date" in payload and "predictions" in payload:
        return jsonify(_envelope(payload, source, error))
    if not isinstance(payload, dict):
        return jsonify(_envelope({}, source, error))
    return jsonify(_envelope(_predictions_for_date(payload, str(payload.get("today") or "")), source, error))


@app.route("/api/predictions/tomorrow")
def predictions_tomorrow():
    payload, source, error = _provider_or_local("/predictions/tomorrow", HF_PREDICTIONS_FILE, default={})
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
        base_summary["available_buying_power_usd"] = float((account or {}).get("balance_usd") or 0.0)
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
                "available_buying_power_usd": float((account or {}).get("balance_usd") or 0.0),
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
            "available_buying_power_usd": float(((live.get("account") or live.get("balance") or {}).get("balance_usd") or 0.0)),
            "portfolio_value_usd": float(((live.get("account") or live.get("balance") or {}).get("portfolio_value_usd") or 0.0)),
        },
        "positions": all_positions,
        "balance": live.get("account") or live.get("balance") or {},
        "account": live.get("account") or live.get("balance") or {},
        "live": live,
    }
    if live.get("ok"):
        account = live.get("account") or live.get("balance") or {}
        submissions["summary"]["available_buying_power_usd"] = float((account or {}).get("balance_usd") or 0.0)
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
    return jsonify(
        {
            "ok": True,
            "automation": state,
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
    result = run_pregame_timing_cycle(
        dry_run=body.get("dry_run"),
        stake_usd=body.get("stake_usd"),
        max_single_orders=body.get("max_single_orders"),
        max_combo_orders=body.get("max_combo_orders"),
        include_combos=body.get("include_combos"),
    )
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
