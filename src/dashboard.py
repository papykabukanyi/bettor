"""HF-backed web dashboard with optional HF Space API proxy."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from flask import Flask, jsonify, render_template

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
HF_STATUS_FILE = DATA_DIR / "hf_pipeline_status.json"
HF_PREDICTIONS_FILE = DATA_DIR / "hf_daily_predictions.json"
HF_HISTORY_FILE = DATA_DIR / "training_history.json"
HF_MARKETS_FILE = DATA_DIR / "hf_daily_prediction_markets.json"
REQUEST_TIMEOUT = int(os.getenv("HF_PROXY_TIMEOUT", "15") or "15")
DISCOVERY_TIMEOUT = int(os.getenv("HF_PROXY_DISCOVERY_TIMEOUT", "4") or "4")
HF_MODEL_REPO = str(os.getenv("HF_MODEL_REPO", "papylove/sportprediction") or "").strip()

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
app = Flask(__name__, template_folder="templates")


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


@app.route("/api/kalshi/submissions")
def kalshi_submissions():
    payload, source, error = _provider_or_local("/kalshi/submissions", HF_MARKETS_FILE, default={})
    if isinstance(payload, dict) and "summary" in payload and "submissions" in payload:
        return jsonify(_envelope(payload, source, error))
    return jsonify(_envelope(_local_submissions_payload(), source, error))


@app.route("/api/kalshi/positions")
def kalshi_positions():
    payload, source, error = _provider_or_local("/kalshi/positions", HF_MARKETS_FILE, default={})
    if isinstance(payload, dict) and "summary" in payload and "positions" in payload:
        return jsonify(_envelope(payload, source, error))
    local = {"ok": True, "updated_at": "", "summary": {"active_positions": 0, "open_notional_usd": 0, "estimated_pnl_usd": 0}, "positions": []}
    return jsonify(_envelope(local, source, error))


@app.route("/api/kalshi/status")
def kalshi_status():
    sub = kalshi_submissions().get_json(silent=True) or {}
    pos = kalshi_positions().get_json(silent=True) or {}
    if (sub.get("source") in {"hf_space", "hf_space_auto"} or pos.get("source") in {"hf_space", "hf_space_auto"}):
        source = "hf_space"
    elif (sub.get("source") == "hf_hub_artifact" or pos.get("source") == "hf_hub_artifact"):
        source = "hf_hub_artifact"
    else:
        source = "hf_local_snapshot"
    payload = {
        "ok": True,
        "source": source,
        "provider_configured": bool(PROVIDER_API_URL),
        "provider_url": PROVIDER_API_URL,
        "provider_source": PROVIDER_API_SOURCE,
        "warning": sub.get("warning") or pos.get("warning") or "",
        "updated_at": (sub.get("updated_at") or pos.get("updated_at") or ""),
        "submissions": sub,
        "positions": pos,
    }
    return jsonify(payload)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000") or "5000")
    app.run(host="0.0.0.0", port=port, debug=False)
