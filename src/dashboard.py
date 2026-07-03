"""Modal-backed web dashboard."""

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
LOCAL_MODAL_DATA = ROOT_DIR / "modal_data"
PROVIDER_API_URL = str(os.getenv("PREDICTIONS_API_URL", "") or os.getenv("MODAL_API_URL", "") or "").strip().rstrip("/")
REQUEST_TIMEOUT = int(os.getenv("MODAL_PROXY_TIMEOUT", "12") or "12")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
app = Flask(__name__, template_folder="templates")


def _load_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def _modal_or_local(path: str, local_file: Path, *, default: Any) -> tuple[Any, bool, str]:
    error = ""
    if PROVIDER_API_URL:
        try:
            response = requests.get(urljoin(PROVIDER_API_URL + "/", path.lstrip("/")), timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json(), True, ""
        except Exception as exc:
            error = str(exc)
            logger.warning("Modal proxy failed for %s: %s", path, exc)
    if local_file.exists():
        return _load_json(local_file, default), False, error
    return default, False, error or "No provider API URL is configured and no local prediction snapshot exists."


def _envelope(payload: Any, source_live: bool, error: str = "") -> dict[str, Any]:
    if isinstance(payload, dict):
        wrapped = dict(payload)
    else:
        wrapped = {"data": payload}
    wrapped.setdefault("ok", not bool(error))
    wrapped.setdefault("source", "modal" if source_live else "local")
    if error:
        wrapped.setdefault("warning", error)
    return wrapped


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/predictions/status")
def predictions_status():
    payload, source_live, error = _modal_or_local("/status", LOCAL_MODAL_DATA / "pipeline" / "status.json", default={})
    if not isinstance(payload, dict) or "metrics" not in payload:
        pipeline = payload if isinstance(payload, dict) else {}
        payload = {
            "ok": bool(pipeline),
            "updated_at": pipeline.get("updated_at", "") if isinstance(pipeline, dict) else "",
            "pipeline": pipeline,
            "metrics": {},
            "model": _load_json(LOCAL_MODAL_DATA / "models" / "model_stats.json", {}),
            "polymarket": {
                "submissions": (_load_json(LOCAL_MODAL_DATA / "polymarket" / "submissions.json", {}) or {}).get("summary", {}),
                "positions": (_load_json(LOCAL_MODAL_DATA / "polymarket" / "positions.json", {}) or {}).get("summary", {}),
            },
        }
    return jsonify(_envelope(payload, source_live, error))


@app.route("/api/predictions/today")
def predictions_today():
    payload, source_live, error = _modal_or_local("/predictions/today", LOCAL_MODAL_DATA / "predictions" / "latest.json", default={})
    if isinstance(payload, dict) and "predictions" in payload and "date" not in payload:
        target_date = str(payload.get("today") or "")
        payload = {
            "ok": True,
            "date": target_date,
            "generated_at": payload.get("generated_at", ""),
            "prediction_count": len([p for p in payload.get("predictions", []) if str((p or {}).get("game_date") or "") == target_date]),
            "model_version": payload.get("model_version", ""),
            "model_name": payload.get("model_name", ""),
            "predictions": [p for p in payload.get("predictions", []) if str((p or {}).get("game_date") or "") == target_date],
        }
    return jsonify(_envelope(payload, source_live, error))


@app.route("/api/predictions/tomorrow")
def predictions_tomorrow():
    payload, source_live, error = _modal_or_local("/predictions/tomorrow", LOCAL_MODAL_DATA / "predictions" / "latest.json", default={})
    if isinstance(payload, dict) and "predictions" in payload and "date" not in payload:
        target_date = str(payload.get("tomorrow") or "")
        payload = {
            "ok": True,
            "date": target_date,
            "generated_at": payload.get("generated_at", ""),
            "prediction_count": len([p for p in payload.get("predictions", []) if str((p or {}).get("game_date") or "") == target_date]),
            "model_version": payload.get("model_version", ""),
            "model_name": payload.get("model_name", ""),
            "predictions": [p for p in payload.get("predictions", []) if str((p or {}).get("game_date") or "") == target_date],
        }
    return jsonify(_envelope(payload, source_live, error))


@app.route("/api/model/stats")
def model_stats():
    payload, source_live, error = _modal_or_local("/model/stats", LOCAL_MODAL_DATA / "models" / "model_stats.json", default={})
    if isinstance(payload, dict) and "current_model" not in payload:
        payload = {
            "ok": bool(payload),
            "updated_at": payload.get("trained_at", "") if isinstance(payload, dict) else "",
            "current_model": payload if isinstance(payload, dict) else {},
            "history": _load_json(LOCAL_MODAL_DATA / "models" / "training_history.json", []),
        }
    return jsonify(_envelope(payload, source_live, error))


@app.route("/api/polymarket/submissions")
def polymarket_submissions():
    payload, source_live, error = _modal_or_local("/polymarket/submissions", LOCAL_MODAL_DATA / "polymarket" / "submissions.json", default={})
    return jsonify(_envelope(payload, source_live, error))


@app.route("/api/polymarket/positions")
def polymarket_positions():
    payload, source_live, error = _modal_or_local("/polymarket/positions", LOCAL_MODAL_DATA / "polymarket" / "positions.json", default={})
    return jsonify(_envelope(payload, source_live, error))


@app.route("/api/polymarket/status")
def polymarket_status():
    submissions = _load_json(LOCAL_MODAL_DATA / "polymarket" / "submissions.json", {})
    positions = _load_json(LOCAL_MODAL_DATA / "polymarket" / "positions.json", {})
    if PROVIDER_API_URL:
        try:
            sub_resp = requests.get(urljoin(PROVIDER_API_URL + "/", "polymarket/submissions"), timeout=REQUEST_TIMEOUT)
            pos_resp = requests.get(urljoin(PROVIDER_API_URL + "/", "polymarket/positions"), timeout=REQUEST_TIMEOUT)
            sub_resp.raise_for_status()
            pos_resp.raise_for_status()
            submissions = sub_resp.json()
            positions = pos_resp.json()
            source = "modal"
            warning = ""
        except Exception as exc:
            source = "local"
            warning = str(exc)
    else:
        source = "local"
        warning = ""
    payload = {
        "ok": True,
        "source": source,
        "warning": warning,
        "updated_at": (submissions or {}).get("updated_at") or (positions or {}).get("updated_at") or "",
        "submissions": submissions,
        "positions": positions,
    }
    return jsonify(payload)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000") or "5000")
    app.run(host="0.0.0.0", port=port, debug=False)
