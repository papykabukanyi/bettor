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
PROVIDER_API_URL = str(
    os.getenv("HF_SPACE_API_URL", "")
    or os.getenv("PREDICTIONS_API_URL", "")
    or ""
).strip().rstrip("/")
REQUEST_TIMEOUT = int(os.getenv("HF_PROXY_TIMEOUT", "15") or "15")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
app = Flask(__name__, template_folder="templates")


def _load_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def _provider_or_local(path: str, local_file: Path, *, default: Any) -> tuple[Any, bool, str]:
    error = ""
    if PROVIDER_API_URL:
        try:
            response = requests.get(urljoin(PROVIDER_API_URL + "/", path.lstrip("/")), timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json(), True, ""
        except Exception as exc:
            error = str(exc)
            logger.warning("Provider proxy failed for %s: %s", path, exc)
    if local_file.exists():
        return _load_json(local_file, default), False, error
    return default, False, error or "HF provider API URL is not configured and no local HF snapshot exists."


def _envelope(payload: Any, source_live: bool, error: str = "") -> dict[str, Any]:
    wrapped = dict(payload) if isinstance(payload, dict) else {"data": payload}
    wrapped.setdefault("ok", not bool(error))
    source = "hf_space" if source_live else "hf_local_snapshot"
    if not source_live and not PROVIDER_API_URL:
        source = "hf_local_snapshot_no_space_url"
    wrapped.setdefault("source", source)
    wrapped.setdefault("provider_configured", bool(PROVIDER_API_URL))
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


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/predictions/status")
def predictions_status():
    payload, source_live, error = _provider_or_local("/status", HF_STATUS_FILE, default={})
    if isinstance(payload, dict) and "pipeline" in payload and "metrics" in payload:
        return jsonify(_envelope(payload, source_live, error))

    status = payload if isinstance(payload, dict) else {}
    preds = _load_json(HF_PREDICTIONS_FILE, {})
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
            "polymarket": {"ok": True},
        },
        "metrics": {
            "total_predictions": int(preds.get("prediction_count") or 0),
            "today_predictions": len([p for p in (preds.get("predictions") or []) if p.get("game_date") == preds.get("today")]),
            "tomorrow_predictions": len([p for p in (preds.get("predictions") or []) if p.get("game_date") == preds.get("tomorrow")]),
            "active_models": 1 if model.get("best_model") else 0,
            "win_rate": float(model.get("best_score") or 0),
        },
        "model": model,
        "polymarket": {"submissions": {}, "positions": {}},
    }
    return jsonify(_envelope(transformed, source_live, error))


@app.route("/api/predictions/today")
def predictions_today():
    payload, source_live, error = _provider_or_local("/predictions/today", HF_PREDICTIONS_FILE, default={})
    if isinstance(payload, dict) and "date" in payload and "predictions" in payload:
        return jsonify(_envelope(payload, source_live, error))
    if not isinstance(payload, dict):
        return jsonify(_envelope({}, source_live, error))
    return jsonify(_envelope(_predictions_for_date(payload, str(payload.get("today") or "")), source_live, error))


@app.route("/api/predictions/tomorrow")
def predictions_tomorrow():
    payload, source_live, error = _provider_or_local("/predictions/tomorrow", HF_PREDICTIONS_FILE, default={})
    if isinstance(payload, dict) and "date" in payload and "predictions" in payload:
        return jsonify(_envelope(payload, source_live, error))
    if not isinstance(payload, dict):
        return jsonify(_envelope({}, source_live, error))
    return jsonify(_envelope(_predictions_for_date(payload, str(payload.get("tomorrow") or "")), source_live, error))


@app.route("/api/model/stats")
def model_stats():
    payload, source_live, error = _provider_or_local("/model/stats", HF_STATUS_FILE, default={})
    if isinstance(payload, dict) and "current_model" in payload:
        return jsonify(_envelope(payload, source_live, error))
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
    return jsonify(_envelope(transformed, source_live, error))


def _local_submissions_payload() -> dict[str, Any]:
    markets = _load_json(HF_MARKETS_FILE, {})
    rows = []
    summary = {"evaluated": 0, "placed": 0, "dry_run": 0, "failed": 0, "skipped": 0, "available_buying_power_usd": 0.0}
    for market in (markets.get("markets") or []):
        if not isinstance(market, dict):
            continue
        status = str(market.get("polymarket_status") or "unavailable").strip().lower()
        rows.append(
            {
                "submitted_at": market.get("detected_at") or "",
                "game": market.get("game") or "",
                "pick": market.get("pick") or "",
                "status": status or "unavailable",
                "price": market.get("polymarket_price"),
                "amount_usd": market.get("stake_usd") or 0,
                "reason": market.get("polymarket_message") or "",
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


@app.route("/api/polymarket/submissions")
def polymarket_submissions():
    payload, source_live, error = _provider_or_local("/polymarket/submissions", HF_MARKETS_FILE, default={})
    if isinstance(payload, dict) and "summary" in payload and "submissions" in payload:
        return jsonify(_envelope(payload, source_live, error))
    return jsonify(_envelope(_local_submissions_payload(), source_live, error))


@app.route("/api/polymarket/positions")
def polymarket_positions():
    payload, source_live, error = _provider_or_local("/polymarket/positions", HF_MARKETS_FILE, default={})
    if isinstance(payload, dict) and "summary" in payload and "positions" in payload:
        return jsonify(_envelope(payload, source_live, error))
    local = {"ok": True, "updated_at": "", "summary": {"active_positions": 0, "open_notional_usd": 0, "estimated_pnl_usd": 0}, "positions": []}
    return jsonify(_envelope(local, source_live, error))


@app.route("/api/polymarket/status")
def polymarket_status():
    sub = polymarket_submissions().get_json(silent=True) or {}
    pos = polymarket_positions().get_json(silent=True) or {}
    source = "hf_space" if (sub.get("source") == "hf_space" or pos.get("source") == "hf_space") else "hf_local_snapshot"
    payload = {
        "ok": True,
        "source": source,
        "provider_configured": bool(PROVIDER_API_URL),
        "warning": sub.get("warning") or pos.get("warning") or "",
        "updated_at": (sub.get("updated_at") or pos.get("updated_at") or ""),
        "submissions": sub,
        "positions": pos,
    }
    return jsonify(payload)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000") or "5000")
    app.run(host="0.0.0.0", port=port, debug=False)
