"""HF-backed web dashboard with optional HF Space API proxy."""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from flask import Flask, jsonify, render_template, request

SRC_DIR = Path(__file__).resolve().parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

try:
    from src.data.kalshi_trade_api import (  # type: ignore
        build_live_snapshot,
        submit_prediction_orders,
    )
except Exception:
    from data.kalshi_trade_api import build_live_snapshot, submit_prediction_orders

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
HF_STATUS_FILE = DATA_DIR / "hf_pipeline_status.json"
HF_PREDICTIONS_FILE = DATA_DIR / "hf_daily_predictions.json"
HF_HISTORY_FILE = DATA_DIR / "training_history.json"
HF_MARKETS_FILE = DATA_DIR / "hf_daily_prediction_markets.json"
KALSHI_AUTOMATION_STATE_FILE = DATA_DIR / "kalshi_automation_status.json"
REQUEST_TIMEOUT = int(os.getenv("HF_PROXY_TIMEOUT", "15") or "15")
DISCOVERY_TIMEOUT = int(os.getenv("HF_PROXY_DISCOVERY_TIMEOUT", "4") or "4")
HF_MODEL_REPO = str(os.getenv("HF_MODEL_REPO", "papylove/sportprediction") or "").strip()

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
app = Flask(__name__, template_folder="templates")


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
    try:
        return build_live_snapshot()
    except Exception as exc:
        return {
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


@app.route("/api/kalshi/submissions")
def kalshi_submissions():
    payload, source, error = _provider_or_local("/kalshi/submissions", HF_MARKETS_FILE, default={})
    base = payload if isinstance(payload, dict) and "summary" in payload and "submissions" in payload else _local_submissions_payload()
    live = _live_kalshi_snapshot()
    if live.get("ok"):
        base_summary = base.get("summary") or {}
        base_summary["available_buying_power_usd"] = float((live.get("balance") or {}).get("balance_usd") or 0.0)
        base["summary"] = base_summary
    wrapped = _envelope(base, source, error or str(live.get("error") or ""))
    wrapped["live"] = live
    return jsonify(wrapped)


@app.route("/api/kalshi/positions")
def kalshi_positions():
    payload, source, error = _provider_or_local("/kalshi/positions", HF_MARKETS_FILE, default={})
    live = _live_kalshi_snapshot()
    if live.get("ok"):
        transformed = {
            "ok": True,
            "updated_at": live.get("updated_at") or "",
            "summary": {
                "active_positions": int(live.get("open_orders_count") or 0),
                "open_notional_usd": float(live.get("open_notional_usd") or 0.0),
                "estimated_pnl_usd": 0.0,
                "available_buying_power_usd": float((live.get("balance") or {}).get("balance_usd") or 0.0),
                "portfolio_value_usd": float((live.get("balance") or {}).get("portfolio_value_usd") or 0.0),
            },
            "positions": live.get("open_orders") or [],
            "balance": live.get("balance") or {},
        }
        wrapped = _envelope(transformed, "kalshi_live", error)
        wrapped["live"] = live
        return jsonify(wrapped)
    if isinstance(payload, dict) and "summary" in payload and "positions" in payload:
        wrapped = _envelope(payload, source, error or str(live.get("error") or ""))
        wrapped["live"] = live
        return jsonify(wrapped)
    local = {"ok": False, "updated_at": "", "summary": {"active_positions": 0, "open_notional_usd": 0, "estimated_pnl_usd": 0}, "positions": [], "balance": {}}
    wrapped = _envelope(local, source, error or str(live.get("error") or ""))
    wrapped["live"] = live
    return jsonify(wrapped)


@app.route("/api/kalshi/status")
def kalshi_status():
    sub = kalshi_submissions().get_json(silent=True) or {}
    pos = kalshi_positions().get_json(silent=True) or {}
    live = pos.get("live") or sub.get("live") or _live_kalshi_snapshot()
    if (sub.get("source") in {"hf_space", "hf_space_auto"} or pos.get("source") in {"hf_space", "hf_space_auto"}):
        source = "hf_space"
    elif (sub.get("source") == "hf_hub_artifact" or pos.get("source") == "hf_hub_artifact"):
        source = "hf_hub_artifact"
    elif pos.get("source") == "kalshi_live":
        source = "kalshi_live"
    else:
        source = "hf_local_snapshot"
    payload = {
        "ok": bool(live.get("ok") or sub.get("ok") or pos.get("ok")),
        "source": source,
        "provider_configured": bool(PROVIDER_API_URL),
        "provider_url": PROVIDER_API_URL,
        "provider_source": PROVIDER_API_SOURCE,
        "warning": sub.get("warning") or pos.get("warning") or str(live.get("error") or ""),
        "updated_at": (sub.get("updated_at") or pos.get("updated_at") or ""),
        "submissions": sub,
        "positions": pos,
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
                "stake_usd": _env_float("KALSHI_AUTOBET_STAKE_USD", 1.0),
                "max_single_orders": _env_int("KALSHI_AUTOBET_MAX_SINGLE_ORDERS", 1),
                "max_combo_orders": _env_int("KALSHI_AUTOBET_MAX_COMBO_ORDERS", 1),
                "combo_enabled": _env_flag("KALSHI_AUTO_CREATE_COMBOS", default=True),
                "combo_live_enabled": _env_flag("KALSHI_AUTO_PLACE_COMBOS", default=True),
            },
        }
    )


@app.route("/api/kalshi/automation/tick", methods=["GET", "POST"])
def kalshi_automation_tick():
    if not _is_cron_authorized():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    body = request.get_json(silent=True) or {}
    stake_usd = float(body.get("stake_usd") or _env_float("KALSHI_AUTOBET_STAKE_USD", 1.0))
    max_single_orders = int(body.get("max_single_orders") or _env_int("KALSHI_AUTOBET_MAX_SINGLE_ORDERS", 1))
    max_combo_orders = int(body.get("max_combo_orders") or _env_int("KALSHI_AUTOBET_MAX_COMBO_ORDERS", 1))
    combo_enabled = bool(body.get("combo_enabled", _env_flag("KALSHI_AUTO_CREATE_COMBOS", default=True)))
    combo_live_enabled = bool(body.get("combo_live_enabled", _env_flag("KALSHI_AUTO_PLACE_COMBOS", default=True)))

    live_enabled = _env_flag("KALSHI_LIVE_TRADING_ENABLED", default=False)
    dry_run = not live_enabled

    payload, _, _ = _provider_or_local("/predictions/today", HF_PREDICTIONS_FILE, default={})
    if not isinstance(payload, dict) or not isinstance(payload.get("predictions"), list):
        payload = _load_json(HF_PREDICTIONS_FILE, {})
    if not isinstance(payload, dict) or not isinstance(payload.get("predictions"), list):
        return jsonify({"ok": False, "error": "Prediction data unavailable."}), 400

    try:
        single_orders = submit_prediction_orders(
            payload,
            stake_usd=stake_usd,
            max_orders=max_single_orders,
            dry_run=dry_run,
        )

        combo_result: dict[str, Any] = {
            "ok": True,
            "combo_enabled": combo_enabled,
            "submitted_count": 0,
            "suggested_count": 0,
            "matched_count": 0,
            "submitted": [],
            "suggestions": [],
        }
        if combo_enabled:
            from data.kalshi_trade_api import build_combo_suggestions_from_predictions, submit_combo_orders

            combo_suggestions = build_combo_suggestions_from_predictions(payload, max_combos=max(5, max_combo_orders * 5))
            combo_result["suggestions"] = combo_suggestions
            combo_result["suggested_count"] = len(combo_suggestions)
            combo_result["matched_count"] = sum(
                1 for combo in combo_suggestions if str(combo.get("kalshi_status") or "").strip().lower() == "matched"
            )
            combo_orders = submit_combo_orders(
                combo_suggestions,
                stake_usd=stake_usd,
                max_orders=max_combo_orders,
                dry_run=(dry_run or (not combo_live_enabled)),
            )
            combo_result.update(combo_orders)

        state = {
            "ok": True,
            "updated_at": single_orders.get("updated_at"),
            "dry_run": dry_run,
            "live_enabled": live_enabled,
            "stake_usd": stake_usd,
            "single_orders": single_orders,
            "combo_orders": combo_result,
        }
        _save_json(KALSHI_AUTOMATION_STATE_FILE, state)
        return jsonify(state)
    except Exception as exc:
        state = {"ok": False, "updated_at": "", "error": str(exc)}
        _save_json(KALSHI_AUTOMATION_STATE_FILE, state)
        return jsonify(state), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000") or "5000")
    app.run(host="0.0.0.0", port=port, debug=False)
