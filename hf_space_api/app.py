from __future__ import annotations

import datetime as dt
import json
import logging
import os
import sys
import threading
from pathlib import Path
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from data.hf_pipeline import HFDirectPipeline  # noqa: E402
from data.kalshi_trade_api import build_live_snapshot, submit_prediction_orders  # noqa: E402

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("hf_space_api")

DATA_DIR = ROOT_DIR / "data"
STATUS_FILE = DATA_DIR / "hf_pipeline_status.json"
PRED_FILE = DATA_DIR / "hf_daily_predictions.json"
HISTORY_FILE = DATA_DIR / "training_history.json"
MARKETS_FILE = DATA_DIR / "hf_daily_prediction_markets.json"
COMBOS_FILE = DATA_DIR / "hf_daily_prediction_combos.json"

HF_AUTORUN_ON_STARTUP = str(os.getenv("HF_AUTORUN_ON_STARTUP", "1")).strip().lower() in {"1", "true", "yes", "on"}
HF_DAILY_RUN_HOUR_ET = int(os.getenv("HF_DAILY_RUN_HOUR_ET", "4") or "4")
HF_DAILY_RUN_MINUTE_ET = int(os.getenv("HF_DAILY_RUN_MINUTE_ET", "15") or "15")
HF_DAILY_CUSTOM_MODEL = str(os.getenv("HF_DAILY_CUSTOM_MODEL", "auto") or "auto").strip().lower()
HF_DAILY_MIN_TRAIN_ROWS = int(os.getenv("HF_DAILY_MIN_TRAIN_ROWS", "200") or "200")
HF_ATTACH_KALSHI = str(os.getenv("HF_ATTACH_KALSHI", "1")).strip().lower() in {"1", "true", "yes", "on"}
HF_BOOTSTRAP_ON_EMPTY = str(os.getenv("HF_BOOTSTRAP_ON_EMPTY", "1")).strip().lower() in {"1", "true", "yes", "on"}
HF_BOOTSTRAP_DAYS = int(os.getenv("HF_BOOTSTRAP_DAYS", "365") or "365")
HF_ACTIVE_SCAN_MINUTES = int(os.getenv("HF_ACTIVE_SCAN_MINUTES", "30") or "30")
HF_ACTIVE_APPEND_DAYS = int(os.getenv("HF_ACTIVE_APPEND_DAYS", "3") or "3")
HF_RETRAIN_INTERVAL_MINUTES = int(os.getenv("HF_RETRAIN_INTERVAL_MINUTES", "180") or "180")
KALSHI_AUTOBET_ENABLED = str(os.getenv("KALSHI_AUTOBET_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
KALSHI_AUTOBET_DRY_RUN = str(os.getenv("AUTOBET_DRY_RUN", "1")).strip().lower() in {"1", "true", "yes", "on"}
KALSHI_AUTOBET_STAKE_USD = float(os.getenv("KALSHI_AUTOBET_STAKE_USD", "1.0") or "1.0")
KALSHI_AUTOBET_MAX_SINGLE_ORDERS = int(os.getenv("KALSHI_AUTOBET_MAX_SINGLE_ORDERS", "1") or "1")
KALSHI_AUTOBET_MAX_COMBO_ORDERS = int(os.getenv("KALSHI_AUTOBET_MAX_COMBO_ORDERS", "1") or "1")
KALSHI_COMBO_ARTIFACT_MAX = int(os.getenv("KALSHI_COMBO_ARTIFACT_MAX", "50") or "50")

app = FastAPI(title="Bettor HF Space API", version="1.0.0")
scheduler = BackgroundScheduler(timezone="America/New_York")
_startup_lock = threading.Lock()
_startup_done = False


class KalshiOrderRequest(BaseModel):
    dry_run: bool = True
    stake_usd: float = 1.0
    max_orders: int = 1
    include_combos: bool = True
    max_combo_orders: int = 1


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


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _refresh_combo_artifact(predictions_payload: dict[str, Any]) -> dict[str, Any]:
    from data.kalshi_trade_api import build_combo_suggestions_from_predictions

    combos = build_combo_suggestions_from_predictions(
        predictions_payload,
        max_combos=max(1, KALSHI_COMBO_ARTIFACT_MAX),
    )
    payload = {
        "ok": True,
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "combo_count": len(combos),
        "combos": combos,
    }
    _save_json(COMBOS_FILE, payload)
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
    status_payload = _load_json(STATUS_FILE, {})
    if HF_BOOTSTRAP_ON_EMPTY and not status_payload.get("bootstrap_completed_at"):
        logger.info("HF bootstrap on empty status (days=%s)", HF_BOOTSTRAP_DAYS)
        pipeline.bootstrap_one_year_history(days_back=HF_BOOTSTRAP_DAYS)
    result = pipeline.run_daily_pipeline(
        custom_model=HF_DAILY_CUSTOM_MODEL,
        min_rows=HF_DAILY_MIN_TRAIN_ROWS,
    )
    if HF_ATTACH_KALSHI:
        from betting_bot import _attach_market_context  # local import to avoid startup import side effects

        _attach_market_context(
            predictions_path=str(PRED_FILE),
            output_path=str(DATA_DIR / "hf_daily_prediction_markets.json"),
        )
    predictions_payload = _load_json(PRED_FILE, {})
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
    append_y = append_runs[1] if len(append_runs) > 1 else {"ok": True, "records": 0}
    append_t = append_runs[0] if append_runs else {"ok": True, "records": 0}
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
            predictions_path=str(PRED_FILE),
            output_path=str(DATA_DIR / "hf_daily_prediction_markets.json"),
        )
    predictions_payload = _load_json(PRED_FILE, {})
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


def _predictions_for_date(payload: dict[str, Any], date_value: str) -> dict[str, Any]:
    all_rows = payload.get("predictions") or []
    rows = [p for p in all_rows if str((p or {}).get("game_date") or "") == date_value]
    effective_date = date_value
    if not rows:
        future_dates = sorted(
            {
                str((p or {}).get("game_date") or "")
                for p in all_rows
                if str((p or {}).get("game_date") or "") >= str(date_value or "")
            }
        )
        if future_dates:
            effective_date = future_dates[0]
            rows = [p for p in all_rows if str((p or {}).get("game_date") or "") == effective_date]
    return {
        "ok": True,
        "date": effective_date,
        "requested_date": date_value,
        "generated_at": payload.get("generated_at", ""),
        "prediction_count": len(rows),
        "model_version": payload.get("model_version", ""),
        "model_name": payload.get("model_type", ""),
        "predictions": rows,
    }


def _local_submissions_payload() -> dict[str, Any]:
    markets = _load_json(MARKETS_FILE, {})
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


@app.on_event("startup")
def _startup() -> None:
    global _startup_done
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
            scheduler.start()
            logger.info(
                "HF scheduler started: active every %d min, daily at %02d:%02d ET",
                max(5, HF_ACTIVE_SCAN_MINUTES),
                HF_DAILY_RUN_HOUR_ET,
                HF_DAILY_RUN_MINUTE_ET,
            )
        if HF_AUTORUN_ON_STARTUP:
            def _runner() -> None:
                try:
                    _run_hf_active_cycle()
                    logger.info("HF startup active cycle completed")
                except Exception as exc:
                    logger.exception("HF startup active cycle failed: %s", exc)

            threading.Thread(target=_runner, daemon=True, name="hf-startup-autorun").start()
        _startup_done = True


@app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "time": dt.datetime.now(dt.timezone.utc).isoformat()}


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "ok": True,
        "service": "bettor-hf-space-api",
        "routes": [
            "/health",
            "/status",
            "/predictions/today",
            "/predictions/tomorrow",
            "/model/stats",
            "/kalshi/submissions",
            "/kalshi/positions",
            "/kalshi/live",
            "/run/bootstrap",
            "/run/daily",
            "/run/active",
        ],
    }


@app.get("/status")
def status() -> dict[str, Any]:
    status_payload = _load_json(STATUS_FILE, {})
    pred_payload = _load_json(PRED_FILE, {})
    return {
        "ok": bool(status_payload),
        "updated_at": status_payload.get("updated_at", ""),
        "pipeline": status_payload,
        "metrics": {
            "total_predictions": int(pred_payload.get("prediction_count") or 0),
            "today_predictions": len([p for p in (pred_payload.get("predictions") or []) if p.get("game_date") == pred_payload.get("today")]),
            "tomorrow_predictions": len([p for p in (pred_payload.get("predictions") or []) if p.get("game_date") == pred_payload.get("tomorrow")]),
            "active_models": 1 if status_payload.get("best_model") else 0,
            "win_rate": float(status_payload.get("cv_roc_auc") or 0),
        },
        "model": {
            "best_model": status_payload.get("best_model", ""),
            "version": status_payload.get("model_version", ""),
            "best_score": status_payload.get("cv_roc_auc", 0),
            "rows": status_payload.get("trained_rows", 0),
            "sports_covered": status_payload.get("sports_covered", []),
            "candidate_count": 3,
        },
    }


@app.get("/predictions/today")
def predictions_today() -> dict[str, Any]:
    payload = _load_json(PRED_FILE, {})
    if not isinstance(payload, dict) or not payload:
        try:
            _run_hf_active_cycle()
            payload = _load_json(PRED_FILE, {})
        except Exception:
            payload = {}
    if not isinstance(payload, dict) or not payload:
        return {"ok": True, "date": "", "requested_date": "", "prediction_count": 0, "predictions": []}
    return _predictions_for_date(payload, str(payload.get("today") or ""))


@app.get("/predictions/tomorrow")
def predictions_tomorrow() -> dict[str, Any]:
    payload = _load_json(PRED_FILE, {})
    if not isinstance(payload, dict) or not payload:
        try:
            _run_hf_active_cycle()
            payload = _load_json(PRED_FILE, {})
        except Exception:
            payload = {}
    if not isinstance(payload, dict) or not payload:
        return {"ok": True, "date": "", "requested_date": "", "prediction_count": 0, "predictions": []}
    return _predictions_for_date(payload, str(payload.get("tomorrow") or ""))


@app.get("/model/stats")
def model_stats() -> dict[str, Any]:
    status_payload = _load_json(STATUS_FILE, {})
    return {
        "ok": bool(status_payload),
        "updated_at": status_payload.get("trained_at", ""),
        "current_model": {
            "best_model": status_payload.get("best_model", ""),
            "version": status_payload.get("model_version", ""),
            "best_score": status_payload.get("cv_roc_auc", 0),
            "rows": status_payload.get("trained_rows", 0),
            "sports_covered": status_payload.get("sports_covered", []),
            "candidate_count": 3,
        },
        "history": _load_json(HISTORY_FILE, []),
    }


@app.get("/kalshi/submissions")
def kalshi_submissions() -> dict[str, Any]:
    base = _local_submissions_payload()
    live = _live_kalshi_snapshot()
    if live.get("ok"):
        summary = base.get("summary") or {}
        summary["available_buying_power_usd"] = float((live.get("balance") or {}).get("balance_usd") or 0.0)
        base["summary"] = summary
    base["live"] = live
    return base


@app.get("/kalshi/positions")
def kalshi_positions() -> dict[str, Any]:
    live = _live_kalshi_snapshot()
    if not live.get("ok"):
        return {
            "ok": False,
            "updated_at": "",
            "summary": {"active_positions": 0, "open_notional_usd": 0, "estimated_pnl_usd": 0},
            "positions": [],
            "balance": {},
            "live": live,
        }
    return {
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
        "live": live,
    }


@app.get("/kalshi/live")
def kalshi_live() -> dict[str, Any]:
    return _live_kalshi_snapshot()


@app.post("/kalshi/place-from-predictions")
def kalshi_place_from_predictions(body: KalshiOrderRequest) -> dict[str, Any]:
    if not body.dry_run and not _env_flag("KALSHI_LIVE_TRADING_ENABLED", default=False):
        raise HTTPException(
            status_code=400,
            detail="Live trading is disabled. Set KALSHI_LIVE_TRADING_ENABLED=1 to allow order placement.",
        )
    payload = _load_json(PRED_FILE, {})
    if not isinstance(payload, dict) or not isinstance(payload.get("predictions"), list):
        raise HTTPException(status_code=400, detail="Prediction data unavailable.")
    try:
        return submit_prediction_orders(
            payload,
            stake_usd=body.stake_usd,
            max_orders=body.max_orders,
            dry_run=body.dry_run,
            include_combos=body.include_combos,
            max_combos=body.max_combo_orders,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/run/bootstrap")
def run_bootstrap(days_back: int = 365) -> dict[str, Any]:
    pipeline = HFDirectPipeline()
    if not pipeline.ok:
        raise HTTPException(status_code=400, detail="HF pipeline not configured.")
    return pipeline.bootstrap_one_year_history(days_back=days_back)


@app.post("/run/daily")
def run_daily() -> dict[str, Any]:
    try:
        return _run_hf_daily_pipeline()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/run/active")
def run_active() -> dict[str, Any]:
    try:
        return _run_hf_active_cycle()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
