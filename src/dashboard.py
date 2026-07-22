"""Kalshi Perps trading bot -- web dashboard + background scheduler.

Single purpose: watch all Kalshi perp instruments (BTC, ETH, SOL, XRP, DOGE,
LTC, BCH, LINK, SUI, NEAR, DOT, HBAR, HYPE, kSHIB, XLM, ZEC), collect their
multi-timeframe price history + news sentiment to a Hugging Face dataset,
train a direction classifier on that history, and run a scalping strategy
that opens small dry-run-by-default positions when the technical signal and
the model agree, taking a small profit and repeating.

Three background jobs, each cross-process locked (see `_locked_job`) so a
single `--workers 1` gunicorn process never runs a job twice concurrently:
  - perps_cycle       every PERPS_CYCLE_MINUTES    -- the actual trading loop
  - perps_data_collect every PERPS_DATA_COLLECT_MINUTES -- archive fresh
                                                            candles + news to HF
  - perps_train       daily at PERPS_TRAIN_HOUR_ET:00 ET  -- retrain the model
"""
from __future__ import annotations

import datetime as dt
import functools
import json
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, render_template, request

SRC_DIR = Path(__file__).resolve().parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import et_today
from data import perps_data, perps_model, perps_strategy
from data.kalshi_perps import get_margin_balance, get_margin_enabled, get_margin_exchange_status, get_margin_positions

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

PERPS_CYCLE_MINUTES = max(1, int(os.getenv("PERPS_CYCLE_MINUTES", "2") or "2"))
PERPS_DATA_COLLECT_MINUTES = max(5, int(os.getenv("PERPS_DATA_COLLECT_MINUTES", "15") or "15"))
PERPS_TRAIN_HOUR_ET = int(os.getenv("PERPS_TRAIN_HOUR_ET", "3") or "3")
# Dry-run is always the hard default regardless of this flag (see
# perps_strategy.LIVE_TRADING_ENABLED) -- this only controls whether the
# scheduler runs the loop AT ALL. Default ON: the whole point of this bot is
# to run continuously, and dry-run cycles place no real orders.
ENABLE_PERPS_SCHEDULER = str(os.getenv("ENABLE_PERPS_SCHEDULER", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
DASHBOARD_LOCAL_AUTORUN = str(os.getenv("DASHBOARD_LOCAL_AUTORUN", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
app = Flask(__name__, template_folder="templates")
scheduler = BackgroundScheduler(timezone="America/New_York")
_startup_lock = threading.Lock()
_startup_done = False

# ---------------------------------------------------------------------------
# Cross-process job lock + run history
# ---------------------------------------------------------------------------
JOB_LOCK_DIR = DATA_DIR / "locks"
JOB_HISTORY_FILE = DATA_DIR / "job_run_history.json"
JOB_HISTORY_MAX = 200
LATEST_CYCLE_FILE = DATA_DIR / "perps_latest_cycle.json"


def _load_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def _save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)


def _append_job_history(name: str, record: dict[str, Any]) -> None:
    try:
        history = _load_json(JOB_HISTORY_FILE, [])
        if not isinstance(history, list):
            history = []
        history.append({"job": name, **record})
        history = history[-JOB_HISTORY_MAX:]
        _save_json(JOB_HISTORY_FILE, history)
    except Exception as exc:
        logger.debug("job history append failed for %s: %s", name, exc)


def _summarize_job_result(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    keys = ("action", "ticker", "realized_pnl_usd", "rows_written", "hf_uploaded", "rows", "model_type")
    return {k: result[k] for k in keys if k in result}


def _locked_job(name: str, stale_after_sec: int = 600):
    """Only one process-wide caller of this job runs at a time. A second
    caller while the lock is held skips immediately rather than blocking or
    running in parallel -- important once this job can place real orders."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            JOB_LOCK_DIR.mkdir(parents=True, exist_ok=True)
            lock_path = JOB_LOCK_DIR / f"{name}.lock"
            acquired = False
            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"{os.getpid()}:{time.time()}".encode("utf-8"))
                os.close(fd)
                acquired = True
            except FileExistsError:
                try:
                    age = time.time() - lock_path.stat().st_mtime
                except Exception:
                    age = 0.0
                if age > stale_after_sec:
                    try:
                        lock_path.unlink()
                        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                        os.write(fd, f"{os.getpid()}:{time.time()}".encode("utf-8"))
                        os.close(fd)
                        acquired = True
                    except Exception:
                        acquired = False
                else:
                    acquired = False

            if not acquired:
                logger.warning("[lock] %s already running elsewhere, skipping this call", name)
                _append_job_history(name, {
                    "status": "skipped_concurrent",
                    "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                })
                return {"ok": True, "skipped": True, "reason": "already_running"}

            started = dt.datetime.now(dt.timezone.utc)
            try:
                result = fn(*args, **kwargs)
                finished = dt.datetime.now(dt.timezone.utc)
                _append_job_history(name, {
                    "status": "ok" if (not isinstance(result, dict) or result.get("ok", True)) else "failed",
                    "started_at": started.isoformat(),
                    "finished_at": finished.isoformat(),
                    "duration_sec": round((finished - started).total_seconds(), 1),
                    "summary": _summarize_job_result(result),
                })
                return result
            except Exception as exc:
                finished = dt.datetime.now(dt.timezone.utc)
                _append_job_history(name, {
                    "status": "error",
                    "started_at": started.isoformat(),
                    "finished_at": finished.isoformat(),
                    "duration_sec": round((finished - started).total_seconds(), 1),
                    "error": str(exc),
                })
                raise
            finally:
                try:
                    lock_path.unlink()
                except Exception:
                    pass
        return wrapper
    return decorator


def _is_cron_authorized() -> bool:
    secret = str(os.getenv("CRON_SECRET", "") or "").strip()
    if not secret:
        return True
    auth = str(request.headers.get("authorization") or "")
    return auth == f"Bearer {secret}"


# ---------------------------------------------------------------------------
# Background jobs
# ---------------------------------------------------------------------------
@_locked_job("perps_cycle", stale_after_sec=300)
def _run_perps_cycle() -> dict[str, Any]:
    result = perps_strategy.run_cycle()
    _save_json(LATEST_CYCLE_FILE, result)
    return result


@_locked_job("perps_data_collect", stale_after_sec=600)
def _run_perps_data_collect() -> dict[str, Any]:
    df = perps_data.collect_dataset_rows()
    if df.empty:
        return {"ok": False, "reason": "no_rows_collected"}
    return perps_data.push_dataset_snapshot(df)


@_locked_job("perps_train", stale_after_sec=1800)
def _run_perps_train() -> dict[str, Any]:
    return perps_model.train_model()


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
            scheduler.add_job(
                _run_perps_data_collect, "interval", minutes=PERPS_DATA_COLLECT_MINUTES,
                id="perps_data_collect", replace_existing=True,
            )
            scheduler.add_job(
                _run_perps_train, "cron", hour=PERPS_TRAIN_HOUR_ET, minute=0,
                id="perps_train", replace_existing=True,
            )
            if ENABLE_PERPS_SCHEDULER:
                scheduler.add_job(
                    _run_perps_cycle, "interval", minutes=PERPS_CYCLE_MINUTES,
                    id="perps_cycle", replace_existing=True,
                )
            scheduler.start()
            logger.info(
                "Perps scheduler started: cycle every %d min (%s), data collect every %d min, train daily at %02d:00 ET, live_trading=%s",
                PERPS_CYCLE_MINUTES, "ENABLED" if ENABLE_PERPS_SCHEDULER else "disabled",
                PERPS_DATA_COLLECT_MINUTES, PERPS_TRAIN_HOUR_ET, perps_strategy.LIVE_TRADING_ENABLED,
            )

        def _runner() -> None:
            try:
                _run_perps_data_collect()
                logger.info("Startup data collect completed")
            except Exception as exc:
                logger.warning("Startup data collect failed: %s", exc)
            try:
                train_result = _run_perps_train()
                logger.info("Startup train attempt: %s", train_result.get("reason", "ok"))
            except Exception as exc:
                logger.warning("Startup train failed: %s", exc)
            if ENABLE_PERPS_SCHEDULER:
                try:
                    _run_perps_cycle()
                    logger.info("Startup perps cycle completed")
                except Exception as exc:
                    logger.exception("Startup perps cycle failed: %s", exc)

        threading.Thread(target=_runner, daemon=True, name="dashboard-perps-startup-autorun").start()
        _startup_done = True


@app.before_request
def _bootstrap_background_jobs() -> None:
    _ensure_background_jobs_started()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/status")
def api_status():
    state = perps_strategy._load_state()  # noqa: SLF001
    _, meta = perps_model.load_model()
    latest_cycle = _load_json(LATEST_CYCLE_FILE, {})

    account_ok = True
    balance_usd = 0.0
    margin_enabled = None
    exchange_active = None
    try:
        margin_enabled = bool(get_margin_enabled().get("enabled"))
    except Exception as exc:
        account_ok = False
        margin_enabled = None
        logger.debug("margin_enabled check failed: %s", exc)
    try:
        exchange_active = bool(get_margin_exchange_status().get("exchange_active"))
    except Exception:
        exchange_active = None
    try:
        balance = get_margin_balance(compute_available_balance=True)
        for sub in (balance.get("subaccount_balances") or []):
            balance_usd = max(balance_usd, float(sub.get("available_balance") or 0.0))
    except Exception as exc:
        account_ok = False
        logger.debug("balance check failed: %s", exc)

    realized_pnl_by_date = state.get("realized_pnl_by_date") or {}
    total_realized_pnl = round(sum(float(v) for v in realized_pnl_by_date.values()), 6)

    return jsonify({
        "ok": True,
        "now": dt.datetime.now(dt.timezone.utc).isoformat(),
        "live_trading_enabled": perps_strategy.LIVE_TRADING_ENABLED,
        "account": {
            "ok": account_ok,
            "margin_enabled": margin_enabled,
            "exchange_active": exchange_active,
            "available_balance_usd": balance_usd,
        },
        "position": state.get("position"),
        "today_realized_pnl_usd": float(realized_pnl_by_date.get(et_today().isoformat(), 0.0)),
        "total_realized_pnl_usd": total_realized_pnl,
        "trade_count": len(state.get("trade_log") or []),
        "model": {
            "trained": meta is not None,
            "model_type": (meta or {}).get("model_type"),
            "trained_at": (meta or {}).get("trained_at"),
            "rows": (meta or {}).get("rows"),
            "scores": (meta or {}).get("scores"),
        },
        "latest_cycle": latest_cycle,
        "watchlist": perps_data.get_watchlist(),
        "params": {
            "trade_size_contracts": perps_strategy.TRADE_SIZE_CONTRACTS,
            "take_profit_pct": perps_strategy.TAKE_PROFIT_PCT,
            "stop_loss_pct": perps_strategy.STOP_LOSS_PCT,
            "max_hold_minutes": perps_strategy.MAX_HOLD_MINUTES,
            "daily_loss_cap_usd": perps_strategy.DAILY_LOSS_CAP_USD,
            "model_confidence_min": perps_strategy.MODEL_CONFIDENCE_MIN,
        },
    })


@app.route("/api/trades")
def api_trades():
    state = perps_strategy._load_state()  # noqa: SLF001
    trade_log = list(reversed(state.get("trade_log") or []))
    return jsonify({
        "ok": True,
        "trade_count": len(trade_log),
        "realized_pnl_by_date": state.get("realized_pnl_by_date") or {},
        "trades": trade_log[:200],
    })


@app.route("/api/positions")
def api_positions():
    try:
        positions = get_margin_positions()
        return jsonify({"ok": True, "positions": positions.get("positions") or []})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "positions": []})


@app.route("/api/perps/tick", methods=["GET", "POST"])
def api_perps_tick():
    """Manually force an immediate strategy cycle instead of waiting for the
    next scheduled interval."""
    if not _is_cron_authorized():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        result = _run_perps_cycle()
        return jsonify(result)
    except Exception as exc:
        logger.exception("[dashboard] manual perps tick failed: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/perps/collect", methods=["GET", "POST"])
def api_perps_collect():
    if not _is_cron_authorized():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_perps_data_collect())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/perps/train", methods=["GET", "POST"])
def api_perps_train():
    if not _is_cron_authorized():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_perps_train())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


_JOB_LABELS = {
    "perps_cycle": f"Trading cycle (every {PERPS_CYCLE_MINUTES} min)",
    "perps_data_collect": f"Data collection -> HF (every {PERPS_DATA_COLLECT_MINUTES} min)",
    "perps_train": f"Model retrain (daily {PERPS_TRAIN_HOUR_ET:02d}:00 ET)",
}


@app.route("/api/server/activity")
def server_activity():
    history = _load_json(JOB_HISTORY_FILE, [])
    if not isinstance(history, list):
        history = []
    recent = list(reversed(history[-60:]))

    running_now: list[dict[str, Any]] = []
    try:
        for lock_file in sorted(JOB_LOCK_DIR.glob("*.lock")):
            job_name = lock_file.stem
            try:
                raw = lock_file.read_text(encoding="utf-8")
                _, _, ts_str = raw.partition(":")
                started_ts = float(ts_str) if ts_str else 0.0
            except Exception:
                started_ts = 0.0
            running_now.append({
                "job": job_name,
                "label": _JOB_LABELS.get(job_name, job_name),
                "running_for_sec": round(time.time() - started_ts, 1) if started_ts else None,
            })
    except FileNotFoundError:
        pass

    last_by_job: dict[str, dict[str, Any]] = {}
    for rec in recent:
        job = rec.get("job")
        if job and job not in last_by_job:
            last_by_job[job] = rec
    for rec in recent:
        rec["label"] = _JOB_LABELS.get(rec.get("job"), rec.get("job"))

    return jsonify({
        "ok": True,
        "now": dt.datetime.now(dt.timezone.utc).isoformat(),
        "scheduler_enabled": ENABLE_PERPS_SCHEDULER,
        "running_now": running_now,
        "last_by_job": last_by_job,
        "recent": recent,
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000") or "5000")
    _ensure_background_jobs_started()
    app.run(host="0.0.0.0", port=port, debug=False)
