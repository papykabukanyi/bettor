"""Alpaca OPTIONS trading bot -- its OWN web dashboard + background
scheduler, running as its own named server/process (alpaca_options_server),
in its own Render service, separate from alpaca_server.py (equities),
alpaca_crypto_server.py (crypto), and app_kalshi.py (Kalshi perps) --
following the same "split into separate services" decision already made
for crypto: a crash, OOM, or redeploy on one asset class can no longer
take any of the others down with it.

Background jobs, each cross-process locked (see server_common.py):
  - alpaca_options_fast_check   every ALPACA_OPTIONS_FAST_CHECK_SECONDS --
                                 manages existing option positions (take-
                                 profit/stop-loss/max-hold/near-expiration)
  - alpaca_options_entry_scan   every ALPACA_OPTIONS_CYCLE_MINUTES -- scans
                                 the fixed options-friendly underlying
                                 universe for a new directional entry
  - alpaca_options_data_collect every ALPACA_OPTIONS_DATA_COLLECT_MINUTES --
                                 archives fresh underlying minute bars to HF
  - alpaca_options_train        daily at ALPACA_OPTIONS_TRAIN_HOUR_ET:00 ET

No explicit market-hours gate on the entry scan -- same precedent already
established by alpaca_server.py's own equities strategy (scan_and_enter has
no session check there either); outside regular hours there's simply
nothing fresh to trade on and any order attempt is queued/rejected by
Alpaca itself, exactly the behavior already accepted for equities.
"""
from __future__ import annotations

import atexit
import datetime as dt
import gc
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
from data import alpaca_client, alpaca_options_data, alpaca_options_model, alpaca_options_strategy, stock_news, threads_post
from server_common import DATA_DIR, is_cron_authorized, load_json, make_job_lock, save_json

# Same real, twice-confirmed production bug already fixed on every other
# server here -- see their own copies of this comment for the full story
# (bare `import huggingface_hub` is NOT enough; PEP 562 lazy __getattr__
# loading means hf_hub_download/HfApi need to be named explicitly to
# actually force their submodules to resolve eagerly).
from huggingface_hub import HfApi, hf_hub_download  # noqa: F401

ALPACA_OPTIONS_CYCLE_MINUTES = max(1, int(os.getenv("ALPACA_OPTIONS_CYCLE_MINUTES", "5") or "5"))
ALPACA_OPTIONS_FAST_CHECK_SECONDS = max(5, int(os.getenv("ALPACA_OPTIONS_FAST_CHECK_SECONDS", "30") or "30"))
ALPACA_OPTIONS_DATA_COLLECT_MINUTES = max(5, int(os.getenv("ALPACA_OPTIONS_DATA_COLLECT_MINUTES", "15") or "15"))
ALPACA_OPTIONS_TRAIN_HOUR_ET = int(os.getenv("ALPACA_OPTIONS_TRAIN_HOUR_ET", "5") or "5")
ALPACA_OPTIONS_STARTUP_GRACE_SECONDS = max(0, int(os.getenv("ALPACA_OPTIONS_STARTUP_GRACE_SECONDS", "60") or "60"))
ENABLE_ALPACA_OPTIONS_SCHEDULER = str(os.getenv("ENABLE_ALPACA_OPTIONS_SCHEDULER", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
DASHBOARD_LOCAL_AUTORUN = str(os.getenv("DASHBOARD_LOCAL_AUTORUN", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
# Cross-links to the other, separately-deployed services -- unknown at
# build time, filled in via env vars once each service's own hostname
# exists. Fall back to "#" (dead link, not a guess) if unset.
PERPS_SERVER_URL = os.getenv("PERPS_SERVER_URL", "#")
ALPACA_STOCKS_SERVER_URL = os.getenv("ALPACA_STOCKS_SERVER_URL", "#")
ALPACA_CRYPTO_SERVER_URL = os.getenv("ALPACA_CRYPTO_SERVER_URL", "#")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
app = Flask("alpaca_options_server", template_folder="templates")
scheduler = BackgroundScheduler(timezone="America/New_York")
_startup_lock = threading.Lock()
_startup_done = False

_locked_job = make_job_lock(DATA_DIR / "alpaca_options_job_run_history.json", DATA_DIR / "alpaca_options_locks")

JOB_HISTORY_FILE = DATA_DIR / "alpaca_options_job_run_history.json"
JOB_LOCK_DIR = DATA_DIR / "alpaca_options_locks"
ALPACA_OPTIONS_LATEST_CYCLE_FILE = DATA_DIR / "alpaca_options_latest_cycle.json"
ALPACA_OPTIONS_LATEST_POSITION_CHECK_FILE = DATA_DIR / "alpaca_options_latest_position_check.json"


# Same reasoning as every other server here: on SIGTERM, APScheduler's
# background thread can still be mid-cycle when the interpreter starts
# tearing down. atexit runs before that race can occur.
@atexit.register
def _shutdown_scheduler() -> None:
    try:
        if scheduler.running:
            scheduler.shutdown(wait=False)
    except Exception:
        logger.exception("[alpaca_options_server] error shutting down scheduler at exit")


# ---------------------------------------------------------------------------
# Background jobs
# ---------------------------------------------------------------------------
@_locked_job("alpaca_options_fast_check", stale_after_sec=60)
def _run_alpaca_options_fast_check() -> dict[str, Any]:
    result = alpaca_options_strategy.manage_open_positions()
    if result.get("action") != "no_position":
        save_json(ALPACA_OPTIONS_LATEST_POSITION_CHECK_FILE, result)
    return result


@_locked_job("alpaca_options_entry_scan", stale_after_sec=300)
def _run_alpaca_options_entry_scan() -> dict[str, Any]:
    result = alpaca_options_strategy.scan_and_enter()
    save_json(ALPACA_OPTIONS_LATEST_CYCLE_FILE, result)
    return result


@_locked_job("alpaca_options_data_collect", stale_after_sec=600)
def _run_alpaca_options_data_collect() -> dict[str, Any]:
    """gc.collect() in `finally` -- same real OOM-mitigation discipline
    proven necessary on every other data-collect job here, applied
    proactively from the start rather than reactively after an incident."""
    try:
        df = alpaca_options_data.collect_dataset_rows()
        if df.empty:
            return {"ok": False, "reason": "no_rows_collected"}
        return alpaca_options_data.push_minute_snapshot(df)
    except Exception as exc:
        logger.warning("[alpaca_options_server] data collect failed: %s", exc)
        return {"ok": False, "error": str(exc)}
    finally:
        gc.collect()


@_locked_job("alpaca_options_train", stale_after_sec=1800)
def _run_alpaca_options_train() -> dict[str, Any]:
    return alpaca_options_model.train_model()


@_locked_job("alpaca_options_threads_trending_news", stale_after_sec=300)
def _run_alpaca_options_threads_trending_news() -> dict[str, Any]:
    """Posts a digest of what's currently trending in stock-market news
    every 30 minutes -- reuses stock_news.py directly (the options
    underlyings are ordinary equities, so the same general-market
    headlines already feeding sentiment_score apply here too)."""
    try:
        headlines = stock_news.get_trending_headlines(limit=5)
        posted = threads_post.post_trending_news(headlines, market="stocks")
        return {"ok": True, "posted": posted, "headline_count": len(headlines)}
    except Exception as exc:
        logger.warning("[alpaca_options_server] Threads trending-news post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def _ensure_background_jobs_started() -> None:
    global _startup_done
    if _startup_done:
        return
    if not DASHBOARD_LOCAL_AUTORUN:
        return
    with _startup_lock:
        if _startup_done:
            return
        if not scheduler.running and ENABLE_ALPACA_OPTIONS_SCHEDULER:
            # next_run_time delayed a full interval -- see the identical
            # fix and its comment in app_kalshi.py's own perps_data_collect
            # registration: without this, APScheduler fires an interval
            # job's FIRST run immediately on scheduler.start(), duplicating
            # the _runner() thread's own direct startup call below.
            scheduler.add_job(
                _run_alpaca_options_data_collect, "interval", minutes=ALPACA_OPTIONS_DATA_COLLECT_MINUTES,
                id="alpaca_options_data_collect", replace_existing=True,
                next_run_time=dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=ALPACA_OPTIONS_DATA_COLLECT_MINUTES),
            )
            scheduler.add_job(
                _run_alpaca_options_train, "cron", hour=ALPACA_OPTIONS_TRAIN_HOUR_ET, minute=0,
                id="alpaca_options_train", replace_existing=True,
            )
            scheduler.add_job(
                _run_alpaca_options_fast_check, "interval", seconds=ALPACA_OPTIONS_FAST_CHECK_SECONDS,
                id="alpaca_options_fast_check", replace_existing=True,
            )
            scheduler.add_job(
                _run_alpaca_options_entry_scan, "interval", minutes=ALPACA_OPTIONS_CYCLE_MINUTES,
                id="alpaca_options_entry_scan", replace_existing=True,
                next_run_time=dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=ALPACA_OPTIONS_STARTUP_GRACE_SECONDS),
            )
            scheduler.add_job(
                _run_alpaca_options_threads_trending_news, "interval", minutes=30,
                id="alpaca_options_threads_trending_news", replace_existing=True,
            )
            scheduler.start()
            logger.info(
                "Alpaca options scheduler started: fast exit check every %ds, entry scan every %d min "
                "(first run in %ds), data collect every %d min, train daily at %02d:00 ET, "
                "mode=%s live_trading=%s",
                ALPACA_OPTIONS_FAST_CHECK_SECONDS, ALPACA_OPTIONS_CYCLE_MINUTES, ALPACA_OPTIONS_STARTUP_GRACE_SECONDS,
                ALPACA_OPTIONS_DATA_COLLECT_MINUTES, ALPACA_OPTIONS_TRAIN_HOUR_ET,
                alpaca_options_strategy.MODE, alpaca_options_strategy.LIVE_TRADING_ENABLED,
            )

        def _runner() -> None:
            # No immediate, unconditional startup data-collect call here --
            # a real, confirmed incident on this exact sibling pattern in
            # alpaca_crypto_server.py showed that calling it unconditionally
            # on every boot can OOM-crash the process, and because it ran on
            # EVERY restart, the crash-triggered restart just re-ran the same
            # expensive step immediately, becoming a self-sustaining crash
            # loop. Fixed there by relying on the already-scheduled
            # alpaca_options_data_collect job (registered above with its own
            # next_run_time delay) instead of an immediate direct call.
            #
            # Same cold-start-only guard as every other server here: only
            # train immediately if nothing is cached yet, so a crash-
            # triggered restart can't turn into a self-sustaining retrain
            # loop.
            try:
                if alpaca_options_model.load_model()[0] is None:
                    train_result = _run_alpaca_options_train()
                    logger.info("Startup alpaca options train attempt (cold start): %s", train_result.get("reason", "ok"))
                else:
                    logger.info("Startup alpaca options train skipped: model already cached, daily cron will retrain")
            except Exception as exc:
                logger.warning("Startup alpaca options train failed: %s", exc)
            # No immediate startup entry scan here -- same rolling-deploy
            # collision reasoning as every other server: the scheduled
            # entry-scan job's own delayed first tick already covers this.

        threading.Thread(target=_runner, daemon=True, name="alpaca-options-server-startup-autorun").start()
        _startup_done = True


@app.before_request
def _bootstrap_background_jobs() -> None:
    _ensure_background_jobs_started()


# Same real bug found and fixed on every other server here: under gunicorn
# this file's ACTUAL production entrypoint never runs the
# `if __name__ == "__main__"` block below, so without this call the
# scheduler was only reachable via the before_request hook above -- meaning
# it wouldn't start until the first real HTTP request arrived, not on
# process boot.
_ensure_background_jobs_started()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route("/")
@app.route("/alpaca-options")
def alpaca_options_dashboard():
    return render_template(
        "alpaca_options_dashboard.html", perps_url=PERPS_SERVER_URL,
        stocks_url=ALPACA_STOCKS_SERVER_URL, crypto_url=ALPACA_CRYPTO_SERVER_URL,
    )


@app.route("/api/alpaca/options/status")
def api_alpaca_options_status():
    state = alpaca_options_strategy._load_state()  # noqa: SLF001
    _, meta = alpaca_options_model.load_model()
    latest_cycle = load_json(ALPACA_OPTIONS_LATEST_CYCLE_FILE, {})
    latest_position_check = load_json(ALPACA_OPTIONS_LATEST_POSITION_CHECK_FILE, {})

    realized_pnl_by_date = state.get("realized_pnl_by_date") or {}
    total_realized_pnl = round(sum(float(v) for v in realized_pnl_by_date.values()), 6)
    positions = [
        {**p, **alpaca_options_strategy.position_exit_levels(p)}
        for p in (state.get("positions") or [])
    ]

    return jsonify({
        "ok": True,
        "now": dt.datetime.now(dt.timezone.utc).isoformat(),
        "mode": alpaca_options_strategy.MODE,
        "live_trading_enabled": alpaca_options_strategy.LIVE_TRADING_ENABLED,
        "alpaca_configured": alpaca_client.is_configured(),
        "balance": state.get("balance", alpaca_options_strategy.SIMULATE_STARTING_BALANCE),
        "available_balance": alpaca_options_strategy.get_available_balance() if alpaca_options_strategy.MODE == "simulate" else None,
        "positions": positions,
        "open_position_count": len(positions),
        "max_concurrent_positions": alpaca_options_strategy.MAX_CONCURRENT_POSITIONS,
        "today_realized_pnl_usd": float(realized_pnl_by_date.get(et_today().isoformat(), 0.0)),
        "total_realized_pnl_usd": total_realized_pnl,
        "trade_count": len(state.get("trade_log") or []),
        "underlyings": alpaca_options_data.get_options_universe(),
        "model": {
            "trained": meta is not None,
            "model_type": (meta or {}).get("model_type"),
            "trained_at": (meta or {}).get("trained_at"),
            "rows": (meta or {}).get("rows"),
            "scores": (meta or {}).get("scores"),
            "feature_importances": (meta or {}).get("feature_importances"),
        },
        "latest_cycle": latest_cycle,
        "latest_position_check": latest_position_check,
        "params": {
            "position_size_pct": alpaca_options_strategy.POSITION_SIZE_PCT,
            "max_concurrent_positions": alpaca_options_strategy.MAX_CONCURRENT_POSITIONS,
            "take_profit_pct": alpaca_options_strategy.TAKE_PROFIT_PCT,
            "stop_loss_pct": alpaca_options_strategy.STOP_LOSS_PCT,
            "max_hold_minutes": alpaca_options_strategy.MAX_HOLD_MINUTES,
            "daily_loss_cap_pct": alpaca_options_strategy.DAILY_LOSS_CAP_PCT,
            "model_confidence_min": alpaca_options_strategy.MODEL_CONFIDENCE_MIN,
            "min_days_to_expiration": alpaca_options_data.MIN_DAYS_TO_EXPIRATION,
            "max_days_to_expiration": alpaca_options_data.MAX_DAYS_TO_EXPIRATION,
            "fast_check_seconds": ALPACA_OPTIONS_FAST_CHECK_SECONDS,
            "entry_scan_minutes": ALPACA_OPTIONS_CYCLE_MINUTES,
            "data_collect_minutes": ALPACA_OPTIONS_DATA_COLLECT_MINUTES,
            "train_hour_et": ALPACA_OPTIONS_TRAIN_HOUR_ET,
        },
    })


@app.route("/api/alpaca/options/trades")
def api_alpaca_options_trades():
    state = alpaca_options_strategy._load_state()  # noqa: SLF001
    trade_log = list(reversed(state.get("trade_log") or []))
    return jsonify({
        "ok": True,
        "trade_count": len(trade_log),
        "realized_pnl_by_date": state.get("realized_pnl_by_date") or {},
        "trades": trade_log[:200],
    })


@app.route("/api/alpaca/options/tick", methods=["GET", "POST"])
def api_alpaca_options_tick():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        fast = _run_alpaca_options_fast_check()
        scan = _run_alpaca_options_entry_scan()
        return jsonify({"ok": True, "fast_check": fast, "entry_scan": scan})
    except Exception as exc:
        logger.exception("[alpaca_options_server] manual tick failed: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/alpaca/options/collect", methods=["GET", "POST"])
def api_alpaca_options_collect():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_alpaca_options_data_collect())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/alpaca/options/train", methods=["GET", "POST"])
def api_alpaca_options_train():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_alpaca_options_train())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


_JOB_LABELS = {
    "alpaca_options_data_collect": f"Alpaca options data collection -> HF (every {ALPACA_OPTIONS_DATA_COLLECT_MINUTES} min)",
    "alpaca_options_train": f"Alpaca options model retrain (daily {ALPACA_OPTIONS_TRAIN_HOUR_ET:02d}:00 ET)",
    "alpaca_options_fast_check": f"Alpaca options fast exit check (every {ALPACA_OPTIONS_FAST_CHECK_SECONDS}s)",
    "alpaca_options_entry_scan": f"Alpaca options entry scan (every {ALPACA_OPTIONS_CYCLE_MINUTES} min)",
    "alpaca_options_threads_trending_news": "Threads trending-news post (every 30 min)",
}


@app.route("/api/server/activity")
def server_activity():
    history = load_json(JOB_HISTORY_FILE, [])
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
        "scheduler_enabled": ENABLE_ALPACA_OPTIONS_SCHEDULER,
        "running_now": running_now,
        "last_by_job": last_by_job,
        "recent": recent,
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5003") or "5003")
    _ensure_background_jobs_started()
    app.run(host="0.0.0.0", port=port, debug=False)
