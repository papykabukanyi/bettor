"""Charles Schwab stock trading bot -- its OWN web dashboard + background
scheduler, running as its own named server/process (schwab_stocks_server),
completely separate from perps_server.py. Nothing in this file imports
perps_* or knows the Kalshi perps bot exists.

Background jobs, each cross-process locked (see server_common.py):
  - schwab_fast_check          every SCHWAB_FAST_CHECK_SECONDS -- manages
                                                                    an existing
                                                                    position
  - schwab_entry_scan          every SCHWAB_CYCLE_MINUTES       -- watchlist
                                                                    scan for a
                                                                    new entry
  - schwab_data_collect        every SCHWAB_DATA_COLLECT_MINUTES -- archives
                                                                     fresh
                                                                     watchlist
                                                                     minute
                                                                     bars to HF
  - schwab_train               daily at SCHWAB_TRAIN_HOUR_ET:00 ET
  - schwab_intensive_training  every SCHWAB_INTENSIVE_TRAINING_MINUTES, but
                                only actually DOES anything while the market
                                is fully closed (nights, weekends): retrains,
                                runs a small backtest sweep, AND advances the
                                full-universe 20-year daily-bar historical
                                backfill by one batch (resumable -- see
                                schwab_data.get_symbols_with_daily_bars).
"""
from __future__ import annotations

import atexit
import datetime as dt
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, redirect, render_template, request

SRC_DIR = Path(__file__).resolve().parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import et_today
from data import schwab_backtest, schwab_client, schwab_data, schwab_model, schwab_strategy
from server_common import DATA_DIR, is_cron_authorized, load_json, make_job_lock, save_json

SCHWAB_CYCLE_MINUTES = max(1, int(os.getenv("SCHWAB_CYCLE_MINUTES", "2") or "2"))
SCHWAB_FAST_CHECK_SECONDS = max(5, int(os.getenv("SCHWAB_FAST_CHECK_SECONDS", "20") or "20"))
SCHWAB_DATA_COLLECT_MINUTES = max(5, int(os.getenv("SCHWAB_DATA_COLLECT_MINUTES", "15") or "15"))
SCHWAB_TRAIN_HOUR_ET = int(os.getenv("SCHWAB_TRAIN_HOUR_ET", "4") or "4")
# Checked every 30 min so it picks up the fully-closed window promptly
# (nights + weekends) -- a no-op the rest of the time.
SCHWAB_INTENSIVE_TRAINING_MINUTES = max(10, int(os.getenv("SCHWAB_INTENSIVE_TRAINING_MINUTES", "30") or "30"))
# How many symbols' worth of 20-year daily bars to push per off-hours tick --
# a full ~12,000-symbol US equity universe at Schwab's 120 req/min limit
# takes hours, so this is a resumable BATCH, not a one-shot attempt at the
# whole universe. See _advance_historical_backfill.
SCHWAB_BACKFILL_BATCH_SIZE = max(1, int(os.getenv("SCHWAB_BACKFILL_BATCH_SIZE", "50") or "50"))
ENABLE_SCHWAB_SCHEDULER = str(os.getenv("ENABLE_SCHWAB_SCHEDULER", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
DASHBOARD_LOCAL_AUTORUN = str(os.getenv("DASHBOARD_LOCAL_AUTORUN", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
# Cross-link to the separately-deployed perps server -- unknown at build
# time (a different Render service gets its own generated hostname), so
# this is filled in via an env var once that service exists rather than
# hardcoded. Falls back to "#" (dead link, not a guess) if unset.
PERPS_SERVER_URL = os.getenv("PERPS_SERVER_URL", "#")
# Same rolling-deploy collision guard as the perps server's own entry-scan
# grace period -- see that file's comment for the full story.
SCHWAB_STARTUP_GRACE_SECONDS = max(0, int(os.getenv("SCHWAB_STARTUP_GRACE_SECONDS", "45") or "45"))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
app = Flask("schwab_stocks_server", template_folder="templates")
scheduler = BackgroundScheduler(timezone="America/New_York")
_startup_lock = threading.Lock()
_startup_done = False

_locked_job = make_job_lock(DATA_DIR / "schwab_job_run_history.json", DATA_DIR / "schwab_locks")

JOB_HISTORY_FILE = DATA_DIR / "schwab_job_run_history.json"
JOB_LOCK_DIR = DATA_DIR / "schwab_locks"
SCHWAB_LATEST_CYCLE_FILE = DATA_DIR / "schwab_latest_cycle.json"
SCHWAB_LATEST_POSITION_CHECK_FILE = DATA_DIR / "schwab_latest_position_check.json"
SCHWAB_LATEST_SWEEP_FILE = DATA_DIR / "schwab_latest_sweep.json"
SCHWAB_LATEST_BACKFILL_FILE = DATA_DIR / "schwab_latest_backfill.json"


# Same reasoning as perps_server.py's own copy of this: on SIGTERM,
# APScheduler's background thread can still be mid-cycle when the
# interpreter starts tearing down, raising "cannot schedule new futures
# after interpreter shutdown". atexit runs before that race can occur.
@atexit.register
def _shutdown_scheduler() -> None:
    try:
        if scheduler.running:
            scheduler.shutdown(wait=False)
    except Exception:
        logger.exception("[schwab_server] error shutting down scheduler at exit")


# Same reasoning as perps_server.py's account-snapshot cache: the dashboard
# polls /api/schwab/status every 10s, and get_market_session() can make a
# real Schwab API call. Market session doesn't change within any given
# minute, so a short cache keeps that poll cheap regardless of refresh rate
# (confirmed live elsewhere this session: an uncached expensive call inline
# in a polled status endpoint caused a real Render OOM for the perps bot).
_MARKET_SESSION_CACHE: dict[str, Any] = {}
_MARKET_SESSION_CACHE_TS = 0.0
_MARKET_SESSION_CACHE_LOCK = threading.Lock()
_MARKET_SESSION_CACHE_TTL_SEC = 60


def _cached_market_session() -> dict[str, Any]:
    global _MARKET_SESSION_CACHE, _MARKET_SESSION_CACHE_TS
    now = time.monotonic()
    with _MARKET_SESSION_CACHE_LOCK:
        if _MARKET_SESSION_CACHE and (now - _MARKET_SESSION_CACHE_TS) < _MARKET_SESSION_CACHE_TTL_SEC:
            return dict(_MARKET_SESSION_CACHE)
    session = schwab_data.get_market_session()
    with _MARKET_SESSION_CACHE_LOCK:
        _MARKET_SESSION_CACHE = dict(session)
        _MARKET_SESSION_CACHE_TS = time.monotonic()
    return session


# ---------------------------------------------------------------------------
# Background jobs
# ---------------------------------------------------------------------------
@_locked_job("schwab_fast_check", stale_after_sec=60)
def _run_schwab_fast_check() -> dict[str, Any]:
    result = schwab_strategy.manage_open_positions()
    if result.get("action") != "no_position":
        save_json(SCHWAB_LATEST_POSITION_CHECK_FILE, result)
    return result


@_locked_job("schwab_entry_scan", stale_after_sec=300)
def _run_schwab_entry_scan() -> dict[str, Any]:
    result = schwab_strategy.scan_and_enter()
    save_json(SCHWAB_LATEST_CYCLE_FILE, result)
    return result


@_locked_job("schwab_data_collect", stale_after_sec=600)
def _run_schwab_data_collect() -> dict[str, Any]:
    """Collects the live watchlist (top symbols by real recent volume +
    volatility -- see schwab_data.get_stock_watchlist) every
    SCHWAB_DATA_COLLECT_MINUTES -- the continuously-"streaming" minute-bar
    archive prediction/training/backtesting reads from. push_minute_snapshot
    merges into today's existing HF shard rather than overwriting it, so a
    symbol that drops off the watchlist between cycles doesn't lose its
    earlier rows for today."""
    try:
        recent = schwab_data.load_training_dataset(max_rows=20_000)
        watchlist = schwab_data.get_stock_watchlist(recent if not recent.empty else None)
        df = schwab_data.collect_dataset_rows(watchlist)
        if df.empty:
            return {"ok": False, "reason": "no_rows_collected"}
        return schwab_data.push_minute_snapshot(df)
    except Exception as exc:
        logger.warning("[schwab_server] data collect failed: %s", exc)
        return {"ok": False, "error": str(exc)}


@_locked_job("schwab_train", stale_after_sec=1800)
def _run_schwab_train() -> dict[str, Any]:
    return schwab_model.train_model()


def _advance_historical_backfill() -> dict[str, Any]:
    """Sends up to SCHWAB_BACKFILL_BATCH_SIZE more symbols' worth of the
    MAXIMUM available daily-bar history (up to 20 years -- a single Schwab
    call per symbol, period=20 is a valid periodType=year value) to HF.
    Resumable across restarts/off-hours windows via
    schwab_data.get_symbols_with_daily_bars() -- a full ~12,000-symbol US
    equity universe at Schwab's 120 req/min limit takes hours, so this
    picks up a bit more each off-hours tick rather than needing to finish
    in one run."""
    try:
        universe = schwab_data.get_us_stock_universe()
    except Exception as exc:
        return {"ok": False, "error": f"universe fetch failed: {exc}"}
    try:
        already_done = schwab_data.get_symbols_with_daily_bars()
    except Exception as exc:
        logger.warning("[schwab_server] could not list existing daily shards, starting fresh: %s", exc)
        already_done = set()

    remaining = [s for s in universe if s not in already_done]
    result: dict[str, Any] = {
        "universe_size": len(universe), "already_done": len(already_done), "remaining_before": len(remaining),
    }
    if not remaining:
        result["action"] = "backfill_complete"
        save_json(SCHWAB_LATEST_BACKFILL_FILE, result)
        return result

    batch = remaining[:SCHWAB_BACKFILL_BATCH_SIZE]
    pushed, failed = 0, 0
    for symbol in batch:
        try:
            df = schwab_data.fetch_daily_bars(symbol, years=20)
            if not df.empty and schwab_data.push_daily_snapshot(symbol, df).get("ok"):
                pushed += 1
            else:
                failed += 1
        except Exception as exc:
            failed += 1
            logger.debug("[schwab_server] daily backfill failed for %s: %s", symbol, exc)
        time.sleep(0.55)  # stay comfortably under Schwab's 120 req/min limit

    result.update({"action": "backfill_batch", "pushed": pushed, "failed": failed, "batch_size": len(batch)})
    save_json(SCHWAB_LATEST_BACKFILL_FILE, result)
    return result


@_locked_job("schwab_intensive_training", stale_after_sec=3600)
def _run_schwab_intensive_training() -> dict[str, Any]:
    """Off-hours-only: while the market is fully closed (nights, weekends),
    use the idle time to (1) retrain, (2) run a small backtest sweep over
    recent real data, and (3) advance the full-universe historical
    backfill by one batch -- so a well-tuned strategy AND the maximum
    available historical dataset are both progressing without ever
    competing with live trading checks. A no-op (not an error) whenever
    the market isn't fully closed."""
    session = schwab_data.get_market_session()
    if session["session"] != "closed":
        return {"ok": True, "skipped": True, "reason": "market_not_closed", "session": session["session"]}

    train_result = schwab_model.train_model()

    sweep_result = None
    try:
        df = schwab_data.load_training_dataset()
        if not df.empty:
            cutoff_ts = df["ts"].quantile(0.7)
            train_df = df[df["ts"] < cutoff_ts]
            test_df = df[df["ts"] >= cutoff_ts]
            fitted = schwab_backtest.fit_backtest_model(train_df)
            test_with_preds = schwab_backtest.add_model_predictions(test_df, fitted)
            sweep_result = schwab_backtest.run_config_sweep(
                test_with_preds, starting_balance=schwab_strategy.SIMULATE_STARTING_BALANCE,
            )
            save_json(SCHWAB_LATEST_SWEEP_FILE, sweep_result)
    except Exception as exc:
        logger.warning("[schwab_server] intensive backtest sweep failed: %s", exc)

    backfill_result = None
    try:
        backfill_result = _advance_historical_backfill()
    except Exception as exc:
        logger.warning("[schwab_server] historical backfill batch failed: %s", exc)

    return {"ok": True, "train_result": train_result, "sweep_result": sweep_result, "backfill_result": backfill_result}


def _ensure_background_jobs_started() -> None:
    global _startup_done
    if _startup_done:
        return
    if not DASHBOARD_LOCAL_AUTORUN:
        return
    with _startup_lock:
        if _startup_done:
            return
        if not scheduler.running and ENABLE_SCHWAB_SCHEDULER:
            scheduler.add_job(
                _run_schwab_data_collect, "interval", minutes=SCHWAB_DATA_COLLECT_MINUTES,
                id="schwab_data_collect", replace_existing=True,
            )
            scheduler.add_job(
                _run_schwab_train, "cron", hour=SCHWAB_TRAIN_HOUR_ET, minute=0,
                id="schwab_train", replace_existing=True,
            )
            scheduler.add_job(
                _run_schwab_intensive_training, "interval", minutes=SCHWAB_INTENSIVE_TRAINING_MINUTES,
                id="schwab_intensive_training", replace_existing=True,
            )
            scheduler.add_job(
                _run_schwab_fast_check, "interval", seconds=SCHWAB_FAST_CHECK_SECONDS,
                id="schwab_fast_check", replace_existing=True,
            )
            scheduler.add_job(
                _run_schwab_entry_scan, "interval", minutes=SCHWAB_CYCLE_MINUTES,
                id="schwab_entry_scan", replace_existing=True,
                next_run_time=dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=SCHWAB_STARTUP_GRACE_SECONDS),
            )
            scheduler.start()
            logger.info(
                "Schwab scheduler started: fast exit check every %ds, entry scan every %d min (first run in %ds), "
                "data collect every %d min, train daily at %02d:00 ET, intensive training checked every %d min, "
                "mode=%s live_trading=%s",
                SCHWAB_FAST_CHECK_SECONDS, SCHWAB_CYCLE_MINUTES, SCHWAB_STARTUP_GRACE_SECONDS,
                SCHWAB_DATA_COLLECT_MINUTES, SCHWAB_TRAIN_HOUR_ET, SCHWAB_INTENSIVE_TRAINING_MINUTES,
                schwab_strategy.MODE, schwab_strategy.LIVE_TRADING_ENABLED,
            )

        def _runner() -> None:
            try:
                _run_schwab_data_collect()
                logger.info("Startup schwab data collect completed")
            except Exception as exc:
                logger.warning("Startup schwab data collect failed: %s", exc)
            # Same cold-start-only guard as perps_server.py's own copy of
            # this: only train immediately if nothing is cached yet, so a
            # crash-triggered restart can't turn into a self-sustaining
            # retrain loop. This is a BRAND NEW service with no scheduled
            # history yet, so without this a fresh deploy would otherwise
            # wait up to 24h (SCHWAB_TRAIN_HOUR_ET) for its first model.
            try:
                if schwab_model.load_model()[0] is None:
                    train_result = _run_schwab_train()
                    logger.info("Startup schwab train attempt (cold start): %s", train_result.get("reason", "ok"))
                else:
                    logger.info("Startup schwab train skipped: model already cached, daily cron will retrain")
            except Exception as exc:
                logger.warning("Startup schwab train failed: %s", exc)
            # No immediate startup entry scan here -- same rolling-deploy
            # collision reasoning as perps_server.py (see
            # PERPS_STARTUP_GRACE_SECONDS's comment there): the scheduled
            # schwab_entry_scan job's own delayed first tick already covers
            # this safely.

        threading.Thread(target=_runner, daemon=True, name="schwab-server-startup-autorun").start()
        _startup_done = True


@app.before_request
def _bootstrap_background_jobs() -> None:
    _ensure_background_jobs_started()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route("/")
@app.route("/schwab")
def schwab_dashboard():
    return render_template("schwab_dashboard.html", perps_url=PERPS_SERVER_URL)


@app.route("/schwab/authorize")
def schwab_authorize():
    """Convenience redirect to Schwab's own login page -- the actual
    authorization step is a real interactive Schwab login (with 2FA) that
    nothing here can do on the account owner's behalf."""
    return redirect(schwab_client.get_authorization_url())


@app.route("/schcallback")
def schwab_callback():
    """Registered as this Schwab app's OAuth redirect_uri -- MUST exactly
    match SCHWAB_REDIRECT_URI and the callback URL registered on Schwab's
    developer portal for THIS server's own public URL (a separate Render
    service now has its own hostname, different from the perps server's)."""
    error = request.args.get("error")
    if error:
        return jsonify({"ok": False, "error": error}), 400
    code = request.args.get("code")
    if not code:
        return jsonify({"ok": False, "error": "missing_code"}), 400
    try:
        schwab_client.exchange_code_for_tokens(code)
    except Exception as exc:
        logger.exception("[schwab_server] schwab token exchange failed")
        return jsonify({"ok": False, "error": str(exc)}), 500
    return jsonify({"ok": True, "message": "Schwab account linked. You can close this tab."})


@app.route("/api/schwab/status")
def api_schwab_status():
    state = schwab_strategy._load_state()  # noqa: SLF001
    _, meta = schwab_model.load_model()
    latest_cycle = load_json(SCHWAB_LATEST_CYCLE_FILE, {})
    latest_position_check = load_json(SCHWAB_LATEST_POSITION_CHECK_FILE, {})
    latest_sweep = load_json(SCHWAB_LATEST_SWEEP_FILE, {})
    latest_backfill = load_json(SCHWAB_LATEST_BACKFILL_FILE, {})
    try:
        market_session = _cached_market_session()
    except Exception:
        market_session = {"session": "unknown", "is_open": False, "source": "error"}

    realized_pnl_by_date = state.get("realized_pnl_by_date") or {}
    total_realized_pnl = round(sum(float(v) for v in realized_pnl_by_date.values()), 6)
    positions = [
        {**p, **schwab_strategy.position_exit_levels(p)}
        for p in (state.get("positions") or [])
    ]
    logged_in = False
    try:
        logged_in = schwab_client.get_valid_access_token() is not None
    except Exception:
        logged_in = False

    return jsonify({
        "ok": True,
        "now": dt.datetime.now(dt.timezone.utc).isoformat(),
        "mode": schwab_strategy.MODE,
        "live_trading_enabled": schwab_strategy.LIVE_TRADING_ENABLED,
        "schwab_logged_in": logged_in,
        "balance": state.get("balance", schwab_strategy.SIMULATE_STARTING_BALANCE),
        "available_balance": schwab_strategy.get_available_balance() if schwab_strategy.MODE == "simulate" else None,
        "positions": positions,
        "open_position_count": len(positions),
        "max_concurrent_positions": schwab_strategy.MAX_CONCURRENT_POSITIONS,
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
        "latest_position_check": latest_position_check,
        "latest_sweep": latest_sweep,
        "latest_backfill": latest_backfill,
        "market_session": market_session,
        "params": {
            "position_size_pct": schwab_strategy.POSITION_SIZE_PCT,
            "max_concurrent_positions": schwab_strategy.MAX_CONCURRENT_POSITIONS,
            "take_profit_pct": schwab_strategy.TAKE_PROFIT_PCT,
            "stop_loss_pct": schwab_strategy.STOP_LOSS_PCT,
            "max_hold_minutes": schwab_strategy.MAX_HOLD_MINUTES,
            "daily_loss_cap_pct": schwab_strategy.DAILY_LOSS_CAP_PCT,
            "model_confidence_min": schwab_strategy.MODEL_CONFIDENCE_MIN,
            "min_volume_z": schwab_strategy.MIN_VOLUME_Z,
            "min_volatility_ratio": schwab_strategy.MIN_VOLATILITY_RATIO,
            "fast_check_seconds": SCHWAB_FAST_CHECK_SECONDS,
            "entry_scan_minutes": SCHWAB_CYCLE_MINUTES,
        },
    })


@app.route("/api/schwab/trades")
def api_schwab_trades():
    state = schwab_strategy._load_state()  # noqa: SLF001
    trade_log = list(reversed(state.get("trade_log") or []))
    return jsonify({
        "ok": True,
        "trade_count": len(trade_log),
        "realized_pnl_by_date": state.get("realized_pnl_by_date") or {},
        "trades": trade_log[:200],
    })


@app.route("/api/schwab/tick", methods=["GET", "POST"])
def api_schwab_tick():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        fast = _run_schwab_fast_check()
        scan = _run_schwab_entry_scan()
        return jsonify({"ok": True, "fast_check": fast, "entry_scan": scan})
    except Exception as exc:
        logger.exception("[schwab_server] manual schwab tick failed: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/schwab/collect", methods=["GET", "POST"])
def api_schwab_collect():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_schwab_data_collect())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/schwab/train", methods=["GET", "POST"])
def api_schwab_train():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_schwab_train())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


_JOB_LABELS = {
    "schwab_data_collect": f"Schwab stock data collection -> HF (every {SCHWAB_DATA_COLLECT_MINUTES} min)",
    "schwab_train": f"Schwab model retrain (daily {SCHWAB_TRAIN_HOUR_ET:02d}:00 ET)",
    "schwab_intensive_training": (
        f"Schwab off-hours intensive training + sweep + historical backfill "
        f"(checked every {SCHWAB_INTENSIVE_TRAINING_MINUTES} min, runs only while market is closed)"
    ),
    "schwab_fast_check": f"Schwab fast exit check (every {SCHWAB_FAST_CHECK_SECONDS}s)",
    "schwab_entry_scan": f"Schwab entry scan (every {SCHWAB_CYCLE_MINUTES} min)",
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
        "scheduler_enabled": ENABLE_SCHWAB_SCHEDULER,
        "running_now": running_now,
        "last_by_job": last_by_job,
        "recent": recent,
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5001") or "5001")
    _ensure_background_jobs_started()
    app.run(host="0.0.0.0", port=port, debug=False)
