"""Alpaca CRYPTO trading bot -- its OWN web dashboard + background
scheduler, running as its own named server/process (alpaca_crypto_server),
in its own Render service, completely separate from alpaca_server.py
(equities) and app_kalshi.py (Kalshi perps).

This split exists for a real, confirmed reason: running the crypto
pipeline in the SAME process as the equities one pushed that 512MB
service into a genuine OOM crash loop (Render's own event log showed
oomKilled restarts within seconds of every boot). Same reasoning as the
original perps/equities split -- "a crash, OOM, or redeploy on one side
can no longer take the other down with it" -- just applied one asset
class later.

Background jobs, each cross-process locked (see server_common.py). No
market-hours gating anywhere here -- crypto trades 24/7:
  - alpaca_crypto_fast_check   every ALPACA_CRYPTO_FAST_CHECK_SECONDS --
                                manages an existing position
  - alpaca_crypto_entry_scan   every ALPACA_CRYPTO_CYCLE_MINUTES -- scans
                                the tradable crypto universe for a new entry
  - alpaca_crypto_data_collect every ALPACA_CRYPTO_DATA_COLLECT_MINUTES --
                                archives fresh minute bars to HF
  - alpaca_crypto_train        every ALPACA_CRYPTO_TRAIN_INTERVAL_MINUTES --
                                unconditional, no session check at all
                                (unlike alpaca_options_server.py's own
                                off-hours-only retrain gate): crypto has no
                                "off-hours" window to reserve training
                                for in the first place, so retraining just
                                rides the same always-on cadence as
                                everything else here, incorporating
                                whatever fresh rows alpaca_crypto_data_collect
                                has archived since the last retrain instead
                                of sitting on a once-daily cron while data
                                keeps arriving around the clock
  - alpaca_crypto_torch_train  daily at ALPACA_CRYPTO_TORCH_TRAIN_HOUR_UTC:00
                                UTC -- retrains the custom PyTorch candidate
                                in isolation (own scheduled job, not bundled
                                with alpaca_crypto_train's own 24/7 sklearn
                                retrain), promotes it only if it beats the
                                currently-live model. Deliberately much less
                                frequent than the sklearn retrain above --
                                see its own docstring for why isolation
                                matters here (measured locally: importing
                                torch alone costs ~154MB RSS).
  - alpaca_crypto_backtest_sweep daily at ALPACA_CRYPTO_BACKTEST_SWEEP_HOUR_UTC:00
                                UTC -- was manual-trigger only while this
                                service ran on a 512MB container with real,
                                confirmed OOM history from this exact job;
                                scheduled automatically now that it's on a
                                2GB (standard plan) container. Staggered an
                                hour after alpaca_crypto_torch_train so the
                                two heaviest jobs here don't stack.
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

from apscheduler.executors.pool import ThreadPoolExecutor as APSThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, render_template, request, send_from_directory
import pandas as pd

SRC_DIR = Path(__file__).resolve().parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import et_today
from data import alpaca_client, alpaca_crypto_data, alpaca_crypto_model, alpaca_crypto_strategy, threads_post
from server_common import DATA_DIR, is_cron_authorized, load_json, make_job_lock, save_json

# Same real, twice-confirmed production bug already fixed on the equities/
# perps servers -- see their own copies of this comment for the full
# story (bare `import huggingface_hub` is NOT enough; PEP 562 lazy
# __getattr__ loading means hf_hub_download/HfApi need to be named
# explicitly to actually force their submodules to resolve eagerly).
from huggingface_hub import HfApi, hf_hub_download  # noqa: F401

ALPACA_CRYPTO_CYCLE_MINUTES = max(1, int(os.getenv("ALPACA_CRYPTO_CYCLE_MINUTES", "2") or "2"))
ALPACA_CRYPTO_FAST_CHECK_SECONDS = max(5, int(os.getenv("ALPACA_CRYPTO_FAST_CHECK_SECONDS", "20") or "20"))
ALPACA_CRYPTO_DATA_COLLECT_MINUTES = max(5, int(os.getenv("ALPACA_CRYPTO_DATA_COLLECT_MINUTES", "15") or "15"))
# Replaces a once-daily fixed-hour cron: crypto trades and collects fresh
# data 24/7 with no closed session to speak of, so there's no "off-hours
# window" to reserve retraining for the way options does -- it just rides
# the same always-on interval cadence as fast_check/entry_scan/data_collect.
# 60 min gives ~24 real retrains/day (vs the old cron's 1), each one
# incorporating whatever fresh rows alpaca_crypto_data_collect (every
# ALPACA_CRYPTO_DATA_COLLECT_MINUTES, also unconditional) has archived
# since the last retrain. A full local profile of this exact training path
# (perps_model.py, structurally identical -- same n_estimators=100
# candidates) measured ~50s wall time even on a large real dataset, nowhere
# near this interval; picked more conservatively than options' own 30-min
# off-hours cadence specifically BECAUSE there's no quiet window here to
# absorb the cost -- this always runs alongside live trading decisions,
# never instead of them.
ALPACA_CRYPTO_TRAIN_INTERVAL_MINUTES = max(15, int(os.getenv("ALPACA_CRYPTO_TRAIN_INTERVAL_MINUTES", "60") or "60"))
# Fixed once-daily UTC hour (crypto has no market session to gate on, unlike
# stocks/options) -- see _run_alpaca_crypto_torch_train's own docstring for
# why this stays isolated from the sklearn retrain's own 24/7 interval.
ALPACA_CRYPTO_TORCH_TRAIN_HOUR_UTC = int(os.getenv("ALPACA_CRYPTO_TORCH_TRAIN_HOUR_UTC", "9") or "9")
# Fixed once-daily UTC hour, staggered an hour after the torch retrain above
# so the two heaviest jobs on this service don't stack. Was manual-trigger
# only (see _run_alpaca_crypto_backtest_sweep's own docstring) while this
# service ran on a 512MB container with real, confirmed OOM history --
# scheduled automatically now that it's on a 2GB (standard plan) container.
ALPACA_CRYPTO_BACKTEST_SWEEP_HOUR_UTC = int(os.getenv("ALPACA_CRYPTO_BACKTEST_SWEEP_HOUR_UTC", "10") or "10")
ALPACA_CRYPTO_STARTUP_GRACE_SECONDS = max(0, int(os.getenv("ALPACA_CRYPTO_STARTUP_GRACE_SECONDS", "60") or "60"))
ENABLE_ALPACA_CRYPTO_SCHEDULER = str(os.getenv("ENABLE_ALPACA_CRYPTO_SCHEDULER", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
DASHBOARD_LOCAL_AUTORUN = str(os.getenv("DASHBOARD_LOCAL_AUTORUN", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
# Cross-links to the other, separately-deployed services -- unknown at
# build time, filled in via env vars once each service's own hostname
# exists. Fall back to "#" (dead link, not a guess) if unset.
PERPS_SERVER_URL = os.getenv("PERPS_SERVER_URL", "#")
ALPACA_STOCKS_SERVER_URL = os.getenv("ALPACA_STOCKS_SERVER_URL", "#")
ALPACA_OPTIONS_SERVER_URL = os.getenv("ALPACA_OPTIONS_SERVER_URL", "#")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
app = Flask("alpaca_crypto_server", template_folder="templates")
# Real, confirmed production incident found in review (same mechanism
# caught live on the equities service, see alpaca_server.py's identical
# comment): APScheduler's default executor allows up to 10 jobs to run
# CONCURRENTLY in separate threads within this one process -- whenever two
# jobs' intervals share a common multiple, their peak memory STACKS
# instead of running one at a time, even on gunicorn's own single
# worker/thread. max_workers=1 forces every job through one queue,
# matching this whole codebase's stated "single worker, one thing at a
# time" design intent everywhere else.
#
# Real, confirmed production incident found while investigating "no
# Threads posts going out" (see app_kalshi.py's identical fix/comment):
# APScheduler's own default misfire_grace_time is a razor-thin 1 SECOND --
# any job whose scheduled fire time slips past that by even normal
# container/GIL scheduling jitter (routinely 10-15s, confirmed live in
# these logs) gets silently marked "missed" and SKIPPED entirely, not
# deferred-and-run-late. The frequent jobs (fast_check, entry_scan) dodge
# this by accident, since as the scheduler's own most-frequent jobs they
# effectively drive its internal wakeup timing; the hourly/half-hourly
# Threads posts do not, and were confirmed hitting this on essentially
# every single interval -- they'd never actually posted. job_defaults
# applies a generous grace window scheduler-wide, harmless for the
# frequent jobs (already on time regardless) and the actual fix here.
scheduler = BackgroundScheduler(
    timezone="America/New_York", job_defaults={"misfire_grace_time": 300},
    executors={"default": APSThreadPoolExecutor(max_workers=1)},
)
_startup_lock = threading.Lock()
_startup_done = False

_locked_job = make_job_lock(DATA_DIR / "alpaca_crypto_job_run_history.json", DATA_DIR / "alpaca_crypto_locks")

JOB_HISTORY_FILE = DATA_DIR / "alpaca_crypto_job_run_history.json"
JOB_LOCK_DIR = DATA_DIR / "alpaca_crypto_locks"
ALPACA_CRYPTO_LATEST_CYCLE_FILE = DATA_DIR / "alpaca_crypto_latest_cycle.json"
ALPACA_CRYPTO_LATEST_POSITION_CHECK_FILE = DATA_DIR / "alpaca_crypto_latest_position_check.json"
ALPACA_CRYPTO_LATEST_SWEEP_FILE = DATA_DIR / "alpaca_crypto_latest_sweep.json"


# Same reasoning as every other server here: on SIGTERM, APScheduler's
# background thread can still be mid-cycle when the interpreter starts
# tearing down. atexit runs before that race can occur.
@atexit.register
def _shutdown_scheduler() -> None:
    try:
        if scheduler.running:
            scheduler.shutdown(wait=False)
    except Exception:
        logger.exception("[alpaca_crypto_server] error shutting down scheduler at exit")


# ---------------------------------------------------------------------------
# Background jobs
# ---------------------------------------------------------------------------
@_locked_job("alpaca_crypto_fast_check", stale_after_sec=60)
def _run_alpaca_crypto_fast_check() -> dict[str, Any]:
    result = alpaca_crypto_strategy.manage_open_positions()
    if result.get("action") != "no_position":
        save_json(ALPACA_CRYPTO_LATEST_POSITION_CHECK_FILE, result)
    return result


@_locked_job("alpaca_crypto_entry_scan", stale_after_sec=300)
def _run_alpaca_crypto_entry_scan() -> dict[str, Any]:
    """gc.collect() here matches the same defense proven necessary on the
    equities entry-scan job this session (no hygiene at all there was
    OOM-crashing that service every 15-20 minutes, around the clock)."""
    try:
        result = alpaca_crypto_strategy.scan_and_enter()
        save_json(ALPACA_CRYPTO_LATEST_CYCLE_FILE, result)
        return result
    finally:
        gc.collect()


@_locked_job("alpaca_crypto_data_collect", stale_after_sec=600)
def _run_alpaca_crypto_data_collect() -> dict[str, Any]:
    """gc.collect() in `finally` -- same real OOM-mitigation discipline
    proven necessary for this exact pipeline when it shared a process
    with the equities strategy; kept here defensively even though this
    service no longer has that specific contention."""
    try:
        df = alpaca_crypto_data.collect_dataset_rows()
        if df.empty:
            return {"ok": False, "reason": "no_rows_collected"}
        return alpaca_crypto_data.push_minute_snapshot(df)
    except Exception as exc:
        logger.warning("[alpaca_crypto_server] data collect failed: %s", exc)
        return {"ok": False, "error": str(exc)}
    finally:
        gc.collect()


@_locked_job("alpaca_crypto_train", stale_after_sec=1800)
def _run_alpaca_crypto_train() -> dict[str, Any]:
    return alpaca_crypto_model.train_model()


@_locked_job("alpaca_crypto_torch_train", stale_after_sec=3600)
def _run_alpaca_crypto_torch_train() -> dict[str, Any]:
    """Daily, fully automatic retrain of the custom PyTorch candidate --
    "it has to do it automatically and improve and learn pattern and self
    improve[,] gather... data and use dataset and live data alone" (the
    user's own words): every run re-loads the SAME growing training
    dataset (the live collector + backfill both keep feeding it) and only
    promotes the new candidate if it actually beats whatever's currently
    live, so the model can only ever ratchet forward, never regress.

    Deliberately its OWN scheduled job, not folded into the every-
    ALPACA_CRYPTO_TRAIN_INTERVAL_MINUTES sklearn retrain above -- see
    _TorchMLPClassifier's own docstring in alpaca_crypto_model.py for why
    (measured locally: importing torch alone costs ~154MB RSS, real weight
    to keep off this service's existing OOM-documented candidate-fitting
    calls). Unlike stocks/options, crypto has no market session to gate
    on -- it's a fixed once-daily UTC cron instead, deliberately much less
    frequent than the 24/7 sklearn retrain, which is the whole point of
    keeping this isolated rather than stacked in."""
    try:
        return alpaca_crypto_model.train_torch_candidate_model()
    except Exception as exc:
        logger.warning("[alpaca_crypto_server] torch candidate training failed: %s", exc)
        return {"ok": False, "error": str(exc)}
    finally:
        gc.collect()


@_locked_job("alpaca_crypto_threads_trending_news", stale_after_sec=300)
def _run_alpaca_crypto_threads_trending_news() -> dict[str, Any]:
    """Posts a digest of what's currently trending in crypto news every 30
    minutes -- same rationale as the equities/perps servers' own copy:
    surfaces the same headlines feeding sentiment_score so what might be
    influencing the model's own decisions is visible. Reuses crypto_news.py
    directly (already coin-mapped, already proven for perps)."""
    try:
        from data import crypto_news
        headlines = crypto_news.get_trending_headlines(limit=5)
        posted = threads_post.post_trending_news(headlines, market="crypto")
        return {"ok": True, "posted": posted, "headline_count": len(headlines)}
    except Exception as exc:
        logger.warning("[alpaca_crypto_server] Threads trending-news post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


@_locked_job("alpaca_crypto_threads_sentiment_snapshot", stale_after_sec=600)
def _run_alpaca_crypto_threads_sentiment_snapshot() -> dict[str, Any]:
    """Posts a per-ticker sentiment bar-chart every 60 minutes -- every
    pair in the tradable universe, each with its OWN real news sentiment
    (crypto_news.get_sentiment(coin), the same call collect_dataset_rows/
    latest_feature_row already make every 15 min, so this is a cache hit
    the large majority of the time). Genuinely different from the
    trending-news post above: that surfaces headlines; this surfaces the
    actual per-ticker SCORES as a picture (auto-capped to the most
    bullish/bearish half-and-half by chart_snapshot.py itself since the
    full 36-pair universe wouldn't fit readably in one image). Read-only,
    never touches order placement."""
    try:
        from data import crypto_news
        symbols = alpaca_crypto_data.get_crypto_universe()
        ticker_sentiments = []
        for symbol in symbols:
            try:
                sentiment = crypto_news.get_sentiment(alpaca_crypto_data.symbol_to_coin(symbol))
                ticker_sentiments.append({"ticker": symbol, "sentiment_score": sentiment["sentiment_score"]})
            except Exception as exc:
                logger.debug("[alpaca_crypto_server] sentiment fetch failed for %s: %s", symbol, exc)
        posted = threads_post.post_sentiment_snapshot(market="crypto", ticker_sentiments=ticker_sentiments)
        return {"ok": True, "posted": posted, "ticker_count": len(ticker_sentiments)}
    except Exception as exc:
        logger.warning("[alpaca_crypto_server] Threads sentiment-snapshot post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


@_locked_job("alpaca_crypto_threads_hourly_status", stale_after_sec=300)
def _run_alpaca_crypto_threads_hourly_status() -> dict[str, Any]:
    """Real gap found in review: perps_strategy.py already posts an hourly
    "what am I holding right now" status regardless of whether a trade
    happened (app_kalshi.py's own _run_perps_threads_hourly_status) --
    crypto never had the equivalent. Best-effort, on top of (not instead
    of) the real-time trade-entry/exit posts -- never allowed to affect
    trading logic, which is why this reads state read-only and never
    touches order placement."""
    try:
        state = alpaca_crypto_strategy._load_state()  # noqa: SLF001
        now = dt.datetime.now(dt.timezone.utc)
        positions = []
        for p in (state.get("positions") or []):
            levels = alpaca_crypto_strategy.position_exit_levels(p)
            opened_at = dt.datetime.fromisoformat(p["opened_at"])
            held_minutes = (now - opened_at).total_seconds() / 60.0
            positions.append({**p, **levels, "ticker": p["symbol"], "held_minutes": held_minutes})
        realized_pnl_by_date = state.get("realized_pnl_by_date") or {}
        today_pnl = float(realized_pnl_by_date.get(et_today().isoformat(), 0.0))
        posted = threads_post.post_hourly_status(positions=positions, today_realized_pnl_usd=today_pnl, market="crypto")
        return {"ok": True, "posted": posted, "open_position_count": len(positions)}
    except Exception as exc:
        logger.warning("[alpaca_crypto_server] Threads hourly status post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


@_locked_job("alpaca_crypto_backtest_sweep", stale_after_sec=1800)
def _run_alpaca_crypto_backtest_sweep(*, days: int = 21) -> dict[str, Any]:
    """Daily, at ALPACA_CRYPTO_BACKTEST_SWEEP_HOUR_UTC:00 -- was manual-
    trigger only (see git history) while this service ran on a 512MB
    container with real, confirmed OOM history from this exact job.
    Unlike alpaca_options_backtest_sweep, which hides its own recurring
    sweep in an off-hours window, crypto trades 24/7 -- there is no safe
    idle window to run a multi-model-fit sweep in, so this still competes
    with live trading decisions for CPU once a day regardless of the
    memory headroom a 2GB container provides. Still reachable manually via
    POST /api/alpaca/crypto/backtest whenever a fresh reading is wanted
    outside the daily cadence."""
    from data import alpaca_crypto_backtest

    try:
        universe = alpaca_crypto_data.get_crypto_universe()
        frames = []
        for symbol in universe:
            feats = alpaca_crypto_backtest.build_pair_frame(symbol, days=days)
            if not feats.empty:
                frames.append(feats)
        if not frames:
            return {"ok": True, "skipped": True, "reason": "no_data"}
        combined = pd.concat(frames, ignore_index=True).sort_values("ts")
        del frames
        cutoff_ts = combined["ts"].quantile(0.7)
        train_df = combined[combined["ts"] < cutoff_ts]
        test_df = combined[combined["ts"] >= cutoff_ts]
        fitted = alpaca_crypto_backtest.fit_backtest_model(train_df)
        test_with_preds = alpaca_crypto_backtest.add_model_predictions(test_df, fitted)
        del combined, train_df, test_df
        gc.collect()
        sweep_result = alpaca_crypto_backtest.run_config_sweep(test_with_preds)
        del test_with_preds
        save_json(ALPACA_CRYPTO_LATEST_SWEEP_FILE, sweep_result)
        return sweep_result
    except Exception as exc:
        logger.warning("[alpaca_crypto_server] backtest sweep failed: %s", exc)
        return {"ok": False, "error": str(exc)}
    finally:
        gc.collect()


def _ensure_background_jobs_started() -> None:
    global _startup_done
    if _startup_done:
        return
    if not DASHBOARD_LOCAL_AUTORUN:
        return
    with _startup_lock:
        if _startup_done:
            return
        if not scheduler.running and ENABLE_ALPACA_CRYPTO_SCHEDULER:
            # next_run_time delayed a full interval -- see the identical
            # fix and its comment in app_kalshi.py's own perps_data_collect
            # registration: without this, APScheduler fires an interval
            # job's FIRST run immediately on scheduler.start(), duplicating
            # the _runner() thread's own direct startup call below.
            scheduler.add_job(
                _run_alpaca_crypto_data_collect, "interval", minutes=ALPACA_CRYPTO_DATA_COLLECT_MINUTES,
                id="alpaca_crypto_data_collect", replace_existing=True,
                next_run_time=dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=ALPACA_CRYPTO_DATA_COLLECT_MINUTES),
            )
            scheduler.add_job(
                _run_alpaca_crypto_train, "interval", minutes=ALPACA_CRYPTO_TRAIN_INTERVAL_MINUTES,
                id="alpaca_crypto_train", replace_existing=True,
                next_run_time=dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=ALPACA_CRYPTO_TRAIN_INTERVAL_MINUTES),
            )
            scheduler.add_job(
                _run_alpaca_crypto_torch_train, "cron", hour=ALPACA_CRYPTO_TORCH_TRAIN_HOUR_UTC, minute=0,
                timezone="UTC", id="alpaca_crypto_torch_train", replace_existing=True,
            )
            scheduler.add_job(
                _run_alpaca_crypto_backtest_sweep, "cron", hour=ALPACA_CRYPTO_BACKTEST_SWEEP_HOUR_UTC, minute=0,
                timezone="UTC", id="alpaca_crypto_backtest_sweep", replace_existing=True,
            )
            scheduler.add_job(
                _run_alpaca_crypto_fast_check, "interval", seconds=ALPACA_CRYPTO_FAST_CHECK_SECONDS,
                id="alpaca_crypto_fast_check", replace_existing=True,
            )
            scheduler.add_job(
                _run_alpaca_crypto_entry_scan, "interval", minutes=ALPACA_CRYPTO_CYCLE_MINUTES,
                id="alpaca_crypto_entry_scan", replace_existing=True,
                next_run_time=dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=ALPACA_CRYPTO_STARTUP_GRACE_SECONDS),
            )
            scheduler.add_job(
                _run_alpaca_crypto_threads_trending_news, "interval", minutes=30,
                id="alpaca_crypto_threads_trending_news", replace_existing=True,
            )
            scheduler.add_job(
                _run_alpaca_crypto_threads_sentiment_snapshot, "interval", minutes=60,
                id="alpaca_crypto_threads_sentiment_snapshot", replace_existing=True,
            )
            scheduler.add_job(
                _run_alpaca_crypto_threads_hourly_status, "interval", hours=1,
                id="alpaca_crypto_threads_hourly_status", replace_existing=True,
            )
            scheduler.start()
            logger.info(
                "Alpaca crypto scheduler started: fast exit check every %ds, entry scan every %d min "
                "(first run in %ds), data collect every %d min, retrain every %d min (24/7, no session gate), "
                "live_trading=%s",
                ALPACA_CRYPTO_FAST_CHECK_SECONDS, ALPACA_CRYPTO_CYCLE_MINUTES, ALPACA_CRYPTO_STARTUP_GRACE_SECONDS,
                ALPACA_CRYPTO_DATA_COLLECT_MINUTES, ALPACA_CRYPTO_TRAIN_INTERVAL_MINUTES,
                alpaca_crypto_strategy.LIVE_TRADING_ENABLED,
            )

        def _runner() -> None:
            # Real, confirmed incident: this used to call
            # _run_alpaca_crypto_data_collect() immediately and unconditionally
            # on every boot. collect_dataset_rows() with no watchlist narrowing
            # pulls LIVE_LOOKBACK_DAYS (5 days of 1-minute bars) for the FULL
            # crypto universe (36 USD pairs on this account, confirmed via
            # Alpaca's own /v2/assets) in one call, then push_minute_snapshot
            # downloads the day's existing shard and holds existing+new+merged
            # copies simultaneously while uploading -- confirmed via Render's
            # own event log to reliably OOM (oomKilled, 512Mi) about a minute
            # after every boot. Because this ran unconditionally on EVERY
            # restart, the OOM-triggered restart immediately re-ran the exact
            # same expensive step, making it a self-sustaining crash loop --
            # exactly the failure mode the train-immediately guard below
            # already exists to prevent, just missing here. The scheduled
            # alpaca_crypto_data_collect job (already registered above with a
            # next_run_time delayed by ALPACA_CRYPTO_DATA_COLLECT_MINUTES)
            # covers first collection on a sane cadence without this.
            #
            # Real gap found in review: manage_open_positions() already
            # reconciles against the real Alpaca account every fast-check
            # cycle (20s), which normally catches a restart-time gap
            # quickly enough -- but there's no EXPLICIT, immediately-
            # observable check confirming an order placed right before a
            # crash (state not yet saved) actually got picked back up. A
            # single get_positions() + local state read/write is cheap --
            # not the heavy collect_dataset_rows()/push_minute_snapshot()
            # chain the OOM incident above is specifically about, so this
            # is safe to run unconditionally on every boot.
            try:
                if alpaca_crypto_strategy.LIVE_TRADING_ENABLED:
                    with alpaca_crypto_strategy._STATE_LOCK:  # noqa: SLF001
                        state = alpaca_crypto_strategy._load_state()  # noqa: SLF001
                        state["positions"] = alpaca_crypto_strategy._reconcile_positions_with_exchange(state)  # noqa: SLF001
                        alpaca_crypto_strategy._save_state(state)  # noqa: SLF001
                    logger.info("Startup reconciliation: %d real open position(s) confirmed against Alpaca", len(state["positions"]))
            except Exception as exc:
                logger.warning("Startup reconciliation failed: %s", exc)
            # Same cold-start-only guard as every other server here: only
            # train immediately if nothing is cached yet, so a crash-
            # triggered restart can't turn into a self-sustaining retrain
            # loop.
            try:
                if alpaca_crypto_model.load_model()[0] is None:
                    train_result = _run_alpaca_crypto_train()
                    logger.info("Startup alpaca crypto train attempt (cold start): %s", train_result.get("reason", "ok"))
                else:
                    logger.info("Startup alpaca crypto train skipped: model already cached, the interval retrain will pick it up")
            except Exception as exc:
                logger.warning("Startup alpaca crypto train failed: %s", exc)
            # No immediate startup entry scan here -- same rolling-deploy
            # collision reasoning as every other server: the scheduled
            # entry-scan job's own delayed first tick already covers this.

        threading.Thread(target=_runner, daemon=True, name="alpaca-crypto-server-startup-autorun").start()
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
@app.route("/alpaca-crypto")
def alpaca_crypto_dashboard():
    return render_template(
        "alpaca_crypto_dashboard.html", perps_url=PERPS_SERVER_URL, stocks_url=ALPACA_STOCKS_SERVER_URL,
        options_url=ALPACA_OPTIONS_SERVER_URL,
    )


@app.route("/chart/<path:filename>")
def chart_snapshot_image(filename):
    """Serves a chart-snapshot PNG publicly -- see app_kalshi.py's own
    copy of this route for the full rationale (Threads fetches the image
    itself from this URL, no raw-upload step exists)."""
    from data import chart_snapshot
    return send_from_directory(chart_snapshot.CHARTS_DIR, filename)


@app.route("/api/alpaca/crypto/status")
def api_alpaca_crypto_status():
    state = alpaca_crypto_strategy._load_state()  # noqa: SLF001
    _, meta = alpaca_crypto_model.load_model()
    latest_cycle = load_json(ALPACA_CRYPTO_LATEST_CYCLE_FILE, {})
    latest_position_check = load_json(ALPACA_CRYPTO_LATEST_POSITION_CHECK_FILE, {})
    latest_sweep = load_json(ALPACA_CRYPTO_LATEST_SWEEP_FILE, {})

    realized_pnl_by_date = state.get("realized_pnl_by_date") or {}
    total_realized_pnl = round(sum(float(v) for v in realized_pnl_by_date.values()), 6)
    positions = [
        {**p, **alpaca_crypto_strategy.position_exit_levels(p)}
        for p in (state.get("positions") or [])
    ]

    account = alpaca_client.get_account() if alpaca_client.is_configured() else {}
    return jsonify({
        "ok": True,
        "now": dt.datetime.now(dt.timezone.utc).isoformat(),
        "account_type": "paper" if "paper-api" in alpaca_client.TRADING_BASE_URL else "live",
        "live_trading_enabled": alpaca_crypto_strategy.LIVE_TRADING_ENABLED,
        "alpaca_configured": alpaca_client.is_configured(),
        "balance": float(account.get("equity") or 0.0),
        "available_balance": float(account.get("cash") or 0.0),
        "positions": positions,
        "open_position_count": len(positions),
        "max_concurrent_positions": alpaca_crypto_strategy.MAX_CONCURRENT_POSITIONS,
        "today_realized_pnl_usd": float(realized_pnl_by_date.get(et_today().isoformat(), 0.0)),
        "total_realized_pnl_usd": total_realized_pnl,
        "trade_count": len(state.get("trade_log") or []),
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
        "latest_sweep": latest_sweep,
        "params": {
            "position_size_pct": alpaca_crypto_strategy.POSITION_SIZE_PCT,
            "max_concurrent_positions": alpaca_crypto_strategy.MAX_CONCURRENT_POSITIONS,
            "take_profit_pct": alpaca_crypto_strategy.TAKE_PROFIT_PCT,
            "stop_loss_pct": alpaca_crypto_strategy.STOP_LOSS_PCT,
            "max_hold_minutes": alpaca_crypto_strategy.MAX_HOLD_MINUTES,
            "daily_loss_cap_pct": alpaca_crypto_strategy.DAILY_LOSS_CAP_PCT,
            "model_confidence_min": alpaca_crypto_strategy.MODEL_CONFIDENCE_MIN,
            "min_volume_z": alpaca_crypto_strategy.MIN_VOLUME_Z,
            "min_volatility_ratio": alpaca_crypto_strategy.MIN_VOLATILITY_RATIO,
            "taker_fee_rate": alpaca_crypto_strategy.TAKER_FEE_RATE,
            "fast_check_seconds": ALPACA_CRYPTO_FAST_CHECK_SECONDS,
            "entry_scan_minutes": ALPACA_CRYPTO_CYCLE_MINUTES,
            "data_collect_minutes": ALPACA_CRYPTO_DATA_COLLECT_MINUTES,
            "train_interval_minutes": ALPACA_CRYPTO_TRAIN_INTERVAL_MINUTES,
        },
    })


@app.route("/api/alpaca/crypto/trades")
def api_alpaca_crypto_trades():
    state = alpaca_crypto_strategy._load_state()  # noqa: SLF001
    trade_log = list(reversed(state.get("trade_log") or []))
    return jsonify({
        "ok": True,
        "trade_count": len(trade_log),
        "realized_pnl_by_date": state.get("realized_pnl_by_date") or {},
        "trades": trade_log[:200],
    })


@app.route("/api/alpaca/crypto/tick", methods=["GET", "POST"])
def api_alpaca_crypto_tick():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        fast = _run_alpaca_crypto_fast_check()
        scan = _run_alpaca_crypto_entry_scan()
        return jsonify({"ok": True, "fast_check": fast, "entry_scan": scan})
    except Exception as exc:
        logger.exception("[alpaca_crypto_server] manual tick failed: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/alpaca/crypto/collect", methods=["GET", "POST"])
def api_alpaca_crypto_collect():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_alpaca_crypto_data_collect())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/alpaca/crypto/train", methods=["GET", "POST"])
def api_alpaca_crypto_train():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_alpaca_crypto_train())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/alpaca/crypto/train_torch", methods=["GET", "POST"])
def api_alpaca_crypto_train_torch():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_alpaca_crypto_torch_train())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/alpaca/crypto/backtest", methods=["GET", "POST"])
def api_alpaca_crypto_backtest():
    """Also runs daily on its own schedule (see _run_alpaca_crypto_backtest_sweep's
    own docstring) -- this route is for an on-demand fresh reading in
    between those runs."""
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_alpaca_crypto_backtest_sweep())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


_JOB_LABELS = {
    "alpaca_crypto_data_collect": f"Alpaca crypto data collection -> HF (every {ALPACA_CRYPTO_DATA_COLLECT_MINUTES} min)",
    "alpaca_crypto_train": f"Alpaca crypto model retrain (every {ALPACA_CRYPTO_TRAIN_INTERVAL_MINUTES} min, 24/7)",
    "alpaca_crypto_torch_train": (
        f"Alpaca crypto custom PyTorch candidate retrain (daily {ALPACA_CRYPTO_TORCH_TRAIN_HOUR_UTC:02d}:00 UTC, "
        f"promoted only if it beats the currently-live model)"
    ),
    "alpaca_crypto_backtest_sweep": f"Alpaca crypto backtest sweep (daily {ALPACA_CRYPTO_BACKTEST_SWEEP_HOUR_UTC:02d}:00 UTC)",
    "alpaca_crypto_fast_check": f"Alpaca crypto fast exit check (every {ALPACA_CRYPTO_FAST_CHECK_SECONDS}s)",
    "alpaca_crypto_entry_scan": f"Alpaca crypto entry scan (every {ALPACA_CRYPTO_CYCLE_MINUTES} min, 24/7)",
    "alpaca_crypto_threads_trending_news": "Threads trending-news post (every 30 min)",
    "alpaca_crypto_threads_sentiment_snapshot": "Threads per-ticker sentiment snapshot (every 60 min)",
    "alpaca_crypto_threads_hourly_status": "Threads hourly open-positions status post",
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
        "scheduler_enabled": ENABLE_ALPACA_CRYPTO_SCHEDULER,
        "running_now": running_now,
        "last_by_job": last_by_job,
        "recent": recent,
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5002") or "5002")
    _ensure_background_jobs_started()
    app.run(host="0.0.0.0", port=port, debug=False)
