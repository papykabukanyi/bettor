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
  - alpaca_options_train        every ALPACA_OPTIONS_OFFHOURS_TRAIN_MINUTES,
                                 but only actually retrains when the market
                                 is NOT in its regular session (pre-market,
                                 post-market, weekends, holidays) -- a real
                                 no-op the instant it's checked during
                                 regular hours, so a multi-minute training
                                 run never contends with the live entry-scan/
                                 fast-check jobs for CPU/memory while real
                                 option orders may be in flight. Options only
                                 gets ALPACA_OPTIONS_DATA_COLLECT_MINUTES of
                                 fresh underlying data regardless of session,
                                 so there's no reason to let the model sit
                                 stale for up to 24h the way a once-daily
                                 cron did -- every off-hours window is now
                                 spent building the best-understood model
                                 the data supports, ready before the next
                                 regular session opens.
  - alpaca_options_torch_train  daily at ALPACA_OPTIONS_TORCH_TRAIN_HOUR_ET:00
                                 ET -- retrains the custom PyTorch candidate
                                 in isolation (own scheduled job, not bundled
                                 with alpaca_options_train's own sklearn/
                                 ensemble candidates), promotes it only if it
                                 beats the currently-live model. Same
                                 regular-hours skip discipline as the job above.

Unlike equities/crypto, entry_scan here IS gated to the regular session
(plus a small edge buffer -- see alpaca_options_strategy.py's own
AVOID_SESSION_EDGE_MINUTES): options liquidity/spreads are meaningfully
worse right at the open/close, a cost equities and 24/7 crypto don't
carry the same way.
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

SRC_DIR = Path(__file__).resolve().parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import et_today
from data import alpaca_client, alpaca_data, alpaca_options_backtest, alpaca_options_data, alpaca_options_model, alpaca_options_strategy, stock_news, threads_post
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
# How often to retrain while the market is closed -- NOT how often while
# it's open (see _run_alpaca_options_train's own session check, which
# makes this a genuine no-op during regular hours). 30 min gives roughly
# 35 real retrains across a typical ~17.5h off-hours window (pre-market +
# post-market + overnight) versus the old once-daily cron's single shot,
# each one incorporating whatever fresh rows alpaca_options_data_collect
# has archived since the last retrain -- a full local profile of this
# exact training path (perps_model.py, structurally identical) measured
# ~50s wall time even on a large real dataset, nowhere near this interval.
ALPACA_OPTIONS_OFFHOURS_TRAIN_MINUTES = max(10, int(os.getenv("ALPACA_OPTIONS_OFFHOURS_TRAIN_MINUTES", "30") or "30"))
# Daily, not every-N-minutes like the main sklearn retrain above -- the
# custom PyTorch candidate is a heavier, isolated job (see
# _run_alpaca_options_torch_train's own docstring), so it gets a low,
# predictable cadence instead.
ALPACA_OPTIONS_TORCH_TRAIN_HOUR_ET = int(os.getenv("ALPACA_OPTIONS_TORCH_TRAIN_HOUR_ET", "5") or "5")
# Same off-hours-only reasoning as training above, on its own separate
# cadence (a backtest sweep is heavier per-run than a single retrain --
# alpaca_options_backtest.run_config_sweep fits/replays 4 full parameter
# configs -- so it runs less often): "backtested for best strategies",
# reporting findings for TAKE_PROFIT_PCT/STOP_LOSS_PCT/MAX_HOLD_MINUTES/
# MODEL_CONFIDENCE_MIN only, never auto-applying a new config to the live
# strategy.
ALPACA_OPTIONS_BACKTEST_SWEEP_MINUTES = max(30, int(os.getenv("ALPACA_OPTIONS_BACKTEST_SWEEP_MINUTES", "120") or "120"))
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
#
# Real, confirmed production incident: _run_alpaca_options_train's data-
# loading phase (hundreds of individual per-shard HF network round trips,
# made meaningfully longer by the recent ALPACA_OPTIONS_MAX_TRAIN_ROWS
# increase) ran for 7+ minutes straight on the shared "default" single-
# worker pool, during which alpaca_options_fast_check -- the exit/TP-SL/
# max-hold check for OPEN positions, meant to run every 30s -- was
# completely blocked (confirmed live: "skipped: maximum number of running
# instances reached" repeating for the entire duration). perps already
# solved this exact problem for exactly this reason (see its own
# identical comment) with a dedicated executor for its own fast_check;
# applying the same fix here.
scheduler = BackgroundScheduler(
    timezone="America/New_York", job_defaults={"misfire_grace_time": 300},
    executors={
        "default": APSThreadPoolExecutor(max_workers=1),
        "fastcheck": APSThreadPoolExecutor(max_workers=1),
    },
)
_startup_lock = threading.Lock()
_startup_done = False

_locked_job = make_job_lock(DATA_DIR / "alpaca_options_job_run_history.json", DATA_DIR / "alpaca_options_locks")

JOB_HISTORY_FILE = DATA_DIR / "alpaca_options_job_run_history.json"
JOB_LOCK_DIR = DATA_DIR / "alpaca_options_locks"
ALPACA_OPTIONS_LATEST_CYCLE_FILE = DATA_DIR / "alpaca_options_latest_cycle.json"
ALPACA_OPTIONS_LATEST_POSITION_CHECK_FILE = DATA_DIR / "alpaca_options_latest_position_check.json"
ALPACA_OPTIONS_LATEST_SWEEP_FILE = DATA_DIR / "alpaca_options_latest_sweep.json"

# Same reasoning as alpaca_server.py's own copy of this: the dashboard
# polls /api/alpaca/options/status every 10s, and get_market_session() can
# make a real Alpaca API call. Session doesn't change within any given
# minute, so a short cache keeps that poll cheap regardless of refresh
# rate. Surfaced on the dashboard so it's visible at a glance whether
# options is currently in its trading window (regular hours) or its
# off-hours training window (see alpaca_options_train's own session gate)
# -- there was no way to tell this from the dashboard before.
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
    session = alpaca_data.get_market_session()
    with _MARKET_SESSION_CACHE_LOCK:
        _MARKET_SESSION_CACHE = dict(session)
        _MARKET_SESSION_CACHE_TS = time.monotonic()
    return session


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
    """gc.collect() here matches the same defense proven necessary on the
    equities entry-scan job this session (no hygiene at all there was
    OOM-crashing that service every 15-20 minutes, around the clock)."""
    try:
        result = alpaca_options_strategy.scan_and_enter()
        save_json(ALPACA_OPTIONS_LATEST_CYCLE_FILE, result)
        return result
    finally:
        gc.collect()


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
def _run_alpaca_options_train(*, force: bool = False) -> dict[str, Any]:
    """Runs every ALPACA_OPTIONS_OFFHOURS_TRAIN_MINUTES, but only actually
    trains outside the regular session -- see ALPACA_OPTIONS_OFFHOURS_TRAIN_MINUTES's
    own comment for why continuous off-hours retraining (not once a day)
    gets the model the best possible understanding of these underlyings
    before the next regular session opens, and why regular hours skip
    outright (a multi-minute retrain has no business competing with live
    entry-scan/fast-check jobs for CPU/memory while real option orders
    may be in flight).

    force=True bypasses the session check -- used by the cold-start path
    (a fresh boot with literally no cached model needs one immediately,
    regardless of session, same as every other service here) and by the
    manual /api/alpaca/options/train endpoint (a human/cron explicitly
    asking for a retrain should get one, not a silent skip)."""
    if not force:
        from data import alpaca_data
        if alpaca_data.get_market_session()["session"] == "regular":
            return {"ok": True, "skipped": "regular_hours"}
    return alpaca_options_model.train_model()


@_locked_job("alpaca_options_torch_train", stale_after_sec=3600)
def _run_alpaca_options_torch_train() -> dict[str, Any]:
    """Daily, fully automatic retrain of the custom PyTorch candidate --
    "it has to do it automatically and improve and learn pattern and self
    improve[,] gather... data and use dataset and live data alone" (the
    user's own words): every run re-loads the SAME growing training
    dataset and only promotes the new candidate if it actually beats
    whatever's currently live, using options' own recency weighting (same
    as the walk-forward loop above) since premium/IV dynamics move fast
    enough that recency matters here more than for stocks/crypto.

    Deliberately its OWN scheduled job at a low daily cadence, not folded
    into _run_alpaca_options_train's own every-N-minutes off-hours loop --
    see _TorchMLPClassifier's own docstring in alpaca_options_model.py for
    why (measured locally: importing torch alone costs ~154MB RSS, real
    weight to keep off a job that already fits up to 12 models per call).
    Same regular-hours skip discipline as every other heavy job here -- a
    multi-second-plus retrain has no business competing with live
    entry-scan/fast-check for CPU/memory while real option orders may be
    in flight."""
    from data import alpaca_data
    if alpaca_data.get_market_session()["session"] == "regular":
        return {"ok": True, "skipped": "regular_hours"}
    try:
        return alpaca_options_model.train_torch_candidate_model()
    except Exception as exc:
        logger.warning("[alpaca_options_server] torch candidate training failed: %s", exc)
        return {"ok": False, "error": str(exc)}
    finally:
        gc.collect()


@_locked_job("alpaca_options_backtest_sweep", stale_after_sec=1800)
def _run_alpaca_options_backtest_sweep() -> dict[str, Any]:
    """Off-hours-only, same reasoning as training: a fitted-model walk-
    forward replay plus a 4-config TAKE_PROFIT_PCT/STOP_LOSS_PCT/
    MAX_HOLD_MINUTES/MODEL_CONFIDENCE_MIN sweep (see
    alpaca_options_backtest.run_config_sweep) has no business competing
    with live entry-scan/fast-check for CPU/memory while real option
    orders may be in flight. Reports findings to the dashboard only --
    NEVER applies a new config to the live strategy automatically; that
    stays a deliberate, reviewed decision.

    Explicit del + gc.collect() after each heavy step mirrors the same
    real OOM-mitigation discipline alpaca_server.py's own intensive-
    training job already needed for the equivalent load-dataset-twice/
    fit-multiple-models shape."""
    from data import alpaca_data
    session = alpaca_data.get_market_session()
    if session["session"] == "regular":
        return {"ok": True, "skipped": True, "reason": "regular_hours", "session": session["session"]}

    try:
        df = alpaca_options_data.load_training_dataset()
        if df.empty:
            return {"ok": True, "skipped": True, "reason": "no_data"}
        cutoff_ts = df["ts"].quantile(0.7)
        train_df = df[df["ts"] < cutoff_ts]
        test_df = df[df["ts"] >= cutoff_ts]
        fitted = alpaca_options_backtest.fit_backtest_model(train_df)
        test_with_preds = alpaca_options_backtest.add_model_predictions(test_df, fitted)
        sweep_result = alpaca_options_backtest.run_config_sweep(test_with_preds)
        save_json(ALPACA_OPTIONS_LATEST_SWEEP_FILE, sweep_result)
        del df, train_df, test_df, test_with_preds, fitted
        return {"ok": True, "sweep_result": sweep_result}
    except Exception as exc:
        logger.warning("[alpaca_options_server] backtest sweep failed: %s", exc)
        return {"ok": False, "error": str(exc)}
    finally:
        gc.collect()


@_locked_job("alpaca_options_threads_trending_news", stale_after_sec=300)
def _run_alpaca_options_threads_trending_news() -> dict[str, Any]:
    """Posts a digest of what's currently trending in stock-market news
    every 30 minutes -- reuses stock_news.py directly (the options
    underlyings are ordinary equities, so the same general-market
    headlines already feeding sentiment_score apply here too)."""
    try:
        headlines = stock_news.get_trending_headlines(limit=5)
        posted = threads_post.post_trending_news(headlines, market="options")
        return {"ok": True, "posted": posted, "headline_count": len(headlines)}
    except Exception as exc:
        logger.warning("[alpaca_options_server] Threads trending-news post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


@_locked_job("alpaca_options_threads_sentiment_snapshot", stale_after_sec=600)
def _run_alpaca_options_threads_sentiment_snapshot() -> dict[str, Any]:
    """Posts a per-ticker sentiment bar-chart every 60 minutes -- every
    underlying this service trades options on, each with its OWN real
    news sentiment (stock_news.get_sentiment(symbol, company_name=...),
    the same call collect_dataset_rows/latest_feature_row already make,
    so this is a cache hit the large majority of the time). Genuinely
    different from the trending-news post above: that surfaces headlines;
    this surfaces the actual per-underlying SCORES as a picture. Read-only,
    never touches order placement."""
    try:
        from data import alpaca_data
        underlyings = alpaca_options_data.get_options_universe()
        ticker_sentiments = []
        for symbol in underlyings:
            try:
                sentiment = stock_news.get_sentiment(symbol, company_name=alpaca_data.get_company_name(symbol))
                ticker_sentiments.append({"ticker": symbol, "sentiment_score": sentiment["sentiment_score"]})
            except Exception as exc:
                logger.debug("[alpaca_options_server] sentiment fetch failed for %s: %s", symbol, exc)
        posted = threads_post.post_sentiment_snapshot(market="options", ticker_sentiments=ticker_sentiments)
        return {"ok": True, "posted": posted, "ticker_count": len(ticker_sentiments)}
    except Exception as exc:
        logger.warning("[alpaca_options_server] Threads sentiment-snapshot post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


@_locked_job("alpaca_options_threads_hourly_status", stale_after_sec=300)
def _run_alpaca_options_threads_hourly_status() -> dict[str, Any]:
    """Real gap found in review: perps_strategy.py already posts an hourly
    "what am I holding right now" status regardless of whether a trade
    happened (app_kalshi.py's own _run_perps_threads_hourly_status) --
    options never had the equivalent. Best-effort, on top of (not instead
    of) the real-time trade-entry/exit posts -- never allowed to affect
    trading logic, which is why this reads state read-only and never
    touches order placement. Uses each position's own contract symbol
    (not the underlying) as "ticker" -- the entry/TP/SL prices are option
    premiums, not underlying share prices, and the real-time entry/exit
    posts already use the contract symbol the same way."""
    try:
        state = alpaca_options_strategy._load_state()  # noqa: SLF001
        now = dt.datetime.now(dt.timezone.utc)
        positions = []
        for p in (state.get("positions") or []):
            levels = alpaca_options_strategy.position_exit_levels(p)
            opened_at = dt.datetime.fromisoformat(p["opened_at"])
            held_minutes = (now - opened_at).total_seconds() / 60.0
            positions.append({**p, **levels, "ticker": p["symbol"], "held_minutes": held_minutes})
        realized_pnl_by_date = state.get("realized_pnl_by_date") or {}
        today_pnl = float(realized_pnl_by_date.get(et_today().isoformat(), 0.0))
        posted = threads_post.post_hourly_status(positions=positions, today_realized_pnl_usd=today_pnl, market="options")
        return {"ok": True, "posted": posted, "open_position_count": len(positions)}
    except Exception as exc:
        logger.warning("[alpaca_options_server] Threads hourly status post failed: %s", exc)
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
                _run_alpaca_options_train, "interval", minutes=ALPACA_OPTIONS_OFFHOURS_TRAIN_MINUTES,
                id="alpaca_options_train", replace_existing=True,
                next_run_time=dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=ALPACA_OPTIONS_OFFHOURS_TRAIN_MINUTES),
            )
            scheduler.add_job(
                _run_alpaca_options_torch_train, "cron", hour=ALPACA_OPTIONS_TORCH_TRAIN_HOUR_ET, minute=0,
                id="alpaca_options_torch_train", replace_existing=True,
            )
            scheduler.add_job(
                _run_alpaca_options_backtest_sweep, "interval", minutes=ALPACA_OPTIONS_BACKTEST_SWEEP_MINUTES,
                id="alpaca_options_backtest_sweep", replace_existing=True,
                next_run_time=dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=ALPACA_OPTIONS_BACKTEST_SWEEP_MINUTES),
            )
            scheduler.add_job(
                _run_alpaca_options_fast_check, "interval", seconds=ALPACA_OPTIONS_FAST_CHECK_SECONDS,
                id="alpaca_options_fast_check", replace_existing=True, executor="fastcheck",
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
            scheduler.add_job(
                _run_alpaca_options_threads_sentiment_snapshot, "interval", minutes=60,
                id="alpaca_options_threads_sentiment_snapshot", replace_existing=True,
            )
            scheduler.add_job(
                _run_alpaca_options_threads_hourly_status, "interval", hours=1,
                id="alpaca_options_threads_hourly_status", replace_existing=True,
            )
            scheduler.start()
            logger.info(
                "Alpaca options scheduler started: fast exit check every %ds, entry scan every %d min "
                "(first run in %ds), data collect every %d min, retrain every %d min off-hours "
                "(skips as a no-op during regular hours), live_trading=%s",
                ALPACA_OPTIONS_FAST_CHECK_SECONDS, ALPACA_OPTIONS_CYCLE_MINUTES, ALPACA_OPTIONS_STARTUP_GRACE_SECONDS,
                ALPACA_OPTIONS_DATA_COLLECT_MINUTES, ALPACA_OPTIONS_OFFHOURS_TRAIN_MINUTES,
                alpaca_options_strategy.LIVE_TRADING_ENABLED,
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
            # Real gap found in review: manage_open_positions() already
            # reconciles against the real Alpaca account every fast-check
            # cycle, which normally catches a restart-time gap quickly
            # enough -- but there's no EXPLICIT, immediately-observable
            # check confirming an order placed right before a crash (state
            # not yet saved) actually got picked back up. A single
            # get_positions() + local state read/write is cheap -- not the
            # heavy data-collect chain the OOM incident above is
            # specifically about, so this is safe on every boot.
            try:
                if alpaca_options_strategy.LIVE_TRADING_ENABLED:
                    with alpaca_options_strategy._STATE_LOCK:  # noqa: SLF001
                        state = alpaca_options_strategy._load_state()  # noqa: SLF001
                        state["positions"] = alpaca_options_strategy._reconcile_positions_with_exchange(state)  # noqa: SLF001
                        alpaca_options_strategy._save_state(state)  # noqa: SLF001
                    logger.info("Startup reconciliation: %d real open position(s) confirmed against Alpaca", len(state["positions"]))
            except Exception as exc:
                logger.warning("Startup reconciliation failed: %s", exc)
            # Same cold-start-only guard as every other server here: only
            # train immediately if nothing is cached yet, so a crash-
            # triggered restart can't turn into a self-sustaining retrain
            # loop.
            try:
                if alpaca_options_model.load_model()[0] is None:
                    # force=True: a fresh boot with no cached model needs
                    # one now regardless of session -- the routine
                    # off-hours-only gate exists to protect regular-hours
                    # CPU/memory, not to block the one-time cold-start case.
                    train_result = _run_alpaca_options_train(force=True)
                    logger.info("Startup alpaca options train attempt (cold start): %s", train_result.get("reason", "ok"))
                else:
                    logger.info("Startup alpaca options train skipped: model already cached, off-hours interval will retrain")
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


@app.route("/chart/<path:filename>")
def chart_snapshot_image(filename):
    """Serves a chart-snapshot PNG publicly -- see app_kalshi.py's own
    copy of this route for the full rationale (Threads fetches the image
    itself from this URL, no raw-upload step exists)."""
    from data import chart_snapshot
    return send_from_directory(chart_snapshot.CHARTS_DIR, filename)


@app.route("/api/alpaca/options/status")
def api_alpaca_options_status():
    state = alpaca_options_strategy._load_state()  # noqa: SLF001
    _, meta = alpaca_options_model.load_model()
    latest_cycle = load_json(ALPACA_OPTIONS_LATEST_CYCLE_FILE, {})
    latest_position_check = load_json(ALPACA_OPTIONS_LATEST_POSITION_CHECK_FILE, {})
    latest_sweep = load_json(ALPACA_OPTIONS_LATEST_SWEEP_FILE, {})
    try:
        market_session = _cached_market_session()
    except Exception:
        market_session = {"session": "unknown", "is_open": False, "source": "error"}

    realized_pnl_by_date = state.get("realized_pnl_by_date") or {}
    total_realized_pnl = round(sum(float(v) for v in realized_pnl_by_date.values()), 6)
    positions = [
        {**p, **alpaca_options_strategy.position_exit_levels(p)}
        for p in (state.get("positions") or [])
    ]

    account = alpaca_client.get_account() if alpaca_client.is_configured() else {}
    return jsonify({
        "ok": True,
        "now": dt.datetime.now(dt.timezone.utc).isoformat(),
        "account_type": "paper" if "paper-api" in alpaca_client.TRADING_BASE_URL else "live",
        "live_trading_enabled": alpaca_options_strategy.LIVE_TRADING_ENABLED,
        "alpaca_configured": alpaca_client.is_configured(),
        "balance": float(account.get("equity") or 0.0),
        "available_balance": float(account.get("cash") or 0.0),
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
        "latest_sweep": latest_sweep,
        "market_session": market_session,
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
            "offhours_train_minutes": ALPACA_OPTIONS_OFFHOURS_TRAIN_MINUTES,
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
        # force=True: a human/cron explicitly hitting this endpoint wants
        # a real retrain, not a silent "skipped: regular_hours".
        return jsonify(_run_alpaca_options_train(force=True))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/alpaca/options/train_torch", methods=["GET", "POST"])
def api_alpaca_options_train_torch():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(alpaca_options_model.train_torch_candidate_model())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/alpaca/options/backtest", methods=["GET", "POST"])
def api_alpaca_options_backtest():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_alpaca_options_backtest_sweep())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


_JOB_LABELS = {
    "alpaca_options_data_collect": f"Alpaca options data collection -> HF (every {ALPACA_OPTIONS_DATA_COLLECT_MINUTES} min)",
    "alpaca_options_train": f"Alpaca options model retrain (every {ALPACA_OPTIONS_OFFHOURS_TRAIN_MINUTES} min off-hours)",
    "alpaca_options_torch_train": (
        f"Alpaca options custom PyTorch candidate retrain (daily {ALPACA_OPTIONS_TORCH_TRAIN_HOUR_ET:02d}:00 ET off-hours, "
        f"promoted only if it beats the currently-live model)"
    ),
    "alpaca_options_backtest_sweep": f"Alpaca options backtest sweep (every {ALPACA_OPTIONS_BACKTEST_SWEEP_MINUTES} min off-hours)",
    "alpaca_options_fast_check": f"Alpaca options fast exit check (every {ALPACA_OPTIONS_FAST_CHECK_SECONDS}s)",
    "alpaca_options_entry_scan": f"Alpaca options entry scan (every {ALPACA_OPTIONS_CYCLE_MINUTES} min)",
    "alpaca_options_threads_trending_news": "Threads trending-news post (every 30 min)",
    "alpaca_options_threads_sentiment_snapshot": "Threads per-ticker sentiment snapshot (every 60 min)",
    "alpaca_options_threads_hourly_status": "Threads hourly open-positions status post",
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
