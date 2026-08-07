"""Alpaca stock trading bot -- its OWN web dashboard + background
scheduler, running as its own named server/process (alpaca_stocks_server),
completely separate from app_kalshi.py. Nothing in this file imports
perps_* or knows the Kalshi perps bot exists.

Background jobs, each cross-process locked (see server_common.py):
  - alpaca_fast_check          every ALPACA_FAST_CHECK_SECONDS -- manages
                                                                    an existing
                                                                    position
  - alpaca_entry_scan          every ALPACA_CYCLE_MINUTES       -- watchlist
                                                                    scan for a
                                                                    new entry
  - alpaca_data_collect        every ALPACA_DATA_COLLECT_MINUTES -- archives
                                                                     fresh
                                                                     watchlist
                                                                     minute
                                                                     bars to HF
  - alpaca_train               daily at ALPACA_TRAIN_HOUR_ET:00 ET
  - alpaca_torch_train         daily at ALPACA_TORCH_TRAIN_HOUR_ET:00 ET --
                                retrains the custom PyTorch candidate in
                                isolation (own scheduled job, not bundled
                                with alpaca_train/intensive_training's own
                                sklearn candidates), promotes it only if it
                                beats the currently-live model.
  - alpaca_intensive_training  every ALPACA_INTENSIVE_TRAINING_MINUTES, but
                                only actually DOES anything while the market
                                is fully closed (nights, weekends): retrains,
                                runs a small backtest sweep, AND advances the
                                full-universe 20-year daily-bar historical
                                backfill by one batch (resumable -- see
                                alpaca_data.get_symbols_with_daily_bars).
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
from data import alpaca_backtest, alpaca_client, alpaca_data, alpaca_model, alpaca_strategy, stock_news, threads_post
from server_common import DATA_DIR, is_cron_authorized, load_json, make_job_lock, save_json

# Real production bug found and fixed on the Schwab side of this same
# server shape (now Alpaca's): every alpaca_*.py module does its OWN lazy
# `from huggingface_hub import ...` inside function bodies (a real, if
# small, startup-time saving when a code path never actually runs). But
# that means the FIRST import of huggingface_hub can happen from EITHER
# the single-threaded gunicorn request handler OR one of APScheduler's own
# background job threads -- two separate threads in the SAME process.
# Python's import machinery serializes concurrent imports of the SAME
# not-yet-loaded module via a per-module lock; if one thread's import is
# slow (huggingface_hub is a large package) while another thread blocks
# waiting for that same lock, the blocked thread can genuinely hang past
# gunicorn's own --timeout -- confirmed live on the predecessor Schwab
# server as a real "WORKER TIMEOUT" stuck inside Python's own
# `importlib._bootstrap._lock_unlock_module`.
#
# A first attempt at this fix (bare `import huggingface_hub`) turned out
# to be INCOMPLETE -- confirmed live on the perps server via a second real
# WORKER TIMEOUT with this exact traceback: huggingface_hub implements
# PEP 562 module-level `__getattr__` lazy loading, so importing the
# top-level package does NOT resolve `hf_hub_download`/`HfApi` themselves
# -- each is its own separate submodule import that only happens the
# FIRST TIME that specific name is accessed, via
# `huggingface_hub.__init__.__getattr__`. That first access can race
# exactly the same way the top-level import used to. Naming both
# attributes explicitly here forces THEIR lazy submodules to resolve too,
# eagerly, single-threaded, before the scheduler or Flask starts handling
# anything -- not just the top-level package object.
from huggingface_hub import HfApi, hf_hub_download  # noqa: F401

ALPACA_CYCLE_MINUTES = max(1, int(os.getenv("ALPACA_CYCLE_MINUTES", "2") or "2"))
ALPACA_FAST_CHECK_SECONDS = max(5, int(os.getenv("ALPACA_FAST_CHECK_SECONDS", "20") or "20"))
ALPACA_DATA_COLLECT_MINUTES = max(5, int(os.getenv("ALPACA_DATA_COLLECT_MINUTES", "15") or "15"))
ALPACA_TRAIN_HOUR_ET = int(os.getenv("ALPACA_TRAIN_HOUR_ET", "4") or "4")
# Staggered an hour after ALPACA_TRAIN_HOUR_ET -- purely for clear separation
# in logs/monitoring; APScheduler's single-worker executor here already
# serializes every job onto one thread, so there's no concurrent-execution
# memory-stacking risk either way.
ALPACA_TORCH_TRAIN_HOUR_ET = int(os.getenv("ALPACA_TORCH_TRAIN_HOUR_ET", "5") or "5")
# Checked every 30 min so it picks up the fully-closed window promptly
# (nights + weekends) -- a no-op the rest of the time.
ALPACA_INTENSIVE_TRAINING_MINUTES = max(10, int(os.getenv("ALPACA_INTENSIVE_TRAINING_MINUTES", "30") or "30"))
# How many symbols' worth of 20-year daily bars to push per off-hours tick --
# a full US equity universe at Alpaca's rate limit still takes a while, so
# this is a resumable BATCH, not a one-shot attempt at the whole universe.
# See _advance_historical_backfill.
ALPACA_BACKFILL_BATCH_SIZE = max(1, int(os.getenv("ALPACA_BACKFILL_BATCH_SIZE", "50") or "50"))
ENABLE_ALPACA_SCHEDULER = str(os.getenv("ENABLE_ALPACA_SCHEDULER", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
DASHBOARD_LOCAL_AUTORUN = str(os.getenv("DASHBOARD_LOCAL_AUTORUN", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
# Cross-link to the separately-deployed perps server -- unknown at build
# time (a different Render service gets its own generated hostname), so
# this is filled in via an env var once that service exists rather than
# hardcoded. Falls back to "#" (dead link, not a guess) if unset.
PERPS_SERVER_URL = os.getenv("PERPS_SERVER_URL", "#")
# Cross-link to the separately-deployed Alpaca CRYPTO service (split out
# into its own process/service after a real, confirmed OOM crash loop from
# running both strategies in one 512MB service -- see alpaca_crypto_server.py's
# own docstring for the full story).
ALPACA_CRYPTO_SERVER_URL = os.getenv("ALPACA_CRYPTO_SERVER_URL", "#")
# Cross-link to the separately-deployed Alpaca OPTIONS service (its own
# service from the start -- see alpaca_options_server.py's own docstring).
ALPACA_OPTIONS_SERVER_URL = os.getenv("ALPACA_OPTIONS_SERVER_URL", "#")
# Same rolling-deploy collision guard as the perps server's own entry-scan
# grace period -- see that file's comment for the full story.
ALPACA_STARTUP_GRACE_SECONDS = max(0, int(os.getenv("ALPACA_STARTUP_GRACE_SECONDS", "45") or "45"))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
app = Flask("alpaca_stocks_server", template_folder="templates")
# Real, confirmed production incident found in review: APScheduler's default
# executor allows up to 10 jobs to run CONCURRENTLY in separate threads
# within this one process -- when two jobs' intervals share a common
# multiple (e.g. the 15-min data_collect and 30-min intensive_training
# jobs both land on the same tick), their peak memory STACKS instead of
# running one at a time, even on gunicorn's own single worker/thread.
# Confirmed live via Render's own logs: an oomKilled crash landed right
# after data_collect, intensive_training, and threads_trending_news all
# fired at the identical scheduled instant. max_workers=1 forces every job
# through one queue, matching this whole codebase's stated "single
# worker, one thing at a time" design intent everywhere else.
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

_locked_job = make_job_lock(DATA_DIR / "alpaca_job_run_history.json", DATA_DIR / "alpaca_locks")

JOB_HISTORY_FILE = DATA_DIR / "alpaca_job_run_history.json"
JOB_LOCK_DIR = DATA_DIR / "alpaca_locks"
ALPACA_LATEST_CYCLE_FILE = DATA_DIR / "alpaca_latest_cycle.json"
ALPACA_LATEST_POSITION_CHECK_FILE = DATA_DIR / "alpaca_latest_position_check.json"
ALPACA_LATEST_SWEEP_FILE = DATA_DIR / "alpaca_latest_sweep.json"
ALPACA_LATEST_BACKFILL_FILE = DATA_DIR / "alpaca_latest_backfill.json"


# Same reasoning as app_kalshi.py's own copy of this: on SIGTERM,
# APScheduler's background thread can still be mid-cycle when the
# interpreter starts tearing down, raising "cannot schedule new futures
# after interpreter shutdown". atexit runs before that race can occur.
@atexit.register
def _shutdown_scheduler() -> None:
    try:
        if scheduler.running:
            scheduler.shutdown(wait=False)
    except Exception:
        logger.exception("[alpaca_server] error shutting down scheduler at exit")


# Same reasoning as app_kalshi.py's account-snapshot cache: the dashboard
# polls /api/alpaca/status every 10s, and get_market_session() can make a
# real Alpaca API call. Market session doesn't change within any given
# minute, so a short cache keeps that poll cheap regardless of refresh rate.
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


# ---------------------------------------------------------------------------
# Background jobs
# ---------------------------------------------------------------------------
@_locked_job("alpaca_fast_check", stale_after_sec=60)
def _run_alpaca_fast_check() -> dict[str, Any]:
    result = alpaca_strategy.manage_open_positions()
    if result.get("action") != "no_position":
        save_json(ALPACA_LATEST_POSITION_CHECK_FILE, result)
    return result


@_locked_job("alpaca_entry_scan", stale_after_sec=300)
def _run_alpaca_entry_scan() -> dict[str, Any]:
    """Real, confirmed production incident (Render's own events, this
    session), two rounds: first, this service was OOM-killing every 15-20
    minutes because scan_and_enter()'s load_training_dataset() call (every
    ALPACA_CYCLE_MINUTES, 2 min by default) had no gc.collect() at all --
    fixed by adding one. Crashes then continued at a 15-30 min cadence
    anyway, because gc.collect() can't reduce PEAK memory reached DURING
    that call, and a WATCHLIST_TOP_N=100 watchlist (double every other
    service's hot-loop universe) made both that call and scan_and_enter's
    own per-symbol loop genuinely too heavy for 512MB. Fixed by lowering
    WATCHLIST_TOP_N to 40 and load_training_dataset's max_rows to 5000
    (ranking-only, doesn't need training-grade depth)."""
    try:
        result = alpaca_strategy.scan_and_enter()
        save_json(ALPACA_LATEST_CYCLE_FILE, result)
        return result
    finally:
        gc.collect()


@_locked_job("alpaca_data_collect", stale_after_sec=600)
def _run_alpaca_data_collect() -> dict[str, Any]:
    """Collects the live watchlist (top symbols by real recent volume +
    volatility -- see alpaca_data.get_stock_watchlist) every
    ALPACA_DATA_COLLECT_MINUTES -- the continuously-"streaming" minute-bar
    archive prediction/training/backtesting reads from. push_minute_snapshot
    merges into today's existing HF shard rather than overwriting it, so a
    symbol that drops off the watchlist between cycles doesn't lose its
    earlier rows for today.

    Confirmed real recurring OOM on this exact service (512MB, running
    the stock AND crypto pipelines in the same process) -- the explicit
    `finally: gc.collect()` here mirrors the same real fix perps_data.py's
    own heaviest job already needed."""
    try:
        recent = alpaca_data.load_training_dataset(max_rows=5_000)
        watchlist = alpaca_data.get_stock_watchlist(recent if not recent.empty else None)
        df = alpaca_data.collect_dataset_rows(watchlist)
        if df.empty:
            return {"ok": False, "reason": "no_rows_collected"}
        return alpaca_data.push_minute_snapshot(df)
    except Exception as exc:
        logger.warning("[alpaca_server] data collect failed: %s", exc)
        return {"ok": False, "error": str(exc)}
    finally:
        gc.collect()


@_locked_job("alpaca_train", stale_after_sec=1800)
def _run_alpaca_train() -> dict[str, Any]:
    return alpaca_model.train_model()


@_locked_job("alpaca_torch_train", stale_after_sec=3600)
def _run_alpaca_torch_train() -> dict[str, Any]:
    """Daily, fully automatic retrain of the custom PyTorch candidate --
    "it has to do it automatically and improve and learn pattern and self
    improve[,] gather... data and use dataset and live data alone" (the
    user's own words): every run re-loads the SAME growing training
    dataset (the live collector + backfill both keep feeding it) and only
    promotes the new candidate if it actually beats whatever's currently
    live, so the model can only ever ratchet forward, never regress, purely
    from real archived + live data with no manual trigger required.

    Deliberately its OWN scheduled job, not folded into
    _run_alpaca_intensive_training above -- see _TorchMLPClassifier's own
    docstring in alpaca_model.py for why (measured locally: importing torch
    alone costs ~154MB RSS, real weight to keep off that job's already
    OOM-documented multi-candidate call). Scheduled at a different hour
    than both alpaca_train and alpaca_intensive_training's daily neighbors
    so it doesn't compound with them -- though APScheduler's single-worker
    executor here already serializes every job onto one thread regardless,
    ruling out true concurrent-execution memory stacking either way."""
    try:
        return alpaca_model.train_torch_candidate_model()
    except Exception as exc:
        logger.warning("[alpaca_server] torch candidate training failed: %s", exc)
        return {"ok": False, "error": str(exc)}
    finally:
        gc.collect()


@_locked_job("alpaca_threads_trending_news", stale_after_sec=300)
def _run_alpaca_threads_trending_news() -> dict[str, Any]:
    """Posts a digest of what's currently trending in stock-market news
    every 30 minutes -- the same general-market headlines sentiment_score
    is already built from, surfaced so the news potentially influencing
    the model's own decisions is visible rather than an invisible input.
    Read-only, never touches order placement."""
    try:
        headlines = stock_news.get_trending_headlines(limit=5)
        posted = threads_post.post_trending_news(headlines, market="stocks")
        return {"ok": True, "posted": posted, "headline_count": len(headlines)}
    except Exception as exc:
        logger.warning("[alpaca_server] Threads trending-news post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


@_locked_job("alpaca_threads_sentiment_snapshot", stale_after_sec=600)
def _run_alpaca_threads_sentiment_snapshot() -> dict[str, Any]:
    """Posts a per-ticker sentiment bar-chart every 60 minutes -- every
    symbol on the current watchlist, each with its OWN real news
    sentiment (stock_news.get_sentiment(symbol, company_name=...), the
    same call collect_dataset_rows/latest_feature_row already make, so
    this is a cache hit the large majority of the time). Genuinely
    different from the trending-news post above: that surfaces headlines;
    this surfaces the actual per-ticker SCORES as a picture. Read-only,
    never touches order placement."""
    try:
        recent = alpaca_data.load_training_dataset(max_rows=5_000)
        watchlist = alpaca_data.get_stock_watchlist(recent if not recent.empty else None)
        ticker_sentiments = []
        for symbol in watchlist:
            try:
                sentiment = stock_news.get_sentiment(symbol, company_name=alpaca_data.get_company_name(symbol))
                ticker_sentiments.append({"ticker": symbol, "sentiment_score": sentiment["sentiment_score"]})
            except Exception as exc:
                logger.debug("[alpaca_server] sentiment fetch failed for %s: %s", symbol, exc)
        posted = threads_post.post_sentiment_snapshot(market="stocks", ticker_sentiments=ticker_sentiments)
        return {"ok": True, "posted": posted, "ticker_count": len(ticker_sentiments)}
    except Exception as exc:
        logger.warning("[alpaca_server] Threads sentiment-snapshot post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


@_locked_job("alpaca_threads_hourly_status", stale_after_sec=300)
def _run_alpaca_threads_hourly_status() -> dict[str, Any]:
    """Real gap found in review: perps_strategy.py already posts an hourly
    "what am I holding right now" status regardless of whether a trade
    happened (app_kalshi.py's own _run_perps_threads_hourly_status) --
    stocks never had the equivalent. Best-effort, on top of (not instead
    of) the real-time trade-entry/exit posts -- never allowed to affect
    trading logic, which is why this reads state read-only and never
    touches order placement."""
    try:
        state = alpaca_strategy._load_state()  # noqa: SLF001
        now = dt.datetime.now(dt.timezone.utc)
        positions = []
        for p in (state.get("positions") or []):
            levels = alpaca_strategy.position_exit_levels(p)
            opened_at = dt.datetime.fromisoformat(p["opened_at"])
            held_minutes = (now - opened_at).total_seconds() / 60.0
            positions.append({**p, **levels, "ticker": p["symbol"], "held_minutes": held_minutes})
        realized_pnl_by_date = state.get("realized_pnl_by_date") or {}
        today_pnl = float(realized_pnl_by_date.get(et_today().isoformat(), 0.0))
        posted = threads_post.post_hourly_status(positions=positions, today_realized_pnl_usd=today_pnl, market="stocks")
        return {"ok": True, "posted": posted, "open_position_count": len(positions)}
    except Exception as exc:
        logger.warning("[alpaca_server] Threads hourly status post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def _advance_historical_backfill() -> dict[str, Any]:
    """Sends up to ALPACA_BACKFILL_BATCH_SIZE more symbols' worth of the
    MAXIMUM available daily-bar history (up to 20 years) to HF. Resumable
    across restarts/off-hours windows via alpaca_data.get_symbols_with_daily_bars()
    -- the full tradable universe at Alpaca's rate limit still takes a
    while, so this picks up a bit more each off-hours tick rather than
    needing to finish in one run."""
    try:
        universe = alpaca_data.get_us_stock_universe()
    except Exception as exc:
        return {"ok": False, "error": f"universe fetch failed: {exc}"}
    try:
        already_done = alpaca_data.get_symbols_with_daily_bars()
    except Exception as exc:
        logger.warning("[alpaca_server] could not list existing daily shards, starting fresh: %s", exc)
        already_done = set()

    remaining = [s for s in universe if s not in already_done]
    result: dict[str, Any] = {
        "universe_size": len(universe), "already_done": len(already_done), "remaining_before": len(remaining),
    }
    if not remaining:
        result["action"] = "backfill_complete"
        save_json(ALPACA_LATEST_BACKFILL_FILE, result)
        return result

    batch = remaining[:ALPACA_BACKFILL_BATCH_SIZE]
    pushed, failed = 0, 0
    for symbol in batch:
        try:
            df = alpaca_data.fetch_daily_bars(symbol, years=20)
            if not df.empty and alpaca_data.push_daily_snapshot(symbol, df).get("ok"):
                pushed += 1
            else:
                failed += 1
        except Exception as exc:
            failed += 1
            logger.debug("[alpaca_server] daily backfill failed for %s: %s", symbol, exc)
        time.sleep(0.3)  # stay comfortably under Alpaca's 200 req/min free-tier limit

    result.update({"action": "backfill_batch", "pushed": pushed, "failed": failed, "batch_size": len(batch)})
    save_json(ALPACA_LATEST_BACKFILL_FILE, result)
    return result


@_locked_job("alpaca_intensive_training", stale_after_sec=3600)
def _run_alpaca_intensive_training() -> dict[str, Any]:
    """Off-hours-only: while the market is fully closed (nights, weekends),
    use the idle time to (1) retrain, (2) run a small backtest sweep over
    recent real data, and (3) advance the full-universe historical
    backfill by one batch -- so a well-tuned strategy AND the maximum
    available historical dataset are both progressing without ever
    competing with live trading checks. A no-op (not an error) whenever
    the market isn't fully closed.

    Confirmed real recurring OOM on this exact service (512MB, running
    the stock AND crypto pipelines in the same process): this is the
    single heaviest job in the whole system -- it loads the full training
    dataset TWICE (once inside train_model(), again here for the sweep),
    trains up to 6 model candidates total, then a 4-config sweep, then a
    historical-bar backfill batch, all in one call with nothing freed in
    between. Explicit `del` + gc.collect() after each heavy step mirrors
    the same real fix perps_data.py's own heaviest job already needed."""
    session = alpaca_data.get_market_session()
    if session["session"] != "closed":
        return {"ok": True, "skipped": True, "reason": "market_not_closed", "session": session["session"]}

    train_result = alpaca_model.train_model()
    gc.collect()

    sweep_result = None
    try:
        df = alpaca_data.load_training_dataset()
        if not df.empty:
            cutoff_ts = df["ts"].quantile(0.7)
            train_df = df[df["ts"] < cutoff_ts]
            test_df = df[df["ts"] >= cutoff_ts]
            fitted = alpaca_backtest.fit_backtest_model(train_df)
            test_with_preds = alpaca_backtest.add_model_predictions(test_df, fitted)
            sweep_result = alpaca_backtest.run_config_sweep(test_with_preds)
            save_json(ALPACA_LATEST_SWEEP_FILE, sweep_result)
            del df, train_df, test_df, test_with_preds, fitted
        else:
            del df
    except Exception as exc:
        logger.warning("[alpaca_server] intensive backtest sweep failed: %s", exc)
    finally:
        gc.collect()

    backfill_result = None
    try:
        backfill_result = _advance_historical_backfill()
    except Exception as exc:
        logger.warning("[alpaca_server] historical backfill batch failed: %s", exc)
    finally:
        gc.collect()

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
        if not scheduler.running and ENABLE_ALPACA_SCHEDULER:
            # next_run_time delayed a full interval -- see the identical fix
            # and its comment in app_kalshi.py's own perps_data_collect
            # registration: without this, APScheduler fires an interval job's
            # FIRST run immediately on scheduler.start(), duplicating the
            # _runner() thread's own direct startup call to this same
            # function right below.
            scheduler.add_job(
                _run_alpaca_data_collect, "interval", minutes=ALPACA_DATA_COLLECT_MINUTES,
                id="alpaca_data_collect", replace_existing=True,
                next_run_time=dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=ALPACA_DATA_COLLECT_MINUTES),
            )
            scheduler.add_job(
                _run_alpaca_train, "cron", hour=ALPACA_TRAIN_HOUR_ET, minute=0,
                id="alpaca_train", replace_existing=True,
            )
            scheduler.add_job(
                _run_alpaca_intensive_training, "interval", minutes=ALPACA_INTENSIVE_TRAINING_MINUTES,
                id="alpaca_intensive_training", replace_existing=True,
            )
            scheduler.add_job(
                _run_alpaca_torch_train, "cron", hour=ALPACA_TORCH_TRAIN_HOUR_ET, minute=0,
                id="alpaca_torch_train", replace_existing=True,
            )
            scheduler.add_job(
                _run_alpaca_fast_check, "interval", seconds=ALPACA_FAST_CHECK_SECONDS,
                id="alpaca_fast_check", replace_existing=True,
            )
            scheduler.add_job(
                _run_alpaca_entry_scan, "interval", minutes=ALPACA_CYCLE_MINUTES,
                id="alpaca_entry_scan", replace_existing=True,
                next_run_time=dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=ALPACA_STARTUP_GRACE_SECONDS),
            )
            scheduler.add_job(
                _run_alpaca_threads_trending_news, "interval", minutes=30,
                id="alpaca_threads_trending_news", replace_existing=True,
            )
            scheduler.add_job(
                _run_alpaca_threads_sentiment_snapshot, "interval", minutes=60,
                id="alpaca_threads_sentiment_snapshot", replace_existing=True,
            )
            scheduler.add_job(
                _run_alpaca_threads_hourly_status, "interval", hours=1,
                id="alpaca_threads_hourly_status", replace_existing=True,
            )
            scheduler.start()
            logger.info(
                "Alpaca scheduler started: fast exit check every %ds, entry scan every %d min (first run in %ds), "
                "data collect every %d min, train daily at %02d:00 ET, intensive training checked every %d min, "
                "live_trading=%s",
                ALPACA_FAST_CHECK_SECONDS, ALPACA_CYCLE_MINUTES, ALPACA_STARTUP_GRACE_SECONDS,
                ALPACA_DATA_COLLECT_MINUTES, ALPACA_TRAIN_HOUR_ET, ALPACA_INTENSIVE_TRAINING_MINUTES,
                alpaca_strategy.LIVE_TRADING_ENABLED,
            )

        def _runner() -> None:
            try:
                # Real gap found in review: manage_open_positions() already
                # reconciles against the real Alpaca account every fast-check
                # cycle (20s), which normally catches a restart-time gap
                # quickly enough -- but there's no EXPLICIT, immediately-
                # observable check confirming an order placed right before a
                # crash (state not yet saved) actually got picked back up.
                # An explicit startup pass makes this immediate (not a ~20s
                # implicit wait) and gives a clear, loggable confirmation.
                if alpaca_strategy.LIVE_TRADING_ENABLED:
                    with alpaca_strategy._STATE_LOCK:  # noqa: SLF001
                        state = alpaca_strategy._load_state()  # noqa: SLF001
                        state["positions"] = alpaca_strategy._reconcile_positions_with_exchange(state)  # noqa: SLF001
                        alpaca_strategy._save_state(state)  # noqa: SLF001
                    logger.info("Startup reconciliation: %d real open position(s) confirmed against Alpaca", len(state["positions"]))
            except Exception as exc:
                logger.warning("Startup reconciliation failed: %s", exc)
            try:
                _run_alpaca_data_collect()
                logger.info("Startup alpaca data collect completed")
            except Exception as exc:
                logger.warning("Startup alpaca data collect failed: %s", exc)
            # Same cold-start-only guard as app_kalshi.py's own copy of
            # this: only train immediately if nothing is cached yet, so a
            # crash-triggered restart can't turn into a self-sustaining
            # retrain loop.
            try:
                if alpaca_model.load_model()[0] is None:
                    train_result = _run_alpaca_train()
                    logger.info("Startup alpaca train attempt (cold start): %s", train_result.get("reason", "ok"))
                else:
                    logger.info("Startup alpaca train skipped: model already cached, daily cron will retrain")
            except Exception as exc:
                logger.warning("Startup alpaca train failed: %s", exc)
            # No immediate startup entry scan here -- same rolling-deploy
            # collision reasoning as app_kalshi.py: the scheduled
            # alpaca_entry_scan job's own delayed first tick already covers
            # this safely.

        threading.Thread(target=_runner, daemon=True, name="alpaca-server-startup-autorun").start()
        _startup_done = True


@app.before_request
def _bootstrap_background_jobs() -> None:
    _ensure_background_jobs_started()


# Same real bug found and fixed on the perps side (app_kalshi.py): under
# gunicorn this file's ACTUAL production entrypoint never runs the
# `if __name__ == "__main__"` block below, so without this call the
# scheduler was only reachable via the before_request hook above -- meaning
# it wouldn't start until the first real HTTP request arrived, not on
# process boot. Calling it here means data collection/trading starts the
# instant the worker boots, with no dependency on anyone visiting the
# dashboard.
_ensure_background_jobs_started()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route("/")
@app.route("/alpaca")
def alpaca_dashboard():
    return render_template(
        "alpaca_dashboard.html", perps_url=PERPS_SERVER_URL, crypto_url=ALPACA_CRYPTO_SERVER_URL,
        options_url=ALPACA_OPTIONS_SERVER_URL,
    )


@app.route("/chart/<path:filename>")
def chart_snapshot_image(filename):
    """Serves a chart-snapshot PNG publicly -- see app_kalshi.py's own
    copy of this route for the full rationale (Threads fetches the image
    itself from this URL, no raw-upload step exists)."""
    from data import chart_snapshot
    return send_from_directory(chart_snapshot.CHARTS_DIR, filename)


@app.route("/api/alpaca/status")
def api_alpaca_status():
    state = alpaca_strategy._load_state()  # noqa: SLF001
    _, meta = alpaca_model.load_model()
    latest_cycle = load_json(ALPACA_LATEST_CYCLE_FILE, {})
    latest_position_check = load_json(ALPACA_LATEST_POSITION_CHECK_FILE, {})
    latest_sweep = load_json(ALPACA_LATEST_SWEEP_FILE, {})
    latest_backfill = load_json(ALPACA_LATEST_BACKFILL_FILE, {})
    try:
        market_session = _cached_market_session()
    except Exception:
        market_session = {"session": "unknown", "is_open": False, "source": "error"}

    realized_pnl_by_date = state.get("realized_pnl_by_date") or {}
    total_realized_pnl = round(sum(float(v) for v in realized_pnl_by_date.values()), 6)
    positions = [
        {**p, **alpaca_strategy.position_exit_levels(p)}
        for p in (state.get("positions") or [])
    ]

    account = alpaca_client.get_account() if alpaca_client.is_configured() else {}
    return jsonify({
        "ok": True,
        "now": dt.datetime.now(dt.timezone.utc).isoformat(),
        "account_type": "paper" if "paper-api" in alpaca_client.TRADING_BASE_URL else "live",
        "live_trading_enabled": alpaca_strategy.LIVE_TRADING_ENABLED,
        # Unlike Schwab's interactive OAuth login, Alpaca just needs a
        # static key/secret pair -- "configured" (both present) is the
        # whole readiness check, there's no separate "logged in" state.
        "alpaca_configured": alpaca_client.is_configured(),
        "balance": float(account.get("equity") or 0.0),
        "available_balance": float(account.get("cash") or 0.0),
        "positions": positions,
        "open_position_count": len(positions),
        "max_concurrent_positions": alpaca_strategy.MAX_CONCURRENT_POSITIONS,
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
        "latest_backfill": latest_backfill,
        "market_session": market_session,
        "params": {
            "position_size_pct": alpaca_strategy.POSITION_SIZE_PCT,
            "max_concurrent_positions": alpaca_strategy.MAX_CONCURRENT_POSITIONS,
            "take_profit_pct": alpaca_strategy.TAKE_PROFIT_PCT,
            "stop_loss_pct": alpaca_strategy.STOP_LOSS_PCT,
            "max_hold_minutes": alpaca_strategy.MAX_HOLD_MINUTES,
            "daily_loss_cap_pct": alpaca_strategy.DAILY_LOSS_CAP_PCT,
            "model_confidence_min": alpaca_strategy.MODEL_CONFIDENCE_MIN,
            "min_volume_z": alpaca_strategy.MIN_VOLUME_Z,
            "min_volatility_ratio": alpaca_strategy.MIN_VOLATILITY_RATIO,
            "fast_check_seconds": ALPACA_FAST_CHECK_SECONDS,
            "entry_scan_minutes": ALPACA_CYCLE_MINUTES,
            "data_collect_minutes": ALPACA_DATA_COLLECT_MINUTES,
            "train_hour_et": ALPACA_TRAIN_HOUR_ET,
        },
    })


@app.route("/api/alpaca/trades")
def api_alpaca_trades():
    state = alpaca_strategy._load_state()  # noqa: SLF001
    trade_log = list(reversed(state.get("trade_log") or []))
    return jsonify({
        "ok": True,
        "trade_count": len(trade_log),
        "realized_pnl_by_date": state.get("realized_pnl_by_date") or {},
        "trades": trade_log[:200],
    })


@app.route("/api/alpaca/tick", methods=["GET", "POST"])
def api_alpaca_tick():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        fast = _run_alpaca_fast_check()
        scan = _run_alpaca_entry_scan()
        return jsonify({"ok": True, "fast_check": fast, "entry_scan": scan})
    except Exception as exc:
        logger.exception("[alpaca_server] manual alpaca tick failed: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/alpaca/collect", methods=["GET", "POST"])
def api_alpaca_collect():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_alpaca_data_collect())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/alpaca/train", methods=["GET", "POST"])
def api_alpaca_train():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_alpaca_train())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/alpaca/train_torch", methods=["GET", "POST"])
def api_alpaca_train_torch():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_alpaca_torch_train())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


_JOB_LABELS = {
    "alpaca_data_collect": f"Alpaca stock data collection -> HF (every {ALPACA_DATA_COLLECT_MINUTES} min)",
    "alpaca_train": f"Alpaca model retrain (daily {ALPACA_TRAIN_HOUR_ET:02d}:00 ET)",
    "alpaca_torch_train": (
        f"Alpaca custom PyTorch candidate retrain (daily {ALPACA_TORCH_TRAIN_HOUR_ET:02d}:00 ET, "
        f"promoted only if it beats the currently-live model)"
    ),
    "alpaca_intensive_training": (
        f"Alpaca off-hours intensive training + sweep + historical backfill "
        f"(checked every {ALPACA_INTENSIVE_TRAINING_MINUTES} min, runs only while market is closed)"
    ),
    "alpaca_fast_check": f"Alpaca fast exit check (every {ALPACA_FAST_CHECK_SECONDS}s)",
    "alpaca_entry_scan": f"Alpaca entry scan (every {ALPACA_CYCLE_MINUTES} min)",
    "alpaca_threads_trending_news": "Threads trending-news post (every 30 min)",
    "alpaca_threads_sentiment_snapshot": "Threads per-ticker sentiment snapshot (every 60 min)",
    "alpaca_threads_hourly_status": "Threads hourly open-positions status post",
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
        "scheduler_enabled": ENABLE_ALPACA_SCHEDULER,
        "running_now": running_now,
        "last_by_job": last_by_job,
        "recent": recent,
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5001") or "5001")
    _ensure_background_jobs_started()
    app.run(host="0.0.0.0", port=port, debug=False)
