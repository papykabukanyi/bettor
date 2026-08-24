"""Kalshi Perps trading bot -- its OWN web dashboard + background scheduler,
running as its own named server/process (kalshi_perps_server), completely
separate from src/alpaca_server.py. Nothing in this file imports alpaca_*
or knows the Alpaca stocks bot exists. This is the single, canonical, complete file for
the Kalshi Perps server -- not a stub that imports the real logic from
somewhere else, specifically so nothing here can be silently broken by an
unrelated rename/move elsewhere in the repo (see the "ModuleNotFoundError:
No module named 'app'" production incident this replaces the root cause
of). Render's actual, live-configured Start Command is `gunicorn app:app`,
and app.py (repo root) is a one-line shim: `from app_kalshi import app`.

Single purpose: watch Kalshi's most active AND most volatile perp
instruments (see perps_data.get_watchlist() -- ranked live by 24h volume +
recent volatility, out of all 16 it lists: BTC, ETH, SOL, XRP, DOGE, LTC,
BCH, LINK, SUI, NEAR, DOT, HBAR, HYPE, kSHIB, XLM, ZEC), collect their
multi-timeframe price history + news sentiment to a Hugging Face dataset,
train a direction classifier on that history, and run a growth strategy that
splits the account into up to MAX_CONCURRENT_POSITIONS portions (each sized
at POSITION_SIZE_PCT of current balance, using each market's own embedded
leverage), opens dry-run-by-default positions when the technical signal and
the model agree, and takes profit per portion -- compounding as it grows.

Background jobs, each cross-process locked (see src/server_common.py) so a
single `--workers 1` gunicorn process never runs a job twice concurrently:
  - perps_fast_check    every PERPS_FAST_CHECK_SECONDS  -- ONLY manages an
                                                            existing position
                                                            (exit check incl.
                                                            velocity-based
                                                            quick-profit); the
                                                            "take profit fast
                                                            on a quick move"
                                                            loop
  - perps_entry_scan    every PERPS_CYCLE_MINUTES        -- full watchlist
                                                            scan for a NEW
                                                            entry (skips if a
                                                            position is
                                                            already open)
  - perps_data_collect  every PERPS_DATA_COLLECT_MINUTES -- archive fresh
                                                            candles + news to HF
  - perps_train         daily at PERPS_TRAIN_HOUR_ET:00 ET -- retrain the model,
                                                            now outcome-weighted by
                                                            trade_log's own real
                                                            wins/losses (see
                                                            perps_model.
                                                            _trade_outcome_sample_weight)
  - perps_trade_analysis daily at PERPS_TRADE_ANALYSIS_HOUR_ET:
                         PERPS_TRADE_ANALYSIS_MINUTE_ET ET -- turns trade_log's
                                                            real history into
                                                            win/loss diagnostics
                                                            (perps_trade_analysis.py),
                                                            posts a summary to
                                                            Threads, and applies a
                                                            small evidence-gated
                                                            MODEL_CONFIDENCE_MIN
                                                            adjustment when the
                                                            evidence clearly
                                                            supports it
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
from flask import Flask, Response, jsonify, redirect, render_template, request, send_from_directory
from werkzeug.middleware.proxy_fix import ProxyFix

# This file lives at the REPO ROOT (not inside src/), unlike every module it
# imports below -- SRC_DIR must be computed from here explicitly rather than
# just Path(__file__).resolve().parent, and Flask's template_folder must be
# an ABSOLUTE path into src/templates for the same reason (Flask resolves a
# relative template_folder against this file's own directory, which is now
# the repo root, not src/).
ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import et_today
from data import crypto_news, perps_data, perps_model, perps_strategy, perps_trade_analysis, threads_client, threads_post

# Real production bug found and fixed on the sibling stocks server (now
# alpaca_server.py, same comment there in full): every perps_*.py module
# does its own lazy `from huggingface_hub import ...` inside function
# bodies, which can race between the gunicorn request-handling thread and
# an APScheduler background job thread both importing it for the first
# time at once -- confirmed live there as a real "WORKER TIMEOUT" stuck
# inside Python's own import lock.
#
# A first attempt at this fix (bare `import huggingface_hub`) turned out
# to be INCOMPLETE -- confirmed live via a second real WORKER TIMEOUT with
# this exact traceback: huggingface_hub implements PEP 562 module-level
# `__getattr__` lazy loading, so importing the top-level package does NOT
# resolve `hf_hub_download`/`HfApi` themselves -- each is its own separate
# submodule import that only happens the FIRST TIME that specific name is
# accessed, via `huggingface_hub.__init__.__getattr__`. That first access
# can race exactly the same way the top-level import used to. Naming both
# attributes explicitly here forces THEIR lazy submodules to resolve too,
# eagerly, single-threaded, before the scheduler or Flask starts handling
# anything -- not just the top-level package object.
from huggingface_hub import HfApi, hf_hub_download  # noqa: F401
from data.kalshi_perps import get_margin_balance, get_margin_enabled, get_margin_exchange_status, get_margin_positions
from server_common import DATA_DIR, check_rate_limit, is_cron_authorized, load_json, make_job_lock, save_json, win_rate_stats

PERPS_CYCLE_MINUTES = max(1, int(os.getenv("PERPS_CYCLE_MINUTES", "2") or "2"))
PERPS_FAST_CHECK_SECONDS = max(5, int(os.getenv("PERPS_FAST_CHECK_SECONDS", "20") or "20"))
PERPS_DATA_COLLECT_MINUTES = max(5, int(os.getenv("PERPS_DATA_COLLECT_MINUTES", "15") or "15"))
PERPS_TRAIN_HOUR_ET = int(os.getenv("PERPS_TRAIN_HOUR_ET", "3") or "3")
# 30 min after PERPS_TRAIN_HOUR_ET, not the same minute -- runs after the
# fresh model/data from the train job above have settled, not concurrently
# with them.
PERPS_TRADE_ANALYSIS_HOUR_ET = int(os.getenv("PERPS_TRADE_ANALYSIS_HOUR_ET", "3") or "3")
PERPS_TRADE_ANALYSIS_MINUTE_ET = int(os.getenv("PERPS_TRADE_ANALYSIS_MINUTE_ET", "30") or "30")
# Render's rolling (zero-downtime) deploy briefly runs the OLD and NEW
# instance of this service at once -- the new one passes its health check
# and starts serving before the old one receives SIGTERM. Since this app's
# background scheduler runs independently of HTTP traffic, BOTH instances'
# schedulers are live during that overlap. Confirmed live in production
# logs: two full "Perps scheduler started" + "Startup entry scan completed"
# sequences firing within 3 seconds of each other on one deploy -- meaning
# two independent processes could each place a REAL entry order for the
# same signal. This delay makes a freshly-booted instance wait before its
# first entry-scan tick, giving the overlap window (observed as roughly
# 10-30s) time to resolve so only one live instance is actually opening new
# positions at a time. Exits (perps_fast_check) are NOT delayed -- closing
# a position redundantly from two instances is safe (the second reduce_only
# attempt just finds nothing left to close), so there's no reason to slow
# down stop-loss/take-profit coverage.
PERPS_STARTUP_GRACE_SECONDS = max(0, int(os.getenv("PERPS_STARTUP_GRACE_SECONDS", "45") or "45"))
# Dry-run is always the hard default regardless of this flag (see
# perps_strategy.LIVE_TRADING_ENABLED) -- this only controls whether the
# scheduler runs the loop AT ALL. Default ON: the whole point of this bot is
# to run continuously, and dry-run cycles place no real orders.
ENABLE_PERPS_SCHEDULER = str(os.getenv("ENABLE_PERPS_SCHEDULER", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
DASHBOARD_LOCAL_AUTORUN = str(os.getenv("DASHBOARD_LOCAL_AUTORUN", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
# Cross-links to the separately-deployed Alpaca stocks and Alpaca crypto
# servers -- unknown at build time (each Render service gets its own
# generated hostname), so these are filled in via env vars once each
# service exists rather than hardcoded. Fall back to "#" (dead link, not a
# guess) if unset.
ALPACA_SERVER_URL = os.getenv("ALPACA_SERVER_URL", "#")
ALPACA_CRYPTO_SERVER_URL = os.getenv("ALPACA_CRYPTO_SERVER_URL", "#")
ALPACA_OPTIONS_SERVER_URL = os.getenv("ALPACA_OPTIONS_SERVER_URL", "#")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
# Real, confirmed noise found in review: huggingface_hub's own internal
# logger warns "No files have been modified since last commit. Skipping
# to prevent empty commit." on every single upload call that happened to
# produce identical bytes to what's already archived -- EXPECTED,
# harmless behavior (confirmed live: 500+ occurrences per service across
# 5 days), not a real warning, but at WARNING level it drowned out
# genuine warnings in the same log stream. Downgraded to this specific
# library logger only -- this app's OWN logging (via `logger` below)
# is completely unaffected.
logging.getLogger("huggingface_hub.hf_api").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
app = Flask("kalshi_perps_server", template_folder=str(SRC_DIR / "templates"), static_folder=str(SRC_DIR / "static"))
# Render terminates TLS and proxies every request to this process over its
# own internal network -- without this, request.remote_addr is Render's
# internal proxy IP (10.x.x.x) for every single visitor, not the real
# client, which would make both the rate limiter below and any future
# per-IP logic meaningless (everyone looks like the same "IP"). ProxyFix
# trusts exactly one hop of X-Forwarded-For/-Proto, matching Render's own
# single-proxy architecture (confirmed live: Render's own edge access logs
# already show a real clientIP= field distinct from what this process saw
# before this fix).
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
# Real, confirmed production incident found in review (same mechanism
# caught live on the equities service, see alpaca_server.py's identical
# comment): APScheduler's default executor allows up to 10 jobs to run
# CONCURRENTLY in separate threads within this one process -- whenever two
# jobs' intervals share a common multiple, their peak memory STACKS
# instead of running one at a time, even on gunicorn's own single
# worker/thread. "default" gets max_workers=1 for the same reason as
# every other service. Real money here specifically: perps_fast_check is
# the exit/TP-SL/max-hold check for OPEN positions every 20s -- it must
# never queue behind a slow job (data_collect, the up-to-30-min daily
# train) sharing the default pool, so it gets its own dedicated
# single-worker executor instead of contending with everything else.
#
# Real, confirmed production incident found while investigating "no
# Threads posts going out": APScheduler's own default misfire_grace_time
# is a razor-thin 1 SECOND -- any job whose scheduled fire time slips past
# that by even normal container/GIL scheduling jitter (routinely 10-15s,
# confirmed live in these logs) gets silently marked "missed" and SKIPPED
# entirely, not deferred-and-run-late. perps_fast_check (20s) and
# perps_entry_scan (2min) dodge this by accident -- as the scheduler's own
# most-frequent jobs, they effectively drive its internal wakeup timing,
# so they land within that 1s window almost every time. The much-less-
# frequent hourly/half-hourly Threads posts (hourly_status,
# sentiment_snapshot, trending_news) do NOT get that same luck and were
# confirmed hitting this on essentially every single interval, meaning
# they'd never actually posted since misfire handling silently drops a
# missed run rather than queuing it. job_defaults applies a generous grace
# window scheduler-wide -- harmless for the real-money-critical jobs
# (a fast_check running a few seconds late is a non-issue; it was already
# on time regardless) and the actual fix for the ones that weren't.
scheduler = BackgroundScheduler(
    timezone="America/New_York",
    job_defaults={"misfire_grace_time": 300},
    executors={
        "default": APSThreadPoolExecutor(max_workers=1),
        # max_workers=2, not 1 -- same real, confirmed fix as the identical
        # executor in alpaca_crypto_server.py (see its comment): a single
        # worker serializing fast_check with entry_scan/trending_news/
        # sentiment_snapshot/hourly_status structurally guarantees skipped
        # fast_check ticks whenever any of those (measured 30s+) runs longer
        # than fast_check's own interval -- a capacity problem no stagger
        # offset can fix. A 2nd worker lets fast_check run concurrently.
        "fastcheck": APSThreadPoolExecutor(max_workers=2),
    },
)
_startup_lock = threading.Lock()
_startup_done = False

_locked_job = make_job_lock(DATA_DIR / "perps_job_run_history.json", DATA_DIR / "perps_locks")

# Confirmed live: on SIGTERM (a normal restart/redeploy), gunicorn's worker
# begins interpreter shutdown while APScheduler's own background thread --
# independent of the request-handling thread gunicorn manages -- can still
# be mid-cycle and try to submit a job to its thread pool executor right as
# that pool is being torn down, raising "RuntimeError: cannot schedule new
# futures after interpreter shutdown" (visible in production logs). A raw
# SIGTERM signal handler would risk overriding gunicorn's own graceful
# worker shutdown; atexit runs earlier in the SAME interpreter shutdown
# sequence without touching signal disposition at all, so telling the
# scheduler to stop here reliably happens before that race can occur.
@atexit.register
def _shutdown_scheduler() -> None:
    try:
        if scheduler.running:
            scheduler.shutdown(wait=False)
    except Exception:
        logger.exception("[app_kalshi] error shutting down scheduler at exit")


# `/api/status` used to make 3 sequential blocking Kalshi calls on every
# dashboard poll (every 10s from the browser). Worst case (each near its own
# timeout) that adds up to more than gunicorn's default 30s worker timeout,
# which kills and restarts the ONE worker -- which re-runs the whole startup
# sequence (scheduler re-added, jobs re-registered) and is exactly what made
# the dashboard appear to "lose everything" and stutter. A short cache
# means only one real request per window actually hits Kalshi.
_ACCOUNT_SNAPSHOT_CACHE: dict[str, Any] = {}
_ACCOUNT_SNAPSHOT_CACHE_TS = 0.0
_ACCOUNT_SNAPSHOT_CACHE_LOCK = threading.Lock()
_ACCOUNT_SNAPSHOT_CACHE_TTL_SEC = max(5, int(os.getenv("ACCOUNT_SNAPSHOT_CACHE_TTL_SEC", "12") or "12"))
_ACCOUNT_SNAPSHOT_REFRESH_TIMEOUT_SEC = int(os.getenv("ACCOUNT_SNAPSHOT_REFRESH_TIMEOUT_SEC", "20") or "20")


def _refresh_account_snapshot_cache() -> dict[str, Any]:
    """Does the real Kalshi network round trip and writes the cache. Real,
    confirmed live incident (2026-08-22): calling this synchronously from
    the HTTP request thread on a cache miss let a single slow upstream call
    (measured 57s+ on the equivalent Alpaca account-fetch this same day)
    run right up against gunicorn's --timeout 300, which sent the ONE
    worker a SIGKILL ("WORKER TIMEOUT") and restarted it -- exactly the
    "dashboard loses everything and stutters" failure mode this cache was
    originally built to prevent, just via the miss path instead of the hit
    path. Moving the actual fetch here and calling it ONLY from the
    fast_check background job (see its own comment) means a slow upstream
    call blocks a scheduler thread, never a web request -- but fast_check
    itself is trading-critical (manage_open_positions), so this is ALSO
    wrapped in call_with_hard_timeout (same pattern as every HF download in
    this codebase, see server_common's own docstring) so a truly hung
    Kalshi call can't stall real position management either, just this one
    balance refresh."""
    from server_common import call_with_hard_timeout
    return call_with_hard_timeout(
        _do_refresh_account_snapshot, timeout_sec=_ACCOUNT_SNAPSHOT_REFRESH_TIMEOUT_SEC,
        on_timeout={"ok": False, "margin_enabled": None, "exchange_active": None, "available_balance_usd": 0.0},
    )


def _do_refresh_account_snapshot() -> dict[str, Any]:
    global _ACCOUNT_SNAPSHOT_CACHE, _ACCOUNT_SNAPSHOT_CACHE_TS
    account_ok = True
    balance_usd = 0.0
    margin_enabled = None
    exchange_active = None
    try:
        margin_enabled = bool(get_margin_enabled().get("enabled"))
    except Exception as exc:
        account_ok = False
        # Raised from debug -- real, confirmed live investigation
        # (2026-08-23): the dashboard's Balance figure sat frozen at one
        # exact value for 3+ hours while the SEPARATELY-tracked milestone
        # panel (record_milestone, fed from this SAME function's own
        # return value, see _run_perps_fast_check's own caller below)
        # showed the real balance moving the whole time -- meaning
        # WHICHEVER of these three sub-fetches was actually failing during
        # that window did so silently, since debug-level never reaches
        # Render's shipped logs. Promoted to warning so a recurrence is
        # actually visible instead of invisible-by-design.
        logger.warning("[app_kalshi] margin_enabled check failed during account snapshot refresh: %s", exc)
    try:
        exchange_active = bool(get_margin_exchange_status().get("exchange_active"))
    except Exception as exc:
        logger.warning("[app_kalshi] exchange_status check failed during account snapshot refresh: %s", exc)
    try:
        balance = get_margin_balance(compute_available_balance=True)
        subaccounts = balance.get("subaccount_balances") or []
        for sub in subaccounts:
            balance_usd = max(balance_usd, float(sub.get("available_balance") or 0.0))
        if len(subaccounts) > 1:
            # The real, confirmed-live discrepancy above is consistent
            # with this max()-over-subaccounts picking a DIFFERENT (and
            # possibly stale/inactive) subaccount than the one actually
            # funding perps positions, if this account ever has more than
            # one -- logged whenever that's actually the case so a
            # recurrence can be diagnosed from real data instead of
            # guessed at again.
            logger.info(
                "[app_kalshi] account snapshot: %d subaccounts, balances=%s, using max=%.4f",
                len(subaccounts), [round(float(s.get("available_balance") or 0.0), 4) for s in subaccounts], balance_usd,
            )
    except Exception as exc:
        account_ok = False
        logger.warning("[app_kalshi] balance check failed during account snapshot refresh: %s", exc)

    snapshot = {
        "ok": account_ok, "margin_enabled": margin_enabled,
        "exchange_active": exchange_active, "available_balance_usd": balance_usd,
    }
    with _ACCOUNT_SNAPSHOT_CACHE_LOCK:
        _ACCOUNT_SNAPSHOT_CACHE = dict(snapshot)
        _ACCOUNT_SNAPSHOT_CACHE_TS = time.monotonic()
    return snapshot


def _cached_account_snapshot() -> dict[str, Any]:
    """Read-only from the HTTP route's point of view -- never makes a
    network call itself (see _refresh_account_snapshot_cache's docstring
    for why). Falls back to one inline fetch only if nothing has ever been
    cached yet (cold start, before fast_check's first tick)."""
    with _ACCOUNT_SNAPSHOT_CACHE_LOCK:
        if _ACCOUNT_SNAPSHOT_CACHE:
            return dict(_ACCOUNT_SNAPSHOT_CACHE)
    return _refresh_account_snapshot_cache()


JOB_HISTORY_FILE = DATA_DIR / "perps_job_run_history.json"
LATEST_CYCLE_FILE = DATA_DIR / "perps_latest_cycle.json"
LATEST_POSITION_CHECK_FILE = DATA_DIR / "perps_latest_position_check.json"
MILESTONES_FILE = DATA_DIR / "perps_milestones.json"
JOB_LOCK_DIR = DATA_DIR / "perps_locks"


# ---------------------------------------------------------------------------
# Background jobs
# ---------------------------------------------------------------------------
@_locked_job("perps_fast_check", stale_after_sec=60)
def _run_perps_fast_check() -> dict[str, Any]:
    # dry_run=False here does NOT itself enable live orders --
    # perps_strategy's hard safety floor forces dry-run regardless of this
    # value unless KALSHI_PERPS_LIVE_TRADING_ENABLED=1 is ALSO set. Passing
    # False here just means that when that env var IS set, this actual
    # production loop honors it instead of silently staying dry-run forever
    # (which a caller-side default of None/True would otherwise do).
    result = perps_strategy.manage_open_positions(dry_run=False)
    if result.get("action") != "no_position":
        save_json(LATEST_POSITION_CHECK_FILE, result)
    # Refreshes the dashboard's account-balance cache from THIS background
    # thread instead of the web request thread -- see
    # _refresh_account_snapshot_cache's docstring for the real incident this
    # prevents. TTL-gated here (same constant the cache always used) so it's
    # still just one real Kalshi call per window, not one per fast_check tick.
    now = time.monotonic()
    if (now - _ACCOUNT_SNAPSHOT_CACHE_TS) >= _ACCOUNT_SNAPSHOT_CACHE_TTL_SEC:
        try:
            snapshot = _refresh_account_snapshot_cache()
            if snapshot.get("ok"):
                milestones = perps_strategy.record_milestone(snapshot["available_balance_usd"])
                save_json(MILESTONES_FILE, milestones)
            else:
                # Real gap found in review (2026-08-23 dashboard investigation):
                # this branch silently skipped record_milestone with NO log at
                # all when snapshot["ok"] was False (margin_enabled or balance
                # check failed inside _do_refresh_account_snapshot) -- meaning
                # the milestone panel could go stale for an unknown number of
                # cycles with zero visibility into why. Logged at warning now.
                logger.warning("[app_kalshi] account snapshot not ok this cycle, milestone update skipped: %s", snapshot)
        except Exception as exc:
            logger.warning("[app_kalshi] background account snapshot refresh failed: %s", exc)
    return result


@_locked_job("perps_entry_scan", stale_after_sec=300)
def _run_perps_entry_scan() -> dict[str, Any]:
    """Real, confirmed OOM events on this exact service this session
    (Render's own events, oomKilled=true) -- gc.collect() here matches the
    same defense already proven necessary on the equities entry-scan job
    (which had none at all and was OOM-crashing every 15-20 minutes)."""
    try:
        result = perps_strategy.scan_and_enter(dry_run=False)  # see _run_perps_fast_check
        save_json(LATEST_CYCLE_FILE, result)
        return result
    finally:
        gc.collect()


@_locked_job("perps_manual_cycle", stale_after_sec=300)
def _run_perps_manual_cycle() -> dict[str, Any]:
    """Manual/legacy full cycle (fast check + entry scan in one call) for
    the manual tick endpoint and scripts/run_perps_cycle.py -- production
    scheduling always uses the split fast/slow jobs above instead."""
    result = perps_strategy.run_cycle(dry_run=False)  # see _run_perps_fast_check
    save_json(LATEST_POSITION_CHECK_FILE, result.get("position_management") or {})
    save_json(LATEST_CYCLE_FILE, result.get("entry_scan") or {})
    return result


@_locked_job("perps_data_collect", stale_after_sec=600)
def _run_perps_data_collect() -> dict[str, Any]:
    # Real gap found in review: every equivalent data_collect job on the
    # other 3 services (alpaca stocks/crypto/options) wraps its body in a
    # `finally: gc.collect()` -- this one never did, despite
    # collect_dataset_rows()/push_dataset_snapshot() being the same
    # confirmed-real OOM shape (an oomKilled restart landed mid-upload
    # inside push_dataset_snapshot again during a live monitoring pass,
    # after that function's own prior two rounds of fixes -- its own
    # docstring already calls this "a much rarer, residual risk", so this
    # is an additional margin, not a claim of a full fix).
    #
    # Off the request path on purpose -- see refresh_ticker_activity_cache's
    # own docstring for why this must never be triggered from /api/status.
    try:
        perps_data.refresh_ticker_activity_cache()
    except Exception:
        logger.exception("[app_kalshi] ticker activity cache refresh failed")
    try:
        df = perps_data.collect_dataset_rows()
        if df.empty:
            return {"ok": False, "reason": "no_rows_collected"}
        result = perps_data.push_dataset_snapshot(df)
        # Best-effort, non-fatal -- see crypto_correlation.py's own module
        # docstring for the full cross-market design. refresh_perps_study
        # builds perps' own within-market study straight from `df` (no
        # extra network call); pull_alpaca_crypto_correlation_study is the
        # ONE small HF download that brings in Alpaca crypto's own,
        # broader-universe study (pushed by alpaca_crypto_server.py's own
        # data-collect job). Both cached in-process so the fast/entry-scan
        # loops (perps_strategy.py) only ever do an in-memory lookup.
        try:
            from data import crypto_correlation
            crypto_correlation.refresh_perps_study(
                df, id_col="ticker", leader_id="BTC", coin_of=perps_data.coin_for_ticker,
            )
        except Exception as exc:
            logger.warning("[app_kalshi] perps correlation study refresh failed: %s", exc)
        try:
            perps_data.pull_alpaca_crypto_correlation_study()
        except Exception as exc:
            logger.warning("[app_kalshi] alpaca crypto correlation study pull failed: %s", exc)
        return result
    finally:
        gc.collect()


@_locked_job("perps_train", stale_after_sec=1800)
def _run_perps_train() -> dict[str, Any]:
    # perps_model.py never imports perps_strategy.py directly (perps_strategy
    # already imports FROM perps_model -- predict_direction et al -- so the
    # reverse import would be circular); the trade_log this feeds into
    # outcome-aware sample weighting is read here instead, where both
    # modules are already available.
    try:
        trade_log = perps_strategy._load_state().get("trade_log")  # noqa: SLF001
    except Exception as exc:
        logger.warning("[app_kalshi] could not read trade_log for outcome-aware training: %s", exc)
        trade_log = None
    return perps_model.train_model(trade_log=trade_log)


@_locked_job("perps_trade_analysis", stale_after_sec=1800)
def _run_perps_trade_analysis() -> dict[str, Any]:
    """Daily: turns trade_log's own real history into win/loss diagnostics
    (perps_trade_analysis.analyze_trade_history), posts a summary to
    Threads, and applies two independent, small, bounded, evidence-gated
    tunes -- ONLY when their own gate clearly supports it (see each
    recommend function's own docstring): the confidence floor
    (recommend_confidence_threshold) and, "remembering" whether the
    chart-study layer itself is worth trusting
    (recommend_correlation_study_weight). Read-only over state except for
    those two narrow, evidence-gated writes; never touches order placement
    or position management directly."""
    try:
        state = perps_strategy._load_state()  # noqa: SLF001
        trade_log = state.get("trade_log") or []
        analysis = perps_trade_analysis.analyze_trade_history(trade_log)
        tuning_state = state.get("tuning") or {}
        current_threshold = tuning_state.get("model_confidence_min", perps_strategy.MODEL_CONFIDENCE_MIN)
        recommendation = perps_trade_analysis.recommend_confidence_threshold(trade_log, current_threshold=current_threshold)

        tuning_applied = None
        if recommendation.get("should_apply"):
            tuning_applied = perps_strategy.apply_confidence_threshold_override(
                recommendation["recommended_threshold"],
                reason=(
                    f"evidence-gated: {recommendation['candidate']['trades']} real trades at "
                    f"{recommendation['recommended_threshold']:.2f}+ confidence outperformed the "
                    f"{recommendation['baseline']['trades']}-trade baseline at {current_threshold:.2f}"
                ),
            )
            logger.info("[app_kalshi] perps confidence threshold tuned: %s", tuning_applied)

        # Same evidence-gated discipline, for whether the chart-study layer
        # (crypto_correlation.py) itself is worth trusting -- see
        # recommend_correlation_study_weight's own docstring for the 3
        # possible outcomes (enable/increase_weight/disable).
        current_correlation_enabled = tuning_state.get("correlation_study_enabled", perps_strategy.USE_CORRELATION_STUDY)
        current_correlation_max_adjustment = tuning_state.get(
            "correlation_confidence_max_adjustment", perps_strategy.CORRELATION_CONFIDENCE_MAX_ADJUSTMENT,
        )
        correlation_recommendation = perps_trade_analysis.recommend_correlation_study_weight(
            trade_log, current_enabled=current_correlation_enabled, current_max_adjustment=current_correlation_max_adjustment,
        )
        correlation_tuning_applied = None
        if correlation_recommendation.get("should_apply"):
            correlation_tuning_applied = perps_strategy.apply_correlation_study_override(
                enabled=correlation_recommendation.get("recommended_enabled"),
                max_adjustment=correlation_recommendation.get("recommended_max_adjustment"),
                reason=(
                    f"evidence-gated: {correlation_recommendation['action']} -- "
                    f"{correlation_recommendation['agreed']['trades']}-trade agreed bucket "
                    f"(win rate {correlation_recommendation['agreed']['win_rate']:.0%}) vs "
                    f"{correlation_recommendation['baseline']['trades']}-trade baseline "
                    f"(win rate {correlation_recommendation['baseline']['win_rate']:.0%})"
                ),
            )
            logger.info("[app_kalshi] perps correlation study tuned: %s", correlation_tuning_applied)

        posted = False
        if analysis.get("trades_analyzed"):
            summary_text = perps_trade_analysis.format_analysis_summary_text(analysis, tuning=recommendation)
            try:
                posted = threads_post.post_trade_analysis_summary(summary_text)
            except Exception:
                logger.warning("[app_kalshi] Threads trade-analysis post failed", exc_info=True)

        return {
            "ok": True, "analysis": analysis, "tuning_recommendation": recommendation, "tuning_applied": tuning_applied,
            "correlation_tuning_recommendation": correlation_recommendation, "correlation_tuning_applied": correlation_tuning_applied,
            "posted": posted,
        }
    except Exception as exc:
        logger.exception("[app_kalshi] perps trade analysis failed")
        return {"ok": False, "error": str(exc)}


@_locked_job("perps_threads_hourly_status", stale_after_sec=300)
def _run_perps_threads_hourly_status() -> dict[str, Any]:
    """Posts a status update every hour regardless of whether a trade
    happened -- what position(s) the bot is currently holding (or that
    it's flat) plus today's realized P&L. Best-effort, on top of (not
    instead of) the real-time trade-entry/restart posts -- never allowed
    to affect trading logic, which is why this reads state read-only and
    never touches order placement."""
    try:
        state = perps_strategy._load_state()  # noqa: SLF001
        now = dt.datetime.now(dt.timezone.utc)
        positions = []
        for p in (state.get("positions") or []):
            levels = perps_strategy.position_exit_levels(p)
            opened_at = dt.datetime.fromisoformat(p["opened_at"])
            held_minutes = (now - opened_at).total_seconds() / 60.0
            positions.append({**p, **levels, "held_minutes": held_minutes})
        realized_pnl_by_date = state.get("realized_pnl_by_date") or {}
        today_pnl = float(realized_pnl_by_date.get(et_today().isoformat(), 0.0))
        posted = threads_post.post_hourly_status(positions=positions, today_realized_pnl_usd=today_pnl)
        return {"ok": True, "posted": posted, "open_position_count": len(positions)}
    except Exception as exc:
        logger.warning("[app_kalshi] Threads hourly status post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


@_locked_job("perps_threads_trending_news", stale_after_sec=300)
def _run_perps_threads_trending_news() -> dict[str, Any]:
    """Real, confirmed duplication bug: this job and Alpaca crypto's own
    trending-news job both independently fetched crypto_news.get_trending_story()
    and posted it -- but ALL 4 services share ONE real Threads account
    (see threads_client.py's own HF_MODEL_REPO, the same durable-state
    file every service reads its token from), so the SAME headline/photo
    went out twice, ~30 minutes apart, under two different labels. That
    reads as spam to a real follower, the opposite of the growth this
    post exists for. Alpaca crypto (src/alpaca_crypto_server.py) now
    solely owns the crypto trending-news beat -- this job intentionally
    no-ops rather than re-posting the same story a second time. Kept
    scheduled (not removed) so it stays visible in the job-activity log
    and is trivial to re-enable if the two are ever meant to diverge."""
    return {"ok": True, "posted": False, "action": "skipped_duplicate_beat", "owner": "alpaca_crypto"}


@_locked_job("perps_threads_sentiment_snapshot", stale_after_sec=600)
def _run_perps_threads_sentiment_snapshot() -> dict[str, Any]:
    """Posts a per-ticker sentiment bar-chart every 60 minutes -- every
    ticker on the current watchlist, each with its OWN real news
    sentiment (crypto_news.get_sentiment(coin), the same call
    perps_data.py's own collect/latest_feature_row already make, so this
    is a cache hit the large majority of the time, not new network load).
    Genuinely different from the trending-news post above: that surfaces
    headlines; this surfaces the actual per-ticker SCORES as a picture.
    Read-only, never touches order placement."""
    try:
        tickers = perps_data.get_watchlist()
        ticker_sentiments = []
        for ticker in tickers:
            try:
                sentiment = crypto_news.get_sentiment(perps_data.coin_for_ticker(ticker))
                ticker_sentiments.append({"ticker": ticker, "sentiment_score": sentiment["sentiment_score"]})
            except Exception as exc:
                logger.debug("[app_kalshi] sentiment fetch failed for %s: %s", ticker, exc)
        posted = threads_post.post_sentiment_snapshot(market="perps", ticker_sentiments=ticker_sentiments)
        return {"ok": True, "posted": posted, "ticker_count": len(ticker_sentiments)}
    except Exception as exc:
        logger.warning("[app_kalshi] Threads sentiment-snapshot post failed: %s", exc)
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
        if not scheduler.running:
            # next_run_time delayed a full interval: without this, APScheduler
            # fires an interval job's FIRST run immediately on scheduler.start()
            # -- meaning every restart ran this (the heaviest thing besides
            # training: live candle+news fetch across every active ticker,
            # feature engineering, an HF upload) TWICE back to back, once here
            # and once via _runner()'s own direct startup call below. Confirmed
            # live: instance restarts happening every ~2 minutes, right around
            # an HF upload -- this redundant doubling made every boot heavier
            # than it needed to be at exactly the moment memory is tightest.
            scheduler.add_job(
                _run_perps_data_collect, "interval", minutes=PERPS_DATA_COLLECT_MINUTES,
                id="perps_data_collect", replace_existing=True,
                next_run_time=dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=PERPS_DATA_COLLECT_MINUTES),
            )
            scheduler.add_job(
                _run_perps_train, "cron", hour=PERPS_TRAIN_HOUR_ET, minute=0,
                id="perps_train", replace_existing=True,
            )
            scheduler.add_job(
                _run_perps_trade_analysis, "cron", hour=PERPS_TRADE_ANALYSIS_HOUR_ET, minute=PERPS_TRADE_ANALYSIS_MINUTE_ET,
                id="perps_trade_analysis", replace_existing=True,
            )
            # executor="fastcheck" on these (and entry_scan/fast_check below)
            # -- real, confirmed production incident: with only fast_check
            # isolated from "default", a slow train/data_collect run (made
            # meaningfully longer by the recent MAX_TRAIN_ROWS increase) was
            # still able to block entry_scan and every Threads post for
            # 7-18+ minutes at a stretch on the equivalent Alpaca services --
            # long enough to blow past even the widened 300s misfire grace
            # and get silently skipped anyway. All of these are themselves
            # fast, bounded operations (seconds, not minutes) sharing this
            # pool safely with fast_check the same way -- moving them here
            # protects them from the SLOW jobs still on "default" without
            # meaningfully risking fast_check's own real-money-critical
            # cadence.
            # Real, confirmed live: with no stagger, hourly_status (1h) and
            # sentiment_snapshot (60min) always land on the EXACT same tick
            # (both added back-to-back with no offset), and trending_news
            # (30min) coincides with them every other cycle too -- data_collect
            # (15min) piles on as well since 60 is a multiple of 15. That
            # cluster running back-to-back took ~20s, long enough to bump
            # into fast_check's own 20s cadence and cause a real, if minor
            # (one cycle, immediately recovered), skip once an hour.
            # Staggering each job's first run spreads them across the hour
            # instead of clustering at :00 -- same next_run_time technique
            # data_collect's own comment above already documents.
            now_utc = dt.datetime.now(dt.timezone.utc)
            scheduler.add_job(
                _run_perps_threads_hourly_status, "interval", hours=1,
                id="perps_threads_hourly_status", replace_existing=True, executor="fastcheck",
            )
            scheduler.add_job(
                _run_perps_threads_trending_news, "interval", minutes=30,
                id="perps_threads_trending_news", replace_existing=True, executor="fastcheck",
                next_run_time=now_utc + dt.timedelta(minutes=5),
            )
            scheduler.add_job(
                _run_perps_threads_sentiment_snapshot, "interval", minutes=60,
                id="perps_threads_sentiment_snapshot", replace_existing=True, executor="fastcheck",
                next_run_time=now_utc + dt.timedelta(minutes=10),
            )
            if ENABLE_PERPS_SCHEDULER:
                scheduler.add_job(
                    _run_perps_fast_check, "interval", seconds=PERPS_FAST_CHECK_SECONDS,
                    id="perps_fast_check", replace_existing=True, executor="fastcheck",
                )
                scheduler.add_job(
                    _run_perps_entry_scan, "interval", minutes=PERPS_CYCLE_MINUTES,
                    id="perps_entry_scan", replace_existing=True, executor="fastcheck",
                    next_run_time=dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=PERPS_STARTUP_GRACE_SECONDS),
                )
            scheduler.start()
            logger.info(
                "Perps scheduler started: fast exit check every %ds, entry scan every %d min (%s, first run in %ds), "
                "data collect every %d min, train daily at %02d:00 ET, Threads hourly status post every 1h, live_trading=%s",
                PERPS_FAST_CHECK_SECONDS, PERPS_CYCLE_MINUTES, "ENABLED" if ENABLE_PERPS_SCHEDULER else "disabled",
                PERPS_STARTUP_GRACE_SECONDS, PERPS_DATA_COLLECT_MINUTES, PERPS_TRAIN_HOUR_ET, perps_strategy.LIVE_TRADING_ENABLED,
            )

        def _runner() -> None:
            try:
                threads_post.post_restart_notice()
            except Exception:
                logger.warning("Startup Threads restart notice failed", exc_info=True)
            try:
                # Real gap found in review: manage_open_positions() already
                # reconciles against the real Kalshi account every fast-check
                # cycle (20s), which normally catches a restart-time gap
                # quickly enough -- but there's no EXPLICIT, immediately-
                # observable check confirming an order placed right before a
                # crash (state not yet saved) actually got picked back up.
                # An explicit startup pass makes this immediate (not a ~20s
                # implicit wait) and gives a clear, loggable confirmation
                # rather than relying on it happening silently as a side
                # effect of the next scheduled tick.
                if perps_strategy.LIVE_TRADING_ENABLED:
                    with perps_strategy._STATE_LOCK:  # noqa: SLF001
                        state = perps_strategy._load_state()  # noqa: SLF001
                        state["positions"] = perps_strategy._reconcile_positions_with_exchange(state)  # noqa: SLF001
                        perps_strategy._save_state(state)  # noqa: SLF001
                    logger.info("Startup reconciliation: %d real open position(s) confirmed against Kalshi", len(state["positions"]))
            except Exception as exc:
                logger.warning("Startup reconciliation failed: %s", exc)
            try:
                _run_perps_data_collect()
                logger.info("Startup data collect completed")
            except Exception as exc:
                logger.warning("Startup data collect failed: %s", exc)
            # Full training (load the whole capped dataset + fit 3 candidate
            # models) is the heaviest thing this process does. Only run it at
            # boot on a genuine cold start (no model cached locally or on HF
            # yet) -- otherwise every restart (including one caused BY an OOM)
            # would immediately retrigger the heaviest operation again,
            # turning a single OOM into a self-sustaining crash loop. The
            # daily cron job still retrains on schedule regardless.
            try:
                if perps_model.load_model()[0] is None:
                    train_result = _run_perps_train()
                    logger.info("Startup train attempt (cold start): %s", train_result.get("reason", "ok"))
                else:
                    logger.info("Startup train skipped: model already cached, daily cron will retrain")
            except Exception as exc:
                logger.warning("Startup train failed: %s", exc)
            # No immediate startup entry scan here (deliberately removed) --
            # confirmed live on this account: a fresh instance calling this
            # the instant it boots, during Render's rolling-deploy overlap
            # window, meant the OLD and NEW instance could each place a REAL
            # duplicate entry order for the same signal within seconds of
            # each other. The scheduled perps_entry_scan job (see
            # PERPS_STARTUP_GRACE_SECONDS above) already covers this on its
            # own delayed first tick -- this redundant immediate call only
            # ever made the collision window worse, never faster in any way
            # that mattered (2 minutes vs waiting for the grace period).

        threading.Thread(target=_runner, daemon=True, name="perps-server-startup-autorun").start()
        _startup_done = True


@app.before_request
def _bootstrap_background_jobs() -> None:
    _ensure_background_jobs_started()


@app.before_request
def _enforce_rate_limit():
    # Only the public read surface needs this -- an already-authorized
    # (CRON_SECRET-bearing) caller is the account owner's own automation,
    # never rate-limited. See check_rate_limit's own docstring for why
    # this exists (protecting the single gunicorn worker from a scraped/
    # hammered shared link) and why it's not a security boundary.
    if request.path.startswith("/api/") and not is_cron_authorized(request):
        client_ip = request.remote_addr or "unknown"
        if not check_rate_limit(client_ip):
            return jsonify({"ok": False, "error": "rate_limited"}), 429
    return None


@app.after_request
def _set_security_headers(response):
    # This dashboard is meant to be safely shareable as a read-only public
    # link -- every route that can actually CHANGE anything already requires
    # is_cron_authorized (a bearer-token secret, see CRON_SECRET), so these
    # are defense-in-depth for the read-only surface, not the primary
    # control: block this response from being framed by another site
    # (clickjacking), stop browsers from MIME-sniffing a response into
    # something more dangerous than its declared Content-Type, and avoid
    # leaking this dashboard's own URL (which could contain no secrets, but
    # there's no reason to send it) as a Referer header when a visitor
    # clicks an outbound link.
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    return response


# Real bug found and fixed here: under gunicorn (`gunicorn app:app`, this
# process's ACTUAL production entrypoint), nothing calls `if __name__ ==
# "__main__"` -- that block only runs when this file is executed directly
# (`python app_kalshi.py`), never on a plain import. That left
# _ensure_background_jobs_started() reachable ONLY via the before_request
# hook above, meaning the scheduler (and therefore all live trading/data
# collection) would not start until the very first real HTTP request
# arrived -- confirmed live: Render's own deploy-readiness check is a raw
# port probe, not a Flask request, so after any restart the bot sat
# completely idle until a human (or a monitoring script) happened to load
# the dashboard or hit an API route. Calling it here, at module import
# time, means the worker process starts trading/collecting the instant it
# boots, with no dependency on anyone visiting the dashboard. The
# before_request hook above is kept as a harmless no-op safety net (it
# short-circuits immediately once _startup_done is set).
_ensure_background_jobs_started()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route("/")
def hub():
    """A link hub, not the perps dashboard itself -- this domain is the
    first thing anyone hits, and now that the Alpaca stocks and Alpaca
    crypto bots are each their own separate server on their own domain,
    "/" needs to point to all of them rather than silently assuming perps.
    See hub.html; /perps below is the actual dashboard."""
    return render_template(
        "hub.html", alpaca_url=ALPACA_SERVER_URL, alpaca_crypto_url=ALPACA_CRYPTO_SERVER_URL,
        alpaca_options_url=ALPACA_OPTIONS_SERVER_URL,
    )


@app.route("/perps")
def index():
    return render_template(
        "dashboard.html", alpaca_url=ALPACA_SERVER_URL, alpaca_crypto_url=ALPACA_CRYPTO_SERVER_URL,
        alpaca_options_url=ALPACA_OPTIONS_SERVER_URL,
    )


@app.route("/alpaca")
def alpaca_redirect():
    """This domain hasn't served /alpaca directly since the split into
    separate servers -- confirmed live this used to just 404 with no
    explanation. Redirect to the real Alpaca service if it's been
    deployed and ALPACA_SERVER_URL is configured; otherwise send back to
    the hub, which explains that it isn't deployed yet instead of a bare
    dead link."""
    if ALPACA_SERVER_URL and ALPACA_SERVER_URL != "#":
        return redirect(f"{ALPACA_SERVER_URL.rstrip('/')}/alpaca")
    return redirect("/")


@app.route("/alpaca-crypto")
def alpaca_crypto_redirect():
    """Same convenience redirect as /alpaca above, for the separate
    Alpaca crypto service."""
    if ALPACA_CRYPTO_SERVER_URL and ALPACA_CRYPTO_SERVER_URL != "#":
        return redirect(f"{ALPACA_CRYPTO_SERVER_URL.rstrip('/')}/alpaca-crypto")
    return redirect("/")


@app.route("/alpaca-options")
def alpaca_options_redirect():
    """Same convenience redirect as /alpaca above, for the separate
    Alpaca options service."""
    if ALPACA_OPTIONS_SERVER_URL and ALPACA_OPTIONS_SERVER_URL != "#":
        return redirect(f"{ALPACA_OPTIONS_SERVER_URL.rstrip('/')}/alpaca-options")
    return redirect("/")


@app.route("/pr")
def privacy_policy():
    """Meta requires a Privacy Policy URL on file for any app requesting
    Threads API scopes -- this is that page, registered in the Threads app
    settings as this service's privacy policy URL."""
    return render_template("privacy_policy.html", updated_at=dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d"))


@app.route("/chart/<path:filename>")
def chart_snapshot_image(filename):
    """Serves a chart-snapshot PNG (see chart_snapshot.py) publicly --
    Threads' media-container API fetches the image itself from a URL it's
    given, there is no raw-upload step, so this route is what makes
    threads_post.maybe_post_trade_entry_chart() actually work.
    send_from_directory guards against path traversal on its own."""
    from data import chart_snapshot
    return send_from_directory(chart_snapshot.CHARTS_DIR, filename)


@app.route("/threads/authorize")
def threads_authorize():
    """Convenience redirect to Threads' own login page -- the actual
    authorization step is a real interactive Threads/Instagram login that
    nothing here can do on the account owner's behalf."""
    return redirect(threads_client.get_authorization_url())


@app.route("/threadscallback")
def threads_callback():
    """Registered as this Threads app's OAuth redirect_uri -- MUST exactly
    match THREADS_REDIRECT_URI and the callback URL registered on Meta's
    developer portal for this app."""
    error = request.args.get("error")
    if error:
        return jsonify({"ok": False, "error": error}), 400
    code = request.args.get("code")
    if not code:
        return jsonify({"ok": False, "error": "missing_code"}), 400
    try:
        threads_client.exchange_code_for_tokens(code)
    except Exception:
        # Public route (Meta's own OAuth redirect target) -- same reasoning
        # as /api/positions above, keep the real exception in logs only.
        logger.exception("[app_kalshi] threads token exchange failed")
        return jsonify({"ok": False, "error": "token_exchange_failed"}), 500
    return jsonify({"ok": True, "message": "Threads account linked. You can close this tab."})


@app.route("/api/perps/report.pdf")
def api_perps_report_pdf():
    """Downloadable PDF account report -- "what has the live account
    actually made" as a document, not just a dashboard someone has to
    screenshot. Pulls from the exact same durable state (trade_log,
    realized_pnl_by_date) and real account balance /api/status itself
    uses, so the numbers always match. Unlike the Threads-posting
    modules, this is NOT best-effort by design: a user who just clicked
    "download" should see a real error if generation fails, not silent
    nothing."""
    from data import perps_report

    state = perps_strategy._load_state()  # noqa: SLF001
    account = _cached_account_snapshot()
    pdf_bytes = perps_report.generate_pdf_report(
        state=state, account_balance_usd=account.get("available_balance_usd"),
        live_trading_enabled=perps_strategy.LIVE_TRADING_ENABLED,
    )
    filename = f"kalshi-perps-report-{dt.datetime.now(dt.timezone.utc).date().isoformat()}.pdf"
    return Response(
        pdf_bytes, mimetype="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.route("/api/status")
def api_status():
    state = perps_strategy._load_state()  # noqa: SLF001
    _, meta = perps_model.load_model()
    latest_cycle = load_json(LATEST_CYCLE_FILE, {})
    latest_position_check = load_json(LATEST_POSITION_CHECK_FILE, {})
    account = _cached_account_snapshot()

    realized_pnl_by_date = state.get("realized_pnl_by_date") or {}
    total_realized_pnl = round(sum(float(v) for v in realized_pnl_by_date.values()), 6)
    # Real gap found in review: the dashboard showed entry price + static
    # TP/SL levels for every open position but never its CURRENT price or
    # unrealized P&L -- the single highest-value "what is the bot actually
    # doing right now" fact was missing. manage_open_positions() already
    # fetches a fresh current_price per position every fast_check cycle
    # (to decide whether to exit) and now records it on each check
    # (perps_strategy.py) -- reused here via a ticker lookup instead of a
    # second, redundant fetch. Also surfaces the real exit_check reason
    # text (WHY a position is/isn't closing yet), previously computed but
    # never rendered anywhere.
    checks_by_ticker = {c["ticker"]: c for c in latest_position_check.get("checks") or [] if c.get("ticker")}
    positions = []
    for p in state.get("positions") or []:
        enriched = {**p, **perps_strategy.position_exit_levels(p)}
        check = checks_by_ticker.get(p.get("ticker"))
        if check and check.get("current_price") is not None:
            current_price = float(check["current_price"])
            entry_price = float(p.get("entry_price") or 0.0)
            count = float(p.get("count") or 0.0)
            is_short = p.get("side") == "short"
            signed_change = (entry_price - current_price) if is_short else (current_price - entry_price)
            enriched["current_price"] = current_price
            enriched["unrealized_pnl_usd"] = round(signed_change * count, 6)
            enriched["unrealized_pnl_pct"] = round(signed_change / entry_price, 6) if entry_price > 0 else None
            enriched["exit_check"] = check.get("exit_check")
        positions.append(enriched)

    return jsonify({
        "ok": True,
        "now": dt.datetime.now(dt.timezone.utc).isoformat(),
        "live_trading_enabled": perps_strategy.LIVE_TRADING_ENABLED,
        "account": account,
        "positions": positions,
        "open_position_count": len(positions),
        "max_concurrent_positions": perps_strategy.MAX_CONCURRENT_POSITIONS,
        "today_realized_pnl_usd": float(realized_pnl_by_date.get(et_today().isoformat(), 0.0)),
        "total_realized_pnl_usd": total_realized_pnl,
        "trade_count": len(state.get("trade_log") or []),
        "win_rate": win_rate_stats(state.get("trade_log") or []),
        "milestones": load_json(MILESTONES_FILE, {}),
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
        "watchlist": perps_data.get_watchlist(),
        "params": {
            "position_size_pct": perps_strategy.POSITION_SIZE_PCT,
            "max_concurrent_positions": perps_strategy.MAX_CONCURRENT_POSITIONS,
            "take_profit_pct": perps_strategy.TAKE_PROFIT_PCT,
            "stop_loss_pct": perps_strategy.STOP_LOSS_PCT,
            "quick_profit_pct": perps_strategy.QUICK_PROFIT_PCT,
            "quick_profit_velocity_pct_per_min": perps_strategy.QUICK_PROFIT_VELOCITY_PCT_PER_MIN,
            "high_volatility_threshold": perps_strategy.HIGH_VOLATILITY_THRESHOLD,
            "volatility_quick_profit_pct": perps_strategy.VOLATILITY_QUICK_PROFIT_PCT,
            "max_hold_minutes": perps_strategy.MAX_HOLD_MINUTES,
            "daily_loss_cap_pct": perps_strategy.DAILY_LOSS_CAP_PCT,
            "model_confidence_min": perps_strategy.MODEL_CONFIDENCE_MIN,
            "shorts_enabled": perps_strategy.ENABLE_SHORTS,
            "maker_orders_enabled": perps_strategy.ENABLE_MAKER_ORDERS,
            "threads_post_configured": threads_post.is_configured(),
            "fast_check_seconds": PERPS_FAST_CHECK_SECONDS,
            "entry_scan_minutes": PERPS_CYCLE_MINUTES,
            "data_collect_minutes": PERPS_DATA_COLLECT_MINUTES,
            "train_hour_et": PERPS_TRAIN_HOUR_ET,
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
    except Exception:
        # Public, unauthenticated route -- the raw exception text (which can
        # include internal file paths, library/argument names, or upstream
        # URLs) stays in the server's own logs only, never echoed back to an
        # anonymous visitor of a shared dashboard link.
        logger.warning("[app_kalshi] /api/positions failed", exc_info=True)
        return jsonify({"ok": False, "error": "positions_unavailable", "positions": []})


@app.route("/api/perps/tick", methods=["GET", "POST"])
def api_perps_tick():
    """Manually force an immediate full cycle (fast exit check, then entry
    scan if nothing was open) instead of waiting for the next scheduled
    interval."""
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        result = _run_perps_manual_cycle()
        return jsonify(result)
    except Exception as exc:
        logger.exception("[app_kalshi] manual perps tick failed: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/perps/fast-check", methods=["GET", "POST"])
def api_perps_fast_check():
    """Manually force an immediate position exit check only (what the fast
    loop does every PERPS_FAST_CHECK_SECONDS)."""
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_perps_fast_check())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/perps/collect", methods=["GET", "POST"])
def api_perps_collect():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_perps_data_collect())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/perps/train", methods=["GET", "POST"])
def api_perps_train():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_perps_train())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/perps/trade_analysis", methods=["GET", "POST"])
def api_perps_trade_analysis():
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_perps_trade_analysis())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


_JOB_LABELS = {
    "perps_fast_check": f"Fast exit check (every {PERPS_FAST_CHECK_SECONDS}s)",
    "perps_entry_scan": f"Entry scan -- all instruments (every {PERPS_CYCLE_MINUTES} min)",
    "perps_manual_cycle": "Manual full cycle",
    "perps_data_collect": f"Data collection -> HF (every {PERPS_DATA_COLLECT_MINUTES} min)",
    "perps_train": f"Model retrain (daily {PERPS_TRAIN_HOUR_ET:02d}:00 ET)",
    "perps_trade_analysis": (
        f"Trade win/loss analysis + evidence-gated confidence tuning "
        f"(daily {PERPS_TRADE_ANALYSIS_HOUR_ET:02d}:{PERPS_TRADE_ANALYSIS_MINUTE_ET:02d} ET)"
    ),
    "perps_threads_hourly_status": "Threads hourly status post",
    "perps_threads_trending_news": "Threads trending-news post (every 30 min)",
    "perps_threads_sentiment_snapshot": "Threads per-ticker sentiment snapshot (every 60 min)",
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
        "scheduler_enabled": ENABLE_PERPS_SCHEDULER,
        "running_now": running_now,
        "last_by_job": last_by_job,
        "recent": recent,
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000") or "5000")
    _ensure_background_jobs_started()
    app.run(host="0.0.0.0", port=port, debug=False)
