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
  - kalshi_15m_data_collect every KALSHI_15M_DATA_COLLECT_MINUTES --
                                                            archive fresh candles
                                                            (relabeled to a 15-
                                                            minute horizon) for
                                                            Kalshi's OWN separate
                                                            15-minute event-contract
                                                            markets (KXBTC15M etc,
                                                            see kalshi_15m.py) to HF.
                                                            First phase of a new
                                                            market build -- model/
                                                            strategy/order-execution
                                                            still to come; this job
                                                            exists to start
                                                            accumulating real
                                                            training data immediately
                                                            rather than block on the
                                                            rest of the build.
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
import uuid
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
from data import (
    ai_monitor, crypto_news, kalshi_15m, kalshi_15m_backtest, kalshi_15m_data, kalshi_15m_meta_model,
    kalshi_15m_metals_backtest, kalshi_15m_metals_data, kalshi_15m_metals_model, kalshi_15m_model,
    kalshi_15m_strategy, perps_data, perps_meta_model, perps_model, perps_strategy, perps_trade_analysis,
    strategy_sweep, threads_client, threads_post,
)

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
# Kalshi's own 15-minute event-contract markets (KXBTC15M etc.) -- a
# genuinely new, separate product from perps (see kalshi_15m.py's own
# module docstring), first priority is just getting real data flowing:
# per explicit user direction ("scope and start building now"), this data
# collection job ships first, ahead of the model/strategy/order-execution
# layers still to come, since every day of delay here is a day of lost
# training data for a market with zero archived history yet.
KALSHI_15M_DATA_COLLECT_MINUTES = max(5, int(os.getenv("KALSHI_15M_DATA_COLLECT_MINUTES", "5") or "5"))
# GOLD/SILVER/COPPER (kalshi_15m_metals_data.py) -- a genuinely different
# collection cadence from crypto's above: this market has NO perps-
# contract feed to reuse (crypto's own proxy), so it builds its own
# price history from scratch, one point per cycle -- every 1 minute
# (the tightest floor this whole codebase's *_DATA_COLLECT_MINUTES
# constants use anywhere) to reach the 245-row minimum feature window as
# fast as real-world data allows (~4 hours), not artificially slower.
KALSHI_15M_METALS_DATA_COLLECT_MINUTES = max(1, int(os.getenv("KALSHI_15M_METALS_DATA_COLLECT_MINUTES", "1") or "1"))
# Combined entry-scan + settlement-check cycle -- unlike perps' own
# fast_check (a sub-minute stop-loss/take-profit reaction loop), nothing
# here is latency-critical: a position's only exit is its window settling
# at a fixed, known time, and a fresh window only opens every 15 minutes
# anyway. Every 2 minutes is ample to catch a freshly-opened window with
# still-plenty of MIN_SECONDS_TO_CLOSE_FOR_ENTRY left, and to book a
# settlement promptly after it resolves.
KALSHI_15M_CYCLE_MINUTES = max(1, int(os.getenv("KALSHI_15M_CYCLE_MINUTES", "2") or "2"))
# Off-hours-agnostic (crypto trades 24/7, unlike options) -- just a
# different hour than perps_train (3 ET) and stocks/crypto/options' own
# daily retrains, so this doesn't contend with any of them for CPU at the
# exact same minute.
KALSHI_15M_TRAIN_HOUR_ET = int(os.getenv("KALSHI_15M_TRAIN_HOUR_ET", "4") or "4")
# Custom PyTorch MLP challenger candidate (kalshi_15m_model.
# train_torch_candidate_model) -- own low-frequency daily schedule, kept
# separate from the walk-forward train job above for the same reason
# alpaca_options_model.py's own identical candidate is: `import torch`
# alone costs real memory (~154MB RSS, measured elsewhere in this
# codebase), so it only gets paid when this specific job actually runs,
# not on every walk-forward retrain. 1 hour after the regular train (not
# the same tick) so it always re-scores against that day's freshly
# walk-forward-trained model, not the previous day's -- see
# train_torch_candidate_model's own champion/challenger promotion logic
# for why that ordering matters (it only promotes if it BEATS the
# current model's fresh score). Metals excluded for now: its own
# archive hasn't even completed one regular walk-forward training run
# yet (see MIN_ROWS_FOR_FEATURES's own ~4h cold-start requirement) --
# a bigger model needs more data to justify its added capacity, not less.
# Default 7 (not the reflexive train+1): options' OWN identical torch
# candidate already defaults to 5 and ai_monitor to 6 -- picking a
# distinct hour avoids two heavy torch trainings landing on the exact
# same minute in this one shared process.
KALSHI_15M_TORCH_TRAIN_HOUR_ET = int(os.getenv("KALSHI_15M_TORCH_TRAIN_HOUR_ET", "7") or "7")
# Real gap this closes: the live collector only ever archives what it
# observes going forward (see kalshi_15m_data.backfill_minute_history's
# own docstring) -- any gap from a missed collection cycle (a restart,
# a transient Kalshi API failure) is a permanent hole in the training
# archive unless something re-heals it. Runs a SMALL trailing-window
# backfill (not the full historical depth -- that's the one-off manual
# /api/kalshi15m/backfill route's job) daily, 30 min before training so
# that day's model always sees a gap-healed archive. Runs in-process as
# a background job, not an HTTP request, so the gunicorn request-timeout
# concern that route's own docstring describes doesn't apply here --
# still kept small (a few days, not the full backfill's 90) since this
# runs EVERY day, not once.
KALSHI_15M_RECONCILE_DAYS = max(1, int(os.getenv("KALSHI_15M_RECONCILE_DAYS", "3") or "3"))
# Daily aggregate win/loss review over the WHOLE real trade history --
# see kalshi_15m_trade_analysis.analyze_trade_history's own module
# docstring for why this is additive to, not a replacement for,
# kalshi_15m_strategy's own existing 5-trade batch review + confidence
# auto-tuner (kalshi_15m_strategy._maybe_run_batch_trade_analysis).
# Mirrors alpaca_options_server.py's own identical, simpler-than-perps'
# daily job shape (read-only report, no separate tuning pass here -- the
# batch review above already owns that). 5:30am ET: after the regular
# train (4) and before the torch train (7)/ai_monitor (6), an open slot.
KALSHI_15M_TRADE_ANALYSIS_HOUR_ET = int(os.getenv("KALSHI_15M_TRADE_ANALYSIS_HOUR_ET", "5") or "5")
KALSHI_15M_TRADE_ANALYSIS_MINUTE_ET = int(os.getenv("KALSHI_15M_TRADE_ANALYSIS_MINUTE_ET", "30") or "30")
# Regularly-scheduled backtest + forward test -- per explicit user
# direction: "it need a backtest and a forward test to be regularly
# implemented." kalshi_15m_backtest.run_walkforward_backtest already
# existed (multiple expanding-window folds -- a real forward test, not
# just one lucky split) but had only ever been run manually. Crypto only
# (kalshi_15m_backtest.py, built on kalshi_15m_model/kalshi_15m_data) --
# kalshi_15m_metals_backtest.py (added later the same session GOLD/
# SILVER/COPPER became this account's own permanent, sole live-entry
# universe -- see kalshi_15m_strategy.ACTIVE_ENTRY_COINS) is the metals
# counterpart, wired into _run_kalshi_15m_strategy_sweep below rather
# than this job -- this one stays crypto-only so it keeps its own
# existing daily cadence/auto-retrain-on-loss behavior unchanged.
# 8am ET: after every other daily kalshi_15m job (train 4, trade-analysis
# 5:30, ai_monitor 6, torch-train 7) so this always backtests that day's
# freshest models.
KALSHI_15M_BACKTEST_HOUR_ET = int(os.getenv("KALSHI_15M_BACKTEST_HOUR_ET", "8") or "8")
KALSHI_15M_LATEST_BACKTEST_FILE = DATA_DIR / "kalshi_15m_latest_backtest.json"
# Large-scale metals strategy sweep -- per explicit user direction: "we
# need to work on over 10000 mix of strategies in the backtest and...
# perform a forward test with real data and a huge historical data of
# the main 3 we will trade... generate[] [strategies,] optimise the
# model to understand that." See strategy_sweep.py's own module
# docstring for the full design (why 10,000+ combinations is
# computationally realistic: no swept parameter affects model training,
# so each walk-forward fold fits its model exactly once and replays its
# own cached predictions through every combination). WEEKLY, not daily
# (unlike the crypto backtest above) -- a real, disclosed cost tradeoff:
# a 10,000+-combination sweep is a materially bigger compute job than
# the existing daily backtest's own single-parameter-set run, on the SAME
# shared, single-process container every live market trades on (see
# combined_app.py's own docstring) -- see strategy_sweep.run_parameter_sweep's
# own max_seconds safety valve for the hard runtime cap this job also
# relies on. Sunday, 9am ET: after the metals data-collect job has had a
# full week to accumulate more real archive, and clear of every OTHER
# kalshi_15m daily job's own schedule.
KALSHI_15M_STRATEGY_SWEEP_DAY_OF_WEEK = os.getenv("KALSHI_15M_STRATEGY_SWEEP_DAY_OF_WEEK", "sun")
KALSHI_15M_STRATEGY_SWEEP_HOUR_ET = int(os.getenv("KALSHI_15M_STRATEGY_SWEEP_HOUR_ET", "9") or "9")
KALSHI_15M_LATEST_STRATEGY_SWEEP_FILE = DATA_DIR / "kalshi_15m_latest_strategy_sweep.json"


def _default_kalshi_15m_strategy_sweep_grid() -> dict[str, list[float | int]]:
    """The actual '10,000+ mix of strategies' grid -- 15 confidence
    floors x 10 yes-side surcharges x 9 assumed entry prices (a real
    sensitivity sweep across kalshi_15m_metals_backtest.py's own
    disclosed pricing-assumption limitation, per that module's own
    docstring suggestion) x 4 position sizes x 3 concurrency caps =
    16,200 combinations. A plain function (not a module-level constant)
    so a future change to these ranges doesn't need a process restart to
    take effect via a test's own monkeypatch, same reasoning
    kalshi_15m_trade_analysis.py's own recommend_* functions already
    follow for their tunable constants."""
    return {
        "model_confidence_min": [round(0.50 + 0.025 * i, 4) for i in range(15)],  # 0.50 .. 0.85
        "yes_confidence_extra_required": [round(0.10 * i / 9, 4) for i in range(10)],  # 0.00 .. 0.10
        "assumed_entry_price": [round(0.10 * i, 2) for i in range(1, 10)],  # 0.10 .. 0.90
        "position_size_pct": [0.02, 0.05, 0.08, 0.12],
        "max_concurrent_positions": [1, 3, 5],
    }
# Read-only, project-WIDE AI-powered analysis layer covering all 5
# markets (perps, stocks, crypto, options, kalshi_15m), added per
# explicit user direction (chosen over "replace the prediction model
# with Claude entirely" via an AskUserQuestion, widened from Kalshi
# 15-minute markets only to "across all of the bots" in the same build,
# then moved off the Anthropic API onto HF's own Inference Providers --
# reusing this same process's existing HF_API_KEY -- once the separate
# Anthropic billing requirement turned out to be an unwanted surprise)
# -- see ai_monitor.py's own module docstring for the full design and
# why it never touches order placement on any market. Default
# 6am ET: safely after every daily training job across all 5 markets
# (perps 3, kalshi_15m/stocks 4, stocks/options' own torch retrains 5) so
# each day's review reflects that day's freshly-trained models, not the
# previous day's.
AI_MONITOR_HOUR_ET = int(os.getenv("AI_MONITOR_HOUR_ET", "6") or "6")
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
# Real, confirmed constraint this whole comment block predates: every
# incident above happened on Render's own 512MB-1GB per-service
# containers. This process now runs on a Hugging Face Docker Space's
# "cpu-upgrade" tier (8 vCPU / 32GB RAM, confirmed via this session's own
# migration work) -- SHARED across all 4 markets in one process, but
# still ~30-60x the memory headroom any single one of them had before.
# Per explicit user direction ("maximize the use of the HF server"):
# default bumped 1->3 -- comfortably bounded well below the 10 APScheduler
# would allow unbounded (the ORIGINAL incident), while letting a Threads
# post/data_collect/train no longer fully serialize behind each other the
# way a single worker forces. fastcheck bumped 2->3 for the same
# resource-availability reason, plus a little extra margin specifically
# for the real-money exit check it exists to protect from ever queuing.
scheduler = BackgroundScheduler(
    timezone="America/New_York",
    job_defaults={"misfire_grace_time": 300},
    executors={
        "default": APSThreadPoolExecutor(max_workers=3),
        "fastcheck": APSThreadPoolExecutor(max_workers=3),
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


@_locked_job("kalshi_15m_data_collect", stale_after_sec=600)
def _run_kalshi_15m_data_collect() -> dict[str, Any]:
    """Data collection for Kalshi's 15-minute event-contract markets --
    see kalshi_15m_data.py's own module docstring. Deliberately simpler
    than _run_perps_data_collect above (no ticker-activity-cache refresh,
    no correlation-study wiring) -- this module's universe is a small
    fixed 5-coin list, not a dynamically-ranked watchlist, and the
    correlation-study infra is a separate, later decision, not silently
    bundled into the first data-collection pass for a brand new market."""
    try:
        df = kalshi_15m_data.collect_dataset_rows()
        if df.empty:
            return {"ok": False, "reason": "no_rows_collected"}
        # Caches this cycle's own crypto frame for the metals data-collect
        # job below to fold into ITS OWN correlation study as cross-asset
        # peers (see crypto_correlation.set_latest_kalshi_15m_crypto_df's
        # own comment) -- best-effort, must never block the archival push.
        try:
            from data import crypto_correlation
            crypto_correlation.set_latest_kalshi_15m_crypto_df(df)
        except Exception as exc:
            logger.warning("[app_kalshi] caching crypto df for cross-asset correlation failed (non-fatal): %s", exc)
        return kalshi_15m_data.push_dataset_snapshot(df)
    finally:
        gc.collect()


@_locked_job("kalshi_15m_metals_data_collect", stale_after_sec=600)
def _run_kalshi_15m_metals_data_collect() -> dict[str, Any]:
    """Data collection for Kalshi's 15-minute GOLD/SILVER/COPPER markets
    -- see kalshi_15m_metals_data.py's own module docstring for why this
    is a genuinely different pipeline from the crypto one above (no
    perps-contract feed to reuse; builds its own price history from a
    free spot-price API, one point per cycle)."""
    try:
        df = kalshi_15m_metals_data.collect_dataset_rows()
        if df.empty:
            return {"ok": False, "reason": "no_rows_collected"}
        # Refreshes the metals correlation study (see crypto_correlation.py's
        # own refresh_metals_study comment) on the SAME df this cycle just
        # collected, PLUS whatever crypto frame the crypto data-collect job
        # above last cached -- no extra network call either way. Unlike
        # crypto's own correlation study (owned/refreshed by perps' own,
        # separate data-collect job), metals has no other owner: this IS
        # the one place its data gets collected, so this job is the one
        # that must keep the study current. Best-effort -- must never block the
        # actual archival push below.
        try:
            from data import crypto_correlation
            crypto_correlation.refresh_metals_study(df, crypto_df=crypto_correlation.get_latest_kalshi_15m_crypto_df())
        except Exception as exc:
            logger.warning("[app_kalshi] metals correlation study refresh failed (non-fatal): %s", exc)
        return kalshi_15m_metals_data.push_dataset_snapshot(df)
    finally:
        gc.collect()


@_locked_job("kalshi_15m_cycle", stale_after_sec=300)
def _run_kalshi_15m_cycle() -> dict[str, Any]:
    """Settlement check FIRST, then early-exit management, then entry
    scan -- freeing a just-settled OR just-early-exited coin's slot
    before deciding whether to enter a new position matters here (unlike
    perps' separate fast_check/entry_scan jobs) since this single
    combined job is this market's only cycle, see
    KALSHI_15M_CYCLE_MINUTES's own comment for why latency isn't a
    concern worth two separate jobs. dry_run=False here defers the actual
    live/dry decision to kalshi_15m_strategy.LIVE_TRADING_ENABLED (see
    that module's own docstring) -- set on the live Space, order
    mechanics confirmed against a real Kalshi response, so this places
    real orders. manage_open_positions' own USE_EARLY_EXIT is a separate,
    still-off-by-default gate (see its own module-level comment) -- this
    call is always made so its own decisions stay visible for
    observability even before that flag is ever turned on."""
    settlement_result = kalshi_15m_strategy.check_settlements()
    management_result = kalshi_15m_strategy.manage_open_positions(dry_run=False)
    entry_result = kalshi_15m_strategy.scan_and_enter(dry_run=False)
    # See compute_win_streak_cooldown_active's own comment -- the moment a
    # real win streak is long enough to pause on, kick off the retrain +
    # backtest verification in the BACKGROUND (never inline here: a full
    # retrain can genuinely take real wall-clock time, and this cycle's
    # own settlement/management/entry-scan work for every OTHER coin must
    # never wait on it). _locked_job below makes a second trigger while
    # one is already running a safe no-op, so a slow verification can
    # never stack up duplicate retrains across consecutive 2-minute ticks.
    cooldown = entry_result.get("win_streak_cooldown") or {}
    if cooldown.get("active"):
        threading.Thread(
            target=_run_kalshi_15m_win_streak_verification, args=(cooldown,),
            daemon=True, name="kalshi15m-win-streak-verification",
        ).start()
    return {
        "ok": True, "settlements": settlement_result, "management": management_result, "entries": entry_result,
    }


@_locked_job("kalshi_15m_win_streak_verification", stale_after_sec=1800)
def _run_kalshi_15m_win_streak_verification(cooldown: dict[str, Any]) -> dict[str, Any]:
    """Triggered from _run_kalshi_15m_cycle the instant scan_and_enter's
    own win_streak_cooldown reports active=True -- per explicit user
    direction ("after a couple of winning strikes[,] take a break and
    restudy... retrain again with the model[,] and go back after making
    sure it's going to keep winning"). Retrains both models fresh (same
    real trade_log every retrain here already trains on), then runs a
    real walk-forward backtest -- same "make sure" standard
    _run_kalshi_15m_backtest's own auto-retrain-on-loss trigger already
    uses (a non-negative mean_return_pct). Passing clears the cooldown
    for THIS exact streak (kalshi_15m_strategy.apply_win_streak_cooldown_result);
    anything else leaves it active, so the NEXT cycle's own win-streak
    check simply retries -- cheap and safe to re-attempt, no persistent
    failure state to get stuck in."""
    try:
        trade_log = kalshi_15m_strategy._load_state().get("trade_log")  # noqa: SLF001
    except Exception as exc:
        logger.warning("[app_kalshi] could not read kalshi_15m trade_log for win-streak retrain: %s", exc)
        trade_log = None

    retrain_results: dict[str, Any] = {}
    try:
        retrain_results["crypto"] = kalshi_15m_model.train_model(trade_log=trade_log)
    except Exception as exc:
        logger.warning("[app_kalshi] win-streak crypto retrain failed: %s", exc)
        retrain_results["crypto"] = {"ok": False, "error": str(exc)}
    try:
        retrain_results["metals"] = kalshi_15m_metals_model.train_model(trade_log=trade_log)
    except Exception as exc:
        logger.warning("[app_kalshi] win-streak metals retrain failed: %s", exc)
        retrain_results["metals"] = {"ok": False, "error": str(exc)}

    try:
        backtest_result = kalshi_15m_backtest.run_walkforward_backtest()
    except Exception as exc:
        logger.warning("[app_kalshi] win-streak backtest verification failed: %s", exc)
        backtest_result = {"ok": False, "error": str(exc)}

    mean_return = backtest_result.get("mean_return_pct")
    passed = bool(backtest_result.get("ok")) and mean_return is not None and mean_return >= 0
    cooldown_result = kalshi_15m_strategy.apply_win_streak_cooldown_result(
        passed, real_trade_count=cooldown.get("real_trade_count"),
        reason=f"walkforward_backtest mean_return_pct={mean_return}",
    )
    logger.info(
        "[app_kalshi] kalshi_15m win-streak verification (streak=%s): backtest mean_return_pct=%s -> %s",
        cooldown.get("streak"), mean_return, "cleared" if cooldown_result.get("cleared") else "still cooling down",
    )
    return {"retrain": retrain_results, "backtest": backtest_result, "cooldown_result": cooldown_result}


@_locked_job("kalshi_15m_reconcile", stale_after_sec=600)
def _run_kalshi_15m_reconcile() -> dict[str, Any]:
    """See KALSHI_15M_RECONCILE_DAYS's own comment for the full rationale.
    Crypto only -- metals has no historical backfill capability at all
    (no free historical price API exists, see kalshi_15m_metals_data.py's
    own module docstring), so there's nothing for this job to reconcile
    there."""
    return kalshi_15m_data.backfill_minute_history(days=KALSHI_15M_RECONCILE_DAYS)


@_locked_job("kalshi_15m_train", stale_after_sec=1800)
def _run_kalshi_15m_train() -> dict[str, Any]:
    """Trains BOTH models sharing this one job/cadence -- crypto's
    kalshi_15m_model and metals' own kalshi_15m_metals_model (see
    kalshi_15m_strategy.ASSET_SERIES' own comment for why these are two
    separate models, not one). Each is independently best-effort: a
    failure in one (e.g. metals not having reached MIN_TRAIN_ROWS yet)
    must not block the other from training."""
    try:
        trade_log = kalshi_15m_strategy._load_state().get("trade_log")  # noqa: SLF001
    except Exception as exc:
        logger.warning("[app_kalshi] could not read kalshi_15m trade_log for outcome-aware training: %s", exc)
        trade_log = None

    crypto_result = kalshi_15m_model.train_model(trade_log=trade_log)
    try:
        metals_result = kalshi_15m_metals_model.train_model(trade_log=trade_log)
    except Exception as exc:
        logger.warning("[app_kalshi] kalshi_15m metals training failed: %s", exc)
        metals_result = {"ok": False, "error": str(exc)}

    # Meta-labeling (see kalshi_15m_meta_model.py's own module docstring) --
    # fully separate and best-effort: must never affect this job's own
    # primary result either way, same pattern _run_perps_train's own
    # identical call uses. Crypto only (see that module's docstring on
    # why); only worth attempting once a fresh primary crypto model
    # actually exists to build out-of-fold labels from.
    # KALSHI_15M_USE_META_MODEL stays off by default regardless of whether
    # this succeeds -- training it here just keeps a fresh one available
    # on HF for offline backtest validation before that flag is turned on.
    if crypto_result.get("ok"):
        try:
            kalshi_15m_meta_model.train_meta_model()
        except Exception as exc:
            logger.warning("[app_kalshi] kalshi_15m meta-model training failed (non-fatal): %s", exc)
    return {"ok": True, "crypto": crypto_result, "metals": metals_result}


@_locked_job("kalshi_15m_torch_train", stale_after_sec=1800)
def _run_kalshi_15m_torch_train() -> dict[str, Any]:
    """See KALSHI_15M_TORCH_TRAIN_HOUR_ET's own comment and kalshi_15m_
    model.train_torch_candidate_model's own docstring for the full
    champion/challenger design -- crypto only, metals excluded for now
    (see that same comment for why)."""
    try:
        trade_log = kalshi_15m_strategy._load_state().get("trade_log")  # noqa: SLF001
    except Exception as exc:
        logger.warning("[app_kalshi] could not read kalshi_15m trade_log for torch training: %s", exc)
        trade_log = None
    try:
        return kalshi_15m_model.train_torch_candidate_model(trade_log=trade_log)
    except Exception as exc:
        logger.warning("[app_kalshi] kalshi_15m torch candidate training failed: %s", exc)
        return {"ok": False, "error": str(exc)}
    finally:
        gc.collect()


@_locked_job("kalshi_15m_trade_analysis", stale_after_sec=300)
def _run_kalshi_15m_trade_analysis() -> dict[str, Any]:
    """Daily aggregate win/loss review over the WHOLE real trade history --
    see KALSHI_15M_TRADE_ANALYSIS_HOUR_ET's own comment and
    kalshi_15m_trade_analysis.analyze_trade_history's own module
    docstring. Read-only over state (no confidence tuning here -- that
    already happens on a faster, real-trade-count-driven cadence via
    kalshi_15m_strategy._maybe_run_batch_trade_analysis); exposed to the
    dashboard via /api/kalshi15m/trade_analysis below. No Threads post --
    kalshi_15m has no Threads presence at all today (see threads_post.py:
    every _MARKET_HASHTAGS/_MARKET_LABELS entry covers only the original
    4 markets), so posting here would silently fall back to perps' own
    hashtags/label, misattributing this market's analysis."""
    from data import kalshi_15m_trade_analysis
    try:
        state = kalshi_15m_strategy._load_state()  # noqa: SLF001
        trade_log = state.get("trade_log") or []
        analysis = kalshi_15m_trade_analysis.analyze_trade_history(trade_log)
        return {"ok": True, "analysis": analysis}
    except Exception as exc:
        logger.warning("[app_kalshi] kalshi_15m trade analysis failed: %s", exc)
        return {"ok": False, "error": str(exc)}


@_locked_job("kalshi_15m_backtest", stale_after_sec=3600)
def _run_kalshi_15m_backtest() -> dict[str, Any]:
    """See KALSHI_15M_BACKTEST_HOUR_ET's own comment -- a regularly-
    scheduled multi-fold walk-forward replay (a real forward test: each
    fold's own train window is strictly earlier than its test window,
    same leakage-free discipline as every other walk-forward in this
    codebase), not the one-off manual run this has been until now.
    Reacts to a confirmed losing result the same "use everything the bot
    has as a resource" way every sibling market's own
    maybe_auto_improve_from_backtest does: an immediate extra retrain of
    both the primary and torch candidate models, so a real edge
    regression doesn't just sit there until the next scheduled off-hours
    train. Never touches order placement or position management --
    read-only over trading, adjusts training/reporting only."""
    try:
        result = kalshi_15m_backtest.run_walkforward_backtest()
        if not result.get("ok"):
            return result
        save_json(KALSHI_15M_LATEST_BACKTEST_FILE, result)

        mean_return = result.get("mean_return_pct")
        if mean_return is not None and mean_return < 0:
            logger.warning(
                "[app_kalshi] kalshi_15m walk-forward backtest shows a loss (mean_return_pct=%.4f) -- retraining now",
                mean_return,
            )
            try:
                trade_log = kalshi_15m_strategy._load_state().get("trade_log")  # noqa: SLF001
            except Exception as exc:
                logger.warning("[app_kalshi] could not read kalshi_15m trade_log for auto-retrain: %s", exc)
                trade_log = None
            try:
                retrain_result = kalshi_15m_model.train_model(trade_log=trade_log)
                result["auto_retrain"] = {"ok": retrain_result.get("ok"), "rows": retrain_result.get("rows")}
            except Exception as exc:
                logger.warning("[app_kalshi] kalshi_15m auto-retrain after losing backtest failed: %s", exc)
            try:
                torch_result = kalshi_15m_model.train_torch_candidate_model(trade_log=trade_log)
                result["auto_torch_retrain"] = {"ok": torch_result.get("ok"), "promoted": torch_result.get("promoted")}
            except Exception as exc:
                logger.warning("[app_kalshi] kalshi_15m auto-torch-retrain after losing backtest failed: %s", exc)
        return result
    except Exception as exc:
        logger.warning("[app_kalshi] kalshi_15m backtest failed: %s", exc)
        return {"ok": False, "error": str(exc)}
    finally:
        gc.collect()


@_locked_job("kalshi_15m_strategy_sweep", stale_after_sec=3600)
def _run_kalshi_15m_strategy_sweep(param_grid: dict[str, list[Any]] | None = None) -> dict[str, Any]:
    """See KALSHI_15M_STRATEGY_SWEEP_HOUR_ET's own comment -- runs
    strategy_sweep.run_parameter_sweep against kalshi_15m_metals_backtest
    (GOLD/SILVER/COPPER -- this account's own real, permanent live-entry
    universe, see kalshi_15m_strategy.ACTIVE_ENTRY_COINS), saves the
    ranked result, and -- when the sweep actually surfaces a combination
    that beats this account's OWN currently-live parameters with real,
    walk-forward-consistent evidence -- logs that clearly rather than
    silently. Deliberately does NOT auto-apply anything: unlike
    apply_confidence_threshold_override's own narrow, single-parameter,
    already-proven pattern, this sweeps FIVE parameters against a
    disclosed-limitation backtest (see kalshi_15m_metals_backtest.py's
    own module docstring on what it can't replay -- cross-asset
    correlation, coin/hour trust) -- a human decision point, not a
    for-now-safe auto-apply. Never touches order placement or position
    management -- read-only over trading, reporting only."""
    try:
        grid = param_grid or _default_kalshi_15m_strategy_sweep_grid()
        result = strategy_sweep.run_parameter_sweep(kalshi_15m_metals_backtest, grid, coins=sorted(kalshi_15m_strategy.ACTIVE_ENTRY_COINS))
        if not result.get("ok"):
            return result
        save_json(KALSHI_15M_LATEST_STRATEGY_SWEEP_FILE, result)

        top = result.get("top_strategies") or []
        if top:
            best = top[0]
            logger.info(
                "[app_kalshi] kalshi_15m strategy sweep: %d/%d combinations had real evidence, best = %s "
                "(mean_return_pct=%.4f, profitable_fold_ratio=%.2f, total_trades=%d)",
                result.get("combinations_with_evidence", 0), result.get("combinations_evaluated", 0),
                best["params"], best["mean_return_pct"], best["profitable_fold_ratio"], best["total_trades"],
            )
        else:
            logger.info("[app_kalshi] kalshi_15m strategy sweep: no combination cleared the real-evidence bar")
        return result
    except Exception as exc:
        logger.warning("[app_kalshi] kalshi_15m strategy sweep failed: %s", exc)
        return {"ok": False, "error": str(exc)}
    finally:
        gc.collect()


@_locked_job("ai_monitor", stale_after_sec=180)
def _run_ai_monitor() -> dict[str, Any]:
    """See ai_monitor.py's own module docstring -- a read-only, project-
    wide, AI-powered review across all 5 markets, never a predictor.
    stale_after_sec is short (180s) since a single Anthropic API call,
    not the heavy multi-candidate model fits any market's own train job
    does, is the only real work here."""
    return ai_monitor.run_monitor_cycle()


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
    result = perps_model.train_model(trade_log=trade_log)
    # Meta-labeling (see perps_meta_model.py's own module docstring) --
    # fully separate and best-effort: must never affect this job's own
    # primary result either way, so its own exception is caught here, not
    # inside train_meta_model itself. Only worth attempting once a fresh
    # primary model actually exists to build out-of-fold labels from.
    # PERPS_USE_META_MODEL stays off by default regardless of whether this
    # succeeds -- training it here just keeps a fresh one available on HF
    # for offline backtest validation before that flag is ever turned on.
    if result.get("ok"):
        try:
            perps_meta_model.train_meta_model()
        except Exception as exc:
            logger.warning("[app_kalshi] meta-model training failed (non-fatal): %s", exc)
    return result


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

        # Same evidence-gated discipline for the scale-in/partial-exit/
        # conviction-sizing live trial -- see
        # perps_trade_analysis.recommend_position_management_trial's own
        # docstring for the 4 possible outcomes per feature.
        position_management_applied: dict[str, Any] = {}
        for feature, (tuning_key, global_name) in perps_strategy._POSITION_MANAGEMENT_TUNING_KEYS.items():  # noqa: SLF001
            current_enabled = bool(tuning_state.get(tuning_key, getattr(perps_strategy, global_name)))
            pm_recommendation = perps_trade_analysis.recommend_position_management_trial(
                trade_log, feature=feature, current_enabled=current_enabled,
            )
            if pm_recommendation.get("should_apply"):
                applied = perps_strategy.apply_position_management_override(
                    feature, enabled=bool(pm_recommendation["recommended_enabled"]),
                    reason=f"evidence-gated: {pm_recommendation.get('action')}",
                )
                position_management_applied[feature] = applied
                logger.info("[app_kalshi] perps %s tuned: %s", feature, applied)

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
            "position_management_applied": position_management_applied,
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
                _run_kalshi_15m_data_collect, "interval", minutes=KALSHI_15M_DATA_COLLECT_MINUTES,
                id="kalshi_15m_data_collect", replace_existing=True,
                next_run_time=dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=KALSHI_15M_DATA_COLLECT_MINUTES),
            )
            scheduler.add_job(
                _run_kalshi_15m_metals_data_collect, "interval", minutes=KALSHI_15M_METALS_DATA_COLLECT_MINUTES,
                id="kalshi_15m_metals_data_collect", replace_existing=True,
                next_run_time=dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=KALSHI_15M_METALS_DATA_COLLECT_MINUTES),
            )
            scheduler.add_job(
                _run_kalshi_15m_cycle, "interval", minutes=KALSHI_15M_CYCLE_MINUTES,
                id="kalshi_15m_cycle", replace_existing=True,
                next_run_time=dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=KALSHI_15M_CYCLE_MINUTES),
            )
            _reconcile_hour, _reconcile_minute = divmod((KALSHI_15M_TRAIN_HOUR_ET * 60 - 30) % (24 * 60), 60)
            scheduler.add_job(
                _run_kalshi_15m_reconcile, "cron", hour=_reconcile_hour, minute=_reconcile_minute,
                id="kalshi_15m_reconcile", replace_existing=True,
            )
            scheduler.add_job(
                _run_kalshi_15m_train, "cron", hour=KALSHI_15M_TRAIN_HOUR_ET, minute=0,
                id="kalshi_15m_train", replace_existing=True,
            )
            scheduler.add_job(
                _run_kalshi_15m_torch_train, "cron", hour=KALSHI_15M_TORCH_TRAIN_HOUR_ET, minute=0,
                id="kalshi_15m_torch_train", replace_existing=True,
            )
            scheduler.add_job(
                _run_kalshi_15m_trade_analysis, "cron",
                hour=KALSHI_15M_TRADE_ANALYSIS_HOUR_ET, minute=KALSHI_15M_TRADE_ANALYSIS_MINUTE_ET,
                id="kalshi_15m_trade_analysis", replace_existing=True,
            )
            scheduler.add_job(
                _run_kalshi_15m_backtest, "cron", hour=KALSHI_15M_BACKTEST_HOUR_ET, minute=0,
                id="kalshi_15m_backtest", replace_existing=True,
            )
            scheduler.add_job(
                _run_kalshi_15m_strategy_sweep, "cron",
                day_of_week=KALSHI_15M_STRATEGY_SWEEP_DAY_OF_WEEK, hour=KALSHI_15M_STRATEGY_SWEEP_HOUR_ET, minute=0,
                id="kalshi_15m_strategy_sweep", replace_existing=True,
            )
            scheduler.add_job(
                _run_ai_monitor, "cron", hour=AI_MONITOR_HOUR_ET, minute=0,
                id="ai_monitor", replace_existing=True,
            )
            scheduler.add_job(
                _run_perps_train, "cron", hour=PERPS_TRAIN_HOUR_ET, minute=0,
                id="perps_train", replace_existing=True,
            )
            scheduler.add_job(
                _run_perps_trade_analysis, "cron", hour=PERPS_TRADE_ANALYSIS_HOUR_ET, minute=PERPS_TRADE_ANALYSIS_MINUTE_ET,
                id="perps_trade_analysis", replace_existing=True,
            )
            # Threads content jobs (hourly_status/trending_news/sentiment_snapshot):
            # briefly moved to external cron-job.org triggers (see
            # api_perps_threads_trending_news and its 2 siblings below,
            # kept as manual/fallback triggers) specifically to cut
            # non-trading-critical job/executor overhead out of Render's
            # own metered-cost process. Restored here as in-process jobs
            # now that this runs on a flat-rate Hugging Face Space instead
            # -- there's no more per-resource cost benefit to routing
            # through an external scheduler, and running everything in
            # this one already-24/7 process is simpler than coordinating
            # an external trigger service. executor="fastcheck" (like
            # fast_check/entry_scan below): real, confirmed production
            # incident this session -- with only fast_check isolated from
            # "default", a slow train/data_collect run could block these
            # for minutes at a stretch; all of these are themselves fast,
            # bounded operations sharing this pool safely with fast_check
            # the same way. Staggered next_run_time so hourly_status (1h),
            # sentiment_snapshot (60min), and trending_news (30min) don't
            # all land on the exact same tick (confirmed live incident:
            # that cluster running back-to-back once bumped into
            # fast_check's own cadence and caused a one-cycle skip).
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
            # Same cold-start safety net as perps just above, for the same
            # reason -- REAL gap found live: kalshi_15m_train only ever
            # fires on its own daily cron (hour=KALSHI_15M_TRAIN_HOUR_ET),
            # and with this process restarting periodically (the recurring
            # dead-HF_API_KEY fix cycle, redeploys, etc.), a container can
            # keep missing that one 5-minute daily window indefinitely --
            # confirmed live: both kalshi-15m-model and
            # kalshi-15m-metals-model HF repos still returned 404 (never
            # once created) despite the data-collection side working the
            # whole time. Only runs if EITHER model is uncached (mirrors
            # _run_kalshi_15m_train's own "train both, tolerate one
            # failing" design), so a normal restart with an already-cached
            # model still skips this and waits for the daily cron.
            try:
                crypto_cached = kalshi_15m_model.load_model()[0] is not None
                metals_cached = kalshi_15m_metals_model.load_model()[0] is not None
                if not crypto_cached or not metals_cached:
                    train_result = _run_kalshi_15m_train()
                    logger.info("Startup kalshi_15m train attempt (cold start): %s", train_result)
                else:
                    logger.info("Startup kalshi_15m train skipped: both models already cached, daily cron will retrain")
            except Exception as exc:
                logger.warning("Startup kalshi_15m train failed: %s", exc)
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
    # Defense-in-depth headers regardless of the access model -- the
    # primary control on every route here (read or write) is this Space's
    # own privacy: only the owner's account/collaborators can reach it at
    # all (see is_cron_authorized's own docstring for why that function is
    # a no-op now, not a secret check). These headers: block this response
    # from being framed by another site (clickjacking), stop browsers from
    # MIME-sniffing a response into something more dangerous than its
    # declared Content-Type, and avoid leaking this dashboard's own URL
    # (which could contain no secrets, but there's no reason to send it)
    # as a Referer header when a visitor clicks an outbound link.
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


@app.route("/kalshi15m")
def kalshi_15m_dashboard():
    """Kalshi's own 15-minute event-contract markets -- see
    kalshi_15m.py's own module docstring for the product and
    kalshi_15m_strategy.py's for why this stays dry-run-only for now."""
    return render_template("kalshi_15m_dashboard.html")


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
    from data import crypto_correlation

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
        # Real trades only -- explicit user direction ("we doing only real
        # data please not dry run or fake"). See alpaca_server.py's
        # identical fix for the full rationale.
        "trade_count": sum(1 for t in (state.get("trade_log") or []) if not t.get("dry_run")),
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
        # Real diagnostic visibility: how many instruments the chart-study
        # layer actually has enough history for right now, vs. how many it
        # knows about at all -- surfaces a real data-pipeline gap (most
        # instruments failing to collect, or too thin a history window)
        # directly here, instead of only showing up as scattered "no ...
        # data" reasons in individual Threads posts (see
        # crypto_correlation.study_health's own docstring).
        "correlation_study_health": {
            "perps_study": crypto_correlation.study_health(crypto_correlation.get_perps_study()),
            "remote_alpaca_study": crypto_correlation.study_health(crypto_correlation.get_remote_alpaca_study()),
        },
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
            "scale_in_enabled": perps_strategy.USE_SCALE_IN,
            "partial_exit_enabled": perps_strategy.USE_PARTIAL_EXIT,
            "conviction_sizing_enabled": perps_strategy.USE_CONVICTION_SIZING,
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


@app.route("/api/kalshi15m/status")
def api_kalshi_15m_status():
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    _, crypto_meta = kalshi_15m_model.load_model()
    _, metals_meta = kalshi_15m_metals_model.load_model()
    trade_log = state.get("trade_log") or []
    realized_pnl_by_date = state.get("realized_pnl_by_date") or {}
    total_realized_pnl = round(sum(float(v) for v in realized_pnl_by_date.values()), 6)

    positions = []
    for p in state.get("positions") or []:
        enriched = dict(p)
        remaining = None
        if p.get("close_time"):
            remaining = kalshi_15m.seconds_to_close({"close_time": p["close_time"]})
        enriched["seconds_to_close"] = remaining
        positions.append(enriched)

    return jsonify({
        "ok": True,
        "now": dt.datetime.now(dt.timezone.utc).isoformat(),
        "live_trading_enabled": kalshi_15m_strategy.LIVE_TRADING_ENABLED,
        # Real gap this closes: no balance field existed here at all --
        # the dashboard had no way to show what this strategy is actually
        # sizing positions against. Same real shard-2-specific balance
        # _account_budget_usd() itself now uses (see that function's own
        # docstring for the real pooled-balance bug this shares a fix
        # with) -- a $100 placeholder while dry-run, matching what
        # position sizing itself falls back to.
        "account_budget_usd": kalshi_15m_strategy._account_budget_usd(),  # noqa: SLF001
        "positions": positions,
        "open_position_count": len(positions),
        "max_concurrent_positions": kalshi_15m_strategy.MAX_CONCURRENT_POSITIONS,
        "today_realized_pnl_usd": float(realized_pnl_by_date.get(et_today().isoformat(), 0.0)),
        "total_realized_pnl_usd": total_realized_pnl,
        # Real trades only -- see alpaca_server.py's identical fix this
        # same session ("we doing only real data please not dry run or fake").
        "trade_count": sum(1 for t in trade_log if not t.get("dry_run")),
        "win_rate": win_rate_stats(trade_log),
        # Two independent models -- see kalshi_15m_strategy.ASSET_SERIES'
        # own comment for why crypto and metals can't share one (a
        # genuinely different, leaner feature set for metals).
        "model": {
            "trained": crypto_meta is not None,
            "model_type": (crypto_meta or {}).get("model_type"),
            "trained_at": (crypto_meta or {}).get("trained_at"),
            "rows": (crypto_meta or {}).get("rows"),
            "scores": (crypto_meta or {}).get("scores"),
            "feature_importances": (crypto_meta or {}).get("feature_importances"),
        },
        "metals_model": {
            "trained": metals_meta is not None,
            "model_type": (metals_meta or {}).get("model_type"),
            "trained_at": (metals_meta or {}).get("trained_at"),
            "rows": (metals_meta or {}).get("rows"),
            "scores": (metals_meta or {}).get("scores"),
            "feature_importances": (metals_meta or {}).get("feature_importances"),
        },
        "universe": list(kalshi_15m_strategy.ASSET_SERIES.keys()),
        "crypto_universe": kalshi_15m_data.get_universe(),
        "metals_universe": kalshi_15m_metals_data.get_universe(),
        # See ACTIVE_ENTRY_COINS' own comment -- the rest of "universe"
        # above stays fully wired for data collection/correlation/
        # existing-position management, just not new entries.
        "active_entry_coins": sorted(kalshi_15m_strategy.ACTIVE_ENTRY_COINS),
        "params": {
            "model_confidence_min": kalshi_15m_strategy.MODEL_CONFIDENCE_MIN,
            "yes_confidence_extra_required": kalshi_15m_strategy.YES_CONFIDENCE_EXTRA_REQUIRED,
            "position_size_pct": kalshi_15m_strategy.POSITION_SIZE_PCT,
            "max_concurrent_positions": kalshi_15m_strategy.MAX_CONCURRENT_POSITIONS,
            "min_seconds_to_close_for_entry": kalshi_15m_strategy.MIN_SECONDS_TO_CLOSE_FOR_ENTRY,
            "use_volume_confirmation": kalshi_15m_strategy.USE_VOLUME_CONFIRMATION,
            "use_real_outcome_calibration": kalshi_15m_strategy.USE_REAL_OUTCOME_CALIBRATION,
            "cycle_minutes": KALSHI_15M_CYCLE_MINUTES,
            "data_collect_minutes": KALSHI_15M_DATA_COLLECT_MINUTES,
            "train_hour_et": KALSHI_15M_TRAIN_HOUR_ET,
        },
    })


@app.route("/api/kalshi15m/balance-by-shard", methods=["GET"])
def api_kalshi_15m_balance_by_shard():
    """Read-only diagnostic: answers "does shard 2 (Crypto/Commodities --
    where these 15-minute markets actually settle orders, confirmed live
    via a real insufficient_shard_balance error) have any collateral on
    it" without guessing -- see kalshi_15m.get_subaccount_balances's own
    docstring for why /portfolio/balance alone (no exchange_index) can't
    answer this (it reports pooled across all shards, not per-shard).
    Never places an order or moves money -- a pure GET."""
    try:
        subaccounts = kalshi_15m.get_subaccount_balances()
        shard2 = kalshi_15m.get_balance_by_shard(exchange_index=2)
        return jsonify({"ok": True, "subaccount_balances": subaccounts, "shard_2_crypto_commodities": shard2})
    except Exception as exc:
        logger.warning("[app_kalshi] kalshi_15m balance-by-shard check failed", exc_info=True)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/kalshi15m/real-positions", methods=["GET"])
def api_kalshi_15m_real_positions():
    """Read-only diagnostic: what Kalshi's OWN account actually holds
    right now, straight from /portfolio/positions and /portfolio/orders
    -- for comparing against this app's own locally-recorded `positions`/
    `trade_log` bookkeeping (see /api/kalshi15m/status), which only ever
    reflects what THIS code intended/recorded, not independently-verified
    proof of what Kalshi's matching engine actually did with each order.
    Never places an order or moves money -- pure GETs."""
    try:
        positions = kalshi_15m.get_portfolio_positions()
        orders = kalshi_15m.get_orders()
        return jsonify({"ok": True, "market_positions": positions, "orders": orders})
    except Exception as exc:
        logger.warning("[app_kalshi] kalshi_15m real-positions check failed", exc_info=True)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/kalshi15m/diagnose-entries", methods=["GET"])
def api_kalshi_15m_diagnose_entries():
    """Read-only diagnostic: runs the EXACT same evaluate_candidate call
    scan_and_enter itself would make right now for every coin in
    ACTIVE_ENTRY_COINS, with the SAME trade_log-derived overrides
    (confidence floor, correlation study) -- answers "why isn't the bot
    entering anything right now" directly instead of guessing from logs
    that don't record per-check rejection reasons. Never places an order
    or mutates state -- evaluate_candidate is a pure function."""
    from data import kalshi_15m_strategy
    try:
        state = kalshi_15m_strategy._load_state()  # noqa: SLF001
        tuning = state.get("tuning") or {}
        trade_log = state.get("trade_log")
        win_streak_cooldown = kalshi_15m_strategy.compute_win_streak_cooldown_active(state)
        current_et_hour = kalshi_15m_strategy._current_et_hour()  # noqa: SLF001
        hour_trust = kalshi_15m_strategy.hour_is_trusted(current_et_hour, trade_log)
        per_coin = {}
        for coin in sorted(kalshi_15m_strategy.ACTIVE_ENTRY_COINS):
            coin_trust = kalshi_15m_strategy.coin_is_trusted(coin, trade_log)
            decision = kalshi_15m_strategy.evaluate_candidate(
                coin,
                confidence_min=tuning.get("model_confidence_min"),
                correlation_study_enabled=tuning.get("correlation_study_enabled"),
                correlation_max_adjustment=tuning.get("correlation_confidence_max_adjustment"),
                trade_log=trade_log,
            )
            per_coin[coin] = {"coin_trust": coin_trust, "decision": decision}
        return jsonify({
            "ok": True, "current_et_hour": current_et_hour, "win_streak_cooldown": win_streak_cooldown,
            "hour_trust": hour_trust, "open_position_count": len(state.get("positions") or []), "per_coin": per_coin,
        })
    except Exception as exc:
        logger.warning("[app_kalshi] kalshi_15m diagnose-entries failed", exc_info=True)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/ai-report", methods=["GET"])
def api_ai_report():
    """The latest saved project-wide, AI-powered status review -- see
    ai_monitor.py's own module docstring. A public read like every other
    status route here; never None-vs-missing-key ambiguous -- returns
    {"ok": True, "report": None} explicitly when nothing has run yet, not
    a 404, so the hub page can render a clear "not available yet" state
    instead of treating it as an error."""
    saved = ai_monitor.get_latest_report()
    return jsonify({"ok": True, "report": saved})


@app.route("/api/ai-report/run", methods=["POST"])
def api_ai_report_run():
    """Manual trigger -- same on-demand convention as every other manual
    route here. Real cost note (not a safety gate, just an honest one):
    each call spends real Anthropic API tokens, unlike every read-only
    route on this Space."""
    try:
        result = ai_monitor.run_monitor_cycle()
        return jsonify(result)
    except Exception as exc:
        logger.warning("[app_kalshi] AI monitor manual run failed", exc_info=True)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/kalshi15m/backfill", methods=["POST"])
def api_kalshi_15m_backfill():
    """Manually triggers kalshi_15m_data.backfill_minute_history -- see its
    own docstring for the full design. Runs synchronously, requires the
    same CRON_SECRET bearer every other manual trigger route here does.

    Defaults to 7 days, not the function's own 90-day default: the
    Dockerfile's gunicorn --timeout is 300s, and a full 90-day chunked
    backfill (24h chunks x 90 x 5 coins = 450+ candle-fetch calls, plus up
    to ~90 HF uploads) could genuinely exceed that and get SIGKILLed
    mid-run -- recoverable (each date's own upload already merges with
    whatever's there, so a partial run just needs repeating), but wasteful
    to risk by default. Call repeatedly with different `days` values (or
    just this same default, several times) to build up more history
    safely within the timeout, rather than one large call."""
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        days = int(request.args.get("days", "7") or "7")
        result = kalshi_15m_data.backfill_minute_history(days=days)
        return jsonify(result)
    except Exception as exc:
        logger.warning("[app_kalshi] kalshi_15m backfill failed", exc_info=True)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/kalshi15m/verify-order-mechanics", methods=["POST"])
def api_kalshi_15m_verify_order_mechanics():
    """One-off, manually-triggered diagnostic: places a REAL order on a
    currently-open market to confirm kalshi_15m.create_order's
    /portfolio/events/orders payload is actually accepted by this
    account -- the one piece kalshi_15m_strategy.py's own docstring flags
    as not yet verified live (this dev machine's local Kalshi credentials
    are separately confirmed stale, blocking that check from a dev
    machine; this route lets the already-working LIVE Space credentials
    answer it instead).

    Deliberately structured so a real fill is essentially impossible AND
    economically trivial even if it somehow happened: 1 contract,
    time_in_force="immediate_or_cancel" (fills instantly or cancels, no
    resting order left over), at price=0.01 -- a real market maker would
    have to be willing to sell a YES contract for one cent, ~100x below
    any real 15-minute BTC/ETH/SOL/XRP/DOGE contract's own actual price
    (see kalshi_15m.py's own confirmed live market snapshot: last_price
    around $0.46). This is NOT wired into any scheduled job -- it exists
    purely for this one manual verification, requires the same
    CRON_SECRET bearer every other manual trigger route here does, and
    does nothing to kalshi_15m_strategy.LIVE_TRADING_ENABLED itself
    (still False regardless of this route's result -- flipping that is a
    separate, deliberate decision made after reviewing what this
    confirms)."""
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        market = None
        chosen_coin = None
        for coin, series_ticker in kalshi_15m_strategy.ASSET_SERIES.items():
            market = kalshi_15m.get_current_window_market(series_ticker)
            if market is not None:
                chosen_coin = coin
                break
        if market is None:
            return jsonify({"ok": False, "reason": "no_open_window_on_any_asset_right_now"})

        client_order_id = str(uuid.uuid4())
        order_result = kalshi_15m.create_order(
            ticker=market["ticker"], side="bid", count=1, price=0.01,
            client_order_id=client_order_id, time_in_force="immediate_or_cancel",
        )
        return jsonify({
            "ok": True, "coin": chosen_coin, "ticker": market["ticker"],
            "client_order_id": client_order_id, "order_result": order_result,
        })
    except Exception as exc:
        logger.warning("[app_kalshi] kalshi_15m order-mechanics verification failed", exc_info=True)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/kalshi15m/trades")
def api_kalshi_15m_trades():
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    trade_log = list(reversed(state.get("trade_log") or []))
    return jsonify({
        "ok": True,
        "trade_count": len(trade_log),
        "realized_pnl_by_date": state.get("realized_pnl_by_date") or {},
        "trades": trade_log[:200],
    })


@app.route("/api/threads/posts")
def api_threads_posts():
    """Public, unauthenticated, read-only feed of this account's own
    Threads posts, newest first -- served from the durable HF-backed
    archive (see threads_client.get_posts_archive/sync_posts_archive's own
    docstrings) so this reflects real history, not just whatever shallow
    recent window Meta's own API happens to show right now. Built for
    CumDev's blog to poll (see docs/PUBLIC_THREADS_API.md for the full
    contract) instead of CumDev needing its own separate Threads OAuth
    connection -- every service in this codebase posts as the SAME Threads
    account, so this one endpoint on this one service already sees
    everything, regardless of which bot actually posted it.

    Query params: `limit` (default 25, capped at 50), `since_id` (a post id
    from a previous call -- returns only posts newer than it, for cheap
    incremental polling). Falls back to a live (uncached-archive) Meta
    fetch if the archive is empty (e.g. before the very first sync has
    ever run) so this isn't useless on day one. CORS is wide open (`*`)
    since this only ever re-serves this account's own already-public
    Threads content -- there's nothing here a browser-side fetch from
    another origin needs blocking."""
    try:
        limit = int(request.args.get("limit", 25))
    except (TypeError, ValueError):
        limit = 25
    limit = max(1, min(limit, 50))
    since_id = request.args.get("since_id") or None
    try:
        posts = threads_client.get_posts_archive() or threads_client.list_recent_posts(limit=50)
        if since_id:
            truncated = []
            for post in posts:
                if post.get("id") == since_id:
                    break
                truncated.append(post)
            posts = truncated
        posts = posts[:limit]
        body = {"ok": True, "count": len(posts), "posts": posts}
    except Exception:
        # Public route -- the raw exception (can include upstream URLs/
        # internal details) stays in the server's own logs only.
        logger.warning("[app_kalshi] /api/threads/posts failed", exc_info=True)
        body = {"ok": False, "error": "threads_posts_unavailable", "count": 0, "posts": []}
    resp = jsonify(body)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/api/threads/posts/sync", methods=["GET", "POST"])
def api_threads_posts_sync():
    """Trigger route for an external scheduler (cron-job.org, see
    docs/PUBLIC_THREADS_API.md for the recommended cadence) to keep the
    durable posts archive above up to date -- GET+POST both accepted, same
    convention as this file's other manual-trigger routes
    (/api/perps/tick and friends), since a plain scheduled HTTP hit is
    usually a GET. Requires the same CRON_SECRET bearer token every other
    trigger route here already does (see is_cron_authorized) -- this one
    pushes to HF on every real sync, so it's not left open to anonymous
    callers the way the pure-read /api/threads/posts above is. Best-effort:
    never raises, reports what happened."""
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        result = threads_client.sync_posts_archive()
        return jsonify({"ok": True, **result})
    except Exception as exc:
        logger.warning("[app_kalshi] /api/threads/posts/sync failed", exc_info=True)
        return jsonify({"ok": False, "error": str(exc)})


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


@app.route("/api/perps/threads/trending-news", methods=["GET", "POST"])
def api_perps_threads_trending_news():
    """Trigger route for an external scheduler (cron-job.org) to run the
    Threads trending-news post instead of this service's own internal
    APScheduler -- see this file's own module-level comment on moving
    non-trading-critical Threads content jobs off Render's internal
    scheduler to cut down on in-process job/executor overhead, keeping the
    scheduler focused on the trading-critical jobs (fast_check, entry_scan,
    data_collect, train). Same CRON_SECRET gate as every other trigger
    route here."""
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_perps_threads_trending_news())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/perps/threads/sentiment-snapshot", methods=["GET", "POST"])
def api_perps_threads_sentiment_snapshot():
    """Trigger route for an external scheduler -- see
    api_perps_threads_trending_news's own docstring."""
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_perps_threads_sentiment_snapshot())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/perps/threads/hourly-status", methods=["GET", "POST"])
def api_perps_threads_hourly_status():
    """Trigger route for an external scheduler -- see
    api_perps_threads_trending_news's own docstring."""
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_perps_threads_hourly_status())
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


@app.route("/api/kalshi15m/trade_analysis", methods=["GET", "POST"])
def api_kalshi_15m_trade_analysis():
    """See _run_kalshi_15m_trade_analysis's own docstring. GET for the
    dashboard's own read; POST also accepted (matching every sibling
    market's identical route) for an external scheduler to force a fresh
    run on demand -- no cron secret to check any more (see
    is_cron_authorized's own module-level note: removed entirely, this
    Space's own privacy is the access boundary)."""
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    try:
        return jsonify(_run_kalshi_15m_trade_analysis())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/kalshi15m/backtest", methods=["GET", "POST"])
def api_kalshi_15m_backtest():
    """GET returns the last SCHEDULED result (see
    KALSHI_15M_BACKTEST_HOUR_ET's own comment) -- cheap, instant,
    dashboard-safe. POST actually runs a fresh walk-forward now (a real
    cost concern -- this repo's own confirmed 90-day-backfill gunicorn-
    timeout precedent, see /api/kalshi15m/backfill's own docstring,
    applies here too) -- a deliberate, on-demand check, never something
    the dashboard should call by default."""
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    if request.method == "GET":
        return jsonify(load_json(KALSHI_15M_LATEST_BACKTEST_FILE, {"ok": False, "reason": "no_backtest_run_yet"}))
    try:
        return jsonify(_run_kalshi_15m_backtest())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/kalshi15m/strategy-sweep", methods=["GET", "POST"])
def api_kalshi_15m_strategy_sweep():
    """GET returns the last (scheduled or manually-triggered) sweep
    result -- cheap, instant, dashboard-safe. POST starts a fresh sweep
    in the BACKGROUND and returns immediately with {"ok": True,
    "started": True} -- unlike /api/kalshi15m/backtest's own synchronous
    POST, a 10,000+-combination sweep can genuinely run for many minutes
    (see strategy_sweep.run_parameter_sweep's own max_seconds, default 30
    minutes), far past gunicorn's own --timeout 300 -- see
    _run_kalshi_15m_win_streak_verification's own identical background-
    thread pattern for why this must never block the request/response
    cycle (or, worse, this cycle's own settlement/management/entry-scan
    work for every market on this shared process). _locked_job makes a
    second trigger while one is already running a safe no-op. Optional
    JSON body {"param_grid": {...}} overrides the real, ~16,200-
    combination default grid (_default_kalshi_15m_strategy_sweep_grid) --
    e.g. for a smaller, faster manual sanity check."""
    if not is_cron_authorized(request):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    if request.method == "GET":
        return jsonify(load_json(KALSHI_15M_LATEST_STRATEGY_SWEEP_FILE, {"ok": False, "reason": "no_sweep_run_yet"}))
    body = request.get_json(silent=True) or {}
    param_grid = body.get("param_grid")
    threading.Thread(
        target=_run_kalshi_15m_strategy_sweep, args=(param_grid,),
        daemon=True, name="kalshi15m-strategy-sweep",
    ).start()
    return jsonify({"ok": True, "started": True})


_JOB_LABELS = {
    "perps_fast_check": f"Fast exit check (every {PERPS_FAST_CHECK_SECONDS}s)",
    "perps_entry_scan": f"Entry scan -- all instruments (every {PERPS_CYCLE_MINUTES} min)",
    "perps_manual_cycle": "Manual full cycle",
    "perps_data_collect": f"Data collection -> HF (every {PERPS_DATA_COLLECT_MINUTES} min)",
    "kalshi_15m_data_collect": f"Kalshi 15m crypto markets data collection -> HF (every {KALSHI_15M_DATA_COLLECT_MINUTES} min)",
    "kalshi_15m_metals_data_collect": f"Kalshi 15m gold/silver/copper data collection -> HF (every {KALSHI_15M_METALS_DATA_COLLECT_MINUTES} min)",
    "kalshi_15m_cycle": f"Kalshi 15m markets settlement check + entry scan (every {KALSHI_15M_CYCLE_MINUTES} min)",
    "kalshi_15m_reconcile": f"Kalshi 15m crypto archive gap-heal, trailing {KALSHI_15M_RECONCILE_DAYS}d (daily, 30 min before training)",
    "kalshi_15m_train": f"Kalshi 15m markets model retrain, crypto + metals (daily {KALSHI_15M_TRAIN_HOUR_ET:02d}:00 ET)",
    "kalshi_15m_torch_train": f"Kalshi 15m crypto custom PyTorch MLP challenger, promoted only if it beats the current model (daily {KALSHI_15M_TORCH_TRAIN_HOUR_ET:02d}:00 ET)",
    "kalshi_15m_trade_analysis": (
        f"Kalshi 15m trade win/loss analysis (daily {KALSHI_15M_TRADE_ANALYSIS_HOUR_ET:02d}:{KALSHI_15M_TRADE_ANALYSIS_MINUTE_ET:02d} ET; "
        f"a faster, evidence-gated confidence-floor auto-tune already runs every 5 real trades)"
    ),
    "kalshi_15m_backtest": (
        f"Kalshi 15m crypto walk-forward backtest + forward test (daily {KALSHI_15M_BACKTEST_HOUR_ET:02d}:00 ET; "
        f"auto-retrains both models immediately on a confirmed losing result)"
    ),
    "kalshi_15m_strategy_sweep": (
        f"Kalshi 15m GOLD/SILVER/COPPER strategy sweep, ~16,200 parameter combinations walk-forward "
        f"backtested and ranked (weekly, {KALSHI_15M_STRATEGY_SWEEP_DAY_OF_WEEK} {KALSHI_15M_STRATEGY_SWEEP_HOUR_ET:02d}:00 ET; "
        f"reports only, never auto-applies)"
    ),
    "ai_monitor": f"Project-wide AI-powered status review (HF Inference), read-only, all 5 markets (daily {AI_MONITOR_HOUR_ET:02d}:00 ET)",
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
