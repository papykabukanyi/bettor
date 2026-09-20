"""Shared, brand-agnostic web-server plumbing used by BOTH app_kalshi.py
and alpaca_server.py -- job locking/history and small JSON helpers. Nothing
in this module knows anything about Kalshi or Alpaca specifically; each
server keeps its own job-lock directory and history file (see DATA_DIR
usage at each call site) so the two processes never contend over the same
files even when run side by side locally.
"""
from __future__ import annotations

import datetime as dt
import functools
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def call_with_hard_timeout(fn, *, timeout_sec: float, on_timeout: Any = None) -> Any:
    """Runs `fn()` on a worker thread and gives up after `timeout_sec`,
    returning `on_timeout` instead of blocking forever.

    Real, confirmed production incident (Render's own logs, 9 occurrences
    in 24h on the perps service alone): every *_strategy.py's own
    `_pull_durable_state_from_hf()` calls `huggingface_hub.hf_hub_download`
    with NO timeout of its own. huggingface_hub's internal shared-session
    lock can occasionally hang for minutes (seen live: a request stuck
    inside `get_session()`'s `_CLIENT_LOCK`, not a slow HTTP response --
    ordinary `except Exception` around the call never catches a hang that
    never raises). With --workers 1, that hang froze the ENTIRE process --
    every other request AND the background scheduler -- until gunicorn's
    own worker timeout finally SIGKILLed it. A `try/except` cannot bound a
    hang; only an actual deadline on a separate thread can, which is what
    this provides.

    Does NOT (and cannot, in plain Python) forcibly kill the underlying
    thread if it's still hung when the deadline passes -- it just stops
    THIS caller from waiting on it, converting an unbounded process-wide
    freeze into a bounded, single-call degradation."""
    from concurrent.futures import ThreadPoolExecutor
    from concurrent.futures import TimeoutError as FutureTimeoutError

    executor = ThreadPoolExecutor(max_workers=1)
    try:
        future = executor.submit(fn)
        return future.result(timeout=timeout_sec)
    except FutureTimeoutError:
        logger.warning("[server_common] call_with_hard_timeout: %s exceeded %ss, giving up", getattr(fn, "__name__", fn), timeout_sec)
        return on_timeout
    finally:
        # wait=False: exiting must never itself block on the (possibly
        # still-hung) worker thread -- that would silently reintroduce the
        # exact freeze this function exists to prevent.
        executor.shutdown(wait=False)


def pull_json_from_hf(repo_id: str, filename: str, *, token: str, timeout_sec: float, repo_type: str = "model") -> Any:
    """Generic small-JSON-file pull from any HF repo -- the same shape
    threads_post.py's own private _pull_json_from_hf has used for a while
    (Threads dedup/token state), generalized here with a repo_id/token
    parameter so callers reading/writing a MARKET-SPECIFIC repo (e.g. a
    sweep/backfill/walkforward result pushed by a job that no longer runs
    on this Render service at all) can reuse one implementation instead of
    hand-rolling their own copy per market. None on any failure (missing
    file, no token, network hiccup, a hang bounded by call_with_hard_timeout
    above) -- never raises, same best-effort contract as every other
    HF-touching function in this codebase."""
    if not token:
        return None

    def _download() -> Any:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(repo_id=repo_id, filename=filename, repo_type=repo_type, token=token)
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    try:
        return call_with_hard_timeout(_download, timeout_sec=timeout_sec)
    except Exception as exc:
        logger.info("[server_common] no %s on HF repo %s yet (or fetch failed): %s", filename, repo_id, exc)
        return None


def push_json_to_hf(
    repo_id: str, filename: str, data: Any, *, token: str, timeout_sec: float,
    commit_message: str, repo_type: str = "model",
) -> None:
    """Generic small-JSON-file push to any HF repo -- see
    pull_json_from_hf's own docstring for why this is a shared helper
    rather than a per-market copy. Best-effort, never raises: a failed
    push here means the next status-route read falls back to a stale/
    cached value, not that the caller's own already-completed work is
    lost."""
    if not token:
        return
    import tempfile

    def _upload() -> None:
        from huggingface_hub import HfApi
        api = HfApi(token=token)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            json.dump(data, tmp, indent=2, default=str)
            tmp_path = tmp.name
        try:
            api.upload_file(
                path_or_fileobj=tmp_path, path_in_repo=filename,
                repo_id=repo_id, repo_type=repo_type, commit_message=commit_message,
            )
        finally:
            os.unlink(tmp_path)

    try:
        call_with_hard_timeout(_upload, timeout_sec=timeout_sec)
    except Exception as exc:
        logger.warning("[server_common] %s push to HF repo %s failed: %s", filename, repo_id, exc)


def load_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)


# Real gap found in review: `*_trade_analysis.py` on each of the 4 services
# already computes rich win/loss diagnostics (used for confidence-threshold
# auto-tuning and the downloadable PDF reports), but that number was never
# persisted anywhere a live status route could cheaply read it -- so none
# of the 4 dashboards ever showed a running win-rate stat, even though
# every trade already has realized_pnl_usd recorded. This is deliberately
# NOT that deeper analysis (no confidence calibration, no per-symbol
# breakdown) -- just a fast, live win/loss count plain enough for
# every status route to compute on every request without it mattering.
def win_rate_stats(trade_log: list[dict[str, Any]], *, recent_n: int = 50) -> dict[str, Any]:
    """`trade_log` newest-last (this codebase's own convention -- appended
    to as trades close). Returns win/loss counts + rate over the WHOLE log
    and, separately, just the most recent `recent_n` trades -- a bot that
    was profitable for its first 200 trades but has been losing for its
    last 20 should show that shift, not bury it in an all-time average.

    A row with exit_kind == "partial" (perps' USE_PARTIAL_EXIT -- selling
    only part of a still-open position to lock in gains) is real,
    informational P&L but NOT a resolved win/loss outcome, so it's excluded
    here the same way it's excluded from the evidence-gated trade-analysis
    tuning that also reads this trade_log -- otherwise one position's
    lifecycle could count as multiple independent trades. Rows without the
    field (every trade_log entry predating this, and every non-perps
    service's trade log) default to "full" so nothing regresses.

    Real, confirmed bug found via a real dashboard-numbers investigation: a
    dry-run "trade" (never touched the real account) was counted here right
    alongside real ones, understating the real win rate and trade count on
    every dashboard -- confirmed live, 5 phantom dry-run stocks trades
    dragged a real 18-trade log's win rate down. Explicit user direction:
    "we doing only real data please not dry run or fake"."""
    closed = [
        t for t in trade_log
        if isinstance(t, dict) and t.get("realized_pnl_usd") is not None
        and t.get("exit_kind", "full") == "full" and not t.get("dry_run")
    ]
    if not closed:
        return {"trade_count": 0, "win_count": 0, "win_rate": None, "recent_trade_count": 0, "recent_win_count": 0, "recent_win_rate": None}

    def _rate(trades: list[dict[str, Any]]) -> dict[str, Any]:
        wins = sum(1 for t in trades if float(t["realized_pnl_usd"]) > 0)
        return {"trade_count": len(trades), "win_count": wins, "win_rate": round(wins / len(trades), 4)}

    overall = _rate(closed)
    recent = closed[-recent_n:]
    recent_stats = _rate(recent)
    return {
        **overall,
        "recent_trade_count": recent_stats["trade_count"],
        "recent_win_count": recent_stats["win_count"],
        "recent_win_rate": recent_stats["win_rate"],
    }


# Real gap found in review: none of the 4 dashboards ever showed progress
# toward a goal -- just the current balance, in isolation, with no sense of
# "is this actually working" over time. Percentage-based (not fixed dollar
# tiers) so the SAME tier ladder means something whether the account is
# worth $70 (perps, real money) or $97,000 (Alpaca paper) -- a fixed-dollar
# milestone list would either be meaningless noise for the small account or
# take years to hit for the large one. `state` is the caller's own durable
# dict (persisted by whatever mechanism that service already uses) -- this
# function only reads/writes the one sub-key it owns and returns the
# snapshot; it never does its own I/O, matching every other function in
# this module.
MILESTONE_PCT_TIERS: list[float] = [5, 10, 25, 50, 100, 200, 500, 1000, 2500, 5000, 10000]


def milestone_snapshot(state: dict[str, Any], *, current_balance: float, key: str = "milestones") -> dict[str, Any]:
    """Tracks two things against a durable starting baseline (set once, the
    first time this is ever called for a given `state` dict, never reset):
    total return % since that baseline, and the account's own all-time
    high-water mark (so a real drawdown is visible even while total return
    is still positive -- "treat the balance seriously" means noticing a
    slide from the peak, not just whether today's number beats day one).
    `next_milestone_pct`/`pct_to_next_milestone` walk MILESTONE_PCT_TIERS to
    report the next round-number gain target still ahead."""
    milestones = state.setdefault(key, {})
    if not milestones or not milestones.get("baseline_balance"):
        milestones["baseline_balance"] = current_balance
        milestones["baseline_set_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        milestones["high_water_mark"] = current_balance
        milestones["high_water_mark_at"] = milestones["baseline_set_at"]

    baseline = float(milestones["baseline_balance"])
    if current_balance > float(milestones["high_water_mark"]):
        milestones["high_water_mark"] = current_balance
        milestones["high_water_mark_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    peak = float(milestones["high_water_mark"])

    total_return_pct = ((current_balance - baseline) / baseline) if baseline else 0.0
    drawdown_from_peak_pct = ((current_balance - peak) / peak) if peak else 0.0

    next_tier = next((t for t in MILESTONE_PCT_TIERS if t > total_return_pct * 100), None)
    prev_tier = max((t for t in MILESTONE_PCT_TIERS if t <= total_return_pct * 100), default=0)

    return {
        "baseline_balance": round(baseline, 6),
        "baseline_set_at": milestones["baseline_set_at"],
        "high_water_mark": round(peak, 6),
        "high_water_mark_at": milestones["high_water_mark_at"],
        "current_balance": round(current_balance, 6),
        "total_return_pct": round(total_return_pct, 6),
        "drawdown_from_peak_pct": round(drawdown_from_peak_pct, 6),
        "last_milestone_pct": prev_tier,
        "next_milestone_pct": next_tier,
        "pct_to_next_milestone": round(next_tier - total_return_pct * 100, 4) if next_tier is not None else None,
    }


def maybe_schedule_hf_model_recheck(
    *, refresh_state: dict[str, Any], lock: threading.Lock, recheck_interval_sec: float,
    check_fn: Callable[[], None],
) -> None:
    """Fire-and-forget, rate-limited "is there a newer model on HF?" check.

    Real, confirmed bug this exists to fix: every *_model.py's own
    load_model() only ever re-downloads a model from HF when the local
    model FILE is missing entirely -- never when a fresher one has been
    uploaded while a local copy already exists. That was harmless while
    training always happened IN-PROCESS (the trainer overwrites local
    disk directly at the end of its own run, in the same process that
    will next call load_model()) -- but once training runs somewhere else
    entirely (a scheduled Hugging Face Job, not this Render process), the
    live process would otherwise only ever pick up a new model on its
    next full restart.

    Callers: call this unconditionally on every load_model() invocation
    that already has SOMETHING cached (see each *_model.py's own call
    site) -- it self-rate-limits via `recheck_interval_sec`, so calling it
    once per prediction (potentially many times per entry_scan cycle) is
    cheap; real work only happens once per window. Never blocks the
    caller: at most spawns ONE background daemon thread per window,
    guarded by `lock` + `refresh_state["checking"]` so concurrent callers
    (entry_scan evaluating several tickers back to back) can't spawn more
    than one at a time. `check_fn` is expected to bound its own HF calls
    (e.g. via call_with_hard_timeout/pull_json_from_hf above) -- this
    function has no way to kill a hung thread once started, only to avoid
    ever waiting on one itself."""
    now = time.time()
    with lock:
        if refresh_state.get("checking") or (now - refresh_state.get("last_checked_at", 0.0)) < recheck_interval_sec:
            return
        refresh_state["checking"] = True
        refresh_state["last_checked_at"] = now

    def _runner() -> None:
        try:
            check_fn()
        except Exception as exc:
            logger.debug("[server_common] background HF model recheck failed: %s", exc)
        finally:
            with lock:
                refresh_state["checking"] = False

    threading.Thread(target=_runner, daemon=True, name="hf-model-recheck").start()


def refresh_model_if_hf_has_a_newer_one(
    *, model_repo: str, token: str, meta_filename: str, timeout_sec: float,
    current_trained_at: Any, download_fn: Callable[[], bool], invalidate_fn: Callable[[], None],
) -> None:
    """Pulls just the small `meta_filename` JSON from `model_repo` (NOT the
    full model artifact -- cheap enough to check often) and compares its
    own `trained_at` against `current_trained_at` (whatever the caller's
    in-process cache currently holds). Any DIFFERENT value is treated as
    newer -- a real retrain always changes `trained_at`, and this
    deliberately avoids assuming a comparable timestamp format so it works
    unchanged regardless of how any given market formats it. Only on a
    genuine difference does this call `download_fn()` (the caller's own
    full model+meta HF download) and, if that succeeds, `invalidate_fn()`
    (drop the caller's own in-process cache so the NEXT load_model() call
    re-reads the freshly-downloaded file off local disk instead of serving
    the stale in-memory one). A no-op -- including on any network failure
    -- leaves the caller's current model completely untouched, meant to be
    run from inside maybe_schedule_hf_model_recheck's own background
    thread, never on a caller's hot path."""
    remote_meta = pull_json_from_hf(model_repo, meta_filename, token=token, timeout_sec=timeout_sec)
    if not remote_meta:
        return
    remote_trained_at = remote_meta.get("trained_at")
    if remote_trained_at is None or remote_trained_at == current_trained_at:
        return
    if download_fn():
        invalidate_fn()
        logger.info(
            "[server_common] picked up a newer model from HF repo %s (trained_at %s -> %s)",
            model_repo, current_trained_at, remote_trained_at,
        )


def _summarize_job_result(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    keys = ("action", "ticker", "symbol", "realized_pnl_usd", "rows_written", "hf_uploaded", "rows", "model_type")
    return {k: result[k] for k in keys if k in result}


def make_job_lock(job_history_file: Path, job_lock_dir: Path, job_history_max: int = 200):
    """Returns a `_locked_job(name, stale_after_sec=600)` decorator bound to
    the given history file / lock directory -- each server calls this once
    with its OWN paths, so app_kalshi.py and alpaca_server.py never share a
    lock directory or history file even though the locking LOGIC is
    identical."""

    def _append_job_history(name: str, record: dict[str, Any]) -> None:
        try:
            history = load_json(job_history_file, [])
            if not isinstance(history, list):
                history = []
            history.append({"job": name, **record})
            history = history[-job_history_max:]
            save_json(job_history_file, history)
        except Exception as exc:
            logger.debug("job history append failed for %s: %s", name, exc)

    def _locked_job(name: str, stale_after_sec: int = 600):
        """Only one process-wide caller of this job runs at a time. A second
        caller while the lock is held skips immediately rather than blocking
        or running in parallel -- important once a job can place real orders."""
        def decorator(fn):
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                job_lock_dir.mkdir(parents=True, exist_ok=True)
                lock_path = job_lock_dir / f"{name}.lock"
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

    return _locked_job


def is_cron_authorized(request, secret_env_var: str = "CRON_SECRET") -> bool:
    """A caller matching EITHER form is authorized -- see the X-Cron-Secret
    branch's own comment for why both need to exist.

    Real, live-confirmed structural bug this closes: once this process
    moved onto a PRIVATE Hugging Face Space (see docs/RENDER_TO_HF_
    MIGRATION.md), HF's OWN edge/proxy gate for that Space's direct
    `.hf.space` domain inspects the SAME `Authorization` header this
    function used to be the only consumer of -- confirmed live: any
    Authorization header that isn't a real HF access token (including
    literally "Bearer <CRON_SECRET>") gets rejected at HF's OWN edge with
    HF's own branded 404 page, never reaching this app (or this function)
    at all; no Authorization header at all is rejected the same way. A
    caller therefore could never pass BOTH gates with only one
    Authorization header (HF's gate needs a real HF token in it; this
    function needed the exact CRON_SECRET value in it instead) --
    cron-job.org and any plain curl caller using only CRON_SECRET has
    been silently unable to reach ANY cron-gated route on this Space
    since it went private, whether or not this function's own check was
    otherwise correct. `X-Cron-Secret: <value>` lets a caller send the
    real HF token via Authorization (for HF's gate) and the app's own
    secret via this separate header (for this check) in the same
    request, with zero change to the original Authorization-header form
    for anywhere that ISN'T behind that same gate (e.g. a future public
    Space, or a local dev run)."""
    secret = str(os.getenv(secret_env_var, "") or "").strip()
    if not secret:
        return True
    auth = str(request.headers.get("authorization") or "")
    if auth == f"Bearer {secret}":
        return True
    header_secret = str(request.headers.get("x-cron-secret") or "").strip()
    return header_secret == secret


# Lightweight abuse guard for the PUBLIC read surface (dashboard page +
# /api/status, /api/trades, /api/server/activity, ...) once a dashboard
# link is actually shared -- every route that can CHANGE anything already
# requires is_cron_authorized (a real secret), so this exists only to keep
# the single gunicorn worker (--workers 1 --threads 1 on every service
# here) responsive for legitimate viewers if the link gets scraped or
# hammered, not as a security boundary of its own. Plain in-memory sliding
# window, safe under the confirmed single-process/single-worker deployment
# this whole codebase already depends on elsewhere (e.g. every
# module-level cache in app_kalshi.py) -- would need a shared store
# (Redis, etc.) instead if a service here ever moves to >1 worker/instance.
_RATE_LIMIT_WINDOWS: dict[str, list[float]] = {}
_RATE_LIMIT_LOCK = threading.Lock()
_RATE_LIMIT_MAX_TRACKED_IPS = 5000  # bounds memory if scraped by many distinct IPs at once


def check_rate_limit(client_ip: str, *, max_requests: int = 120, window_sec: float = 60.0) -> bool:
    """True if `client_ip` is within its allowance this window, False if
    the caller should respond 429. Default (120 requests/60s per IP) is
    generous for a real human with the dashboard open -- the page itself
    polls 3 endpoints every 10s, i.e. ~18 requests/min per genuine viewer,
    well under this ceiling even across a few open tabs."""
    now = time.monotonic()
    with _RATE_LIMIT_LOCK:
        if len(_RATE_LIMIT_WINDOWS) > _RATE_LIMIT_MAX_TRACKED_IPS:
            _RATE_LIMIT_WINDOWS.clear()
        timestamps = _RATE_LIMIT_WINDOWS.setdefault(client_ip, [])
        cutoff = now - window_sec
        while timestamps and timestamps[0] < cutoff:
            timestamps.pop(0)
        if len(timestamps) >= max_requests:
            return False
        timestamps.append(now)
        return True
