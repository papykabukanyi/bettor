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
import time
from pathlib import Path
from typing import Any

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
    secret = str(os.getenv(secret_env_var, "") or "").strip()
    if not secret:
        return True
    auth = str(request.headers.get("authorization") or "")
    return auth == f"Bearer {secret}"
