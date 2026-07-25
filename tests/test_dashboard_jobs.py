"""Dashboard job-locking + run-history behavior. This is the safety net
against duplicate concurrent execution of a job that can place real Kalshi
orders -- a second caller while the lock is held must skip immediately
rather than run in parallel, and a stale lock (crashed process) must not
permanently wedge the job."""
from __future__ import annotations

import os
import time

import pandas as pd
import pytest

import dashboard


@pytest.fixture(autouse=True)
def _isolated_job_dirs(tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard, "JOB_LOCK_DIR", tmp_path / "locks")
    monkeypatch.setattr(dashboard, "JOB_HISTORY_FILE", tmp_path / "job_run_history.json")
    yield


def test_locked_job_runs_and_records_success():
    calls = []

    @dashboard._locked_job("test_job")  # noqa: SLF001
    def _job():
        calls.append(1)
        return {"ok": True, "action": "did_something"}

    result = _job()
    assert result == {"ok": True, "action": "did_something"}
    assert len(calls) == 1

    history = dashboard._load_json(dashboard.JOB_HISTORY_FILE, [])  # noqa: SLF001
    assert len(history) == 1
    assert history[0]["job"] == "test_job"
    assert history[0]["status"] == "ok"


def test_locked_job_second_concurrent_call_is_skipped():
    @dashboard._locked_job("test_job")  # noqa: SLF001
    def _job():
        return {"ok": True}

    dashboard.JOB_LOCK_DIR.mkdir(parents=True, exist_ok=True)
    lock_path = dashboard.JOB_LOCK_DIR / "test_job.lock"
    lock_path.write_text(f"12345:{time.time()}", encoding="utf-8")

    result = _job()
    assert result["skipped"] is True
    assert result["reason"] == "already_running"

    history = dashboard._load_json(dashboard.JOB_HISTORY_FILE, [])  # noqa: SLF001
    assert history[-1]["status"] == "skipped_concurrent"


def test_locked_job_takes_over_a_stale_lock():
    @dashboard._locked_job("test_job", stale_after_sec=1)  # noqa: SLF001
    def _job():
        return {"ok": True, "ran": True}

    dashboard.JOB_LOCK_DIR.mkdir(parents=True, exist_ok=True)
    lock_path = dashboard.JOB_LOCK_DIR / "test_job.lock"
    old_ts = time.time() - 10
    lock_path.write_text(f"12345:{old_ts}", encoding="utf-8")
    # The takeover decision is based on the lock FILE's mtime, not the
    # timestamp embedded in its contents -- backdate the actual file.
    os.utime(lock_path, (old_ts, old_ts))

    result = _job()
    assert result.get("ran") is True


def test_locked_job_records_error_and_releases_lock():
    @dashboard._locked_job("test_job")  # noqa: SLF001
    def _job():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        _job()

    history = dashboard._load_json(dashboard.JOB_HISTORY_FILE, [])  # noqa: SLF001
    assert history[-1]["status"] == "error"
    assert "boom" in history[-1]["error"]
    assert not (dashboard.JOB_LOCK_DIR / "test_job.lock").exists()


def test_production_jobs_actually_honor_the_live_trading_flag(monkeypatch):
    """perps_strategy's dry_run default is safe-by-default (None -> True)
    specifically so ad-hoc/manual callers never go live by accident -- but
    that means the REAL production scheduler must explicitly pass
    dry_run=False, or KALSHI_PERPS_LIVE_TRADING_ENABLED=1 would silently do
    nothing forever. Lock in that the three production entry points
    (the two scheduled jobs + the manual tick handler) all pass it."""
    from data import perps_strategy as strat

    captured = {}
    monkeypatch.setattr(strat, "manage_open_positions", lambda **kw: captured.setdefault("fast_check", kw) or {"action": "no_position"})
    monkeypatch.setattr(strat, "scan_and_enter", lambda **kw: captured.setdefault("entry_scan", kw) or {"action": "none"})
    monkeypatch.setattr(strat, "run_cycle", lambda **kw: captured.setdefault("manual_cycle", kw) or {})

    dashboard._run_perps_fast_check.__wrapped__()  # noqa: SLF001
    dashboard._run_perps_entry_scan.__wrapped__()  # noqa: SLF001
    dashboard._run_perps_manual_cycle.__wrapped__()  # noqa: SLF001

    assert captured["fast_check"].get("dry_run") is False
    assert captured["entry_scan"].get("dry_run") is False
    assert captured["manual_cycle"].get("dry_run") is False


def test_data_collect_job_refreshes_ticker_activity_cache_off_the_request_path(monkeypatch):
    """The volatility-ranking cache must only ever be refreshed from here
    (a scheduled background job) -- confirmed live that refreshing it
    inline from /api/status caused a fresh Render OOM, since that request
    path could run concurrently with the startup training thread's own
    full-size archive load right when memory is already tightest."""
    from data import perps_data

    calls = []
    monkeypatch.setattr(perps_data, "refresh_ticker_activity_cache", lambda **kw: calls.append(kw))
    monkeypatch.setattr(perps_data, "collect_dataset_rows", lambda: pd.DataFrame())

    dashboard._run_perps_data_collect.__wrapped__()  # noqa: SLF001

    assert len(calls) == 1


def test_data_collect_job_still_collects_if_cache_refresh_fails(monkeypatch):
    from data import perps_data

    def fail():
        raise RuntimeError("HF archive listing failed")

    collected = []
    monkeypatch.setattr(perps_data, "refresh_ticker_activity_cache", fail)
    monkeypatch.setattr(perps_data, "collect_dataset_rows", lambda: collected.append(1) or pd.DataFrame())

    dashboard._run_perps_data_collect.__wrapped__()  # noqa: SLF001

    assert collected == [1]


class _FakeScheduler:
    def __init__(self, running, shutdown_fn=None):
        self.running = running
        self._shutdown_fn = shutdown_fn or (lambda **kw: None)

    def shutdown(self, **kw):
        return self._shutdown_fn(**kw)


def test_shutdown_scheduler_stops_a_running_scheduler(monkeypatch):
    """Confirmed live: on SIGTERM (a normal restart/redeploy), APScheduler's
    own background thread could still be mid-cycle and try to submit a job
    to its thread pool right as the interpreter tears it down, raising
    "cannot schedule new futures after interpreter shutdown". Shutting the
    scheduler down at exit prevents that race."""
    calls = []
    monkeypatch.setattr(dashboard, "scheduler", _FakeScheduler(True, lambda **kw: calls.append(kw)))
    dashboard._shutdown_scheduler()
    assert calls == [{"wait": False}]


def test_shutdown_scheduler_is_a_noop_when_not_running(monkeypatch):
    def fail_if_called(**kw):
        raise AssertionError("must not call shutdown() on a scheduler that isn't running")

    monkeypatch.setattr(dashboard, "scheduler", _FakeScheduler(False, fail_if_called))
    dashboard._shutdown_scheduler()  # must not raise


def test_shutdown_scheduler_swallows_errors(monkeypatch):
    """This runs at interpreter shutdown -- it must never itself raise and
    block/interfere with the process actually exiting."""
    def raise_error(**kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(dashboard, "scheduler", _FakeScheduler(True, raise_error))
    dashboard._shutdown_scheduler()  # must not raise
