"""Dashboard job-locking + run-history behavior. This is the safety net
against duplicate concurrent execution of a job that can place real Kalshi
orders -- a second caller while the lock is held must skip immediately
rather than run in parallel, and a stale lock (crashed process) must not
permanently wedge the job."""
from __future__ import annotations

import os
import time

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
