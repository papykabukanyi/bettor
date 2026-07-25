"""Generic job-locking + run-history behavior shared by BOTH perps_server.py
and schwab_server.py via server_common.make_job_lock. This is the safety net
against duplicate concurrent execution of a job that can place real orders --
a second caller while the lock is held must skip immediately rather than run
in parallel, and a stale lock (crashed process) must not permanently wedge
the job. Server-specific job wiring is covered separately in
test_perps_server_jobs.py / test_schwab_server_jobs.py."""
from __future__ import annotations

import os
import time

import pytest

import server_common


@pytest.fixture
def _lock(tmp_path):
    """A fresh, isolated make_job_lock() instance per test -- mirrors how
    each real server calls make_job_lock() exactly once with its OWN paths."""
    history_file = tmp_path / "job_run_history.json"
    lock_dir = tmp_path / "locks"
    return server_common.make_job_lock(history_file, lock_dir), history_file, lock_dir


def test_locked_job_runs_and_records_success(_lock):
    locked_job, history_file, _ = _lock
    calls = []

    @locked_job("test_job")
    def _job():
        calls.append(1)
        return {"ok": True, "action": "did_something"}

    result = _job()
    assert result == {"ok": True, "action": "did_something"}
    assert len(calls) == 1

    history = server_common.load_json(history_file, [])
    assert len(history) == 1
    assert history[0]["job"] == "test_job"
    assert history[0]["status"] == "ok"


def test_locked_job_second_concurrent_call_is_skipped(_lock):
    locked_job, history_file, lock_dir = _lock

    @locked_job("test_job")
    def _job():
        return {"ok": True}

    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / "test_job.lock"
    lock_path.write_text(f"12345:{time.time()}", encoding="utf-8")

    result = _job()
    assert result["skipped"] is True
    assert result["reason"] == "already_running"

    history = server_common.load_json(history_file, [])
    assert history[-1]["status"] == "skipped_concurrent"


def test_locked_job_takes_over_a_stale_lock(_lock):
    locked_job, _, lock_dir = _lock

    @locked_job("test_job", stale_after_sec=1)
    def _job():
        return {"ok": True, "ran": True}

    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / "test_job.lock"
    old_ts = time.time() - 10
    lock_path.write_text(f"12345:{old_ts}", encoding="utf-8")
    # The takeover decision is based on the lock FILE's mtime, not the
    # timestamp embedded in its contents -- backdate the actual file.
    os.utime(lock_path, (old_ts, old_ts))

    result = _job()
    assert result.get("ran") is True


def test_locked_job_records_error_and_releases_lock(_lock):
    locked_job, history_file, lock_dir = _lock

    @locked_job("test_job")
    def _job():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        _job()

    history = server_common.load_json(history_file, [])
    assert history[-1]["status"] == "error"
    assert "boom" in history[-1]["error"]
    assert not (lock_dir / "test_job.lock").exists()


def test_is_cron_authorized_allows_everything_when_no_secret_configured(monkeypatch):
    monkeypatch.delenv("CRON_SECRET", raising=False)

    class _Req:
        headers: dict = {}

    assert server_common.is_cron_authorized(_Req()) is True


def test_is_cron_authorized_requires_matching_bearer_token(monkeypatch):
    monkeypatch.setenv("CRON_SECRET", "s3cret")

    class _Req:
        def __init__(self, auth):
            self.headers = {"authorization": auth} if auth else {}

    assert server_common.is_cron_authorized(_Req("Bearer s3cret")) is True
    assert server_common.is_cron_authorized(_Req("Bearer wrong")) is False
    assert server_common.is_cron_authorized(_Req(None)) is False
