"""Cross-process job locking and stale-data suppression: the exact bug behind
"Dashboard scheduler started" appearing twice and every job running 2-3x
concurrently in production logs. A second caller while a job is "running"
must skip immediately (not block, not run in parallel), and a payload whose
own "today" field is definitively in the past must never be served as if it
were current."""
from __future__ import annotations

import time

import pytest


@pytest.fixture
def dashboard_module(monkeypatch, tmp_path):
    import dashboard as dash

    monkeypatch.setattr(dash, "DATA_DIR", tmp_path)
    monkeypatch.setattr(dash, "JOB_LOCK_DIR", tmp_path / "locks")
    monkeypatch.setattr(dash, "JOB_HISTORY_FILE", tmp_path / "job_run_history.json")
    return dash


def test_second_concurrent_caller_skips_instead_of_running_in_parallel(dashboard_module):
    call_count = {"n": 0}

    @dashboard_module._locked_job("test_job", stale_after_sec=60)
    def slow_job():
        call_count["n"] += 1
        time.sleep(0.3)
        return {"ok": True}

    results = []
    import threading
    threads = [threading.Thread(target=lambda: results.append(slow_job())) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert call_count["n"] == 1, "only one of the 3 concurrent callers should have actually executed the job"
    skipped = [r for r in results if r.get("skipped")]
    assert len(skipped) == 2


def test_stale_lock_is_taken_over_not_permanently_stuck(dashboard_module):
    lock_dir = dashboard_module.JOB_LOCK_DIR
    lock_dir.mkdir(parents=True, exist_ok=True)
    stale_lock = lock_dir / "old_job.lock"
    stale_lock.write_text("99999:0")  # pid that can't exist, timestamp far in the past
    import os
    old_time = time.time() - 3600
    os.utime(stale_lock, (old_time, old_time))

    @dashboard_module._locked_job("old_job", stale_after_sec=60)
    def job():
        return {"ok": True}

    result = job()
    assert result == {"ok": True}, "a lock older than stale_after_sec must be taken over, not block forever"


def test_job_history_is_recorded(dashboard_module):
    @dashboard_module._locked_job("history_job", stale_after_sec=60)
    def job():
        return {"ok": True, "records": 5}

    job()
    history = dashboard_module._load_json(dashboard_module.JOB_HISTORY_FILE, [])
    assert len(history) == 1
    assert history[0]["job"] == "history_job"
    assert history[0]["status"] == "ok"
    assert history[0]["summary"]["records"] == 5


def test_predictions_payload_from_two_weeks_ago_is_treated_as_stale(dashboard_module):
    old_payload = {"today": "2026-01-01", "predictions": [{"game_date": "2026-01-01"}]}
    assert dashboard_module._is_predictions_payload_stale("/predictions/today", old_payload) is True


def test_fresh_predictions_payload_is_not_stale(dashboard_module):
    import datetime
    fresh_payload = {"today": datetime.date.today().isoformat(), "predictions": []}
    assert dashboard_module._is_predictions_payload_stale("/predictions/today", fresh_payload) is False
