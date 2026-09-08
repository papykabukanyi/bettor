"""Generic job-locking + run-history behavior shared by BOTH app_kalshi.py
and alpaca_server.py via server_common.make_job_lock. This is the safety net
against duplicate concurrent execution of a job that can place real orders --
a second caller while the lock is held must skip immediately rather than run
in parallel, and a stale lock (crashed process) must not permanently wedge
the job. Server-specific job wiring is covered separately in
test_app_kalshi_jobs.py / test_alpaca_server_jobs.py."""
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


def test_call_with_hard_timeout_returns_the_function_result_when_fast_enough():
    assert server_common.call_with_hard_timeout(lambda: 42, timeout_sec=2) == 42


def test_call_with_hard_timeout_returns_the_fallback_when_the_deadline_passes():
    """Real, confirmed production incident this locks in: an unbounded
    huggingface_hub call hung long enough (its own internal session lock,
    not a slow response) to freeze an entire --workers 1 process for
    minutes, 9 times in 24h, until gunicorn's own timeout finally killed
    it. A plain try/except can't catch a hang that never raises -- only a
    real deadline on a separate thread, proven here, actually bounds it."""
    def _hangs_forever():
        time.sleep(30)
        return "should never get here"

    start = time.monotonic()
    result = server_common.call_with_hard_timeout(_hangs_forever, timeout_sec=0.2, on_timeout="gave_up")
    elapsed = time.monotonic() - start

    assert result == "gave_up"
    assert elapsed < 5  # must return promptly, not wait out the full 30s hang


def test_call_with_hard_timeout_propagates_a_real_exception_from_the_function():
    def _raises():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        server_common.call_with_hard_timeout(_raises, timeout_sec=2)


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


@pytest.fixture(autouse=True)
def _reset_rate_limit_state():
    """Module-level dict, shared across tests unless reset -- same reasoning
    as every other module-global-cache test fixture in this repo."""
    server_common._RATE_LIMIT_WINDOWS.clear()  # noqa: SLF001
    yield
    server_common._RATE_LIMIT_WINDOWS.clear()  # noqa: SLF001


def test_check_rate_limit_allows_requests_under_the_cap():
    for _ in range(5):
        assert server_common.check_rate_limit("1.2.3.4", max_requests=5, window_sec=60.0) is True


def test_check_rate_limit_blocks_once_the_cap_is_reached():
    for _ in range(5):
        server_common.check_rate_limit("1.2.3.4", max_requests=5, window_sec=60.0)
    assert server_common.check_rate_limit("1.2.3.4", max_requests=5, window_sec=60.0) is False


def test_check_rate_limit_tracks_each_ip_independently():
    for _ in range(5):
        server_common.check_rate_limit("1.2.3.4", max_requests=5, window_sec=60.0)
    # A different IP has its own, fresh allowance.
    assert server_common.check_rate_limit("5.6.7.8", max_requests=5, window_sec=60.0) is True


def test_check_rate_limit_allows_again_once_the_window_elapses(monkeypatch):
    fake_now = [1000.0]
    monkeypatch.setattr(server_common.time, "monotonic", lambda: fake_now[0])
    for _ in range(5):
        server_common.check_rate_limit("1.2.3.4", max_requests=5, window_sec=60.0)
    assert server_common.check_rate_limit("1.2.3.4", max_requests=5, window_sec=60.0) is False

    fake_now[0] += 61.0  # past the window
    assert server_common.check_rate_limit("1.2.3.4", max_requests=5, window_sec=60.0) is True


def test_check_rate_limit_clears_all_state_past_the_tracked_ip_ceiling(monkeypatch):
    monkeypatch.setattr(server_common, "_RATE_LIMIT_MAX_TRACKED_IPS", 3)
    for i in range(4):
        server_common.check_rate_limit(f"ip-{i}", max_requests=100, window_sec=60.0)
    # The size check runs BEFORE each insert, so the dict is allowed to grow
    # to exactly one past the ceiling (4) before the NEXT call sees a size
    # that's actually over the limit and clears -- the whole dict resets
    # rather than growing unbounded under a scrape/flood from many IPs.
    assert len(server_common._RATE_LIMIT_WINDOWS) == 4  # noqa: SLF001
    server_common.check_rate_limit("ip-new", max_requests=100, window_sec=60.0)
    assert len(server_common._RATE_LIMIT_WINDOWS) == 1  # noqa: SLF001


def _trade(pnl):
    return {"realized_pnl_usd": pnl}


def test_win_rate_stats_empty_log():
    stats = server_common.win_rate_stats([])
    assert stats == {
        "trade_count": 0, "win_count": 0, "win_rate": None,
        "recent_trade_count": 0, "recent_win_count": 0, "recent_win_rate": None,
    }


def test_win_rate_stats_counts_wins_and_losses():
    """Real gap found in review: *_trade_analysis.py already computes rich
    win/loss diagnostics, but that number was never persisted anywhere a
    live status route could cheaply read it -- none of the 4 dashboards
    ever showed a running win-rate stat."""
    trades = [_trade(10.0), _trade(-5.0), _trade(3.0), _trade(0.0), _trade(-1.0)]
    stats = server_common.win_rate_stats(trades)
    assert stats["trade_count"] == 5
    assert stats["win_count"] == 2  # a $0.00 trade is neither a win nor a loss
    assert stats["win_rate"] == pytest.approx(0.4)


def test_win_rate_stats_recent_window_can_differ_from_all_time():
    """A bot profitable for its first 200 trades but losing its last 20
    should show that shift, not bury it in an all-time average."""
    old_wins = [_trade(1.0) for _ in range(20)]
    recent_losses = [_trade(-1.0) for _ in range(5)]
    stats = server_common.win_rate_stats(old_wins + recent_losses, recent_n=5)
    assert stats["win_rate"] == pytest.approx(20 / 25)
    assert stats["recent_trade_count"] == 5
    assert stats["recent_win_count"] == 0
    assert stats["recent_win_rate"] == pytest.approx(0.0)


def test_win_rate_stats_ignores_trades_with_no_realized_pnl_yet():
    trades = [_trade(10.0), {"symbol": "AAPL"}, {"realized_pnl_usd": None}]
    stats = server_common.win_rate_stats(trades)
    assert stats["trade_count"] == 1
    assert stats["win_count"] == 1


def test_win_rate_stats_excludes_partial_exit_rows():
    """A "partial" row (perps' USE_PARTIAL_EXIT) is real P&L on a position
    that's still open, not a resolved win/loss -- must not be counted as
    an independent trade outcome. Rows without exit_kind (every trade log
    predating this feature) default to "full" and are counted exactly as
    before."""
    trades = [
        {**_trade(10.0), "exit_kind": "full"},
        {**_trade(3.0), "exit_kind": "partial"},
        {**_trade(-2.0), "exit_kind": "partial"},
        _trade(-1.0),  # no exit_kind at all -- defaults to "full"
    ]
    stats = server_common.win_rate_stats(trades)
    assert stats["trade_count"] == 2
    assert stats["win_count"] == 1


def test_milestone_snapshot_sets_baseline_on_first_call():
    state = {}
    snap = server_common.milestone_snapshot(state, current_balance=100.0)
    assert snap["baseline_balance"] == 100.0
    assert snap["high_water_mark"] == 100.0
    assert snap["total_return_pct"] == pytest.approx(0.0)
    assert snap["next_milestone_pct"] == 5


def test_milestone_snapshot_baseline_persists_across_calls():
    """Real gap found in review: a naive re-derivation of "baseline" from
    the current balance every call would never let a gain accumulate --
    the baseline must be set ONCE and never move, matching the same
    durable-state discipline as daily_reference_balance elsewhere."""
    state = {}
    server_common.milestone_snapshot(state, current_balance=100.0)
    snap = server_common.milestone_snapshot(state, current_balance=110.0)
    assert snap["baseline_balance"] == 100.0
    assert snap["total_return_pct"] == pytest.approx(0.10)
    assert snap["last_milestone_pct"] == 10
    assert snap["next_milestone_pct"] == 25
    assert snap["pct_to_next_milestone"] == pytest.approx(15.0)


def test_milestone_snapshot_tracks_high_water_mark_through_a_drawdown():
    """A real drawdown from the peak must stay visible even while total
    return is still positive -- "treat the balance seriously" means
    noticing a slide from the peak, not just today vs day one."""
    state = {}
    server_common.milestone_snapshot(state, current_balance=100.0)
    server_common.milestone_snapshot(state, current_balance=150.0)
    snap = server_common.milestone_snapshot(state, current_balance=120.0)
    assert snap["high_water_mark"] == 150.0
    assert snap["total_return_pct"] == pytest.approx(0.20)
    assert snap["drawdown_from_peak_pct"] == pytest.approx((120.0 - 150.0) / 150.0)


def test_milestone_snapshot_next_milestone_is_none_past_the_last_tier():
    state = {}
    snap = server_common.milestone_snapshot(state, current_balance=100.0)
    snap = server_common.milestone_snapshot(state, current_balance=100.0 * 101)  # +10,000%
    assert snap["next_milestone_pct"] is None
    assert snap["pct_to_next_milestone"] is None
    assert snap["last_milestone_pct"] == 10000


def test_milestone_snapshot_zero_baseline_does_not_divide_by_zero():
    state = {}
    snap = server_common.milestone_snapshot(state, current_balance=0.0)
    assert snap["total_return_pct"] == 0.0
    snap = server_common.milestone_snapshot(state, current_balance=5.0)
    assert snap["drawdown_from_peak_pct"] == 0.0


# ── pull_json_from_hf / push_json_to_hf ──────────────────────────────────

def test_pull_json_from_hf_returns_none_without_a_token():
    assert server_common.pull_json_from_hf("some/repo", "f.json", token="", timeout_sec=5) is None


def test_pull_json_from_hf_downloads_and_parses_json(monkeypatch, tmp_path):
    import huggingface_hub

    payload_path = tmp_path / "f.json"
    payload_path.write_text('{"a": 1}', encoding="utf-8")
    captured = {}

    def fake_hf_hub_download(*, repo_id, filename, repo_type, token):
        captured.update(repo_id=repo_id, filename=filename, repo_type=repo_type, token=token)
        return str(payload_path)

    monkeypatch.setattr(huggingface_hub, "hf_hub_download", fake_hf_hub_download)
    result = server_common.pull_json_from_hf("papylove/alpaca-model", "f.json", token="tok", timeout_sec=5)
    assert result == {"a": 1}
    assert captured == {"repo_id": "papylove/alpaca-model", "filename": "f.json", "repo_type": "model", "token": "tok"}


def test_pull_json_from_hf_returns_none_on_failure(monkeypatch):
    import huggingface_hub

    def raise_error(**kw):
        raise RuntimeError("not found")

    monkeypatch.setattr(huggingface_hub, "hf_hub_download", raise_error)
    assert server_common.pull_json_from_hf("some/repo", "f.json", token="tok", timeout_sec=5) is None


def test_push_json_to_hf_is_a_noop_without_a_token(monkeypatch):
    import huggingface_hub

    def fail_if_called(*a, **k):
        raise AssertionError("must not touch HF at all without a token")

    monkeypatch.setattr(huggingface_hub, "HfApi", fail_if_called)
    server_common.push_json_to_hf("some/repo", "f.json", {"a": 1}, token="", timeout_sec=5, commit_message="x")


def test_push_json_to_hf_uploads_the_data(monkeypatch):
    import huggingface_hub

    captured = {}

    class _FakeApi:
        def __init__(self, token):
            captured["token"] = token

        def upload_file(self, *, path_or_fileobj, path_in_repo, repo_id, repo_type, commit_message):
            import json
            with open(path_or_fileobj, encoding="utf-8") as f:
                captured["uploaded"] = json.load(f)
            captured.update(path_in_repo=path_in_repo, repo_id=repo_id, repo_type=repo_type, commit_message=commit_message)

    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeApi)
    server_common.push_json_to_hf(
        "papylove/alpaca-model", "latest_sweep.json", {"best": "config"},
        token="tok", timeout_sec=5, commit_message="update sweep",
    )
    assert captured == {
        "token": "tok", "uploaded": {"best": "config"}, "path_in_repo": "latest_sweep.json",
        "repo_id": "papylove/alpaca-model", "repo_type": "model", "commit_message": "update sweep",
    }


def test_push_json_to_hf_never_raises_on_failure(monkeypatch):
    import huggingface_hub

    def raise_error(token):
        raise RuntimeError("HF is down")

    monkeypatch.setattr(huggingface_hub, "HfApi", raise_error)
    server_common.push_json_to_hf("some/repo", "f.json", {"a": 1}, token="tok", timeout_sec=5, commit_message="x")


# ── maybe_schedule_hf_model_recheck / refresh_model_if_hf_has_a_newer_one ─

def test_maybe_schedule_hf_model_recheck_fires_the_check_fn_once():
    refresh_state = {"last_checked_at": 0.0, "checking": False}
    lock = server_common.threading.Lock()
    fired = server_common.threading.Event()

    def check_fn():
        fired.set()

    server_common.maybe_schedule_hf_model_recheck(refresh_state=refresh_state, lock=lock, recheck_interval_sec=600, check_fn=check_fn)
    assert fired.wait(timeout=2)


def test_maybe_schedule_hf_model_recheck_respects_the_rate_limit_window():
    refresh_state = {"last_checked_at": time.time(), "checking": False}
    lock = server_common.threading.Lock()
    calls = []

    server_common.maybe_schedule_hf_model_recheck(
        refresh_state=refresh_state, lock=lock, recheck_interval_sec=600, check_fn=lambda: calls.append(1),
    )
    time.sleep(0.1)
    assert calls == []  # still well within the window -- must not have fired


def test_maybe_schedule_hf_model_recheck_does_not_spawn_a_second_thread_while_one_is_in_flight():
    refresh_state = {"last_checked_at": 0.0, "checking": False}
    lock = server_common.threading.Lock()
    started = server_common.threading.Event()
    release = server_common.threading.Event()
    call_count = {"n": 0}

    def slow_check_fn():
        call_count["n"] += 1
        started.set()
        release.wait(timeout=2)

    server_common.maybe_schedule_hf_model_recheck(refresh_state=refresh_state, lock=lock, recheck_interval_sec=600, check_fn=slow_check_fn)
    assert started.wait(timeout=2)
    # A second call while the first check is still running (and well within
    # the rate-limit window either way) must not spawn a second thread.
    server_common.maybe_schedule_hf_model_recheck(refresh_state=refresh_state, lock=lock, recheck_interval_sec=600, check_fn=slow_check_fn)
    release.set()
    time.sleep(0.1)
    assert call_count["n"] == 1


def test_maybe_schedule_hf_model_recheck_resets_checking_flag_even_if_check_fn_raises():
    refresh_state = {"last_checked_at": 0.0, "checking": False}
    lock = server_common.threading.Lock()
    done = server_common.threading.Event()

    def raise_and_signal():
        done.set()
        raise RuntimeError("boom")

    server_common.maybe_schedule_hf_model_recheck(refresh_state=refresh_state, lock=lock, recheck_interval_sec=600, check_fn=raise_and_signal)
    assert done.wait(timeout=2)
    time.sleep(0.05)  # let the runner's finally block actually execute
    assert refresh_state["checking"] is False


def test_refresh_model_if_hf_has_a_newer_one_downloads_and_invalidates_on_a_different_trained_at(monkeypatch):
    monkeypatch.setattr(server_common, "pull_json_from_hf", lambda *a, **k: {"trained_at": "2026-02-01T00:00:00+00:00"})
    calls = {"downloaded": False, "invalidated": False}
    server_common.refresh_model_if_hf_has_a_newer_one(
        model_repo="papylove/alpaca-model", token="tok", meta_filename="meta.json", timeout_sec=5,
        current_trained_at="2026-01-01T00:00:00+00:00",
        download_fn=lambda: calls.update(downloaded=True) or True,
        invalidate_fn=lambda: calls.update(invalidated=True),
    )
    assert calls == {"downloaded": True, "invalidated": True}


def test_refresh_model_if_hf_has_a_newer_one_is_a_noop_when_trained_at_matches(monkeypatch):
    monkeypatch.setattr(server_common, "pull_json_from_hf", lambda *a, **k: {"trained_at": "2026-01-01T00:00:00+00:00"})

    def fail_if_called():
        raise AssertionError("must not download when trained_at is unchanged")

    server_common.refresh_model_if_hf_has_a_newer_one(
        model_repo="papylove/alpaca-model", token="tok", meta_filename="meta.json", timeout_sec=5,
        current_trained_at="2026-01-01T00:00:00+00:00", download_fn=fail_if_called, invalidate_fn=fail_if_called,
    )


def test_refresh_model_if_hf_has_a_newer_one_is_a_noop_when_remote_meta_is_missing(monkeypatch):
    monkeypatch.setattr(server_common, "pull_json_from_hf", lambda *a, **k: None)

    def fail_if_called():
        raise AssertionError("must not download without real remote meta")

    server_common.refresh_model_if_hf_has_a_newer_one(
        model_repo="papylove/alpaca-model", token="tok", meta_filename="meta.json", timeout_sec=5,
        current_trained_at="2026-01-01T00:00:00+00:00", download_fn=fail_if_called, invalidate_fn=fail_if_called,
    )


def test_refresh_model_if_hf_has_a_newer_one_does_not_invalidate_if_download_fails(monkeypatch):
    monkeypatch.setattr(server_common, "pull_json_from_hf", lambda *a, **k: {"trained_at": "2026-02-01T00:00:00+00:00"})

    def fail_if_called():
        raise AssertionError("must not invalidate the cache when the download itself failed")

    server_common.refresh_model_if_hf_has_a_newer_one(
        model_repo="papylove/alpaca-model", token="tok", meta_filename="meta.json", timeout_sec=5,
        current_trained_at="2026-01-01T00:00:00+00:00", download_fn=lambda: False, invalidate_fn=fail_if_called,
    )
