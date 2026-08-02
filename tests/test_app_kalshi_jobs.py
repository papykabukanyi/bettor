"""Perps-server-specific job wiring + scheduler shutdown behavior. Generic
job-locking mechanics (shared with alpaca_server.py) are covered in
test_server_common.py instead -- this file only tests things that are
actually specific to app_kalshi.py: that its production job functions
honor the live-trading dry_run gate, that its data-collect job refreshes the
volatility-ranking cache off the request path, and that its scheduler
shutdown handler is safe."""
from __future__ import annotations

import pandas as pd
import pytest

import app_kalshi


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

    app_kalshi._run_perps_fast_check.__wrapped__()  # noqa: SLF001
    app_kalshi._run_perps_entry_scan.__wrapped__()  # noqa: SLF001
    app_kalshi._run_perps_manual_cycle.__wrapped__()  # noqa: SLF001

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

    app_kalshi._run_perps_data_collect.__wrapped__()  # noqa: SLF001

    assert len(calls) == 1


def test_data_collect_job_still_collects_if_cache_refresh_fails(monkeypatch):
    from data import perps_data

    def fail():
        raise RuntimeError("HF archive listing failed")

    collected = []
    monkeypatch.setattr(perps_data, "refresh_ticker_activity_cache", fail)
    monkeypatch.setattr(perps_data, "collect_dataset_rows", lambda: collected.append(1) or pd.DataFrame())

    app_kalshi._run_perps_data_collect.__wrapped__()  # noqa: SLF001

    assert collected == [1]


def test_threads_hourly_status_job_reports_open_positions_with_held_minutes(monkeypatch):
    import datetime as dt
    from data import perps_strategy as strat, threads_post

    opened_at = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=30)).isoformat()
    monkeypatch.setattr(strat, "_load_state", lambda: {
        "positions": [{"ticker": "KXBTCPERP", "side": "long", "entry_price": 6.5, "opened_at": opened_at}],
        "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(strat, "position_exit_levels", lambda p: {"take_profit_price": 6.6, "stop_loss_price": 6.4})
    captured = {}
    monkeypatch.setattr(threads_post, "post_hourly_status", lambda **kw: captured.update(kw) or True)

    result = app_kalshi._run_perps_threads_hourly_status.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert result["open_position_count"] == 1
    assert captured["positions"][0]["ticker"] == "KXBTCPERP"
    assert captured["positions"][0]["take_profit_price"] == 6.6
    assert captured["positions"][0]["held_minutes"] == pytest.approx(30.0, abs=1.0)


def test_threads_hourly_status_job_reports_flat_with_no_positions(monkeypatch):
    from data import perps_strategy as strat, threads_post

    monkeypatch.setattr(strat, "_load_state", lambda: {"positions": [], "realized_pnl_by_date": {}})
    captured = {}
    monkeypatch.setattr(threads_post, "post_hourly_status", lambda **kw: captured.update(kw) or True)

    result = app_kalshi._run_perps_threads_hourly_status.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert result["open_position_count"] == 0
    assert captured["positions"] == []


def test_threads_hourly_status_job_never_raises_on_failure(monkeypatch):
    from data import perps_strategy as strat

    def raise_error():
        raise RuntimeError("state file corrupted")

    monkeypatch.setattr(strat, "_load_state", raise_error)
    result = app_kalshi._run_perps_threads_hourly_status.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


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
    monkeypatch.setattr(app_kalshi, "scheduler", _FakeScheduler(True, lambda **kw: calls.append(kw)))
    app_kalshi._shutdown_scheduler()
    assert calls == [{"wait": False}]


def test_shutdown_scheduler_is_a_noop_when_not_running(monkeypatch):
    def fail_if_called(**kw):
        raise AssertionError("must not call shutdown() on a scheduler that isn't running")

    monkeypatch.setattr(app_kalshi, "scheduler", _FakeScheduler(False, fail_if_called))
    app_kalshi._shutdown_scheduler()  # must not raise


def test_shutdown_scheduler_swallows_errors(monkeypatch):
    """This runs at interpreter shutdown -- it must never itself raise and
    block/interfere with the process actually exiting."""
    def raise_error(**kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(app_kalshi, "scheduler", _FakeScheduler(True, raise_error))
    app_kalshi._shutdown_scheduler()  # must not raise
