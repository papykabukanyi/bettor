"""Alpaca-options-server-specific job wiring and scheduler shutdown
behavior -- its own separate Render service, split out the same way
crypto was after a real, confirmed OOM crash loop from running multiple
strategies in one 512MB process. Generic job-locking mechanics are covered
in test_server_common.py instead."""
from __future__ import annotations

import pandas as pd
import pytest

import alpaca_options_server


def test_fast_check_job_calls_manage_open_positions(monkeypatch):
    from data import alpaca_options_strategy as strat

    monkeypatch.setattr(strat, "manage_open_positions", lambda: {"action": "no_position", "checks": []})
    result = alpaca_options_server._run_alpaca_options_fast_check.__wrapped__()  # noqa: SLF001
    assert result == {"action": "no_position", "checks": []}


def test_entry_scan_job_calls_scan_and_enter(monkeypatch):
    from data import alpaca_options_strategy as strat

    monkeypatch.setattr(strat, "scan_and_enter", lambda: {"opened": [{"symbol": "AAPL", "action": "opened"}]})
    result = alpaca_options_server._run_alpaca_options_entry_scan.__wrapped__()  # noqa: SLF001
    assert result["opened"][0]["symbol"] == "AAPL"


def test_data_collect_job_returns_no_rows_when_nothing_collected(monkeypatch):
    from data import alpaca_options_data

    monkeypatch.setattr(alpaca_options_data, "collect_dataset_rows", lambda: pd.DataFrame())
    result = alpaca_options_server._run_alpaca_options_data_collect.__wrapped__()  # noqa: SLF001
    assert result == {"ok": False, "reason": "no_rows_collected"}


def test_data_collect_job_pushes_collected_rows(monkeypatch):
    from data import alpaca_options_data

    df = pd.DataFrame({"symbol": ["AAPL"], "ts": [1]})
    monkeypatch.setattr(alpaca_options_data, "collect_dataset_rows", lambda: df)
    pushed = {}

    def fake_push(d):
        pushed["df"] = d
        return {"ok": True}

    monkeypatch.setattr(alpaca_options_data, "push_minute_snapshot", fake_push)
    result = alpaca_options_server._run_alpaca_options_data_collect.__wrapped__()  # noqa: SLF001
    assert result == {"ok": True}
    assert list(pushed["df"]["symbol"]) == ["AAPL"]


def test_train_job_calls_train_model(monkeypatch):
    from data import alpaca_options_model

    monkeypatch.setattr(alpaca_options_model, "train_model", lambda: {"ok": True, "rows": 500})
    result = alpaca_options_server._run_alpaca_options_train.__wrapped__()  # noqa: SLF001
    assert result == {"ok": True, "rows": 500}


def test_threads_trending_news_job_posts_the_fetched_headlines(monkeypatch):
    from data import stock_news, threads_post

    monkeypatch.setattr(stock_news, "get_trending_headlines", lambda limit=5: ["Apple beats earnings", "Fed holds rates"])
    captured = {}
    monkeypatch.setattr(threads_post, "post_trending_news", lambda headlines, *, market: captured.update(headlines=headlines, market=market) or True)

    result = alpaca_options_server._run_alpaca_options_threads_trending_news.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "headline_count": 2}
    assert captured["market"] == "stocks"


def test_threads_trending_news_job_never_raises_on_failure(monkeypatch):
    from data import stock_news

    def raise_error(limit=5):
        raise RuntimeError("rss down")

    monkeypatch.setattr(stock_news, "get_trending_headlines", raise_error)
    result = alpaca_options_server._run_alpaca_options_threads_trending_news.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


def test_api_alpaca_options_status_reports_configured_flag(monkeypatch):
    from data import alpaca_client

    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)
    with alpaca_options_server.app.test_client() as client:
        resp = client.get("/api/alpaca/options/status")
        assert resp.status_code == 200
        assert resp.get_json()["alpaca_configured"] is True


class _FakeScheduler:
    def __init__(self, running, shutdown_fn=None):
        self.running = running
        self._shutdown_fn = shutdown_fn or (lambda **kw: None)

    def shutdown(self, **kw):
        return self._shutdown_fn(**kw)


def test_shutdown_scheduler_stops_a_running_scheduler(monkeypatch):
    calls = []
    monkeypatch.setattr(alpaca_options_server, "scheduler", _FakeScheduler(True, lambda **kw: calls.append(kw)))
    alpaca_options_server._shutdown_scheduler()
    assert calls == [{"wait": False}]


def test_shutdown_scheduler_is_a_noop_when_not_running(monkeypatch):
    def fail_if_called(**kw):
        raise AssertionError("must not call shutdown() on a scheduler that isn't running")

    monkeypatch.setattr(alpaca_options_server, "scheduler", _FakeScheduler(False, fail_if_called))
    alpaca_options_server._shutdown_scheduler()  # must not raise


def test_shutdown_scheduler_swallows_errors(monkeypatch):
    def raise_error(**kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(alpaca_options_server, "scheduler", _FakeScheduler(True, raise_error))
    alpaca_options_server._shutdown_scheduler()  # must not raise
