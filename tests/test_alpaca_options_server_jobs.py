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


def test_train_job_calls_train_model_when_market_is_off_hours(monkeypatch):
    from data import alpaca_data, alpaca_options_model

    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "pre_market", "is_open": False})
    monkeypatch.setattr(alpaca_options_model, "train_model", lambda: {"ok": True, "rows": 500})
    result = alpaca_options_server._run_alpaca_options_train.__wrapped__()  # noqa: SLF001
    assert result == {"ok": True, "rows": 500}


def test_train_job_skips_as_a_no_op_during_regular_hours(monkeypatch):
    from data import alpaca_data, alpaca_options_model

    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "regular", "is_open": True})
    called = {"train": False}
    monkeypatch.setattr(alpaca_options_model, "train_model", lambda: called.update(train=True) or {"ok": True})
    result = alpaca_options_server._run_alpaca_options_train.__wrapped__()  # noqa: SLF001
    assert result == {"ok": True, "skipped": "regular_hours"}
    assert called["train"] is False


def test_train_job_force_bypasses_the_regular_hours_skip(monkeypatch):
    from data import alpaca_data, alpaca_options_model

    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "regular", "is_open": True})
    monkeypatch.setattr(alpaca_options_model, "train_model", lambda: {"ok": True, "rows": 500})
    result = alpaca_options_server._run_alpaca_options_train.__wrapped__(force=True)  # noqa: SLF001
    assert result == {"ok": True, "rows": 500}


def test_backtest_sweep_job_is_a_noop_during_regular_hours(monkeypatch):
    from data import alpaca_data

    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "regular", "is_open": True})
    called = []
    monkeypatch.setattr(alpaca_options_server.alpaca_options_data, "load_training_dataset", lambda: called.append(1) or pd.DataFrame())
    result = alpaca_options_server._run_alpaca_options_backtest_sweep.__wrapped__()  # noqa: SLF001
    assert result["skipped"] is True
    assert result["reason"] == "regular_hours"
    assert called == []  # must not even load the dataset while the market is open


def test_backtest_sweep_job_skips_cleanly_with_no_data(monkeypatch):
    from data import alpaca_data

    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "closed", "is_open": False})
    monkeypatch.setattr(alpaca_options_server.alpaca_options_data, "load_training_dataset", lambda: pd.DataFrame())
    result = alpaca_options_server._run_alpaca_options_backtest_sweep.__wrapped__()  # noqa: SLF001
    assert result["skipped"] is True
    assert result["reason"] == "no_data"


def test_backtest_sweep_job_runs_and_saves_results_when_market_closed(monkeypatch, tmp_path):
    from data import alpaca_data

    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "closed", "is_open": False})
    df = pd.DataFrame({"symbol": ["AAPL"] * 10, "ts": list(range(10))})
    monkeypatch.setattr(alpaca_options_server.alpaca_options_data, "load_training_dataset", lambda: df)
    monkeypatch.setattr(alpaca_options_server.alpaca_options_backtest, "fit_backtest_model", lambda train_df: None)
    monkeypatch.setattr(alpaca_options_server.alpaca_options_backtest, "add_model_predictions", lambda test_df, fitted: test_df)
    fake_sweep = {"all_configs": [], "ranked": [], "best": None}
    monkeypatch.setattr(alpaca_options_server.alpaca_options_backtest, "run_config_sweep", lambda test_with_preds, **kw: fake_sweep)
    monkeypatch.setattr(alpaca_options_server, "ALPACA_OPTIONS_LATEST_SWEEP_FILE", tmp_path / "sweep.json")

    result = alpaca_options_server._run_alpaca_options_backtest_sweep.__wrapped__()  # noqa: SLF001
    assert result["ok"] is True
    assert result["sweep_result"] == fake_sweep
    assert (tmp_path / "sweep.json").exists()


def test_backtest_sweep_job_never_raises_on_failure(monkeypatch):
    from data import alpaca_data

    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "closed", "is_open": False})

    def raise_error():
        raise RuntimeError("HF download failed")

    monkeypatch.setattr(alpaca_options_server.alpaca_options_data, "load_training_dataset", raise_error)
    result = alpaca_options_server._run_alpaca_options_backtest_sweep.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


def test_api_alpaca_options_backtest_route_requires_cron_auth(monkeypatch):
    monkeypatch.setattr(alpaca_options_server, "is_cron_authorized", lambda request: False)
    with alpaca_options_server.app.test_client() as client:
        resp = client.get("/api/alpaca/options/backtest")
        assert resp.status_code == 401


def test_threads_trending_news_job_skips_posting_since_alpaca_stocks_owns_this_beat(monkeypatch):
    """Real, confirmed duplication bug: this job and Alpaca stocks' own
    trending-news job both posted the SAME stock-market story to the
    SAME shared Threads account (all 4 services share one account).
    Fixed by having this job intentionally no-op -- must never call
    stock_news.get_trending_story()/threads_post.post_trending_news at all."""
    from data import stock_news, threads_post

    def fail_if_called(*a, **k):
        raise AssertionError("must not fetch/post -- alpaca_stocks owns this beat now")

    monkeypatch.setattr(stock_news, "get_trending_story", fail_if_called)
    monkeypatch.setattr(threads_post, "post_trending_news", fail_if_called)

    result = alpaca_options_server._run_alpaca_options_threads_trending_news.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "posted": False, "action": "skipped_duplicate_beat", "owner": "alpaca_stocks"}


def test_threads_sentiment_snapshot_job_posts_per_ticker_sentiment(monkeypatch):
    from data import alpaca_data, alpaca_options_data, stock_news, threads_post

    monkeypatch.setattr(alpaca_options_data, "get_options_universe", lambda: ["AAPL", "MSFT"])
    monkeypatch.setattr(alpaca_data, "get_company_name", lambda symbol: f"{symbol} Inc.")
    monkeypatch.setattr(stock_news, "get_sentiment", lambda symbol, **kw: {"sentiment_score": 0.3 if symbol == "AAPL" else -0.4})

    captured = {}
    monkeypatch.setattr(threads_post, "post_sentiment_snapshot", lambda *, market, ticker_sentiments: captured.update(market=market, ticker_sentiments=ticker_sentiments) or True)

    result = alpaca_options_server._run_alpaca_options_threads_sentiment_snapshot.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "ticker_count": 2}
    assert captured["market"] == "options"
    assert {"ticker": "AAPL", "sentiment_score": 0.3} in captured["ticker_sentiments"]
    assert {"ticker": "MSFT", "sentiment_score": -0.4} in captured["ticker_sentiments"]


def test_threads_sentiment_snapshot_job_never_raises_on_failure(monkeypatch):
    from data import alpaca_options_data

    def raise_error():
        raise RuntimeError("universe unavailable")

    monkeypatch.setattr(alpaca_options_data, "get_options_universe", raise_error)
    result = alpaca_options_server._run_alpaca_options_threads_sentiment_snapshot.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


def test_threads_hourly_status_job_posts_open_positions(monkeypatch):
    import datetime as dt
    from data import alpaca_options_strategy, threads_post

    opened_at = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=15)).isoformat()
    monkeypatch.setattr(alpaca_options_strategy, "_load_state", lambda: {
        "positions": [{
            "symbol": "AAPL240223C00195000", "underlying_symbol": "AAPL", "strategy": "naked",
            "entry_price": 5.0, "count": 1, "opened_at": opened_at,
        }],
        "realized_pnl_by_date": {},
    })

    captured = {}
    monkeypatch.setattr(threads_post, "post_hourly_status", lambda *, positions, today_realized_pnl_usd, market: captured.update(positions=positions, market=market) or True)

    result = alpaca_options_server._run_alpaca_options_threads_hourly_status.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "open_position_count": 1}
    assert captured["market"] == "options"
    assert captured["positions"][0]["ticker"] == "AAPL240223C00195000"
    assert captured["positions"][0]["held_minutes"] == pytest.approx(15.0, abs=0.5)


def test_threads_hourly_status_job_never_raises_on_failure(monkeypatch):
    from data import alpaca_options_strategy

    def raise_error():
        raise RuntimeError("state file corrupted")

    monkeypatch.setattr(alpaca_options_strategy, "_load_state", raise_error)
    result = alpaca_options_server._run_alpaca_options_threads_hourly_status.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


def test_api_alpaca_options_status_reports_configured_flag(monkeypatch):
    from data import alpaca_client

    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0", "equity": "500.0"})
    with alpaca_options_server.app.test_client() as client:
        resp = client.get("/api/alpaca/options/status")
        assert resp.status_code == 200
        assert resp.get_json()["alpaca_configured"] is True


def test_api_alpaca_options_status_attaches_unrealized_pnl_and_exit_check_to_open_positions(monkeypatch):
    """Real gap found in review: the dashboard showed entry premium +
    static TP/SL levels for every open position but never its CURRENT
    premium, unrealized P&L, or the real exit_check reason text -- even
    though manage_open_positions() already computes both every fast_check
    cycle. Same *100 contract multiplier the real exit-booking code uses:
    entry premium 2.00, current 2.50, count 3 -> +$150.00."""
    from data import alpaca_client
    from data import alpaca_options_strategy as strat

    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0", "equity": "500.0"})
    monkeypatch.setattr(strat, "_load_state", lambda: {
        "positions": [{
            "symbol": "AAPL240223C00195000", "entry_price": 2.00, "count": 3.0,
            "opened_at": "2026-08-19T00:00:00+00:00",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })

    def fake_load_json(path, default):
        if str(path).endswith("alpaca_options_latest_position_check.json"):
            return {"checks": [{"symbol": "AAPL240223C00195000", "ok": True, "exit_check": "holding (+25.00%)", "current_price": 2.50}]}
        return default

    monkeypatch.setattr(alpaca_options_server, "load_json", fake_load_json)

    with alpaca_options_server.app.test_client() as client:
        resp = client.get("/api/alpaca/options/status")
        position = resp.get_json()["positions"][0]
        assert position["current_price"] == 2.50
        assert position["unrealized_pnl_usd"] == pytest.approx(150.0, abs=0.01)
        assert position["unrealized_pnl_pct"] == pytest.approx(0.25, abs=0.001)
        assert position["exit_check"] == "holding (+25.00%)"


def test_api_alpaca_options_status_omits_unrealized_pnl_when_no_current_price_is_available(monkeypatch):
    from data import alpaca_client
    from data import alpaca_options_strategy as strat

    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0", "equity": "500.0"})
    monkeypatch.setattr(strat, "_load_state", lambda: {
        "positions": [{
            "symbol": "AAPL240223C00195000", "entry_price": 2.00, "count": 3.0,
            "opened_at": "2026-08-19T00:00:00+00:00",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_options_server, "load_json", lambda path, default: default)

    with alpaca_options_server.app.test_client() as client:
        resp = client.get("/api/alpaca/options/status")
        position = resp.get_json()["positions"][0]
        assert "unrealized_pnl_usd" not in position


def test_api_alpaca_options_status_reports_the_real_market_session(monkeypatch):
    """Real gap found in review: options had no way to tell from the
    dashboard whether it was in its trading window (regular hours) or its
    off-hours model-retraining window -- unlike alpaca_server.py's own
    status route, which has always surfaced this for stocks."""
    from data import alpaca_data

    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "pre_market", "is_open": False, "source": "fallback"})
    alpaca_options_server._MARKET_SESSION_CACHE.clear()  # noqa: SLF001
    alpaca_options_server._MARKET_SESSION_CACHE_TS = 0.0  # noqa: SLF001
    with alpaca_options_server.app.test_client() as client:
        resp = client.get("/api/alpaca/options/status")
        assert resp.status_code == 200
        assert resp.get_json()["market_session"]["session"] == "pre_market"


def test_chart_snapshot_route_serves_a_real_saved_png(monkeypatch, tmp_path):
    from data import chart_snapshot

    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    candles = [{"ts": i, "open": 100.0 + i * 0.1, "high": 100.2 + i * 0.1, "low": 99.9 + i * 0.1, "close": 100.1 + i * 0.1} for i in range(30)]
    path = chart_snapshot.generate_candlestick_chart(
        ticker="AAPL", market="options", candles=candles,
        entry_price=101.0, take_profit_price=102.0, stop_loss_price=100.0,
    )
    assert path is not None

    with alpaca_options_server.app.test_client() as client:
        resp = client.get(f"/chart/{path.name}")
        assert resp.status_code == 200
        assert resp.data[:8] == b"\x89PNG\r\n\x1a\n"


def test_chart_snapshot_route_404s_for_a_missing_file():
    with alpaca_options_server.app.test_client() as client:
        resp = client.get("/chart/does-not-exist.png")
        assert resp.status_code == 404


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


# ---------------------------------------------------------------------------
# Threads content jobs moved off this service's own internal APScheduler to
# external cron-job.org triggers (see docs/CRON_JOB_MIGRATION.md) -- these
# routes are the trigger surface, same CRON_SECRET-gated convention as
# /api/alpaca/options/tick and friends.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("path,job_name", [
    ("/api/alpaca/options/threads/trending-news", "_run_alpaca_options_threads_trending_news"),
    ("/api/alpaca/options/threads/sentiment-snapshot", "_run_alpaca_options_threads_sentiment_snapshot"),
    ("/api/alpaca/options/threads/hourly-status", "_run_alpaca_options_threads_hourly_status"),
])
def test_threads_trigger_routes_require_cron_authorization(monkeypatch, path, job_name):
    monkeypatch.setenv("CRON_SECRET", "real-secret")
    with alpaca_options_server.app.test_client() as client:
        resp = client.post(path)
        assert resp.status_code == 401


@pytest.mark.parametrize("path,job_name", [
    ("/api/alpaca/options/threads/trending-news", "_run_alpaca_options_threads_trending_news"),
    ("/api/alpaca/options/threads/sentiment-snapshot", "_run_alpaca_options_threads_sentiment_snapshot"),
    ("/api/alpaca/options/threads/hourly-status", "_run_alpaca_options_threads_hourly_status"),
])
def test_threads_trigger_routes_call_the_right_job_when_authorized(monkeypatch, path, job_name):
    monkeypatch.setattr(alpaca_options_server, job_name, lambda: {"ok": True, "posted": True})
    with alpaca_options_server.app.test_client() as client:
        resp = client.post(path)
        assert resp.status_code == 200
        assert resp.get_json() == {"ok": True, "posted": True}


@pytest.mark.parametrize("path,job_name", [
    ("/api/alpaca/options/threads/trending-news", "_run_alpaca_options_threads_trending_news"),
    ("/api/alpaca/options/threads/sentiment-snapshot", "_run_alpaca_options_threads_sentiment_snapshot"),
    ("/api/alpaca/options/threads/hourly-status", "_run_alpaca_options_threads_hourly_status"),
])
def test_threads_trigger_routes_never_raise_on_a_backend_failure(monkeypatch, path, job_name):
    def raise_error():
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(alpaca_options_server, job_name, raise_error)
    with alpaca_options_server.app.test_client() as client:
        resp = client.post(path)
        assert resp.status_code == 500
        assert resp.get_json()["ok"] is False
