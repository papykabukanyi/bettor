"""Alpaca-crypto-server-specific job wiring and scheduler shutdown
behavior -- its own separate Render service, split out from
alpaca_server.py (equities) after a real, confirmed OOM crash loop from
running both strategies in one 512MB process. Generic job-locking
mechanics are covered in test_server_common.py instead."""
from __future__ import annotations

import pandas as pd
import pytest

import alpaca_crypto_server


def test_fast_check_job_calls_manage_open_positions(monkeypatch):
    from data import alpaca_crypto_strategy as strat

    monkeypatch.setattr(strat, "manage_open_positions", lambda: {"action": "no_position", "checks": []})
    result = alpaca_crypto_server._run_alpaca_crypto_fast_check.__wrapped__()  # noqa: SLF001
    assert result == {"action": "no_position", "checks": []}


def test_entry_scan_job_calls_scan_and_enter(monkeypatch):
    from data import alpaca_crypto_strategy as strat

    monkeypatch.setattr(strat, "scan_and_enter", lambda: {"opened": [{"symbol": "BTC/USD", "action": "opened"}]})
    result = alpaca_crypto_server._run_alpaca_crypto_entry_scan.__wrapped__()  # noqa: SLF001
    assert result["opened"][0]["symbol"] == "BTC/USD"


def test_data_collect_job_returns_no_rows_when_nothing_collected(monkeypatch):
    from data import alpaca_crypto_data

    monkeypatch.setattr(alpaca_crypto_data, "collect_dataset_rows", lambda: pd.DataFrame())
    result = alpaca_crypto_server._run_alpaca_crypto_data_collect.__wrapped__()  # noqa: SLF001
    assert result == {"ok": False, "reason": "no_rows_collected"}


def test_data_collect_job_pushes_collected_rows(monkeypatch):
    from data import alpaca_crypto_data

    df = pd.DataFrame({"symbol": ["BTC/USD"], "ts": [1]})
    monkeypatch.setattr(alpaca_crypto_data, "collect_dataset_rows", lambda: df)
    pushed = {}

    def fake_push(d):
        pushed["df"] = d
        return {"ok": True}

    monkeypatch.setattr(alpaca_crypto_data, "push_minute_snapshot", fake_push)
    result = alpaca_crypto_server._run_alpaca_crypto_data_collect.__wrapped__()  # noqa: SLF001
    assert result == {"ok": True}
    assert list(pushed["df"]["symbol"]) == ["BTC/USD"]


def test_train_job_calls_train_model(monkeypatch):
    from data import alpaca_crypto_model

    monkeypatch.setattr(alpaca_crypto_model, "train_model", lambda: {"ok": True, "rows": 500})
    result = alpaca_crypto_server._run_alpaca_crypto_train.__wrapped__()  # noqa: SLF001
    assert result == {"ok": True, "rows": 500}


def test_threads_trending_news_job_posts_the_fetched_story(monkeypatch):
    from data import crypto_news, threads_post

    story = {"title": "Bitcoin rallies", "link": "https://x.com/a", "image_url": "https://x.com/i.jpg", "source": "cointelegraph", "secondary": []}
    monkeypatch.setattr(crypto_news, "get_trending_story", lambda: story)
    captured = {}
    monkeypatch.setattr(threads_post, "post_trending_news", lambda s, *, market: captured.update(story=s, market=market) or True)

    result = alpaca_crypto_server._run_alpaca_crypto_threads_trending_news.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "story": "Bitcoin rallies"}
    assert captured["market"] == "crypto"


def test_threads_trending_news_job_never_raises_on_failure(monkeypatch):
    from data import crypto_news

    def raise_error():
        raise RuntimeError("rss down")

    monkeypatch.setattr(crypto_news, "get_trending_story", raise_error)
    result = alpaca_crypto_server._run_alpaca_crypto_threads_trending_news.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


def test_threads_sentiment_snapshot_job_posts_per_ticker_sentiment(monkeypatch):
    from data import alpaca_crypto_data, crypto_news, threads_post

    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BTC/USD", "ETH/USD"])
    monkeypatch.setattr(alpaca_crypto_data, "symbol_to_coin", lambda symbol: symbol.split("/")[0])
    monkeypatch.setattr(crypto_news, "get_sentiment", lambda coin, **kw: {"sentiment_score": 0.6 if coin == "BTC" else -0.3})

    captured = {}
    monkeypatch.setattr(threads_post, "post_sentiment_snapshot", lambda *, market, ticker_sentiments: captured.update(market=market, ticker_sentiments=ticker_sentiments) or True)

    result = alpaca_crypto_server._run_alpaca_crypto_threads_sentiment_snapshot.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "ticker_count": 2}
    assert captured["market"] == "crypto"
    assert {"ticker": "BTC/USD", "sentiment_score": 0.6} in captured["ticker_sentiments"]
    assert {"ticker": "ETH/USD", "sentiment_score": -0.3} in captured["ticker_sentiments"]


def test_threads_sentiment_snapshot_job_never_raises_on_failure(monkeypatch):
    from data import alpaca_crypto_data

    def raise_error():
        raise RuntimeError("universe unavailable")

    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", raise_error)
    result = alpaca_crypto_server._run_alpaca_crypto_threads_sentiment_snapshot.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


def test_threads_hourly_status_job_posts_open_positions(monkeypatch):
    import datetime as dt
    from data import alpaca_crypto_strategy, threads_post

    opened_at = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=15)).isoformat()
    monkeypatch.setattr(alpaca_crypto_strategy, "_load_state", lambda: {
        "positions": [{"symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001, "opened_at": opened_at}],
        "realized_pnl_by_date": {},
    })

    captured = {}
    monkeypatch.setattr(threads_post, "post_hourly_status", lambda *, positions, today_realized_pnl_usd, market: captured.update(positions=positions, market=market) or True)

    result = alpaca_crypto_server._run_alpaca_crypto_threads_hourly_status.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "open_position_count": 1}
    assert captured["market"] == "crypto"
    assert captured["positions"][0]["ticker"] == "BTC/USD"
    assert captured["positions"][0]["held_minutes"] == pytest.approx(15.0, abs=0.5)


def test_threads_hourly_status_job_never_raises_on_failure(monkeypatch):
    from data import alpaca_crypto_strategy

    def raise_error():
        raise RuntimeError("state file corrupted")

    monkeypatch.setattr(alpaca_crypto_strategy, "_load_state", raise_error)
    result = alpaca_crypto_server._run_alpaca_crypto_threads_hourly_status.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


def test_api_alpaca_crypto_status_reports_configured_flag(monkeypatch):
    from data import alpaca_client

    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0", "equity": "500.0"})
    with alpaca_crypto_server.app.test_client() as client:
        resp = client.get("/api/alpaca/crypto/status")
        assert resp.status_code == 200
        assert resp.get_json()["alpaca_configured"] is True


def test_api_alpaca_crypto_status_attaches_unrealized_pnl_and_exit_check_to_open_positions(monkeypatch):
    """Real gap found in review: the dashboard showed entry price + static
    TP/SL levels for every open position but never its CURRENT price,
    unrealized P&L, or the real exit_check reason text -- even though
    manage_open_positions() already computes both every fast_check cycle.
    Long-only: entry 100.0, current 105.0, count 2 -> +$10.00."""
    from data import alpaca_client
    from data import alpaca_crypto_strategy as strat

    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0", "equity": "500.0"})
    monkeypatch.setattr(strat, "_load_state", lambda: {
        "positions": [{"symbol": "BTC/USD", "entry_price": 100.0, "count": 2.0, "opened_at": "2026-08-19T00:00:00+00:00"}],
        "trade_log": [], "realized_pnl_by_date": {},
    })

    def fake_load_json(path, default):
        if str(path).endswith("alpaca_crypto_latest_position_check.json"):
            return {"checks": [{"symbol": "BTC/USD", "ok": True, "exit_check": "holding (+5.00%)", "current_price": 105.0}]}
        return default

    monkeypatch.setattr(alpaca_crypto_server, "load_json", fake_load_json)

    with alpaca_crypto_server.app.test_client() as client:
        resp = client.get("/api/alpaca/crypto/status")
        position = resp.get_json()["positions"][0]
        assert position["current_price"] == 105.0
        assert position["unrealized_pnl_usd"] == pytest.approx(10.0, abs=0.01)
        assert position["unrealized_pnl_pct"] == pytest.approx(0.05, abs=0.001)
        assert position["exit_check"] == "holding (+5.00%)"


def test_api_alpaca_crypto_status_omits_unrealized_pnl_when_no_current_price_is_available(monkeypatch):
    from data import alpaca_client
    from data import alpaca_crypto_strategy as strat

    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0", "equity": "500.0"})
    monkeypatch.setattr(strat, "_load_state", lambda: {
        "positions": [{"symbol": "BTC/USD", "entry_price": 100.0, "count": 2.0, "opened_at": "2026-08-19T00:00:00+00:00"}],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_crypto_server, "load_json", lambda path, default: default)

    with alpaca_crypto_server.app.test_client() as client:
        resp = client.get("/api/alpaca/crypto/status")
        position = resp.get_json()["positions"][0]
        assert "unrealized_pnl_usd" not in position


def test_walkforward_backtest_job_saves_the_result(monkeypatch, tmp_path):
    from data import alpaca_crypto_backtest

    fake_result = {"ok": True, "fold_count": 4, "profitable_fold_count": 2, "mean_return_pct": 0.03}
    monkeypatch.setattr(alpaca_crypto_backtest, "run_walkforward_backtest", lambda: fake_result)
    monkeypatch.setattr(alpaca_crypto_server, "ALPACA_CRYPTO_LATEST_WALKFORWARD_FILE", tmp_path / "walkforward.json")

    result = alpaca_crypto_server._run_alpaca_crypto_walkforward_backtest.__wrapped__()  # noqa: SLF001

    assert result == fake_result
    saved = alpaca_crypto_server.load_json(tmp_path / "walkforward.json", {})
    assert saved == fake_result


def test_walkforward_backtest_job_returns_ok_false_on_failure(monkeypatch):
    from data import alpaca_crypto_backtest

    def raise_error():
        raise RuntimeError("no data available")

    monkeypatch.setattr(alpaca_crypto_backtest, "run_walkforward_backtest", raise_error)
    result = alpaca_crypto_server._run_alpaca_crypto_walkforward_backtest.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


def test_api_alpaca_crypto_report_pdf_downloads_a_real_pdf(monkeypatch):
    from data import alpaca_client

    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0", "equity": "500.0"})
    with alpaca_crypto_server.app.test_client() as client:
        resp = client.get("/api/alpaca/crypto/report.pdf")
        assert resp.status_code == 200
        assert resp.mimetype == "application/pdf"
        assert resp.data[:5] == b"%PDF-"
        assert "attachment" in resp.headers.get("Content-Disposition", "")


class _FakeScheduler:
    def __init__(self, running, shutdown_fn=None):
        self.running = running
        self._shutdown_fn = shutdown_fn or (lambda **kw: None)

    def shutdown(self, **kw):
        return self._shutdown_fn(**kw)


def test_shutdown_scheduler_stops_a_running_scheduler(monkeypatch):
    calls = []
    monkeypatch.setattr(alpaca_crypto_server, "scheduler", _FakeScheduler(True, lambda **kw: calls.append(kw)))
    alpaca_crypto_server._shutdown_scheduler()
    assert calls == [{"wait": False}]


def test_shutdown_scheduler_is_a_noop_when_not_running(monkeypatch):
    def fail_if_called(**kw):
        raise AssertionError("must not call shutdown() on a scheduler that isn't running")

    monkeypatch.setattr(alpaca_crypto_server, "scheduler", _FakeScheduler(False, fail_if_called))
    alpaca_crypto_server._shutdown_scheduler()  # must not raise


def test_shutdown_scheduler_swallows_errors(monkeypatch):
    def raise_error(**kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(alpaca_crypto_server, "scheduler", _FakeScheduler(True, raise_error))
    alpaca_crypto_server._shutdown_scheduler()  # must not raise


# ---------------------------------------------------------------------------
# Threads content jobs moved off this service's own internal APScheduler to
# external cron-job.org triggers (see docs/CRON_JOB_MIGRATION.md) -- these
# routes are the trigger surface, same CRON_SECRET-gated convention as
# /api/alpaca/crypto/tick and friends.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("path,job_name", [
    ("/api/alpaca/crypto/threads/trending-news", "_run_alpaca_crypto_threads_trending_news"),
    ("/api/alpaca/crypto/threads/sentiment-snapshot", "_run_alpaca_crypto_threads_sentiment_snapshot"),
    ("/api/alpaca/crypto/threads/hourly-status", "_run_alpaca_crypto_threads_hourly_status"),
])
def test_threads_trigger_routes_require_cron_authorization(monkeypatch, path, job_name):
    monkeypatch.setenv("CRON_SECRET", "real-secret")
    with alpaca_crypto_server.app.test_client() as client:
        resp = client.post(path)
        assert resp.status_code == 401


@pytest.mark.parametrize("path,job_name", [
    ("/api/alpaca/crypto/threads/trending-news", "_run_alpaca_crypto_threads_trending_news"),
    ("/api/alpaca/crypto/threads/sentiment-snapshot", "_run_alpaca_crypto_threads_sentiment_snapshot"),
    ("/api/alpaca/crypto/threads/hourly-status", "_run_alpaca_crypto_threads_hourly_status"),
])
def test_threads_trigger_routes_call_the_right_job_when_authorized(monkeypatch, path, job_name):
    monkeypatch.setattr(alpaca_crypto_server, job_name, lambda: {"ok": True, "posted": True})
    with alpaca_crypto_server.app.test_client() as client:
        resp = client.post(path)
        assert resp.status_code == 200
        assert resp.get_json() == {"ok": True, "posted": True}


@pytest.mark.parametrize("path,job_name", [
    ("/api/alpaca/crypto/threads/trending-news", "_run_alpaca_crypto_threads_trending_news"),
    ("/api/alpaca/crypto/threads/sentiment-snapshot", "_run_alpaca_crypto_threads_sentiment_snapshot"),
    ("/api/alpaca/crypto/threads/hourly-status", "_run_alpaca_crypto_threads_hourly_status"),
])
def test_threads_trigger_routes_never_raise_on_a_backend_failure(monkeypatch, path, job_name):
    def raise_error():
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(alpaca_crypto_server, job_name, raise_error)
    with alpaca_crypto_server.app.test_client() as client:
        resp = client.post(path)
        assert resp.status_code == 500
        assert resp.get_json()["ok"] is False
