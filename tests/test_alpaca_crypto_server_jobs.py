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


def test_threads_trending_news_job_posts_the_fetched_headlines(monkeypatch):
    from data import crypto_news, threads_post

    monkeypatch.setattr(crypto_news, "get_trending_headlines", lambda limit=5: ["Bitcoin rallies", "ETF inflows rise"])
    captured = {}
    monkeypatch.setattr(threads_post, "post_trending_news", lambda headlines, *, market: captured.update(headlines=headlines, market=market) or True)

    result = alpaca_crypto_server._run_alpaca_crypto_threads_trending_news.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "headline_count": 2}
    assert captured["market"] == "crypto"


def test_threads_trending_news_job_never_raises_on_failure(monkeypatch):
    from data import crypto_news

    def raise_error(limit=5):
        raise RuntimeError("rss down")

    monkeypatch.setattr(crypto_news, "get_trending_headlines", raise_error)
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


def test_api_alpaca_crypto_status_reports_configured_flag(monkeypatch):
    from data import alpaca_client

    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0", "equity": "500.0"})
    with alpaca_crypto_server.app.test_client() as client:
        resp = client.get("/api/alpaca/crypto/status")
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
