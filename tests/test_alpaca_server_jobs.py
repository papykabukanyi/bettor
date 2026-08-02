"""Alpaca-server-specific job wiring, scheduler shutdown behavior, and the
off-hours historical-backfill job (alpaca_server.py's own addition, with no
app_kalshi.py equivalent -- it fulfills the "max historical dataset sent
to Hugging Face" requirement). Generic job-locking mechanics are covered in
test_server_common.py instead."""
from __future__ import annotations

import pandas as pd
import pytest

import alpaca_server


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    """The real backfill loop sleeps briefly per symbol to respect Alpaca's
    rate limit -- tests exercise several symbols per case, so this would
    otherwise make the suite slow for no reason."""
    monkeypatch.setattr(alpaca_server.time, "sleep", lambda _seconds: None)


def test_fast_check_job_calls_manage_open_positions(monkeypatch):
    from data import alpaca_strategy as strat

    monkeypatch.setattr(strat, "manage_open_positions", lambda: {"action": "no_position", "checks": []})
    result = alpaca_server._run_alpaca_fast_check.__wrapped__()  # noqa: SLF001
    assert result == {"action": "no_position", "checks": []}


def test_entry_scan_job_calls_scan_and_enter(monkeypatch):
    from data import alpaca_strategy as strat

    monkeypatch.setattr(strat, "scan_and_enter", lambda: {"opened": [{"symbol": "AAPL", "action": "opened"}]})
    result = alpaca_server._run_alpaca_entry_scan.__wrapped__()  # noqa: SLF001
    assert result["opened"][0]["symbol"] == "AAPL"


def test_data_collect_job_returns_no_rows_when_nothing_collected(monkeypatch):
    from data import alpaca_data

    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent=None: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "collect_dataset_rows", lambda symbols: pd.DataFrame())

    result = alpaca_server._run_alpaca_data_collect.__wrapped__()  # noqa: SLF001
    assert result == {"ok": False, "reason": "no_rows_collected"}


def test_data_collect_job_pushes_collected_rows(monkeypatch):
    from data import alpaca_data

    df = pd.DataFrame({"symbol": ["AAPL"], "ts": [1]})
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent=None: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "collect_dataset_rows", lambda symbols: df)
    pushed = {}

    def fake_push(d):
        pushed["df"] = d
        return {"ok": True}

    monkeypatch.setattr(alpaca_data, "push_minute_snapshot", fake_push)

    result = alpaca_server._run_alpaca_data_collect.__wrapped__()  # noqa: SLF001
    assert result == {"ok": True}
    assert list(pushed["df"]["symbol"]) == ["AAPL"]


def test_train_job_calls_train_model(monkeypatch):
    from data import alpaca_model

    monkeypatch.setattr(alpaca_model, "train_model", lambda: {"ok": True, "rows": 500})
    result = alpaca_server._run_alpaca_train.__wrapped__()  # noqa: SLF001
    assert result == {"ok": True, "rows": 500}


def test_threads_trending_news_job_posts_the_fetched_headlines(monkeypatch):
    from data import stock_news, threads_post

    monkeypatch.setattr(stock_news, "get_trending_headlines", lambda limit=5: ["Markets rally", "Fed holds rates"])
    captured = {}
    monkeypatch.setattr(threads_post, "post_trending_news", lambda headlines, *, market: captured.update(headlines=headlines, market=market) or True)

    result = alpaca_server._run_alpaca_threads_trending_news.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "headline_count": 2}
    assert captured["market"] == "stocks"
    assert captured["headlines"] == ["Markets rally", "Fed holds rates"]


def test_threads_trending_news_job_never_raises_on_failure(monkeypatch):
    from data import stock_news

    def raise_error(limit=5):
        raise RuntimeError("rss down")

    monkeypatch.setattr(stock_news, "get_trending_headlines", raise_error)
    result = alpaca_server._run_alpaca_threads_trending_news.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


# ---------------------------------------------------------------------------
# Historical backfill -- the "max historical dataset -> HF" job
# ---------------------------------------------------------------------------
def test_advance_historical_backfill_reports_complete_when_nothing_remains(monkeypatch, tmp_path):
    from data import alpaca_data

    monkeypatch.setattr(alpaca_data, "get_us_stock_universe", lambda: ["AAPL", "MSFT"])
    monkeypatch.setattr(alpaca_data, "get_symbols_with_daily_bars", lambda: {"AAPL", "MSFT"})
    monkeypatch.setattr(alpaca_server, "ALPACA_LATEST_BACKFILL_FILE", tmp_path / "backfill.json")

    result = alpaca_server._advance_historical_backfill()

    assert result["action"] == "backfill_complete"
    assert result["universe_size"] == 2
    assert result["already_done"] == 2
    saved = alpaca_server.load_json(tmp_path / "backfill.json", {})
    assert saved["action"] == "backfill_complete"


def test_advance_historical_backfill_fetches_and_pushes_a_batch(monkeypatch, tmp_path):
    from data import alpaca_data

    monkeypatch.setattr(alpaca_data, "get_us_stock_universe", lambda: ["AAPL", "MSFT", "NVDA"])
    monkeypatch.setattr(alpaca_data, "get_symbols_with_daily_bars", lambda: {"AAPL"})
    monkeypatch.setattr(alpaca_server, "ALPACA_BACKFILL_BATCH_SIZE", 10)
    monkeypatch.setattr(alpaca_server, "ALPACA_LATEST_BACKFILL_FILE", tmp_path / "backfill.json")

    fetched = []

    def fake_fetch(symbol, years=20):
        fetched.append(symbol)
        return pd.DataFrame({"ts": [1], "close": [1.0]})

    monkeypatch.setattr(alpaca_data, "fetch_daily_bars", fake_fetch)
    monkeypatch.setattr(alpaca_data, "push_daily_snapshot", lambda symbol, df: {"ok": True})

    result = alpaca_server._advance_historical_backfill()

    assert sorted(fetched) == ["MSFT", "NVDA"]  # AAPL already done -- resumable, not re-fetched
    assert result["action"] == "backfill_batch"
    assert result["pushed"] == 2
    assert result["failed"] == 0
    assert result["remaining_before"] == 2


def test_advance_historical_backfill_counts_failures_without_raising(monkeypatch, tmp_path):
    from data import alpaca_data

    monkeypatch.setattr(alpaca_data, "get_us_stock_universe", lambda: ["AAPL", "BADSYM"])
    monkeypatch.setattr(alpaca_data, "get_symbols_with_daily_bars", lambda: set())
    monkeypatch.setattr(alpaca_server, "ALPACA_LATEST_BACKFILL_FILE", tmp_path / "backfill.json")

    def fake_fetch(symbol, years=20):
        if symbol == "BADSYM":
            raise RuntimeError("symbol not found")
        return pd.DataFrame({"ts": [1], "close": [1.0]})

    monkeypatch.setattr(alpaca_data, "fetch_daily_bars", fake_fetch)
    monkeypatch.setattr(alpaca_data, "push_daily_snapshot", lambda symbol, df: {"ok": True})

    result = alpaca_server._advance_historical_backfill()

    assert result["pushed"] == 1
    assert result["failed"] == 1


def test_advance_historical_backfill_batch_size_limits_how_many_symbols_advance(monkeypatch, tmp_path):
    """A full US equity universe can't be pulled in one off-hours tick --
    only ALPACA_BACKFILL_BATCH_SIZE symbols should advance per call, leaving
    the rest for the next tick."""
    from data import alpaca_data

    monkeypatch.setattr(alpaca_data, "get_us_stock_universe", lambda: ["A", "B", "C", "D"])
    monkeypatch.setattr(alpaca_data, "get_symbols_with_daily_bars", lambda: set())
    monkeypatch.setattr(alpaca_server, "ALPACA_BACKFILL_BATCH_SIZE", 2)
    monkeypatch.setattr(alpaca_server, "ALPACA_LATEST_BACKFILL_FILE", tmp_path / "backfill.json")

    fetched = []
    monkeypatch.setattr(alpaca_data, "fetch_daily_bars", lambda symbol, years=20: fetched.append(symbol) or pd.DataFrame({"ts": [1], "close": [1.0]}))
    monkeypatch.setattr(alpaca_data, "push_daily_snapshot", lambda symbol, df: {"ok": True})

    result = alpaca_server._advance_historical_backfill()

    assert len(fetched) == 2
    assert result["batch_size"] == 2
    assert result["remaining_before"] == 4


def test_intensive_training_is_a_noop_while_market_is_open(monkeypatch):
    from data import alpaca_data

    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "regular", "is_open": True, "source": "fallback"})

    calls = []
    monkeypatch.setattr(alpaca_server, "_advance_historical_backfill", lambda: calls.append(1))

    result = alpaca_server._run_alpaca_intensive_training.__wrapped__()  # noqa: SLF001

    assert result["skipped"] is True
    assert result["reason"] == "market_not_closed"
    assert calls == []  # backfill must not advance while the market is open either


def test_intensive_training_trains_and_advances_backfill_when_market_closed(monkeypatch, tmp_path):
    from data import alpaca_data, alpaca_model

    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "closed", "is_open": False, "source": "fallback"})
    monkeypatch.setattr(alpaca_model, "train_model", lambda: {"ok": True, "rows": 1000})
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda: pd.DataFrame())  # empty -> sweep skipped safely
    backfill_calls = []
    monkeypatch.setattr(alpaca_server, "_advance_historical_backfill", lambda: backfill_calls.append(1) or {"action": "backfill_batch", "pushed": 5})

    result = alpaca_server._run_alpaca_intensive_training.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert result["train_result"] == {"ok": True, "rows": 1000}
    assert result["backfill_result"] == {"action": "backfill_batch", "pushed": 5}
    assert backfill_calls == [1]


def test_intensive_training_still_advances_backfill_if_sweep_raises(monkeypatch):
    """A backtest sweep failure (bad data, a fitting error) must not prevent
    the historical backfill from making progress that tick -- these are two
    independent pieces of off-hours work."""
    from data import alpaca_data, alpaca_model

    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "closed", "is_open": False, "source": "fallback"})
    monkeypatch.setattr(alpaca_model, "train_model", lambda: {"ok": True, "rows": 1000})

    def raise_error(**kw):
        raise RuntimeError("HF listing failed")

    monkeypatch.setattr(alpaca_data, "load_training_dataset", raise_error)
    backfill_calls = []
    monkeypatch.setattr(alpaca_server, "_advance_historical_backfill", lambda: backfill_calls.append(1) or {"action": "backfill_complete"})

    result = alpaca_server._run_alpaca_intensive_training.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert result["sweep_result"] is None
    assert backfill_calls == [1]


class _FakeScheduler:
    def __init__(self, running, shutdown_fn=None):
        self.running = running
        self._shutdown_fn = shutdown_fn or (lambda **kw: None)

    def shutdown(self, **kw):
        return self._shutdown_fn(**kw)


def test_shutdown_scheduler_stops_a_running_scheduler(monkeypatch):
    calls = []
    monkeypatch.setattr(alpaca_server, "scheduler", _FakeScheduler(True, lambda **kw: calls.append(kw)))
    alpaca_server._shutdown_scheduler()
    assert calls == [{"wait": False}]


def test_shutdown_scheduler_is_a_noop_when_not_running(monkeypatch):
    def fail_if_called(**kw):
        raise AssertionError("must not call shutdown() on a scheduler that isn't running")

    monkeypatch.setattr(alpaca_server, "scheduler", _FakeScheduler(False, fail_if_called))
    alpaca_server._shutdown_scheduler()  # must not raise


def test_shutdown_scheduler_swallows_errors(monkeypatch):
    def raise_error(**kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(alpaca_server, "scheduler", _FakeScheduler(True, raise_error))
    alpaca_server._shutdown_scheduler()  # must not raise


def test_api_alpaca_status_reports_configured_flag(monkeypatch):
    from data import alpaca_client

    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)
    with alpaca_server.app.test_client() as client:
        resp = client.get("/api/alpaca/status")
        assert resp.status_code == 200
        assert resp.get_json()["alpaca_configured"] is True


# ---------------------------------------------------------------------------
# Crypto strategy job wiring -- separate from the equities jobs above, no
# market-hours gating.
# ---------------------------------------------------------------------------
def test_crypto_fast_check_job_calls_manage_open_positions(monkeypatch):
    from data import alpaca_crypto_strategy as strat

    monkeypatch.setattr(strat, "manage_open_positions", lambda: {"action": "no_position", "checks": []})
    result = alpaca_server._run_alpaca_crypto_fast_check.__wrapped__()  # noqa: SLF001
    assert result == {"action": "no_position", "checks": []}


def test_crypto_entry_scan_job_calls_scan_and_enter(monkeypatch):
    from data import alpaca_crypto_strategy as strat

    monkeypatch.setattr(strat, "scan_and_enter", lambda: {"opened": [{"symbol": "BTC/USD", "action": "opened"}]})
    result = alpaca_server._run_alpaca_crypto_entry_scan.__wrapped__()  # noqa: SLF001
    assert result["opened"][0]["symbol"] == "BTC/USD"


def test_crypto_data_collect_job_returns_no_rows_when_nothing_collected(monkeypatch):
    from data import alpaca_crypto_data

    monkeypatch.setattr(alpaca_crypto_data, "collect_dataset_rows", lambda: pd.DataFrame())
    result = alpaca_server._run_alpaca_crypto_data_collect.__wrapped__()  # noqa: SLF001
    assert result == {"ok": False, "reason": "no_rows_collected"}


def test_crypto_data_collect_job_pushes_collected_rows(monkeypatch):
    from data import alpaca_crypto_data

    df = pd.DataFrame({"symbol": ["BTC/USD"], "ts": [1]})
    monkeypatch.setattr(alpaca_crypto_data, "collect_dataset_rows", lambda: df)
    pushed = {}

    def fake_push(d):
        pushed["df"] = d
        return {"ok": True}

    monkeypatch.setattr(alpaca_crypto_data, "push_minute_snapshot", fake_push)
    result = alpaca_server._run_alpaca_crypto_data_collect.__wrapped__()  # noqa: SLF001
    assert result == {"ok": True}
    assert list(pushed["df"]["symbol"]) == ["BTC/USD"]


def test_crypto_train_job_calls_train_model(monkeypatch):
    from data import alpaca_crypto_model

    monkeypatch.setattr(alpaca_crypto_model, "train_model", lambda: {"ok": True, "rows": 500})
    result = alpaca_server._run_alpaca_crypto_train.__wrapped__()  # noqa: SLF001
    assert result == {"ok": True, "rows": 500}


def test_api_alpaca_crypto_status_reports_configured_flag(monkeypatch):
    from data import alpaca_client

    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)
    with alpaca_server.app.test_client() as client:
        resp = client.get("/api/alpaca/crypto/status")
        assert resp.status_code == 200
        assert resp.get_json()["alpaca_configured"] is True
