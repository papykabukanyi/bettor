"""Schwab-server-specific job wiring, scheduler shutdown behavior, and the
off-hours historical-backfill job (schwab_server.py's own addition, with no
app_kalshi.py equivalent -- it fulfills the "max historical dataset sent
to Hugging Face" requirement). Generic job-locking mechanics are covered in
test_server_common.py instead."""
from __future__ import annotations

import pandas as pd
import pytest

import schwab_server


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    """The real backfill loop sleeps ~0.55s per symbol to respect Schwab's
    rate limit -- tests exercise several symbols per case, so this would
    otherwise make the suite slow for no reason."""
    monkeypatch.setattr(schwab_server.time, "sleep", lambda _seconds: None)


def test_fast_check_job_calls_manage_open_positions(monkeypatch):
    from data import schwab_strategy as strat

    monkeypatch.setattr(strat, "manage_open_positions", lambda: {"action": "no_position", "checks": []})
    result = schwab_server._run_schwab_fast_check.__wrapped__()  # noqa: SLF001
    assert result == {"action": "no_position", "checks": []}


def test_entry_scan_job_calls_scan_and_enter(monkeypatch):
    from data import schwab_strategy as strat

    monkeypatch.setattr(strat, "scan_and_enter", lambda: {"opened": [{"symbol": "AAPL", "action": "opened"}]})
    result = schwab_server._run_schwab_entry_scan.__wrapped__()  # noqa: SLF001
    assert result["opened"][0]["symbol"] == "AAPL"


def test_data_collect_job_returns_no_rows_when_nothing_collected(monkeypatch):
    from data import schwab_data

    monkeypatch.setattr(schwab_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(schwab_data, "get_stock_watchlist", lambda recent=None: ["AAPL"])
    monkeypatch.setattr(schwab_data, "collect_dataset_rows", lambda symbols: pd.DataFrame())

    result = schwab_server._run_schwab_data_collect.__wrapped__()  # noqa: SLF001
    assert result == {"ok": False, "reason": "no_rows_collected"}


def test_data_collect_job_pushes_collected_rows(monkeypatch):
    from data import schwab_data

    df = pd.DataFrame({"symbol": ["AAPL"], "ts": [1]})
    monkeypatch.setattr(schwab_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(schwab_data, "get_stock_watchlist", lambda recent=None: ["AAPL"])
    monkeypatch.setattr(schwab_data, "collect_dataset_rows", lambda symbols: df)
    pushed = {}

    def fake_push(d):
        pushed["df"] = d
        return {"ok": True}

    monkeypatch.setattr(schwab_data, "push_minute_snapshot", fake_push)

    result = schwab_server._run_schwab_data_collect.__wrapped__()  # noqa: SLF001
    assert result == {"ok": True}
    assert list(pushed["df"]["symbol"]) == ["AAPL"]


def test_train_job_calls_train_model(monkeypatch):
    from data import schwab_model

    monkeypatch.setattr(schwab_model, "train_model", lambda: {"ok": True, "rows": 500})
    result = schwab_server._run_schwab_train.__wrapped__()  # noqa: SLF001
    assert result == {"ok": True, "rows": 500}


# ---------------------------------------------------------------------------
# Historical backfill -- the new "max historical dataset -> HF" job
# ---------------------------------------------------------------------------
def test_advance_historical_backfill_reports_complete_when_nothing_remains(monkeypatch, tmp_path):
    from data import schwab_data

    monkeypatch.setattr(schwab_data, "get_us_stock_universe", lambda: ["AAPL", "MSFT"])
    monkeypatch.setattr(schwab_data, "get_symbols_with_daily_bars", lambda: {"AAPL", "MSFT"})
    monkeypatch.setattr(schwab_server, "SCHWAB_LATEST_BACKFILL_FILE", tmp_path / "backfill.json")

    result = schwab_server._advance_historical_backfill()

    assert result["action"] == "backfill_complete"
    assert result["universe_size"] == 2
    assert result["already_done"] == 2
    saved = schwab_server.load_json(tmp_path / "backfill.json", {})
    assert saved["action"] == "backfill_complete"


def test_advance_historical_backfill_fetches_and_pushes_a_batch(monkeypatch, tmp_path):
    from data import schwab_data

    monkeypatch.setattr(schwab_data, "get_us_stock_universe", lambda: ["AAPL", "MSFT", "NVDA"])
    monkeypatch.setattr(schwab_data, "get_symbols_with_daily_bars", lambda: {"AAPL"})
    monkeypatch.setattr(schwab_server, "SCHWAB_BACKFILL_BATCH_SIZE", 10)
    monkeypatch.setattr(schwab_server, "SCHWAB_LATEST_BACKFILL_FILE", tmp_path / "backfill.json")

    fetched = []

    def fake_fetch(symbol, years=20):
        fetched.append(symbol)
        return pd.DataFrame({"ts": [1], "close": [1.0]})

    monkeypatch.setattr(schwab_data, "fetch_daily_bars", fake_fetch)
    monkeypatch.setattr(schwab_data, "push_daily_snapshot", lambda symbol, df: {"ok": True})

    result = schwab_server._advance_historical_backfill()

    assert sorted(fetched) == ["MSFT", "NVDA"]  # AAPL already done -- resumable, not re-fetched
    assert result["action"] == "backfill_batch"
    assert result["pushed"] == 2
    assert result["failed"] == 0
    assert result["remaining_before"] == 2


def test_advance_historical_backfill_counts_failures_without_raising(monkeypatch, tmp_path):
    from data import schwab_data

    monkeypatch.setattr(schwab_data, "get_us_stock_universe", lambda: ["AAPL", "BADSYM"])
    monkeypatch.setattr(schwab_data, "get_symbols_with_daily_bars", lambda: set())
    monkeypatch.setattr(schwab_server, "SCHWAB_LATEST_BACKFILL_FILE", tmp_path / "backfill.json")

    def fake_fetch(symbol, years=20):
        if symbol == "BADSYM":
            raise RuntimeError("symbol not found")
        return pd.DataFrame({"ts": [1], "close": [1.0]})

    monkeypatch.setattr(schwab_data, "fetch_daily_bars", fake_fetch)
    monkeypatch.setattr(schwab_data, "push_daily_snapshot", lambda symbol, df: {"ok": True})

    result = schwab_server._advance_historical_backfill()

    assert result["pushed"] == 1
    assert result["failed"] == 1


def test_advance_historical_backfill_batch_size_limits_how_many_symbols_advance(monkeypatch, tmp_path):
    """A full ~12,000-symbol universe can't be pulled in one off-hours tick
    at Schwab's rate limit -- only SCHWAB_BACKFILL_BATCH_SIZE symbols should
    advance per call, leaving the rest for the next tick."""
    from data import schwab_data

    monkeypatch.setattr(schwab_data, "get_us_stock_universe", lambda: ["A", "B", "C", "D"])
    monkeypatch.setattr(schwab_data, "get_symbols_with_daily_bars", lambda: set())
    monkeypatch.setattr(schwab_server, "SCHWAB_BACKFILL_BATCH_SIZE", 2)
    monkeypatch.setattr(schwab_server, "SCHWAB_LATEST_BACKFILL_FILE", tmp_path / "backfill.json")

    fetched = []
    monkeypatch.setattr(schwab_data, "fetch_daily_bars", lambda symbol, years=20: fetched.append(symbol) or pd.DataFrame({"ts": [1], "close": [1.0]}))
    monkeypatch.setattr(schwab_data, "push_daily_snapshot", lambda symbol, df: {"ok": True})

    result = schwab_server._advance_historical_backfill()

    assert len(fetched) == 2
    assert result["batch_size"] == 2
    assert result["remaining_before"] == 4


def test_intensive_training_is_a_noop_while_market_is_open(monkeypatch):
    from data import schwab_data

    monkeypatch.setattr(schwab_data, "get_market_session", lambda: {"session": "regular", "is_open": True, "source": "fallback"})

    calls = []
    monkeypatch.setattr(schwab_server, "_advance_historical_backfill", lambda: calls.append(1))

    result = schwab_server._run_schwab_intensive_training.__wrapped__()  # noqa: SLF001

    assert result["skipped"] is True
    assert result["reason"] == "market_not_closed"
    assert calls == []  # backfill must not advance while the market is open either


def test_intensive_training_trains_and_advances_backfill_when_market_closed(monkeypatch, tmp_path):
    from data import schwab_data, schwab_model

    monkeypatch.setattr(schwab_data, "get_market_session", lambda: {"session": "closed", "is_open": False, "source": "fallback"})
    monkeypatch.setattr(schwab_model, "train_model", lambda: {"ok": True, "rows": 1000})
    monkeypatch.setattr(schwab_data, "load_training_dataset", lambda: pd.DataFrame())  # empty -> sweep skipped safely
    backfill_calls = []
    monkeypatch.setattr(schwab_server, "_advance_historical_backfill", lambda: backfill_calls.append(1) or {"action": "backfill_batch", "pushed": 5})

    result = schwab_server._run_schwab_intensive_training.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert result["train_result"] == {"ok": True, "rows": 1000}
    assert result["backfill_result"] == {"action": "backfill_batch", "pushed": 5}
    assert backfill_calls == [1]


def test_intensive_training_still_advances_backfill_if_sweep_raises(monkeypatch):
    """A backtest sweep failure (bad data, a fitting error) must not prevent
    the historical backfill from making progress that tick -- these are two
    independent pieces of off-hours work."""
    from data import schwab_data, schwab_model

    monkeypatch.setattr(schwab_data, "get_market_session", lambda: {"session": "closed", "is_open": False, "source": "fallback"})
    monkeypatch.setattr(schwab_model, "train_model", lambda: {"ok": True, "rows": 1000})

    def raise_error(**kw):
        raise RuntimeError("HF listing failed")

    monkeypatch.setattr(schwab_data, "load_training_dataset", raise_error)
    backfill_calls = []
    monkeypatch.setattr(schwab_server, "_advance_historical_backfill", lambda: backfill_calls.append(1) or {"action": "backfill_complete"})

    result = schwab_server._run_schwab_intensive_training.__wrapped__()  # noqa: SLF001

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
    monkeypatch.setattr(schwab_server, "scheduler", _FakeScheduler(True, lambda **kw: calls.append(kw)))
    schwab_server._shutdown_scheduler()
    assert calls == [{"wait": False}]


def test_shutdown_scheduler_is_a_noop_when_not_running(monkeypatch):
    def fail_if_called(**kw):
        raise AssertionError("must not call shutdown() on a scheduler that isn't running")

    monkeypatch.setattr(schwab_server, "scheduler", _FakeScheduler(False, fail_if_called))
    schwab_server._shutdown_scheduler()  # must not raise


def test_shutdown_scheduler_swallows_errors(monkeypatch):
    def raise_error(**kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(schwab_server, "scheduler", _FakeScheduler(True, raise_error))
    schwab_server._shutdown_scheduler()  # must not raise
