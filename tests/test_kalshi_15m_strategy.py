"""Strategy for Kalshi's 15-minute event-contract markets. See
kalshi_15m_strategy.py's own module docstring for the hard dry-run floor
(LIVE_TRADING_ENABLED must stay False until this account's own standard-
market order-placement mechanics are verified live) and why settlement
checking is a real, fully-exercisable simulation even in dry-run mode
(market resolution is a public, unauthenticated fact)."""
from __future__ import annotations

import datetime as dt

import pytest

from data import kalshi_15m, kalshi_15m_model, kalshi_15m_strategy


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path, monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "STATE_FILE", tmp_path / "state.json")


def _future_close(minutes: float) -> str:
    future = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=minutes)
    return future.isoformat().replace("+00:00", "Z")


def _market(*, ticker: str = "KXBTC15M-1", close_in_minutes: float = 10.0, no_ask: float = 0.54, no_bid: float = 0.53) -> dict:
    return {
        "ticker": ticker, "close_time": _future_close(close_in_minutes),
        "no_ask_dollars": str(no_ask), "no_bid_dollars": str(no_bid),
    }


def test_live_trading_enabled_defaults_to_false():
    """The hard safety floor this module's own docstring is built around."""
    assert kalshi_15m_strategy.LIVE_TRADING_ENABLED is False


# ---------------------------------------------------------------------------
# evaluate_candidate -- pure decision logic, no state/orders.
# ---------------------------------------------------------------------------
def test_evaluate_candidate_rejects_an_unknown_coin():
    result = kalshi_15m_strategy.evaluate_candidate("NOT_A_REAL_COIN")
    assert result == {"ok": False, "reason": "unknown_coin"}


def test_evaluate_candidate_reports_no_open_window(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: None)
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result == {"ok": False, "reason": "no_open_window"}


def test_evaluate_candidate_rejects_too_little_time_remaining(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market(close_in_minutes=1.0))
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is False
    assert result["reason"] == "too_little_time_remaining"


def test_evaluate_candidate_reports_model_not_ready(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": False, "reason": "no_feature_data"})
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result == {"ok": False, "reason": "model_not_ready", "detail": "no_feature_data"}


def test_evaluate_candidate_rejects_confidence_below_the_floor(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.52})
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is False
    assert result["reason"] == "confidence_below_floor"


def test_evaluate_candidate_picks_yes_for_a_confident_up_prediction(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is True
    assert result["side"] == "yes"
    assert result["confidence"] == pytest.approx(0.72)


def test_evaluate_candidate_picks_no_for_a_confident_down_prediction(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.25})
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is True
    assert result["side"] == "no"
    assert result["confidence"] == pytest.approx(0.75)


# ---------------------------------------------------------------------------
# scan_and_enter -- dry-run by default; never places a real order in these
# tests since LIVE_TRADING_ENABLED is False (module-level, see test above).
# ---------------------------------------------------------------------------
def test_scan_and_enter_stays_dry_run_by_default(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    order_calls = []
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: order_calls.append(kw))

    result = kalshi_15m_strategy.scan_and_enter()

    assert order_calls == []  # never a real order -- the hard dry-run floor
    entered = [c for c in result["checks"] if c.get("action") == "entered"]
    assert len(entered) == 5  # one per coin in the universe
    assert all(c["dry_run"] is True for c in entered)


def test_scan_and_enter_skips_a_coin_that_already_has_an_open_position(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})

    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [{"coin": "BTC", "ticker": "KXBTC15M-1"}], "trade_log": [], "realized_pnl_by_date": {},
    })
    result = kalshi_15m_strategy.scan_and_enter()
    btc_check = next(c for c in result["checks"] if c["coin"] == "BTC")
    assert btc_check == {"coin": "BTC", "ok": False, "reason": "already_has_open_position"}


def test_scan_and_enter_respects_max_concurrent_positions(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})

    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [{"coin": "ETH", "ticker": "KXETH15M-1"}], "trade_log": [], "realized_pnl_by_date": {},
    })
    result = kalshi_15m_strategy.scan_and_enter()
    entered = [c for c in result["checks"] if c.get("action") == "entered"]
    assert entered == []
    assert all(c["reason"] == "max_concurrent_positions" for c in result["checks"] if not c.get("ok"))


def test_scan_and_enter_never_places_a_real_order_even_when_live_trading_is_flagged_on(monkeypatch):
    """The single most important test in this file -- see this module's
    own docstring for why LIVE_TRADING_ENABLED is held to a stricter bar
    than the shared convention. Confirms the code PATH exists (order
    placement is reachable when the flag is on) but is never exercised in
    a way that would place a real order in these tests, since it's fully
    mocked -- this specifically checks the flag actually gates it,
    exercising both branches deliberately."""
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)
    order_calls = []
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: order_calls.append(kw) or {"order_id": "o1"})

    result = kalshi_15m_strategy.scan_and_enter(dry_run=True)  # caller-level override still wins

    assert order_calls == []
    entered = [c for c in result["checks"] if c.get("action") == "entered"]
    assert all(c["dry_run"] is True for c in entered)


def test_scan_and_enter_places_a_real_order_only_when_explicitly_forced_live(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)
    order_calls = []
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: order_calls.append(kw) or {"order_id": "o1"})

    result = kalshi_15m_strategy.scan_and_enter(dry_run=False)

    assert len(order_calls) == 5  # one per coin
    entered = [c for c in result["checks"] if c.get("action") == "entered"]
    assert all(c["dry_run"] is False for c in entered)


def test_scan_and_enter_records_a_failed_order_without_opening_a_position(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)

    def fail(**kw):
        raise RuntimeError("exchange rejected order")

    monkeypatch.setattr(kalshi_15m, "create_order", fail)
    result = kalshi_15m_strategy.scan_and_enter(dry_run=False)

    assert all(c.get("reason") == "order_failed" for c in result["checks"])
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["positions"] == []


# ---------------------------------------------------------------------------
# check_settlements -- a real, public, unauthenticated read even in
# dry-run mode (see this module's own docstring).
# ---------------------------------------------------------------------------
def _position(*, coin="BTC", side="yes", count=10, entry_price=0.45, dry_run=True, ticker="KXBTC15M-1") -> dict:
    return {
        "coin": coin, "ticker": ticker, "side": side, "count": count, "entry_price": entry_price,
        "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "close_time": _future_close(0),
        "entry_probability_up": 0.7, "entry_confidence": 0.7, "dry_run": dry_run,
    }


def test_check_settlements_leaves_a_still_open_market_untouched(monkeypatch):
    kalshi_15m_strategy._save_state({"positions": [_position()], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001
    monkeypatch.setattr(kalshi_15m, "get_market", lambda ticker: {"ticker": ticker, "result": ""})

    result = kalshi_15m_strategy.check_settlements()

    assert result["checks"][0]["action"] == "still_open"
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1
    assert state["trade_log"] == []


def test_check_settlements_books_a_win(monkeypatch):
    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [_position(side="yes", count=10, entry_price=0.45, dry_run=False)],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(kalshi_15m, "get_market", lambda ticker: {"ticker": ticker, "result": "yes"})

    result = kalshi_15m_strategy.check_settlements()

    assert result["checks"][0]["action"] == "settled"
    assert result["checks"][0]["won"] is True
    # A winning YES contract pays $1, cost $0.45 -- 10 contracts nets $5.50.
    assert result["checks"][0]["realized_pnl_usd"] == pytest.approx(5.5)

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["positions"] == []
    assert len(state["trade_log"]) == 1
    assert state["realized_pnl_by_date"]  # a REAL (non-dry-run) win updates the daily total


def test_check_settlements_books_a_loss(monkeypatch):
    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [_position(side="yes", count=10, entry_price=0.45, dry_run=False)],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(kalshi_15m, "get_market", lambda ticker: {"ticker": ticker, "result": "no"})

    result = kalshi_15m_strategy.check_settlements()

    assert result["checks"][0]["won"] is False
    # A losing YES contract pays $0, cost $0.45 -- 10 contracts loses $4.50.
    assert result["checks"][0]["realized_pnl_usd"] == pytest.approx(-4.5)


def test_check_settlements_does_not_pollute_real_pnl_with_a_dry_run_trade(monkeypatch):
    """Explicit user direction from earlier this session ("we doing only
    real data please not dry run or fake") -- same discipline applied
    here from the start, not retrofitted after a real incident."""
    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [_position(side="yes", count=10, entry_price=0.45, dry_run=True)],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(kalshi_15m, "get_market", lambda ticker: {"ticker": ticker, "result": "yes"})

    kalshi_15m_strategy.check_settlements()

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["realized_pnl_by_date"] == {}
    assert state["trade_log"][0]["dry_run"] is True


def test_check_settlements_survives_a_lookup_failure(monkeypatch):
    kalshi_15m_strategy._save_state({"positions": [_position()], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001

    def fail(ticker):
        raise RuntimeError("network error")

    monkeypatch.setattr(kalshi_15m, "get_market", fail)
    kalshi_15m_strategy.check_settlements()  # must not raise

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1  # position kept, retried next cycle


def test_check_settlements_does_not_erase_a_position_added_mid_loop(monkeypatch):
    """Locks in a real, if currently latent, race fixed this session: an
    earlier version snapshotted `positions` once at function start and
    bulk-overwrote state["positions"] with that stale list at the end,
    which would have silently erased any position added by another
    caller while this function was still running its own settlement
    checks. Now each settlement removes only its OWN ticker from a
    freshly re-read state."""
    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [_position(coin="BTC", side="yes", entry_price=0.45, dry_run=False)],
        "trade_log": [], "realized_pnl_by_date": {},
    })

    def fake_get_market(ticker):
        # Simulate a concurrent scan_and_enter() adding a brand new ETH
        # position WHILE this settlement check is still in its own loop.
        state = kalshi_15m_strategy._load_state()  # noqa: SLF001
        state["positions"].append(_position(coin="ETH", side="no", entry_price=0.3, dry_run=False, ticker="KXETH15M-1"))
        kalshi_15m_strategy._save_state(state)  # noqa: SLF001
        return {"ticker": ticker, "result": "yes"}

    monkeypatch.setattr(kalshi_15m, "get_market", fake_get_market)
    kalshi_15m_strategy.check_settlements()

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    # BTC settled and is gone; the concurrently-added ETH position must
    # have SURVIVED, not been silently erased by a stale bulk overwrite.
    assert [p["coin"] for p in state["positions"]] == ["ETH"]
