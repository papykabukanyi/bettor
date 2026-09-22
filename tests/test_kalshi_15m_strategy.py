"""Strategy for Kalshi's 15-minute event-contract markets. See
kalshi_15m_strategy.py's own module docstring for the hard dry-run floor
(LIVE_TRADING_ENABLED must stay False until this account's own standard-
market order-placement mechanics are verified live) and why settlement
checking is a real, fully-exercisable simulation even in dry-run mode
(market resolution is a public, unauthenticated fact)."""
from __future__ import annotations

import datetime as dt

import pytest

from data import crypto_correlation, kalshi_15m, kalshi_15m_meta_model, kalshi_15m_metals_model, kalshi_15m_model, kalshi_15m_strategy


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
def _mock_confident_prediction(monkeypatch, probability_up: float = 0.72) -> None:
    """Mocks BOTH model dispatch targets (crypto and metals -- see
    kalshi_15m_strategy._predict_direction's own docstring) identically,
    so a test exercising the FULL merged 8-asset universe doesn't
    accidentally only cover the 5 crypto ones."""
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": probability_up})
    monkeypatch.setattr(kalshi_15m_metals_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": probability_up})


def test_scan_and_enter_stays_dry_run_by_default(monkeypatch):
    # MAX_CONCURRENT_POSITIONS' own default (5) is deliberately smaller
    # than the full 8-asset universe -- raised here since capacity isn't
    # what this test is about (see test_scan_and_enter_respects_max_concurrent_positions
    # for that).
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", len(kalshi_15m_strategy.ASSET_SERIES))
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)
    order_calls = []
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: order_calls.append(kw))

    result = kalshi_15m_strategy.scan_and_enter()

    assert order_calls == []  # never a real order -- the hard dry-run floor
    entered = [c for c in result["checks"] if c.get("action") == "entered"]
    assert len(entered) == len(kalshi_15m_strategy.ASSET_SERIES)  # one per asset in the FULL merged universe
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
    _mock_confident_prediction(monkeypatch)
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)
    order_calls = []
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: order_calls.append(kw) or {"order": {"order_id": "o1"}})

    result = kalshi_15m_strategy.scan_and_enter(dry_run=True)  # caller-level override still wins

    assert order_calls == []
    entered = [c for c in result["checks"] if c.get("action") == "entered"]
    assert all(c["dry_run"] is True for c in entered)


def test_scan_and_enter_places_a_real_order_only_when_explicitly_forced_live(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", len(kalshi_15m_strategy.ASSET_SERIES))
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)
    order_calls = []
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: order_calls.append(kw) or {"order": {"order_id": "o1"}})
    # Real bug this whole file's own suite originally missed: without
    # mocking get_orders (the post-placement fill check), an unmocked
    # network call fails, filled_count stays 0, and NOTHING gets recorded
    # as "entered" -- the old version of this test's own final assertion
    # (all(... for c in entered)) was vacuously True on an EMPTY list, so
    # it kept "passing" without checking anything real. Mocking a real
    # fill here restores what this test actually claims to verify.
    monkeypatch.setattr(kalshi_15m, "get_orders", lambda ticker=None, status=None: [{"order_id": "o1", "fill_count_fp": "10.00"}])

    result = kalshi_15m_strategy.scan_and_enter(dry_run=False)

    assert len(order_calls) == len(kalshi_15m_strategy.ASSET_SERIES)  # one per asset in the FULL merged universe
    entered = [c for c in result["checks"] if c.get("action") == "entered"]
    assert len(entered) == len(kalshi_15m_strategy.ASSET_SERIES)
    assert all(c["dry_run"] is False for c in entered)


def test_scan_and_enter_unwraps_the_real_nested_create_order_response(monkeypatch):
    """SECOND real, live, confirmed bug found on this account (discovered
    hours after the side/price fix above shipped, by cross-checking
    /api/kalshi15m/real-positions -- two real, currently-open Kalshi
    positions -- against /api/kalshi15m/status showing zero tracked
    positions): Kalshi's own create-order response nests the order object
    under an "order" key (confirmed against this account's own real
    orders, and matching create_margin_order's own identical response
    shape for the same endpoint family -- see perps_strategy.py's own
    `order_result.get("order") or order_result` unwrap). The old code did
    `order_result.get("order_id")` directly, which was ALWAYS None on a
    real response -- that None then never matched any real order_id in
    the fresh_orders fill-check list, so filled_count stayed 0 and every
    real order (filled or not) was reported "order_not_filled" and
    silently dropped from local tracking, even though the order really
    filled and real money was really at risk. This test uses the REAL
    nested shape (every other test in this file already does too, post-
    fix) -- it would have failed against the old flat-only extraction."""
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: {"order": {"order_id": "real-nested-id"}})
    monkeypatch.setattr(kalshi_15m, "get_orders", lambda ticker=None, status=None: [{"order_id": "real-nested-id", "fill_count_fp": "5.00"}])

    result = kalshi_15m_strategy.scan_and_enter(dry_run=False)

    entered = [c for c in result["checks"] if c.get("action") == "entered"]
    assert len(entered) == 1
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1
    assert state["positions"][0]["order_id"] == "real-nested-id"
    assert state["positions"][0]["count"] == 5.0


def test_scan_and_enter_still_works_with_a_flat_create_order_response(monkeypatch):
    """Defensive fallback -- `order_result.get("order") or order_result`
    -- keeps working if Kalshi (or a future SDK version) ever returns the
    order flat at the top level instead of nested."""
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: {"order_id": "flat-id"})
    monkeypatch.setattr(kalshi_15m, "get_orders", lambda ticker=None, status=None: [{"order_id": "flat-id", "fill_count_fp": "5.00"}])

    result = kalshi_15m_strategy.scan_and_enter(dry_run=False)

    entered = [c for c in result["checks"] if c.get("action") == "entered"]
    assert len(entered) == 1
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["positions"][0]["order_id"] == "flat-id"


def test_scan_and_enter_records_a_failed_order_without_opening_a_position(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)

    def fail(**kw):
        raise RuntimeError("exchange rejected order")

    monkeypatch.setattr(kalshi_15m, "create_order", fail)
    result = kalshi_15m_strategy.scan_and_enter(dry_run=False)

    assert all(c.get("reason") == "order_failed" for c in result["checks"])
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["positions"] == []


# ---------------------------------------------------------------------------
# Real, live, confirmed bug found by cross-checking this account's own real
# Kalshi order history against this module's bookkeeping: a "no" decision
# used to submit side="bid" (buy YES) -- Kalshi's create-order-v2 `side`
# ALWAYS refers to the YES leg, so this executed as a real BUY-YES fill,
# the exact opposite of the intended "no" position, with real money. Fixed
# to side="ask" (sell YES) at price = 1 - no_ask. These tests pin down the
# exact side/price Kalshi actually receives -- the ORIGINAL bug shipped
# specifically because no test ever asserted these values.
# ---------------------------------------------------------------------------
def test_scan_and_enter_sends_the_correct_side_and_price_for_a_no_decision(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market(no_ask=0.60, no_bid=0.55))
    _mock_confident_prediction(monkeypatch, probability_up=0.25)  # -> "no"
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)
    order_calls = []
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: order_calls.append(kw) or {"order": {"order_id": "o1"}})
    monkeypatch.setattr(kalshi_15m, "get_orders", lambda ticker=None, status=None: [{"order_id": "o1", "fill_count_fp": "5.00"}])

    kalshi_15m_strategy.scan_and_enter(dry_run=False)

    assert len(order_calls) == 1
    call = order_calls[0]
    assert call["side"] == "ask"  # sell YES -- NOT "bid", the original bug
    assert call["price"] == pytest.approx(1.0 - 0.60)  # 1 - no_ask, NOT no_ask directly


def test_scan_and_enter_sends_the_correct_side_and_price_for_a_yes_decision(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market(no_ask=0.60, no_bid=0.55))
    _mock_confident_prediction(monkeypatch, probability_up=0.75)  # -> "yes"
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)
    order_calls = []
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: order_calls.append(kw) or {"order": {"order_id": "o1"}})
    monkeypatch.setattr(kalshi_15m, "get_orders", lambda ticker=None, status=None: [{"order_id": "o1", "fill_count_fp": "5.00"}])

    kalshi_15m_strategy.scan_and_enter(dry_run=False)

    assert len(order_calls) == 1
    call = order_calls[0]
    assert call["side"] == "bid"  # buy YES -- unchanged, was already correct
    assert call["price"] == pytest.approx(1.0 - 0.55)  # 1 - no_bid


def test_scan_and_enter_records_the_no_side_cost_basis_not_the_yes_sell_price(monkeypatch):
    """The SEPARATE real bug this also fixes: entry_price used to store
    whatever price Kalshi received (the YES-denominated sell price for a
    "no" position), which check_settlements' own count * (1 -
    entry_price) formula assumes is the cost basis IN THE HELD SIDE's OWN
    terms -- silently mispricing every "no" settlement even after fixing
    just the side/price sent to Kalshi."""
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market(no_ask=0.60, no_bid=0.55))
    _mock_confident_prediction(monkeypatch, probability_up=0.25)  # -> "no"
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: {"order": {"order_id": "o1"}})
    monkeypatch.setattr(kalshi_15m, "get_orders", lambda ticker=None, status=None: [{"order_id": "o1", "fill_count_fp": "5.00"}])

    kalshi_15m_strategy.scan_and_enter(dry_run=False)

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    position = state["positions"][0]
    assert position["entry_price"] == pytest.approx(0.60)  # no_ask -- the real NO cost basis
    assert position["count"] == 5.0  # the REAL fill count, not whatever was requested


def test_scan_and_enter_does_not_open_a_position_when_the_order_never_fills(monkeypatch):
    """Real, live, confirmed bug this fixes: this code used to record a
    "position" the instant create_order returned an order_id, with no
    check that the order actually filled -- an IOC order that crosses no
    one gets Kalshi's own status "canceled" with fill_count 0, and this
    was recording that as a real open position anyway."""
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: {"order": {"order_id": "o1"}})
    monkeypatch.setattr(kalshi_15m, "get_orders", lambda ticker=None, status=None: [{"order_id": "o1", "status": "canceled", "fill_count_fp": "0.00"}])

    result = kalshi_15m_strategy.scan_and_enter(dry_run=False)

    assert all(c.get("reason") == "order_not_filled" for c in result["checks"] if not c.get("ok"))
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["positions"] == []


def test_scan_and_enter_survives_a_fill_check_failure_without_opening_a_phantom_position(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: {"order": {"order_id": "o1"}})

    def fail(**kw):
        raise RuntimeError("network error checking fill")

    monkeypatch.setattr(kalshi_15m, "get_orders", fail)
    kalshi_15m_strategy.scan_and_enter(dry_run=False)

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["positions"] == []  # fails closed, never assumes a fill it couldn't verify


def test_scan_and_enter_dry_run_still_records_the_full_requested_count(monkeypatch):
    # Dry-run has no real order/fill to verify -- must keep simulating the
    # full requested size, not silently collapse to 0.
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market(no_ask=0.60, no_bid=0.55))
    _mock_confident_prediction(monkeypatch, probability_up=0.25)  # -> "no"
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)

    kalshi_15m_strategy.scan_and_enter()  # LIVE_TRADING_ENABLED is False -> dry_run

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    position = state["positions"][0]
    assert position["count"] > 0
    assert position["entry_price"] == pytest.approx(0.60)


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


# ---------------------------------------------------------------------------
# _predict_direction -- the dispatch between crypto's kalshi_15m_model and
# metals' own kalshi_15m_metals_model (see kalshi_15m_strategy.py's own
# module-level ASSET_SERIES comment for why these need genuinely
# different data/model pairs).
# ---------------------------------------------------------------------------
def test_predict_direction_dispatches_crypto_coins_to_the_crypto_model(monkeypatch):
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "source": "crypto", "coin": coin})
    monkeypatch.setattr(kalshi_15m_metals_model, "predict_direction", lambda coin: {"model_ok": True, "source": "metals", "coin": coin})
    result = kalshi_15m_strategy._predict_direction("BTC")  # noqa: SLF001
    assert result["source"] == "crypto"


def test_predict_direction_dispatches_metals_to_the_metals_model(monkeypatch):
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "source": "crypto", "coin": coin})
    monkeypatch.setattr(kalshi_15m_metals_model, "predict_direction", lambda coin: {"model_ok": True, "source": "metals", "coin": coin})
    for metal in ("GOLD", "SILVER", "COPPER"):
        result = kalshi_15m_strategy._predict_direction(metal)  # noqa: SLF001
        assert result["source"] == "metals"


def test_asset_series_covers_all_8_assets_with_no_overlap():
    assert len(kalshi_15m_strategy.ASSET_SERIES) == 8
    assert set(kalshi_15m_strategy.ASSET_SERIES) == {
        "BTC", "ETH", "SOL", "XRP", "DOGE", "GOLD", "SILVER", "COPPER",
    }


# ---------------------------------------------------------------------------
# Durable-state HF backup -- real, live, confirmed bug this closes: unlike
# every other market here, this module had NO HF backup for its own state
# at all. Confirmed live: a routine restart wiped 118 real trades and a
# real day's P&L total instantly. See HF_DURABLE_STATE_REPO's own comment.
# ---------------------------------------------------------------------------
class _FakeHfApi:
    captured_upload: dict = {}

    def __init__(self, token=None):
        pass

    def upload_file(self, *, path_or_fileobj, path_in_repo, repo_id, repo_type, commit_message):
        import json as _json
        _FakeHfApi.captured_upload.setdefault("uploads", []).append({
            "path_in_repo": path_in_repo, "repo_id": repo_id,
            "content": _json.loads(open(path_or_fileobj, encoding="utf-8").read()),
        })


def test_check_settlements_pushes_durable_state_for_a_real_settled_trade(monkeypatch):
    import huggingface_hub

    monkeypatch.setattr(kalshi_15m_strategy, "HF_API_KEY", "fake-hf-token")
    _FakeHfApi.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)

    state = {"positions": [_position(coin="BTC", side="yes", entry_price=0.5, dry_run=False, ticker="KXBTC15M-1")], "trade_log": [], "realized_pnl_by_date": {}}
    kalshi_15m_strategy._save_state(state)  # noqa: SLF001
    monkeypatch.setattr(kalshi_15m, "get_market", lambda ticker: {"ticker": ticker, "result": "yes"})

    kalshi_15m_strategy.check_settlements()

    uploads = _FakeHfApi.captured_upload["uploads"]
    assert len(uploads) == 1
    assert uploads[0]["repo_id"] == kalshi_15m_strategy.HF_DURABLE_STATE_REPO
    assert uploads[0]["path_in_repo"] == kalshi_15m_strategy._DURABLE_STATE_HF_FILENAME  # noqa: SLF001
    assert len(uploads[0]["content"]["trade_log"]) == 1


def test_check_settlements_does_not_push_durable_state_for_a_dry_run_trade(monkeypatch):
    import huggingface_hub

    monkeypatch.setattr(kalshi_15m_strategy, "HF_API_KEY", "fake-hf-token")
    _FakeHfApi.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)

    state = {"positions": [_position(coin="BTC", side="yes", entry_price=0.5, dry_run=True, ticker="KXBTC15M-1")], "trade_log": [], "realized_pnl_by_date": {}}
    kalshi_15m_strategy._save_state(state)  # noqa: SLF001
    monkeypatch.setattr(kalshi_15m, "get_market", lambda ticker: {"ticker": ticker, "result": "yes"})

    kalshi_15m_strategy.check_settlements()

    assert _FakeHfApi.captured_upload.get("uploads", []) == []


def test_load_state_restores_from_hf_when_local_file_is_missing(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "HF_API_KEY", "fake-hf-token")
    backup = {"positions": [], "trade_log": [{"coin": "BTC", "realized_pnl_usd": 1.5}], "realized_pnl_by_date": {"2026-09-21": 1.5}}

    import huggingface_hub
    tmp_file = kalshi_15m_strategy.DATA_DIR / "hf_backup_kalshi15m_state.json"
    tmp_file.write_text(__import__("json").dumps(backup), encoding="utf-8")
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: str(tmp_file))

    result = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert result["trade_log"] == backup["trade_log"]
    assert result["realized_pnl_by_date"] == backup["realized_pnl_by_date"]


def test_load_state_falls_back_to_empty_when_hf_has_no_backup_either(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "HF_API_KEY", "fake-hf-token")
    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: (_ for _ in ()).throw(RuntimeError("404")))

    result = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert result == {"positions": [], "trade_log": [], "realized_pnl_by_date": {}, "tuning": {}}


# ---------------------------------------------------------------------------
# _account_budget_usd -- real, live, confirmed bug this fixes: used to call
# get_portfolio_balance() with no exchange_index, which per Kalshi's own
# docs returns the balance POOLED ACROSS ALL SHARDS -- not what's actually
# usable on shard 2 specifically, where these markets settle orders.
# ---------------------------------------------------------------------------
def test_account_budget_usd_uses_the_shard_2_specific_balance_not_the_pooled_total(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    captured = {}

    def fake_get_balance_by_shard(*, exchange_index):
        captured["exchange_index"] = exchange_index
        return {"balance_dollars": "1.90"}

    monkeypatch.setattr(kalshi_15m, "get_balance_by_shard", fake_get_balance_by_shard)
    result = kalshi_15m_strategy._account_budget_usd()  # noqa: SLF001

    assert captured["exchange_index"] == 2
    assert result == pytest.approx(1.90)


def test_account_budget_usd_falls_back_to_placeholder_on_a_real_api_failure(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)

    def fail(*, exchange_index):
        raise RuntimeError("Kalshi API error 500")

    monkeypatch.setattr(kalshi_15m, "get_balance_by_shard", fail)
    assert kalshi_15m_strategy._account_budget_usd() == 100.0  # noqa: SLF001


def test_account_budget_usd_is_the_placeholder_in_dry_run():
    assert kalshi_15m_strategy._account_budget_usd() == 100.0  # noqa: SLF001 -- LIVE_TRADING_ENABLED is False by default


# ---------------------------------------------------------------------------
# evaluate_candidate's confidence_min override + scan_and_enter's own
# durable-state-driven tuning + apply_confidence_threshold_override +
# _maybe_run_batch_trade_analysis -- the "train to avoid bad positions"
# feature: an evidence-gated confidence floor genuinely learned from this
# account's own real trade history, same pattern every other market here
# already uses (see kalshi_15m_trade_analysis.py's own module docstring).
# ---------------------------------------------------------------------------
def test_evaluate_candidate_confidence_min_override_lets_a_lower_floor_through(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    # confidence 0.55 fails the real module default (0.58) but clears an
    # explicitly LOWERED override (0.50) -- proves the override, not the
    # default, decided this.
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.55})
    assert kalshi_15m_strategy.evaluate_candidate("BTC")["ok"] is False  # sanity: fails under the real default
    result = kalshi_15m_strategy.evaluate_candidate("BTC", confidence_min=0.50)
    assert result["ok"] is True


def test_evaluate_candidate_confidence_min_override_raises_the_floor_above_default(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.65})
    # 0.65 clears the real module default (0.58) but not an override raised
    # to 0.70 -- proves the override, when given, takes priority.
    result = kalshi_15m_strategy.evaluate_candidate("BTC", confidence_min=0.70)
    assert result["ok"] is False
    assert result["reason"] == "confidence_below_floor"
    assert result["confidence"] == pytest.approx(0.65)


def test_evaluate_candidate_defaults_to_the_module_floor_when_no_override_given(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.60})
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is True  # 0.60 clears the real 0.58 default


def test_scan_and_enter_reads_the_confidence_floor_learned_from_real_trade_history(monkeypatch):
    """A real trade-history-driven override in state["tuning"] must take
    priority over the module-level MODEL_CONFIDENCE_MIN default -- the
    whole point of apply_confidence_threshold_override existing."""
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", len(kalshi_15m_strategy.ASSET_SERIES))
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    # 0.63 confidence clears the real module default (0.58) but not the
    # learned override below (0.70).
    _mock_confident_prediction(monkeypatch, probability_up=0.63)
    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [], "trade_log": [], "realized_pnl_by_date": {},
        "tuning": {"model_confidence_min": 0.70},
    })

    result = kalshi_15m_strategy.scan_and_enter()

    entered = [c for c in result["checks"] if c.get("action") == "entered"]
    assert entered == []  # every candidate correctly rejected under the LEARNED, stricter floor
    rejected = [c for c in result["checks"] if c.get("reason") == "confidence_below_floor"]
    assert len(rejected) == len(kalshi_15m_strategy.ASSET_SERIES)


def test_apply_confidence_threshold_override_persists_the_new_threshold(monkeypatch):
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001

    applied = kalshi_15m_strategy.apply_confidence_threshold_override(0.66, reason="test evidence")

    assert applied["model_confidence_min"] == 0.66
    assert applied["reason"] == "test evidence"
    assert applied["previous"] == kalshi_15m_strategy.MODEL_CONFIDENCE_MIN
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["tuning"]["model_confidence_min"] == 0.66


def test_apply_confidence_threshold_override_pushes_durable_state(monkeypatch):
    import huggingface_hub

    monkeypatch.setattr(kalshi_15m_strategy, "HF_API_KEY", "fake-hf-token")
    _FakeHfApi.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001

    kalshi_15m_strategy.apply_confidence_threshold_override(0.66, reason="test evidence")

    uploads = _FakeHfApi.captured_upload["uploads"]
    assert len(uploads) == 1
    assert uploads[0]["content"]["tuning"]["model_confidence_min"] == 0.66


def test_maybe_run_batch_trade_analysis_noops_before_5_new_real_trades():
    real_trades = [
        {"coin": "BTC", "side": "yes", "realized_pnl_usd": 1.0, "dry_run": False, "entry_confidence": 0.6,
         "opened_at": _future_close(0), "closed_at": _future_close(0)}
        for _ in range(4)
    ]
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": real_trades, "realized_pnl_by_date": {}})  # noqa: SLF001

    result = kalshi_15m_strategy._maybe_run_batch_trade_analysis()  # noqa: SLF001

    assert result is None


def test_maybe_run_batch_trade_analysis_runs_at_5_new_real_trades_and_applies_tuning(monkeypatch):
    from data import kalshi_15m_trade_analysis

    real_trades = [
        {"coin": "BTC", "side": "yes", "realized_pnl_usd": 1.0, "dry_run": False, "entry_confidence": 0.6,
         "opened_at": _future_close(0), "closed_at": _future_close(0)}
        for _ in range(5)
    ]
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": real_trades, "realized_pnl_by_date": {}})  # noqa: SLF001
    monkeypatch.setattr(
        kalshi_15m_trade_analysis, "recommend_confidence_threshold",
        lambda trade_log, *, current_threshold: {"ok": True, "should_apply": True, "recommended_threshold": 0.66},
    )

    result = kalshi_15m_strategy._maybe_run_batch_trade_analysis()  # noqa: SLF001

    assert result is not None
    assert result["trades_analyzed"] == 5
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["tuning"]["model_confidence_min"] == 0.66
    assert state["last_batch_analysis_trade_count"] == 5


def test_maybe_run_batch_trade_analysis_does_not_re_run_until_5_more_real_trades_land():
    real_trades = [
        {"coin": "BTC", "side": "yes", "realized_pnl_usd": 1.0, "dry_run": False, "entry_confidence": 0.6,
         "opened_at": _future_close(0), "closed_at": _future_close(0)}
        for _ in range(5)
    ]
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": real_trades, "realized_pnl_by_date": {}})  # noqa: SLF001
    first = kalshi_15m_strategy._maybe_run_batch_trade_analysis()  # noqa: SLF001
    assert first is not None

    second = kalshi_15m_strategy._maybe_run_batch_trade_analysis()  # noqa: SLF001
    assert second is None  # no new real trades since the last run


def test_check_settlements_triggers_the_batch_analysis_pass(monkeypatch):
    """Wiring check -- check_settlements must call
    _maybe_run_batch_trade_analysis after booking a settlement. (That
    function's own internal try/except, exercised separately above, is
    what keeps a real failure there from ever affecting settlement
    booking -- same "callee owns its own safety" convention every sibling
    market's identical call site already uses, e.g.
    alpaca_options_strategy.manage_open_positions's own un-wrapped call.)"""
    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [_position(coin="BTC", side="yes", entry_price=0.5, dry_run=False, ticker="KXBTC15M-1")],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(kalshi_15m, "get_market", lambda ticker: {"ticker": ticker, "result": "yes"})

    called = {"n": 0}
    monkeypatch.setattr(kalshi_15m_strategy, "_maybe_run_batch_trade_analysis", lambda: called.__setitem__("n", called["n"] + 1))
    result = kalshi_15m_strategy.check_settlements()

    assert result["ok"] is True
    assert called["n"] == 1


# ---------------------------------------------------------------------------
# Correlation-study confidence layer -- reuses perps' own already-running
# in-process correlation study (crypto_correlation.perps_correlation_bullishness)
# for kalshi_15m's crypto coins only. Off by default (USE_CORRELATION_STUDY);
# computed and attached unconditionally for observability either way -- see
# crypto_correlation.py's own module docstring and
# test_perps_strategy.py's own identical test family for the pattern this
# mirrors.
# ---------------------------------------------------------------------------
def test_evaluate_candidate_attaches_correlation_reading_for_a_crypto_coin_even_when_flag_off(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    monkeypatch.setattr(
        crypto_correlation, "perps_correlation_bullishness",
        lambda coin, row=None: {"score": 0.9, "reason": "strong confirmation", "components": {}},
    )
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is True
    assert result["correlation_score"] == 0.9
    assert result["correlation_reason"] == "strong confirmation"


def test_evaluate_candidate_never_calls_the_correlation_study_for_a_metals_coin(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_metals_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    called = {"n": 0}

    def _boom(coin, row=None):
        called["n"] += 1
        raise AssertionError("should never be called for a metals coin")

    monkeypatch.setattr(crypto_correlation, "perps_correlation_bullishness", _boom)
    result = kalshi_15m_strategy.evaluate_candidate("GOLD")
    assert result["ok"] is True
    assert result["correlation_score"] == 0.0
    assert result["correlation_reason"] is None
    assert called["n"] == 0


def test_evaluate_candidate_correlation_confirmation_lowers_the_bar_when_flag_on(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    # probability_up=0.55 alone would miss the real default (0.58) by 0.03
    # -- within CORRELATION_CONFIDENCE_MAX_ADJUSTMENT (0.06), so a maximally
    # bullish correlation reading should be enough to clear it.
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.55})
    monkeypatch.setattr(
        crypto_correlation, "perps_correlation_bullishness",
        lambda coin, row=None: {"score": 1.0, "reason": "max confirmation", "components": {}},
    )
    assert kalshi_15m_strategy.evaluate_candidate("BTC")["ok"] is False  # sanity: fails with the study off

    result = kalshi_15m_strategy.evaluate_candidate("BTC", correlation_study_enabled=True)
    assert result["ok"] is True


def test_evaluate_candidate_correlation_disagreement_raises_the_bar_when_flag_on(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    # probability_up=0.60 alone clears the real default (0.58) -- a
    # maximally bearish correlation reading raises the bar past it
    # (0.58 + 0.06 = 0.64), so this must now be rejected.
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.60})
    monkeypatch.setattr(
        crypto_correlation, "perps_correlation_bullishness",
        lambda coin, row=None: {"score": -1.0, "reason": "max disagreement", "components": {}},
    )
    assert kalshi_15m_strategy.evaluate_candidate("BTC")["ok"] is True  # sanity: passes with the study off

    result = kalshi_15m_strategy.evaluate_candidate("BTC", correlation_study_enabled=True)
    assert result["ok"] is False


def test_evaluate_candidate_correlation_score_flips_sign_for_a_no_decision(monkeypatch):
    """Bullish-signed at the source -- must flip for a "no" (down)
    decision, same convention crypto_correlation.py documents for a perps
    short: a maximally BEARISH reading should CONFIRM (lower the bar for)
    a "no" decision, not disagree with it."""
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    # probability_up=0.45 -> side="no", confidence=0.55, which misses the
    # real default (0.58) by 0.03 without help.
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.45})
    monkeypatch.setattr(
        crypto_correlation, "perps_correlation_bullishness",
        lambda coin, row=None: {"score": -1.0, "reason": "max bearish confirmation", "components": {}},
    )
    result = kalshi_15m_strategy.evaluate_candidate("BTC", correlation_study_enabled=True)
    assert result["ok"] is True
    assert result["side"] == "no"


def test_evaluate_candidate_passes_the_predictions_own_feature_row_to_the_correlation_study(monkeypatch):
    """No second, real (candle fetch + sentiment + feature engineering)
    latest_feature_row call for the same coin/cycle -- reuses the row
    predict_direction already computed."""
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    sentinel_row = {"ret_5m": 0.01, "trend_1h": 0.02}
    monkeypatch.setattr(
        kalshi_15m_model, "predict_direction",
        lambda coin: {"model_ok": True, "probability_up": 0.72, "feature_row": sentinel_row},
    )
    captured = {}

    def _capture(coin, row=None):
        captured["row"] = row
        return {"score": 0.0, "reason": "neutral", "components": {}}

    monkeypatch.setattr(crypto_correlation, "perps_correlation_bullishness", _capture)
    kalshi_15m_strategy.evaluate_candidate("BTC")
    assert captured["row"] is sentinel_row


def test_apply_correlation_study_override_persists_only_the_fields_given(monkeypatch):
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001

    applied = kalshi_15m_strategy.apply_correlation_study_override(enabled=True, reason="test evidence")
    assert applied["correlation_study_enabled"] is True
    assert "correlation_confidence_max_adjustment" not in applied

    applied2 = kalshi_15m_strategy.apply_correlation_study_override(max_adjustment=0.09, reason="more evidence")
    assert applied2["correlation_study_enabled"] is True  # untouched by the second call
    assert applied2["correlation_confidence_max_adjustment"] == 0.09


def test_apply_confidence_threshold_override_and_apply_correlation_study_override_coexist(monkeypatch):
    """Regression test for a real bug already found and fixed once in
    perps_strategy.py's own identical pair: a wholesale state["tuning"]
    replace in either function would silently erase the other's keys."""
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001

    kalshi_15m_strategy.apply_confidence_threshold_override(0.66, reason="confidence evidence")
    kalshi_15m_strategy.apply_correlation_study_override(enabled=True, max_adjustment=0.09, reason="correlation evidence")

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["tuning"]["model_confidence_min"] == 0.66  # survived the correlation override
    assert state["tuning"]["correlation_study_enabled"] is True
    assert state["tuning"]["correlation_confidence_max_adjustment"] == 0.09

    kalshi_15m_strategy.apply_confidence_threshold_override(0.70, reason="more confidence evidence")
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["tuning"]["model_confidence_min"] == 0.70
    assert state["tuning"]["correlation_study_enabled"] is True  # survived the SECOND confidence override too
    assert state["tuning"]["correlation_confidence_max_adjustment"] == 0.09


def test_scan_and_enter_reads_the_correlation_override_from_state_tuning(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", len(kalshi_15m_strategy.ASSET_SERIES))
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch, probability_up=0.60)
    monkeypatch.setattr(
        crypto_correlation, "perps_correlation_bullishness",
        lambda coin, row=None: {"score": -1.0, "reason": "max disagreement", "components": {}},
    )
    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [], "trade_log": [], "realized_pnl_by_date": {},
        "tuning": {"correlation_study_enabled": True, "correlation_confidence_max_adjustment": 0.06},
    })

    result = kalshi_15m_strategy.scan_and_enter()

    entered = {c["coin"] for c in result["checks"] if c.get("action") == "entered"}
    # Every CRYPTO candidate rejected: 0.60 confidence - 0.06 disagreement
    # penalty < 0.58 floor. Metals coins have no correlation study (see
    # USE_CORRELATION_STUDY's own comment) and still enter normally at
    # 0.60 >= the real 0.58 default.
    assert entered.isdisjoint(kalshi_15m.KNOWN_15M_SERIES)
    assert entered == set(kalshi_15m.KNOWN_15M_METALS_SERIES)


def test_scan_and_enter_records_the_entry_correlation_score_on_the_position(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", len(kalshi_15m_strategy.ASSET_SERIES))
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch, probability_up=0.72)
    monkeypatch.setattr(
        crypto_correlation, "perps_correlation_bullishness",
        lambda coin, row=None: {"score": 0.4, "reason": "some confirmation", "components": {}},
    )
    kalshi_15m_strategy.scan_and_enter()
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    btc_position = next(p for p in state["positions"] if p["coin"] == "BTC")
    assert btc_position["entry_correlation_score"] == 0.4
    gold_position = next(p for p in state["positions"] if p["coin"] == "GOLD")
    assert gold_position["entry_correlation_score"] == 0.0  # no correlation study for metals


def test_maybe_run_batch_trade_analysis_applies_correlation_tuning_too(monkeypatch):
    from data import kalshi_15m_trade_analysis

    real_trades = [
        {"coin": "BTC", "side": "yes", "realized_pnl_usd": 1.0, "dry_run": False, "entry_confidence": 0.6,
         "entry_correlation_score": 0.5, "opened_at": _future_close(0), "closed_at": _future_close(0)}
        for _ in range(5)
    ]
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": real_trades, "realized_pnl_by_date": {}})  # noqa: SLF001
    monkeypatch.setattr(
        kalshi_15m_trade_analysis, "recommend_confidence_threshold",
        lambda trade_log, *, current_threshold: {"ok": True, "should_apply": False},
    )
    monkeypatch.setattr(
        kalshi_15m_trade_analysis, "recommend_correlation_study_weight",
        lambda trade_log, *, current_enabled, current_max_adjustment: {
            "ok": True, "should_apply": True, "action": "enable",
            "recommended_enabled": True, "recommended_max_adjustment": current_max_adjustment,
        },
    )

    kalshi_15m_strategy._maybe_run_batch_trade_analysis()  # noqa: SLF001

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["tuning"]["correlation_study_enabled"] is True


# ---------------------------------------------------------------------------
# Meta-labeling layer (USE_META_MODEL) -- see kalshi_15m_meta_model.py's own
# module docstring. Off by default; crypto only (no metals meta-model).
# ---------------------------------------------------------------------------
def test_evaluate_candidate_meta_model_disabled_by_default_never_calls_trust_score(monkeypatch):
    assert kalshi_15m_strategy.USE_META_MODEL is False  # module default -- not touched by this test
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})

    def fail_if_called(row, *, primary_probability_up):
        raise AssertionError("must not call trust_score while USE_META_MODEL is off")

    monkeypatch.setattr(kalshi_15m_meta_model, "trust_score", fail_if_called)
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is True
    assert "meta_trust_score" not in result


def test_evaluate_candidate_meta_model_blocks_entry_on_low_trust(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "USE_META_MODEL", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    monkeypatch.setattr(kalshi_15m_meta_model, "trust_score", lambda row, *, primary_probability_up: 0.1)

    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is False
    assert result["reason"] == "meta_model_trust_too_low"
    assert result["meta_trust_score"] == 0.1


def test_evaluate_candidate_meta_model_allows_entry_on_high_trust(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "USE_META_MODEL", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    monkeypatch.setattr(kalshi_15m_meta_model, "trust_score", lambda row, *, primary_probability_up: 0.9)

    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is True
    assert result["meta_trust_score"] == 0.9


def test_evaluate_candidate_meta_model_fails_open_with_no_trust_score_available(monkeypatch):
    """None (no meta-model trained yet, or a row missing context
    features) must never block an otherwise-qualifying entry."""
    monkeypatch.setattr(kalshi_15m_strategy, "USE_META_MODEL", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    monkeypatch.setattr(kalshi_15m_meta_model, "trust_score", lambda row, *, primary_probability_up: None)

    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is True
    assert "meta_trust_score" not in result


def test_evaluate_candidate_meta_model_never_calls_trust_score_for_a_metals_coin(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "USE_META_MODEL", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_metals_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})

    def fail_if_called(row, *, primary_probability_up):
        raise AssertionError("must not call trust_score for a metals coin -- no metals meta-model exists")

    monkeypatch.setattr(kalshi_15m_meta_model, "trust_score", fail_if_called)
    result = kalshi_15m_strategy.evaluate_candidate("GOLD")
    assert result["ok"] is True


# ---------------------------------------------------------------------------
# Conviction sizing -- the one of perps' 3 position-management-trial
# features that maps onto this market's product (see USE_CONVICTION_SIZING's
# own comment on why scale-in/partial-exit have no kalshi_15m equivalent).
# ---------------------------------------------------------------------------
def test_compute_conviction_size_multiplier_returns_one_with_missing_inputs():
    assert kalshi_15m_strategy.compute_conviction_size_multiplier(None, 0.58) == 1.0
    assert kalshi_15m_strategy.compute_conviction_size_multiplier(0.7, None) == 1.0
    assert kalshi_15m_strategy.compute_conviction_size_multiplier(0.7, 1.0) == 1.0  # division-by-zero guard


def test_compute_conviction_size_multiplier_at_the_floor_is_the_min_multiplier():
    result = kalshi_15m_strategy.compute_conviction_size_multiplier(0.58, 0.58)
    assert result == pytest.approx(kalshi_15m_strategy.CONVICTION_SIZE_MIN_MULTIPLIER)


def test_compute_conviction_size_multiplier_at_maximum_conviction_is_the_max_multiplier():
    result = kalshi_15m_strategy.compute_conviction_size_multiplier(1.0, 0.58)
    assert result == pytest.approx(kalshi_15m_strategy.CONVICTION_SIZE_MAX_MULTIPLIER)


def test_compute_conviction_size_multiplier_is_monotonic_in_conviction():
    low = kalshi_15m_strategy.compute_conviction_size_multiplier(0.60, 0.58)
    high = kalshi_15m_strategy.compute_conviction_size_multiplier(0.80, 0.58)
    assert low < high


def test_evaluate_candidate_reports_the_effective_confidence_min_for_sizing(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["effective_confidence_min"] == kalshi_15m_strategy.MODEL_CONFIDENCE_MIN


def test_apply_conviction_sizing_override_persists_the_flag(monkeypatch):
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001
    applied = kalshi_15m_strategy.apply_conviction_sizing_override(enabled=True, reason="test evidence")
    assert applied["conviction_sizing_enabled"] is True
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["tuning"]["conviction_sizing_enabled"] is True


def test_all_three_tuning_overrides_coexist(monkeypatch):
    """Regression test extending test_apply_confidence_threshold_override_and_apply_correlation_study_override_coexist
    to all 3 evidence-gated tunes -- none may wipe another's keys."""
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001

    kalshi_15m_strategy.apply_confidence_threshold_override(0.66, reason="confidence evidence")
    kalshi_15m_strategy.apply_correlation_study_override(enabled=True, max_adjustment=0.09, reason="correlation evidence")
    kalshi_15m_strategy.apply_conviction_sizing_override(enabled=True, reason="sizing evidence")

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    tuning = state["tuning"]
    assert tuning["model_confidence_min"] == 0.66
    assert tuning["correlation_study_enabled"] is True
    assert tuning["correlation_confidence_max_adjustment"] == 0.09
    assert tuning["conviction_sizing_enabled"] is True


def test_scan_and_enter_sizes_a_high_conviction_entry_larger_when_enabled(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market(no_ask=0.51, no_bid=0.5))
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.99})
    monkeypatch.setattr(crypto_correlation, "perps_correlation_bullishness", lambda coin, row=None: {"score": 0.0, "reason": "neutral", "components": {}})
    # LIVE_TRADING_ENABLED stays False (module default) -- _account_budget_usd()
    # then returns its fixed 100.0 placeholder with no real network call,
    # which is exactly the deterministic budget this test's own math below
    # assumes.

    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [], "trade_log": [], "realized_pnl_by_date": {}, "tuning": {"conviction_sizing_enabled": True},
    })
    kalshi_15m_strategy.scan_and_enter(dry_run=True)
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    entered = state["positions"][0]

    # Without conviction sizing, count would be int(100 * 0.05 / 0.5) = 10.
    # At max conviction (probability_up=0.99 vs a 0.58 floor), the
    # multiplier is CONVICTION_SIZE_MAX_MULTIPLIER (1.5x) -> 15.
    assert entered["count"] > 10
    assert entered["entry_conviction_sizing_enabled"] is True


def test_scan_and_enter_records_conviction_sizing_disabled_by_default(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    kalshi_15m_strategy.scan_and_enter()
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["positions"][0]["entry_conviction_sizing_enabled"] is False
