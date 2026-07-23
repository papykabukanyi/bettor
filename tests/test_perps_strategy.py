"""Kalshi Perps strategy decision logic. Pure-function / mocked-network
tests covering the actual entry/exit rules: dip detection, the bearish trend
filter, the direction-model override, take-profit, stop-loss, velocity-based
quick-profit, max-hold-time exits, dry-run gating, leveraged position
sizing, multi-position slot management, and the percentage-based daily loss
cap. If any of these regress, a live (even dry-run) cycle could silently do
the wrong thing with a leveraged product -- so the core decisions are locked
down here."""
from __future__ import annotations

import datetime as dt

import pytest

from data import perps_strategy as strat


@pytest.fixture(autouse=True)
def _no_external_price_network_calls(monkeypatch):
    """crypto_prices.get_fast_price hits real exchanges (Coinbase/Kraken) --
    every test here defaults it to "unavailable" so the suite never touches
    the network; tests that specifically exercise the external-price
    integration override this explicitly."""
    monkeypatch.setattr(strat, "get_fast_price", lambda coin: None)


def _row(**overrides):
    base = {"ticker": "KXBTCPERP", "current_price": 6.60, "short_ma": 6.63, "trend_pct": 0.0}
    base.update(overrides)
    return base


def test_dip_in_flat_trend_triggers_technical_entry():
    should_enter, reason = strat.decide_entry_technical(_row())
    assert should_enter
    assert "dip" in reason


def test_dip_in_strong_downtrend_is_filtered_out():
    should_enter, reason = strat.decide_entry_technical(_row(trend_pct=-0.05))
    assert not should_enter
    assert "trend filter" in reason


def test_negligible_move_does_not_trigger_technical_entry():
    should_enter, _ = strat.decide_entry_technical(_row(current_price=6.632, short_ma=6.633))
    assert not should_enter


def _position(entry_price=6.60, minutes_ago=0, ticker="KXBTCPERP", count=1.0, side=None):
    opened = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=minutes_ago)
    pos = {"ticker": ticker, "entry_price": entry_price, "count": count, "opened_at": opened.isoformat()}
    if side is not None:
        pos["side"] = side
    return pos


def test_take_profit_exit():
    pos = _position()
    should_exit, reason = strat.decide_exit(pos, 6.60 * (1 + strat.TAKE_PROFIT_PCT + 0.001))
    assert should_exit and "take_profit" in reason


def test_stop_loss_exit():
    pos = _position()
    should_exit, reason = strat.decide_exit(pos, 6.60 * (1 - strat.STOP_LOSS_PCT - 0.001))
    assert should_exit and "stop_loss" in reason


def test_max_hold_time_forces_exit_even_at_small_gain():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(pos, 6.601)
    assert should_exit and "max_hold_time" in reason


def test_holds_when_nothing_triggered():
    pos = _position(minutes_ago=1)
    should_exit, reason = strat.decide_exit(pos, 6.605)
    assert not should_exit
    assert "holding" in reason


def test_quick_profit_exit_triggers_on_fast_favorable_velocity():
    pos = _position()
    # Gain is above QUICK_PROFIT_PCT but below the standard TAKE_PROFIT_PCT --
    # only the velocity signal should be able to trigger this exit.
    price = 6.60 * (1 + strat.QUICK_PROFIT_PCT + 0.0002)
    assert price < 6.60 * (1 + strat.TAKE_PROFIT_PCT)
    should_exit, reason = strat.decide_exit(
        pos, price, velocity_pct_per_min=strat.QUICK_PROFIT_VELOCITY_PCT_PER_MIN + 0.001,
    )
    assert should_exit and "quick_profit" in reason


def test_quick_profit_does_not_trigger_on_slow_gain():
    pos = _position()
    price = 6.60 * (1 + strat.QUICK_PROFIT_PCT + 0.0002)
    should_exit, reason = strat.decide_exit(pos, price, velocity_pct_per_min=0.0001)
    assert not should_exit
    assert "holding" in reason


def test_update_velocity_returns_none_until_two_samples_span_time():
    pos = _position()
    now = dt.datetime.now(dt.timezone.utc)
    v1 = strat._update_velocity(pos, 6.60, now)  # noqa: SLF001
    assert v1 is None
    v2 = strat._update_velocity(pos, 6.63, now + dt.timedelta(seconds=30))  # noqa: SLF001
    assert v2 is not None
    assert v2 > 0  # price rose over the window -> positive velocity


# ── Bidirectional (short) trading -- gated behind ENABLE_SHORTS ─────────────

def _rally_row(**overrides):
    # Mirror of _row(): price sits ABOVE the short MA (a small rally).
    base = {"ticker": "KXBTCPERP", "current_price": 6.63, "short_ma": 6.60, "trend_pct": 0.0}
    base.update(overrides)
    return base


def test_rally_in_flat_trend_triggers_short_technical_entry():
    should_enter, reason = strat.decide_entry_technical(_rally_row(), side="short")
    assert should_enter
    assert "rally" in reason


def test_rally_in_strong_uptrend_is_filtered_out_for_shorts():
    should_enter, reason = strat.decide_entry_technical(_rally_row(trend_pct=0.05), side="short")
    assert not should_enter
    assert "trend filter" in reason


def test_dip_and_rally_conditions_are_mutually_exclusive_on_the_same_row():
    """A dip signal for longs and a rally signal for shorts can never both
    fire on the same price/MA snapshot -- they're mirror images of the same
    comparison."""
    row = _row()  # price below short MA -- a dip
    long_ok, _ = strat.decide_entry_technical(row, side="long")
    short_ok, _ = strat.decide_entry_technical(row, side="short")
    assert long_ok and not short_ok


def test_evaluate_candidate_ignores_shorts_when_disabled(monkeypatch):
    monkeypatch.setattr(strat, "ENABLE_SHORTS", False)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _rally_row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "down", "probability_up": 0.1,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    # A rally + confident down-prediction would qualify as a SHORT, but the
    # feature is off -- must not enter at all (must never silently go long
    # on a signal that was actually a short setup).
    assert result["should_enter"] is False


def test_evaluate_candidate_enters_short_on_rally_and_confident_down_prediction(monkeypatch):
    monkeypatch.setattr(strat, "ENABLE_SHORTS", True)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _rally_row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "down", "probability_up": 0.1,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True
    assert result["side"] == "short"
    assert result["score"] == pytest.approx(0.9)  # confidence = 1 - probability_up


def test_evaluate_candidate_does_not_short_on_technicals_alone_without_a_model(monkeypatch):
    """Shorting without any model confirmation at all is a materially
    different risk than the existing long-side technical-only fallback --
    must not enter."""
    monkeypatch.setattr(strat, "ENABLE_SHORTS", True)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _rally_row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False, "ticker": ticker})
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is False


def test_evaluate_candidate_rejects_short_when_model_predicts_up(monkeypatch):
    monkeypatch.setattr(strat, "ENABLE_SHORTS", True)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _rally_row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.9,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is False


def test_short_take_profit_exit_on_falling_price():
    pos = _position(side="short")
    # Price fell below entry by more than TAKE_PROFIT_PCT -- profitable for a short.
    should_exit, reason = strat.decide_exit(pos, 6.60 * (1 - strat.TAKE_PROFIT_PCT - 0.001))
    assert should_exit and "take_profit" in reason


def test_short_stop_loss_exit_on_rising_price():
    pos = _position(side="short")
    # Price rose above entry by more than STOP_LOSS_PCT -- a loss for a short.
    should_exit, reason = strat.decide_exit(pos, 6.60 * (1 + strat.STOP_LOSS_PCT + 0.001))
    assert should_exit and "stop_loss" in reason


def test_short_holds_when_price_barely_moves():
    pos = _position(side="short", minutes_ago=1)
    should_exit, reason = strat.decide_exit(pos, 6.595)
    assert not should_exit
    assert "holding" in reason


def test_short_quick_profit_requires_favorable_falling_velocity():
    pos = _position(side="short")
    price = 6.60 * (1 - strat.QUICK_PROFIT_PCT - 0.0002)  # profitable-for-a-short gain
    # A RISING raw velocity is UNFAVORABLE for a short -- must not trigger quick-profit.
    should_exit, reason = strat.decide_exit(pos, price, velocity_pct_per_min=strat.QUICK_PROFIT_VELOCITY_PCT_PER_MIN + 0.001)
    assert not should_exit

    # A FALLING raw velocity (price dropping fast) IS favorable for a short.
    should_exit, reason = strat.decide_exit(pos, price, velocity_pct_per_min=-(strat.QUICK_PROFIT_VELOCITY_PCT_PER_MIN + 0.001))
    assert should_exit and "quick_profit" in reason


def _real_short_position(ticker, count, entry_price):
    return {"market_ticker": ticker, "position": str(-abs(float(count))), "entry_price": str(entry_price), "is_portfolio": True}


def test_real_open_positions_by_ticker_derives_short_side_from_negative_sign(monkeypatch):
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [
        _real_short_position("KXETHPERP", "10.00", "50.0000"),
    ]})
    result = strat._real_open_positions_by_ticker()  # noqa: SLF001
    assert result == {"KXETHPERP": {"count": 10.0, "entry_price": 50.0, "side": "short"}}


def test_reconcile_adopts_untracked_real_short_position(monkeypatch):
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [
        _real_short_position("KXETHPERP", "10.00", "50.0000"),
    ]})
    reconciled = strat._reconcile_positions_with_exchange({"positions": []})  # noqa: SLF001
    assert len(reconciled) == 1
    assert reconciled[0]["side"] == "short"
    assert reconciled[0]["count"] == 10.0


def test_manage_open_positions_closes_short_by_buying_back(monkeypatch, tmp_path):
    """Closing a short must place a BID (buy-back) order, never an ASK --
    an ASK reduce_only on a short position would be nonsensical (it would
    try to sell MORE of something already sold short)."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({
        "positions": [_position(ticker="KXETHPERP", entry_price=50.0, count=10.0, side="short")],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    # First call is the pre-decision reconciliation (must match local state
    # exactly -- still 10 short); second call is the post-order fill
    # verification, after the buy-back fully closed it.
    calls = {"n": 0}

    def fake_positions():
        calls["n"] += 1
        count = "10.00" if calls["n"] == 1 else "0.00"
        return {"positions": [_real_short_position("KXETHPERP", count, "50.0000")] if float(count) > 0 else []}

    monkeypatch.setattr(strat, "get_margin_positions", fake_positions)
    # Price fell -- profitable for a short, should trigger take-profit.
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=50.0 * (1 - strat.TAKE_PROFIT_PCT - 0.001)))

    captured_orders = []

    def fake_create_order(**kwargs):
        captured_orders.append(kwargs)
        return {"order": {"fill_count": str(kwargs["count"])}}

    monkeypatch.setattr(strat, "create_margin_order", fake_create_order)

    result = strat.manage_open_positions(dry_run=False)
    assert result["action"] == "closed"
    assert len(captured_orders) == 1
    assert captured_orders[0]["side"] == "bid"
    assert captured_orders[0]["reduce_only"] is True
    # Price fell and it's a short -- must be a GAIN, not a loss.
    assert result["closed"][0]["realized_pnl_usd"] > 0


def test_scan_and_enter_opens_short_with_an_ask_order(monkeypatch, tmp_path):
    """Opening a short must place an ASK order with reduce_only NOT set --
    this is a brand new position, not closing an existing long."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(strat, "ENABLE_SHORTS", True)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response())
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 10.0)
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda tickers=None, exclude=None: (
            [{"ticker": "KXETHPERP", "current_price": 6.60, "reason": "test rally", "score": 0.9, "side": "short"}], [],
        ),
    )
    captured_orders = []

    def fake_create_order(**kwargs):
        captured_orders.append(kwargs)
        return {"order": {"fill_count": str(kwargs["count"])}}

    monkeypatch.setattr(strat, "create_margin_order", fake_create_order)
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [
        _real_short_position("KXETHPERP", "6.00", "6.60"),
    ]})

    result = strat.scan_and_enter(dry_run=False)
    assert result["opened"][0]["action"] == "opened"
    assert result["opened"][0]["side"] == "short"
    assert len(captured_orders) == 1
    assert captured_orders[0]["side"] == "ask"
    assert captured_orders[0].get("reduce_only", False) is False
    state = strat._load_state()  # noqa: SLF001
    assert state["positions"][0]["side"] == "short"


def test_evaluate_candidate_falls_back_to_technical_when_model_not_trained(monkeypatch):
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False, "ticker": ticker})
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True
    assert result["model_ok"] is False
    assert "fallback" in result["reason"]


def test_evaluate_candidate_model_blocks_entry_when_predicting_down(monkeypatch):
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "down", "probability_up": 0.3,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is False
    assert result["model_ok"] is True


def test_evaluate_candidate_model_confirms_entry_with_high_confidence(monkeypatch):
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.9,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True
    assert result["score"] == 0.9


def test_evaluate_candidate_sets_flat_technical_ok_on_a_qualifying_entry(monkeypatch):
    """The dashboard reads `technical_ok` as a flat top-level field (see
    dashboard.html's candidates table) -- it must be True whenever a
    qualifying entry actually fired on a real technical signal, not just on
    the non-qualifying fallback path."""
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.9,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True
    assert result["technical_ok"] is True


def test_scan_for_entries_excludes_already_held_tickers(monkeypatch):
    monkeypatch.setattr(strat, "get_watchlist", lambda: ["KXBTCPERP", "KXETHPERP"])
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(ticker=ticker))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False})
    qualifying, candidates = strat.scan_for_entries(exclude={"KXBTCPERP"})
    assert len(candidates) == 1
    assert candidates[0]["ticker"] == "KXETHPERP"


# ── Leveraged position sizing ────────────────────────────────────────────────

def test_compute_leveraged_count_uses_the_markets_leverage_multiplier():
    # $10 balance, 20% budget = $2 margin, 6x leverage = $12 notional,
    # at $2/contract that's 6 contracts -- NOT 1, which is the whole point
    # of sizing off the multiplier instead of a fixed 1-contract size.
    market = {"price": 2.0, "leverage_estimate": 6.0}
    count, detail = strat.compute_leveraged_count(10.0, market)
    assert count == 6
    assert detail["margin_budget_usd"] == 2.0
    assert detail["notional_capacity_usd"] == 12.0


def test_compute_leveraged_count_defaults_to_1x_leverage_if_missing():
    market = {"price": 2.0}
    count, detail = strat.compute_leveraged_count(10.0, market)
    assert detail["leverage_estimate"] == 1.0
    assert count == 1  # $10 * 20% = $2 margin_budget, 1x leverage => $2 notional => 1 contract at $2


def test_compute_leveraged_count_returns_zero_when_budget_too_small():
    market = {"price": 1000.0, "leverage_estimate": 2.0}
    count, _ = strat.compute_leveraged_count(1.0, market)
    assert count == 0


# ── Multi-position management ────────────────────────────────────────────────

def _market_response(price=6.60, tick_size=0.0001, leverage_estimate=6.0, contract_size=0.0001):
    return {"market": {
        "price": price, "tick_size": tick_size, "leverage_estimate": leverage_estimate, "contract_size": contract_size,
    }}


def test_manage_open_positions_returns_no_position_without_touching_the_network(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")

    def fail_if_called(ticker):
        raise AssertionError("get_margin_market must not be called when there's nothing to manage")

    monkeypatch.setattr(strat, "get_margin_market", fail_if_called)
    result = strat.manage_open_positions()
    assert result["action"] == "no_position"


def test_manage_open_positions_never_opens_a_new_position(monkeypatch, tmp_path):
    """The fast loop's only job is exits -- it must never be the one that
    opens a position, even indirectly."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")

    def fail_if_called(*a, **k):
        raise AssertionError("manage_open_positions must never place an entry order")

    monkeypatch.setattr(strat, "create_margin_order", fail_if_called)
    result = strat.manage_open_positions()
    assert result["action"] == "no_position"
    state = strat._load_state()  # noqa: SLF001
    assert state.get("positions") == []


def test_manage_open_positions_handles_each_position_independently(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)

    def fake_market(ticker):
        # BTC is up big (should close on take-profit); ETH barely moved (should hold).
        if ticker == "KXBTCPERP":
            return _market_response(price=6.60 * (1 + strat.TAKE_PROFIT_PCT + 0.001))
        return _market_response(price=100.001)

    monkeypatch.setattr(strat, "get_margin_market", fake_market)
    strat._save_state({
        "positions": [
            _position(ticker="KXBTCPERP", entry_price=6.60),
            _position(ticker="KXETHPERP", entry_price=100.0),
        ],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert len(result["closed"]) == 1
    assert result["closed"][0]["ticker"] == "KXBTCPERP"
    assert result["open_position_count"] == 1

    state = strat._load_state()  # noqa: SLF001
    remaining_tickers = [p["ticker"] for p in state["positions"]]
    assert remaining_tickers == ["KXETHPERP"]


def test_manage_open_positions_uses_external_velocity_as_an_early_quick_profit_trigger(monkeypatch, tmp_path):
    """Kalshi's own price barely moved (no velocity signal there), but an
    independent live exchange shows a fast favorable move -- that alone
    should be enough to trigger the quick-profit exit, since Kalshi's own
    quote can lag a deep spot venue."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    gain_price = 6.60 * (1 + strat.QUICK_PROFIT_PCT + 0.0002)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=gain_price))
    monkeypatch.setattr(strat, "coin_for_ticker", lambda ticker: "BTC")

    calls = {"n": 0}

    def fake_fast_price(coin):
        # First call establishes the baseline sample; second call (next
        # tick) shows a fast favorable move on the external venue.
        calls["n"] += 1
        price = 100.0 if calls["n"] == 1 else 100.0 * (1 + strat.QUICK_PROFIT_VELOCITY_PCT_PER_MIN * 2)
        return {"price": price, "source": "coinbase", "delayed": False}

    monkeypatch.setattr(strat, "get_fast_price", fake_fast_price)

    strat._save_state({
        "positions": [_position(ticker="KXBTCPERP", entry_price=6.60)],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    strat.manage_open_positions()  # establishes the first external sample
    result = strat.manage_open_positions()  # second tick, ~1 min of wall-clock apart in mocked samples
    # Force enough elapsed time between samples for a velocity to compute --
    # patch position's external samples timestamp to simulate real spacing.
    state = strat._load_state()  # noqa: SLF001
    if state["positions"]:
        samples = state["positions"][0].get("external_price_samples", [])
        if len(samples) >= 2:
            samples[0][0] -= 60  # pretend the first sample was 60s earlier
            strat._save_state(state)
            result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert "quick_profit" in result["closed"][0]["reason"]


def test_delayed_external_price_is_never_used_for_velocity(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=6.605))
    monkeypatch.setattr(strat, "get_fast_price", lambda coin: {"price": 999999.0, "source": "api_ninjas", "delayed": True})
    strat._save_state({
        "positions": [_position(ticker="KXBTCPERP", entry_price=6.60)],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    result = strat.manage_open_positions()
    check = result["checks"][0]
    assert check["external_velocity_pct_per_min"] is None


def test_scan_and_enter_skips_when_no_slots_open(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    monkeypatch.setattr(strat, "MAX_CONCURRENT_POSITIONS", 2)

    def fail_if_called(*a, **k):
        raise AssertionError("scan_for_entries must not run once every slot is full")

    monkeypatch.setattr(strat, "scan_for_entries", fail_if_called)
    strat._save_state({
        "positions": [_position(ticker="KXBTCPERP"), _position(ticker="KXETHPERP")],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    result = strat.scan_and_enter()
    assert result["action"] == "max_positions_open"


def test_scan_and_enter_never_opens_a_second_position_in_the_same_instrument(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response())
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 10.0)
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda tickers=None, exclude=None: (
            [{"ticker": "KXBTCPERP", "current_price": 6.60, "reason": "test dip", "score": 0.9}]
            if "KXBTCPERP" not in (exclude or set()) else [],
            [],
        ),
    )
    strat._save_state({
        "positions": [_position(ticker="KXBTCPERP")],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    result = strat.scan_and_enter()
    # KXBTCPERP is already held, so the (stubbed) scan correctly excludes it
    # and nothing new gets opened.
    assert result["action"] == "none"


def test_scan_and_enter_rejects_entry_on_large_kalshi_external_price_deviation(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    monkeypatch.setattr(strat, "coin_for_ticker", lambda ticker: "BTC")
    # Kalshi contract implies a spot price of 6.60 / 0.0001 = $66,000; the
    # external venue says $50,000 -- a large, real disagreement that should
    # block the entry rather than trust a possibly-stale Kalshi tick.
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=6.60, tick_size=0.0001))
    monkeypatch.setattr(strat, "get_fast_price", lambda coin: {"price": 50000.0, "source": "coinbase", "delayed": False})
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda tickers=None, exclude=None: ([{"ticker": "KXBTCPERP", "current_price": 6.60, "reason": "test dip", "score": 0.9}], []),
    )

    def fail_if_called(*a, **k):
        raise AssertionError("create_margin_order must not be called when the price sanity check fails")

    monkeypatch.setattr(strat, "create_margin_order", fail_if_called)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "skipped_price_deviation"


def test_dry_run_never_places_a_real_order(monkeypatch, tmp_path):
    """Regardless of the trading-enabled flag's value, passing dry_run=True
    (or the module default when the env flag is off) must never call the
    real order-placement function."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)

    def fail_if_called(*a, **k):
        raise AssertionError("create_margin_order must not be called while dry-run is in effect")

    monkeypatch.setattr(strat, "create_margin_order", fail_if_called)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response())
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 10.0)
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda tickers=None, exclude=None: ([{"ticker": "KXBTCPERP", "current_price": 6.60, "reason": "test dip", "score": 0.9}], []),
    )

    result = strat.scan_and_enter()
    assert result["dry_run"] is True
    assert result["action"] == "opened"
    assert result["opened"][0]["count"] >= 1


def test_daily_loss_cap_blocks_new_entries(monkeypatch, tmp_path):
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(strat, "STATE_FILE", state_file)
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 100.0)
    strat._save_state({
        "positions": [],
        "realized_pnl_by_date": {strat._today_str(): -100.0 * strat.DAILY_LOSS_CAP_PCT - 1.0},
        "trade_log": [], "daily_reference_balance": {strat._today_str(): 100.0},
    })
    result = strat.scan_and_enter()
    assert result["action"] == "skipped_daily_loss_cap"


def test_daily_loss_cap_is_a_percentage_of_the_days_starting_balance(monkeypatch, tmp_path):
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(strat, "STATE_FILE", state_file)
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 5.0)  # balance shrank intraday
    # Loss so far is small relative to the DAY'S STARTING balance ($100), even
    # though it looks large relative to the current (shrunk) balance ($5) --
    # the cap must be checked against the reference, not the live balance.
    strat._save_state({
        "positions": [],
        "realized_pnl_by_date": {strat._today_str(): -1.0},
        "trade_log": [], "daily_reference_balance": {strat._today_str(): 100.0},
    })
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda tickers=None, exclude=None: ([], []),
    )
    result = strat.scan_and_enter()
    assert result["action"] != "skipped_daily_loss_cap"


# ── Exchange reconciliation + real-fill verification ─────────────────────────
# Confirmed live on the real account: immediate_or_cancel orders repeatedly
# came back fill_count 0.00 (fully canceled, nothing executed) while the old
# code unconditionally trusted the requested count -- creating phantom local
# positions the dashboard showed as "open" with nothing actually held, an
# untracked real position (no local record at all, so no take-profit/
# stop-loss coverage), and a local position undercounting a real one that
# had partially filled across multiple attempts. These tests lock down the
# fix: never trust a requested count, always verify against Kalshi's own
# GET /margin/positions.

def _real_position(ticker, count, entry_price, is_portfolio=True):
    return {"market_ticker": ticker, "position": str(count), "entry_price": str(entry_price), "is_portfolio": is_portfolio}


def test_real_open_positions_by_ticker_ignores_non_portfolio_and_zero_rows(monkeypatch):
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [
        _real_position("KXBCHPERP", "0.00", "0.0000", is_portfolio=False),
        _real_position("KXSOLPERP", "4.00", "7.7572"),
        _real_position("KXNEARPERP", "0.00", "0.0000"),
    ]})
    result = strat._real_open_positions_by_ticker()  # noqa: SLF001
    assert result == {"KXSOLPERP": {"count": 4.0, "entry_price": 7.7572, "side": "long"}}


def test_real_open_positions_by_ticker_returns_none_on_api_failure(monkeypatch):
    def fail():
        raise RuntimeError("network down")
    monkeypatch.setattr(strat, "get_margin_positions", fail)
    assert strat._real_open_positions_by_ticker() is None  # noqa: SLF001


def test_reconcile_adopts_untracked_real_position(monkeypatch):
    """A real position exists on Kalshi (e.g. from a prior entry whose fill
    was never verified) that local state never recorded at all -- it must
    be adopted so it starts getting monitored for exit, instead of sitting
    with zero take-profit/stop-loss coverage forever."""
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [
        _real_position("KXSOLPERP", "4.00", "7.7572"),
    ]})
    reconciled = strat._reconcile_positions_with_exchange({"positions": []})  # noqa: SLF001
    assert len(reconciled) == 1
    assert reconciled[0]["ticker"] == "KXSOLPERP"
    assert reconciled[0]["count"] == 4.0
    assert reconciled[0]["entry_price"] == 7.7572


def test_reconcile_corrects_mismatched_count_and_entry_price(monkeypatch):
    """Local state thought KXBCHPERP was 7 contracts @ 2.1848; the real
    account had accumulated 74 contracts @ 2.1823 across several partial
    fills the old code never verified. Reconciliation must correct both."""
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [
        _real_position("KXBCHPERP", "74.00", "2.1823"),
    ]})
    local_state = {"positions": [_position(ticker="KXBCHPERP", entry_price=2.1848, count=7.0)]}
    reconciled = strat._reconcile_positions_with_exchange(local_state)  # noqa: SLF001
    assert len(reconciled) == 1
    assert reconciled[0]["count"] == 74.0
    assert reconciled[0]["entry_price"] == 2.1823


def test_reconcile_drops_phantom_position_with_no_real_fill(monkeypatch):
    """Local state recorded an open KXXRPPERP position, but the entry order
    actually had fill_count 0.00 on Kalshi's side -- nothing was ever really
    bought. Reconciliation must drop it, not leave a phantom position
    showing on the dashboard forever."""
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": []})
    local_state = {"positions": [_position(ticker="KXXRPPERP", entry_price=1.1338, count=1.0)]}
    reconciled = strat._reconcile_positions_with_exchange(local_state)  # noqa: SLF001
    assert reconciled == []


def test_reconcile_leaves_local_state_untouched_when_real_positions_check_fails(monkeypatch):
    """A transient API error while checking real positions must never be
    treated as "confirmed nothing is open" -- that would wipe out tracking
    on every ticker on a mere network hiccup."""
    def fail():
        raise RuntimeError("network down")
    monkeypatch.setattr(strat, "get_margin_positions", fail)
    local_state = {"positions": [_position(ticker="KXBCHPERP")]}
    reconciled = strat._reconcile_positions_with_exchange(local_state)  # noqa: SLF001
    assert len(reconciled) == 1
    assert reconciled[0]["ticker"] == "KXBCHPERP"


def test_manage_open_positions_reconciles_before_deciding_exits(monkeypatch, tmp_path):
    """With live trading on, manage_open_positions must pull in a real
    position local state never knew about (here: KXSOLPERP) so it actually
    gets a take-profit/stop-loss check instead of sitting unmonitored."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {}})
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [
        _real_position("KXSOLPERP", "4.00", "7.7572"),
    ]})
    # Price barely moved -- should just be adopted and held, not exited.
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=7.758))

    result = strat.manage_open_positions(dry_run=False)
    assert result["action"] == "none"
    state = strat._load_state()  # noqa: SLF001
    tickers = [p["ticker"] for p in state["positions"]]
    assert tickers == ["KXSOLPERP"]


def test_manage_open_positions_keeps_position_when_exit_order_does_not_fill(monkeypatch, tmp_path):
    """A stop-loss/take-profit exit order placed as immediate_or_cancel can
    come back with fill_count 0 (nothing executed) -- the old code removed
    the position from local state regardless, making the dashboard show
    "closed" while the real position was still fully open on Kalshi. Must
    keep monitoring it instead."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({
        "positions": [_position(ticker="KXBCHPERP", entry_price=2.1823, count=74.0)],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    # Reconciliation before the exit decision reports the position unchanged.
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [
        _real_position("KXBCHPERP", "74.00", "2.1823"),
    ]})
    # Price triggers take-profit...
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=2.1823 * (1 + strat.TAKE_PROFIT_PCT + 0.001)))
    # ...but the exit order itself never fills.
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: {"order": {"fill_count": "0.00"}})

    result = strat.manage_open_positions(dry_run=False)
    assert result["action"] == "none"
    assert result["checks"][0].get("exit_order_not_filled") is True
    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1
    assert state["positions"][0]["count"] == 74.0
    assert state["trade_log"] == []  # no fake trade recorded


def test_manage_open_positions_keeps_remainder_on_partial_exit_fill(monkeypatch, tmp_path):
    """The exit order fills only part of the position -- the filled portion
    should be recorded as a real closed trade, and the rest must stay open
    and continue being monitored, not vanish."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({
        "positions": [_position(ticker="KXBCHPERP", entry_price=2.1823, count=74.0)],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    # First call is the pre-decision reconciliation (must match local state
    # exactly, or this test would be exercising reconciliation-correction
    # instead of partial-fill handling); second call is the post-order
    # verification, after the exit order has closed 24 of the 74.
    calls = {"n": 0}

    def fake_positions():
        calls["n"] += 1
        count = "74.00" if calls["n"] == 1 else "50.00"
        return {"positions": [_real_position("KXBCHPERP", count, "2.1823")]}

    monkeypatch.setattr(strat, "get_margin_positions", fake_positions)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=2.1823 * (1 + strat.TAKE_PROFIT_PCT + 0.001)))
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: {"order": {"fill_count": "24.00"}})

    result = strat.manage_open_positions(dry_run=False)
    assert result["action"] == "closed"
    assert result["closed"][0]["count"] == 24.0
    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1
    assert state["positions"][0]["count"] == 50.0


def test_scan_and_enter_skips_recording_a_position_when_entry_order_does_not_fill(monkeypatch, tmp_path):
    """The entry buy order comes back fill_count 0 (fully canceled) --
    confirmed live behavior for immediate_or_cancel orders that miss the
    market. Must not record a phantom position that was never actually
    bought."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response())
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 10.0)
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda tickers=None, exclude=None: (
            [{"ticker": "KXXRPPERP", "current_price": 6.60, "reason": "test dip", "score": 0.9}], [],
        ),
    )
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: {"order": {"fill_count": "0.00"}})
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": []})

    result = strat.scan_and_enter(dry_run=False)
    assert result["opened"][0]["action"] == "skipped_entry_not_filled"
    state = strat._load_state()  # noqa: SLF001
    assert state.get("positions") == []


def test_scan_and_enter_records_actual_filled_count_not_requested_count(monkeypatch, tmp_path):
    """Requested 6 contracts, only 4 actually filled (confirmed live
    pattern: several partial fills smaller than requested) -- local state
    must reflect what was ACTUALLY bought, using Kalshi's own entry price,
    not the requested count/price."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=2.0, leverage_estimate=6.0))
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 10.0)  # sizes to 6 contracts, see sizing test above
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda tickers=None, exclude=None: (
            [{"ticker": "KXSOLPERP", "current_price": 2.0, "reason": "test dip", "score": 0.9}], [],
        ),
    )
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: {"order": {"fill_count": "4.00"}})
    # First call is the pre-scan reconciliation (nothing real held yet);
    # second call is the post-order verification, after the buy filled 4.
    calls = {"n": 0}

    def fake_positions():
        calls["n"] += 1
        if calls["n"] == 1:
            return {"positions": []}
        return {"positions": [_real_position("KXSOLPERP", "4.00", "1.9998")]}

    monkeypatch.setattr(strat, "get_margin_positions", fake_positions)

    result = strat.scan_and_enter(dry_run=False)
    assert result["opened"][0]["action"] == "opened"
    assert result["opened"][0]["count"] == 4.0
    state = strat._load_state()  # noqa: SLF001
    assert state["positions"][0]["count"] == 4.0
    assert state["positions"][0]["entry_price"] == 1.9998


def test_scan_and_enter_merges_a_confirmed_fill_into_a_concurrently_adopted_position(monkeypatch, tmp_path):
    """A real bug an adversarial review caught: if the fast loop's own
    reconciliation adopts a position for this exact ticker WHILE this order
    is in flight (unlocked, network-bound), the old code's final
    'already held' check would discard the just-confirmed real fill as
    "skipped_slot_taken" -- silently losing track of real contracts that
    genuinely executed. It must be merged into the existing entry instead."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response())
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 10.0)
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda tickers=None, exclude=None: (
            [{"ticker": "KXSOLPERP", "current_price": 6.60, "reason": "test dip", "score": 0.9}], [],
        ),
    )
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: {"order": {"fill_count": "4.00"}})
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [
        _real_position("KXSOLPERP", "4.00", "1.9998"),
    ]})

    # Simulate the race: between the pre-scan reconciliation (empty) and the
    # final lock-protected write, the OTHER loop already wrote this exact
    # ticker into local state (as if its own reconciliation had adopted it).
    strat._save_state({
        "positions": [_position(ticker="KXSOLPERP", entry_price=1.9998, count=4.0)],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })

    result = strat.scan_and_enter(dry_run=False)
    assert result["opened"][0]["action"] != "skipped_slot_taken"
    state = strat._load_state()  # noqa: SLF001
    # Exactly one tracked position for this ticker, reflecting Kalshi's own
    # confirmed total -- not silently dropped, not duplicated.
    matching = [p for p in state["positions"] if p["ticker"] == "KXSOLPERP"]
    assert len(matching) == 1
    assert matching[0]["count"] == 4.0


def test_run_cycle_manages_positions_then_scans_for_entries(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=6.605))
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 10.0)
    monkeypatch.setattr(strat, "scan_for_entries", lambda tickers=None, exclude=None: ([], []))
    strat._save_state({
        "positions": [_position(ticker="KXBTCPERP", entry_price=6.55)],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    result = strat.run_cycle()
    assert "position_management" in result and "entry_scan" in result
    assert result["position_management"]["action"] in ("none", "closed")
