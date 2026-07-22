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


def _position(entry_price=6.60, minutes_ago=0, ticker="KXBTCPERP", count=1.0):
    opened = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=minutes_ago)
    return {"ticker": ticker, "entry_price": entry_price, "count": count, "opened_at": opened.isoformat()}


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
