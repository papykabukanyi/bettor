"""Alpaca crypto strategy decision logic. Mirrors test_alpaca_strategy.py's
discipline; focuses on what's genuinely different here: notional-based
position sizing (no whole-share affordability problem), no market-hours
gating, and plain market close orders instead of a bracket order (crypto
has no bracket orders on Alpaca at all) reconciled against real
/v2/positions state instead -- the same real-account ground-truth check
perps_strategy.py's own manage_open_positions/scan_and_enter use, brought
over as part of matching perps' own "brain" (fee accounting, adaptive
per-pair exits, velocity-based quick-profit, candidate ranking by score)."""
from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from data import alpaca_client, alpaca_crypto_data, alpaca_crypto_model, threads_post, alpaca_crypto_strategy as strat


def _row(**overrides):
    base = {
        "symbol": "BTC/USD", "current_price": 65000.0, "short_ma": 65200.0,
        "dollar_volume_z": 2.0, "volatility_5": 0.002, "volatility_30": 0.001,
    }
    base.update(overrides)
    return base


def test_entry_requires_an_unusual_volume_spike():
    """Volume+volatility gates are now a deliberate, real filter (see
    MIN_VOLUME_Z's own docstring in alpaca_crypto_strategy.py): low/negative
    volume must block an otherwise-valid dip entry."""
    should_enter, reason = strat.decide_entry_technical(_row(dollar_volume_z=-2.0))
    assert not should_enter
    assert "volume" in reason


def test_entry_volume_gate_can_still_be_re_enabled_via_env(monkeypatch):
    monkeypatch.setattr(strat, "MIN_VOLUME_Z", 1.5)
    should_enter, reason = strat.decide_entry_technical(_row(dollar_volume_z=0.5))
    assert not should_enter
    assert "volume" in reason


def test_entry_fires_when_all_gates_pass():
    should_enter, reason = strat.decide_entry_technical(_row())
    assert should_enter


def test_evaluate_candidate_technical_only_fallback_without_a_model():
    result = strat.evaluate_candidate(_row(), model_prediction=None)
    assert result["should_enter"]
    assert result["model_ok"] is False


def test_evaluate_candidate_requires_model_confidence_when_a_model_exists():
    low_confidence = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.51})
    assert not low_confidence["should_enter"]
    high_confidence = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.7})
    assert high_confidence["should_enter"]


def _position(entry_price=65000.0, minutes_ago=0):
    opened = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=minutes_ago)
    return {"entry_price": entry_price, "opened_at": opened.isoformat()}


def test_decide_exit_take_profit():
    pos = _position()
    should_exit, reason = strat.decide_exit(pos, 65000.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001))
    assert should_exit and "take_profit" in reason


def test_decide_exit_stop_loss():
    pos = _position()
    should_exit, reason = strat.decide_exit(pos, 65000.0 * (1 - strat.STOP_LOSS_PCT - 0.001))
    assert should_exit and "stop_loss" in reason


def test_decide_exit_max_hold_time():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(pos, 65000.0)
    assert should_exit and "max_hold_time" in reason


# ── "Promising position" max_hold_time extension ────────────────────────────
# See perps_strategy.py's own PROMISING_PROGRESS_FRACTION comment for the
# full rationale and real backtest findings.

def test_promising_position_by_price_progress_gets_extended_past_max_hold():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(pos, 65000.0 * 1.005)  # +0.5% vs 1% TP
    assert not should_exit
    assert "holding" in reason


def test_promising_position_still_force_closed_once_extension_window_elapses():
    past_extension = strat.MAX_HOLD_MINUTES + strat.MAX_HOLD_EXTENSION_MINUTES + 1
    pos = _position(minutes_ago=past_extension)
    should_exit, reason = strat.decide_exit(pos, 65000.0 * 1.005)
    assert should_exit and "max_hold_time" in reason


def test_volume_and_momentum_confluence_extends_even_without_price_progress():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(
        pos, 65000.0 * 1.0005, dollar_volume_z=2.0, momentum_pct=0.001,
    )
    assert not should_exit
    assert "holding" in reason


def test_momentum_extension_requires_position_not_already_reversing():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(
        pos, 65000.0 * (1 - strat.STOP_LOSS_PCT * 0.5), dollar_volume_z=2.0, momentum_pct=0.001,
    )
    assert should_exit and "max_hold_time" in reason


def test_position_exit_levels():
    levels = strat.position_exit_levels({"entry_price": 65000.0})
    assert levels["take_profit_price"] == round(65000.0 * (1 + strat.TAKE_PROFIT_PCT), 6)
    assert levels["stop_loss_price"] == round(65000.0 * (1 - strat.STOP_LOSS_PCT), 6)


def test_compute_position_notional_is_a_dollar_amount_not_a_share_count():
    """Crypto never has the "can't afford one whole share" problem
    equities have -- notional sizing works at any price, including a
    $65,000 coin against a $500 account."""
    notional = strat.compute_position_notional(500.0)
    assert notional == round(500.0 * strat.POSITION_SIZE_PCT, 2)
    assert notional > 0


def test_compute_position_notional_floors_at_zero_for_a_negative_balance():
    assert strat.compute_position_notional(-10.0) == 0.0


# ---------------------------------------------------------------------------
# Simulate/live position lifecycle -- state persistence, entry, exit.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _isolated_state(tmp_path, monkeypatch):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "alpaca_crypto_state.json")
    monkeypatch.setattr(strat, "HF_API_KEY", "")
    # get_available_balance() now always calls alpaca_client.get_account()
    # (no more "simulate mode" local-math fallback) -- every scan_and_enter
    # test implicitly depends on this succeeding for position sizing, so a
    # sane default lives here; tests exercising the loss cap or reconcile
    # path override it with a specific value where the number matters.
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0"})
    yield


def test_load_state_defaults_to_an_empty_position_list():
    state = strat._load_state()  # noqa: SLF001
    assert state["positions"] == []
    assert "balance" not in state


def test_get_current_price_averages_bid_and_ask(monkeypatch):
    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", lambda symbol: {"ap": 65010.0, "bp": 64990.0})
    assert strat.get_current_price("BTC/USD") == pytest.approx(65000.0)


def test_get_current_price_returns_none_on_a_failed_quote(monkeypatch):
    def fail(symbol):
        raise RuntimeError("network down")

    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", fail)
    assert strat.get_current_price("BTC/USD") is None


def _entry_row(**overrides):
    base = {
        "symbol": "BTC/USD", "current_price": 65000.0, "short_ma": 65200.0,
        "dollar_volume_z": 2.0, "volatility_5": 0.002, "volatility_30": 0.001,
    }
    base.update(overrides)
    return base


def test_scan_and_enter_dry_run_opens_a_position_without_any_real_order(monkeypatch):
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BTC/USD"])
    monkeypatch.setattr(alpaca_crypto_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_crypto_model, "predict_direction", lambda symbol: {"model_ok": False})

    def fail_if_called(*a, **k):
        raise AssertionError("dry-run must never place a real order")

    monkeypatch.setattr(alpaca_client, "place_order", fail_if_called)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    assert result["opened"][0]["dry_run"] is True

    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1
    assert state["positions"][0]["symbol"] == "BTC/USD"
    assert state["positions"][0]["count"] > 0  # a fractional coin count, computed from notional/entry_price


def test_scan_and_enter_skips_a_symbol_already_held(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "positions": [{"symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001}],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BTC/USD"])

    def fail_if_called(symbol):
        raise AssertionError("must not re-evaluate a symbol that's already held")

    monkeypatch.setattr(alpaca_crypto_data, "latest_feature_row", fail_if_called)
    result = strat.scan_and_enter()
    assert result["opened"] == []


def test_scan_and_enter_respects_the_daily_loss_cap(monkeypatch):
    today = strat._today_str()  # noqa: SLF001
    strat._save_state({  # noqa: SLF001
        "positions": [], "trade_log": [],
        "realized_pnl_by_date": {today: -100.0},  # -20% of $500, breaches the 10% default cap
    })
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0"})
    result = strat.scan_and_enter()
    assert result["action"] == "daily_loss_cap_breached"


def test_scan_and_enter_posts_to_threads_on_a_dry_run_entry(monkeypatch):
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BTC/USD"])
    monkeypatch.setattr(alpaca_crypto_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_crypto_model, "predict_direction", lambda symbol: {"model_ok": False})

    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_entry", lambda **kw: posted.update(kw) or True)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    assert posted["ticker"] == "BTC/USD"
    assert posted["side"] == "long"
    assert posted["dry_run"] is True


def test_scan_and_enter_still_opens_the_position_even_if_threads_post_raises(monkeypatch):
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BTC/USD"])
    monkeypatch.setattr(alpaca_crypto_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_crypto_model, "predict_direction", lambda symbol: {"model_ok": False})

    def raise_error(**kwargs):
        raise RuntimeError("simulated Threads API outage")

    monkeypatch.setattr(threads_post, "post_trade_entry", raise_error)
    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1


def test_scan_and_enter_one_symbol_failing_does_not_block_the_others(monkeypatch):
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BAD/USD", "BTC/USD"])

    def fake_feature_row(symbol):
        if symbol == "BAD/USD":
            raise RuntimeError("data fetch failed")
        return _entry_row(symbol=symbol)

    monkeypatch.setattr(alpaca_crypto_data, "latest_feature_row", fake_feature_row)
    monkeypatch.setattr(alpaca_crypto_model, "predict_direction", lambda symbol: {"model_ok": False})

    result = strat.scan_and_enter()
    outcomes = {o["symbol"]: o for o in result["opened"]}
    assert outcomes["BAD/USD"]["ok"] is False
    assert outcomes["BTC/USD"]["ok"] is True and outcomes["BTC/USD"]["action"] == "opened"


def test_manage_open_positions_posts_a_threads_exit_on_close(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 65000.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_exit", lambda **kw: posted.update(kw) or True)

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert posted["ticker"] == "BTC/USD"
    assert posted["market"] == "crypto"
    assert posted["pnl_usd"] > 0


def _one_min_df(n=30, base_ts=None):
    base_ts = base_ts or int(dt.datetime.now(dt.timezone.utc).timestamp()) - n * 60
    rows = []
    price = 65000.0
    for i in range(n):
        o = price
        price += 10.0
        rows.append({"ts": base_ts + i * 60, "open": o, "high": max(o, price) + 5, "low": min(o, price) - 5, "close": price})
    return pd.DataFrame(rows)


def test_candles_as_dicts_converts_a_dataframe_to_plain_dicts():
    dicts = strat._candles_as_dicts(_one_min_df(5))  # noqa: SLF001
    assert len(dicts) == 5
    assert set(dicts[0].keys()) == {"ts", "open", "high", "low", "close"}


def test_scan_and_enter_posts_a_candlestick_entry_chart(monkeypatch):
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BTC/USD"])
    monkeypatch.setattr(alpaca_crypto_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_crypto_model, "predict_direction", lambda symbol: {"model_ok": False})
    monkeypatch.setattr(alpaca_crypto_data, "fetch_recent_crypto_bars", lambda symbol: _one_min_df())

    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_entry_chart", lambda **kw: posted.update(kw) or True)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    assert posted["ticker"] == "BTC/USD"
    assert posted["market"] == "crypto"
    assert len(posted["candles"]) == 30
    assert posted["entry_index"] == 29


def test_manage_open_positions_posts_a_candlestick_exit_chart_on_close(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 65000.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})
    monkeypatch.setattr(alpaca_crypto_data, "fetch_recent_crypto_bars", lambda symbol: _one_min_df())

    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_exit_chart", lambda **kw: posted.update(kw) or True)

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert posted["ticker"] == "BTC/USD"
    assert posted["market"] == "crypto"
    assert posted["pnl_usd"] == result["closed"][0]["realized_pnl_usd"]


def test_maybe_run_batch_trade_analysis_runs_at_the_batch_boundary_and_posts(monkeypatch):
    trades = [
        {
            "symbol": "BTC/USD", "realized_pnl_usd": 1.0, "dry_run": False, "reason": "take_profit (+2%)",
            "entry_price": 65000.0, "exit_price": 65100.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "closed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        for _ in range(5)
    ]
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": trades})  # noqa: SLF001
    monkeypatch.setattr(alpaca_crypto_data, "fetch_recent_crypto_bars", lambda symbol: _one_min_df())
    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_analysis_summary", lambda text, **kw: posted.update(text=text, **kw) or True)

    strat._maybe_run_batch_trade_analysis()  # noqa: SLF001
    assert posted["market"] == "crypto"
    assert "5" in posted["text"]
    state = strat._load_state()  # noqa: SLF001
    assert state["last_batch_analysis_trade_count"] == 5


def test_maybe_run_batch_trade_analysis_skips_below_batch_size(monkeypatch):
    trades = [{"symbol": "BTC/USD", "realized_pnl_usd": 1.0, "dry_run": False} for _ in range(3)]
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": trades})  # noqa: SLF001
    called = {"n": 0}
    monkeypatch.setattr(threads_post, "post_trade_analysis_summary", lambda *a, **kw: called.update(n=called["n"] + 1))
    strat._maybe_run_batch_trade_analysis()  # noqa: SLF001
    assert called["n"] == 0


def test_manage_open_positions_returns_no_position_without_any_state():
    assert strat.manage_open_positions()["action"] == "no_position"


def test_manage_open_positions_dry_run_closes_on_take_profit(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 65000.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert "take_profit" in result["closed"][0]["reason"]
    state = strat._load_state()  # noqa: SLF001
    assert state["positions"] == []


def test_manage_open_positions_leaves_position_open_when_nothing_triggers(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", lambda symbol: {"ap": 65005.0, "bp": 65005.0})
    result = strat.manage_open_positions()
    assert result["action"] == "no_change"


def test_manage_open_positions_never_places_a_real_order_in_dry_run(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 65000.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    def fail_if_called(*a, **k):
        raise AssertionError("dry-run must never place a real order")

    monkeypatch.setattr(alpaca_client, "place_order", fail_if_called)
    result = strat.manage_open_positions()
    assert result["action"] == "closed"


def test_manage_open_positions_live_mode_places_a_plain_market_sell(monkeypatch):
    """Unlike the equities strategy, there's no get_position/close_position
    reconciliation dance here -- crypto has no broker-native bracket order
    that could have already closed the position, so a triggered exit is
    always a fresh, plain market sell."""
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 65000.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    captured = {}
    monkeypatch.setattr(alpaca_client, "build_crypto_order", lambda **kw: captured.update(kw) or {"symbol": kw["symbol"]})
    monkeypatch.setattr(alpaca_client, "place_order", lambda spec: "order-2")

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert captured["side"] == "sell"
    assert captured["qty"] == 0.001


# ---------------------------------------------------------------------------
# Fee accounting -- real gap found in review: this strategy was booking
# GROSS price movement as realized P&L with no fee subtracted at all.
# ---------------------------------------------------------------------------
def test_round_trip_fee_usd_charges_taker_rate_on_both_legs():
    fee = strat.round_trip_fee_usd(100.0, 110.0, 2.0)
    assert fee == round(100.0 * 2.0 * strat.TAKER_FEE_RATE + 110.0 * 2.0 * strat.TAKER_FEE_RATE, 6)
    assert fee > 0


def test_round_trip_fee_usd_taker_fee_rate_override_replaces_the_live_global():
    """Real bug found and fixed in review: alpaca_crypto_backtest.py's own
    simulate() threads a taker_fee_rate override all the way down to the
    fee calculation, but round_trip_fee_usd used to ignore it and always
    read the live TAKER_FEE_RATE global -- a backtest fee-rate sweep was
    silently a no-op. The override must actually change the fee, and
    omitting it must still fall back to the live rate (the real call site
    in manage_open_positions never passes it)."""
    live_fee = strat.round_trip_fee_usd(100.0, 110.0, 2.0)
    overridden = strat.round_trip_fee_usd(100.0, 110.0, 2.0, taker_fee_rate=0.0)
    assert overridden == 0.0
    assert live_fee > 0
    assert strat.round_trip_fee_usd(100.0, 110.0, 2.0) == live_fee  # no override -> unchanged live behavior


def test_manage_open_positions_books_net_pnl_after_fees(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 65000.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    result = strat.manage_open_positions()
    trade = result["closed"][0]
    expected_gross = round((take_profit_price - 65000.0) * 0.001, 6)
    expected_fee = strat.round_trip_fee_usd(65000.0, take_profit_price, 0.001)
    assert trade["gross_pnl_usd"] == expected_gross
    assert trade["fee_usd"] == pytest.approx(expected_fee)
    assert trade["realized_pnl_usd"] == pytest.approx(round(expected_gross - expected_fee, 6))
    assert trade["realized_pnl_usd"] < trade["gross_pnl_usd"]


# ---------------------------------------------------------------------------
# Candidate ranking by score -- scan_and_enter must fill its limited slots
# with the BEST qualifying candidates, not whichever sorts first.
# ---------------------------------------------------------------------------
def test_evaluate_candidate_score_is_model_confidence_when_model_is_used():
    result = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.83})
    assert result["should_enter"]
    assert result["score"] == pytest.approx(0.83)


def test_evaluate_candidate_score_is_zero_when_it_does_not_qualify():
    result = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.51})
    assert not result["should_enter"]
    assert result["score"] == 0.0


def test_evaluate_candidate_confidence_min_override_replaces_the_module_default():
    with_stricter_override = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.6}, confidence_min=0.65)
    assert not with_stricter_override["should_enter"]
    with_looser_override = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.52}, confidence_min=0.5)
    assert with_looser_override["should_enter"]


def test_scan_and_enter_fills_the_single_slot_with_the_best_scoring_candidate(monkeypatch):
    monkeypatch.setattr(strat, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["ETH/USD", "BTC/USD"])

    def fake_feature_row(symbol):
        return _entry_row(symbol=symbol, current_price=100.0, short_ma=101.0)

    def fake_predict(symbol):
        # BTC/USD is the weaker signal, ETH/USD the stronger one -- despite
        # sorting AFTER "BTC/USD" alphabetically, ETH/USD must win the slot.
        return {"model_ok": True, "probability_up": 0.60 if symbol == "BTC/USD" else 0.90}

    monkeypatch.setattr(alpaca_crypto_data, "latest_feature_row", fake_feature_row)
    monkeypatch.setattr(alpaca_crypto_model, "predict_direction", fake_predict)

    result = strat.scan_and_enter()
    outcomes = {o["symbol"]: o for o in result["opened"]}
    assert outcomes["ETH/USD"]["action"] == "opened"
    assert outcomes["BTC/USD"]["action"] == "skipped_slot_taken"


def test_scan_and_enter_dedupes_correlated_coins_across_quote_currencies(monkeypatch):
    """get_crypto_universe() can return more than one quote currency for
    the same coin (e.g. BTC/USD and BTC/USDT) -- holding both at once
    would just be two bets on the identical underlying move."""
    monkeypatch.setattr(strat, "MAX_CONCURRENT_POSITIONS", 5)
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BTC/USD", "BTC/USDT"])
    monkeypatch.setattr(alpaca_crypto_data, "latest_feature_row", lambda symbol: _entry_row(symbol=symbol))
    monkeypatch.setattr(alpaca_crypto_model, "predict_direction", lambda symbol: {"model_ok": False})

    result = strat.scan_and_enter()
    opened_symbols = [o["symbol"] for o in result["opened"] if o["action"] == "opened"]
    assert len(opened_symbols) == 1
    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1


# ---------------------------------------------------------------------------
# Adaptive per-pair exit levels -- scaled to each pair's own volatility_30
# at entry, not one flat percentage applied identically to every pair.
# ---------------------------------------------------------------------------
def test_adaptive_exit_pcts_falls_back_to_flat_defaults_without_volatility():
    pcts = strat.adaptive_exit_pcts(None)
    assert pcts["take_profit_pct"] == strat.TAKE_PROFIT_PCT
    assert pcts["stop_loss_pct"] == strat.STOP_LOSS_PCT


def test_adaptive_exit_pcts_scales_with_entry_volatility():
    low_vol = strat.adaptive_exit_pcts(0.0005)
    high_vol = strat.adaptive_exit_pcts(0.01)
    assert high_vol["take_profit_pct"] > low_vol["take_profit_pct"]
    assert high_vol["stop_loss_pct"] > low_vol["stop_loss_pct"]


def test_adaptive_exit_pcts_respects_floors_and_ceilings():
    tiny = strat.adaptive_exit_pcts(1e-9)
    assert tiny["take_profit_pct"] >= strat.MIN_TAKE_PROFIT_PCT
    huge = strat.adaptive_exit_pcts(10.0)
    assert huge["take_profit_pct"] <= strat.MAX_TAKE_PROFIT_PCT


def test_adaptive_exit_pcts_falls_back_to_flat_defaults_on_nan():
    """Real edge case: a rolling-window feature still NaN this early (e.g.
    right after a pair's data collection started) must fall back to the
    same flat defaults as a missing value -- Python's own `nan <= 0` and
    `not nan` are both False, so a naive falsy/<=0 guard alone would miss
    this and let NaN propagate into the clamped result."""
    pcts = strat.adaptive_exit_pcts(float("nan"))
    assert pcts["take_profit_pct"] == strat.TAKE_PROFIT_PCT
    assert pcts["stop_loss_pct"] == strat.STOP_LOSS_PCT


def test_decide_exit_uses_the_positions_own_adaptive_levels():
    pos = _position(entry_price=100.0)
    pos["entry_volatility_30"] = 0.01  # wide enough to push take-profit well above the flat default
    exit_pcts = strat.adaptive_exit_pcts(0.01)
    should_exit, reason = strat.decide_exit(pos, 100.0 * (1 + exit_pcts["take_profit_pct"] + 0.001))
    assert should_exit and "take_profit" in reason
    # The SAME gain would NOT have triggered the flat-default target if it's smaller.
    if exit_pcts["take_profit_pct"] > strat.TAKE_PROFIT_PCT:
        no_vol_pos = _position(entry_price=100.0)
        still_exits, _ = strat.decide_exit(no_vol_pos, 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.0005))
        assert still_exits  # sanity: flat default still fires on its own smaller target


def test_position_exit_levels_includes_quick_profit_price():
    levels = strat.position_exit_levels({"entry_price": 65000.0})
    assert "quick_profit_price" in levels
    assert levels["quick_profit_price"] < levels["take_profit_price"]


# ---------------------------------------------------------------------------
# Velocity-based quick-profit / volatility-quick-profit exits.
# ---------------------------------------------------------------------------
def test_update_velocity_returns_none_on_the_first_sample():
    pos = {}
    now = dt.datetime.now(dt.timezone.utc)
    assert strat._update_velocity(pos, 100.0, now) is None  # noqa: SLF001
    assert len(pos["price_samples"]) == 1


def test_update_velocity_computes_percent_per_minute():
    pos = {}
    now = dt.datetime.now(dt.timezone.utc)
    strat._update_velocity(pos, 100.0, now)  # noqa: SLF001
    later = now + dt.timedelta(minutes=1)
    velocity = strat._update_velocity(pos, 101.0, later)  # noqa: SLF001
    assert velocity == pytest.approx(0.01, rel=1e-3)


def test_sample_volatility_needs_at_least_three_samples():
    assert strat._sample_volatility([[0, 100.0], [1, 101.0]]) is None  # noqa: SLF001
    result = strat._sample_volatility([[0, 100.0], [1, 101.0], [2, 99.0]])  # noqa: SLF001
    assert result is not None and result > 0


def test_decide_exit_quick_profit_on_fast_favorable_velocity():
    pos = _position(entry_price=100.0)
    pos["entry_volatility_30"] = 0.01
    exit_pcts = strat.adaptive_exit_pcts(0.01)
    price = 100.0 * (1 + exit_pcts["quick_profit_pct"] + 0.0005)
    should_exit, reason = strat.decide_exit(
        pos, price, velocity_pct_per_min=strat.QUICK_PROFIT_VELOCITY_PCT_PER_MIN + 0.001,
    )
    assert should_exit and "quick_profit" in reason


def test_manage_open_positions_persists_velocity_samples_across_a_non_exit_cycle(monkeypatch):
    """Velocity tracking is useless if price_samples resets every cycle --
    a position that doesn't exit must still have its samples PERSISTED,
    not just mutated in memory and discarded."""
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_client, "get_crypto_latest_quote", lambda symbol: {"ap": 65005.0, "bp": 65005.0})
    result = strat.manage_open_positions()
    assert result["action"] == "no_change"
    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"][0].get("price_samples") or []) == 1


# ---------------------------------------------------------------------------
# Reconciliation against the real Alpaca account (live mode only).
# ---------------------------------------------------------------------------
def test_normalize_symbol_strips_separators():
    assert strat._normalize_symbol("BTC/USD") == strat._normalize_symbol("BTCUSD")  # noqa: SLF001


def test_real_open_positions_by_symbol_filters_to_crypto_asset_class(monkeypatch):
    positions = [
        {"symbol": "AAPL", "qty": "10", "avg_entry_price": "150.0", "asset_class": "us_equity"},
        {"symbol": "BTC/USD", "qty": "0.002", "avg_entry_price": "64000.0", "asset_class": "crypto"},
    ]
    monkeypatch.setattr(alpaca_client, "get_positions", lambda: positions)
    real = strat._real_open_positions_by_symbol()  # noqa: SLF001
    assert list(real.keys()) == [strat._normalize_symbol("BTC/USD")]  # noqa: SLF001
    assert real[strat._normalize_symbol("BTC/USD")]["count"] == pytest.approx(0.002)  # noqa: SLF001


def test_real_open_positions_by_symbol_returns_none_on_a_failed_fetch(monkeypatch):
    def fail():
        raise RuntimeError("network down")

    monkeypatch.setattr(alpaca_client, "get_positions", fail)
    assert strat._real_open_positions_by_symbol() is None  # noqa: SLF001


def test_reconcile_adopts_an_untracked_real_position(monkeypatch):
    monkeypatch.setattr(alpaca_client, "get_positions", lambda: [
        {"symbol": "ETH/USD", "qty": "0.5", "avg_entry_price": "3000.0", "asset_class": "crypto"},
    ])
    reconciled = strat._reconcile_positions_with_exchange({"positions": []})  # noqa: SLF001
    assert len(reconciled) == 1
    assert reconciled[0]["symbol"] == "ETH/USD"
    assert reconciled[0]["count"] == pytest.approx(0.5)


def test_reconcile_corrects_a_drifted_local_position(monkeypatch):
    monkeypatch.setattr(alpaca_client, "get_positions", lambda: [
        {"symbol": "BTC/USD", "qty": "0.002", "avg_entry_price": "64500.0", "asset_class": "crypto"},
    ])
    local = [{"symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001, "opened_at": "2026-01-01T00:00:00+00:00"}]
    reconciled = strat._reconcile_positions_with_exchange({"positions": local})  # noqa: SLF001
    assert len(reconciled) == 1
    assert reconciled[0]["count"] == pytest.approx(0.002)
    assert reconciled[0]["entry_price"] == pytest.approx(64500.0)


def test_reconcile_adopts_a_position_using_alpacas_own_slash_less_symbol_format(monkeypatch):
    """Real, confirmed production bug: GET /v2/positions returns crypto
    symbols WITHOUT the "/" separator (e.g. "XRPUSD"), not the "XRP/USD"
    format get_current_price()/fetch_recent_crypto_bars() expect
    everywhere else -- a real ~$14,435 XRP position got adopted as
    "XRPUSD" and every subsequent quote fetch for it failed with a 400,
    leaving it unpriceable and unmanageable. Must be reconstructed to the
    canonical form by matching against the tradable universe."""
    monkeypatch.setattr(alpaca_client, "get_positions", lambda: [
        {"symbol": "XRPUSD", "qty": "13648.492425039", "avg_entry_price": "1.057817121", "asset_class": "crypto"},
    ])
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["XRP/USD", "BTC/USD", "ETH/USD"])
    reconciled = strat._reconcile_positions_with_exchange({"positions": []})  # noqa: SLF001
    assert len(reconciled) == 1
    assert reconciled[0]["symbol"] == "XRP/USD"


def test_reconcile_self_heals_a_position_already_stored_under_the_wrong_symbol(monkeypatch):
    """A position adopted BEFORE the canonical-symbol fix existed would
    already be sitting in state as "XRPUSD" -- normalized keys still
    match on every later reconcile (both normalize to "XRPUSD"), so the
    "correct drifted position" branch must also fix the symbol field
    itself, not just count/entry_price, or a once-broken position would
    stay broken forever even after this fix ships."""
    monkeypatch.setattr(alpaca_client, "get_positions", lambda: [
        {"symbol": "XRPUSD", "qty": "13648.492425039", "avg_entry_price": "1.057817121", "asset_class": "crypto"},
    ])
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["XRP/USD", "BTC/USD", "ETH/USD"])
    local = [{"symbol": "XRPUSD", "entry_price": 1.057817121, "count": 13648.492425039, "opened_at": "2026-01-01T00:00:00+00:00"}]
    reconciled = strat._reconcile_positions_with_exchange({"positions": local})  # noqa: SLF001
    assert len(reconciled) == 1
    assert reconciled[0]["symbol"] == "XRP/USD"


def test_reconcile_drops_a_phantom_local_position(monkeypatch):
    monkeypatch.setattr(alpaca_client, "get_positions", lambda: [])
    local = [{"symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001, "opened_at": "2026-01-01T00:00:00+00:00"}]
    reconciled = strat._reconcile_positions_with_exchange({"positions": local})  # noqa: SLF001
    assert reconciled == []


def test_reconcile_returns_local_positions_unchanged_when_the_real_fetch_fails(monkeypatch):
    def fail():
        raise RuntimeError("network down")

    monkeypatch.setattr(alpaca_client, "get_positions", fail)
    local = [{"symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001, "opened_at": "2026-01-01T00:00:00+00:00"}]
    reconciled = strat._reconcile_positions_with_exchange({"positions": local})  # noqa: SLF001
    assert reconciled == local


def test_scan_and_enter_reconciles_before_entering_in_live_mode(monkeypatch):
    """A slot already occupied by a real, untracked exchange position must
    not also be counted as free -- reconciliation has to run BEFORE the
    slot-availability check."""
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(strat, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(alpaca_client, "get_positions", lambda: [
        {"symbol": "ETH/USD", "qty": "0.5", "avg_entry_price": "3000.0", "asset_class": "crypto"},
    ])
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0"})
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BTC/USD"])
    monkeypatch.setattr(alpaca_crypto_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_crypto_model, "predict_direction", lambda symbol: {"model_ok": False})

    result = strat.scan_and_enter()
    outcomes = {o["symbol"]: o for o in result["opened"]}
    assert outcomes["BTC/USD"]["action"] == "skipped_slot_taken"
    state = strat._load_state()  # noqa: SLF001
    assert any(p["symbol"] == "ETH/USD" for p in state["positions"])


# ---------------------------------------------------------------------------
# Daily reference balance -- fixed to the day's ACTUAL starting balance,
# not whatever the continuously-updated tracked balance happens to be.
# ---------------------------------------------------------------------------
def test_reference_balance_for_today_is_captured_once():
    state = {}
    first = strat._reference_balance_for_today(state, 500.0)  # noqa: SLF001
    second = strat._reference_balance_for_today(state, 999.0)  # noqa: SLF001
    assert first == 500.0
    assert second == 500.0  # unchanged by a later, different reading


def test_reference_balance_for_today_returns_none_without_a_real_reading():
    assert strat._reference_balance_for_today({}, None) is None  # noqa: SLF001
