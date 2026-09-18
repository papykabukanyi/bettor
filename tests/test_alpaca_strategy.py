"""Alpaca equities strategy decision logic -- separate from and parallel to
test_perps_strategy.py. Pure-function tests: volume/volatility entry gate,
take-profit/stop-loss/max-hold exits, position sizing, and the simulate/
live position-lifecycle engine (state persistence, entry, exit)."""
from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from data import alpaca_client, alpaca_data, alpaca_model, threads_post, alpaca_strategy as strat


@pytest.fixture(autouse=True)
def _regular_market_session(monkeypatch):
    """scan_and_enter/manage_open_positions are now session-aware (see
    alpaca_strategy.py's own docstrings) -- default every test to the
    regular session, matching what these tests already assumed implicitly
    before that awareness existed. Tests that actually exercise pre/post
    market behavior override this explicitly."""
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "regular", "is_open": True, "source": "test"})
    yield


@pytest.fixture(autouse=True)
def _no_real_prewarm_network_calls(monkeypatch):
    """Real, confirmed production incident: scan_and_enter now prewarms
    sentiment/minute-bars concurrently before its main loop (see either
    prewarm function's own docstring) -- any test that mocks the
    per-symbol fetch but not these two left them REAL and unmocked,
    hitting real network endpoints during a test run. One such gap
    (test_collect_dataset_rows_uses_the_given_symbols in
    test_alpaca_crypto_data.py) genuinely hung a full test-suite run.
    Defaulted to a safe no-op for every test in this file; a test that
    actually cares about prewarm behavior itself (see
    test_scan_and_enter_prewarms_sentiment_for_symbols_not_already_held)
    overrides this with its own monkeypatch.setattr call, which simply
    wins since it runs later in the same test."""
    monkeypatch.setattr(alpaca_data, "prewarm_sentiment", lambda *a, **kw: None)
    monkeypatch.setattr(alpaca_data, "prewarm_minute_bars", lambda *a, **kw: None)
    yield


def _row(**overrides):
    base = {
        "symbol": "AAPL", "current_price": 100.0, "short_ma": 100.3,
        "dollar_volume_z": 2.0, "volatility_5": 0.002, "volatility_30": 0.001,
    }
    base.update(overrides)
    return base


def test_entry_requires_an_unusual_volume_spike(monkeypatch):
    """Volume+volatility gates are now a deliberate, real filter (see
    MIN_VOLUME_Z's own docstring in alpaca_strategy.py): low/negative
    volume must block an otherwise-valid dip entry."""
    should_enter, reason = strat.decide_entry_technical(_row(dollar_volume_z=-2.0))
    assert not should_enter
    assert "volume" in reason


def test_entry_requires_elevated_volatility_relative_to_its_own_baseline():
    should_enter, reason = strat.decide_entry_technical(_row(volatility_5=0.0005, volatility_30=0.002))
    assert not should_enter
    assert "volatile" in reason


def test_entry_volume_gate_can_still_be_re_enabled_via_env(monkeypatch):
    """Confirms this is a real, working opt-in override, not dead config."""
    monkeypatch.setattr(strat, "MIN_VOLUME_Z", 1.5)
    should_enter, reason = strat.decide_entry_technical(_row(dollar_volume_z=0.5))
    assert not should_enter
    assert "volume" in reason


def test_entry_requires_a_real_dip():
    should_enter, reason = strat.decide_entry_technical(_row(short_ma=100.0, current_price=100.0))
    assert not should_enter
    assert "dip" in reason


def test_entry_fires_when_all_gates_pass():
    should_enter, reason = strat.decide_entry_technical(_row())
    assert should_enter


def test_evaluate_candidate_technical_only_fallback_without_a_model():
    result = strat.evaluate_candidate(_row(), model_prediction=None)
    assert result["technical_ok"]
    assert result["should_enter"]
    assert result["model_ok"] is False


def test_evaluate_candidate_requires_model_confidence_when_a_model_exists():
    low_confidence = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.51})
    assert not low_confidence["should_enter"]

    high_confidence = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.7})
    assert high_confidence["should_enter"]


def test_evaluate_candidate_skips_model_check_when_technical_gate_fails():
    no_dip_row = _row(short_ma=100.0, current_price=100.0)
    result = strat.evaluate_candidate(no_dip_row, {"model_ok": True, "probability_up": 0.9})
    assert not result["should_enter"]
    assert result["model_ok"] is False


def test_evaluate_candidate_score_is_model_probability_when_model_based():
    result = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.7})
    assert result["should_enter"]
    assert result["score"] == 0.7


def test_evaluate_candidate_score_is_dip_depth_in_technical_only_fallback():
    result = strat.evaluate_candidate(_row(), model_prediction=None)
    assert result["should_enter"]
    assert result["score"] > 0.0


def test_evaluate_candidate_confidence_min_override_replaces_the_module_default():
    # 0.6 clears the module default (0.55) but not an explicit, stricter override.
    with_default = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.6})
    assert with_default["should_enter"]
    with_stricter_override = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.6}, confidence_min=0.65)
    assert not with_stricter_override["should_enter"]
    with_looser_override = strat.evaluate_candidate(_row(), {"model_ok": True, "probability_up": 0.52}, confidence_min=0.5)
    assert with_looser_override["should_enter"]


def _position(entry_price=100.0, minutes_ago=0):
    opened = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=minutes_ago)
    return {"entry_price": entry_price, "opened_at": opened.isoformat()}


def test_decide_exit_take_profit():
    pos = _position(entry_price=100.0)
    should_exit, reason = strat.decide_exit(pos, 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001))
    assert should_exit and "take_profit" in reason


def test_decide_exit_stop_loss():
    pos = _position(entry_price=100.0)
    should_exit, reason = strat.decide_exit(pos, 100.0 * (1 - strat.STOP_LOSS_PCT - 0.001))
    assert should_exit and "stop_loss" in reason


def test_decide_exit_max_hold_time():
    pos = _position(entry_price=100.0, minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(pos, 100.0)  # flat price -- neither TP nor SL
    assert should_exit and "max_hold_time" in reason


# ── "Promising position" max_hold_time extension ────────────────────────────
# See perps_strategy.py's own PROMISING_PROGRESS_FRACTION comment for the
# full rationale and real backtest findings.

def test_promising_position_by_price_progress_gets_extended_past_max_hold():
    pos = _position(entry_price=100.0, minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(pos, 100.0 * 1.005)  # +0.5% vs 1% TP
    assert not should_exit
    assert "holding" in reason


def test_promising_position_still_force_closed_once_extension_window_elapses():
    past_extension = strat.MAX_HOLD_MINUTES + strat.MAX_HOLD_EXTENSION_MINUTES + 1
    pos = _position(entry_price=100.0, minutes_ago=past_extension)
    should_exit, reason = strat.decide_exit(pos, 100.0 * 1.005)
    assert should_exit and "max_hold_time" in reason


def test_volume_and_momentum_confluence_extends_even_without_price_progress():
    pos = _position(entry_price=100.0, minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(
        pos, 100.0 * 1.0005, dollar_volume_z=2.0, momentum_pct=0.001,
    )
    assert not should_exit
    assert "holding" in reason


def test_momentum_extension_requires_position_not_already_reversing():
    pos = _position(entry_price=100.0, minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(
        pos, 100.0 * (1 - strat.STOP_LOSS_PCT * 0.5), dollar_volume_z=2.0, momentum_pct=0.001,
    )
    assert should_exit and "max_hold_time" in reason


def test_decide_exit_holds_when_nothing_triggers():
    pos = _position(entry_price=100.0)
    should_exit, reason = strat.decide_exit(pos, 100.1)
    assert not should_exit and "holding" in reason


def test_decide_exit_respects_explicit_simulated_now():
    """A backtest replaying historical rows must use the SIMULATED current
    time, not real wall-clock time -- the exact bug that was found and
    fixed in the Kalshi perps backtest earlier this session."""
    opened = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
    pos = {"entry_price": 100.0, "opened_at": opened.isoformat()}
    sim_now = opened + dt.timedelta(minutes=5)  # well under MAX_HOLD_MINUTES
    should_exit, reason = strat.decide_exit(pos, 100.1, now=sim_now)
    assert not should_exit  # would be a false max_hold_time if real wall-clock time leaked in


def test_position_exit_levels():
    levels = strat.position_exit_levels({"entry_price": 100.0})
    assert levels["take_profit_price"] == round(100.0 * (1 + strat.TAKE_PROFIT_PCT), 6)
    assert levels["stop_loss_price"] == round(100.0 * (1 - strat.STOP_LOSS_PCT), 6)


# ---------------------------------------------------------------------------
# Adaptive per-symbol exit levels -- scaled to each symbol's own
# volatility_30 at entry, not one flat percentage applied identically to
# every symbol.
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
    """Real edge case: a rolling-window feature still NaN this early must
    fall back to the same flat defaults as a missing value -- Python's own
    `nan <= 0` and `not nan` are both False, so a naive falsy/<=0 guard
    alone would miss this and let NaN propagate into the clamped result."""
    pcts = strat.adaptive_exit_pcts(float("nan"))
    assert pcts["take_profit_pct"] == strat.TAKE_PROFIT_PCT
    assert pcts["stop_loss_pct"] == strat.STOP_LOSS_PCT


def test_decide_exit_uses_the_positions_own_adaptive_levels():
    pos = _position(entry_price=100.0)
    pos["entry_volatility_30"] = 0.01  # wide enough to push take-profit well above the flat default
    exit_pcts = strat.adaptive_exit_pcts(0.01)
    should_exit, reason = strat.decide_exit(pos, 100.0 * (1 + exit_pcts["take_profit_pct"] + 0.001))
    assert should_exit and "take_profit" in reason
    if exit_pcts["take_profit_pct"] > strat.TAKE_PROFIT_PCT:
        no_vol_pos = _position(entry_price=100.0)
        still_exits, _ = strat.decide_exit(no_vol_pos, 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.0005))
        assert still_exits  # sanity: flat default still fires on its own smaller target


def test_position_exit_levels_uses_the_adaptive_percentage_not_the_flat_global():
    pos = {"entry_price": 100.0, "entry_volatility_30": 0.01}
    levels = strat.position_exit_levels(pos)
    adaptive = strat.adaptive_exit_pcts(0.01)
    assert levels["take_profit_price"] == round(100.0 * (1 + adaptive["take_profit_pct"]), 6)
    assert adaptive["take_profit_pct"] != strat.TAKE_PROFIT_PCT
    assert levels["take_profit_price"] != round(100.0 * (1 + strat.TAKE_PROFIT_PCT), 6)


def test_compute_position_size_floors_to_whole_shares():
    count = strat.compute_position_size(1000.0, 30.0)
    assert count == int((1000.0 * strat.POSITION_SIZE_PCT) // 30.0)


def test_compute_position_size_zero_at_zero_price():
    assert strat.compute_position_size(1000.0, 0.0) == 0


# ---------------------------------------------------------------------------
# Position lifecycle -- state persistence, entry, exit.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _isolated_state(tmp_path, monkeypatch):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "alpaca_state.json")
    monkeypatch.setattr(strat, "HF_API_KEY", "")  # no real network for the HF durable-state mirror by default
    # get_available_balance() always calls alpaca_client.get_account() (no
    # more "simulate mode" local-math fallback) -- every scan_and_enter
    # test implicitly depends on this succeeding for position sizing, so a
    # sane default lives here; tests exercising the loss cap or reconcile
    # path override it with a specific value where the number matters.
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "100.0"})
    yield


def test_push_durable_state_to_hf_is_bounded_by_a_hard_timeout(monkeypatch):
    """Real, confirmed production incident -- see perps_strategy.py's own
    identical fix for the full incident writeup: this upload used to be a
    plain, unbounded call, always made while _STATE_LOCK is held (every
    push_durable=True caller goes through _save_state), so a genuine
    huggingface_hub internal-session-lock hang could hold _STATE_LOCK
    indefinitely, freezing the entire --workers 1 process until gunicorn's
    own 300s worker timeout SIGKILLed it."""
    import server_common

    monkeypatch.setattr(strat, "HF_API_KEY", "fake-key")
    captured = {}

    def fake_hard_timeout(fn, *, timeout_sec, on_timeout=None):
        captured["timeout_sec"] = timeout_sec
        return fn()

    monkeypatch.setattr(server_common, "call_with_hard_timeout", fake_hard_timeout)
    uploaded = {}
    fake_hf_api = type("FakeHfApi", (), {
        "__init__": lambda self, token=None: None,
        "upload_file": lambda self, **kw: uploaded.update(kw),
    })
    monkeypatch.setattr("huggingface_hub.HfApi", fake_hf_api)

    strat._push_durable_state_to_hf({"positions": [], "trade_log": [{"x": 1}], "realized_pnl_by_date": {}, "daily_reference_balance": {}})  # noqa: SLF001

    assert captured["timeout_sec"] == strat._DURABLE_STATE_HF_TIMEOUT_SEC  # noqa: SLF001
    assert uploaded


def test_push_durable_state_to_hf_never_hangs_when_the_upload_hangs(monkeypatch):
    """End-to-end: even a genuinely hanging upload must not block the
    caller past the configured timeout."""
    monkeypatch.setattr(strat, "HF_API_KEY", "fake-key")
    monkeypatch.setattr(strat, "_DURABLE_STATE_HF_TIMEOUT_SEC", 0.2)

    class _HangingHfApi:
        def __init__(self, token=None):
            pass

        def upload_file(self, **kw):
            import time
            time.sleep(5)

    monkeypatch.setattr("huggingface_hub.HfApi", _HangingHfApi)

    import time as real_time
    start = real_time.monotonic()
    strat._push_durable_state_to_hf({"positions": [], "trade_log": [], "realized_pnl_by_date": {}, "daily_reference_balance": {}})  # noqa: SLF001
    assert real_time.monotonic() - start < 2.0


def test_load_state_defaults_to_an_empty_position_list():
    state = strat._load_state()  # noqa: SLF001
    assert state["positions"] == []
    assert "balance" not in state


def test_get_available_balance_reads_the_real_alpaca_cash_balance(monkeypatch):
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "321.0"})
    assert strat.get_available_balance() == 321.0


def test_get_current_price_averages_bid_and_ask_from_the_latest_quote(monkeypatch):
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": 124.0, "bp": 122.9})
    assert strat.get_current_price("AAPL") == pytest.approx(123.45)


def test_get_current_price_falls_back_to_whichever_side_is_present(monkeypatch):
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": 124.0, "bp": 0})
    assert strat.get_current_price("AAPL") == 124.0


def test_get_current_price_returns_none_on_a_failed_quote(monkeypatch):
    def fail(symbol):
        raise RuntimeError("network down")

    monkeypatch.setattr(alpaca_client, "get_latest_quote", fail)
    assert strat.get_current_price("AAPL") is None


def _entry_row(**overrides):
    # A $100 simulate balance at the default 45% position size needs an
    # affordable-in-whole-shares current_price.
    base = {
        "symbol": "AAPL", "current_price": 5.0, "short_ma": 5.015,
        "dollar_volume_z": 2.0, "volatility_5": 0.002, "volatility_30": 0.001,
    }
    base.update(overrides)
    return base


def test_scan_and_enter_dry_run_opens_a_position_without_any_real_order(monkeypatch):
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})

    def fail_if_called(*a, **k):
        raise AssertionError("dry-run must never place a real order")

    monkeypatch.setattr(alpaca_client, "place_order", fail_if_called)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    assert result["opened"][0]["dry_run"] is True

    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1
    assert state["positions"][0]["symbol"] == "AAPL"
    assert state["positions"][0]["order_id"] is None


def test_scan_and_enter_prewarms_sentiment_for_symbols_not_already_held(monkeypatch):
    """Real, confirmed root cause this fixes (same shape as
    crypto_news.prewarm_sentiment, see its own docstring) -- evaluating
    each watchlist symbol's sentiment SEQUENTIALLY risked delaying the
    separate exit-management job on the shared single-threaded worker."""
    strat._save_state({  # noqa: SLF001
        "positions": [{"symbol": "MSFT", "entry_price": 300.0, "count": 1}],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL", "MSFT"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "get_company_name", lambda symbol: f"{symbol} Inc.")
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: None)  # short-circuit past the loop body
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})
    prewarmed_with = []
    monkeypatch.setattr(alpaca_data, "prewarm_sentiment", lambda pairs, **kw: prewarmed_with.extend(pairs))
    # Real, confirmed test-hygiene gap: scan_and_enter also prewarms
    # minute-bars now (see prewarm_minute_bars' own docstring) -- left
    # unmocked here, it calls the REAL fetch_recent_minute_bars, hitting a
    # real network endpoint during a test run (a real hang from this exact
    # gap was observed in alpaca_crypto_data.py's own collect_dataset_rows
    # tests).
    monkeypatch.setattr(alpaca_data, "prewarm_minute_bars", lambda *a, **kw: None)

    strat.scan_and_enter()
    # MSFT is already held -- must not be re-prewarmed.
    assert prewarmed_with == [("AAPL", "AAPL Inc.")]


def test_scan_and_enter_still_works_if_sentiment_prewarm_fails(monkeypatch):
    """Best-effort optimization only -- a failure here must never block a
    real entry."""
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "get_company_name", lambda symbol: f"{symbol} Inc.")
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})
    monkeypatch.setattr(alpaca_data, "prewarm_minute_bars", lambda *a, **kw: None)

    def raise_error(pairs, **kw):
        raise RuntimeError("simulated prewarm failure")

    monkeypatch.setattr(alpaca_data, "prewarm_sentiment", raise_error)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"


def test_scan_and_enter_places_an_extended_hours_limit_order_in_pre_market(monkeypatch):
    """A bracket order is regular-hours only -- pre/post-market must place
    a plain limit order with extended_hours=true instead."""
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "pre_market", "is_open": False, "source": "test"})
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "100.0"})

    captured = {}
    monkeypatch.setattr(alpaca_client, "place_order", lambda order_spec: captured.update(order_spec) or "order-1")
    monkeypatch.setattr(alpaca_client, "get_order", lambda order_id: {"filled_qty": "1", "filled_avg_price": "150.0"})

    result = strat.scan_and_enter(dry_run=False)
    assert result["opened"][0]["action"] == "opened"
    assert captured["type"] == "limit"
    assert captured["extended_hours"] is True
    assert captured["side"] == "buy"
    assert "order_class" not in captured


def test_scan_and_enter_places_a_bracket_order_during_regular_hours(monkeypatch):
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "100.0"})

    captured = {}
    monkeypatch.setattr(alpaca_client, "place_order", lambda order_spec: captured.update(order_spec) or "order-1")
    monkeypatch.setattr(alpaca_client, "get_order", lambda order_id: {"filled_qty": "1", "filled_avg_price": "150.0"})

    result = strat.scan_and_enter(dry_run=False)
    assert result["opened"][0]["action"] == "opened"
    assert captured["order_class"] == "bracket"


# ── Real, confirmed live incident (2026-09-14): an order being PLACED was
# treated as the position being OPEN, with no fill check at all -- during
# extended hours (a plain LIMIT order, unlike a bracket order, can easily
# sit unfilled) this repeated the same "buy AAPL" order and Threads post
# every single 2-minute entry-scan cycle for 90+ minutes straight, because
# the never-verified, still-resting order was never recorded locally, so
# existing_symbols never learned AAPL was "already handled." ────────────────

def test_scan_and_enter_does_not_open_a_position_for_an_unfilled_order(monkeypatch):
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "post_market", "is_open": False, "source": "test"})
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "100.0"})
    monkeypatch.setattr(alpaca_client, "place_order", lambda order_spec: "order-1")
    monkeypatch.setattr(alpaca_client, "get_order", lambda order_id: {"filled_qty": "0", "status": "accepted"})
    cancelled = []
    monkeypatch.setattr(alpaca_client, "cancel_order", lambda order_id: cancelled.append(order_id))

    result = strat.scan_and_enter(dry_run=False)

    assert result["opened"][0]["action"] == "skipped_order_not_filled"
    assert cancelled == ["order-1"]
    state = strat._load_state()  # noqa: SLF001
    assert state["positions"] == []  # never recorded -- the next scan can genuinely retry


def test_scan_and_enter_re_evaluates_the_same_symbol_after_an_unfilled_order(monkeypatch):
    """The actual fix, not just its side effect: a symbol whose order didn't
    fill must NOT be treated as "already handled" -- it has to remain a
    fresh candidate on the very next scan, closing the exact loop that
    caused 90+ minutes of repeated real orders and Threads posts."""
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "post_market", "is_open": False, "source": "test"})
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "100.0"})
    monkeypatch.setattr(alpaca_client, "place_order", lambda order_spec: "order-1")
    monkeypatch.setattr(alpaca_client, "get_order", lambda order_id: {"filled_qty": "0"})
    monkeypatch.setattr(alpaca_client, "cancel_order", lambda order_id: None)

    first = strat.scan_and_enter(dry_run=False)
    second = strat.scan_and_enter(dry_run=False)

    assert first["opened"][0]["action"] == "skipped_order_not_filled"
    assert second["opened"][0]["action"] == "skipped_order_not_filled"  # re-evaluated, not "already held"


def test_scan_and_enter_survives_a_fill_check_failure_by_treating_it_as_unfilled(monkeypatch):
    """A network hiccup checking the order's own status must fail closed --
    never assume a fill it couldn't actually verify."""
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "100.0"})
    monkeypatch.setattr(alpaca_client, "place_order", lambda order_spec: "order-1")

    def raise_error(order_id):
        raise RuntimeError("simulated network failure")

    monkeypatch.setattr(alpaca_client, "get_order", raise_error)
    monkeypatch.setattr(alpaca_client, "cancel_order", lambda order_id: None)

    result = strat.scan_and_enter(dry_run=False)

    assert result["opened"][0]["action"] == "skipped_order_not_filled"
    state = strat._load_state()  # noqa: SLF001
    assert state["positions"] == []


def test_scan_and_enter_uses_the_real_fill_price_not_the_pre_order_estimate(monkeypatch):
    """A marketable limit order can fill at a slightly different price than
    the quote it was sized against -- the recorded position (and its
    take-profit/stop-loss levels) must reflect what actually happened, not
    the pre-order guess."""
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row(current_price=150.0, short_ma=150.45))
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "10000.0"})
    monkeypatch.setattr(alpaca_client, "place_order", lambda order_spec: "order-1")
    monkeypatch.setattr(alpaca_client, "get_order", lambda order_id: {"filled_qty": "3", "filled_avg_price": "150.75"})

    result = strat.scan_and_enter(dry_run=False)

    assert result["opened"][0]["action"] == "opened"
    state = strat._load_state()  # noqa: SLF001
    position = state["positions"][0]
    assert position["entry_price"] == 150.75
    assert position["count"] == 3.0
    expected_levels = strat.position_exit_levels({"entry_price": 150.75, "entry_volatility_30": 0.001})
    assert position["take_profit_price"] == pytest.approx(expected_levels["take_profit_price"])


def test_scan_and_enter_skips_entirely_when_market_is_fully_closed(monkeypatch):
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "closed", "is_open": False, "source": "test"})

    def fail_if_called(*a, **kw):
        raise AssertionError("must not evaluate any symbol when the market is fully closed")

    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", fail_if_called)
    monkeypatch.setattr(alpaca_data, "latest_feature_row", fail_if_called)

    result = strat.scan_and_enter()
    assert result == {"opened": [], "action": "market_closed"}


def test_scan_and_enter_skips_a_symbol_already_held(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "positions": [{"symbol": "AAPL", "entry_price": 100.0, "count": 1.0}],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())

    def fail_if_called(symbol):
        raise AssertionError("must not re-evaluate a symbol that's already held")

    monkeypatch.setattr(alpaca_data, "latest_feature_row", fail_if_called)

    result = strat.scan_and_enter()
    assert result["opened"] == []


def test_scan_and_enter_skips_a_symbol_still_on_a_post_stop_loss_cooldown(monkeypatch):
    """Real, confirmed production incident (see
    SYMBOL_COOLDOWN_AFTER_STOP_LOSS_MINUTES's own comment): AAPL gapped
    down pre-market on 2026-08-27 and the strategy kept re-buying the same
    'dip' within minutes of each stop_loss, three times, for ~$9,700 of
    real realized loss. A symbol on an active cooldown must be skipped
    entirely -- not even feature-fetched -- until the cooldown expires."""
    future_cooldown = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=30)).isoformat()
    strat._save_state({  # noqa: SLF001
        "positions": [], "trade_log": [], "realized_pnl_by_date": {},
        "symbol_cooldown_until": {"AAPL": future_cooldown},
    })
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())

    def fail_if_called(symbol):
        raise AssertionError("must not evaluate a symbol still on its stop_loss cooldown")

    monkeypatch.setattr(alpaca_data, "latest_feature_row", fail_if_called)

    result = strat.scan_and_enter()
    assert result["opened"] == [{"symbol": "AAPL", "ok": True, "action": "skipped_cooldown", "reason": f"stop_loss cooldown until {future_cooldown}"}]


def test_scan_and_enter_re_evaluates_a_symbol_once_its_cooldown_has_expired(monkeypatch):
    past_cooldown = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).isoformat()
    strat._save_state({  # noqa: SLF001
        "positions": [], "trade_log": [], "realized_pnl_by_date": {},
        "symbol_cooldown_until": {"AAPL": past_cooldown},
    })
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _row(symbol=symbol))
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: None)
    monkeypatch.setattr(strat, "get_available_balance", lambda: 100.0)

    result = strat.scan_and_enter(dry_run=True)
    assert result["opened"][0]["symbol"] == "AAPL"
    assert result["opened"][0].get("action") != "skipped_cooldown"


def test_manage_open_positions_sets_a_cooldown_after_a_real_stop_loss_exit(monkeypatch):
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
            "take_profit_price": 101.0, "stop_loss_price": 95.0,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": 95.0, "bp": 95.0})
    monkeypatch.setattr(alpaca_client, "get_position", lambda symbol: None)  # bracket already closed it

    result = strat.manage_open_positions()
    assert result["action"] == "closed"

    state = strat._load_state()  # noqa: SLF001
    assert "AAPL" in state.get("symbol_cooldown_until", {})
    cooldown_until = dt.datetime.fromisoformat(state["symbol_cooldown_until"]["AAPL"])
    assert cooldown_until > dt.datetime.now(dt.timezone.utc)


def test_scan_and_enter_respects_the_daily_loss_cap(monkeypatch):
    today = strat._today_str()  # noqa: SLF001
    strat._save_state({  # noqa: SLF001
        "positions": [], "trade_log": [],
        "realized_pnl_by_date": {today: -20.0},  # -20% of $100, breaches the 10% default cap
    })
    result = strat.scan_and_enter()
    assert result["action"] == "daily_loss_cap_breached"


def test_scan_and_enter_posts_to_threads_on_a_dry_run_entry(monkeypatch):
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})

    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_entry", lambda **kw: posted.update(kw) or True)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    assert posted["ticker"] == "AAPL"
    assert posted["side"] == "long"
    assert posted["dry_run"] is True  # dry-run -- must be flagged [SIMULATED]
    assert posted["take_profit_price"] > posted["entry_price"]
    assert posted["stop_loss_price"] < posted["entry_price"]


def test_scan_and_enter_still_opens_the_position_even_if_threads_post_raises(monkeypatch):
    """A Threads failure must never be allowed to affect a real/simulated
    entry, since post_trade_entry() is called AFTER the position is
    already saved to state."""
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})

    def raise_error(**kwargs):
        raise RuntimeError("simulated Threads API outage")

    monkeypatch.setattr(threads_post, "post_trade_entry", raise_error)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1


def test_scan_and_enter_one_symbol_failing_does_not_block_the_others(monkeypatch):
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["BAD", "AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())

    def fake_feature_row(symbol):
        if symbol == "BAD":
            raise RuntimeError("data fetch failed")
        return _entry_row(symbol=symbol)

    monkeypatch.setattr(alpaca_data, "latest_feature_row", fake_feature_row)
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})

    result = strat.scan_and_enter()
    outcomes = {o["symbol"]: o for o in result["opened"]}
    assert outcomes["BAD"]["ok"] is False
    assert outcomes["AAPL"]["ok"] is True and outcomes["AAPL"]["action"] == "opened"


def test_manage_open_positions_returns_no_position_without_any_state():
    result = strat.manage_open_positions()
    assert result["action"] == "no_position"


def test_manage_open_positions_posts_a_threads_exit_on_close(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_exit", lambda **kw: posted.update(kw) or True)

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert posted["ticker"] == "AAPL"
    assert posted["market"] == "stocks"
    assert posted["pnl_usd"] > 0


def _one_min_df(n=30, base_ts=None):
    base_ts = base_ts or int(dt.datetime.now(dt.timezone.utc).timestamp()) - n * 60
    rows = []
    price = 100.0
    for i in range(n):
        o = price
        price += 0.05
        rows.append({"ts": base_ts + i * 60, "open": o, "high": max(o, price) + 0.02, "low": min(o, price) - 0.02, "close": price})
    return pd.DataFrame(rows)


def test_candles_as_dicts_converts_a_dataframe_to_plain_dicts():
    dicts = strat._candles_as_dicts(_one_min_df(5))  # noqa: SLF001
    assert len(dicts) == 5
    assert set(dicts[0].keys()) == {"ts", "open", "high", "low", "close"}


def test_index_for_ts_finds_the_closest_candle():
    df = _one_min_df(10, base_ts=1000)
    idx = strat._index_for_ts(df, dt.datetime.fromtimestamp(1000 + 3 * 60, dt.timezone.utc).isoformat())  # noqa: SLF001
    assert idx == 3


def test_scan_and_enter_posts_a_candlestick_entry_chart(monkeypatch):
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})
    monkeypatch.setattr(alpaca_data, "fetch_recent_minute_bars", lambda symbol: _one_min_df())

    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_entry_chart", lambda **kw: posted.update(kw) or True)

    result = strat.scan_and_enter()
    assert result["opened"][0]["action"] == "opened"
    assert posted["ticker"] == "AAPL"
    assert posted["market"] == "stocks"
    assert len(posted["candles"]) == 30
    assert posted["entry_index"] == 29


def test_manage_open_positions_posts_a_candlestick_exit_chart_on_close(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})
    monkeypatch.setattr(alpaca_data, "fetch_recent_minute_bars", lambda symbol: _one_min_df())

    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_exit_chart", lambda **kw: posted.update(kw) or True)

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert posted["ticker"] == "AAPL"
    assert posted["market"] == "stocks"
    assert posted["pnl_usd"] == result["closed"][0]["realized_pnl_usd"]


def test_manage_open_positions_records_entry_context_and_hold_minutes_on_close(monkeypatch):
    opened = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=7)
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0, "opened_at": opened.isoformat(),
            "order_id": None, "entry_probability_up": 0.7, "entry_model_direction": "up", "entry_reason": "dip + model",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    result = strat.manage_open_positions()
    trade = result["closed"][0]
    assert trade["entry_probability_up"] == 0.7
    assert trade["entry_reason"] == "dip + model"
    assert trade["hold_minutes"] == pytest.approx(7, abs=0.1)


def test_maybe_run_batch_trade_analysis_runs_at_the_batch_boundary_and_posts(monkeypatch):
    trades = [
        {
            "symbol": "AAPL", "realized_pnl_usd": 1.0, "dry_run": False, "reason": "take_profit (+2%)",
            "entry_price": 100.0, "exit_price": 101.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "closed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        for _ in range(5)
    ]
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": trades})  # noqa: SLF001
    monkeypatch.setattr(alpaca_data, "fetch_recent_minute_bars", lambda symbol: _one_min_df())
    posted = {}
    monkeypatch.setattr(threads_post, "post_trade_analysis_summary", lambda text, **kw: posted.update(text=text, **kw) or True)

    strat._maybe_run_batch_trade_analysis()  # noqa: SLF001
    assert posted["market"] == "stocks"
    assert "5" in posted["text"]
    state = strat._load_state()  # noqa: SLF001
    assert state["last_batch_analysis_trade_count"] == 5


def test_maybe_run_batch_trade_analysis_skips_below_batch_size(monkeypatch):
    trades = [{"symbol": "AAPL", "realized_pnl_usd": 1.0, "dry_run": False} for _ in range(3)]
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": trades})  # noqa: SLF001
    called = {"n": 0}
    monkeypatch.setattr(threads_post, "post_trade_analysis_summary", lambda *a, **kw: called.update(n=called["n"] + 1))
    strat._maybe_run_batch_trade_analysis()  # noqa: SLF001
    assert called["n"] == 0


# ── maybe_auto_improve_from_backtest -- the user's own explicit request:
# "whenever you get negative return on backtest and forward test[,] need
# to automatically improve" applied to stocks too ("review it... apply
# improvement needed so it can be profitable"). Mirrors
# test_alpaca_crypto_strategy.py's/test_alpaca_options_strategy.py's own
# identical coverage. ─────────────────────────────────────────────────

def test_maybe_auto_improve_from_backtest_no_op_when_both_are_profitable():
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": []})  # noqa: SLF001
    sweep = {"all_configs": [{"label": "current_defaults", "return_pct": 0.05, "trade_count": 50, "low_sample": False}]}
    result = strat.maybe_auto_improve_from_backtest(sweep, {"mean_return_pct": 0.02})
    assert result["triggered"] is False


def test_maybe_auto_improve_from_backtest_applies_a_better_confidence_and_retrains(monkeypatch):
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": []})  # noqa: SLF001
    sweep = {"all_configs": [
        {"label": "current_defaults", "return_pct": -0.05, "trade_count": 50, "low_sample": False, "model_confidence_min": 0.52},
        {"return_pct": 0.05, "trade_count": 50, "low_sample": False, "model_confidence_min": 0.60},
    ]}
    train_calls = []
    torch_calls = []
    monkeypatch.setattr(alpaca_model, "train_model", lambda **kw: train_calls.append(kw) or {"ok": True, "rows": 1000})
    monkeypatch.setattr(alpaca_model, "train_torch_candidate_model", lambda **kw: torch_calls.append(kw) or {"ok": True, "promoted": False})

    result = strat.maybe_auto_improve_from_backtest(sweep, {"mean_return_pct": -0.02})

    assert result["triggered"] is True
    assert result["confidence_recommendation"]["should_apply"] is True
    state = strat._load_state()  # noqa: SLF001
    assert state["tuning"]["model_confidence_min"] == 0.60
    assert len(train_calls) == 1
    assert len(torch_calls) == 1


def test_maybe_auto_improve_from_backtest_still_retrains_when_no_better_confidence_found(monkeypatch):
    """A loss with no sweep evidence pointing at a better confidence floor
    must still fall back to the "use everything the bot has" retrain --
    the two responses are independent."""
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": []})  # noqa: SLF001
    sweep = {"all_configs": [{"label": "current_defaults", "return_pct": -0.05, "trade_count": 50, "low_sample": False}]}
    train_calls = []
    monkeypatch.setattr(alpaca_model, "train_model", lambda **kw: train_calls.append(kw) or {"ok": True, "rows": 1000})
    monkeypatch.setattr(alpaca_model, "train_torch_candidate_model", lambda **kw: {"ok": True, "promoted": False})

    result = strat.maybe_auto_improve_from_backtest(sweep, None)

    assert result["triggered"] is True
    assert result["confidence_recommendation"]["should_apply"] is False
    assert len(train_calls) == 1


def test_maybe_auto_improve_from_backtest_survives_a_retrain_failure(monkeypatch):
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": []})  # noqa: SLF001
    sweep = {"all_configs": [{"label": "current_defaults", "return_pct": -0.05, "trade_count": 50, "low_sample": False}]}

    def raise_error(**kw):
        raise RuntimeError("simulated retrain crash")

    monkeypatch.setattr(alpaca_model, "train_model", raise_error)
    monkeypatch.setattr(alpaca_model, "train_torch_candidate_model", raise_error)

    result = strat.maybe_auto_improve_from_backtest(sweep, None)  # must not raise
    assert result["triggered"] is True


def test_maybe_auto_improve_from_backtest_never_retrains_without_a_loss(monkeypatch):
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": []})  # noqa: SLF001

    def fail_if_called(**kw):
        raise AssertionError("must not retrain when neither backtest shows a loss")

    monkeypatch.setattr(alpaca_model, "train_model", fail_if_called)
    sweep = {"all_configs": [{"label": "current_defaults", "return_pct": 0.05, "trade_count": 50, "low_sample": False}]}

    strat.maybe_auto_improve_from_backtest(sweep, {"mean_return_pct": 0.02})


def test_manage_open_positions_dry_run_closes_on_take_profit(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert "take_profit" in result["closed"][0]["reason"]

    state = strat._load_state()  # noqa: SLF001
    assert state["positions"] == []
    expected_gain = round(take_profit_price - 100.0, 6)
    assert state["trade_log"][0]["realized_pnl_usd"] == expected_gain


def test_manage_open_positions_leaves_position_open_when_nothing_triggers(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": 100.05, "bp": 100.05})
    result = strat.manage_open_positions()
    assert result["action"] == "no_change"
    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1


def test_manage_open_positions_one_bad_quote_does_not_block_the_others(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "positions": [
            {"symbol": "BAD", "entry_price": 100.0, "count": 1.0, "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None},
            {"symbol": "AAPL", "entry_price": 100.0, "count": 1.0, "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None},
        ],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)

    def fake_quote(symbol):
        if symbol == "BAD":
            raise RuntimeError("quote service down")
        return {"ap": take_profit_price, "bp": take_profit_price}

    monkeypatch.setattr(alpaca_client, "get_latest_quote", fake_quote)
    result = strat.manage_open_positions()
    assert any(c["symbol"] == "BAD" and c["ok"] is False for c in result["checks"])
    assert any(t["symbol"] == "AAPL" for t in result["closed"])
    state = strat._load_state()  # noqa: SLF001
    assert [p["symbol"] for p in state["positions"]] == ["BAD"]  # AAPL closed, BAD stayed open


def test_manage_open_positions_never_places_a_real_order_in_dry_run(monkeypatch):
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    def fail_if_called(*a, **k):
        raise AssertionError("dry-run must never place or cancel a real order")

    monkeypatch.setattr(alpaca_client, "place_order", fail_if_called)
    monkeypatch.setattr(alpaca_client, "close_position", fail_if_called)

    result = strat.manage_open_positions()
    assert result["action"] == "closed"


# ---------------------------------------------------------------------------
# Live-mode exit reconciliation -- new relative to the former Schwab
# strategy: before forcing a live exit, check whether Alpaca's own bracket
# take-profit/stop-loss already closed the position moments earlier (a real
# double-sell risk if not checked).
# ---------------------------------------------------------------------------
def test_manage_open_positions_live_mode_closes_via_close_position_when_still_open(monkeypatch):
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})
    # First call is the pre-decision "still open?" check (still open); second
    # call is the post-order fill verification, after the close fully filled.
    calls = {"n": 0}

    def fake_get_position(symbol):
        calls["n"] += 1
        return {"symbol": symbol, "qty": "1"} if calls["n"] == 1 else None

    monkeypatch.setattr(alpaca_client, "get_position", fake_get_position)
    closed_calls = []
    monkeypatch.setattr(alpaca_client, "close_position", lambda symbol: closed_calls.append(symbol))

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert closed_calls == ["AAPL"]


def test_manage_open_positions_does_not_book_a_phantom_trade_when_the_close_order_has_not_filled_yet(monkeypatch):
    """Real, confirmed production incident: booking P&L immediately after
    SUBMITTING a close order (without checking it actually filled) produced
    8 duplicate "closed" trade_log entries for the same real META position
    -- a stop_loss that took several fast_check cycles to actually fill,
    re-discovered as "untracked" and re-booked every cycle, ~$6,400 of
    phantom recorded loss. get_position must be checked AGAIN after placing
    the close order -- if the position is still fully there, no trade
    should be booked and the position must stay open for the next cycle."""
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})
    # Every check (before AND after the close attempt) sees the position
    # still fully open -- the close order was submitted but never filled.
    monkeypatch.setattr(alpaca_client, "get_position", lambda symbol: {"symbol": symbol, "qty": "1"})
    monkeypatch.setattr(alpaca_client, "close_position", lambda symbol: None)

    result = strat.manage_open_positions()
    assert result["action"] == "no_change"
    assert result["closed"] == []
    assert result["checks"][0]["error"] == "exit_order_did_not_fill"
    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1  # still open, not silently dropped
    assert state["trade_log"] == []  # no phantom trade booked


def test_manage_open_positions_books_only_the_actually_filled_quantity_on_a_partial_fill(monkeypatch):
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 10.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})
    calls = {"n": 0}

    def fake_get_position(symbol):
        calls["n"] += 1
        # Still open before the close attempt (10 shares); only 6 filled,
        # 4 remain after we tried to close the full 10.
        return {"symbol": symbol, "qty": "10" if calls["n"] == 1 else "4"}

    monkeypatch.setattr(alpaca_client, "get_position", fake_get_position)
    monkeypatch.setattr(alpaca_client, "close_position", lambda symbol: None)

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert result["closed"][0]["count"] == 6.0
    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1  # remainder kept open, not dropped
    assert state["positions"][0]["count"] == 4.0


def test_manage_open_positions_live_mode_reconciles_without_double_selling_when_bracket_already_closed_it(monkeypatch):
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
            "take_profit_price": 101.0, "stop_loss_price": 99.0,
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})
    # Alpaca's own bracket order already closed this position -- no position exists anymore.
    monkeypatch.setattr(alpaca_client, "get_position", lambda symbol: None)

    def fail_if_called(symbol):
        raise AssertionError("must not attempt to close a position that's already gone (double-sell risk)")

    monkeypatch.setattr(alpaca_client, "close_position", fail_if_called)

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    # Reconciled using the position's OWN stored take-profit level, not the
    # (possibly stale) live quote fetched after the bracket already fired.
    assert result["closed"][0]["exit_price"] == 101.0


def test_manage_open_positions_suppresses_a_duplicate_exit_from_repeated_re_adoption(monkeypatch):
    """Real, confirmed production incident (see the dedup guard's own
    comment in manage_open_positions): when Alpaca's bracket already closed
    a position and reconciliation re-ADOPTS the same still-lingering
    exchange position as a fresh 'untracked' one on the very next tick
    (each adoption stamping a brand new opened_at), the still_open == False
    branch re-booked the identical close over and over -- 8 times for one
    real META position, 3-5 times for two real AAPL positions. Simulates
    exactly that: two consecutive manage_open_positions calls, each seeing
    the SAME symbol/count/stored-price position but a DIFFERENT opened_at
    (as a fresh adoption would produce) -- only the first should book a
    trade; the second must be suppressed, not double-counted in
    realized_pnl_by_date."""
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)

    def _adopted_position(opened_at: str) -> dict:
        return {
            "symbol": "META", "entry_price": 590.027647, "count": 34.0,
            "opened_at": opened_at, "order_id": None,
            "take_profit_price": 620.0, "stop_loss_price": 556.37,
        }

    strat._save_state({  # noqa: SLF001
        "positions": [_adopted_position("2026-08-06T20:00:00.000000+00:00")],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": 556.37, "bp": 556.37})
    # Alpaca's own bracket already closed this position -- both calls see it gone.
    monkeypatch.setattr(alpaca_client, "get_position", lambda symbol: None)

    first = strat.manage_open_positions()
    assert first["action"] == "closed"
    assert len(first["closed"]) == 1
    assert first["closed"][0]["realized_pnl_usd"] == pytest.approx((556.37 - 590.027647) * 34.0)

    state_after_first = strat._load_state()  # noqa: SLF001
    assert len(state_after_first["trade_log"]) == 1
    booked_pnl = state_after_first["realized_pnl_by_date"][strat._today_str()]  # noqa: SLF001

    # Re-adoption on the next tick: same symbol/count/stored price, but a
    # freshly-stamped (different) opened_at -- exactly what
    # _reconcile_positions_with_exchange's adoption path produces.
    strat._save_state({  # noqa: SLF001
        "positions": [_adopted_position("2026-08-06T20:00:15.000000+00:00")],
        "trade_log": state_after_first["trade_log"],
        "realized_pnl_by_date": state_after_first["realized_pnl_by_date"],
    })

    second = strat.manage_open_positions()
    assert second["action"] == "no_change"
    assert second["closed"] == []
    assert second["checks"][0]["action"] == "duplicate_exit_suppressed"

    state_after_second = strat._load_state()  # noqa: SLF001
    # No second trade_log entry, and NOT double-counted in today's realized P&L.
    assert len(state_after_second["trade_log"]) == 1
    assert state_after_second["realized_pnl_by_date"][strat._today_str()] == pytest.approx(booked_pnl)
    # The phantom re-adopted position must still be cleared so it can't loop forever.
    assert state_after_second["positions"] == []


def test_manage_open_positions_uses_an_extended_hours_limit_order_during_pre_market(monkeypatch):
    """close_position() liquidates at market -- Alpaca rejects that outside
    9:30-4:00 ET. Pre/post-market must use a plain limit sell with
    extended_hours=true instead, checked at EXIT time (not entry time)."""
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "pre_market", "is_open": False, "source": "test"})
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})
    # First call is the pre-decision "still open?" check (still open); second
    # call is the post-order fill verification, after the close fully filled.
    calls = {"n": 0}

    def fake_get_position(symbol):
        calls["n"] += 1
        return {"symbol": symbol, "qty": "1"} if calls["n"] == 1 else None

    monkeypatch.setattr(alpaca_client, "get_position", fake_get_position)

    def fail_if_called(symbol):
        raise AssertionError("close_position() is a market order -- must not be used outside regular hours")

    monkeypatch.setattr(alpaca_client, "close_position", fail_if_called)
    monkeypatch.setattr(alpaca_client, "get_orders", lambda **kwargs: [])
    captured = {}
    monkeypatch.setattr(alpaca_client, "place_order", lambda order_spec: captured.update(order_spec) or "order-2")

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert captured["type"] == "limit"
    assert captured["extended_hours"] is True
    assert captured["side"] == "sell"


def test_manage_open_positions_cancels_open_bracket_legs_before_an_extended_hours_exit(monkeypatch):
    """Real, confirmed live incident (2026-08-18): a stock position entered
    via a bracket order still has its take-profit/stop-loss CHILD orders
    open on Alpaca's books during extended hours (only close_position()'s
    own market-hours path cancels them automatically). Placing a new sell
    limit on top of that still-open bracket leg oversells the position from
    Alpaca's perspective and gets rejected with 403 -- confirmed via 1149
    consecutive failures on one real NVDA position, unable to exit for
    ~6.5 hours. Any open order for the symbol must be canceled first."""
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "after_hours", "is_open": False, "source": "test"})
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "NVDA", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})
    calls = {"n": 0}

    def fake_get_position(symbol):
        calls["n"] += 1
        return {"symbol": symbol, "qty": "1"} if calls["n"] == 1 else None

    monkeypatch.setattr(alpaca_client, "get_position", fake_get_position)
    monkeypatch.setattr(alpaca_client, "close_position", lambda symbol: (_ for _ in ()).throw(AssertionError("must not be used outside regular hours")))
    monkeypatch.setattr(
        alpaca_client, "get_orders",
        lambda **kwargs: [{"id": "bracket-tp-leg"}, {"id": "bracket-sl-leg"}] if kwargs.get("symbols") == ["NVDA"] else [],
    )
    canceled = []
    monkeypatch.setattr(alpaca_client, "cancel_order", lambda order_id: canceled.append(order_id))
    monkeypatch.setattr(alpaca_client, "place_order", lambda order_spec: "order-2")

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert set(canceled) == {"bracket-tp-leg", "bracket-sl-leg"}


def test_manage_open_positions_extended_hours_exit_still_places_order_if_cancel_fails(monkeypatch):
    """A best-effort cleanup -- if canceling the old bracket legs itself
    fails, still attempt the real exit order rather than giving up
    entirely (the exit order may still succeed, or fail with a clearer
    error that at least gets logged)."""
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "after_hours", "is_open": False, "source": "test"})
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "NVDA", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})
    calls = {"n": 0}

    def fake_get_position(symbol):
        calls["n"] += 1
        return {"symbol": symbol, "qty": "1"} if calls["n"] == 1 else None

    monkeypatch.setattr(alpaca_client, "get_position", fake_get_position)

    def raise_error(**kwargs):
        raise RuntimeError("network error")

    monkeypatch.setattr(alpaca_client, "get_orders", raise_error)
    placed = {}
    monkeypatch.setattr(alpaca_client, "place_order", lambda order_spec: placed.update(order_spec) or "order-2")

    result = strat.manage_open_positions()
    assert result["action"] == "closed"
    assert placed["side"] == "sell"


def test_manage_open_positions_defers_a_live_exit_when_market_is_fully_closed(monkeypatch):
    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "closed", "is_open": False, "source": "test"})
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({  # noqa: SLF001
        "positions": [{
            "symbol": "AAPL", "entry_price": 100.0, "count": 1.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": "order-1",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    take_profit_price = 100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(alpaca_client, "get_latest_quote", lambda symbol: {"ap": take_profit_price, "bp": take_profit_price})

    def fail_if_called(*a, **kw):
        raise AssertionError("must not attempt any order when the market is fully closed")

    monkeypatch.setattr(alpaca_client, "get_position", fail_if_called)
    monkeypatch.setattr(alpaca_client, "close_position", fail_if_called)
    monkeypatch.setattr(alpaca_client, "place_order", fail_if_called)

    result = strat.manage_open_positions()
    assert result["action"] == "no_change"
    # The position must still be there, untouched, to retry next cycle.
    with strat._STATE_LOCK:  # noqa: SLF001
        state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1


# ---------------------------------------------------------------------------
# Reconciliation against the real Alpaca account -- same ground-truth check
# already proven in alpaca_crypto_strategy.py/perps_strategy.py, ported here
# now that this strategy always trades against the real account.
# ---------------------------------------------------------------------------
def test_real_open_positions_by_symbol_filters_to_equity_asset_class(monkeypatch):
    positions = [
        {"symbol": "BTC/USD", "qty": "0.002", "avg_entry_price": "64000.0", "asset_class": "crypto"},
        {"symbol": "AAPL", "qty": "10", "avg_entry_price": "150.0", "asset_class": "us_equity"},
    ]
    monkeypatch.setattr(alpaca_client, "get_positions", lambda: positions)
    real = strat._real_open_positions_by_symbol()  # noqa: SLF001
    assert list(real.keys()) == ["AAPL"]
    assert real["AAPL"]["count"] == pytest.approx(10.0)


def test_real_open_positions_by_symbol_returns_none_on_a_failed_fetch(monkeypatch):
    def fail():
        raise RuntimeError("network down")

    monkeypatch.setattr(alpaca_client, "get_positions", fail)
    assert strat._real_open_positions_by_symbol() is None  # noqa: SLF001


def test_reconcile_adopts_an_untracked_real_position(monkeypatch):
    monkeypatch.setattr(alpaca_client, "get_positions", lambda: [
        {"symbol": "MSFT", "qty": "5", "avg_entry_price": "300.0", "asset_class": "us_equity"},
    ])
    reconciled = strat._reconcile_positions_with_exchange({"positions": []})  # noqa: SLF001
    assert len(reconciled) == 1
    assert reconciled[0]["symbol"] == "MSFT"
    assert reconciled[0]["count"] == pytest.approx(5.0)


def test_reconcile_corrects_a_drifted_local_position(monkeypatch):
    monkeypatch.setattr(alpaca_client, "get_positions", lambda: [
        {"symbol": "AAPL", "qty": "10", "avg_entry_price": "151.5", "asset_class": "us_equity"},
    ])
    local = [{"symbol": "AAPL", "entry_price": 150.0, "count": 9.0, "opened_at": "2026-01-01T00:00:00+00:00"}]
    reconciled = strat._reconcile_positions_with_exchange({"positions": local})  # noqa: SLF001
    assert len(reconciled) == 1
    assert reconciled[0]["count"] == pytest.approx(10.0)
    assert reconciled[0]["entry_price"] == pytest.approx(151.5)


def test_reconcile_drops_a_phantom_local_position(monkeypatch):
    monkeypatch.setattr(alpaca_client, "get_positions", lambda: [])
    local = [{"symbol": "AAPL", "entry_price": 150.0, "count": 10.0, "opened_at": "2026-01-01T00:00:00+00:00"}]
    reconciled = strat._reconcile_positions_with_exchange({"positions": local})  # noqa: SLF001
    assert reconciled == []


def test_reconcile_returns_local_positions_unchanged_when_the_real_fetch_fails(monkeypatch):
    def fail():
        raise RuntimeError("network down")

    monkeypatch.setattr(alpaca_client, "get_positions", fail)
    local = [{"symbol": "AAPL", "entry_price": 150.0, "count": 10.0, "opened_at": "2026-01-01T00:00:00+00:00"}]
    reconciled = strat._reconcile_positions_with_exchange({"positions": local})  # noqa: SLF001
    assert reconciled == local


def test_scan_and_enter_reconciles_before_entering_in_live_mode(monkeypatch):
    """A slot already occupied by a real, untracked exchange position must
    not also be counted as free -- reconciliation has to run BEFORE the
    slot-availability check."""
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(strat, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(alpaca_client, "get_positions", lambda: [
        {"symbol": "MSFT", "qty": "5", "avg_entry_price": "300.0", "asset_class": "us_equity"},
    ])
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0"})
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL"])
    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "latest_feature_row", lambda symbol: _entry_row())
    monkeypatch.setattr(alpaca_model, "predict_direction", lambda symbol: {"model_ok": False})

    result = strat.scan_and_enter()
    outcomes = {o["symbol"]: o for o in result["opened"]}
    assert outcomes["AAPL"]["action"] == "skipped_slot_taken"
    state = strat._load_state()  # noqa: SLF001
    assert any(p["symbol"] == "MSFT" for p in state["positions"])


# ---------------------------------------------------------------------------
# Daily reference balance -- fixed to the day's ACTUAL starting balance,
# not whatever the continuously-updated tracked balance happens to be.
# ---------------------------------------------------------------------------
def test_reference_balance_for_today_is_captured_once():
    state = {}
    first = strat._reference_balance_for_today(state, 100.0)  # noqa: SLF001
    second = strat._reference_balance_for_today(state, 999.0)  # noqa: SLF001
    assert first == 100.0
    assert second == 100.0  # unchanged by a later, different reading


def test_reference_balance_for_today_returns_none_without_a_real_reading():
    assert strat._reference_balance_for_today({}, None) is None  # noqa: SLF001


def test_record_milestone_persists_baseline_and_high_water_mark(monkeypatch, tmp_path):
    """Real gap found in review: the dashboard never showed progress toward
    a goal, just the current balance in isolation. record_milestone must
    persist its baseline/high-water-mark in the SAME durable state as
    positions/trade_log so it survives a restart, not just live in memory."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "alpaca_state.json")
    strat._save_state({"positions": [], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001

    snapshot = strat.record_milestone(100.0)
    assert snapshot["baseline_balance"] == 100.0
    assert snapshot["high_water_mark"] == 100.0

    snapshot = strat.record_milestone(150.0)
    assert snapshot["baseline_balance"] == 100.0  # never moves
    assert snapshot["high_water_mark"] == 150.0
    assert snapshot["total_return_pct"] == pytest.approx(0.5)

    state = strat._load_state()  # noqa: SLF001
    assert state["milestones"]["baseline_balance"] == 100.0
    assert state["milestones"]["high_water_mark"] == 150.0
