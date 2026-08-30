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


@pytest.fixture(autouse=True)
def _deterministic_fee_rate(monkeypatch):
    """round_trip_fee_usd() would otherwise hit the real GET /margin/fee_tiers
    endpoint on a cache miss -- every test here gets a fixed, pre-populated
    cache (falls back to the confirmed-live 0.008 taker rate per ticker) so
    fee deduction is deterministic and the suite never touches the network
    for this. Same for the maker side (0.05% confirmed-live rate)."""
    monkeypatch.setattr(strat, "_FEE_RATE_CACHE", {"rates": {}, "computed_at": strat.time.time()})
    monkeypatch.setattr(strat, "_MAKER_FEE_RATE_CACHE", {"rates": {}, "computed_at": strat.time.time()})


def test_taker_fee_rate_falls_back_to_default_on_a_missing_ticker(monkeypatch):
    monkeypatch.setattr(strat, "_FEE_RATE_CACHE", {"rates": {"KXBTCPERP": 0.002}, "computed_at": strat.time.time()})
    assert strat._taker_fee_rate("KXBTCPERP") == 0.002  # noqa: SLF001
    assert strat._taker_fee_rate("KXZECPERP") == strat.DEFAULT_TAKER_FEE_RATE  # noqa: SLF001


def test_taker_fee_rate_refreshes_from_the_live_endpoint_on_a_cold_cache(monkeypatch):
    monkeypatch.setattr(strat, "_FEE_RATE_CACHE", {"rates": None, "computed_at": 0.0})
    monkeypatch.setattr(strat, "get_margin_fee_tiers", lambda: {"taker_fee_rates": {"KXBTCPERP": 0.008}})
    assert strat._taker_fee_rate("KXBTCPERP") == 0.008  # noqa: SLF001


def test_taker_fee_rate_falls_back_to_default_when_the_live_call_fails(monkeypatch):
    monkeypatch.setattr(strat, "_FEE_RATE_CACHE", {"rates": None, "computed_at": 0.0})

    def fail():
        raise RuntimeError("network down")

    monkeypatch.setattr(strat, "get_margin_fee_tiers", fail)
    assert strat._taker_fee_rate("KXBTCPERP") == strat.DEFAULT_TAKER_FEE_RATE  # noqa: SLF001


def test_round_trip_fee_usd_charges_both_legs():
    # Confirmed live: a real NEAR trade (8 contracts, ~$1.80 entry) paid a fee
    # consistent with the confirmed-live 0.008 taker rate on EACH leg.
    fee = strat.round_trip_fee_usd("KXNEARPERP", entry_price=1.80, exit_price=1.7927, count=8.0)
    assert fee == round((1.80 + 1.7927) * 8.0 * strat.DEFAULT_TAKER_FEE_RATE, 6)


def _row(**overrides):
    # volatility_5/dollar_volume_z default comfortably above
    # MIN_ENTRY_VOLATILITY/MIN_ENTRY_VOLUME_Z so existing entry tests
    # reflect a normally-active market by default; tests specifically
    # targeting the low-volatility/low-volume gates override them.
    base = {
        "ticker": "KXBTCPERP", "current_price": 6.60, "short_ma": 6.63, "trend_pct": 0.0,
        "volatility_5": 0.002, "dollar_volume_z": 2.0,
    }
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


def _position(entry_price=6.60, minutes_ago=0, ticker="KXBTCPERP", count=1.0, side=None, entry_volatility_30=None):
    opened = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=minutes_ago)
    pos = {"ticker": ticker, "entry_price": entry_price, "count": count, "opened_at": opened.isoformat()}
    if side is not None:
        pos["side"] = side
    if entry_volatility_30 is not None:
        pos["entry_volatility_30"] = entry_volatility_30
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


# ── "Promising position" max_hold_time extension ────────────────────────────
# max_hold_time shouldn't be the ONLY factor forcing an exit -- see
# PROMISING_PROGRESS_FRACTION's own module-level comment for the full
# rationale and real backtest findings.

def test_promising_position_by_price_progress_gets_extended_past_max_hold():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    # +1% gain: well above PROMISING_PROGRESS_FRACTION (0.25) of the 2%
    # flat take_profit_pct.
    should_exit, reason = strat.decide_exit(pos, 6.60 * 1.01)
    assert not should_exit
    assert "holding" in reason


def test_promising_position_still_force_closed_once_extension_window_elapses():
    past_extension = strat.MAX_HOLD_MINUTES + strat.MAX_HOLD_EXTENSION_MINUTES + 1
    pos = _position(minutes_ago=past_extension)
    should_exit, reason = strat.decide_exit(pos, 6.60 * 1.01)
    assert should_exit and "max_hold_time" in reason


def test_non_promising_position_still_exits_exactly_at_max_hold_time():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    # +0.1%: below the promising price threshold, no volume/momentum/
    # breakout/sentiment signals passed -- must exit on schedule.
    should_exit, reason = strat.decide_exit(pos, 6.60 * 1.001)
    assert should_exit and "max_hold_time" in reason


def test_volume_and_momentum_confluence_extends_even_without_price_progress():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(
        pos, 6.60 * 1.001,  # flat price -- not promising on its own
        dollar_volume_z=2.0, momentum_pct=0.001,
    )
    assert not should_exit
    assert "holding" in reason


def test_breakout_signal_with_volume_extends_even_without_price_progress():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(
        pos, 6.60 * 1.001, dollar_volume_z=2.0, breakout_pct_b=0.95,
    )
    assert not should_exit
    assert "holding" in reason


def test_sentiment_signal_alone_extends_even_without_price_progress():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(pos, 6.60 * 1.001, sentiment_score=0.5)
    assert not should_exit
    assert "holding" in reason


def test_momentum_signal_without_confirming_volume_does_not_extend():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(
        pos, 6.60 * 1.001, dollar_volume_z=0.1, momentum_pct=0.001,
    )
    assert should_exit and "max_hold_time" in reason


def test_momentum_extension_requires_position_not_already_reversing():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    # A real adverse move (short of stop_loss) with otherwise-qualifying
    # volume/momentum must NOT be rescued by the momentum path -- that
    # path only extends a developing WINNER, not a reversing position.
    should_exit, reason = strat.decide_exit(
        pos, 6.60 * (1 - strat.STOP_LOSS_PCT * 0.5), dollar_volume_z=2.0, momentum_pct=0.001,
    )
    assert should_exit and "max_hold_time" in reason


def test_model_confidence_extends_past_max_hold_even_without_other_signals():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(
        pos, 6.60 * 1.001,  # flat -- not promising on price/momentum/breakout/sentiment
        model_ok=True, probability_up=strat.PROMISING_MODEL_CONFIDENCE + 0.01,
    )
    assert not should_exit
    assert "holding" in reason


def test_model_confidence_below_bar_does_not_extend():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(
        pos, 6.60 * 1.001, model_ok=True, probability_up=strat.PROMISING_MODEL_CONFIDENCE - 0.2,
    )
    assert should_exit and "max_hold_time" in reason


# ── Pre-exit study: a few minutes before max_hold, a flat/losing position
# whose model now confidently expects a reversal, with no volume confirming
# continuation, quits early instead of riding out a dead clock. See
# PRE_EXIT_STUDY_MINUTES's own module-level comment for the full rationale.

def test_pre_exit_study_quits_early_on_a_flat_position_the_model_now_expects_to_reverse():
    just_inside_window = strat.MAX_HOLD_MINUTES - strat.PRE_EXIT_STUDY_MINUTES + 1
    pos = _position(minutes_ago=just_inside_window)
    should_exit, reason = strat.decide_exit(
        pos, 6.60,  # flat, change_pct == 0
        dollar_volume_z=0.1,  # not confirming continuation
        model_ok=True, probability_up=1.0 - strat.PROMISING_MODEL_CONFIDENCE,
    )
    assert should_exit
    assert "pre_exit_study" in reason


def test_pre_exit_study_does_not_fire_before_its_window_opens():
    too_early = strat.MAX_HOLD_MINUTES - strat.PRE_EXIT_STUDY_MINUTES - 1
    pos = _position(minutes_ago=too_early)
    should_exit, reason = strat.decide_exit(
        pos, 6.60, dollar_volume_z=0.1,
        model_ok=True, probability_up=1.0 - strat.PROMISING_MODEL_CONFIDENCE,
    )
    assert not should_exit
    assert "holding" in reason


def test_pre_exit_study_does_not_quit_a_position_that_is_still_winning():
    just_inside_window = strat.MAX_HOLD_MINUTES - strat.PRE_EXIT_STUDY_MINUTES + 1
    pos = _position(minutes_ago=just_inside_window)
    should_exit, reason = strat.decide_exit(
        pos, 6.60 * 1.001,  # a small real gain, not flat/losing
        dollar_volume_z=0.1, model_ok=True, probability_up=1.0 - strat.PROMISING_MODEL_CONFIDENCE,
    )
    assert not should_exit
    assert "holding" in reason


def test_pre_exit_study_does_not_quit_when_volume_confirms_continuation():
    just_inside_window = strat.MAX_HOLD_MINUTES - strat.PRE_EXIT_STUDY_MINUTES + 1
    pos = _position(minutes_ago=just_inside_window)
    should_exit, reason = strat.decide_exit(
        pos, 6.60, dollar_volume_z=strat.PROMISING_VOLUME_Z,
        model_ok=True, probability_up=1.0 - strat.PROMISING_MODEL_CONFIDENCE,
    )
    assert not should_exit
    assert "holding" in reason


def test_pre_exit_study_does_not_quit_without_a_confidently_reversing_model():
    just_inside_window = strat.MAX_HOLD_MINUTES - strat.PRE_EXIT_STUDY_MINUTES + 1
    pos = _position(minutes_ago=just_inside_window)
    should_exit, reason = strat.decide_exit(
        pos, 6.60, dollar_volume_z=0.1, model_ok=True, probability_up=0.5,
    )
    assert not should_exit
    assert "holding" in reason


def test_pre_exit_study_is_a_no_op_without_a_model_prediction():
    """model_ok defaults to False -- every pre-existing caller/test that
    never passes model_ok/probability_up must behave exactly as before."""
    just_inside_window = strat.MAX_HOLD_MINUTES - strat.PRE_EXIT_STUDY_MINUTES + 1
    pos = _position(minutes_ago=just_inside_window)
    should_exit, reason = strat.decide_exit(pos, 6.60, dollar_volume_z=0.1)
    assert not should_exit
    assert "holding" in reason


def test_pre_exit_study_is_side_aware_for_shorts():
    """A short's favorable direction is a FALLING price -- 'model favors
    reversal' for a short means the model now expects price to RISE
    (probability_up high), the mirror image of the long case."""
    just_inside_window = strat.MAX_HOLD_MINUTES - strat.PRE_EXIT_STUDY_MINUTES + 1
    pos = _position(minutes_ago=just_inside_window, side="short")
    should_exit, reason = strat.decide_exit(
        pos, 6.60,  # flat
        dollar_volume_z=0.1, model_ok=True, probability_up=strat.PROMISING_MODEL_CONFIDENCE,
    )
    assert should_exit
    assert "pre_exit_study" in reason


# ── USE_TREND_TRAILING_STRATEGY: trailing-stop exit ─────────────────────────
# See the constant's own module-level comment for the full backtest/
# rationale. Deliberately its own simple exit shape (stop_loss + trailing
# + max_hold only) -- NOT combined with take_profit/quick_profit/
# pre_exit_study/promising-extension, which were tuned for the old
# fixed-take-profit shape and were never backtested together with this.

def test_trend_trailing_off_by_default_uses_the_old_fixed_take_profit(monkeypatch):
    """The flag defaults to False -- decide_exit must behave exactly as
    before (fixed 2% take-profit) when it's off, regardless of how far
    price has moved favorably."""
    pos = _position(minutes_ago=1)
    should_exit, reason = strat.decide_exit(pos, 6.60 * (1 + strat.TAKE_PROFIT_PCT + 0.001))
    assert should_exit and "take_profit" in reason


def test_trend_trailing_stop_loss_fires_before_any_trailing_activates(monkeypatch):
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    pos = _position(minutes_ago=1)
    should_exit, reason = strat.decide_exit(pos, 6.60 * (1 - strat.TRAILING_STOP_LOSS_PCT - 0.001))
    assert should_exit and "stop_loss" in reason


def test_trend_trailing_does_not_exit_a_small_favorable_move_before_activation(monkeypatch):
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    pos = _position(minutes_ago=1)
    should_exit, reason = strat.decide_exit(pos, 6.60 * (1 + strat.TRAILING_ACTIVATION_PCT * 0.5))
    assert not should_exit
    assert "holding" in reason


def test_trend_trailing_exits_on_a_real_retracement_from_the_peak(monkeypatch):
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    pos = _position(minutes_ago=1)
    entry_price = 6.60
    peak_price = entry_price * (1 + strat.TRAILING_ACTIVATION_PCT * 2)
    should_exit, reason = strat.decide_exit(pos, peak_price)  # first tick sets the peak, activates trailing
    assert not should_exit

    # retrace_pct = (peak - current) / entry_price -- an absolute move off
    # entry_price, not a percentage of peak_price itself.
    retraced_price = peak_price - entry_price * (strat.TRAILING_DISTANCE_PCT + 0.001)
    should_exit, reason = strat.decide_exit(pos, retraced_price)
    assert should_exit
    assert "trailing_stop" in reason


# ── WIDEN_TRAILING_WHEN_PROMISING (flagged, default OFF) ────────────────────

def test_widen_trailing_flag_off_by_default_preserves_existing_behavior(monkeypatch):
    """Default-OFF must be a pure no-op -- same posture as
    SKIP_QUICK_PROFIT_WHEN_PROMISING and USE_TREND_TRAILING_STRATEGY
    itself when they first shipped."""
    assert strat.WIDEN_TRAILING_WHEN_PROMISING is False
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    pos = _position(minutes_ago=1)
    entry_price = 6.60
    peak_price = entry_price * (1 + strat.TRAILING_ACTIVATION_PCT * 2)
    strat.decide_exit(pos, peak_price)  # activates trailing
    retraced_price = peak_price - entry_price * (strat.TRAILING_DISTANCE_PCT + 0.001)
    should_exit, reason = strat.decide_exit(
        pos, retraced_price, dollar_volume_z=strat.PROMISING_VOLUME_Z + 1.0, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_exit and "trailing_stop" in reason and "widened" not in reason


def test_widen_trailing_when_promising_holds_through_the_normal_distance(monkeypatch):
    """A retrace that would trigger the STANDARD trailing distance, with
    real volume-confirmed momentum continuation, must NOT exit -- it needs
    the WIDER distance instead."""
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    monkeypatch.setattr(strat, "WIDEN_TRAILING_WHEN_PROMISING", True)
    pos = _position(minutes_ago=1)
    entry_price = 6.60
    peak_price = entry_price * (1 + strat.TRAILING_ACTIVATION_PCT * 2)
    strat.decide_exit(pos, peak_price, dollar_volume_z=strat.PROMISING_VOLUME_Z + 1.0, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001)

    retraced_price = peak_price - entry_price * (strat.TRAILING_DISTANCE_PCT + 0.001)
    should_exit, reason = strat.decide_exit(
        pos, retraced_price, dollar_volume_z=strat.PROMISING_VOLUME_Z + 1.0, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert not should_exit
    assert "holding" in reason


def test_widen_trailing_when_promising_still_exits_once_the_wider_distance_is_hit(monkeypatch):
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    monkeypatch.setattr(strat, "WIDEN_TRAILING_WHEN_PROMISING", True)
    pos = _position(minutes_ago=1)
    entry_price = 6.60
    peak_price = entry_price * (1 + strat.TRAILING_ACTIVATION_PCT * 2)
    strat.decide_exit(pos, peak_price, dollar_volume_z=strat.PROMISING_VOLUME_Z + 1.0, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001)

    wider_distance = strat.TRAILING_DISTANCE_PCT * strat.TRAILING_DISTANCE_WIDEN_MULTIPLIER
    retraced_price = peak_price - entry_price * (wider_distance + 0.001)
    should_exit, reason = strat.decide_exit(
        pos, retraced_price, dollar_volume_z=strat.PROMISING_VOLUME_Z + 1.0, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_exit
    assert "trailing_stop widened" in reason


def test_widen_trailing_when_promising_does_not_widen_without_volume_confirmation(monkeypatch):
    """Momentum alone, with no real volume behind it, must not widen the
    stop -- matches the same volume-gating discipline the fixed-take-profit
    path's own promising check already applies."""
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    monkeypatch.setattr(strat, "WIDEN_TRAILING_WHEN_PROMISING", True)
    pos = _position(minutes_ago=1)
    entry_price = 6.60
    peak_price = entry_price * (1 + strat.TRAILING_ACTIVATION_PCT * 2)
    strat.decide_exit(pos, peak_price, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001)  # no dollar_volume_z

    retraced_price = peak_price - entry_price * (strat.TRAILING_DISTANCE_PCT + 0.001)
    should_exit, reason = strat.decide_exit(pos, retraced_price, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001)
    assert should_exit and "widened" not in reason


def test_widen_trailing_when_promising_via_breakout_signal_too(monkeypatch):
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    monkeypatch.setattr(strat, "WIDEN_TRAILING_WHEN_PROMISING", True)
    pos = _position(minutes_ago=1)
    entry_price = 6.60
    peak_price = entry_price * (1 + strat.TRAILING_ACTIVATION_PCT * 2)
    strat.decide_exit(pos, peak_price, dollar_volume_z=strat.PROMISING_VOLUME_Z + 1.0, breakout_pct_b=strat.PROMISING_BREAKOUT_PCT_B + 0.01)

    retraced_price = peak_price - entry_price * (strat.TRAILING_DISTANCE_PCT + 0.001)
    should_exit, reason = strat.decide_exit(
        pos, retraced_price, dollar_volume_z=strat.PROMISING_VOLUME_Z + 1.0, breakout_pct_b=strat.PROMISING_BREAKOUT_PCT_B + 0.01,
    )
    assert not should_exit


def test_widen_trailing_when_promising_never_touches_the_stop_loss_boundary(monkeypatch):
    """The hard risk boundary (stop_loss) must never be widened/skipped --
    only the discretionary retrace-based trailing trigger is affected."""
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    monkeypatch.setattr(strat, "WIDEN_TRAILING_WHEN_PROMISING", True)
    pos = _position(minutes_ago=1)
    price = 6.60 * (1 - strat.TRAILING_STOP_LOSS_PCT - 0.001)
    should_exit, reason = strat.decide_exit(
        pos, price, dollar_volume_z=strat.PROMISING_VOLUME_Z + 1.0, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_exit and "stop_loss" in reason


def test_widen_trailing_when_promising_is_side_aware_for_a_short(monkeypatch):
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    monkeypatch.setattr(strat, "WIDEN_TRAILING_WHEN_PROMISING", True)
    pos = _position(minutes_ago=1, side="short")
    entry_price = 6.60
    peak_price = entry_price * (1 - strat.TRAILING_ACTIVATION_PCT * 2)
    # Falling raw momentum is FAVORABLE (continuation) for a short.
    strat.decide_exit(pos, peak_price, dollar_volume_z=strat.PROMISING_VOLUME_Z + 1.0, momentum_pct=-(strat.PROMISING_MOMENTUM_PCT + 0.001))

    retraced_price = peak_price + entry_price * (strat.TRAILING_DISTANCE_PCT + 0.001)
    should_exit, reason = strat.decide_exit(
        pos, retraced_price, dollar_volume_z=strat.PROMISING_VOLUME_Z + 1.0, momentum_pct=-(strat.PROMISING_MOMENTUM_PCT + 0.001),
    )
    assert not should_exit


def test_trend_trailing_keeps_running_while_still_near_its_own_peak(monkeypatch):
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    pos = _position(minutes_ago=1)
    peak_price = 6.60 * (1 + strat.TRAILING_ACTIVATION_PCT * 2)
    strat.decide_exit(pos, peak_price)  # activates trailing

    barely_off_peak = peak_price * (1 - strat.TRAILING_DISTANCE_PCT * 0.1)
    should_exit, reason = strat.decide_exit(pos, barely_off_peak)
    assert not should_exit
    assert "holding" in reason


def test_trend_trailing_exits_on_max_hold_regardless_of_price(monkeypatch):
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    pos = _position(minutes_ago=strat.TRAILING_MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(pos, 6.60 * 1.001)
    assert should_exit and "max_hold_time" in reason


def test_trend_trailing_is_side_aware_for_a_short(monkeypatch):
    """A short's favorable direction is a FALLING price -- stop_loss/peak/
    retracement all mirror the long case."""
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    pos = _position(minutes_ago=1, side="short")
    should_exit, reason = strat.decide_exit(pos, 6.60 * (1 + strat.TRAILING_STOP_LOSS_PCT + 0.001))
    assert should_exit and "stop_loss" in reason

    pos2 = _position(minutes_ago=1, side="short")
    entry_price = 6.60
    peak_price = entry_price * (1 - strat.TRAILING_ACTIVATION_PCT * 2)
    strat.decide_exit(pos2, peak_price)  # activates trailing (price fell, favorable for a short)
    retraced_price = peak_price + entry_price * (strat.TRAILING_DISTANCE_PCT + 0.001)
    should_exit, reason = strat.decide_exit(pos2, retraced_price)
    assert should_exit and "trailing_stop" in reason


# ── Per-currency adaptive exit thresholds ───────────────────────────────────
# Confirmed via a per-ticker backtest breakdown: BTC's own volatility_30 is
# roughly 4x lower than kSHIB's -- these lock down that each position's
# actual take-profit/stop-loss is customized to THAT specific currency
# rather than one flat percentage for every coin.
def test_adaptive_exit_pcts_falls_back_to_flat_global_without_volatility():
    result = strat.adaptive_exit_pcts(None)
    assert result == {
        "take_profit_pct": strat.TAKE_PROFIT_PCT, "stop_loss_pct": strat.STOP_LOSS_PCT,
        "quick_profit_pct": strat.QUICK_PROFIT_PCT, "volatility_quick_profit_pct": strat.VOLATILITY_QUICK_PROFIT_PCT,
    }
    assert strat.adaptive_exit_pcts(0.0) == result
    assert strat.adaptive_exit_pcts(-0.001) == result


def test_adaptive_exit_pcts_scales_up_for_a_more_volatile_currency():
    calm = strat.adaptive_exit_pcts(0.0004)   # roughly BTC's own real mean volatility_30
    volatile = strat.adaptive_exit_pcts(0.0016)  # roughly kSHIB's -- ~4x higher
    assert volatile["take_profit_pct"] > calm["take_profit_pct"]
    assert volatile["stop_loss_pct"] > calm["stop_loss_pct"]


def test_adaptive_exit_pcts_respects_the_floor_for_a_near_zero_volatility_coin():
    result = strat.adaptive_exit_pcts(0.00001)  # would compute to well under the floor unscaled
    assert result["take_profit_pct"] == strat.MIN_TAKE_PROFIT_PCT
    assert result["stop_loss_pct"] == strat.MIN_STOP_LOSS_PCT


def test_adaptive_exit_pcts_respects_the_ceiling_for_an_extremely_volatile_coin():
    result = strat.adaptive_exit_pcts(1.0)  # absurdly high, must clamp rather than explode
    assert result["take_profit_pct"] == strat.MAX_TAKE_PROFIT_PCT
    assert result["stop_loss_pct"] == strat.MAX_STOP_LOSS_PCT


def test_adaptive_exit_pcts_quick_profit_levels_are_a_fraction_of_take_profit():
    result = strat.adaptive_exit_pcts(0.0008)
    assert result["quick_profit_pct"] == pytest.approx(result["take_profit_pct"] * 0.9)
    assert result["volatility_quick_profit_pct"] == pytest.approx(result["take_profit_pct"] * 0.8)
    assert result["quick_profit_pct"] < result["take_profit_pct"]
    assert result["volatility_quick_profit_pct"] < result["quick_profit_pct"]


def test_decide_exit_uses_a_wider_take_profit_for_a_more_volatile_currency():
    """The SAME percentage gain should NOT trigger take_profit for a highly
    volatile coin's position if it's still under THAT coin's own (wider)
    adaptive target, even though it would for a calm coin with a tighter one."""
    calm_pos = _position(entry_price=6.60, minutes_ago=1, entry_volatility_30=0.0004)
    volatile_pos = _position(entry_price=6.60, minutes_ago=1, entry_volatility_30=0.0016)
    calm_target = strat.adaptive_exit_pcts(0.0004)["take_profit_pct"]
    volatile_target = strat.adaptive_exit_pcts(0.0016)["take_profit_pct"]
    # A gain between the two targets: clears the calm coin's tighter target
    # but not the volatile coin's wider one.
    mid_gain_price = 6.60 * (1 + (calm_target + volatile_target) / 2)

    calm_exit, calm_reason = strat.decide_exit(calm_pos, mid_gain_price)
    volatile_exit, volatile_reason = strat.decide_exit(volatile_pos, mid_gain_price)
    assert calm_exit and "take_profit" in calm_reason
    assert not volatile_exit


def test_position_exit_levels_uses_the_adaptive_percentage_not_the_flat_global():
    pos = _position(entry_price=100.0, entry_volatility_30=0.0016)  # well above BTC-like calm
    levels = strat.position_exit_levels(pos)
    adaptive = strat.adaptive_exit_pcts(0.0016)
    assert levels["take_profit_price"] == round(100.0 * (1 + adaptive["take_profit_pct"]), 6)
    # Must differ from what the flat global would have produced, proving
    # the adaptive value (not TAKE_PROFIT_PCT) is what's actually used.
    assert adaptive["take_profit_pct"] != strat.TAKE_PROFIT_PCT
    assert levels["take_profit_price"] != round(100.0 * (1 + strat.TAKE_PROFIT_PCT), 6)


def test_position_exit_levels_falls_back_to_flat_global_without_stored_volatility():
    """An older position from before this field existed must keep working
    exactly as before, not error out or silently use a wrong value."""
    pos = _position(entry_price=100.0)  # no entry_volatility_30 at all
    levels = strat.position_exit_levels(pos)
    assert levels["take_profit_price"] == round(100.0 * (1 + strat.TAKE_PROFIT_PCT), 6)


def test_decide_exit_uses_real_wall_clock_time_by_default():
    """Live trading correctness: without an explicit `now`, max_hold_time
    must be judged against the REAL current time, matching production
    behavior exactly."""
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(pos, 6.601)
    assert should_exit and "max_hold_time" in reason


def test_decide_exit_respects_an_explicit_simulated_now():
    """A real bug this locks in: a backtest replays historical rows, often
    opened weeks/months before the real date the backtest happens to run
    on. Without an explicit `now` override, held_minutes would be computed
    against the REAL wall-clock time, making it enormous and forcing
    max_hold_time on virtually every simulated position's first tick after
    opening regardless of price movement -- confirmed live: 99.7% of
    simulated exits were max_hold_time before this fix."""
    long_ago = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
    pos = {
        "ticker": "KXBTCPERP", "entry_price": 6.60, "count": 1.0,
        "opened_at": long_ago.isoformat(),
    }
    # Simulated "now" is only 1 minute after the position opened --
    # nowhere near MAX_HOLD_MINUTES -- even though the REAL current time is
    # years later.
    sim_now = long_ago + dt.timedelta(minutes=1)
    should_exit, reason = strat.decide_exit(pos, 6.601, now=sim_now)
    assert not should_exit
    assert "holding" in reason

    # And it DOES fire once the SIMULATED clock passes MAX_HOLD_MINUTES,
    # not the real one.
    sim_now_later = long_ago + dt.timedelta(minutes=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(pos, 6.601, now=sim_now_later)
    assert should_exit and "max_hold_time" in reason


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


# ── SKIP_QUICK_PROFIT_WHEN_PROMISING (flagged, default OFF) ─────────────────

def test_skip_quick_profit_flag_off_by_default_preserves_existing_behavior(monkeypatch):
    """Default-OFF must be a pure no-op -- confirms this flagged feature
    can ship without changing anything for the live account until
    explicitly turned on, same posture as USE_TREND_TRAILING_STRATEGY."""
    assert strat.SKIP_QUICK_PROFIT_WHEN_PROMISING is False
    pos = _position()
    price = 6.60 * (1 + strat.QUICK_PROFIT_PCT + 0.0002)
    should_exit, reason = strat.decide_exit(
        pos, price, velocity_pct_per_min=strat.QUICK_PROFIT_VELOCITY_PCT_PER_MIN + 0.001,
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1.0, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_exit and "quick_profit" in reason


def test_skip_quick_profit_when_promising_holds_through_confirmed_continuation(monkeypatch):
    """Real, volume-confirmed momentum continuation -- with the flag on,
    quick_profit must NOT fire; the position keeps holding toward the
    bigger take-profit target instead."""
    monkeypatch.setattr(strat, "SKIP_QUICK_PROFIT_WHEN_PROMISING", True)
    pos = _position()
    price = 6.60 * (1 + strat.QUICK_PROFIT_PCT + 0.0002)
    should_exit, reason = strat.decide_exit(
        pos, price, velocity_pct_per_min=strat.QUICK_PROFIT_VELOCITY_PCT_PER_MIN + 0.001,
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1.0, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert not should_exit
    assert "holding" in reason


def test_skip_quick_profit_when_promising_still_fires_without_volume_confirmation(monkeypatch):
    """Momentum alone, with no real volume behind it, must not be enough
    to override the fast exit -- matches the same volume-gating discipline
    the max_hold "promising" check already applies."""
    monkeypatch.setattr(strat, "SKIP_QUICK_PROFIT_WHEN_PROMISING", True)
    pos = _position()
    price = 6.60 * (1 + strat.QUICK_PROFIT_PCT + 0.0002)
    should_exit, reason = strat.decide_exit(
        pos, price, velocity_pct_per_min=strat.QUICK_PROFIT_VELOCITY_PCT_PER_MIN + 0.001,
        dollar_volume_z=None, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_exit and "quick_profit" in reason


def test_skip_quick_profit_when_promising_via_breakout_signal_too(monkeypatch):
    monkeypatch.setattr(strat, "SKIP_QUICK_PROFIT_WHEN_PROMISING", True)
    pos = _position()
    price = 6.60 * (1 + strat.QUICK_PROFIT_PCT + 0.0002)
    should_exit, reason = strat.decide_exit(
        pos, price, velocity_pct_per_min=strat.QUICK_PROFIT_VELOCITY_PCT_PER_MIN + 0.001,
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1.0, breakout_pct_b=strat.PROMISING_BREAKOUT_PCT_B + 0.01,
    )
    assert not should_exit


def test_skip_quick_profit_when_promising_still_fires_without_any_continuation_signal(monkeypatch):
    """Flag on, but neither momentum nor breakout confirms continuation --
    quick_profit still fires normally."""
    monkeypatch.setattr(strat, "SKIP_QUICK_PROFIT_WHEN_PROMISING", True)
    pos = _position()
    price = 6.60 * (1 + strat.QUICK_PROFIT_PCT + 0.0002)
    should_exit, reason = strat.decide_exit(
        pos, price, velocity_pct_per_min=strat.QUICK_PROFIT_VELOCITY_PCT_PER_MIN + 0.001,
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1.0,
    )
    assert should_exit and "quick_profit" in reason


def test_skip_quick_profit_when_promising_applies_to_shorts_too(monkeypatch):
    """A short position's FAVORABLE momentum/breakout are sign-flipped from
    the raw values (same convention every other side-aware signal here
    already uses) -- confirms the skip logic respects that, not just the
    long-only case."""
    monkeypatch.setattr(strat, "SKIP_QUICK_PROFIT_WHEN_PROMISING", True)
    pos = _position(side="short")
    price = 6.60 * (1 - strat.QUICK_PROFIT_PCT - 0.0002)
    # Falling raw momentum is FAVORABLE (continuation) for a short.
    should_exit, reason = strat.decide_exit(
        pos, price, velocity_pct_per_min=-(strat.QUICK_PROFIT_VELOCITY_PCT_PER_MIN + 0.001),
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1.0, momentum_pct=-(strat.PROMISING_MOMENTUM_PCT + 0.001),
    )
    assert not should_exit


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
    base = {
        "ticker": "KXBTCPERP", "current_price": 6.63, "short_ma": 6.60, "trend_pct": 0.0,
        "volatility_5": 0.002, "dollar_volume_z": 2.0,
    }
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


def test_position_exit_levels_for_a_long():
    pos = _position(entry_price=100.0)
    levels = strat.position_exit_levels(pos)
    assert levels["take_profit_price"] == round(100.0 * (1 + strat.TAKE_PROFIT_PCT), 6)
    assert levels["stop_loss_price"] == round(100.0 * (1 - strat.STOP_LOSS_PCT), 6)
    assert levels["quick_profit_price"] == round(100.0 * (1 + strat.QUICK_PROFIT_PCT), 6)
    # Take-profit above entry, stop-loss below -- as it must be for a long.
    assert levels["take_profit_price"] > 100.0 > levels["stop_loss_price"]


def test_position_exit_levels_for_a_short_are_mirrored():
    pos = _position(entry_price=100.0, side="short")
    levels = strat.position_exit_levels(pos)
    assert levels["take_profit_price"] == round(100.0 * (1 - strat.TAKE_PROFIT_PCT), 6)
    assert levels["stop_loss_price"] == round(100.0 * (1 + strat.STOP_LOSS_PCT), 6)
    # Take-profit BELOW entry, stop-loss ABOVE -- the mirror image of a long.
    assert levels["take_profit_price"] < 100.0 < levels["stop_loss_price"]


def test_position_exit_levels_uses_the_trailing_activation_price_in_trailing_mode(monkeypatch):
    """Real bug found and fixed: this function used to keep returning the
    OLD fixed-take-profit numbers even after decide_exit() switched to the
    trailing path, so the dashboard/Threads posts showed levels that no
    longer matched what would actually trigger an exit."""
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    pos = _position(entry_price=100.0)
    levels = strat.position_exit_levels(pos)
    assert levels["take_profit_price"] == round(100.0 * (1 + strat.TRAILING_ACTIVATION_PCT), 6)
    assert levels["stop_loss_price"] == round(100.0 * (1 - strat.TRAILING_STOP_LOSS_PCT), 6)
    # No None/crash risk for a caller that formats this with :.4f unconditionally.
    assert levels["quick_profit_price"] == levels["take_profit_price"]


def test_position_exit_levels_trailing_mode_is_mirrored_for_a_short(monkeypatch):
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    pos = _position(entry_price=100.0, side="short")
    levels = strat.position_exit_levels(pos)
    assert levels["take_profit_price"] == round(100.0 * (1 - strat.TRAILING_ACTIVATION_PCT), 6)
    assert levels["stop_loss_price"] == round(100.0 * (1 + strat.TRAILING_STOP_LOSS_PCT), 6)
    assert levels["take_profit_price"] < 100.0 < levels["stop_loss_price"]


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


# ── Volatility-aware quick profit ────────────────────────────────────────────

def test_sample_volatility_needs_at_least_three_samples():
    assert strat._sample_volatility([[0, 100.0], [1, 100.1]]) is None  # noqa: SLF001


def test_sample_volatility_is_higher_for_choppier_samples():
    calm = strat._sample_volatility([[0, 100.0], [1, 100.01], [2, 100.02], [3, 100.03]])  # noqa: SLF001
    choppy = strat._sample_volatility([[0, 100.0], [1, 102.0], [2, 99.0], [3, 103.0]])  # noqa: SLF001
    assert calm is not None and choppy is not None
    assert choppy > calm


def test_high_volatility_triggers_profit_at_a_smaller_gain_than_normal():
    pos = _position()
    # Below the standard TAKE_PROFIT_PCT and QUICK_PROFIT_PCT, but at/above
    # the volatility-specific (smaller) threshold.
    price = 6.60 * (1 + strat.VOLATILITY_QUICK_PROFIT_PCT + 0.0001)
    assert price < 6.60 * (1 + strat.QUICK_PROFIT_PCT)

    should_exit, reason = strat.decide_exit(pos, price, current_volatility=0.0)
    assert not should_exit  # calm market -- not enough gain yet for the normal path

    should_exit, reason = strat.decide_exit(pos, price, current_volatility=strat.HIGH_VOLATILITY_THRESHOLD + 0.001)
    assert should_exit and "volatility_quick_profit" in reason


def test_high_volatility_alone_does_not_trigger_without_any_gain():
    pos = _position()
    should_exit, reason = strat.decide_exit(pos, 6.60, current_volatility=strat.HIGH_VOLATILITY_THRESHOLD + 0.01)
    assert not should_exit


def test_volatility_quick_profit_applies_symmetrically_to_shorts():
    pos = _position(side="short")
    price = 6.60 * (1 - strat.VOLATILITY_QUICK_PROFIT_PCT - 0.0001)  # price fell -- a gain for a short
    should_exit, reason = strat.decide_exit(pos, price, current_volatility=strat.HIGH_VOLATILITY_THRESHOLD + 0.001)
    assert should_exit and "volatility_quick_profit" in reason


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
    # Price fell 5% -- profitable for a short and comfortably clears the
    # real ~1.6% round-trip taker fee, so it should trigger take-profit AND
    # remain a net gain (not just a gross one).
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=50.0 * 0.95))

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


def test_manage_open_positions_deducts_the_real_round_trip_fee_from_realized_pnl(monkeypatch, tmp_path):
    """realized_pnl_usd must be NET (what the real Kalshi account balance
    actually reflects), not the gross price-delta alone -- confirmed live
    that gross-only tracking systematically overstated performance by the
    real taker round-trip fee (a real NEAR trade: +$0.0608 gross booked as
    -$0.1701 net)."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    strat._save_state({
        "positions": [_position(ticker="KXETHPERP", entry_price=50.0, count=10.0, side="long")],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    calls = {"n": 0}

    def fake_positions():
        calls["n"] += 1
        count = "10.00" if calls["n"] == 1 else "0.00"
        return {"positions": [_real_position("KXETHPERP", count, "50.0000")] if float(count) > 0 else []}

    monkeypatch.setattr(strat, "get_margin_positions", fake_positions)
    exit_price = 50.0 * 1.05  # +5% -- a real gain for a long
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=exit_price))
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: {"order": {"fill_count": str(kwargs["count"])}})

    result = strat.manage_open_positions(dry_run=False)
    trade = result["closed"][0]
    expected_fee = strat.round_trip_fee_usd("KXETHPERP", 50.0, trade["exit_price"], 10.0)
    assert trade["fee_usd"] == expected_fee
    assert trade["gross_pnl_usd"] > 0  # price rose -- a real gross gain for a long
    assert trade["realized_pnl_usd"] == round(trade["gross_pnl_usd"] - trade["fee_usd"], 6)


def test_manage_open_positions_dry_run_trades_pay_no_fee(monkeypatch, tmp_path):
    """A dry-run trade never touches the real exchange, so it must not book
    a fee that was never actually charged."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    strat._save_state({
        "positions": [_position(ticker="KXETHPERP", entry_price=50.0, count=10.0, side="long")],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=50.0 * 1.05))

    result = strat.manage_open_positions(dry_run=True)
    assert result["closed"][0]["fee_usd"] == 0.0


def test_manage_open_positions_posts_a_threads_exit_on_close(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    strat._save_state({
        "positions": [_position(ticker="KXETHPERP", entry_price=50.0, count=10.0, side="long")],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=50.0 * 1.05))

    posted = {}
    monkeypatch.setattr(strat.threads_post, "post_trade_exit", lambda **kw: posted.update(kw) or True)

    result = strat.manage_open_positions(dry_run=True)
    assert result["action"] == "closed"
    assert posted["ticker"] == "KXETHPERP"
    assert posted["market"] == "perps"


def test_manage_open_positions_still_closes_the_position_even_if_threads_post_raises(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    strat._save_state({
        "positions": [_position(ticker="KXETHPERP", entry_price=50.0, count=10.0, side="long")],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=50.0 * 1.05))

    def raise_error(**kwargs):
        raise RuntimeError("simulated Threads API outage")

    monkeypatch.setattr(strat.threads_post, "post_trade_exit", raise_error)
    result = strat.manage_open_positions(dry_run=True)
    assert result["action"] == "closed"
    assert len(result["closed"]) == 1


def _one_min_df(n=30, base_ts=None):
    import pandas as pd
    base_ts = base_ts or int(dt.datetime.now(dt.timezone.utc).timestamp()) - n * 60
    rows = []
    price = 50.0
    for i in range(n):
        o = price
        price += 0.05
        rows.append({"ts": base_ts + i * 60, "open": o, "high": max(o, price) + 0.02, "low": min(o, price) - 0.02, "close": price})
    return pd.DataFrame(rows)


def test_candles_as_dicts_converts_a_dataframe_to_plain_dicts():
    df = _one_min_df(5)
    dicts = strat._candles_as_dicts(df)  # noqa: SLF001
    assert len(dicts) == 5
    assert set(dicts[0].keys()) == {"ts", "open", "high", "low", "close"}


def test_candles_as_dicts_handles_an_empty_dataframe():
    import pandas as pd
    assert strat._candles_as_dicts(pd.DataFrame()) == []  # noqa: SLF001


def test_index_for_ts_finds_the_closest_candle():
    df = _one_min_df(10, base_ts=1000)
    idx = strat._index_for_ts(df, dt.datetime.fromtimestamp(1000 + 3 * 60, dt.timezone.utc).isoformat())  # noqa: SLF001
    assert idx == 3


def test_index_for_ts_returns_none_when_target_is_far_outside_the_window():
    df = _one_min_df(10, base_ts=1_000_000)
    idx = strat._index_for_ts(df, dt.datetime.fromtimestamp(1, dt.timezone.utc).isoformat())  # noqa: SLF001
    assert idx is None


def test_index_for_ts_returns_none_without_a_timestamp():
    df = _one_min_df(10)
    assert strat._index_for_ts(df, None) is None  # noqa: SLF001


def test_manage_open_positions_posts_a_candlestick_exit_chart_on_close(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    strat._save_state({
        "positions": [_position(ticker="KXETHPERP", entry_price=50.0, count=10.0, side="long")],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=50.0 * 1.05))
    monkeypatch.setattr(strat, "fetch_candle_frames", lambda ticker: (_one_min_df(), _one_min_df()))

    posted = {}
    monkeypatch.setattr(strat.threads_post, "post_trade_exit_chart", lambda **kw: posted.update(kw) or True)

    result = strat.manage_open_positions(dry_run=True)
    assert result["action"] == "closed"
    assert posted["ticker"] == "KXETHPERP"
    assert posted["market"] == "perps"
    assert len(posted["candles"]) == 30
    assert posted["pnl_usd"] == result["closed"][0]["realized_pnl_usd"]


def test_manage_open_positions_still_closes_if_exit_chart_post_raises(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    strat._save_state({
        "positions": [_position(ticker="KXETHPERP", entry_price=50.0, count=10.0, side="long")],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=50.0 * 1.05))
    monkeypatch.setattr(strat, "fetch_candle_frames", lambda ticker: (_one_min_df(), _one_min_df()))

    def raise_error(**kwargs):
        raise RuntimeError("simulated Threads API outage")

    monkeypatch.setattr(strat.threads_post, "post_trade_exit_chart", raise_error)
    result = strat.manage_open_positions(dry_run=True)
    assert result["action"] == "closed"


def test_maybe_run_batch_trade_analysis_skips_below_batch_size(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    strat._save_state({
        "positions": [], "realized_pnl_by_date": {}, "daily_reference_balance": {},
        "trade_log": [{"ticker": "KXBTCPERP", "realized_pnl_usd": 1.0, "dry_run": False}] * 3,
    })
    called = {"n": 0}
    monkeypatch.setattr(strat.threads_post, "post_trade_analysis_summary", lambda *a, **kw: called.update(n=called["n"] + 1))
    strat._maybe_run_batch_trade_analysis()  # noqa: SLF001
    assert called["n"] == 0


def test_maybe_run_batch_trade_analysis_runs_at_the_batch_boundary_and_posts(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    trades = [
        {
            "ticker": "KXBTCPERP", "side": "long", "realized_pnl_usd": 1.0, "dry_run": False,
            "reason": "take_profit (+2%)", "entry_price": 50.0, "exit_price": 51.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "closed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        for _ in range(5)
    ]
    strat._save_state({
        "positions": [], "realized_pnl_by_date": {}, "daily_reference_balance": {}, "trade_log": trades,
    })
    monkeypatch.setattr(strat, "fetch_candle_frames", lambda ticker: (_one_min_df(), _one_min_df()))
    posted = {}
    monkeypatch.setattr(strat.threads_post, "post_trade_analysis_summary", lambda text, **kw: posted.update(text=text, **kw) or True)

    strat._maybe_run_batch_trade_analysis()  # noqa: SLF001
    assert posted["market"] == "perps"
    assert "5" in posted["text"]  # trades_analyzed count shows up in the digest
    state = strat._load_state()
    assert state["last_batch_analysis_trade_count"] == 5


def test_maybe_run_batch_trade_analysis_does_not_rerun_for_the_same_trades(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    trades = [{"ticker": "KXBTCPERP", "realized_pnl_usd": 1.0, "dry_run": False} for _ in range(5)]
    strat._save_state({
        "positions": [], "realized_pnl_by_date": {}, "daily_reference_balance": {},
        "trade_log": trades, "last_batch_analysis_trade_count": 5,
    })
    called = {"n": 0}
    monkeypatch.setattr(strat.threads_post, "post_trade_analysis_summary", lambda *a, **kw: called.update(n=called["n"] + 1))
    strat._maybe_run_batch_trade_analysis()  # noqa: SLF001
    assert called["n"] == 0


def test_maybe_run_batch_trade_analysis_starts_a_position_management_trial(monkeypatch, tmp_path):
    """Once enough real trade history exists with a feature never having
    been on, the batch review should start a live trial for it -- same
    evidence-gated wiring as the confidence-threshold/correlation-study
    tunes right above it in this same function."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    trades = [
        {
            "ticker": "KXBTCPERP", "side": "long", "realized_pnl_usd": 0.1, "dry_run": False,
            "reason": "take_profit (+2%)", "entry_price": 50.0, "exit_price": 51.0,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "closed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "entry_partial_exit_enabled": False, "entry_scale_in_enabled": False, "entry_conviction_sizing_enabled": False,
        }
        for _ in range(20)
    ]
    strat._save_state({
        "positions": [], "realized_pnl_by_date": {}, "daily_reference_balance": {}, "trade_log": trades,
    })
    monkeypatch.setattr(strat, "fetch_candle_frames", lambda ticker: (_one_min_df(), _one_min_df()))
    monkeypatch.setattr(strat.threads_post, "post_trade_analysis_summary", lambda *a, **kw: True)

    strat._maybe_run_batch_trade_analysis()  # noqa: SLF001

    state = strat._load_state()
    assert state["tuning"]["partial_exit_enabled"] is True
    assert state["tuning"]["reason"] == "5-trade batch review (start_trial)"


def test_manage_open_positions_triggers_batch_analysis_on_close(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    existing_trades = [
        {"ticker": "KXBTCPERP", "realized_pnl_usd": 1.0, "dry_run": False} for _ in range(4)
    ]
    strat._save_state({
        "positions": [_position(ticker="KXETHPERP", entry_price=50.0, count=10.0, side="long")],
        "realized_pnl_by_date": {}, "daily_reference_balance": {}, "trade_log": existing_trades,
    })
    calls = {"n": 0}

    def fake_positions():
        calls["n"] += 1
        count = "10.00" if calls["n"] == 1 else "0.00"
        return {"positions": [_real_position("KXETHPERP", count, "50.0000")] if float(count) > 0 else []}

    monkeypatch.setattr(strat, "get_margin_positions", fake_positions)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=50.0 * 1.05))
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: {"order": {"fill_count": str(kwargs["count"])}})
    monkeypatch.setattr(strat, "fetch_candle_frames", lambda ticker: (_one_min_df(), _one_min_df()))

    called = {"n": 0}
    monkeypatch.setattr(strat.threads_post, "post_trade_analysis_summary", lambda *a, **kw: called.update(n=called["n"] + 1))

    result = strat.manage_open_positions(dry_run=False)
    assert result["action"] == "closed"
    # 4 pre-existing real trades + 1 just closed real trade == 5 -> crosses the batch boundary.
    assert called["n"] == 1


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
        lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: (
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


def test_evaluate_candidate_blocks_entry_when_volatility_too_low(monkeypatch):
    """Confirmed live: a real share of exits were max_hold_time timeouts in
    a market that just wasn't moving. A dip signal + confident model in an
    otherwise-calm market must not enter -- there's no reason to expect a
    fast exit there."""
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(volatility_5=strat.MIN_ENTRY_VOLATILITY - 0.0001))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.9,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is False
    assert "volatility" in result["reason"]


def test_evaluate_candidate_enters_when_volatility_meets_the_bar(monkeypatch):
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(volatility_5=strat.MIN_ENTRY_VOLATILITY + 0.0001))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.9,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True


def test_evaluate_candidate_blocks_entry_when_calm_relative_to_own_baseline(monkeypatch):
    """"Study each currency": a fixed absolute volatility floor alone can't
    tell "quiet for BTC" from "quiet for a naturally choppy small-cap".
    Here volatility_5 clears the absolute floor but is well below this
    coin's OWN recent (30-min) baseline -- must not enter."""
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(
        volatility_5=strat.MIN_ENTRY_VOLATILITY + 0.0001, volatility_30=(strat.MIN_ENTRY_VOLATILITY + 0.0001) * 5,
    ))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.9,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is False
    assert "baseline" in result["reason"]


def test_evaluate_candidate_enters_when_active_relative_to_own_baseline(monkeypatch):
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(
        volatility_5=strat.MIN_ENTRY_VOLATILITY + 0.0001, volatility_30=(strat.MIN_ENTRY_VOLATILITY + 0.0001) * 0.5,
    ))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.9,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True


def test_evaluate_candidate_skips_relative_check_without_a_baseline(monkeypatch):
    """No volatility_30 available (e.g. not enough history yet) -- must not
    block on the relative check, only the absolute floor still applies."""
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(
        volatility_5=strat.MIN_ENTRY_VOLATILITY + 0.0001, volatility_30=0.0,
    ))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.9,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True


def test_evaluate_candidate_falls_back_to_technical_when_model_not_trained(monkeypatch):
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False, "ticker": ticker})
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True
    assert result["model_ok"] is False
    assert "fallback" in result["reason"]


# ── USE_TREND_TRAILING_STRATEGY: 4h trend alignment on entry ────────────────
# See the constant's own module-level comment for the full backtest/rationale.

def test_trend_trailing_strategy_off_by_default_ignores_trend_4h(monkeypatch):
    """The flag defaults to False -- an unfavorable trend_4h must NOT block
    entry unless the strategy variant is explicitly turned on."""
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(trend_4h=-0.05))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.9,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True


def test_trend_trailing_strategy_blocks_a_long_dip_against_the_4h_trend(monkeypatch):
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(trend_4h=-0.02))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.9,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is False
    assert "4h trend" in result["reason"]


def test_trend_trailing_strategy_allows_a_long_dip_with_the_4h_trend(monkeypatch):
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(trend_4h=0.02))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.9,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True


def test_trend_trailing_strategy_blocks_entry_with_no_4h_trend_data(monkeypatch):
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(trend_4h=None))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.9,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is False
    assert "no 4h trend data" in result["reason"]


def test_trend_trailing_strategy_blocks_a_short_rally_against_the_4h_trend(monkeypatch):
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    monkeypatch.setattr(strat, "ENABLE_SHORTS", True)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _rally_row(trend_4h=0.02))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "down", "probability_up": 0.1,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is False
    assert "4h trend" in result["reason"]


def test_trend_trailing_strategy_allows_a_short_rally_with_the_4h_trend(monkeypatch):
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    monkeypatch.setattr(strat, "ENABLE_SHORTS", True)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _rally_row(trend_4h=-0.02))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "down", "probability_up": 0.1,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True


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


def test_scan_for_entries_prewarms_sentiment_for_tickers_not_already_held(monkeypatch):
    """Real, confirmed root cause this fixes on the crypto (Alpaca) side of
    this exact shared sentiment module -- see crypto_news.prewarm_sentiment's
    own docstring. Ported here for the same reason/consistency."""
    monkeypatch.setattr(strat, "get_watchlist", lambda: ["KXBTCPERP", "KXETHPERP"])
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(ticker=ticker))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False})
    prewarmed_with = []
    monkeypatch.setattr(strat, "prewarm_sentiment", lambda coins, **kw: prewarmed_with.extend(coins))

    strat.scan_for_entries(exclude={"KXBTCPERP"})
    assert prewarmed_with == [strat.coin_for_ticker("KXETHPERP")]


def test_scan_for_entries_still_works_if_sentiment_prewarm_fails(monkeypatch):
    """Best-effort optimization only -- a failure here must never block
    entry evaluation."""
    monkeypatch.setattr(strat, "get_watchlist", lambda: ["KXBTCPERP"])
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(ticker=ticker))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False})

    def raise_error(coins, **kw):
        raise RuntimeError("simulated prewarm failure")

    monkeypatch.setattr(strat, "prewarm_sentiment", raise_error)
    qualifying, candidates = strat.scan_for_entries()
    assert len(candidates) == 1


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

def _market_response(price=6.60, tick_size=0.0001, leverage_estimate=6.0, contract_size=0.0001, bid=None, ask=None):
    market = {
        "price": price, "tick_size": tick_size, "leverage_estimate": leverage_estimate, "contract_size": contract_size,
    }
    if bid is not None:
        market["bid"] = bid
    if ask is not None:
        market["ask"] = ask
    return {"market": market}


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


def test_manage_open_positions_one_malformed_position_does_not_block_the_others(monkeypatch, tmp_path):
    """A position dict this code doesn't fully recognize -- e.g. adopted
    from the real account, or left over from an older deployed version with
    a different schema -- must never take down monitoring for every OTHER
    healthy open position in the same cycle. It should be logged, left
    untouched for retry, and every other position still gets its exit
    check."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)

    def fake_market(ticker):
        if ticker == "KXBTCPERP":
            return _market_response(price=6.60 * (1 + strat.TAKE_PROFIT_PCT + 0.001))
        return _market_response(price=100.001)

    monkeypatch.setattr(strat, "get_margin_market", fake_market)
    malformed = {"ticker": "KXWEIRDPERP"}  # missing entry_price/count/opened_at entirely
    strat._save_state({
        "positions": [
            malformed,
            _position(ticker="KXBTCPERP", entry_price=6.60),
            _position(ticker="KXETHPERP", entry_price=100.0),
        ],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })

    result = strat.manage_open_positions()
    assert result["ok"] is False  # flagged, but did not raise/crash
    assert result["action"] == "closed"
    assert result["closed"][0]["ticker"] == "KXBTCPERP"  # the healthy position still got closed

    state = strat._load_state()  # noqa: SLF001
    remaining_tickers = [p["ticker"] for p in state["positions"]]
    assert set(remaining_tickers) == {"KXWEIRDPERP", "KXETHPERP"}  # malformed one retained as-is for retry


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
        lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: (
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
        lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: ([{"ticker": "KXBTCPERP", "current_price": 6.60, "reason": "test dip", "score": 0.9}], []),
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
        lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: ([{"ticker": "KXBTCPERP", "current_price": 6.60, "reason": "test dip", "score": 0.9}], []),
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
        lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: ([], []),
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


def test_real_open_positions_by_ticker_ignores_only_zero_count_rows(monkeypatch):
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [
        _real_position("KXBCHPERP", "0.00", "0.0000", is_portfolio=False),
        _real_position("KXSOLPERP", "4.00", "7.7572"),
        _real_position("KXNEARPERP", "0.00", "0.0000"),
    ]})
    result = strat._real_open_positions_by_ticker()  # noqa: SLF001
    assert result == {"KXSOLPERP": {"count": 4.0, "entry_price": 7.7572, "side": "long"}}


def test_real_open_positions_by_ticker_includes_non_portfolio_nonzero_rows(monkeypatch):
    """Real bug found live: `is_portfolio` distinguishes portfolio- vs
    isolated-margined positions, NOT "real vs not real" -- a real, non-zero,
    real-money HYPE position with is_portfolio=False was silently excluded
    by an earlier version of this filter, left with zero stop-loss/
    take-profit coverage. Must be included regardless of is_portfolio."""
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [
        _real_position("KXHYPEPERP", "8.00", "5.8773", is_portfolio=False),
    ]})
    result = strat._real_open_positions_by_ticker()  # noqa: SLF001
    assert result == {"KXHYPEPERP": {"count": 8.0, "entry_price": 5.8773, "side": "long"}}


def test_real_open_positions_by_ticker_aggregates_multiple_rows_for_the_same_ticker(monkeypatch):
    """Kalshi can return multiple rows for the same ticker across margin
    modes/subaccounts -- must sum signed counts and weight-average the
    entry price rather than letting one row silently overwrite another."""
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [
        _real_position("KXHYPEPERP", "8.00", "5.00", is_portfolio=False),
        _real_position("KXHYPEPERP", "2.00", "8.00", is_portfolio=True),
    ]})
    result = strat._real_open_positions_by_ticker()  # noqa: SLF001
    assert result["KXHYPEPERP"]["count"] == 10.0
    # Weighted average: (8*5.00 + 2*8.00) / 10 = 5.60
    assert result["KXHYPEPERP"]["entry_price"] == pytest.approx(5.60)
    assert result["KXHYPEPERP"]["side"] == "long"


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
        lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: (
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
        lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: (
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


def test_scan_and_enter_posts_to_threads_with_the_real_entry_and_exit_levels(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=2.0, leverage_estimate=6.0))
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 10.0)
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: (
            [{"ticker": "KXSOLPERP", "current_price": 2.0, "reason": "test dip", "volatility_30": 0.0006}], [],
        ),
    )
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: {"order": {"fill_count": "4.00"}})
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [_real_position("KXSOLPERP", "4.00", "1.9998")]})

    posted = {}
    monkeypatch.setattr(
        strat.threads_post, "post_trade_entry",
        lambda **kwargs: posted.update(kwargs) or True,
    )

    result = strat.scan_and_enter(dry_run=False)
    assert result["opened"][0]["action"] == "opened"
    assert posted["ticker"] == "KXSOLPERP"
    assert posted["entry_price"] == 1.9998
    assert posted["reason"] == "test dip"
    assert posted["dry_run"] is False
    assert posted["take_profit_price"] > posted["entry_price"]
    assert posted["stop_loss_price"] < posted["entry_price"]


def test_scan_and_enter_still_opens_the_real_position_even_if_threads_post_raises(monkeypatch, tmp_path):
    """The entry order is real money -- a Threads failure must never be
    allowed to affect it, since post_trade_entry() is called AFTER the
    position is already saved to state."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=2.0, leverage_estimate=6.0))
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 10.0)
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: (
            [{"ticker": "KXSOLPERP", "current_price": 2.0, "reason": "test dip"}], [],
        ),
    )
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: {"order": {"fill_count": "4.00"}})
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [_real_position("KXSOLPERP", "4.00", "1.9998")]})

    def raise_error(**kwargs):
        raise RuntimeError("simulated Threads API outage")

    monkeypatch.setattr(strat.threads_post, "post_trade_entry", raise_error)

    result = strat.scan_and_enter(dry_run=False)
    assert result["opened"][0]["action"] == "opened"
    state = strat._load_state()  # noqa: SLF001
    assert state["positions"][0]["count"] == 4.0


def test_scan_and_enter_posts_a_candlestick_entry_chart(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=2.0, leverage_estimate=6.0))
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 10.0)
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: (
            [{"ticker": "KXSOLPERP", "current_price": 2.0, "reason": "test dip", "volatility_30": 0.0006}], [],
        ),
    )
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: {"order": {"fill_count": "4.00"}})
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [_real_position("KXSOLPERP", "4.00", "1.9998")]})
    monkeypatch.setattr(strat, "fetch_candle_frames", lambda ticker: (_one_min_df(base_ts=1000), _one_min_df()))

    posted = {}
    monkeypatch.setattr(strat.threads_post, "post_trade_entry_chart", lambda **kwargs: posted.update(kwargs) or True)

    result = strat.scan_and_enter(dry_run=False)
    assert result["opened"][0]["action"] == "opened"
    assert posted["ticker"] == "KXSOLPERP"
    assert posted["market"] == "perps"
    assert len(posted["candles"]) == 30
    assert posted["entry_index"] == 29  # most recent candle -- the fill just happened


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
        lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: (
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


def test_scan_and_enter_one_failed_entry_does_not_block_the_others(monkeypatch, tmp_path):
    """An unexpected failure placing/booking one candidate's real entry
    order (network error, API error, anything) must not abort scanning for
    every OTHER qualifying candidate in the same cycle."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response())
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 10.0)
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: (
            [
                {"ticker": "KXBTCPERP", "current_price": 6.60, "reason": "test dip", "score": 0.95},
                {"ticker": "KXETHPERP", "current_price": 6.60, "reason": "test dip", "score": 0.9},
            ],
            [],
        ),
    )

    def flaky_create_order(**kwargs):
        if kwargs["ticker"] == "KXBTCPERP":
            raise RuntimeError("network blew up placing this one order")
        return {"order": {"fill_count": str(kwargs["count"])}}

    monkeypatch.setattr(strat, "create_margin_order", flaky_create_order)
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [
        _real_position("KXETHPERP", "6.00", "6.60"),
    ]})

    result = strat.scan_and_enter(dry_run=False)
    by_ticker = {o["ticker"]: o for o in result["opened"]}
    assert by_ticker["KXBTCPERP"]["action"] == "entry_failed"
    assert by_ticker["KXETHPERP"]["action"] == "opened"  # the healthy candidate still got entered
    state = strat._load_state()  # noqa: SLF001
    assert [p["ticker"] for p in state["positions"]] == ["KXETHPERP"]


# ── Durable state survives Render's ephemeral (no persistent disk) restarts ──
# Confirmed: this app's free-tier Render plan has no attached disk, so any
# restart boots from a fresh filesystem. Open positions recover fine via
# exchange reconciliation, but trade_log/realized_pnl_by_date/
# daily_reference_balance have no such ground truth -- without a backup, a
# restart could silently reset the daily loss cap's reference point and
# forget a loss already taken earlier the same day.

def _durable_state(trade_log=None, realized_pnl_by_date=None, daily_reference_balance=None):
    return {
        "positions": [], "trade_log": trade_log or [],
        "realized_pnl_by_date": realized_pnl_by_date or {}, "daily_reference_balance": daily_reference_balance or {},
    }


def test_push_durable_state_to_hf_is_bounded_by_a_hard_timeout(monkeypatch):
    """Real, confirmed production incident: this upload used to be a plain,
    unbounded call -- always made while _STATE_LOCK is held (every
    push_durable=True caller goes through _save_state) -- so a genuine
    huggingface_hub internal-session-lock hang (the same one
    call_with_hard_timeout's own docstring documents, and
    _pull_durable_state_from_hf already guarded against) could hold
    _STATE_LOCK indefinitely, freezing the entire --workers 1 process
    (fast_check/entry_scan included) until gunicorn's own 300s worker
    timeout SIGKILLed it. Confirms the push is now routed through the same
    call_with_hard_timeout mechanism, not a direct call."""
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

    strat._push_durable_state_to_hf(_durable_state(trade_log=[{"x": 1}]))  # noqa: SLF001

    assert captured["timeout_sec"] == strat._DURABLE_STATE_HF_TIMEOUT_SEC  # noqa: SLF001
    assert uploaded  # the upload itself still actually happened


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
    strat._push_durable_state_to_hf(_durable_state())  # noqa: SLF001 -- must return quickly, not hang for 5s
    assert real_time.monotonic() - start < 2.0


def test_save_state_does_not_push_to_hf_by_default(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "HF_API_KEY", "fake-key")

    def fail_if_called(state):
        raise AssertionError("must not push to HF unless push_durable=True")

    monkeypatch.setattr(strat, "_push_durable_state_to_hf", fail_if_called)
    strat._save_state(_durable_state())  # noqa: SLF001


def test_save_state_pushes_only_the_durable_slice_when_requested(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "HF_API_KEY", "fake-key")
    monkeypatch.setattr(strat, "_last_durable_push_ts", 0.0)
    pushed = []
    monkeypatch.setattr(strat, "_push_durable_state_to_hf", lambda state: pushed.append(state))

    state = {
        "positions": [_position(ticker="KXBTCPERP")],  # must NOT end up needed in the pushed slice
        "trade_log": [{"ticker": "KXBTCPERP", "realized_pnl_usd": 1.0}],
        "realized_pnl_by_date": {"2026-07-23": 1.0}, "daily_reference_balance": {"2026-07-23": 20.0},
    }
    strat._save_state(state, push_durable=True)  # noqa: SLF001
    assert len(pushed) == 1
    slice_ = strat._durable_state_slice(pushed[0])  # noqa: SLF001
    assert slice_["trade_log"] == state["trade_log"]
    assert slice_["realized_pnl_by_date"] == state["realized_pnl_by_date"]
    assert slice_["daily_reference_balance"] == state["daily_reference_balance"]


def test_save_state_throttles_rapid_back_to_back_durable_pushes(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "HF_API_KEY", "fake-key")
    monkeypatch.setattr(strat, "_last_durable_push_ts", 0.0)
    calls = {"n": 0}
    monkeypatch.setattr(strat, "_push_durable_state_to_hf", lambda state: calls.__setitem__("n", calls["n"] + 1))

    strat._save_state(_durable_state(), push_durable=True)  # noqa: SLF001
    strat._save_state(_durable_state(), push_durable=True)  # noqa: SLF001 -- immediately after, same instant
    assert calls["n"] == 1


def test_load_state_recovers_durable_state_from_hf_when_local_file_is_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "does_not_exist" / "state.json")
    monkeypatch.setattr(strat, "HF_API_KEY", "fake-key")
    recovered = {
        "trade_log": [{"ticker": "KXETHPERP", "realized_pnl_usd": 2.5}],
        "realized_pnl_by_date": {"2026-07-23": 2.5}, "daily_reference_balance": {"2026-07-23": 42.97},
    }
    monkeypatch.setattr(strat, "_pull_durable_state_from_hf", lambda: recovered)

    state = strat._load_state()  # noqa: SLF001
    assert state["positions"] == []  # positions are NOT recovered this way -- reconciliation's job
    assert state["trade_log"] == recovered["trade_log"]
    assert state["realized_pnl_by_date"] == recovered["realized_pnl_by_date"]
    assert state["daily_reference_balance"] == recovered["daily_reference_balance"]


def test_load_state_defaults_cleanly_when_hf_recovery_also_fails(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "does_not_exist" / "state.json")
    monkeypatch.setattr(strat, "HF_API_KEY", "")  # no key at all -- can't even try
    state = strat._load_state()  # noqa: SLF001
    assert state == {"positions": [], "trade_log": [], "realized_pnl_by_date": {}, "daily_reference_balance": {}}


def test_manage_open_positions_pushes_durable_state_only_when_a_trade_closes(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    monkeypatch.setattr(strat, "HF_API_KEY", "fake-key")
    monkeypatch.setattr(strat, "_last_durable_push_ts", 0.0)
    pushed = {"n": 0}
    monkeypatch.setattr(strat, "_push_durable_state_to_hf", lambda state: pushed.__setitem__("n", pushed["n"] + 1))

    # Price barely moves -- nothing should exit, nothing should push.
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=6.605))
    strat._save_state({
        "positions": [_position(ticker="KXBTCPERP", entry_price=6.60)],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    strat.manage_open_positions()
    assert pushed["n"] == 0

    # Now a big favorable move -- take-profit fires, a trade closes.
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=6.60 * (1 + strat.TAKE_PROFIT_PCT + 0.001)))
    strat.manage_open_positions()
    assert pushed["n"] == 1


def test_run_cycle_manages_positions_then_scans_for_entries(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=6.605))
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 10.0)
    monkeypatch.setattr(strat, "scan_for_entries", lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: ([], []))
    strat._save_state({
        "positions": [_position(ticker="KXBTCPERP", entry_price=6.55)],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    result = strat.run_cycle()
    assert "position_management" in result and "entry_scan" in result
    assert result["position_management"]["action"] in ("none", "closed")


# ── Maker (post_only) order placement, with a taker fallback ───────────────
# Confirmed via a real 14-day backtest: at Kalshi's 0.8%/leg taker rate, no
# threshold combination tested was net profitable. Maker fills cost 16x less
# (0.05%/leg) -- these lock down the placement/fallback/fee-accounting logic
# that captures that saving without weakening a stop-loss's guarantee of
# actually closing the position.
@pytest.fixture(autouse=True)
def _no_real_sleep_in_maker_poll(monkeypatch):
    """The real poll loop sleeps MAKER_FILL_POLL_INTERVAL_SEC between checks
    -- tiny windows here so tests run instantly regardless."""
    monkeypatch.setattr(strat, "MAKER_FILL_WAIT_SECONDS", 0.05)
    monkeypatch.setattr(strat, "MAKER_FILL_POLL_INTERVAL_SEC", 0.01)


def test_maker_price_uses_bid_for_a_buy_order():
    market = _market_response(bid=6.50, ask=6.52)["market"]
    assert strat._maker_price("bid", market, tick_size=0.0001) == 6.50  # noqa: SLF001


def test_maker_price_uses_ask_for_a_sell_order():
    market = _market_response(bid=6.50, ask=6.52)["market"]
    assert strat._maker_price("ask", market, tick_size=0.0001) == 6.52  # noqa: SLF001


def test_maker_price_returns_none_when_bid_ask_missing():
    market = _market_response()["market"]  # no bid/ask set
    assert strat._maker_price("bid", market, tick_size=0.0001) is None  # noqa: SLF001


def test_maker_price_returns_none_for_a_non_positive_quote():
    market = _market_response(bid=0.0, ask=-1.0)["market"]
    assert strat._maker_price("bid", market, tick_size=0.0001) is None  # noqa: SLF001
    assert strat._maker_price("ask", market, tick_size=0.0001) is None  # noqa: SLF001


def test_maker_fee_rate_uses_ticker_specific_rate_from_cache(monkeypatch):
    monkeypatch.setattr(strat, "_MAKER_FEE_RATE_CACHE", {"rates": {"KXBTCPERP": 0.0003}, "computed_at": strat.time.time()})
    assert strat._maker_fee_rate("KXBTCPERP") == 0.0003  # noqa: SLF001
    assert strat._maker_fee_rate("KXZECPERP") == strat.DEFAULT_MAKER_FEE_RATE  # noqa: SLF001


def test_maker_fee_rate_refreshes_from_the_live_endpoint_on_a_cold_cache(monkeypatch):
    monkeypatch.setattr(strat, "_MAKER_FEE_RATE_CACHE", {"rates": None, "computed_at": 0.0})
    monkeypatch.setattr(strat, "get_margin_fee_tiers", lambda: {"maker_fee_rates": {"KXBTCPERP": 0.0005}})
    assert strat._maker_fee_rate("KXBTCPERP") == 0.0005  # noqa: SLF001


def test_round_trip_fee_usd_defaults_to_taker_both_legs():
    fee = strat.round_trip_fee_usd("KXNEARPERP", entry_price=1.80, exit_price=1.79, count=8.0)
    assert fee == round((1.80 + 1.79) * 8.0 * strat.DEFAULT_TAKER_FEE_RATE, 6)


def test_round_trip_fee_usd_uses_maker_rate_only_for_the_maker_leg(monkeypatch):
    monkeypatch.setattr(strat, "_MAKER_FEE_RATE_CACHE", {"rates": {"KXNEARPERP": 0.0005}, "computed_at": strat.time.time()})
    monkeypatch.setattr(strat, "_FEE_RATE_CACHE", {"rates": {"KXNEARPERP": 0.008}, "computed_at": strat.time.time()})
    fee = strat.round_trip_fee_usd("KXNEARPERP", entry_price=1.80, exit_price=1.79, count=8.0, entry_is_maker=True)
    expected = round(1.80 * 8.0 * 0.0005 + 1.79 * 8.0 * 0.008, 6)
    assert fee == expected
    assert fee < strat.round_trip_fee_usd("KXNEARPERP", entry_price=1.80, exit_price=1.79, count=8.0)


def test_round_trip_fee_usd_both_legs_maker_is_far_cheaper_than_both_taker(monkeypatch):
    monkeypatch.setattr(strat, "_MAKER_FEE_RATE_CACHE", {"rates": {}, "computed_at": strat.time.time()})
    monkeypatch.setattr(strat, "_FEE_RATE_CACHE", {"rates": {}, "computed_at": strat.time.time()})
    maker_fee = strat.round_trip_fee_usd(
        "KXBTCPERP", entry_price=6.60, exit_price=6.65, count=2.0, entry_is_maker=True, exit_is_maker=True,
    )
    taker_fee = strat.round_trip_fee_usd("KXBTCPERP", entry_price=6.60, exit_price=6.65, count=2.0)
    assert maker_fee < taker_fee / 10  # confirmed-live rates are a 16x difference


def _order_response(order_id="ord-1"):
    return {"order": {"order_id": order_id}}


def test_maker_order_fills_fully_within_poll_window_no_fallback(monkeypatch):
    placed = []

    def fake_create(**kwargs):
        placed.append(kwargs)
        return _order_response()

    monkeypatch.setattr(strat, "create_margin_order", fake_create)
    monkeypatch.setattr(strat, "get_margin_order", lambda order_id: {"order": {"fill_count": 5.0, "remaining_count": 0.0}})
    monkeypatch.setattr(strat, "cancel_margin_order", lambda order_id: (_ for _ in ()).throw(AssertionError("must not cancel a fully-filled order")))

    result, fill_type = strat._place_order_maker_then_fallback(  # noqa: SLF001
        ticker="KXBTCPERP", order_side="bid", count=5.0, maker_price=6.60, fallback_price=6.61,
        reduce_only=False, urgent=False,
    )
    assert fill_type == "maker"
    assert len(placed) == 1  # only the maker order -- no taker fallback call
    assert placed[0]["time_in_force"] == "good_till_canceled"
    assert placed[0]["post_only"] is True


def test_maker_order_unfilled_urgent_falls_back_to_taker(monkeypatch):
    placed = []

    def fake_create(**kwargs):
        placed.append(kwargs)
        return _order_response()

    monkeypatch.setattr(strat, "create_margin_order", fake_create)
    monkeypatch.setattr(strat, "get_margin_order", lambda order_id: {"order": {"fill_count": 0.0, "remaining_count": 5.0}})
    canceled = []
    monkeypatch.setattr(strat, "cancel_margin_order", lambda order_id: canceled.append(order_id))

    result, fill_type = strat._place_order_maker_then_fallback(  # noqa: SLF001
        ticker="KXBTCPERP", order_side="ask", count=5.0, maker_price=6.65, fallback_price=6.60,
        reduce_only=True, urgent=True,
    )
    assert fill_type == "taker_fallback"
    assert len(canceled) == 1  # the unfilled maker order was canceled first
    assert len(placed) == 2  # maker attempt + taker fallback
    assert placed[1]["time_in_force"] == "immediate_or_cancel"
    assert placed[1]["count"] == 5.0  # the FULL remaining count, none of it filled
    assert placed[1]["price"] == 6.60  # the fallback (marketable) price, not the maker price


def test_maker_order_unfilled_non_urgent_returns_none_no_fallback(monkeypatch):
    placed = []
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: placed.append(kwargs) or _order_response())
    monkeypatch.setattr(strat, "get_margin_order", lambda order_id: {"order": {"fill_count": 0.0, "remaining_count": 5.0}})
    monkeypatch.setattr(strat, "cancel_margin_order", lambda order_id: None)

    result, fill_type = strat._place_order_maker_then_fallback(  # noqa: SLF001
        ticker="KXBTCPERP", order_side="bid", count=5.0, maker_price=6.60, fallback_price=6.61,
        reduce_only=False, urgent=False,
    )
    assert result is None
    assert fill_type == "unfilled"
    assert len(placed) == 1  # never placed a taker fallback


def test_maker_order_partial_fill_urgent_falls_back_for_remainder_only(monkeypatch):
    placed = []
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: placed.append(kwargs) or _order_response())
    monkeypatch.setattr(strat, "get_margin_order", lambda order_id: {"order": {"fill_count": 3.0, "remaining_count": 2.0}})
    monkeypatch.setattr(strat, "cancel_margin_order", lambda order_id: None)

    result, fill_type = strat._place_order_maker_then_fallback(  # noqa: SLF001
        ticker="KXBTCPERP", order_side="ask", count=5.0, maker_price=6.65, fallback_price=6.60,
        reduce_only=True, urgent=True,
    )
    assert fill_type == "taker_fallback"
    assert len(placed) == 2
    assert placed[1]["count"] == 2.0  # only the unfilled remainder, not the original 5.0


def test_maker_order_partial_fill_non_urgent_keeps_partial_no_fallback(monkeypatch):
    placed = []
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: placed.append(kwargs) or _order_response())
    monkeypatch.setattr(strat, "get_margin_order", lambda order_id: {"order": {"fill_count": 3.0, "remaining_count": 2.0}})
    monkeypatch.setattr(strat, "cancel_margin_order", lambda order_id: None)

    result, fill_type = strat._place_order_maker_then_fallback(  # noqa: SLF001
        ticker="KXBTCPERP", order_side="bid", count=5.0, maker_price=6.60, fallback_price=6.61,
        reduce_only=False, urgent=False,
    )
    assert result is not None  # some of it filled -- caller verifies the real amount via the exchange itself
    assert fill_type == "unfilled"
    assert len(placed) == 1


def test_maker_order_post_only_cross_cancel_stops_polling_early(monkeypatch):
    """A stale bid/ask snapshot at placement time can make the exchange
    reject the post_only order outright as an immediate cross -- polling
    must recognize this and stop immediately rather than waiting out the
    full window on an order that no longer exists."""
    poll_calls = []

    def fake_get_order(order_id):
        poll_calls.append(order_id)
        return {"order": {"fill_count": 0.0, "remaining_count": 5.0, "last_update_reason": "PostOnlyCrossCancel"}}

    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: _order_response())
    monkeypatch.setattr(strat, "get_margin_order", fake_get_order)
    monkeypatch.setattr(strat, "cancel_margin_order", lambda order_id: None)
    # A much longer window than the test should actually take if early-exit works.
    monkeypatch.setattr(strat, "MAKER_FILL_WAIT_SECONDS", 5.0)
    monkeypatch.setattr(strat, "MAKER_FILL_POLL_INTERVAL_SEC", 0.01)

    strat._place_order_maker_then_fallback(  # noqa: SLF001
        ticker="KXBTCPERP", order_side="bid", count=5.0, maker_price=6.60, fallback_price=6.61,
        reduce_only=False, urgent=False,
    )
    assert len(poll_calls) == 1  # stopped after the very first poll, not several seconds of retries


def test_maker_order_missing_order_id_treated_as_fully_unfilled(monkeypatch):
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: {"order": {}})  # no order_id at all

    def fail_if_polled(order_id):
        raise AssertionError("must not poll for a fill without a real order_id")

    monkeypatch.setattr(strat, "get_margin_order", fail_if_polled)
    placed_fallback = []
    monkeypatch.setattr(strat, "cancel_margin_order", lambda order_id: None)

    def fake_create_order_side_effect(**kwargs):
        placed_fallback.append(kwargs)
        return {"order": {}}

    result, fill_type = strat._place_order_maker_then_fallback(  # noqa: SLF001
        ticker="KXBTCPERP", order_side="ask", count=5.0, maker_price=6.65, fallback_price=6.60,
        reduce_only=True, urgent=True,
    )
    assert fill_type == "taker_fallback"


def test_manage_open_positions_take_profit_never_attempts_a_maker_order(monkeypatch, tmp_path):
    """Real, confirmed live incident (2026-08-19): Kalshi's API rejects
    post_only combined with reduce_only outright ("reduce_only can only be
    used with IoC or FoK orders") -- there is no maker-fee path available
    for ANY exit, take-profit included, regardless of ENABLE_MAKER_ORDERS.
    3 real positions sat stuck, unable to close, for 300+ cycles before this
    was caught. Exits must always go straight to a plain IoC taker order."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(strat, "ENABLE_MAKER_ORDERS", True)
    strat._save_state({
        "positions": [_position(ticker="KXBTCPERP", entry_price=6.60, count=5.0, side="long")],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    exit_price = 6.60 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=exit_price, bid=exit_price - 0.001, ask=exit_price))
    calls = {"n": 0}

    def fake_positions():
        calls["n"] += 1
        count = "5.00" if calls["n"] == 1 else "0.00"
        return {"positions": [_real_position("KXBTCPERP", count, "6.60")] if float(count) > 0 else []}

    monkeypatch.setattr(strat, "get_margin_positions", fake_positions)

    placed = []

    def fake_create(**kwargs):
        placed.append(kwargs)
        return _order_response()

    monkeypatch.setattr(strat, "create_margin_order", fake_create)

    result = strat.manage_open_positions(dry_run=False)
    assert result["action"] == "closed"
    assert len(placed) == 1  # straight to taker -- no maker attempt at all
    assert placed[0].get("post_only", False) is False
    assert placed[0]["time_in_force"] == "immediate_or_cancel"
    assert result["closed"][0]["exit_fill_type"] == "taker_fallback"


def test_manage_open_positions_stop_loss_never_attempts_a_maker_order(monkeypatch, tmp_path):
    """Same real incident as the take-profit test above -- a stop-loss exit
    must also go straight to a taker order, never the (permanently broken
    for exits) maker path."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(strat, "ENABLE_MAKER_ORDERS", True)
    strat._save_state({
        "positions": [_position(ticker="KXBTCPERP", entry_price=6.60, count=5.0, side="long")],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    exit_price = 6.60 * (1 - strat.STOP_LOSS_PCT - 0.001)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=exit_price, bid=exit_price, ask=exit_price + 0.001))
    calls = {"n": 0}

    def fake_positions():
        calls["n"] += 1
        count = "5.00" if calls["n"] == 1 else "0.00"
        return {"positions": [_real_position("KXBTCPERP", count, "6.60")] if float(count) > 0 else []}

    monkeypatch.setattr(strat, "get_margin_positions", fake_positions)

    placed = []
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: placed.append(kwargs) or _order_response())

    result = strat.manage_open_positions(dry_run=False)
    assert result["action"] == "closed"
    assert len(placed) == 1  # a single, guaranteed taker order -- no maker attempt first
    assert placed[0]["time_in_force"] == "immediate_or_cancel"
    assert result["closed"][0]["exit_fill_type"] == "taker_fallback"


def test_manage_open_positions_exit_is_a_taker_order_even_with_no_bid_ask(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(strat, "ENABLE_MAKER_ORDERS", True)
    strat._save_state({
        "positions": [_position(ticker="KXBTCPERP", entry_price=6.60, count=5.0, side="long")],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    exit_price = 6.60 * (1 + strat.TAKE_PROFIT_PCT + 0.001)
    # No bid/ask in this market response -- irrelevant to exits now (they
    # never call _maker_price at all), included to confirm that stays true.
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=exit_price))
    calls = {"n": 0}

    def fake_positions():
        calls["n"] += 1
        count = "5.00" if calls["n"] == 1 else "0.00"
        return {"positions": [_real_position("KXBTCPERP", count, "6.60")] if float(count) > 0 else []}

    monkeypatch.setattr(strat, "get_margin_positions", fake_positions)

    placed = []
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: placed.append(kwargs) or _order_response())

    result = strat.manage_open_positions(dry_run=False)
    assert result["action"] == "closed"
    assert len(placed) == 1
    assert placed[0]["time_in_force"] == "immediate_or_cancel"
    assert placed[0].get("post_only", False) is False  # plain taker call -- post_only never set


def test_scan_and_enter_tries_maker_order_for_a_new_entry_when_enabled(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(strat, "ENABLE_MAKER_ORDERS", True)
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {}})
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 100.0)
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: (
            [{"ticker": "KXBTCPERP", "current_price": 6.60, "reason": "dip", "side": "long"}],
            [{"ticker": "KXBTCPERP", "should_enter": True, "reason": "dip"}],
        ),
    )
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=6.60, bid=6.595, ask=6.60))
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {"positions": [_real_position("KXBTCPERP", "5.00", "6.595")]})

    placed = []
    monkeypatch.setattr(strat, "create_margin_order", lambda **kwargs: placed.append(kwargs) or _order_response())
    monkeypatch.setattr(strat, "get_margin_order", lambda order_id: {"order": {"fill_count": 5.0, "remaining_count": 0.0}})

    result = strat.scan_and_enter(dry_run=False)
    assert result["opened"][0]["action"] == "opened"
    assert result["opened"][0]["entry_fill_type"] == "maker"
    assert placed[0]["post_only"] is True
    assert placed[0]["side"] == "bid"


# ---------------------------------------------------------------------------
# Post-trade analysis feature: entry-time context capture, per-trade save
# durability, trade_log capping, and the evidence-gated confidence-
# threshold override -- all built to feed perps_trade_analysis.py real data
# to work with, and to close a real, confirmed gap where a trade's own
# record could be silently lost to a crash before the loop's old batched
# end-of-cycle save.
# ---------------------------------------------------------------------------
def test_evaluate_candidate_uses_module_default_confidence_when_no_override(monkeypatch):
    monkeypatch.setattr(strat, "MODEL_CONFIDENCE_MIN", 0.6)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.55,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is False  # 0.55 < module default 0.6


def test_evaluate_candidate_confidence_min_override_can_allow_a_lower_bar(monkeypatch):
    monkeypatch.setattr(strat, "MODEL_CONFIDENCE_MIN", 0.6)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.55,
    })
    result = strat.evaluate_candidate("KXBTCPERP", confidence_min=0.5)
    assert result["should_enter"] is True  # 0.55 >= the 0.5 override, even though it's below the module default


def test_evaluate_candidate_confidence_min_override_can_raise_the_bar(monkeypatch):
    monkeypatch.setattr(strat, "MODEL_CONFIDENCE_MIN", 0.55)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.58,
    })
    result = strat.evaluate_candidate("KXBTCPERP", confidence_min=0.62)
    assert result["should_enter"] is False  # 0.58 clears the module default but not the (higher) override


def test_apply_confidence_threshold_override_persists_to_state(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "MODEL_CONFIDENCE_MIN", 0.58)
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {}})  # noqa: SLF001

    result = strat.apply_confidence_threshold_override(0.63, reason="evidence-gated: test")

    assert result["model_confidence_min"] == 0.63
    assert result["previous"] == 0.58
    assert result["reason"] == "evidence-gated: test"
    reloaded = strat._load_state()  # noqa: SLF001
    assert reloaded["tuning"]["model_confidence_min"] == 0.63


def test_apply_confidence_threshold_override_does_not_clobber_an_existing_correlation_override(monkeypatch, tmp_path):
    """Real bug found and fixed: this used to overwrite state["tuning"]
    wholesale -- applying a confidence-threshold tune after a correlation-
    study tune (or vice versa) would silently wipe the other one out."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    strat._save_state({  # noqa: SLF001
        "positions": [], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
        "tuning": {"correlation_study_enabled": True, "correlation_confidence_max_adjustment": 0.09},
    })

    strat.apply_confidence_threshold_override(0.63, reason="test")

    reloaded = strat._load_state()  # noqa: SLF001
    assert reloaded["tuning"]["model_confidence_min"] == 0.63
    assert reloaded["tuning"]["correlation_study_enabled"] is True
    assert reloaded["tuning"]["correlation_confidence_max_adjustment"] == 0.09


def test_apply_correlation_study_override_persists_to_state(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "USE_CORRELATION_STUDY", False)
    monkeypatch.setattr(strat, "CORRELATION_CONFIDENCE_MAX_ADJUSTMENT", 0.06)
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {}})  # noqa: SLF001

    result = strat.apply_correlation_study_override(enabled=True, reason="evidence-gated: test")

    assert result["correlation_study_enabled"] is True
    assert result["previous_correlation_study_enabled"] is False
    reloaded = strat._load_state()  # noqa: SLF001
    assert reloaded["tuning"]["correlation_study_enabled"] is True


def test_apply_correlation_study_override_only_changes_the_field_passed(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    strat._save_state({  # noqa: SLF001
        "positions": [], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
        "tuning": {"correlation_study_enabled": True, "correlation_confidence_max_adjustment": 0.06},
    })

    strat.apply_correlation_study_override(max_adjustment=0.09, reason="test")

    reloaded = strat._load_state()  # noqa: SLF001
    assert reloaded["tuning"]["correlation_study_enabled"] is True  # untouched
    assert reloaded["tuning"]["correlation_confidence_max_adjustment"] == 0.09


def test_apply_correlation_study_override_does_not_clobber_the_confidence_threshold(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    strat._save_state({  # noqa: SLF001
        "positions": [], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
        "tuning": {"model_confidence_min": 0.63},
    })

    strat.apply_correlation_study_override(enabled=True, reason="test")

    reloaded = strat._load_state()  # noqa: SLF001
    assert reloaded["tuning"]["model_confidence_min"] == 0.63
    assert reloaded["tuning"]["correlation_study_enabled"] is True


def test_apply_position_management_override_rejects_an_unknown_feature(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {}})  # noqa: SLF001
    try:
        strat.apply_position_management_override("not_a_real_feature", enabled=True, reason="test")
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_apply_position_management_override_persists_to_state(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "USE_PARTIAL_EXIT", False)
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {}})  # noqa: SLF001

    result = strat.apply_position_management_override("partial_exit", enabled=True, reason="evidence-gated: test")

    assert result["partial_exit_enabled"] is True
    assert result["previous_partial_exit_enabled"] is False
    assert result["reason"] == "evidence-gated: test"
    reloaded = strat._load_state()  # noqa: SLF001
    assert reloaded["tuning"]["partial_exit_enabled"] is True


def test_apply_position_management_override_does_not_clobber_other_tuning_keys(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    strat._save_state({  # noqa: SLF001
        "positions": [], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
        "tuning": {
            "model_confidence_min": 0.63, "correlation_study_enabled": True,
            "scale_in_enabled": False, "conviction_sizing_enabled": True,
        },
    })

    strat.apply_position_management_override("partial_exit", enabled=True, reason="test")

    reloaded = strat._load_state()  # noqa: SLF001
    assert reloaded["tuning"]["model_confidence_min"] == 0.63
    assert reloaded["tuning"]["correlation_study_enabled"] is True
    assert reloaded["tuning"]["scale_in_enabled"] is False
    assert reloaded["tuning"]["conviction_sizing_enabled"] is True
    assert reloaded["tuning"]["partial_exit_enabled"] is True


def test_apply_position_management_override_only_changes_the_feature_passed(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    strat._save_state({  # noqa: SLF001
        "positions": [], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
        "tuning": {"scale_in_enabled": True, "partial_exit_enabled": True, "conviction_sizing_enabled": True},
    })

    strat.apply_position_management_override("conviction_sizing", enabled=False, reason="test")

    reloaded = strat._load_state()  # noqa: SLF001
    assert reloaded["tuning"]["scale_in_enabled"] is True
    assert reloaded["tuning"]["partial_exit_enabled"] is True
    assert reloaded["tuning"]["conviction_sizing_enabled"] is False


def test_scan_and_enter_reads_the_confidence_override_from_state(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    strat._save_state({  # noqa: SLF001
        "positions": [], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
        "tuning": {"model_confidence_min": 0.71, "updated_at": "x", "reason": "test"},
    })
    captured = {}

    def fake_scan_for_entries(tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None):
        captured["confidence_min"] = confidence_min
        return [], []

    monkeypatch.setattr(strat, "scan_for_entries", fake_scan_for_entries)
    strat.scan_and_enter()
    assert captured["confidence_min"] == 0.71


def test_scan_and_enter_reads_the_correlation_study_override_from_state(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    strat._save_state({  # noqa: SLF001
        "positions": [], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
        "tuning": {"correlation_study_enabled": True, "correlation_confidence_max_adjustment": 0.09},
    })
    captured = {}

    def fake_scan_for_entries(tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None):
        captured["correlation_study_enabled"] = correlation_study_enabled
        captured["correlation_max_adjustment"] = correlation_max_adjustment
        return [], []

    monkeypatch.setattr(strat, "scan_for_entries", fake_scan_for_entries)
    strat.scan_and_enter()
    assert captured["correlation_study_enabled"] is True
    assert captured["correlation_max_adjustment"] == 0.09


def test_scan_and_enter_captures_entry_time_model_context_in_position(monkeypatch, tmp_path):
    """A win/loss post-mortem needs to know what the model actually saw at
    entry -- previously this only ever existed transiently in
    evaluate_candidate's own return dict and was never persisted."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {}})  # noqa: SLF001
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 100.0)
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: (
            [{
                "ticker": "KXBTCPERP", "current_price": 6.60, "reason": "dip; model predicts up (p=0.71)",
                "side": "long", "score": 0.71, "probability_up": 0.71, "model_direction": "up",
                "trend_pct": 0.002, "volatility_30": 0.0015,
            }],
            [],
        ),
    )
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=6.60))

    result = strat.scan_and_enter(dry_run=True)
    assert result["opened"][0]["action"] == "opened"

    state = strat._load_state()  # noqa: SLF001
    position = state["positions"][0]
    assert position["entry_probability_up"] == 0.71
    assert position["entry_model_direction"] == "up"
    assert position["entry_score"] == 0.71
    assert position["entry_trend_pct"] == 0.002
    assert position["entry_reason"] == "dip; model predicts up (p=0.71)"


def test_manage_open_positions_copies_entry_context_into_the_closed_trade(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    position = _position(ticker="KXBTCPERP", entry_price=6.60, minutes_ago=12)
    position.update({
        "entry_probability_up": 0.68, "entry_model_direction": "up", "entry_score": 0.68,
        "entry_trend_pct": 0.001, "entry_reason": "dip; model predicts up (p=0.68)",
    })
    strat._save_state({  # noqa: SLF001
        "positions": [position], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=6.60 * (1 + strat.TAKE_PROFIT_PCT + 0.001)))

    result = strat.manage_open_positions()
    trade = result["closed"][0]
    assert trade["entry_probability_up"] == 0.68
    assert trade["entry_model_direction"] == "up"
    assert trade["entry_score"] == 0.68
    assert trade["entry_reason"] == "dip; model predicts up (p=0.68)"
    assert trade["opened_at"] == position["opened_at"]
    assert trade["hold_minutes"] == pytest.approx(12.0, abs=0.5)


def test_manage_open_positions_saves_state_immediately_after_each_trade_closes(monkeypatch, tmp_path):
    """Real, confirmed gap this closes: the loop used to save state ONCE at
    the very end, after every position had been processed -- a crash after
    a real exit fill but before that final save silently lost the trade's
    OWN record forever (the position itself is safe either way, via
    reconciliation). Two positions closing in the same cycle must trigger
    (at least) two durable saves, not one."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    monkeypatch.setattr(strat, "_DURABLE_PUSH_MIN_INTERVAL_SEC", 0)  # don't let the throttle mask this
    strat._save_state({  # noqa: SLF001
        "positions": [
            _position(ticker="KXBTCPERP", entry_price=6.60),
            _position(ticker="KXETHPERP", entry_price=100.0),
        ],
        "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    # Both comfortably clear take-profit -- both close this cycle.
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(
        price=(6.60 if ticker == "KXBTCPERP" else 100.0) * (1 + strat.TAKE_PROFIT_PCT + 0.001),
    ))

    durable_push_calls = []
    monkeypatch.setattr(strat, "_push_durable_state_to_hf", lambda state: durable_push_calls.append(len(state.get("trade_log") or [])))

    result = strat.manage_open_positions()
    assert len(result["closed"]) == 2
    # The first two pushes must show trade_log growing ONE AT A TIME (1,
    # then 2) -- proving each trade was saved the instant it existed, not
    # batched until the whole loop finished. (A final push with both
    # already in it, from the loop's own end-of-cycle save, is expected
    # and harmless -- redundant, not a bug.)
    assert durable_push_calls[:2] == [1, 2]


def test_manage_open_positions_caps_trade_log_at_max_entries(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    monkeypatch.setattr(strat, "MAX_TRADE_LOG_ENTRIES", 5)
    existing_trades = [{"ticker": "KXBTCPERP", "closed_at": f"2026-01-0{i+1}T00:00:00+00:00", "realized_pnl_usd": 0.01} for i in range(5)]
    strat._save_state({  # noqa: SLF001
        "positions": [_position(ticker="KXETHPERP", entry_price=100.0)],
        "realized_pnl_by_date": {}, "trade_log": existing_trades, "daily_reference_balance": {},
    })
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=100.0 * (1 + strat.TAKE_PROFIT_PCT + 0.001)))

    strat.manage_open_positions()

    state = strat._load_state()  # noqa: SLF001
    assert len(state["trade_log"]) == 5  # capped, not 6
    # The oldest entry (2026-01-01) was trimmed, the newest real close survived.
    assert state["trade_log"][-1]["ticker"] == "KXETHPERP"
    assert all(t.get("closed_at") != "2026-01-01T00:00:00+00:00" for t in state["trade_log"])


def test_record_milestone_persists_baseline_and_high_water_mark(monkeypatch, tmp_path):
    """Real gap found in review: the dashboard never showed progress toward
    a goal, just the current balance in isolation. record_milestone must
    persist its baseline/high-water-mark in the SAME durable state as
    positions/trade_log so it survives a restart, not just live in memory."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    strat._save_state({"positions": [], "realized_pnl_by_date": {}, "trade_log": {}, "daily_reference_balance": {}})  # noqa: SLF001

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


# ── Chart-study confidence layer (USE_CORRELATION_STUDY) ────────────────────
# See crypto_correlation.py's own module docstring for the full design.
# Default OFF -- the score/reason must always be attached for observability,
# but must never change should_enter/score/decide_exit until explicitly
# turned on.

def test_evaluate_candidate_attaches_correlation_reading_even_when_flag_off(monkeypatch):
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False, "ticker": ticker})
    monkeypatch.setattr(
        strat.crypto_correlation, "perps_correlation_bullishness",
        lambda coin, row=None: {"score": 0.9, "reason": "strong confirmation", "components": {}},
    )
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["correlation_score"] == 0.9
    assert result["correlation_reason"] == "strong confirmation"
    # Flag is off by default -- score must be the plain dip-depth score, not
    # nudged by the correlation reading above.
    assert "correlation" not in result["reason"]


def test_evaluate_candidate_correlation_confirmation_lowers_the_bar_when_flag_on(monkeypatch):
    monkeypatch.setattr(strat, "USE_CORRELATION_STUDY", True)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    # probability_up=0.55 alone would miss MODEL_CONFIDENCE_MIN (0.58) by
    # 0.03 -- within CORRELATION_CONFIDENCE_MAX_ADJUSTMENT (0.06), so a
    # maximally bullish correlation reading should be enough to clear it.
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.55,
    })
    monkeypatch.setattr(
        strat.crypto_correlation, "perps_correlation_bullishness",
        lambda coin, row=None: {"score": 1.0, "reason": "max confirmation", "components": {}},
    )
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True
    assert "correlation study" in result["reason"]


def test_evaluate_candidate_correlation_study_enabled_override_works_even_when_the_module_flag_is_off(monkeypatch):
    """The per-call override params (correlation_study_enabled/
    correlation_max_adjustment) are what scan_and_enter feeds from
    apply_correlation_study_override's durable state -- must take effect
    on their own, without the env-controlled USE_CORRELATION_STUDY flag
    also being on, so evidence-gated tuning can enable this without a
    redeploy."""
    assert strat.USE_CORRELATION_STUDY is False  # module default -- not touched by this test
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.55,
    })
    monkeypatch.setattr(
        strat.crypto_correlation, "perps_correlation_bullishness",
        lambda coin, row=None: {"score": 1.0, "reason": "max confirmation", "components": {}},
    )
    result = strat.evaluate_candidate("KXBTCPERP", correlation_study_enabled=True, correlation_max_adjustment=0.06)
    assert result["should_enter"] is True
    assert "correlation study" in result["reason"]


def test_evaluate_candidate_correlation_disagreement_raises_the_bar_when_flag_on(monkeypatch):
    monkeypatch.setattr(strat, "USE_CORRELATION_STUDY", True)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    # probability_up=0.60 alone would clear MODEL_CONFIDENCE_MIN (0.58) --
    # a maximally bearish correlation reading raises the bar past it
    # (0.58 + 0.06 = 0.64), so this must now be rejected.
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "ticker": ticker, "direction": "up", "probability_up": 0.60,
    })
    monkeypatch.setattr(
        strat.crypto_correlation, "perps_correlation_bullishness",
        lambda coin, row=None: {"score": -1.0, "reason": "max disagreement", "components": {}},
    )
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is False


def test_evaluate_candidate_correlation_veto_on_technical_only_fallback_when_flag_on(monkeypatch):
    monkeypatch.setattr(strat, "USE_CORRELATION_STUDY", True)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False, "ticker": ticker})
    monkeypatch.setattr(
        strat.crypto_correlation, "perps_correlation_bullishness",
        lambda coin, row=None: {"score": -0.9, "reason": "strongly disagrees", "components": {}},
    )
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is False
    assert "correlation study disagrees" in result["reason"]


def test_decide_exit_ignores_correlation_score_when_omitted():
    """Regression guard: correlation_score defaults to None, so a flat/
    losing position near max_hold must behave exactly as it did before this
    param existed."""
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(pos, 6.60 * 1.001)
    assert should_exit and "max_hold_time" in reason


def test_decide_exit_favorable_correlation_extends_a_flat_position_past_max_hold():
    pos = _position(minutes_ago=strat.MAX_HOLD_MINUTES + 1)
    should_exit, reason = strat.decide_exit(
        pos, 6.60 * 1.001,  # flat price -- not promising on its own
        correlation_score=strat.PROMISING_CORRELATION_SCORE + 0.1,
    )
    assert not should_exit
    assert "holding" in reason


def test_decide_exit_unfavorable_correlation_triggers_early_pre_exit_study():
    past_pre_exit = strat.MAX_HOLD_MINUTES - strat.PRE_EXIT_STUDY_MINUTES + 1
    pos = _position(minutes_ago=past_pre_exit)
    should_exit, reason = strat.decide_exit(
        pos, 6.60 * 0.999,  # flat/losing
        correlation_score=-(strat.PROMISING_CORRELATION_SCORE + 0.1),
    )
    assert should_exit
    assert "pre_exit_study" in reason
    assert "favors reversal" in reason


def test_decide_exit_unfavorable_correlation_flips_sign_for_a_short():
    """Short positions favor a FALLING price -- a bearish (negative)
    correlation_score is therefore FAVORABLE for a short, the mirror image
    of the long case above."""
    past_pre_exit = strat.MAX_HOLD_MINUTES - strat.PRE_EXIT_STUDY_MINUTES + 1
    pos = _position(minutes_ago=past_pre_exit, entry_price=6.60, side="short")
    should_exit, reason = strat.decide_exit(
        pos, 6.60 * 1.001,  # flat/losing for a short
        correlation_score=strat.PROMISING_CORRELATION_SCORE + 0.1,  # bullish -> unfavorable for a short
    )
    assert should_exit
    assert "pre_exit_study" in reason


# ── Conviction-scaled entry sizing (USE_CONVICTION_SIZING) ──────────────────

def test_compute_leveraged_count_applies_a_size_multiplier():
    market = {"price": 2.0, "leverage_estimate": 6.0}
    count, detail = strat.compute_leveraged_count(10.0, market, size_multiplier=1.5)
    # $10 * 20% * 1.5 = $3 margin, 6x leverage = $18 notional, at $2/contract = 9.
    assert count == 9
    assert detail["margin_budget_usd"] == 3.0
    assert detail["size_multiplier"] == 1.5


def test_compute_leveraged_count_default_multiplier_matches_old_behavior():
    market = {"price": 2.0, "leverage_estimate": 6.0}
    count, detail = strat.compute_leveraged_count(10.0, market)
    assert count == 6
    assert detail["size_multiplier"] == 1.0


def test_evaluate_candidate_exposes_entry_confidence_on_the_model_confirmed_path(monkeypatch):
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {
        "model_ok": True, "direction": "up", "probability_up": 0.9, "ticker": ticker,
    })
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True
    assert result["entry_confidence"] == pytest.approx(0.9)
    assert result["effective_confidence_min"] == pytest.approx(strat.MODEL_CONFIDENCE_MIN)


def test_evaluate_candidate_omits_entry_confidence_on_technical_only_fallback(monkeypatch):
    """No trained model yet -- the technical-only-fallback score is on a
    completely different scale (~0.002-0.02) than model confidence
    (~0.5-1.0) and must never be mistaken for it by a sizing caller."""
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row())
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False, "ticker": ticker})
    result = strat.evaluate_candidate("KXBTCPERP")
    assert result["should_enter"] is True
    assert "entry_confidence" not in result
    assert "effective_confidence_min" not in result


def test_scan_and_enter_conviction_sizing_off_by_default(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response())
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 10.0)
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: (
            [{
                "ticker": "KXBTCPERP", "current_price": 6.60, "reason": "test", "score": 0.9,
                "entry_confidence": 0.99, "effective_confidence_min": strat.MODEL_CONFIDENCE_MIN,
            }], [],
        ),
    )
    result = strat.scan_and_enter(dry_run=True)
    assert result["opened"][0]["sizing"]["size_multiplier"] == 1.0


def test_scan_and_enter_sizes_a_high_conviction_entry_larger_than_a_borderline_one(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "USE_CONVICTION_SIZING", True)
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response())
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 10.0)

    def _run(confidence):
        monkeypatch.setattr(strat, "STATE_FILE", tmp_path / f"state_{confidence}.json")
        monkeypatch.setattr(
            strat, "scan_for_entries",
            lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None, confidence=confidence: (
                [{
                    "ticker": "KXBTCPERP", "current_price": 6.60, "reason": "test", "score": confidence,
                    "entry_confidence": confidence, "effective_confidence_min": strat.MODEL_CONFIDENCE_MIN,
                }], [],
            ),
        )
        result = strat.scan_and_enter(dry_run=True)
        return result["opened"][0]["sizing"]["size_multiplier"]

    borderline_multiplier = _run(strat.MODEL_CONFIDENCE_MIN + 0.001)
    max_conviction_multiplier = _run(1.0)
    assert borderline_multiplier == pytest.approx(strat.CONVICTION_SIZE_MIN_MULTIPLIER, abs=0.02)
    assert max_conviction_multiplier == pytest.approx(strat.CONVICTION_SIZE_MAX_MULTIPLIER)
    assert max_conviction_multiplier > borderline_multiplier


# ── Scale-in (USE_SCALE_IN) ──────────────────────────────────────────────────

def _scale_in_position(*, minutes_ago=15, entry_price=6.60, count=6.0, **extra):
    # minutes_ago/entry_price/count route through _position() (it converts
    # minutes_ago into a real opened_at timestamp -- it isn't itself a key
    # on the returned dict) -- everything else (original_count, scale_ins,
    # last_scale_in_at, ...) is a plain field set/overridden afterward.
    pos = _position(entry_price=entry_price, minutes_ago=minutes_ago, count=count)
    pos["original_count"] = count
    pos.update(extra)
    return pos


def _promising_price(pos, take_profit_fraction=0.6):
    exit_pcts = strat.adaptive_exit_pcts(pos.get("entry_volatility_30"))
    entry_price = pos["entry_price"]
    return entry_price * (1 + exit_pcts["take_profit_pct"] * take_profit_fraction)


def test_should_scale_in_false_by_default():
    pos = _scale_in_position()
    should_add, _ = strat._should_scale_in(  # noqa: SLF001
        pos, _promising_price(pos), now=dt.datetime.now(dt.timezone.utc),
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_add is False


def test_should_scale_in_fires_when_profitable_enough_and_continuation_confirmed(monkeypatch):
    monkeypatch.setattr(strat, "USE_SCALE_IN", True)
    pos = _scale_in_position()
    should_add, reason = strat._should_scale_in(  # noqa: SLF001
        pos, _promising_price(pos), now=dt.datetime.now(dt.timezone.utc),
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_add is True
    assert "scale_in" in reason


def test_should_scale_in_false_when_not_profitable_enough(monkeypatch):
    monkeypatch.setattr(strat, "USE_SCALE_IN", True)
    pos = _scale_in_position()
    should_add, _ = strat._should_scale_in(  # noqa: SLF001
        pos, _promising_price(pos, take_profit_fraction=0.1), now=dt.datetime.now(dt.timezone.utc),
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_add is False


def test_should_scale_in_false_without_continuation_confirmation(monkeypatch):
    monkeypatch.setattr(strat, "USE_SCALE_IN", True)
    pos = _scale_in_position()
    should_add, reason = strat._should_scale_in(  # noqa: SLF001
        pos, _promising_price(pos), now=dt.datetime.now(dt.timezone.utc),
        dollar_volume_z=None, momentum_pct=None,
    )
    assert should_add is False
    assert "no confirmed continuation" in reason


def test_should_scale_in_respects_cooldown_since_entry(monkeypatch):
    monkeypatch.setattr(strat, "USE_SCALE_IN", True)
    pos = _scale_in_position(minutes_ago=strat.SCALE_IN_MIN_MINUTES_SINCE_ENTRY - 1)
    should_add, _ = strat._should_scale_in(  # noqa: SLF001
        pos, _promising_price(pos), now=dt.datetime.now(dt.timezone.utc),
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_add is False


def test_should_scale_in_respects_cooldown_since_last_scale_in(monkeypatch):
    monkeypatch.setattr(strat, "USE_SCALE_IN", True)
    now = dt.datetime.now(dt.timezone.utc)
    pos = _scale_in_position(
        last_scale_in_at=(now - dt.timedelta(minutes=strat.SCALE_IN_MIN_MINUTES_SINCE_LAST - 1)).isoformat(),
    )
    should_add, _ = strat._should_scale_in(  # noqa: SLF001
        pos, _promising_price(pos), now=now,
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_add is False


def test_should_scale_in_respects_max_count_cap(monkeypatch):
    monkeypatch.setattr(strat, "USE_SCALE_IN", True)
    pos = _scale_in_position(scale_ins=[{"at": "x", "count_added": 1.0, "price": 6.6, "reason": "r"}] * strat.SCALE_IN_MAX_COUNT)
    should_add, _ = strat._should_scale_in(  # noqa: SLF001
        pos, _promising_price(pos), now=dt.datetime.now(dt.timezone.utc),
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_add is False


def test_should_scale_in_false_without_original_count_recorded(monkeypatch):
    """A position adopted from exchange reconciliation, or opened before
    this feature existed, has no original_count -- fails closed rather
    than guessing."""
    monkeypatch.setattr(strat, "USE_SCALE_IN", True)
    pos = _position(entry_price=6.60, minutes_ago=15, count=6.0)  # no original_count
    should_add, _ = strat._should_scale_in(  # noqa: SLF001
        pos, _promising_price(pos), now=dt.datetime.now(dt.timezone.utc),
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_add is False


def test_should_scale_in_disabled_in_trailing_mode(monkeypatch):
    monkeypatch.setattr(strat, "USE_SCALE_IN", True)
    monkeypatch.setattr(strat, "USE_TREND_TRAILING_STRATEGY", True)
    pos = _scale_in_position()
    should_add, _ = strat._should_scale_in(  # noqa: SLF001
        pos, _promising_price(pos), now=dt.datetime.now(dt.timezone.utc),
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_add is False


def test_compute_scale_in_count_caps_at_max_total_size_multiple():
    market = {"price": 2.0, "leverage_estimate": 6.0}
    # Already near the cap (original_count=6, max multiple 1.5 => cap 9,
    # already at 8) -- only 1 more contract of room regardless of what a
    # fresh SCALE_IN_SIZE_FRACTION-sized budget could otherwise afford.
    count, detail = strat.compute_scale_in_count(1000.0, market, original_count=6.0, current_count=8.0)
    assert count == 1
    assert detail["max_total_count"] == 9.0


def test_compute_scale_in_count_returns_zero_once_already_at_the_cap():
    market = {"price": 2.0, "leverage_estimate": 6.0}
    count, _ = strat.compute_scale_in_count(1000.0, market, original_count=6.0, current_count=9.0)
    assert count == 0


def test_manage_open_positions_places_a_non_reduce_only_scale_in_order(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "USE_SCALE_IN", True)
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    pos = _position(ticker="KXBTCPERP", entry_price=6.60, minutes_ago=15, count=6.0)
    pos["original_count"] = 6.0
    strat._save_state({  # noqa: SLF001
        "positions": [pos], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    add_price = _promising_price(pos)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=add_price))
    # First call is the pre-decision reconciliation (must match local state
    # exactly); second call is the post-scale-in-order verification, after
    # 1 more contract has been added.
    calls = {"n": 0}

    def fake_positions():
        calls["n"] += 1
        count = "6.00" if calls["n"] == 1 else "7.00"
        return {"positions": [_real_position("KXBTCPERP", count, "6.60")]}

    monkeypatch.setattr(strat, "get_margin_positions", fake_positions)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, macd_hist_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    ))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False, "ticker": ticker})
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 100.0)

    captured = {}

    def fake_create_margin_order(**kwargs):
        captured.update(kwargs)
        return {"order": {"fill_count": "1.00"}}

    monkeypatch.setattr(strat, "create_margin_order", fake_create_margin_order)

    result = strat.manage_open_positions(dry_run=False)
    assert result["ok"] is True
    assert captured["side"] == "bid"
    assert captured.get("reduce_only") is not True

    state = strat._load_state()  # noqa: SLF001
    updated = state["positions"][0]
    assert len(updated.get("scale_ins") or []) == 1
    assert updated["count"] > 6.0  # blended count from the Kalshi re-read


def test_manage_open_positions_does_not_scale_in_when_daily_loss_cap_breached(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "USE_SCALE_IN", True)
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    pos = _position(ticker="KXBTCPERP", entry_price=6.60, minutes_ago=15, count=6.0)
    pos["original_count"] = 6.0
    strat._save_state({  # noqa: SLF001
        "positions": [pos], "realized_pnl_by_date": {strat._today_str(): -1000.0},  # noqa: SLF001
        "trade_log": [], "daily_reference_balance": {strat._today_str(): 100.0},  # noqa: SLF001
    })
    add_price = _promising_price(pos)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=add_price))
    monkeypatch.setattr(strat, "get_margin_positions", lambda: {
        "positions": [_real_position("KXBTCPERP", "6.00", "6.60")],
    })
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, macd_hist_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    ))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False, "ticker": ticker})

    def fail_if_called(**kwargs):
        raise AssertionError("must not place a scale-in order while the daily loss cap is breached")

    monkeypatch.setattr(strat, "create_margin_order", fail_if_called)
    result = strat.manage_open_positions(dry_run=False)
    assert result["ok"] is True
    state = strat._load_state()  # noqa: SLF001
    assert (state["positions"][0].get("scale_ins") or []) == []


def test_manage_open_positions_does_not_scale_in_in_the_same_cycle_as_an_exit(monkeypatch, tmp_path):
    """A position that's exiting this cycle must never also get a scale-in
    order -- decide_exit unconditionally wins."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "USE_SCALE_IN", True)
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    pos = _position(ticker="KXBTCPERP", entry_price=6.60, minutes_ago=15, count=6.0)
    pos["original_count"] = 6.0
    strat._save_state({  # noqa: SLF001
        "positions": [pos], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    # Priced well past take_profit -- decide_exit fires a full close.
    exit_price = pos["entry_price"] * (1 + strat.TAKE_PROFIT_PCT + 0.01)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=exit_price))
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, macd_hist_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    ))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False, "ticker": ticker})

    def fail_if_scale_in_attempted(**kwargs):
        if not kwargs.get("reduce_only"):
            raise AssertionError("must not place a non-reduce_only (scale-in) order in the same cycle as an exit")
        return {"order": {"fill_count": str(kwargs["count"])}}

    monkeypatch.setattr(strat, "create_margin_order", fail_if_scale_in_attempted)
    result = strat.manage_open_positions(dry_run=False)
    assert result["action"] == "closed"


def test_manage_open_positions_posts_a_scale_in_threads_message(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "USE_SCALE_IN", True)
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    pos = _position(ticker="KXBTCPERP", entry_price=6.60, minutes_ago=15, count=6.0)
    pos["original_count"] = 6.0
    strat._save_state({  # noqa: SLF001
        "positions": [pos], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    add_price = _promising_price(pos)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=add_price))
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 100.0)
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, macd_hist_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    ))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False, "ticker": ticker})

    posted = {}
    monkeypatch.setattr(strat.threads_post, "post_scale_in", lambda **kw: posted.update(kw) or True)

    result = strat.manage_open_positions(dry_run=True)
    assert result["ok"] is True
    assert posted.get("ticker") == "KXBTCPERP"


def test_scan_and_enter_technical_only_fallback_gets_no_conviction_size_bonus(monkeypatch, tmp_path):
    """Regression guard: a technical-only-fallback candidate has no
    entry_confidence/effective_confidence_min (see evaluate_candidate) --
    USE_CONVICTION_SIZING must leave its size_multiplier at 1.0, never try
    to interpret its unrelated score as a confidence reading."""
    monkeypatch.setattr(strat, "USE_CONVICTION_SIZING", True)
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response())
    monkeypatch.setattr(strat, "_available_balance_usd", lambda: 10.0)
    monkeypatch.setattr(
        strat, "scan_for_entries",
        lambda tickers=None, exclude=None, confidence_min=None, correlation_study_enabled=None, correlation_max_adjustment=None: (
            [{"ticker": "KXBTCPERP", "current_price": 6.60, "reason": "technical-only fallback", "score": 0.0165}], [],
        ),
    )
    result = strat.scan_and_enter(dry_run=True)
    assert result["opened"][0]["sizing"]["size_multiplier"] == 1.0


# ── Partial exit (USE_PARTIAL_EXIT) ──────────────────────────────────────────

def _tp_price(pos, extra_pct=0.001):
    exit_pcts = strat.adaptive_exit_pcts(pos.get("entry_volatility_30"))
    return pos["entry_price"] * (1 + exit_pcts["take_profit_pct"] + extra_pct)


def test_decide_exit_returns_full_take_profit_when_partial_exit_disabled():
    pos = _position(count=strat.MIN_COUNT_FOR_PARTIAL_EXIT)
    should_exit, reason = strat.decide_exit(
        pos, _tp_price(pos),
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_exit
    assert reason.startswith("take_profit")


def test_decide_exit_returns_partial_take_profit_when_promising_and_not_yet_taken(monkeypatch):
    monkeypatch.setattr(strat, "USE_PARTIAL_EXIT", True)
    pos = _position(count=strat.MIN_COUNT_FOR_PARTIAL_EXIT)
    should_exit, reason = strat.decide_exit(
        pos, _tp_price(pos),
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_exit
    assert reason.startswith("partial_take_profit")


def test_decide_exit_returns_full_take_profit_when_not_promising(monkeypatch):
    monkeypatch.setattr(strat, "USE_PARTIAL_EXIT", True)
    pos = _position(count=strat.MIN_COUNT_FOR_PARTIAL_EXIT)
    should_exit, reason = strat.decide_exit(pos, _tp_price(pos))  # no continuation signals at all
    assert should_exit
    assert reason.startswith("take_profit")
    assert not reason.startswith("partial_take_profit")


def test_decide_exit_closes_the_remainder_fully_via_the_tightened_stop_after_retracement(monkeypatch):
    """Once a partial exit has been taken, the remainder is governed by the
    tightened, locked-in-profit stop (see adaptive_exit_pcts), not the
    ordinary take_profit level -- a retracement down to that tightened
    stop closes the WHOLE remainder via the stop_loss path, never
    partial_take_profit again (capped at one partial exit per position)."""
    monkeypatch.setattr(strat, "USE_PARTIAL_EXIT", True)
    pos = _position(count=strat.MIN_COUNT_FOR_PARTIAL_EXIT)
    pos["partial_exit_taken"] = True
    tightened = strat.adaptive_exit_pcts(None, partial_exit_taken=True)
    price = pos["entry_price"] * (1 - tightened["stop_loss_pct"] - 0.0005)
    should_exit, reason = strat.decide_exit(pos, price)
    assert should_exit
    assert reason.startswith("stop_loss")


def test_decide_exit_does_not_partial_exit_below_min_count(monkeypatch):
    monkeypatch.setattr(strat, "USE_PARTIAL_EXIT", True)
    pos = _position(count=strat.MIN_COUNT_FOR_PARTIAL_EXIT - 1)
    should_exit, reason = strat.decide_exit(
        pos, _tp_price(pos),
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, momentum_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    )
    assert should_exit
    assert reason.startswith("take_profit")
    assert not reason.startswith("partial_take_profit")


def test_decide_exit_a_partial_exited_position_does_not_immediately_re_trigger_take_profit(monkeypatch):
    """Regression guard for the bug caught during implementation: once
    partial_exit_taken is set, change_pct is STILL at/above take_profit_pct
    on the very next check (price hasn't moved) -- the ordinary take_profit
    branch must be skipped entirely, not just its partial sub-branch, or
    the remainder would get fully closed moments after the partial exit."""
    monkeypatch.setattr(strat, "USE_PARTIAL_EXIT", True)
    pos = _position(count=strat.MIN_COUNT_FOR_PARTIAL_EXIT)
    pos["partial_exit_taken"] = True
    should_exit, reason = strat.decide_exit(pos, _tp_price(pos))  # price unchanged since the partial exit
    assert not should_exit
    assert "holding" in reason


def test_decide_exit_stop_loss_stays_full_exit_even_with_partial_exit_enabled(monkeypatch):
    """Stop-loss is always a full close on purpose -- partial exit only
    ever applies to the take_profit path (see USE_PARTIAL_EXIT's own
    comment)."""
    monkeypatch.setattr(strat, "USE_PARTIAL_EXIT", True)
    pos = _position(count=strat.MIN_COUNT_FOR_PARTIAL_EXIT)
    should_exit, reason = strat.decide_exit(pos, 6.60 * (1 - strat.STOP_LOSS_PCT - 0.001))
    assert should_exit
    assert reason.startswith("stop_loss")


def test_decide_exit_quick_profit_stays_full_exit_even_with_partial_exit_enabled(monkeypatch):
    monkeypatch.setattr(strat, "USE_PARTIAL_EXIT", True)
    pos = _position(count=strat.MIN_COUNT_FOR_PARTIAL_EXIT)
    price = 6.60 * (1 + strat.QUICK_PROFIT_PCT + 0.001)
    should_exit, reason = strat.decide_exit(pos, price, velocity_pct_per_min=strat.QUICK_PROFIT_VELOCITY_PCT_PER_MIN + 0.01)
    assert should_exit
    assert reason.startswith("quick_profit")


def test_adaptive_exit_pcts_tightens_stop_after_partial_exit():
    normal = strat.adaptive_exit_pcts(None)
    tightened = strat.adaptive_exit_pcts(None, partial_exit_taken=True)
    assert tightened["stop_loss_pct"] < 0  # flips decide_exit's check to a locked-in-PROFIT floor
    assert tightened["stop_loss_pct"] == pytest.approx(-strat.PARTIAL_EXIT_STOP_LOCK_FRACTION * normal["take_profit_pct"])
    assert tightened["take_profit_pct"] == normal["take_profit_pct"]  # only the stop changes


def test_position_exit_levels_reflects_tightened_stop_after_partial_exit():
    pos = _position(entry_price=6.60, count=4.0)
    normal_levels = strat.position_exit_levels(pos)
    pos["partial_exit_taken"] = True
    tightened_levels = strat.position_exit_levels(pos)
    # A negative stop_loss_pct means the stop price sits ABOVE entry for a
    # long -- protecting a real profit instead of only protecting breakeven.
    assert tightened_levels["stop_loss_price"] > pos["entry_price"]
    assert tightened_levels["stop_loss_price"] > normal_levels["stop_loss_price"]


def test_manage_open_positions_closes_only_the_partial_fraction_and_keeps_remainder_open(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "USE_PARTIAL_EXIT", True)
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", True)
    # minutes_ago set so manage_open_positions's lazy promising-signal fetch
    # actually triggers (see the widened held_minutes_check condition) --
    # otherwise latest_feature_row/predict_direction below are never called
    # and decide_exit never sees any continuation signal at all.
    pos = _position(
        ticker="KXBTCPERP", entry_price=6.60, count=strat.MIN_COUNT_FOR_PARTIAL_EXIT * 2.0,
        minutes_ago=strat.MAX_HOLD_MINUTES - strat.PRE_EXIT_STUDY_MINUTES,
    )
    strat._save_state({  # noqa: SLF001
        "positions": [pos], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    tp_price = _tp_price(pos)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=tp_price))
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, macd_hist_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    ))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False, "ticker": ticker})

    expected_close = round(pos["count"] * strat.PARTIAL_EXIT_FRACTION, 6)
    captured = {}

    def fake_create_margin_order(**kwargs):
        captured.update(kwargs)
        return {"order": {"fill_count": str(kwargs["count"])}}

    monkeypatch.setattr(strat, "create_margin_order", fake_create_margin_order)
    # First call is the pre-decision reconciliation (must match local state
    # exactly -- the full pre-partial-exit count); second call is the
    # post-order verification, after the partial close has filled.
    calls = {"n": 0}

    def fake_positions():
        calls["n"] += 1
        count = pos["count"] if calls["n"] == 1 else pos["count"] - expected_close
        return {"positions": [_real_position("KXBTCPERP", str(count), "6.60")]}

    monkeypatch.setattr(strat, "get_margin_positions", fake_positions)

    result = strat.manage_open_positions(dry_run=False)
    assert result["action"] == "closed"
    assert captured["count"] == expected_close
    assert captured["reduce_only"] is True
    assert result["closed"][0]["exit_kind"] == "partial"
    assert result["closed"][0]["remaining_count"] == pytest.approx(pos["count"] - expected_close)

    state = strat._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1
    remainder = state["positions"][0]
    assert remainder["count"] == pytest.approx(pos["count"] - expected_close)
    assert remainder["partial_exit_taken"] is True
    assert remainder["partial_exit_count"] == expected_close


def test_manage_open_positions_full_closes_the_remainder_on_a_second_take_profit_hit(monkeypatch, tmp_path):
    """A position that already had its partial exit closes FULLY on its
    next take_profit-level check (decide_exit skips the partial branch once
    partial_exit_taken is set -- see that function's own comment)."""
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "USE_PARTIAL_EXIT", True)
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    pos = _position(ticker="KXBTCPERP", entry_price=6.60, count=4.0)
    pos["partial_exit_taken"] = True
    strat._save_state({  # noqa: SLF001
        "positions": [pos], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    # Price retraces to the now-tightened, locked-in-profit stop level.
    tightened = strat.adaptive_exit_pcts(None, partial_exit_taken=True)
    price = pos["entry_price"] * (1 - tightened["stop_loss_pct"] - 0.0005)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=price))

    result = strat.manage_open_positions(dry_run=True)
    assert result["action"] == "closed"
    assert result["closed"][0]["count"] == 4.0
    assert result["closed"][0]["exit_kind"] == "full"
    state = strat._load_state()  # noqa: SLF001
    assert state["positions"] == []


def test_manage_open_positions_posts_a_partial_exit_threads_message_not_the_full_exit_post(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(strat, "USE_PARTIAL_EXIT", True)
    monkeypatch.setattr(strat, "LIVE_TRADING_ENABLED", False)
    pos = _position(
        ticker="KXBTCPERP", entry_price=6.60, count=strat.MIN_COUNT_FOR_PARTIAL_EXIT * 2.0,
        minutes_ago=strat.MAX_HOLD_MINUTES - strat.PRE_EXIT_STUDY_MINUTES,
    )
    strat._save_state({  # noqa: SLF001
        "positions": [pos], "realized_pnl_by_date": {}, "trade_log": [], "daily_reference_balance": {},
    })
    tp_price = _tp_price(pos)
    monkeypatch.setattr(strat, "get_margin_market", lambda ticker: _market_response(price=tp_price))
    monkeypatch.setattr(strat, "latest_feature_row", lambda ticker: _row(
        dollar_volume_z=strat.PROMISING_VOLUME_Z + 1, macd_hist_pct=strat.PROMISING_MOMENTUM_PCT + 0.001,
    ))
    monkeypatch.setattr(strat, "predict_direction", lambda ticker: {"model_ok": False, "ticker": ticker})

    def fail_if_called(**kw):
        raise AssertionError("a partial exit must post via post_partial_exit, not the full-close post_trade_exit")

    monkeypatch.setattr(strat.threads_post, "post_trade_exit", fail_if_called)
    posted = {}
    monkeypatch.setattr(strat.threads_post, "post_partial_exit", lambda **kw: posted.update(kw) or True)

    result = strat.manage_open_positions(dry_run=True)
    assert result["action"] == "closed"
    assert posted.get("ticker") == "KXBTCPERP"


# ── Trade-log correctness (exit_kind) ────────────────────────────────────────

def test_batch_trade_analysis_ignores_partial_exit_rows_toward_batch_size(monkeypatch, tmp_path):
    monkeypatch.setattr(strat, "STATE_FILE", tmp_path / "state.json")
    partial_trades = [{"ticker": "KXBTCPERP", "dry_run": False, "exit_kind": "partial", "realized_pnl_usd": 1.0}] * 10
    strat._save_state({  # noqa: SLF001
        "positions": [], "trade_log": partial_trades, "realized_pnl_by_date": {}, "daily_reference_balance": {},
    })

    def fail_if_called(*a, **k):
        raise AssertionError("must not run batch analysis when only partial-exit rows have accumulated")

    monkeypatch.setattr(strat.perps_trade_analysis, "analyze_recent_trade_batch", fail_if_called)
    strat._maybe_run_batch_trade_analysis()  # noqa: SLF001
    # No assertion needed beyond "did not raise" -- fail_if_called above
    # would have raised if the (partial-only) trade count wrongly reached
    # perps_trade_analysis.BATCH_SIZE.
