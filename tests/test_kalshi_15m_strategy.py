"""Strategy for Kalshi's 15-minute event-contract markets. See
kalshi_15m_strategy.py's own module docstring for the hard dry-run floor
(LIVE_TRADING_ENABLED must stay False until this account's own standard-
market order-placement mechanics are verified live) and why settlement
checking is a real, fully-exercisable simulation even in dry-run mode
(market resolution is a public, unauthenticated fact)."""
from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from data import crypto_correlation, kalshi_15m, kalshi_15m_meta_model, kalshi_15m_metals_model, kalshi_15m_model, kalshi_15m_strategy


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path, monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "STATE_FILE", tmp_path / "state.json")


@pytest.fixture(autouse=True)
def _full_entry_universe(monkeypatch):
    """ACTIVE_ENTRY_COINS defaults to GOLD/SILVER/COPPER only in
    production (see its own comment) -- the overwhelming majority of
    tests here exercise the full 14-coin universe via crypto-only mocks
    and don't care about that restriction, so it's reset to the full
    universe by default here; the handful of tests for the restriction
    itself explicitly monkeypatch it back down."""
    monkeypatch.setattr(kalshi_15m_strategy, "ACTIVE_ENTRY_COINS", frozenset(kalshi_15m_strategy.ASSET_SERIES))


@pytest.fixture(autouse=True)
def _no_win_streak_cooldown_by_default(monkeypatch):
    """WIN_STREAK_COOLDOWN_ENABLED defaults to True in production (see its
    own comment) -- a 2-real-win streak is a common, even accidental,
    fixture shape across this file's OTHER tests (graduated concurrency,
    conviction/win-streak sizing, etc.), none of which are testing THIS
    feature, so it's disabled by default here; the dedicated tests for it
    below explicitly re-enable it."""
    monkeypatch.setattr(kalshi_15m_strategy, "WIN_STREAK_COOLDOWN_ENABLED", False)


@pytest.fixture(autouse=True)
def _reset_correlation_caches():
    """crypto_correlation's own study/crypto-df caches are module-level
    globals -- reset before AND after every test in this file (not just
    test_crypto_correlation.py's own suite) so nothing leaks into or out
    of the metals_volume_proxy_confirmed tests below."""
    crypto_correlation._METALS_STUDY = {}  # noqa: SLF001
    crypto_correlation._LATEST_KALSHI_15M_CRYPTO_DF = pd.DataFrame()  # noqa: SLF001
    yield
    crypto_correlation._METALS_STUDY = {}  # noqa: SLF001
    crypto_correlation._LATEST_KALSHI_15M_CRYPTO_DF = pd.DataFrame()  # noqa: SLF001


def _future_close(minutes: float) -> str:
    future = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=minutes)
    return future.isoformat().replace("+00:00", "Z")


def _market(*, ticker: str = "KXBTC15M-1", close_in_minutes: float = 14.0, no_ask: float = 0.54, no_bid: float = 0.53) -> dict:
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


def test_evaluate_candidate_rejects_the_5_to_10_minute_window_real_data_showed_was_worst(monkeypatch):
    """REAL, data-driven adjustment: this account's own first 168 real
    trades, bucketed by hold time, showed entries made with only 5-10
    minutes left in the window (the old 300s/5min floor's own worst
    tail) were clearly the weakest performers (33% win rate) vs the
    10-15min (42%) and 15min+ (46%) buckets. MIN_SECONDS_TO_CLOSE_FOR_ENTRY
    was raised from 300 to 600 specifically to cut this bucket out --
    8 minutes (480s) remaining, which the OLD floor would have allowed,
    must now be rejected."""
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market(close_in_minutes=8.0))
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
    monkeypatch.setattr(kalshi_15m_strategy, "GRADUATED_CONCURRENCY_ENABLED", False)
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
    # Graduated concurrency (default ON, 1 starting slot) would otherwise
    # reject every coin on "max_concurrent_positions" before ever
    # reaching the per-coin check this test is actually about -- not
    # what's under test here.
    monkeypatch.setattr(kalshi_15m_strategy, "GRADUATED_CONCURRENCY_ENABLED", False)
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
    monkeypatch.setattr(kalshi_15m_strategy, "GRADUATED_CONCURRENCY_ENABLED", False)
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setenv("KALSHI_15M_LIVE_TRADING_ENABLED", "1")
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
    monkeypatch.setenv("KALSHI_15M_LIVE_TRADING_ENABLED", "1")
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
    monkeypatch.setenv("KALSHI_15M_LIVE_TRADING_ENABLED", "1")
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


def test_scan_and_enter_trusts_the_fill_count_from_the_create_response_itself(monkeypatch):
    """FOURTH real, live, confirmed bug: found by re-reading Kalshi's own
    create-order-v2 API reference after real positions kept appearing on
    this account (via /api/kalshi15m/real-positions) with zero
    corresponding local record, even AFTER the order_id-unwrap fix above
    was already live. Root cause: the old fill-check made a SEPARATE
    GET /portfolio/orders call immediately after CREATE returned -- a
    real eventual-consistency race against Kalshi's own backend, where a
    genuinely-filled order could still read back as "not found yet" a
    few hundred milliseconds later. Kalshi's own docs confirm CREATE's
    own response already carries fill_count/remaining_count
    synchronously for an IOC order. This test proves the fix: get_orders
    is mocked to return NOTHING matching (simulating that exact race),
    yet the position must still be recorded correctly because the
    create response itself already said it filled."""
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setenv("KALSHI_15M_LIVE_TRADING_ENABLED", "1")
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)
    monkeypatch.setattr(
        kalshi_15m, "create_order",
        lambda **kw: {"order": {"order_id": "race-id", "fill_count_fp": "10.00", "remaining_count_fp": "0.00"}},
    )

    def fail_if_called(ticker=None, status=None):
        raise AssertionError("must not need the separate GET /portfolio/orders call when CREATE already answered")

    monkeypatch.setattr(kalshi_15m, "get_orders", fail_if_called)

    result = kalshi_15m_strategy.scan_and_enter(dry_run=False)

    entered = [c for c in result["checks"] if c.get("action") == "entered"]
    assert len(entered) == 1
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["positions"][0]["count"] == 10.0


def test_scan_and_enter_falls_back_to_get_orders_when_the_create_response_has_no_fill_info(monkeypatch):
    """Defensive fallback still works when a real response genuinely
    doesn't carry fill info in the CREATE response (not the expected
    path for a real IOC order per Kalshi's own docs, but kept safe
    regardless)."""
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setenv("KALSHI_15M_LIVE_TRADING_ENABLED", "1")
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: {"order": {"order_id": "no-fill-info-id"}})
    monkeypatch.setattr(kalshi_15m, "get_orders", lambda ticker=None, status=None: [{"order_id": "no-fill-info-id", "fill_count_fp": "7.00"}])

    result = kalshi_15m_strategy.scan_and_enter(dry_run=False)

    entered = [c for c in result["checks"] if c.get("action") == "entered"]
    assert len(entered) == 1
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["positions"][0]["count"] == 7.0


def test_scan_and_enter_records_a_failed_order_without_opening_a_position(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setenv("KALSHI_15M_LIVE_TRADING_ENABLED", "1")
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


def test_scan_and_enter_blocks_a_real_order_when_the_env_var_was_just_flipped_off(monkeypatch):
    """THIRD real, live, confirmed bug: user report "i shut it off but it
    kept making trades". Root cause: LIVE_TRADING_ENABLED (module
    constant, read from the env var ONCE at import time) doesn't notice a
    live env var flip until this process itself restarts -- and an HF
    Space variable update's own restart isn't instantaneous, so a
    kalshi_15m_cycle run could still fire on the stale cached value
    mid-restart-window. This test simulates exactly that: the CACHED
    LIVE_TRADING_ENABLED constant still reads True (as if this process
    hasn't restarted since being enabled), but the REAL env var has
    already been flipped off -- a real order must NEVER be placed in
    that state."""
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)  # stale cached "still on"
    monkeypatch.delenv("KALSHI_15M_LIVE_TRADING_ENABLED", raising=False)  # real env var: off (unset -> default False)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)
    order_calls = []
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: order_calls.append(kw) or {"order": {"order_id": "o1"}})

    result = kalshi_15m_strategy.scan_and_enter(dry_run=False)

    assert order_calls == []  # the fresh re-check must have blocked every single one
    assert all(c.get("reason") == "live_trading_disabled_fresh_check" for c in result["checks"] if not c.get("ok"))
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["positions"] == []


def test_scan_and_enter_still_places_a_real_order_when_the_env_var_genuinely_agrees(monkeypatch):
    """Sanity check for the test above -- the fresh re-check isn't just
    blocking everything; it correctly ALLOWS a real order through when
    the real env var genuinely says live trading is on."""
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setenv("KALSHI_15M_LIVE_TRADING_ENABLED", "1")
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)
    monkeypatch.setattr(kalshi_15m_strategy, "_account_budget_usd", lambda: 100.0)
    order_calls = []
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: order_calls.append(kw) or {"order": {"order_id": "o1"}})
    monkeypatch.setattr(kalshi_15m, "get_orders", lambda ticker=None, status=None: [{"order_id": "o1", "fill_count_fp": "5.00"}])

    result = kalshi_15m_strategy.scan_and_enter(dry_run=False)

    assert len(order_calls) >= 1
    entered = [c for c in result["checks"] if c.get("action") == "entered"]
    assert len(entered) >= 1


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
    monkeypatch.setenv("KALSHI_15M_LIVE_TRADING_ENABLED", "1")
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
    monkeypatch.setenv("KALSHI_15M_LIVE_TRADING_ENABLED", "1")
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
    monkeypatch.setenv("KALSHI_15M_LIVE_TRADING_ENABLED", "1")
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
    monkeypatch.setenv("KALSHI_15M_LIVE_TRADING_ENABLED", "1")
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
    monkeypatch.setenv("KALSHI_15M_LIVE_TRADING_ENABLED", "1")
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


def test_check_settlements_trade_is_counted_by_the_shared_win_rate_stats_helper(monkeypatch):
    """REAL, LIVE, CONFIRMED BUG this proves fixed: server_common.win_rate_stats
    (the shared helper every dashboard's own win-rate/trade-count numbers
    go through) excludes anything but exit_kind == "full" -- confirmed
    live, this market's own dashboard showed trade_count=0/win_rate=null
    despite 93 real, correctly-recorded trades, because this module used
    to write "settled"/"early" instead."""
    from server_common import win_rate_stats

    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [_position(side="yes", count=10, entry_price=0.45, dry_run=False)],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(kalshi_15m, "get_market", lambda ticker: {"ticker": ticker, "result": "yes"})

    kalshi_15m_strategy.check_settlements()

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    stats = win_rate_stats(state["trade_log"])
    assert stats["trade_count"] == 1
    assert stats["win_count"] == 1
    assert stats["win_rate"] == 1.0


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


def test_asset_series_covers_all_14_assets_with_no_overlap():
    assert len(kalshi_15m_strategy.ASSET_SERIES) == 14
    assert set(kalshi_15m_strategy.ASSET_SERIES) == {
        "BTC", "ETH", "SOL", "XRP", "DOGE", "BCH", "NEAR", "HYPE", "ZEC",
        "GOLD", "SILVER", "COPPER", "PLATINUM", "PALLADIUM",
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
# _normalize_trade_log_exit_kind -- self-healing migration for a REAL,
# LIVE, CONFIRMED bug: trade_log entries written before the exit_kind fix
# persisted "settled"/"early" instead of "full", making 93 real trades
# invisible to server_common.win_rate_stats (every dashboard's own
# win-rate/trade-count numbers).
# ---------------------------------------------------------------------------
def test_normalize_trade_log_exit_kind_fixes_old_settled_entries():
    state = {"trade_log": [{"coin": "BTC", "exit_kind": "settled"}]}
    changed = kalshi_15m_strategy._normalize_trade_log_exit_kind(state)  # noqa: SLF001
    assert changed is True
    assert state["trade_log"][0]["exit_kind"] == "full"
    assert state["trade_log"][0]["close_reason"] == "settlement"


def test_normalize_trade_log_exit_kind_fixes_old_early_entries():
    state = {"trade_log": [{"coin": "BTC", "exit_kind": "early"}]}
    changed = kalshi_15m_strategy._normalize_trade_log_exit_kind(state)  # noqa: SLF001
    assert changed is True
    assert state["trade_log"][0]["exit_kind"] == "full"
    assert state["trade_log"][0]["close_reason"] == "early_exit"


def test_normalize_trade_log_exit_kind_is_a_genuine_no_op_once_already_fixed():
    state = {"trade_log": [{"coin": "BTC", "exit_kind": "full", "close_reason": "settlement"}]}
    changed = kalshi_15m_strategy._normalize_trade_log_exit_kind(state)  # noqa: SLF001
    assert changed is False


def test_load_state_self_heals_old_exit_kind_values_from_disk(monkeypatch):
    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [], "realized_pnl_by_date": {},
        "trade_log": [{"coin": "BTC", "realized_pnl_usd": 1.0, "dry_run": False, "exit_kind": "settled"}],
    })

    from server_common import win_rate_stats
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001

    assert state["trade_log"][0]["exit_kind"] == "full"
    assert win_rate_stats(state["trade_log"])["trade_count"] == 1


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
    # probability_up=0.55 -> side="yes", confidence=0.55. Fails the real
    # yes-adjusted default (0.58 + 0.07 = 0.65) but clears an explicitly
    # LOWERED override (0.45 + 0.07 = 0.52) -- proves the override, not
    # the default, decided this.
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.55})
    assert kalshi_15m_strategy.evaluate_candidate("BTC")["ok"] is False  # sanity: fails under the real default
    result = kalshi_15m_strategy.evaluate_candidate("BTC", confidence_min=0.45)
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
    # probability_up=0.70 -> side="yes"; clears the real yes-adjusted
    # default (0.58 + 0.07 = 0.65).
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.70})
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is True


def test_scan_and_enter_reads_the_confidence_floor_learned_from_real_trade_history(monkeypatch):
    """A real trade-history-driven override in state["tuning"] must take
    priority over the module-level MODEL_CONFIDENCE_MIN default -- the
    whole point of apply_confidence_threshold_override existing."""
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", len(kalshi_15m_strategy.ASSET_SERIES))
    monkeypatch.setattr(kalshi_15m_strategy, "GRADUATED_CONCURRENCY_ENABLED", False)
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


def test_evaluate_candidate_never_calls_the_crypto_correlation_study_for_a_metals_coin(monkeypatch):
    """A metals coin gets its OWN, separate study (metals_correlation_bullishness)
    -- crypto's own perps_correlation_bullishness must never be called for
    one, even though both now feed the same USE_CORRELATION_STUDY nudge."""
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_metals_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    called = {"n": 0}

    def _boom(coin, row=None):
        called["n"] += 1
        raise AssertionError("should never be called for a metals coin")

    monkeypatch.setattr(crypto_correlation, "perps_correlation_bullishness", _boom)
    monkeypatch.setattr(
        crypto_correlation, "metals_correlation_bullishness",
        lambda coin, row=None: {"score": 0.3, "reason": "some metals confirmation", "components": {}},
    )
    result = kalshi_15m_strategy.evaluate_candidate("GOLD")
    assert result["ok"] is True
    assert result["correlation_score"] == 0.3
    assert result["correlation_reason"] == "some metals confirmation"
    assert called["n"] == 0


def test_evaluate_candidate_correlation_confirmation_lowers_the_bar_when_flag_on(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    # probability_up=0.62 -> side="yes", confidence=0.62. The real
    # yes-adjusted default (0.58 + YES_CONFIDENCE_EXTRA_REQUIRED's 0.07 =
    # 0.65) misses by 0.03 -- within CORRELATION_CONFIDENCE_MAX_ADJUSTMENT
    # (0.06), so a maximally bullish correlation reading should be enough
    # to clear it.
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.62})
    monkeypatch.setattr(
        crypto_correlation, "perps_correlation_bullishness",
        lambda coin, row=None: {"score": 1.0, "reason": "max confirmation", "components": {}},
    )
    assert kalshi_15m_strategy.evaluate_candidate("BTC")["ok"] is False  # sanity: fails with the study off

    result = kalshi_15m_strategy.evaluate_candidate("BTC", correlation_study_enabled=True)
    assert result["ok"] is True


def test_evaluate_candidate_correlation_disagreement_raises_the_bar_when_flag_on(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    # probability_up=0.68 -> side="yes", confidence=0.68, which clears the
    # yes-adjusted default (0.58 + 0.07 = 0.65) alone -- a maximally
    # bearish correlation reading raises the bar past it (0.65 + 0.06 =
    # 0.71), so this must now be rejected.
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.68})
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
    monkeypatch.setattr(kalshi_15m_strategy, "GRADUATED_CONCURRENCY_ENABLED", False)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    # probability_up=0.68 -> side="yes" for every coin. Real yes-adjusted
    # floor is 0.58 + 0.07 = 0.65, which 0.68 clears alone.
    _mock_confident_prediction(monkeypatch, probability_up=0.68)
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
    # Every CRYPTO candidate rejected: the disagreement penalty raises its
    # floor to 0.65 + 0.06 = 0.71, which 0.68 misses. Metals coins get
    # their OWN (real, unmocked) metals_correlation_bullishness reading --
    # with no cached metals study and no feature_row in this test, that
    # study contributes a neutral 0.0, so metals still clear the plain
    # 0.65 yes-adjusted floor unaided.
    assert entered.isdisjoint(kalshi_15m.KNOWN_15M_SERIES)
    assert entered == set(kalshi_15m.KNOWN_15M_METALS_SERIES)


def test_scan_and_enter_records_the_entry_correlation_score_on_the_position(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", len(kalshi_15m_strategy.ASSET_SERIES))
    monkeypatch.setattr(kalshi_15m_strategy, "GRADUATED_CONCURRENCY_ENABLED", False)
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
    assert gold_position["entry_correlation_score"] == 0.0  # metals study is genuinely empty in this test process


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
# Volume + price-action confirmation -- per explicit, repeated user
# direction: "we need to use volume studies and current volume need to
# be high to enter... bot need to work on that and price action... enter
# trades on volume times only" / "we need to get on the position only
# when volume is high so we in and out within those kalshi minutes." On
# by default (see USE_VOLUME_CONFIRMATION's own comment); crypto only (no
# volume data exists for metals at all).
# ---------------------------------------------------------------------------
def test_use_volume_confirmation_defaults_to_true():
    assert kalshi_15m_strategy.USE_VOLUME_CONFIRMATION is True


def test_volume_and_price_action_confirmed_fails_open_with_no_feature_row():
    assert kalshi_15m_strategy.volume_and_price_action_confirmed(None)["confirmed"] is True


def test_volume_and_price_action_confirmed_fails_open_with_missing_fields():
    result = kalshi_15m_strategy.volume_and_price_action_confirmed({"rsi_14": 0.5})  # no dollar_volume_z/ret_5m
    assert result["confirmed"] is True
    assert result["reason"] == "volume_or_price_action_data_unavailable"


def test_volume_and_price_action_confirmed_rejects_low_volume():
    row = {"dollar_volume_z": 0.5, "ret_5m": 0.01}  # below the 1.0 default floor
    result = kalshi_15m_strategy.volume_and_price_action_confirmed(row)
    assert result["confirmed"] is False
    assert result["reason"] == "volume_not_high_enough"


def test_volume_and_price_action_confirmed_rejects_a_flat_price():
    row = {"dollar_volume_z": 2.0, "ret_5m": 0.0001}  # high volume, but price hasn't moved
    result = kalshi_15m_strategy.volume_and_price_action_confirmed(row)
    assert result["confirmed"] is False
    assert result["reason"] == "price_action_too_flat"


def test_volume_and_price_action_confirmed_passes_with_both_high_volume_and_real_movement():
    row = {"dollar_volume_z": 2.0, "ret_5m": 0.01}
    result = kalshi_15m_strategy.volume_and_price_action_confirmed(row)
    assert result["confirmed"] is True
    assert result["reason"] == "volume_and_price_action_confirmed"


def test_evaluate_candidate_ignores_volume_confirmation_when_the_flag_is_off(monkeypatch):
    # USE_VOLUME_CONFIRMATION now defaults to True (see its own comment) --
    # explicitly disabled here to prove the flag, not luck, controls this.
    monkeypatch.setattr(kalshi_15m_strategy, "USE_VOLUME_CONFIRMATION", False)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(
        kalshi_15m_model, "predict_direction",
        lambda coin: {"model_ok": True, "probability_up": 0.72, "feature_row": {"dollar_volume_z": -5.0, "ret_5m": 0.0}},
    )
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is True  # low volume + flat price -- would fail if the flag were on


def test_evaluate_candidate_blocks_entry_on_low_volume_when_enabled(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "USE_VOLUME_CONFIRMATION", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(
        kalshi_15m_model, "predict_direction",
        lambda coin: {"model_ok": True, "probability_up": 0.72, "feature_row": {"dollar_volume_z": 0.2, "ret_5m": 0.01}},
    )
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is False
    assert result["reason"] == "volume_not_high_enough"


def test_evaluate_candidate_allows_entry_on_high_volume_and_real_movement_when_enabled(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "USE_VOLUME_CONFIRMATION", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(
        kalshi_15m_model, "predict_direction",
        lambda coin: {"model_ok": True, "probability_up": 0.72, "feature_row": {"dollar_volume_z": 2.5, "ret_5m": 0.01}},
    )
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is True


def test_evaluate_candidate_fails_open_for_a_metals_coin_with_no_correlated_peer_data(monkeypatch):
    """A metals coin now gets metals_volume_proxy_confirmed's own real
    cross-asset proxy (see its own comment) instead of skipping the gate
    entirely -- but with no correlation study data cached yet (this
    test's fresh module state), that proxy has nothing to read and fails
    OPEN, same "a missing signal never blocks a trade" posture as every
    other optional signal here."""
    monkeypatch.setattr(kalshi_15m_strategy, "USE_VOLUME_CONFIRMATION", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_metals_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    result = kalshi_15m_strategy.evaluate_candidate("GOLD")
    assert result["ok"] is True


# ---------------------------------------------------------------------------
# metals_volume_proxy_confirmed -- per explicit, repeated user direction
# ("entry always need to happen on High volume time"): GOLD/SILVER/COPPER
# are this account's entire live entry universe (ACTIVE_ENTRY_COINS), so
# volume_and_price_action_confirmed's own crypto-only scope would
# otherwise make this gate a no-op in production. Proxies real activity
# off this metal's own most-correlated crypto peer's REAL dollar_volume_z.
# (See the autouse _reset_correlation_caches fixture near the top of this
# file for why these tests can freely set crypto_correlation's own
# module-level study/crypto-df caches without leaking into other tests.)
# ---------------------------------------------------------------------------
def test_metals_volume_proxy_confirmed_fails_open_with_no_feature_row():
    result = kalshi_15m_strategy.metals_volume_proxy_confirmed("GOLD", None)
    assert result["confirmed"] is True
    assert result["reason"] == "volume_proxy_or_price_action_data_unavailable"


def test_metals_volume_proxy_confirmed_fails_open_with_no_correlated_peers():
    crypto_correlation._METALS_STUDY = {"corr": {}}  # noqa: SLF001
    result = kalshi_15m_strategy.metals_volume_proxy_confirmed("GOLD", {"ret_5m": 0.01})
    assert result["confirmed"] is True


def test_metals_volume_proxy_confirmed_ignores_a_metals_only_peer():
    # Correlated only with another metal (SILVER) -- never a valid volume
    # proxy source (no real volume data exists for any metal).
    crypto_correlation._METALS_STUDY = {"corr": {"GOLD": {"SILVER": 0.9}}}  # noqa: SLF001
    result = kalshi_15m_strategy.metals_volume_proxy_confirmed("GOLD", {"ret_5m": 0.01})
    assert result["confirmed"] is True
    assert result["reason"] == "volume_proxy_or_price_action_data_unavailable"


def test_metals_volume_proxy_confirmed_reads_the_most_correlated_crypto_peers_volume():
    crypto_correlation._METALS_STUDY = {"corr": {"GOLD": {"ETH": 0.3, "BTC": 0.8}}}  # noqa: SLF001
    crypto_correlation._LATEST_KALSHI_15M_CRYPTO_DF = pd.DataFrame({  # noqa: SLF001
        "symbol": ["BTC", "ETH"], "ts": [1, 1], "dollar_volume_z": [2.5, 0.1],
    })
    result = kalshi_15m_strategy.metals_volume_proxy_confirmed("GOLD", {"ret_5m": 0.01})
    assert result["confirmed"] is True
    assert result["dollar_volume_z"] == pytest.approx(2.5)  # BTC (highest |corr|), not ETH


def test_metals_volume_proxy_confirmed_rejects_low_peer_volume():
    crypto_correlation._METALS_STUDY = {"corr": {"GOLD": {"BTC": 0.8}}}  # noqa: SLF001
    crypto_correlation._LATEST_KALSHI_15M_CRYPTO_DF = pd.DataFrame({  # noqa: SLF001
        "symbol": ["BTC"], "ts": [1], "dollar_volume_z": [0.2],
    })
    result = kalshi_15m_strategy.metals_volume_proxy_confirmed("GOLD", {"ret_5m": 0.01})
    assert result["confirmed"] is False
    assert result["reason"] == "volume_proxy_not_high_enough"


def test_metals_volume_proxy_confirmed_rejects_a_flat_price():
    crypto_correlation._METALS_STUDY = {"corr": {"GOLD": {"BTC": 0.8}}}  # noqa: SLF001
    crypto_correlation._LATEST_KALSHI_15M_CRYPTO_DF = pd.DataFrame({  # noqa: SLF001
        "symbol": ["BTC"], "ts": [1], "dollar_volume_z": [2.5],
    })
    result = kalshi_15m_strategy.metals_volume_proxy_confirmed("GOLD", {"ret_5m": 0.0001})
    assert result["confirmed"] is False
    assert result["reason"] == "price_action_too_flat"


def test_evaluate_candidate_uses_the_metals_volume_proxy_when_enabled(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "USE_VOLUME_CONFIRMATION", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(
        kalshi_15m_metals_model, "predict_direction",
        lambda coin: {"model_ok": True, "probability_up": 0.72, "feature_row": {"ret_5m": 0.0001}},
    )
    crypto_correlation._METALS_STUDY = {"corr": {"GOLD": {"BTC": 0.8}}}  # noqa: SLF001
    crypto_correlation._LATEST_KALSHI_15M_CRYPTO_DF = pd.DataFrame({  # noqa: SLF001
        "symbol": ["BTC"], "ts": [1], "dollar_volume_z": [2.5],
    })
    result = kalshi_15m_strategy.evaluate_candidate("GOLD")
    assert result["ok"] is False
    assert result["reason"] == "price_action_too_flat"  # high peer volume, but GOLD's own price is flat


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
    # probability_up=0.72 -> side="yes", so the reported floor includes the
    # yes-side surcharge (see YES_CONFIDENCE_EXTRA_REQUIRED's own comment).
    assert result["effective_confidence_min"] == pytest.approx(
        kalshi_15m_strategy.MODEL_CONFIDENCE_MIN + kalshi_15m_strategy.YES_CONFIDENCE_EXTRA_REQUIRED,
    )


# ---------------------------------------------------------------------------
# Per-side confidence floor -- a real, evidence-driven correction (see
# YES_CONFIDENCE_EXTRA_REQUIRED's own comment): this account's own first
# 319 real trades showed "no" decisions winning far more often than "yes"
# decisions, so "yes" alone must now clear a meaningfully higher bar.
# ---------------------------------------------------------------------------
def test_yes_confidence_extra_required_raises_the_bar_for_a_yes_decision_only(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    # probability_up=0.60 -> side="yes", confidence=0.60. Clears the plain
    # module default (0.58) but not once the yes-side surcharge (0.07) is
    # added (0.58 + 0.07 = 0.65).
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.60})
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is False
    assert result["reason"] == "confidence_below_floor"


def test_yes_confidence_extra_required_does_not_affect_a_no_decision(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    # probability_up=0.40 -> side="no", confidence=0.60 -- the identical
    # confidence to the "yes" case above, but "no" gets no surcharge, so
    # this must clear the plain 0.58 default and enter.
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.40})
    result = kalshi_15m_strategy.evaluate_candidate("BTC")
    assert result["ok"] is True
    assert result["side"] == "no"
    assert result["effective_confidence_min"] == pytest.approx(kalshi_15m_strategy.MODEL_CONFIDENCE_MIN)


def test_yes_confidence_extra_required_applies_before_the_correlation_study_nudge(monkeypatch):
    """A maximally bullish correlation reading should nudge off the
    already-side-corrected 0.65 baseline (-> 0.59), not the shared 0.58 --
    see YES_CONFIDENCE_EXTRA_REQUIRED's own comment on ordering."""
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    # probability_up=0.60 -> side="yes", confidence=0.60. Misses the plain
    # yes-adjusted floor (0.65) by 0.05 -- within the 0.06 max correlation
    # adjustment, so max bullish confirmation should be just enough.
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.60})
    monkeypatch.setattr(
        crypto_correlation, "perps_correlation_bullishness",
        lambda coin, row=None: {"score": 1.0, "reason": "max confirmation", "components": {}},
    )
    result = kalshi_15m_strategy.evaluate_candidate("BTC", correlation_study_enabled=True)
    assert result["ok"] is True
    assert result["effective_confidence_min"] == pytest.approx(0.59)


# ---------------------------------------------------------------------------
# Permanent entry-universe narrowing -- per explicit user direction ("GOLD/
# SILVER/COPPER let['s] boost all our focus on them"). Only gates NEW
# entries; ASSET_SERIES itself (data collection, correlation study,
# existing-position management) stays the full universe.
# ---------------------------------------------------------------------------
def test_active_entry_coins_env_default_string_is_gold_silver_copper():
    """Documents the literal default string ACTIVE_ENTRY_COINS is built
    from -- a regression guard against an accidental typo/reorder in that
    literal, independent of the autouse _full_entry_universe fixture's
    own test-time override above (which replaces the ATTRIBUTE, not the
    source this asserts against)."""
    import inspect
    source = inspect.getsource(kalshi_15m_strategy)
    assert 'os.getenv("KALSHI_15M_ACTIVE_ENTRY_COINS", "GOLD,SILVER,COPPER")' in source


def test_scan_and_enter_skips_a_coin_outside_the_active_entry_universe(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "ACTIVE_ENTRY_COINS", frozenset({"GOLD", "SILVER", "COPPER"}))
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", len(kalshi_15m_strategy.ASSET_SERIES))
    monkeypatch.setattr(kalshi_15m_strategy, "GRADUATED_CONCURRENCY_ENABLED", False)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)

    result = kalshi_15m_strategy.scan_and_enter()

    excluded_checks = {c["coin"]: c for c in result["checks"] if c["coin"] in kalshi_15m.KNOWN_15M_SERIES}
    for coin, check in excluded_checks.items():
        assert check["ok"] is False
        assert check["reason"] == "coin_outside_active_entry_universe"
    # PLATINUM/PALLADIUM (metals, but outside the default 3) are excluded too.
    for coin in ("PLATINUM", "PALLADIUM"):
        check = next(c for c in result["checks"] if c["coin"] == coin)
        assert check["reason"] == "coin_outside_active_entry_universe"
    # GOLD/SILVER/COPPER (inside the universe) proceed to a real decision --
    # none of them get the exclusion reason.
    for coin in ("GOLD", "SILVER", "COPPER"):
        check = next(c for c in result["checks"] if c["coin"] == coin)
        assert check.get("reason") != "coin_outside_active_entry_universe"


def test_scan_and_enter_active_entry_universe_does_not_block_an_existing_open_position(monkeypatch):
    """An already-open position on an excluded coin is untouched by this
    gate -- it's still reported/managed via _has_open_position's own
    check, not silently abandoned."""
    monkeypatch.setattr(kalshi_15m_strategy, "ACTIVE_ENTRY_COINS", frozenset({"GOLD", "SILVER", "COPPER"}))
    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [{
            "coin": "BTC", "side": "yes", "count": 1.0, "entry_price": 0.5, "dry_run": True,
            "ticker": "KXBTC15M-1", "close_time": _future_close(10), "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)

    result = kalshi_15m_strategy.scan_and_enter()

    btc_check = next(c for c in result["checks"] if c["coin"] == "BTC")
    # Whichever reason wins (the universe gate fires first in the real
    # loop order, ahead of already_has_open_position) doesn't matter here
    # -- the point is BTC is never silently dropped from the checks list,
    # and never treated as a fresh "entered" candidate.
    assert btc_check["ok"] is False
    entered = [c["coin"] for c in result["checks"] if c.get("action") == "entered"]
    assert "BTC" not in entered


# ---------------------------------------------------------------------------
# Real-trade-outcome probability recalibration -- per explicit user
# direction ("give me suggestion[s]... to get super consistant at
# winning"), responding to this account's own real confidence-bucket
# data showing the model's stated confidence was INVERTED (higher
# confidence = LOWER real accuracy).
# ---------------------------------------------------------------------------
def _real_trade(probability_up: float, *, won: bool) -> dict:
    return {"dry_run": False, "entry_probability_up": probability_up, "realized_pnl_usd": 1.0 if won else -1.0}


def test_calibrate_probability_up_returns_raw_with_insufficient_history():
    trade_log = [_real_trade(0.7, won=True)] * 5  # far fewer than the real 150-trade default
    result = kalshi_15m_strategy.calibrate_probability_up(0.72, trade_log)
    assert result == {
        "probability_up": 0.72, "applied": False, "reason": "insufficient_real_trade_history", "real_trades": 5,
    }


def test_calibrate_probability_up_with_no_trade_log_returns_raw():
    result = kalshi_15m_strategy.calibrate_probability_up(0.72, None)
    assert result["probability_up"] == 0.72
    assert result["applied"] is False


def test_calibrate_probability_up_ignores_dry_run_trades(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "REAL_OUTCOME_CALIBRATION_MIN_TRADES", 5)
    real = [_real_trade(0.7, won=True)] * 3
    dry = [{**_real_trade(0.7, won=True), "dry_run": True}] * 10  # would clear the floor if wrongly counted
    result = kalshi_15m_strategy.calibrate_probability_up(0.72, real + dry)
    assert result["applied"] is False
    assert result["real_trades"] == 3


def test_calibrate_probability_up_ignores_trades_missing_required_fields(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "REAL_OUTCOME_CALIBRATION_MIN_TRADES", 5)
    real = [_real_trade(0.7, won=True)] * 3
    missing_prob = [{"dry_run": False, "realized_pnl_usd": 1.0}] * 10  # no entry_probability_up
    missing_pnl = [{"dry_run": False, "entry_probability_up": 0.7}] * 10  # no realized_pnl_usd
    result = kalshi_15m_strategy.calibrate_probability_up(0.72, real + missing_prob + missing_pnl)
    assert result["applied"] is False
    assert result["real_trades"] == 3


def test_calibrate_probability_up_corrects_a_real_overconfidence_pattern(monkeypatch):
    """This account's own real signature: LOWER stated confidence (0.6)
    actually won every time, HIGHER stated confidence (0.9) actually lost
    every time -- isotonic regression must pool that inversion rather
    than trust the raw ordering, collapsing both into a single ~0.5
    (coin-flip) region."""
    monkeypatch.setattr(kalshi_15m_strategy, "REAL_OUTCOME_CALIBRATION_MIN_TRADES", 10)
    trade_log = [_real_trade(0.6, won=True) for _ in range(5)] + [_real_trade(0.9, won=False) for _ in range(5)]
    result = kalshi_15m_strategy.calibrate_probability_up(0.75, trade_log)
    assert result["applied"] is True
    assert result["reason"] == "real_outcome_calibrated"
    assert result["raw_probability_up"] == 0.75
    assert result["probability_up"] == pytest.approx(0.5, abs=0.05)  # far from the raw 0.75


def test_calibrate_probability_up_handles_a_genuinely_well_calibrated_history(monkeypatch):
    """The non-adversarial case: low stated confidence actually loses,
    high stated confidence actually wins -- isotonic regression should
    leave that ordering close to untouched, not distort a model that's
    already reading correctly."""
    monkeypatch.setattr(kalshi_15m_strategy, "REAL_OUTCOME_CALIBRATION_MIN_TRADES", 10)
    trade_log = [_real_trade(0.55, won=False) for _ in range(5)] + [_real_trade(0.9, won=True) for _ in range(5)]
    result = kalshi_15m_strategy.calibrate_probability_up(0.9, trade_log)
    assert result["applied"] is True
    assert result["probability_up"] > 0.5


def test_evaluate_candidate_applies_real_outcome_calibration_when_trade_log_given(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "REAL_OUTCOME_CALIBRATION_MIN_TRADES", 10)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    # A genuinely well-calibrated (non-adversarial) real history that
    # still shifts a fresh 0.80 raw reading -- picked so the CALIBRATED
    # value still comfortably clears the yes-adjusted confidence floor
    # (0.65), so this test exercises the full success path, not just an
    # early confidence_below_floor rejection (see the dedicated
    # calibrate_probability_up unit tests above for the dramatic
    # overconfidence-collapse case).
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.80})
    trade_log = [_real_trade(0.65, won=False) for _ in range(5)] + [_real_trade(0.85, won=True) for _ in range(5)]

    result = kalshi_15m_strategy.evaluate_candidate("BTC", trade_log=trade_log)

    assert result["ok"] is True
    assert result["raw_probability_up"] == pytest.approx(0.80)
    assert result["real_outcome_calibration_applied"] is True
    assert result["probability_up"] != result["raw_probability_up"]


def test_evaluate_candidate_skips_calibration_without_a_trade_log(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})

    result = kalshi_15m_strategy.evaluate_candidate("BTC")

    assert result["real_outcome_calibration_applied"] is False
    assert result["probability_up"] == result["raw_probability_up"] == pytest.approx(0.72)


def test_evaluate_candidate_use_real_outcome_calibration_override_disables_it(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "REAL_OUTCOME_CALIBRATION_MIN_TRADES", 10)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.75})
    trade_log = [_real_trade(0.6, won=True) for _ in range(5)] + [_real_trade(0.9, won=False) for _ in range(5)]

    result = kalshi_15m_strategy.evaluate_candidate("BTC", trade_log=trade_log, use_real_outcome_calibration=False)

    assert result["real_outcome_calibration_applied"] is False
    assert result["probability_up"] == result["raw_probability_up"] == pytest.approx(0.75)


def test_use_real_outcome_calibration_defaults_to_true():
    assert kalshi_15m_strategy.USE_REAL_OUTCOME_CALIBRATION is True


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
    # At near-max conviction (probability_up=0.99 vs a yes-adjusted 0.65
    # floor -- 0.58 default + YES_CONFIDENCE_EXTRA_REQUIRED's 0.07), the
    # multiplier lands near CONVICTION_SIZE_MAX_MULTIPLIER (1.5x) -> 14+.
    assert entered["count"] > 10
    assert entered["entry_conviction_sizing_enabled"] is True


def test_scan_and_enter_records_conviction_sizing_disabled_by_default(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    kalshi_15m_strategy.scan_and_enter()
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["positions"][0]["entry_conviction_sizing_enabled"] is False


# ---------------------------------------------------------------------------
# Per-symbol loss-streak throttle -- per explicit user direction: "if it's
# keep losing a certain symbol[,] retain[,] redo and reduce size until it
# start[s] getting strikes." Always on (a pure risk-REDUCER).
# ---------------------------------------------------------------------------
def _loss_streak_trade(*, coin="BTC", pnl, dry_run=False):
    return {"coin": coin, "realized_pnl_usd": pnl, "dry_run": dry_run}


def test_loss_streak_multiplier_is_1_with_insufficient_history():
    trades = [_loss_streak_trade(pnl=-1.0), _loss_streak_trade(pnl=-1.0)]  # only 2, floor is 3
    assert kalshi_15m_strategy.compute_loss_streak_size_multiplier("BTC", trades) == 1.0


def test_loss_streak_multiplier_throttles_after_3_consecutive_real_losses():
    trades = [_loss_streak_trade(pnl=-1.0) for _ in range(3)]
    result = kalshi_15m_strategy.compute_loss_streak_size_multiplier("BTC", trades)
    assert result == kalshi_15m_strategy.LOSS_STREAK_SIZE_MULTIPLIER
    assert result < 1.0


def test_loss_streak_multiplier_resets_the_moment_a_real_win_lands():
    trades = [_loss_streak_trade(pnl=-1.0), _loss_streak_trade(pnl=-1.0), _loss_streak_trade(pnl=1.0)]
    assert kalshi_15m_strategy.compute_loss_streak_size_multiplier("BTC", trades) == 1.0


def test_loss_streak_multiplier_ignores_dry_run_trades():
    trades = [_loss_streak_trade(pnl=-1.0, dry_run=True) for _ in range(5)]
    assert kalshi_15m_strategy.compute_loss_streak_size_multiplier("BTC", trades) == 1.0


def test_loss_streak_multiplier_is_coin_specific():
    trades = [_loss_streak_trade(coin="BTC", pnl=-1.0) for _ in range(3)]
    assert kalshi_15m_strategy.compute_loss_streak_size_multiplier("BTC", trades) < 1.0
    assert kalshi_15m_strategy.compute_loss_streak_size_multiplier("ETH", trades) == 1.0


# ---------------------------------------------------------------------------
# Per-symbol WIN-streak size increase -- per explicit user direction:
# "position increase only when its consistent win after win then we
# increase the position sizes." Off by default (unlike the loss-streak
# throttle) -- this one INCREASES exposure, so it needs real evidence
# first, same posture as conviction sizing/correlation study/meta-model.
# ---------------------------------------------------------------------------
def test_win_streak_multiplier_is_1_with_insufficient_history():
    trades = [_loss_streak_trade(pnl=1.0), _loss_streak_trade(pnl=1.0)]  # only 2, floor is 3
    assert kalshi_15m_strategy.compute_win_streak_size_multiplier("BTC", trades) == 1.0


def test_win_streak_multiplier_grows_after_3_consecutive_real_wins():
    trades = [_loss_streak_trade(pnl=1.0) for _ in range(3)]
    result = kalshi_15m_strategy.compute_win_streak_size_multiplier("BTC", trades)
    assert result == kalshi_15m_strategy.WIN_STREAK_SIZE_MULTIPLIER
    assert result > 1.0


def test_win_streak_multiplier_resets_the_moment_a_real_loss_lands():
    trades = [_loss_streak_trade(pnl=1.0), _loss_streak_trade(pnl=1.0), _loss_streak_trade(pnl=-1.0)]
    assert kalshi_15m_strategy.compute_win_streak_size_multiplier("BTC", trades) == 1.0


def test_win_streak_multiplier_ignores_dry_run_trades():
    trades = [_loss_streak_trade(pnl=1.0, dry_run=True) for _ in range(5)]
    assert kalshi_15m_strategy.compute_win_streak_size_multiplier("BTC", trades) == 1.0


def test_win_streak_multiplier_is_coin_specific():
    trades = [_loss_streak_trade(coin="BTC", pnl=1.0) for _ in range(3)]
    assert kalshi_15m_strategy.compute_win_streak_size_multiplier("BTC", trades) > 1.0
    assert kalshi_15m_strategy.compute_win_streak_size_multiplier("ETH", trades) == 1.0


def test_scan_and_enter_does_not_grow_contracts_after_a_win_streak_when_the_flag_is_off(monkeypatch):
    assert kalshi_15m_strategy.USE_WIN_STREAK_SIZING is False  # module default -- not touched by this test
    # This test's own 3-real-win fixture is exactly what WIN_STREAK_COOLDOWN
    # would otherwise pause entries for (see its own tests below) -- not
    # what this test is about, so disabled here.
    monkeypatch.setattr(kalshi_15m_strategy, "WIN_STREAK_COOLDOWN_ENABLED", False)
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market(no_ask=0.51, no_bid=0.5))
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    monkeypatch.setattr(crypto_correlation, "perps_correlation_bullishness", lambda coin, row=None: {"score": 0.0, "reason": "neutral", "components": {}})
    winning_trades = [{"coin": "BTC", "realized_pnl_usd": 1.0, "dry_run": False} for _ in range(3)]
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": winning_trades, "realized_pnl_by_date": {}})  # noqa: SLF001

    kalshi_15m_strategy.scan_and_enter()

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    btc_position = state["positions"][0]
    assert btc_position["count"] == 10  # int(100 * 0.05 / 0.5) -- unchanged, flag is off
    assert btc_position["entry_win_streak_multiplier"] == 1.0


def test_scan_and_enter_grows_contracts_after_a_real_winning_streak_when_enabled(monkeypatch):
    # See the sibling test above -- same reasoning.
    monkeypatch.setattr(kalshi_15m_strategy, "WIN_STREAK_COOLDOWN_ENABLED", False)
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market(no_ask=0.51, no_bid=0.5))
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    monkeypatch.setattr(crypto_correlation, "perps_correlation_bullishness", lambda coin, row=None: {"score": 0.0, "reason": "neutral", "components": {}})
    winning_trades = [{"coin": "BTC", "realized_pnl_usd": 1.0, "dry_run": False} for _ in range(3)]
    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [], "trade_log": winning_trades, "realized_pnl_by_date": {},
        "tuning": {"win_streak_sizing_enabled": True},
    })

    kalshi_15m_strategy.scan_and_enter()

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    btc_position = state["positions"][0]
    assert btc_position["count"] > 10  # grown above the un-multiplied 10
    assert btc_position["entry_win_streak_multiplier"] == kalshi_15m_strategy.WIN_STREAK_SIZE_MULTIPLIER


def test_apply_win_streak_sizing_override_persists_the_flag():
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001
    applied = kalshi_15m_strategy.apply_win_streak_sizing_override(enabled=True, reason="test evidence")
    assert applied["win_streak_sizing_enabled"] is True
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["tuning"]["win_streak_sizing_enabled"] is True


# ---------------------------------------------------------------------------
# Entry feature snapshot -- per explicit user direction: "this need to
# remember what lead to a trade and study it... with all indicator and
# times frames."
# ---------------------------------------------------------------------------
def test_clean_feature_snapshot_returns_none_for_missing_input():
    assert kalshi_15m_strategy._clean_feature_snapshot(None) is None  # noqa: SLF001
    assert kalshi_15m_strategy._clean_feature_snapshot({}) is None  # noqa: SLF001


def test_clean_feature_snapshot_drops_non_numeric_fields_and_coerces_floats():
    row = {"symbol": "BTC", "ret_5m": 0.01, "trend_1h": "0.02", "rsi_14": 0.6, "junk": "not a number"}
    result = kalshi_15m_strategy._clean_feature_snapshot(row)  # noqa: SLF001
    assert "symbol" not in result
    assert "junk" not in result
    assert result["ret_5m"] == 0.01
    assert result["trend_1h"] == 0.02
    assert result["rsi_14"] == 0.6


def test_scan_and_enter_records_the_entry_feature_snapshot(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    monkeypatch.setattr(
        kalshi_15m_model, "predict_direction",
        lambda coin: {"model_ok": True, "probability_up": 0.72, "feature_row": {"symbol": coin, "ret_5m": 0.02, "rsi_14": 0.7}},
    )
    kalshi_15m_strategy.scan_and_enter()
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    btc_position = next(p for p in state["positions"] if p["coin"] == "BTC")
    assert btc_position["entry_feature_snapshot"] == {"ret_5m": 0.02, "rsi_14": 0.7}


def test_scan_and_enter_shrinks_contracts_after_a_real_losing_streak_on_that_coin(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 1)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market(no_ask=0.51, no_bid=0.5))
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.72})
    monkeypatch.setattr(crypto_correlation, "perps_correlation_bullishness", lambda coin, row=None: {"score": 0.0, "reason": "neutral", "components": {}})
    losing_trades = [{"coin": "BTC", "realized_pnl_usd": -1.0, "dry_run": False} for _ in range(3)]
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": losing_trades, "realized_pnl_by_date": {}})  # noqa: SLF001

    kalshi_15m_strategy.scan_and_enter()

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    btc_position = state["positions"][0]
    assert btc_position["coin"] == "BTC"
    # Without the throttle: int(100 * 0.05 / 0.5) = 10. Throttled at 0.5x: 5.
    assert btc_position["count"] < 10
    assert btc_position["entry_loss_streak_multiplier"] == kalshi_15m_strategy.LOSS_STREAK_SIZE_MULTIPLIER


# ---------------------------------------------------------------------------
# Early exit -- "stay or close" a still-open real position. Off by default
# (USE_EARLY_EXIT); per explicit user direction: "help also determine
# staying or closing the winning position."
# ---------------------------------------------------------------------------
def test_exit_order_side_and_price_for_a_yes_position():
    market = _market(no_ask=0.60, no_bid=0.55)
    side, price = kalshi_15m_strategy._exit_order_side_and_price("yes", market)  # noqa: SLF001
    assert side == "ask"  # sell yes to close a long-yes position
    assert price == pytest.approx(1.0 - 0.60)


def test_exit_order_side_and_price_for_a_no_position():
    market = _market(no_ask=0.60, no_bid=0.55)
    side, price = kalshi_15m_strategy._exit_order_side_and_price("no", market)  # noqa: SLF001
    assert side == "bid"  # buy yes back to close a short-yes ("no") position
    assert price == pytest.approx(1.0 - 0.55)


def test_current_exit_value_for_a_yes_position():
    market = _market(no_ask=0.60, no_bid=0.55)
    assert kalshi_15m_strategy._current_exit_value("yes", market) == pytest.approx(0.40)  # noqa: SLF001


def test_current_exit_value_for_a_no_position():
    market = _market(no_ask=0.60, no_bid=0.55)
    assert kalshi_15m_strategy._current_exit_value("no", market) == pytest.approx(0.55)  # noqa: SLF001


def _open_position(*, coin="BTC", side="yes", entry_price=0.5, ticker="KXBTC15M-1", opened_seconds_ago=300):
    opened_at = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=opened_seconds_ago)).isoformat()
    return {
        "coin": coin, "ticker": ticker, "side": side, "count": 10.0, "entry_price": entry_price,
        "opened_at": opened_at, "close_time": _future_close(5), "entry_probability_up": 0.7,
        "entry_confidence": 0.7, "dry_run": False, "client_order_id": "c1", "order_id": "o1",
        "entry_correlation_score": 0.0, "entry_conviction_sizing_enabled": False,
        "entry_loss_streak_multiplier": 1.0,
    }


def test_decide_early_exit_holds_when_reassessment_unavailable():
    result = kalshi_15m_strategy.decide_early_exit(_open_position(), {"model_ok": False}, current_value=0.6)
    assert result["should_exit"] is False
    assert result["reason"] == "reassessment_not_available"


def test_decide_early_exit_holds_when_thesis_still_agrees():
    position = _open_position(side="yes")
    reassessment = {"model_ok": True, "side": "yes", "confidence": 0.9}
    result = kalshi_15m_strategy.decide_early_exit(position, reassessment, current_value=0.6)
    assert result["should_exit"] is False
    assert result["reason"] == "thesis_still_agrees"


def test_decide_early_exit_holds_when_the_flip_is_too_weak():
    position = _open_position(side="yes")
    reassessment = {"model_ok": True, "side": "no", "confidence": 0.52}  # flip_strength 0.02 < 0.08 default margin
    result = kalshi_15m_strategy.decide_early_exit(position, reassessment, current_value=0.6)
    assert result["should_exit"] is False
    assert result["reason"] == "flip_too_weak"


def test_decide_early_exit_locks_in_profit_on_a_strong_flip_while_winning():
    position = _open_position(side="yes", entry_price=0.5)
    reassessment = {"model_ok": True, "side": "no", "confidence": 0.75}
    result = kalshi_15m_strategy.decide_early_exit(position, reassessment, current_value=0.7)  # currently profitable
    assert result["should_exit"] is True
    assert result["reason"] == "lock_in_profit"
    assert result["unrealized_pnl_per_contract"] == pytest.approx(0.2)


def test_decide_early_exit_cuts_the_loss_on_a_strong_flip_while_losing():
    position = _open_position(side="yes", entry_price=0.5)
    reassessment = {"model_ok": True, "side": "no", "confidence": 0.75}
    result = kalshi_15m_strategy.decide_early_exit(position, reassessment, current_value=0.3)  # currently losing
    assert result["should_exit"] is True
    assert result["reason"] == "cut_loss"


def test_manage_open_positions_skips_dry_run_positions_entirely(monkeypatch):
    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [{**_open_position(), "dry_run": True}], "trade_log": [], "realized_pnl_by_date": {},
    })

    def fail_if_called(series_ticker):
        raise AssertionError("must never look up a market for a dry-run position")

    monkeypatch.setattr(kalshi_15m, "get_current_window_market", fail_if_called)
    result = kalshi_15m_strategy.manage_open_positions()
    assert result["checks"] == []


def test_manage_open_positions_skips_a_position_held_too_briefly(monkeypatch):
    position = _open_position(opened_seconds_ago=5)
    kalshi_15m_strategy._save_state({"positions": [position], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market(ticker=position["ticker"]))

    result = kalshi_15m_strategy.manage_open_positions()

    assert result["checks"][0]["reason"] == "too_early_to_manage"


def test_manage_open_positions_computes_but_never_places_a_real_order_when_the_flag_is_off(monkeypatch):
    assert kalshi_15m_strategy.USE_EARLY_EXIT is False  # module default -- not touched by this test
    position = _open_position(side="yes", entry_price=0.5)
    kalshi_15m_strategy._save_state({"positions": [position], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market(ticker=position["ticker"], no_ask=0.65, no_bid=0.60))
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.2})  # flips hard to "no"
    order_calls = []
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: order_calls.append(kw))

    # dry_run=False in isolation -- the ONLY thing blocking a real order
    # here must be USE_EARLY_EXIT itself, not also the separate dry-run
    # gate.
    result = kalshi_15m_strategy.manage_open_positions(dry_run=False)

    assert result["checks"][0]["should_exit"] is True  # computed for observability
    assert order_calls == []  # never actually placed -- flag is off
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1  # position untouched


def test_manage_open_positions_closes_a_position_early_when_enabled(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "USE_EARLY_EXIT", True)
    position = _open_position(side="yes", entry_price=0.5)
    kalshi_15m_strategy._save_state({"positions": [position], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market(ticker=position["ticker"], no_ask=0.65, no_bid=0.60))
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.2})  # flips hard to "no"
    order_calls = []
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: order_calls.append(kw) or {"order": {"order_id": "exit1"}})
    monkeypatch.setattr(kalshi_15m, "get_orders", lambda ticker=None, status=None: [{"order_id": "exit1", "fill_count_fp": "10.00"}])

    result = kalshi_15m_strategy.manage_open_positions(dry_run=False)

    assert len(order_calls) == 1
    assert order_calls[0]["side"] == "ask"  # closing a "yes" position sells yes
    assert result["checks"][0]["action"] == "closed_early"
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["positions"] == []
    trade = state["trade_log"][0]
    # "full" (not "early") -- see the real win_rate_stats exit_kind bug
    # this dict's own comment fixes; close_reason is where the
    # settlement-vs-early distinction actually lives now.
    assert trade["exit_kind"] == "full"
    assert trade["close_reason"] == "early_exit"
    assert trade["exit_reason"] in ("lock_in_profit", "cut_loss")
    assert trade["realized_pnl_usd"] == pytest.approx(10.0 * (0.35 - 0.5))  # current_value=1-0.65=0.35, entry=0.5


def test_manage_open_positions_leaves_the_position_open_when_the_exit_order_never_fills(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "USE_EARLY_EXIT", True)
    position = _open_position(side="yes", entry_price=0.5)
    kalshi_15m_strategy._save_state({"positions": [position], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market(ticker=position["ticker"], no_ask=0.65, no_bid=0.60))
    monkeypatch.setattr(kalshi_15m_model, "predict_direction", lambda coin: {"model_ok": True, "probability_up": 0.2})
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: {"order": {"order_id": "exit1"}})
    monkeypatch.setattr(kalshi_15m, "get_orders", lambda ticker=None, status=None: [{"order_id": "exit1", "fill_count_fp": "0.00"}])

    kalshi_15m_strategy.manage_open_positions(dry_run=False)

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert len(state["positions"]) == 1  # never removed -- the order never actually filled


# ---------------------------------------------------------------------------
# Graduated concurrency -- per explicit user direction: "since the
# balance is low let['s] focus on 1 [position] after another win to grow
# the balance[,] [then] increase to 2 at a time[,] so on and forth."
# ---------------------------------------------------------------------------
def _gc_trade(*, pnl: float, dry_run: bool = False) -> dict:
    return {"coin": "BTC", "realized_pnl_usd": pnl, "dry_run": dry_run}


def test_graduated_concurrency_starts_at_1_slot_with_no_real_history():
    assert kalshi_15m_strategy.compute_graduated_max_concurrent_positions([]) == 1
    assert kalshi_15m_strategy.compute_graduated_max_concurrent_positions(None) == 1


def test_graduated_concurrency_grows_by_1_slot_per_real_win():
    trades = [_gc_trade(pnl=1.0)]
    assert kalshi_15m_strategy.compute_graduated_max_concurrent_positions(trades) == 2
    trades = [_gc_trade(pnl=1.0), _gc_trade(pnl=1.0)]
    assert kalshi_15m_strategy.compute_graduated_max_concurrent_positions(trades) == 3


def test_graduated_concurrency_drops_straight_back_to_1_on_the_next_loss():
    trades = [_gc_trade(pnl=1.0), _gc_trade(pnl=1.0), _gc_trade(pnl=1.0), _gc_trade(pnl=-1.0)]
    assert kalshi_15m_strategy.compute_graduated_max_concurrent_positions(trades) == 1


def test_graduated_concurrency_is_capped_at_max_concurrent_positions(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 2)
    trades = [_gc_trade(pnl=1.0) for _ in range(10)]  # would otherwise grow to 11 slots
    assert kalshi_15m_strategy.compute_graduated_max_concurrent_positions(trades) == 2


def test_graduated_concurrency_ignores_dry_run_trades():
    trades = [_gc_trade(pnl=1.0, dry_run=True) for _ in range(5)]
    assert kalshi_15m_strategy.compute_graduated_max_concurrent_positions(trades) == 1


def test_graduated_concurrency_returns_the_flat_ceiling_when_disabled(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "GRADUATED_CONCURRENCY_ENABLED", False)
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", 5)
    trades = []  # no real history -- would otherwise mean just 1 slot
    assert kalshi_15m_strategy.compute_graduated_max_concurrent_positions(trades) == 5


# ---------------------------------------------------------------------------
# Win-streak cooldown -- per explicit user direction: "after a couple of
# winning strikes[,] take a break and restudy... retrain again with the
# model[,] and go back after making sure it's going to keep winning."
# ---------------------------------------------------------------------------
def test_win_streak_cooldown_inactive_with_no_streak(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "WIN_STREAK_COOLDOWN_ENABLED", True)
    state = {"trade_log": [_gc_trade(pnl=1.0)], "tuning": {}}  # only 1 win -- below MIN_STREAK (2)
    result = kalshi_15m_strategy.compute_win_streak_cooldown_active(state)
    assert result["active"] is False
    assert result["reason"] == "no_streak"


def test_win_streak_cooldown_activates_at_the_min_streak(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "WIN_STREAK_COOLDOWN_ENABLED", True)
    state = {"trade_log": [_gc_trade(pnl=1.0), _gc_trade(pnl=1.0)], "tuning": {}}
    result = kalshi_15m_strategy.compute_win_streak_cooldown_active(state)
    assert result["active"] is True
    assert result["streak"] == 2
    assert result["real_trade_count"] == 2


def test_win_streak_cooldown_disabled_flag_never_activates(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "WIN_STREAK_COOLDOWN_ENABLED", False)
    state = {"trade_log": [_gc_trade(pnl=1.0), _gc_trade(pnl=1.0), _gc_trade(pnl=1.0)], "tuning": {}}
    result = kalshi_15m_strategy.compute_win_streak_cooldown_active(state)
    assert result["active"] is False
    assert result["reason"] == "disabled"


def test_win_streak_cooldown_clears_once_verification_recorded_for_this_exact_streak(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "WIN_STREAK_COOLDOWN_ENABLED", True)
    trade_log = [_gc_trade(pnl=1.0), _gc_trade(pnl=1.0)]
    state = {"trade_log": trade_log, "tuning": {"win_streak_cooldown": {"cleared_at_real_trade_count": 2}}}
    result = kalshi_15m_strategy.compute_win_streak_cooldown_active(state)
    assert result["active"] is False
    assert result["reason"] == "cleared_after_verification"


def test_win_streak_cooldown_a_new_win_after_clearing_re_triggers(monkeypatch):
    """A stale cleared-flag from an EARLIER streak must never suppress a
    fresh one -- cleared_at_real_trade_count only matches the exact
    streak it was recorded for."""
    monkeypatch.setattr(kalshi_15m_strategy, "WIN_STREAK_COOLDOWN_ENABLED", True)
    trade_log = [_gc_trade(pnl=1.0), _gc_trade(pnl=1.0), _gc_trade(pnl=1.0)]  # streak grew to 3
    state = {"trade_log": trade_log, "tuning": {"win_streak_cooldown": {"cleared_at_real_trade_count": 2}}}
    result = kalshi_15m_strategy.compute_win_streak_cooldown_active(state)
    assert result["active"] is True


def test_apply_win_streak_cooldown_result_persists_on_pass(monkeypatch):
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001
    applied = kalshi_15m_strategy.apply_win_streak_cooldown_result(True, real_trade_count=7, reason="test evidence")
    assert applied["cleared"] is True
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert state["tuning"]["win_streak_cooldown"]["cleared_at_real_trade_count"] == 7


def test_apply_win_streak_cooldown_result_persists_nothing_on_fail(monkeypatch):
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": [], "realized_pnl_by_date": {}})  # noqa: SLF001
    applied = kalshi_15m_strategy.apply_win_streak_cooldown_result(False, real_trade_count=7, reason="still losing")
    assert applied["cleared"] is False
    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    assert "win_streak_cooldown" not in state["tuning"]


def test_scan_and_enter_blocks_all_entries_during_a_win_streak_cooldown(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "WIN_STREAK_COOLDOWN_ENABLED", True)
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", len(kalshi_15m_strategy.ASSET_SERIES))
    monkeypatch.setattr(kalshi_15m_strategy, "GRADUATED_CONCURRENCY_ENABLED", False)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)
    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [], "trade_log": [_gc_trade(pnl=1.0), _gc_trade(pnl=1.0)], "realized_pnl_by_date": {},
    })

    result = kalshi_15m_strategy.scan_and_enter()

    assert result["win_streak_cooldown"]["active"] is True
    entered = [c for c in result["checks"] if c.get("action") == "entered"]
    assert entered == []
    assert all(c["reason"] == "win_streak_cooldown_active" for c in result["checks"])


def test_scan_and_enter_reports_win_streak_cooldown_inactive_with_no_streak(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "WIN_STREAK_COOLDOWN_ENABLED", True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    result = kalshi_15m_strategy.scan_and_enter()
    assert result["win_streak_cooldown"]["active"] is False
    assert result["win_streak_cooldown"]["reason"] == "no_streak"


def test_scan_and_enter_only_opens_1_position_with_no_real_trade_history(monkeypatch):
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)
    monkeypatch.setattr(crypto_correlation, "perps_correlation_bullishness", lambda coin, row=None: {"score": 0.0, "reason": "neutral", "components": {}})

    result = kalshi_15m_strategy.scan_and_enter()

    entered = [c for c in result["checks"] if c.get("action") == "entered"]
    assert len(entered) == 1  # graduated concurrency's own starting slot, not the flat MAX_CONCURRENT_POSITIONS ceiling
    concurrency_rejections = [c for c in result["checks"] if c.get("reason") == "max_concurrent_positions"]
    assert len(concurrency_rejections) == len(kalshi_15m_strategy.ASSET_SERIES) - 1


# ---------------------------------------------------------------------------
# Per-coin trust gate -- per explicit user direction: "the bot need to
# know by now after analyzing[,] he need to know the patterns and what
# its best on."
# ---------------------------------------------------------------------------
def _trust_trade(*, coin="BTC", pnl: float, dry_run: bool = False) -> dict:
    return {"coin": coin, "realized_pnl_usd": pnl, "dry_run": dry_run}


def test_coin_is_trusted_with_insufficient_history():
    trades = [_trust_trade(pnl=-1.0) for _ in range(3)]  # below the 8-trade floor
    result = kalshi_15m_strategy.coin_is_trusted("BTC", trades)
    assert result["trusted"] is True
    assert result["reason"] == "insufficient_history"


def test_coin_is_trusted_with_a_healthy_track_record():
    trades = [_trust_trade(pnl=2.0) for _ in range(5)] + [_trust_trade(pnl=-1.0) for _ in range(3)]
    result = kalshi_15m_strategy.coin_is_trusted("BTC", trades)
    assert result["trusted"] is True
    assert result["reason"] == "track_record_ok"


def test_coin_is_trusted_pauses_a_coin_with_a_clearly_poor_track_record():
    trades = [_trust_trade(pnl=-1.0) for _ in range(7)] + [_trust_trade(pnl=0.5) for _ in range(1)]
    result = kalshi_15m_strategy.coin_is_trusted("BTC", trades)
    assert result["trusted"] is False
    assert result["reason"] == "poor_real_track_record"
    assert result["win_rate"] < kalshi_15m_strategy.COIN_TRUST_MIN_WIN_RATE


def test_coin_is_trusted_needs_both_a_low_win_rate_and_a_negative_average_pnl():
    """A coin that wins RARELY but big (e.g. a few large wins offsetting
    many small losses) must not get paused on win rate alone."""
    trades = [_trust_trade(pnl=-0.1) for _ in range(6)] + [_trust_trade(pnl=5.0) for _ in range(2)]
    result = kalshi_15m_strategy.coin_is_trusted("BTC", trades)
    assert result["trusted"] is True  # win rate is low (25%) but avg P&L is positive


def test_coin_is_trusted_ignores_dry_run_trades():
    trades = [_trust_trade(pnl=-1.0, dry_run=True) for _ in range(10)]
    result = kalshi_15m_strategy.coin_is_trusted("BTC", trades)
    assert result["trusted"] is True
    assert result["reason"] == "insufficient_history"


def test_coin_is_trusted_is_coin_specific():
    bad_btc = [_trust_trade(coin="BTC", pnl=-1.0) for _ in range(8)]
    assert kalshi_15m_strategy.coin_is_trusted("BTC", bad_btc)["trusted"] is False
    assert kalshi_15m_strategy.coin_is_trusted("ETH", bad_btc)["trusted"] is True


def test_scan_and_enter_skips_a_coin_with_a_poor_real_track_record(monkeypatch):
    monkeypatch.setattr(kalshi_15m_strategy, "GRADUATED_CONCURRENCY_ENABLED", False)
    monkeypatch.setattr(kalshi_15m_strategy, "MAX_CONCURRENT_POSITIONS", len(kalshi_15m_strategy.ASSET_SERIES))
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: _market())
    _mock_confident_prediction(monkeypatch)
    monkeypatch.setattr(crypto_correlation, "perps_correlation_bullishness", lambda coin, row=None: {"score": 0.0, "reason": "neutral", "components": {}})
    bad_btc_trades = [{"coin": "BTC", "realized_pnl_usd": -1.0, "dry_run": False} for _ in range(8)]
    kalshi_15m_strategy._save_state({"positions": [], "trade_log": bad_btc_trades, "realized_pnl_by_date": {}})  # noqa: SLF001

    result = kalshi_15m_strategy.scan_and_enter()

    btc_check = next(c for c in result["checks"] if c["coin"] == "BTC")
    assert btc_check["ok"] is False
    assert btc_check["reason"] == "poor_real_track_record"
    entered_coins = {c["coin"] for c in result["checks"] if c.get("action") == "entered"}
    assert "BTC" not in entered_coins
    assert "ETH" in entered_coins  # a different coin's own track record is untouched
