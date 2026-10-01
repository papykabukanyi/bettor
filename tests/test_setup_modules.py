"""Every bot's own price-action setup module (perps, Kalshi 15m, Alpaca
stocks/crypto/options) must implement the method the same, correct way."""
from __future__ import annotations

import datetime as dt
import importlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

MODULES = ["perps_setup", "kalshi_15m_setup", "alpaca_setup", "alpaca_crypto_setup", "alpaca_options_setup"]
FIXTURE = Path(__file__).parent / "fixtures" / "setup_btc_1m.parquet"
SETUP_AS_OF = 1785036900  # a real BTC breakout-retest (Coinbase 1m, 2026-07-26)


@pytest.fixture(scope="module")
def btc() -> pd.DataFrame:
    return pd.read_parquet(FIXTURE)


@pytest.fixture(params=MODULES)
def mod(request, monkeypatch):
    """Each bot's module with the same plan parameters (stop half a 15m ATR
    beyond invalidation, reward/risk 2), so these tests check the method
    itself; each bot's own tuned values are tested separately."""
    m = importlib.import_module(f"data.{request.param}")
    monkeypatch.setattr(m, "STOP_BUFFER_ATR15", 0.5)
    monkeypatch.setattr(m, "MIN_RR", 2.0)
    return m


def _utc_eval(mod, df, as_of, **kw):
    """Crypto-session evaluation for every module (stock modules default to
    the US session; the fixture is 24/7 crypto)."""
    ctx = mod.prepare(df, session="utc_day")
    return mod.evaluate(ctx, as_of, **kw)


def test_finds_the_real_breakout_retest(mod, btc):
    r = _utc_eval(mod, btc, SETUP_AS_OF, sides=("long",), fee_rate_roundtrip=0.001, spread_bps=0.5)
    assert r["valid"] is True and r["side"] == "long" and r["setup"] == "breakout_retest"
    plan = r["plan"]
    assert plan["stop"] < plan["entry"] < plan["target"]
    assert plan["rr_net"] >= mod.MIN_RR
    for rule in ("trend", "breakout", "volume", "retest", "hold", "vwap", "momentum", "divergence", "news", "risk_reward"):
        assert r["checks"][rule]["ok"] is True, rule


def test_never_looks_ahead(mod, btc):
    past = btc[btc.ts <= SETUP_AS_OF]
    full = _utc_eval(mod, btc, SETUP_AS_OF, sides=("long", "short"), fee_rate_roundtrip=0.001)
    truncated = _utc_eval(mod, past, SETUP_AS_OF, sides=("long", "short"), fee_rate_roundtrip=0.001)
    assert full["valid"] == truncated["valid"] and full["plan"] == truncated["plan"] and full["setup_id"] == truncated["setup_id"]


def test_a_mirrored_chart_gives_the_same_setup_as_a_short(mod, btc):
    long_r = _utc_eval(mod, btc, SETUP_AS_OF, sides=("long",), fee_rate_roundtrip=0.0)
    c = 2 * long_r["plan"]["entry"]  # reflect around the entry so percentage rules see the same price level
    mirrored = btc.assign(open=c - btc.open, high=c - btc.low, low=c - btc.high, close=c - btc.close)
    short_r = _utc_eval(mod, mirrored, SETUP_AS_OF, sides=("short",), fee_rate_roundtrip=0.0)
    assert short_r["valid"] is True and short_r["side"] == "short"
    assert short_r["plan"]["entry"] == pytest.approx(c - long_r["plan"]["entry"])
    assert short_r["plan"]["stop"] == pytest.approx(c - long_r["plan"]["stop"])
    assert short_r["plan"]["target"] == pytest.approx(c - long_r["plan"]["target"])
    assert short_r["checks"]["trend"]["lower_high"] is True and short_r["checks"]["trend"]["lower_low"] is True


def test_costs_that_ruin_reward_risk_skip_the_trade(mod, btc):
    r = _utc_eval(mod, btc, SETUP_AS_OF, sides=("long",), fee_rate_roundtrip=0.05)
    assert r["valid"] is False and r["reason"] == "risk_reward"


def test_news_against_the_trade_blocks_it(mod, btc):
    r = _utc_eval(mod, btc, SETUP_AS_OF, sides=("long",), fee_rate_roundtrip=0.001, news_score=-0.9)
    assert r["valid"] is False and r["reason"] == "news"


def test_rejects_when_there_is_not_enough_history(mod, btc):
    r = _utc_eval(mod, btc.tail(60), SETUP_AS_OF, sides=("long",))
    assert r["valid"] is False and r["reason"] == "data"


@pytest.mark.parametrize("side,price,expected", [
    ("long", 99.0, "stop_loss"), ("long", 111.0, "take_profit"), ("long", 105.0, None),
    ("short", 111.0, "stop_loss"), ("short", 99.0, "take_profit"), ("short", 105.0, None),
])
def test_exits_only_at_the_planned_stop_or_target(mod, side, price, expected):
    stop, target = (100.0, 110.0) if side == "long" else (110.0, 100.0)
    position = {"side": side, "setup_stop_price": stop, "setup_target_price": target}
    should, reason = mod.plan_exit(position, price, held_minutes=1)
    assert should is (expected is not None)
    if expected:
        assert reason.startswith(expected)
    assert mod.exit_levels(position) == {"take_profit_price": target, "stop_loss_price": stop}


def test_the_safety_backstop_closes_a_stale_position(mod):
    position = {"side": "long", "setup_stop_price": 100.0, "setup_target_price": 110.0}
    should, reason = mod.plan_exit(position, 105.0, held_minutes=mod.MAX_HOLD_SAFETY_MINUTES)
    assert should and reason.startswith("max_hold_safety")


def test_replay_trades_each_breakout_once_and_exits_at_plan(mod, btc):
    trades = mod.replay(btc, sides=("long", "short"), fee_rate_roundtrip=0.001, spread_bps=0.5, max_hold_minutes=24 * 60)
    if not trades.empty:
        assert set(trades.exit) <= {"stop", "target", "max_hold_safety", "session_close"}
        assert (trades.entry_ts.diff().dropna() > 0).all()
    s = mod.summarize(trades)
    assert s["trades"] == len(trades)


# ---- market-specific adapters ----

def test_alpaca_bars_are_shifted_from_start_to_end_time():
    from data import alpaca_crypto_setup, alpaca_setup
    bars = pd.DataFrame({"ts": [1_790_000_000], "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0], "volume": [1.0]})
    assert alpaca_crypto_setup.candles_from_bars(bars)["ts"].iloc[0] == 1_790_000_060
    ts_open = int(pd.Timestamp("2026-09-30 09:30", tz="America/New_York").timestamp())
    stock_bars = pd.concat([bars, bars], ignore_index=True).assign(ts=[ts_open - 60, ts_open])
    rth = alpaca_setup.regular_session_candles(stock_bars)
    assert list(rth.ts) == [ts_open + 60]


@pytest.mark.parametrize("module", ["alpaca_setup", "alpaca_options_setup"])
def test_stock_session_rules(module):
    m = importlib.import_module(f"data.{module}")
    at = lambda hh, mm: int(pd.Timestamp(f"2026-09-30 {hh:02d}:{mm:02d}", tz="America/New_York").timestamp()) + 60  # noqa: E731
    assert m.entry_allowed(at(10, 0)) and not m.entry_allowed(at(15, 45)) and not m.entry_allowed(at(9, 0))
    assert m.must_be_flat(at(15, 55)) and not m.must_be_flat(at(15, 0))
    saturday = int(pd.Timestamp("2026-10-03 11:00", tz="America/New_York").timestamp())
    assert not m.entry_allowed(saturday)
    assert m.setup_from_bars(pd.DataFrame(), news_score=None, now=saturday)["reason"] == "session"


def test_perps_live_setup_uses_the_candles_spread_and_refuses_stale_data(btc):
    from data import perps_setup
    candles = btc[btc.ts <= SETUP_AS_OF + 120].assign(bid_close=lambda d: d.close - 0.5, ask_close=lambda d: d.close + 0.5)
    r = perps_setup.setup_from_candles(candles, sides=("long",), fee_rate_roundtrip=0.001, news_score=None, now=SETUP_AS_OF + 150)
    assert r["as_of"] == SETUP_AS_OF and r["spread_bps"] > 0
    stale = perps_setup.setup_from_candles(candles, sides=("long",), fee_rate_roundtrip=0.001, news_score=None, now=SETUP_AS_OF + 3600)
    assert stale["reason"] == "data"


def test_kalshi_contract_plan_prices_reward_and_risk_from_the_chart_plan():
    from data import kalshi_15m_setup as ks
    setup = {"side": "long", "vol_per_min": 0.0008, "plan": {"entry": 100.0, "stop": 99.7, "target": 100.9}}
    market = {"yes_bid_dollars": "0.49", "yes_ask_dollars": "0.51"}
    cp = ks.contract_plan(setup, market, seconds_to_close=600, strike_underlying=100.0)
    assert cp["contract_side"] == "yes" and cp["ask"] == 0.51
    assert cp["value_at_target"] > 0.9 and cp["value_at_stop"] < 0.3
    assert cp["rr"] == pytest.approx(cp["reward"] / cp["risk"], abs=0.01)
    short = ks.contract_plan({**setup, "side": "short", "plan": {"entry": 100.0, "stop": 100.3, "target": 99.1}}, market,
                             seconds_to_close=600, strike_underlying=100.0)
    assert short["contract_side"] == "no" and short["ask"] == pytest.approx(0.51)
    far = ks.contract_plan(setup, {"yes_bid_dollars": "0.94", "yes_ask_dollars": "0.95"}, seconds_to_close=600, strike_underlying=100.0)
    assert far["ok"] is False and far["reason"] == "contract_rr_below_min"


def test_kalshi_fair_value_is_a_probability():
    from data import kalshi_15m_setup as ks
    assert ks.fair_value_yes(100.0, 100.0, 0.001, 10) == pytest.approx(0.5)
    assert ks.fair_value_yes(101.0, 100.0, 0.001, 10) > 0.9
    assert ks.fair_value_yes(101.0, 100.0, 0.001, 0) == 1.0


# ---- rule 6: correlation with the market leader ----

def _leader_eval(mod, btc, leader_df, **kw):
    ctx = mod.prepare(btc, session="utc_day")
    leader = None if leader_df is None else mod.prepare(leader_df, session="utc_day")
    return mod.evaluate(ctx, SETUP_AS_OF, sides=("long",), fee_rate_roundtrip=0.001, spread_bps=0.5,
                        leader=leader, leader_symbol="LEAD", **kw)


def test_a_leader_moving_with_the_trade_keeps_the_setup(mod, btc):
    r = _leader_eval(mod, btc, btc)
    assert r["valid"] is True
    c = r["checks"]["correlation"]
    assert c["ok"] is True and c["corr"] == pytest.approx(1.0) and c["leader_dir"] in ("up", "mixed")


def test_a_correlated_leader_falling_against_a_long_blocks_it(mod, btc, monkeypatch):
    # Two days of returns, so the injected last-hour drop doesn't itself decorrelate the pair.
    monkeypatch.setattr(mod, "CORR_LOOKBACK_5M", 576)
    lead = btc[btc.ts <= SETUP_AS_OF].copy()
    last_hour = lead.ts > SETUP_AS_OF - 3600
    factor = np.ones(len(lead))
    factor[last_hour.to_numpy()] = np.linspace(1.0, 0.95, int(last_hour.sum()))
    for k in ("open", "high", "low", "close"):
        lead[k] = lead[k] * factor
    r = _leader_eval(mod, btc, lead)
    assert r["valid"] is False and r["reason"] == "correlation"
    c = r["by_side"]["long"]["checks"]["correlation"]
    assert c["ok"] is False and c["leader_dir"] == "down" and c["corr"] > mod.CORR_MIN


def test_an_uncorrelated_leader_does_not_apply(mod, btc):
    shuffled = btc.copy()
    shuffled[["open", "high", "low", "close", "volume"]] = btc[["open", "high", "low", "close", "volume"]].iloc[::-1].to_numpy()
    r = _leader_eval(mod, btc, shuffled)
    c = r["checks"]["correlation"]
    assert abs(c["corr"]) < mod.CORR_MIN and c["applies"] is False and r["valid"] is True


def test_missing_leader_data_fails_closed(mod, btc):
    r = _leader_eval(mod, btc, None, require_leader=True)
    assert r["valid"] is False and r["reason"] == "correlation"
    assert r["by_side"]["long"]["checks"]["correlation"]["detail"] == "no leader data"


def test_each_bot_names_its_own_leader():
    from data import alpaca_crypto_setup, alpaca_options_setup, alpaca_setup, kalshi_15m_setup, perps_setup
    assert perps_setup.leader_for("SOL") == "BTC" and perps_setup.leader_for("BTC") == "ETH"
    assert alpaca_crypto_setup.leader_for("SOL/USD") == "BTC/USD" and alpaca_crypto_setup.leader_for("BTC/USD") == "ETH/USD"
    assert alpaca_setup.leader_for("NVDA") == "SPY" and alpaca_setup.leader_for("SPY") == "QQQ"
    assert alpaca_options_setup.leader_for("AAPL") == "SPY"
    assert kalshi_15m_setup.leader_for("XRP") == "BTC" and kalshi_15m_setup.leader_for("SILVER") == "GOLD"
    assert kalshi_15m_setup.leader_for("GOLD") == "SILVER"


def test_perps_uses_its_walk_forward_plan_parameters():
    from data import perps_setup
    assert perps_setup.STOP_BUFFER_ATR15 == 1.5 and perps_setup.MIN_RR == 3.0
