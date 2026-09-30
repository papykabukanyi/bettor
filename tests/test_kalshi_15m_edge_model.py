"""kalshi_15m_edge_model: market-anchored probabilities, walk-forward
certification on real-quote-shaped data, and live EV scoring."""
from __future__ import annotations

import datetime as dt
import json
import math

import numpy as np
import pandas as pd
import pytest

from data import kalshi_15m_edge_model as em

DAY = 86400


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(em, "LOCAL_ARTIFACT_PATH", tmp_path / "edge.json")
    monkeypatch.setattr(em, "HF_API_KEY", "")
    monkeypatch.setattr(em, "_cache", {"artifact": None, "last_remote_attempt": 0.0})


def _window(coin, open_ts, mids, *, result, spread=0.02, volume=100.0, oi=1000.0):
    return [{
        "coin": coin, "series": "", "ticker": f"{coin}-{open_ts}", "open_ts": open_ts, "close_ts": open_ts + 900,
        "end_period_ts": open_ts + 60 * m, "minute": m, "yes_bid": round(mid - spread / 2, 4),
        "yes_ask": round(mid + spread / 2, 4), "last": mid, "mean": mid, "volume": volume, "open_interest": oi,
        "result": result, "floor_strike": None, "expiration_value": None,
    } for m, mid in enumerate(mids, start=1)]


def _synthetic_market(true_p, *, days=14, windows_per_day=96, seed=0, coin="BTC"):
    rng = np.random.default_rng(seed)
    start = 1_790_000_000 - (1_790_000_000 % DAY)
    rows = []
    for d in range(days):
        for w in range(windows_per_day):
            mid = float(rng.uniform(0.3, 0.7))
            p = true_p(mid)
            result = "yes" if rng.random() < p else "no"
            rows += _window(coin, start + d * DAY + w * 900, [mid] * 14, result=result)
    return pd.DataFrame(rows)


def test_fee_per_contract_rounds_up_to_the_cent():
    assert em.fee_per_contract(0.5) == pytest.approx(0.02)
    assert em.fee_per_contract(0.9) == pytest.approx(0.01)


def test_flow_features_never_look_ahead():
    early = pd.DataFrame(_window("BTC", 0, [0.5, 0.52, 0.55, 0.6, 0.62], result="yes"))
    full = pd.DataFrame(_window("BTC", 0, [0.5, 0.52, 0.55, 0.6, 0.62, 0.9, 0.1, 0.95], result="yes"))
    a = em.add_flow_features(early).set_index("minute")
    b = em.add_flow_features(full).set_index("minute")
    for col in em.FLOW_BASE:
        assert a.loc[1:5, col].tolist() == pytest.approx(b.loc[1:5, col].tolist())


def test_flow_features_cross_market_and_lead():
    rows = pd.DataFrame(_window("BTC", 0, [0.5, 0.6], result="yes") + _window("ETH", 0, [0.5, 0.5], result="yes"))
    f = em.add_flow_features(rows).set_index(["coin", "minute"])
    lm = lambda p: math.log(p / (1 - p))  # noqa: E731
    assert f.loc[("ETH", 2), "mkt_gap"] == pytest.approx(lm(0.6) - lm(0.5))
    assert f.loc[("ETH", 2), "lead_gap"] == pytest.approx(lm(0.6) - lm(0.5))
    assert f.loc[("ETH", 2), "lead_d1"] == pytest.approx(lm(0.6) - lm(0.5))
    assert f.loc[("BTC", 2), "lead_gap"] == 0.0
    assert f.loc[("BTC", 2), "d1"] == pytest.approx(lm(0.6) - lm(0.5))


def test_an_efficient_market_is_not_certified():
    quotes = _synthetic_market(lambda mid: mid)
    result = em.train_and_certify(quotes=quotes, underlying=pd.DataFrame())
    assert result["ok"] is True
    assert result["certified"]["crypto"] is False


def test_a_real_mispricing_is_certified_and_scored_live():
    quotes = _synthetic_market(lambda mid: 1 / (1 + math.exp(-3.0 * math.log(mid / (1 - mid)))))
    result = em.train_and_certify(quotes=quotes, underlying=pd.DataFrame())
    assert result["certified"]["crypto"] is True
    stats = result["candidates"]["crypto"][result["chosen"]["crypto"]]
    assert stats["trades"] >= em.CERTIFY_MIN_TRADES and stats["t_stat"] >= em.CERTIFY_MIN_T

    market = {"ticker": "X", "yes_bid_dollars": "0.64", "yes_ask_dollars": "0.66"}
    spec = em.load_artifact()["markets"]["crypto"]
    if spec["model"] == "quote":
        ev = em.evaluate_market("BTC", market, seconds_to_close=600)
        assert ev["ok"] is True and ev["certified"] is True
        assert ev["side"] == "yes" and ev["edge"] > em.EV_MIN_EDGE


def test_simulate_rule_takes_the_earliest_qualifying_minute_once_per_window():
    oos = pd.DataFrame(_window("BTC", 0, [0.5] * 14, result="yes"))
    oos["p"] = 0.8
    oos["y"] = 1
    oos["day"] = "d"
    trades = em.simulate_rule(oos, min_edge=0.0)
    assert len(trades) == 1
    assert trades.iloc[0]["minute"] == em.EV_ENTRY_MIN_MINUTE


def _artifact(features, coef, intercept, certified=True, model="quote"):
    return {"markets": {"crypto": {"model": model, "certified": certified, "features": features, "coef": coef, "intercept": intercept}}}


def test_evaluate_market_without_an_artifact():
    assert em.evaluate_market("BTC", {}, seconds_to_close=600)["reason"] == "no_edge_model_yet"


def test_evaluate_market_prices_the_edge_at_the_real_ask_after_fees():
    artifact = _artifact(["lm", "lm_t"], [1.0, 0.0], 0.0)
    market = {"ticker": "X", "yes_bid_dollars": "0.49", "yes_ask_dollars": "0.51"}
    ev = em.evaluate_market("BTC", market, seconds_to_close=600, artifact=artifact)
    assert ev["p_yes"] == pytest.approx(0.5)
    assert ev["edge_yes"] == pytest.approx(0.5 - 0.51 - 0.02)
    assert ev["edge"] < 0


def test_evaluate_market_respects_the_entry_minutes():
    artifact = _artifact(["lm", "lm_t"], [1.0, 0.0], 0.0)
    market = {"ticker": "X", "yes_bid_dollars": "0.49", "yes_ask_dollars": "0.51"}
    assert em.evaluate_market("BTC", market, seconds_to_close=60, artifact=artifact)["reason"] == "outside_ev_entry_minutes"


def test_flow_model_needs_flow_features():
    artifact = _artifact(em.FLOW_FEATURES, [0.0] * len(em.FLOW_FEATURES), 0.0, model="quote_flow")
    market = {"ticker": "X", "yes_bid_dollars": "0.49", "yes_ask_dollars": "0.51"}
    assert em.evaluate_market("BTC", market, seconds_to_close=600, artifact=artifact)["reason"] == "flow_features_unavailable"


def test_flow_scoring_uses_the_minute_close_and_the_worse_ask():
    artifact = _artifact(em.FLOW_FEATURES, [1.0] + [0.0] * (len(em.FLOW_FEATURES) - 1), 1.0, model="quote_flow")
    market = {"ticker": "X", "yes_bid_dollars": "0.51", "yes_ask_dollars": "0.53"}
    flow_row = {"ticker": "X", "minute": 5, "yes_bid": 0.49, "yes_ask": 0.51, **{f: 0.0 for f in em.FLOW_BASE}}
    ev = em.evaluate_market("BTC", market, seconds_to_close=590, flow_row=flow_row, artifact=artifact)
    assert ev["minute"] == 5
    assert ev["p_yes"] == pytest.approx(1 / (1 + math.exp(-1.0)), abs=1e-4)
    assert ev["ask_side"] == pytest.approx(0.53)


def test_flow_scoring_skips_a_quote_that_moved_since_the_minute_close():
    artifact = _artifact(em.FLOW_FEATURES, [0.0] * len(em.FLOW_FEATURES), 0.0, model="quote_flow")
    market = {"ticker": "X", "yes_bid_dollars": "0.69", "yes_ask_dollars": "0.71"}
    flow_row = {"ticker": "X", "minute": 5, "yes_bid": 0.49, "yes_ask": 0.51, **{f: 0.0 for f in em.FLOW_BASE}}
    ev = em.evaluate_market("BTC", market, seconds_to_close=590, flow_row=flow_row, artifact=artifact)
    assert ev["reason"] == "quote_moved_since_minute_close"


def test_artifact_round_trips_through_the_local_file():
    em.save_artifact({"markets": {"crypto": {"model": "quote"}}, "built_at_utc": "t"})
    em._cache["artifact"] = None  # noqa: SLF001
    assert em.load_artifact()["markets"]["crypto"]["model"] == "quote"
    assert em.summary()["markets"]["crypto"]["model"] == "quote"


def test_build_frame_joins_underlying_from_the_minute_before_the_quote():
    open_ts = 1_790_000_000 - (1_790_000_000 % 900)
    quotes = pd.DataFrame(_window("BTC", open_ts, [0.5, 0.5, 0.5], result="yes"))
    ts = np.arange(open_ts - 2 * DAY, open_ts + 600, 60)
    underlying = pd.DataFrame({
        "symbol": "BTC", "ts": ts, "close": np.linspace(100, 110, len(ts)), "ret_5m": 0.001, "ret_10m": 0.001,
        "ret_15m": 0.001, "ret_30m": 0.001, "trend_1h": 0.001, "trend_2h": 0.001, "trend_4h": 0.001,
        "volatility_15": 0.001, "volatility_30": 0.001, "dollar_volume_z": 0.5,
    })
    frame = em.build_frame(quotes, underlying).set_index("minute")
    close_at = dict(zip(underlying.ts, underlying.close))
    assert frame.loc[2, "close"] == pytest.approx(close_at[open_ts + 60])
    assert frame.loc[2, "close_open"] == pytest.approx(close_at[open_ts])
    assert bool(frame.loc[2, "has_underlying"]) is True
