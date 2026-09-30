"""perps_spot_lead: Coinbase spot leading the Kalshi perp."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data import perps_spot_lead as sl

T0 = 1_790_000_000 - (1_790_000_000 % 60)


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(sl, "LOCAL_ARTIFACT_PATH", tmp_path / "lead.json")
    monkeypatch.setattr(sl, "HF_API_KEY", "")
    monkeypatch.setattr(sl, "_cache", {"artifact": None, "last_remote_attempt": 0.0})


def _series(prices, start=T0):
    return pd.DataFrame({"ts": [start + 60 * (i + 1) for i in range(len(prices))], "close": prices})


def test_features_match_scales_and_measure_the_lag():
    spot = _series([100.0] * 70 + [100.1])
    perp = _series([0.01] * 71)                      # perp quoted at 1/10,000 and hasn't moved yet
    f = sl.build_features(perp, spot).set_index("ts")
    last = f.iloc[-1]
    assert last["spot_r1"] == pytest.approx(np.log(100.1 / 100.0) * 1e4)
    assert last["perp_r1"] == pytest.approx(0.0)
    assert last["lag_gap"] == pytest.approx(last["spot_r5"])
    assert abs(last["basis_bps"]) < 15 and not bool(last["bad_print"])


def test_a_perp_far_from_spot_is_a_bad_print():
    f = sl.build_features(_series([1.0] * 60 + [1.05]), _series([1.0] * 61))
    assert bool(f.iloc[-1]["bad_print"]) is True


def _lead_lag_history(days=16, seed=0, lag=True):
    """Spot is a random walk; the perp follows it with a 2-minute lag (or,
    with lag=False, is an independent walk)."""
    rng = np.random.default_rng(seed)
    n = days * 1440
    spot = 100 * np.exp(np.cumsum(rng.normal(0, 0.0008, n)))
    perp = np.concatenate([[spot[0], spot[0]], spot[:-2]]) if lag else spot.copy()
    ts = T0 + 60 * np.arange(1, n + 1)
    return (pd.DataFrame({"ticker": "KXHYPEPERP", "ts": ts, "close": perp, "bid_close": perp, "ask_close": perp}),
            pd.DataFrame({"coin": "HYPE", "ts": ts, "close": spot}))


def test_a_real_lead_certifies_and_predicts_live():
    perp, spot = _lead_lag_history()
    result = sl.train_and_evaluate(perp_history=perp, spot_history=spot)
    assert result["certified"] is True
    assert result["oos"]["strong_5bps"]["hit_rate"] > 0.7
    art = sl.load_artifact()
    tail_perp, tail_spot = perp.tail(150).drop(columns="ticker"), spot.tail(150).drop(columns="coin")
    live = sl.predict_from_rows(tail_perp, tail_spot, art, now=float(tail_perp.ts.iloc[-1]) + 30)
    assert live["ok"] is True and live["certified"] is True
    expected_sign = np.sign(tail_spot.close.iloc[-1] - tail_perp.close.iloc[-1])
    assert np.sign(live["pred_bps"]) == expected_sign or abs(live["pred_bps"]) < 1


def test_a_perp_that_already_matches_spot_has_nothing_to_lead_and_does_not_certify():
    perp, spot = _lead_lag_history(lag=False)
    assert sl.train_and_evaluate(perp_history=perp, spot_history=spot)["certified"] is False


def test_live_prediction_refuses_stale_data_and_bad_prints():
    art = {"features": sl.FEATURES, "coef": [1.0] + [0.0] * 5, "intercept": 0.0, "certified": True}
    perp, spot = _series([1.0] * 120), _series([1.0] * 120)
    last_ts = float(perp.ts.iloc[-1])
    assert sl.predict_from_rows(perp, spot, art, now=last_ts + 600)["reason"] == "stale_data"
    spike = _series([1.0] * 119 + [1.05])
    assert sl.predict_from_rows(spike, spot, art, now=last_ts + 30)["reason"] == "bad_print"


def test_live_prediction_without_a_model_or_coinbase_market():
    assert sl.live_prediction("KXGOLDPERP")["reason"] == "no_coinbase_market"
    assert sl.live_prediction("KXBTCPERP")["reason"] == "no_spot_lead_model_yet"


def test_training_without_aligned_history_reports_uncertified():
    result = sl.train_and_evaluate(perp_history=pd.DataFrame(columns=["ticker", "ts", "close"]), spot_history=pd.DataFrame(columns=["coin", "ts", "close"]))
    assert result == {"ok": False, "certified": False, "reason": "no_aligned_history"}
