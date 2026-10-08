"""The 15m new-signal study: features read only what was known at the
decision minute, the walk-forward never peeks at the week it trades, and
only a real held-out edge is adopted."""
from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from data import kalshi_15m_signal_study as sig

OPEN = 1_790_000_100 // 900 * 900  # a quarter-hour boundary


def _candles(start: int, closes: list[float]) -> pd.DataFrame:
    ts = start + 60 * np.arange(len(closes))
    c = np.asarray(closes, dtype=float)
    return pd.DataFrame({"ts": ts, "open": c, "high": c + 1, "low": c - 1, "close": c, "volume": 1.0})


def test_a_chart_reads_moves_volatility_and_the_settlement_reference():
    closes = [100.0 * math.exp(0.001 * ((k % 5) - 2)) for k in range(40)]
    chart = sig.Chart(_candles(OPEN - 39 * 60, closes), minute_average=True)
    i = len(closes) - 1
    assert chart.r1[i] == pytest.approx(math.log(closes[i] / closes[i - 1]))
    assert chart.r3[i] == pytest.approx(math.log(closes[i] / closes[i - 3]))
    assert chart.vol[i] == pytest.approx(np.std(np.diff(np.log(closes[-31:])), ddof=1))  # as the live setup reads it
    assert chart.ref[i] == pytest.approx((closes[i] * 4) / 4)  # mean of open/high/low/close
    assert chart.at(np.array([OPEN, OPEN + 60, OPEN + 200]))[:2].tolist() == [i, i]  # fresh within STALE_SEC
    assert chart.at(np.array([OPEN + 200]))[0] == -1  # too old
    assert chart.at(np.array([OPEN + 30]), exact=True)[0] == -1


def _quotes(minutes=range(1, 13), bid=0.45, ask=0.47, result="yes", ticker="KXBTC15M-X", open_ts=OPEN):
    return pd.DataFrame({"ticker": ticker, "coin": "BTC", "open_ts": open_ts, "minute": list(minutes), "yes_bid": bid,
                         "yes_ask": ask, "result": result})


def test_features_use_the_candle_ending_at_each_decision_minute_and_the_strike_at_the_open():
    from data import kalshi_15m_setup
    closes = [100.0] * 40 + [100.0 + 0.1 * k for k in range(1, 16)]
    own = sig.Chart(_candles(OPEN - 39 * 60, closes))
    f = sig.features(_quotes(minutes=[3]), own)
    row = f.iloc[0]
    t_idx = 39 + 3  # the candle ending at open + 3 minutes
    assert row["fair"] == pytest.approx(kalshi_15m_setup.fair_value_yes(closes[t_idx], 100.0, own.vol[t_idx], 12.0))
    assert row["own_r1"] == pytest.approx(np.clip(math.log(closes[t_idx] / closes[t_idx - 1]) / own.vol[t_idx], -5, 5))
    assert row["logit_mid"] == pytest.approx(math.log(0.46 / 0.54))
    assert row["us_open"] == 0.0 and row["spy_r1"] == 0.0


def test_equity_moves_count_only_when_spy_printed_that_minute():
    own = sig.Chart(_candles(OPEN - 39 * 60, [100.0 + 0.01 * (k % 7) for k in range(60)]))
    spy_closes = [500.0 + 0.05 * (k % 3) for k in range(42)]
    spy = sig.Chart(_candles(OPEN - 39 * 60, spy_closes), minute_average=False)  # last candle ends at open + 2 min
    f = sig.features(_quotes(minutes=[2, 5]), own, spy=spy)
    assert f["us_open"].tolist() == [1.0, 0.0]
    assert f.loc[1, "spy_r1"] == 0.0


def test_rows_without_a_fresh_chart_are_dropped():
    own = sig.Chart(_candles(OPEN - 39 * 60, [100.0 + 0.01 * (k % 4) for k in range(41)]))  # stops at open + 1 minute
    assert sig.features(_quotes(minutes=[1, 6]), own)["minute"].tolist() == [1]


def _rows(n_windows: int, *, signal: float, weeks: int, seed: int = 0) -> pd.DataFrame:
    """Synthetic decision rows: the market prices p_mkt; the true chance is
    shifted by `signal` * x, a feature the market ignores."""
    rng = np.random.default_rng(seed)
    rows = []
    for w in range(n_windows):
        open_ts = OPEN + 900 * w
        p_mkt = float(rng.uniform(0.3, 0.7))
        x = float(rng.normal())
        p_true = 1 / (1 + math.exp(-(math.log(p_mkt / (1 - p_mkt)) + signal * x)))
        result = "yes" if rng.random() < p_true else "no"
        for m in (1, 2):
            rows.append({"ticker": f"T{w}", "coin": "BTC", "open_ts": open_ts, "minute": m, "yes_bid": round(p_mkt - 0.01, 2),
                         "yes_ask": round(p_mkt + 0.01, 2), "result": result, "logit_mid": math.log(p_mkt / (1 - p_mkt)),
                         "fair_gap": x, "own_r1": rng.normal(), "own_r3": rng.normal()})
    df = pd.DataFrame(rows)
    df["y"] = (df["result"] == "yes").astype(int)
    df["week"] = [f"2026-W{int(k * weeks / len(df)):02d}" for k in range(len(df))]
    return df


def test_trades_take_the_first_qualifying_minute_once_per_window_and_pay_the_fee():
    df = _rows(3, signal=0.0, weeks=1)
    p = np.full(len(df), 0.95)  # a model sure of YES everywhere
    t = sig.trade(df, p, 0.02, ("yes",))
    assert len(t) == 3 and set(t["minute"]) == {1} and set(t["side"]) == {"yes"}
    first = df.drop_duplicates("ticker").reset_index(drop=True)
    won = (first["result"] == "yes").astype(float)
    assert t["pnl"].tolist() == pytest.approx((won - first["yes_ask"] - sig.taker_fee(first["yes_ask"].to_numpy())).tolist())
    assert sig.trade(df, np.full(len(df), 0.05), 0.0, ("yes",)).empty  # never buys NO when only YES is allowed


def test_the_threshold_comes_from_the_week_before_and_none_when_nothing_paid():
    df = _rows(400, signal=0.0, weeks=1, seed=3)
    assert sig.choose_theta(df, np.full(len(df), 0.0), ("yes",)) == math.inf


def test_a_real_held_out_edge_is_adopted_and_noise_is_not():
    strong = _rows(6000, signal=1.5, weeks=8, seed=1)
    wf = sig.walk_forward(strong, ["fair_gap"], ("yes",))
    assert len(wf["weeks"]) == 8 - sig.MIN_TRAIN_WEEKS and wf["trades"] >= sig.ADOPT_MIN_TRADES
    assert wf["total"] > 0 and wf["t_clustered"] >= sig.ADOPT_MIN_T and wf["adopt"] is True
    noise = _rows(6000, signal=0.0, weeks=8, seed=2)
    assert sig.walk_forward(noise, ["fair_gap", "own_r1", "own_r3"], ("yes",))["adopt"] is False


def test_models_fit_once_and_are_shared_between_side_sets():
    df = _rows(2000, signal=1.0, weeks=5, seed=4)
    fits: dict = {}
    sig.walk_forward(df, ["fair_gap"], ("yes",), fits)
    n = len(fits)
    sig.walk_forward(df, ["fair_gap"], ("yes", "no"), fits)
    assert n == len(fits) == 5 - sig.MIN_TRAIN_WEEKS + 1  # one model per cutoff week


def test_the_study_runs_only_when_due_and_the_cores_are_free(monkeypatch, tmp_path):
    from data import setup_backtest_job
    monkeypatch.setattr(setup_backtest_job, "LOCAL_DIR", tmp_path)
    launched = []
    monkeypatch.setattr(sig, "launch", lambda: launched.append(1) or {"ok": True, "action": "launched"})
    monkeypatch.setattr(sig, "latest", lambda: None)
    monkeypatch.setattr(setup_backtest_job, "_running", lambda name: name == "perps_multiyear")
    assert sig.maybe_start()["action"] == "a_multiyear_study_is_running" and not launched
    monkeypatch.setattr(setup_backtest_job, "_running", lambda name: False)
    assert sig.maybe_start()["action"] == "launched" and launched
    import datetime as dt
    fresh = {"version": sig.VERSION, "computed_at": dt.datetime.now(dt.timezone.utc).isoformat()}
    monkeypatch.setattr(sig, "latest", lambda: fresh)
    assert sig.maybe_start()["action"] == "fresh"


def test_nothing_is_adopted_without_a_winning_study(monkeypatch):
    monkeypatch.setattr(sig, "latest", lambda: None)
    assert sig.adopted()["enforce"] is False
    monkeypatch.setattr(sig, "latest", lambda: {"adopted": {"enforce": False, "reason": "no family cleared the bar on held-out weeks"}})
    assert sig.adopted() == {"enforce": False, "reason": "no family cleared the bar on held-out weeks"}
    assert sig.LIVE_SIDES == ("yes",)
