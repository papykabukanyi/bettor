"""Every ticker x timeframe x approach study: clean data in, positions that
only use closed bars, stock positions flat overnight, and a walk-forward
that never chooses with a year it then trades."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from data import approach_study as st

T0 = 1_700_000_040  # a minute END


def _minutes(closes, start=T0):
    c = np.asarray(closes, dtype=float)
    return pd.DataFrame({"ts": start + 60 * np.arange(len(c)), "open": c, "high": c * 1.0005, "low": c * 0.9995, "close": c,
                         "volume": 1.0})


def test_cleaning_drops_duplicates_impossible_candles_and_snap_back_prints_only():
    rng = np.random.default_rng(0)
    closes = 100 * np.exp(np.cumsum(rng.normal(0, 0.0005, 600)))
    closes[300] *= 1.06            # a bad print: +6% for one minute, then straight back
    closes[450:] *= 1.04           # a real move: +4% that stays
    df = _minutes(closes)
    df = pd.concat([df, df.iloc[[10]]])                              # a duplicate minute
    df.loc[df.index[20], "high"] = df["low"].iloc[20] * 0.9          # an impossible candle
    from data import bar_quality
    clean, q = bar_quality.clean(df, kind="crypto")
    assert q["duplicates"] == 1 and q["impossible"] == 1 and q["bad_prints"] == 1
    assert closes[300] not in set(clean["close"]) and np.isclose(clean["close"].iloc[-1], closes[-1])
    assert len(clean) == 600 - 2


def test_bars_end_at_their_period_and_daily_bars_follow_the_trading_day():
    df = _minutes(np.arange(1, 31, dtype=float), start=(T0 // 300) * 300 + 60)
    b = st.bars(df, 5, kind="crypto")
    assert (b["ts"] % 300 == 0).all() and b["close"].iloc[0] == 5.0 and b["open"].iloc[0] == 1.0
    assert b["volume"].iloc[0] == 5.0 and not b["session_end"].any()


def _stock_days(n_days=3, per_day=390, drift=0.0):
    rows = []
    for d in range(n_days):
        open_end = int(pd.Timestamp(f"2026-03-0{2 + d} 09:31", tz="America/New_York").timestamp())
        for m in range(per_day):
            px = 100 + drift * (d * per_day + m)
            rows.append({"ts": open_end + 60 * m, "open": px, "high": px, "low": px, "close": px, "volume": 1.0})
    return pd.DataFrame(rows)


def test_stock_intraday_positions_are_flat_at_each_session_close():
    b = st.bars(_stock_days(), 60, kind="stock")
    assert b["session_end"].sum() == 3
    y = st.yearly(b, np.ones(len(b)))
    assert y["turnover"].sum() == pytest.approx(2 * 3)  # in each morning, out at each close
    gap = _stock_days()
    gap.loc[gap.index >= 390, ["open", "high", "low", "close"]] += 10.0  # an overnight gap up
    b2 = st.bars(gap, 60, kind="stock")
    assert st.yearly(b2, np.ones(len(b2)))["gross"].sum() == pytest.approx(0.0)  # never held through the gap


def test_approaches_read_only_closed_bars_and_sides_stay_independent():
    up = pd.DataFrame({"ts": T0 + 60 * np.arange(300), "close": np.linspace(100, 130, 300)})
    up["open"] = up["high"] = up["low"] = up["close"]
    up["high"] *= 1.001
    up["low"] *= 0.999
    pos = st.position(up, "trend", {"fast": 5, "slow": 20}, long_short=True)
    assert (pos[:20] == 0).all() and (pos[25:] == 1).all()
    mom = st.position(up, "momentum", {"n": 3}, long_short=False)
    assert set(np.unique(mom)) <= {0.0, 1.0} and mom[-1] == 1.0
    steep = up.assign(close=np.linspace(100, 160, 300))
    steep["open"], steep["high"], steep["low"] = steep["close"], steep["close"] * 1.0001, steep["close"] * 0.9999
    brk = st.position(steep, "breakout", {"n": 20}, long_short=True)
    assert brk[-1] == 1.0 and (brk >= 0).all()  # new highs never close the long through a short exit
    # Bollinger fades: a sharp drop to -3 sd goes long and is out once back at the mean.
    c = np.r_[np.full(60, 100.0) + np.sin(np.arange(60)) * 0.1, [95.0], np.full(20, 100.0)]
    b = pd.DataFrame({"ts": T0 + 60 * np.arange(len(c)), "close": c, "open": c, "high": c, "low": c})
    boll = st.position(b, "bollinger", {"n": 20, "k": 2.0}, long_short=False)
    assert boll[60] == 1.0 and boll[-1] == 0.0


def _rows(symbol_years, good_from):
    """Two settings: A pays every year, B pays only from `good_from`."""
    rows = []
    for y in symbol_years:
        rows.append({"tf": "1h", "approach": "trend", "params": json.dumps({"fast": 5, "slow": 20}), "sides": "long", "year": y,
                     "gross": 0.05, "turnover": 20.0, "bars_held": 100})
        rows.append({"tf": "1h", "approach": "momentum", "params": json.dumps({"n": 3}), "sides": "long", "year": y,
                     "gross": 0.50 if y >= good_from else -0.30, "turnover": 20.0, "bars_held": 100})
    return pd.DataFrame(rows)


def test_the_walk_forward_never_chooses_with_the_year_it_trades():
    years = list(range(2016, 2026))
    wf = {(r["family"]): r for r in st.walk_forward_symbol(_rows(years, good_from=2025), cost=0.0, sides_allowed=("long",))}
    any_ = wf["any"]
    assert [p["year"] for p in any_["by_year"]] == years[1:]
    assert all(p["pick"].startswith("trend") for p in any_["by_year"])  # momentum's 2025 win was never visible before 2025
    assert any_["oos_total"] == pytest.approx(0.05 * 9)
    costly = {r["family"]: r for r in st.walk_forward_symbol(_rows(years, good_from=2030), cost=0.01, sides_allowed=("long",))}
    assert costly["any"]["oos_total"] == pytest.approx(0.0)  # after costs nothing paid: it stays out


def test_hour_patterns_trade_only_hours_that_paid_in_every_earlier_year():
    recs = [{"year": y, "key": k, "sum": (0.02 if k == 14 else -0.01), "count": 250} for y in range(2021, 2026) for k in (13, 14)]
    hold = pd.Series({("1h", y): 0.01 for y in range(2021, 2026)})
    (wf,) = st.walk_forward_patterns({"hours": recs}, cost=0.0, hold=hold)
    assert wf["family"] == "hours" and wf["last_pick"] == [14] and wf["oos_total"] == pytest.approx(0.02 * 4)
    assert wf["hold_total"] == pytest.approx(0.01 * 4) and wf["excess"] == pytest.approx(0.04)


def test_adoption_needs_years_consistency_and_strength():
    rec = {"oos_years": 8, "oos_total": 0.4, "years_up": 0.875, "t": 3.1, "excess": 0.1}
    assert st.adoptable(rec)
    assert not st.adoptable({**rec, "t": 1.9}) and not st.adoptable({**rec, "years_up": 0.5}) and not st.adoptable({**rec, "oos_years": 2})
    assert not st.adoptable({**rec, "excess": -0.2})  # worse than just holding the ticker


def test_holding_is_the_benchmark_never_a_pick():
    rows = _rows(list(range(2016, 2026)), good_from=2030)
    hold = rows[rows["approach"] == "trend"].assign(approach="hold", params="{}", gross=0.30)  # a bull market: holding made 30%/yr
    wf = {r["family"]: r for r in st.walk_forward_symbol(pd.concat([rows, hold]), cost=0.0, sides_allowed=("long",))}
    assert "hold" not in wf and all(not p["pick"].startswith("hold") for p in wf["any"]["by_year"])
    assert wf["any"]["hold_total"] == pytest.approx(0.30 * 9) and wf["any"]["excess"] == pytest.approx(0.05 * 9 - 0.30 * 9)
    assert not st.adoptable({**wf["any"], "t": 9.0})


def test_summary_scores_each_bot_with_its_own_costs_and_sides():
    per_symbol = {"AAPL": {"rows": _rows(list(range(2016, 2026)), good_from=2030).to_dict("records"), "patterns": {}, "quality": {}}}
    tickers = {"AAPL": {"kind": "stock", "bots": ["stocks"]}}
    bots = st.summarize(per_symbol, tickers)
    assert list(bots) == ["stocks"] and bots["stocks"]["cost_per_side"] == st.BOT_COST_PER_SIDE["stocks"]
    assert bots["stocks"]["best_per_ticker"]["AAPL"]["family"] == "trend"
