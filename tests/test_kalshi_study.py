"""The perps / 15m multi-year study on Alpaca's archives: entry conditions
(hour, weekday, news, leader), learned walk-forward, enforced live only
when they beat the trained result out of sample."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data import setup_backtest_job as job

T_SUN_10UTC = int(pd.Timestamp("2026-09-27T10:00:00Z").timestamp())  # a Sunday


@pytest.fixture(autouse=True)
def _local_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(job, "LOCAL_DIR", tmp_path)
    monkeypatch.delenv("HF_API_KEY", raising=False)


def test_entry_conditions_are_bucketed_relative_to_the_trade():
    f = job.pattern_features(ts=T_SUN_10UTC, side="long", news_count=3, news_score=0.4, leader_corr=0.8, leader_dir="down")
    assert f == {"hour_block": "h08", "weekday": "sun", "news": "with", "leader": "against"}
    f = job.pattern_features(ts=T_SUN_10UTC, side="short", news_count=3, news_score=0.4, leader_corr=0.8, leader_dir="down")
    assert f["news"] == "against" and f["leader"] == "with"
    assert job.pattern_features(ts=T_SUN_10UTC, side="long", news_count=0, news_score=0.0, leader_corr=0.2,
                                leader_dir="up")["news"] == "none"
    assert job.pattern_features(ts=T_SUN_10UTC, side="long", news_count=2, news_score=0.0, leader_corr=0.2,
                                leader_dir="up")["leader"] == "independent"
    assert job.pattern_features(ts=T_SUN_10UTC, side="long", news_count=None, news_score=None, leader_corr=None,
                                leader_dir=None)["leader"] == "n/a"
    assert job.blocked_reason({"weekday": ["sun"]}, f) == "weekday=sun" and job.blocked_reason({}, f) is None


def _pattern_trades():
    rows = []
    rng = np.random.default_rng(1)
    for year in (2023, 2024, 2025, 2026):
        base = int(pd.Timestamp(f"{year}-03-01", tz="UTC").timestamp())
        for i in range(120):
            weekday = "sun" if i % 4 == 0 else "tue"
            # The setup pays on weekdays and loses on Sundays, every year.
            r = (-0.004 if weekday == "sun" else 0.003) + rng.normal(0, 0.001)
            rows.append({"symbol": "BTC", "param": "1.5:3.0", "entry_ts": base + i * 3600, "net_return": r,
                         "hour_block": "h08", "weekday": weekday, "news": "none", "leader": "with"})
    return pd.DataFrame(rows)


def test_losing_conditions_are_learned_from_prior_years_only_and_beat_the_trained_result():
    pt = job.walk_forward_patterns(_pattern_trades(), default_param="1.5:3.0", lookback_years=1)
    assert [y["year"] for y in pt["years"]] == [2024, 2025, 2026]
    assert all(y["blocked"] == {"weekday": ["sun"]} for y in pt["years"])
    assert pt["with_patterns"]["avg"] > pt["trained"]["avg"] > 0
    assert pt["blocked_now"] == {"weekday": ["sun"]} and pt["enforce"] is True
    assert pt["by_condition"]["weekday"]["sun"]["avg"] < 0 < pt["by_condition"]["weekday"]["tue"]["avg"]


def test_the_bot_enforces_patterns_only_when_they_beat_training():
    pt = job.walk_forward_patterns(_pattern_trades(), default_param="1.5:3.0", lookback_years=1)
    result = {"computed_at": "x", "grid": ["1.5:3.0"], "walk_forward": {"enforce": False, "out_of_sample": {"avg": 0.001}},
              "trained": {"enforce": False}, "patterns": pt}
    e = job._eligibility_from(result)  # noqa: SLF001
    assert e["source"] == "patterns" and e["enforce"] and e["blocked"] == {"weekday": ["sun"]} and e["symbols"] == ["BTC"]
    result["patterns"] = {**pt, "enforce": False}
    assert job._eligibility_from(result)["blocked"] == {}  # noqa: SLF001


def test_kalshi_study_symbols_are_what_alpaca_charts():
    perps = job._study_symbols("perps")  # noqa: SLF001
    assert {"BTC", "ETH", "SHIB", "GOLD", "SILVER"} <= set(perps) and not {"NEAR", "SUI", "ZEC"} & set(perps)
    k15 = job._study_symbols("kalshi15m")  # noqa: SLF001
    assert {"BTC", "ADA", "GOLD", "WTI", "NATGAS"} <= set(k15) and "NEAR" not in k15
    assert (1.5, 3.0) in job.study_grid("perps") and (0.5, 2.0) in job.study_grid("kalshi15m")
    assert len(job.study_grid("perps")) == 16 and (2.0, 4.0) in job.study_grid("kalshi15m")
    assert job.study_grid("stocks") == list(job.PARAM_GRID)


def test_a_kalshi_study_symbol_is_replayed_per_setting_and_annotated(monkeypatch):
    from data import alpaca_news_history, perps_setup
    candles = pd.read_parquet(__import__("pathlib").Path(__file__).parent / "fixtures" / "setup_btc_1m.parquet")
    monkeypatch.setattr(job, "_study_candles", lambda sym: (candles, "utc_day"))
    t0 = int(candles.ts.iloc[-1])
    monkeypatch.setattr(alpaca_news_history, "load", lambda **kw: pd.DataFrame(
        {"id": [1], "created_at": [t0 - 600], "headline": [""], "summary": [""], "symbols": ["BTCUSD"], "source": [""],
         "url": [""], "score": [0.5]}))

    def fake_replay(df, **kw):
        assert kw["session"] == "utc_day" and kw["leader_symbol"] == "ETH"
        return pd.DataFrame([{"entry_ts": t0, "side": "long", "net_return": 0.01, "leader_corr": 0.9, "leader_dir": "up"}])

    monkeypatch.setattr(perps_setup, "replay", fake_replay)
    out = job._multiyear_kalshi_symbol(("perps", "BTC", [(1.0, 2.0), (1.5, 3.0)], {"fee_rate_roundtrip": 0.001, "spread_bps": 5.0}))  # noqa: SLF001
    assert sorted(out.param) == ["1.0:2.0", "1.5:3.0"]
    assert set(out.news) == {"with"} and set(out.leader) == {"with"} and set(out.news_count) == {1.0}
    assert (perps_setup.STOP_BUFFER_ATR15, perps_setup.MIN_RR) == (1.5, 3.0)  # defaults restored


def test_15m_windows_settle_on_the_real_close_and_price_contracts_at_fair_value(monkeypatch):
    from data import kalshi_15m_setup as k
    t_open = 1_790_000_100 // 900 * 900
    n = 300
    ts = t_open - 200 * 60 + 60 * np.arange(n)
    close = 100.0 + 0.01 * np.arange(n)  # rising: a long setup wins
    spot = pd.DataFrame({"ts": ts, "open": close, "high": close + 0.005, "low": close - 0.005, "close": close, "volume": 1.0})
    calls = []

    def fake_eval(ctx, as_of, **kw):
        calls.append(as_of)
        if as_of != k.latest_closed_5m(t_open + 60):
            return {"valid": False}
        return {"valid": True, "side": "long", "setup": "breakout_retest", "setup_id": "s1",
                "plan": {"stop": 90.0, "target": 1000.0}, "checks": {"correlation": {"corr": 0.7, "leader_dir": "up"}}}

    monkeypatch.setattr(k, "evaluate", fake_eval)
    monkeypatch.setattr(k, "contract_plan", lambda setup, market, **kw: {"ok": True, "contract_side": "yes",
                                                                         "ask": float(market["yes_ask_dollars"]), "rr": 3.0})
    t = k.replay_windows(spot, half_spread=0.02)
    row = t[t.open_ts == t_open].iloc[0]
    assert row.result == "yes" and row.exit == "settled" and row.minute == 1
    assert row.pnl_per_contract == pytest.approx(1.0 - row.ask - __import__("data.kalshi_15m", fromlist=["x"]).taker_fee_usd(10, row.ask) / 10)
    assert row.net_return == pytest.approx(row.pnl_per_contract / row.ask) and row.leader_dir == "up"


@pytest.mark.parametrize("module_name", ["perps_setup", "kalshi_15m_setup"])
def test_the_setting_free_cache_changes_nothing_but_the_work(module_name):
    """Every plan setting's replay is identical with and without the cache,
    and the cache answers most bars after the first setting."""
    import importlib
    from pathlib import Path
    m = importlib.import_module(f"data.{module_name}")
    candles = pd.read_parquet(Path(__file__).parent / "fixtures" / "setup_btc_1m.parquet")
    default = (m.STOP_BUFFER_ATR15, m.MIN_RR)
    grid = [(0.5, 1.5), (1.0, 2.0), (2.0, 4.0)]

    def run():
        out = []
        for sb, rr in grid:
            m.STOP_BUFFER_ATR15, m.MIN_RR = sb, rr
            if module_name == "perps_setup":
                t = m.replay(candles, sides=("long", "short"), fee_rate_roundtrip=0.0008, spread_bps=10.0)
            else:
                t = m.replay_windows(candles, half_spread=0.01)
            out.append(t.reset_index(drop=True))
        m.STOP_BUFFER_ATR15, m.MIN_RR = default
        return out

    plain = run()
    with job._SettingFreeCache(m) as cache:  # noqa: SLF001
        cached = run()
    assert m.evaluate is cache.original
    for a, b in zip(plain, cached):
        pd.testing.assert_frame_equal(a, b)
    assert cache.hits > cache.misses


def test_crypto_15m_settles_on_the_minute_mean_and_commodities_on_the_close():
    from data import kalshi_15m_setup as k
    assert k.settles_on_minute_average("BTC") and not k.settles_on_minute_average("GOLD")
    one_min = pd.DataFrame({"ts": [1_790_000_040, 1_790_000_100], "open": [10.0, 20.0], "high": [12.0, 24.0],
                            "low": [9.0, 18.0], "close": [11.0, 22.0], "volume": [1.0, 1.0]})
    assert k.price_at(one_min, 1_790_000_100) == 22.0
    assert k.price_at(one_min, 1_790_000_100, average=True) == pytest.approx(21.0)


def test_a_study_of_an_older_version_is_run_again(monkeypatch):
    launched = []
    monkeypatch.setattr(job, "launch", lambda name: launched.append(name) or {"action": "launched"})
    monkeypatch.setattr(job, "archive_ready", lambda bot: True)
    monkeypatch.setattr(job, "eligibility", lambda bot: None)
    monkeypatch.setattr(job, "_running", lambda name: False)
    grid = [f"{a}:{b}" for a, b in job.study_grid("kalshi15m")]
    import json as _json
    (job.LOCAL_DIR / "kalshi15m_multiyear.json").write_text(_json.dumps({"grid": grid}))  # version 1
    job.maybe_start_multiyear("kalshi15m")
    assert launched == ["kalshi15m_multiyear"]
    launched.clear()
    (job.LOCAL_DIR / "kalshi15m_multiyear.json").write_text(_json.dumps({"grid": grid, "version": job.STUDY_VERSION["kalshi15m"]}))
    assert job.maybe_start_multiyear("kalshi15m")["action"] == "already_published" and launched == []
