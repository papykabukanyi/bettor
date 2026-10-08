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
    assert f == {"hour_block": "h08", "weekday": "sun", "news": "with", "leader": "against", "side": "long", "vol_regime": "n/a",
                 "us_market": "n/a"}
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
    assert len(job.study_grid("kalshi15m")) == 16 and (2.0, 4.0) in job.study_grid("kalshi15m")
    # perps: wider stops (to 4x) and, on each, 6 exit rules -- time exits and the break-even stop
    assert len(job.study_grid("perps")) == 28 and (4.0, 3.0) in job.study_grid("perps")
    assert len(job.grid_labels("perps")) == 168 and job.default_param("perps") in job.grid_labels("perps")
    assert "1.5:3.0:8.0:1.0" in job.grid_labels("perps")
    # Stocks and options study the same 16 settings as the Kalshi bots, on every prior year.
    assert job.study_grid("stocks") == job.study_grid("kalshi15m") or len(job.study_grid("stocks")) >= 16
    assert job.MULTIYEAR["stocks"]["lookback"] == 0 and "stocks" in job.ARCHIVE_STUDY_BOTS


def test_a_kalshi_study_symbol_is_replayed_per_setting_and_annotated(monkeypatch):
    from data import alpaca_news_history, perps_setup
    candles = pd.read_parquet(__import__("pathlib").Path(__file__).parent / "fixtures" / "setup_btc_1m.parquet")
    monkeypatch.setattr(job, "_study_candles", lambda sym, bot=None: (candles, "utc_day"))
    t0 = int(candles.ts.iloc[-1])
    news = pd.DataFrame({"id": [1], "created_at": [t0 - 600], "headline": [""], "summary": [""], "symbols": ["BTCUSD"],
                         "source": [""], "url": [""], "score": [0.5]})
    monkeypatch.setattr(alpaca_news_history, "index_for", lambda symbols: alpaca_news_history.NewsIndex(news, symbols))

    def fake_replay(df, **kw):
        assert kw["session"] == "utc_day" and kw["leader_symbol"] == "ETH"
        return pd.DataFrame([{"entry_ts": t0, "side": "long", "net_return": 0.01, "leader_corr": 0.9, "leader_dir": "up"}])

    monkeypatch.setattr(perps_setup, "replay", fake_replay)
    out = job._multiyear_kalshi_symbol(("perps", "BTC", [(1.0, 2.0), (1.5, 3.0)], {"fee_rate_roundtrip": 0.001, "spread_bps": 5.0}))  # noqa: SLF001
    assert sorted(out.param) == ["1.0:2.0", "1.5:3.0"]
    assert set(out.news) == {"with"} and set(out.leader) == {"with"} and set(out.news_count) == {1.0}
    assert (perps_setup.STOP_BUFFER_ATR15, perps_setup.MIN_RR) == (1.5, 3.0)  # defaults restored
    # One column per condition: the replay's own `side` once (a second copy
    # broke every study's analysis -- "Grouper for 'side' not 1-dimensional").
    assert list(out.columns).count("side") == 1
    job.learn_blocked(out, min_trades=1)


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
    monkeypatch.setattr(job, "STUDY_PRIORITY", [])  # the order between bots is tested on its own
    launched = []
    monkeypatch.setattr(job, "launch", lambda name: launched.append(name) or {"action": "launched"})
    monkeypatch.setattr(job, "archive_ready", lambda bot: True)
    monkeypatch.setattr(job, "eligibility", lambda bot: None)
    monkeypatch.setattr(job, "_running", lambda name: False)
    grid = job.grid_labels("kalshi15m")
    import json as _json
    (job.LOCAL_DIR / "kalshi15m_multiyear.json").write_text(_json.dumps({"grid": grid}))  # version 1
    job.maybe_start_multiyear("kalshi15m")
    assert launched == ["kalshi15m_multiyear"]
    launched.clear()
    (job.LOCAL_DIR / "kalshi15m_multiyear.json").write_text(_json.dumps({"grid": grid, "version": job.STUDY_VERSION["kalshi15m"]}))
    assert job.maybe_start_multiyear("kalshi15m")["action"] == "already_published" and launched == []



def test_every_prior_year_teaches_each_year_with_the_expanding_window():
    pt = job.walk_forward_patterns(_pattern_trades(), default_param="1.5:3.0", lookback_years=0)
    assert [y["year"] for y in pt["years"]] == [2024, 2025, 2026]
    assert all(y["blocked"] == {"weekday": ["sun"]} for y in pt["years"]) and pt["enforce"] is True
    assert "every prior year" in pt["rule"]


def test_volatility_regime_has_no_lookahead_and_matches_live():
    rng = np.random.default_rng(3)
    n = 1440 * 120
    ts = 1_700_000_000 + 60 * np.arange(n)
    sigma = np.where(np.arange(n) < n - 1440 * 5, 0.0005, 0.003)  # the last 5 days turn wild
    close = 100 * np.exp(np.cumsum(rng.normal(0, sigma)))
    candles = pd.DataFrame({"ts": ts, "open": close, "high": close, "low": close, "close": close, "volume": 1.0})
    regimes, now = job.vol_regimes(candles, [ts[1440 * 100], ts[-1]], "utc_day")
    assert regimes[0] in ("low", "normal", "high") and regimes[1] == "high"
    assert now and now[0] < now[1]
    assert job.vol_regime_now(candles.tail(2000), now, "utc_day") == "high"
    assert job.vol_regime_now(candles.tail(2000), None, "utc_day") == "n/a"


def test_the_us_market_condition_reads_spy_without_lookahead():
    # SPY's 2026-10-01 session (EDT): opens 13:30 UTC; rises, then falls below the open.
    t0 = int(pd.Timestamp("2026-10-01T13:31:00Z").timestamp())  # end of the first minute
    spy = pd.DataFrame({"ts": [t0, t0 + 60, t0 + 3600], "open": [100.0, 100.5, 99.0], "high": [100.6, 101.0, 99.5],
                        "low": [99.9, 100.4, 98.8], "close": [100.5, 100.9, 99.2], "volume": [1.0, 1.0, 1.0]})
    states = job.us_market_states(spy, [t0 + 120, t0 + 3660, int(pd.Timestamp("2026-10-03T15:00:00Z").timestamp())])
    assert states == ["up", "down", "closed"]  # a Saturday is closed
    assert job.us_market_states(None, [t0]) == ["n/a"]


def test_the_crypto_bot_is_studied_long_only_on_its_pairs(monkeypatch):
    from data import alpaca_crypto_history, alpaca_crypto_setup
    assert job.MULTIYEAR["crypto"]["sides"] == ("long",) and job.MULTIYEAR["crypto"]["lookback"] == 0
    assert "crypto" in job.ARCHIVE_STUDY_BOTS and len(job.study_grid("crypto")) >= 16
    monkeypatch.setattr(alpaca_crypto_history, "crypto_bot_coins", lambda: ["AVAX", "UNI"])
    monkeypatch.setattr(alpaca_crypto_history, "load", lambda coin, years=None: pd.DataFrame({"ts": [1]}) if coin != "UNI" else pd.DataFrame())
    syms = job._study_symbols("crypto")  # noqa: SLF001
    assert "AVAX/USD" in syms and "BTC/USD" in syms and "UNI/USD" not in syms  # no archive, not studied
    assert alpaca_crypto_setup.leader_for("AVAX/USD") == "BTC/USD"


def test_the_crypto_bot_skips_a_condition_its_study_found_losing(monkeypatch):
    from data import alpaca_crypto_setup, alpaca_crypto_strategy, alpaca_news
    monkeypatch.setattr(alpaca_news, "sentiment", lambda s: {"sentiment_score": 0.0, "headline_volume": 0})
    monkeypatch.setattr(alpaca_crypto_setup, "live_setup", lambda symbol, **kw: {
        "valid": True, "side": "long", "setup": "breakout_retest", "setup_id": "x1", "plan": {"stop": 1.0, "target": 2.0, "rr_net": 2.5},
        "checks": {"correlation": {"corr": 0.8, "leader_dir": "up"}}, "chart_source": "test", "chart_price": 1.5})
    monkeypatch.setattr(job, "us_market_now", lambda now=None: "down")
    monkeypatch.setattr(job, "eligibility", lambda bot: {"enforce": True, "symbols": ["AVAX/USD"], "rule": "r",
                                                          "blocked": {"us_market": ["down"]}})
    r = alpaca_crypto_strategy.evaluate_setup_candidate("AVAX/USDT")
    assert r["should_enter"] is False and "us_market=down" in r["reason"]
    monkeypatch.setattr(job, "eligibility", lambda bot: {"enforce": True, "symbols": ["BTC/USD"], "rule": "r"})
    assert "not eligible" in alpaca_crypto_strategy.evaluate_setup_candidate("AVAX/USD")["reason"]
    monkeypatch.setattr(job, "eligibility", lambda bot: None)
    assert alpaca_crypto_strategy.evaluate_setup_candidate("AVAX/USD")["should_enter"] is True  # nothing proven: nothing enforced


def test_perps_exit_rules_are_replayed_on_the_same_entries(monkeypatch):
    """One pass, one entry, three exit rules on their own timelines: the
    plain plan rides to the end, the 1-hour hold closes on time, the
    break-even stop closes at entry after the reversal."""
    import numpy as np
    from data import perps_setup
    t0 = 1_790_000_000 // 300 * 300
    n = 240
    close = np.full(n, 100.0)
    close[40:50] = 101.2   # +1.2R (stop 99): break-even triggers
    close[50:60] = 99.8    # back below entry, above the 99 stop
    close[60:] = 100.5
    df = pd.DataFrame({"ts": t0 + 60 * (np.arange(n) + 1), "open": np.r_[100.0, close[:-1]], "high": close + 0.05,
                       "low": close - 0.05, "close": close, "volume": 1.0})
    entry_at = int(t0 + 30 * 60)

    def fake_evaluate(ctx, as_of, **kw):
        if int(as_of) != entry_at:
            return {"valid": False, "reason": "trend"}
        return {"valid": True, "side": "long", "setup_id": "s1", "setup": "breakout", "checks": {},
                "plan": {"stop": 99.0, "target": 110.0, "rr_net": 9.0}}

    monkeypatch.setattr(perps_setup, "evaluate", fake_evaluate)
    t = perps_setup.replay(df, sides=("long",), fee_rate_roundtrip=0.0, spread_bps=0.0,
                           exits=[(1440.0, 0.0), (60.0, 0.0), (1440.0, 1.0)])
    by = {(r.hold_h, r.be_r): r for r in t.itertuples()}
    assert len(t) == 3 and set(t.entry_ts) == {entry_at + 60}
    assert by[(24.0, 0.0)].exit == "max_hold_safety" and by[(24.0, 0.0)].gross_return == pytest.approx(0.005)
    assert by[(1.0, 0.0)].exit == "time_exit"
    assert by[(24.0, 1.0)].exit == "breakeven" and by[(24.0, 1.0)].gross_return == pytest.approx(0.0)


def test_a_perps_study_trade_carries_its_full_setting(monkeypatch):
    from data import alpaca_news_history, perps_setup
    candles = pd.read_parquet(__import__("pathlib").Path(__file__).parent / "fixtures" / "setup_btc_1m.parquet")
    monkeypatch.setattr(job, "_study_candles", lambda sym, bot=None: (candles, "utc_day"))
    monkeypatch.setattr(alpaca_news_history, "index_for", lambda symbols: None)
    t0 = int(candles.ts.iloc[-1])
    seen = []

    def fake_replay(df, **kw):
        seen.append(kw["exits"])
        return pd.DataFrame([{"entry_ts": t0, "side": "long", "net_return": 0.01, "leader_corr": 0.9, "leader_dir": "up",
                              "hold_h": h / 60.0, "be_r": b} for h, b in kw["exits"]])

    monkeypatch.setattr(perps_setup, "replay", fake_replay)
    out = job._multiyear_kalshi_symbol(("perps", "BTC", [(1.5, 3.0)], {"fee_rate_roundtrip": 0.001, "spread_bps": 5.0}))  # noqa: SLF001
    assert seen == [[(h * 60.0, b) for h, b in job.PERPS_EXITS]]
    assert sorted(out.param) == sorted(job._param_label(1.5, 3.0, h, b) for h, b in job.PERPS_EXITS)  # noqa: SLF001
    assert job._param_values("2.5:3.0:8.0:1.0") == {"STOP_BUFFER_ATR15": 2.5, "MIN_RR": 3.0, "MAX_HOLD_HOURS": 8.0, "BREAKEVEN_R": 1.0}  # noqa: SLF001


def test_a_stock_study_symbol_is_replayed_on_sip_with_its_session_rules(monkeypatch):
    """Stocks and options run the same full-history study as the other
    bots: SIP regular session, the module's session rules, every setting,
    each trade with its entry conditions."""
    from data import alpaca_news_history, alpaca_setup
    candles = pd.read_parquet(__import__("pathlib").Path(__file__).parent / "fixtures" / "setup_btc_1m.parquet")
    seen = []
    monkeypatch.setattr(job, "_study_candles", lambda sym, bot=None: seen.append((sym, bot)) or (candles, "us_equity"))
    monkeypatch.setattr(alpaca_news_history, "index_for", lambda symbols: None)
    t0 = int(candles.ts.iloc[-1])

    def fake_replay(df, **kw):
        assert kw["entry_allowed"] is alpaca_setup.entry_allowed and kw["force_exit"] is alpaca_setup.must_be_flat
        return pd.DataFrame([{"entry_ts": t0, "side": "long", "net_return": 0.01, "leader_corr": 0.9, "leader_dir": "up"}])

    monkeypatch.setattr(alpaca_setup, "replay", fake_replay)
    out = job._multiyear_kalshi_symbol(("stocks", "AAPL", [(0.5, 2.0), (1.0, 3.0)], {"fee_rate_roundtrip": 0.0, "spread_bps": 2.0}))  # noqa: SLF001
    assert sorted(out.param) == ["0.5:2.0", "1.0:3.0"] and {"hour_block", "weekday", "us_market"} <= set(out.columns)
    assert ("AAPL", "stocks") in seen and (alpaca_setup.leader_for("AAPL"), "stocks") in seen
