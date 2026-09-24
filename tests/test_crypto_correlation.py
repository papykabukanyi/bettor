"""Correlation-derived chart studies (peer confirmation, leader divergence,
breadth) shared between perps_strategy.py and alpaca_crypto_strategy.py --
see crypto_correlation.py's own module docstring for the full cross-market
design. Pure-function tests on synthetic return series with known
relationships (a coin engineered to be highly correlated with a leader, one
engineered to have recently lagged it) so the sign/magnitude of every score
is checked against ground truth, not just "doesn't crash"."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data import crypto_correlation as cc


def _returns_frame(series: dict[str, list[float]], *, id_col: str = "ticker") -> pd.DataFrame:
    rows = []
    for coin, rets in series.items():
        for i, r in enumerate(rets):
            rows.append({id_col: coin, "ts": i, "ret_5m": r})
    return pd.DataFrame(rows)


@pytest.fixture(autouse=True)
def _reset_caches():
    """Every study-cache getter/setter is module-level global state -- reset
    before AND after each test so tests can't leak into each other."""
    cc._PERPS_STUDY = {}
    cc._ALPACA_STUDY = {}
    cc._REMOTE_ALPACA_STUDY = {}
    cc._METALS_STUDY = {}
    cc._LATEST_KALSHI_15M_CRYPTO_DF = pd.DataFrame()
    yield
    cc._PERPS_STUDY = {}
    cc._ALPACA_STUDY = {}
    cc._REMOTE_ALPACA_STUDY = {}
    cc._METALS_STUDY = {}
    cc._LATEST_KALSHI_15M_CRYPTO_DF = pd.DataFrame()


def _synthetic_universe(n: int = 300, seed: int = 0):
    rng = np.random.default_rng(seed)
    btc = rng.normal(0, 0.001, n)
    eth = btc * 0.9 + rng.normal(0, 0.0003, n)  # tightly tracks BTC
    lag = btc.copy()
    lag[-30:] = rng.normal(-0.0006, 0.0002, 30)  # sharply underperforms BTC recently
    lead = btc.copy()
    lead[-30:] = rng.normal(0.0006, 0.0002, 30)  # sharply outperforms BTC recently
    return {"BTC": list(btc), "ETH": list(eth), "LAG": list(lag), "LEAD": list(lead)}


def test_build_study_empty_df_returns_empty_shape():
    result = cc.build_study(pd.DataFrame(), id_col="ticker", leader_id="BTC")
    assert result["ids"] == []
    assert result["corr"] == {}
    assert result["breadth"] is None
    assert result["divergence_z"] == {}


def test_build_study_finds_strong_positive_correlation():
    df = _returns_frame(_synthetic_universe())
    study = cc.build_study(df, id_col="ticker", leader_id="BTC")
    assert study["corr"]["BTC"]["ETH"] > 0.85
    assert study["corr"]["ETH"]["BTC"] > 0.85  # symmetric


def test_build_study_below_min_periods_is_omitted():
    # Fewer rows than MIN_CORR_PERIODS -- a correlation reading here would be
    # noise, not signal, so it must be left out entirely rather than
    # reported with false confidence.
    tiny = {"BTC": [0.001, 0.002, -0.001], "ETH": [0.001, 0.002, -0.001]}
    df = _returns_frame(tiny)
    study = cc.build_study(df, id_col="ticker", leader_id="BTC")
    assert study["corr"] == {}


def test_leader_divergence_flags_the_lagging_coin_as_negative_z():
    df = _returns_frame(_synthetic_universe())
    study = cc.build_study(df, id_col="ticker", leader_id="BTC")
    assert study["divergence_z"]["LAG"] < -1.0
    assert study["divergence_z"]["LEAD"] > 1.0


def test_leader_divergence_bullishness_reads_lag_as_bullish_and_lead_as_bearish():
    df = _returns_frame(_synthetic_universe())
    study = cc.build_study(df, id_col="ticker", leader_id="BTC")
    lag_score, lag_reason = cc._leader_divergence_bullishness(study, "LAG")  # noqa: SLF001
    lead_score, lead_reason = cc._leader_divergence_bullishness(study, "LEAD")  # noqa: SLF001
    assert lag_score > 0  # lagging the leader -> mean-reversion catch-up -> bullish
    assert "lagging" in lag_reason
    assert lead_score < 0  # overextended vs the leader -> mean-reversion pullback -> bearish
    assert "overextended" in lead_reason


def test_leader_divergence_bullishness_missing_data_is_neutral():
    score, reason = cc._leader_divergence_bullishness({}, "ETH")  # noqa: SLF001
    assert score == 0.0
    assert "no leader-divergence data" in reason


def test_peer_confirmation_bullishness_positive_when_correlated_peer_moves_up():
    study = {
        "corr": {"ETH": {"BTC": 0.9}},
        "cum_return": {"BTC": 0.01, "ETH": 0.005},
    }
    score, reason = cc._peer_confirmation_bullishness(study, "ETH")  # noqa: SLF001
    assert score > 0
    assert "BTC" in reason


def test_peer_confirmation_bullishness_negative_when_negatively_correlated_peer_moves_up():
    study = {
        "corr": {"ETH": {"BTC": -0.8}},
        "cum_return": {"BTC": 0.01, "ETH": 0.005},
    }
    score, _ = cc._peer_confirmation_bullishness(study, "ETH")  # noqa: SLF001
    assert score < 0


def test_peer_confirmation_bullishness_ignores_peers_below_min_corr():
    study = {
        "corr": {"ETH": {"BTC": 0.1}},  # below default min_corr=0.4
        "cum_return": {"BTC": 0.01, "ETH": 0.005},
    }
    score, reason = cc._peer_confirmation_bullishness(study, "ETH")  # noqa: SLF001
    assert score == 0.0
    assert "no correlated peers" in reason


def test_breadth_bullishness_matches_raw_breadth_value():
    assert cc._breadth_bullishness({"breadth": 0.6})[0] == 0.6  # noqa: SLF001
    assert cc._breadth_bullishness({"breadth": -0.6})[0] == -0.6  # noqa: SLF001
    assert cc._breadth_bullishness({})[0] == 0.0  # noqa: SLF001


def test_refresh_and_get_perps_study_round_trips():
    df = _returns_frame(_synthetic_universe())
    assert cc.get_perps_study() == {}
    cc.refresh_perps_study(df, id_col="ticker", leader_id="BTC")
    assert cc.get_perps_study()["ids"]


def test_remote_alpaca_study_empty_until_set():
    assert cc.get_remote_alpaca_study() == {}


def test_remote_alpaca_study_set_and_get_round_trips():
    import datetime as dt
    study = {
        "computed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "ids": ["BTC"], "corr": {}, "breadth": 0.1, "divergence_z": {}, "cum_return": {},
    }
    cc.set_remote_alpaca_study(study)
    assert cc.get_remote_alpaca_study() == study


def test_remote_alpaca_study_none_or_empty_input_does_not_clear_existing():
    import datetime as dt
    study = {"computed_at": dt.datetime.now(dt.timezone.utc).isoformat(), "ids": ["BTC"], "corr": {}, "breadth": None, "divergence_z": {}, "cum_return": {}}
    cc.set_remote_alpaca_study(study)
    cc.set_remote_alpaca_study(None)
    cc.set_remote_alpaca_study({})
    assert cc.get_remote_alpaca_study() == study


def test_remote_alpaca_study_treated_as_empty_once_stale(monkeypatch):
    import datetime as dt
    stale_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=cc._REMOTE_ALPACA_STUDY_STALE_AFTER_SEC + 60)  # noqa: SLF001
    cc.set_remote_alpaca_study({"computed_at": stale_at.isoformat(), "ids": ["BTC"], "corr": {}, "breadth": 0.1, "divergence_z": {}, "cum_return": {}})
    assert cc.get_remote_alpaca_study() == {}


def test_perps_correlation_bullishness_blends_local_and_remote_studies():
    local_df = _returns_frame(_synthetic_universe(seed=1))
    cc.refresh_perps_study(local_df, id_col="ticker", leader_id="BTC")

    import datetime as dt
    remote_df = _returns_frame(_synthetic_universe(seed=2))
    remote_study = cc.build_study(remote_df, id_col="ticker", leader_id="BTC")
    remote_study["computed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    cc.set_remote_alpaca_study(remote_study)

    result = cc.perps_correlation_bullishness("LAG")
    assert -1.0 <= result["score"] <= 1.0
    assert set(result["components"]) == {"perps_peers", "alpaca_peers", "alpaca_divergence", "alpaca_breadth", "multi_timeframe"}
    # LAG is engineered to have underperformed its leader in both universes
    # -- the remote leader-divergence component specifically should read
    # bullish (mean-reversion catch-up), since that's the one component this
    # module's docstring says perps should draw most heavily from Alpaca for.
    assert result["components"]["alpaca_divergence"] > 0
    # No row passed -- multi-timeframe component must read neutral, not error.
    assert result["components"]["multi_timeframe"] == 0.0


def test_perps_correlation_bullishness_blends_in_multi_timeframe_row_when_given():
    result_without_row = cc.perps_correlation_bullishness("BTC")
    bullish_row = {"ret_5m": 0.003, "ret_15m": 0.005, "trend_1h": 0.01, "trend_4h": 0.02, "rsi_14": 0.7}
    result_with_row = cc.perps_correlation_bullishness("BTC", bullish_row)
    assert result_with_row["components"]["multi_timeframe"] > 0
    assert result_with_row["score"] > result_without_row["score"]


def test_alpaca_correlation_bullishness_uses_only_the_local_alpaca_study():
    df = _returns_frame(_synthetic_universe(seed=3), id_col="symbol")
    cc.refresh_alpaca_study(df, id_col="symbol", leader_id="BTC")
    result = cc.alpaca_correlation_bullishness("LAG")
    assert -1.0 <= result["score"] <= 1.0
    assert set(result["components"]) == {"peers", "divergence", "breadth", "multi_timeframe"}
    assert result["components"]["divergence"] > 0


def test_format_reason_omits_no_data_components():
    """Real feedback: a coin too new/thin to correlate against anything
    yet used to post a "Why:" reason that was mostly "no ... data"
    placeholders -- only components that actually had something to say
    should show up in the joined text."""
    components = {
        "perps_peers": (0.0, "no correlated peers with data"),
        "alpaca_peers": (0.0, "no correlated peers with data"),
        "alpaca_divergence": (0.4, "lagging BTC (z=-1.20)"),
        "alpaca_breadth": (0.37, "breadth +0.37 (up-skewed)"),
        "multi_timeframe": (0.1, "4/9 timeframes bullish, 5/9 bearish"),
    }
    reason = cc._format_reason(components)  # noqa: SLF001
    assert "no correlated peers with data" not in reason
    assert "lagging BTC" in reason
    assert "breadth +0.37" in reason
    assert "4/9 timeframes" in reason


def test_format_reason_falls_back_to_a_placeholder_when_nothing_has_data():
    components = {
        "perps_peers": (0.0, "no correlated peers with data"),
        "alpaca_divergence": (0.0, "no leader-divergence data"),
        "multi_timeframe": (0.0, "no multi-timeframe data"),
    }
    assert cc._format_reason(components) == "no chart-study signal available yet"  # noqa: SLF001


def test_perps_correlation_bullishness_reason_omits_no_data_components_for_an_unknown_coin():
    """End-to-end: a coin present in neither study still gets a valid,
    UNCHANGED score (each missing component already contributes a neutral
    0.0), but its posted reason text shouldn't be cluttered with "no ...
    data" placeholders for the components that have nothing to say."""
    cc.refresh_perps_study(_returns_frame(_synthetic_universe(seed=1)), id_col="ticker", leader_id="BTC")
    result = cc.perps_correlation_bullishness("NOT_A_REAL_COIN")
    assert result["score"] == 0.0
    assert "no correlated peers with data" not in result["reason"]
    assert "no leader-divergence data" not in result["reason"]
    assert result["reason"] == "no chart-study signal available yet"


def test_study_health_reports_coverage():
    study = {
        "computed_at": "2026-01-01T00:00:00+00:00",
        "ids": ["BTC", "ETH", "WLD"],
        "corr": {"BTC": {"ETH": 0.8}, "ETH": {"BTC": 0.8}},
        "divergence_z": {"ETH": 0.5},
        "breadth": 0.37,
    }
    health = cc.study_health(study)
    assert health == {
        "computed_at": "2026-01-01T00:00:00+00:00", "num_ids": 3,
        "num_with_peer_data": 2, "num_with_divergence_data": 1, "breadth": 0.37,
    }


def test_study_health_on_an_empty_study():
    assert cc.study_health({}) == {
        "computed_at": None, "num_ids": 0, "num_with_peer_data": 0,
        "num_with_divergence_data": 0, "breadth": None,
    }


def test_multi_timeframe_bullishness_none_row_is_neutral():
    score, reason = cc.multi_timeframe_bullishness(None)
    assert score == 0.0
    assert "no multi-timeframe data" in reason


def test_multi_timeframe_bullishness_empty_row_is_neutral():
    score, reason = cc.multi_timeframe_bullishness({})
    assert score == 0.0
    assert "no multi-timeframe data" in reason


def test_multi_timeframe_bullishness_all_timeframes_bullish_gives_max_score():
    row = {
        "ret_5m": 0.003, "ret_15m": 0.005, "ret_30m": 0.008,
        "trend_1h": 0.01, "trend_2h": 0.014, "trend_3h": 0.017, "trend_4h": 0.02,
        "macd_hist_pct": 0.001, "rsi_14": 0.7,
    }
    score, reason = cc.multi_timeframe_bullishness(row)
    assert score == pytest.approx(1.0)
    assert "9/9 timeframes bullish" in reason


def test_multi_timeframe_bullishness_all_timeframes_bearish_gives_min_score():
    row = {
        "ret_5m": -0.003, "ret_15m": -0.005, "ret_30m": -0.008,
        "trend_1h": -0.01, "trend_2h": -0.014, "trend_3h": -0.017, "trend_4h": -0.02,
        "macd_hist_pct": -0.001, "rsi_14": 0.3,
    }
    score, reason = cc.multi_timeframe_bullishness(row)
    assert score == pytest.approx(-1.0)
    assert "0/9 timeframes bullish, 9/9 bearish" in reason


def test_multi_timeframe_bullishness_mixed_timeframes_nets_toward_zero():
    row = {"ret_5m": 0.003, "trend_4h": -0.02}  # one bullish, one bearish, equal magnitude
    score, reason = cc.multi_timeframe_bullishness(row)
    assert score == pytest.approx(0.0)
    assert "1/2 timeframes bullish, 1/2 bearish" in reason


def test_multi_timeframe_bullishness_ignores_missing_and_nan_fields():
    row = {"ret_5m": 0.003, "ret_15m": None, "trend_1h": float("nan")}
    score, reason = cc.multi_timeframe_bullishness(row)
    assert score == pytest.approx(1.0)  # only ret_5m contributes, and it's max-bullish
    assert "1/1 timeframes bullish" in reason


def test_build_study_from_wide_as_of_ts_excludes_later_rows():
    """Leakage-free guard for the backtest's own use of this (see
    perps_backtest.py): a study computed as_of an early timestamp must be
    identical to one computed from a df truncated at that same timestamp --
    later rows must never influence an earlier decision."""
    df = _returns_frame(_synthetic_universe())
    wide = cc._pivot_returns(df, id_col="ticker", ts_col="ts", ret_col="ret_5m")  # noqa: SLF001
    cutoff = 200
    truncated_study = cc.build_study(df[df["ts"] <= cutoff], id_col="ticker", leader_id="BTC")
    as_of_study = cc.build_study_from_wide(wide, as_of_ts=cutoff, leader_id="BTC")
    assert truncated_study["divergence_z"] == as_of_study["divergence_z"]
    assert truncated_study["corr"] == as_of_study["corr"]


def test_build_study_coin_of_mapper_normalizes_id_column():
    df = pd.DataFrame([
        {"ticker": "KXBTCPERP", "ts": i, "ret_5m": v} for i, v in enumerate([0.001] * 40)
    ] + [
        {"ticker": "KXETHPERP", "ts": i, "ret_5m": v} for i, v in enumerate([0.0009] * 40)
    ])
    study = cc.build_study(df, id_col="ticker", leader_id="BTC", coin_of=lambda t: t.replace("KX", "").replace("PERP", ""))
    assert study["ids"] == ["BTC", "ETH"]


# ---------------------------------------------------------------------------
# Metals cross-asset correlation -- per explicit user direction ("use other
# asset for correlation and learn what other things correlate with
# GOLD/SILVER/COPPER[,] all possibilit[y]"). refresh_metals_study's new
# crypto_df param folds kalshi_15m's own crypto universe into the SAME
# study as extra candidate peers -- see its own docstring for why no
# other function needs to change.
# ---------------------------------------------------------------------------
def _symbol_frame(symbol: str, ts_values: list[float], rets: list[float]) -> pd.DataFrame:
    return pd.DataFrame({"symbol": [symbol] * len(rets), "ts": ts_values, "ret_5m": rets})


def test_latest_kalshi_15m_crypto_df_round_trips():
    df = _symbol_frame("BTC", [0, 300], [0.001, 0.002])
    cc.set_latest_kalshi_15m_crypto_df(df)
    pd.testing.assert_frame_equal(cc.get_latest_kalshi_15m_crypto_df(), df)


def test_latest_kalshi_15m_crypto_df_starts_empty():
    assert cc.get_latest_kalshi_15m_crypto_df().empty


def test_latest_kalshi_15m_crypto_df_empty_input_does_not_clear_existing():
    df = _symbol_frame("BTC", [0, 300], [0.001, 0.002])
    cc.set_latest_kalshi_15m_crypto_df(df)
    cc.set_latest_kalshi_15m_crypto_df(pd.DataFrame())
    pd.testing.assert_frame_equal(cc.get_latest_kalshi_15m_crypto_df(), df)


def test_bucket_ts_for_join_rounds_down_to_the_5_minute_grid():
    df = _symbol_frame("GOLD", [10, 299, 300], [0.001, 0.002, 0.003])
    bucketed = cc._bucket_ts_for_join(df)  # noqa: SLF001
    assert list(bucketed["ts"]) == [0, 300]  # 10 and 299 collapse into the same 0-bucket


def test_bucket_ts_for_join_keeps_the_latest_row_within_a_collapsed_bucket():
    df = _symbol_frame("GOLD", [10, 250], [0.001, 0.999])
    bucketed = cc._bucket_ts_for_join(df)  # noqa: SLF001
    assert len(bucketed) == 1
    assert bucketed.iloc[0]["ret_5m"] == pytest.approx(0.999)  # the later (ts=250) row wins


def test_bucket_ts_for_join_empty_or_none_returns_empty():
    assert cc._bucket_ts_for_join(pd.DataFrame()).empty  # noqa: SLF001
    assert cc._bucket_ts_for_join(None).empty  # noqa: SLF001


def test_bucket_ts_for_join_missing_required_columns_passes_through_unchanged():
    # No "symbol" column -- can't bucket for a join, so this is returned
    # as-is (fails safe: pass through unusable-for-joining data rather
    # than silently dropping it) instead of being emptied out.
    df = pd.DataFrame({"ts": [1]})
    result = cc._bucket_ts_for_join(df)  # noqa: SLF001
    pd.testing.assert_frame_equal(result, df)


def test_refresh_metals_study_without_crypto_df_is_unchanged():
    metals_df = _returns_frame(_synthetic_universe(), id_col="symbol")
    study = cc.refresh_metals_study(metals_df, leader_id="BTC")
    assert cc.get_metals_study() is study
    assert study["corr"]["BTC"]["ETH"] > 0.85


def test_refresh_metals_study_folds_in_a_real_cross_asset_correlation():
    rng = np.random.default_rng(7)
    n = 300
    gold = rng.normal(0, 0.001, n)
    btc = gold * 0.9 + rng.normal(0, 0.0003, n)  # engineered to track GOLD tightly
    # Metals collect on a clean 5-minute grid; crypto collects offset by
    # 10s -- both must land in the SAME 5-minute bucket after rounding.
    metals_df = _symbol_frame("GOLD", [300 * i for i in range(n)], list(gold))
    crypto_df = _symbol_frame("BTC", [300 * i + 10 for i in range(n)], list(btc))

    study = cc.refresh_metals_study(metals_df, leader_id="GOLD", crypto_df=crypto_df)

    assert study["corr"]["GOLD"]["BTC"] > 0.85
    assert study["corr"]["BTC"]["GOLD"] > 0.85  # symmetric


def test_refresh_metals_study_ignores_an_empty_crypto_df():
    metals_df = _returns_frame(_synthetic_universe(), id_col="symbol")
    without = cc.build_study(metals_df, id_col="symbol", leader_id="BTC")
    with_empty = cc.refresh_metals_study(metals_df, leader_id="BTC", crypto_df=pd.DataFrame())
    assert with_empty["corr"] == without["corr"]
    assert with_empty["ids"] == without["ids"]


def test_metals_correlation_bullishness_picks_up_a_real_cross_asset_peer():
    """End-to-end: once refresh_metals_study has folded crypto in,
    metals_correlation_bullishness (called with a metal, exactly as
    kalshi_15m_strategy.evaluate_candidate does) sees BTC as a real
    correlated peer with zero changes needed to that function."""
    rng = np.random.default_rng(11)
    n = 300
    gold = rng.normal(0, 0.001, n)
    btc = gold * 0.9 + rng.normal(0, 0.0003, n)
    metals_df = _symbol_frame("GOLD", [300 * i for i in range(n)], list(gold))
    crypto_df = _symbol_frame("BTC", [300 * i + 10 for i in range(n)], list(btc))
    cc.refresh_metals_study(metals_df, leader_id="GOLD", crypto_df=crypto_df)

    result = cc.metals_correlation_bullishness("GOLD")
    assert "BTC" in result["reason"]
