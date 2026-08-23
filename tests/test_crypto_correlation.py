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
    yield
    cc._PERPS_STUDY = {}
    cc._ALPACA_STUDY = {}
    cc._REMOTE_ALPACA_STUDY = {}


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
