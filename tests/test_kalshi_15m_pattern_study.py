"""kalshi_15m_pattern_study: per-coin up-rate profiles with overlap-aware
significance, and a loader that never trains inline."""
from __future__ import annotations

import datetime as dt
import json

import numpy as np
import pandas as pd
import pytest

from data import kalshi_15m_pattern_study as ps


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(ps, "_PROFILES_LOCAL_PATH", tmp_path / "profiles.json")
    monkeypatch.setattr(ps, "HF_API_KEY", "")
    monkeypatch.setattr(ps, "_cache", {"profiles": None, "last_remote_attempt": 0.0})


def _rows(n, *, symbol="BTC", start_ts=1_790_000_000, label=None, rng=None, **overrides):
    rng = rng or np.random.default_rng(0)
    base = {
        "symbol": symbol, "trend_1h": 0.01, "ret_15m": 0.005, "volatility_15": 0.0005, "rsi_14": 0.5,
        "dollar_volume_z": 0.2, "macd_hist_pct": 0.0001,
    }
    base.update(overrides)
    labels = rng.integers(0, 2, n) if label is None else np.full(n, label)
    return pd.DataFrame([{**base, "ts": start_ts + 60 * i, "label_up": float(labels[i])} for i in range(n)])


def test_et_hour_follows_daylight_saving():
    summer = dt.datetime(2026, 7, 1, 16, 0, tzinfo=dt.timezone.utc).timestamp()
    winter = dt.datetime(2026, 12, 1, 16, 0, tzinfo=dt.timezone.utc).timestamp()
    assert ps._et_hour_from_ts(summer) == 12
    assert ps._et_hour_from_ts(winter) == 11


@pytest.mark.parametrize("fn,value,expected", [
    (ps._vol_regime, 0.0002, "low"), (ps._vol_regime, 0.0005, "medium"), (ps._vol_regime, 0.002, "high"),
    (ps._rsi_regime, 0.2, "oversold"), (ps._rsi_regime, 0.5, "neutral"), (ps._rsi_regime, 0.8, "overbought"),
    (ps._volume_regime, 1.5, "high"), (ps._volume_regime, 0.1, "normal"),
    (ps._macd_regime, 0.001, "bullish"), (ps._macd_regime, -0.001, "bearish"),
    (ps._cascade_regime, 1.0, "strong_up"), (ps._cascade_regime, 0.25, "weak_up"),
    (ps._cascade_regime, -0.5, "weak_down"), (ps._cascade_regime, -1.0, "strong_down"),
])
def test_bucket_helpers(fn, value, expected):
    assert fn(value) == expected
    assert fn(None) is None
    assert fn(float("nan")) is None


def test_trend_alignment():
    assert ps._trend_alignment(0.01, 0.002) == "aligned_up"
    assert ps._trend_alignment(-0.01, -0.002) == "aligned_down"
    assert ps._trend_alignment(0.01, -0.002) == "misaligned"
    assert ps._trend_alignment(None, 0.002) is None


def test_mtf_cascade_score_needs_four_timeframes():
    assert ps.mtf_cascade_score([0.1, 0.2, -0.1]) is None
    assert ps.mtf_cascade_score([0.1, 0.2, 0.3, 0.4]) == 1.0
    assert ps.mtf_cascade_score([0.1, -0.2, 0.3, -0.4, None]) == 0.0


def test_analyze_patterns_requires_columns():
    assert ps.analyze_patterns(pd.DataFrame([{"symbol": "BTC", "ts": 1, "label_up": 1}])) == {}


def test_analyze_patterns_skips_coins_below_the_effective_sample_floor():
    n = ps.LABEL_OVERLAP_MINUTES * ps.MIN_EFFECTIVE_SAMPLES - 1
    assert ps.analyze_patterns(_rows(n)) == {}


def test_analyze_patterns_counts_effective_samples_not_overlapping_rows():
    n = ps.LABEL_OVERLAP_MINUTES * ps.MIN_EFFECTIVE_SAMPLES * 2
    profile = ps.analyze_patterns(_rows(n))["BTC"]
    assert profile["total_samples"] == n
    assert profile["effective_samples"] == pytest.approx(n / ps.LABEL_OVERLAP_MINUTES)
    for key in ("by_hour", "by_vol_regime", "by_rsi_regime", "by_trend_alignment", "by_volume_regime", "by_macd_regime", "by_cascade_regime"):
        assert key in profile


def test_a_strong_real_difference_is_significant_and_noise_is_not():
    rng = np.random.default_rng(3)
    up = _rows(3000, trend_1h=0.01, ret_15m=0.005, label=1, rng=rng)
    down = _rows(3000, trend_1h=0.01, ret_15m=-0.005, label=0, rng=rng, start_ts=1_790_500_000)
    noise = _rows(6000, trend_1h=-0.01, ret_15m=-0.005, rng=rng, start_ts=1_791_000_000)
    profile = ps.analyze_patterns(pd.concat([up, down, noise], ignore_index=True))["BTC"]
    assert profile["by_trend_alignment"]["aligned_up"]["significant"] is True
    assert profile["by_trend_alignment"]["misaligned"]["significant"] is True
    assert profile["by_trend_alignment"]["aligned_down"]["significant"] is False


def test_small_buckets_with_extreme_rates_are_not_significant():
    rng = np.random.default_rng(4)
    big = _rows(6000, rsi_14=0.5, rng=rng)
    tiny = _rows(60, rsi_14=0.2, label=1, rng=rng, start_ts=1_799_000_000)
    profile = ps.analyze_patterns(pd.concat([big, tiny], ignore_index=True))["BTC"]
    assert profile["by_rsi_regime"]["oversold"]["win_rate"] == 1.0
    assert profile["by_rsi_regime"]["oversold"]["significant"] is False


def _profile_with(bucket_key, dim_key, win_rate, significant=True):
    return {"BTC": {
        "overall_win_rate": 0.5, "total_samples": 9000,
        dim_key: {bucket_key: {"samples": 3000, "effective_samples": 200, "win_rate": win_rate,
                               "delta": win_rate - 0.5, "z": 5.0, "significant": significant}},
    }}


FEATURE_ROW = {"trend_1h": 0.01, "ret_15m": 0.005, "volatility_15": 0.0005, "rsi_14": 0.5, "dollar_volume_z": 0.2, "macd_hist_pct": 0.0001}


def test_score_is_side_aware():
    profiles = _profile_with("aligned_up", "by_trend_alignment", 0.6)
    assert ps.score_entry_conditions("BTC", "yes", FEATURE_ROW, profiles)["score"] == 1
    no = ps.score_entry_conditions("BTC", "no", FEATURE_ROW, profiles)
    assert no["score"] == -1
    assert no["unfavorable"] == ["trend_alignment"]


def test_score_ignores_insignificant_buckets():
    profiles = _profile_with("aligned_up", "by_trend_alignment", 0.9, significant=False)
    result = ps.score_entry_conditions("BTC", "yes", FEATURE_ROW, profiles)
    assert result["score"] == 0
    assert result["details"]["trend_alignment"]["verdict"] == "neutral"


def test_score_without_profiles_or_row_is_neutral():
    assert ps.score_entry_conditions("BTC", "yes", FEATURE_ROW, {})["score"] == 0
    assert ps.score_entry_conditions("BTC", "yes", None, {"BTC": {}})["score"] == 0
    assert ps.score_entry_conditions("ETH", "yes", FEATURE_ROW, {"BTC": {}})["reason"] == "no_profile_for_coin"


def test_score_derives_the_cascade_when_the_row_has_no_precomputed_score():
    row = {**FEATURE_ROW, "ret_5m": 0.1, "ret_10m": 0.1, "ret_30m": 0.1, "trend_2h": 0.1, "trend_4h": 0.1}
    profiles = _profile_with("strong_up", "by_cascade_regime", 0.6)
    result = ps.score_entry_conditions("BTC", "yes", row, profiles)
    assert result["mtf_cascade_score"] == 1.0
    assert "mtf_cascade" in result["favorable"]


def test_load_pattern_profiles_never_builds_inline(monkeypatch):
    monkeypatch.setattr(ps, "build_pattern_profiles", lambda: pytest.fail("must not train inline"))
    assert ps.load_pattern_profiles() == {}


def test_load_pattern_profiles_reads_the_local_file_then_caches():
    ps._PROFILES_LOCAL_PATH.write_text(json.dumps({"profiles": {"BTC": {"overall_win_rate": 0.5}}}))
    assert ps.load_pattern_profiles() == {"BTC": {"overall_win_rate": 0.5}}
    ps._PROFILES_LOCAL_PATH.unlink()
    assert ps.load_pattern_profiles() == {"BTC": {"overall_win_rate": 0.5}}


def test_remote_load_is_retried_at_most_every_fifteen_minutes(monkeypatch):
    import server_common
    calls = []
    monkeypatch.setattr(ps, "HF_API_KEY", "token")
    monkeypatch.setattr(server_common, "call_with_hard_timeout", lambda fn, timeout_sec: calls.append(1) or None)
    assert ps.load_pattern_profiles() == {}
    assert ps.load_pattern_profiles() == {}
    assert len(calls) == 1


def test_build_pattern_profiles_saves_and_refreshes_the_cache(monkeypatch):
    from data import kalshi_15m_data, kalshi_15m_metals_data
    rows = _rows(ps.LABEL_OVERLAP_MINUTES * ps.MIN_EFFECTIVE_SAMPLES * 2)
    monkeypatch.setattr(kalshi_15m_data, "load_training_dataset", lambda max_shards: rows)
    monkeypatch.setattr(kalshi_15m_metals_data, "load_training_dataset", lambda max_shards: pd.DataFrame())
    result = ps.build_pattern_profiles()
    assert result["ok"] is True and result["coins"] == ["BTC"]
    assert json.loads(ps._PROFILES_LOCAL_PATH.read_text())["coins"] == ["BTC"]
    assert "BTC" in ps.load_pattern_profiles()
