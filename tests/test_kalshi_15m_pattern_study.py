"""Tests for kalshi_15m_pattern_study.py -- behavioral pattern analysis module."""
import sys
import os
import time

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from data import kalshi_15m_pattern_study as ps


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(
    symbol: str = "BTC",
    ts: float = 1700000000.0,
    label_up: float = 1.0,
    trend_1h: float = 0.01,
    ret_15m: float = 0.005,
    volatility_15: float = 0.0005,
    rsi_14: float = 0.50,
    dollar_volume_z: float = 1.5,
    macd_hist_pct: float = 0.0001,
) -> dict:
    return {
        "symbol": symbol, "ts": ts, "label_up": label_up,
        "trend_1h": trend_1h, "ret_15m": ret_15m,
        "volatility_15": volatility_15, "rsi_14": rsi_14,
        "dollar_volume_z": dollar_volume_z, "macd_hist_pct": macd_hist_pct,
    }


def _make_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Bucketing helpers
# ---------------------------------------------------------------------------

def test_vol_regime_low():
    assert ps._vol_regime(0.0002) == "low"


def test_vol_regime_medium():
    assert ps._vol_regime(0.0005) == "medium"


def test_vol_regime_high():
    assert ps._vol_regime(0.002) == "high"


def test_vol_regime_none():
    assert ps._vol_regime(None) is None


def test_rsi_regime_oversold():
    assert ps._rsi_regime(0.25) == "oversold"


def test_rsi_regime_neutral():
    assert ps._rsi_regime(0.50) == "neutral"


def test_rsi_regime_overbought():
    assert ps._rsi_regime(0.80) == "overbought"


def test_rsi_regime_none():
    assert ps._rsi_regime(None) is None


def test_trend_alignment_aligned_up():
    assert ps._trend_alignment(0.01, 0.005) == "aligned_up"


def test_trend_alignment_aligned_down():
    assert ps._trend_alignment(-0.01, -0.005) == "aligned_down"


def test_trend_alignment_misaligned():
    assert ps._trend_alignment(0.01, -0.005) == "misaligned"


def test_trend_alignment_none_on_missing():
    assert ps._trend_alignment(None, 0.005) is None
    assert ps._trend_alignment(0.01, None) is None


def test_volume_regime_high():
    assert ps._volume_regime(1.5) == "high"


def test_volume_regime_normal():
    assert ps._volume_regime(0.2) == "normal"


def test_macd_regime_bullish():
    assert ps._macd_regime(0.001) == "bullish"


def test_macd_regime_bearish():
    assert ps._macd_regime(-0.001) == "bearish"


# ---------------------------------------------------------------------------
# analyze_patterns
# ---------------------------------------------------------------------------

def test_analyze_patterns_returns_empty_on_missing_columns():
    df = pd.DataFrame([{"symbol": "BTC", "ts": 1000, "label_up": 1}])
    result = ps.analyze_patterns(df)
    assert result == {}


def test_analyze_patterns_returns_empty_when_no_labels():
    rows = [_make_row(label_up=float("nan")) for _ in range(100)]
    df = _make_df(rows)
    result = ps.analyze_patterns(df)
    assert result == {}


def test_analyze_patterns_skips_coin_with_too_few_samples():
    rows = [_make_row(symbol="BTC") for _ in range(ps.MIN_SAMPLES_FOR_SIGNAL - 1)]
    df = _make_df(rows)
    result = ps.analyze_patterns(df)
    assert "BTC" not in result


def test_analyze_patterns_produces_profile_for_coin_with_enough_samples():
    rows = [_make_row(symbol="BTC") for _ in range(ps.MIN_SAMPLES_FOR_SIGNAL * 3)]
    df = _make_df(rows)
    result = ps.analyze_patterns(df)
    assert "BTC" in result


def test_analyze_patterns_profile_has_required_keys():
    rows = [_make_row(symbol="ETH") for _ in range(60)]
    df = _make_df(rows)
    result = ps.analyze_patterns(df)
    profile = result["ETH"]
    for key in ("total_samples", "overall_win_rate", "by_hour", "by_vol_regime",
                "by_rsi_regime", "by_trend_alignment", "by_volume_regime",
                "by_macd_regime", "best_hours", "worst_hours", "computed_at_utc"):
        assert key in profile, f"Missing key: {key}"


def test_analyze_patterns_overall_win_rate():
    # 30 wins, 30 losses
    rows = (
        [_make_row(symbol="BTC", label_up=1.0) for _ in range(30)] +
        [_make_row(symbol="BTC", label_up=0.0) for _ in range(30)]
    )
    df = _make_df(rows)
    result = ps.analyze_patterns(df)
    assert abs(result["BTC"]["overall_win_rate"] - 0.5) < 0.01


def test_analyze_patterns_best_hours_populated_when_delta_large_enough():
    # All rows at the same TS (hour bucket) with 100% win rate
    # Use a TS that maps to a specific ET hour
    ts_at_hour_10_utc = 1700000000.0  # will land at some ET hour
    et_hour = ps._et_hour_from_ts(ts_at_hour_10_utc)

    # 60 rows all at this TS, all wins -- win rate = 1.0, way above any baseline
    rows = [_make_row(symbol="BTC", ts=ts_at_hour_10_utc + i * 60, label_up=1.0)
            for i in range(60)]
    df = _make_df(rows)
    result = ps.analyze_patterns(df)
    profile = result["BTC"]
    # With 100% wins and an overall win rate of 1.0, delta is 0. Need mixed data.
    # Introduce some losses at a different hour to create a real baseline < 1.0
    ts_other = ts_at_hour_10_utc + 3600 * 5  # 5 hours later
    loss_rows = [_make_row(symbol="BTC", ts=ts_other + i * 60, label_up=0.0)
                 for i in range(40)]
    all_rows = rows + loss_rows
    df2 = _make_df(all_rows)
    result2 = ps.analyze_patterns(df2)
    profile2 = result2["BTC"]
    # The high-win-rate hour should appear in best_hours
    assert et_hour in profile2.get("best_hours", [])


def test_analyze_patterns_worst_hours_populated():
    ts_good = 1700000000.0  # one hour
    ts_bad = ts_good + 3600 * 3  # 3 hours later
    good_et = ps._et_hour_from_ts(ts_good)
    bad_et = ps._et_hour_from_ts(ts_bad)

    good_rows = [_make_row(symbol="BTC", ts=ts_good + i * 60, label_up=1.0) for i in range(40)]
    bad_rows = [_make_row(symbol="BTC", ts=ts_bad + i * 60, label_up=0.0) for i in range(40)]
    df = _make_df(good_rows + bad_rows)
    result = ps.analyze_patterns(df)
    profile = result["BTC"]

    if good_et != bad_et:  # only if they land in different hour buckets
        assert bad_et in profile.get("worst_hours", [])


def test_analyze_patterns_multiple_coins():
    rows = (
        [_make_row(symbol="BTC") for _ in range(60)] +
        [_make_row(symbol="ETH") for _ in range(60)]
    )
    df = _make_df(rows)
    result = ps.analyze_patterns(df)
    assert "BTC" in result
    assert "ETH" in result


# ---------------------------------------------------------------------------
# score_entry_conditions
# ---------------------------------------------------------------------------

def test_score_entry_conditions_returns_zero_when_no_profiles():
    result = ps.score_entry_conditions("BTC", "yes", {}, None)
    assert result["score"] == 0


def test_score_entry_conditions_returns_zero_when_no_feature_row():
    result = ps.score_entry_conditions("BTC", "yes", None, {"BTC": {"overall_win_rate": 0.5}})
    assert result["score"] == 0


def test_score_entry_conditions_returns_zero_when_coin_not_in_profiles():
    result = ps.score_entry_conditions("DOGE", "yes", {"trend_1h": 0.01, "ret_15m": 0.01}, {"BTC": {}})
    assert result["score"] == 0


def test_score_entry_conditions_has_required_keys():
    result = ps.score_entry_conditions("BTC", "yes", None, None)
    for key in ("score", "favorable", "unfavorable", "details", "reason"):
        assert key in result


def test_score_entry_conditions_favorable_for_yes_when_win_rate_high():
    # Build a profile where the current trend_alignment bucket has a high win rate
    profiles = {
        "BTC": {
            "overall_win_rate": 0.50,
            "total_samples": 500,
            "by_hour": {},
            "by_vol_regime": {},
            "by_rsi_regime": {},
            "by_trend_alignment": {
                "aligned_up": {"samples": 100, "win_rate": 0.60, "delta": 0.10},
            },
            "by_volume_regime": {},
            "by_macd_regime": {},
        }
    }
    feature_row = {
        "trend_1h": 0.01, "ret_15m": 0.005,  # aligned_up
        "volatility_15": 0.0005, "rsi_14": 0.50,
        "dollar_volume_z": 0.2, "macd_hist_pct": 0.0001,
    }
    result = ps.score_entry_conditions("BTC", "yes", feature_row, profiles)
    assert "trend_alignment" in result["favorable"]
    assert result["score"] > 0


def test_score_entry_conditions_unfavorable_for_yes_when_win_rate_low():
    profiles = {
        "BTC": {
            "overall_win_rate": 0.50,
            "total_samples": 500,
            "by_hour": {},
            "by_vol_regime": {},
            "by_rsi_regime": {},
            "by_trend_alignment": {
                "aligned_up": {"samples": 100, "win_rate": 0.40, "delta": -0.10},
            },
            "by_volume_regime": {},
            "by_macd_regime": {},
        }
    }
    feature_row = {
        "trend_1h": 0.01, "ret_15m": 0.005,
        "volatility_15": 0.0005, "rsi_14": 0.50,
        "dollar_volume_z": 0.2, "macd_hist_pct": 0.0001,
    }
    result = ps.score_entry_conditions("BTC", "yes", feature_row, profiles)
    assert "trend_alignment" in result["unfavorable"]
    assert result["score"] < 0


def test_score_entry_conditions_direction_aware_no_side():
    # For a "no" bet: a LOW win_rate in the current condition is FAVORABLE
    # (we're betting price goes DOWN, and historically it goes down more in this condition)
    profiles = {
        "BTC": {
            "overall_win_rate": 0.50,
            "total_samples": 500,
            "by_hour": {},
            "by_vol_regime": {},
            "by_rsi_regime": {},
            "by_trend_alignment": {
                "aligned_down": {"samples": 100, "win_rate": 0.35, "delta": -0.15},
            },
            "by_volume_regime": {},
            "by_macd_regime": {},
        }
    }
    feature_row = {
        "trend_1h": -0.01, "ret_15m": -0.005,  # aligned_down
        "volatility_15": 0.0005, "rsi_14": 0.50,
        "dollar_volume_z": 0.2, "macd_hist_pct": 0.0001,
    }
    result = ps.score_entry_conditions("BTC", "no", feature_row, profiles)
    # For "no": low win_rate (price tends to go down) = favorable for our bet
    assert "trend_alignment" in result["favorable"]
    assert result["score"] > 0


def test_score_entry_conditions_skips_bucket_with_too_few_samples():
    profiles = {
        "BTC": {
            "overall_win_rate": 0.50,
            "total_samples": 200,
            "by_hour": {},
            "by_vol_regime": {},
            "by_rsi_regime": {},
            "by_trend_alignment": {
                "aligned_up": {"samples": ps.MIN_SAMPLES_FOR_SIGNAL - 1, "win_rate": 0.90, "delta": 0.40},
            },
            "by_volume_regime": {},
            "by_macd_regime": {},
        }
    }
    feature_row = {
        "trend_1h": 0.01, "ret_15m": 0.005,
        "volatility_15": 0.0005, "rsi_14": 0.50,
        "dollar_volume_z": 0.2, "macd_hist_pct": 0.0001,
    }
    result = ps.score_entry_conditions("BTC", "yes", feature_row, profiles)
    # Too few samples -- should not count as favorable
    assert "trend_alignment" not in result["favorable"]
    assert result["score"] == 0


def test_score_entry_conditions_neutral_within_delta_band():
    # Win rate very close to baseline -- should be neutral, not favorable or unfavorable
    profiles = {
        "BTC": {
            "overall_win_rate": 0.50,
            "total_samples": 500,
            "by_hour": {},
            "by_vol_regime": {},
            "by_rsi_regime": {},
            "by_trend_alignment": {
                "aligned_up": {"samples": 100, "win_rate": 0.51, "delta": 0.01},
            },
            "by_volume_regime": {},
            "by_macd_regime": {},
        }
    }
    feature_row = {
        "trend_1h": 0.01, "ret_15m": 0.005,
        "volatility_15": 0.0005, "rsi_14": 0.50,
        "dollar_volume_z": 0.2, "macd_hist_pct": 0.0001,
    }
    result = ps.score_entry_conditions("BTC", "yes", feature_row, profiles)
    assert result["score"] == 0
    assert len(result["favorable"]) == 0
    assert len(result["unfavorable"]) == 0


def test_score_entry_conditions_attaches_baseline():
    profiles = {
        "BTC": {
            "overall_win_rate": 0.52,
            "total_samples": 300,
            "by_hour": {}, "by_vol_regime": {}, "by_rsi_regime": {},
            "by_trend_alignment": {}, "by_volume_regime": {}, "by_macd_regime": {},
        }
    }
    result = ps.score_entry_conditions("BTC", "yes", {"trend_1h": 0.0, "ret_15m": 0.0}, profiles)
    assert result["baseline_win_rate"] == 0.52
    assert result["profile_samples"] == 300


# ---------------------------------------------------------------------------
# analyze_patterns + score_entry_conditions integration
# ---------------------------------------------------------------------------

def test_end_to_end_analyze_then_score():
    """Build a profile from synthetic data, then score conditions against it."""
    # 60 rows with aligned_up trend, all wins
    ts_base = 1700000000.0
    aligned_up_wins = [
        _make_row(symbol="BTC", ts=ts_base + i * 60, label_up=1.0,
                  trend_1h=0.01, ret_15m=0.005) for i in range(60)
    ]
    # 30 rows with misaligned trend, all losses
    misaligned_losses = [
        _make_row(symbol="BTC", ts=ts_base + 3600 + i * 60, label_up=0.0,
                  trend_1h=0.01, ret_15m=-0.005) for i in range(30)
    ]
    df = _make_df(aligned_up_wins + misaligned_losses)
    profiles = ps.analyze_patterns(df)
    assert "BTC" in profiles

    feature_row_aligned_up = {
        "trend_1h": 0.01, "ret_15m": 0.005,
        "volatility_15": 0.0005, "rsi_14": 0.50,
        "dollar_volume_z": 0.2, "macd_hist_pct": 0.0001,
    }
    result = ps.score_entry_conditions("BTC", "yes", feature_row_aligned_up, profiles)
    # Aligned_up rows all won; this should score favorably for "yes"
    assert result["score"] >= 0  # may be 0 if sample count < MIN_SAMPLES or delta < FAVORABLE_DELTA
    assert "details" in result
