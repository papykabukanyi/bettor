"""Per-coin behavioral pattern study for Kalshi 15-minute markets.

Analyzes the historical archive (from kalshi_15m_data.load_training_dataset) to extract
per-coin win-rate profiles across five behavioral dimensions that go beyond the model's
own raw probability_up signal:

  1. Hour of day (ET, 0–23): captures intraday regime shifts (US session open/close,
     Asian session, dead of night) that affect how directionally reliable each coin's
     own technical indicators are in a given 15-min window.
  2. Volatility regime (low / medium / high): captures whether the current volatility_15
     level is a regime where 15-min directional continuation is likely vs. reversion.
  3. RSI regime (oversold / neutral / overbought): the classic mean-reversion / continuation
     split -- studied per-coin since what counts as "high RSI" for volatile DOGE is
     different from stable GOLD.
  4. Multi-timeframe alignment (aligned_up / aligned_down / misaligned): when the 1-hour
     trend (trend_1h, price change over 60 minutes) and the 15-minute momentum (ret_15m)
     agree in direction, does that coin tend to continue or reverse? This is the core
     "what the user asked for" -- "study all the other ticker hr for all those other
     currency and understand with the 15 min how to get profitable."
  5. Volume regime (high / normal): dollar_volume_z above/below threshold -- when real
     volume is running hot, does this coin's directional signal tend to be more reliable
     or noisier? Studied per-coin since each has a different baseline.

Each condition's win rate is computed against the coin's own baseline (overall_win_rate).
A condition is:
  - "favorable" if its historical win rate beats baseline by >= FAVORABLE_DELTA with
    >= MIN_SAMPLES observations in that bucket.
  - "unfavorable" if its historical win rate lags baseline by >= UNFAVORABLE_DELTA.
  - "neutral" otherwise (not enough data yet, or within the noise band).

score_entry_conditions() is direction-aware: for a "yes" bet (expecting price to rise),
a high historical win rate in the current condition is favorable; for a "no" bet (expecting
price to fall), it's the opposite. Score = favorable_count - unfavorable_count.

The strategy uses this as an ADDITIONAL observability layer (always computed, always
attached to evaluate_candidate's result) and an OPTIONAL entry gate when
USE_PATTERN_STUDY is True: entries are suppressed when the pattern score is below
PATTERN_STUDY_MIN_SCORE (default -1 -- two net unfavorable conditions), regardless of
model confidence. Same "observability before gating" discipline as USE_REAL_OUTCOME_CALIBRATION
and USE_CORRELATION_STUDY.

Profiles are rebuilt from the full HF archive once per REBUILD_INTERVAL_HOURS (default 6)
and cached in-process. The rebuild is cheap relative to a retrain: one pandas groupby over
the already-loaded DataFrame, no model training.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from server_common import DATA_DIR

logger = logging.getLogger(__name__)

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_KALSHI_15M_DATASET_REPO = os.getenv("HF_KALSHI_15M_DATASET_REPO", "papylove/kalshi-15m-data")

# ---- Tuning thresholds ----
MIN_SAMPLES_FOR_SIGNAL = int(os.getenv("KALSHI_15M_PATTERN_MIN_SAMPLES", "30") or "30")
FAVORABLE_DELTA = float(os.getenv("KALSHI_15M_PATTERN_FAVORABLE_DELTA", "0.04") or "0.04")
UNFAVORABLE_DELTA = float(os.getenv("KALSHI_15M_PATTERN_UNFAVORABLE_DELTA", "0.04") or "0.04")

# How far back (in archive shards) to analyze -- same as load_training_dataset's
# max_shards default. Studying too much history risks including regimes that no
# longer apply; too little gives thin per-bucket samples. 60 days balances both.
PATTERN_STUDY_MAX_SHARDS = int(os.getenv("KALSHI_15M_PATTERN_STUDY_MAX_SHARDS", "60") or "60")

# ET offset from UTC: -4 (EDT, summer) / -5 (EST, winter). Use -4 as a year-round
# approximation for crypto 24/7 analysis -- acceptable since we're bucketing into
# hour-of-day bins, not trying to pin exact session boundaries.
_ET_OFFSET_HOURS = int(os.getenv("KALSHI_15M_ET_OFFSET_HOURS", "-4") or "-4")

# ---- In-process profile cache ----
_PROFILES_CACHE: dict[str, Any] | None = None
_PROFILES_CACHE_TS: float = 0.0
REBUILD_INTERVAL_HOURS = float(os.getenv("KALSHI_15M_PATTERN_REBUILD_HOURS", "6") or "6")
_PROFILES_LOCAL_PATH = DATA_DIR / "kalshi_15m_pattern_profiles.json"


# ---------------------------------------------------------------------------
# Feature bucketing helpers -- all pure functions, fast, no IO.
# ---------------------------------------------------------------------------

def _et_hour_from_ts(ts_unix: float) -> int:
    """UTC Unix timestamp → US Eastern hour (0–23)."""
    utc_hour = int((ts_unix % 86400) // 3600)
    return (utc_hour + _ET_OFFSET_HOURS) % 24


def _current_et_hour() -> int:
    return _et_hour_from_ts(time.time())


def _vol_regime(volatility_15: float | None) -> str | None:
    if volatility_15 is None or volatility_15 != volatility_15:  # NaN
        return None
    if volatility_15 < 0.0003:
        return "low"
    if volatility_15 < 0.0010:
        return "medium"
    return "high"


def _rsi_regime(rsi_14: float | None) -> str | None:
    if rsi_14 is None or rsi_14 != rsi_14:
        return None
    if rsi_14 < 0.35:
        return "oversold"
    if rsi_14 > 0.65:
        return "overbought"
    return "neutral"


def _trend_alignment(trend_1h: float | None, ret_15m: float | None) -> str | None:
    """Are the 1-hour trend and the 15-minute momentum pointing the same way?
    aligned_up / aligned_down / misaligned -- None when either is unavailable."""
    if trend_1h is None or ret_15m is None:
        return None
    if trend_1h != trend_1h or ret_15m != ret_15m:  # NaN
        return None
    if trend_1h > 0 and ret_15m > 0:
        return "aligned_up"
    if trend_1h < 0 and ret_15m < 0:
        return "aligned_down"
    return "misaligned"


def _volume_regime(dollar_volume_z: float | None) -> str | None:
    if dollar_volume_z is None or dollar_volume_z != dollar_volume_z:
        return None
    return "high" if dollar_volume_z > 0.5 else "normal"


def _macd_regime(macd_hist_pct: float | None) -> str | None:
    if macd_hist_pct is None or macd_hist_pct != macd_hist_pct:
        return None
    return "bullish" if macd_hist_pct > 0 else "bearish"


def _cascade_regime(mtf_cascade_score: float | None) -> str | None:
    """Map the multi-timeframe alignment score to a named regime.
    strong_up: >= +0.75 (6+ of 8 timeframes bullish)
    weak_up: 0 to +0.75
    weak_down: -0.75 to 0
    strong_down: <= -0.75 (6+ of 8 timeframes bearish)
    """
    if mtf_cascade_score is None or mtf_cascade_score != mtf_cascade_score:
        return None
    if mtf_cascade_score >= 0.75:
        return "strong_up"
    if mtf_cascade_score >= 0.0:
        return "weak_up"
    if mtf_cascade_score >= -0.75:
        return "weak_down"
    return "strong_down"


# ---------------------------------------------------------------------------
# Core analysis -- takes the full training DataFrame, returns nested profile dict.
# ---------------------------------------------------------------------------

def analyze_patterns(df: "pd.DataFrame") -> dict[str, Any]:  # type: ignore[name-defined]  # noqa: F821
    """Compute per-coin behavioral win-rate profiles from the training archive.

    Args:
        df: Full DataFrame from kalshi_15m_data.load_training_dataset() or
            kalshi_15m_metals_data.load_training_dataset() -- must contain at minimum:
            symbol, ts, label_up, trend_1h, ret_15m, volatility_15, rsi_14,
            dollar_volume_z, macd_hist_pct.

    Returns:
        dict mapping coin (str) -> profile dict with keys:
            total_samples, overall_win_rate,
            by_hour (ET hour 0-23), by_vol_regime, by_rsi_regime,
            by_trend_alignment, by_volume_regime, by_macd_regime,
            best_hours, worst_hours, computed_at_utc.

    Only rows with a real label (label_up 0 or 1, not NaN) are analyzed.
    A bucket is only reported when it has >= MIN_SAMPLES_FOR_SIGNAL rows.
    """
    import pandas as pd  # noqa: PLC0415 -- lazy, avoids import at module load
    required = {"symbol", "ts", "label_up", "trend_1h", "ret_15m",
                "volatility_15", "rsi_14", "dollar_volume_z", "macd_hist_pct"}
    missing = required - set(df.columns)
    if missing:
        logger.warning("[kalshi_15m_pattern_study] analyze_patterns: missing columns %s", missing)
        return {}
    # mtf_cascade_score is optional -- only present once the extended pipeline
    # has run; analyze_patterns computes it on the fly from available columns.
    _tf_cols_for_cascade = ["ret_5m", "ret_10m", "ret_15m", "ret_30m",
                             "trend_1h", "trend_2h", "trend_4h", "trend_8h"]
    _cascade_cols = [c for c in _tf_cols_for_cascade if c in df.columns]

    labeled = df.dropna(subset=["label_up"]).copy()
    if labeled.empty:
        return {}

    labeled["_label"] = labeled["label_up"].astype(float)
    labeled["_hour_et"] = labeled["ts"].apply(lambda t: _et_hour_from_ts(float(t)))
    labeled["_vol_regime"] = labeled["volatility_15"].apply(_vol_regime)
    labeled["_rsi_regime"] = labeled["rsi_14"].apply(_rsi_regime)
    labeled["_trend_align"] = labeled.apply(
        lambda r: _trend_alignment(r["trend_1h"], r["ret_15m"]), axis=1
    )
    labeled["_vol_bucket"] = labeled["dollar_volume_z"].apply(_volume_regime)
    labeled["_macd_regime"] = labeled["macd_hist_pct"].apply(_macd_regime)

    # Multi-timeframe cascade score -- computed per row from whichever of the
    # 8 standard timeframes exist in the archive. NaN for rows with < 4 valid values.
    if _cascade_cols:
        def _row_cascade(row: "pd.Series") -> float | None:  # type: ignore[name-defined]
            vals = [float(row[c]) for c in _cascade_cols
                    if row[c] == row[c]]  # NaN check
            if len(vals) < 4:
                return None
            up = sum(1 for v in vals if v > 0)
            return round((up / len(vals)) * 2.0 - 1.0, 4)
        labeled["_mtf_cascade"] = labeled.apply(_row_cascade, axis=1)
        labeled["_cascade_regime"] = labeled["_mtf_cascade"].apply(_cascade_regime)
    else:
        labeled["_cascade_regime"] = None

    profiles: dict[str, Any] = {}
    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    for coin, coin_df in labeled.groupby("symbol"):
        n = len(coin_df)
        if n < MIN_SAMPLES_FOR_SIGNAL:
            continue

        overall_win_rate = float(coin_df["_label"].mean())

        def _bucket_stats(col_name: str) -> dict[str, dict[str, Any]]:
            result: dict[str, dict[str, Any]] = {}
            for key, grp in coin_df.dropna(subset=[col_name]).groupby(col_name):
                if len(grp) < MIN_SAMPLES_FOR_SIGNAL:
                    continue
                win_rate = float(grp["_label"].mean())
                result[str(key)] = {
                    "samples": int(len(grp)),
                    "win_rate": round(win_rate, 4),
                    "delta": round(win_rate - overall_win_rate, 4),
                }
            return result

        by_hour = _bucket_stats("_hour_et")
        by_vol = _bucket_stats("_vol_regime")
        by_rsi = _bucket_stats("_rsi_regime")
        by_align = _bucket_stats("_trend_align")
        by_volume = _bucket_stats("_vol_bucket")
        by_macd = _bucket_stats("_macd_regime")
        by_cascade = _bucket_stats("_cascade_regime") if "_cascade_regime" in coin_df.columns else {}

        # Best/worst hours: hours where the historical delta is large enough to act on
        best_hours = sorted(
            [int(h) for h, v in by_hour.items() if v["delta"] >= FAVORABLE_DELTA],
            key=lambda h: -by_hour[str(h)]["delta"],
        )
        worst_hours = sorted(
            [int(h) for h, v in by_hour.items() if v["delta"] <= -UNFAVORABLE_DELTA],
            key=lambda h: by_hour[str(h)]["delta"],
        )

        profiles[str(coin)] = {
            "total_samples": n,
            "overall_win_rate": round(overall_win_rate, 4),
            "by_hour": by_hour,
            "by_vol_regime": by_vol,
            "by_rsi_regime": by_rsi,
            "by_trend_alignment": by_align,
            "by_volume_regime": by_volume,
            "by_macd_regime": by_macd,
            "by_cascade_regime": by_cascade,
            "best_hours": best_hours,
            "worst_hours": worst_hours,
            "computed_at_utc": now_utc,
        }

    return profiles


# ---------------------------------------------------------------------------
# Build / persist / load
# ---------------------------------------------------------------------------

def build_pattern_profiles() -> dict[str, Any]:
    """Load the full archive, run analyze_patterns, save locally + push to HF.

    Returns a result dict with keys: ok, coins, total_samples, saved_local, hf_uploaded.
    Separate crypto (kalshi_15m_data) and metals (kalshi_15m_metals_data) archives
    are merged before analysis so each coin's profile reflects ALL available history.
    """
    from data import kalshi_15m_data, kalshi_15m_metals_data  # noqa: PLC0415
    import pandas as pd  # noqa: PLC0415

    frames = []
    try:
        crypto_df = kalshi_15m_data.load_training_dataset(max_shards=PATTERN_STUDY_MAX_SHARDS)
        if not crypto_df.empty:
            frames.append(crypto_df)
    except Exception as exc:
        logger.warning("[kalshi_15m_pattern_study] crypto archive load failed: %s", exc)

    try:
        metals_df = kalshi_15m_metals_data.load_training_dataset(max_shards=PATTERN_STUDY_MAX_SHARDS)
        if not metals_df.empty:
            frames.append(metals_df)
    except Exception as exc:
        logger.warning("[kalshi_15m_pattern_study] metals archive load failed: %s", exc)

    if not frames:
        return {"ok": False, "reason": "no_archive_data"}

    try:
        combined = pd.concat(frames, ignore_index=True)
    except Exception as exc:
        return {"ok": False, "reason": f"concat_failed: {exc}"}

    profiles = analyze_patterns(combined)
    if not profiles:
        return {"ok": False, "reason": "analyze_returned_empty"}

    # Wrap in a top-level envelope with build metadata
    envelope = {
        "profiles": profiles,
        "built_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total_rows": int(len(combined)),
        "coins": sorted(profiles.keys()),
    }

    # Save locally
    saved_local = False
    try:
        _PROFILES_LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _PROFILES_LOCAL_PATH.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(envelope, f, indent=2)
        tmp.replace(_PROFILES_LOCAL_PATH)
        saved_local = True
    except Exception as exc:
        logger.warning("[kalshi_15m_pattern_study] local save failed: %s", exc)

    # Update the in-process cache immediately so next evaluate_candidate call
    # benefits without waiting for the next load_pattern_profiles() call.
    global _PROFILES_CACHE, _PROFILES_CACHE_TS
    _PROFILES_CACHE = profiles
    _PROFILES_CACHE_TS = time.time()

    # Push to HF
    hf_uploaded = False
    if HF_API_KEY and saved_local:
        try:
            from huggingface_hub import HfApi  # noqa: PLC0415
            api = HfApi(token=HF_API_KEY)
            api.upload_file(
                path_or_fileobj=str(_PROFILES_LOCAL_PATH),
                path_in_repo="pattern_study/latest_profiles.json",
                repo_id=HF_KALSHI_15M_DATASET_REPO,
                repo_type="dataset",
                commit_message=f"kalshi 15m pattern profiles rebuild {envelope['built_at_utc']}",
            )
            hf_uploaded = True
        except Exception as exc:
            logger.warning("[kalshi_15m_pattern_study] HF upload failed: %s", exc)

    return {
        "ok": True,
        "coins": sorted(profiles.keys()),
        "total_samples": int(len(combined)),
        "saved_local": saved_local,
        "hf_uploaded": hf_uploaded,
        "built_at_utc": envelope["built_at_utc"],
    }


def load_pattern_profiles(*, force_rebuild: bool = False) -> dict[str, Any]:
    """Return the cached per-coin profiles, rebuilding from archive when the cache
    is stale (> REBUILD_INTERVAL_HOURS old) or missing. Falls back gracefully to
    an empty dict if nothing is available -- evaluate_candidate treats empty profiles
    as 'no signal' (score 0, never blocks an entry).

    force_rebuild=True bypasses the TTL check and always runs a full rebuild."""
    global _PROFILES_CACHE, _PROFILES_CACHE_TS

    age_sec = time.time() - _PROFILES_CACHE_TS
    if not force_rebuild and _PROFILES_CACHE is not None and age_sec < REBUILD_INTERVAL_HOURS * 3600:
        return _PROFILES_CACHE

    # Try loading from the local file first (fast path -- avoids a full archive reload
    # if the file is fresh enough, e.g. from a previous build_pattern_profiles call)
    if not force_rebuild and _PROFILES_LOCAL_PATH.exists():
        try:
            with open(_PROFILES_LOCAL_PATH, "r", encoding="utf-8") as f:
                envelope = json.load(f)
            file_age_sec = time.time() - _PROFILES_LOCAL_PATH.stat().st_mtime
            if file_age_sec < REBUILD_INTERVAL_HOURS * 3600:
                profiles = envelope.get("profiles") or {}
                if profiles:
                    _PROFILES_CACHE = profiles
                    _PROFILES_CACHE_TS = time.time()
                    return profiles
        except Exception as exc:
            logger.warning("[kalshi_15m_pattern_study] local profile load failed: %s", exc)

    # Full rebuild from archive -- may be slow (loads + scans the full HF archive)
    try:
        result = build_pattern_profiles()
        if result.get("ok") and _PROFILES_CACHE is not None:
            return _PROFILES_CACHE
    except Exception as exc:
        logger.warning("[kalshi_15m_pattern_study] profile rebuild failed: %s", exc)

    return _PROFILES_CACHE or {}


# ---------------------------------------------------------------------------
# Live scoring -- called from evaluate_candidate for each entry candidate.
# ---------------------------------------------------------------------------

def score_entry_conditions(
    coin: str,
    side: str,  # "yes" (bet price goes up) or "no" (bet price goes down)
    feature_row: dict[str, Any] | None,
    profiles: dict[str, Any] | None,
) -> dict[str, Any]:
    """Score the current market conditions for this coin/side combination against
    the learned behavioral profiles.

    Returns:
        score: int, positive = conditions favor this side, negative = against.
        favorable: list of dimension names where conditions favor this side.
        unfavorable: list of dimension names where conditions disfavor this side.
        details: dict mapping dimension name -> {win_rate, baseline, delta, samples, verdict}.
        reason: brief string summary.

    A score of 0 means neutral (not enough data, or conditions within the noise band) --
    never blocks an entry on its own. Scores <= PATTERN_STUDY_MIN_SCORE may block an
    entry if USE_PATTERN_STUDY is enabled in the strategy.

    The function is PURE (no IO, no state) and fast (only dict lookups). The profiles
    argument is pre-loaded by load_pattern_profiles() and cached in-process."""
    if not profiles or not feature_row:
        return {"score": 0, "reason": "no_profile_or_feature_row", "favorable": [], "unfavorable": [], "details": {}}

    profile = profiles.get(coin)
    if not profile:
        return {"score": 0, "reason": "no_profile_for_coin", "coin": coin, "favorable": [], "unfavorable": [], "details": {}}

    baseline = float(profile.get("overall_win_rate", 0.5))
    total_samples = profile.get("total_samples", 0)

    # For a "yes" bet: favorable = conditions where price historically goes UP more than baseline.
    # For a "no" bet: favorable = conditions where price historically goes DOWN more than baseline
    #   (i.e., win_rate below baseline -- since label_up=1 means price went UP).
    # flip = 1.0 for "yes", -1.0 for "no": multiplies the delta to make it side-relative.
    flip = 1.0 if side == "yes" else -1.0

    favorable: list[str] = []
    unfavorable: list[str] = []
    details: dict[str, Any] = {}

    def _check_bucket(dim_name: str, bucket_data: dict[str, dict[str, Any]], bucket_key: str | None) -> None:
        if not bucket_key or bucket_key not in bucket_data:
            return
        bkt = bucket_data[bucket_key]
        if bkt.get("samples", 0) < MIN_SAMPLES_FOR_SIGNAL:
            return
        win_rate = float(bkt["win_rate"])
        delta = win_rate - baseline
        side_delta = delta * flip  # positive = favorable for this side
        verdict = "neutral"
        if side_delta >= FAVORABLE_DELTA:
            favorable.append(dim_name)
            verdict = "favorable"
        elif side_delta <= -UNFAVORABLE_DELTA:
            unfavorable.append(dim_name)
            verdict = "unfavorable"
        details[dim_name] = {
            "bucket": bucket_key,
            "win_rate": win_rate,
            "baseline": round(baseline, 4),
            "delta": round(delta, 4),
            "side_delta": round(side_delta, 4),
            "samples": bkt["samples"],
            "verdict": verdict,
        }

    # 1. Hour of day
    current_hour = str(_current_et_hour())
    _check_bucket("hour_et", profile.get("by_hour", {}), current_hour)

    # 2. Volatility regime
    vol_key = _vol_regime(feature_row.get("volatility_15"))
    _check_bucket("vol_regime", profile.get("by_vol_regime", {}), vol_key)

    # 3. RSI regime
    rsi_key = _rsi_regime(feature_row.get("rsi_14"))
    _check_bucket("rsi_regime", profile.get("by_rsi_regime", {}), rsi_key)

    # 4. Multi-timeframe alignment -- the core "study hr vs 15m" dimension
    align_key = _trend_alignment(feature_row.get("trend_1h"), feature_row.get("ret_15m"))
    _check_bucket("trend_alignment", profile.get("by_trend_alignment", {}), align_key)

    # 5. Volume regime (skipped for metals if dollar_volume_z is near 0 / absent)
    vol_bucket = _volume_regime(feature_row.get("dollar_volume_z"))
    _check_bucket("volume_regime", profile.get("by_volume_regime", {}), vol_bucket)

    # 6. MACD direction
    macd_key = _macd_regime(feature_row.get("macd_hist_pct"))
    _check_bucket("macd_regime", profile.get("by_macd_regime", {}), macd_key)

    # 7. Multi-timeframe cascade alignment -- the fullest signal: all 8 timeframes
    # (5m, 10m, 15m, 30m, 1h, 2h, 4h, 8h) summarized into one direction vote.
    # When the pre-computed mtf_cascade_score is available (set by
    # kalshi_15m_data.latest_feature_row), use it directly. Otherwise derive it
    # on the fly from whatever timeframe returns exist in the feature_row.
    cascade_score_val = feature_row.get("mtf_cascade_score")
    if cascade_score_val is None:
        _tf_keys = ["ret_5m", "ret_10m", "ret_15m", "ret_30m",
                    "trend_1h", "trend_2h", "trend_4h", "trend_8h"]
        _tf_vals = [feature_row.get(k) for k in _tf_keys if feature_row.get(k) is not None]
        if len(_tf_vals) >= 4:
            _up = sum(1 for v in _tf_vals if v > 0)
            cascade_score_val = round((_up / len(_tf_vals)) * 2.0 - 1.0, 4)
    cascade_key = _cascade_regime(cascade_score_val)
    _check_bucket("mtf_cascade", profile.get("by_cascade_regime", {}), cascade_key)

    score = len(favorable) - len(unfavorable)

    return {
        "score": score,
        "favorable": favorable,
        "unfavorable": unfavorable,
        "details": details,
        "coin": coin,
        "side": side,
        "baseline_win_rate": round(baseline, 4),
        "profile_samples": total_samples,
        "reason": f"score_{score}" if (favorable or unfavorable) else "all_neutral",
    }
