"""Per-coin behavioral pattern study for Kalshi 15-minute markets.

Studies each coin's archived minute rows (kalshi_15m_data / kalshi_15m_metals_data)
and reports how often price finished higher 15 minutes later under different
conditions: ET hour, volatility regime, RSI regime, 1h-vs-15m trend alignment,
volume regime, MACD direction, and the 8-timeframe cascade (5m/10m/15m/30m/1h/2h/
4h/8h all voting on direction).

Two statistical facts shape every number here:
  - Each minute row's label looks 15 minutes ahead, so consecutive rows share
    most of their outcome. A bucket of N rows carries roughly N/15 independent
    observations (`effective_samples`), and significance is judged on that.
  - These are UNCONDITIONAL up-rates. Kalshi's own quote already reacts to the
    same momentum, so a "favorable" bucket here is not by itself a profitable
    entry. That price-vs-outcome question is answered by kalshi_15m_edge_model,
    which uses these same multi-timeframe features against the real quote.
    That is why this study is observability by default (USE_PATTERN_STUDY off
    in the strategy) rather than an entry gate.

Profiles are built only by the scheduled job (build_pattern_profiles). Live
callers use load_pattern_profiles(), which reads the in-process cache, the
local file, or the published HF copy, and never loads the training archive.
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any

from server_common import DATA_DIR

logger = logging.getLogger(__name__)

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_KALSHI_15M_DATASET_REPO = os.getenv("HF_KALSHI_15M_DATASET_REPO", "papylove/kalshi-15m-data")
HF_PROFILES_PATH = "pattern_study/latest_profiles.json"

LABEL_OVERLAP_MINUTES = 15
MIN_EFFECTIVE_SAMPLES = int(os.getenv("KALSHI_15M_PATTERN_MIN_EFFECTIVE_SAMPLES", "30") or "30")
MIN_DELTA = float(os.getenv("KALSHI_15M_PATTERN_MIN_DELTA", "0.02") or "0.02")
MIN_Z = float(os.getenv("KALSHI_15M_PATTERN_MIN_Z", "2.0") or "2.0")
PATTERN_STUDY_MAX_SHARDS = int(os.getenv("KALSHI_15M_PATTERN_STUDY_MAX_SHARDS", "60") or "60")

CASCADE_COLUMNS = ["ret_5m", "ret_10m", "ret_15m", "ret_30m", "trend_1h", "trend_2h", "trend_4h", "trend_8h"]
CASCADE_MIN_TIMEFRAMES = 4

_PROFILES_LOCAL_PATH = DATA_DIR / "kalshi_15m_pattern_profiles.json"
_REMOTE_RETRY_SEC = 900.0
_REMOTE_TIMEOUT_SEC = 20
_cache: dict[str, Any] = {"profiles": None, "last_remote_attempt": 0.0}


def _et_zone():
    try:
        import zoneinfo
        return zoneinfo.ZoneInfo("America/New_York")
    except Exception:
        import pytz
        return pytz.timezone("America/New_York")


def _et_hour_from_ts(ts_unix: float) -> int:
    import datetime as dt
    return dt.datetime.fromtimestamp(float(ts_unix), tz=_et_zone()).hour


def _current_et_hour() -> int:
    return _et_hour_from_ts(time.time())


def _is_nan(v: Any) -> bool:
    return v is None or v != v


def _vol_regime(volatility_15: float | None) -> str | None:
    if _is_nan(volatility_15):
        return None
    if volatility_15 < 0.0003:
        return "low"
    if volatility_15 < 0.0010:
        return "medium"
    return "high"


def _rsi_regime(rsi_14: float | None) -> str | None:
    if _is_nan(rsi_14):
        return None
    if rsi_14 < 0.35:
        return "oversold"
    if rsi_14 > 0.65:
        return "overbought"
    return "neutral"


def _trend_alignment(trend_1h: float | None, ret_15m: float | None) -> str | None:
    if _is_nan(trend_1h) or _is_nan(ret_15m):
        return None
    if trend_1h > 0 and ret_15m > 0:
        return "aligned_up"
    if trend_1h < 0 and ret_15m < 0:
        return "aligned_down"
    return "misaligned"


def _volume_regime(dollar_volume_z: float | None) -> str | None:
    if _is_nan(dollar_volume_z):
        return None
    return "high" if dollar_volume_z > 0.5 else "normal"


def _macd_regime(macd_hist_pct: float | None) -> str | None:
    if _is_nan(macd_hist_pct):
        return None
    return "bullish" if macd_hist_pct > 0 else "bearish"


def mtf_cascade_score(values: list[float | None]) -> float | None:
    """Signed share of timeframes pointing up: +1 all up, -1 all down."""
    valid = [v for v in values if not _is_nan(v)]
    if len(valid) < CASCADE_MIN_TIMEFRAMES:
        return None
    up = sum(1 for v in valid if v > 0)
    return round((up / len(valid)) * 2.0 - 1.0, 4)


def _cascade_regime(score: float | None) -> str | None:
    if _is_nan(score):
        return None
    if score >= 0.75:
        return "strong_up"
    if score >= 0.0:
        return "weak_up"
    if score > -0.75:
        return "weak_down"
    return "strong_down"


def _bucket_verdict(win_rate: float, baseline: float, samples: int) -> dict[str, Any]:
    n_eff = samples / LABEL_OVERLAP_MINUTES
    delta = win_rate - baseline
    se = math.sqrt(max(baseline * (1.0 - baseline), 1e-9) / max(n_eff, 1e-9))
    z = delta / se
    significant = n_eff >= MIN_EFFECTIVE_SAMPLES and abs(delta) >= MIN_DELTA and abs(z) >= MIN_Z
    return {
        "samples": int(samples), "effective_samples": round(n_eff, 1),
        "win_rate": round(win_rate, 4), "delta": round(delta, 4), "z": round(z, 2), "significant": bool(significant),
    }


def analyze_patterns(df: "pd.DataFrame") -> dict[str, Any]:  # noqa: F821
    """Per-coin up-rate profiles from labeled archive rows. Needs at least
    symbol, ts, label_up, trend_1h, ret_15m, volatility_15, rsi_14,
    dollar_volume_z, macd_hist_pct; any CASCADE_COLUMNS present feed the
    cascade dimension."""
    import numpy as np
    import pandas as pd

    required = {"symbol", "ts", "label_up", "trend_1h", "ret_15m", "volatility_15", "rsi_14", "dollar_volume_z", "macd_hist_pct"}
    missing = required - set(df.columns)
    if missing:
        logger.warning("[kalshi_15m_pattern_study] analyze_patterns: missing columns %s", missing)
        return {}
    d = df.dropna(subset=["label_up"])
    if d.empty:
        return {}

    frame = pd.DataFrame({"symbol": d["symbol"].astype(str).values, "y": d["label_up"].astype(float).values})
    frame["hour_et"] = pd.to_datetime(d["ts"].astype(float).values, unit="s", utc=True).tz_convert(_et_zone()).hour
    vol = d["volatility_15"].astype(float).values
    frame["vol_regime"] = np.select([vol < 0.0003, vol < 0.0010, vol >= 0.0010], ["low", "medium", "high"], default=None)
    rsi = d["rsi_14"].astype(float).values
    frame["rsi_regime"] = np.select([rsi < 0.35, rsi > 0.65, (rsi >= 0.35) & (rsi <= 0.65)], ["oversold", "overbought", "neutral"], default=None)
    t1h, r15 = d["trend_1h"].astype(float).values, d["ret_15m"].astype(float).values
    valid_align = ~(np.isnan(t1h) | np.isnan(r15))
    frame["trend_alignment"] = np.where(
        ~valid_align, None,
        np.where((t1h > 0) & (r15 > 0), "aligned_up", np.where((t1h < 0) & (r15 < 0), "aligned_down", "misaligned")),
    )
    dvz = d["dollar_volume_z"].astype(float).values
    frame["volume_regime"] = np.where(np.isnan(dvz), None, np.where(dvz > 0.5, "high", "normal"))
    macd = d["macd_hist_pct"].astype(float).values
    frame["macd_regime"] = np.where(np.isnan(macd), None, np.where(macd > 0, "bullish", "bearish"))

    cascade_cols = [c for c in CASCADE_COLUMNS if c in d.columns]
    if cascade_cols:
        tf = d[cascade_cols].astype(float)
        n_valid = tf.notna().sum(axis=1)
        n_up = (tf > 0).sum(axis=1)
        score = np.where(n_valid >= CASCADE_MIN_TIMEFRAMES, (n_up / n_valid.replace(0, np.nan)) * 2.0 - 1.0, np.nan)
        frame["cascade_regime"] = np.select(
            [score >= 0.75, score >= 0.0, score > -0.75, score <= -0.75],
            ["strong_up", "weak_up", "weak_down", "strong_down"], default=None,
        )
    else:
        frame["cascade_regime"] = None

    dims = {
        "by_hour": "hour_et", "by_vol_regime": "vol_regime", "by_rsi_regime": "rsi_regime",
        "by_trend_alignment": "trend_alignment", "by_volume_regime": "volume_regime",
        "by_macd_regime": "macd_regime", "by_cascade_regime": "cascade_regime",
    }
    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    profiles: dict[str, Any] = {}
    for coin, g in frame.groupby("symbol"):
        n = len(g)
        if n / LABEL_OVERLAP_MINUTES < MIN_EFFECTIVE_SAMPLES:
            continue
        baseline = float(g["y"].mean())
        profile: dict[str, Any] = {
            "total_samples": int(n), "effective_samples": round(n / LABEL_OVERLAP_MINUTES, 1),
            "overall_win_rate": round(baseline, 4), "computed_at_utc": now_utc,
        }
        for key, col in dims.items():
            stats = g.dropna(subset=[col]).groupby(col)["y"].agg(["mean", "size"])
            profile[key] = {str(k): _bucket_verdict(float(r["mean"]), baseline, int(r["size"])) for k, r in stats.iterrows()}
        by_hour = profile["by_hour"]
        profile["best_hours"] = sorted((int(h) for h, v in by_hour.items() if v["significant"] and v["delta"] > 0), key=lambda h: -by_hour[str(h)]["delta"])
        profile["worst_hours"] = sorted((int(h) for h, v in by_hour.items() if v["significant"] and v["delta"] < 0), key=lambda h: by_hour[str(h)]["delta"])
        profiles[str(coin)] = profile
    return profiles


def build_pattern_profiles() -> dict[str, Any]:
    """Scheduled-job entry point: loads both archives, rebuilds profiles,
    saves locally, publishes to HF, refreshes the in-process cache."""
    import pandas as pd

    from data import kalshi_15m_data, kalshi_15m_metals_data

    frames = []
    for name, module in (("crypto", kalshi_15m_data), ("metals", kalshi_15m_metals_data)):
        try:
            part = module.load_training_dataset(max_shards=PATTERN_STUDY_MAX_SHARDS)
            if not part.empty:
                frames.append(part)
        except Exception as exc:
            logger.warning("[kalshi_15m_pattern_study] %s archive load failed: %s", name, exc)
    if not frames:
        return {"ok": False, "reason": "no_archive_data"}

    combined = pd.concat(frames, ignore_index=True)
    total_rows = int(len(combined))
    profiles = analyze_patterns(combined)
    del combined, frames
    if not profiles:
        return {"ok": False, "reason": "analyze_returned_empty"}

    built_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    envelope = {"profiles": profiles, "built_at_utc": built_at, "total_rows": total_rows, "coins": sorted(profiles)}
    _PROFILES_LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _PROFILES_LOCAL_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(envelope), encoding="utf-8")
    tmp.replace(_PROFILES_LOCAL_PATH)
    _cache["profiles"] = profiles

    hf_uploaded = False
    if HF_API_KEY:
        try:
            from huggingface_hub import HfApi
            HfApi(token=HF_API_KEY).upload_file(
                path_or_fileobj=str(_PROFILES_LOCAL_PATH), path_in_repo=HF_PROFILES_PATH,
                repo_id=HF_KALSHI_15M_DATASET_REPO, repo_type="dataset",
                commit_message=f"kalshi 15m pattern profiles {built_at}",
            )
            hf_uploaded = True
        except Exception as exc:
            logger.warning("[kalshi_15m_pattern_study] HF upload failed: %s", exc)
    return {"ok": True, "coins": sorted(profiles), "total_rows": total_rows, "hf_uploaded": hf_uploaded, "built_at_utc": built_at}


def load_pattern_profiles() -> dict[str, Any]:
    """Cache, then local file, then the published HF copy (retried at most
    every 15 minutes). Returns {} when nothing is available yet."""
    if _cache["profiles"] is not None:
        return _cache["profiles"]
    if _PROFILES_LOCAL_PATH.exists():
        try:
            profiles = json.loads(_PROFILES_LOCAL_PATH.read_text(encoding="utf-8")).get("profiles") or {}
            _cache["profiles"] = profiles
            return profiles
        except Exception as exc:
            logger.warning("[kalshi_15m_pattern_study] local profile read failed: %s", exc)
    now = time.time()
    if not HF_API_KEY or now - _cache["last_remote_attempt"] < _REMOTE_RETRY_SEC:
        return {}
    _cache["last_remote_attempt"] = now

    def _download() -> dict[str, Any]:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(repo_id=HF_KALSHI_15M_DATASET_REPO, filename=HF_PROFILES_PATH, repo_type="dataset", token=HF_API_KEY)
        with open(path, encoding="utf-8") as f:
            return json.load(f).get("profiles") or {}

    try:
        from server_common import call_with_hard_timeout
        profiles = call_with_hard_timeout(_download, timeout_sec=_REMOTE_TIMEOUT_SEC)
    except Exception as exc:
        logger.info("[kalshi_15m_pattern_study] no published profiles yet: %s", exc)
        return {}
    if not profiles:
        return {}
    _cache["profiles"] = profiles
    return profiles


def score_entry_conditions(
    coin: str, side: str, feature_row: dict[str, Any] | None, profiles: dict[str, Any] | None,
) -> dict[str, Any]:
    """Side-relative read of the current conditions against this coin's
    profile: only statistically significant buckets count. Pure function."""
    empty = {"score": 0, "favorable": [], "unfavorable": [], "details": {}}
    if not profiles or not feature_row:
        return {**empty, "reason": "no_profile_or_feature_row"}
    profile = profiles.get(coin)
    if not profile:
        return {**empty, "reason": "no_profile_for_coin", "coin": coin}

    flip = 1.0 if side == "yes" else -1.0
    favorable: list[str] = []
    unfavorable: list[str] = []
    details: dict[str, Any] = {}

    cascade = feature_row.get("mtf_cascade_score")
    if _is_nan(cascade):
        cascade = mtf_cascade_score([feature_row.get(c) for c in CASCADE_COLUMNS])
    current = {
        "hour_et": ("by_hour", str(_current_et_hour())),
        "vol_regime": ("by_vol_regime", _vol_regime(feature_row.get("volatility_15"))),
        "rsi_regime": ("by_rsi_regime", _rsi_regime(feature_row.get("rsi_14"))),
        "trend_alignment": ("by_trend_alignment", _trend_alignment(feature_row.get("trend_1h"), feature_row.get("ret_15m"))),
        "volume_regime": ("by_volume_regime", _volume_regime(feature_row.get("dollar_volume_z"))),
        "macd_regime": ("by_macd_regime", _macd_regime(feature_row.get("macd_hist_pct"))),
        "mtf_cascade": ("by_cascade_regime", _cascade_regime(cascade)),
    }
    for dim, (profile_key, bucket) in current.items():
        bucket_stats = (profile.get(profile_key) or {}).get(bucket) if bucket else None
        if not bucket_stats:
            continue
        side_delta = float(bucket_stats["delta"]) * flip
        verdict = "neutral"
        if bucket_stats.get("significant"):
            verdict = "favorable" if side_delta > 0 else "unfavorable"
            (favorable if side_delta > 0 else unfavorable).append(dim)
        details[dim] = {**bucket_stats, "bucket": bucket, "side_delta": round(side_delta, 4), "verdict": verdict}

    score = len(favorable) - len(unfavorable)
    return {
        "score": score, "favorable": favorable, "unfavorable": unfavorable, "details": details,
        "coin": coin, "side": side, "mtf_cascade_score": cascade,
        "baseline_win_rate": profile.get("overall_win_rate"), "profile_samples": profile.get("total_samples"),
        "reason": f"score_{score}" if (favorable or unfavorable) else "all_neutral",
    }
