"""One-off migration: the existing Kalshi HF archive was written with the
OLD engineer_features() schema (8 features, 30-minute label horizon). The
new schema (17 features: 1/3/5/10/15/30-minute + 1/2/3/4-hour timeframes,
1-minute label horizon) adds columns that simply don't exist in those old
rows at all -- not NaN, missing entirely -- so training directly against
the old archive would fail outright (dropna against a nonexistent column
raises a KeyError).

This rebuilds the archive's feature columns from the raw ticker/ts/close
series it already contains (every archived row keeps ts+close regardless of
which feature-engineering version wrote it): per ticker, runs the FULL
continuous close series through the CURRENT engineer_features() (must stay
continuous -- slicing by day first would truncate the rolling lookback
windows and wrongly invalidate each day's first ~245 minutes), then
re-buckets the result BY CALENDAR DAY and re-uploads shards under the exact
data/YYYY-MM-DD.parquet naming _DATE_SHARD_RE / load_training_dataset()
expect -- replacing each day's existing shard with a recomputed version
covering that same day, rather than writing under a name the loader would
never pick up.

Known simplification, documented rather than hidden: the original archive's
per-row sentiment_score (whatever the live news reading was at collection
time) is not preserved through this recompute -- engineer_features() takes
one sentiment scalar per ticker-batch, not a per-row historical series, so
this pass uses 0.0 throughout, identical to the existing, already-documented
limitation in perps_backtest.py. The technical features are unaffected.

Also has no genuinely separate archived hourly-candle series to reuse (only
per-minute rows were ever archived) -- reconstructs an hourly series by
taking the last close in each hour-bucket of the same per-minute data,
which is what a real 60-minute candle's close would represent anyway.

Usage: python scripts/recompute_kalshi_archive_features.py
"""
from __future__ import annotations

import logging
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import config  # noqa: E402  bootstraps .env
from data.perps_data import HF_API_KEY, HF_DATASET_REPO, engineer_features, load_training_dataset  # noqa: E402

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("recompute_kalshi_archive_features")


def _hourly_from_one_min(one_min_df: pd.DataFrame) -> pd.DataFrame:
    if one_min_df.empty:
        return pd.DataFrame({"ts": pd.Series(dtype="int64"), "close": pd.Series(dtype="float64")})
    df = one_min_df.copy()
    df["_hour_bucket"] = df["ts"] // 3600
    hourly = df.groupby("_hour_bucket", as_index=False).last()[["ts", "close"]]
    return hourly.sort_values("ts").reset_index(drop=True)


def main() -> None:
    if not HF_API_KEY:
        logger.error("HF_API_KEY not set -- aborting")
        return

    logger.info("Loading full raw archive (ticker/ts/close only matters here) ...")
    raw = load_training_dataset(max_rows=10**9)
    if raw.empty:
        logger.error("archive is empty -- nothing to recompute")
        return
    raw = raw[["ticker", "ts", "close"]].drop_duplicates(subset=["ticker", "ts"])
    logger.info("Loaded %d raw ticker/ts/close rows across %d tickers", len(raw), raw["ticker"].nunique())

    all_recomputed = []
    for ticker, group in raw.groupby("ticker"):
        one_min_df = group[["ts", "close"]].sort_values("ts").reset_index(drop=True)
        hourly_df = _hourly_from_one_min(one_min_df)
        feats = engineer_features(one_min_df, hourly_df, sentiment_score=0.0)
        if feats.empty:
            logger.warning("[%s] recompute produced no valid rows (only %d raw rows) -- skipping", ticker, len(one_min_df))
            continue
        feats.insert(0, "ticker", ticker)
        all_recomputed.append(feats)
        logger.info("[%s] recomputed %d rows from %d raw rows", ticker, len(feats), len(one_min_df))

    if not all_recomputed:
        logger.error("nothing recomputed across any ticker -- aborting before touching HF")
        return

    combined = pd.concat(all_recomputed, ignore_index=True)
    combined["_date_str"] = pd.to_datetime(combined["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")

    from huggingface_hub import HfApi
    api = HfApi(token=HF_API_KEY)

    total_rows = 0
    for date_str, day_group in combined.groupby("_date_str"):
        day_df = day_group.drop(columns=["_date_str"]).reset_index(drop=True)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            day_df.to_parquet(tmp.name, index=False)
            tmp_path = tmp.name
        try:
            api.upload_file(
                path_or_fileobj=tmp_path, path_in_repo=f"data/{date_str}.parquet",
                repo_id=HF_DATASET_REPO, repo_type="dataset",
                commit_message=f"recompute features for {date_str} (new multi-timeframe schema)",
            )
        finally:
            os.unlink(tmp_path)
        total_rows += len(day_df)
        logger.info("[%s] uploaded %d recomputed rows (replaces the old shard for this date)", date_str, len(day_df))

    logger.info("DONE. Total recomputed rows uploaded across %d days: %d", combined["_date_str"].nunique(), total_rows)


if __name__ == "__main__":
    main()
