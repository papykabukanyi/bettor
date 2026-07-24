"""One-off (re-runnable) job: build a deep historical pretraining dataset
from Coinbase's public API (free, no key, not geo-blocked -- Binance is)
for every Kalshi perp coin, in the exact same engineered-feature schema the
live Kalshi pipeline uses (see perps_data.engineer_features), so the
resulting rows can be trained on directly alongside -- or instead of --
Kalshi's own (currently ~7-week-old) archive.

Fetches, per coin:
  - ~4 years of 60-minute candles (cheap: ~117 API calls) for the
    longer-scale trend/volatility features (trend_1h..trend_4h, trend_pct).
  - ~90 days of 1-minute candles for the tighter intraday features
    (ret_1m..ret_30m, volatility_5/15/30) and the 1-minute-ahead label
    (originally 180 days; halved after repeated local disk-exhaustion
    crashes writing the larger files -- see MINUTE_DAYS below).
Newer coins (HYPE, SUI, NEAR...) simply won't have 4 years of real history
-- the fetcher stops early at each coin's actual listing date rather than
erroring or fabricating data.

sentiment_score is held at 0.0 throughout, same documented limitation as
perps_backtest.py: there's no free historical news-sentiment archive to
pull from years back, so this tests/trains the technical signal honestly
rather than faking sentiment history.

Resumable: skips any ticker whose file already exists on HF (see
_already_uploaded_tickers) -- this job has crashed and been restarted
several times on this machine (disk exhaustion, HF's 128-commits/hour cap),
and re-fetching ~7 minutes of API calls for an already-saved ticker would
be pure waste. Re-run this script as many times as needed; it always picks
up where it left off.

Usage: python scripts/build_coinbase_pretrain_dataset.py
"""
from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import config  # noqa: E402  bootstraps .env
from data.coinbase_history import COINBASE_PRODUCT_BY_COIN, fetch_coinbase_history  # noqa: E402
from data.kalshi_perps import KNOWN_PERP_TICKERS  # noqa: E402
from data.perps_data import HF_API_KEY, HF_DATASET_REPO, coin_for_ticker, engineer_features  # noqa: E402

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("build_coinbase_pretrain_dataset")

HOURLY_DAYS = 1460  # ~4 years
# Lowered from 180 after repeated disk-exhaustion crashes on this machine
# ("No space left on device" mid-write, twice) -- halving the largest part
# of each ticker's data roughly halves its temp-file size.
MINUTE_DAYS = 90
# Lowered to 1 (was 4, then 2): even 2 tickers' temp parquet files at once
# (~35MB each) was enough to exhaust local disk a second time. By now well
# over an hour has passed since the initial commit burst, so HF's rolling
# 128/hour window should have real headroom again even at 1 commit/ticker.
TICKERS_PER_COMMIT = 1


def _ensure_repo() -> None:
    from huggingface_hub import HfApi
    api = HfApi(token=HF_API_KEY)
    try:
        api.repo_info(repo_id=HF_DATASET_REPO, repo_type="dataset")
    except Exception:
        api.create_repo(repo_id=HF_DATASET_REPO, repo_type="dataset", exist_ok=True, private=False)


def _already_uploaded_tickers() -> set[str]:
    """Resume support -- this job has crashed and been restarted several
    times (disk exhaustion, HF rate limits); skip tickers whose file is
    already on HF instead of re-fetching ~7 minutes of API calls for
    nothing."""
    from huggingface_hub import HfApi
    api = HfApi(token=HF_API_KEY)
    try:
        files = api.list_repo_files(repo_id=HF_DATASET_REPO, repo_type="dataset")
    except Exception as exc:
        logger.warning("could not list existing files for resume check: %s", exc)
        return set()
    prefix, suffix = "external/coinbase_pretrain_", ".parquet"
    return {f[len(prefix):-len(suffix)] for f in files if f.startswith(prefix) and f.endswith(suffix)}


def _commit_batch(batch: list[tuple[str, "pd.DataFrame"]]) -> None:  # noqa: F821
    from huggingface_hub import CommitOperationAdd, HfApi

    api = HfApi(token=HF_API_KEY)
    operations = []
    tmp_paths = []
    try:
        for ticker, df in batch:
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                df.to_parquet(tmp.name, index=False)
                tmp_paths.append(tmp.name)
                operations.append(CommitOperationAdd(
                    path_in_repo=f"external/coinbase_pretrain_{ticker}.parquet", path_or_fileobj=tmp.name,
                ))
        tickers_str = ", ".join(t for t, _ in batch)
        api.create_commit(
            repo_id=HF_DATASET_REPO, repo_type="dataset", operations=operations,
            commit_message=f"coinbase pretraining data for {tickers_str}",
        )
    finally:
        for p in tmp_paths:
            try:
                os.unlink(p)
            except OSError:
                pass


def main() -> None:
    if not HF_API_KEY:
        logger.error("HF_API_KEY not set -- cannot push results, aborting")
        return
    _ensure_repo()
    already_done = _already_uploaded_tickers()
    if already_done:
        logger.info("resuming: %d tickers already on HF, skipping: %s", len(already_done), sorted(already_done))

    total_rows = 0
    pending_batch = []
    for ticker in KNOWN_PERP_TICKERS:
        if ticker in already_done:
            continue
        coin = coin_for_ticker(ticker)
        product_id = COINBASE_PRODUCT_BY_COIN.get(coin)
        if not product_id:
            logger.warning("no Coinbase mapping for %s (%s) -- skipping", ticker, coin)
            continue

        t0 = time.time()
        logger.info("[%s / %s] fetching hourly (~%d days) ...", ticker, product_id, HOURLY_DAYS)
        hourly_df = fetch_coinbase_history(product_id, days=HOURLY_DAYS, granularity_sec=3600)
        logger.info(
            "[%s] hourly: %d rows (%s -> %s)", ticker, len(hourly_df),
            hourly_df["ts"].min() if not hourly_df.empty else None,
            hourly_df["ts"].max() if not hourly_df.empty else None,
        )

        logger.info("[%s] fetching 1-minute (~%d days) ...", ticker, MINUTE_DAYS)
        one_min_df = fetch_coinbase_history(product_id, days=MINUTE_DAYS, granularity_sec=60)
        logger.info(
            "[%s] 1-minute: %d rows (%s -> %s)", ticker, len(one_min_df),
            one_min_df["ts"].min() if not one_min_df.empty else None,
            one_min_df["ts"].max() if not one_min_df.empty else None,
        )

        if one_min_df.empty:
            logger.warning("[%s] no 1-minute data at all -- skipping feature engineering", ticker)
            continue

        feats = engineer_features(one_min_df, hourly_df, sentiment_score=0.0)
        if feats.empty:
            logger.warning("[%s] engineered feature frame is empty (not enough history) -- skipping", ticker)
            continue
        feats.insert(0, "ticker", ticker)
        feats.insert(1, "source", "coinbase")

        pending_batch.append((ticker, feats))
        total_rows += len(feats)
        logger.info("[%s] engineered %d rows in %.1fs (running total: %d rows)", ticker, len(feats), time.time() - t0, total_rows)

        if len(pending_batch) >= TICKERS_PER_COMMIT:
            _commit_batch(pending_batch)
            logger.info("committed batch of %d tickers to HF", len(pending_batch))
            pending_batch = []

    if pending_batch:
        _commit_batch(pending_batch)
        logger.info("committed final batch of %d tickers to HF", len(pending_batch))

    logger.info("DONE. Total engineered rows pushed across all tickers: %d", total_rows)


if __name__ == "__main__":
    main()
