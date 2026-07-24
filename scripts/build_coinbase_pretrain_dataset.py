"""One-off (re-runnable) job: build a deep historical pretraining dataset
from Coinbase's public API (free, no key, not geo-blocked -- Binance is)
for every Kalshi perp coin, in the exact same engineered-feature schema the
live Kalshi pipeline uses (see perps_data.engineer_features), so the
resulting rows can be trained on directly alongside -- or instead of --
Kalshi's own (currently ~7-week-old) archive.

Fetches, per coin:
  - ~4 years of 60-minute candles (cheap: ~117 API calls) for the
    longer-scale trend/volatility features (trend_1h..trend_4h, trend_pct).
  - ~180 days of 1-minute candles (~864 API calls) for the tighter
    intraday features (ret_1m..ret_30m, volatility_5/15/30) and the
    1-minute-ahead label.
Newer coins (HYPE, SUI, NEAR...) simply won't have 4 years of real history
-- the fetcher stops early at each coin's actual listing date rather than
erroring or fabricating data.

sentiment_score is held at 0.0 throughout, same documented limitation as
perps_backtest.py: there's no free historical news-sentiment archive to
pull from years back, so this tests/trains the technical signal honestly
rather than faking sentiment history.

Streams one ticker at a time straight to HF and discards the local frame
immediately after, so local disk usage never holds more than one ticker's
data at once regardless of how many tickers this covers.

Usage: python scripts/build_coinbase_pretrain_dataset.py
"""
from __future__ import annotations

import logging
import sys
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
MINUTE_DAYS = 180


def _ensure_repo() -> None:
    from huggingface_hub import HfApi
    api = HfApi(token=HF_API_KEY)
    try:
        api.repo_info(repo_id=HF_DATASET_REPO, repo_type="dataset")
    except Exception:
        api.create_repo(repo_id=HF_DATASET_REPO, repo_type="dataset", exist_ok=True, private=False)


def _push_ticker_frame(ticker: str, df) -> None:
    import tempfile
    import os
    from huggingface_hub import HfApi

    api = HfApi(token=HF_API_KEY)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        df.to_parquet(tmp.name, index=False)
        tmp_path = tmp.name
    try:
        api.upload_file(
            path_or_fileobj=tmp_path,
            path_in_repo=f"external/coinbase_pretrain_{ticker}.parquet",
            repo_id=HF_DATASET_REPO, repo_type="dataset",
            commit_message=f"coinbase pretraining data for {ticker}",
        )
    finally:
        os.unlink(tmp_path)


def main() -> None:
    if not HF_API_KEY:
        logger.error("HF_API_KEY not set -- cannot push results, aborting")
        return
    _ensure_repo()

    total_rows = 0
    for ticker in KNOWN_PERP_TICKERS:
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

        _push_ticker_frame(ticker, feats)
        total_rows += len(feats)
        logger.info(
            "[%s] pushed %d engineered rows to HF in %.1fs (running total: %d rows)",
            ticker, len(feats), time.time() - t0, total_rows,
        )

    logger.info("DONE. Total engineered rows pushed across all tickers: %d", total_rows)


if __name__ == "__main__":
    main()
