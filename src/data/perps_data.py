"""Kalshi Perps market data: multi-timeframe candles -> engineered features,
archived to a Hugging Face dataset so the model always has a growing history
to train on (not just whatever Kalshi's API window happens to cover).

Data flow:
  collect_dataset_rows(tickers)  -> pulls 1-min + 60-min candles per ticker
                                     from Kalshi, engineers leakage-free
                                     features + a forward-looking label,
                                     returns a pandas DataFrame
  push_dataset_snapshot(df)      -> appends/dedupes into today's parquet
                                     shard and uploads it to the HF dataset
                                     repo (HF_DATASET_REPO)
  load_training_dataset()        -> downloads every shard from the HF
                                     dataset repo and concatenates them --
                                     this is the "download bitcoin data from
                                     Hugging Face" step the model trains on

Kalshi's candlesticks endpoint caps each call at 5000 candles, so 1-minute
history is fetched in a bounded recent window (a few days) per call -- fine,
since accumulated HF history is what gives the model real depth over time.
"""
from __future__ import annotations

import logging
import os
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd

from data.crypto_news import get_sentiment
from data.kalshi_perps import KNOWN_PERP_TICKERS, get_margin_candlesticks, list_margin_markets

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_DATASET_REPO = os.getenv("HF_DATASET_REPO", "papylove/kalshi-perps-data")

# Exact shape of the paths push_dataset_snapshot() writes: "data/YYYY-MM-DD.parquet".
_DATE_SHARD_RE = re.compile(r"^data/\d{4}-\d{2}-\d{2}\.parquet$")

# How much 1-minute history to pull per collection cycle. Kept well under the
# 5000-candle-per-call cap (a 3-day window is ~4320 one-minute candles).
CANDLE_1M_LOOKBACK_HOURS = int(os.getenv("PERPS_CANDLE_1M_LOOKBACK_HOURS", "72") or "72")
CANDLE_60M_LOOKBACK_HOURS = int(os.getenv("PERPS_CANDLE_60M_LOOKBACK_HOURS", "720") or "720")  # 30 days
LABEL_HORIZON_MINUTES = int(os.getenv("PERPS_LABEL_HORIZON_MINUTES", "30") or "30")

# The HF archive grows every day forever; without a cap, load_training_dataset()
# would eventually load an unbounded number of rows into memory on every train
# run. Render's free tier caps the whole process at 512MB, and this dataset's
# ticker column (kept as Python string objects until capped/typed below) is
# the single biggest memory cost of loading it -- bounding row count bounds
# memory regardless of how much history accumulates over time.
MAX_TRAIN_ROWS = int(os.getenv("PERPS_MAX_TRAIN_ROWS", "150000") or "150000")

_TICKER_TO_COIN = {
    "KXBTCPERP": "BTC", "KXETHPERP": "ETH", "KXSOLPERP": "SOL", "KXXRPPERP": "XRP",
    "KXDOGEPERP": "DOGE", "KXLTCPERP": "LTC", "KXBCHPERP": "BCH", "KXLINKPERP": "LINK",
    "KXSUIPERP": "SUI", "KXNEARPERP": "NEAR", "KXDOTPERP": "DOT", "KXHBARPERP": "HBAR",
    "KXHYPEPERP": "HYPE", "KXKSHIBPERP": "KSHIB", "KXXLMPERP": "XLM", "KXZECPERP": "ZEC",
}


def coin_for_ticker(ticker: str) -> str:
    return _TICKER_TO_COIN.get(ticker, ticker.replace("KX", "").replace("PERP", ""))


def get_watchlist() -> list[str]:
    """All perp instruments Kalshi currently lists, live -- falls back to the
    known snapshot if the listing call fails (e.g. transient network issue)."""
    try:
        markets = list_margin_markets()
        tickers = [m["ticker"] for m in markets if m.get("ticker")]
        if tickers:
            return sorted(tickers)
    except Exception as exc:
        logger.warning("[perps_data] list_margin_markets failed, using known list: %s", exc)
    return list(KNOWN_PERP_TICKERS)


def _candles_to_frame(candles: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for c in candles:
        price = c.get("price") or {}
        close = price.get("close")
        ts = c.get("end_period_ts")
        if close is None or ts is None:
            continue
        rows.append({"ts": int(ts), "close": float(close)})
    if not rows:
        # Explicit numeric dtypes even when empty -- a bare
        # pd.DataFrame(columns=[...]) defaults every column to "object",
        # which silently upcasts an entire pd.concat() to object dtype the
        # moment ANY chunk in a chained fetch (e.g. fetch_extended_candles
        # asking further back than a newly-listed ticker's history) comes
        # back empty, breaking pd.merge_asof downstream with a dtype error.
        return pd.DataFrame({"ts": pd.Series(dtype="int64"), "close": pd.Series(dtype="float64")})
    return pd.DataFrame(rows).drop_duplicates(subset="ts").sort_values("ts").reset_index(drop=True)


_CANDLE_CACHE_TTL_SEC = int(os.getenv("PERPS_CANDLE_CACHE_TTL_SEC", "45") or "45")
_candle_cache: dict[str, tuple[pd.DataFrame, pd.DataFrame, float]] = {}


def fetch_candle_frames(ticker: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (one_minute_df, hourly_df) each with columns [ts, close].
    Short TTL cache so a strategy scan across many tickers and a dashboard
    status check landing in the same window don't double the Kalshi API
    calls for the same ticker."""
    cached = _candle_cache.get(ticker)
    now_mono = time.monotonic()
    if cached and (now_mono - cached[2]) < _CANDLE_CACHE_TTL_SEC:
        return cached[0], cached[1]

    now = int(time.time())
    one_min = get_margin_candlesticks(
        ticker, start_ts=now - CANDLE_1M_LOOKBACK_HOURS * 3600, end_ts=now, period_interval=1,
    )
    hourly = get_margin_candlesticks(
        ticker, start_ts=now - CANDLE_60M_LOOKBACK_HOURS * 3600, end_ts=now, period_interval=60,
    )
    one_min_df = _candles_to_frame(one_min.get("candlesticks") or [])
    hourly_df = _candles_to_frame(hourly.get("candlesticks") or [])
    _candle_cache[ticker] = (one_min_df, hourly_df, now_mono)
    return one_min_df, hourly_df


def engineer_features(one_min_df: pd.DataFrame, hourly_df: pd.DataFrame, *, sentiment_score: float) -> pd.DataFrame:
    """Leakage-free technical features computed only from data up to and
    including each row's own timestamp -- every rolling/shift operation looks
    backward only, never forward."""
    if one_min_df.empty or len(one_min_df) < 35:
        return pd.DataFrame()

    df = one_min_df.copy()
    df["ret_1m"] = df["close"].pct_change(1)
    df["ret_5m"] = df["close"].pct_change(5)
    df["ret_15m"] = df["close"].pct_change(15)
    df["ma_5"] = df["close"].rolling(5).mean()
    df["ma_15"] = df["close"].rolling(15).mean()
    df["ma_30"] = df["close"].rolling(30).mean()
    df["dist_to_ma_15"] = (df["close"] - df["ma_15"]) / df["ma_15"]
    df["dist_to_ma_30"] = (df["close"] - df["ma_30"]) / df["ma_30"]
    df["volatility_15"] = df["ret_1m"].rolling(15).std()

    if not hourly_df.empty and len(hourly_df) >= 2:
        hourly_sorted = hourly_df.sort_values("ts")
        hourly_sorted["trend_pct"] = hourly_sorted["close"].pct_change(
            min(6, len(hourly_sorted) - 1)
        )
        df = pd.merge_asof(
            df.sort_values("ts"), hourly_sorted[["ts", "trend_pct"]].sort_values("ts"),
            on="ts", direction="backward",
        )
    else:
        df["trend_pct"] = 0.0

    df["sentiment_score"] = float(sentiment_score)

    horizon = LABEL_HORIZON_MINUTES
    df["future_close"] = df["close"].shift(-horizon)
    df["label_up"] = (df["future_close"] > df["close"]).astype("Int64")
    df.loc[df["future_close"].isna(), "label_up"] = pd.NA

    return df.dropna(subset=["ma_30", "volatility_15", "trend_pct"]).reset_index(drop=True)


FEATURE_COLUMNS = [
    "ret_1m", "ret_5m", "ret_15m", "dist_to_ma_15", "dist_to_ma_30",
    "volatility_15", "trend_pct", "sentiment_score",
]


def collect_dataset_rows(tickers: list[str] | None = None) -> pd.DataFrame:
    """Fetch + engineer features for every ticker in the watchlist. Returns a
    single concatenated DataFrame with a `ticker` column, ready to push to HF
    or feed straight into training."""
    watchlist = tickers or get_watchlist()
    frames = []
    for ticker in watchlist:
        try:
            one_min_df, hourly_df = fetch_candle_frames(ticker)
            sentiment = get_sentiment(coin_for_ticker(ticker))
            feats = engineer_features(one_min_df, hourly_df, sentiment_score=sentiment["sentiment_score"])
            if feats.empty:
                continue
            feats.insert(0, "ticker", ticker)
            frames.append(feats)
        except Exception as exc:
            logger.warning("[perps_data] collect failed for %s: %s", ticker, exc)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def latest_feature_row(ticker: str) -> dict[str, Any] | None:
    """The single most-recent feature row for one ticker, for live prediction.
    Its label is always NaN (the future outcome hasn't happened yet) -- that's
    expected and fine, we only need the feature columns here."""
    try:
        one_min_df, hourly_df = fetch_candle_frames(ticker)
        sentiment = get_sentiment(coin_for_ticker(ticker))
        feats = engineer_features(one_min_df, hourly_df, sentiment_score=sentiment["sentiment_score"])
        if feats.empty:
            return None
        last = feats.iloc[-1]
        row = {col: float(last[col]) for col in FEATURE_COLUMNS}
        row["ticker"] = ticker
        row["current_price"] = float(one_min_df["close"].iloc[-1])
        row["short_ma"] = float(last["ma_15"])
        row["trend_pct"] = float(last["trend_pct"])
        return row
    except Exception as exc:
        logger.warning("[perps_data] latest_feature_row failed for %s: %s", ticker, exc)
        return None


# ---------------------------------------------------------------------------
# Hugging Face dataset persistence
# ---------------------------------------------------------------------------
_HF_REPO_VERIFIED_CACHE: dict[str, tuple[bool, float]] = {}
_HF_REPO_VERIFIED_TTL_SEC = 24 * 3600
_HF_REPO_FAILURE_RETRY_SEC = 300


def _ensure_dataset_repo() -> bool:
    if not HF_API_KEY:
        return False
    cached = _HF_REPO_VERIFIED_CACHE.get(HF_DATASET_REPO)
    now = time.time()
    if cached:
        ok, checked_at = cached
        ttl = _HF_REPO_VERIFIED_TTL_SEC if ok else _HF_REPO_FAILURE_RETRY_SEC
        if (now - checked_at) < ttl:
            return ok
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
        try:
            api.repo_info(repo_id=HF_DATASET_REPO, repo_type="dataset")
        except Exception:
            api.create_repo(repo_id=HF_DATASET_REPO, repo_type="dataset", exist_ok=True, private=False)
        _HF_REPO_VERIFIED_CACHE[HF_DATASET_REPO] = (True, now)
        return True
    except Exception as exc:
        logger.warning("[perps_data] could not verify/create dataset repo: %s", exc)
        _HF_REPO_VERIFIED_CACHE[HF_DATASET_REPO] = (False, now)
        return False


def push_dataset_snapshot(df: pd.DataFrame) -> dict[str, Any]:
    """Merge new rows into today's parquet shard and upload it to HF. Local
    shard files under data/perps_dataset/ are the source of truth for the
    merge/dedupe step; HF is the durable archive + what training reads from
    on a fresh machine."""
    if df.empty:
        return {"ok": False, "reason": "no_rows"}

    shard_dir = DATA_DIR / "perps_dataset"
    shard_dir.mkdir(parents=True, exist_ok=True)
    today = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    shard_path = shard_dir / f"{today}.parquet"

    if shard_path.exists():
        existing = pd.read_parquet(shard_path)
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df
    combined = combined.drop_duplicates(subset=["ticker", "ts"]).sort_values(["ticker", "ts"])
    combined.to_parquet(shard_path, index=False)

    result: dict[str, Any] = {"ok": True, "rows_written": len(combined), "shard": str(shard_path)}
    if not _ensure_dataset_repo():
        result["hf_uploaded"] = False
        return result
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
        api.upload_file(
            path_or_fileobj=str(shard_path),
            path_in_repo=f"data/{today}.parquet",
            repo_id=HF_DATASET_REPO,
            repo_type="dataset",
            commit_message=f"perps data {today}",
        )
        result["hf_uploaded"] = True
    except Exception as exc:
        logger.warning("[perps_data] HF upload failed: %s", exc)
        result["hf_uploaded"] = False
        result["hf_error"] = str(exc)
    return result


def load_training_dataset(*, max_shards: int = 90, max_rows: int | None = None) -> pd.DataFrame:
    """ALWAYS merges local shards with the full HF dataset archive (deduped
    on ticker+ts) rather than treating HF as only a cold-start fallback --
    a long-running deployment accumulates its own local shards from its
    rolling ~72h collection window and would otherwise never pick up the
    deeper history archived to HF, training on a much thinner slice of data
    than actually exists. This is the "download from Hugging Face" step the
    model actually trains on.

    The result is capped to the most recent `max_rows` rows (default
    MAX_TRAIN_ROWS) -- the HF archive grows every day forever, so without a
    cap this would eventually load more data than fits in Render's 512MB
    memory ceiling. Training on the most recent slice also naturally favors
    current market regime over stale history."""
    shard_dir = DATA_DIR / "perps_dataset"
    local_files = sorted(shard_dir.glob("*.parquet")) if shard_dir.exists() else []
    frames = []
    for f in local_files:
        try:
            frames.append(pd.read_parquet(f))
        except Exception as exc:
            logger.warning("[perps_data] failed to read local shard %s: %s", f, exc)

    if HF_API_KEY:
        try:
            from huggingface_hub import HfApi, hf_hub_download
            api = HfApi(token=HF_API_KEY)
            # Matched against the EXACT pattern push_dataset_snapshot() writes
            # ("data/YYYY-MM-DD.parquet") -- a bare "data/" prefix isn't
            # actually unique: a previous, unrelated pipeline that once wrote
            # to this same HF account archived its own files under
            # "data/pregame_schedule/*.parquet", which also starts with
            # "data/" and would otherwise get fully DOWNLOADED (not just
            # flagged) before the post-download schema check ever caught it.
            hf_files = [
                f for f in api.list_repo_files(repo_id=HF_DATASET_REPO, repo_type="dataset")
                if _DATE_SHARD_RE.match(f)
            ]
            hf_files = sorted(hf_files)[-max_shards:]
            for f in hf_files:
                try:
                    local_path = hf_hub_download(repo_id=HF_DATASET_REPO, filename=f, repo_type="dataset", token=HF_API_KEY)
                    shard = pd.read_parquet(local_path)
                    if "ticker" in shard.columns and "ts" in shard.columns:
                        frames.append(shard)
                    else:
                        logger.warning("[perps_data] skipping HF shard with unexpected schema: %s", f)
                except Exception as exc:
                    logger.warning("[perps_data] failed to read HF shard %s: %s", f, exc)
        except Exception as exc:
            logger.warning("[perps_data] HF dataset listing failed: %s", exc)

    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    del frames  # the individual shard frames are redundant with `combined`; drop the reference before the next copy-making step
    if "ticker" in combined.columns and "ts" in combined.columns:
        combined = combined.drop_duplicates(subset=["ticker", "ts"])
        combined["ticker"] = combined["ticker"].astype("category")  # object-dtype strings are the single biggest memory cost of this frame
        cap = MAX_TRAIN_ROWS if max_rows is None else max_rows
        if cap and len(combined) > cap:
            combined = combined.sort_values("ts").tail(cap).reset_index(drop=True)
    return combined
