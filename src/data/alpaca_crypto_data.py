"""Alpaca crypto market data -- a new, separate pipeline from alpaca_data.py
(equities), built the same way the user asked: "the same recipe as perps"
(perps_data.py's technical-indicator + real news-sentiment approach), but
trading crypto pairs THROUGH Alpaca instead of Kalshi perpetual futures.

Genuinely different from alpaca_data.py in a few ways, not just a renamed
copy:
  - Crypto trades 24/7 -- no market session, no time_of_day_pct (which was
    anchored to the 9:30-4:00 equity session). Uses perps_data.py's own
    hour_sin/hour_cos/dow_sin/dow_cos cyclical time encoding instead, which
    makes sense for a market with no daily open/close.
  - Sentiment reuses crypto_news.py directly (already coin-mapped, already
    proven for perps) rather than stock_news.py -- "BTC/USD" maps to the
    coin "BTC" crypto_news.get_sentiment() already knows how to fetch.
  - The tradable universe (36 USD-quoted pairs on this account, confirmed
    via Alpaca's own /v2/assets -- smaller than equities' thousands, but
    not as small as originally assumed here) is tracked directly with no
    watchlist-ranking step, same as perps_data.py's own small fixed
    KNOWN_PERP_TICKERS list.

Data flow (mirrors alpaca_data.py's shape):
  get_crypto_universe()        -> tradable USD-quoted crypto pairs, from
                                   Alpaca's own /v2/assets (asset_class=crypto)
  fetch_recent_crypto_bars(symbol) -> a short, cached window for live
                                   scanning/collection
  engineer_features(df)        -> leakage-free technical features + real
                                   crypto sentiment, same backward-only
                                   discipline as every other pipeline here
  push_minute_snapshot(df)     -> append/dedupe into today's parquet shard,
                                   upload to HF_ALPACA_CRYPTO_DATASET_REPO
"""
from __future__ import annotations

import datetime as dt
import gc
import logging
import os
import re
import time
from typing import Any

import numpy as np
import pandas as pd

from data import alpaca_client
from data.crypto_correlation import ALPACA_CRYPTO_CORRELATION_STUDY_HF_FILENAME
from data.crypto_news import get_sentiment, prewarm_sentiment

logger = logging.getLogger(__name__)

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_ALPACA_CRYPTO_DATASET_REPO = os.getenv("HF_ALPACA_CRYPTO_DATASET_REPO", "papylove/alpaca-crypto-data")


def symbol_to_coin(symbol: str) -> str:
    """"BTC/USD" -> "BTC" -- the coin code crypto_news.get_sentiment() and
    its _COIN_QUERIES dict already key off of."""
    return symbol.split("/")[0].upper()


_UNIVERSE_CACHE_TTL_SEC = 24 * 3600
_universe_cache: dict[str, Any] = {"symbols": None, "computed_at": 0.0}


# USD, and the two major dollar-pegged stablecoin quote currencies --
# USDT/USDC both target a tight 1:1 peg to the dollar, so treating a
# "XRP/USDT" quote as dollar-denominated for this strategy's own P&L math
# is a reasonable, deliberate approximation, not a real currency-mixing
# bug. Explicitly does NOT include BTC-quoted pairs (e.g. "ETH/BTC") --
# that price is a ratio against another volatile asset, not a dollar
# figure at all; multiplying it straight into `(current_price - entry_price)
# * count` the way manage_open_positions() does would produce a nonsense
# "USD" P&L number, a real correctness bug this deliberately avoids
# introducing while still widening the pool of tradable pairs.
_USD_EQUIVALENT_QUOTE_SUFFIXES = ("/USD", "/USDT", "/USDC")


def get_crypto_universe(*, force: bool = False) -> list[str]:
    """Every tradable USD-equivalent-quoted crypto pair on this Alpaca
    account (BTC/USD, ETH/USD, XRP/USDT, ... -- see
    _USD_EQUIVALENT_QUOTE_SUFFIXES for exactly which quote currencies
    count). Widened from USD-only: Alpaca's own crypto asset list spans
    56 pairs across 20+ coins quoted in BTC/USD/USDT/USDC combined
    (confirmed via Alpaca's docs), and restricting to /USD-only left real,
    tradable pairs (anything quoted only in USDT/USDC) off the table for
    no platform reason -- the user's own explicit ask was a WIDER universe,
    not just deeper logic on the same narrow one. Cached for
    _UNIVERSE_CACHE_TTL_SEC, same as alpaca_data.py's equity universe."""
    now = time.time()
    if not force and _universe_cache["symbols"] is not None and (now - _universe_cache["computed_at"]) < _UNIVERSE_CACHE_TTL_SEC:
        return _universe_cache["symbols"]

    try:
        assets = alpaca_client.get_assets(status="active", asset_class="crypto")
        symbols = sorted({
            a["symbol"] for a in assets
            if a.get("tradable") and a.get("symbol", "").endswith(_USD_EQUIVALENT_QUOTE_SUFFIXES)
        })
    except Exception as exc:
        logger.warning("[alpaca_crypto_data] failed to fetch crypto asset universe: %s", exc)
        symbols = _universe_cache["symbols"] or []

    _universe_cache["symbols"] = symbols
    _universe_cache["computed_at"] = now
    return symbols


def _bars_to_df(bars: list[dict[str, Any]]) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame({
            "ts": pd.Series(dtype="int64"), "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"), "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"), "volume": pd.Series(dtype="float64"),
        })
    rows = [{
        "ts": int(pd.Timestamp(b["t"]).timestamp()),
        "open": float(b["o"]), "high": float(b["h"]), "low": float(b["l"]),
        "close": float(b["c"]), "volume": float(b.get("v") or 0.0),
    } for b in bars]
    return pd.DataFrame(rows).drop_duplicates(subset="ts").sort_values("ts").reset_index(drop=True)


# Real, confirmed oomKilled crash caught live (mid-monitoring, right after
# the n_estimators=100->60 model fix deployed -- proving that fix alone
# wasn't sufficient) landing inside _run_alpaca_crypto_data_collect, which
# calls collect_dataset_rows() with no symbol narrowing -- the FULL ~56-pair
# universe, each fetched at this lookback depth, all held in memory
# simultaneously before the final concat + push_minute_snapshot's own
# download-existing-shard/merge/reupload. Unlike equities (which fixed the
# equivalent issue by narrowing WATCHLIST_TOP_N), crypto deliberately
# collects its full tradable universe with no ranking step (see this
# module's own docstring), so the lookback depth is the lever here instead:
# engineer_features' longest rolling window is ~90 minutes, so 5 days was
# already far more than feature computation actually needs.
LIVE_LOOKBACK_DAYS = int(os.getenv("ALPACA_CRYPTO_LIVE_LOOKBACK_DAYS", "2") or "2")
_MINUTE_BAR_CACHE_TTL_SEC = int(os.getenv("ALPACA_CRYPTO_MINUTE_BAR_CACHE_TTL_SEC", "90") or "90")
# Defensive bound matching the same fix applied to alpaca_data.py's own
# equivalent cache after a real, confirmed OOM was traced to it never
# being pruned -- crypto's universe is small/fixed so this is much less
# likely to matter here, but costs nothing to guard against the same
# unbounded-growth shape.
_MINUTE_BAR_CACHE_MAX_AGE_SEC = _MINUTE_BAR_CACHE_TTL_SEC * 40
_minute_bar_cache: dict[str, tuple[pd.DataFrame, float]] = {}


def _prune_minute_bar_cache(now_mono: float) -> None:
    stale = [k for k, (_, ts) in _minute_bar_cache.items() if (now_mono - ts) > _MINUTE_BAR_CACHE_MAX_AGE_SEC]
    for k in stale:
        del _minute_bar_cache[k]


def fetch_crypto_bars(symbol: str, *, days: int = LIVE_LOOKBACK_DAYS) -> pd.DataFrame:
    """Minute OHLCV for one crypto pair over the given lookback window --
    a single call (alpaca_client.get_crypto_bars paginates internally)."""
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(days=days)
    try:
        bars = alpaca_client.get_crypto_bars([symbol], timeframe="1Min", start=start.isoformat(), end=end.isoformat()).get(symbol, [])
    except Exception as exc:
        logger.warning("[alpaca_crypto_data] bar fetch failed for %s: %s", symbol, exc)
        return _bars_to_df([])
    return _bars_to_df(bars)


def fetch_recent_crypto_bars(symbol: str, *, days: int = LIVE_LOOKBACK_DAYS) -> pd.DataFrame:
    """Short-window, short-TTL-cached fetch for live feature computation --
    same rate-limit-conscious caching discipline as alpaca_data.py's
    fetch_recent_minute_bars."""
    cache_key = f"{symbol}:{days}"
    cached = _minute_bar_cache.get(cache_key)
    now_mono = time.monotonic()
    if cached and (now_mono - cached[1]) < _MINUTE_BAR_CACHE_TTL_SEC:
        return cached[0]
    df = fetch_crypto_bars(symbol, days=days)
    _minute_bar_cache[cache_key] = (df, now_mono)
    _prune_minute_bar_cache(now_mono)
    return df


FEATURE_COLUMNS = [
    "ret_1m", "ret_5m", "ret_15m", "ret_30m", "ret_60m",
    "dist_to_ma_15", "dist_to_ma_30",
    "volatility_5", "volatility_15", "volatility_30",
    "volume_ratio_5", "volume_ratio_15", "dollar_volume_z",
    "rsi_14", "macd_hist_pct", "bb_pct_b", "bb_bandwidth", "atr_pct", "stoch_k",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos", "sentiment_score",
]
MIN_ROWS_FOR_FEATURES = 65  # the 60-minute return window + a small buffer


def engineer_features(one_min_df: pd.DataFrame, *, sentiment_score: float = 0.0) -> pd.DataFrame:
    """Leakage-free volume + volatility + return features from 1-minute
    bars -- identical technical-indicator formulas to alpaca_data.py's own
    engineer_features (RSI/MACD/Bollinger/ATR/stochastic/volume ratios),
    with hour/day-of-week cyclical encoding in place of time_of_day_pct
    (crypto has no market session to anchor that feature to) and real
    sentiment_score from crypto_news.get_sentiment(), broadcast as a
    constant across the batch same as every other pipeline here."""
    if one_min_df.empty or len(one_min_df) < MIN_ROWS_FOR_FEATURES:
        return pd.DataFrame()

    df = one_min_df.copy()
    df["ret_1m"] = df["close"].pct_change(1)
    df["ret_5m"] = df["close"].pct_change(5)
    df["ret_15m"] = df["close"].pct_change(15)
    df["ret_30m"] = df["close"].pct_change(30)
    df["ret_60m"] = df["close"].pct_change(60)
    df["ma_15"] = df["close"].rolling(15).mean()
    df["ma_30"] = df["close"].rolling(30).mean()
    df["dist_to_ma_15"] = (df["close"] - df["ma_15"]) / df["ma_15"]
    df["dist_to_ma_30"] = (df["close"] - df["ma_30"]) / df["ma_30"]
    df["volatility_5"] = df["ret_1m"].rolling(5).std()
    df["volatility_15"] = df["ret_1m"].rolling(15).std()
    df["volatility_30"] = df["ret_1m"].rolling(30).std()

    vol_ma_5 = df["volume"].rolling(5).mean()
    vol_ma_15 = df["volume"].rolling(15).mean()
    vol_ma_60 = df["volume"].rolling(60).mean().replace(0, float("nan"))
    df["volume_ratio_5"] = vol_ma_5 / vol_ma_60
    df["volume_ratio_15"] = vol_ma_15 / vol_ma_60
    dollar_volume = df["close"] * df["volume"]
    dv_mean_60 = dollar_volume.rolling(60).mean()
    dv_std_60 = dollar_volume.rolling(60).std().replace(0, float("nan"))
    df["dollar_volume_z"] = (dollar_volume - dv_mean_60) / dv_std_60

    delta = df["close"].diff()
    avg_gain = delta.clip(lower=0).rolling(14).mean()
    avg_loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi_raw = 100 - (100 / (1 + rs))
    rsi_raw = rsi_raw.mask((avg_loss == 0) & (avg_gain > 0), 100.0)
    df["rsi_14"] = rsi_raw / 100.0

    ema_12 = df["close"].ewm(span=12, adjust=False).mean()
    ema_26 = df["close"].ewm(span=26, adjust=False).mean()
    macd_line = ema_12 - ema_26
    macd_signal = macd_line.ewm(span=9, adjust=False).mean()
    df["macd_hist_pct"] = (macd_line - macd_signal) / df["close"]

    bb_mid = df["close"].rolling(20).mean()
    bb_std = df["close"].rolling(20).std()
    bb_range = (4 * bb_std).replace(0, float("nan"))
    df["bb_pct_b"] = (df["close"] - (bb_mid - 2 * bb_std)) / bb_range
    df["bb_bandwidth"] = (4 * bb_std) / bb_mid

    prev_close = df["close"].shift(1)
    true_range = pd.concat([
        df["high"] - df["low"], (df["high"] - prev_close).abs(), (df["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    df["atr_pct"] = true_range.rolling(14).mean() / df["close"]

    low_14 = df["low"].rolling(14).min()
    high_14 = df["high"].rolling(14).max()
    stoch_range = (high_14 - low_14).replace(0, float("nan"))
    df["stoch_k"] = (df["close"] - low_14) / stoch_range

    ts_utc = pd.to_datetime(df["ts"], unit="s", utc=True)
    hour_frac = ts_utc.dt.hour + ts_utc.dt.minute / 60.0
    df["hour_sin"] = np.sin(2 * np.pi * hour_frac / 24.0)
    df["hour_cos"] = np.cos(2 * np.pi * hour_frac / 24.0)
    dow = ts_utc.dt.dayofweek
    df["dow_sin"] = np.sin(2 * np.pi * dow / 7.0)
    df["dow_cos"] = np.cos(2 * np.pi * dow / 7.0)

    df["sentiment_score"] = float(sentiment_score)

    horizon = 1
    df["future_close"] = df["close"].shift(-horizon)
    df["label_up"] = (df["future_close"] > df["close"]).astype("Int64")
    df.loc[df["future_close"].isna(), "label_up"] = pd.NA

    return df.dropna(subset=FEATURE_COLUMNS).reset_index(drop=True)


def _upload_shard(df: pd.DataFrame, *, path_in_repo: str, commit_message: str) -> dict[str, Any]:
    if not HF_API_KEY:
        return {"ok": False, "reason": "no_hf_api_key"}
    import tempfile
    from huggingface_hub import HfApi

    try:
        api = HfApi(token=HF_API_KEY)
        try:
            api.repo_info(repo_id=HF_ALPACA_CRYPTO_DATASET_REPO, repo_type="dataset")
        except Exception:
            api.create_repo(repo_id=HF_ALPACA_CRYPTO_DATASET_REPO, repo_type="dataset", exist_ok=True, private=False)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            df.to_parquet(tmp.name, index=False)
            tmp_path = tmp.name
        try:
            api.upload_file(
                path_or_fileobj=tmp_path, path_in_repo=path_in_repo,
                repo_id=HF_ALPACA_CRYPTO_DATASET_REPO, repo_type="dataset", commit_message=commit_message,
            )
        finally:
            os.unlink(tmp_path)
        return {"ok": True, "rows": len(df), "path": path_in_repo}
    except Exception as exc:
        logger.warning("[alpaca_crypto_data] HF upload failed for %s: %s", path_in_repo, exc)
        return {"ok": False, "error": str(exc)}


def push_correlation_study_to_hf(study: dict[str, Any]) -> dict[str, Any]:
    """Pushes the small correlation-study JSON (see crypto_correlation.py's
    own module docstring) built THIS collect cycle from the same `df`
    collect_dataset_rows() just fetched -- one small file, not the raw
    archive, so perps_data.py's own pull (see
    pull_alpaca_crypto_correlation_study) is cheap and fast on its own,
    independent collect cycle. Best-effort/non-fatal by design, same as
    perps_strategy.py's own _push_durable_state_to_hf: losing one cycle's
    push just means the next cycle's push (15min away, well inside
    get_remote_alpaca_study's own staleness window) catches up."""
    if not HF_API_KEY:
        return {"ok": False, "reason": "no_hf_api_key"}
    import json
    import tempfile
    from huggingface_hub import HfApi

    try:
        api = HfApi(token=HF_API_KEY)
        try:
            api.repo_info(repo_id=HF_ALPACA_CRYPTO_DATASET_REPO, repo_type="dataset")
        except Exception:
            api.create_repo(repo_id=HF_ALPACA_CRYPTO_DATASET_REPO, repo_type="dataset", exist_ok=True, private=False)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp.write(json.dumps(study, indent=2))
            tmp_path = tmp.name
        try:
            api.upload_file(
                path_or_fileobj=tmp_path, path_in_repo=ALPACA_CRYPTO_CORRELATION_STUDY_HF_FILENAME,
                repo_id=HF_ALPACA_CRYPTO_DATASET_REPO, repo_type="dataset",
                commit_message="update alpaca crypto correlation study",
            )
        finally:
            os.unlink(tmp_path)
        return {"ok": True, "ids": len(study.get("ids") or [])}
    except Exception as exc:
        logger.warning("[alpaca_crypto_data] correlation study push to HF failed: %s", exc)
        return {"ok": False, "error": str(exc)}


_DATE_SHARD_RE = re.compile(r"^minute/\d{4}-\d{2}-\d{2}\.parquet$")


def push_minute_snapshot(df: pd.DataFrame) -> dict[str, Any]:
    """Minute bars sharded by calendar day across ALL crypto pairs -- same
    merge-not-overwrite discipline as alpaca_data.py's own push_minute_snapshot.

    Confirmed real, recurring OOM on this exact function (512Mi, oomKilled
    reliably during the upload a moment after every boot): downloading the
    existing shard, concatenating, then uploading held existing+df+combined
    simultaneously with nothing freed until the whole call returned. The
    explicit `del`+gc.collect() here mirrors the same real fix already
    proven necessary on this pipeline's own train_model()."""
    if df.empty:
        return {"ok": False, "reason": "no_rows"}
    if not HF_API_KEY:
        return {"ok": False, "reason": "no_hf_api_key"}

    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    path_in_repo = f"minute/{today}.parquet"
    combined = df
    try:
        from huggingface_hub import hf_hub_download
        existing_path = hf_hub_download(repo_id=HF_ALPACA_CRYPTO_DATASET_REPO, filename=path_in_repo, repo_type="dataset", token=HF_API_KEY)
        existing = pd.read_parquet(existing_path)
        combined = pd.concat([existing, df], ignore_index=True)
        del existing
    except Exception as exc:
        logger.info("[alpaca_crypto_data] no existing minute shard for %s yet (or fetch failed), starting fresh: %s", today, exc)

    if "symbol" in combined.columns and "ts" in combined.columns:
        combined = combined.drop_duplicates(subset=["symbol", "ts"]).sort_values(["symbol", "ts"]).reset_index(drop=True)
    del df
    gc.collect()
    try:
        return _upload_shard(combined, path_in_repo=path_in_repo, commit_message=f"append crypto minute bars: {today}")
    finally:
        del combined
        gc.collect()


def _upload_shards_batch(entries: list[tuple[str, pd.DataFrame]], *, commit_message: str) -> dict[str, bool]:
    """Upload multiple date-shards in ONE HF commit instead of one commit per
    shard -- independent copy of alpaca_options_data's own version (itself
    copied from alpaca_data.py), same rationale: a real, confirmed incident
    on the stocks backfill hit HF's 128-commits/hour repo cap partway
    through a one-commit-per-date loop, silently dropping most of the run's
    dates (the caller only fails per-shard, so the run still reported
    ok:true overall). Batching many files into a single create_commit call
    keeps total commits per backfill run far below that cap no matter how
    many calendar dates are touched."""
    if not entries:
        return {}
    import tempfile
    from huggingface_hub import CommitOperationAdd, HfApi

    api = HfApi(token=HF_API_KEY)
    try:
        api.repo_info(repo_id=HF_ALPACA_CRYPTO_DATASET_REPO, repo_type="dataset")
    except Exception:
        api.create_repo(repo_id=HF_ALPACA_CRYPTO_DATASET_REPO, repo_type="dataset", exist_ok=True, private=False)

    tmp_paths = []
    try:
        operations = []
        for path_in_repo, df in entries:
            tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
            df.to_parquet(tmp.name, index=False)
            tmp.close()
            tmp_paths.append(tmp.name)
            operations.append(CommitOperationAdd(path_in_repo=path_in_repo, path_or_fileobj=tmp.name))
        api.create_commit(repo_id=HF_ALPACA_CRYPTO_DATASET_REPO, repo_type="dataset", operations=operations, commit_message=commit_message)
        return {path_in_repo: True for path_in_repo, _ in entries}
    except Exception as exc:
        logger.warning("[alpaca_crypto_data] HF batch upload failed for %d shard(s): %s", len(entries), exc)
        return {path_in_repo: False for path_in_repo, _ in entries}
    finally:
        for p in tmp_paths:
            try:
                os.unlink(p)
            except Exception:
                pass


def backfill_minute_history(symbols: list[str], *, days: int = 90) -> dict[str, Any]:
    """Deep historical minute-bar backfill -- the live collector
    (push_minute_snapshot, called from _run_alpaca_crypto_data_collect) only
    ever writes TODAY's shard, so the archive load_training_dataset()/
    backtests read from only ever grows one day at a time from whenever
    collection first started. Same shape and reasoning as alpaca_data.py's/
    alpaca_options_data.py's own backfill_minute_history -- crypto never
    got this treatment: confirmed live, this pipeline's own real archive
    only had 7 daily shards (a week) covering exactly the organic
    collect-one-day-at-a-time growth since this pipeline started, while
    Alpaca's own crypto bars go back much further (crypto trades 24/7, no
    market-session gate, so a `days`-long fetch_crypto_bars call covers
    genuinely continuous history, not just regular-session minutes the way
    equities/options backfills do).

    Historical sentiment for arbitrary past dates isn't available from any
    free news API -- held at neutral (0.0) for every backfilled row, same
    disclosed limitation as the equities/options versions."""
    from collections import defaultdict
    if not HF_API_KEY:
        return {"ok": False, "reason": "no_hf_api_key"}

    by_date: dict[str, list[pd.DataFrame]] = defaultdict(list)
    symbols_processed = 0
    for symbol in symbols:
        try:
            one_min_df = fetch_crypto_bars(symbol, days=days)
            if one_min_df.empty:
                continue
            feats = engineer_features(one_min_df, sentiment_score=0.0)
            del one_min_df
            if feats.empty:
                continue
            feats.insert(0, "symbol", symbol)
            date_strs = pd.to_datetime(feats["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
            for date_str, group in feats.groupby(date_strs):
                by_date[date_str].append(group.reset_index(drop=True))
            del feats, date_strs
            symbols_processed += 1
        except Exception as exc:
            logger.warning("[alpaca_crypto_data] backfill fetch failed for %s: %s", symbol, exc)
        gc.collect()

    from huggingface_hub import hf_hub_download
    _BATCH_SIZE = 20  # ~5 commits for a full 90-day backfill, far under HF's 128/hour cap
    shard_row_counts: dict[str, int] = {}
    pending: list[tuple[str, pd.DataFrame]] = []

    def _flush(batch_num: int) -> None:
        if not pending:
            return
        results = _upload_shards_batch(pending, commit_message=f"backfill crypto minute bars: batch {batch_num}")
        for path_in_repo, df in pending:
            date_str = path_in_repo.rsplit("/", 1)[-1].removesuffix(".parquet")
            shard_row_counts[date_str] = len(df) if results.get(path_in_repo) else -1
        pending.clear()

    batch_num = 0
    for date_str in sorted(by_date.keys()):
        groups = by_date.pop(date_str)
        combined_new = pd.concat(groups, ignore_index=True)
        del groups
        path_in_repo = f"minute/{date_str}.parquet"
        try:
            existing_path = hf_hub_download(repo_id=HF_ALPACA_CRYPTO_DATASET_REPO, filename=path_in_repo, repo_type="dataset", token=HF_API_KEY)
            existing = pd.read_parquet(existing_path)
            combined = pd.concat([existing, combined_new], ignore_index=True)
            del existing
        except Exception:
            combined = combined_new
        del combined_new
        combined = combined.drop_duplicates(subset=["symbol", "ts"]).sort_values(["symbol", "ts"]).reset_index(drop=True)
        pending.append((path_in_repo, combined))
        if len(pending) >= _BATCH_SIZE:
            batch_num += 1
            _flush(batch_num)
        gc.collect()
    if pending:
        batch_num += 1
        _flush(batch_num)

    return {
        "ok": True, "symbols_processed": symbols_processed, "symbols_requested": len(symbols),
        "dates_written": sum(1 for v in shard_row_counts.values() if v >= 0),
        "dates_failed": sum(1 for v in shard_row_counts.values() if v < 0),
        "shard_row_counts": shard_row_counts,
    }


MAX_TRAIN_ROWS = int(os.getenv("ALPACA_CRYPTO_MAX_TRAIN_ROWS", "150000") or "150000")


def collect_dataset_rows(symbols: list[str] | None = None) -> pd.DataFrame:
    """Fetch + engineer features for the given crypto pairs (default: the
    full tradable universe -- small enough, unlike equities, that there's
    no need to narrow to a ranked top-N watchlist first)."""
    target_symbols = symbols if symbols is not None else get_crypto_universe()
    # Same real fix as scan_and_enter's own (see prewarm_sentiment's own
    # docstring) -- this job runs across the FULL universe, not just the
    # watchlist, so it's an even bigger sequential-fetch cost than entry_scan.
    try:
        prewarm_sentiment([symbol_to_coin(s) for s in target_symbols])
    except Exception as exc:
        logger.debug("[alpaca_crypto_data] sentiment prewarm failed (non-fatal): %s", exc)
    frames = []
    for symbol in target_symbols:
        try:
            one_min_df = fetch_recent_crypto_bars(symbol)
            sentiment = get_sentiment(symbol_to_coin(symbol))
            feats = engineer_features(one_min_df, sentiment_score=sentiment["sentiment_score"])
            if feats.empty:
                continue
            feats.insert(0, "symbol", symbol)
            frames.append(feats)
        except Exception as exc:
            logger.warning("[alpaca_crypto_data] collect failed for %s: %s", symbol, exc)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def latest_feature_row(symbol: str) -> dict[str, Any] | None:
    """The single most-recent feature row for one crypto pair, for live
    prediction. Its label is always NaN (the future outcome hasn't
    happened yet) -- expected, we only need the feature columns here."""
    try:
        one_min_df = fetch_recent_crypto_bars(symbol)
        sentiment = get_sentiment(symbol_to_coin(symbol))
        feats = engineer_features(one_min_df, sentiment_score=sentiment["sentiment_score"])
        if feats.empty:
            return None
        last = feats.iloc[-1]
        row = {col: float(last[col]) for col in FEATURE_COLUMNS}
        row["symbol"] = symbol
        row["current_price"] = float(one_min_df["close"].iloc[-1])
        row["short_ma"] = float(last["ma_15"])
        return row
    except Exception as exc:
        logger.warning("[alpaca_crypto_data] latest_feature_row failed for %s: %s", symbol, exc)
        return None


# 45s/25s -- real, confirmed live recalibration on the equities sibling of
# this function, twice over: both 10s/8s and 20s/15s proved tight enough
# that a real (non-hung, just GIL-contended with the scheduler's own
# concurrent HF/Alpaca calls) call regularly exceeded them and returned an
# empty "no_data" result. 300s gunicorn --timeout (render.yaml) leaves
# ample headroom for this.
_LOAD_TRAINING_DATASET_LIST_TIMEOUT_SEC = int(os.getenv("ALPACA_CRYPTO_LOAD_TRAINING_DATASET_LIST_TIMEOUT_SEC", "45") or "45")
_LOAD_TRAINING_DATASET_SHARD_TIMEOUT_SEC = int(os.getenv("ALPACA_CRYPTO_LOAD_TRAINING_DATASET_SHARD_TIMEOUT_SEC", "25") or "25")


def load_training_dataset(*, max_shards: int = 90, max_rows: int | None = None) -> pd.DataFrame:
    """Downloads minute-bar shards from HF_ALPACA_CRYPTO_DATASET_REPO,
    most-recent-first, stopping once enough rows are in hand to cover the
    cap -- same discipline as every other load_training_dataset here.

    Real, confirmed production incident on the equities sibling of this
    exact function (see alpaca_data.py's own copy of this docstring for the
    full incident writeup): called synchronously from a Flask request
    handler, neither the initial list_repo_files call nor any of the
    up-to-90 sequential hf_hub_download calls used to have a timeout --
    huggingface_hub's internal shared-session lock can hang indefinitely,
    and even without a hang, 90 sequential downloads can legitimately
    exceed gunicorn's own --timeout ceiling. Each call below is
    individually bounded so ONE stuck/slow shard degrades to "skip it,
    keep going" instead of freezing the whole function or (worse) getting
    gunicorn's WORKER TIMEOUT to SIGABRT the process -- which, since the
    background APScheduler thread lives in that same process, would take
    every scheduled job down with it too.

    Real, confirmed production incident on the equities sibling of this
    exact function (a 512Mi oomKilled during its own daily train cron):
    accumulating every downloaded shard in one Python list and
    pd.concat()-ing them ALL at once at the end was fine when the archive
    only held a handful of days, but the real historical backfill this same
    session added (~8 days -> a full year, ~250+ shards per market) means
    max_shards=90 can now legitimately mean up to 90 full-day, all-symbol
    DataFrames held simultaneously in memory before the final concat even
    starts. Flushing into a running `combined` frame every
    _SHARD_FLUSH_BATCH shards bounds peak "raw shard frames held at once"
    to that batch size instead of max_shards.

    Real, confirmed production incident on the equities sibling of this
    exact function, found immediately after deploying the timeout fix
    above: call_with_hard_timeout spins up a FRESH ThreadPoolExecutor per
    call and (by design) can't forcibly kill an underlying hung thread,
    only stop waiting on it. Calling it once per shard -- up to 91 times in
    a single invocation -- on a job that repeats regularly meant any
    transient slow/hung call left an abandoned thread running in the
    background indefinitely, competing for the same GIL as every later
    call and visibly degrading them. _shared_hf_call() below reuses ONE
    bounded-size executor for every HF call in a single invocation instead,
    explicitly shut down when this function returns."""
    if not HF_API_KEY:
        return pd.DataFrame()
    cap = MAX_TRAIN_ROWS if max_rows is None else max_rows
    stop_after_rows = int(cap * 1.5) if cap else None
    _SHARD_FLUSH_BATCH = 10
    pending: list[pd.DataFrame] = []
    combined: pd.DataFrame | None = None
    accumulated_rows = 0

    def _flush_pending() -> None:
        nonlocal combined, pending
        if not pending:
            return
        combined = pd.concat([combined, *pending], ignore_index=True) if combined is not None else pd.concat(pending, ignore_index=True)
        pending = []
        gc.collect()

    from concurrent.futures import ThreadPoolExecutor
    from concurrent.futures import TimeoutError as FutureTimeoutError
    executor = ThreadPoolExecutor(max_workers=4)

    def _shared_hf_call(fn, *, timeout_sec: float):
        try:
            return executor.submit(fn).result(timeout=timeout_sec)
        except FutureTimeoutError:
            logger.warning("[alpaca_crypto_data] HF call exceeded %ss, giving up", timeout_sec)
            return None

    try:
        from huggingface_hub import HfApi, hf_hub_download
        api = HfApi(token=HF_API_KEY)
        raw_files = _shared_hf_call(
            lambda: api.list_repo_files(repo_id=HF_ALPACA_CRYPTO_DATASET_REPO, repo_type="dataset"),
            timeout_sec=_LOAD_TRAINING_DATASET_LIST_TIMEOUT_SEC,
        )
        if raw_files is None:
            return pd.DataFrame()
        hf_files = [f for f in raw_files if _DATE_SHARD_RE.match(f)]
        hf_files = sorted(hf_files, reverse=True)[:max_shards]
        for f in hf_files:
            if stop_after_rows and accumulated_rows >= stop_after_rows:
                break
            try:
                local_path = _shared_hf_call(
                    lambda f=f: hf_hub_download(repo_id=HF_ALPACA_CRYPTO_DATASET_REPO, filename=f, repo_type="dataset", token=HF_API_KEY),
                    timeout_sec=_LOAD_TRAINING_DATASET_SHARD_TIMEOUT_SEC,
                )
                if local_path is None:
                    continue
                shard = pd.read_parquet(local_path)
                if "symbol" in shard.columns and "ts" in shard.columns:
                    pending.append(shard)
                    accumulated_rows += len(shard)
                    if len(pending) >= _SHARD_FLUSH_BATCH:
                        _flush_pending()
                else:
                    logger.warning("[alpaca_crypto_data] skipping shard with unexpected schema: %s", f)
            except Exception as exc:
                logger.warning("[alpaca_crypto_data] failed to read shard %s: %s", f, exc)
    except Exception as exc:
        logger.warning("[alpaca_crypto_data] HF dataset listing failed: %s", exc)
    finally:
        executor.shutdown(wait=False)
    _flush_pending()

    if combined is None or combined.empty:
        return pd.DataFrame()
    if "symbol" in combined.columns and "ts" in combined.columns:
        combined = combined.drop_duplicates(subset=["symbol", "ts"])
        combined["symbol"] = combined["symbol"].astype("category")
        if cap and len(combined) > cap:
            combined = combined.sort_values("ts").tail(cap).reset_index(drop=True)
    return combined
