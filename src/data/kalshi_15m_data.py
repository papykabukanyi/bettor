"""Data pipeline for Kalshi's 15-minute event-contract markets (see
kalshi_15m.py's own module docstring for the product itself).

Deliberately reuses perps_data.py's own candle feed and feature-engineering
formulas rather than duplicating them: the underlying price signal (BTC/
ETH/SOL/XRP/DOGE technicals) is IDENTICAL to what perps_strategy.py already
predicts on, computed from the SAME Kalshi perp contract's own candlestick
series (KXBTCPERP etc.) as a real, already-flowing proxy for the CF
Benchmarks index these 15m markets actually settle against -- confirmed
this account has no independent access to that exact settlement index, and
a perp contract's own mark price tracks its underlying asset's real price
extremely tightly via continuous arbitrage, making it the best available
real signal without adding a new external data dependency. What's
genuinely NEW here is only the LABEL: perps_data.engineer_features()
computes its own label at perps_data.LABEL_HORIZON_MINUTES (1 minute) --
this module calls that function UNCHANGED for the full feature set, then
overwrites future_close/label_up with a 15-minute-forward horizon (see
LABEL_HORIZON_MINUTES below), matching what Kalshi's own contract actually
resolves on.

Same "independent per-market module, not a shared base class" convention
this whole codebase already follows (see any of the 4 existing markets'
own *_data.py docstrings) -- this is a 5th such module, not a
parametrization of perps_data.py itself.
"""
from __future__ import annotations

import gc
import logging
import os
import re
from typing import Any

import pandas as pd

from data import perps_data
from data.crypto_news import get_sentiment
from server_common import DATA_DIR

logger = logging.getLogger(__name__)

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_KALSHI_15M_DATASET_REPO = os.getenv("HF_KALSHI_15M_DATASET_REPO", "papylove/kalshi-15m-data")

# The 5 underlyings this module trades -- see kalshi_15m.KNOWN_15M_SERIES's
# own comment for why this is scoped to perps_strategy.py's existing 5, not
# the full confirmed-live set of 15m series.
COIN_TO_PERPS_TICKER = {
    "BTC": "KXBTCPERP", "ETH": "KXETHPERP", "SOL": "KXSOLPERP",
    "XRP": "KXXRPPERP", "DOGE": "KXDOGEPERP",
    # 4 more real, confirmed-live 15-minute series (see kalshi_15m.py's
    # own KNOWN_15M_SERIES comment) -- perps already tracks all 4 of
    # these underlyings via its own _TICKER_TO_COIN, so this is the same
    # zero-new-infrastructure proxy reuse as the 5 above.
    "BCH": "KXBCHPERP", "NEAR": "KXNEARPERP", "HYPE": "KXHYPEPERP", "ZEC": "KXZECPERP",
}

# The real, resolution-matching horizon: Kalshi's own 15m contracts compare
# a reference price exactly 15 minutes apart (see kalshi_15m.py's own
# module docstring) -- NOT perps_data.LABEL_HORIZON_MINUTES (1), which
# stays untouched here (this module reads that constant only to know it's
# DIFFERENT from its own, never writes it).
LABEL_HORIZON_MINUTES = int(os.getenv("KALSHI_15M_LABEL_HORIZON_MINUTES", "15") or "15")


def get_universe() -> list[str]:
    return list(COIN_TO_PERPS_TICKER.keys())


def _relabel_for_horizon(feats: pd.DataFrame) -> pd.DataFrame:
    """Overwrites perps_data.engineer_features' own 1-minute-horizon
    future_close/label_up with this module's 15-minute one, computed from
    the SAME already-present `close` column -- see this module's own
    docstring for why the rest of the feature set is reused unchanged."""
    feats = feats.copy()
    feats["future_close"] = feats["close"].shift(-LABEL_HORIZON_MINUTES)
    feats["label_up"] = (feats["future_close"] > feats["close"]).astype("Int64")
    feats.loc[feats["future_close"].isna(), "label_up"] = pd.NA
    return feats


def collect_dataset_rows(coins: list[str] | None = None) -> pd.DataFrame:
    """Fetch + engineer features for the given coins (default: this
    module's own full universe -- see COIN_TO_PERPS_TICKER), relabeled to
    a 15-minute horizon."""
    target_coins = coins if coins is not None else get_universe()
    frames = []
    for coin in target_coins:
        perps_ticker = COIN_TO_PERPS_TICKER.get(coin)
        if not perps_ticker:
            logger.warning("[kalshi_15m_data] no perps ticker mapping for coin %s", coin)
            continue
        try:
            one_min_df, hourly_df = perps_data.fetch_candle_frames(perps_ticker)
            sentiment = get_sentiment(coin, use_limited_sources=True)
            feats = perps_data.engineer_features(one_min_df, hourly_df, sentiment_score=sentiment["sentiment_score"])
            if feats.empty:
                continue
            feats = _relabel_for_horizon(feats)
            feats.insert(0, "symbol", coin)
            frames.append(feats)
        except Exception as exc:
            logger.warning("[kalshi_15m_data] collect failed for %s: %s", coin, exc)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def latest_feature_row(coin: str) -> dict[str, Any] | None:
    """The single most-recent feature row for one coin, for live
    prediction -- label is always NaN (the future 15 minutes haven't
    happened yet), only the feature columns matter here."""
    perps_ticker = COIN_TO_PERPS_TICKER.get(coin)
    if not perps_ticker:
        return None
    try:
        one_min_df, hourly_df = perps_data.fetch_candle_frames(perps_ticker)
        sentiment = get_sentiment(coin, use_limited_sources=True)
        feats = perps_data.engineer_features(one_min_df, hourly_df, sentiment_score=sentiment["sentiment_score"])
        if feats.empty:
            return None
        last = feats.iloc[-1]
        row = {col: float(last[col]) for col in perps_data.FEATURE_COLUMNS}
        row["symbol"] = coin
        row["current_price"] = float(one_min_df["close"].iloc[-1])
        return row
    except Exception as exc:
        logger.warning("[kalshi_15m_data] latest_feature_row failed for %s: %s", coin, exc)
        return None


# ---------------------------------------------------------------------------
# HF archival -- faithfully mirrors perps_data.py's own push_dataset_snapshot/
# load_training_dataset (see either's own docstring for the full incident
# history behind every timeout/atomic-write/OOM-avoidance choice below);
# this is a fresh, adapted copy, not a call into perps_data.py's own
# functions, since those are hardcoded to perps' own HF_DATASET_REPO/shard
# directory -- see this module's own top docstring on why independent
# per-market modules are this codebase's established convention.
# ---------------------------------------------------------------------------
_DATE_SHARD_RE = re.compile(r"^data/\d{4}-\d{2}-\d{2}\.parquet$")
MAX_TRAIN_ROWS = int(os.getenv("KALSHI_15M_MAX_TRAIN_ROWS", "400000") or "400000")


def _ensure_dataset_repo() -> bool:
    if not HF_API_KEY:
        return False
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
        try:
            api.repo_info(repo_id=HF_KALSHI_15M_DATASET_REPO, repo_type="dataset")
        except Exception:
            api.create_repo(repo_id=HF_KALSHI_15M_DATASET_REPO, repo_type="dataset", exist_ok=True, private=False)
        return True
    except Exception as exc:
        logger.warning("[kalshi_15m_data] could not verify/create dataset repo: %s", exc)
        return False


def _is_rate_limit_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "429" in text or "rate limit" in text


def retry_on_rate_limit(fn, *, attempts: int = 3, backoff_sec: float = 5.0):
    import time
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if not _is_rate_limit_error(exc) or attempt == attempts - 1:
                raise
            time.sleep(backoff_sec * (attempt + 1))
    raise last_exc  # pragma: no cover -- attempts >= 1 always either returns or raises above


def backfill_minute_history(coins: list[str] | None = None, *, days: int = 90) -> dict[str, Any]:
    """Deep historical backfill -- the live collector
    (_run_kalshi_15m_data_collect) only ever archives what it observes
    going forward, so without this the archive load_training_dataset()/
    a future backtest reads from would otherwise grow one 5-minute cycle
    at a time from whenever collection first started, the same real gap
    alpaca_crypto_data.backfill_minute_history was built to close there.

    Fetches directly from Kalshi's own margin candlesticks API (the same
    endpoint fetch_candle_frames uses live, just a much wider window here)
    in CHUNK_HOURS-sized chunks -- not yet confirmed against a real,
    wide-range call on this account (this dev machine's own local Kalshi
    credentials are separately confirmed stale, see kalshi_15m.py's own
    module docstring), so this stays conservative (1440 one-minute
    candles per call, well under any reasonable API page-size cap) rather
    than assume a single 90-day call would succeed unchunked. Historical
    sentiment for arbitrary past dates isn't available from any free news
    API -- held at neutral (0.0) for every backfilled row, same disclosed
    limitation as alpaca_crypto_data.py's own identical backfill."""
    import time
    from collections import defaultdict

    from data import kalshi_perps

    if not HF_API_KEY:
        return {"ok": False, "reason": "no_hf_api_key"}

    target_coins = coins if coins is not None else get_universe()
    _CHUNK_HOURS = 24
    now = int(time.time())
    window_start = now - days * 86400

    by_date: dict[str, list[pd.DataFrame]] = defaultdict(list)
    coins_processed = 0
    for coin in target_coins:
        perps_ticker = COIN_TO_PERPS_TICKER.get(coin)
        if not perps_ticker:
            continue
        try:
            one_min_frames = []
            chunk_start = window_start
            while chunk_start < now:
                chunk_end = min(chunk_start + _CHUNK_HOURS * 3600, now)
                try:
                    raw = kalshi_perps.get_margin_candlesticks(
                        perps_ticker, start_ts=chunk_start, end_ts=chunk_end, period_interval=1,
                    )
                    frame = perps_data._candles_to_frame(raw.get("candlesticks") or [])  # noqa: SLF001
                    if not frame.empty:
                        one_min_frames.append(frame)
                except Exception as exc:
                    logger.warning("[kalshi_15m_data] backfill chunk fetch failed for %s [%s, %s]: %s", coin, chunk_start, chunk_end, exc)
                chunk_start = chunk_end
            if not one_min_frames:
                continue
            one_min_df = pd.concat(one_min_frames, ignore_index=True).drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
            del one_min_frames

            try:
                hourly_raw = kalshi_perps.get_margin_candlesticks(
                    perps_ticker, start_ts=window_start, end_ts=now, period_interval=60,
                )
                hourly_df = perps_data._candles_to_frame(hourly_raw.get("candlesticks") or [])  # noqa: SLF001
            except Exception as exc:
                logger.warning("[kalshi_15m_data] backfill hourly fetch failed for %s: %s", coin, exc)
                hourly_df = pd.DataFrame()

            feats = perps_data.engineer_features(one_min_df, hourly_df, sentiment_score=0.0)
            del one_min_df, hourly_df
            if feats.empty:
                continue
            feats = _relabel_for_horizon(feats)
            feats.insert(0, "symbol", coin)
            date_strs = pd.to_datetime(feats["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
            for date_str, group in feats.groupby(date_strs):
                by_date[date_str].append(group.reset_index(drop=True))
            del feats, date_strs
            coins_processed += 1
        except Exception as exc:
            logger.warning("[kalshi_15m_data] backfill failed for %s: %s", coin, exc)
        gc.collect()

    if not by_date:
        return {"ok": True, "coins_processed": coins_processed, "coins_requested": len(target_coins), "dates_written": 0}

    from huggingface_hub import HfApi, hf_hub_download
    api = HfApi(token=HF_API_KEY)
    _ensure_dataset_repo()
    dates_written = 0
    for date_str in sorted(by_date.keys()):
        groups = by_date.pop(date_str)
        combined_new = pd.concat(groups, ignore_index=True)
        del groups
        path_in_repo = f"data/{date_str}.parquet"
        try:
            existing_path = hf_hub_download(repo_id=HF_KALSHI_15M_DATASET_REPO, filename=path_in_repo, repo_type="dataset", token=HF_API_KEY)
            existing = pd.read_parquet(existing_path)
            combined = pd.concat([existing, combined_new], ignore_index=True)
            del existing
        except Exception:
            combined = combined_new
        del combined_new
        combined = combined.drop_duplicates(subset=["symbol", "ts"], keep="last").sort_values(["symbol", "ts"]).reset_index(drop=True)
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            combined.to_parquet(tmp.name, index=False)
            tmp_path = tmp.name
        try:
            retry_on_rate_limit(lambda: api.upload_file(
                path_or_fileobj=tmp_path, path_in_repo=path_in_repo,
                repo_id=HF_KALSHI_15M_DATASET_REPO, repo_type="dataset",
                commit_message=f"backfill kalshi 15m crypto minute bars: {date_str}",
            ))
            dates_written += 1
        except Exception as exc:
            logger.warning("[kalshi_15m_data] backfill upload failed for %s: %s", date_str, exc)
        finally:
            os.unlink(tmp_path)
        del combined
        gc.collect()

    return {
        "ok": True, "coins_processed": coins_processed, "coins_requested": len(target_coins),
        "dates_written": dates_written,
    }


def push_dataset_snapshot(df: pd.DataFrame) -> dict[str, Any]:
    """Merge new rows into today's parquet shard and upload it to HF --
    same merge/dedupe/atomic-write/OOM-avoidance discipline as
    perps_data.push_dataset_snapshot (see its own docstring for the full
    incident writeups this is deliberately replicating up front, rather
    than waiting to relearn each one)."""
    if df.empty:
        return {"ok": False, "reason": "no_rows"}

    shard_dir = DATA_DIR / "kalshi_15m_dataset"
    shard_dir.mkdir(parents=True, exist_ok=True)
    today = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    shard_path = shard_dir / f"{today}.parquet"

    if shard_path.exists():
        existing = pd.read_parquet(shard_path)
        combined = pd.concat([existing, df], ignore_index=True)
        del existing, df
    else:
        combined = df
    combined = combined.drop_duplicates(subset=["symbol", "ts"], keep="last").sort_values(["symbol", "ts"])
    tmp_path = shard_path.with_suffix(".parquet.tmp")
    combined.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, shard_path)
    rows_written = len(combined)
    del combined
    gc.collect()

    result: dict[str, Any] = {"ok": True, "rows_written": rows_written, "shard": str(shard_path)}
    if not _ensure_dataset_repo():
        result["hf_uploaded"] = False
        return result
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
        retry_on_rate_limit(lambda: api.upload_file(
            path_or_fileobj=str(shard_path),
            path_in_repo=f"data/{today}.parquet",
            repo_id=HF_KALSHI_15M_DATASET_REPO,
            repo_type="dataset",
            commit_message=f"kalshi 15m data {today}",
        ))
        result["hf_uploaded"] = True
    except Exception as exc:
        logger.warning("[kalshi_15m_data] HF upload failed: %s", exc)
        result["hf_uploaded"] = False
        result["hf_error"] = str(exc)
    gc.collect()
    return result


_LOAD_TRAINING_DATASET_LIST_TIMEOUT_SEC = int(os.getenv("KALSHI_15M_LOAD_TRAINING_DATASET_LIST_TIMEOUT_SEC", "45") or "45")
_LOAD_TRAINING_DATASET_SHARD_TIMEOUT_SEC = int(os.getenv("KALSHI_15M_LOAD_TRAINING_DATASET_SHARD_TIMEOUT_SEC", "25") or "25")


def load_training_dataset(*, max_shards: int = 90, max_rows: int | None = None) -> pd.DataFrame:
    """ALWAYS merges local shards with the full HF dataset archive (deduped
    on symbol+ts) -- see perps_data.load_training_dataset's own docstring
    for the full rationale (identical here) and the real incidents (hangs,
    OOM) the timeout/batched-download protections below were earned from."""
    shard_dir = DATA_DIR / "kalshi_15m_dataset"
    local_files = sorted(shard_dir.glob("*.parquet")) if shard_dir.exists() else []
    frames = []
    for f in local_files:
        try:
            frames.append(pd.read_parquet(f))
        except Exception as exc:
            logger.warning("[kalshi_15m_data] failed to read local shard %s: %s", f, exc)

    if HF_API_KEY:
        from concurrent.futures import ThreadPoolExecutor
        from concurrent.futures import TimeoutError as FutureTimeoutError
        _SHARD_DOWNLOAD_WORKERS = 8
        executor = ThreadPoolExecutor(max_workers=_SHARD_DOWNLOAD_WORKERS)

        def _download_shard(f: str) -> str | None:
            try:
                from huggingface_hub import hf_hub_download
                return hf_hub_download(repo_id=HF_KALSHI_15M_DATASET_REPO, filename=f, repo_type="dataset", token=HF_API_KEY)
            except Exception as exc:
                logger.warning("[kalshi_15m_data] failed to download HF shard %s: %s", f, exc)
                return None

        try:
            from huggingface_hub import HfApi
            api = HfApi(token=HF_API_KEY)
            raw_files = executor.submit(
                lambda: api.list_repo_files(repo_id=HF_KALSHI_15M_DATASET_REPO, repo_type="dataset"),
            ).result(timeout=_LOAD_TRAINING_DATASET_LIST_TIMEOUT_SEC)
            hf_files = [f for f in (raw_files or []) if _DATE_SHARD_RE.match(f)]
            hf_files = sorted(hf_files, reverse=True)[:max_shards]
            cap = MAX_TRAIN_ROWS if max_rows is None else max_rows
            stop_after_rows = int(cap * 1.5) if cap else None
            accumulated_rows = sum(len(fr) for fr in frames)
            for batch_start in range(0, len(hf_files), _SHARD_DOWNLOAD_WORKERS):
                if stop_after_rows and accumulated_rows >= stop_after_rows:
                    break
                batch = hf_files[batch_start:batch_start + _SHARD_DOWNLOAD_WORKERS]
                futures = {f: executor.submit(_download_shard, f) for f in batch}
                for f, future in futures.items():
                    try:
                        local_path = future.result(timeout=_LOAD_TRAINING_DATASET_SHARD_TIMEOUT_SEC)
                    except FutureTimeoutError:
                        logger.warning("[kalshi_15m_data] HF shard download %s exceeded %ss, giving up", f, _LOAD_TRAINING_DATASET_SHARD_TIMEOUT_SEC)
                        continue
                    except Exception as exc:
                        logger.warning("[kalshi_15m_data] failed to read HF shard %s: %s", f, exc)
                        continue
                    if local_path is None:
                        continue
                    try:
                        shard = pd.read_parquet(local_path)
                        if "symbol" in shard.columns and "ts" in shard.columns:
                            frames.append(shard)
                            accumulated_rows += len(shard)
                        else:
                            logger.warning("[kalshi_15m_data] skipping HF shard with unexpected schema: %s", f)
                    except Exception as exc:
                        logger.warning("[kalshi_15m_data] failed to read HF shard %s: %s", f, exc)
        except Exception as exc:
            logger.warning("[kalshi_15m_data] HF dataset listing failed: %s", exc)
        finally:
            executor.shutdown(wait=False)

    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    del frames
    if "symbol" in combined.columns and "ts" in combined.columns:
        combined = combined.drop_duplicates(subset=["symbol", "ts"])
        combined["symbol"] = combined["symbol"].astype("category")
        cap = MAX_TRAIN_ROWS if max_rows is None else max_rows
        if cap and len(combined) > cap:
            combined = combined.sort_values("ts").tail(cap).reset_index(drop=True)
    return combined
