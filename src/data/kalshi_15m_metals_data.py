"""Data pipeline for Kalshi's 15-minute GOLD/SILVER/COPPER markets
(KXGOLD15M/KXSILVER15M/KXCOPPER15M -- see kalshi_15m.py's own module
docstring for the product). A genuinely different data situation from
kalshi_15m_data.py's own crypto universe: Kalshi has NO perpetual-futures
contract for metals to reuse a rich candlestick feed from (crypto's own
proxy -- see that module's docstring), so this builds its OWN price
history from scratch, one point at a time.

Price source: api.gold-api.com's plain spot-price endpoint
(GET /price/{symbol}, symbols XAU/XAG/HG) -- confirmed live, genuinely
free, no API key at all. Deliberately NOT Pyth's own Hermes service
(the actual settlement source these Kalshi markets resolve against,
confirmed via GET /series/{ticker}'s own settlement_sources) -- confirmed
live this session that Hermes now requires a paid API key
(HTTP 401 on /v2/updates/price/latest as of the August 2026 Pyth Core
upgrade) that would add a new credential dependency the user would need
to separately obtain; gold-api.com's own values track spot gold/silver/
copper closely enough for a DIRECTION classifier (never claims exact
settlement-price parity the way perps' proxy reasoning does for crypto).

No historical bars exist anywhere free for this -- unlike every other
market here, this module can't backfill a single day of history on day
one. It builds its own rolling window one point per collection cycle
(see KALSHI_15M_METALS_DATA_COLLECT_MINUTES's own comment on why that
cadence is tighter than crypto's), persisted locally (kalshi_15m_metals_price_history/
{metal}.parquet).

That local file USED TO be the only copy, on the reasoning that it's a
rolling feature-computation window, not the durable training record (the
labeled, feature-engineered rows this module produces each cycle ARE
archived to HF via push_dataset_snapshot, same as every sibling module).
Real gap that reasoning missed, confirmed live: MIN_ROWS_FOR_FEATURES
(245, ~4 hours one point/minute) means this whole pipeline can't produce
its FIRST feature row until the raw window survives that long
uninterrupted -- and this process restarts often enough (redeploys, the
recurring dead-HF-key fix cycle) that it went days without ever once
reaching 245 minutes, so push_dataset_snapshot never got anything to
archive in the first place. The raw window is now ALSO backed up to HF
(see _maybe_push_price_history_to_hf), rate-limited to once every
PRICE_HISTORY_HF_PUSH_MINUTES rather than every single collection tick,
and restored from there on a cold local start -- bounding real data loss
on a restart to that interval instead of a full reset to zero.

Leaner FEATURE_COLUMNS than crypto's (see METALS_FEATURE_COLUMNS below)
by real necessity, not oversight: a plain spot price has no volume, open
interest, bid/ask, or high/low -- so no volume_ratio/dollar_volume_z/
oi_change_pct/spread_pct/atr_pct/stoch_k (all of which need one of
those). Every feature that only needs a close-price series (returns,
moving-average distance, volatility, RSI, MACD, Bollinger, time-of-day/
day-of-week) is computed identically to perps_data.engineer_features'
own formulas.
"""
from __future__ import annotations

import datetime as dt
import gc
import logging
import os
import re
from typing import Any

import numpy as np
import pandas as pd
import requests

from server_common import DATA_DIR

logger = logging.getLogger(__name__)

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_KALSHI_15M_METALS_DATASET_REPO = os.getenv("HF_KALSHI_15M_METALS_DATASET_REPO", "papylove/kalshi-15m-metals-data")

GOLD_API_BASE_URL = os.getenv("GOLD_API_BASE_URL", "https://api.gold-api.com").rstrip("/")
GOLD_API_TIMEOUT_SEC = int(os.getenv("GOLD_API_TIMEOUT_SEC", "10") or "10")

METAL_TO_SYMBOL = {"GOLD": "XAU", "SILVER": "XAG", "COPPER": "HG"}

LABEL_HORIZON_MINUTES = int(os.getenv("KALSHI_15M_METALS_LABEL_HORIZON_MINUTES", "15") or "15")

# A bounded rolling window, not the full ever-growing history -- this file
# exists purely to give engineer_features enough backward-looking rows to
# compute its longest lookback (4h/240min) after a restart; the durable,
# ever-growing TRAINING record lives on HF instead (see this module's own
# docstring). 24h at 1-point-per-collection-cycle is generous headroom
# over the 245-row minimum with room to spare even if a cycle is missed.
PRICE_HISTORY_MAX_ROWS = int(os.getenv("KALSHI_15M_METALS_PRICE_HISTORY_MAX_ROWS", "1440") or "1440")
MIN_ROWS_FOR_FEATURES = 245  # matches perps_data.MIN_ONE_MIN_ROWS_FOR_FEATURES -- the 4h/240min lookback + buffer

# How often the raw rolling price-history window itself gets backed up to
# HF (see this module's own docstring for why this exists at all) -- once
# every N minutes per metal, not every single collection tick. 3 metals x
# 1 upload/N-minutes is a small, deliberate HF write rate; N=15 bounds
# real data loss on a restart to at most 15 of the 245 minutes needed to
# ever produce a feature row, while keeping this well under the ~1
# upload/second range that would risk HF rate limits across all 3 metals
# combined with everything else this process already writes to HF.
PRICE_HISTORY_HF_PUSH_MINUTES = int(os.getenv("KALSHI_15M_METALS_PRICE_HISTORY_HF_PUSH_MINUTES", "15") or "15")
_last_price_history_push_at: dict[str, float] = {}

METALS_FEATURE_COLUMNS = [
    "ret_1m", "ret_3m", "ret_5m", "ret_10m", "ret_15m", "ret_30m",
    "trend_1h", "trend_2h", "trend_3h", "trend_4h",
    "dist_to_ma_15", "dist_to_ma_30",
    "volatility_5", "volatility_15", "volatility_30",
    "rsi_14", "macd_hist_pct", "bb_pct_b", "bb_bandwidth",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
]


def get_universe() -> list[str]:
    return list(METAL_TO_SYMBOL.keys())


def fetch_latest_price(metal: str) -> dict[str, Any] | None:
    """One real, current spot price -- {"price": float, "ts": int} or None
    on any failure (network error, unexpected symbol, malformed response).
    Never raises -- a missing point this cycle just means the rolling
    history has one fewer row, not a hard failure."""
    symbol = METAL_TO_SYMBOL.get(metal)
    if not symbol:
        return None
    try:
        resp = requests.get(f"{GOLD_API_BASE_URL}/price/{symbol}", timeout=GOLD_API_TIMEOUT_SEC)
        resp.raise_for_status()
        data = resp.json()
        price = float(data["price"])
        updated_at = data.get("updatedAt")
        ts = int(dt.datetime.fromisoformat(str(updated_at).replace("Z", "+00:00")).timestamp()) if updated_at else int(dt.datetime.now(dt.timezone.utc).timestamp())
        return {"price": price, "ts": ts}
    except Exception as exc:
        logger.warning("[kalshi_15m_metals_data] price fetch failed for %s: %s", metal, exc)
        return None


def _price_history_path(metal: str):
    return DATA_DIR / "kalshi_15m_metals_price_history" / f"{metal}.parquet"


def _price_history_hf_path_in_repo(metal: str) -> str:
    return f"price_history/{metal}.parquet"


def _restore_price_history_from_hf(metal: str) -> pd.DataFrame | None:
    """Cold-local-start recovery -- see this module's own docstring. Best
    effort: no HF key, no repo yet, or no prior backup all just mean
    "nothing to restore", not an error."""
    if not HF_API_KEY:
        return None
    try:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(
            repo_id=HF_KALSHI_15M_METALS_DATASET_REPO, filename=_price_history_hf_path_in_repo(metal),
            repo_type="dataset", token=HF_API_KEY,
        )
        return pd.read_parquet(path)
    except Exception as exc:
        logger.info("[kalshi_15m_metals_data] no HF price-history backup to restore for %s: %s", metal, exc)
        return None


def _load_price_history(metal: str) -> pd.DataFrame:
    path = _price_history_path(metal)
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception as exc:
            logger.warning("[kalshi_15m_metals_data] failed to read local price history for %s: %s", metal, exc)
    # Local file missing (a fresh container, most likely) -- see if HF has
    # a recent backup before falling back to a genuinely empty history.
    restored = _restore_price_history_from_hf(metal)
    if restored is not None:
        try:
            _save_price_history(metal, restored, push_to_hf=False)  # noqa: E501 -- write straight back to local disk, no need to re-push what we just pulled
        except Exception as exc:
            logger.warning("[kalshi_15m_metals_data] failed to write restored price history to local disk for %s: %s", metal, exc)
        return restored
    return pd.DataFrame(columns=["ts", "close"])


def _maybe_push_price_history_to_hf(metal: str, df: pd.DataFrame) -> None:
    """Rate-limited per PRICE_HISTORY_HF_PUSH_MINUTES -- see this module's
    own docstring and PRICE_HISTORY_HF_PUSH_MINUTES's own comment for why
    this isn't pushed on every single collection tick. Best effort: a
    failure here never blocks the caller (the local save already
    succeeded by the time this runs)."""
    if not HF_API_KEY or df.empty:
        return
    import time
    now = time.time()
    last = _last_price_history_push_at.get(metal, 0.0)
    if (now - last) < PRICE_HISTORY_HF_PUSH_MINUTES * 60:
        return
    try:
        from huggingface_hub import HfApi
        if not _ensure_dataset_repo():
            return
        api = HfApi(token=HF_API_KEY)
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            df.to_parquet(tmp.name, index=False)
            tmp_path = tmp.name
        try:
            retry_on_rate_limit(lambda: api.upload_file(
                path_or_fileobj=tmp_path, path_in_repo=_price_history_hf_path_in_repo(metal),
                repo_id=HF_KALSHI_15M_METALS_DATASET_REPO, repo_type="dataset",
                commit_message=f"backup {metal} 15m metals raw price-history window",
            ))
            _last_price_history_push_at[metal] = now
        finally:
            os.unlink(tmp_path)
    except Exception as exc:
        logger.warning("[kalshi_15m_metals_data] price-history HF backup failed for %s: %s", metal, exc)


def _save_price_history(metal: str, df: pd.DataFrame, *, push_to_hf: bool = True) -> None:
    path = _price_history_path(metal)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, path)
    if push_to_hf:
        _maybe_push_price_history_to_hf(metal, df)


def _append_price_point(metal: str, ts: int, price: float) -> pd.DataFrame:
    """Appends one point, dedupes on ts (keep the newest value for a
    given minute, matching every sibling *_data.py's own drop_duplicates
    convention), rounds each ts down to the minute (this endpoint's own
    "now" can land anywhere within a minute; the model's OWN 1-minute-bar
    convention needs a consistent grid), trims to PRICE_HISTORY_MAX_ROWS,
    persists, and returns the resulting frame."""
    minute_ts = (int(ts) // 60) * 60
    history = _load_price_history(metal)
    new_row = pd.DataFrame({"ts": [minute_ts], "close": [float(price)]})
    combined = pd.concat([history, new_row], ignore_index=True)
    combined = combined.drop_duplicates(subset=["ts"], keep="last").sort_values("ts")
    if len(combined) > PRICE_HISTORY_MAX_ROWS:
        combined = combined.tail(PRICE_HISTORY_MAX_ROWS).reset_index(drop=True)
    _save_price_history(metal, combined)
    return combined


def engineer_metals_features(price_df: pd.DataFrame) -> pd.DataFrame:
    """Leaner sibling of perps_data.engineer_features -- see this
    module's own docstring for exactly which indicators are dropped and
    why (all need volume/OI/bid-ask/high-low, none of which a plain spot
    price has). Every formula below for a feature this DOES compute is
    identical to perps_data.engineer_features' own -- same leakage-free,
    backward-looking-only discipline."""
    if price_df.empty or len(price_df) < MIN_ROWS_FOR_FEATURES:
        return pd.DataFrame()

    df = price_df.sort_values("ts").reset_index(drop=True).copy()
    df["ret_1m"] = df["close"].pct_change(1)
    df["ret_3m"] = df["close"].pct_change(3)
    df["ret_5m"] = df["close"].pct_change(5)
    df["ret_10m"] = df["close"].pct_change(10)
    df["ret_15m"] = df["close"].pct_change(15)
    df["ret_30m"] = df["close"].pct_change(30)
    df["trend_1h"] = df["close"].pct_change(60)
    df["trend_2h"] = df["close"].pct_change(120)
    df["trend_3h"] = df["close"].pct_change(180)
    df["trend_4h"] = df["close"].pct_change(240)
    df["ma_15"] = df["close"].rolling(15).mean()
    df["ma_30"] = df["close"].rolling(30).mean()
    df["dist_to_ma_15"] = (df["close"] - df["ma_15"]) / df["ma_15"]
    df["dist_to_ma_30"] = (df["close"] - df["ma_30"]) / df["ma_30"]
    df["volatility_5"] = df["ret_1m"].rolling(5).std()
    df["volatility_15"] = df["ret_1m"].rolling(15).std()
    df["volatility_30"] = df["ret_1m"].rolling(30).std()

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

    ts_utc = pd.to_datetime(df["ts"], unit="s", utc=True)
    hour_frac = ts_utc.dt.hour + ts_utc.dt.minute / 60.0
    df["hour_sin"] = np.sin(2 * np.pi * hour_frac / 24.0)
    df["hour_cos"] = np.cos(2 * np.pi * hour_frac / 24.0)
    dow = ts_utc.dt.dayofweek
    df["dow_sin"] = np.sin(2 * np.pi * dow / 7.0)
    df["dow_cos"] = np.cos(2 * np.pi * dow / 7.0)

    horizon = LABEL_HORIZON_MINUTES
    df["future_close"] = df["close"].shift(-horizon)
    df["label_up"] = (df["future_close"] > df["close"]).astype("Int64")
    df.loc[df["future_close"].isna(), "label_up"] = pd.NA

    return df.dropna(subset=METALS_FEATURE_COLUMNS).reset_index(drop=True)


def collect_dataset_rows(metals: list[str] | None = None) -> pd.DataFrame:
    """Fetches one fresh price point per metal, appends it to that
    metal's own persisted rolling history, re-engineers features over the
    resulting window, and returns the combined, symbol-tagged frame --
    same shape/contract as every sibling *_data.py's own
    collect_dataset_rows."""
    target_metals = metals if metals is not None else get_universe()
    frames = []
    for metal in target_metals:
        try:
            point = fetch_latest_price(metal)
            if point is None:
                continue
            history = _append_price_point(metal, point["ts"], point["price"])
            feats = engineer_metals_features(history)
            if feats.empty:
                continue
            feats.insert(0, "symbol", metal)
            frames.append(feats)
        except Exception as exc:
            logger.warning("[kalshi_15m_metals_data] collect failed for %s: %s", metal, exc)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def latest_feature_row(metal: str) -> dict[str, Any] | None:
    """The single most-recent feature row for one metal, for live
    prediction -- does NOT fetch a new price point itself (collect_dataset_rows,
    on its own schedule, is the only writer of the rolling history) so a
    prediction always reflects the same data the archived dataset does."""
    history = _load_price_history(metal)
    feats_all = engineer_metals_features(history)
    if feats_all.empty:
        return None
    # engineer_metals_features already dropped the tail rows whose label
    # is NaN (dropna(subset=METALS_FEATURE_COLUMNS) doesn't touch label_up,
    # but a too-short history drops out entirely) -- the actual most
    # recent row is the true live-prediction point regardless of its own
    # (unknowable-yet) label.
    last = feats_all.iloc[-1]
    row = {col: float(last[col]) for col in METALS_FEATURE_COLUMNS}
    row["symbol"] = metal
    row["current_price"] = float(last["close"])
    return row


# ---------------------------------------------------------------------------
# HF archival -- see kalshi_15m_data.py's own identical section for the
# full incident-history rationale behind every timeout/atomic-write choice.
# ---------------------------------------------------------------------------
_DATE_SHARD_RE = re.compile(r"^data/\d{4}-\d{2}-\d{2}\.parquet$")
MAX_TRAIN_ROWS = int(os.getenv("KALSHI_15M_METALS_MAX_TRAIN_ROWS", "400000") or "400000")


def _ensure_dataset_repo() -> bool:
    if not HF_API_KEY:
        return False
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
        try:
            api.repo_info(repo_id=HF_KALSHI_15M_METALS_DATASET_REPO, repo_type="dataset")
        except Exception:
            api.create_repo(repo_id=HF_KALSHI_15M_METALS_DATASET_REPO, repo_type="dataset", exist_ok=True, private=False)
        return True
    except Exception as exc:
        logger.warning("[kalshi_15m_metals_data] could not verify/create dataset repo: %s", exc)
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
    raise last_exc  # pragma: no cover


def push_dataset_snapshot(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"ok": False, "reason": "no_rows"}

    shard_dir = DATA_DIR / "kalshi_15m_metals_dataset"
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
            repo_id=HF_KALSHI_15M_METALS_DATASET_REPO,
            repo_type="dataset",
            commit_message=f"kalshi 15m metals data {today}",
        ))
        result["hf_uploaded"] = True
    except Exception as exc:
        logger.warning("[kalshi_15m_metals_data] HF upload failed: %s", exc)
        result["hf_uploaded"] = False
        result["hf_error"] = str(exc)
    gc.collect()
    return result


_LOAD_TRAINING_DATASET_LIST_TIMEOUT_SEC = int(os.getenv("KALSHI_15M_METALS_LOAD_TRAINING_DATASET_LIST_TIMEOUT_SEC", "45") or "45")
_LOAD_TRAINING_DATASET_SHARD_TIMEOUT_SEC = int(os.getenv("KALSHI_15M_METALS_LOAD_TRAINING_DATASET_SHARD_TIMEOUT_SEC", "25") or "25")


def load_training_dataset(*, max_shards: int = 90, max_rows: int | None = None) -> pd.DataFrame:
    shard_dir = DATA_DIR / "kalshi_15m_metals_dataset"
    local_files = sorted(shard_dir.glob("*.parquet")) if shard_dir.exists() else []
    frames = []
    for f in local_files:
        try:
            frames.append(pd.read_parquet(f))
        except Exception as exc:
            logger.warning("[kalshi_15m_metals_data] failed to read local shard %s: %s", f, exc)

    if HF_API_KEY:
        from concurrent.futures import ThreadPoolExecutor
        from concurrent.futures import TimeoutError as FutureTimeoutError
        _SHARD_DOWNLOAD_WORKERS = 8
        executor = ThreadPoolExecutor(max_workers=_SHARD_DOWNLOAD_WORKERS)

        def _download_shard(f: str) -> str | None:
            try:
                from huggingface_hub import hf_hub_download
                return hf_hub_download(repo_id=HF_KALSHI_15M_METALS_DATASET_REPO, filename=f, repo_type="dataset", token=HF_API_KEY)
            except Exception as exc:
                logger.warning("[kalshi_15m_metals_data] failed to download HF shard %s: %s", f, exc)
                return None

        try:
            from huggingface_hub import HfApi
            api = HfApi(token=HF_API_KEY)
            raw_files = executor.submit(
                lambda: api.list_repo_files(repo_id=HF_KALSHI_15M_METALS_DATASET_REPO, repo_type="dataset"),
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
                        logger.warning("[kalshi_15m_metals_data] HF shard download %s exceeded %ss, giving up", f, _LOAD_TRAINING_DATASET_SHARD_TIMEOUT_SEC)
                        continue
                    except Exception as exc:
                        logger.warning("[kalshi_15m_metals_data] failed to read HF shard %s: %s", f, exc)
                        continue
                    if local_path is None:
                        continue
                    try:
                        shard = pd.read_parquet(local_path)
                        if "symbol" in shard.columns and "ts" in shard.columns:
                            frames.append(shard)
                            accumulated_rows += len(shard)
                        else:
                            logger.warning("[kalshi_15m_metals_data] skipping HF shard with unexpected schema: %s", f)
                    except Exception as exc:
                        logger.warning("[kalshi_15m_metals_data] failed to read HF shard %s: %s", f, exc)
        except Exception as exc:
            logger.warning("[kalshi_15m_metals_data] HF dataset listing failed: %s", exc)
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
