"""Data pipeline for Kalshi's 15-minute GOLD/SILVER/COPPER/PLATINUM/PALLADIUM
markets (KXGOLD15M/KXSILVER15M/KXCOPPER15M/KXPLATINUM15M/KXPALLADIUM15M --
see kalshi_15m.py's own module docstring for the product; platinum/
palladium added once gold-api.com was confirmed live to also serve those
two spot prices at the exact same free, no-key endpoint shape). A
genuinely different data situation from
kalshi_15m_data.py's own crypto universe: Kalshi has NO perpetual-futures
contract for metals to reuse a rich candlestick feed from (crypto's own
proxy -- see that module's docstring), so this builds its OWN price
history from scratch, one point at a time.

Price source, BOTH historical and live: Yahoo Finance's public chart API,
real 1-minute OHLC for the underlying COMEX futures contract per metal
(GC=F/SI=F/HG=F/PL=F/PA=F) -- confirmed live, free, no API key. A
disclosed proxy for spot (futures track spot closely enough for a
direction classifier, never exact settlement-price parity -- same
reasoning perps' own crypto proxy already uses). Deliberately NOT Pyth's
own Hermes service (the actual settlement source these Kalshi markets
resolve against, confirmed via GET /series/{ticker}'s own
settlement_sources) -- confirmed live this session that Hermes now
requires a paid API key (HTTP 401 on /v2/updates/price/latest as of the
August 2026 Pyth Core upgrade).

Real, live-confirmed reason this ISN'T gold-api.com's own spot-price
endpoint (the original source here, used for live collection only until
this same session's own investigation): at the exact same moment,
gold-api.com's spot price and Yahoo's own futures price for gold differed
by $28.50 (0.67%) -- a real, persistent spot/futures basis, not noise.
Historical backfill (see backfill_minute_history, added the same session)
used Yahoo Finance from the start (gold-api.com's own /history endpoint
is real but needs a paid key -- confirmed live: HTTP 401, "No x-api-key
header"), so splicing gold-api.com spot (live) with Yahoo futures
(backfilled) into one training archive baked a fake, non-market price
jump into every return-based feature at that splice boundary -- a real,
concrete, evidence-backed explanation for why a 4.3x-bigger archive
didn't move walk-forward accuracy off ~50%, per explicit user direction:
"something is wrong with the data source... we need different data
source that actually works and provide data reliably." Switching live
collection to the SAME Yahoo Finance source the backfill already used
eliminates that splice artifact at the root, rather than papering over it
with a rescale. The existing (spot/futures-mixed) archive was wiped and
rebuilt from Yahoo alone rather than patched, once this fix shipped.

Two real, confirmed Yahoo Finance API constraints (never guessed, see
backfill_minute_history's own use of them): at most 8 days of 1-minute
data per request, and 1-minute data is retained for only the last 30 real
days total -- both confirmed via a real HTTP 422 from Yahoo's own API at
each boundary.

Day-to-day, this still builds its own rolling window one point per
collection cycle (see KALSHI_15M_METALS_DATA_COLLECT_MINUTES's own comment
on why that cadence is tighter than crypto's), persisted locally
(kalshi_15m_metals_price_history/{metal}.parquet) -- the backfill only
closes the STARTING gap (an archive with days of real history instead of
however long the live collector has happened to be running), it doesn't
replace the live collector's own ongoing, real-time-priced role.

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

from data.crypto_news import get_generic_sentiment
from server_common import DATA_DIR

logger = logging.getLogger(__name__)

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_KALSHI_15M_METALS_DATASET_REPO = os.getenv("HF_KALSHI_15M_METALS_DATASET_REPO", "papylove/kalshi-15m-metals-data")

# EXTERNAL_PRICE_API_TIMEOUT_SEC (not GOLD_API_TIMEOUT_SEC -- renamed once
# gold-api.com stopped being the only, or even the live-collection, price
# source here; see fetch_latest_price's own docstring for why): shared
# real-network-call timeout for every external price fetch this module
# makes (Yahoo Finance, for both backfill_minute_history and
# fetch_latest_price).
EXTERNAL_PRICE_API_TIMEOUT_SEC = int(os.getenv("EXTERNAL_PRICE_API_TIMEOUT_SEC", "10") or "10")

# Real gap closed, not a deliberate exclusion this module's own docstring
# ever actually disclosed (unlike volume/OI-derived features): live news
# sentiment for gold/silver/copper via crypto_news.get_generic_sentiment
# (the free-source-only, non-crypto-specific subset of that same
# pipeline). Plain, distinctive search terms -- "gold" alone would also
# match unrelated headlines ("gold medal", "golden"), so each query pins
# down the commodity explicitly.
METAL_TO_NEWS_QUERY = {
    "GOLD": "gold price commodity", "SILVER": "silver price commodity", "COPPER": "copper price commodity",
    "PLATINUM": "platinum price commodity", "PALLADIUM": "palladium price commodity",
}

# A second, metals-specific newsroom feed (matched per-metal the same way
# crypto_news.get_sentiment's own _match_headlines_for_coin matches
# CoinTelegraph/CryptoSlate/Decrypt per-coin) was tried here and pulled
# back out after live re-verification failed: Mining.com's feed returned
# real RSS once, then 403'd four straight times; the next candidate tried
# (FXStreet) failed the same way under repeat requests. See
# crypto_news.get_generic_sentiment's own docstring for the full evidence.
# Metals runs on the single Google News query below until a genuinely
# reliable second source turns up.
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
    "sentiment_score",
]


def get_universe() -> list[str]:
    return list(YAHOO_FUTURES_SYMBOL.keys())


# Real, free, no-key historical minute-bar source -- found and live-
# verified this session, closing the gap this module's own top docstring
# used to claim had no fix. See that docstring for the full evidence
# (gold-api.com's own /history needs a paid key; Yahoo Finance's public
# chart API doesn't) and the 2 real API constraints backfill_minute_history
# below respects.
YAHOO_FUTURES_SYMBOL = {
    "GOLD": "GC=F", "SILVER": "SI=F", "COPPER": "HG=F", "PLATINUM": "PL=F", "PALLADIUM": "PA=F",
}
_YAHOO_CHART_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
_YAHOO_MAX_1M_DAYS_PER_REQUEST = 7  # real limit confirmed live at 8 -- kept a day under as a safety margin against boundary rounding
_YAHOO_MAX_1M_LOOKBACK_DAYS = 29  # real limit confirmed live at 30 -- same safety margin


def _fetch_yahoo_1m_chunk(symbol: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    """One real HTTP call to Yahoo Finance's public chart API -- no key,
    no auth, confirmed live for all 5 of this module's own futures
    symbols. Returns a DataFrame with real "ts"/"close" columns (empty on
    any failure -- never raises, matching every other real-data fetch in
    this module)."""
    try:
        resp = requests.get(
            f"{_YAHOO_CHART_BASE_URL}/{symbol}",
            params={"interval": "1m", "period1": start_ts, "period2": end_ts},
            headers={"User-Agent": "Mozilla/5.0"}, timeout=EXTERNAL_PRICE_API_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        data = resp.json()
        result = (data.get("chart") or {}).get("result") or []
        if not result:
            return pd.DataFrame()
        ts = result[0].get("timestamp") or []
        quote = ((result[0].get("indicators") or {}).get("quote") or [{}])[0]
        closes = quote.get("close") or []
        if not ts or not closes:
            return pd.DataFrame()
        df = pd.DataFrame({"ts": ts, "close": closes}).dropna()
        df["ts"] = df["ts"].astype(int)
        return df
    except Exception as exc:
        logger.warning("[kalshi_15m_metals_data] Yahoo Finance chunk fetch failed for %s: %s", symbol, exc)
        return pd.DataFrame()


def backfill_minute_history(metals: list[str] | None = None, *, days: int = _YAHOO_MAX_1M_LOOKBACK_DAYS) -> dict[str, Any]:
    """Deep historical backfill -- the live collector
    (_run_kalshi_15m_metals_data_collect) only ever archives what it
    observes going forward, so without this the archive
    load_training_dataset() reads from would otherwise grow one collection
    cycle at a time from whenever collection first started, the same real
    gap kalshi_15m_data.backfill_minute_history/
    alpaca_crypto_data.backfill_minute_history were each built to close
    for their own markets. See this module's own top docstring for the
    real source (Yahoo Finance futures) and its 2 real, confirmed
    constraints (`days` is silently capped at _YAHOO_MAX_1M_LOOKBACK_DAYS,
    the real total-retention limit; requests are chunked at
    _YAHOO_MAX_1M_DAYS_PER_REQUEST, the real per-request limit).

    Historical news sentiment for arbitrary past dates isn't available
    from any free API -- held at neutral (0.0) for every backfilled row,
    same disclosed limitation as every sibling module's own backfill."""
    if not HF_API_KEY:
        return {"ok": False, "reason": "no_hf_api_key"}
    days = min(days, _YAHOO_MAX_1M_LOOKBACK_DAYS)

    target_metals = metals if metals is not None else get_universe()
    now = int(dt.datetime.now(dt.timezone.utc).timestamp())
    window_start = now - days * 86400

    by_date: dict[str, list[pd.DataFrame]] = {}
    metals_processed = 0
    for metal in target_metals:
        symbol = YAHOO_FUTURES_SYMBOL.get(metal)
        if not symbol:
            continue
        try:
            chunks = []
            chunk_start = window_start
            while chunk_start < now:
                chunk_end = min(chunk_start + _YAHOO_MAX_1M_DAYS_PER_REQUEST * 86400, now)
                chunk_df = _fetch_yahoo_1m_chunk(symbol, chunk_start, chunk_end)
                if not chunk_df.empty:
                    chunks.append(chunk_df)
                chunk_start = chunk_end
            if not chunks:
                continue
            price_df = pd.concat(chunks, ignore_index=True).drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
            del chunks

            feats = engineer_metals_features(price_df, sentiment_score=0.0)
            del price_df
            if feats.empty:
                continue
            feats.insert(0, "symbol", metal)
            date_strs = pd.to_datetime(feats["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
            for date_str, group in feats.groupby(date_strs):
                by_date.setdefault(date_str, []).append(group.reset_index(drop=True))
            del feats, date_strs
            metals_processed += 1
        except Exception as exc:
            logger.warning("[kalshi_15m_metals_data] backfill failed for %s: %s", metal, exc)
        gc.collect()

    if not by_date:
        return {"ok": True, "metals_processed": metals_processed, "metals_requested": len(target_metals), "dates_written": 0}

    import tempfile

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
            existing_path = hf_hub_download(repo_id=HF_KALSHI_15M_METALS_DATASET_REPO, filename=path_in_repo, repo_type="dataset", token=HF_API_KEY)
            existing = pd.read_parquet(existing_path)
            combined = pd.concat([existing, combined_new], ignore_index=True)
            del existing
        except Exception:
            combined = combined_new
        combined = combined.drop_duplicates(subset=["symbol", "ts"], keep="last").sort_values(["symbol", "ts"])
        del combined_new
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                combined.to_parquet(tmp.name, index=False)
                tmp_path = tmp.name
            del combined
            retry_on_rate_limit(lambda tp=tmp_path, pir=path_in_repo, ds=date_str: api.upload_file(
                path_or_fileobj=tp, path_in_repo=pir,
                repo_id=HF_KALSHI_15M_METALS_DATASET_REPO, repo_type="dataset",
                commit_message=f"kalshi 15m metals backfill {ds}",
            ))
            dates_written += 1
        except Exception as exc:
            logger.warning("[kalshi_15m_metals_data] backfill upload failed for %s: %s", date_str, exc)
        finally:
            if tmp_path:
                os.unlink(tmp_path)
        gc.collect()

    return {"ok": True, "metals_processed": metals_processed, "metals_requested": len(target_metals), "dates_written": dates_written}


def fetch_latest_price(metal: str) -> dict[str, Any] | None:
    """One real, current price -- {"price": float, "ts": int} or None on
    any failure (network error, unexpected symbol, malformed response).
    Never raises -- a missing point this cycle just means the rolling
    history has one fewer row, not a hard failure.

    Yahoo Finance futures (the SAME real source backfill_minute_history
    uses for historical data) -- switched from gold-api.com's own spot
    price after a real, live-confirmed finding: at the exact same moment,
    gold-api.com's spot price and Yahoo's own futures price differed by
    $28.50 on gold (0.67%) -- a real, persistent spot/futures basis, not
    noise. Splicing spot (live-collected) and futures (backfilled) data
    into one training archive baked a fake, non-market price jump into
    every return-based feature at that splice boundary -- a real, concrete
    explanation for why a 4.3x-bigger archive didn't move walk-forward
    accuracy off ~50%. One consistent real source for both historical and
    live data eliminates that artifact at the root, per explicit user
    direction after this finding: "we need different data source that
    actually works and provide data reliably.\""""
    symbol = YAHOO_FUTURES_SYMBOL.get(metal)
    if not symbol:
        return None
    try:
        resp = requests.get(
            f"{_YAHOO_CHART_BASE_URL}/{symbol}",
            params={"interval": "1m", "range": "1d"},
            headers={"User-Agent": "Mozilla/5.0"}, timeout=EXTERNAL_PRICE_API_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        data = resp.json()
        result = (data.get("chart") or {}).get("result") or []
        if not result:
            return None
        ts_list = result[0].get("timestamp") or []
        quote = ((result[0].get("indicators") or {}).get("quote") or [{}])[0]
        closes = quote.get("close") or []
        # Walk backward for the most recent REAL (non-null) close -- a
        # live symbol's own last bar is sometimes still-forming/null.
        for i in range(len(closes) - 1, -1, -1):
            if closes[i] is not None and ts_list[i] is not None:
                return {"price": float(closes[i]), "ts": int(ts_list[i])}
        return None
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


def engineer_metals_features(price_df: pd.DataFrame, sentiment_score: float = 0.0) -> pd.DataFrame:
    """Leaner sibling of perps_data.engineer_features -- see this
    module's own docstring for exactly which indicators are dropped and
    why (all need volume/OI/bid-ask/high-low, none of which a plain spot
    price has). Every formula below for a feature this DOES compute is
    identical to perps_data.engineer_features' own -- same leakage-free,
    backward-looking-only discipline.

    sentiment_score: same convention as perps_data.engineer_features'
    own identically-named parameter -- ONE scalar for the whole call,
    broadcast across every row, since it reflects sentiment as of NOW
    (when this function runs), not a historical time series. Real,
    live-fetched news sentiment for the live collection/prediction path
    (see collect_dataset_rows/latest_feature_row); the historical
    backfill path this market has none of yet (no free API exists) would
    pass 0.0, same disclosed-limitation convention as every sibling
    module's own backfill."""
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
    df["sentiment_score"] = float(sentiment_score)

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
            sentiment = get_generic_sentiment(METAL_TO_NEWS_QUERY[metal], cache_key=metal)
            feats = engineer_metals_features(history, sentiment_score=sentiment["sentiment_score"])
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
    prediction always reflects the same data the archived dataset does.
    DOES fetch a fresh sentiment reading, though (cheap, cached -- see
    get_generic_sentiment's own TTL), matching kalshi_15m_data.latest_
    feature_row's own identical convention: a live PREDICTION should
    reflect sentiment as of right now, not whatever it was at the last
    collection tick."""
    history = _load_price_history(metal)
    sentiment = get_generic_sentiment(METAL_TO_NEWS_QUERY[metal], cache_key=metal)
    feats_all = engineer_metals_features(history, sentiment_score=sentiment["sentiment_score"])
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


def _seed_local_shard_from_hf(path_in_repo: str, local_path) -> None:
    """See kalshi_15m_data._seed_local_shard_from_hf: prevents the first
    push after a restart from overwriting the day's earlier HF data."""
    if local_path.exists() or not HF_API_KEY:
        return

    def _download() -> str | None:
        from huggingface_hub import hf_hub_download
        try:
            return hf_hub_download(repo_id=HF_KALSHI_15M_METALS_DATASET_REPO, filename=path_in_repo, repo_type="dataset", token=HF_API_KEY)
        except Exception:
            return None

    from server_common import call_with_hard_timeout
    remote = call_with_hard_timeout(_download, timeout_sec=30)
    if remote:
        import shutil
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(remote, local_path)


def push_dataset_snapshot(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"ok": False, "reason": "no_rows"}

    shard_dir = DATA_DIR / "kalshi_15m_metals_dataset"
    shard_dir.mkdir(parents=True, exist_ok=True)
    today = pd.Timestamp.now("UTC").strftime("%Y-%m-%d")
    shard_path = shard_dir / f"{today}.parquet"
    _seed_local_shard_from_hf(f"data/{today}.parquet", shard_path)

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
