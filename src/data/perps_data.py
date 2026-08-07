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

import gc
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

import numpy as np
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
# Lowered (72 -> 8, 720 -> 24) after a real production incident: confirmed
# live via Render's own event log that _run_perps_data_collect was OOM-
# killing the container on almost exactly its own 15-minute schedule, for
# 13+ hours straight -- a genuine 15-minute-interval crash loop, not a rare
# one-off. engineer_features' longest ACTUAL rolling window is trend_4h
# (pct_change(240), 4 hours of 1-min data) and trend_pct's 6-period
# pct_change on hourly data (6 hours) -- the old 72h/720h(30-day) windows
# were 9x/30x bigger than anything a single feature computation needs.
# push_dataset_snapshot()'s merge/dedup against the existing archive means
# a shorter per-cycle fetch loses nothing: only the newest rows since the
# last cycle are actually new, everything older just gets deduped away
# after costing memory/CPU to re-fetch and re-engineer for no benefit.
# Raised back up slightly (8 -> 20) after the OOM fix went live and
# revealed a real side effect: confirmed empirically that Kalshi's own
# candlestick response has genuine gaps for quieter/lower-liquidity perp
# instruments (e.g. KXLINKPERP returned only 139 usable 1-min rows in an
# 8h window, well under MIN_ONE_MIN_ROWS_FOR_FEATURES=245), silently
# dropping 5 of 13 active tickers from the archive entirely every cycle.
# 20h gives every tested quiet ticker a real margin above that floor
# (KXLINKPERP: 315 rows) while still being ~3.6x smaller than the original
# 72h -- re-profiled at this setting and confirmed peak memory barely
# moved (246MB vs 236MB at 8h), nowhere near the level that caused the
# original crash loop.
CANDLE_1M_LOOKBACK_HOURS = int(os.getenv("PERPS_CANDLE_1M_LOOKBACK_HOURS", "20") or "20")
CANDLE_60M_LOOKBACK_HOURS = int(os.getenv("PERPS_CANDLE_60M_LOOKBACK_HOURS", "24") or "24")
# 1 minute (not 30) -- matches how this strategy actually holds a position:
# take-profit/quick-profit/stop-loss are all evaluated every
# PERPS_FAST_CHECK_SECONDS (20s), so a model trained to predict "up or down
# 30 minutes from now" was answering a materially different question than
# the one the strategy actually needs ("what happens in the next
# candle-or-two"). A 1-minute-ahead label is a much closer match.
LABEL_HORIZON_MINUTES = int(os.getenv("PERPS_LABEL_HORIZON_MINUTES", "1") or "1")

# The HF archive grows every day forever; without a cap, load_training_dataset()
# would eventually load an unbounded number of rows into memory on every train
# run. Render's Starter plan caps the whole process at 512MB, and this
# dataset's ticker column (kept as Python string objects until capped/typed
# below) is the single biggest memory cost of loading it -- bounding row
# count bounds memory regardless of how much history accumulates over time.
#
# Confirmed live: 150,000 was sized for the ORIGINAL ~22-column feature set.
# Adding the volume/open-interest/spread/time-of-day features (this session)
# grew that to ~31 columns (~1.4x) with no matching cap adjustment, and a
# real production retrain then OOM-killed the service outright ("Ran out of
# memory (used over 512MB)"). Lowered well below the naive 150000/1.4≈106000
# parity point for real headroom, not just a wash -- train_model() also
# briefly holds the train/test split arrays AND a final full-data refit
# simultaneously, so the actual peak is higher than the row count alone
# suggests.
# Lowered AGAIN (80000 -> 40000) after that 80000 cap still wasn't enough:
# confirmed live via Render's own event log, a second real OOM (this time a
# container-level kill, "oomKilled: memoryLimit 512Mi") plus a separate
# worker SIGKILL specifically during the daily cron retrain. Isolated local
# profiling at the 80000 cap (real archived data, only 67941 rows currently
# available) still peaked at ~341MB in a FRESH process with nothing else
# loaded -- on the real server, train_model() runs inside the SAME
# long-lived worker that also holds a loaded live model, per-ticker candle
# caches, and every imported library's own baseline footprint, so the true
# peak there is higher than this isolated number. This is now paired with
# freeing `frame` before the final refit (see train_model()) and an atomic
# local-shard write (see push_dataset_snapshot()) so a crash mid-write can
# no longer corrupt that day's archive.
MAX_TRAIN_ROWS = int(os.getenv("PERPS_MAX_TRAIN_ROWS", "40000") or "40000")

_TICKER_TO_COIN = {
    "KXBTCPERP": "BTC", "KXETHPERP": "ETH", "KXSOLPERP": "SOL", "KXXRPPERP": "XRP",
    "KXDOGEPERP": "DOGE", "KXLTCPERP": "LTC", "KXBCHPERP": "BCH", "KXLINKPERP": "LINK",
    "KXSUIPERP": "SUI", "KXNEARPERP": "NEAR", "KXDOTPERP": "DOT", "KXHBARPERP": "HBAR",
    "KXHYPEPERP": "HYPE", "KXKSHIBPERP": "KSHIB", "KXXLMPERP": "XLM", "KXZECPERP": "ZEC",
}


def coin_for_ticker(ticker: str) -> str:
    return _TICKER_TO_COIN.get(ticker, ticker.replace("KX", "").replace("PERP", ""))


# Confirmed live: 3 of Kalshi's 16 listed perp instruments (DOT, HBAR, XLM)
# are status="inactive" with zero volume/open interest -- every scan cycle
# was wasting API calls and getting no usable data back from them.
#
# Among the 13 ACTIVE instruments, volume and volatility are almost
# INVERSELY correlated: BTC/ETH carry by far the most 24h volume but are
# the LEAST volatile of the set, while ZEC/LINK/NEAR/kSHIB/LTC have much
# less volume but are meaningfully more volatile. A single-metric filter
# misses real opportunity either way, so both get ranked and combined.
WATCHLIST_TOP_N = int(os.getenv("PERPS_WATCHLIST_TOP_N", "8") or "8")
_TICKER_ACTIVITY_CACHE: dict[str, Any] = {"volatility_by_ticker": None, "computed_at": 0.0}
_TICKER_ACTIVITY_CACHE_TTL_SEC = int(os.getenv("PERPS_WATCHLIST_REFRESH_SEC", str(6 * 3600)) or str(6 * 3600))


def _recent_volatility_by_ticker() -> dict[str, float]:
    """Average volatility_15 per ticker over a SMALL recent slice of the
    archive -- a handful of shards/rows is plenty to characterize each
    ticker's current volatility, and deliberately far below
    load_training_dataset()'s training-grade default (90 shards / up to
    MAX_TRAIN_ROWS rows). Confirmed live: calling that full-size default
    from here caused a fresh Render OOM, because it could run concurrently
    with the startup training thread's OWN full-size load of the same
    archive -- the single heaviest operation this process does, doubled,
    right at the moment memory is already tightest."""
    try:
        df = load_training_dataset(max_shards=5, max_rows=5000)
    except Exception as exc:
        logger.warning("[perps_data] could not load archive for volatility ranking: %s", exc)
        return {}
    if df.empty or "volatility_15" not in df.columns:
        return {}
    return df.groupby("ticker")["volatility_15"].mean().to_dict()


def refresh_ticker_activity_cache(*, force: bool = False) -> None:
    """Recomputes the cached per-ticker volatility used by watchlist
    ranking. Called ONLY from the periodic perps_data_collect job (off the
    request path) -- never from get_watchlist()/_rank_tickers_by_volume_
    and_volatility() directly, since those run inline inside /api/status on
    every dashboard poll, and this needs an HF archive download. Skips the
    refresh unless the cache is empty, forced, or past its TTL."""
    now = time.time()
    if (
        not force
        and _TICKER_ACTIVITY_CACHE["volatility_by_ticker"] is not None
        and (now - _TICKER_ACTIVITY_CACHE["computed_at"]) < _TICKER_ACTIVITY_CACHE_TTL_SEC
    ):
        return
    _TICKER_ACTIVITY_CACHE["volatility_by_ticker"] = _recent_volatility_by_ticker()
    _TICKER_ACTIVITY_CACHE["computed_at"] = now


def _rank_tickers_by_volume_and_volatility(markets: list[dict[str, Any]]) -> list[str]:
    """Active tickers only, ranked by combining a live-volume rank (free --
    already in the same market listing response) with a periodically
    refreshed volatility rank (see refresh_ticker_activity_cache). Reads
    whatever is cached ONLY -- never triggers an archive load itself, so
    calling this (via get_watchlist()) from a request handler is always
    cheap. Before the cache is first populated (e.g. right after a cold
    start), this falls back to volume-only ranking. Lower combined rank =
    more active AND more volatile; returned best-first."""
    volatility_by_ticker = _TICKER_ACTIVITY_CACHE["volatility_by_ticker"] or {}

    active = [m for m in markets if m.get("status") == "active" and m.get("ticker")]
    if not active:
        return []

    by_volume = sorted(active, key=lambda m: -float(m.get("volume_24h_notional_value_dollars") or 0.0))
    volume_rank = {m["ticker"]: i for i, m in enumerate(by_volume)}

    by_volatility = sorted(active, key=lambda m: -volatility_by_ticker.get(m["ticker"], 0.0))
    volatility_rank = {m["ticker"]: i for i, m in enumerate(by_volatility)}

    fallback_rank = len(active)
    return sorted(
        (m["ticker"] for m in active),
        key=lambda t: volume_rank.get(t, fallback_rank) + volatility_rank.get(t, fallback_rank),
    )


# Real gap found in review: get_watchlist() calls this on EVERY /api/status
# request with zero caching -- a live, authenticated Kalshi API call
# fetching every listed perp instrument's full detail. Confirmed live: a
# single external consumer (a third-party "polymarket-paper-lab" client)
# was polling /api/status roughly once every 20 seconds, meaning this
# single-threaded (--workers 1 --threads 1) process was re-fetching and
# re-parsing this same payload ~180 times/hour just to answer status
# checks, layered on top of the SAME live call from fast_check/entry_scan's
# own trading decisions -- real, continuous CPU/memory churn contributing
# to a recurring OOM crash loop that started showing up around this same
# window. Markets don't meaningfully change within seconds, so a short TTL
# cache (matching app_kalshi.py's own _cached_account_snapshot pattern)
# keeps every caller within the TTL window free.
_MARGIN_MARKETS_CACHE: dict[str, Any] = {"markets": None, "computed_at": 0.0}
_MARGIN_MARKETS_CACHE_TTL_SEC = int(os.getenv("PERPS_MARGIN_MARKETS_CACHE_TTL_SEC", "30") or "30")


def _cached_list_margin_markets() -> list[dict[str, Any]]:
    now = time.time()
    if _MARGIN_MARKETS_CACHE["markets"] is not None and (now - _MARGIN_MARKETS_CACHE["computed_at"]) < _MARGIN_MARKETS_CACHE_TTL_SEC:
        return _MARGIN_MARKETS_CACHE["markets"]
    markets = list_margin_markets()
    _MARGIN_MARKETS_CACHE["markets"] = markets
    _MARGIN_MARKETS_CACHE["computed_at"] = now
    return markets


def get_active_tickers() -> list[str]:
    """EVERY active (non-inactive) perp instrument, unnarrowed -- used for
    data collection specifically, which must keep archiving history for
    every coin, not just today's top WATCHLIST_TOP_N. get_watchlist()'s
    own volatility ranking depends on this same archive: if collection
    only ever fed the narrowed list back into itself, a coin left out
    today could never accumulate the data needed to prove it deserves a
    spot later -- a self-reinforcing bias where only whatever ranks well
    RIGHT NOW ever gets considered again."""
    try:
        markets = _cached_list_margin_markets()
        active = sorted(m["ticker"] for m in markets if m.get("status") == "active" and m.get("ticker"))
        if active:
            return active
    except Exception as exc:
        logger.warning("[perps_data] list_margin_markets failed, using known list: %s", exc)
    return list(KNOWN_PERP_TICKERS)


def get_watchlist() -> list[str]:
    """The most active AND most volatile perp instruments Kalshi currently
    lists, live -- falls back to the known snapshot if the listing call
    fails (e.g. transient network issue). Excludes inactive-status
    instruments entirely, and narrows down to the top WATCHLIST_TOP_N
    (default 8) by combined volume+volatility rank rather than watching
    every active instrument regardless of how quiet or illiquid it is."""
    try:
        markets = _cached_list_margin_markets()
        ranked = _rank_tickers_by_volume_and_volatility(markets)
        if ranked:
            if WATCHLIST_TOP_N > 0:
                ranked = ranked[:WATCHLIST_TOP_N]
            return sorted(ranked)
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
        # high/low (confirmed live in every candle's own "price" sub-object,
        # alongside close) enable real ATR/Stochastic indicators below rather
        # than a close-only approximation -- falls back to close itself if
        # either is ever missing so a partial/malformed candle can't produce
        # a nonsensical (e.g. negative) range.
        high = price.get("high")
        low = price.get("low")
        # "open" is read the same defensive way -- if this exact API
        # response ever omits it, _fill_missing_open below approximates it
        # from the previous row's close (standard when only a close-chain
        # is available) rather than defaulting to close-of-self, which
        # would silently render every candlestick as a flat doji.
        open_ = price.get("open")
        # Confirmed live: every candle Kalshi returns ALSO carries real
        # volume, dollar volume, open interest, and a bid/ask sub-object --
        # previously fetched and immediately thrown away. These enable real
        # volume-spike and open-interest-based features below (see
        # engineer_features) instead of only ever looking at price.
        bid = c.get("bid") or {}
        ask = c.get("ask") or {}
        bid_close = bid.get("close")
        ask_close = ask.get("close")
        rows.append({
            "ts": int(ts), "close": float(close),
            "open": float(open_) if open_ is not None else None,
            "high": float(high) if high is not None else float(close),
            "low": float(low) if low is not None else float(close),
            "volume": float(c.get("volume") or 0.0),
            "volume_notional": float(c.get("volume_notional_value_dollars") or 0.0),
            "open_interest": float(c["open_interest"]) if c.get("open_interest") is not None else float("nan"),
            "bid_close": float(bid_close) if bid_close is not None else float(close),
            "ask_close": float(ask_close) if ask_close is not None else float(close),
        })
    if not rows:
        # Explicit numeric dtypes even when empty -- a bare
        # pd.DataFrame(columns=[...]) defaults every column to "object",
        # which silently upcasts an entire pd.concat() to object dtype the
        # moment ANY chunk in a chained fetch (e.g. fetch_extended_candles
        # asking further back than a newly-listed ticker's history) comes
        # back empty, breaking pd.merge_asof downstream with a dtype error.
        return pd.DataFrame({
            "ts": pd.Series(dtype="int64"), "close": pd.Series(dtype="float64"),
            "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"), "low": pd.Series(dtype="float64"),
            "volume": pd.Series(dtype="float64"), "volume_notional": pd.Series(dtype="float64"),
            "open_interest": pd.Series(dtype="float64"),
            "bid_close": pd.Series(dtype="float64"), "ask_close": pd.Series(dtype="float64"),
        })
    df = pd.DataFrame(rows).drop_duplicates(subset="ts").sort_values("ts").reset_index(drop=True)
    _fill_missing_open(df)
    return df


def _fill_missing_open(df: pd.DataFrame) -> None:
    """In place: any row with no real "open" from the API gets the
    previous row's close (the first row, with no previous close, falls
    back to its own close -- a single-candle doji is the best honest
    guess with zero prior information)."""
    if df.empty:
        return
    missing = df["open"].isna()
    if not missing.any():
        return
    prev_close = df["close"].shift(1)
    df.loc[missing, "open"] = prev_close[missing]
    df["open"] = df["open"].fillna(df["close"])


_CANDLE_CACHE_TTL_SEC = int(os.getenv("PERPS_CANDLE_CACHE_TTL_SEC", "45") or "45")
# Defensive bound matching the same fix applied to alpaca_data.py's own
# equivalent cache after a real, confirmed OOM was traced to it never
# being pruned -- perps' active-ticker set is small/bounded so this is
# much less likely to matter here, but a delisted/inactive ticker's entry
# would otherwise sit forever, and this costs nothing to guard against.
_CANDLE_CACHE_MAX_AGE_SEC = _CANDLE_CACHE_TTL_SEC * 40
_candle_cache: dict[str, tuple[pd.DataFrame, pd.DataFrame, float]] = {}


def _prune_candle_cache(now_mono: float) -> None:
    stale = [k for k, (_, _, ts) in _candle_cache.items() if (now_mono - ts) > _CANDLE_CACHE_MAX_AGE_SEC]
    for k in stale:
        del _candle_cache[k]


def fetch_candle_frames(ticker: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (one_minute_df, hourly_df) each with columns [ts, close, open,
    high, low]. Short TTL cache so a strategy scan across many tickers and a
    dashboard status check landing in the same window don't double the
    Kalshi API calls for the same ticker."""
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
    _prune_candle_cache(now_mono)
    return one_min_df, hourly_df



# The candlestick API's ONLY native resolutions are 1-minute, 60-minute, and
# 1440-minute (daily) -- there is no sub-minute data available at all, from
# Kalshi or otherwise, for a product that only started trading 2026-06-04.
# Every timeframe from 1 minute up is derived directly from the 1-minute
# close series below (pct_change/rolling over N minutes) rather than
# needing a separate fetch per timeframe -- a "4-hour trend" is just
# close.pct_change(240) on the same series already being pulled for
# everything else.
MIN_ONE_MIN_ROWS_FOR_FEATURES = 245  # the 240-minute (4h) window + a small buffer


def engineer_features(one_min_df: pd.DataFrame, hourly_df: pd.DataFrame, *, sentiment_score: float) -> pd.DataFrame:
    """Leakage-free technical features computed only from data up to and
    including each row's own timestamp -- every rolling/shift operation looks
    backward only, never forward. Spans 1/3/5/10/15/30-minute and
    1/2/3/4-hour timeframes (see MIN_ONE_MIN_ROWS_FOR_FEATURES above for why
    only these and not anything finer)."""
    if one_min_df.empty or len(one_min_df) < MIN_ONE_MIN_ROWS_FOR_FEATURES:
        return pd.DataFrame()

    df = one_min_df.copy()
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
    df["ma_5"] = df["close"].rolling(5).mean()
    df["ma_15"] = df["close"].rolling(15).mean()
    df["ma_30"] = df["close"].rolling(30).mean()
    df["dist_to_ma_15"] = (df["close"] - df["ma_15"]) / df["ma_15"]
    df["dist_to_ma_30"] = (df["close"] - df["ma_30"]) / df["ma_30"]
    # Volatility at three scales -- short (5min) catches an immediate spike,
    # medium (15min, the original) and 30min give the strategy a read on
    # whether the CURRENT move is unusual relative to recent choppiness.
    df["volatility_5"] = df["ret_1m"].rolling(5).std()
    df["volatility_15"] = df["ret_1m"].rolling(15).std()
    df["volatility_30"] = df["ret_1m"].rolling(30).std()

    # Classic technical indicators, all backward-looking only (leakage-free).
    # Normalized to roughly the same 0-1-or-small-decimal scale as the
    # features above (RSI/100, ATR and MACD as a % of price) rather than
    # their traditional raw scales, so no single feature dominates a
    # LogisticRegression fit purely by having a much bigger native range.
    delta = df["close"].diff()
    avg_gain = delta.clip(lower=0).rolling(14).mean()
    avg_loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi_raw = 100 - (100 / (1 + rs))
    # A zero-loss window (e.g. a strict uptrend) makes `rs` NaN via the
    # replace(0, nan) above (guarding against a literal 0/0 divide) -- but
    # that's a real, common case that should resolve to exactly 100
    # (maximally overbought), not NaN. Restore it specifically; a truly
    # FLAT window (zero gains AND zero losses) correctly stays NaN and
    # drops out via dropna below.
    rsi_raw = rsi_raw.mask((avg_loss == 0) & (avg_gain > 0), 100.0)
    df["rsi_14"] = rsi_raw / 100.0

    ema_12 = df["close"].ewm(span=12, adjust=False).mean()
    ema_26 = df["close"].ewm(span=26, adjust=False).mean()
    macd_line = ema_12 - ema_26
    macd_signal = macd_line.ewm(span=9, adjust=False).mean()
    df["macd_hist_pct"] = (macd_line - macd_signal) / df["close"]

    bb_mid = df["close"].rolling(20).mean()
    bb_std = df["close"].rolling(20).std()
    bb_range = (4 * bb_std).replace(0, float("nan"))  # upper(+2std) - lower(-2std)
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

    # Volume: is RIGHT NOW unusually busy for THIS coin, relative to its OWN
    # recent baseline -- not an absolute threshold, since BTC's raw volume
    # dwarfs kSHIB's ("study each currency", same philosophy as the
    # volatility features above). Previously unused despite Kalshi returning
    # real volume on every candle already being fetched.
    # .fillna(0.0) on all three: a genuinely illiquid stretch (or open
    # interest briefly missing from Kalshi's own response) should degrade to
    # "no detected signal" rather than NaN-ing out (and dropna()-dropping)
    # the ENTIRE row over one ancillary field -- every other feature on that
    # row is still perfectly valid and shouldn't be thrown away with it.
    vol_ma_5 = df["volume"].rolling(5).mean()
    vol_ma_15 = df["volume"].rolling(15).mean()
    vol_ma_60 = df["volume"].rolling(60).mean().replace(0, float("nan"))
    df["volume_ratio_5"] = (vol_ma_5 / vol_ma_60).fillna(0.0)
    df["volume_ratio_15"] = (vol_ma_15 / vol_ma_60).fillna(0.0)
    dv_mean_60 = df["volume_notional"].rolling(60).mean()
    dv_std_60 = df["volume_notional"].rolling(60).std().replace(0, float("nan"))
    df["dollar_volume_z"] = ((df["volume_notional"] - dv_mean_60) / dv_std_60).fillna(0.0)

    # Open interest: a genuinely perp/futures-specific signal with no
    # equities equivalent -- rising OI alongside a rising price means fresh
    # money is entering long (real conviction, not just existing longs
    # trading among themselves); rising OI with a falling price means fresh
    # shorts; falling OI means positions are being closed/unwound (low
    # conviction either direction). Also previously fetched and discarded.
    df["oi_change_pct"] = df["open_interest"].ffill().pct_change(15).fillna(0.0)

    # Bid-ask spread as a % of the mid price -- a liquidity/microstructure
    # read: a wide spread means a real fill costs more than the last-trade
    # price suggests, stacking on top of the already-confirmed 1.6%
    # round-trip taker fee.
    mid = (df["bid_close"] + df["ask_close"]) / 2.0
    df["spread_pct"] = (df["ask_close"] - df["bid_close"]) / mid.replace(0, float("nan"))

    # Time-of-day / day-of-week: crypto perps trade 24/7 (no open/close like
    # equities), but volume and volatility still show a real daily pattern
    # (US/Asia/Europe session overlaps) and a real weekly one (thinner
    # weekend liquidity) -- cyclical sin/cos encoding since hour 23 and hour
    # 0 are adjacent on a clock, not 23 apart on a raw linear scale.
    ts_utc = pd.to_datetime(df["ts"], unit="s", utc=True)
    hour_frac = ts_utc.dt.hour + ts_utc.dt.minute / 60.0
    df["hour_sin"] = np.sin(2 * np.pi * hour_frac / 24.0)
    df["hour_cos"] = np.cos(2 * np.pi * hour_frac / 24.0)
    dow = ts_utc.dt.dayofweek
    df["dow_sin"] = np.sin(2 * np.pi * dow / 7.0)
    df["dow_cos"] = np.cos(2 * np.pi * dow / 7.0)

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

    return df.dropna(subset=FEATURE_COLUMNS).reset_index(drop=True)


FEATURE_COLUMNS = [
    "ret_1m", "ret_3m", "ret_5m", "ret_10m", "ret_15m", "ret_30m",
    "trend_1h", "trend_2h", "trend_3h", "trend_4h",
    "dist_to_ma_15", "dist_to_ma_30",
    "volatility_5", "volatility_15", "volatility_30",
    "rsi_14", "macd_hist_pct", "bb_pct_b", "bb_bandwidth", "atr_pct", "stoch_k",
    "volume_ratio_5", "volume_ratio_15", "dollar_volume_z", "oi_change_pct", "spread_pct",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "trend_pct", "sentiment_score",
]


def collect_dataset_rows(tickers: list[str] | None = None) -> pd.DataFrame:
    """Fetch + engineer features for every ACTIVE ticker (not narrowed to
    today's top-N watchlist -- see get_active_tickers()). Returns a single
    concatenated DataFrame with a `ticker` column, ready to push to HF or
    feed straight into training."""
    active_tickers = tickers or get_active_tickers()
    # The quota-limited sentiment sources (CryptoPanic/newsdata.io) are
    # gated to watchlist tickers only -- see get_sentiment()'s
    # use_limited_sources docstring for why.
    watchlist_tickers = set(get_watchlist())
    frames = []
    for ticker in active_tickers:
        try:
            one_min_df, hourly_df = fetch_candle_frames(ticker)
            sentiment = get_sentiment(coin_for_ticker(ticker), use_limited_sources=ticker in watchlist_tickers)
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
        sentiment = get_sentiment(coin_for_ticker(ticker), use_limited_sources=ticker in get_watchlist())
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


def _is_rate_limit_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "429" in text or "rate limit" in text


def retry_on_rate_limit(fn, *, attempts: int = 3, backoff_sec: float = 5.0):
    """HF's commit/upload endpoints have a stricter, separate rate limit
    from general API reads -- confirmed live (a 429 with "943/1000 requests
    remaining" in THIS 300s window, i.e. the commit endpoint's own limit,
    not the general one). A 429 there is transient; retry with backoff
    instead of dropping the push for that whole cycle."""
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if not _is_rate_limit_error(exc) or attempt == attempts - 1:
                raise
            time.sleep(backoff_sec * (attempt + 1))
    raise last_exc  # unreachable given the loop above always returns or raises


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
        # `existing` and `df` are now fully redundant with `combined` (a
        # concat is always a copy, never a view) -- freeing them here
        # matters because this whole merge-dedupe-rewrite step runs EVERY
        # 15-minute cycle against the WHOLE day's accumulated shard, which
        # only grows as the day goes on. Confirmed live: this function
        # (plus the collect step feeding it) was OOM-killing the container
        # on almost exactly its own 15-minute schedule for 13+ hours
        # straight before this fix.
        del existing, df
    else:
        combined = df
    # keep="last": this cycle's freshly re-fetched rows are concatenated
    # AFTER the existing shard, so when the same (ticker, ts) already
    # exists in today's shard, the NEWEST computed version wins. This
    # matters whenever engineer_features gains new columns: the
    # CANDLE_1M_LOOKBACK_HOURS rolling window keeps re-fetching and
    # re-computing the last few hours on every collection cycle, so a row
    # archived under an older schema gets naturally upgraded to the fuller
    # one within one more cycle instead of staying stuck with the columns
    # that existed the day it was first written.
    combined = combined.drop_duplicates(subset=["ticker", "ts"], keep="last").sort_values(["ticker", "ts"])
    # Write to a temp file THEN atomically rename into place (os.replace is
    # atomic on POSIX) -- confirmed live: a plain to_parquet(shard_path)
    # direct write left a truncated, corrupted shard ("Parquet magic bytes
    # not found in footer") after the process got OOM-killed mid-write.
    # This way a crash mid-write either leaves the OLD file fully intact or
    # the NEW file fully written -- never a partial one.
    tmp_path = shard_path.with_suffix(".parquet.tmp")
    combined.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, shard_path)
    rows_written = len(combined)
    # The upload below reads straight from shard_path on disk, not from this
    # object -- free the biggest thing this function ever holds (the full
    # cumulative day's shard) before it, rather than let it ride until the
    # function returns. Confirmed live: this exact function was the
    # dominant remaining memory driver in an isolated profile even after
    # the CANDLE_1M_LOOKBACK_HOURS fix, and a residual (much rarer, but
    # real) OOM still recurred here after that fix -- gc.collect() is a
    # small additional margin, not a full fix on its own.
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
            repo_id=HF_DATASET_REPO,
            repo_type="dataset",
            commit_message=f"perps data {today}",
        ))
        result["hf_uploaded"] = True
    except Exception as exc:
        logger.warning("[perps_data] HF upload failed: %s", exc)
        result["hf_uploaded"] = False
        result["hf_error"] = str(exc)
    gc.collect()
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
            # Most-recent-first, and stop once enough rows are already in
            # hand to satisfy the row cap below -- the archive grows every
            # day forever, so downloading and parsing EVERY shard (up to
            # max_shards) just to immediately truncate most of it away was
            # wasted work that got worse every single day. Confirmed live:
            # this was the recurring/worsening Render OOM source -- with
            # ~52 days archived, a plain (uncapped) load_training_dataset()
            # call downloaded and held all 52 days in memory before ever
            # truncating to the most recent MAX_TRAIN_ROWS. A small safety
            # margin (1.5x the cap) covers the rows that drop_duplicates
            # below will remove on any local/HF overlap.
            hf_files = sorted(hf_files, reverse=True)[:max_shards]
            cap = MAX_TRAIN_ROWS if max_rows is None else max_rows
            stop_after_rows = int(cap * 1.5) if cap else None
            accumulated_rows = sum(len(fr) for fr in frames)
            for f in hf_files:
                if stop_after_rows and accumulated_rows >= stop_after_rows:
                    break
                try:
                    local_path = hf_hub_download(repo_id=HF_DATASET_REPO, filename=f, repo_type="dataset", token=HF_API_KEY)
                    shard = pd.read_parquet(local_path)
                    if "ticker" in shard.columns and "ts" in shard.columns:
                        frames.append(shard)
                        accumulated_rows += len(shard)
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
