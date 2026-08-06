"""Alpaca stock market data -- separate from and independent of the Kalshi
perps bot (different broker, different asset class, different HF repos:
HF_ALPACA_DATASET_REPO / HF_ALPACA_MODEL_REPO). This pipeline is used ONLY
for trading equities/ETFs via the Alpaca paper (or, eventually, live)
account; it never reads or writes any Kalshi perps state.

Data flow (mirrors perps_data.py's/the former schwab_data.py's shape):
  get_us_stock_universe()      -> the tradable symbol universe, straight
                                   from Alpaca's own /v2/assets -- Alpaca IS
                                   the authoritative source here (unlike
                                   Schwab, which had no bulk listing
                                   endpoint and needed NASDAQ's own public
                                   symbol directory as a workaround)
  fetch_daily_bars(symbol)      -> up to 20 years of daily OHLCV
  fetch_minute_bars(symbol)     -> minute OHLCV over a given lookback --
                                   Alpaca has no ~35-day retention ceiling
                                   the way Schwab did, and alpaca_client's
                                   own get_bars() already paginates
                                   internally, so no manual day-windowed
                                   chaining loop is needed here
  fetch_recent_minute_bars(symbol) -> a short (LIVE_LOOKBACK_DAYS), cached
                                   window for repeated live scanning/
                                   collection
  engineer_features(df)        -> leakage-free volume/volatility/return
                                   features, same backward-only discipline
                                   as the perps pipeline
  push_daily_snapshot(df) /
  push_minute_snapshot(df)     -> append/dedupe into today's parquet shard,
                                   upload to HF_ALPACA_DATASET_REPO
"""
from __future__ import annotations

import datetime as dt
import gc
import logging
import os
import re
import time
from typing import Any

import pandas as pd

from data import alpaca_client
from data.stock_news import get_sentiment

logger = logging.getLogger(__name__)

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_ALPACA_DATASET_REPO = os.getenv("HF_ALPACA_DATASET_REPO", "papylove/alpaca-data")

_SESSION_NAME_MAP = {"preMarket": "pre_market", "regularMarket": "regular", "postMarket": "post_market"}


def _now_et() -> dt.datetime:
    try:
        import zoneinfo
        eastern = zoneinfo.ZoneInfo("America/New_York")
    except Exception:
        import pytz
        eastern = pytz.timezone("America/New_York")
    return dt.datetime.now(tz=eastern)


def _fallback_market_session() -> dict[str, Any]:
    """Hand-computed ET time-window check -- doesn't know about market
    holidays (Alpaca's own /v2/clock does), but works with zero
    credentials/network call at all."""
    now_et = _now_et()
    if now_et.weekday() >= 5:  # Saturday=5, Sunday=6
        return {"session": "closed", "is_open": False, "source": "fallback"}
    t = now_et.time()
    if dt.time(4, 0) <= t < dt.time(9, 30):
        return {"session": "pre_market", "is_open": False, "source": "fallback"}
    if dt.time(9, 30) <= t < dt.time(16, 0):
        return {"session": "regular", "is_open": True, "source": "fallback"}
    if dt.time(16, 0) <= t < dt.time(20, 0):
        return {"session": "post_market", "is_open": False, "source": "fallback"}
    return {"session": "closed", "is_open": False, "source": "fallback"}


def get_market_session() -> dict[str, Any]:
    """{"session": "closed"|"pre_market"|"regular"|"post_market", "is_open":
    bool, "source": "alpaca"|"fallback"}. Alpaca's /v2/clock is
    authoritative for the regular-session open/closed boundary (accounts
    for holidays correctly); it doesn't itself distinguish pre/post-market,
    so the hand-computed ET fallback is reused for THAT distinction only
    when the regular session is confirmed closed. Must never raise -- the
    scheduler uses this to decide whether to run intensive off-hours
    training vs. live trading checks."""
    try:
        clock = alpaca_client.get_clock()
        if bool(clock.get("is_open")):
            return {"session": "regular", "is_open": True, "source": "alpaca"}
        fallback = _fallback_market_session()
        return {"session": fallback["session"], "is_open": False, "source": "alpaca"}
    except Exception as exc:
        logger.info("[alpaca_data] clock lookup failed, using ET fallback: %s", exc)
        return _fallback_market_session()


_UNIVERSE_CACHE_TTL_SEC = 24 * 3600
_universe_cache: dict[str, Any] = {"symbols": None, "names": {}, "computed_at": 0.0}


def get_us_stock_universe(*, force: bool = False) -> list[str]:
    """Every tradable US equity/ETF symbol on this Alpaca account, straight
    from /v2/assets. Cached for _UNIVERSE_CACHE_TTL_SEC since the tradable
    universe barely changes day to day. Also populates the symbol->company
    name cache (see get_company_name()) as a side effect, at zero extra
    API-call cost -- /v2/assets already returns both in one response."""
    now = time.time()
    if not force and _universe_cache["symbols"] is not None and (now - _universe_cache["computed_at"]) < _UNIVERSE_CACHE_TTL_SEC:
        return _universe_cache["symbols"]

    try:
        assets = alpaca_client.get_assets(status="active", asset_class="us_equity")
        tradable = [a for a in assets if a.get("tradable") and a.get("symbol")]
        symbols = sorted({a["symbol"] for a in tradable})
        names = {a["symbol"]: a.get("name", "") for a in tradable}
    except Exception as exc:
        logger.warning("[alpaca_data] failed to fetch asset universe: %s", exc)
        symbols = _universe_cache["symbols"] or []
        names = _universe_cache["names"] or {}

    _universe_cache["symbols"] = symbols
    _universe_cache["names"] = names
    _universe_cache["computed_at"] = now
    return symbols


def get_company_name(symbol: str) -> str | None:
    """The company name Alpaca's own /v2/assets has on file for `symbol`
    (e.g. "Apple Inc. Common Stock" for AAPL) -- used to build a real,
    distinctive news-search query for stock_news.get_sentiment() instead of
    an ambiguous bare ticker. None if the universe hasn't been fetched yet
    or the symbol isn't in it; callers fall back to the ticker itself."""
    if _universe_cache["symbols"] is None:
        get_us_stock_universe()
    return _universe_cache["names"].get(symbol.upper())


def _bars_to_df(bars: list[dict[str, Any]]) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame({
            "ts": pd.Series(dtype="int64"), "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"), "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"), "volume": pd.Series(dtype="float64"),
        })
    rows = [{
        "ts": int(pd.Timestamp(b["t"]).timestamp()),  # Alpaca bars use an RFC-3339 UTC timestamp string
        "open": float(b["o"]), "high": float(b["h"]), "low": float(b["l"]),
        "close": float(b["c"]), "volume": float(b.get("v") or 0.0),
    } for b in bars]
    return pd.DataFrame(rows).drop_duplicates(subset="ts").sort_values("ts").reset_index(drop=True)


def fetch_daily_bars(symbol: str, *, years: int = 20) -> pd.DataFrame:
    """Up to `years` of daily OHLCV -- a single call (alpaca_client.get_bars
    paginates internally if the range needs more than one page)."""
    years = min(20, max(1, years))
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(days=365 * years)
    bars = alpaca_client.get_bars(
        [symbol], timeframe="1Day", start=start.date().isoformat(), end=end.date().isoformat(),
    ).get(symbol, [])
    return _bars_to_df(bars)


def fetch_minute_bars(symbol: str, *, days: int = 35) -> pd.DataFrame:
    """Minute OHLCV over the given lookback window. Unlike Schwab (which
    capped at 10 days per call and needed an explicit chained-window loop
    to cover more), a single alpaca_client.get_bars call covers the whole
    range -- its own page_token loop already handles pagination."""
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(days=days)
    try:
        bars = alpaca_client.get_bars(
            [symbol], timeframe="1Min", start=start.isoformat(), end=end.isoformat(),
        ).get(symbol, [])
    except Exception as exc:
        logger.warning("[alpaca_data] minute-bar fetch failed for %s: %s", symbol, exc)
        return _bars_to_df([])
    return _bars_to_df(bars)


# "Live" lookback for scanning/collection -- deliberately short. Alpaca has
# no ~35-day retention ceiling the way Schwab did, but engineer_features'
# longest rolling window is ~90 minutes, and push_minute_snapshot already
# merges/dedupes against existing shards, so nothing is lost by not
# re-pulling a long window on every call.
LIVE_LOOKBACK_DAYS = int(os.getenv("ALPACA_LIVE_LOOKBACK_DAYS", "5") or "5")
_MINUTE_BAR_CACHE_TTL_SEC = int(os.getenv("ALPACA_MINUTE_BAR_CACHE_TTL_SEC", "90") or "90")
# Real, confirmed production OOM caught live via active monitoring: this
# cache is written on every fetch but was never pruned, and its key is
# per-symbol -- unlike crypto/perps (a small, fixed, bounded universe of
# pairs/tickers), the equities watchlist ranks over the full tradable
# market and its top-N membership rotates over time, so distinct symbols
# accumulate in here forever as different names cycle in and out of the
# watchlist across hours of uptime. A crash landed inside data_collect
# right as a brand-new (empty) daily shard was being checked, ruling out
# "the shard is big" as the cause -- pointing at exactly this kind of
# unbounded, uptime-proportional growth instead of a per-cycle peak.
_MINUTE_BAR_CACHE_MAX_AGE_SEC = _MINUTE_BAR_CACHE_TTL_SEC * 40
_minute_bar_cache: dict[str, tuple[pd.DataFrame, float]] = {}


def _prune_minute_bar_cache(now_mono: float) -> None:
    stale = [k for k, (_, ts) in _minute_bar_cache.items() if (now_mono - ts) > _MINUTE_BAR_CACHE_MAX_AGE_SEC]
    for k in stale:
        del _minute_bar_cache[k]


def fetch_recent_minute_bars(symbol: str, *, days: int = LIVE_LOOKBACK_DAYS) -> pd.DataFrame:
    """Short-window, short-TTL-cached minute-bar fetch for LIVE feature
    computation (latest_feature_row / the periodic dataset-collection job)
    -- NOT fetch_minute_bars' deeper-history call, which is for a rare full
    backfill, not something to repeat on every scan.

    Same real bug this cache/window fixed on the Schwab side: scan_and_enter's
    per-symbol loop calls latest_feature_row(symbol) directly AND
    predict_direction(symbol) calls it AGAIN internally, and a separate
    periodic data-collect cycle fetches the SAME watchlist symbols too.
    Without this cache, one entry-scan cycle across a top-N watchlist could
    issue several times more API calls than necessary every cycle."""
    cache_key = f"{symbol}:{days}"
    cached = _minute_bar_cache.get(cache_key)
    now_mono = time.monotonic()
    if cached and (now_mono - cached[1]) < _MINUTE_BAR_CACHE_TTL_SEC:
        return cached[0]
    df = fetch_minute_bars(symbol, days=days)
    _minute_bar_cache[cache_key] = (df, now_mono)
    _prune_minute_bar_cache(now_mono)
    return df


FEATURE_COLUMNS = [
    "ret_1m", "ret_5m", "ret_15m", "ret_30m", "ret_60m",
    "dist_to_ma_15", "dist_to_ma_30",
    "volatility_5", "volatility_15", "volatility_30",
    "volume_ratio_5", "volume_ratio_15", "dollar_volume_z",
    "rsi_14", "macd_hist_pct", "bb_pct_b", "bb_bandwidth", "atr_pct", "stoch_k",
    "time_of_day_pct", "sentiment_score",
]
MIN_ROWS_FOR_FEATURES = 65  # the 60-minute return window + a small buffer


def _time_of_day_pct(ts_series: pd.Series) -> pd.Series:
    """0.0 at the 9:30am ET regular-session open, 1.0 at the 4:00pm ET
    close -- negative during pre-market, >1 during post-market. Stocks have
    a well-known intraday U-shape (highest volume/volatility right after
    open and right before close, quiet through the middle), so this lets
    the model learn "is this a normally-active TIME of day for this
    pattern" directly, rather than treating every minute as interchangeable."""
    et = pd.to_datetime(ts_series, unit="s", utc=True).dt.tz_convert("America/New_York")
    market_open = et.dt.normalize() + pd.Timedelta(hours=9, minutes=30)
    minutes_since_open = (et - market_open).dt.total_seconds() / 60.0
    return minutes_since_open / 390.0  # 390min = the 6.5-hour regular session


def engineer_features(one_min_df: pd.DataFrame, *, sentiment_score: float = 0.0) -> pd.DataFrame:
    """Leakage-free volume + volatility + return features from 1-minute
    bars -- every rolling/shift operation looks backward only, mirroring
    perps_data.engineer_features()'s discipline exactly. sentiment_score
    (from stock_news.get_sentiment(), see collect_dataset_rows/
    latest_feature_row) is broadcast as a constant column across the whole
    batch, same as perps_data.py's own sentiment_score wiring."""
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

    # Volume/volatility recognition: is RIGHT NOW unusually busy/volatile
    # for this stock, relative to its OWN recent baseline -- not an
    # absolute threshold, since a mega-cap's "normal" volume dwarfs a
    # small-cap's in raw share count.
    vol_ma_5 = df["volume"].rolling(5).mean()
    vol_ma_15 = df["volume"].rolling(15).mean()
    vol_ma_60 = df["volume"].rolling(60).mean().replace(0, float("nan"))
    df["volume_ratio_5"] = vol_ma_5 / vol_ma_60
    df["volume_ratio_15"] = vol_ma_15 / vol_ma_60
    dollar_volume = df["close"] * df["volume"]
    dv_mean_60 = dollar_volume.rolling(60).mean()
    dv_std_60 = dollar_volume.rolling(60).std().replace(0, float("nan"))
    df["dollar_volume_z"] = (dollar_volume - dv_mean_60) / dv_std_60

    # "Pro indicators" -- classic technical analysis, all backward-looking
    # only (leakage-free), normalized to roughly the same 0-1-or-small-
    # decimal scale as the features above. Identical formulas to
    # perps_data.py's indicator block; duplicated rather than shared since
    # the two pipelines are deliberately independent (see this module's own
    # docstring) and must never import from each other.
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

    df["time_of_day_pct"] = _time_of_day_pct(df["ts"])
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
        # Real bug found and fixed: unlike _push_model_to_hf() (which already
        # falls back to create_repo when the model repo doesn't exist yet),
        # this upload assumed the dataset repo already existed -- silently
        # failing every push with a RepositoryNotFoundError until a human
        # created it by hand. A brand-new HF_ALPACA_DATASET_REPO name (as
        # opposed to reusing an already-existing repo) hits this on the very
        # first push.
        try:
            api.repo_info(repo_id=HF_ALPACA_DATASET_REPO, repo_type="dataset")
        except Exception:
            api.create_repo(repo_id=HF_ALPACA_DATASET_REPO, repo_type="dataset", exist_ok=True, private=False)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            df.to_parquet(tmp.name, index=False)
            tmp_path = tmp.name
        try:
            api.upload_file(
                path_or_fileobj=tmp_path, path_in_repo=path_in_repo,
                repo_id=HF_ALPACA_DATASET_REPO, repo_type="dataset", commit_message=commit_message,
            )
        finally:
            os.unlink(tmp_path)
        return {"ok": True, "rows": len(df), "path": path_in_repo}
    except Exception as exc:
        logger.warning("[alpaca_data] HF upload failed for %s: %s", path_in_repo, exc)
        return {"ok": False, "error": str(exc)}


def _upload_shards_batch(entries: list[tuple[str, pd.DataFrame]], *, commit_message: str) -> dict[str, bool]:
    """Upload multiple date-shards in ONE HF commit instead of one commit per
    shard. Real, confirmed incident: backfill_minute_history's original
    one-commit-per-date loop hit HF's 128-commits/hour repo cap partway
    through a 251-date stocks backfill, silently dropping ~118 dates (the
    caller only marks a shard failed individually, so the run still reported
    ok:true overall -- confirmed live: only 133 of 251 attempted shards
    actually existed on the repo afterward). Batching many files into a
    single create_commit call keeps total commits per backfill run far below
    that cap no matter how many calendar dates are touched."""
    if not entries:
        return {}
    import tempfile
    from huggingface_hub import CommitOperationAdd, HfApi

    api = HfApi(token=HF_API_KEY)
    try:
        api.repo_info(repo_id=HF_ALPACA_DATASET_REPO, repo_type="dataset")
    except Exception:
        api.create_repo(repo_id=HF_ALPACA_DATASET_REPO, repo_type="dataset", exist_ok=True, private=False)

    tmp_paths = []
    try:
        operations = []
        for path_in_repo, df in entries:
            tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
            df.to_parquet(tmp.name, index=False)
            tmp.close()
            tmp_paths.append(tmp.name)
            operations.append(CommitOperationAdd(path_in_repo=path_in_repo, path_or_fileobj=tmp.name))
        api.create_commit(repo_id=HF_ALPACA_DATASET_REPO, repo_type="dataset", operations=operations, commit_message=commit_message)
        return {path_in_repo: True for path_in_repo, _ in entries}
    except Exception as exc:
        logger.warning("[alpaca_data] HF batch upload failed for %d shard(s): %s", len(entries), exc)
        return {path_in_repo: False for path_in_repo, _ in entries}
    finally:
        for p in tmp_paths:
            try:
                os.unlink(p)
            except Exception:
                pass


def push_daily_snapshot(symbol: str, df: pd.DataFrame) -> dict[str, Any]:
    """One parquet file per symbol under daily/ -- 20 years of daily bars is
    small enough per symbol that re-uploading the whole series each refresh
    (rather than day-by-day sharding, which only makes sense for the much
    higher-frequency minute data) is simplest and keeps every symbol's full
    history in one place."""
    if df.empty:
        return {"ok": False, "reason": "no_rows"}
    return _upload_shard(df, path_in_repo=f"daily/{symbol}.parquet", commit_message=f"update daily bars: {symbol}")


def get_symbols_with_daily_bars() -> set[str]:
    """Symbols that already have a daily-bar parquet on HF -- makes the full-
    universe historical backfill resumable across restarts/deploys/off-hours
    windows instead of re-fetching every symbol from scratch each run."""
    if not HF_API_KEY:
        return set()
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
        files = api.list_repo_files(repo_id=HF_ALPACA_DATASET_REPO, repo_type="dataset")
        return {f[len("daily/"):-len(".parquet")] for f in files if f.startswith("daily/") and f.endswith(".parquet")}
    except Exception as exc:
        logger.warning("[alpaca_data] could not list existing daily shards: %s", exc)
        return set()


def push_minute_snapshot(df: pd.DataFrame) -> dict[str, Any]:
    """Minute bars are sharded by calendar day across ALL symbols in one
    file (matching the Kalshi perps archive's data/YYYY-MM-DD.parquet
    convention). Downloads whatever's already there first and merges/
    dedupes (same discipline as perps_data.push_dataset_snapshot), so
    today's shard is a genuinely cumulative record of every symbol
    collected today, not just whichever ones happened to be on the
    watchlist most recently.

    Real, confirmed production OOM found in review: this had never picked
    up the explicit del+gc.collect() fix alpaca_crypto_data.py's/
    alpaca_options_data.py's own copies of this exact function already
    needed (same root cause -- downloading the existing shard,
    concatenating, then uploading held existing+df+combined
    simultaneously with nothing freed until the whole call returned) --
    confirmed live via Render's own logs: an oomKilled restart caught
    mid-flight inside THIS exact function (the log trail ended right
    after the HF shard-listing HEAD requests this function's own
    hf_hub_download call makes, with the crash landing a couple seconds
    later). That fix alone wasn't sufficient either -- see WATCHLIST_TOP_N's
    own comment for the rest of this incident (the watchlist size driving
    this shard's per-cycle growth, not just this function's own cleanup,
    was the remaining lever)."""
    if df.empty:
        return {"ok": False, "reason": "no_rows"}
    if not HF_API_KEY:
        return {"ok": False, "reason": "no_hf_api_key"}

    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    path_in_repo = f"minute/{today}.parquet"
    combined = df
    try:
        from huggingface_hub import hf_hub_download
        existing_path = hf_hub_download(repo_id=HF_ALPACA_DATASET_REPO, filename=path_in_repo, repo_type="dataset", token=HF_API_KEY)
        existing = pd.read_parquet(existing_path)
        combined = pd.concat([existing, df], ignore_index=True)
        del existing
    except Exception as exc:
        logger.info("[alpaca_data] no existing minute shard for %s yet (or fetch failed), starting fresh: %s", today, exc)

    if "symbol" in combined.columns and "ts" in combined.columns:
        combined = combined.drop_duplicates(subset=["symbol", "ts"]).sort_values(["symbol", "ts"]).reset_index(drop=True)
    del df
    gc.collect()
    try:
        return _upload_shard(combined, path_in_repo=path_in_repo, commit_message=f"append minute bars: {today}")
    finally:
        del combined
        gc.collect()


def backfill_minute_history(symbols: list[str], *, days: int = 90) -> dict[str, Any]:
    """Deep historical minute-bar backfill -- the live collector
    (push_minute_snapshot, called from _run_alpaca_data_collect) only ever
    writes TODAY's shard, so the archive load_training_dataset()/backtests
    read from only ever grows one day at a time from whenever collection
    first started. Confirmed live: only ~8 days of real history existed
    for this service despite Alpaca actually providing a full year of real
    1-minute bars per symbol (confirmed live: a 365-day AAPL fetch returned
    98,659 real rows in under 10s) -- this catches the archive up in one
    pass instead of waiting ~90 days for the live collector to accumulate
    the same depth organically.

    Processes symbol-by-symbol (bounds peak memory to one symbol's fetched
    window at a time, not the whole multi-symbol backfill), but
    accumulates every symbol's per-CALENDAR-DATE contribution into a
    shared in-memory map and uploads ONCE PER DATE at the end -- keeps HF
    round-trips bounded by calendar days, not symbols x days (the naive
    "merge and reupload after every single symbol" shape would be days x
    len(symbols) round trips instead of just days).

    Historical sentiment for arbitrary past dates isn't available from any
    free news API -- held at neutral (0.0) for every backfilled row, the
    same disclosed limitation perps_backtest.py's/alpaca_crypto_backtest.py's
    own sentiment gap already established for this codebase, rather than
    applying TODAY's sentiment to old rows (which would be actively wrong,
    not just absent)."""
    from collections import defaultdict
    if not HF_API_KEY:
        return {"ok": False, "reason": "no_hf_api_key"}

    by_date: dict[str, list[pd.DataFrame]] = defaultdict(list)
    symbols_processed = 0
    for symbol in symbols:
        try:
            one_min_df = fetch_minute_bars(symbol, days=days)
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
            logger.warning("[alpaca_data] backfill fetch failed for %s: %s", symbol, exc)
        gc.collect()

    from huggingface_hub import hf_hub_download
    _BATCH_SIZE = 20  # ~13 commits for a full 251-trading-day year, far under HF's 128/hour cap
    shard_row_counts: dict[str, int] = {}
    pending: list[tuple[str, pd.DataFrame]] = []

    def _flush(batch_num: int) -> None:
        if not pending:
            return
        results = _upload_shards_batch(pending, commit_message=f"backfill minute bars: batch {batch_num}")
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
            existing_path = hf_hub_download(repo_id=HF_ALPACA_DATASET_REPO, filename=path_in_repo, repo_type="dataset", token=HF_API_KEY)
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


# ---------------------------------------------------------------------------
# Watchlist: even at Alpaca's more generous 200 req/min free-tier ceiling,
# truly continuous minute-level refresh across the full tradable equity
# universe isn't practical -- same "broad archive, narrow live watchlist"
# split already proven out for the Kalshi perps bot and the former Schwab
# pipeline. The full universe still gets periodic (not per-minute) daily+
# minute backfills; only the ranked top-N watchlist gets refreshed on a
# fast, live cadence.
#
# Real, confirmed production incident: this was 100 (roughly double every
# other service's hot-loop universe -- perps: 8 tickers, options: 10
# underlyings, crypto: up to ~56 pairs), and this service (512MB) kept
# OOM-killing every 15-30 minutes even after two separate, real memory
# fixes elsewhere (RandomForest/GBM n_estimators, push_minute_snapshot's
# del/gc.collect()) were deployed and confirmed live via Render's own
# events. Every 100-symbol watchlist cycle drives scan_and_enter's
# per-symbol loop (fetch+engineer+predict x100, every ALPACA_CYCLE_MINUTES)
# AND collect_dataset_rows' full 5-day-lookback feature frame per symbol
# (x100, every ALPACA_DATA_COLLECT_MINUTES, then merged/reuploaded whole
# via push_minute_snapshot) -- both scale linearly with this number, and
# crypto's own comparable pipeline (up to ~56 pairs, same lookback/cadence)
# has had zero such crashes, so the size of this watchlist -- not just the
# cleanup discipline around it -- was the remaining real lever.
# ---------------------------------------------------------------------------
WATCHLIST_TOP_N = int(os.getenv("ALPACA_WATCHLIST_TOP_N", "40") or "40")
MAX_TRAIN_ROWS = int(os.getenv("ALPACA_MAX_TRAIN_ROWS", "150000") or "150000")
_DATE_SHARD_RE = re.compile(r"^minute/\d{4}-\d{2}-\d{2}\.parquet$")


def _recent_volume_and_volatility_by_symbol(df: pd.DataFrame) -> pd.DataFrame:
    """Per-symbol recent activity summary from already-collected minute
    data -- mirrors perps_data.py's volume+volatility watchlist ranking."""
    if df.empty:
        return pd.DataFrame(columns=["symbol", "dollar_volume", "volatility"])
    working = df.copy()
    working["dollar_volume"] = working["volume"] * working["close"]
    if "volatility_15" in working.columns:
        grouped = working.groupby("symbol").agg(dollar_volume=("dollar_volume", "mean"), volatility=("volatility_15", "mean"))
    else:
        grouped = working.groupby("symbol").agg(dollar_volume=("dollar_volume", "mean"), volatility=("close", "std"))
    return grouped.reset_index()


def get_stock_watchlist(recent_df: pd.DataFrame | None = None) -> list[str]:
    """Top WATCHLIST_TOP_N symbols by combined (recent dollar volume +
    recent volatility) rank -- the ones actually worth a fast, live-refresh
    cadence. Falls back to a small, safe default (mega-cap, highly liquid
    names) if no recent data has been collected yet or ranking fails."""
    fallback = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "AMD", "NFLX"]
    try:
        if recent_df is None or recent_df.empty:
            return fallback
        activity = _recent_volume_and_volatility_by_symbol(recent_df)
        if activity.empty:
            return fallback
        by_volume = activity.sort_values("dollar_volume", ascending=False)
        volume_rank = {s: i for i, s in enumerate(by_volume["symbol"])}
        by_volatility = activity.sort_values("volatility", ascending=False)
        volatility_rank = {s: i for i, s in enumerate(by_volatility["symbol"])}
        fallback_rank = len(activity)
        ranked = sorted(
            activity["symbol"],
            key=lambda s: volume_rank.get(s, fallback_rank) + volatility_rank.get(s, fallback_rank),
        )
        return sorted(ranked[:WATCHLIST_TOP_N]) or fallback
    except Exception as exc:
        logger.warning("[alpaca_data] watchlist ranking failed, using fallback: %s", exc)
        return fallback


def collect_dataset_rows(symbols: list[str] | None = None) -> pd.DataFrame:
    """Fetch + engineer features for the given symbols (default: the live
    watchlist -- NOT the full universe, which is handled by a separate,
    slower periodic backfill job)."""
    target_symbols = symbols if symbols is not None else get_stock_watchlist()
    frames = []
    for symbol in target_symbols:
        try:
            one_min_df = fetch_recent_minute_bars(symbol)
            sentiment = get_sentiment(symbol, company_name=get_company_name(symbol))
            feats = engineer_features(one_min_df, sentiment_score=sentiment["sentiment_score"])
            if feats.empty:
                continue
            feats.insert(0, "symbol", symbol)
            frames.append(feats)
        except Exception as exc:
            logger.warning("[alpaca_data] collect failed for %s: %s", symbol, exc)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def latest_feature_row(symbol: str) -> dict[str, Any] | None:
    """The single most-recent feature row for one symbol, for live
    prediction. Its label is always NaN (the future outcome hasn't
    happened yet) -- expected, we only need the feature columns here."""
    try:
        one_min_df = fetch_recent_minute_bars(symbol)
        sentiment = get_sentiment(symbol, company_name=get_company_name(symbol))
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
        logger.warning("[alpaca_data] latest_feature_row failed for %s: %s", symbol, exc)
        return None


# 20s/15s (not a tighter value): real, confirmed live recalibration --
# call_with_hard_timeout runs each call on its own thread, but this service
# still only has ONE Python interpreter (GIL) shared with the APScheduler
# background thread's own concurrent HF/Alpaca calls (fast_check every 20s,
# entry_scan every 2min, data_collect every 15min). A 10s/8s timeout was
# tight enough that a real (non-hung, just GIL-contended) call regularly
# exceeded it and returned an empty "no_data" result -- safe, but
# needlessly conservative. Confirmed live: gunicorn's own --timeout is now
# 300s (see render.yaml), leaving ample headroom for these more generous
# per-call ceilings.
_LOAD_TRAINING_DATASET_LIST_TIMEOUT_SEC = int(os.getenv("ALPACA_LOAD_TRAINING_DATASET_LIST_TIMEOUT_SEC", "20") or "20")
_LOAD_TRAINING_DATASET_SHARD_TIMEOUT_SEC = int(os.getenv("ALPACA_LOAD_TRAINING_DATASET_SHARD_TIMEOUT_SEC", "15") or "15")


def load_training_dataset(*, max_shards: int = 90, max_rows: int | None = None) -> pd.DataFrame:
    """Downloads minute-bar shards from HF_ALPACA_DATASET_REPO, most-recent-
    first, stopping once enough rows are in hand to cover the cap (with a
    safety margin for dedup).

    Real, confirmed production incident: called synchronously from a Flask
    request handler (both the pre-existing /api/alpaca/train route and the
    newer /api/alpaca/train_torch route, via train_model()/
    train_torch_candidate_model()), this used to have NO timeout on either
    the initial list_repo_files call or any of the up-to-90 sequential
    hf_hub_download calls. huggingface_hub's internal shared-session lock
    can hang indefinitely (the exact same class of incident
    call_with_hard_timeout's own docstring documents for perps'
    _pull_durable_state_from_hf) -- and even without a hang, 90 fully
    sequential downloads can legitimately exceed gunicorn's own --timeout
    ceiling on these services. Seen live: gunicorn's WORKER TIMEOUT fired
    mid-call, forcibly SIGABRT-ing the worker (and, since the background
    APScheduler thread lives in that same process, taking every scheduled
    job down with it until gunicorn rebooted a replacement worker).

    Each call below is individually bounded so ONE stuck/slow shard
    degrades to "skip this shard, keep going" (preserving the original
    per-shard try/except's partial-results resilience) instead of freezing
    the whole function -- wrapping the whole loop as a single hard-timeout
    call would instead have discarded every already-downloaded shard the
    moment any one of them timed out.

    Real, confirmed production incident (Render's own events: oomKilled,
    512Mi limit, during the daily 04:00 ET alpaca_train cron): this used to
    accumulate every downloaded shard in one Python list and pd.concat()
    them ALL at once at the end. That was fine back when the archive only
    held a handful of days -- but the real historical backfill this same
    session added (~8 days -> a full year, ~253 shards) means max_shards=90
    now legitimately means up to 90 full-day, all-symbol DataFrames held
    simultaneously in memory before the final concat even starts, on a
    512MB container. Flushing into a running `combined` frame every
    _SHARD_FLUSH_BATCH shards (with an explicit gc.collect()) bounds peak
    "raw shard frames held at once" to that batch size instead of
    max_shards, the same incremental-accumulation discipline this
    session's own backfill_minute_history() already uses for HF uploads."""
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

    try:
        from huggingface_hub import HfApi, hf_hub_download
        from server_common import call_with_hard_timeout
        api = HfApi(token=HF_API_KEY)
        raw_files = call_with_hard_timeout(
            lambda: api.list_repo_files(repo_id=HF_ALPACA_DATASET_REPO, repo_type="dataset"),
            timeout_sec=_LOAD_TRAINING_DATASET_LIST_TIMEOUT_SEC, on_timeout=None,
        )
        if raw_files is None:
            return pd.DataFrame()
        hf_files = [f for f in raw_files if _DATE_SHARD_RE.match(f)]
        hf_files = sorted(hf_files, reverse=True)[:max_shards]
        for f in hf_files:
            if stop_after_rows and accumulated_rows >= stop_after_rows:
                break
            try:
                local_path = call_with_hard_timeout(
                    lambda f=f: hf_hub_download(repo_id=HF_ALPACA_DATASET_REPO, filename=f, repo_type="dataset", token=HF_API_KEY),
                    timeout_sec=_LOAD_TRAINING_DATASET_SHARD_TIMEOUT_SEC, on_timeout=None,
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
                    logger.warning("[alpaca_data] skipping shard with unexpected schema: %s", f)
            except Exception as exc:
                logger.warning("[alpaca_data] failed to read shard %s: %s", f, exc)
    except Exception as exc:
        logger.warning("[alpaca_data] HF dataset listing failed: %s", exc)
    _flush_pending()

    if combined is None or combined.empty:
        return pd.DataFrame()
    if "symbol" in combined.columns and "ts" in combined.columns:
        combined = combined.drop_duplicates(subset=["symbol", "ts"])
        combined["symbol"] = combined["symbol"].astype("category")
        if cap and len(combined) > cap:
            combined = combined.sort_values("ts").tail(cap).reset_index(drop=True)
    return combined
