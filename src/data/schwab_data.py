"""Charles Schwab stock market data -- separate from and independent of the
Kalshi perps bot (different broker, different asset class, different HF
repos: HF_STOCK_DATASET_REPO / HF_STOCK_MODEL_REPO). This pipeline is used
ONLY for trading equities/ETFs via the Schwab account; it never reads or
writes any Kalshi perps state.

Data flow (mirrors perps_data.py's shape, adapted for stocks):
  get_us_stock_universe()      -> the full list of US-listed symbols, from
                                   NASDAQ's own free public symbol directory
                                   (broker APIs don't offer a bulk "list
                                   every listed symbol" endpoint)
  fetch_daily_bars(symbol)     -> up to 20 years of daily OHLCV (single
                                   Schwab call -- period=20 is a valid
                                   periodType=year value)
  fetch_minute_bars(symbol)    -> the ~30-35 days of 1-minute OHLCV Schwab
                                   actually retains (chained calls -- a
                                   single periodType=day call caps at 10
                                   days per Schwab's own period enum). Used
                                   for a one-time/rare full backfill only.
  fetch_recent_minute_bars(symbol) -> a short (LIVE_LOOKBACK_DAYS), cached
                                   window for repeated live scanning/
                                   collection -- see its own docstring for
                                   why the full 35-day fetch was a real
                                   rate-limit problem here.
  engineer_features(df)        -> leakage-free volume/volatility/return
                                   features, same backward-only discipline
                                   as the perps pipeline
  push_daily_snapshot(df) /
  push_minute_snapshot(df)     -> append/dedupe into today's parquet shard,
                                   upload to HF_STOCK_DATASET_REPO
"""
from __future__ import annotations

import datetime as dt
import io
import logging
import os
import re
import time
from typing import Any

import pandas as pd
import requests

from data import schwab_client

logger = logging.getLogger(__name__)

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_STOCK_DATASET_REPO = os.getenv("HF_STOCK_DATASET_REPO", "papylove/schwab-data")

_TIMEOUT_SEC = 15
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
    holidays (Schwab's own endpoint does), but works before OAuth login is
    even complete and needs no network call at all."""
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
    bool, "source": "schwab"|"fallback"}. Tries Schwab's own authoritative
    market-hours endpoint first (correctly accounts for holidays); falls
    back to a hand-computed ET check (doesn't know about holidays, but
    needs no login and never fails) if that call doesn't work -- this must
    never raise, since the scheduler uses it to decide whether to run
    intensive off-hours training vs. live trading checks."""
    try:
        data = schwab_client.get("/marketdata/v1/markets", params={"markets": "equity"})
        equity_markets = data.get("equity") or {}
        equity = equity_markets.get("EQ") or next(iter(equity_markets.values()), {})
        is_open = bool(equity.get("isOpen"))
        session_hours = equity.get("sessionHours") or {}
        now = dt.datetime.now(dt.timezone.utc)
        for raw_name, mapped_name in _SESSION_NAME_MAP.items():
            for window in session_hours.get(raw_name) or []:
                start = dt.datetime.fromisoformat(window["start"])
                end = dt.datetime.fromisoformat(window["end"])
                if start <= now <= end:
                    return {"session": mapped_name, "is_open": is_open, "source": "schwab"}
        return {"session": "closed", "is_open": is_open, "source": "schwab"}
    except Exception as exc:
        logger.info("[schwab_data] market-hours lookup failed, using ET fallback: %s", exc)
        return _fallback_market_session()
_NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
_OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
_UNIVERSE_CACHE_TTL_SEC = 24 * 3600
_universe_cache: dict[str, Any] = {"symbols": None, "computed_at": 0.0}


def get_us_stock_universe(*, force: bool = False) -> list[str]:
    """Every US-listed equity/ETF symbol, from NASDAQ's own free public
    symbol directory (updated daily, no auth needed) -- Schwab's own API
    only supports looking up symbols you already know, not listing the
    whole market. Cached for _UNIVERSE_CACHE_TTL_SEC since the listed-symbol
    universe barely changes day to day."""
    now = time.time()
    if not force and _universe_cache["symbols"] is not None and (now - _universe_cache["computed_at"]) < _UNIVERSE_CACHE_TTL_SEC:
        return _universe_cache["symbols"]

    symbols: set[str] = set()
    for url, symbol_col in [(_NASDAQ_LISTED_URL, "Symbol"), (_OTHER_LISTED_URL, "ACT Symbol")]:
        try:
            resp = requests.get(url, timeout=_TIMEOUT_SEC)
            resp.raise_for_status()
            # Last line of both files is a "File Creation Time" footer, not
            # a real row -- confirmed live it can have the SAME field count
            # as the header (just short/blank values), so pandas doesn't
            # treat it as a parse error to skip; it must be dropped by
            # content before parsing, not relied on to fail structurally.
            lines = [ln for ln in resp.text.splitlines() if not ln.startswith("File Creation Time")]
            df = pd.read_csv(io.StringIO("\n".join(lines)), sep="|", on_bad_lines="skip")
            test_col = "Test Issue"
            if test_col in df.columns:
                df = df[df[test_col] != "Y"]
            symbols.update(str(s).strip() for s in df[symbol_col].dropna() if str(s).strip())
        except Exception as exc:
            logger.warning("[schwab_data] failed to fetch symbol directory %s: %s", url, exc)

    result = sorted(symbols)
    _universe_cache["symbols"] = result
    _universe_cache["computed_at"] = now
    return result


def _candles_to_df(candles: list[dict[str, Any]]) -> pd.DataFrame:
    if not candles:
        return pd.DataFrame({
            "ts": pd.Series(dtype="int64"), "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"), "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"), "volume": pd.Series(dtype="float64"),
        })
    rows = [{
        "ts": int(c["datetime"]) // 1000,  # Schwab candles use epoch MILLISECONDS
        "open": float(c["open"]), "high": float(c["high"]), "low": float(c["low"]),
        "close": float(c["close"]), "volume": float(c.get("volume") or 0.0),
    } for c in candles]
    return pd.DataFrame(rows).drop_duplicates(subset="ts").sort_values("ts").reset_index(drop=True)


def fetch_daily_bars(symbol: str, *, years: int = 20) -> pd.DataFrame:
    """Up to `years` of daily OHLCV -- a single call, since Schwab's own
    periodType=year enum directly supports up to 20 (the docs list valid
    year periods as 1/2/3/5/10/15/20)."""
    years = min(20, max(1, years))
    data = schwab_client.get("/marketdata/v1/pricehistory", params={
        "symbol": symbol, "periodType": "year", "period": years,
        "frequencyType": "daily", "frequency": 1, "needExtendedHoursData": False,
    })
    return _candles_to_df(data.get("candles") or [])


def fetch_minute_bars(symbol: str, *, days: int = 35) -> pd.DataFrame:
    """The full ~30-35 days of 1-minute OHLCV Schwab actually retains.
    periodType=day caps at period=10 per call (Schwab's own enum), so this
    chains backward in 10-day windows via explicit startDate/endDate rather
    than relying on period alone -- same "chain calls to cover more than
    one request allows" shape as the Kalshi perps candle fetcher."""
    frames = []
    now = dt.datetime.now(dt.timezone.utc)
    window_end = now
    remaining_days = days
    while remaining_days > 0:
        span_days = min(10, remaining_days)
        window_start = window_end - dt.timedelta(days=span_days)
        try:
            data = schwab_client.get("/marketdata/v1/pricehistory", params={
                "symbol": symbol, "periodType": "day", "frequencyType": "minute", "frequency": 1,
                "startDate": int(window_start.timestamp() * 1000), "endDate": int(window_end.timestamp() * 1000),
                "needExtendedHoursData": False,
            })
            frames.append(_candles_to_df(data.get("candles") or []))
        except Exception as exc:
            logger.warning("[schwab_data] minute-bar chunk fetch failed for %s: %s", symbol, exc)
            break
        window_end = window_start
        remaining_days -= span_days
    frames = [f for f in frames if not f.empty]
    if not frames:
        return _candles_to_df([])
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset="ts").sort_values("ts").reset_index(drop=True)


# "Live" lookback for scanning/collection -- deliberately much shorter than
# fetch_minute_bars' full 35-day retained window. engineer_features' longest
# rolling window is ~90 minutes, and push_minute_snapshot already merges/
# dedupes against existing shards, so nothing is lost by not re-pulling 35
# days on every call; a few recent days is more than enough buffer for
# weekends/holidays. LIVE_LOOKBACK_DAYS <= 10 fits Schwab's periodType=day
# single-call cap, so this is ALSO a single API call per symbol instead of
# fetch_minute_bars(days=35)'s ~4 chained calls.
LIVE_LOOKBACK_DAYS = int(os.getenv("SCHWAB_LIVE_LOOKBACK_DAYS", "5") or "5")
_MINUTE_BAR_CACHE_TTL_SEC = int(os.getenv("SCHWAB_MINUTE_BAR_CACHE_TTL_SEC", "90") or "90")
_minute_bar_cache: dict[str, tuple[pd.DataFrame, float]] = {}


def fetch_recent_minute_bars(symbol: str, *, days: int = LIVE_LOOKBACK_DAYS) -> pd.DataFrame:
    """Short-window, short-TTL-cached minute-bar fetch for LIVE feature
    computation (latest_feature_row / the periodic dataset-collection job)
    -- NOT fetch_minute_bars' full deep-history chain, which is for a rare
    full backfill, not something to repeat on every scan.

    Real bug found and fixed here: scan_and_enter's per-symbol loop calls
    latest_feature_row(symbol) directly AND predict_direction(symbol) calls
    it AGAIN internally, and a separate 15-minute data-collect cycle fetches
    the SAME watchlist symbols too. Before this cache/window existed, all
    three call sites used fetch_minute_bars(symbol, days=35) (chained ~4
    calls per symbol) with zero rate-limit awareness -- at
    WATCHLIST_TOP_N=100, one 2-minute entry-scan cycle alone could issue up
    to ~800 Schwab API calls, more than 6x Schwab's own 120 req/min limit,
    which would have silently 429'd most of the watchlist on every cycle
    the moment a real login made this code path actually run."""
    cache_key = f"{symbol}:{days}"
    cached = _minute_bar_cache.get(cache_key)
    now_mono = time.monotonic()
    if cached and (now_mono - cached[1]) < _MINUTE_BAR_CACHE_TTL_SEC:
        return cached[0]
    df = fetch_minute_bars(symbol, days=days)
    _minute_bar_cache[cache_key] = (df, now_mono)
    return df


FEATURE_COLUMNS = [
    "ret_1m", "ret_5m", "ret_15m", "ret_30m", "ret_60m",
    "dist_to_ma_15", "dist_to_ma_30",
    "volatility_5", "volatility_15", "volatility_30",
    "volume_ratio_5", "volume_ratio_15", "dollar_volume_z",
    "rsi_14", "macd_hist_pct", "bb_pct_b", "bb_bandwidth", "atr_pct", "stoch_k",
    "time_of_day_pct",
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


def engineer_features(one_min_df: pd.DataFrame) -> pd.DataFrame:
    """Leakage-free volume + volatility + return features from 1-minute
    bars -- every rolling/shift operation looks backward only, mirroring
    perps_data.engineer_features()'s discipline exactly."""
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
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            df.to_parquet(tmp.name, index=False)
            tmp_path = tmp.name
        try:
            api.upload_file(
                path_or_fileobj=tmp_path, path_in_repo=path_in_repo,
                repo_id=HF_STOCK_DATASET_REPO, repo_type="dataset", commit_message=commit_message,
            )
        finally:
            os.unlink(tmp_path)
        return {"ok": True, "rows": len(df), "path": path_in_repo}
    except Exception as exc:
        logger.warning("[schwab_data] HF upload failed for %s: %s", path_in_repo, exc)
        return {"ok": False, "error": str(exc)}


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
        files = api.list_repo_files(repo_id=HF_STOCK_DATASET_REPO, repo_type="dataset")
        return {f[len("daily/"):-len(".parquet")] for f in files if f.startswith("daily/") and f.endswith(".parquet")}
    except Exception as exc:
        logger.warning("[schwab_data] could not list existing daily shards: %s", exc)
        return set()


def push_minute_snapshot(df: pd.DataFrame) -> dict[str, Any]:
    """Minute bars are sharded by calendar day across ALL symbols in one
    file (matching the Kalshi perps archive's data/YYYY-MM-DD.parquet
    convention) -- there are far more of these than daily bars, and today's
    shard is what a live collection job appends/dedupes into repeatedly.

    Real bug fixed here: this used to upload `df` directly, OVERWRITING
    today's whole shard with only the current cycle's watchlist -- any
    symbol that was in an EARLIER cycle's watchlist but has since dropped
    out (the ranking refreshes periodically) would have its already-
    collected rows for today silently destroyed by the next cycle's
    upload. Now downloads whatever's already there first and merges/dedupes
    (same discipline as perps_data.push_dataset_snapshot), so today's shard
    is a genuinely cumulative record of every symbol collected today, not
    just whichever ones happened to be on the watchlist most recently."""
    if df.empty:
        return {"ok": False, "reason": "no_rows"}
    if not HF_API_KEY:
        return {"ok": False, "reason": "no_hf_api_key"}

    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    path_in_repo = f"minute/{today}.parquet"
    combined = df
    try:
        from huggingface_hub import hf_hub_download
        existing_path = hf_hub_download(repo_id=HF_STOCK_DATASET_REPO, filename=path_in_repo, repo_type="dataset", token=HF_API_KEY)
        existing = pd.read_parquet(existing_path)
        combined = pd.concat([existing, df], ignore_index=True)
    except Exception as exc:
        logger.info("[schwab_data] no existing minute shard for %s yet (or fetch failed), starting fresh: %s", today, exc)

    if "symbol" in combined.columns and "ts" in combined.columns:
        combined = combined.drop_duplicates(subset=["symbol", "ts"]).sort_values(["symbol", "ts"]).reset_index(drop=True)
    return _upload_shard(combined, path_in_repo=path_in_repo, commit_message=f"append minute bars: {today}")


# ---------------------------------------------------------------------------
# Watchlist: Schwab's 120 req/min rate limit makes truly continuous
# minute-level refresh impossible across the full ~12,000-symbol US equity
# universe (a single full pass alone takes ~100 minutes) -- exactly the
# same "broad archive, narrow live watchlist" split already proven out for
# the Kalshi perps bot (get_active_tickers() vs get_watchlist()). The full
# universe still gets periodic (not per-minute) daily+minute backfills;
# only the ranked top-N watchlist gets refreshed on a fast, live cadence.
# ---------------------------------------------------------------------------
WATCHLIST_TOP_N = int(os.getenv("SCHWAB_WATCHLIST_TOP_N", "100") or "100")
MAX_TRAIN_ROWS = int(os.getenv("SCHWAB_MAX_TRAIN_ROWS", "150000") or "150000")
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
        logger.warning("[schwab_data] watchlist ranking failed, using fallback: %s", exc)
        return fallback


def collect_dataset_rows(symbols: list[str] | None = None) -> pd.DataFrame:
    """Fetch + engineer features for the given symbols (default: the live
    watchlist -- NOT the full universe, which is handled by a separate,
    slower periodic backfill job given the rate limit)."""
    target_symbols = symbols if symbols is not None else get_stock_watchlist()
    frames = []
    for symbol in target_symbols:
        try:
            one_min_df = fetch_recent_minute_bars(symbol)
            feats = engineer_features(one_min_df)
            if feats.empty:
                continue
            feats.insert(0, "symbol", symbol)
            frames.append(feats)
        except Exception as exc:
            logger.warning("[schwab_data] collect failed for %s: %s", symbol, exc)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def latest_feature_row(symbol: str) -> dict[str, Any] | None:
    """The single most-recent feature row for one symbol, for live
    prediction. Its label is always NaN (the future outcome hasn't
    happened yet) -- expected, we only need the feature columns here."""
    try:
        one_min_df = fetch_recent_minute_bars(symbol)
        feats = engineer_features(one_min_df)
        if feats.empty:
            return None
        last = feats.iloc[-1]
        row = {col: float(last[col]) for col in FEATURE_COLUMNS}
        row["symbol"] = symbol
        row["current_price"] = float(one_min_df["close"].iloc[-1])
        row["short_ma"] = float(last["ma_15"])
        return row
    except Exception as exc:
        logger.warning("[schwab_data] latest_feature_row failed for %s: %s", symbol, exc)
        return None


def load_training_dataset(*, max_shards: int = 90, max_rows: int | None = None) -> pd.DataFrame:
    """Downloads minute-bar shards from HF_STOCK_DATASET_REPO, most-recent-
    first, stopping once enough rows are in hand to cover the cap (with a
    safety margin for dedup) -- built this way from day one, unlike the
    Kalshi perps archive which had to learn this the hard way after a real
    OOM (see perps_data.load_training_dataset's own history)."""
    if not HF_API_KEY:
        return pd.DataFrame()
    cap = MAX_TRAIN_ROWS if max_rows is None else max_rows
    stop_after_rows = int(cap * 1.5) if cap else None
    frames: list[pd.DataFrame] = []
    accumulated_rows = 0
    try:
        from huggingface_hub import HfApi, hf_hub_download
        api = HfApi(token=HF_API_KEY)
        hf_files = [f for f in api.list_repo_files(repo_id=HF_STOCK_DATASET_REPO, repo_type="dataset") if _DATE_SHARD_RE.match(f)]
        hf_files = sorted(hf_files, reverse=True)[:max_shards]
        for f in hf_files:
            if stop_after_rows and accumulated_rows >= stop_after_rows:
                break
            try:
                local_path = hf_hub_download(repo_id=HF_STOCK_DATASET_REPO, filename=f, repo_type="dataset", token=HF_API_KEY)
                shard = pd.read_parquet(local_path)
                if "symbol" in shard.columns and "ts" in shard.columns:
                    frames.append(shard)
                    accumulated_rows += len(shard)
                else:
                    logger.warning("[schwab_data] skipping shard with unexpected schema: %s", f)
            except Exception as exc:
                logger.warning("[schwab_data] failed to read shard %s: %s", f, exc)
    except Exception as exc:
        logger.warning("[schwab_data] HF dataset listing failed: %s", exc)

    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    del frames
    if "symbol" in combined.columns and "ts" in combined.columns:
        combined = combined.drop_duplicates(subset=["symbol", "ts"])
        combined["symbol"] = combined["symbol"].astype("category")
        if cap and len(combined) > cap:
            combined = combined.sort_values("ts").tail(cap).reset_index(drop=True)
    return combined
