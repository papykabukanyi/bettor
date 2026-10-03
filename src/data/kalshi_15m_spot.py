"""Real 1-minute spot history for the crypto underlyings, from Alpaca's
crypto market data at its Kraken US location ("us-1", see
alpaca_client.CHART_CRYPTO_LOC). Kraken is one of the exchanges CF
Benchmarks' Real-Time Index -- what every KX*15M crypto contract settles on
-- is built from.

Until Oct 2026 this archive came from Coinbase Exchange's public candles,
validated against Kalshi's own settlement values on 15,936 windows (2.87 bps
median error, close-vs-open direction matching the settled result 93.5%).
Alpaca's Kraken bars track Coinbase minute for minute (24h check, Oct 2026:
median 0.4 bps for BTC/ETH/SOL, 0.7-3.5 bps for the alts, 6 bps for SHIB),
with every minute present. rebuild_from_alpaca() re-reads the archive from
Alpaca once so every backtest and live chart reads the same source. Coins
Alpaca doesn't list (NEAR, ZEC, SUI, BNB, HBAR, XLM, TON) have no chart, so
the bots don't trade them; their old Coinbase rows stay in the archive.

Live reads (recent_series) put the Alpaca stream's minute bars
(alpaca_stream "crypto", same venue) on top of the REST history, so a bar
is visible the second it closes.

Stored as spot_history/{UTC date}.parquet (coin, ts = candle END, open,
high, low, close, volume) in the 15m dataset repo. Alpaca prints a bar for
every quoted minute (zero volume, quote-mid prices when nothing traded);
complete_minutes() forward-fills any minute still missing.
"""
from __future__ import annotations

import datetime as dt
import gc
import json
import logging
import math
import os
import re
import threading
import time
from typing import Any

import pandas as pd

from server_common import DATA_DIR

logger = logging.getLogger(__name__)

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_KALSHI_15M_DATASET_REPO = os.getenv("HF_KALSHI_15M_DATASET_REPO", "papylove/kalshi-15m-data")

SOURCE = "alpaca"
# Every coin a Kalshi bot trades that Alpaca's Kraken feed carries (checked
# Oct 2026: a bar every minute of a 24h window for each).
SPOT_PRODUCTS = {
    "BTC": "BTC/USD", "ETH": "ETH/USD", "SOL": "SOL/USD", "XRP": "XRP/USD", "DOGE": "DOGE/USD",
    "BCH": "BCH/USD", "HYPE": "HYPE/USD", "ADA": "ADA/USD", "LINK": "LINK/USD", "LTC": "LTC/USD",
    "AAVE": "AAVE/USD", "DOT": "DOT/USD", "SHIB": "SHIB/USD",
}
# A traded symbol whose chart is another product's (kSHIB perp = 1,000 SHIB;
# price units differ, the chart's shape doesn't).
CHART_ALIASES = {"KSHIB": "SHIB"}


def chart_coin(coin: str) -> str:
    return CHART_ALIASES.get(coin, coin)


MAX_CANDLES_PER_CALL = 300  # live top-up window, minutes
MAX_PAGES = 60
REQUEST_SPACING_SEC = 0.0
LOCAL_DIR = DATA_DIR / "kalshi_15m_spot_history"
SOURCE_MARKER = "spot_history/SOURCE.json"
COLUMNS = ["coin", "ts", "open", "high", "low", "close", "volume"]
FEATURE_COLUMNS = [
    "close", "ret_5m", "ret_10m", "ret_15m", "ret_30m", "trend_1h", "trend_2h", "trend_4h", "trend_8h", "trend_1d",
    "volatility_15", "volatility_30", "dollar_volume_z",
]
LIVE_CACHE_HOURS = 27
_SHARD_RE = re.compile(r"^spot_history/(\d{4}-\d{2}-\d{2})\.parquet$")
_HF_TIMEOUT_SEC = 30
_cache_lock = threading.Lock()
_live_cache: dict[str, pd.DataFrame] = {}


_unlisted: dict[str, float] = {}  # coin -> time Alpaca returned no bars for {coin}/USD
_last_refresh: dict[str, float] = {}
LIVE_REFRESH_MIN_SEC = 20


def product_for(coin: str) -> str:
    """The Alpaca pair for a coin: the configured one, else {coin}/USD
    (charts for any coin another bot trades, e.g. Alpaca crypto pairs)."""
    return SPOT_PRODUCTS.get(coin) or f"{coin}/USD"


def is_listed(coin: str) -> bool:
    return coin in SPOT_PRODUCTS or time.time() - _unlisted.get(coin, 0.0) > 86400


def _iso(ts: int) -> str:
    return dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_many(coins: list[str], start_ts: int, end_ts: int) -> pd.DataFrame:
    """1-minute candles ending in (start_ts, end_ts] for several coins in one
    paginated request. Alpaca stamps a bar by its START; stored by END. A
    coin outside SPOT_PRODUCTS with no bars over an hour or more is
    remembered as unlisted for a day."""
    from data import alpaca_client
    pairs = {product_for(c): c for c in coins}
    bars = alpaca_client.get_crypto_bars(list(pairs), start=_iso(start_ts), end=_iso(end_ts),
                                         loc=alpaca_client.CHART_CRYPTO_LOC, max_pages=MAX_PAGES)
    if end_ts - start_ts >= 3600:
        for pair, coin in pairs.items():
            if coin not in SPOT_PRODUCTS and not bars.get(pair):
                _unlisted[coin] = time.time()
    frames = []
    for pair, rows in bars.items():
        if pair not in pairs or not rows:
            continue
        df = pd.DataFrame(rows).rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
        df["ts"] = ((pd.to_datetime(df["t"], utc=True) - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1)).astype("int64") + 60
        df["coin"] = pairs[pair]
        frames.append(df[(df["ts"] > start_ts) & (df["ts"] <= end_ts)])
    if not frames:
        return pd.DataFrame(columns=COLUMNS)
    out = pd.concat(frames, ignore_index=True)
    out["volume"] = out["volume"].fillna(0.0)
    return out[COLUMNS].drop_duplicates(["coin", "ts"]).sort_values(["coin", "ts"]).reset_index(drop=True)


def fetch_candles(coin: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    """One coin's 1-minute candles ending in (start_ts, end_ts]."""
    return fetch_many([coin], start_ts, end_ts)


def collect_since(since_ts: int, *, until_ts: int | None = None, coins: list[str] | None = None) -> pd.DataFrame:
    until_ts = int(until_ts or time.time())
    try:
        return fetch_many(coins or list(SPOT_PRODUCTS), since_ts, until_ts)
    except Exception as exc:
        logger.warning("[kalshi_15m_spot] candle fetch failed: %s", exc)
        return pd.DataFrame(columns=COLUMNS)


def _hf_download(path_in_repo: str) -> str | None:
    if not HF_API_KEY:
        return None

    def _download() -> str | None:
        from huggingface_hub import hf_hub_download
        try:
            return hf_hub_download(repo_id=HF_KALSHI_15M_DATASET_REPO, filename=path_in_repo, repo_type="dataset", token=HF_API_KEY)
        except Exception:
            return None

    from server_common import call_with_hard_timeout
    return call_with_hard_timeout(_download, timeout_sec=_HF_TIMEOUT_SEC)


def push_spot_history(df: pd.DataFrame) -> dict[str, Any]:
    """Merges into each UTC date's shard -- unioned with the shard already on
    HF -- and uploads it."""
    if df.empty:
        return {"ok": False, "reason": "no_rows"}
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    dates = pd.to_datetime(df["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    api = None
    if HF_API_KEY:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
    written, uploaded = [], []
    for date_str, part in df.groupby(dates):
        path_in_repo = f"spot_history/{date_str}.parquet"
        local_path = LOCAL_DIR / f"{date_str}.parquet"
        # Union with what HF already holds, every time (not only when the
        # local copy is missing): rows another writer added -- a gap backfill,
        # a run before a restart -- are kept, never overwritten. Local and
        # new rows win on duplicates.
        frames = [part]
        if local_path.exists():
            frames.insert(0, pd.read_parquet(local_path))
        remote = _hf_download(path_in_repo)
        if remote:
            frames.insert(0, pd.read_parquet(remote))
        combined = pd.concat(frames, ignore_index=True).drop_duplicates(["coin", "ts"], keep="last").sort_values(["coin", "ts"])
        tmp = local_path.with_suffix(".parquet.tmp")
        combined.to_parquet(tmp, index=False)
        os.replace(tmp, local_path)
        written.append(date_str)
        if api is not None:
            try:
                from server_common import call_with_hard_timeout
                call_with_hard_timeout(lambda lp=local_path, pir=path_in_repo, ds=date_str: api.upload_file(
                    path_or_fileobj=str(lp), path_in_repo=pir, repo_id=HF_KALSHI_15M_DATASET_REPO,
                    repo_type="dataset", commit_message=f"alpaca crypto 1m spot history {ds}"), timeout_sec=_HF_TIMEOUT_SEC * 2)
                uploaded.append(date_str)
            except Exception as exc:
                logger.warning("[kalshi_15m_spot] HF upload failed for %s: %s", date_str, exc)
    gc.collect()
    return {"ok": True, "rows": int(len(df)), "dates_written": written, "dates_uploaded": uploaded}


def run_incremental(*, lookback_hours: float = 2.0) -> dict[str, Any]:
    df = collect_since(int(time.time() - lookback_hours * 3600))
    result = push_spot_history(df)
    _merge_into_live_cache(df)
    return result


def backfill(*, days: int = 70, coins: list[str] | None = None) -> dict[str, Any]:
    """Day by day so memory stays bounded; `coins` limits it to some products
    (each day's shard merges with what HF already holds)."""
    now = int(time.time())
    totals: dict[str, Any] = {"ok": True, "rows": 0, "dates_written": [], "coins": coins or list(SPOT_PRODUCTS)}
    for day in range(days, 0, -1):
        start = now - day * 86400
        df = collect_since(start, until_ts=start + 86400, coins=coins)
        if df.empty:
            continue
        result = push_spot_history(df)
        totals["rows"] += result.get("rows", 0)
        totals["dates_written"] = sorted(set(totals["dates_written"]) | set(result.get("dates_written") or []))
    return totals


def list_hf_shard_dates() -> list[str]:
    if not HF_API_KEY:
        return []

    def _list() -> list[str]:
        from huggingface_hub import HfApi
        files = HfApi(token=HF_API_KEY).list_repo_files(repo_id=HF_KALSHI_15M_DATASET_REPO, repo_type="dataset")
        return sorted(m.group(1) for m in (_SHARD_RE.match(f) for f in files) if m)

    from server_common import call_with_hard_timeout
    return call_with_hard_timeout(_list, timeout_sec=_HF_TIMEOUT_SEC, on_timeout=[]) or []


def load_spot_history(*, days: int = 90) -> pd.DataFrame:
    dates = set(list_hf_shard_dates()[-days:])
    if LOCAL_DIR.exists():
        dates |= {p.stem for p in LOCAL_DIR.glob("*.parquet")}
    frames = []
    for date_str in sorted(dates)[-days:]:
        local_path = LOCAL_DIR / f"{date_str}.parquet"
        path = str(local_path) if local_path.exists() else _hf_download(f"spot_history/{date_str}.parquet")
        if path:
            try:
                frames.append(pd.read_parquet(path))
            except Exception as exc:
                logger.warning("[kalshi_15m_spot] could not read shard %s: %s", date_str, exc)
    if not frames:
        return pd.DataFrame(columns=COLUMNS)
    return pd.concat(frames, ignore_index=True).drop_duplicates(["coin", "ts"], keep="last").sort_values(["coin", "ts"])


def complete_minutes(df: pd.DataFrame) -> pd.DataFrame:
    """Per coin, a gap-free minute grid: no-trade minutes carry the previous
    close forward (open/high/low = that close) with zero volume."""
    out = []
    for coin, g in df.groupby("coin"):
        g = g.drop_duplicates("ts").set_index("ts").sort_index()
        grid = range(int(g.index.min()), int(g.index.max()) + 60, 60)
        g = g.reindex(grid)
        g["close"] = g["close"].ffill()
        for col in ("open", "high", "low"):
            g[col] = g[col].fillna(g["close"])
        g["volume"] = g["volume"].fillna(0.0)
        g["coin"] = coin
        out.append(g.rename_axis("ts").reset_index())
    return pd.concat(out, ignore_index=True)[COLUMNS] if out else pd.DataFrame(columns=COLUMNS)


def engineer_spot_features(df: pd.DataFrame) -> pd.DataFrame:
    """Multi-timeframe features on the complete minute grid, each row using
    only data at or before its own ts. Shared by training and live scoring."""
    full = complete_minutes(df)
    out = []
    for _, g in full.groupby("coin"):
        g = g.sort_values("ts").copy()
        c = g["close"].astype(float)
        for name, k in (("ret_5m", 5), ("ret_10m", 10), ("ret_15m", 15), ("ret_30m", 30), ("trend_1h", 60),
                        ("trend_2h", 120), ("trend_4h", 240), ("trend_8h", 480), ("trend_1d", 1440)):
            g[name] = c.pct_change(k, fill_method=None)
        r1 = c.pct_change(1, fill_method=None)
        g["volatility_15"] = r1.rolling(15).std()
        g["volatility_30"] = r1.rolling(30).std()
        dv = g["volume"].astype(float) * c
        sd = dv.rolling(60).std().replace(0, math.nan)
        g["dollar_volume_z"] = ((dv - dv.rolling(60).mean()) / sd).fillna(0.0)
        out.append(g)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=["coin", "ts"] + FEATURE_COLUMNS)


def _merge_into_live_cache(df: pd.DataFrame) -> None:
    if df.empty:
        return
    cutoff = int(time.time()) - LIVE_CACHE_HOURS * 3600
    with _cache_lock:
        for coin, part in df.groupby("coin"):
            prev = _live_cache.get(coin)
            merged = part if prev is None else pd.concat([prev, part], ignore_index=True)
            merged = merged.drop_duplicates(["coin", "ts"], keep="last").sort_values("ts")
            _live_cache[coin] = merged[merged["ts"] > cutoff].reset_index(drop=True)


def recent_series(coin: str) -> pd.DataFrame:
    """The last LIVE_CACHE_HOURS of 1-minute candles: the process cache
    (filled from Alpaca once) topped up with the newest 300 minutes and
    the stream's bars."""
    now = int(time.time())
    if not is_listed(coin):
        return pd.DataFrame(columns=COLUMNS)
    with _cache_lock:
        cached = _live_cache.get(coin)
        fresh = time.time() - _last_refresh.get(coin, 0.0) < LIVE_REFRESH_MIN_SEC
    if not (fresh and cached is not None and not cached.empty):
        # Several bots read the same coin within one cycle: one request per
        # LIVE_REFRESH_MIN_SEC (a still-forming minute is re-fetched after).
        with _cache_lock:
            _last_refresh[coin] = time.time()
        if cached is None or cached.empty or int(cached["ts"].min()) > now - (LIVE_CACHE_HOURS - 1) * 3600:
            _merge_into_live_cache(fetch_candles(coin, now - LIVE_CACHE_HOURS * 3600, now))
        else:
            _merge_into_live_cache(fetch_candles(coin, max(int(cached["ts"].max()) - 600, now - MAX_CANDLES_PER_CALL * 60), now))
    _merge_into_live_cache(stream_candles(coin))
    with _cache_lock:
        return _live_cache.get(coin, pd.DataFrame(columns=COLUMNS)).copy()


def stream_candles(coin: str) -> pd.DataFrame:
    """The Alpaca crypto stream's minute bars for the coin (same venue as
    the REST history), in this module's shape (ts = END)."""
    try:
        from data import alpaca_stream
        live = alpaca_stream.merge_live("crypto", product_for(coin), None)
    except Exception:
        return pd.DataFrame(columns=COLUMNS)
    if live is None or live.empty:
        return pd.DataFrame(columns=COLUMNS)
    out = live.copy()
    out["ts"] = out["ts"].astype("int64") + 60
    out["coin"] = coin
    return out[COLUMNS]


def live_underlying_row(coin: str, market: dict[str, Any], *, as_of_ts: int | None = None) -> dict[str, Any] | None:
    """Spot features at the candle ending at or before as_of_ts (default: the
    latest), plus the window's strike. None when there's no candle within 90
    seconds of as_of_ts -- stale data never scores a live entry."""
    if coin not in SPOT_PRODUCTS:
        return None
    series = recent_series(coin)
    if series.empty:
        return None
    feats = engineer_spot_features(series)
    if as_of_ts is not None:
        feats = feats[feats["ts"] <= as_of_ts]
        if feats.empty or int(feats["ts"].iloc[-1]) < as_of_ts - 90:
            return None
    last = feats.iloc[-1]
    row = {col: (None if pd.isna(last[col]) else float(last[col])) for col in FEATURE_COLUMNS}
    row["ts"] = int(last["ts"])
    strike = market.get("floor_strike")
    row["floor_strike"] = float(strike) if strike not in (None, "") else None
    return row


def grade_against_settlements(quotes: pd.DataFrame, spot: pd.DataFrame) -> dict[str, Any]:
    """How well this spot history reproduces Kalshi's own settlement values:
    per coin, median/95th-pct error (bps) vs floor_strike at open and
    expiration_value at close, and how often spot's close-vs-open direction
    matches the settled result."""
    import numpy as np

    w = quotes.drop_duplicates("ticker")[["coin", "ticker", "open_ts", "close_ts", "result", "floor_strike", "expiration_value"]]
    w = w[w.coin.isin(SPOT_PRODUCTS) & w.floor_strike.notna() & w.expiration_value.notna()].reset_index(drop=True)
    if w.empty or spot.empty:
        return {"ok": False, "reason": "nothing_to_grade"}
    s = spot[["coin", "ts", "close"]].rename(columns={"ts": "_ts"}).sort_values("_ts")
    s["_ts"] = s["_ts"].astype("int64")

    def at(col: str) -> pd.Series:
        t = w[["coin"]].assign(target=w[col].astype("int64"), _row=w.index).sort_values("target")
        m = pd.merge_asof(t, s, left_on="target", right_on="_ts", by="coin", direction="backward", tolerance=300)
        return m.set_index("_row")["close"].reindex(w.index)

    w["p_open"], w["p_close"] = at("open_ts"), at("close_ts")
    g = w.dropna(subset=["p_open", "p_close"]).copy()
    g["err_open_bps"] = (g.p_open / g.floor_strike - 1).abs() * 1e4
    g["err_close_bps"] = (g.p_close / g.expiration_value - 1).abs() * 1e4
    g["match"] = np.where(g.p_close > g.p_open, "yes", "no") == g.result
    per_coin = {
        coin: {
            "windows": int(len(p)), "coverage": round(len(p) / int((w.coin == coin).sum()), 4),
            "open_err_bps_median": round(float(p.err_open_bps.median()), 2),
            "close_err_bps_median": round(float(p.err_close_bps.median()), 2),
            "close_err_bps_p95": round(float(p.err_close_bps.quantile(0.95)), 2),
            "outcome_match": round(float(p.match.mean()), 4),
        }
        for coin, p in g.groupby("coin")
    }
    return {
        "ok": True, "windows": int(len(w)), "graded": int(len(g)),
        "outcome_match": round(float(g.match.mean()), 4) if len(g) else None,
        "close_err_bps_median": round(float(g.err_close_bps.median()), 2) if len(g) else None,
        "per_coin": per_coin,
    }


def _merge_days(coins: list[str], days: int) -> list[str]:
    """Fetch `days` of Alpaca history for `coins` day by day and merge each
    day into its local shard (unioned with HF's copy; the new Alpaca rows
    win minute for minute). Returns the dates touched."""
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    now = int(time.time())
    touched: list[str] = []
    for day in range(days, 0, -1):
        start = now - day * 86400
        df = collect_since(start, until_ts=start + 86400, coins=coins)
        if df.empty:
            continue
        for date_str, part in df.groupby(pd.to_datetime(df["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")):
            local_path = LOCAL_DIR / f"{date_str}.parquet"
            frames = [part]
            if local_path.exists():
                frames.insert(0, pd.read_parquet(local_path))
            else:
                remote = _hf_download(f"spot_history/{date_str}.parquet")
                if remote:
                    frames.insert(0, pd.read_parquet(remote))
            combined = pd.concat(frames, ignore_index=True).drop_duplicates(["coin", "ts"], keep="last").sort_values(["coin", "ts"])
            tmp = local_path.with_suffix(".parquet.tmp")
            combined.to_parquet(tmp, index=False)
            os.replace(tmp, local_path)
            if date_str not in touched:
                touched.append(date_str)
        gc.collect()
    return touched


def _upload_dates(dates: list[str], *, message: str, files_per_commit: int, extra: dict[str, bytes] | None = None) -> list[str]:
    """Upload local shards a few dozen per commit -- never one commit per
    day, which would eat the dataset repo's commit quota that every live
    collector shares. `extra` adds small files to the last commit."""
    uploaded: list[str] = []
    if not HF_API_KEY or not (dates or extra):
        return uploaded
    from huggingface_hub import CommitOperationAdd, HfApi

    from server_common import call_with_hard_timeout
    api = HfApi(token=HF_API_KEY)
    batches = [dates[i:i + files_per_commit] for i in range(0, len(dates), files_per_commit)] or [[]]
    for n, batch in enumerate(batches):
        ops = [CommitOperationAdd(f"spot_history/{d}.parquet", str(LOCAL_DIR / f"{d}.parquet")) for d in batch]
        if extra and n == len(batches) - 1:
            ops += [CommitOperationAdd(path, data) for path, data in extra.items()]
        label = f"{batch[0]}..{batch[-1]}" if batch else "marker"
        try:
            call_with_hard_timeout(lambda o=ops, lb=label: api.create_commit(
                repo_id=HF_KALSHI_15M_DATASET_REPO, repo_type="dataset", operations=o,
                commit_message=f"{message} {lb}"), timeout_sec=300)
            uploaded += batch
        except Exception as exc:
            logger.warning("[kalshi_15m_spot] upload failed for %s: %s", label, exc)
            return uploaded
    return uploaded


def backfill_missing_products(*, days: int = 70, files_per_commit: int = 25) -> dict[str, Any]:
    """Real history for any product the most recent HF shard doesn't carry
    yet (e.g. a coin just added to SPOT_PRODUCTS), all days merged locally
    first, then uploaded a few dozen shards per commit."""
    dates = list_hf_shard_dates()
    if not dates:
        return {"ok": False, "reason": "no_hf_history"}
    latest = load_spot_history(days=1)
    have = set(latest["coin"]) if not latest.empty else set()
    missing = [c for c in SPOT_PRODUCTS if c not in have]
    if not missing:
        return {"ok": True, "missing": []}
    touched = _merge_days(missing, days)
    uploaded = _upload_dates(touched, message=f"alpaca crypto 1m spot history backfill ({', '.join(missing)})",
                             files_per_commit=files_per_commit)
    return {"ok": True, "missing": missing, "dates_written": touched, "dates_uploaded": uploaded,
            "commits": (len(touched) + files_per_commit - 1) // files_per_commit}


def archive_source() -> dict[str, Any] | None:
    """The archive's SOURCE.json marker, or None (still Coinbase-era)."""
    path = _hf_download(SOURCE_MARKER)
    if not path:
        return None
    try:
        return json.loads(open(path, encoding="utf-8").read())
    except Exception:
        return None


def rebuild_from_alpaca(*, days: int | None = None, files_per_commit: int = 25) -> dict[str, Any]:
    """Once: re-read the whole archive window from Alpaca so backtests read
    the same source the bots trade on. Alpaca rows replace the Coinbase-era
    rows minute for minute; coins Alpaca doesn't list keep their old rows.
    Writes SOURCE.json last, so an interrupted rebuild runs again."""
    marker = archive_source()
    if marker and marker.get("source") == SOURCE:
        return {"ok": True, "action": "already_rebuilt", "marker": marker}
    dates = list_hf_shard_dates()
    if not dates:
        return {"ok": False, "reason": "no_hf_history"}
    if days is None:
        first = dt.datetime.strptime(dates[0], "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
        days = (dt.datetime.now(dt.timezone.utc) - first).days + 1
    touched = _merge_days(list(SPOT_PRODUCTS), days)
    info = {"source": SOURCE, "venue": "Kraken US via Alpaca (us-1)", "coins": sorted(SPOT_PRODUCTS),
            "rebuilt_at": dt.datetime.now(dt.timezone.utc).isoformat(), "days": days, "first_date": dates[0]}
    uploaded = _upload_dates(touched, message="alpaca crypto 1m spot history rebuild", files_per_commit=files_per_commit,
                             extra={SOURCE_MARKER: json.dumps(info, indent=2).encode()})
    ok = len(uploaded) == len(touched)
    return {"ok": ok, "days": days, "dates_written": len(touched), "dates_uploaded": len(uploaded)}
