"""Real 1-minute spot history for the crypto underlyings, from Coinbase
Exchange's public candles API (no key). Coinbase is one of the exchanges CF
Benchmarks' Real-Time Index -- what every KX*15M crypto contract settles on
-- is built from.

Validated against Kalshi's own published settlement values (floor_strike at
window open, expiration_value at close) on 15,936 real windows: 100%
coverage, 2.87 bps median error, direction of spot close-vs-open matching
the settled result 93.5% of the time (the rest are windows that finished
within a couple of bps of the strike, where the index's 60-second
multi-exchange average decides). The Kalshi perp archive this replaces for
the edge model covered only 75% of those windows (HYPE/NEAR/ZEC under 50%)
at 3.03 bps / 92.3%.

Stored as spot_history/{UTC date}.parquet (coin, ts = candle END, open,
high, low, close, volume) in the 15m dataset repo. Coinbase omits minutes
with no trades; complete_minutes() forward-fills those with zero volume.
"""
from __future__ import annotations

import gc
import logging
import math
import os
import re
import threading
import time
from typing import Any

import pandas as pd
import requests

from server_common import DATA_DIR

logger = logging.getLogger(__name__)

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_KALSHI_15M_DATASET_REPO = os.getenv("HF_KALSHI_15M_DATASET_REPO", "papylove/kalshi-15m-data")

COINBASE_URL = "https://api.exchange.coinbase.com/products/{product}/candles"
COINBASE_PRODUCTS = {
    "BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD", "XRP": "XRP-USD", "DOGE": "DOGE-USD",
    "BCH": "BCH-USD", "NEAR": "NEAR-USD", "HYPE": "HYPE-USD", "ZEC": "ZEC-USD",
    # Perp-only underlyings (no Kalshi 15m series), for perps_spot_lead.
    "LINK": "LINK-USD", "LTC": "LTC-USD", "SUI": "SUI-USD", "ADA": "ADA-USD", "AAVE": "AAVE-USD", "BNB": "BNB-USD",
    # Kalshi perps that had no chart before (all confirmed live on Coinbase).
    "DOT": "DOT-USD", "HBAR": "HBAR-USD", "XLM": "XLM-USD", "SHIB": "SHIB-USD",
}
# A traded symbol whose chart is another product's (kSHIB perp = 1,000 SHIB;
# price units differ, the chart's shape doesn't).
CHART_ALIASES = {"KSHIB": "SHIB"}


def chart_coin(coin: str) -> str:
    return CHART_ALIASES.get(coin, coin)
MAX_CANDLES_PER_CALL = 300
REQUEST_SPACING_SEC = 0.12
LOCAL_DIR = DATA_DIR / "kalshi_15m_spot_history"
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


def fetch_candles(coin: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    """1-minute candles for [start_ts, end_ts), paginated 300 per request."""
    product = COINBASE_PRODUCTS[coin]
    rows: list[list[float]] = []
    t = int(start_ts)
    while t < end_ts:
        t2 = min(t + MAX_CANDLES_PER_CALL * 60, int(end_ts))
        for attempt in range(5):
            resp = requests.get(COINBASE_URL.format(product=product), params={"granularity": 60, "start": t, "end": t2},
                                headers={"User-Agent": "bettor-kalshi-15m"}, timeout=30)
            if resp.status_code == 429:
                time.sleep(1.0 + attempt)
                continue
            resp.raise_for_status()
            rows += resp.json()
            break
        t = t2
        time.sleep(REQUEST_SPACING_SEC)
    if not rows:
        return pd.DataFrame(columns=COLUMNS)
    df = pd.DataFrame(rows, columns=["start", "low", "high", "open", "close", "volume"])
    df["ts"] = df["start"].astype("int64") + 60
    df["coin"] = coin
    df = df[(df["ts"] > start_ts) & (df["ts"] <= end_ts)]
    return df[COLUMNS].drop_duplicates(["coin", "ts"]).sort_values("ts").reset_index(drop=True)


def collect_since(since_ts: int, *, until_ts: int | None = None, coins: list[str] | None = None) -> pd.DataFrame:
    until_ts = int(until_ts or time.time())
    frames = []
    for coin in coins or list(COINBASE_PRODUCTS):
        try:
            frames.append(fetch_candles(coin, since_ts, until_ts))
        except Exception as exc:
            logger.warning("[kalshi_15m_spot] candle fetch failed for %s: %s", coin, exc)
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS)


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
    """Merges into each UTC date's shard (seeded from HF when the local copy
    is missing, e.g. after a restart) and uploads it."""
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
        frames = [part]
        if local_path.exists():
            frames.insert(0, pd.read_parquet(local_path))
        else:
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
                api.upload_file(path_or_fileobj=str(local_path), path_in_repo=path_in_repo, repo_id=HF_KALSHI_15M_DATASET_REPO,
                                repo_type="dataset", commit_message=f"coinbase 1m spot history {date_str}")
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
    totals: dict[str, Any] = {"ok": True, "rows": 0, "dates_written": [], "coins": coins or list(COINBASE_PRODUCTS)}
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
    (filled from HF/Coinbase once) topped up with the newest 300 minutes."""
    now = int(time.time())
    with _cache_lock:
        cached = _live_cache.get(coin)
    if cached is None or cached.empty or int(cached["ts"].min()) > now - (LIVE_CACHE_HOURS - 1) * 3600:
        _merge_into_live_cache(fetch_candles(coin, now - LIVE_CACHE_HOURS * 3600, now))
    else:
        _merge_into_live_cache(fetch_candles(coin, max(int(cached["ts"].max()) - 600, now - MAX_CANDLES_PER_CALL * 60), now))
    with _cache_lock:
        return _live_cache.get(coin, pd.DataFrame(columns=COLUMNS)).copy()


def live_underlying_row(coin: str, market: dict[str, Any], *, as_of_ts: int | None = None) -> dict[str, Any] | None:
    """Spot features at the candle ending at or before as_of_ts (default: the
    latest), plus the window's strike. None when there's no candle within 90
    seconds of as_of_ts -- stale data never scores a live entry."""
    if coin not in COINBASE_PRODUCTS:
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
    w = w[w.coin.isin(COINBASE_PRODUCTS) & w.floor_strike.notna() & w.expiration_value.notna()].reset_index(drop=True)
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


def backfill_missing_products(*, days: int = 70) -> dict[str, Any]:
    """Real history for any product the most recent HF shard doesn't carry
    yet (e.g. a coin just added to COINBASE_PRODUCTS)."""
    dates = list_hf_shard_dates()
    if not dates:
        return {"ok": False, "reason": "no_hf_history"}
    latest = load_spot_history(days=1)
    have = set(latest["coin"]) if not latest.empty else set()
    missing = [c for c in COINBASE_PRODUCTS if c not in have]
    if not missing:
        return {"ok": True, "missing": []}
    return {**backfill(days=days, coins=missing), "missing": missing}
