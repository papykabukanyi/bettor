"""Kalshi 15-minute contract quote history: every settled window's own
per-minute yes_bid / yes_ask / last / volume / open interest, plus the real
settlement result, for every crypto and metals series.

This is the dataset the edge model is judged against. The model-direction
backtests elsewhere fill at a fixed assumed price; real entries pay Kalshi's
ask, and Kalshi's quote already prices in most of what a technical model can
see (22 days / 21,808 real windows: the mid is calibrated to within ~1-2
points in every elapsed-minute bucket). Any claimed edge has to beat this
history at real asks after fees.

Public, unauthenticated endpoints only:
    GET /markets?series_ticker=X&status=settled&min_close_ts=T  (paginated)
    GET /markets/candlesticks?market_tickers=...&start_ts&end_ts&period_interval=1
        -- capped at 10,000 candles per call, counted as tickers x the full
        requested span, so windows are batched a few opens at a time.

Stored as quote_history/{window-open UTC date}.parquet in the 15m dataset
repo, deduped on (ticker, minute).
"""
from __future__ import annotations

import datetime as dt
import gc
import logging
import os
import re
import time
from collections import defaultdict
from typing import Any

import pandas as pd

from data import kalshi_15m
from data.kalshi_client import _request_json
from server_common import DATA_DIR

logger = logging.getLogger(__name__)

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_KALSHI_15M_DATASET_REPO = os.getenv("HF_KALSHI_15M_DATASET_REPO", "papylove/kalshi-15m-data")

SERIES: dict[str, str] = {**kalshi_15m.KNOWN_15M_SERIES, **kalshi_15m.KNOWN_15M_METALS_SERIES}
WINDOW_SECONDS = 900
MAX_CANDLES_PER_CALL = 10000
MAX_TICKERS_PER_CALL = 100
LOCAL_DIR = DATA_DIR / "kalshi_15m_quote_history"
_SHARD_RE = re.compile(r"^quote_history/(\d{4}-\d{2}-\d{2})\.parquet$")
_HF_TIMEOUT_SEC = 30

COLUMNS = [
    "coin", "series", "ticker", "open_ts", "close_ts", "end_period_ts", "minute",
    "yes_bid", "yes_ask", "last", "mean", "volume", "open_interest",
    "result", "floor_strike", "expiration_value",
]


def _iso_ts(value: str) -> int:
    return int(dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())


def _num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def fetch_settled_markets(series_ticker: str, since_ts: int, until_ts: int | None = None) -> list[dict[str, Any]]:
    markets: list[dict[str, Any]] = []
    cursor = None
    while True:
        params: dict[str, Any] = {"series_ticker": series_ticker, "status": "settled", "limit": 1000, "min_close_ts": int(since_ts)}
        if until_ts is not None:
            params["max_close_ts"] = int(until_ts)
        if cursor:
            params["cursor"] = cursor
        data = _request_json("GET", "/markets", params=params)
        page = data.get("markets") or []
        markets += [m for m in page if m.get("result") in ("yes", "no") and _iso_ts(m["close_time"]) >= since_ts]
        cursor = data.get("cursor")
        if not page or not cursor or _iso_ts(page[-1]["close_time"]) < since_ts:
            return markets
        time.sleep(0.2)


def _batches(opens: list[int], tickers_by_open: dict[int, list[str]]) -> list[list[int]]:
    """Consecutive window opens grouped so tickers x span stays under the cap."""
    groups: list[list[int]] = []
    i = 0
    while i < len(opens):
        k = 1
        while i + k < len(opens):
            group = opens[i:i + k + 1]
            n_tickers = sum(len(tickers_by_open[o]) for o in group)
            span_minutes = (group[-1] + WINDOW_SECONDS - group[0]) // 60
            if n_tickers * span_minutes > MAX_CANDLES_PER_CALL or n_tickers > MAX_TICKERS_PER_CALL:
                break
            k += 1
        groups.append(opens[i:i + k])
        i += k
    return groups


def candles_to_rows(meta: dict[str, dict[str, Any]], payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in payload.get("markets") or []:
        ticker = entry.get("market_ticker") or entry.get("ticker")
        market = meta.get(ticker)
        if market is None:
            continue
        open_ts, close_ts = _iso_ts(market["open_time"]), _iso_ts(market["close_time"])
        for candle in entry.get("candlesticks") or []:
            end_ts = int(candle["end_period_ts"])
            if end_ts <= open_ts or end_ts > close_ts:
                continue
            yes_bid, yes_ask, price = candle.get("yes_bid") or {}, candle.get("yes_ask") or {}, candle.get("price") or {}
            rows.append({
                "coin": market["_coin"], "series": market["_series"], "ticker": ticker,
                "open_ts": open_ts, "close_ts": close_ts, "end_period_ts": end_ts, "minute": (end_ts - open_ts) // 60,
                "yes_bid": _num(yes_bid.get("close_dollars")), "yes_ask": _num(yes_ask.get("close_dollars")),
                "last": _num(price.get("close_dollars")), "mean": _num(price.get("mean_dollars")),
                "volume": _num(candle.get("volume_fp")) or 0.0, "open_interest": _num(candle.get("open_interest_fp")) or 0.0,
                "result": market["result"], "floor_strike": _num(market.get("floor_strike")),
                "expiration_value": _num(market.get("expiration_value")),
            })
    return rows


def collect_since(since_ts: int, *, until_ts: int | None = None, series: dict[str, str] | None = None) -> pd.DataFrame:
    meta: dict[str, dict[str, Any]] = {}
    for coin, series_ticker in (series or SERIES).items():
        try:
            for market in fetch_settled_markets(series_ticker, since_ts, until_ts):
                meta[market["ticker"]] = {**market, "_coin": coin, "_series": series_ticker}
        except Exception as exc:
            logger.warning("[kalshi_15m_quotes] settled-market listing failed for %s: %s", series_ticker, exc)
    tickers_by_open: dict[int, list[str]] = defaultdict(list)
    for ticker, market in meta.items():
        tickers_by_open[_iso_ts(market["open_time"])].append(ticker)
    opens = sorted(tickers_by_open)

    rows: list[dict[str, Any]] = []
    for group in _batches(opens, tickers_by_open):
        tickers = [t for o in group for t in tickers_by_open[o]]
        try:
            payload = _request_json("GET", "/markets/candlesticks", params={
                "market_tickers": ",".join(tickers), "start_ts": group[0],
                "end_ts": group[-1] + WINDOW_SECONDS, "period_interval": 1,
            })
        except Exception as exc:
            logger.warning("[kalshi_15m_quotes] candle batch failed at open %s: %s", group[0], exc)
            continue
        rows += candles_to_rows(meta, payload)
        time.sleep(0.15)
    return pd.DataFrame(rows, columns=COLUMNS)


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


def push_quote_history(df: pd.DataFrame) -> dict[str, Any]:
    """Merges new rows into each window-open date's shard (seeded from HF
    when the local copy is missing, e.g. after a restart) and uploads it."""
    if df.empty:
        return {"ok": False, "reason": "no_rows"}
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    dates = pd.to_datetime(df["open_ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    written, uploaded = [], []
    api = None
    if HF_API_KEY:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
    for date_str, part in df.groupby(dates):
        path_in_repo = f"quote_history/{date_str}.parquet"
        local_path = LOCAL_DIR / f"{date_str}.parquet"
        frames = [part]
        if local_path.exists():
            frames.insert(0, pd.read_parquet(local_path))
        else:
            remote = _hf_download(path_in_repo)
            if remote:
                frames.insert(0, pd.read_parquet(remote))
        combined = pd.concat(frames, ignore_index=True).drop_duplicates(["ticker", "minute"], keep="last")
        combined = combined.sort_values(["open_ts", "coin", "minute"]).reset_index(drop=True)
        tmp = local_path.with_suffix(".parquet.tmp")
        combined.to_parquet(tmp, index=False)
        os.replace(tmp, local_path)
        written.append(date_str)
        if api is not None:
            try:
                api.upload_file(
                    path_or_fileobj=str(local_path), path_in_repo=path_in_repo,
                    repo_id=HF_KALSHI_15M_DATASET_REPO, repo_type="dataset",
                    commit_message=f"kalshi 15m quote history {date_str}",
                )
                uploaded.append(date_str)
            except Exception as exc:
                logger.warning("[kalshi_15m_quotes] HF upload failed for %s: %s", date_str, exc)
        del combined, frames
    gc.collect()
    return {"ok": True, "rows": int(len(df)), "dates_written": written, "dates_uploaded": uploaded}


def run_incremental(*, lookback_hours: float = 4.0) -> dict[str, Any]:
    since = int(time.time() - lookback_hours * 3600)
    df = collect_since(since)
    result = push_quote_history(df)
    result["windows"] = int(df["ticker"].nunique()) if not df.empty else 0
    return result


def backfill(*, days: int = 21) -> dict[str, Any]:
    """Day-by-day so a long backfill never holds every row in memory."""
    now = int(time.time())
    totals = {"ok": True, "rows": 0, "windows": 0, "dates_written": []}
    for day in range(days, 0, -1):
        start = now - day * 86400
        df = collect_since(start, until_ts=start + 86400)
        if df.empty:
            continue
        result = push_quote_history(df)
        totals["rows"] += result.get("rows", 0)
        totals["windows"] += int(df["ticker"].nunique())
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


def load_quote_history(*, days: int = 45) -> pd.DataFrame:
    """Local shards plus the HF archive for the most recent `days` dates."""
    dates = set(list_hf_shard_dates()[-days:])
    if LOCAL_DIR.exists():
        dates |= {p.stem for p in LOCAL_DIR.glob("*.parquet")}
    frames = []
    for date_str in sorted(dates)[-days:]:
        local_path = LOCAL_DIR / f"{date_str}.parquet"
        path = str(local_path) if local_path.exists() else _hf_download(f"quote_history/{date_str}.parquet")
        if not path:
            continue
        try:
            frames.append(pd.read_parquet(path))
        except Exception as exc:
            logger.warning("[kalshi_15m_quotes] could not read shard %s: %s", date_str, exc)
    if not frames:
        return pd.DataFrame(columns=COLUMNS)
    return pd.concat(frames, ignore_index=True).drop_duplicates(["ticker", "minute"], keep="last")


def archive_summary() -> dict[str, Any]:
    dates = list_hf_shard_dates()
    return {"hf_shard_dates": len(dates), "first_date": dates[0] if dates else None, "last_date": dates[-1] if dates else None}
