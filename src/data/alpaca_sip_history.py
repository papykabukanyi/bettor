"""Consolidated (SIP) 1-minute bar history for every US stock the Alpaca
bots trade or lead with, stored on HF and appended daily.

Source: Alpaca market data, feed=sip (all US exchanges, consolidated volume
-- the account's Algo Trader Plus plan), split-adjusted, from 2016. One
parquet per symbol per calendar year in a private HF dataset:

    bars_1m/{SYMBOL}/{YEAR}.parquet   ts (bar START, unix s), open, high,
                                      low, close, volume, trade_count, vwap

Every bar Alpaca reports is kept (pre/post-market included); consumers pick
the regular session. Uploads go a few dozen files per commit -- never one
commit per file, which would eat the repo's commit quota -- and always
union with what HF already holds, so no writer overwrites another's rows.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)

HF_REPO = os.getenv("HF_ALPACA_SIP_REPO", "papylove/alpaca-sip-minute-bars")
DATA_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
ROOT_DIR = Path(__file__).resolve().parents[2]
LOCAL_DIR = Path(os.getenv("ALPACA_SIP_HISTORY_DIR", str(ROOT_DIR / "data" / "alpaca_sip_history")))
START_YEAR = int(os.getenv("ALPACA_SIP_HISTORY_START_YEAR", "2016") or "2016")
FILES_PER_COMMIT = 60
PAGE_LIMIT = 10000
COLUMNS = ["ts", "open", "high", "low", "close", "volume", "trade_count", "vwap"]


def commodity_etfs() -> list[str]:
    """The ETFs the Kalshi bots chart commodities on (GLD, SLV, CPER, ...)."""
    from data import kalshi_15m_setup
    return sorted(set(kalshi_15m_setup.METAL_CHART_SYMBOL.values()))


def universe() -> list[str]:
    """Every symbol the stock and options bots can trade, plus the leaders
    and the Kalshi bots' commodity ETFs."""
    from data import alpaca_data, alpaca_options_data
    return sorted(set(alpaca_data.BROAD_CANDIDATE_UNIVERSE) | set(alpaca_options_data.OPTIONS_UNDERLYINGS) | {"SPY", "QQQ"}
                  | set(commodity_etfs()))


def _headers(key_id: str | None = None, secret: str | None = None) -> dict[str, str]:
    return {"APCA-API-KEY-ID": key_id or os.getenv("ALPACA_API_KEY_ID", ""),
            "APCA-API-SECRET-KEY": secret or os.getenv("ALPACA_API_SECRET_KEY", "")}


def fetch_bars(symbol: str, start: dt.datetime, end: dt.datetime, *, headers: dict[str, str] | None = None) -> pd.DataFrame:
    """SIP 1-minute bars for [start, end), paginated 10,000 per request."""
    params: dict[str, Any] = {"timeframe": "1Min", "start": start.isoformat(), "end": end.isoformat(), "feed": "sip",
                              "adjustment": "split", "limit": PAGE_LIMIT}
    rows: list[dict[str, Any]] = []
    while True:
        for attempt in range(6):
            resp = requests.get(f"{DATA_URL}/v2/stocks/{symbol}/bars", headers=headers or _headers(), params=params, timeout=60)
            if resp.status_code == 429:
                time.sleep(2 + attempt * 2)
                continue
            resp.raise_for_status()
            break
        body = resp.json()
        rows += body.get("bars") or []
        token = body.get("next_page_token")
        if not token:
            break
        params["page_token"] = token
    if not rows:
        return pd.DataFrame(columns=COLUMNS)
    df = pd.DataFrame(rows).rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume",
                                            "n": "trade_count", "vw": "vwap"})
    # Unix seconds independent of the datetime resolution pandas parses to
    # (microseconds here, not nanoseconds).
    df["ts"] = ((pd.to_datetime(df["t"], utc=True) - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1)).astype("int64")
    for col in COLUMNS:
        if col not in df:
            df[col] = float("nan")
    return df[COLUMNS].drop_duplicates("ts").sort_values("ts").reset_index(drop=True)


def _local_path(symbol: str, year: int) -> Path:
    return LOCAL_DIR / "bars_1m" / symbol / f"{year}.parquet"


def _repo_path(symbol: str, year: int) -> str:
    return f"bars_1m/{symbol}/{year}.parquet"


def _hf_token() -> str:
    return os.getenv("HF_API_KEY", "")


def _download(symbol: str, year: int) -> pd.DataFrame | None:
    token = _hf_token()
    if not token:
        return None
    try:
        from huggingface_hub import hf_hub_download
        return pd.read_parquet(hf_hub_download(HF_REPO, _repo_path(symbol, year), repo_type="dataset", token=token))
    except Exception:
        return None


def write_year(symbol: str, year: int, new: pd.DataFrame, *, merge_remote: bool = True) -> Path:
    """Union new rows with the local and HF copies of that symbol-year."""
    path = _local_path(symbol, year)
    path.parent.mkdir(parents=True, exist_ok=True)
    frames = [new]
    if path.exists():
        frames.insert(0, pd.read_parquet(path))
    if merge_remote:
        remote = _download(symbol, year)
        if remote is not None:
            frames.insert(0, remote)
    combined = pd.concat(frames, ignore_index=True).drop_duplicates("ts", keep="last").sort_values("ts").reset_index(drop=True)
    tmp = path.with_suffix(".parquet.tmp")
    combined.to_parquet(tmp, index=False)
    os.replace(tmp, path)
    return path


def upload(paths: list[tuple[str, int]], *, message: str) -> list[tuple[str, int]]:
    """Upload symbol-year files, FILES_PER_COMMIT per commit."""
    token = _hf_token()
    if not token or not paths:
        return []
    from huggingface_hub import CommitOperationAdd, HfApi

    from server_common import call_with_hard_timeout
    api = HfApi(token=token)
    api.create_repo(HF_REPO, repo_type="dataset", private=True, exist_ok=True)
    done: list[tuple[str, int]] = []
    for i in range(0, len(paths), FILES_PER_COMMIT):
        batch = paths[i:i + FILES_PER_COMMIT]
        ops = [CommitOperationAdd(_repo_path(s, y), str(_local_path(s, y))) for s, y in batch]
        try:
            call_with_hard_timeout(lambda o=ops: api.create_commit(repo_id=HF_REPO, repo_type="dataset", operations=o,
                                                                    commit_message=f"{message} ({len(o)} files)"),
                                   timeout_sec=600)
            done += batch
        except Exception as exc:
            logger.warning("[alpaca_sip_history] upload failed for %d files: %s", len(batch), exc)
    return done


def backfill(symbols: list[str] | None = None, *, start_year: int = START_YEAR, headers: dict[str, str] | None = None,
             merge_remote: bool = True, publish: bool = True) -> dict[str, Any]:
    """Full history per symbol, year by year (memory stays bounded)."""
    from server_common import task_end, task_start, task_step
    symbols = symbols or universe()
    now = dt.datetime.now(dt.timezone.utc)
    written: list[tuple[str, int]] = []
    rows = 0
    task_start("SIP minute bars", len(symbols), detail=f"every exchange, {start_year} to today")
    for k, symbol in enumerate(symbols):
        task_step("SIP minute bars", done=k, current=symbol)
        for year in range(start_year, now.year + 1):
            start = dt.datetime(year, 1, 1, tzinfo=dt.timezone.utc)
            end = min(dt.datetime(year + 1, 1, 1, tzinfo=dt.timezone.utc), now)
            try:
                df = fetch_bars(symbol, start, end, headers=headers)
            except Exception as exc:
                logger.warning("[alpaca_sip_history] %s %s failed: %s", symbol, year, exc)
                continue
            if df.empty:
                continue
            write_year(symbol, year, df, merge_remote=merge_remote)
            written.append((symbol, year))
            rows += len(df)
    uploaded = upload(written, message="SIP 1m bars backfill") if publish else []
    task_end("SIP minute bars", detail=f"{len(written)} symbol-years, {rows:,} bars")
    return {"ok": True, "symbols": len(symbols), "files": len(written), "rows": rows, "uploaded": len(uploaded)}


def missing_symbols() -> list[str]:
    """Universe symbols with no file for this year on HF yet."""
    token = _hf_token()
    if not token:
        return []
    from huggingface_hub import HfApi
    files = set(HfApi(token=token).list_repo_files(HF_REPO, repo_type="dataset"))
    year = dt.datetime.now(dt.timezone.utc).year
    return [s for s in universe() if _repo_path(s, year) not in files]


def backfill_missing() -> dict[str, Any]:
    """Full history for any symbol just added to the universe (e.g. a new
    commodity ETF); a no-op once every symbol has its files."""
    missing = missing_symbols()
    if not missing:
        return {"ok": True, "missing": []}
    return {**backfill(missing), "missing": missing}


def append_recent(symbols: list[str] | None = None, *, days: int = 4) -> dict[str, Any]:
    """Daily top-up on the Space: the last few days for every symbol, merged
    into the HF year files."""
    symbols = symbols or universe()
    now = dt.datetime.now(dt.timezone.utc)
    start = now - dt.timedelta(days=days)
    written: list[tuple[str, int]] = []
    for symbol in symbols:
        try:
            df = fetch_bars(symbol, start, now)
        except Exception as exc:
            logger.warning("[alpaca_sip_history] append %s failed: %s", symbol, exc)
            continue
        if df.empty:
            continue
        years = pd.to_datetime(df["ts"], unit="s", utc=True).dt.year
        for year, part in df.groupby(years):
            write_year(symbol, int(year), part)
            written.append((symbol, int(year)))
    uploaded = upload(written, message=f"SIP 1m bars {now.date()}")
    return {"ok": True, "files": len(written), "uploaded": len(uploaded)}


def load(symbol: str, *, years: list[int] | None = None) -> pd.DataFrame:
    """A symbol's stored 1-minute bars (ts = bar START): local copy, else HF."""
    now_year = dt.datetime.now(dt.timezone.utc).year
    frames = []
    for year in years or list(range(START_YEAR, now_year + 1)):
        path = _local_path(symbol, year)
        df = pd.read_parquet(path) if path.exists() else _download(symbol, year)
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=COLUMNS)
    return pd.concat(frames, ignore_index=True).drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
