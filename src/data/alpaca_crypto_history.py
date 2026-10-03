"""Multi-year 1-minute crypto history for every coin the Kalshi bots trade,
from Alpaca at the Kraken US location (alpaca_client.CHART_CRYPTO_LOC --
the same venue the live charts read), stored on HF and appended daily.

Alpaca's Kraken history starts in 2023. One parquet per coin per calendar
year in a private HF dataset:

    bars_1m/{COIN}/{YEAR}.parquet   ts (bar START, unix s), open, high,
                                    low, close, volume, trade_count, vwap

Fetched a month at a time (memory stays bounded); uploads go a few dozen
files per commit and always union with what HF already holds.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

HF_REPO = os.getenv("HF_ALPACA_CRYPTO_REPO", "papylove/alpaca-crypto-minute-bars")
ROOT_DIR = Path(__file__).resolve().parents[2]
LOCAL_DIR = Path(os.getenv("ALPACA_CRYPTO_HISTORY_DIR", str(ROOT_DIR / "data" / "alpaca_crypto_history")))
START_YEAR = int(os.getenv("ALPACA_CRYPTO_HISTORY_START_YEAR", "2022") or "2022")
FILES_PER_COMMIT = 40
COLUMNS = ["ts", "open", "high", "low", "close", "volume", "trade_count", "vwap"]


def universe() -> list[str]:
    """Every coin the Kalshi bots chart (kalshi_15m_spot.SPOT_PRODUCTS)."""
    from data import kalshi_15m_spot
    return sorted(kalshi_15m_spot.SPOT_PRODUCTS)


def _iso(t: dt.datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_bars(coin: str, start: dt.datetime, end: dt.datetime) -> pd.DataFrame:
    """1-minute bars for [start, end), a month per request series."""
    from data import alpaca_client, kalshi_15m_spot
    pair = kalshi_15m_spot.product_for(coin)
    frames = []
    t = start
    while t < end:
        nxt = min((t.replace(day=1) + dt.timedelta(days=32)).replace(day=1), end)
        rows = alpaca_client.get_crypto_bars([pair], start=_iso(t), end=_iso(nxt - dt.timedelta(seconds=1)),
                                             loc=alpaca_client.CHART_CRYPTO_LOC, max_pages=200).get(pair) or []
        if rows:
            frames.append(pd.DataFrame(rows))
        t = nxt
    if not frames:
        return pd.DataFrame(columns=COLUMNS)
    df = pd.concat(frames, ignore_index=True).rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume",
                                                               "n": "trade_count", "vw": "vwap"})
    df["ts"] = ((pd.to_datetime(df["t"], utc=True) - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1)).astype("int64")
    for col in COLUMNS:
        if col not in df:
            df[col] = float("nan")
    return df[COLUMNS].drop_duplicates("ts").sort_values("ts").reset_index(drop=True)


def _local_path(coin: str, year: int) -> Path:
    return LOCAL_DIR / "bars_1m" / coin / f"{year}.parquet"


def _repo_path(coin: str, year: int) -> str:
    return f"bars_1m/{coin}/{year}.parquet"


def _hf_token() -> str:
    return os.getenv("HF_API_KEY", "")


def _download(coin: str, year: int) -> pd.DataFrame | None:
    token = _hf_token()
    if not token:
        return None
    try:
        from huggingface_hub import hf_hub_download
        return pd.read_parquet(hf_hub_download(HF_REPO, _repo_path(coin, year), repo_type="dataset", token=token))
    except Exception:
        return None


def write_year(coin: str, year: int, new: pd.DataFrame, *, merge_remote: bool = True) -> Path:
    """Union new rows with the local and HF copies of that coin-year."""
    path = _local_path(coin, year)
    path.parent.mkdir(parents=True, exist_ok=True)
    frames = [new]
    if path.exists():
        frames.insert(0, pd.read_parquet(path))
    if merge_remote:
        remote = _download(coin, year)
        if remote is not None:
            frames.insert(0, remote)
    combined = pd.concat(frames, ignore_index=True).drop_duplicates("ts", keep="last").sort_values("ts").reset_index(drop=True)
    tmp = path.with_suffix(".parquet.tmp")
    combined.to_parquet(tmp, index=False)
    os.replace(tmp, path)
    return path


def upload(paths: list[tuple[str, int]], *, message: str) -> list[tuple[str, int]]:
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
        ops = [CommitOperationAdd(_repo_path(c, y), str(_local_path(c, y))) for c, y in batch]
        try:
            call_with_hard_timeout(lambda o=ops: api.create_commit(repo_id=HF_REPO, repo_type="dataset", operations=o,
                                                                    commit_message=f"{message} ({len(o)} files)"),
                                   timeout_sec=600)
            done += batch
        except Exception as exc:
            logger.warning("[alpaca_crypto_history] upload failed for %d files: %s", len(batch), exc)
    return done


def backfill(coins: list[str] | None = None, *, start_year: int = START_YEAR) -> dict[str, Any]:
    """Full history per coin, year by year."""
    coins = coins or universe()
    now = dt.datetime.now(dt.timezone.utc)
    written: list[tuple[str, int]] = []
    rows = 0
    for coin in coins:
        for year in range(start_year, now.year + 1):
            start = dt.datetime(year, 1, 1, tzinfo=dt.timezone.utc)
            end = min(dt.datetime(year + 1, 1, 1, tzinfo=dt.timezone.utc), now)
            try:
                df = fetch_bars(coin, start, end)
            except Exception as exc:
                logger.warning("[alpaca_crypto_history] %s %s failed: %s", coin, year, exc)
                continue
            if df.empty:
                continue
            write_year(coin, year, df)
            written.append((coin, year))
            rows += len(df)
    uploaded = upload(written, message="Alpaca Kraken US 1m bars backfill")
    return {"ok": True, "coins": len(coins), "files": len(written), "rows": rows, "uploaded": len(uploaded)}


def missing_coins() -> list[str]:
    token = _hf_token()
    if not token:
        return []
    from huggingface_hub import HfApi
    try:
        files = set(HfApi(token=token).list_repo_files(HF_REPO, repo_type="dataset"))
    except Exception:
        files = set()  # the repo doesn't exist yet: every coin is missing
    year = dt.datetime.now(dt.timezone.utc).year
    return [c for c in universe() if _repo_path(c, year) not in files]


def backfill_missing() -> dict[str, Any]:
    missing = missing_coins()
    if not missing:
        return {"ok": True, "missing": []}
    return {**backfill(missing), "missing": missing}


def append_recent(coins: list[str] | None = None, *, days: int = 3) -> dict[str, Any]:
    """Daily top-up: the last few days for every coin, merged into HF."""
    coins = coins or universe()
    now = dt.datetime.now(dt.timezone.utc)
    written: list[tuple[str, int]] = []
    for coin in coins:
        try:
            df = fetch_bars(coin, now - dt.timedelta(days=days), now)
        except Exception as exc:
            logger.warning("[alpaca_crypto_history] append %s failed: %s", coin, exc)
            continue
        if df.empty:
            continue
        years = pd.to_datetime(df["ts"], unit="s", utc=True).dt.year
        for year, part in df.groupby(years):
            write_year(coin, int(year), part)
            written.append((coin, int(year)))
    uploaded = upload(written, message=f"Alpaca Kraken US 1m bars {now.date()}")
    return {"ok": True, "files": len(written), "uploaded": len(uploaded)}


def archive_ready(*, min_fraction: float = 0.98) -> bool:
    """True once this year's file is on HF for (nearly) every coin."""
    token = _hf_token()
    if not token:
        return False
    try:
        from huggingface_hub import HfApi
        files = set(HfApi(token=token).list_repo_files(HF_REPO, repo_type="dataset"))
    except Exception:
        return False
    year = dt.datetime.now(dt.timezone.utc).year
    coins = universe()
    return bool(coins) and sum(1 for c in coins if _repo_path(c, year) in files) >= min_fraction * len(coins)


def load(coin: str, *, years: list[int] | None = None) -> pd.DataFrame:
    """A coin's stored 1-minute bars (ts = bar START): local copy, else HF."""
    now_year = dt.datetime.now(dt.timezone.utc).year
    frames = []
    for year in years or list(range(START_YEAR, now_year + 1)):
        path = _local_path(coin, year)
        if path.exists():
            frames.append(pd.read_parquet(path))
            continue
        remote = _download(coin, year)
        if remote is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(f".{os.getpid()}.tmp")  # study workers read the same coin at once
            remote.to_parquet(tmp, index=False)
            os.replace(tmp, path)
            frames.append(remote)
    if not frames:
        return pd.DataFrame(columns=COLUMNS)
    return pd.concat(frames, ignore_index=True).drop_duplicates("ts").sort_values("ts").reset_index(drop=True)


def candles(coin: str, *, years: list[int] | None = None) -> pd.DataFrame:
    """The coin's history in the setup modules' shape (ts = candle END)."""
    df = load(coin, years=years)
    if df.empty:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    out = df[["ts", "open", "high", "low", "close", "volume"]].copy()
    out["ts"] = out["ts"].astype("int64") + 60
    out["volume"] = out["volume"].fillna(0.0)
    return out
