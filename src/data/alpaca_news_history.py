"""Historical news for what the Kalshi bots trade, from Alpaca (Benzinga),
stored on HF so the studies can see what the news said at every moment.

Every article tagged with a Kalshi coin (BTCUSD, ETHUSD, ...), a commodity
ETF (GLD, USO, ...) or a market leader (SPY, QQQ), from START_YEAR. One
parquet per month in a private HF dataset:

    news/{YYYY-MM}.parquet   id, created_at (unix s), headline, summary,
                             symbols (comma-joined), source, url, score

score is the headline+summary sentiment in [-1, 1] (the same word lists
the live news rule uses), fixed when stored. sentiment_at() reads the
archive without lookahead: only articles created at or before the moment.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

HF_REPO = os.getenv("HF_ALPACA_NEWS_REPO", "papylove/alpaca-news-archive")
ROOT_DIR = Path(__file__).resolve().parents[2]
LOCAL_DIR = Path(os.getenv("ALPACA_NEWS_HISTORY_DIR", str(ROOT_DIR / "data" / "alpaca_news_history")))
START_YEAR = int(os.getenv("ALPACA_NEWS_HISTORY_START_YEAR", "2016") or "2016")
FILES_PER_COMMIT = 24
SYMBOLS_PER_REQUEST = 40
COLUMNS = ["id", "created_at", "headline", "summary", "symbols", "source", "url", "score"]


def universe() -> list[str]:
    """News tickers for every Kalshi coin and commodity, plus SPY/QQQ."""
    from data import alpaca_news, kalshi_15m_setup, kalshi_15m_spot
    syms = {f"{c}USD" for c in kalshi_15m_spot.SPOT_PRODUCTS}
    for asset in kalshi_15m_setup.METAL_CHART_SYMBOL:
        syms |= set(alpaca_news.news_symbols(asset))
    return sorted(syms | {"SPY", "QQQ"})


def _score(text: str) -> float:
    from data.crypto_news import _score_headlines
    return float(_score_headlines([text])[0])


def fetch_month(year: int, month: int, *, symbols: list[str] | None = None, max_pages: int = 2000) -> pd.DataFrame:
    """Every article in one calendar month tagged with any of `symbols`."""
    from data import alpaca_client
    symbols = symbols or universe()
    start = dt.datetime(year, month, 1, tzinfo=dt.timezone.utc)
    end = (start + dt.timedelta(days=32)).replace(day=1)
    rows: list[dict[str, Any]] = []

    def pull(chunk: list[str]) -> list[dict[str, Any]]:
        got: list[dict[str, Any]] = []
        params: dict[str, Any] = {"symbols": ",".join(chunk), "limit": 50, "sort": "asc",
                                  "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"), "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                  "include_content": "false"}
        for _ in range(max_pages):
            data = alpaca_client._data_get("/v1beta1/news", params=params)  # noqa: SLF001
            got += data.get("news") or []
            if not data.get("next_page_token"):
                break
            params["page_token"] = data["next_page_token"]
        return got

    for i in range(0, len(symbols), SYMBOLS_PER_REQUEST):
        chunk = symbols[i:i + SYMBOLS_PER_REQUEST]
        try:
            rows += pull(chunk)
        except Exception as exc:
            # One ticker Alpaca rejects must not cost the month: one by one.
            logger.warning("[alpaca_news_history] %s-%02d batch failed (%s); per ticker", year, month, exc)
            for sym in chunk:
                try:
                    rows += pull([sym])
                except Exception as exc2:
                    logger.warning("[alpaca_news_history] %s %s-%02d skipped: %s", sym, year, month, exc2)
    if not rows:
        return pd.DataFrame(columns=COLUMNS)
    df = pd.DataFrame(rows)
    out = pd.DataFrame({
        "id": df["id"].astype("int64"),
        "created_at": ((pd.to_datetime(df["created_at"], utc=True) - pd.Timestamp("1970-01-01", tz="UTC"))
                       // pd.Timedelta(seconds=1)).astype("int64"),
        "headline": df.get("headline", "").fillna("").astype(str),
        "summary": df.get("summary", "").fillna("").astype(str) if "summary" in df else "",
        "symbols": df["symbols"].apply(lambda s: ",".join(str(x).upper() for x in (s or []))),
        "source": df.get("source", "benzinga"),
        "url": df.get("url", ""),
    })
    out["score"] = [_score(f"{h} {s}") for h, s in zip(out["headline"], out["summary"])]
    return out.drop_duplicates("id").sort_values("created_at").reset_index(drop=True)[COLUMNS]


def _month_key(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def _local_path(key: str) -> Path:
    return LOCAL_DIR / "news" / f"{key}.parquet"


def _repo_path(key: str) -> str:
    return f"news/{key}.parquet"


def _hf_token() -> str:
    return os.getenv("HF_API_KEY", "")


def _hf_files() -> set[str]:
    token = _hf_token()
    if not token:
        return set()
    try:
        from huggingface_hub import HfApi
        return set(HfApi(token=token).list_repo_files(HF_REPO, repo_type="dataset"))
    except Exception:
        return set()


def _months(start_year: int) -> list[tuple[int, int]]:
    now = dt.datetime.now(dt.timezone.utc)
    return [(y, m) for y in range(start_year, now.year + 1) for m in range(1, 13) if (y, m) <= (now.year, now.month)]


def upload(keys: list[str], *, message: str) -> list[str]:
    token = _hf_token()
    if not token or not keys:
        return []
    from huggingface_hub import CommitOperationAdd, HfApi

    from server_common import call_with_hard_timeout
    api = HfApi(token=token)
    api.create_repo(HF_REPO, repo_type="dataset", private=True, exist_ok=True)
    done: list[str] = []
    for i in range(0, len(keys), FILES_PER_COMMIT):
        batch = keys[i:i + FILES_PER_COMMIT]
        ops = [CommitOperationAdd(_repo_path(k), str(_local_path(k))) for k in batch]
        try:
            call_with_hard_timeout(lambda o=ops: api.create_commit(repo_id=HF_REPO, repo_type="dataset", operations=o,
                                                                    commit_message=f"{message} ({len(o)} months)"),
                                   timeout_sec=600)
            done += batch
        except Exception as exc:
            logger.warning("[alpaca_news_history] upload failed for %s..%s: %s", batch[0], batch[-1], exc)
    return done


def _write(key: str, df: pd.DataFrame) -> None:
    path = _local_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)


def backfill(*, start_year: int = START_YEAR, refresh_recent: int = 2) -> dict[str, Any]:
    """Every month not on HF yet (plus the last `refresh_recent` months,
    still filling), uploaded in batches as it goes."""
    on_hf = _hf_files()
    months = _months(start_year)
    recent = {_month_key(y, m) for y, m in months[-refresh_recent:]}
    todo = [(y, m) for y, m in months if _repo_path(_month_key(y, m)) not in on_hf or _month_key(y, m) in recent]
    written, uploaded, articles = [], [], 0
    for y, m in todo:
        key = _month_key(y, m)
        try:
            df = fetch_month(y, m)
        except Exception as exc:
            logger.warning("[alpaca_news_history] %s failed: %s", key, exc)
            continue
        _write(key, df)
        written.append(key)
        articles += len(df)
        if len(written) - len(uploaded) >= FILES_PER_COMMIT:
            uploaded += upload(written[len(uploaded):], message="Alpaca news archive")
    uploaded += upload(written[len(uploaded):], message="Alpaca news archive")
    return {"ok": True, "months": len(written), "uploaded": len(uploaded), "articles": articles}


def append_recent() -> dict[str, Any]:
    """Daily: re-read this month and last (articles still arriving)."""
    return backfill(start_year=dt.datetime.now(dt.timezone.utc).year - 1, refresh_recent=2)


def archive_ready(*, start_year: int = START_YEAR) -> bool:
    files = _hf_files()
    months = _months(start_year)
    return bool(months) and all(_repo_path(_month_key(y, m)) in files for y, m in months[:-1])


def load(*, start_year: int = START_YEAR, end_year: int | None = None) -> pd.DataFrame:
    """The stored archive between those years: local copy, else HF."""
    end_year = end_year or dt.datetime.now(dt.timezone.utc).year
    token = _hf_token()
    frames = []
    for y, m in _months(start_year):
        if y > end_year:
            break
        key = _month_key(y, m)
        path = _local_path(key)
        if not path.exists() and token:
            try:
                from huggingface_hub import hf_hub_download
                src = hf_hub_download(HF_REPO, _repo_path(key), repo_type="dataset", token=token)
                path.parent.mkdir(parents=True, exist_ok=True)
                tmp = path.with_suffix(f".{os.getpid()}.tmp")  # study workers read the archive at once
                pd.read_parquet(src).to_parquet(tmp, index=False)
                os.replace(tmp, path)
            except Exception:
                continue
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame(columns=COLUMNS)
    return pd.concat(frames, ignore_index=True).drop_duplicates("id").sort_values("created_at").reset_index(drop=True)


class NewsIndex:
    """Fast as-of news reads for one asset's tickers over a stored archive."""

    def __init__(self, archive: pd.DataFrame, symbols: list[str]):
        wanted = {s.upper() for s in symbols}
        tagged = archive[archive["symbols"].apply(lambda s: bool(wanted & set(str(s).split(","))))] if not archive.empty else archive
        tagged = tagged.sort_values("created_at")
        self.t = tagged["created_at"].to_numpy("int64") if not tagged.empty else np.zeros(0, dtype="int64")
        scores = tagged["score"].to_numpy(float) if not tagged.empty else np.zeros(0)
        self.scored = (scores != 0).astype(float)
        self.cum_score = np.concatenate([[0.0], np.cumsum(scores)])
        self.cum_scored = np.concatenate([[0.0], np.cumsum(self.scored)])

    def at(self, ts: int, *, hours: float = 6.0) -> dict[str, float]:
        """Articles in (ts - hours, ts]: count and mean sentiment of the ones
        with sentiment words (0.0 when none) -- the live rule's reading."""
        lo = int(np.searchsorted(self.t, ts - hours * 3600, side="right"))
        hi = int(np.searchsorted(self.t, ts, side="right"))
        n_scored = self.cum_scored[hi] - self.cum_scored[lo]
        score = (self.cum_score[hi] - self.cum_score[lo]) / n_scored if n_scored > 0 else 0.0
        return {"count": float(hi - lo), "score": float(score)}
