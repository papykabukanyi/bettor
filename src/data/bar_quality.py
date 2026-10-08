"""Clean 1-minute candles for every study (user request 2026-10-08: "the
clear data no skip no bad data pure data"): duplicate minutes dropped,
impossible candles (high below low, prices <= 0) dropped, and bad prints
removed -- a one-minute move of 2%+ that is 25x the recent typical move and
snaps at least 70% back the next minute. Minutes that never traded are not
invented (a coin with no trade that minute has no candle). Studies only:
the live charts read the stream."""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def clean(candles: pd.DataFrame, *, kind: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Sorted unique 1-minute candles with impossible candles and snap-back
    bad prints removed, and per-year quality: minutes present vs expected
    (stocks: 390 a trading day; crypto: every minute between the first and
    last), duplicates, impossible candles, bad prints removed."""
    raw_rows = int(len(candles))
    c = candles.sort_values("ts")
    dups = int(c["ts"].duplicated().sum())
    c = c.drop_duplicates("ts", keep="last")
    px = c[["open", "high", "low", "close"]]
    impossible = (px <= 0).any(axis=1) | (c["high"] < px.max(axis=1) - 1e-12) | (c["low"] > px.min(axis=1) + 1e-12)
    c = c[~impossible].reset_index(drop=True)
    r = np.diff(np.log(c["close"].to_numpy(float)), prepend=np.nan)
    typical = pd.Series(np.abs(r)).rolling(240, min_periods=60).median().to_numpy()
    nxt = np.append(r[1:], np.nan)
    spike = (np.abs(r) > 0.02) & (np.abs(r) > 25 * np.maximum(np.nan_to_num(typical, nan=1.0), 1e-5)) \
        & (np.abs(r + nxt) < 0.3 * np.abs(r))
    spike = np.nan_to_num(spike, nan=0).astype(bool)
    c = c[~spike].reset_index(drop=True)
    years = pd.to_datetime(c["ts"], unit="s", utc=True).dt.year
    quality = {}
    for y, g in c.groupby(years):
        if kind == "stock":
            days = pd.to_datetime(g["ts"] - 60, unit="s", utc=True).dt.tz_convert("America/New_York").dt.date.nunique()
            expected = days * 390
        else:
            expected = int((g["ts"].max() - g["ts"].min()) // 60 + 1)
        quality[int(y)] = {"minutes": int(len(g)), "expected": int(expected), "present_pct": round(100.0 * len(g) / max(expected, 1), 2)}
    return c, {"raw_rows": raw_rows, "duplicates": dups, "impossible": int(impossible.sum()), "bad_prints": int(spike.sum()),
               "years": quality}
