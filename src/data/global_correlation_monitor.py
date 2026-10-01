"""Global correlation monitor -- one view across all five bots.

Every bot applies the correlation rule on its own (its setup module's
correlation_check against its own leader). This module watches the whole
book instead:

  - a correlation matrix of 15-minute log returns across every instrument
    the bots trade or lead with: the Coinbase coins (real 1-minute spot),
    SPY/QQQ and any stock or option underlying currently held (Alpaca), and
    the COMEX metals (Yahoo, ~10 minutes delayed -- fine for correlation over
    a day, never used to time an entry);
  - each leader's regime (BTC for crypto, SPY for stocks, GOLD for metals):
    last hour and last four hours up, down, or mixed;
  - every open position across perps, Kalshi 15m, Alpaca stocks, crypto and
    options, as one exposure list on a common symbol (BTC, AAPL, GOLD) and
    direction (long/short);
  - clusters of exposures that are really the same bet: same direction on
    instruments with correlation >= EXPOSURE_CORR (the same underlying held
    by two bots counts as correlation 1).

Bots call correlated_exposure() before an entry: a new position that would
make more than MAX_SAME_BET open positions on effectively the same bet is
skipped. Pairs the matrix can't measure yet only match on the same symbol.
refresh() runs on a schedule (app_kalshi) and publishes the snapshot to HF.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or str(default))
    except ValueError:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or str(default))
    except ValueError:
        return int(default)


EXPOSURE_CORR = _env_float("GLOBAL_CORR_EXPOSURE_CORR", 0.8)
MAX_SAME_BET = _env_int("GLOBAL_CORR_MAX_SAME_BET", 2)
MIN_OVERLAP = _env_int("GLOBAL_CORR_MIN_OVERLAP", 40)       # overlapping 15m returns needed for a pair
LOOKBACK_15M = _env_int("GLOBAL_CORR_LOOKBACK_15M", 192)    # two days of 15m returns
STOCK_LEADERS = ("SPY", "QQQ")
METALS = ("GOLD", "SILVER", "COPPER")
LEADERS = {"crypto": "BTC", "stocks": "SPY", "metals": "GOLD"}
HF_REPO = os.getenv("HF_DATASET_REPO", "papylove/kalshi-perps-data")
HF_PATH = "correlation/latest.json"
HF_PUSH_MIN_INTERVAL_SEC = 3600
ROOT_DIR = Path(__file__).resolve().parents[2]
LOCAL_FILE = Path(os.getenv("GLOBAL_CORR_FILE", str(ROOT_DIR / "data" / "global_correlation.json")))

_lock = threading.Lock()
_snapshot: dict[str, Any] = {}
_last_hf_push = 0.0


# ---------------------------------------------------------------------------
# Exposure across bots
# ---------------------------------------------------------------------------

def _option_direction(p: dict[str, Any]) -> str:
    if p.get("setup_side") in ("long", "short"):
        return p["setup_side"]
    bullish_leg = (p.get("option_type") or "").lower() == "call"
    if p.get("strategy") == "credit_spread":
        bullish_leg = not bullish_leg  # the long leg of a credit spread is the protective, opposite-side one
    return "long" if bullish_leg else "short"


def open_exposures() -> list[dict[str, Any]]:
    """Every open position across the five bots: {bot, symbol, direction,
    dry_run}. A bot whose state can't be read is skipped (logged)."""
    out: list[dict[str, Any]] = []

    def collect(bot: str, module, convert) -> None:
        # The bot's state file directly: never its _load_state, whose
        # missing-file path pulls from HF (a network call per check).
        try:
            positions = (json.loads(Path(module.STATE_FILE).read_text(encoding="utf-8")) or {}).get("positions") or []
        except FileNotFoundError:
            return
        except Exception as exc:
            logger.warning("[global_correlation] could not read %s positions: %s", bot, exc)
            return
        try:
            for p in positions:
                sym, direction = convert(p)
                if sym:
                    out.append({"bot": bot, "symbol": sym, "direction": direction, "dry_run": bool(p.get("dry_run"))})
        except Exception as exc:
            logger.warning("[global_correlation] could not read %s positions: %s", bot, exc)

    from data import (alpaca_crypto_strategy, alpaca_options_strategy, alpaca_strategy, kalshi_15m_strategy,
                      perps_strategy)
    from data.alpaca_crypto_data import symbol_to_coin
    from data.perps_data import coin_for_ticker

    from data.kalshi_15m_spot import chart_coin
    collect("perps", perps_strategy, lambda p: (chart_coin(coin_for_ticker(p.get("ticker", ""))), p.get("side") or "long"))
    collect("kalshi15m", kalshi_15m_strategy,
            lambda p: (p.get("coin"), p.get("setup_side") or ("long" if p.get("side") == "yes" else "short")))
    collect("stocks", alpaca_strategy, lambda p: (p.get("symbol"), "long"))
    collect("crypto", alpaca_crypto_strategy, lambda p: (symbol_to_coin(p.get("symbol", "")), "long"))
    collect("options", alpaca_options_strategy, lambda p: (p.get("underlying_symbol"), _option_direction(p)))
    return out


# ---------------------------------------------------------------------------
# Returns and correlation
# ---------------------------------------------------------------------------

def returns_15m(one_min: pd.DataFrame) -> pd.Series:
    """15-minute log returns between CONSECUTIVE closed 15m candles (ts =
    candle end); gaps (closed sessions, missing data) never form a return."""
    if one_min is None or one_min.empty:
        return pd.Series(dtype=float)
    d = one_min.dropna(subset=["close"]).sort_values("ts")
    bucket = ((d["ts"].astype("int64") - 1) // 900 + 1) * 900
    closes = d.groupby(bucket)["close"].last()
    closes = closes[closes.index <= int(time.time()) // 900 * 900]  # closed candles only
    r = np.log(closes).diff()
    consecutive = pd.Series(closes.index, index=closes.index).diff() == 900
    return r[consecutive].tail(LOOKBACK_15M)


def correlation_matrix(returns: dict[str, pd.Series]) -> dict[str, dict[str, float | None]]:
    names = sorted(returns)
    matrix: dict[str, dict[str, float | None]] = {a: {} for a in names}
    for i, a in enumerate(names):
        matrix[a][a] = 1.0
        for b in names[i + 1:]:
            j = returns[a].index.intersection(returns[b].index)
            value = None
            if len(j) >= MIN_OVERLAP:
                x, y = returns[a].loc[j].to_numpy(float), returns[b].loc[j].to_numpy(float)
                if np.std(x) > 0 and np.std(y) > 0:
                    value = round(float(np.corrcoef(x, y)[0, 1]), 3)
            matrix[a][b] = matrix[b][a] = value
    return matrix


def regime(one_min: pd.DataFrame) -> dict[str, Any]:
    """Up / down / mixed from the last hour and last four hours."""
    if one_min is None or one_min.empty:
        return {"state": "unknown"}
    d = one_min.sort_values("ts")
    last_ts, close = int(d["ts"].iloc[-1]), float(d["close"].iloc[-1])

    def ret(seconds: int) -> float | None:
        past = d[d["ts"] <= last_ts - seconds]
        return None if past.empty else close / float(past["close"].iloc[-1]) - 1.0

    r1, r4 = ret(3600), ret(4 * 3600)
    state = "mixed"
    if r1 is not None and r4 is not None:
        state = "up" if r1 > 0 and r4 > 0 else "down" if r1 < 0 and r4 < 0 else "mixed"
    return {"state": state, "ret_1h_pct": None if r1 is None else round(r1 * 100, 3),
            "ret_4h_pct": None if r4 is None else round(r4 * 100, 3), "last_ts": last_ts, "price": close}


def _load_series(exposures: list[dict[str, Any]]) -> dict[str, pd.DataFrame]:
    from data import alpaca_data, kalshi_15m_setup, kalshi_15m_spot
    series: dict[str, pd.DataFrame] = {}
    for coin in kalshi_15m_spot.COINBASE_PRODUCTS:
        try:
            series[coin] = kalshi_15m_spot.recent_series(coin)
        except Exception as exc:
            logger.warning("[global_correlation] Coinbase series failed for %s: %s", coin, exc)
    stocks = set(STOCK_LEADERS) | {e["symbol"] for e in exposures if e["bot"] in ("stocks", "options")}
    for sym in sorted(stocks):
        try:
            bars = alpaca_data.fetch_recent_minute_bars(sym)
            if bars is not None and not bars.empty:
                series[sym] = bars.assign(ts=bars["ts"].astype("int64") + 60)  # Alpaca stamps bars by start
        except Exception as exc:
            logger.warning("[global_correlation] Alpaca bars failed for %s: %s", sym, exc)
    for metal in METALS:
        try:
            series[metal] = kalshi_15m_setup.metals_candles(metal)
        except Exception as exc:
            logger.warning("[global_correlation] Yahoo series failed for %s: %s", metal, exc)
    return {k: v for k, v in series.items() if v is not None and not v.empty}


def _corr(matrix: dict[str, dict[str, float | None]], a: str, b: str) -> float | None:
    if a == b:
        return 1.0
    return (matrix.get(a) or {}).get(b)


def same_bet_clusters(exposures: list[dict[str, Any]], matrix: dict[str, dict[str, float | None]]) -> list[dict[str, Any]]:
    """Groups of open positions that are effectively one bet (connected by
    same-direction correlation >= EXPOSURE_CORR, or opposite direction on a
    correlation <= -EXPOSURE_CORR)."""
    n = len(exposures)
    parent = list(range(n))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    for i in range(n):
        for j in range(i + 1, n):
            c = _corr(matrix, exposures[i]["symbol"], exposures[j]["symbol"])
            same_dir = exposures[i]["direction"] == exposures[j]["direction"]
            if c is not None and ((same_dir and c >= EXPOSURE_CORR) or (not same_dir and c <= -EXPOSURE_CORR)):
                parent[find(i)] = find(j)
    groups: dict[int, list[dict[str, Any]]] = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(exposures[i])
    return [{"size": len(g), "positions": g} for g in groups.values() if len(g) > 1]


def refresh() -> dict[str, Any]:
    """Recompute the snapshot from real data; publish it."""
    started = time.time()
    exposures = open_exposures()
    series = _load_series(exposures)
    returns = {k: returns_15m(v) for k, v in series.items()}
    returns = {k: v for k, v in returns.items() if len(v) >= MIN_OVERLAP}
    matrix = correlation_matrix(returns)
    leaders = {group: {"symbol": sym, **regime(series.get(sym))} for group, sym in LEADERS.items()}
    snapshot = {
        "ok": True, "computed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "seconds": round(time.time() - started, 1), "symbols": sorted(returns), "matrix": matrix,
        "returns_counts": {k: int(len(v)) for k, v in returns.items()}, "leaders": leaders,
        "exposures": exposures, "clusters": same_bet_clusters(exposures, matrix),
        "params": {"exposure_corr": EXPOSURE_CORR, "max_same_bet": MAX_SAME_BET, "lookback_15m": LOOKBACK_15M,
                   "min_overlap": MIN_OVERLAP},
        "notes": {"metals": "Yahoo COMEX 1-minute data is ~10 minutes delayed: used for correlation, never for entry timing."},
    }
    with _lock:
        _snapshot.clear()
        _snapshot.update(snapshot)
    _save_local(snapshot)
    _push_hf(snapshot)
    return {k: snapshot[k] for k in ("ok", "computed_at", "seconds", "symbols", "leaders", "clusters")}


def latest() -> dict[str, Any]:
    with _lock:
        if _snapshot:
            return dict(_snapshot)
    try:
        return json.loads(LOCAL_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"ok": False, "reason": "not_computed_yet"}


def correlated_exposure(symbol: str, direction: str, *, bot: str) -> dict[str, Any]:
    """Would opening `direction` on `symbol` (from `bot`) exceed MAX_SAME_BET
    open positions on effectively the same bet? Uses live positions and the
    latest matrix; an unmeasured pair only matches on the same symbol."""
    matrix = (latest() or {}).get("matrix") or {}
    matches = []
    for e in open_exposures():
        c = _corr(matrix, symbol, e["symbol"])
        if c is None:
            continue
        same_dir = e["direction"] == direction
        if (same_dir and c >= EXPOSURE_CORR) or (not same_dir and c <= -EXPOSURE_CORR):
            matches.append({**e, "corr": c})
    blocked = len(matches) + 1 > MAX_SAME_BET
    detail = (f"{len(matches)} open position(s) already on this bet: "
              + ", ".join(f"{m['bot']} {m['direction']} {m['symbol']} (corr {m['corr']:+.2f})" for m in matches)
              if matches else "no correlated open positions")
    return {"ok": not blocked, "blocked": blocked, "matches": matches, "max_same_bet": MAX_SAME_BET,
            "exposure_corr": EXPOSURE_CORR, "detail": detail, "bot": bot}


def _save_local(snapshot: dict[str, Any]) -> None:
    try:
        LOCAL_FILE.parent.mkdir(parents=True, exist_ok=True)
        LOCAL_FILE.write_text(json.dumps(snapshot), encoding="utf-8")
    except Exception as exc:
        logger.warning("[global_correlation] local save failed: %s", exc)


def _push_hf(snapshot: dict[str, Any]) -> None:
    global _last_hf_push
    token = os.getenv("HF_API_KEY", "")
    if not token or time.time() - _last_hf_push < HF_PUSH_MIN_INTERVAL_SEC:
        return
    _last_hf_push = time.time()

    def upload() -> None:
        from huggingface_hub import HfApi
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
            json.dump(snapshot, tmp)
            path = tmp.name
        try:
            HfApi(token=token).upload_file(path_or_fileobj=path, path_in_repo=HF_PATH, repo_id=HF_REPO, repo_type="dataset",
                                           commit_message="update global correlation snapshot")
        finally:
            os.unlink(path)

    try:
        from server_common import call_with_hard_timeout
        call_with_hard_timeout(upload, timeout_sec=20)
    except Exception as exc:
        logger.warning("[global_correlation] HF push failed: %s", exc)
