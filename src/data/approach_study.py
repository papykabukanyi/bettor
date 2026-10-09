"""Every ticker every bot trades, on every chart timeframe, over all the
clean history on HF: which approach works, learned walk-forward on years
the choice never saw (user request 2026-10-08: "all the chart time of all
ticker need to be learn and how to approach it", on clean data -- "no skip
no bad data").

Data (Alpaca's archives, all of it -- stocks/ETFs/commodity ETFs from
2016 on the SIP feed, crypto from 2021 on the Kraken US feed):
  cleaned before anything learns from it: duplicate minutes dropped,
  impossible candles (high below low, prices <= 0) dropped, and bad prints
  removed -- a one-minute move of 2%+ that is 25x the recent typical move
  and snaps at least 70% back the next minute. Stocks are read on the
  regular session. Each symbol-year's minutes present vs expected, and
  what was removed, are published with the results.

Timeframes: 1m, 5m, 15m, 1h and 1d (crypto also 4h); bars are built from
the clean minutes (ts = END). Stock intraday positions are flat at each
session's close (the stocks bot never holds overnight); a daily bar holds
across days.

Approaches (each long-only and long/short, a small parameter grid):
  trend      EMA fast above slow
  momentum   the sign of the last N bars' move
  breakout   a close beyond the last N bars' high/low, out at the N/2 low/high
  bollinger  fade a close k standard deviations from its N-bar mean, out at the mean
  rsi        fade RSI(14) below 30 / above 70, out at 50
  hours      trade only the hours of day that made money in every earlier year (1h)
  weekdays   trade only the weekdays that made money in earlier years (1d)

Costs: each bot's real cost per side (fee + half its spread) on every
position change.

Walk-forward: for each ticker, timeframe and allowed side set, every year
after the first is traded by the approach + setting with the best net
result over all earlier years; that year's result is out of sample. The
summary says, per bot, which timeframe and approach family works across
its tickers out of sample, and per ticker how to approach it.

Runs on HF in its own low-priority process with a worker pool
(python -m data.approach_study); the hub's processing panel shows it."""
from __future__ import annotations

import datetime as dt
import json
import logging
import math
import os
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

SRC_DIR = Path(__file__).resolve().parents[1]
NAME = "approach_study"
VERSION = 1
HF_PATH = "setup_strategy/approach_study.json"
WORKERS = int(os.getenv("APPROACH_STUDY_WORKERS", "6") or "6")

TIMEFRAMES = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440}
STOCK_TIMEFRAMES = ("1m", "5m", "15m", "1h", "1d")
CRYPTO_TIMEFRAMES = ("1m", "5m", "15m", "1h", "4h", "1d")
CONFIGS: list[tuple[str, dict[str, float]]] = (
    [("trend", {"fast": f, "slow": s}) for f, s in ((5, 20), (10, 50), (20, 100))]
    + [("momentum", {"n": n}) for n in (3, 12, 48)]
    + [("breakout", {"n": n}) for n in (20, 55)]
    + [("bollinger", {"n": n, "k": k}) for n, k in ((20, 2.0), (50, 2.5))]
    + [("rsi", {"n": 14})]
)
SIDE_SETS = ("long", "long_short")
# Each bot's tickers, its side sets, and its real cost per side (fee + half
# spread, as a fraction of price). Options are scored on the underlying
# (an option's own spread is not in this cost); 15m on the coin's chart.
BOT_COST_PER_SIDE = {"stocks": 0.0002, "options": 0.0002, "crypto": 0.0028, "perps": 0.0005, "kalshi15m": 0.0005}
BOT_SIDES = {"stocks": ("long",), "options": ("long", "long_short"), "crypto": ("long",), "perps": ("long", "long_short"),
             "kalshi15m": ("long",)}
MIN_TRADES_TO_CHOOSE = 10
ADOPT_MIN_YEARS = 3
ADOPT_MIN_YEARS_UP = 0.7
ADOPT_MIN_T = 2.5


def _local_dir() -> Path:
    from data import setup_backtest_job
    return setup_backtest_job.LOCAL_DIR


# ---------------------------------------------------------------------------
# Clean data
# ---------------------------------------------------------------------------
def bars(one_min: pd.DataFrame, minutes: int, *, kind: str) -> pd.DataFrame:
    """Timeframe bars from clean 1-minute candles (ts = END): a bar ends at
    its period's end; daily bars per New York trading day (stocks) or UTC
    day (crypto). `session_end` marks a stock bar that closes its session."""
    d = one_min[["ts", "open", "high", "low", "close", "volume"]]
    if minutes == 1:
        out = d.copy()
    else:
        if minutes == 1440:
            local = pd.to_datetime(d["ts"] - 60, unit="s", utc=True)
            if kind == "stock":
                local = local.dt.tz_convert("America/New_York")
            key = local.dt.strftime("%Y-%m-%d")
        else:
            key = (np.ceil(d["ts"] / (60 * minutes)) * 60 * minutes).astype("int64")
        g = d.groupby(key, sort=True)
        out = pd.DataFrame({"ts": g["ts"].max(), "open": g["open"].first(), "high": g["high"].max(), "low": g["low"].min(),
                            "close": g["close"].last(), "volume": g["volume"].sum()}).reset_index(drop=True)
    out = out.sort_values("ts").reset_index(drop=True)
    if kind == "stock" and minutes < 1440:
        day = pd.to_datetime(out["ts"] - 60, unit="s", utc=True).dt.tz_convert("America/New_York").dt.date
        out["session_end"] = (day != day.shift(-1)).to_numpy()
    else:
        out["session_end"] = False
    return out


# ---------------------------------------------------------------------------
# Approaches -- the position held after each bar's close (+1 long, -1 short)
# ---------------------------------------------------------------------------
def _state(entry: np.ndarray, exit_: np.ndarray) -> np.ndarray:
    """1 from an entry until its exit (an entry wins on the same bar)."""
    sig = np.full(len(entry), np.nan)
    sig[np.asarray(exit_, dtype=bool)] = 0.0
    sig[np.asarray(entry, dtype=bool)] = 1.0
    return pd.Series(sig).ffill().fillna(0.0).to_numpy()


def _hold(entry_long: np.ndarray, exit_long: np.ndarray, entry_short: np.ndarray, exit_short: np.ndarray,
          long_short: bool) -> np.ndarray:
    """A position from entry/exit signals: each side in at its entry, out at
    its own exit (a short's exit never closes a long)."""
    long = _state(entry_long, exit_long)
    return long - _state(entry_short, exit_short) if long_short else long


def position(b: pd.DataFrame, approach: str, p: dict[str, float], *, long_short: bool) -> np.ndarray:
    c = b["close"]
    if approach == "trend":
        fast, slow = c.ewm(span=int(p["fast"]), adjust=False).mean(), c.ewm(span=int(p["slow"]), adjust=False).mean()
        pos = np.where(fast > slow, 1.0, -1.0 if long_short else 0.0)
        pos[: int(p["slow"])] = 0.0
        return pos
    if approach == "momentum":
        m = np.log(c / c.shift(int(p["n"]))).to_numpy()
        pos = np.sign(np.nan_to_num(m))
        return pos if long_short else np.maximum(pos, 0.0)
    if approach == "breakout":
        n = int(p["n"])
        hi, lo = b["high"].rolling(n).max().shift(1), b["low"].rolling(n).min().shift(1)
        hi2, lo2 = b["high"].rolling(max(n // 2, 2)).max().shift(1), b["low"].rolling(max(n // 2, 2)).min().shift(1)
        return _hold((c > hi).to_numpy(), (c < lo2).to_numpy(), (c < lo).to_numpy(), (c > hi2).to_numpy(), long_short)
    if approach == "bollinger":
        n, k = int(p["n"]), float(p["k"])
        mu, sd = c.rolling(n).mean(), c.rolling(n).std()
        z = ((c - mu) / sd).to_numpy()
        return _hold(np.nan_to_num(z < -k), np.nan_to_num(z >= 0), np.nan_to_num(z > k), np.nan_to_num(z <= 0), long_short)
    if approach == "rsi":
        n = int(p["n"])
        d = c.diff()
        up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
        dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
        rsi = (100 - 100 / (1 + up / dn.replace(0, np.nan))).to_numpy()
        return _hold(np.nan_to_num(rsi < 30), np.nan_to_num(rsi > 50), np.nan_to_num(rsi > 70), np.nan_to_num(rsi < 50), long_short)
    raise ValueError(approach)


def yearly(b: pd.DataFrame, pos: np.ndarray) -> pd.DataFrame:
    """Per year: gross log return of the held position, position changes
    (turnover in units of the position) and bars held. Stock intraday
    positions are closed at each session's last bar."""
    pos = pos.copy()
    pos[b["session_end"].to_numpy()] = 0.0
    r = np.diff(np.log(b["close"].to_numpy(float)), prepend=np.nan)
    held = np.concatenate([[0.0], pos[:-1]])
    gross = np.nan_to_num(held * r)
    turn = np.abs(np.diff(pos, prepend=0.0))
    year = pd.to_datetime(b["ts"], unit="s", utc=True).dt.year.to_numpy()
    df = pd.DataFrame({"year": year, "gross": gross, "turnover": turn, "held": held != 0})
    return df.groupby("year").agg(gross=("gross", "sum"), turnover=("turnover", "sum"), bars_held=("held", "sum")).reset_index()


def _pattern_yearly(b: pd.DataFrame, unit: str) -> pd.DataFrame:
    """Per year and hour (1h bars) or weekday (1d bars): the long bar return
    sum -- the hours/weekdays walk-forward picks from these. A stock's first
    intraday bar of a session doesn't carry the overnight gap."""
    r = np.diff(np.log(b["close"].to_numpy(float)), prepend=np.nan)
    r[np.concatenate([[False], b["session_end"].to_numpy()[:-1]])] = 0.0
    t = pd.to_datetime(b["ts"] - 60, unit="s", utc=True)
    key = t.dt.hour if unit == "hour" else t.dt.weekday
    df = pd.DataFrame({"year": t.dt.year, "key": key, "r": np.nan_to_num(r)})
    return df.groupby(["year", "key"])["r"].agg(["sum", "count"]).reset_index()


def instrument_table(symbol: str, kind: str) -> dict[str, Any]:
    """Everything one ticker contributes: its data quality and, per
    timeframe x approach x setting x side set x year, gross, turnover and
    bars held; plus the hour/weekday tables."""
    from data import alpaca_crypto_history, alpaca_setup, alpaca_sip_history, bar_quality
    if kind == "stock":
        raw = alpaca_setup.regular_session_candles(alpaca_sip_history.load(symbol))
    else:
        raw = alpaca_crypto_history.candles(symbol)
    if raw is None or raw.empty:
        return {"symbol": symbol, "kind": kind, "rows": [], "quality": {"raw_rows": 0}}
    one_min, quality = bar_quality.clean(raw, kind=kind)
    rows: list[dict[str, Any]] = []
    patterns: dict[str, list[dict[str, Any]]] = {}
    for tf in (STOCK_TIMEFRAMES if kind == "stock" else CRYPTO_TIMEFRAMES):
        b = bars(one_min, TIMEFRAMES[tf], kind=kind)
        if len(b) < 200:
            continue
        runs = [(a, p, sd) for a, p in CONFIGS for sd in SIDE_SETS] + [("hold", {}, "long")]  # hold: the benchmark
        for approach, params, sides in runs:
            pos = np.ones(len(b)) if approach == "hold" else position(b, approach, params, long_short=sides == "long_short")
            y = yearly(b, pos)
            for rec in y.itertuples(index=False):
                rows.append({"tf": tf, "approach": approach, "params": json.dumps(params, sort_keys=True), "sides": sides,
                             "year": int(rec.year), "gross": float(rec.gross), "turnover": float(rec.turnover),
                             "bars_held": int(rec.bars_held)})
        if tf == "1h":
            patterns["hours"] = _pattern_yearly(b, "hour").to_dict("records")
        if tf == "1d":
            patterns["weekdays"] = _pattern_yearly(b, "weekday").to_dict("records")
    return {"symbol": symbol, "kind": kind, "rows": rows, "patterns": patterns, "quality": quality}


# ---------------------------------------------------------------------------
# Walk-forward over years
# ---------------------------------------------------------------------------
def walk_forward_symbol(rows: pd.DataFrame, cost: float, sides_allowed: tuple[str, ...]) -> list[dict[str, Any]]:
    """For each timeframe and side set: every year after the first traded
    by the approach + setting with the best net over all earlier years
    (at least MIN_TRADES_TO_CHOOSE position changes); also per approach
    family (its best setting so far). Net = gross - cost x turnover."""
    out = []
    rows = rows.copy()
    rows["net"] = rows["gross"] - cost * rows["turnover"]
    hold = rows[rows["approach"] == "hold"].groupby(["tf", "year"])["net"].sum()
    r = rows[rows["sides"].isin(sides_allowed) & (rows["approach"] != "hold")]
    if r.empty:
        return out
    for (tf, sides), g in r.groupby(["tf", "sides"]):
        years = sorted(g["year"].unique())
        for family in ["any"] + sorted(g["approach"].unique()):
            gg = g if family == "any" else g[g["approach"] == family]
            picks = []
            for y in years[1:]:
                past = gg[gg["year"] < y].groupby(["approach", "params"]).agg(net=("net", "sum"), turnover=("turnover", "sum"))
                past = past[past["turnover"] >= MIN_TRADES_TO_CHOOSE]
                if past.empty:
                    continue
                best = past["net"].idxmax()
                if past.loc[best, "net"] <= 0:
                    picks.append({"year": int(y), "net": 0.0, "pick": None})  # nothing made money before: stay out
                    continue
                now = gg[(gg["year"] == y) & (gg["approach"] == best[0]) & (gg["params"] == best[1])]
                picks.append({"year": int(y), "net": float(now["net"].sum()), "pick": f"{best[0]} {best[1]}"})
            if picks:
                nets = np.array([p["net"] for p in picks])
                traded = [p for p in picks if p["pick"]]
                t = float(nets.mean() / (nets.std(ddof=1) / math.sqrt(len(nets)))) if len(nets) > 2 and nets.std(ddof=1) > 0 else None
                hold_total = float(sum(hold.get((tf, p["year"]), 0.0) for p in picks))
                out.append({"tf": tf, "sides": sides, "family": family, "oos_years": len(picks), "years_traded": len(traded),
                            "oos_total": round(float(nets.sum()), 4), "oos_mean": round(float(nets.mean()), 4),
                            "hold_total": round(hold_total, 4), "excess": round(float(nets.sum()) - hold_total, 4),
                            "years_up": round(sum(n > 0 for n in nets) / len(nets), 3), "t": None if t is None else round(t, 2),
                            "last_pick": picks[-1]["pick"], "by_year": picks})
    return out


def walk_forward_patterns(patterns: dict[str, list[dict[str, Any]]], cost: float,
                          hold: pd.Series | None = None) -> list[dict[str, Any]]:
    """Hours of day / weekdays: each year trades the keys whose long return
    was positive over every earlier year (in each one), paying the cost to
    enter and exit each traded bar. `hold` (net per (tf, year)) is the
    benchmark: holding the ticker on that timeframe."""
    out = []
    for name, recs in (patterns or {}).items():
        df = pd.DataFrame(recs)
        if df.empty:
            continue
        years = sorted(df["year"].unique())
        picks = []
        for y in years[1:]:
            past = df[df["year"] < y]
            good = [k for k, g in past.groupby("key") if len(g) == len(past["year"].unique()) and (g["sum"] > 0).all()]
            now = df[(df["year"] == y) & (df["key"].isin(good))]
            net = float(now["sum"].sum() - 2 * cost * now["count"].sum())
            picks.append({"year": int(y), "net": net if good else 0.0, "keys": [int(k) for k in good]})
        if picks:
            nets = np.array([p["net"] for p in picks])
            t = float(nets.mean() / (nets.std(ddof=1) / math.sqrt(len(nets)))) if len(nets) > 2 and nets.std(ddof=1) > 0 else None
            tf = "1h" if name == "hours" else "1d"
            hold_total = float(sum(hold.get((tf, p["year"]), 0.0) for p in picks)) if hold is not None else 0.0
            out.append({"tf": tf, "sides": "long", "family": name, "oos_years": len(picks),
                        "hold_total": round(hold_total, 4), "excess": round(float(nets.sum()) - hold_total, 4),
                        "years_traded": sum(1 for p in picks if p["keys"]), "oos_total": round(float(nets.sum()), 4),
                        "oos_mean": round(float(nets.mean()), 4), "years_up": round(sum(n > 0 for n in nets) / len(nets), 3),
                        "t": None if t is None else round(t, 2), "last_pick": picks[-1]["keys"], "by_year": picks})
    return out


def adoptable(rec: dict[str, Any]) -> bool:
    """Out of sample: enough years, most of them up, a t-stat past
    ADOPT_MIN_T, and better than simply holding the ticker on the same
    timeframe in the same years (a long-only filter in a bull market can
    look good just by holding)."""
    return bool(rec["oos_years"] >= ADOPT_MIN_YEARS and rec["oos_total"] > 0 and rec["years_up"] >= ADOPT_MIN_YEARS_UP
                and (rec["t"] or 0) >= ADOPT_MIN_T and rec.get("excess", 0.0) > 0)


# ---------------------------------------------------------------------------
# The run (its own process on HF)
# ---------------------------------------------------------------------------
def universe() -> dict[str, dict[str, Any]]:
    """Every ticker of every bot: {symbol: {"kind": stock|crypto, "bots": [...]}}."""
    from data import alpaca_crypto_history, alpaca_data, alpaca_options_data, kalshi_15m_setup, kalshi_15m_spot, perps_data
    from data import kalshi_15m_strategy
    out: dict[str, dict[str, Any]] = {}

    def add(sym: str, kind: str, bot: str) -> None:
        out.setdefault(sym, {"kind": kind, "bots": []})
        if bot not in out[sym]["bots"]:
            out[sym]["bots"].append(bot)

    for s in sorted(set(alpaca_data.BROAD_CANDIDATE_UNIVERSE) | {"SPY", "QQQ"}):
        add(s, "stock", "stocks")
    for s in alpaca_options_data.OPTIONS_UNDERLYINGS:
        add(s, "stock", "options")
    for c in sorted(set(alpaca_crypto_history.universe()) | set(alpaca_crypto_history.crypto_bot_coins())):
        add(c, "crypto", "crypto")
    for t in perps_data.chartable_tickers():
        coin = kalshi_15m_spot.chart_coin(perps_data.coin_for_ticker(t))
        etf = kalshi_15m_setup.METAL_CHART_SYMBOL.get(coin)
        add(etf or coin, "stock" if etf else "crypto", "perps")
    for coin in kalshi_15m_strategy.ACTIVE_ENTRY_COINS:
        etf = kalshi_15m_setup.METAL_CHART_SYMBOL.get(coin)
        if etf or coin in kalshi_15m_spot.SPOT_PRODUCTS:
            add(etf or coin, "stock" if etf else "crypto", "kalshi15m")
    return out


def _progress(**kw: Any) -> None:
    path = _local_dir() / f"{NAME}_progress.json"
    try:
        old = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except (OSError, ValueError):
        old = {}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({**old, **kw, "updated_at": time.time()}), encoding="utf-8")
    except OSError:
        pass


def _worker(args: tuple[str, str]) -> dict[str, Any]:
    try:
        os.nice(5)
    except (AttributeError, OSError):
        pass
    return instrument_table(*args)


def summarize(per_symbol: dict[str, dict[str, Any]], tickers: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Per bot: each timeframe x family's out-of-sample record across its
    tickers, and per ticker the best timeframe/approach out of sample."""
    bots: dict[str, Any] = {}
    for bot, cost in BOT_COST_PER_SIDE.items():
        recs = []
        for sym, info in tickers.items():
            if bot not in info["bots"] or sym not in per_symbol:
                continue
            rows = pd.DataFrame(per_symbol[sym]["rows"])
            wf = walk_forward_symbol(rows, cost, BOT_SIDES[bot]) if not rows.empty else []
            hold = None
            if not rows.empty:
                h = rows[rows["approach"] == "hold"]
                hold = (h["gross"] - cost * h["turnover"]).groupby([h["tf"], h["year"]]).sum()
            wf += walk_forward_patterns(per_symbol[sym].get("patterns") or {}, cost, hold)
            for rec in wf:
                recs.append({"symbol": sym, **{k: v for k, v in rec.items() if k != "by_year"}, "by_year": rec["by_year"],
                             "adopt": adoptable(rec)})
        if not recs:
            continue
        df = pd.DataFrame(recs)
        table = []
        for (tf, family, sides), g in df.groupby(["tf", "family", "sides"]):
            table.append({"tf": tf, "family": family, "sides": sides, "tickers": int(len(g)),
                          "tickers_positive": round(float((g["oos_total"] > 0).mean()), 3),
                          "median_oos_per_year": round(float(g["oos_mean"].median()), 4),
                          "tickers_beating_hold": round(float((g["excess"] > 0).mean()), 3),
                          "median_excess_total": round(float(g["excess"].median()), 4),
                          "mean_oos_per_year": round(float(g["oos_mean"].mean()), 4),
                          "adoptable_tickers": int(g["adopt"].sum())})
        table.sort(key=lambda r: (-r["tickers_positive"], -r["median_oos_per_year"]))
        best = {}
        for sym, g in df[df["family"] != "any"].groupby("symbol"):
            top = g.sort_values(["adopt", "oos_total"], ascending=False).iloc[0]
            best[sym] = {k: (top[k] if not isinstance(top[k], np.generic) else top[k].item())
                         for k in ("tf", "family", "sides", "oos_years", "oos_total", "oos_mean", "hold_total", "excess", "years_up", "t",
                                   "last_pick", "adopt")}
        bots[bot] = {"cost_per_side": cost, "sides": list(BOT_SIDES[bot]), "tickers": int(df["symbol"].nunique()),
                     "families": table, "best_per_ticker": best,
                     "adoptable": [r for r in recs if r["adopt"] and r["family"] != "any"]}
    return bots


def run(*, publish: bool = True) -> dict[str, Any]:
    started = time.time()
    tickers = universe()
    todo = sorted(tickers)
    _progress(stage="replaying every ticker on every timeframe", done=0, total=len(todo),
              started_at=dt.datetime.now(dt.timezone.utc).isoformat(), current=[])
    # Archives to local disk once (the backfills keep them current).
    per_symbol: dict[str, dict[str, Any]] = {}
    done = 0
    with ProcessPoolExecutor(max_workers=WORKERS) as pool:
        futs = {pool.submit(_worker, (sym, tickers[sym]["kind"])): sym for sym in todo}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                per_symbol[sym] = fut.result()
            except Exception as exc:
                logger.warning("[approach_study] %s failed: %s", sym, exc)
                per_symbol[sym] = {"symbol": sym, "kind": tickers[sym]["kind"], "rows": [], "error": str(exc)[:300], "quality": {}}
            done += 1
            _progress(done=done, last_symbol=sym)
    _progress(stage="walk-forward over the years, per bot")
    bots = summarize(per_symbol, tickers)
    quality = {sym: v.get("quality") for sym, v in per_symbol.items()}
    years = sorted({y for q in quality.values() for y in ((q or {}).get("years") or {})})
    result = {"ok": True, "version": VERSION, "computed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
              "tickers": len(tickers), "years": [min(years), max(years)] if years else None,
              "data_quality": {"tickers": len(quality), "bad_prints_removed": int(sum((q or {}).get("bad_prints", 0) for q in quality.values())),
                               "duplicates_removed": int(sum((q or {}).get("duplicates", 0) for q in quality.values())),
                               "impossible_removed": int(sum((q or {}).get("impossible", 0) for q in quality.values())),
                               "failed": sorted(s for s, v in per_symbol.items() if v.get("error")),
                               "per_ticker": quality},
              "bots": bots, "seconds": round(time.time() - started)}
    path = _local_dir() / f"{NAME}.json"
    path.write_text(json.dumps(result, default=str), encoding="utf-8")
    if publish:
        _progress(stage="publishing")
        result["published"] = _publish(result)
        path.write_text(json.dumps(result, default=str), encoding="utf-8")
    _progress(stage="done", done=len(todo))
    return result


def _publish(result: dict[str, Any]) -> bool:
    token = os.getenv("HF_API_KEY", "")
    if not token:
        return False
    try:
        from huggingface_hub import HfApi

        from data import setup_backtest_job
        HfApi(token=token).upload_file(path_or_fileobj=json.dumps(result, default=str).encode(), path_in_repo=HF_PATH,
                                       repo_id=setup_backtest_job.REPOS["stocks"], repo_type="model",
                                       commit_message="every ticker x timeframe x approach, walk-forward")
        return True
    except Exception as exc:
        logger.warning("[approach_study] publish failed: %s", exc)
        return False


_latest_cache: dict[str, Any] = {}


def latest() -> dict[str, Any] | None:
    cached = _latest_cache.get("v")
    try:
        newer = (_local_dir() / f"{NAME}.json").stat().st_mtime > (cached[0] if cached else 0.0)
    except OSError:
        newer = False
    if cached and time.time() - cached[0] < 600 and not newer:
        return cached[1]
    data = None
    try:
        data = json.loads((_local_dir() / f"{NAME}.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        token = os.getenv("HF_API_KEY", "")
        if token:
            try:
                from huggingface_hub import hf_hub_download

                from data import setup_backtest_job
                from server_common import call_with_hard_timeout
                p = call_with_hard_timeout(lambda: hf_hub_download(setup_backtest_job.REPOS["stocks"], HF_PATH, repo_type="model",
                                                                   token=token), timeout_sec=20)
                data = json.loads(Path(p).read_text(encoding="utf-8")) if p else None
            except Exception:
                data = None
    _latest_cache["v"] = (time.time(), data)
    return data


def summary() -> dict[str, Any]:
    """The compact view for the API: per bot, the top timeframe x family
    rows and how many tickers have an approach that cleared the bar."""
    data = latest() or {}
    out = {"computed_at": data.get("computed_at"), "tickers": data.get("tickers"), "years": data.get("years"),
           "data_quality": {k: v for k, v in (data.get("data_quality") or {}).items() if k != "per_ticker"}, "bots": {}}
    for bot, b in (data.get("bots") or {}).items():
        out["bots"][bot] = {"tickers": b.get("tickers"), "top": (b.get("families") or [])[:8],
                            "adoptable": [{k: r.get(k) for k in ("symbol", "tf", "family", "sides", "oos_years", "oos_mean", "excess", "years_up", "t", "last_pick")}
                                          for r in (b.get("adoptable") or [])][:40]}
    return out


# ---------------------------------------------------------------------------
# Its own low-priority process
# ---------------------------------------------------------------------------
def _running() -> bool:
    from data import setup_backtest_job
    return setup_backtest_job._running(NAME)  # noqa: SLF001


def progress() -> dict[str, Any] | None:
    try:
        return json.loads((_local_dir() / f"{NAME}_progress.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def launch() -> dict[str, Any]:
    d = _local_dir()
    d.mkdir(parents=True, exist_ok=True)
    if _running():
        return {"ok": True, "action": "already_running"}
    (d / f"{NAME}_progress.json").unlink(missing_ok=True)
    log = open(d / f"{NAME}.log", "w")  # noqa: SIM115 -- handed to the child process
    proc = subprocess.Popen([sys.executable, "-m", "data.approach_study"], cwd=str(SRC_DIR), env=dict(os.environ),
                            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    (d / f"{NAME}.pid").write_text(str(proc.pid))
    return {"ok": True, "action": "launched", "pid": proc.pid}


def maybe_start() -> dict[str, Any]:
    """Run when there is no result of this version from the last 7 days
    and no multi-year study holds the cores (it is the user's forced run
    after a deploy: no other wait)."""
    from data import setup_backtest_job
    if _running():
        return {"ok": True, "action": "running"}
    if any(setup_backtest_job._running(f"{b}_multiyear") for b in setup_backtest_job.MULTIYEAR):  # noqa: SLF001
        return {"ok": True, "action": "a_multiyear_study_is_running"}
    data = latest() or {}
    try:
        age_days = (time.time() - dt.datetime.fromisoformat(str(data.get("computed_at"))).timestamp()) / 86400
    except (TypeError, ValueError):
        age_days = math.inf
    if data.get("version") == VERSION and age_days < 7:
        return {"ok": True, "action": "fresh"}
    err = _local_dir() / f"{NAME}_error.json"
    if err.exists() and time.time() - err.stat().st_mtime < 3 * 3600:
        return {"ok": True, "action": "failed_recently"}
    _latest_cache.clear()
    return launch()


def overview_row(now: float | None = None) -> dict[str, Any]:
    now = time.time() if now is None else now
    data = latest() or {}
    row: dict[str, Any] = {"name": "All tickers x all timeframes", "last_published_at": data.get("computed_at"),
                           "last_seconds": data.get("seconds"), "tickers": data.get("tickers"), "years": data.get("years")}
    if _running():
        p = progress() or {}
        try:
            elapsed = now - dt.datetime.fromisoformat(p.get("started_at")).timestamp()
        except (TypeError, ValueError):
            elapsed = None
        done, total = float(p.get("done") or 0), float(p.get("total") or 1)
        eta = elapsed / done * (total - done) if elapsed and done else None
        row.update(state="stalled" if now - float(p.get("updated_at") or now) > 45 * 60 else "running",
                   progress={"stage": p.get("stage"), "done": p.get("done"), "total": p.get("total"),
                             "percent": round(100 * done / total, 1), "elapsed_sec": elapsed, "eta_sec": eta,
                             "idle_sec": max(0.0, now - float(p.get("updated_at") or now)), "last_symbol": p.get("last_symbol")})
        return row
    err = _local_dir() / f"{NAME}_error.json"
    if err.exists() and now - err.stat().st_mtime < 3 * 3600:
        try:
            row["error"] = json.loads(err.read_text(encoding="utf-8")).get("error", "")[-300:]
        except (OSError, ValueError):
            row["error"] = ""
        row["state"] = "failed"
        return row
    row["state"] = "done" if data.get("version") == VERSION else "due"
    if data:
        row["adoptable"] = {bot: len(b.get("adoptable") or []) for bot, b in (data.get("bots") or {}).items()}
    return row


if __name__ == "__main__":
    try:
        os.nice(10)
    except (AttributeError, OSError):
        pass
    logging.basicConfig(level=logging.INFO)
    sys.path.insert(0, str(SRC_DIR))
    try:
        (_local_dir() / f"{NAME}_error.json").unlink(missing_ok=True)
        out = run()
        print(json.dumps({"ok": out.get("ok"), "tickers": out.get("tickers"), "seconds": out.get("seconds"),
                          "published": out.get("published")}))
    except BaseException:
        import traceback

        from data import setup_backtest_job
        setup_backtest_job._write_error(NAME, traceback.format_exc())  # noqa: SLF001
        _progress(stage="failed")
        raise
    finally:
        (_local_dir() / f"{NAME}.pid").unlink(missing_ok=True)
