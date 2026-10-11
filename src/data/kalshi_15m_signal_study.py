"""The 15m bot's new-signal study (user request 2026-10-08): does
information the 15m market might price slowly predict how a window settles
better than Kalshi's own price does?

  fair      Alpaca's price against the window's strike: the random-walk fair
            value (kalshi_15m_setup.fair_value_yes) against Kalshi's price,
            and the coin's own last 1 and 3 minutes
  leader    the leader's spot on Alpaca (BTC for altcoins, ETH for BTC,
            SILVER for GOLD ...) against the coin's own move
  equities  the US stock market's last 1 and 3 minutes (SPY and QQQ on the
            SIP feed, regular session)
  flow      Kalshi's own order flow in the minute (volume, open-interest
            change, trades against the mid) -- research only: reading it
            live would take one more Kalshi call per market per minute

Every family is a market-relative logistic model: Kalshi's own price (the
logit of its mid) is always an input, so whatever a family adds is
information the price didn't carry. Moves are scaled by each chart's own
one-minute volatility (the same 30-candle measure the live setup reads).

Walk-forward on held-out weeks: each week is traded by a model fit on every
earlier week, at an entry threshold picked on the week before it by a model
that never saw that week; one entry per window (the first minute 1-12 whose
edge clears the threshold), at the real ask with Kalshi's real taker fee for
one contract, held to settlement. A family is adopted only when its
held-out record clears every ADOPT_* bar (enough trades, a t-stat clustered
by window time so coins moving together count once, most weeks up, the
latest weeks up). Runs on HF in its own low-priority process
(python -m data.kalshi_15m_signal_study); the hub shows its progress."""
from __future__ import annotations

import datetime as dt
import json
import logging
import math
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

SRC_DIR = Path(__file__).resolve().parents[1]
NAME = "kalshi15m_signal"
VERSION = 2  # 2: crypto charts cleaned (bar_quality)
HF_PATH = "setup_strategy/signal_study.json"

DECISION_MINUTES = tuple(range(1, 13))          # at least 3 minutes left
THRESHOLDS = (0.0, 0.01, 0.02, 0.03, 0.05, 0.08)  # edge after the fee, per contract
MIN_TRAIN_WEEKS = 3
THETA_MIN_TRADES = 10
STALE_SEC = 120
VOL_CANDLES = 30
Z_CLIP = 5.0
ADOPT_MIN_TRADES = int(os.getenv("KALSHI_15M_SIGNAL_ADOPT_MIN_TRADES", "50") or "50")
# 2.5, not 2: four live families are scored, so one could clear t >= 2 by
# luck alone (about a 1-in-11 chance); 2.5 keeps that near 1 in 40.
ADOPT_MIN_T = float(os.getenv("KALSHI_15M_SIGNAL_ADOPT_MIN_T", "2.5") or "2.5")
ADOPT_MIN_WEEKS_UP = 0.6
ADOPT_RECENT_WEEKS = 4
REFRESH_DAYS = 14  # relearned every 2 weeks (user 2026-10-10)
# The live bot buys YES only (user decision 2026-10-04); the study scores
# both sides so a NO edge would show, but only these sides can be adopted.
LIVE_SIDES = tuple(s for s in (x.strip() for x in os.getenv("KALSHI_15M_SIGNAL_SIDES", "yes").split(","))
                   if s in ("yes", "no")) or ("yes",)

FAIR = ["fair_gap", "own_r1", "own_r3"]
LEADER = ["lead_r1", "lead_r3", "lead_gap3"]
EQUITIES = ["spy_r1", "spy_r3", "qqq_r1", "qqq_r3", "us_open"]
FLOW = ["log_volume", "d_oi", "last_gap", "mean_gap"]
FAMILIES: dict[str, list[str]] = {
    "market only": [],
    "fair": FAIR,
    "fair + leader": FAIR + LEADER,
    "fair + equities": FAIR + EQUITIES,
    "fair + leader + equities": FAIR + LEADER + EQUITIES,
    "flow (research)": FAIR + FLOW,
}
LIVE_FAMILIES = ("fair", "fair + leader", "fair + equities", "fair + leader + equities")


def _local_dir() -> Path:
    from data import setup_backtest_job
    return setup_backtest_job.LOCAL_DIR


# ---------------------------------------------------------------------------
# Features -- one code path for the study's history and the live bot's now
# ---------------------------------------------------------------------------
class Chart:
    """One instrument's 1-minute candles (ts = END) as arrays: log change
    over the last 1 and 3 candles, the volatility of the last VOL_CANDLES
    log changes (as kalshi_15m_setup.live_setup reads it) and the settlement
    reference (the minute's mean for crypto, its close otherwise)."""

    def __init__(self, candles: pd.DataFrame, *, minute_average: bool = True):
        c = candles.sort_values("ts").drop_duplicates("ts")
        self.ts = c["ts"].to_numpy("int64")
        self.close = c["close"].to_numpy(float)
        logc = np.log(self.close)
        self.r1 = np.diff(logc, prepend=np.nan)
        self.r3 = logc - np.concatenate([np.full(min(3, len(logc)), np.nan), logc[:-3]])[:len(logc)]
        self.vol = pd.Series(self.r1).rolling(VOL_CANDLES, min_periods=VOL_CANDLES).std(ddof=1).to_numpy()
        o, h, low = (c[k].to_numpy(float) for k in ("open", "high", "low"))
        self.ref = (o + h + low + self.close) / 4.0 if minute_average else self.close

    def at(self, t: np.ndarray, *, exact: bool = False) -> np.ndarray:
        """Index of the latest candle ending at or before each t (no older
        than STALE_SEC; exactly at t when `exact`), else -1."""
        t = np.asarray(t, dtype="int64")
        if not len(self.ts):
            return np.full(len(t), -1)
        i = np.searchsorted(self.ts, t, side="right") - 1
        j = np.clip(i, 0, None)
        ok = (i >= 0) & ((self.ts[j] == t) if exact else (self.ts[j] >= t - STALE_SEC))
        return np.where(ok, i, -1)

    def scaled(self, i: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """The last 1 and 3 candles' moves in units of the chart's own
        one-minute volatility (0 where unknown), clipped to +-Z_CLIP."""
        j = np.clip(i, 0, None)
        vol = np.where(i >= 0, self.vol[j], np.nan)
        z1 = np.where(i >= 0, self.r1[j], np.nan) / vol
        z3 = np.where(i >= 0, self.r3[j], np.nan) / (vol * math.sqrt(3.0))
        return (np.clip(np.nan_to_num(z1, nan=0.0, posinf=0.0, neginf=0.0), -Z_CLIP, Z_CLIP),
                np.clip(np.nan_to_num(z3, nan=0.0, posinf=0.0, neginf=0.0), -Z_CLIP, Z_CLIP))


def _logit(p: np.ndarray) -> np.ndarray:
    p = np.clip(np.asarray(p, dtype=float), 0.01, 0.99)
    return np.log(p / (1.0 - p))


def features(quotes: pd.DataFrame, own: Chart, leader: Chart | None = None, spy: Chart | None = None,
             qqq: Chart | None = None) -> pd.DataFrame:
    """The decision rows for one coin: `quotes` holds Kalshi's quote at the
    end of window minute `minute` (open_ts, minute, yes_bid, yes_ask; volume,
    open_interest, last, mean for the research family). Rows whose chart
    can't price them (no fresh candle, no volatility yet, no strike candle)
    are dropped."""
    from data import kalshi_15m_setup
    q = quotes.sort_values(["open_ts", "minute"]).reset_index(drop=True)
    open_ts = q["open_ts"].to_numpy("int64")
    minute = q["minute"].to_numpy("int64")
    t = open_ts + 60 * minute
    i, i0 = own.at(t), own.at(open_ts)
    j = np.clip(i, 0, None)
    price = own.close[j]
    vol = own.vol[j]
    strike = own.ref[np.clip(i0, 0, None)]
    minutes_left = 15.0 - minute
    ok = (i >= 0) & (i0 >= 0) & np.isfinite(vol) & (vol > 0)
    fair = np.array([kalshi_15m_setup.fair_value_yes(float(a), float(b), float(v), float(m)) if good else np.nan
                     for a, b, v, m, good in zip(price, strike, vol, minutes_left, ok)])
    out = q.copy()
    bid, ask = q["yes_bid"].to_numpy(float), q["yes_ask"].to_numpy(float)
    mid = (bid + ask) / 2.0
    out["logit_mid"] = _logit(mid)
    out["fair"] = fair
    out["fair_gap"] = np.clip(_logit(fair) - out["logit_mid"].to_numpy(), -Z_CLIP, Z_CLIP)
    out["own_r1"], out["own_r3"] = own.scaled(i)
    if leader is not None:
        out["lead_r1"], out["lead_r3"] = leader.scaled(leader.at(t))
    else:
        out["lead_r1"] = out["lead_r3"] = 0.0
    out["lead_gap3"] = out["lead_r3"] - out["own_r3"]
    for name, chart in (("spy", spy), ("qqq", qqq)):
        k = chart.at(t, exact=True) if chart is not None else np.full(len(t), -1)
        out[f"{name}_r1"], out[f"{name}_r3"] = chart.scaled(k) if chart is not None else (0.0, 0.0)
        if name == "spy":
            out["us_open"] = (k >= 0).astype(float)
    for col in ("volume", "open_interest", "last", "mean"):
        if col not in out:
            out[col] = np.nan
    out["log_volume"] = np.log1p(out["volume"].fillna(0.0).clip(lower=0.0))
    oi = np.log1p(out["open_interest"].fillna(0.0).clip(lower=0.0))
    prev = oi.groupby(out["open_ts"]).shift(1)  # the window's previous minute (rows sorted by window, minute)
    out["d_oi"] = (oi - prev).fillna(0.0)
    out["last_gap"] = (out["last"] - mid).fillna(0.0)
    out["mean_gap"] = (out["mean"] - mid).fillna(0.0)
    keep = ok & (bid > 0) & (ask > bid - 1e-9) & (ask < 1)
    return out[keep].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Model -- market-relative logistic regression, stored as plain numbers
# ---------------------------------------------------------------------------
def fit(df: pd.DataFrame, cols: list[str]) -> dict[str, Any]:
    from sklearn.linear_model import LogisticRegression
    names = ["logit_mid"] + list(cols)
    x = df[names].to_numpy(float)
    mu, sd = x.mean(axis=0), x.std(axis=0)
    sd[sd == 0] = 1.0
    m = LogisticRegression(C=1.0, max_iter=500).fit((x - mu) / sd, df["y"].to_numpy(int))
    return {"cols": names, "mean": mu.tolist(), "std": sd.tolist(), "coef": m.coef_[0].tolist(), "intercept": float(m.intercept_[0])}


def predict(model: dict[str, Any], df: pd.DataFrame) -> np.ndarray:
    x = df[model["cols"]].to_numpy(float)
    z = ((x - np.asarray(model["mean"])) / np.asarray(model["std"])) @ np.asarray(model["coef"]) + model["intercept"]
    return 1.0 / (1.0 + np.exp(-z))


def taker_fee(price: np.ndarray) -> np.ndarray:
    """Kalshi's taker fee for a one-contract order (kalshi_15m.taker_fee_usd)."""
    from data import kalshi_15m
    p = np.asarray(price, dtype=float)
    fee = np.ceil(kalshi_15m.TAKER_FEE_RATE * p * (1.0 - p) * 100.0 - 1e-9) / 100.0
    return np.where((p > 0) & (p < 1), fee, 0.0)


def trade(df: pd.DataFrame, p: np.ndarray, theta: float, sides: tuple[str, ...]) -> pd.DataFrame:
    """One entry per window: the first decision minute whose edge (the
    model's probability minus the ask and fee) clears theta on an allowed
    side, held to settlement."""
    if not np.isfinite(theta) or df.empty:
        return pd.DataFrame(columns=["open_ts", "ticker", "coin", "minute", "side", "price", "edge", "pnl"])
    ask_yes, ask_no = df["yes_ask"].to_numpy(float), 1.0 - df["yes_bid"].to_numpy(float)
    e_yes = p - ask_yes - taker_fee(ask_yes) if "yes" in sides else np.full(len(df), -np.inf)
    e_no = (1.0 - p) - ask_no - taker_fee(ask_no) if "no" in sides else np.full(len(df), -np.inf)
    side = np.where(e_yes >= e_no, "yes", "no")
    edge = np.maximum(e_yes, e_no)
    price = np.where(side == "yes", ask_yes, ask_no)
    cand = df.assign(side=side, edge=edge, price=price)[edge >= theta]
    if cand.empty:
        return pd.DataFrame(columns=["open_ts", "ticker", "coin", "minute", "side", "price", "edge", "pnl"])
    first = cand.sort_values(["ticker", "minute"]).drop_duplicates("ticker")
    won = (first["result"] == first["side"]).astype(float)
    first = first.assign(pnl=won - first["price"] - taker_fee(first["price"].to_numpy()))
    return first[["open_ts", "ticker", "coin", "minute", "side", "price", "edge", "pnl"]].reset_index(drop=True)


def choose_theta(df: pd.DataFrame, p: np.ndarray, sides: tuple[str, ...]) -> float:
    """The threshold that made the most on `df` with at least
    THETA_MIN_TRADES trades; infinite (no trades) when none made money."""
    best, best_total = math.inf, 0.0
    for th in THRESHOLDS:
        t = trade(df, p, th, sides)
        if len(t) >= THETA_MIN_TRADES and t["pnl"].sum() > best_total:
            best, best_total = th, float(t["pnl"].sum())
    return best


def clustered_t(trades: pd.DataFrame) -> float | None:
    """t-stat of the mean P&L with every window time counted once (coins
    in the same quarter hour move together)."""
    if trades.empty:
        return None
    per_slot = trades.groupby("open_ts")["pnl"].sum()
    if len(per_slot) < 3 or per_slot.std(ddof=1) == 0:
        return None
    return float(per_slot.mean() / (per_slot.std(ddof=1) / math.sqrt(len(per_slot))))


def walk_forward(df: pd.DataFrame, cols: list[str], sides: tuple[str, ...],
                 fits: dict[tuple, dict[str, Any]] | None = None) -> dict[str, Any]:
    """Every week after the first MIN_TRAIN_WEEKS traded out of sample.
    `fits` shares the models between side sets (a model never depends on
    which sides are traded)."""
    fits = {} if fits is None else fits

    def model_before(week: str) -> dict[str, Any]:
        key = (tuple(cols), week)
        if key not in fits:
            fits[key] = fit(df[df["week"] < week], cols)
        return fits[key]

    weeks = sorted(df["week"].unique())
    rows, frames = [], []
    for k in range(MIN_TRAIN_WEEKS, len(weeks)):
        val = df[df["week"] == weeks[k - 1]]
        theta = choose_theta(val, predict(model_before(weeks[k - 1]), val), sides)
        test = df[df["week"] == weeks[k]]
        t = trade(test, predict(model_before(weeks[k]), test), theta, sides)
        rows.append({"week": str(weeks[k]), "theta": None if not np.isfinite(theta) else theta, "trades": int(len(t)),
                     "pnl": round(float(t["pnl"].sum()), 4)})
        frames.append(t)
    held = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["pnl", "open_ts", "coin"])
    traded_weeks = [r for r in rows if r["trades"] > 0]
    recent = rows[-ADOPT_RECENT_WEEKS:]
    out = {"weeks": rows, "trades": int(len(held)), "total": round(float(held["pnl"].sum()), 4),
           "avg": round(float(held["pnl"].mean()), 4) if len(held) else None,
           "win_rate": round(float((held["pnl"] > 0).mean()), 4) if len(held) else None,
           "t_clustered": None if (tc := clustered_t(held)) is None else round(tc, 2),
           "weeks_up": round(sum(r["pnl"] > 0 for r in traded_weeks) / len(traded_weeks), 3) if traded_weeks else None,
           "recent_total": round(sum(r["pnl"] for r in recent), 4),
           "by_coin": {c: {"trades": int(len(g)), "total": round(float(g["pnl"].sum()), 4)} for c, g in held.groupby("coin")}
           if len(held) else {}}
    out["adopt"] = bool(out["trades"] >= ADOPT_MIN_TRADES and out["total"] > 0 and (out["t_clustered"] or 0) >= ADOPT_MIN_T
                        and (out["weeks_up"] or 0) >= ADOPT_MIN_WEEKS_UP and out["recent_total"] > 0)
    return out


# ---------------------------------------------------------------------------
# The study run (its own process on HF) and its published result
# ---------------------------------------------------------------------------
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


def _charts(coins: list[str], years: list[int]) -> dict[str, Chart]:
    """Every chart the study reads, from the same Alpaca archives as the
    live charts: crypto on the Kraken US feed, commodities on their SIP
    ETF's regular session, SPY and QQQ on the SIP feed's regular session."""
    from data import alpaca_crypto_history, alpaca_setup, alpaca_sip_history, kalshi_15m_setup, setup_backtest_job
    charts: dict[str, Chart] = {}
    wanted = set(coins) | {kalshi_15m_setup.leader_for(c) for c in coins}
    for sym in sorted(wanted):
        if sym in kalshi_15m_setup.METAL_CHART_SYMBOL:
            candles = setup_backtest_job._study_candles(sym)[0]  # noqa: SLF001
        else:
            from data import bar_quality
            candles = alpaca_crypto_history.candles(sym, years=years)
            candles = bar_quality.clean(candles, kind="crypto")[0] if candles is not None and not candles.empty else candles
        if candles is not None and not candles.empty:
            charts[sym] = Chart(candles, minute_average=kalshi_15m_setup.settles_on_minute_average(sym))
    for sym in ("SPY", "QQQ"):
        candles = alpaca_setup.regular_session_candles(alpaca_sip_history.load(sym, years=years))
        if not candles.empty:
            charts[sym] = Chart(candles, minute_average=False)
    return charts


def build_dataset(quotes: pd.DataFrame, charts: dict[str, Chart]) -> pd.DataFrame:
    from data import kalshi_15m_setup
    q = quotes[quotes["minute"].isin(DECISION_MINUTES) & quotes["result"].isin(["yes", "no"])]
    frames = []
    for coin, cq in q.groupby("coin"):
        if coin not in charts:
            continue
        f = features(cq, charts[coin], charts.get(kalshi_15m_setup.leader_for(coin)), charts.get("SPY"), charts.get("QQQ"))
        if not f.empty:
            frames.append(f)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df["y"] = (df["result"] == "yes").astype(int)
    df["week"] = pd.to_datetime(df["open_ts"], unit="s", utc=True).dt.tz_localize(None).dt.to_period("W-SUN").astype(str)
    return df


def run(*, publish: bool = True) -> dict[str, Any]:
    from data import kalshi_15m_quotes, kalshi_15m_strategy
    started = time.time()
    _progress(stage="loading Kalshi's quotes and the Alpaca charts", done=0, total=len(FAMILIES) * 2,
              started_at=dt.datetime.now(dt.timezone.utc).isoformat())
    quotes = kalshi_15m_quotes.load_quote_history(days=400)
    coins = sorted(c for c in kalshi_15m_strategy.ACTIVE_ENTRY_COINS if c in set(quotes["coin"]))
    years = sorted({int(y) for y in pd.to_datetime(quotes["open_ts"], unit="s", utc=True).dt.year.unique()})
    charts = _charts(coins, years)
    df = build_dataset(quotes, charts)
    result: dict[str, Any] = {"ok": not df.empty, "version": VERSION, "computed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                              "coins": sorted(df["coin"].unique()) if not df.empty else [], "rows": int(len(df)),
                              "windows": int(df["ticker"].nunique()) if not df.empty else 0,
                              "weeks": sorted(df["week"].unique()) if not df.empty else [], "live_sides": list(LIVE_SIDES),
                              "bar": {"min_trades": ADOPT_MIN_TRADES, "min_t": ADOPT_MIN_T, "min_weeks_up": ADOPT_MIN_WEEKS_UP,
                                      "recent_weeks": ADOPT_RECENT_WEEKS}, "families": {}}
    if not df.empty and len(result["weeks"]) > MIN_TRAIN_WEEKS:
        done, fits = 0, {}
        for family, cols in FAMILIES.items():
            result["families"][family] = {}
            for sides in (("yes",), ("yes", "no")):
                _progress(stage=f"walk-forward: {family}, {'+'.join(sides)}", done=done)
                result["families"][family]["+".join(sides)] = walk_forward(df, cols, sides, fits)
                done += 1
                _progress(done=done)
        live_key = "+".join(LIVE_SIDES)
        winners = [(f, result["families"][f][live_key]) for f in LIVE_FAMILIES if result["families"][f].get(live_key, {}).get("adopt")]
        if winners:
            family, record = max(winners, key=lambda w: w[1]["total"])
            cols = FAMILIES[family]
            weeks = result["weeks"]
            inner = fit(df[df["week"] < weeks[-1]], cols)
            last = df[df["week"] == weeks[-1]]
            theta = choose_theta(last, predict(inner, last), LIVE_SIDES)
            result["adopted"] = {"enforce": bool(np.isfinite(theta)), "family": family, "sides": list(LIVE_SIDES),
                                 "theta": theta if np.isfinite(theta) else None, "model": fit(df, cols), "held_out": record}
        else:
            result["adopted"] = {"enforce": False, "reason": "no family cleared the bar on held-out weeks"}
    else:
        result["adopted"] = {"enforce": False, "reason": f"needs more than {MIN_TRAIN_WEEKS} weeks of quotes"}
    result["seconds"] = round(time.time() - started)
    path = _local_dir() / f"{NAME}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, default=str), encoding="utf-8")
    if publish:
        _progress(stage="publishing")
        result["published"] = _publish(result)
        path.write_text(json.dumps(result, default=str), encoding="utf-8")
    _progress(stage="done", done=len(FAMILIES) * 2)
    return result


def _publish(result: dict[str, Any]) -> bool:
    token = os.getenv("HF_API_KEY", "")
    if not token:
        return False
    try:
        from huggingface_hub import HfApi

        from data import setup_backtest_job
        HfApi(token=token).upload_file(path_or_fileobj=json.dumps(result, indent=2, default=str).encode(), path_in_repo=HF_PATH,
                                       repo_id=setup_backtest_job.REPOS["kalshi15m"], repo_type="model",
                                       commit_message="15m new-signal study")
        return True
    except Exception as exc:
        logger.warning("[kalshi_15m_signal_study] publish failed: %s", exc)
        return False


_latest_cache: dict[str, Any] = {}


def latest() -> dict[str, Any] | None:
    """The latest study (local result, else the published one on HF;
    cached ten minutes, or until the study writes a newer result)."""
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
                path = call_with_hard_timeout(lambda: hf_hub_download(setup_backtest_job.REPOS["kalshi15m"], HF_PATH, repo_type="model",
                                                                      token=token), timeout_sec=10)
                data = json.loads(Path(path).read_text(encoding="utf-8")) if path else None
            except Exception:
                data = None
    _latest_cache["v"] = (time.time(), data)
    return data


def adopted() -> dict[str, Any]:
    """What the live bot may trade: the adopted family's model and
    threshold, or {"enforce": False}."""
    data = latest() or {}
    a = data.get("adopted") or {}
    return a if a.get("enforce") else {"enforce": False, "reason": a.get("reason") or "no study yet"}


def summary() -> dict[str, Any]:
    """The board's line: each live family's held-out record on the live
    sides, and the verdict."""
    data = latest() or {}
    key = "+".join(data.get("live_sides") or LIVE_SIDES)
    fams = {f: {k: (v.get(key) or {}).get(k) for k in ("trades", "avg", "total", "t_clustered", "weeks_up", "adopt")}
            for f, v in (data.get("families") or {}).items()}
    return {"computed_at": data.get("computed_at"), "windows": data.get("windows"), "weeks": len(data.get("weeks") or []),
            "families": fams, "adopted": adopted(), "running": _running()}


def overview_row(now: float | None = None) -> dict[str, Any]:
    """The hub's processing-panel row: running (stage, families done,
    elapsed, last sign of progress), failed, up to date with the verdict,
    or due."""
    now = time.time() if now is None else now
    data = latest() or {}
    row: dict[str, Any] = {"name": "15m new-signal study", "last_published_at": data.get("computed_at"),
                           "last_seconds": data.get("seconds"), "windows": data.get("windows"), "weeks": len(data.get("weeks") or [])}
    best = None
    key = "+".join(data.get("live_sides") or LIVE_SIDES)
    for fam in LIVE_FAMILIES:
        rec = ((data.get("families") or {}).get(fam) or {}).get(key)
        if rec and (best is None or rec["total"] > best[1]["total"]):
            best = (fam, rec)
    if best:
        row["best"] = {"family": best[0], **{k: best[1].get(k) for k in ("trades", "avg", "total", "t_clustered", "weeks_up", "adopt")}}
    row["adopted"] = adopted()
    if _running():
        p = progress() or {}
        started = p.get("started_at")
        try:
            elapsed = now - dt.datetime.fromisoformat(started).timestamp()
        except (TypeError, ValueError):
            elapsed = None
        idle = now - float(p.get("updated_at") or now)
        pct = 100.0 * float(p.get("done") or 0) / float(p.get("total") or 1)
        row.update(state="stalled" if idle > 45 * 60 else "running",
                   progress={"stage": p.get("stage"), "done": p.get("done"), "total": p.get("total"), "percent": round(pct, 1),
                             "elapsed_sec": elapsed, "idle_sec": max(0.0, idle)})
        return row
    err = _local_dir() / f"{NAME}_error.json"
    if err.exists() and now - err.stat().st_mtime < 6 * 3600:
        try:
            row["error"] = json.loads(err.read_text(encoding="utf-8")).get("error", "")[-300:]
        except (OSError, ValueError):
            row["error"] = ""
        row.update(state="failed", retry_at=dt.datetime.fromtimestamp(err.stat().st_mtime + 6 * 3600, dt.timezone.utc).isoformat())
        return row
    try:
        age_days = (now - dt.datetime.fromisoformat(str(data.get("computed_at"))).timestamp()) / 86400
    except (TypeError, ValueError):
        age_days = math.inf
    row["state"] = "done" if data.get("version") == VERSION and age_days < REFRESH_DAYS else "due"
    return row


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
    proc = subprocess.Popen([sys.executable, "-m", "data.kalshi_15m_signal_study"], cwd=str(SRC_DIR), env=dict(os.environ),
                            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    (d / f"{NAME}.pid").write_text(str(proc.pid))
    return {"ok": True, "action": "launched", "pid": proc.pid}


def maybe_start() -> dict[str, Any]:
    """Checked on the Space: run the study when it has no result of this
    version from the last REFRESH_DAYS, and no multi-year study is using
    the cores."""
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
    if data.get("version") == VERSION and age_days < REFRESH_DAYS:
        return {"ok": True, "action": "fresh"}
    err = _local_dir() / f"{NAME}_error.json"
    if err.exists() and time.time() - err.stat().st_mtime < 6 * 3600:
        return {"ok": True, "action": "failed_recently"}
    _latest_cache.clear()
    return launch()


if __name__ == "__main__":
    try:
        os.nice(15)
    except (AttributeError, OSError):
        pass
    logging.basicConfig(level=logging.INFO)
    sys.path.insert(0, str(SRC_DIR))
    try:
        (_local_dir() / f"{NAME}_error.json").unlink(missing_ok=True)
        out = run()
        print(json.dumps({k: out.get(k) for k in ("ok", "windows", "rows", "seconds", "published")}, default=str))
        print(json.dumps(out.get("adopted"), default=str)[:2000])
    except BaseException:
        import traceback

        from data import setup_backtest_job
        setup_backtest_job._write_error(NAME, traceback.format_exc())  # noqa: SLF001
        _progress(stage="failed")
        raise
    finally:
        (_local_dir() / f"{NAME}.pid").unlink(missing_ok=True)
