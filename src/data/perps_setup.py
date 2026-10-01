"""Kalshi perps price-action setup -- this bot's own implementation of the
trading method, independent of every other bot's copy:

  1. Price action   15-minute trend from confirmed swing points: higher
                    highs AND higher lows for longs (lower highs and lower
                    lows for shorts), structure intact; breakout candles,
                    rejection wicks and engulfing candles read on 5-minute
                    candles.
  2. Support and    zones built from earlier swing points that already
     resistance     stopped price. Two setups: a 5-minute close through
                    the zone that retests and holds it with a reaction
                    candle (breakout_retest), or a close through support
                    that is reclaimed within 30 minutes -- a failed
                    breakdown / bear trap (failed_breakdown). A close back
                    inside the zone after a breakout is a failed breakout.
  3. Volume and     the breakout (or reclaim) candle on at least VOLUME_MULT
     VWAP           x the recent average volume; price above a rising
                    session VWAP (below a falling one for shorts).
  4. RSI and MACD   momentum confirmation (RSI above 50 and MACD histogram
                    positive) or a supporting divergence; a divergence
                    against the trade blocks it.
  5. News           news sentiment against the trade blocks it.
  6. Correlation    when the symbol moves with its market leader (see
                    LEADER_OVERRIDES / DEFAULT_LEADER), the leader must
                    not be trending against the trade (correlation_check).
                    Across all bots, global_correlation_monitor caps
                    open positions on effectively the same bet.

Then the plan: invalidation is the far side of the zone (or the trap low),
the stop sits STOP_BUFFER_ATR15 x the 15-minute ATR beyond it, the target is the next
opposing zone (or a measured move), and the trade is skipped unless
reward/risk after costs is at least MIN_RR. Exits are the planned stop or
target only.

Kalshi perps specifics: long and short (shorts only when perps_strategy
ENABLE_SHORTS is on). The chart is the coin's real Coinbase spot 1-minute
candles (chart_candles) -- the perp itself goes idle and prints outliers,
so its own volume can't carry the volume rule. The plan's stop and target
are carried into the perp's price by plan_at_price, which re-checks
reward/risk at the real execution price. Costs are Kalshi's real perp fee
(both legs, passed in by perps_strategy) plus the perp's live bid/ask
spread; session VWAP resets at 00:00 UTC. risk_sized_count sizes the
position so the stop loses a fixed share of the balance.

prepare() builds candles and indicators from 1-minute OHLCV; evaluate()
uses only candles closed by as_of, so replay() (backtest) and the live bot
run the same code. Shorts run the long rules on a mirrored chart. All
timestamps are candle END times in unix seconds.
"""
from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


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


PIVOT_LEFT = 2
PIVOT_RIGHT = 2
ZONE_LOOKBACK_15M = _env_int("PERPS_SETUP_ZONE_LOOKBACK_15M", 192)          # 2 days of 15m candles
ZONE_CLUSTER_ATR = _env_float("PERPS_SETUP_ZONE_CLUSTER_ATR", 0.35)
BREAKOUT_LOOKBACK_5M = _env_int("PERPS_SETUP_BREAKOUT_LOOKBACK_5M", 12)      # breakout within the last hour
VOLUME_LOOKBACK_5M = 20
VOLUME_MULT = _env_float("PERPS_SETUP_VOLUME_MULT", 1.5)
RETEST_TOL_ATR = _env_float("PERPS_SETUP_RETEST_TOL_ATR", 0.25)
# Walk-forward (real Coinbase 1m, Jul-Sep 2026, maker costs): picked on the
# first half among stop buffer {0.5, 1, 1.5} x reward/risk {1.5, 2, 3}, the
# unseen second half lost -0.08%/trade vs -0.44% for 0.5 / 2.0.
STOP_BUFFER_ATR15 = _env_float("PERPS_SETUP_STOP_BUFFER_ATR15", 1.5)     # stop sits beyond the invalidation zone by 1.5 x the 15m ATR
STRONG_CLOSE_POSITION = 0.6                                           # close in the top 40% of the candle's range
FAIL_WINDOW_5M = 6                                                    # a failed breakdown must be reclaimed within 30 minutes
VWAP_SLOPE_MINUTES = 60
MIN_RR = _env_float("PERPS_SETUP_MIN_RR", 3.0)
MIN_RISK_PCT = _env_float("PERPS_SETUP_MIN_RISK_PCT", 0.001)
MAX_RISK_PCT = _env_float("PERPS_SETUP_MAX_RISK_PCT", 0.03)
RISK_PER_TRADE_PCT = _env_float("PERPS_SETUP_RISK_PER_TRADE_PCT", 0.01)  # balance lost if the stop is hit
NEWS_BLOCK = _env_float("PERPS_SETUP_NEWS_BLOCK", 0.3)

# 6. Correlation -- the coin's leader is BTC (ETH for BTC itself), read on its Coinbase chart. Over the last
# CORR_LOOKBACK_5M closed 5-minute returns, when the symbol moves with its
# leader (|correlation| >= CORR_MIN) the leader must not be going the other
# way right now: leader above its session VWAP with a rising last hour is
# "up", below it with a falling hour "down", anything else "mixed" (never
# blocks). A weakly correlated symbol trades on its own chart.
CORR_LOOKBACK_5M = _env_int("PERPS_SETUP_CORR_LOOKBACK_5M", 576)
CORR_MIN = _env_float("PERPS_SETUP_CORR_MIN", 0.5)
CORR_MIN_OVERLAP = 60
COVERAGE_WINDOW_MIN = _env_int("PERPS_SETUP_COVERAGE_WINDOW_MIN", 240)
MIN_COVERAGE = _env_float("PERPS_SETUP_MIN_COVERAGE", 0.8)            # share of 1m candles that must exist
LEADER_OVERRIDES: dict[str, str] = {"BTC": "ETH", "GOLD": "SILVER", "SILVER": "GOLD"}
DEFAULT_LEADER = "BTC"


def leader_for(symbol: str) -> str:
    return LEADER_OVERRIDES.get(symbol, DEFAULT_LEADER)
DIVERGENCE_RSI_POINTS = 1.0
MIN_BARS_15M = 30
MIN_BARS_5M = VOLUME_LOOKBACK_5M + 5

SESSION = "utc_day"
MAX_HOLD_SAFETY_MINUTES = _env_int("PERPS_SETUP_MAX_HOLD_SAFETY_MINUTES", 1440)
STALE_AFTER_SEC = 180

CHECK_ORDER = ["data", "trend", "breakout", "volume", "retest", "hold", "vwap", "momentum", "divergence", "news", "risk_reward", "correlation"]


# ---------------------------------------------------------------------------
# Candles and indicators
# ---------------------------------------------------------------------------

def resample(df1: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """1-minute OHLCV (ts = candle end) -> `minutes` candles aligned to the
    clock, ts = candle end. The last candle may still be forming; callers
    filter by as_of."""
    if df1.empty:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    step = minutes * 60
    d = df1.sort_values("ts")
    end = ((d["ts"].astype("int64") - 1) // step + 1) * step
    g = d.groupby(end.values)
    out = pd.DataFrame({
        "open": g["open"].first(), "high": g["high"].max(), "low": g["low"].min(),
        "close": g["close"].last(), "volume": g["volume"].sum(),
    })
    return out.rename_axis("ts").reset_index()


def wilder_rsi(close: np.ndarray, n: int = 14) -> np.ndarray:
    s = pd.Series(close, dtype=float)
    delta = s.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    rsi = rsi.where(~((loss == 0) & (gain > 0)), 100.0)
    return rsi.to_numpy()


def macd_hist(close: np.ndarray) -> np.ndarray:
    s = pd.Series(close, dtype=float)
    line = s.ewm(span=12, adjust=False).mean() - s.ewm(span=26, adjust=False).mean()
    return (line - line.ewm(span=9, adjust=False).mean()).to_numpy()


def wilder_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, n: int = 14) -> np.ndarray:
    prev = np.concatenate([[np.nan], close[:-1]])
    tr = np.nanmax(np.vstack([high - low, np.abs(high - prev), np.abs(low - prev)]), axis=0)
    return pd.Series(tr).ewm(alpha=1 / n, adjust=False, min_periods=n).mean().to_numpy()


def session_keys(ts: pd.Series, session: str) -> pd.Series:
    """Which trading session each 1-minute candle belongs to: UTC calendar
    day for 24/7 crypto, the New York trading day for stocks."""
    t = pd.to_datetime(ts.astype("int64") - 1, unit="s", utc=True)
    if session == "us_equity":
        return t.dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d")
    return t.dt.strftime("%Y-%m-%d")


def session_vwap(df1: pd.DataFrame, session: str) -> np.ndarray:
    d = df1.sort_values("ts")
    tp = (d["high"] + d["low"] + d["close"]) / 3.0
    vol = d["volume"].astype(float).clip(lower=0)
    keys = session_keys(d["ts"], session).values
    pv = pd.Series((tp * vol).values).groupby(keys).cumsum().values
    vv = pd.Series(vol.values).groupby(keys).cumsum().values
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(vv > 0, pv / vv, np.nan)


@dataclass
class Frame:
    """One timeframe's candles + indicators as plain arrays."""
    ts: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    atr: np.ndarray
    rsi: np.ndarray
    macd: np.ndarray
    ph: np.ndarray   # index is a confirmed pivot high (known PIVOT_RIGHT candles later)
    pl: np.ndarray

    @classmethod
    def build(cls, bars: pd.DataFrame) -> "Frame":
        h, lo, c = (bars[k].to_numpy(float) for k in ("high", "low", "close"))
        n = len(bars)
        ph, pl = np.zeros(n, bool), np.zeros(n, bool)
        for i in range(PIVOT_LEFT, n - PIVOT_RIGHT):
            left_h, right_h = h[i - PIVOT_LEFT:i], h[i + 1:i + 1 + PIVOT_RIGHT]
            left_l, right_l = lo[i - PIVOT_LEFT:i], lo[i + 1:i + 1 + PIVOT_RIGHT]
            ph[i] = h[i] > left_h.max() and h[i] >= right_h.max()
            pl[i] = lo[i] < left_l.min() and lo[i] <= right_l.min()
        return cls(
            ts=bars["ts"].to_numpy("int64"), open=bars["open"].to_numpy(float), high=h, low=lo, close=c,
            volume=bars["volume"].to_numpy(float), atr=wilder_atr(h, lo, c), rsi=wilder_rsi(c), macd=macd_hist(c),
            ph=ph, pl=pl,
        )

    def mirrored(self) -> "Frame":
        return Frame(
            ts=self.ts, open=-self.open, high=-self.low, low=-self.high, close=-self.close, volume=self.volume,
            atr=self.atr, rsi=100.0 - self.rsi, macd=-self.macd, ph=self.pl, pl=self.ph,
        )


@dataclass
class Context:
    f5: Frame
    f15: Frame
    ts1: np.ndarray
    vwap: np.ndarray
    step5: int = 300
    step15: int = 900
    session: str = "utc_day"

    def mirrored(self) -> "Context":
        return Context(self.f5.mirrored(), self.f15.mirrored(), self.ts1, -self.vwap, self.step5, self.step15, self.session)


def prepare(df1: pd.DataFrame, *, session: str = "utc_day") -> Context:
    """df1: 1-minute candles with ts (candle end), open, high, low, close,
    volume. session: "utc_day" (crypto) or "us_equity" (stocks)."""
    d = df1.dropna(subset=["open", "high", "low", "close"]).sort_values("ts").drop_duplicates("ts")
    d = d.assign(volume=d["volume"].fillna(0.0))
    return Context(
        f5=Frame.build(resample(d, 5)), f15=Frame.build(resample(d, 15)),
        ts1=d["ts"].to_numpy("int64"), vwap=session_vwap(d, session), session=session,
    )


# ---------------------------------------------------------------------------
# Setup evaluation
# ---------------------------------------------------------------------------

def _zones(f15: Frame, idx: np.ndarray, atr15: float, *, use: str = "high") -> list[dict[str, Any]]:
    """Support/resistance ZONES from confirmed swing points at the given 15m
    indices (swing highs -> resistance, swing lows -> support), clustered
    when within ZONE_CLUSTER_ATR * ATR of each other. A zone's low/high span
    its member swing points; touches = how many times price turned there."""
    prices = f15.high if use == "high" else f15.low
    pts = sorted((float(prices[i]), int(i)) for i in idx)
    zones: list[dict[str, Any]] = []
    for price, i in pts:
        if zones and price - zones[-1]["high"] <= ZONE_CLUSTER_ATR * atr15:
            z = zones[-1]
            z["high"] = price
            z["touches"] += 1
            z["pivots"].append(i)
        else:
            zones.append({"low": price, "high": price, "touches": 1, "pivots": [i]})
    return zones


def _candle_shape(f: Frame, j: int) -> dict[str, Any]:
    """Price-action read of one candle, in the trade's direction (shorts are
    read on the mirrored chart): whether it closed with the trade, where it
    closed in its range, body size, a rejection wick against the trade, an
    engulfing of the prior candle."""
    rng = max(f.high[j] - f.low[j], 1e-12)
    body = abs(f.close[j] - f.open[j])
    lower_wick = min(f.open[j], f.close[j]) - f.low[j]
    return {
        "with_trade": bool(f.close[j] > f.open[j]),
        "close_position": round(float((f.close[j] - f.low[j]) / rng), 2),
        "body_pct": round(float(body / rng), 2),
        "rejection_wick": bool(lower_wick >= 2 * body and lower_wick >= 0.5 * rng),
        "engulfing": bool(j > 0 and f.close[j] > f.open[j] and f.close[j - 1] < f.open[j - 1]
                          and f.close[j] >= f.open[j - 1] and f.open[j] <= f.close[j - 1]),
    }


def _reaction_ok(shape: dict[str, Any]) -> bool:
    """Buyers visibly defended the level: a bullish candle that is a
    rejection wick, an engulfing candle, or a close in the top of its range."""
    return shape["with_trade"] and (shape["rejection_wick"] or shape["engulfing"] or shape["close_position"] >= STRONG_CLOSE_POSITION)


def _vwap_at(ctx: Context, ts: int) -> float | None:
    i = int(np.searchsorted(ctx.ts1, ts, side="right")) - 1
    return float(ctx.vwap[i]) if i >= 0 and np.isfinite(ctx.vwap[i]) else None


def _plan(close: float, stop: float, zones_above: list[dict[str, Any]], measured_target: float, *,
          fee_rate_roundtrip: float, spread_bps: float, min_rr: float) -> tuple[bool, dict[str, Any]]:
    overhead = [z for z in zones_above if z["low"] > close]
    if overhead:
        target, basis = float(min(z["low"] for z in overhead)), "next resistance"
    else:
        target, basis = float(measured_target), "measured move"
    # Shorts are planned on the mirrored (negated) chart, so percentages and
    # costs are measured against the absolute price.
    ref = abs(close)
    risk, reward = close - stop, target - close
    fee_cost = ref * (fee_rate_roundtrip + spread_bps / 1e4)
    rr = reward / risk if risk > 0 else 0.0
    rr_net = (reward - fee_cost) / (risk + fee_cost) if risk > 0 else 0.0
    plan = {"entry": close, "stop": float(stop), "target": target, "target_basis": basis,
            "risk_pct": round(risk / ref, 5), "reward_pct": round(reward / ref, 5),
            "rr": round(rr, 2), "rr_net": round(rr_net, 2), "fee_cost_pct": round(fee_cost / ref, 5)}
    ok = reward > 0 and MIN_RISK_PCT <= risk / ref <= MAX_RISK_PCT and rr_net >= min_rr
    return ok, plan


def _evaluate_long(ctx: Context, as_of: int, *, fee_rate_roundtrip: float, spread_bps: float,
                   news_score: float | None, min_rr: float) -> dict[str, Any]:
    """Long view. Two setups, both requiring the 15m uptrend:
      breakout_retest  -- 5m close through resistance on volume, a retest
                          of the zone that holds with a bullish reaction.
      failed_breakdown -- 5m close below support that is reclaimed (close
                          back above the whole zone) on volume within
                          FAIL_WINDOW_5M candles: a bear trap in an uptrend.
    The first rule that fails is reported; if both setups fail, the one
    that got further through the checklist is returned."""
    f5, f15 = ctx.f5, ctx.f15
    n5 = int(np.searchsorted(f5.ts, as_of, side="right"))
    n15 = int(np.searchsorted(f15.ts, as_of, side="right"))
    if n5 < MIN_BARS_5M or n15 < MIN_BARS_15M or not np.isfinite(f15.atr[n15 - 1]) or not np.isfinite(f5.atr[n5 - 1]):
        return {"valid": False, "reason": "data", "checks": {"data": {"ok": False, "bars_5m": n5, "bars_15m": n15}}}
    last = n5 - 1
    close = float(f5.close[last])
    atr5, atr15 = float(f5.atr[last]), float(f15.atr[n15 - 1])
    base_checks: dict[str, Any] = {"data": {"ok": True}}

    # 1. Trend on the 15m chart from confirmed swing points.
    known = n15 - PIVOT_RIGHT
    highs = list(np.flatnonzero(f15.ph[:max(known, 0)]))
    lows = list(np.flatnonzero(f15.pl[:max(known, 0)]))
    if len(highs) < 2 or len(lows) < 2:
        base_checks["trend"] = {"ok": False, "detail": "not enough swing points"}
        return {"valid": False, "reason": "trend", "checks": base_checks}
    h1, h2, l1, l2 = highs[-2], highs[-1], lows[-2], lows[-1]
    hh, hl, intact = f15.high[h2] > f15.high[h1], f15.low[l2] > f15.low[l1], close > f15.low[l2]
    trend = {"higher_high": bool(hh), "higher_low": bool(hl), "structure_intact": bool(intact),
             "swing_highs": [float(f15.high[h1]), float(f15.high[h2])], "swing_lows": [float(f15.low[l1]), float(f15.low[l2])]}
    base_checks["trend"] = {"ok": bool(hh and hl and intact), **trend}
    if not (hh and hl and intact):
        return {"valid": False, "reason": "trend", "checks": base_checks}

    lookback_from = known - ZONE_LOOKBACK_15M
    resistance = _zones(f15, np.array([i for i in highs if i >= lookback_from], dtype=int), atr15, use="high")
    support = _zones(f15, np.array([i for i in lows if i >= lookback_from], dtype=int), atr15, use="low")

    def formed_before(z: dict[str, Any], ts: int) -> bool:
        return max(f15.ts[p + PIVOT_RIGHT] for p in z["pivots"]) <= ts

    def avg_volume(k: int) -> float:
        return float(np.mean(f5.volume[max(0, k - VOLUME_LOOKBACK_5M):k])) if k > 0 else 0.0

    def finish(setup: str, checks: dict[str, Any], stop: float, measured: float, setup_id: str) -> dict[str, Any]:
        """Shared tail: VWAP, momentum, divergence, news, plan."""
        def fail(name: str, **info: Any) -> dict[str, Any]:
            checks.pop("_bullish_divergence", None)
            checks[name] = {"ok": False, **info}
            return {"valid": False, "reason": name, "setup": setup, "checks": checks}

        vwap_now, vwap_then = _vwap_at(ctx, as_of), _vwap_at(ctx, as_of - VWAP_SLOPE_MINUTES * 60)
        if vwap_now is not None:
            rising = vwap_then is None or vwap_now >= vwap_then
            if close <= vwap_now or not rising:
                return fail("vwap", close=close, vwap=vwap_now, vwap_rising=rising)
        checks["vwap"] = {"ok": True, "vwap": vwap_now}

        rsi5, macd5 = float(f5.rsi[last]), float(f5.macd[last])
        confirmed = rsi5 > 50 and macd5 > 0
        bullish_div = checks.get("_bullish_divergence", False)
        if not (confirmed or bullish_div):
            return fail("momentum", rsi=round(rsi5, 1), macd_hist=macd5, bullish_divergence=bullish_div)
        checks["momentum"] = {"ok": True, "rsi": round(rsi5, 1), "macd_hist": macd5, "via": "confirmation" if confirmed else "bullish divergence"}

        bearish_div = bool(f15.high[h2] > f15.high[h1] and f15.rsi[h2] < f15.rsi[h1] - DIVERGENCE_RSI_POINTS)
        if bearish_div:
            return fail("divergence", price_highs=[float(f15.high[h1]), float(f15.high[h2])],
                        rsi_at_highs=[round(float(f15.rsi[h1]), 1), round(float(f15.rsi[h2]), 1)])
        checks["divergence"] = {"ok": True}

        if news_score is not None and news_score <= -NEWS_BLOCK:
            return fail("news", sentiment=news_score)
        checks["news"] = {"ok": True, "sentiment": news_score}

        ok, plan = _plan(close, stop, resistance, measured, fee_rate_roundtrip=fee_rate_roundtrip,
                         spread_bps=spread_bps, min_rr=min_rr)
        checks.pop("_bullish_divergence", None)
        if not ok:
            checks["risk_reward"] = {"ok": False, **plan, "min_rr": min_rr}
            return {"valid": False, "reason": "risk_reward", "setup": setup, "checks": checks, "plan": plan}
        checks["risk_reward"] = {"ok": True, **plan}
        return {"valid": True, "reason": "setup_confirmed", "setup": setup, "checks": checks, "plan": plan, "setup_id": setup_id}

    # 2-3a. Breakout -> retest -> hold.
    def breakout_retest() -> dict[str, Any]:
        checks = dict(base_checks)

        def fail(name: str, **info: Any) -> dict[str, Any]:
            checks[name] = {"ok": False, **info}
            return {"valid": False, "reason": name, "setup": "breakout_retest", "checks": checks}

        found = None
        for k in range(last, max(last - BREAKOUT_LOOKBACK_5M, 1) - 1, -1):
            for z in sorted(resistance, key=lambda z: -z["high"]):
                if formed_before(z, int(f5.ts[k]) - ctx.step5) and f5.close[k] > z["high"] >= f5.close[k - 1]:
                    found = (k, z)
                    break
            if found:
                break
        if found is None:
            return fail("breakout", zones=[{"low": round(z["low"], 6), "high": round(z["high"], 6), "touches": z["touches"]} for z in resistance[-4:]])
        k, zone = found
        shape_k = _candle_shape(f5, k)
        checks["breakout"] = {"ok": True, "zone_low": float(zone["low"]), "zone_high": float(zone["high"]), "touches": zone["touches"],
                              "breakout_ts": int(f5.ts[k]), "breakout_close": float(f5.close[k]), "candle": shape_k}
        if not (shape_k["with_trade"] and shape_k["close_position"] >= STRONG_CLOSE_POSITION):
            return fail("breakout", weak_breakout_candle=True, candle=shape_k)
        ratio = float(f5.volume[k] / avg_volume(k)) if avg_volume(k) > 0 else 0.0
        if ratio < VOLUME_MULT:
            return fail("volume", volume_ratio=round(ratio, 2), required=VOLUME_MULT)
        checks["volume"] = {"ok": True, "volume_ratio": round(ratio, 2)}
        after = range(k + 1, last + 1)
        if any(f5.close[j] < zone["low"] for j in after):
            return fail("retest", failed_breakout=True, detail="closed back below the level")
        tol = RETEST_TOL_ATR * atr5
        retests = [j for j in after if f5.low[j] <= zone["high"] + tol and f5.close[j] > zone["high"] and _reaction_ok(_candle_shape(f5, j))]
        if not retests:
            return fail("retest", failed_breakout=False, detail="waiting for a retest that holds with a bullish reaction")
        j = retests[-1]
        checks["retest"] = {"ok": True, "retest_ts": int(f5.ts[j]), "retest_low": float(f5.low[j]), "candle": _candle_shape(f5, j)}
        if close <= zone["high"]:
            return fail("hold", close=close)
        checks["hold"] = {"ok": True, "close": close}
        stop = float(min(f5.low[j], zone["low"]) - STOP_BUFFER_ATR15 * atr15)
        base_low = float(np.min(f15.low[min(zone["pivots"]):n15]))
        measured = float(zone["high"] + (zone["high"] - base_low))
        return finish("breakout_retest", checks, stop, measured, f"breakout_retest:{int(f5.ts[k])}:{zone['high']:.8g}")

    # 2-3b. Failed breakdown (bear trap) -> reclaim.
    def failed_breakdown() -> dict[str, Any]:
        checks = dict(base_checks)

        def fail(name: str, **info: Any) -> dict[str, Any]:
            checks[name] = {"ok": False, **info}
            return {"valid": False, "reason": name, "setup": "failed_breakdown", "checks": checks}

        found = None
        for r in range(last, max(last - BREAKOUT_LOOKBACK_5M, 1) - 1, -1):
            for z in sorted(support, key=lambda z: z["low"]):
                for k in range(r - 1, max(r - FAIL_WINDOW_5M, 1) - 1, -1):
                    if (formed_before(z, int(f5.ts[k]) - ctx.step5) and f5.close[k] < z["low"] <= f5.close[k - 1]
                            and f5.close[r] > z["high"] >= f5.close[r - 1]):
                        found = (k, r, z)
                        break
                if found:
                    break
            if found:
                break
        if found is None:
            return fail("breakout", detail="no failed breakdown of support")
        k, r, zone = found
        trap_low = float(np.min(f5.low[k:r + 1]))
        shape_r = _candle_shape(f5, r)
        checks["breakout"] = {"ok": True, "zone_low": float(zone["low"]), "zone_high": float(zone["high"]), "touches": zone["touches"],
                              "breakout_ts": int(f5.ts[r]), "breakout_close": float(f5.close[r]), "trap_low": trap_low, "candle": shape_r}
        if not _reaction_ok(shape_r):
            return fail("breakout", weak_reclaim_candle=True, candle=shape_r)
        ratio = float(f5.volume[r] / avg_volume(r)) if avg_volume(r) > 0 else 0.0
        if ratio < VOLUME_MULT:
            return fail("volume", volume_ratio=round(ratio, 2), required=VOLUME_MULT)
        checks["volume"] = {"ok": True, "volume_ratio": round(ratio, 2)}
        if any(f5.close[j] < zone["low"] for j in range(r + 1, last + 1)):
            return fail("retest", failed_breakout=True, detail="lost the reclaimed level again")
        checks["retest"] = {"ok": True, "detail": "support reclaimed"}
        if close <= zone["high"]:
            return fail("hold", close=close)
        checks["hold"] = {"ok": True, "close": close}
        # Bullish divergence at the trap: a lower low than the previous 5m
        # swing low with a higher RSI.
        prior_lows = [i for i in range(max(0, k - 36), k) if f5.low[i] == np.min(f5.low[max(0, i - 2):i + 3])]
        trap_idx = int(k + np.argmin(f5.low[k:r + 1]))
        if prior_lows:
            p = prior_lows[-1]
            checks["_bullish_divergence"] = bool(f5.low[trap_idx] < f5.low[p] and f5.rsi[trap_idx] > f5.rsi[p] + DIVERGENCE_RSI_POINTS)
        stop = float(trap_low - STOP_BUFFER_ATR15 * atr15)
        measured = float(zone["high"] + 2 * (zone["high"] - trap_low))
        return finish("failed_breakdown", checks, stop, measured, f"failed_breakdown:{int(f5.ts[r])}:{zone['low']:.8g}")

    results = [breakout_retest(), failed_breakdown()]
    valid = [x for x in results if x["valid"]]
    if valid:
        return valid[0]
    return max(results, key=lambda x: CHECK_ORDER.index(x["reason"]) if x["reason"] in CHECK_ORDER else -1)


def _unmirror(result: dict[str, Any]) -> dict[str, Any]:
    """Short results are computed on the mirrored chart; flip prices back."""
    def flip(v: Any) -> Any:
        return -v if isinstance(v, (int, float)) and not isinstance(v, bool) else v

    price_keys = {"entry", "stop", "target", "zone_low", "zone_high", "breakout_close", "retest_low", "close", "vwap", "macd_hist", "trap_low"}
    out = dict(result)
    if "plan" in out:
        out["plan"] = {k: (flip(v) if k in price_keys else v) for k, v in out["plan"].items()}
    checks = {}
    for name, c in (out.get("checks") or {}).items():
        c = {k: (flip(v) if k in price_keys else v) for k, v in c.items()}
        if name == "breakout":
            c["zone_low"], c["zone_high"] = c.get("zone_high"), c.get("zone_low")
        for key in ("swing_highs", "swing_lows", "price_highs"):
            if key in c:
                c[key] = [-x for x in c[key]]
        if name == "trend" and "higher_high" in c:
            # On the mirrored chart highs are the real lows: a mirrored
            # higher high is a real lower low, a mirrored higher low a real
            # lower high.
            c["lower_low"], c["lower_high"] = c.pop("higher_high"), c.pop("higher_low")
            c["swing_highs"], c["swing_lows"] = c.pop("swing_lows"), c.pop("swing_highs")
        if name == "momentum" and "rsi" in c:
            c["rsi"] = round(100.0 - c["rsi"], 1)
        if name == "news" and c.get("sentiment") is not None:
            c["sentiment"] = -c["sentiment"]
        checks[name] = c
    out["checks"] = checks
    if "setup_id" in out:
        kind, ts, lvl = out["setup_id"].split(":")
        out["setup_id"] = f"{kind}:{ts}:{-float(lvl):.8g}"
    return out




def data_coverage(ctx: Context, as_of: int) -> dict[str, Any]:
    """How complete the 1-minute chart is over the last COVERAGE_WINDOW_MIN
    minutes before as_of (trading minutes only for a US-equity session):
    candles present / candles expected. Gappy data can fake a breakout or a
    volume spike, so a setup needs at least MIN_COVERAGE."""
    ends = np.arange(as_of - COVERAGE_WINDOW_MIN * 60 + 60, as_of + 1, 60, dtype="int64")
    if ctx.session == "us_equity":
        t = pd.to_datetime(ends - 60, unit="s", utc=True).tz_convert("America/New_York")
        minute = np.asarray(t.hour * 60 + t.minute)
        ends = ends[(minute >= 9 * 60 + 30) & (minute < 16 * 60) & (np.asarray(t.weekday) < 5)]
    expected = int(len(ends))
    if expected < 30:
        return {"ok": True, "coverage": None, "expected_minutes": expected, "detail": "too early in the session to measure"}
    present = int(np.isin(ends, ctx.ts1).sum())
    coverage = present / expected
    ok = coverage >= MIN_COVERAGE
    return {"ok": ok, "coverage": round(coverage, 3), "expected_minutes": expected, "present_minutes": present,
            **({} if ok else {"detail": f"only {coverage:.0%} of the last {expected} one-minute candles exist (needs {MIN_COVERAGE:.0%})"})}

def correlation_check(ctx: Context, leader: Context | None, as_of: int, side: str, *,
                      leader_symbol: str | None = None) -> dict[str, Any]:
    """Rule 6 for one side: the symbol's correlation with its leader over the
    last CORR_LOOKBACK_5M consecutive 5-minute returns, and whether the
    leader is moving against the trade right now. Missing or stale leader
    data fails the rule (never assumes agreement)."""
    base = {"leader": leader_symbol}
    if leader is None:
        return {**base, "ok": False, "detail": "no leader data"}
    a_mask, b_mask = ctx.f5.ts <= as_of, leader.f5.ts <= as_of
    common, ia, ib = np.intersect1d(ctx.f5.ts[a_mask], leader.f5.ts[b_mask], return_indices=True)
    common, ia, ib = common[-(CORR_LOOKBACK_5M + 1):], ia[-(CORR_LOOKBACK_5M + 1):], ib[-(CORR_LOOKBACK_5M + 1):]
    consecutive = np.diff(common) == 300
    ra = np.diff(np.log(np.abs(ctx.f5.close[a_mask][ia])))[consecutive]
    rb = np.diff(np.log(np.abs(leader.f5.close[b_mask][ib])))[consecutive]
    if len(ra) < CORR_MIN_OVERLAP or np.std(ra) == 0 or np.std(rb) == 0:
        return {**base, "ok": False, "detail": f"only {len(ra)} overlapping 5m returns with the leader"}
    corr = float(np.corrcoef(ra, rb)[0, 1])
    li = int(np.searchsorted(leader.f5.ts, as_of, side="right")) - 1
    if li < 12 or leader.f5.ts[li] < as_of - 600:
        return {**base, "ok": False, "corr": round(corr, 2), "detail": "leader data stale"}
    lc = np.abs(leader.f5.close)
    ret_1h = float(lc[li] / lc[li - 12] - 1.0)
    vwap = _vwap_at(leader, as_of)
    vwap = abs(vwap) if vwap is not None else None
    if vwap is not None and lc[li] > vwap and ret_1h > 0:
        leader_dir = 1
    elif vwap is not None and lc[li] < vwap and ret_1h < 0:
        leader_dir = -1
    else:
        leader_dir = 0
    info = {**base, "corr": round(corr, 2), "leader_dir": {1: "up", -1: "down", 0: "mixed"}[leader_dir],
            "leader_ret_1h_pct": round(ret_1h * 100, 3), "leader_vs_vwap": None if vwap is None else ("above" if lc[li] > vwap else "below")}
    if abs(corr) < CORR_MIN:
        return {**info, "ok": True, "applies": False, "detail": f"correlation {corr:+.2f} below {CORR_MIN}: trades on its own chart"}
    trade_dir = 1 if side == "long" else -1
    implied = leader_dir * (1 if corr > 0 else -1)
    ok = implied != -trade_dir
    return {**info, "ok": ok, "applies": True,
            "detail": f"leader {leader_symbol or ''} {info['leader_dir']} (corr {corr:+.2f})" + ("" if ok else " against the trade")}


def _apply_correlation(ctx: Context, by_side: dict[str, Any], as_of: int, leader: Context | None,
                       leader_symbol: str | None) -> None:
    for side, r in list(by_side.items()):
        if not r.get("valid"):
            continue
        check = correlation_check(ctx, leader, as_of, side, leader_symbol=leader_symbol)
        r = {**r, "checks": {**(r.get("checks") or {}), "correlation": check}}
        if not check["ok"]:
            r = {**r, "valid": False, "reason": "correlation"}
        by_side[side] = r


def evaluate(
    ctx: Context, as_of: int, *, sides: tuple[str, ...] = ("long", "short"), fee_rate_roundtrip: float = 0.0,
    spread_bps: float = 0.0, news_score: float | None = None, min_rr: float | None = None,
    leader: Context | None = None, leader_symbol: str | None = None, require_leader: bool = False,
) -> dict[str, Any]:
    """The setup, if any, as of `as_of` (unix seconds): only candles that had
    closed by then are used. With a leader (or require_leader, which fails
    the correlation rule when the leader is missing) a valid side must also
    pass correlation_check. Returns {"valid", "side", "reason", "checks",
    "plan", "setup_id", "by_side"}; `reason` is the first rule that failed
    (or "setup_confirmed")."""
    as_of = int(as_of)
    min_rr = MIN_RR if min_rr is None else min_rr
    coverage = data_coverage(ctx, as_of)
    if not coverage["ok"]:
        return {"valid": False, "reason": "data", "side": None, "closest_side": None, "by_side": {},
                "checks": {"data": coverage}}
    by_side: dict[str, Any] = {}
    if "long" in sides:
        by_side["long"] = _evaluate_long(ctx, as_of, fee_rate_roundtrip=fee_rate_roundtrip, spread_bps=spread_bps,
                                         news_score=news_score, min_rr=min_rr)
    if "short" in sides:
        mirrored_news = None if news_score is None else -news_score
        raw = _evaluate_long(ctx.mirrored(), as_of, fee_rate_roundtrip=fee_rate_roundtrip, spread_bps=spread_bps,
                             news_score=mirrored_news, min_rr=min_rr)
        by_side["short"] = _unmirror(raw)
    if leader is not None or require_leader:
        _apply_correlation(ctx, by_side, as_of, leader, leader_symbol)
    for r in by_side.values():
        if isinstance(r.get("checks"), dict) and r["checks"].get("data", {}).get("ok"):
            r["checks"]["data"] = {**r["checks"]["data"], "coverage": coverage.get("coverage")}
    valid = [s for s, r in by_side.items() if r.get("valid")]
    if valid:
        side = valid[0]
        return {**by_side[side], "side": side, "by_side": by_side}
    # Report the side that got furthest through the checklist.
    def progress(r: dict[str, Any]) -> int:
        return CHECK_ORDER.index(r.get("reason")) if r.get("reason") in CHECK_ORDER else -1
    side = max(by_side, key=lambda s: progress(by_side[s])) if by_side else None
    best = by_side.get(side, {"valid": False, "reason": "no_sides"})
    return {**best, "valid": False, "side": None, "closest_side": side, "by_side": by_side}


def latest_closed_5m(now_ts: float) -> int:
    """End time of the most recent fully closed 5-minute candle."""
    return int(math.floor(now_ts / 300.0) * 300)


# ---------------------------------------------------------------------------
# Plan-based exits and replay
# ---------------------------------------------------------------------------

def exit_levels(position: dict[str, Any]) -> dict[str, float]:
    """The planned stop and target a position was opened with."""
    return {"take_profit_price": float(position["setup_target_price"]), "stop_loss_price": float(position["setup_stop_price"])}


def has_plan(position: dict[str, Any]) -> bool:
    return position.get("setup_stop_price") is not None and position.get("setup_target_price") is not None


def plan_exit(position: dict[str, Any], price: float, *, held_minutes: float | None = None) -> tuple[bool, str]:
    """Exit only at the planned stop or target (MAX_HOLD_SAFETY_MINUTES is
    an operational backstop, not a trading rule)."""
    stop, target = float(position["setup_stop_price"]), float(position["setup_target_price"])
    if position.get("side") == "short":
        if price >= stop:
            return True, f"stop_loss (setup invalidation {stop:.6g})"
        if price <= target:
            return True, f"take_profit (setup target {target:.6g})"
    else:
        if price <= stop:
            return True, f"stop_loss (setup invalidation {stop:.6g})"
        if price >= target:
            return True, f"take_profit (setup target {target:.6g})"
    if held_minutes is not None and held_minutes >= MAX_HOLD_SAFETY_MINUTES:
        return True, f"max_hold_safety ({held_minutes:.0f} min)"
    return False, "holding for the planned stop or target"


def replay(df1: pd.DataFrame, *, sides: tuple[str, ...], fee_rate_roundtrip: float, spread_bps: float,
           entry_allowed=None, force_exit=None, max_hold_minutes: int | None = None,
           leader_df: pd.DataFrame | None = None, leader_symbol: str | None = None) -> pd.DataFrame:
    """Backtest on real 1-minute candles (ts = candle end): evaluate every
    closed 5m candle, enter at the next 1m open, exit only at the planned
    stop or target (stop first when both fall inside one candle), one
    position at a time, each breakout traded once. entry_allowed(ts) and
    force_exit(ts) let a market add session rules."""
    ctx = prepare(df1, session=SESSION)
    leader_ctx = prepare(leader_df, session=SESSION) if leader_df is not None and not leader_df.empty else None
    d = df1.sort_values("ts").reset_index(drop=True)
    ts1 = d["ts"].to_numpy("int64")
    o, h, lo, c = (d[k].to_numpy(float) for k in ("open", "high", "low", "close"))
    hold_cap = MAX_HOLD_SAFETY_MINUTES if max_hold_minutes is None else max_hold_minutes
    trades, busy_until, used = [], 0, set()
    for as_of in ctx.f5.ts:
        as_of = int(as_of)
        if as_of < busy_until:
            continue
        i = int(np.searchsorted(ts1, as_of, side="right"))
        if i >= len(ts1) or (entry_allowed is not None and not entry_allowed(int(ts1[i]))):
            continue
        r = evaluate(ctx, as_of, sides=sides, fee_rate_roundtrip=fee_rate_roundtrip, spread_bps=spread_bps,
                     leader=leader_ctx, leader_symbol=leader_symbol, require_leader=leader_df is not None)
        if not r["valid"] or r["setup_id"] in used:
            continue
        used.add(r["setup_id"])
        sign = 1 if r["side"] == "long" else -1
        entry, stop, target = o[i], r["plan"]["stop"], r["plan"]["target"]
        if sign * (entry - stop) <= 0 or sign * (target - entry) <= 0:
            continue
        exit_px, why, j = None, None, i
        while j < len(ts1) and ts1[j] - ts1[i] < hold_cap * 60:
            if (lo[j] <= stop) if sign > 0 else (h[j] >= stop):
                exit_px, why = stop, "stop"
                break
            if (h[j] >= target) if sign > 0 else (lo[j] <= target):
                exit_px, why = target, "target"
                break
            if force_exit is not None and force_exit(int(ts1[j])):
                exit_px, why = c[j], "session_close"
                break
            j += 1
        if exit_px is None:
            j = min(j, len(ts1) - 1)
            exit_px, why = c[j], "max_hold_safety"
        gross = sign * (exit_px - entry) / entry
        net = gross - fee_rate_roundtrip - spread_bps / 1e4
        trades.append({"entry_ts": int(ts1[i]), "exit_ts": int(ts1[j]), "side": r["side"], "setup": r.get("setup"),
                       "entry": float(entry), "stop": float(stop), "target": float(target), "exit": why,
                       "gross_return": gross, "net_return": net, "r_multiple": net / (sign * (entry - stop) / entry),
                       "planned_rr": r["plan"]["rr_net"]})
        busy_until = int(ts1[j])
    return pd.DataFrame(trades)


def summarize(trades: pd.DataFrame) -> dict[str, Any]:
    if trades.empty:
        return {"trades": 0}
    wins = trades[trades.net_return > 0]
    losses = trades[trades.net_return <= 0]
    pf = float(wins.net_return.sum() / -losses.net_return.sum()) if len(losses) and losses.net_return.sum() < 0 else None
    return {
        "trades": int(len(trades)), "win_rate": round(float(len(wins) / len(trades)), 4),
        "avg_net_return_pct": round(float(trades.net_return.mean() * 100), 3),
        "avg_r": round(float(trades.r_multiple.mean()), 2), "total_net_return_pct": round(float(trades.net_return.sum() * 100), 2),
        "profit_factor": None if pf is None else round(pf, 2), "exits": {k: int(v) for k, v in trades.exit.value_counts().items()},
        "by_setup": {k: int(v) for k, v in trades.setup.value_counts().items()},
    }


def market_spread_bps(market: dict[str, Any]) -> float | None:
    """The perp's live bid/ask spread in bps, from a /margin/markets snapshot."""
    try:
        bid, ask = float(market.get("bid")), float(market.get("ask"))
    except (TypeError, ValueError):
        return None
    if bid <= 0 or ask < bid:
        return None
    return (ask - bid) / ((ask + bid) / 2.0) * 1e4


def live_quote(market: dict[str, Any]) -> dict[str, float] | None:
    """The perp's live bid, ask and mid, or None without a two-sided quote."""
    try:
        bid, ask = float(market.get("bid")), float(market.get("ask"))
    except (TypeError, ValueError):
        return None
    if bid <= 0 or ask < bid:
        return None
    return {"bid": bid, "ask": ask, "mid": (bid + ask) / 2.0}


METAL_COINS = frozenset({"GOLD", "SILVER"})


def chart_session(coin: str) -> str:
    """Metal perps are read on their ETF (US-equity session); coins trade 24/7."""
    return "us_equity" if coin in METAL_COINS else SESSION


def _chart_for_coin(coin: str) -> pd.DataFrame:
    from data import kalshi_15m_setup, kalshi_15m_spot
    if coin in METAL_COINS:
        return kalshi_15m_setup.metals_candles(coin)
    if coin not in kalshi_15m_spot.COINBASE_PRODUCTS:
        return pd.DataFrame()
    return kalshi_15m_spot.recent_series(coin)[["ts", "open", "high", "low", "close", "volume"]]


def chart_candles(ticker: str) -> pd.DataFrame:
    """The perp's real 1-minute chart (ts = END): the coin's Coinbase spot
    market -- the perp itself goes idle and prints outliers, so its own
    volume can't carry the volume rule -- or, for the gold and silver
    perps, GLD/SLV (real-time exchange data, US session; see
    kalshi_15m_setup.METAL_CHART_SYMBOL for how they were graded)."""
    from data import kalshi_15m_spot, perps_data
    return _chart_for_coin(kalshi_15m_spot.chart_coin(perps_data.coin_for_ticker(ticker)))


def live_setup(ticker: str, *, sides: tuple[str, ...], fee_rate_roundtrip: float, news_score: float | None,
               spread_bps: float = 0.0, now: float | None = None) -> dict[str, Any]:
    """The setup on this perp's coin right now (Coinbase spot chart), with
    its leader's Coinbase chart for the correlation rule."""
    from data import kalshi_15m_spot, perps_data
    coin = kalshi_15m_spot.chart_coin(perps_data.coin_for_ticker(ticker))
    leader_symbol = leader_for(coin)
    leader_candles = _chart_for_coin(leader_symbol)
    return setup_from_candles(chart_candles(ticker), sides=sides, fee_rate_roundtrip=fee_rate_roundtrip,
                              news_score=news_score, spread_bps=spread_bps, now=now,
                              leader_candles=leader_candles if not leader_candles.empty else None,
                              leader_symbol=leader_symbol, require_leader=True,
                              session=chart_session(coin), leader_session=chart_session(leader_symbol))


def setup_from_candles(one_min: pd.DataFrame, *, sides: tuple[str, ...], fee_rate_roundtrip: float,
                       news_score: float | None, spread_bps: float | None = None, now: float | None = None,
                       leader_candles: pd.DataFrame | None = None, leader_symbol: str | None = None,
                       require_leader: bool = False, session: str = SESSION, leader_session: str | None = None) -> dict[str, Any]:
    """spread_bps: the traded instrument's spread; None reads it from the
    candles' own bid_close/ask_close when they carry them."""
    now = time.time() if now is None else now
    if one_min is None or one_min.empty:
        return {"valid": False, "reason": "data", "checks": {"data": {"ok": False, "detail": "no chart candles for this coin"}}}
    last_ts = int(one_min["ts"].max())
    if now - last_ts > STALE_AFTER_SEC:
        return {"valid": False, "reason": "data", "checks": {"data": {"ok": False, "detail": f"last candle {int(now - last_ts)}s old"}}}
    if spread_bps is None:
        spread_bps = 0.0
        last = one_min.sort_values("ts").iloc[-1]
        if {"bid_close", "ask_close"} <= set(one_min.columns) and last["bid_close"] > 0:
            mid = (last["bid_close"] + last["ask_close"]) / 2.0
            spread_bps = max(0.0, float((last["ask_close"] - last["bid_close"]) / mid * 1e4))
    as_of = latest_closed_5m(min(now, last_ts))
    ctx = prepare(one_min[["ts", "open", "high", "low", "close", "volume"]], session=session)
    leader = (prepare(leader_candles, session=leader_session or session)
              if leader_candles is not None and not leader_candles.empty else None)
    result = evaluate(ctx, as_of, sides=sides, fee_rate_roundtrip=fee_rate_roundtrip, spread_bps=spread_bps, news_score=news_score,
                      leader=leader, leader_symbol=leader_symbol, require_leader=require_leader)
    chart_price = float(one_min.sort_values("ts")["close"].iloc[-1])
    return {**result, "as_of": as_of, "spread_bps": round(spread_bps, 2), "chart_price": chart_price}


def plan_at_price(setup: dict[str, Any], *, price: float, chart_price: float, fee_rate_roundtrip: float,
                  spread_bps: float, min_rr: float | None = None) -> dict[str, Any]:
    """The chart plan in the perp's own price units, re-checked at the real
    execution price. Levels scale by price / chart_price (the perp's basis
    and contract size against spot); reward/risk is recomputed from `price`
    with costs, and the trade is skipped if it no longer clears min_rr or
    price is already beyond the stop or target."""
    min_rr = MIN_RR if min_rr is None else min_rr
    if price <= 0 or chart_price <= 0:
        return {"ok": False, "reason": "no_price"}
    k = price / chart_price
    plan = setup["plan"]
    stop, target = plan["stop"] * k, plan["target"] * k
    sign = 1.0 if setup["side"] == "long" else -1.0
    risk, reward = sign * (price - stop), sign * (target - price)
    cost = price * (fee_rate_roundtrip + spread_bps / 1e4)
    rr_net = (reward - cost) / (risk + cost) if risk > 0 else 0.0
    ok = risk > 0 and reward > 0 and rr_net >= min_rr
    return {"ok": ok, "reason": "rr_ok" if ok else ("beyond_plan" if risk <= 0 or reward <= 0 else "rr_below_min"),
            "entry": price, "stop": stop, "target": target, "rr_net": round(rr_net, 3), "min_rr": min_rr,
            "risk_pct": round(risk / price, 6) if risk > 0 else None, "basis_ratio": k}


def risk_sized_count(*, balance_usd: float, risk_pct_of_balance: float, price: float, stop: float,
                     fee_rate_roundtrip: float, contract_multiplier: float = 1.0) -> int:
    """Whole contracts such that hitting the stop (plus both fees) loses at
    most risk_pct_of_balance of the balance. contract_multiplier converts a
    price move into dollars per contract (1.0 when price is per contract)."""
    loss_per_contract = (abs(price - stop) + price * fee_rate_roundtrip) * contract_multiplier
    if balance_usd <= 0 or loss_per_contract <= 0:
        return 0
    return int(balance_usd * risk_pct_of_balance // loss_per_contract)


def strategy_card() -> dict[str, Any]:
    """This bot's strategy as published to its HF model repo: every rule
    parameter in force (env overrides included) and the method in words."""
    params = {k: v for k, v in globals().items()
              if k.isupper() and not k.startswith("_") and isinstance(v, (int, float, str, list, tuple, dict))}
    return {"module": __name__, "method": (__doc__ or "").strip(), "params": params}
