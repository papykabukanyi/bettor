"""Market-anchored edge model for Kalshi 15-minute contracts.

Starts from Kalshi's own quote as the probability (it is well calibrated:
22 days / 21,808 real windows, within ~1-2 points everywhere) and asks
whether anything moves it enough to beat the real ask after fees:

  quote      logit(mid), logit(mid) x elapsed -- a pure recalibration
  quote_mtf  (crypto) + real Coinbase spot (kalshi_15m_spot): distance from
             the window's strike in vol units, 5m/15m/1h/4h/8h/1d momentum
             (vol-normalized), the 8-timeframe cascade vote, and dollar-
             volume z, each also x elapsed
  quote_flow + the contract's own price action (1m/3m quote change), its
             volume and open-interest flow, and cross-market correlation:
             every series shares the same 15-minute windows, so the other
             contracts' quotes/moves (and the lead contract's -- BTC for
             crypto, GOLD for metals) at the same minute are features

Research baseline (22 days, 21,808 windows, walk-forward): quote and
quote_mtf never beat Kalshi's price; quote_flow on crypto was the one
positive result (+2.2c/contract over 2,018 trades at the live rule, t=1.6)
-- promising, not yet significant, so it trades only once certification
passes on the growing archive.

Every candidate is judged walk-forward by day on the real quote archive
(kalshi_15m_quotes) with the EXACT live rule: entry minutes
[EV_ENTRY_MIN_MINUTE, EV_ENTRY_MAX_MINUTE], edge = p(side) - ask - fee >=
EV_MIN_EDGE, one entry per window, fees at 1-contract rounding. A market is
certified only if the chosen model beats the raw mid out of sample (window
bootstrap CI above zero) AND the rule's out-of-sample P&L clears
CERTIFY_MIN_T over at least CERTIFY_MIN_TRADES trades and CERTIFY_MIN_OOS_DAYS
days, with a majority of those days profitable.

Underlying features are joined from the minute BEFORE each quote, matching
what the live bot actually has (its last closed perp candle lags Kalshi's
live quote by up to a minute).
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any

from data import kalshi_15m
from server_common import DATA_DIR

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


HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_KALSHI_15M_DATASET_REPO = os.getenv("HF_KALSHI_15M_DATASET_REPO", "papylove/kalshi-15m-data")
HF_ARTIFACT_PATH = "edge_model/latest.json"
LOCAL_ARTIFACT_PATH = DATA_DIR / "kalshi_15m_edge_model.json"

EV_MIN_EDGE = _env_float("KALSHI_15M_EV_MIN_EDGE", 0.03)
EV_ENTRY_MIN_MINUTE = _env_float("KALSHI_15M_EV_ENTRY_MIN_MINUTE", 1.0)
EV_ENTRY_MAX_MINUTE = _env_float("KALSHI_15M_EV_ENTRY_MAX_MINUTE", 12.0)
EV_MAX_SPREAD = _env_float("KALSHI_15M_EV_MAX_SPREAD", 0.10)
FEE_CONTRACTS = 1

CERTIFY_MIN_TRADES = _env_int("KALSHI_15M_EV_CERTIFY_MIN_TRADES", 200)
CERTIFY_MIN_T = _env_float("KALSHI_15M_EV_CERTIFY_MIN_T", 2.5)
CERTIFY_MIN_OOS_DAYS = _env_int("KALSHI_15M_EV_CERTIFY_MIN_OOS_DAYS", 5)
CERTIFY_MIN_PROFITABLE_DAY_SHARE = _env_float("KALSHI_15M_EV_CERTIFY_MIN_PROFITABLE_DAY_SHARE", 0.55)
TRAIN_MIN_DAYS = _env_int("KALSHI_15M_EV_TRAIN_MIN_DAYS", 7)
HISTORY_DAYS = _env_int("KALSHI_15M_EV_HISTORY_DAYS", 90)

EV_MAX_QUOTE_DRIFT = _env_float("KALSHI_15M_EV_MAX_QUOTE_DRIFT", 0.03)

MTF_BASE = ["move_z", "m5", "m15", "m1h", "m4h", "m8h", "m1d", "cascade", "dvz"]
FLOW_BASE = ["d1", "d3", "vol_rel", "oi_chg", "mkt_gap", "mkt_d1", "lead_gap", "lead_d1"]
XSPOT_BASE = ["lead_move_z", "lead_m5", "mkt_move_z", "mkt_m5"]
SPOT_BASED = set(MTF_BASE) | set(XSPOT_BASE)
QUOTE_FEATURES = ["lm", "lm_t"]


def _with_elapsed(base: list[str]) -> list[str]:
    return base + [f"{f}_t" for f in base]


MTF_FEATURES = QUOTE_FEATURES + _with_elapsed(MTF_BASE)
FLOW_FEATURES = QUOTE_FEATURES + _with_elapsed(FLOW_BASE)
FULL_FEATURES = QUOTE_FEATURES + _with_elapsed(FLOW_BASE) + _with_elapsed(MTF_BASE) + _with_elapsed(XSPOT_BASE)
CANDIDATES: dict[str, dict[str, list[str]]] = {
    "crypto": {"quote": QUOTE_FEATURES, "quote_mtf": MTF_FEATURES, "quote_flow": FLOW_FEATURES, "quote_full": FULL_FEATURES},
    "metals": {"quote": QUOTE_FEATURES, "quote_flow": FLOW_FEATURES},
}
REGULARIZATION_C = {"quote": 1.0, "quote_mtf": 0.1, "quote_flow": 0.1, "quote_full": 0.05}

# Per-coin eligibility inside a certified market: real data behind the coin
# and the certified model actually working on it out of sample.
COIN_MIN_HISTORY_DAYS = _env_int("KALSHI_15M_EV_COIN_MIN_HISTORY_DAYS", 20)
COIN_MIN_OOS_TRADES = _env_int("KALSHI_15M_EV_COIN_MIN_OOS_TRADES", 30)
COIN_MIN_SPOT_COVERAGE = _env_float("KALSHI_15M_EV_COIN_MIN_SPOT_COVERAGE", 0.95)
COIN_MIN_SPOT_OUTCOME_MATCH = _env_float("KALSHI_15M_EV_COIN_MIN_SPOT_OUTCOME_MATCH", 0.90)
SPOT_MAX_AGE_SEC = _env_int("KALSHI_15M_EV_SPOT_MAX_AGE_SEC", 180)
LEAD_COIN = {"crypto": "BTC", "metals": "GOLD"}
CASCADE_SOURCE_COLUMNS = ["ret_5m", "ret_10m", "ret_15m", "ret_30m", "trend_1h", "trend_2h", "trend_4h", "trend_8h"]
UNDERLYING_COLUMNS = [
    "close", "ret_5m", "ret_10m", "ret_15m", "ret_30m", "trend_1h", "trend_2h", "trend_4h", "trend_8h", "trend_1d",
    "volatility_15", "volatility_30", "dollar_volume_z",
]

_REMOTE_RETRY_SEC = 900.0
_cache: dict[str, Any] = {"artifact": None, "last_remote_attempt": 0.0}


def market_kind(coin: str) -> str | None:
    if coin in kalshi_15m.KNOWN_15M_SERIES:
        return "crypto"
    if coin in kalshi_15m.KNOWN_15M_METALS_SERIES:
        return "metals"
    return None


def _logit(p: float) -> float:
    p = min(max(p, 1e-3), 1 - 1e-3)
    return math.log(p / (1 - p))


def _sigmoid(z: float) -> float:
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    e = math.exp(z)
    return e / (1.0 + e)


def fee_per_contract(price: float, contracts: int = FEE_CONTRACTS) -> float:
    return kalshi_15m.taker_fee_usd(contracts, price) / contracts


def add_flow_features(rows: "pd.DataFrame") -> "pd.DataFrame":  # noqa: F821
    """Contract price action, volume/OI flow, and cross-market features from
    per-minute quote rows (coin, ticker, open_ts, minute, yes_bid, yes_ask,
    volume, open_interest). Every value at minute m uses only minute <= m
    data from windows sharing the same open_ts. Same function for history
    and live rows."""
    import numpy as np

    q = rows.dropna(subset=["yes_bid", "yes_ask"]).copy()
    q["market"] = q.coin.map(market_kind)
    mid = ((q.yes_bid + q.yes_ask) / 2.0).clip(0.01, 0.99)
    q["_lmf"] = np.log(mid / (1 - mid))
    q = q.sort_values(["ticker", "minute"])
    g = q.groupby("ticker", sort=False)
    d1 = q["_lmf"] - g["_lmf"].shift(1)
    q["d1"] = d1.fillna(0.0)
    q["d3"] = (q["_lmf"] - g["_lmf"].shift(3)).fillna(q["d1"])
    vol_mean_prior = g["volume"].transform(lambda s: s.expanding().mean().shift(1))
    q["vol_rel"] = np.log1p(q.volume.astype(float)) - np.log1p(vol_mean_prior.fillna(q.volume).astype(float))
    oi = q.open_interest.astype(float)
    q["oi_chg"] = np.log1p(oi) - np.log1p(g["open_interest"].shift(3).fillna(q.open_interest).astype(float))

    key = ["market", "open_ts", "minute"]
    sums = q.groupby(key).agg(_sum_lm=("_lmf", "sum"), _sum_d1=("d1", "sum"), _n=("_lmf", "size")).reset_index()
    q = q.merge(sums, on=key, how="left")
    others = (q["_n"] - 1).where(q["_n"] > 1)
    q["mkt_gap"] = ((q["_sum_lm"] - q["_lmf"]) / others - q["_lmf"]).fillna(0.0)
    q["mkt_d1"] = ((q["_sum_d1"] - q["d1"]) / others).fillna(0.0)
    lead = q[q.coin == q.market.map(LEAD_COIN)][key + ["_lmf", "d1"]].rename(columns={"_lmf": "_lead_lm", "d1": "_lead_d1"})
    q = q.merge(lead.drop_duplicates(key), on=key, how="left")
    is_lead = q.coin == q.market.map(LEAD_COIN)
    q["lead_gap"] = (q["_lead_lm"] - q["_lmf"]).where(~is_lead).fillna(0.0)
    q["lead_d1"] = q["_lead_d1"].where(~is_lead).fillna(0.0)
    for col in FLOW_BASE:
        q[col] = q[col].replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(-6, 6)
    return q.drop(columns=["_lmf", "_sum_lm", "_sum_d1", "_n", "_lead_lm", "_lead_d1"])


def add_features(df: "pd.DataFrame") -> "pd.DataFrame":  # noqa: F821
    """Vectorized feature construction shared by training and live scoring.
    Needs mid and minute; the mtf columns also need floor_strike plus the
    spot UNDERLYING_COLUMNS (missing ones become 0)."""
    import numpy as np

    out = df.copy()
    mid = out["mid"].astype(float).clip(1e-3, 1 - 1e-3)
    t = out["minute"].astype(float) / 15.0
    out["lm"] = np.log(mid / (1 - mid))
    out["lm_t"] = out["lm"] * t
    for col in FLOW_BASE + XSPOT_BASE:
        if col in out.columns:
            out[f"{col}_t"] = out[col].astype(float) * t
    if "close" not in out.columns:
        return out
    for col in UNDERLYING_COLUMNS + ["floor_strike"]:
        if col not in out.columns:
            out[col] = np.nan
    tau = (15.0 - out["minute"].astype(float)).clip(lower=0.5)
    v15 = out["volatility_15"].astype(float)
    v30 = out["volatility_30"].astype(float).fillna(v15)
    for col in ("trend_8h", "trend_1d"):
        if col not in out.columns:
            out[col] = np.nan
    with np.errstate(divide="ignore", invalid="ignore"):
        out["move_z"] = np.log(out["close"].astype(float) / out["floor_strike"].astype(float)) / (v15 * np.sqrt(tau))
        out["m5"] = out["ret_5m"] / (v15 * math.sqrt(5))
        out["m15"] = out["ret_15m"] / (v15 * math.sqrt(15))
        out["m1h"] = out["trend_1h"] / (v30 * math.sqrt(60))
        out["m4h"] = out["trend_4h"] / (v30 * math.sqrt(240))
        out["m8h"] = out["trend_8h"] / (v30 * math.sqrt(480))
        out["m1d"] = out["trend_1d"] / (v30 * math.sqrt(1440))
    tf = out[[c for c in CASCADE_SOURCE_COLUMNS if c in out.columns]].astype(float)
    n_valid = tf.notna().sum(axis=1)
    out["cascade"] = np.where(n_valid >= 4, ((tf > 0).sum(axis=1) / n_valid.replace(0, np.nan)) * 2 - 1, 0.0)
    for col in ("move_z", "m5", "m15", "m1h", "m4h", "m8h", "m1d"):
        out[col] = out[col].replace([np.inf, -np.inf], np.nan).clip(-6, 6).fillna(0.0)
    out["dvz"] = out["dollar_volume_z"].astype(float).clip(-4, 6).fillna(0.0) if "dollar_volume_z" in out.columns else 0.0
    out["cascade"] = out["cascade"].fillna(0.0)
    for col in MTF_BASE:
        out[f"{col}_t"] = out[col] * t
    return out


def add_cross_spot_features(q: "pd.DataFrame") -> "pd.DataFrame":  # noqa: F821
    """Cross-asset spot correlation at each shared window minute: the lead
    coin's (BTC's) distance from its own strike and 5m momentum, and the
    leave-one-out average of every other coin's. Needs market, open_ts,
    minute, has_underlying, move_z, m5 (from add_features)."""
    import numpy as np

    q = q.copy()
    key = ["market", "open_ts", "minute"]
    has = q["has_underlying"].astype(bool)
    own_mz = q["move_z"].where(has, 0.0).astype(float)
    own_m5 = q["m5"].where(has, 0.0).astype(float)
    q["_mz"], q["_m5"], q["_has"] = own_mz, own_m5, has.astype(int)
    sums = q.groupby(key).agg(_s_mz=("_mz", "sum"), _s_m5=("_m5", "sum"), _n=("_has", "sum")).reset_index()
    q = q.merge(sums, on=key, how="left")
    others = (q["_n"] - q["_has"]).where(lambda s: s > 0)
    q["mkt_move_z"] = ((q["_s_mz"] - q["_mz"]) / others).fillna(0.0)
    q["mkt_m5"] = ((q["_s_m5"] - q["_m5"]) / others).fillna(0.0)
    is_lead = q["coin"] == q["market"].map(LEAD_COIN)
    lead = q[is_lead & has][key + ["_mz", "_m5"]].drop_duplicates(key).rename(columns={"_mz": "lead_move_z", "_m5": "lead_m5"})
    q = q.merge(lead, on=key, how="left")
    for col in ("lead_move_z", "lead_m5"):
        q[col] = q[col].where(~is_lead.values).fillna(0.0)
    t = q["minute"].astype(float) / 15.0
    for col in XSPOT_BASE:
        q[col] = q[col].replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(-6, 6)
        q[f"{col}_t"] = q[col] * t
    return q.drop(columns=["_mz", "_m5", "_has", "_s_mz", "_s_m5", "_n"])


def build_frame(quotes: "pd.DataFrame", underlying: "pd.DataFrame | None" = None) -> "pd.DataFrame":  # noqa: F821
    """Valid quote rows (0 < bid < ask < 1, spread <= EV_MAX_SPREAD, minutes
    1-14) with outcome, day, market kind and features. `underlying` is
    kalshi_15m_spot.engineer_spot_features output (coin, ts, close, ...),
    joined as of one minute before each quote."""
    import numpy as np
    import pandas as pd

    q = quotes[quotes.coin.map(market_kind).notna()]
    q = add_flow_features(q)
    q = q[(q.yes_bid > 0) & (q.yes_ask < 1) & (q.yes_ask > q.yes_bid) & (q.yes_ask - q.yes_bid <= EV_MAX_SPREAD)]
    q = q[(q.minute >= 1) & (q.minute <= 14)].copy()
    q["mid"] = (q.yes_bid + q.yes_ask) / 2.0
    q["y"] = (q.result == "yes").astype(int)
    q["day"] = pd.to_datetime(q.open_ts, unit="s", utc=True).dt.strftime("%Y-%m-%d")

    if underlying is not None and not underlying.empty:
        u = underlying.rename(columns={"symbol": "coin"}).copy()
        u["coin"] = u["coin"].astype(str)
        u["ts"] = u["ts"].astype("int64")
        cols = [c for c in UNDERLYING_COLUMNS if c in u.columns]
        u = u[["coin", "ts"] + cols].sort_values("ts")

        def asof(left: pd.DataFrame, key: str, right_cols: list[str], suffix: str) -> pd.DataFrame:
            right = u[["coin", "ts"] + right_cols].rename(columns={c: c + suffix for c in right_cols}).rename(columns={"ts": "_rts"})
            left = left.sort_values(key)
            merged = pd.merge_asof(left, right, left_on=key, right_on="_rts", by="coin", direction="backward", tolerance=90)
            return merged.drop(columns="_rts")

        q["_feat_ts"] = q.end_period_ts.astype("int64") - 60
        q = asof(q, "_feat_ts", cols, "").drop(columns=["_feat_ts"])
        q["has_underlying"] = q["close"].notna() & q["floor_strike"].notna() & (q["volatility_15"].fillna(0) > 0)
    else:
        q["has_underlying"] = False
    q = add_features(q)
    if "move_z" in q.columns:
        q = add_cross_spot_features(q)
    return q.reset_index(drop=True)


def _fit(X: "np.ndarray", y: "np.ndarray", c_reg: float) -> tuple[list[float], float]:  # noqa: F821
    from sklearn.linear_model import LogisticRegression
    model = LogisticRegression(C=c_reg, max_iter=2000).fit(X, y)
    return [float(v) for v in model.coef_[0]], float(model.intercept_[0])


def _predict(X: "np.ndarray", coef: list[float], intercept: float) -> "np.ndarray":  # noqa: F821
    import numpy as np
    z = X @ np.asarray(coef) + intercept
    return 1.0 / (1.0 + np.exp(-z))


def walk_forward(frame: "pd.DataFrame", features: list[str], c_reg: float) -> "pd.DataFrame":  # noqa: F821
    """Trains on every day strictly before each test day (after TRAIN_MIN_DAYS)."""
    import pandas as pd
    days = sorted(frame.day.unique())
    parts = []
    for i, day in enumerate(days):
        if i < TRAIN_MIN_DAYS:
            continue
        train, test = frame[frame.day.isin(days[:i])], frame[frame.day == day].copy()
        if train.y.nunique() < 2 or test.empty:
            continue
        coef, intercept = _fit(train[features].values, train.y.values, c_reg)
        test["p"] = _predict(test[features].values, coef, intercept)
        parts.append(test)
    return pd.concat(parts) if parts else frame.iloc[0:0].assign(p=[])


def simulate_rule(oos: "pd.DataFrame", *, min_edge: float = None) -> "pd.DataFrame":  # noqa: F821
    """The live rule applied to out-of-sample rows: earliest qualifying
    minute per window, side with the larger edge, 1-contract fee rounding."""
    import numpy as np
    min_edge = EV_MIN_EDGE if min_edge is None else min_edge
    g = oos[(oos.minute >= EV_ENTRY_MIN_MINUTE) & (oos.minute <= EV_ENTRY_MAX_MINUTE)].copy()
    if g.empty:
        return g.assign(pnl=[], side_yes=[], price=[], edge=[])
    no_ask = 1.0 - g.yes_bid
    edge_yes = g.p - g.yes_ask - g.yes_ask.map(fee_per_contract)
    edge_no = (1.0 - g.p) - no_ask - no_ask.map(fee_per_contract)
    g["side_yes"] = edge_yes >= edge_no
    g["edge"] = np.maximum(edge_yes, edge_no)
    g["price"] = np.where(g.side_yes, g.yes_ask, no_ask)
    g = g[g.edge >= min_edge].sort_values("minute").groupby("ticker").head(1)
    won = np.where(g.side_yes, g.y == 1, g.y == 0).astype(float)
    g["pnl"] = won - g.price - g.price.map(fee_per_contract)
    return g


def _log_loss(p: "np.ndarray", y: "np.ndarray") -> "np.ndarray":  # noqa: F821
    import numpy as np
    p = np.clip(p, 1e-4, 1 - 1e-4)
    return -(y * np.log(p) + (1 - y) * np.log(1 - p))


def certify(oos: "pd.DataFrame", trades: "pd.DataFrame") -> dict[str, Any]:  # noqa: F821
    import numpy as np
    import pandas as pd
    if oos.empty:
        return {"certified": False, "reason": "no_out_of_sample_rows"}
    ll_mid = _log_loss(oos.mid.values, oos.y.values)
    ll_model = _log_loss(oos.p.values, oos.y.values)
    per_window = pd.DataFrame({"ticker": oos.ticker.values, "gain": ll_mid - ll_model}).groupby("ticker").gain.mean().values
    rng = np.random.default_rng(0)
    boots = np.array([rng.choice(per_window, len(per_window)).mean() for _ in range(1000)])
    stats: dict[str, Any] = {
        "oos_rows": int(len(oos)), "oos_windows": int(oos.ticker.nunique()), "oos_days": int(oos.day.nunique()),
        "log_loss_mid": round(float(ll_mid.mean()), 5), "log_loss_model": round(float(ll_model.mean()), 5),
        "log_loss_gain_ci95": [round(float(np.percentile(boots, 2.5)), 5), round(float(np.percentile(boots, 97.5)), 5)],
        "trades": int(len(trades)),
    }
    if len(trades):
        per_open = trades.groupby("open_ts").pnl.sum()
        sd = per_open.std(ddof=1) if len(per_open) > 1 else 0.0
        t_stat = float(per_open.mean() / (sd / math.sqrt(len(per_open)))) if sd and sd > 0 else 0.0
        daily = trades.groupby("day").pnl.sum()
        stats.update({
            "pnl_per_contract": round(float(trades.pnl.mean()), 4), "pnl_total_1ct": round(float(trades.pnl.sum()), 2),
            "win_rate": round(float((trades.pnl > 0).mean()), 4), "avg_price": round(float(trades.price.mean()), 4),
            "t_stat": round(t_stat, 2), "trade_days": int(len(daily)), "profitable_day_share": round(float((daily > 0).mean()), 3),
        })
    failures = []
    if stats["log_loss_gain_ci95"][0] <= 0:
        failures.append("does_not_beat_kalshi_quote_out_of_sample")
    if stats["trades"] < CERTIFY_MIN_TRADES:
        failures.append("too_few_out_of_sample_trades")
    if stats.get("t_stat", 0.0) < CERTIFY_MIN_T:
        failures.append("pnl_not_significant")
    if stats.get("trade_days", 0) < CERTIFY_MIN_OOS_DAYS:
        failures.append("too_few_out_of_sample_days")
    if stats.get("profitable_day_share", 0.0) < CERTIFY_MIN_PROFITABLE_DAY_SHARE:
        failures.append("too_few_profitable_days")
    stats["certified"] = not failures
    stats["reason"] = "certified" if not failures else ",".join(failures)
    return stats


def _uses_spot(features: list[str]) -> bool:
    return any(f in SPOT_BASED for f in features)


def _trade_breakdown(trades: "pd.DataFrame", by: "str | pd.Series") -> dict[str, Any]:  # noqa: F821
    if trades.empty:
        return {}
    g = trades.groupby(by)
    return {
        str(k): {"trades": int(len(v)), "pnl_per_contract": round(float(v.pnl.mean()), 4),
                 "pnl_total_1ct": round(float(v.pnl.sum()), 2), "win_rate": round(float((v.pnl > 0).mean()), 4)}
        for k, v in g
    }


def coin_eligibility(
    part: "pd.DataFrame", trades: "pd.DataFrame", *, market_certified: bool, uses_spot: bool,  # noqa: F821
    spot_validation: dict[str, Any] | None,
) -> tuple[list[str], dict[str, Any]]:
    """A coin trades only if its market's model is certified AND the coin
    has its own real history, the model made money on it out of sample, and
    (for spot-based models) its spot data reproduces Kalshi's settlements."""
    per_spot = (spot_validation or {}).get("per_coin") or {}
    eligible, stats = [], {}
    for coin in sorted(part.coin.unique()):
        ct = trades[trades.coin == coin] if not trades.empty else trades
        s: dict[str, Any] = {
            "history_days": int(part[part.coin == coin].day.nunique()), "oos_trades": int(len(ct)),
            "oos_pnl_total_1ct": round(float(ct.pnl.sum()), 2) if len(ct) else 0.0,
            "oos_pnl_per_contract": round(float(ct.pnl.mean()), 4) if len(ct) else None,
        }
        reasons = []
        if not market_certified:
            reasons.append("market_not_certified")
        if s["history_days"] < COIN_MIN_HISTORY_DAYS:
            reasons.append("not_enough_history")
        if s["oos_trades"] < COIN_MIN_OOS_TRADES:
            reasons.append("too_few_out_of_sample_trades")
        if s["oos_pnl_total_1ct"] <= 0:
            reasons.append("not_profitable_out_of_sample")
        if uses_spot:
            v = per_spot.get(coin) or {}
            s["spot_coverage"], s["spot_outcome_match"] = v.get("coverage"), v.get("outcome_match")
            if (v.get("coverage") or 0) < COIN_MIN_SPOT_COVERAGE or (v.get("outcome_match") or 0) < COIN_MIN_SPOT_OUTCOME_MATCH:
                reasons.append("spot_data_not_validated")
        s["eligible"] = not reasons
        s["reasons"] = reasons
        stats[coin] = s
        if not reasons:
            eligible.append(coin)
    return eligible, stats


def learned_patterns(rows: "pd.DataFrame", features: list[str], coef: list[float], trades: "pd.DataFrame") -> dict[str, Any]:  # noqa: F821
    """What the model learned: each feature's pull on the log-odds per one
    standard deviation (how far it moves p away from Kalshi's own price),
    and where its out-of-sample opportunities came from."""
    import pandas as pd
    sd = rows[features].astype(float).std().fillna(0.0)
    pull = sorted(((f, round(float(c) * float(sd[f]), 4)) for f, c in zip(features, coef) if f not in ("lm", "lm_t")),
                  key=lambda kv: -abs(kv[1]))
    out: dict[str, Any] = {"feature_pull_per_sd": dict(pull[:15])}
    if not trades.empty:
        minute_bucket = pd.cut(trades.minute, [0, 3, 6, 9, 12, 15], labels=["1-3", "4-6", "7-9", "10-12", "13-15"])
        out["by_minute"] = _trade_breakdown(trades, minute_bucket.astype(str))
        out["by_side"] = _trade_breakdown(trades, trades.side_yes.map({True: "yes", False: "no"}))
        out["by_coin"] = _trade_breakdown(trades, "coin")
        out["by_entry_price"] = _trade_breakdown(trades, pd.cut(trades.price, [0, .3, .5, .7, .9, 1]).astype(str))
    return out


def train_and_certify(
    quotes: "pd.DataFrame | None" = None, underlying: "pd.DataFrame | None" = None,  # noqa: F821
    spot: "pd.DataFrame | None" = None,  # noqa: F821
) -> dict[str, Any]:
    """`underlying` = already-engineered spot features; `spot` = raw spot
    candles (engineered and graded here). Both default to the HF archives."""
    from data import kalshi_15m_quotes, kalshi_15m_spot

    if quotes is None:
        quotes = kalshi_15m_quotes.load_quote_history(days=HISTORY_DAYS)
    if quotes.empty:
        return {"ok": False, "reason": "no_quote_history"}

    data_validation: dict[str, Any] = {"ok": False, "reason": "no_spot_history"}
    if underlying is None:
        try:
            if spot is None:
                spot = kalshi_15m_spot.load_spot_history(days=HISTORY_DAYS + 2)
            if not spot.empty:
                data_validation = kalshi_15m_spot.grade_against_settlements(quotes, spot)
                underlying = kalshi_15m_spot.engineer_spot_features(spot)
        except Exception as exc:
            logger.warning("[kalshi_15m_edge_model] spot history load failed: %s", exc)
            underlying = None
        spot = None
    frame = build_frame(quotes, underlying)

    markets: dict[str, Any] = {}
    for kind, candidates in CANDIDATES.items():
        part = frame[frame.market == kind]
        results: dict[str, Any] = {}
        trades_by: dict[str, Any] = {}
        for name, features in candidates.items():
            if _uses_spot(features) and "move_z" not in part.columns:
                results[name] = {"certified": False, "reason": "no_spot_history"}
                continue
            rows = part[part.has_underlying] if _uses_spot(features) else part
            if rows.day.nunique() <= TRAIN_MIN_DAYS or rows.y.nunique() < 2:
                results[name] = {"certified": False, "reason": "not_enough_history", "days": int(rows.day.nunique())}
                continue
            oos = walk_forward(rows, features, REGULARIZATION_C[name])
            trades = simulate_rule(oos)
            stats = certify(oos, trades)
            coef, intercept = _fit(rows[features].values, rows.y.values, REGULARIZATION_C[name])
            results[name] = {**stats, "features": features, "coef": coef, "intercept": intercept,
                             "oos_by_coin": _trade_breakdown(trades, "coin")}
            trades_by[name] = (rows, trades)
        certified = [n for n, r in results.items() if r.get("certified")]
        pool = certified or [n for n in results if "coef" in results[n]]
        chosen = min(pool, key=lambda n: results[n].get("log_loss_model", 9.9)) if pool else None
        spec: dict[str, Any] = {
            "model": chosen, "certified": bool(chosen and results[chosen].get("certified")),
            "features": results[chosen]["features"] if chosen else [], "coef": results[chosen]["coef"] if chosen else [],
            "intercept": results[chosen]["intercept"] if chosen else 0.0, "eligible_coins": [], "coin_stats": {},
            "candidates": {n: {k: v for k, v in r.items() if k not in ("coef", "intercept", "features")} for n, r in results.items()},
        }
        if chosen:
            rows, trades = trades_by[chosen]
            spec["eligible_coins"], spec["coin_stats"] = coin_eligibility(
                part, trades, market_certified=spec["certified"], uses_spot=_uses_spot(spec["features"]),
                spot_validation=data_validation,
            )
            spec["learned_patterns"] = learned_patterns(rows, spec["features"], spec["coef"], trades)
        markets[kind] = spec

    artifact = {
        "built_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "quote_rows": int(len(frame)), "windows": int(frame.ticker.nunique()), "days": int(frame.day.nunique()),
        "first_day": frame.day.min(), "last_day": frame.day.max(),
        "rule": {"min_edge": EV_MIN_EDGE, "entry_min_minute": EV_ENTRY_MIN_MINUTE, "entry_max_minute": EV_ENTRY_MAX_MINUTE, "fee_contracts": FEE_CONTRACTS},
        "spot_data_validation": data_validation,
        "markets": markets,
    }
    save_artifact(artifact)
    return {"ok": True, **{k: v for k, v in artifact.items() if k != "markets"},
            "certified": {k: v["certified"] for k, v in markets.items()},
            "chosen": {k: v["model"] for k, v in markets.items()},
            "eligible_coins": {k: v["eligible_coins"] for k, v in markets.items()},
            "candidates": {k: v["candidates"] for k, v in markets.items()}}


def save_artifact(artifact: dict[str, Any]) -> None:
    LOCAL_ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = LOCAL_ARTIFACT_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(artifact, default=str), encoding="utf-8")
    tmp.replace(LOCAL_ARTIFACT_PATH)
    _cache["artifact"] = artifact
    if not HF_API_KEY:
        return
    try:
        from huggingface_hub import HfApi
        HfApi(token=HF_API_KEY).upload_file(
            path_or_fileobj=str(LOCAL_ARTIFACT_PATH), path_in_repo=HF_ARTIFACT_PATH,
            repo_id=HF_KALSHI_15M_DATASET_REPO, repo_type="dataset",
            commit_message=f"kalshi 15m edge model {artifact.get('built_at_utc')}",
        )
    except Exception as exc:
        logger.warning("[kalshi_15m_edge_model] artifact upload failed: %s", exc)


def load_artifact() -> dict[str, Any] | None:
    """Cache, local file, then the published HF copy (retried at most every
    15 minutes). Never trains."""
    if _cache["artifact"] is not None:
        return _cache["artifact"]
    if LOCAL_ARTIFACT_PATH.exists():
        try:
            _cache["artifact"] = json.loads(LOCAL_ARTIFACT_PATH.read_text(encoding="utf-8"))
            return _cache["artifact"]
        except Exception as exc:
            logger.warning("[kalshi_15m_edge_model] local artifact unreadable: %s", exc)
    now = time.time()
    if not HF_API_KEY or now - _cache["last_remote_attempt"] < _REMOTE_RETRY_SEC:
        return None
    _cache["last_remote_attempt"] = now

    def _download() -> dict[str, Any]:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(repo_id=HF_KALSHI_15M_DATASET_REPO, filename=HF_ARTIFACT_PATH, repo_type="dataset", token=HF_API_KEY)
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    try:
        from server_common import call_with_hard_timeout
        artifact = call_with_hard_timeout(_download, timeout_sec=20)
    except Exception as exc:
        logger.info("[kalshi_15m_edge_model] no published artifact yet: %s", exc)
        return None
    if artifact:
        _cache["artifact"] = artifact
    return artifact


def _quote(market: dict[str, Any]) -> tuple[float, float] | None:
    try:
        bid = float(market.get("yes_bid_dollars"))
        ask = float(market.get("yes_ask_dollars"))
    except (TypeError, ValueError):
        return None
    if not (0.0 < bid < ask < 1.0) or ask - bid > EV_MAX_SPREAD:
        return None
    return bid, ask


def live_flow_rows(markets_by_coin: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Per coin, the flow-feature row at the current window's last closed
    minute, computed across every open window at once (cross-market
    features need all of them). One public candlesticks call."""
    import pandas as pd

    from data import kalshi_15m_quotes
    from data.kalshi_client import _request_json

    meta = {}
    for coin, market in markets_by_coin.items():
        if market and market.get("ticker") and market.get("open_time"):
            meta[market["ticker"]] = {**market, "_coin": coin, "_series": market.get("series_ticker") or "", "result": market.get("result") or ""}
    if not meta:
        return {}
    start = min(kalshi_15m_quotes._iso_ts(m["open_time"]) for m in meta.values())  # noqa: SLF001
    payload = _request_json("GET", "/markets/candlesticks", params={
        "market_tickers": ",".join(meta), "start_ts": start, "end_ts": int(time.time()), "period_interval": 1,
    })
    rows = pd.DataFrame(kalshi_15m_quotes.candles_to_rows(meta, payload), columns=kalshi_15m_quotes.COLUMNS)
    if rows.empty:
        return {}
    flow = add_flow_features(rows)
    latest = flow.sort_values("minute").groupby("coin").tail(1)
    return {r["coin"]: r for r in latest.to_dict("records")}


def live_spot_rows(markets_by_coin: dict[str, dict[str, Any]], minute_by_coin: dict[str, float] | None = None) -> dict[str, dict[str, Any]]:
    """Per crypto coin: spot features as of one minute before its quote
    minute (as in training) plus the cross-asset spot features computed
    across every coin at that same minute, through the same functions
    training uses."""
    import pandas as pd

    from data import kalshi_15m_quotes, kalshi_15m_spot

    now = time.time()
    rows = []
    for coin, market in markets_by_coin.items():
        if market_kind(coin) != "crypto" or coin not in kalshi_15m_spot.COINBASE_PRODUCTS or not market.get("open_time"):
            continue
        open_ts = kalshi_15m_quotes._iso_ts(market["open_time"])  # noqa: SLF001
        minute = (minute_by_coin or {}).get(coin)
        if minute is None:
            minute = float(int((now - open_ts) // 60))
        try:
            u = kalshi_15m_spot.live_underlying_row(coin, market, as_of_ts=open_ts + 60 * (int(minute) - 1))
        except Exception as exc:
            logger.warning("[kalshi_15m_edge_model] live spot row failed for %s: %s", coin, exc)
            continue
        if not u or not u.get("floor_strike") or not u.get("volatility_15"):
            continue
        rows.append({**u, "coin": coin, "market": "crypto", "open_ts": open_ts, "minute": float(minute), "mid": 0.5, "has_underlying": True})
    if not rows:
        return {}
    feats = add_cross_spot_features(add_features(pd.DataFrame(rows)))
    keep = UNDERLYING_COLUMNS + ["floor_strike", "minute"] + XSPOT_BASE
    return {r["coin"]: {k: r.get(k) for k in keep} for r in feats.to_dict("records")}


def evaluate_market(
    coin: str, market: dict[str, Any], *, seconds_to_close: float,
    underlying_row: dict[str, Any] | None = None, flow_row: dict[str, Any] | None = None,
    artifact: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Live scoring of one open window: p(yes) from the market's current
    model, the edge of each side after fees, and the side worth taking if
    any. With a flow_row, features come from the last closed minute exactly
    as in training; the edge is then priced at the worse of that minute's
    ask and the live ask, and skipped if the live quote has drifted more
    than EV_MAX_QUOTE_DRIFT from it. Pure apart from the cached artifact."""
    import pandas as pd

    kind = market_kind(coin)
    artifact = artifact if artifact is not None else load_artifact()
    spec = ((artifact or {}).get("markets") or {}).get(kind or "")
    if not spec or not spec.get("coef"):
        return {"ok": False, "reason": "no_edge_model_yet", "market_kind": kind}
    features = spec["features"]
    uses_flow = any(f in FLOW_BASE for f in features)
    live_minute = (900.0 - float(seconds_to_close)) / 60.0
    base = {"market_kind": kind, "model": spec.get("model"), "certified": bool(spec.get("certified")), "minute": round(live_minute, 2)}
    live_quote = _quote(market)
    if live_quote is None:
        return {**base, "ok": False, "reason": "no_valid_quote"}
    live_bid, live_ask = live_quote

    if flow_row is not None and flow_row.get("ticker") == market.get("ticker"):
        close_quote = _quote({"yes_bid_dollars": flow_row.get("yes_bid"), "yes_ask_dollars": flow_row.get("yes_ask")})
        if close_quote is None:
            return {**base, "ok": False, "reason": "no_valid_minute_close_quote"}
        bid, ask = close_quote
        minute = float(flow_row["minute"])
        base["minute"] = minute
        if abs((live_bid + live_ask) / 2.0 - (bid + ask) / 2.0) > EV_MAX_QUOTE_DRIFT:
            return {**base, "ok": False, "reason": "quote_moved_since_minute_close"}
        row: dict[str, Any] = {"mid": (bid + ask) / 2.0, "minute": minute, **{f: flow_row.get(f, 0.0) for f in FLOW_BASE}}
        exec_yes_ask, exec_no_ask = max(ask, live_ask), max(1.0 - bid, 1.0 - live_bid)
    elif uses_flow:
        return {**base, "ok": False, "reason": "flow_features_unavailable"}
    else:
        bid, ask, minute = live_bid, live_ask, live_minute
        row = {"mid": (bid + ask) / 2.0, "minute": minute}
        exec_yes_ask, exec_no_ask = live_ask, 1.0 - live_bid

    if not (EV_ENTRY_MIN_MINUTE <= minute <= EV_ENTRY_MAX_MINUTE):
        return {**base, "ok": False, "reason": "outside_ev_entry_minutes"}
    if _uses_spot(features):
        u = underlying_row or {}
        if not u.get("close") or not u.get("floor_strike") or not u.get("volatility_15"):
            return {**base, "ok": False, "reason": "underlying_unavailable"}
        if any(f in XSPOT_BASE for f in features) and any(u.get(f) is None for f in XSPOT_BASE):
            return {**base, "ok": False, "reason": "cross_asset_spot_unavailable"}
        row.update({k: u.get(k) for k in UNDERLYING_COLUMNS + ["floor_strike"] + XSPOT_BASE})
    feats = add_features(pd.DataFrame([row]))
    x = [float(feats.iloc[0][f]) for f in features]
    p_yes = _sigmoid(spec["intercept"] + sum(c * v for c, v in zip(spec["coef"], x)))
    edge_yes = p_yes - exec_yes_ask - fee_per_contract(exec_yes_ask)
    edge_no = (1.0 - p_yes) - exec_no_ask - fee_per_contract(exec_no_ask)
    side = "yes" if edge_yes >= edge_no else "no"
    return {
        **base, "ok": True, "coin_eligible": coin in (spec.get("eligible_coins") or []),
        "p_yes": round(p_yes, 4), "mid": round(row["mid"], 4),
        "yes_bid": live_bid, "yes_ask": live_ask,
        "edge_yes": round(edge_yes, 4), "edge_no": round(edge_no, 4), "side": side, "edge": round(max(edge_yes, edge_no), 4),
        "p_side": round(p_yes if side == "yes" else 1.0 - p_yes, 4),
        "ask_side": round(exec_yes_ask if side == "yes" else exec_no_ask, 4),
    }


def summary() -> dict[str, Any]:
    artifact = load_artifact()
    if not artifact:
        return {"available": False}
    return {
        "available": True, "built_at_utc": artifact.get("built_at_utc"), "days": artifact.get("days"),
        "windows": artifact.get("windows"), "rule": artifact.get("rule"),
        "spot_data_validation": artifact.get("spot_data_validation"),
        "markets": {
            k: {
                "model": v.get("model"), "certified": v.get("certified"), "eligible_coins": v.get("eligible_coins") or [],
                "coin_stats": v.get("coin_stats"), "learned_patterns": v.get("learned_patterns"), "candidates": v.get("candidates"),
            }
            for k, v in (artifact.get("markets") or {}).items()
        },
    }
