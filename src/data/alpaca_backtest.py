"""Walk-forward backtest for the Alpaca equities strategy -- structurally
mirrors perps_backtest.py (reuses the REAL decide_entry_technical/
decide_exit functions from alpaca_strategy.py, chronological train/test
split, never a random shuffle) but simplified for a cash equities account:
no leverage, no shorts, no per-trade fee model (Alpaca charges $0
commission on equity/ETF trades). Completely separate from and never
touches perps_backtest.py or any Kalshi state.
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score

from data import alpaca_strategy as strat
from data.alpaca_data import FEATURE_COLUMNS

logger = logging.getLogger(__name__)

# n_estimators=60 (not the previous 150/100): real, confirmed production
# OOM incidents on the live equities service (Render's own events: two
# oomKilled restarts roughly ALPACA_INTENSIVE_TRAINING_MINUTES apart,
# i.e. exactly on _run_alpaca_intensive_training's own cadence). That job
# calls fit_backtest_model() (this file) in the SAME call as
# alpaca_model.py's own train_model() -- "trains up to 6 model candidates
# total" per that job's own docstring -- and this file's own candidates
# had never picked up the n_estimators reduction perps_model.py/
# alpaca_options_model.py/alpaca_model.py already proved necessary for
# this exact multi-candidate-fit-in-one-call shape on the same 512MB
# ceiling. This module DOES run on that same memory-capped Render dyno
# (confirmed live via the crash trace above) -- not a one-off local/
# offline script the way some other backtest engines here are.
_CANDIDATES = {
    "logistic_regression": lambda: LogisticRegression(max_iter=1000, class_weight="balanced"),
    "random_forest": lambda: RandomForestClassifier(
        n_estimators=60, max_depth=6, min_samples_leaf=20, class_weight="balanced", random_state=42, n_jobs=1,
    ),
    "gradient_boosting": lambda: GradientBoostingClassifier(
        n_estimators=60, max_depth=3, learning_rate=0.05, random_state=42,
    ),
}


def fit_backtest_model(train_df: pd.DataFrame, *, min_rows: int = 300) -> dict[str, Any] | None:
    """In-memory-only fit -- never touches the live production model files
    or Hugging Face. None (technical-only simulation) if there isn't
    enough training-window data yet."""
    labeled = train_df.dropna(subset=["label_up"] + FEATURE_COLUMNS).copy()
    if len(labeled) < min_rows:
        return None
    labeled["label_up"] = labeled["label_up"].astype(int)
    symbol_categories = list(labeled["symbol"].astype("category").cat.categories)
    labeled["symbol_code"] = labeled["symbol"].astype("category").cat.codes
    labeled = labeled.sort_values("ts")

    feature_cols = FEATURE_COLUMNS + ["symbol_code"]
    split_idx = int(len(labeled) * 0.85)
    train_part, holdout = labeled.iloc[:split_idx], labeled.iloc[split_idx:]
    if holdout.empty or holdout["label_up"].nunique() < 2:
        return None

    best_name, best_model, best_score = None, None, -1.0
    for name, factory in _CANDIDATES.items():
        try:
            model = factory()
            model.fit(train_part[feature_cols].values, train_part["label_up"].values)
            preds = model.predict(holdout[feature_cols].values)
            proba = model.predict_proba(holdout[feature_cols].values)[:, 1]
            acc = float(accuracy_score(holdout["label_up"].values, preds))
            auc = float(roc_auc_score(holdout["label_up"].values, proba)) if holdout["label_up"].nunique() > 1 else 0.5
            combined = (acc + auc) / 2.0
            if combined > best_score:
                best_name, best_model, best_score = name, model, combined
        except Exception as exc:
            logger.warning("[alpaca_backtest] candidate %s failed to fit: %s", name, exc)
    if best_model is None:
        return None
    best_model.fit(labeled[feature_cols].values, labeled["label_up"].values)
    return {"model": best_model, "model_type": best_name, "feature_cols": feature_cols, "symbol_categories": symbol_categories}


def add_model_predictions(df: pd.DataFrame, fitted: dict[str, Any] | None) -> pd.DataFrame:
    """Batch-predict once, reused across a parameter sweep -- same
    large-speedup rationale as perps_backtest.add_model_predictions."""
    df = df.copy()
    if fitted is None:
        df["model_probability_up"] = np.nan
        return df
    categories = fitted["symbol_categories"]
    symbol_codes = df["symbol"].map(lambda s: float(categories.index(s)) if s in categories else -1.0)
    feature_cols = fitted["feature_cols"]
    x = df[[c for c in feature_cols if c != "symbol_code"]].copy()
    x["symbol_code"] = symbol_codes
    x = x[feature_cols].values
    df["model_probability_up"] = fitted["model"].predict_proba(x)[:, 1]
    return df


def simulate(
    test_df: pd.DataFrame, fitted: dict[str, Any] | None = None, *,
    starting_balance: float = 10_000.0,
    position_size_pct: float | None = None,
    max_concurrent_positions: int | None = None,
    min_volume_z: float | None = None,
    min_volatility_ratio: float | None = None,
    entry_dip_pct: float | None = None,
    model_confidence_min: float | None = None,
    daily_loss_cap_pct: float | None = None,
) -> dict[str, Any]:
    """Walk forward through `test_df` (all symbols, sorted by ts) replaying
    the real alpaca_strategy decision functions. Every strategy parameter
    can be overridden per-call for a parameter sweep without touching
    process-wide env vars between runs."""
    position_size_pct = strat.POSITION_SIZE_PCT if position_size_pct is None else position_size_pct
    max_concurrent_positions = strat.MAX_CONCURRENT_POSITIONS if max_concurrent_positions is None else max_concurrent_positions
    min_volume_z = strat.MIN_VOLUME_Z if min_volume_z is None else min_volume_z
    min_volatility_ratio = strat.MIN_VOLATILITY_RATIO if min_volatility_ratio is None else min_volatility_ratio
    entry_dip_pct = strat.ENTRY_DIP_PCT if entry_dip_pct is None else entry_dip_pct
    model_confidence_min = strat.MODEL_CONFIDENCE_MIN if model_confidence_min is None else model_confidence_min
    daily_loss_cap_pct = strat.DAILY_LOSS_CAP_PCT if daily_loss_cap_pct is None else daily_loss_cap_pct

    df = test_df.sort_values("ts").reset_index(drop=True)
    if "model_probability_up" not in df.columns:
        df = add_model_predictions(df, fitted)

    balance = starting_balance
    open_positions: dict[str, dict[str, Any]] = {}
    trades: list[dict[str, Any]] = []
    daily_pnl: dict[str, float] = {}
    daily_reference_balance: dict[str, float] = {}

    for row in df.itertuples(index=False):
        symbol = row.symbol
        price = float(row.close)
        date_str = pd.Timestamp(row.ts, unit="s", tz="UTC").strftime("%Y-%m-%d")
        if date_str not in daily_reference_balance:
            daily_reference_balance[date_str] = balance

        pos = open_positions.get(symbol)
        if pos is not None:
            sim_now = pd.Timestamp(row.ts, unit="s", tz="UTC").to_pydatetime()
            should_exit, reason = strat.decide_exit(pos, price, now=sim_now)
            if should_exit:
                gross = round((price - pos["entry_price"]) * pos["count"], 6)
                realized = gross  # no per-trade fee model -- Alpaca is commission-free on equities/ETFs
                balance += realized
                daily_pnl[date_str] = daily_pnl.get(date_str, 0.0) + realized
                trades.append({
                    "symbol": symbol, "entry_price": pos["entry_price"], "exit_price": price,
                    "count": pos["count"], "realized_pnl_usd": realized, "reason": reason,
                    "opened_ts": pos["opened_ts"], "closed_ts": row.ts,
                    "held_minutes": (row.ts - pos["opened_ts"]) / 60.0,
                })
                del open_positions[symbol]
            continue

        if symbol in open_positions or len(open_positions) >= max_concurrent_positions:
            continue

        reference_balance = daily_reference_balance[date_str]
        if reference_balance > 0 and daily_pnl.get(date_str, 0.0) <= -abs(daily_loss_cap_pct) * reference_balance:
            continue  # daily loss cap breached -- exits still happen above, only new entries are blocked

        dollar_volume_z = row.dollar_volume_z
        volatility_5 = row.volatility_5
        volatility_30 = row.volatility_30
        dist_to_ma_15 = row.dist_to_ma_15
        short_ma = price / (1 + dist_to_ma_15) if (1 + dist_to_ma_15) != 0 else price
        dip_pct = (short_ma - price) / short_ma if short_ma > 0 else 0.0

        proba_up = row.model_probability_up
        model_ok = proba_up == proba_up  # not NaN -> a model exists

        volume_ok = dollar_volume_z is not None and dollar_volume_z == dollar_volume_z and dollar_volume_z >= min_volume_z
        volatility_ok = volatility_30 <= 0 or (volatility_5 / volatility_30) >= min_volatility_ratio
        if not (volume_ok and volatility_ok and dip_pct >= entry_dip_pct):
            continue
        if model_ok and not (proba_up >= 0.5 and proba_up >= model_confidence_min):
            continue

        committed = sum(p["entry_price"] * p["count"] for p in open_positions.values())
        available = balance - committed
        budget = available * position_size_pct
        count = int(budget // price) if price > 0 else 0
        if count < 1:
            continue

        open_positions[symbol] = {
            "entry_price": price, "count": float(count),
            "opened_at": pd.Timestamp(row.ts, unit="s", tz="UTC").isoformat(), "opened_ts": row.ts,
        }

    total_pnl = sum(t["realized_pnl_usd"] for t in trades)
    wins = [t for t in trades if t["realized_pnl_usd"] > 0]
    span_days = max(1e-9, (df["ts"].max() - df["ts"].min()) / 86400.0) if not df.empty else 1.0

    return {
        "starting_balance": starting_balance,
        "ending_balance_realized": round(starting_balance + total_pnl, 6),
        "total_realized_pnl_usd": round(total_pnl, 6),
        "return_pct": round(total_pnl / starting_balance, 6) if starting_balance else 0.0,
        "trade_count": len(trades),
        "win_count": len(wins),
        "win_rate": round(len(wins) / len(trades), 4) if trades else 0.0,
        "trades_per_day": round(len(trades) / span_days, 3),
        "open_positions_at_end": len(open_positions),
        "span_days": round(span_days, 2),
        "trades": trades,
    }


# A small, deliberately bounded grid -- this runs as a recurring BACKGROUND
# job on the same memory-constrained dyno as live trading, potentially many
# times over a single off-hours window, not as a one-off offline script
# (see the Kalshi perps bot's own much larger, one-time 450-combo sweep
# for contrast). Widening this list trades more thorough coverage for more
# CPU/memory per run -- keep it modest.
_SWEEP_GRID = [
    {"take_profit_pct": 0.01, "stop_loss_pct": 0.008, "max_hold_minutes": 120},
    {"take_profit_pct": 0.02, "stop_loss_pct": 0.015, "max_hold_minutes": 240},
    {"take_profit_pct": 0.015, "stop_loss_pct": 0.01, "max_hold_minutes": 60},
    {"take_profit_pct": 0.008, "stop_loss_pct": 0.006, "max_hold_minutes": 30},
]


def run_config_sweep(
    test_with_preds: pd.DataFrame, *, starting_balance: float = 100.0, min_trades: int = 5,
) -> dict[str, Any]:
    """Tries _SWEEP_GRID's small set of take-profit/stop-loss/max-hold
    combinations against the SAME fitted-model predictions (cheap -- no
    re-fitting per config), ranked by return_pct among configs that fired
    at least `min_trades` (avoids crowning a config that "won" on 1-2 lucky
    trades). Reports findings only -- never applies a new config to the
    live strategy itself; that stays a deliberate, reviewed decision.

    Restores alpaca_strategy's real (env-configured) parameters before
    returning, no matter what -- each grid entry temporarily overwrites
    those SAME module-level globals to reuse simulate()'s real decide_exit
    logic, and leaving the last grid entry's values in place would mean
    live trading silently runs on leftover sweep parameters instead of its
    actual configured ones."""
    original = {
        "take_profit_pct": strat.TAKE_PROFIT_PCT, "stop_loss_pct": strat.STOP_LOSS_PCT,
        "max_hold_minutes": strat.MAX_HOLD_MINUTES,
    }
    try:
        results = []
        for config in _SWEEP_GRID:
            strat.TAKE_PROFIT_PCT = config["take_profit_pct"]
            strat.STOP_LOSS_PCT = config["stop_loss_pct"]
            strat.MAX_HOLD_MINUTES = config["max_hold_minutes"]
            result = simulate(test_with_preds, fitted=None, starting_balance=starting_balance)
            results.append({**config, "trade_count": result["trade_count"], "win_rate": result["win_rate"], "return_pct": result["return_pct"]})
    finally:
        strat.TAKE_PROFIT_PCT = original["take_profit_pct"]
        strat.STOP_LOSS_PCT = original["stop_loss_pct"]
        strat.MAX_HOLD_MINUTES = original["max_hold_minutes"]

    qualified = [r for r in results if r["trade_count"] >= min_trades]
    ranked = sorted(qualified or results, key=lambda r: -r["return_pct"])
    return {"all_configs": results, "ranked": ranked, "best": ranked[0] if ranked else None}
