"""Walk-forward backtest for the Alpaca options strategy -- structurally
mirrors alpaca_backtest.py (reuses alpaca_options_data's own
FEATURE_COLUMNS, chronological train/holdout split, never a random
shuffle), but options needs one real adaptation the equities/crypto/perps
backtests don't: this codebase has NEVER archived historical OPTION
PREMIUM quotes over time (see alpaca_options_strategy.py's own scan_and_enter
docstring -- "this pipeline doesn't record option-quote history over time,
only point-in-time quotes at scan time"), only the underlying's own
minute bars. There is no real historical premium series to replay a
contract's actual price path against.

Rather than fabricate a fake premium history and pretend it's real, this
backtest is explicit about what it actually measures: whether
evaluate_candidate's technical-dip + model-confidence entry signal, fired
against REAL archived underlying data, would have anticipated a real,
profitable directional move -- the thing that actually drives an option's
value regardless of the exact Greeks. The synthetic premium path used to
apply decide_exit's real percentage-based TAKE_PROFIT_PCT/STOP_LOSS_PCT/
MAX_HOLD_MINUTES logic is the underlying's own subsequent return, scaled
by OPTIONS_PREMIUM_SENSITIVITY -- the SAME "a 1% underlying move can
easily be a 10-20%+ option premium move" ratio alpaca_options_strategy.py's
own module docstring already assumes when it set TAKE_PROFIT_PCT=30%/
STOP_LOSS_PCT=20% (percentages that would be absurd applied directly to a
stock's own 1-2%/day typical moves), not a new, unjustified number
invented for this file. Real premium behavior also depends on IV and time
decay this simplification doesn't model -- this backtest validates
DIRECTIONAL SIGNAL QUALITY, not exact expected option P&L; treat its
return_pct as directional evidence for tuning TAKE_PROFIT_PCT/STOP_LOSS_PCT/
MAX_HOLD_MINUTES/MODEL_CONFIDENCE_MIN, not a promise of real trading
returns.

Applies identically whether the live strategy is actually trading naked
contracts or debit spreads (see ENTRY_STRATEGY) -- a spread's real max
gain/loss are both smaller in magnitude than a naked contract's (capped,
defined-risk), but the DIRECTION and RELATIVE ordering of which entry
signals were the best ones is the same either way, since a spread is
still fundamentally a bet on the same underlying direction this
backtest's signal-quality measurement is about.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score

from data import alpaca_options_strategy as strat
from data.alpaca_options_data import FEATURE_COLUMNS

logger = logging.getLogger(__name__)

# See this module's own docstring for why this specific ratio, not an
# arbitrary one: the same "10-20x" relationship already implied by
# alpaca_options_strategy.py's own TAKE_PROFIT_PCT=30%/STOP_LOSS_PCT=20%
# defaults being set roughly an order of magnitude wider than the
# underlying's own typical daily move. Env-overridable since it's
# explicitly a simplification, not a measured constant.
OPTIONS_PREMIUM_SENSITIVITY = float(os.getenv("ALPACA_OPTIONS_BACKTEST_PREMIUM_SENSITIVITY", "12.0") or "12.0")

_CANDIDATES = {
    "logistic_regression": lambda: LogisticRegression(max_iter=1000, class_weight="balanced"),
    "random_forest": lambda: RandomForestClassifier(
        n_estimators=150, max_depth=6, min_samples_leaf=20, class_weight="balanced", random_state=42, n_jobs=1,
    ),
    "gradient_boosting": lambda: GradientBoostingClassifier(
        n_estimators=100, max_depth=3, learning_rate=0.05, random_state=42,
    ),
}


def fit_backtest_model(train_df: pd.DataFrame, *, min_rows: int = 300) -> dict[str, Any] | None:
    """In-memory-only fit -- never touches the live production model files
    or Hugging Face. A single chronological holdout split (not the live
    model's own walk-forward CV) -- deliberately the faster, cheaper shape,
    same reasoning perps_backtest.py already documents: this can run
    repeatedly inside a parameter sweep, optimized for fast repeated runs,
    not the one daily production retrain. None (technical-only simulation)
    if there isn't enough training-window data yet."""
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
            logger.warning("[alpaca_options_backtest] candidate %s failed to fit: %s", name, exc)
    if best_model is None:
        return None
    best_model.fit(labeled[feature_cols].values, labeled["label_up"].values)
    return {"model": best_model, "model_type": best_name, "feature_cols": feature_cols, "symbol_categories": symbol_categories}


def add_model_predictions(df: pd.DataFrame, fitted: dict[str, Any] | None) -> pd.DataFrame:
    """Batch-predict once, reused across a parameter sweep -- same
    large-speedup rationale as alpaca_backtest.add_model_predictions."""
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
    starting_balance: float = 500.0,
    position_size_pct: float | None = None,
    max_concurrent_positions: int | None = None,
    model_confidence_min: float | None = None,
    take_profit_pct: float | None = None,
    stop_loss_pct: float | None = None,
    max_hold_minutes: int | None = None,
    daily_loss_cap_pct: float | None = None,
    premium_sensitivity: float | None = None,
) -> dict[str, Any]:
    """Walk forward through `test_df` (all underlyings, sorted by ts),
    replaying evaluate_candidate's REAL technical-dip + model-confidence
    signal (see alpaca_options_strategy.py) -- entries require a real
    model prediction, exactly like the live strategy's own "no technical-
    only cold-start fallback" rule. A synthetic premium path (see this
    module's own docstring for OPTIONS_PREMIUM_SENSITIVITY) stands in for
    the real option quote history this codebase has never archived, fed
    through the REAL decide_exit/position_exit_levels percentage logic so
    the actual live TAKE_PROFIT_PCT/STOP_LOSS_PCT/MAX_HOLD_MINUTES
    thresholds are what get evaluated, not a reimplementation of them.

    decide_exit()/compute_contract_qty() read TAKE_PROFIT_PCT/STOP_LOSS_PCT/
    MAX_HOLD_MINUTES/POSITION_SIZE_PCT as MODULE-LEVEL globals on
    alpaca_options_strategy, not as function parameters -- so a sweep over
    these has to temporarily overwrite those same globals for the
    duration of this call and restore them afterward no matter what
    (never leave live trading running on leftover sweep values), the same
    real mechanism alpaca_backtest.py's own run_config_sweep already
    needs, just centralized here in simulate() itself so ANY caller
    (direct or via run_config_sweep) gets it correctly, not just the sweep
    path."""
    position_size_pct = strat.POSITION_SIZE_PCT if position_size_pct is None else position_size_pct
    max_concurrent_positions = strat.MAX_CONCURRENT_POSITIONS if max_concurrent_positions is None else max_concurrent_positions
    model_confidence_min = strat.MODEL_CONFIDENCE_MIN if model_confidence_min is None else model_confidence_min
    take_profit_pct = strat.TAKE_PROFIT_PCT if take_profit_pct is None else take_profit_pct
    stop_loss_pct = strat.STOP_LOSS_PCT if stop_loss_pct is None else stop_loss_pct
    max_hold_minutes = strat.MAX_HOLD_MINUTES if max_hold_minutes is None else max_hold_minutes
    daily_loss_cap_pct = strat.DAILY_LOSS_CAP_PCT if daily_loss_cap_pct is None else daily_loss_cap_pct
    premium_sensitivity = OPTIONS_PREMIUM_SENSITIVITY if premium_sensitivity is None else premium_sensitivity

    original_globals = {
        "TAKE_PROFIT_PCT": strat.TAKE_PROFIT_PCT, "STOP_LOSS_PCT": strat.STOP_LOSS_PCT,
        "MAX_HOLD_MINUTES": strat.MAX_HOLD_MINUTES, "POSITION_SIZE_PCT": strat.POSITION_SIZE_PCT,
    }
    strat.TAKE_PROFIT_PCT = take_profit_pct
    strat.STOP_LOSS_PCT = stop_loss_pct
    strat.MAX_HOLD_MINUTES = max_hold_minutes
    strat.POSITION_SIZE_PCT = position_size_pct
    try:
        return _simulate_inner(
            test_df, fitted, starting_balance=starting_balance, max_concurrent_positions=max_concurrent_positions,
            model_confidence_min=model_confidence_min, daily_loss_cap_pct=daily_loss_cap_pct,
            premium_sensitivity=premium_sensitivity,
        )
    finally:
        strat.TAKE_PROFIT_PCT = original_globals["TAKE_PROFIT_PCT"]
        strat.STOP_LOSS_PCT = original_globals["STOP_LOSS_PCT"]
        strat.MAX_HOLD_MINUTES = original_globals["MAX_HOLD_MINUTES"]
        strat.POSITION_SIZE_PCT = original_globals["POSITION_SIZE_PCT"]


def _simulate_inner(
    test_df: pd.DataFrame, fitted: dict[str, Any] | None, *,
    starting_balance: float, max_concurrent_positions: int, model_confidence_min: float,
    daily_loss_cap_pct: float, premium_sensitivity: float,
) -> dict[str, Any]:
    """The actual walk-forward loop -- pulled out of simulate() purely so
    that function's try/finally global-restore wrapper doesn't have to
    nest the entire loop body a level deeper. Only ever called from
    simulate(), with the real alpaca_options_strategy globals it depends
    on (TAKE_PROFIT_PCT/STOP_LOSS_PCT/MAX_HOLD_MINUTES/POSITION_SIZE_PCT)
    already set for this run."""
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
        underlying_price = float(row.close)
        if underlying_price <= 0:
            continue
        date_str = pd.Timestamp(row.ts, unit="s", tz="UTC").strftime("%Y-%m-%d")
        if date_str not in daily_reference_balance:
            daily_reference_balance[date_str] = balance

        pos = open_positions.get(symbol)
        if pos is not None:
            # Synthetic premium: this contract's own entry premium scaled
            # by the underlying's move since entry x premium_sensitivity
            # (a directional-bet leverage proxy -- see module docstring),
            # floored at a cent since a real premium can't go negative.
            underlying_change_pct = (underlying_price - pos["entry_underlying_price"]) / pos["entry_underlying_price"]
            signed_change = underlying_change_pct if pos["direction"] == "up" else -underlying_change_pct
            synthetic_premium = max(0.01, pos["entry_price"] * (1 + signed_change * premium_sensitivity))

            sim_now = pd.Timestamp(row.ts, unit="s", tz="UTC").to_pydatetime()
            should_exit, reason = strat.decide_exit(pos, synthetic_premium, now=sim_now)
            if should_exit:
                gross = round((synthetic_premium - pos["entry_price"]) * pos["count"] * 100, 6)
                balance += gross
                daily_pnl[date_str] = daily_pnl.get(date_str, 0.0) + gross
                trades.append({
                    "symbol": symbol, "entry_price": pos["entry_price"], "exit_price": synthetic_premium,
                    "count": pos["count"], "realized_pnl_usd": gross, "reason": reason,
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

        proba_up = row.model_probability_up
        model_ok = proba_up == proba_up  # not NaN -> a model exists
        if not model_ok:
            continue  # no technical-only fallback -- matches evaluate_candidate exactly
        if proba_up >= model_confidence_min:
            direction = "up"
        elif proba_up <= (1.0 - model_confidence_min):
            direction = "down"
        else:
            continue

        # No real contract chain to select a strike from here (this
        # backtest only has archived UNDERLYING bars) -- entry premium is
        # approximated as ENTRY_PREMIUM_PCT_OF_UNDERLYING of the
        # underlying's own price, a rough near-the-money weekly-option
        # rule of thumb, only used to size a plausible contract count via
        # the real compute_contract_qty.
        entry_premium = max(0.01, underlying_price * _ENTRY_PREMIUM_PCT_OF_UNDERLYING)
        available_balance = balance - sum(p["entry_price"] * p["count"] * 100 for p in open_positions.values())
        count = strat.compute_contract_qty(available_balance, entry_premium)
        if count < 1:
            continue

        open_positions[symbol] = {
            "entry_price": entry_premium, "count": count, "direction": direction,
            "entry_underlying_price": underlying_price,
            "opened_at": pd.Timestamp(row.ts, unit="s", tz="UTC").isoformat(), "opened_ts": row.ts,
            # decide_exit needs a real expiration_date -- approximate
            # MID_DAYS_TO_EXPIRATION out from entry, matching select_contract's
            # own MIN/MAX_DAYS_TO_EXPIRATION window's rough midpoint.
            "expiration_date": (pd.Timestamp(row.ts, unit="s", tz="UTC") + pd.Timedelta(days=21)).date().isoformat(),
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


# Rough near-the-money weekly-option rule of thumb -- only used to pick a
# PLAUSIBLE entry premium for contract-count sizing in simulate() above,
# since this backtest has no real contract chain to select a strike/
# premium from (only archived underlying bars). Not a claim of pricing
# accuracy -- see this module's own docstring. Calibrated to this
# codebase's own already-validated affordability boundary, not an
# independent guess: at the real SIMULATE_STARTING_BALANCE=$500 x
# POSITION_SIZE_PCT=20%, a slot's budget is exactly $100, and
# compute_contract_qty needs contract_price*100 <= $100 -- i.e. a premium
# at or under $1.00 -- to ever afford even one contract at all (the same
# "$1.00-premium contract" reference point alpaca_options_strategy.py's
# own POSITION_SIZE_PCT/MAX_CONCURRENT_POSITIONS comment was explicitly
# tuned around). 0.005 (0.5%) keeps AAPL's (~$195) assumed premium just
# under that boundary (~$0.98); confirmed live in review that a naive
# 2%-or-even-0.8% guess instead priced every synthetic underlying here
# out of the real $500 balance entirely -- every sweep config fired ZERO
# trades, silently making the whole sweep meaningless -- caught before
# shipping, not a theoretical concern.
_ENTRY_PREMIUM_PCT_OF_UNDERLYING = float(os.getenv("ALPACA_OPTIONS_BACKTEST_ENTRY_PREMIUM_PCT", "0.005") or "0.005")


# Small, deliberately bounded grid -- runs as a recurring BACKGROUND job on
# the same memory-constrained dyno as live trading (see
# alpaca_options_server.py's own off-hours-only sweep job), not a one-off
# offline script. Widening this trades more thorough coverage for more
# CPU/memory per run -- keep it modest.
_SWEEP_GRID = [
    {"take_profit_pct": 0.30, "stop_loss_pct": 0.20, "max_hold_minutes": 180, "model_confidence_min": 0.58},
    {"take_profit_pct": 0.50, "stop_loss_pct": 0.25, "max_hold_minutes": 240, "model_confidence_min": 0.55},
    {"take_profit_pct": 0.20, "stop_loss_pct": 0.15, "max_hold_minutes": 90, "model_confidence_min": 0.60},
    {"take_profit_pct": 0.40, "stop_loss_pct": 0.30, "max_hold_minutes": 180, "model_confidence_min": 0.52},
]


def run_config_sweep(
    test_with_preds: pd.DataFrame, *, starting_balance: float = 500.0, min_trades: int = 5,
) -> dict[str, Any]:
    """Tries _SWEEP_GRID's small set of take-profit/stop-loss/max-hold/
    model-confidence combinations against the SAME fitted-model
    predictions (cheap -- no re-fitting per config), ranked by return_pct
    among configs that fired at least `min_trades` (avoids crowning a
    config that "won" on 1-2 lucky trades). Reports findings only -- never
    applies a new config to the live strategy itself; that stays a
    deliberate, reviewed decision."""
    results = []
    for config in _SWEEP_GRID:
        result = simulate(
            test_with_preds, fitted=None, starting_balance=starting_balance,
            take_profit_pct=config["take_profit_pct"], stop_loss_pct=config["stop_loss_pct"],
            max_hold_minutes=config["max_hold_minutes"], model_confidence_min=config["model_confidence_min"],
        )
        results.append({**config, "trade_count": result["trade_count"], "win_rate": result["win_rate"], "return_pct": result["return_pct"]})

    qualified = [r for r in results if r["trade_count"] >= min_trades]
    ranked = sorted(qualified or results, key=lambda r: -r["return_pct"])
    return {"all_configs": results, "ranked": ranked, "best": ranked[0] if ranked else None}
