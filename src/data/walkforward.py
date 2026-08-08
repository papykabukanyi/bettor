"""Shared expanding-window, multi-fold walk-forward backtest driver, used
by all 4 asset classes (perps_backtest.py, alpaca_backtest.py,
alpaca_crypto_backtest.py, alpaca_options_backtest.py).

Every one of those modules already does a single chronological 70/30
train/test split (`run_backtest`). That answers "would this have worked on
one holdout period" but not "does this hold up across DIFFERENT market
regimes" -- a strategy that looks great on one lucky 30% slice and falls
apart on the next one isn't a strategy that's actually learned a durable
pattern, it's overfit to whatever happened to be in that slice.

This module carves the SAME combined, already-fetched multi-ticker/symbol
dataframe into several overlapping-train / non-overlapping-test folds via
quantile cutoffs on `ts` (expanding window: every fold's training data
starts from the very beginning, so later folds train on strictly more
history, mirroring how the live bot's own model only ever accumulates more
data over time -- never a random shuffle, which would leak future rows into
a "past" training window). Each fold independently fits a fresh in-memory
model on its own training slice and simulates its own test slice using the
SAME real `fit_fn`/`simulate_fn` each service's backtest module already
uses for its single-split `run_backtest`, so this is never a second,
diverging implementation of the entry/exit rules.

Deliberately NOT wired into any of the 4 services' recurring scheduled
jobs: fitting up to 3 model candidates per fold, times N folds, is a
multiple of the already-memory-sensitive single-split sweep those jobs run
(confirmed real OOM history on the stocks/options/crypto dynos this
session). This is reusable, callable, testable module code -- invoked
on-demand (a manual API call, an offline script, or a test), exactly how
perps_backtest.run_backtest() itself is already never called from
app_kalshi.py.
"""
from __future__ import annotations

import logging
import math
from typing import Any, Callable

import pandas as pd

logger = logging.getLogger(__name__)

# Expanding window: train always starts at the very beginning (0.00); only
# the test slice's own start/end move forward each fold. 4 folds, each
# testing on a fresh ~15-point-of-the-timeline slice, leaves every fold's
# training window strictly non-overlapping with its OWN test window while
# still growing fold-over-fold -- the exact shape already proven ad-hoc
# earlier this session investigating perps' stale_position mechanism.
DEFAULT_FOLD_BOUNDS: list[tuple[float, float, float]] = [
    (0.00, 0.40, 0.55),
    (0.00, 0.55, 0.70),
    (0.00, 0.70, 0.85),
    (0.00, 0.85, 1.00),
]


def run_walkforward_folds(
    combined: pd.DataFrame,
    *,
    fit_fn: Callable[[pd.DataFrame], Any],
    simulate_fn: Callable[..., dict[str, Any]],
    fold_bounds: list[tuple[float, float, float]] | None = None,
    simulate_kwargs: dict[str, Any] | None = None,
    min_train_rows: int = 300,
) -> dict[str, Any]:
    """Run `fold_bounds` folds of (fit on train quantile-slice, simulate on
    test quantile-slice) over `combined` (must have a `ts` column, any
    number of rows/tickers). `fit_fn`/`simulate_fn` are each service's own
    REAL `fit_backtest_model`/`simulate` -- this function only decides
    which rows go in which fold, never how a fold gets scored.

    A fold with too little training data (`min_train_rows`, matching each
    service's own `fit_backtest_model` default) or an empty test slice is
    skipped rather than force-fit on too little evidence -- same
    evidence-gated discipline as the confidence-tuning work elsewhere in
    this codebase (CONFIDENCE_TUNING_MIN_TRADES). `fit_fn` is still called
    with whatever rows are there even below `min_train_rows`, since every
    `fit_backtest_model` implementation already returns None (technical-
    only simulation) below its own `min_rows` -- this just skips the fold
    entirely when there's nothing to test at all."""
    fold_bounds = fold_bounds or DEFAULT_FOLD_BOUNDS
    simulate_kwargs = simulate_kwargs or {}
    if combined.empty:
        return {"ok": False, "reason": "no_data"}

    folds: list[dict[str, Any]] = []
    for train_start_q, test_start_q, test_end_q in fold_bounds:
        train_start_ts = combined["ts"].quantile(train_start_q)
        test_start_ts = combined["ts"].quantile(test_start_q)
        test_end_ts = combined["ts"].quantile(test_end_q)
        train_df = combined[(combined["ts"] >= train_start_ts) & (combined["ts"] < test_start_ts)]
        test_df = combined[(combined["ts"] >= test_start_ts) & (combined["ts"] <= test_end_ts)]
        if len(train_df) < min_train_rows or test_df.empty:
            logger.info(
                "[walkforward] skipping fold (%.2f, %.2f, %.2f): train_rows=%d test_rows=%d",
                train_start_q, test_start_q, test_end_q, len(train_df), len(test_df),
            )
            continue

        try:
            fitted = fit_fn(train_df)
            result = simulate_fn(test_df, fitted, **simulate_kwargs)
        except Exception as exc:
            logger.warning("[walkforward] fold (%.2f, %.2f, %.2f) failed: %s", train_start_q, test_start_q, test_end_q, exc)
            continue

        result["fold_bounds"] = [train_start_q, test_start_q, test_end_q]
        result["train_rows"] = len(train_df)
        result["test_rows"] = len(test_df)
        result["model_used"] = fitted.get("model_type") if isinstance(fitted, dict) else None
        folds.append(result)

    if not folds:
        return {"ok": False, "reason": "no_fold_had_enough_data"}

    return {"ok": True, "folds": folds, **summarize_folds(folds)}


def summarize_folds(folds: list[dict[str, Any]]) -> dict[str, Any]:
    """Cross-fold consistency stats, not just a total. "Learning a pattern"
    means a strategy that holds up across MULTIPLE separate periods -- a
    big total return built from one spectacular fold and three losing ones
    is exactly the overfit-to-one-slice failure mode this module exists to
    catch, so `profitable_fold_ratio`/`std_return_pct` are surfaced
    alongside the aggregate, not buried inside `folds` for the caller to
    have to compute themselves."""
    returns = [f["return_pct"] for f in folds]
    n = len(returns)
    mean_return = sum(returns) / n
    variance = sum((r - mean_return) ** 2 for r in returns) / n
    std_return = math.sqrt(variance)
    win_rates = [f["win_rate"] for f in folds if f.get("trade_count", 0) > 0]
    profitable_folds = sum(1 for r in returns if r > 0)

    return {
        "fold_count": n,
        "profitable_fold_count": profitable_folds,
        "profitable_fold_ratio": round(profitable_folds / n, 4),
        "total_trade_count": sum(f.get("trade_count", 0) for f in folds),
        "total_realized_pnl_usd": round(sum(f.get("total_realized_pnl_usd", 0.0) for f in folds), 6),
        "mean_return_pct": round(mean_return, 6),
        "std_return_pct": round(std_return, 6),
        "min_return_pct": round(min(returns), 6),
        "max_return_pct": round(max(returns), 6),
        "mean_win_rate": round(sum(win_rates) / len(win_rates), 4) if win_rates else 0.0,
    }
