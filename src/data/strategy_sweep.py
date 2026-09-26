"""Generic parameter-sweep engine over any *_backtest.py module's own
run_walkforward_backtest/simulate/fit_backtest_model/load_training_dataset
contract -- built per explicit user direction: "we need to work on over
10000 mix of strategies in the backtest and... perform a forward test
with real data and a huge historical data of the main 3 we will trade...
generate[] [strategies,] optimise the model to understand that."

Key efficiency insight (already anticipated in kalshi_15m_backtest.py's
own module comments on why add_model_predictions is vectorized and
cached): NONE of the strategy parameters this sweep varies
(model_confidence_min, yes_confidence_extra_required, assumed_entry_price,
position_size_pct, max_concurrent_positions) affect how the underlying
model is TRAINED -- fit_backtest_model never sees any of them, only
simulate() does. So a sweep across thousands of combinations never needs
to refit a model thousands of times: each walk-forward FOLD fits its own
model exactly ONCE (identical fold slicing to walkforward.run_walkforward_folds,
so results line up with a plain run_walkforward_backtest call on the
same data/bounds) and caches that fold's own model_probability_up column
via add_model_predictions; every one of the sweep's parameter
combinations then replays that SAME cached column through simulate() --
a cheap threshold/sizing-only pass over already-labeled rows, not a
refit. This is what makes "over 10000 strategies" computationally
realistic within a background job's own reasonable runtime, not a
promise this module ever fabricates -- see run_parameter_sweep's own
`elapsed_sec` in its result for the real, measured cost each time it
runs.

Same "reuse the real decision functions, never reimplement the rules"
principle every backtest module here already holds itself to -- this
module contains NO strategy logic of its own; it only decides WHICH
parameter combinations to try (a plain cartesian product over the caller's
own grid) and how to RANK their own already-real simulate() results,
using the SAME "a strategy that only looks good on one lucky fold hasn't
learned anything durable" walk-forward discipline walkforward.py's own
module docstring already establishes -- a combination only earns a
ranking once it cleared a REAL evidence bar (min_trades_per_fold per
fold, min_folds_with_trades folds), matching this codebase's own
MIN_BUCKET_TRADES-style gating everywhere else (kalshi_15m_trade_analysis.py,
coin_is_trusted/hour_is_trusted).
"""
from __future__ import annotations

import itertools
import logging
import time
from typing import Any

import pandas as pd

from data import walkforward

logger = logging.getLogger(__name__)

# A deliberately generous default ceiling, not a promise every sweep hits
# it -- see run_parameter_sweep's own max_combinations param. 10,000+ is
# realistic (see this module's own docstring on why), but an accidental
# huge grid (e.g. 6 params x 20 values each = 64,000,000) must fail fast
# and clearly rather than silently running for hours.
DEFAULT_MAX_COMBINATIONS = 50_000


def cartesian_product(param_grid: dict[str, list[Any]]) -> list[dict[str, Any]]:
    """Every combination of param_grid's own values -- {"a": [1, 2], "b":
    [3, 4]} -> [{"a": 1, "b": 3}, {"a": 1, "b": 4}, {"a": 2, "b": 3},
    {"a": 2, "b": 4}]. {} (no params to sweep) -> [{}] (exactly one
    "combination": the callee's own defaults) rather than an empty list,
    so a caller sweeping zero dimensions still gets one real backtest
    run, not silence."""
    if not param_grid:
        return [{}]
    keys = list(param_grid.keys())
    return [dict(zip(keys, combo)) for combo in itertools.product(*(param_grid[k] for k in keys))]


def _fold_score(fold_result: dict[str, Any]) -> dict[str, Any] | None:
    """One fold's own simulate() result, reduced to what the ranking below
    needs -- None (skip, not "0 risk") when the fold produced no trades at
    all; a combination that never fires hasn't been tested by that fold,
    it's simply invisible to it."""
    if fold_result.get("trade_count", 0) <= 0:
        return None
    return {
        "return_pct": fold_result.get("return_pct", 0.0),
        "win_rate": fold_result.get("win_rate", 0.0),
        "trade_count": fold_result.get("trade_count", 0),
        "directional_accuracy": fold_result.get("directional_accuracy"),
    }


def _build_fold_frames(backtest_module: Any, combined: pd.DataFrame, fold_bounds: list[tuple[float, float, float]]) -> list[dict[str, Any]]:
    """Fits + caches predictions for each fold EXACTLY once -- identical
    quantile-based train/test slicing to walkforward.run_walkforward_folds
    (see that function's own implementation), so results are directly
    comparable to a plain run_walkforward_backtest call on the same
    data/bounds. Returns a list of {"test_df_with_predictions",
    "fold_bounds", "train_rows", "test_rows", "model_used"} dicts, one per
    fold that had enough training data -- skips exactly the folds
    walkforward.py's own driver would skip, for the same reason."""
    fold_frames: list[dict[str, Any]] = []
    for train_start_q, test_start_q, test_end_q in fold_bounds:
        train_start_ts = combined["ts"].quantile(train_start_q)
        test_start_ts = combined["ts"].quantile(test_start_q)
        test_end_ts = combined["ts"].quantile(test_end_q)
        train_df = combined[(combined["ts"] >= train_start_ts) & (combined["ts"] < test_start_ts)]
        test_df = combined[(combined["ts"] >= test_start_ts) & (combined["ts"] <= test_end_ts)]
        if len(train_df) < 300 or test_df.empty:
            logger.info(
                "[strategy_sweep] skipping fold (%.2f, %.2f, %.2f): train_rows=%d test_rows=%d",
                train_start_q, test_start_q, test_end_q, len(train_df), len(test_df),
            )
            continue
        try:
            fitted = backtest_module.fit_backtest_model(train_df)
            test_with_preds = backtest_module.add_model_predictions(test_df, fitted)
        except Exception as exc:
            logger.warning("[strategy_sweep] fold (%.2f, %.2f, %.2f) failed to fit/predict: %s", train_start_q, test_start_q, test_end_q, exc)
            continue
        fold_frames.append({
            "test_df": test_with_preds, "fold_bounds": [train_start_q, test_start_q, test_end_q],
            "train_rows": len(train_df), "test_rows": len(test_df),
            "model_used": fitted.get("model_type") if isinstance(fitted, dict) else None,
        })
    return fold_frames


def run_parameter_sweep(
    backtest_module: Any, param_grid: dict[str, list[Any]], *,
    days: int | None = None, coins: list[str] | None = None,
    fold_bounds: list[tuple[float, float, float]] | None = None,
    starting_balance: float = 100.0, min_trades_per_fold: int = 5,
    min_folds_with_trades: int = 2, top_n: int = 25,
    max_combinations: int = DEFAULT_MAX_COMBINATIONS, max_seconds: float | None = 1800.0,
) -> dict[str, Any]:
    """Runs EVERY combination in param_grid's own cartesian product through
    a REAL walk-forward backtest -- the SAME multi-fold, expanding-window
    forward-test discipline every *_backtest.py module here already uses
    -- against `backtest_module`'s own real data/fit/simulate functions
    (pass the module object itself: kalshi_15m_backtest or
    kalshi_15m_metals_backtest). Ranks surviving combinations by walk-
    forward consistency, requiring a REAL sample (min_trades_per_fold per
    fold, min_folds_with_trades separate folds) before a combination is
    even considered -- a combination that "wins" on 2 trades in 1 fold
    hasn't demonstrated anything.

    `max_seconds` (default 30 minutes) is a real, hard wall-clock safety
    valve -- this runs on the SAME shared, single-process, multi-market
    container every other live trading job here runs on (see
    combined_app.py's own docstring), and a genuinely huge grid (the
    default caller-facing grid comfortably exceeds 10,000 combinations)
    run unbounded could starve that process's own live trading
    responsiveness for hours. Checked between combinations (never mid-
    fold-simulation), so this always stops at a clean combination
    boundary, never a half-evaluated one -- a run that hits this returns
    everything scored so far with "stopped_early": True rather than
    nothing at all.

    Returns {"ok", "combinations_tried", "combinations_evaluated",
    "combinations_with_evidence", "folds_used", "top_strategies": [...],
    "elapsed_sec", "stopped_early"} on success, or {"ok": False, "reason":
    ...} when there's no data, no qualifying folds, or the requested grid
    exceeds max_combinations (fails fast and clearly rather than silently
    running for hours). Each `top_strategies` entry: {"params",
    "folds_with_evidence", "total_folds", "mean_return_pct",
    "std_return_pct", "profitable_fold_ratio", "mean_win_rate",
    "total_trades"} -- sorted by (profitable_fold_ratio, mean_return_pct)
    descending, the same "consistency first, then magnitude" ordering
    walkforward.summarize_folds's own docstring already argues for (a
    strategy profitable in 4/4 real folds beats one profitable in 1/4
    folds even if that one fold was bigger)."""
    started = time.monotonic()
    combos = cartesian_product(param_grid)
    if len(combos) > max_combinations:
        return {"ok": False, "reason": "grid_too_large", "combinations_requested": len(combos), "max_combinations": max_combinations}

    combined = backtest_module.load_training_dataset()
    if combined.empty:
        return {"ok": False, "reason": "no_data"}
    if coins:
        combined = combined[combined["symbol"].isin(coins)]
    if days:
        cutoff = combined["ts"].max() - days * 86400
        combined = combined[combined["ts"] >= cutoff]
    combined = backtest_module._one_row_per_window(combined)  # noqa: SLF001 -- same-package sibling module, not a public API boundary
    if combined.empty:
        return {"ok": False, "reason": "no_data"}

    bounds = fold_bounds or walkforward.DEFAULT_FOLD_BOUNDS
    fold_frames = _build_fold_frames(backtest_module, combined, bounds)
    if not fold_frames:
        return {"ok": False, "reason": "no_qualifying_folds"}

    results: list[dict[str, Any]] = []
    stopped_early = False
    combinations_evaluated = 0
    for combo in combos:
        if max_seconds is not None and (time.monotonic() - started) >= max_seconds:
            stopped_early = True
            break
        combinations_evaluated += 1
        fold_scores = []
        for fold in fold_frames:
            try:
                sim = backtest_module.simulate(fold["test_df"], None, starting_balance=starting_balance, **combo)
            except Exception as exc:
                logger.warning("[strategy_sweep] combo %s failed on fold %s: %s", combo, fold["fold_bounds"], exc)
                continue
            scored = _fold_score(sim)
            if scored and scored["trade_count"] >= min_trades_per_fold:
                fold_scores.append(scored)
        if len(fold_scores) < min_folds_with_trades:
            continue
        returns = [f["return_pct"] for f in fold_scores]
        win_rates = [f["win_rate"] for f in fold_scores]
        mean_return = sum(returns) / len(returns)
        results.append({
            "params": combo,
            "folds_with_evidence": len(fold_scores),
            "total_folds": len(fold_frames),
            "mean_return_pct": round(mean_return, 6),
            "std_return_pct": round(float(pd.Series(returns).std(ddof=0)), 6) if len(returns) > 1 else 0.0,
            "profitable_fold_ratio": round(sum(1 for r in returns if r > 0) / len(returns), 4),
            "mean_win_rate": round(sum(win_rates) / len(win_rates), 4),
            "total_trades": sum(f["trade_count"] for f in fold_scores),
        })

    results.sort(key=lambda r: (r["profitable_fold_ratio"], r["mean_return_pct"]), reverse=True)
    elapsed = time.monotonic() - started
    return {
        "ok": True, "combinations_tried": len(combos), "combinations_evaluated": combinations_evaluated,
        "combinations_with_evidence": len(results), "stopped_early": stopped_early,
        "folds_used": len(fold_frames), "fold_models_used": [f["model_used"] for f in fold_frames],
        "top_strategies": results[:top_n], "elapsed_sec": round(elapsed, 2),
    }
