"""Generic parameter-sweep engine over any *_backtest.py module's own
fit_backtest_model/add_model_predictions/simulate contract -- built per
explicit user direction: "we need to work on over 10000 mix of strategies
in the backtest and... perform a forward test with real data and a huge
historical data of the main 3 we will trade... generate[] [strategies,]
optimise the model to understand that." Then widened further, per
"HF should have a model that generate millions of strategies... if this
work we will expand to the other bots" and "let do this for all the bots
all of those jobs are handle by HF": genuinely market-agnostic (perps,
kalshi_15m crypto, kalshi_15m metals, alpaca stocks/crypto/options all
share the same fit/predict/simulate shape -- see scripts/strategy_sweep_job.py,
which drives this module for all six), parallelizable across CPU cores so
"millions of combinations" is a real wall-clock possibility on real HF Job
compute (not this codebase's own live trading process -- see that
script's own docstring on why), and able to genuinely FORWARD-test its own
survivors against a slice of real data none of the walk-forward folds
that picked them ever saw.

Key efficiency insight (already anticipated in kalshi_15m_backtest.py's
own module comments on why add_model_predictions is vectorized and
cached): NONE of the strategy parameters this sweep varies
(model_confidence_min, yes_confidence_extra_required, assumed_entry_price,
position_size_pct, max_concurrent_positions, ...) affect how the
underlying model is TRAINED -- fit_backtest_model never sees any of them,
only simulate() does. So a sweep across thousands (or millions) of
combinations never needs to refit a model thousands of times: each
walk-forward FOLD fits its own model exactly ONCE (identical fold slicing
to walkforward.run_walkforward_folds, so results line up with a plain
run_walkforward_backtest call on the same data/bounds) and caches that
fold's own model_probability_up column via add_model_predictions; every
one of the sweep's parameter combinations then replays that SAME cached
column through simulate() -- a cheap threshold/sizing-only pass over
already-labeled rows, not a refit. This is what makes "over 10000
strategies" (or, with real multi-core parallelism -- see n_workers below
-- genuinely millions) computationally realistic within a real job's own
runtime, not a promise this module ever fabricates -- see
run_parameter_sweep's own `elapsed_sec` in its result for the real,
measured cost each time it runs.

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

Forward-test holdout (`holdout_bounds`): the walk-forward folds above
already guard against "only looks good on one lucky split," but every one
of those folds' TEST windows still fed into the very same ranking that
picked a combination's parameters -- a combination that "wins" this sweep
could still just be the one that happened to fit those specific windows
best (the multiple-comparisons problem: test enough combinations against
the same finite real history and some will look good by chance alone).
`holdout_bounds` reserves one MORE slice of real data -- by convention,
the most recent one, via DEFAULT_FOLD_BOUNDS_WITH_HOLDOUT below -- that no
walk-forward fold's train OR test window ever touches, and only evaluates
it against the sweep's own already-chosen top_n survivors AFTER ranking,
never using it to pick or re-rank them. A combination's `forward_tested`
flag means it also cleared min_holdout_trades on data neither its own
selection nor any fold's fit ever saw -- the closest this module can get
to "would this genuinely still work going forward," not just "did it fit
the past." See run_parameter_sweep's own docstring for exactly what's
attached to each top_strategies entry.
"""
from __future__ import annotations

import importlib
import itertools
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Any

import pandas as pd

from data import walkforward

logger = logging.getLogger(__name__)

# A deliberately generous default ceiling, not a promise every sweep hits
# it -- see run_parameter_sweep's own max_combinations param. 10,000+ is
# realistic (see this module's own docstring on why), and with real
# multi-core parallelism (n_workers > 1) millions is too -- but an
# accidental huge grid (e.g. 6 params x 20 values each = 64,000,000) must
# fail fast and clearly rather than silently running for hours. Raise this
# per-call (scripts/strategy_sweep_job.py does, once it knows its own
# n_workers/max_seconds budget) rather than raising the default itself.
DEFAULT_MAX_COMBINATIONS = 50_000

# Real, live finding (not a hypothetical): the already-deployed 16,200-
# combination kalshi_15m_metals sweep surfaced top_strategies with
# mean_return_pct as high as 3.09e+20 -- traced to a low assumed_entry_price
# (kalshi_15m_backtest.simulate's/kalshi_15m_metals_backtest.simulate's own
# disclosed pricing-assumption limitation: EVERY trade fills at one fixed
# price regardless of the model's real confidence) combined with
# position_size_pct reinvesting a % of an ever-growing balance across
# 700+ trades in a single fold -- genuine multiplicative compounding, not
# a display bug, but one with no real-world liquidity/fill-size ceiling to
# bound it the way an actual Kalshi order book would. A number like this
# is not a strategy that "won" the sweep, it's a numerical artifact of
# probing a combination a real account could never actually execute at
# that size -- excluded from ranking/top_strategies entirely (counted
# separately in combinations_excluded_as_unrealistic) rather than left to
# silently crowd out real, plausible findings or mislead anyone glancing
# at "top strategy: 3e20% return."
#
# First set to 100,000 (1000x) as a "clearly generous, never hides a real
# finding" ceiling -- but a REAL full 544,000-combination run on real HF
# Job compute (see docs/HF_JOBS_STRATEGY_SWEEP_MIGRATION.md) showed that
# was still too loose: entries at ~98,000% (980x an account, in the SAME
# assumed_entry_price=0.15 mechanism) slipped through easily, just under
# the old bar. Tightened to 5,000% (50x an account in one fold's test
# window) -- even a genuinely exceptional real trading edge sustained
# over a few hundred trades in a matter of weeks should not plausibly
# exceed that; anything beyond it is far more likely to be this same
# fixed-price-compounding artifact than a real, executable finding.
MAX_PLAUSIBLE_MEAN_RETURN_PCT = 5_000.0

# The same 4 expanding-window folds walkforward.DEFAULT_FOLD_BOUNDS
# already uses, minus the last one -- reserved instead as an untouched
# forward-test holdout (see this module's own docstring on why). Passed
# together as `fold_bounds=DEFAULT_FOLD_BOUNDS_WITH_HOLDOUT,
# holdout_bounds=DEFAULT_HOLDOUT_BOUNDS` by scripts/strategy_sweep_job.py.
DEFAULT_FOLD_BOUNDS_WITH_HOLDOUT: list[tuple[float, float, float]] = [
    (0.00, 0.40, 0.55),
    (0.00, 0.55, 0.70),
    (0.00, 0.70, 0.85),
]
DEFAULT_HOLDOUT_BOUNDS: tuple[float, float] = (0.85, 1.00)


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


def _score_combo(combo: dict[str, Any], fold_scores: list[dict[str, Any]], total_folds: int) -> dict[str, Any]:
    """Shared by both the sequential and parallel execution paths so they
    build byte-for-byte the same result shape from the same raw per-fold
    scores -- the only difference between the two paths is WHERE simulate()
    actually runs, never how its output is summarized."""
    returns = [f["return_pct"] for f in fold_scores]
    win_rates = [f["win_rate"] for f in fold_scores]
    mean_return = sum(returns) / len(returns)
    return {
        "params": combo,
        "folds_with_evidence": len(fold_scores),
        "total_folds": total_folds,
        "mean_return_pct": round(mean_return, 6),
        "std_return_pct": round(float(pd.Series(returns).std(ddof=0)), 6) if len(returns) > 1 else 0.0,
        "profitable_fold_ratio": round(sum(1 for r in returns if r > 0) / len(returns), 4),
        "mean_win_rate": round(sum(win_rates) / len(win_rates), 4),
        "total_trades": sum(f["trade_count"] for f in fold_scores),
    }


def _build_fold_frames(
    backtest_module: Any, combined: pd.DataFrame, fold_bounds: list[tuple[float, float, float]],
) -> list[dict[str, Any]]:
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


# ---------------------------------------------------------------------------
# Parallel execution -- module-level (picklable) worker state + functions,
# used only when run_parameter_sweep's own n_workers > 1. A fresh
# ProcessPoolExecutor's initializer sets these once per WORKER PROCESS
# (not once per task), so the real per-task cost stays exactly what it is
# sequentially: one simulate() call per fold per combination. Workers only
# ever need simulate() (a pure function of an already-predicted DataFrame
# + hyperparameters) -- fit_backtest_model/add_model_predictions already
# ran once in the parent process building fold_frames, so no model or
# training data ever needs to cross the process boundary, only the
# already-labeled test frames (see _build_fold_frames).
# ---------------------------------------------------------------------------
_worker_backtest_module: Any = None
_worker_fold_frames: list[dict[str, Any]] = []
_worker_starting_balance: float = 100.0
_worker_min_trades_per_fold: int = 5
_worker_extra_simulate_kwargs: dict[str, Any] = {}


def _sweep_worker_init(
    backtest_module_name: str, fold_frames: list[dict[str, Any]],
    starting_balance: float, min_trades_per_fold: int, extra_simulate_kwargs: dict[str, Any],
) -> None:
    global _worker_backtest_module, _worker_fold_frames, _worker_starting_balance
    global _worker_min_trades_per_fold, _worker_extra_simulate_kwargs
    _worker_backtest_module = importlib.import_module(backtest_module_name)
    _worker_fold_frames = fold_frames
    _worker_starting_balance = starting_balance
    _worker_min_trades_per_fold = min_trades_per_fold
    _worker_extra_simulate_kwargs = extra_simulate_kwargs


def _sweep_worker_evaluate(combo: dict[str, Any]) -> dict[str, Any]:
    """Runs in a worker process: replays ONE parameter combination across
    every precomputed fold, returning the SAME {"combo", "fold_scores"}
    shape the sequential path builds inline -- filtering by
    min_trades_per_fold here (not in the parent) keeps the IPC payload
    small when most combinations don't clear it."""
    fold_scores = []
    for fold in _worker_fold_frames:
        try:
            sim = _worker_backtest_module.simulate(
                fold["test_df"], None, starting_balance=_worker_starting_balance,
                **_worker_extra_simulate_kwargs, **combo,
            )
        except Exception as exc:
            logger.warning("[strategy_sweep] combo %s failed on fold %s: %s", combo, fold["fold_bounds"], exc)
            continue
        scored = _fold_score(sim)
        if scored and scored["trade_count"] >= _worker_min_trades_per_fold:
            fold_scores.append(scored)
    return {"combo": combo, "fold_scores": fold_scores}


def run_parameter_sweep(
    backtest_module: Any, param_grid: dict[str, list[Any]], *,
    combined: pd.DataFrame | None = None,
    days: int | None = None, coins: list[str] | None = None,
    fold_bounds: list[tuple[float, float, float]] | None = None,
    holdout_bounds: tuple[float, float] | None = None, min_holdout_trades: int = 10,
    starting_balance: float = 100.0, min_trades_per_fold: int = 5,
    min_folds_with_trades: int = 2, top_n: int = 25,
    max_combinations: int = DEFAULT_MAX_COMBINATIONS, max_seconds: float | None = 1800.0,
    n_workers: int = 1, extra_simulate_kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Runs EVERY combination in param_grid's own cartesian product through
    a REAL walk-forward backtest -- the SAME multi-fold, expanding-window
    forward-test discipline every *_backtest.py module here already uses
    -- against `backtest_module`'s own real fit/predict/simulate functions
    (pass the module object itself: perps_backtest, alpaca_backtest,
    alpaca_crypto_backtest, alpaca_options_backtest, kalshi_15m_backtest,
    or kalshi_15m_metals_backtest -- see scripts/strategy_sweep_job.py for
    how each one is actually driven). Ranks surviving combinations by
    walk-forward consistency, requiring a REAL sample (min_trades_per_fold
    per fold, min_folds_with_trades separate folds) before a combination
    is even considered -- a combination that "wins" on 2 trades in 1 fold
    hasn't demonstrated anything.

    `combined`: pass an already-loaded, already-preprocessed DataFrame
    (matching the shape `backtest_module.simulate` expects: sorted by
    "ts", already one row per decision point) for a market whose own data
    pipeline doesn't fit the kalshi_15m-shaped
    `backtest_module.load_training_dataset()` /
    `backtest_module._one_row_per_window()` convention (perps and the 3
    alpaca markets build their own combined frame differently -- see each
    one's own run_walkforward_backtest). When omitted (kalshi_15m's own
    call sites), falls back to that convention automatically, applying
    `coins`/`days` the same way run_walkforward_backtest already does.

    `n_workers` (default 1, sequential -- unchanged, fully backward
    compatible behavior): when > 1, evaluates combinations across a real
    ProcessPoolExecutor instead of one at a time, which is what actually
    makes "millions of combinations" a real wall-clock possibility (see
    this module's own docstring) rather than a number nobody could ever
    wait for. Numerically identical results either way -- parallelism only
    changes WHERE simulate() runs and in what order results complete, the
    final ranking is sorted the same way regardless. Only meant for a real,
    dedicated job (see scripts/strategy_sweep_job.py); the default of 1
    is deliberately what every existing in-Space call site still gets.

    `extra_simulate_kwargs`: fixed (non-swept) kwargs merged into every
    simulate() call -- e.g. perps_backtest.simulate's own
    leverage_by_ticker, computed once by the caller rather than re-derived
    per combination.

    `holdout_bounds` / `min_holdout_trades`: see this module's own
    docstring on the forward-test this adds on top of walk-forward ranking
    -- when given, every surviving top_n entry also gets a `holdout`
    sub-dict ({"return_pct", "win_rate", "trade_count", "forward_tested"})
    from replaying that SAME combination against a slice of `combined`
    no fold's fit or test window above ever touched. `forward_tested` is
    True only when that holdout slice ALSO produced at least
    min_holdout_trades real trades -- a combination that never fires in
    the holdout window hasn't been forward-tested at all, whatever its
    walk-forward ranking says.

    `max_seconds` (default 30 minutes) is a real, hard wall-clock safety
    valve. Checked between combinations when sequential, between batches
    of `n_workers`-sized work when parallel -- never mid-fold-simulation,
    so this always stops at a clean boundary, never a half-evaluated
    combination. A run that hits this returns everything scored so far
    with "stopped_early": True rather than nothing at all.

    Returns {"ok", "combinations_tried", "combinations_evaluated",
    "combinations_with_evidence", "combinations_excluded_as_unrealistic",
    "folds_used", "top_strategies": [...], "elapsed_sec", "stopped_early"}
    on success, or {"ok": False, "reason":
    ...} when there's no data, no qualifying folds, or the requested grid
    exceeds max_combinations (fails fast and clearly rather than silently
    running for hours). Each `top_strategies` entry: {"params",
    "folds_with_evidence", "total_folds", "mean_return_pct",
    "std_return_pct", "profitable_fold_ratio", "mean_win_rate",
    "total_trades"[, "holdout"]} -- sorted by (profitable_fold_ratio,
    mean_return_pct) descending, the same "consistency first, then
    magnitude" ordering walkforward.summarize_folds's own docstring
    already argues for (a strategy profitable in 4/4 real folds beats one
    profitable in 1/4 folds even if that one fold was bigger)."""
    started = time.monotonic()
    extra_simulate_kwargs = extra_simulate_kwargs or {}
    combos = cartesian_product(param_grid)
    if len(combos) > max_combinations:
        return {"ok": False, "reason": "grid_too_large", "combinations_requested": len(combos), "max_combinations": max_combinations}

    if combined is None:
        combined = backtest_module.load_training_dataset()
        if combined.empty:
            return {"ok": False, "reason": "no_data"}
        if coins:
            combined = combined[combined["symbol"].isin(coins)]
        if days:
            cutoff = combined["ts"].max() - days * 86400
            combined = combined[combined["ts"] >= cutoff]
        if hasattr(backtest_module, "_one_row_per_window"):
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
    combinations_excluded_as_unrealistic = 0

    def _keep_or_reject(combo: dict[str, Any], fold_scores: list[dict[str, Any]]) -> None:
        nonlocal combinations_excluded_as_unrealistic
        entry = _score_combo(combo, fold_scores, len(fold_frames))
        if abs(entry["mean_return_pct"]) > MAX_PLAUSIBLE_MEAN_RETURN_PCT:
            combinations_excluded_as_unrealistic += 1
            logger.warning(
                "[strategy_sweep] excluding combo %s: mean_return_pct=%.3e exceeds MAX_PLAUSIBLE_MEAN_RETURN_PCT "
                "(%.0f) -- see this module's own docstring on the real compounding artifact this guards against",
                combo, entry["mean_return_pct"], MAX_PLAUSIBLE_MEAN_RETURN_PCT,
            )
            return
        results.append(entry)

    if n_workers > 1:
        batch_size = max(1, n_workers * 8)
        idx = 0
        with ProcessPoolExecutor(
            max_workers=n_workers, initializer=_sweep_worker_init,
            initargs=(backtest_module.__name__, fold_frames, starting_balance, min_trades_per_fold, extra_simulate_kwargs),
        ) as pool:
            while idx < len(combos):
                if max_seconds is not None and (time.monotonic() - started) >= max_seconds:
                    stopped_early = True
                    break
                batch = combos[idx: idx + batch_size]
                idx += len(batch)
                for outcome in pool.map(_sweep_worker_evaluate, batch):
                    combinations_evaluated += 1
                    fold_scores = outcome["fold_scores"]
                    if len(fold_scores) < min_folds_with_trades:
                        continue
                    _keep_or_reject(outcome["combo"], fold_scores)
    else:
        for combo in combos:
            if max_seconds is not None and (time.monotonic() - started) >= max_seconds:
                stopped_early = True
                break
            combinations_evaluated += 1
            fold_scores = []
            for fold in fold_frames:
                try:
                    sim = backtest_module.simulate(fold["test_df"], None, starting_balance=starting_balance, **extra_simulate_kwargs, **combo)
                except Exception as exc:
                    logger.warning("[strategy_sweep] combo %s failed on fold %s: %s", combo, fold["fold_bounds"], exc)
                    continue
                scored = _fold_score(sim)
                if scored and scored["trade_count"] >= min_trades_per_fold:
                    fold_scores.append(scored)
            if len(fold_scores) < min_folds_with_trades:
                continue
            _keep_or_reject(combo, fold_scores)

    results.sort(key=lambda r: (r["profitable_fold_ratio"], r["mean_return_pct"]), reverse=True)
    top_strategies = results[:top_n]

    if holdout_bounds is not None and top_strategies:
        holdout_fold_frames = _build_fold_frames(backtest_module, combined, [(0.0, holdout_bounds[0], holdout_bounds[1])])
        if holdout_fold_frames:
            holdout_df = holdout_fold_frames[0]["test_df"]
            for entry in top_strategies:
                try:
                    sim = backtest_module.simulate(holdout_df, None, starting_balance=starting_balance, **extra_simulate_kwargs, **entry["params"])
                    trade_count = sim.get("trade_count", 0)
                    entry["holdout"] = {
                        "return_pct": sim.get("return_pct", 0.0), "win_rate": sim.get("win_rate", 0.0),
                        "trade_count": trade_count, "forward_tested": trade_count >= min_holdout_trades,
                    }
                except Exception as exc:
                    logger.warning("[strategy_sweep] holdout evaluation failed for %s: %s", entry["params"], exc)
                    entry["holdout"] = {"return_pct": 0.0, "win_rate": 0.0, "trade_count": 0, "forward_tested": False, "error": str(exc)}
        else:
            logger.info("[strategy_sweep] holdout_bounds given but no qualifying holdout fold (insufficient data) -- top_strategies left without a 'holdout' key")

    elapsed = time.monotonic() - started
    return {
        "ok": True, "combinations_tried": len(combos), "combinations_evaluated": combinations_evaluated,
        "combinations_with_evidence": len(results), "combinations_excluded_as_unrealistic": combinations_excluded_as_unrealistic,
        "stopped_early": stopped_early,
        "folds_used": len(fold_frames), "fold_models_used": [f["model_used"] for f in fold_frames],
        "top_strategies": top_strategies, "elapsed_sec": round(elapsed, 2), "n_workers": n_workers,
    }
