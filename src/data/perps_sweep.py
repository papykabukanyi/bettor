"""Parameter sweep over the perps backtest engine (perps_backtest.py) --
explores a large, randomly-sampled combinatorial space across every study/
flag the live bot has (correlation study, scale-in, partial exit,
conviction-scaled sizing, plus the core confidence/shorts tunables) against
REAL historical Kalshi data, ranks configs by walk-forward performance, and
reports the winners.

Two-stage, not brute-force: a literal grid across this many dimensions would
be many orders of magnitude too large to run in any reasonable time (a
single simulate() call over a multi-week, multi-instrument window already
takes real seconds). Stage 1 screens N randomly-sampled configs with ONE
fast single-split simulate() call each, on data fetched and model-predicted
ONCE up front (see simulate()'s own docstring on why precomputing
model_probability_up once matters for a sweep). Stage 2 re-validates only
the top-K survivors with a full multi-fold walk-forward run
(walkforward.run_walkforward_folds) so a config that only looked good on
one lucky split doesn't get crowned a winner -- directly the same
"backtest need to happen in multiple time frame[s] to learn patterns"
principle run_walkforward_backtest itself exists for.

Every value in the parameter space below is a REAL, already-live-coded
tunable (perps_strategy.py's own module constants) -- this sweep doesn't
invent new knobs, it searches the space of ones that already exist.
"""
from __future__ import annotations

import logging
import random
import time
from typing import Any

import pandas as pd

from data import perps_backtest as bt
from data import perps_strategy as strat
from data import walkforward

logger = logging.getLogger(__name__)

# Passed straight through as simulate() kwargs -- these ARE simulate()'s own
# parameters (use_scale_in/use_partial_exit/use_conviction_sizing included,
# even though internally they're applied via a temporary module-attribute
# patch inside simulate() itself -- see that function's own docstring).
SIMULATE_KWARG_SPACE: dict[str, list[Any]] = {
    "model_confidence_min": [0.55, 0.58, 0.62, 0.65],
    "enable_shorts": [True, False],
    "use_correlation_study": [True, False],
    "correlation_confidence_max_adjustment": [0.04, 0.06, 0.10],
    "use_scale_in": [True, False],
    "use_partial_exit": [True, False],
    "use_conviction_sizing": [True, False],
}
# These are the SUB-parameters of the 3 new features -- read as bare
# perps_strategy module globals inside adaptive_exit_pcts/_should_scale_in/
# compute_conviction_size_multiplier/compute_scale_in_count, with no
# dedicated simulate() parameter (see simulate()'s own docstring for why --
# matches how several PRE-EXISTING constants like PROMISING_VOLUME_Z are
# already varied in an ad hoc sweep, module-attribute-first). A sweep sets
# these directly on `strat` before each simulate() call.
MODULE_ATTR_SPACE: dict[str, list[Any]] = {
    "SCALE_IN_MIN_PROGRESS_FRACTION": [0.3, 0.5, 0.7],
    "SCALE_IN_SIZE_FRACTION": [0.3, 0.5, 0.75],
    "SCALE_IN_MAX_TOTAL_SIZE_MULTIPLE": [1.25, 1.5, 2.0],
    "PARTIAL_EXIT_FRACTION": [0.3, 0.5, 0.7],
    "PARTIAL_EXIT_STOP_LOCK_FRACTION": [0.15, 0.3, 0.5],
    "CONVICTION_SIZE_MIN_MULTIPLIER": [0.5, 0.7, 0.85],
    "CONVICTION_SIZE_MAX_MULTIPLIER": [1.25, 1.5, 2.0],
}


def _sample_config(rng: random.Random) -> dict[str, Any]:
    cfg = {k: rng.choice(v) for k, v in SIMULATE_KWARG_SPACE.items()}
    cfg.update({k: rng.choice(v) for k, v in MODULE_ATTR_SPACE.items()})
    return cfg


def _apply_module_attrs(cfg: dict[str, Any]) -> None:
    for name in MODULE_ATTR_SPACE:
        setattr(strat, name, cfg[name])


def _score(result: dict[str, Any]) -> float:
    """Stage-1 ranking metric: return_pct, lightly discounted when a config
    produced very few trades (a rare-but-strong signal isn't thrown out --
    it's just not favored over one with more supporting evidence, since a
    5-trade sample can look great by chance)."""
    trades = result.get("trade_count", 0)
    confidence = min(1.0, trades / 15.0)
    return result.get("return_pct", 0.0) * confidence


def sample_configs(n_configs: int, *, seed: int = 42) -> list[dict[str, Any]]:
    """Deduplicated random sample from the combined parameter space --
    exposed separately from run_sweep so a caller can inspect/log the exact
    configs about to be screened before committing to the (slow) run."""
    rng = random.Random(seed)
    seen: set[tuple] = set()
    configs: list[dict[str, Any]] = []
    attempts = 0
    max_attempts = max(n_configs * 20, 1000)
    while len(configs) < n_configs and attempts < max_attempts:
        attempts += 1
        cfg = _sample_config(rng)
        key = tuple(sorted(cfg.items()))
        if key in seen:
            continue
        seen.add(key)
        configs.append(cfg)
    return configs


def run_sweep(
    *, n_configs: int = 300, days: int = 30, top_k: int = 10, tickers: list[str] | None = None,
    seed: int = 42, progress_every: int = 25,
) -> dict[str, Any]:
    """Fetches real history for `tickers` (default: the live watchlist)
    covering `days`, fits one model on the training 70% (same 70/30 split
    run_backtest uses), then screens `n_configs` sampled parameter
    combinations against the test 30% with a single simulate() call each.
    The top `top_k` by score get re-validated with a full multi-fold
    walk-forward run for robustness.

    Returns {"ok", "tickers", "days", "n_configs_screened", "train_rows",
    "test_rows", "screened_top_50", "finalists"} -- each finalist carries
    both its stage-1 single-split stats AND its stage-2 walk-forward
    confirmation stats, so a result that looked good on the fast screen but
    falls apart across folds is visibly distinguishable from one that
    holds up."""
    watchlist = tickers or bt.get_watchlist()
    frames = []
    for ticker in watchlist:
        try:
            feats = bt.build_ticker_frame(ticker, days=days)
            if not feats.empty:
                frames.append(feats)
        except Exception as exc:
            logger.warning("[perps_sweep] build_ticker_frame failed for %s: %s", ticker, exc)
    if not frames:
        return {"ok": False, "reason": "no_data"}
    combined = pd.concat(frames, ignore_index=True).sort_values("ts")
    cutoff_ts = combined["ts"].quantile(0.7)
    train_df = combined[combined["ts"] < cutoff_ts]
    test_df = combined[combined["ts"] >= cutoff_ts]
    if test_df.empty:
        return {"ok": False, "reason": "no_test_rows"}
    present_tickers = sorted(test_df["ticker"].unique())
    leverage_by_ticker = bt.fetch_leverage_by_ticker(present_tickers)

    fitted = bt.fit_backtest_model(train_df)
    test_df = bt.add_model_predictions(test_df, fitted)  # precomputed ONCE, reused by every screened config

    configs = sample_configs(n_configs, seed=seed)
    original_module_attrs = {name: getattr(strat, name) for name in MODULE_ATTR_SPACE}

    screened: list[dict[str, Any]] = []
    t0 = time.time()
    try:
        for i, cfg in enumerate(configs):
            _apply_module_attrs(cfg)
            simulate_kwargs = {k: cfg[k] for k in SIMULATE_KWARG_SPACE}
            try:
                result = bt.simulate(test_df, fitted=None, leverage_by_ticker=leverage_by_ticker, **simulate_kwargs)
            except Exception as exc:
                logger.warning("[perps_sweep] config %d failed: %s", i, exc)
                continue
            screened.append({
                "config": dict(cfg), "return_pct": result["return_pct"], "win_rate": result["win_rate"],
                "trade_count": result["trade_count"], "trades_per_day": result.get("trades_per_day"),
                "score": _score(result),
            })
            if progress_every and (i + 1) % progress_every == 0:
                elapsed = time.time() - t0
                logger.info(
                    "[perps_sweep] screened %d/%d configs (%.1fs elapsed, %.2fs/config, best so far return_pct=%.4f)",
                    i + 1, len(configs), elapsed, elapsed / (i + 1),
                    max((r["return_pct"] for r in screened), default=0.0),
                )

        screened.sort(key=lambda r: r["score"], reverse=True)
        finalists = [dict(r) for r in screened[:top_k]]

        for finalist in finalists:
            cfg = finalist["config"]
            _apply_module_attrs(cfg)
            simulate_kwargs = {k: cfg[k] for k in SIMULATE_KWARG_SPACE}
            try:
                wf = walkforward.run_walkforward_folds(
                    combined, fit_fn=bt.fit_backtest_model, simulate_fn=bt.simulate,
                    simulate_kwargs={"leverage_by_ticker": leverage_by_ticker, **simulate_kwargs},
                )
                finalist["walkforward"] = {
                    k: wf.get(k) for k in
                    ("fold_count", "profitable_fold_ratio", "mean_return_pct", "std_return_pct", "mean_win_rate")
                }
            except Exception as exc:
                logger.warning("[perps_sweep] walk-forward confirmation failed for a finalist: %s", exc)
                finalist["walkforward"] = {"error": str(exc)}
    finally:
        for name, value in original_module_attrs.items():
            setattr(strat, name, value)

    elapsed_total = time.time() - t0
    return {
        "ok": True, "tickers": watchlist, "days": days,
        "n_configs_requested": n_configs, "n_configs_screened": len(screened),
        "train_rows": len(train_df), "test_rows": len(test_df),
        "elapsed_sec": round(elapsed_total, 1),
        "screened_top_50": screened[:50], "finalists": finalists,
    }
