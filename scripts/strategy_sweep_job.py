"""Runs ONE real, evidence-gated, walk-forward-and-forward-tested strategy
sweep for ONE market, as a standalone process -- no Flask, no APScheduler.
Built to run as a Hugging Face Job (see docs/HF_JOBS_STRATEGY_SWEEP_MIGRATION.md
for the exact commands, compute-flavor cost table, and scheduling) rather
than as a background thread on the live, shared, multi-market trading
Space -- per explicit user direction: "let take full advantage of HF
pro... need to create model that... generate a lot... 1000 of strategies
that is fully backtested and fully forward tested and ready to fire...
need to use all the resource[s] across and apply to each bot what its
need[s]", "let do this for all the bots all of those jobs are handle by
HF", and "we need to take full advantage of the CPU and RAM to the max to
generate multiple real strategies and combinations with real data."

Reuses data.strategy_sweep.run_parameter_sweep for every market -- this
script's own job is just per-market wiring: which backtest module, how to
build its `combined` dataframe (see MARKET_CONFIGS below -- kalshi_15m and
stocks fit that module's own load_training_dataset()/`_one_row_per_window`
convention directly; options needs one extra preprocessing call; crypto
and perps build their own combined frame from a live per-symbol fetch, the
same way each one's own run_walkforward_backtest already does), which
default parameter grid to sweep, and which of that market's own existing
HF_*_MODEL_REPO to publish the result to (server_common.push_json_to_hf --
the SAME generic small-JSON-to-HF-repo helper this codebase already uses
for a sweep/backfill/walkforward result "pushed by a job that no longer
runs on this Render service at all" -- see that function's own
docstring). Each live server's own status/sweep route reads this same
file back via pull_json_from_hf -- see docs/HF_JOBS_STRATEGY_SWEEP_MIGRATION.md's
own wiring section for exactly which route, per market.

Real, measured throughput this script's own grid sizing is based on (not
guessed): a single simulate() call over a ~1,000-row fold took ~27ms on
this codebase's own dev machine (one CPU core) -- see
docs/HF_JOBS_STRATEGY_SWEEP_MIGRATION.md's own cost table for the full
combos/sec-per-core -> total-combos-per-run-per-flavor math this script's
--max-seconds/--n-workers defaults are chosen from. "Millions of
combinations" is a real, reachable number given a big-CPU flavor and a
multi-hour run -- not a promise this script or strategy_sweep.py ever
fabricates; see each run's own real `elapsed_sec`/`combinations_evaluated`
in its result for what actually happened.

Usage:
    python scripts/strategy_sweep_job.py --market kalshi_15m
    python scripts/strategy_sweep_job.py --market kalshi_15m_metals --n-workers 30 --max-seconds 10800
    python scripts/strategy_sweep_job.py --market perps --dry-run   # print grid size, don't run or push
"""
from __future__ import annotations

import argparse
import json
import logging
import multiprocessing
import os
import sys
from pathlib import Path
from typing import Any, Callable

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("strategy_sweep_job")


def _load_dotenv() -> None:
    """Minimal .env loader (mirrors training_job.py's/threads_content_job.py's
    own) so this script works standalone for a local/manual run. On Hugging
    Face Jobs itself, real secrets come from the job's own configured
    `env`/`secrets` instead -- this is a no-op there since no .env file
    exists in that container."""
    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        return
    lines = env_path.read_text(encoding="utf-8").splitlines()
    for raw in lines:
        raw = raw.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key, value = key.strip(), value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _kalshi_15m_grid() -> dict[str, list[float | int]]:
    """40 confidence floors x 20 yes-side surcharges x 17 assumed entry
    prices (a real sensitivity sweep across this market's own disclosed
    pricing-assumption limitation) x 8 position sizes x 5 concurrency caps
    = 544,000 combinations -- the same 5 dimensions
    app_kalshi._default_kalshi_15m_strategy_sweep_grid already swept at
    16,200, widened here for a real dedicated job's much bigger time/CPU
    budget instead of the live Space's own 30-minute background-thread cap."""
    return {
        "model_confidence_min": [round(0.50 + 0.35 * i / 39, 4) for i in range(40)],
        "yes_confidence_extra_required": [round(0.10 * i / 19, 4) for i in range(20)],
        "assumed_entry_price": [round(0.10 + 0.05 * i, 2) for i in range(17)],
        "position_size_pct": [0.01, 0.02, 0.03, 0.05, 0.07, 0.08, 0.10, 0.12],
        "max_concurrent_positions": [1, 2, 3, 4, 5],
    }


def _stocks_grid() -> dict[str, list[float | int]]:
    """30 confidence floors x 15 entry-dip levels x 8 volume-z floors x 8
    position sizes x 5 concurrency caps = 144,000 combinations."""
    return {
        "model_confidence_min": [round(0.50 + 0.30 * i / 29, 4) for i in range(30)],
        "entry_dip_pct": [round(0.005 + 0.045 * i / 14, 4) for i in range(15)],
        "min_volume_z": [round(-1.0 + 0.5 * i, 2) for i in range(8)],
        "position_size_pct": [0.02, 0.04, 0.06, 0.08, 0.10, 0.12, 0.15, 0.20],
        "max_concurrent_positions": [1, 2, 3, 4, 5],
    }


def _crypto_grid() -> dict[str, list[float | int]]:
    """30 confidence floors x 12 take-profit levels x 12 stop-loss levels x
    8 position sizes x 5 concurrency caps = 172,800 combinations."""
    return {
        "model_confidence_min": [round(0.50 + 0.30 * i / 29, 4) for i in range(30)],
        "take_profit_pct": [round(0.005 + 0.045 * i / 11, 4) for i in range(12)],
        "stop_loss_pct": [round(0.005 + 0.045 * i / 11, 4) for i in range(12)],
        "position_size_pct": [0.02, 0.04, 0.06, 0.08, 0.10, 0.12, 0.15, 0.20],
        "max_concurrent_positions": [1, 2, 3, 4, 5],
    }


def _options_grid() -> dict[str, list[float | int]]:
    """30 confidence floors x 12 take-profit levels x 12 stop-loss levels x
    8 position sizes x 5 concurrency caps = 172,800 combinations."""
    return {
        "model_confidence_min": [round(0.50 + 0.30 * i / 29, 4) for i in range(30)],
        "take_profit_pct": [round(0.10 + 0.90 * i / 11, 4) for i in range(12)],
        "stop_loss_pct": [round(0.10 + 0.60 * i / 11, 4) for i in range(12)],
        "position_size_pct": [0.02, 0.04, 0.06, 0.08, 0.10, 0.12, 0.15, 0.20],
        "max_concurrent_positions": [1, 2, 3, 4, 5],
    }


def _perps_grid() -> dict[str, list[float | int]]:
    """30 confidence floors x 12 entry-dip levels x 8 position sizes x 5
    concurrency caps x 2 (enable_shorts on/off) = 28,800 -- widen the
    numeric dims relative to the other markets' 5-deep grids since this
    one has a bool dimension instead of a 5th numeric one."""
    return {
        "model_confidence_min": [round(0.50 + 0.30 * i / 29, 4) for i in range(30)],
        "entry_dip_pct": [round(0.005 + 0.045 * i / 11, 4) for i in range(12)],
        "position_size_pct": [0.02, 0.04, 0.06, 0.08, 0.10, 0.12, 0.15, 0.20],
        "max_concurrent_positions": [1, 2, 3, 4, 5],
        "enable_shorts": [True, False],
    }


def _kalshi_15m_combined(days: int | None) -> Any:
    from data import kalshi_15m_backtest, kalshi_15m_data
    combined = kalshi_15m_data.load_training_dataset()
    if days and not combined.empty:
        cutoff = combined["ts"].max() - days * 86400
        combined = combined[combined["ts"] >= cutoff]
    # REAL bug this fixes: run_parameter_sweep's own combined=None auto-load
    # path calls _one_row_per_window automatically -- passing combined=
    # explicitly (needed so days/coins filtering happens exactly once,
    # here) bypasses that unless done here too. Without it, this replays
    # MULTIPLE rows per real 15-minute window (whatever sub-window
    # collection granularity the archive happens to hold), not the single
    # decision point the live strategy actually evaluates once per window
    # -- inflates trade_count and skews every downstream return/win-rate
    # number. Confirmed live: a real sweep run against un-windowed data
    # took 8x longer AND operated on rows the live strategy would never
    # have separately acted on.
    if not combined.empty:
        combined = kalshi_15m_backtest._one_row_per_window(combined)  # noqa: SLF001
    return combined


def _kalshi_15m_metals_combined(days: int | None) -> Any:
    from data import kalshi_15m_metals_backtest, kalshi_15m_metals_data
    combined = kalshi_15m_metals_data.load_training_dataset()
    if days and not combined.empty:
        cutoff = combined["ts"].max() - days * 86400
        combined = combined[combined["ts"] >= cutoff]
    if not combined.empty:  # see _kalshi_15m_combined's own comment on why this step is required here
        combined = kalshi_15m_metals_backtest._one_row_per_window(combined)  # noqa: SLF001
    return combined


def _stocks_combined(days: int | None) -> Any:
    from data.alpaca_data import load_training_dataset
    return load_training_dataset()


def _options_combined(days: int | None) -> Any:
    from data.alpaca_options_data import ensure_options_feature_columns, load_training_dataset
    return ensure_options_feature_columns(load_training_dataset())


def _crypto_combined(days: int | None) -> Any:
    import pandas as pd
    from data.alpaca_crypto_backtest import build_pair_frame
    from data.alpaca_crypto_data import get_crypto_universe
    universe = get_crypto_universe()
    frames = [build_pair_frame(s, days=days or 30) for s in universe]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values("ts")


def _perps_combined(days: int | None) -> Any:
    import pandas as pd
    from data.perps_backtest import build_ticker_frame
    from data.perps_data import get_watchlist
    watchlist = get_watchlist()
    frames = [build_ticker_frame(t, days=days or 30) for t in watchlist]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values("ts")


def _perps_extra_kwargs(combined: Any) -> dict[str, Any]:
    from data.perps_backtest import fetch_leverage_by_ticker
    tickers = sorted(combined["ticker"].unique()) if not combined.empty else []
    return {"leverage_by_ticker": fetch_leverage_by_ticker(tickers)}


# name -> (backtest_module_path, grid_fn, combined_fn, extra_kwargs_fn | None, hf_repo_module_path, hf_repo_attr, result_filename)
MARKET_CONFIGS: dict[str, dict[str, Any]] = {
    "kalshi_15m": {
        "backtest_module": "data.kalshi_15m_backtest", "grid_fn": _kalshi_15m_grid, "combined_fn": _kalshi_15m_combined,
        "extra_kwargs_fn": None, "repo_module": "data.kalshi_15m_model", "repo_attr": "HF_KALSHI_15M_MODEL_REPO",
        "result_filename": "strategy_sweep_kalshi_15m.json",
    },
    "kalshi_15m_metals": {
        "backtest_module": "data.kalshi_15m_metals_backtest", "grid_fn": _kalshi_15m_grid, "combined_fn": _kalshi_15m_metals_combined,
        "extra_kwargs_fn": None, "repo_module": "data.kalshi_15m_metals_model", "repo_attr": "HF_KALSHI_15M_METALS_MODEL_REPO",
        "result_filename": "strategy_sweep_kalshi_15m_metals.json",
    },
    "stocks": {
        "backtest_module": "data.alpaca_backtest", "grid_fn": _stocks_grid, "combined_fn": _stocks_combined,
        "extra_kwargs_fn": None, "repo_module": "data.alpaca_model", "repo_attr": "HF_ALPACA_MODEL_REPO",
        "result_filename": "strategy_sweep_stocks.json",
    },
    "crypto": {
        "backtest_module": "data.alpaca_crypto_backtest", "grid_fn": _crypto_grid, "combined_fn": _crypto_combined,
        "extra_kwargs_fn": None, "repo_module": "data.alpaca_crypto_model", "repo_attr": "HF_ALPACA_CRYPTO_MODEL_REPO",
        "result_filename": "strategy_sweep_crypto.json",
    },
    "options": {
        "backtest_module": "data.alpaca_options_backtest", "grid_fn": _options_grid, "combined_fn": _options_combined,
        "extra_kwargs_fn": None, "repo_module": "data.alpaca_options_model", "repo_attr": "HF_ALPACA_OPTIONS_MODEL_REPO",
        "result_filename": "strategy_sweep_options.json",
    },
    "perps": {
        "backtest_module": "data.perps_backtest", "grid_fn": _perps_grid, "combined_fn": _perps_combined,
        "extra_kwargs_fn": _perps_extra_kwargs, "repo_module": "data.perps_model", "repo_attr": "HF_MODEL_REPO",
        "result_filename": "strategy_sweep_perps.json",
    },
}


def run_market_sweep(
    market: str, *, n_workers: int, max_seconds: float, max_combinations: int,
    days: int | None, param_grid: dict[str, list[Any]] | None, dry_run: bool,
) -> dict[str, Any]:
    import importlib
    import time

    cfg = MARKET_CONFIGS[market]
    backtest_module = importlib.import_module(cfg["backtest_module"])
    grid = param_grid or cfg["grid_fn"]()

    if dry_run:
        from data import strategy_sweep
        combo_count = len(strategy_sweep.cartesian_product(grid))
        return {"ok": True, "dry_run": True, "market": market, "combinations": combo_count, "grid": grid}

    logger.info("[strategy_sweep_job] %s: building combined dataset (days=%s)...", market, days)
    combined = cfg["combined_fn"](days)
    if combined is None or combined.empty:
        return {"ok": False, "reason": "no_data", "market": market}

    extra_kwargs = cfg["extra_kwargs_fn"](combined) if cfg["extra_kwargs_fn"] else None

    from data import strategy_sweep
    logger.info(
        "[strategy_sweep_job] %s: %d rows, sweeping %d combinations across %d workers (max_seconds=%s)...",
        market, len(combined), len(strategy_sweep.cartesian_product(grid)), n_workers, max_seconds,
    )
    started = time.monotonic()
    result = strategy_sweep.run_parameter_sweep(
        backtest_module, grid, combined=combined,
        fold_bounds=strategy_sweep.DEFAULT_FOLD_BOUNDS_WITH_HOLDOUT,
        holdout_bounds=strategy_sweep.DEFAULT_HOLDOUT_BOUNDS,
        n_workers=n_workers, max_seconds=max_seconds, max_combinations=max_combinations,
        extra_simulate_kwargs=extra_kwargs,
    )
    result["market"] = market
    result["wall_clock_sec"] = round(time.monotonic() - started, 2)
    logger.info(
        "[strategy_sweep_job] %s: done in %.1fs -- %d/%d combinations evaluated, %d with evidence, stopped_early=%s",
        market, result["wall_clock_sec"], result.get("combinations_evaluated", 0), result.get("combinations_tried", 0),
        result.get("combinations_with_evidence", 0), result.get("stopped_early"),
    )

    if result.get("ok"):
        _publish_result(market, result)
    return result


def _publish_result(market: str, result: dict[str, Any]) -> None:
    """Pushes the real result to that market's own existing HF model repo
    (server_common.push_json_to_hf -- the SAME generic small-JSON-to-HF
    helper this codebase already uses for exactly this "a job that no
    longer runs on this Render service at all" case) so every live
    server's own status/sweep route can read it straight back
    (pull_json_from_hf) without this script or the live trading process
    ever needing to share compute again. Best-effort: a failed push here
    is logged, never raised -- the caller already has the real, computed
    result either way (printed to this job's own stdout/logs)."""
    import importlib

    from server_common import push_json_to_hf

    cfg = MARKET_CONFIGS[market]
    repo_module = importlib.import_module(cfg["repo_module"])
    repo_id = getattr(repo_module, cfg["repo_attr"])
    token = os.getenv("HF_API_KEY", "")
    if not token:
        logger.warning("[strategy_sweep_job] HF_API_KEY not set -- result computed but NOT published to HF")
        return
    push_json_to_hf(
        repo_id, cfg["result_filename"], result, token=token, timeout_sec=60.0,
        commit_message=f"strategy sweep: {market} ({result.get('combinations_with_evidence', 0)} combos with evidence)",
    )
    logger.info("[strategy_sweep_job] %s: published to %s/%s", market, repo_id, cfg["result_filename"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--market", required=True, choices=sorted(MARKET_CONFIGS.keys()))
    parser.add_argument("--n-workers", type=int, default=max(1, (os.cpu_count() or 2) - 2), help="Default: all CPUs minus 2, leaving headroom for the OS/orchestration.")
    parser.add_argument("--max-seconds", type=float, default=3600.0 * 2, help="Hard wall-clock cap (default 2 hours).")
    parser.add_argument("--max-combinations", type=int, default=2_000_000)
    parser.add_argument("--days", type=int, default=None, help="Limit to the most recent N days of archive; default uses the full archive.")
    parser.add_argument("--param-grid", type=str, default=None, help="JSON-encoded param grid override; default uses this market's own built-in grid.")
    parser.add_argument("--dry-run", action="store_true", help="Print the grid size and exit -- no data load, no sweep, no HF push.")
    args = parser.parse_args()

    _load_dotenv()
    param_grid = json.loads(args.param_grid) if args.param_grid else None
    try:
        result = run_market_sweep(
            args.market, n_workers=args.n_workers, max_seconds=args.max_seconds,
            max_combinations=args.max_combinations, days=args.days, param_grid=param_grid, dry_run=args.dry_run,
        )
    except Exception as exc:  # defense in depth -- a real HF Job's exit code is the only signal anyone watching sees
        logger.exception("[strategy_sweep_job] %s raised unexpectedly", args.market)
        result = {"ok": False, "error": str(exc), "market": args.market}
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("ok", True) else 1


if __name__ == "__main__":
    # Real, not cosmetic: ProcessPoolExecutor workers need a start method
    # that works identically whether this runs as a plain `python` process
    # (any OS) or inside an HF Job's own Linux container -- "fork" avoids
    # re-importing/re-running this whole script's own argparse/__main__
    # block in every worker (which "spawn" would do), and is available on
    # every POSIX platform this ever actually runs on (HF Jobs = Linux;
    # local dev = Linux or macOS). Set once, before any pool is created.
    if sys.platform != "win32":
        multiprocessing.set_start_method("fork", force=True)
    raise SystemExit(main())
