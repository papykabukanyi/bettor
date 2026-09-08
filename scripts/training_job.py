"""Runs ONE model-training/backtest job for ONE market, as a standalone
process -- no Flask, no APScheduler, no `_locked_job` (this process's own
single invocation IS the concurrency boundary, the same assumption
`scripts/threads_content_job.py` already relies on for its own job class).
Built to run as a scheduled Hugging Face Job instead of a job registered
on a Render service's own in-process APScheduler -- see
docs/HF_JOBS_TRAINING_MIGRATION.md for the full architecture, secrets
checklist, and the local-disk-to-HF migration needed before some of
these jobs (the ones that currently write a sweep/backfill/walkforward
result to LOCAL Render disk for their own /api/*/status route to read)
can safely move.

Each function below is a near-verbatim copy of the corresponding
`_run_*_train`/`_run_*_backtest_sweep`/`_run_*_torch_train` job body
already living in app_kalshi.py/alpaca_server.py/alpaca_crypto_server.py/
alpaca_options_server.py -- same logic, reimplemented against the
portable `data.*` functions directly instead of importing the Flask-
app-entangled, `@_locked_job`-decorated originals (which can't be
imported standalone without dragging in their whole Flask server
module).

IMPORTANT prerequisite this script depends on: every `*_model.py`'s
`load_model()` now periodically re-checks HF for a newer model even once
a local copy is cached (see server_common.maybe_schedule_hf_model_recheck)
-- without that fix, a model trained here (on a process separate from the
live trading service) would never be picked up by that service until its
next full restart.

Markets/jobs implemented so far: `train` for all 4 markets (none of the
plain train jobs write anything to local disk -- only the sweep/backfill/
intensive-training/walkforward jobs do, which is exactly why those are
NOT here yet). backtest-sweep/walkforward/torch-train land once each
market's own local-disk-to-HF migration (where needed) is built and
verified -- see docs/HF_JOBS_TRAINING_MIGRATION.md for the sequencing.

Usage:
    python scripts/training_job.py --job train --market perps
    python scripts/training_job.py --job train --market options
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Callable

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("training_job")


def _load_dotenv() -> None:
    """Minimal .env loader (mirrors run_perps_cycle.py's/
    threads_content_job.py's own) so this script works standalone for a
    local/manual run. On Hugging Face Jobs itself, real secrets come from
    the job's own configured environment instead -- this is a no-op there
    since no .env file exists in that container."""
    import os

    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        return
    lines = env_path.read_text(encoding="utf-8").splitlines()
    idx = 0
    while idx < len(lines):
        raw = lines[idx].strip()
        idx += 1
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key, value = key.strip(), value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _perps_train() -> dict[str, Any]:
    from data import perps_model, perps_strategy
    try:
        trade_log = perps_strategy._load_state().get("trade_log")  # noqa: SLF001
    except Exception as exc:
        logger.warning("could not read trade_log for outcome-aware training: %s", exc)
        trade_log = None
    return perps_model.train_model(trade_log=trade_log)


def _stocks_train() -> dict[str, Any]:
    from data import alpaca_model
    return alpaca_model.train_model()


def _crypto_train() -> dict[str, Any]:
    from data import alpaca_crypto_model
    return alpaca_crypto_model.train_model()


def _options_train() -> dict[str, Any]:
    # Same off-hours-only gate as the original _run_alpaca_options_train:
    # a multi-minute retrain has no business competing with live
    # entry-scan/fast-check for CPU/memory while real option orders may
    # be in flight. Kept here (not just left to the HF Job's own CRON
    # schedule) so this stays a safe no-op even if the schedule is ever
    # set to fire during regular hours by mistake.
    from data import alpaca_data, alpaca_options_model
    if alpaca_data.get_market_session()["session"] == "regular":
        return {"ok": True, "skipped": "regular_hours"}
    return alpaca_options_model.train_model()


_JOBS: dict[tuple[str, str], Callable[[], dict[str, Any]]] = {
    ("train", "perps"): _perps_train,
    ("train", "stocks"): _stocks_train,
    ("train", "crypto"): _crypto_train,
    ("train", "options"): _options_train,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--job", required=True, choices=["train", "backtest-sweep", "walkforward", "torch-train"])
    parser.add_argument("--market", required=True, choices=["perps", "stocks", "crypto", "options"])
    args = parser.parse_args()

    fn = _JOBS.get((args.job, args.market))
    if fn is None:
        result = {
            "ok": False,
            "error": f"no such job/market combination implemented yet: {args.job}/{args.market} "
                     "(see this script's own docstring for what's built so far)",
        }
        print(json.dumps(result, indent=2))
        return 1

    _load_dotenv()
    try:
        result = fn()
    except Exception as exc:  # each job function's own internals already handle their own errors -- defense in depth only
        logger.exception("job raised unexpectedly")
        result = {"ok": False, "error": str(exc)}
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("ok", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
