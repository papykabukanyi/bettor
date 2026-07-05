#!/usr/bin/env python3
"""Run the HF pipeline continuously in the background."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from data.hf_pipeline import HFDirectPipeline  # noqa: E402
from betting_bot import _attach_market_context  # noqa: E402


def _run_cycle(min_rows: int, custom_model: str, attach_markets: bool) -> dict:
    pipeline = HFDirectPipeline()
    if not pipeline.ok:
        raise RuntimeError("HF pipeline not configured. Set HF_API_KEY, HF_DATASET_REPO, and HF_MODEL_REPO.")
    result = pipeline.run_daily_pipeline(custom_model=custom_model, min_rows=min_rows)
    if attach_markets:
        _attach_market_context(
            predictions_path=str(ROOT_DIR / "data" / "hf_daily_predictions.json"),
            output_path=str(ROOT_DIR / "data" / "hf_daily_prediction_markets.json"),
        )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Continuous HF pipeline runner")
    parser.add_argument("--interval-minutes", type=int, default=30, help="Minutes between active runs")
    parser.add_argument("--min-train-rows", type=int, default=200, help="Minimum rows before retraining")
    parser.add_argument(
        "--custom-model",
        default="auto",
        choices=["auto", "gradient_boosting", "random_forest", "logistic_regression"],
        help="Model choice for retraining",
    )
    parser.add_argument("--no-attach-markets", action="store_true", help="Skip Kalshi market enrichment")
    args = parser.parse_args()

    interval_seconds = max(300, int(args.interval_minutes) * 60)
    attach_markets = not bool(args.no_attach_markets)

    while True:
        started_at = dt.datetime.now(dt.timezone.utc).isoformat()
        try:
            result = _run_cycle(args.min_train_rows, args.custom_model, attach_markets)
            print(
                json.dumps(
                    {
                        "ok": bool(result.get("ok")),
                        "started_at": started_at,
                        "completed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                        "append_records": int(((result.get("append_today") or {}).get("records") or 0))
                        + int(((result.get("append_yesterday") or {}).get("records") or 0)),
                        "prediction_count": int(((result.get("predictions") or {}).get("prediction_count") or 0)),
                    }
                ),
                flush=True,
            )
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "started_at": started_at,
                        "failed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                        "error": str(exc),
                    }
                ),
                flush=True,
            )
        time.sleep(interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
