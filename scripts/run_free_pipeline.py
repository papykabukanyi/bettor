from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from modal_app.daily_data_fetch import run_daily_data_fetch
from modal_app.daily_predict import run_daily_predict
from modal_app.daily_train import run_daily_train


def _print_block(label: str, payload: dict) -> None:
    print(f"\n=== {label} ===")
    print(json.dumps(payload, indent=2))


def main() -> None:
    bootstrap_days = int(os.getenv("MODAL_BOOTSTRAP_DAYS", "120") or "120")
    backfill_days = int(os.getenv("MODAL_DAILY_BACKFILL_DAYS", "3") or "3")
    dry_run = str(os.getenv("POLYMARKET_DRY_RUN", "true") or "true").strip().lower() not in {"0", "false", "no"}

    fetch_summary = run_daily_data_fetch(bootstrap_days=bootstrap_days, backfill_days=backfill_days)
    train_summary = run_daily_train()
    predict_summary = run_daily_predict(dry_run=dry_run)

    _print_block("fetch_summary", fetch_summary)
    _print_block("train_summary", train_summary)
    _print_block(
        "predict_summary",
        {
            "ok": bool(predict_summary.get("ok")),
            "generated_at": predict_summary.get("generated_at"),
            "prediction_count": predict_summary.get("prediction_count"),
            "submission_summary": predict_summary.get("submission_summary", {}),
        },
    )


if __name__ == "__main__":
    main()
