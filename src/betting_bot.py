#!/usr/bin/env python3
"""HF-first betting bot CLI."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

warnings_filter_applied = False
if not warnings_filter_applied:
    import warnings

    warnings.filterwarnings("ignore")
    warnings_filter_applied = True

SRC_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SRC_DIR)

from config import HF_INFERENCE_ENDPOINT, HF_INFERENCE_MODEL


DEFAULT_PREDICTIONS_OUTPUT = os.path.join("data", "hf_daily_predictions.json")
DEFAULT_MARKETS_OUTPUT = os.path.join("data", "hf_daily_prediction_markets.json")



def _load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default



def _prediction_to_market_candidate(prediction: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(prediction, dict) or prediction.get("error"):
        return None
    home_team = str(prediction.get("home_team") or "").strip()
    away_team = str(prediction.get("away_team") or "").strip()
    if not home_team or not away_team:
        return None
    home_prob = float(prediction.get("home_win_prob") or 0.5)
    away_prob = float(prediction.get("away_win_prob") or 0.5)
    pick = home_team if home_prob >= away_prob else away_team
    return {
        "uid": str(prediction.get("prediction_id") or f"{away_team}@{home_team}"),
        "prediction_uid": str(prediction.get("prediction_id") or f"{away_team}@{home_team}"),
        "kind": "single",
        "sport": str(prediction.get("sport") or "").strip().lower(),
        "bet_type": "moneyline",
        "pick": pick,
        "label": f"{pick} moneyline",
        "game": f"{away_team} @ {home_team}",
        "home_team": home_team,
        "away_team": away_team,
        "game_date": str(prediction.get("game_date") or "").strip(),
        "game_time": str(prediction.get("game_time") or "").strip(),
        "scheduled_start": str(prediction.get("scheduled_start") or "").strip(),
        "model_prob": max(home_prob, away_prob),
        "confidence": float(prediction.get("confidence") or max(home_prob, away_prob)),
        "model_version": prediction.get("model_version"),
        "model_type": prediction.get("model_type"),
    }



def _attach_market_context(predictions_path: str, output_path: str) -> dict[str, Any]:
    payload = _load_json(predictions_path, {})
    predictions = payload.get("predictions") or []
    candidates = [row for row in (_prediction_to_market_candidate(pred) for pred in predictions) if row]
    if not candidates:
        result = {"ok": True, "count": 0, "markets": []}
    else:
        enriched = list(candidates)
        kalshi_error = ""
        polymarket_error = ""
        try:
            from data.kalshi import attach_kalshi_to_bets

            enriched = attach_kalshi_to_bets(enriched)
        except Exception as exc:
            kalshi_error = str(exc)
            print(f"[HF][MARKETS] Kalshi enrichment skipped: {exc}")
        try:
            from data.polymarket import attach_polymarket_to_bets

            enriched = attach_polymarket_to_bets(enriched)
        except Exception as exc:
            polymarket_error = str(exc)
            print(f"[HF][MARKETS] Polymarket enrichment skipped: {exc}")
        result = {
            "ok": True,
            "count": len(enriched),
            "kalshi_error": kalshi_error,
            "polymarket_error": polymarket_error,
            "markets": enriched,
        }
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2)
    return result



def _run_hf_pipeline_mode(args: argparse.Namespace) -> int:
    from data.hf_pipeline import HFDirectPipeline

    pipeline = HFDirectPipeline(
        dataset_repo=args.hf_dataset_repo,
        model_repo=args.hf_model_repo,
    )
    if not pipeline.ok:
        print("[HF] Pipeline is not configured. Set HF_API_KEY and retry.")
        return 1

    print(f"[HF] Dataset repo: {pipeline.dataset_repo_id}")
    print(f"[HF] Model repo:   {pipeline.model_repo_id}")

    if args.hf_bootstrap:
        result = pipeline.bootstrap_one_year_history(days_back=args.hf_days_back)
        print("[HF][BOOTSTRAP]", json.dumps(result, indent=2))

    if args.hf_append_daily:
        result = pipeline.append_daily_results()
        print("[HF][APPEND]", json.dumps(result, indent=2))

    if args.hf_retrain_publish:
        summary = pipeline.train_and_publish_best_model(
            min_rows=args.hf_min_train_rows,
            forced_model=args.hf_custom_model,
        )
        print(
            "[HF][TRAIN] "
            f"rows={summary.rows} best={summary.best_model} "
            f"cv_roc_auc={summary.cv_roc_auc:.4f} repo={summary.repo_id}"
        )

    if args.hf_daily_run:
        result = pipeline.run_daily_pipeline(
            custom_model=args.hf_custom_model,
            min_rows=args.hf_min_train_rows,
            predictions_output_path=args.hf_predictions_output,
            via_api=bool(args.hf_predict_via_api),
            model_id=args.hf_inference_model or HF_INFERENCE_MODEL or pipeline.model_repo_id,
            endpoint_url=args.hf_endpoint_url or HF_INFERENCE_ENDPOINT or "",
        )
        print("[HF][DAILY]", json.dumps(result, indent=2))
        if args.hf_attach_markets:
            market_result = _attach_market_context(
                args.hf_predictions_output,
                args.hf_markets_output,
            )
            print("[HF][MARKETS]", json.dumps(market_result, indent=2))

    if args.hf_predict_matchup:
        home_team, away_team = args.hf_predict_matchup
        if args.hf_predict_via_api:
            result = pipeline.predict_via_hf_api(
                home_team=home_team,
                away_team=away_team,
                season=args.hf_predict_season,
                model_id=args.hf_inference_model or HF_INFERENCE_MODEL or pipeline.model_repo_id,
                endpoint_url=args.hf_endpoint_url or HF_INFERENCE_ENDPOINT or "",
            )
            print("[HF][PREDICT_API]", json.dumps(result, indent=2))
        else:
            result = pipeline.predict_from_model_repo(
                home_team=home_team,
                away_team=away_team,
                season=args.hf_predict_season,
            )
            print("[HF][PREDICT_MODEL]", json.dumps(result, indent=2))

    return 0



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="HF-first betting bot")
    parser.add_argument("--hf-bootstrap", action="store_true", help="Load historical games into the HF dataset")
    parser.add_argument("--hf-append-daily", action="store_true", help="Append completed games into the HF dataset")
    parser.add_argument("--hf-retrain-publish", action="store_true", help="Retrain and publish the best HF model")
    parser.add_argument("--hf-daily-run", action="store_true", help="Run the full HF daily pipeline")
    parser.add_argument("--hf-days-back", type=int, default=365, help="Historical lookback for HF bootstrap")
    parser.add_argument("--hf-min-train-rows", type=int, default=200, help="Minimum rows required before HF retrain")
    parser.add_argument(
        "--hf-custom-model",
        default="auto",
        choices=["auto", "gradient_boosting", "random_forest", "logistic_regression"],
        help="Model choice for HF retraining",
    )
    parser.add_argument("--hf-dataset-repo", default="", help="Override HF dataset repo name or id")
    parser.add_argument("--hf-model-repo", default="", help="Override HF model repo name or id")
    parser.add_argument(
        "--hf-predictions-output",
        default=DEFAULT_PREDICTIONS_OUTPUT,
        help="File path for generated HF predictions JSON",
    )
    parser.add_argument(
        "--hf-predict-matchup",
        nargs=2,
        metavar=("HOME_TEAM", "AWAY_TEAM"),
        help="Predict one matchup from the HF model",
    )
    parser.add_argument("--hf-predict-season", type=int, default=0, help="Season/year feature for HF matchup prediction")
    parser.add_argument("--hf-predict-via-api", action="store_true", help="Use the HF inference API instead of model download")
    parser.add_argument("--hf-endpoint-url", default="", help="Custom HF inference endpoint URL")
    parser.add_argument("--hf-inference-model", default="", help="HF model id for API inference calls")
    parser.add_argument(
        "--hf-attach-markets",
        action="store_true",
        help="Attach Kalshi and Polymarket market context after an HF daily run",
    )
    parser.add_argument(
        "--hf-markets-output",
        default=DEFAULT_MARKETS_OUTPUT,
        help="File path for Kalshi and Polymarket enrichment output",
    )
    return parser



def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    requested_modes = any(
        [
            args.hf_bootstrap,
            args.hf_append_daily,
            args.hf_retrain_publish,
            args.hf_daily_run,
            bool(args.hf_predict_matchup),
        ]
    )
    if not requested_modes:
        args.hf_daily_run = True

    print("=" * 60)
    print("  HF-FIRST BETTING BOT")
    print("=" * 60)
    return _run_hf_pipeline_mode(args)


if __name__ == "__main__":
    raise SystemExit(main())
