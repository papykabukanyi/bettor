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


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except Exception:
        return float(default)



def _prediction_to_market_candidate(prediction: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(prediction, dict) or prediction.get("error"):
        return None
    home_team = str(prediction.get("home_team") or "").strip()
    away_team = str(prediction.get("away_team") or "").strip()
    if not home_team or not away_team:
        return None
    home_prob = _as_float(prediction.get("home_win_prob"), 0.5)
    away_prob = _as_float(prediction.get("away_win_prob"), 0.5)
    scope = str(prediction.get("prediction_scope") or "game_prop").strip().lower()
    market_type = str(prediction.get("market_type") or "").strip().lower()
    market_name = str(prediction.get("market_name") or "").strip()
    player_name = str(prediction.get("player_name") or "").strip()
    player_team = str(prediction.get("player_team") or "").strip()
    prop_line = prediction.get("prop_line")
    predicted_side = str(prediction.get("predicted_side") or "").strip().lower()

    if scope == "player_prop":
        pick = str(prediction.get("predicted_label") or prediction.get("predicted_outcome") or "over").strip()
        label = market_name or market_type or "player prop"
        direction = predicted_side or ("under" if "under" in pick.lower() else "over")
        return {
            "uid": str(prediction.get("prediction_id") or f"{away_team}@{home_team}:{player_name}:{market_type}"),
            "prediction_uid": str(prediction.get("prediction_id") or f"{away_team}@{home_team}:{player_name}:{market_type}"),
            "kind": "player_prop",
            "sport": str(prediction.get("sport") or "").strip().lower(),
            "bet_type": market_type or "player_prop",
            "prop_type": market_type or "player_prop",
            "line": (_as_float(prop_line, 0.0) if prop_line not in {None, ""} else None),
            "direction": direction,
            "pick": pick,
            "label": f"{player_name} {label}".strip(),
            "name": player_name,
            "player_name": player_name,
            "team": player_team or (home_team if home_prob >= away_prob else away_team),
            "game": f"{away_team} @ {home_team}",
            "home_team": home_team,
            "away_team": away_team,
            "league": str(prediction.get("league") or "").strip(),
            "market_name": market_name,
            "game_date": str(prediction.get("game_date") or "").strip(),
            "game_time": str(prediction.get("game_time") or "").strip(),
            "scheduled_start": str(prediction.get("scheduled_start") or prediction.get("game_time") or "").strip(),
            "model_prob": max(_as_float(prediction.get("over_prob"), 0.0), _as_float(prediction.get("under_prob"), 0.0), _as_float(prediction.get("confidence"), 0.0)),
            "confidence": _as_float(prediction.get("confidence"), 0.0),
            "model_version": prediction.get("model_version"),
            "model_type": prediction.get("model_type"),
        }

    pick = home_team if home_prob >= away_prob else away_team
    bet_type = "moneyline"
    if any(token in market_type for token in ("total", "over_under")):
        bet_type = "total"
    elif any(token in market_type for token in ("spread", "handicap")):
        bet_type = "spread"
    return {
        "uid": str(prediction.get("prediction_id") or f"{away_team}@{home_team}"),
        "prediction_uid": str(prediction.get("prediction_id") or f"{away_team}@{home_team}"),
        "kind": "single",
        "sport": str(prediction.get("sport") or "").strip().lower(),
        "bet_type": bet_type,
        "pick": pick,
        "label": market_name or f"{pick} {bet_type}",
        "game": f"{away_team} @ {home_team}",
        "home_team": home_team,
        "away_team": away_team,
        "league": str(prediction.get("league") or "").strip(),
        "market_name": market_name,
        "game_date": str(prediction.get("game_date") or "").strip(),
        "game_time": str(prediction.get("game_time") or "").strip(),
        "scheduled_start": str(prediction.get("scheduled_start") or prediction.get("game_time") or "").strip(),
        "model_prob": max(home_prob, away_prob),
        "confidence": _as_float(prediction.get("confidence"), max(home_prob, away_prob)),
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
        try:
            from data.kalshi import attach_kalshi_to_bets

            enriched = attach_kalshi_to_bets(enriched, force_refresh=True)
        except Exception as exc:
            kalshi_error = str(exc)
            print(f"[HF][MARKETS] Kalshi enrichment skipped: {exc}")
        by_uid = {
            str(row.get("prediction_uid") or row.get("uid") or ""): row
            for row in enriched
            if isinstance(row, dict)
        }
        updated_predictions: list[dict[str, Any]] = []
        for pred in predictions:
            row = dict(pred) if isinstance(pred, dict) else {}
            uid = str(row.get("prediction_id") or "")
            linked = by_uid.get(uid) or {}
            row["kalshi"] = {
                "status": str(linked.get("kalshi_status") or "unavailable"),
                "ticker": str(linked.get("kalshi_ticker") or ""),
                "event_ticker": str(linked.get("kalshi_event_ticker") or ""),
                "side": str(linked.get("kalshi_side") or ""),
                "price_cents": linked.get("kalshi_price_cents"),
                "series_ticker": str(linked.get("kalshi_series_ticker") or ""),
            }
            updated_predictions.append(row)
        if isinstance(payload, dict):
            payload["predictions"] = updated_predictions
            with open(predictions_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
        result = {
            "ok": True,
            "count": len(enriched),
            "kalshi_error": kalshi_error,
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
        help="Attach Kalshi market context after an HF daily run",
    )
    parser.add_argument(
        "--hf-markets-output",
        default=DEFAULT_MARKETS_OUTPUT,
        help="File path for Kalshi enrichment output",
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
