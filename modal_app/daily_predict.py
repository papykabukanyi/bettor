from __future__ import annotations

import datetime as dt
import json

import joblib
import pandas as pd

from modal_app import common
from modal_app.polymarket_submit import auto_submit_predictions

app = common.make_app("bettor-daily-predict")


def _load_model_bundle() -> dict:
    bundle = joblib.load(common.path_for("models", "best_model.joblib"))
    if not isinstance(bundle, dict) or "pipeline" not in bundle:
        raise RuntimeError("Model artifact is missing pipeline metadata.")
    return bundle


def _prediction_row(game: dict, pipeline, metadata: dict) -> dict:
    game_date = str(game.get("game_date") or common.today_et().isoformat())
    game_dt = pd.to_datetime(game_date, errors="coerce")
    month = int(game_dt.month if not pd.isna(game_dt) else common.today_et().month)
    day_of_week = int(game_dt.dayofweek if not pd.isna(game_dt) else common.today_et().weekday())
    features = pd.DataFrame(
        [
            {
                "home_team": str(game.get("home_team") or "").strip(),
                "away_team": str(game.get("away_team") or "").strip(),
                "sport": str(game.get("sport") or "mlb").strip().lower(),
                "league": str(game.get("league") or "").strip(),
                "season": int(str(game_date[:4] or common.today_et().year)),
                "month": month,
                "day_of_week": day_of_week,
            }
        ]
    )
    probabilities = pipeline.predict_proba(features)[0]
    home_prob = float(probabilities[1])
    away_prob = float(probabilities[0])
    predicted_team = str(game.get("home_team") or "") if home_prob >= away_prob else str(game.get("away_team") or "")
    confidence = max(home_prob, away_prob)
    return {
        "prediction_id": f"{game.get('sport', 'sport')}::{game.get('game_id', '')}::{game_date}",
        "game_id": game.get("game_id"),
        "sport": game.get("sport"),
        "league": game.get("league"),
        "home_team": game.get("home_team"),
        "away_team": game.get("away_team"),
        "game_date": game_date,
        "game_time": game.get("game_time"),
        "scheduled_start": game.get("scheduled_start") or game.get("game_time"),
        "home_win_prob": round(home_prob, 4),
        "away_win_prob": round(away_prob, 4),
        "confidence": round(confidence, 4),
        "confidence_tier": common.confidence_tier(confidence),
        "predicted_team": predicted_team,
        "prediction_breakdown": {
            "home": round(home_prob, 4),
            "away": round(away_prob, 4),
            "spread_from_coin_flip": round(abs(home_prob - 0.5), 4),
        },
        "model_version": metadata.get("version", ""),
        "model_name": metadata.get("best_model", ""),
        "model_score": metadata.get("best_score"),
        "predicted_at": common.now_utc_iso(),
    }


def run_daily_predict(*, dry_run: bool | None = None) -> dict:
    common.ensure_directories()
    bundle = _load_model_bundle()
    pipeline = bundle["pipeline"]
    metadata = bundle.get("metadata") or {}

    schedule_payload = common.load_schedule_payload()
    today = common.today_et()
    tomorrow = today + dt.timedelta(days=1)
    today_games = schedule_payload.get("today_games") if str(schedule_payload.get("today") or "") == today.isoformat() else None
    tomorrow_games = schedule_payload.get("tomorrow_games") if str(schedule_payload.get("tomorrow") or "") == tomorrow.isoformat() else None
    if not isinstance(today_games, list):
        today_games = common.fetch_upcoming_games(today)
    if not isinstance(tomorrow_games, list):
        tomorrow_games = common.fetch_upcoming_games(tomorrow)

    predictions = [_prediction_row(game, pipeline, metadata) for game in [*today_games, *tomorrow_games] if isinstance(game, dict)]
    submission_payload = auto_submit_predictions(predictions, dry_run=dry_run)
    submission_lookup = submission_payload.get("latest_by_prediction") if isinstance(submission_payload, dict) else {}
    submission_lookup = submission_lookup if isinstance(submission_lookup, dict) else {}
    for prediction in predictions:
        submission = submission_lookup.get(str(prediction.get("prediction_id") or ""), {})
        prediction["polymarket"] = {
            "status": submission.get("status") or "pending",
            "market_slug": submission.get("market_slug") or "",
            "side": submission.get("side") or "",
            "amount_usd": submission.get("amount_usd") or 0,
            "price": submission.get("price"),
            "order_id": submission.get("order_id") or "",
            "reason": submission.get("reason") or "",
        }

    payload = {
        "ok": True,
        "generated_at": common.now_utc_iso(),
        "today": today.isoformat(),
        "tomorrow": tomorrow.isoformat(),
        "prediction_count": len(predictions),
        "model_version": metadata.get("version", ""),
        "model_name": metadata.get("best_model", ""),
        "model_score": metadata.get("best_score"),
        "predictions": predictions,
        "submission_summary": submission_payload.get("summary", {}),
    }
    common.save_predictions_payload(payload)
    summary = {
        "ok": True,
        "generated_at": payload["generated_at"],
        "today": payload["today"],
        "tomorrow": payload["tomorrow"],
        "prediction_count": payload["prediction_count"],
        "model_version": payload["model_version"],
        "model_name": payload["model_name"],
        "submission_summary": payload["submission_summary"],
    }
    common.save_json(common.path_for("pipeline", "predict_summary.json"), summary)
    common.update_pipeline_status("predict", summary)
    return payload


@app.function(
    image=common.image,
    volumes={common.REMOTE_VOLUME_MOUNT: common.volume},
    secrets=common.modal_secrets(),
    schedule=common.modal.Cron("0 4 * * *", timezone="America/New_York"),
    timeout=60 * 30,
)
def daily_predict() -> dict:
    result = run_daily_predict()
    common.commit_volume()
    return result


@app.local_entrypoint()
def main(dry_run: bool = True):
    print(json.dumps(run_daily_predict(dry_run=dry_run), indent=2))
