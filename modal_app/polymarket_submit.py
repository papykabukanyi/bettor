from __future__ import annotations

import json
import os
from typing import Any

from modal_app import common

MIN_VALUE_EDGE = float(os.getenv("MIN_VALUE_EDGE", "0.05") or "0.05")
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.25") or "0.25")
BANKROLL = float(os.getenv("BANKROLL", "1000") or "1000")
MIN_BET_USD = float(os.getenv("POLYMARKET_MIN_BET_USD", "1") or "1")
MAX_BET_USD = float(os.getenv("POLYMARKET_MAX_BET_USD", "25") or "25")
SLIPPAGE_BUFFER = float(os.getenv("POLYMARKET_SLIPPAGE_BUFFER", "0.02") or "0.02")
DRY_RUN_DEFAULT = str(os.getenv("POLYMARKET_DRY_RUN", "true") or "true").strip().lower() in {"1", "true", "yes", "on"}
ACTIVE_STATUSES = {"submitted", "filled", "pending", "dry_run"}


def _prediction_to_market_candidate(prediction: dict[str, Any]) -> dict[str, Any]:
    home_prob = float(prediction.get("home_win_prob") or 0.5)
    away_prob = float(prediction.get("away_win_prob") or 0.5)
    pick = str(prediction.get("home_team") or "") if home_prob >= away_prob else str(prediction.get("away_team") or "")
    return {
        "uid": str(prediction.get("prediction_id") or ""),
        "prediction_uid": str(prediction.get("prediction_id") or ""),
        "kind": "single",
        "sport": str(prediction.get("sport") or "").strip().lower(),
        "bet_type": "moneyline",
        "pick": pick,
        "label": f"{pick} moneyline",
        "game": f"{prediction.get('away_team', 'Away')} @ {prediction.get('home_team', 'Home')}",
        "home_team": prediction.get("home_team"),
        "away_team": prediction.get("away_team"),
        "game_date": prediction.get("game_date"),
        "game_time": prediction.get("game_time"),
        "scheduled_start": prediction.get("scheduled_start") or prediction.get("game_time"),
        "model_prob": max(home_prob, away_prob),
        "confidence": float(prediction.get("confidence") or max(home_prob, away_prob)),
        "model_version": prediction.get("model_version"),
        "model_type": prediction.get("model_name") or prediction.get("model_type"),
    }


def _kelly_size(probability: float, market_price: float) -> float:
    market_price = max(0.01, min(float(market_price or 0.5), 0.99))
    edge = float(probability or 0.0) - market_price
    if edge <= 0:
        return 0.0
    raw_fraction = edge / max(1e-6, 1.0 - market_price)
    wager = BANKROLL * raw_fraction * KELLY_FRACTION
    wager = max(0.0, min(wager, MAX_BET_USD))
    return round(wager, 2)


def _extract_order_id(response: dict[str, Any]) -> str:
    for key in ("id", "order_id", "orderId"):
        value = response.get(key)
        if value:
            return str(value)
    nested = response.get("response") if isinstance(response.get("response"), dict) else {}
    for key in ("id", "order_id", "orderId"):
        value = nested.get(key)
        if value:
            return str(value)
    return ""


def _normalize_position_status(entry: dict[str, Any], order_payload: dict[str, Any] | None = None) -> str:
    if order_payload:
        order = order_payload.get("order") if isinstance(order_payload.get("order"), dict) else {}
        status = str(order.get("status") or order.get("state") or order.get("orderStatus") or "").strip().lower()
        if status:
            if "fill" in status:
                return "filled"
            if "cancel" in status:
                return "cancelled"
            if "reject" in status or "fail" in status:
                return "failed"
            return status
    return str(entry.get("status") or "pending").strip().lower() or "pending"


def refresh_positions_snapshot(entries: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    existing = entries or (common.load_submissions_payload().get("submissions") or [])
    positions: list[dict[str, Any]] = []
    for entry in existing:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("status") or "").lower() not in ACTIVE_STATUSES:
            continue
        order_id = str(entry.get("order_id") or "")
        order_payload: dict[str, Any] | None = None
        if order_id and str(entry.get("status") or "").lower() != "dry_run":
            try:
                from data.polymarket import get_order

                order_payload = get_order(order_id)
            except Exception:
                order_payload = None
        status = _normalize_position_status(entry, order_payload)
        positions.append(
            {
                "prediction_id": entry.get("prediction_id"),
                "market_slug": entry.get("market_slug"),
                "pick": entry.get("pick"),
                "side": entry.get("side"),
                "status": status,
                "amount_usd": float(entry.get("amount_usd") or 0.0),
                "price": entry.get("price"),
                "probability": entry.get("probability"),
                "expected_edge": entry.get("expected_edge"),
                "submitted_at": entry.get("submitted_at"),
                "order_id": order_id,
                "pnl_estimate_usd": round(float(entry.get("expected_edge") or 0.0) * float(entry.get("amount_usd") or 0.0), 4),
            }
        )
    payload = {
        "ok": True,
        "updated_at": common.now_utc_iso(),
        "count": len(positions),
        "summary": {
            "active_positions": len(positions),
            "open_notional_usd": round(sum(float(item.get("amount_usd") or 0.0) for item in positions), 2),
            "estimated_pnl_usd": round(sum(float(item.get("pnl_estimate_usd") or 0.0) for item in positions), 4),
        },
        "positions": positions,
    }
    common.save_positions_payload(payload)
    return payload


def auto_submit_predictions(predictions: list[dict[str, Any]], *, dry_run: bool | None = None) -> dict[str, Any]:
    dry_run = DRY_RUN_DEFAULT if dry_run is None else bool(dry_run)
    common.ensure_directories()
    if not predictions:
        empty = {"ok": True, "updated_at": common.now_utc_iso(), "summary": {"evaluated": 0, "placed": 0, "dry_run": 0, "failed": 0, "skipped": 0}, "submissions": []}
        common.save_submissions_payload(empty)
        refresh_positions_snapshot([])
        return empty

    from data.polymarket import attach_polymarket_to_bets, get_balance, get_market_bbo, place_order

    previous_payload = common.load_submissions_payload()
    previous_entries = previous_payload.get("submissions") if isinstance(previous_payload, dict) else []
    previous_entries = previous_entries if isinstance(previous_entries, list) else []
    seen_predictions = {str(item.get("prediction_id") or "") for item in previous_entries if isinstance(item, dict)}

    candidates = [_prediction_to_market_candidate(prediction) for prediction in predictions if isinstance(prediction, dict)]
    enriched = attach_polymarket_to_bets(candidates, force_refresh=True)
    try:
        balance_payload = get_balance()
    except Exception as exc:
        balance_payload = {"ok": False, "error": str(exc), "buying_power_usd": BANKROLL}

    buying_power = float(balance_payload.get("buying_power_usd") or balance_payload.get("balance_usd") or BANKROLL)
    submissions: list[dict[str, Any]] = list(previous_entries)
    placed = dry_count = failed = skipped = 0
    latest_by_prediction: dict[str, dict[str, Any]] = {}

    prediction_index = {str(row.get("prediction_id") or ""): row for row in predictions if isinstance(row, dict)}
    for enriched_row in enriched:
        prediction = prediction_index.get(str(enriched_row.get("uid") or ""), {})
        prediction_id = str(prediction.get("prediction_id") or enriched_row.get("uid") or "")
        probability = float(enriched_row.get("model_prob") or prediction.get("confidence") or 0.0)
        confidence_tier = str(prediction.get("confidence_tier") or common.confidence_tier(probability))
        status = str(enriched_row.get("polymarket_status") or "unavailable").lower()
        market_slug = str(enriched_row.get("polymarket_market_slug") or "")
        side = str(enriched_row.get("polymarket_side") or "yes").lower() or "yes"
        price = enriched_row.get("polymarket_price")
        if market_slug:
            try:
                snapshot = get_market_bbo(market_slug)
                price = snapshot.get("best_ask") or snapshot.get("current_px") or price
            except Exception:
                pass
        price = float(price or 0.5)
        edge = round(probability - price, 4)
        amount = _kelly_size(probability, price)
        pick = str(enriched_row.get("pick") or prediction.get("predicted_team") or "")

        entry = {
            "prediction_id": prediction_id,
            "game": f"{prediction.get('away_team', 'Away')} @ {prediction.get('home_team', 'Home')}",
            "game_date": prediction.get("game_date"),
            "sport": prediction.get("sport"),
            "pick": pick,
            "side": side,
            "market_slug": market_slug,
            "market_status": status,
            "probability": round(probability, 4),
            "price": round(price, 4),
            "expected_edge": edge,
            "amount_usd": amount,
            "confidence_tier": confidence_tier,
            "submitted_at": common.now_utc_iso(),
            "dry_run": dry_run,
        }

        if prediction_id in seen_predictions:
            entry["status"] = "skipped"
            entry["reason"] = "already_submitted"
            skipped += 1
        elif confidence_tier.lower() not in {"elite", "solid"}:
            entry["status"] = "skipped"
            entry["reason"] = "confidence_below_solid"
            skipped += 1
        elif status != "matched" or not market_slug:
            entry["status"] = "skipped"
            entry["reason"] = "no_open_polymarket_match"
            skipped += 1
        elif edge < MIN_VALUE_EDGE:
            entry["status"] = "skipped"
            entry["reason"] = "edge_below_threshold"
            skipped += 1
        elif amount < MIN_BET_USD:
            entry["status"] = "skipped"
            entry["reason"] = "kelly_bet_below_minimum"
            skipped += 1
        elif buying_power < amount:
            entry["status"] = "skipped"
            entry["reason"] = "insufficient_buying_power"
            skipped += 1
        elif dry_run:
            entry["status"] = "dry_run"
            entry["limit_price"] = round(min(0.99, price + SLIPPAGE_BUFFER), 4)
            dry_count += 1
        else:
            try:
                response = place_order(
                    market_slug=market_slug,
                    amount_usd=amount,
                    side=side,
                    price=round(min(0.99, price + SLIPPAGE_BUFFER), 4),
                    order_type="ORDER_TYPE_LIMIT",
                )
                entry["status"] = "submitted"
                entry["order_id"] = _extract_order_id(response)
                entry["response"] = response
                buying_power = max(0.0, buying_power - amount)
                placed += 1
            except Exception as exc:
                entry["status"] = "failed"
                entry["reason"] = str(exc)
                failed += 1

        submissions.append(entry)
        latest_by_prediction[prediction_id] = entry

    summary = {
        "evaluated": len(enriched),
        "placed": placed,
        "dry_run": dry_count,
        "failed": failed,
        "skipped": skipped,
        "available_buying_power_usd": round(buying_power, 2),
    }
    payload = {
        "ok": True,
        "updated_at": common.now_utc_iso(),
        "balance": balance_payload,
        "summary": summary,
        "submissions": submissions[-500:],
        "latest_by_prediction": latest_by_prediction,
    }
    common.save_submissions_payload(payload)
    refresh_positions_snapshot(payload["submissions"])
    common.update_pipeline_status("polymarket", {"ok": True, "updated_at": common.now_utc_iso(), **summary})
    return payload


def run_submission_job() -> dict[str, Any]:
    payload = common.load_predictions_payload()
    predictions = payload.get("predictions") if isinstance(payload, dict) else []
    predictions = predictions if isinstance(predictions, list) else []
    return auto_submit_predictions(predictions)


if __name__ == "__main__":
    print(json.dumps(run_submission_job(), indent=2))
