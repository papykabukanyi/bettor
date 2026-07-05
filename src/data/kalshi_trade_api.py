"""Kalshi Trade API client (docs-based).

Implements authenticated request signing exactly as described in:
https://docs.kalshi.com/getting_started/quick_start_authenticated_requests
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import os
import uuid
from typing import Any
from urllib.parse import urlencode, urlparse

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

KALSHI_BASE_URL = str(
    os.getenv("KALSHI_BASE_URL", "https://external-api.kalshi.com/trade-api/v2")
).rstrip("/")
KALSHI_TIMEOUT_SEC = int(os.getenv("KALSHI_TIMEOUT_SEC", "15") or "15")
_BASE_PATH = urlparse(KALSHI_BASE_URL).path.rstrip("/")


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _load_private_key_pem() -> bytes:
    inline = str(os.getenv("KALSHI_PRIVATE_KEY", "") or "").strip()
    if inline:
        return inline.replace("\\n", "\n").encode("utf-8")
    raise RuntimeError("Kalshi private key missing. Set KALSHI_PRIVATE_KEY.")


def _load_private_key():
    pem = _load_private_key_pem()
    return serialization.load_pem_private_key(pem, password=None)


def _signed_headers(method: str, sign_path: str) -> dict[str, str]:
    api_key = str(os.getenv("KALSHI_API_KEY", "") or "").strip()
    if not api_key:
        raise RuntimeError("Kalshi API key missing. Set KALSHI_API_KEY.")
    ts_ms = str(int(dt.datetime.now(tz=dt.timezone.utc).timestamp() * 1000))
    key = _load_private_key()
    message = f"{ts_ms}{method.upper()}{sign_path.split('?', 1)[0]}".encode("utf-8")
    signature = key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("ascii"),
    }


def _request_json(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
    auth: bool = False,
) -> dict[str, Any]:
    clean_path = "/" + path.lstrip("/")
    url = f"{KALSHI_BASE_URL}{clean_path}"
    headers: dict[str, str] = {}
    if auth:
        sign_path = f"{_BASE_PATH}{clean_path}"
        if params:
            sign_path = f"{sign_path}?{urlencode(params, doseq=True)}"
        headers.update(_signed_headers(method, sign_path))
    if payload is not None:
        headers["Content-Type"] = "application/json"
    response = requests.request(
        method=method.upper(),
        url=url,
        params=params,
        data=json.dumps(payload) if payload is not None else None,
        headers=headers,
        timeout=KALSHI_TIMEOUT_SEC,
    )
    if response.status_code >= 400:
        body = response.text[:400]
        raise RuntimeError(f"Kalshi API error {response.status_code}: {body}")
    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError("Kalshi API returned non-object JSON payload.")
    return data


def get_exchange_status() -> dict[str, Any]:
    return _request_json("GET", "/exchange/status", auth=False)


def get_balance(subaccount: int | None = None) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if subaccount is not None:
        params["subaccount"] = int(subaccount)
    return _request_json("GET", "/portfolio/balance", params=params, auth=True)


def get_orders(
    *,
    status: str | None = None,
    limit: int = 100,
    cursor: str | None = None,
    ticker: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": max(1, min(int(limit), 1000))}
    if status:
        params["status"] = str(status)
    if cursor:
        params["cursor"] = str(cursor)
    if ticker:
        params["ticker"] = str(ticker)
    return _request_json("GET", "/portfolio/orders", params=params, auth=True)


def get_open_orders(*, max_pages: int = 10, page_limit: int = 200) -> list[dict[str, Any]]:
    orders: list[dict[str, Any]] = []
    cursor = ""
    for _ in range(max(1, int(max_pages))):
        payload = get_orders(status="resting", limit=page_limit, cursor=cursor or None)
        page_rows = payload.get("orders") or []
        for row in page_rows:
            if isinstance(row, dict):
                orders.append(row)
        cursor = str(payload.get("cursor") or "").strip()
        if not cursor:
            break
    return orders


def create_order_v2(order_payload: dict[str, Any]) -> dict[str, Any]:
    """Place an authenticated order via Kalshi V2 endpoint."""
    return _request_json("POST", "/portfolio/events/orders", payload=order_payload, auth=True)


def _book_side_for_outcome(outcome_side: str) -> str:
    side = str(outcome_side or "").strip().lower()
    return "bid" if side == "yes" else "ask"


def _v2_yes_leg_price(outcome_side: str, outcome_price_cents: int) -> float:
    side = str(outcome_side or "").strip().lower()
    raw = max(1, min(99, int(outcome_price_cents or 0)))
    if side == "yes":
        return raw / 100.0
    return max(0.01, min(0.99, (100 - raw) / 100.0))


def _count_for_target_notional(stake_usd: float, outcome_price_cents: int) -> float:
    price = max(0.01, min(0.99, float(int(outcome_price_cents or 0)) / 100.0))
    contracts = round(float(stake_usd) / price, 2)
    return max(0.01, contracts)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def build_order_candidates_from_predictions(
    predictions_payload: dict[str, Any],
    *,
    force_refresh: bool = True,
) -> list[dict[str, Any]]:
    """Resolve Kalshi market matches for prediction rows and return matched candidates."""
    from betting_bot import _prediction_to_market_candidate
    from data.kalshi import attach_kalshi_to_bets

    predictions = (predictions_payload or {}).get("predictions") or []
    if not isinstance(predictions, list):
        return []
    candidates = [row for row in (_prediction_to_market_candidate(p) for p in predictions) if row]
    if not candidates:
        return []
    enriched = attach_kalshi_to_bets(candidates, force_refresh=force_refresh)
    matched: list[dict[str, Any]] = []
    by_uid = {
        str(p.get("prediction_id") or ""): p
        for p in predictions
        if isinstance(p, dict)
    }
    for row in enriched:
        if not isinstance(row, dict):
            continue
        if str(row.get("kalshi_status") or "").strip().lower() != "matched":
            continue
        ticker = str(row.get("kalshi_ticker") or "").strip()
        side = str(row.get("kalshi_side") or "").strip().lower()
        price_cents = int(_as_float(row.get("kalshi_price_cents"), 0))
        uid = str(row.get("prediction_uid") or row.get("uid") or "").strip()
        if not ticker or side not in {"yes", "no"} or price_cents <= 0:
            continue
        original = by_uid.get(uid) or {}
        matched.append(
            {
                "prediction_uid": uid,
                "prediction": original,
                "market": row,
                "ticker": ticker,
                "outcome_side": side,
                "outcome_price_cents": price_cents,
                "confidence": _as_float(original.get("confidence"), _as_float(row.get("confidence"), 0.0)),
            }
        )
    matched.sort(key=lambda x: x.get("confidence") or 0.0, reverse=True)
    return matched


def submit_prediction_orders(
    predictions_payload: dict[str, Any],
    *,
    stake_usd: float = 1.0,
    max_orders: int = 1,
    dry_run: bool = True,
    include_combos: bool = False,
    max_combos: int = 0,
) -> dict[str, Any]:
    """Submit Kalshi orders for matched predictions with a fixed USD stake per order."""
    target_stake = max(0.01, float(stake_usd))
    limit = max(1, int(max_orders))
    candidates = build_order_candidates_from_predictions(predictions_payload, force_refresh=True)
    selected = candidates[:limit]
    submitted: list[dict[str, Any]] = []
    for cand in selected:
        side = str(cand.get("outcome_side") or "yes")
        price_cents = int(cand.get("outcome_price_cents") or 0)
        ticker = str(cand.get("ticker") or "")
        count = _count_for_target_notional(target_stake, price_cents)
        payload = {
            "ticker": ticker,
            "client_order_id": str(uuid.uuid4()),
            "side": _book_side_for_outcome(side),
            "count": f"{count:.2f}",
            "price": f"{_v2_yes_leg_price(side, price_cents):.4f}",
            "time_in_force": "good_till_canceled",
            "self_trade_prevention_type": "taker_at_cross",
            "post_only": False,
            "cancel_order_on_pause": False,
            "reduce_only": False,
            "exchange_index": 0,
        }
        base_result = {
            "prediction_uid": cand.get("prediction_uid") or "",
            "ticker": ticker,
            "outcome_side": side,
            "outcome_price_cents": price_cents,
            "book_side": payload["side"],
            "order_price_dollars": payload["price"],
            "count": payload["count"],
            "target_stake_usd": round(target_stake, 2),
            "confidence": cand.get("confidence") or 0.0,
            "prediction": cand.get("prediction") or {},
            "market": cand.get("market") or {},
            "dry_run": bool(dry_run),
        }
        if dry_run:
            base_result["preview_order_payload"] = payload
            submitted.append(base_result)
            continue
        order_result = create_order_v2(payload)
        base_result["order_response"] = order_result
        submitted.append(base_result)
    result: dict[str, Any] = {
        "ok": True,
        "updated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "dry_run": bool(dry_run),
        "stake_usd": round(target_stake, 2),
        "max_orders": limit,
        "matched_count": len(candidates),
        "selected_count": len(selected),
        "submitted_count": len(submitted),
        "submitted": submitted,
    }
    if include_combos and int(max_combos or 0) > 0:
        combo_suggestions = build_combo_suggestions_from_predictions(
            predictions_payload,
            max_combos=max(1, int(max_combos) * 5),
        )
        combo_orders = submit_combo_orders(
            combo_suggestions,
            stake_usd=target_stake,
            max_orders=max(1, int(max_combos)),
            dry_run=dry_run,
        )
        result["combo_enabled"] = True
        result["combo_orders"] = combo_orders
    else:
        result["combo_enabled"] = False
        result["combo_orders"] = {
            "ok": True,
            "updated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "dry_run": bool(dry_run),
            "suggested_count": 0,
            "matched_count": 0,
            "selected_count": 0,
            "submitted_count": 0,
            "submitted": [],
        }
    return result


def build_combo_suggestions_from_predictions(
    predictions_payload: dict[str, Any],
    *,
    max_combos: int = 20,
) -> list[dict[str, Any]]:
    """Build and resolve combo suggestions derived from current predictions."""
    from betting_bot import _prediction_to_market_candidate
    from data.kalshi import resolve_ready_bets, suggest_combo_bets

    predictions = (predictions_payload or {}).get("predictions") or []
    if not isinstance(predictions, list):
        return []
    candidates = [row for row in (_prediction_to_market_candidate(p) for p in predictions) if row]
    if not candidates:
        return []

    for row in candidates:
        prob = max(0.01, min(0.99, _as_float(row.get("model_prob"), _as_float(row.get("confidence"), 0.5))))
        row["model_prob"] = prob
        row["dec_odds"] = round(1.0 / prob, 3)

    single_res = resolve_ready_bets(candidates, force_refresh=True)
    combo_suggestions = suggest_combo_bets(
        candidates,
        resolutions=(single_res.get("resolutions") or {}),
        max_combos=max(1, int(max_combos)),
    )
    if not combo_suggestions:
        return []
    combo_res = resolve_ready_bets(combo_suggestions, force_refresh=False)
    by_uid = combo_res.get("resolutions") or {}
    enriched: list[dict[str, Any]] = []
    for combo in combo_suggestions:
        uid = str(combo.get("uid") or "")
        res = by_uid.get(uid) or {}
        enriched.append(
            {
                **combo,
                "kalshi_status": str(res.get("status") or "unavailable"),
                "kalshi_message": str(res.get("message") or ""),
                "kalshi_ticker": str(res.get("market_ticker") or ""),
                "kalshi_side": str(res.get("side") or "yes").lower(),
                "kalshi_price_cents": int(_as_float(res.get("price_cents"), 0)),
            }
        )
    return enriched


def submit_combo_orders(
    combo_suggestions: list[dict[str, Any]],
    *,
    stake_usd: float = 1.0,
    max_orders: int = 1,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Submit Kalshi combo orders for matched combo suggestions."""
    target_stake = max(0.01, float(stake_usd))
    limit = max(1, int(max_orders))
    matched = [
        combo
        for combo in (combo_suggestions or [])
        if isinstance(combo, dict)
        and str(combo.get("kalshi_status") or "").strip().lower() == "matched"
        and str(combo.get("kalshi_ticker") or "").strip()
        and int(_as_float(combo.get("kalshi_price_cents"), 0)) > 0
    ]
    selected = matched[:limit]
    submitted: list[dict[str, Any]] = []
    for combo in selected:
        side = str(combo.get("kalshi_side") or "yes").strip().lower()
        price_cents = int(_as_float(combo.get("kalshi_price_cents"), 0))
        ticker = str(combo.get("kalshi_ticker") or "").strip()
        count = _count_for_target_notional(target_stake, price_cents)
        payload = {
            "ticker": ticker,
            "client_order_id": str(uuid.uuid4()),
            "side": _book_side_for_outcome(side),
            "count": f"{count:.2f}",
            "price": f"{_v2_yes_leg_price(side, price_cents):.4f}",
            "time_in_force": "good_till_canceled",
            "self_trade_prevention_type": "taker_at_cross",
            "post_only": False,
            "cancel_order_on_pause": False,
            "reduce_only": False,
            "exchange_index": 0,
        }
        result = {
            "combo_uid": str(combo.get("uid") or ""),
            "label": str(combo.get("label") or ""),
            "ticker": ticker,
            "outcome_side": side,
            "outcome_price_cents": price_cents,
            "book_side": payload["side"],
            "order_price_dollars": payload["price"],
            "count": payload["count"],
            "target_stake_usd": round(target_stake, 2),
            "dry_run": bool(dry_run),
            "combo": combo,
        }
        if dry_run:
            result["preview_order_payload"] = payload
            submitted.append(result)
            continue
        order_result = create_order_v2(payload)
        result["order_response"] = order_result
        submitted.append(result)
    return {
        "ok": True,
        "updated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "dry_run": bool(dry_run),
        "stake_usd": round(target_stake, 2),
        "max_orders": limit,
        "suggested_count": len(combo_suggestions or []),
        "matched_count": len(matched),
        "selected_count": len(selected),
        "submitted_count": len(submitted),
        "submitted": submitted,
    }


def build_live_snapshot() -> dict[str, Any]:
    exchange: dict[str, Any] = {}
    balance: dict[str, Any] = {}
    open_orders: list[dict[str, Any]] = []
    errors: list[str] = []

    try:
        exchange = get_exchange_status()
    except Exception as exc:
        errors.append(f"exchange: {exc}")

    try:
        balance = get_balance()
    except Exception as exc:
        errors.append(f"balance: {exc}")

    try:
        open_orders = get_open_orders()
    except Exception as exc:
        errors.append(f"orders: {exc}")

    notional = 0.0
    normalized_orders: list[dict[str, Any]] = []
    for order in open_orders:
        remaining = _to_float(order.get("remaining_count_fp"), 0.0)
        yes_price = _to_float(order.get("yes_price_dollars"), 0.0)
        no_price = _to_float(order.get("no_price_dollars"), 0.0)
        side = str(order.get("outcome_side") or order.get("side") or "").strip().lower()
        price = no_price if side == "no" and no_price > 0 else yes_price
        notional += remaining * max(0.0, price)
        normalized_orders.append(
            {
                "order_id": str(order.get("order_id") or ""),
                "ticker": str(order.get("ticker") or ""),
                "status": str(order.get("status") or ""),
                "side": side,
                "yes_price_dollars": yes_price,
                "no_price_dollars": no_price,
                "remaining_count_fp": str(order.get("remaining_count_fp") or "0"),
                "created_time": order.get("created_time") or "",
                "last_update_time": order.get("last_update_time") or "",
            }
        )

    balance_cents = int(_to_float(balance.get("balance") or balance.get("balance_cents") or balance.get("cash_balance"), 0.0))
    portfolio_cents = int(_to_float(balance.get("portfolio_value") or balance.get("portfolio_value_cents"), 0.0))
    live_ok = bool(exchange or balance or normalized_orders)
    account = {
        "balance_cents": balance_cents,
        "balance_usd": balance_cents / 100.0,
        "balance_dollars": balance.get("balance_dollars") or balance.get("balance"),
        "portfolio_value_cents": portfolio_cents,
        "portfolio_value_usd": portfolio_cents / 100.0,
        "updated_ts": balance.get("updated_ts") or balance.get("updated_time") or "",
        "raw": balance,
    }
    return {
        "ok": live_ok,
        "updated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "exchange": exchange,
        "account": account,
        "balance": account,
        "open_orders": normalized_orders,
        "positions": normalized_orders,
        "open_orders_count": len(normalized_orders),
        "open_notional_usd": round(notional, 2),
        "errors": errors,
        "error": "; ".join(errors),
    }
