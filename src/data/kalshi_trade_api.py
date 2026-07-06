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
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def _extract_first_numeric(payload: dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        if key in payload and payload.get(key) not in (None, ""):
            return _to_float(payload.get(key), 0.0)
    return None


def _is_integer_like(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if text.startswith(("+", "-")):
        text = text[1:]
    return text.isdigit()


def _normalize_balance_usd(balance: dict[str, Any], aggregated_balance_usd: float) -> float:
    direct_usd = _extract_first_numeric(
        balance,
        [
            "available_buying_power_dollars",
            "available_balance_dollars",
            "buying_power_dollars",
            "cash_balance_dollars",
            "balance_dollars",
            "balance_usd",
        ],
    )
    if direct_usd is not None and direct_usd > 0:
        return round(direct_usd, 2)

    cents_value = _extract_first_numeric(
        balance,
        [
            "available_buying_power_cents",
            "available_balance_cents",
            "buying_power_cents",
            "balance_cents",
        ],
    )
    if cents_value is not None and cents_value > 0:
        return round(cents_value / 100.0, 2)

    raw_balance = balance.get("balance")
    if raw_balance not in (None, ""):
        numeric = _to_float(raw_balance, 0.0)
        if numeric > 0:
            if _is_integer_like(raw_balance):
                as_cents = round(numeric / 100.0, 2)
                if aggregated_balance_usd > 0:
                    cents_gap = abs(as_cents - aggregated_balance_usd)
                    dollars_gap = abs(numeric - aggregated_balance_usd)
                    return as_cents if cents_gap <= dollars_gap else round(numeric, 2)
                return as_cents
            return round(numeric, 2)

    if aggregated_balance_usd > 0:
        return round(aggregated_balance_usd, 2)
    return 0.0


def _normalize_portfolio_value_usd(balance: dict[str, Any], fallback_balance_usd: float) -> float:
    direct_usd = _extract_first_numeric(
        balance,
        [
            "portfolio_value_dollars",
            "portfolio_value_usd",
            "account_value_dollars",
            "account_value_usd",
            "nav_dollars",
            "nav_usd",
        ],
    )
    if direct_usd is not None and direct_usd > 0:
        return round(direct_usd, 2)

    cents_value = _extract_first_numeric(
        balance,
        [
            "portfolio_value_cents",
            "account_value_cents",
            "nav_cents",
        ],
    )
    if cents_value is not None and cents_value > 0:
        return round(cents_value / 100.0, 2)

    raw = balance.get("portfolio_value")
    if raw not in (None, ""):
        numeric = _to_float(raw, 0.0)
        if numeric > 0:
            if _is_integer_like(raw) and numeric >= 100000:
                return round(numeric / 100.0, 2)
            return round(numeric, 2)

    return round(max(fallback_balance_usd, 0.0), 2)


def _coerce_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _coerce_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _pick_balance_payload(payload: dict[str, Any]) -> dict[str, Any]:
    base = _coerce_dict(payload)
    nested = _coerce_dict(base.get("balance"))
    if nested:
        merged = dict(base)
        merged.update(nested)
        return merged
    for key in ("account", "portfolio", "data", "result"):
        node = _coerce_dict(base.get(key))
        if node:
            merged = dict(base)
            merged.update(node)
            return merged
    return base


def _extract_subaccount_rows(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    base = _coerce_dict(payload)
    for key in ("subaccount_balances", "balances", "subaccounts", "accounts", "rows", "data"):
        rows = base.get(key)
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)], key
        if isinstance(rows, dict):
            nested_rows, nested_source = _extract_subaccount_rows(rows)
            if nested_rows:
                return nested_rows, f"{key}.{nested_source}"
    return [], ""


def _extract_positions(positions_payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    payload = _coerce_dict(positions_payload)
    market_positions = [row for row in _coerce_list(payload.get("market_positions")) if isinstance(row, dict)]
    event_positions = [row for row in _coerce_list(payload.get("event_positions")) if isinstance(row, dict)]
    if market_positions or event_positions:
        return market_positions, event_positions

    generic_rows = []
    for key in ("positions", "open_positions", "rows", "data"):
        rows = payload.get(key)
        if isinstance(rows, list):
            generic_rows = [row for row in rows if isinstance(row, dict)]
            if generic_rows:
                break
        if isinstance(rows, dict):
            for nested_key in ("positions", "open_positions", "rows", "data"):
                nested = rows.get(nested_key)
                if isinstance(nested, list):
                    generic_rows = [row for row in nested if isinstance(row, dict)]
                    if generic_rows:
                        break
            if generic_rows:
                break

    for row in generic_rows:
        ticker = str(row.get("ticker") or row.get("market_ticker") or "").strip()
        if ticker:
            row.setdefault("ticker", ticker)
        event_ticker = str(row.get("event_ticker") or row.get("event") or "").strip()
        if event_ticker:
            row.setdefault("event_ticker", event_ticker)
        if "market_exposure_dollars" not in row:
            if row.get("event_exposure_dollars") is not None:
                row["market_exposure_dollars"] = row.get("event_exposure_dollars")
            elif row.get("exposure_dollars") is not None:
                row["market_exposure_dollars"] = row.get("exposure_dollars")
            elif row.get("exposure") is not None:
                row["market_exposure_dollars"] = row.get("exposure")

    market_like = [row for row in generic_rows if str(row.get("ticker") or "").strip()]
    event_like = [row for row in generic_rows if str(row.get("event_ticker") or "").strip()]
    return market_like, event_like


def _row_balance_usd(row: dict[str, Any]) -> float:
    if not isinstance(row, dict):
        return 0.0
    direct_usd = _extract_first_numeric(
        row,
        [
            "available_buying_power_dollars",
            "available_balance_dollars",
            "buying_power_dollars",
            "cash_balance_dollars",
            "balance_dollars",
            "balance_usd",
        ],
    )
    if direct_usd is not None and direct_usd > 0:
        return float(direct_usd)
    cents_value = _extract_first_numeric(
        row,
        [
            "available_buying_power_cents",
            "available_balance_cents",
            "buying_power_cents",
            "balance_cents",
        ],
    )
    if cents_value is not None and cents_value > 0:
        return float(cents_value) / 100.0
    raw_balance = row.get("balance")
    if raw_balance not in (None, ""):
        numeric = _to_float(raw_balance, 0.0)
        if numeric > 0:
            if _is_integer_like(raw_balance):
                return float(numeric) / 100.0
            return float(numeric)
    return 0.0


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


def get_all_subaccount_balances() -> dict[str, Any]:
    return _request_json("GET", "/portfolio/subaccounts/balances", auth=True)


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


def get_positions(
    *,
    limit: int = 100,
    cursor: str | None = None,
    count_filter: str | None = None,
    ticker: str | None = None,
    event_ticker: str | None = None,
    subaccount: int | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": max(1, min(int(limit), 1000))}
    if cursor:
        params["cursor"] = str(cursor)
    if count_filter:
        params["count_filter"] = str(count_filter)
    if ticker:
        params["ticker"] = str(ticker)
    if event_ticker:
        params["event_ticker"] = str(event_ticker)
    if subaccount is not None:
        params["subaccount"] = int(subaccount)
    return _request_json("GET", "/portfolio/positions", params=params, auth=True)


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
    by_uid: dict[str, dict[str, Any]] = {}
    for p in predictions:
        if not isinstance(p, dict):
            continue
        for key in ("prediction_id", "prediction_uid", "uid", "bet_uid"):
            value = str(p.get(key) or "").strip()
            if value and value not in by_uid:
                by_uid[value] = p
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
    # Re-resolve combos against a fresh catalog so newly-open combo markets
    # are not missed when the single pass already succeeded.
    combo_res = resolve_ready_bets(combo_suggestions, force_refresh=True)
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
    subaccount_balances: dict[str, Any] = {}
    open_orders: list[dict[str, Any]] = []
    positions_payload: dict[str, Any] = {}
    errors: list[str] = []

    def _capture(key: str, fn, *args, **kwargs):
        try:
            return key, fn(*args, **kwargs), ""
        except Exception as exc:
            return key, None, str(exc)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(_capture, "exchange", get_exchange_status),
            executor.submit(_capture, "balance", get_balance),
            executor.submit(_capture, "subaccount_balances", get_all_subaccount_balances),
            executor.submit(_capture, "orders", get_open_orders, max_pages=max(1, int(os.getenv("KALSHI_SNAP_MAX_PAGES", "2") or "2")), page_limit=200),
            executor.submit(_capture, "positions", get_positions, count_filter="position,total_traded"),
        ]
        for future in as_completed(futures):
            key, value, err = future.result()
            if err:
                errors.append(f"{key}: {err}")
                continue
            if key == "exchange" and isinstance(value, dict):
                exchange = value
            elif key == "balance" and isinstance(value, dict):
                balance = value
            elif key == "subaccount_balances" and isinstance(value, dict):
                subaccount_balances = value
            elif key == "orders" and isinstance(value, list):
                open_orders = value
            elif key == "positions" and isinstance(value, dict):
                positions_payload = value

    balance = _pick_balance_payload(balance)
    all_balances, subaccount_balance_source = _extract_subaccount_rows(subaccount_balances)

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

    market_notional = 0.0
    event_notional = 0.0
    normalized_market_positions: list[dict[str, Any]] = []
    normalized_event_positions: list[dict[str, Any]] = []
    market_positions, event_positions = _extract_positions(positions_payload)
    for row in market_positions:
        if not isinstance(row, dict):
            continue
        exposure = _to_float(row.get("market_exposure_dollars"), 0.0)
        market_notional += abs(exposure)
        normalized_market_positions.append(
            {
                "ticker": str(row.get("ticker") or ""),
                "position_fp": str(row.get("position_fp") or "0"),
                "total_traded_dollars": _to_float(row.get("total_traded_dollars"), 0.0),
                "market_exposure_dollars": exposure,
                "realized_pnl_dollars": _to_float(row.get("realized_pnl_dollars"), 0.0),
                "fees_paid_dollars": _to_float(row.get("fees_paid_dollars"), 0.0),
                "last_updated_ts": row.get("last_updated_ts") or "",
                "position_type": "market",
            }
        )

    for row in event_positions:
        if not isinstance(row, dict):
            continue
        exposure = _to_float(
            row.get("event_exposure_dollars"),
            _to_float(row.get("market_exposure_dollars"), 0.0),
        )
        event_notional += abs(exposure)
        normalized_event_positions.append(
            {
                "event_ticker": str(row.get("event_ticker") or ""),
                "ticker": str(row.get("event_ticker") or row.get("ticker") or ""),
                "position_fp": str(row.get("total_cost_shares_fp") or row.get("position_fp") or "0"),
                "total_traded_dollars": _to_float(row.get("total_cost_dollars"), _to_float(row.get("total_traded_dollars"), 0.0)),
                "market_exposure_dollars": exposure,
                "realized_pnl_dollars": _to_float(row.get("realized_pnl_dollars"), 0.0),
                "fees_paid_dollars": _to_float(row.get("fees_paid_dollars"), 0.0),
                "last_updated_ts": row.get("last_updated_ts") or "",
                "position_type": "event",
                "total_cost_dollars": _to_float(row.get("total_cost_dollars"), 0.0),
                "total_cost_shares_fp": str(row.get("total_cost_shares_fp") or "0"),
                "event_exposure_dollars": exposure,
            }
        )

    aggregated_balance_usd = 0.0
    aggregated_updated_ts = ""
    if isinstance(all_balances, list) and all_balances:
        for row in all_balances:
            if not isinstance(row, dict):
                continue
            aggregated_balance_usd += _row_balance_usd(row)
            updated_ts = row.get("updated_ts")
            if updated_ts and (not aggregated_updated_ts or str(updated_ts) > aggregated_updated_ts):
                aggregated_updated_ts = str(updated_ts)

    primary_balance_usd = _normalize_balance_usd(balance, aggregated_balance_usd)

    balance_cents = int(round(max(primary_balance_usd, 0.0) * 100))
    portfolio_value_usd = _normalize_portfolio_value_usd(balance, primary_balance_usd)
    portfolio_cents = int(round(max(portfolio_value_usd, 0.0) * 100))
    has_authenticated_account = bool(balance or all_balances)
    has_positions_or_orders = bool(normalized_orders or normalized_market_positions or normalized_event_positions)
    live_ok = bool(has_authenticated_account or has_positions_or_orders)
    all_positions = [*normalized_market_positions, *normalized_event_positions]
    open_position_notional = round(market_notional + event_notional, 2)
    account = {
        "balance_cents": balance_cents,
        "balance_usd": balance_cents / 100.0,
        "buying_power_usd": balance_cents / 100.0,
        "balance_dollars": balance.get("balance_dollars") or balance.get("balance"),
        "portfolio_value_cents": portfolio_cents,
        "portfolio_value_usd": portfolio_cents / 100.0,
        "updated_ts": balance.get("updated_ts") or subaccount_balances.get("updated_ts") or aggregated_updated_ts or balance.get("updated_time") or "",
        "balance_breakdown": balance.get("balance_breakdown") or [],
        "raw": balance,
        "subaccount_balances": all_balances,
        "subaccount_balance_source": subaccount_balance_source,
    }
    return {
        "ok": live_ok,
        "updated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "exchange": exchange,
        "account": account,
        "balance": account,
        "positions_payload": positions_payload,
        "market_positions": normalized_market_positions,
        "event_positions": normalized_event_positions,
        "all_positions": all_positions,
        "open_orders": normalized_orders,
        "positions": all_positions or normalized_orders,
        "open_orders_count": len(normalized_orders),
        "position_count": len(all_positions),
        "open_notional_usd": round(notional + open_position_notional, 2),
        "errors": errors,
        "error": "; ".join(errors),
    }
