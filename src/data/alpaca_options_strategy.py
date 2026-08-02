"""Alpaca OPTIONS trading strategy -- a new, separate strategy built the
same recipe as every other one here: real technical indicators + real
news sentiment feeding a trained direction classifier, same chronological-
holdout discipline (see alpaca_options_model.py).

Genuinely different in a few ways that matter for real-money safety, not
just mechanically ported from the equities/crypto strategies:

  - No technical-only cold-start fallback. The equities/crypto strategies
    will enter on a pure volume/volatility signal before any model exists
    (a reasonable bet with a whole share or a fractional coin). Options
    add real leverage AND time decay (theta) on top of direction risk --
    entering purely on "something looks unusual" without the model's own
    directional confidence is a materially worse bet here. Entries require
    a real trained model (see evaluate_candidate); there's simply no entry
    at all until one exists.

  - Direction is expressed via CONTRACT TYPE, not order side. A confident
    "up" prediction buys a call; a confident "down" prediction buys a put
    -- both are still a `side="buy"` order (Alpaca options Level 2 only
    covers buying calls/puts, not writing/selling them), so this is
    naturally bidirectional through contract choice alone, no margin or
    short-selling approval needed.

  - Exits also force-close near expiration (see _near_expiration), on top
    of the usual take-profit/stop-loss/max-hold checks -- letting an
    option ride into its own expiration risks assignment or the contract
    going worthless for reasons that have nothing to do with the
    strategy's own exit logic.

  - No broker-native bracket order (Alpaca's own docs don't address
    order_class for options at all -- treated here as "assume unsupported
    until proven otherwise"), so exits are managed by this bot's own poll
    loop, the same pattern already proven for crypto.

  - Position sizing is a whole number of CONTRACTS (qty), sized from the
    contract's own premium * its 100-share multiplier -- options don't
    support notional orders at all (confirmed via Alpaca's docs).
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


# Options swing much harder (percentage-wise) than their underlying, so
# these defaults are deliberately wider than the equities/crypto
# strategies' own TP/SL -- a 1% underlying move can easily be a 10-20%+
# option premium move.
MODEL_CONFIDENCE_MIN = _env_float("ALPACA_OPTIONS_MODEL_CONFIDENCE_MIN", 0.58)
TAKE_PROFIT_PCT = _env_float("ALPACA_OPTIONS_TAKE_PROFIT_PCT", 0.30)
STOP_LOSS_PCT = _env_float("ALPACA_OPTIONS_STOP_LOSS_PCT", 0.20)
MAX_HOLD_MINUTES = _env_int("ALPACA_OPTIONS_MAX_HOLD_MINUTES", 180)
# Force-close a held contract once fewer than this many days remain before
# expiration, regardless of TP/SL/max-hold -- avoids assignment risk and a
# contract decaying to worthless for reasons unrelated to this strategy's
# own exit signal.
MIN_DAYS_TO_EXPIRATION_BEFORE_FORCED_EXIT = _env_int("ALPACA_OPTIONS_MIN_DAYS_TO_EXPIRATION_BEFORE_FORCED_EXIT", 2)

POSITION_SIZE_PCT = _env_float("ALPACA_OPTIONS_POSITION_SIZE_PCT", 0.25)
MAX_CONCURRENT_POSITIONS = max(1, _env_int("ALPACA_OPTIONS_MAX_CONCURRENT_POSITIONS", 2))
DAILY_LOSS_CAP_PCT = _env_float("ALPACA_OPTIONS_DAILY_LOSS_CAP_PCT", 0.10)

LIVE_TRADING_ENABLED = str(os.getenv("ALPACA_OPTIONS_LIVE_TRADING_ENABLED", "")).strip().lower() in {"1", "true", "yes"}


def evaluate_candidate(row: dict[str, Any], model_prediction: dict[str, Any] | None) -> dict[str, Any]:
    """No technical-only fallback here -- see this module's own docstring
    for why. Returns should_enter=False with no trained model at all."""
    result: dict[str, Any] = {
        "symbol": row.get("symbol"), "model_ok": False, "should_enter": False, "direction": None,
    }
    if not model_prediction or not model_prediction.get("model_ok"):
        result["reason"] = "no trained model yet -- options entries require real model confidence"
        return result

    proba_up = model_prediction["probability_up"]
    result["model_ok"] = True
    result["probability_up"] = proba_up
    if proba_up >= MODEL_CONFIDENCE_MIN:
        result["should_enter"] = True
        result["direction"] = "up"
        result["reason"] = f"model confident UP ({proba_up:.2%}) -- buying a call"
    elif proba_up <= (1.0 - MODEL_CONFIDENCE_MIN):
        result["should_enter"] = True
        result["direction"] = "down"
        result["reason"] = f"model confident DOWN ({proba_up:.2%}) -- buying a put"
    else:
        result["reason"] = f"model not confident enough either way ({proba_up:.2%})"
    return result


def _near_expiration(position: dict[str, Any], *, now: dt.datetime) -> bool:
    expiration_date = position.get("expiration_date")
    if not expiration_date:
        return False
    exp = dt.datetime.fromisoformat(expiration_date).replace(tzinfo=dt.timezone.utc)
    return (exp - now).days < MIN_DAYS_TO_EXPIRATION_BEFORE_FORCED_EXIT


def decide_exit(
    position: dict[str, Any], current_price: float, *, now: dt.datetime | None = None,
) -> tuple[bool, str]:
    """Long-only (buying calls/puts is always a long position in the
    contract itself, regardless of which direction it bets on the
    underlying): a RISING contract premium is favorable."""
    now = now if now is not None else dt.datetime.now(dt.timezone.utc)
    if _near_expiration(position, now=now):
        return True, "near_expiration"

    entry_price = float(position["entry_price"])
    change_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0.0

    if change_pct >= TAKE_PROFIT_PCT:
        return True, f"take_profit ({change_pct:+.3%})"
    if change_pct <= -STOP_LOSS_PCT:
        return True, f"stop_loss ({change_pct:+.3%})"

    opened_at = dt.datetime.fromisoformat(position["opened_at"])
    held_minutes = (now - opened_at).total_seconds() / 60.0
    if held_minutes >= MAX_HOLD_MINUTES:
        return True, f"max_hold_time ({held_minutes:.0f}min, {change_pct:+.3%})"
    return False, f"holding ({change_pct:+.3%}, {held_minutes:.0f}min)"


def position_exit_levels(position: dict[str, Any]) -> dict[str, float]:
    entry_price = float(position["entry_price"])
    return {
        "take_profit_price": round(entry_price * (1 + TAKE_PROFIT_PCT), 6),
        "stop_loss_price": round(entry_price * (1 - STOP_LOSS_PCT), 6),
    }


def compute_contract_qty(available_balance_usd: float, contract_price: float, *, multiplier: int = 100) -> int:
    """Whole contracts only (confirmed required by Alpaca for options).
    `contract_price` is the per-share premium; the real cost of one
    contract is contract_price * multiplier (typically 100 shares)."""
    if contract_price <= 0:
        return 0
    cost_per_contract = contract_price * multiplier
    if cost_per_contract <= 0:
        return 0
    budget = max(0.0, available_balance_usd) * POSITION_SIZE_PCT
    return int(budget // cost_per_contract)


# ---------------------------------------------------------------------------
# Two modes, one codebase -- same dual-gate posture as every other strategy
# here.
# ---------------------------------------------------------------------------
MODE = os.getenv("ALPACA_OPTIONS_MODE", "simulate").strip().lower()
SIMULATE_STARTING_BALANCE = _env_float("ALPACA_OPTIONS_SIMULATE_STARTING_BALANCE", 500.0)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
STATE_FILE = Path(os.getenv("ALPACA_OPTIONS_STATE_FILE", str(DATA_DIR / "alpaca_options_state.json")))
_STATE_LOCK = threading.RLock()

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_ALPACA_OPTIONS_MODEL_REPO = os.getenv("HF_ALPACA_OPTIONS_MODEL_REPO", "papylove/alpaca-options-model")
_DURABLE_STATE_HF_FILENAME = "alpaca_options_durable_state.json"
_DURABLE_PUSH_MIN_INTERVAL_SEC = 30
_last_durable_push_ts = 0.0


def _today_str() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def _durable_state_slice(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "balance": state.get("balance", SIMULATE_STARTING_BALANCE),
        "positions": state.get("positions") or [],
        "trade_log": state.get("trade_log") or [],
        "realized_pnl_by_date": state.get("realized_pnl_by_date") or {},
    }


def _push_durable_state_to_hf(state: dict[str, Any]) -> None:
    if not HF_API_KEY:
        return
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
        payload = json.dumps(_durable_state_slice(state), indent=2)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp.write(payload)
            tmp_path = tmp.name
        try:
            api.upload_file(
                path_or_fileobj=tmp_path, path_in_repo=_DURABLE_STATE_HF_FILENAME,
                repo_id=HF_ALPACA_OPTIONS_MODEL_REPO, repo_type="model", commit_message="update alpaca options durable state",
            )
        finally:
            os.unlink(tmp_path)
    except Exception as exc:
        logger.warning("[alpaca_options_strategy] durable state push to HF failed: %s", exc)


def _pull_durable_state_from_hf() -> dict[str, Any] | None:
    if not HF_API_KEY:
        return None
    try:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(
            repo_id=HF_ALPACA_OPTIONS_MODEL_REPO, filename=_DURABLE_STATE_HF_FILENAME, repo_type="model", token=HF_API_KEY,
        )
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        logger.info("[alpaca_options_strategy] no durable state on HF yet (or fetch failed): %s", exc)
        return None


def _load_state() -> dict[str, Any]:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        base = {
            "balance": SIMULATE_STARTING_BALANCE, "positions": [], "trade_log": [], "realized_pnl_by_date": {},
        }
        durable = _pull_durable_state_from_hf()
        if durable:
            base.update(durable)
            logger.info("[alpaca_options_strategy] recovered durable state from HF after local state was missing")
        return base
    state.setdefault("balance", SIMULATE_STARTING_BALANCE)
    state.setdefault("positions", [])
    state.setdefault("trade_log", [])
    state.setdefault("realized_pnl_by_date", {})
    return state


def _save_state(state: dict[str, Any], *, push_durable: bool = False) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    if not push_durable:
        return
    global _last_durable_push_ts
    now = time.time()
    if now - _last_durable_push_ts >= _DURABLE_PUSH_MIN_INTERVAL_SEC:
        _last_durable_push_ts = now
        _push_durable_state_to_hf(state)


def get_available_balance() -> float:
    if MODE == "live":
        from data import alpaca_client
        account = alpaca_client.get_account()
        return float(account.get("cash") or 0.0)
    state = _load_state()
    committed = sum(float(p["entry_price"]) * float(p["count"]) * 100 for p in (state.get("positions") or []))
    return max(0.0, float(state.get("balance", SIMULATE_STARTING_BALANCE)) - committed)


def get_current_option_price(contract_symbol: str) -> float | None:
    from data import alpaca_client
    try:
        quote = alpaca_client.get_option_latest_quote(contract_symbol)
        ask, bid = quote.get("ap"), quote.get("bp")
        if ask and bid:
            return (float(ask) + float(bid)) / 2.0
        price = ask or bid
        return float(price) if price else None
    except Exception as exc:
        logger.warning("[alpaca_options_strategy] option quote fetch failed for %s: %s", contract_symbol, exc)
        return None


def scan_and_enter(symbols: list[str] | None = None, *, dry_run: bool | None = None) -> dict[str, Any]:
    """Evaluates each underlying for a directional options entry. Requires
    a real trained model (see evaluate_candidate) -- no technical-only
    cold-start fallback. "simulate" mode always paper-trades; "live" mode
    places a real plain market buy order for the chosen contract UNLESS
    dry_run resolves True."""
    from data.alpaca_options_data import get_options_universe, latest_feature_row, select_contract
    from data.alpaca_options_model import predict_direction
    from data import alpaca_client, threads_post

    effective_dry_run = (not LIVE_TRADING_ENABLED) if dry_run is None else dry_run
    if symbols is None:
        symbols = get_options_universe()

    opened: list[dict[str, Any]] = []
    with _STATE_LOCK:
        state = _load_state()
        existing_underlyings = {p["underlying_symbol"] for p in (state.get("positions") or [])}
        open_count = len(state.get("positions") or [])
        reference_balance = float(state.get("balance", SIMULATE_STARTING_BALANCE))
        today_pnl = float((state.get("realized_pnl_by_date") or {}).get(_today_str(), 0.0))
    if reference_balance > 0 and today_pnl <= -abs(DAILY_LOSS_CAP_PCT) * reference_balance:
        return {"opened": [], "action": "daily_loss_cap_breached"}

    for symbol in symbols:
        try:
            if symbol in existing_underlyings:
                continue
            if open_count >= MAX_CONCURRENT_POSITIONS:
                opened.append({"symbol": symbol, "ok": True, "action": "skipped_slot_taken"})
                continue

            row = latest_feature_row(symbol)
            if row is None:
                continue
            model_prediction = predict_direction(symbol)
            candidate = evaluate_candidate(row, model_prediction)
            if not candidate["should_enter"]:
                opened.append({"symbol": symbol, "ok": True, "action": "skipped", "reason": candidate["reason"]})
                continue

            contract = select_contract(symbol, direction=candidate["direction"], current_price=row["current_price"])
            if contract is None:
                opened.append({"symbol": symbol, "ok": True, "action": "skipped_no_contract"})
                continue
            contract_symbol = contract["symbol"]

            contract_price = get_current_option_price(contract_symbol)
            if contract_price is None or contract_price <= 0:
                opened.append({"symbol": symbol, "ok": True, "action": "skipped_no_option_quote"})
                continue

            available_balance = get_available_balance()
            qty = compute_contract_qty(available_balance, contract_price)
            if qty < 1:
                opened.append({"symbol": symbol, "ok": True, "action": "skipped_insufficient_budget"})
                continue

            levels = position_exit_levels({"entry_price": contract_price})
            order_id = None
            if MODE == "live" and not effective_dry_run:
                order_spec = alpaca_client.build_option_order(symbol=contract_symbol, side="buy", qty=qty)
                order_id = alpaca_client.place_order(order_spec)

            position = {
                "symbol": contract_symbol, "underlying_symbol": symbol, "option_type": contract.get("type"),
                "expiration_date": contract.get("expiration_date"), "strike_price": contract.get("strike_price"),
                "entry_price": contract_price, "count": qty,
                "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "order_id": order_id, **levels,
            }
            with _STATE_LOCK:
                state = _load_state()
                positions = state.get("positions") or []
                if any(p["underlying_symbol"] == symbol for p in positions) or len(positions) >= MAX_CONCURRENT_POSITIONS:
                    opened.append({"symbol": symbol, "ok": True, "action": "skipped_slot_taken"})
                    continue
                positions.append(position)
                state["positions"] = positions
                _save_state(state)
                existing_underlyings.add(symbol)
                open_count = len(positions)
            trade_dry_run = effective_dry_run if MODE == "live" else True
            opened.append({
                "symbol": symbol, "ok": True, "action": "opened", "contract_symbol": contract_symbol,
                "option_type": contract.get("type"), "entry_price": contract_price, "count": qty, "dry_run": trade_dry_run,
            })
            try:
                threads_post.post_trade_entry(
                    ticker=contract_symbol, side="long",
                    entry_price=contract_price, take_profit_price=levels["take_profit_price"],
                    stop_loss_price=levels["stop_loss_price"], reason=candidate["reason"], dry_run=trade_dry_run,
                    market="options",
                )
            except Exception:
                logger.warning("[alpaca_options_strategy] Threads post for %s entry failed", contract_symbol, exc_info=True)
            # No chart-snapshot post here, unlike the other three strategies:
            # there's no historical option-PREMIUM series to chart on the
            # same scale as entry/take-profit/stop-loss (those are premium
            # dollars, not the underlying's price) -- this pipeline doesn't
            # record option-quote history over time, only point-in-time
            # quotes at scan time. Charting the underlying's own price
            # series instead would put a strike-price reference line next
            # to numbers on a completely different scale, which would be
            # confusing rather than informative.
        except Exception as exc:
            opened.append({"symbol": symbol, "ok": False, "action": "entry_failed", "error": str(exc)})

    return {"opened": opened}


def manage_open_positions(*, dry_run: bool | None = None) -> dict[str, Any]:
    """Checks every open option position for an exit (take-profit/stop-
    loss/max-hold/near-expiration). No broker-native bracket to reconcile
    against -- a triggered exit is always a fresh, plain market sell of
    the contract, same as crypto's own manage_open_positions."""
    from data import threads_post

    effective_dry_run = (not LIVE_TRADING_ENABLED) if dry_run is None else dry_run
    with _STATE_LOCK:
        state = _load_state()
        positions = list(state.get("positions") or [])
    if not positions:
        return {"action": "no_position", "checks": []}

    closed: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = []

    for position in positions:
        contract_symbol = position["symbol"]
        underlying_symbol = position["underlying_symbol"]
        try:
            current_price = get_current_option_price(contract_symbol)
            if current_price is None:
                checks.append({"symbol": contract_symbol, "ok": False, "error": "no_quote_available"})
                continue

            should_exit, reason = decide_exit(position, current_price)
            if not should_exit:
                checks.append({"symbol": contract_symbol, "ok": True, "exit_check": reason})
                continue

            if MODE == "live" and not effective_dry_run:
                from data import alpaca_client
                try:
                    order_spec = alpaca_client.build_option_order(symbol=contract_symbol, side="sell", qty=int(position["count"]))
                    alpaca_client.place_order(order_spec)
                except Exception as exc:
                    logger.warning("[alpaca_options_strategy] close order failed for %s: %s", contract_symbol, exc)

            gross = round((current_price - float(position["entry_price"])) * float(position["count"]) * 100, 6)
            with _STATE_LOCK:
                state = _load_state()
                state["balance"] = round(float(state.get("balance", SIMULATE_STARTING_BALANCE)) + gross, 6)
                by_date = state.setdefault("realized_pnl_by_date", {})
                today = _today_str()
                by_date[today] = round(float(by_date.get(today, 0.0)) + gross, 6)
                trade = {
                    "closed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "symbol": contract_symbol, "underlying_symbol": underlying_symbol,
                    "entry_price": position["entry_price"], "exit_price": current_price,
                    "count": position["count"], "realized_pnl_usd": gross, "reason": reason,
                    "dry_run": effective_dry_run if MODE == "live" else True,
                }
                state.setdefault("trade_log", []).append(trade)
                state["positions"] = [p for p in (state.get("positions") or []) if p["symbol"] != contract_symbol]
                _save_state(state, push_durable=True)
            closed.append(trade)
            try:
                threads_post.post_trade_exit(
                    ticker=contract_symbol, side="long", entry_price=float(position["entry_price"]),
                    exit_price=current_price, pnl_usd=gross, reason=reason, dry_run=trade["dry_run"], market="options",
                )
            except Exception:
                logger.warning("[alpaca_options_strategy] Threads post for %s exit failed", contract_symbol, exc_info=True)
        except Exception as exc:
            logger.warning("[alpaca_options_strategy] could not process position for %s -- leaving untouched this cycle: %s", contract_symbol, exc)
            checks.append({"symbol": contract_symbol, "ok": False, "error": str(exc)})

    return {"action": "closed" if closed else "no_change", "closed": closed, "checks": checks}
