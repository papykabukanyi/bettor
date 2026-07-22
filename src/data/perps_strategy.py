"""Kalshi Perps scalping strategy across ALL listed instruments (BTC, ETH,
SOL, XRP, DOGE, LTC, BCH, LINK, SUI, NEAR, DOT, HBAR, HYPE, kSHIB, XLM, ZEC):
"buy small dips the model + technicals agree are heading up, take a small
profit, repeat."

Every safety decision lives HERE, not in the generic API client, so it's all
in one place to review:

  - DRY RUN BY DEFAULT. Real orders require BOTH `KALSHI_PERPS_LIVE_TRADING_ENABLED=1`
    in the environment AND the caller not passing dry_run=True.
  - Exactly ONE open position at a time, across ALL instruments. No
    pyramiding, no averaging down, no spreading a ~$10 account across many
    tiny positions at once.
  - A hard per-trade size in WHOLE contracts (PERPS_TRADE_SIZE_CONTRACTS,
    default 1 -- these markets don't support fractional contracts).
  - A hard daily realized-loss cap (PERPS_DAILY_LOSS_CAP_USD) that halts new
    entries (but not exits) once breached for the day.
  - Every position has a take-profit, a stop-loss, AND a max hold time.

Signal logic (two independent checks must both agree -- neither alone is
enough to enter a real-money position):
  1. Technical scalper filter: 60-minute candles are a trend filter (skip
     entries if the market fell more than TREND_FILTER_DOWN_PCT over the
     lookback window); a 1-minute short moving average detects a small local
     dip (ENTRY_DIP_PCT below it triggers interest).
  2. Direction model (perps_model.py): a classifier trained on Hugging
     Face-archived multi-timeframe history + news sentiment predicts up/down
     over the next PERPS_LABEL_HORIZON_MINUTES (default 30). Entry requires
     probability_up >= PERPS_MODEL_CONFIDENCE_MIN.

Until enough historical data has been collected for a model to exist yet
(the first days of running), the model check is skipped and the strategy
runs on the technical filter alone -- clearly flagged in every result as
`model_ok: false` so it's obvious from the dashboard when the model has
kicked in.

This is a real trading strategy with real risk. No strategy in this space is
risk-free, and past price behavior does not guarantee future results. Start
in dry-run, watch its decisions for a while, and only enable live trading
once you're comfortable with them.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any

from data.kalshi_perps import cancel_margin_order, create_margin_order, get_margin_balance, get_margin_market
from data.perps_data import get_watchlist, latest_feature_row
from data.perps_model import predict_direction

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
STATE_FILE = Path(os.getenv("PERPS_STATE_FILE", str(DATA_DIR / "perps_state.json")))


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or str(default))
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or str(default))
    except Exception:
        return int(default)


# ── Tunable parameters (all overridable via env, conservative defaults) ─────
TRADE_SIZE_CONTRACTS = max(1, _env_int("PERPS_TRADE_SIZE_CONTRACTS", 1))
TAKE_PROFIT_PCT = _env_float("PERPS_TAKE_PROFIT_PCT", 0.004)   # +0.4% -> take small profit
STOP_LOSS_PCT = _env_float("PERPS_STOP_LOSS_PCT", 0.008)       # -0.8% -> cut the loss
MAX_HOLD_MINUTES = _env_int("PERPS_MAX_HOLD_MINUTES", 30)
ENTRY_DIP_PCT = _env_float("PERPS_ENTRY_DIP_PCT", 0.0015)      # 0.15% below short MA triggers interest
SHORT_MA_MINUTES = _env_int("PERPS_SHORT_MA_MINUTES", 15)
TREND_FILTER_DOWN_PCT = _env_float("PERPS_TREND_FILTER_DOWN_PCT", 0.02)  # skip entries if down >2%
MODEL_CONFIDENCE_MIN = _env_float("PERPS_MODEL_CONFIDENCE_MIN", 0.55)
DAILY_LOSS_CAP_USD = _env_float("PERPS_DAILY_LOSS_CAP_USD", 1.0)
LIVE_TRADING_ENABLED = _env_flag("KALSHI_PERPS_LIVE_TRADING_ENABLED", default=False)


def _load_state() -> dict[str, Any]:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"position": None, "trade_log": [], "realized_pnl_by_date": {}}


def _save_state(state: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def _today_str() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def _round_price(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return round(price, 4)
    steps = round(price / tick_size)
    return round(steps * tick_size, 6)


def decide_entry_technical(row: dict[str, Any]) -> tuple[bool, str]:
    """The scalper filter alone: trend + local dip. `row` needs
    current_price, short_ma, trend_pct (as returned by
    perps_data.latest_feature_row / perps_model.predict_direction)."""
    if row["trend_pct"] < -TREND_FILTER_DOWN_PCT:
        return False, f"trend filter: down {row['trend_pct']:.2%}, skipping entries"
    dip_pct = (row["short_ma"] - row["current_price"]) / row["short_ma"] if row["short_ma"] > 0 else 0.0
    if dip_pct >= ENTRY_DIP_PCT:
        return True, f"price {dip_pct:.3%} below {SHORT_MA_MINUTES}-min average -- small dip"
    return False, f"no dip signal (price {dip_pct:+.3%} vs short MA)"


def evaluate_candidate(ticker: str) -> dict[str, Any]:
    """Combine the technical scalper filter with the direction model for one
    ticker. `should_enter` requires the technical dip signal; if a trained
    model exists it must ALSO predict "up" with enough confidence. If no
    model exists yet, the technical signal alone decides (clearly flagged)."""
    row = latest_feature_row(ticker)
    if row is None:
        return {"ticker": ticker, "should_enter": False, "reason": "no_feature_data", "model_ok": False}

    technical_ok, technical_reason = decide_entry_technical(row)
    prediction = predict_direction(ticker)
    model_ok = bool(prediction.get("model_ok"))

    result: dict[str, Any] = {
        "ticker": ticker, "current_price": row["current_price"], "short_ma": row["short_ma"],
        "trend_pct": row["trend_pct"], "technical_ok": technical_ok, "technical_reason": technical_reason,
        "model_ok": model_ok,
    }
    if model_ok:
        result["probability_up"] = prediction["probability_up"]
        result["model_direction"] = prediction["direction"]

    if not technical_ok:
        result["should_enter"] = False
        result["reason"] = technical_reason
        return result

    if not model_ok:
        result["should_enter"] = True
        result["reason"] = f"{technical_reason} (model not trained yet -- technical-only fallback)"
        result["score"] = ENTRY_DIP_PCT + (row["short_ma"] - row["current_price"]) / row["short_ma"]
        return result

    if prediction["direction"] == "up" and prediction["probability_up"] >= MODEL_CONFIDENCE_MIN:
        result["should_enter"] = True
        result["reason"] = f"{technical_reason}; model predicts up (p={prediction['probability_up']:.2f})"
        result["score"] = prediction["probability_up"]
        return result

    result["should_enter"] = False
    result["reason"] = f"{technical_reason}, but model predicts {prediction['direction']} (p_up={prediction['probability_up']:.2f})"
    return result


def scan_for_best_entry(tickers: list[str] | None = None) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Evaluate every ticker in the watchlist; return the single best entry
    candidate (highest score) plus every candidate's evaluation for
    observability. Returns (None, [...]) if nothing qualifies."""
    watchlist = tickers or get_watchlist()
    candidates = [evaluate_candidate(t) for t in watchlist]
    qualifying = [c for c in candidates if c.get("should_enter")]
    if not qualifying:
        return None, candidates
    best = max(qualifying, key=lambda c: c.get("score", 0.0))
    return best, candidates


def decide_exit(position: dict[str, Any], current_price: float) -> tuple[bool, str]:
    entry_price = float(position["entry_price"])
    change_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0.0
    if change_pct >= TAKE_PROFIT_PCT:
        return True, f"take_profit ({change_pct:+.3%})"
    if change_pct <= -STOP_LOSS_PCT:
        return True, f"stop_loss ({change_pct:+.3%})"
    opened_at = dt.datetime.fromisoformat(position["opened_at"])
    held_minutes = (dt.datetime.now(dt.timezone.utc) - opened_at).total_seconds() / 60.0
    if held_minutes >= MAX_HOLD_MINUTES:
        return True, f"max_hold_time ({held_minutes:.0f}min, {change_pct:+.3%})"
    return False, f"holding ({change_pct:+.3%}, {held_minutes:.0f}min)"


def _daily_loss_cap_breached(state: dict[str, Any]) -> bool:
    today_pnl = float((state.get("realized_pnl_by_date") or {}).get(_today_str(), 0.0))
    return today_pnl <= -abs(DAILY_LOSS_CAP_USD)


def run_cycle(*, dry_run: bool | None = None) -> dict[str, Any]:
    """One strategy iteration: manage an existing position (check exit) or
    scan every instrument for a new entry. Call this on a short interval
    (e.g. every 1-5 minutes) to match the scalping cadence."""
    effective_dry_run = True if not LIVE_TRADING_ENABLED else bool(dry_run if dry_run is not None else True)
    state = _load_state()
    result: dict[str, Any] = {"ok": True, "dry_run": effective_dry_run, "action": "none"}

    position = state.get("position")

    if position is not None:
        ticker = position["ticker"]
        try:
            market = get_margin_market(ticker)
            current_price = float((market.get("market") or {}).get("price") or 0.0)
            tick_size = float((market.get("market") or {}).get("tick_size") or 0.0001)
        except Exception as exc:
            result["ok"] = False
            result["action"] = "price_fetch_failed"
            result["error"] = str(exc)
            return result

        should_exit, reason = decide_exit(position, current_price)
        result["exit_check"] = reason
        result["ticker"] = ticker
        if should_exit:
            count = float(position["count"])
            exit_price = _round_price(current_price, tick_size)
            order_result = None
            if not effective_dry_run:
                order_result = create_margin_order(
                    ticker=ticker, side="ask", count=count, price=exit_price,
                    client_order_id=str(uuid.uuid4()), time_in_force="immediate_or_cancel", reduce_only=True,
                )
            entry_price = float(position["entry_price"])
            # Each market's quoted "price" IS the per-contract dollar value
            # already (e.g. KXBTCPERP's "0.0001 BTC" contract priced ~$6.63
            # when BTC ~$66,300), so P&L is simply the price delta times
            # contract count -- no separate multiplier needed.
            realized_pnl = round((exit_price - entry_price) * count, 6)
            by_date = state.setdefault("realized_pnl_by_date", {})
            by_date[_today_str()] = round(float(by_date.get(_today_str(), 0.0)) + realized_pnl, 6)
            state.setdefault("trade_log", []).append({
                "closed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "ticker": ticker, "entry_price": entry_price, "exit_price": exit_price,
                "count": count, "realized_pnl_usd": realized_pnl, "reason": reason, "dry_run": effective_dry_run,
            })
            state["position"] = None
            result["action"] = "closed"
            result["realized_pnl_usd"] = realized_pnl
            result["order_result"] = order_result
        _save_state(state)
        return result

    if _daily_loss_cap_breached(state):
        result["action"] = "skipped_daily_loss_cap"
        return result

    best, candidates = scan_for_best_entry()
    result["candidates"] = candidates
    if best is None:
        return result

    ticker = best["ticker"]
    try:
        market = get_margin_market(ticker)
        tick_size = float((market.get("market") or {}).get("tick_size") or 0.0001)
    except Exception as exc:
        result["action"] = "skipped_market_fetch_failed"
        result["error"] = str(exc)
        return result

    entry_price = _round_price(best["current_price"], tick_size)
    count = float(TRADE_SIZE_CONTRACTS)  # whole contracts only
    estimated_notional_usd = round(count * entry_price, 4)
    order_result = None
    if not effective_dry_run:
        try:
            balance = get_margin_balance(compute_available_balance=True)
            available = 0.0
            for sub in (balance.get("subaccount_balances") or []):
                available = max(available, float(sub.get("available_balance") or 0.0))
            if available < estimated_notional_usd:
                result["action"] = "skipped_insufficient_balance"
                result["available_balance_usd"] = available
                return result
        except Exception as exc:
            result["action"] = "skipped_balance_check_failed"
            result["error"] = str(exc)
            return result
        order_result = create_margin_order(
            ticker=ticker, side="bid", count=count, price=entry_price,
            client_order_id=str(uuid.uuid4()), time_in_force="immediate_or_cancel",
        )
    state["position"] = {
        "ticker": ticker, "entry_price": entry_price, "count": count,
        "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "dry_run": effective_dry_run,
    }
    _save_state(state)
    result["action"] = "opened"
    result["ticker"] = ticker
    result["entry_price"] = entry_price
    result["count"] = count
    result["reason"] = best["reason"]
    result["order_result"] = order_result
    return result
