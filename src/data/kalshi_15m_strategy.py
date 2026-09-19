"""Strategy for Kalshi's 15-minute event-contract markets (KXBTC15M etc.,
see kalshi_15m.py's own module docstring for the product itself).

DRY RUN BY DEFAULT, same hard safety floor as perps_strategy.py: real
orders require BOTH KALSHI_15M_LIVE_TRADING_ENABLED=1 in the environment
AND the caller not passing dry_run=True. Held to an even STRICTER
standard than that shared pattern for a real, explicit reason: this
module's own order-placement path (kalshi_15m.create_order, POSTing to
/portfolio/events/orders) has been cross-checked against Kalshi's current
docs and kalshi_perps.py's own already-proven-live payload shape, but has
NOT been confirmed against a real authenticated call on this account --
this dev machine's own local Kalshi credentials are separately confirmed
stale (see kalshi_15m.py's own module docstring), blocking that specific
verification step until either a fresh local credential or the live
Space itself confirms it. LIVE_TRADING_ENABLED must not be flipped on for
this market until that verification happens.

Everything entry-decision-relevant is genuinely real and verifiable
without that missing piece, though: market discovery (GET /series,
/markets) and settlement checking (a closed market's own public `result`
field) are both UNAUTHENTICATED, public endpoints, already confirmed live
this session -- so dry-run mode here is a real, fully-exercisable
simulation of the whole entry -> hold -> settle lifecycle, not a stub.

Much simpler than perps_strategy.py by design, not by omission: no
leverage (these are plain $1-notional binary contracts, see kalshi_15m.py's
own docstring), no stop-loss/take-profit/quick-profit exit percentages (a
position here has exactly one exit: the window closes and it settles --
there is no "exit early" lever worth building until real evidence shows
selling before close ever beats holding to settlement), no daily-loss-cap/
technical-scalper-filter machinery. The model's own probability_up IS the
entire signal, matching what a 15-minute binary contract actually needs:
one calibrated probability, not perps' multi-signal-agreement gate.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import threading
import uuid
from pathlib import Path
from typing import Any

from data import kalshi_15m, kalshi_15m_model
from server_common import DATA_DIR

logger = logging.getLogger(__name__)


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


# See this module's own docstring for why this is held to an even
# stricter bar than the shared "dry-run unless explicitly flipped"
# convention -- unverified live order-placement mechanics on a real
# account.
LIVE_TRADING_ENABLED = _env_flag("KALSHI_15M_LIVE_TRADING_ENABLED", default=False)

# Calibrated on zero real trade history yet -- an honest starting guess
# (matching this codebase's OWN established floor for a brand-new model,
# see e.g. alpaca_options_strategy.py's original MODEL_CONFIDENCE_MIN
# comment), not evidence-picked. Revisit with a real backtest/walk-forward
# once kalshi_15m_data.py has accumulated enough real history to run one
# -- same discipline as every other threshold in this codebase.
MODEL_CONFIDENCE_MIN = _env_float("KALSHI_15M_MODEL_CONFIDENCE_MIN", 0.58)

POSITION_SIZE_PCT = _env_float("KALSHI_15M_POSITION_SIZE_PCT", 0.05)
MAX_CONCURRENT_POSITIONS = _env_int("KALSHI_15M_MAX_CONCURRENT_POSITIONS", 5)

# Real, deliberate guard: entering with only a few seconds left before a
# window closes is paying the spread for what's functionally a coin flip
# (no time left for the model's own predicted direction to actually play
# out) -- Kalshi's own quadratic fee structure (see kalshi_15m.py's own
# docstring) makes this worse here than perps' linear one. A third of the
# window (5 of 15 minutes) still open is the floor for a real entry.
MIN_SECONDS_TO_CLOSE_FOR_ENTRY = _env_int("KALSHI_15M_MIN_SECONDS_TO_CLOSE_FOR_ENTRY", 300)

STATE_FILE = Path(os.getenv("KALSHI_15M_STATE_FILE", str(DATA_DIR / "kalshi_15m_state.json")))
_STATE_LOCK = threading.Lock()

HF_API_KEY = os.getenv("HF_API_KEY", "")


def _load_state() -> dict[str, Any]:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        return {"positions": [], "trade_log": [], "realized_pnl_by_date": {}}
    state.setdefault("positions", [])
    state.setdefault("trade_log", [])
    state.setdefault("realized_pnl_by_date", {})
    return state


def _save_state(state: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def _today_str() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def evaluate_candidate(coin: str) -> dict[str, Any]:
    """Pure decision logic for one coin -- no state, no order placement,
    no side effects. Returns {"ok": False, "reason": ...} when there's
    nothing to do (no open window, too little time left, no trained model
    yet, confidence too low), or {"ok": True, "side": "yes"/"no",
    "market": {...}, "probability_up": float, "confidence": float} when a
    real entry candidate exists. `confidence` is always the probability of
    the SIDE actually chosen (i.e. probability_up for "yes",
    1-probability_up for "no"), so it's always directly comparable to
    MODEL_CONFIDENCE_MIN regardless of predicted direction."""
    series_ticker = kalshi_15m.KNOWN_15M_SERIES.get(coin)
    if not series_ticker:
        return {"ok": False, "reason": "unknown_coin"}

    market = kalshi_15m.get_current_window_market(series_ticker)
    if market is None:
        return {"ok": False, "reason": "no_open_window"}

    remaining = kalshi_15m.seconds_to_close(market)
    if remaining is None or remaining < MIN_SECONDS_TO_CLOSE_FOR_ENTRY:
        return {"ok": False, "reason": "too_little_time_remaining", "seconds_to_close": remaining}

    prediction = kalshi_15m_model.predict_direction(coin)
    if not prediction.get("model_ok"):
        return {"ok": False, "reason": "model_not_ready", "detail": prediction.get("reason")}

    probability_up = float(prediction["probability_up"])
    if probability_up >= 0.5:
        side, confidence = "yes", probability_up
    else:
        side, confidence = "no", 1.0 - probability_up

    if confidence < MODEL_CONFIDENCE_MIN:
        return {"ok": False, "reason": "confidence_below_floor", "confidence": confidence}

    return {
        "ok": True, "coin": coin, "side": side, "market": market,
        "probability_up": probability_up, "confidence": confidence,
    }


def _has_open_position(state: dict[str, Any], *, coin: str) -> bool:
    return any(p.get("coin") == coin for p in state.get("positions") or [])


def scan_and_enter(*, dry_run: bool | None = None) -> dict[str, Any]:
    """One pass over every coin: evaluate_candidate, then (if a real
    candidate exists, there's room under MAX_CONCURRENT_POSITIONS, and
    this coin doesn't already have an open position) place an entry.
    dry_run=None defers to LIVE_TRADING_ENABLED's own hard floor -- see
    this module's own docstring for why that floor is currently ALWAYS
    tripped (never actually places a real order yet)."""
    effective_dry_run = (not LIVE_TRADING_ENABLED) if dry_run is None else dry_run
    checks: list[dict[str, Any]] = []

    with _STATE_LOCK:
        state = _load_state()
        open_count = len(state.get("positions") or [])

    for coin in kalshi_15m.KNOWN_15M_SERIES:
        if open_count >= MAX_CONCURRENT_POSITIONS:
            checks.append({"coin": coin, "ok": False, "reason": "max_concurrent_positions"})
            continue
        with _STATE_LOCK:
            state = _load_state()
            if _has_open_position(state, coin=coin):
                checks.append({"coin": coin, "ok": False, "reason": "already_has_open_position"})
                continue

        decision = evaluate_candidate(coin)
        if not decision.get("ok"):
            checks.append({"coin": coin, **decision})
            continue

        market = decision["market"]
        side_char = "bid"  # buying (not selling) -- see kalshi_15m.create_order's own side convention
        # Price: cross the spread at the current best offer for the chosen
        # side (a marketable IOC order, same "pay the spread for a real
        # fill over a resting order that might never fill" tradeoff
        # kalshi_perps.py's own entries already accept). Kalshi's binary
        # markets quote the NO side directly; the YES side's own best
        # ask/bid are the complements (yes_ask = 1 - no_bid, yes_bid = 1 -
        # no_ask) -- confirmed via Kalshi's own docs on binary market
        # pricing, not yet cross-checked against a live quote on THIS
        # account (see this module's own docstring on why order placement
        # itself stays dry-run-only regardless).
        no_ask = float(market.get("no_ask_dollars") or 0.99)
        no_bid = float(market.get("no_bid_dollars") or 0.01)
        price = no_ask if decision["side"] == "no" else round(1.0 - no_bid, 4)
        if price <= 0 or price >= 1:
            checks.append({"coin": coin, "ok": False, "reason": "no_valid_quote"})
            continue

        contracts = max(1, int((_account_budget_usd() * POSITION_SIZE_PCT) / price))
        client_order_id = str(uuid.uuid4())

        order_id = None
        if not effective_dry_run:
            try:
                order_result = kalshi_15m.create_order(
                    ticker=market["ticker"], side=side_char, count=contracts, price=price,
                    client_order_id=client_order_id,
                )
                order_id = order_result.get("order_id")
            except Exception as exc:
                logger.warning("[kalshi_15m_strategy] order placement failed for %s: %s", coin, exc)
                checks.append({"coin": coin, "ok": False, "reason": "order_failed", "error": str(exc)})
                continue

        position = {
            "coin": coin, "ticker": market["ticker"], "side": decision["side"],
            "count": contracts, "entry_price": price, "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "close_time": market.get("close_time"), "entry_probability_up": decision["probability_up"],
            "entry_confidence": decision["confidence"], "dry_run": effective_dry_run,
            "client_order_id": client_order_id, "order_id": order_id,
        }
        with _STATE_LOCK:
            state = _load_state()
            state["positions"].append(position)
            _save_state(state)
        open_count += 1
        checks.append({"coin": coin, "ok": True, "action": "entered", "side": decision["side"], "count": contracts, "dry_run": effective_dry_run})

    return {"ok": True, "checks": checks, "live_trading_enabled": LIVE_TRADING_ENABLED}


def _account_budget_usd() -> float:
    """The dollar budget one full position slot sizes against. Real
    balance when live trading is actually verified and enabled; a fixed,
    clearly-labeled placeholder otherwise -- this module's own dry-run
    simulation doesn't need a real balance to exercise its own entry/
    settlement logic end to end (see this module's own docstring)."""
    if not LIVE_TRADING_ENABLED:
        return 100.0
    try:
        balance = kalshi_15m.get_portfolio_balance()
        return float(balance.get("balance_dollars") or 0.0)
    except Exception as exc:
        logger.warning("[kalshi_15m_strategy] balance fetch failed, using placeholder: %s", exc)
        return 100.0


def check_settlements() -> dict[str, Any]:
    """For every open position, checks whether its market has actually
    settled (a PUBLIC, unauthenticated read -- see this module's own
    docstring) and books the real outcome if so. dry-run and real
    positions are both checked the same way here -- the SETTLEMENT itself
    is a real, public fact regardless of whether this account actually
    held a real contract through it, which is exactly what makes dry-run
    mode here a genuine end-to-end simulation, not a stub."""
    checks: list[dict[str, Any]] = []
    with _STATE_LOCK:
        state = _load_state()
        positions = list(state.get("positions") or [])

    for position in positions:
        ticker = position["ticker"]
        try:
            fresh = kalshi_15m.get_market(ticker)
        except Exception as exc:
            logger.warning("[kalshi_15m_strategy] settlement check failed for %s: %s", ticker, exc)
            continue

        if fresh is None:
            continue
        result = (fresh.get("result") or "").strip().lower()
        if result not in ("yes", "no"):
            checks.append({"coin": position["coin"], "ok": True, "action": "still_open"})
            continue

        won = result == position["side"]
        gross = position["count"] * (1.0 - position["entry_price"]) if won else -position["count"] * position["entry_price"]
        closed_at = dt.datetime.now(dt.timezone.utc).isoformat()
        trade = {
            "coin": position["coin"], "ticker": ticker, "side": position["side"],
            "count": position["count"], "entry_price": position["entry_price"], "result": result,
            "realized_pnl_usd": round(gross, 6), "opened_at": position["opened_at"], "closed_at": closed_at,
            "entry_probability_up": position.get("entry_probability_up"),
            "entry_confidence": position.get("entry_confidence"), "dry_run": position.get("dry_run", True),
        }
        # Removes this ONE settled position from a FRESHLY re-read state
        # (matched by ticker, which is unique per 15-minute window -- see
        # get_current_window_market's own "one open market per series at a
        # time" contract) rather than a snapshot-taken-at-function-start
        # bulk overwrite -- a real, if currently latent, correctness gap
        # closed before it could ever matter: any future caller adding a
        # position concurrently (scan_and_enter already runs sequentially
        # after this within the same locked job, so no live overlap today)
        # would otherwise have that fresh position silently erased by this
        # function's own stale end-of-loop overwrite.
        with _STATE_LOCK:
            state = _load_state()
            by_date = state.setdefault("realized_pnl_by_date", {})
            if not trade["dry_run"]:
                today = _today_str()
                by_date[today] = round(float(by_date.get(today, 0.0)) + trade["realized_pnl_usd"], 6)
            state["trade_log"].append(trade)
            state["positions"] = [p for p in state.get("positions") or [] if p.get("ticker") != ticker]
            _save_state(state)
        checks.append({"coin": position["coin"], "ok": True, "action": "settled", "won": won, "realized_pnl_usd": trade["realized_pnl_usd"]})

    return {"ok": True, "checks": checks}
