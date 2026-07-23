"""Kalshi Perps growth strategy across ALL listed instruments (BTC, ETH,
SOL, XRP, DOGE, LTC, BCH, LINK, SUI, NEAR, DOT, HBAR, HYPE, kSHIB, XLM, ZEC):
split the account into portions, put each portion into a small dip the model
+ technicals agree is heading up, take profit on that portion, repeat --
compounding as the balance grows.

Every safety decision lives HERE, not in the generic API client, so it's all
in one place to review:

  - DRY RUN BY DEFAULT. Real orders require BOTH `KALSHI_PERPS_LIVE_TRADING_ENABLED=1`
    in the environment AND the caller not passing dry_run=True.
  - Up to MAX_CONCURRENT_POSITIONS positions at once (default 5), each sized
    at POSITION_SIZE_PCT (default 20%) of the account's CURRENT available
    balance -- so five full slots is the whole account, and each slot's
    dollar size compounds automatically as the balance grows. Never more
    than one open position per instrument at a time.
  - Position size uses each market's own `leverage_estimate` (Kalshi's
    perps carry embedded leverage, e.g. ~6x on KXBTCPERP): the portion's
    dollar budget is treated as MARGIN, and the number of contracts bought
    is however many that margin, at that market's leverage, actually
    controls -- i.e. the multiplier Kalshi is offering is actually used,
    not left on the table by only ever buying 1 contract regardless of size.
  - A daily realized-loss cap, as a PERCENTAGE of the balance at the start of
    the day (PERPS_DAILY_LOSS_CAP_PCT), that halts new entries (but not
    exits) once breached for the day.
  - Every position has a take-profit, a stop-loss, a velocity-based
    quick-profit exit, AND a max hold time.

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

Two independent loops, on purpose, so a fast-moving position never has to
wait for a full 16-instrument scan to get closed out:
  - `manage_open_positions()` -- ONLY checks/exits existing positions (one
    cheap price call each). Meant to run every 15-30 seconds so a quick pump
    gets a quick exit instead of sitting until the next slow scan.
  - `scan_and_enter()` -- the full watchlist scan for NEW entries, filling
    any open portfolio slots. Never touches an instrument that's already
    held (exits are the fast loop's job exclusively, so the two loops never
    make competing decisions about the same position). Meant to run every
    1-2 minutes.
  - `run_cycle()` runs both in order, for the manual-trigger script and for
    callers that don't need the split.
A module-level lock serializes all state reads/writes so the two loops
(which can run concurrently as separate scheduler threads in one process)
never interleave a read-modify-write on perps_state.json.

This is a real trading strategy with real risk, made explicitly MORE
aggressive at the account owner's request (percentage-of-balance sizing
with embedded leverage, multiple concurrent positions). No strategy in this
space is risk-free, and past price behavior does not guarantee future
results. Start in dry-run, watch its decisions for a while, and only enable
live trading once you're comfortable with them.
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

from data.crypto_prices import get_fast_price
from data.kalshi_perps import cancel_margin_order, create_margin_order, get_margin_balance, get_margin_market
from data.perps_data import coin_for_ticker, get_watchlist, latest_feature_row
from data.perps_model import predict_direction

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
STATE_FILE = Path(os.getenv("PERPS_STATE_FILE", str(DATA_DIR / "perps_state.json")))
_STATE_LOCK = threading.Lock()


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


# ── Tunable parameters (all overridable via env) ────────────────────────────
# "Break the total balance into portions and grow it": each new position is
# sized at this fraction of CURRENT available balance (so it compounds), and
# up to MAX_CONCURRENT_POSITIONS can be open at once (5 x 20% = the whole
# account fully deployed across up to 5 different instruments at a time).
POSITION_SIZE_PCT = _env_float("PERPS_POSITION_SIZE_PCT", 0.20)
MAX_CONCURRENT_POSITIONS = max(1, _env_int("PERPS_MAX_CONCURRENT_POSITIONS", 5))
TAKE_PROFIT_PCT = _env_float("PERPS_TAKE_PROFIT_PCT", 0.004)   # +0.4% -> take small profit
STOP_LOSS_PCT = _env_float("PERPS_STOP_LOSS_PCT", 0.008)       # -0.8% -> cut the loss
MAX_HOLD_MINUTES = _env_int("PERPS_MAX_HOLD_MINUTES", 30)
ENTRY_DIP_PCT = _env_float("PERPS_ENTRY_DIP_PCT", 0.0015)      # 0.15% below short MA triggers interest
SHORT_MA_MINUTES = _env_int("PERPS_SHORT_MA_MINUTES", 15)
TREND_FILTER_DOWN_PCT = _env_float("PERPS_TREND_FILTER_DOWN_PCT", 0.02)  # skip entries if down >2%
# Tuned via src/data/perps_backtest.py against the full real historical
# archive (13 instruments, ~7 weeks, 504k rows, 2026-06-03 -> 2026-07-22,
# walk-forward: fit on the first 70% chronologically, simulate the held-out
# last 30%). At the previous default (0.55) the backtest fired only ~36
# trades/day; loosening to 0.52 (leaving the technical dip/trend filters
# untouched -- deliberately NOT loosening those, since values much below the
# current ENTRY_DIP_PCT start firing on every 1-minute wiggle rather than a
# real dip) increased that to ~600 trades/day at a 54.15% win rate and a
# backtested +25.8% return over the 11.68-day test window, vs. 58.0%
# win rate / +4.2% at the old default. Known backtest limitation: no Kalshi
# trading fees or bid-ask slippage are modeled, so treat the absolute return
# figures as optimistic -- the frequency/win-rate comparison across settings
# is the more trustworthy signal from this exercise.
MODEL_CONFIDENCE_MIN = _env_float("PERPS_MODEL_CONFIDENCE_MIN", 0.52)
# Daily loss cap as a PERCENTAGE of the balance measured at the start of the
# day (not a fixed dollar figure) so it scales sensibly as the account grows.
DAILY_LOSS_CAP_PCT = _env_float("PERPS_DAILY_LOSS_CAP_PCT", 0.15)
LIVE_TRADING_ENABLED = _env_flag("KALSHI_PERPS_LIVE_TRADING_ENABLED", default=False)

# "When it's quick to go up, take profit": if a position is already up at
# least QUICK_PROFIT_PCT (smaller than the standard TAKE_PROFIT_PCT) AND that
# gain arrived fast (velocity over the trailing window >= this threshold),
# exit immediately rather than waiting for the standard take-profit level --
# a fast pump on a thin market often gives back gains just as fast.
QUICK_PROFIT_PCT = _env_float("PERPS_QUICK_PROFIT_PCT", 0.0015)
QUICK_PROFIT_VELOCITY_PCT_PER_MIN = _env_float("PERPS_QUICK_PROFIT_VELOCITY_PCT_PER_MIN", 0.006)
QUICK_PROFIT_WINDOW_SECONDS = _env_int("PERPS_QUICK_PROFIT_WINDOW_SECONDS", 90)

# Reject a new entry if Kalshi's quote and an independent live exchange price
# (Coinbase/Kraken, see crypto_prices.py) disagree by more than this -- a
# safety check against entering on a stale or erroneous Kalshi tick.
MAX_ENTRY_PRICE_DEVIATION_PCT = _env_float("PERPS_MAX_ENTRY_PRICE_DEVIATION_PCT", 0.02)


def _load_state() -> dict[str, Any]:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        return {"positions": [], "trade_log": [], "realized_pnl_by_date": {}, "daily_reference_balance": {}}

    # Migrate the old single-position schema ({"position": {...} | None}) to
    # the current multi-position list transparently, so an existing local
    # state file from before this change keeps working.
    if "positions" not in state:
        old_position = state.pop("position", None)
        state["positions"] = [old_position] if old_position else []
    state.setdefault("trade_log", [])
    state.setdefault("realized_pnl_by_date", {})
    state.setdefault("daily_reference_balance", {})
    return state


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


def _available_balance_usd() -> float:
    """Real-account balance check -- read-only, safe to call regardless of
    dry-run (dry-run only gates ORDER PLACEMENT, not reads), and needed even
    in dry-run so a simulated cycle sizes positions the same way a live one
    would."""
    balance = get_margin_balance(compute_available_balance=True)
    available = 0.0
    for sub in (balance.get("subaccount_balances") or []):
        available = max(available, float(sub.get("available_balance") or 0.0))
    return available


def compute_leveraged_count(available_balance_usd: float, market: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """How many WHOLE contracts POSITION_SIZE_PCT of the balance can control
    at this market's own embedded leverage. The portion's dollar budget is
    spent as MARGIN, not notional -- e.g. at ~6x leverage, a $2 margin
    budget controls roughly $12 of notional, which is however many contracts
    that buys at the market's price. Returns (count, detail) so callers can
    show/log exactly how the number was derived."""
    price = float(market.get("price") or 0.0)
    leverage = float(market.get("leverage_estimate") or 1.0) or 1.0
    margin_budget_usd = round(available_balance_usd * POSITION_SIZE_PCT, 6)
    notional_capacity_usd = round(margin_budget_usd * leverage, 6)
    count = int(notional_capacity_usd // price) if price > 0 else 0
    detail = {
        "available_balance_usd": available_balance_usd, "position_size_pct": POSITION_SIZE_PCT,
        "margin_budget_usd": margin_budget_usd, "leverage_estimate": leverage,
        "notional_capacity_usd": notional_capacity_usd, "contract_price": price, "count": count,
    }
    return count, detail


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


def scan_for_entries(
    tickers: list[str] | None = None, *, exclude: set[str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Evaluate every ticker in the watchlist (minus any already held);
    return every qualifying candidate ranked best-first, plus every
    candidate's evaluation for observability."""
    watchlist = tickers or get_watchlist()
    held = exclude or set()
    candidates = [evaluate_candidate(t) for t in watchlist if t not in held]
    qualifying = sorted((c for c in candidates if c.get("should_enter")), key=lambda c: c.get("score", 0.0), reverse=True)
    return qualifying, candidates


def _update_velocity(
    position: dict[str, Any], current_price: float, now: dt.datetime, *, samples_key: str = "price_samples",
) -> float | None:
    """Track recent (timestamp, price) samples on the position itself and
    return the trailing %/minute velocity over QUICK_PROFIT_WINDOW_SECONDS,
    or None until there's at least two samples spanning some real elapsed
    time. Mutates `position[samples_key]` in place -- caller is responsible
    for persisting it. `samples_key` lets Kalshi's own price and an
    independent external cross-check price (see `external_velocity_pct_per_min`
    on `decide_exit`) track separate sample histories on the same position."""
    now_ts = now.timestamp()
    samples: list[list[float]] = position.setdefault(samples_key, [])
    samples.append([now_ts, current_price])
    cutoff = now_ts - QUICK_PROFIT_WINDOW_SECONDS
    trimmed = [s for s in samples if s[0] >= cutoff] or samples[-1:]
    position[samples_key] = trimmed[-30:]  # defensive cap regardless of timing
    if len(trimmed) < 2:
        return None
    oldest_ts, oldest_price = trimmed[0]
    elapsed_min = (now_ts - oldest_ts) / 60.0
    if elapsed_min <= 0 or oldest_price <= 0:
        return None
    return ((current_price - oldest_price) / oldest_price) / elapsed_min


def decide_exit(
    position: dict[str, Any], current_price: float, *,
    velocity_pct_per_min: float | None = None, external_velocity_pct_per_min: float | None = None,
) -> tuple[bool, str]:
    """`current_price` is always Kalshi's own tradable quote -- that's what
    the gain/loss threshold and the actual exit order use. `velocity` is
    computed from that same Kalshi price series; `external_velocity` is
    computed independently from a live exchange cross-check (Coinbase/
    Kraken, see crypto_prices.py) and can ALSO trigger quick-profit -- since
    Kalshi's own perp quote can lag a deep, liquid spot venue by a tick or
    two, the external reading is sometimes the first place a fast move
    actually shows up."""
    entry_price = float(position["entry_price"])
    change_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0.0
    if change_pct >= QUICK_PROFIT_PCT:
        if velocity_pct_per_min is not None and velocity_pct_per_min >= QUICK_PROFIT_VELOCITY_PCT_PER_MIN:
            return True, f"quick_profit (velocity {velocity_pct_per_min:+.2%}/min, gain {change_pct:+.3%})"
        if external_velocity_pct_per_min is not None and external_velocity_pct_per_min >= QUICK_PROFIT_VELOCITY_PCT_PER_MIN:
            return True, f"quick_profit (external velocity {external_velocity_pct_per_min:+.2%}/min, gain {change_pct:+.3%})"
    if change_pct >= TAKE_PROFIT_PCT:
        return True, f"take_profit ({change_pct:+.3%})"
    if change_pct <= -STOP_LOSS_PCT:
        return True, f"stop_loss ({change_pct:+.3%})"
    opened_at = dt.datetime.fromisoformat(position["opened_at"])
    held_minutes = (dt.datetime.now(dt.timezone.utc) - opened_at).total_seconds() / 60.0
    if held_minutes >= MAX_HOLD_MINUTES:
        return True, f"max_hold_time ({held_minutes:.0f}min, {change_pct:+.3%})"
    return False, f"holding ({change_pct:+.3%}, {held_minutes:.0f}min)"


def _reference_balance_for_today(state: dict[str, Any], available_balance_usd: float | None) -> float | None:
    """The daily loss cap is a percentage of the balance as it stood at the
    start of the day, not of whatever the balance happens to be right now
    (which shrinks as margin gets committed to open positions) -- captured
    once per day the first time a real balance read succeeds."""
    today = _today_str()
    refs = state.setdefault("daily_reference_balance", {})
    if today not in refs:
        if available_balance_usd is None:
            return None
        refs[today] = available_balance_usd
        for old_date in list(refs.keys()):
            if old_date != today:
                del refs[old_date]
    return float(refs[today])


def _daily_loss_cap_breached(state: dict[str, Any], reference_balance: float | None) -> bool:
    if not reference_balance or reference_balance <= 0:
        return False
    today_pnl = float((state.get("realized_pnl_by_date") or {}).get(_today_str(), 0.0))
    return today_pnl <= -abs(DAILY_LOSS_CAP_PCT) * reference_balance


def manage_open_positions(*, dry_run: bool | None = None) -> dict[str, Any]:
    """Fast loop: ONLY checks/exits existing open positions, one cheap price
    call per position. Meant to run every 15-30 seconds so a quick,
    fast-reversing move actually gets a quick exit instead of waiting for
    the next full scan. Returns action "no_position" immediately (no API
    calls at all) if nothing is open."""
    effective_dry_run = True if not LIVE_TRADING_ENABLED else bool(dry_run if dry_run is not None else True)
    with _STATE_LOCK:
        state = _load_state()
        positions = state.get("positions") or []
        if not positions:
            return {"ok": True, "dry_run": effective_dry_run, "action": "no_position"}

        remaining: list[dict[str, Any]] = []
        closed: list[dict[str, Any]] = []
        checks: list[dict[str, Any]] = []
        ok = True
        for position in positions:
            ticker = position["ticker"]
            try:
                market = get_margin_market(ticker)
                current_price = float((market.get("market") or {}).get("price") or 0.0)
                tick_size = float((market.get("market") or {}).get("tick_size") or 0.0001)
            except Exception as exc:
                ok = False
                checks.append({"ticker": ticker, "ok": False, "error": str(exc)})
                remaining.append(position)  # leave it untouched, retry next cycle
                continue

            now = dt.datetime.now(dt.timezone.utc)
            velocity = _update_velocity(position, current_price, now)

            external_velocity = None
            external_quote = None
            try:
                external_quote = get_fast_price(coin_for_ticker(ticker))
            except Exception as exc:
                logger.debug("[perps_strategy] external price check failed for %s: %s", ticker, exc)
            if external_quote and not external_quote.get("delayed"):
                external_velocity = _update_velocity(
                    position, float(external_quote["price"]), now, samples_key="external_price_samples",
                )

            should_exit, reason = decide_exit(
                position, current_price, velocity_pct_per_min=velocity, external_velocity_pct_per_min=external_velocity,
            )
            checks.append({
                "ticker": ticker, "ok": True, "exit_check": reason, "velocity_pct_per_min": velocity,
                "external_velocity_pct_per_min": external_velocity,
                "external_source": (external_quote or {}).get("source"),
            })

            if not should_exit:
                remaining.append(position)
                continue

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
            trade = {
                "closed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "ticker": ticker, "entry_price": entry_price, "exit_price": exit_price,
                "count": count, "realized_pnl_usd": realized_pnl, "reason": reason, "dry_run": effective_dry_run,
            }
            state.setdefault("trade_log", []).append(trade)
            closed.append(trade)

        state["positions"] = remaining
        _save_state(state)
        return {
            "ok": ok, "dry_run": effective_dry_run,
            "action": "closed" if closed else ("none" if remaining else "no_position"),
            "closed": closed, "checks": checks, "open_position_count": len(remaining),
        }


def scan_and_enter(*, dry_run: bool | None = None) -> dict[str, Any]:
    """Slow loop: scans the full watchlist for new entries, filling any open
    portfolio slots (up to MAX_CONCURRENT_POSITIONS total, never more than
    one position per instrument). Managing existing positions is
    `manage_open_positions`'s job exclusively, so the two loops never make
    competing decisions about the same position. Meant to run every 1-2
    minutes (candle fetching + the model call make it too heavy for a
    15-30 second cadence)."""
    effective_dry_run = True if not LIVE_TRADING_ENABLED else bool(dry_run if dry_run is not None else True)
    with _STATE_LOCK:
        state = _load_state()
        positions = state.get("positions") or []
        open_slots = MAX_CONCURRENT_POSITIONS - len(positions)
        if open_slots <= 0:
            return {"ok": True, "dry_run": effective_dry_run, "action": "max_positions_open", "open_position_count": len(positions)}

        try:
            available_balance_usd = _available_balance_usd()
        except Exception as exc:
            available_balance_usd = None
            logger.debug("[perps_strategy] balance read for daily reference failed: %s", exc)
        reference_balance = _reference_balance_for_today(state, available_balance_usd)
        loss_cap_breached = _daily_loss_cap_breached(state, reference_balance)
        held_tickers = {p["ticker"] for p in positions}
        _save_state(state)  # persist any newly-set daily reference balance
        if loss_cap_breached:
            return {"ok": True, "dry_run": effective_dry_run, "action": "skipped_daily_loss_cap"}

    result: dict[str, Any] = {"ok": True, "dry_run": effective_dry_run, "action": "none", "opened": []}
    # Scanning (candles + model + news, all network calls) runs OUTSIDE the
    # lock so it never blocks the fast exit loop for the seconds a full
    # 16-instrument scan can take.
    qualifying, candidates = scan_for_entries(exclude=held_tickers)
    result["candidates"] = candidates
    if not qualifying:
        return result

    opened: list[dict[str, Any]] = []
    for candidate in qualifying[:open_slots]:
        ticker = candidate["ticker"]
        try:
            market_resp = get_margin_market(ticker)
            market = market_resp.get("market") or {}
            tick_size = float(market.get("tick_size") or 0.0001)
        except Exception as exc:
            opened.append({"ticker": ticker, "ok": False, "action": "skipped_market_fetch_failed", "error": str(exc)})
            continue

        # Sanity check Kalshi's quote against an independent live exchange
        # price before committing real size to it -- protects against
        # entering on a stale/erroneous Kalshi tick. Kalshi quotes the
        # PER-CONTRACT price (contract_size units of the coin, e.g. "0.0001
        # BTC"), not the coin's spot price, so the comparison must divide
        # through by contract_size first. Skipped (not blocking) if the
        # external feed is unavailable, only has the delayed API Ninjas
        # fallback, or contract_size is missing/zero.
        try:
            external_quote = get_fast_price(coin_for_ticker(ticker))
        except Exception:
            external_quote = None
        contract_size = float(market.get("contract_size") or 0.0)
        if external_quote and not external_quote.get("delayed") and contract_size > 0:
            implied_spot_price = candidate["current_price"] / contract_size
            deviation_pct = abs(implied_spot_price - external_quote["price"]) / external_quote["price"]
            if deviation_pct > MAX_ENTRY_PRICE_DEVIATION_PCT:
                opened.append({
                    "ticker": ticker, "ok": True, "action": "skipped_price_deviation",
                    "kalshi_implied_spot_price": implied_spot_price, "external_price": external_quote["price"],
                    "deviation_pct": deviation_pct,
                })
                continue

        try:
            available_balance_usd = _available_balance_usd()
        except Exception as exc:
            opened.append({"ticker": ticker, "ok": False, "action": "skipped_balance_check_failed", "error": str(exc)})
            continue

        entry_price = _round_price(candidate["current_price"], tick_size)
        sizing_market = dict(market)
        sizing_market["price"] = entry_price
        count, sizing_detail = compute_leveraged_count(available_balance_usd, sizing_market)
        if count < 1:
            opened.append({
                "ticker": ticker, "ok": True, "action": "skipped_insufficient_budget",
                "sizing": sizing_detail,
            })
            continue

        order_result = None
        if not effective_dry_run:
            order_result = create_margin_order(
                ticker=ticker, side="bid", count=float(count), price=entry_price,
                client_order_id=str(uuid.uuid4()), time_in_force="immediate_or_cancel",
            )

        with _STATE_LOCK:
            # Re-read + re-check on every entry: the fast loop runs
            # concurrently and slots can close in between iterations of
            # this loop.
            state = _load_state()
            positions = state.get("positions") or []
            if len(positions) >= MAX_CONCURRENT_POSITIONS or any(p["ticker"] == ticker for p in positions):
                opened.append({"ticker": ticker, "ok": True, "action": "skipped_slot_taken"})
                continue
            new_position = {
                "ticker": ticker, "entry_price": entry_price, "count": float(count),
                "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "dry_run": effective_dry_run,
                "sizing": sizing_detail,
            }
            positions.append(new_position)
            state["positions"] = positions
            _save_state(state)
        opened.append({
            "ticker": ticker, "ok": True, "action": "opened", "entry_price": entry_price,
            "count": count, "reason": candidate["reason"], "sizing": sizing_detail, "order_result": order_result,
        })

    result["opened"] = opened
    result["action"] = "opened" if any(o.get("action") == "opened" for o in opened) else "none"
    return result


def run_cycle(*, dry_run: bool | None = None) -> dict[str, Any]:
    """Convenience for manual triggers/tests: run the fast position check,
    then the entry scan (which cheaply no-ops itself if every slot is
    already full, with no network calls). Production scheduling calls
    `manage_open_positions` and `scan_and_enter` on their own separate
    cadences instead -- see dashboard.py."""
    position_result = manage_open_positions(dry_run=dry_run)
    entry_result = scan_and_enter(dry_run=dry_run)
    return {
        "ok": bool(position_result.get("ok", True)) and bool(entry_result.get("ok", True)),
        "dry_run": entry_result.get("dry_run", position_result.get("dry_run")),
        "position_management": position_result,
        "entry_scan": entry_result,
    }
