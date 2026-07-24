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
import statistics
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from data.crypto_prices import get_fast_price
from data.kalshi_perps import (
    cancel_margin_order, create_margin_order, get_margin_balance, get_margin_market, get_margin_positions,
)
from data.perps_data import coin_for_ticker, get_watchlist, latest_feature_row
from data.perps_model import predict_direction

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
STATE_FILE = Path(os.getenv("PERPS_STATE_FILE", str(DATA_DIR / "perps_state.json")))
_STATE_LOCK = threading.Lock()

# Render's free web service plan has NO persistent disk -- every restart
# (a redeploy, a platform-triggered restart, anything) boots from a
# completely fresh filesystem, wiping STATE_FILE entirely. Open positions
# survive this fine (see _reconcile_positions_with_exchange -- Kalshi's own
# /margin/positions is ground truth), but trade_log/realized_pnl_by_date/
# daily_reference_balance have no such ground truth to recover from: without
# a backup, a restart silently resets today's realized P&L to zero AND
# resets the daily loss cap's reference point to whatever the balance
# happens to be post-restart -- meaning a real loss already taken before a
# restart could be forgotten, letting cumulative same-day losses exceed the
# intended DAILY_LOSS_CAP_PCT after multiple restarts. These fields get
# mirrored to the HF model repo (small JSON, not the market dataset) and
# pulled back on a cold start, same durability pattern already used for the
# trained model itself.
HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_MODEL_REPO = os.getenv("HF_MODEL_REPO", "papylove/kalshi-perps-model")
_DURABLE_STATE_HF_FILENAME = "perps_durable_state.json"
_DURABLE_PUSH_MIN_INTERVAL_SEC = 30  # avoid HF's stricter per-commit rate limit on back-to-back pushes
_last_durable_push_ts = 0.0


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

# "When it's choppy/fast, take the smaller sure thing": if the position's
# OWN recent price samples (the same ones _update_velocity already tracks --
# no extra API call) show stdev-of-returns above this threshold, the market
# is currently volatile, and a volatile move can reverse just as fast as it
# arrived. In that regime, take profit at a SMALLER gain than the normal
# quick-profit level rather than holding out for the full TAKE_PROFIT_PCT.
HIGH_VOLATILITY_THRESHOLD = _env_float("PERPS_HIGH_VOLATILITY_THRESHOLD", 0.002)
VOLATILITY_QUICK_PROFIT_PCT = _env_float("PERPS_VOLATILITY_QUICK_PROFIT_PCT", 0.001)

# Reject a new entry if Kalshi's quote and an independent live exchange price
# (Coinbase/Kraken, see crypto_prices.py) disagree by more than this -- a
# safety check against entering on a stale or erroneous Kalshi tick.
MAX_ENTRY_PRICE_DEVIATION_PCT = _env_float("PERPS_MAX_ENTRY_PRICE_DEVIATION_PCT", 0.02)

# Default OFF: the strategy has only ever gone long in production. Shorting
# is a materially different risk shape (a short loses on a RISING price
# instead of a falling one) that has never run live on this account, so it
# gets its own explicit opt-in rather than turning on the moment this code
# ships -- same "start conservative, prove it out, then enable" posture as
# LIVE_TRADING_ENABLED itself. When on, entries can go either direction:
# LONG on a small dip + model predicting up, SHORT on a small rally + model
# predicting down (mirrored technical + model gate, see decide_entry_technical
# and evaluate_candidate). Every take-profit/stop-loss/quick-profit/max-hold
# exit rule applies symmetrically to both, just measuring gain/loss in the
# direction that's actually favorable for that position's side.
ENABLE_SHORTS = _env_flag("PERPS_ENABLE_SHORTS", default=False)


def _durable_state_slice(state: dict[str, Any]) -> dict[str, Any]:
    """The parts of state that can't be recovered from Kalshi's own account
    (open positions can -- see _reconcile_positions_with_exchange)."""
    return {
        "trade_log": state.get("trade_log") or [],
        "realized_pnl_by_date": state.get("realized_pnl_by_date") or {},
        "daily_reference_balance": state.get("daily_reference_balance") or {},
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
                repo_id=HF_MODEL_REPO, repo_type="model", commit_message="update perps durable state",
            )
        finally:
            os.unlink(tmp_path)
    except Exception as exc:
        logger.warning("[perps_strategy] durable state push to HF failed: %s", exc)


def _pull_durable_state_from_hf() -> dict[str, Any] | None:
    if not HF_API_KEY:
        return None
    try:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(
            repo_id=HF_MODEL_REPO, filename=_DURABLE_STATE_HF_FILENAME, repo_type="model", token=HF_API_KEY,
        )
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        logger.info("[perps_strategy] no durable state on HF yet (or fetch failed): %s", exc)
        return None


def _load_state() -> dict[str, Any]:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        # No local file -- either a genuine cold start or (far more likely
        # on Render's free tier, which has no persistent disk) a fresh
        # instance after any restart. Recover trade history / today's
        # realized P&L / the daily loss cap's reference balance from HF
        # rather than silently starting them all at zero -- see the module-
        # level comment above HF_API_KEY for why that matters.
        base = {"positions": [], "trade_log": [], "realized_pnl_by_date": {}, "daily_reference_balance": {}}
        durable = _pull_durable_state_from_hf()
        if durable:
            base.update(durable)
            logger.info("[perps_strategy] recovered durable state from HF after local state was missing")
        return base

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


def _save_state(state: dict[str, Any], *, push_durable: bool = False) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    if not push_durable:
        return
    # Throttled, not on every call -- push_durable is only ever passed True
    # on genuinely infrequent events (a trade just closed, or a new day's
    # reference balance was just captured), but the guard is cheap insurance
    # against HF's stricter per-commit rate limit if those ever coincide.
    global _last_durable_push_ts
    now = time.time()
    if now - _last_durable_push_ts >= _DURABLE_PUSH_MIN_INTERVAL_SEC:
        _last_durable_push_ts = now
        _push_durable_state_to_hf(state)


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


def decide_entry_technical(row: dict[str, Any], side: str = "long") -> tuple[bool, str]:
    """The scalper filter alone: trend + local dip/rally. `row` needs
    current_price, short_ma, trend_pct (as returned by
    perps_data.latest_feature_row / perps_model.predict_direction).

    `side="long"` (default, and the only mode ever run live to date): skip
    entries in a strong downtrend, then look for price sitting a bit BELOW
    the short moving average (a small dip -- contrarian, expecting a bounce).

    `side="short"`: the mirror image -- skip entries in a strong uptrend,
    then look for price sitting a bit ABOVE the short moving average (a
    small rally -- contrarian, expecting a pullback). dip_pct and rally_pct
    have opposite signs off the same MA, so long and short conditions can
    never both be true at once for the same row."""
    if side == "short":
        if row["trend_pct"] > TREND_FILTER_DOWN_PCT:
            return False, f"trend filter: up {row['trend_pct']:.2%}, skipping short entries"
        rally_pct = (row["current_price"] - row["short_ma"]) / row["short_ma"] if row["short_ma"] > 0 else 0.0
        if rally_pct >= ENTRY_DIP_PCT:
            return True, f"price {rally_pct:.3%} above {SHORT_MA_MINUTES}-min average -- small rally"
        return False, f"no rally signal (price {rally_pct:+.3%} vs short MA)"

    if row["trend_pct"] < -TREND_FILTER_DOWN_PCT:
        return False, f"trend filter: down {row['trend_pct']:.2%}, skipping entries"
    dip_pct = (row["short_ma"] - row["current_price"]) / row["short_ma"] if row["short_ma"] > 0 else 0.0
    if dip_pct >= ENTRY_DIP_PCT:
        return True, f"price {dip_pct:.3%} below {SHORT_MA_MINUTES}-min average -- small dip"
    return False, f"no dip signal (price {dip_pct:+.3%} vs short MA)"


def evaluate_candidate(ticker: str) -> dict[str, Any]:
    """Combine the technical scalper filter with the direction model for one
    ticker, considering a LONG entry (dip + model predicts up) and, if
    ENABLE_SHORTS is on, a SHORT entry (rally + model predicts down) --
    mutually exclusive by construction (see decide_entry_technical), so at
    most one can qualify. If no trained model exists yet, the technical
    signal alone decides (clearly flagged) -- long side only, since shorting
    without any model confirmation at all is a materially different risk to
    take on unconfirmed technicals."""
    row = latest_feature_row(ticker)
    if row is None:
        return {"ticker": ticker, "should_enter": False, "reason": "no_feature_data", "model_ok": False, "technical_ok": False}

    prediction = predict_direction(ticker)
    model_ok = bool(prediction.get("model_ok"))

    result: dict[str, Any] = {
        "ticker": ticker, "current_price": row["current_price"], "short_ma": row["short_ma"],
        "trend_pct": row["trend_pct"], "model_ok": model_ok, "technical_ok": False,
    }
    if model_ok:
        result["probability_up"] = prediction["probability_up"]
        result["model_direction"] = prediction["direction"]

    reasons = []
    for side in (("long", "short") if ENABLE_SHORTS else ("long",)):
        technical_ok, technical_reason = decide_entry_technical(row, side=side)
        result[f"{side}_technical_ok"] = technical_ok
        # Set as soon as EITHER side's filter passes, before any early
        # return below -- the dashboard reads this flat field directly, and
        # it must reflect reality regardless of which branch returns.
        if technical_ok:
            result["technical_ok"] = True
        if not technical_ok:
            reasons.append(technical_reason)
            continue

        if not model_ok:
            if side == "short":
                # Shorting on technicals alone (no model to confirm the
                # direction at all yet) is not a risk worth taking.
                reasons.append(f"{technical_reason} (model not trained yet -- shorts require model confirmation)")
                continue
            result["should_enter"] = True
            result["side"] = side
            result["reason"] = f"{technical_reason} (model not trained yet -- technical-only fallback)"
            result["score"] = ENTRY_DIP_PCT + (row["short_ma"] - row["current_price"]) / row["short_ma"]
            return result

        wanted_direction = "up" if side == "long" else "down"
        confidence = prediction["probability_up"] if side == "long" else (1.0 - prediction["probability_up"])
        if prediction["direction"] == wanted_direction and confidence >= MODEL_CONFIDENCE_MIN:
            result["should_enter"] = True
            result["side"] = side
            result["reason"] = f"{technical_reason}; model predicts {wanted_direction} (p={confidence:.2f})"
            result["score"] = confidence
            return result
        reasons.append(f"{technical_reason}, but model predicts {prediction['direction']} (p_up={prediction['probability_up']:.2f})")

    result["should_enter"] = False
    result["reason"] = " | ".join(reasons) if reasons else "no dip or rally signal"
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


def _sample_volatility(samples: list[list[float]]) -> float | None:
    """Stdev of consecutive-sample percent changes within the position's own
    existing rolling price-sample window (see _update_velocity) -- reuses
    data already being collected rather than an extra API call or a heavier
    latest_feature_row() fetch, which would defeat the point of the fast
    loop being cheap. None until there are at least 3 samples (2 changes)."""
    if len(samples) < 3:
        return None
    prices = [p for _, p in samples]
    changes = [
        (prices[i] - prices[i - 1]) / prices[i - 1]
        for i in range(1, len(prices)) if prices[i - 1] > 0
    ]
    if len(changes) < 2:
        return None
    return statistics.stdev(changes)


def decide_exit(
    position: dict[str, Any], current_price: float, *,
    velocity_pct_per_min: float | None = None, external_velocity_pct_per_min: float | None = None,
    current_volatility: float | None = None,
) -> tuple[bool, str]:
    """`current_price` is always Kalshi's own tradable quote -- that's what
    the gain/loss threshold and the actual exit order use. `velocity` is
    computed from that same Kalshi price series; `external_velocity` is
    computed independently from a live exchange cross-check (Coinbase/
    Kraken, see crypto_prices.py) and can ALSO trigger quick-profit -- since
    Kalshi's own perp quote can lag a deep, liquid spot venue by a tick or
    two, the external reading is sometimes the first place a fast move
    actually shows up.

    `position["side"]` defaults to "long" (every position before shorts
    existed, and every position from a deployment with ENABLE_SHORTS off,
    has no `side` key at all). For a short, a FALLING price is the
    favorable direction, so change_pct and the velocity readings are both
    sign-flipped before applying the exact same thresholds -- the take-
    profit/stop-loss/quick-profit/max-hold RULES are identical for both
    sides, only which price direction counts as "favorable" differs.

    `current_volatility` (see _sample_volatility) is direction-agnostic --
    change_pct is already side-aware, so "the market is choppy right now"
    means the same thing regardless of which way this position is facing."""
    entry_price = float(position["entry_price"])
    is_short = position.get("side") == "short"
    if is_short:
        change_pct = (entry_price - current_price) / entry_price if entry_price > 0 else 0.0
        favorable_velocity = -velocity_pct_per_min if velocity_pct_per_min is not None else None
        favorable_external_velocity = -external_velocity_pct_per_min if external_velocity_pct_per_min is not None else None
    else:
        change_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0.0
        favorable_velocity = velocity_pct_per_min
        favorable_external_velocity = external_velocity_pct_per_min

    if change_pct >= QUICK_PROFIT_PCT:
        if favorable_velocity is not None and favorable_velocity >= QUICK_PROFIT_VELOCITY_PCT_PER_MIN:
            return True, f"quick_profit (velocity {velocity_pct_per_min:+.2%}/min, gain {change_pct:+.3%})"
        if favorable_external_velocity is not None and favorable_external_velocity >= QUICK_PROFIT_VELOCITY_PCT_PER_MIN:
            return True, f"quick_profit (external velocity {external_velocity_pct_per_min:+.2%}/min, gain {change_pct:+.3%})"
    if (
        current_volatility is not None and current_volatility >= HIGH_VOLATILITY_THRESHOLD
        and change_pct >= VOLATILITY_QUICK_PROFIT_PCT
    ):
        return True, f"volatility_quick_profit (volatility {current_volatility:.4f}, gain {change_pct:+.3%})"
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


def _real_open_positions_by_ticker() -> dict[str, dict[str, Any]] | None:
    """Ground truth from Kalshi's own GET /margin/positions -- local
    bookkeeping only ever records an order having been PLACED, never
    confirms it actually FILLED. Confirmed live on this account: repeated
    entry/exit orders placed as `time_in_force=immediate_or_cancel` came
    back with `fill_count: 0.00` (canceled, nothing executed), yet the
    local state that assumed success still added/removed a position as if
    it had. Returns None (never an empty dict) on a failed API call so
    callers can tell "confirmed no real positions" apart from "couldn't
    check" and avoid wiping out tracking on a transient error.

    Per Kalshi's own OpenAPI spec, `position` is a SIGNED quantity (negative
    = short, positive = long) with no separate direction field -- direction
    is derived from the sign here, once, so nothing downstream has to
    re-derive it."""
    try:
        data = get_margin_positions()
    except Exception as exc:
        logger.warning("[perps_strategy] could not fetch real positions for reconciliation: %s", exc)
        return None
    result: dict[str, dict[str, Any]] = {}
    for p in data.get("positions") or []:
        if not p.get("is_portfolio"):
            continue  # non-portfolio subaccount rows observed at 0 size; not real tradable exposure
        raw_count = float(p.get("position") or 0.0)
        ticker = p.get("market_ticker")
        if raw_count == 0 or not ticker:
            continue
        result[ticker] = {
            "count": abs(raw_count), "entry_price": float(p.get("entry_price") or 0.0),
            "side": "short" if raw_count < 0 else "long",
        }
    return result


def _reconcile_positions_with_exchange(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Make local `state["positions"]` match what Kalshi's own account
    actually holds before any exit/entry decision is made. Handles all
    three ways local bookkeeping can have drifted from reality:
      - a real position exists that local state never recorded (a prior
        entry attempt's fill was never verified) -> ADOPT it, so it starts
        being monitored for take-profit/stop-loss instead of sitting with
        no coverage at all;
      - a local position's count/entry_price doesn't match the real one
        (partial fills accumulated differently than assumed) -> CORRECT it;
      - a local position has no real counterpart at all (the entry order
        never actually filled) -> DROP it without recording a fake trade.
    Only ever called when live trading is actually active (see callers) --
    in dry-run, local positions are simulated and deliberately have no
    real-exchange counterpart, so reconciling would just erase them."""
    local_positions = state.get("positions") or []
    real = _real_open_positions_by_ticker()
    if real is None:
        return local_positions

    local_by_ticker = {p["ticker"]: p for p in local_positions}
    reconciled: list[dict[str, Any]] = []
    for ticker, real_pos in real.items():
        local = local_by_ticker.get(ticker)
        if local is None:
            logger.warning(
                "[perps_strategy] adopting untracked real position: %s %s x%.2f @ %.4f",
                real_pos["side"], ticker, real_pos["count"], real_pos["entry_price"],
            )
            reconciled.append({
                "ticker": ticker, "entry_price": real_pos["entry_price"], "count": real_pos["count"],
                "side": real_pos["side"],
                "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "dry_run": False,
                "sizing": {"note": "adopted_from_exchange_reconciliation"},
            })
            continue
        if (
            abs(float(local["count"]) - real_pos["count"]) > 1e-9
            or abs(float(local["entry_price"]) - real_pos["entry_price"]) > 1e-6
            or local.get("side", "long") != real_pos["side"]
        ):
            logger.warning(
                "[perps_strategy] correcting local position for %s: count %.2f->%.2f, entry %.4f->%.4f, side %s->%s",
                ticker, float(local["count"]), real_pos["count"], float(local["entry_price"]), real_pos["entry_price"],
                local.get("side", "long"), real_pos["side"],
            )
        local["count"] = real_pos["count"]
        local["entry_price"] = real_pos["entry_price"]
        local["side"] = real_pos["side"]
        reconciled.append(local)

    for ticker in set(local_by_ticker) - set(real):
        logger.warning("[perps_strategy] dropping phantom local position (no matching real fill): %s", ticker)

    return reconciled


def manage_open_positions(*, dry_run: bool | None = None) -> dict[str, Any]:
    """Fast loop: ONLY checks/exits existing open positions, one cheap price
    call per position. Meant to run every 15-30 seconds so a quick,
    fast-reversing move actually gets a quick exit instead of waiting for
    the next full scan. Returns action "no_position" immediately (no API
    calls at all) if nothing is open."""
    effective_dry_run = True if not LIVE_TRADING_ENABLED else bool(dry_run if dry_run is not None else True)
    with _STATE_LOCK:
        state = _load_state()
        if not effective_dry_run:
            # Ground-truth check first, before deciding anything -- see
            # _reconcile_positions_with_exchange. Dry-run positions are
            # simulated and have no real counterpart, so this only ever
            # runs when orders are actually being placed.
            state["positions"] = _reconcile_positions_with_exchange(state)
            _save_state(state)
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
            current_volatility = _sample_volatility(position.get("price_samples") or [])

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
                current_volatility=current_volatility,
            )
            checks.append({
                "ticker": ticker, "ok": True, "exit_check": reason, "velocity_pct_per_min": velocity,
                "external_velocity_pct_per_min": external_velocity, "current_volatility": current_volatility,
                "external_source": (external_quote or {}).get("source"),
            })

            if not should_exit:
                remaining.append(position)
                continue

            count = float(position["count"])
            side = position.get("side", "long")
            exit_price = _round_price(current_price, tick_size)
            entry_price = float(position["entry_price"])
            order_result = None
            closed_count = count
            if not effective_dry_run:
                # Closing a long means selling (ask); closing a short means
                # buying back (bid) -- both reduce_only so it can only ever
                # shrink/close this exact position, never open a new one.
                order_result = create_margin_order(
                    ticker=ticker, side=("ask" if side == "long" else "bid"), count=count, price=exit_price,
                    client_order_id=str(uuid.uuid4()), time_in_force="immediate_or_cancel", reduce_only=True,
                )
                # An immediate_or_cancel order can fill zero, partially, or
                # fully -- confirmed live on this account (repeated exit
                # attempts came back fill_count 0.00, i.e. fully canceled).
                # Re-check the real position size right after to find out
                # what actually happened before touching bookkeeping/P&L.
                real_after = _real_open_positions_by_ticker()
                if real_after is not None:
                    closed_count = round(max(0.0, count - real_after.get(ticker, {}).get("count", 0.0)), 6)
                else:
                    logger.warning(
                        "[perps_strategy] could not verify exit fill for %s after placing order -- "
                        "assuming full close (order_result=%s)", ticker, order_result,
                    )

            if closed_count <= 0:
                # Nothing actually closed -- position is unchanged on the
                # exchange, keep tracking it as-is and retry next cycle.
                remaining.append(position)
                checks[-1]["exit_order_not_filled"] = True
                continue

            # Each market's quoted "price" IS the per-contract dollar value
            # already (e.g. KXBTCPERP's "0.0001 BTC" contract priced ~$6.63
            # when BTC ~$66,300), so P&L is simply the price delta times
            # contract count -- no separate multiplier needed. A short
            # profits when price FALLS, so the delta is entry-minus-exit
            # instead of exit-minus-entry.
            if side == "short":
                realized_pnl = round((entry_price - exit_price) * closed_count, 6)
            else:
                realized_pnl = round((exit_price - entry_price) * closed_count, 6)
            by_date = state.setdefault("realized_pnl_by_date", {})
            by_date[_today_str()] = round(float(by_date.get(_today_str(), 0.0)) + realized_pnl, 6)
            trade = {
                "closed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "ticker": ticker, "side": side, "entry_price": entry_price, "exit_price": exit_price,
                "count": closed_count, "realized_pnl_usd": realized_pnl, "reason": reason, "dry_run": effective_dry_run,
            }
            state.setdefault("trade_log", []).append(trade)
            closed.append(trade)

            if closed_count < count:
                # Partial fill -- the remainder is still genuinely open on
                # the exchange, keep monitoring it rather than dropping it.
                remainder = dict(position)
                remainder["count"] = round(count - closed_count, 6)
                remaining.append(remainder)

        state["positions"] = remaining
        # push_durable only when a trade actually closed this cycle (real
        # money moved, realized_pnl_by_date changed) -- not on every 20s
        # tick just because positions/velocity samples were touched.
        _save_state(state, push_durable=bool(closed))
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
        if not effective_dry_run:
            # Same ground-truth check as manage_open_positions() -- makes
            # sure `held_tickers` below (which decides what NOT to
            # re-enter) reflects reality even if the fast loop hasn't
            # ticked yet (e.g. right after a fresh deploy).
            state["positions"] = _reconcile_positions_with_exchange(state)
            _save_state(state)
        positions = state.get("positions") or []
        open_slots = MAX_CONCURRENT_POSITIONS - len(positions)
        if open_slots <= 0:
            return {"ok": True, "dry_run": effective_dry_run, "action": "max_positions_open", "open_position_count": len(positions)}

        try:
            available_balance_usd = _available_balance_usd()
        except Exception as exc:
            available_balance_usd = None
            logger.debug("[perps_strategy] balance read for daily reference failed: %s", exc)
        reference_was_just_set = _today_str() not in (state.get("daily_reference_balance") or {})
        reference_balance = _reference_balance_for_today(state, available_balance_usd)
        loss_cap_breached = _daily_loss_cap_breached(state, reference_balance)
        held_tickers = {p["ticker"] for p in positions}
        # push_durable only on the (once-daily) event a fresh reference
        # balance gets captured -- this is the value a restart must not be
        # allowed to silently lose, see the module-level comment above
        # HF_API_KEY.
        _save_state(state, push_durable=reference_was_just_set)
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
            # candidate["current_price"] came from latest_feature_row(), which
            # is fine for DECIDING whether to enter (a dip/rally signal
            # doesn't need sub-second freshness) but is backed by a 45-second
            # candle cache -- not fresh enough to actually PRICE a real
            # order. get_margin_market() above is never cached, so its own
            # "price" field is what the order and the price-sanity check
            # below should both use. Falls back to the candidate price only
            # if the fresh quote is somehow missing.
            fresh_price = float(market.get("price") or 0.0) or candidate["current_price"]
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
            implied_spot_price = fresh_price / contract_size
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

        entry_price = _round_price(fresh_price, tick_size)
        sizing_market = dict(market)
        sizing_market["price"] = entry_price
        count, sizing_detail = compute_leveraged_count(available_balance_usd, sizing_market)
        if count < 1:
            opened.append({
                "ticker": ticker, "ok": True, "action": "skipped_insufficient_budget",
                "sizing": sizing_detail,
            })
            continue

        side = candidate.get("side", "long")
        order_result = None
        actual_count: float = float(count)
        actual_entry_price = entry_price
        if not effective_dry_run:
            # Opening a long means buying (bid); opening a short means
            # selling (ask) with reduce_only NOT set, since this is a new
            # position, not closing an existing long.
            order_result = create_margin_order(
                ticker=ticker, side=("bid" if side == "long" else "ask"), count=float(count), price=entry_price,
                client_order_id=str(uuid.uuid4()), time_in_force="immediate_or_cancel",
            )
            # An immediate_or_cancel order can fill zero, partially, or
            # fully -- confirmed live on this account (several buy attempts
            # came back fill_count 0.00; others partially filled less than
            # requested). Never record a position based on the REQUESTED
            # count; verify what actually landed on the exchange first.
            real_after = _real_open_positions_by_ticker()
            if real_after is not None:
                real_pos = real_after.get(ticker)
                actual_count = real_pos["count"] if real_pos else 0.0
                if real_pos and real_pos["entry_price"] > 0:
                    actual_entry_price = real_pos["entry_price"]
            else:
                logger.warning(
                    "[perps_strategy] could not verify entry fill for %s after placing order -- "
                    "assuming full fill (order_result=%s)", ticker, order_result,
                )

        if actual_count <= 0:
            opened.append({
                "ticker": ticker, "ok": True, "action": "skipped_entry_not_filled",
                "reason": candidate["reason"], "order_result": order_result,
            })
            continue

        with _STATE_LOCK:
            # Re-read: the fast loop's own reconciliation runs concurrently
            # and can have adopted/updated this EXACT ticker in the interim
            # (e.g. a stray earlier fill it just discovered). We already
            # verified real money actually filled above (actual_count > 0)
            # -- if a tracked entry for this ticker already exists, MERGE
            # this fill into it (real_after/actual_* already reflect
            # Kalshi's own up-to-date TOTAL for the ticker, not just this
            # order's delta) rather than silently discarding a confirmed
            # real fill via "skipped_slot_taken", which would leave more
            # real contracts open than local state tracks -- the exact
            # under-tracked-position failure this whole fix exists to close.
            state = _load_state()
            positions = state.get("positions") or []
            existing_idx = next((i for i, p in enumerate(positions) if p["ticker"] == ticker), None)
            if existing_idx is None and len(positions) >= MAX_CONCURRENT_POSITIONS:
                opened.append({"ticker": ticker, "ok": True, "action": "skipped_slot_taken"})
                continue
            if existing_idx is not None:
                merged = dict(positions[existing_idx])
                merged["count"] = float(actual_count)
                merged["entry_price"] = actual_entry_price
                merged["side"] = side
                positions[existing_idx] = merged
            else:
                positions.append({
                    "ticker": ticker, "entry_price": actual_entry_price, "count": float(actual_count),
                    "side": side,
                    "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "dry_run": effective_dry_run,
                    "sizing": sizing_detail,
                })
            state["positions"] = positions
            _save_state(state)
        opened.append({
            "ticker": ticker, "ok": True, "action": "opened", "side": side, "entry_price": actual_entry_price,
            "count": actual_count, "reason": candidate["reason"], "sizing": sizing_detail, "order_result": order_result,
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
