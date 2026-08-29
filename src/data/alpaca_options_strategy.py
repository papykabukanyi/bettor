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

  - No broker-native BRACKET order for options, so exits (take-profit/
    stop-loss/max-hold/near-expiration) are managed by this bot's own poll
    loop, the same pattern already proven for crypto. Alpaca DOES support
    a real order_class for options, though: "mleg" (multi-leg), confirmed
    via their own docs AND this account's real options_approved_level (3)
    -- see ENTRY_STRATEGY below.

  - Position sizing is a whole number of CONTRACTS/SPREADS (qty), sized
    from the position's own entry cost (a naked contract's premium, or a
    spread's net debit) * its 100-share multiplier -- options don't
    support notional orders at all (confirmed via Alpaca's docs).

  - Two entry strategies, selected by ENTRY_STRATEGY (default
    "debit_spread"): a naked long call/put (unlimited upside, full
    premium at risk, Level 2), or a vertical debit spread (the same near-
    the-money contract PLUS a further-out-of-the-money short leg sold
    against it -- cheaper to enter, capped gain AND capped loss, Level 3;
    see select_spread_contracts/build_option_spread_order). Same
    directional signal either way -- this only changes how it's expressed.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import math
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
# Real, confirmed via a 4-fold walk-forward backtest (run_walkforward_backtest,
# real production load_training_dataset path, 50,568 rows) plus a
# corroborating single 80/20 split: at the old default 0.58, options placed
# almost no trades at all (17 trades across all 4 folds combined, 3 of them
# zero-trade folds) with a negative mean_return (-1.83%) -- essentially the
# same "threshold above the model's own achievable confidence" miscalibration
# already fixed for stocks (see that constant's own comment). 0.53 was the
# best-corroborated candidate across BOTH tests (walk-forward: 0.75
# profitable-fold-ratio, mean_return +3.29%, vs 0.58's 0.25/-1.83%; single
# split: +3.08% vs the same split's negative numbers at every threshold
# under the old, buggy backtest). Also confirmed via a widened sweep of 6
# entry-signal archetypes (momentum/breakout/RSI/trend-alignment/confluence
# confirmation layered on top of the model's directional call) that EVERY
# technical-confirmation variant performed clearly worse than this plain
# confidence-only baseline (-10% to -24% vs baseline's positive return) --
# don't reintroduce one of those without new evidence.
MODEL_CONFIDENCE_MIN = _env_float("ALPACA_OPTIONS_MODEL_CONFIDENCE_MIN", 0.53)
# Real gap found in review, per explicit user direction: unlike the other
# 3 services, this strategy never had ANY volume/volatility pre-filter at
# all -- entries fired purely on model confidence, regardless of whether
# the underlying was actually seeing real participation/movement right
# now. A confident model prediction on a quiet, thin-volume underlying is
# a materially weaker real-world signal than the same prediction during
# genuine activity -- options amplify the underlying's move, so entering
# when that move has no real momentum behind it risks paying the wide
# TAKE_PROFIT_PCT/STOP_LOSS_PCT spread for nothing. Same real, non-extreme
# values as alpaca_strategy.py's/alpaca_crypto_strategy.py's own identical
# gates, for consistency across all 4 services.
MIN_VOLUME_Z = _env_float("ALPACA_OPTIONS_MIN_VOLUME_Z", 1.0)
MIN_VOLATILITY_RATIO = _env_float("ALPACA_OPTIONS_MIN_VOLATILITY_RATIO", 1.1)  # volatility_5 / volatility_30
TAKE_PROFIT_PCT = _env_float("ALPACA_OPTIONS_TAKE_PROFIT_PCT", 0.30)
STOP_LOSS_PCT = _env_float("ALPACA_OPTIONS_STOP_LOSS_PCT", 0.20)
MAX_HOLD_MINUTES = _env_int("ALPACA_OPTIONS_MAX_HOLD_MINUTES", 180)
# Per-ticker adaptive take-profit/stop-loss -- same methodology
# perps_strategy.py/alpaca_crypto_strategy.py/alpaca_strategy.py already use
# (see any of their own adaptive_exit_pcts docstrings). `entry_volatility_30`
# here is the UNDERLYING's own volatility (this module reuses alpaca_data.py's
# equities feature engineering directly -- see the module docstring), on the
# SAME small scale as stocks'/crypto's own (roughly 0.0004-0.002), while an
# option's own premium target is ~30x wider than theirs (0.30 vs ~0.01) --
# see this constant block's own comment above on why. Naively reusing
# stocks'/crypto's own vol multiples here would silently always clamp to
# the floor (a no-op adaptive mechanism), so these are scaled up
# proportionally (~30x, matching the flat-default ratio) instead -- a
# reasoned starting point given real, KNOWN underlying-to-premium
# amplification, NOT a claim of independent backtest validation (no
# historical premium series exists to backtest against -- see
# alpaca_options_trade_analysis.py's own docstring on that same
# limitation). Fully env-overridable once real trade history justifies
# retuning.
TAKE_PROFIT_VOL_MULTIPLE = _env_float("ALPACA_OPTIONS_TAKE_PROFIT_VOL_MULTIPLE", 45.0)
STOP_LOSS_VOL_MULTIPLE = _env_float("ALPACA_OPTIONS_STOP_LOSS_VOL_MULTIPLE", 30.0)
MIN_TAKE_PROFIT_PCT = _env_float("ALPACA_OPTIONS_MIN_TAKE_PROFIT_PCT", 0.30)
MAX_TAKE_PROFIT_PCT = _env_float("ALPACA_OPTIONS_MAX_TAKE_PROFIT_PCT", 1.00)
MIN_STOP_LOSS_PCT = _env_float("ALPACA_OPTIONS_MIN_STOP_LOSS_PCT", 0.20)
MAX_STOP_LOSS_PCT = _env_float("ALPACA_OPTIONS_MAX_STOP_LOSS_PCT", 0.60)
# Tried and reverted: a "stale/flat position" early exit (see
# perps_strategy.py's own comment for the identical mechanism and full
# rationale for reverting it) was added here too, then reverted after a
# real multi-fold backtest on perps showed it performed worse than not
# having it at every hold time tested, 16/16 comparisons -- the mechanism
# cut positions early without reducing the fee/decay cost of a flat
# contract, while forfeiting the chance for a quiet-then-moving position to
# still reach take_profit. Reverted here for consistency. If revisiting,
# backtest this strategy's own history first -- don't re-add on hypothesis
# alone.
#
# max_hold_time shouldn't be the ONLY factor forcing an exit -- see
# perps_strategy.py's own PROMISING_PROGRESS_FRACTION comment for the full
# rationale, thresholds, and real backtest findings (price-progress alone
# showed a real, if modest, improvement there; the volume/momentum/breakout
# path tested WORSE in isolation, so it's kept conservative/near-dormant by
# default here too -- ported for consistency, not independently backtested
# on options' own history). Extension is more generous than perps' here: no
# funding payment to worry about, and _near_expiration (below) already
# provides an independent hard backstop regardless of this extension.
MAX_HOLD_EXTENSION_MINUTES = _env_int("ALPACA_OPTIONS_MAX_HOLD_EXTENSION_MINUTES", 120)
PROMISING_PROGRESS_FRACTION = _env_float("ALPACA_OPTIONS_PROMISING_PROGRESS_FRACTION", 0.25)
PROMISING_VOLUME_Z = _env_float("ALPACA_OPTIONS_PROMISING_VOLUME_Z", 1.0)
PROMISING_MOMENTUM_PCT = _env_float("ALPACA_OPTIONS_PROMISING_MOMENTUM_PCT", 0.0003)
PROMISING_BREAKOUT_PCT_B = _env_float("ALPACA_OPTIONS_PROMISING_BREAKOUT_PCT_B", 0.85)
PROMISING_SENTIMENT_SCORE = _env_float("ALPACA_OPTIONS_PROMISING_SENTIMENT_SCORE", 0.3)
# Force-close a held contract once fewer than this many days remain before
# expiration, regardless of TP/SL/max-hold -- avoids assignment risk and a
# contract decaying to worthless for reasons unrelated to this strategy's
# own exit signal.
MIN_DAYS_TO_EXPIRATION_BEFORE_FORCED_EXIT = _env_int("ALPACA_OPTIONS_MIN_DAYS_TO_EXPIRATION_BEFORE_FORCED_EXIT", 2)

# "Pick the best times": real options spreads are consistently widest right
# at the regular-session open (overnight-gap price discovery still
# settling) and right at the close (last-minute positioning) -- skip NEW
# entries in those windows even though the session itself is open.
AVOID_SESSION_EDGE_MINUTES = _env_int("ALPACA_OPTIONS_AVOID_SESSION_EDGE_MINUTES", 15)


def _minutes_from_session_edge() -> float:
    """Minutes since the regular session opened OR until it closes,
    whichever is smaller -- large (effectively "not near an edge") outside
    the regular session entirely, since the session-type gate above
    already handles that case separately."""
    try:
        import zoneinfo
        eastern = zoneinfo.ZoneInfo("America/New_York")
    except Exception:
        import pytz
        eastern = pytz.timezone("America/New_York")
    now_et = dt.datetime.now(tz=eastern)
    open_dt = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    close_dt = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    since_open = (now_et - open_dt).total_seconds() / 60.0
    until_close = (close_dt - now_et).total_seconds() / 60.0
    return min(since_open, until_close)

# Raised from 2/25% -- "place a lot of options trades of many profitable
# legs" was explicitly asked for. Contracts are lumpy (whole units,
# each costing premium x the 100-share multiplier -- see
# compute_contract_qty), so slot size can't shrink as far as the equities/
# crypto strategies' own: on a $500 account, a 15% ($75) slot couldn't
# afford even one contract at a $1.00 premium at all. 4 slots x 20% ($100
# each) keeps every slot able to afford a real, near-the-money weekly
# contract while still allowing multiple simultaneous legs across
# different underlyings, matching perps_strategy.py's own more permissive
# concurrent-slot posture without being unaffordable in practice.
POSITION_SIZE_PCT = _env_float("ALPACA_OPTIONS_POSITION_SIZE_PCT", 0.20)
MAX_CONCURRENT_POSITIONS = max(1, _env_int("ALPACA_OPTIONS_MAX_CONCURRENT_POSITIONS", 4))
DAILY_LOSS_CAP_PCT = _env_float("ALPACA_OPTIONS_DAILY_LOSS_CAP_PCT", 0.10)
# Same unbounded-growth guard perps_strategy.py already needed (a real,
# confirmed OOM contributor there over weeks of live trading) -- keeps the
# most recent entries, oldest-first trimmed.
MAX_TRADE_LOG_ENTRIES = _env_int("ALPACA_OPTIONS_MAX_TRADE_LOG_ENTRIES", 2000)

LIVE_TRADING_ENABLED = str(os.getenv("ALPACA_OPTIONS_LIVE_TRADING_ENABLED", "")).strip().lower() in {"1", "true", "yes"}

# "Need to know about all the ways to make money with options" -- confirmed
# via Alpaca's own docs that this account's REAL options_approved_level is
# 3, which explicitly covers vertical debit spreads ("Buy a call spread"/
# "Buy a put spread") as a genuine order_class="mleg" order, not just
# naked long calls/puts (Level 2). A debit spread buys the same near-the-
# money contract this strategy already picks, then SELLS a further-out-of-
# the-money contract against it -- cheaper to enter (the short leg's
# premium partially offsets the long leg's cost) and DEFINED-RISK (max
# gain AND max loss are both capped the instant it's opened), in exchange
# for giving up unlimited upside. Same directional signal this strategy's
# model already produces either way -- this only changes HOW that read
# gets expressed, not what triggers it.
#
# Default "debit_spread", not "naked": cheaper entries mean the same
# POSITION_SIZE_PCT budget affords MORE simultaneous positions -- directly
# serves "place a lot of options trades of many profitable legs" (the
# reason MAX_CONCURRENT_POSITIONS/POSITION_SIZE_PCT were raised earlier).
# Fully revertable via env var if naked calls/puts (uncapped upside, no
# short-leg liquidity dependency) turn out to perform better once there's
# real trade history for both.
ENTRY_STRATEGY = os.getenv("ALPACA_OPTIONS_ENTRY_STRATEGY", "debit_spread").strip().lower()
if ENTRY_STRATEGY not in {"debit_spread", "naked"}:
    ENTRY_STRATEGY = "debit_spread"

# mleg orders require a real limit price -- unlike build_option_order's
# plain market order, there's no way to say "fill at whatever the market
# clears at" for a multi-leg combo (confirmed via Alpaca's own docs: every
# multi-leg example uses type="limit"). A marketable limit priced slightly
# worse than the current net mid (get_current_spread_price) keeps a real
# chance of actually filling without paying/accepting an unbounded price --
# same tradeoff any real limit order makes.
SPREAD_LIMIT_SLIPPAGE_PCT = _env_float("ALPACA_OPTIONS_SPREAD_LIMIT_SLIPPAGE_PCT", 0.10)


def evaluate_candidate(
    row: dict[str, Any], model_prediction: dict[str, Any] | None, *, confidence_min: float | None = None,
) -> dict[str, Any]:
    """No technical-only fallback here -- see this module's own docstring
    for why. Returns should_enter=False with no trained model at all.

    `confidence_min` overrides the module-level MODEL_CONFIDENCE_MIN
    default when given -- see scan_and_enter, which reads a durable-state
    override set by alpaca_options_trade_analysis.recommend_confidence_threshold's
    own evidence-gated tuning (apply_confidence_threshold_override), same
    pattern perps_strategy.py already uses.

    Volume/volatility checked BEFORE the model confidence itself (see
    MIN_VOLUME_Z's own comment) -- a confident prediction on a quiet
    underlying still doesn't clear the bar."""
    result: dict[str, Any] = {
        "symbol": row.get("symbol"), "model_ok": False, "should_enter": False, "direction": None, "score": 0.0,
    }
    if not model_prediction or not model_prediction.get("model_ok"):
        result["reason"] = "no trained model yet -- options entries require real model confidence"
        return result

    dollar_volume_z = row.get("dollar_volume_z")
    if dollar_volume_z is None or dollar_volume_z < MIN_VOLUME_Z:
        result["reason"] = f"underlying volume not unusual enough (z={dollar_volume_z})"
        return result
    volatility_5 = row.get("volatility_5") or 0.0
    volatility_30 = row.get("volatility_30") or 0.0
    if volatility_30 > 0 and (volatility_5 / volatility_30) < MIN_VOLATILITY_RATIO:
        result["reason"] = "underlying not more volatile than its own recent baseline"
        return result

    effective_confidence_min = confidence_min if confidence_min is not None else MODEL_CONFIDENCE_MIN
    proba_up = model_prediction["probability_up"]
    result["model_ok"] = True
    result["probability_up"] = proba_up
    if proba_up >= effective_confidence_min:
        result["should_enter"] = True
        result["direction"] = "up"
        result["reason"] = f"model confident UP ({proba_up:.2%}) -- buying a call"
        result["score"] = proba_up
    elif proba_up <= (1.0 - effective_confidence_min):
        result["should_enter"] = True
        result["direction"] = "down"
        result["reason"] = f"model confident DOWN ({proba_up:.2%}) -- buying a put"
        result["score"] = 1.0 - proba_up
    else:
        result["reason"] = f"model not confident enough either way ({proba_up:.2%})"
    return result


def _near_expiration(position: dict[str, Any], *, now: dt.datetime) -> bool:
    expiration_date = position.get("expiration_date")
    if not expiration_date:
        return False
    exp = dt.datetime.fromisoformat(expiration_date).replace(tzinfo=dt.timezone.utc)
    return (exp - now).days < MIN_DAYS_TO_EXPIRATION_BEFORE_FORCED_EXIT


def adaptive_exit_pcts(entry_volatility_30: float | None) -> dict[str, float]:
    """Take-profit/stop-loss percentages customized to the UNDERLYING's own
    volatility at entry time -- see TAKE_PROFIT_VOL_MULTIPLE's own comment
    for the full rationale (including why the multiples are scaled ~30x
    vs. stocks'/crypto's own). Falls back to the flat global
    TAKE_PROFIT_PCT/STOP_LOSS_PCT if no volatility was captured, or on NaN
    specifically -- Python's own NaN comparisons are always False, so a
    naive falsy/<=0 guard alone would miss it."""
    if not entry_volatility_30 or entry_volatility_30 <= 0 or math.isnan(entry_volatility_30):
        return {"take_profit_pct": TAKE_PROFIT_PCT, "stop_loss_pct": STOP_LOSS_PCT}
    horizon_scale = math.sqrt(max(1, MAX_HOLD_MINUTES))
    take_profit = min(MAX_TAKE_PROFIT_PCT, max(MIN_TAKE_PROFIT_PCT, TAKE_PROFIT_VOL_MULTIPLE * entry_volatility_30 * horizon_scale))
    stop_loss = min(MAX_STOP_LOSS_PCT, max(MIN_STOP_LOSS_PCT, STOP_LOSS_VOL_MULTIPLE * entry_volatility_30 * horizon_scale))
    return {"take_profit_pct": take_profit, "stop_loss_pct": stop_loss}


def decide_exit(
    position: dict[str, Any], current_price: float, *, now: dt.datetime | None = None,
    dollar_volume_z: float | None = None, momentum_pct: float | None = None,
    breakout_pct_b: float | None = None, sentiment_score: float | None = None,
) -> tuple[bool, str]:
    """Long-only (buying calls/puts is always a long position in the
    contract itself, regardless of which direction it bets on the
    underlying): a RISING contract premium is favorable. Exit levels are
    per-position ADAPTIVE (see adaptive_exit_pcts) -- scaled to the
    UNDERLYING's own volatility_30 at entry, not one flat percentage
    applied identically to every contract.

    `dollar_volume_z`/`momentum_pct` (macd_hist_pct)/`breakout_pct_b`
    (bb_pct_b)/`sentiment_score` feed the max_hold_time "promising position"
    extension only -- see perps_strategy.py's own PROMISING_PROGRESS_FRACTION
    comment for the full rationale, thresholds, and real backtest findings
    (ported here for consistency, not independently backtested on options'
    own history)."""
    now = now if now is not None else dt.datetime.now(dt.timezone.utc)
    if _near_expiration(position, now=now):
        return True, "near_expiration"

    entry_price = float(position["entry_price"])
    exit_pcts = adaptive_exit_pcts(position.get("entry_volatility_30"))
    take_profit_pct = exit_pcts["take_profit_pct"]
    stop_loss_pct = exit_pcts["stop_loss_pct"]
    change_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0.0

    if change_pct >= take_profit_pct:
        return True, f"take_profit ({change_pct:+.3%}, target {take_profit_pct:.2%})"
    if change_pct <= -stop_loss_pct:
        return True, f"stop_loss ({change_pct:+.3%}, target {stop_loss_pct:.2%})"

    opened_at = dt.datetime.fromisoformat(position["opened_at"])
    held_minutes = (now - opened_at).total_seconds() / 60.0
    if held_minutes >= MAX_HOLD_MINUTES:
        progress_frac = (change_pct / take_profit_pct) if take_profit_pct > 0 else 0.0
        price_promising = progress_frac >= PROMISING_PROGRESS_FRACTION
        volume_confirmed = dollar_volume_z is not None and dollar_volume_z >= PROMISING_VOLUME_Z
        momentum_promising = (
            volume_confirmed and change_pct >= 0
            and momentum_pct is not None and momentum_pct >= PROMISING_MOMENTUM_PCT
        )
        breakout_promising = (
            volume_confirmed and change_pct >= 0
            and breakout_pct_b is not None and breakout_pct_b >= PROMISING_BREAKOUT_PCT_B
        )
        sentiment_promising = sentiment_score is not None and sentiment_score >= PROMISING_SENTIMENT_SCORE
        promising = price_promising or momentum_promising or breakout_promising or sentiment_promising
        if not promising or held_minutes >= MAX_HOLD_MINUTES + MAX_HOLD_EXTENSION_MINUTES:
            return True, f"max_hold_time ({held_minutes:.0f}min, {change_pct:+.3%})"
    return False, f"holding ({change_pct:+.3%}, {held_minutes:.0f}min)"


def position_exit_levels(position: dict[str, Any]) -> dict[str, float]:
    """The actual take-profit/stop-loss PRICE levels for a position,
    derived from the same per-position-adaptive percentages decide_exit()
    applies (see adaptive_exit_pcts) -- exists so callers (the dashboard)
    can show real exit levels rather than just trusting the flat config
    exists somewhere."""
    entry_price = float(position["entry_price"])
    exit_pcts = adaptive_exit_pcts(position.get("entry_volatility_30"))
    return {
        "take_profit_price": round(entry_price * (1 + exit_pcts["take_profit_pct"]), 6),
        "stop_loss_price": round(entry_price * (1 - exit_pcts["stop_loss_pct"]), 6),
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


def _entry_context(candidate: dict[str, Any], row: dict[str, Any] | None = None) -> dict[str, Any]:
    """Entry-time model/technical context -- what the model actually saw at
    decision time, so a post-trade analysis can ask "what led to this
    win/loss" instead of only ever knowing how it ended. Same fields (by
    name) perps_strategy.py already captures, so
    alpaca_options_trade_analysis.py can mirror its logic. `row` (the
    underlying's own engineered feature row, when the caller has it) adds
    the raw indicator values used by the exit chart's indicator panel."""
    row = row or {}
    return {
        "entry_probability_up": candidate.get("probability_up"),
        "entry_model_direction": candidate.get("direction"),
        "entry_reason": candidate.get("reason"),
        # candidate["score"] -- the one number actually compared against
        # MODEL_CONFIDENCE_MIN at entry (proba_up for a call, 1-proba_up
        # for a put) -- carried through so recommend_confidence_threshold
        # can ask real trade history whether a HIGHER floor would have
        # performed better.
        "entry_score": candidate.get("score"),
        "entry_dollar_volume_z": row.get("dollar_volume_z"),
        "entry_macd_hist_pct": row.get("macd_hist_pct"),
        "entry_bb_pct_b": row.get("bb_pct_b"),
        "entry_rsi_14": row.get("rsi_14"),
        "entry_sentiment_score": row.get("sentiment_score"),
    }


def _candles_as_dicts(df) -> list[dict[str, Any]]:
    """Converts a fetch_recent_minute_bars-style DataFrame into the plain
    list[dict] shape both chart_snapshot.generate_candlestick_chart and
    alpaca_options_trade_analysis expect -- keeps those two modules
    pandas-free."""
    if df is None or df.empty:
        return []
    cols = [c for c in ("ts", "open", "high", "low", "close") if c in df.columns]
    return df[cols].to_dict("records")


def _index_for_ts(df, iso_ts: str | None) -> int | None:
    """Which row of a fetch_recent_minute_bars-style DataFrame (of the
    UNDERLYING -- see this module's own docstring on why options charts the
    underlying, not the option's own premium) is closest to a given ISO
    timestamp. None if there's no timestamp, no data, or the closest
    candle is more than an hour away."""
    if df is None or df.empty or not iso_ts:
        return None
    try:
        target = dt.datetime.fromisoformat(str(iso_ts).replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError):
        return None
    ts_list = list(df["ts"])
    if not ts_list:
        return None
    best_idx, best_diff = 0, abs(ts_list[0] - target)
    for i, ts in enumerate(ts_list):
        diff = abs(ts - target)
        if diff < best_diff:
            best_idx, best_diff = i, diff
    return best_idx if best_diff <= 3600 else None


def _maybe_run_batch_trade_analysis() -> None:
    """Every alpaca_options_trade_analysis.BATCH_SIZE newly-closed REAL
    trades, studies that recent batch -- win/loss patterns and (underlying-
    only, never premium-dollar) drift diagnostics -- and posts a Threads
    snapshot. Called right after manage_open_positions closes trades,
    outside _STATE_LOCK (same reasoning as every other Threads/network call
    in this module). Best-effort: any failure here is logged and
    swallowed, never allowed to affect trading."""
    from data import alpaca_options_trade_analysis
    from data import threads_post
    from data.alpaca_data import fetch_recent_minute_bars

    try:
        with _STATE_LOCK:
            state = _load_state()
            trade_log = state.get("trade_log") or []
            real_trades = [t for t in trade_log if not t.get("dry_run")]
            last_count = int(state.get("last_batch_analysis_trade_count") or 0)
            if len(real_trades) - last_count < alpaca_options_trade_analysis.BATCH_SIZE:
                return
            state["last_batch_analysis_trade_count"] = len(real_trades)
            _save_state(state, push_durable=True)
            current_threshold = (state.get("tuning") or {}).get("model_confidence_min", MODEL_CONFIDENCE_MIN)

        recent = real_trades[-alpaca_options_trade_analysis.BATCH_SIZE:]
        candles_by_underlying: dict[str, list[dict[str, Any]]] = {}
        for underlying in {t.get("underlying_symbol") for t in recent if t.get("underlying_symbol")}:
            try:
                candles_by_underlying[underlying] = _candles_as_dicts(fetch_recent_minute_bars(underlying))
            except Exception:
                logger.debug("[alpaca_options_strategy] candle fetch for batch analysis failed for %s", underlying, exc_info=True)

        batch = alpaca_options_trade_analysis.analyze_recent_trade_batch(
            real_trades, underlying_candles_by_symbol=candles_by_underlying,
        )
        text = alpaca_options_trade_analysis.format_batch_snapshot_text(batch, market="options")
        threads_post.post_trade_analysis_summary(text, market="options")

        tuning_rec = alpaca_options_trade_analysis.recommend_confidence_threshold(real_trades, current_threshold=current_threshold)
        if tuning_rec.get("should_apply"):
            apply_confidence_threshold_override(tuning_rec["recommended_threshold"], reason="5-trade batch review")
    except Exception:
        logger.warning("[alpaca_options_strategy] batch trade analysis failed", exc_info=True)


# ---------------------------------------------------------------------------
# Always trades against the real Alpaca account (paper or live, whichever
# ALPACA_TRADING_BASE_URL points at) -- the custom local-balance "simulate"
# mode this used to have was removed per the user's explicit request:
# Alpaca's own paper account is now the single source of truth for balance/
# positions/fills, not a hand-rolled virtual ledger. LIVE_TRADING_ENABLED
# remains the one safety gate on whether an order is actually placed vs.
# dry-run (decide but don't call the order API).
# ---------------------------------------------------------------------------

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


def apply_confidence_threshold_override(new_threshold: float, *, reason: str) -> dict[str, Any]:
    """Applies an evidence-gated confidence-floor adjustment (see
    alpaca_options_trade_analysis.recommend_confidence_threshold) durably,
    WITHOUT a redeploy -- stored in state["tuning"] (pushed to HF like the
    rest of durable state) and read by scan_and_enter on every cycle, not
    the OS env var MODEL_CONFIDENCE_MIN is seeded from at import time. Same
    pattern perps_strategy.py already uses."""
    with _STATE_LOCK:
        state = _load_state()
        previous = (state.get("tuning") or {}).get("model_confidence_min", MODEL_CONFIDENCE_MIN)
        state["tuning"] = {
            "model_confidence_min": new_threshold,
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "reason": reason, "previous": previous,
        }
        _save_state(state, push_durable=True)
        return dict(state["tuning"])


def _durable_state_slice(state: dict[str, Any]) -> dict[str, Any]:
    # "tuning" (the evidence-gated confidence-threshold override -- see
    # apply_confidence_threshold_override above) MUST be included here -- a
    # real, confirmed bug found in perps_strategy.py's own identical slice
    # left it out, silently resetting any confidence threshold actually
    # LEARNED from real trade history back to the hardcoded default on
    # every single deploy. Included from the start here rather than
    # repeating that bug.
    return {
        "positions": state.get("positions") or [],
        "trade_log": state.get("trade_log") or [],
        "realized_pnl_by_date": state.get("realized_pnl_by_date") or {},
        "daily_reference_balance": state.get("daily_reference_balance") or {},
        "tuning": state.get("tuning") or {},
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


_DURABLE_STATE_HF_TIMEOUT_SEC = int(os.getenv("ALPACA_OPTIONS_DURABLE_STATE_HF_TIMEOUT_SEC", "10") or "10")


def _pull_durable_state_from_hf() -> dict[str, Any] | None:
    if not HF_API_KEY:
        return None

    def _download() -> dict[str, Any]:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(
            repo_id=HF_ALPACA_OPTIONS_MODEL_REPO, filename=_DURABLE_STATE_HF_FILENAME, repo_type="model", token=HF_API_KEY,
        )
        return json.loads(Path(path).read_text(encoding="utf-8"))

    try:
        # Real, confirmed production incident (same call shape, on the
        # perps service): unbounded, this can hang for minutes on
        # huggingface_hub's own internal session lock, freezing this whole
        # --workers 1 process until gunicorn's worker timeout kills it. See
        # server_common.call_with_hard_timeout's own docstring.
        from server_common import call_with_hard_timeout
        return call_with_hard_timeout(_download, timeout_sec=_DURABLE_STATE_HF_TIMEOUT_SEC)
    except Exception as exc:
        logger.info("[alpaca_options_strategy] no durable state on HF yet (or fetch failed): %s", exc)
        return None


def _load_state() -> dict[str, Any]:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        base = {
            "positions": [], "trade_log": [], "realized_pnl_by_date": {}, "daily_reference_balance": {},
        }
        durable = _pull_durable_state_from_hf()
        if durable:
            base.update(durable)
            logger.info("[alpaca_options_strategy] recovered durable state from HF after local state was missing")
        return base
    state.setdefault("positions", [])
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
    global _last_durable_push_ts
    now = time.time()
    if now - _last_durable_push_ts >= _DURABLE_PUSH_MIN_INTERVAL_SEC:
        _last_durable_push_ts = now
        _push_durable_state_to_hf(state)


def record_milestone(current_balance: float) -> dict[str, Any]:
    """Persists profit-milestone/goal progress in the SAME durable state as
    positions/trade_log (see server_common.milestone_snapshot for the tier
    logic) -- called once per fast_check cycle from the server's own
    account-cache refresh, right after a fresh real balance is fetched, so
    this never makes its own network call. Pushed durably to HF only when
    the all-time high-water mark actually improves (a genuinely meaningful
    event), not on every cycle -- matches this file's existing
    push_durable-is-for-meaningful-events-only discipline."""
    from server_common import milestone_snapshot
    with _STATE_LOCK:
        state = _load_state()
        prev_hwm = (state.get("milestones") or {}).get("high_water_mark")
        snapshot = milestone_snapshot(state, current_balance=current_balance)
        new_peak = prev_hwm is None or snapshot["high_water_mark"] > prev_hwm
        _save_state(state, push_durable=new_peak)
    return snapshot


def get_available_balance() -> float:
    """The real Alpaca cash balance."""
    from data import alpaca_client
    account = alpaca_client.get_account()
    return float(account.get("cash") or 0.0)


def _reference_balance_for_today(state: dict[str, Any], available_balance_usd: float | None) -> float | None:
    """The daily loss cap is a percentage of the balance as it stood at the
    START of the day, not of whatever Alpaca's account balance happens to be
    at the moment it's checked (which drifts throughout the day as trades
    close) -- captured once per day the first time a real balance read
    succeeds. Same pattern as alpaca_strategy.py's/alpaca_crypto_strategy.py's
    own _reference_balance_for_today."""
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


def _real_open_positions_by_symbol() -> dict[str, dict[str, Any]] | None:
    """Ground truth from Alpaca's own GET /v2/positions -- local bookkeeping
    only ever records an order having been PLACED, never confirms it
    actually FILLED at the assumed price/quantity. Returns None (never an
    empty dict) on a failed API call so callers can tell "confirmed no real
    positions" apart from "couldn't check" and avoid wiping out tracking on
    a transient error -- same discipline as alpaca_crypto_strategy.py's own
    _real_open_positions_by_symbol.

    /v2/positions returns EVERY asset class this account holds (equities,
    crypto, and options share one Alpaca account) -- filtered here to
    asset_class=="us_option" so an equity or crypto position can never be
    mistaken for one of this strategy's own."""
    from data import alpaca_client
    try:
        positions = alpaca_client.get_positions()
    except Exception as exc:
        logger.warning("[alpaca_options_strategy] could not fetch real positions for reconciliation: %s", exc)
        return None
    result: dict[str, dict[str, Any]] = {}
    for p in positions:
        if p.get("asset_class") != "us_option":
            continue
        symbol = p.get("symbol") or ""
        qty = float(p.get("qty") or 0.0)
        if not symbol or qty == 0:
            continue
        # Real, confirmed bug this `side` field fixes: Alpaca's own qty sign
        # (or "side") is the ONLY thing that distinguishes a genuine long
        # holding from a SHORT one -- collapsing both to abs(qty) here threw
        # that distinction away, and _reconcile_positions_with_exchange
        # below used to treat a debit spread's own SHORT leg (a real
        # position Alpaca tracks per-contract even though it was opened as
        # one multi-leg order) as an untracked NAKED long to "sell to
        # close." See that function's own comment for the full incident.
        side = str(p.get("side") or "").lower() or ("short" if qty < 0 else "long")
        result[symbol] = {"count": abs(qty), "entry_price": float(p.get("avg_entry_price") or 0.0), "side": side}
    return result


def _reconcile_positions_with_exchange(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Makes local state["positions"] match what the real Alpaca account
    actually holds before any exit/entry decision is made -- same three-way
    adopt/correct/drop logic as alpaca_crypto_strategy.py's own
    _reconcile_positions_with_exchange.

    Debit spreads hold TWO contracts (long + short leg) under one logical
    position -- only the long leg's symbol is reconciled here (matching the
    entry/exit code, which already tracks the spread as a single unit keyed
    on the long contract's symbol). The short leg is STILL a real position
    Alpaca reports on /v2/positions (options are tracked per-contract even
    when opened as one multi-leg order) -- it must be recognized as already
    accounted for, not treated as untracked, and never adopted as a naked
    position even if it somehow isn't.

    Real, confirmed production incident this fixes: without excluding it,
    a spread's own short leg looked "untracked" on EVERY reconciliation
    call and got adopted as a fresh naked position each time (fresh
    opened_at, entry_price = the short leg's own fill price) -- then
    manage_open_positions evaluated it as if it were a LONG holding, so a
    RISING quote (a real LOSS on an actual short) computed as a fake
    PROFIT, attempted an invalid single-leg "sell to close" against a
    position that was never long in the first place (Alpaca rejected it,
    422), and then booked a fabricated "successful" trade anyway and
    posted it to Threads -- repeating every fast_check cycle for as long
    as the spread's short leg stayed open on the real account. Confirmed
    live 2026-08-29: 199 identical fake trades over ~100 minutes, all
    Alpaca PAPER (no real money), before the spread finally closed for
    real and the loop stopped on its own.

    Only ever called when live trading is actually active (see callers) --
    in dry-run, local positions are hypothetical (no order was ever placed)
    and deliberately have no real-exchange counterpart, so reconciling
    would just erase them."""
    local_positions = state.get("positions") or []
    real = _real_open_positions_by_symbol()
    if real is None:
        return local_positions

    local_by_symbol = {p["symbol"]: p for p in local_positions}
    known_short_leg_symbols = {
        p["short_symbol"] for p in local_positions if p.get("strategy") == "debit_spread" and p.get("short_symbol")
    }
    reconciled: list[dict[str, Any]] = []
    for symbol, real_pos in real.items():
        if symbol in known_short_leg_symbols:
            # Already accounted for as this spread's own short leg -- see
            # this function's own docstring. Not added to `reconciled` on
            # its own; the spread's long-leg entry (below, or already in
            # local_by_symbol) is what represents this position.
            continue
        if real_pos.get("side") == "short":
            # This strategy never intentionally opens/holds a naked short
            # by itself -- a real short position that ISN'T a known
            # spread's own leg is either a stale leg from a spread whose
            # local record was already dropped, or a genuine anomaly.
            # Either way, "sell to close" (what adopting this as naked
            # would attempt) would ADD to the short, not close it -- surface
            # it for a human instead of acting on it.
            logger.warning(
                "[alpaca_options_strategy] real SHORT options position with no matching tracked spread -- "
                "leaving alone rather than risk trading it backwards: %s x%d @ %.4f",
                symbol, int(real_pos["count"]), real_pos["entry_price"],
            )
            continue
        local = local_by_symbol.get(symbol)
        if local is None:
            logger.warning(
                "[alpaca_options_strategy] adopting untracked real position: %s x%d @ %.4f",
                symbol, int(real_pos["count"]), real_pos["entry_price"],
            )
            reconciled.append({
                "symbol": symbol, "underlying_symbol": symbol, "strategy": "naked",
                "entry_price": real_pos["entry_price"], "count": real_pos["count"],
                "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
            })
            continue
        # Real bug found in review while cleaning up after the incident
        # above: for a debit_spread, real_pos["entry_price"] here is only
        # ever the LONG leg's OWN fill price (from /v2/positions, one
        # symbol at a time) -- not this position's real entry_price, which
        # is the NET DEBIT of both legs combined (see scan_and_enter's own
        # net_debit). Correcting entry_price from real_pos for a spread
        # would silently replace a correct, cheaper net-debit cost basis
        # with a more expensive single-leg price, understating every real
        # gain and skewing take-profit/stop-loss thresholds off a wrong
        # baseline. Only `count` (contract quantity) is safe to sync this
        # way for a spread -- both legs share the same quantity, so the
        # long leg's own real qty is a valid ground truth for that.
        is_spread = local.get("strategy") == "debit_spread"
        if (
            abs(float(local["count"]) - real_pos["count"]) > 1e-9
            or (not is_spread and abs(float(local["entry_price"]) - real_pos["entry_price"]) > 1e-6)
        ):
            logger.warning(
                "[alpaca_options_strategy] correcting local position for %s: count %.4f->%.4f%s",
                symbol, float(local["count"]), real_pos["count"],
                "" if is_spread else f", entry {float(local['entry_price']):.4f}->{real_pos['entry_price']:.4f}",
            )
        local["count"] = real_pos["count"]
        if not is_spread:
            local["entry_price"] = real_pos["entry_price"]
        reconciled.append(local)

    for symbol in local_by_symbol:
        if symbol not in real:
            logger.warning("[alpaca_options_strategy] dropping phantom local position (no matching real fill): %s", symbol)

    return reconciled


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


def get_current_spread_price(long_symbol: str, short_symbol: str) -> float | None:
    """Net MID value of a vertical spread -- the long leg's own mid quote
    minus the short leg's own mid quote, same bid/ask-averaging
    simplification get_current_option_price already uses for a single
    contract, applied to both legs. This is what decide_exit/
    position_exit_levels compare against entry_price for a debit-spread
    position (see scan_and_enter) -- a rising net value is favorable, the
    exact same "long the underlying instrument" shape a naked long call/
    put already has, just computed from two quotes instead of one. None if
    either leg's quote is unavailable."""
    long_price = get_current_option_price(long_symbol)
    short_price = get_current_option_price(short_symbol)
    if long_price is None or short_price is None:
        return None
    return round(long_price - short_price, 4)


def scan_and_enter(symbols: list[str] | None = None, *, dry_run: bool | None = None) -> dict[str, Any]:
    """Evaluates each underlying for a directional options entry. Requires
    a real trained model (see evaluate_candidate) -- no technical-only
    cold-start fallback. Places a real plain market buy order for the
    chosen contract against the Alpaca account ALPACA_TRADING_BASE_URL
    points at UNLESS dry_run resolves True.

    Regular-hours only, unlike the equities/crypto strategies -- Alpaca
    does not support extended-hours trading on OPTIONS contracts at all
    (no analogous type="limit"+extended_hours=true path exists for them
    the way it does for stocks), so pre/post-market is skipped outright
    rather than attempting an order the broker would just reject. The
    underlying's OWN premarket/afterhours price action still reaches this
    strategy's features regardless -- alpaca_options_data.py reuses
    alpaca_data.py's equities feature engineering directly, and THAT data
    collection has no session gate at all (runs continuously, same as
    perps), so overnight moves in the underlying are already reflected by
    the time regular-hours entries evaluate here.

    "Pick the best times": also skips the first/last
    AVOID_SESSION_EDGE_MINUTES of the regular session even once it's open
    -- real options spreads are consistently widest right at the open
    (overnight-gap price discovery still settling) and right at the close
    (last-minute positioning), so a technically-valid signal there is
    priced worse than the exact same signal 15 minutes later. Exits are
    never gated by this -- managing existing risk is always allowed,
    only NEW entries wait out the edge."""
    from data.alpaca_data import fetch_recent_minute_bars, get_market_session
    from data.alpaca_options_data import get_options_universe, latest_feature_row, select_contract, select_spread_contracts
    from data.alpaca_options_model import predict_direction
    from data.stock_news import prewarm_sentiment
    from data import alpaca_client, threads_post

    if get_market_session()["session"] != "regular":
        return {"opened": [], "action": "market_not_regular_hours"}
    if _minutes_from_session_edge() < AVOID_SESSION_EDGE_MINUTES:
        return {"opened": [], "action": "too_close_to_session_edge"}

    effective_dry_run = (not LIVE_TRADING_ENABLED) if dry_run is None else dry_run
    if symbols is None:
        symbols = get_options_universe()

    opened: list[dict[str, Any]] = []
    with _STATE_LOCK:
        state = _load_state()
        if not effective_dry_run:
            # Ground-truth check first, before deciding anything -- see
            # _reconcile_positions_with_exchange. Dry-run positions are
            # purely hypothetical (no order was ever placed), so this only
            # ever runs when orders are actually being placed.
            state["positions"] = _reconcile_positions_with_exchange(state)
            _save_state(state)
        existing_underlyings = {p["underlying_symbol"] for p in (state.get("positions") or [])}
        open_count = len(state.get("positions") or [])
        try:
            available_balance_usd = get_available_balance()
        except Exception as exc:
            available_balance_usd = None
            logger.debug("[alpaca_options_strategy] balance read for daily reference failed: %s", exc)
        reference_was_just_set = _today_str() not in (state.get("daily_reference_balance") or {})
        reference_balance = _reference_balance_for_today(state, available_balance_usd)
        today_pnl = float((state.get("realized_pnl_by_date") or {}).get(_today_str(), 0.0))
        loss_cap_breached = bool(
            reference_balance and reference_balance > 0
            and today_pnl <= -abs(DAILY_LOSS_CAP_PCT) * reference_balance
        )
        # A confidence floor genuinely learned from this account's own real
        # trade history (see alpaca_options_trade_analysis.recommend_confidence_threshold
        # + apply_confidence_threshold_override above) -- falls back to the
        # module-level MODEL_CONFIDENCE_MIN default until enough real trades
        # exist to justify moving it. Same pattern perps_strategy.py uses.
        confidence_min_override = (state.get("tuning") or {}).get("model_confidence_min")
        _save_state(state, push_durable=reference_was_just_set)
    if loss_cap_breached:
        return {"opened": [], "action": "daily_loss_cap_breached"}

    # See stock_news.prewarm_sentiment's own docstring for the full,
    # confirmed root cause this fixes on the crypto side (same shape here)
    # -- fetches sentiment for every not-yet-held underlying CONCURRENTLY
    # so the sequential loop below hits a warm cache instead of each
    # underlying's own blocking network fetch.
    try:
        prewarm_sentiment([(s, None) for s in symbols if s not in existing_underlyings])
    except Exception as exc:
        logger.debug("[alpaca_options_strategy] sentiment prewarm failed (non-fatal): %s", exc)

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
            candidate = evaluate_candidate(row, model_prediction, confidence_min=confidence_min_override)
            if not candidate["should_enter"]:
                opened.append({"symbol": symbol, "ok": True, "action": "skipped", "reason": candidate["reason"]})
                continue

            if ENTRY_STRATEGY == "debit_spread":
                spread = select_spread_contracts(symbol, direction=candidate["direction"], current_price=row["current_price"])
                if spread is None:
                    opened.append({"symbol": symbol, "ok": True, "action": "skipped_no_contract"})
                    continue
                long_contract, short_contract = spread
                long_symbol, short_symbol = long_contract["symbol"], short_contract["symbol"]

                # net_debit is what a naked contract's own contract_price
                # represents below -- the "cost per position unit"
                # compute_contract_qty/position_exit_levels/decide_exit
                # are all already generic against, so nothing downstream
                # needs to know this came from two quotes instead of one.
                net_debit = get_current_spread_price(long_symbol, short_symbol)
                if net_debit is None or net_debit <= 0:
                    opened.append({"symbol": symbol, "ok": True, "action": "skipped_no_option_quote"})
                    continue

                available_balance = get_available_balance()
                qty = compute_contract_qty(available_balance, net_debit)
                if qty < 1:
                    opened.append({"symbol": symbol, "ok": True, "action": "skipped_insufficient_budget"})
                    continue

                # entry_volatility_30 threaded through here (not just onto
                # the stored position below) -- without it, the STORED
                # take_profit_price/stop_loss_price would use the flat
                # fallback percentages while the actual exit decision
                # (decide_exit, which reads position["entry_volatility_30"]
                # once the position exists) used the adaptive ones -- a
                # real bug this session already found and fixed once for
                # crypto's own identical entry-time context capture.
                levels = position_exit_levels({"entry_price": net_debit, "entry_volatility_30": row.get("volatility_30")})
                order_id = None
                if not effective_dry_run:
                    limit_price = net_debit * (1 + SPREAD_LIMIT_SLIPPAGE_PCT)
                    order_spec = alpaca_client.build_option_spread_order(
                        long_symbol=long_symbol, short_symbol=short_symbol, qty=qty, limit_price=limit_price,
                    )
                    order_id = alpaca_client.place_order(order_spec)

                position = {
                    "symbol": long_symbol, "underlying_symbol": symbol, "option_type": long_contract.get("type"),
                    "strategy": "debit_spread", "short_symbol": short_symbol,
                    "long_strike": long_contract.get("strike_price"), "short_strike": short_contract.get("strike_price"),
                    "expiration_date": long_contract.get("expiration_date"),
                    "entry_price": net_debit, "count": qty,
                    "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "order_id": order_id, "entry_volatility_30": row.get("volatility_30"),
                    **levels, **_entry_context(candidate, row),
                }
                contract_symbol, contract_type, entry_price = long_symbol, long_contract.get("type"), net_debit
            else:
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

                levels = position_exit_levels({"entry_price": contract_price, "entry_volatility_30": row.get("volatility_30")})
                order_id = None
                if not effective_dry_run:
                    order_spec = alpaca_client.build_option_order(symbol=contract_symbol, side="buy", qty=qty)
                    order_id = alpaca_client.place_order(order_spec)

                position = {
                    "symbol": contract_symbol, "underlying_symbol": symbol, "option_type": contract.get("type"),
                    "strategy": "naked",
                    "expiration_date": contract.get("expiration_date"), "strike_price": contract.get("strike_price"),
                    "entry_price": contract_price, "count": qty,
                    "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "order_id": order_id, "entry_volatility_30": row.get("volatility_30"),
                    **levels, **_entry_context(candidate, row),
                }
                contract_type, entry_price = contract.get("type"), contract_price

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
            trade_dry_run = effective_dry_run
            opened.append({
                "symbol": symbol, "ok": True, "action": "opened", "contract_symbol": contract_symbol,
                "strategy": position["strategy"], "option_type": contract_type, "entry_price": entry_price,
                "count": qty, "dry_run": trade_dry_run,
            })
            try:
                threads_post.post_trade_entry(
                    ticker=contract_symbol, side="long",
                    entry_price=entry_price, take_profit_price=levels["take_profit_price"],
                    stop_loss_price=levels["stop_loss_price"], reason=candidate["reason"], dry_run=trade_dry_run,
                    market="options",
                )
            except Exception:
                logger.warning("[alpaca_options_strategy] Threads post for %s entry failed", contract_symbol, exc_info=True)
            # Charts the UNDERLYING's own price action, not the option's
            # premium -- this pipeline doesn't record option-quote history
            # over time, only point-in-time quotes at scan time, so there's
            # no premium series to chart in the first place. Deliberately
            # passes NO entry/take-profit/stop-loss price-LEVEL lines
            # (those are premium dollars, a different scale than the
            # underlying's price -- would silently mislabel the chart);
            # only an ENTRY time-marker plus a subtitle making clear this is
            # the underlying, not the contract's own price.
            try:
                from data import chart_snapshot
                one_min_df = fetch_recent_minute_bars(symbol)
                indicators = chart_snapshot.format_technical_indicators(row)
                if candidate.get("probability_up") is not None:
                    indicators["Model prob up"] = f"{candidate['probability_up']:.1%}"
                threads_post.post_trade_entry_chart(
                    ticker=contract_symbol, market="options", candles=_candles_as_dicts(one_min_df),
                    entry_index=(len(one_min_df) - 1) if not one_min_df.empty else None,
                    side="long", dry_run=trade_dry_run, indicators=indicators,
                    subtitle=f"{symbol} underlying price action (option premium not charted separately)",
                )
            except Exception:
                logger.warning("[alpaca_options_strategy] Threads chart post for %s entry failed", contract_symbol, exc_info=True)
        except Exception as exc:
            opened.append({"symbol": symbol, "ok": False, "action": "entry_failed", "error": str(exc)})

    return {"opened": opened}


def manage_open_positions(*, dry_run: bool | None = None) -> dict[str, Any]:
    """Checks every open option position for an exit (take-profit/stop-
    loss/max-hold/near-expiration). No broker-native bracket to reconcile
    against -- a triggered exit is always a fresh, plain market sell of
    the contract, same as crypto's own manage_open_positions."""
    from data import threads_post
    from data.alpaca_data import fetch_recent_minute_bars

    effective_dry_run = (not LIVE_TRADING_ENABLED) if dry_run is None else dry_run
    with _STATE_LOCK:
        state = _load_state()
        if not effective_dry_run:
            state["positions"] = _reconcile_positions_with_exchange(state)
            _save_state(state)
        positions = list(state.get("positions") or [])
    if not positions:
        return {"action": "no_position", "checks": []}

    closed: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = []

    for position in positions:
        contract_symbol = position["symbol"]
        underlying_symbol = position["underlying_symbol"]
        is_spread = position.get("strategy") == "debit_spread"
        try:
            if is_spread:
                current_price = get_current_spread_price(contract_symbol, position["short_symbol"])
            else:
                current_price = get_current_option_price(contract_symbol)
            if current_price is None:
                checks.append({"symbol": contract_symbol, "ok": False, "error": "no_quote_available"})
                continue

            # Volume/momentum/breakout/sentiment (of the UNDERLYING, same
            # basis the entry model itself uses) only matter to decide_exit's
            # "promising position" extension, which only activates once a
            # position has already reached MAX_HOLD_MINUTES -- fetched
            # lazily, only in that case, to keep this loop cheap in the
            # common case (see perps_strategy.py's identical pattern).
            dollar_volume_z = momentum_pct = breakout_pct_b = sentiment_score_value = None
            opened_at_check = dt.datetime.fromisoformat(position["opened_at"])
            held_minutes_check = (dt.datetime.now(dt.timezone.utc) - opened_at_check).total_seconds() / 60.0
            if held_minutes_check >= MAX_HOLD_MINUTES:
                try:
                    from data.alpaca_data import latest_feature_row
                    promising_row = latest_feature_row(underlying_symbol)
                except Exception as exc:
                    promising_row = None
                    logger.debug("[alpaca_options_strategy] promising-signal feature fetch failed for %s: %s", underlying_symbol, exc)
                if promising_row:
                    dollar_volume_z = promising_row.get("dollar_volume_z")
                    momentum_pct = promising_row.get("macd_hist_pct")
                    breakout_pct_b = promising_row.get("bb_pct_b")
                    sentiment_score_value = promising_row.get("sentiment_score")

            should_exit, reason = decide_exit(
                position, current_price,
                dollar_volume_z=dollar_volume_z, momentum_pct=momentum_pct,
                breakout_pct_b=breakout_pct_b, sentiment_score=sentiment_score_value,
            )
            if not should_exit:
                checks.append({"symbol": contract_symbol, "ok": True, "exit_check": reason, "current_price": current_price})
                continue

            closed_count = float(position["count"])
            if not effective_dry_run:
                from data import alpaca_client
                try:
                    if is_spread:
                        # Closing a debit spread is typically a net CREDIT
                        # received (or a smaller debit paid, if closing at
                        # a loss) -- priced slightly below the current mid
                        # to stay marketable, same slippage buffer opening
                        # a spread uses in the opposite direction.
                        limit_price = max(0.01, current_price * (1 - SPREAD_LIMIT_SLIPPAGE_PCT))
                        order_spec = alpaca_client.build_option_spread_order(
                            long_symbol=contract_symbol, short_symbol=position["short_symbol"],
                            qty=int(position["count"]), limit_price=limit_price, closing=True,
                        )
                    else:
                        order_spec = alpaca_client.build_option_order(symbol=contract_symbol, side="sell", qty=int(position["count"]))
                    alpaca_client.place_order(order_spec)
                except Exception as exc:
                    logger.warning("[alpaca_options_strategy] close order failed for %s: %s", contract_symbol, exc)
                # A submitted close order is not a confirmed fill -- same
                # real-fill-verification discipline as alpaca_strategy.py's
                # own exit path fixed after a real, confirmed incident there:
                # booking P&L unconditionally produced duplicate trade_log
                # entries for the same real position across several
                # fast_check cycles before the order actually filled, each
                # re-discovered as "untracked" by reconciliation and
                # re-booked. Re-check the real remaining quantity on the
                # long leg (the one this position's own symbol/count already
                # tracks, spread or naked) right after, and only book what
                # actually closed.
                try:
                    pos_after = alpaca_client.get_position(contract_symbol)
                    remaining_qty = float(pos_after["qty"]) if pos_after else 0.0
                    closed_count = round(max(0.0, float(position["count"]) - remaining_qty), 6)
                except Exception as exc:
                    logger.warning(
                        "[alpaca_options_strategy] could not verify exit fill for %s after placing order -- assuming full close: %s",
                        contract_symbol, exc,
                    )
                if closed_count <= 0:
                    # Nothing actually filled yet -- keep monitoring the
                    # still-real position next cycle rather than booking a
                    # trade that never happened.
                    checks.append({"symbol": contract_symbol, "ok": False, "error": "exit_order_did_not_fill"})
                    continue

            gross = round((current_price - float(position["entry_price"])) * closed_count * 100, 6)
            opened_at = position.get("opened_at")
            closed_at = dt.datetime.now(dt.timezone.utc).isoformat()
            hold_minutes = None
            if opened_at:
                try:
                    opened_dt = dt.datetime.fromisoformat(opened_at)
                    hold_minutes = round((dt.datetime.now(dt.timezone.utc) - opened_dt).total_seconds() / 60, 2)
                except (ValueError, TypeError):
                    hold_minutes = None
            with _STATE_LOCK:
                state = _load_state()
                by_date = state.setdefault("realized_pnl_by_date", {})
                today = _today_str()
                by_date[today] = round(float(by_date.get(today, 0.0)) + gross, 6)
                trade = {
                    "closed_at": closed_at, "opened_at": opened_at, "hold_minutes": hold_minutes,
                    "symbol": contract_symbol, "underlying_symbol": underlying_symbol,
                    "strategy": position.get("strategy", "naked"), "option_type": position.get("option_type"),
                    "entry_price": position["entry_price"], "exit_price": current_price,
                    "count": closed_count, "realized_pnl_usd": gross, "reason": reason,
                    "dry_run": effective_dry_run,
                    # Entry-time context copied from the position -- see
                    # scan_and_enter's own _entry_context() for why.
                    "entry_probability_up": position.get("entry_probability_up"),
                    "entry_model_direction": position.get("entry_model_direction"),
                    "entry_reason": position.get("entry_reason"),
                    "entry_score": position.get("entry_score"),
                    "entry_dollar_volume_z": position.get("entry_dollar_volume_z"),
                    "entry_macd_hist_pct": position.get("entry_macd_hist_pct"),
                    "entry_bb_pct_b": position.get("entry_bb_pct_b"),
                    "entry_rsi_14": position.get("entry_rsi_14"),
                    "entry_sentiment_score": position.get("entry_sentiment_score"),
                }
                trade_log = state.setdefault("trade_log", [])
                trade_log.append(trade)
                if len(trade_log) > MAX_TRADE_LOG_ENTRIES:
                    del trade_log[: len(trade_log) - MAX_TRADE_LOG_ENTRIES]
                if closed_count < float(position["count"]) - 1e-6:
                    # Partial fill -- the remainder is still genuinely open,
                    # keep monitoring it rather than dropping it.
                    remaining_qty = round(float(position["count"]) - closed_count, 6)
                    state["positions"] = [
                        {**p, "count": remaining_qty} if p["symbol"] == contract_symbol else p
                        for p in (state.get("positions") or [])
                    ]
                else:
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
            try:
                from data import chart_snapshot
                one_min_df = fetch_recent_minute_bars(underlying_symbol)
                indicators = chart_snapshot.format_technical_indicators({
                    "rsi_14": trade.get("entry_rsi_14"), "macd_hist_pct": trade.get("entry_macd_hist_pct"),
                    "bb_pct_b": trade.get("entry_bb_pct_b"), "dollar_volume_z": trade.get("entry_dollar_volume_z"),
                    "sentiment_score": trade.get("entry_sentiment_score"),
                })
                if trade.get("entry_probability_up") is not None:
                    indicators["Model prob up"] = f"{trade['entry_probability_up']:.1%}"
                if hold_minutes is not None:
                    indicators["Held"] = f"{hold_minutes:.0f}min"
                indicators["Exit reason"] = str(reason)
                threads_post.post_trade_exit_chart(
                    ticker=contract_symbol, market="options", candles=_candles_as_dicts(one_min_df),
                    entry_index=_index_for_ts(one_min_df, opened_at), exit_index=_index_for_ts(one_min_df, closed_at),
                    side="long", pnl_usd=gross, dry_run=trade["dry_run"], indicators=indicators,
                    subtitle=f"{underlying_symbol} underlying price action (option premium not charted separately)",
                )
            except Exception:
                logger.warning("[alpaca_options_strategy] Threads exit chart post for %s failed", contract_symbol, exc_info=True)
        except Exception as exc:
            logger.warning("[alpaca_options_strategy] could not process position for %s -- leaving untouched this cycle: %s", contract_symbol, exc)
            checks.append({"symbol": contract_symbol, "ok": False, "error": str(exc)})

    if closed:
        _maybe_run_batch_trade_analysis()

    return {"action": "closed" if closed else "no_change", "closed": closed, "checks": checks}
