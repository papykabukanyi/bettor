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
import math
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
    cancel_margin_order, create_margin_order, get_margin_balance, get_margin_fee_tiers, get_margin_market,
    get_margin_order, get_margin_positions,
)
from data.perps_data import coin_for_ticker, fetch_candle_frames, get_watchlist, latest_feature_row
from data.perps_model import predict_direction
from data import perps_trade_analysis, threads_post

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


# Real transaction cost -- confirmed live via GET /margin/fee_tiers on this
# account: taker=0.008 (80 bps), maker=0.0005 (5 bps), flat across every
# ticker. Every entry/exit order this bot places uses time_in_force=
# immediate_or_cancel (a taker fill -- see the create_margin_order calls
# below), so a ROUND TRIP (entry + exit) costs ~1.6% of notional regardless
# of direction or outcome. Confirmed live: a real NEAR trade showed a gross
# gain of +$0.0608 book as a NET loss of -$0.1701 once real fees applied --
# every profit target below must clear this cost with real margin, or the
# strategy loses money even when its predictions are RIGHT.
_FEE_RATE_CACHE: dict[str, Any] = {"rates": None, "computed_at": 0.0}
_FEE_RATE_CACHE_TTL_SEC = 24 * 3600  # fee schedules don't change minute to minute
DEFAULT_TAKER_FEE_RATE = _env_float("PERPS_TAKER_FEE_RATE", 0.008)


def _taker_fee_rate(ticker: str) -> float:
    """Real per-leg taker fee rate for this ticker (decimal fraction of
    notional) -- falls back to DEFAULT_TAKER_FEE_RATE (the confirmed-live
    flat rate) if the live /margin/fee_tiers call fails or doesn't list
    this ticker."""
    now = time.time()
    if _FEE_RATE_CACHE["rates"] is None or (now - _FEE_RATE_CACHE["computed_at"]) >= _FEE_RATE_CACHE_TTL_SEC:
        try:
            _FEE_RATE_CACHE["rates"] = get_margin_fee_tiers().get("taker_fee_rates") or {}
        except Exception as exc:
            logger.warning("[perps_strategy] could not refresh fee tiers, using default taker rate: %s", exc)
            _FEE_RATE_CACHE["rates"] = {}
        _FEE_RATE_CACHE["computed_at"] = now
    return float((_FEE_RATE_CACHE["rates"] or {}).get(ticker, DEFAULT_TAKER_FEE_RATE))


def round_trip_fee_usd(
    ticker: str, entry_price: float, exit_price: float, count: float, *,
    entry_is_maker: bool = False, exit_is_maker: bool = False,
) -> float:
    """Real dollar cost of the round trip -- entry and exit are each their
    own fill, each charged whichever rate that specific fill actually was
    (maker or taker), not both assumed taker. A leg that was PARTIALLY a
    maker fill before falling back to taker for the remainder is simply
    counted as taker for this estimate -- Kalshi's own account balance is
    the real, authoritative ledger regardless; this figure only drives the
    bot's OWN trade-log display, so a mild taker-leaning approximation in
    the rare mixed-fill case is an acceptable, conservative simplification."""
    entry_rate = _maker_fee_rate(ticker) if entry_is_maker else _taker_fee_rate(ticker)
    exit_rate = _maker_fee_rate(ticker) if exit_is_maker else _taker_fee_rate(ticker)
    return round(entry_price * count * entry_rate + exit_price * count * exit_rate, 6)


# ── Maker (post_only) order placement, with a taker fallback ───────────────
# Confirmed via GET /margin/fee_tiers: maker fills cost 0.05% (5bps) per leg
# vs taker's 0.8% (80bps) -- a 16x difference. A backtest across 14 days of
# real Kalshi history (with fee-aware exit thresholds already tuned) found
# NO taker-fee parameter combination that was net profitable: every config
# lost money, and losses shrank monotonically as trade FREQUENCY was
# reduced, converging toward "don't trade at all" as the best taker-fee
# outcome. Placing a post_only maker order first -- falling back to a real
# taker fill only when reliability matters more than the fee savings (a
# stop-loss or max-hold exit) -- is the single highest-leverage lever
# available without touching the underlying entry/exit signal at all.
DEFAULT_MAKER_FEE_RATE = _env_float("PERPS_MAKER_FEE_RATE", 0.0005)
_MAKER_FEE_RATE_CACHE: dict[str, Any] = {"rates": None, "computed_at": 0.0}
# Default OFF: this changes HOW every real order is placed (a resting limit
# order that might not fill at all, instead of an immediate market-like
# fill) -- same "start conservative, prove it out, then enable" posture as
# LIVE_TRADING_ENABLED/ENABLE_SHORTS. Only flip on after reviewing a
# backtest run with this enabled.
ENABLE_MAKER_ORDERS = _env_flag("PERPS_ENABLE_MAKER_ORDERS", default=False)
MAKER_FILL_WAIT_SECONDS = _env_int("PERPS_MAKER_FILL_WAIT_SECONDS", 12)
MAKER_FILL_POLL_INTERVAL_SEC = _env_float("PERPS_MAKER_FILL_POLL_INTERVAL_SEC", 2.0)


def _maker_fee_rate(ticker: str) -> float:
    """Real per-leg MAKER fee rate for this ticker -- mirrors
    _taker_fee_rate's own live-fetch-then-cache pattern, just reading the
    "maker_fee_rates" side of the same /margin/fee_tiers response."""
    now = time.time()
    if _MAKER_FEE_RATE_CACHE["rates"] is None or (now - _MAKER_FEE_RATE_CACHE["computed_at"]) >= _FEE_RATE_CACHE_TTL_SEC:
        try:
            _MAKER_FEE_RATE_CACHE["rates"] = get_margin_fee_tiers().get("maker_fee_rates") or {}
        except Exception as exc:
            logger.warning("[perps_strategy] could not refresh maker fee tiers, using default: %s", exc)
            _MAKER_FEE_RATE_CACHE["rates"] = {}
        _MAKER_FEE_RATE_CACHE["computed_at"] = now
    return float((_MAKER_FEE_RATE_CACHE["rates"] or {}).get(ticker, DEFAULT_MAKER_FEE_RATE))


def _maker_price(order_side: str, market: dict[str, Any], tick_size: float) -> float | None:
    """The maker-safe price for a new post_only order -- joining the
    current best bid (a "bid"/buy order) or best ask (an "ask"/sell order)
    never crosses the spread, so the exchange won't reject it as an
    immediate cross. None if the market snapshot didn't carry a usable
    bid/ask (falls back to a plain taker order entirely in that case)."""
    raw = market.get("bid") if order_side == "bid" else market.get("ask")
    try:
        price = float(raw)
    except (TypeError, ValueError):
        return None
    if price <= 0:
        return None
    return _round_price(price, tick_size)


def _place_order_maker_then_fallback(
    *, ticker: str, order_side: str, count: float, maker_price: float, fallback_price: float,
    reduce_only: bool, urgent: bool,
) -> tuple[dict[str, Any] | None, str]:
    """Places a post_only (good_till_canceled) maker order first, to
    capture the far lower maker fee. Polls briefly for a fill; on timeout,
    cancels whatever's left resting. `urgent` callers (a stop-loss or
    max-hold exit, where a guaranteed exit matters more than the fee
    savings) get an immediate real taker fallback for the unfilled
    remainder. Non-urgent callers (a new entry, a take-profit/quick-profit
    exit) just get back whatever DID fill (possibly nothing) -- the
    existing caller-side "verify the real exchange position afterward"
    logic already treats a zero-fill result as "retry next cycle", so no
    special-casing is needed there.

    Returns (order_result_or_None, fill_type) where fill_type is "maker"
    (fully filled by the resting order), "taker_fallback" (the urgent
    fallback fired, with or without a partial maker fill first), or
    "unfilled" (non-urgent, nothing filled at all)."""
    order_result = create_margin_order(
        ticker=ticker, side=order_side, count=count, price=maker_price,
        client_order_id=str(uuid.uuid4()), time_in_force="good_till_canceled",
        post_only=True, reduce_only=reduce_only,
    )
    order = order_result.get("order") or order_result
    order_id = order.get("order_id")
    filled_count = 0.0
    remaining = count
    if order_id:
        deadline = time.monotonic() + MAKER_FILL_WAIT_SECONDS
        while time.monotonic() < deadline:
            time.sleep(MAKER_FILL_POLL_INTERVAL_SEC)
            try:
                status = get_margin_order(order_id).get("order") or {}
            except Exception as exc:
                logger.warning("[perps_strategy] could not poll maker order %s for %s: %s", order_id, ticker, exc)
                break
            filled_count = float(status.get("fill_count") or 0.0)
            remaining = float(status.get("remaining_count") or 0.0)
            if remaining <= 0:
                break
            if status.get("last_update_reason") == "PostOnlyCrossCancel":
                # Rejected outright -- the bid/ask snapshot used to price it
                # was already stale by the time it reached the exchange. No
                # point continuing to poll an order that no longer exists.
                break
        if remaining > 0:
            try:
                cancel_margin_order(order_id)
            except Exception as exc:
                logger.debug(
                    "[perps_strategy] cancel of unfilled/partial maker order %s failed "
                    "(may have just filled): %s", order_id, exc,
                )
    else:
        logger.warning("[perps_strategy] maker order for %s returned no order_id -- cannot poll for a fill", ticker)

    if remaining <= 0:
        return order_result, "maker"
    if not urgent:
        return (order_result if filled_count > 0 else None), "unfilled"

    fallback_result = create_margin_order(
        ticker=ticker, side=order_side, count=remaining, price=fallback_price,
        client_order_id=str(uuid.uuid4()), time_in_force="immediate_or_cancel", reduce_only=reduce_only,
    )
    return fallback_result, "taker_fallback"


# ── Tunable parameters (all overridable via env) ────────────────────────────
# "Break the total balance into portions and grow it": each new position is
# sized at this fraction of CURRENT available balance (so it compounds), and
# up to MAX_CONCURRENT_POSITIONS can be open at once (5 x 20% = the whole
# account fully deployed across up to 5 different instruments at a time).
POSITION_SIZE_PCT = _env_float("PERPS_POSITION_SIZE_PCT", 0.20)
MAX_CONCURRENT_POSITIONS = max(1, _env_int("PERPS_MAX_CONCURRENT_POSITIONS", 5))
# Confirmed via a real backtest (14 days, 8 watchlist coins, fresh model
# fit on today's expanded feature set): at the OLD defaults (0.4%/0.8%/30min),
# 87% of trades timed out at max_hold_time without ever reaching either
# threshold -- the position simply never had TIME to develop a real move.
# A dedicated hold-time sweep (10/15/20/30/45/60/90/120/180/240 minutes, all
# else held fixed) showed a clean, monotonic pattern: every single hold time
# was still net-unprofitable at Kalshi's taker fee rate, but losses shrank
# steadily as hold time grew (10min: -45.5% -> 240min: -35.4% over the same
# test window) -- more time genuinely lets a real (if still fee-challenged)
# move develop instead of forcing an exit on noise. 240 min was the least-
# bad result tested, but that's now suspiciously close to Kalshi's own
# ~4-hour funding interval (confirmed live via GET /margin/funding_rates/
# estimate) -- a position held that long risks actually crossing a real
# funding payment, an uncounted cost this backtest doesn't model at all.
# 180 min captures nearly all the same improvement (-39.0% vs -35.4%) while
# staying safely short of that boundary. TAKE_PROFIT_PCT/STOP_LOSS_PCT
# raised to genuinely fee-aware levels alongside this -- with only 30
# minutes to work with, a bigger target was moot (the price never got
# there anyway); with 180 minutes, it can actually matter.
# IMPORTANT, still true: none of this makes the strategy robustly
# profitable on its own -- it makes it lose LESS. The single biggest real
# lever found this session is enabling maker (post_only) orders
# (PERPS_ENABLE_MAKER_ORDERS, see below), which cuts the fee itself ~16x;
# that combined with this longer, more selective hold is the most credible
# path toward actual profitability, not either alone.
TAKE_PROFIT_PCT = _env_float("PERPS_TAKE_PROFIT_PCT", 0.02)
STOP_LOSS_PCT = _env_float("PERPS_STOP_LOSS_PCT", 0.015)
MAX_HOLD_MINUTES = _env_int("PERPS_MAX_HOLD_MINUTES", 180)
# Tried and reverted: an earlier "stale/flat position" early exit (closing a
# position early if it hadn't moved much in either direction by the halfway
# point of max_hold_time) was added and shipped based on a plausible
# hypothesis, then reverted after a real multi-fold walk-forward backtest
# (4 independent ~8-day test periods across 65 days of real history)
# showed it performed WORSE than not having it at every single hold time
# tested, 16/16 comparisons. The mechanism cut positions early without
# actually reducing the fee cost of a flat trade (that cost is roughly
# fixed regardless of when the position closes), while forfeiting the
# chance for a quiet-then-moving position to still reach take_profit. If
# revisiting this idea, backtest it first -- don't re-add on hypothesis alone.
#
# max_hold_time shouldn't be the ONLY factor forcing an exit -- a position
# that's already most of the way to its own take_profit target, OR one
# that's showing real building momentum even if price hasn't fully caught
# up yet, is doing exactly what it's supposed to; force-closing it right at
# the timeout throws away a likely winner for no reason. decide_exit()'s
# "promising" check is now an OR of two independent paths:
#   1. Price-progress: change_pct has already captured at least
#      PROMISING_PROGRESS_FRACTION of take_profit_pct.
#   2. Momentum/breakout confluence: real elevated volume
#      (dollar_volume_z >= PROMISING_VOLUME_Z, i.e. actual participation,
#      not noise) together with EITHER strong directional momentum
#      (macd_hist_pct >= PROMISING_MOMENTUM_PCT) OR a breakout signal
#      (bb_pct_b >= PROMISING_BREAKOUT_PCT_B, price pushing to/through its
#      own recent Bollinger range) -- AND the position isn't already
#      reversing (change_pct >= 0).
# Real backtest results on a 13.45-day window (same fitted model/test split
# used throughout this session's perps work): PROMISING_PROGRESS_FRACTION
# alone (0.25, recalibrated -- see below) gave a real, if modest,
# improvement over no extension at all (return -30.04% vs -30.20%, win
# rate 5.2% vs 3.5%, 58 trades). The momentum/breakout path tested in
# ISOLATION (extending on volume+momentum/breakout confluence WITHOUT
# price already confirming) came back slightly WORSE than no extension at
# all (-30.25% vs -30.20%) -- a technical signal without price
# confirmation turned out noisier than hoped, not the free win it looked
# like on paper. PROMISING_VOLUME_Z/MOMENTUM_PCT/BREAKOUT_PCT_B are
# therefore left at conservative (originally-derived, ~90th-percentile-of-
# all-data) levels that make this path rare/near-dormant by default rather
# than shipped as an active driver on unproven evidence -- fully wired and
# env-tunable if a larger real sample later justifies loosening them, but
# not turned on by default just because the capability exists. (Thresholds
# were first tried at the ~65-85th percentile of the CONDITIONAL
# distribution among positions actually surviving to 180min -- a real,
# n=25 sample -- which is what surfaced the isolated-path result above.)
# A position that's flat/losing and shows none of this is NOT "promising"
# and still exits on schedule exactly as before. MAX_HOLD_EXTENSION_MINUTES
# caps how much extra time ANY promising path can grant, so this can never
# run unbounded. Deliberately modest here (vs. the other 3 services)
# because Kalshi perps carries a real ~4-hour funding payment this backtest
# doesn't model (see MAX_HOLD_MINUTES's own earlier history above) -- the
# extended ceiling (180+45=225min) stays safely short of that boundary
# rather than risking an uncounted funding cost on top of an already-thin
# edge. A `sentiment_score`-based path also exists (see decide_exit's own
# docstring) but is NOT covered by the backtest below -- perps_backtest.py
# holds sentiment at a flat 0.0 (no free historical news archive), a
# disclosed, pre-existing limitation, not something this change fixes.
MAX_HOLD_EXTENSION_MINUTES = _env_int("PERPS_MAX_HOLD_EXTENSION_MINUTES", 45)
PROMISING_PROGRESS_FRACTION = _env_float("PERPS_PROMISING_PROGRESS_FRACTION", 0.25)
PROMISING_VOLUME_Z = _env_float("PERPS_PROMISING_VOLUME_Z", 1.0)
PROMISING_MOMENTUM_PCT = _env_float("PERPS_PROMISING_MOMENTUM_PCT", 0.0003)
PROMISING_BREAKOUT_PCT_B = _env_float("PERPS_PROMISING_BREAKOUT_PCT_B", 0.85)
PROMISING_SENTIMENT_SCORE = _env_float("PERPS_PROMISING_SENTIMENT_SCORE", 0.3)

# Explicit user direction: time alone shouldn't decide either side of the
# max_hold boundary -- a position shouldn't be force-held to the exact
# minute just to then get force-closed, and it shouldn't be force-closed
# purely because the clock ran out either. PRE_EXIT_STUDY_MINUTES opens a
# window BEFORE MAX_HOLD_MINUTES where decide_exit() runs a real model
# study (perps_model.predict_direction -- the same trained classifier
# entries use, hosted on Hugging Face Hub, hence "HF model") instead of
# only ever reacting once the deadline has already passed:
#   - Early quit: if the position is flat/losing (change_pct <= 0) AND the
#     model now confidently expects the OPPOSITE direction
#     (favorable_model_confidence <= 1-PROMISING_MODEL_CONFIDENCE) AND
#     volume isn't confirming continuation (dollar_volume_z <
#     PROMISING_VOLUME_Z) -- i.e. nothing is happening and the model has
#     turned against it -- close up to PRE_EXIT_STUDY_MINUTES early instead
#     of waiting out a dead clock.
#   - Extension: the same model confidence, now checked THE OTHER WAY
#     (favorable_model_confidence >= PROMISING_MODEL_CONFIDENCE), is added
#     as one more OR path alongside price/momentum/breakout/sentiment in
#     the existing max_hold "promising" check below.
# PROMISING_MODEL_CONFIDENCE: real multi-fold walk-forward backtest run
# (4 independent test folds across 65 days of real history, same
# methodology as PROMISING_PROGRESS_FRACTION's own validation, model
# predictions computed per-row exactly as perps_model.predict_direction
# would live) swept 0.55/0.56/0.58/0.62 against this feature fully OFF.
# 0.58 -- which turns out to exactly match MODEL_CONFIDENCE_MIN, the
# already-live entry conviction bar -- beat the OFF baseline in 4/4 folds
# (a clean, consistent win, if a modest one: return improved by roughly
# 0.01-0.13 percentage points per fold, trade count essentially unchanged).
# 0.55/0.56 fired more often but were a mixed bag (2/4 folds better, 2/4
# worse); 0.62 (this constant's first guess, before checking the model's
# actual probability spread) turned out almost never satisfiable -- this
# model's real probability_up rarely strays more than ~3-4 std deviations
# from 0.5, so 0.62 produced 0 early quits across all 4 folds and was a
# no-op. Lesson: pick this kind of threshold from the model's own real
# output distribution, not by analogy alone.
PRE_EXIT_STUDY_MINUTES = _env_float("PERPS_PRE_EXIT_STUDY_MINUTES", 6.0)
PROMISING_MODEL_CONFIDENCE = _env_float("PERPS_PROMISING_MODEL_CONFIDENCE", 0.58)

# ── Trend + trailing-stop strategy variant (flagged, default OFF) ──────────
# Real finding from a 2400-combo backtest grid + a genuine out-of-sample
# holdout check this session: the fixed take-profit/stop-loss shape above
# needs an ~87% win rate just to clear round-trip TAKER fees (2% floor TP
# - 1.6% fee = only 0.4% net win vs a 1% floor SL + fee = -2.6% net loss)
# -- but every entry signal tested produced a 4-13% win rate, nowhere
# close. This variant pairs three things that, TOGETHER, closed most of
# that gap in backtest (none alone was tested as sufficient):
#   1. Entry requires the 4h trend to actively AGREE with the trade
#      direction (buy dips only in uptrends, sell rallies only in
#      downtrends) -- trend_4h was captured but never used in any entry
#      decision before this.
#   2. Exit replaces the fixed take-profit with a trailing stop (let a
#      winner run, lock in profit as it develops, exit on a real
#      retracement rather than a fixed target) plus a much longer max_hold
#      (a real move needs time to develop) and a tighter flat stop-loss.
#   3. Maker-fee order placement (ENABLE_MAKER_ORDERS, cuts the fee itself
#      16x) -- must be enabled ALONGSIDE this for the backtest economics to
#      actually apply; this flag alone does not change how orders are
#      placed.
# Result: win rate rose to ~31% (vs 4-13%), gross return turned slightly
# positive, net loss shrank from -13.7% to -2.9%/+0.3% across the explore
# fold -- but the untouched holdout fold's own sample was too thin (3
# trades) to confirm robust profitability either way. Real, replicated
# IMPROVEMENT over the fixed-TP shape on the same holdout data, not yet
# proven profitable on its own -- shipped for genuine live forward testing
# per explicit direction, not as a finished, validated strategy. Default
# OFF, same "prove it out live" posture as ENABLE_MAKER_ORDERS/ENABLE_SHORTS.
# KNOWN, DISCLOSED GAP: TRAILING_MAX_HOLD_MINUTES's ~24h default crosses
# several of Kalshi's own ~4-hour funding cycles (see MAX_HOLD_MINUTES's
# own comment on why 180min was chosen specifically to stay under ONE
# cycle) -- funding cost is NOT modeled in the backtest above at all and
# could erode some or all of the improvement found. Watch real funding
# payments closely once this is live.
USE_TREND_TRAILING_STRATEGY = _env_flag("PERPS_USE_TREND_TRAILING_STRATEGY", default=False)
TRAILING_STOP_LOSS_PCT = _env_float("PERPS_TRAILING_STOP_LOSS_PCT", 0.01)
TRAILING_ACTIVATION_PCT = _env_float("PERPS_TRAILING_ACTIVATION_PCT", 0.08)
TRAILING_DISTANCE_PCT = _env_float("PERPS_TRAILING_DISTANCE_PCT", 0.02)
TRAILING_MAX_HOLD_MINUTES = _env_int("PERPS_TRAILING_MAX_HOLD_MINUTES", 1440)

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
# win rate / +4.2% at the old default. At the time of this specific tuning
# exercise, no Kalshi trading fees were modeled, so treat these particular
# absolute return figures as optimistic (fees are now modeled -- see
# round_trip_fee_usd / DEFAULT_TAKER_FEE_RATE above and perps_backtest.py's
# simulate() -- any NEW backtest run reflects real net-of-fee returns).
#
# Raised from 0.52 after this session's fee-aware sweeps: at 0.52, a real
# 14-day backtest lost money in every configuration tested regardless of
# exit thresholds or hold time, and tightening confidence consistently
# reduced the loss (e.g. at 0.58 + a 120min hold, win_rate rose to 5.8% and
# the loss shrank materially vs. 0.52 at the same hold, on real 8-coin
# history) -- same "fewer, higher-conviction trades cost less in fees"
# pattern as the hold-time finding above.
MODEL_CONFIDENCE_MIN = _env_float("PERPS_MODEL_CONFIDENCE_MIN", 0.58)
# Daily loss cap as a PERCENTAGE of the balance measured at the start of the
# day (not a fixed dollar figure) so it scales sensibly as the account grows.
DAILY_LOSS_CAP_PCT = _env_float("PERPS_DAILY_LOSS_CAP_PCT", 0.15)
# Real, confirmed production incident: trade_log had no size cap at all,
# an already-flagged candidate contributor to this service's own recurring
# OOM pattern over weeks of continuous live trading. Oldest entries trimmed
# first -- see manage_open_positions's own comment at the append site.
MAX_TRADE_LOG_ENTRIES = _env_int("PERPS_MAX_TRADE_LOG_ENTRIES", 2000)
LIVE_TRADING_ENABLED = _env_flag("KALSHI_PERPS_LIVE_TRADING_ENABLED", default=False)

# "When it's quick to go up, take profit": if a position is already up at
# least QUICK_PROFIT_PCT (smaller than the standard TAKE_PROFIT_PCT) AND that
# gain arrived fast (velocity over the trailing window >= this threshold),
# exit immediately rather than waiting for the standard take-profit level --
# a fast pump on a thin market often gives back gains just as fast.
# Raised alongside TAKE_PROFIT_PCT -- the OLD 0.15% was below the ~1.6%
# round-trip taker fee, meaning a "quick profit" exit was mathematically
# guaranteed to book a net loss even when it fired exactly as designed
# (confirmed directly against real trade history this session).
QUICK_PROFIT_PCT = _env_float("PERPS_QUICK_PROFIT_PCT", 0.018)
QUICK_PROFIT_VELOCITY_PCT_PER_MIN = _env_float("PERPS_QUICK_PROFIT_VELOCITY_PCT_PER_MIN", 0.006)
QUICK_PROFIT_WINDOW_SECONDS = _env_int("PERPS_QUICK_PROFIT_WINDOW_SECONDS", 90)

# "When it's choppy/fast, take the smaller sure thing": if the position's
# OWN recent price samples (the same ones _update_velocity already tracks --
# no extra API call) show stdev-of-returns above this threshold, the market
# is currently volatile, and a volatile move can reverse just as fast as it
# arrived. In that regime, take profit at a SMALLER gain than the normal
# quick-profit level rather than holding out for the full TAKE_PROFIT_PCT.
# Same fee-aware raise as QUICK_PROFIT_PCT above -- the old 0.1% couldn't
# possibly clear the real round-trip cost either.
HIGH_VOLATILITY_THRESHOLD = _env_float("PERPS_HIGH_VOLATILITY_THRESHOLD", 0.002)
VOLATILITY_QUICK_PROFIT_PCT = _env_float("PERPS_VOLATILITY_QUICK_PROFIT_PCT", 0.016)

# "Only enter when it can get out fast": confirmed live on the real account
# -- a real slice of exits are max_hold_time timeouts (sat the full 30
# minutes going nowhere, sometimes closing at a small loss) instead of a
# clean take-profit/quick-profit, meaning the entry fired in a market that
# wasn't actually moving enough for the fast-exit machinery to ever engage.
# Gating entries on the market's OWN current volatility_5 feature (already
# computed by engineer_features, no extra API call) means a position only
# ever opens somewhere plausibly volatile enough to swing into profit and
# get closed quickly -- repeatedly, many times a day -- rather than sitting
# and waiting on a calm one. Set below HIGH_VOLATILITY_THRESHOLD (the
# EXIT-side "market is choppy" bar) since requiring the exit-side bar just
# to enter would be overly restrictive.
MIN_ENTRY_VOLATILITY = _env_float("PERPS_MIN_ENTRY_VOLATILITY", 0.0008)

# "Study each currency": MIN_ENTRY_VOLATILITY above is one fixed number
# applied identically to all 16 instruments, but a coin's own NORMAL
# volatility varies a lot by instrument -- a level that's unremarkable for
# a naturally choppy small-cap could be a genuine spike for a calmer major.
# volatility_30 (engineer_features' own longer rolling window) is each
# ticker's OWN recent baseline, recomputed fresh every time from that same
# ticker's own data -- requiring volatility_5 (right now) to be at least
# this many TIMES that ticker's own volatility_30 means the gate adapts
# per-currency automatically, without needing a separate per-ticker
# historical-percentile system. Applied ALONGSIDE (not instead of)
# MIN_ENTRY_VOLATILITY -- the absolute floor still guards against entering
# anywhere when the whole market is dead, this catches "calm FOR THIS COIN
# specifically, even if technically above the absolute floor."
# Raised from 1.0 (no relative gate at all) after this session's sweeps
# showed requiring at least 1.3x a coin's own baseline volatility to enter
# consistently reduced both trade count and loss size on real history,
# without ever showing a clear win-rate downside.
MIN_ENTRY_RELATIVE_VOLATILITY_RATIO = _env_float("PERPS_MIN_ENTRY_RELATIVE_VOLATILITY_RATIO", 1.3)

# Real gap found in review, per explicit user direction: this strategy has
# always gated entries on volatility (see MIN_ENTRY_VOLATILITY/
# MIN_ENTRY_RELATIVE_VOLATILITY_RATIO above), but dollar_volume_z (real
# participation, not just price movement) was only ever CAPTURED at entry
# time for observability/the promising-position exit extension -- never
# actually a requirement to enter at all. A price move on thin volume is a
# materially weaker signal than the same move with real participation
# behind it. 1.0 (a meaningfully-above-average z-score, not an extreme
# one) matches PROMISING_VOLUME_Z's own already-validated bar for "real
# participation" on the exit side, applied here as an entry requirement
# too -- same alpaca_strategy.py/alpaca_crypto_strategy.py values, for
# consistency across all 4 services.
MIN_ENTRY_VOLUME_Z = _env_float("PERPS_MIN_ENTRY_VOLUME_Z", 1.0)

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
    (open positions can -- see _reconcile_positions_with_exchange).

    Real, confirmed bug found in review: "tuning" (the evidence-gated
    confidence-threshold override -- see apply_confidence_threshold_override)
    was missing from this slice entirely. Confirmed live: every deploy logs
    "recovered durable state from HF after local state was missing" (local
    disk doesn't survive a fresh deploy) -- so any confidence threshold
    this account had actually LEARNED from real trade history was silently
    reset back to the hardcoded MODEL_CONFIDENCE_MIN default on every single
    deploy, with no error or warning. The whole point of this mechanism is
    that it persists WITHOUT a redeploy; it wasn't surviving the deploys
    that keep happening anyway."""
    return {
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
                repo_id=HF_MODEL_REPO, repo_type="model", commit_message="update perps durable state",
            )
        finally:
            os.unlink(tmp_path)
    except Exception as exc:
        logger.warning("[perps_strategy] durable state push to HF failed: %s", exc)


_DURABLE_STATE_HF_TIMEOUT_SEC = int(os.getenv("PERPS_DURABLE_STATE_HF_TIMEOUT_SEC", "10") or "10")


def _pull_durable_state_from_hf() -> dict[str, Any] | None:
    if not HF_API_KEY:
        return None

    def _download() -> dict[str, Any]:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(
            repo_id=HF_MODEL_REPO, filename=_DURABLE_STATE_HF_FILENAME, repo_type="model", token=HF_API_KEY,
        )
        return json.loads(Path(path).read_text(encoding="utf-8"))

    try:
        # Real, confirmed production incident: this call, unbounded, hung
        # long enough (huggingface_hub's own internal session lock, not a
        # slow response -- see server_common.call_with_hard_timeout's own
        # docstring) to freeze this entire --workers 1 process for minutes,
        # 9 times in 24h, until gunicorn's own worker timeout finally
        # SIGKILLed it. A hard deadline here is what actually bounds it.
        from server_common import call_with_hard_timeout
        return call_with_hard_timeout(_download, timeout_sec=_DURABLE_STATE_HF_TIMEOUT_SEC)
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


def apply_confidence_threshold_override(new_threshold: float, *, reason: str) -> dict[str, Any]:
    """Applies an evidence-gated confidence-floor adjustment (see
    perps_trade_analysis.recommend_confidence_threshold) durably, WITHOUT a
    redeploy -- stored in state["tuning"] (pushed to HF like the rest of
    durable state) and read by scan_and_enter on every cycle, not the OS
    env var MODEL_CONFIDENCE_MIN is seeded from at import time. Deliberately
    NOT wired to touch Render's own deploy config -- granting the live
    service the ability to modify its own deployment is a materially
    different (and unnecessary) capability than adjusting a value it
    already reads from its own durable state every cycle."""
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


def evaluate_candidate(ticker: str, *, confidence_min: float | None = None) -> dict[str, Any]:
    """Combine the technical scalper filter with the direction model for one
    ticker, considering a LONG entry (dip + model predicts up) and, if
    ENABLE_SHORTS is on, a SHORT entry (rally + model predicts down) --
    mutually exclusive by construction (see decide_entry_technical), so at
    most one can qualify. If no trained model exists yet, the technical
    signal alone decides (clearly flagged) -- long side only, since shorting
    without any model confirmation at all is a materially different risk to
    take on unconfirmed technicals.

    confidence_min overrides the module-level MODEL_CONFIDENCE_MIN default
    when given -- see scan_and_enter, which reads a durable-state override
    set by perps_trade_analysis.recommend_confidence_threshold's own
    evidence-gated tuning (apply_confidence_threshold_override), so a
    confidence floor genuinely learned from this account's real trade
    history can take effect without a redeploy."""
    row = latest_feature_row(ticker)
    if row is None:
        return {"ticker": ticker, "should_enter": False, "reason": "no_feature_data", "model_ok": False, "technical_ok": False}

    prediction = predict_direction(ticker)
    model_ok = bool(prediction.get("model_ok"))

    result: dict[str, Any] = {
        "ticker": ticker, "current_price": row["current_price"], "short_ma": row["short_ma"],
        "trend_pct": row["trend_pct"], "model_ok": model_ok, "technical_ok": False,
        # This coin's OWN recent volatility at the moment of evaluation --
        # carried through to the eventual position dict (see scan_and_enter)
        # so decide_exit/position_exit_levels can customize this position's
        # take-profit/stop-loss to THIS specific currency (see
        # adaptive_exit_pcts) instead of one flat percentage for every coin.
        "volatility_30": row.get("volatility_30"),
        # Raw indicator values at decision time -- not used by any entry
        # RULE here (the technical/model filters above are unchanged), only
        # carried through for observability: the entry-chart's indicator
        # panel (see threads_post.post_trade_entry_chart) and the trade_log
        # entry-context snapshot both read these so a Threads follower (and
        # a future trade-analysis pass) can see what the bot actually saw,
        # not just what it decided.
        "dollar_volume_z": row.get("dollar_volume_z"), "macd_hist_pct": row.get("macd_hist_pct"),
        "bb_pct_b": row.get("bb_pct_b"), "rsi_14": row.get("rsi_14"),
        "sentiment_score": row.get("sentiment_score"),
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

        # Only enter where a fast exit is actually plausible -- confirmed
        # live on the real account: a real share of exits were max_hold_time
        # timeouts (sat the full 30 minutes going nowhere, sometimes closing
        # at a small loss) rather than a clean take-profit/quick-profit,
        # meaning the entry fired somewhere that just wasn't moving. Applies
        # to both sides equally (a calm market is equally uninteresting
        # whichever direction the dip/rally pointed).
        current_volatility = row.get("volatility_5")
        result[f"{side}_volatility"] = current_volatility
        if current_volatility is None or current_volatility < MIN_ENTRY_VOLATILITY:
            reasons.append(f"{technical_reason}, but volatility too low for a fast exit ({current_volatility})")
            continue

        # Per-currency relative check: is THIS coin currently more active
        # than ITS OWN recent (30-min) baseline, not just above the global
        # absolute floor above.
        own_baseline = row.get("volatility_30")
        if own_baseline and own_baseline > 0:
            relative_ratio = current_volatility / own_baseline
            if relative_ratio < MIN_ENTRY_RELATIVE_VOLATILITY_RATIO:
                reasons.append(
                    f"{technical_reason}, but calm relative to this coin's own baseline "
                    f"({relative_ratio:.2f}x < {MIN_ENTRY_RELATIVE_VOLATILITY_RATIO}x)"
                )
                continue

        # Real participation, not just price movement -- a dip/rally on
        # thin volume is a materially weaker signal than the same move
        # with real activity behind it. See MIN_ENTRY_VOLUME_Z's own
        # comment for why this was previously only captured, never
        # enforced, at entry time.
        entry_volume_z = row.get("dollar_volume_z")
        if entry_volume_z is None or entry_volume_z < MIN_ENTRY_VOLUME_Z:
            reasons.append(f"{technical_reason}, but volume not unusual enough (z={entry_volume_z})")
            continue

        # See USE_TREND_TRAILING_STRATEGY's own comment for the full
        # rationale/backtest -- requires the 4h trend to actively AGREE
        # with this trade's direction, not just "not strongly against it"
        # (TREND_FILTER_DOWN_PCT's existing, separate check above).
        if USE_TREND_TRAILING_STRATEGY:
            trend_4h = row.get("trend_4h")
            if trend_4h is None:
                reasons.append(f"{technical_reason}, but no 4h trend data")
                continue
            if side == "long" and trend_4h <= 0:
                reasons.append(f"{technical_reason}, but 4h trend not up ({trend_4h:+.3%})")
                continue
            if side == "short" and trend_4h >= 0:
                reasons.append(f"{technical_reason}, but 4h trend not down ({trend_4h:+.3%})")
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
        effective_confidence_min = confidence_min if confidence_min is not None else MODEL_CONFIDENCE_MIN
        if prediction["direction"] == wanted_direction and confidence >= effective_confidence_min:
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
    tickers: list[str] | None = None, *, exclude: set[str] | None = None, confidence_min: float | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Evaluate every ticker in the watchlist (minus any already held);
    return every qualifying candidate ranked best-first, plus every
    candidate's evaluation for observability. confidence_min passed straight
    through to evaluate_candidate -- see its own docstring."""
    watchlist = tickers or get_watchlist()
    held = exclude or set()
    candidates = [evaluate_candidate(t, confidence_min=confidence_min) for t in watchlist if t not in held]
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


# "Study each currency" applied to EXITS, not just entry selectivity.
# Confirmed via a per-ticker backtest breakdown this session: BTC's mean
# volatility_30 (~0.037%/min) is roughly 4x LOWER than kSHIB's (~0.156%/min)
# -- a flat 2%/1.5% take-profit/stop-loss applied identically to both means
# BTC (which rarely moves that much) almost never reaches its target,
# while a genuinely volatile small-cap's target may be too conservative
# either way. Instead, each position's EFFECTIVE take-profit/stop-loss/
# quick-profit is derived from THAT SPECIFIC ticker's own volatility_30 at
# the moment it was entered, scaled by sqrt(MAX_HOLD_MINUTES) (volatility
# scales with the square root of time under a random-walk assumption, so
# this is an estimate of the coin's own typical move size over the actual
# hold window, not an arbitrary flat number).
# Tuned via a pooled sweep on the same real 14-day/8-coin history: tried
# (tp_mult, sl_mult) in {(1.0,0.7), (1.0,0.9), (1.2,0.9), (1.2,1.1), (1.5,1.0),
# (1.5,1.2), (2.0,1.5)} -- results consistently ranked "wider" combos above
# "tighter" ones (best: 1.5/1.0 at -26.2% pooled return; worst: 1.0/0.9 at
# -27.5%), so a too-tight per-currency stop-loss was clipping winners early
# more than it was saving losers, same pattern as this session's earlier
# hold-time sweep (more room outperformed less room, though nothing tested
# flipped the strategy net-profitable at the taker fee rate).
TAKE_PROFIT_VOL_MULTIPLE = _env_float("PERPS_TAKE_PROFIT_VOL_MULTIPLE", 1.5)
STOP_LOSS_VOL_MULTIPLE = _env_float("PERPS_STOP_LOSS_VOL_MULTIPLE", 1.0)
# Floors/ceilings guard against a pathological per-currency estimate (a
# coin with almost no recorded volatility yet shouldn't get a near-zero
# target that fires on pure noise; a wildly volatile one shouldn't get an
# unreasonably huge target that never fires either).
#
# Real, confirmed finding from reviewing actual trade history: the old
# 0.012 (1.2%) floor sat BELOW the real ~1.6% round-trip taker fee
# (DEFAULT_TAKER_FEE_RATE=0.008, both legs) -- two real trades
# (KXBCHPERP/KXHYPEPERP, both "take_profit" exits, +0.544%/+0.821% gross)
# hit that floor and still booked a NET LOSS despite correctly predicting
# direction and reaching their own target. A take-profit floor below the
# fee cost is a structural leak, not a strategy tradeoff -- raised to sit
# comfortably above it (matches TAKE_PROFIT_PCT's own flat fallback value).
MIN_TAKE_PROFIT_PCT = _env_float("PERPS_MIN_TAKE_PROFIT_PCT", 0.02)
MAX_TAKE_PROFIT_PCT = _env_float("PERPS_MAX_TAKE_PROFIT_PCT", 0.05)
MIN_STOP_LOSS_PCT = _env_float("PERPS_MIN_STOP_LOSS_PCT", 0.01)
MAX_STOP_LOSS_PCT = _env_float("PERPS_MAX_STOP_LOSS_PCT", 0.04)


def adaptive_exit_pcts(entry_volatility_30: float | None) -> dict[str, float]:
    """Take-profit/stop-loss/quick-profit percentages customized to ONE
    specific currency's own volatility at entry time. Falls back to the
    flat global TAKE_PROFIT_PCT/STOP_LOSS_PCT/etc. defaults if no
    volatility was captured (e.g. a position opened before this field
    existed, or a technical-only entry where the value was unavailable) --
    same value every position used before this change, so nothing regresses
    for positions that predate it."""
    if not entry_volatility_30 or entry_volatility_30 <= 0:
        return {
            "take_profit_pct": TAKE_PROFIT_PCT, "stop_loss_pct": STOP_LOSS_PCT,
            "quick_profit_pct": QUICK_PROFIT_PCT, "volatility_quick_profit_pct": VOLATILITY_QUICK_PROFIT_PCT,
        }
    horizon_scale = math.sqrt(max(1, MAX_HOLD_MINUTES))
    take_profit = min(MAX_TAKE_PROFIT_PCT, max(MIN_TAKE_PROFIT_PCT, TAKE_PROFIT_VOL_MULTIPLE * entry_volatility_30 * horizon_scale))
    stop_loss = min(MAX_STOP_LOSS_PCT, max(MIN_STOP_LOSS_PCT, STOP_LOSS_VOL_MULTIPLE * entry_volatility_30 * horizon_scale))
    return {
        "take_profit_pct": take_profit,
        "stop_loss_pct": stop_loss,
        # Quick-profit levels stay a fraction of the (now per-currency)
        # take-profit target -- same "smaller than the full target" shape
        # as the old flat constants, just scaled with everything else.
        "quick_profit_pct": take_profit * 0.9,
        "volatility_quick_profit_pct": take_profit * 0.8,
    }


def _decide_exit_trailing(position: dict[str, Any], current_price: float, *, now: dt.datetime | None = None) -> tuple[bool, str]:
    """USE_TREND_TRAILING_STRATEGY's own exit half -- see that constant's
    module-level comment for the full backtest/rationale. Deliberately
    simple/self-contained (stop_loss + trailing + max_hold only, none of
    take_profit/quick_profit/pre_exit_study/promising-extension) -- those
    mechanisms were tuned and validated against the OLD fixed-take-profit
    shape, and mixing them with a trailing stop was never backtested, so
    this implements EXACTLY the shape that real 2400-combo grid + holdout
    check actually covered, not a guessed hybrid of the two."""
    entry_price = float(position["entry_price"])
    is_short = position.get("side") == "short"
    change_pct = (
        (entry_price - current_price) / entry_price if is_short else (current_price - entry_price) / entry_price
    ) if entry_price > 0 else 0.0
    if change_pct <= -TRAILING_STOP_LOSS_PCT:
        return True, f"stop_loss ({change_pct:+.3%}, target {TRAILING_STOP_LOSS_PCT:.2%})"

    peak = position.get("_trail_peak", entry_price)
    peak = min(peak, current_price) if is_short else max(peak, current_price)
    position["_trail_peak"] = peak
    peak_change_pct = (entry_price - peak) / entry_price if is_short else (peak - entry_price) / entry_price
    if peak_change_pct >= TRAILING_ACTIVATION_PCT:
        retrace_pct = (
            (current_price - peak) / entry_price if is_short else (peak - current_price) / entry_price
        )
        if retrace_pct >= TRAILING_DISTANCE_PCT:
            return True, f"trailing_stop (peak {peak_change_pct:+.3%}, retraced {retrace_pct:.3%})"

    opened_at = dt.datetime.fromisoformat(position["opened_at"])
    now = now if now is not None else dt.datetime.now(dt.timezone.utc)
    held_minutes = (now - opened_at).total_seconds() / 60.0
    if held_minutes >= TRAILING_MAX_HOLD_MINUTES:
        return True, f"max_hold_time ({held_minutes:.0f}min, {change_pct:+.3%})"
    return False, f"holding ({change_pct:+.3%}, peak {peak_change_pct:+.3%}, {held_minutes:.0f}min)"


def decide_exit(
    position: dict[str, Any], current_price: float, *,
    velocity_pct_per_min: float | None = None, external_velocity_pct_per_min: float | None = None,
    current_volatility: float | None = None, now: dt.datetime | None = None,
    dollar_volume_z: float | None = None, momentum_pct: float | None = None,
    breakout_pct_b: float | None = None, sentiment_score: float | None = None,
    model_ok: bool = False, probability_up: float | None = None,
) -> tuple[bool, str]:
    """See USE_TREND_TRAILING_STRATEGY's own comment -- when enabled, exits
    take the simpler, separately-validated trailing-stop path below instead
    of everything else in this function.

    `current_price` is always Kalshi's own tradable quote -- that's what
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
    means the same thing regardless of which way this position is facing.

    `now` defaults to the real wall-clock time (correct for live trading,
    where "now" really is now). A backtest replaying HISTORICAL rows must
    pass its own simulated current timestamp here instead -- confirmed as a
    real bug: without this, held_minutes was computed against the real
    date this backtest happens to be RUN on minus a position opened weeks
    or months in the past, producing an enormous number that blew past
    MAX_HOLD_MINUTES on virtually every position's very first tick after
    opening, regardless of price movement (measured live: 99.7% of
    simulated exits were max_hold_time before this fix, vs a healthy mix of
    take-profit/stop-loss/max_hold in real production trades).

    `dollar_volume_z`/`momentum_pct` (macd_hist_pct)/`breakout_pct_b`
    (bb_pct_b)/`sentiment_score` feed the max_hold_time "promising position"
    extension only (see PROMISING_PROGRESS_FRACTION's own comment) -- all
    optional and side-aware exactly like velocity above, so an omitted
    value simply can't contribute to that decision rather than erroring.
    `sentiment_score` in particular is NOT validated by this repo's own
    backtest (perps_backtest.py holds it at a flat 0.0 -- there's no free
    historical news archive, a disclosed, known limitation), so treat that
    one path as a reasoned-but-backtest-unproven addition.

    `model_ok`/`probability_up` are perps_model.predict_direction's own
    output for this ticker, RE-checked close to the max_hold deadline (see
    PRE_EXIT_STUDY_MINUTES's own comment) rather than trusted from entry
    time -- a live conviction read, not a stale one. Feeds both the early
    pre-exit-study quit path and the max_hold "promising" extension path;
    omit both (the default) to fall back to the pre-existing technical-only
    behavior exactly."""
    if USE_TREND_TRAILING_STRATEGY:
        return _decide_exit_trailing(position, current_price, now=now)
    entry_price = float(position["entry_price"])
    is_short = position.get("side") == "short"
    exit_pcts = adaptive_exit_pcts(position.get("entry_volatility_30"))
    take_profit_pct = exit_pcts["take_profit_pct"]
    stop_loss_pct = exit_pcts["stop_loss_pct"]
    quick_profit_pct = exit_pcts["quick_profit_pct"]
    volatility_quick_profit_pct = exit_pcts["volatility_quick_profit_pct"]
    if is_short:
        change_pct = (entry_price - current_price) / entry_price if entry_price > 0 else 0.0
        favorable_velocity = -velocity_pct_per_min if velocity_pct_per_min is not None else None
        favorable_external_velocity = -external_velocity_pct_per_min if external_velocity_pct_per_min is not None else None
        favorable_momentum = -momentum_pct if momentum_pct is not None else None
        favorable_breakout = (1.0 - breakout_pct_b) if breakout_pct_b is not None else None
        favorable_sentiment = -sentiment_score if sentiment_score is not None else None
        favorable_model_confidence = (1.0 - probability_up) if (model_ok and probability_up is not None) else None
    else:
        change_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0.0
        favorable_velocity = velocity_pct_per_min
        favorable_external_velocity = external_velocity_pct_per_min
        favorable_momentum = momentum_pct
        favorable_breakout = breakout_pct_b
        favorable_sentiment = sentiment_score
        favorable_model_confidence = probability_up if (model_ok and probability_up is not None) else None

    if change_pct >= quick_profit_pct:
        if favorable_velocity is not None and favorable_velocity >= QUICK_PROFIT_VELOCITY_PCT_PER_MIN:
            return True, f"quick_profit (velocity {velocity_pct_per_min:+.2%}/min, gain {change_pct:+.3%})"
        if favorable_external_velocity is not None and favorable_external_velocity >= QUICK_PROFIT_VELOCITY_PCT_PER_MIN:
            return True, f"quick_profit (external velocity {external_velocity_pct_per_min:+.2%}/min, gain {change_pct:+.3%})"
    if (
        current_volatility is not None and current_volatility >= HIGH_VOLATILITY_THRESHOLD
        and change_pct >= volatility_quick_profit_pct
    ):
        return True, f"volatility_quick_profit (volatility {current_volatility:.4f}, gain {change_pct:+.3%})"
    if change_pct >= take_profit_pct:
        return True, f"take_profit ({change_pct:+.3%}, target {take_profit_pct:.2%})"
    if change_pct <= -stop_loss_pct:
        return True, f"stop_loss ({change_pct:+.3%}, target {stop_loss_pct:.2%})"
    opened_at = dt.datetime.fromisoformat(position["opened_at"])
    now = now if now is not None else dt.datetime.now(dt.timezone.utc)
    held_minutes = (now - opened_at).total_seconds() / 60.0

    # Pre-exit study: within PRE_EXIT_STUDY_MINUTES of the max_hold
    # deadline, a flat/losing position (no other exit path has fired yet
    # at this point) that the model now confidently expects to REVERSE,
    # with no volume confirming continuation, gets closed a few minutes
    # early rather than forced to sit out a dead clock. See
    # PRE_EXIT_STUDY_MINUTES's own comment for the full rationale.
    if MAX_HOLD_MINUTES - PRE_EXIT_STUDY_MINUTES <= held_minutes < MAX_HOLD_MINUTES and change_pct <= 0:
        volume_confirmed_early = dollar_volume_z is not None and dollar_volume_z >= PROMISING_VOLUME_Z
        model_favors_reversal = (
            favorable_model_confidence is not None
            and favorable_model_confidence <= (1.0 - PROMISING_MODEL_CONFIDENCE)
        )
        if model_favors_reversal and not volume_confirmed_early:
            return True, (
                f"pre_exit_study ({held_minutes:.0f}min, {MAX_HOLD_MINUTES - held_minutes:.0f}min early: "
                f"model favors reversal p_up={probability_up:.2f}, no confirming volume z={dollar_volume_z})"
            )

    if held_minutes >= MAX_HOLD_MINUTES:
        # A position already most of the way to take_profit, or one showing
        # real building momentum/breakout on real volume, gets extra time
        # instead of being force-closed on a blind timeout -- see
        # PROMISING_PROGRESS_FRACTION's own comment for the full rationale
        # and each threshold's justification. A flat/losing position
        # showing none of this is not "promising" and exits on schedule
        # exactly as before.
        progress_frac = (change_pct / take_profit_pct) if take_profit_pct > 0 else 0.0
        price_promising = progress_frac >= PROMISING_PROGRESS_FRACTION
        volume_confirmed = dollar_volume_z is not None and dollar_volume_z >= PROMISING_VOLUME_Z
        momentum_promising = (
            volume_confirmed and change_pct >= 0
            and favorable_momentum is not None and favorable_momentum >= PROMISING_MOMENTUM_PCT
        )
        breakout_promising = (
            volume_confirmed and change_pct >= 0
            and favorable_breakout is not None and favorable_breakout >= PROMISING_BREAKOUT_PCT_B
        )
        sentiment_promising = favorable_sentiment is not None and favorable_sentiment >= PROMISING_SENTIMENT_SCORE
        model_promising = favorable_model_confidence is not None and favorable_model_confidence >= PROMISING_MODEL_CONFIDENCE
        promising = price_promising or momentum_promising or breakout_promising or sentiment_promising or model_promising
        if not promising or held_minutes >= MAX_HOLD_MINUTES + MAX_HOLD_EXTENSION_MINUTES:
            return True, f"max_hold_time ({held_minutes:.0f}min, {change_pct:+.3%})"
    return False, f"holding ({change_pct:+.3%}, {held_minutes:.0f}min)"


def position_exit_levels(position: dict[str, Any]) -> dict[str, float]:
    """The actual take-profit/stop-loss/quick-profit PRICE levels for a
    position, derived from the SAME per-currency-adaptive percentages
    decide_exit() applies (see adaptive_exit_pcts) -- exists so callers
    (the dashboard) can show, per open position, PROOF that it has real
    exit levels defined, rather than just trusting the config exists
    somewhere. Side-aware: a short's take-profit is BELOW entry (profits
    on a falling price) and its stop-loss is ABOVE entry, the mirror image
    of a long."""
    entry_price = float(position["entry_price"])
    sign = -1.0 if position.get("side") == "short" else 1.0
    exit_pcts = adaptive_exit_pcts(position.get("entry_volatility_30"))
    return {
        "take_profit_price": round(entry_price * (1 + sign * exit_pcts["take_profit_pct"]), 6),
        "stop_loss_price": round(entry_price * (1 - sign * exit_pcts["stop_loss_pct"]), 6),
        "quick_profit_price": round(entry_price * (1 + sign * exit_pcts["quick_profit_pct"]), 6),
    }


def _candles_as_dicts(df) -> list[dict[str, Any]]:
    """Converts a fetch_candle_frames-style DataFrame into the plain
    list[dict] shape both chart_snapshot.generate_candlestick_chart and
    perps_trade_analysis expect -- keeps those two modules pandas-free."""
    if df is None or df.empty:
        return []
    cols = [c for c in ("ts", "open", "high", "low", "close") if c in df.columns]
    return df[cols].to_dict("records")


def _index_for_ts(df, iso_ts: str | None) -> int | None:
    """Which row of a fetch_candle_frames-style DataFrame is closest to a
    given ISO timestamp -- used to mark ENTRY/EXIT on the candlestick chart.
    None if there's no timestamp, no data, or the closest candle is more
    than an hour away (the trade's window genuinely isn't covered by this
    rolling-lookback data -- a wildly wrong index would mislabel the
    chart, so skip the marker instead of guessing)."""
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
    """Every perps_trade_analysis.BATCH_SIZE newly-closed REAL trades,
    studies that recent batch -- win/loss patterns, missed-profit/
    premature-stop diagnostics from real OHLC (see
    perps_trade_analysis.analyze_recent_trade_batch) -- and posts a Threads
    snapshot. Also opportunistically re-runs the existing evidence-gated
    confidence-threshold tuning (the same code app_kalshi.py's daily job
    already uses) so genuine drift gets caught faster than once a day.
    Called right after manage_open_positions closes trades, OUTSIDE
    _STATE_LOCK (same reasoning as every other Threads/network call in
    this module -- must never hold up the fast exit-check loop). Best-
    effort: any failure here is logged and swallowed, never allowed to
    affect trading."""
    try:
        with _STATE_LOCK:
            state = _load_state()
            trade_log = state.get("trade_log") or []
            real_trades = [t for t in trade_log if not t.get("dry_run")]
            last_count = int(state.get("last_batch_analysis_trade_count") or 0)
            if len(real_trades) - last_count < perps_trade_analysis.BATCH_SIZE:
                return
            state["last_batch_analysis_trade_count"] = len(real_trades)
            _save_state(state, push_durable=True)
            current_threshold = (state.get("tuning") or {}).get("model_confidence_min", MODEL_CONFIDENCE_MIN)

        recent = real_trades[-perps_trade_analysis.BATCH_SIZE:]
        candles_by_ticker: dict[str, list[dict[str, Any]]] = {}
        for ticker in {t.get("ticker") for t in recent if t.get("ticker")}:
            try:
                one_min_df, _ = fetch_candle_frames(ticker)
                candles_by_ticker[ticker] = _candles_as_dicts(one_min_df)
            except Exception:
                logger.debug("[perps_strategy] candle fetch for batch analysis failed for %s", ticker, exc_info=True)

        batch = perps_trade_analysis.analyze_recent_trade_batch(real_trades, candles_by_ticker=candles_by_ticker)
        text = perps_trade_analysis.format_batch_snapshot_text(batch, market="perps")
        threads_post.post_trade_analysis_summary(text, market="perps")

        tuning_rec = perps_trade_analysis.recommend_confidence_threshold(real_trades, current_threshold=current_threshold)
        if tuning_rec.get("should_apply"):
            apply_confidence_threshold_override(tuning_rec["recommended_threshold"], reason="5-trade batch review")
    except Exception:
        logger.warning("[perps_strategy] batch trade analysis failed", exc_info=True)


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
    re-derive it.

    Does NOT filter on `is_portfolio` -- confirmed live and a real bug in an
    earlier version of this function: `is_portfolio` distinguishes
    portfolio- vs isolated-margined positions (rows without it are simply
    missing a `margin_used`/`roe` field), NOT "real vs not real" as
    originally assumed. A real, non-zero, real-money HYPE position with
    is_portfolio=False was silently excluded by that filter -- adopted by
    nothing, monitored by nothing, sitting with zero stop-loss/take-profit
    coverage while it lost money. Kalshi can return multiple rows for the
    same ticker across margin modes/subaccounts, so this sums signed counts
    and uses a count-weighted average entry price per ticker rather than
    letting one row silently overwrite another."""
    try:
        data = get_margin_positions()
    except Exception as exc:
        logger.warning("[perps_strategy] could not fetch real positions for reconciliation: %s", exc)
        return None
    totals: dict[str, dict[str, float]] = {}
    for p in data.get("positions") or []:
        ticker = p.get("market_ticker")
        raw_count = float(p.get("position") or 0.0)
        if not ticker or raw_count == 0:
            continue
        entry_price = float(p.get("entry_price") or 0.0)
        agg = totals.setdefault(ticker, {"signed_count": 0.0, "weighted_entry_sum": 0.0, "abs_count_sum": 0.0})
        agg["signed_count"] += raw_count
        agg["weighted_entry_sum"] += entry_price * abs(raw_count)
        agg["abs_count_sum"] += abs(raw_count)

    result: dict[str, dict[str, Any]] = {}
    for ticker, agg in totals.items():
        if agg["signed_count"] == 0 or agg["abs_count_sum"] == 0:
            continue
        result[ticker] = {
            "count": abs(agg["signed_count"]),
            "entry_price": agg["weighted_entry_sum"] / agg["abs_count_sum"],
            "side": "short" if agg["signed_count"] < 0 else "long",
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
            # ticker via .get(), not position["ticker"] -- this whole body
            # is one try/except below specifically so a single malformed or
            # unexpected-schema position (e.g. adopted from a real account
            # position, or left over from an older deployed version with a
            # different position-dict shape) can never raise out of this
            # loop and block the exit check for every OTHER real, healthy
            # position in the same cycle. Confirmed live: reconciliation
            # correctly ADOPTS untracked real positions regardless of which
            # process/version last touched local state -- this is the
            # matching guarantee on the exit-management side, so a position
            # this code doesn't fully recognize gets logged and retried
            # next cycle instead of taking the whole cycle down.
            ticker = position.get("ticker", "<unknown>")
            try:
                market = get_margin_market(ticker)
                current_price = float((market.get("market") or {}).get("price") or 0.0)
                tick_size = float((market.get("market") or {}).get("tick_size") or 0.0001)

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

                # Volume/momentum/breakout/sentiment/model-confidence only
                # matter to decide_exit's pre-exit study and max_hold
                # "promising position" extension, which only activate once
                # a position is within PRE_EXIT_STUDY_MINUTES of
                # MAX_HOLD_MINUTES -- fetched lazily, only in that window,
                # so this fast loop stays cheap for the common case (see
                # _sample_volatility's own comment on why latest_feature_row()
                # is avoided here otherwise).
                dollar_volume_z = momentum_pct = breakout_pct_b = sentiment_score_value = None
                model_ok_value = False
                probability_up_value = None
                opened_at_check = dt.datetime.fromisoformat(position["opened_at"])
                held_minutes_check = (now - opened_at_check).total_seconds() / 60.0
                if held_minutes_check >= MAX_HOLD_MINUTES - PRE_EXIT_STUDY_MINUTES:
                    try:
                        promising_row = latest_feature_row(ticker)
                    except Exception as exc:
                        promising_row = None
                        logger.debug("[perps_strategy] promising-signal feature fetch failed for %s: %s", ticker, exc)
                    if promising_row:
                        dollar_volume_z = promising_row.get("dollar_volume_z")
                        momentum_pct = promising_row.get("macd_hist_pct")
                        breakout_pct_b = promising_row.get("bb_pct_b")
                        sentiment_score_value = promising_row.get("sentiment_score")
                    try:
                        prediction = predict_direction(ticker)
                    except Exception as exc:
                        prediction = None
                        logger.debug("[perps_strategy] pre-exit model study failed for %s: %s", ticker, exc)
                    if prediction and prediction.get("model_ok"):
                        model_ok_value = True
                        probability_up_value = prediction.get("probability_up")

                should_exit, reason = decide_exit(
                    position, current_price, velocity_pct_per_min=velocity, external_velocity_pct_per_min=external_velocity,
                    current_volatility=current_volatility, now=now,
                    dollar_volume_z=dollar_volume_z, momentum_pct=momentum_pct,
                    breakout_pct_b=breakout_pct_b, sentiment_score=sentiment_score_value,
                    model_ok=model_ok_value, probability_up=probability_up_value,
                )
                checks.append({
                    "ticker": ticker, "ok": True, "exit_check": reason, "velocity_pct_per_min": velocity,
                    "external_velocity_pct_per_min": external_velocity, "current_volatility": current_volatility,
                    "external_source": (external_quote or {}).get("source"),
                })
            except Exception as exc:
                ok = False
                logger.warning("[perps_strategy] could not process position for %s -- leaving untouched this cycle: %s", ticker, exc)
                checks.append({"ticker": ticker, "ok": False, "error": str(exc)})
                remaining.append(position)  # leave it untouched, retry next cycle
                continue

            if not should_exit:
                remaining.append(position)
                continue

            try:
                count = float(position["count"])
                side = position.get("side", "long")
                exit_price = _round_price(current_price, tick_size)
                entry_price = float(position["entry_price"])
            except Exception as exc:
                ok = False
                logger.warning("[perps_strategy] could not read count/entry_price for %s -- leaving untouched this cycle: %s", ticker, exc)
                checks[-1]["error"] = str(exc)
                remaining.append(position)
                continue
            try:
                order_result = None
                exit_fill_type = "taker_fallback"
                closed_count = count
                if not effective_dry_run:
                    # Closing a long means selling (ask); closing a short means
                    # buying back (bid) -- both reduce_only so it can only ever
                    # shrink/close this exact position, never open a new one.
                    exit_order_side = "ask" if side == "long" else "bid"
                    # stop_loss/max_hold_time exits need a GUARANTEED close --
                    # a resting maker order that never fills while the price
                    # keeps moving against the position defeats the whole
                    # point of a stop-loss. take_profit/quick_profit exits are
                    # discretionary (missing one just means re-evaluating next
                    # 20s cycle), so they're worth a real shot at the much
                    # cheaper maker fee first.
                    urgent = reason.startswith("stop_loss") or reason.startswith("max_hold_time")
                    maker_px = _maker_price(exit_order_side, market.get("market") or {}, tick_size) if ENABLE_MAKER_ORDERS else None
                    if maker_px is not None:
                        order_result, exit_fill_type = _place_order_maker_then_fallback(
                            ticker=ticker, order_side=exit_order_side, count=count,
                            maker_price=maker_px, fallback_price=exit_price,
                            reduce_only=True, urgent=urgent,
                        )
                    else:
                        order_result = create_margin_order(
                            ticker=ticker, side=exit_order_side, count=count, price=exit_price,
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
                    gross_pnl = round((entry_price - exit_price) * closed_count, 6)
                else:
                    gross_pnl = round((exit_price - entry_price) * closed_count, 6)
                # realized_pnl_usd is the NET figure (what Kalshi's own account
                # balance actually reflects) -- confirmed live that the gross
                # price-delta alone systematically overstates real performance
                # by the taker round-trip cost (see round_trip_fee_usd above).
                fee_usd = round_trip_fee_usd(
                    ticker, entry_price, exit_price, closed_count,
                    entry_is_maker=(position.get("entry_fill_type") == "maker"),
                    exit_is_maker=(exit_fill_type == "maker"),
                ) if not effective_dry_run else 0.0
                realized_pnl = round(gross_pnl - fee_usd, 6)
                by_date = state.setdefault("realized_pnl_by_date", {})
                by_date[_today_str()] = round(float(by_date.get(_today_str(), 0.0)) + realized_pnl, 6)
                opened_at = position.get("opened_at")
                hold_minutes = None
                if opened_at:
                    try:
                        opened_dt = dt.datetime.fromisoformat(opened_at)
                        hold_minutes = round((dt.datetime.now(dt.timezone.utc) - opened_dt).total_seconds() / 60.0, 2)
                    except Exception:
                        pass
                trade = {
                    "closed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "ticker": ticker, "side": side, "entry_price": entry_price, "exit_price": exit_price,
                    "count": closed_count, "gross_pnl_usd": gross_pnl, "fee_usd": fee_usd,
                    "realized_pnl_usd": realized_pnl, "reason": reason, "dry_run": effective_dry_run,
                    "entry_fill_type": position.get("entry_fill_type", "taker_fallback"), "exit_fill_type": exit_fill_type,
                    "opened_at": opened_at, "hold_minutes": hold_minutes,
                    # Entry-time model/technical context (see scan_and_enter) --
                    # what the model/filters actually saw at decision time, so a
                    # post-trade analysis can ask "what led to this win/loss"
                    # instead of only ever knowing how it ended.
                    "entry_probability_up": position.get("entry_probability_up"),
                    "entry_model_direction": position.get("entry_model_direction"),
                    "entry_score": position.get("entry_score"),
                    "entry_trend_pct": position.get("entry_trend_pct"),
                    "entry_volatility_30": position.get("entry_volatility_30"),
                    "entry_reason": position.get("entry_reason"),
                    "entry_dollar_volume_z": position.get("entry_dollar_volume_z"),
                    "entry_macd_hist_pct": position.get("entry_macd_hist_pct"),
                    "entry_bb_pct_b": position.get("entry_bb_pct_b"),
                    "entry_rsi_14": position.get("entry_rsi_14"),
                    "entry_sentiment_score": position.get("entry_sentiment_score"),
                }
                trade_log = state.setdefault("trade_log", [])
                trade_log.append(trade)
                # Real, confirmed production incident found while building this
                # feature: trade_log had no size cap at all -- an already-flagged
                # candidate contributor to this service's own recurring OOM
                # pattern (unbounded growth over weeks of live trading). Keeps
                # the most recent MAX_TRADE_LOG_ENTRIES, oldest-first trimmed --
                # analysis/retraining both already recency-weight anyway, so
                # very old trades contribute the least regardless.
                if len(trade_log) > MAX_TRADE_LOG_ENTRIES:
                    del trade_log[: len(trade_log) - MAX_TRADE_LOG_ENTRIES]
                closed.append(trade)
                # Real, confirmed production incident found while building this
                # feature: this whole loop used to save state ONCE at the very
                # end, after every position had been processed. If the process
                # got OOM-killed after a real exit fill here but before that
                # final save, the position itself was still safe (next boot's
                # reconciliation correctly drops the now-stale local record to
                # match the exchange), but this trade's OWN record -- its
                # win/loss reason, P&L, and now its entry-time context -- was
                # silently lost forever, since the drop path only logs a
                # warning, it doesn't reconstruct a missing trade. Saving right
                # here, the instant the record exists, closes that gap. Cheap:
                # push_durable's own throttle (_DURABLE_PUSH_MIN_INTERVAL_SEC)
                # already caps how often this can actually hit HF even if
                # several positions close in the same cycle.
                _save_state(state, push_durable=True)

                if closed_count < count:
                    # Partial fill -- the remainder is still genuinely open on
                    # the exchange, keep monitoring it rather than dropping it.
                    remainder = dict(position)
                    remainder["count"] = round(count - closed_count, 6)
                    remaining.append(remainder)
            except Exception as exc:
                # Placing the real exit order (or booking its result) failed
                # unexpectedly -- must not abort monitoring for every OTHER
                # open position in this same cycle. Nothing in this block
                # commits local state until the very end, so the position is
                # safely un-mutated either way; keep it as-is and retry.
                ok = False
                logger.exception("[perps_strategy] exit order/booking failed for %s -- leaving untouched this cycle", ticker)
                checks[-1]["error"] = str(exc)
                remaining.append(position)

        state["positions"] = remaining
        # push_durable only when a trade actually closed this cycle (real
        # money moved, realized_pnl_by_date changed) -- not on every 20s
        # tick just because positions/velocity samples were touched.
        _save_state(state, push_durable=bool(closed))
        result = {
            "ok": ok, "dry_run": effective_dry_run,
            "action": "closed" if closed else ("none" if remaining else "no_position"),
            "closed": closed, "checks": checks, "open_position_count": len(remaining),
        }

    # Posted AFTER the lock releases -- same reasoning as scan_and_enter's
    # own entry post, which is likewise outside its (tightly-scoped) lock:
    # a Threads network call must never hold up the fast 15-30s exit-check
    # loop for every OTHER position.
    for trade in closed:
        try:
            threads_post.post_trade_exit(
                ticker=trade["ticker"], side=trade["side"], entry_price=trade["entry_price"],
                exit_price=trade["exit_price"], pnl_usd=trade["realized_pnl_usd"], reason=trade["reason"],
                dry_run=trade["dry_run"], market="perps",
            )
        except Exception:
            logger.warning("[perps_strategy] Threads post for %s exit failed", trade["ticker"], exc_info=True)
        try:
            from data import chart_snapshot
            one_min_df, _ = fetch_candle_frames(trade["ticker"])
            # entry_* fields are what the bot actually saw at ENTRY time
            # (see scan_and_enter's own entry_context) -- remapped to the
            # plain feature names format_technical_indicators expects.
            indicators = chart_snapshot.format_technical_indicators({
                "rsi_14": trade.get("entry_rsi_14"), "macd_hist_pct": trade.get("entry_macd_hist_pct"),
                "bb_pct_b": trade.get("entry_bb_pct_b"), "dollar_volume_z": trade.get("entry_dollar_volume_z"),
                "sentiment_score": trade.get("entry_sentiment_score"), "volatility_30": trade.get("entry_volatility_30"),
            })
            if trade.get("entry_probability_up") is not None:
                indicators["Model prob up"] = f"{trade['entry_probability_up']:.1%}"
            if trade.get("hold_minutes") is not None:
                indicators["Held"] = f"{trade['hold_minutes']:.0f}min"
            if trade.get("fee_usd") is not None:
                indicators["Fees"] = f"${trade['fee_usd']:.3f}"
            indicators["Exit reason"] = str(trade.get("reason", ""))
            threads_post.post_trade_exit_chart(
                ticker=trade["ticker"], market="perps", candles=_candles_as_dicts(one_min_df),
                side=trade["side"], entry_price=trade["entry_price"], exit_price=trade["exit_price"],
                entry_index=_index_for_ts(one_min_df, trade.get("opened_at")),
                exit_index=_index_for_ts(one_min_df, trade.get("closed_at")),
                pnl_usd=trade["realized_pnl_usd"], dry_run=trade["dry_run"], indicators=indicators,
            )
        except Exception:
            logger.warning("[perps_strategy] Threads exit chart post for %s failed", trade["ticker"], exc_info=True)

    if closed:
        _maybe_run_batch_trade_analysis()

    return result


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
        # A confidence floor genuinely learned from this account's own real
        # trade history (see perps_trade_analysis.recommend_confidence_threshold
        # + apply_confidence_threshold_override below) -- falls back to the
        # module-level MODEL_CONFIDENCE_MIN default until enough real trades
        # exist to justify moving it.
        confidence_min_override = (state.get("tuning") or {}).get("model_confidence_min")
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
    qualifying, candidates = scan_for_entries(exclude=held_tickers, confidence_min=confidence_min_override)
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
        try:
            order_result = None
            entry_fill_type = "taker_fallback"
            actual_count: float = float(count)
            actual_entry_price = entry_price
            if not effective_dry_run:
                # Opening a long means buying (bid); opening a short means
                # selling (ask) with reduce_only NOT set, since this is a new
                # position, not closing an existing long.
                entry_order_side = "bid" if side == "long" else "ask"
                # A new entry is always discretionary -- never "urgent" (there
                # is no requirement to open a position this exact cycle), so
                # an unfilled maker attempt just means skipping this ticker
                # and letting the next scan re-evaluate, rather than forcing
                # a worse (taker) fill just to guarantee an entry happens.
                maker_px = _maker_price(entry_order_side, market, tick_size) if ENABLE_MAKER_ORDERS else None
                if maker_px is not None:
                    order_result, entry_fill_type = _place_order_maker_then_fallback(
                        ticker=ticker, order_side=entry_order_side, count=float(count),
                        maker_price=maker_px, fallback_price=entry_price,
                        reduce_only=False, urgent=False,
                    )
                else:
                    order_result = create_margin_order(
                        ticker=ticker, side=entry_order_side, count=float(count), price=entry_price,
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
                # Entry-time model/technical context, carried through to the
                # closed trade_log record when this position eventually exits
                # (see manage_open_positions) -- previously this snapshot
                # existed only transiently in evaluate_candidate()'s return
                # dict and was never persisted anywhere, so a post-trade
                # analysis asking "what did the model actually see when it
                # decided to enter" had nothing to look at. entry_score is
                # candidate["score"] (confidence for a long, 1-probability_up
                # for a short -- see evaluate_candidate), the one number
                # actually compared against MODEL_CONFIDENCE_MIN at entry.
                entry_context = {
                    "entry_probability_up": candidate.get("probability_up"),
                    "entry_model_direction": candidate.get("model_direction"),
                    "entry_score": candidate.get("score"),
                    "entry_trend_pct": candidate.get("trend_pct"),
                    "entry_reason": candidate.get("reason"),
                    # Raw indicator values (see evaluate_candidate's own
                    # comment) -- carried through the same way so the exit
                    # chart's indicator panel can show what the bot saw at
                    # ENTRY, not just at exit.
                    "entry_dollar_volume_z": candidate.get("dollar_volume_z"),
                    "entry_macd_hist_pct": candidate.get("macd_hist_pct"),
                    "entry_bb_pct_b": candidate.get("bb_pct_b"),
                    "entry_rsi_14": candidate.get("rsi_14"),
                    "entry_sentiment_score": candidate.get("sentiment_score"),
                }
                if existing_idx is not None:
                    merged = dict(positions[existing_idx])
                    merged["count"] = float(actual_count)
                    merged["entry_price"] = actual_entry_price
                    merged["side"] = side
                    merged["entry_fill_type"] = entry_fill_type
                    merged["entry_volatility_30"] = candidate.get("volatility_30")
                    merged.update(entry_context)
                    positions[existing_idx] = merged
                else:
                    positions.append({
                        "ticker": ticker, "entry_price": actual_entry_price, "count": float(actual_count),
                        "side": side, "entry_fill_type": entry_fill_type,
                        "entry_volatility_30": candidate.get("volatility_30"),
                        "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "dry_run": effective_dry_run,
                        "sizing": sizing_detail,
                        **entry_context,
                    })
                state["positions"] = positions
                _save_state(state)
            opened.append({
                "ticker": ticker, "ok": True, "action": "opened", "side": side, "entry_price": actual_entry_price,
                "count": actual_count, "reason": candidate["reason"], "sizing": sizing_detail, "order_result": order_result,
                "entry_fill_type": entry_fill_type,
            })
            # Best-effort only -- see threads_post.py's own docstring. Never
            # allow a Threads failure (or it simply not being configured
            # yet) to affect the real entry that already happened above.
            try:
                levels = position_exit_levels(positions[existing_idx] if existing_idx is not None else positions[-1])
                threads_post.post_trade_entry(
                    ticker=ticker, side=side, entry_price=actual_entry_price,
                    take_profit_price=levels["take_profit_price"], stop_loss_price=levels["stop_loss_price"],
                    reason=candidate["reason"], dry_run=effective_dry_run, market="perps",
                )
            except Exception:
                logger.warning("[perps_strategy] Threads post for %s entry failed", ticker, exc_info=True)
            try:
                from data import chart_snapshot
                one_min_df, _ = fetch_candle_frames(ticker)
                indicators = chart_snapshot.format_technical_indicators(candidate)
                if candidate.get("probability_up") is not None:
                    indicators["Model prob up"] = f"{candidate['probability_up']:.1%}"
                if candidate.get("score") is not None:
                    indicators["Entry score"] = f"{candidate['score']:.3f}"
                threads_post.post_trade_entry_chart(
                    ticker=ticker, market="perps", candles=_candles_as_dicts(one_min_df),
                    entry_price=actual_entry_price, take_profit_price=levels["take_profit_price"],
                    stop_loss_price=levels["stop_loss_price"],
                    entry_index=(len(one_min_df) - 1) if not one_min_df.empty else None,
                    side=side, dry_run=effective_dry_run, indicators=indicators,
                )
            except Exception:
                logger.warning("[perps_strategy] Threads chart post for %s entry failed", ticker, exc_info=True)
        except Exception as exc:
            # Placing the real entry order (or booking its result) failed
            # unexpectedly -- must not abort scanning/entering for every
            # OTHER qualifying candidate in this same cycle.
            logger.exception("[perps_strategy] entry order/booking failed for %s", ticker)
            opened.append({"ticker": ticker, "ok": False, "action": "entry_failed", "error": str(exc)})

    result["opened"] = opened
    result["action"] = "opened" if any(o.get("action") == "opened" for o in opened) else "none"
    return result


def run_cycle(*, dry_run: bool | None = None) -> dict[str, Any]:
    """Convenience for manual triggers/tests: run the fast position check,
    then the entry scan (which cheaply no-ops itself if every slot is
    already full, with no network calls). Production scheduling calls
    `manage_open_positions` and `scan_and_enter` on their own separate
    cadences instead -- see app_kalshi.py."""
    position_result = manage_open_positions(dry_run=dry_run)
    entry_result = scan_and_enter(dry_run=dry_run)
    return {
        "ok": bool(position_result.get("ok", True)) and bool(entry_result.get("ok", True)),
        "dry_run": entry_result.get("dry_run", position_result.get("dry_run")),
        "position_management": position_result,
        "entry_scan": entry_result,
    }
