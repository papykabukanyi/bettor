"""Alpaca crypto trading strategy -- a new, separate strategy from
alpaca_strategy.py (equities), built "the same recipe as perps": real
technical indicators + real crypto news sentiment feeding a trained
direction classifier, same chronological-holdout discipline. Long-only to
start, same "start conservative" posture every strategy in this codebase
began with before any bidirectional extension was considered.

Genuinely different from alpaca_strategy.py in its order mechanics, not
just a renamed copy: Alpaca crypto orders do NOT support order_class=
"bracket" or stop orders at all (confirmed via Alpaca's own docs -- crypto
is restricted to market/limit/stop-limit, time_in_force gtc/ioc only). So
unlike the equities strategy (which lets Alpaca's own bracket order manage
the take-profit/stop-loss live on the exchange), this strategy manages
exits itself via its own poll loop -- decide_exit checks the current price
against the position's stored TP/SL/max-hold levels, and a plain market
sell order is placed when triggered. This is the SAME shape Kalshi perps
already uses (perps_strategy.py's own manage_open_positions), since Kalshi
has no broker-native bracket order either -- a genuine convergence, not
extra complexity introduced for its own sake.

Position sizing uses NOTIONAL (dollar amount) rather than a share count --
crypto supports fractional sizing down to 1e-9, so unlike equities (which
can leave a small account unable to afford even one whole share of some
names), a crypto position is never blocked by price alone.

Brought up to parity with perps_strategy.py's own "brain" (the user's own
framing -- "alpaca crypto is basically perp server configure for alpaca
tickers and service same brain... make sure alpaca crypto is the wider
and better version of perps"), adapted for Alpaca's genuinely different
platform mechanics rather than copied verbatim:
  - real round-trip fee accounting (round_trip_fee_usd) -- a correctness
    bug found in review: this strategy was booking GROSS price movement
    as realized P&L with no fee subtracted at all, same class of bug
    perps found live on Kalshi's own (much larger) fee gap
  - candidate ranking by model confidence (scan_and_enter fills its
    limited slots with the BEST-scoring qualifying candidates, not
    whichever happens to sort alphabetically first)
  - per-pair adaptive take-profit/stop-loss (adaptive_exit_pcts) scaled to
    each pair's own volatility_30 at entry, not one flat percentage
    applied identically to BTC and a much choppier small-cap alike
  - velocity-based quick-profit / volatility-quick-profit exits
    (_update_velocity/_sample_volatility)
  - reconciliation against the real Alpaca account before every live
    decision (_reconcile_positions_with_exchange) -- local bookkeeping
    only ever records an order having been PLACED, never confirms it
    actually FILLED at the assumed price/quantity
Deliberately NOT ported: margin/leveraged sizing and short-selling --
confirmed via Alpaca's own docs, crypto here is real spot-only, cash-
settled, no margin and no short selling at all, so perps' leveraged/
bidirectional machinery has no equivalent to port; this strategy's
existing long-only, full-notional-cash sizing is already the correct
shape for this platform, not a gap.
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


# Real gap found in review (same fix as alpaca_strategy.py's own copy of
# this): requiring a volume spike AND a volatility-ratio jump AND a dip,
# all three simultaneously, made real entries rare -- confirmed live,
# almost every cycle across the whole 36-pair universe skipped on "volume
# not unusual enough" before the dip signal was even checked.
# perps_strategy.py's own decide_entry_technical has NO volume/volatility
# gate at all -- just the dip/rally read plus a soft trend filter. Off by
# default now (env-overridable back on): MIN_VOLUME_Z=-1e9 makes
# "z < MIN_VOLUME_Z" unsatisfiable (a z-score is never remotely this large)
# -- NOT float("-inf"): a real, confirmed production bug found live --
# Python's json module happily serializes float("-inf") as the bare token
# `-Infinity`, invalid JSON grammar, so every browser's JSON.parse() threw
# a SyntaxError on every /api/alpaca/crypto/status poll, silently breaking
# this dashboard's entire auto-refresh loop (caught by its own try/catch,
# so the page just sat frozen on its initial "--" placeholders forever) --
# and MIN_VOLATILITY_RATIO=0.0 makes
# "ratio < MIN_VOLATILITY_RATIO" unsatisfiable (a ratio is always >= 0) --
# both checks below become genuine no-ops at these defaults.
MIN_VOLUME_Z = _env_float("ALPACA_CRYPTO_MIN_VOLUME_Z", -1e9)
MIN_VOLATILITY_RATIO = _env_float("ALPACA_CRYPTO_MIN_VOLATILITY_RATIO", 0.0)
ENTRY_DIP_PCT = _env_float("ALPACA_CRYPTO_ENTRY_DIP_PCT", 0.0015)
SHORT_MA_MINUTES = _env_int("ALPACA_CRYPTO_SHORT_MA_MINUTES", 15)

TAKE_PROFIT_PCT = _env_float("ALPACA_CRYPTO_TAKE_PROFIT_PCT", 0.01)
STOP_LOSS_PCT = _env_float("ALPACA_CRYPTO_STOP_LOSS_PCT", 0.008)
MAX_HOLD_MINUTES = _env_int("ALPACA_CRYPTO_MAX_HOLD_MINUTES", 120)
# Real, confirmed finding from reviewing actual trade history: a position
# that moved a literal +0.001% over its full 120-minute hold still netted a
# real -$59 to fees alone -- the round-trip fee is paid whether or not the
# position ever developed. Same underlying pattern perps_strategy.py found
# and fixed with its own identical STALE_POSITION_CHECK_FRACTION (see that
# module's comment for the full rationale) -- a position that hasn't
# captured a meaningful fraction of its own take-profit distance by the
# halfway point of max_hold_time is unlikely to still develop into one by
# the full timeout, so this frees the slot early instead of paying the same
# fee for zero informational value. Deliberately a DIFFERENT lever than
# TAKE_PROFIT_VOL_MULTIPLE/STOP_LOSS_VOL_MULTIPLE above: this doesn't change
# how far a position is allowed to move, only how long the bot waits for one
# that isn't developing at all.
STALE_POSITION_CHECK_FRACTION = _env_float("ALPACA_CRYPTO_STALE_POSITION_CHECK_FRACTION", 0.5)
STALE_POSITION_MAX_PROGRESS_FRACTION = _env_float("ALPACA_CRYPTO_STALE_POSITION_MAX_PROGRESS_FRACTION", 0.25)

MODEL_CONFIDENCE_MIN = _env_float("ALPACA_CRYPTO_MODEL_CONFIDENCE_MIN", 0.55)

POSITION_SIZE_PCT = _env_float("ALPACA_CRYPTO_POSITION_SIZE_PCT", 0.18)
# Raised from 2: perps deploys 5 slots against its own (much smaller,
# narrowed-to-top-8) watchlist -- this strategy was scanning a 36+-pair
# universe (wider still once get_crypto_universe() below adds USDT/USDC-
# quoted pairs) but only ever DEPLOYING capital into 2 of them, the
# opposite of "wider." 5 slots x 18% keeps total deployment (90% of
# balance) below perps' own 5x20%=100%, leaving a little more buffer given
# crypto's real fee drag on every round trip (see TAKER_FEE_RATE) that
# perps doesn't carry at anywhere near this magnitude.
MAX_CONCURRENT_POSITIONS = max(1, _env_int("ALPACA_CRYPTO_MAX_CONCURRENT_POSITIONS", 5))
DAILY_LOSS_CAP_PCT = _env_float("ALPACA_CRYPTO_DAILY_LOSS_CAP_PCT", 0.10)
# Same unbounded-growth guard perps_strategy.py already needed (a real,
# confirmed OOM contributor there over weeks of live trading) -- keeps the
# most recent entries, oldest-first trimmed.
MAX_TRADE_LOG_ENTRIES = _env_int("ALPACA_CRYPTO_MAX_TRADE_LOG_ENTRIES", 2000)

LIVE_TRADING_ENABLED = str(os.getenv("ALPACA_CRYPTO_LIVE_TRADING_ENABLED", "")).strip().lower() in {"1", "true", "yes"}

# Real transaction cost, missing entirely before this change -- confirmed
# via Alpaca's own published crypto fee schedule: Tier 1 (this account's
# presumed tier, absent a live per-account fee-tier endpoint the way
# Kalshi exposes one) charges 0.25% taker / 0.15% maker per leg, volume-
# tiered down for higher-volume accounts. Every order this strategy places
# today is a plain market order (a taker fill), so both legs of a round
# trip use this same rate -- a real ~0.5% round-trip cost that was
# previously being ignored entirely: at the old flat TAKE_PROFIT_PCT=1%,
# that's HALF of every winning trade's target being silently eaten before
# it's even booked, the same "gross profit is actually a net loss" risk
# perps found live on Kalshi's own (much larger, 1.6% round-trip) fee gap.
TAKER_FEE_RATE = _env_float("ALPACA_CRYPTO_TAKER_FEE_RATE", 0.0025)

# Per-pair adaptive take-profit/stop-loss -- same methodology perps_strategy.py
# already proved out via a real pooled backtest sweep across 8 Kalshi
# instruments (see perps_strategy.py's own adaptive_exit_pcts docstring),
# applied here to THIS strategy's own volatility_30 feature (already
# computed by alpaca_crypto_data.engineer_features on every row -- it was
# just never captured onto the position or used at exit time before this).
# The vol multiples (1.5/1.0) are carried over from perps' own tuned
# values as a reasoned starting point, NOT a claim of having been
# independently re-swept against crypto's own volatility distribution yet
# (that would need its own real backtest history, which doesn't exist for
# this strategy today) -- fully env-overridable once live data justifies
# retuning. Floors/ceilings are anchored to THIS strategy's own existing
# flat TAKE_PROFIT_PCT/STOP_LOSS_PCT defaults (not perps') since crypto's
# fee drag and typical hold window genuinely differ from Kalshi's.
TAKE_PROFIT_VOL_MULTIPLE = _env_float("ALPACA_CRYPTO_TAKE_PROFIT_VOL_MULTIPLE", 1.5)
STOP_LOSS_VOL_MULTIPLE = _env_float("ALPACA_CRYPTO_STOP_LOSS_VOL_MULTIPLE", 1.0)
# Real finding from reviewing actual trade history: the old 0.006 (0.6%)
# floor left only a razor-thin ~0.1% margin over the real ~0.5% round-trip
# taker fee (TAKER_FEE_RATE=0.0025, both legs) -- comfortably positive on
# paper, but thin enough that ordinary slippage on a real fill could still
# turn a "successful" minimum-target take-profit into a net loss (the
# structural version of this bug already confirmed live on perps, see its
# own identical comment). Widened to a healthier margin.
MIN_TAKE_PROFIT_PCT = _env_float("ALPACA_CRYPTO_MIN_TAKE_PROFIT_PCT", 0.01)
MAX_TAKE_PROFIT_PCT = _env_float("ALPACA_CRYPTO_MAX_TAKE_PROFIT_PCT", 0.04)
MIN_STOP_LOSS_PCT = _env_float("ALPACA_CRYPTO_MIN_STOP_LOSS_PCT", 0.005)
MAX_STOP_LOSS_PCT = _env_float("ALPACA_CRYPTO_MAX_STOP_LOSS_PCT", 0.03)

# Velocity-based quick-profit exits -- a fast-arriving gain (this pair's
# own price moving quickly in the favorable direction, not just "moved
# enough") gets taken early rather than waiting for the full take-profit
# target, and a choppy/high-volatility stretch takes a smaller sure thing
# instead of risking a round trip back through breakeven. Same mechanism
# as perps_strategy.py's own _update_velocity/_sample_volatility/decide_exit,
# with no Kalshi-specific dependency to adapt -- these operate purely on a
# position's own rolling price-sample history.
QUICK_PROFIT_VELOCITY_PCT_PER_MIN = _env_float("ALPACA_CRYPTO_QUICK_PROFIT_VELOCITY_PCT_PER_MIN", 0.006)
QUICK_PROFIT_WINDOW_SECONDS = _env_int("ALPACA_CRYPTO_QUICK_PROFIT_WINDOW_SECONDS", 90)
HIGH_VOLATILITY_THRESHOLD = _env_float("ALPACA_CRYPTO_HIGH_VOLATILITY_THRESHOLD", 0.002)


def decide_entry_technical(row: dict[str, Any]) -> tuple[bool, str]:
    """row needs: current_price, short_ma, dollar_volume_z, volatility_5,
    volatility_30 (all already computed by engineer_features)."""
    dollar_volume_z = row.get("dollar_volume_z")
    volatility_5 = row.get("volatility_5") or 0.0
    volatility_30 = row.get("volatility_30") or 0.0

    if dollar_volume_z is None or dollar_volume_z < MIN_VOLUME_Z:
        return False, f"volume not unusual enough (z={dollar_volume_z})"
    if volatility_30 > 0 and (volatility_5 / volatility_30) < MIN_VOLATILITY_RATIO:
        return False, "not more volatile than its own recent baseline"

    current_price = row["current_price"]
    short_ma = row["short_ma"]
    if short_ma <= 0:
        return False, "no short MA yet"
    dip_pct = (short_ma - current_price) / short_ma
    if dip_pct < ENTRY_DIP_PCT:
        return False, f"no real dip ({dip_pct:+.3%})"
    return True, f"dip ({dip_pct:+.3%}, z={dollar_volume_z:.2f})"


def evaluate_candidate(row: dict[str, Any], model_prediction: dict[str, Any] | None) -> dict[str, Any]:
    """`score` (added alongside `should_enter`) is what lets scan_and_enter
    fill its limited slots with the BEST qualifying candidates instead of
    whichever sorts first alphabetically -- same shape as perps_strategy.py's
    own scan_for_entries(), which ranks qualifying candidates by score
    before slicing to open_slots."""
    technical_ok, technical_reason = decide_entry_technical(row)
    result: dict[str, Any] = {
        "symbol": row.get("symbol"), "technical_ok": technical_ok, "reason": technical_reason,
        "model_ok": False, "should_enter": False, "score": 0.0,
    }
    if not technical_ok:
        return result

    if model_prediction and model_prediction.get("model_ok"):
        proba_up = model_prediction["probability_up"]
        result["model_ok"] = True
        result["probability_up"] = proba_up
        result["model_direction"] = "up" if proba_up >= 0.5 else "down"
        if proba_up >= MODEL_CONFIDENCE_MIN:
            result["should_enter"] = True
            result["reason"] = f"{technical_reason} + model confident up ({proba_up:.2%})"
            result["score"] = proba_up
    else:
        # No trained model yet -- technical-only fallback (same posture as
        # every other strategy here during the first days of data
        # collection). Scored by dip depth (deeper dip = higher score),
        # mirroring perps_strategy.py's own technical-only fallback score.
        result["should_enter"] = True
        short_ma = row.get("short_ma") or 0.0
        current_price = row.get("current_price") or 0.0
        result["score"] = ENTRY_DIP_PCT + ((short_ma - current_price) / short_ma if short_ma > 0 else 0.0)

    return result


def round_trip_fee_usd(entry_price: float, exit_price: float, count: float) -> float:
    """Real dollar cost of a round trip -- see TAKER_FEE_RATE's own
    docstring for why this was missing entirely before and why it matters
    at this strategy's take-profit scale. Every order this strategy places
    is a plain market order (a taker fill on Alpaca), so both legs use the
    same TAKER_FEE_RATE -- unlike perps, there's no maker-order placement
    here yet to make the entry/exit rates potentially differ."""
    return round(entry_price * count * TAKER_FEE_RATE + exit_price * count * TAKER_FEE_RATE, 6)


def adaptive_exit_pcts(entry_volatility_30: float | None) -> dict[str, float]:
    """Take-profit/stop-loss/quick-profit percentages customized to ONE
    specific pair's own volatility at entry time -- see this module's own
    TAKE_PROFIT_VOL_MULTIPLE comment for the full rationale. Falls back to
    the flat global TAKE_PROFIT_PCT/STOP_LOSS_PCT if no volatility was
    captured (e.g. a position opened before this field existed) -- same
    value every position used before this change, so nothing regresses for
    positions that predate it. Also falls back on NaN specifically (not
    just falsy/<=0): a real edge case caught in review -- Python's own
    NaN comparisons are always False (`nan <= 0` is False, `not nan` is
    also False since NaN is truthy), so a rolling-window feature that's
    still NaN this early (e.g. right after this pair's data collection
    started) would otherwise silently slip past this guard and propagate
    into a nonsensical clamped result instead of the intended fallback."""
    if not entry_volatility_30 or entry_volatility_30 <= 0 or math.isnan(entry_volatility_30):
        return {
            "take_profit_pct": TAKE_PROFIT_PCT, "stop_loss_pct": STOP_LOSS_PCT,
            "quick_profit_pct": TAKE_PROFIT_PCT * 0.9, "volatility_quick_profit_pct": TAKE_PROFIT_PCT * 0.8,
        }
    horizon_scale = math.sqrt(max(1, MAX_HOLD_MINUTES))
    take_profit = min(MAX_TAKE_PROFIT_PCT, max(MIN_TAKE_PROFIT_PCT, TAKE_PROFIT_VOL_MULTIPLE * entry_volatility_30 * horizon_scale))
    stop_loss = min(MAX_STOP_LOSS_PCT, max(MIN_STOP_LOSS_PCT, STOP_LOSS_VOL_MULTIPLE * entry_volatility_30 * horizon_scale))
    return {
        "take_profit_pct": take_profit, "stop_loss_pct": stop_loss,
        "quick_profit_pct": take_profit * 0.9, "volatility_quick_profit_pct": take_profit * 0.8,
    }


def decide_exit(
    position: dict[str, Any], current_price: float, *,
    velocity_pct_per_min: float | None = None, current_volatility: float | None = None,
    now: dt.datetime | None = None,
) -> tuple[bool, str]:
    """Long-only: a RISING price is favorable. Exit levels are per-position
    ADAPTIVE (see adaptive_exit_pcts) -- scaled to this specific pair's own
    volatility_30 at entry, not one flat percentage applied identically to
    BTC and a much choppier small-cap alike. `velocity_pct_per_min`/
    `current_volatility` are optional (see _update_velocity/_sample_volatility)
    -- when the caller doesn't track them, this behaves exactly like the
    pre-upgrade version (take-profit/stop-loss/max-hold only)."""
    entry_price = float(position["entry_price"])
    exit_pcts = adaptive_exit_pcts(position.get("entry_volatility_30"))
    take_profit_pct = exit_pcts["take_profit_pct"]
    stop_loss_pct = exit_pcts["stop_loss_pct"]
    quick_profit_pct = exit_pcts["quick_profit_pct"]
    volatility_quick_profit_pct = exit_pcts["volatility_quick_profit_pct"]
    change_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0.0

    if (
        change_pct >= quick_profit_pct and velocity_pct_per_min is not None
        and velocity_pct_per_min >= QUICK_PROFIT_VELOCITY_PCT_PER_MIN
    ):
        return True, f"quick_profit (velocity {velocity_pct_per_min:+.2%}/min, gain {change_pct:+.3%})"
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
    # Stale/flat position early exit -- see STALE_POSITION_CHECK_FRACTION's
    # own comment. Uses abs(change_pct) deliberately: this targets
    # positions that haven't moved meaningfully in EITHER direction, not
    # ones that are simply losing (a real, distinct case already owned by
    # stop_loss above) -- a position sitting at -0.5% with a -0.8% stop is a
    # normal, developing loser that should be left to either recover or
    # hit its real stop, not cut early by a second, competing mechanism.
    if held_minutes >= MAX_HOLD_MINUTES * STALE_POSITION_CHECK_FRACTION and take_profit_pct > 0:
        if abs(change_pct) < take_profit_pct * STALE_POSITION_MAX_PROGRESS_FRACTION:
            return True, f"stale_position ({held_minutes:.0f}min, {change_pct:+.3%}, flat)"
    if held_minutes >= MAX_HOLD_MINUTES:
        return True, f"max_hold_time ({held_minutes:.0f}min, {change_pct:+.3%})"
    return False, f"holding ({change_pct:+.3%}, {held_minutes:.0f}min)"


def position_exit_levels(position: dict[str, Any]) -> dict[str, float]:
    """The actual take-profit/stop-loss/quick-profit PRICE levels for a
    position, derived from the same per-pair-adaptive percentages
    decide_exit() applies (see adaptive_exit_pcts) -- exists so callers
    (the dashboard) can show, per open position, real exit levels rather
    than just trusting the config exists somewhere."""
    entry_price = float(position["entry_price"])
    exit_pcts = adaptive_exit_pcts(position.get("entry_volatility_30"))
    return {
        "take_profit_price": round(entry_price * (1 + exit_pcts["take_profit_pct"]), 6),
        "stop_loss_price": round(entry_price * (1 - exit_pcts["stop_loss_pct"]), 6),
        "quick_profit_price": round(entry_price * (1 + exit_pcts["quick_profit_pct"]), 6),
    }


def _candles_as_dicts(df) -> list[dict[str, Any]]:
    """Converts a fetch_recent_crypto_bars-style DataFrame into the plain
    list[dict] shape both chart_snapshot.generate_candlestick_chart and
    alpaca_crypto_trade_analysis expect -- keeps those two modules
    pandas-free."""
    if df is None or df.empty:
        return []
    cols = [c for c in ("ts", "open", "high", "low", "close") if c in df.columns]
    return df[cols].to_dict("records")


def _index_for_ts(df, iso_ts: str | None) -> int | None:
    """Which row of a fetch_recent_crypto_bars-style DataFrame is closest to
    a given ISO timestamp -- used to mark ENTRY/EXIT on the candlestick
    chart. None if there's no timestamp, no data, or the closest candle is
    more than an hour away (the trade's window genuinely isn't covered by
    this data -- a wildly wrong index would mislabel the chart, so skip the
    marker instead of guessing)."""
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
    """Every alpaca_crypto_trade_analysis.BATCH_SIZE newly-closed REAL
    trades, studies that recent batch -- win/loss patterns, missed-profit/
    premature-stop diagnostics from real OHLC -- and posts a Threads
    snapshot. Called right after manage_open_positions closes trades,
    outside _STATE_LOCK (same reasoning as every other Threads/network call
    in this module). Best-effort: any failure here is logged and
    swallowed, never allowed to affect trading."""
    from data import alpaca_crypto_trade_analysis
    from data import threads_post
    from data.alpaca_crypto_data import fetch_recent_crypto_bars

    try:
        with _STATE_LOCK:
            state = _load_state()
            trade_log = state.get("trade_log") or []
            real_trades = [t for t in trade_log if not t.get("dry_run")]
            last_count = int(state.get("last_batch_analysis_trade_count") or 0)
            if len(real_trades) - last_count < alpaca_crypto_trade_analysis.BATCH_SIZE:
                return
            state["last_batch_analysis_trade_count"] = len(real_trades)
            _save_state(state, push_durable=True)

        recent = real_trades[-alpaca_crypto_trade_analysis.BATCH_SIZE:]
        candles_by_symbol: dict[str, list[dict[str, Any]]] = {}
        for symbol in {t.get("symbol") for t in recent if t.get("symbol")}:
            try:
                candles_by_symbol[symbol] = _candles_as_dicts(fetch_recent_crypto_bars(symbol))
            except Exception:
                logger.debug("[alpaca_crypto_strategy] candle fetch for batch analysis failed for %s", symbol, exc_info=True)

        batch = alpaca_crypto_trade_analysis.analyze_recent_trade_batch(real_trades, candles_by_symbol=candles_by_symbol)
        text = alpaca_crypto_trade_analysis.format_batch_snapshot_text(batch, market="crypto")
        threads_post.post_trade_analysis_summary(text, market="crypto")
    except Exception:
        logger.warning("[alpaca_crypto_strategy] batch trade analysis failed", exc_info=True)


def _update_velocity(position: dict[str, Any], current_price: float, now: dt.datetime) -> float | None:
    """Tracks recent (timestamp, price) samples on the position itself and
    returns the trailing %/minute velocity over QUICK_PROFIT_WINDOW_SECONDS,
    or None until there's at least two samples spanning some real elapsed
    time. Mutates `position["price_samples"]` in place -- caller is
    responsible for persisting it. Identical mechanism to perps_strategy.py's
    own _update_velocity (no Kalshi-specific dependency to adapt -- this
    operates purely on a position's own rolling price-sample history), minus
    the external-cross-check second sample series (Kalshi's own perp quote
    can lag a deep spot venue by a tick or two; Alpaca's crypto quote IS the
    tradable price directly, so there's no equivalent lag to cross-check)."""
    now_ts = now.timestamp()
    samples: list[list[float]] = position.setdefault("price_samples", [])
    samples.append([now_ts, current_price])
    cutoff = now_ts - QUICK_PROFIT_WINDOW_SECONDS
    trimmed = [s for s in samples if s[0] >= cutoff] or samples[-1:]
    position["price_samples"] = trimmed[-30:]  # defensive cap regardless of timing
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
    data already being collected rather than an extra API call. None until
    there are at least 3 samples (2 changes). Identical to perps_strategy.py's
    own _sample_volatility."""
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


def compute_position_notional(available_balance_usd: float) -> float:
    """A dollar amount, not a share/coin count -- crypto orders accept
    `notional` directly, so unlike equities there's no whole-share
    affordability problem to work around."""
    return round(max(0.0, available_balance_usd) * POSITION_SIZE_PCT, 2)


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
STATE_FILE = Path(os.getenv("ALPACA_CRYPTO_STATE_FILE", str(DATA_DIR / "alpaca_crypto_state.json")))
_STATE_LOCK = threading.RLock()

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_ALPACA_CRYPTO_MODEL_REPO = os.getenv("HF_ALPACA_CRYPTO_MODEL_REPO", "papylove/alpaca-crypto-model")
_DURABLE_STATE_HF_FILENAME = "alpaca_crypto_durable_state.json"
_DURABLE_PUSH_MIN_INTERVAL_SEC = 30
_last_durable_push_ts = 0.0


def _today_str() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def _durable_state_slice(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "positions": state.get("positions") or [],
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
                repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, repo_type="model", commit_message="update alpaca crypto durable state",
            )
        finally:
            os.unlink(tmp_path)
    except Exception as exc:
        logger.warning("[alpaca_crypto_strategy] durable state push to HF failed: %s", exc)


_DURABLE_STATE_HF_TIMEOUT_SEC = int(os.getenv("ALPACA_CRYPTO_DURABLE_STATE_HF_TIMEOUT_SEC", "10") or "10")


def _pull_durable_state_from_hf() -> dict[str, Any] | None:
    if not HF_API_KEY:
        return None

    def _download() -> dict[str, Any]:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(
            repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, filename=_DURABLE_STATE_HF_FILENAME, repo_type="model", token=HF_API_KEY,
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
        logger.info("[alpaca_crypto_strategy] no durable state on HF yet (or fetch failed): %s", exc)
        return None


def _load_state() -> dict[str, Any]:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        base = {
            "positions": [], "trade_log": [], "realized_pnl_by_date": {},
            "daily_reference_balance": {},
        }
        durable = _pull_durable_state_from_hf()
        if durable:
            base.update(durable)
            logger.info("[alpaca_crypto_strategy] recovered durable state from HF after local state was missing")
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


def get_available_balance() -> float:
    """The real Alpaca cash balance (shared with the equities strategy's
    own account -- crypto and stock buying power both draw from the same
    underlying cash)."""
    from data import alpaca_client
    account = alpaca_client.get_account()
    return float(account.get("cash") or 0.0)


def get_current_price(symbol: str) -> float | None:
    """A real-time-ish bid/ask quote."""
    from data import alpaca_client
    try:
        quote = alpaca_client.get_crypto_latest_quote(symbol)
        ask, bid = quote.get("ap"), quote.get("bp")
        if ask and bid:
            return (float(ask) + float(bid)) / 2.0
        price = ask or bid
        return float(price) if price else None
    except Exception as exc:
        logger.warning("[alpaca_crypto_strategy] quote fetch failed for %s: %s", symbol, exc)
        return None


def _reference_balance_for_today(state: dict[str, Any], available_balance_usd: float | None) -> float | None:
    """The daily loss cap is a percentage of the balance as it stood at the
    START of the day, not of whatever the tracked balance happens to be at
    the moment it's checked (which drifts throughout the day as trades
    close) -- captured once per day the first time a real balance read
    succeeds. Same pattern as perps_strategy.py's own
    _reference_balance_for_today; a real correctness gap before this: the
    old inline check used `state["balance"]` directly (the continuously-
    updated running balance), so as the day's P&L moved the balance, the
    cap's effective threshold silently drifted along with it instead of
    staying fixed to the day's actual starting point."""
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


def _normalize_symbol(symbol: str) -> str:
    """Strips separators before comparing symbols across this strategy's
    own "BTC/USD"-style keys and whatever exact string Alpaca's /v2/positions
    endpoint returns -- not guaranteed to use the identical separator
    convention, so comparing on alnum-only characters is the robust choice
    rather than assuming one specific format."""
    return "".join(ch for ch in symbol.upper() if ch.isalnum())


def _real_open_positions_by_symbol() -> dict[str, dict[str, Any]] | None:
    """Ground truth from Alpaca's own GET /v2/positions -- local
    bookkeeping only ever records an order having been PLACED, never
    confirms it actually FILLED at the assumed price/quantity (a notional
    buy's exact fill count in particular is never synchronously confirmed
    -- see build_crypto_order's own docstring). Returns None (never an
    empty dict) on a failed API call so callers can tell "confirmed no
    real positions" apart from "couldn't check" and avoid wiping out
    tracking on a transient error -- same discipline as perps_strategy.py's
    own _real_open_positions_by_ticker.

    /v2/positions returns EVERY asset class this account holds (equities
    and crypto share one Alpaca account) -- filtered here to
    asset_class=="crypto" so a stock position can never be mistaken for
    one of this strategy's own.

    Real, confirmed production bug found in review: /v2/positions returns
    crypto symbols WITHOUT the "/" separator (e.g. "XRPUSD"), not the
    "XRP/USD"-style format get_crypto_universe()/fetch_recent_crypto_bars()/
    get_current_price() all expect everywhere else in this file. An
    ADOPTED position (no local counterpart, so nothing to compare against)
    used to store this raw, slash-less symbol as-is -- confirmed live: a
    real ~$14,435 XRP position got adopted as "XRPUSD" and every
    subsequent quote fetch for it failed with a 400, leaving a real open
    position unpriceable and unmanageable (no take-profit/stop-loss/
    max-hold could ever fire). Reconstructed here by matching against the
    canonical universe list instead of trusting Alpaca's own raw format."""
    from data import alpaca_client
    from data.alpaca_crypto_data import get_crypto_universe
    try:
        positions = alpaca_client.get_positions()
    except Exception as exc:
        logger.warning("[alpaca_crypto_strategy] could not fetch real positions for reconciliation: %s", exc)
        return None
    canonical_by_normalized = {_normalize_symbol(s): s for s in get_crypto_universe()}
    result: dict[str, dict[str, Any]] = {}
    for p in positions:
        if p.get("asset_class") != "crypto":
            continue
        raw_symbol = p.get("symbol") or ""
        qty = float(p.get("qty") or 0.0)
        if not raw_symbol or qty == 0:
            continue
        normalized = _normalize_symbol(raw_symbol)
        canonical_symbol = canonical_by_normalized.get(normalized, raw_symbol)
        if canonical_symbol == raw_symbol and "/" not in raw_symbol:
            logger.warning(
                "[alpaca_crypto_strategy] could not map real position symbol %r to a canonical BASE/QUOTE pair "
                "(not in the current tradable universe) -- using it as-is, quote fetches for it will likely fail",
                raw_symbol,
            )
        result[normalized] = {
            "raw_symbol": canonical_symbol, "count": abs(qty), "entry_price": float(p.get("avg_entry_price") or 0.0),
        }
    return result


def _reconcile_positions_with_exchange(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Makes local state["positions"] match what the real Alpaca account
    actually holds before any exit/entry decision is made. Handles all
    three ways local bookkeeping can have drifted from reality (same three
    modes perps_strategy.py's own _reconcile_positions_with_exchange
    handles):
      - a real position exists that local state never recorded (a prior
        entry order's fill was never verified) -> ADOPT it, so it starts
        being monitored for take-profit/stop-loss instead of sitting with
        no coverage at all;
      - a local position's count/entry_price doesn't match the real one
        (a notional buy's actual fill differed from the assumed
        notional/entry_price split) -> CORRECT it;
      - a local position has no real counterpart at all (the entry order
        never actually filled) -> DROP it without recording a fake trade.
    Only ever called when live trading is actually active (see callers) --
    in dry-run, local positions are hypothetical (no order was ever placed)
    and deliberately have no real-exchange counterpart, so reconciling
    would just erase them."""
    local_positions = state.get("positions") or []
    real = _real_open_positions_by_symbol()
    if real is None:
        return local_positions

    local_by_normalized = {_normalize_symbol(p["symbol"]): p for p in local_positions}
    reconciled: list[dict[str, Any]] = []
    for norm_symbol, real_pos in real.items():
        local = local_by_normalized.get(norm_symbol)
        if local is None:
            logger.warning(
                "[alpaca_crypto_strategy] adopting untracked real position: %s x%.6f @ %.6f",
                real_pos["raw_symbol"], real_pos["count"], real_pos["entry_price"],
            )
            reconciled.append({
                "symbol": real_pos["raw_symbol"], "entry_price": real_pos["entry_price"], "count": real_pos["count"],
                "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
            })
            continue
        if (
            abs(float(local["count"]) - real_pos["count"]) > 1e-9
            or abs(float(local["entry_price"]) - real_pos["entry_price"]) > 1e-6
        ):
            logger.warning(
                "[alpaca_crypto_strategy] correcting local position for %s: count %.6f->%.6f, entry %.6f->%.6f",
                local["symbol"], float(local["count"]), real_pos["count"], float(local["entry_price"]), real_pos["entry_price"],
            )
        if local["symbol"] != real_pos["raw_symbol"]:
            # Self-heals a position adopted before the canonical-symbol
            # mapping fix above existed (e.g. a real position stored as
            # Alpaca's own slash-less "XRPUSD" instead of "XRP/USD") --
            # normalized keys still match here, so this would otherwise
            # keep updating count/entry_price forever on the wrong symbol
            # without ever correcting the field that's actually broken
            # (every quote fetch for it).
            logger.warning(
                "[alpaca_crypto_strategy] correcting local position symbol: %s -> %s",
                local["symbol"], real_pos["raw_symbol"],
            )
            local["symbol"] = real_pos["raw_symbol"]
        local["count"] = real_pos["count"]
        local["entry_price"] = real_pos["entry_price"]
        reconciled.append(local)

    for norm_symbol, p in local_by_normalized.items():
        if norm_symbol not in real:
            logger.warning("[alpaca_crypto_strategy] dropping phantom local position (no matching real fill): %s", p["symbol"])

    return reconciled


def scan_and_enter(symbols: list[str] | None = None, *, dry_run: bool | None = None) -> dict[str, Any]:
    """Evaluates each crypto pair for a new entry. Places a real plain
    market buy order (notional-sized) against the Alpaca account
    ALPACA_TRADING_BASE_URL points at, UNLESS dry_run resolves True --
    same dual-gate posture as every other strategy here. No market-hours
    gating: crypto trades 24/7.

    Two-phase, not first-fit: every symbol not already held gets evaluated
    FIRST, then qualifying candidates are ranked by score (see
    evaluate_candidate) and the best ones fill whatever slots are actually
    open -- same shape as perps_strategy.py's own scan_for_entries() +
    qualifying[:open_slots]. Before this, a wider universe with only
    MAX_CONCURRENT_POSITIONS=2 slots meant which 2 positions got taken was
    essentially alphabetical luck, not the highest-conviction signals.

    Dedupes by BASE ASSET (see alpaca_crypto_data.symbol_to_coin), not just
    exact symbol -- get_crypto_universe() below can return more than one
    quote currency for the same coin (e.g. BTC/USD and BTC/USDT), and
    holding both at once would just be two bets on the identical
    underlying move, not real diversification."""
    from data.alpaca_crypto_data import fetch_recent_crypto_bars, get_crypto_universe, latest_feature_row, symbol_to_coin
    from data.alpaca_crypto_model import predict_direction
    from data import threads_post

    effective_dry_run = (not LIVE_TRADING_ENABLED) if dry_run is None else dry_run
    if symbols is None:
        symbols = get_crypto_universe()

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
        positions = state.get("positions") or []
        existing_coins = {symbol_to_coin(p["symbol"]) for p in positions}
        open_count = len(positions)

        try:
            available_balance_usd = get_available_balance()
        except Exception as exc:
            available_balance_usd = None
            logger.debug("[alpaca_crypto_strategy] balance read for daily reference failed: %s", exc)
        reference_was_just_set = _today_str() not in (state.get("daily_reference_balance") or {})
        reference_balance = _reference_balance_for_today(state, available_balance_usd)
        today_pnl = float((state.get("realized_pnl_by_date") or {}).get(_today_str(), 0.0))
        loss_cap_breached = bool(
            reference_balance and reference_balance > 0
            and today_pnl <= -abs(DAILY_LOSS_CAP_PCT) * reference_balance
        )
        # push_durable only on the (once-daily) event a fresh reference
        # balance gets captured -- the value a restart must not silently lose.
        _save_state(state, push_durable=reference_was_just_set)
    if loss_cap_breached:
        return {"opened": [], "action": "daily_loss_cap_breached"}

    # --- Phase 1: evaluate every symbol not already held (network calls,
    # deliberately outside the lock) -----------------------------------
    qualifying: list[dict[str, Any]] = []
    for symbol in symbols:
        try:
            if symbol_to_coin(symbol) in existing_coins:
                continue
            row = latest_feature_row(symbol)
            if row is None:
                continue
            model_prediction = predict_direction(symbol)
            candidate = evaluate_candidate(row, model_prediction)
            if not candidate["should_enter"]:
                opened.append({"symbol": symbol, "ok": True, "action": "skipped", "reason": candidate["reason"]})
                continue
            candidate["row"] = row
            qualifying.append(candidate)
        except Exception as exc:
            opened.append({"symbol": symbol, "ok": False, "action": "entry_failed", "error": str(exc)})

    # --- Phase 2: best-scoring candidates first, fill whatever slots are
    # actually open -------------------------------------------------------
    open_slots = max(0, MAX_CONCURRENT_POSITIONS - open_count)
    qualifying.sort(key=lambda c: c.get("score", 0.0), reverse=True)
    claimed_coins: set[str] = set()
    for candidate in qualifying:
        symbol = candidate["symbol"]
        coin = symbol_to_coin(symbol)
        if coin in claimed_coins:
            opened.append({"symbol": symbol, "ok": True, "action": "skipped_correlated_coin"})
            continue
        if len(claimed_coins) >= open_slots:
            opened.append({"symbol": symbol, "ok": True, "action": "skipped_slot_taken"})
            continue
        try:
            row = candidate["row"]
            available_balance = get_available_balance()
            entry_price = row["current_price"]
            notional = compute_position_notional(available_balance)
            if notional < 1.0 or entry_price <= 0:
                opened.append({"symbol": symbol, "ok": True, "action": "skipped_insufficient_budget"})
                continue
            count = notional / entry_price

            # entry_volatility_30 threaded through here (not just onto the
            # stored position below) -- real bug caught in review: without
            # it, the STORED take_profit_price/stop_loss_price shown on the
            # dashboard would use the flat fallback percentages while the
            # actual exit decision (decide_exit, which reads
            # position["entry_volatility_30"] once the position exists)
            # used the adaptive ones, silently showing a misleading price.
            levels = position_exit_levels({"entry_price": entry_price, "entry_volatility_30": row.get("volatility_30")})
            order_id = None
            if not effective_dry_run:
                from data import alpaca_client
                order_spec = alpaca_client.build_crypto_order(symbol=symbol, side="buy", notional=notional)
                order_id = alpaca_client.place_order(order_spec)

            # Entry-time model/technical context -- what the model/filters
            # actually saw at decision time, so a post-trade analysis can
            # ask "what led to this win/loss" instead of only ever knowing
            # how it ended. Same fields (by name) perps_strategy.py already
            # captures, so alpaca_crypto_trade_analysis.py can mirror its
            # logic.
            entry_context = {
                "entry_probability_up": candidate.get("probability_up"),
                "entry_model_direction": candidate.get("model_direction"),
                "entry_reason": candidate.get("reason"),
            }
            position = {
                "symbol": symbol, "entry_price": entry_price, "count": count,
                "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "order_id": order_id, "entry_volatility_30": row.get("volatility_30"), "price_samples": [],
                **levels, **entry_context,
            }
            with _STATE_LOCK:
                state = _load_state()
                positions = state.get("positions") or []
                if any(symbol_to_coin(p["symbol"]) == coin for p in positions) or len(positions) >= MAX_CONCURRENT_POSITIONS:
                    opened.append({"symbol": symbol, "ok": True, "action": "skipped_slot_taken"})
                    continue
                positions.append(position)
                state["positions"] = positions
                _save_state(state)
            claimed_coins.add(coin)
            trade_dry_run = effective_dry_run
            opened.append({
                "symbol": symbol, "ok": True, "action": "opened", "entry_price": entry_price,
                "notional": notional, "dry_run": trade_dry_run,
            })
            try:
                threads_post.post_trade_entry(
                    ticker=symbol, side="long", entry_price=entry_price,
                    take_profit_price=levels["take_profit_price"], stop_loss_price=levels["stop_loss_price"],
                    reason=candidate["reason"], dry_run=trade_dry_run, market="crypto",
                )
            except Exception:
                logger.warning("[alpaca_crypto_strategy] Threads post for %s entry failed", symbol, exc_info=True)
            try:
                one_min_df = fetch_recent_crypto_bars(symbol)
                threads_post.post_trade_entry_chart(
                    ticker=symbol, market="crypto", candles=_candles_as_dicts(one_min_df),
                    entry_price=entry_price, take_profit_price=levels["take_profit_price"],
                    stop_loss_price=levels["stop_loss_price"],
                    entry_index=(len(one_min_df) - 1) if not one_min_df.empty else None,
                    side="long", dry_run=trade_dry_run,
                )
            except Exception:
                logger.warning("[alpaca_crypto_strategy] Threads chart post for %s entry failed", symbol, exc_info=True)
        except Exception as exc:
            opened.append({"symbol": symbol, "ok": False, "action": "entry_failed", "error": str(exc)})

    return {"opened": opened}


def manage_open_positions(*, dry_run: bool | None = None) -> dict[str, Any]:
    """Checks every open position for an exit. Unlike the equities
    strategy, there is no broker-native bracket order to reconcile
    against here -- crypto orders don't support them at all, so this
    loop's own decide_exit check is the ONLY thing that ever closes a
    crypto position. A triggered exit places a plain market sell for the
    position's stored (approximate, since a notional buy's exact fill
    quantity isn't synchronously confirmed) coin count -- verified against
    the real account right after, in live mode (see real_after below),
    same as perps_strategy.py's own post-order fill check.

    Single lock scope for the whole pass (not re-acquired per position),
    mirroring perps_strategy.py's own manage_open_positions -- necessary
    so that _update_velocity's price-sample mutations on positions that
    DON'T exit this cycle actually get persisted (a single save at the
    end covers every position, not just the ones that closed)."""
    from data import threads_post
    from data.alpaca_crypto_data import fetch_recent_crypto_bars

    effective_dry_run = (not LIVE_TRADING_ENABLED) if dry_run is None else dry_run
    with _STATE_LOCK:
        state = _load_state()
        if not effective_dry_run:
            state["positions"] = _reconcile_positions_with_exchange(state)
            _save_state(state)
        positions = state.get("positions") or []
        if not positions:
            return {"action": "no_position", "checks": []}

        remaining: list[dict[str, Any]] = []
        closed: list[dict[str, Any]] = []
        checks: list[dict[str, Any]] = []

        for position in positions:
            symbol = position.get("symbol", "<unknown>")
            try:
                current_price = get_current_price(symbol)
                if current_price is None:
                    checks.append({"symbol": symbol, "ok": False, "error": "no_quote_available"})
                    remaining.append(position)
                    continue

                now = dt.datetime.now(dt.timezone.utc)
                velocity = _update_velocity(position, current_price, now)
                current_volatility = _sample_volatility(position.get("price_samples") or [])

                should_exit, reason = decide_exit(
                    position, current_price, velocity_pct_per_min=velocity,
                    current_volatility=current_volatility, now=now,
                )
                if not should_exit:
                    checks.append({
                        "symbol": symbol, "ok": True, "exit_check": reason,
                        "velocity_pct_per_min": velocity, "current_volatility": current_volatility,
                    })
                    remaining.append(position)
                    continue
            except Exception as exc:
                logger.warning("[alpaca_crypto_strategy] could not process position for %s -- leaving untouched this cycle: %s", symbol, exc)
                checks.append({"symbol": symbol, "ok": False, "error": str(exc)})
                remaining.append(position)
                continue

            try:
                closed_count = float(position["count"])
                if not effective_dry_run:
                    from data import alpaca_client
                    try:
                        order_spec = alpaca_client.build_crypto_order(symbol=symbol, side="sell", qty=closed_count)
                        alpaca_client.place_order(order_spec)
                    except Exception as exc:
                        logger.warning("[alpaca_crypto_strategy] close order failed for %s: %s", symbol, exc)
                    # A market order can fill zero, partially, or fully --
                    # same real-fill-verification discipline as
                    # perps_strategy.py's own exit path (immediate_or_cancel
                    # orders there have come back fully canceled in
                    # production). Re-check the real position size right
                    # after to find out what actually happened before
                    # touching bookkeeping/P&L.
                    real_after = _real_open_positions_by_symbol()
                    if real_after is not None:
                        norm = _normalize_symbol(symbol)
                        closed_count = round(max(0.0, float(position["count"]) - real_after.get(norm, {}).get("count", 0.0)), 9)
                    else:
                        logger.warning(
                            "[alpaca_crypto_strategy] could not verify exit fill for %s after placing order -- assuming full close",
                            symbol,
                        )

                if closed_count <= 0:
                    # Nothing actually filled (a full cancel) -- keep
                    # monitoring the still-real position next cycle rather
                    # than booking a trade that never happened.
                    checks.append({"symbol": symbol, "ok": False, "error": "exit_order_did_not_fill"})
                    remaining.append(position)
                    continue

                fee_usd = round_trip_fee_usd(float(position["entry_price"]), current_price, closed_count)
                gross = round((current_price - float(position["entry_price"])) * closed_count, 6)
                net = round(gross - fee_usd, 6)
                by_date = state.setdefault("realized_pnl_by_date", {})
                today = _today_str()
                by_date[today] = round(float(by_date.get(today, 0.0)) + net, 6)
                opened_at = position.get("opened_at")
                closed_at = dt.datetime.now(dt.timezone.utc).isoformat()
                hold_minutes = None
                if opened_at:
                    try:
                        opened_dt = dt.datetime.fromisoformat(opened_at)
                        hold_minutes = round((dt.datetime.now(dt.timezone.utc) - opened_dt).total_seconds() / 60, 2)
                    except (ValueError, TypeError):
                        hold_minutes = None
                trade = {
                    "closed_at": closed_at, "opened_at": opened_at, "hold_minutes": hold_minutes,
                    "symbol": symbol, "entry_price": position["entry_price"], "exit_price": current_price,
                    "count": closed_count, "gross_pnl_usd": gross, "fee_usd": fee_usd, "realized_pnl_usd": net,
                    "reason": reason, "dry_run": effective_dry_run,
                    # Entry-time context copied from the position -- see
                    # scan_and_enter's own comment on why.
                    "entry_probability_up": position.get("entry_probability_up"),
                    "entry_model_direction": position.get("entry_model_direction"),
                    "entry_reason": position.get("entry_reason"),
                }
                trade_log = state.setdefault("trade_log", [])
                trade_log.append(trade)
                if len(trade_log) > MAX_TRADE_LOG_ENTRIES:
                    del trade_log[: len(trade_log) - MAX_TRADE_LOG_ENTRIES]
                closed.append(trade)
            except Exception as exc:
                logger.warning("[alpaca_crypto_strategy] exit order/booking failed for %s -- leaving untouched this cycle: %s", symbol, exc)
                checks.append({"symbol": symbol, "ok": False, "error": str(exc)})
                remaining.append(position)

        state["positions"] = remaining
        # push_durable only when a trade actually closed this cycle (real
        # money/balance moved) -- not on every 20s tick just because
        # positions/velocity samples were touched.
        _save_state(state, push_durable=bool(closed))

    for trade in closed:
        try:
            threads_post.post_trade_exit(
                ticker=trade["symbol"], side="long", entry_price=float(trade["entry_price"]),
                exit_price=trade["exit_price"], pnl_usd=trade["realized_pnl_usd"], reason=trade["reason"],
                dry_run=trade["dry_run"], market="crypto",
            )
        except Exception:
            logger.warning("[alpaca_crypto_strategy] Threads post for %s exit failed", trade["symbol"], exc_info=True)
        try:
            one_min_df = fetch_recent_crypto_bars(trade["symbol"])
            threads_post.post_trade_exit_chart(
                ticker=trade["symbol"], market="crypto", candles=_candles_as_dicts(one_min_df),
                side="long", entry_price=float(trade["entry_price"]), exit_price=trade["exit_price"],
                entry_index=_index_for_ts(one_min_df, trade.get("opened_at")),
                exit_index=_index_for_ts(one_min_df, trade.get("closed_at")),
                pnl_usd=trade["realized_pnl_usd"], dry_run=trade["dry_run"],
            )
        except Exception:
            logger.warning("[alpaca_crypto_strategy] Threads exit chart post for %s failed", trade["symbol"], exc_info=True)

    if closed:
        _maybe_run_batch_trade_analysis()

    return {"action": "closed" if closed else "no_change", "closed": closed, "checks": checks}
