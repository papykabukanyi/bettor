"""Alpaca equities/ETF trading strategy -- separate from and independent of
the Kalshi perps bot's strategy module. Long-only to start (matching the
same "start conservative, prove it out, then extend" posture the perps bot
used before enabling shorts) -- short-selling stocks needs margin approval
and carries materially different risk that hasn't been asked for here.

Alpaca charges $0 commission on equity/ETF trades (same as Schwab) -- the
real (small) costs here are the bid-ask spread and tiny regulatory fees
(SEC fee on sells, FINRA TAF), not modeled explicitly since they're a
rounding error next to Kalshi Perps' 1.6% round-trip taker fee.

Entry: a volume/volatility "something is happening right now" signal
(dollar_volume_z spike + volatility above the symbol's own recent baseline)
combined with a short-term momentum read and (once trained) the direction
model's confidence -- mirrors decide_entry_technical/evaluate_candidate's
shape in perps_strategy.py.
"""
from __future__ import annotations

import datetime as dt
import gc
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


# Re-enabled with real values per explicit user direction: only enter on
# genuine volume + volatility confirmation, even at the cost of fewer
# entries (stocks keeps its own fast/scalping hold-time profile
# unchanged -- this is an entry-quality filter, not a hold-duration one).
# These WERE disabled (MIN_VOLUME_Z=-1e9 / MIN_VOLATILITY_RATIO=0.0, both
# literally unsatisfiable-as-a-filter) after review found requiring a
# volume spike AND a volatility-ratio jump AND a dip, all three
# simultaneously, made real entries rare -- confirmed live, almost every
# cycle across the whole watchlist skipped on "volume not unusual enough"
# before the dip signal was even checked. That history is real, but the
# tradeoff is now a deliberate choice, not an accident: 1.0 (a
# meaningfully-above-average z-score, not an extreme one) and 1.1 (only
# 10% more active than this ticker's OWN 30-min baseline) are picked to be
# real filters without reproducing the near-zero-entries regime the
# original, stricter values caused -- same reasoning and same values as
# alpaca_crypto_strategy.py's own identical fix. Also fixes the real
# -Infinity/JSON.parse() production bug the -1e9 sentinel itself was
# working around: -1e9 is a real, finite float (serializes fine), unlike
# float("-inf") which Python's json module emits as the bare token
# `-Infinity` -- invalid JSON grammar that silently broke
# /api/alpaca/status's entire auto-refresh loop in every browser.
MIN_VOLUME_Z = _env_float("ALPACA_MIN_VOLUME_Z", 1.0)
MIN_VOLATILITY_RATIO = _env_float("ALPACA_MIN_VOLATILITY_RATIO", 1.1)  # volatility_5 / volatility_30

# "Enter on a small pullback, same 0.15% perps_strategy.py itself uses"
ENTRY_DIP_PCT = _env_float("ALPACA_ENTRY_DIP_PCT", 0.0015)
SHORT_MA_MINUTES = _env_int("ALPACA_SHORT_MA_MINUTES", 15)

TAKE_PROFIT_PCT = _env_float("ALPACA_TAKE_PROFIT_PCT", 0.01)
STOP_LOSS_PCT = _env_float("ALPACA_STOP_LOSS_PCT", 0.008)
MAX_HOLD_MINUTES = _env_int("ALPACA_MAX_HOLD_MINUTES", 120)
# Per-ticker adaptive take-profit/stop-loss -- same methodology
# perps_strategy.py/alpaca_crypto_strategy.py already use (see either
# module's own adaptive_exit_pcts docstring for the full rationale): one
# flat TAKE_PROFIT_PCT/STOP_LOSS_PCT applied identically to a quiet
# blue-chip and a much choppier small-cap alike is a real gap, not a
# simplification -- "more individual to the ticker" starts here. Vol
# multiples/floors/ceilings are carried over from crypto's own tuned
# values (crypto's own flat TAKE_PROFIT_PCT/STOP_LOSS_PCT defaults are
# IDENTICAL to stocks' own, 0.01/0.008 -- a reasoned starting point given
# that overlap, NOT a claim of having been independently re-swept against
# stocks' own volatility distribution yet -- fully env-overridable once
# real trade history justifies retuning.
TAKE_PROFIT_VOL_MULTIPLE = _env_float("ALPACA_TAKE_PROFIT_VOL_MULTIPLE", 1.5)
STOP_LOSS_VOL_MULTIPLE = _env_float("ALPACA_STOP_LOSS_VOL_MULTIPLE", 1.0)
MIN_TAKE_PROFIT_PCT = _env_float("ALPACA_MIN_TAKE_PROFIT_PCT", 0.01)
MAX_TAKE_PROFIT_PCT = _env_float("ALPACA_MAX_TAKE_PROFIT_PCT", 0.04)
MIN_STOP_LOSS_PCT = _env_float("ALPACA_MIN_STOP_LOSS_PCT", 0.005)
MAX_STOP_LOSS_PCT = _env_float("ALPACA_MAX_STOP_LOSS_PCT", 0.03)
# Tried and reverted: a "stale/flat position" early exit (see
# perps_strategy.py's own comment for the identical mechanism and full
# rationale for reverting it) was added here too, then reverted after a
# real multi-fold backtest on perps showed it performed worse than not
# having it at every hold time tested, 16/16 comparisons -- the mechanism
# cut positions early without reducing the fee cost of a flat trade, while
# forfeiting the chance for a quiet-then-moving position to still reach
# take_profit. Reverted here for consistency. If revisiting, backtest this
# strategy's own history first -- don't re-add on hypothesis alone.
#
# max_hold_time shouldn't be the ONLY factor forcing an exit -- see
# perps_strategy.py's own PROMISING_PROGRESS_FRACTION comment for the full
# rationale, thresholds, and real backtest findings (price-progress alone
# showed a real, if modest, improvement there; the volume/momentum/breakout
# path tested WORSE in isolation, so it's kept conservative/near-dormant by
# default here too -- ported for consistency, not independently backtested
# on stocks' own history). Extension is more generous than perps' here:
# stocks (unlike Kalshi perps) carries no periodic funding payment to worry
# about crossing.
MAX_HOLD_EXTENSION_MINUTES = _env_int("ALPACA_MAX_HOLD_EXTENSION_MINUTES", 120)
PROMISING_PROGRESS_FRACTION = _env_float("ALPACA_PROMISING_PROGRESS_FRACTION", 0.25)
PROMISING_VOLUME_Z = _env_float("ALPACA_PROMISING_VOLUME_Z", 1.0)
PROMISING_MOMENTUM_PCT = _env_float("ALPACA_PROMISING_MOMENTUM_PCT", 0.0003)
PROMISING_BREAKOUT_PCT_B = _env_float("ALPACA_PROMISING_BREAKOUT_PCT_B", 0.85)
PROMISING_SENTIMENT_SCORE = _env_float("ALPACA_PROMISING_SENTIMENT_SCORE", 0.3)

# Real, confirmed bug found via a real model fit + backtest this session:
# 0.55 was NEVER a real, achievable bar -- the model's own real
# probability_up distribution on real held-out data topped out around
# p90=0.51 (confirmed live too: every watchlist symbol checked in
# production sat at 48-52% confidence), so requiring 0.55 meant the
# confident-up rate was ~0.02% of rows -- essentially zero, which is
# exactly why this account went 10+ days without a single trade. Same
# class of bug, same fix, as perps_strategy.py's own PROMISING_MODEL_CONFIDENCE
# (calibrate the threshold from the model's own real output distribution,
# not a guessed round number). A real single-split backtest (80/20,
# random_forest, 121 days/10 symbols) swept 0.50-0.55: 0.52 was the
# clear, real winner -- 63 trades, 61.9% win rate, +6.27% return, vs. 0
# trades (and thus 0% return) at the old 0.55. Every threshold BELOW
# 0.52 traded more often but for a worse risk-adjusted return (0.50: 136
# trades but only +3.11%), so this isn't "loosen it as much as possible"
# the way perps' volume gate wasn't -- 0.52 is a real, evidence-picked
# middle, not the loosest option tested.
MODEL_CONFIDENCE_MIN = _env_float("ALPACA_MODEL_CONFIDENCE_MIN", 0.52)

# At the perps-style default of 10% per slot across 5 slots, a $100 account
# gets a $10-20 budget per position -- not enough to buy even ONE share of
# most liquid, well-known stocks (a $100+ share price is completely
# ordinary). Real diversification across 5 positions needs real capital;
# at $100, spreading thin just means most "positions" silently can't afford
# a single share. Concentrating into fewer, larger slots is the only way a
# small account can actually hold real, liquid names -- "safe" here comes
# from the tight stop-loss on each position, not from spreading a tiny
# balance thinner.
POSITION_SIZE_PCT = _env_float("ALPACA_POSITION_SIZE_PCT", 0.45)
MAX_CONCURRENT_POSITIONS = max(1, _env_int("ALPACA_MAX_CONCURRENT_POSITIONS", 2))
DAILY_LOSS_CAP_PCT = _env_float("ALPACA_DAILY_LOSS_CAP_PCT", 0.10)
# Same unbounded-growth guard perps_strategy.py already needed (a real,
# confirmed OOM contributor there over weeks of live trading) -- keeps the
# most recent entries, oldest-first trimmed.
MAX_TRADE_LOG_ENTRIES = _env_int("ALPACA_MAX_TRADE_LOG_ENTRIES", 2000)

LIVE_TRADING_ENABLED = str(os.getenv("ALPACA_LIVE_TRADING_ENABLED", "")).strip().lower() in {"1", "true", "yes"}


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


def evaluate_candidate(
    row: dict[str, Any], model_prediction: dict[str, Any] | None, *, confidence_min: float | None = None,
) -> dict[str, Any]:
    """`confidence_min` overrides the module-level MODEL_CONFIDENCE_MIN
    default when given -- see scan_and_enter, which reads a durable-state
    override set by alpaca_trade_analysis.recommend_confidence_threshold's
    own evidence-gated tuning (apply_confidence_threshold_override), same
    pattern perps_strategy.py already uses."""
    technical_ok, technical_reason = decide_entry_technical(row)
    result: dict[str, Any] = {
        "symbol": row.get("symbol"), "technical_ok": technical_ok, "reason": technical_reason,
        "model_ok": False, "should_enter": False, "score": 0.0,
    }
    if not technical_ok:
        return result

    effective_confidence_min = confidence_min if confidence_min is not None else MODEL_CONFIDENCE_MIN
    if model_prediction and model_prediction.get("model_ok"):
        proba_up = model_prediction["probability_up"]
        result["model_ok"] = True
        result["probability_up"] = proba_up
        result["model_direction"] = "up" if proba_up >= 0.5 else "down"
        if proba_up >= effective_confidence_min:
            result["should_enter"] = True
            result["reason"] = f"{technical_reason} + model confident up ({proba_up:.2%})"
            result["score"] = proba_up
    else:
        # No trained model yet -- technical-only fallback (same posture as
        # perps_strategy.py during the first days of data collection).
        # Scored by dip depth (deeper dip = higher score), mirroring
        # perps_strategy.py's own technical-only fallback score.
        result["should_enter"] = True
        short_ma = row.get("short_ma") or 0.0
        current_price = row.get("current_price") or 0.0
        result["score"] = ENTRY_DIP_PCT + ((short_ma - current_price) / short_ma if short_ma > 0 else 0.0)

    return result


def adaptive_exit_pcts(entry_volatility_30: float | None) -> dict[str, float]:
    """Take-profit/stop-loss percentages customized to ONE specific
    symbol's own volatility at entry time -- see TAKE_PROFIT_VOL_MULTIPLE's
    own comment for the full rationale. Falls back to the flat global
    TAKE_PROFIT_PCT/STOP_LOSS_PCT if no volatility was captured (e.g. a
    position opened before this field existed) -- same value every
    position used before this change, so nothing regresses for positions
    that predate it. Also falls back on NaN specifically (not just
    falsy/<=0) -- a real edge case: Python's own NaN comparisons are
    always False, so a rolling-window feature that's still NaN this early
    would otherwise silently slip past a plain `<= 0` guard."""
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
    """Long-only: a RISING price is favorable. Mirrors perps_strategy.py's
    decide_exit() shape (take-profit / stop-loss / max-hold), simplified
    since there's no short side or leverage-fee interaction to account for
    here. Exit levels are per-position ADAPTIVE (see adaptive_exit_pcts) --
    scaled to this specific symbol's own volatility_30 at entry, not one
    flat percentage applied identically to a quiet blue-chip and a much
    choppier small-cap alike.

    `dollar_volume_z`/`momentum_pct` (macd_hist_pct)/`breakout_pct_b`
    (bb_pct_b)/`sentiment_score` feed the max_hold_time "promising position"
    extension only -- see perps_strategy.py's own PROMISING_PROGRESS_FRACTION
    comment for the full rationale, thresholds, and real backtest findings
    (ported here for consistency, not independently backtested on stocks'
    own history)."""
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
    now = now if now is not None else dt.datetime.now(dt.timezone.utc)
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
    derived from the same per-symbol-adaptive percentages decide_exit()
    applies (see adaptive_exit_pcts) -- exists so callers (the bracket
    order placed at entry, the dashboard) can show/use real exit levels
    rather than just trusting the flat config exists somewhere."""
    entry_price = float(position["entry_price"])
    exit_pcts = adaptive_exit_pcts(position.get("entry_volatility_30"))
    return {
        "take_profit_price": round(entry_price * (1 + exit_pcts["take_profit_pct"]), 6),
        "stop_loss_price": round(entry_price * (1 - exit_pcts["stop_loss_pct"]), 6),
    }


def compute_position_size(available_balance_usd: float, price: float) -> int:
    """Whole shares only. Alpaca supports fractional shares on plain
    market/day orders, but NOT on bracket/OCO orders (which this strategy
    relies on for the take-profit/stop-loss pair) -- so integer sizing here
    isn't just conservative, it's a real requirement."""
    if price <= 0:
        return 0
    budget = available_balance_usd * POSITION_SIZE_PCT
    return int(budget // price)


def _candles_as_dicts(df) -> list[dict[str, Any]]:
    """Converts a fetch_recent_minute_bars-style DataFrame into the plain
    list[dict] shape both chart_snapshot.generate_candlestick_chart and
    alpaca_trade_analysis expect -- keeps those two modules pandas-free."""
    if df is None or df.empty:
        return []
    cols = [c for c in ("ts", "open", "high", "low", "close") if c in df.columns]
    return df[cols].to_dict("records")


def _index_for_ts(df, iso_ts: str | None) -> int | None:
    """Which row of a fetch_recent_minute_bars-style DataFrame is closest to
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
    """Every alpaca_trade_analysis.BATCH_SIZE newly-closed REAL trades,
    studies that recent batch -- win/loss patterns, missed-profit/
    premature-stop diagnostics from real OHLC -- and posts a Threads
    snapshot. Called right after manage_open_positions closes trades,
    outside _STATE_LOCK (same reasoning as every other Threads/network call
    in this module). Best-effort: any failure here is logged and
    swallowed, never allowed to affect trading."""
    from data import alpaca_trade_analysis
    from data import threads_post
    from data.alpaca_data import fetch_recent_minute_bars

    try:
        with _STATE_LOCK:
            state = _load_state()
            trade_log = state.get("trade_log") or []
            real_trades = [t for t in trade_log if not t.get("dry_run")]
            last_count = int(state.get("last_batch_analysis_trade_count") or 0)
            if len(real_trades) - last_count < alpaca_trade_analysis.BATCH_SIZE:
                return
            state["last_batch_analysis_trade_count"] = len(real_trades)
            _save_state(state, push_durable=True)
            current_threshold = (state.get("tuning") or {}).get("model_confidence_min", MODEL_CONFIDENCE_MIN)

        recent = real_trades[-alpaca_trade_analysis.BATCH_SIZE:]
        candles_by_symbol: dict[str, list[dict[str, Any]]] = {}
        for symbol in {t.get("symbol") for t in recent if t.get("symbol")}:
            try:
                candles_by_symbol[symbol] = _candles_as_dicts(fetch_recent_minute_bars(symbol))
            except Exception:
                logger.debug("[alpaca_strategy] candle fetch for batch analysis failed for %s", symbol, exc_info=True)

        batch = alpaca_trade_analysis.analyze_recent_trade_batch(real_trades, candles_by_symbol=candles_by_symbol)
        text = alpaca_trade_analysis.format_batch_snapshot_text(batch, market="stocks")
        threads_post.post_trade_analysis_summary(text, market="stocks")

        tuning_rec = alpaca_trade_analysis.recommend_confidence_threshold(real_trades, current_threshold=current_threshold)
        if tuning_rec.get("should_apply"):
            apply_confidence_threshold_override(tuning_rec["recommended_threshold"], reason="5-trade batch review")
    except Exception:
        logger.warning("[alpaca_strategy] batch trade analysis failed", exc_info=True)


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
STATE_FILE = Path(os.getenv("ALPACA_STATE_FILE", str(DATA_DIR / "alpaca_state.json")))
_STATE_LOCK = threading.RLock()

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_ALPACA_MODEL_REPO = os.getenv("HF_ALPACA_MODEL_REPO", "papylove/alpaca-model")
_DURABLE_STATE_HF_FILENAME = "alpaca_durable_state.json"
_DURABLE_PUSH_MIN_INTERVAL_SEC = 30
_last_durable_push_ts = 0.0


def _today_str() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def apply_confidence_threshold_override(new_threshold: float, *, reason: str) -> dict[str, Any]:
    """Applies an evidence-gated confidence-floor adjustment (see
    alpaca_trade_analysis.recommend_confidence_threshold) durably, WITHOUT a
    redeploy -- stored in state["tuning"] (pushed to HF like the rest of
    durable state) and read by scan_and_enter on every cycle, not the OS
    env var MODEL_CONFIDENCE_MIN is seeded from at import time. Same
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
    # apply_confidence_threshold_override) MUST be included here -- a real,
    # confirmed bug found in perps_strategy.py's own identical slice left
    # it out, silently resetting any confidence threshold actually LEARNED
    # from real trade history back to the hardcoded default on every single
    # deploy (local disk doesn't survive a fresh deploy; this slice is what
    # survives via HF). Included from the start here rather than repeating
    # that bug.
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
                repo_id=HF_ALPACA_MODEL_REPO, repo_type="model", commit_message="update alpaca durable state",
            )
        finally:
            os.unlink(tmp_path)
    except Exception as exc:
        logger.warning("[alpaca_strategy] durable state push to HF failed: %s", exc)


_DURABLE_STATE_HF_TIMEOUT_SEC = int(os.getenv("ALPACA_DURABLE_STATE_HF_TIMEOUT_SEC", "10") or "10")


def _pull_durable_state_from_hf() -> dict[str, Any] | None:
    if not HF_API_KEY:
        return None

    def _download() -> dict[str, Any]:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(
            repo_id=HF_ALPACA_MODEL_REPO, filename=_DURABLE_STATE_HF_FILENAME, repo_type="model", token=HF_API_KEY,
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
        logger.info("[alpaca_strategy] no durable state on HF yet (or fetch failed): %s", exc)
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
            logger.info("[alpaca_strategy] recovered durable state from HF after local state was missing")
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
    """The real Alpaca cash balance."""
    from data import alpaca_client
    account = alpaca_client.get_account()
    return float(account.get("cash") or 0.0)


def get_current_price(symbol: str) -> float | None:
    """A real-time-ish bid/ask quote (cheap, no bars history needed)."""
    from data import alpaca_client
    try:
        quote = alpaca_client.get_latest_quote(symbol)
        ask, bid = quote.get("ap"), quote.get("bp")
        if ask and bid:
            return (float(ask) + float(bid)) / 2.0
        price = ask or bid
        return float(price) if price else None
    except Exception as exc:
        logger.warning("[alpaca_strategy] quote fetch failed for %s: %s", symbol, exc)
        return None


def _reference_balance_for_today(state: dict[str, Any], available_balance_usd: float | None) -> float | None:
    """The daily loss cap is a percentage of the balance as it stood at the
    START of the day, not of whatever Alpaca's account balance happens to be
    at the moment it's checked (which drifts throughout the day as trades
    close) -- captured once per day the first time a real balance read
    succeeds. Same pattern as perps_strategy.py's/alpaca_crypto_strategy.py's
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
    asset_class=="us_equity" so a crypto or options position can never be
    mistaken for one of this strategy's own."""
    from data import alpaca_client
    try:
        positions = alpaca_client.get_positions()
    except Exception as exc:
        logger.warning("[alpaca_strategy] could not fetch real positions for reconciliation: %s", exc)
        return None
    result: dict[str, dict[str, Any]] = {}
    for p in positions:
        if p.get("asset_class") != "us_equity":
            continue
        symbol = p.get("symbol") or ""
        qty = float(p.get("qty") or 0.0)
        if not symbol or qty == 0:
            continue
        result[symbol] = {"count": abs(qty), "entry_price": float(p.get("avg_entry_price") or 0.0)}
    return result


def _reconcile_positions_with_exchange(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Makes local state["positions"] match what the real Alpaca account
    actually holds before any exit/entry decision is made. Handles all
    three ways local bookkeeping can have drifted from reality (same three
    modes alpaca_crypto_strategy.py's own _reconcile_positions_with_exchange
    handles):
      - a real position exists that local state never recorded (a prior
        entry order's fill was never verified) -> ADOPT it, so it starts
        being monitored for take-profit/stop-loss instead of sitting with
        no coverage at all;
      - a local position's count/entry_price doesn't match the real one
        -> CORRECT it;
      - a local position has no real counterpart at all (the entry order
        never actually filled) -> DROP it without recording a fake trade.
    Only ever called when live trading is actually active (see callers) --
    in dry-run, local positions are hypothetical (no order was ever placed)
    and deliberately have no real-exchange counterpart, so reconciling
    would just erase them.

    Complements (doesn't replace) the existing get_position() double-sell
    guard in manage_open_positions() -- that guard protects against a
    bracket order's own native TP/SL having already closed a position out
    from under this loop; this proactively fixes count/entry_price drift
    and adopts positions this process never even knew about."""
    local_positions = state.get("positions") or []
    real = _real_open_positions_by_symbol()
    if real is None:
        return local_positions

    local_by_symbol = {p["symbol"]: p for p in local_positions}
    reconciled: list[dict[str, Any]] = []
    for symbol, real_pos in real.items():
        local = local_by_symbol.get(symbol)
        if local is None:
            logger.warning(
                "[alpaca_strategy] adopting untracked real position: %s x%.4f @ %.4f",
                symbol, real_pos["count"], real_pos["entry_price"],
            )
            reconciled.append({
                "symbol": symbol, "entry_price": real_pos["entry_price"], "count": real_pos["count"],
                "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
            })
            continue
        if (
            abs(float(local["count"]) - real_pos["count"]) > 1e-9
            or abs(float(local["entry_price"]) - real_pos["entry_price"]) > 1e-6
        ):
            logger.warning(
                "[alpaca_strategy] correcting local position for %s: count %.4f->%.4f, entry %.4f->%.4f",
                symbol, float(local["count"]), real_pos["count"], float(local["entry_price"]), real_pos["entry_price"],
            )
        local["count"] = real_pos["count"]
        local["entry_price"] = real_pos["entry_price"]
        reconciled.append(local)

    for symbol in local_by_symbol:
        if symbol not in real:
            logger.warning("[alpaca_strategy] dropping phantom local position (no matching real fill): %s", symbol)

    return reconciled


def scan_and_enter(watchlist: list[str] | None = None, *, dry_run: bool | None = None) -> dict[str, Any]:
    """Evaluates each watchlist symbol for a new entry. Places a real order
    against the Alpaca account ALPACA_TRADING_BASE_URL points at UNLESS
    dry_run resolves True, in which case it only reports what it would have
    done -- same dual-gate posture as perps_strategy.py.

    Session-aware, "stream live like perps" -- data collection already ran
    unconditionally 24/7 (no market-hours gate at all), but entries didn't
    account for WHICH kind of order Alpaca actually allows outside the
    regular 9:30-4:00 ET session: a bracket order (entry + linked TP/SL) is
    regular-hours only, and even a plain order must be type="limit" with
    extended_hours=true during pre/post-market -- a market order there is
    simply rejected. Regular session keeps the existing bracket order
    unchanged; pre/post-market places a plain extended-hours limit order
    instead (manage_open_positions then owns TP/SL/max-hold via its own
    poll loop for that position, the same pattern already proven for
    crypto/options, which never had broker-native brackets to begin with).
    Fully closed (no session at all, ~8pm-4am ET) skips entirely -- nothing
    is fillable then regardless of order type."""
    from data.alpaca_data import (
        fetch_recent_minute_bars, get_company_name, get_market_session, get_stock_watchlist,
        latest_feature_row, load_training_dataset, prewarm_sentiment,
    )
    from data.alpaca_model import predict_direction
    from data import threads_post

    effective_dry_run = (not LIVE_TRADING_ENABLED) if dry_run is None else dry_run
    market_session = get_market_session()
    if market_session["session"] == "closed":
        return {"opened": [], "action": "market_closed"}
    is_extended_hours = market_session["session"] != "regular"
    if watchlist is None:
        try:
            # Real, confirmed production incident: this service kept
            # OOM-killing every 15-30 minutes even after the del+gc.collect()
            # below was added, because gc.collect() only frees references
            # AFTER this call returns -- it cannot reduce the PEAK memory
            # reached DURING load_training_dataset() itself. This call is
            # ranking-only (feeding get_stock_watchlist's volume/volatility
            # sort), not training, so it never needed training-grade depth --
            # matching perps_data.py's own _recent_volatility_by_ticker(),
            # which deliberately uses max_rows=5000 for the identical
            # "just need a ranking signal" use case.
            recent = load_training_dataset(max_rows=5_000)
        except Exception:
            recent = None
        watchlist = get_stock_watchlist(recent if recent is not None and not recent.empty else None)
        del recent
        gc.collect()

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
        existing_symbols = {p["symbol"] for p in (state.get("positions") or [])}
        open_count = len(state.get("positions") or [])
        try:
            available_balance_usd = get_available_balance()
        except Exception as exc:
            available_balance_usd = None
            logger.debug("[alpaca_strategy] balance read for daily reference failed: %s", exc)
        reference_was_just_set = _today_str() not in (state.get("daily_reference_balance") or {})
        reference_balance = _reference_balance_for_today(state, available_balance_usd)
        today_pnl = float((state.get("realized_pnl_by_date") or {}).get(_today_str(), 0.0))
        loss_cap_breached = bool(
            reference_balance and reference_balance > 0
            and today_pnl <= -abs(DAILY_LOSS_CAP_PCT) * reference_balance
        )
        # A confidence floor genuinely learned from this account's own real
        # trade history (see alpaca_trade_analysis.recommend_confidence_threshold
        # + apply_confidence_threshold_override below) -- falls back to the
        # module-level MODEL_CONFIDENCE_MIN default until enough real trades
        # exist to justify moving it. Same pattern perps_strategy.py uses.
        confidence_min_override = (state.get("tuning") or {}).get("model_confidence_min")
        _save_state(state, push_durable=reference_was_just_set)
    if loss_cap_breached:
        return {"opened": [], "action": "daily_loss_cap_breached"}

    # See stock_news.prewarm_sentiment's own docstring for the full,
    # confirmed root cause this fixes on the crypto side (same shape here)
    # -- fetches sentiment for every not-yet-held symbol CONCURRENTLY so
    # the sequential loop below hits a warm cache instead of each symbol's
    # own blocking network fetch.
    try:
        prewarm_sentiment([(s, get_company_name(s)) for s in watchlist if s not in existing_symbols])
    except Exception as exc:
        logger.debug("[alpaca_strategy] sentiment prewarm failed (non-fatal): %s", exc)

    for symbol in watchlist:
        try:
            if symbol in existing_symbols:
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

            available_balance = get_available_balance()
            entry_price = row["current_price"]
            count = compute_position_size(available_balance, entry_price)
            if count < 1:
                opened.append({"symbol": symbol, "ok": True, "action": "skipped_insufficient_budget"})
                continue

            # entry_volatility_30 threaded through here (not just onto the
            # stored position below) -- without it, the STORED
            # take_profit_price/stop_loss_price (and the broker-native
            # bracket order placed from them) would use the flat fallback
            # percentages while the actual exit decision (decide_exit,
            # which reads position["entry_volatility_30"] once the
            # position exists) used the adaptive ones -- a real bug this
            # session already caught and fixed once for crypto/options'
            # own entry-time context capture; wired correctly here from
            # the start.
            levels = position_exit_levels({"entry_price": entry_price, "entry_volatility_30": row.get("volatility_30")})
            order_id = None
            if not effective_dry_run:
                from data import alpaca_client
                if is_extended_hours:
                    # Small marketable buffer above the last price -- a
                    # genuine limit (Alpaca requires one outside regular
                    # hours), sized to actually have a real chance of
                    # filling rather than sitting unfilled all session.
                    order_spec = alpaca_client.build_extended_hours_limit_order(
                        symbol=symbol, quantity=count, side="buy", limit_price=entry_price * 1.002,
                    )
                else:
                    order_spec = alpaca_client.build_bracket_order(
                        symbol=symbol, quantity=count, side="buy",
                        take_profit_price=levels["take_profit_price"], stop_loss_price=levels["stop_loss_price"],
                    )
                order_id = alpaca_client.place_order(order_spec)

            # Entry-time model/technical context -- what the model/filters
            # actually saw at decision time, so a post-trade analysis can
            # ask "what led to this win/loss" instead of only ever knowing
            # how it ended. Same fields (by name) perps_strategy.py already
            # captures, so alpaca_trade_analysis.py can mirror its logic.
            entry_context = {
                "entry_probability_up": candidate.get("probability_up"),
                "entry_model_direction": candidate.get("model_direction"),
                "entry_reason": candidate.get("reason"),
                # candidate["score"] -- the one number actually compared
                # against MODEL_CONFIDENCE_MIN at entry (or the technical-
                # only fallback's dip-depth score) -- carried through so
                # recommend_confidence_threshold can ask real trade history
                # whether a HIGHER floor would have performed better.
                "entry_score": candidate.get("score"),
                # Raw indicator values at decision time -- carried through
                # to the closed trade record so the exit chart's indicator
                # panel can show what the bot saw at ENTRY, not just exit
                # (see perps_strategy.py's identical pattern).
                "entry_dollar_volume_z": row.get("dollar_volume_z"),
                "entry_macd_hist_pct": row.get("macd_hist_pct"),
                "entry_bb_pct_b": row.get("bb_pct_b"),
                "entry_rsi_14": row.get("rsi_14"),
                "entry_sentiment_score": row.get("sentiment_score"),
            }
            position = {
                "symbol": symbol, "entry_price": entry_price, "count": float(count),
                "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "order_id": order_id, "entry_volatility_30": row.get("volatility_30"),
                **levels, **entry_context,
            }
            with _STATE_LOCK:
                state = _load_state()
                positions = state.get("positions") or []
                if any(p["symbol"] == symbol for p in positions) or len(positions) >= MAX_CONCURRENT_POSITIONS:
                    opened.append({"symbol": symbol, "ok": True, "action": "skipped_slot_taken"})
                    continue
                positions.append(position)
                state["positions"] = positions
                _save_state(state)
                existing_symbols.add(symbol)
                open_count = len(positions)
            trade_dry_run = effective_dry_run
            opened.append({
                "symbol": symbol, "ok": True, "action": "opened", "entry_price": entry_price,
                "count": count, "dry_run": trade_dry_run,
            })
            # Best-effort only -- see threads_post.py's own docstring. Never
            # allow a Threads failure (or it simply not being configured
            # yet) to affect the real/simulated entry that already happened
            # above. Alpaca strategy is long-only (see decide_exit's own
            # docstring), so side is always "long" here.
            try:
                threads_post.post_trade_entry(
                    ticker=symbol, side="long", entry_price=entry_price,
                    take_profit_price=levels["take_profit_price"], stop_loss_price=levels["stop_loss_price"],
                    reason=candidate["reason"], dry_run=trade_dry_run, market="stocks",
                )
            except Exception:
                logger.warning("[alpaca_strategy] Threads post for %s entry failed", symbol, exc_info=True)
            try:
                from data import chart_snapshot
                one_min_df = fetch_recent_minute_bars(symbol)
                indicators = chart_snapshot.format_technical_indicators(row)
                if candidate.get("probability_up") is not None:
                    indicators["Model prob up"] = f"{candidate['probability_up']:.1%}"
                threads_post.post_trade_entry_chart(
                    ticker=symbol, market="stocks", candles=_candles_as_dicts(one_min_df),
                    entry_price=entry_price, take_profit_price=levels["take_profit_price"],
                    stop_loss_price=levels["stop_loss_price"],
                    entry_index=(len(one_min_df) - 1) if not one_min_df.empty else None,
                    side="long", dry_run=trade_dry_run, indicators=indicators,
                )
            except Exception:
                logger.warning("[alpaca_strategy] Threads chart post for %s entry failed", symbol, exc_info=True)
        except Exception as exc:
            opened.append({"symbol": symbol, "ok": False, "action": "entry_failed", "error": str(exc)})

    return {"opened": opened}


def manage_open_positions(*, dry_run: bool | None = None) -> dict[str, Any]:
    """Checks every open position for an exit. The take-profit/stop-loss
    are ALREADY live on the exchange as a bracket order the instant the
    entry filled (see scan_and_enter) -- this loop's main job is to catch
    max_hold_time (which Alpaca's bracket order has no native concept of)
    and force-close in that case.

    Before forcing an exit, this checks whether the position still actually
    exists on Alpaca (get_position). If Alpaca's own bracket take-profit/
    stop-loss already fired the exit moments earlier, the position is
    already gone -- forcing another close_position call there would be a
    real double-sell attempt, not just a redundant one, so this reconciles
    using the position's own stored target level instead of calling
    close_position again. _reconcile_positions_with_exchange (below) is a
    complementary, proactive check -- it fixes count/entry_price drift and
    adopts untracked positions, but doesn't replace this in-the-moment
    double-sell guard."""
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
    remaining_symbols = {p["symbol"] for p in positions}

    for position in positions:
        symbol = position["symbol"]
        try:
            current_price = get_current_price(symbol)
            if current_price is None:
                checks.append({"symbol": symbol, "ok": False, "error": "no_quote_available"})
                continue

            # Volume/momentum/breakout/sentiment only matter to decide_exit's
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
                    promising_row = latest_feature_row(symbol)
                except Exception as exc:
                    promising_row = None
                    logger.debug("[alpaca_strategy] promising-signal feature fetch failed for %s: %s", symbol, exc)
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
                checks.append({"symbol": symbol, "ok": True, "exit_check": reason})
                continue

            closed_count = float(position["count"])
            if not effective_dry_run:
                from data import alpaca_client
                from data.alpaca_data import get_market_session
                # Checked fresh at EXIT time, not entry time -- a position
                # opened during regular hours can still be sitting open
                # once the session rolls into post-market (max_hold_time,
                # a slow-moving stop), and close_position()'s at-market
                # liquidation would be rejected there just the same as for
                # a position that was opened extended-hours to begin with.
                exit_session = get_market_session()["session"]
                if exit_session == "closed":
                    checks.append({"symbol": symbol, "ok": True, "exit_deferred": "market_closed"})
                    continue
                still_open = True
                try:
                    still_open = alpaca_client.get_position(symbol) is not None
                except Exception as exc:
                    logger.warning("[alpaca_strategy] position lookup failed for %s, assuming still open: %s", symbol, exc)
                if still_open:
                    try:
                        if exit_session == "regular":
                            alpaca_client.close_position(symbol)
                        else:
                            # Extended hours: close_position() liquidates
                            # at market, which Alpaca rejects outside
                            # 9:30-4:00 ET -- needs a plain limit sell with
                            # extended_hours=true instead (no bracket to
                            # rely on either way -- see scan_and_enter).
                            #
                            # Real, confirmed live incident (2026-08-18):
                            # every entry here is a bracket order, whose
                            # take-profit/stop-loss CHILD legs stay open on
                            # Alpaca's books until the bracket parent fills
                            # or they're explicitly canceled -- close_position()
                            # cancels them automatically (its own docstring),
                            # but this manual extended-hours path never did.
                            # Placing a new sell limit while the bracket's own
                            # open sell-side TP leg still exists oversells the
                            # position from Alpaca's perspective, rejected
                            # outright with 403 -- confirmed via 1149
                            # consecutive failures on one real NVDA position
                            # over ~6.5 hours, unable to exit the whole time.
                            # Cancel whatever's still open for this symbol
                            # first so the new order is the only one live.
                            try:
                                for open_order in alpaca_client.get_orders(status="open", symbols=[symbol]):
                                    order_id = open_order.get("id")
                                    if order_id:
                                        alpaca_client.cancel_order(order_id)
                            except Exception as exc:
                                logger.warning(
                                    "[alpaca_strategy] could not cancel existing open orders for %s before "
                                    "extended-hours exit (will still attempt the exit order): %s", symbol, exc,
                                )
                            order_spec = alpaca_client.build_extended_hours_limit_order(
                                symbol=symbol, quantity=position["count"], side="sell",
                                limit_price=current_price * 0.998,
                            )
                            alpaca_client.place_order(order_spec)
                    except Exception as exc:
                        logger.warning("[alpaca_strategy] close_position failed for %s (may have already closed): %s", symbol, exc)
                    # A submitted close order is not a confirmed fill -- real,
                    # confirmed production incident: booking P&L here
                    # unconditionally produced 8 duplicate "closed" trade_log
                    # entries for the SAME real META position (a stop_loss
                    # that took several 20s fast_check cycles to actually
                    # fill) -- each re-discovered as "untracked" by
                    # reconciliation next cycle and re-booked, ~$6,400 of
                    # phantom recorded loss stacked on top of the one real
                    # close. Same real-fill-verification discipline already
                    # proven in perps_strategy.py/alpaca_crypto_strategy.py's
                    # own exit paths: re-check the real remaining quantity
                    # right after, and only book what actually closed.
                    try:
                        pos_after = alpaca_client.get_position(symbol)
                        remaining_qty = float(pos_after["qty"]) if pos_after else 0.0
                        closed_count = round(max(0.0, float(position["count"]) - remaining_qty), 6)
                    except Exception as exc:
                        logger.warning(
                            "[alpaca_strategy] could not verify exit fill for %s after placing order -- assuming full close: %s",
                            symbol, exc,
                        )
                    if closed_count <= 0:
                        # Nothing actually filled yet -- keep monitoring the
                        # still-real position next cycle rather than booking
                        # a trade that never happened.
                        checks.append({"symbol": symbol, "ok": False, "error": "exit_order_did_not_fill"})
                        continue
                elif "take_profit" in reason:
                    current_price = position["take_profit_price"]
                elif "stop_loss" in reason:
                    current_price = position["stop_loss_price"]

            gross = round((current_price - float(position["entry_price"])) * closed_count, 6)
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
                    "symbol": symbol, "entry_price": position["entry_price"], "exit_price": current_price,
                    "count": closed_count, "realized_pnl_usd": gross, "reason": reason,
                    "dry_run": effective_dry_run,
                    # Entry-time context copied from the position -- see
                    # scan_and_enter's own comment on why.
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
                        {**p, "count": remaining_qty} if p["symbol"] == symbol else p
                        for p in (state.get("positions") or [])
                    ]
                else:
                    state["positions"] = [p for p in (state.get("positions") or []) if p["symbol"] != symbol]
                _save_state(state, push_durable=True)
            remaining_symbols.discard(symbol)
            closed.append(trade)
            try:
                threads_post.post_trade_exit(
                    ticker=symbol, side="long", entry_price=float(position["entry_price"]), exit_price=current_price,
                    pnl_usd=gross, reason=reason, dry_run=trade["dry_run"], market="stocks",
                )
            except Exception:
                logger.warning("[alpaca_strategy] Threads post for %s exit failed", symbol, exc_info=True)
            try:
                from data import chart_snapshot
                one_min_df = fetch_recent_minute_bars(symbol)
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
                    ticker=symbol, market="stocks", candles=_candles_as_dicts(one_min_df),
                    side="long", entry_price=float(position["entry_price"]), exit_price=current_price,
                    entry_index=_index_for_ts(one_min_df, opened_at), exit_index=_index_for_ts(one_min_df, closed_at),
                    pnl_usd=gross, dry_run=trade["dry_run"], indicators=indicators,
                )
            except Exception:
                logger.warning("[alpaca_strategy] Threads exit chart post for %s failed", symbol, exc_info=True)
        except Exception as exc:
            logger.warning("[alpaca_strategy] could not process position for %s -- leaving untouched this cycle: %s", symbol, exc)
            checks.append({"symbol": symbol, "ok": False, "error": str(exc)})

    if closed:
        _maybe_run_batch_trade_analysis()

    return {"action": "closed" if closed else "no_change", "closed": closed, "checks": checks}
