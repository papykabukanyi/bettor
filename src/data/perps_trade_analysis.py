"""Post-trade analysis for perps -- turns trade_log's own real history into
structured win/loss diagnostics: which exit reasons, confidence levels,
volatility regimes, tickers, and hold times actually produce wins vs losses
on THIS account's real trading, not just backtested assumptions.

Two downstream consumers depend on this:
  - perps_model.train_model()'s outcome-aware sample weighting (see its own
    docstring) -- the model gets extra signal from the bot's own real wins
    and losses, not just abstract next-minute price direction.
  - app_kalshi.py's scheduled analysis+tuning job, which posts a summary to
    Threads and can nudge MODEL_CONFIDENCE_MIN via a durable-state override
    (perps_strategy.apply_confidence_threshold_override) when the evidence
    is strong enough -- see recommend_confidence_threshold below.

A second, finer-grained layer below (build_trade_snapshot/
analyze_recent_trade_batch) studies the most recent BATCH_SIZE closed
trades every time that many new ones land (see perps_strategy.py's own
manage_open_positions -> _maybe_run_batch_trade_analysis), not just once a
day: for each trade, real 1-minute OHLC candles spanning its own
opened_at -> closed_at window (when still within the live candle cache's
lookback -- older trades outside it just skip the price-derived fields
rather than failing the whole batch) give a concrete, evidence-based
answer to "could this win have captured more?" (max favorable excursion
vs. what was actually realized) and "was this loss premature?" (did price
drift back in our favor shortly after the stop-out). Deliberately does NOT
auto-write new indicators or strategy code from this -- that's a human (or
a future, deliberate feature-engineering pass) call, same posture as
_build_insights above. Where a genuinely reviewed, evidence-gated lever
already exists (recommend_confidence_threshold), the batch can pull it
sooner than the once-a-day job would.

Pure analysis over data already collected -- no network calls, no state
mutation. trade_log is exactly the list perps_strategy.py's own
manage_open_positions() builds and persists; this module makes no
assumption beyond that shape.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Any

logger = logging.getLogger(__name__)

# A bucket's win rate/avg P&L isn't reported (or acted on) unless at least
# this many REAL trades back it -- a 2-trade "100% win rate" is noise, not
# evidence, and this is a real-money account.
MIN_BUCKET_TRADES = 5

_EXIT_REASON_PREFIXES = ("take_profit", "stop_loss", "max_hold_time", "quick_profit", "volatility_quick_profit")
_CONFIDENCE_BUCKET_EDGES = [0.5, 0.55, 0.6, 0.65, 0.7, 1.01]
_HOLD_MINUTES_BUCKETS = [(0, 5, "0-5min"), (5, 15, "5-15min"), (15, 30, "15-30min"), (30, float("inf"), "30min+")]


def _is_win(trade: dict[str, Any]) -> bool:
    return float(trade.get("realized_pnl_usd") or 0.0) > 0


def _bucket_stats(trades: list[dict[str, Any]]) -> dict[str, Any]:
    if not trades:
        return {"trades": 0, "wins": 0, "losses": 0, "win_rate": None, "total_pnl_usd": 0.0, "avg_pnl_usd": None}
    wins = sum(1 for t in trades if _is_win(t))
    total_pnl = round(sum(float(t.get("realized_pnl_usd") or 0.0) for t in trades), 6)
    return {
        "trades": len(trades), "wins": wins, "losses": len(trades) - wins,
        "win_rate": round(wins / len(trades), 4),
        "total_pnl_usd": total_pnl, "avg_pnl_usd": round(total_pnl / len(trades), 6),
    }


def _exit_reason_bucket(reason: str | None) -> str:
    reason = reason or ""
    for prefix in _EXIT_REASON_PREFIXES:
        if reason.startswith(prefix):
            return prefix
    return "other"


def _confidence_bucket_label(score: float | None) -> str | None:
    if score is None:
        return None
    for i in range(len(_CONFIDENCE_BUCKET_EDGES) - 1):
        lo, hi = _CONFIDENCE_BUCKET_EDGES[i], _CONFIDENCE_BUCKET_EDGES[i + 1]
        if lo <= score < hi:
            return f"{lo:.2f}-{min(hi, 1.0):.2f}"
    return None


def _hold_minutes_bucket_label(minutes: float | None) -> str | None:
    if minutes is None:
        return None
    for lo, hi, label in _HOLD_MINUTES_BUCKETS:
        if lo <= minutes < hi:
            return label
    return _HOLD_MINUTES_BUCKETS[-1][2]


def _group_by(trades: list[dict[str, Any]], key_fn) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for t in trades:
        key = key_fn(t)
        if key is None:
            continue
        groups.setdefault(key, []).append(t)
    return {k: _bucket_stats(v) for k, v in groups.items()}


def _build_insights(
    overall: dict[str, Any], by_exit_reason: dict[str, dict[str, Any]], by_confidence: dict[str, dict[str, Any]],
) -> list[str]:
    """Human-readable, evidence-gated observations -- every insight names
    its own sample size so it's clear how much to trust it. Deliberately
    does NOT try to invent new indicators/features on its own (an
    open-ended research problem, not something safe to claim works
    reliably) -- surfaces real correlations a human (or a future,
    deliberate feature-engineering pass) can act on instead."""
    insights: list[str] = []
    if overall.get("trades") is None or overall["trades"] < MIN_BUCKET_TRADES:
        return insights

    stop_loss = by_exit_reason.get("stop_loss")
    if stop_loss and stop_loss["trades"] >= MIN_BUCKET_TRADES:
        insights.append(
            f"{stop_loss['trades']} stop_loss exits, avg ${stop_loss['avg_pnl_usd']:.4f}/trade "
            f"(${stop_loss['total_pnl_usd']:.2f} total)."
        )
    max_hold = by_exit_reason.get("max_hold_time")
    if max_hold and max_hold["trades"] >= MIN_BUCKET_TRADES:
        insights.append(
            f"{max_hold['trades']} max_hold_time exits, win rate {max_hold['win_rate']:.0%} -- "
            f"entries that never found a clean move either way before time ran out."
        )
    take_profit = by_exit_reason.get("take_profit")
    if take_profit and take_profit["trades"] >= MIN_BUCKET_TRADES:
        insights.append(f"{take_profit['trades']} take_profit exits, avg ${take_profit['avg_pnl_usd']:.4f}/trade.")

    confidence_points = sorted(
        ((k, v) for k, v in by_confidence.items() if v["trades"] >= MIN_BUCKET_TRADES), key=lambda kv: kv[0],
    )
    if len(confidence_points) >= 2:
        lowest, highest = confidence_points[0], confidence_points[-1]
        if highest[1]["win_rate"] > lowest[1]["win_rate"]:
            insights.append(
                f"Higher-confidence entries ({highest[0]}) win {highest[1]['win_rate']:.0%} vs "
                f"{lowest[0]}'s {lowest[1]['win_rate']:.0%} -- confidence score is well-calibrated right now."
            )
        elif highest[1]["win_rate"] < lowest[1]["win_rate"]:
            insights.append(
                f"Higher-confidence entries ({highest[0]}) win only {highest[1]['win_rate']:.0%} vs "
                f"{lowest[0]}'s {lowest[1]['win_rate']:.0%} -- confidence score is NOT reliably predictive right now."
            )

    return insights


def analyze_trade_history(trade_log: list[dict[str, Any]] | None, *, include_dry_run: bool = False) -> dict[str, Any]:
    """Real, structured win/loss diagnostics over trade_log. Defaults to
    REAL (non-dry-run) trades only -- dry-run fills don't reflect real
    market slippage/fees and would distort the picture of how the account
    is actually performing."""
    trade_log = trade_log or []
    trades = [t for t in trade_log if include_dry_run or not t.get("dry_run")]
    overall = _bucket_stats(trades)
    if not trades:
        return {"ok": True, "trades_analyzed": 0, "overall": overall, "insights": []}

    by_exit_reason = _group_by(trades, lambda t: _exit_reason_bucket(t.get("reason")))
    by_confidence_bucket = _group_by(trades, lambda t: _confidence_bucket_label(t.get("entry_score")))
    by_ticker = _group_by(trades, lambda t: t.get("ticker"))
    by_side = _group_by(trades, lambda t: t.get("side"))
    by_hold_minutes_bucket = _group_by(trades, lambda t: _hold_minutes_bucket_label(t.get("hold_minutes")))

    return {
        "ok": True, "trades_analyzed": len(trades), "overall": overall,
        "by_exit_reason": by_exit_reason, "by_confidence_bucket": by_confidence_bucket,
        "by_ticker": by_ticker, "by_side": by_side, "by_hold_minutes_bucket": by_hold_minutes_bucket,
        "insights": _build_insights(overall, by_exit_reason, by_confidence_bucket),
    }


# Evidence-gated confidence-threshold tuning -- same "only apply what the
# evidence actually shows, never a fixed a-priori rule" discipline the
# codebase's own backtest-sweep-informs-config pattern already uses
# elsewhere (e.g. alpaca_crypto_backtest.py's own MODEL_CONFIDENCE_MIN
# tune, applied only after a real sweep showed it helped). Bounded to a
# small step per call -- gradual, reviewable movement on a real-money
# account, never one large jump based on a handful of trades.
CONFIDENCE_TUNING_MIN_TRADES = 15
CONFIDENCE_TUNING_CANDIDATE_STEPS = (0.02, 0.04, 0.06, 0.08)
CONFIDENCE_TUNING_MAX_STEP = 0.05


def recommend_confidence_threshold(trade_log: list[dict[str, Any]] | None, *, current_threshold: float) -> dict[str, Any]:
    """Does real trade history show that a HIGHER confidence floor would
    have produced a meaningfully better outcome -- both a better average
    P&L AND an equal-or-better win rate, not just one metric skewed by a
    single large win -- with enough real trades behind the comparison to
    trust it? Returns should_apply=False whenever the evidence is thin or
    doesn't clearly favor moving."""
    trade_log = trade_log or []
    trades = [t for t in trade_log if not t.get("dry_run") and t.get("entry_score") is not None]
    baseline = [t for t in trades if float(t["entry_score"]) >= current_threshold]
    baseline_stats = _bucket_stats(baseline)
    if baseline_stats["trades"] < CONFIDENCE_TUNING_MIN_TRADES:
        return {
            "ok": True, "should_apply": False, "reason": "insufficient_trade_history",
            "current_threshold": current_threshold, "trades_at_current": baseline_stats["trades"],
        }

    best_candidate: dict[str, Any] | None = None
    for step in CONFIDENCE_TUNING_CANDIDATE_STEPS:
        candidate_threshold = round(current_threshold + step, 4)
        cohort = [t for t in trades if float(t["entry_score"]) >= candidate_threshold]
        stats = _bucket_stats(cohort)
        if stats["trades"] < CONFIDENCE_TUNING_MIN_TRADES:
            continue
        improves_pnl = stats["avg_pnl_usd"] is not None and stats["avg_pnl_usd"] > baseline_stats["avg_pnl_usd"]
        improves_win_rate = stats["win_rate"] is not None and stats["win_rate"] >= baseline_stats["win_rate"]
        if improves_pnl and improves_win_rate:
            if best_candidate is None or stats["avg_pnl_usd"] > best_candidate["stats"]["avg_pnl_usd"]:
                best_candidate = {"threshold": candidate_threshold, "stats": stats}

    if best_candidate is None:
        return {
            "ok": True, "should_apply": False, "reason": "no_meaningfully_better_threshold",
            "current_threshold": current_threshold, "baseline": baseline_stats,
        }

    new_threshold = min(best_candidate["threshold"], round(current_threshold + CONFIDENCE_TUNING_MAX_STEP, 4))
    return {
        "ok": True, "should_apply": True, "current_threshold": current_threshold,
        "recommended_threshold": new_threshold, "baseline": baseline_stats, "candidate": best_candidate["stats"],
    }


# Evidence-gated chart-study (crypto_correlation.py) tuning -- same
# discipline as CONFIDENCE_TUNING_* above, applied to a different question:
# not "should the confidence bar move", but "does real trade history show
# this whole extra layer is actually helping, and if so, by how much should
# it be trusted." Bounded per call (CORRELATION_TUNING_MAX_STEP), same
# "gradual, reviewable movement on a real-money account" reasoning.
CORRELATION_TUNING_MIN_TRADES = 15
# A side-adjusted correlation reading below this is closer to noise than a
# real "the study actually had an opinion here" reading -- mirrors
# crypto_correlation.py's own peer-confirmation module's bar for what
# counts as a real signal, not a fitted/backtested value.
CORRELATION_TUNING_AGREEMENT_THRESHOLD = 0.15
CORRELATION_TUNING_MAX_STEP = 0.03
CORRELATION_TUNING_MAX_ADJUSTMENT_CEILING = 0.15  # never let evidence alone drive this arbitrarily high


def recommend_correlation_study_weight(
    trade_log: list[dict[str, Any]] | None, *, current_enabled: bool, current_max_adjustment: float,
) -> dict[str, Any]:
    """Does real closed-trade history show the chart-study layer (see
    crypto_correlation.py's own module docstring) is actually helping --
    trades where it agreed with the side actually taken outperforming
    trades where it didn't (or was neutral), with enough real trades on
    BOTH sides to trust the comparison? Every closed trade already carries
    its own entry_correlation_score (see evaluate_candidate's own comment --
    captured for observability regardless of whether the study was even ON
    at entry time), so this works from day one, before the study has ever
    influenced a single live decision, and keeps working to catch drift
    after it has.

    Three possible outcomes, same "only move on real evidence" posture as
    recommend_confidence_threshold above:
      - should_apply=False, reason="insufficient_trade_history": not
        enough real trades in one or both buckets yet.
      - should_apply=True, action="enable"/"increase_weight": the
        "agreed" bucket clearly outperforms (both better avg P&L AND
        equal-or-better win rate) -- turn it on, or trust it a bit more if
        already on.
      - should_apply=True, action="disable": the "agreed" bucket clearly
        UNDERperforms (both worse avg P&L and worse win rate) while
        currently enabled -- real evidence it's actively hurting, not just
        unproven yet."""
    trade_log = trade_log or []
    trades = [t for t in trade_log if not t.get("dry_run") and t.get("entry_correlation_score") is not None]

    def _agreement(t: dict[str, Any]) -> float:
        # Bullish-signed at capture time (see evaluate_candidate) -- flip
        # for a short exactly like decide_exit's own favorable_correlation
        # convention, so "agreement" always means "favored the side this
        # trade actually took."
        score = float(t["entry_correlation_score"])
        return score if t.get("side", "long") == "long" else -score

    agreed = [t for t in trades if _agreement(t) >= CORRELATION_TUNING_AGREEMENT_THRESHOLD]
    baseline = [t for t in trades if _agreement(t) < CORRELATION_TUNING_AGREEMENT_THRESHOLD]
    agreed_stats = _bucket_stats(agreed)
    baseline_stats = _bucket_stats(baseline)

    if agreed_stats["trades"] < CORRELATION_TUNING_MIN_TRADES or baseline_stats["trades"] < CORRELATION_TUNING_MIN_TRADES:
        return {
            "ok": True, "should_apply": False, "reason": "insufficient_trade_history",
            "current_enabled": current_enabled, "current_max_adjustment": current_max_adjustment,
            "agreed": agreed_stats, "baseline": baseline_stats,
        }

    improves_pnl = agreed_stats["avg_pnl_usd"] > baseline_stats["avg_pnl_usd"]
    improves_win_rate = agreed_stats["win_rate"] >= baseline_stats["win_rate"]
    if improves_pnl and improves_win_rate:
        if not current_enabled:
            return {
                "ok": True, "should_apply": True, "action": "enable",
                "recommended_enabled": True, "recommended_max_adjustment": current_max_adjustment,
                "agreed": agreed_stats, "baseline": baseline_stats,
            }
        new_max_adjustment = min(
            round(current_max_adjustment + CORRELATION_TUNING_MAX_STEP, 4), CORRELATION_TUNING_MAX_ADJUSTMENT_CEILING,
        )
        if new_max_adjustment <= current_max_adjustment:
            return {
                "ok": True, "should_apply": False, "reason": "already_at_ceiling",
                "current_enabled": current_enabled, "current_max_adjustment": current_max_adjustment,
                "agreed": agreed_stats, "baseline": baseline_stats,
            }
        return {
            "ok": True, "should_apply": True, "action": "increase_weight",
            "recommended_enabled": True, "recommended_max_adjustment": new_max_adjustment,
            "agreed": agreed_stats, "baseline": baseline_stats,
        }

    worsens_pnl = agreed_stats["avg_pnl_usd"] < baseline_stats["avg_pnl_usd"]
    worsens_win_rate = agreed_stats["win_rate"] < baseline_stats["win_rate"]
    if worsens_pnl and worsens_win_rate:
        if current_enabled:
            return {
                "ok": True, "should_apply": True, "action": "disable",
                "recommended_enabled": False, "recommended_max_adjustment": current_max_adjustment,
                "agreed": agreed_stats, "baseline": baseline_stats,
            }
        return {
            "ok": True, "should_apply": False, "reason": "disabled_and_evidence_confirms_that",
            "current_enabled": current_enabled, "current_max_adjustment": current_max_adjustment,
            "agreed": agreed_stats, "baseline": baseline_stats,
        }

    return {
        "ok": True, "should_apply": False, "reason": "no_clear_signal",
        "current_enabled": current_enabled, "current_max_adjustment": current_max_adjustment,
        "agreed": agreed_stats, "baseline": baseline_stats,
    }


# ---------------------------------------------------------------------------
# Scale-in / partial-exit / conviction-sizing trial evaluator. Unlike
# correlation_score, these 3 are simple global on/off toggles with no
# natural continuous per-trade reading to split on -- there's no "agreed
# vs disagreed" bucket to compare until something has actually been
# enabled for a real stretch. This runs a genuine live trial instead:
# recommends turning a feature ON once there's enough overall real trade
# history to justify trying it, then -- once enough trades have closed
# WITH it on -- compares that cohort's real performance against the
# trades that closed BEFORE it was enabled, only keeping/re-recommending
# it if the evidence is unambiguous.
# ---------------------------------------------------------------------------

POSITION_MANAGEMENT_TRIAL_MIN_TRADES = 20  # needed in EACH bucket before trusting a with-vs-without comparison
POSITION_MANAGEMENT_MIN_HISTORY_TO_START = {
    "partial_exit": 20,      # pure risk-reduction (locks in profit, tightens the stop on what's left) -- lowest bar
    "conviction_sizing": 30,  # reallocates size within a bounded 0.7x-1.5x range among trades that already qualify
    # "scale_in" has NO entry here on purpose: it's the one feature that
    # adds genuine NEW capital to an already-open position, a materially
    # different risk than resizing or timing an exit that was happening
    # anyway. This tuner will confirm/disable a scale_in trial once a
    # human has started one at least once, but will never auto-START it.
}


def recommend_position_management_trial(
    trade_log: list[dict[str, Any]] | None, *, feature: str, current_enabled: bool,
) -> dict[str, Any]:
    """`feature` is one of "scale_in"/"partial_exit"/"conviction_sizing" --
    see perps_strategy.apply_position_management_override. Reads
    entry_{feature}_enabled off each real trade (see scan_and_enter's own
    entry_context) to split closed trades into "entered while this was ON"
    vs "entered while this was OFF", comparing avg P&L and win rate
    between them once both sides have enough real trades.

    Four possible outcomes:
      - should_apply=True, action="start_trial": nothing has ever been
        enabled yet, but there's enough overall real trade history to
        justify trying it (see POSITION_MANAGEMENT_MIN_HISTORY_TO_START) --
        never offered for "scale_in" (see that constant's own comment).
      - should_apply=False, reason="insufficient_trade_history": not
        enough real trades exist yet in one or both buckets to trust a
        comparison (this covers "no trial started and not enough overall
        history yet" too).
      - should_apply=False, reason="confirmed_enabled" /
        "evidence_favors_enabling_but_currently_off": the "with" cohort
        clearly wins on BOTH avg P&L and win rate. Deliberately NEVER
        auto-(re)enables here, even in the second case -- only a fresh
        start_trial (from a clean OFF state) or a human turns something
        on; this just reports what the evidence already shows.
      - should_apply=True, action="disable": the "with" cohort clearly
        LOSES on both avg P&L and win rate while currently enabled -- real
        evidence it's hurting, not just unproven yet (same bar
        recommend_correlation_study_weight's own disable path uses)."""
    trade_log = trade_log or []
    key = f"entry_{feature}_enabled"
    real_trades = [t for t in trade_log if not t.get("dry_run") and t.get(key) is not None]
    with_feature = [t for t in real_trades if t.get(key) is True]
    without_feature = [t for t in real_trades if t.get(key) is False]
    with_stats = _bucket_stats(with_feature)
    without_stats = _bucket_stats(without_feature)

    if not with_feature:
        min_history = POSITION_MANAGEMENT_MIN_HISTORY_TO_START.get(feature)
        if not current_enabled and min_history is not None and without_stats["trades"] >= min_history:
            return {
                "ok": True, "should_apply": True, "action": "start_trial",
                "recommended_enabled": True, "with_feature": with_stats, "without_feature": without_stats,
            }
        return {
            "ok": True, "should_apply": False, "reason": "insufficient_trade_history",
            "with_feature": with_stats, "without_feature": without_stats,
        }

    if with_stats["trades"] < POSITION_MANAGEMENT_TRIAL_MIN_TRADES or without_stats["trades"] < POSITION_MANAGEMENT_TRIAL_MIN_TRADES:
        return {
            "ok": True, "should_apply": False, "reason": "insufficient_trade_history",
            "with_feature": with_stats, "without_feature": without_stats,
        }

    improves_pnl = with_stats["avg_pnl_usd"] > without_stats["avg_pnl_usd"]
    improves_win_rate = with_stats["win_rate"] >= without_stats["win_rate"]
    if improves_pnl and improves_win_rate:
        reason = "confirmed_enabled" if current_enabled else "evidence_favors_enabling_but_currently_off"
        return {"ok": True, "should_apply": False, "reason": reason, "with_feature": with_stats, "without_feature": without_stats}

    worsens_pnl = with_stats["avg_pnl_usd"] < without_stats["avg_pnl_usd"]
    worsens_win_rate = with_stats["win_rate"] < without_stats["win_rate"]
    if worsens_pnl and worsens_win_rate and current_enabled:
        return {
            "ok": True, "should_apply": True, "action": "disable",
            "recommended_enabled": False, "with_feature": with_stats, "without_feature": without_stats,
        }

    return {"ok": True, "should_apply": False, "reason": "no_clear_signal", "with_feature": with_stats, "without_feature": without_stats}


def format_analysis_summary_text(analysis: dict[str, Any], *, tuning: dict[str, Any] | None = None) -> str:
    """Human-readable digest for the Threads post -- what the account's
    real trading history shows, not a raw data dump."""
    overall = analysis.get("overall") or {}
    if not analysis.get("trades_analyzed"):
        return "Perps trade analysis: not enough closed real trades yet to draw conclusions."

    lines = [
        f"Perps trade review ({analysis['trades_analyzed']} real trades):",
        f"Win rate {overall['win_rate']:.0%} | Total P&L ${overall['total_pnl_usd']:.2f} | "
        f"Avg ${overall['avg_pnl_usd']:.4f}/trade",
    ]
    lines.extend(analysis.get("insights") or [])
    if tuning and tuning.get("should_apply"):
        lines.append(
            f"Confidence floor raised {tuning['current_threshold']:.2f} -> {tuning['recommended_threshold']:.2f} "
            f"based on this evidence."
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Per-trade / small-batch snapshot study -- "every recent BATCH_SIZE trades,
# look at what actually happened and learn from it," distinct from
# analyze_trade_history's longer-horizon aggregate buckets above.
# ---------------------------------------------------------------------------

BATCH_SIZE = 5
# How far past a trade's own close to look for "did price keep moving our
# way after we left" -- 10 one-minute candles is long enough to catch an
# immediate reversal without wandering into unrelated later price action.
POST_EXIT_DRIFT_CANDLES = 10
# A post-exit reversal/drift smaller than this is noise, not a signal that
# the exit was premature -- keeps _build_batch_recommendations from firing
# on sub-tick wiggle.
POST_EXIT_DRIFT_MEANINGFUL_PCT = 0.003
# A win that captured less than this fraction of its own max favorable
# excursion is flagged as "left profit on the table."
LOW_CAPTURE_RATIO = 0.5


def _parse_iso(ts: str | None) -> dt.datetime | None:
    if not ts:
        return None
    try:
        return dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _price_excursion(trade: dict[str, Any], candles: list[dict[str, Any]]) -> dict[str, Any]:
    """Real max-favorable/max-adverse excursion during the trade's own
    opened_at -> closed_at window, plus how price drifted in the
    POST_EXIT_DRIFT_CANDLES right after close -- all from real 1-minute
    OHLC, not estimated. Returns {} (not partial/fabricated values) if the
    trade's window isn't covered by `candles` at all (e.g. older than the
    live candle cache's lookback) or any required field is missing."""
    side = trade.get("side", "long")
    entry_price = trade.get("entry_price")
    opened = _parse_iso(trade.get("opened_at"))
    closed = _parse_iso(trade.get("closed_at"))
    if not candles or entry_price in (None, 0) or opened is None or closed is None:
        return {}
    entry_price = float(entry_price)
    opened_ts, closed_ts = opened.timestamp(), closed.timestamp()
    ordered = sorted(candles, key=lambda c: c.get("ts", 0))
    window = [c for c in ordered if opened_ts <= c.get("ts", 0) <= closed_ts]
    if not window:
        return {}

    highs = [float(c["high"]) for c in window if c.get("high") is not None]
    lows = [float(c["low"]) for c in window if c.get("low") is not None]
    if not highs or not lows:
        return {}
    if side == "short":
        mfe_usd = entry_price - min(lows)
        mae_usd = max(highs) - entry_price
    else:
        mfe_usd = max(highs) - entry_price
        mae_usd = entry_price - min(lows)

    result: dict[str, Any] = {"mfe_usd": round(mfe_usd, 6), "mae_usd": round(mae_usd, 6)}

    post_window = [c for c in ordered if c.get("ts", 0) > closed_ts][:POST_EXIT_DRIFT_CANDLES]
    exit_price = trade.get("exit_price")
    if post_window and exit_price:
        last_close = float(post_window[-1]["close"])
        exit_price = float(exit_price)
        raw_drift = (last_close - exit_price) / exit_price
        # Sign-flipped for a short so "positive" always means "price kept
        # moving the direction that would have helped this trade" for
        # either side, not just literally up.
        result["post_exit_drift_pct"] = round(raw_drift if side != "short" else -raw_drift, 6)

    return result


def build_trade_snapshot(trade: dict[str, Any], *, candles: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """One closed trade's "full snap" -- outcome, the entry-time context
    already captured on the trade record (see perps_strategy.py's
    manage_open_positions), and (when candles cover its window) real
    price-action diagnostics: how much favorable move was actually
    available (mfe_usd) vs. realized, how far the stop was actually
    tested (mae_usd), and whether price drifted back in our favor shortly
    after exit. `lesson` turns that into one human-readable takeaway."""
    pnl = float(trade.get("realized_pnl_usd") or 0.0)
    outcome = "win" if pnl > 0 else "loss" if pnl < 0 else "flat"
    snapshot: dict[str, Any] = {
        "ticker": trade.get("ticker"), "side": trade.get("side"), "outcome": outcome,
        "pnl_usd": round(pnl, 6), "reason": trade.get("reason"), "hold_minutes": trade.get("hold_minutes"),
        "entry_score": trade.get("entry_score"), "entry_probability_up": trade.get("entry_probability_up"),
    }
    snapshot.update(_price_excursion(trade, candles or []))
    if snapshot.get("mfe_usd") and snapshot["mfe_usd"] > 0:
        snapshot["capture_ratio"] = round(pnl / snapshot["mfe_usd"], 4)
    snapshot["lesson"] = _lesson_for(snapshot)
    return snapshot


def _lesson_for(s: dict[str, Any]) -> str:
    ticker = s.get("ticker") or "?"
    pnl = s["pnl_usd"]
    if s["outcome"] == "win":
        capture = s.get("capture_ratio")
        if capture is not None and capture < LOW_CAPTURE_RATIO:
            return (
                f"{ticker}: WIN ${pnl:.2f} but captured only {capture:.0%} of the available favorable move "
                f"(MFE ${s['mfe_usd']:.2f}) -- exit may have been early or the take-profit too tight."
            )
        if capture is not None:
            return f"{ticker}: WIN ${pnl:.2f}, captured {capture:.0%} of the available move -- well-timed exit."
        return f"{ticker}: WIN ${pnl:.2f}."
    if s["outcome"] == "loss":
        drift = s.get("post_exit_drift_pct")
        if drift is not None and drift > POST_EXIT_DRIFT_MEANINGFUL_PCT:
            return (
                f"{ticker}: LOSS ${pnl:.2f} ({s.get('reason')}) -- price moved back in our favor by "
                f"{drift:.2%} shortly after exit, the stop may be too tight for current volatility."
            )
        confidence = s.get("entry_score")
        if confidence is not None and confidence >= 0.65:
            return (
                f"{ticker}: LOSS ${pnl:.2f} despite a high entry confidence ({confidence:.0%}) -- "
                f"worth flagging for the next retrain, possible regime the model hasn't adapted to."
            )
        return f"{ticker}: LOSS ${pnl:.2f} ({s.get('reason')})."
    return f"{ticker}: closed flat."


def _build_batch_recommendations(snapshots: list[dict[str, Any]]) -> list[str]:
    """Pattern-level recommendations across the batch -- deliberately
    phrased as things worth a dedicated backtest/feature-engineering look,
    never as an auto-applied code change (see this module's own docstring
    on why that stays a human/deliberate-pass decision)."""
    recs: list[str] = []
    losses = [s for s in snapshots if s["outcome"] == "loss"]
    wins = [s for s in snapshots if s["outcome"] == "win"]

    reversal_losses = [s for s in losses if (s.get("post_exit_drift_pct") or 0) > POST_EXIT_DRIFT_MEANINGFUL_PCT]
    if len(reversal_losses) >= 2:
        recs.append(
            f"{len(reversal_losses)} of the last {len(losses)} losses reversed favorably shortly after exit -- "
            f"consider a wider or volatility-scaled stop-loss for choppy conditions."
        )

    high_conf_losses = [s for s in losses if (s.get("entry_score") or 0) >= 0.65]
    if len(high_conf_losses) >= 2:
        recs.append(
            f"{len(high_conf_losses)} of the last {len(losses)} losses had high entry confidence (>=65%) -- "
            f"flagging for the next retrain, may indicate a regime the model hasn't adapted to yet."
        )

    low_capture_wins = [s for s in wins if (s.get("capture_ratio") or 1.0) < LOW_CAPTURE_RATIO]
    if len(low_capture_wins) >= 2:
        recs.append(
            f"{len(low_capture_wins)} of the last {len(wins)} wins captured under half of the available "
            f"favorable move -- consider a trailing take-profit or a wider initial target."
        )

    exit_reason_counts: dict[str, int] = {}
    for s in losses:
        key = _exit_reason_bucket(s.get("reason"))
        exit_reason_counts[key] = exit_reason_counts.get(key, 0) + 1
    if exit_reason_counts:
        dominant_reason, dominant_count = max(exit_reason_counts.items(), key=lambda kv: kv[1])
        if dominant_count >= 3:
            recs.append(
                f"{dominant_count} of the last {len(losses)} losses shared exit reason '{dominant_reason}' -- "
                f"a recurring pattern worth a dedicated backtest sweep to see if a new indicator/filter would "
                f"catch it earlier."
            )

    return recs


def analyze_recent_trade_batch(
    trade_log: list[dict[str, Any]] | None, *, candles_by_ticker: dict[str, list[dict[str, Any]]] | None = None,
    batch_size: int = BATCH_SIZE, include_dry_run: bool = False,
) -> dict[str, Any]:
    """Studies the most recent `batch_size` closed real trades (oldest to
    newest) -- see build_trade_snapshot for what each one gets. Intended to
    run every time that many NEW trades close (see perps_strategy.py's
    _maybe_run_batch_trade_analysis), giving faster feedback than the
    once-daily analyze_trade_history above. `candles_by_ticker` should have
    one entry per ticker actually appearing in the batch (recent 1-minute
    OHLC covering at least back to the oldest trade's opened_at) -- a
    missing/empty entry just means that trade's snapshot skips the
    price-derived fields, never a hard failure."""
    trade_log = trade_log or []
    trades = [t for t in trade_log if include_dry_run or not t.get("dry_run")]
    if not trades:
        return {"ok": True, "trades_analyzed": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0, "snapshots": [], "recommendations": []}

    recent = trades[-batch_size:]
    candles_by_ticker = candles_by_ticker or {}
    snapshots = [
        build_trade_snapshot(t, candles=candles_by_ticker.get(t.get("ticker"))) for t in recent
    ]
    wins = sum(1 for s in snapshots if s["outcome"] == "win")
    losses = sum(1 for s in snapshots if s["outcome"] == "loss")
    total_pnl = round(sum(s["pnl_usd"] for s in snapshots), 6)
    return {
        "ok": True, "trades_analyzed": len(snapshots), "wins": wins, "losses": losses,
        "total_pnl_usd": total_pnl, "snapshots": snapshots,
        "recommendations": _build_batch_recommendations(snapshots),
    }


def format_batch_snapshot_text(batch: dict[str, Any], *, market: str = "perps") -> str:
    """Human-readable Threads digest for one recent-trade-batch study --
    every trade's own one-line lesson, plus any pattern-level
    recommendations across the batch."""
    label = market.capitalize()
    if not batch.get("trades_analyzed"):
        return f"{label} trade snapshot: no closed real trades yet."
    lines = [
        f"{label} trade snapshot (last {batch['trades_analyzed']}): "
        f"{batch['wins']}W/{batch['losses']}L, total ${batch['total_pnl_usd']:+.2f}",
    ]
    lines.extend(f"- {s['lesson']}" for s in batch["snapshots"])
    lines.extend(batch.get("recommendations") or [])
    return "\n".join(lines)
