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

Pure analysis over data already collected -- no network calls, no state
mutation. trade_log is exactly the list perps_strategy.py's own
manage_open_positions() builds and persists; this module makes no
assumption beyond that shape.
"""
from __future__ import annotations

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
