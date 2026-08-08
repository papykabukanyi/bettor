"""Post-trade analysis for Alpaca crypto -- mirrors perps_trade_analysis.py's
own structure and reasoning (see its module docstring for the full design
rationale); this file adapts field names to crypto's own trade_log shape
("symbol" instead of "ticker", always side="long" -- crypto is long-only,
see alpaca_crypto_strategy.py's decide_exit) rather than sharing code,
matching this codebase's established pattern of independent per-market
modules instead of a shared base class.

Studies the most recent BATCH_SIZE closed trades every time that many new
ones land (see alpaca_crypto_strategy.py's manage_open_positions ->
_maybe_run_batch_trade_analysis): for each trade, real 1-minute OHLC bars
spanning its own opened_at -> closed_at window give a concrete, evidence-
based answer to "could this win have captured more?" (max favorable
excursion vs. what was actually realized) and "was this loss premature?"
(did price drift back in our favor shortly after the exit). Deliberately
does NOT auto-write new indicators or strategy code from this -- that's a
human (or a future, deliberate feature-engineering pass) call.

Pure analysis over data already collected -- no network calls, no state
mutation.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Any

logger = logging.getLogger(__name__)

BATCH_SIZE = 5
POST_EXIT_DRIFT_CANDLES = 10
POST_EXIT_DRIFT_MEANINGFUL_PCT = 0.003
LOW_CAPTURE_RATIO = 0.5
_EXIT_REASON_PREFIXES = ("take_profit", "stop_loss", "max_hold_time", "quick_profit", "volatility_quick_profit")


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


# Same evidence-gated confidence-threshold tuning as perps_trade_analysis.py
# (see its own recommend_confidence_threshold docstring for the full
# rationale) -- this module stays pure analysis (recommends, never writes);
# the actual narrow write happens in alpaca_crypto_strategy.apply_confidence_threshold_override,
# called by the caller only when should_apply is True, mirroring perps'
# exact recommend-vs-apply split.
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


def _exit_reason_bucket(reason: str | None) -> str:
    reason = reason or ""
    for prefix in _EXIT_REASON_PREFIXES:
        if reason.startswith(prefix):
            return prefix
    return "other"


def _parse_iso(ts: str | None) -> dt.datetime | None:
    if not ts:
        return None
    try:
        return dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _price_excursion(trade: dict[str, Any], candles: list[dict[str, Any]]) -> dict[str, Any]:
    """Real max-favorable/max-adverse excursion during the trade's own
    opened_at -> closed_at window, plus post-exit drift -- see
    perps_trade_analysis._price_excursion for the full reasoning. Long-only
    here (crypto never shorts -- see alpaca_crypto_strategy.decide_exit)."""
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
    mfe_usd = max(highs) - entry_price
    mae_usd = entry_price - min(lows)
    result: dict[str, Any] = {"mfe_usd": round(mfe_usd, 6), "mae_usd": round(mae_usd, 6)}

    post_window = [c for c in ordered if c.get("ts", 0) > closed_ts][:POST_EXIT_DRIFT_CANDLES]
    exit_price = trade.get("exit_price")
    if post_window and exit_price:
        last_close = float(post_window[-1]["close"])
        exit_price = float(exit_price)
        result["post_exit_drift_pct"] = round((last_close - exit_price) / exit_price, 6)

    return result


def build_trade_snapshot(trade: dict[str, Any], *, candles: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """One closed trade's "full snap" -- see perps_trade_analysis's own
    function of the same name for the full design; adapted for crypto's
    trade_log shape ("symbol" key, always long)."""
    pnl = float(trade.get("realized_pnl_usd") or 0.0)
    outcome = "win" if pnl > 0 else "loss" if pnl < 0 else "flat"
    snapshot: dict[str, Any] = {
        "symbol": trade.get("symbol"), "outcome": outcome, "pnl_usd": round(pnl, 6),
        "reason": trade.get("reason"), "hold_minutes": trade.get("hold_minutes"),
        "entry_probability_up": trade.get("entry_probability_up"),
    }
    snapshot.update(_price_excursion(trade, candles or []))
    if snapshot.get("mfe_usd") and snapshot["mfe_usd"] > 0:
        snapshot["capture_ratio"] = round(pnl / snapshot["mfe_usd"], 4)
    snapshot["lesson"] = _lesson_for(snapshot)
    return snapshot


def _lesson_for(s: dict[str, Any]) -> str:
    symbol = s.get("symbol") or "?"
    pnl = s["pnl_usd"]
    if s["outcome"] == "win":
        capture = s.get("capture_ratio")
        if capture is not None and capture < LOW_CAPTURE_RATIO:
            return (
                f"{symbol}: WIN ${pnl:.2f} but captured only {capture:.0%} of the available favorable move "
                f"(MFE ${s['mfe_usd']:.2f}) -- exit may have been early or the take-profit too tight."
            )
        if capture is not None:
            return f"{symbol}: WIN ${pnl:.2f}, captured {capture:.0%} of the available move -- well-timed exit."
        return f"{symbol}: WIN ${pnl:.2f}."
    if s["outcome"] == "loss":
        drift = s.get("post_exit_drift_pct")
        if drift is not None and drift > POST_EXIT_DRIFT_MEANINGFUL_PCT:
            return (
                f"{symbol}: LOSS ${pnl:.2f} ({s.get('reason')}) -- price moved back in our favor by "
                f"{drift:.2%} shortly after exit, the stop may be too tight for current volatility."
            )
        probability_up = s.get("entry_probability_up")
        if probability_up is not None and probability_up >= 0.65:
            return (
                f"{symbol}: LOSS ${pnl:.2f} despite a high entry model confidence ({probability_up:.0%}) -- "
                f"worth flagging for the next retrain, possible regime the model hasn't adapted to."
            )
        return f"{symbol}: LOSS ${pnl:.2f} ({s.get('reason')})."
    return f"{symbol}: closed flat."


def _build_batch_recommendations(snapshots: list[dict[str, Any]]) -> list[str]:
    recs: list[str] = []
    losses = [s for s in snapshots if s["outcome"] == "loss"]
    wins = [s for s in snapshots if s["outcome"] == "win"]

    reversal_losses = [s for s in losses if (s.get("post_exit_drift_pct") or 0) > POST_EXIT_DRIFT_MEANINGFUL_PCT]
    if len(reversal_losses) >= 2:
        recs.append(
            f"{len(reversal_losses)} of the last {len(losses)} losses reversed favorably shortly after exit -- "
            f"consider a wider or volatility-scaled stop-loss for choppy conditions."
        )

    high_conf_losses = [s for s in losses if (s.get("entry_probability_up") or 0) >= 0.65]
    if len(high_conf_losses) >= 2:
        recs.append(
            f"{len(high_conf_losses)} of the last {len(losses)} losses had high entry model confidence (>=65%) -- "
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
    trade_log: list[dict[str, Any]] | None, *, candles_by_symbol: dict[str, list[dict[str, Any]]] | None = None,
    batch_size: int = BATCH_SIZE, include_dry_run: bool = False,
) -> dict[str, Any]:
    """Studies the most recent `batch_size` closed real trades. See
    perps_trade_analysis.analyze_recent_trade_batch for the full design;
    `candles_by_symbol` should have one entry per symbol actually appearing
    in the batch (recent 1-minute OHLC covering at least back to the oldest
    trade's opened_at) -- a missing/empty entry just skips that trade's
    price-derived fields, never a hard failure."""
    trade_log = trade_log or []
    trades = [t for t in trade_log if include_dry_run or not t.get("dry_run")]
    if not trades:
        return {"ok": True, "trades_analyzed": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0, "snapshots": [], "recommendations": []}

    recent = trades[-batch_size:]
    candles_by_symbol = candles_by_symbol or {}
    snapshots = [
        build_trade_snapshot(t, candles=candles_by_symbol.get(t.get("symbol"))) for t in recent
    ]
    wins = sum(1 for s in snapshots if s["outcome"] == "win")
    losses = sum(1 for s in snapshots if s["outcome"] == "loss")
    total_pnl = round(sum(s["pnl_usd"] for s in snapshots), 6)
    return {
        "ok": True, "trades_analyzed": len(snapshots), "wins": wins, "losses": losses,
        "total_pnl_usd": total_pnl, "snapshots": snapshots,
        "recommendations": _build_batch_recommendations(snapshots),
    }


def format_batch_snapshot_text(batch: dict[str, Any], *, market: str = "crypto") -> str:
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
