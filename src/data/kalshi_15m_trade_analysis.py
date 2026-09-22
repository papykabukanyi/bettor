"""Post-trade analysis for Kalshi's 15-minute event-contract markets --
mirrors alpaca_options_trade_analysis.py's own structure and reasoning
(see its module docstring for the full design rationale), the pattern
every other market here already has (perps_trade_analysis.py,
alpaca_strategy/alpaca_crypto_strategy's own siblings, and options') and
this market -- the newest one -- never had. Built per explicit user
direction: "review the last live trades and make sure the model review
them as a part of the process[,] each time there[']s a loss[,] analyse
the full loss and make sure it learn[s] and don't repeat again."

Two real, deliberate differences from every sibling module, both
structural consequences of kalshi_15m_strategy.py's own much simpler
position lifecycle (see its own module docstring: "no leverage... no
stop-loss/take-profit... a position here has exactly one exit: the
window closes and it settles"):

- No exit_reason bucket, and no post-exit-drift signal on a single
  trade's own "lesson" snapshot -- there is only one exit reason
  (settlement) and no concept of "the market kept moving after exit"
  that means anything once a binary contract has already settled.
- A NEW `side` bucket (yes vs no) no sibling module has, because this
  account just lived through a real, confirmed bug where every "no"
  decision was, for a period, placed as the OPPOSITE real order (see
  kalshi_15m_strategy.scan_and_enter's own docstring/comments on the
  order-side fix). A stark yes/no win-rate gap in trades from BEFORE
  that fix is exactly the kind of evidence this module exists to
  surface; a persistent gap AFTER the fix would be real signal the
  model itself is asymmetric across the two sides, not just a bug
  artifact.

Reuses kalshi_15m_model.py's/kalshi_15m_metals_model.py's own EXISTING
win/loss reweighting during training (`_trade_outcome_sample_weight`,
already live in both) -- that's the silent TRAINING half of "learn from
losses"; this module is the human/AI-readable REPORTING half, plus THREE
evidence-gated LIVE levers every sibling module already has some version
of: recommend_confidence_threshold (confidence floor),
recommend_correlation_study_weight (the chart-study confidence nudge),
and recommend_conviction_sizing_trial (position sizing) -- applied via
kalshi_15m_strategy.apply_confidence_threshold_override/
apply_correlation_study_override/apply_conviction_sizing_override
respectively. None of these four replace each other.

Pure analysis over data already collected -- no network calls, no state
mutation.
"""
from __future__ import annotations

from typing import Any

BATCH_SIZE = 5


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


def _group_by(trades: list[dict[str, Any]], key_fn) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for t in trades:
        key = key_fn(t)
        if key is None:
            continue
        groups.setdefault(key, []).append(t)
    return {k: _bucket_stats(v) for k, v in groups.items()}


# Same evidence-gated confidence-threshold tuning as every sibling
# module's own recommend_confidence_threshold (see e.g.
# alpaca_options_trade_analysis.py's own identical function for the full
# rationale) -- this module stays pure analysis (recommends, never
# writes); the actual narrow write happens in
# kalshi_15m_strategy.apply_confidence_threshold_override, called by the
# caller only when should_apply is True. Keyed on `entry_confidence`
# (already the probability of the SIDE actually chosen -- see
# kalshi_15m_strategy.evaluate_candidate's own docstring -- so it's
# directly comparable across yes AND no trades, unlike raw
# probability_up would be).
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
    trades = [t for t in trade_log if not t.get("dry_run") and t.get("entry_confidence") is not None]
    baseline = [t for t in trades if float(t["entry_confidence"]) >= current_threshold]
    baseline_stats = _bucket_stats(baseline)
    if baseline_stats["trades"] < CONFIDENCE_TUNING_MIN_TRADES:
        return {
            "ok": True, "should_apply": False, "reason": "insufficient_trade_history",
            "current_threshold": current_threshold, "trades_at_current": baseline_stats["trades"],
        }

    best_candidate: dict[str, Any] | None = None
    for step in CONFIDENCE_TUNING_CANDIDATE_STEPS:
        candidate_threshold = round(current_threshold + step, 4)
        cohort = [t for t in trades if float(t["entry_confidence"]) >= candidate_threshold]
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


# Same evidence-gated tuning as perps_trade_analysis.py's own identical
# recommend_correlation_study_weight (see its docstring for the full
# rationale) -- does real closed-trade history show the chart-study layer
# (kalshi_15m_strategy.evaluate_candidate's own correlation_score,
# captured on every trade regardless of whether the study was even ON at
# entry time) is actually helping? Crypto trades only in practice (no
# correlation study exists for metals -- see USE_CORRELATION_STUDY's own
# comment -- so metals trades all carry entry_correlation_score == 0.0
# and land in the "baseline" bucket, never "agreed").
CORRELATION_TUNING_MIN_TRADES = 15
CORRELATION_TUNING_AGREEMENT_THRESHOLD = 0.15
CORRELATION_TUNING_MAX_STEP = 0.03
CORRELATION_TUNING_MAX_ADJUSTMENT_CEILING = 0.15  # never let evidence alone drive this arbitrarily high


def recommend_correlation_study_weight(
    trade_log: list[dict[str, Any]] | None, *, current_enabled: bool, current_max_adjustment: float,
) -> dict[str, Any]:
    """Does real closed-trade history show the chart-study layer is
    actually helping -- trades where it agreed with the side actually
    taken outperforming trades where it didn't (or was neutral), with
    enough real trades on BOTH sides to trust the comparison? Three
    possible outcomes, same "only move on real evidence" posture as
    recommend_confidence_threshold above:
      - should_apply=False, reason="insufficient_trade_history": not
        enough real trades in one or both buckets yet.
      - should_apply=True, action="enable"/"increase_weight": the
        "agreed" bucket clearly outperforms -- turn it on, or trust it a
        bit more if already on.
      - should_apply=True, action="disable": the "agreed" bucket clearly
        UNDERperforms while currently enabled -- real evidence it's
        actively hurting, not just unproven yet."""
    trade_log = trade_log or []
    trades = [t for t in trade_log if not t.get("dry_run") and t.get("entry_correlation_score") is not None]

    def _agreement(t: dict[str, Any]) -> float:
        # Bullish-signed at capture time -- flip for "no", same convention
        # evaluate_candidate's own side_correlation_score uses, so
        # "agreement" always means "favored the side this trade actually
        # took."
        score = float(t["entry_correlation_score"])
        return score if t.get("side", "yes") == "yes" else -score

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


# Same evidence-gated trial as perps_trade_analysis.recommend_position_management_trial
# (see its docstring for the full rationale), scoped down to the ONE
# feature that actually maps onto this market's product -- conviction
# sizing (see kalshi_15m_strategy.USE_CONVICTION_SIZING's own comment on
# why scale-in/partial-exit have no kalshi_15m equivalent at all). A
# single dedicated function rather than perps' generalized feature-name-
# keyed dispatcher: with only one feature in this category, that
# generality would be pure overhead here.
CONVICTION_SIZING_TRIAL_MIN_TRADES = 20  # needed in EACH bucket before trusting a with-vs-without comparison
CONVICTION_SIZING_MIN_HISTORY_TO_START = 30  # real trades (all with it OFF) before even proposing a trial


def recommend_conviction_sizing_trial(trade_log: list[dict[str, Any]] | None, *, current_enabled: bool) -> dict[str, Any]:
    """Reads entry_conviction_sizing_enabled off each real trade (see
    kalshi_15m_strategy.scan_and_enter's own position dict) to split
    closed trades into "entered while this was ON" vs "entered while this
    was OFF", comparing avg P&L and win rate between them once both sides
    have enough real trades. Four possible outcomes, same posture as
    recommend_correlation_study_weight above:
      - should_apply=True, action="start_trial": nothing has ever been
        enabled yet, but there's enough overall real trade history to
        justify trying it.
      - should_apply=False, reason="insufficient_trade_history": not
        enough real trades exist yet in one or both buckets.
      - should_apply=False, reason="confirmed_enabled" /
        "evidence_favors_enabling_but_currently_off": the "with" cohort
        clearly wins -- reports it, never auto-(re)enables from here.
      - should_apply=True, action="disable": the "with" cohort clearly
        LOSES while currently enabled -- real evidence it's hurting."""
    trade_log = trade_log or []
    real_trades = [t for t in trade_log if not t.get("dry_run") and t.get("entry_conviction_sizing_enabled") is not None]
    with_feature = [t for t in real_trades if t.get("entry_conviction_sizing_enabled") is True]
    without_feature = [t for t in real_trades if t.get("entry_conviction_sizing_enabled") is False]
    with_stats = _bucket_stats(with_feature)
    without_stats = _bucket_stats(without_feature)

    if not with_feature:
        if not current_enabled and without_stats["trades"] >= CONVICTION_SIZING_MIN_HISTORY_TO_START:
            return {
                "ok": True, "should_apply": True, "action": "start_trial",
                "recommended_enabled": True, "with_feature": with_stats, "without_feature": without_stats,
            }
        return {
            "ok": True, "should_apply": False, "reason": "insufficient_trade_history",
            "with_feature": with_stats, "without_feature": without_stats,
        }

    if with_stats["trades"] < CONVICTION_SIZING_TRIAL_MIN_TRADES or without_stats["trades"] < CONVICTION_SIZING_TRIAL_MIN_TRADES:
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


MIN_BUCKET_TRADES = 5
_CONFIDENCE_BUCKET_EDGES = [0.5, 0.58, 0.65, 0.75, 1.01]


def _confidence_bucket_label(confidence: float | None) -> str | None:
    if confidence is None:
        return None
    for i in range(len(_CONFIDENCE_BUCKET_EDGES) - 1):
        lo, hi = _CONFIDENCE_BUCKET_EDGES[i], _CONFIDENCE_BUCKET_EDGES[i + 1]
        if lo <= confidence < hi:
            return f"{lo:.2f}-{min(hi, 1.0):.2f}"
    return None


def _side_bucket_label(trade: dict[str, Any]) -> str | None:
    side = trade.get("side")
    return side if side in ("yes", "no") else None


def _hold_minutes(trade: dict[str, Any]) -> float | None:
    import datetime as dt
    opened = trade.get("opened_at")
    closed = trade.get("closed_at")
    if not opened or not closed:
        return None
    try:
        opened_dt = dt.datetime.fromisoformat(str(opened).replace("Z", "+00:00"))
        closed_dt = dt.datetime.fromisoformat(str(closed).replace("Z", "+00:00"))
        return (closed_dt - opened_dt).total_seconds() / 60.0
    except (ValueError, TypeError):
        return None


_HOLD_MINUTES_BUCKETS = [(0, 5, "0-5min"), (5, 10, "5-10min"), (10, 15, "10-15min"), (15, float("inf"), "15min+")]


def _hold_minutes_bucket_label(minutes: float | None) -> str | None:
    if minutes is None:
        return None
    for lo, hi, label in _HOLD_MINUTES_BUCKETS:
        if lo <= minutes < hi:
            return label
    return _HOLD_MINUTES_BUCKETS[-1][2]


def _build_insights(
    overall: dict[str, Any], by_side: dict[str, dict[str, Any]], by_confidence: dict[str, dict[str, Any]],
) -> list[str]:
    """Human-readable, evidence-gated observations -- every insight names
    its own sample size so it's clear how much to trust it. Deliberately
    does NOT try to invent new indicators/features on its own -- surfaces
    real correlations a human (or a future, deliberate feature-
    engineering pass) can act on instead."""
    insights: list[str] = []
    if overall.get("trades") is None or overall["trades"] < MIN_BUCKET_TRADES:
        return insights

    yes_stats, no_stats = by_side.get("yes"), by_side.get("no")
    if yes_stats and no_stats and yes_stats["trades"] >= MIN_BUCKET_TRADES and no_stats["trades"] >= MIN_BUCKET_TRADES:
        gap = abs(yes_stats["win_rate"] - no_stats["win_rate"])
        if gap >= 0.15:
            better, worse = (("yes", "no") if yes_stats["win_rate"] > no_stats["win_rate"] else ("no", "yes"))
            better_stats, worse_stats = by_side[better], by_side[worse]
            insights.append(
                f"'{better}' entries win {better_stats['win_rate']:.0%} ({better_stats['trades']} trades) vs "
                f"'{worse}' entries' {worse_stats['win_rate']:.0%} ({worse_stats['trades']} trades) -- a stark "
                f"gap worth checking isn't a leftover artifact of the order-side bug fixed in scan_and_enter."
            )

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
    market slippage/fees (and, before the order-side fix, wouldn't even
    reflect the bug that was distorting real fills) and would distort the
    picture of how the account is actually performing."""
    trade_log = trade_log or []
    trades = [t for t in trade_log if include_dry_run or not t.get("dry_run")]
    overall = _bucket_stats(trades)
    if not trades:
        return {"ok": True, "trades_analyzed": 0, "overall": overall, "insights": []}

    by_side = _group_by(trades, _side_bucket_label)
    by_confidence_bucket = _group_by(trades, lambda t: _confidence_bucket_label(t.get("entry_confidence")))
    by_coin = _group_by(trades, lambda t: t.get("coin"))
    by_hold_minutes_bucket = _group_by(trades, lambda t: _hold_minutes_bucket_label(_hold_minutes(t)))

    return {
        "ok": True, "trades_analyzed": len(trades), "overall": overall,
        "by_side": by_side, "by_confidence_bucket": by_confidence_bucket,
        "by_coin": by_coin, "by_hold_minutes_bucket": by_hold_minutes_bucket,
        "insights": _build_insights(overall, by_side, by_confidence_bucket),
    }


def format_analysis_summary_text(analysis: dict[str, Any], *, tuning: dict[str, Any] | None = None) -> str:
    """Human-readable digest -- what the account's real trading history
    shows, not a raw data dump. No Threads posting caller exists for this
    market yet (kalshi_15m has no Threads presence at all today), so this
    is consumed by the dashboard/API and, indirectly, by ai_monitor.py's
    own snapshot -- not posted anywhere on its own."""
    overall = analysis.get("overall") or {}
    if not analysis.get("trades_analyzed"):
        return "Kalshi 15m trade analysis: not enough closed real trades yet to draw conclusions."

    lines = [
        f"Kalshi 15m trade review ({analysis['trades_analyzed']} real trades):",
        f"Win rate {overall['win_rate']:.0%} | Total P&L ${overall['total_pnl_usd']:.2f} | "
        f"Avg ${overall['avg_pnl_usd']:.2f}/trade",
    ]
    lines.extend(analysis.get("insights") or [])
    if tuning and tuning.get("should_apply"):
        lines.append(
            f"Confidence floor raised {tuning['current_threshold']:.2f} -> {tuning['recommended_threshold']:.2f} "
            f"based on this evidence."
        )
    return "\n".join(lines)


def _lesson_for(trade: dict[str, Any], *, outcome: str, pnl: float) -> str:
    coin = trade.get("coin") or "?"
    side = trade.get("side") or "?"
    if outcome == "win":
        return f"{coin} ({side}): WIN ${pnl:.2f}."
    confidence = trade.get("entry_confidence")
    if confidence is not None and confidence >= 0.65:
        return (
            f"{coin} ({side}): LOSS ${pnl:.2f} despite a high entry confidence ({confidence:.0%}) -- "
            f"worth flagging for the next retrain, possible regime the model hasn't adapted to."
        )
    return f"{coin} ({side}): LOSS ${pnl:.2f}."


def build_trade_snapshot(trade: dict[str, Any]) -> dict[str, Any]:
    """One settled trade's "full snap". No post-exit-drift/MFE-MAE field
    here (see this module's own docstring on why -- a settled binary
    contract has no "kept moving after exit" concept worth measuring)."""
    pnl = float(trade.get("realized_pnl_usd") or 0.0)
    outcome = "win" if pnl > 0 else "loss" if pnl < 0 else "flat"
    snapshot: dict[str, Any] = {
        "coin": trade.get("coin"), "side": trade.get("side"), "outcome": outcome, "pnl_usd": round(pnl, 6),
        "entry_confidence": trade.get("entry_confidence"), "hold_minutes": _hold_minutes(trade),
    }
    snapshot["lesson"] = _lesson_for(trade, outcome=outcome, pnl=pnl)
    return snapshot


def _build_batch_recommendations(snapshots: list[dict[str, Any]]) -> list[str]:
    recs: list[str] = []
    losses = [s for s in snapshots if s["outcome"] == "loss"]

    high_conf_losses = [s for s in losses if (s.get("entry_confidence") or 0) >= 0.65]
    if len(high_conf_losses) >= 2:
        recs.append(
            f"{len(high_conf_losses)} of the last {len(losses)} losses had high entry confidence -- "
            f"flagging for the next retrain, may indicate a regime the model hasn't adapted to yet."
        )

    side_counts: dict[str, int] = {}
    for s in losses:
        side = s.get("side")
        if side:
            side_counts[side] = side_counts.get(side, 0) + 1
    if side_counts:
        dominant_side, dominant_count = max(side_counts.items(), key=lambda kv: kv[1])
        if dominant_count >= 3 and dominant_count == len(losses):
            recs.append(
                f"All {dominant_count} of the last {len(losses)} losses were '{dominant_side}' entries -- "
                f"a recurring one-sided pattern worth a closer look."
            )

    return recs


def analyze_recent_trade_batch(trade_log: list[dict[str, Any]] | None, *, batch_size: int = BATCH_SIZE, include_dry_run: bool = False) -> dict[str, Any]:
    """Studies the most recent `batch_size` closed real trades."""
    trade_log = trade_log or []
    trades = [t for t in trade_log if include_dry_run or not t.get("dry_run")]
    if not trades:
        return {"ok": True, "trades_analyzed": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0, "snapshots": [], "recommendations": []}

    recent = trades[-batch_size:]
    snapshots = [build_trade_snapshot(t) for t in recent]
    wins = sum(1 for s in snapshots if s["outcome"] == "win")
    losses = sum(1 for s in snapshots if s["outcome"] == "loss")
    total_pnl = round(sum(s["pnl_usd"] for s in snapshots), 6)
    return {
        "ok": True, "trades_analyzed": len(snapshots), "wins": wins, "losses": losses,
        "total_pnl_usd": total_pnl, "snapshots": snapshots,
        "recommendations": _build_batch_recommendations(snapshots),
    }


def format_batch_snapshot_text(batch: dict[str, Any]) -> str:
    if not batch.get("trades_analyzed"):
        return "Kalshi 15m trade snapshot: no closed real trades yet."
    lines = [
        f"Kalshi 15m trade snapshot (last {batch['trades_analyzed']}): "
        f"{batch['wins']}W/{batch['losses']}L, total ${batch['total_pnl_usd']:+.2f}",
    ]
    lines.extend(f"- {s['lesson']}" for s in batch["snapshots"])
    lines.extend(batch.get("recommendations") or [])
    return "\n".join(lines)
