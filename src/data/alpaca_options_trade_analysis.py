"""Post-trade analysis for Alpaca options -- mirrors perps_trade_analysis.py's
own structure and reasoning (see its module docstring for the full design
rationale), with one deliberate difference: this module does NOT compute
max-favorable/max-adverse-excursion in premium dollars, because this
pipeline doesn't record option-PREMIUM history over time (only point-in-time
quotes at scan time -- see alpaca_options_strategy.py's own comment on why
it skips a chart-snapshot post too). Fabricating a premium-scale MFE/MAE
from underlying price data would silently mix two different scales (a
contract's premium doesn't move 1:1, or even linearly, with the underlying)
-- worse than not having the number at all.

What IS available and genuinely informative: the underlying's own real
price action after the trade closed. `_underlying_post_exit_drift_pct`
below measures whether the UNDERLYING kept moving the direction that would
have helped this position (up after a call, down after a put) shortly
after exit -- a directional signal only, never converted to a dollar
figure, and always labeled as underlying-based in the lesson text.

Studies the most recent BATCH_SIZE closed trades every time that many new
ones land (see alpaca_options_strategy.py's manage_open_positions ->
_maybe_run_batch_trade_analysis). Deliberately does NOT auto-write new
indicators or strategy code from this -- that's a human (or a future,
deliberate feature-engineering pass) call.

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
_EXIT_REASON_PREFIXES = ("take_profit", "stop_loss", "max_hold_time", "near_expiration")


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
# the actual narrow write happens in alpaca_options_strategy.apply_confidence_threshold_override,
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


# Evidence-gated auto-improvement triggered by a NEGATIVE backtest/
# walk-forward result -- "whenever you get negative return on backtest and
# forward test, automatically improve the crypto side using everything the
# bot has as a resource" (the user's own request, applied here to options
# too: "get in more trades and able to get it by itself"). Mirrors
# alpaca_crypto_trade_analysis.py's own identical pair of functions --
# see that module for the full design rationale. Reuses the SAME
# apply_confidence_threshold_override mechanism recommend_confidence_threshold
# above already writes through (no new write path); this just adds a
# SECOND, backtest-driven source of evidence for that one existing lever.
#
# Deliberately scoped to MODEL_CONFIDENCE_MIN only, not also
# TAKE_PROFIT_PCT/STOP_LOSS_PCT/MAX_HOLD_MINUTES despite the sweep varying
# those too -- same reasoning as crypto's own identical scope decision:
# adaptive_exit_pcts() (see its own docstring) scales take-profit/stop-loss
# to each UNDERLYING's own entry-time volatility for the overwhelming
# majority of real trades, and only falls back to the flat
# TAKE_PROFIT_PCT/STOP_LOSS_PCT constants when entry_volatility_30 is
# missing. A real difference in the sweep's own reported return_pct across
# TP/SL variants is very likely coming from whatever ELSE that variant
# also changed, not the TP/SL change itself, for live trading specifically.
# Confidence, by contrast, is a plain threshold with no adaptive layer in
# between -- safe to act on directly.
BACKTEST_TUNING_MIN_SAMPLE_TRADES = 20
BACKTEST_TUNING_MIN_RETURN_MARGIN_PCT = 0.02  # 2 percentage points -- avoids chasing sweep noise


def recommend_confidence_from_backtest(sweep_result: dict[str, Any] | None, *, current_threshold: float) -> dict[str, Any]:
    """Scans a alpaca_options_backtest.run_config_sweep() result
    (`all_configs`, not just the ranked/best cutoff -- this wants every
    row, including ones run_config_sweep's own min_trades filter would
    otherwise hide from `ranked`) for a variant that (a) sets a DIFFERENT
    model_confidence_min than what's live today, (b) has an adequately-
    sized, non-low_sample trade count, and (c) returned a meaningfully
    better return_pct than the sweep's own "current_defaults" row (see
    alpaca_options_backtest._current_defaults_config), falling back to
    matching by model_confidence_min value directly if that row is
    somehow missing (an older cached sweep result predating that anchor
    row). Returns should_apply=False whenever the sweep result is
    missing/malformed, too thin to trust, or doesn't show a clear,
    adequately-sampled improvement -- same non-committal-by-default
    posture as recommend_confidence_threshold above."""
    configs = (sweep_result or {}).get("all_configs") or []
    if not configs:
        return {"ok": True, "should_apply": False, "reason": "no_sweep_data", "current_threshold": current_threshold}

    def _is_current(cfg: dict[str, Any]) -> bool:
        if cfg.get("label") == "current_defaults":
            return True
        confidence = cfg.get("model_confidence_min")
        return confidence is not None and abs(float(confidence) - current_threshold) < 1e-9

    current_variant = next((c for c in configs if _is_current(c)), None)
    current_return = float(current_variant["return_pct"]) if current_variant and current_variant.get("return_pct") is not None else None

    best_candidate: dict[str, Any] | None = None
    for cfg in configs:
        confidence = cfg.get("model_confidence_min")
        if confidence is None or abs(float(confidence) - current_threshold) < 1e-9:
            continue  # not a confidence-varying variant, or matches what's already live
        if cfg.get("low_sample") or (cfg.get("trade_count") or 0) < BACKTEST_TUNING_MIN_SAMPLE_TRADES:
            continue
        return_pct = cfg.get("return_pct")
        if return_pct is None or return_pct <= 0:
            continue
        if current_return is not None and float(return_pct) < current_return + BACKTEST_TUNING_MIN_RETURN_MARGIN_PCT:
            continue
        if best_candidate is None or float(return_pct) > float(best_candidate["return_pct"]):
            best_candidate = cfg

    if best_candidate is None:
        return {
            "ok": True, "should_apply": False, "reason": "no_meaningfully_better_variant",
            "current_threshold": current_threshold, "current_return_pct": current_return,
        }

    return {
        "ok": True, "should_apply": True, "current_threshold": current_threshold,
        "recommended_threshold": float(best_candidate["model_confidence_min"]),
        "current_return_pct": current_return, "candidate": best_candidate,
    }


def backtest_shows_a_loss(sweep_result: dict[str, Any] | None, walkforward_result: dict[str, Any] | None) -> dict[str, Any]:
    """True (with the evidence attached) when the most recent sweep's own
    current-config reading, OR the walk-forward's own mean return across
    folds, shows a real loss -- the trigger condition for the auto-
    improvement pass above. Deliberately OR, not AND: either result on its
    own is real evidence of a live, structural problem worth reacting to
    immediately rather than waiting for both to agree."""
    reasons: list[str] = []
    configs = (sweep_result or {}).get("all_configs") or []
    current_variant = next(
        (c for c in configs if c.get("label") == "current_defaults"), None,
    )
    if current_variant and current_variant.get("return_pct") is not None and float(current_variant["return_pct"]) < 0:
        reasons.append(f"sweep current_defaults return_pct={current_variant['return_pct']:.4f}")

    wf_mean = (walkforward_result or {}).get("mean_return_pct")
    if wf_mean is not None and float(wf_mean) < 0:
        reasons.append(f"walk-forward mean_return_pct={wf_mean:.4f}")

    return {"is_loss": bool(reasons), "reasons": reasons}


def _exit_reason_bucket(reason: str | None) -> str:
    reason = reason or ""
    for prefix in _EXIT_REASON_PREFIXES:
        if reason.startswith(prefix):
            return prefix
    return "other"


# Real, confirmed gap found studying every market's win/loss patterns
# together (per explicit user direction: "make sure the options is
# studying all recent alpaca trades it made win or loss to avoid future
# loss also on top of what it does already"): unlike perps_trade_analysis.py
# and (as of tonight) alpaca_crypto_trade_analysis.py, this module never
# had an aggregate "bucket by X, flag a stark win-rate gap" analysis over
# the WHOLE trade history -- only the per-trade "lesson" snapshots above
# (which study the last BATCH_SIZE trades) and the confidence/backtest
# auto-tuners (which look at trade history too, but only through the
# narrow lens of "would a higher confidence floor have done better").
# Ported directly from alpaca_crypto_trade_analysis.py's own
# analyze_trade_history/_build_insights/_group_by (itself ported from
# perps_trade_analysis.py; see either's own docstring for the full
# rationale) -- reuses this module's OWN already-identical _is_win/
# _bucket_stats/_exit_reason_bucket rather than duplicating those.
#
# One real adaptation for options specifically: buckets by
# `underlying_symbol`, not `symbol` -- an options contract's own `symbol`
# (the OCC-style contract string) encodes strike+expiry and is therefore
# different on almost every single trade, which would fragment this into
# mostly-singleton buckets below MIN_BUCKET_TRADES forever. The ticker the
# model actually predicts on, and the one a real recurring pattern would
# show up against, is the underlying -- same reasoning
# alpaca_options_model._trade_outcome_sample_weight already uses.
MIN_BUCKET_TRADES = 5
_CONFIDENCE_BUCKET_EDGES = [0.5, 0.55, 0.6, 0.65, 0.7, 1.01]
_HOLD_MINUTES_BUCKETS = [(0, 5, "0-5min"), (5, 15, "5-15min"), (15, 30, "15-30min"), (30, float("inf"), "30min+")]


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
    near_expiration = by_exit_reason.get("near_expiration")
    if near_expiration and near_expiration["trades"] >= MIN_BUCKET_TRADES:
        insights.append(
            f"{near_expiration['trades']} near_expiration exits, win rate {near_expiration['win_rate']:.0%} -- "
            f"positions held too close to expiry to let the thesis play out."
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
    market slippage/fees and would distort the picture of how the account
    is actually performing (explicit user direction: "we doing only real
    data please not dry run or fake")."""
    trade_log = trade_log or []
    trades = [t for t in trade_log if include_dry_run or not t.get("dry_run")]
    overall = _bucket_stats(trades)
    if not trades:
        return {"ok": True, "trades_analyzed": 0, "overall": overall, "insights": []}

    by_exit_reason = _group_by(trades, lambda t: _exit_reason_bucket(t.get("reason")))
    by_confidence_bucket = _group_by(trades, lambda t: _confidence_bucket_label(t.get("entry_score")))
    by_symbol = _group_by(trades, lambda t: t.get("underlying_symbol") or t.get("symbol"))
    by_hold_minutes_bucket = _group_by(trades, lambda t: _hold_minutes_bucket_label(t.get("hold_minutes")))

    return {
        "ok": True, "trades_analyzed": len(trades), "overall": overall,
        "by_exit_reason": by_exit_reason, "by_confidence_bucket": by_confidence_bucket,
        "by_symbol": by_symbol, "by_hold_minutes_bucket": by_hold_minutes_bucket,
        "insights": _build_insights(overall, by_exit_reason, by_confidence_bucket),
    }


def format_analysis_summary_text(analysis: dict[str, Any], *, tuning: dict[str, Any] | None = None) -> str:
    """Human-readable digest for the Threads post -- what the account's
    real trading history shows, not a raw data dump. Ported directly from
    alpaca_crypto_trade_analysis.py's own identical function. `tuning` is
    accepted for interface parity with perps'/crypto's version but is
    never populated by the options daily job -- options' own confidence
    tuning already happens on its own, more frequent cadence via
    alpaca_options_strategy._maybe_run_batch_trade_analysis and
    maybe_auto_improve_from_backtest, so this job stays pure analysis, not
    a second place that could also decide to move the same live
    parameter."""
    overall = analysis.get("overall") or {}
    if not analysis.get("trades_analyzed"):
        return "Options trade analysis: not enough closed real trades yet to draw conclusions."

    lines = [
        f"Options trade review ({analysis['trades_analyzed']} real trades):",
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


def _parse_iso(ts: str | None) -> dt.datetime | None:
    if not ts:
        return None
    try:
        return dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _underlying_post_exit_drift_pct(trade: dict[str, Any], underlying_candles: list[dict[str, Any]]) -> float | None:
    """% the UNDERLYING moved in the POST_EXIT_DRIFT_CANDLES minutes after
    this trade's own closed_at, sign-adjusted so positive always means
    "the direction that would have helped this position" (up for a call,
    down for a put) -- a directional signal only, never a premium-dollar
    estimate (see this module's own docstring on why)."""
    closed = _parse_iso(trade.get("closed_at"))
    if not underlying_candles or closed is None:
        return None
    closed_ts = closed.timestamp()
    ordered = sorted(underlying_candles, key=lambda c: c.get("ts", 0))
    at_close = [c for c in ordered if c.get("ts", 0) <= closed_ts]
    post_window = [c for c in ordered if c.get("ts", 0) > closed_ts][:POST_EXIT_DRIFT_CANDLES]
    if not at_close or not post_window:
        return None
    underlying_at_close = float(at_close[-1]["close"])
    underlying_after = float(post_window[-1]["close"])
    if underlying_at_close <= 0:
        return None
    raw_drift = (underlying_after - underlying_at_close) / underlying_at_close
    option_type = (trade.get("option_type") or "").lower()
    return round(raw_drift if option_type != "put" else -raw_drift, 6)


def build_trade_snapshot(trade: dict[str, Any], *, underlying_candles: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """One closed trade's "full snap" -- see perps_trade_analysis's own
    function of the same name for the full design. No mfe_usd/mae_usd/
    capture_ratio here (see this module's own docstring on why) --
    underlying_drift_pct is the closest available signal, and it's
    directional-only, never a dollar amount."""
    pnl = float(trade.get("realized_pnl_usd") or 0.0)
    outcome = "win" if pnl > 0 else "loss" if pnl < 0 else "flat"
    snapshot: dict[str, Any] = {
        "symbol": trade.get("symbol"), "underlying_symbol": trade.get("underlying_symbol"),
        "option_type": trade.get("option_type"), "outcome": outcome, "pnl_usd": round(pnl, 6),
        "reason": trade.get("reason"), "hold_minutes": trade.get("hold_minutes"),
        "entry_probability_up": trade.get("entry_probability_up"),
    }
    drift = _underlying_post_exit_drift_pct(trade, underlying_candles or [])
    if drift is not None:
        snapshot["underlying_post_exit_drift_pct"] = drift
    snapshot["lesson"] = _lesson_for(snapshot)
    return snapshot


def _lesson_for(s: dict[str, Any]) -> str:
    symbol = s.get("symbol") or "?"
    pnl = s["pnl_usd"]
    if s["outcome"] == "win":
        return f"{symbol}: WIN ${pnl:.2f}."
    if s["outcome"] == "loss":
        drift = s.get("underlying_post_exit_drift_pct")
        if drift is not None and drift > POST_EXIT_DRIFT_MEANINGFUL_PCT:
            return (
                f"{symbol}: LOSS ${pnl:.2f} ({s.get('reason')}) -- the underlying kept moving in our favor by "
                f"{drift:.2%} shortly after exit, the stop/hold window may be too tight for current volatility."
            )
        probability_up = s.get("entry_probability_up")
        if probability_up is not None and (probability_up >= 0.65 or probability_up <= 0.35):
            return (
                f"{symbol}: LOSS ${pnl:.2f} despite a high entry model confidence "
                f"({max(probability_up, 1 - probability_up):.0%}) -- worth flagging for the next retrain, "
                f"possible regime the model hasn't adapted to."
            )
        return f"{symbol}: LOSS ${pnl:.2f} ({s.get('reason')})."
    return f"{symbol}: closed flat."


def _build_batch_recommendations(snapshots: list[dict[str, Any]]) -> list[str]:
    recs: list[str] = []
    losses = [s for s in snapshots if s["outcome"] == "loss"]

    reversal_losses = [s for s in losses if (s.get("underlying_post_exit_drift_pct") or 0) > POST_EXIT_DRIFT_MEANINGFUL_PCT]
    if len(reversal_losses) >= 2:
        recs.append(
            f"{len(reversal_losses)} of the last {len(losses)} losses saw the underlying reverse favorably "
            f"shortly after exit -- consider a wider stop or longer hold window for choppy conditions."
        )

    high_conf_losses = [
        s for s in losses
        if s.get("entry_probability_up") is not None and (s["entry_probability_up"] >= 0.65 or s["entry_probability_up"] <= 0.35)
    ]
    if len(high_conf_losses) >= 2:
        recs.append(
            f"{len(high_conf_losses)} of the last {len(losses)} losses had high entry model confidence -- "
            f"flagging for the next retrain, may indicate a regime the model hasn't adapted to yet."
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
    trade_log: list[dict[str, Any]] | None, *, underlying_candles_by_symbol: dict[str, list[dict[str, Any]]] | None = None,
    batch_size: int = BATCH_SIZE, include_dry_run: bool = False,
) -> dict[str, Any]:
    """Studies the most recent `batch_size` closed real trades.
    `underlying_candles_by_symbol` is keyed by underlying_symbol (recent
    1-minute OHLC of the UNDERLYING, not the option's own premium -- see
    this module's own docstring) -- a missing/empty entry just skips that
    trade's drift field, never a hard failure."""
    trade_log = trade_log or []
    trades = [t for t in trade_log if include_dry_run or not t.get("dry_run")]
    if not trades:
        return {"ok": True, "trades_analyzed": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0, "snapshots": [], "recommendations": []}

    recent = trades[-batch_size:]
    underlying_candles_by_symbol = underlying_candles_by_symbol or {}
    snapshots = [
        build_trade_snapshot(t, underlying_candles=underlying_candles_by_symbol.get(t.get("underlying_symbol")))
        for t in recent
    ]
    wins = sum(1 for s in snapshots if s["outcome"] == "win")
    losses = sum(1 for s in snapshots if s["outcome"] == "loss")
    total_pnl = round(sum(s["pnl_usd"] for s in snapshots), 6)
    return {
        "ok": True, "trades_analyzed": len(snapshots), "wins": wins, "losses": losses,
        "total_pnl_usd": total_pnl, "snapshots": snapshots,
        "recommendations": _build_batch_recommendations(snapshots),
    }


def format_batch_snapshot_text(batch: dict[str, Any], *, market: str = "options") -> str:
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
