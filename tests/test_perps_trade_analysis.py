"""Post-trade win/loss analysis for perps. Synthetic trade_log data only --
pure computation over dicts, no network/state/model involved."""
from __future__ import annotations

from data import perps_trade_analysis as pta


def _trade(
    *, pnl: float, reason: str = "take_profit (+2%)", dry_run: bool = False,
    entry_score: float | None = 0.6, hold_minutes: float | None = 10.0, ticker: str = "KXBTCPERP", side: str = "long",
) -> dict:
    return {
        "ticker": ticker, "side": side, "realized_pnl_usd": pnl, "reason": reason,
        "dry_run": dry_run, "entry_score": entry_score, "hold_minutes": hold_minutes,
    }


def test_analyze_trade_history_with_no_trades_returns_zero_state():
    result = pta.analyze_trade_history([])
    assert result["ok"] is True
    assert result["trades_analyzed"] == 0
    assert result["overall"]["win_rate"] is None


def test_analyze_trade_history_excludes_dry_run_trades_by_default():
    trades = [_trade(pnl=1.0, dry_run=True), _trade(pnl=-1.0, dry_run=False)]
    result = pta.analyze_trade_history(trades)
    assert result["trades_analyzed"] == 1
    assert result["overall"]["total_pnl_usd"] == -1.0


def test_analyze_trade_history_can_include_dry_run_trades():
    trades = [_trade(pnl=1.0, dry_run=True), _trade(pnl=-1.0, dry_run=False)]
    result = pta.analyze_trade_history(trades, include_dry_run=True)
    assert result["trades_analyzed"] == 2


def test_analyze_trade_history_computes_win_rate_and_pnl():
    trades = [_trade(pnl=1.0), _trade(pnl=2.0), _trade(pnl=-1.0)]
    result = pta.analyze_trade_history(trades)
    overall = result["overall"]
    assert overall["trades"] == 3
    assert overall["wins"] == 2
    assert overall["losses"] == 1
    assert overall["win_rate"] == round(2 / 3, 4)
    assert overall["total_pnl_usd"] == 2.0


def test_analyze_trade_history_buckets_by_exit_reason():
    trades = [
        _trade(pnl=1.0, reason="take_profit (+2%)"),
        _trade(pnl=-0.5, reason="stop_loss (-1%)"),
        _trade(pnl=-0.2, reason="max_hold_time (30min)"),
        _trade(pnl=0.3, reason="something_unrecognized"),
    ]
    result = pta.analyze_trade_history(trades)
    by_reason = result["by_exit_reason"]
    assert by_reason["take_profit"]["trades"] == 1
    assert by_reason["stop_loss"]["trades"] == 1
    assert by_reason["max_hold_time"]["trades"] == 1
    assert by_reason["other"]["trades"] == 1


def test_analyze_trade_history_buckets_by_confidence():
    trades = [_trade(pnl=1.0, entry_score=0.52), _trade(pnl=1.0, entry_score=0.72), _trade(pnl=1.0, entry_score=None)]
    result = pta.analyze_trade_history(trades)
    by_confidence = result["by_confidence_bucket"]
    assert by_confidence["0.50-0.55"]["trades"] == 1
    assert by_confidence["0.70-1.00"]["trades"] == 1
    # entry_score=None trades are excluded from this breakdown (can't bucket them), not silently miscounted.
    assert sum(v["trades"] for v in by_confidence.values()) == 2


def test_analyze_trade_history_buckets_by_hold_minutes():
    trades = [_trade(pnl=1.0, hold_minutes=2.0), _trade(pnl=1.0, hold_minutes=20.0), _trade(pnl=1.0, hold_minutes=45.0)]
    result = pta.analyze_trade_history(trades)
    by_hold = result["by_hold_minutes_bucket"]
    assert by_hold["0-5min"]["trades"] == 1
    assert by_hold["15-30min"]["trades"] == 1
    assert by_hold["30min+"]["trades"] == 1


def test_analyze_trade_history_insights_require_minimum_sample_size():
    """A 2-trade "100% win rate" is noise, not evidence -- insights must not
    fire below MIN_BUCKET_TRADES."""
    trades = [_trade(pnl=1.0, reason="stop_loss (-1%)"), _trade(pnl=1.0, reason="stop_loss (-1%)")]
    result = pta.analyze_trade_history(trades)
    assert result["insights"] == []


def test_analyze_trade_history_insights_fire_with_enough_samples():
    trades = [_trade(pnl=-0.3, reason="stop_loss (-1%)") for _ in range(pta.MIN_BUCKET_TRADES)]
    result = pta.analyze_trade_history(trades)
    assert any("stop_loss" in insight for insight in result["insights"])


def test_analyze_trade_history_confidence_calibration_insight():
    low = [_trade(pnl=-0.3, entry_score=0.51) for _ in range(pta.MIN_BUCKET_TRADES)]
    high = [_trade(pnl=1.0, entry_score=0.85) for _ in range(pta.MIN_BUCKET_TRADES)]
    result = pta.analyze_trade_history(low + high)
    assert any("well-calibrated" in insight for insight in result["insights"])


def test_recommend_confidence_threshold_insufficient_history_does_not_apply():
    trades = [_trade(pnl=1.0, entry_score=0.6) for _ in range(3)]
    result = pta.recommend_confidence_threshold(trades, current_threshold=0.58)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"


def test_recommend_confidence_threshold_recommends_when_evidence_supports_it():
    # Baseline (>= 0.58): a mix of wins/losses. Higher band (>= 0.62): consistently better.
    trades = []
    for _ in range(pta.CONFIDENCE_TUNING_MIN_TRADES):
        trades.append(_trade(pnl=-0.2, entry_score=0.59))  # loses, below the higher band
    for _ in range(pta.CONFIDENCE_TUNING_MIN_TRADES):
        trades.append(_trade(pnl=0.5, entry_score=0.65))  # wins, clears the higher band too
    result = pta.recommend_confidence_threshold(trades, current_threshold=0.58)
    assert result["should_apply"] is True
    assert result["recommended_threshold"] > 0.58
    assert result["recommended_threshold"] <= round(0.58 + pta.CONFIDENCE_TUNING_MAX_STEP, 4)


def test_recommend_confidence_threshold_does_not_apply_without_a_clear_improvement():
    """Same win rate/avg P&L at every confidence level -- no reason to move."""
    trades = [_trade(pnl=0.1, entry_score=score) for score in [0.59, 0.63, 0.67, 0.71] for _ in range(10)]
    result = pta.recommend_confidence_threshold(trades, current_threshold=0.58)
    assert result["should_apply"] is False


def test_recommend_confidence_threshold_ignores_dry_run_trades():
    trades = [_trade(pnl=5.0, entry_score=0.65, dry_run=True) for _ in range(pta.CONFIDENCE_TUNING_MIN_TRADES)]
    result = pta.recommend_confidence_threshold(trades, current_threshold=0.58)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"


def test_format_analysis_summary_text_with_no_trades():
    text = pta.format_analysis_summary_text({"trades_analyzed": 0, "overall": {}})
    assert "not enough" in text.lower()


def test_format_analysis_summary_text_includes_win_rate_and_pnl():
    trades = [_trade(pnl=1.0), _trade(pnl=-0.5)]
    analysis = pta.analyze_trade_history(trades)
    text = pta.format_analysis_summary_text(analysis)
    assert "Win rate" in text
    assert "P&L" in text


def test_format_analysis_summary_text_mentions_applied_tuning():
    trades = [_trade(pnl=1.0)]
    analysis = pta.analyze_trade_history(trades)
    tuning = {"should_apply": True, "current_threshold": 0.58, "recommended_threshold": 0.62}
    text = pta.format_analysis_summary_text(analysis, tuning=tuning)
    assert "0.58" in text and "0.62" in text
