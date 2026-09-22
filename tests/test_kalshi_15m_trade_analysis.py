"""Post-trade win/loss analysis for Kalshi's 15-minute markets.

This file tests kalshi_15m_trade_analysis.py's own pure functions in
isolation, on synthetic trade_log dicts -- the SAME testing convention
every sibling *_trade_analysis.py module's own test file already uses
(perps/options/crypto/stocks), so these tests run instantly and
deterministically in CI with no live Kalshi account, HF credentials, or
trained model required. That is a property of THIS TEST FILE only, not
of the feature: in production, kalshi_15m_strategy.check_settlements
calls these same functions with the REAL trade_log (real settled trades,
real model confidence scores, real correlation-study readings) every
time a trade closes, and a positive result
(apply_confidence_threshold_override/apply_correlation_study_override)
writes back into the REAL durable state that scan_and_enter reads on its
next real live cycle -- see kalshi_15m_strategy.py's own module docstring
and _maybe_run_batch_trade_analysis for that wiring.

See kalshi_15m_trade_analysis.py's own module docstring for the two
deliberate differences from every sibling *_trade_analysis.py module: no
exit_reason bucket (there's only one exit here -- settlement), and a NEW
`side` (yes/no) bucket this account's own real order-side bug makes
genuinely worth watching."""
from __future__ import annotations

from data import kalshi_15m_trade_analysis as k15ta


def _trade(
    *, coin: str = "BTC", side: str = "yes", pnl: float, entry_confidence: float = 0.6, dry_run: bool = False,
    opened_at: str = "2026-08-01T12:00:00+00:00", closed_at: str = "2026-08-01T12:10:00+00:00",
) -> dict:
    return {
        "coin": coin, "side": side, "realized_pnl_usd": pnl, "entry_confidence": entry_confidence,
        "dry_run": dry_run, "opened_at": opened_at, "closed_at": closed_at,
    }


# ---------------------------------------------------------------------------
# analyze_trade_history
# ---------------------------------------------------------------------------
def test_analyze_trade_history_with_no_trades_is_a_safe_empty_result():
    result = k15ta.analyze_trade_history([])
    assert result == {"ok": True, "trades_analyzed": 0, "overall": {
        "trades": 0, "wins": 0, "losses": 0, "win_rate": None, "total_pnl_usd": 0.0, "avg_pnl_usd": None,
    }, "insights": []}


def test_analyze_trade_history_excludes_dry_run_trades_by_default():
    trades = [_trade(pnl=5.0, dry_run=True), _trade(pnl=3.0, dry_run=False)]
    result = k15ta.analyze_trade_history(trades)
    assert result["trades_analyzed"] == 1
    assert result["overall"]["total_pnl_usd"] == 3.0


def test_analyze_trade_history_includes_dry_run_when_asked():
    trades = [_trade(pnl=5.0, dry_run=True), _trade(pnl=3.0, dry_run=False)]
    result = k15ta.analyze_trade_history(trades, include_dry_run=True)
    assert result["trades_analyzed"] == 2


def test_analyze_trade_history_computes_win_rate_and_pnl():
    trades = [_trade(pnl=2.0), _trade(pnl=-1.0), _trade(pnl=3.0), _trade(pnl=-2.0), _trade(pnl=1.0)]
    result = k15ta.analyze_trade_history(trades)
    overall = result["overall"]
    assert overall["trades"] == 5
    assert overall["wins"] == 3
    assert overall["losses"] == 2
    assert overall["total_pnl_usd"] == 3.0
    assert overall["win_rate"] == 0.6


def test_analyze_trade_history_buckets_by_side_confidence_coin_and_hold_minutes():
    trades = [
        _trade(coin="BTC", side="yes", pnl=1.0, entry_confidence=0.6),
        _trade(coin="ETH", side="no", pnl=-1.0, entry_confidence=0.8),
    ]
    result = k15ta.analyze_trade_history(trades)
    assert set(result["by_side"].keys()) <= {"yes", "no"}
    assert "BTC" in result["by_coin"]
    assert "ETH" in result["by_coin"]
    assert result["by_confidence_bucket"]
    assert result["by_hold_minutes_bucket"]


def test_analyze_trade_history_flags_a_stark_yes_no_win_rate_gap():
    """The one bucket no sibling market's trade_analysis module has --
    directly relevant given this account's own real, confirmed order-side
    bug (see kalshi_15m_strategy.scan_and_enter's own docstring)."""
    yes_wins = [_trade(coin="BTC", side="yes", pnl=1.0) for _ in range(5)]
    no_losses = [_trade(coin="BTC", side="no", pnl=-1.0) for _ in range(5)]
    result = k15ta.analyze_trade_history(yes_wins + no_losses)
    assert any("'yes'" in i and "'no'" in i for i in result["insights"])


def test_analyze_trade_history_does_not_flag_a_small_yes_no_gap():
    trades = [_trade(coin="BTC", side="yes", pnl=1.0) for _ in range(3)] + \
        [_trade(coin="BTC", side="yes", pnl=-1.0) for _ in range(2)] + \
        [_trade(coin="BTC", side="no", pnl=1.0) for _ in range(3)] + \
        [_trade(coin="BTC", side="no", pnl=-1.0) for _ in range(2)]
    result = k15ta.analyze_trade_history(trades)
    assert not any("win" in i and "vs" in i and ("'yes'" in i or "'no'" in i) for i in result["insights"])


def test_analyze_trade_history_flags_well_calibrated_confidence():
    low = [_trade(pnl=-1.0, entry_confidence=0.52) for _ in range(5)]
    high = [_trade(pnl=1.0, entry_confidence=0.90) for _ in range(5)]
    result = k15ta.analyze_trade_history(low + high)
    assert any("well-calibrated" in i for i in result["insights"])


def test_analyze_trade_history_flags_poorly_calibrated_confidence():
    low = [_trade(pnl=1.0, entry_confidence=0.52) for _ in range(5)]
    high = [_trade(pnl=-1.0, entry_confidence=0.90) for _ in range(5)]
    result = k15ta.analyze_trade_history(low + high)
    assert any("NOT reliably predictive" in i for i in result["insights"])


def test_analyze_trade_history_stays_silent_below_the_minimum_bucket_size():
    trades = [_trade(pnl=1.0), _trade(pnl=-1.0)]
    result = k15ta.analyze_trade_history(trades)
    assert result["insights"] == []


# ---------------------------------------------------------------------------
# recommend_confidence_threshold -- evidence-gated, never a blind knob turn.
# ---------------------------------------------------------------------------
def test_recommend_confidence_threshold_does_not_apply_with_thin_history():
    trades = [_trade(pnl=1.0, entry_confidence=0.6) for _ in range(5)]
    result = k15ta.recommend_confidence_threshold(trades, current_threshold=0.58)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"


def test_recommend_confidence_threshold_does_not_apply_without_a_clear_improvement():
    # Every trade performs identically regardless of confidence -- no
    # candidate step can show BOTH a better win rate AND a better avg P&L.
    trades = [_trade(pnl=1.0 if i % 2 == 0 else -1.0, entry_confidence=0.6 + (i % 5) * 0.05) for i in range(20)]
    result = k15ta.recommend_confidence_threshold(trades, current_threshold=0.58)
    assert result["should_apply"] is False


def test_recommend_confidence_threshold_applies_when_a_higher_floor_clearly_wins():
    baseline = [_trade(pnl=-0.5, entry_confidence=0.60) for _ in range(15)]
    high_confidence_winners = [_trade(pnl=2.0, entry_confidence=0.90) for _ in range(15)]
    trades = baseline + high_confidence_winners
    result = k15ta.recommend_confidence_threshold(trades, current_threshold=0.58)
    assert result["should_apply"] is True
    assert result["recommended_threshold"] > 0.58
    assert result["recommended_threshold"] <= 0.58 + k15ta.CONFIDENCE_TUNING_MAX_STEP


def test_recommend_confidence_threshold_ignores_dry_run_and_missing_confidence_trades():
    trades = [_trade(pnl=1.0, dry_run=True)] * 20
    trades += [{"coin": "BTC", "side": "yes", "realized_pnl_usd": 1.0, "dry_run": False}] * 20  # no entry_confidence
    result = k15ta.recommend_confidence_threshold(trades, current_threshold=0.58)
    assert result["should_apply"] is False
    assert result["trades_at_current"] == 0


# ---------------------------------------------------------------------------
# recommend_correlation_study_weight -- same evidence-gated posture, keyed
# on entry_correlation_score (bullish-signed, flipped for "no" -- same
# convention kalshi_15m_strategy.evaluate_candidate's own
# side_correlation_score uses).
# ---------------------------------------------------------------------------
def _corr_trade(*, side: str = "yes", pnl: float, score: float, dry_run: bool = False) -> dict:
    return {"coin": "BTC", "side": side, "realized_pnl_usd": pnl, "entry_correlation_score": score, "dry_run": dry_run}


def test_recommend_correlation_study_weight_does_not_apply_with_thin_history():
    trades = [_corr_trade(pnl=1.0, score=0.5) for _ in range(5)]
    result = k15ta.recommend_correlation_study_weight(trades, current_enabled=False, current_max_adjustment=0.06)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"


def test_recommend_correlation_study_weight_recommends_enabling_when_agreement_clearly_wins():
    agreed = [_corr_trade(side="yes", pnl=2.0, score=0.8) for _ in range(15)]  # agrees with "yes"
    baseline = [_corr_trade(side="yes", pnl=-0.5, score=0.0) for _ in range(15)]  # neutral/disagreeing
    result = k15ta.recommend_correlation_study_weight(agreed + baseline, current_enabled=False, current_max_adjustment=0.06)
    assert result["should_apply"] is True
    assert result["action"] == "enable"
    assert result["recommended_enabled"] is True


def test_recommend_correlation_study_weight_increases_weight_when_already_enabled_and_winning():
    agreed = [_corr_trade(side="yes", pnl=2.0, score=0.8) for _ in range(15)]
    baseline = [_corr_trade(side="yes", pnl=-0.5, score=0.0) for _ in range(15)]
    result = k15ta.recommend_correlation_study_weight(agreed + baseline, current_enabled=True, current_max_adjustment=0.06)
    assert result["should_apply"] is True
    assert result["action"] == "increase_weight"
    assert result["recommended_max_adjustment"] > 0.06


def test_recommend_correlation_study_weight_recommends_disabling_when_agreement_clearly_loses():
    agreed = [_corr_trade(side="yes", pnl=-2.0, score=0.8) for _ in range(15)]  # agrees but LOSES
    baseline = [_corr_trade(side="yes", pnl=1.0, score=0.0) for _ in range(15)]
    result = k15ta.recommend_correlation_study_weight(agreed + baseline, current_enabled=True, current_max_adjustment=0.06)
    assert result["should_apply"] is True
    assert result["action"] == "disable"
    assert result["recommended_enabled"] is False


def test_recommend_correlation_study_weight_flips_sign_for_a_no_side_trade():
    """A "no" trade with a NEGATIVE correlation score is the score
    AGREEING with the side actually taken (bearish-signed for a "no")."""
    agreed = [_corr_trade(side="no", pnl=2.0, score=-0.8) for _ in range(15)]
    baseline = [_corr_trade(side="no", pnl=-0.5, score=0.0) for _ in range(15)]
    result = k15ta.recommend_correlation_study_weight(agreed + baseline, current_enabled=False, current_max_adjustment=0.06)
    assert result["should_apply"] is True
    assert result["action"] == "enable"


def test_recommend_correlation_study_weight_ignores_dry_run_and_missing_score_trades():
    trades = [_corr_trade(pnl=1.0, score=0.8, dry_run=True)] * 20
    trades += [{"coin": "BTC", "side": "yes", "realized_pnl_usd": 1.0, "dry_run": False}] * 20  # no entry_correlation_score
    result = k15ta.recommend_correlation_study_weight(trades, current_enabled=False, current_max_adjustment=0.06)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"


# ---------------------------------------------------------------------------
# recommend_conviction_sizing_trial -- same evidence-gated trial as
# perps' own recommend_position_management_trial, scoped to the ONE
# feature (conviction sizing) that maps onto kalshi_15m's product.
# ---------------------------------------------------------------------------
def _cs_trade(*, pnl: float, enabled: bool | None) -> dict:
    return {"coin": "BTC", "side": "yes", "realized_pnl_usd": pnl, "entry_conviction_sizing_enabled": enabled, "dry_run": False}


def test_recommend_conviction_sizing_trial_proposes_a_start_trial_with_enough_history():
    trades = [_cs_trade(pnl=1.0, enabled=False) for _ in range(30)]
    result = k15ta.recommend_conviction_sizing_trial(trades, current_enabled=False)
    assert result["should_apply"] is True
    assert result["action"] == "start_trial"
    assert result["recommended_enabled"] is True


def test_recommend_conviction_sizing_trial_does_not_propose_a_trial_below_the_history_floor():
    trades = [_cs_trade(pnl=1.0, enabled=False) for _ in range(10)]
    result = k15ta.recommend_conviction_sizing_trial(trades, current_enabled=False)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"


def test_recommend_conviction_sizing_trial_confirms_enabled_when_evidence_favors_it():
    with_feature = [_cs_trade(pnl=2.0, enabled=True) for _ in range(20)]
    without_feature = [_cs_trade(pnl=1.0, enabled=False) for _ in range(20)]
    result = k15ta.recommend_conviction_sizing_trial(with_feature + without_feature, current_enabled=True)
    assert result["should_apply"] is False
    assert result["reason"] == "confirmed_enabled"


def test_recommend_conviction_sizing_trial_reports_favoring_enabling_when_currently_off():
    with_feature = [_cs_trade(pnl=2.0, enabled=True) for _ in range(20)]
    without_feature = [_cs_trade(pnl=1.0, enabled=False) for _ in range(20)]
    result = k15ta.recommend_conviction_sizing_trial(with_feature + without_feature, current_enabled=False)
    assert result["should_apply"] is False
    assert result["reason"] == "evidence_favors_enabling_but_currently_off"


def test_recommend_conviction_sizing_trial_recommends_disabling_when_evidence_turns_against_it():
    with_feature = [_cs_trade(pnl=-1.0, enabled=True) for _ in range(20)]
    without_feature = [_cs_trade(pnl=1.0, enabled=False) for _ in range(20)]
    result = k15ta.recommend_conviction_sizing_trial(with_feature + without_feature, current_enabled=True)
    assert result["should_apply"] is True
    assert result["action"] == "disable"
    assert result["recommended_enabled"] is False


def test_recommend_conviction_sizing_trial_ignores_dry_run_and_missing_flag_trades():
    trades = [_cs_trade(pnl=1.0, enabled=True)] * 5
    for t in trades:
        t["dry_run"] = True
    trades += [{"coin": "BTC", "side": "yes", "realized_pnl_usd": 1.0, "dry_run": False}] * 30  # no entry_conviction_sizing_enabled
    result = k15ta.recommend_conviction_sizing_trial(trades, current_enabled=False)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"


# ---------------------------------------------------------------------------
# recommend_win_streak_sizing_trial -- identical structure to
# recommend_conviction_sizing_trial above, for the win-streak size
# increase.
# ---------------------------------------------------------------------------
def _ws_trade(*, pnl: float, enabled: bool | None) -> dict:
    return {"coin": "BTC", "side": "yes", "realized_pnl_usd": pnl, "entry_win_streak_sizing_enabled": enabled, "dry_run": False}


def test_recommend_win_streak_sizing_trial_proposes_a_start_trial_with_enough_history():
    trades = [_ws_trade(pnl=1.0, enabled=False) for _ in range(30)]
    result = k15ta.recommend_win_streak_sizing_trial(trades, current_enabled=False)
    assert result["should_apply"] is True
    assert result["action"] == "start_trial"
    assert result["recommended_enabled"] is True


def test_recommend_win_streak_sizing_trial_does_not_propose_a_trial_below_the_history_floor():
    trades = [_ws_trade(pnl=1.0, enabled=False) for _ in range(10)]
    result = k15ta.recommend_win_streak_sizing_trial(trades, current_enabled=False)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"


def test_recommend_win_streak_sizing_trial_recommends_disabling_when_evidence_turns_against_it():
    with_feature = [_ws_trade(pnl=-1.0, enabled=True) for _ in range(20)]
    without_feature = [_ws_trade(pnl=1.0, enabled=False) for _ in range(20)]
    result = k15ta.recommend_win_streak_sizing_trial(with_feature + without_feature, current_enabled=True)
    assert result["should_apply"] is True
    assert result["action"] == "disable"
    assert result["recommended_enabled"] is False


def test_recommend_win_streak_sizing_trial_confirms_enabled_when_evidence_favors_it():
    with_feature = [_ws_trade(pnl=2.0, enabled=True) for _ in range(20)]
    without_feature = [_ws_trade(pnl=1.0, enabled=False) for _ in range(20)]
    result = k15ta.recommend_win_streak_sizing_trial(with_feature + without_feature, current_enabled=True)
    assert result["should_apply"] is False
    assert result["reason"] == "confirmed_enabled"


# ---------------------------------------------------------------------------
# build_trade_snapshot / _lesson_for
# ---------------------------------------------------------------------------
def test_build_trade_snapshot_win():
    snap = k15ta.build_trade_snapshot(_trade(coin="BTC", side="yes", pnl=5.0))
    assert snap["outcome"] == "win"
    assert snap["lesson"] == "BTC (yes): WIN $5.00."
    assert snap["hold_minutes"] == 10.0


def test_build_trade_snapshot_loss_at_low_confidence():
    snap = k15ta.build_trade_snapshot(_trade(coin="ETH", side="no", pnl=-2.0, entry_confidence=0.60))
    assert snap["outcome"] == "loss"
    assert snap["lesson"] == "ETH (no): LOSS $-2.00."


def test_build_trade_snapshot_loss_at_high_confidence_flags_for_retrain():
    snap = k15ta.build_trade_snapshot(_trade(coin="ETH", side="no", pnl=-2.0, entry_confidence=0.72))
    assert "worth flagging for the next retrain" in snap["lesson"]


def test_build_trade_snapshot_never_produces_a_post_exit_drift_field():
    """The core structural difference from options/perps -- a settled
    binary contract has no "kept moving after exit" concept."""
    snap = k15ta.build_trade_snapshot(_trade(pnl=5.0))
    assert "underlying_post_exit_drift_pct" not in snap
    assert "mfe_usd" not in snap


# ---------------------------------------------------------------------------
# analyze_recent_trade_batch / _build_batch_recommendations
# ---------------------------------------------------------------------------
def test_analyze_recent_trade_batch_with_no_trades():
    result = k15ta.analyze_recent_trade_batch([])
    assert result == {"ok": True, "trades_analyzed": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0, "snapshots": [], "recommendations": []}


def test_analyze_recent_trade_batch_studies_only_the_last_batch_size_trades():
    trades = [_trade(pnl=float(i)) for i in range(10)]
    result = k15ta.analyze_recent_trade_batch(trades, batch_size=3)
    assert result["trades_analyzed"] == 3
    assert [s["pnl_usd"] for s in result["snapshots"]] == [7.0, 8.0, 9.0]


def test_analyze_recent_trade_batch_recommends_a_retrain_look_on_repeated_high_confidence_losses():
    trades = [_trade(pnl=-1.0, entry_confidence=0.70) for _ in range(2)] + [_trade(pnl=1.0)]
    result = k15ta.analyze_recent_trade_batch(trades)
    assert any("flagging for the next retrain" in r for r in result["recommendations"])


def test_analyze_recent_trade_batch_flags_a_one_sided_losing_streak():
    trades = [_trade(side="no", pnl=-1.0, entry_confidence=0.6) for _ in range(3)]
    result = k15ta.analyze_recent_trade_batch(trades)
    assert any("'no' entries" in r for r in result["recommendations"])


def test_analyze_recent_trade_batch_stays_quiet_on_a_mixed_losing_batch():
    trades = [_trade(side="no", pnl=-1.0, entry_confidence=0.6), _trade(side="yes", pnl=-1.0, entry_confidence=0.6), _trade(side="no", pnl=1.0)]
    result = k15ta.analyze_recent_trade_batch(trades)
    assert result["recommendations"] == []


# ---------------------------------------------------------------------------
# format_analysis_summary_text / format_batch_snapshot_text
# ---------------------------------------------------------------------------
def test_format_analysis_summary_text_with_no_trades():
    text = k15ta.format_analysis_summary_text(k15ta.analyze_trade_history([]))
    assert text == "Kalshi 15m trade analysis: not enough closed real trades yet to draw conclusions."


def test_format_analysis_summary_text_includes_the_win_rate_and_pnl():
    trades = [_trade(pnl=1.0), _trade(pnl=-0.5)]
    text = k15ta.format_analysis_summary_text(k15ta.analyze_trade_history(trades))
    assert "Kalshi 15m trade review (2 real trades):" in text
    assert "Win rate 50%" in text
    assert "$0.50" in text


def test_format_analysis_summary_text_mentions_an_applied_tuning_change():
    trades = [_trade(pnl=1.0), _trade(pnl=-0.5)]
    tuning = {"should_apply": True, "current_threshold": 0.58, "recommended_threshold": 0.62}
    text = k15ta.format_analysis_summary_text(k15ta.analyze_trade_history(trades), tuning=tuning)
    assert "0.58 -> 0.62" in text


def test_format_batch_snapshot_text_with_no_trades():
    text = k15ta.format_batch_snapshot_text(k15ta.analyze_recent_trade_batch([]))
    assert text == "Kalshi 15m trade snapshot: no closed real trades yet."


def test_format_batch_snapshot_text_lists_each_lesson():
    trades = [_trade(coin="BTC", side="yes", pnl=1.0), _trade(coin="ETH", side="no", pnl=-1.0)]
    text = k15ta.format_batch_snapshot_text(k15ta.analyze_recent_trade_batch(trades))
    assert "1W/1L" in text
    assert "BTC (yes): WIN $1.00." in text
    assert "ETH (no): LOSS $-1.00." in text
