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


# ---------------------------------------------------------------------------
# Per-trade / small-batch snapshot study
# ---------------------------------------------------------------------------

def _closed_trade(
    *, pnl: float, side: str = "long", entry_price: float = 100.0, exit_price: float = 101.0,
    reason: str = "take_profit (+2%)", opened_at: str = "2026-08-01T12:00:00+00:00",
    closed_at: str = "2026-08-01T12:10:00+00:00", ticker: str = "KXBTCPERP",
    entry_score: float | None = 0.6, dry_run: bool = False,
) -> dict:
    return {
        "ticker": ticker, "side": side, "realized_pnl_usd": pnl, "reason": reason, "dry_run": dry_run,
        "entry_price": entry_price, "exit_price": exit_price, "opened_at": opened_at, "closed_at": closed_at,
        "entry_score": entry_score, "hold_minutes": 10.0,
    }


def _candle(ts: int, *, o: float, h: float, l: float, c: float) -> dict:
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c}


def _ts(iso: str) -> int:
    import datetime as dt
    return int(dt.datetime.fromisoformat(iso).timestamp())


def test_build_trade_snapshot_without_candles_skips_price_derived_fields():
    trade = _closed_trade(pnl=5.0)
    snap = pta.build_trade_snapshot(trade, candles=None)
    assert snap["outcome"] == "win"
    assert snap["ticker"] == "KXBTCPERP"
    assert "mfe_usd" not in snap
    assert snap["lesson"].startswith("KXBTCPERP: WIN")


def test_build_trade_snapshot_computes_mfe_mae_for_a_long_win():
    opened, closed = "2026-08-01T12:00:00+00:00", "2026-08-01T12:05:00+00:00"
    trade = _closed_trade(pnl=1.0, side="long", entry_price=100.0, exit_price=101.0, opened_at=opened, closed_at=closed)
    candles = [
        _candle(_ts(opened), o=100.0, h=100.5, l=99.8, c=100.2),
        _candle(_ts(opened) + 60, o=100.2, h=103.0, l=100.0, c=102.5),  # best favorable excursion: high=103
        _candle(_ts(closed), o=102.5, h=102.6, l=101.0, c=101.0),
    ]
    snap = pta.build_trade_snapshot(trade, candles=candles)
    assert snap["mfe_usd"] == 3.0  # 103 - 100 entry
    assert snap["mae_usd"] == 0.2  # 100 - 99.8
    assert snap["capture_ratio"] < 0.5  # realized 1.0 / mfe 3.0
    assert "captured only" in snap["lesson"]


def test_build_trade_snapshot_computes_mae_mfe_for_a_short():
    opened, closed = "2026-08-01T12:00:00+00:00", "2026-08-01T12:05:00+00:00"
    trade = _closed_trade(pnl=2.0, side="short", entry_price=100.0, exit_price=98.0, opened_at=opened, closed_at=closed)
    candles = [
        _candle(_ts(opened), o=100.0, h=101.0, l=99.0, c=99.5),
        _candle(_ts(closed), o=99.5, h=100.2, l=97.0, c=98.0),
    ]
    snap = pta.build_trade_snapshot(trade, candles=candles)
    # short: mfe = entry - min(low) = 100 - 97 = 3; mae = max(high) - entry = 101 - 100 = 1
    assert snap["mfe_usd"] == 3.0
    assert snap["mae_usd"] == 1.0


def test_build_trade_snapshot_detects_a_premature_stop_loss():
    opened, closed = "2026-08-01T12:00:00+00:00", "2026-08-01T12:05:00+00:00"
    trade = _closed_trade(
        pnl=-2.0, side="long", entry_price=100.0, exit_price=98.0, reason="stop_loss (-2%)",
        opened_at=opened, closed_at=closed, entry_score=0.55,
    )
    candles = [
        _candle(_ts(opened), o=100.0, h=100.2, l=99.0, c=99.5),
        _candle(_ts(closed), o=99.5, h=99.6, l=98.0, c=98.0),
        # Price reverses favorably right after the stop-out.
        _candle(_ts(closed) + 60, o=98.0, h=100.5, l=98.0, c=100.5),
    ]
    snap = pta.build_trade_snapshot(trade, candles=candles)
    assert snap["post_exit_drift_pct"] > 0
    assert "moved back in our favor" in snap["lesson"]


def test_build_trade_snapshot_flags_high_confidence_loss_without_reversal():
    trade = _closed_trade(pnl=-1.0, reason="stop_loss (-1%)", entry_score=0.7)
    snap = pta.build_trade_snapshot(trade, candles=None)
    assert "high entry confidence" in snap["lesson"]


def test_build_trade_snapshot_handles_a_flat_trade():
    trade = _closed_trade(pnl=0.0)
    snap = pta.build_trade_snapshot(trade, candles=None)
    assert snap["outcome"] == "flat"
    assert "flat" in snap["lesson"]


def test_analyze_recent_trade_batch_with_no_trades():
    result = pta.analyze_recent_trade_batch([])
    assert result["trades_analyzed"] == 0
    assert result["snapshots"] == []


def test_analyze_recent_trade_batch_only_uses_the_most_recent_n():
    trades = [_closed_trade(pnl=float(i), ticker=f"T{i}") for i in range(10)]
    result = pta.analyze_recent_trade_batch(trades, batch_size=5)
    assert result["trades_analyzed"] == 5
    assert [s["ticker"] for s in result["snapshots"]] == ["T5", "T6", "T7", "T8", "T9"]


def test_analyze_recent_trade_batch_excludes_dry_run_by_default():
    trades = [_closed_trade(pnl=1.0, dry_run=True), _closed_trade(pnl=-1.0, dry_run=False)]
    result = pta.analyze_recent_trade_batch(trades)
    assert result["trades_analyzed"] == 1


def test_analyze_recent_trade_batch_counts_wins_and_losses():
    trades = [
        _closed_trade(pnl=1.0, ticker="A"), _closed_trade(pnl=-1.0, ticker="B"),
        _closed_trade(pnl=2.0, ticker="C"), _closed_trade(pnl=-0.5, ticker="D"), _closed_trade(pnl=0.0, ticker="E"),
    ]
    result = pta.analyze_recent_trade_batch(trades)
    assert result["wins"] == 2
    assert result["losses"] == 2
    assert result["total_pnl_usd"] == 1.5


def test_analyze_recent_trade_batch_recommends_wider_stop_on_repeated_reversals():
    opened, closed = "2026-08-01T12:00:00+00:00", "2026-08-01T12:05:00+00:00"
    reversal_candles = [
        _candle(_ts(opened), o=100.0, h=100.2, l=99.0, c=99.5),
        _candle(_ts(closed), o=99.5, h=99.6, l=98.0, c=98.0),
        _candle(_ts(closed) + 60, o=98.0, h=101.0, l=98.0, c=101.0),
    ]
    trades = [
        _closed_trade(pnl=-2.0, ticker="A", reason="stop_loss (-2%)", opened_at=opened, closed_at=closed, exit_price=98.0),
        _closed_trade(pnl=-2.0, ticker="B", reason="stop_loss (-2%)", opened_at=opened, closed_at=closed, exit_price=98.0),
    ]
    candles_by_ticker = {"A": reversal_candles, "B": reversal_candles}
    result = pta.analyze_recent_trade_batch(trades, candles_by_ticker=candles_by_ticker)
    assert any("wider" in r or "volatility-scaled" in r for r in result["recommendations"])


def test_analyze_recent_trade_batch_recommends_trailing_tp_on_low_capture_wins():
    opened, closed = "2026-08-01T12:00:00+00:00", "2026-08-01T12:05:00+00:00"
    low_capture_candles = [
        _candle(_ts(opened), o=100.0, h=100.5, l=99.9, c=100.1),
        _candle(_ts(opened) + 60, o=100.1, h=110.0, l=100.0, c=105.0),
        _candle(_ts(closed), o=105.0, h=105.2, l=104.9, c=101.0),
    ]
    trades = [
        _closed_trade(pnl=1.0, ticker="A", opened_at=opened, closed_at=closed, exit_price=101.0),
        _closed_trade(pnl=1.0, ticker="B", opened_at=opened, closed_at=closed, exit_price=101.0),
    ]
    candles_by_ticker = {"A": low_capture_candles, "B": low_capture_candles}
    result = pta.analyze_recent_trade_batch(trades, candles_by_ticker=candles_by_ticker)
    assert any("trailing take-profit" in r for r in result["recommendations"])


def test_format_batch_snapshot_text_with_no_trades():
    text = pta.format_batch_snapshot_text({"trades_analyzed": 0})
    assert "no closed real trades" in text.lower()


def test_format_batch_snapshot_text_includes_each_trades_lesson():
    trades = [_closed_trade(pnl=1.0, ticker="A"), _closed_trade(pnl=-1.0, ticker="B", reason="stop_loss (-1%)")]
    batch = pta.analyze_recent_trade_batch(trades)
    text = pta.format_batch_snapshot_text(batch, market="perps")
    assert "A:" in text
    assert "B:" in text
    assert "2W" not in text  # sanity: not accidentally reusing analyze_trade_history's format
    assert "1W/1L" in text
