"""Post-trade win/loss analysis for Alpaca stocks. Synthetic trade_log data
only -- pure computation over dicts, no network/state/model involved. See
tests/test_perps_trade_analysis.py for the sibling perps coverage this
mirrors."""
from __future__ import annotations

import datetime as dt

from data import alpaca_trade_analysis as ata


def _closed_trade(
    *, pnl: float, entry_price: float = 100.0, exit_price: float = 101.0,
    reason: str = "take_profit (+2%)", opened_at: str = "2026-08-01T12:00:00+00:00",
    closed_at: str = "2026-08-01T12:10:00+00:00", symbol: str = "AAPL",
    entry_probability_up: float | None = 0.6, dry_run: bool = False, entry_score: float | None = 0.6,
) -> dict:
    return {
        "symbol": symbol, "realized_pnl_usd": pnl, "reason": reason, "dry_run": dry_run,
        "entry_price": entry_price, "exit_price": exit_price, "opened_at": opened_at, "closed_at": closed_at,
        "entry_probability_up": entry_probability_up, "hold_minutes": 10.0, "entry_score": entry_score,
    }


def _candle(ts: int, *, o: float, h: float, l: float, c: float) -> dict:
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c}


def _ts(iso: str) -> int:
    return int(dt.datetime.fromisoformat(iso).timestamp())


def test_build_trade_snapshot_without_candles_skips_price_derived_fields():
    snap = ata.build_trade_snapshot(_closed_trade(pnl=5.0), candles=None)
    assert snap["outcome"] == "win"
    assert snap["symbol"] == "AAPL"
    assert "mfe_usd" not in snap
    assert snap["lesson"].startswith("AAPL: WIN")


def test_build_trade_snapshot_computes_mfe_mae():
    opened, closed = "2026-08-01T12:00:00+00:00", "2026-08-01T12:05:00+00:00"
    trade = _closed_trade(pnl=1.0, entry_price=100.0, exit_price=101.0, opened_at=opened, closed_at=closed)
    candles = [
        _candle(_ts(opened), o=100.0, h=100.5, l=99.8, c=100.2),
        _candle(_ts(opened) + 60, o=100.2, h=103.0, l=100.0, c=102.5),
        _candle(_ts(closed), o=102.5, h=102.6, l=101.0, c=101.0),
    ]
    snap = ata.build_trade_snapshot(trade, candles=candles)
    assert snap["mfe_usd"] == 3.0
    assert snap["mae_usd"] == 0.2
    assert snap["capture_ratio"] < 0.5
    assert "captured only" in snap["lesson"]


def test_build_trade_snapshot_detects_a_premature_stop_loss():
    opened, closed = "2026-08-01T12:00:00+00:00", "2026-08-01T12:05:00+00:00"
    trade = _closed_trade(pnl=-2.0, entry_price=100.0, exit_price=98.0, reason="stop_loss (-2%)", opened_at=opened, closed_at=closed, entry_probability_up=0.55)
    candles = [
        _candle(_ts(opened), o=100.0, h=100.2, l=99.0, c=99.5),
        _candle(_ts(closed), o=99.5, h=99.6, l=98.0, c=98.0),
        _candle(_ts(closed) + 60, o=98.0, h=100.5, l=98.0, c=100.5),
    ]
    snap = ata.build_trade_snapshot(trade, candles=candles)
    assert snap["post_exit_drift_pct"] > 0
    assert "moved back in our favor" in snap["lesson"]


def test_build_trade_snapshot_flags_high_confidence_loss_without_reversal():
    trade = _closed_trade(pnl=-1.0, reason="stop_loss (-1%)", entry_probability_up=0.7)
    snap = ata.build_trade_snapshot(trade, candles=None)
    assert "high entry model confidence" in snap["lesson"]


def test_build_trade_snapshot_handles_a_flat_trade():
    snap = ata.build_trade_snapshot(_closed_trade(pnl=0.0), candles=None)
    assert snap["outcome"] == "flat"
    assert "flat" in snap["lesson"]


def test_analyze_recent_trade_batch_with_no_trades():
    result = ata.analyze_recent_trade_batch([])
    assert result["trades_analyzed"] == 0
    assert result["snapshots"] == []


def test_analyze_recent_trade_batch_only_uses_the_most_recent_n():
    trades = [_closed_trade(pnl=float(i), symbol=f"T{i}") for i in range(10)]
    result = ata.analyze_recent_trade_batch(trades, batch_size=5)
    assert result["trades_analyzed"] == 5
    assert [s["symbol"] for s in result["snapshots"]] == ["T5", "T6", "T7", "T8", "T9"]


def test_analyze_recent_trade_batch_excludes_dry_run_by_default():
    trades = [_closed_trade(pnl=1.0, dry_run=True), _closed_trade(pnl=-1.0, dry_run=False)]
    result = ata.analyze_recent_trade_batch(trades)
    assert result["trades_analyzed"] == 1


def test_analyze_recent_trade_batch_counts_wins_and_losses():
    trades = [
        _closed_trade(pnl=1.0, symbol="A"), _closed_trade(pnl=-1.0, symbol="B"),
        _closed_trade(pnl=2.0, symbol="C"), _closed_trade(pnl=-0.5, symbol="D"), _closed_trade(pnl=0.0, symbol="E"),
    ]
    result = ata.analyze_recent_trade_batch(trades)
    assert result["wins"] == 2
    assert result["losses"] == 2
    assert result["total_pnl_usd"] == 1.5


def test_analyze_recent_trade_batch_recommends_trailing_tp_on_low_capture_wins():
    opened, closed = "2026-08-01T12:00:00+00:00", "2026-08-01T12:05:00+00:00"
    low_capture_candles = [
        _candle(_ts(opened), o=100.0, h=100.5, l=99.9, c=100.1),
        _candle(_ts(opened) + 60, o=100.1, h=110.0, l=100.0, c=105.0),
        _candle(_ts(closed), o=105.0, h=105.2, l=104.9, c=101.0),
    ]
    trades = [
        _closed_trade(pnl=1.0, symbol="A", opened_at=opened, closed_at=closed, exit_price=101.0),
        _closed_trade(pnl=1.0, symbol="B", opened_at=opened, closed_at=closed, exit_price=101.0),
    ]
    candles_by_symbol = {"A": low_capture_candles, "B": low_capture_candles}
    result = ata.analyze_recent_trade_batch(trades, candles_by_symbol=candles_by_symbol)
    assert any("trailing take-profit" in r for r in result["recommendations"])


def test_format_batch_snapshot_text_with_no_trades():
    assert "no closed real trades" in ata.format_batch_snapshot_text({"trades_analyzed": 0}).lower()


def test_format_batch_snapshot_text_includes_each_trades_lesson():
    trades = [_closed_trade(pnl=1.0, symbol="A"), _closed_trade(pnl=-1.0, symbol="B", reason="stop_loss (-1%)")]
    batch = ata.analyze_recent_trade_batch(trades)
    text = ata.format_batch_snapshot_text(batch, market="stocks")
    assert "A:" in text and "B:" in text
    assert "1W/1L" in text


def test_recommend_confidence_threshold_insufficient_history_does_not_apply():
    trades = [_closed_trade(pnl=1.0, entry_score=0.6) for _ in range(3)]
    result = ata.recommend_confidence_threshold(trades, current_threshold=0.55)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"


def test_recommend_confidence_threshold_recommends_when_evidence_supports_it():
    trades = []
    for _ in range(ata.CONFIDENCE_TUNING_MIN_TRADES):
        trades.append(_closed_trade(pnl=-0.2, entry_score=0.56))  # loses, below the higher band
    for _ in range(ata.CONFIDENCE_TUNING_MIN_TRADES):
        trades.append(_closed_trade(pnl=0.5, entry_score=0.62))  # wins, clears the higher band too
    result = ata.recommend_confidence_threshold(trades, current_threshold=0.55)
    assert result["should_apply"] is True
    assert result["recommended_threshold"] > 0.55
    assert result["recommended_threshold"] <= round(0.55 + ata.CONFIDENCE_TUNING_MAX_STEP, 4)


def test_recommend_confidence_threshold_does_not_apply_without_a_clear_improvement():
    trades = [_closed_trade(pnl=0.1, entry_score=score) for score in [0.56, 0.60, 0.64, 0.68] for _ in range(10)]
    result = ata.recommend_confidence_threshold(trades, current_threshold=0.55)
    assert result["should_apply"] is False


def test_recommend_confidence_threshold_ignores_dry_run_trades():
    trades = [_closed_trade(pnl=5.0, entry_score=0.62, dry_run=True) for _ in range(ata.CONFIDENCE_TUNING_MIN_TRADES)]
    result = ata.recommend_confidence_threshold(trades, current_threshold=0.55)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"
