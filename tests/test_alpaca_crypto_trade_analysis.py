"""Post-trade win/loss analysis for Alpaca crypto. Synthetic trade_log data
only -- pure computation over dicts, no network/state/model involved. See
tests/test_perps_trade_analysis.py for the sibling perps coverage this
mirrors."""
from __future__ import annotations

import datetime as dt

from data import alpaca_crypto_trade_analysis as acta


def _closed_trade(
    *, pnl: float, entry_price: float = 65000.0, exit_price: float = 65500.0,
    reason: str = "take_profit (+2%)", opened_at: str = "2026-08-01T12:00:00+00:00",
    closed_at: str = "2026-08-01T12:10:00+00:00", symbol: str = "BTC/USD",
    entry_probability_up: float | None = 0.6, dry_run: bool = False, entry_score: float | None = 0.6,
    entry_correlation_score: float | None = None,
) -> dict:
    return {
        "symbol": symbol, "realized_pnl_usd": pnl, "reason": reason, "dry_run": dry_run,
        "entry_price": entry_price, "exit_price": exit_price, "opened_at": opened_at, "closed_at": closed_at,
        "entry_probability_up": entry_probability_up, "hold_minutes": 10.0, "entry_score": entry_score,
        "entry_correlation_score": entry_correlation_score,
    }


def _candle(ts: int, *, o: float, h: float, l: float, c: float) -> dict:
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c}


def _ts(iso: str) -> int:
    return int(dt.datetime.fromisoformat(iso).timestamp())


def test_build_trade_snapshot_without_candles_skips_price_derived_fields():
    snap = acta.build_trade_snapshot(_closed_trade(pnl=50.0), candles=None)
    assert snap["outcome"] == "win"
    assert snap["symbol"] == "BTC/USD"
    assert "mfe_usd" not in snap
    assert snap["lesson"].startswith("BTC/USD: WIN")


def test_build_trade_snapshot_computes_mfe_mae():
    opened, closed = "2026-08-01T12:00:00+00:00", "2026-08-01T12:05:00+00:00"
    trade = _closed_trade(pnl=100.0, entry_price=65000.0, exit_price=65500.0, opened_at=opened, closed_at=closed)
    candles = [
        _candle(_ts(opened), o=65000.0, h=65100.0, l=64900.0, c=65050.0),
        _candle(_ts(opened) + 60, o=65050.0, h=66000.0, l=65000.0, c=65800.0),
        _candle(_ts(closed), o=65800.0, h=65820.0, l=65400.0, c=65500.0),
    ]
    snap = acta.build_trade_snapshot(trade, candles=candles)
    assert snap["mfe_usd"] == 1000.0
    assert snap["mae_usd"] == 100.0
    assert snap["capture_ratio"] < 0.5
    assert "captured only" in snap["lesson"]


def test_build_trade_snapshot_detects_a_premature_stop_loss():
    opened, closed = "2026-08-01T12:00:00+00:00", "2026-08-01T12:05:00+00:00"
    trade = _closed_trade(pnl=-200.0, entry_price=65000.0, exit_price=64500.0, reason="stop_loss (-2%)", opened_at=opened, closed_at=closed, entry_probability_up=0.55)
    candles = [
        _candle(_ts(opened), o=65000.0, h=65100.0, l=64800.0, c=64900.0),
        _candle(_ts(closed), o=64900.0, h=64950.0, l=64500.0, c=64500.0),
        _candle(_ts(closed) + 60, o=64500.0, h=65600.0, l=64500.0, c=65600.0),
    ]
    snap = acta.build_trade_snapshot(trade, candles=candles)
    assert snap["post_exit_drift_pct"] > 0
    assert "moved back in our favor" in snap["lesson"]


def test_build_trade_snapshot_flags_high_confidence_loss_without_reversal():
    trade = _closed_trade(pnl=-50.0, reason="stop_loss (-1%)", entry_probability_up=0.7)
    snap = acta.build_trade_snapshot(trade, candles=None)
    assert "high entry model confidence" in snap["lesson"]


def test_build_trade_snapshot_handles_a_flat_trade():
    snap = acta.build_trade_snapshot(_closed_trade(pnl=0.0), candles=None)
    assert snap["outcome"] == "flat"
    assert "flat" in snap["lesson"]


def test_analyze_recent_trade_batch_with_no_trades():
    result = acta.analyze_recent_trade_batch([])
    assert result["trades_analyzed"] == 0
    assert result["snapshots"] == []


def test_analyze_recent_trade_batch_only_uses_the_most_recent_n():
    trades = [_closed_trade(pnl=float(i), symbol=f"T{i}/USD") for i in range(10)]
    result = acta.analyze_recent_trade_batch(trades, batch_size=5)
    assert result["trades_analyzed"] == 5
    assert [s["symbol"] for s in result["snapshots"]] == ["T5/USD", "T6/USD", "T7/USD", "T8/USD", "T9/USD"]


def test_analyze_recent_trade_batch_excludes_dry_run_by_default():
    trades = [_closed_trade(pnl=1.0, dry_run=True), _closed_trade(pnl=-1.0, dry_run=False)]
    result = acta.analyze_recent_trade_batch(trades)
    assert result["trades_analyzed"] == 1


def test_analyze_recent_trade_batch_counts_wins_and_losses():
    trades = [
        _closed_trade(pnl=1.0, symbol="A/USD"), _closed_trade(pnl=-1.0, symbol="B/USD"),
        _closed_trade(pnl=2.0, symbol="C/USD"), _closed_trade(pnl=-0.5, symbol="D/USD"), _closed_trade(pnl=0.0, symbol="E/USD"),
    ]
    result = acta.analyze_recent_trade_batch(trades)
    assert result["wins"] == 2
    assert result["losses"] == 2
    assert result["total_pnl_usd"] == 1.5


def test_format_batch_snapshot_text_with_no_trades():
    assert "no closed real trades" in acta.format_batch_snapshot_text({"trades_analyzed": 0}).lower()


def test_format_batch_snapshot_text_includes_each_trades_lesson():
    trades = [_closed_trade(pnl=1.0, symbol="A/USD"), _closed_trade(pnl=-1.0, symbol="B/USD", reason="stop_loss (-1%)")]
    batch = acta.analyze_recent_trade_batch(trades)
    text = acta.format_batch_snapshot_text(batch, market="crypto")
    assert "A/USD:" in text and "B/USD:" in text
    assert "1W/1L" in text


def test_recommend_confidence_threshold_insufficient_history_does_not_apply():
    trades = [_closed_trade(pnl=1.0, entry_score=0.55) for _ in range(3)]
    result = acta.recommend_confidence_threshold(trades, current_threshold=0.55)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"


def test_recommend_confidence_threshold_recommends_when_evidence_supports_it():
    trades = []
    for _ in range(acta.CONFIDENCE_TUNING_MIN_TRADES):
        trades.append(_closed_trade(pnl=-0.2, entry_score=0.56))  # loses, below the higher band
    for _ in range(acta.CONFIDENCE_TUNING_MIN_TRADES):
        trades.append(_closed_trade(pnl=0.5, entry_score=0.62))  # wins, clears the higher band too
    result = acta.recommend_confidence_threshold(trades, current_threshold=0.55)
    assert result["should_apply"] is True
    assert result["recommended_threshold"] > 0.55
    assert result["recommended_threshold"] <= round(0.55 + acta.CONFIDENCE_TUNING_MAX_STEP, 4)


def test_recommend_confidence_threshold_does_not_apply_without_a_clear_improvement():
    trades = [_closed_trade(pnl=0.1, entry_score=score) for score in [0.56, 0.60, 0.64, 0.68] for _ in range(10)]
    result = acta.recommend_confidence_threshold(trades, current_threshold=0.55)
    assert result["should_apply"] is False


def test_recommend_confidence_threshold_ignores_dry_run_trades():
    trades = [_closed_trade(pnl=5.0, entry_score=0.62, dry_run=True) for _ in range(acta.CONFIDENCE_TUNING_MIN_TRADES)]
    result = acta.recommend_confidence_threshold(trades, current_threshold=0.55)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"


# ── recommend_correlation_study_weight -- see test_perps_trade_analysis.py's
# sibling coverage for the full rationale; identical logic here, long-only
# (no side-flip needed, see this module's own docstring).

def test_recommend_correlation_study_weight_insufficient_history_does_not_apply():
    trades = [_closed_trade(pnl=1.0, entry_correlation_score=0.5) for _ in range(3)]
    result = acta.recommend_correlation_study_weight(trades, current_enabled=False, current_max_adjustment=0.06)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"


def test_recommend_correlation_study_weight_recommends_enabling_when_agreement_outperforms():
    trades = []
    for _ in range(acta.CORRELATION_TUNING_MIN_TRADES):
        trades.append(_closed_trade(pnl=1.0, entry_correlation_score=0.8))
    for _ in range(acta.CORRELATION_TUNING_MIN_TRADES):
        trades.append(_closed_trade(pnl=-0.5, entry_correlation_score=0.0))
    result = acta.recommend_correlation_study_weight(trades, current_enabled=False, current_max_adjustment=0.06)
    assert result["should_apply"] is True
    assert result["action"] == "enable"


def test_recommend_correlation_study_weight_increases_the_weight_when_already_enabled_and_working():
    trades = []
    for _ in range(acta.CORRELATION_TUNING_MIN_TRADES):
        trades.append(_closed_trade(pnl=1.0, entry_correlation_score=0.8))
    for _ in range(acta.CORRELATION_TUNING_MIN_TRADES):
        trades.append(_closed_trade(pnl=-0.5, entry_correlation_score=0.0))
    result = acta.recommend_correlation_study_weight(trades, current_enabled=True, current_max_adjustment=0.06)
    assert result["should_apply"] is True
    assert result["action"] == "increase_weight"
    assert result["recommended_max_adjustment"] == round(0.06 + acta.CORRELATION_TUNING_MAX_STEP, 4)


def test_recommend_correlation_study_weight_recommends_disabling_when_it_actively_hurts():
    trades = []
    for _ in range(acta.CORRELATION_TUNING_MIN_TRADES):
        trades.append(_closed_trade(pnl=-0.5, entry_correlation_score=0.8))
    for _ in range(acta.CORRELATION_TUNING_MIN_TRADES):
        trades.append(_closed_trade(pnl=1.0, entry_correlation_score=0.0))
    result = acta.recommend_correlation_study_weight(trades, current_enabled=True, current_max_adjustment=0.06)
    assert result["should_apply"] is True
    assert result["action"] == "disable"


def test_recommend_correlation_study_weight_ignores_dry_run_trades():
    trades = [
        _closed_trade(pnl=5.0, entry_correlation_score=0.8, dry_run=True) for _ in range(acta.CORRELATION_TUNING_MIN_TRADES * 2)
    ]
    result = acta.recommend_correlation_study_weight(trades, current_enabled=False, current_max_adjustment=0.06)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"


# ── recommend_confidence_from_backtest / backtest_shows_a_loss -- the
# user's own explicit request: "whenever you get negative return on
# backtest and forward test[,] need to automatically improve" the crypto
# side. See alpaca_crypto_strategy.maybe_auto_improve_from_backtest's own
# docstring for the full design and why TP/SL are deliberately NOT also
# auto-tuned here. ───────────────────────────────────────────────────────

def _sweep_config(label, *, return_pct=None, trade_count=50, low_sample=False, model_confidence_min=None):
    cfg = {"label": label, "trade_count": trade_count, "win_rate": 0.5, "return_pct": return_pct, "low_sample": low_sample}
    if model_confidence_min is not None:
        cfg["model_confidence_min"] = model_confidence_min
    return cfg


def test_recommend_confidence_from_backtest_with_no_sweep_data_does_not_apply():
    result = acta.recommend_confidence_from_backtest(None, current_threshold=0.55)
    assert result["should_apply"] is False
    assert result["reason"] == "no_sweep_data"


def test_recommend_confidence_from_backtest_finds_a_meaningfully_better_variant():
    sweep = {"configs": [
        _sweep_config("current_defaults", return_pct=-0.137, model_confidence_min=0.55),
        _sweep_config("higher_confidence_only", return_pct=0.05, model_confidence_min=0.62),
    ]}
    result = acta.recommend_confidence_from_backtest(sweep, current_threshold=0.55)
    assert result["should_apply"] is True
    assert result["recommended_threshold"] == 0.62


def test_recommend_confidence_from_backtest_ignores_a_low_sample_variant():
    sweep = {"configs": [
        _sweep_config("current_defaults", return_pct=-0.137, model_confidence_min=0.55),
        _sweep_config("higher_confidence_only", return_pct=0.5, model_confidence_min=0.62, low_sample=True),
    ]}
    result = acta.recommend_confidence_from_backtest(sweep, current_threshold=0.55)
    assert result["should_apply"] is False
    assert result["reason"] == "no_meaningfully_better_variant"


def test_recommend_confidence_from_backtest_ignores_a_negative_variant():
    sweep = {"configs": [
        _sweep_config("current_defaults", return_pct=-0.137, model_confidence_min=0.55),
        _sweep_config("also_bad", return_pct=-0.05, model_confidence_min=0.62),
    ]}
    result = acta.recommend_confidence_from_backtest(sweep, current_threshold=0.55)
    assert result["should_apply"] is False


def test_recommend_confidence_from_backtest_requires_a_real_margin_not_just_any_improvement():
    sweep = {"configs": [
        _sweep_config("current_defaults", return_pct=0.01, model_confidence_min=0.55),
        _sweep_config("barely_better", return_pct=0.015, model_confidence_min=0.62),  # < 2pp margin
    ]}
    result = acta.recommend_confidence_from_backtest(sweep, current_threshold=0.55)
    assert result["should_apply"] is False


def test_recommend_confidence_from_backtest_picks_the_best_of_several_candidates():
    sweep = {"configs": [
        _sweep_config("current_defaults", return_pct=-0.10, model_confidence_min=0.55),
        _sweep_config("ok", return_pct=0.02, model_confidence_min=0.60),
        _sweep_config("better", return_pct=0.08, model_confidence_min=0.65),
    ]}
    result = acta.recommend_confidence_from_backtest(sweep, current_threshold=0.55)
    assert result["should_apply"] is True
    assert result["recommended_threshold"] == 0.65


def test_backtest_shows_a_loss_true_from_a_losing_sweep():
    sweep = {"configs": [_sweep_config("current_defaults", return_pct=-0.137)]}
    result = acta.backtest_shows_a_loss(sweep, None)
    assert result["is_loss"] is True
    assert any("sweep" in r for r in result["reasons"])


def test_backtest_shows_a_loss_true_from_a_losing_walkforward():
    result = acta.backtest_shows_a_loss(None, {"mean_return_pct": -0.03})
    assert result["is_loss"] is True
    assert any("walk-forward" in r for r in result["reasons"])


def test_backtest_shows_a_loss_false_when_both_are_profitable():
    sweep = {"configs": [_sweep_config("current_defaults", return_pct=0.05)]}
    result = acta.backtest_shows_a_loss(sweep, {"mean_return_pct": 0.02})
    assert result["is_loss"] is False


def test_backtest_shows_a_loss_false_with_no_data_at_all():
    result = acta.backtest_shows_a_loss(None, None)
    assert result["is_loss"] is False
