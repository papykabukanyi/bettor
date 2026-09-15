"""Post-trade win/loss analysis for Alpaca options. Synthetic trade_log
data only -- pure computation over dicts, no network/state/model involved.
Unlike the sibling perps/stocks/crypto modules, this one never computes
premium-dollar MFE/MAE (no historical option-premium series exists to
derive it from -- see alpaca_options_trade_analysis.py's own docstring);
coverage here specifically confirms that constraint is respected (no
mfe_usd/capture_ratio ever appears) while the underlying-based directional
drift signal still works."""
from __future__ import annotations

import datetime as dt

from data import alpaca_options_trade_analysis as aota


def _closed_trade(
    *, pnl: float, option_type: str = "call", reason: str = "take_profit (+2%)",
    opened_at: str = "2026-08-01T12:00:00+00:00", closed_at: str = "2026-08-01T12:10:00+00:00",
    symbol: str = "AAPL240223C00195000", underlying_symbol: str = "AAPL",
    entry_probability_up: float | None = 0.6, dry_run: bool = False, entry_score: float | None = 0.6,
) -> dict:
    return {
        "symbol": symbol, "underlying_symbol": underlying_symbol, "option_type": option_type,
        "realized_pnl_usd": pnl, "reason": reason, "dry_run": dry_run,
        "opened_at": opened_at, "closed_at": closed_at,
        "entry_probability_up": entry_probability_up, "hold_minutes": 10.0, "entry_score": entry_score,
    }


def _candle(ts: int, *, o: float, h: float, l: float, c: float) -> dict:
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c}


def _ts(iso: str) -> int:
    return int(dt.datetime.fromisoformat(iso).timestamp())


def test_build_trade_snapshot_never_produces_a_premium_dollar_mfe():
    """The core constraint this module exists to respect -- no historical
    premium series means no dollar-scale excursion figure, ever, even with
    real underlying candles supplied."""
    opened, closed = "2026-08-01T12:00:00+00:00", "2026-08-01T12:05:00+00:00"
    trade = _closed_trade(pnl=50.0, opened_at=opened, closed_at=closed)
    candles = [
        _candle(_ts(opened), o=190.0, h=191.0, l=189.5, c=190.5),
        _candle(_ts(closed), o=190.5, h=192.0, l=190.0, c=191.5),
    ]
    snap = aota.build_trade_snapshot(trade, underlying_candles=candles)
    assert "mfe_usd" not in snap
    assert "mae_usd" not in snap
    assert "capture_ratio" not in snap


def test_build_trade_snapshot_without_candles_skips_drift_field():
    snap = aota.build_trade_snapshot(_closed_trade(pnl=5.0), underlying_candles=None)
    assert snap["outcome"] == "win"
    assert "underlying_post_exit_drift_pct" not in snap
    assert snap["lesson"] == "AAPL240223C00195000: WIN $5.00."


def test_build_trade_snapshot_detects_a_premature_exit_on_a_call_loss():
    """A call loses; the underlying keeps rising after we closed --
    favorable drift for a call (positive raw price change)."""
    opened, closed = "2026-08-01T12:00:00+00:00", "2026-08-01T12:05:00+00:00"
    trade = _closed_trade(pnl=-50.0, option_type="call", reason="stop_loss (-1%)", opened_at=opened, closed_at=closed, entry_probability_up=0.55)
    candles = [
        _candle(_ts(opened), o=190.0, h=190.2, l=189.0, c=189.5),
        _candle(_ts(closed), o=189.5, h=189.6, l=189.0, c=189.0),
        _candle(_ts(closed) + 60, o=189.0, h=192.0, l=189.0, c=192.0),
    ]
    snap = aota.build_trade_snapshot(trade, underlying_candles=candles)
    assert snap["underlying_post_exit_drift_pct"] > 0
    assert "kept moving" in snap["lesson"]


def test_build_trade_snapshot_detects_a_premature_exit_on_a_put_loss():
    """A put loses; the underlying keeps FALLING after we closed --
    favorable drift for a put (sign-flipped from the raw price change)."""
    opened, closed = "2026-08-01T12:00:00+00:00", "2026-08-01T12:05:00+00:00"
    trade = _closed_trade(pnl=-50.0, option_type="put", reason="stop_loss (-1%)", opened_at=opened, closed_at=closed, entry_probability_up=0.35)
    candles = [
        _candle(_ts(opened), o=190.0, h=190.5, l=189.5, c=190.0),
        _candle(_ts(closed), o=190.0, h=190.2, l=189.8, c=190.0),
        _candle(_ts(closed) + 60, o=190.0, h=190.0, l=187.0, c=187.0),
    ]
    snap = aota.build_trade_snapshot(trade, underlying_candles=candles)
    assert snap["underlying_post_exit_drift_pct"] > 0
    assert "kept moving" in snap["lesson"]


def test_build_trade_snapshot_flags_high_confidence_loss_without_reversal():
    trade = _closed_trade(pnl=-50.0, reason="stop_loss (-1%)", entry_probability_up=0.7)
    snap = aota.build_trade_snapshot(trade, underlying_candles=None)
    assert "high entry model confidence" in snap["lesson"]


def test_build_trade_snapshot_handles_a_flat_trade():
    snap = aota.build_trade_snapshot(_closed_trade(pnl=0.0), underlying_candles=None)
    assert snap["outcome"] == "flat"
    assert "flat" in snap["lesson"]


def test_analyze_recent_trade_batch_with_no_trades():
    result = aota.analyze_recent_trade_batch([])
    assert result["trades_analyzed"] == 0
    assert result["snapshots"] == []


def test_analyze_recent_trade_batch_only_uses_the_most_recent_n():
    trades = [_closed_trade(pnl=float(i), symbol=f"T{i}") for i in range(10)]
    result = aota.analyze_recent_trade_batch(trades, batch_size=5)
    assert result["trades_analyzed"] == 5
    assert [s["symbol"] for s in result["snapshots"]] == ["T5", "T6", "T7", "T8", "T9"]


def test_analyze_recent_trade_batch_excludes_dry_run_by_default():
    trades = [_closed_trade(pnl=1.0, dry_run=True), _closed_trade(pnl=-1.0, dry_run=False)]
    result = aota.analyze_recent_trade_batch(trades)
    assert result["trades_analyzed"] == 1


def test_analyze_recent_trade_batch_counts_wins_and_losses():
    trades = [
        _closed_trade(pnl=1.0, symbol="A"), _closed_trade(pnl=-1.0, symbol="B"),
        _closed_trade(pnl=2.0, symbol="C"), _closed_trade(pnl=-0.5, symbol="D"), _closed_trade(pnl=0.0, symbol="E"),
    ]
    result = aota.analyze_recent_trade_batch(trades)
    assert result["wins"] == 2
    assert result["losses"] == 2
    assert result["total_pnl_usd"] == 1.5


def test_format_batch_snapshot_text_with_no_trades():
    assert "no closed real trades" in aota.format_batch_snapshot_text({"trades_analyzed": 0}).lower()


def test_format_batch_snapshot_text_includes_each_trades_lesson():
    trades = [_closed_trade(pnl=1.0, symbol="A"), _closed_trade(pnl=-1.0, symbol="B", reason="stop_loss (-1%)")]
    batch = aota.analyze_recent_trade_batch(trades)
    text = aota.format_batch_snapshot_text(batch, market="options")
    assert "A:" in text and "B:" in text
    assert "1W/1L" in text


def test_recommend_confidence_threshold_insufficient_history_does_not_apply():
    trades = [_closed_trade(pnl=1.0, entry_score=0.58) for _ in range(3)]
    result = aota.recommend_confidence_threshold(trades, current_threshold=0.58)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"


def test_recommend_confidence_threshold_recommends_when_evidence_supports_it():
    trades = []
    for _ in range(aota.CONFIDENCE_TUNING_MIN_TRADES):
        trades.append(_closed_trade(pnl=-0.2, entry_score=0.59))  # loses, below the higher band
    for _ in range(aota.CONFIDENCE_TUNING_MIN_TRADES):
        trades.append(_closed_trade(pnl=0.5, entry_score=0.65))  # wins, clears the higher band too
    result = aota.recommend_confidence_threshold(trades, current_threshold=0.58)
    assert result["should_apply"] is True
    assert result["recommended_threshold"] > 0.58
    assert result["recommended_threshold"] <= round(0.58 + aota.CONFIDENCE_TUNING_MAX_STEP, 4)


def test_recommend_confidence_threshold_does_not_apply_without_a_clear_improvement():
    trades = [_closed_trade(pnl=0.1, entry_score=score) for score in [0.59, 0.63, 0.67, 0.71] for _ in range(10)]
    result = aota.recommend_confidence_threshold(trades, current_threshold=0.58)
    assert result["should_apply"] is False


def test_recommend_confidence_threshold_ignores_dry_run_trades():
    trades = [_closed_trade(pnl=5.0, entry_score=0.65, dry_run=True) for _ in range(aota.CONFIDENCE_TUNING_MIN_TRADES)]
    result = aota.recommend_confidence_threshold(trades, current_threshold=0.58)
    assert result["should_apply"] is False
    assert result["reason"] == "insufficient_trade_history"


# ── recommend_confidence_from_backtest / backtest_shows_a_loss -- the
# user's own explicit request: "whenever you get negative return on
# backtest and forward test[,] need to automatically improve" applied to
# options too ("get in more trades and able to get it by itself"). Mirrors
# test_alpaca_crypto_trade_analysis.py's own identical coverage, adapted
# to alpaca_options_backtest.run_config_sweep's own "all_configs" key
# (not "configs") -- see recommend_confidence_from_backtest's own
# docstring for why. ───────────────────────────────────────────────────

def _sweep_config(label, *, return_pct=None, trade_count=50, low_sample=False, model_confidence_min=None):
    cfg = {"trade_count": trade_count, "return_pct": return_pct, "low_sample": low_sample}
    if label is not None:
        cfg["label"] = label
    if model_confidence_min is not None:
        cfg["model_confidence_min"] = model_confidence_min
    return cfg


def test_recommend_confidence_from_backtest_with_no_sweep_data_does_not_apply():
    result = aota.recommend_confidence_from_backtest(None, current_threshold=0.53)
    assert result["should_apply"] is False
    assert result["reason"] == "no_sweep_data"


def test_recommend_confidence_from_backtest_finds_a_meaningfully_better_variant():
    sweep = {"all_configs": [
        _sweep_config("current_defaults", return_pct=-0.137, model_confidence_min=0.53),
        _sweep_config(None, return_pct=0.05, model_confidence_min=0.62),
    ]}
    result = aota.recommend_confidence_from_backtest(sweep, current_threshold=0.53)
    assert result["should_apply"] is True
    assert result["recommended_threshold"] == 0.62


def test_recommend_confidence_from_backtest_ignores_a_low_sample_variant():
    sweep = {"all_configs": [
        _sweep_config("current_defaults", return_pct=-0.137, model_confidence_min=0.53),
        _sweep_config(None, return_pct=0.5, model_confidence_min=0.62, low_sample=True),
    ]}
    result = aota.recommend_confidence_from_backtest(sweep, current_threshold=0.53)
    assert result["should_apply"] is False
    assert result["reason"] == "no_meaningfully_better_variant"


def test_recommend_confidence_from_backtest_ignores_a_negative_variant():
    sweep = {"all_configs": [
        _sweep_config("current_defaults", return_pct=-0.137, model_confidence_min=0.53),
        _sweep_config(None, return_pct=-0.05, model_confidence_min=0.62),
    ]}
    result = aota.recommend_confidence_from_backtest(sweep, current_threshold=0.53)
    assert result["should_apply"] is False


def test_recommend_confidence_from_backtest_requires_a_real_margin_not_just_any_improvement():
    sweep = {"all_configs": [
        _sweep_config("current_defaults", return_pct=0.01, model_confidence_min=0.53),
        _sweep_config(None, return_pct=0.015, model_confidence_min=0.62),  # < 2pp margin
    ]}
    result = aota.recommend_confidence_from_backtest(sweep, current_threshold=0.53)
    assert result["should_apply"] is False


def test_recommend_confidence_from_backtest_picks_the_best_of_several_candidates():
    sweep = {"all_configs": [
        _sweep_config("current_defaults", return_pct=-0.10, model_confidence_min=0.53),
        _sweep_config(None, return_pct=0.02, model_confidence_min=0.58),
        _sweep_config(None, return_pct=0.08, model_confidence_min=0.65),
    ]}
    result = aota.recommend_confidence_from_backtest(sweep, current_threshold=0.53)
    assert result["should_apply"] is True
    assert result["recommended_threshold"] == 0.65


def test_backtest_shows_a_loss_true_from_a_losing_sweep():
    sweep = {"all_configs": [_sweep_config("current_defaults", return_pct=-0.137)]}
    result = aota.backtest_shows_a_loss(sweep, None)
    assert result["is_loss"] is True
    assert any("sweep" in r for r in result["reasons"])


def test_backtest_shows_a_loss_true_from_a_losing_walkforward():
    result = aota.backtest_shows_a_loss(None, {"mean_return_pct": -0.03})
    assert result["is_loss"] is True
    assert any("walk-forward" in r for r in result["reasons"])


def test_backtest_shows_a_loss_false_when_both_are_profitable():
    sweep = {"all_configs": [_sweep_config("current_defaults", return_pct=0.05)]}
    result = aota.backtest_shows_a_loss(sweep, {"mean_return_pct": 0.02})
    assert result["is_loss"] is False


def test_backtest_shows_a_loss_false_with_no_data_at_all():
    result = aota.backtest_shows_a_loss(None, None)
    assert result["is_loss"] is False
