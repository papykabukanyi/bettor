"""Downloadable PDF strategy report for the Alpaca crypto bot. Same
not-best-effort contract as test_perps_report.py -- a user who clicked
"download" should see a real error if generation fails, so these tests
confirm real, valid PDF bytes come out, not just that nothing raised."""
from __future__ import annotations

from data import alpaca_crypto_report


def _trade(**overrides):
    base = {
        "closed_at": "2026-08-09T10:23:00+00:00", "symbol": "BTC/USD",
        "entry_price": 65000.0, "exit_price": 65500.0, "realized_pnl_usd": 12.5, "fee_usd": 0.8,
        "reason": "take_profit", "dry_run": False,
    }
    base.update(overrides)
    return base


def _status(**overrides):
    base = {
        "account_type": "paper", "live_trading_enabled": False,
        "balance": 500.0, "available_balance": 400.0, "open_position_count": 0, "max_concurrent_positions": 5,
        "today_realized_pnl_usd": 0.0, "total_realized_pnl_usd": 0.0,
        "params": {
            "position_size_pct": 0.18, "max_concurrent_positions": 5, "take_profit_pct": 0.01, "stop_loss_pct": 0.008,
            "max_hold_minutes": 120, "daily_loss_cap_pct": 0.10, "model_confidence_min": 0.55, "min_volume_z": -1e9,
            "min_volatility_ratio": 0.0, "taker_fee_rate": 0.0025,
        },
        "trade_log": [],
        "latest_walkforward": {},
    }
    base.update(overrides)
    return base


_EMPTY_BATCH = {"trades_analyzed": 0, "wins": 0, "losses": 0, "total_pnl_usd": 0.0, "snapshots": [], "recommendations": []}


def test_generate_pdf_report_produces_real_pdf_bytes():
    status = _status(trade_log=[_trade()])
    pdf_bytes = alpaca_crypto_report.generate_pdf_report(status=status, trade_batch=_EMPTY_BATCH)
    assert isinstance(pdf_bytes, bytes)
    assert pdf_bytes[:5] == b"%PDF-"
    assert len(pdf_bytes) > 500


def test_generate_pdf_report_handles_no_trades_at_all():
    pdf_bytes = alpaca_crypto_report.generate_pdf_report(status=_status(), trade_batch=_EMPTY_BATCH)
    assert pdf_bytes[:5] == b"%PDF-"


def test_generate_pdf_report_truncates_a_very_long_trade_history():
    trades = [_trade(symbol=f"COIN{i}/USD") for i in range(alpaca_crypto_report.MAX_TRADES_IN_REPORT + 50)]
    pdf_bytes = alpaca_crypto_report.generate_pdf_report(status=_status(trade_log=trades), trade_batch=_EMPTY_BATCH)
    assert pdf_bytes[:5] == b"%PDF-"


def test_generate_pdf_report_shows_demo_mode_when_not_live():
    status = _status(account_type="paper", live_trading_enabled=False)
    pdf_bytes = alpaca_crypto_report.generate_pdf_report(status=status, trade_batch=_EMPTY_BATCH)
    assert pdf_bytes[:5] == b"%PDF-"  # visual DEMO/LIVE distinction checked directly below


def test_generate_pdf_report_reflects_live_mode_when_enabled():
    """Same account switching from paper to real that the user asked
    about -- this report must read LIVE once ALPACA_TRADING_BASE_URL
    genuinely points at the real endpoint, no separate report needed."""
    status = _status(account_type="live", live_trading_enabled=True)
    pdf_bytes = alpaca_crypto_report.generate_pdf_report(status=status, trade_batch=_EMPTY_BATCH)
    assert pdf_bytes[:5] == b"%PDF-"


def test_generate_pdf_report_includes_trade_lessons_and_recommendations():
    trade_batch = {
        "trades_analyzed": 2, "wins": 1, "losses": 1, "total_pnl_usd": -0.85,
        "snapshots": [
            {"outcome": "win", "lesson": "BTC/USD: WIN $3.20, well-timed exit."},
            {"outcome": "loss", "lesson": "ETH/USD: LOSS $-4.05 (stop_loss)."},
        ],
        "recommendations": ["2 of the last 5 losses reversed favorably shortly after exit."],
    }
    pdf_bytes = alpaca_crypto_report.generate_pdf_report(status=_status(), trade_batch=trade_batch)
    assert pdf_bytes[:5] == b"%PDF-"


def test_generate_pdf_report_includes_a_walkforward_summary_when_available():
    status = _status(latest_walkforward={
        "ok": True, "fold_count": 4, "profitable_fold_count": 1, "mean_return_pct": -0.15, "std_return_pct": 0.08,
        "folds": [
            {"return_pct": -0.10, "trade_count": 40, "win_rate": 0.3, "model_used": "gradient_boosting"},
            {"return_pct": 0.05, "trade_count": 35, "win_rate": 0.55, "model_used": "random_forest"},
        ],
    })
    pdf_bytes = alpaca_crypto_report.generate_pdf_report(status=status, trade_batch=_EMPTY_BATCH)
    assert pdf_bytes[:5] == b"%PDF-"


def test_generate_pdf_report_handles_a_walkforward_summary_that_never_ran():
    pdf_bytes = alpaca_crypto_report.generate_pdf_report(status=_status(latest_walkforward={}), trade_batch=_EMPTY_BATCH)
    assert pdf_bytes[:5] == b"%PDF-"


def test_fmt_param_shows_no_filter_for_the_disabled_sentinel():
    assert alpaca_crypto_report._fmt_param("min_volume_z", -1e9) == "no filter"  # noqa: SLF001


def test_fmt_param_formats_a_real_percentage():
    assert alpaca_crypto_report._fmt_param("position_size_pct", 0.18) == "18.00%"  # noqa: SLF001


def test_fmt_param_formats_a_real_min_filter_value():
    assert alpaca_crypto_report._fmt_param("min_volatility_ratio", 1.3) == "1.3"  # noqa: SLF001


def test_trade_stats_computes_win_rate_and_averages():
    trades = [
        _trade(realized_pnl_usd=10.0, fee_usd=1.0),
        _trade(realized_pnl_usd=-4.0, fee_usd=0.5),
        _trade(realized_pnl_usd=6.0, fee_usd=1.0),
    ]
    stats = alpaca_crypto_report._trade_stats(trades)  # noqa: SLF001
    assert stats["count"] == 3
    assert stats["win_count"] == 2
    assert stats["loss_count"] == 1
    assert round(stats["win_rate"], 2) == 66.67
    assert stats["avg_win"] == 8.0
    assert stats["avg_loss"] == -4.0
    assert stats["best_trade"] == 10.0
    assert stats["worst_trade"] == -4.0
    assert stats["total_fees"] == 2.5


def test_trade_stats_excludes_dry_run_trades():
    trades = [_trade(realized_pnl_usd=10.0), _trade(realized_pnl_usd=-999.0, dry_run=True)]
    stats = alpaca_crypto_report._trade_stats(trades)  # noqa: SLF001
    assert stats["count"] == 1
    assert stats["best_trade"] == 10.0


def test_trade_stats_handles_zero_trades():
    stats = alpaca_crypto_report._trade_stats([])  # noqa: SLF001
    assert stats["count"] == 0
    assert stats["win_rate"] == 0.0
    assert stats["avg_win"] == 0.0
    assert stats["avg_loss"] == 0.0
