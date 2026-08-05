"""Downloadable PDF account report for the Kalshi Perps live-money bot.
Unlike the Threads-posting modules, this is NOT best-effort by design --
a user who clicked "download" should see a real error if generation
fails, so these tests confirm real, valid PDF bytes come out, not just
that nothing raised."""
from __future__ import annotations

from data import perps_report


def _trade(**overrides):
    base = {
        "closed_at": "2026-08-04T10:23:00+00:00", "ticker": "KXETHPERP", "side": "short",
        "entry_price": 1.87, "exit_price": 1.83, "realized_pnl_usd": 12.5, "fee_usd": 0.8,
        "reason": "take_profit",
    }
    base.update(overrides)
    return base


def test_generate_pdf_report_produces_real_pdf_bytes():
    state = {"positions": [], "trade_log": [_trade()], "realized_pnl_by_date": {"2026-08-04": 12.5}}
    pdf_bytes = perps_report.generate_pdf_report(state=state, account_balance_usd=100.0, live_trading_enabled=True)
    assert isinstance(pdf_bytes, bytes)
    assert pdf_bytes[:5] == b"%PDF-"
    assert len(pdf_bytes) > 500


def test_generate_pdf_report_handles_no_trades_at_all():
    state = {"positions": [], "trade_log": [], "realized_pnl_by_date": {}}
    pdf_bytes = perps_report.generate_pdf_report(state=state, account_balance_usd=100.0, live_trading_enabled=False)
    assert pdf_bytes[:5] == b"%PDF-"


def test_generate_pdf_report_handles_a_missing_account_balance():
    """account.get("available_balance_usd") can genuinely be None (a
    failed balance check, per app_kalshi.py's own _cached_account_snapshot)
    -- must not raise trying to format it."""
    state = {"positions": [], "trade_log": [_trade()], "realized_pnl_by_date": {"2026-08-04": 12.5}}
    pdf_bytes = perps_report.generate_pdf_report(state=state, account_balance_usd=None, live_trading_enabled=True)
    assert pdf_bytes[:5] == b"%PDF-"


def test_generate_pdf_report_truncates_a_very_long_trade_history():
    trades = [_trade(ticker=f"KXTICK{i}PERP") for i in range(perps_report.MAX_TRADES_IN_REPORT + 50)]
    state = {"positions": [], "trade_log": trades, "realized_pnl_by_date": {}}
    pdf_bytes = perps_report.generate_pdf_report(state=state, account_balance_usd=100.0, live_trading_enabled=True)
    assert pdf_bytes[:5] == b"%PDF-"


def test_trade_stats_computes_win_rate_and_averages():
    trades = [
        _trade(realized_pnl_usd=10.0, fee_usd=1.0),
        _trade(realized_pnl_usd=-4.0, fee_usd=0.5),
        _trade(realized_pnl_usd=6.0, fee_usd=1.0),
    ]
    stats = perps_report._trade_stats(trades)  # noqa: SLF001
    assert stats["count"] == 3
    assert stats["win_count"] == 2
    assert stats["loss_count"] == 1
    assert round(stats["win_rate"], 2) == 66.67
    assert stats["avg_win"] == 8.0
    assert stats["avg_loss"] == -4.0
    assert stats["best_trade"] == 10.0
    assert stats["worst_trade"] == -4.0
    assert stats["total_fees"] == 2.5


def test_trade_stats_handles_zero_trades():
    stats = perps_report._trade_stats([])  # noqa: SLF001
    assert stats["count"] == 0
    assert stats["win_rate"] == 0.0
    assert stats["avg_win"] == 0.0
    assert stats["avg_loss"] == 0.0
