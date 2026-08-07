"""Chart-snapshot PNG generation for Threads trade-entry/exit posts. Real
Pillow rendering (no mocking the drawing itself -- the whole point is
verifying a real, valid PNG comes out), but isolated to a tmp_path
directory so tests never touch the real data/charts/ dir."""
from __future__ import annotations

import pytest

from data import chart_snapshot


@pytest.fixture(autouse=True)
def _isolated_charts_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    yield


def _candles(n=60, base=100.0):
    candles = []
    price = base
    for i in range(n):
        o = price
        price += (i % 7) * 0.3 - 1.0
        c = price
        h = max(o, c) + 0.2
        l = min(o, c) - 0.2
        candles.append({"ts": i, "open": o, "high": h, "low": l, "close": c})
    return candles


def test_generate_candlestick_chart_returns_none_with_too_little_history():
    assert chart_snapshot.generate_candlestick_chart(
        ticker="AAPL", market="stocks", candles=_candles(2), entry_price=100.5,
        take_profit_price=101.5, stop_loss_price=99.5,
    ) is None


def test_generate_candlestick_chart_saves_a_real_png_file():
    path = chart_snapshot.generate_candlestick_chart(
        ticker="AAPL", market="stocks", candles=_candles(), entry_price=100.0,
        take_profit_price=101.0, stop_loss_price=99.0, side="long",
    )
    assert path is not None
    assert path.exists()
    assert path.suffix == ".png"
    assert path.stat().st_size > 0
    # A real PNG file starts with this exact 8-byte magic signature.
    assert path.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


def test_generate_candlestick_chart_sanitizes_ticker_and_market_in_the_filename():
    path = chart_snapshot.generate_candlestick_chart(
        ticker="BTC/USD", market="crypto", candles=_candles(), entry_price=100.0,
        take_profit_price=101.0, stop_loss_price=99.0,
    )
    assert path is not None
    assert "/" not in path.name


def test_generate_candlestick_chart_handles_a_short_side():
    path = chart_snapshot.generate_candlestick_chart(
        ticker="ETHPERP", market="perps", candles=_candles(), entry_price=100.0,
        take_profit_price=99.0, stop_loss_price=101.0, side="short",
    )
    assert path is not None


def test_generate_candlestick_chart_survives_a_flat_price_series():
    """entry == take_profit == stop_loss == every candle: span would be
    zero without the padding fallback -- must not divide by zero or
    crash."""
    flat = [{"ts": i, "open": 5.0, "high": 5.0, "low": 5.0, "close": 5.0} for i in range(20)]
    path = chart_snapshot.generate_candlestick_chart(
        ticker="FLAT", market="stocks", candles=flat, entry_price=5.0,
        take_profit_price=5.0, stop_loss_price=5.0,
    )
    assert path is not None


def test_generate_candlestick_chart_prunes_old_files_beyond_the_max(monkeypatch):
    monkeypatch.setattr(chart_snapshot, "MAX_STORED_CHARTS", 3)
    for i in range(6):
        chart_snapshot.generate_candlestick_chart(
            ticker=f"SYM{i}", market="stocks", candles=_candles(), entry_price=100.0,
            take_profit_price=101.0, stop_loss_price=99.0,
        )
    remaining = list(chart_snapshot.CHARTS_DIR.glob("*.png"))
    assert len(remaining) <= 3


def test_generate_candlestick_chart_caps_candles_and_shifts_indices(monkeypatch):
    monkeypatch.setattr(chart_snapshot, "MAX_CANDLESTICKS", 10)
    path = chart_snapshot.generate_candlestick_chart(
        ticker="AAPL", market="stocks", candles=_candles(50), entry_price=100.0,
        entry_index=5, exit_index=45, pnl_usd=1.0,
    )
    # Just confirms it doesn't blow up when indices point outside the
    # trimmed window (entry_index=5 is trimmed away entirely) -- must
    # clamp/skip gracefully, not crash or draw off-canvas.
    assert path is not None


def test_generate_candlestick_chart_optional_levels_can_all_be_omitted():
    """options' underlying-price chart passes no entry/exit price levels at
    all (a different scale than the option's own premium) -- only index
    markers. Must still render."""
    path = chart_snapshot.generate_candlestick_chart(
        ticker="AAPL", market="options", candles=_candles(), side="long",
        entry_index=10, exit_index=40, pnl_usd=-3.0,
        subtitle="Underlying price action (option premium not charted separately)",
    )
    assert path is not None
    assert path.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


def test_generate_candlestick_chart_colors_title_by_pnl():
    win_path = chart_snapshot.generate_candlestick_chart(
        ticker="AAPL", market="stocks", candles=_candles(), pnl_usd=5.0,
    )
    loss_path = chart_snapshot.generate_candlestick_chart(
        ticker="AAPL", market="stocks", candles=_candles(), pnl_usd=-5.0,
    )
    assert win_path is not None and loss_path is not None


def test_public_url_for_returns_none_without_render_external_url(monkeypatch):
    monkeypatch.delenv("RENDER_EXTERNAL_URL", raising=False)
    path = chart_snapshot.generate_candlestick_chart(
        ticker="AAPL", market="stocks", candles=_candles(), entry_price=100.0,
        take_profit_price=101.0, stop_loss_price=99.0,
    )
    assert chart_snapshot.public_url_for(path) is None


def test_public_url_for_builds_the_full_public_url(monkeypatch):
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://bettor-schwab.onrender.com")
    path = chart_snapshot.generate_candlestick_chart(
        ticker="AAPL", market="stocks", candles=_candles(), entry_price=100.0,
        take_profit_price=101.0, stop_loss_price=99.0,
    )
    url = chart_snapshot.public_url_for(path)
    assert url == f"https://bettor-schwab.onrender.com/chart/{path.name}"


def _sentiment_rows(n=10):
    return [{"ticker": f"SYM{i}", "sentiment_score": (i - n / 2) / (n / 2)} for i in range(n)]


def test_generate_sentiment_snapshot_returns_none_for_empty_input():
    assert chart_snapshot.generate_sentiment_snapshot(market="stocks", ticker_sentiments=[]) is None


def test_generate_sentiment_snapshot_ignores_rows_missing_required_fields():
    rows = [{"ticker": "AAPL"}, {"sentiment_score": 0.5}, {"ticker": "MSFT", "sentiment_score": 0.2}]
    path = chart_snapshot.generate_sentiment_snapshot(market="stocks", ticker_sentiments=rows)
    assert path is not None
    assert path.exists()
    assert path.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


def test_generate_sentiment_snapshot_saves_a_real_png_file():
    path = chart_snapshot.generate_sentiment_snapshot(market="crypto", ticker_sentiments=_sentiment_rows())
    assert path is not None
    assert path.exists()
    assert path.suffix == ".png"
    assert path.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


def test_generate_sentiment_snapshot_caps_a_large_universe_to_the_most_extreme_rows():
    rows = _sentiment_rows(n=60)
    path = chart_snapshot.generate_sentiment_snapshot(market="crypto", ticker_sentiments=rows)
    assert path is not None
    # Just confirms it doesn't blow up / produce an unreasonably huge image
    # for a large universe (crypto's own 36+ pairs) -- capped rendering,
    # not an ever-growing canvas.
    from PIL import Image
    with Image.open(path) as img:
        assert img.height < 60 * chart_snapshot._SENTIMENT_ROW_HEIGHT + 200  # noqa: SLF001


def test_generate_sentiment_snapshot_handles_all_neutral_scores():
    rows = [{"ticker": "AAPL", "sentiment_score": 0.0}, {"ticker": "MSFT", "sentiment_score": 0.0}]
    path = chart_snapshot.generate_sentiment_snapshot(market="stocks", ticker_sentiments=rows)
    assert path is not None
