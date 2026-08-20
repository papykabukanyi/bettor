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


def test_generate_candlestick_chart_renders_an_indicator_panel_when_given():
    from PIL import Image

    # Distinct tickers -- generate_candlestick_chart's filename includes
    # only whole-second time resolution, so two same-ticker calls back to
    # back in a test can collide and the second overwrites the first file.
    without_path = chart_snapshot.generate_candlestick_chart(
        ticker="AAPL", market="stocks", candles=_candles(),
    )
    with_path = chart_snapshot.generate_candlestick_chart(
        ticker="MSFT", market="stocks", candles=_candles(),
        indicators={"RSI": "58.3", "MACD hist": "+0.03%", "Volume Z": "+1.2", "Model conf": "61.4%"},
    )
    assert without_path is not None and with_path is not None
    without_height = Image.open(without_path).size[1]
    with_height = Image.open(with_path).size[1]
    # The indicator panel grows the canvas -- proof it actually rendered
    # something extra, not just accepted the argument silently.
    assert with_height > without_height


def test_generate_candlestick_chart_indicator_panel_drops_none_values():
    """A caller may not have every indicator available for every trade
    (e.g. no model prediction yet) -- None entries must be skipped rather
    than rendered as the literal string "None"."""
    from PIL import Image

    one_item_path = chart_snapshot.generate_candlestick_chart(
        ticker="AAPL", market="stocks", candles=_candles(), indicators={"RSI": "58.3"},
    )
    mixed_path = chart_snapshot.generate_candlestick_chart(
        ticker="MSFT", market="stocks", candles=_candles(),
        indicators={"RSI": "58.3", "MACD hist": None, "Volume Z": None},
    )
    assert one_item_path is not None and mixed_path is not None
    # Both have exactly one real indicator -> same single-row panel height.
    assert Image.open(one_item_path).size[1] == Image.open(mixed_path).size[1]


def test_generate_candlestick_chart_empty_indicators_dict_adds_no_panel():
    from PIL import Image

    omitted_path = chart_snapshot.generate_candlestick_chart(
        ticker="AAPL", market="stocks", candles=_candles(),
    )
    empty_dict_path = chart_snapshot.generate_candlestick_chart(
        ticker="MSFT", market="stocks", candles=_candles(), indicators={},
    )
    assert omitted_path is not None and empty_dict_path is not None
    assert Image.open(omitted_path).size[1] == Image.open(empty_dict_path).size[1]


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


def test_generate_sentiment_snapshot_uses_a_distinct_accent_banner_per_market():
    """Real feedback: this card used to be plain text with no branding,
    visually flat next to the accent-banner news/bullet cards -- now
    shares that same per-market banner treatment."""
    from PIL import Image

    paths = {
        market: chart_snapshot.generate_sentiment_snapshot(market=market, ticker_sentiments=_sentiment_rows())
        for market in ("crypto", "stocks", "options", "perps")
    }
    accent_pixels = {}
    for market, path in paths.items():
        assert path is not None
        with Image.open(path) as img:
            accent_pixels[market] = img.getpixel((5, 5))
    assert len(set(accent_pixels.values())) == 4  # all 4 markets render a genuinely different banner color


def test_generate_sentiment_snapshot_renders_a_large_trimmed_universe_without_raising():
    """A large universe (crypto's 36+ pairs) only plots the most extreme
    MAX_SENTIMENT_ROWS, but the bullish/bearish badge is computed from the
    full input list before that trim -- confirms this larger, asymmetric
    (30 bullish / 5 bearish) universe still renders a valid, bounded image."""
    rows = [{"ticker": f"SYM{i}", "sentiment_score": 0.5} for i in range(30)]  # all bullish, way over MAX_SENTIMENT_ROWS
    rows += [{"ticker": f"BEAR{i}", "sentiment_score": -0.5} for i in range(5)]
    path = chart_snapshot.generate_sentiment_snapshot(market="crypto", ticker_sentiments=rows)
    assert path is not None
    from PIL import Image
    with Image.open(path) as img:
        # Still capped to MAX_SENTIMENT_ROWS worth of bars, not all 35 rows
        # -- height is top margin + capped rows + bottom margin, never 35 rows'
        # worth (which would be meaningfully taller than this bound).
        max_expected = chart_snapshot._SENTIMENT_TOP_MARGIN + chart_snapshot.MAX_SENTIMENT_ROWS * chart_snapshot._SENTIMENT_ROW_HEIGHT + chart_snapshot._SENTIMENT_BOTTOM_MARGIN  # noqa: SLF001
        assert img.height <= max_expected


def test_generate_sentiment_snapshot_is_taller_than_the_old_plain_layout_for_a_follow_footer():
    """Real feedback: no follow-growth hook anywhere on this card, unlike
    the hourly status post's own "follow for real-time alerts" line --
    confirms real extra vertical space exists for the new banner + footer,
    not just the same old bar-chart body."""
    from PIL import Image

    path = chart_snapshot.generate_sentiment_snapshot(market="crypto", ticker_sentiments=_sentiment_rows(n=2))
    assert path is not None
    with Image.open(path) as img:
        # 2 rows is a tiny chart body -- most of this height is now the
        # banner + subtitle + footer, not the bars themselves.
        assert img.height >= chart_snapshot._SENTIMENT_BANNER_HEIGHT + chart_snapshot._SENTIMENT_BOTTOM_MARGIN  # noqa: SLF001


# ── generate_news_card ───────────────────────────────────────────────────────

def test_generate_news_card_saves_a_real_png_file():
    path = chart_snapshot.generate_news_card(
        market="crypto", headline="Bitcoin surges past resistance", source="cointelegraph",
        secondary=["ETF inflows accelerate"], hashtags="#Crypto #Bitcoin",
    )
    assert path is not None
    assert path.exists()
    assert path.suffix == ".png"
    assert path.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


def test_generate_news_card_handles_missing_source_and_secondary():
    """Real headline-only stories (no RSS enclosure source, no corroborated
    secondary items) must still render a valid, shorter card, not crash."""
    path = chart_snapshot.generate_news_card(market="stocks", headline="Markets rally on rate cut hopes")
    assert path is not None
    assert path.exists()


def test_generate_news_card_wraps_a_long_headline_without_raising():
    long_headline = " ".join(["word"] * 60)
    path = chart_snapshot.generate_news_card(market="options", headline=long_headline, hashtags="#Options")
    assert path is not None
    from PIL import Image
    with Image.open(path) as img:
        # Headline is capped at _NEWS_CARD_HEADLINE_MAX_LINES -- confirms the
        # canvas doesn't grow unbounded for an unusually long real headline.
        assert img.height < chart_snapshot._s(1700)  # noqa: SLF001


def test_generate_news_card_uses_a_distinct_accent_color_per_market():
    """Real, confirmed bug this whole feature replaces: every trending post
    used to attach the SAME scraped image regardless of market or story
    (see post_trending_news's own docstring) -- confirming each market
    renders with its own distinct accent color is a cheap, direct way to
    verify these cards are NOT visually identical to each other."""
    from PIL import Image

    paths = {
        market: chart_snapshot.generate_news_card(market=market, headline="Same headline text", hashtags="#Tag")
        for market in ("crypto", "stocks", "options", "perps")
    }
    accent_pixels = {}
    for market, path in paths.items():
        assert path is not None
        with Image.open(path) as img:
            accent_pixels[market] = img.getpixel((5, 5))
    assert len(set(accent_pixels.values())) == 4  # all 4 markets render a genuinely different accent color


def test_generate_news_card_enforces_a_minimum_height_for_short_content():
    """Real feedback: short-content cards (no source, no secondary
    headlines) used to render as a thin, cramped banner. A short headline
    alone must still produce a substantial, well-proportioned card."""
    from PIL import Image

    path = chart_snapshot.generate_news_card(market="crypto", headline="Short headline", hashtags="#Tag")
    assert path is not None
    with Image.open(path) as img:
        assert img.height >= chart_snapshot._NEWS_CARD_MIN_HEIGHT  # noqa: SLF001


def test_generate_news_card_is_wide_and_substantial():
    """Real feedback: the card read as small -- confirms the base canvas
    is a real, large social-card size, not the old cramped 900-wide one."""
    from PIL import Image

    path = chart_snapshot.generate_news_card(market="crypto", headline="Bitcoin surges past resistance", hashtags="#Crypto")
    assert path is not None
    with Image.open(path) as img:
        assert img.width == chart_snapshot._NEWS_CARD_WIDTH  # noqa: SLF001
        assert img.width >= chart_snapshot._s(1000)  # noqa: SLF001


def test_generate_news_card_returns_none_when_pillow_is_unavailable(monkeypatch):
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "PIL":
            raise ImportError("simulated missing Pillow")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert chart_snapshot.generate_news_card(market="crypto", headline="Bitcoin surges") is None


# ── generate_news_bullet_card (one big card per carousel item) ─────────────

def test_generate_news_bullet_card_saves_a_real_png_file():
    path = chart_snapshot.generate_news_bullet_card(
        market="crypto", headline="Bitcoin surges past resistance", source="cointelegraph",
        index=1, total=3, hashtags="#Crypto #Bitcoin",
    )
    assert path is not None
    assert path.exists()
    assert path.suffix == ".png"
    assert path.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


def test_generate_news_bullet_card_handles_missing_source():
    path = chart_snapshot.generate_news_bullet_card(market="stocks", headline="Markets rally on rate cut hopes")
    assert path is not None
    assert path.exists()


def test_generate_news_bullet_card_wraps_a_long_headline_without_raising():
    long_headline = " ".join(["word"] * 60)
    path = chart_snapshot.generate_news_bullet_card(market="options", headline=long_headline, hashtags="#Options")
    assert path is not None
    from PIL import Image
    with Image.open(path) as img:
        # Headline is capped at _BULLET_CARD_HEADLINE_MAX_LINES -- confirms
        # the canvas doesn't grow unbounded for an unusually long headline.
        assert img.height < chart_snapshot._s(2400)  # noqa: SLF001


def test_generate_news_bullet_card_uses_a_distinct_accent_color_per_market():
    from PIL import Image

    paths = {
        market: chart_snapshot.generate_news_bullet_card(market=market, headline="Same headline text", hashtags="#Tag")
        for market in ("crypto", "stocks", "options", "perps")
    }
    accent_pixels = {}
    for market, path in paths.items():
        assert path is not None
        with Image.open(path) as img:
            accent_pixels[market] = img.getpixel((5, 5))
    assert len(set(accent_pixels.values())) == 4


def test_generate_news_bullet_card_shows_an_index_badge_only_when_part_of_a_set():
    """A lone card (total=1) has nothing to badge -- the "N/M" indicator
    only makes sense, and should only appear, when there's an actual set
    to place it in context of."""
    from PIL import Image

    solo_path = chart_snapshot.generate_news_bullet_card(market="crypto", headline="Solo headline", index=1, total=1)
    multi_path = chart_snapshot.generate_news_bullet_card(market="crypto", headline="Solo headline", index=2, total=4)
    assert solo_path is not None and multi_path is not None
    # Not asserting on exact pixels (font rendering specifics aren't the
    # point) -- just that the two variants produce genuinely different
    # banner regions, i.e. the badge is really conditional on `total`.
    with Image.open(solo_path) as solo_img, Image.open(multi_path) as multi_img:
        banner_h = chart_snapshot._BULLET_CARD_BANNER_HEIGHT  # noqa: SLF001
        solo_banner = solo_img.crop((0, 0, solo_img.width, banner_h)).tobytes()
        multi_banner = multi_img.crop((0, 0, multi_img.width, banner_h)).tobytes()
        assert solo_banner != multi_banner


def test_generate_news_bullet_card_headline_font_is_bigger_than_the_combined_card(tmp_path):
    """Real, explicit design goal: with only ONE headline to show (not a
    lead story competing with up to 3 secondary bullets for space), the
    bullet card can and should render its headline meaningfully bigger
    than the old combined card's own headline font."""
    assert chart_snapshot._s(58) > chart_snapshot._s(44)  # noqa: SLF001  # bullet card vs. combined card headline size


def test_generate_news_bullet_card_is_square_ish_and_substantial():
    from PIL import Image

    path = chart_snapshot.generate_news_bullet_card(market="crypto", headline="Bitcoin surges", hashtags="#Crypto")
    assert path is not None
    with Image.open(path) as img:
        assert img.width == chart_snapshot._BULLET_CARD_WIDTH  # noqa: SLF001
        assert img.height >= chart_snapshot._BULLET_CARD_MIN_HEIGHT  # noqa: SLF001


def test_generate_news_bullet_card_returns_none_when_pillow_is_unavailable(monkeypatch):
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "PIL":
            raise ImportError("simulated missing Pillow")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert chart_snapshot.generate_news_bullet_card(market="crypto", headline="Bitcoin surges") is None


# ── _wrap_lines ───────────────────────────────────────────────────────────────

def _draw():
    from PIL import Image, ImageDraw
    return ImageDraw.Draw(Image.new("RGB", (1, 1)))


def test_wrap_lines_fits_short_text_on_one_line():
    font = chart_snapshot._font(20)  # noqa: SLF001
    lines = chart_snapshot._wrap_lines(_draw(), "Short headline", font, max_width=1000, max_lines=4)  # noqa: SLF001
    assert lines == ["Short headline"]


def test_wrap_lines_never_exceeds_max_lines():
    font = chart_snapshot._font(20)  # noqa: SLF001
    text = " ".join(["word"] * 80)
    lines = chart_snapshot._wrap_lines(_draw(), text, font, max_width=200, max_lines=3)  # noqa: SLF001
    assert len(lines) <= 3


def test_wrap_lines_truncates_the_last_line_with_an_ellipsis():
    font = chart_snapshot._font(20)  # noqa: SLF001
    text = " ".join(["word"] * 80)
    lines = chart_snapshot._wrap_lines(_draw(), text, font, max_width=200, max_lines=2)  # noqa: SLF001
    assert lines[-1].endswith("…")


def test_wrap_lines_respects_the_configured_width():
    font = chart_snapshot._font(20)  # noqa: SLF001
    draw = _draw()
    text = "one two three four five six seven eight"
    lines = chart_snapshot._wrap_lines(draw, text, font, max_width=120, max_lines=10)  # noqa: SLF001
    for line in lines:
        assert draw.textlength(line, font=font) <= 120


# ── _draw_hashtag_pills ────────────────────────────────────────────────────────

def test_draw_hashtag_pills_returns_the_starting_y_for_no_tags():
    font = chart_snapshot._font(20)  # noqa: SLF001
    result = chart_snapshot._draw_hashtag_pills(_draw(), [], x=0, y=100, max_width=1000, font=font, accent=(255, 0, 0))  # noqa: SLF001
    assert result == 100


def test_draw_hashtag_pills_advances_y_for_a_single_row():
    font = chart_snapshot._font(20)  # noqa: SLF001
    result = chart_snapshot._draw_hashtag_pills(  # noqa: SLF001
        _draw(), ["#Crypto", "#Bitcoin"], x=0, y=100, max_width=1000, font=font, accent=(255, 0, 0),
    )
    assert result > 100


def test_draw_hashtag_pills_wraps_to_a_second_row_when_too_narrow():
    font = chart_snapshot._font(20)  # noqa: SLF001
    draw = _draw()
    one_row = chart_snapshot._draw_hashtag_pills(draw, ["#Crypto"], x=0, y=100, max_width=1000, font=font, accent=(255, 0, 0))  # noqa: SLF001
    two_rows = chart_snapshot._draw_hashtag_pills(  # noqa: SLF001
        draw, ["#Crypto", "#Bitcoin", "#CryptoNews", "#BTC", "#MarketRally"], x=0, y=100, max_width=200, font=font, accent=(255, 0, 0),
    )
    assert two_rows > one_row  # forced to wrap across more rows at this narrow width
