"""Threads trade-entry/restart-notice posting -- must silently no-op
without a completed login, must never raise (a failure here can never be
allowed to affect real trade execution), and must format/truncate post
text correctly."""
from __future__ import annotations

import time

import pytest

from data import threads_post


@pytest.fixture(autouse=True)
def _isolated_dedup_state(monkeypatch):
    """Both dedup stores (recently-posted news, already-replied-to posts)
    cache in an in-memory module global that's meant to persist for a real
    process's whole lifetime -- reset between tests here so one test's
    successful post doesn't "poison" a later test using the same
    title/post id as a false recent-duplicate."""
    monkeypatch.setattr(threads_post, "_recent_news_cache", None)
    monkeypatch.setattr(threads_post, "_replied_posts_cache", None)
    monkeypatch.setattr(threads_post, "HF_API_KEY", "")  # no real network for the HF mirror by default
    yield


def test_is_configured_reflects_whether_a_real_login_has_completed(monkeypatch):
    monkeypatch.setattr(threads_post.threads_client, "get_valid_access_token", lambda: None)
    assert threads_post.is_configured() is False
    monkeypatch.setattr(threads_post.threads_client, "get_valid_access_token", lambda: "at-1")
    assert threads_post.is_configured() is True


def test_post_trade_entry_returns_false_without_a_completed_login(monkeypatch):
    """create_and_publish_post() itself raises "No valid Threads access
    token" when no login has ever completed (see threads_client.py) --
    post_trade_entry() must catch that and return False, never raise."""
    def raise_no_token(text):
        raise RuntimeError("No valid Threads access token -- complete the interactive login first")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", raise_no_token)
    result = threads_post.post_trade_entry(
        ticker="KXBTCPERP", side="long", entry_price=6.5, take_profit_price=6.6,
        stop_loss_price=6.4, reason="test", dry_run=False,
    )
    assert result is False


def test_post_trade_entry_respects_the_disable_flag(monkeypatch):
    monkeypatch.setattr(threads_post, "THREADS_POST_ENABLED", False)
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    result = threads_post.post_trade_entry(
        ticker="KXBTCPERP", side="long", entry_price=6.5, take_profit_price=6.6,
        stop_loss_price=6.4, reason="test", dry_run=False,
    )
    assert result is False
    assert posted == []


def test_post_trade_entry_posts_formatted_text_when_configured(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text) or "post-1")
    result = threads_post.post_trade_entry(
        ticker="KXBTCPERP", side="long", entry_price=6.5, take_profit_price=6.63,
        stop_loss_price=6.4, reason="dip signal + model confidence 0.61", dry_run=False,
    )
    assert result is True
    assert len(posted) == 1
    text = posted[0]
    assert "KXBTCPERP" in text
    assert "LONG" in text
    assert "6.5000" in text
    assert "6.6300" in text
    assert "6.4000" in text
    assert "dip signal" in text
    assert "[SIMULATED]" not in text


def test_post_trade_entry_marks_dry_run_trades_as_simulated(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    threads_post.post_trade_entry(
        ticker="KXBTCPERP", side="short", entry_price=6.5, take_profit_price=6.4,
        stop_loss_price=6.6, reason="test", dry_run=True,
    )
    assert "[SIMULATED]" in posted[0]
    assert "SHORT" in posted[0]


def test_post_trade_entry_defaults_to_the_perps_label_and_hashtags(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    threads_post.post_trade_entry(
        ticker="KXBTCPERP", side="long", entry_price=6.5, take_profit_price=6.6,
        stop_loss_price=6.4, reason="test", dry_run=False,
    )
    assert "Kalshi Perps" in posted[0]
    assert "#Kalshi" in posted[0]


def test_post_trade_entry_labels_and_hashtags_each_market_distinctly(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    expectations = {
        "stocks": ("Alpaca Stocks", "#StockMarket"),
        "crypto": ("Alpaca Crypto", "#Bitcoin"),
        "options": ("Alpaca Options", "#OptionsTrading"),
    }
    for market, (label, hashtag) in expectations.items():
        posted.clear()
        threads_post.post_trade_entry(
            ticker="XYZ", side="long", entry_price=1.0, take_profit_price=1.1,
            stop_loss_price=0.9, reason="test", dry_run=False, market=market,
        )
        assert label in posted[0]
        assert hashtag in posted[0]
        assert "Kalshi Perps" not in posted[0]


def test_post_trade_entry_never_raises_on_api_failure(monkeypatch):
    def raise_error(text):
        raise RuntimeError("simulated Threads API failure")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", raise_error)
    result = threads_post.post_trade_entry(
        ticker="KXBTCPERP", side="long", entry_price=6.5, take_profit_price=6.6,
        stop_loss_price=6.4, reason="test", dry_run=False,
    )
    assert result is False


def test_post_trade_exit_reports_a_win_with_pnl_and_hashtags(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    result = threads_post.post_trade_exit(
        ticker="KXBTCPERP", side="long", entry_price=6.5, exit_price=6.7,
        pnl_usd=12.34, reason="take_profit", dry_run=False, market="perps",
    )
    assert result is True
    text = posted[0]
    assert "CLOSED LONG KXBTCPERP" in text
    assert "WIN" in text
    assert "+12.34" in text
    assert "6.5000" in text and "6.7000" in text
    assert "take_profit" in text
    assert "#Kalshi" in text
    assert "[SIMULATED]" not in text


def test_post_trade_exit_reports_a_loss(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    threads_post.post_trade_exit(
        ticker="AAPL", side="long", entry_price=190.0, exit_price=185.0,
        pnl_usd=-5.0, reason="stop_loss", dry_run=True, market="stocks",
    )
    text = posted[0]
    assert "LOSS" in text
    assert "-5.00" in text
    assert "[SIMULATED]" in text
    assert "Alpaca Stocks" in text


def test_post_trade_exit_respects_the_disable_flag(monkeypatch):
    monkeypatch.setattr(threads_post, "THREADS_POST_ENABLED", False)
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    result = threads_post.post_trade_exit(
        ticker="AAPL", side="long", entry_price=190.0, exit_price=185.0,
        pnl_usd=-5.0, reason="stop_loss", dry_run=True,
    )
    assert result is False
    assert posted == []


def test_post_trade_exit_never_raises_on_api_failure(monkeypatch):
    def raise_error(text):
        raise RuntimeError("simulated Threads API failure")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", raise_error)
    result = threads_post.post_trade_exit(
        ticker="AAPL", side="long", entry_price=190.0, exit_price=185.0,
        pnl_usd=-5.0, reason="stop_loss", dry_run=True,
    )
    assert result is False


def test_post_restart_notice_posts_the_default_message(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    result = threads_post.post_restart_notice()
    assert result is True
    assert posted == ["Money Bot has restarted!"]


def test_post_restart_notice_accepts_a_custom_message(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    threads_post.post_restart_notice("custom restart message")
    assert posted == ["custom restart message"]


def test_post_restart_notice_respects_the_disable_flag(monkeypatch):
    monkeypatch.setattr(threads_post, "THREADS_POST_ENABLED", False)
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    assert threads_post.post_restart_notice() is False
    assert posted == []


def test_post_restart_notice_never_raises_on_api_failure(monkeypatch):
    def raise_error(text):
        raise RuntimeError("simulated Threads API failure")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", raise_error)
    assert threads_post.post_restart_notice() is False


def test_hourly_status_reports_flat_with_no_open_positions(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    result = threads_post.post_hourly_status(positions=[], today_realized_pnl_usd=0.0)
    assert result is True
    assert "flat" in posted[0].lower()
    assert "Today's P&L: +0.00" in posted[0]


def test_hourly_status_lists_every_open_position(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    positions = [
        {
            "ticker": "KXBTCPERP", "side": "long", "entry_price": 6.5, "held_minutes": 42.3,
            "take_profit_price": 6.63, "stop_loss_price": 6.4,
        },
        {
            "ticker": "KXETHPERP", "side": "short", "entry_price": 3.2, "held_minutes": 5.0,
            "take_profit_price": 3.1, "stop_loss_price": 3.28,
        },
    ]
    threads_post.post_hourly_status(positions=positions, today_realized_pnl_usd=-1.25)
    text = posted[0]
    assert "2 open positions" in text
    assert "LONG KXBTCPERP" in text
    assert "SHORT KXETHPERP" in text
    assert "held 42min" in text
    assert "Today's P&L: -1.25" in text


def test_hourly_status_singular_wording_for_one_position(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    threads_post.post_hourly_status(
        positions=[{"ticker": "KXBTCPERP", "side": "long", "entry_price": 6.5, "held_minutes": 1.0,
                    "take_profit_price": 6.6, "stop_loss_price": 6.4}],
    )
    assert "1 open position" in posted[0]
    assert "1 open positions" not in posted[0]


def test_hourly_status_omits_pnl_line_when_not_provided(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    threads_post.post_hourly_status(positions=[])
    assert "P&L" not in posted[0]


def test_hourly_status_includes_a_follow_growth_hook(monkeypatch):
    """Real ask: the old status dump gave a reader no reason to stick
    around or follow -- every post now carries a follow CTA and growth
    hashtags on top of the market's own base tag set."""
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    threads_post.post_hourly_status(positions=[])
    text = posted[0]
    assert "follow" in text.lower()
    assert "#FollowForMore" in text
    assert "#AlgoTrading" in text
    assert threads_post._hashtags_for_market("perps") in text  # noqa: SLF001 -- base tags still present, not replaced


def test_hourly_status_formats_a_long_hold_in_hours_not_raw_minutes(monkeypatch):
    """Real gap: the new trailing-stop strategy can hold a position most
    of a day (up to TRAILING_MAX_HOLD_MINUTES) -- "held 1020min" reads far
    worse than "held 17h0m"."""
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    threads_post.post_hourly_status(positions=[{
        "ticker": "KXLINKPERP", "side": "short", "entry_price": 9.4179, "held_minutes": 1020.0,
        "take_profit_price": 9.26, "stop_loss_price": 9.5241,
    }])
    text = posted[0]
    assert "held 17h" in text
    assert "1020min" not in text


def test_format_held_duration_under_an_hour():
    assert threads_post._format_held_duration(42.3) == "42min"  # noqa: SLF001


def test_format_held_duration_on_the_hour_has_no_trailing_minutes():
    assert threads_post._format_held_duration(120) == "2h"  # noqa: SLF001


def test_format_held_duration_none_is_a_placeholder():
    assert threads_post._format_held_duration(None) == "?"  # noqa: SLF001


def test_hourly_status_respects_the_disable_flag(monkeypatch):
    monkeypatch.setattr(threads_post, "THREADS_POST_ENABLED", False)
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    assert threads_post.post_hourly_status(positions=[]) is False
    assert posted == []


def test_hourly_status_never_raises_on_api_failure(monkeypatch):
    def raise_error(text):
        raise RuntimeError("simulated Threads API failure")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", raise_error)
    assert threads_post.post_hourly_status(positions=[]) is False


def _candles(n=60, base=100.0):
    candles = []
    price = base
    for i in range(n):
        o = price
        price += (i % 7) * 0.3 - 1.0
        c = price
        candles.append({"ts": i, "open": o, "high": max(o, c) + 0.2, "low": min(o, c) - 0.2, "close": c})
    return candles


def test_post_trade_entry_chart_respects_the_disable_flag(monkeypatch):
    monkeypatch.setattr(threads_post, "THREADS_POST_ENABLED", False)
    result = threads_post.post_trade_entry_chart(
        ticker="AAPL", market="stocks", candles=_candles(), entry_price=100.0,
        take_profit_price=101.0, stop_loss_price=99.0, dry_run=False,
    )
    assert result is False


def test_post_trade_entry_chart_skips_without_enough_price_history():
    result = threads_post.post_trade_entry_chart(
        ticker="AAPL", market="stocks", candles=_candles()[:2], entry_price=100.5,
        take_profit_price=101.0, stop_loss_price=99.0, dry_run=False,
    )
    assert result is False


def test_post_trade_entry_chart_skips_without_a_known_public_url(monkeypatch, tmp_path):
    from data import chart_snapshot

    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    monkeypatch.delenv("RENDER_EXTERNAL_URL", raising=False)
    result = threads_post.post_trade_entry_chart(
        ticker="AAPL", market="stocks", candles=_candles(), entry_price=100.0,
        take_profit_price=101.0, stop_loss_price=99.0, dry_run=False,
    )
    assert result is False


def test_post_trade_entry_chart_posts_the_image_when_everything_lines_up(monkeypatch, tmp_path):
    from data import chart_snapshot

    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://bettor-schwab.onrender.com")

    captured = {}
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_image_post",
        lambda image_url, text="": captured.update(image_url=image_url, text=text) or "post-1",
    )

    result = threads_post.post_trade_entry_chart(
        ticker="AAPL", market="stocks", candles=_candles(), entry_price=100.0,
        take_profit_price=101.0, stop_loss_price=99.0, entry_index=10, side="long", dry_run=False,
    )
    assert result is True
    assert captured["image_url"].startswith("https://bettor-schwab.onrender.com/chart/")
    assert "Alpaca Stocks" in captured["text"]
    assert "AAPL" in captured["text"]
    assert "#StockMarket" in captured["text"]


def test_post_trade_entry_chart_never_raises_on_api_failure(monkeypatch, tmp_path):
    from data import chart_snapshot

    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://bettor-schwab.onrender.com")

    def raise_error(image_url, text=""):
        raise RuntimeError("simulated Threads API failure")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_image_post", raise_error)
    result = threads_post.post_trade_entry_chart(
        ticker="AAPL", market="stocks", candles=_candles(), entry_price=100.0,
        take_profit_price=101.0, stop_loss_price=99.0, dry_run=False,
    )
    assert result is False


def test_post_trade_exit_chart_respects_the_disable_flag(monkeypatch):
    monkeypatch.setattr(threads_post, "THREADS_POST_ENABLED", False)
    result = threads_post.post_trade_exit_chart(
        ticker="AAPL", market="stocks", candles=_candles(), entry_price=100.0,
        exit_price=101.0, pnl_usd=5.0, dry_run=False,
    )
    assert result is False


def test_post_trade_exit_chart_posts_the_image_with_win_loss_in_the_caption(monkeypatch, tmp_path):
    from data import chart_snapshot

    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://bettor-schwab.onrender.com")

    captured = {}
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_image_post",
        lambda image_url, text="": captured.update(image_url=image_url, text=text) or "post-1",
    )

    result = threads_post.post_trade_exit_chart(
        ticker="AAPL", market="stocks", candles=_candles(), entry_price=100.0, exit_price=104.0,
        entry_index=5, exit_index=50, side="long", pnl_usd=12.5, dry_run=False,
    )
    assert result is True
    assert "WIN" in captured["text"]
    assert "+12.50" in captured["text"]


def test_post_trade_exit_chart_reports_loss_in_the_caption(monkeypatch, tmp_path):
    from data import chart_snapshot

    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://bettor-schwab.onrender.com")

    captured = {}
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_image_post",
        lambda image_url, text="": captured.update(image_url=image_url, text=text) or "post-1",
    )

    result = threads_post.post_trade_exit_chart(
        ticker="AAPL", market="stocks", candles=_candles(), entry_price=100.0, exit_price=97.0,
        side="long", pnl_usd=-3.0, dry_run=False,
    )
    assert result is True
    assert "LOSS" in captured["text"]
    assert "-3.00" in captured["text"]


def test_post_trade_exit_chart_supports_no_price_levels_for_options(monkeypatch, tmp_path):
    """options posts an underlying-price chart with no entry/exit price
    reference lines (a different scale than the option's own premium) --
    just index markers and a clarifying subtitle."""
    from data import chart_snapshot

    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://bettor-schwab.onrender.com")
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_image_post", lambda url, text="": "post-1")

    result = threads_post.post_trade_exit_chart(
        ticker="AAPL260116C00150000", market="options", candles=_candles(), entry_index=5, exit_index=40,
        side="long", pnl_usd=-8.0, dry_run=False, subtitle="Underlying price action (option premium not charted separately)",
    )
    assert result is True


def test_post_trade_exit_chart_never_raises_on_api_failure(monkeypatch, tmp_path):
    from data import chart_snapshot

    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://bettor-schwab.onrender.com")

    def raise_error(image_url, text=""):
        raise RuntimeError("simulated Threads API failure")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_image_post", raise_error)
    result = threads_post.post_trade_exit_chart(
        ticker="AAPL", market="stocks", candles=_candles(), entry_price=100.0, exit_price=99.0,
        pnl_usd=-1.0, dry_run=False,
    )
    assert result is False


def _sentiment_rows(n=10):
    return [{"ticker": f"SYM{i}", "sentiment_score": (i - n / 2) / (n / 2)} for i in range(n)]


def test_post_sentiment_snapshot_respects_the_disable_flag(monkeypatch):
    monkeypatch.setattr(threads_post, "THREADS_POST_ENABLED", False)
    result = threads_post.post_sentiment_snapshot(market="stocks", ticker_sentiments=_sentiment_rows())
    assert result is False


def test_post_sentiment_snapshot_skips_without_enough_data(monkeypatch, tmp_path):
    from data import chart_snapshot

    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    result = threads_post.post_sentiment_snapshot(market="stocks", ticker_sentiments=[])
    assert result is False


def test_post_sentiment_snapshot_skips_without_a_known_public_url(monkeypatch, tmp_path):
    from data import chart_snapshot

    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    monkeypatch.delenv("RENDER_EXTERNAL_URL", raising=False)
    result = threads_post.post_sentiment_snapshot(market="stocks", ticker_sentiments=_sentiment_rows())
    assert result is False


def test_post_sentiment_snapshot_posts_the_image_when_everything_lines_up(monkeypatch, tmp_path):
    from data import chart_snapshot

    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://bettor-alpaca-crypto.onrender.com")

    captured = {}
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_image_post",
        lambda image_url, text="": captured.update(image_url=image_url, text=text) or "post-1",
    )

    result = threads_post.post_sentiment_snapshot(market="crypto", ticker_sentiments=_sentiment_rows())
    assert result is True
    assert captured["image_url"].startswith("https://bettor-alpaca-crypto.onrender.com/chart/")
    assert "Alpaca Crypto" in captured["text"]
    assert "#Bitcoin" in captured["text"]
    # Real feedback: this caption was missing the same follower-growth
    # hashtags the hourly status post already carries.
    assert "#AlgoTrading" in captured["text"]
    assert "#FollowForMore" in captured["text"]


def test_post_sentiment_snapshot_never_raises_on_api_failure(monkeypatch, tmp_path):
    from data import chart_snapshot

    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://bettor-alpaca-crypto.onrender.com")

    def raise_error(image_url, text=""):
        raise RuntimeError("simulated Threads API failure")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_image_post", raise_error)
    result = threads_post.post_sentiment_snapshot(market="crypto", ticker_sentiments=_sentiment_rows())
    assert result is False


def _story(
    title="Bitcoin surges past resistance", *, link="https://example.com/article",
    image_url="https://example.com/photo.jpg", source="cointelegraph", secondary=None,
):
    return {
        "title": title, "link": link, "image_url": image_url, "source": source,
        "secondary": secondary if secondary is not None else ["ETF inflows accelerate"],
    }


def test_trending_news_reports_nothing_notable_with_no_story(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    result = threads_post.post_trending_news(None, market="crypto")
    assert result is True
    assert "nothing notable" in posted[0].lower()


def _mock_charts(monkeypatch, tmp_path):
    from data import chart_snapshot

    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://bettor-schwab.onrender.com")


def test_trending_news_posts_a_carousel_with_one_card_per_headline_and_hashtags_only_caption(monkeypatch, tmp_path):
    """Real, confirmed bug the whole generated-image-card approach (single
    or carousel) replaces: post_trending_news used to attach the story's
    own raw image_url (or one scraped via og:image) as-is -- for Google-
    News-sourced stories that scrape resolved the SAME static Google
    branding image for every article regardless of headline (confirmed
    live). Now the image(s) are GENERATED from the story's own text.

    A multi-headline story (the default _story() -- 1 lead + 1 secondary =
    2 headlines) must post a real Threads CAROUSEL, one big card per
    headline, not cram everything onto one image. Real feedback: the
    caption used to repeat everything already on the card(s) -- pure
    duplication. The Threads post TEXT next to the carousel must be
    hashtags only."""
    _mock_charts(monkeypatch, tmp_path)
    posted = []
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_carousel_post",
        lambda image_urls, text="": posted.append((image_urls, text)),
    )
    threads_post.post_trending_news(_story(), market="crypto")
    image_urls, text = posted[0]
    assert len(image_urls) == 2  # lead headline + 1 secondary
    assert all(u.startswith("https://bettor-schwab.onrender.com/chart/") for u in image_urls)
    assert "Bitcoin surges past resistance" not in text  # lives on the cards now, not the caption
    assert "ETF inflows accelerate" not in text
    assert "#Crypto" in text
    # Extracted from the headline's own text on top of the base market tags.
    assert "#Bitcoin" in text


def test_trending_news_bakes_each_headline_and_hashtags_into_its_own_bullet_card(monkeypatch, tmp_path):
    """Direct check that each carousel item IS its own headline's card, not
    a caption next to an unrelated photo -- generate_news_bullet_card must
    be called once per headline with this story's own real text, correct
    index/total, and the shared hashtags -- and only the LEAD card gets the
    story's source (secondary headlines don't carry their own source)."""
    from data import chart_snapshot

    _mock_charts(monkeypatch, tmp_path)
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_carousel_post", lambda image_urls, text="": None)
    captured = []
    real_generate = chart_snapshot.generate_news_bullet_card

    def spy(**kwargs):
        captured.append(kwargs)
        return real_generate(**kwargs)

    monkeypatch.setattr(chart_snapshot, "generate_news_bullet_card", spy)
    threads_post.post_trending_news(_story(), market="crypto")

    assert len(captured) == 2
    assert captured[0]["market"] == "crypto"
    assert captured[0]["headline"] == "Bitcoin surges past resistance"
    assert captured[0]["source"] == "cointelegraph"
    assert captured[0]["index"] == 1 and captured[0]["total"] == 2
    assert captured[1]["headline"] == "ETF inflows accelerate"
    assert captured[1]["source"] == ""  # only the lead headline carries a source
    assert captured[1]["index"] == 2 and captured[1]["total"] == 2
    assert "#Crypto" in captured[0]["hashtags"] and "#Crypto" in captured[1]["hashtags"]


def test_trending_news_with_a_single_headline_skips_the_carousel_entirely(monkeypatch, tmp_path):
    """Only one headline this cycle (no secondary stories) -- a 1-item
    "carousel" isn't a real carousel (Threads itself requires >=2), so this
    must go straight to the single combined-card path without ever
    attempting create_and_publish_carousel_post."""
    from data import chart_snapshot

    _mock_charts(monkeypatch, tmp_path)

    def fail_if_called(*a, **k):
        raise AssertionError("must not attempt a carousel with only one headline")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_carousel_post", fail_if_called)
    monkeypatch.setattr(chart_snapshot, "generate_news_bullet_card", fail_if_called)
    posted = []
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_image_post",
        lambda image_url, text="": posted.append((image_url, text)),
    )
    threads_post.post_trending_news(_story(secondary=[]), market="crypto")
    assert len(posted) == 1


def test_trending_news_falls_back_to_the_single_card_when_the_carousel_post_fails(monkeypatch, tmp_path):
    """The carousel attempt (rendering or posting) can fail for any reason
    -- a real trending post via the older single combined-card path still
    beats posting nothing at all."""
    _mock_charts(monkeypatch, tmp_path)

    def raise_error(image_urls, text=""):
        raise RuntimeError("Threads rejected the carousel")

    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_carousel_post", raise_error)
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_image_post",
        lambda image_url, text="": posted.append((image_url, text)),
    )
    result = threads_post.post_trending_news(_story(), market="crypto")
    assert result is True
    assert len(posted) == 1
    assert posted[0][0].startswith("https://bettor-schwab.onrender.com/chart/")


def test_trending_news_labels_stocks_market(monkeypatch, tmp_path):
    _mock_charts(monkeypatch, tmp_path)
    posted = []
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_image_post",
        lambda image_url, text="": posted.append((image_url, text)),
    )
    threads_post.post_trending_news(_story("Markets rally on rate cut hopes", secondary=[]), market="stocks")
    assert "#StockMarket" in posted[0][1]


def test_trending_news_labels_options_market(monkeypatch, tmp_path):
    """Real, confirmed mislabeling bug found in review: this used to
    collapse every market that wasn't literally "crypto" into "Stocks" --
    so options' own trending-news post rendered indistinguishably from
    the actual stocks service's own posts, and got the wrong hashtags. The
    market label itself now lives on the generated card's own banner (see
    test_generate_news_card_uses_a_distinct_accent_color_per_market) --
    this checks the caption still gets the right per-market hashtags, not
    stocks' own."""
    _mock_charts(monkeypatch, tmp_path)
    posted = []
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_image_post",
        lambda image_url, text="": posted.append((image_url, text)),
    )
    threads_post.post_trending_news(_story("Big tech earnings beat expectations", secondary=[]), market="options")
    text = posted[0][1]
    assert "#OptionsTrading" in text
    assert "#StockMarket" not in text


def test_trending_news_labels_perps_market(monkeypatch, tmp_path):
    _mock_charts(monkeypatch, tmp_path)
    posted = []
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_image_post",
        lambda image_url, text="": posted.append((image_url, text)),
    )
    threads_post.post_trending_news(_story("Prediction markets see record volume", secondary=[]), market="perps")
    text = posted[0][1]
    assert "#Kalshi" in text


def test_trending_news_falls_back_to_text_when_the_card_cant_be_generated(monkeypatch):
    """Card rendering failed (or no RENDER_EXTERNAL_URL to host it, e.g.
    running locally) -- a real trending post (text-only) still beats
    posting nothing at all."""
    from data import chart_snapshot

    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    monkeypatch.setattr(chart_snapshot, "generate_news_card", lambda **kwargs: None)
    threads_post.post_trending_news(_story(secondary=[]), market="crypto")
    assert "Bitcoin surges past resistance" in posted[0]


def test_trending_news_falls_back_to_text_when_the_image_post_itself_fails(monkeypatch, tmp_path):
    _mock_charts(monkeypatch, tmp_path)

    def raise_error(image_url, text=""):
        raise RuntimeError("Threads rejected the image URL")

    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_image_post", raise_error)
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    result = threads_post.post_trending_news(_story(secondary=[]), market="crypto")
    assert result is True
    assert "Bitcoin surges past resistance" in posted[0]


def test_trending_news_respects_the_disable_flag(monkeypatch):
    monkeypatch.setattr(threads_post, "THREADS_POST_ENABLED", False)
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_image_post", lambda image_url, text="": posted.append(text))
    assert threads_post.post_trending_news(_story(), market="crypto") is False
    assert posted == []


def test_trending_news_never_raises_when_both_image_and_text_posting_fail(monkeypatch):
    def raise_error(*args, **kwargs):
        raise RuntimeError("simulated Threads API failure")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_image_post", raise_error)
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", raise_error)
    assert threads_post.post_trending_news(_story(secondary=[]), market="crypto") is False


# ── Recently-posted-story dedup ──────────────────────────────────────────────

def test_is_recent_duplicate_story_true_for_exact_title_match():
    threads_post._record_posted_story("crypto", "Bitcoin surges past resistance")
    assert threads_post._is_recent_duplicate_story("crypto", "Bitcoin surges past resistance") is True  # noqa: SLF001


def test_is_recent_duplicate_story_true_for_a_near_duplicate_title():
    """Same technique crypto_news.py's own cross-outlet corroboration
    uses: sharing 3+ significant words counts as the same real-world
    story even with different wording/outlet."""
    threads_post._record_posted_story("crypto", "Bitcoin surges past key resistance level")
    assert threads_post._is_recent_duplicate_story("crypto", "BTC surges past resistance again today") is True  # noqa: SLF001


def test_is_recent_duplicate_story_false_for_a_genuinely_different_story():
    threads_post._record_posted_story("crypto", "Bitcoin surges past resistance")
    assert threads_post._is_recent_duplicate_story("crypto", "Ethereum staking yields drop sharply") is False  # noqa: SLF001


def test_is_recent_duplicate_story_false_once_aged_out(monkeypatch):
    threads_post._record_posted_story("crypto", "Bitcoin surges past resistance")
    # Push the recorded timestamp back past the max-age window.
    stale_ts = time.time() - threads_post._RECENT_NEWS_MAX_AGE_SEC - 1  # noqa: SLF001
    threads_post._recent_news_cache["crypto"][0]["posted_at"] = stale_ts  # noqa: SLF001
    assert threads_post._is_recent_duplicate_story("crypto", "Bitcoin surges past resistance") is False  # noqa: SLF001


def test_is_recent_duplicate_story_is_scoped_per_market():
    threads_post._record_posted_story("crypto", "Bitcoin surges past resistance")
    assert threads_post._is_recent_duplicate_story("stocks", "Bitcoin surges past resistance") is False  # noqa: SLF001


def test_post_trending_news_skips_a_recently_posted_duplicate_story(monkeypatch):
    threads_post._record_posted_story("crypto", "Bitcoin surges past resistance")
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    result = threads_post.post_trending_news(_story(), market="crypto")
    assert result is True
    assert "nothing notable" in posted[0].lower()


def test_post_trending_news_records_the_story_after_a_successful_text_post(monkeypatch):
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: True)
    result = threads_post.post_trending_news(_story(secondary=[]), market="crypto")
    assert result is True
    assert threads_post._is_recent_duplicate_story("crypto", "Bitcoin surges past resistance") is True  # noqa: SLF001


def test_post_trending_news_does_not_record_anything_on_total_failure(monkeypatch):
    def raise_error(*a, **k):
        raise RuntimeError("simulated Threads API failure")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_image_post", raise_error)
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", raise_error)
    threads_post.post_trending_news(_story(secondary=[]), market="crypto")
    assert threads_post._is_recent_duplicate_story("crypto", "Bitcoin surges past resistance") is False  # noqa: SLF001


# ── "News anchor" persona rewrite integration ────────────────────────────────

def test_post_trending_news_uses_the_anchor_rewritten_headline_when_available(monkeypatch):
    from data import threads_persona
    monkeypatch.setattr(threads_persona, "anchor_rewrite_headline", lambda title, **kw: "BREAKING: BTC blasts through the ceiling")
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    threads_post.post_trending_news(_story(secondary=[]), market="crypto")
    assert "BREAKING: BTC blasts through the ceiling" in posted[0]
    assert "Bitcoin surges past resistance" not in posted[0]


def test_post_trending_news_falls_back_to_the_plain_headline_when_the_anchor_rewrite_returns_none(monkeypatch):
    from data import threads_persona
    monkeypatch.setattr(threads_persona, "anchor_rewrite_headline", lambda title, **kw: None)
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    threads_post.post_trending_news(_story(secondary=[]), market="crypto")
    assert "Bitcoin surges past resistance" in posted[0]


def test_post_trending_news_falls_back_to_the_plain_headline_when_the_anchor_rewrite_raises(monkeypatch):
    from data import threads_persona

    def raise_error(title, **kw):
        raise RuntimeError("simulated HF outage")

    monkeypatch.setattr(threads_persona, "anchor_rewrite_headline", raise_error)
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    result = threads_post.post_trending_news(_story(secondary=[]), market="crypto")
    assert result is True
    assert "Bitcoin surges past resistance" in posted[0]


# ── reply_to_trending_keyword_posts ──────────────────────────────────────────

def _keyword_post(post_id="post-1", text="crypto is having a moment", username="someuser"):
    return {"id": post_id, "text": text, "username": username}


def test_reply_to_trending_keyword_posts_replies_up_to_the_cap(monkeypatch):
    from data import threads_persona
    posts = [_keyword_post(f"post-{i}") for i in range(5)]
    monkeypatch.setattr(threads_post.threads_client, "search_keyword_posts", lambda query, **kw: posts)
    monkeypatch.setattr(threads_persona, "anchor_draft_reply", lambda text, **kw: "Great point!")
    replied_calls = []
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_post",
        lambda text, **kw: replied_calls.append((text, kw.get("reply_to_id"))),
    )
    result = threads_post.reply_to_trending_keyword_posts("crypto", market="crypto", max_replies=2)
    assert result["ok"] is True
    assert len(result["replied"]) == 2
    assert len(replied_calls) == 2


def test_reply_to_trending_keyword_posts_skips_already_replied_posts(monkeypatch):
    from data import threads_persona
    threads_post._record_replied_post("post-1")  # noqa: SLF001
    monkeypatch.setattr(threads_post.threads_client, "search_keyword_posts", lambda query, **kw: [_keyword_post("post-1"), _keyword_post("post-2")])
    monkeypatch.setattr(threads_persona, "anchor_draft_reply", lambda text, **kw: "Great point!")
    replied_calls = []
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_post",
        lambda text, **kw: replied_calls.append(kw.get("reply_to_id")),
    )
    result = threads_post.reply_to_trending_keyword_posts("crypto", market="crypto")
    assert result["replied"][0]["post_id"] == "post-2"
    assert result["skipped_already_replied"] == 1
    assert replied_calls == ["post-2"]


def test_reply_to_trending_keyword_posts_never_posts_a_generic_fallback_reply(monkeypatch):
    """A drafting failure must skip the post entirely -- never fall back to
    a generic template reply (a low-quality reply is worse than none)."""
    from data import threads_persona
    monkeypatch.setattr(threads_post.threads_client, "search_keyword_posts", lambda query, **kw: [_keyword_post("post-1")])
    monkeypatch.setattr(threads_persona, "anchor_draft_reply", lambda text, **kw: None)

    def fail_if_called(*a, **k):
        raise AssertionError("must not post a reply when drafting failed")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", fail_if_called)
    result = threads_post.reply_to_trending_keyword_posts("crypto", market="crypto")
    assert result["replied"] == []


def test_reply_to_trending_keyword_posts_returns_ok_false_when_search_fails(monkeypatch):
    def raise_error(query, **kw):
        raise RuntimeError("No valid Threads access token")

    monkeypatch.setattr(threads_post.threads_client, "search_keyword_posts", raise_error)
    result = threads_post.reply_to_trending_keyword_posts("crypto", market="crypto")
    assert result["ok"] is False
    assert result["replied"] == []


def test_reply_to_trending_keyword_posts_respects_the_disable_flag(monkeypatch):
    monkeypatch.setattr(threads_post, "THREADS_POST_ENABLED", False)

    def fail_if_called(*a, **k):
        raise AssertionError("must not search at all when disabled")

    monkeypatch.setattr(threads_post.threads_client, "search_keyword_posts", fail_if_called)
    result = threads_post.reply_to_trending_keyword_posts("crypto", market="crypto")
    assert result["ok"] is False
    assert result["replied"] == []


def test_reply_to_trending_keyword_posts_records_replies_for_future_dedup(monkeypatch):
    from data import threads_persona
    monkeypatch.setattr(threads_post.threads_client, "search_keyword_posts", lambda query, **kw: [_keyword_post("post-1")])
    monkeypatch.setattr(threads_persona, "anchor_draft_reply", lambda text, **kw: "Great point!")
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text, **kw: True)
    threads_post.reply_to_trending_keyword_posts("crypto", market="crypto")
    assert "post-1" in threads_post._load_replied_posts()  # noqa: SLF001


# ── post_trending_news's no-fresh-story fallback: reply first, not filler ──

def test_post_trending_news_replies_instead_of_posting_filler_when_there_is_no_story(monkeypatch):
    from data import threads_persona
    monkeypatch.setattr(threads_post.threads_client, "search_keyword_posts", lambda query, **kw: [_keyword_post("post-1")])
    monkeypatch.setattr(threads_persona, "anchor_draft_reply", lambda text, **kw: "Great point!")
    reply_calls = []
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_post",
        lambda text, **kw: reply_calls.append((text, kw.get("reply_to_id"))),
    )
    result = threads_post.post_trending_news(None, market="crypto")
    assert result is True
    assert len(reply_calls) == 1
    assert reply_calls[0][1] == "post-1"  # a real reply, not the "nothing notable" filler


def test_post_trending_news_replies_instead_of_filler_for_a_recently_posted_duplicate(monkeypatch):
    from data import threads_persona
    threads_post._record_posted_story("crypto", "Bitcoin surges past resistance")  # noqa: SLF001
    monkeypatch.setattr(threads_post.threads_client, "search_keyword_posts", lambda query, **kw: [_keyword_post("post-1")])
    monkeypatch.setattr(threads_persona, "anchor_draft_reply", lambda text, **kw: "Great point!")
    reply_calls = []
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_post",
        lambda text, **kw: reply_calls.append(kw.get("reply_to_id")),
    )
    result = threads_post.post_trending_news(_story(), market="crypto")
    assert result is True
    assert reply_calls == ["post-1"]


def test_post_trending_news_falls_back_to_filler_only_when_the_reply_round_finds_nothing(monkeypatch):
    monkeypatch.setattr(threads_post.threads_client, "search_keyword_posts", lambda query, **kw: [])
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text, **kw: posted.append(text))
    result = threads_post.post_trending_news(None, market="crypto")
    assert result is True
    assert "nothing notable" in posted[0].lower()


def test_post_trending_news_falls_back_to_filler_when_the_reply_round_itself_raises(monkeypatch):
    def raise_error(*a, **k):
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(threads_post, "reply_to_trending_keyword_posts", raise_error)
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text, **kw: posted.append(text))
    result = threads_post.post_trending_news(None, market="crypto")
    assert result is True
    assert "nothing notable" in posted[0].lower()


# ── post_trending_news's duplicate-story fallback: commentary first ────────

def test_post_trending_news_posts_commentary_instead_of_reply_for_a_duplicate_story(monkeypatch):
    from data import threads_persona
    threads_post._record_posted_story("crypto", "Bitcoin surges past resistance")  # noqa: SLF001
    monkeypatch.setattr(
        threads_persona, "anchor_commentary",
        lambda title, **kw: "This kind of move usually means one thing: momentum traders are just getting started.",
    )

    def fail_if_called(*a, **k):
        raise AssertionError("commentary succeeded -- must not fall through to the reply round")

    monkeypatch.setattr(threads_post, "reply_to_trending_keyword_posts", fail_if_called)
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    result = threads_post.post_trending_news(_story(), market="crypto")
    assert result is True
    assert "momentum traders are just getting started" in posted[0]


def test_post_trending_news_falls_back_to_reply_when_commentary_returns_none_for_a_duplicate(monkeypatch):
    from data import threads_persona
    threads_post._record_posted_story("crypto", "Bitcoin surges past resistance")  # noqa: SLF001
    monkeypatch.setattr(threads_persona, "anchor_commentary", lambda title, **kw: None)
    monkeypatch.setattr(threads_post.threads_client, "search_keyword_posts", lambda query, **kw: [_keyword_post("post-1")])
    monkeypatch.setattr(threads_persona, "anchor_draft_reply", lambda text, **kw: "Great point!")
    reply_calls = []
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_post",
        lambda text, **kw: reply_calls.append(kw.get("reply_to_id")),
    )
    result = threads_post.post_trending_news(_story(), market="crypto")
    assert result is True
    assert reply_calls == ["post-1"]


def test_post_trending_news_does_not_attempt_commentary_when_there_was_no_duplicate_at_all(monkeypatch):
    """story=None from the start (every feed failed) -- there's nothing to
    comment ON, so this must go straight to the reply round, not call the
    commentary model with an empty/missing story."""
    from data import threads_persona

    def fail_if_called(*a, **k):
        raise AssertionError("must not attempt commentary when there was never a duplicate story to comment on")

    monkeypatch.setattr(threads_persona, "anchor_commentary", fail_if_called)
    monkeypatch.setattr(threads_post.threads_client, "search_keyword_posts", lambda query, **kw: [])
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text, **kw: posted.append(text))
    result = threads_post.post_trending_news(None, market="crypto")
    assert result is True
    assert "nothing notable" in posted[0].lower()


# ── _last_known_story / commentary-as-a-picture ─────────────────────────────

def test_last_known_story_returns_none_when_nothing_was_ever_recorded():
    assert threads_post._last_known_story("crypto") is None  # noqa: SLF001


def test_last_known_story_returns_the_most_recently_recorded_story():
    threads_post._record_posted_story("crypto", "Old story", source="decrypt", secondary=["a headline"])  # noqa: SLF001
    threads_post._record_posted_story("crypto", "Newer story", source="cointelegraph", secondary=["b headline"])  # noqa: SLF001
    result = threads_post._last_known_story("crypto")  # noqa: SLF001
    assert result["title"] == "Newer story"
    assert result["source"] == "cointelegraph"
    assert result["secondary"] == ["b headline"]


def test_post_trending_news_comments_on_the_last_known_story_when_the_feed_is_genuinely_empty(monkeypatch):
    """No duplicate this cycle (story=None from the very start -- every feed
    failed), but this market DID post something earlier -- that's still a
    real, on-topic subject worth commenting on instead of giving up."""
    from data import threads_persona
    threads_post._record_posted_story("crypto", "Bitcoin surges past resistance", source="cointelegraph")  # noqa: SLF001
    monkeypatch.setattr(threads_persona, "anchor_commentary", lambda title, **kw: "Still the story everyone's watching.")

    def fail_if_called(*a, **k):
        raise AssertionError("commentary succeeded -- must not fall through to the reply round")

    monkeypatch.setattr(threads_post, "reply_to_trending_keyword_posts", fail_if_called)
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    result = threads_post.post_trending_news(None, market="crypto")
    assert result is True
    assert "Still the story everyone's watching." in posted[0]


def test_post_trending_news_renders_commentary_as_an_image_card_when_possible(monkeypatch, tmp_path):
    from data import threads_persona
    _mock_charts(monkeypatch, tmp_path)
    threads_post._record_posted_story("crypto", "Bitcoin surges past resistance", source="cointelegraph")  # noqa: SLF001
    monkeypatch.setattr(threads_persona, "anchor_commentary", lambda title, **kw: "Still the story everyone's watching.")
    posted_images = []
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_image_post",
        lambda image_url, text="": posted_images.append((image_url, text)),
    )

    def fail_if_called(text):
        raise AssertionError("a real card was generated -- must not fall back to a plain text post")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", fail_if_called)
    result = threads_post.post_trending_news(None, market="crypto")
    assert result is True
    assert len(posted_images) == 1
    assert posted_images[0][0]  # a real image URL was posted, not empty


def test_post_trending_news_falls_back_to_text_when_the_commentary_card_cannot_be_generated(monkeypatch):
    """No RENDER_EXTERNAL_URL / rendering failure -- still posts the real
    commentary as plain text rather than giving up entirely."""
    from data import threads_persona
    threads_post._record_posted_story("crypto", "Bitcoin surges past resistance", source="cointelegraph")  # noqa: SLF001
    monkeypatch.setattr(threads_persona, "anchor_commentary", lambda title, **kw: "Still the story everyone's watching.")
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    result = threads_post.post_trending_news(None, market="crypto")
    assert result is True
    assert "Still the story everyone's watching." in posted[0]
