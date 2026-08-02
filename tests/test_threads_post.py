"""Threads trade-entry/restart-notice posting -- must silently no-op
without a completed login, must never raise (a failure here can never be
allowed to affect real trade execution), and must format/truncate post
text correctly."""
from __future__ import annotations

from data import threads_post


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


def _closes(n=60, base=100.0):
    return [base + (i % 7) * 0.3 - 1.0 for i in range(n)]


def test_maybe_post_trade_entry_chart_skips_when_the_random_roll_misses(monkeypatch):
    monkeypatch.setattr(threads_post, "CHART_SNAPSHOT_PROBABILITY", 0.0)
    result = threads_post.maybe_post_trade_entry_chart(
        ticker="AAPL", market="stocks", closes=_closes(), entry_price=100.0,
        take_profit_price=101.0, stop_loss_price=99.0, dry_run=False,
    )
    assert result is False


def test_maybe_post_trade_entry_chart_respects_the_disable_flag(monkeypatch):
    monkeypatch.setattr(threads_post, "THREADS_POST_ENABLED", False)
    monkeypatch.setattr(threads_post, "CHART_SNAPSHOT_PROBABILITY", 1.0)
    result = threads_post.maybe_post_trade_entry_chart(
        ticker="AAPL", market="stocks", closes=_closes(), entry_price=100.0,
        take_profit_price=101.0, stop_loss_price=99.0, dry_run=False,
    )
    assert result is False


def test_maybe_post_trade_entry_chart_skips_without_enough_price_history(monkeypatch):
    monkeypatch.setattr(threads_post, "CHART_SNAPSHOT_PROBABILITY", 1.0)
    result = threads_post.maybe_post_trade_entry_chart(
        ticker="AAPL", market="stocks", closes=[100.0, 100.5], entry_price=100.5,
        take_profit_price=101.0, stop_loss_price=99.0, dry_run=False,
    )
    assert result is False


def test_maybe_post_trade_entry_chart_skips_without_a_known_public_url(monkeypatch, tmp_path):
    from data import chart_snapshot

    monkeypatch.setattr(threads_post, "CHART_SNAPSHOT_PROBABILITY", 1.0)
    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    monkeypatch.delenv("RENDER_EXTERNAL_URL", raising=False)
    result = threads_post.maybe_post_trade_entry_chart(
        ticker="AAPL", market="stocks", closes=_closes(), entry_price=100.0,
        take_profit_price=101.0, stop_loss_price=99.0, dry_run=False,
    )
    assert result is False


def test_maybe_post_trade_entry_chart_posts_the_image_when_everything_lines_up(monkeypatch, tmp_path):
    from data import chart_snapshot

    monkeypatch.setattr(threads_post, "CHART_SNAPSHOT_PROBABILITY", 1.0)
    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://bettor-schwab.onrender.com")

    captured = {}
    monkeypatch.setattr(
        threads_post.threads_client, "create_and_publish_image_post",
        lambda image_url, text="": captured.update(image_url=image_url, text=text) or "post-1",
    )

    result = threads_post.maybe_post_trade_entry_chart(
        ticker="AAPL", market="stocks", closes=_closes(), entry_price=100.0,
        take_profit_price=101.0, stop_loss_price=99.0, side="long", dry_run=False,
    )
    assert result is True
    assert captured["image_url"].startswith("https://bettor-schwab.onrender.com/chart/")
    assert "Alpaca Stocks" in captured["text"]
    assert "AAPL" in captured["text"]
    assert "#StockMarket" in captured["text"]


def test_maybe_post_trade_entry_chart_never_raises_on_api_failure(monkeypatch, tmp_path):
    from data import chart_snapshot

    monkeypatch.setattr(threads_post, "CHART_SNAPSHOT_PROBABILITY", 1.0)
    monkeypatch.setattr(chart_snapshot, "CHARTS_DIR", tmp_path / "charts")
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://bettor-schwab.onrender.com")

    def raise_error(image_url, text=""):
        raise RuntimeError("simulated Threads API failure")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_image_post", raise_error)
    result = threads_post.maybe_post_trade_entry_chart(
        ticker="AAPL", market="stocks", closes=_closes(), entry_price=100.0,
        take_profit_price=101.0, stop_loss_price=99.0, dry_run=False,
    )
    assert result is False


def test_trending_news_reports_nothing_notable_with_no_headlines(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    result = threads_post.post_trending_news([], market="crypto")
    assert result is True
    assert "nothing notable" in posted[0].lower()


def test_trending_news_lists_every_headline_and_labels_the_market(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    threads_post.post_trending_news(["Bitcoin surges past resistance", "ETF inflows accelerate"], market="crypto")
    text = posted[0]
    assert "Crypto trending news" in text
    assert "Bitcoin surges past resistance" in text
    assert "ETF inflows accelerate" in text


def test_trending_news_labels_stocks_market(monkeypatch):
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    threads_post.post_trending_news(["Markets rally on rate cut hopes"], market="stocks")
    assert "Stocks trending news" in posted[0]


def test_trending_news_respects_the_disable_flag(monkeypatch):
    monkeypatch.setattr(threads_post, "THREADS_POST_ENABLED", False)
    posted = []
    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", lambda text: posted.append(text))
    assert threads_post.post_trending_news(["headline"], market="crypto") is False
    assert posted == []


def test_trending_news_never_raises_on_api_failure(monkeypatch):
    def raise_error(text):
        raise RuntimeError("simulated Threads API failure")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", raise_error)
    assert threads_post.post_trending_news(["headline"], market="crypto") is False
