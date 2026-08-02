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


def test_post_trade_entry_never_raises_on_api_failure(monkeypatch):
    def raise_error(text):
        raise RuntimeError("simulated Threads API failure")

    monkeypatch.setattr(threads_post.threads_client, "create_and_publish_post", raise_error)
    result = threads_post.post_trade_entry(
        ticker="KXBTCPERP", side="long", entry_price=6.5, take_profit_price=6.6,
        stop_loss_price=6.4, reason="test", dry_run=False,
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
