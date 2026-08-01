"""X (Twitter) trade-entry posting -- must silently no-op without full
credentials, must never raise (a failure here can never be allowed to
affect real trade execution), and must format/truncate tweet text
correctly."""
from __future__ import annotations

from data import x_post


def _reset_client_cache():
    x_post._client_cache["client"] = None  # noqa: SLF001
    x_post._client_cache["checked"] = False  # noqa: SLF001


def _set_all_credentials(monkeypatch):
    monkeypatch.setattr(x_post, "TWITTER_CONSUMER_KEY", "ck")
    monkeypatch.setattr(x_post, "TWITTER_CONSUMER_SECRET", "cs")
    monkeypatch.setattr(x_post, "TWITTER_ACCESS_TOKEN", "at")
    monkeypatch.setattr(x_post, "TWITTER_ACCESS_TOKEN_SECRET", "ats")


class _FakeClient:
    def __init__(self):
        self.tweets: list[str] = []
        self.raise_on_create = False

    def create_tweet(self, *, text):
        if self.raise_on_create:
            raise RuntimeError("simulated X API failure")
        self.tweets.append(text)


def test_is_configured_false_when_any_credential_missing(monkeypatch):
    monkeypatch.setattr(x_post, "TWITTER_CONSUMER_KEY", "ck")
    monkeypatch.setattr(x_post, "TWITTER_CONSUMER_SECRET", "cs")
    monkeypatch.setattr(x_post, "TWITTER_ACCESS_TOKEN", "")
    monkeypatch.setattr(x_post, "TWITTER_ACCESS_TOKEN_SECRET", "")
    assert x_post.is_configured() is False


def test_is_configured_true_when_all_four_present(monkeypatch):
    _set_all_credentials(monkeypatch)
    assert x_post.is_configured() is True


def test_post_trade_entry_skips_silently_without_full_credentials(monkeypatch):
    monkeypatch.setattr(x_post, "TWITTER_ACCESS_TOKEN", "")
    monkeypatch.setattr(x_post, "TWITTER_ACCESS_TOKEN_SECRET", "")
    _reset_client_cache()
    result = x_post.post_trade_entry(
        ticker="KXBTCPERP", side="long", entry_price=6.5, take_profit_price=6.6,
        stop_loss_price=6.4, reason="test", dry_run=False,
    )
    assert result is False


def test_post_trade_entry_respects_the_disable_flag(monkeypatch):
    _set_all_credentials(monkeypatch)
    monkeypatch.setattr(x_post, "TWITTER_POST_ENABLED", False)
    fake = _FakeClient()
    monkeypatch.setattr(x_post, "_get_client", lambda: fake)
    result = x_post.post_trade_entry(
        ticker="KXBTCPERP", side="long", entry_price=6.5, take_profit_price=6.6,
        stop_loss_price=6.4, reason="test", dry_run=False,
    )
    assert result is False
    assert fake.tweets == []


def test_post_trade_entry_posts_formatted_text_when_configured(monkeypatch):
    _set_all_credentials(monkeypatch)
    fake = _FakeClient()
    monkeypatch.setattr(x_post, "_get_client", lambda: fake)
    result = x_post.post_trade_entry(
        ticker="KXBTCPERP", side="long", entry_price=6.5, take_profit_price=6.63,
        stop_loss_price=6.4, reason="dip signal + model confidence 0.61", dry_run=False,
    )
    assert result is True
    assert len(fake.tweets) == 1
    text = fake.tweets[0]
    assert "KXBTCPERP" in text
    assert "LONG" in text
    assert "6.5000" in text
    assert "6.6300" in text
    assert "6.4000" in text
    assert "dip signal" in text
    assert "[SIMULATED]" not in text


def test_post_trade_entry_marks_dry_run_trades_as_simulated(monkeypatch):
    _set_all_credentials(monkeypatch)
    fake = _FakeClient()
    monkeypatch.setattr(x_post, "_get_client", lambda: fake)
    x_post.post_trade_entry(
        ticker="KXBTCPERP", side="short", entry_price=6.5, take_profit_price=6.4,
        stop_loss_price=6.6, reason="test", dry_run=True,
    )
    assert "[SIMULATED]" in fake.tweets[0]
    assert "SHORT" in fake.tweets[0]


def test_post_trade_entry_truncates_at_280_chars(monkeypatch):
    _set_all_credentials(monkeypatch)
    fake = _FakeClient()
    monkeypatch.setattr(x_post, "_get_client", lambda: fake)
    x_post.post_trade_entry(
        ticker="KXBTCPERP", side="long", entry_price=6.5, take_profit_price=6.6,
        stop_loss_price=6.4, reason="x" * 500, dry_run=False,
    )
    assert len(fake.tweets[0]) <= 280


def test_post_trade_entry_never_raises_on_api_failure(monkeypatch):
    _set_all_credentials(monkeypatch)
    fake = _FakeClient()
    fake.raise_on_create = True
    monkeypatch.setattr(x_post, "_get_client", lambda: fake)
    result = x_post.post_trade_entry(
        ticker="KXBTCPERP", side="long", entry_price=6.5, take_profit_price=6.6,
        stop_loss_price=6.4, reason="test", dry_run=False,
    )
    assert result is False


def test_post_restart_notice_posts_the_default_message(monkeypatch):
    _set_all_credentials(monkeypatch)
    fake = _FakeClient()
    monkeypatch.setattr(x_post, "_get_client", lambda: fake)
    result = x_post.post_restart_notice()
    assert result is True
    assert fake.tweets == ["MMM has Restarted"]


def test_post_restart_notice_accepts_a_custom_message(monkeypatch):
    _set_all_credentials(monkeypatch)
    fake = _FakeClient()
    monkeypatch.setattr(x_post, "_get_client", lambda: fake)
    x_post.post_restart_notice("custom restart message")
    assert fake.tweets == ["custom restart message"]


def test_post_restart_notice_skips_silently_without_credentials(monkeypatch):
    monkeypatch.setattr(x_post, "TWITTER_ACCESS_TOKEN", "")
    monkeypatch.setattr(x_post, "TWITTER_ACCESS_TOKEN_SECRET", "")
    _reset_client_cache()
    assert x_post.post_restart_notice() is False


def test_post_restart_notice_respects_the_disable_flag(monkeypatch):
    _set_all_credentials(monkeypatch)
    monkeypatch.setattr(x_post, "TWITTER_POST_ENABLED", False)
    fake = _FakeClient()
    monkeypatch.setattr(x_post, "_get_client", lambda: fake)
    assert x_post.post_restart_notice() is False
    assert fake.tweets == []


def test_post_restart_notice_never_raises_on_api_failure(monkeypatch):
    _set_all_credentials(monkeypatch)
    fake = _FakeClient()
    fake.raise_on_create = True
    monkeypatch.setattr(x_post, "_get_client", lambda: fake)
    assert x_post.post_restart_notice() is False


def test_get_client_caches_after_first_check(monkeypatch):
    _set_all_credentials(monkeypatch)
    _reset_client_cache()
    monkeypatch.setattr(x_post, "TWITTER_ACCESS_TOKEN", "")  # not configured
    assert x_post._get_client() is None  # noqa: SLF001
    # Even if credentials become valid afterward, the cached "checked" result
    # (None, already computed) is reused within THIS process -- matches every
    # other cached-client pattern in this codebase (e.g. schwab_model's model
    # cache), a fresh deploy/restart is what picks up a new value.
    monkeypatch.setattr(x_post, "TWITTER_ACCESS_TOKEN", "at")
    assert x_post._get_client() is None  # noqa: SLF001
