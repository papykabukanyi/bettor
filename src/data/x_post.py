"""Posts a real-time note to X (Twitter) every time the Kalshi Perps bot
enters a real trade -- ticker, side, entry price, take-profit/stop-loss
targets, and why the model/technical signal triggered the entry.

Best-effort only, by design: a failure here must NEVER block or delay real
trade execution, mirroring how a failed news-sentiment fetch never blocks
the trading loop either (see crypto_news.py's own established discipline).
Every function in this module catches its own exceptions and returns a
plain bool/None rather than raising.

Posting requires OAuth 1.0a user-context credentials: TWITTER_CONSUMER_KEY/
TWITTER_CONSUMER_SECRET (the app's own keys) PLUS TWITTER_ACCESS_TOKEN/
TWITTER_ACCESS_TOKEN_SECRET (a token tied to the specific account that will
post -- generated once on the same developer-portal "Keys and tokens" page,
no interactive login needed after that, unlike Schwab's OAuth). A Bearer
Token or an OAuth 2.0 Client ID/Secret alone cannot post tweets -- those are
app-only/read-auth and a separate per-user consent flow, respectively.
"""
from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

TWITTER_CONSUMER_KEY = os.getenv("TWITTER_CONSUMER_KEY", "")
TWITTER_CONSUMER_SECRET = os.getenv("TWITTER_CONSUMER_SECRET", "")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN", "")
TWITTER_ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_TOKEN_SECRET", "")
TWITTER_POST_ENABLED = str(os.getenv("TWITTER_POST_ENABLED", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}

_TWEET_MAX_CHARS = 280

_client_cache: dict[str, Any] = {"client": None, "checked": False}


def is_configured() -> bool:
    """True once every credential posting actually needs is present --
    surfaced to /api/status so the dashboard can show whether this is wired
    up yet without needing to check server logs."""
    return bool(
        TWITTER_CONSUMER_KEY and TWITTER_CONSUMER_SECRET
        and TWITTER_ACCESS_TOKEN and TWITTER_ACCESS_TOKEN_SECRET
    )


def _get_client() -> Any | None:
    """Cached after the first attempt (success OR failure) -- avoids
    re-importing tweepy and rebuilding a client every single trade entry,
    but re-checked per process (not persisted), so a mid-run env var fix
    takes effect on the next restart same as every other credential here."""
    if _client_cache["checked"]:
        return _client_cache["client"]
    _client_cache["checked"] = True
    if not is_configured():
        logger.info(
            "[x_post] Twitter/X posting not fully configured (missing access "
            "token) -- skipping. Consumer key/secret + bearer token + OAuth "
            "2.0 client id/secret alone are not enough to post."
        )
        return None
    try:
        import tweepy
        client = tweepy.Client(
            consumer_key=TWITTER_CONSUMER_KEY, consumer_secret=TWITTER_CONSUMER_SECRET,
            access_token=TWITTER_ACCESS_TOKEN, access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
        )
        _client_cache["client"] = client
        return client
    except Exception as exc:
        logger.warning("[x_post] failed to initialize Twitter client: %s", exc)
        return None


def _format_trade_entry_text(
    *, ticker: str, side: str, entry_price: float, take_profit_price: float,
    stop_loss_price: float, reason: str, dry_run: bool,
) -> str:
    tag = "[SIMULATED] " if dry_run else ""
    direction = "SHORT" if side == "short" else "LONG"
    text = (
        f"{tag}Kalshi Perps: {direction} {ticker} @ {entry_price:.4f}\n"
        f"Take-profit: {take_profit_price:.4f} | Stop-loss: {stop_loss_price:.4f}\n"
        f"Why: {reason}"
    )
    if len(text) > _TWEET_MAX_CHARS:
        text = text[: _TWEET_MAX_CHARS - 1] + "…"
    return text


def post_trade_entry(
    *, ticker: str, side: str, entry_price: float, take_profit_price: float,
    stop_loss_price: float, reason: str, dry_run: bool,
) -> bool:
    """Posts one tweet describing a just-opened position. Returns whether it
    actually posted (False for "not configured" and for any real failure
    alike -- callers should never branch on the failure reason, only log it,
    since this must never affect trading logic)."""
    if not TWITTER_POST_ENABLED:
        return False
    client = _get_client()
    if client is None:
        return False
    text = _format_trade_entry_text(
        ticker=ticker, side=side, entry_price=entry_price, take_profit_price=take_profit_price,
        stop_loss_price=stop_loss_price, reason=reason, dry_run=dry_run,
    )
    try:
        client.create_tweet(text=text)
        return True
    except Exception as exc:
        logger.warning("[x_post] failed to post trade entry for %s: %s", ticker, exc)
        return False


def post_restart_notice(message: str = "MMM has Restarted") -> bool:
    """Posts a short note once per process boot -- called from the SAME
    once-per-boot startup path that already logs "Startup data collect
    completed" (see app_kalshi.py's `_ensure_background_jobs_started`), so
    this fires once per real deploy AND once per crash-triggered restart
    (the process itself has no way to distinguish the two from the inside --
    Render's own event log is the only thing that actually knows which).
    Same best-effort, never-raise contract as post_trade_entry()."""
    if not TWITTER_POST_ENABLED:
        return False
    client = _get_client()
    if client is None:
        return False
    try:
        client.create_tweet(text=message[:_TWEET_MAX_CHARS])
        return True
    except Exception as exc:
        logger.warning("[x_post] failed to post restart notice: %s", exc)
        return False
