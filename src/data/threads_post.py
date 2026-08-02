"""Posts a real-time note to Meta Threads every time the Kalshi Perps bot
(or the Alpaca stocks bot) enters a real trade -- ticker, side, entry
price, take-profit/stop-loss targets, and why the model/technical signal
triggered the entry. Also posts a short notice on every process boot, an
hourly status update (open positions, or "flat", plus today's realized
P&L), and a 30-minute trending-news digest (see post_trending_news) meant
to surface whatever news might be influencing the bot's own decisions
right now, not just report positions after the fact.

Best-effort only, by design: a failure here must NEVER block or delay real
trade execution, mirroring how a failed news-sentiment fetch never blocks
the trading loop either (see crypto_news.py's own established discipline).
Every function in this module catches its own exceptions and returns a
plain bool rather than raising.

Posting requires a completed interactive Threads login (see
threads_client.get_authorization_url()) -- THREADS_APP_ID/THREADS_APP_SECRET
alone cannot post anything on their own.
"""
from __future__ import annotations

import logging
import os

from data import threads_client

logger = logging.getLogger(__name__)

THREADS_POST_ENABLED = str(os.getenv("THREADS_POST_ENABLED", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}

_THREADS_POST_MAX_CHARS = 500


def is_configured() -> bool:
    """True once a real login has actually completed (a token is present)
    -- surfaced to /api/status so the dashboard can show whether this is
    wired up yet without needing to check server logs."""
    return bool(threads_client.get_valid_access_token())


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
    if len(text) > _THREADS_POST_MAX_CHARS:
        text = text[: _THREADS_POST_MAX_CHARS - 1] + "…"
    return text


def post_trade_entry(
    *, ticker: str, side: str, entry_price: float, take_profit_price: float,
    stop_loss_price: float, reason: str, dry_run: bool,
) -> bool:
    """Posts one Threads post describing a just-opened position. Returns
    whether it actually posted (False for "not configured" and for any
    real failure alike -- callers should never branch on the failure
    reason, only log it, since this must never affect trading logic)."""
    if not THREADS_POST_ENABLED:
        return False
    text = _format_trade_entry_text(
        ticker=ticker, side=side, entry_price=entry_price, take_profit_price=take_profit_price,
        stop_loss_price=stop_loss_price, reason=reason, dry_run=dry_run,
    )
    try:
        threads_client.create_and_publish_post(text)
        return True
    except Exception as exc:
        logger.warning("[threads_post] failed to post trade entry for %s: %s", ticker, exc)
        return False


def post_restart_notice(message: str = "Money Bot has restarted!") -> bool:
    """Posts a short note once per process boot -- see app_kalshi.py's
    `_ensure_background_jobs_started` for the once-per-boot call site. Same
    best-effort, never-raise contract as post_trade_entry()."""
    if not THREADS_POST_ENABLED:
        return False
    try:
        threads_client.create_and_publish_post(message[:_THREADS_POST_MAX_CHARS])
        return True
    except Exception as exc:
        logger.warning("[threads_post] failed to post restart notice: %s", exc)
        return False


def _format_hourly_status_text(*, positions: list[dict], today_realized_pnl_usd: float | None) -> str:
    if not positions:
        text = "Hourly status: flat (no open positions) -- scanning for the next trade."
    else:
        count = len(positions)
        lines = [f"Hourly status: {count} open position{'s' if count != 1 else ''}"]
        for p in positions:
            direction = "SHORT" if p.get("side") == "short" else "LONG"
            held_minutes = p.get("held_minutes")
            held_str = f"{held_minutes:.0f}min" if held_minutes is not None else "?"
            entry_price = p.get("entry_price", 0.0)
            take_profit_price = p.get("take_profit_price", entry_price)
            stop_loss_price = p.get("stop_loss_price", entry_price)
            lines.append(
                f"{direction} {p.get('ticker', '?')} @ {entry_price:.4f} (held {held_str}) "
                f"TP {take_profit_price:.4f} / SL {stop_loss_price:.4f}"
            )
        text = "\n".join(lines)
    if today_realized_pnl_usd is not None:
        text += f"\nToday's P&L: {today_realized_pnl_usd:+.2f}"
    if len(text) > _THREADS_POST_MAX_CHARS:
        text = text[: _THREADS_POST_MAX_CHARS - 1] + "…"
    return text


def post_hourly_status(*, positions: list[dict], today_realized_pnl_usd: float | None = None) -> bool:
    """Posts a status update every hour regardless of whether a trade
    happened -- what position(s) the bot is currently holding (or that
    it's flat), plus today's realized P&L. Same best-effort, never-raise
    contract as the other posts here."""
    if not THREADS_POST_ENABLED:
        return False
    text = _format_hourly_status_text(positions=positions, today_realized_pnl_usd=today_realized_pnl_usd)
    try:
        threads_client.create_and_publish_post(text)
        return True
    except Exception as exc:
        logger.warning("[threads_post] failed to post hourly status: %s", exc)
        return False


def _format_trending_news_text(*, headlines: list[str], market: str) -> str:
    label = "Crypto" if market == "crypto" else "Stocks"
    if not headlines:
        text = f"{label} trending news: nothing notable right now."
    else:
        lines = [f"{label} trending news (may be influencing current decisions):"]
        lines.extend(f"- {h}" for h in headlines)
        text = "\n".join(lines)
    if len(text) > _THREADS_POST_MAX_CHARS:
        text = text[: _THREADS_POST_MAX_CHARS - 1] + "…"
    return text


def post_trending_news(headlines: list[str], *, market: str) -> bool:
    """Posts a digest of what's currently trending in crypto or stock news
    -- the same headlines (or general-market equivalent) feeding into
    sentiment_score for the direction models, made visible rather than
    staying an invisible input nobody can see. Runs every 30 minutes (see
    app_kalshi.py's/alpaca_server.py's own scheduled job) independent of
    whether any trade happened. Same best-effort, never-raise contract as
    every other post here."""
    if not THREADS_POST_ENABLED:
        return False
    text = _format_trending_news_text(headlines=headlines, market=market)
    try:
        threads_client.create_and_publish_post(text)
        return True
    except Exception as exc:
        logger.warning("[threads_post] failed to post trending news: %s", exc)
        return False
