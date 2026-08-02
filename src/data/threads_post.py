"""Posts a real-time note to Meta Threads every time the Kalshi Perps bot
enters a real trade -- ticker, side, entry price, take-profit/stop-loss
targets, and why the model/technical signal triggered the entry. Also
posts a short notice on every process boot.

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


def post_restart_notice(message: str = "MMM has Restarted") -> bool:
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
