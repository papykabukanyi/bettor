"""Generates a small PNG chart of a trade's recent price action + entry/
take-profit/stop-loss levels, for the occasional Threads chart-snapshot
post (see threads_post.maybe_post_trade_entry_chart) -- meant to show
followers not just THAT a trade was entered but the actual expectation
(where it profits, where it's cut) at a glance.

Pillow, not matplotlib: a real, confirmed OOM incident this session (see
alpaca_crypto_data.py's own docstring) made new-dependency memory weight a
real constraint on these 512Mi services, not a style preference --
drawing a line + a few horizontal reference levels doesn't need
matplotlib's full plotting stack.

Best-effort only, by design, same as every other Threads-adjacent module
here: chart generation NEVER raises out to its caller (returns None on any
failure) and must never affect trade execution.
"""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
CHARTS_DIR = Path(os.getenv("CHART_SNAPSHOT_DIR", str(ROOT_DIR / "data" / "charts")))
# Render's disk is small and shared with everything else this process
# writes -- cap how many chart files accumulate rather than growing
# unbounded across a long-running instance.
MAX_STORED_CHARTS = int(os.getenv("CHART_SNAPSHOT_MAX_STORED", "40") or "40")

_WIDTH, _HEIGHT = 800, 450
_MARGIN = 50
_RIGHT_LABEL_WIDTH = 90
_BG = (10, 12, 17)
_AXIS = (35, 40, 56)
_PRICE_LINE = (59, 130, 246)
_ENTRY_COLOR = (231, 236, 245)
_TP_COLOR = (34, 197, 94)
_SL_COLOR = (239, 68, 68)
_TEXT_COLOR = (231, 236, 245)


def _prune_old_charts() -> None:
    try:
        files = sorted(CHARTS_DIR.glob("*.png"), key=lambda p: p.stat().st_mtime)
        excess = len(files) - MAX_STORED_CHARTS
        for f in files[:max(0, excess)]:
            f.unlink(missing_ok=True)
    except Exception as exc:
        logger.debug("[chart_snapshot] prune failed (non-fatal): %s", exc)


def _sanitize(ticker: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in ticker)


def generate_entry_chart(
    *, ticker: str, market: str, closes: list[float], entry_price: float,
    take_profit_price: float, stop_loss_price: float, side: str = "long",
) -> Path | None:
    """Renders recent close prices as a line, with horizontal dashed-style
    reference levels for entry/take-profit/stop-loss. Returns the saved
    PNG's path, or None if there isn't enough data or rendering fails --
    callers must treat None as "skip this post", never raise."""
    if len(closes) < 5:
        return None
    try:
        from PIL import Image, ImageDraw
    except Exception as exc:
        logger.warning("[chart_snapshot] Pillow unavailable: %s", exc)
        return None

    try:
        img = Image.new("RGB", (_WIDTH, _HEIGHT), _BG)
        draw = ImageDraw.Draw(img)

        plot_left, plot_right = _MARGIN, _WIDTH - _MARGIN - _RIGHT_LABEL_WIDTH
        plot_top, plot_bottom = 60, _HEIGHT - _MARGIN

        all_values = list(closes) + [entry_price, take_profit_price, stop_loss_price]
        lo, hi = min(all_values), max(all_values)
        span = (hi - lo) or max(abs(hi), 1e-9)
        pad = span * 0.08
        lo, hi = lo - pad, hi + pad
        span = hi - lo

        def y_for(value: float) -> float:
            return plot_bottom - ((value - lo) / span) * (plot_bottom - plot_top)

        def x_for(idx: int) -> float:
            if len(closes) <= 1:
                return plot_left
            return plot_left + (idx / (len(closes) - 1)) * (plot_right - plot_left)

        draw.rectangle([plot_left, plot_top, plot_right, plot_bottom], outline=_AXIS, width=1)

        def dashed_hline(value: float, color: tuple[int, int, int], label: str) -> None:
            y = y_for(value)
            x = plot_left
            while x < plot_right:
                draw.line([(x, y), (min(x + 8, plot_right), y)], fill=color, width=2)
                x += 14
            draw.text((plot_right + 4, y - 6), label, fill=color)

        dashed_hline(take_profit_price, _TP_COLOR, f"TP {take_profit_price:.4f}")
        dashed_hline(stop_loss_price, _SL_COLOR, f"SL {stop_loss_price:.4f}")
        dashed_hline(entry_price, _ENTRY_COLOR, f"Entry {entry_price:.4f}")

        points = [(x_for(i), y_for(v)) for i, v in enumerate(closes)]
        if len(points) >= 2:
            draw.line(points, fill=_PRICE_LINE, width=2)

        direction = "SHORT" if side == "short" else "LONG"
        title = f"{market.upper()} -- {direction} {ticker}"
        draw.text((_MARGIN, 20), title, fill=_TEXT_COLOR)

        CHARTS_DIR.mkdir(parents=True, exist_ok=True)
        filename = f"{_sanitize(market)}_{_sanitize(ticker)}_{int(time.time())}.png"
        out_path = CHARTS_DIR / filename
        img.save(out_path, format="PNG")
        _prune_old_charts()
        return out_path
    except Exception as exc:
        logger.warning("[chart_snapshot] rendering failed for %s: %s", ticker, exc)
        return None
    finally:
        try:
            del img, draw
        except Exception:
            pass


_SENTIMENT_ROW_HEIGHT = 26
_SENTIMENT_TOP_MARGIN = 60
_SENTIMENT_BOTTOM_MARGIN = 30
_SENTIMENT_LABEL_WIDTH = 100
_SENTIMENT_SCORE_WIDTH = 70
_SENTIMENT_BAR_HALF_WIDTH = 260
_SENTIMENT_POS_COLOR = (34, 197, 94)
_SENTIMENT_NEG_COLOR = (239, 68, 68)
_SENTIMENT_NEUTRAL_COLOR = (100, 106, 122)
_SENTIMENT_AXIS_COLOR = (35, 40, 56)
# Every ticker on a large watchlist (crypto's 36 pairs) in one image would
# be unreadable -- keep the snapshot to the tickers with something actually
# worth showing: the most bullish and most bearish, half and half.
MAX_SENTIMENT_ROWS = int(os.getenv("CHART_SENTIMENT_MAX_ROWS", "20") or "20")


def generate_sentiment_snapshot(*, market: str, ticker_sentiments: list[dict[str, Any]]) -> Path | None:
    """Renders a per-ticker sentiment bar chart -- one row per ticker,
    a bar extending right (green, bullish) or left (red, bearish) from a
    zero-line, sized by |sentiment_score|. `ticker_sentiments` items need
    "ticker" and "sentiment_score" (from *_news.get_sentiment(), already
    real per-ticker news sentiment, not invented for this chart). Returns
    None (skip posting) on empty input or any rendering failure."""
    rows = [r for r in ticker_sentiments if r.get("ticker") and r.get("sentiment_score") is not None]
    if not rows:
        return None
    try:
        from PIL import Image, ImageDraw
    except Exception as exc:
        logger.warning("[chart_snapshot] Pillow unavailable: %s", exc)
        return None

    rows.sort(key=lambda r: r["sentiment_score"], reverse=True)
    if len(rows) > MAX_SENTIMENT_ROWS:
        half = MAX_SENTIMENT_ROWS // 2
        rows = rows[:half] + rows[-half:]

    width = _SENTIMENT_LABEL_WIDTH + _SENTIMENT_BAR_HALF_WIDTH * 2 + _SENTIMENT_SCORE_WIDTH + _MARGIN * 2
    height = _SENTIMENT_TOP_MARGIN + len(rows) * _SENTIMENT_ROW_HEIGHT + _SENTIMENT_BOTTOM_MARGIN

    try:
        img = Image.new("RGB", (width, height), _BG)
        draw = ImageDraw.Draw(img)

        draw.text((_MARGIN, 20), f"{market.upper()} -- Sentiment Snapshot", fill=_TEXT_COLOR)

        zero_x = _MARGIN + _SENTIMENT_LABEL_WIDTH + _SENTIMENT_BAR_HALF_WIDTH
        draw.line([(zero_x, _SENTIMENT_TOP_MARGIN - 5), (zero_x, height - _SENTIMENT_BOTTOM_MARGIN)], fill=_SENTIMENT_AXIS_COLOR, width=1)

        for i, row in enumerate(rows):
            y = _SENTIMENT_TOP_MARGIN + i * _SENTIMENT_ROW_HEIGHT
            score = max(-1.0, min(1.0, float(row["sentiment_score"])))
            ticker = str(row["ticker"])
            draw.text((_MARGIN, y + 5), ticker[:14], fill=_TEXT_COLOR)

            bar_len = abs(score) * _SENTIMENT_BAR_HALF_WIDTH
            color = _SENTIMENT_POS_COLOR if score > 0.02 else _SENTIMENT_NEG_COLOR if score < -0.02 else _SENTIMENT_NEUTRAL_COLOR
            bar_top, bar_bottom = y + 3, y + _SENTIMENT_ROW_HEIGHT - 8
            if score >= 0:
                draw.rectangle([zero_x, bar_top, zero_x + bar_len, bar_bottom], fill=color)
            else:
                draw.rectangle([zero_x - bar_len, bar_top, zero_x, bar_bottom], fill=color)

            score_x = _MARGIN + _SENTIMENT_LABEL_WIDTH + _SENTIMENT_BAR_HALF_WIDTH * 2 + 10
            draw.text((score_x, y + 5), f"{score:+.2f}", fill=color)

        CHARTS_DIR.mkdir(parents=True, exist_ok=True)
        filename = f"sentiment_{_sanitize(market)}_{int(time.time())}.png"
        out_path = CHARTS_DIR / filename
        img.save(out_path, format="PNG")
        _prune_old_charts()
        return out_path
    except Exception as exc:
        logger.warning("[chart_snapshot] sentiment snapshot rendering failed for %s: %s", market, exc)
        return None
    finally:
        try:
            del img, draw
        except Exception:
            pass


def public_url_for(chart_path: Path) -> str | None:
    """Builds the publicly-fetchable URL Threads' own servers need to
    actually retrieve the image (Threads' media-container API takes an
    image_url it fetches itself, not a raw upload) -- Render auto-injects
    RENDER_EXTERNAL_URL for every web service, so this needs no per-service
    config. None (skip posting) if that's unset, e.g. running locally."""
    base_url = os.getenv("RENDER_EXTERNAL_URL", "")
    if not base_url:
        return None
    return f"{base_url.rstrip('/')}/chart/{chart_path.name}"
