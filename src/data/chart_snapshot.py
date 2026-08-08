"""Generates PNG charts for Threads image posts -- a trade's recent
candlestick price action + entry/take-profit/stop-loss/exit levels (see
threads_post.post_trade_entry_chart / post_trade_exit_chart), and a
per-ticker sentiment snapshot (see threads_post.post_sentiment_snapshot).
Meant to show followers not just THAT something happened but what it
actually looks like, at a glance, on a phone screen.

Pillow, not matplotlib: a real, confirmed OOM incident this session (see
alpaca_crypto_data.py's own docstring) made new-dependency memory weight a
real constraint on these 512Mi services, not a style preference --
drawing lines/bars + reference levels doesn't need matplotlib's full
plotting stack. Text uses Pillow's own bundled scalable font
(ImageFont.load_default(size=N), Pillow >=10.1 -- no OS font file
dependency, so this renders identically on Render as it does locally)
instead of the earlier tiny fixed-size bitmap default, which read as
cramped and hard to read on a phone.

Colors follow the dataviz skill's diverging-pair formula (two hues + a
neutral gray midpoint at zero) using validated dark-surface-safe steps
(node scripts/validate_palette.js "#16a34a,#dc2626" --mode dark: passes
lightness band, chroma floor, contrast). Red/green (not the skill's own
blue/red reference pair) is a deliberate, considered choice here, not an
oversight: gain/loss color convention is close to universal in financial
charts, and this chart already carries a strong SECOND, color-independent
encoding of the same signal -- bar direction (right=bullish, left=bearish)
plus a signed numeric value on every bar -- so the CVD-separation floor
this pair doesn't clear on its own is covered by that redundancy, exactly
the condition the skill's own rule requires before allowing it.

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

# "Very clear HD" -- every dimension/line-width/font-size below is defined
# in LOGICAL pixels and multiplied by this at the one call site (_s()) that
# actually draws with it, so the whole chart renders at native higher
# resolution (not a blurry upscale of a smaller image) with zero layout
# math duplicated. Cheap: at 2x, the biggest image here is still ~5MB in
# memory before PNG compression, trivial against the 512Mi ceiling that
# made memory a real constraint elsewhere in this codebase.
_SCALE = int(os.getenv("CHART_SNAPSHOT_SCALE", "2") or "2")


def _s(n: float) -> int:
    return round(n * _SCALE)

_BG = (13, 15, 20)
_AXIS = (42, 47, 61)
_GRID = (28, 32, 43)
_ENTRY_COLOR = (231, 236, 245)
_TP_COLOR = (34, 197, 94)
_SL_COLOR = (239, 68, 68)
_WIN_COLOR = (34, 197, 94)
_LOSS_COLOR = (239, 68, 68)
_CANDLE_UP_COLOR = (34, 197, 94)
_CANDLE_DOWN_COLOR = (239, 68, 68)
_TEXT_PRIMARY = (231, 236, 245)
_TEXT_MUTED = (146, 153, 168)


def _font(size: int):
    """Pillow's own bundled scalable font -- no external .ttf dependency,
    so this is guaranteed present on Render the same as locally. Falls
    back to the tiny fixed-size legacy default only if this exact Pillow
    build somehow lacks the `size` kwarg (pre-10.1)."""
    from PIL import ImageFont
    try:
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


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


_WIDTH, _HEIGHT = _s(900), _s(500)
_MARGIN = _s(56)
_RIGHT_LABEL_WIDTH = _s(132)
# Any more candles than this on a 900-logical-pixel-wide image makes each
# candle body sub-pixel at normal viewing size -- callers should already be
# trimming to recent history, but this is a hard backstop so a caller
# passing e.g. a whole day of 1-minute candles can't produce an unreadable
# smear instead of a chart.
MAX_CANDLESTICKS = 90
# "Make sure all the indicators used and factors are showing on that
# picture" -- a dedicated footer strip listing whatever the caller decides
# is relevant to THIS specific decision (technical indicators, model
# confidence, volume/momentum/breakout/sentiment signals, exit reason,
# etc.), not hardcoded here -- each of the 4 strategies' own feature set
# differs (leverage for perps, IV-adjacent figures for options, ...), so
# chart_snapshot.py stays a generic renderer and the caller supplies
# already-labeled, already-formatted strings. Up to 4 columns per row so a
# realistic 8-12 item indicator set still fits in 2-3 rows without
# shrinking the font past readable-on-a-phone.
_INDICATOR_COLUMNS = 4
_INDICATOR_ROW_HEIGHT = _s(26)
_INDICATOR_TOP_PADDING = _s(14)
_INDICATOR_BOTTOM_PADDING = _s(12)
_INDICATOR_COLOR = (180, 186, 199)
_INDICATOR_LABEL_COLOR = (146, 153, 168)


def generate_candlestick_chart(
    *, ticker: str, market: str, candles: list[dict[str, float]], side: str = "long",
    entry_price: float | None = None, exit_price: float | None = None,
    take_profit_price: float | None = None, stop_loss_price: float | None = None,
    entry_index: int | None = None, exit_index: int | None = None,
    pnl_usd: float | None = None, subtitle: str | None = None,
    indicators: dict[str, Any] | None = None,
) -> Path | None:
    """Renders real OHLC candlesticks -- the "whole idea" of a trade at a
    glance: what price actually did, not just a single close-price line.
    `candles` is chronological, each item needing "open"/"high"/"low"/
    "close" (a "ts" key is fine but unused for layout, which is purely
    index-based). Reference levels (entry/take-profit/stop-loss/exit) are
    all optional dashed horizontal lines -- omit whichever don't apply
    (e.g. options' underlying-price chart passes none of these, since the
    option's own premium is on a completely different scale than the
    underlying's price; see threads_post.post_trade_entry_chart). Optional
    entry_index/exit_index draw a vertical dashed marker at that candle,
    useful even when the price-level lines don't apply. pnl_usd (if given)
    colors the title/exit line green (win) or red (loss).

    `indicators` (optional): a label -> already-formatted-value mapping
    (e.g. {"RSI": "62.3", "MACD hist": "+0.031%", "Volume Z": "+1.24",
    "Model conf": "61.4%"}) rendered as a compact footer grid below the
    plot, so a follower sees not just what the price did but WHY the bot
    acted -- every real factor the decision actually used, not a curated
    subset. Grows the image height to fit; omit for charts where no
    decision-relevant indicators apply. Returns the saved PNG's path, or
    None if there isn't enough data or rendering fails -- callers must
    treat None as "skip this post", never raise."""
    if len(candles) < 5:
        return None
    try:
        from PIL import Image, ImageDraw
    except Exception as exc:
        logger.warning("[chart_snapshot] Pillow unavailable: %s", exc)
        return None

    if len(candles) > MAX_CANDLESTICKS:
        trim = len(candles) - MAX_CANDLESTICKS
        candles = candles[-MAX_CANDLESTICKS:]
        # An index that falls before the trimmed window is off-screen --
        # clamping it to the left edge would draw a marker on a candle that
        # isn't actually the entry/exit, silently mislabeling the chart.
        # Dropping it (None = don't draw that marker) is the honest choice.
        entry_index = None if entry_index is None or entry_index - trim < 0 else entry_index - trim
        exit_index = None if exit_index is None or exit_index - trim < 0 else exit_index - trim

    indicator_items = [(str(k), str(v)) for k, v in (indicators or {}).items() if v is not None]
    indicator_rows = -(-len(indicator_items) // _INDICATOR_COLUMNS) if indicator_items else 0  # ceil div
    panel_height = (
        _INDICATOR_TOP_PADDING + indicator_rows * _INDICATOR_ROW_HEIGHT + _INDICATOR_BOTTOM_PADDING
        if indicator_rows else 0
    )
    total_height = _HEIGHT + panel_height

    img = None
    try:
        img = Image.new("RGB", (_WIDTH, total_height), _BG)
        draw = ImageDraw.Draw(img)
        title_font, label_font, small_font = _font(_s(24)), _font(_s(15)), _font(_s(13))
        indicator_font = _font(_s(13))

        plot_left, plot_right = _MARGIN, _WIDTH - _MARGIN - _RIGHT_LABEL_WIDTH
        plot_top, plot_bottom = _s(84), _HEIGHT - _MARGIN

        all_values = [float(c["high"]) for c in candles] + [float(c["low"]) for c in candles]
        for level in (entry_price, exit_price, take_profit_price, stop_loss_price):
            if level is not None:
                all_values.append(float(level))
        lo, hi = min(all_values), max(all_values)
        span = (hi - lo) or max(abs(hi), 1e-9)
        pad = span * 0.1
        lo, hi = lo - pad, hi + pad
        span = hi - lo

        def y_for(value: float) -> float:
            return plot_bottom - ((value - lo) / span) * (plot_bottom - plot_top)

        n = len(candles)
        slot_w = (plot_right - plot_left) / n

        def x_center(idx: int) -> float:
            return plot_left + (idx + 0.5) * slot_w

        # Faint horizontal gridlines behind everything -- an anchor for the
        # eye without competing with the candles or reference levels.
        for frac in (0.25, 0.5, 0.75):
            gy = plot_top + frac * (plot_bottom - plot_top)
            draw.line([(plot_left, gy), (plot_right, gy)], fill=_GRID, width=_s(1))
        draw.rectangle([plot_left, plot_top, plot_right, plot_bottom], outline=_AXIS, width=_s(1))

        def dashed_hline(value: float, color: tuple[int, int, int], label: str) -> None:
            y = y_for(value)
            x = plot_left
            while x < plot_right:
                draw.line([(x, y), (min(x + _s(10), plot_right), y)], fill=color, width=_s(2))
                x += _s(17)
            draw.text((plot_right + _s(10), y - _s(8)), label, fill=color, font=label_font)

        if take_profit_price is not None:
            dashed_hline(take_profit_price, _TP_COLOR, f"TP {take_profit_price:.4f}")
        if stop_loss_price is not None:
            dashed_hline(stop_loss_price, _SL_COLOR, f"SL {stop_loss_price:.4f}")
        if entry_price is not None:
            dashed_hline(entry_price, _ENTRY_COLOR, f"Entry {entry_price:.4f}")
        if exit_price is not None:
            exit_color = _LOSS_COLOR if (pnl_usd or 0) < 0 else _WIN_COLOR
            dashed_hline(exit_price, exit_color, f"Exit {exit_price:.4f}")

        # Candle body width leaves a thin gap between candles so individual
        # bars stay visually distinct rather than forming a solid block.
        body_w = max(1, slot_w - _s(2))
        for i, c in enumerate(candles):
            o, h, l, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
            cx = x_center(i)
            color = _CANDLE_UP_COLOR if cl >= o else _CANDLE_DOWN_COLOR
            draw.line([(cx, y_for(h)), (cx, y_for(l))], fill=color, width=max(1, _s(1)))
            body_top, body_bottom = y_for(max(o, cl)), y_for(min(o, cl))
            if body_bottom - body_top < 1:
                body_bottom = body_top + 1
            draw.rectangle(
                [cx - body_w / 2, body_top, cx + body_w / 2, body_bottom], fill=color,
            )

        def dashed_vline(idx: int, color: tuple[int, int, int], label: str) -> None:
            x = x_center(max(0, min(idx, n - 1)))
            y = plot_top
            while y < plot_bottom:
                draw.line([(x, y), (x, min(y + _s(8), plot_bottom))], fill=color, width=_s(2))
                y += _s(14)
            draw.text((x + _s(4), plot_top - _s(2)), label, fill=color, font=small_font)

        if entry_index is not None:
            dashed_vline(entry_index, _ENTRY_COLOR, "ENTRY")
        if exit_index is not None:
            exit_color = _LOSS_COLOR if (pnl_usd or 0) < 0 else _WIN_COLOR
            dashed_vline(exit_index, exit_color, "EXIT")

        direction = "SHORT" if side == "short" else "LONG"
        title = f"{market.upper()} -- {direction} {ticker}"
        title_color = _TEXT_PRIMARY
        if pnl_usd is not None:
            result_word = "WIN" if pnl_usd > 0 else "LOSS" if pnl_usd < 0 else "FLAT"
            title = f"{title} -- {result_word} {pnl_usd:+.2f}"
            title_color = _WIN_COLOR if pnl_usd > 0 else _LOSS_COLOR if pnl_usd < 0 else _TEXT_PRIMARY
        draw.text((_MARGIN, _s(24)), title, fill=title_color, font=title_font)
        draw.text((_MARGIN, _s(56)), subtitle or "Recent price action", fill=_TEXT_MUTED, font=small_font)

        if indicator_items:
            panel_top = _HEIGHT
            draw.line([(_MARGIN, panel_top), (_WIDTH - _MARGIN, panel_top)], fill=_AXIS, width=_s(1))
            col_w = (_WIDTH - _MARGIN * 2) / _INDICATOR_COLUMNS
            for i, (label, value) in enumerate(indicator_items):
                row, col = divmod(i, _INDICATOR_COLUMNS)
                x = _MARGIN + col * col_w
                y = panel_top + _INDICATOR_TOP_PADDING + row * _INDICATOR_ROW_HEIGHT
                draw.text((x, y), f"{label}", fill=_INDICATOR_LABEL_COLOR, font=indicator_font)
                label_w = draw.textlength(f"{label} ", font=indicator_font)
                draw.text((x + label_w, y), f"{value}", fill=_INDICATOR_COLOR, font=indicator_font)

        CHARTS_DIR.mkdir(parents=True, exist_ok=True)
        filename = f"{_sanitize(market)}_{_sanitize(ticker)}_{int(time.time())}.png"
        out_path = CHARTS_DIR / filename
        img.save(out_path, format="PNG")
        _prune_old_charts()
        return out_path
    except Exception as exc:
        logger.warning("[chart_snapshot] candlestick rendering failed for %s: %s", ticker, exc)
        return None
    finally:
        if img is not None:
            del img


_SENTIMENT_ROW_HEIGHT = _s(34)
_SENTIMENT_TOP_MARGIN = _s(96)
_SENTIMENT_BOTTOM_MARGIN = _s(40)
_SENTIMENT_LABEL_WIDTH = _s(108)
_SENTIMENT_SCORE_WIDTH = _s(62)
_SENTIMENT_BAR_HALF_WIDTH = _s(280)
_SENTIMENT_POS_COLOR = (22, 163, 74)   # validated: node validate_palette.js "#16a34a,#dc2626" --mode dark
_SENTIMENT_NEG_COLOR = (220, 38, 38)   # passes lightness band + chroma floor + contrast on the dark surface
_SENTIMENT_NEUTRAL_COLOR = (110, 116, 130)
# Every ticker on a large watchlist (crypto's 36 pairs) in one image would
# be unreadable -- keep the snapshot to the tickers with something actually
# worth showing: the most bullish and most bearish, half and half.
MAX_SENTIMENT_ROWS = int(os.getenv("CHART_SENTIMENT_MAX_ROWS", "20") or "20")


def generate_sentiment_snapshot(*, market: str, ticker_sentiments: list[dict[str, Any]]) -> Path | None:
    """Renders a per-ticker sentiment bar chart -- one row per ticker, a
    bar extending right (green, bullish) or left (red, bearish) from a
    zero-line, length proportional to |sentiment_score|, with the signed
    score printed on every bar (color is never the ONLY encoding of
    direction here -- see this module's own docstring on why). Ticker
    labels stay in neutral text ink, never the bar's color, per the
    dataviz skill's own "text wears text tokens" rule. `ticker_sentiments`
    items need "ticker" and "sentiment_score" (from *_news.get_sentiment(),
    already real per-ticker news sentiment, not invented for this chart).
    Returns None (skip posting) on empty input or any rendering failure."""
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

    width = _MARGIN * 2 + _SENTIMENT_LABEL_WIDTH + _SENTIMENT_BAR_HALF_WIDTH * 2 + _SENTIMENT_SCORE_WIDTH
    height = _SENTIMENT_TOP_MARGIN + len(rows) * _SENTIMENT_ROW_HEIGHT + _SENTIMENT_BOTTOM_MARGIN

    img = None
    try:
        img = Image.new("RGB", (width, height), _BG)
        draw = ImageDraw.Draw(img)
        title_font, subtitle_font, label_font, score_font, axis_font = (
            _font(_s(26)), _font(_s(14)), _font(_s(15)), _font(_s(15)), _font(_s(12)),
        )

        draw.text((_MARGIN, _s(24)), f"{market.upper()} -- Sentiment Snapshot", fill=_TEXT_PRIMARY, font=title_font)
        draw.text((_MARGIN, _s(58)), "Per-ticker news sentiment, most bullish to most bearish", fill=_TEXT_MUTED, font=subtitle_font)

        zero_x = _MARGIN + _SENTIMENT_LABEL_WIDTH + _SENTIMENT_BAR_HALF_WIDTH
        axis_top = _SENTIMENT_TOP_MARGIN - _s(14)
        axis_bottom = height - _SENTIMENT_BOTTOM_MARGIN + _s(6)
        draw.line([(zero_x, axis_top), (zero_x, axis_bottom)], fill=_AXIS, width=_s(1))
        # End labels directly on the axis -- the chart reads correctly even
        # if red/green itself doesn't (see module docstring): direction is
        # ALSO which side of this line the bar is on.
        draw.text((zero_x - _SENTIMENT_BAR_HALF_WIDTH, axis_bottom + _s(6)), "<- Bearish", fill=_SENTIMENT_NEG_COLOR, font=axis_font)
        bullish_w = draw.textlength("Bullish ->", font=axis_font)
        draw.text((zero_x + _SENTIMENT_BAR_HALF_WIDTH - bullish_w, axis_bottom + _s(6)), "Bullish ->", fill=_SENTIMENT_POS_COLOR, font=axis_font)

        for i, row in enumerate(rows):
            y = _SENTIMENT_TOP_MARGIN + i * _SENTIMENT_ROW_HEIGHT
            score = max(-1.0, min(1.0, float(row["sentiment_score"])))
            ticker = str(row["ticker"])
            bar_top, bar_bottom = y + _s(5), y + _SENTIMENT_ROW_HEIGHT - _s(10)
            bar_mid = (bar_top + bar_bottom) / 2
            label_h = label_font.size
            draw.text((_MARGIN, bar_mid - label_h / 2), ticker[:14], fill=_TEXT_PRIMARY, font=label_font)

            bar_len = abs(score) * _SENTIMENT_BAR_HALF_WIDTH
            color = _SENTIMENT_POS_COLOR if score > 0.02 else _SENTIMENT_NEG_COLOR if score < -0.02 else _SENTIMENT_NEUTRAL_COLOR
            radius = min(_s(4), (bar_bottom - bar_top) / 2)
            if bar_len < 1:
                draw.line([(zero_x - _s(2), bar_mid), (zero_x + _s(2), bar_mid)], fill=color, width=_s(3))
            elif score >= 0:
                draw.rounded_rectangle([zero_x, bar_top, zero_x + bar_len, bar_bottom], radius=radius, fill=color)
            else:
                draw.rounded_rectangle([zero_x - bar_len, bar_top, zero_x, bar_bottom], radius=radius, fill=color)

            score_x = _MARGIN + _SENTIMENT_LABEL_WIDTH + _SENTIMENT_BAR_HALF_WIDTH * 2 + _s(14)
            draw.text((score_x, bar_mid - label_h / 2), f"{score:+.2f}", fill=color, font=score_font)

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
        if img is not None:
            del img


def format_technical_indicators(row: dict[str, Any] | None) -> dict[str, str]:
    """Formats a raw engineered-feature-row-like dict (dollar_volume_z,
    macd_hist_pct, bb_pct_b, rsi_14, sentiment_score, volatility_30 --
    the same FEATURE_COLUMNS names every one of the 4 strategies' own data
    modules already uses) into display-ready label -> string entries for
    generate_candlestick_chart's `indicators` panel. Shared here so all 4
    services format the same underlying signals identically rather than
    four slightly-drifting copies. Missing/None values are simply omitted
    (not rendered as "None") -- callers merge the result with their own
    service-specific extras (model confidence, hold time, exit reason,
    ...) before passing to generate_candlestick_chart."""
    if not row:
        return {}
    out: dict[str, str] = {}
    rsi = row.get("rsi_14")
    if rsi is not None:
        out["RSI"] = f"{float(rsi):.1f}"
    macd = row.get("macd_hist_pct")
    if macd is not None:
        out["MACD hist"] = f"{float(macd):+.3%}"
    bb = row.get("bb_pct_b")
    if bb is not None:
        out["BB %B"] = f"{float(bb):.2f}"
    vol_z = row.get("dollar_volume_z")
    if vol_z is not None:
        out["Volume Z"] = f"{float(vol_z):+.2f}"
    sentiment = row.get("sentiment_score")
    if sentiment is not None:
        out["Sentiment"] = f"{float(sentiment):+.2f}"
    volatility_30 = row.get("volatility_30")
    if volatility_30 is not None:
        out["Volatility 30m"] = f"{float(volatility_30):.3%}"
    return out


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
