"""Runs ONE Threads content job (trending-news or sentiment-snapshot) for
ONE market, as a standalone process -- no Flask, no APScheduler, no
_locked_job (this process's own single invocation IS the concurrency
boundary, the same assumption cron-job.org's existing HTTP-triggered
routes already rely on). Built to run as a Hugging Face Jobs scheduled
run instead of a Render-hosted route -- see docs/CRON_JOB_MIGRATION.md
for the full architecture, secrets checklist, and why this exists (both
job types are read-only fetch-news-and-post; neither ever touches order
placement, so nothing trading-critical needs to be configured for this
script to run).

Each function below is a near-verbatim copy of the corresponding
`_run_*_threads_*` job body already living in alpaca_server.py/
alpaca_crypto_server.py/alpaca_options_server.py/app_kalshi.py -- same
logic, just reimplemented against the portable `data.*` functions
directly instead of importing the Flask-app-entangled, `@_locked_job`-
decorated originals (which can't be imported standalone without dragging
in their whole Flask server module).

trending-news only exists for stocks/crypto: options/perps intentionally
have no trending-news job at all -- see the options and perps servers'
own `_run_*_threads_trending_news` docstrings for the previously-fixed
duplicate-post bug that's why those two defer to stocks/crypto instead of
re-fetching and re-posting the same story a second time.

Usage:
    python scripts/threads_content_job.py --job trending-news --market stocks
    python scripts/threads_content_job.py --job sentiment-snapshot --market crypto
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Callable

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("threads_content_job")


def _load_dotenv() -> None:
    """Minimal .env loader (mirrors run_perps_cycle.py's own) so this
    script works standalone for a local/manual run. On Hugging Face Jobs
    itself, real secrets come from the job's own configured environment
    instead -- this is a no-op there since no .env file exists in that
    container."""
    import os

    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        return
    lines = env_path.read_text(encoding="utf-8").splitlines()
    idx = 0
    while idx < len(lines):
        raw = lines[idx].strip()
        idx += 1
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key, value = key.strip(), value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _stocks_trending_news() -> dict[str, Any]:
    from data import stock_news, threads_post
    try:
        story = stock_news.get_trending_story(
            exclude=lambda title: threads_post.is_recent_duplicate_story("stocks", title),
        )
        posted = threads_post.post_trending_news(story, market="stocks")
        return {"ok": True, "posted": posted, "story": (story or {}).get("title")}
    except Exception as exc:
        logger.warning("stocks trending-news post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def _crypto_trending_news() -> dict[str, Any]:
    from data import crypto_news, threads_post
    try:
        story = crypto_news.get_trending_story(
            exclude=lambda title: threads_post.is_recent_duplicate_story("crypto", title),
        )
        posted = threads_post.post_trending_news(story, market="crypto")
        return {"ok": True, "posted": posted, "story": (story or {}).get("title")}
    except Exception as exc:
        logger.warning("crypto trending-news post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def _stocks_sentiment_snapshot() -> dict[str, Any]:
    from data import alpaca_data, stock_news, threads_post
    try:
        recent = alpaca_data.load_training_dataset(max_rows=5_000)
        watchlist = alpaca_data.get_stock_watchlist(recent if not recent.empty else None)
        ticker_sentiments = []
        for symbol in watchlist:
            try:
                sentiment = stock_news.get_sentiment(symbol, company_name=alpaca_data.get_company_name(symbol))
                ticker_sentiments.append({"ticker": symbol, "sentiment_score": sentiment["sentiment_score"]})
            except Exception as exc:
                logger.debug("sentiment fetch failed for %s: %s", symbol, exc)
        posted = threads_post.post_sentiment_snapshot(market="stocks", ticker_sentiments=ticker_sentiments)
        return {"ok": True, "posted": posted, "ticker_count": len(ticker_sentiments)}
    except Exception as exc:
        logger.warning("stocks sentiment-snapshot post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def _crypto_sentiment_snapshot() -> dict[str, Any]:
    from data import alpaca_crypto_data, crypto_news, threads_post
    try:
        symbols = alpaca_crypto_data.get_crypto_universe()
        ticker_sentiments = []
        for symbol in symbols:
            try:
                sentiment = crypto_news.get_sentiment(alpaca_crypto_data.symbol_to_coin(symbol))
                ticker_sentiments.append({"ticker": symbol, "sentiment_score": sentiment["sentiment_score"]})
            except Exception as exc:
                logger.debug("sentiment fetch failed for %s: %s", symbol, exc)
        posted = threads_post.post_sentiment_snapshot(market="crypto", ticker_sentiments=ticker_sentiments)
        return {"ok": True, "posted": posted, "ticker_count": len(ticker_sentiments)}
    except Exception as exc:
        logger.warning("crypto sentiment-snapshot post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def _options_sentiment_snapshot() -> dict[str, Any]:
    from data import alpaca_data, alpaca_options_data, stock_news, threads_post
    try:
        underlyings = alpaca_options_data.get_options_universe()
        ticker_sentiments = []
        for symbol in underlyings:
            try:
                sentiment = stock_news.get_sentiment(symbol, company_name=alpaca_data.get_company_name(symbol))
                ticker_sentiments.append({"ticker": symbol, "sentiment_score": sentiment["sentiment_score"]})
            except Exception as exc:
                logger.debug("sentiment fetch failed for %s: %s", symbol, exc)
        posted = threads_post.post_sentiment_snapshot(market="options", ticker_sentiments=ticker_sentiments)
        return {"ok": True, "posted": posted, "ticker_count": len(ticker_sentiments)}
    except Exception as exc:
        logger.warning("options sentiment-snapshot post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def _perps_sentiment_snapshot() -> dict[str, Any]:
    from data import crypto_news, perps_data, threads_post
    try:
        tickers = perps_data.get_watchlist()
        ticker_sentiments = []
        for ticker in tickers:
            try:
                sentiment = crypto_news.get_sentiment(perps_data.coin_for_ticker(ticker))
                ticker_sentiments.append({"ticker": ticker, "sentiment_score": sentiment["sentiment_score"]})
            except Exception as exc:
                logger.debug("sentiment fetch failed for %s: %s", ticker, exc)
        posted = threads_post.post_sentiment_snapshot(market="perps", ticker_sentiments=ticker_sentiments)
        return {"ok": True, "posted": posted, "ticker_count": len(ticker_sentiments)}
    except Exception as exc:
        logger.warning("perps sentiment-snapshot post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


_JOBS: dict[tuple[str, str], Callable[[], dict[str, Any]]] = {
    ("trending-news", "stocks"): _stocks_trending_news,
    ("trending-news", "crypto"): _crypto_trending_news,
    ("sentiment-snapshot", "stocks"): _stocks_sentiment_snapshot,
    ("sentiment-snapshot", "crypto"): _crypto_sentiment_snapshot,
    ("sentiment-snapshot", "options"): _options_sentiment_snapshot,
    ("sentiment-snapshot", "perps"): _perps_sentiment_snapshot,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--job", required=True, choices=["trending-news", "sentiment-snapshot"])
    parser.add_argument("--market", required=True, choices=["stocks", "crypto", "options", "perps"])
    args = parser.parse_args()

    fn = _JOBS.get((args.job, args.market))
    if fn is None:
        result = {
            "ok": False,
            "error": f"no such job/market combination: {args.job}/{args.market} "
                     "(trending-news only runs for stocks/crypto -- see this script's own docstring)",
        }
        print(json.dumps(result, indent=2))
        return 1

    _load_dotenv()
    try:
        result = fn()
    except Exception as exc:  # a job function's own internal try/except should already catch this -- defense in depth only
        logger.exception("job raised unexpectedly")
        result = {"ok": False, "error": str(exc)}
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("ok", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
