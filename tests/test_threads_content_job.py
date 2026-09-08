"""scripts/threads_content_job.py -- the standalone, Flask-free script
meant to run as a Hugging Face Jobs scheduled run instead of a Render-
hosted cron-job.org-triggered route. Each job function is a near-verbatim
copy of the corresponding `_run_*_threads_*` body already covered by
tests/test_alpaca_server_jobs.py and friends -- these tests verify the
SAME behavior survived the copy (right market threaded through, right
data.* functions called, dedup predicate wired to the right market's own
pool), plus the script's own CLI dispatch/exit-code logic that has no
equivalent in the Flask-hosted originals."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import threads_content_job as job  # noqa: E402


def test_stocks_trending_news_posts_the_fetched_story(monkeypatch):
    from data import stock_news, threads_post

    story = {"title": "Markets rally", "link": "https://x.com/a", "image_url": None, "source": "cnbc.com", "secondary": []}
    monkeypatch.setattr(stock_news, "get_trending_story", lambda **kw: story)
    captured = {}
    monkeypatch.setattr(threads_post, "post_trending_news", lambda s, *, market: captured.update(story=s, market=market) or True)

    result = job._stocks_trending_news()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "story": "Markets rally"}
    assert captured["market"] == "stocks"


def test_stocks_trending_news_wires_the_stocks_dedup_pool(monkeypatch):
    from data import stock_news, threads_post

    captured_kwargs = {}
    monkeypatch.setattr(stock_news, "get_trending_story", lambda **kw: captured_kwargs.update(kw) or None)
    monkeypatch.setattr(threads_post, "post_trending_news", lambda s, *, market: False)
    is_dup_calls = []
    monkeypatch.setattr(threads_post, "is_recent_duplicate_story", lambda market, title: is_dup_calls.append((market, title)) or False)

    job._stocks_trending_news()  # noqa: SLF001

    captured_kwargs["exclude"]("some headline")
    assert is_dup_calls == [("stocks", "some headline")]


def test_stocks_trending_news_never_raises_on_failure(monkeypatch):
    from data import stock_news

    def raise_error(**kw):
        raise RuntimeError("rss down")

    monkeypatch.setattr(stock_news, "get_trending_story", raise_error)
    result = job._stocks_trending_news()  # noqa: SLF001
    assert result["ok"] is False


def test_crypto_trending_news_posts_the_fetched_story(monkeypatch):
    from data import crypto_news, threads_post

    story = {"title": "Bitcoin rallies", "link": "https://x.com/a", "image_url": "https://x.com/i.jpg", "source": "cointelegraph", "secondary": []}
    monkeypatch.setattr(crypto_news, "get_trending_story", lambda **kw: story)
    captured = {}
    monkeypatch.setattr(threads_post, "post_trending_news", lambda s, *, market: captured.update(story=s, market=market) or True)

    result = job._crypto_trending_news()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "story": "Bitcoin rallies"}
    assert captured["market"] == "crypto"


def test_crypto_trending_news_wires_the_crypto_dedup_pool(monkeypatch):
    from data import crypto_news, threads_post

    captured_kwargs = {}
    monkeypatch.setattr(crypto_news, "get_trending_story", lambda **kw: captured_kwargs.update(kw) or None)
    monkeypatch.setattr(threads_post, "post_trending_news", lambda s, *, market: False)
    is_dup_calls = []
    monkeypatch.setattr(threads_post, "is_recent_duplicate_story", lambda market, title: is_dup_calls.append((market, title)) or False)

    job._crypto_trending_news()  # noqa: SLF001

    captured_kwargs["exclude"]("some headline")
    assert is_dup_calls == [("crypto", "some headline")]


def test_stocks_sentiment_snapshot_posts_every_watchlist_ticker(monkeypatch):
    from data import alpaca_data, stock_news, threads_post

    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL", "MSFT"])
    monkeypatch.setattr(alpaca_data, "get_company_name", lambda symbol: f"{symbol} Inc.")
    monkeypatch.setattr(stock_news, "get_sentiment", lambda symbol, *, company_name: {"sentiment_score": 0.5})
    captured = {}
    monkeypatch.setattr(threads_post, "post_sentiment_snapshot", lambda *, market, ticker_sentiments: captured.update(market=market, ticker_sentiments=ticker_sentiments) or True)

    result = job._stocks_sentiment_snapshot()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "ticker_count": 2}
    assert captured["market"] == "stocks"
    assert captured["ticker_sentiments"] == [{"ticker": "AAPL", "sentiment_score": 0.5}, {"ticker": "MSFT", "sentiment_score": 0.5}]


def test_stocks_sentiment_snapshot_skips_a_ticker_whose_sentiment_fetch_fails(monkeypatch):
    from data import alpaca_data, stock_news, threads_post

    monkeypatch.setattr(alpaca_data, "load_training_dataset", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alpaca_data, "get_stock_watchlist", lambda recent: ["AAPL", "BROKEN"])
    monkeypatch.setattr(alpaca_data, "get_company_name", lambda symbol: f"{symbol} Inc.")

    def fake_sentiment(symbol, *, company_name):
        if symbol == "BROKEN":
            raise RuntimeError("news fetch failed")
        return {"sentiment_score": 0.5}

    monkeypatch.setattr(stock_news, "get_sentiment", fake_sentiment)
    monkeypatch.setattr(threads_post, "post_sentiment_snapshot", lambda *, market, ticker_sentiments: True)

    result = job._stocks_sentiment_snapshot()  # noqa: SLF001
    assert result["ticker_count"] == 1


def test_crypto_sentiment_snapshot_posts_every_universe_symbol(monkeypatch):
    from data import alpaca_crypto_data, crypto_news, threads_post

    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BTC/USD", "ETH/USD"])
    monkeypatch.setattr(alpaca_crypto_data, "symbol_to_coin", lambda symbol: symbol.split("/")[0].lower())
    monkeypatch.setattr(crypto_news, "get_sentiment", lambda coin: {"sentiment_score": -0.2})
    captured = {}
    monkeypatch.setattr(threads_post, "post_sentiment_snapshot", lambda *, market, ticker_sentiments: captured.update(market=market, ticker_sentiments=ticker_sentiments) or True)

    result = job._crypto_sentiment_snapshot()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "ticker_count": 2}
    assert captured["market"] == "crypto"


def test_options_sentiment_snapshot_posts_every_underlying(monkeypatch):
    from data import alpaca_data, alpaca_options_data, stock_news, threads_post

    monkeypatch.setattr(alpaca_options_data, "get_options_universe", lambda: ["AAPL", "TSLA"])
    monkeypatch.setattr(alpaca_data, "get_company_name", lambda symbol: f"{symbol} Inc.")
    monkeypatch.setattr(stock_news, "get_sentiment", lambda symbol, *, company_name: {"sentiment_score": 0.1})
    captured = {}
    monkeypatch.setattr(threads_post, "post_sentiment_snapshot", lambda *, market, ticker_sentiments: captured.update(market=market, ticker_sentiments=ticker_sentiments) or True)

    result = job._options_sentiment_snapshot()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "ticker_count": 2}
    assert captured["market"] == "options"


def test_perps_sentiment_snapshot_posts_every_watchlist_ticker(monkeypatch):
    from data import crypto_news, perps_data, threads_post

    monkeypatch.setattr(perps_data, "get_watchlist", lambda: ["BTCUSD", "ETHUSD"])
    monkeypatch.setattr(perps_data, "coin_for_ticker", lambda ticker: ticker.replace("USD", "").lower())
    monkeypatch.setattr(crypto_news, "get_sentiment", lambda coin: {"sentiment_score": 0.3})
    captured = {}
    monkeypatch.setattr(threads_post, "post_sentiment_snapshot", lambda *, market, ticker_sentiments: captured.update(market=market, ticker_sentiments=ticker_sentiments) or True)

    result = job._perps_sentiment_snapshot()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "ticker_count": 2}
    assert captured["market"] == "perps"


def test_jobs_table_covers_exactly_the_six_real_job_market_combinations():
    """trending-news only exists for stocks/crypto (options/perps
    intentionally have no trending-news job at all -- see this script's
    own module docstring); sentiment-snapshot covers all four markets."""
    assert set(job._JOBS.keys()) == {  # noqa: SLF001
        ("trending-news", "stocks"), ("trending-news", "crypto"),
        ("sentiment-snapshot", "stocks"), ("sentiment-snapshot", "crypto"),
        ("sentiment-snapshot", "options"), ("sentiment-snapshot", "perps"),
    }


def test_main_dispatches_to_the_right_job_function(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["threads_content_job.py", "--job", "sentiment-snapshot", "--market", "crypto"])
    monkeypatch.setattr(job, "_load_dotenv", lambda: None)
    monkeypatch.setitem(job._JOBS, ("sentiment-snapshot", "crypto"), lambda: {"ok": True, "posted": True, "ticker_count": 3})  # noqa: SLF001

    exit_code = job.main()

    assert exit_code == 0
    printed = capsys.readouterr().out
    assert '"ticker_count": 3' in printed


def test_main_returns_a_nonzero_exit_code_when_the_job_reports_not_ok(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["threads_content_job.py", "--job", "trending-news", "--market", "stocks"])
    monkeypatch.setattr(job, "_load_dotenv", lambda: None)
    monkeypatch.setitem(job._JOBS, ("trending-news", "stocks"), lambda: {"ok": False, "error": "boom"})  # noqa: SLF001

    assert job.main() == 1


def test_main_rejects_an_invalid_job_market_combination(monkeypatch, capsys):
    """trending-news has no options/perps variant -- argparse itself
    allows the combination (each flag's choices are independent), so this
    has to be caught at dispatch time, not parse time."""
    monkeypatch.setattr(sys, "argv", ["threads_content_job.py", "--job", "trending-news", "--market", "options"])

    assert job.main() == 1
    printed = capsys.readouterr().out
    assert "no such job/market combination" in printed
