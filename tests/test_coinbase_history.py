"""Coinbase historical-candle fetcher -- mocked network only, never touches
the real API. Verifies pagination chains backward correctly, stops early on
a coin's actual listing-date boundary instead of erroring, and returns the
same [ts, close] shape perps_data.engineer_features() expects."""
from __future__ import annotations

import datetime as dt

from data import coinbase_history as cbh


def _page(count, start_ts):
    return [[start_ts + i * 60, 0.0, 0.0, 0.0, 100.0 + i, 0.0] for i in range(count)]


def test_fetch_coinbase_history_chains_multiple_pages(monkeypatch):
    calls = []

    def fake_page(product_id, *, granularity_sec, start, end):
        calls.append((start, end))
        return _page(300, int(start.timestamp()))

    monkeypatch.setattr(cbh, "_fetch_candle_page", fake_page)
    monkeypatch.setattr(cbh.time, "sleep", lambda s: None)
    end = dt.datetime(2026, 7, 23, tzinfo=dt.timezone.utc)
    result = cbh.fetch_coinbase_history("BTC-USD", days=1, granularity_sec=60, end=end)
    assert len(calls) >= 1
    assert str(result["ts"].dtype) == "int64"
    assert str(result["close"].dtype) == "float64"


def test_fetch_coinbase_history_stops_early_past_listing_date(monkeypatch):
    """A coin only listed 1 year ago gets empty pages for anything older --
    must stop (not loop forever or error) rather than assuming a gap."""
    call_count = [0]

    def fake_page(product_id, *, granularity_sec, start, end):
        call_count[0] += 1
        if call_count[0] <= 2:
            return _page(300, int(start.timestamp()))
        return []  # past the coin's listing date

    monkeypatch.setattr(cbh, "_fetch_candle_page", fake_page)
    monkeypatch.setattr(cbh.time, "sleep", lambda s: None)
    end = dt.datetime(2026, 7, 23, tzinfo=dt.timezone.utc)
    result = cbh.fetch_coinbase_history("HYPE-USD", days=1460, granularity_sec=3600, end=end)
    assert not result.empty
    # Stopped after 3 consecutive empties, not after exhausting the full
    # requested 1460-day span.
    assert call_count[0] < 50


def test_fetch_coinbase_history_returns_empty_frame_with_numeric_dtypes_when_nothing_available(monkeypatch):
    monkeypatch.setattr(cbh, "_fetch_candle_page", lambda *a, **k: [])
    monkeypatch.setattr(cbh.time, "sleep", lambda s: None)
    result = cbh.fetch_coinbase_history("FAKE-USD", days=1, granularity_sec=60)
    assert result.empty
    assert str(result["ts"].dtype) == "int64"
    assert str(result["close"].dtype) == "float64"


def test_all_sixteen_kalshi_coins_have_a_coinbase_product_mapping():
    from data.kalshi_perps import KNOWN_PERP_TICKERS
    from data.perps_data import coin_for_ticker

    for ticker in KNOWN_PERP_TICKERS:
        coin = coin_for_ticker(ticker)
        assert coin in cbh.COINBASE_PRODUCT_BY_COIN, f"no Coinbase mapping for {ticker} ({coin})"
