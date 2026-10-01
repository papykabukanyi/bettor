"""Real, complete chart data behind every bot's setup: which source each
symbol is read on, and the data rule that refuses gappy or stale charts."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "setup_btc_1m.parquet"
SETUP_AS_OF = 1785036900


def test_every_kalshi_perp_has_a_coinbase_chart():
    from data import kalshi_15m_spot
    from data.kalshi_perps import KNOWN_PERP_TICKERS
    from data.perps_data import coin_for_ticker
    missing = [t for t in KNOWN_PERP_TICKERS
               if kalshi_15m_spot.chart_coin(coin_for_ticker(t)) not in kalshi_15m_spot.COINBASE_PRODUCTS]
    assert missing == []
    assert kalshi_15m_spot.chart_coin("KSHIB") == "SHIB" and kalshi_15m_spot.chart_coin("BTC") == "BTC"


def test_kshib_perp_reads_the_shib_chart(monkeypatch):
    from data import kalshi_15m_spot, perps_setup
    seen = []
    monkeypatch.setattr(kalshi_15m_spot, "recent_series", lambda coin: seen.append(coin) or pd.read_parquet(FIXTURE))
    assert not perps_setup.chart_candles("KXKSHIBPERP").empty and seen == ["SHIB"]


@pytest.mark.parametrize("metal,symbol", [("GOLD", "GLD"), ("SILVER", "SLV"), ("COPPER", "HG=F")])
def test_metals_are_read_on_their_real_time_chart(monkeypatch, metal, symbol):
    import requests

    from data import kalshi_15m_setup
    urls = []

    class Resp:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {"chart": {"result": [{"timestamp": [1790000000], "indicators": {"quote": [
                {"open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0], "volume": [5]}]}}]}}

    monkeypatch.setattr(requests, "get", lambda url, **kw: urls.append(url) or Resp())
    kalshi_15m_setup._metals_cache.clear()  # noqa: SLF001
    kalshi_15m_setup.metals_candles(metal)
    assert urls[0].endswith(f"/{symbol}")
    assert kalshi_15m_setup.session_for(metal) == ("us_equity" if metal in ("GOLD", "SILVER") else "utc_day")


def _with_gaps(df: pd.DataFrame, keep_every: int) -> pd.DataFrame:
    recent = df.ts > SETUP_AS_OF - 4 * 3600
    keep = ~recent | (np.arange(len(df)) % keep_every == 0)
    return df[keep]


@pytest.mark.parametrize("module", ["perps_setup", "alpaca_crypto_setup", "kalshi_15m_setup"])
def test_a_gappy_chart_fails_the_data_rule(module):
    import importlib
    m = importlib.import_module(f"data.{module}")
    btc = pd.read_parquet(FIXTURE)
    full = m.data_coverage(m.prepare(btc, session="utc_day"), SETUP_AS_OF)
    assert full["ok"] and full["coverage"] == pytest.approx(1.0, abs=0.01)
    gappy = m.evaluate(m.prepare(_with_gaps(btc, 2), session="utc_day"), SETUP_AS_OF, sides=("long",))
    assert gappy["valid"] is False and gappy["reason"] == "data"
    assert gappy["checks"]["data"]["coverage"] == pytest.approx(0.5, abs=0.02)


def test_stock_coverage_counts_only_trading_minutes():
    from data import alpaca_setup
    open_end = int(pd.Timestamp("2026-09-30 09:31", tz="America/New_York").timestamp())
    session = pd.DataFrame({"ts": open_end + 60 * np.arange(300), "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0})
    ctx = alpaca_setup.prepare(session, session="us_equity")
    noon = open_end + 60 * 150
    c = alpaca_setup.data_coverage(ctx, noon)
    assert c["ok"] and c["coverage"] == 1.0 and c["expected_minutes"] == 151
    early = alpaca_setup.data_coverage(ctx, open_end + 60 * 10)
    assert early["ok"] and early["coverage"] is None  # too early in the session to measure


def _bars(n=10):
    return pd.DataFrame({"ts": 1_790_000_000 + 60 * np.arange(n), "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0})


@pytest.mark.parametrize("module", ["alpaca_setup", "alpaca_options_setup"])
@pytest.mark.parametrize("first_reason,decides_on_consolidated", [("trend", False), ("breakout", False), ("volume", True), ("data", True)])
def test_stocks_are_decided_on_consolidated_bars_once_they_get_past_the_price_structure(
        monkeypatch, module, first_reason, decides_on_consolidated):
    import importlib

    from data import alpaca_data
    m = importlib.import_module(f"data.{module}")
    iex, cons = _bars(), _bars().assign(volume=25.0)
    monkeypatch.setattr(alpaca_data, "fetch_recent_minute_bars", lambda symbol, **kw: iex)
    monkeypatch.setattr(m, "consolidated_bars", lambda symbol: cons)
    calls = []

    def fake_setup_from_bars(bars, **kw):
        calls.append(bars is cons)
        return {"valid": False, "reason": first_reason if len(calls) == 1 else "risk_reward", "checks": {}}

    monkeypatch.setattr(m, "setup_from_bars", fake_setup_from_bars)
    r = m.live_setup("COST", news_score=None)
    assert (r["chart_source"] == "consolidated (Yahoo)") is decides_on_consolidated
    assert calls == ([False, True] if decides_on_consolidated else [False])
