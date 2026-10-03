"""Real, complete chart data behind every bot's setup: which source each
symbol is read on, and the data rule that refuses gappy or stale charts."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "setup_btc_1m.parquet"
SETUP_AS_OF = 1785036900


def test_every_kalshi_perp_alpaca_carries_has_a_chart():
    """Perps chart on Alpaca only: every perp whose coin Alpaca's Kraken feed
    carries has a chart; the rest (not listed on Alpaca) have none."""
    from data import kalshi_15m_spot
    from data.kalshi_perps import KNOWN_PERP_TICKERS
    from data.perps_data import coin_for_ticker
    missing = sorted(kalshi_15m_spot.chart_coin(coin_for_ticker(t)) for t in KNOWN_PERP_TICKERS
                     if kalshi_15m_spot.chart_coin(coin_for_ticker(t)) not in kalshi_15m_spot.SPOT_PRODUCTS)
    assert missing == ["HBAR", "NEAR", "SUI", "XLM", "ZEC"]
    assert kalshi_15m_spot.chart_coin("KSHIB") == "SHIB" and kalshi_15m_spot.chart_coin("BTC") == "BTC"


def test_kshib_perp_reads_the_shib_chart(monkeypatch):
    from data import kalshi_15m_spot, perps_setup
    seen = []
    monkeypatch.setattr(kalshi_15m_spot, "recent_series", lambda coin: seen.append(coin) or pd.read_parquet(FIXTURE))
    assert not perps_setup.chart_candles("KXKSHIBPERP").empty and seen == ["SHIB"]


@pytest.mark.parametrize("metal,symbol", [("GOLD", "GLD"), ("SILVER", "SLV"), ("COPPER", "CPER"), ("PLATINUM", "PPLT"),
                                          ("PALLADIUM", "PALL"), ("WTI", "USO"), ("NATGAS", "UNG")])
def test_commodities_are_read_on_their_sip_etf_chart(monkeypatch, metal, symbol):
    from data import alpaca_client, alpaca_stream, kalshi_15m_setup
    calls = []

    def fake_bars(symbols, *, timeframe, start, feed):
        calls.append((symbols, feed))
        return {symbols[0]: [{"t": "2026-10-02T14:30:00Z", "o": 1.0, "h": 1.2, "l": 0.9, "c": 1.1, "v": 500}]}

    monkeypatch.setattr(alpaca_client, "get_bars", fake_bars)
    monkeypatch.setattr(alpaca_stream, "_streams", {})
    kalshi_15m_setup._metals_cache.clear()  # noqa: SLF001
    df = kalshi_15m_setup.metals_candles(metal)
    assert calls == [([symbol], "sip")]
    assert int(df.ts.iloc[0]) == int(pd.Timestamp("2026-10-02T14:31:00Z").timestamp())  # stored by END
    assert kalshi_15m_setup.session_for(metal) == "us_equity"


def test_a_commodity_chart_carries_the_streams_newer_bars(monkeypatch):
    from data import alpaca_client, alpaca_stream, kalshi_15m_setup
    t0 = int(pd.Timestamp("2026-10-02T14:30:00Z").timestamp())
    monkeypatch.setattr(alpaca_client, "get_bars", lambda symbols, **kw: {"GLD": [{"t": "2026-10-02T14:30:00Z", "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}]})

    class FakeStream:
        def bars(self, symbol):
            assert symbol == "GLD"
            return pd.DataFrame([{"ts": t0 + 60, "open": 2.0, "high": 2.0, "low": 2.0, "close": 2.0, "volume": 9.0}])

    monkeypatch.setattr(alpaca_stream, "_streams", {"stocks": FakeStream()})
    kalshi_15m_setup._metals_cache.clear()  # noqa: SLF001
    df = kalshi_15m_setup.metals_candles("GOLD")
    assert df.close.tolist() == [1.0, 2.0] and df.ts.tolist() == [t0 + 60, t0 + 120]


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


def test_gold_perp_reads_gld_with_silver_as_leader(monkeypatch):
    from data import kalshi_15m_setup, perps_setup
    seen = []
    monkeypatch.setattr(kalshi_15m_setup, "metals_candles", lambda metal: seen.append(metal) or pd.read_parquet(FIXTURE))
    assert not perps_setup.chart_candles("KXGOLDPERP").empty and seen == ["GOLD"]
    assert perps_setup.leader_for("GOLD") == "SILVER" and perps_setup.chart_session("GOLD") == "us_equity"
    assert perps_setup.chart_session("BTC") == "utc_day"


def test_alpaca_crypto_reads_the_more_complete_real_chart(monkeypatch):
    from data import alpaca_crypto_data, alpaca_crypto_setup, kalshi_15m_spot
    now = 1_790_100_000
    full = pd.DataFrame({"ts": now // 60 * 60 - 60 * np.arange(300)[::-1], "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0})
    gappy_bars = full.iloc[::3].assign(ts=lambda d: d.ts - 60)  # Alpaca bars are stamped by start
    monkeypatch.setattr(alpaca_crypto_data, "fetch_recent_crypto_bars", lambda symbol, **kw: gappy_bars)
    monkeypatch.setattr(kalshi_15m_spot, "is_listed", lambda coin: True)
    monkeypatch.setattr(kalshi_15m_spot, "recent_series", lambda coin: full.assign(coin=coin))
    df, source = alpaca_crypto_setup.chart_candles("SOL/USD", now=now)
    assert source.startswith("Alpaca Kraken US SOL/USD (100% complete") and len(df) == 300
    monkeypatch.setattr(kalshi_15m_spot, "is_listed", lambda coin: False)
    df, source = alpaca_crypto_setup.chart_candles("SOL/USD", now=now)
    assert source.startswith("Alpaca exchange SOL/USD")


def test_stablecoin_pairs_are_never_traded():
    from data import alpaca_crypto_setup
    r = alpaca_crypto_setup.live_setup("USDT/USD", fee_rate_roundtrip=0.005, news_score=None)
    assert r["valid"] is False and "stablecoin" in r["checks"]["data"]["detail"]


def test_a_kraken_chart_plan_is_carried_to_the_alpaca_exchange_price():
    from data import alpaca_crypto_setup
    plan = alpaca_crypto_setup.plan_at_price({"plan": {"stop": 99.0, "target": 106.0}}, price=100.5, fee_rate_roundtrip=0.0,
                                             spread_bps=0.0, chart_price=100.0, min_rr=1.0)
    assert plan["stop"] == pytest.approx(99.495) and plan["target"] == pytest.approx(106.53)
