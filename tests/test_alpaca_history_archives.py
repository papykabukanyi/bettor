"""Alpaca archives behind the perps/15m studies: Kraken-via-Alpaca minute
bars and Benzinga news, with no-lookahead news reads."""
from __future__ import annotations

import pandas as pd
import pytest

from data import alpaca_client, alpaca_crypto_history as ch, alpaca_news_history as nh


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(ch, "LOCAL_DIR", tmp_path / "crypto")
    monkeypatch.setattr(nh, "LOCAL_DIR", tmp_path / "news")
    monkeypatch.delenv("HF_API_KEY", raising=False)


def test_crypto_history_reads_the_kraken_venue_a_month_at_a_time(monkeypatch):
    import datetime as dt
    calls = []

    def fake_bars(symbols, *, start, end, loc, max_pages):
        calls.append((symbols, start[:7], loc))
        return {symbols[0]: [{"t": start, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 3, "n": 4, "vw": 1.2}]}

    monkeypatch.setattr(alpaca_client, "get_crypto_bars", fake_bars)
    df = ch.fetch_bars("SOL", dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc), dt.datetime(2024, 4, 1, tzinfo=dt.timezone.utc))
    assert [c[1] for c in calls] == ["2024-01", "2024-02", "2024-03"] and {c[2] for c in calls} == {"us-1"}
    assert calls[0][0] == ["SOL/USD"] and list(df.columns) == ch.COLUMNS and len(df) == 3
    ch.write_year("SOL", 2024, df, merge_remote=False)
    candles = ch.candles("SOL", years=[2024])
    assert (candles.ts.to_numpy() == df.ts.to_numpy() + 60).all()  # stored by START, read by END


def test_news_months_are_fetched_scored_and_tagged(monkeypatch):
    pages = []

    def fake_get(path, *, params):
        pages.append(params.get("page_token"))
        if params.get("page_token") is None:
            return {"news": [{"id": 1, "created_at": "2024-03-01T10:00:00Z", "headline": "Bitcoin surges to record high",
                              "summary": "", "symbols": ["BTCUSD"], "source": "benzinga", "url": "u1"}], "next_page_token": "p2"}
        return {"news": [{"id": 2, "created_at": "2024-03-02T10:00:00Z", "headline": "Gold plunges", "summary": "selloff deepens",
                          "symbols": ["GLD"], "source": "benzinga", "url": "u2"}], "next_page_token": None}

    monkeypatch.setattr(alpaca_client, "_data_get", fake_get)
    df = nh.fetch_month(2024, 3, symbols=["BTCUSD", "GLD"])
    assert pages == [None, "p2"] and list(df.columns) == nh.COLUMNS
    assert df.loc[df.id == 1, "score"].item() > 0 and df.loc[df.id == 2, "score"].item() < 0
    assert df.loc[df.id == 2, "symbols"].item() == "GLD"


def test_news_reads_never_look_ahead():
    t0 = 1_790_000_000
    archive = pd.DataFrame({"id": [1, 2, 3], "created_at": [t0 - 7200, t0 - 60, t0 + 60], "headline": "", "summary": "",
                            "symbols": ["BTCUSD", "BTCUSD,ETHUSD", "BTCUSD"], "source": "", "url": "",
                            "score": [0.5, -1.0, 1.0]})
    idx = nh.NewsIndex(archive, ["BTCUSD"])
    assert idx.at(t0, hours=6) == {"count": 2.0, "score": -0.25}  # the article after t0 is not seen
    assert idx.at(t0, hours=1) == {"count": 1.0, "score": -1.0}
    assert nh.NewsIndex(archive, ["SOLUSD"]).at(t0) == {"count": 0.0, "score": 0.0}


def test_news_universe_covers_every_kalshi_coin_and_commodity():
    u = nh.universe()
    assert {"BTCUSD", "ETHUSD", "HYPEUSD", "GLD", "USO", "UNG", "SPY"} <= set(u)
