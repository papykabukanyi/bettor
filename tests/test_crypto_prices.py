"""crypto_prices: the perps' live price cross-check, Alpaca only."""
from __future__ import annotations

import pytest

from data import alpaca_client, alpaca_stream, crypto_prices as cp


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    monkeypatch.setattr(cp, "_cache", {})


def test_the_stream_quote_comes_first(monkeypatch):
    monkeypatch.setattr(alpaca_stream, "latest_price", lambda kind, pair, max_age_sec: {"price": 101.0, "age_sec": 1.2, "kind": "quote_mid"}
                        if (kind, pair) == ("crypto", "BTC/USD") else None)
    monkeypatch.setattr(cp, "_fetch_alpaca_latest", lambda pair: pytest.fail("no REST call when the stream is fresh"))
    assert cp.get_fast_price("BTC") == {"price": 101.0, "source": "alpaca_stream", "delayed": False, "age_sec": 1.2}


def test_rest_latest_quote_at_the_chart_venue_otherwise(monkeypatch):
    monkeypatch.setattr(alpaca_stream, "latest_price", lambda *a, **k: None)
    seen = []

    def fake_get(path, *, params):
        seen.append(path)
        return {"quotes": {"SHIB/USD": {"bp": 0.00001, "ap": 0.00003}}}

    monkeypatch.setattr(alpaca_client, "_crypto_data_get", fake_get)
    r = cp.get_fast_price("KSHIB")
    assert r["source"] == "alpaca_latest_quote" and r["price"] == pytest.approx(0.00002)
    assert seen == [f"/v1beta3/crypto/{alpaca_client.CHART_CRYPTO_LOC}/latest/quotes"]


def test_nothing_for_coins_alpaca_does_not_carry():
    assert cp.get_fast_price("NEAR") is None and cp.get_fast_price("GOLD") is None
