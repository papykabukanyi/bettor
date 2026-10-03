"""Live Alpaca minute-bar streams (alpaca_stream)."""
from __future__ import annotations

import json

import pandas as pd

from data import alpaca_stream as s


def _stream(symbols=("AAPL", "SPY")):
    return s.BarStream("stocks", "wss://example/v2/sip", lambda: list(symbols))


def test_minute_and_corrected_bars_are_kept_by_bar_start_time():
    st = _stream()
    st._handle([{"T": "b", "S": "AAPL", "t": "2026-10-05T13:30:00Z", "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 100},  # noqa: SLF001
                {"T": "b", "S": "AAPL", "t": "2026-10-05T13:31:00Z", "o": 1.5, "h": 2, "l": 1, "c": 1.8, "v": 50}])
    st._handle([{"T": "u", "S": "AAPL", "t": "2026-10-05T13:31:00Z", "o": 1.5, "h": 2.2, "l": 1, "c": 1.9, "v": 60}])  # noqa: SLF001
    bars = st.bars("AAPL")
    assert list(bars.ts) == [1791207000, 1791207060]
    assert bars.iloc[-1]["close"] == 1.9 and bars.iloc[-1]["volume"] == 60  # the correction replaced the bar
    assert st.status()["bars"] == 3 and st.status()["last_bar_age_sec"] is not None


def test_stream_bars_go_on_top_of_rest_history(monkeypatch):
    st = _stream()
    st._handle([{"T": "b", "S": "SPY", "t": "2026-10-05T13:31:00Z", "o": 2, "h": 2, "l": 2, "c": 2, "v": 9}])  # noqa: SLF001
    monkeypatch.setitem(s._streams, "stocks", st)  # noqa: SLF001
    rest = pd.DataFrame({"ts": [1791207000, 1791207060], "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0})
    merged = s.merge_live("stocks", "SPY", rest)
    assert list(merged.ts) == [1791207000, 1791207060] and merged.iloc[-1]["close"] == 2.0
    assert s.merge_live("crypto", "BTC/USD", rest) is rest  # no crypto stream running: unchanged


class _FakeWs:
    def __init__(self):
        self.sent = []

    def send(self, raw):
        self.sent.append(json.loads(raw))


def test_subscriptions_follow_the_trading_universe():
    universe = ["AAPL", "SPY"]
    st = s.BarStream("stocks", "wss://example", lambda: list(universe))
    ws = _FakeWs()
    st._subscribe(ws, force=True)  # noqa: SLF001
    assert ws.sent[-1] == {"action": "subscribe", "bars": ["AAPL", "SPY"], "updatedBars": ["AAPL", "SPY"]}
    universe[:] = ["SPY", "NVDA"]
    st._subscribe(ws)  # noqa: SLF001
    assert {"action": "unsubscribe", "bars": ["AAPL"], "updatedBars": ["AAPL"]} in ws.sent
    assert ws.sent[-1]["bars"] == ["NVDA", "SPY"]


def test_streams_stay_off_without_keys(monkeypatch):
    monkeypatch.delenv("ALPACA_API_KEY_ID", raising=False)
    assert s.start_all() == {"ok": False, "reason": "disabled_or_no_keys"}


def test_live_ticks_give_the_price_as_of_now():
    st = s.BarStream("crypto", "wss://example/v1beta3/crypto/us-1", lambda: ["BTC/USD"],
                     lambda: {"quotes": ["BTC/USD"]})
    st._handle([{"T": "q", "S": "BTC/USD", "t": "2026-10-05T13:30:05Z", "bp": 100.0, "ap": 100.2, "bs": 1, "as": 1}])  # noqa: SLF001
    st._handle([{"T": "q", "S": "BTC/USD", "t": "2026-10-05T13:30:01Z", "bp": 90.0, "ap": 90.2}])  # noqa: SLF001 (older: ignored)
    st._handle([{"T": "t", "S": "GLD", "t": "2026-10-05T13:30:07Z", "p": 380.5, "s": 10}])  # noqa: SLF001
    s._streams["crypto"] = st  # noqa: SLF001
    try:
        at = pd.Timestamp("2026-10-05T13:30:05Z").timestamp()
        tick = s.latest_price("crypto", "BTC/USD", now=at + 3)
        assert tick["price"] == 100.1 and tick["kind"] == "quote_mid" and tick["age_sec"] == 3.0
        assert s.latest_price("crypto", "BTC/USD", now=at + 60) is None  # too old: callers use the bar close
        assert s.latest_price("crypto", "GLD", now=at + 3)["price"] == 380.5
        assert s.latest_price("stocks", "GLD", now=at) is None  # no stocks stream running
    finally:
        s._streams.pop("crypto", None)  # noqa: SLF001


def test_tick_channels_are_subscribed_alongside_bars():
    st = s.BarStream("crypto", "wss://example", lambda: ["BTC/USD", "SOL/USD"], lambda: {"quotes": ["BTC/USD"]})
    ws = _FakeWs()
    st._subscribe(ws, force=True)  # noqa: SLF001
    assert ws.sent[-1] == {"action": "subscribe", "bars": ["BTC/USD", "SOL/USD"], "updatedBars": ["BTC/USD", "SOL/USD"],
                           "quotes": ["BTC/USD"]}
    assert st.status()["subscribed_ticks"] == 1


def test_the_crypto_stream_reads_the_chart_venue_with_the_kalshi_coins(monkeypatch):
    from data import alpaca_client, alpaca_crypto_data
    monkeypatch.setenv("ALPACA_API_KEY_ID", "k")
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["SOL/USD", "USDT/USD", "PEPE/USDC", "NEAR/USD"])
    fresh = pd.Timestamp.now(tz="UTC").isoformat()

    def latest_bars(path, *, params):
        assert path.endswith(f"/{alpaca_client.CHART_CRYPTO_LOC}/latest/bars")
        return {"bars": {p: {"t": "2025-10-03T04:59:00Z" if p == "NEAR/USD" else fresh} for p in params["symbols"].split(",")}}

    monkeypatch.setattr(alpaca_client, "_crypto_data_get", latest_bars)
    monkeypatch.setattr(s, "_venue_cache", {"at": 0.0, "key": None, "pairs": None})
    started = []
    monkeypatch.setattr(s.BarStream, "start", lambda self: started.append(self.name))
    monkeypatch.setattr(s, "_streams", {})
    s.start_all()
    crypto = s._streams["crypto"]  # noqa: SLF001
    assert crypto.url.endswith(f"/v1beta3/crypto/{alpaca_client.CHART_CRYPTO_LOC}")
    symbols = crypto.symbols_fn()
    assert "SOL/USD" in symbols and "BTC/USD" in symbols and "HYPE/USD" in symbols
    assert "USDT/USD" not in symbols and "PEPE/USDC" not in symbols
    assert "NEAR/USD" not in symbols  # the venue's last bar for it is a year old
    assert "GLD" in s._streams["stocks"].symbols_fn() and s._streams["stocks"].ticks_fn() == {"trades": s._commodity_etfs()}  # noqa: SLF001
